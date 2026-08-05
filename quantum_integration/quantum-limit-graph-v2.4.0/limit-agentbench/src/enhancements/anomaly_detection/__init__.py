"""
Enhanced Anomaly Detection for Sustainability Metrics v2.1.0
==============================================================
Multi‑Teacher On‑Policy Distillation for Adaptive Anomaly Response

All existing features (config, persistence, models, explanations, drift, API, metrics)
are retained. The response selection logic is now learned via distillation.
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

# ============================================================================
# 1. CONFIGURATION (Pydantic)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class AnomalyConfig(BaseModel):
        """Configuration for anomaly detection."""
        # Model type: "isolation_forest", "autoencoder", "threshold", "online_svm"
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
        # Persistence
        persistence_enabled: bool = True
        persistence_path: str = Field("./anomaly_state.db")
        model_save_path: str = Field("./models/")
        # Explanation
        enable_explanation: bool = True
        # Concept drift
        concept_drift_enabled: bool = True
        drift_threshold_multiplier: float = Field(2.0, gt=0)
        # Alert routing
        webhook_url: Optional[str] = None
        # Integration callbacks
        adaptive_cost_callback: Optional[Callable] = None
        predictive_maintenance_callback: Optional[Callable] = None

        # NEW: Distillation parameters
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

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
    # Fallback to dict if Pydantic not available
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
        # NEW distillation defaults
        "distillation_epsilon": 0.1,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
    }

# ============================================================================
# 2. DATA STRUCTURES
# ============================================================================
@dataclass
class AnomalyEvent:
    """Represents a detected anomaly."""
    timestamp: datetime
    node_id: str
    metric_name: str
    metric_value: float
    anomaly_score: float  # -1 normal, 1 anomaly (or probability)
    description: str
    alert_sent: bool = False
    auto_response_taken: str = ""  # "reroute", "restart", "none"
    # New: explanation
    explanation: Optional[Dict[str, float]] = None  # feature contributions

@dataclass
class Explanation:
    """Explanation of an anomaly detection."""
    feature_contributions: Dict[str, float]
    threshold_used: float
    reconstruction_error: float

# ============================================================================
# 3. TELEMETRY BUFFER (with persistence support)
# ============================================================================
class TelemetryBuffer:
    """Maintains a rolling window of telemetry data for each node, with persistence."""
    def __init__(self, window_size: int = 100, persistence_manager: Optional['PersistenceManager'] = None):
        self.window_size = window_size
        self.buffers: Dict[str, Dict[str, deque]] = {}
        self.persistence = persistence_manager

    def add_sample(self, node_id: str, metrics: Dict[str, float]) -> None:
        """Add a new sample for a node."""
        if node_id not in self.buffers:
            self.buffers[node_id] = {}
        for name, value in metrics.items():
            if name not in self.buffers[node_id]:
                self.buffers[node_id][name] = deque(maxlen=self.window_size)
            self.buffers[node_id][name].append(value)
        # Persist to DB if enabled
        if self.persistence:
            self.persistence.save_telemetry(node_id, metrics)

    def get_data(self, node_id: str, metric_names: List[str]) -> np.ndarray:
        """
        Return a 2D array of shape (samples, features) for the given node.
        Samples are the most recent up to window_size.
        """
        if node_id not in self.buffers:
            return np.empty((0, len(metric_names)))
        data = []
        for name in metric_names:
            if name in self.buffers[node_id]:
                data.append(list(self.buffers[node_id][name]))
            else:
                data.append([])
        # Transpose to (samples, features)
        samples = np.array(data).T
        return samples

    def get_latest(self, node_id: str, metric_names: List[str]) -> np.ndarray:
        """Return the latest sample (1D array) for the node."""
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
        """Check if we have at least some data for all metrics."""
        if node_id not in self.buffers:
            return False
        for name in metric_names:
            if name not in self.buffers[node_id] or len(self.buffers[node_id][name]) < 10:
                return False
        return True

    def load_from_persistence(self, node_id: str, metric_names: List[str], limit: int = 1000):
        """Load historical data from DB into buffer."""
        if not self.persistence:
            return
        records = self.persistence.load_telemetry(node_id, limit)
        if not records:
            return
        # Rebuild buffer
        if node_id not in self.buffers:
            self.buffers[node_id] = {}
        for name in metric_names:
            self.buffers[node_id][name] = deque(maxlen=self.window_size)
        for record in reversed(records):  # oldest first
            for name in metric_names:
                if name in record:
                    self.buffers[node_id][name].append(record[name])

# ============================================================================
# 4. PERSISTENCE MANAGER (SQLite)
# ============================================================================
class PersistenceManager:
    """Stores telemetry and trained models in SQLite."""
    def __init__(self, config: Union['AnomalyConfig', Dict[str, Any]]):
        self.config = config
        # If config is Pydantic, convert to dict
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
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_telemetry_node_time ON telemetry (node_id, timestamp)
        """)
        conn.commit()
        conn.close()

    def save_telemetry(self, node_id: str, metrics: Dict[str, float]):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO telemetry (node_id, timestamp, energy_joules, carbon_kg, helium_usage, latency_ms, accuracy)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            node_id, time.time(),
            metrics.get('energy_joules', 0),
            metrics.get('carbon_kg', 0),
            metrics.get('helium_usage', 0),
            metrics.get('latency_ms', 0),
            metrics.get('accuracy', 0)
        ))
        conn.commit()
        conn.close()

    def load_telemetry(self, node_id: str, limit: int = 1000) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("""
            SELECT timestamp, energy_joules, carbon_kg, helium_usage, latency_ms, accuracy
            FROM telemetry WHERE node_id = ? ORDER BY timestamp DESC LIMIT ?
        """, (node_id, limit)).fetchall()
        conn.close()
        return [{
            'timestamp': r[0],
            'energy_joules': r[1],
            'carbon_kg': r[2],
            'helium_usage': r[3],
            'latency_ms': r[4],
            'accuracy': r[5]
        } for r in rows]

    def save_model(self, node_id: str, model: 'BaseAnomalyModel'):
        model_blob = pickle.dumps(model)
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO models (node_id, model_type, model_blob, trained_at, config_snapshot)
            VALUES (?, ?, ?, ?, ?)
        """, (
            node_id,
            model.__class__.__name__,
            model_blob,
            time.time(),
            json.dumps(self.config_dict)
        ))
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
# 5. ANOMALY DETECTION MODELS (Enhanced)
# ============================================================================
class BaseAnomalyModel:
    """Abstract base for anomaly detection models."""
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.is_trained = False
        self.feature_names = config.get('metrics_features', [])

    def train(self, data: np.ndarray) -> None:
        raise NotImplementedError

    def partial_fit(self, data: np.ndarray) -> None:
        """For online models."""
        raise NotImplementedError

    def predict(self, data: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    def explain(self, data: np.ndarray) -> Dict[str, float]:
        """Return feature contributions for the latest sample."""
        raise NotImplementedError

class IsolationForestModel(BaseAnomalyModel):
    """Isolation Forest wrapper."""
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.model = None
        self.contamination = config.get('contamination', 0.05)

    def train(self, data: np.ndarray) -> None:
        if data.shape[0] < 10:
            logger.warning("Not enough data to train Isolation Forest.")
            self.is_trained = False
            return
        if not SKLEARN_AVAILABLE:
            logger.error("scikit-learn not installed; cannot use IsolationForest.")
            self.is_trained = False
            return
        self.model = IsolationForest(contamination=self.contamination, random_state=42)
        self.model.fit(data)
        self.is_trained = True

    def partial_fit(self, data: np.ndarray) -> None:
        # No online training for Isolation Forest, so we just retrain if needed
        self.train(data)

    def predict(self, data: np.ndarray) -> np.ndarray:
        if not self.is_trained or self.model is None:
            return np.full(data.shape[0], -1)
        return self.model.predict(data)

    def explain(self, data: np.ndarray) -> Dict[str, float]:
        """Feature contributions based on average path length (simplified)."""
        # For Isolation Forest, we can compute anomaly score contributions
        # Here we use the feature-wise anomaly score (implementation simplified)
        contributions = {}
        for i, name in enumerate(self.feature_names):
            # Use the feature's value relative to the mean of training data
            # This is a heuristic; real SHAP would be better.
            if self.model and hasattr(self.model, 'estimators_'):
                # Simple: use the mean of the feature across the training set
                # This is placeholder; real explanation is complex.
                contributions[name] = 0.0
        return contributions

class OnlineSVM(BaseAnomalyModel):
    """Online One-Class SVM using SGD."""
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.model = None
        self.nu = config.get('contamination', 0.05)
        self.initialized = False

    def train(self, data: np.ndarray) -> None:
        if not ONLINE_AVAILABLE:
            logger.error("SGDOneClassSVM not available; falling back to IsolationForest.")
            return
        if data.shape[0] < 10:
            return
        self.model = SGDOneClassSVM(nu=self.nu, random_state=42)
        self.model.partial_fit(data)
        self.is_trained = True
        self.initialized = True

    def partial_fit(self, data: np.ndarray) -> None:
        if not self.initialized:
            self.train(data)
        else:
            self.model.partial_fit(data)

    def predict(self, data: np.ndarray) -> np.ndarray:
        if not self.is_trained or self.model is None:
            return np.full(data.shape[0], -1)
        return self.model.predict(data)  # -1 normal, 1 anomaly

    def explain(self, data: np.ndarray) -> Dict[str, float]:
        # For SVM, we can use feature weights if available
        if self.model and hasattr(self.model, 'coef_'):
            coefs = self.model.coef_.flatten()
            contributions = {name: coefs[i] for i, name in enumerate(self.feature_names)}
            # Normalize to percentage
            total = np.sum(np.abs(coefs)) + 1e-8
            return {k: v / total for k, v in contributions.items()}
        return {}

class AutoencoderModel(BaseAnomalyModel):
    """Simple autoencoder using PyTorch."""
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.model = None
        self.input_dim = None
        self.hidden_dims = config.get('autoencoder_hidden', [16, 8, 16])
        self.reconstruction_threshold = None
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.encoder = None
        self.decoder = None

    def _build_network(self, input_dim: int) -> nn.Module:
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch not installed; cannot use autoencoder.")
        layers = []
        dims = [input_dim] + self.hidden_dims
        for i in range(len(dims) - 1):
            layers.append(nn.Linear(dims[i], dims[i+1]))
            layers.append(nn.ReLU())
        # Decoder (symmetric)
        for i in range(len(dims) - 2, 0, -1):
            layers.append(nn.Linear(dims[i+1], dims[i]))
            layers.append(nn.ReLU())
        layers.append(nn.Linear(dims[1], input_dim))
        return nn.Sequential(*layers)

    def train(self, data: np.ndarray) -> None:
        if data.shape[0] < 10:
            logger.warning("Not enough data to train autoencoder.")
            self.is_trained = False
            return
        if not TORCH_AVAILABLE:
            logger.error("PyTorch not installed; cannot use autoencoder.")
            self.is_trained = False
            return

        self.input_dim = data.shape[1]
        self.model = self._build_network(self.input_dim).to(self.device)
        data_tensor = torch.tensor(data, dtype=torch.float32).to(self.device)
        optimizer = optim.Adam(self.model.parameters(), lr=0.001)
        criterion = nn.MSELoss()

        # Train for a few epochs
        self.model.train()
        for epoch in range(50):
            optimizer.zero_grad()
            reconstructed = self.model(data_tensor)
            loss = criterion(reconstructed, data_tensor)
            loss.backward()
            optimizer.step()

        # Compute reconstruction errors on training data to set threshold
        self.model.eval()
        with torch.no_grad():
            reconstructed = self.model(data_tensor)
            errors = torch.mean((reconstructed - data_tensor) ** 2, dim=1).cpu().numpy()
        self.reconstruction_threshold = np.percentile(errors, 95)
        self.is_trained = True

    def partial_fit(self, data: np.ndarray) -> None:
        # For autoencoder, we can fine-tune with new data
        # This is not implemented; we'll just retrain if needed.
        self.train(data)

    def predict(self, data: np.ndarray) -> np.ndarray:
        if not self.is_trained or self.model is None:
            return np.full(data.shape[0], -1)
        data_tensor = torch.tensor(data, dtype=torch.float32).to(self.device)
        self.model.eval()
        with torch.no_grad():
            reconstructed = self.model(data_tensor)
            errors = torch.mean((reconstructed - data_tensor) ** 2, dim=1).cpu().numpy()
        return np.where(errors > self.reconstruction_threshold, 1, -1)

    def explain(self, data: np.ndarray) -> Dict[str, float]:
        """Feature contributions based on reconstruction error per feature."""
        if not self.is_trained or self.model is None:
            return {}
        data_tensor = torch.tensor(data, dtype=torch.float32).to(self.device)
        self.model.eval()
        with torch.no_grad():
            reconstructed = self.model(data_tensor)
            diff = (reconstructed - data_tensor).cpu().numpy()
        # Mean absolute error per feature over the batch
        contributions = {}
        for i, name in enumerate(self.feature_names):
            contributions[name] = np.mean(np.abs(diff[:, i]))
        total = sum(contributions.values()) + 1e-8
        return {k: v / total for k, v in contributions.items()}

class ThresholdModel(BaseAnomalyModel):
    """Simple threshold‑based anomaly detection using rolling mean and std."""
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.threshold_multiplier = config.get('energy_spike_threshold', 2.0)
        self.means = None
        self.stds = None

    def train(self, data: np.ndarray) -> None:
        if data.shape[0] == 0:
            self.is_trained = False
            return
        self.means = np.mean(data, axis=0)
        self.stds = np.std(data, axis=0)
        self.stds[self.stds == 0] = 1e-6
        self.is_trained = True

    def partial_fit(self, data: np.ndarray) -> None:
        # Simple online update: moving average
        # Not implemented; we'll just retrain.
        self.train(data)

    def predict(self, data: np.ndarray) -> np.ndarray:
        if not self.is_trained:
            return np.full(data.shape[0], -1)
        z_scores = np.abs((data - self.means) / self.stds)
        anomalies = np.any(z_scores > self.threshold_multiplier, axis=1)
        return np.where(anomalies, 1, -1)

    def explain(self, data: np.ndarray) -> Dict[str, float]:
        if not self.is_trained:
            return {}
        z_scores = np.abs((data - self.means) / self.stds)
        contributions = {}
        total = np.sum(z_scores) + 1e-8
        for i, name in enumerate(self.feature_names):
            contributions[name] = z_scores[0, i] / total
        return contributions

# ============================================================================
# 6. NEW: DISTILLATION COMPONENTS FOR RESPONSE SELECTION
# ============================================================================

@dataclass
class AnomalyResponseState:
    """State for the distillation agent."""
    anomaly_score: float
    metric_name_encoded: float  # one-hot index as float (0-4 for 5 metrics)
    node_id_hash: float  # simplified: hash of node_id normalized
    persistent_count: int
    carbon_intensity: float
    system_load: float
    hour_of_day: float
    # Historical
    recent_action_success_rate: float
    avg_reward: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 10‑dim numeric feature vector."""
        features = [
            self.anomaly_score,
            self.metric_name_encoded / 5.0,
            self.node_id_hash,
            min(self.persistent_count / 10.0, 1.0),
            min(self.carbon_intensity / 1000.0, 1.0),
            min(self.system_load / 100.0, 1.0),
            self.hour_of_day / 24.0,
            self.recent_action_success_rate,
            self.avg_reward,
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: AnomalyResponseState) -> np.ndarray:
        """Return probability vector over 5 actions."""
        pass

    @abstractmethod
    def confidence(self, state: AnomalyResponseState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class ResponseRuleBasedTeacher(Teacher):
    """Rule‑based expert: uses original heuristics."""
    ACTION_SPACE = ['alert_only', 'reroute', 'restart', 'escalate', 'adaptive_cost']

    def predict(self, state: AnomalyResponseState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.persistent_count >= 3:  # original threshold
            probs[2] = 0.8   # restart
        elif state.anomaly_score > 0.8:
            probs[1] = 0.7   # reroute
        elif state.metric_name_encoded in [0,1]:  # energy or carbon
            probs[4] = 0.6   # adaptive_cost
        else:
            probs[0] = 0.6   # alert_only
        return probs / probs.sum()

    def confidence(self, state: AnomalyResponseState) -> float:
        if state.persistent_count >= 3:
            return 0.6
        return 0.4


class ResponseHistoricalMLTeacher(Teacher):
    """Offline trained classifier on historical optimal actions."""
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if model_path and Path(model_path).exists() and SKLEARN_ML:
            import joblib
            self.model = joblib.load(model_path)

    def predict(self, state: AnomalyResponseState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: AnomalyResponseState) -> float:
        return 0.7 if self.model is not None else 0.0


class ResponseStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, detector: 'AnomalyDetector', lr: float = 0.1):
        self.detector = detector
        self.lr = lr
        self.weights = np.zeros((9, 5))  # 9 features, 5 actions
        self._load_state()

    def _load_state(self):
        # Load from persistence (we'll store in the detector's persistence)
        # For simplicity, we'll use a separate key in the state table.
        pass

    def _save_state(self):
        pass

    def predict(self, state: AnomalyResponseState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: AnomalyResponseState) -> float:
        return 0.5

    def update(self, state: AnomalyResponseState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x


class DistillationStudent:
    """Linear softmax student updated via distillation + policy gradient."""
    def __init__(self, feature_dim: int = 9, n_classes: int = 5, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector)
        logits = state_vector @ self.weights + self.biases

        # Distillation gradient (KL divergence)
        grad_distill = -(teacher_probs - current_probs)

        # Policy gradient (REINFORCE)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)

        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec: np.ndarray, action: int, reward: float,
             next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self):
        return len(self.buffer)


class DistillationResponseOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for anomaly response selection.
    """
    ACTION_SPACE = ['alert_only', 'reroute', 'restart', 'escalate', 'adaptive_cost']

    def __init__(self, detector: 'AnomalyDetector', config: Dict[str, Any]):
        self.detector = detector
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            ResponseRuleBasedTeacher(),
            ResponseHistoricalMLTeacher(),  # optionally load model
            ResponseStatefulQTeacher(detector)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_action(self, state: AnomalyResponseState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()

        # Ensemble teachers
        teacher_probs = np.zeros(5)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            teacher_probs += prob * conf
            total_conf += conf
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

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1

        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])

        # Update Q-teacher (if we have the original state)
        # We'll do that separately in the main loop.

    def get_stats(self) -> Dict:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'weights_norm': float(np.linalg.norm(self.student.weights))
        }


# ============================================================================
# 7. ANOMALY DETECTOR ORCHESTRATOR (Enhanced)
# ============================================================================
class AnomalyDetector:
    """
    Main anomaly detection engine. Maintains per‑node models, ingests telemetry,
    raises events, and provides enhanced features with distillation.
    """

    def __init__(self, config: Optional[Union['AnomalyConfig', Dict]] = None):
        if config is None:
            config = ANOMALY_CONFIG.copy() if isinstance(ANOMALY_CONFIG, dict) else ANOMALY_CONFIG
        # Convert to dict for uniform access
        if hasattr(config, 'dict'):
            self.config = config.dict()
        else:
            self.config = config.copy() if isinstance(config, dict) else dict(config)

        # Persistence
        self.persistence = None
        if self.config.get('persistence_enabled', True):
            self.persistence = PersistenceManager(self.config)

        # Buffer
        self.buffer = TelemetryBuffer(self.config.get('window_size', 100), self.persistence)

        # Models
        self.models: Dict[str, BaseAnomalyModel] = {}  # node_id -> model
        self.last_training: Dict[str, float] = {}  # node_id -> timestamp
        self.anomaly_history: Dict[str, List[AnomalyEvent]] = {}
        self.alert_cooldown: Dict[str, float] = {}  # node_id -> last alert time
        self.persistent_anomaly_count: Dict[str, int] = {}
        # Concept drift tracking
        self.drift_scores: Dict[str, deque] = {}
        # Per-node locks
        self._node_locks: Dict[str, asyncio.Lock] = {}

        # Model factory
        model_type = self.config.get('model_type', 'isolation_forest')
        if model_type == "isolation_forest":
            self.ModelClass = IsolationForestModel
        elif model_type == "autoencoder":
            self.ModelClass = AutoencoderModel
        elif model_type == "online_svm":
            self.ModelClass = OnlineSVM
        else:
            self.ModelClass = ThresholdModel

        # External integration hooks
        self.alert_callback: Optional[Callable[[AnomalyEvent], Any]] = None
        self.auto_response_callback: Optional[Callable[[AnomalyEvent], Any]] = None
        self.evolutionary_engine_callback: Optional[Callable[[str, float], Any]] = None
        self.adaptive_cost_callback: Optional[Callable[[float], Any]] = None
        self.predictive_maintenance_callback: Optional[Callable[[str, float], Any]] = None

        # NEW: Distillation optimizer
        self.response_optimizer = DistillationResponseOptimizer(self, self.config)

        # Prometheus metrics
        if PROMETHEUS_AVAILABLE:
            self.metrics = {
                'detections': Counter('anomaly_detections_total', ['node', 'metric']),
                'alerts': Counter('anomaly_alerts_total', ['node', 'metric']),
                'auto_responses': Counter('anomaly_auto_responses_total', ['node', 'action']),
                'latency': Histogram('anomaly_detection_latency_seconds'),
                'buffer_size': Gauge('anomaly_buffer_size', ['node']),
                'model_state': Gauge('anomaly_model_state', ['node', 'model_type']),
            }
        else:
            self.metrics = {}

        logger.info("Enhanced AnomalyDetector initialized with config: %s", self.config)

    # Callback registration
    def register_alert_callback(self, callback: Callable[[AnomalyEvent], Any]):
        self.alert_callback = callback

    def register_auto_response_callback(self, callback: Callable[[AnomalyEvent], Any]):
        self.auto_response_callback = callback

    def register_evolutionary_engine_callback(self, callback: Callable[[str, float], Any]):
        self.evolutionary_engine_callback = callback

    def register_adaptive_cost_callback(self, callback: Callable[[float], Any]):
        self.adaptive_cost_callback = callback

    def register_predictive_maintenance_callback(self, callback: Callable[[str, float], Any]):
        self.predictive_maintenance_callback = callback

    # ----- Model management -----
    def _ensure_model(self, node_id: str) -> BaseAnomalyModel:
        """Create or retrieve a model for a node, with persistence loading."""
        if node_id not in self.models:
            # Try to load from persistence
            model = None
            if self.persistence:
                model = self.persistence.load_model(node_id)
            if model is None:
                model = self.ModelClass(self.config)
                self.last_training[node_id] = 0.0
            else:
                self.last_training[node_id] = time.time()
                logger.info(f"Loaded model for node {node_id} from persistence")
            self.models[node_id] = model
            self.anomaly_history[node_id] = []
            self.drift_scores[node_id] = deque(maxlen=100)
            self._node_locks[node_id] = asyncio.Lock()
        return self.models[node_id]

    def _should_retrain(self, node_id: str) -> bool:
        """Check if enough time has passed since last training."""
        if node_id not in self.last_training:
            return True
        elapsed = time.time() - self.last_training[node_id]
        return elapsed > self.config.get('retrain_interval_seconds', 3600)

    def _update_model(self, node_id: str, data: np.ndarray) -> None:
        """Train or retrain the model for a node if conditions met."""
        model = self._ensure_model(node_id)
        if self._should_retrain(node_id) and data.shape[0] >= 10:
            # For online models, we can do partial fit, but for others we retrain
            if isinstance(model, OnlineSVM):
                model.partial_fit(data)
            else:
                model.train(data)
            self.last_training[node_id] = time.time()
            # Save model to persistence
            if self.persistence:
                self.persistence.save_model(node_id, model)

    # ----- Missing data imputation -----
    def _impute_missing(self, metrics: Dict[str, float], node_id: str) -> Dict[str, float]:
        """Fill missing values with forward fill from history."""
        features = self.config.get('metrics_features', [])
        imputed = {}
        for feat in features:
            if feat in metrics and metrics[feat] is not None:
                imputed[feat] = metrics[feat]
            else:
                # Look up last known value from buffer
                if node_id in self.buffer.buffers and feat in self.buffer.buffers[node_id]:
                    last_values = list(self.buffer.buffers[node_id][feat])
                    if last_values:
                        imputed[feat] = last_values[-1]
                    else:
                        imputed[feat] = 0.0
                else:
                    imputed[feat] = 0.0
        return imputed

    # ----- Concept drift detection -----
    def _check_concept_drift(self, node_id: str, reconstruction_error: float) -> bool:
        """Check if reconstruction error distribution has shifted."""
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
        if reconstruction_error > mean + self.config.get('drift_threshold_multiplier', 2.0) * std:
            logger.warning(f"Concept drift detected for node {node_id}, retraining model")
            return True
        return False

    # ----- Main ingest method (async) -----
    async def ingest(self, node_id: str, metrics: Dict[str, float]) -> Optional[AnomalyEvent]:
        """
        Process a new telemetry sample for a node asynchronously.
        Returns an AnomalyEvent if an anomaly is detected, else None.
        """
        start_time = time.time()
        # Impute missing values
        metrics = self._impute_missing(metrics, node_id)

        # Filter to configured features
        features = self.config.get('metrics_features', ["energy_joules", "carbon_kg", "helium_usage", "latency_ms", "accuracy"])
        filtered_metrics = {k: v for k, v in metrics.items() if k in features}

        # Update buffer (and persistence)
        self.buffer.add_sample(node_id, filtered_metrics)

        # Check if we have enough data
        if not self.buffer.has_enough_data(node_id, features):
            return None

        # Get data window
        data_window = self.buffer.get_data(node_id, features)
        if data_window.shape[0] < 10:
            return None

        # Update model (train if needed)
        self._update_model(node_id, data_window)

        # Get latest sample
        latest = self.buffer.get_latest(node_id, features)
        if latest.size == 0:
            return None

        # Predict on latest sample
        latest_reshaped = latest.reshape(1, -1)
        model = self._ensure_model(node_id)
        if not model.is_trained:
            return None
        prediction = model.predict(latest_reshaped)[0]

        # Check for concept drift (reconstruction error for autoencoder)
        if self.config.get('concept_drift_enabled', True) and isinstance(model, AutoencoderModel):
            # Compute reconstruction error for latest
            data_tensor = torch.tensor(latest_reshaped, dtype=torch.float32).to(model.device)
            model.model.eval()
            with torch.no_grad():
                reconstructed = model.model(data_tensor)
                error = torch.mean((reconstructed - data_tensor) ** 2).item()
            if self._check_concept_drift(node_id, error):
                # Trigger retraining
                model.train(data_window)
                self.last_training[node_id] = time.time()
                if self.persistence:
                    self.persistence.save_model(node_id, model)

        # Anomaly detection
        if prediction == 1:
            event = self._create_event(node_id, filtered_metrics, model, prediction)
            await self._handle_anomaly(event)  # now async
            # Record metrics
            if PROMETHEUS_AVAILABLE:
                self.metrics['detections'].labels(node=node_id, metric=event.metric_name).inc()
                self.metrics['latency'].observe(time.time() - start_time)
            return event
        else:
            # Reset persistent count if normal
            async with self._get_node_lock(node_id):
                self.persistent_anomaly_count[node_id] = 0
            return None

    def _get_node_lock(self, node_id: str) -> asyncio.Lock:
        """Get or create a lock for a node."""
        if node_id not in self._node_locks:
            self._node_locks[node_id] = asyncio.Lock()
        return self._node_locks[node_id]

    # ----- Event creation and handling (enhanced) -----
    def _create_event(self, node_id: str, metrics: Dict[str, float], model: BaseAnomalyModel, prediction: int) -> AnomalyEvent:
        """Create an AnomalyEvent object with explanation."""
        features = self.config.get('metrics_features', [])
        # Determine which metric is most anomalous
        if isinstance(model, ThresholdModel):
            means = model.means
            stds = model.stds
            if means is not None:
                metric_values = np.array([metrics.get(f, 0.0) for f in features])
                z_scores = np.abs((metric_values - means) / (stds + 1e-6))
                idx = np.argmax(z_scores)
                metric_name = features[idx]
                metric_value = metrics.get(metric_name, 0.0)
            else:
                metric_name = features[0] if features else "unknown"
                metric_value = metrics.get(metric_name, 0.0)
        else:
            metric_name = features[0] if features else "unknown"
            metric_value = metrics.get(metric_name, 0.0)

        # Anomaly score: for Isolation Forest, -1 normal, 1 anomaly
        score = 0.9 if prediction == 1 else 0.1

        # Generate explanation
        explanation = None
        if self.config.get('enable_explanation', True):
            try:
                latest = self.buffer.get_latest(node_id, features)
                latest_reshaped = latest.reshape(1, -1)
                explanation = model.explain(latest_reshaped)
            except Exception as e:
                logger.debug(f"Explanation generation failed: {e}")

        desc = f"Anomaly detected on {node_id}: {metric_name} = {metric_value:.4f} (above expected range)."
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

    async def _handle_anomaly(self, event: AnomalyEvent) -> None:
        """Process an anomaly using the distillation agent."""
        node_id = event.node_id
        async with self._get_node_lock(node_id):
            self.persistent_anomaly_count[node_id] = self.persistent_anomaly_count.get(node_id, 0) + 1

            # Build state for distillation
            state = self._get_response_state(event)
            # Select action
            action, action_idx, state_vec, teacher_probs = await self.response_optimizer.select_action(state, exploration=True)

            # Execute action
            await self._execute_action(action, event)

            # We'll compute reward after a short observation period (simulated here)
            # In production, you would wait for subsequent metrics to see effect.
            # For demo, we simulate a reward based on action success.
            # Real implementation: after a timeout, query metrics and compute reward.
            reward = self._simulate_reward(action, event)
            next_state = self._get_response_state(event)  # could be different after action

            # Update agent
            await self.response_optimizer.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs)

            # Call integration callbacks unconditionally (or based on action)
            if self.evolutionary_engine_callback:
                self._safe_call_callback(self.evolutionary_engine_callback, node_id, event.anomaly_score)
            if self.adaptive_cost_callback:
                self._safe_call_callback(self.adaptive_cost_callback, event.anomaly_score)
            if self.predictive_maintenance_callback:
                self._safe_call_callback(self.predictive_maintenance_callback, node_id, event.anomaly_score)

            # Store in history
            if node_id not in self.anomaly_history:
                self.anomaly_history[node_id] = []
            event.auto_response_taken = action
            self.anomaly_history[node_id].append(event)
            if len(self.anomaly_history[node_id]) > 100:
                self.anomaly_history[node_id] = self.anomaly_history[node_id][-100:]

            # Webhook
            webhook_url = self.config.get('webhook_url')
            if webhook_url:
                asyncio.create_task(self._send_webhook(event, webhook_url))

    # ----- NEW: Build response state -----
    def _get_response_state(self, event: AnomalyEvent) -> AnomalyResponseState:
        """Build state for the distillation agent."""
        # Map metric name to index (one-hot encoded as float)
        metric_names = self.config.get('metrics_features', [])
        try:
            metric_idx = metric_names.index(event.metric_name)
        except ValueError:
            metric_idx = 0
        metric_encoded = float(metric_idx)

        # Node hash (simplified)
        node_hash = float(int(hashlib.md5(event.node_id.encode()).hexdigest()[:8], 16)) / (16**8)

        # Persistent count
        persistent_count = self.persistent_anomaly_count.get(event.node_id, 0)

        # External signals (mock)
        carbon_intensity = 400.0  # could be fetched from carbon manager
        system_load = 50.0        # mock
        hour = datetime.now().hour

        # Historical stats from recent events
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

    # ----- NEW: Execute action -----
    async def _execute_action(self, action: str, event: AnomalyEvent):
        """Perform the selected action."""
        logger.info(f"Executing action '{action}' for node {event.node_id}")
        if action == 'alert_only':
            # Send alert if not in cooldown
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
                logger.info(f"AUTO‑REROUTE for {event.node_id} due to anomaly.")
        elif action == 'restart':
            if self.auto_response_callback:
                self._safe_call_callback(self.auto_response_callback, event)
            else:
                logger.info(f"AUTO‑RESTART for {event.node_id} due to persistent anomalies.")
            self.persistent_anomaly_count[event.node_id] = 0
        elif action == 'escalate':
            # Escalate to human (could be a separate callback)
            logger.warning(f"ESCALATE: anomaly on {event.node_id} requires human attention.")
            if self.alert_callback:
                self._safe_call_callback(self.alert_callback, event)
        elif action == 'adaptive_cost':
            if self.adaptive_cost_callback:
                self._safe_call_callback(self.adaptive_cost_callback, event.anomaly_score)
            else:
                logger.info(f"ADAPTIVE_COST triggered for {event.node_id}.")
        else:
            logger.warning(f"Unknown action: {action}")

    # ----- NEW: Simulate reward (placeholder) -----
    def _simulate_reward(self, action: str, event: AnomalyEvent) -> float:
        """Simulate reward based on action effectiveness. In production, compute from real feedback."""
        # For demo: if action was 'restart' and persistent_count high, it's good.
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

    # ----- Safe callback -----
    def _safe_call_callback(self, callback: Callable, *args, **kwargs):
        """Call a callback safely, supporting both sync and async."""
        try:
            result = callback(*args, **kwargs)
            if asyncio.iscoroutine(result):
                asyncio.create_task(result)
        except Exception as e:
            logger.error("Callback execution failed", error=str(e))

    # ----- Webhook -----
    async def _send_webhook(self, event: AnomalyEvent, url: str):
        """Send anomaly event to a webhook URL."""
        try:
            if not AIOHTTP_AVAILABLE:
                logger.warning("aiohttp not installed; cannot send webhook.")
                return
            payload = {
                'event': 'anomaly_detected',
                'node_id': event.node_id,
                'metric': event.metric_name,
                'value': event.metric_value,
                'score': event.anomaly_score,
                'description': event.description,
                'timestamp': event.timestamp.isoformat(),
                'explanation': event.explanation,
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=5) as resp:
                    if resp.status != 200:
                        logger.warning(f"Webhook returned {resp.status}")
        except Exception as e:
            logger.error(f"Webhook send failed: {e}")

    # ----- Public utility methods -----
    async def load_persisted_data(self, node_id: str):
        """Load historical telemetry from DB into buffer."""
        if self.persistence:
            self.buffer.load_from_persistence(node_id, self.config.get('metrics_features', []))

    def get_anomaly_history(self, node_id: str, limit: int = 100) -> List[AnomalyEvent]:
        return self.anomaly_history.get(node_id, [])[-limit:]

    def get_model_status(self, node_id: str) -> Dict:
        if node_id not in self.models:
            return {"status": "no_model"}
        model = self.models[node_id]
        return {
            "status": "trained" if model.is_trained else "untrained",
            "model_type": model.__class__.__name__,
            "last_training": self.last_training.get(node_id, 0),
            "buffer_size": sum(len(q) for q in self.buffer.buffers.get(node_id, {}).values()) if node_id in self.buffer.buffers else 0,
        }

    # ----- NEW: Get distillation stats -----
    def get_distillation_stats(self) -> Dict:
        return self.response_optimizer.get_stats()

    async def shutdown(self):
        """Save models and close persistence."""
        if self.persistence:
            for node_id, model in self.models.items():
                self.persistence.save_model(node_id, model)
        logger.info("AnomalyDetector shutdown complete.")

# ============================================================================
# 8. TELEMETRY COLLECTOR (async)
# ============================================================================
class TelemetryCollector:
    """Async TelemetryCollector that feeds metrics to the AnomalyDetector."""
    def __init__(self, anomaly_detector: AnomalyDetector):
        self.detector = anomaly_detector
        self.is_running = False

    def start(self):
        self.is_running = True
        logger.info("TelemetryCollector started.")

    def stop(self):
        self.is_running = False
        logger.info("TelemetryCollector stopped.")

    async def receive_telemetry(self, node_id: str, metrics: Dict[str, float]) -> Optional[AnomalyEvent]:
        """Async receive telemetry."""
        if not self.is_running:
            logger.warning("TelemetryCollector not running; ignoring sample.")
            return None
        event = await self.detector.ingest(node_id, metrics)
        return event

# ============================================================================
# 9. STUB COMPONENTS FOR INTEGRATION (AlertEscalation, EvolutionaryEngine)
# ============================================================================
class AlertEscalationSystem:
    """Stub for alert escalation."""
    async def send_alert(self, event: AnomalyEvent):
        logger.info(f"AlertEscalationSystem: {event.description}")
        # In real implementation, send via email/Slack/PagerDuty

class EvolutionaryEngine:
    """Stub for evolutionary engine that prunes based on anomalies."""
    async def receive_anomaly_feedback(self, node_id: str, severity: float):
        logger.info(f"EvolutionaryEngine: node {node_id} severity {severity}")

# ============================================================================
# 10. CONVENIENCE FACTORY
# ============================================================================
def create_anomaly_detection_system(config: Optional[Union[Dict, 'AnomalyConfig']] = None) -> Dict[str, Any]:
    """
    Factory to create the entire anomaly detection pipeline with all integrations.
    """
    if config is None:
        if PYDANTIC_AVAILABLE:
            config = AnomalyConfig()
        else:
            config = ANOMALY_CONFIG.copy()

    detector = AnomalyDetector(config)
    telemetry_collector = TelemetryCollector(detector)
    alert_system = AlertEscalationSystem()
    evolutionary_engine = EvolutionaryEngine()

    # Wire callbacks
    detector.register_alert_callback(alert_system.send_alert)
    detector.register_evolutionary_engine_callback(evolutionary_engine.receive_anomaly_feedback)

    # Auto‑response callback (can be a separate module)
    def auto_response_callback(event: AnomalyEvent):
        logger.info(f"Auto‑response triggered: {event.auto_response_taken} on {event.node_id}")
        # Here you would call actual orchestration (e.g., reroute tasks, restart service)
    detector.register_auto_response_callback(auto_response_callback)

    return {
        "detector": detector,
        "telemetry_collector": telemetry_collector,
        "alert_system": alert_system,
        "evolutionary_engine": evolutionary_engine,
    }

# ============================================================================
# 11. REST API (FastAPI) – Optional
# ============================================================================
if FASTAPI_AVAILABLE:
    from fastapi import FastAPI, HTTPException, BackgroundTasks
    from fastapi.responses import Response

    app = FastAPI(title="Anomaly Detection API", version="2.1.0")
    detector: Optional[AnomalyDetector] = None
    REGISTRY = CollectorRegistry() if PROMETHEUS_AVAILABLE else None

    @app.get("/metrics")
    async def get_metrics():
        if PROMETHEUS_AVAILABLE and detector:
            return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
        return {"error": "Prometheus not enabled"}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/nodes/{node_id}/status")
    async def node_status(node_id: str):
        if not detector:
            raise HTTPException(503, "Detector not initialized")
        return detector.get_model_status(node_id)

    @app.get("/nodes/{node_id}/history")
    async def node_history(node_id: str, limit: int = 100):
        if not detector:
            raise HTTPException(503, "Detector not initialized")
        return detector.get_anomaly_history(node_id, limit)

    @app.post("/nodes/{node_id}/ingest")
    async def ingest_telemetry(node_id: str, metrics: Dict[str, float], background_tasks: BackgroundTasks):
        if not detector:
            raise HTTPException(503, "Detector not initialized")
        # We'll run async in background to avoid blocking
        background_tasks.add_task(detector.ingest, node_id, metrics)
        return {"status": "ingested"}

    @app.on_event("startup")
    async def startup():
        global detector
        if PYDANTIC_AVAILABLE:
            config = AnomalyConfig()
        else:
            config = ANOMALY_CONFIG.copy()
        detector = AnomalyDetector(config)
        logger.info("FastAPI startup complete")

    @app.on_event("shutdown")
    async def shutdown():
        if detector:
            await detector.shutdown()
        logger.info("FastAPI shutdown complete")

# ============================================================================
# 12. UNIT TEST STUBS (pytest)
# ============================================================================
def test_anomaly_detector():
    """Example test stub."""
    # Create a config with a simple threshold model
    if PYDANTIC_AVAILABLE:
        config = AnomalyConfig(model_type="threshold", window_size=10)
    else:
        config = ANOMALY_CONFIG.copy()
        config["model_type"] = "threshold"
        config["window_size"] = 10
    detector = AnomalyDetector(config)
    # Simulate data
    for i in range(20):
        metrics = {"energy_joules": 10 + np.random.normal(0, 1)}
        detector.ingest("node-001", metrics)
    # Inject anomaly
    event = detector.ingest("node-001", {"energy_joules": 100})
    assert event is not None
    assert event.node_id == "node-001"
    assert event.metric_name == "energy_joules"

# ============================================================================
# 13. EXAMPLE USAGE (if run directly)
# ============================================================================
if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)

    async def main():
        # Setup config with distillation enabled
        if PYDANTIC_AVAILABLE:
            config = AnomalyConfig(model_type="online_svm", window_size=20, persistence_enabled=False,
                                   distillation_epsilon=0.1, distillation_train_every=5)
        else:
            config = ANOMALY_CONFIG.copy()
            config["model_type"] = "online_svm"
            config["window_size"] = 20
            config["persistence_enabled"] = False
            config["distillation_epsilon"] = 0.1
            config["distillation_train_every"] = 5

        detector = AnomalyDetector(config)

        # Simulate normal data
        for i in range(50):
            metrics = {
                "energy_joules": 10 + np.random.normal(0, 1),
                "carbon_kg": 0.5 + np.random.normal(0, 0.1),
                "helium_usage": 0.02 + np.random.normal(0, 0.005),
                "latency_ms": 50 + np.random.normal(0, 5),
                "accuracy": 0.95 + np.random.normal(0, 0.02),
            }
            await detector.ingest("node-001", metrics)
            await asyncio.sleep(0.01)

        # Inject anomalies (multiple to trigger learning)
        for _ in range(5):
            anomaly_metrics = {
                "energy_joules": 100,
                "carbon_kg": 0.5,
                "helium_usage": 0.02,
                "latency_ms": 50,
                "accuracy": 0.95,
            }
            event = await detector.ingest("node-001", anomaly_metrics)
            if event:
                logger.info(f"Detected anomaly: {event.description}, action: {event.auto_response_taken}")
            await asyncio.sleep(0.1)

        logger.info(f"Distillation stats: {detector.get_distillation_stats()}")
        logger.info("Anomaly detection demo complete")

    asyncio.run(main())
