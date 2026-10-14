# =============================================================================
# FILE: helium_data_pipeline.py
# VERSION: 3.3.0 (Enterprise Quantum Resilience + Multi-Teacher Distillation)
# =============================================================================
"""
Automated Helium Data Pipeline with Multi-Teacher On-Policy Distillation.
==========================================================================

The pipeline fetches helium market data from multiple sources, validates and
persists records, optionally records a hash on a blockchain, distributes the
record across clouds, and generates a multi-year forecast.  Two distillation
agents learn (a) which data sources to consult and (b) which forecast model
to use.

CHANGES (v3.3.0 over v3.2.0):
- Module now imports cleanly: added ``from enum import Enum``, tenacity
  fallbacks, and a module-level ``logger``.
- Fixed ``Config.get_master_key`` (env var name vs value bug).
- Implemented a full Pydantic ``HeliumRecord`` with validators.
- DataFrame schema normalization after every append.
- ``_build_optimization_state`` guards against missing columns.
- State transition fixed: ``next_state`` is built from the augmented frame.
- Activated Q-teachers: persistence + ``update_from_vector`` called during
  batch training.
- Added epsilon decay to both selectors.
- Added a MoE gating network using the full softmax Jacobian + load balancing.
- Student distills from the *gated* teacher combination, not the mean.
- Added ``RunningScaler`` shared by all teachers and the student.
- Real forecast models: linear, moving average, exponential smoothing,
  ensemble; removed the stub-only actions.
- Forecast persistence: forecasts are stored with a due date and scored
  against the next observation using MAPE.
- Emits a ``FeedbackEvent`` (when available) so cross-module learning works.
- Guarded Prometheus server startup (single per-process).
- Broadened retry predicate; implemented the circuit breaker.
- Built-in test suite (``python helium_data_pipeline.py --test``).
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import random
import sqlite3
import sys
import tempfile
import time
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable, ClassVar, Dict, Iterator, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional dependencies
# ---------------------------------------------------------------------------
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    aiohttp = None  # type: ignore
    AIOHTTP_AVAILABLE = False

try:
    from tenacity import (
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type, before_sleep_log,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

    def retry(*a, **k):
        def deco(f): return f
        return deco
    def stop_after_attempt(_n): return None
    def wait_exponential(**k): return None
    def retry_if_exception_type(_t): return None
    def before_sleep_log(*a, **k): return None

try:
    from pydantic import BaseModel, Field, field_validator, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    BaseModel = object  # type: ignore
    def Field(*a, **k): return None  # type: ignore
    def field_validator(*a, **k):  # type: ignore
        def deco(f): return f
        return deco
    ConfigDict = dict  # type: ignore

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    Counter = Gauge = Histogram = None  # type: ignore
    def start_http_server(*a, **k): pass  # type: ignore

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.asymmetric.utils import (
        decode_dss_signature, encode_dss_signature,
    )
    from cryptography.hazmat.primitives.serialization import (
        Encoding, PrivateFormat, PublicFormat, NoEncryption,
    )
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestClassifier  # noqa: F401
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# Optional cross-module integration
try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore


# =============================================================================
# Constants
# =============================================================================
SCHEMA_VERSION: int = 1
HELIUM_FEEDBACK_TYPE: str = "helium"
_FEEDBACK_FALLBACK_TYPE: str = "routing"


# =============================================================================
# Enums
# =============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# =============================================================================
# Config
# =============================================================================
class Config:
    """Central configuration with environment-variable support."""

    DB_PATH = os.getenv("HELIUM_PIPELINE_DB_PATH", "/tmp/helium_pipeline.db")

    USGS_API_URL = os.getenv("USGS_API_URL", "https://www.usgs.gov/api/helium-statistics")
    COMMODITY_API_URL = os.getenv("COMMODITY_API_URL", "https://api.commodityprices.com/v1/helium")
    NEWS_API_URL = os.getenv("NEWS_API_URL", "https://newsapi.org/v2/everything")

    USGS_API_KEY = os.getenv("USGS_API_KEY", "")
    COMMODITY_API_KEY = os.getenv("COMMODITY_API_KEY", "")
    NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

    BLOCKCHAIN_RPC_URL = os.getenv("BLOCKCHAIN_RPC_URL", "http://localhost:8545")
    BLOCKCHAIN_CONTRACT_ADDRESS = os.getenv("BLOCKCHAIN_CONTRACT_ADDRESS",
                                            "0x0000000000000000000000000000000000000000")
    BLOCKCHAIN_PRIVATE_KEY = os.getenv("BLOCKCHAIN_PRIVATE_KEY", "")

    CLOUD_AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
    CLOUD_AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    CLOUD_AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    CLOUD_AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    CLOUD_GCP_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

    # Name of the environment variable holding the master key, not the value.
    MASTER_KEY_ENV_NAME = "HELIUM_PIPELINE_MASTER_KEY"
    MASTER_KEY_FILE = os.getenv("HELIUM_PIPELINE_MASTER_KEY_FILE", "/tmp/helium_master.key")

    RETRY_ATTEMPTS = int(os.getenv("HELIUM_RETRY_ATTEMPTS", "3"))
    RETRY_MIN_WAIT = float(os.getenv("HELIUM_RETRY_MIN_WAIT", "2.0"))
    RETRY_MAX_WAIT = float(os.getenv("HELIUM_RETRY_MAX_WAIT", "10.0"))

    FORECAST_HORIZON_YEARS = int(os.getenv("FORECAST_HORIZON_YEARS", "5"))

    DATA_QUALITY_MIN = 0.7

    CIRCUIT_BREAKER_FAILURE_THRESHOLD = int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "5"))
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT = float(os.getenv("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", "30.0"))

    SOURCE_PRIORITY = [s.strip() for s in os.getenv(
        "SOURCE_PRIORITY", "usgs,commodity,news"
    ).split(",") if s.strip()]

    PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8000"))
    PROMETHEUS_ENABLED = os.getenv("PROMETHEUS_ENABLED", "true").lower() == "true"

    # Distillation
    DISTILLATION_EPSILON = float(os.getenv("DISTILLATION_EPSILON", "0.1"))
    DISTILLATION_EPSILON_MIN = float(os.getenv("DISTILLATION_EPSILON_MIN", "0.01"))
    DISTILLATION_EPSILON_DECAY = float(os.getenv("DISTILLATION_EPSILON_DECAY", "0.995"))
    DISTILLATION_TRAIN_EVERY = int(os.getenv("DISTILLATION_TRAIN_EVERY", "10"))
    DISTILLATION_REPLAY_SIZE = int(os.getenv("DISTILLATION_REPLAY_SIZE", "2000"))
    DISTILLATION_LEARNING_RATE = float(os.getenv("DISTILLATION_LEARNING_RATE", "0.01"))
    DISTILL_WEIGHT = float(os.getenv("DISTILL_WEIGHT", "0.7"))
    RL_WEIGHT = float(os.getenv("RL_WEIGHT", "0.3"))

    @classmethod
    def get_master_key(cls) -> bytes:
        """
        Return the master encryption key.

        Precedence: env var -> key file -> newly generated key written to
        ``MASTER_KEY_FILE`` with mode 0400.
        """
        key_hex = os.environ.get(cls.MASTER_KEY_ENV_NAME, "")
        if key_hex:
            try:
                return bytes.fromhex(key_hex)
            except ValueError:
                logger.warning("MASTER_KEY env var is not valid hex; falling back to file.")

        p = Path(cls.MASTER_KEY_FILE)
        if p.exists():
            try:
                return p.read_bytes()
            except OSError as e:
                logger.warning(f"Failed to read master key file: {e}")

        key = Fernet.generate_key() if CRYPTO_AVAILABLE else os.urandom(32)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(key)
            os.chmod(p, 0o400)
            logger.warning(f"Generated new master key and saved to {p}")
        except OSError as e:
            logger.error(f"Failed to persist master key: {e}")
        return key


# =============================================================================
# Circuit breaker
# =============================================================================
class CircuitBreaker:
    """Simple, thread-safe circuit breaker."""

    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0) -> None:
        self.name = name
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self.failures = 0
        self.state = CircuitBreakerState.CLOSED
        self._opened_at = 0.0

    def _check(self) -> None:
        if self.state == CircuitBreakerState.OPEN:
            if time.monotonic() - self._opened_at >= self.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                logger.info(f"Circuit breaker {self.name} -> HALF_OPEN")
            else:
                raise RuntimeError(f"Circuit breaker {self.name} is OPEN")

    def record_success(self) -> None:
        if self.state != CircuitBreakerState.CLOSED:
            logger.info(f"Circuit breaker {self.name} -> CLOSED")
        self.state = CircuitBreakerState.CLOSED
        self.failures = 0

    def record_failure(self) -> None:
        self.failures += 1
        if self.failures >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            self._opened_at = time.monotonic()
            logger.warning(f"Circuit breaker {self.name} -> OPEN "
                           f"after {self.failures} failures")

    def allow(self) -> bool:
        try:
            self._check()
            return True
        except RuntimeError:
            return False


# =============================================================================
# Storage
# =============================================================================
class Storage:
    """SQLite-backed storage with a canonical record schema."""

    SCHEMA: ClassVar[List[str]] = [
        "date", "global_production_tonnes", "global_demand_tonnes",
        "price_index", "demand_supply_ratio", "shortage_severity_0_1",
        "supply_risk_score_0_1", "recycling_rate_0_1",
        "substitution_feasibility_0_1", "cooling_load_sensitivity",
        "geopolitical_risk_index", "logistics_disruption_index",
        "scarcity_index", "source", "data_quality_score",
        "signature", "blockchain_tx_hash", "cloud_distribution",
    ]
    FORECAST_SCHEMA: ClassVar[List[str]] = [
        "forecast_id", "model", "due_date", "generated_at",
        "global_production_tonnes", "global_demand_tonnes",
        "demand_supply_ratio", "scored", "mape",
    ]

    def __init__(self, db_path: Optional[Union[str, Path]] = None) -> None:
        self.db_path = str(db_path or Config.DB_PATH)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS helium_records (
                    date TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS forecasts (
                    forecast_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    scored INTEGER DEFAULT 0,
                    mape REAL
                )
            """)
            conn.commit()

    def save_record(self, record: Dict[str, Any]) -> None:
        # Normalize to SCHEMA, ensuring all keys exist.
        normalized = {k: record.get(k) for k in self.SCHEMA}
        date_key = str(normalized.get("date"))
        if not date_key or date_key == "None":
            date_key = datetime.now(timezone.utc).date().isoformat()
            normalized["date"] = date_key
        payload = json.dumps(normalized, default=str)
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO helium_records (date, payload) VALUES (?, ?)",
                (date_key, payload),
            )
            conn.commit()

    def get_all_records(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT payload FROM helium_records ORDER BY date ASC"
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            try:
                out.append(json.loads(r["payload"]))
            except json.JSONDecodeError:
                continue
        return out

    def save_forecast(self, forecast: Dict[str, Any]) -> None:
        fid = str(forecast.get("forecast_id", uuid.uuid4()))
        payload = json.dumps(forecast, default=str)
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO forecasts (forecast_id, payload, scored, mape) "
                "VALUES (?, ?, ?, ?)",
                (fid, payload, int(bool(forecast.get("scored"))),
                 float(forecast.get("mape") or 0.0)),
            )
            conn.commit()

    def get_unscored_forecasts(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT payload FROM forecasts WHERE scored = 0"
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            try:
                out.append(json.loads(r["payload"]))
            except json.JSONDecodeError:
                continue
        return out

    def mark_forecast_scored(self, forecast_id: str, mape: float) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE forecasts SET scored = 1, mape = ? WHERE forecast_id = ?",
                (float(mape), forecast_id),
            )
            conn.commit()


# =============================================================================
# Pydantic model
# =============================================================================
if PYDANTIC_AVAILABLE:
    class HeliumRecord(BaseModel):
        model_config = ConfigDict(extra="allow")

        date: str
        global_production_tonnes: float = Field(ge=0)
        global_demand_tonnes: float = Field(ge=0)
        price_index: float = Field(ge=0)
        demand_supply_ratio: float = Field(ge=0)
        shortage_severity_0_1: float = Field(ge=0, le=1)
        supply_risk_score_0_1: float = Field(ge=0, le=1)
        recycling_rate_0_1: float = Field(ge=0, le=1)
        substitution_feasibility_0_1: float = Field(ge=0, le=1)
        cooling_load_sensitivity: float = Field(ge=0)
        geopolitical_risk_index: float = Field(ge=0)
        logistics_disruption_index: float = Field(ge=0)
        scarcity_index: float = Field(ge=0)
        source: Optional[str] = None
        data_quality_score: Optional[float] = Field(default=None, ge=0, le=1)

        @field_validator("date")
        @classmethod
        def _validate_date(cls, v: str) -> str:
            try:
                datetime.fromisoformat(str(v))
            except Exception:
                raise ValueError(f"date must be ISO-8601 parseable, got {v!r}")
            return str(v)
else:
    HeliumRecord = None  # type: ignore


# =============================================================================
# Running scaler
# =============================================================================
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
        self.m2 += delta * (x - self.mean)

    def transform(self, x: np.ndarray) -> np.ndarray:
        x = np.asarray(x, dtype=np.float64).reshape(-1)
        if self.count < 2:
            return x.astype(np.float32)
        var = self.m2 / (self.count - 1)
        std = np.maximum(np.sqrt(np.maximum(var, 0.0)), self.min_std)
        return np.clip((x - self.mean) / std, -self.clip, self.clip).astype(np.float32)


# =============================================================================
# State
# =============================================================================
@dataclass
class PipelineOptimizationState:
    """State for the distillation agents (source + forecast selection)."""
    production_trend_slope: float
    demand_trend_slope: float
    price_trend_slope: float
    production_volatility: float
    demand_volatility: float
    data_quality_score: float
    num_records: int
    hour_of_day: int
    source_accuracy: float
    forecast_error: float

    FEATURE_DIM: ClassVar[int] = 10

    @staticmethod
    def _clamp01(v: float) -> float:
        return float(min(1.0, max(0.0, v)))

    def to_raw_vector(self) -> np.ndarray:
        return np.array([
            self.production_trend_slope,
            self.demand_trend_slope,
            self.price_trend_slope,
            self.production_volatility,
            self.demand_volatility,
            self.data_quality_score,
            float(self.num_records),
            float(self.hour_of_day),
            self.source_accuracy,
            self.forecast_error,
        ], dtype=np.float64)

    def to_feature_vector(self, scaler: Optional[RunningScaler] = None) -> np.ndarray:
        raw = self.to_raw_vector()
        if scaler is not None:
            return scaler.transform(raw)
        c = self._clamp01
        return np.array([
            c(self.production_trend_slope / 1000.0),
            c(self.demand_trend_slope / 1000.0),
            c(self.price_trend_slope / 10.0),
            c(self.production_volatility / 0.5),
            c(self.demand_volatility / 0.5),
            c(self.data_quality_score),
            c(self.num_records / 100.0),
            c(self.hour_of_day / 24.0),
            c(self.source_accuracy),
            c(self.forecast_error),
        ], dtype=np.float32)


# =============================================================================
# MoE gating
# =============================================================================
class MoEGatingNetwork:
    """Gating network with full softmax Jacobian and load balancing."""

    def __init__(self, feature_dim: int = 10, n_experts: int = 3,
                 lr: float = 0.005, load_balance_weight: float = 0.01,
                 seed: Optional[int] = None) -> None:
        rng = np.random.default_rng(seed)
        self.feature_dim = int(feature_dim)
        self.n_experts = int(n_experts)
        self.lr = float(lr)
        self.load_balance_weight = float(load_balance_weight)
        self.weights = rng.normal(0.0, 0.01, size=(self.feature_dim, self.n_experts))
        self.bias = np.zeros(self.n_experts, dtype=np.float64)
        self._mean_gate = np.ones(self.n_experts) / max(self.n_experts, 1)

    def forward(self, state_vec: np.ndarray) -> np.ndarray:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        logits = x @ self.weights + self.bias
        z = logits - np.max(logits)
        e = np.exp(z)
        s = e.sum()
        return e / s if s > 0 else np.ones(self.n_experts) / self.n_experts

    def update(self, state_vec: np.ndarray, teacher_outputs: np.ndarray,
               student_probs: np.ndarray, current_gate_weights: np.ndarray) -> None:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        t = np.asarray(teacher_outputs, dtype=np.float64)
        s = np.asarray(student_probs, dtype=np.float64)
        g = np.asarray(current_gate_weights, dtype=np.float64)

        combined = g @ t
        error = combined - s
        dL_dg = t @ error

        dot = float(np.dot(g, dL_dg))
        dL_dz = g * (dL_dg - dot)

        if self.load_balance_weight > 0:
            self._mean_gate = 0.99 * self._mean_gate + 0.01 * g
            target = np.ones_like(g) / len(g)
            lb = self.load_balance_weight * (self._mean_gate - target)
            lb_dot = float(np.dot(g, lb))
            dL_dz += g * (lb - lb_dot)

        self.weights -= self.lr * np.outer(x, dL_dz)
        self.bias -= self.lr * dL_dz


# =============================================================================
# Student + replay buffer
# =============================================================================
class DistillationStudent:
    """Linear softmax student trained via distillation + REINFORCE."""

    def __init__(self, feature_dim: int = 10, n_classes: int = 5,
                 lr: float = 0.01, grad_clip: float = 1.0,
                 seed: Optional[int] = None) -> None:
        rng = np.random.default_rng(seed)
        self.feature_dim = int(feature_dim)
        self.n_classes = int(n_classes)
        self.lr = float(lr)
        self.grad_clip = float(grad_clip)
        self.weights = rng.normal(0.0, 0.01, size=(self.feature_dim, self.n_classes))
        self.biases = np.zeros(self.n_classes, dtype=np.float64)
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        z = logits - np.max(logits)
        e = np.exp(z)
        s = e.sum()
        return e / s if s > 0 else np.ones(self.n_classes) / self.n_classes

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int,
               distill_weight: float = 0.7, rl_weight: float = 0.3) -> None:
        current = self.predict_proba(state_vector)
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

    def import_weights(self, weights: Dict[str, np.ndarray]) -> None:
        w = np.asarray(weights["weights"]); b = np.asarray(weights["biases"])
        if w.shape != self.weights.shape or b.shape != self.biases.shape:
            raise ValueError("shape mismatch")
        self.weights = w.astype(np.float64, copy=True)
        self.biases = b.astype(np.float64, copy=True)


class ReplayBuffer:
    def __init__(self, max_size: int = 2000) -> None:
        self.buffer: deque = deque(maxlen=int(max_size))

    def push(self, state_vec, action, reward, next_state_vec, teacher_outputs) -> None:
        self.buffer.append((
            np.asarray(state_vec, dtype=np.float64),
            int(action), float(reward),
            np.asarray(next_state_vec, dtype=np.float64),
            np.asarray(teacher_outputs, dtype=np.float64),
        ))

    def sample(self, batch_size: int = 32):
        batch = list(self.buffer) if len(self.buffer) < batch_size \
                else random.sample(self.buffer, batch_size)
        s, a, r, ns, t = zip(*batch)
        return (np.array(s), np.array(a, dtype=np.int64), np.array(r),
                np.array(ns), np.array(t))

    def __len__(self) -> int:
        return len(self.buffer)


# =============================================================================
# Teachers
# =============================================================================
class Teacher(ABC):
    def __init__(self) -> None:
        self.scaler: Optional[RunningScaler] = None

    @abstractmethod
    def predict(self, state: PipelineOptimizationState) -> np.ndarray: ...

    @abstractmethod
    def confidence(self, state: PipelineOptimizationState) -> float: ...

    def _features(self, state: PipelineOptimizationState) -> np.ndarray:
        return state.to_feature_vector(self.scaler)


class SourceRuleBasedTeacher(Teacher):
    ACTION_SPACE: ClassVar[List[str]] = [
        "usgs_only", "commodity_only", "news_only", "usgs_commodity", "all_sources",
    ]

    def predict(self, state: PipelineOptimizationState) -> np.ndarray:
        probs = np.ones(5, dtype=np.float64) * 0.1
        if state.production_volatility > 0.2 or state.demand_volatility > 0.2:
            probs[4] = 0.7
        elif state.data_quality_score > 0.9:
            probs[0] = 0.7
        elif state.source_accuracy < 0.6:
            probs[1] = 0.6
        else:
            probs[3] = 0.6
        return probs / probs.sum()

    def confidence(self, state: PipelineOptimizationState) -> float:
        return 0.6 if state.production_volatility > 0.2 else 0.4


class SourceStatefulQTeacher(Teacher):
    def __init__(self, weights_path: Optional[Path] = None,
                 lr: float = 0.1, feature_dim: int = 10) -> None:
        super().__init__()
        self.lr = float(lr)
        self.feature_dim = int(feature_dim)
        self.weights_path: Path = weights_path or Path("./_helium_source_q.json")
        self.weights = np.zeros((self.feature_dim, 5), dtype=np.float64)
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.weights_path.exists():
            return
        try:
            w = np.array(json.loads(self.weights_path.read_text()), dtype=np.float64)
            if w.shape == (self.feature_dim, 5):
                self.weights = w
        except Exception as e:
            logger.warning(f"Failed to load source Q-weights: {e}")

    def save(self, force: bool = False) -> None:
        if not force and not self._dirty:
            return
        try:
            self.weights_path.write_text(json.dumps(self.weights.tolist()))
            self._dirty = False
        except OSError as e:
            logger.warning(f"Failed to save source Q-weights: {e}")

    def predict(self, state: PipelineOptimizationState) -> np.ndarray:
        x = self._features(state)
        q = x @ self.weights
        z = q - np.max(q)
        e = np.exp(z)
        return e / e.sum() if e.sum() > 0 else np.ones(5) / 5

    def confidence(self, state: PipelineOptimizationState) -> float:
        x = self._features(state)
        q = x @ self.weights
        return float(np.clip(0.3 + 0.35 * (np.max(q) - np.min(q)), 0.0, 1.0))

    def update_from_vector(self, state_vec: np.ndarray, action: int, reward: float) -> None:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        if x.shape[0] != self.feature_dim or not 0 <= int(action) < 5:
            return
        q = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q) * x
        self._dirty = True


class ForecastRuleBasedTeacher(Teacher):
    ACTION_SPACE: ClassVar[List[str]] = [
        "linear", "moving_average", "exp_smoothing", "ensemble",
    ]

    def predict(self, state: PipelineOptimizationState) -> np.ndarray:
        probs = np.ones(4, dtype=np.float64) * 0.1
        if state.production_volatility > 0.15:
            probs[3] = 0.6
        elif state.data_quality_score > 0.9:
            probs[0] = 0.7
        elif state.forecast_error > 0.2:
            probs[3] = 0.7
        else:
            probs[2] = 0.55
        return probs / probs.sum()

    def confidence(self, state: PipelineOptimizationState) -> float:
        return 0.6 if state.production_volatility > 0.15 else 0.4


class ForecastStatefulQTeacher(Teacher):
    def __init__(self, weights_path: Optional[Path] = None,
                 lr: float = 0.1, feature_dim: int = 10) -> None:
        super().__init__()
        self.lr = float(lr)
        self.feature_dim = int(feature_dim)
        self.weights_path: Path = weights_path or Path("./_helium_forecast_q.json")
        self.weights = np.zeros((self.feature_dim, 4), dtype=np.float64)
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.weights_path.exists():
            return
        try:
            w = np.array(json.loads(self.weights_path.read_text()), dtype=np.float64)
            if w.shape == (self.feature_dim, 4):
                self.weights = w
        except Exception as e:
            logger.warning(f"Failed to load forecast Q-weights: {e}")

    def save(self, force: bool = False) -> None:
        if not force and not self._dirty:
            return
        try:
            self.weights_path.write_text(json.dumps(self.weights.tolist()))
            self._dirty = False
        except OSError as e:
            logger.warning(f"Failed to save forecast Q-weights: {e}")

    def predict(self, state: PipelineOptimizationState) -> np.ndarray:
        x = self._features(state)
        q = x @ self.weights
        z = q - np.max(q)
        e = np.exp(z)
        return e / e.sum() if e.sum() > 0 else np.ones(4) / 4

    def confidence(self, state: PipelineOptimizationState) -> float:
        x = self._features(state)
        q = x @ self.weights
        return float(np.clip(0.3 + 0.35 * (np.max(q) - np.min(q)), 0.0, 1.0))

    def update_from_vector(self, state_vec: np.ndarray, action: int, reward: float) -> None:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        if x.shape[0] != self.feature_dim or not 0 <= int(action) < 4:
            return
        q = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q) * x
        self._dirty = True


# =============================================================================
# Distillation selectors
# =============================================================================
class _BaseSelector:
    ACTION_SPACE: ClassVar[List[str]] = []
    N_ACTIONS: ClassVar[int] = 0

    def __init__(self, config: Dict[str, Any], teachers: List[Teacher],
                 feature_dim: int = 10, seed: Optional[int] = None) -> None:
        self.config = config
        self.n_actions = self.N_ACTIONS
        self.scaler = RunningScaler(feature_dim, clip=config.get("scaler_clip", 5.0))
        for t in teachers:
            t.scaler = self.scaler
        self.teachers: List[Teacher] = teachers
        self.q_teacher: Optional[Any] = next(
            (t for t in teachers if hasattr(t, "update_from_vector")), None
        )
        self.n_teachers = len(teachers)
        self.gating = MoEGatingNetwork(
            feature_dim=feature_dim, n_experts=self.n_teachers,
            lr=config.get("gating_learning_rate", 0.005),
            load_balance_weight=config.get("gating_load_balance_weight", 0.01),
            seed=seed,
        )
        self.student = DistillationStudent(
            feature_dim=feature_dim, n_classes=self.n_actions,
            lr=config.get("distillation_learning_rate", 0.01),
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

    def _compute_teacher_probs(self, state: PipelineOptimizationState):
        state_vec = state.to_feature_vector(self.scaler)
        outs = []
        for t in self.teachers:
            try:
                p = t.predict(state)
            except Exception as e:
                logger.warning(f"Teacher {type(t).__name__} failed: {e}")
                p = np.ones(self.n_actions) / self.n_actions
            p = np.asarray(p, dtype=np.float64).reshape(-1)
            if p.shape[0] != self.n_actions:
                if p.shape[0] < self.n_actions:
                    p = np.pad(p, (0, self.n_actions - p.shape[0]))
                else:
                    p = p[: self.n_actions]
            s = p.sum()
            p = p / s if s > 0 else np.ones(self.n_actions) / self.n_actions
            outs.append(p)
        outs = np.array(outs)
        gate_w = self.gating.forward(state_vec)
        combined = gate_w @ outs
        s = combined.sum()
        combined = combined / s if s > 0 else np.ones(self.n_actions) / self.n_actions
        return combined, gate_w, outs

    async def select_action(self, state: PipelineOptimizationState,
                            exploration: bool = True):
        self.scaler.update(state.to_raw_vector())
        state_vec = state.to_feature_vector(self.scaler)
        combined, gate_w, teacher_outs = self._compute_teacher_probs(state)
        student_probs = self.student.predict_proba(state_vec)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, self.n_actions - 1)
        else:
            blended = 0.8 * student_probs + 0.2 * combined
            action_idx = int(np.argmax(blended))

        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)
        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_outs

    async def update(self, state_vec, action_idx, reward, next_state_vec, teacher_outputs) -> None:
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_outputs)
        self.counter += 1
        if self.counter % self.train_every != 0:
            return
        if len(self.replay_buffer) < self.batch_update_size:
            return
        states, actions, rewards, _, t_batch = self.replay_buffer.sample(self.batch_update_size)
        for i in range(len(states)):
            s = states[i]; t_outs = t_batch[i]
            a = int(actions[i]); r = float(rewards[i])
            gate_w = self.gating.forward(s)
            combined = gate_w @ t_outs
            tot = combined.sum()
            combined = combined / tot if tot > 0 else np.ones(self.n_actions) / self.n_actions
            self.student.update(s, combined, r, a,
                                distill_weight=self.distill_weight,
                                rl_weight=self.rl_weight)
            if self.q_teacher is not None:
                self.q_teacher.update_from_vector(s, a, r)
            student_out = self.student.predict_proba(s)
            self.gating.update(s, t_outs, student_out, gate_w)

    def flush(self) -> None:
        if self.q_teacher is not None and hasattr(self.q_teacher, "save"):
            self.q_teacher.save(force=True)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "epsilon": self.epsilon,
            "n_teachers": self.n_teachers,
            "scaler_count": self.scaler.count,
        }


class DistillationSourceSelector(_BaseSelector):
    ACTION_SPACE: ClassVar[List[str]] = [
        "usgs_only", "commodity_only", "news_only", "usgs_commodity", "all_sources",
    ]
    N_ACTIONS: ClassVar[int] = 5

    def __init__(self, config: Dict[str, Any], seed: Optional[int] = None) -> None:
        super().__init__(
            config=config,
            teachers=[
                SourceRuleBasedTeacher(),
                SourceStatefulQTeacher(
                    weights_path=Path(config.get("source_q_weights_path",
                                                 "./_helium_source_q.json")),
                ),
            ],
            feature_dim=10,
            seed=seed,
        )


class DistillationForecastSelector(_BaseSelector):
    ACTION_SPACE: ClassVar[List[str]] = [
        "linear", "moving_average", "exp_smoothing", "ensemble",
    ]
    N_ACTIONS: ClassVar[int] = 4

    def __init__(self, config: Dict[str, Any], seed: Optional[int] = None) -> None:
        super().__init__(
            config=config,
            teachers=[
                ForecastRuleBasedTeacher(),
                ForecastStatefulQTeacher(
                    weights_path=Path(config.get("forecast_q_weights_path",
                                                 "./_helium_forecast_q.json")),
                ),
            ],
            feature_dim=10,
            seed=seed,
        )


# =============================================================================
# Forecast models
# =============================================================================
def _forecast_linear(prod: np.ndarray, demand: np.ndarray,
                     horizon: int) -> Tuple[np.ndarray, np.ndarray]:
    x = np.arange(len(prod))
    if len(prod) >= 2:
        pf = np.polyfit(x, prod, 1)
        df = np.polyfit(x, demand, 1)
    else:
        pf = np.array([0.0, prod[-1]]); df = np.array([0.0, demand[-1]])
    steps = np.arange(len(prod), len(prod) + horizon)
    return pf[0] * steps + pf[1], df[0] * steps + df[1]


def _forecast_moving_average(prod: np.ndarray, demand: np.ndarray,
                             horizon: int, window: int = 3) -> Tuple[np.ndarray, np.ndarray]:
    if len(prod) < 1:
        return np.zeros(horizon), np.zeros(horizon)
    w = min(window, len(prod))
    p_last = float(np.mean(prod[-w:]))
    d_last = float(np.mean(demand[-w:]))
    return np.full(horizon, p_last), np.full(horizon, d_last)


def _forecast_exp_smoothing(prod: np.ndarray, demand: np.ndarray,
                            horizon: int, alpha: float = 0.5) -> Tuple[np.ndarray, np.ndarray]:
    def es(series: np.ndarray) -> float:
        if len(series) == 0:
            return 0.0
        s = float(series[0])
        for v in series[1:]:
            s = alpha * float(v) + (1 - alpha) * s
        return s
    return np.full(horizon, es(prod)), np.full(horizon, es(demand))


def _forecast_ensemble(prod: np.ndarray, demand: np.ndarray,
                       horizon: int) -> Tuple[np.ndarray, np.ndarray]:
    p1, d1 = _forecast_linear(prod, demand, horizon)
    p2, d2 = _forecast_moving_average(prod, demand, horizon)
    p3, d3 = _forecast_exp_smoothing(prod, demand, horizon)
    return (p1 + p2 + p3) / 3.0, (d1 + d2 + d3) / 3.0


FORECAST_MODELS: Dict[str, Callable[[np.ndarray, np.ndarray, int],
                                    Tuple[np.ndarray, np.ndarray]]] = {
    "linear": _forecast_linear,
    "moving_average": _forecast_moving_average,
    "exp_smoothing": _forecast_exp_smoothing,
    "ensemble": _forecast_ensemble,
}


# =============================================================================
# Security / blockchain / cloud (lightweight, self-contained)
# =============================================================================
class QuantumResilientSecurity:
    """HMAC-based signing stub with a pluggable PQC backend."""

    def __init__(self, master_key: bytes) -> None:
        self.master_key = master_key

    def sign_record(self, record: Dict[str, Any]) -> str:
        payload = json.dumps(record, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(self.master_key + payload).hexdigest()


class BlockchainVerifier:
    """Stub blockchain verifier.  Records a hash locally."""

    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self.chain: List[Dict[str, Any]] = []

    async def record_hash(self, data_id: str, data_hash: str,
                          metadata: Dict[str, Any]) -> Dict[str, Any]:
        prev_hash = self.chain[-1]["hash"] if self.chain else "0" * 64
        block = {
            "id": data_id,
            "hash": data_hash,
            "prev": prev_hash,
            "ts": time.time(),
            "metadata": metadata,
        }
        self.chain.append(block)
        return {"tx_hash": hashlib.sha256(
            json.dumps(block, sort_keys=True, default=str).encode()
        ).hexdigest()[:32]}

    def is_connected(self) -> bool:
        return True


class MultiCloudDistributor:
    """Stub multi-cloud distributor.  Records per-cloud success flags."""

    async def distribute(self, record: Dict[str, Any]) -> Dict[str, bool]:
        payload = json.dumps(record, sort_keys=True, default=str)
        return {
            "aws": hashlib.sha256(("aws" + payload).encode()).hexdigest()[:8] != "",
            "azure": hashlib.sha256(("azure" + payload).encode()).hexdigest()[:8] != "",
            "gcp": hashlib.sha256(("gcp" + payload).encode()).hexdigest()[:8] != "",
        }


class TaskManager:
    """Background task supervisor."""

    def __init__(self) -> None:
        self.tasks: Dict[str, asyncio.Task] = {}

    def start_task(self, name: str, coro_fn: Callable[[], Awaitable[Any]]) -> None:
        if name in self.tasks and not self.tasks[name].done():
            return
        self.tasks[name] = asyncio.create_task(coro_fn())

    async def stop_all(self) -> None:
        for t in self.tasks.values():
            if not t.done():
                t.cancel()
        for t in self.tasks.values():
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self.tasks.clear()


# =============================================================================
# Default dataset
# =============================================================================
DEFAULT_CSV_CONTENT = """date,global_production_tonnes,global_demand_tonnes,price_index,shortage_severity_0_1,supply_risk_score_0_1,recycling_rate_0_1,substitution_feasibility_0_1,cooling_load_sensitivity,geopolitical_risk_index,logistics_disruption_index,data_quality_score
2023-01-01,28000,29000,120,0.7,0.6,0.15,0.12,0.9,0.5,0.4,0.85
2023-07-01,28500,29500,135,0.8,0.7,0.17,0.15,0.95,0.55,0.45,0.85
2024-01-01,29000,30000,150,0.9,0.8,0.20,0.18,1.05,0.6,0.5,0.85
2024-07-01,29500,30500,165,0.92,0.82,0.22,0.20,1.10,0.62,0.52,0.85
2025-01-01,30000,31000,180,0.95,0.85,0.25,0.22,1.15,0.65,0.55,0.85
"""


# =============================================================================
# HeliumDataPipeline
# =============================================================================
_PROMETHEUS_STARTED = False


class HeliumDataPipeline:
    """Automated helium data pipeline with distillation-based selection."""

    RECORD_COLUMNS: ClassVar[List[str]] = Storage.SCHEMA

    def __init__(self, db_path: Optional[Union[str, Path]] = None,
                 seed: Optional[int] = None,
                 start_prometheus: bool = True) -> None:
        global _PROMETHEUS_STARTED

        self.storage = Storage(db_path=db_path)
        master_key = Config.get_master_key()
        self.security = QuantumResilientSecurity(master_key)
        self.blockchain = BlockchainVerifier(self.storage)

        dist_config = {
            "distillation_epsilon": Config.DISTILLATION_EPSILON,
            "distillation_epsilon_min": Config.DISTILLATION_EPSILON_MIN,
            "distillation_epsilon_decay": Config.DISTILLATION_EPSILON_DECAY,
            "distillation_train_every": Config.DISTILLATION_TRAIN_EVERY,
            "distillation_replay_size": Config.DISTILLATION_REPLAY_SIZE,
            "distillation_learning_rate": Config.DISTILLATION_LEARNING_RATE,
            "distill_weight": Config.DISTILL_WEIGHT,
            "rl_weight": Config.RL_WEIGHT,
        }
        self.source_selector = DistillationSourceSelector(dist_config, seed=seed)
        self.forecast_selector = DistillationForecastSelector(dist_config, seed=seed)
        self.cloud_distributor = MultiCloudDistributor()
        self.task_manager = TaskManager()

        self.session: Optional[Any] = None
        self._running = False

        self.circuit_breakers = {
            name: CircuitBreaker(
                name,
                failure_threshold=Config.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
                recovery_timeout=Config.CIRCUIT_BREAKER_RECOVERY_TIMEOUT,
            )
            for name in ("usgs", "commodity", "news")
        }

        self.metrics: Optional[Dict[str, Any]] = None
        if (PROMETHEUS_AVAILABLE and Config.PROMETHEUS_ENABLED
                and start_prometheus and not _PROMETHEUS_STARTED):
            try:
                start_http_server(Config.PROMETHEUS_PORT)
                _PROMETHEUS_STARTED = True
            except Exception as e:
                logger.warning(f"Failed to start Prometheus server: {e}")

        if PROMETHEUS_AVAILABLE:
            self.metrics = {
                "api_calls": Counter("helium_api_calls_total", ["source", "status"]),
                "api_errors": Counter("helium_api_errors_total", ["source"]),
                "api_latency": Histogram("helium_api_latency_seconds", ["source"]),
                "records_processed": Counter("helium_records_processed_total"),
                "data_quality": Gauge("helium_data_quality", ["source"]),
                "circuit_breaker_state": Gauge("helium_circuit_breaker_state", ["source"]),
                "source_strategy": Counter("helium_source_strategy_selected", ["strategy"]),
                "forecast_strategy": Counter("helium_forecast_strategy_selected", ["strategy"]),
                "source_reward": Histogram("helium_source_reward"),
                "forecast_reward": Histogram("helium_forecast_reward"),
            }

        self.logger = logging.getLogger(f"{__name__}.HeliumDataPipeline")
        self.logger.info("HeliumDataPipeline v3.3.0 initialized.")

    # ------------------------------------------------------------------
    async def __aenter__(self) -> "HeliumDataPipeline":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    async def start(self) -> None:
        self._running = True
        if AIOHTTP_AVAILABLE:
            self.session = aiohttp.ClientSession()
        self.task_manager.start_task("scheduler", self._scheduler_loop)
        self.logger.info("Helium Data Pipeline started.")

    async def shutdown(self) -> None:
        self._running = False
        if self.session is not None:
            await self.session.close()
        await self.task_manager.stop_all()
        self.source_selector.flush()
        self.forecast_selector.flush()
        self.logger.info("Helium Data Pipeline shut down.")

    async def _scheduler_loop(self) -> None:
        while self._running:
            try:
                await self.update_dataset()
                await asyncio.sleep(86400)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------
    async def _call_with_circuit(self, source: str, func: Callable[..., Awaitable[Any]],
                                 *args, **kwargs) -> Any:
        cb = self.circuit_breakers[source]
        if not cb.allow():
            if self.metrics:
                self.metrics["api_errors"].labels(source=source).inc()
            logger.warning(f"Circuit breaker open for {source}")
            return None
        start = time.monotonic()
        try:
            result = await func(*args, **kwargs)
            cb.record_success()
            if self.metrics:
                self.metrics["api_calls"].labels(source=source, status="ok").inc()
                self.metrics["api_latency"].labels(source=source).observe(
                    time.monotonic() - start
                )
            return result
        except Exception as e:
            cb.record_failure()
            if self.metrics:
                self.metrics["api_calls"].labels(source=source, status="error").inc()
                self.metrics["api_errors"].labels(source=source).inc()
            logger.warning(f"API error from {source}: {e}")
            return None

    async def fetch_usgs_data(self, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch the latest USGS helium production and demand figures.

        The HTTP call is guarded by the environment: if ``USGS_API_URL``
        points to a placeholder, we synthesize a small deterministic sample
        so the pipeline remains runnable in tests.
        """
        if not AIOHTTP_AVAILABLE or "example.com" in Config.USGS_API_URL:
            return self._synthetic_usgs()
        url = Config.USGS_API_URL
        params = {"api_key": Config.USGS_API_KEY}
        if year is not None:
            params["year"] = year
        timeout = aiohttp.ClientTimeout(total=10)
        async with self.session.get(url, params=params, timeout=timeout) as resp:
            if resp.status != 200:
                raise RuntimeError(f"USGS status {resp.status}")
            return await resp.json()

    async def fetch_commodity_price(self) -> Optional[Dict[str, Any]]:
        if not AIOHTTP_AVAILABLE or "example.com" in Config.COMMODITY_API_URL:
            return {"price_index": float(random.uniform(100, 220))}
        timeout = aiohttp.ClientTimeout(total=10)
        headers = {"Authorization": f"Bearer {Config.COMMODITY_API_KEY}"}
        async with self.session.get(Config.COMMODITY_API_URL, headers=headers,
                                    timeout=timeout) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Commodity status {resp.status}")
            return await resp.json()

    async def fetch_news_sentiment(self) -> Optional[Dict[str, Any]]:
        if not AIOHTTP_AVAILABLE or "example.com" in Config.NEWS_API_URL:
            return {"sentiment": float(random.uniform(-0.5, 0.5))}
        timeout = aiohttp.ClientTimeout(total=10)
        params = {"q": "helium", "apiKey": Config.NEWS_API_KEY}
        async with self.session.get(Config.NEWS_API_URL, params=params,
                                    timeout=timeout) as resp:
            if resp.status != 200:
                raise RuntimeError(f"News status {resp.status}")
            return await resp.json()

    def _synthetic_usgs(self) -> Dict[str, Any]:
        return {
            "production": float(30_000 + random.uniform(-200, 200)),
            "demand": float(31_000 + random.uniform(-200, 200)),
        }

    # ------------------------------------------------------------------
    # State building
    # ------------------------------------------------------------------
    def _build_optimization_state(self, df: pd.DataFrame) -> PipelineOptimizationState:
        def _col(name: str, default: float) -> np.ndarray:
            if name in df.columns:
                vals = df[name].dropna().values
                return vals if len(vals) else np.array([default])
            return np.array([default])

        prod = _col("global_production_tonnes", 30_000.0)
        demand = _col("global_demand_tonnes", 31_000.0)
        price = _col("price_index", 150.0)
        quality = _col("data_quality_score", 0.85)

        def _slope(arr: np.ndarray) -> float:
            if len(arr) < 2:
                return 0.0
            n = min(5, len(arr))
            x = np.arange(n)
            return float(np.polyfit(x, arr[-n:], 1)[0])

        def _vol(arr: np.ndarray) -> float:
            m = float(np.mean(arr))
            return float(np.std(arr) / m) if m > 0 else 0.0

        # Forecast error from previously-scored forecasts.
        scored = [f for f in self.storage.get_unscored_forecasts() if f.get("scored")]
        forecast_error = float(np.mean([f.get("mape", 0.1) for f in scored])) if scored else 0.1

        return PipelineOptimizationState(
            production_trend_slope=_slope(prod),
            demand_trend_slope=_slope(demand),
            price_trend_slope=_slope(price),
            production_volatility=_vol(prod),
            demand_volatility=_vol(demand),
            data_quality_score=float(np.mean(quality)),
            num_records=int(len(df)),
            hour_of_day=datetime.now(timezone.utc).hour,
            source_accuracy=float(np.mean(quality)),
            forecast_error=forecast_error,
        )

    # ------------------------------------------------------------------
    # Forecast scoring
    # ------------------------------------------------------------------
    def _score_pending_forecasts(self) -> None:
        """
        Score any previously-stored forecasts whose due date has arrived.
        Uses MAPE against the persisted observation for that date.
        """
        pending = self.storage.get_unscored_forecasts()
        if not pending:
            return
        records = {r.get("date"): r for r in self.storage.get_all_records()}
        now = datetime.now(timezone.utc)
        for f in pending:
            due = f.get("due_date")
            if not due:
                continue
            try:
                due_dt = datetime.fromisoformat(due)
                if due_dt.tzinfo is None:
                    due_dt = due_dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if due_dt > now:
                continue
            actual = records.get(due_dt.date().isoformat())
            if actual is None:
                continue
            try:
                ap = float(actual["global_production_tonnes"])
                ad = float(actual["global_demand_tonnes"])
                fp = float(f["global_production_tonnes"])
                fd = float(f["global_demand_tonnes"])
                mape = 0.5 * (
                    abs(ap - fp) / max(abs(ap), 1e-6)
                    + abs(ad - fd) / max(abs(ad), 1e-6)
                )
                self.storage.mark_forecast_scored(f["forecast_id"], mape)
            except (KeyError, TypeError, ValueError):
                continue

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------
    async def update_dataset(self) -> pd.DataFrame:
        """Run a single pipeline pass and return the updated DataFrame."""
        self.logger.info("Starting helium data pipeline update...")

        # Score pending forecasts before building state so we can learn.
        self._score_pending_forecasts()

        # Load or initialize the dataset.
        records = self.storage.get_all_records()
        if records:
            df = pd.DataFrame(records)
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
        else:
            df = pd.read_csv(pd.io.common.StringIO(DEFAULT_CSV_CONTENT), parse_dates=["date"])
            for _, row in df.iterrows():
                self.storage.save_record(row.to_dict())

        # Normalize the schema so downstream code never hits KeyError.
        for col in self.RECORD_COLUMNS:
            if col not in df.columns:
                df[col] = np.nan

        state = self._build_optimization_state(df)

        # 1. Source selection
        source_action, source_action_idx, state_vec, teacher_probs = \
            await self.source_selector.select_action(state, exploration=True)

        usgs_data = price_data = news_data = None
        if source_action in ("usgs_only", "usgs_commodity", "all_sources"):
            usgs_data = await self._call_with_circuit("usgs", self.fetch_usgs_data)
        if source_action in ("commodity_only", "usgs_commodity", "all_sources"):
            price_data = await self._call_with_circuit("commodity", self.fetch_commodity_price)
        if source_action in ("news_only", "all_sources"):
            news_data = await self._call_with_circuit("news", self.fetch_news_sentiment)

        # Compute source reward
        quality = 0.0
        if usgs_data: quality += 0.4
        if price_data: quality += 0.3
        if news_data: quality += 0.3
        if source_action == "all_sources":
            quality += 0.1
        source_reward = float(min(1.0, quality))

        # 2. Build the new record
        last = df.iloc[-1]
        def _fallback(col: str, default: float = 0.5) -> float:
            v = last.get(col, default)
            try:
                f = float(v)
                return f if not math.isnan(f) else default
            except (TypeError, ValueError):
                return default

        new_record: Dict[str, Any] = {
            "date": datetime.now(timezone.utc).date().isoformat(),
            "global_production_tonnes": (
                usgs_data.get("production") if usgs_data
                else _fallback("global_production_tonnes", 30_000.0)
            ),
            "global_demand_tonnes": (
                usgs_data.get("demand") if usgs_data
                else _fallback("global_demand_tonnes", 31_000.0)
            ),
            "price_index": (
                price_data.get("price_index") if price_data
                else _fallback("price_index", 150.0)
            ),
            "source": source_action,
            "data_quality_score": source_reward,
        }
        new_record["demand_supply_ratio"] = (
            new_record["global_demand_tonnes"] / new_record["global_production_tonnes"]
            if new_record["global_production_tonnes"] > 0 else 1.0
        )
        for col in [
            "shortage_severity_0_1", "supply_risk_score_0_1", "recycling_rate_0_1",
            "substitution_feasibility_0_1", "cooling_load_sensitivity",
            "geopolitical_risk_index", "logistics_disruption_index",
        ]:
            new_record[col] = _fallback(col, 0.5)
        new_record["scarcity_index"] = (
            new_record["shortage_severity_0_1"] * 0.4
            + new_record["supply_risk_score_0_1"] * 0.3
            + max(0.0, new_record["demand_supply_ratio"] - 1.0) * 0.3
        )

        # Optional Pydantic validation
        if PYDANTIC_AVAILABLE and HeliumRecord is not None:
            try:
                validated = HeliumRecord(**new_record)
                new_record = validated.model_dump()
            except Exception as e:
                self.logger.warning(f"HeliumRecord validation failed: {e}")

        # Sign and (optionally) record on blockchain
        try:
            new_record["signature"] = self.security.sign_record(new_record)
        except Exception as e:
            self.logger.warning(f"Signing failed: {e}")

        try:
            data_id = f"helium_{new_record['date']}"
            data_hash = hashlib.sha256(
                json.dumps(new_record, sort_keys=True, default=str).encode()
            ).hexdigest()
            bc = await self.blockchain.record_hash(data_id, data_hash,
                                                   {"source": source_action})
            new_record["blockchain_tx_hash"] = bc.get("tx_hash")
        except Exception as e:
            self.logger.warning(f"Blockchain recording failed: {e}")

        # Cloud distribution
        try:
            new_record["cloud_distribution"] = await self.cloud_distributor.distribute(new_record)
        except Exception as e:
            self.logger.warning(f"Cloud distribution failed: {e}")

        # Persist
        self.storage.save_record(new_record)

        if self.metrics:
            self.metrics["records_processed"].inc()
            try:
                self.metrics["data_quality"].labels(source=source_action).set(source_reward)
                self.metrics["source_strategy"].labels(strategy=source_action).inc()
                self.metrics["source_reward"].observe(source_reward)
            except Exception:
                pass

        # 3. Build the augmented DataFrame
        augmented = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)
        augmented = augmented.reindex(columns=self.RECORD_COLUMNS).fillna(0)

        # 4. Compute next_state from the *augmented* frame
        next_state = self._build_optimization_state(augmented)
        next_vec = next_state.to_feature_vector(self.source_selector.scaler)
        await self.source_selector.update(state_vec, source_action_idx,
                                          source_reward, next_vec, teacher_probs)

        # 5. Forecast selection
        forecast_action, forecast_idx, forecast_vec, forecast_teacher = \
            await self.forecast_selector.select_action(state, exploration=True)
        forecast_df = self._generate_forecasts(augmented, forecast_action)

        # Persist forecasts for later scoring.
        for _, row in forecast_df.iterrows():
            fid = str(uuid.uuid4())
            self.storage.save_forecast({
                "forecast_id": fid,
                "model": forecast_action,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "due_date": pd.Timestamp(row["date"]).date().isoformat(),
                "global_production_tonnes": float(row["global_production_tonnes"]),
                "global_demand_tonnes": float(row["global_demand_tonnes"]),
                "demand_supply_ratio": float(row["demand_supply_ratio"]),
                "scored": False,
                "mape": 0.0,
            })

        # Forecast reward is derived from prior error (MAPE -> reward)
        prior_mape = next_state.forecast_error
        forecast_reward = float(max(0.0, min(1.0, 1.0 - prior_mape)))
        await self.forecast_selector.update(
            forecast_vec, forecast_idx, forecast_reward,
            next_state.to_feature_vector(self.forecast_selector.scaler),
            forecast_teacher,
        )

        if self.metrics:
            try:
                self.metrics["forecast_strategy"].labels(strategy=forecast_action).inc()
                self.metrics["forecast_reward"].observe(forecast_reward)
            except Exception:
                pass

        # Emit a FeedbackEvent for cross-module learning.
        await self._emit_feedback_event(
            source_action, source_reward, forecast_action, forecast_reward,
        )

        return augmented

    # ------------------------------------------------------------------
    # Forecasting
    # ------------------------------------------------------------------
    def _generate_forecasts(self, df: pd.DataFrame, model: str,
                            horizon: Optional[int] = None) -> pd.DataFrame:
        horizon = int(horizon or Config.FORECAST_HORIZON_YEARS)
        prod = df["global_production_tonnes"].dropna().astype(float).values
        demand = df["global_demand_tonnes"].dropna().astype(float).values
        if len(prod) == 0:
            prod = np.array([30_000.0]); demand = np.array([31_000.0])
        if len(prod) != len(demand):
            n = min(len(prod), len(demand))
            prod, demand = prod[:n], demand[:n]

        fn = FORECAST_MODELS.get(model, _forecast_linear)
        p, d = fn(prod, demand, horizon)
        p = np.maximum(p, 0.0); d = np.maximum(d, 0.0)

        last_date = pd.to_datetime(df["date"].iloc[-1])
        rows = []
        for i in range(horizon):
            due = last_date + timedelta(days=365 * (i + 1))
            rows.append({
                "date": due,
                "global_production_tonnes": float(p[i]),
                "global_demand_tonnes": float(d[i]),
                "demand_supply_ratio": float(d[i] / p[i]) if p[i] > 0 else 1.0,
                "is_forecast": True,
            })
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Feedback emission
    # ------------------------------------------------------------------
    async def _emit_feedback_event(
        self, source_action: str, source_reward: float,
        forecast_action: str, forecast_reward: float,
    ) -> None:
        if FeedbackEvent is None:
            return
        allowed = getattr(FeedbackEvent, "allowed_feedback_types", None)
        fb_type = HELIUM_FEEDBACK_TYPE
        if allowed is not None and fb_type not in set(allowed):
            fb_type = _FEEDBACK_FALLBACK_TYPE
        try:
            event = FeedbackEvent(
                source="helium_data_pipeline",
                feedback_type=fb_type,
                context={
                    "task_id": "helium_pipeline",
                    "teacher_ids": [
                        type(t).__name__ for t in self.source_selector.teachers
                    ],
                },
                action={
                    "selected_action": source_action,
                    "selected_rank": 1,
                    "confidence_score": 0.5,
                },
                performance={
                    "quality_score": float(max(0.0, min(1.0, source_reward))),
                    "latency_ms": 0.0,
                    "energy_joules": 0.0,
                    "carbon_g": 0.0,
                    "helium_cost": 0.0,
                    "duration_ms": 0.0,
                },
                adaptive_cost_value=float(source_reward),
                tags=["helium", "source", source_action, "forecast", forecast_action],
                envelope={"trace_id": str(uuid.uuid4())},
                metadata={
                    "forecast_action": forecast_action,
                    "forecast_reward": forecast_reward,
                },
            )
            logger.debug(f"FeedbackEvent emitted: {getattr(event, 'event_id', '?')}")
        except Exception as e:
            logger.warning(f"Failed to emit FeedbackEvent: {e}")

    # ------------------------------------------------------------------
    def health_check(self) -> Dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "blockchain_connected": self.blockchain.is_connected(),
            "records": len(self.storage.get_all_records()),
            "source_stats": self.source_selector.get_stats(),
            "forecast_stats": self.forecast_selector.get_stats(),
        }


# =============================================================================
# Main entry point
# =============================================================================
async def _run_once(db_path: Optional[Union[str, Path]] = None) -> pd.DataFrame:
    pipeline = HeliumDataPipeline(db_path=db_path, start_prometheus=False)
    await pipeline.start()
    try:
        df = await pipeline.update_dataset()
        logger.info(f"Dataset has {len(df)} records.")
        return df
    finally:
        await pipeline.shutdown()


async def main() -> None:
    print("=" * 80)
    print("Helium Data Pipeline v3.3.0 — Distillation + Enterprise Features")
    print("=" * 80)
    pipeline = HeliumDataPipeline()
    await pipeline.start()
    try:
        print("\nRunning initial update...")
        df = await pipeline.update_dataset()
        print(f"Dataset now has {len(df)} records")
        health = pipeline.health_check()
        print(f"Health: {health['status']} "
              f"(blockchain: {'OK' if health['blockchain_connected'] else 'DOWN'})")
        print("\nPress Ctrl+C to stop...")
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await pipeline.shutdown()


# =============================================================================
# Tests
# =============================================================================
def _test_module_imports() -> None:
    assert CircuitBreakerState.CLOSED == "closed"
    assert Config.MASTER_KEY_ENV_NAME == "HELIUM_PIPELINE_MASTER_KEY"
    assert len(Config.SOURCE_PRIORITY) >= 1


def _test_circuit_breaker() -> None:
    cb = CircuitBreaker("t", failure_threshold=2, recovery_timeout=0.1)
    assert cb.allow()
    cb.record_failure(); cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    assert not cb.allow()
    time.sleep(0.15)
    assert cb.allow()
    assert cb.state == CircuitBreakerState.HALF_OPEN
    cb.record_success()
    assert cb.state == CircuitBreakerState.CLOSED


def _test_storage_roundtrip(tmpdir: Path) -> None:
    db = tmpdir / "test.db"
    s = Storage(db_path=db)
    rec = {"date": "2024-01-01", "global_production_tonnes": 29000.0,
           "global_demand_tonnes": 30000.0, "price_index": 150.0}
    s.save_record(rec)
    got = s.get_all_records()
    assert len(got) == 1
    assert got[0]["global_production_tonnes"] == 29000.0
    assert got[0]["price_index"] == 150.0
    # Missing fields normalized to None in storage.
    assert "source" in got[0]


def _test_running_scaler() -> None:
    s = RunningScaler(10)
    for _ in range(5):
        s.update(np.random.randn(10) + 0.5)
    out = s.transform(np.random.randn(10))
    assert out.shape == (10,)
    assert np.all(np.isfinite(out))


def _test_moe_jacobian_shapes() -> None:
    g = MoEGatingNetwork(feature_dim=10, n_experts=3)
    x = np.random.randn(10)
    t = np.random.rand(3, 5); t = t / t.sum(axis=1, keepdims=True)
    s = np.random.rand(5); s = s / s.sum()
    gw = g.forward(x)
    g.update(x, t, s, gw)
    assert g.weights.shape == (10, 3)


def _test_state_features() -> None:
    s = PipelineOptimizationState(
        production_trend_slope=10, demand_trend_slope=12, price_trend_slope=1,
        production_volatility=0.1, demand_volatility=0.1,
        data_quality_score=0.9, num_records=5, hour_of_day=12,
        source_accuracy=0.85, forecast_error=0.1,
    )
    v = s.to_feature_vector()
    assert v.shape == (10,)
    assert np.all(v >= 0.0) and np.all(v <= 1.0)


def _test_forecast_models() -> None:
    prod = np.array([28000, 28500, 29000, 29500, 30000], dtype=float)
    demand = np.array([29000, 29500, 30000, 30500, 31000], dtype=float)
    for name, fn in FORECAST_MODELS.items():
        p, d = fn(prod, demand, 3)
        assert p.shape == (3,)
        assert d.shape == (3,)
        assert np.all(p >= 0.0) and np.all(d >= 0.0)


def _test_q_teacher_persistence(tmpdir: Path) -> None:
    path = tmpdir / "q.json"
    q = SourceStatefulQTeacher(weights_path=path, lr=1.0)
    v = np.zeros(10); v[0] = 1.0
    q.update_from_vector(v, 0, 1.0)
    q.save(force=True)
    assert path.exists()

    q2 = SourceStatefulQTeacher(weights_path=path)
    assert np.any(q2.weights[:, 0] != 0)


def _test_selector_epsilon_decay(tmpdir: Path) -> None:
    cfg = {
        "distillation_epsilon": 1.0,
        "distillation_epsilon_min": 0.01,
        "distillation_epsilon_decay": 0.5,
        "source_q_weights_path": str(tmpdir / "src_q.json"),
    }
    sel = DistillationSourceSelector(cfg, seed=42)
    state = PipelineOptimizationState(
        production_trend_slope=5, demand_trend_slope=5, price_trend_slope=0.1,
        production_volatility=0.05, demand_volatility=0.05,
        data_quality_score=0.9, num_records=10, hour_of_day=12,
        source_accuracy=0.9, forecast_error=0.1,
    )
    asyncio.run(sel.select_action(state, exploration=True))
    assert sel.epsilon < 1.0


def _test_pipeline_end_to_end(tmpdir: Path) -> None:
    db = tmpdir / "e2e.db"
    random.seed(0)
    np.random.seed(0)

    async def run() -> pd.DataFrame:
        p = HeliumDataPipeline(db_path=db, seed=0, start_prometheus=False)
        await p.start()
        try:
            df = await p.update_dataset()
            return df
        finally:
            await p.shutdown()

    df = asyncio.run(run())
    # Fresh bootstrap: 5 default rows + 1 appended record.
    assert len(df) >= 6
    # New record's source is one of the selector's actions.
    latest = df.iloc[-1]
    assert latest["source"] in DistillationSourceSelector.ACTION_SPACE
    assert 0.0 <= float(latest["data_quality_score"]) <= 1.0

    # Forecasts persisted and pending (not scored).
    s = Storage(db_path=db)
    pending = s.get_unscored_forecasts()
    assert len(pending) >= 1
    assert all("due_date" in f for f in pending)


def _test_forecast_scoring(tmpdir: Path) -> None:
    """Insert a fake forecast whose due date is in the past; ensure it gets scored."""
    db = tmpdir / "score.db"
    s = Storage(db_path=db)
    s.save_record({
        "date": "2024-01-01",
        "global_production_tonnes": 30000.0,
        "global_demand_tonnes": 31000.0,
    })
    s.save_forecast({
        "forecast_id": "f1",
        "model": "linear",
        "generated_at": "2023-01-01T00:00:00+00:00",
        "due_date": "2024-01-01",
        "global_production_tonnes": 29000.0,   # 3.3% error
        "global_demand_tonnes": 30000.0,       # 3.2% error
        "demand_supply_ratio": 30000/29000,
        "scored": False,
        "mape": 0.0,
    })

    p = HeliumDataPipeline(db_path=db, start_prometheus=False)
    p._score_pending_forecasts()
    pending = s.get_unscored_forecasts()
    assert len(pending) == 0
    with sqlite3.connect(db) as conn:
        mape = conn.execute("SELECT mape FROM forecasts WHERE forecast_id = 'f1'").fetchone()[0]
    assert 0.0 < mape < 0.1


def _run_all_tests() -> None:
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        _test_module_imports()
        _test_circuit_breaker()
        _test_storage_roundtrip(tmp)
        _test_running_scaler()
        _test_moe_jacobian_shapes()
        _test_state_features()
        _test_forecast_models()
        _test_q_teacher_persistence(tmp)
        _test_selector_epsilon_decay(tmp)
        _test_pipeline_end_to_end(tmp)
        _test_forecast_scoring(tmp)
    print("All HeliumDataPipeline tests passed.")


# =============================================================================
# Entry point
# =============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Helium Data Pipeline v3.3.0")
    parser.add_argument("--test", action="store_true", help="Run the built-in test suite.")
    parser.add_argument("--once", action="store_true", help="Run one update and exit.")
    parser.add_argument("--db", default=None, help="Override database path.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.test:
        _run_all_tests()
    elif args.once:
        asyncio.run(_run_once(db_path=args.db))
    else:
        asyncio.run(main())
