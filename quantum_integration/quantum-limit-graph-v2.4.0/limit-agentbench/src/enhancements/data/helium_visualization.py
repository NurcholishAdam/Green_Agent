# File: helium_visualization_v2_3_0.py
# Version: 2.3.0 (Adaptive Layout with Multi-Teacher Distillation)
"""
Interactive Helium Market Dashboard with Adaptive Layout Selection.

Uses Multi-Teacher On-Policy Distillation to choose the most relevant charts
based on current market conditions and user engagement.

ENHANCEMENTS OVER v2.2.0:
- Fixed missing imports (asyncio, dataclass, Tuple) that made the module
  unrunnable.
- Implemented chart methods and create_kpi_dashboard (v2.2.0 stubs).
- Replaced asyncio.run-in-sync-method with a background event loop; sync
  callers (Dash callbacks, CLI) submit work via run_coroutine_threadsafe.
- Adopted the enhanced distillation stack: RunningScaler shared by all
  agents, MoEGatingNetwork with the full softmax Jacobian + load balancing,
  epsilon decay with a floor, Q-teacher update during batch training, and
  dirty-flag Q-weight persistence.
- Replay buffer stores the full (n_teachers, n_actions) matrix, not the
  averaged vector.
- Fixed reward attribution: chart-level rewards reward the layout that
  emphasized the chart.  The state transition is now real (built from
  post-interaction state) and the state vector is stored in the log.
- user_preference_score is computed from interaction history, not hardcoded.
- Regime vocabulary normalized so both the synthetic generator and any
  historical CSV use the same labels.
- Layout selection is cached; static HTML and Dash server agree.
- train_historical_model is implemented (RandomForest with guards).
- np.random.default_rng replaces the global numpy RNG.
- UTC-aware timestamps.
- FeedbackEvent emitted on each layout decision when available.
- Interaction log schema fixed with state_vector persisted.
- Expanded test suite covering charts, KPIs, the reward loop, and the
  offline ML trainer.

All previous features (synthetic fallback, Dash server, CLI) retained.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import pickle
import random
import threading
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, ClassVar, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

try:
    import dash
    from dash import dcc, html, Input, Output, State
    DASH_AVAILABLE = True
except ImportError:
    DASH_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False
    RandomForestClassifier = None  # type: ignore
    LabelEncoder = None  # type: ignore

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore


__version__ = "2.3.0"
__all__ = [
    "__version__",
    "Config",
    "DashboardState",
    "RunningScaler",
    "MoEGatingNetwork",
    "DistillationStudent",
    "ReplayBuffer",
    "Teacher",
    "LayoutRuleBasedTeacher",
    "LayoutHistoricalMLTeacher",
    "LayoutStatefulQTeacher",
    "DistillationDashboardOptimizer",
    "HeliumMarketDashboard",
    "generate_synthetic_data",
    "train_historical_model",
]


# ============================================================================
# Utilities
# ============================================================================
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _softmax(logits: np.ndarray) -> np.ndarray:
    z = logits - np.max(logits)
    e = np.exp(z)
    s = e.sum()
    return e / s if s > 0 else np.ones_like(e) / len(e)


# ============================================================================
# Configuration
# ============================================================================
class Config:
    """Central configuration with environment variable support."""
    DATA_PATH = os.getenv("HELIUM_DASHBOARD_DATA", "./data/helium_realtime.csv")
    FORECAST_PATH = os.getenv(
        "HELIUM_DASHBOARD_FORECAST", "./data/helium_forecasts.csv"
    )
    OUTPUT_PATH = os.getenv("HELIUM_DASHBOARD_OUTPUT", "./helium_dashboard.html")
    LOG_LEVEL = os.getenv("HELIUM_DASHBOARD_LOG", "INFO")

    DISTILLATION_EPSILON = float(os.getenv("DASHBOARD_DISTILLATION_EPSILON", "0.1"))
    DISTILLATION_EPSILON_MIN = float(
        os.getenv("DASHBOARD_DISTILLATION_EPSILON_MIN", "0.01")
    )
    DISTILLATION_EPSILON_DECAY = float(
        os.getenv("DASHBOARD_DISTILLATION_EPSILON_DECAY", "0.995")
    )
    DISTILLATION_TRAIN_EVERY = int(
        os.getenv("DASHBOARD_DISTILLATION_TRAIN_EVERY", "10")
    )
    DISTILLATION_REPLAY_SIZE = int(
        os.getenv("DASHBOARD_DISTILLATION_REPLAY_SIZE", "2000")
    )
    DISTILLATION_LEARNING_RATE = float(
        os.getenv("DASHBOARD_DISTILLATION_LEARNING_RATE", "0.01")
    )
    DISTILL_WEIGHT = float(os.getenv("DASHBOARD_DISTILL_WEIGHT", "0.7"))
    RL_WEIGHT = float(os.getenv("DASHBOARD_RL_WEIGHT", "0.3"))
    GATING_LEARNING_RATE = float(os.getenv("DASHBOARD_GATING_LR", "0.005"))
    GATING_LOAD_BALANCE_WEIGHT = float(
        os.getenv("DASHBOARD_GATING_LB", "0.01")
    )

    Q_WEIGHTS_PATH = os.getenv(
        "DASHBOARD_Q_WEIGHTS", "./dashboard_q_weights.json"
    )
    INTERACTION_LOGS_PATH = os.getenv(
        "DASHBOARD_INTERACTION_LOGS", "./dashboard_interactions.csv"
    )
    HISTORICAL_MODEL_PATH = os.getenv(
        "DASHBOARD_HISTORICAL_MODEL", "./dashboard_historical_model.pkl"
    )

    # Reward weighting for chart interactions.
    TIME_SCORE_MAX_SECONDS = float(os.getenv("DASHBOARD_TIME_SCORE_MAX", "60.0"))


logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================================
# Background event loop
# ============================================================================
class _BackgroundLoop:
    """
    A single background thread running an asyncio event loop.

    Sync callers (Dash callbacks, CLI helpers) submit coroutines here via
    ``submit(coro)``, which blocks until completion.  This avoids the
    ``asyncio.run() inside running event loop`` crash that occurs when a
    synchronous wrapper is called from an async context.
    """

    def __init__(self) -> None:
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def _ensure_started(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is not None and self._loop.is_running():
                return self._loop
            loop = asyncio.new_event_loop()

            def runner() -> None:
                asyncio.set_event_loop(loop)
                loop.run_forever()

            t = threading.Thread(
                target=runner, name="helium-dashboard-loop", daemon=True
            )
            t.start()
            self._loop = loop
            self._thread = t
            return loop

    def submit(self, coro: Awaitable[Any]) -> Any:
        loop = self._ensure_started()
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        return fut.result()

    def shutdown(self) -> None:
        with self._lock:
            if self._loop is not None and self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
            self._loop = None
            self._thread = None


_background_loop = _BackgroundLoop()


# ============================================================================
# Synthetic data
# ============================================================================
def generate_synthetic_data(n_periods: int = 60,
                            start_date: str = "2020-01-01",
                            seed: int = 42) -> pd.DataFrame:
    """Generate synthetic helium data using a seeded local RNG."""
    logger.info("Generating synthetic data for demo purposes.")
    rng = np.random.default_rng(seed)
    dates = pd.date_range(start=start_date, periods=n_periods, freq="ME")
    t = np.arange(n_periods)

    production = np.clip(
        28000 - t * 40 + rng.normal(0, 300, n_periods), 20000, 35000
    )
    demand = np.clip(
        27000 + t * 80 + rng.normal(0, 400, n_periods), 25000, 45000
    )
    price = 100 * np.exp(np.cumsum(rng.normal(0.005, 0.1, n_periods)))
    seasonal = 1 + 0.1 * np.sin(2 * np.pi * t / 12)
    price = np.clip(price * seasonal, 50, 500)
    ratio = demand / production
    shortage = np.clip((ratio - 0.95) * 4, 0.05, 1.0)
    supply_risk = np.clip(
        0.2 + t * 0.002 + 0.1 * np.sin(2 * np.pi * t / 24)
        + rng.normal(0, 0.05, n_periods), 0.1, 0.9,
    )
    recycling = np.clip(
        0.10 + t * 0.003 + rng.normal(0, 0.01, n_periods), 0.05, 0.40
    )
    substitution = np.clip(
        0.08 + t * 0.004 + rng.normal(0, 0.01, n_periods), 0.05, 0.50
    )
    cooling = np.clip(
        0.85 + t * 0.005 + rng.normal(0, 0.02, n_periods), 0.7, 1.3
    )
    geo = np.clip(
        0.3 + 0.2 * np.sin(2 * np.pi * t / 36)
        + rng.normal(0, 0.05, n_periods), 0.1, 0.8,
    )
    logistics = np.clip(
        0.2 + t * 0.001 + rng.normal(0, 0.05, n_periods), 0.1, 0.7
    )
    new_capacity = np.maximum(
        500, 2000 + t * 100 + rng.normal(0, 200, n_periods)
    )

    scarcity = np.clip(shortage * 0.6 + supply_risk * 0.4, 0, 1)
    price_vol = np.clip(
        pd.Series(price).rolling(6).std().fillna(5).values, 1, 30
    )

    regimes = []
    for sc in scarcity:
        if sc > 0.7:
            regimes.append("critical")
        elif sc > 0.5:
            regimes.append("high")
        elif sc > 0.3:
            regimes.append("moderate")
        else:
            regimes.append("low")

    carbon = np.clip(
        300 + 200 * scarcity + rng.normal(0, 50, n_periods), 50, 800
    )
    renewable = np.clip(
        30 + 40 * (1 - scarcity) + rng.normal(0, 10, n_periods), 5, 95
    )
    circularity = (recycling + substitution) / 2
    thermal = cooling * scarcity
    future_supply = np.clip((new_capacity / production) * 100, 0, 50)
    capacity_util = production / (production + new_capacity)
    esg = np.clip(
        (recycling * 40 + (1 - supply_risk) * 30 + (1 - geo) * 30) * 100, 0, 100
    )
    regulatory = np.clip(geo * 0.5 + logistics * 0.5, 0, 1)

    return pd.DataFrame({
        "date": dates,
        "global_production_tonnes": np.round(production, 0),
        "global_demand_tonnes": np.round(demand, 0),
        "price_index": np.round(price, 1),
        "shortage_severity_0_1": np.round(shortage, 3),
        "supply_risk_score_0_1": np.round(supply_risk, 3),
        "recycling_rate_0_1": np.round(recycling, 3),
        "substitution_feasibility_0_1": np.round(substitution, 3),
        "cooling_load_sensitivity": np.round(cooling, 3),
        "geopolitical_risk_index": np.round(geo, 3),
        "logistics_disruption_index": np.round(logistics, 3),
        "new_production_capacity_tonnes": np.round(new_capacity, 0),
        "helium_scarcity_impact": np.round(scarcity, 3),
        "price_volatility": np.round(price_vol, 2),
        "market_regime": regimes,
        "carbon_intensity_associated": np.round(carbon, 0),
        "renewable_energy_pct": np.round(renewable, 1),
        "demand_supply_ratio": np.round(ratio, 3),
        "circularity_potential": np.round(circularity, 3),
        "thermal_impact_factor": np.round(thermal, 3),
        "future_supply_potential_pct": np.round(future_supply, 1),
        "capacity_utilization_rate": np.round(capacity_util, 3),
        "esg_score": np.round(esg, 1),
        "regulatory_risk_score": np.round(regulatory, 3),
    })


# ============================================================================
# State
# ============================================================================
@dataclass
class DashboardState:
    """State for the distillation agent."""
    scarcity_index: float
    price_volatility: float
    demand_supply_ratio: float
    recycling_rate: float
    geopolitical_risk: float
    regime_crisis: float
    regime_tightening: float
    regime_normal: float
    regime_stable: float
    scarcity_trend: float
    price_trend: float
    user_preference_score: float

    FEATURE_DIM: ClassVar[int] = 12

    @staticmethod
    def _c(v: float) -> float:
        return float(min(1.0, max(0.0, v)))

    def to_raw_vector(self) -> np.ndarray:
        return np.array([
            self.scarcity_index,
            self.price_volatility,
            self.demand_supply_ratio,
            self.recycling_rate,
            self.geopolitical_risk,
            self.regime_crisis,
            self.regime_tightening,
            self.regime_normal,
            self.regime_stable,
            self.scarcity_trend,
            self.price_trend,
            self.user_preference_score,
        ], dtype=np.float64)

    def to_feature_vector(self, scaler: Optional["RunningScaler"] = None) -> np.ndarray:
        raw = self.to_raw_vector()
        if scaler is not None:
            return scaler.transform(raw)
        c = self._c
        return np.array([
            c(self.scarcity_index),
            c(self.price_volatility / 30.0),
            c(self.demand_supply_ratio / 2.0),
            c(self.recycling_rate),
            c(self.geopolitical_risk),
            c(self.regime_crisis),
            c(self.regime_tightening),
            c(self.regime_normal),
            c(self.regime_stable),
            min(max(self.scarcity_trend / 0.1, -1.0), 1.0),
            min(max(self.price_trend / 50.0, -1.0), 1.0),
            c(self.user_preference_score),
        ], dtype=np.float32)


# ============================================================================
# Running scaler
# ============================================================================
class RunningScaler:
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


# ============================================================================
# MoE gating (full softmax Jacobian + load balancing)
# ============================================================================
class MoEGatingNetwork:
    def __init__(self, feature_dim: int = 12, n_experts: int = 3,
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
        return _softmax(x @ self.weights + self.bias)

    def update(self, state_vec, teacher_outputs, student_probs,
               current_gate_weights) -> None:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        t = np.asarray(teacher_outputs, dtype=np.float64)
        s = np.asarray(student_probs, dtype=np.float64)
        g = np.asarray(current_gate_weights, dtype=np.float64)

        error = (g @ t) - s
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


# ============================================================================
# Student + replay buffer
# ============================================================================
class DistillationStudent:
    def __init__(self, feature_dim: int = 12, n_classes: int = 5,
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
        return _softmax(state_vector @ self.weights + self.biases)

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


# ============================================================================
# Teachers
# ============================================================================
class Teacher(ABC):
    def __init__(self) -> None:
        self.scaler: Optional[RunningScaler] = None

    @abstractmethod
    def predict(self, state: DashboardState) -> np.ndarray: ...
    @abstractmethod
    def confidence(self, state: DashboardState) -> float: ...

    def _features(self, state: DashboardState) -> np.ndarray:
        return state.to_feature_vector(self.scaler)


class LayoutRuleBasedTeacher(Teacher):
    ACTION_SPACE: ClassVar[List[str]] = [
        "balanced", "scarcity_focus", "supply_focus",
        "risk_focus", "circularity_focus",
    ]

    def predict(self, state: DashboardState) -> np.ndarray:
        probs = np.ones(5, dtype=np.float64) * 0.1
        if state.scarcity_index > 0.7:
            probs[1] = 0.8
        elif state.demand_supply_ratio > 1.3 or state.demand_supply_ratio < 0.8:
            probs[2] = 0.7
        elif state.geopolitical_risk > 0.7:
            probs[3] = 0.7
        elif state.recycling_rate < 0.2:
            probs[4] = 0.6
        else:
            probs[0] = 0.6
        return _softmax(probs)

    def confidence(self, state: DashboardState) -> float:
        return 0.6 if state.scarcity_index > 0.7 else 0.4


class LayoutHistoricalMLTeacher(Teacher):
    EXPECTED_FEATURE_DIM: ClassVar[int] = 12

    def __init__(self, model_path: Optional[Path] = None) -> None:
        super().__init__()
        self.model = None
        self.label_encoder = None
        self.model_path = Path(model_path or Config.HISTORICAL_MODEL_PATH)
        if self.model_path.exists():
            try:
                with open(self.model_path, "rb") as f:
                    self.model, self.label_encoder = pickle.load(f)
                n_features = getattr(self.model, "n_features_in_", None)
                if n_features is not None and n_features != self.EXPECTED_FEATURE_DIM:
                    logger.warning(
                        f"Historical model expects {n_features} features, "
                        f"pipeline produces {self.EXPECTED_FEATURE_DIM}; disabling."
                    )
                    self.model = None
                    self.label_encoder = None
                else:
                    logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: DashboardState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(5) / 5
        x = self._features(state).reshape(1, -1)
        if not hasattr(self.model, "predict_proba"):
            return np.ones(5) / 5
        return self.model.predict_proba(x)[0]

    def confidence(self, state: DashboardState) -> float:
        return 0.7 if self.model is not None else 0.0


class LayoutStatefulQTeacher(Teacher):
    def __init__(self, weights_path: Optional[Path] = None, lr: float = 0.1,
                 feature_dim: int = 12) -> None:
        super().__init__()
        self.lr = float(lr)
        self.feature_dim = int(feature_dim)
        self.weights_path = Path(weights_path or Config.Q_WEIGHTS_PATH)
        self.weights = np.zeros((self.feature_dim, 5), dtype=np.float64)
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.weights_path.exists():
            return
        try:
            w = np.array(
                json.loads(self.weights_path.read_text()), dtype=np.float64
            )
            if w.shape == (self.feature_dim, 5):
                self.weights = w
                logger.info(f"Loaded Q-weights from {self.weights_path}")
            else:
                logger.warning(
                    f"Q-weights shape {w.shape}, expected "
                    f"({self.feature_dim},5); reinitializing."
                )
        except Exception as e:
            logger.error(f"Failed to load Q-weights: {e}")

    def save(self, force: bool = False) -> None:
        if not force and not self._dirty:
            return
        try:
            self.weights_path.parent.mkdir(parents=True, exist_ok=True)
            self.weights_path.write_text(json.dumps(self.weights.tolist()))
            self._dirty = False
        except OSError as e:
            logger.warning(f"Failed to save Q-weights: {e}")

    def predict(self, state: DashboardState) -> np.ndarray:
        x = self._features(state)
        return _softmax(x @ self.weights)

    def confidence(self, state: DashboardState) -> float:
        x = self._features(state)
        q = x @ self.weights
        return float(np.clip(0.3 + 0.35 * (np.max(q) - np.min(q)), 0.0, 1.0))

    def update_from_vector(self, state_vec: np.ndarray, action: int,
                           reward: float) -> None:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        if x.shape[0] != self.feature_dim or not 0 <= int(action) < 5:
            return
        q = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q) * x
        self._dirty = True

    def update(self, state: DashboardState, action: int, reward: float) -> None:
        self.update_from_vector(self._features(state), action, reward)


# ============================================================================
# Dashboard optimizer
# ============================================================================
class DistillationDashboardOptimizer:
    ACTION_SPACE: ClassVar[List[str]] = [
        "balanced", "scarcity_focus", "supply_focus",
        "risk_focus", "circularity_focus",
    ]
    N_ACTIONS: ClassVar[int] = 5

    def __init__(self, config: Dict[str, Any],
                 q_weights_path: Optional[Path] = None,
                 seed: Optional[int] = None) -> None:
        self.config = config
        self.feature_dim = DashboardState.FEATURE_DIM
        self.scaler = RunningScaler(self.feature_dim, clip=config.get("scaler_clip", 5.0))

        self.student = DistillationStudent(
            feature_dim=self.feature_dim,
            n_classes=self.N_ACTIONS,
            lr=config.get("distillation_learning_rate", 0.01),
            seed=seed,
        )

        self.teachers: List[Teacher] = [
            LayoutRuleBasedTeacher(),
            LayoutHistoricalMLTeacher(),
            LayoutStatefulQTeacher(
                weights_path=q_weights_path or Path(Config.Q_WEIGHTS_PATH),
            ),
        ]
        for t in self.teachers:
            t.scaler = self.scaler

        self.q_teacher: Optional[LayoutStatefulQTeacher] = next(
            (t for t in self.teachers if isinstance(t, LayoutStatefulQTeacher)), None
        )
        self.n_teachers = len(self.teachers)

        self.gating = MoEGatingNetwork(
            feature_dim=self.feature_dim,
            n_experts=self.n_teachers,
            lr=config.get("gating_learning_rate", 0.005),
            load_balance_weight=config.get("gating_load_balance_weight", 0.01),
            seed=seed,
        )

        self.replay_buffer = ReplayBuffer(
            max_size=config.get("distillation_replay_size", 2000)
        )
        self.epsilon = float(config.get("distillation_epsilon", 0.1))
        self.epsilon_min = float(config.get("distillation_epsilon_min", 0.01))
        self.epsilon_decay = float(config.get("distillation_epsilon_decay", 0.995))
        self.train_every = int(config.get("distillation_train_every", 10))
        self.counter = 0
        self.distill_weight = float(config.get("distill_weight", 0.7))
        self.rl_weight = float(config.get("rl_weight", 0.3))
        self.batch_update_size = int(config.get("batch_update_size", 8))

        self.last_gate_entropy: float = 0.0

    def _compute_teacher_probs(self, state: DashboardState):
        state_vec = state.to_feature_vector(self.scaler)
        outs = []
        for t in self.teachers:
            try:
                p = t.predict(state)
            except Exception as e:
                logger.warning(f"Teacher {type(t).__name__} failed: {e}")
                p = np.ones(self.N_ACTIONS) / self.N_ACTIONS
            p = np.asarray(p, dtype=np.float64).reshape(-1)
            if p.shape[0] != self.N_ACTIONS:
                if p.shape[0] < self.N_ACTIONS:
                    p = np.pad(p, (0, self.N_ACTIONS - p.shape[0]))
                else:
                    p = p[: self.N_ACTIONS]
            s = p.sum()
            p = p / s if s > 0 else np.ones(self.N_ACTIONS) / self.N_ACTIONS
            outs.append(p)
        outs = np.array(outs)
        gate_w = self.gating.forward(state_vec)
        combined = gate_w @ outs
        s = combined.sum()
        combined = combined / s if s > 0 else np.ones(self.N_ACTIONS) / self.N_ACTIONS
        return combined, gate_w, outs

    async def select_layout(self, state: DashboardState, exploration: bool = True):
        self.scaler.update(state.to_raw_vector())
        state_vec = state.to_feature_vector(self.scaler)
        combined, gate_w, teacher_outs = self._compute_teacher_probs(state)
        student_probs = self.student.predict_proba(state_vec)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, self.N_ACTIONS - 1)
        else:
            blended = 0.8 * student_probs + 0.2 * combined
            action_idx = int(np.argmax(blended))

        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

        g = np.clip(gate_w, 1e-12, 1.0)
        self.last_gate_entropy = float(-np.sum(g * np.log(g)))

        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_outs

    async def update(self, state_vec, action_idx, reward,
                     next_state_vec, teacher_outputs) -> None:
        self.replay_buffer.push(state_vec, action_idx, reward,
                                next_state_vec, teacher_outputs)
        self.counter += 1
        if self.counter % self.train_every != 0:
            return
        if len(self.replay_buffer) < self.batch_update_size:
            return

        states, actions, rewards, _, t_batch = self.replay_buffer.sample(
            self.batch_update_size
        )
        for i in range(len(states)):
            s = states[i]; t_outs = t_batch[i]
            a = int(actions[i]); r = float(rewards[i])
            gate_w = self.gating.forward(s)
            combined = gate_w @ t_outs
            tot = combined.sum()
            combined = combined / tot if tot > 0 else np.ones(self.N_ACTIONS) / self.N_ACTIONS
            self.student.update(s, combined, r, a,
                                distill_weight=self.distill_weight,
                                rl_weight=self.rl_weight)
            if self.q_teacher is not None:
                self.q_teacher.update_from_vector(s, a, r)
            student_out = self.student.predict_proba(s)
            self.gating.update(s, t_outs, student_out, gate_w)

    def flush(self) -> None:
        if self.q_teacher is not None:
            self.q_teacher.save(force=True)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "epsilon": self.epsilon,
            "n_teachers": self.n_teachers,
            "scaler_count": self.scaler.count,
            "gate_entropy": self.last_gate_entropy,
        }


# ============================================================================
# Regime normalization
# ============================================================================
_REGIME_MAP: Dict[str, str] = {
    "low": "low",
    "low scarcity": "low",
    "stable": "low",
    "moderate": "moderate",
    "moderate scarcity": "moderate",
    "normal": "moderate",
    "high": "high",
    "high scarcity": "high",
    "tightening": "high",
    "critical": "critical",
    "critical scarcity": "critical",
    "crisis": "critical",
}


def _normalize_regime(regime: Any) -> str:
    if regime is None:
        return "low"
    key = str(regime).strip().lower()
    return _REGIME_MAP.get(key, "low")


# ============================================================================
# Chart names
# ============================================================================
_CHART_NAMES: Tuple[str, ...] = (
    "supply_demand", "scarcity_price", "risk_radar",
    "forecast", "circularity",
)


# ============================================================================
# Main dashboard
# ============================================================================
class HeliumMarketDashboard:
    """Interactive dashboard for helium market visualization with adaptive layout."""

    CHART_ORDERS: ClassVar[Dict[str, List[str]]] = {
        "balanced": list(_CHART_NAMES),
        "scarcity_focus": ["forecast", "scarcity_price", "supply_demand",
                           "risk_radar", "circularity"],
        "supply_focus": ["supply_demand", "scarcity_price", "forecast",
                         "circularity", "risk_radar"],
        "risk_focus": ["risk_radar", "scarcity_price", "supply_demand",
                       "forecast", "circularity"],
        "circularity_focus": ["circularity", "supply_demand",
                              "scarcity_price", "risk_radar", "forecast"],
    }

    INTERACTION_LOG_COLUMNS: ClassVar[List[str]] = [
        "timestamp", "chart", "time_spent", "rating", "reward",
        "layout", "action_idx", "state_vector",
    ]

    def __init__(
        self,
        data_path: Optional[str] = None,
        forecast_path: Optional[str] = None,
        generate_synthetic_fallback: bool = True,
        seed: Optional[int] = None,
    ) -> None:
        self.data_path = data_path or Config.DATA_PATH
        self.forecast_path = forecast_path or Config.FORECAST_PATH
        self.generate_synthetic_fallback = generate_synthetic_fallback
        self.seed = int(seed if seed is not None else 42)
        self._rng = np.random.default_rng(self.seed)

        self.df: Optional[pd.DataFrame] = None
        self.forecasts: Optional[pd.DataFrame] = None
        self._load_data()
        self._load_forecast()
        self._calculate_metrics()

        self.layout_optimizer = DistillationDashboardOptimizer({
            "distillation_epsilon": Config.DISTILLATION_EPSILON,
            "distillation_epsilon_min": Config.DISTILLATION_EPSILON_MIN,
            "distillation_epsilon_decay": Config.DISTILLATION_EPSILON_DECAY,
            "distillation_train_every": Config.DISTILLATION_TRAIN_EVERY,
            "distillation_replay_size": Config.DISTILLATION_REPLAY_SIZE,
            "distillation_learning_rate": Config.DISTILLATION_LEARNING_RATE,
            "distill_weight": Config.DISTILL_WEIGHT,
            "rl_weight": Config.RL_WEIGHT,
            "gating_learning_rate": Config.GATING_LEARNING_RATE,
            "gating_load_balance_weight": Config.GATING_LOAD_BALANCE_WEIGHT,
        }, seed=self.seed)

        self.interaction_log: List[Dict[str, Any]] = []
        self._chart_totals: Dict[str, float] = {}
        self._layout_totals: Dict[str, float] = {}
        self._layout_counts: Dict[str, int] = {}

        self.last_layout: Optional[str] = None
        self.last_action_idx: Optional[int] = None
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_teacher_probs: Optional[np.ndarray] = None
        self._cached_layout: Optional[Tuple[str, int, np.ndarray, np.ndarray]] = None

        logger.info("HeliumMarketDashboard initialized (v%s)", __version__)

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------
    def _load_data(self) -> None:
        if os.path.exists(self.data_path):
            try:
                self.df = pd.read_csv(self.data_path, parse_dates=["date"])
                logger.info(f"Loaded data from {self.data_path}")
                return
            except Exception as e:
                logger.warning(f"Failed to load data: {e}")

        if self.generate_synthetic_fallback:
            self.df = generate_synthetic_data(seed=self.seed)
            logger.info("Using synthetic data.")
        else:
            raise FileNotFoundError(f"Data file not found: {self.data_path}")

    def _load_forecast(self) -> None:
        if os.path.exists(self.forecast_path):
            try:
                self.forecasts = pd.read_csv(self.forecast_path, parse_dates=["date"])
                logger.info(f"Loaded forecasts from {self.forecast_path}")
            except Exception as e:
                logger.warning(f"Failed to load forecasts: {e}")
                self.forecasts = None
        else:
            logger.info("No forecast file found; forecast chart will be omitted.")
            self.forecasts = None

    def _calculate_metrics(self) -> None:
        if self.df is None:
            return
        self.df["date"] = pd.to_datetime(self.df["date"])
        if "global_production_tonnes" in self.df.columns and \
           "global_demand_tonnes" in self.df.columns:
            self.df["deficit"] = (
                self.df["global_demand_tonnes"] - self.df["global_production_tonnes"]
            )
        if "price_index" in self.df.columns:
            self.df["price_change"] = self.df["price_index"].pct_change() * 100

        if "demand_supply_ratio" not in self.df.columns and \
           "global_production_tonnes" in self.df.columns:
            self.df["demand_supply_ratio"] = (
                self.df["global_demand_tonnes"] / self.df["global_production_tonnes"]
            )

        if "helium_scarcity_impact" not in self.df.columns:
            shortage = self.df.get("shortage_severity_0_1", 0.5)
            supply_risk = self.df.get("supply_risk_score_0_1", 0.5)
            self.df["helium_scarcity_impact"] = np.clip(
                shortage * 0.6 + supply_risk * 0.4, 0, 1
            )
        else:
            self.df["helium_scarcity_impact"] = \
                self.df["helium_scarcity_impact"].clip(0, 1)

        if "price_volatility" not in self.df.columns and \
           "price_index" in self.df.columns:
            self.df["price_volatility"] = (
                self.df["price_index"].rolling(6).std().fillna(5).clip(1, 30)
            )

        if "market_regime" in self.df.columns:
            self.df["market_regime"] = self.df["market_regime"].apply(_normalize_regime)
        else:
            conds = [
                self.df["helium_scarcity_impact"] < 0.3,
                (self.df["helium_scarcity_impact"] >= 0.3) &
                    (self.df["helium_scarcity_impact"] < 0.6),
                (self.df["helium_scarcity_impact"] >= 0.6) &
                    (self.df["helium_scarcity_impact"] < 0.8),
                self.df["helium_scarcity_impact"] >= 0.8,
            ]
            self.df["market_regime"] = np.select(
                conds, ["low", "moderate", "high", "critical"], default="low"
            )

        if "circularity_potential" not in self.df.columns:
            recycling = self.df.get("recycling_rate_0_1", 0.1)
            substitution = self.df.get("substitution_feasibility_0_1", 0.1)
            self.df["circularity_potential"] = (recycling + substitution) / 2

        for col, default in [
            ("esg_score", 50.0),
            ("renewable_energy_pct", 30.0),
            ("geopolitical_risk_index", 0.3),
            ("supply_risk_score_0_1", 0.3),
            ("recycling_rate_0_1", 0.1),
            ("capacity_utilization_rate", 0.9),
            ("future_supply_potential_pct", 5.0),
            ("regulatory_risk_score", 0.3),
        ]:
            if col not in self.df.columns:
                self.df[col] = default

        logger.info("Derived metrics calculated.")

    # ------------------------------------------------------------------
    # State building
    # ------------------------------------------------------------------
    def _user_preference_score(self) -> float:
        """Mean reward over recent interactions, clamped to [0, 1]."""
        if not self.interaction_log:
            return 0.5
        recent = self.interaction_log[-20:]
        rewards = [float(e.get("reward", 0.0)) for e in recent]
        if not rewards:
            return 0.5
        return float(np.clip(np.mean(rewards), 0.0, 1.0))

    def _build_dashboard_state(self) -> DashboardState:
        if self.df is None or len(self.df) == 0:
            raise RuntimeError("No data loaded; call _load_data first.")

        latest = self.df.iloc[-1]
        window = 3
        prev = self.df.iloc[-1 - window] if len(self.df) > window else self.df.iloc[0]
        span = max(1, window)

        scarcity = float(latest.get("helium_scarcity_impact", 0.5))
        volatility = float(latest.get("price_volatility", 5.0))
        demand_supply = float(latest.get("demand_supply_ratio", 1.0))
        recycling = float(latest.get("recycling_rate_0_1", 0.1))
        geo = float(latest.get("geopolitical_risk_index", 0.3))

        regime = _normalize_regime(latest.get("market_regime", "low"))
        crisis = 1.0 if regime == "critical" else 0.0
        tightening = 1.0 if regime == "high" else 0.0
        normal = 1.0 if regime == "moderate" else 0.0
        stable = 1.0 if regime == "low" else 0.0

        scarcity_trend = (
            scarcity - float(prev.get("helium_scarcity_impact", scarcity))
        ) / span
        price_trend = (
            float(latest.get("price_index", 100.0))
            - float(prev.get("price_index", 100.0))
        ) / span

        return DashboardState(
            scarcity_index=scarcity,
            price_volatility=volatility,
            demand_supply_ratio=demand_supply,
            recycling_rate=recycling,
            geopolitical_risk=geo,
            regime_crisis=crisis,
            regime_tightening=tightening,
            regime_normal=normal,
            regime_stable=stable,
            scarcity_trend=scarcity_trend,
            price_trend=price_trend,
            user_preference_score=self._user_preference_score(),
        )

    # ------------------------------------------------------------------
    # Layout selection
    # ------------------------------------------------------------------
    def _select_layout_sync(self, refresh: bool = False
                            ) -> Tuple[str, int, np.ndarray, np.ndarray]:
        if self._cached_layout is not None and not refresh:
            return self._cached_layout
        state = self._build_dashboard_state()
        result = _background_loop.submit(
            self.layout_optimizer.select_layout(state, exploration=True)
        )
        layout, action_idx, state_vec, teacher_probs = result
        self.last_layout = layout
        self.last_action_idx = action_idx
        self.last_state_vec = state_vec
        self.last_teacher_probs = teacher_probs
        self._cached_layout = result
        self._emit_feedback_event(layout, state)
        return result

    def refresh_layout(self) -> str:
        """Force a fresh layout selection."""
        layout, *_ = self._select_layout_sync(refresh=True)
        logger.info(f"Refreshed layout: {layout}")
        return layout

    def _emit_feedback_event(self, layout: str, state: DashboardState) -> None:
        if FeedbackEvent is None:
            return
        try:
            event = FeedbackEvent(
                source="helium_market_dashboard",
                feedback_type="routing",
                context={
                    "task_id": "dashboard_layout",
                    "teacher_ids": [type(t).__name__ for t in self.layout_optimizer.teachers],
                },
                action={
                    "selected_action": layout,
                    "selected_rank": 1,
                    "confidence_score": 0.5,
                },
                performance={
                    "quality_score": float(max(0.0, min(1.0, state.user_preference_score))),
                    "latency_ms": 0.0,
                    "energy_joules": 0.0,
                    "carbon_g": 0.0,
                    "helium_cost": 0.0,
                    "duration_ms": 0.0,
                },
                adaptive_cost_value=0.0,
                tags=["dashboard", "layout", layout],
                envelope={"trace_id": str(uuid.uuid4())},
            )
            logger.debug(f"FeedbackEvent emitted: {getattr(event, 'event_id', '?')}")
        except Exception as e:
            logger.warning(f"Failed to emit FeedbackEvent: {e}")

    # ------------------------------------------------------------------
    # Chart builders
    # ------------------------------------------------------------------
    def create_supply_demand_chart(self) -> go.Figure:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Scatter(x=self.df["date"], y=self.df["global_production_tonnes"],
                       name="Production", line=dict(color="#1f77b4")),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(x=self.df["date"], y=self.df["global_demand_tonnes"],
                       name="Demand", line=dict(color="#d62728")),
            secondary_y=False,
        )
        if "price_index" in self.df.columns:
            fig.add_trace(
                go.Scatter(x=self.df["date"], y=self.df["price_index"],
                           name="Price Index", line=dict(color="#2ca02c", dash="dot")),
                secondary_y=True,
            )
        fig.update_layout(
            title="Supply vs Demand",
            template="plotly_white",
            height=380,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(l=40, r=40, t=60, b=40),
        )
        fig.update_yaxes(title_text="Tonnes", secondary_y=False)
        fig.update_yaxes(title_text="Price Index", secondary_y=True)
        return fig

    def create_scarcity_price_heatmap(self) -> go.Figure:
        df = self.df.copy()
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
        pivot = df.pivot_table(
            index="month", columns="year", values="price_index", aggfunc="mean"
        )
        fig = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=[str(c) for c in pivot.columns],
            y=[str(i) for i in pivot.index],
            colorscale="RdYlGn_r",
            colorbar=dict(title="Price"),
        ))
        fig.update_layout(
            title="Scarcity-Price Heatmap (Price Index by Year/Month)",
            template="plotly_white",
            height=380,
            margin=dict(l=40, r=40, t=60, b=40),
        )
        fig.update_xaxes(title_text="Year")
        fig.update_yaxes(title_text="Month")
        return fig

    def create_risk_radar(self) -> go.Figure:
        latest = self.df.iloc[-1]
        categories = [
            "Supply Risk", "Geopolitical Risk", "Logistics Disruption",
            "Regulatory Risk", "Scarcity Impact",
        ]
        values = [
            float(latest.get("supply_risk_score_0_1", 0.3)),
            float(latest.get("geopolitical_risk_index", 0.3)),
            float(latest.get("logistics_disruption_index", 0.3)),
            float(latest.get("regulatory_risk_score", 0.3)),
            float(latest.get("helium_scarcity_impact", 0.5)),
        ]
        fig = go.Figure(data=go.Scatterpolar(
            r=values + [values[0]],
            theta=categories + [categories[0]],
            fill="toself",
            name="Current",
        ))
        fig.update_layout(
            title="Risk Radar",
            template="plotly_white",
            height=380,
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            margin=dict(l=40, r=40, t=60, b=40),
        )
        return fig

    def create_forecast_chart(self) -> go.Figure:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=self.df["date"], y=self.df["global_demand_tonnes"],
            name="Historical Demand", line=dict(color="#d62728"),
        ))
        if self.forecasts is not None and "date" in self.forecasts.columns:
            fig.add_trace(go.Scatter(
                x=self.forecasts["date"],
                y=self.forecasts.get("global_demand_tonnes",
                                     self.forecasts.iloc[:, 1]),
                name="Forecast Demand", line=dict(color="#ff7f0e", dash="dash"),
            ))
        fig.update_layout(
            title="Demand Forecast",
            template="plotly_white",
            height=380,
            margin=dict(l=40, r=40, t=60, b=40),
        )
        fig.update_yaxes(title_text="Tonnes")
        return fig

    def create_circularity_progress(self) -> go.Figure:
        latest = self.df.iloc[-1]
        recycling = float(latest.get("recycling_rate_0_1", 0.1)) * 100
        substitution = float(latest.get("substitution_feasibility_0_1", 0.1)) * 100
        circularity = float(latest.get("circularity_potential", 0.1)) * 100
        target = 50.0
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Current", x=["Recycling", "Substitution", "Circularity"],
                             y=[recycling, substitution, circularity],
                             marker_color="#2ca02c"))
        fig.add_trace(go.Bar(name="2030 Target", x=["Recycling", "Substitution", "Circularity"],
                             y=[target, target, target],
                             marker_color="#dddddd"))
        fig.update_layout(
            title="Circularity Progress (vs 2030 Target)",
            barmode="group",
            template="plotly_white",
            height=380,
            margin=dict(l=40, r=40, t=60, b=40),
        )
        fig.update_yaxes(title_text="Percent (%)")
        return fig

    def _chart_for(self, name: str) -> Optional[go.Figure]:
        if name == "supply_demand":
            return self.create_supply_demand_chart()
        if name == "scarcity_price":
            return self.create_scarcity_price_heatmap()
        if name == "risk_radar":
            return self.create_risk_radar()
        if name == "forecast":
            return self.create_forecast_chart() if self.forecasts is not None else None
        if name == "circularity":
            return self.create_circularity_progress()
        return None

    # ------------------------------------------------------------------
    # KPI
    # ------------------------------------------------------------------
    def create_kpi_dashboard(self) -> Dict[str, Dict[str, Any]]:
        latest = self.df.iloc[-1]
        prev = self.df.iloc[-2] if len(self.df) > 1 else latest

        def _pct_change(curr: float, prev_v: float) -> str:
            if prev_v == 0:
                return "—"
            change = (curr - prev_v) / abs(prev_v) * 100
            return f"{change:+.2f}%"

        price = float(latest.get("price_index", 0.0))
        price_prev = float(prev.get("price_index", price))
        prod = float(latest.get("global_production_tonnes", 0.0))
        prod_prev = float(prev.get("global_production_tonnes", prod))
        demand = float(latest.get("global_demand_tonnes", 0.0))
        demand_prev = float(prev.get("global_demand_tonnes", demand))
        scarcity = float(latest.get("helium_scarcity_impact", 0.0))
        esg = float(latest.get("esg_score", 0.0))

        return {
            "Price Index": {
                "value": f"{price:.1f}",
                "change": _pct_change(price, price_prev),
                "trend": "up" if price >= price_prev else "down",
                "color": "#d62728" if price >= price_prev else "#2ca02c",
            },
            "Production (t)": {
                "value": f"{prod:,.0f}",
                "change": _pct_change(prod, prod_prev),
                "trend": "up" if prod >= prod_prev else "down",
                "color": "#1f77b4" if prod >= prod_prev else "#ff7f0e",
            },
            "Demand (t)": {
                "value": f"{demand:,.0f}",
                "change": _pct_change(demand, demand_prev),
                "trend": "up" if demand >= demand_prev else "down",
                "color": "#8c564b" if demand >= demand_prev else "#2ca02c",
            },
            "Scarcity": {
                "value": f"{scarcity:.2f}",
                "change": f"{scarcity - float(prev.get('helium_scarcity_impact', scarcity)):+.2f}",
                "trend": "up" if scarcity > 0.7 else "down",
                "color": "#d62728" if scarcity > 0.7 else "#2ca02c",
            },
            "ESG Score": {
                "value": f"{esg:.1f}",
                "change": f"{esg - float(prev.get('esg_score', esg)):+.1f}",
                "trend": "up" if esg >= 50 else "down",
                "color": "#2ca02c" if esg >= 50 else "#d62728",
            },
        }

    # ------------------------------------------------------------------
    # HTML dashboard
    # ------------------------------------------------------------------
    def generate_html_dashboard(self, output_file: Optional[str] = None) -> str:
        output_file = output_file or Config.OUTPUT_PATH
        layout, _, _, _ = self._select_layout_sync()
        chart_order = self._get_chart_order(layout)
        kpis = self.create_kpi_dashboard()

        chart_html = ""
        for name in chart_order:
            fig = self._chart_for(name)
            if fig is None:
                continue
            chart_html += (
                '<div class="chart-container">'
                + pio.to_html(fig, full_html=False, config={"displayModeBar": False})
                + "</div>"
            )

        kpi_html = '<div class="kpi-container">'
        for label, kpi in kpis.items():
            color = kpi["color"]
            kpi_html += f"""
            <div class="kpi-card" style="border-left: 4px solid {color};">
                <h3>{label}</h3>
                <p class="kpi-value">{kpi['value']}</p>
                <p class="kpi-change" style="color: {color};">{kpi['change']}</p>
            </div>
            """
        kpi_html += "</div>"

        layout_indicator = (
            f'<div class="layout-indicator">Current layout: '
            f"<strong>{layout}</strong></div>"
        )

        latest = self.df.iloc[-1]
        scarcity = float(latest.get("helium_scarcity_impact", 0.0))
        deficit = float(latest.get("deficit", 0.0))
        recycling = float(latest.get("recycling_rate_0_1", 0.0))

        html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Helium Market Intelligence Dashboard</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                   color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; }}
        .kpi-container {{ display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 30px; }}
        .kpi-card {{ flex: 1; min-width: 200px; background: white; padding: 20px;
                     border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .kpi-value {{ font-size: 28px; font-weight: bold; margin: 10px 0; }}
        .kpi-change {{ font-size: 14px; }}
        .chart-container {{ background: white; padding: 20px; border-radius: 10px;
                            margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .layout-indicator {{ background: #e3f2fd; padding: 10px; border-radius: 5px;
                             margin-bottom: 20px; }}
        h1 {{ margin: 0; }}
        .subtitle {{ margin: 10px 0 0; opacity: 0.9; }}
        @media (max-width: 768px) {{ .kpi-container {{ flex-direction: column; }} }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Helium Market Intelligence Dashboard</h1>
            <p class="subtitle">Real-time market monitoring | Updated: {_utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        </div>
        {layout_indicator}
        {kpi_html}
        {chart_html}
        <div class="chart-container">
            <h3>Market Insights</h3>
            <ul>
                <li><strong>Scarcity index:</strong> {scarcity:.2f}
                    - {'Critical' if scarcity > 0.7 else 'Stable'}</li>
                <li><strong>Supply-Demand Gap:</strong> {deficit:+,.0f} tonnes
                    - {'Deficit' if deficit > 0 else 'Surplus'}</li>
                <li><strong>Recycling Progress:</strong> {recycling:.1%} (2030 target 50%)</li>
            </ul>
        </div>
    </div>
</body>
</html>"""

        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        logger.info(f"Dashboard generated: {output_file}")
        return output_file

    def _get_chart_order(self, layout: str) -> List[str]:
        return list(self.CHART_ORDERS.get(layout, list(_CHART_NAMES)))

    # ------------------------------------------------------------------
    # Interaction recording
    # ------------------------------------------------------------------
    _CHART_TO_LAYOUT: ClassVar[Dict[str, str]] = {
        "supply_demand": "supply_focus",
        "scarcity_price": "scarcity_focus",
        "risk_radar": "risk_focus",
        "forecast": "scarcity_focus",
        "circularity": "circularity_focus",
    }

    async def record_interaction_async(
        self, chart_name: str, time_spent: float,
        explicit_rating: Optional[float] = None,
    ) -> None:
        if chart_name not in _CHART_NAMES:
            raise ValueError(f"Unknown chart: {chart_name!r}")

        time_score = min(
            1.0, max(0.0, time_spent / Config.TIME_SCORE_MAX_SECONDS)
        )
        if explicit_rating is not None:
            reward = 0.6 * time_score + 0.4 * float(
                np.clip(explicit_rating, 0.0, 1.0)
            )
        else:
            reward = time_score

        # Attribute reward to the layout that emphasises this chart.
        attributed_layout = self._CHART_TO_LAYOUT.get(chart_name, "balanced")
        attributed_idx = DistillationDashboardOptimizer.ACTION_SPACE.index(attributed_layout)

        entry = {
            "timestamp": _utc_now().isoformat(),
            "chart": chart_name,
            "time_spent": float(time_spent),
            "rating": float(explicit_rating) if explicit_rating is not None else "",
            "reward": float(reward),
            "layout": attributed_layout,
            "action_idx": int(attributed_idx),
            "state_vector": json.dumps(
                (self.last_state_vec.tolist()
                 if self.last_state_vec is not None else [])
            ),
        }
        self.interaction_log.append(entry)
        self._chart_totals[chart_name] = self._chart_totals.get(chart_name, 0.0) + reward
        self._layout_totals[attributed_layout] = (
            self._layout_totals.get(attributed_layout, 0.0) + reward
        )
        self._layout_counts[attributed_layout] = (
            self._layout_counts.get(attributed_layout, 0) + 1
        )

        try:
            await asyncio.to_thread(self._append_interaction_log, entry)
        except Exception as e:
            logger.warning(f"Failed to append interaction log: {e}")

        # Update distillation agent with the attributed action.
        if self.last_state_vec is not None:
            next_state_vec = self._build_dashboard_state().to_feature_vector(
                self.layout_optimizer.scaler
            )
            await self.layout_optimizer.update(
                self.last_state_vec,
                attributed_idx,
                reward,
                next_state_vec,
                self.last_teacher_probs if self.last_teacher_probs is not None
                else np.ones((1, DistillationDashboardOptimizer.N_ACTIONS)) / 5,
            )
            # Invalidate the cached layout since the agent has learned.
            self._cached_layout = None

    def record_interaction(
        self, chart_name: str, time_spent: float,
        explicit_rating: Optional[float] = None,
    ) -> None:
        """Synchronous wrapper that submits to the background loop."""
        _background_loop.submit(self.record_interaction_async(
            chart_name, time_spent, explicit_rating
        ))

    def _append_interaction_log(self, entry: Dict[str, Any]) -> None:
        log_path = Path(Config.INTERACTION_LOGS_PATH)
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        exists = log_path.exists()
        row = {col: entry.get(col, "") for col in self.INTERACTION_LOG_COLUMNS}
        pd.DataFrame([row]).to_csv(log_path, mode="a", header=not exists, index=False)

    # ------------------------------------------------------------------
    # Offline ML training
    # ------------------------------------------------------------------
    @classmethod
    def train_historical_model(
        cls, log_path: Optional[Path] = None,
        model_path: Optional[Path] = None,
    ) -> Optional[Path]:
        if not SKLEARN_ML:
            logger.warning("scikit-learn not available; skipping training.")
            return None

        lp = Path(log_path or Config.INTERACTION_LOGS_PATH)
        mp = Path(model_path or Config.HISTORICAL_MODEL_PATH)
        if not lp.exists():
            logger.warning(f"Interaction log not found at {lp}.")
            return None

        try:
            df = pd.read_csv(lp)
        except Exception as e:
            logger.error(f"Failed to read interaction log: {e}")
            return None

        if "state_vector" not in df.columns or "layout" not in df.columns:
            logger.warning("Interaction log missing state_vector/layout columns.")
            return None

        X_list, y_list = [], []
        for _, row in df.iterrows():
            try:
                sv = json.loads(row["state_vector"])
                if not isinstance(sv, list) or len(sv) != DashboardState.FEATURE_DIM:
                    continue
                X_list.append(sv)
                y_list.append(str(row["layout"]))
            except Exception:
                continue

        if len(X_list) < 10:
            logger.warning("Too few valid samples to train historical model.")
            return None

        X = np.array(X_list, dtype=np.float64)
        y = np.array(y_list)
        if len(np.unique(y)) < 2:
            logger.warning("Only one class present; skipping model training.")
            return None

        le = LabelEncoder()
        y_enc = le.fit_transform(y)
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X, y_enc)

        mp.parent.mkdir(parents=True, exist_ok=True)
        with open(mp, "wb") as f:
            pickle.dump((clf, le), f)
        logger.info(f"Historical ML model saved to {mp}")
        return mp

    # ------------------------------------------------------------------
    # Dash server
    # ------------------------------------------------------------------
    def serve_dash(self, host: str = "127.0.0.1", port: int = 8050) -> None:
        if not DASH_AVAILABLE:
            raise ImportError("Dash not installed. Install with: pip install dash")

        layout, _, _, _ = self._select_layout_sync()
        chart_order = self._get_chart_order(layout)

        app = dash.Dash(__name__, title="Helium Dashboard")
        chart_components = []
        for name in chart_order:
            fig = self._chart_for(name)
            if fig is None:
                continue
            chart_components.append(
                dcc.Graph(id=f"chart-{name}", figure=fig,
                          config={"displayModeBar": False})
            )

        app.layout = html.Div([
            html.H1("Helium Market Dashboard", style={"textAlign": "center"}),
            html.Div(f"Current layout: {layout}", id="layout-indicator",
                     style={"padding": "10px", "background": "#e3f2fd"}),
            html.Div(id="kpi-display"),
            *chart_components,
            dcc.Interval(id="interval-component", interval=300000, n_intervals=0),
            html.Div(id="hidden-layout", style={"display": "none"}, children=layout),
        ])

        @app.callback(Output("kpi-display", "children"),
                      Input("interval-component", "n_intervals"))
        def _update_kpis(_):
            kpis = self.create_kpi_dashboard()
            divs = []
            for label, kpi in kpis.items():
                color = kpi["color"]
                divs.append(html.Div([
                    html.H3(label, style={"margin": "0"}),
                    html.P(kpi["value"], style={"fontSize": "24px",
                                                "fontWeight": "bold",
                                                "margin": "10px 0"}),
                    html.P(kpi["change"], style={"color": color}),
                ], style={"flex": 1, "padding": "10px",
                          "border": f"1px solid {color}",
                          "borderRadius": "5px"}))
            return html.Div(divs, style={"display": "flex", "gap": "10px",
                                         "flexWrap": "wrap"})

        # Chart click callbacks record a small reward for the clicked chart.
        for name in _CHART_NAMES:
            chart_id = f"chart-{name}"
            if not any(c.id == chart_id for c in chart_components):
                continue

            @app.callback(
                Output("hidden-layout", "children"),
                Input(chart_id, "clickData"),
                State("hidden-layout", "children"),
            )
            def _on_click(click_data, current_layout, _name=name):
                if click_data is None:
                    return current_layout
                # Attribute a modest reward for engaging with the chart.
                self.record_interaction(_name, time_spent=5.0)
                return current_layout

        logger.info(f"Starting Dash server at http://{host}:{port}")
        app.run(host=host, port=port, debug=False)

    # ------------------------------------------------------------------
    def export_chart(self, chart_name: str, output_file: str,
                     format: str = "html") -> Optional[str]:
        fig = self._chart_for(chart_name)
        if fig is None:
            logger.warning(f"Chart {chart_name} unavailable.")
            return None
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        if format == "html":
            fig.write_html(output_file)
        elif format == "png":
            try:
                fig.write_image(output_file)
            except Exception as e:
                logger.error(f"PNG export failed: {e}")
                return None
        else:
            logger.warning(f"Unsupported format: {format}")
            return None
        logger.info(f"Exported {chart_name} to {output_file}")
        return output_file

    def shutdown(self) -> None:
        self.layout_optimizer.flush()


# ============================================================================
# Module-level helper (kept for compatibility)
# ============================================================================
def train_historical_model(log_path: Optional[Path] = None,
                           model_path: Optional[Path] = None) -> Optional[Path]:
    return HeliumMarketDashboard.train_historical_model(log_path, model_path)


# ============================================================================
# CLI
# ============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Helium Market Dashboard")
    parser.add_argument("--data", default=Config.DATA_PATH)
    parser.add_argument("--forecast", default=Config.FORECAST_PATH)
    parser.add_argument("--output", default=Config.OUTPUT_PATH)
    parser.add_argument("--no-fallback", action="store_true")
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument(
        "--export-chart", choices=list(_CHART_NAMES), default=None,
    )
    parser.add_argument("--export-format", choices=["html", "png"], default="html")
    parser.add_argument("--export-output", default="chart.html")
    parser.add_argument("--train-historical", action="store_true")
    parser.add_argument("--test", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.test:
        _run_all_tests()
        return
    if args.train_historical:
        train_historical_model()
        return

    dashboard = HeliumMarketDashboard(
        data_path=args.data,
        forecast_path=args.forecast,
        generate_synthetic_fallback=not args.no_fallback,
    )

    try:
        if args.export_chart:
            path = dashboard.export_chart(
                args.export_chart, args.export_output, args.export_format
            )
            if path:
                print(f"Exported chart: {path}")
            return
        if args.serve:
            dashboard.serve_dash(port=args.port)
        else:
            path = dashboard.generate_html_dashboard(args.output)
            print(f"Dashboard generated: {path}. Open in browser.")
    finally:
        dashboard.shutdown()


# ============================================================================
# Tests
# ============================================================================
def _test_state_features() -> None:
    s = DashboardState(
        scarcity_index=0.5, price_volatility=10.0, demand_supply_ratio=1.2,
        recycling_rate=0.3, geopolitical_risk=0.4,
        regime_crisis=0.0, regime_tightening=0.0, regime_normal=1.0,
        regime_stable=0.0, scarcity_trend=0.02, price_trend=5.0,
        user_preference_score=0.5,
    )
    v = s.to_feature_vector()
    assert v.shape == (12,)
    assert np.all(v >= 0.0) and np.all(v <= 1.0)


def _test_rule_teacher() -> None:
    t = LayoutRuleBasedTeacher()
    s = DashboardState(
        scarcity_index=0.8, price_volatility=10.0, demand_supply_ratio=1.2,
        recycling_rate=0.3, geopolitical_risk=0.4,
        regime_crisis=0.0, regime_tightening=0.0, regime_normal=1.0,
        regime_stable=0.0, scarcity_trend=0.02, price_trend=5.0,
        user_preference_score=0.5,
    )
    p = t.predict(s)
    assert abs(p.sum() - 1.0) < 1e-9
    assert p[1] > 0.5


def _test_moe_jacobian_shapes() -> None:
    g = MoEGatingNetwork(feature_dim=12, n_experts=3)
    x = np.random.randn(12)
    t = np.random.rand(3, 5); t = t / t.sum(axis=1, keepdims=True)
    s = np.random.rand(5); s = s / s.sum()
    gw = g.forward(x)
    g.update(x, t, s, gw)
    assert g.weights.shape == (12, 3)


def _test_running_scaler() -> None:
    s = RunningScaler(12)
    for _ in range(5):
        s.update(np.random.randn(12) + 0.5)
    out = s.transform(np.random.randn(12))
    assert out.shape == (12,)
    assert np.all(np.isfinite(out))


def _test_q_teacher_persistence(tmpdir: Path) -> None:
    p = tmpdir / "q.json"
    q = LayoutStatefulQTeacher(weights_path=p, lr=1.0)
    v = np.zeros(12); v[0] = 1.0
    q.update_from_vector(v, 0, 1.0)
    q.save(force=True)
    assert p.exists()
    q2 = LayoutStatefulQTeacher(weights_path=p)
    assert np.any(q2.weights[:, 0] != 0)


def _test_epsilon_decay(tmpdir: Path) -> None:
    cfg = {
        "distillation_epsilon": 1.0,
        "distillation_epsilon_min": 0.01,
        "distillation_epsilon_decay": 0.5,
    }
    opt = DistillationDashboardOptimizer(
        cfg, q_weights_path=tmpdir / "q.json", seed=0,
    )
    s = DashboardState(
        scarcity_index=0.5, price_volatility=10.0, demand_supply_ratio=1.2,
        recycling_rate=0.3, geopolitical_risk=0.4,
        regime_crisis=0.0, regime_tightening=0.0, regime_normal=1.0,
        regime_stable=0.0, scarcity_trend=0.02, price_trend=5.0,
        user_preference_score=0.5,
    )
    _background_loop.submit(opt.select_layout(s, exploration=True))
    assert opt.epsilon < 1.0


def _test_chart_orders(tmpdir: Path) -> None:
    d = HeliumMarketDashboard(generate_synthetic_fallback=True, seed=0)
    order = d._get_chart_order("scarcity_focus")
    assert order[0] == "forecast"
    for layout in DistillationDashboardOptimizer.ACTION_SPACE:
        assert set(d._get_chart_order(layout)) == set(_CHART_NAMES)
    d.shutdown()


def _test_charts_and_kpi(tmpdir: Path) -> None:
    d = HeliumMarketDashboard(generate_synthetic_fallback=True, seed=0)
    for name in _CHART_NAMES:
        fig = d._chart_for(name)
        # forecast may be None if no forecast file loaded
        if name == "forecast" and d.forecasts is None:
            assert fig is None
        else:
            assert fig is not None
    kpis = d.create_kpi_dashboard()
    assert "Price Index" in kpis
    assert "Production (t)" in kpis
    d.shutdown()


def _test_html_generation(tmpdir: Path) -> None:
    out = tmpdir / "dashboard.html"
    d = HeliumMarketDashboard(generate_synthetic_fallback=True, seed=0)
    path = d.generate_html_dashboard(str(out))
    assert Path(path).exists()
    content = Path(path).read_text()
    assert "Helium Market Intelligence Dashboard" in content
    d.shutdown()


def _test_interaction_reward_attribution(tmpdir: Path) -> None:
    # Redirect logs to tmpdir via env.
    old = os.environ.get("DASHBOARD_INTERACTION_LOGS")
    os.environ["DASHBOARD_INTERACTION_LOGS"] = str(tmpdir / "interactions.csv")
    try:
        d = HeliumMarketDashboard(generate_synthetic_fallback=True, seed=0)
        d._select_layout_sync(refresh=True)
        _background_loop.submit(d.record_interaction_async(
            "risk_radar", time_spent=30.0, explicit_rating=0.9,
        ))
        assert d.interaction_log
        assert "risk_focus" in d._layout_totals
        # Cached layout invalidated after learning.
        assert d._cached_layout is None
        d.shutdown()
    finally:
        if old is None:
            os.environ.pop("DASHBOARD_INTERACTION_LOGS", None)
        else:
            os.environ["DASHBOARD_INTERACTION_LOGS"] = old


def _test_train_historical_model_guard(tmpdir: Path) -> None:
    log = tmpdir / "logs.csv"
    model = tmpdir / "model.pkl"
    # Single-class → should not train.
    rows = []
    for _ in range(15):
        rows.append({
            "state_vector": json.dumps([0.5] * 12),
            "layout": "balanced",
        })
    pd.DataFrame(rows).to_csv(log, index=False)
    result = HeliumMarketDashboard.train_historical_model(log_path=log, model_path=model)
    assert result is None


def _test_background_loop_reentrancy() -> None:
    async def coro():
        return 42
    assert _background_loop.submit(coro()) == 42
    # Submitting from inside an existing async context works too.
    async def outer():
        return _background_loop.submit(coro())
    assert asyncio.run(outer()) == 42


def _run_all_tests() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        _test_state_features()
        _test_rule_teacher()
        _test_moe_jacobian_shapes()
        _test_running_scaler()
        _test_q_teacher_persistence(tmp)
        _test_epsilon_decay(tmp)
        _test_chart_orders(tmp)
        _test_charts_and_kpi(tmp)
        _test_html_generation(tmp)
        _test_interaction_reward_attribution(tmp)
        _test_train_historical_model_guard(tmp)
        _test_background_loop_reentrancy()
    print("All HeliumMarketDashboard tests passed.")


if __name__ == "__main__":
    main()
