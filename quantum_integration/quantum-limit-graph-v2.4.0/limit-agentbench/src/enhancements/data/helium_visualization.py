# File: helium_visualization_v2_2_0.py
# Version: 2.2.0 (Adaptive Layout with Multi‑Teacher Distillation)
"""
Interactive Helium Market Dashboard with Adaptive Layout Selection.

Uses Multi‑Teacher On‑Policy Distillation to choose the most relevant charts
based on current market conditions and user engagement.

ENHANCEMENTS OVER v2.1.0:
- Adaptive layout selection via distillation.
- State‑aware prioritization of charts (scarcity, supply, risk, circularity).
- Online learning from user interactions (time spent, explicit feedback).
- Teachers: rule‑based, historical ML, stateful Q.
- Student: linear softmax with distillation + REINFORCE.
- Persistence for Q‑teacher weights.
- Unit tests for distillation components.
"""

import os
import sys
import argparse
import logging
import warnings
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Union, Any
from collections import deque
from abc import ABC, abstractmethod
import json
import random
import numpy as np
import pickle

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.io as pio

# For optional web server
try:
    import dash
    from dash import dcc, html, Input, Output
    DASH_AVAILABLE = True
except ImportError:
    DASH_AVAILABLE = False

# scikit-learn for ML teacher
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

warnings.filterwarnings('ignore')

# ============================================================================
# Configuration
# ============================================================================
class Config:
    """Central configuration with environment variable support."""
    DATA_PATH = os.getenv('HELIUM_DASHBOARD_DATA', './data/helium_realtime.csv')
    FORECAST_PATH = os.getenv('HELIUM_DASHBOARD_FORECAST', './data/helium_forecasts.csv')
    OUTPUT_PATH = os.getenv('HELIUM_DASHBOARD_OUTPUT', './helium_dashboard.html')
    LOG_LEVEL = os.getenv('HELIUM_DASHBOARD_LOG', 'INFO')

    # Distillation parameters
    DISTILLATION_EPSILON = float(os.getenv('DASHBOARD_DISTILLATION_EPSILON', '0.1'))
    DISTILLATION_TRAIN_EVERY = int(os.getenv('DASHBOARD_DISTILLATION_TRAIN_EVERY', '10'))
    DISTILLATION_REPLAY_SIZE = int(os.getenv('DASHBOARD_DISTILLATION_REPLAY_SIZE', '2000'))
    DISTILLATION_LEARNING_RATE = float(os.getenv('DASHBOARD_DISTILLATION_LEARNING_RATE', '0.01'))
    DISTILL_WEIGHT = float(os.getenv('DASHBOARD_DISTILL_WEIGHT', '0.7'))
    RL_WEIGHT = float(os.getenv('DASHBOARD_RL_WEIGHT', '0.3'))

    # Persistence paths
    Q_WEIGHTS_PATH = os.getenv('DASHBOARD_Q_WEIGHTS', './dashboard_q_weights.json')
    INTERACTION_LOGS_PATH = os.getenv('DASHBOARD_INTERACTION_LOGS', './dashboard_interactions.csv')
    HISTORICAL_MODEL_PATH = os.getenv('DASHBOARD_HISTORICAL_MODEL', './dashboard_historical_model.pkl')

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Synthetic Data Generator (fallback)
# ============================================================================
def generate_synthetic_data(n_periods: int = 60, start_date: str = "2020-01-01") -> pd.DataFrame:
    """Generate synthetic helium data for demonstration when file not found."""
    logger.info("Generating synthetic data for demo purposes.")
    np.random.seed(42)
    dates = pd.date_range(start=start_date, periods=n_periods, freq='M')
    t = np.arange(n_periods)

    production = np.clip(28000 - t * 40 + np.random.normal(0, 300, n_periods), 20000, 35000)
    demand = np.clip(27000 + t * 80 + np.random.normal(0, 400, n_periods), 25000, 45000)
    price = 100 * np.exp(np.cumsum(np.random.normal(0.005, 0.1, n_periods)))
    seasonal = 1 + 0.1 * np.sin(2 * np.pi * t / 12)
    price = price * seasonal
    price = np.clip(price, 50, 500)
    demand_supply_ratio = demand / production
    shortage = np.clip((demand_supply_ratio - 0.95) * 4, 0.05, 1.0)
    supply_risk = np.clip(0.2 + t * 0.002 + 0.1 * np.sin(2 * np.pi * t / 24) + np.random.normal(0, 0.05, n_periods), 0.1, 0.9)
    recycling = np.clip(0.10 + t * 0.003 + np.random.normal(0, 0.01, n_periods), 0.05, 0.40)
    substitution = np.clip(0.08 + t * 0.004 + np.random.normal(0, 0.01, n_periods), 0.05, 0.50)
    cooling = np.clip(0.85 + t * 0.005 + np.random.normal(0, 0.02, n_periods), 0.7, 1.3)
    geo_risk = np.clip(0.3 + 0.2 * np.sin(2 * np.pi * t / 36) + np.random.normal(0, 0.05, n_periods), 0.1, 0.8)
    logistics = np.clip(0.2 + t * 0.001 + np.random.normal(0, 0.05, n_periods), 0.1, 0.7)
    new_capacity = np.maximum(500, 2000 + t * 100 + np.random.normal(0, 200, n_periods))

    scarcity_impact = np.clip(shortage * 0.6 + supply_risk * 0.4, 0, 1)
    price_volatility = pd.Series(price).rolling(6).std().fillna(5).values
    price_volatility = np.clip(price_volatility, 1, 30)
    market_regime = []
    for sc in scarcity_impact:
        if sc > 0.7: regime = "crisis"
        elif sc > 0.5: regime = "tightening"
        elif sc > 0.3: regime = "normal"
        else: regime = "stable"
        market_regime.append(regime)
    carbon_intensity = np.clip(300 + 200 * scarcity_impact + np.random.normal(0, 50, n_periods), 50, 800)
    renewable_pct = np.clip(30 + 40 * (1 - scarcity_impact) + np.random.normal(0, 10, n_periods), 5, 95)
    circularity_potential = (recycling + substitution) / 2
    thermal_impact = cooling * scarcity_impact
    future_supply_potential = np.clip((new_capacity / production) * 100, 0, 50)
    capacity_utilization = production / (production + new_capacity)
    esg_score = np.clip((recycling * 40 + (1 - supply_risk) * 30 + (1 - geo_risk) * 30) * 100, 0, 100)
    regulatory_risk = np.clip(geo_risk * 0.5 + logistics * 0.5, 0, 1)

    df = pd.DataFrame({
        'date': dates,
        'global_production_tonnes': np.round(production, 0),
        'global_demand_tonnes': np.round(demand, 0),
        'price_index': np.round(price, 1),
        'shortage_severity_0_1': np.round(shortage, 3),
        'supply_risk_score_0_1': np.round(supply_risk, 3),
        'recycling_rate_0_1': np.round(recycling, 3),
        'substitution_feasibility_0_1': np.round(substitution, 3),
        'cooling_load_sensitivity': np.round(cooling, 3),
        'geopolitical_risk_index': np.round(geo_risk, 3),
        'logistics_disruption_index': np.round(logistics, 3),
        'new_production_capacity_tonnes': np.round(new_capacity, 0),
        'helium_scarcity_impact': np.round(scarcity_impact, 3),
        'price_volatility': np.round(price_volatility, 2),
        'market_regime': market_regime,
        'carbon_intensity_associated': np.round(carbon_intensity, 0),
        'renewable_energy_pct': np.round(renewable_pct, 1),
        'demand_supply_ratio': np.round(demand_supply_ratio, 3),
        'circularity_potential': np.round(circularity_potential, 3),
        'thermal_impact_factor': np.round(thermal_impact, 3),
        'future_supply_potential_pct': np.round(future_supply_potential, 1),
        'capacity_utilization_rate': np.round(capacity_utilization, 3),
        'esg_score': np.round(esg_score, 1),
        'regulatory_risk_score': np.round(regulatory_risk, 3)
    })
    return df


# ============================================================================
# DISTILLATION COMPONENTS FOR ADAPTIVE DASHBOARD LAYOUT
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
    scarcity_trend: float  # change over last 3 periods
    price_trend: float    # change over last 3 periods
    user_preference_score: float  # 0-1, from past interactions

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 12‑dim numeric feature vector."""
        features = [
            self.scarcity_index,
            min(self.price_volatility / 30.0, 1.0),
            min(self.demand_supply_ratio / 2.0, 1.0),
            self.recycling_rate,
            self.geopolitical_risk,
            self.regime_crisis,
            self.regime_tightening,
            self.regime_normal,
            self.regime_stable,
            min(max(self.scarcity_trend / 0.1, -1.0), 1.0),
            min(max(self.price_trend / 50.0, -1.0), 1.0),
            self.user_preference_score,
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: DashboardState) -> np.ndarray:
        """Return probability vector over 5 layout strategies."""
        pass

    @abstractmethod
    def confidence(self, state: DashboardState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class LayoutRuleBasedTeacher(Teacher):
    """Rule‑based expert."""
    ACTION_SPACE = ['balanced', 'scarcity_focus', 'supply_focus', 'risk_focus', 'circularity_focus']

    def predict(self, state: DashboardState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.scarcity_index > 0.7:
            probs[1] = 0.8   # scarcity_focus
        elif state.demand_supply_ratio > 1.3 or state.demand_supply_ratio < 0.8:
            probs[2] = 0.7   # supply_focus
        elif state.geopolitical_risk > 0.7:
            probs[3] = 0.7   # risk_focus
        elif state.recycling_rate < 0.2:
            probs[4] = 0.6   # circularity_focus
        else:
            probs[0] = 0.6   # balanced
        return probs / probs.sum()

    def confidence(self, state: DashboardState) -> float:
        if state.scarcity_index > 0.7:
            return 0.6
        return 0.4


class LayoutHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past interaction logs."""
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(Config.HISTORICAL_MODEL_PATH)
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: DashboardState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: DashboardState) -> float:
        return 0.7 if self.model is not None else 0.0


class LayoutStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, dashboard: 'HeliumMarketDashboard', lr: float = 0.1):
        self.dashboard = dashboard
        self.lr = lr
        self.weights = np.zeros((12, 5))  # 12 features, 5 actions
        self._load_state()

    def _load_state(self):
        """Load Q‑weights from JSON file."""
        path = Path(Config.Q_WEIGHTS_PATH)
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        """Save Q‑weights to JSON file."""
        path = Path(Config.Q_WEIGHTS_PATH)
        try:
            with open(path, 'w') as f:
                json.dump(self.weights.tolist(), f)
            logger.info(f"Saved Q‑teacher weights to {path}")
        except Exception as e:
            logger.error(f"Failed to save Q‑weights: {e}")

    def predict(self, state: DashboardState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: DashboardState) -> float:
        return 0.5

    def update(self, state: DashboardState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 12, n_classes: int = 5, lr: float = 0.01):
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

        # Distillation gradient
        grad_distill = -(teacher_probs - current_probs)

        # Policy gradient
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


class DistillationDashboardOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for dashboard layout selection.
    """
    ACTION_SPACE = ['balanced', 'scarcity_focus', 'supply_focus', 'risk_focus', 'circularity_focus']

    def __init__(self, dashboard: 'HeliumMarketDashboard', config: Dict[str, Any]):
        self.dashboard = dashboard
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            LayoutRuleBasedTeacher(),
            LayoutHistoricalMLTeacher(),
            LayoutStatefulQTeacher(dashboard)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_layout(self, state: DashboardState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
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

    def get_stats(self) -> Dict:
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}


# ============================================================================
# HeliumMarketDashboard (Enhanced with Adaptive Layout)
# ============================================================================
class HeliumMarketDashboard:
    """Interactive dashboard for helium market visualization with adaptive layout."""

    def __init__(
        self,
        data_path: Optional[str] = None,
        forecast_path: Optional[str] = None,
        generate_synthetic_fallback: bool = True,
    ):
        """
        Initialize the dashboard.

        Args:
            data_path: Path to CSV data file. If None, uses Config.DATA_PATH.
            forecast_path: Path to CSV forecast file. If None, uses Config.FORECAST_PATH.
            generate_synthetic_fallback: If True, generate synthetic data when file not found.
        """
        self.data_path = data_path or Config.DATA_PATH
        self.forecast_path = forecast_path or Config.FORECAST_PATH
        self.generate_synthetic_fallback = generate_synthetic_fallback

        self.df = None
        self.forecasts = None

        self._load_data()
        self._load_forecast()
        self._calculate_metrics()

        # Distillation optimizer
        self.distillation_config = {
            'distillation_epsilon': Config.DISTILLATION_EPSILON,
            'distillation_train_every': Config.DISTILLATION_TRAIN_EVERY,
            'distillation_replay_size': Config.DISTILLATION_REPLAY_SIZE,
            'distillation_learning_rate': Config.DISTILLATION_LEARNING_RATE,
        }
        self.layout_optimizer = DistillationDashboardOptimizer(self, self.distillation_config)

        # Interaction tracking
        self.interaction_log: List[Dict] = []
        self.last_layout: Optional[str] = None
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        logger.info("HeliumMarketDashboard initialized with adaptive layout support.")

    def _load_data(self):
        """Load main data from CSV or fallback to synthetic."""
        if os.path.exists(self.data_path):
            try:
                self.df = pd.read_csv(self.data_path, parse_dates=['date'])
                logger.info(f"Loaded data from {self.data_path}")
                return
            except Exception as e:
                logger.warning(f"Failed to load data: {e}")

        if self.generate_synthetic_fallback:
            self.df = generate_synthetic_data()
            logger.info("Using synthetic data.")
        else:
            raise FileNotFoundError(f"Data file not found: {self.data_path}")

    def _load_forecast(self):
        """Load forecast data if available."""
        if os.path.exists(self.forecast_path):
            try:
                self.forecasts = pd.read_csv(self.forecast_path, parse_dates=['date'])
                logger.info(f"Loaded forecasts from {self.forecast_path}")
            except Exception as e:
                logger.warning(f"Failed to load forecasts: {e}")
                self.forecasts = None
        else:
            logger.info("No forecast file found. Forecast chart will be omitted.")

    def _calculate_metrics(self):
        """Compute derived metrics from available columns."""
        if self.df is None:
            return

        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['deficit'] = self.df['global_demand_tonnes'] - self.df['global_production_tonnes']
        self.df['price_change'] = self.df['price_index'].pct_change() * 100

        if 'demand_supply_ratio' not in self.df.columns:
            self.df['demand_supply_ratio'] = self.df['global_demand_tonnes'] / self.df['global_production_tonnes']

        if 'helium_scarcity_impact' not in self.df.columns:
            shortage = self.df['shortage_severity_0_1']
            supply_risk = self.df['supply_risk_score_0_1']
            self.df['helium_scarcity_impact'] = np.clip(shortage * 0.6 + supply_risk * 0.4, 0, 1)
        else:
            self.df['helium_scarcity_impact'] = self.df['helium_scarcity_impact'].clip(0, 1)

        if 'price_volatility' not in self.df.columns:
            self.df['price_volatility'] = self.df['price_index'].rolling(6).std().fillna(5).clip(1, 30)

        if 'market_regime' not in self.df.columns:
            conditions = [
                (self.df['helium_scarcity_impact'] < 0.3),
                (self.df['helium_scarcity_impact'] >= 0.3) & (self.df['helium_scarcity_impact'] < 0.6),
                (self.df['helium_scarcity_impact'] >= 0.6) & (self.df['helium_scarcity_impact'] < 0.8),
                (self.df['helium_scarcity_impact'] >= 0.8)
            ]
            regimes = ['Low Scarcity', 'Moderate Scarcity', 'High Scarcity', 'Critical Scarcity']
            self.df['market_regime'] = np.select(conditions, regimes)

        if 'circularity_potential' not in self.df.columns:
            recycling = self.df['recycling_rate_0_1']
            substitution = self.df['substitution_feasibility_0_1']
            self.df['circularity_potential'] = (recycling + substitution) / 2

        logger.info("Derived metrics calculated successfully.")

    # ---------- State building ----------
    def _build_dashboard_state(self) -> DashboardState:
        """Build state for the distillation agent from current data."""
        latest = self.df.iloc[-1]
        prev = self.df.iloc[-3] if len(self.df) >= 3 else self.df.iloc[0]

        scarcity = latest['helium_scarcity_impact']
        volatility = latest['price_volatility']
        demand_supply = latest['demand_supply_ratio']
        recycling = latest['recycling_rate_0_1']
        geopolitical = latest['geopolitical_risk_index']

        # Regime one-hot
        regime = latest['market_regime']
        crisis = 1.0 if regime == 'Critical Scarcity' else 0.0
        tightening = 1.0 if regime == 'High Scarcity' else 0.0
        normal = 1.0 if regime == 'Moderate Scarcity' else 0.0
        stable = 1.0 if regime == 'Low Scarcity' else 0.0

        # Trends
        scarcity_trend = (scarcity - prev['helium_scarcity_impact']) / 3 if len(self.df) >= 3 else 0
        price_trend = (latest['price_index'] - prev['price_index']) / 3 if len(self.df) >= 3 else 0

        # User preference score (from past interactions) – placeholder, can be updated via feedback
        user_pref = 0.5

        return DashboardState(
            scarcity_index=scarcity,
            price_volatility=volatility,
            demand_supply_ratio=demand_supply,
            recycling_rate=recycling,
            geopolitical_risk=geopolitical,
            regime_crisis=crisis,
            regime_tightening=tightening,
            regime_normal=normal,
            regime_stable=stable,
            scarcity_trend=scarcity_trend,
            price_trend=price_trend,
            user_preference_score=user_pref,
        )

    # ---------- Chart generation methods (unchanged) ----------
    def create_supply_demand_chart(self) -> go.Figure:
        # ... same as original ...
        pass

    def create_scarcity_price_heatmap(self) -> go.Figure:
        # ... same as original ...
        pass

    def create_risk_radar(self) -> go.Figure:
        # ... same as original ...
        pass

    def create_forecast_chart(self) -> go.Figure:
        # ... same as original ...
        pass

    def create_circularity_progress(self) -> go.Figure:
        # ... same as original ...
        pass

    # ---------- KPI and dashboard generation (enhanced) ----------
    def create_kpi_dashboard(self) -> Dict:
        # ... same as original ...
        pass

    def generate_html_dashboard(self, output_file: Optional[str] = None) -> str:
        """
        Generate complete HTML dashboard with adaptive layout.
        """
        if output_file is None:
            output_file = Config.OUTPUT_PATH

        # Build state and select layout
        state = self._build_dashboard_state()
        layout, action_idx, state_vec, teacher_probs = asyncio.run(
            self.layout_optimizer.select_layout(state, exploration=True)
        )
        self.last_layout = layout
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        logger.info(f"Selected layout: {layout}")

        kpis = self.create_kpi_dashboard()

        # Generate charts
        supply_demand = self.create_supply_demand_chart()
        scarcity_price = self.create_scarcity_price_heatmap()
        risk_radar = self.create_risk_radar()
        forecast = self.create_forecast_chart()
        circularity = self.create_circularity_progress()

        # Determine chart order and emphasis based on layout
        chart_order = self._get_chart_order(layout)

        # Build chart HTML in the determined order
        chart_html = ""
        for chart_name in chart_order:
            if chart_name == 'supply_demand':
                chart_html += f'<div class="chart-container">{pio.to_html(supply_demand, full_html=False, config={"displayModeBar": False})}</div>'
            elif chart_name == 'scarcity_price':
                chart_html += f'<div class="chart-container">{pio.to_html(scarcity_price, full_html=False, config={"displayModeBar": False})}</div>'
            elif chart_name == 'risk_radar':
                chart_html += f'<div class="chart-container">{pio.to_html(risk_radar, full_html=False, config={"displayModeBar": False})}</div>'
            elif chart_name == 'forecast':
                if self.forecasts is not None:
                    chart_html += f'<div class="chart-container">{pio.to_html(forecast, full_html=False, config={"displayModeBar": False})}</div>'
            elif chart_name == 'circularity':
                chart_html += f'<div class="chart-container">{pio.to_html(circularity, full_html=False, config={"displayModeBar": False})}</div>'

        # KPI HTML (unchanged)
        kpi_html = '<div class="kpi-container">'
        for name, kpi in kpis.items():
            color = kpi['color']
            kpi_html += f'''
            <div class="kpi-card" style="border-left: 4px solid {color};">
                <h3>{name}</h3>
                <p class="kpi-value">{kpi['value']}</p>
                <p class="kpi-change" style="color: {color if kpi['trend'] == 'up' else 'green'}">{kpi['change']}</p>
            </div>
            '''
        kpi_html += '</div>'

        # Layout indicator
        layout_indicator = f'<div class="layout-indicator">Current layout: <strong>{layout}</strong></div>'

        # Combine into HTML
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Helium Market Intelligence Dashboard</title>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
                .container {{ max-width: 1400px; margin: 0 auto; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; }}
                .kpi-container {{ display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 30px; }}
                .kpi-card {{ flex: 1; min-width: 200px; background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .kpi-value {{ font-size: 28px; font-weight: bold; margin: 10px 0; }}
                .kpi-change {{ font-size: 14px; }}
                .chart-container {{ background: white; padding: 20px; border-radius: 10px; margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .layout-indicator {{ background: #e3f2fd; padding: 10px; border-radius: 5px; margin-bottom: 20px; }}
                h1 {{ margin: 0; }}
                .subtitle {{ margin: 10px 0 0; opacity: 0.9; }}
                @media (max-width: 768px) {{
                    .kpi-container {{ flex-direction: column; }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📈 Helium Market Intelligence Dashboard</h1>
                    <p class="subtitle">Real-time market monitoring & predictive analytics | Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>

                {layout_indicator}
                {kpi_html}
                {chart_html}

                <div class="chart-container">
                    <h3>📊 Market Insights</h3>
                    <ul>
                        <li><strong>Critical Threshold Alert:</strong> Scarcity index currently at {self.df.iloc[-1]['helium_scarcity_impact']:.2f} - {'⚠️ Critical' if self.df.iloc[-1]['helium_scarcity_impact'] > 0.7 else 'Stable'}</li>
                        <li><strong>Supply-Demand Gap:</strong> {self.df.iloc[-1]['deficit']:+,.0f} tonnes - {'Deficit' if self.df.iloc[-1]['deficit'] > 0 else 'Surplus'}</li>
                        <li><strong>Recycling Progress:</strong> {self.df.iloc[-1]['recycling_rate_0_1']:.1%} of target (2030: 50%)</li>
                        <li><strong>Price Forecast:</strong> Expected to {'increase' if self.forecasts is not None and self.forecasts['price_index'].iloc[-1] > self.df['price_index'].iloc[-1] else 'stabilize'} in coming years</li>
                    </ul>
                </div>
            </div>
        </body>
        </html>
        """

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        logger.info(f"Dashboard generated: {output_file}")
        return output_file

    def _get_chart_order(self, layout: str) -> List[str]:
        """Return chart order based on selected layout."""
        all_charts = ['supply_demand', 'scarcity_price', 'risk_radar', 'forecast', 'circularity']
        if layout == 'balanced':
            return all_charts
        elif layout == 'scarcity_focus':
            return ['forecast', 'scarcity_price', 'supply_demand', 'risk_radar', 'circularity']
        elif layout == 'supply_focus':
            return ['supply_demand', 'scarcity_price', 'forecast', 'circularity', 'risk_radar']
        elif layout == 'risk_focus':
            return ['risk_radar', 'scarcity_price', 'supply_demand', 'forecast', 'circularity']
        elif layout == 'circularity_focus':
            return ['circularity', 'supply_demand', 'scarcity_price', 'risk_radar', 'forecast']
        else:
            return all_charts

    # ---------- Interaction recording ----------
    def record_interaction(self, chart_name: str, time_spent: float, explicit_rating: Optional[float] = None):
        """
        Record user interaction with a chart to update the distillation agent.

        Args:
            chart_name: Name of the chart viewed.
            time_spent: Time spent (seconds).
            explicit_rating: Optional user rating (0-1).
        """
        # Compute reward based on time spent and rating
        # Normalize time_spent to 0-1 (e.g., max 60 seconds)
        time_score = min(1.0, time_spent / 60.0)
        if explicit_rating is not None:
            reward = 0.6 * time_score + 0.4 * explicit_rating
        else:
            reward = time_score

        # Log interaction
        self.interaction_log.append({
            'timestamp': datetime.now().isoformat(),
            'chart': chart_name,
            'time_spent': time_spent,
            'rating': explicit_rating,
            'reward': reward,
        })
        # Append to CSV for offline training
        log_path = Path(Config.INTERACTION_LOGS_PATH)
        df_log = pd.DataFrame([self.interaction_log[-1]])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

        # Update distillation agent if we have a recorded layout
        if self.last_layout is not None and self.last_state_vec is not None:
            # Compute next state (could be same)
            state = self._build_dashboard_state()
            next_state_vec = state.to_feature_vector()
            asyncio.run(
                self.layout_optimizer.update(
                    self.last_state_vec,
                    self.last_action_idx,
                    reward,
                    next_state_vec,
                    self.last_teacher_probs
                )
            )
            logger.debug(f"Updated layout agent with reward: {reward:.3f}")

    # ---------- Train historical ML model from logs ----------
    @classmethod
    def train_historical_model(cls, log_path: Optional[Path] = None, model_path: Optional[Path] = None):
        """
        Train a RandomForestClassifier from past interaction logs.
        """
        log_path = log_path or Path(Config.INTERACTION_LOGS_PATH)
        model_path = model_path or Path(Config.HISTORICAL_MODEL_PATH)

        if not log_path.exists():
            logger.warning(f"Interaction logs not found at {log_path}. No model trained.")
            return

        df_logs = pd.read_csv(log_path)
        if len(df_logs) < 10:
            logger.warning("Not enough logs to train historical model (need at least 10).")
            return

        # Prepare features (state vectors) and labels (layout)
        # We need to reconstruct state vectors; we'll assume they were logged separately.
        # For simplicity, we'll compute state from the current data.
        # In production, you would store state vectors in the log.
        # Here we just log a placeholder; a real implementation would require state vectors.
        logger.info("Historical ML training requires state vectors in logs. Please implement logging of state vectors.")
        # For demonstration, we skip actual training.

    # ---------- Export methods (unchanged) ----------
    def export_chart(self, chart_name: str, output_file: str, format: str = 'html'):
        # ... same as original ...
        pass

    # ---------- Dash web server (enhanced) ----------
    def serve_dash(self, host: str = '127.0.0.1', port: int = 8050):
        """
        Serve the dashboard as a Dash web app with adaptive layout.
        """
        if not DASH_AVAILABLE:
            raise ImportError("Dash not installed. Install with: pip install dash")

        from dash import dcc, html, Input, Output, State

        app = dash.Dash(__name__, title="Helium Dashboard")

        # Build state and select layout
        state = self._build_dashboard_state()
        layout, action_idx, state_vec, teacher_probs = asyncio.run(
            self.layout_optimizer.select_layout(state, exploration=True)
        )
        self.last_layout = layout
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        chart_order = self._get_chart_order(layout)

        # Create chart components in the determined order
        chart_components = []
        for chart_name in chart_order:
            if chart_name == 'supply_demand':
                chart_components.append(dcc.Graph(id='supply-demand', figure=self.create_supply_demand_chart()))
            elif chart_name == 'scarcity_price':
                chart_components.append(dcc.Graph(id='scarcity-price', figure=self.create_scarcity_price_heatmap()))
            elif chart_name == 'risk_radar':
                chart_components.append(dcc.Graph(id='risk-radar', figure=self.create_risk_radar()))
            elif chart_name == 'forecast':
                if self.forecasts is not None:
                    chart_components.append(dcc.Graph(id='forecast', figure=self.create_forecast_chart()))
            elif chart_name == 'circularity':
                chart_components.append(dcc.Graph(id='circularity', figure=self.create_circularity_progress()))

        app.layout = html.Div([
            html.H1("Helium Market Dashboard", style={'textAlign': 'center'}),
            html.Div(f"Current layout: {layout}", id='layout-indicator'),
            html.Div(id='kpi-display'),
            *chart_components,
            dcc.Interval(id='interval-component', interval=300000, n_intervals=0),
            # Hidden div to store layout selection for reward
            html.Div(id='hidden-layout', style={'display': 'none'}, children=layout),
        ])

        @app.callback(
            Output('kpi-display', 'children'),
            Input('interval-component', 'n_intervals')
        )
        def update_kpis(_):
            kpis = self.create_kpi_dashboard()
            kpi_divs = []
            for name, kpi in kpis.items():
                color = kpi['color']
                kpi_divs.append(html.Div([
                    html.H3(name, style={'margin': '0'}),
                    html.P(kpi['value'], style={'fontSize': '24px', 'fontWeight': 'bold', 'margin': '10px 0'}),
                    html.P(f"{kpi['change']} vs previous", style={'color': color})
                ], style={'flex': 1, 'padding': '10px', 'border': f'1px solid {color}', 'borderRadius': '5px'}))
            return html.Div(kpi_divs, style={'display': 'flex', 'gap': '10px', 'flexWrap': 'wrap'})

        # Callback to record interactions (chart clicks, time spent)
        @app.callback(
            Output('hidden-layout', 'children'),
            Input('supply-demand', 'clickData'),
            Input('scarcity-price', 'clickData'),
            Input('risk-radar', 'clickData'),
            Input('forecast', 'clickData'),
            Input('circularity', 'clickData'),
            State('hidden-layout', 'children'),
        )
        def record_chart_click(*args):
            # This is a placeholder; in a real app, you would track time spent.
            return args[-1]  # return the layout unchanged

        logger.info(f"Starting Dash server at http://{host}:{port}")
        app.run_server(host=host, port=port, debug=False)


# ============================================================================
# CLI Interface (unchanged)
# ============================================================================
def parse_args():
    parser = argparse.ArgumentParser(description="Helium Market Dashboard")
    parser.add_argument('--data', default=Config.DATA_PATH, help='Path to CSV data file')
    parser.add_argument('--forecast', default=Config.FORECAST_PATH, help='Path to CSV forecast file')
    parser.add_argument('--output', default=Config.OUTPUT_PATH, help='Output HTML file path')
    parser.add_argument('--no-fallback', action='store_true', help='Disable synthetic fallback')
    parser.add_argument('--serve', action='store_true', help='Serve as web app via Dash')
    parser.add_argument('--port', type=int, default=8050, help='Port for Dash server')
    parser.add_argument('--export-chart', choices=['supply_demand', 'scarcity_price', 'risk_radar', 'forecast', 'circularity'],
                        help='Export a single chart')
    parser.add_argument('--export-format', choices=['html', 'png'], default='html', help='Export format')
    parser.add_argument('--export-output', default='chart.html', help='Output file for exported chart')
    return parser.parse_args()


def main():
    args = parse_args()

    dashboard = HeliumMarketDashboard(
        data_path=args.data,
        forecast_path=args.forecast,
        generate_synthetic_fallback=not args.no_fallback,
    )

    if args.export_chart:
        dashboard.export_chart(args.export_chart, args.export_output, args.export_format)
        return

    if args.serve:
        dashboard.serve_dash(port=args.port)
    else:
        dashboard.generate_html_dashboard(args.output)
        print(f"Dashboard generated: {args.output}. Open in browser.")


# ============================================================================
# UNIT TESTS (for distillation components)
# ============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationComponents(IsolatedAsyncioTestCase):
    def setUp(self):
        self.dashboard = HeliumMarketDashboard(generate_synthetic_fallback=True)

    def test_state_feature_vector(self):
        state = DashboardState(
            scarcity_index=0.5,
            price_volatility=10,
            demand_supply_ratio=1.2,
            recycling_rate=0.3,
            geopolitical_risk=0.4,
            regime_crisis=0.0,
            regime_tightening=0.0,
            regime_normal=1.0,
            regime_stable=0.0,
            scarcity_trend=0.02,
            price_trend=5,
            user_preference_score=0.5,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 12)

    def test_rule_based_teacher(self):
        teacher = LayoutRuleBasedTeacher()
        state = DashboardState(
            scarcity_index=0.8,
            price_volatility=10,
            demand_supply_ratio=1.2,
            recycling_rate=0.3,
            geopolitical_risk=0.4,
            regime_crisis=0.0,
            regime_tightening=0.0,
            regime_normal=1.0,
            regime_stable=0.0,
            scarcity_trend=0.02,
            price_trend=5,
            user_preference_score=0.5,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(np.sum(probs), 1.0)
        self.assertGreater(probs[1], 0.5)  # scarcity_focus should be highest

    def test_get_chart_order(self):
        order = self.dashboard._get_chart_order('scarcity_focus')
        self.assertEqual(order[0], 'forecast')


if __name__ == "__main__":
    main()
