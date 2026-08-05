"""
Enhanced Predictive Maintenance for Hardware Based on Sustainability Metrics v3.1.0
============================================================================

Tracks energy efficiency (FLOPs/Joule) per node over time, forecasts when
efficiency will drop below a threshold, simulates replacement impact via
DigitalTwin, and generates maintenance recommendations during low‑carbon periods.

ENHANCEMENTS OVER v3.0.0:
- Adaptive action selection (replace/refurbish/monitor) via Multi‑Teacher Distillation.
- State‑aware decisions based on efficiency, slope, days to threshold, cost savings,
  carbon intensity, material index, and historical performance.
- Online learning from outcome metrics (energy saved, cost saved, carbon saved).
- Teachers: rule‑based, historical ML, stateful Q.
- Student: linear softmax with distillation + REINFORCE.
- Persistence for Q‑teacher weights and interaction logs.
- Offline training for historical ML teacher.
- Unit tests for distillation components.
All previous features (async persistence, forecasting, digital twin, carbon/LCA integration, etc.) retained.
"""

import asyncio
import json
import logging
import os
import sqlite3
import pickle
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Callable, Union
import numpy as np
from collections import deque
import random
from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path

# ---------- Pydantic ----------
from pydantic import BaseModel, Field, field_validator, ConfigDict

# ---------- aiosqlite ----------
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ---------- FastAPI ----------
try:
    from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
    from fastapi.responses import JSONResponse
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# ---------- Optional forecasting libraries ----------
try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

try:
    from statsmodels.tsa.arima.model import ARIMA
    ARIMA_AVAILABLE = True
except ImportError:
    ARIMA_AVAILABLE = False

# ---------- Tenacity for retries ----------
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ---------- scikit-learn for ML teacher ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# ---------- Circuit Breaker ----------
class CircuitBreaker:
    """Async circuit breaker with half‑open state."""
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = "closed"
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self._state == "open":
                if time.time() - self._last_failure_time > self.recovery_timeout:
                    self._state = "half-open"
                    self._failure_count = 0
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is open")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == "half-open":
                    self._state = "closed"
                    self._failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = time.time()
                if self._failure_count >= self.failure_threshold:
                    self._state = "open"
            raise e

# ============================================================================
# 1. CONFIGURATION (Pydantic, always used)
# ============================================================================
class PredictiveMaintenanceConfig(BaseModel):
    """Configuration for predictive maintenance."""
    # Efficiency threshold (FLOPs/Joule) below which maintenance is triggered
    efficiency_threshold: float = Field(1.0e9, gt=0)
    # Minimum number of data points to make a forecast
    min_data_points: int = Field(10, ge=5)
    # Forecast horizon (days) to look ahead for threshold crossing
    forecast_horizon_days: int = Field(30, ge=1)
    # Confidence interval width (percentage) for forecast
    forecast_confidence: float = Field(0.95, gt=0, lt=1)
    # Low‑carbon windows (static times of day)
    low_carbon_windows: List[Dict[str, str]] = Field(
        default_factory=lambda: [
            {"start": "02:00", "end": "06:00"},
            {"start": "12:00", "end": "14:00"},
        ]
    )
    # Default replacement efficiency gain (factor)
    replacement_efficiency_gain: float = Field(1.2, gt=1.0)
    # Refurbishment efficiency gain (factor)
    refurbishment_efficiency_gain: float = Field(1.05, ge=1.0)
    # Maintenance lead time (days) to schedule before predicted failure
    maintenance_lead_time: int = Field(7, ge=0)
    # How often to update forecasts (seconds)
    refresh_interval: int = Field(3600, ge=60)
    # Persistence
    persistence_enabled: bool = True
    persistence_path: str = Field("./predictive_maintenance.db")
    # Data retention (days); records older than this are pruned
    data_retention_days: int = Field(365, ge=1)
    # Carbon intensity integration
    carbon_intensity_enabled: bool = True
    carbon_intensity_api_key: Optional[str] = None
    carbon_region: str = "global"
    # LCA integration
    lca_enabled: bool = True
    # Anomaly trigger
    anomaly_trigger_enabled: bool = True
    # Cost parameters
    hardware_cost_usd: float = Field(5000.0, gt=0)
    maintenance_cost_usd: float = Field(500.0, ge=0)
    carbon_offset_price_per_kg_usd: float = Field(0.10, gt=0)
    electricity_price_per_kwh_usd: float = Field(0.12, gt=0)

    # NEW: Distillation parameters
    distillation_epsilon: float = Field(0.1, ge=0, le=1)
    distillation_train_every: int = Field(10, ge=1)
    distillation_replay_size: int = Field(2000, ge=10)
    distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
    distill_weight: float = Field(0.7, ge=0, le=1)
    rl_weight: float = Field(0.3, ge=0, le=1)

    # Persistence paths for distillation
    q_weights_path: str = Field("./pm_q_weights.json")
    interaction_logs_path: str = Field("./pm_interactions.csv")
    historical_model_path: str = Field("./pm_historical_model.pkl")

    @field_validator('low_carbon_windows')
    @classmethod
    def validate_windows(cls, v):
        for w in v:
            if 'start' not in w or 'end' not in w:
                raise ValueError("Each window must have 'start' and 'end'")
            start = datetime.strptime(w['start'], "%H:%M").time()
            end = datetime.strptime(w['end'], "%H:%M").time()
            if start >= end:
                raise ValueError("Window start must be before end")
        return v

    model_config = ConfigDict(env_prefix="PRED_MAINT_")

    @classmethod
    def from_dict(cls, data: Dict) -> "PredictiveMaintenanceConfig":
        return cls(**data)

# ============================================================================
# 2. DATA STRUCTURES
# ============================================================================
@dataclass
class EfficiencyRecord:
    timestamp: datetime
    flops_per_joule: float
    energy_joules: float
    flops: float

@dataclass
class MaintenanceRecommendation:
    node_id: str
    current_efficiency: float
    predicted_efficiency_in_30_days: float
    threshold: float
    days_to_threshold: float
    recommended_action: str
    suggested_date: datetime
    carbon_savings_kg: float
    cost_savings_usd: float
    payback_days: Optional[float]
    simulation_result: Dict[str, Any] = field(default_factory=dict)

# ============================================================================
# 3. PERSISTENCE MANAGER (Async SQLite with connection pooling)
# ============================================================================
class PersistenceManager:
    """Async persistence for efficiency history and recommendations."""
    def __init__(self, config: PredictiveMaintenanceConfig):
        self.config = config
        self.db_path = config.persistence_path
        self._pool: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
        self._init_db()

    async def _init_db(self):
        async with self._get_connection() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS efficiency_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT,
                    timestamp REAL,
                    flops_per_joule REAL,
                    energy_joules REAL,
                    flops REAL
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS recommendations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT,
                    recommendation TEXT,  # JSON
                    created_at REAL
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_efficiency_node_time ON efficiency_history (node_id, timestamp)
            """)
            await conn.commit()

    async def _get_connection(self):
        if AIOSQLITE_AVAILABLE:
            if self._pool is None:
                self._pool = await aiosqlite.connect(self.db_path)
            return self._pool
        else:
            # Fallback to sync sqlite3 + thread offload
            return sqlite3.connect(self.db_path)

    async def _execute(self, query: str, params: tuple = ()):
        if AIOSQLITE_AVAILABLE:
            async with self._get_connection() as conn:
                return await conn.execute(query, params)
        else:
            def sync_exec():
                conn = sqlite3.connect(self.db_path)
                cur = conn.execute(query, params)
                conn.commit()
                return cur
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, sync_exec)

    async def _prune_old_data(self):
        cutoff = datetime.now() - timedelta(days=self.config.data_retention_days)
        await self._execute(
            "DELETE FROM efficiency_history WHERE timestamp < ?",
            (cutoff.timestamp(),)
        )

    async def save_efficiency(self, node_id: str, record: EfficiencyRecord):
        await self._prune_old_data()
        await self._execute(
            """
            INSERT INTO efficiency_history (node_id, timestamp, flops_per_joule, energy_joules, flops)
            VALUES (?, ?, ?, ?, ?)
            """,
            (node_id, record.timestamp.timestamp(), record.flops_per_joule,
             record.energy_joules, record.flops)
        )

    async def load_efficiency(self, node_id: str, limit: int = 1000) -> List[EfficiencyRecord]:
        rows = await self._execute(
            """
            SELECT timestamp, flops_per_joule, energy_joules, flops
            FROM efficiency_history WHERE node_id = ? ORDER BY timestamp DESC LIMIT ?
            """,
            (node_id, limit)
        )
        records = []
        async for row in rows:
            records.append(EfficiencyRecord(
                timestamp=datetime.fromtimestamp(row[0]),
                flops_per_joule=row[1],
                energy_joules=row[2],
                flops=row[3]
            ))
        return records

    async def save_recommendation(self, rec: MaintenanceRecommendation):
        await self._execute(
            """
            INSERT INTO recommendations (node_id, recommendation, created_at)
            VALUES (?, ?, ?)
            """,
            (rec.node_id, json.dumps(rec.__dict__, default=str), datetime.now().timestamp())
        )

    async def load_recommendations(self, node_id: Optional[str] = None, limit: int = 10) -> List[MaintenanceRecommendation]:
        if node_id:
            rows = await self._execute(
                """
                SELECT recommendation FROM recommendations
                WHERE node_id = ? ORDER BY created_at DESC LIMIT ?
                """,
                (node_id, limit)
            )
        else:
            rows = await self._execute(
                """
                SELECT recommendation FROM recommendations ORDER BY created_at DESC LIMIT ?
                """,
                (limit,)
            )
        recs = []
        async for row in rows:
            data = json.loads(row[0])
            rec = MaintenanceRecommendation(**data)
            rec.suggested_date = datetime.fromisoformat(data['suggested_date'])
            recs.append(rec)
        return recs

    async def close(self):
        if AIOSQLITE_AVAILABLE and self._pool:
            await self._pool.close()
            self._pool = None

# ============================================================================
# 4. NODE EFFICIENCY TRACKER (unchanged)
# ============================================================================
class NodeEfficiencyTracker:
    def __init__(self, config: PredictiveMaintenanceConfig, persistence: Optional[PersistenceManager] = None):
        self.config = config
        self.history: Dict[str, List[EfficiencyRecord]] = {}
        self.max_history = 1000
        self.persistence = persistence
        self._lock = asyncio.Lock()

    async def _load_node_history(self, node_id: str):
        if self.persistence:
            records = await self.persistence.load_efficiency(node_id, limit=self.max_history)
            if records:
                async with self._lock:
                    self.history[node_id] = records

    async def add_measurement(self, node_id: str, flops: float, energy_joules: float) -> None:
        if node_id not in self.history:
            await self._load_node_history(node_id)
        if node_id not in self.history:
            self.history[node_id] = []

        efficiency = flops / energy_joules if energy_joules > 0 else 0.0
        record = EfficiencyRecord(
            timestamp=datetime.now(),
            flops_per_joule=efficiency,
            energy_joules=energy_joules,
            flops=flops,
        )
        async with self._lock:
            self.history[node_id].append(record)
            if len(self.history[node_id]) > self.max_history:
                self.history[node_id] = self.history[node_id][-self.max_history:]

        if self.persistence:
            await self.persistence.save_efficiency(node_id, record)

    async def get_efficiency_series(self, node_id: str) -> Tuple[np.ndarray, np.ndarray]:
        if node_id not in self.history:
            await self._load_node_history(node_id)
        if node_id not in self.history or len(self.history[node_id]) == 0:
            return np.array([]), np.array([])
        async with self._lock:
            records = self.history[node_id]
        times = np.array([r.timestamp.timestamp() for r in records])
        effs = np.array([r.flops_per_joule for r in records])
        return times, effs

    async def get_latest_efficiency(self, node_id: str) -> Optional[float]:
        if node_id not in self.history:
            await self._load_node_history(node_id)
        if node_id not in self.history or not self.history[node_id]:
            return None
        async with self._lock:
            return self.history[node_id][-1].flops_per_joule

    async def get_node_health(self, node_id: str, threshold: float) -> Dict[str, Any]:
        eff = await self.get_latest_efficiency(node_id)
        if eff is None:
            return {"status": "unknown", "efficiency": None}
        if eff < threshold:
            return {"status": "critical", "efficiency": eff}
        times, effs = await self.get_efficiency_series(node_id)
        if len(effs) > 5:
            slope = np.polyfit(range(len(effs)), effs, 1)[0]
            if slope < 0:
                return {"status": "degrading", "efficiency": eff}
        return {"status": "healthy", "efficiency": eff}

# ============================================================================
# 5. PREDICTIVE REFLEXIVITY (unchanged)
# ============================================================================
class PredictiveReflexivity:
    def __init__(self, config: PredictiveMaintenanceConfig):
        self.config = config
        self.horizon_days = config.forecast_horizon_days
        self.min_points = config.min_data_points

    def forecast(self, times: np.ndarray, values: np.ndarray) -> Dict[str, Any]:
        if len(values) < self.min_points:
            return {"error": "Insufficient data"}

        if STATSMODELS_AVAILABLE:
            try:
                model = ExponentialSmoothing(values, trend='add', seasonal=None, damped_trend=True)
                fit = model.fit()
                forecast = fit.forecast(self.horizon_days)
                trend = fit.params.get('trend', 0.0)
                slope = trend
                residuals = values - fit.fittedvalues
                std_res = np.std(residuals)
                z = 1.96
                lower = forecast - z * std_res
                upper = forecast + z * std_res
                return self._postprocess_forecast(times, values, forecast, lower, upper, slope=slope)
            except Exception as e:
                logger.warning(f"Exponential smoothing failed: {e}, falling back to linear regression")

        return self._linear_forecast(times, values)

    def _linear_forecast(self, times: np.ndarray, values: np.ndarray) -> Dict[str, Any]:
        t0 = times[0]
        x = (times - t0) / (24 * 3600)
        y = values
        coeffs = np.polyfit(x, y, 1)
        slope = coeffs[0]
        intercept = coeffs[1]

        last_day = x[-1]
        future_days = np.linspace(last_day, last_day + self.horizon_days, 100)
        predictions = slope * future_days + intercept

        residuals = y - (slope * x + intercept)
        std_res = np.std(residuals)
        z = 1.96
        lower = predictions - z * std_res
        upper = predictions + z * std_res
        return self._postprocess_forecast(times, values, predictions, lower, upper, slope=slope)

    def _postprocess_forecast(self, times, values, predictions, lower, upper, slope):
        threshold = self.config.efficiency_threshold
        days_to_threshold = None
        crossing_day = None
        future_days = np.linspace(0, self.horizon_days, len(predictions))
        last_day = (times[-1] - times[0]) / (24 * 3600)
        for i, pred in enumerate(predictions):
            if pred < threshold:
                crossing_day = future_days[i]
                days_to_threshold = crossing_day
                break
        if predictions[0] < threshold:
            days_to_threshold = 0.0

        return {
            "slope": slope,
            "predictions": predictions.tolist(),
            "future_days": future_days.tolist(),
            "lower_bound": lower.tolist(),
            "upper_bound": upper.tolist(),
            "days_to_threshold": days_to_threshold,
            "crossing_day": crossing_day,
        }

# ============================================================================
# 6. DIGITALTWIN SIMULATOR (unchanged)
# ============================================================================
class DigitalTwinSimulator:
    def __init__(self, config: PredictiveMaintenanceConfig,
                 carbon_manager: Optional[Any] = None,
                 lca_client: Optional[Any] = None):
        self.config = config
        self.carbon_manager = carbon_manager
        self.lca_client = lca_client
        self.co2_per_kwh = 0.2
        self._carbon_circuit = CircuitBreaker("carbon_api")
        self._lca_circuit = CircuitBreaker("lca_api")

    async def _get_carbon_intensity(self) -> float:
        if not self.config.carbon_intensity_enabled or self.carbon_manager is None:
            return self.co2_per_kwh
        try:
            intensity_data = await self._carbon_circuit.call(
                self.carbon_manager.get_current_intensity
            )
            return intensity_data.get('intensity', 400) / 1000
        except Exception as e:
            logger.warning(f"Carbon intensity retrieval failed: {e}")
            return self.co2_per_kwh

    async def _get_material_index(self, hardware_model: str) -> float:
        if not self.config.lca_enabled or self.lca_client is None:
            return 0.0
        try:
            result = await self._lca_circuit.call(
                self.lca_client.get_material_index,
                hardware_model
            )
            return result.get('material_index', 0.0)
        except Exception as e:
            logger.warning(f"LCA material index retrieval failed: {e}")
            return 0.0

    async def simulate_replacement(
        self,
        node_id: str,
        current_efficiency: float,
        action: str = "replace",
        expected_new_efficiency: float = None,
        workload_flops_per_day: float = 1e12,
        simulation_days: int = 365,
        hardware_model: Optional[str] = None,
    ) -> Dict[str, Any]:
        if action == "replace":
            gain = self.config.replacement_efficiency_gain
        else:
            gain = self.config.refurbishment_efficiency_gain

        if expected_new_efficiency is None:
            expected_new_efficiency = current_efficiency * gain

        energy_current = workload_flops_per_day / current_efficiency
        energy_new = workload_flops_per_day / expected_new_efficiency
        energy_saved_per_day = energy_current - energy_new
        energy_saved_total = energy_saved_per_day * simulation_days

        carbon_intensity = await self._get_carbon_intensity()
        co2_saved_total = energy_saved_total / 3.6e6 * carbon_intensity
        cost_saved_total = energy_saved_total / 3.6e6 * self.config.electricity_price_per_kwh_usd

        hardware_cost = self.config.hardware_cost_usd if action == "replace" else 0.0
        maintenance_cost = self.config.maintenance_cost_usd if action == "refurbish" else 0.0
        total_initial_cost = hardware_cost + maintenance_cost

        carbon_offset_value = co2_saved_total * self.config.carbon_offset_price_per_kg_usd
        net_savings = cost_saved_total + carbon_offset_value - total_initial_cost

        daily_savings = (cost_saved_total + carbon_offset_value) / simulation_days
        payback_days = total_initial_cost / daily_savings if daily_savings > 0 else None

        return {
            "node_id": node_id,
            "action": action,
            "current_efficiency": current_efficiency,
            "new_efficiency": expected_new_efficiency,
            "workload_flops_per_day": workload_flops_per_day,
            "simulation_days": simulation_days,
            "energy_saved_per_day_joules": energy_saved_per_day,
            "energy_saved_total_joules": energy_saved_total,
            "co2_saved_total_kg": co2_saved_total,
            "cost_saved_total_usd": cost_saved_total,
            "carbon_offset_value_usd": carbon_offset_value,
            "hardware_cost_usd": hardware_cost,
            "maintenance_cost_usd": maintenance_cost,
            "total_initial_cost_usd": total_initial_cost,
            "net_savings_usd": net_savings,
            "payback_days": payback_days,
        }

# ============================================================================
# 7. DISTILLATION COMPONENTS FOR ACTION SELECTION
# ============================================================================

@dataclass
class MaintenanceState:
    """State for the distillation agent."""
    current_efficiency: float
    slope: float
    days_to_threshold: float
    net_savings_replace: float
    net_savings_refurbish: float
    payback_replace: Optional[float]
    payback_refurbish: Optional[float]
    carbon_intensity: float
    material_index: float
    # Historical performance (from logs)
    action_success_rates: Dict[str, float]  # replace, refurbish, monitor
    avg_reward: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 11‑dim numeric feature vector."""
        features = [
            min(self.current_efficiency / 5e9, 1.0),
            min(abs(self.slope) / 1e7, 1.0),
            min(self.days_to_threshold / 90.0, 1.0),
            min(self.net_savings_replace / 10000.0, 1.0),
            min(self.net_savings_refurbish / 10000.0, 1.0),
            min(self.carbon_intensity / 1.0, 1.0),
            min(self.material_index / 2.0, 1.0),
            self.action_success_rates.get('replace', 0.5),
            self.action_success_rates.get('refurbish', 0.5),
            self.action_success_rates.get('monitor', 0.5),
            self.avg_reward,
        ]
        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: MaintenanceState) -> np.ndarray:
        """Return probability vector over 3 actions."""
        pass

    @abstractmethod
    def confidence(self, state: MaintenanceState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class ActionRuleBasedTeacher(Teacher):
    """Rule‑based expert."""
    ACTIONS = ['replace', 'refurbish', 'monitor']

    def predict(self, state: MaintenanceState) -> np.ndarray:
        probs = np.ones(3) * 0.1
        if state.days_to_threshold <= 0:
            if state.net_savings_replace > state.net_savings_refurbish:
                probs[0] = 0.8  # replace
            else:
                probs[1] = 0.8  # refurbish
        elif state.days_to_threshold <= 14:  # lead_time + 7
            if state.net_savings_replace > state.net_savings_refurbish:
                probs[0] = 0.7
            else:
                probs[1] = 0.7
        else:
            probs[2] = 0.7  # monitor
        return probs / probs.sum()

    def confidence(self, state: MaintenanceState) -> float:
        if state.days_to_threshold <= 0:
            return 0.6
        return 0.4


class ActionHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past interactions."""
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(PredictiveMaintenanceConfig().historical_model_path)
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: MaintenanceState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(3) / 3
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: MaintenanceState) -> float:
        return 0.7 if self.model is not None else 0.0


class ActionStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((11, 3))  # 11 features, 3 actions
        self._load_state()

    def _load_state(self):
        path = Path(PredictiveMaintenanceConfig().q_weights_path)
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path(PredictiveMaintenanceConfig().q_weights_path)
        with open(path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: MaintenanceState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: MaintenanceState) -> float:
        return 0.5

    def update(self, state: MaintenanceState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 11, n_classes: int = 3, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray, num_classes: int) -> np.ndarray:
        if num_classes != self.n_classes:
            new_weights = np.zeros((self.weights.shape[0], num_classes))
            new_biases = np.zeros(num_classes)
            min_dim = min(self.n_classes, num_classes)
            new_weights[:, :min_dim] = self.weights[:, :min_dim]
            new_biases[:min_dim] = self.biases[:min_dim]
            self.weights = new_weights
            self.biases = new_biases
            self.n_classes = num_classes
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector, self.n_classes)
        logits = state_vector @ self.weights + self.biases

        grad_distill = -(teacher_probs - current_probs)
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


class DistillationActionOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for maintenance action selection.
    Actions: replace, refurbish, monitor.
    """
    ACTIONS = ['replace', 'refurbish', 'monitor']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            ActionRuleBasedTeacher(),
            ActionHistoricalMLTeacher(),
            ActionStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_action(self, state: MaintenanceState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = 3

        teacher_probs = np.zeros(n)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            if len(prob) != n:
                if len(prob) < n:
                    prob = np.pad(prob, (0, n - len(prob)), 'constant')
                else:
                    prob = prob[:n]
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(n) / n

        student_probs = self.student.predict_proba(state_vec, n)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, n - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.ACTIONS[action_idx], action_idx, state_vec, teacher_probs

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
# 8. MAINTENANCE SCHEDULER (ENHANCED)
# ============================================================================
class MaintenanceScheduler:
    """
    Generates maintenance recommendations and schedules them during low‑carbon windows.
    Uses distillation to select action.
    """
    def __init__(self, config: PredictiveMaintenanceConfig,
                 carbon_manager: Optional[Any] = None):
        self.config = config
        self.carbon_manager = carbon_manager
        self.low_carbon_windows = config.low_carbon_windows
        self.lead_time = config.maintenance_lead_time
        self.recommendations: Dict[str, MaintenanceRecommendation] = {}
        self._lock = asyncio.Lock()

        # Distillation action optimizer
        self.action_optimizer = DistillationActionOptimizer({
            'distillation_epsilon': config.distillation_epsilon,
            'distillation_train_every': config.distillation_train_every,
            'distillation_replay_size': config.distillation_replay_size,
            'distillation_learning_rate': config.distillation_learning_rate,
        })

        # Interaction tracking
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

    def parse_time(self, time_str: str) -> datetime.time:
        return datetime.strptime(time_str, "%H:%M").time()

    async def get_next_low_carbon_window(self, from_date: datetime) -> Optional[datetime]:
        today = from_date.date()
        for window in self.low_carbon_windows:
            start_time = self.parse_time(window["start"])
            end_time = self.parse_time(window["end"])
            candidate = datetime.combine(today, start_time)
            if candidate > from_date:
                return candidate
        tomorrow = today + timedelta(days=1)
        first_window = self.low_carbon_windows[0]
        start_time = self.parse_time(first_window["start"])
        return datetime.combine(tomorrow, start_time)

    async def generate_recommendation(
        self,
        node_id: str,
        current_efficiency: float,
        forecast_result: Dict[str, Any],
        sim_replace: Dict[str, Any],
        sim_refurb: Dict[str, Any],
    ) -> MaintenanceRecommendation:
        """
        Create a maintenance recommendation using distillation to select action.
        """
        # Build state
        state = MaintenanceState(
            current_efficiency=current_efficiency,
            slope=forecast_result.get("slope", 0),
            days_to_threshold=forecast_result.get("days_to_threshold", float('inf')),
            net_savings_replace=sim_replace.get("net_savings_usd", 0),
            net_savings_refurbish=sim_refurb.get("net_savings_usd", 0),
            payback_replace=sim_replace.get("payback_days"),
            payback_refurbish=sim_refurb.get("payback_days"),
            carbon_intensity=await self._get_carbon_intensity(),
            material_index=sim_replace.get("material_index", 0),
            action_success_rates=self._get_action_success_rates(),
            avg_reward=self._get_avg_reward(),
        )

        # Select action via distillation
        action, action_idx, state_vec, teacher_probs = await self.action_optimizer.select_action(state, exploration=True)
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        # Determine suggested date
        threshold = self.config.efficiency_threshold
        days_to = forecast_result.get("days_to_threshold")

        if action == "monitor" or days_to is None or days_to > 30:
            suggested_date = datetime.now() + timedelta(days=30)
        else:
            # Schedule maintenance before crossing
            crossing_date = datetime.now() + timedelta(days=days_to)
            maintenance_date = crossing_date - timedelta(days=self.lead_time)
            suggested_date = await self.get_next_low_carbon_window(maintenance_date)

        # Use the simulation result that matches the chosen action
        if action == "replace":
            simulation_result = sim_replace
        elif action == "refurbish":
            simulation_result = sim_refurb
        else:
            simulation_result = {}  # monitor

        # Carbon and cost savings
        co2_saved = simulation_result.get("co2_saved_total_kg", 0.0) if action in ["replace", "refurbish"] else 0.0
        cost_saved = simulation_result.get("net_savings_usd", 0.0) if action in ["replace", "refurbish"] else 0.0
        payback = simulation_result.get("payback_days")

        # Predict efficiency in 30 days
        slope = forecast_result.get("slope", 0)
        pred_eff_30 = current_efficiency + slope * 30

        rec = MaintenanceRecommendation(
            node_id=node_id,
            current_efficiency=current_efficiency,
            predicted_efficiency_in_30_days=pred_eff_30,
            threshold=threshold,
            days_to_threshold=days_to if days_to is not None else float('inf'),
            recommended_action=action,
            suggested_date=suggested_date,
            carbon_savings_kg=co2_saved,
            cost_savings_usd=cost_saved,
            payback_days=payback,
            simulation_result=simulation_result,
        )
        async with self._lock:
            self.recommendations[node_id] = rec
        logger.info(f"Maintenance scheduled for node {node_id}: {rec.recommended_action} on {rec.suggested_date}")
        return rec

    async def record_outcome(self, node_id: str, action: str, actual_energy_saved: float, actual_cost_saved: float, actual_carbon_saved: float):
        """
        Record the outcome of a maintenance action to update the distillation agent.
        """
        # Compute reward based on savings
        reward = 0.0
        if actual_energy_saved > 0:
            reward += 0.4 * min(1.0, actual_energy_saved / 1e6)
        if actual_cost_saved > 0:
            reward += 0.3 * min(1.0, actual_cost_saved / 1000)
        if actual_carbon_saved > 0:
            reward += 0.3 * min(1.0, actual_carbon_saved / 100)
        reward = max(0.0, min(1.0, reward))

        # Log interaction
        self.interaction_log.append({
            'timestamp': datetime.now().isoformat(),
            'node_id': node_id,
            'action': action,
            'reward': reward,
        })
        log_path = Path(PredictiveMaintenanceConfig().interaction_logs_path)
        df_log = pd.DataFrame([self.interaction_log[-1]])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

        # Update agent
        if self.last_state_vec is not None and self.last_action_idx is not None:
            next_state_vec = self.last_state_vec  # for simplicity, same state
            await self.action_optimizer.update(
                self.last_state_vec,
                self.last_action_idx,
                reward,
                next_state_vec,
                self.last_teacher_probs
            )

    def _get_action_success_rates(self) -> Dict[str, float]:
        """Compute success rates from interaction log."""
        rates = {'replace': 0.5, 'refurbish': 0.5, 'monitor': 0.5}
        if not self.interaction_log:
            return rates
        for action in rates.keys():
            entries = [e for e in self.interaction_log[-100:] if e['action'] == action]
            if entries:
                successes = sum(1 for e in entries if e['reward'] > 0.5)
                rates[action] = successes / len(entries)
        return rates

    def _get_avg_reward(self) -> float:
        if not self.interaction_log:
            return 0.5
        rewards = [e['reward'] for e in self.interaction_log[-50:]]
        return np.mean(rewards) if rewards else 0.5

    async def _get_carbon_intensity(self) -> float:
        # Simplified: return default or from carbon_manager
        if self.carbon_manager and self.config.carbon_intensity_enabled:
            try:
                intensity_data = await self.carbon_manager.get_current_intensity()
                return intensity_data.get('intensity', 400) / 1000
            except:
                pass
        return 0.4

    async def get_recommendations(self) -> List[MaintenanceRecommendation]:
        async with self._lock:
            return list(self.recommendations.values())

# ============================================================================
# 9. MAIN ORCHESTRATOR (ENHANCED)
# ============================================================================
class PredictiveMaintenanceEngine:
    """
    Orchestrates the entire predictive maintenance pipeline with distillation.
    """

    def __init__(
        self,
        config: Optional[Union[Dict, PredictiveMaintenanceConfig]] = None,
        carbon_manager: Optional[Any] = None,
        lca_client: Optional[Any] = None,
        anomaly_detector: Optional[Any] = None,
    ):
        if config is None:
            self.config = PredictiveMaintenanceConfig()
        elif isinstance(config, dict):
            self.config = PredictiveMaintenanceConfig.from_dict(config)
        else:
            self.config = config

        self.carbon_manager = carbon_manager
        self.lca_client = lca_client
        self.anomaly_detector = anomaly_detector

        self.persistence = PersistenceManager(self.config) if self.config.persistence_enabled else None
        self.tracker = NodeEfficiencyTracker(self.config, self.persistence)
        self.forecaster = PredictiveReflexivity(self.config)
        self.simulator = DigitalTwinSimulator(self.config, carbon_manager, lca_client)
        self.scheduler = MaintenanceScheduler(self.config, carbon_manager)

        # External hooks
        self.telemetry_callback: Optional[Callable] = None
        self.dashboard_callback: Optional[Callable] = None

        # Prometheus metrics
        if PROMETHEUS_AVAILABLE:
            self.metrics = {
                'recommendations': Counter('pm_recommendations_total', ['node', 'action']),
                'analysis_latency': Histogram('pm_analysis_latency_seconds'),
                'nodes_tracked': Gauge('pm_nodes_tracked'),
            }
        else:
            self.metrics = {}

        # Background task for periodic analysis
        self._background_task: Optional[asyncio.Task] = None
        if self.config.refresh_interval > 0:
            self._background_task = asyncio.create_task(self._periodic_analysis())

        logger.info("PredictiveMaintenanceEngine initialized with distillation")

    def register_telemetry_source(self, callback: Callable):
        self.telemetry_callback = callback

    def register_dashboard_callback(self, callback: Callable):
        self.dashboard_callback = callback

    async def update_node(self, node_id: str, flops: float, energy_joules: float) -> None:
        await self.tracker.add_measurement(node_id, flops, energy_joules)

    async def analyze_node(self, node_id: str) -> Optional[MaintenanceRecommendation]:
        start_time = time.time()
        times, effs = await self.tracker.get_efficiency_series(node_id)
        if len(effs) < self.config.min_data_points:
            logger.debug(f"Node {node_id} has insufficient data for forecasting.")
            return None

        forecast = self.forecaster.forecast(times, effs)
        if "error" in forecast:
            logger.warning(f"Forecast error for {node_id}: {forecast['error']}")
            return None

        current_eff = await self.tracker.get_latest_efficiency(node_id)
        if current_eff is None:
            return None

        if self.anomaly_detector and self.config.anomaly_trigger_enabled:
            try:
                anomalies = await self.anomaly_detector.get_recent_anomalies(node_id, minutes=60)
                if anomalies:
                    logger.info(f"Anomalies detected for node {node_id}, forcing analysis")
            except Exception as e:
                logger.warning(f"Anomaly detector error: {e}")

        total_flops = sum(r.flops for r in self.tracker.history.get(node_id, []))
        days = len(self.tracker.history.get(node_id, []))
        avg_flops_per_day = total_flops / max(days, 1) if days > 0 else 1e12

        hardware_model = f"node_{node_id}_model"
        material_index = await self.simulator._get_material_index(hardware_model)

        sim_replace = await self.simulator.simulate_replacement(
            node_id, current_eff, action="replace",
            workload_flops_per_day=avg_flops_per_day,
            hardware_model=hardware_model
        )
        sim_refurb = await self.simulator.simulate_replacement(
            node_id, current_eff, action="refurbish",
            workload_flops_per_day=avg_flops_per_day,
            hardware_model=hardware_model
        )

        # Generate recommendation via scheduler (which uses distillation)
        rec = await self.scheduler.generate_recommendation(
            node_id,
            current_eff,
            forecast,
            sim_replace,
            sim_refurb,
        )

        # Persist recommendation
        if self.persistence:
            await self.persistence.save_recommendation(rec)

        # Update dashboard
        if self.dashboard_callback:
            self.dashboard_callback(rec)

        # Metrics
        if PROMETHEUS_AVAILABLE:
            self.metrics['recommendations'].labels(node=node_id, action=rec.recommended_action).inc()
            self.metrics['analysis_latency'].observe(time.time() - start_time)
            self.metrics['nodes_tracked'].set(len(self.tracker.history))

        logger.info(f"Analysis for node {node_id}: action={rec.recommended_action}, days_to_threshold={rec.days_to_threshold:.1f}")
        return rec

    async def run_analysis(self, node_ids: List[str] = None) -> List[MaintenanceRecommendation]:
        if node_ids is None:
            node_ids = list(self.tracker.history.keys())

        recommendations = []
        for node_id in node_ids:
            rec = await self.analyze_node(node_id)
            if rec:
                recommendations.append(rec)
        return recommendations

    async def _periodic_analysis(self):
        while True:
            try:
                await asyncio.sleep(self.config.refresh_interval)
                await self.run_analysis()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Periodic analysis error: {e}")

    async def get_dashboard_data(self) -> Dict[str, Any]:
        recs = await self.scheduler.get_recommendations()
        data = {
            "total_nodes": len(self.tracker.history),
            "recommendations": [
                {
                    "node_id": r.node_id,
                    "action": r.recommended_action,
                    "suggested_date": r.suggested_date.isoformat(),
                    "carbon_savings_kg": r.carbon_savings_kg,
                    "cost_savings_usd": r.cost_savings_usd,
                    "payback_days": r.payback_days,
                    "current_efficiency": r.current_efficiency,
                    "days_to_threshold": r.days_to_threshold,
                }
                for r in recs
            ],
            "efficiency_threshold": self.config.efficiency_threshold,
        }
        return data

    async def get_health(self) -> Dict[str, Any]:
        return {
            "status": "healthy",
            "dependencies": {
                "persistence": self.persistence is not None,
                "carbon_manager": self.carbon_manager is not None,
                "lca_client": self.lca_client is not None,
                "anomaly_detector": self.anomaly_detector is not None,
            },
            "nodes_tracked": len(self.tracker.history),
            "recommendations_pending": len(self.scheduler.recommendations),
            "distillation_stats": self.scheduler.action_optimizer.get_stats(),
        }

    async def shutdown(self):
        logger.info("Shutting down PredictiveMaintenanceEngine")
        if self._background_task:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass
        if self.persistence:
            await self.persistence.close()
        logger.info("Shutdown complete")


# ============================================================================
# 10. INTEGRATION WITH SUSTAINABILITYDASHBOARD (mock, unchanged)
# ============================================================================
class SustainabilityDashboard:
    def __init__(self):
        self.recommendations = []

    def update(self, rec: MaintenanceRecommendation):
        self.recommendations.append(rec)
        logger.info(f"Dashboard updated with maintenance for {rec.node_id}")


# ============================================================================
# 11. CONVENIENCE FACTORY
# ============================================================================
def create_predictive_maintenance_system(
    config: Optional[Union[Dict, PredictiveMaintenanceConfig]] = None,
    carbon_manager: Optional[Any] = None,
    lca_client: Optional[Any] = None,
    anomaly_detector: Optional[Any] = None,
) -> Dict[str, Any]:
    engine = PredictiveMaintenanceEngine(config, carbon_manager, lca_client, anomaly_detector)
    dashboard = SustainabilityDashboard()
    engine.register_dashboard_callback(dashboard.update)
    return {
        "engine": engine,
        "tracker": engine.tracker,
        "forecaster": engine.forecaster,
        "simulator": engine.simulator,
        "scheduler": engine.scheduler,
        "dashboard": dashboard,
        "persistence": engine.persistence,
    }


# ============================================================================
# 12. REST API (FastAPI) – Optional (unchanged)
# ============================================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Predictive Maintenance API", version="3.1.0")
    engine: Optional[PredictiveMaintenanceEngine] = None

    @app.get("/health")
    async def health():
        if not engine:
            raise HTTPException(503, "Engine not initialized")
        return await engine.get_health()

    @app.get("/nodes")
    async def list_nodes():
        if not engine:
            raise HTTPException(503, "Engine not initialized")
        return {"nodes": list(engine.tracker.history.keys())}

    @app.get("/nodes/{node_id}/status")
    async def node_status(node_id: str):
        if not engine:
            raise HTTPException(503, "Engine not initialized")
        health = await engine.tracker.get_node_health(node_id, engine.config.efficiency_threshold)
        return health

    @app.get("/recommendations")
    async def get_recommendations():
        if not engine:
            raise HTTPException(503, "Engine not initialized")
        return await engine.get_dashboard_data()

    @app.post("/analyze/{node_id}")
    async def analyze_node(node_id: str, background_tasks: BackgroundTasks):
        if not engine:
            raise HTTPException(503, "Engine not initialized")
        background_tasks.add_task(engine.analyze_node, node_id)
        return {"status": "analysis started"}

    @app.on_event("startup")
    async def startup():
        global engine
        engine = PredictiveMaintenanceEngine()
        logger.info("FastAPI startup complete")

    @app.on_event("shutdown")
    async def shutdown():
        if engine:
            await engine.shutdown()
        logger.info("FastAPI shutdown complete")


# ============================================================================
# 13. UNIT TESTS (Phase 10)
# ============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationComponents(IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            'distillation_epsilon': 0.0,
            'distillation_replay_size': 10,
            'distillation_learning_rate': 0.01,
            'distillation_train_every': 10,
        }
        self.optimizer = DistillationActionOptimizer(self.config)

    def test_state_feature_vector(self):
        state = MaintenanceState(
            current_efficiency=2e9,
            slope=-1e7,
            days_to_threshold=5,
            net_savings_replace=1000,
            net_savings_refurbish=500,
            payback_replace=100,
            payback_refurbish=200,
            carbon_intensity=0.4,
            material_index=0.5,
            action_success_rates={'replace':0.8, 'refurbish':0.6, 'monitor':0.4},
            avg_reward=0.7,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 11)

    def test_rule_based_teacher(self):
        teacher = ActionRuleBasedTeacher()
        state = MaintenanceState(
            current_efficiency=2e9,
            slope=-1e7,
            days_to_threshold=0,
            net_savings_replace=1000,
            net_savings_refurbish=500,
            payback_replace=100,
            payback_refurbish=200,
            carbon_intensity=0.4,
            material_index=0.5,
            action_success_rates={},
            avg_reward=0.5,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])  # replace should be highest

    async def test_select_action(self):
        state = MaintenanceState(
            current_efficiency=2e9,
            slope=-1e7,
            days_to_threshold=5,
            net_savings_replace=1000,
            net_savings_refurbish=500,
            payback_replace=100,
            payback_refurbish=200,
            carbon_intensity=0.4,
            material_index=0.5,
            action_success_rates={},
            avg_reward=0.5,
        )
        action, idx, state_vec, teacher_probs = await self.optimizer.select_action(state, exploration=False)
        self.assertIn(action, self.optimizer.ACTIONS)

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(11)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(3)/3)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# 14. OFFLINE TRAINING FOR HISTORICAL ML
# ============================================================================
def train_historical_model(log_path: Path = Path(PredictiveMaintenanceConfig().interaction_logs_path),
                           model_path: Path = Path(PredictiveMaintenanceConfig().historical_model_path)):
    """
    Train a RandomForestClassifier from past interaction logs.
    """
    if not log_path.exists():
        logger.warning(f"Interaction logs not found at {log_path}. No model trained.")
        return

    df_logs = pd.read_csv(log_path)
    if len(df_logs) < 10:
        logger.warning("Not enough logs to train historical model (need at least 10).")
        return

    X_list = []
    y_list = []
    for _, row in df_logs.iterrows():
        state_vec = json.loads(row['state_vector'])
        X_list.append(state_vec)
        y_list.append(row['action'])

    X = np.array(X_list)
    y = np.array(y_list)

    le = LabelEncoder()
    y_encoded = le.fit_transform(y)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y_encoded)

    with open(model_path, 'wb') as f:
        pickle.dump((model, le), f)
    logger.info(f"Historical ML model trained and saved to {model_path}")


# ============================================================================
# 15. EXAMPLE USAGE (unchanged but with distillation)
# ============================================================================
if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)

    async def main():
        system = create_predictive_maintenance_system()
        engine = system["engine"]
        tracker = system["tracker"]

        node = "node-001"
        base_flops = 1e12
        initial_eff = 2.0e9
        for i in range(100):
            eff = initial_eff * (1 - i * 0.01)
            energy = base_flops / eff
            await tracker.add_measurement(node, base_flops, energy)

        recs = await engine.run_analysis([node])
        for rec in recs:
            print(f"Recommendation for {rec.node_id}:")
            print(f"  Action: {rec.recommended_action}")
            print(f"  Suggested date: {rec.suggested_date}")
            print(f"  Carbon savings: {rec.carbon_savings_kg:.2f} kg CO₂")
            print(f"  Cost savings: {rec.cost_savings_usd:.2f} USD")
            print(f"  Payback days: {rec.payback_days:.1f}")
            print(f"  Current efficiency: {rec.current_efficiency:.2e} FLOPs/J")
            print(f"  Days to threshold: {rec.days_to_threshold:.1f}")

        dashboard_data = await engine.get_dashboard_data()
        print("\nDashboard data:")
        print(json.dumps(dashboard_data, indent=2))

        await engine.shutdown()

    asyncio.run(main())
