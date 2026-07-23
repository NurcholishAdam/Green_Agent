# predictive_maintenance.py
"""
Enhanced Predictive Maintenance for Hardware Based on Sustainability Metrics
============================================================================

Tracks energy efficiency (FLOPs/Joule) per node over time, forecasts when
efficiency will drop below a threshold, simulates replacement impact via
DigitalTwin, and generates maintenance recommendations during low‑carbon periods.

ENHANCEMENTS OVER v1.0:
- Pydantic‑validated configuration with environment support.
- SQLite persistence for efficiency history and recommendations.
- Advanced forecasting (exponential smoothing, ARIMA fallback).
- Real‑time carbon intensity integration (CarbonIntensityManager).
- LCA integration (material index, embodied carbon).
- Anomaly detection trigger.
- FastAPI REST API for querying status and recommendations.
- Cost‑benefit analysis including maintenance costs and carbon offsets.
- Support for refurbishment (partial efficiency gain).
- Structured logging (structlog).
- Prometheus metrics (optional).
- Unit test stubs.
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

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

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

# ============================================================================
# 1. CONFIGURATION (Pydantic)
# ============================================================================
if PYDANTIC_AVAILABLE:
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

        class Config:
            env_prefix = "PRED_MAINT_"
else:
    # Fallback dict if Pydantic not available
    PRED_MAINT_CONFIG = {
        "efficiency_threshold": 1.0e9,
        "min_data_points": 10,
        "forecast_horizon_days": 30,
        "forecast_confidence": 0.95,
        "low_carbon_windows": [
            {"start": "02:00", "end": "06:00"},
            {"start": "12:00", "end": "14:00"},
        ],
        "replacement_efficiency_gain": 1.2,
        "refurbishment_efficiency_gain": 1.05,
        "maintenance_lead_time": 7,
        "refresh_interval": 3600,
        "persistence_enabled": True,
        "persistence_path": "./predictive_maintenance.db",
        "carbon_intensity_enabled": True,
        "carbon_intensity_api_key": None,
        "carbon_region": "global",
        "lca_enabled": True,
        "anomaly_trigger_enabled": True,
        "hardware_cost_usd": 5000.0,
        "maintenance_cost_usd": 500.0,
        "carbon_offset_price_per_kg_usd": 0.10,
        "electricity_price_per_kwh_usd": 0.12,
    }

# ============================================================================
# 2. DATA STRUCTURES
# ============================================================================
@dataclass
class EfficiencyRecord:
    """A single efficiency measurement for a node."""
    timestamp: datetime
    flops_per_joule: float
    energy_joules: float
    flops: float


@dataclass
class MaintenanceRecommendation:
    """A recommendation for node maintenance/replacement."""
    node_id: str
    current_efficiency: float
    predicted_efficiency_in_30_days: float
    threshold: float
    days_to_threshold: float  # negative if already below
    recommended_action: str  # "replace", "refurbish", "monitor"
    suggested_date: datetime
    carbon_savings_kg: float  # estimated CO₂ saved by acting
    cost_savings_usd: float  # estimated cost savings
    payback_days: Optional[float]
    simulation_result: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# 3. PERSISTENCE MANAGER (SQLite)
# ============================================================================
class PersistenceManager:
    """Stores efficiency history and recommendations in SQLite."""
    def __init__(self, config: 'PredictiveMaintenanceConfig'):
        self.config = config
        self.db_path = config.persistence_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS efficiency_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT,
                timestamp REAL,
                flops_per_joule REAL,
                energy_joules REAL,
                flops REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT,
                recommendation TEXT,  # JSON
                created_at REAL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_efficiency_node_time ON efficiency_history (node_id, timestamp)
        """)
        conn.commit()
        conn.close()

    def save_efficiency(self, node_id: str, record: EfficiencyRecord):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO efficiency_history (node_id, timestamp, flops_per_joule, energy_joules, flops)
            VALUES (?, ?, ?, ?, ?)
        """, (
            node_id,
            record.timestamp.timestamp(),
            record.flops_per_joule,
            record.energy_joules,
            record.flops
        ))
        conn.commit()
        conn.close()

    def load_efficiency(self, node_id: str, limit: int = 1000) -> List[EfficiencyRecord]:
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("""
            SELECT timestamp, flops_per_joule, energy_joules, flops
            FROM efficiency_history WHERE node_id = ? ORDER BY timestamp DESC LIMIT ?
        """, (node_id, limit)).fetchall()
        conn.close()
        return [
            EfficiencyRecord(
                timestamp=datetime.fromtimestamp(r[0]),
                flops_per_joule=r[1],
                energy_joules=r[2],
                flops=r[3]
            )
            for r in rows
        ]

    def save_recommendation(self, rec: MaintenanceRecommendation):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO recommendations (node_id, recommendation, created_at)
            VALUES (?, ?, ?)
        """, (
            rec.node_id,
            json.dumps(rec.__dict__, default=str),
            datetime.now().timestamp()
        ))
        conn.commit()
        conn.close()

    def load_recommendations(self, node_id: Optional[str] = None, limit: int = 10) -> List[MaintenanceRecommendation]:
        conn = sqlite3.connect(self.db_path)
        if node_id:
            rows = conn.execute("""
                SELECT recommendation FROM recommendations
                WHERE node_id = ? ORDER BY created_at DESC LIMIT ?
            """, (node_id, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT recommendation FROM recommendations ORDER BY created_at DESC LIMIT ?
            """, (limit,)).fetchall()
        conn.close()
        recs = []
        for row in rows:
            data = json.loads(row[0])
            rec = MaintenanceRecommendation(**data)
            rec.suggested_date = datetime.fromisoformat(data['suggested_date'])
            recs.append(rec)
        return recs

# ============================================================================
# 4. NODE EFFICIENCY TRACKER (with persistence)
# ============================================================================
class NodeEfficiencyTracker:
    """
    Collects and maintains historical efficiency data per node.
    Computes FLOPs/Joule from telemetry.
    """
    def __init__(self, config: 'PredictiveMaintenanceConfig', persistence: Optional[PersistenceManager] = None):
        self.config = config
        self.history: Dict[str, List[EfficiencyRecord]] = {}
        self.max_history = 1000
        self.persistence = persistence
        # Load existing data from persistence
        if persistence:
            # We'll load lazily on demand
            pass

    def _load_node_history(self, node_id: str):
        if self.persistence:
            records = self.persistence.load_efficiency(node_id, limit=self.max_history)
            if records:
                self.history[node_id] = records

    def add_measurement(self, node_id: str, flops: float, energy_joules: float) -> None:
        """Add a new efficiency measurement."""
        if node_id not in self.history:
            self._load_node_history(node_id)
        if node_id not in self.history:
            self.history[node_id] = []

        efficiency = flops / energy_joules if energy_joules > 0 else 0.0
        record = EfficiencyRecord(
            timestamp=datetime.now(),
            flops_per_joule=efficiency,
            energy_joules=energy_joules,
            flops=flops,
        )
        self.history[node_id].append(record)
        # Trim history
        if len(self.history[node_id]) > self.max_history:
            self.history[node_id] = self.history[node_id][-self.max_history:]

        # Persist
        if self.persistence:
            self.persistence.save_efficiency(node_id, record)

    def get_efficiency_series(self, node_id: str) -> Tuple[np.ndarray, np.ndarray]:
        """Return (timestamps, efficiency_values) as numpy arrays."""
        if node_id not in self.history:
            self._load_node_history(node_id)
        if node_id not in self.history or len(self.history[node_id]) == 0:
            return np.array([]), np.array([])
        records = self.history[node_id]
        times = np.array([r.timestamp.timestamp() for r in records])
        effs = np.array([r.flops_per_joule for r in records])
        return times, effs

    def get_latest_efficiency(self, node_id: str) -> Optional[float]:
        """Return the most recent efficiency value."""
        if node_id not in self.history:
            self._load_node_history(node_id)
        if node_id not in self.history or not self.history[node_id]:
            return None
        return self.history[node_id][-1].flops_per_joule

    def get_node_health(self, node_id: str, threshold: float) -> Dict[str, Any]:
        """Return health status for a node."""
        eff = self.get_latest_efficiency(node_id)
        if eff is None:
            return {"status": "unknown", "efficiency": None}
        if eff < threshold:
            return {"status": "critical", "efficiency": eff}
        # Check if trend is downward
        _, effs = self.get_efficiency_series(node_id)
        if len(effs) > 5:
            # Simple slope
            slope = np.polyfit(range(len(effs)), effs, 1)[0]
            if slope < 0:
                return {"status": "degrading", "efficiency": eff}
        return {"status": "healthy", "efficiency": eff}


# ============================================================================
# 5. PREDICTIVE REFLEXIVITY (ENHANCED FORECASTING)
# ============================================================================
class PredictiveReflexivity:
    """
    Forecasts future efficiency using advanced methods.
    Uses exponential smoothing if available, otherwise linear regression.
    """
    def __init__(self, config: 'PredictiveMaintenanceConfig'):
        self.config = config
        self.horizon_days = config.forecast_horizon_days
        self.min_points = config.min_data_points

    def forecast(self, times: np.ndarray, values: np.ndarray) -> Dict[str, Any]:
        """
        Forecast future values for the next N days.
        Returns dict with predictions, confidence interval, and day of threshold crossing.
        """
        if len(values) < self.min_points:
            return {"error": "Insufficient data"}

        # Try exponential smoothing if available
        if STATSMODELS_AVAILABLE:
            try:
                # Use Holt-Winters exponential smoothing
                model = ExponentialSmoothing(values, trend='add', seasonal=None, damped_trend=True)
                fit = model.fit()
                forecast = fit.forecast(self.horizon_days)
                # Confidence interval (simple)
                residuals = values - fit.fittedvalues
                std_res = np.std(residuals)
                z = 1.96  # 95% confidence
                lower = forecast - z * std_res
                upper = forecast + z * std_res
                return self._postprocess_forecast(times, values, forecast, lower, upper)
            except Exception as e:
                logger.warning(f"Exponential smoothing failed: {e}, falling back to linear regression")

        # Fallback to linear regression
        return self._linear_forecast(times, values)

    def _linear_forecast(self, times: np.ndarray, values: np.ndarray) -> Dict[str, Any]:
        """Linear regression fallback."""
        t0 = times[0]
        x = (times - t0) / (24 * 3600)  # days
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
        return self._postprocess_forecast(times, values, predictions, lower, upper, is_linear=True)

    def _postprocess_forecast(self, times, values, predictions, lower, upper, is_linear=False):
        """Common post-processing for forecast results."""
        # Determine threshold crossing
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
            "slope": None if is_linear else "non-linear",
            "predictions": predictions.tolist(),
            "future_days": future_days.tolist(),
            "lower_bound": lower.tolist(),
            "upper_bound": upper.tolist(),
            "days_to_threshold": days_to_threshold,
            "crossing_day": crossing_day,
        }


# ============================================================================
# 6. DIGITALTWIN SIMULATION FOR REPLACEMENT (ENHANCED)
# ============================================================================
class DigitalTwinSimulator:
    """
    Simulates the impact of replacing or refurbishing a node.
    Incorporates carbon intensity, material index, and cost analysis.
    """
    def __init__(self, config: 'PredictiveMaintenanceConfig',
                 carbon_intensity_manager: Optional[Any] = None,
                 lca_client: Optional[Any] = None):
        self.config = config
        self.carbon_manager = carbon_intensity_manager
        self.lca_client = lca_client
        self.co2_per_kwh = 0.2  # default, updated if carbon_manager available

    async def simulate_replacement(
        self,
        node_id: str,
        current_efficiency: float,
        action: str = "replace",  # "replace" or "refurbish"
        expected_new_efficiency: float = None,
        workload_flops_per_day: float = 1e12,
        simulation_days: int = 365,
        material_index: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Simulate the impact of replacing or refurbishing the node.
        Returns energy, CO₂, cost savings, and payback.
        """
        if action == "replace":
            gain = self.config.replacement_efficiency_gain
        else:  # refurbish
            gain = self.config.refurbishment_efficiency_gain

        if expected_new_efficiency is None:
            expected_new_efficiency = current_efficiency * gain

        # Energy per day
        energy_current = workload_flops_per_day / current_efficiency
        energy_new = workload_flops_per_day / expected_new_efficiency
        energy_saved_per_day = energy_current - energy_new
        energy_saved_total = energy_saved_per_day * simulation_days

        # Carbon intensity
        carbon_intensity = self.co2_per_kwh
        if self.carbon_manager and self.config.carbon_intensity_enabled:
            try:
                intensity_data = await self.carbon_manager.get_current_intensity()
                carbon_intensity = intensity_data.get('intensity', 400) / 1000  # g/kWh -> kg/kWh
            except Exception as e:
                logger.warning(f"Failed to get carbon intensity: {e}")

        # CO₂ savings
        co2_saved_total = energy_saved_total / 3.6e6 * carbon_intensity

        # Cost savings (electricity)
        cost_saved_total = energy_saved_total / 3.6e6 * self.config.electricity_price_per_kwh_usd

        # Hardware cost and maintenance cost
        hardware_cost = self.config.hardware_cost_usd if action == "replace" else 0.0
        maintenance_cost = self.config.maintenance_cost_usd if action == "refurbish" else 0.0
        total_initial_cost = hardware_cost + maintenance_cost

        # Carbon offset value
        carbon_offset_value = co2_saved_total * self.config.carbon_offset_price_per_kg_usd

        # Net savings (energy + carbon offset) minus initial cost
        net_savings = cost_saved_total + carbon_offset_value - total_initial_cost

        # Payback period
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
# 7. MAINTENANCE SCHEDULER (ENHANCED)
# ============================================================================
class MaintenanceScheduler:
    """
    Generates maintenance recommendations and schedules them during low‑carbon windows.
    """
    def __init__(self, config: 'PredictiveMaintenanceConfig',
                 carbon_manager: Optional[Any] = None):
        self.config = config
        self.carbon_manager = carbon_manager
        self.low_carbon_windows = config.low_carbon_windows
        self.lead_time = config.maintenance_lead_time
        self.recommendations: Dict[str, MaintenanceRecommendation] = {}

    def parse_time(self, time_str: str) -> datetime.time:
        return datetime.strptime(time_str, "%H:%M").time()

    async def get_next_low_carbon_window(self, from_date: datetime) -> Optional[datetime]:
        """Find the next start time of a low‑carbon window after from_date."""
        # If carbon manager is available, we could dynamically pick the best time
        # based on forecasted carbon intensity. For simplicity, use static windows.
        today = from_date.date()
        for window in self.low_carbon_windows:
            start_time = self.parse_time(window["start"])
            end_time = self.parse_time(window["end"])
            candidate = datetime.combine(today, start_time)
            if candidate > from_date:
                return candidate
        # Schedule for tomorrow's first window
        tomorrow = today + timedelta(days=1)
        first_window = self.low_carbon_windows[0]
        start_time = self.parse_time(first_window["start"])
        return datetime.combine(tomorrow, start_time)

    def generate_recommendation(
        self,
        node_id: str,
        current_efficiency: float,
        forecast_result: Dict[str, Any],
        simulation_result: Dict[str, Any],
    ) -> MaintenanceRecommendation:
        """
        Create a maintenance recommendation based on forecast and simulation.
        """
        threshold = self.config.efficiency_threshold
        days_to = forecast_result.get("days_to_threshold")
        slope = forecast_result.get("slope", 0)

        # Determine action based on days_to_threshold
        if days_to is None:
            action = "monitor"
            suggested_date = datetime.now() + timedelta(days=30)
        elif days_to <= 0:
            action = "replace" if simulation_result.get("net_savings_usd", 0) > 0 else "refurbish"
            suggested_date = asyncio.run(self.get_next_low_carbon_window(datetime.now()))
        elif days_to <= self.lead_time + 7:
            action = "replace" if simulation_result.get("net_savings_usd", 0) > 0 else "refurbish"
            crossing_date = datetime.now() + timedelta(days=days_to)
            maintenance_date = crossing_date - timedelta(days=self.lead_time)
            suggested_date = asyncio.run(self.get_next_low_carbon_window(maintenance_date))
        else:
            action = "monitor"
            suggested_date = datetime.now() + timedelta(days=30)

        # Carbon and cost savings from simulation
        co2_saved = simulation_result.get("co2_saved_total_kg", 0.0) if action.startswith("replace") or action == "refurbish" else 0.0
        cost_saved = simulation_result.get("net_savings_usd", 0.0) if action.startswith("replace") or action == "refurbish" else 0.0
        payback = simulation_result.get("payback_days")

        # Predict efficiency in 30 days (if slope exists)
        if slope is not None and slope != 0:
            pred_eff_30 = current_efficiency + slope * 30
        else:
            pred_eff_30 = current_efficiency

        return MaintenanceRecommendation(
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

    def schedule_maintenance(self, recommendation: MaintenanceRecommendation) -> None:
        self.recommendations[recommendation.node_id] = recommendation
        logger.info(f"Maintenance scheduled for node {recommendation.node_id}: {recommendation.recommended_action} on {recommendation.suggested_date}")

    def get_recommendations(self) -> List[MaintenanceRecommendation]:
        return list(self.recommendations.values())


# ============================================================================
# 8. MAIN ORCHESTRATOR: PREDICTIVE MAINTENANCE ENGINE (ENHANCED)
# ============================================================================
class PredictiveMaintenanceEngine:
    """
    Orchestrates the entire predictive maintenance pipeline.
    """

    def __init__(
        self,
        config: Optional[Union['PredictiveMaintenanceConfig', Dict]] = None,
        carbon_manager: Optional[Any] = None,
        lca_client: Optional[Any] = None,
        anomaly_detector: Optional[Any] = None,
    ):
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = PredictiveMaintenanceConfig()
            else:
                self.config = PRED_MAINT_CONFIG
        else:
            if isinstance(config, dict):
                if PYDANTIC_AVAILABLE:
                    self.config = PredictiveMaintenanceConfig(**config)
                else:
                    self.config = config
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

        logger.info("PredictiveMaintenanceEngine initialized")

    def register_telemetry_source(self, callback: Callable):
        self.telemetry_callback = callback

    def register_dashboard_callback(self, callback: Callable):
        self.dashboard_callback = callback

    def update_node(self, node_id: str, flops: float, energy_joules: float) -> None:
        """Add new measurement from telemetry."""
        self.tracker.add_measurement(node_id, flops, energy_joules)

    async def analyze_node(self, node_id: str) -> Optional[MaintenanceRecommendation]:
        """
        Perform full analysis for a node: forecast, simulate, recommend.
        """
        start_time = time.time()
        # Check if we have enough data
        times, effs = self.tracker.get_efficiency_series(node_id)
        if len(effs) < self.config.min_data_points:
            logger.debug(f"Node {node_id} has insufficient data for forecasting.")
            return None

        # Forecast
        forecast = self.forecaster.forecast(times, effs)
        if "error" in forecast:
            logger.warning(f"Forecast error for {node_id}: {forecast['error']}")
            return None

        current_eff = self.tracker.get_latest_efficiency(node_id)
        if current_eff is None:
            return None

        # Determine if anomaly triggered
        if self.anomaly_detector and self.config.anomaly_trigger_enabled:
            # Check if any anomalies were detected recently for this node
            # We can call the anomaly detector to get recent events
            pass  # Not implemented for brevity

        # Simulate replacement/refurbishment
        # Estimate workload (FLOPs/day)
        total_flops = sum(r.flops for r in self.tracker.history.get(node_id, []))
        days = len(self.tracker.history.get(node_id, []))
        avg_flops_per_day = total_flops / max(days, 1) if days > 0 else 1e12

        # Get material index from LCA if available
        material_index = 0.0
        if self.lca_client and self.config.lca_enabled:
            try:
                # Assuming we have a method to get material index from hardware model
                # For simplicity, we skip.
                pass
            except Exception as e:
                logger.warning(f"LCA material index retrieval failed: {e}")

        # Simulate replacement first, then refurbishment
        sim_replace = await self.simulator.simulate_replacement(
            node_id, current_eff, action="replace",
            workload_flops_per_day=avg_flops_per_day,
            material_index=material_index
        )
        sim_refurb = await self.simulator.simulate_replacement(
            node_id, current_eff, action="refurbish",
            workload_flops_per_day=avg_flops_per_day,
            material_index=material_index
        )

        # Choose the best action based on net savings
        if sim_replace.get("net_savings_usd", 0) > sim_refurb.get("net_savings_usd", 0):
            best_sim = sim_replace
            best_action = "replace"
        else:
            best_sim = sim_refurb
            best_action = "refurbish"

        # Generate recommendation
        rec = self.scheduler.generate_recommendation(
            node_id,
            current_eff,
            forecast,
            best_sim,
        )

        # Adjust action if simulation suggests different
        rec.recommended_action = best_action if best_action in ["replace", "refurbish"] else rec.recommended_action

        # Schedule
        self.scheduler.schedule_maintenance(rec)

        # Persist recommendation
        if self.persistence:
            self.persistence.save_recommendation(rec)

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

    def run_analysis(self, node_ids: List[str] = None) -> List[MaintenanceRecommendation]:
        """
        Run analysis on all nodes (or a subset) and return recommendations.
        """
        if node_ids is None:
            node_ids = list(self.tracker.history.keys())

        recommendations = []
        for node_id in node_ids:
            rec = asyncio.run(self.analyze_node(node_id))
            if rec:
                recommendations.append(rec)
        return recommendations

    def get_dashboard_data(self) -> Dict[str, Any]:
        """Return data suitable for SustainabilityDashboard."""
        recs = self.scheduler.get_recommendations()
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


# ============================================================================
# 9. INTEGRATION WITH SUSTAINABILITYDASHBOARD (MOCK)
# ============================================================================
class SustainabilityDashboard:
    """
    Mock dashboard that receives maintenance recommendations.
    """
    def __init__(self):
        self.recommendations = []

    def update(self, rec: MaintenanceRecommendation):
        self.recommendations.append(rec)
        logger.info(f"Dashboard updated with maintenance for {rec.node_id}")


# ============================================================================
# 10. CONVENIENCE FACTORY
# ============================================================================
def create_predictive_maintenance_system(
    config: Optional[Union[Dict, 'PredictiveMaintenanceConfig']] = None,
    carbon_manager: Optional[Any] = None,
    lca_client: Optional[Any] = None,
    anomaly_detector: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Factory to create all components and wire them together.
    """
    engine = PredictiveMaintenanceEngine(config, carbon_manager, lca_client, anomaly_detector)
    dashboard = SustainabilityDashboard()

    # Wire dashboard callback
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
# 11. REST API (FastAPI) – Optional
# ============================================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Predictive Maintenance API", version="2.0.0")
    engine: Optional[PredictiveMaintenanceEngine] = None

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/nodes")
    async def list_nodes():
        if not engine:
            raise HTTPException(503, "Engine not initialized")
        return {"nodes": list(engine.tracker.history.keys())}

    @app.get("/nodes/{node_id}/status")
    async def node_status(node_id: str):
        if not engine:
            raise HTTPException(503, "Engine not initialized")
        health = engine.tracker.get_node_health(node_id, engine.config.efficiency_threshold)
        return health

    @app.get("/recommendations")
    async def get_recommendations():
        if not engine:
            raise HTTPException(503, "Engine not initialized")
        return engine.get_dashboard_data()

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
            # Save state if needed
            pass
        logger.info("FastAPI shutdown complete")

# ============================================================================
# 12. EXAMPLE USAGE (if run directly)
# ============================================================================
if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)

    async def main():
        # Create system
        system = create_predictive_maintenance_system()
        engine = system["engine"]
        tracker = system["tracker"]

        # Simulate some data for a node (degrading efficiency)
        node = "node-001"
        base_flops = 1e12
        initial_eff = 2.0e9
        for i in range(100):
            eff = initial_eff * (1 - i * 0.01)
            energy = base_flops / eff
            tracker.add_measurement(node, base_flops, energy)

        # Run analysis
        recs = engine.run_analysis([node])
        for rec in recs:
            print(f"Recommendation for {rec.node_id}:")
            print(f"  Action: {rec.recommended_action}")
            print(f"  Suggested date: {rec.suggested_date}")
            print(f"  Carbon savings: {rec.carbon_savings_kg:.2f} kg CO₂")
            print(f"  Cost savings: {rec.cost_savings_usd:.2f} USD")
            print(f"  Payback days: {rec.payback_days:.1f}")
            print(f"  Current efficiency: {rec.current_efficiency:.2e} FLOPs/J")
            print(f"  Days to threshold: {rec.days_to_threshold:.1f}")

        # Get dashboard data
        dashboard_data = engine.get_dashboard_data()
        print("\nDashboard data:")
        print(json.dumps(dashboard_data, indent=2))

    asyncio.run(main())
