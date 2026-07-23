# predictive_maintenance.py
"""
Predictive Maintenance for Hardware Based on Sustainability Metrics
===================================================================

Tracks energy efficiency (FLOPs/Joule) per node over time, forecasts when
efficiency will drop below a threshold, simulates replacement impact via
DigitalTwin, and generates maintenance recommendations during low‑carbon periods.

Integrates with:
- NodeEfficiencyTracker (collects and stores efficiency history)
- PredictiveReflexivity (forecasting module)
- DigitalTwin (simulate node replacement)
- MaintenanceScheduler (schedule actions)
- SustainabilityDashboard (reporting)
"""

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Callable
import numpy as np
from collections import deque

logger = logging.getLogger(__name__)

# ============================================================================
# 1. CONFIGURATION
# ============================================================================
PRED_MAINT_CONFIG = {
    # Efficiency threshold (FLOPs/Joule) below which maintenance is triggered
    "efficiency_threshold": 1.0e9,  # 1 GFLOPS/Joule (example)
    # Minimum number of data points to make a forecast
    "min_data_points": 10,
    # Forecast horizon (days) to look ahead for threshold crossing
    "forecast_horizon_days": 30,
    # Confidence interval width (percentage) for forecast
    "forecast_confidence": 0.95,
    # Low‑carbon window: times of day when energy grid is greenest
    "low_carbon_windows": [
        {"start": "02:00", "end": "06:00"},
        {"start": "12:00", "end": "14:00"},
    ],
    # Default replacement efficiency gain (factor)
    "replacement_efficiency_gain": 1.2,  # new node is 20% more efficient
    # Maintenance lead time (days) to schedule before predicted failure
    "maintenance_lead_time": 7,
    # How often to update forecasts (seconds)
    "refresh_interval": 3600,
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
    simulation_result: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# 3. NODE EFFICIENCY TRACKER
# ============================================================================
class NodeEfficiencyTracker:
    """
    Collects and maintains historical efficiency data per node.
    Computes FLOPs/Joule from telemetry.
    """
    def __init__(self, max_history: int = 1000):
        self.history: Dict[str, List[EfficiencyRecord]] = {}
        self.max_history = max_history

    def add_measurement(self, node_id: str, flops: float, energy_joules: float) -> None:
        """Add a new efficiency measurement."""
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

    def get_efficiency_series(self, node_id: str) -> Tuple[np.ndarray, np.ndarray]:
        """Return (timestamps, efficiency_values) as numpy arrays."""
        if node_id not in self.history or len(self.history[node_id]) == 0:
            return np.array([]), np.array([])
        records = self.history[node_id]
        times = np.array([r.timestamp.timestamp() for r in records])
        effs = np.array([r.flops_per_joule for r in records])
        return times, effs

    def get_latest_efficiency(self, node_id: str) -> Optional[float]:
        """Return the most recent efficiency value."""
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
# 4. PREDICTIVE REFLEXIVITY (FORECASTING)
# ============================================================================
class PredictiveReflexivity:
    """
    Forecasts future efficiency based on historical trend.
    Uses linear regression with confidence intervals.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.horizon_days = config.get("forecast_horizon_days", 30)
        self.min_points = config.get("min_data_points", 10)

    def forecast(self, times: np.ndarray, values: np.ndarray) -> Dict[str, Any]:
        """
        Forecast future values for the next N days.
        Returns dict with predictions, confidence interval, and day of threshold crossing.
        """
        if len(values) < self.min_points:
            return {"error": "Insufficient data"}

        # Convert times to days relative to first
        t0 = times[0]
        x = (times - t0) / (24 * 3600)  # days
        y = values

        # Linear regression
        coeffs = np.polyfit(x, y, 1)
        slope = coeffs[0]
        intercept = coeffs[1]

        # Predict for horizon
        last_day = x[-1]
        future_days = np.linspace(last_day, last_day + self.horizon_days, 100)
        future_times = t0 + future_days * 24 * 3600
        predictions = slope * future_days + intercept

        # Confidence interval (simple: based on residual std)
        residuals = y - (slope * x + intercept)
        std_res = np.std(residuals)
        z = 1.96  # 95% confidence
        lower = predictions - z * std_res
        upper = predictions + z * std_res

        # Find when threshold is crossed (if ever)
        threshold = self.config.get("efficiency_threshold", 1e9)
        days_to_threshold = None
        crossing_day = None
        for i, pred in enumerate(predictions):
            if pred < threshold:
                crossing_day = future_days[i]
                days_to_threshold = crossing_day - last_day
                break

        # If already below, days_to_threshold negative or zero
        if predictions[0] < threshold:
            days_to_threshold = 0.0

        return {
            "slope": slope,
            "intercept": intercept,
            "predictions": predictions.tolist(),
            "future_days": future_days.tolist(),
            "lower_bound": lower.tolist(),
            "upper_bound": upper.tolist(),
            "days_to_threshold": days_to_threshold,
            "crossing_day": crossing_day,
        }


# ============================================================================
# 5. DIGITALTWIN SIMULATION FOR REPLACEMENT
# ============================================================================
class DigitalTwinSimulator:
    """
    Simulates the impact of replacing a node with a new one.
    Estimates energy, carbon, and cost savings over a period.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.co2_per_kwh = 0.2  # kg CO₂ per kWh (adjustable)
        self.efficiency_gain = config.get("replacement_efficiency_gain", 1.2)

    def simulate_replacement(
        self,
        node_id: str,
        current_efficiency: float,
        expected_new_efficiency: float = None,
        workload_flops_per_day: float = 1e12,  # example
        simulation_days: int = 365,
    ) -> Dict[str, Any]:
        """
        Simulate the impact of replacing the node.
        Returns energy, CO₂, and cost savings.
        """
        if expected_new_efficiency is None:
            expected_new_efficiency = current_efficiency * self.efficiency_gain

        # Energy per day: FLOPs per day / efficiency
        energy_current = workload_flops_per_day / current_efficiency
        energy_new = workload_flops_per_day / expected_new_efficiency

        energy_saved_per_day = energy_current - energy_new
        energy_saved_total = energy_saved_per_day * simulation_days

        # CO₂ savings: energy (J) -> kWh -> kg CO₂
        # 1 kWh = 3.6e6 J
        co2_saved_total = energy_saved_total / 3.6e6 * self.co2_per_kwh

        # Cost savings (assuming $0.12/kWh)
        cost_saved_total = energy_saved_total / 3.6e6 * 0.12

        return {
            "node_id": node_id,
            "current_efficiency": current_efficiency,
            "new_efficiency": expected_new_efficiency,
            "workload_flops_per_day": workload_flops_per_day,
            "simulation_days": simulation_days,
            "energy_saved_per_day_joules": energy_saved_per_day,
            "energy_saved_total_joules": energy_saved_total,
            "co2_saved_total_kg": co2_saved_total,
            "cost_saved_total_usd": cost_saved_total,
            "payback_days": self._estimate_payback(cost_saved_total, simulation_days),
        }

    def _estimate_payback(self, cost_saved_total: float, simulation_days: int) -> Optional[float]:
        """Estimate payback period in days (assuming hardware cost)."""
        # Hardcode a mock hardware cost; in reality you'd get it from node descriptor.
        hardware_cost = 5000  # USD (example)
        if cost_saved_total <= 0:
            return None
        daily_savings = cost_saved_total / simulation_days
        if daily_savings == 0:
            return None
        return hardware_cost / daily_savings


# ============================================================================
# 6. MAINTENANCE SCHEDULER
# ============================================================================
class MaintenanceScheduler:
    """
    Generates maintenance recommendations and schedules them during low‑carbon windows.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.low_carbon_windows = config.get("low_carbon_windows", [])
        self.lead_time = config.get("maintenance_lead_time", 7)
        self.recommendations: Dict[str, MaintenanceRecommendation] = {}

    def parse_time(self, time_str: str) -> datetime.time:
        """Convert "HH:MM" to time object."""
        return datetime.strptime(time_str, "%H:%M").time()

    def get_next_low_carbon_window(self, from_date: datetime) -> Optional[datetime]:
        """Find the next start time of a low‑carbon window after from_date."""
        today = from_date.date()
        for window in self.low_carbon_windows:
            start_time = self.parse_time(window["start"])
            end_time = self.parse_time(window["end"])
            # Check if window is still available today
            candidate = datetime.combine(today, start_time)
            if candidate > from_date:
                return candidate
        # Otherwise, schedule for tomorrow's first window
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
        threshold = self.config["efficiency_threshold"]
        days_to = forecast_result.get("days_to_threshold")
        slope = forecast_result.get("slope", 0)

        if days_to is None:
            action = "monitor"
            suggested_date = datetime.now() + timedelta(days=30)
        elif days_to <= 0:
            action = "replace_immediately"
            # Schedule for next low‑carbon window
            suggested_date = self.get_next_low_carbon_window(datetime.now())
        elif days_to <= self.lead_time + 7:
            action = "replace_soon"
            # Schedule for the low‑carbon window just before threshold crossing
            # We want to act before the threshold is crossed; schedule lead_time days before crossing
            crossing_date = datetime.now() + timedelta(days=days_to)
            maintenance_date = crossing_date - timedelta(days=self.lead_time)
            # Adjust to nearest low‑carbon window
            suggested_date = self.get_next_low_carbon_window(maintenance_date)
        else:
            action = "monitor"
            suggested_date = datetime.now() + timedelta(days=30)

        # Calculate carbon savings if we replace now (from simulation)
        carbon_savings = simulation_result.get("co2_saved_total_kg", 0.0) if action.startswith("replace") else 0.0

        # Predict efficiency in 30 days (if slope exists)
        if slope != 0:
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
            carbon_savings_kg=carbon_savings,
            simulation_result=simulation_result,
        )

    def schedule_maintenance(self, recommendation: MaintenanceRecommendation) -> None:
        """Store recommendation and (in real implementation) send to scheduling system."""
        self.recommendations[recommendation.node_id] = recommendation
        logger.info(f"Maintenance scheduled for node {recommendation.node_id}: {recommendation.recommended_action} on {recommendation.suggested_date}")

    def get_recommendations(self) -> List[MaintenanceRecommendation]:
        """Return all current recommendations."""
        return list(self.recommendations.values())


# ============================================================================
# 7. MAIN ORCHESTRATOR: PREDICTIVE MAINTENANCE ENGINE
# ============================================================================
class PredictiveMaintenanceEngine:
    """
    Orchestrates the entire predictive maintenance pipeline.
    """
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or PRED_MAINT_CONFIG
        self.tracker = NodeEfficiencyTracker()
        self.forecaster = PredictiveReflexivity(self.config)
        self.simulator = DigitalTwinSimulator(self.config)
        self.scheduler = MaintenanceScheduler(self.config)

        # External hooks
        self.telemetry_callback: Optional[Callable] = None
        self.dashboard_callback: Optional[Callable] = None

    def register_telemetry_source(self, callback: Callable):
        """Register a function that fetches telemetry for a node."""
        self.telemetry_callback = callback

    def register_dashboard_callback(self, callback: Callable):
        """Register a function to update dashboard with recommendations."""
        self.dashboard_callback = callback

    def update_node(self, node_id: str, flops: float, energy_joules: float) -> None:
        """Add new measurement from telemetry."""
        self.tracker.add_measurement(node_id, flops, energy_joules)

    def analyze_node(self, node_id: str) -> Optional[MaintenanceRecommendation]:
        """
        Perform full analysis for a node: forecast, simulate, recommend.
        """
        # Check if we have enough data
        times, effs = self.tracker.get_efficiency_series(node_id)
        if len(effs) < self.config["min_data_points"]:
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

        # Simulate replacement
        # Estimate workload (FLOPs/day) from historical data
        # For simplicity, average daily FLOPs
        total_flops = sum(r.flops for r in self.tracker.history.get(node_id, []))
        days = len(self.tracker.history.get(node_id, []))
        avg_flops_per_day = total_flops / max(days, 1) if days > 0 else 1e12

        simulation = self.simulator.simulate_replacement(
            node_id,
            current_eff,
            workload_flops_per_day=avg_flops_per_day,
            simulation_days=365,  # 1 year
        )

        # Generate recommendation
        rec = self.scheduler.generate_recommendation(
            node_id,
            current_eff,
            forecast,
            simulation,
        )

        # Schedule
        self.scheduler.schedule_maintenance(rec)

        # Update dashboard if callback registered
        if self.dashboard_callback:
            self.dashboard_callback(rec)

        return rec

    def run_analysis(self, node_ids: List[str] = None) -> List[MaintenanceRecommendation]:
        """
        Run analysis on all nodes (or a subset) and return recommendations.
        """
        if node_ids is None:
            node_ids = list(self.tracker.history.keys())

        recommendations = []
        for node_id in node_ids:
            rec = self.analyze_node(node_id)
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
                    "current_efficiency": r.current_efficiency,
                }
                for r in recs
            ],
            "efficiency_threshold": self.config["efficiency_threshold"],
        }
        return data


# ============================================================================
# 8. INTEGRATION WITH SUSTAINABILITYDASHBOARD (MOCK)
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
# 9. CONVENIENCE FACTORY
# ============================================================================
def create_predictive_maintenance_system(config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Factory to create all components and wire them together.
    """
    if config is None:
        config = PRED_MAINT_CONFIG

    engine = PredictiveMaintenanceEngine(config)
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
    }


# ============================================================================
# 10. EXAMPLE USAGE (if run directly)
# ============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Create system
    system = create_predictive_maintenance_system()
    engine = system["engine"]
    tracker = system["tracker"]

    # Simulate some data for a node (degrading efficiency)
    node = "node-001"
    base_flops = 1e12  # FLOPs per measurement
    initial_eff = 2.0e9  # FLOPs/Joule
    # Simulate degradation over 100 measurements (efficiency decreases linearly)
    for i in range(100):
        # Efficiency drops by 1% each step
        eff = initial_eff * (1 - i * 0.01)
        # Energy = FLOPs / efficiency
        energy = base_flops / eff
        tracker.add_measurement(node, base_flops, energy)
        # Simulate time passing (e.g., one hour per measurement)
        # Not needed for demo

    # Run analysis
    recs = engine.run_analysis([node])
    for rec in recs:
        print(f"Recommendation for {rec.node_id}:")
        print(f"  Action: {rec.recommended_action}")
        print(f"  Suggested date: {rec.suggested_date}")
        print(f"  Carbon savings: {rec.carbon_savings_kg:.2f} kg CO₂")
        print(f"  Current efficiency: {rec.current_efficiency:.2e} FLOPs/J")
        print(f"  Days to threshold: {rec.days_to_threshold:.1f}")
        print(f"  Simulation result: {rec.simulation_result}")

    # Get dashboard data
    dashboard_data = engine.get_dashboard_data()
    print("\nDashboard data:")
    print(json.dumps(dashboard_data, indent=2))
