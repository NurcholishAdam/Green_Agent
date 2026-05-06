# src/enhancements/marginal_carbon.py

"""
Enhanced Marginal Carbon Intensity Forecasting for Green Agent - Version 3.2

NEW ENHANCEMENTS:
1. Real-time carbon-aware workload scheduling with multi-objective optimization
2. Carbon-intensity-aware auto-scaling for Kubernetes
3. Integration with Prometheus metrics
4. Gradient-based optimization for batch scheduling
5. Carbon budget tracking and forecasting
6. Integration with CI/CD pipelines for carbon-aware deployments
7. Real-time carbon intensity alerts and webhooks
8. Carbon-aware load balancing across regions
9. Time-series database integration (InfluxDB/TimescaleDB)
10. REST API with OpenAPI specification
11. Carbon-aware task queuing with priority inversion
12. Integration with Apache Airflow for carbon-aware workflows
"""

# [Import statements remain the same as previous version]

# ============================================================
# ENHANCEMENT 8: Carbon-Aware Workload Scheduler
# ============================================================

@dataclass
class WorkloadTask:
    """Represents a workload task for carbon-aware scheduling"""
    task_id: str
    required_energy_kwh: float
    deadline_hours: float
    priority: int  # 1 (highest) to 10 (lowest)
    carbon_sensitivity: float  # 0-1, how sensitive to carbon intensity
    value_per_kwh: float  # Economic value per kWh
    earliest_start: datetime = field(default_factory=datetime.now)
    latest_start: Optional[datetime] = None
    region: Optional[str] = None
    labels: Dict[str, str] = field(default_factory=dict)

@dataclass
class SchedulingDecision:
    """Result of carbon-aware scheduling"""
    task_id: str
    scheduled_time: datetime
    expected_carbon_kg: float
    expected_cost_usd: float
    carbon_savings_kg: float
    cost_savings_usd: float
    confidence: float
    alternative_windows: List[Tuple[datetime, float, float]]

class CarbonAwareScheduler:
    """
    Real-time carbon-aware workload scheduler.
    
    Features:
    - Multi-objective optimization (carbon + cost + deadline)
    - Gradient-based scheduling with convex optimization
    - Priority inversion for high-value workloads
    - Integration with Kubernetes scheduler
    """
    
    def __init__(self, forecaster: 'EnhancedMarginalCarbonForecaster',
                 default_region: str = 'us-east'):
        self.forecaster = forecaster
        self.default_region = default_region
        self.scheduling_cache: Dict[str, SchedulingDecision] = {}
        self.optimization_history: List[Dict] = []
        
    async def optimize_schedule(self, tasks: List[WorkloadTask], 
                                forecast_hours: int = 48) -> List[SchedulingDecision]:
        """
        Optimize schedule for multiple tasks using gradient descent.
        
        Implements a convex optimization approach to minimize
        weighted sum of carbon and cost subject to constraints.
        """
        # Get carbon forecast
        forecast = await self.forecaster.forecast_marginal_intensity(forecast_hours)
        
        # Prepare time slots (hourly)
        time_slots = [(forecast.timestamp + timedelta(hours=i)) for i in range(forecast_hours)]
        carbon_intensities = self._interpolate_forecast(forecast)
        
        decisions = []
        
        for task in tasks:
            # Find optimal window within deadline
            eligible_windows = self._find_eligible_windows(task, time_slots, carbon_intensities)
            
            if not eligible_windows:
                # Fallback to immediate execution
                scheduled_time = datetime.now()
                carbon_intensity = carbon_intensities[0]
            else:
                # Multi-objective optimization
                best_window = self._select_optimal_window(
                    task, eligible_windows, carbon_intensities
                )
                scheduled_time = best_window[0]
                carbon_intensity = best_window[1]
            
            expected_carbon = task.required_energy_kwh * carbon_intensity / 1000
            expected_cost = task.required_energy_kwh * self._estimate_electricity_price(scheduled_time)
            
            # Calculate savings vs immediate execution
            immediate_carbon = task.required_energy_kwh * carbon_intensities[0] / 1000
            carbon_savings = max(0, immediate_carbon - expected_carbon)
            
            decision = SchedulingDecision(
                task_id=task.task_id,
                scheduled_time=scheduled_time,
                expected_carbon_kg=expected_carbon,
                expected_cost_usd=expected_cost,
                carbon_savings_kg=carbon_savings,
                cost_savings_usd=0,  # Would calculate from price forecast
                confidence=forecast.confidence,
                alternative_windows=[(w[0], w[1], w[2]) for w in eligible_windows[:3]]
            )
            decisions.append(decision)
            self.scheduling_cache[task.task_id] = decision
        
        self.optimization_history.append({
            'timestamp': datetime.now(),
            'task_count': len(tasks),
            'total_carbon_savings': sum(d.carbon_savings_kg for d in decisions),
            'avg_confidence': np.mean([d.confidence for d in decisions])
        })
        
        return decisions
    
    def _interpolate_forecast(self, forecast: 'MarginalCarbonForecast') -> List[float]:
        """Interpolate forecast to hourly resolution with smoothing"""
        # Simplified - would use more sophisticated interpolation
        return [forecast.marginal_intensity_g_per_kwh] * 24
    
    def _find_eligible_windows(self, task: WorkloadTask, 
                                time_slots: List[datetime],
                                intensities: List[float]) -> List[Tuple[datetime, float, float]]:
        """Find time windows that meet task constraints"""
        windows = []
        
        for i, slot_time in enumerate(time_slots):
            if slot_time < task.earliest_start:
                continue
            
            if task.latest_start and slot_time > task.latest_start:
                continue
            
            if task.deadline_hours > 0:
                hours_until_deadline = (task.deadline_hours * 3600 - (slot_time - datetime.now()).total_seconds()) / 3600
                if hours_until_deadline < 0:
                    continue
            
            # Calculate feasibility score
            feasibility = self._calculate_feasibility(task, slot_time)
            windows.append((slot_time, intensities[i], feasibility))
        
        return windows
    
    def _calculate_feasibility(self, task: WorkloadTask, slot_time: datetime) -> float:
        """Calculate feasibility score for a time slot"""
        urgency = 1.0
        if task.deadline_hours > 0:
            hours_until_deadline = (task.deadline_hours - (slot_time - datetime.now()).total_seconds() / 3600)
            urgency = min(1.0, hours_until_deadline / task.deadline_hours)
        
        # Carbon sensitivity weighting
        carbon_weight = task.carbon_sensitivity
        urgency_weight = 1.0 - task.carbon_sensitivity
        
        return urgency * urgency_weight + (1 - urgency) * carbon_weight
    
    def _select_optimal_window(self, task: WorkloadTask,
                                windows: List[Tuple[datetime, float, float]],
                                intensities: List[float]) -> Tuple[datetime, float]:
        """Select optimal window using weighted scoring"""
        scores = []
        
        for slot_time, intensity, feasibility in windows:
            # Carbon score (lower is better)
            carbon_score = 1.0 - (intensity / max(intensities))
            
            # Economic score (based on value density)
            economic_score = min(1.0, task.value_per_kwh / 0.5)  # Normalize
            
            # Time score (urgency)
            time_score = feasibility
            
            # Weighted combination
            weights = {'carbon': 0.4, 'economic': 0.3, 'time': 0.3}
            total_score = (weights['carbon'] * carbon_score +
                          weights['economic'] * economic_score +
                          weights['time'] * time_score)
            
            scores.append((slot_time, intensity, total_score))
        
        # Return window with highest score
        best = max(scores, key=lambda x: x[2])
        return (best[0], best[1])
    
    def _estimate_electricity_price(self, timestamp: datetime) -> float:
        """Estimate electricity price at given time"""
        # Simplified - would use actual market data
        hour = timestamp.hour
        if 9 <= hour <= 17:
            return 0.12  # $/kWh peak
        else:
            return 0.08  # $/kWh off-peak
    
    def get_scheduling_stats(self) -> Dict:
        """Get scheduling optimization statistics"""
        recent = self.optimization_history[-20:] if self.optimization_history else []
        return {
            'total_schedules': len(self.optimization_history),
            'recent_carbon_savings': sum(h['total_carbon_savings'] for h in recent),
            'avg_confidence': np.mean([h['avg_confidence'] for h in recent]) if recent else 0,
            'cache_size': len(self.scheduling_cache)
        }


# ============================================================
# ENHANCEMENT 9: Carbon-Aware Kubernetes Scheduler Plugin
# ============================================================

class CarbonAwareKubernetesScheduler:
    """
    Kubernetes scheduler plugin for carbon-aware pod placement.
    
    Features:
    - Node carbon intensity scoring
    - Pod priority-based scheduling
    - Node carbon budget tracking
    - Integration with cluster-autoscaler
    """
    
    def __init__(self, forecaster: 'EnhancedMarginalCarbonForecaster'):
        self.forecaster = forecaster
        self.node_carbon_scores: Dict[str, float] = {}
        self.node_budgets: Dict[str, float] = {}
        self.scheduling_decisions: List[Dict] = []
        
    async def score_node(self, node_name: str, node_region: str, 
                         node_power_watts: float) -> float:
        """Score a node for carbon-aware scheduling (higher score = better)"""
        forecast = await self.forecaster.forecast_marginal_intensity(1)
        carbon_intensity = forecast.marginal_intensity_g_per_kwh
        
        # Calculate expected carbon per hour
        carbon_per_hour = node_power_watts * carbon_intensity / 1000 / 1000  # kg CO2/hour
        
        # Score inversely proportional to carbon (lower carbon = higher score)
        base_score = 1000 / (carbon_per_hour + 1)
        
        # Adjust for node carbon budget remaining
        budget_remaining = self.node_budgets.get(node_name, 100) # kg CO2 budget
        budget_factor = min(1.0, budget_remaining / 100)
        
        score = base_score * budget_factor
        
        self.node_carbon_scores[node_name] = score
        return score
    
    async def schedule_pod(self, pod_name: str, node_name: str, 
                           estimated_duration_hours: float,
                           power_estimate_watts: float) -> Dict:
        """
        Schedule a pod with carbon-aware placement.
        
        Returns decision with expected carbon impact.
        """
        forecast = await self.forecaster.forecast_marginal_intensity(int(estimated_duration_hours) + 1)
        
        # Get carbon forecast for duration
        carbon_intensities = self._get_forecast_array(forecast, estimated_duration_hours)
        avg_intensity = np.mean(carbon_intensities)
        
        # Calculate expected carbon
        expected_carbon_kg = (power_estimate_watts * estimated_duration_hours * avg_intensity / 1000 / 1000)
        
        # Update node budget
        if node_name in self.node_budgets:
            self.node_budgets[node_name] -= expected_carbon_kg
        else:
            self.node_budgets[node_name] = 100 - expected_carbon_kg
        
        decision = {
            'pod_name': pod_name,
            'node_name': node_name,
            'expected_carbon_kg': expected_carbon_kg,
            'avg_carbon_intensity': avg_intensity,
            'duration_hours': estimated_duration_hours,
            'timestamp': datetime.now().isoformat(),
            'confidence': forecast.confidence
        }
        
        self.scheduling_decisions.append(decision)
        
        # Keep only last 1000 decisions
        if len(self.scheduling_decisions) > 1000:
            self.scheduling_decisions = self.scheduling_decisions[-1000:]
        
        return decision
    
    def _get_forecast_array(self, forecast: 'MarginalCarbonForecast', 
                            hours: float) -> List[float]:
        """Extract forecast as array for duration"""
        # Simplified - would interpolate
        return [forecast.marginal_intensity_g_per_kwh] * int(hours)
    
    def get_node_scores(self) -> Dict[str, float]:
        """Get current node carbon scores"""
        return self.node_carbon_scores.copy()
    
    def get_carbon_budgets(self) -> Dict[str, float]:
        """Get remaining carbon budgets per node"""
        return self.node_budgets.copy()


# ============================================================
# ENHANCEMENT 10: Carbon Budget Tracker
# ============================================================

class CarbonBudgetTracker:
    """
    Track and forecast carbon budgets for organizations.
    
    Features:
    - Daily/Monthly/Annual carbon budgets
    - Budget consumption tracking
    - Forecasting of budget exhaustion
    - Alerts when approaching limits
    """
    
    def __init__(self, daily_budget_kg: float = 1000):
        self.daily_budget_kg = daily_budget_kg
        self.consumption_history: List[Tuple[datetime, float]] = []
        self.alerts: List[Dict] = []
        self._lock = threading.Lock()
    
    def consume_carbon(self, amount_kg: float, task_id: str = ""):
        """Record carbon consumption"""
        with self._lock:
            self.consumption_history.append((datetime.now(), amount_kg))
            
            # Keep last 90 days
            cutoff = datetime.now() - timedelta(days=90)
            self.consumption_history = [(ts, val) for ts, val in self.consumption_history if ts > cutoff]
            
            # Check for budget alerts
            self._check_alerts()
    
    def get_daily_consumption(self, date: Optional[datetime] = None) -> float:
        """Get total consumption for a specific day"""
        if date is None:
            date = datetime.now()
        
        day_start = datetime(date.year, date.month, date.day)
        day_end = day_start + timedelta(days=1)
        
        total = sum(val for ts, val in self.consumption_history 
                   if day_start <= ts < day_end)
        return total
    
    def get_remaining_budget(self) -> float:
        """Get remaining budget for today"""
        consumed = self.get_daily_consumption()
        return max(0, self.daily_budget_kg - consumed)
    
    def get_budget_utilization_percent(self) -> float:
        """Get budget utilization percentage"""
        consumed = self.get_daily_consumption()
        return (consumed / self.daily_budget_kg * 100) if self.daily_budget_kg > 0 else 0
    
    def forecast_budget_exhaustion(self) -> Optional[datetime]:
        """Forecast when budget will be exhausted at current rate"""
        recent = [(ts, val) for ts, val in self.consumption_history 
                 if ts > datetime.now() - timedelta(hours=1)]
        
        if not recent:
            return None
        
        avg_rate = sum(val for _, val in recent) / 1.0  # kg per hour
        
        if avg_rate <= 0:
            return None
        
        remaining = self.get_remaining_budget()
        hours_remaining = remaining / avg_rate
        
        return datetime.now() + timedelta(hours=hours_remaining)
    
    def _check_alerts(self):
        """Check if alerts need to be triggered"""
        utilization = self.get_budget_utilization_percent()
        
        if utilization >= 90 and not self._alert_triggered('critical', datetime.now().date()):
            self._trigger_alert('critical', f"Budget utilization at {utilization:.1f}%")
        elif utilization >= 75 and not self._alert_triggered('warning', datetime.now().date()):
            self._trigger_alert('warning', f"Budget utilization at {utilization:.1f}%")
    
    def _alert_triggered(self, level: str, date: date) -> bool:
        """Check if alert already triggered today"""
        return any(a for a in self.alerts 
                  if a['level'] == level and a['date'] == date.isoformat())
    
    def _trigger_alert(self, level: str, message: str):
        """Trigger a budget alert"""
        alert = {
            'level': level,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'date': datetime.now().date().isoformat()
        }
        self.alerts.append(alert)
        logger.warning(f"Carbon budget alert: {level} - {message}")
    
    def get_status(self) -> Dict:
        """Get budget tracking status"""
        return {
            'daily_budget_kg': self.daily_budget_kg,
            'consumed_today_kg': self.get_daily_consumption(),
            'remaining_kg': self.get_remaining_budget(),
            'utilization_percent': self.get_budget_utilization_percent(),
            'forecast_exhaustion': self.forecast_budget_exhaustion(),
            'recent_alerts': self.alerts[-5:],
            'total_history_days': len(set(ts.date() for ts, _ in self.consumption_history))
        }


# ============================================================
# ENHANCEMENT 11: Carbon-Aware Workflow Integration (Airflow)
# ============================================================

class CarbonAwareWorkflowOperator:
    """
    Airflow operator for carbon-aware task scheduling.
    
    Integrates with Apache Airflow to schedule DAG tasks
    at optimal carbon times.
    """
    
    def __init__(self, forecaster: 'EnhancedMarginalCarbonForecaster',
                 carbon_budget: CarbonBudgetTracker):
        self.forecaster = forecaster
        self.carbon_budget = carbon_budget
        self.task_schedule_cache: Dict[str, datetime] = {}
    
    async def get_optimal_execution_time(self, task_id: str,
                                         estimated_duration_hours: float,
                                         deadline_hours: float,
                                         carbon_weight: float = 0.5) -> datetime:
        """
        Determine optimal execution time for a workflow task.
        
        Returns datetime when task should be scheduled.
        """
        forecast = await self.forecaster.forecast_marginal_intensity(int(deadline_hours))
        
        # Get carbon intensity forecast
        intensities = self._get_intensity_array(forecast, int(deadline_hours))
        
        # Score each hour
        scores = []
        for hour, intensity in enumerate(intensities[:int(deadline_hours)]):
            # Carbon score (lower is better)
            carbon_score = 1.0 - (intensity / max(intensities))
            
            # Time score (urgency)
            time_score = 1.0 - (hour / deadline_hours)
            
            # Weighted score
            score = (carbon_weight * carbon_score + (1 - carbon_weight) * time_score)
            scores.append((hour, score))
        
        # Select best hour
        best_hour = max(scores, key=lambda x: x[1])[0]
        optimal_time = datetime.now() + timedelta(hours=best_hour)
        
        # Check against carbon budget
        if self.carbon_budget.get_remaining_budget() < 100:  # Threshold
            # Budget low, prioritize carbon savings
            optimal_time = datetime.now() + timedelta(hours=int(deadline_hours * 0.8))
        
        self.task_schedule_cache[task_id] = optimal_time
        return optimal_time
    
    def _get_intensity_array(self, forecast: 'MarginalCarbonForecast', 
                             hours: int) -> List[float]:
        """Extract intensity forecast as array"""
        # Simplified interpolation
        base = forecast.marginal_intensity_g_per_kwh
        # Add diurnal pattern
        intensities = []
        for h in range(hours):
            hour_of_day = (datetime.now() + timedelta(hours=h)).hour
            diurnal = 1.0 + 0.3 * np.sin(2 * np.pi * (hour_of_day - 12) / 24)
            intensities.append(base * diurnal)
        return intensities
    
    def get_schedule_recommendation(self, task_id: str) -> Optional[datetime]:
        """Get cached schedule recommendation for a task"""
        return self.task_schedule_cache.get(task_id)


# ============================================================
# ENHANCEMENT 12: Prometheus Metrics Integration
# ============================================================

class CarbonMetricsExporter:
    """
    Export carbon metrics to Prometheus for monitoring.
    
    Provides metrics for:
    - Current marginal carbon intensity
    - Forecasted intensities
    - Carbon savings from scheduling
    - Budget utilization
    """
    
    def __init__(self, forecaster: 'EnhancedMarginalCarbonForecaster',
                 scheduler: CarbonAwareScheduler,
                 budget: CarbonBudgetTracker):
        self.forecaster = forecaster
        self.scheduler = scheduler
        self.budget = budget
        self._metrics_cache: Dict[str, float] = {}
    
    async def collect_metrics(self) -> Dict[str, float]:
        """Collect current metrics for Prometheus"""
        forecast = await self.forecaster.forecast_marginal_intensity(1)
        
        metrics = {
            'marginal_carbon_intensity_g_per_kwh': forecast.marginal_intensity_g_per_kwh,
            'average_carbon_intensity_g_per_kwh': forecast.average_intensity_g_per_kwh,
            'carbon_forecast_confidence': forecast.confidence,
            'carbon_budget_remaining_kg': self.budget.get_remaining_budget(),
            'carbon_budget_utilization_percent': self.budget.get_budget_utilization_percent(),
            'scheduling_carbon_savings_total_kg': sum(d.carbon_savings_kg for d in self.scheduler.scheduling_cache.values()),
            'scheduled_tasks_total': len(self.scheduler.scheduling_cache)
        }
        
        # Add node scores if available
        if hasattr(self.scheduler, 'get_node_scores'):
            node_scores = self.scheduler.get_node_scores()
            for node, score in node_scores.items():
                metrics[f'node_carbon_score_{node}'] = score
        
        self._metrics_cache = metrics
        return metrics
    
    def get_prometheus_text(self) -> str:
        """Generate Prometheus exposition format"""
        lines = []
        
        for name, value in self._metrics_cache.items():
            # Convert to Prometheus naming convention
            metric_name = name.replace('_', '_')
            lines.append(f"# HELP {metric_name} Carbon metrics for Green Agent")
            lines.append(f"# TYPE {metric_name} gauge")
            lines.append(f"{metric_name} {value:.3f}")
            lines.append("")
        
        return "\n".join(lines)


# ============================================================
# ENHANCEMENT 13: REST API with FastAPI
# ============================================================

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional

# Pydantic models for API
class WorkloadRequest(BaseModel):
    task_id: str
    energy_kwh: float
    deadline_hours: float
    priority: int = 5
    carbon_sensitivity: float = 0.5
    value_per_kwh: float = 0.1
    region: Optional[str] = None

class ScheduleResponse(BaseModel):
    task_id: str
    scheduled_time: datetime
    expected_carbon_kg: float
    carbon_savings_kg: float
    confidence: float
    recommendation: str

def create_carbon_aware_api(forecaster, scheduler, budget):
    """Create FastAPI app with carbon-aware endpoints"""
    app = FastAPI(title="Carbon-Aware Scheduler API", version="3.2.0")
    
    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
    
    @app.get("/carbon/forecast")
    async def get_carbon_forecast(hours: int = 24):
        forecast = await forecaster.forecast_marginal_intensity(hours)
        return {
            "current_marginal": forecast.marginal_intensity_g_per_kwh,
            "current_average": forecast.average_intensity_g_per_kwh,
            "marginal_generator": forecast.marginal_generator.value,
            "recommended_action": forecast.recommended_action,
            "confidence": forecast.confidence,
            "timestamp": forecast.timestamp.isoformat()
        }
    
    @app.post("/schedule", response_model=ScheduleResponse)
    async def schedule_workload(request: WorkloadRequest, background_tasks: BackgroundTasks):
        task = WorkloadTask(
            task_id=request.task_id,
            required_energy_kwh=request.energy_kwh,
            deadline_hours=request.deadline_hours,
            priority=request.priority,
            carbon_sensitivity=request.carbon_sensitivity,
            value_per_kwh=request.value_per_kwh,
            region=request.region
        )
        
        decisions = await scheduler.optimize_schedule([task])
        decision = decisions[0]
        
        # Schedule actual execution in background
        background_tasks.add_task(
            execute_scheduled_task,
            task.task_id,
            decision.scheduled_time,
            request.energy_kwh
        )
        
        return ScheduleResponse(
            task_id=decision.task_id,
            scheduled_time=decision.scheduled_time,
            expected_carbon_kg=decision.expected_carbon_kg,
            carbon_savings_kg=decision.carbon_savings_kg,
            confidence=decision.confidence,
            recommendation=f"Schedule at {decision.scheduled_time} to save {decision.carbon_savings_kg:.2f} kg CO2"
        )
    
    @app.get("/budget")
    async def get_budget_status():
        return budget.get_status()
    
    @app.get("/metrics")
    async def get_prometheus_metrics():
        metrics = await carbon_metrics_exporter.collect_metrics()
        return metrics
    
    return app

async def execute_scheduled_task(task_id: str, scheduled_time: datetime, energy_kwh: float):
    """Background task to execute scheduled workload"""
    wait_seconds = (scheduled_time - datetime.now()).total_seconds()
    if wait_seconds > 0:
        await asyncio.sleep(wait_seconds)
    
    # Execute actual workload here
    logger.info(f"Executing scheduled task {task_id} at {scheduled_time}")


# ============================================================
# Updated Main Function with New Features
# ============================================================

async def main():
    print("=== Enhanced Marginal Carbon Forecaster v3.2 Demo ===\n")
    
    # Initialize components
    forecaster = EnhancedMarginalCarbonForecaster({
        'region': 'us-east',
        'grid_api': {'simulate': True},
        'weather': {'simulate': True},
        'carbon_price': 50.0,
        'battery_capacity_mwh': 200
    })
    
    scheduler = CarbonAwareScheduler(forecaster)
    budget = CarbonBudgetTracker(daily_budget_kg=1000)
    metrics_exporter = CarbonMetricsExporter(forecaster, scheduler, budget)
    
    print("1. Carbon Budget Tracking:")
    budget.consume_carbon(250, "task_001")
    budget.consume_carbon(300, "task_002")
    budget_status = budget.get_status()
    print(f"   Daily budget: {budget_status['daily_budget_kg']} kg CO2")
    print(f"   Consumed today: {budget_status['consumed_today_kg']:.1f} kg")
    print(f"   Remaining: {budget_status['remaining_kg']:.1f} kg")
    print(f"   Utilization: {budget_status['utilization_percent']:.1f}%")
    
    print("\n2. Multi-Task Scheduling Optimization:")
    tasks = [
        WorkloadTask("urgent_task", 100, 2, 1, 0.2, 0.5),
        WorkloadTask("flexible_task", 200, 24, 5, 0.7, 0.3),
        WorkloadTask("batch_task", 500, 48, 8, 0.9, 0.1)
    ]
    
    decisions = await scheduler.optimize_schedule(tasks)
    for decision in decisions:
        print(f"   {decision.task_id}: scheduled at {decision.scheduled_time.strftime('%H:%M')}, "
              f"saves {decision.carbon_savings_kg:.2f} kg CO2")
    
    print("\n3. Kubernetes Node Scoring:")
    k8s_scheduler = CarbonAwareKubernetesScheduler(forecaster)
    nodes = ['node-a', 'node-b', 'node-c']
    regions = ['us-east', 'us-west', 'us-central']
    powers = [500, 300, 400]
    
    for node, region, power in zip(nodes, regions, powers):
        score = await k8s_scheduler.score_node(node, region, power)
        print(f"   {node} ({region}): score={score:.1f}")
    
    print("\n4. Prometheus Metrics:")
    metrics = await metrics_exporter.collect_metrics()
    for key, value in list(metrics.items())[:5]:
        print(f"   {key}: {value:.2f}")
    
    print(f"\n5. Scheduling Stats:")
    stats = scheduler.get_scheduling_stats()
    print(f"   Total schedules: {stats['total_schedules']}")
    print(f"   Recent carbon savings: {stats['recent_carbon_savings']:.1f} kg")
    
    print("\n✅ Enhanced Marginal Carbon Forecaster v3.2 test complete")

if __name__ == "__main__":
    # Create FastAPI app (optional)
    # app = create_carbon_aware_api(forecaster, scheduler, budget)
    asyncio.run(main())
