"""
Green Agent Dashboard API Server v5.0.0 (Enhanced)
Enhanced with health checks, metrics, Prometheus integration,
and advanced enhancement metrics:
- LIMIT Graph
- MODP (Multi-Objective Decision Process)
- RLHF (Reinforcement Learning from Human Feedback)
- Multi-Teacher On-Policy Distillation
- Bio-inspired Optimisation (Evolutionary)
- Mixture-of-Experts (MoE) expert gating
- FlexGen execution backend

Original functionality is preserved when enhancements are disabled.
"""

from fastapi import FastAPI, WebSocket, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
from typing import Dict, List, Optional
import asyncio
import json
import time
from pathlib import Path
import uvicorn
from datetime import datetime, timedelta
import numpy as np

# Prometheus metrics
from prometheus_client import (
    generate_latest, 
    CONTENT_TYPE_LATEST, 
    Counter, 
    Gauge, 
    Histogram,
    CollectorRegistry,
    REGISTRY
)

# Define Prometheus metrics (original)
ENERGY_CONSUMED = Counter(
    'green_agent_energy_consumed_kwh', 
    'Total energy consumed in kWh',
    ['mode', 'task_type']
)

CARBON_EMITTED = Counter(
    'green_agent_carbon_emitted_kg', 
    'Total carbon emitted in kg CO2',
    ['mode', 'carbon_zone']
)

CARBON_INTENSITY = Gauge(
    'green_agent_carbon_intensity', 
    'Current carbon intensity in gCO2/kWh',
    ['region']
)

TASKS_COMPLETED = Counter(
    'green_agent_tasks_completed', 
    'Total tasks completed',
    ['mode', 'status']
)

QUEUE_DEPTH = Gauge(
    'green_agent_queue_depth', 
    'Current task queue depth'
)

ACCURACY_SCORE = Gauge(
    'green_agent_accuracy_score', 
    'Model accuracy score'
)

EXECUTION_TIME = Histogram(
    'green_agent_execution_time_seconds', 
    'Task execution time in seconds',
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
)

ACTIVE_WORKERS = Gauge(
    'green_agent_active_workers', 
    'Number of active Ray workers'
)

# ---------------------------------------------------------------------------
# Enhanced Prometheus metrics for advanced modules
# ---------------------------------------------------------------------------
MODP_SCORE = Gauge(
    'green_agent_modp_score',
    'Multi-Objective Decision Process composite score (0-1)'
)

RLHF_FEEDBACK = Gauge(
    'green_agent_rlhf_feedback',
    'RLHF human feedback score (0-1)'
)

GRAPH_CENTRALITY = Gauge(
    'green_agent_graph_centrality',
    'LIMIT Graph centrality metric'
)

GRAPH_CONNECTIVITY = Gauge(
    'green_agent_graph_connectivity',
    'LIMIT Graph connectivity metric'
)

DISTILLATION_UPDATE_COUNT = Counter(
    'green_agent_distillation_update_count',
    'Number of distillation student updates'
)

MOE_GATE_WEIGHT = Gauge(
    'green_agent_moe_gate_weight',
    'MoE gating weight (average)',
    ['expert']
)

EVOLUTIONARY_BEST_FITNESS = Gauge(
    'green_agent_evolutionary_best_fitness',
    'Evolutionary optimizer best fitness'
)

FLEXGEN_ENERGY = Counter(
    'green_agent_flexgen_energy_joules_total',
    'Total energy consumed by FlexGen in joules'
)


class DashboardAPI:
    """FastAPI-based dashboard server with enhanced health checks"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.use_enhancements = self.config.get('enhancements', {}).get('enabled', True)
        
        self.app = FastAPI(
            title="Green Agent Dashboard",
            description="Real-time monitoring for sustainable AI",
            version="5.0.0",
            docs_url="/docs",
            redoc_url="/redoc"
        )
        
        # CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Data stores
        self.metrics_history: List[Dict] = []
        self.execution_logs: List[Dict] = []
        self.pareto_frontier: List[Dict] = []
        self.active_tasks: Dict[str, Dict] = {}
        
        # WebSocket connections
        self.websocket_connections: List[WebSocket] = []
        
        # Component health status
        self.component_health = {
            'api': True,
            'database': False,
            'ray_cluster': False,
            'carbon_api': False,
            'dashboard': True
        }
        
        # Startup time
        self.start_time = time.time()
        
        # Setup routes
        self._setup_routes()
        
        # Background tasks
        self.running = False
    
    def _setup_routes(self):
        """Setup API routes"""
        
        @self.app.get("/")
        async def root():
            """Root endpoint with service information"""
            return {
                "service": "Green Agent Dashboard",
                "version": "5.0.0",
                "status": "running",
                "uptime_seconds": time.time() - self.start_time,
                "enhancements_enabled": self.use_enhancements,
                "endpoints": {
                    "health": "/health",
                    "ready": "/ready",
                    "live": "/live",
                    "metrics": "/metrics",
                    "metrics/realtime": "/metrics/realtime",
                    "executions": "/executions",
                    "pareto": "/analytics/pareto",
                    "carbon": "/carbon/forecast",
                    "enhancements/status": "/enhancements/status"
                }
            }
        
        @self.app.get("/health", tags=["Health"])
        async def health_check():
            """Liveness probe - is the service running?"""
            uptime = time.time() - self.start_time
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "version": "5.0.0",
                "uptime_seconds": round(uptime, 2),
                "component": "api"
            }
        
        @self.app.get("/ready", tags=["Health"])
        async def readiness_check():
            """Readiness probe - is the service ready to accept traffic?"""
            checks = {
                "api": True,
                "database": await self._check_database(),
                "ray_cluster": await self._check_ray_connection(),
                "carbon_api": await self._check_carbon_api(),
                "dashboard": True
            }
            
            self.component_health = checks
            all_healthy = all(checks.values())
            healthy_count = sum(1 for v in checks.values() if v)
            total_count = len(checks)
            
            status_code = 200 if all_healthy else 503
            
            return JSONResponse(
                status_code=status_code,
                content={
                    "ready": all_healthy,
                    "checks": checks,
                    "healthy_components": f"{healthy_count}/{total_count}",
                    "timestamp": datetime.now().isoformat()
                }
            )
        
        @self.app.get("/live", tags=["Health"])
        async def liveness_check():
            """Liveness probe - alternative endpoint"""
            return {"status": "alive", "timestamp": datetime.now().isoformat()}
        
        @self.app.get("/metrics", tags=["Monitoring"])
        async def prometheus_metrics():
            """Prometheus metrics endpoint"""
            # Update dynamic metrics
            QUEUE_DEPTH.set(len(self.active_tasks))
            ACTIVE_WORKERS.set(len(self.execution_logs[-100:]))
            
            if self.execution_logs:
                recent_accuracy = np.mean([log.get('accuracy', 0) for log in self.execution_logs[-100:]])
                ACCURACY_SCORE.set(recent_accuracy)
            
            # Update enhanced metrics if enabled
            if self.use_enhancements:
                self._update_enhanced_metrics_from_history()
            
            return Response(
                content=generate_latest(REGISTRY),
                media_type=CONTENT_TYPE_LATEST
            )
        
        @self.app.get("/metrics/realtime", tags=["Monitoring"])
        async def get_realtime_metrics():
            """Get current system metrics in JSON format, including enhancements"""
            if not self.metrics_history:
                return self._default_metrics()
            
            latest = self.metrics_history[-1]
            result = {
                "timestamp": latest.get('timestamp'),
                "energy_consumption": latest.get('energy', 0),
                "carbon_footprint": latest.get('carbon', 0),
                "carbon_intensity": latest.get('carbon_intensity', 0),
                "active_tasks": len(self.active_tasks),
                "completed_tasks": len(self.execution_logs),
                "avg_accuracy": np.mean([log.get('accuracy', 0) for log in self.execution_logs[-100:]]),
                "negawatt_reward": latest.get('negawatt_reward', 0),
                "pareto_frontier": self.pareto_frontier[-10:] if self.pareto_frontier else [],
                "component_health": self.component_health
            }
            
            # Include enhanced metrics if available
            if self.use_enhancements:
                enhanced = self._get_latest_enhanced_metrics()
                if enhanced:
                    result["enhancements"] = enhanced
            
            return result
        
        @self.app.get("/executions", tags=["Data"])
        async def get_execution_logs(limit: int = 100):
            """Get execution history"""
            return {
                "executions": self.execution_logs[-limit:],
                "total": len(self.execution_logs)
            }
        
        @self.app.get("/analytics/pareto", tags=["Analytics"])
        async def get_pareto_frontier():
            """Get Pareto frontier analysis"""
            if not self.pareto_frontier:
                return {
                    "frontier_points": [],
                    "metadata": {
                        "generated_at": datetime.now().isoformat(),
                        "total_points": 0
                    }
                }
            
            return {
                "frontier_points": self.pareto_frontier,
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "total_points": len(self.pareto_frontier),
                    "best_accuracy": max(p.get('accuracy', 0) for p in self.pareto_frontier),
                    "lowest_energy": min(p.get('energy', float('inf')) for p in self.pareto_frontier)
                }
            }
        
        @self.app.get("/carbon/forecast", tags=["Carbon"])
        async def get_carbon_forecast(hours: int = 24):
            """Get carbon intensity forecast"""
            now = datetime.now()
            forecast = []
            
            for i in range(hours):
                timestamp = now + timedelta(hours=i)
                hour = timestamp.hour
                base_intensity = 200
                if 6 <= hour <= 18:
                    intensity = base_intensity + 100 * np.sin((hour - 6) * np.pi / 12)
                else:
                    intensity = base_intensity - 50
                
                zone = self._get_carbon_zone(intensity)
                
                forecast.append({
                    "timestamp": timestamp.isoformat(),
                    "intensity_gco2_kwh": max(50, min(400, intensity)),
                    "zone": zone,
                    "recommendation": self._get_carbon_recommendation(intensity)
                })
                
                if i == 0:
                    CARBON_INTENSITY.labels(region='default').set(intensity)
            
            return {
                "forecast": forecast,
                "current_zone": self._get_carbon_zone(forecast[0]["intensity_gco2_kwh"]),
                "recommendation": self._get_carbon_recommendation(forecast[0]["intensity_gco2_kwh"])
            }
        
        @self.app.post("/executions/log", tags=["Data"])
        async def log_execution(execution: Dict):
            """Log task execution and update metrics (including enhanced)"""
            self.execution_logs.append({
                **execution,
                "logged_at": datetime.now().isoformat()
            })
            
            # Update standard Prometheus metrics
            ENERGY_CONSUMED.labels(
                mode=execution.get('mode', 'unknown'),
                task_type=execution.get('task_type', 'unknown')
            ).inc(execution.get('energy', 0))
            
            CARBON_EMITTED.labels(
                mode=execution.get('mode', 'unknown'),
                carbon_zone=execution.get('carbon_zone', 'unknown')
            ).inc(execution.get('carbon', 0))
            
            TASKS_COMPLETED.labels(
                mode=execution.get('mode', 'unknown'),
                status='success' if execution.get('success', False) else 'failed'
            ).inc()
            
            EXECUTION_TIME.observe(execution.get('execution_time', 0))
            
            # Update metrics history
            self.metrics_history.append({
                "timestamp": datetime.now().isoformat(),
                "energy": execution.get('energy', 0),
                "carbon": execution.get('carbon', 0),
                "carbon_intensity": execution.get('carbon_intensity', 0),
                "negawatt_reward": execution.get('negawatt_reward', 0),
                # include enhanced metrics if present
                "modp_score": execution.get('modp_score'),
                "graph_metrics": execution.get('graph_metrics'),
                "human_feedback_score": execution.get('human_feedback_score'),
                "distillation_update_count": execution.get('distillation_update_count'),
                "moe_gate_weights": execution.get('moe_gate_weights'),
                "evolutionary_best_fitness": execution.get('evolutionary_best_fitness'),
                "flexgen_energy_joules": execution.get('flexgen_energy_joules')
            })
            
            # Update enhanced Prometheus metrics
            if self.use_enhancements:
                self._update_enhanced_metrics(execution)
            
            # Update Pareto frontier
            self._update_pareto_frontier(execution)
            
            # Broadcast to WebSocket clients
            await self._broadcast_metrics(execution)
            
            return {"status": "logged", "timestamp": datetime.now().isoformat()}
        
        @self.app.get("/enhancements/status", tags=["Enhancements"])
        async def get_enhancement_status():
            """Get status of advanced enhancements"""
            if not self.use_enhancements:
                return {"enabled": False, "message": "Enhancements not enabled"}
            latest = self._get_latest_enhanced_metrics()
            return {
                "enabled": True,
                "metrics": latest,
                "components": ["limit_graph", "modp", "rlhf", "distillation", "moe", "evolutionary", "flexgen"]
            }
        
        @self.app.websocket("/ws/metrics")
        async def websocket_metrics(websocket: WebSocket):
            """WebSocket endpoint for real-time metrics (including enhancements)"""
            await websocket.accept()
            self.websocket_connections.append(websocket)
            
            try:
                while True:
                    metrics = await get_realtime_metrics()
                    await websocket.send_json(metrics)
                    await asyncio.sleep(5)
            except Exception as e:
                self.websocket_connections.remove(websocket)
        
        @self.app.get("/tasks/active", tags=["Tasks"])
        async def get_active_tasks():
            """Get currently active tasks"""
            return {
                "tasks": list(self.active_tasks.values()),
                "count": len(self.active_tasks),
                "queue_depth": len(self.active_tasks)
            }
        
        @self.app.post("/tasks/{task_id}/update", tags=["Tasks"])
        async def update_task(task_id: str, update: Dict):
            """Update task status"""
            if task_id in self.active_tasks:
                self.active_tasks[task_id].update(update)
                return {"status": "updated", "task_id": task_id}
            raise HTTPException(status_code=404, detail="Task not found")
        
        @self.app.get("/components/health", tags=["Health"])
        async def get_component_health():
            """Get detailed component health status"""
            return {
                "components": self.component_health,
                "overall_healthy": all(self.component_health.values()),
                "healthy_count": sum(1 for v in self.component_health.values() if v),
                "total_count": len(self.component_health),
                "timestamp": datetime.now().isoformat()
            }
    
    async def _check_database(self) -> bool:
        """Check database connectivity"""
        try:
            data_dir = Path("data")
            data_dir.mkdir(exist_ok=True)
            test_file = data_dir / ".health_check"
            test_file.write_text("ok")
            test_file.unlink()
            return True
        except Exception as e:
            return False
    
    async def _check_ray_connection(self) -> bool:
        """Check Ray cluster connectivity"""
        try:
            import ray
            if ray.is_initialized():
                ray.nodes()
                return True
            return False
        except Exception as e:
            return False
    
    async def _check_carbon_api(self) -> bool:
        """Check carbon API connectivity"""
        try:
            from src.carbon.forecasting_engine import CarbonForecaster
            forecaster = CarbonForecaster(self.config)
            await forecaster.initialize()
            intensity = await forecaster.get_current_intensity()
            await forecaster.close()
            return intensity > 0
        except Exception as e:
            return False
    
    def _get_carbon_zone(self, intensity: float) -> str:
        """Determine carbon zone"""
        if intensity < 50:
            return "green"
        elif intensity < 200:
            return "yellow"
        else:
            return "red"
    
    def _get_carbon_recommendation(self, intensity: float) -> str:
        """Get recommendation based on carbon intensity"""
        if intensity < 50:
            return "OPTIMAL: Run compute-intensive tasks now"
        elif intensity < 200:
            return "MODERATE: Run standard tasks, defer if possible"
        else:
            return "HIGH: Defer non-urgent tasks, use eco mode"
    
    def _update_pareto_frontier(self, execution: Dict):
        """Update Pareto frontier with new execution"""
        point = {
            "accuracy": execution.get('accuracy', 0),
            "energy": execution.get('energy', 0),
            "carbon": execution.get('carbon', 0),
            "timestamp": datetime.now().isoformat()
        }
        # Include modp score if available
        if execution.get('modp_score') is not None:
            point['modp_score'] = execution['modp_score']
        self.pareto_frontier.append(point)
        
        if len(self.pareto_frontier) > 1000:
            self.pareto_frontier = self.pareto_frontier[-1000:]
        
        self.pareto_frontier = self._calculate_pareto_frontier(self.pareto_frontier)
    
    def _calculate_pareto_frontier(self, points: List[Dict]) -> List[Dict]:
        """Calculate Pareto frontier from points"""
        if not points:
            return []
        frontier = []
        for point in points:
            dominated = False
            for other in points:
                if (other['accuracy'] >= point['accuracy'] and 
                    other['energy'] <= point['energy'] and
                    (other['accuracy'] > point['accuracy'] or 
                     other['energy'] < point['energy'])):
                    dominated = True
                    break
            if not dominated:
                frontier.append(point)
        return frontier
    
    async def _broadcast_metrics(self, metrics: Dict):
        """Broadcast metrics to WebSocket clients"""
        if not self.websocket_connections:
            return
        message = {
            "type": "metrics_update",
            "data": metrics,
            "timestamp": datetime.now().isoformat()
        }
        disconnected = []
        for ws in self.websocket_connections:
            try:
                await ws.send_json(message)
            except:
                disconnected.append(ws)
        for ws in disconnected:
            self.websocket_connections.remove(ws)
    
    def _default_metrics(self) -> Dict:
        """Return default metrics when no data available, including enhanced defaults"""
        result = {
            "timestamp": datetime.now().isoformat(),
            "energy_consumption": 0,
            "carbon_footprint": 0,
            "carbon_intensity": 0,
            "active_tasks": 0,
            "completed_tasks": 0,
            "avg_accuracy": 0,
            "negawatt_reward": 0,
            "pareto_frontier": [],
            "component_health": self.component_health
        }
        if self.use_enhancements:
            result["enhancements"] = {
                "modp_score": None,
                "graph_metrics": {},
                "human_feedback_score": None,
                "distillation_update_count": 0,
                "moe_gate_weights": [],
                "evolutionary_best_fitness": None,
                "flexgen_energy_joules": 0
            }
        return result
    
    def _update_enhanced_metrics(self, execution: Dict):
        """Update enhanced Prometheus gauges/counters based on execution data"""
        # MODP score
        if execution.get('modp_score') is not None:
            MODP_SCORE.set(execution['modp_score'])
        
        # RLHF feedback
        if execution.get('human_feedback_score') is not None:
            RLHF_FEEDBACK.set(execution['human_feedback_score'])
        
        # Graph metrics
        graph_metrics = execution.get('graph_metrics')
        if graph_metrics:
            if 'centrality' in graph_metrics:
                GRAPH_CENTRALITY.set(graph_metrics['centrality'])
            if 'connectivity' in graph_metrics:
                GRAPH_CONNECTIVITY.set(graph_metrics['connectivity'])
        
        # Distillation update count (increment)
        if execution.get('distillation_update_count'):
            DISTILLATION_UPDATE_COUNT.inc(execution['distillation_update_count'])
        else:
            DISTILLATION_UPDATE_COUNT.inc(0)  # ensure it appears
        
        # MoE gate weights (set average)
        moe_weights = execution.get('moe_gate_weights')
        if moe_weights:
            if isinstance(moe_weights, (list, tuple)):
                for i, w in enumerate(moe_weights):
                    MOE_GATE_WEIGHT.labels(expert=str(i)).set(w)
            elif isinstance(moe_weights, dict):
                for expert, w in moe_weights.items():
                    MOE_GATE_WEIGHT.labels(expert=expert).set(w)
        
        # Evolutionary best fitness
        if execution.get('evolutionary_best_fitness') is not None:
            EVOLUTIONARY_BEST_FITNESS.set(execution['evolutionary_best_fitness'])
        
        # FlexGen energy
        if execution.get('flexgen_energy_joules'):
            FLEXGEN_ENERGY.inc(execution['flexgen_energy_joules'])
    
    def _update_enhanced_metrics_from_history(self):
        """Recompute enhanced metrics from the last execution log (for Prometheus endpoint)."""
        if not self.execution_logs:
            return
        latest = self.execution_logs[-1]
        self._update_enhanced_metrics(latest)
    
    def _get_latest_enhanced_metrics(self) -> Dict:
        """Get latest enhanced metrics from the most recent execution log or history."""
        if not self.execution_logs:
            return {}
        latest = self.execution_logs[-1]
        return {
            "modp_score": latest.get('modp_score'),
            "graph_metrics": latest.get('graph_metrics', {}),
            "human_feedback_score": latest.get('human_feedback_score'),
            "distillation_update_count": latest.get('distillation_update_count'),
            "moe_gate_weights": latest.get('moe_gate_weights'),
            "evolutionary_best_fitness": latest.get('evolutionary_best_fitness'),
            "flexgen_energy_joules": latest.get('flexgen_energy_joules')
        }
    
    async def start(self):
        """Start dashboard server"""
        self.running = True
        
        config = uvicorn.Config(
            self.app,
            host=self.config.get('dashboard', {}).get('host', '0.0.0.0'),
            port=self.config.get('dashboard', {}).get('port', 8000),
            log_level="info"
        )
        
        self.server = uvicorn.Server(config)
        asyncio.create_task(self.server.serve())
        print(f"📊 Dashboard started at http://localhost:{config.port}")
        print(f"📈 Metrics endpoint: http://localhost:{config.port}/metrics")
        print(f"❤️  Health check: http://localhost:{config.port}/health")
        print(f"✅ Readiness check: http://localhost:{config.port}/ready")
        if self.use_enhancements:
            print(f"🧠 Enhancements status: http://localhost:{config.port}/enhancements/status")
    
    async def stop(self):
        """Stop dashboard server"""
        self.running = False
        if hasattr(self, 'server'):
            self.server.should_exit = True
            print(" Dashboard stopped")


if __name__ == "__main__":
    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True)
