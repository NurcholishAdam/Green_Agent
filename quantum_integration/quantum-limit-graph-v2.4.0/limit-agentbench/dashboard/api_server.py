# File: dashboard/api_server.py

from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Dict, List, Optional
import asyncio
import json
import time
from pathlib import Path
import uvicorn
from datetime import datetime, timedelta
import numpy as np

class DashboardAPI:
    """FastAPI-based dashboard server"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.app = FastAPI(
            title="Green Agent Dashboard",
            description="Real-time monitoring for sustainable AI",
            version="5.0.0"
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
        
        # Setup routes
        self._setup_routes()
        
        # Background tasks
        self.running = False
    
    def _setup_routes(self):
        """Setup API routes"""
        
        @self.app.get("/")
        async def root():
            return {
                "service": "Green Agent Dashboard",
                "version": "5.0.0",
                "status": "running",
                "endpoints": {
                    "health": "/health",
                    "metrics": "/metrics/realtime",
                    "executions": "/executions",
                    "pareto": "/analytics/pareto",
                    "carbon": "/carbon/forecast"
                }
            }
        
        @self.app.get("/health")
        async def health_check():
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "uptime_seconds": time.time() - self.start_time if hasattr(self, 'start_time') else 0
            }
        
        @self.app.get("/ready")
        async def readiness_check():
            return {
                "ready": True,
                "components": {
                    "api": "ok",
                    "database": "ok",
                    "metrics": "ok"
                }
            }
        
        @self.app.get("/metrics/realtime")
        async def get_realtime_metrics():
            """Get current system metrics"""
            if not self.metrics_history:
                return self._default_metrics()
            
            latest = self.metrics_history[-1]
            
            return {
                "timestamp": latest.get('timestamp'),
                "energy_consumption": latest.get('energy', 0),
                "carbon_footprint": latest.get('carbon', 0),
                "active_tasks": len(self.active_tasks),
                "completed_tasks": len(self.execution_logs),
                "avg_accuracy": np.mean([log.get('accuracy', 0) for log in self.execution_logs[-100:]]),
                "negawatt_reward": latest.get('negawatt_reward', 0),
                "pareto_frontier": self.pareto_frontier[-10:] if self.pareto_frontier else []
            }
        
        @self.app.get("/executions")
        async def get_execution_logs(limit: int = 100):
            """Get execution history"""
            return {
                "executions": self.execution_logs[-limit:],
                "total": len(self.execution_logs)
            }
        
        @self.app.get("/analytics/pareto")
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
        
        @self.app.get("/carbon/forecast")
        async def get_carbon_forecast(hours: int = 24):
            """Get carbon intensity forecast"""
            # Simulated forecast - in production, call carbon API
            now = datetime.now()
            forecast = []
            
            for i in range(hours):
                timestamp = now + timedelta(hours=i)
                # Simulate daily pattern (lower at night, higher during day)
                hour = timestamp.hour
                base_intensity = 200
                if 6 <= hour <= 18:
                    intensity = base_intensity + 100 * np.sin((hour - 6) * np.pi / 12)
                else:
                    intensity = base_intensity - 50
                
                forecast.append({
                    "timestamp": timestamp.isoformat(),
                    "intensity_gco2_kwh": max(50, min(400, intensity)),
                    "zone": self._get_carbon_zone(intensity)
                })
            
            return {
                "forecast": forecast,
                "current_zone": self._get_carbon_zone(forecast[0]["intensity_gco2_kwh"]),
                "recommendation": self._get_carbon_recommendation(forecast[0]["intensity_gco2_kwh"])
            }
        
        @self.app.post("/executions/log")
        async def log_execution(execution: Dict):
            """Log task execution"""
            self.execution_logs.append({
                **execution,
                "logged_at": datetime.now().isoformat()
            })
            
            # Update metrics history
            self.metrics_history.append({
                "timestamp": datetime.now().isoformat(),
                "energy": execution.get('energy', 0),
                "carbon": execution.get('carbon', 0),
                "negawatt_reward": execution.get('negawatt_reward', 0)
            })
            
            # Update Pareto frontier
            self._update_pareto_frontier(execution)
            
            # Broadcast to WebSocket clients
            await self._broadcast_metrics(execution)
            
            return {"status": "logged"}
        
        @self.app.websocket("/ws/metrics")
        async def websocket_metrics(websocket: WebSocket):
            """WebSocket endpoint for real-time metrics"""
            await websocket.accept()
            self.websocket_connections.append(websocket)
            
            try:
                while True:
                    # Send metrics every 5 seconds
                    metrics = await get_realtime_metrics()
                    await websocket.send_json(metrics)
                    await asyncio.sleep(5)
            except:
                self.websocket_connections.remove(websocket)
        
        @self.app.get("/tasks/active")
        async def get_active_tasks():
            """Get currently active tasks"""
            return {
                "tasks": list(self.active_tasks.values()),
                "count": len(self.active_tasks)
            }
        
        @self.app.post("/tasks/{task_id}/update")
        async def update_task(task_id: str, update: Dict):
            """Update task status"""
            if task_id in self.active_tasks:
                self.active_tasks[task_id].update(update)
                return {"status": "updated"}
            raise HTTPException(status_code=404, detail="Task not found")
    
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
        
        self.pareto_frontier.append(point)
        
        # Keep only last 1000 points
        if len(self.pareto_frontier) > 1000:
            self.pareto_frontier = self.pareto_frontier[-1000:]
        
        # Calculate actual Pareto frontier (non-dominated points)
        self.pareto_frontier = self._calculate_pareto_frontier(self.pareto_frontier)
    
    def _calculate_pareto_frontier(self, points: List[Dict]) -> List[Dict]:
        """Calculate Pareto frontier from points"""
        if not points:
            return []
        
        frontier = []
        for point in points:
            dominated = False
            for other in points:
                # Check if other dominates point
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
        
        # Remove disconnected clients
        for ws in disconnected:
            self.websocket_connections.remove(ws)
    
    def _default_metrics(self) -> Dict:
        """Return default metrics when no data available"""
        return {
            "timestamp": datetime.now().isoformat(),
            "energy_consumption": 0,
            "carbon_footprint": 0,
            "active_tasks": 0,
            "completed_tasks": 0,
            "avg_accuracy": 0,
            "negawatt_reward": 0,
            "pareto_frontier": []
        }
    
    async def start(self):
        """Start dashboard server"""
        self.running = True
        self.start_time = time.time()
        
        config = uvicorn.Config(
            self.app,
            host=self.config.get('dashboard', {}).get('host', '0.0.0.0'),
            port=self.config.get('dashboard', {}).get('port', 8000),
            log_level="info"
        )
        
        self.server = uvicorn.Server(config)
        
        # Run in background task
        asyncio.create_task(self.server.serve())
        print(f"📊 Dashboard started at http://localhost:{config.port}")
    
    async def stop(self):
        """Stop dashboard server"""
        self.running = False
        if hasattr(self, 'server'):
            self.server.should_exit = True
            print(" Dashboard stopped")


# HTML Dashboard (served separately)
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Green Agent Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-2.20.0.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1400px; margin: 0 auto; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .metric { font-size: 2em; font-weight: bold; color: #2ecc71; }
        .label { color: #7f8c8d; margin-top: 5px; }
        h1 { color: #2c3e50; }
        .status { display: inline-block; padding: 5px 10px; border-radius: 4px; }
        .status.green { background: #d4edda; color: #155724; }
        .status.yellow { background: #fff3cd; color: #856404; }
        .status.red { background: #f8d7da; color: #721c24; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🌱 Green Agent Dashboard</h1>
        
        <div class="grid">
            <div class="card">
                <div class="label">Energy Consumption</div>
                <div class="metric" id="energy">0 kWh</div>
            </div>
            
            <div class="card">
                <div class="label">Carbon Footprint</div>
                <div class="metric" id="carbon">0 kg CO₂</div>
            </div>
            
            <div class="card">
                <div class="label">Active Tasks</div>
                <div class="metric" id="tasks">0</div>
            </div>
            
            <div class="card">
                <div class="label">Average Accuracy</div>
                <div class="metric" id="accuracy">0%</div>
            </div>
        </div>
        
        <div class="card" style="margin-top: 20px;">
            <h2>Pareto Frontier</h2>
            <div id="pareto-plot" style="width:100%;height:400px;"></div>
        </div>
    </div>
    
    <script>
        // WebSocket connection for real-time updates
        const ws = new WebSocket(`ws://${location.host}/ws/metrics`);
        
        ws.onmessage = function(event) {
            const data = JSON.parse(event.data);
            updateDashboard(data);
        };
        
        function updateDashboard(data) {
            document.getElementById('energy').textContent = 
                data.energy_consumption.toFixed(3) + ' kWh';
            document.getElementById('carbon').textContent = 
                data.carbon_footprint.toFixed(3) + ' kg CO₂';
            document.getElementById('tasks').textContent = data.active_tasks;
            document.getElementById('accuracy').textContent = 
                (data.avg_accuracy * 100).toFixed(1) + '%';
            
            // Update Pareto plot
            if (data.pareto_frontier && data.pareto_frontier.length > 0) {
                updateParetoPlot(data.pareto_frontier);
            }
        }
        
        function updateParetoPlot(points) {
            const trace = {
                x: points.map(p => p.energy),
                y: points.map(p => p.accuracy),
                mode: 'markers+lines',
                type: 'scatter',
                marker: { color: '#2ecc71', size: 10 }
            };
            
            const layout = {
                title: 'Accuracy vs Energy',
                xaxis: { title: 'Energy (kWh)' },
                yaxis: { title: 'Accuracy' }
            };
            
            Plotly.newPlot('pareto-plot', [trace], layout);
        }
        
        // Initial fetch
        fetch('/metrics/realtime')
            .then(r => r.json())
            .then(updateDashboard);
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True)
