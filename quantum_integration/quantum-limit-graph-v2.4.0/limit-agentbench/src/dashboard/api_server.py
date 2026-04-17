"""
Green Agent v5.0.0 - Dashboard API Server
Layer 11: Real-time visualization and monitoring
File: dashboard/api_server.py
"""

from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Dict, List
from pydantic import BaseModel
from datetime import datetime
import asyncio
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Green Agent Dashboard",
    description="Real-time monitoring for sustainable AI",
    version="5.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory data stores
metrics_history: List[Dict] = []
execution_logs: List[Dict] = []
active_tasks: Dict[str, Dict] = {}
websocket_connections: List[WebSocket] = []


class ExecutionLog(BaseModel):
    """Schema for execution log entries"""
    task_id: str
    energy_consumed: float
    carbon_emitted: float
    accuracy: float
    carbon_zone: str
    timestamp: str


@app.get("/")
async def root():
    """Root endpoint with service information"""
    return {
        "service": "Green Agent Dashboard",
        "version": "5.0.0",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "ready": "/ready",
            "metrics": "/metrics",
            "executions": "/executions",
            "websocket": "/ws/metrics"
        }
    }


@app.get("/health")
async def health_check():
    """Liveness probe - is the service running?"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "5.0.0"
    }


@app.get("/ready")
async def readiness_check():
    """Readiness probe - is the service ready to accept traffic?"""
    # Check dependencies
    checks = {
        "api": True,
        "database": True,  # In-memory for now
        "carbon_api": True,  # Assume available
    }
    
    all_healthy = all(checks.values())
    status_code = 200 if all_healthy else 503
    
    return JSONResponse(
        status_code=status_code,
        content={
            "ready": all_healthy,
            "checks": checks,
            "timestamp": datetime.now().isoformat()
        }
    )


@app.get("/metrics")
async def prometheus_metrics():
    """Prometheus metrics endpoint"""
    # Return simple metrics for now
    # In production, use prometheus_client
    return {
        "green_agent_energy_consumed_kwh": sum(e.get('energy_consumed', 0) for e in metrics_history),
        "green_agent_carbon_emitted_kg": sum(e.get('carbon_emitted', 0) for e in metrics_history),
        "green_agent_tasks_completed": len(execution_logs),
        "green_agent_active_tasks": len(active_tasks),
        "timestamp": datetime.now().isoformat()
    }


@app.get("/executions")
async def get_executions(limit: int = 100):
    """Get execution history"""
    return {
        "executions": execution_logs[-limit:],
        "total": len(execution_logs)
    }


@app.post("/executions/log")
async def log_execution(execution: ExecutionLog):
    """Log task execution"""
    execution_dict = execution.dict()
    execution_dict['logged_at'] = datetime.now().isoformat()
    
    # Store in history
    metrics_history.append(execution_dict)
    execution_logs.append(execution_dict)
    
    # Broadcast to WebSocket clients
    await _broadcast_metrics(execution_dict)
    
    return {"status": "logged", "timestamp": datetime.now().isoformat()}


@app.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    """WebSocket endpoint for real-time metrics"""
    await websocket.accept()
    websocket_connections.append(websocket)
    
    try:
        while True:
            # Send latest metrics
            if metrics_history:
                await websocket.send_json({
                    "type": "metrics_update",
                    "data": metrics_history[-1],
                    "timestamp": datetime.now().isoformat()
                })
            await asyncio.sleep(5)
    except Exception as e:
        logger.warning(f"WebSocket error: {e}")
    finally:
        if websocket in websocket_connections:
            websocket_connections.remove(websocket)


async def _broadcast_metrics(metrics: Dict):
    """Broadcast metrics to all WebSocket clients"""
    if not websocket_connections:
        return
    
    message = {
        "type": "metrics_update",
        "data": metrics,
        "timestamp": datetime.now().isoformat()
    }
    
    disconnected = []
    for ws in websocket_connections:
        try:
            await ws.send_json(message)
        except:
            disconnected.append(ws)
    
    for ws in disconnected:
        websocket_connections.remove(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
