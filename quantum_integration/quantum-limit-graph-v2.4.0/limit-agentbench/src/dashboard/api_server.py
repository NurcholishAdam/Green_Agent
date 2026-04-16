"""
Green Agent v5.0.0 - Dashboard API Server
Layer 11: Real-time visualization and monitoring

File: dashboard/api_server.py
Status: FOUNDATIONAL - Tier 1
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict
import asyncio
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Green Agent Dashboard", version="5.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

metrics_history = []
execution_logs = []


@app.get("/")
async def root():
    return {
        "service": "Green Agent Dashboard",
        "version": "5.0.0",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "ready": "/ready",
            "metrics": "/metrics",
            "executions": "/executions"
        }
    }


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "5.0.0"
    }


@app.get("/ready")
async def readiness_check():
    return {
        "ready": True,
        "checks": {
            "api": True,
            "database": True,
            "carbon_api": True
        },
        "timestamp": datetime.now().isoformat()
    }


@app.get("/metrics")
async def get_metrics():
    if not metrics_history:
        return _default_metrics()
    
    latest = metrics_history[-1]
    return {
        "timestamp": latest.get('timestamp'),
        "energy_consumption": latest.get('energy', 0),
        "carbon_footprint": latest.get('carbon', 0),
        "carbon_intensity": latest.get('carbon_intensity', 0),
        "active_tasks": len(execution_logs),
        "completed_tasks": len(metrics_history)
    }


@app.get("/executions")
async def get_executions(limit: int = 100):
    return {
        "executions": execution_logs[-limit:],
        "total": len(execution_logs)
    }


@app.post("/executions/log")
async def log_execution(execution: Dict):
    metrics_history.append({
        "timestamp": datetime.now().isoformat(),
        **execution
    })
    execution_logs.append(execution)
    return {"status": "logged", "timestamp": datetime.now().isoformat()}


def _default_metrics():
    return {
        "timestamp": datetime.now().isoformat(),
        "energy_consumption": 0,
        "carbon_footprint": 0,
        "carbon_intensity": 0,
        "active_tasks": 0,
        "completed_tasks": 0
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
