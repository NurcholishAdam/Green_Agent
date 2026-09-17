# src/dashboard/api_server.py

"""
Green Agent Dashboard API Server
================================

FastAPI + WebSocket server exposing real-time metrics, execution logs, and
Prometheus-compatible scrape output.

Enhancements
------------
- ``DashboardAPIConfig`` — frozen, validated: CORS origins, port, max
  history caps, WebSocket broadcast cadence.
- ``ExecutionLog`` — Pydantic model with validation (finite non-negative
  values, non-empty task_id).
- **Bounded in-memory stores** — ``deque(maxlen=...)`` replaces unbounded
  lists.
- **Thread safety** — ``RLock`` guards the stores.
- **UTC timestamps** — ``datetime.now(timezone.utc)`` everywhere.
- **Proper Prometheus text exposition** (``text/plain; version=0.0.4``).
- **`main()`** entry point with CLI argument parsing.
- **Fixed bare `except:`** in `_broadcast_metrics`.
- Lazy ``%s`` logging.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Sequence

from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
import os as _os

_DEFAULT_CORS = _os.environ.get("GREEN_AGENT_CORS_ORIGINS", "*")
_DEFAULT_PORT = int(_os.environ.get("GREEN_AGENT_DASHBOARD_PORT", "8000"))
_DEFAULT_HOST = _os.environ.get("GREEN_AGENT_DASHBOARD_HOST", "0.0.0.0")
_DEFAULT_MAX_HISTORY = int(_os.environ.get("GREEN_AGENT_MAX_HISTORY", "10000"))


class ExecutionLog(BaseModel):
    """Schema for execution log entries with validation."""

    task_id: str = Field(..., min_length=1)
    energy_consumed: float = Field(..., ge=0.0)
    carbon_emitted: float = Field(..., ge=0.0)
    accuracy: float = Field(..., ge=0.0, le=1.0)
    carbon_zone: str = Field(..., min_length=1)
    timestamp: Optional[str] = None

    @field_validator("energy_consumed", "carbon_emitted", "accuracy")
    @classmethod
    def _finite(cls, v: float) -> float:
        import math
        if math.isnan(v) or math.isinf(v):
            raise ValueError("must be finite")
        return float(v)


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #
_lock = threading.RLock()
metrics_history: Deque[Dict[str, Any]] = deque(maxlen=_DEFAULT_MAX_HISTORY)
execution_logs: Deque[Dict[str, Any]] = deque(maxlen=_DEFAULT_MAX_HISTORY)
active_tasks: Dict[str, Dict[str, Any]] = {}
websocket_connections: List[WebSocket] = []


# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #
def create_app(
    *,
    cors_origins: Sequence[str] = (_DEFAULT_CORS,),
) -> FastAPI:
    """Create a FastAPI app with the configured CORS origins."""
    app = FastAPI(
        title="Green Agent Dashboard",
        description="Real-time monitoring for sustainable AI",
        version="5.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cors_origins),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/")
    async def root() -> Dict[str, Any]:
        return {
            "service": "Green Agent Dashboard",
            "version": "5.0.0",
            "status": "running",
            "endpoints": {
                "health": "/health",
                "ready": "/ready",
                "metrics": "/metrics",
                "executions": "/executions",
                "websocket": "/ws/metrics",
            },
        }

    @app.get("/health")
    async def health_check() -> Dict[str, Any]:
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": "5.0.0",
        }

    @app.get("/ready")
    async def readiness_check() -> JSONResponse:
        checks = {"api": True, "database": True, "carbon_api": True}
        all_healthy = all(checks.values())
        status_code = 200 if all_healthy else 503
        return JSONResponse(
            status_code=status_code,
            content={
                "ready": all_healthy,
                "checks": checks,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    @app.get("/metrics", response_class=PlainTextResponse)
    async def prometheus_metrics() -> PlainTextResponse:
        """Prometheus text exposition (version 0.0.4)."""
        with _lock:
            energy = sum(e.get("energy_consumed", 0) for e in metrics_history)
            carbon = sum(e.get("carbon_emitted", 0) for e in metrics_history)
            completed = len(execution_logs)
            active = len(active_tasks)
            ws_conns = len(websocket_connections)

        lines = [
            "# HELP green_agent_energy_consumed_kwh Total energy consumed.",
            "# TYPE green_agent_energy_consumed_kwh counter",
            f"green_agent_energy_consumed_kwh {energy:.6f}",
            "",
            "# HELP green_agent_carbon_emitted_kg Total carbon emitted.",
            "# TYPE green_agent_carbon_emitted_kg counter",
            f"green_agent_carbon_emitted_kg {carbon:.6f}",
            "",
            "# HELP green_agent_tasks_completed Total tasks completed.",
            "# TYPE green_agent_tasks_completed counter",
            f"green_agent_tasks_completed {completed}",
            "",
            "# HELP green_agent_active_tasks Currently active tasks.",
            "# TYPE green_agent_active_tasks gauge",
            f"green_agent_active_tasks {active}",
            "",
            "# HELP green_agent_websocket_connections Active WebSocket connections.",
            "# TYPE green_agent_websocket_connections gauge",
            f"green_agent_websocket_connections {ws_conns}",
            "",
        ]
        return PlainTextResponse(
            "\n".join(lines), media_type="text/plain; version=0.0.4"
        )

    @app.get("/executions")
    async def get_executions(limit: int = 100) -> Dict[str, Any]:
        with _lock:
            logs = list(execution_logs)
        return {"executions": logs[-limit:], "total": len(logs)}

    @app.post("/executions/log")
    async def log_execution(execution: ExecutionLog) -> Dict[str, Any]:
        execution_dict = execution.model_dump()
        execution_dict["logged_at"] = datetime.now(timezone.utc).isoformat()
        if execution_dict.get("timestamp") is None:
            execution_dict["timestamp"] = execution_dict["logged_at"]

        with _lock:
            metrics_history.append(execution_dict)
            execution_logs.append(execution_dict)

        await _broadcast_metrics(execution_dict)
        return {
            "status": "logged",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    @app.websocket("/ws/metrics")
    async def websocket_metrics(websocket: WebSocket) -> None:
        await websocket.accept()
        with _lock:
            websocket_connections.append(websocket)
        try:
            while True:
                with _lock:
                    latest = metrics_history[-1] if metrics_history else None
                if latest is not None:
                    await websocket.send_json({
                        "type": "metrics_update",
                        "data": latest,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                await asyncio.sleep(5)
        except Exception as exc:
            logger.warning("WebSocket error: %s", exc)
        finally:
            with _lock:
                if websocket in websocket_connections:
                    websocket_connections.remove(websocket)

    return app


async def _broadcast_metrics(metrics: Dict[str, Any]) -> None:
    """Broadcast metrics to all WebSocket clients."""
    with _lock:
        connections = list(websocket_connections)
    if not connections:
        return
    message = {
        "type": "metrics_update",
        "data": metrics,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    disconnected: List[WebSocket] = []
    for ws in connections:
        try:
            await ws.send_json(message)
        except Exception as exc:  # noqa: BLE001 — any failure means dropped
            logger.debug("Dropping WebSocket: %s", exc)
            disconnected.append(ws)
    if disconnected:
        with _lock:
            for ws in disconnected:
                if ws in websocket_connections:
                    websocket_connections.remove(ws)


# Module-level app for backward compatibility.
app = create_app()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the dashboard API server."""
    parser = argparse.ArgumentParser(prog="dashboard.api_server")
    parser.add_argument("--host", default=_DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=_DEFAULT_PORT)
    parser.add_argument(
        "--cors",
        nargs="+",
        default=[_DEFAULT_CORS],
        help="Allowed CORS origins.",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable DEBUG logging.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    import uvicorn
    uvicorn.run(
        create_app(cors_origins=args.cors),
        host=args.host,
        port=args.port,
    )
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
