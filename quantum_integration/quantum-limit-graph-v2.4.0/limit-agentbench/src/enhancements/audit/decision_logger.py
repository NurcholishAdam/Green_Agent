"""
Decision Audit & Dashboard persistence.
Provides structured logging of all decisions and a FastAPI endpoint.
"""
from fastapi import FastAPI, APIRouter
from pydantic import BaseModel
import uvicorn
from typing import List, Optional
from ..storage import Storage
from ..config import config
from ..logger import logger
import threading

class DecisionAudit:
    """Handles high-fidelity decision logging and exposure via REST API."""

    def __init__(self, storage: Storage):
        self.storage = storage
        self._app = None
        self._server_thread = None
        self.router = APIRouter()
        self._setup_routes()

    def _setup_routes(self):
        @self.router.get("/decisions")
        async def get_decisions(limit: int = 100):
            events = self.storage.get_feedback_events(limit)
            return {"status": "success", "count": len(events), "events": events}

        @self.router.get("/decisions/{task_id}")
        async def get_decision(task_id: str):
            # simplified, in real implementation query by task_id
            return {"status": "success", "task_id": task_id}

        @self.router.get("/health")
        async def health():
            return {"status": "healthy", "service": "green-agent-audit"}

    def start_dashboard(self):
        """Start FastAPI server in background thread."""
        if not config.DASHBOARD_ENABLED:
            logger.info("Dashboard disabled by config.")
            return

        self._app = FastAPI(title="Green Agent Audit Dashboard")
        self._app.include_router(self.router, prefix="/api/v1")
        
        def run_server():
            uvicorn.run(
                self._app,
                host="0.0.0.0",
                port=config.DASHBOARD_PORT,
                log_level="info"
            )
        
        self._server_thread = threading.Thread(target=run_server, daemon=True)
        self._server_thread.start()
        logger.info(f"Audit dashboard started on port {config.DASHBOARD_PORT}")

    def stop_dashboard(self):
        if self._server_thread:
            # uvicorn doesn't have a clean stop in threading, but we can handle gracefully
            logger.info("Stopping dashboard...")
