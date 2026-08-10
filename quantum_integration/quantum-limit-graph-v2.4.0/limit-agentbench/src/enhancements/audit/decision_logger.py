"""
Decision Audit & Dashboard Persistence
=======================================
Enhanced FastAPI server for decision audit, benchmark results, and drift events.
"""
import asyncio
import threading
import time
import uuid
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, APIRouter, Depends, HTTPException, Query, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn
from ..storage import Storage
from ..config import config
from ..logger import logger

# --------------------------------------------------------------------------
# Security
# --------------------------------------------------------------------------
security = HTTPBearer()

async def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Verify that the provided Bearer token matches the configured API key.
    """
    if credentials.credentials != config.DASHBOARD_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return credentials.credentials

# --------------------------------------------------------------------------
# Pydantic response models
# --------------------------------------------------------------------------
class FeedbackEventResponse(BaseModel):
    event_id: str
    timestamp: float
    task_id: str
    model_id: Optional[str] = None
    teacher_id: Optional[str] = None
    selected_action: str
    quality_score: float
    latency_ms: float
    energy_joules: float
    carbon_g: float
    helium_cost: Optional[float] = None
    feedback_type: str
    adaptive_cost_value: float
    metadata: Dict[str, Any] = Field(default_factory=dict)

class DecisionListResponse(BaseModel):
    status: str
    count: int
    events: List[FeedbackEventResponse]

class BenchmarkResultResponse(BaseModel):
    run_id: str
    policy_name: str
    timestamp: float
    sample_count: int
    metrics: Dict[str, float]
    confidence_intervals: Dict[str, List[float]]
    p_value: Optional[float] = None

class DriftEventResponse(BaseModel):
    snapshot_id: str
    timestamp: float
    reason: str
    cost_score: float

# --------------------------------------------------------------------------
# Request logging middleware
# --------------------------------------------------------------------------
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    logger.info(
        "API request",
        method=request.method,
        path=request.url.path,
        status=response.status_code,
        duration_ms=round(duration * 1000, 2),
        client=request.client.host if request.client else None,
    )
    return response

# --------------------------------------------------------------------------
# Main DecisionAudit class
# --------------------------------------------------------------------------
class DecisionAudit:
    """
    Handles high‑fidelity decision logging and exposure via a FastAPI REST API.
    Enhanced with authentication, pagination, filtering, and integration with
    benchmarks and drift detection.
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        self._app: Optional[FastAPI] = None
        self._server: Optional[uvicorn.Server] = None
        self._server_thread: Optional[threading.Thread] = None
        self._stop_event = asyncio.Event()
        self.router = APIRouter()
        self._setup_routes()
        self._setup_middleware()

    def _setup_middleware(self):
        """Add CORS and request logging middleware."""
        # CORS – allow origins from config (comma‑separated)
        origins = []
        if config.DASHBOARD_CORS_ORIGINS:
            origins = [origin.strip() for origin in config.DASHBOARD_CORS_ORIGINS.split(",")]
        else:
            origins = ["*"]

        # We'll add middleware to the app when it's created, not to router.
        # So we store the origins for later.
        self.cors_origins = origins

    def _setup_routes(self):
        """Define all API routes."""

        # 1. Decisions endpoints
        @self.router.get("/decisions", response_model=DecisionListResponse)
        async def get_decisions(
            limit: int = Query(100, ge=1, le=1000),
            offset: int = Query(0, ge=0),
            feedback_type: Optional[str] = Query(None),
            teacher_id: Optional[str] = Query(None),
            task_id: Optional[str] = Query(None),
            start_timestamp: Optional[float] = Query(None),
            end_timestamp: Optional[float] = Query(None),
            api_key: str = Depends(verify_api_key),
        ):
            """
            Retrieve feedback events with pagination and filters.
            """
            try:
                events = self.storage.get_feedback_events(
                    limit=limit,
                    offset=offset,
                    feedback_type=feedback_type,
                    teacher_id=teacher_id,
                    task_id=task_id,
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                )
                return {"status": "success", "count": len(events), "events": events}
            except Exception as e:
                logger.exception("Error fetching decisions")
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.router.get("/decisions/{task_id}")
        async def get_decision(task_id: str, api_key: str = Depends(verify_api_key)):
            """
            Retrieve a single feedback event by its task_id.
            """
            try:
                event = self.storage.get_feedback_event_by_task_id(task_id)
                if not event:
                    raise HTTPException(status_code=404, detail="Decision not found")
                return {"status": "success", "event": event}
            except HTTPException:
                raise
            except Exception as e:
                logger.exception(f"Error fetching decision for task_id={task_id}")
                raise HTTPException(status_code=500, detail="Internal server error")

        # 2. Benchmark endpoints
        @self.router.get("/benchmark/latest", response_model=List[BenchmarkResultResponse])
        async def get_latest_benchmarks(api_key: str = Depends(verify_api_key)):
            """
            Return the most recent benchmark result for each policy.
            """
            try:
                runs = self.storage.get_benchmark_results(days_back=7)
                if not runs:
                    return []
                # Group by policy and pick the latest
                latest = {}
                for run in runs:
                    policy = run["policy_name"]
                    if policy not in latest or run["timestamp"] > latest[policy]["timestamp"]:
                        latest[policy] = run
                # Convert to response model
                results = []
                for policy, run in latest.items():
                    results.append({
                        "run_id": run["run_id"],
                        "policy_name": policy,
                        "timestamp": run["timestamp"],
                        "sample_count": run["sample_count"],
                        "metrics": {
                            "quality": run["avg_quality"],
                            "carbon": run["avg_carbon"],
                            "latency": run["avg_latency"],
                            "energy": run["total_energy"],
                            "cost": run["avg_cost"],
                        },
                        "confidence_intervals": {},  # if stored, would retrieve from metadata
                        "p_value": None,
                    })
                return results
            except Exception as e:
                logger.exception("Error fetching benchmark results")
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.router.get("/benchmark/history")
        async def get_benchmark_history(
            policy: Optional[str] = Query(None),
            limit: int = Query(100, ge=1),
            api_key: str = Depends(verify_api_key),
        ):
            """
            Return historical benchmark runs, optionally filtered by policy.
            """
            try:
                runs = self.storage.get_benchmark_results(days_back=30)  # last 30 days
                if policy:
                    runs = [r for r in runs if r["policy_name"] == policy]
                # Sort by timestamp descending and truncate
                runs = sorted(runs, key=lambda r: r["timestamp"], reverse=True)[:limit]
                return {"status": "success", "count": len(runs), "runs": runs}
            except Exception as e:
                logger.exception("Error fetching benchmark history")
                raise HTTPException(status_code=500, detail="Internal server error")

        # 3. Drift events endpoints
        @self.router.get("/drift/events", response_model=List[DriftEventResponse])
        async def get_drift_events(
            limit: int = Query(50, ge=1),
            api_key: str = Depends(verify_api_key),
        ):
            """
            Retrieve recent drift detection events.
            """
            try:
                events = self.storage.get_drift_events(limit=limit)
                return events
            except Exception as e:
                logger.exception("Error fetching drift events")
                raise HTTPException(status_code=500, detail="Internal server error")

        # 4. Health check (public, no auth)
        @self.router.get("/health")
        async def health():
            return {"status": "healthy", "service": "green-agent-audit"}

    # --------------------------------------------------------------------------
    # Lifecycle management
    # --------------------------------------------------------------------------
    async def start_dashboard(self):
        """
        Start the FastAPI server in a background thread.
        """
        if not config.DASHBOARD_ENABLED:
            logger.info("Dashboard disabled by config.")
            return

        # Create FastAPI app
        self._app = FastAPI(
            title="Green Agent Audit Dashboard",
            version="3.2.0",
            description="API for decision audit, benchmarks, and drift events.",
        )

        # Add CORS middleware
        self._app.add_middleware(
            CORSMiddleware,
            allow_origins=self.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Add request logging middleware
        self._app.middleware("http")(log_requests)

        # Include router
        self._app.include_router(self.router, prefix="/api/v1")

        # Configure uvicorn server
        config_kwargs = {
            "host": "0.0.0.0",
            "port": config.DASHBOARD_PORT,
            "log_level": "info",
            "loop": "asyncio",
        }

        def run_server():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._server = uvicorn.Server(uvicorn.Config(self._app, **config_kwargs))
            # Run server until stop event is set
            loop.run_until_complete(self._server.serve())

        self._server_thread = threading.Thread(target=run_server, daemon=True)
        self._server_thread.start()
        logger.info(f"Audit dashboard started on port {config.DASHBOARD_PORT}")

    async def stop_dashboard(self):
        """
        Gracefully stop the dashboard server.
        """
        if self._server:
            self._server.should_exit = True
        if self._server_thread and self._server_thread.is_alive():
            # Wait a short time for the thread to exit
            self._server_thread.join(timeout=5)
        logger.info("Dashboard stopped.")

# --------------------------------------------------------------------------
# Additional Storage methods (to be added to the existing Storage class)
# --------------------------------------------------------------------------
# The following methods should be added to the `Storage` class in storage.py:
#
# def get_feedback_events(self, limit=100, offset=0, feedback_type=None,
#                         teacher_id=None, task_id=None, start_timestamp=None,
#                         end_timestamp=None) -> List[Dict]:
#     # Implement query with filters and pagination
#     pass
#
# def get_feedback_event_by_task_id(self, task_id: str) -> Optional[Dict]:
#     # Retrieve a single event
#     pass
#
# def get_benchmark_results(self, days_back=7) -> List[Dict]:
#     # Retrieve benchmark runs
#     pass
#
# def get_drift_events(self, limit=50) -> List[Dict]:
#     # Retrieve drift events from drift_states table
#     pass
#
# Also, add config variables:
# DASHBOARD_API_KEY (string, required)
# DASHBOARD_CORS_ORIGINS (optional, comma-separated)
