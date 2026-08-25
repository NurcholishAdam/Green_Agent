#!/usr/bin/env python3
"""
Decision Audit & Dashboard Persistence
=======================================
Enhanced FastAPI server for decision audit, benchmark results, drift events,
MoE expert metrics, and MODP Pareto front – with central component integration.
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

# Central Green Agent components
from ..storage import Storage
from ..config import config
from ..logger import logger
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry

# --------------------------------------------------------------------------
# Security
# --------------------------------------------------------------------------
security = HTTPBearer()

async def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Verify that the provided Bearer token matches the configured API key.
    If DASHBOARD_API_KEY is empty, authentication is disabled (for local dev).
    """
    if not config.DASHBOARD_API_KEY:
        return "local"
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

class ExpertMetricsResponse(BaseModel):
    expert_id: str
    usage: int
    success_rate: float
    avg_latency_ms: float
    p95_latency_ms: float

class ParetoFrontResponse(BaseModel):
    expert_id: str
    quality_score: float
    carbon_g: float
    latency_ms: float
    energy_joules: float

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
# Main DecisionAudit class (Enhanced)
# --------------------------------------------------------------------------
class DecisionAudit:
    """
    Handles high‑fidelity decision logging and exposure via a FastAPI REST API.
    Enhanced with authentication, pagination, filtering, MoE metrics, and MODP.
    """

    def __init__(
        self,
        storage: Storage,
        adaptive_cost: Optional[AdaptiveCostFunction] = None,
        pareto_gating: Optional[ParetoGating] = None,
        drift_detector: Optional[DriftDetector] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        metrics: Optional[MetricsRegistry] = None,
        bio_core: Optional[Any] = None,
    ):
        self.storage = storage
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.queue = message_queue
        self.metrics = metrics
        self.bio_core = bio_core

        self._app: Optional[FastAPI] = None
        self._server: Optional[uvicorn.Server] = None
        self._server_thread: Optional[threading.Thread] = None
        self._running = False
        self.router = APIRouter()
        self._setup_routes()
        self.cors_origins = []
        if config.DASHBOARD_CORS_ORIGINS:
            self.cors_origins = [origin.strip() for origin in config.DASHBOARD_CORS_ORIGINS.split(",")]
        else:
            self.cors_origins = ["*"]

    def _setup_routes(self):
        """Define all API routes."""

        # ----- Health (public) -----
        @self.router.get("/health")
        async def health():
            return {"status": "healthy", "service": "green-agent-audit", "version": "3.3.0"}

        # ----- Decisions -----
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
            """Retrieve feedback events with pagination and filters."""
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
            except AttributeError:
                raise HTTPException(status_code=501, detail="Storage method not implemented")
            except Exception as e:
                logger.exception("Error fetching decisions")
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.router.get("/decisions/{task_id}")
        async def get_decision(task_id: str, api_key: str = Depends(verify_api_key)):
            """Retrieve a single feedback event by its task_id."""
            try:
                event = self.storage.get_feedback_event_by_task_id(task_id)
                if not event:
                    raise HTTPException(status_code=404, detail="Decision not found")
                return {"status": "success", "event": event}
            except AttributeError:
                raise HTTPException(status_code=501, detail="Storage method not implemented")
            except HTTPException:
                raise
            except Exception:
                logger.exception(f"Error fetching decision for task_id={task_id}")
                raise HTTPException(status_code=500, detail="Internal server error")

        # ----- Benchmarks -----
        @self.router.get("/benchmark/latest", response_model=List[BenchmarkResultResponse])
        async def get_latest_benchmarks(api_key: str = Depends(verify_api_key)):
            """Return the most recent benchmark result for each policy."""
            try:
                runs = self.storage.get_benchmark_results(days_back=7)
                if not runs:
                    return []
                latest = {}
                for run in runs:
                    policy = run.get("policy_name")
                    if policy is None:
                        continue
                    if policy not in latest or run.get("timestamp", 0) > latest[policy].get("timestamp", 0):
                        latest[policy] = run
                results = []
                for policy, run in latest.items():
                    results.append({
                        "run_id": run.get("run_id", ""),
                        "policy_name": policy,
                        "timestamp": run.get("timestamp", 0),
                        "sample_count": run.get("sample_count", 0),
                        "metrics": {
                            "quality": run.get("avg_quality", 0.0),
                            "carbon": run.get("avg_carbon", 0.0),
                            "latency": run.get("avg_latency", 0.0),
                            "energy": run.get("total_energy", 0.0),
                            "cost": run.get("avg_cost", 0.0),
                        },
                        "confidence_intervals": run.get("confidence_intervals", {}),
                        "p_value": run.get("p_value"),
                    })
                return results
            except AttributeError:
                raise HTTPException(status_code=501, detail="Storage method not implemented")
            except Exception:
                logger.exception("Error fetching benchmark results")
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.router.get("/benchmark/history")
        async def get_benchmark_history(
            policy: Optional[str] = Query(None),
            limit: int = Query(100, ge=1),
            api_key: str = Depends(verify_api_key),
        ):
            """Return historical benchmark runs, optionally filtered by policy."""
            try:
                runs = self.storage.get_benchmark_results(days_back=30)
                if policy:
                    runs = [r for r in runs if r.get("policy_name") == policy]
                runs = sorted(runs, key=lambda r: r.get("timestamp", 0), reverse=True)[:limit]
                return {"status": "success", "count": len(runs), "runs": runs}
            except AttributeError:
                raise HTTPException(status_code=501, detail="Storage method not implemented")
            except Exception:
                logger.exception("Error fetching benchmark history")
                raise HTTPException(status_code=500, detail="Internal server error")

        # ----- Drift events -----
        @self.router.get("/drift/events", response_model=List[DriftEventResponse])
        async def get_drift_events(
            limit: int = Query(50, ge=1),
            api_key: str = Depends(verify_api_key),
        ):
            """Retrieve recent drift detection events."""
            try:
                events = self.storage.get_drift_events(limit=limit)
                return events
            except AttributeError:
                raise HTTPException(status_code=501, detail="Storage method not implemented")
            except Exception:
                logger.exception("Error fetching drift events")
                raise HTTPException(status_code=500, detail="Internal server error")

        # ----- MoE Expert Metrics (NEW) -----
        @self.router.get("/experts/metrics", response_model=List[ExpertMetricsResponse])
        async def get_expert_metrics(
            api_key: str = Depends(verify_api_key),
        ):
            """Return per‑expert usage, success rate, and latency."""
            try:
                usage = self.storage.get_expert_usage() if hasattr(self.storage, 'get_expert_usage') else {}
                success = self.storage.get_expert_success_rate() if hasattr(self.storage, 'get_expert_success_rate') else {}
                latency = self.storage.get_expert_latency_stats() if hasattr(self.storage, 'get_expert_latency_stats') else {}
                experts = set(usage.keys()) | set(success.keys()) | set(latency.keys())
                result = []
                for eid in experts:
                    result.append({
                        "expert_id": eid,
                        "usage": usage.get(eid, 0),
                        "success_rate": success.get(eid, 0.0),
                        "avg_latency_ms": latency.get(eid, {}).get("avg_ms", 0.0),
                        "p95_latency_ms": latency.get(eid, {}).get("p95_ms", 0.0),
                    })
                return result
            except Exception:
                logger.exception("Error fetching expert metrics")
                raise HTTPException(status_code=500, detail="Internal server error")

        # ----- MODP Pareto Front (NEW) -----
        @self.router.get("/mopd/pareto-front", response_model=List[ParetoFrontResponse])
        async def get_pareto_front(
            api_key: str = Depends(verify_api_key),
        ):
            """Return the current Pareto front of expert configurations."""
            if self.pareto is None:
                raise HTTPException(status_code=404, detail="ParetoGating not configured")
            try:
                front = self.pareto.get_front() if hasattr(self.pareto, 'get_front') else []
                result = []
                for item in front:
                    result.append({
                        "expert_id": item.get("expert_id", "unknown"),
                        "quality_score": item.get("quality_score", 0.0),
                        "carbon_g": item.get("carbon_g", 0.0),
                        "latency_ms": item.get("latency_ms", 0.0),
                        "energy_joules": item.get("energy_joules", 0.0),
                    })
                return result
            except Exception:
                logger.exception("Error fetching Pareto front")
                raise HTTPException(status_code=500, detail="Internal server error")

        # ----- Adaptive Cost Weights (NEW) -----
        @self.router.get("/mopd/weights")
        async def get_adaptive_weights(api_key: str = Depends(verify_api_key)):
            """Get current adaptive cost weights."""
            if self.adaptive_cost is None:
                raise HTTPException(status_code=404, detail="AdaptiveCostFunction not configured")
            return {"weights": self.adaptive_cost.get_current_weights()}

        @self.router.post("/mopd/weights")
        async def set_adaptive_weights(
            weights: Dict[str, float],
            api_key: str = Depends(verify_api_key),
        ):
            """Update adaptive cost weights (admin only)."""
            if self.adaptive_cost is None:
                raise HTTPException(status_code=404, detail="AdaptiveCostFunction not configured")
            try:
                self.adaptive_cost.update_weights(weights)
                return {"status": "success", "weights": self.adaptive_cost.get_current_weights()}
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

    # --------------------------------------------------------------------------
    # Lifecycle management
    # --------------------------------------------------------------------------
    async def start_dashboard(self):
        """Start the FastAPI server in a background thread."""
        if not config.DASHBOARD_ENABLED:
            logger.info("Dashboard disabled by config.")
            return
        if self._running:
            logger.warning("Dashboard already running.")
            return

        # Create FastAPI app
        self._app = FastAPI(
            title="Green Agent Audit Dashboard",
            version="3.3.0",
            description="API for decision audit, benchmarks, drift events, and MoE metrics.",
        )

        # Add CORS middleware (before routes)
        self._app.add_middleware(
            CORSMiddleware,
            allow_origins=self.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Add request logging middleware (before routes)
        self._app.middleware("http")(log_requests)

        # Include router
        self._app.include_router(self.router, prefix="/api/v1")

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
            try:
                loop.run_until_complete(self._server.serve())
            finally:
                loop.close()

        self._server_thread = threading.Thread(target=run_server, daemon=True)
        self._server_thread.start()
        self._running = True
        logger.info(f"Audit dashboard started on port {config.DASHBOARD_PORT}")

    async def stop_dashboard(self):
        """Gracefully stop the dashboard server."""
        if not self._running:
            logger.info("Dashboard not running.")
            return
        if self._server:
            self._server.should_exit = True
        if self._server_thread and self._server_thread.is_alive():
            # Wait a short time for the thread to exit
            self._server_thread.join(timeout=5)
        self._running = False
        logger.info("Dashboard stopped.")
