# src/agentbeats/server.py

"""
A2A FastAPI Server

Exposes the Green Sustainability Agent over an A2A-compatible HTTP API:

- ``POST /a2a/task``            — submit an assessment request
- ``GET  /a2a/task/{task_id}``  — poll task status / result
- ``GET  /health``              — liveness + readiness probe
- ``GET  /metrics``             — Prometheus text-format metrics

Original behaviour preserved
----------------------------
- ``app`` is a module-level :class:`FastAPI` instance.
- ``agent`` is a module-level :class:`GreenSustainabilityAgent`.
- ``python server.py`` starts uvicorn on ``0.0.0.0:8000``.

Enhancements
------------
- Full Pydantic models for requests, responses, and task records.
- Bounded, thread-safe task registry with TTL-based eviction.
- All previously ``pass``-ing endpoints now implemented.
- Prometheus text exposition for ``/metrics``.
- Structured error handling (``GreenAgentError`` / ``A2AHandlerError`` /
  ``ValidationError``) with proper HTTP status codes.
- Request-ID middleware for end-to-end tracing.
- Configurable via :class:`ServerConfig` (no magic numbers).
- Graceful lifespan startup/shutdown.
- ``__main__`` smoke test using ``TestClient``.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, AsyncIterator, Dict, List, Mapping, Optional

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field, field_validator

# ---- Local imports (support both package and script-style invocation) ------ #
try:
    from .green_agent import (
        GreenAgentError,
        GreenSustainabilityAgent,
    )
except ImportError:  # pragma: no cover — script-style fallback
    from green_agent import (  # type: ignore
        GreenAgentError,
        GreenSustainabilityAgent,
    )

# Optional dependency on the A2A handler (for its error type).
try:
    from .a2a_handler import A2AHandlerError
except ImportError:  # pragma: no cover
    try:
        from a2a_handler import A2AHandlerError  # type: ignore
    except ImportError:
        class A2AHandlerError(ValueError):  # type: ignore[no-redef]
            """Fallback shim when a2a_handler is unavailable."""

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ServerConfig:
    """Tunable parameters for the A2A server."""

    host: str = "0.0.0.0"
    port: int = 8000
    cors_origins: tuple = ("*",)
    max_tasks_in_memory: int = 1000
    task_ttl_seconds: float = 3600.0
    request_timeout_seconds: float = 60.0
    service_name: str = "green-agent-a2a"

    def __post_init__(self) -> None:
        if not (1 <= self.port <= 65_535):
            raise ValueError(f"port must be in [1, 65535], got {self.port}.")
        if self.max_tasks_in_memory <= 0:
            raise ValueError("max_tasks_in_memory must be > 0.")
        if self.task_ttl_seconds <= 0:
            raise ValueError("task_ttl_seconds must be > 0.")
        if self.request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be > 0.")


# --------------------------------------------------------------------------- #
# Pydantic models
# --------------------------------------------------------------------------- #
class AgentResult(BaseModel):
    """One agent's row inside an assessment request."""

    agent_id: str = Field(..., min_length=1, description="Unique agent identifier")
    accuracy: float = Field(..., ge=0.0, le=1.0, description="Task accuracy")
    energy_kwh: float = Field(..., ge=0.0, description="Energy consumed (kWh)")
    carbon_kg: float = Field(..., ge=0.0, description="Carbon emitted (kg CO2e)")
    latency_ms: float = Field(..., ge=0.0, description="Task latency (ms)")

    @field_validator("accuracy", "energy_kwh", "carbon_kg", "latency_ms")
    @classmethod
    def _finite(cls, v: float) -> float:
        import math

        if math.isnan(v) or math.isinf(v):
            raise ValueError("value must be finite")
        return float(v)


class AssessmentRequest(BaseModel):
    """Payload for ``POST /a2a/task``."""

    agent_url: Optional[str] = Field(
        None, description="Optional remote agent URL for A2A forwarding"
    )
    results: List[AgentResult] = Field(..., min_length=1)
    label: Optional[str] = Field(None, max_length=128)


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskRecord(BaseModel):
    """In-memory record of a submitted task."""

    task_id: str
    status: TaskStatus
    created_at: float
    updated_at: float
    request: AssessmentRequest
    response: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class TaskAcceptedResponse(BaseModel):
    task_id: str
    status: TaskStatus


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    uptime_seconds: float
    tasks_in_memory: int


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    request_id: Optional[str] = None


# --------------------------------------------------------------------------- #
# Task registry (bounded + TTL)
# --------------------------------------------------------------------------- #
class TaskRegistry:
    """Thread-safe, bounded, TTL-evicting store of :class:`TaskRecord`."""

    def __init__(self, max_size: int, ttl_seconds: float) -> None:
        import threading

        self._lock = threading.RLock()
        self._max_size = max_size
        self._ttl = ttl_seconds
        # ``OrderedDict`` preserves insertion order → O(1) eviction of oldest.
        self._tasks: "OrderedDict[str, TaskRecord]" = OrderedDict()

    def add(self, record: TaskRecord) -> None:
        with self._lock:
            self._tasks[record.task_id] = record
            self._evict_expired_locked()
            while len(self._tasks) > self._max_size:
                self._tasks.popitem(last=False)

    def get(self, task_id: str) -> Optional[TaskRecord]:
        with self._lock:
            self._evict_expired_locked()
            return self._tasks.get(task_id)

    def update(self, task_id: str, **fields: Any) -> Optional[TaskRecord]:
        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                return None
            updated = record.model_copy(
                update={**fields, "updated_at": time.time()}
            )
            self._tasks[task_id] = updated
            return updated

    def size(self) -> int:
        with self._lock:
            self._evict_expired_locked()
            return len(self._tasks)

    def _evict_expired_locked(self) -> None:
        now = time.time()
        expired = [
            tid for tid, rec in self._tasks.items()
            if now - rec.updated_at > self._ttl
        ]
        for tid in expired:
            self._tasks.pop(tid, None)


# --------------------------------------------------------------------------- #
# Application state
# --------------------------------------------------------------------------- #
_config = ServerConfig()
_agent = GreenSustainabilityAgent()
_registry = TaskRegistry(
    max_size=_config.max_tasks_in_memory,
    ttl_seconds=_config.task_ttl_seconds,
)
_started_at = time.time()
_request_counter = {"total": 0, "errors": 0}


# --------------------------------------------------------------------------- #
# Lifespan
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info(
        "%s starting on %s:%d (max_tasks=%d, ttl=%.0fs)",
        _config.service_name,
        _config.host,
        _config.port,
        _config.max_tasks_in_memory,
        _config.task_ttl_seconds,
    )
    try:
        yield
    finally:
        logger.info(
            "%s shutting down (uptime=%.1fs, tasks=%d)",
            _config.service_name,
            time.time() - _started_at,
            _registry.size(),
        )


# --------------------------------------------------------------------------- #
# FastAPI app
# --------------------------------------------------------------------------- #
app = FastAPI(
    title="Green Agent A2A Server",
    version="2.4.0",
    description=(
        "A2A-compatible HTTP interface for the Green Sustainability Agent. "
        "Scores agents using Pareto optimality across accuracy, energy, "
        "carbon, and latency objectives."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=list(_config.cors_origins),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------------------------------------------------------------------- #
# Middleware: request-id + timing
# --------------------------------------------------------------------------- #
@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
    start = time.perf_counter()

    _request_counter["total"] += 1

    try:
        response = await call_next(request)
    except Exception:
        _request_counter["errors"] += 1
        logger.exception("Unhandled error on %s %s", request.method, request.url.path)
        raise

    elapsed_ms = (time.perf_counter() - start) * 1000.0
    response.headers["x-request-id"] = request_id
    response.headers["x-response-time-ms"] = f"{elapsed_ms:.2f}"

    logger.info(
        "%s %s -> %d in %.2fms (rid=%s)",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
        request_id,
    )
    return response


# --------------------------------------------------------------------------- #
# Exception handlers
# --------------------------------------------------------------------------- #
@app.exception_handler(RequestValidationError)
async def _validation_handler(request: Request, exc: RequestValidationError):
    _request_counter["errors"] += 1
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=ErrorResponse(
            error="validation_error",
            detail=str(exc.errors()),
            request_id=request.headers.get("x-request-id"),
        ).model_dump(),
    )


@app.exception_handler(GreenAgentError)
async def _green_agent_handler(request: Request, exc: GreenAgentError):
    _request_counter["errors"] += 1
    logger.warning("GreenAgentError: %s", exc)
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=ErrorResponse(
            error="green_agent_error",
            detail=str(exc),
            request_id=request.headers.get("x-request-id"),
        ).model_dump(),
    )


@app.exception_handler(A2AHandlerError)
async def _a2a_handler_error(request: Request, exc: A2AHandlerError):
    _request_counter["errors"] += 1
    logger.warning("A2AHandlerError: %s", exc)
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=ErrorResponse(
            error="a2a_handler_error",
            detail=str(exc),
            request_id=request.headers.get("x-request-id"),
        ).model_dump(),
    )


# --------------------------------------------------------------------------- #
# Dependency: shared agent
# --------------------------------------------------------------------------- #
def get_agent() -> GreenSustainabilityAgent:
    return _agent


def get_registry() -> TaskRegistry:
    return _registry


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@app.post(
    "/a2a/task",
    response_model=Dict[str, Any],
    status_code=status.HTTP_200_OK,
    summary="Submit an assessment request",
)
async def receive_task(
    request: AssessmentRequest,
    agent: GreenSustainabilityAgent = Depends(get_agent),
    registry: TaskRegistry = Depends(get_registry),
) -> Dict[str, Any]:
    """
    Receive an A2A assessment request and score it synchronously.

    The task is recorded in the in-memory registry so that its result can
    later be retrieved via ``GET /a2a/task/{task_id}``.
    """
    task_id = uuid.uuid4().hex
    now = time.time()

    record = TaskRecord(
        task_id=task_id,
        status=TaskStatus.RUNNING,
        created_at=now,
        updated_at=now,
        request=request,
    )
    registry.add(record)

    try:
        # The enhanced GreenSustainabilityAgent exposes ``score_with_pareto``;
        # if a future revision adds a higher-level ``handle_assessment_request``,
        # we prefer it.
        handler = getattr(agent, "handle_assessment_request", None)
        if callable(handler):
            result = await handler(request.model_dump())
        else:
            result = await agent.score_with_pareto(
                [r.model_dump() for r in request.results],
                label=request.label,
            )

        registry.update(
            task_id,
            status=TaskStatus.COMPLETED,
            response=result,
        )
        return {"task_id": task_id, **result}

    except Exception as exc:
        logger.exception("Task %s failed: %s", task_id, exc)
        registry.update(task_id, status=TaskStatus.FAILED, error=str(exc))
        raise


@app.get(
    "/a2a/task/{task_id}",
    response_model=TaskRecord,
    summary="Get the status / result of a submitted task",
)
async def get_task_status(
    task_id: str,
    registry: TaskRegistry = Depends(get_registry),
) -> TaskRecord:
    """Return the stored record for ``task_id``, or 404 if unknown/expired."""
    record = registry.get(task_id)
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task '{task_id}' not found or expired.",
        )
    return record


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness & readiness probe",
)
async def health_check(
    registry: TaskRegistry = Depends(get_registry),
) -> HealthResponse:
    """Return ``{"status": "healthy", ...}`` with basic service info."""
    return HealthResponse(
        status="healthy",
        service=_config.service_name,
        version=app.version,
        uptime_seconds=time.time() - _started_at,
        tasks_in_memory=registry.size(),
    )


@app.get(
    "/metrics",
    response_class=PlainTextResponse,
    summary="Prometheus text exposition",
)
async def metrics(
    registry: TaskRegistry = Depends(get_registry),
) -> PlainTextResponse:
    """Expose basic metrics in Prometheus text format (version 0.0.4)."""
    uptime = time.time() - _started_at
    stats = _agent.statistics()

    lines: List[str] = [
        "# HELP green_agent_uptime_seconds Process uptime in seconds.",
        "# TYPE green_agent_uptime_seconds gauge",
        f"green_agent_uptime_seconds {uptime:.3f}",
        "",
        "# HELP green_agent_requests_total Total HTTP requests received.",
        "# TYPE green_agent_requests_total counter",
        f"green_agent_requests_total {_request_counter['total']}",
        "",
        "# HELP green_agent_errors_total Total error responses emitted.",
        "# TYPE green_agent_errors_total counter",
        f"green_agent_errors_total {_request_counter['errors']}",
        "",
        "# HELP green_agent_tasks_in_memory Current task records held in memory.",
        "# TYPE green_agent_tasks_in_memory gauge",
        f"green_agent_tasks_in_memory {registry.size()}",
        "",
        "# HELP green_agent_scorings_total Total score_with_pareto invocations.",
        "# TYPE green_agent_scorings_total counter",
        f"green_agent_scorings_total {stats.get('count') or 0}",
        "",
    ]

    if stats.get("mean_agents") is not None:
        lines.extend([
            "# HELP green_agent_mean_agents Mean agents per assessment.",
            "# TYPE green_agent_mean_agents gauge",
            f"green_agent_mean_agents {stats['mean_agents']:.4f}",
            "",
        ])
    if stats.get("mean_frontier_ratio") is not None:
        lines.extend([
            "# HELP green_agent_mean_frontier_ratio Mean frontier / total agent ratio.",
            "# TYPE green_agent_mean_frontier_ratio gauge",
            f"green_agent_mean_frontier_ratio {stats['mean_frontier_ratio']:.4f}",
            "",
        ])

    return PlainTextResponse("\n".join(lines), media_type="text/plain; version=0.0.4")


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def main() -> None:  # pragma: no cover — trivial wiring
    import uvicorn

    uvicorn.run(
        app,
        host=_config.host,
        port=_config.port,
        log_level="info",
    )


if __name__ == "__main__":  # pragma: no cover
    # When invoked as ``python -m agentbeats.server --smoke``, run the smoke
    # test instead of the live server (useful for CI).
    import sys

    if "--smoke" in sys.argv:
        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
        )
        from fastapi.testclient import TestClient

        with TestClient(app) as client:
            # Health
            r = client.get("/health")
            assert r.status_code == 200 and r.json()["status"] == "healthy"
            print("health   :", r.json())

            # Submit a task
            payload = {
                "results": [
                    {"agent_id": "alpha", "accuracy": 0.95,
                     "energy_kwh": 0.010, "carbon_kg": 0.0030, "latency_ms": 120.0},
                    {"agent_id": "beta", "accuracy": 0.90,
                     "energy_kwh": 0.003, "carbon_kg": 0.0010, "latency_ms": 90.0},
                ],
                "label": "smoke",
            }
            r = client.post("/a2a/task", json=payload)
            assert r.status_code == 200, r.text
            body = r.json()
            task_id = body["task_id"]
            print("task_id  :", task_id)
            print("frontier :", [p["agent_id"] for p in body["frontier"]])

            # Poll status
            r = client.get(f"/a2a/task/{task_id}")
            assert r.status_code == 200
            assert r.json()["status"] == "completed"
            print("status   :", r.json()["status"])

            # Metrics
            r = client.get("/metrics")
            assert r.status_code == 200 and "green_agent_uptime_seconds" in r.text
            print("metrics  : OK")

            # 404 for unknown task
            r = client.get("/a2a/task/does-not-exist")
            assert r.status_code == 404
            print("404 path : OK")

            # Validation error
            r = client.post("/a2a/task", json={"results": []})
            assert r.status_code == 422
            print("422 path : OK")

        print("Smoke test passed.")
    else:
        main()
