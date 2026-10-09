#!/usr/bin/env python3
"""
Decision Audit & Dashboard Persistence v4.0.0
==============================================
Enhanced FastAPI server for decision audit, benchmark results, drift events,
MoE expert metrics, MODP Pareto front, LIMIT Graph, RLHF preference pairs,
and bio-inspired optimizer runs — with central component integration.

v4.0.0 changes (P0–P3 hardening):
P0:
  - Startup fails when DASHBOARD_API_KEY missing in production.
  - Token comparison uses hmac.compare_digest.
  - WebSocket auth via short-lived ticket (no token in URL).
  - Cross-thread locks use threading.Lock; per-loop locks created lazily.
  - No mutable default args.
  - CORS: "*" cannot be combined with allow_credentials=True.
  - Capability checks at startup; storage method 501 is explicit.
  - Uniform ErrorResponse via global exception handlers.
P1:
  - Pydantic request bodies for all POST endpoints.
  - /ready, /version, /metrics endpoints.
  - Request ID middleware + structured logs (X-Request-ID).
  - TTL + proxy-aware rate limiter, pluggable backend.
  - WebSocket broadcast wired to real events with topic filtering.
  - Cursor-based pagination on list endpoints.
  - Single source of truth for VERSION.
P2:
  - DashboardComponents dataclass + factory.
  - AuditStorageAdapter with capability checks.
  - PSO as async job with status endpoint.
  - RLHF pair validation + idempotency.
  - Adaptive weight validation + audit log.
  - WebSocket topic filtering and heartbeats.
P3:
  - OpenTelemetry tracing (optional).
  - Per-route and per-user rate limits.
  - Auth modes: none | api_key | jwt (explicit config).
  - Strict CORS env parsing.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import hashlib
import hmac
import json
import os
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import uvicorn
from fastapi import (
    APIRouter, BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query,
    Request, Response, WebSocket, WebSocketDisconnect,
)
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Central Green Agent components (with fallback)
# ---------------------------------------------------------------------------
try:
    from ..storage import Storage  # type: ignore
    from ..config import config  # type: ignore
    from ..logger import logger  # type: ignore
    from ..routing.pareto_gating import ParetoGating  # type: ignore
    from ..feedback.adaptive_cost import AdaptiveCostFunction  # type: ignore
    from ..safety.drift_detector import DriftDetector  # type: ignore
    from ..scaling.message_queue import AsyncMessageQueue  # type: ignore
    from ..metrics import MetricsRegistry  # type: ignore
    CENTRAL_AVAILABLE = True
except Exception:  # pragma: no cover - standalone fallback
    CENTRAL_AVAILABLE = False

    class _FallbackConfig:
        ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
        DASHBOARD_ENABLED = True
        DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8000"))
        DASHBOARD_API_KEY = os.getenv("DASHBOARD_API_KEY", "")
        DASHBOARD_AUTH_MODE = os.getenv("DASHBOARD_AUTH_MODE", "api_key")
        DASHBOARD_JWT_SECRET = os.getenv("DASHBOARD_JWT_SECRET", "")
        DASHBOARD_JWT_ALGORITHM = os.getenv("DASHBOARD_JWT_ALGORITHM", "HS256")
        DASHBOARD_CORS_ORIGINS = os.getenv("DASHBOARD_CORS_ORIGINS", "")
        DASHBOARD_RATE_LIMIT_DEFAULT = int(os.getenv("DASHBOARD_RATE_LIMIT_DEFAULT", "200"))
        DASHBOARD_TRUSTED_PROXIES = os.getenv("DASHBOARD_TRUSTED_PROXIES", "")

    config = _FallbackConfig()

    class Storage:  # type: ignore
        pass

    class ParetoGating:  # type: ignore
        def get_front(self):
            return []

    class AdaptiveCostFunction:  # type: ignore
        ALLOWED_WEIGHT_KEYS = {"quality", "carbon", "latency", "energy", "health", "atp"}
        def get_current_weights(self):
            return {}
        def update_weights(self, weights):
            pass

    class DriftDetector:  # type: ignore
        pass

    class AsyncMessageQueue:  # type: ignore
        async def publish(self, *a, **k):
            return None

    class MetricsRegistry:  # type: ignore
        pass

    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("dashboard")  # type: ignore

# Enhancement core (optional)
try:
    from ..core import (  # type: ignore
        LimitGraphManager, MODPOptimizer, RLHFTrainer,
        ParticleSwarmOptimizer, MoEGatingNetwork,
        GeneticHyperparameterOptimizer,
    )
    ENHANCEMENTS_CORE_AVAILABLE = True
except Exception:
    ENHANCEMENTS_CORE_AVAILABLE = False
    LimitGraphManager = MODPOptimizer = RLHFTrainer = None  # type: ignore
    ParticleSwarmOptimizer = MoEGatingNetwork = GeneticHyperparameterOptimizer = None  # type: ignore
    try:
        logger.warning("Enhanced core components not available; dashboard degraded.")  # type: ignore
    except Exception:
        pass

# OpenTelemetry (optional)
try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    _TRACER = trace.get_tracer("green_agent.dashboard")
    OTEL_AVAILABLE = True
except Exception:
    _TRACER = None
    Status = StatusCode = None  # type: ignore
    OTEL_AVAILABLE = False

# PyJWT (optional, required only for auth_mode=jwt)
try:
    import jwt as _pyjwt
    PYJWT_AVAILABLE = True
except Exception:
    _pyjwt = None
    PYJWT_AVAILABLE = False

# Prometheus (optional)
try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False


# ===========================================================================
# 0. CONSTANTS
# ===========================================================================
VERSION = "4.0.0"
SERVICE_NAME = "green-agent-audit"
DEFAULT_PAGE_LIMIT = 100
MAX_PAGE_LIMIT = 1000

# Contextvar for request id (visible to logs)
_request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")


def new_request_id() -> str:
    return uuid.uuid4().hex[:16]


def current_request_id() -> str:
    return _request_id_ctx.get()


# ===========================================================================
# 1. AUTH
# ===========================================================================
class AuthMode(str, Enum):
    NONE = "none"
    API_KEY = "api_key"
    JWT = "jwt"


@dataclass
class Principal:
    subject: str
    mode: AuthMode
    is_admin: bool = False
    claims: Dict[str, Any] = field(default_factory=dict)


class AuthConfigError(RuntimeError):
    pass


class AuthManager:
    """
    Centralised auth.
    - mode=none : only allowed when ENVIRONMENT is development-like.
    - mode=api_key : requires DASHBOARD_API_KEY; compared with hmac.compare_digest.
    - mode=jwt : requires DASHBOARD_JWT_SECRET and PyJWT installed.
    """

    def __init__(self) -> None:
        env = (getattr(config, "ENVIRONMENT", "production") or "production").lower()
        self.environment = env
        self.is_production = env in {"production", "prod"}

        raw_mode = (getattr(config, "DASHBOARD_AUTH_MODE", "api_key") or "api_key").lower()
        try:
            self.mode = AuthMode(raw_mode)
        except ValueError:
            raise AuthConfigError(f"Unknown DASHBOARD_AUTH_MODE: {raw_mode!r}")

        self.api_key = getattr(config, "DASHBOARD_API_KEY", "") or ""
        self.jwt_secret = getattr(config, "DASHBOARD_JWT_SECRET", "") or ""
        self.jwt_algorithm = getattr(config, "DASHBOARD_JWT_ALGORITHM", "HS256") or "HS256"
        self.admin_claim_key = "is_admin"

        self._validate()

    def _validate(self) -> None:
        if self.mode == AuthMode.NONE and self.is_production:
            raise AuthConfigError(
                "DASHBOARD_AUTH_MODE=none is not permitted in production."
            )
        if self.mode == AuthMode.API_KEY:
            if not self.api_key:
                if self.is_production:
                    raise AuthConfigError(
                        "DASHBOARD_API_KEY is required when DASHBOARD_AUTH_MODE=api_key in production."
                    )
                # dev: warn only
                try:
                    logger.warning("DASHBOARD_API_KEY empty; dashboard auth disabled (dev mode).")
                except Exception:
                    pass
                self.mode = AuthMode.NONE
        if self.mode == AuthMode.JWT:
            if not PYJWT_AVAILABLE:
                raise AuthConfigError("PyJWT is required for auth_mode=jwt but is not installed.")
            if not self.jwt_secret:
                raise AuthConfigError("DASHBOARD_JWT_SECRET is required for auth_mode=jwt.")

    def _decode_jwt(self, token: str) -> Dict[str, Any]:
        assert _pyjwt is not None
        try:
            payload = _pyjwt.decode(
                token, self.jwt_secret, algorithms=[self.jwt_algorithm],
                options={"require": ["exp", "sub"]},
            )
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"Invalid token: {e}")
        return payload

    def verify_token(self, token: Optional[str]) -> Principal:
        if self.mode == AuthMode.NONE:
            return Principal(subject="anonymous", mode=self.mode, is_admin=False)

        if not token:
            raise HTTPException(status_code=401, detail="Missing bearer token")

        if self.mode == AuthMode.API_KEY:
            if not hmac.compare_digest(token, self.api_key):
                raise HTTPException(status_code=403, detail="Invalid API key")
            return Principal(subject="api-key", mode=self.mode, is_admin=True)

        # JWT
        payload = self._decode_jwt(token)
        subject = str(payload.get("sub", "unknown"))
        is_admin = bool(payload.get(self.admin_claim_key, False))
        return Principal(subject=subject, mode=self.mode, is_admin=is_admin, claims=payload)


# ===========================================================================
# 2. RATE LIMITING
# ===========================================================================
class RateLimitBackend:
    def is_allowed(self, key: str, max_requests: int, window_seconds: int) -> bool:
        raise NotImplementedError


class InMemoryRateLimitBackend(RateLimitBackend):
    """Sliding window log with periodic TTL eviction. Thread-safe."""

    def __init__(self, eviction_interval: float = 30.0):
        self._lock = threading.Lock()
        self._buckets: Dict[str, List[float]] = defaultdict(list)
        self._eviction_interval = eviction_interval
        self._last_eviction = time.time()

    def _evict(self, now: float) -> None:
        # Periodically remove stale entries to prevent unbounded growth
        if now - self._last_eviction < self._eviction_interval:
            return
        stale_keys = []
        for k, v in self._buckets.items():
            cutoff = now - 3600
            v[:] = [t for t in v if t > cutoff]
            if not v:
                stale_keys.append(k)
        for k in stale_keys:
            self._buckets.pop(k, None)
        self._last_eviction = now

    def is_allowed(self, key: str, max_requests: int, window_seconds: int) -> bool:
        now = time.time()
        cutoff = now - window_seconds
        with self._lock:
            self._evict(now)
            bucket = self._buckets[key]
            bucket[:] = [t for t in bucket if t > cutoff]
            if len(bucket) >= max_requests:
                return False
            bucket.append(now)
            return True


class RedisRateLimitBackend(RateLimitBackend):
    """Token-bucket style limiter using Redis INCR + EXPIRE. Optional."""

    def __init__(self, redis_client: Any):
        self._r = redis_client

    def is_allowed(self, key: str, max_requests: int, window_seconds: int) -> bool:
        try:
            pipe = self._r.pipeline()
            pipe.incr(key)
            pipe.expire(key, window_seconds, nx=True)
            count, _ = pipe.execute()
            return int(count) <= max_requests
        except Exception:
            # Fail open on backend errors to avoid outages; log for ops.
            try:
                logger.warning("Redis rate limiter error; failing open.")
            except Exception:
                pass
            return True


class RateLimiter:
    def __init__(self, backend: Optional[RateLimitBackend] = None):
        self.backend: RateLimitBackend = backend or InMemoryRateLimitBackend()

    def check(self, scope: str, identity: str, max_requests: int, window_seconds: int) -> None:
        key = f"rl:{scope}:{identity}"
        if not self.backend.is_allowed(key, max_requests, window_seconds):
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded",
                headers={"Retry-After": str(window_seconds)},
            )


def client_ip(request: Request, trusted_proxies: Set[str]) -> str:
    peer = request.client.host if request.client else "unknown"
    if peer in trusted_proxies:
        fwd = request.headers.get("x-forwarded-for", "")
        if fwd:
            return fwd.split(",")[0].strip()
        real = request.headers.get("x-real-ip")
        if real:
            return real.strip()
    return peer


# ===========================================================================
# 3. ERROR MODEL
# ===========================================================================
class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    request_id: str
    status: int


def _error_json(status: int, error: str, detail: Optional[str] = None) -> JSONResponse:
    body = ErrorResponse(
        error=error, detail=detail, request_id=current_request_id(), status=status
    ).model_dump()
    return JSONResponse(status_code=status, content=body)


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    return _error_json(exc.status_code, error="http_error", detail=str(exc.detail))


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    return _error_json(422, error="validation_error", detail=json.dumps(exc.errors(), default=str))


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    try:
        logger.exception("Unhandled exception", request_id=current_request_id(), path=str(request.url))
    except Exception:
        pass
    return _error_json(500, error="internal_error", detail="Internal server error")


# ===========================================================================
# 4. REQUEST ID MIDDLEWARE + TRACING
# ===========================================================================
def traced(span_name: str) -> Callable:
    """Decorator to wrap an async route with an OTel span (no-op if OTel missing)."""

    def deco(fn: Callable) -> Callable:
        if not OTEL_AVAILABLE:
            return fn

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            with _TRACER.start_as_current_span(span_name) as span:
                span.set_attribute("request.id", current_request_id())
                try:
                    return await fn(*args, **kwargs)
                except HTTPException as e:
                    span.set_status(Status(StatusCode.ERROR, str(e.detail)))
                    raise
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise

        return wrapper

    return deco


async def request_context_middleware(request: Request, call_next):
    rid = request.headers.get("x-request-id") or new_request_id()
    token = _request_id_ctx.set(rid)
    start = time.time()
    try:
        response = await call_next(request)
    finally:
        _request_id_ctx.reset(token)
    duration_ms = (time.time() - start) * 1000
    response.headers["X-Request-ID"] = rid
    try:
        logger.info(
            "API request",
            request_id=rid,
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round(duration_ms, 2),
        )
    except Exception:
        pass
    return response


# ===========================================================================
# 5. WEBSOCKET MANAGER (topics, tickets, heartbeat)
# ===========================================================================
@dataclass
class _WsConnection:
    websocket: WebSocket
    topics: Set[str]
    principal_subject: str
    connected_at: float


class WebSocketTicketStore:
    """Short-lived, one-time tickets for WebSocket auth."""

    def __init__(self, ttl_seconds: int = 30, max_pending: int = 10_000):
        self._ttl = ttl_seconds
        self._max = max_pending
        self._lock = threading.Lock()
        self._tickets: Dict[str, Tuple[str, float]] = {}  # ticket -> (subject, expiry)

    def issue(self, subject: str) -> str:
        ticket = uuid.uuid4().hex
        now = time.time()
        with self._lock:
            self._evict(now)
            if len(self._tickets) >= self._max:
                # drop oldest
                oldest = min(self._tickets.items(), key=lambda kv: kv[1][1])
                self._tickets.pop(oldest[0], None)
            self._tickets[ticket] = (subject, now + self._ttl)
        return ticket

    def consume(self, ticket: str) -> Optional[str]:
        now = time.time()
        with self._lock:
            self._evict(now)
            entry = self._tickets.pop(ticket, None)
            if not entry:
                return None
            subject, expires = entry
            if expires < now:
                return None
            return subject

    def _evict(self, now: float) -> None:
        stale = [t for t, (_, exp) in self._tickets.items() if exp < now]
        for t in stale:
            self._tickets.pop(t, None)


class WebSocketManager:
    def __init__(self, heartbeat_seconds: float = 25.0):
        self._lock = threading.Lock()
        self._connections: Dict[WebSocket, _WsConnection] = {}
        self._topics: Dict[str, Set[WebSocket]] = defaultdict(set)
        self._heartbeat_seconds = heartbeat_seconds
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def connect(self, websocket: WebSocket, principal_subject: str, topics: Set[str]) -> None:
        await websocket.accept()
        conn = _WsConnection(
            websocket=websocket,
            topics=set(topics),
            principal_subject=principal_subject,
            connected_at=time.time(),
        )
        with self._lock:
            self._connections[websocket] = conn
            for t in conn.topics:
                self._topics[t].add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        with self._lock:
            conn = self._connections.pop(websocket, None)
            if conn:
                for t in conn.topics:
                    self._topics[t].discard(websocket)
                    if not self._topics[t]:
                        self._topics.pop(t, None)

    def update_topics(self, websocket: WebSocket, topics: Set[str]) -> None:
        with self._lock:
            conn = self._connections.get(websocket)
            if not conn:
                return
            for t in conn.topics - topics:
                self._topics[t].discard(websocket)
            for t in topics - conn.topics:
                self._topics[t].add(websocket)
            conn.topics = set(topics)

    async def broadcast(self, topic: str, payload: Dict[str, Any]) -> int:
        message = json.dumps({"type": topic, "ts": time.time(), "data": payload}, default=str)
        with self._lock:
            targets = set(self._topics.get(topic, set()))
            targets |= set(self._topics.get("*", set()))
        sent = 0
        for ws in list(targets):
            try:
                await ws.send_text(message)
                sent += 1
            except Exception:
                self.disconnect(ws)
        return sent

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_seconds)
            with self._lock:
                conns = list(self._connections.keys())
            for ws in conns:
                try:
                    await ws.send_text(json.dumps({"type": "ping", "ts": time.time()}))
                except Exception:
                    self.disconnect(ws)

    def start_heartbeat(self) -> None:
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop_heartbeat(self) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None


# ===========================================================================
# 6. ASYNC JOB MANAGER (for PSO and other expensive ops)
# ===========================================================================
class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Job:
    job_id: str
    name: str
    status: JobStatus = JobStatus.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    owner: Optional[str] = None


class JobManager:
    def __init__(self, max_jobs: int = 200, ttl_seconds: int = 3600):
        self._lock = threading.Lock()
        self._jobs: Dict[str, Job] = {}
        self._max_jobs = max_jobs
        self._ttl = ttl_seconds

    def _evict(self) -> None:
        if len(self._jobs) <= self._max_jobs:
            return
        now = time.time()
        stale = [jid for jid, j in self._jobs.items()
                 if j.finished_at and now - j.finished_at > self._ttl]
        for jid in stale:
            self._jobs.pop(jid, None)
        if len(self._jobs) > self._max_jobs:
            oldest = sorted(self._jobs.items(), key=lambda kv: kv[1].created_at)
            for jid, _ in oldest[: len(self._jobs) - self._max_jobs]:
                self._jobs.pop(jid, None)

    def create(self, name: str, owner: Optional[str]) -> Job:
        job = Job(job_id=uuid.uuid4().hex, name=name, owner=owner)
        with self._lock:
            self._jobs[job.job_id] = job
            self._evict()
        return job

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def update(self, job_id: str, **fields: Any) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            for k, v in fields.items():
                setattr(job, k, v)

    async def run(self, job_id: str, coro_factory: Callable[[], Any]) -> None:
        self.update(job_id, status=JobStatus.RUNNING, started_at=time.time())
        try:
            result = await coro_factory()
            self.update(
                job_id, status=JobStatus.SUCCEEDED, result=result, finished_at=time.time()
            )
        except asyncio.CancelledError:
            self.update(job_id, status=JobStatus.CANCELLED, finished_at=time.time())
            raise
        except Exception as e:
            try:
                logger.exception("Job failed", job_id=job_id)
            except Exception:
                pass
            self.update(job_id, status=JobStatus.FAILED, error=str(e), finished_at=time.time())


# ===========================================================================
# 7. PYDANTIC REQUEST / RESPONSE MODELS
# ===========================================================================
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
    next_cursor: Optional[str] = None


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


class LimitGraphNodeResponse(BaseModel):
    node_id: str
    graph_id: str
    node_type: Optional[str] = None
    attributes: Dict[str, Any] = Field(default_factory=dict)
    timestamp: Optional[str] = None


class LimitGraphEdgeResponse(BaseModel):
    edge_id: str
    graph_id: str
    source_node: str
    target_node: str
    weight: Optional[float] = None
    attributes: Dict[str, Any] = Field(default_factory=dict)
    timestamp: Optional[str] = None


class LimitGraphNodeCreate(BaseModel):
    node_id: str = Field(..., min_length=1, max_length=256)
    node_type: Optional[str] = Field(None, max_length=64)
    attributes: Dict[str, Any] = Field(default_factory=dict)


class LimitGraphEdgeCreate(BaseModel):
    edge_id: str = Field(..., min_length=1, max_length=256)
    source: str = Field(..., min_length=1, max_length=256)
    target: str = Field(..., min_length=1, max_length=256)
    weight: Optional[float] = None
    attributes: Dict[str, Any] = Field(default_factory=dict)


class RLHFPairResponse(BaseModel):
    pair_id: str
    prompt: str
    chosen_response: str
    rejected_response: str
    reward_difference: float
    metadata: Optional[Dict[str, Any]] = None
    timestamp: Optional[str] = None


class RLHFPairCreate(BaseModel):
    pair_id: Optional[str] = Field(None, max_length=128)
    prompt: str = Field(..., min_length=1, max_length=4096)
    chosen: str = Field(..., min_length=1, max_length=256)
    rejected: str = Field(..., min_length=1, max_length=256)
    reward_diff: float
    metadata: Optional[Dict[str, Any]] = None

    @field_validator("chosen")
    @classmethod
    def _chosen_nonempty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("chosen must not be blank")
        return v

    @field_validator("rejected")
    @classmethod
    def _rejected_differs(cls, v: str, info) -> str:
        chosen = info.data.get("chosen")
        if chosen is not None and v == chosen:
            raise ValueError("chosen and rejected must differ")
        return v


class WeightsUpdate(BaseModel):
    weights: Dict[str, float] = Field(..., min_length=1)


class BioRunResponse(BaseModel):
    run_id: str
    algorithm: str
    problem_id: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    best_solution: Dict[str, Any] = Field(default_factory=dict)
    best_fitness: float
    timestamp: Optional[str] = None


class JobResponse(BaseModel):
    job_id: str
    name: str
    status: str
    created_at: float
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    result: Optional[Any] = None
    error: Optional[str] = None


class ReadyResponse(BaseModel):
    ready: bool
    checks: Dict[str, bool]
    request_id: str


class VersionResponse(BaseModel):
    service: str
    version: str
    otel: bool
    prometheus: bool
    auth_mode: str
    environment: str


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str


# ===========================================================================
# 8. STORAGE ADAPTER + CAPABILITY CHECKS
# ===========================================================================
class AuditStorageAdapter:
    """
    Thin adapter around Storage with capability checks at startup.
    Returns 501 with a uniform body when a method is not implemented.
    """

    REQUIRED = (
        "get_feedback_events",
        "get_feedback_event_by_task_id",
        "get_benchmark_results",
        "get_drift_events",
    )
    OPTIONAL = (
        "get_expert_usage",
        "get_expert_success_rate",
        "get_expert_latency_stats",
        "get_bio_runs",
        "log_audit",
    )

    def __init__(self, storage: Storage):
        self.storage = storage
        self.caps: Dict[str, bool] = {}
        for name in self.REQUIRED + self.OPTIONAL:
            self.caps[name] = hasattr(storage, name) and callable(getattr(storage, name, None))

    def require(self, name: str) -> None:
        if not self.caps.get(name, False):
            raise HTTPException(status_code=501, detail=f"Storage method not implemented: {name}")

    def __getattr__(self, name: str) -> Any:
        # Delegate to storage; require() already raised if missing.
        return getattr(self.storage, name)


# ===========================================================================
# 9. COMPONENTS BUNDLE + FACTORY
# ===========================================================================
@dataclass
class DashboardComponents:
    storage: AuditStorageAdapter
    adaptive_cost: Optional[AdaptiveCostFunction] = None
    pareto: Optional[ParetoGating] = None
    drift: Optional[DriftDetector] = None
    queue: Optional[AsyncMessageQueue] = None
    metrics: Optional[MetricsRegistry] = None
    limit_graph_manager: Optional[Any] = None
    modp_solver: Optional[Any] = None
    rlhf_trainer: Optional[Any] = None
    pso_optimizer: Optional[Any] = None
    moe_gating: Optional[Any] = None
    ga_optimizer: Optional[Any] = None


def build_dashboard_components(
    storage: Storage,
    adaptive_cost: Optional[AdaptiveCostFunction] = None,
    pareto_gating: Optional[ParetoGating] = None,
    drift_detector: Optional[DriftDetector] = None,
    message_queue: Optional[AsyncMessageQueue] = None,
    metrics: Optional[MetricsRegistry] = None,
    bio_core: Optional[Any] = None,
) -> DashboardComponents:
    adapter = AuditStorageAdapter(storage)

    cfg_dict = {
        "distillation_epsilon": getattr(config, "DISTILLATION_EPSILON", 0.1),
        "distillation_epsilon_min": getattr(config, "DISTILLATION_EPSILON_MIN", 0.01),
        "distillation_epsilon_decay": getattr(config, "DISTILLATION_EPSILON_DECAY", 0.995),
        "distillation_train_every": getattr(config, "DISTILLATION_TRAIN_EVERY", 10),
        "distillation_replay_size": getattr(config, "DISTILLATION_REPLAY_SIZE", 2000),
        "distillation_learning_rate": getattr(config, "DISTILLATION_LEARNING_RATE", 0.01),
        "distill_weight": getattr(config, "DISTILL_WEIGHT", 0.7),
        "rl_weight": getattr(config, "RL_WEIGHT", 0.3),
        "moe_expert_count": getattr(config, "MOE_EXPERT_COUNT", 4),
        "pso_particles": getattr(config, "PSO_PARTICLES", 10),
        "pso_iterations": getattr(config, "PSO_ITERATIONS", 20),
    }

    comps = DashboardComponents(storage=adapter,
                                adaptive_cost=adaptive_cost,
                                pareto=pareto_gating,
                                drift=drift_detector,
                                queue=message_queue,
                                metrics=metrics)

    if not ENHANCEMENTS_CORE_AVAILABLE:
        return comps

    try:
        if hasattr(storage, "save_limit_graph_metadata"):
            comps.limit_graph_manager = LimitGraphManager(storage)
        if hasattr(storage, "save_modp_state"):
            comps.modp_solver = MODPOptimizer(storage)
        if hasattr(storage, "save_preference_pair"):
            comps.rlhf_trainer = RLHFTrainer(storage)
        if hasattr(storage, "save_bio_run"):
            comps.pso_optimizer = ParticleSwarmOptimizer(storage, cfg_dict)
        if hasattr(storage, "log_routing_decision"):
            comps.moe_gating = MoEGatingNetwork(storage, cfg_dict)
        if hasattr(storage, "save_ga_population"):
            comps.ga_optimizer = GeneticHyperparameterOptimizer(storage, cfg_dict)
    except Exception:
        try:
            logger.exception("Failed to instantiate one or more enhancement components.")
        except Exception:
            pass

    return comps


# ===========================================================================
# 10. DECISION AUDIT
# ===========================================================================
def _parse_trusted_proxies() -> Set[str]:
    raw = getattr(config, "DASHBOARD_TRUSTED_PROXIES", "") or ""
    return {p.strip() for p in raw.split(",") if p.strip()}


def _parse_cors_origins() -> Tuple[List[str], bool]:
    """Return (origins, allow_credentials). '*' is never combined with credentials."""
    raw = getattr(config, "DASHBOARD_CORS_ORIGINS", "") or ""
    origins = [o.strip() for o in raw.split(",") if o.strip()]
    if not origins:
        # Safe default: no cross-origin in production, wildcard in dev without credentials.
        if (getattr(config, "ENVIRONMENT", "production") or "production").lower() in {"production", "prod"}:
            return [], False
        return ["*"], False
    if "*" in origins:
        return ["*"], False
    return origins, True


def _decode_cursor(cursor: Optional[str]) -> Optional[float]:
    if not cursor:
        return None
    try:
        return float(cursor)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cursor")


def _encode_cursor(value: float) -> str:
    return f"{value:.6f}"


class DecisionAudit:
    def __init__(
        self,
        storage: Storage,
        adaptive_cost: Optional[AdaptiveCostFunction] = None,
        pareto_gating: Optional[ParetoGating] = None,
        drift_detector: Optional[DriftDetector] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        metrics: Optional[MetricsRegistry] = None,
        bio_core: Optional[Any] = None,
        auth_manager: Optional[AuthManager] = None,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        self.components = build_dashboard_components(
            storage, adaptive_cost, pareto_gating, drift_detector,
            message_queue, metrics, bio_core,
        )
        self.storage = self.components.storage
        self.adaptive_cost = self.components.adaptive_cost
        self.pareto = self.components.pareto
        self.drift = self.components.drift
        self.queue = self.components.queue
        self.metrics = self.components.metrics
        self.bio_core = bio_core

        self.auth = auth_manager or AuthManager()
        self.rate_limiter = rate_limiter or RateLimiter()
        self.trusted_proxies = _parse_trusted_proxies()

        self.ws_manager = WebSocketManager()
        self.tickets = WebSocketTicketStore()
        self.jobs = JobManager()

        # Thread-safe lock for weight updates (used across threads).
        self._weights_lock = threading.Lock()

        self._app: Optional[FastAPI] = None
        self._server: Optional[uvicorn.Server] = None
        self._server_thread: Optional[threading.Thread] = None
        self._running = False
        self._lifecycle_lock = threading.Lock()

        self.router = APIRouter()
        self._setup_routes()

        self.cors_origins, self.cors_allow_credentials = _parse_cors_origins()

    # ------------------------------------------------------------------ auth
    security = HTTPBearer(auto_error=False)

    def _principal(self, creds: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> Principal:
        token = creds.credentials if creds else None
        return self.auth.verify_token(token)

    def _require_admin(self, principal: Principal = Depends(_principal)) -> Principal:
        if not principal.is_admin and principal.mode != AuthMode.NONE:
            raise HTTPException(status_code=403, detail="Admin privileges required")
        return principal

    def _rate_limit(self, scope: str, max_requests: int, window: int):
        async def dep(request: Request, principal: Principal = Depends(self._principal)) -> None:
            identity = principal.subject if principal.subject != "anonymous" else client_ip(request, self.trusted_proxies)
            self.rate_limiter.check(scope, identity, max_requests, window)
        return dep

    # ------------------------------------------------------------- routes
    def _setup_routes(self) -> None:
        router = self.router

        # ------- health / version / ready -------
        @router.get("/health", response_model=HealthResponse)
        async def health():
            return {"status": "healthy", "service": SERVICE_NAME, "version": VERSION}

        @router.get("/version", response_model=VersionResponse)
        async def version_endpoint():
            return {
                "service": SERVICE_NAME,
                "version": VERSION,
                "otel": OTEL_AVAILABLE,
                "prometheus": PROMETHEUS_AVAILABLE,
                "auth_mode": self.auth.mode.value,
                "environment": self.auth.environment,
            }

        @router.get("/ready", response_model=ReadyResponse)
        async def ready():
            checks: Dict[str, bool] = {}
            checks["storage_required"] = all(
                self.storage.caps.get(m, False) for m in AuditStorageAdapter.REQUIRED
            )
            checks["adaptive_cost"] = self.adaptive_cost is not None
            checks["pareto"] = self.pareto is not None
            checks["drift"] = self.drift is not None
            checks["auth_configured"] = (
                self.auth.mode == AuthMode.NONE or bool(self.auth.api_key or self.auth.jwt_secret)
            )
            ready_ok = checks["storage_required"] and checks["auth_configured"]
            if not ready_ok:
                raise HTTPException(status_code=503, detail=json.dumps(checks))
            return {"ready": True, "checks": checks, "request_id": current_request_id()}

        @router.get("/metrics")
        async def metrics_endpoint(_: Principal = Depends(self._principal)):
            if not PROMETHEUS_AVAILABLE:
                raise HTTPException(status_code=501, detail="Prometheus client not installed")
            return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

        # ------- WS ticket -------
        @router.post("/ws-ticket")
        async def issue_ws_ticket(principal: Principal = Depends(self._principal)):
            ticket = self.tickets.issue(principal.subject)
            return {"ticket": ticket, "expires_in": 30, "request_id": current_request_id()}

        # ------- decisions -------
        @router.get("/decisions", response_model=DecisionListResponse)
        @traced("decisions.list")
        async def get_decisions(
            limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
            cursor: Optional[str] = Query(None, description="Cursor from previous page"),
            feedback_type: Optional[str] = Query(None),
            teacher_id: Optional[str] = Query(None),
            task_id: Optional[str] = Query(None),
            start_timestamp: Optional[float] = Query(None),
            end_timestamp: Optional[float] = Query(None),
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("decisions_list", 200, 60)),
        ):
            self.storage.require("get_feedback_events")
            cursor_ts = _decode_cursor(cursor)
            kwargs: Dict[str, Any] = dict(
                limit=limit, feedback_type=feedback_type, teacher_id=teacher_id,
                task_id=task_id, start_timestamp=start_timestamp, end_timestamp=end_timestamp,
            )
            try:
                events = self.storage.get_feedback_events(**kwargs, cursor_ts=cursor_ts)
            except TypeError:
                # Fallback: storage backend doesn't support cursor_ts
                events = self.storage.get_feedback_events(**kwargs)
                if cursor_ts is not None:
                    events = [e for e in events if e.get("timestamp", 0) < cursor_ts]
                events = events[:limit]
            next_cursor = None
            if events and len(events) == limit:
                next_cursor = _encode_cursor(float(events[-1].get("timestamp", 0)))
            return {
                "status": "success",
                "count": len(events),
                "events": events,
                "next_cursor": next_cursor,
            }

        @router.get("/decisions/{task_id}")
        @traced("decisions.get")
        async def get_decision(
            task_id: str,
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("decisions_get", 300, 60)),
        ):
            self.storage.require("get_feedback_event_by_task_id")
            event = self.storage.get_feedback_event_by_task_id(task_id)
            if not event:
                raise HTTPException(status_code=404, detail="Decision not found")
            return {"status": "success", "event": event}

        # ------- benchmarks -------
        @router.get("/benchmark/latest", response_model=List[BenchmarkResultResponse])
        @traced("benchmark.latest")
        async def get_latest_benchmarks(
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("bench_latest", 120, 60)),
        ):
            self.storage.require("get_benchmark_results")
            runs = self.storage.get_benchmark_results(days_back=7)
            if not runs:
                return []
            latest: Dict[str, Dict[str, Any]] = {}
            for run in runs:
                policy = run.get("policy_name")
                if policy is None:
                    continue
                if policy not in latest or run.get("timestamp", 0) > latest[policy].get("timestamp", 0):
                    latest[policy] = run
            return [
                {
                    "run_id": r.get("run_id", ""),
                    "policy_name": p,
                    "timestamp": r.get("timestamp", 0),
                    "sample_count": r.get("sample_count", 0),
                    "metrics": {
                        "quality": r.get("avg_quality", 0.0),
                        "carbon": r.get("avg_carbon", 0.0),
                        "latency": r.get("avg_latency", 0.0),
                        "energy": r.get("total_energy", 0.0),
                        "cost": r.get("avg_cost", 0.0),
                    },
                    "confidence_intervals": r.get("confidence_intervals", {}),
                    "p_value": r.get("p_value"),
                }
                for p, r in latest.items()
            ]

        @router.get("/benchmark/history")
        @traced("benchmark.history")
        async def get_benchmark_history(
            policy: Optional[str] = Query(None),
            limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("bench_history", 120, 60)),
        ):
            self.storage.require("get_benchmark_results")
            runs = self.storage.get_benchmark_results(days_back=30)
            if policy:
                runs = [r for r in runs if r.get("policy_name") == policy]
            runs = sorted(runs, key=lambda r: r.get("timestamp", 0), reverse=True)[:limit]
            return {"status": "success", "count": len(runs), "runs": runs}

        # ------- drift -------
        @router.get("/drift/events", response_model=List[DriftEventResponse])
        @traced("drift.events")
        async def get_drift_events(
            limit: int = Query(50, ge=1, le=MAX_PAGE_LIMIT),
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("drift_events", 120, 60)),
        ):
            self.storage.require("get_drift_events")
            return self.storage.get_drift_events(limit=limit)

        # ------- MoE metrics -------
        @router.get("/experts/metrics", response_model=List[ExpertMetricsResponse])
        async def get_expert_metrics(
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("expert_metrics", 120, 60)),
        ):
            usage = self.storage.get_expert_usage() if self.storage.caps.get("get_expert_usage") else {}
            success = self.storage.get_expert_success_rate() if self.storage.caps.get("get_expert_success_rate") else {}
            latency = self.storage.get_expert_latency_stats() if self.storage.caps.get("get_expert_latency_stats") else {}
            experts = set(usage) | set(success) | set(latency)
            return [
                {
                    "expert_id": eid,
                    "usage": int(usage.get(eid, 0)),
                    "success_rate": float(success.get(eid, 0.0)),
                    "avg_latency_ms": float(latency.get(eid, {}).get("avg_ms", 0.0)),
                    "p95_latency_ms": float(latency.get(eid, {}).get("p95_ms", 0.0)),
                }
                for eid in experts
            ]

        # ------- MODP -------
        @router.get("/mopd/pareto-front", response_model=List[ParetoFrontResponse])
        async def get_pareto_front(
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("pareto", 120, 60)),
        ):
            if self.pareto is None:
                raise HTTPException(status_code=404, detail="ParetoGating not configured")
            front = self.pareto.get_front() if hasattr(self.pareto, "get_front") else []
            return [
                {
                    "expert_id": i.get("expert_id", "unknown"),
                    "quality_score": float(i.get("quality_score", 0.0)),
                    "carbon_g": float(i.get("carbon_g", 0.0)),
                    "latency_ms": float(i.get("latency_ms", 0.0)),
                    "energy_joules": float(i.get("energy_joules", 0.0)),
                }
                for i in front
            ]

        @router.get("/mopd/weights")
        async def get_adaptive_weights(
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("weights_get", 120, 60)),
        ):
            if self.adaptive_cost is None:
                raise HTTPException(status_code=404, detail="AdaptiveCostFunction not configured")
            return {"weights": self.adaptive_cost.get_current_weights()}

        @router.post("/mopd/weights")
        async def set_adaptive_weights(
            payload: WeightsUpdate,
            principal: Principal = Depends(self._require_admin),
        ):
            if self.adaptive_cost is None:
                raise HTTPException(status_code=404, detail="AdaptiveCostFunction not configured")

            allowed = getattr(AdaptiveCostFunction, "ALLOWED_WEIGHT_KEYS", None)
            if allowed:
                unknown = set(payload.weights) - set(allowed)
                if unknown:
                    raise HTTPException(status_code=400, detail=f"Unknown weight keys: {sorted(unknown)}")
            bad = {k: v for k, v in payload.weights.items() if not isinstance(v, (int, float)) or v < 0}
            if bad:
                raise HTTPException(status_code=400, detail=f"Invalid weight values: {bad}")

            with self._weights_lock:
                try:
                    self.adaptive_cost.update_weights(payload.weights)
                except Exception as e:
                    raise HTTPException(status_code=400, detail=str(e))
                current = self.adaptive_cost.get_current_weights()

            # Audit log
            if self.storage.caps.get("log_audit"):
                try:
                    self.storage.log_audit(
                        actor=principal.subject,
                        action="update_adaptive_weights",
                        target="adaptive_cost",
                        details={"weights": payload.weights},
                        request_id=current_request_id(),
                    )
                except Exception:
                    pass

            await self.ws_manager.broadcast("weights", current)
            return {"status": "success", "weights": current, "request_id": current_request_id()}

        # ------- LIMIT graph -------
        @router.get("/limit-graph/{graph_id}/nodes", response_model=List[LimitGraphNodeResponse])
        async def get_limit_graph_nodes(
            graph_id: str,
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("lg_nodes_get", 120, 60)),
        ):
            if self.components.limit_graph_manager is None:
                raise HTTPException(status_code=404, detail="LimitGraphManager not available")
            return self.components.limit_graph_manager.get_nodes(graph_id)

        @router.get("/limit-graph/{graph_id}/edges", response_model=List[LimitGraphEdgeResponse])
        async def get_limit_graph_edges(
            graph_id: str,
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("lg_edges_get", 120, 60)),
        ):
            if self.components.limit_graph_manager is None:
                raise HTTPException(status_code=404, detail="LimitGraphManager not available")
            return self.components.limit_graph_manager.get_edges(graph_id)

        @router.post("/limit-graph/{graph_id}/nodes")
        async def add_limit_graph_node(
            graph_id: str,
            payload: LimitGraphNodeCreate,
            principal: Principal = Depends(self._require_admin),
        ):
            if self.components.limit_graph_manager is None:
                raise HTTPException(status_code=404, detail="LimitGraphManager not available")
            self.components.limit_graph_manager.add_node(
                graph_id, payload.node_id, payload.node_type, payload.attributes
            )
            await self.ws_manager.broadcast("limit_graph", {
                "graph_id": graph_id, "op": "add_node", "node_id": payload.node_id,
            })
            return {"status": "success", "node_id": payload.node_id, "request_id": current_request_id()}

        @router.post("/limit-graph/{graph_id}/edges")
        async def add_limit_graph_edge(
            graph_id: str,
            payload: LimitGraphEdgeCreate,
            principal: Principal = Depends(self._require_admin),
        ):
            if self.components.limit_graph_manager is None:
                raise HTTPException(status_code=404, detail="LimitGraphManager not available")
            self.components.limit_graph_manager.add_edge(
                payload.edge_id, graph_id, payload.source, payload.target,
                payload.weight, payload.attributes,
            )
            await self.ws_manager.broadcast("limit_graph", {
                "graph_id": graph_id, "op": "add_edge", "edge_id": payload.edge_id,
            })
            return {"status": "success", "edge_id": payload.edge_id, "request_id": current_request_id()}

        # ------- RLHF -------
        @router.get("/rlhf/pairs", response_model=List[RLHFPairResponse])
        async def get_rlhf_pairs(
            limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("rlhf_get", 120, 60)),
        ):
            if self.components.rlhf_trainer is None:
                raise HTTPException(status_code=404, detail="RLHFTrainer not available")
            return self.components.rlhf_trainer.get_pairs(limit)

        @router.post("/rlhf/pairs")
        async def record_rlhf_pair(
            payload: RLHFPairCreate,
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("rlhf_post", 60, 60)),
        ):
            if self.components.rlhf_trainer is None:
                raise HTTPException(status_code=404, detail="RLHFTrainer not available")

            # Whitelist of actions (if available)
            allowed = None
            try:
                from ..core import DistillationResponseOptimizer  # type: ignore
                allowed = set(getattr(DistillationResponseOptimizer, "ACTION_SPACE", []))
            except Exception:
                allowed = None
            if allowed:
                if payload.chosen not in allowed or payload.rejected not in allowed:
                    raise HTTPException(
                        status_code=422,
                        detail=f"chosen/rejected must be one of {sorted(allowed)}",
                    )

            pair_id = payload.pair_id or uuid.uuid4().hex

            # Idempotency: if storage supports get_preference_pair, check for existing
            existing = None
            if hasattr(self.storage.storage, "get_preference_pair"):
                try:
                    existing = self.storage.storage.get_preference_pair(pair_id)
                except Exception:
                    existing = None
            if existing:
                raise HTTPException(status_code=409, detail=f"Pair {pair_id} already exists")

            self.components.rlhf_trainer.record_pair(
                pair_id=pair_id,
                prompt=payload.prompt,
                chosen=payload.chosen,
                rejected=payload.rejected,
                reward_diff=payload.reward_diff,
                metadata=payload.metadata,
            )
            await self.ws_manager.broadcast("rlhf", {
                "op": "record_pair", "pair_id": pair_id,
                "chosen": payload.chosen, "rejected": payload.rejected,
            })
            return {"status": "success", "pair_id": pair_id, "request_id": current_request_id()}

        # ------- Bio / PSO -------
        @router.get("/bio/runs", response_model=List[BioRunResponse])
        async def get_bio_runs(
            algorithm: Optional[str] = Query(None),
            limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("bio_runs", 120, 60)),
        ):
            if not self.storage.caps.get("get_bio_runs"):
                raise HTTPException(status_code=501, detail="Storage method not implemented: get_bio_runs")
            return self.storage.get_bio_runs(algorithm=algorithm, limit=limit)

        @router.post("/bio/pso/jobs", status_code=202)
        async def create_pso_job(
            background: BackgroundTasks,
            principal: Principal = Depends(self._require_admin),
        ):
            if self.components.pso_optimizer is None:
                raise HTTPException(status_code=404, detail="PSO optimizer not available")
            job = self.jobs.create("pso_optimize", owner=principal.subject)

            async def _runner():
                return await self.components.pso_optimizer.optimize()

            async def _schedule():
                await self.jobs.run(job.job_id, _runner)

            asyncio.create_task(_schedule())
            return {"status": "accepted", "job_id": job.job_id, "request_id": current_request_id()}

        @router.get("/bio/pso/jobs/{job_id}", response_model=JobResponse)
        async def get_pso_job(
            job_id: str,
            principal: Principal = Depends(self._principal),
        ):
            job = self.jobs.get(job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            if job.owner and job.owner != principal.subject and not principal.is_admin:
                raise HTTPException(status_code=403, detail="Not authorized for this job")
            return {
                "job_id": job.job_id, "name": job.name, "status": job.status.value,
                "created_at": job.created_at, "started_at": job.started_at,
                "finished_at": job.finished_at, "result": job.result, "error": job.error,
            }

        @router.post("/bio/pso/optimize")
        async def run_pso_optimization_legacy(
            principal: Principal = Depends(self._require_admin),
            _: None = Depends(self._rate_limit("pso_legacy", 5, 60)),
        ):
            # Kept for backwards compatibility; prefer the job endpoint.
            if self.components.pso_optimizer is None:
                raise HTTPException(status_code=404, detail="PSO optimizer not available")
            best = await self.components.pso_optimizer.optimize()
            await self.ws_manager.broadcast("pso", {"best": best})
            return {"status": "success", "best_hyperparameters": best}

        # ------- MoE experts -------
        @router.get("/moe/experts")
        async def list_moe_experts(
            principal: Principal = Depends(self._principal),
            _: None = Depends(self._rate_limit("moe_experts", 120, 60)),
        ):
            if self.components.moe_gating is None:
                raise HTTPException(status_code=404, detail="MoE gating not available")
            return {"expert_names": self.components.moe_gating.expert_names}

    # ----------------------------------------------------------- websocket
    def _attach_websocket(self, app: FastAPI) -> None:
        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            ticket = websocket.query_params.get("ticket")
            subject = self.tickets.consume(ticket) if ticket else None
            if not subject:
                # No header-based fallback for browsers, so reject.
                await websocket.close(code=1008)
                return

            # Optional topic subscription via query param: ?topics=decisions,rlhf
            topics_raw = websocket.query_params.get("topics", "*")
            topics = {t.strip() for t in topics_raw.split(",") if t.strip()} or {"*"}

            await self.ws_manager.connect(websocket, principal_subject=subject, topics=topics)
            try:
                while True:
                    data = await websocket.receive_text()
                    # Simple protocol: {"op":"subscribe","topics":[...]} / {"op":"ping"}
                    try:
                        msg = json.loads(data)
                    except Exception:
                        await websocket.send_text(json.dumps({"type": "error", "detail": "bad json"}))
                        continue
                    op = msg.get("op")
                    if op == "ping":
                        await websocket.send_text(json.dumps({"type": "pong", "ts": time.time()}))
                    elif op == "subscribe":
                        new_topics = set(msg.get("topics") or [])
                        self.ws_manager.update_topics(websocket, new_topics or {"*"})
                        await websocket.send_text(
                            json.dumps({"type": "subscribed", "topics": sorted(new_topics or ["*"])})
                        )
                    else:
                        await websocket.send_text(json.dumps({"type": "ack"}))
            except WebSocketDisconnect:
                self.ws_manager.disconnect(websocket)
            except Exception:
                self.ws_manager.disconnect(websocket)

    # --------------------------------------------------------------- app
    def build_app(self) -> FastAPI:
        app = FastAPI(
            title="Green Agent Audit Dashboard",
            version=VERSION,
            description=(
                "API for decision audit, benchmarks, drift events, MoE metrics, "
                "LIMIT Graph, RLHF, and bio-inspired optimization."
            ),
        )

        # CORS (never "*" + credentials)
        if self.cors_origins:
            app.add_middleware(
                CORSMiddleware,
                allow_origins=self.cors_origins,
                allow_credentials=self.cors_allow_credentials,
                allow_methods=["*"],
                allow_headers=["*"],
            )

        # Middleware: request id + logging
        app.middleware("http")(request_context_middleware)

        # Global exception handlers: uniform ErrorResponse
        app.add_exception_handler(HTTPException, http_exception_handler)
        app.add_exception_handler(RequestValidationError, validation_exception_handler)
        app.add_exception_handler(Exception, unhandled_exception_handler)

        # Routes
        app.include_router(self.router, prefix="/api/v1")
        self._attach_websocket(app)

        # Lifespan: start heartbeat, log startup warnings
        @app.on_event("startup")
        async def _startup():
            self.ws_manager.start_heartbeat()
            try:
                logger.info("Dashboard starting", version=VERSION, auth_mode=self.auth.mode.value)
            except Exception:
                pass

        @app.on_event("shutdown")
        async def _shutdown():
            await self.ws_manager.stop_heartbeat()

        self._app = app
        return app

    # ----------------------------------------------------------- lifecycle
    async def start_dashboard(self) -> None:
        if not getattr(config, "DASHBOARD_ENABLED", True):
            logger.info("Dashboard disabled by config.")
            return
        with self._lifecycle_lock:
            if self._running:
                logger.warning("Dashboard already running.")
                return
            app = self.build_app()

            cfg_kwargs = {
                "host": "0.0.0.0",
                "port": int(getattr(config, "DASHBOARD_PORT", 8000)),
                "log_level": "info",
                "loop": "asyncio",
            }
            ready = threading.Event()
            bind_error: Dict[str, Any] = {}

            def run_server():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    self._server = uvicorn.Server(uvicorn.Config(app, **cfg_kwargs))
                    # Signal readiness before blocking
                    loop.call_soon(ready.set)
                    loop.run_until_complete(self._server.serve())
                except Exception as e:
                    bind_error["error"] = str(e)
                    ready.set()
                finally:
                    loop.close()

            self._server_thread = threading.Thread(target=run_server, daemon=True, name="audit-dashboard")
            self._server_thread.start()
            ready.wait(timeout=5)
            if "error" in bind_error:
                raise RuntimeError(f"Failed to start dashboard: {bind_error['error']}")
            self._running = True
            logger.info(f"Audit dashboard started on port {cfg_kwargs['port']}")

    async def stop_dashboard(self) -> None:
        with self._lifecycle_lock:
            if not self._running:
                logger.info("Dashboard not running.")
                return
            if self._server:
                self._server.should_exit = True
            if self._server_thread and self._server_thread.is_alive():
                self._server_thread.join(timeout=5)
            self._running = False
            logger.info("Dashboard stopped.")


# ===========================================================================
# 11. CONVENIENCE FACTORY
# ===========================================================================
def create_dashboard(
    storage: Storage,
    adaptive_cost: Optional[AdaptiveCostFunction] = None,
    pareto_gating: Optional[ParetoGating] = None,
    drift_detector: Optional[DriftDetector] = None,
    message_queue: Optional[AsyncMessageQueue] = None,
    metrics: Optional[MetricsRegistry] = None,
    bio_core: Optional[Any] = None,
    auth_manager: Optional[AuthManager] = None,
    rate_limiter: Optional[RateLimiter] = None,
) -> DecisionAudit:
    return DecisionAudit(
        storage=storage,
        adaptive_cost=adaptive_cost,
        pareto_gating=pareto_gating,
        drift_detector=drift_detector,
        message_queue=message_queue,
        metrics=metrics,
        bio_core=bio_core,
        auth_manager=auth_manager,
        rate_limiter=rate_limiter,
    )


# ===========================================================================
# 12. ENTRY POINT
# ===========================================================================
async def _main() -> None:
    # Standalone mode: expect a real Storage instance to be provided by the host app.
    # In tests, pass a duck-typed object with the required methods.
    try:
        from ..storage import Storage  # type: ignore
        storage = Storage()
    except Exception:
        logger.error("Cannot start standalone without a Storage implementation.")
        return

    dashboard = create_dashboard(storage)
    await dashboard.start_dashboard()
    # In a real deployment, keep the process alive via the host app.


if __name__ == "__main__":
    asyncio.run(_main())
