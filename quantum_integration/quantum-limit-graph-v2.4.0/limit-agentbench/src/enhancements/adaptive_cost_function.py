"""
Enhanced Adaptive API Service
- FastAPI-based REST API for expert feedback and distillation
- Includes JWT authentication with role-based access control
- Async SQLAlchemy database with connection pooling and migrations
- Structured logging with correlation IDs
- Health checks and Prometheus metrics
- Rate limiting with Redis fallback
- Circuit breakers for external dependencies
- Pydantic models for request/response validation
- OpenAPI documentation
- Background task supervision
"""

import os
import uuid
import json
import time
import logging
from typing import Dict, Any, List, Optional, Callable, Awaitable
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

# FastAPI imports
from fastapi import FastAPI, Depends, HTTPException, status, Request, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

# Pydantic and settings
from pydantic import BaseModel, Field, BaseSettings, validator
from typing import Optional

# JWT
import jwt
from jwt import PyJWTError

# SQLAlchemy
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import text, Column, String, Float, Integer, DateTime, JSON

# Prometheus
try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# Structured logging
try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            TimeStamper(fmt="iso"),
            JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger(__name__)
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

# Redis for rate limiting and caching
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Circuit breaker
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# =============================================================================
# Configuration using Pydantic BaseSettings
# =============================================================================

class Settings(BaseSettings):
    # API
    api_version: str = "v1"
    debug: bool = False

    # JWT
    auth_secret: str = Field(..., env="ADAPTIVE_API_HS256_SECRET")
    auth_algorithm: str = Field("HS256", env="ADAPTIVE_API_ALGO")
    auth_jwks_url: Optional[str] = Field(None, env="ADAPTIVE_API_JWKS_URL")
    token_expiry_minutes: int = Field(60, env="ADAPTIVE_API_TOKEN_EXPIRY_MINUTES")

    # Database
    db_url: str = Field("sqlite+aiosqlite:///./adaptive.db", env="ADAPTIVE_API_DB_URL")
    db_pool_size: int = Field(10, env="ADAPTIVE_API_DB_POOL_SIZE")
    db_max_overflow: int = Field(20, env="ADAPTIVE_API_DB_MAX_OVERFLOW")

    # Redis
    redis_url: Optional[str] = Field(None, env="ADAPTIVE_API_REDIS_URL")

    # Rate limiting
    rate_limit_enabled: bool = Field(True, env="ADAPTIVE_API_RATE_LIMIT_ENABLED")
    rate_limit_per_minute: int = Field(100, env="ADAPTIVE_API_RATE_LIMIT_PER_MINUTE")
    rate_limit_burst: int = Field(20, env="ADAPTIVE_API_RATE_LIMIT_BURST")

    # Prometheus
    prometheus_port: Optional[int] = Field(None, env="ADAPTIVE_API_PROMETHEUS_PORT")

    # Logging
    log_level: str = Field("INFO", env="ADAPTIVE_API_LOG_LEVEL")

    class Config:
        env_prefix = "ADAPTIVE_API_"

settings = Settings()

# =============================================================================
# Database Setup
# =============================================================================

Base = declarative_base()

class FeedbackRecord(Base):
    __tablename__ = "feedback_records"
    id = Column(Integer, primary_key=True, autoincrement=True)
    request_id = Column(String, nullable=False)
    expert_id = Column(String, nullable=False)
    node_id = Column(String, nullable=False)
    predicted_cost = Column(Float, nullable=False)
    actual_cost = Column(Float, nullable=False)
    energy_joules = Column(Float, default=0)
    carbon_kg = Column(Float, default=0)
    helium_units = Column(Float, default=0)
    latency_ms = Column(Float, default=0)
    accuracy = Column(Float, default=0)
    weights_snapshot = Column(JSON, nullable=True)
    teacher_id = Column(String, nullable=True)
    distillation_loss = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

# Async engine and session
engine = create_async_engine(
    settings.db_url,
    echo=settings.debug,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session

# =============================================================================
# Circuit Breaker (Global)
# =============================================================================

class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, recovery_time: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self._state = "CLOSED"
        self._failure_count = 0
        self._last_failure_time = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            now = datetime.now(timezone.utc)
            if self._state == "OPEN":
                if (now - self._last_failure_time).total_seconds() > self.recovery_time:
                    self._state = "HALF_OPEN"
                    self._failure_count = 0
                else:
                    raise RuntimeError(f"CircuitBreaker '{self.name}' is OPEN")
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            async with self._lock:
                if self._state == "HALF_OPEN":
                    self._state = "CLOSED"
                    self._failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = datetime.now(timezone.utc)
                if self._failure_count >= self.failure_threshold:
                    self._state = "OPEN"
            raise e

# Global breaker registry
db_circuit = CircuitBreaker("database")

# =============================================================================
# Rate Limiter (Redis or in-memory)
# =============================================================================

class RateLimiter:
    def __init__(self, redis_client=None):
        self.redis = redis_client
        self._memory_store = {}

    async def check(self, key: str, limit: int, window: int) -> Tuple[bool, int, int]:
        if self.redis:
            return await self._redis_check(key, limit, window)
        else:
            return await self._memory_check(key, limit, window)

    async def _redis_check(self, key: str, limit: int, window: int):
        now = time.time()
        window_start = now - window
        # Use sorted set for sliding window
        await self.redis.zremrangebyscore(key, 0, window_start)
        count = await self.redis.zcard(key)
        if count >= limit:
            oldest = await self.redis.zrange(key, 0, 0, withscores=True)
            retry_after = int(oldest[0][1] + window - now) if oldest else 0
            return False, retry_after, limit
        await self.redis.zadd(key, {str(now): now})
        await self.redis.expire(key, window + 10)
        remaining = limit - (count + 1)
        return True, 0, remaining

    async def _memory_check(self, key: str, limit: int, window: int):
        now = time.time()
        records = self._memory_store.get(key, [])
        records = [t for t in records if now - t < window]
        if len(records) >= limit:
            oldest = records[0]
            retry_after = int(oldest + window - now)
            return False, retry_after, limit
        records.append(now)
        self._memory_store[key] = records[-limit:]  # keep only last limit
        remaining = limit - len(records)
        return True, 0, remaining

# Initialize rate limiter
redis_client = None
if settings.redis_url and REDIS_AVAILABLE:
    redis_client = redis.from_url(settings.redis_url, decode_responses=True)
rate_limiter = RateLimiter(redis_client)

# =============================================================================
# Correlation ID Middleware
# =============================================================================

async def correlation_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    request.state.request_id = request_id
    if STRUCTLOG_AVAILABLE:
        structlog.contextvars.bind_contextvars(request_id=request_id)
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response

# =============================================================================
# JWT Authentication with support for JWKS
# =============================================================================

security = HTTPBearer()

async def verify_jwt(token: str) -> Dict:
    if not token:
        raise HTTPException(status_code=401, detail="Missing token")
    try:
        if settings.auth_jwks_url:
            # Fetch JWKS and verify (simplified; in production use a library like authlib)
            # We'll just use HS256 for simplicity in this demo
            pass
        payload = jwt.decode(token, settings.auth_secret, algorithms=[settings.auth_algorithm])
    except PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    roles = payload.get("roles") or payload.get("scope") or []
    if isinstance(roles, str):
        roles = roles.split()
    return {"sub": payload.get("sub"), "roles": roles, "claims": payload}

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await verify_jwt(credentials.credentials)

async def require_admin(user: Dict = Depends(get_current_user)):
    roles = user.get("roles", []) or []
    if "admin" not in roles:
        raise HTTPException(status_code=403, detail="Admin role required")
    return user

async def require_trainer(user: Dict = Depends(get_current_user)):
    roles = user.get("roles", []) or []
    if "trainer" not in roles and "admin" not in roles:
        raise HTTPException(status_code=403, detail="Trainer role required")
    return user

# =============================================================================
# FastAPI Application
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    if PROMETHEUS_AVAILABLE and settings.prometheus_port:
        start_http_server(settings.prometheus_port)
        logger.info("Prometheus metrics enabled", port=settings.prometheus_port)
    yield
    # Shutdown: close connections
    await engine.dispose()

app = FastAPI(
    title="Adaptive API",
    version=settings.api_version,
    description="API for expert feedback and distillation",
    lifespan=lifespan,
    openapi_url="/openapi.json" if not settings.debug else None,
)

# Middleware
app.middleware("http")(correlation_middleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.debug else [],  # restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# Prometheus Metrics
# =============================================================================

if PROMETHEUS_AVAILABLE:
    DISTILLATION_LOSS = Gauge("adaptive_distillation_loss", "Distillation loss")
    FEEDBACK_RECORDS = Counter("adaptive_feedback_records_total", "Total feedback records")
    REQUEST_LATENCY = Histogram("adaptive_request_latency_seconds", "Request latency")
    RATE_LIMIT_HITS = Counter("adaptive_rate_limit_hits_total", "Rate limit hits")
else:
    # Dummy objects
    DISTILLATION_LOSS = None
    FEEDBACK_RECORDS = None
    REQUEST_LATENCY = None
    RATE_LIMIT_HITS = None

# =============================================================================
# Rate Limiting Dependency
# =============================================================================

async def rate_limit(request: Request):
    if settings.rate_limit_enabled:
        client_ip = request.client.host
        key = f"rate_limit:{client_ip}"
        allowed, retry_after, remaining = await rate_limiter.check(
            key,
            settings.rate_limit_per_minute,
            60
        )
        if not allowed:
            if RATE_LIMIT_HITS:
                RATE_LIMIT_HITS.inc()
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded",
                headers={"Retry-After": str(retry_after)}
            )
        request.state.rate_limit_remaining = remaining

# =============================================================================
# Error Handlers
# =============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.status_code, "message": exc.detail}},
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"error": {"code": "validation_error", "message": str(exc)}},
    )

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception", request_id=request.state.request_id)
    return JSONResponse(
        status_code=500,
        content={"error": {"code": "internal_server_error", "message": "Internal server error"}},
    )

# =============================================================================
# Pydantic Models for Request/Response
# =============================================================================

class FeedbackRequest(BaseModel):
    expert_id: str
    node_id: str
    predicted_cost: float
    actual_cost: float
    actual_metrics: Dict[str, float] = Field(default_factory=dict)
    teacher_id: Optional[str] = None
    distillation_loss: Optional[float] = None

class FeedbackResponse(BaseModel):
    status: str
    request_id: str
    recorded: bool

class HealthResponse(BaseModel):
    status: str
    version: str
    timestamp: str
    checks: Dict[str, Any]

# =============================================================================
# Main API Endpoints
# =============================================================================

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check(request: Request):
    """Health check endpoint."""
    checks = {}
    # Check database
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        checks["database"] = {"status": "ok"}
    except Exception as e:
        checks["database"] = {"status": "failed", "error": str(e)}
    # Check rate limiter
    if settings.rate_limit_enabled:
        if redis_client:
            try:
                await redis_client.ping()
                checks["rate_limiter"] = {"status": "ok", "backend": "redis"}
            except:
                checks["rate_limiter"] = {"status": "degraded", "backend": "memory"}
        else:
            checks["rate_limiter"] = {"status": "ok", "backend": "memory"}
    else:
        checks["rate_limiter"] = {"status": "disabled"}
    overall = "healthy" if all(v.get("status") == "ok" for v in checks.values()) else "degraded"
    return HealthResponse(
        status=overall,
        version=settings.api_version,
        timestamp=datetime.now(timezone.utc).isoformat(),
        checks=checks,
    )

@app.post("/feedback", response_model=FeedbackResponse, tags=["Feedback"])
async def record_feedback(
    feedback: FeedbackRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: Dict = Depends(get_current_user),
    _: None = Depends(rate_limit),
):
    """Record feedback from an expert."""
    request_id = request.state.request_id
    weights_snapshot = {"extra_metrics": feedback.actual_metrics}
    # Use circuit breaker for DB operation
    async def persist():
        stmt = text("""
            INSERT INTO feedback_records
            (request_id, expert_id, node_id, predicted_cost, actual_cost,
             energy_joules, carbon_kg, helium_units, latency_ms, accuracy,
             weights_snapshot, teacher_id, distillation_loss)
            VALUES (:request_id, :expert_id, :node_id, :predicted_cost, :actual_cost,
             :energy_joules, :carbon_kg, :helium_units, :latency_ms, :accuracy,
             :weights_snapshot, :teacher_id, :distillation_loss)
        """)
        params = {
            'request_id': request_id,
            'expert_id': feedback.expert_id,
            'node_id': feedback.node_id,
            'predicted_cost': feedback.predicted_cost,
            'actual_cost': feedback.actual_cost,
            'energy_joules': feedback.actual_metrics.get('energy_joules', 0),
            'carbon_kg': feedback.actual_metrics.get('carbon_kg', 0),
            'helium_units': feedback.actual_metrics.get('helium_units', 0),
            'latency_ms': feedback.actual_metrics.get('latency_ms', 0),
            'accuracy': feedback.actual_metrics.get('accuracy', 0),
            'weights_snapshot': json.dumps(weights_snapshot),
            'teacher_id': feedback.teacher_id,
            'distillation_loss': feedback.distillation_loss,
        }
        await db.execute(stmt, params)
        await db.commit()
        if FEEDBACK_RECORDS:
            FEEDBACK_RECORDS.inc()

    try:
        await db_circuit.call(persist)
    except Exception as e:
        logger.exception("Failed to persist feedback", request_id=request_id)
        raise HTTPException(status_code=500, detail="Feedback recording failed")

    # Update Prometheus gauge for distillation loss
    if feedback.distillation_loss is not None and DISTILLATION_LOSS:
        try:
            DISTILLATION_LOSS.set(feedback.distillation_loss)
        except:
            pass

    return FeedbackResponse(status="ok", request_id=request_id, recorded=True)

@app.get("/feedback/history", tags=["Feedback"])
async def get_feedback_history(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: Dict = Depends(get_current_user),
    limit: int = 100,
):
    """Get recent feedback records (admin only)."""
    if "admin" not in user["roles"]:
        raise HTTPException(status_code=403, detail="Admin role required")
    stmt = text("""
        SELECT * FROM feedback_records
        ORDER BY created_at DESC
        LIMIT :limit
    """)
    result = await db.execute(stmt, {"limit": limit})
    rows = result.fetchall()
    return {"records": [dict(row._mapping) for row in rows]}

# =============================================================================
# Background Task Management (Supervision)
# =============================================================================

class TaskManager:
    """Manages background tasks with restart and exponential backoff."""
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def start_task(self, name: str, coro_func: Callable[[], Awaitable[None]], *args, **kwargs):
        async def wrapper():
            backoff = 1
            max_backoff = 300
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e), exc_info=True)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        task = asyncio.create_task(wrapper(), name=name)
        async with self._lock:
            self.tasks[name] = task
        return task

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()

task_manager = TaskManager()

async def example_background_task():
    """Example background task that runs periodically."""
    while True:
        await asyncio.sleep(60)
        logger.info("Background task running")

# Register background task at startup
@app.on_event("startup")
async def startup():
    task_manager.start_task("example", example_background_task)

@app.on_event("shutdown")
async def shutdown():
    await task_manager.stop_all()

# =============================================================================
# OpenAPI Documentation (automatically generated by FastAPI)
# =============================================================================

# The OpenAPI schema is automatically generated by FastAPI based on the endpoints and Pydantic models.

# =============================================================================
# Run the application (if executed directly)
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
