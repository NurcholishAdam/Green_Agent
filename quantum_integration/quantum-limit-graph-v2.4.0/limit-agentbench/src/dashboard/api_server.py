# src/dashboard/api_server.py

"""
Green Agent Dashboard API Server
================================

FastAPI + WebSocket server exposing real-time metrics, execution logs,
and Prometheus-compatible scrape output.

Run with::

    python -m dashboard.api_server --host 127.0.0.1 --port 8000

or::

    uvicorn dashboard.api_server:app --host 127.0.0.1 --port 8000

Enhancements
------------
- ``DashboardAPIConfig`` — frozen, fully validated: CORS origins, port,
  host, max history caps, WebSocket broadcast cadence. Environment
  variables are read at **application construction time**, so a bad
  ``GREEN_AGENT_DASHBOARD_PORT`` value raises :class:`DashboardAPIError`
  with the offending variable named, not a ``ValueError`` at module
  import.
- ``ExecutionLog`` — Pydantic model with validation (finite non-negative
  values, non-empty ``task_id``, ISO-8601 ``timestamp``, ``extra="forbid"``).
  ``bool`` is rejected in numeric fields (Pydantic's default coercion
  would have accepted ``True`` as ``1.0``).
- **Real Prometheus counters** — monotonic ``total_energy_consumed`` /
  ``total_carbon_emitted`` / ``total_tasks_completed`` are incremented
  on every accepted log entry; ``/metrics`` reads the counters, not
  ``sum(bounded_deque)`` (which would stop growing once the deque fills
  and start decreasing as old entries roll off — violating the
  ``counter`` contract).
- **Per-app state** — everything (deques, lock, counters, active tasks,
  websocket subscribers) lives on ``app.state``. Two ``create_app()``
  calls in the same process are independent — no shared globals, no
  cross-test contamination.
- **CORS is coherent** — ``allow_credentials=False`` when
  ``allow_origins`` includes ``"*"`` (the combination the Fetch standard
  forbids). A caller who needs credentialed requests configures explicit
  origins; ``"*"`` continues to work for anonymous access.
- **WebSocket pub/sub** — every connection owns an ``asyncio.Queue``.
  Broadcasts are synchronous ``put_nowait`` calls, so a POST does not
  wait on N websocket sends; a slow client's queue is capped and drops
  messages rather than blocking the broadcaster. No polling loop; the
  connection only sends on new data.
- **Lifespan-based shutdown** — active WebSocket subscribers are woken
  with a sentinel on shutdown, the FastAPI app closes cleanly.
- ``/executions`` — ``limit`` is bounded to ``[1, 10_000]`` (previously
  ``limit=0`` returned every entry and negative limits returned
  unexpected slices); an ``order`` parameter (``asc``/``desc``) is
  supported.
- ``/ready`` — the API check acquires the state lock; the other checks
  are marked as placeholders so an operator sees that the endpoint is
  not yet a real readiness probe.
- ``active_tasks`` is now live — ``POST /executions/start`` registers a
  running task, ``POST /executions/log`` removes it on completion, and
  ``/metrics`` reports the real count.
- ``main()`` — wraps ``import uvicorn`` with a clear error;
  CLI overrides for ``--host`` / ``--port`` / ``--cors`` are optional and
  fall back to environment variables.
- ``__version__`` exported and used in FastAPI, ``/``, and ``/health``.
- A ``_warn_if_exposed`` helper logs at ``INFO`` when the server is
  binding to a public interface.
- ``__main__`` uses the documented ``streamlit.runtime.exists``-style
  pattern (no — Streamlit is not used here; ``__main__`` simply calls
  ``main()``).

Compatibility
-------------
- ``create_app(*, cors_origins=...)`` — signature preserved; a new
  keyword-only ``config=`` parameter takes precedence. Passing both
  ``config`` and ``cors_origins`` raises :class:`DashboardAPIError`.
- ``main(argv=None) -> int`` — same signature, same return type.
- ``app`` — still importable as a module attribute; it is now lazily
  constructed (PEP 562) so importing the module does **not** read
  environment variables. ``from dashboard.api_server import app`` still
  works and raises :class:`DashboardAPIError` on misconfiguration at
  that point.
- ``ExecutionLog`` — model schema unchanged; extra fields are now
  rejected (``extra="forbid"``); ``timestamp`` must be ISO-8601 when
  supplied; ``bool`` is no longer accepted in numeric fields.
- ``/metrics`` — the ``counter`` metrics now reflect monotonic totals
  (previously they reflected the sum of a bounded deque, which capped
  at ``max_history`` and could decrease).
- ``POST /executions/log`` — response model is now
  :class:`LogResponse` (same fields as the previous
  ``Dict[str, Any]``); the endpoint no longer blocks on WebSocket sends.
- ``POST /executions/start`` — new endpoint.
- ``GET /executions`` — ``limit`` is bounded; ``order`` query parameter
  added; response includes an ``order`` echo.
- ``/ready`` — checks dict is now honest about which checks are
  placeholders.

Requires Python 3.8+.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import math
import os
import threading
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import (
    Any, AsyncIterator, Deque, Dict, List, Optional, Sequence, Tuple,
)

from fastapi import FastAPI, Query, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class DashboardAPIError(ValueError):
    """Raised for invalid API configuration or runtime errors."""


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DashboardAPIConfig:
    """Frozen, validated configuration for the dashboard API server."""

    cors_origins: Tuple[str, ...] = ("*",)
    host: str = "0.0.0.0"
    port: int = 8000
    max_history: int = 10_000

    def __post_init__(self) -> None:
        if not isinstance(self.cors_origins, (tuple, list)) or (
            not self.cors_origins
        ):
            raise DashboardAPIError(
                "cors_origins must be a non-empty sequence of strings."
            )
        cleaned: List[str] = []
        for i, origin in enumerate(self.cors_origins):
            if not isinstance(origin, str) or not origin:
                raise DashboardAPIError(
                    f"cors_origins[{i}] must be a non-empty string."
                )
            cleaned.append(origin)
        if not isinstance(self.cors_origins, tuple):
            object.__setattr__(
                self, "cors_origins", tuple(cleaned),
            )

        if not isinstance(self.host, str) or not self.host:
            raise DashboardAPIError(
                "host must be a non-empty string."
            )
        if isinstance(self.port, bool) or not isinstance(self.port, int):
            raise DashboardAPIError("port must be an int.")
        if not 1 <= self.port <= 65_535:
            raise DashboardAPIError(
                f"port must be in [1, 65535], got {self.port}."
            )
        if isinstance(self.max_history, bool) or not isinstance(
            self.max_history, int
        ):
            raise DashboardAPIError("max_history must be an int.")
        if self.max_history <= 0:
            raise DashboardAPIError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cors_origins": list(self.cors_origins),
            "host": self.host,
            "port": self.port,
            "max_history": self.max_history,
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        *,
        strict: bool = True,
    ) -> "DashboardAPIConfig":
        if not isinstance(data, dict):
            raise DashboardAPIError(
                "DashboardAPIConfig.from_dict expects a Mapping."
            )
        valid = {
            "cors_origins", "host", "port", "max_history",
        }
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise DashboardAPIError(
                    f"unknown DashboardAPIConfig keys: {unknown}"
                )
        try:
            kwargs: Dict[str, Any] = {}
            for k, v in data.items():
                if k not in valid:
                    continue
                if k == "cors_origins":
                    kwargs[k] = tuple(v or ())
                else:
                    kwargs[k] = v
            return cls(**kwargs)
        except DashboardAPIError:
            raise
        except (TypeError, ValueError) as exc:
            raise DashboardAPIError(
                f"invalid DashboardAPIConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "DashboardAPIConfig("
            f"cors_origins={list(self.cors_origins)!r}, "
            f"host={self.host!r}, "
            f"port={self.port}, "
            f"max_history={self.max_history})"
        )


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    """Read ``name`` from the environment; raise on invalid values."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise DashboardAPIError(
            f"environment variable {name}={raw!r} is not an integer: {exc}"
        ) from exc
    if value < minimum:
        raise DashboardAPIError(
            f"environment variable {name}={value} must be >= {minimum}."
        )
    return value


def _config_from_env() -> DashboardAPIConfig:
    """Build a :class:`DashboardAPIConfig` from environment variables."""
    raw_cors = os.environ.get("GREEN_AGENT_CORS_ORIGINS", "*")
    origins: Tuple[str, ...] = tuple(
        s.strip() for s in raw_cors.split(",") if s.strip()
    )
    if not origins:
        origins = ("*",)
    return DashboardAPIConfig(
        cors_origins=origins,
        host=os.environ.get("GREEN_AGENT_DASHBOARD_HOST", "0.0.0.0"),
        port=_env_int("GREEN_AGENT_DASHBOARD_PORT", 8000),
        max_history=_env_int("GREEN_AGENT_MAX_HISTORY", 10_000),
    )


def _coerce_config(
    config: Optional[DashboardAPIConfig],
    cors_origins: Optional[Sequence[str]],
) -> DashboardAPIConfig:
    """Return a validated config, honoring the two construction paths."""
    if config is not None and cors_origins is not None:
        raise DashboardAPIError(
            "pass either config= or cors_origins=, not both."
        )
    if config is not None:
        if not isinstance(config, DashboardAPIConfig):
            raise DashboardAPIError(
                f"config must be a DashboardAPIConfig, got "
                f"{type(config).__name__}."
            )
        return config
    if cors_origins is not None:
        if isinstance(cors_origins, str):
            raise DashboardAPIError(
                "cors_origins must be a sequence of strings, not a "
                "single string; use [cors_origins] instead."
            )
        return DashboardAPIConfig(cors_origins=tuple(cors_origins))
    return _config_from_env()


def _warn_if_exposed(host: str) -> None:
    """Log at INFO when the server will be reachable from any interface."""
    if host in ("0.0.0.0", "::"):
        logger.info(
            "Dashboard API server binding to %r — reachable from any "
            "network interface. Use --host 127.0.0.1 for local-only "
            "access.",
            host,
        )


# --------------------------------------------------------------------------- #
# Pydantic models
# --------------------------------------------------------------------------- #
class ExecutionLog(BaseModel):
    """Schema for execution log entries with validation."""

    model_config = ConfigDict(extra="forbid")

    task_id: str = Field(..., min_length=1)
    energy_consumed: float = Field(..., ge=0.0)
    carbon_emitted: float = Field(..., ge=0.0)
    accuracy: float = Field(..., ge=0.0, le=1.0)
    carbon_zone: str = Field(..., min_length=1)
    timestamp: Optional[str] = None

    @field_validator(
        "energy_consumed", "carbon_emitted", "accuracy", mode="before",
    )
    @classmethod
    def _reject_bool(cls, v: Any) -> Any:
        if isinstance(v, bool):
            raise ValueError(
                "must be numeric, not bool (True/False are not metrics)"
            )
        return v

    @field_validator("energy_consumed", "carbon_emitted", "accuracy")
    @classmethod
    def _finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("must be finite")
        return v

    @field_validator("timestamp")
    @classmethod
    def _valid_iso_timestamp(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                f"timestamp must be ISO-8601: {exc}"
            ) from exc
        return v


class ActiveTaskRequest(BaseModel):
    """Body for ``POST /executions/start``."""

    model_config = ConfigDict(extra="forbid")

    task_id: str = Field(..., min_length=1)
    started_at: Optional[str] = None

    @field_validator("started_at")
    @classmethod
    def _valid_iso_started_at(
        cls, v: Optional[str],
    ) -> Optional[str]:
        if v is None:
            return v
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                f"started_at must be ISO-8601: {exc}"
            ) from exc
        return v


class LogResponse(BaseModel):
    status: str
    timestamp: str


class StartResponse(BaseModel):
    status: str
    task_id: str
    timestamp: str


# --------------------------------------------------------------------------- #
# Lifespan
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    yield
    # Shutdown: wake up all websocket subscribers so their handlers
    # exit cleanly, then log.
    with app.state.lock:
        subscribers = list(app.state.websocket_subscribers.values())
    for q in subscribers:
        try:
            q.put_nowait(None)  # sentinel
        except asyncio.QueueFull:
            pass
    logger.info("Dashboard API server shutdown.")


# --------------------------------------------------------------------------- #
# Broadcast
# --------------------------------------------------------------------------- #
def _broadcast_metrics(app_state: Any, metrics: Dict[str, Any]) -> None:
    """
    Enqueue ``metrics`` into every subscriber's queue.

    Synchronous — the caller does not await any client send. Slow
    clients have a bounded queue; overflow drops the message and logs at
    ``DEBUG``.
    """
    message = {
        "type": "metrics_update",
        "data": metrics,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    with app_state.lock:
        subscribers = list(app_state.websocket_subscribers.values())
    if not subscribers:
        return
    dropped = 0
    for q in subscribers:
        try:
            q.put_nowait(message)
        except asyncio.QueueFull:
            dropped += 1
    if dropped:
        logger.debug(
            "Broadcast: %d/%d subscribers dropped (queue full).",
            dropped, len(subscribers),
        )


# --------------------------------------------------------------------------- #
# App factory
# --------------------------------------------------------------------------- #
def create_app(
    *,
    config: Optional[DashboardAPIConfig] = None,
    cors_origins: Optional[Sequence[str]] = None,
) -> FastAPI:
    """
    Build a fully-wired FastAPI app.

    Parameters
    ----------
    config : DashboardAPIConfig, optional
    cors_origins : Sequence[str], optional
        Backward-compatible shortcut that constructs a config with the
        given origins and the default host/port/max_history. Cannot be
        combined with ``config``.
    """
    cfg = _coerce_config(config, cors_origins)

    app = FastAPI(
        title="Green Agent Dashboard",
        description="Real-time monitoring for sustainable AI",
        version=__version__,
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=_lifespan,
    )

    # ---- CORS: credentials + wildcard is forbidden by the Fetch spec -- #
    allow_credentials = "*" not in cfg.cors_origins
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cfg.cors_origins),
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- Per-app state ---------------------------------------------- #
    app.state.config = cfg
    app.state.lock = threading.RLock()
    app.state.metrics_history: Deque[Dict[str, Any]] = deque(
        maxlen=cfg.max_history,
    )
    app.state.execution_logs: Deque[Dict[str, Any]] = deque(
        maxlen=cfg.max_history,
    )
    app.state.active_tasks: Dict[str, Dict[str, Any]] = {}
    app.state.websocket_subscribers: Dict[int, "asyncio.Queue"] = {}
    # Monotonic counters — real Prometheus counters, not deque sums.
    app.state.total_energy_consumed: float = 0.0
    app.state.total_carbon_emitted: float = 0.0
    app.state.total_tasks_completed: int = 0

    # ------------------------------------------------------------------ #
    # Routes
    # ------------------------------------------------------------------ #
    @app.get("/")
    async def root() -> Dict[str, Any]:
        return {
            "service": "Green Agent Dashboard",
            "version": __version__,
            "status": "running",
            "endpoints": {
                "health": "/health",
                "ready": "/ready",
                "metrics": "/metrics",
                "executions": "/executions",
                "start": "/executions/start",
                "log": "/executions/log",
                "websocket": "/ws/metrics",
            },
        }

    @app.get("/health")
    async def health_check() -> Dict[str, Any]:
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": __version__,
        }

    @app.get("/ready")
    async def readiness_check() -> JSONResponse:
        checks: Dict[str, Any] = {}
        try:
            with app.state.lock:
                pass
            checks["api"] = True
        except Exception:
            checks["api"] = False
        # Placeholders — replace with real dependency probes when
        # database / carbon API clients are wired in.
        checks["database"] = "not_configured"
        checks["carbon_api"] = "not_configured"
        # Only "False" entries are unhealthy; strings mean "not checked".
        all_healthy = not any(c is False for c in checks.values())
        return JSONResponse(
            status_code=200 if all_healthy else 503,
            content={
                "ready": all_healthy,
                "checks": checks,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    @app.get("/metrics", response_class=PlainTextResponse)
    async def prometheus_metrics() -> str:
        """Prometheus text exposition (version 0.0.4)."""
        with app.state.lock:
            energy = app.state.total_energy_consumed
            carbon = app.state.total_carbon_emitted
            completed = app.state.total_tasks_completed
            active = len(app.state.active_tasks)
            ws_conns = len(app.state.websocket_subscribers)

        lines = [
            "# HELP green_agent_energy_consumed_kwh "
            "Total energy consumed.",
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
            "# HELP green_agent_websocket_connections "
            "Active WebSocket connections.",
            "# TYPE green_agent_websocket_connections gauge",
            f"green_agent_websocket_connections {ws_conns}",
            "",
        ]
        return "\n".join(lines)

    @app.get("/executions")
    async def get_executions(
        limit: int = Query(100, ge=1, le=10_000),
        order: str = Query("asc", pattern="^(asc|desc)$"),
    ) -> Dict[str, Any]:
        with app.state.lock:
            logs = list(app.state.execution_logs)
        if order == "desc":
            selected = list(reversed(logs))[:limit]
        else:
            selected = logs[-limit:]
        return {
            "executions": selected,
            "total": len(logs),
            "order": order,
        }

    @app.post("/executions/start", response_model=StartResponse)
    async def start_execution(
        request: ActiveTaskRequest,
    ) -> StartResponse:
        now = datetime.now(timezone.utc).isoformat()
        with app.state.lock:
            app.state.active_tasks[request.task_id] = {
                "task_id": request.task_id,
                "started_at": request.started_at or now,
                "registered_at": now,
            }
        return StartResponse(
            status="started", task_id=request.task_id, timestamp=now,
        )

    @app.post("/executions/log", response_model=LogResponse)
    async def log_execution(execution: ExecutionLog) -> LogResponse:
        now = datetime.now(timezone.utc).isoformat()
        execution_dict = execution.model_dump()
        execution_dict["logged_at"] = now
        if execution_dict.get("timestamp") is None:
            execution_dict["timestamp"] = now

        with app.state.lock:
            app.state.metrics_history.append(execution_dict)
            app.state.execution_logs.append(dict(execution_dict))
            app.state.total_energy_consumed += float(
                execution.energy_consumed
            )
            app.state.total_carbon_emitted += float(
                execution.carbon_emitted
            )
            app.state.total_tasks_completed += 1
            app.state.active_tasks.pop(execution.task_id, None)

        _broadcast_metrics(app.state, execution_dict)
        return LogResponse(status="logged", timestamp=now)

    @app.websocket("/ws/metrics")
    async def websocket_metrics(websocket: WebSocket) -> None:
        await websocket.accept()
        ws_id = id(websocket)
        queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        with app.state.lock:
            app.state.websocket_subscribers[ws_id] = queue
            latest = (
                app.state.metrics_history[-1]
                if app.state.metrics_history
                else None
            )

        # Send current state immediately so a fresh connection has data.
        if latest is not None:
            await websocket.send_json({
                "type": "metrics_update",
                "data": latest,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        try:
            while True:
                message = await queue.get()
                if message is None:  # shutdown sentinel
                    await websocket.close()
                    break
                await websocket.send_json(message)
        except Exception:
            logger.exception("WebSocket handler failed")
        finally:
            with app.state.lock:
                app.state.websocket_subscribers.pop(ws_id, None)

    return app


# --------------------------------------------------------------------------- #
# Lazy module-level app
# --------------------------------------------------------------------------- #
_app: Optional[FastAPI] = None


def __getattr__(name: str) -> Any:
    """
    Lazily construct the module-level ``app`` (PEP 562).

    Importing the module does not read environment variables; accessing
    ``api_server.app`` does. Misconfiguration therefore surfaces on
    first access with a clear :class:`DashboardAPIError`, not as a
    ``ValueError`` from ``int("abc")`` at import time.
    """
    global _app
    if name == "app":
        if _app is None:
            _app = create_app()
        return _app
    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}"
    )


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dashboard.api_server")
    parser.add_argument(
        "--host", default=None,
        help=(
            "Interface to bind to (default: from GREEN_AGENT_DASHBOARD_HOST "
            "or '0.0.0.0')."
        ),
    )
    parser.add_argument(
        "--port", type=int, default=None,
        help=(
            "Port to listen on (default: from GREEN_AGENT_DASHBOARD_PORT "
            "or 8000)."
        ),
    )
    parser.add_argument(
        "--cors", nargs="+", default=None,
        help=(
            "Allowed CORS origins (default: comma-separated "
            "GREEN_AGENT_CORS_ORIGINS, or '*')."
        ),
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable DEBUG logging.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the dashboard API server."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # ---- Build config from env, then apply CLI overrides ---------- #
    try:
        cfg = _config_from_env()
    except DashboardAPIError as exc:
        logger.error("Configuration error: %s", exc)
        return 1

    overrides: Dict[str, Any] = {}
    if args.host is not None:
        overrides["host"] = args.host
    if args.port is not None:
        overrides["port"] = args.port
    if args.cors is not None:
        overrides["cors_origins"] = tuple(args.cors)
    if overrides:
        try:
            cfg = replace(cfg, **overrides)
        except DashboardAPIError as exc:
            logger.error("Configuration error: %s", exc)
            return 1

    _warn_if_exposed(cfg.host)

    try:
        import uvicorn  # type: ignore
    except ImportError:
        logger.error(
            "uvicorn is required to run the server; install it with "
            "`pip install uvicorn`."
        )
        return 1

    try:
        app_instance = create_app(config=cfg)
    except DashboardAPIError as exc:
        logger.error("Could not create app: %s", exc)
        return 1

    uvicorn.run(app_instance, host=cfg.host, port=cfg.port)
    return 0


__all__: List[str] = [
    "__version__",
    "ActiveTaskRequest",
    "DashboardAPIConfig",
    "DashboardAPIError",
    "ExecutionLog",
    "LogResponse",
    "StartResponse",
    "create_app",
    "main",
]


def __dir__() -> List[str]:
    return sorted(__all__)


if __name__ == "__main__":
    import sys
    sys.exit(main())
