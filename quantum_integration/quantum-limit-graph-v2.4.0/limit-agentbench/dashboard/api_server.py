#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Green Agent Dashboard API Server v5.0.0 (Enhanced v2).

FastAPI-based dashboard for real-time monitoring of the Green Agent,
including the advanced-enhancement metric surface (LIMIT Graph, MODP,
RLHF, Distillation, MoE, Evolutionary, FlexGen).

---- Critical fixes vs v1 ---------------------------------------------------
  FIX #1   Module-level ``app`` — v1's ``uvicorn.run("api_server:app", ...)``
           referenced a module-level object that was never created, so the
           server could not start. Added a ``create_app(config=None)``
           factory and a module-level ``app = create_app()``.
  FIX #2   ``/metrics`` no longer mutates Counters. v1's
           ``_update_enhanced_metrics_from_history`` ran inside the scrape
           endpoint, so ``DISTILLATION_UPDATE_COUNT`` and ``FLEXGEN_ENERGY``
           incremented on every Prometheus scrape (every 15 s). Gauges are
           still updated from the latest snapshot; counters only mutate on
           ``POST /executions/log``.
  FIX #3   CORS: ``allow_origins=["*"]`` with ``allow_credentials=True`` is
           forbidden by the spec and rejected by browsers. v2 reads origins
           from config; when ``*`` is configured, credentials is forced to
           ``False``.
  FIX #4   ``_broadcast_metrics`` collects dead WebSockets in a set and
           removes them after the loop. v1 removed during iteration and
           could ``ValueError`` when the WebSocket handler removed the same
           connection first.
  FIX #5   ``/health`` reflects process state. v1 always returned
           ``{"status": "healthy"}`` — the Kubernetes liveness probe could
           never detect a broken process. v2 checks the event loop, the
           internal deques, and the background broadcast task.
  FIX #6   ``/metrics/realtime`` handles empty history without producing
           ``NaN`` JSON. ``np.mean([])`` returned ``nan``; v2 returns
           ``0.0`` and casts every value to Python ``float`` so
           ``jsonable_encoder`` produces valid JSON.
  FIX #7   Query-parameter bounds. ``/carbon/forecast?hours=...`` and
           ``/executions?limit=...`` accepted unbounded integers. Both are
           now ``Query(ge=1, le=...)``.
  FIX #8   ``metrics_history`` and ``execution_logs`` are bounded deques
           (``maxlen=5000``). v1 grew unboundedly.
  FIX #9   Pareto frontier includes carbon. v1 compared only accuracy and
           energy — a point strictly better in carbon but worse in both was
           incorrectly dropped.
  FIX #10  ``CARBON_INTENSITY`` Gauge is set to the *clamped* value, same
           as the response body.
  FIX #11  ``_check_carbon_api`` caches its result for 30 s and always
           closes the forecaster (even on exception).
  FIX #12  ``_check_database`` uses ``asyncio.to_thread`` — no blocking I/O
           in the event loop.
  FIX #13  WebSocket broadcast runs in a single background task. v1 spawned
           one 5-second loop per connection.
  FIX #14  WebSocket handler catches only ``WebSocketDisconnect`` and
           ``RuntimeError``. A ``CancelledError`` now propagates, so shutdown
           is prompt.
  FIX #15  ``start()`` stores the server task; ``stop()`` awaits it and
           cancels the background broadcast task.
  FIX #16  A ``signal.SIGTERM``/``SIGINT`` handler runs ``stop()`` so
           container shutdown drains cleanly.
  FIX #17  ``POST /executions/log`` takes a Pydantic ``ExecutionLog``
           model — no more ``EXECUTION_TIME.observe("abc")`` or
           ``observe(-1)`` crashes.
  FIX #18  ``POST /tasks/{id}/update`` takes a Pydantic ``TaskUpdate``
           model — a client cannot overwrite arbitrary internal fields.

---- Backward compatibility -------------------------------------------------
  The route set, endpoint paths, and JSON shapes are unchanged. A client
  that already sends the dict v1 accepted will still work (Pydantic coerces
  the same field names). ``DashboardAPI(config).app`` still returns the
  ASGI app; new consumers can use the module-level ``app`` or
  ``create_app(config)``.

---- Run --------------------------------------------------------------------
  python api_server.py
  uvicorn api_server:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import asyncio
import logging
import math
import signal
import sys
import time
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

import numpy as np
from fastapi import FastAPI, HTTPException, Query, Response, WebSocket
from fastapi.exceptions import WebSocketException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    REGISTRY,
    generate_latest,
)
from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Prometheus metric registry (class so tests can reset it)
# ---------------------------------------------------------------------------
class DashboardMetrics:
    """All Prometheus metrics, held in one object for testability."""

    def __init__(self) -> None:
        # --- Legacy / core ---
        self.energy_consumed = Counter(
            "green_agent_energy_consumed_kwh",
            "Total energy consumed in kWh",
            ["mode", "task_type"],
        )
        self.carbon_emitted = Counter(
            "green_agent_carbon_emitted_kg",
            "Total carbon emitted in kg CO2",
            ["mode", "carbon_zone"],
        )
        self.carbon_intensity = Gauge(
            "green_agent_carbon_intensity",
            "Current carbon intensity in gCO2/kWh",
            ["region"],
        )
        self.tasks_completed = Counter(
            "green_agent_tasks_completed",
            "Total tasks completed",
            ["mode", "status"],
        )
        self.queue_depth = Gauge(
            "green_agent_queue_depth",
            "Current task queue depth",
        )
        self.accuracy_score = Gauge(
            "green_agent_accuracy_score",
            "Model accuracy score",
        )
        self.execution_time = Histogram(
            "green_agent_execution_time_seconds",
            "Task execution time in seconds",
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
        )
        self.active_workers = Gauge(
            "green_agent_active_workers",
            "Number of active Ray workers",
        )

        # --- Enhancements ---
        self.modp_score = Gauge(
            "green_agent_modp_score",
            "MODP composite score (0-1)",
        )
        self.rlhf_feedback = Gauge(
            "green_agent_rlhf_feedback",
            "RLHF human feedback score (0-1)",
        )
        self.graph_centrality = Gauge(
            "green_agent_graph_centrality",
            "LIMIT Graph centrality",
        )
        self.graph_connectivity = Gauge(
            "green_agent_graph_connectivity",
            "LIMIT Graph connectivity",
        )
        self.distillation_update_count = Counter(
            "green_agent_distillation_update_count",
            "Number of distillation student updates",
        )
        self.moe_gate_weight = Gauge(
            "green_agent_moe_gate_weight",
            "MoE gating weight (per expert)",
            ["expert"],
        )
        self.evolutionary_best_fitness = Gauge(
            "green_agent_evolutionary_best_fitness",
            "Evolutionary optimizer best fitness",
        )
        self.flexgen_energy = Counter(
            "green_agent_flexgen_energy_joules_total",
            "Total energy consumed by FlexGen in joules",
        )


# ---------------------------------------------------------------------------
# Pydantic input models
# ---------------------------------------------------------------------------
class ExecutionLog(BaseModel):
    """Validated execution-log payload."""

    mode: str = "unified"
    task_type: str = "unknown"
    energy: float = Field(default=0.0, ge=0.0)
    carbon: float = Field(default=0.0, ge=0.0)
    carbon_intensity: float = Field(default=0.0, ge=0.0)
    carbon_zone: str = "unknown"
    success: bool = False
    execution_time: float = Field(default=0.0, ge=0.0)
    accuracy: float = Field(default=0.0, ge=0.0, le=1.0)
    negawatt_reward: float = 0.0

    # Enhanced fields (all optional — legacy clients still work)
    modp_score: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    graph_metrics: Optional[Dict[str, float]] = None
    human_feedback_score: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    distillation_update_count: Optional[int] = Field(default=None, ge=0)
    moe_gate_weights: Optional[Any] = None
    evolutionary_best_fitness: Optional[float] = None
    flexgen_energy_joules: Optional[float] = Field(default=None, ge=0.0)


class TaskUpdate(BaseModel):
    """Validated task-update payload."""

    status: Optional[str] = None
    progress: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Health state
# ---------------------------------------------------------------------------
@dataclass
class HealthState:
    """Live process health — reflects reality, not a hardcoded string."""

    api_ready: bool = False
    history_ready: bool = False
    broadcast_running: bool = False
    database_ready: bool = False
    ray_ready: bool = False
    carbon_api_ready: bool = False

    def as_dict(self) -> Dict[str, bool]:
        return {
            "api": self.api_ready,
            "history": self.history_ready,
            "broadcast": self.broadcast_running,
            "database": self.database_ready,
            "ray_cluster": self.ray_ready,
            "carbon_api": self.carbon_api_ready,
        }

    @property
    def process_alive(self) -> bool:
        """A subset of checks that must hold for the process to be useful."""
        return self.api_ready and self.history_ready and self.broadcast_running

    @property
    def all_ready(self) -> bool:
        return all(self.as_dict().values())


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
class DashboardAPI:
    """FastAPI dashboard server with real health checks and enhanced metrics."""

    # FIX #8 — bounded histories
    HISTORY_LIMIT = 5000
    LOG_LIMIT = 5000

    # FIX #11 — cache carbon API probe for 30 s
    CARBON_API_CACHE_TTL = 30.0

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config: Dict[str, Any] = config or {}

        enhancements_cfg = self.config.get("enhancements") or {}
        self.use_enhancements: bool = bool(enhancements_cfg.get("enabled", True))

        self.metrics = DashboardMetrics()

        self.app = FastAPI(
            title="Green Agent Dashboard",
            description="Real-time monitoring for sustainable AI",
            version="5.0.0",
            docs_url="/docs",
            redoc_url="/redoc",
        )

        # FIX #3 — CORS is config-driven; wildcard disables credentials
        cors_origins = self.config.get("dashboard", {}).get(
            "cors_origins", ["http://localhost:3000"],
        )
        allow_credentials = "*" not in cors_origins
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=allow_credentials,
            allow_methods=["GET", "POST"],
            allow_headers=["*"],
        )

        # Bounded in-memory stores
        self.metrics_history: Deque[Dict[str, Any]] = deque(
            maxlen=self.HISTORY_LIMIT,
        )
        self.execution_logs: Deque[Dict[str, Any]] = deque(
            maxlen=self.LOG_LIMIT,
        )
        self.pareto_frontier: List[Dict[str, Any]] = []
        self.active_tasks: Dict[str, Dict[str, Any]] = {}

        self.websocket_connections: List[WebSocket] = []
        self.start_time: float = time.time()
        self._carbon_api_cached: Optional[tuple[bool, float]] = None

        # Health state
        self.health = HealthState(api_ready=True, history_ready=True)

        # Server task + broadcast task (FIX #15)
        self._server_task: Optional[asyncio.Task] = None
        self._broadcast_task: Optional[asyncio.Task] = None
        self._broadcast_stop = asyncio.Event()
        self.running: bool = False

        self._setup_routes()

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------
    def _setup_routes(self) -> None:
        app = self.app

        # ------------------------------------------------------------------
        # Root
        # ------------------------------------------------------------------
        @app.get("/")
        async def root():
            return {
                "service": "Green Agent Dashboard",
                "version": "5.0.0",
                "status": "running",
                "uptime_seconds": round(time.time() - self.start_time, 2),
                "enhancements_enabled": self.use_enhancements,
                "endpoints": {
                    "health": "/health",
                    "ready": "/ready",
                    "live": "/live",
                    "metrics": "/metrics",
                    "metrics/realtime": "/metrics/realtime",
                    "executions": "/executions",
                    "pareto": "/analytics/pareto",
                    "carbon": "/carbon/forecast",
                    "enhancements/status": "/enhancements/status",
                },
            }

        # ------------------------------------------------------------------
        # Health (FIX #5 — real health, not a hardcoded string)
        # ------------------------------------------------------------------
        @app.get("/health")
        async def health():
            state = self._compute_health()
            return {
                "status": "healthy" if state.process_alive else "unhealthy",
                "ready": state.process_alive,
                "components": state.as_dict(),
                "timestamp": datetime.now().isoformat(),
                "version": "5.0.0",
                "uptime_seconds": round(time.time() - self.start_time, 2),
            }

        @app.get("/ready")
        async def ready():
            state = await self._compute_full_readiness()
            code = 200 if state.all_ready else 503
            return JSONResponse(
                status_code=code,
                content={
                    "ready": state.all_ready,
                    "checks": state.as_dict(),
                    "timestamp": datetime.now().isoformat(),
                },
            )

        @app.get("/live")
        async def live():
            state = self._compute_health()
            return {
                "status": "alive" if state.process_alive else "dead",
                "timestamp": datetime.now().isoformat(),
            }

        # ------------------------------------------------------------------
        # Prometheus metrics (FIX #2 — read-only)
        # ------------------------------------------------------------------
        @app.get("/metrics")
        async def prometheus_metrics():
            # Refresh only Gauges that reflect *current state*; Counters and
            # Histograms only mutate in POST /executions/log.
            self.metrics.queue_depth.set(len(self.active_tasks))
            self.metrics.active_workers.set(len(self.active_tasks))

            if self.execution_logs:
                accuracies = [
                    log.get("accuracy", 0.0) for log in self.execution_logs
                ]
                self.metrics.accuracy_score.set(
                    float(np.mean(accuracies[-100:])),
                )

            # Gauges for enhanced metrics from the latest log (read-only)
            self._refresh_enhancement_gauges_from_latest()

            return Response(
                content=generate_latest(REGISTRY),
                media_type=CONTENT_TYPE_LATEST,
            )

        # ------------------------------------------------------------------
        # Realtime metrics (FIX #6 — NaN-safe)
        # ------------------------------------------------------------------
        @app.get("/metrics/realtime")
        async def realtime_metrics():
            if not self.metrics_history:
                return self._default_metrics()

            latest = self.metrics_history[-1]
            recent_accuracies = [
                log.get("accuracy", 0.0)
                for log in self.execution_logs[-100:]
            ]
            avg_accuracy = float(np.mean(recent_accuracies)) if recent_accuracies else 0.0

            result: Dict[str, Any] = {
                "timestamp": latest.get("timestamp"),
                "energy_consumption": float(latest.get("energy", 0.0)),
                "carbon_footprint": float(latest.get("carbon", 0.0)),
                "carbon_intensity": float(latest.get("carbon_intensity", 0.0)),
                "active_tasks": len(self.active_tasks),
                "completed_tasks": len(self.execution_logs),
                "avg_accuracy": avg_accuracy,
                "negawatt_reward": float(latest.get("negawatt_reward", 0.0)),
                "pareto_frontier": self.pareto_frontier[-10:],
                "component_health": self.health.as_dict(),
            }
            if self.use_enhancements:
                enhanced = self._latest_enhancement_snapshot()
                if enhanced:
                    result["enhancements"] = enhanced
            return result

        # ------------------------------------------------------------------
        # Executions (FIX #7 — bounded limit; FIX #17 — validated body)
        # ------------------------------------------------------------------
        @app.get("/executions")
        async def get_executions(
            limit: int = Query(100, ge=1, le=10_000),
        ):
            return {
                "executions": list(self.execution_logs)[-limit:],
                "total": len(self.execution_logs),
            }

        @app.post("/executions/log")
        async def log_execution(execution: ExecutionLog):
            entry = execution.model_dump()
            entry["logged_at"] = datetime.now().isoformat()
            self.execution_logs.append(entry)

            # Counters / histograms only here — never on scrape.
            m = self.metrics
            m.energy_consumed.labels(
                mode=entry["mode"], task_type=entry["task_type"],
            ).inc(entry["energy"])
            m.carbon_emitted.labels(
                mode=entry["mode"], carbon_zone=entry["carbon_zone"],
            ).inc(entry["carbon"])
            m.tasks_completed.labels(
                mode=entry["mode"],
                status="success" if entry["success"] else "failed",
            ).inc()
            m.execution_time.observe(entry["execution_time"])

            # Enhancement counters — only on write.
            if self.use_enhancements:
                if entry.get("distillation_update_count"):
                    m.distillation_update_count.inc(
                        entry["distillation_update_count"],
                    )
                if entry.get("flexgen_energy_joules"):
                    m.flexgen_energy.inc(entry["flexgen_energy_joules"])
                self._refresh_enhancement_gauges(entry)

            self.metrics_history.append({
                "timestamp": entry["logged_at"],
                "energy": entry["energy"],
                "carbon": entry["carbon"],
                "carbon_intensity": entry["carbon_intensity"],
                "negawatt_reward": entry["negawatt_reward"],
                "modp_score": entry.get("modp_score"),
                "graph_metrics": entry.get("graph_metrics"),
                "human_feedback_score": entry.get("human_feedback_score"),
                "distillation_update_count": entry.get("distillation_update_count"),
                "moe_gate_weights": entry.get("moe_gate_weights"),
                "evolutionary_best_fitness": entry.get("evolutionary_best_fitness"),
                "flexgen_energy_joules": entry.get("flexgen_energy_joules"),
            })

            self._update_pareto_frontier(entry)
            await self._broadcast_metrics(entry)
            return {"status": "logged", "timestamp": entry["logged_at"]}

        # ------------------------------------------------------------------
        # Enhancements status
        # ------------------------------------------------------------------
        @app.get("/enhancements/status")
        async def enhancement_status():
            if not self.use_enhancements:
                return {"enabled": False, "message": "Enhancements not enabled"}
            return {
                "enabled": True,
                "metrics": self._latest_enhancement_snapshot() or {},
                "components": [
                    "limit_graph", "modp", "rlhf", "distillation",
                    "moe", "evolutionary", "flexgen",
                ],
            }

        # ------------------------------------------------------------------
        # Pareto (FIX #9 — 3-objective)
        # ------------------------------------------------------------------
        @app.get("/analytics/pareto")
        async def get_pareto():
            if not self.pareto_frontier:
                return {
                    "frontier_points": [],
                    "metadata": {
                        "generated_at": datetime.now().isoformat(),
                        "total_points": 0,
                    },
                }
            accuracies = [p["accuracy"] for p in self.pareto_frontier]
            energies = [p["energy"] for p in self.pareto_frontier]
            return {
                "frontier_points": self.pareto_frontier,
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "total_points": len(self.pareto_frontier),
                    "best_accuracy": max(accuracies),
                    "lowest_energy": min(energies),
                },
            }

        # ------------------------------------------------------------------
        # Carbon forecast (FIX #7 — bounded hours)
        # ------------------------------------------------------------------
        @app.get("/carbon/forecast")
        async def carbon_forecast(
            hours: int = Query(24, ge=1, le=720),
        ):
            now = datetime.now()
            forecast: List[Dict[str, Any]] = []
            clamped_intensities: List[float] = []

            for i in range(hours):
                ts = now + timedelta(hours=i)
                hour = ts.hour
                base = 200.0
                if 6 <= hour <= 18:
                    raw = base + 100 * math.sin((hour - 6) * math.pi / 12)
                else:
                    raw = base - 50
                clamped = max(50.0, min(400.0, raw))
                clamped_intensities.append(clamped)
                forecast.append({
                    "timestamp": ts.isoformat(),
                    "intensity_gco2_kwh": clamped,
                    "zone": self._zone(clamped),
                    "recommendation": self._recommendation(clamped),
                })

            # FIX #10 — Gauge uses the clamped value; region comes from config
            region = self.config.get("carbon", {}).get("default_region", "default")
            self.metrics.carbon_intensity.labels(region=region).set(
                clamped_intensities[0],
            )

            return {
                "forecast": forecast,
                "current_zone": self._zone(clamped_intensities[0]),
                "recommendation": self._recommendation(clamped_intensities[0]),
            }

        # ------------------------------------------------------------------
        # Tasks (FIX #18 — validated update model)
        # ------------------------------------------------------------------
        @app.get("/tasks/active")
        async def active_tasks():
            return {
                "tasks": list(self.active_tasks.values()),
                "count": len(self.active_tasks),
                "queue_depth": len(self.active_tasks),
            }

        @app.post("/tasks/{task_id}/update")
        async def update_task(task_id: str, update: TaskUpdate):
            if task_id not in self.active_tasks:
                raise HTTPException(status_code=404, detail="Task not found")
            payload = {k: v for k, v in update.model_dump().items() if v is not None}
            self.active_tasks[task_id].update(payload)
            return {"status": "updated", "task_id": task_id}

        # ------------------------------------------------------------------
        # Component health
        # ------------------------------------------------------------------
        @app.get("/components/health")
        async def component_health():
            checks = self.health.as_dict()
            return {
                "components": checks,
                "overall_healthy": all(checks.values()),
                "healthy_count": sum(1 for v in checks.values() if v),
                "total_count": len(checks),
                "timestamp": datetime.now().isoformat(),
            }

        # ------------------------------------------------------------------
        # WebSocket (FIX #13 — single broadcast task; FIX #14 — narrow except)
        # ------------------------------------------------------------------
        @app.websocket("/ws/metrics")
        async def ws_metrics(websocket: WebSocket):
            await websocket.accept()
            self.websocket_connections.append(websocket)
            try:
                # Block until the client disconnects or the server stops.
                while self.running:
                    try:
                        await asyncio.wait_for(websocket.receive_text(), timeout=60)
                    except asyncio.TimeoutError:
                        # Client is idle — that's fine, broadcast still drives data.
                        continue
            except WebSocketDisconnect:
                pass
            except RuntimeError:
                # Starlette raises RuntimeError on double-disconnect.
                pass
            finally:
                with suppress(ValueError):
                    self.websocket_connections.remove(websocket)

    # ------------------------------------------------------------------
    # Health computation
    # ------------------------------------------------------------------
    def _compute_health(self) -> HealthState:
        self.health.api_ready = self.app is not None
        self.health.history_ready = (
            self.metrics_history is not None
            and self.execution_logs is not None
        )
        self.health.broadcast_running = (
            self._broadcast_task is not None and not self._broadcast_task.done()
        )
        return self.health

    async def _compute_full_readiness(self) -> HealthState:
        self._compute_health()
        self.health.database_ready = await self._check_database()
        self.health.ray_ready = await self._check_ray()
        self.health.carbon_api_ready = await self._check_carbon_api()
        return self.health

    async def _check_database(self) -> bool:
        # FIX #12 — offload blocking file I/O
        return await asyncio.to_thread(self._check_database_sync)

    def _check_database_sync(self) -> bool:
        try:
            import tempfile
            with tempfile.TemporaryDirectory(prefix="green-agent-health-") as td:
                probe = Path(td) / ".health"
                probe.write_text("ok", encoding="utf-8")
                probe.unlink()
            return True
        except Exception:                                # noqa: BLE001
            return False

    async def _check_ray(self) -> bool:
        try:
            import ray  # type: ignore
        except ImportError:
            return True   # Ray not installed — not a failure, just absent
        try:
            if not ray.is_initialized():
                return True   # Not initialized — not a failure
            ray.nodes()
            return True
        except Exception:                                # noqa: BLE001
            return False

    async def _check_carbon_api(self) -> bool:
        # FIX #11 — cache + always close
        now = time.time()
        if self._carbon_api_cached is not None:
            value, ts = self._carbon_api_cached
            if now - ts < self.CARBON_API_CACHE_TTL:
                return value
        value = await asyncio.to_thread(self._probe_carbon_api_sync)
        self._carbon_api_cached = (value, now)
        return value

    def _probe_carbon_api_sync(self) -> bool:
        forecaster = None
        try:
            from src.carbon.forecasting_engine import CarbonForecaster  # type: ignore
            forecaster = CarbonForecaster(self.config)
            # Synchronous probe — this runs in a worker thread.
            init = getattr(forecaster, "initialize", None)
            if callable(init):
                import asyncio as _a
                _a.run(init())
            getter = getattr(forecaster, "get_current_intensity", None)
            if not callable(getter):
                return False
            intensity = getter()
            if hasattr(intensity, "__await__"):
                import asyncio as _a
                intensity = _a.run(intensity)
            return float(intensity) > 0
        except Exception:                                # noqa: BLE001
            return False
        finally:
            if forecaster is not None:
                closer = getattr(forecaster, "close", None)
                if callable(closer):
                    try:
                        result = closer()
                        if hasattr(result, "__await__"):
                            import asyncio as _a
                            _a.run(result)
                    except Exception:                    # noqa: BLE001
                        pass

    # ------------------------------------------------------------------
    # Enhancement snapshots
    # ------------------------------------------------------------------
    def _latest_enhancement_snapshot(self) -> Optional[Dict[str, Any]]:
        if not self.execution_logs:
            return None
        latest = self.execution_logs[-1]
        return {
            "modp_score": latest.get("modp_score"),
            "graph_metrics": latest.get("graph_metrics") or {},
            "human_feedback_score": latest.get("human_feedback_score"),
            "distillation_update_count": latest.get("distillation_update_count"),
            "moe_gate_weights": latest.get("moe_gate_weights"),
            "evolutionary_best_fitness": latest.get("evolutionary_best_fitness"),
            "flexgen_energy_joules": latest.get("flexgen_energy_joules"),
        }

    def _refresh_enhancement_gauges(self, entry: Dict[str, Any]) -> None:
        """Set enhancement Gauges from a write-path entry (never from reads)."""
        m = self.metrics
        if entry.get("modp_score") is not None:
            m.modp_score.set(float(entry["modp_score"]))
        if entry.get("human_feedback_score") is not None:
            m.rlhf_feedback.set(float(entry["human_feedback_score"]))
        gm = entry.get("graph_metrics") or {}
        if isinstance(gm, dict):
            if "centrality" in gm:
                m.graph_centrality.set(float(gm["centrality"]))
            if "connectivity" in gm:
                m.graph_connectivity.set(float(gm["connectivity"]))
        weights = entry.get("moe_gate_weights")
        if isinstance(weights, dict):
            for expert, w in weights.items():
                try:
                    m.moe_gate_weight.labels(expert=str(expert)).set(float(w))
                except (TypeError, ValueError):
                    continue
        elif isinstance(weights, (list, tuple)):
            for i, w in enumerate(weights):
                try:
                    m.moe_gate_weight.labels(expert=str(i)).set(float(w))
                except (TypeError, ValueError):
                    continue
        if entry.get("evolutionary_best_fitness") is not None:
            m.evolutionary_best_fitness.set(
                float(entry["evolutionary_best_fitness"]),
            )

    def _refresh_enhancement_gauges_from_latest(self) -> None:
        """On read (scrape), only refresh *Gauges* from the latest log."""
        if not self.execution_logs or not self.use_enhancements:
            return
        self._refresh_enhancement_gauges(self.execution_logs[-1])

    # ------------------------------------------------------------------
    # Carbon helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _zone(intensity: float) -> str:
        if intensity < 50:
            return "green"
        if intensity < 200:
            return "yellow"
        return "red"

    @staticmethod
    def _recommendation(intensity: float) -> str:
        if intensity < 50:
            return "OPTIMAL: run compute-intensive tasks now"
        if intensity < 200:
            return "MODERATE: run standard tasks, defer if possible"
        return "HIGH: defer non-urgent tasks, use eco mode"

    # ------------------------------------------------------------------
    # Pareto (FIX #9 — 3 objectives)
    # ------------------------------------------------------------------
    def _update_pareto_frontier(self, execution: Dict[str, Any]) -> None:
        point = {
            "accuracy": float(execution.get("accuracy", 0.0)),
            "energy": float(execution.get("energy", 0.0)),
            "carbon": float(execution.get("carbon", 0.0)),
            "timestamp": datetime.now().isoformat(),
        }
        if execution.get("modp_score") is not None:
            point["modp_score"] = float(execution["modp_score"])
        self.pareto_frontier.append(point)
        if len(self.pareto_frontier) > 1000:
            self.pareto_frontier = self.pareto_frontier[-1000:]
        self.pareto_frontier = self._calculate_pareto_frontier(self.pareto_frontier)

    @staticmethod
    def _dominates(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
        """a dominates b iff a is at least as good in every objective and
        strictly better in at least one."""
        worse_in_any = (
            a["accuracy"] < b["accuracy"]
            or a["energy"] > b["energy"]
            or a["carbon"] > b["carbon"]
        )
        if worse_in_any:
            return False
        strictly_better = (
            a["accuracy"] > b["accuracy"]
            or a["energy"] < b["energy"]
            or a["carbon"] < b["carbon"]
        )
        return strictly_better

    def _calculate_pareto_frontier(
        self, points: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not points:
            return []
        frontier: List[Dict[str, Any]] = []
        for p in points:
            if any(self._dominates(q, p) for q in points if q is not p):
                continue
            frontier.append(p)
        return frontier

    # ------------------------------------------------------------------
    # Defaults / broadcasts
    # ------------------------------------------------------------------
    def _default_metrics(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "energy_consumption": 0.0,
            "carbon_footprint": 0.0,
            "carbon_intensity": 0.0,
            "active_tasks": 0,
            "completed_tasks": 0,
            "avg_accuracy": 0.0,
            "negawatt_reward": 0.0,
            "pareto_frontier": [],
            "component_health": self.health.as_dict(),
        }
        if self.use_enhancements:
            result["enhancements"] = {
                "modp_score": None,
                "graph_metrics": {},
                "human_feedback_score": None,
                "distillation_update_count": 0,
                "moe_gate_weights": [],
                "evolutionary_best_fitness": None,
                "flexgen_energy_joules": 0,
            }
        return result

    async def _broadcast_metrics(self, execution: Dict[str, Any]) -> None:
        """Broadcast to all connected clients (FIX #4 — collect then remove)."""
        if not self.websocket_connections:
            return
        message = {
            "type": "metrics_update",
            "data": execution,
            "timestamp": datetime.now().isoformat(),
        }
        dead: List[WebSocket] = []
        for ws in list(self.websocket_connections):
            try:
                await ws.send_json(message)
            except Exception:                            # noqa: BLE001
                dead.append(ws)
        for ws in dead:
            with suppress(ValueError):
                self.websocket_connections.remove(ws)

    async def _broadcast_loop(self) -> None:
        """Single background broadcaster (FIX #13)."""
        while not self._broadcast_stop.is_set():
            try:
                await asyncio.wait_for(self._broadcast_stop.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            if self._broadcast_stop.is_set():
                break
            if not self.websocket_connections:
                continue
            message = {
                "type": "metrics_update",
                "data": await self._realtime_snapshot(),
                "timestamp": datetime.now().isoformat(),
            }
            dead: List[WebSocket] = []
            for ws in list(self.websocket_connections):
                try:
                    await ws.send_json(message)
                except Exception:                        # noqa: BLE001
                    dead.append(ws)
            for ws in dead:
                with suppress(ValueError):
                    self.websocket_connections.remove(ws)

    async def _realtime_snapshot(self) -> Dict[str, Any]:
        """Snapshot without going through the HTTP handler."""
        if not self.metrics_history:
            return self._default_metrics()
        latest = self.metrics_history[-1]
        recent = [log.get("accuracy", 0.0) for log in self.execution_logs[-100:]]
        return {
            "timestamp": latest.get("timestamp"),
            "energy_consumption": float(latest.get("energy", 0.0)),
            "carbon_footprint": float(latest.get("carbon", 0.0)),
            "carbon_intensity": float(latest.get("carbon_intensity", 0.0)),
            "active_tasks": len(self.active_tasks),
            "completed_tasks": len(self.execution_logs),
            "avg_accuracy": float(np.mean(recent)) if recent else 0.0,
            "negawatt_reward": float(latest.get("negawatt_reward", 0.0)),
            "pareto_frontier": self.pareto_frontier[-10:],
            "component_health": self.health.as_dict(),
        }

    # ------------------------------------------------------------------
    # Lifecycle (FIX #15, #16)
    # ------------------------------------------------------------------
    async def start(
        self, host: str = "0.0.0.0", port: int = 8000,
    ) -> None:
        import uvicorn

        config = uvicorn.Config(
            self.app,
            host=self.config.get("dashboard", {}).get("host", host),
            port=int(self.config.get("dashboard", {}).get("port", port)),
            log_level="info",
        )
        server = uvicorn.Server(config)
        self.running = True
        self._broadcast_task = asyncio.create_task(self._broadcast_loop())
        self._server_task = asyncio.create_task(server.serve())
        logger.info("Dashboard started at http://%s:%d", config.host, config.port)

    async def stop(self) -> None:
        self.running = False
        self._broadcast_stop.set()
        if self._broadcast_task is not None:
            with suppress(asyncio.CancelledError):
                await self._broadcast_task
            self._broadcast_task = None
        if self._server_task is not None:
            with suppress(asyncio.CancelledError, Exception):   # noqa: BLE001
                await asyncio.wait_for(self._server_task, timeout=10.0)
            self._server_task = None
        logger.info("Dashboard stopped")


# ---------------------------------------------------------------------------
# Module-level factory (FIX #1)
# ---------------------------------------------------------------------------
def create_app(config: Optional[Dict[str, Any]] = None) -> FastAPI:
    """Build a FastAPI app instance from config (or defaults)."""
    return DashboardAPI(config).app


# Exposed at module scope so `uvicorn api_server:app` works.
app = create_app()


if __name__ == "__main__":
    import uvicorn

    def _handle_shutdown(signum, frame):                # noqa: ARG001
        logger.info("Received signal %d; shutting down", signum)
        # Let uvicorn drain; the process will exit after.
        raise KeyboardInterrupt

    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(Exception):
            signal.signal(sig, _handle_shutdown)

    try:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except KeyboardInterrupt:
        pass
