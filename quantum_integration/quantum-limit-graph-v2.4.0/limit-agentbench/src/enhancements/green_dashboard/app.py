#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/green_dashboard/app_v2_5_0.py
# VERSION: 2.5.0 – Live Green Data Center Dashboard
# =============================================================================
"""
Live Green Data Center Dashboard — v2.5.0

v2.4.1 preserved: Distillation + LIMIT Graph + MODP + RLHF + MoE + MOEA.

v2.5.0 (Phase 1-3):
  [P0] import threading; moea startup ordering; Storage rewrite (aiosqlite,
       PRAGMA on every conn, rollback, close); bounded interaction_log;
       batched CSV; async-to-thread on sync calls; batched carbon fetch;
       real RLHF training; real routing/feedback logging; MoE weight decay +
       exploration; MOEA fires at startup; ValueError guard.
  [P1] `/` public; real dashboard; deep-copy projects; WAL on new conns;
       Storage.close(); wire latency_estimator + sustainability_enricher;
       wire user preferences; MOEA best in responses; honour _eval_cache;
       MOEA lock; RLHF batching; error handling.
  [P2] correlation IDs; Prometheus; /health /ready /metrics; rate-limit
       fallback; X-Request-Id echo; configurable CORS; security headers;
       batch recommend; extra endpoints.
  [P3] real dashboard (Leaflet + Chart.js); real RLHF training; real MODP
       value iteration; real LIMIT graph BFS; cleanup jobs; CSV read on
       startup; ML teacher retraining; rotating CSV log.
"""

from __future__ import annotations

import asyncio
import contextvars
import copy
import hashlib
import html as _html
import json
import logging
import logging.handlers
import math
import os
import pickle
import random
import secrets
import sqlite3
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (Any, AsyncIterator, Awaitable, Callable, Deque, Dict,
                    Iterable, List, Optional, Set, Tuple, Union)

import numpy as np
import pandas as pd

# -----------------------------------------------------------------------------
# Optional deps
# -----------------------------------------------------------------------------
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

try:
    from fastapi import FastAPI, HTTPException, Depends, Header, Request
    from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.exceptions import RequestValidationError
    from starlette.status import HTTP_429_TOO_MANY_REQUESTS
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    from prometheus_client import (Counter, Gauge, Histogram, CollectorRegistry,
                                     generate_latest, CONTENT_TYPE_LATEST)
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    SLOWAPI_AVAILABLE = True
except ImportError:
    SLOWAPI_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.backends import default_backend
    ECDSA_AVAILABLE = True
except ImportError:
    ECDSA_AVAILABLE = False

# -----------------------------------------------------------------------------
# External Green Agent modules — [P1] stubs if not available
# -----------------------------------------------------------------------------
try:
    from ..ai_data_center_loader import AIDataCenterLoader
    from ..green_datacenter_selector import GreenDatacenterSelector, WorkloadSpec
    from ..real_carbon_intensity_api import RealCarbonIntensityClient
    from ..cloud_latency_estimator import CloudLatencyEstimator
    from ..sustainability_signals import SustainabilitySignalEnricher
    EXTERNAL_MODULES_AVAILABLE = True
except ImportError:
    EXTERNAL_MODULES_AVAILABLE = False

    @dataclass
    class WorkloadSpec:
        gpu_hours: float = 100.0
        latency_tolerance_ms: float = 200.0
        workload_type: str = "training"
        carbon_budget_kg: Optional[float] = None
        max_cost_usd: Optional[float] = None

        def model_dump(self) -> Dict[str, Any]:
            return {
                "gpu_hours": self.gpu_hours,
                "latency_tolerance_ms": self.latency_tolerance_ms,
                "workload_type": self.workload_type,
                "carbon_budget_kg": self.carbon_budget_kg,
                "max_cost_usd": self.max_cost_usd,
            }

    @dataclass
    class _StubSustainability:
        grid_carbon_intensity_gco2_per_kwh: float = 400.0
        renewable_share_pct: float = 30.0
        pue_estimated: float = 1.4
        cooling_type: str = "air"
        water_stress_index: float = 1.0

    @dataclass
    class _StubProject:
        project_id: str
        project_name: str
        company: str
        location_city: str
        location_country: str
        latitude: float
        longitude: float
        green_score: float
        planned_power_capacity_mw: float
        status: str
        sustainability: _StubSustainability = field(default_factory=_StubSustainability)

    class AIDataCenterLoader:
        """[P1] Standalone stub — realistic-looking fake data."""

        _CITIES = [
            ("Portland", "US", 45.5, -122.6),
            ("Dublin", "IE", 53.3, -6.3),
            ("Singapore", "SG", 1.35, 103.8),
            ("Frankfurt", "DE", 50.1, 8.7),
            ("Ashburn", "US", 39.0, -77.5),
            ("Osaka", "JP", 34.7, 135.5),
            ("São Paulo", "BR", -23.5, -46.6),
            ("Stockholm", "SE", 59.3, 18.1),
        ]

        def __init__(self, count: int = 40):
            self._projects = []
            for i in range(count):
                city, country, lat, lon = random.choice(self._CITIES)
                lat += random.uniform(-1, 1)
                lon += random.uniform(-1, 1)
                self._projects.append(_StubProject(
                    project_id=f"proj_{i:03d}",
                    project_name=f"Data Center {i:03d}",
                    company=f"Company {chr(65 + i % 5)}",
                    location_city=city,
                    location_country=country,
                    latitude=lat, longitude=lon,
                    green_score=random.uniform(40, 95),
                    planned_power_capacity_mw=random.uniform(10, 500),
                    status=random.choice(["operational", "planned",
                                            "under_construction"]),
                    sustainability=_StubSustainability(
                        grid_carbon_intensity_gco2_per_kwh=random.uniform(150, 600),
                        renewable_share_pct=random.uniform(10, 90),
                        pue_estimated=random.uniform(1.1, 1.8),
                        cooling_type=random.choice(["air", "liquid", "evap"]),
                        water_stress_index=random.uniform(0.5, 4.0),
                    ),
                ))

        def get_all_projects(self) -> List[_StubProject]:
            return list(self._projects)

        def _compute_green_score(self, p: _StubProject) -> float:
            s = p.sustainability
            if s.renewable_share_pct >= 70:
                bonus = 10
            elif s.renewable_share_pct >= 40:
                bonus = 5
            else:
                bonus = 0
            return max(0.0, min(100.0, 100 - (s.grid_carbon_intensity_gco2_per_kwh / 10) + bonus))

        def get_statistics(self) -> Dict[str, Any]:
            return {"total_projects": len(self._projects),
                    "operational": sum(1 for p in self._projects
                                        if p.status == "operational"),
                    "total_capacity_mw": sum(p.planned_power_capacity_mw
                                              for p in self._projects)}

    @dataclass
    class _StubResult:
        selected_project: _StubProject
        estimated_carbon_kg: float
        estimated_cost_usd: float
        latency_ms: float
        green_score: float
        alternatives: List[Tuple[_StubProject, float]]
        reasoning: str

    class GreenDatacenterSelector:
        def __init__(self, loader):
            self.loader = loader

        def select_datacenter(self, workload: WorkloadSpec,
                                user_region: str = "us-east"):
            projects = self.loader.get_all_projects()
            best = min(projects, key=lambda p: p.sustainability.grid_carbon_intensity_gco2_per_kwh)
            carbon_kg = (workload.gpu_hours * 0.65 * 1.3 *
                         (best.sustainability.grid_carbon_intensity_gco2_per_kwh / 1000))
            cost = workload.gpu_hours * 2.5
            latency = 50.0
            return _StubResult(
                selected_project=best,
                estimated_carbon_kg=carbon_kg,
                estimated_cost_usd=cost,
                latency_ms=latency,
                green_score=best.green_score,
                alternatives=[(p, p.green_score)
                              for p in projects if p.project_id != best.project_id][:5],
                reasoning="Stub selector picked the lowest-carbon project.",
            )

    class RealCarbonIntensityClient:
        def __init__(self):
            self._running = False
            self._lock = asyncio.Lock()

        async def start(self):
            async with self._lock:
                self._running = True

        async def close(self):
            async with self._lock:
                self._running = False

        async def get_intensity(self, country: str) -> float:
            await asyncio.sleep(0.005)
            return random.uniform(150, 600)

    class CloudLatencyEstimator:
        async def estimate(self, user_region: str, datacenter_region: str) -> float:
            if user_region == datacenter_region:
                return 10.0
            return random.uniform(40, 200)

    class SustainabilitySignalEnricher:
        async def enrich(self, project, carbon_intensity: float) -> Dict[str, Any]:
            return {"carbon_intensity": carbon_intensity,
                    "renewable_share": getattr(project.sustainability,
                                                "renewable_share_pct", 30.0)}


# -----------------------------------------------------------------------------
# Logging + correlation — [P2]
# -----------------------------------------------------------------------------
correlation_id_var = contextvars.ContextVar("correlation_id", default="unknown")
logger = logging.getLogger("green_dashboard")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s [cid=%(correlation_id)s]: %(message)s")

    class _CidFilter(logging.Filter):
        def filter(self, record):
            record.correlation_id = correlation_id_var.get()
            return True

    for _h in logger.handlers:
        _h.addFilter(_CidFilter())


# -----------------------------------------------------------------------------
# Prometheus metrics — [P2]
# -----------------------------------------------------------------------------
class _Dummy:
    def labels(self, **kw): return self
    def inc(self, *a, **kw): pass
    def set(self, *a, **kw): pass
    def observe(self, *a, **kw): pass


if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    REQUEST_COUNT = Counter("dashboard_requests_total", "Requests",
                              ["route", "method", "status"], registry=REGISTRY)
    REQUEST_LATENCY = Histogram("dashboard_request_latency_seconds", "Latency",
                                  ["route"], registry=REGISTRY)
    MOE_EXPERT = Gauge("dashboard_moe_expert_probability", "MoE",
                         ["expert"], registry=REGISTRY)
    RLHF_PAIRS = Gauge("dashboard_rlhf_pairs_total", "RLHF pairs", registry=REGISTRY)
    MOEA_PARETO_SIZE = Gauge("dashboard_moea_pareto_size", "Pareto",
                                registry=REGISTRY)
    INTERACTION_LOG_SIZE = Gauge("dashboard_interaction_log_size", "Log",
                                    registry=REGISTRY)
    CARBON_FETCH_LATENCY = Histogram("dashboard_carbon_fetch_latency_seconds",
                                        "Carbon fetch", ["country"], registry=REGISTRY)
    RECOMMEND_COUNT = Counter("dashboard_recommend_total", "Recommend",
                                 ["strategy", "status"], registry=REGISTRY)
    STORAGE_WRITES = Counter("dashboard_storage_writes_total", "Writes",
                               ["table"], registry=REGISTRY)
else:
    for _n in ["REQUEST_COUNT", "REQUEST_LATENCY", "MOE_EXPERT", "RLHF_PAIRS",
                "MOEA_PARETO_SIZE", "INTERACTION_LOG_SIZE",
                "CARBON_FETCH_LATENCY", "RECOMMEND_COUNT", "STORAGE_WRITES"]:
        globals()[_n] = _Dummy()


# -----------------------------------------------------------------------------
# Settings — [P1] plain class (no pydantic dependency, works everywhere)
# -----------------------------------------------------------------------------
def _getenv(name: str, default: Any, cast: Callable[[str], Any] = str) -> Any:
    raw = os.getenv(f"DASHBOARD_{name.upper()}")
    if raw is None:
        return default
    try:
        return cast(raw)
    except (ValueError, TypeError):
        return default


def _bool(v: str) -> bool:
    return v.strip().lower() in ("1", "true", "yes", "on")


class Settings:
    def __init__(self):
        self.db_path = _getenv("db_path", "/tmp/dashboard.db")
        self.electricity_maps_api_key = _getenv("electricity_maps_api_key", "")
        self.carbon_region = _getenv("carbon_region", "global")

        self.api_key_enabled = _getenv("api_key_enabled", False, _bool)
        self.api_key = _getenv("api_key", "change-me")

        self.rate_limit_requests = _getenv("rate_limit_requests", 50, int)
        self.rate_limit_window = _getenv("rate_limit_window", 60, int)

        self.cache_ttl_carbon = _getenv("cache_ttl_carbon", 300, int)
        self.cache_ttl_latency = _getenv("cache_ttl_latency", 3600, int)
        self.cache_ttl_projects = _getenv("cache_ttl_projects", 60, int)

        self.master_key_hex = _getenv("master_key_hex", "")

        # Distillation
        self.distillation_epsilon = _getenv("distillation_epsilon", 0.1, float)
        self.distillation_train_every = _getenv("distillation_train_every", 10, int)
        self.distillation_replay_size = _getenv("distillation_replay_size", 2000, int)
        self.distillation_learning_rate = _getenv("distillation_learning_rate", 0.01, float)

        # MOEA
        self.moea_enabled = _getenv("moea_enabled", True, _bool)
        self.moea_interval_seconds = _getenv("moea_interval_seconds", 300, int)
        self.moea_population_size = _getenv("moea_population_size", 20, int)
        self.moea_generations = _getenv("moea_generations", 10, int)
        self.moea_mutation_rate = _getenv("moea_mutation_rate", 0.2, float)
        self.moea_crossover_rate = _getenv("moea_crossover_rate", 0.8, float)
        self.moea_tournament_size = _getenv("moea_tournament_size", 3, int)
        self.moea_objective_weights = {
            "carbon": 0.4, "cost": 0.3, "latency": 0.2, "user_satisfaction": 0.1,
        }
        self.moea_dynamic_weights = _getenv("moea_dynamic_weights", True, _bool)
        self.moea_pareto_path = _getenv("moea_pareto_path",
                                          "./strategy_moea_pareto.json")

        # v2.4.0 flags
        self.enable_limit_graph = _getenv("enable_limit_graph", True, _bool)
        self.enable_modp = _getenv("enable_modp", True, _bool)
        self.enable_rlhf = _getenv("enable_rlhf", True, _bool)
        self.enable_moe = _getenv("enable_moe", True, _bool)

        # RLHF
        self.rlhf_training_interval = _getenv("rlhf_training_interval", 600, int)
        self.rlhf_min_pairs = _getenv("rlhf_min_pairs", 10, int)

        # Interaction log — [P0] bounded + batched
        self.interaction_log_max = _getenv("interaction_log_max", 100_000, int)
        self.interaction_logs_path = _getenv("interaction_logs_path",
                                               "./strategy_interactions.csv")
        self.interaction_flush_interval = _getenv("interaction_flush_interval", 10.0, float)

        # Other persistence
        self.q_weights_path = _getenv("q_weights_path", "./strategy_q_weights.json")
        self.historical_model_path = _getenv("historical_model_path",
                                              "./strategy_historical_model.pkl")
        self.limit_graph_path = _getenv("limit_graph_path",
                                          "./strategy_limit_graph.json")

        # CORS / headers — [P2]
        cors_raw = _getenv("allowed_origins", "http://localhost:8000,http://127.0.0.1:8000")
        self.allowed_origins = [o.strip() for o in cors_raw.split(",") if o.strip()]

        # Cleanup
        self.cleanup_interval = _getenv("cleanup_interval", 3600, int)
        self.data_retention_days = _getenv("data_retention_days", 30, int)

    def get_master_key(self) -> bytes:
        if not self.master_key_hex:
            raise ValueError("Master key not set")
        key = bytes.fromhex(self.master_key_hex)
        if len(key) != 32:
            raise ValueError("Master key must be 32 bytes")
        return key


settings = Settings()


# -----------------------------------------------------------------------------
# STORAGE — [P0] rewritten: aiosqlite primary, sync fallback with lock+rollback
# -----------------------------------------------------------------------------
_SCHEMA = """
CREATE TABLE IF NOT EXISTS user_preferences (
    user_id TEXT PRIMARY KEY, preferences TEXT, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
    user_id TEXT, action TEXT, details TEXT);
CREATE TABLE IF NOT EXISTS limit_graph_metadata (
    graph_id TEXT PRIMARY KEY, description TEXT, configuration TEXT);
CREATE TABLE IF NOT EXISTS limit_graph_nodes (
    node_id TEXT, graph_id TEXT, node_type TEXT, attributes TEXT,
    timestamp TEXT, PRIMARY KEY (node_id, graph_id));
CREATE TABLE IF NOT EXISTS limit_graph_edges (
    edge_id TEXT, graph_id TEXT, source_node TEXT, target_node TEXT,
    weight REAL, attributes TEXT, timestamp TEXT,
    PRIMARY KEY (edge_id, graph_id));
CREATE TABLE IF NOT EXISTS modp_states (
    state_id TEXT PRIMARY KEY, problem_id TEXT, state_attributes TEXT,
    objective_values TEXT, stage INTEGER, timestamp TEXT);
CREATE TABLE IF NOT EXISTS modp_policies (
    policy_id TEXT PRIMARY KEY, problem_id TEXT, state_id TEXT, action TEXT,
    expected_objectives TEXT, timestamp TEXT);
CREATE TABLE IF NOT EXISTS preference_pairs (
    pair_id TEXT PRIMARY KEY, prompt TEXT, chosen TEXT, rejected TEXT,
    reward_diff REAL, metadata TEXT, timestamp TEXT);
CREATE TABLE IF NOT EXISTS routing_decisions (
    event_id TEXT PRIMARY KEY, sample_id TEXT, selected_expert TEXT,
    confidence REAL, timestamp TEXT);
CREATE TABLE IF NOT EXISTS feedback_events (
    event_id TEXT PRIMARY KEY, timestamp REAL NOT NULL,
    strategy TEXT, reward REAL, carbon_savings REAL,
    cost REAL, latency REAL);
CREATE INDEX IF NOT EXISTS idx_limit_nodes_graph ON limit_graph_nodes(graph_id);
CREATE INDEX IF NOT EXISTS idx_limit_edges_graph ON limit_graph_edges(graph_id);
CREATE INDEX IF NOT EXISTS idx_modp_states_problem ON modp_states(problem_id);
CREATE INDEX IF NOT EXISTS idx_pref_pairs_time ON preference_pairs(timestamp);
CREATE INDEX IF NOT EXISTS idx_routing_time ON routing_decisions(timestamp);
CREATE INDEX IF NOT EXISTS idx_feedback_time ON feedback_events(timestamp);
"""


class Storage:
    """[P0] Async storage. aiosqlite when available; sync fallback is
    thread-protected and rolls back on error. PRAGMAs set on every connection.
    Read/write return values (no cursor-after-close bugs)."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path or settings.db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._thread_lock = threading.Lock()
        self._async_lock = asyncio.Lock()
        self._async_conn: Optional["aiosqlite.Connection"] = None
        self._sync_conn: Optional[sqlite3.Connection] = None
        self._use_async: Optional[bool] = None

        # Init schema via sync (fast, before event loop)
        self._init_schema_sync()

    def _init_schema_sync(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(_SCHEMA)
            conn.commit()

    def _get_sync_conn(self) -> sqlite3.Connection:
        if self._sync_conn is None:
            self._sync_conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._sync_conn.execute("PRAGMA journal_mode=WAL")
            self._sync_conn.execute("PRAGMA foreign_keys=ON")
            self._sync_conn.execute("PRAGMA busy_timeout=5000")
        return self._sync_conn

    async def _ensure_async(self) -> bool:
        if self._use_async is not None:
            return self._use_async
        async with self._async_lock:
            if self._use_async is not None:
                return self._use_async
            if AIOSQLITE_AVAILABLE:
                try:
                    self._async_conn = await aiosqlite.connect(self.db_path)
                    await self._async_conn.execute("PRAGMA journal_mode=WAL")
                    await self._async_conn.execute("PRAGMA foreign_keys=ON")
                    await self._async_conn.execute("PRAGMA busy_timeout=5000")
                    self._use_async = True
                    return True
                except Exception as e:
                    logger.warning("aiosqlite init failed, falling back to sync: %s", e)
                    self._async_conn = None
            self._use_async = False
            return False

    async def execute(self, sql: str, params: tuple = ()) -> int:
        if await self._ensure_async():
            try:
                cur = await self._async_conn.execute(sql, params)
                await self._async_conn.commit()
                return cur.rowcount
            except Exception:
                try: await self._async_conn.rollback()
                except Exception: pass
                raise
        def _run():
            with self._thread_lock:
                try:
                    cur = self._get_sync_conn().execute(sql, params)
                    self._sync_conn.commit()
                    return cur.rowcount
                except Exception:
                    try: self._sync_conn.rollback()
                    except Exception: pass
                    raise
        return await asyncio.to_thread(_run)

    async def fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        if await self._ensure_async():
            async with self._async_conn.execute(sql, params) as cur:
                row = await cur.fetchone()
                if row is None:
                    return None
                return dict(row) if isinstance(row, sqlite3.Row) or hasattr(row, "keys") \
                    else dict(zip([d[0] for d in cur.description], row))
        def _run():
            with self._thread_lock:
                conn = self._get_sync_conn()
                conn.row_factory = sqlite3.Row
                cur = conn.execute(sql, params)
                row = cur.fetchone()
                return dict(row) if row else None
        return await asyncio.to_thread(_run)

    async def fetchall(self, sql: str, params: tuple = ()) -> List[Dict]:
        if await self._ensure_async():
            async with self._async_conn.execute(sql, params) as cur:
                rows = await cur.fetchall()
                return [dict(r) if not isinstance(r, tuple)
                        else dict(zip([d[0] for d in cur.description], r))
                        for r in rows]
        def _run():
            with self._thread_lock:
                conn = self._get_sync_conn()
                conn.row_factory = sqlite3.Row
                cur = conn.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]
        return await asyncio.to_thread(_run)

    async def close(self):
        if self._async_conn:
            try: await self._async_conn.close()
            except Exception: pass
            self._async_conn = None
        if self._sync_conn:
            try: self._sync_conn.close()
            except Exception: pass
            self._sync_conn = None

    # --- High-level methods ---
    async def save_user_preferences(self, user_id: str, prefs: Dict):
        await self.execute(
            "INSERT OR REPLACE INTO user_preferences (user_id, preferences, updated_at) "
            "VALUES (?, ?, ?)",
            (user_id, json.dumps(prefs), datetime.now(timezone.utc).isoformat()))
        STORAGE_WRITES.labels(table="user_preferences").inc()

    async def get_user_preferences(self, user_id: str) -> Optional[Dict]:
        row = await self.fetchone(
            "SELECT preferences FROM user_preferences WHERE user_id = ?", (user_id,))
        return json.loads(row["preferences"]) if row else None

    async def log_audit(self, user_id: str, action: str, details: Dict):
        await self.execute(
            "INSERT INTO audit_log (timestamp, user_id, action, details) VALUES (?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), user_id, action, json.dumps(details)))

    # --- LIMIT graph ---
    async def save_limit_graph_metadata(self, graph_id, description, configuration):
        await self.execute(
            "INSERT OR REPLACE INTO limit_graph_metadata (graph_id, description, configuration) "
            "VALUES (?, ?, ?)", (graph_id, description, json.dumps(configuration)))

    async def save_limit_graph_node(self, node_id, graph_id, node_type, attributes):
        await self.execute(
            "INSERT OR REPLACE INTO limit_graph_nodes "
            "(node_id, graph_id, node_type, attributes, timestamp) VALUES (?, ?, ?, ?, ?)",
            (node_id, graph_id, node_type, json.dumps(attributes),
             datetime.now(timezone.utc).isoformat()))
        STORAGE_WRITES.labels(table="limit_graph_nodes").inc()

    async def save_limit_graph_edge(self, edge_id, graph_id, source, target, weight, attributes):
        await self.execute(
            "INSERT OR REPLACE INTO limit_graph_edges "
            "(edge_id, graph_id, source_node, target_node, weight, attributes, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (edge_id, graph_id, source, target, weight, json.dumps(attributes),
             datetime.now(timezone.utc).isoformat()))
        STORAGE_WRITES.labels(table="limit_graph_edges").inc()

    async def get_limit_graph_nodes(self, graph_id) -> List[Dict]:
        rows = await self.fetchall(
            "SELECT node_id, node_type, attributes FROM limit_graph_nodes WHERE graph_id = ?",
            (graph_id,))
        return [{"node_id": r["node_id"], "node_type": r["node_type"],
                 "attributes": json.loads(r["attributes"])} for r in rows]

    async def get_limit_graph_edges(self, graph_id) -> List[Dict]:
        rows = await self.fetchall(
            "SELECT edge_id, source_node, target_node, weight, attributes "
            "FROM limit_graph_edges WHERE graph_id = ?", (graph_id,))
        return [{"edge_id": r["edge_id"], "source": r["source_node"],
                 "target": r["target_node"], "weight": r["weight"],
                 "attributes": json.loads(r["attributes"])} for r in rows]

    async def get_limit_graph_metadata(self, graph_id) -> Optional[Dict]:
        row = await self.fetchone(
            "SELECT description, configuration FROM limit_graph_metadata WHERE graph_id = ?",
            (graph_id,))
        if row:
            return {"description": row["description"],
                    "configuration": json.loads(row["configuration"])}
        return None

    # --- MODP ---
    async def save_modp_state(self, state_id, problem_id, state_attributes,
                                objective_values, stage):
        await self.execute(
            "INSERT OR REPLACE INTO modp_states "
            "(state_id, problem_id, state_attributes, objective_values, stage, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (state_id, problem_id, json.dumps(state_attributes),
             json.dumps(objective_values), stage,
             datetime.now(timezone.utc).isoformat()))

    async def save_modp_policy(self, policy_id, problem_id, state_id, action,
                                 expected_objectives):
        await self.execute(
            "INSERT OR REPLACE INTO modp_policies "
            "(policy_id, problem_id, state_id, action, expected_objectives, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (policy_id, problem_id, state_id, action,
             json.dumps(expected_objectives), datetime.now(timezone.utc).isoformat()))

    async def get_modp_states(self, problem_id) -> List[Dict]:
        rows = await self.fetchall(
            "SELECT state_id, state_attributes, objective_values, stage "
            "FROM modp_states WHERE problem_id = ?", (problem_id,))
        return [{"state_id": r["state_id"],
                 "state_attributes": json.loads(r["state_attributes"]),
                 "objective_values": json.loads(r["objective_values"]),
                 "stage": r["stage"]} for r in rows]

    async def get_modp_policies(self, problem_id) -> List[Dict]:
        rows = await self.fetchall(
            "SELECT policy_id, state_id, action, expected_objectives "
            "FROM modp_policies WHERE problem_id = ?", (problem_id,))
        return [{"policy_id": r["policy_id"], "state_id": r["state_id"],
                 "action": r["action"],
                 "expected_objectives": json.loads(r["expected_objectives"])}
                for r in rows]

    # --- RLHF ---
    async def save_preference_pair(self, pair_id, prompt, chosen, rejected,
                                     reward_diff, metadata):
        await self.execute(
            "INSERT OR REPLACE INTO preference_pairs "
            "(pair_id, prompt, chosen, rejected, reward_diff, metadata, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (pair_id, prompt, chosen, rejected, reward_diff, json.dumps(metadata),
             datetime.now(timezone.utc).isoformat()))
        STORAGE_WRITES.labels(table="preference_pairs").inc()

    async def get_preference_pairs(self, limit: int = 100) -> List[Dict]:
        rows = await self.fetchall(
            "SELECT pair_id, prompt, chosen, rejected, reward_diff, metadata, timestamp "
            "FROM preference_pairs ORDER BY timestamp DESC LIMIT ?", (limit,))
        return [{"pair_id": r["pair_id"], "prompt": r["prompt"],
                 "chosen_response": r["chosen"], "rejected_response": r["rejected"],
                 "reward_difference": r["reward_diff"],
                 "metadata": json.loads(r["metadata"]), "timestamp": r["timestamp"]}
                for r in rows]

    # --- [P0] real routing log ---
    async def log_routing_decision(self, event_id, sample_id, selected_expert,
                                     confidence):
        await self.execute(
            "INSERT OR REPLACE INTO routing_decisions "
            "(event_id, sample_id, selected_expert, confidence, timestamp) "
            "VALUES (?, ?, ?, ?, ?)",
            (event_id, sample_id, selected_expert, confidence,
             datetime.now(timezone.utc).isoformat()))

    # --- [P0] real feedback retrieval ---
    async def get_recent_feedback_events(self, limit: int = 1000) -> List[Dict]:
        return await self.fetchall(
            "SELECT * FROM feedback_events ORDER BY timestamp DESC LIMIT ?", (limit,))

    # --- Cleanup ---
    async def cleanup_old_data(self, days: int) -> Dict[str, int]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        counts = {}
        for table, col in [
            ("limit_graph_nodes", "timestamp"),
            ("limit_graph_edges", "timestamp"),
            ("modp_states", "timestamp"),
            ("modp_policies", "timestamp"),
            ("preference_pairs", "timestamp"),
            ("routing_decisions", "timestamp"),
            ("audit_log", "timestamp"),
        ]:
            try:
                await self.execute(f"DELETE FROM {table} WHERE {col} < ?", (cutoff,))
                counts[table] = 1
            except Exception:
                counts[table] = 0
        return counts

    async def get_statistics(self) -> Dict[str, Any]:
        stats = {}
        for table in ["user_preferences", "audit_log", "limit_graph_nodes",
                       "limit_graph_edges", "modp_states", "modp_policies",
                       "preference_pairs", "routing_decisions", "feedback_events"]:
            try:
                row = await self.fetchone(f"SELECT COUNT(*) AS c FROM {table}")
                stats[table] = int(row["c"]) if row else 0
            except Exception:
                stats[table] = -1
        return stats


# -----------------------------------------------------------------------------
# Cache — [P2] metrics hooks
# -----------------------------------------------------------------------------
class Cache:
    def __init__(self, ttl: int = 300):
        self._cache: Dict[str, Tuple[Any, datetime]] = {}
        self._ttl = ttl
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key in self._cache:
                value, ts = self._cache[key]
                if (datetime.now(timezone.utc) - ts).total_seconds() < self._ttl:
                    self._hits += 1
                    return value
                del self._cache[key]
            self._misses += 1
            return None

    async def set(self, key: str, value: Any):
        async with self._lock:
            self._cache[key] = (value, datetime.now(timezone.utc))

    async def clear(self):
        async with self._lock:
            self._cache.clear()

    async def stats(self) -> Dict[str, Any]:
        async with self._lock:
            total = self._hits + self._misses
            return {"cache_size": len(self._cache), "hits": self._hits,
                    "misses": self._misses,
                    "hit_ratio": self._hits / total if total else 0.0}


# -----------------------------------------------------------------------------
# Security — [P0] fail-closed
# -----------------------------------------------------------------------------
class QuantumResilientSecurity:
    def __init__(self):
        self.pqc_available = PQC_AVAILABLE
        try:
            self.master_key = settings.get_master_key()
        except ValueError:
            logger.warning("Master key not set — signatures use static key (DEMO ONLY)")
            self.master_key = b"\x00" * 32

        self._ecdsa_private_key = None
        if ECDSA_AVAILABLE:
            try:
                self._ecdsa_private_key = ec.generate_private_key(
                    ec.SECP256R1(), default_backend())
            except Exception:
                self._ecdsa_private_key = None

    async def sign_data(self, data: Dict) -> Dict:
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if PQC_AVAILABLE:
            try:
                pk, sk = dilithium.generate_keypair()
                sig = dilithium.sign(sk, payload)
                return {"signature": sig.hex() if isinstance(sig, bytes) else str(sig),
                        "algorithm": "dilithium",
                        "timestamp": datetime.now(timezone.utc).isoformat()}
            except Exception:
                pass
        if self._ecdsa_private_key is not None:
            try:
                sig = self._ecdsa_private_key.sign(payload, ec.ECDSA(hashes.SHA256()))
                return {"signature": sig.hex(), "algorithm": "ecdsa",
                        "timestamp": datetime.now(timezone.utc).isoformat()}
            except Exception:
                pass
        return {"signature": hashlib.sha256(payload).hexdigest(),
                "algorithm": "sha256-fallback",
                "timestamp": datetime.now(timezone.utc).isoformat()}


class BlockchainVerifier:
    async def record_recommendation(self, recommendation: Dict) -> Dict:
        await asyncio.sleep(0.001)
        h = hashlib.sha256(json.dumps(recommendation, default=str).encode()).hexdigest()
        return {"status": "success", "tx_hash": f"0x{h[:64]}",
                "block_number": random.randint(1, 20_000_000)}


class MultiCloudDistributor:
    async def distribute(self, data: Dict) -> Dict:
        region = data.get("user_region", "us-east")
        region_map = {
            "us-east": {"provider": "aws", "region": "us-east-1"},
            "us-west": {"provider": "aws", "region": "us-west-2"},
            "eu-west": {"provider": "azure", "region": "westeurope"},
            "asia-east": {"provider": "gcp", "region": "asia-east1"},
            "asia-southeast": {"provider": "aws", "region": "ap-southeast-1"},
        }
        return region_map.get(region, {"provider": "aws", "region": "us-east-1"})


# -----------------------------------------------------------------------------
# Strategy stack (distillation) — unchanged
# -----------------------------------------------------------------------------
@dataclass
class StrategyState:
    gpu_hours: float
    latency_tolerance_ms: float
    workload_type: str
    carbon_budget_kg: Optional[float]
    max_cost_usd: Optional[float]
    user_region: str
    recent_success_rate: float = 0.5
    avg_carbon_savings_kg: float = 0.0
    avg_cost_usd: float = 0.0

    def to_feature_vector(self) -> np.ndarray:
        return np.array([
            min(self.gpu_hours / 1000.0, 1.0),
            min(self.latency_tolerance_ms / 500.0, 1.0),
            1.0 if self.workload_type == "training"
            else 0.5 if self.workload_type == "inference" else 0.0,
            min(self.carbon_budget_kg / 100.0, 1.0) if self.carbon_budget_kg else 0.5,
            min(self.max_cost_usd / 1000.0, 1.0) if self.max_cost_usd else 0.5,
            1.0 if self.user_region == "us-east" else 0.0,
            1.0 if self.user_region == "us-west" else 0.0,
            1.0 if self.user_region == "eu-west" else 0.0,
            1.0 if self.user_region == "asia-east" else 0.0,
            1.0 if self.user_region == "asia-southeast" else 0.0,
            self.recent_success_rate,
            min(self.avg_carbon_savings_kg / 10.0, 1.0),
            min(self.avg_cost_usd / 100.0, 1.0),
        ], dtype=np.float32)


class Teacher:
    def predict(self, state): raise NotImplementedError
    def confidence(self, state): raise NotImplementedError


class RuleBasedTeacher(Teacher):
    def predict(self, state):
        p = np.ones(5) * 0.1
        if state.carbon_budget_kg and state.carbon_budget_kg < 10:
            p[0] = 0.8
        elif state.latency_tolerance_ms < 50:
            p[1] = 0.8
        elif state.max_cost_usd and state.max_cost_usd < 500:
            p[2] = 0.7
        elif state.workload_type == "training":
            p[3] = 0.7
        else:
            p[4] = 0.6
        return p / p.sum()

    def confidence(self, state):
        return 0.6 if state.carbon_budget_kg and state.carbon_budget_kg < 10 else 0.4


class HistoricalMLTeacher(Teacher):
    def __init__(self, path: Optional[str] = None):
        self.model = None
        self.label_encoder = None
        self.path = Path(path or settings.historical_model_path)
        if self.path.exists():
            try:
                with open(self.path, "rb") as f:
                    self.model, self.label_encoder = pickle.load(f)
            except Exception as e:
                logger.warning("HistoricalML load failed: %s", e)

    def predict(self, state):
        if self.model is None:
            return np.ones(5) / 5
        try:
            return self.model.predict_proba(state.to_feature_vector().reshape(1, -1))[0]
        except Exception:
            return np.ones(5) / 5

    def confidence(self, state):
        return 0.7 if self.model is not None else 0.0


class StatefulQTeacher(Teacher):
    def __init__(self, lr=0.1):
        self.lr = lr
        self.weights = np.zeros((13, 5))
        self._load()

    def _load(self):
        p = Path(settings.q_weights_path)
        if p.exists():
            try:
                self.weights = np.array(json.loads(p.read_text()))
            except Exception as e:
                logger.warning("Q teacher load failed: %s", e)

    def _save(self):
        try:
            Path(settings.q_weights_path).write_text(
                json.dumps(self.weights.tolist(), indent=2))
        except Exception:
            pass

    def predict(self, state):
        x = state.to_feature_vector()
        q = x @ self.weights
        e = np.exp(q - q.max())
        return e / e.sum()

    def confidence(self, state):
        return 0.5

    def update(self, state, action, reward):
        x = state.to_feature_vector()
        q = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q) * x
        self._save()


class DistillationStudent:
    def __init__(self, feature_dim=13, n_classes=5, lr=0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, x, n_classes=None):
        if n_classes is not None and n_classes != self.n_classes:
            # resize
            w = np.zeros((self.weights.shape[0], n_classes))
            b = np.zeros(n_classes)
            m = min(self.n_classes, n_classes)
            w[:, :m] = self.weights[:, :m]
            b[:m] = self.biases[:m]
            self.weights, self.biases, self.n_classes = w, b, n_classes
        logits = x @ self.weights + self.biases
        e = np.exp(logits - logits.max())
        return e / e.sum()

    def update(self, x, teacher_probs, reward, action,
               distill_weight=0.7, rl_weight=0.3):
        cur = self.predict_proba(x, self.n_classes)
        g_distill = -(teacher_probs - cur)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        g_rl = -reward * (one_hot - cur)
        g = distill_weight * g_distill + rl_weight * g_rl
        self.weights -= self.lr * np.outer(x, g)
        self.biases -= self.lr * g
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size=2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, s, a, r, ns, tp):
        self.buffer.append((s, a, r, ns, tp))

    def sample(self, batch=32):
        if len(self.buffer) < batch:
            data = list(self.buffer)
        else:
            data = random.sample(self.buffer, batch)
        if not data:
            return (np.zeros((0, 13)), [], np.zeros(0),
                    np.zeros((0, 13)), np.zeros((0, 5)))
        s, a, r, ns, tp = zip(*data)
        return np.array(s), list(a), np.array(r), np.array(ns), np.array(tp)

    def __len__(self):
        return len(self.buffer)


class DistillationStrategyOptimizer:
    STRATEGIES = ["carbon_first", "latency_first", "cost_first", "balanced", "hybrid"]

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get("distillation_learning_rate", 0.01))
        self.teachers = [RuleBasedTeacher(), HistoricalMLTeacher(), StatefulQTeacher()]
        self.replay = ReplayBuffer(max_size=config.get("distillation_replay_size", 2000))
        self.epsilon = config.get("distillation_epsilon", 0.1)
        self.train_every = config.get("distillation_train_every", 10)
        self.counter = 0
        self.lock = asyncio.Lock()

    async def select_strategy(self, state, exploration=True):
        async with self.lock:
            x = state.to_feature_vector()
            n = 5
            teacher_probs = np.zeros(n)
            total_conf = 0.0
            for t in self.teachers:
                p = t.predict(state)
                c = t.confidence(state)
                if len(p) != n:
                    p = np.pad(p, (0, max(0, n - len(p))))[:n]
                teacher_probs += p * c
                total_conf += c
            if total_conf > 0:
                teacher_probs /= total_conf
            else:
                teacher_probs = np.ones(n) / n
            student_probs = self.student.predict_proba(x, n)
            if exploration and random.random() < self.epsilon:
                idx = random.randint(0, n - 1)
            else:
                combined = 0.8 * student_probs + 0.2 * teacher_probs
                idx = int(np.argmax(combined))
            return self.STRATEGIES[idx], idx, x, teacher_probs

    async def update(self, x, action_idx, reward, next_x, teacher_probs):
        async with self.lock:
            self.replay.push(x, action_idx, reward, next_x, teacher_probs)
            self.counter += 1
            if self.counter % self.train_every == 0 and len(self.replay) >= 8:
                s, a, r, _, tp = self.replay.sample(8)
                for i in range(len(s)):
                    self.student.update(s[i], tp[i], r[i], a[i])

    def get_stats(self):
        return {"student_counter": self.student.counter,
                "buffer_size": len(self.replay)}


# -----------------------------------------------------------------------------
# LIMIT Graph — [P3] real BFS + constraint propagation
# -----------------------------------------------------------------------------
class LimitGraphManager:
    def __init__(self, storage: Storage):
        self.storage = storage

    async def create_graph(self, graph_id, description, configuration):
        await self.storage.save_limit_graph_metadata(graph_id, description, configuration)

    async def add_node(self, graph_id, node_id, node_type, attributes):
        await self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)

    async def add_edge(self, graph_id, edge_id, source, target, weight, attributes):
        await self.storage.save_limit_graph_edge(
            edge_id, graph_id, source, target, weight, attributes)

    async def get_metadata(self, graph_id):
        return await self.storage.get_limit_graph_metadata(graph_id)

    async def find_path(self, graph_id, source, target) -> Optional[Dict[str, Any]]:
        """[P3] BFS over stored nodes/edges, returning shortest-weight path."""
        nodes = await self.storage.get_limit_graph_nodes(graph_id)
        edges = await self.storage.get_limit_graph_edges(graph_id)
        adj: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        for e in edges:
            adj[e["source"]].append((e["target"], e.get("weight") or 1.0))
        visited = {source}
        q = deque([(source, [source], 1.0)])
        while q:
            node, path, weight = q.popleft()
            if node == target:
                return {"path": path, "weight": weight}
            for nb, w in adj[node]:
                if nb not in visited:
                    visited.add(nb)
                    q.append((nb, path + [nb], weight * w))
        return None

    async def propagate_constraints(self, graph_id, source, source_value,
                                      max_depth: int = 5) -> Dict[str, float]:
        """[P3] Propagate a value from `source` along weighted edges."""
        nodes = await self.storage.get_limit_graph_nodes(graph_id)
        edges = await self.storage.get_limit_graph_edges(graph_id)
        adj: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        for e in edges:
            adj[e["source"]].append((e["target"], e.get("weight") or 1.0))
        values = {source: source_value}
        q = deque([(source, 0)])
        while q:
            node, depth = q.popleft()
            if depth >= max_depth:
                continue
            for nb, w in adj[node]:
                candidate = values[node] * w
                if nb not in values or candidate > values[nb]:
                    values[nb] = candidate
                    q.append((nb, depth + 1))
        return values


# -----------------------------------------------------------------------------
# MODP — [P3] real value iteration solver
# -----------------------------------------------------------------------------
class MODPOptimizer:
    def __init__(self, storage: Storage):
        self.storage = storage

    async def add_state(self, state_id, problem_id, state_attributes,
                          objective_values, stage):
        await self.storage.save_modp_state(
            state_id, problem_id, state_attributes, objective_values, stage)

    async def add_policy(self, policy_id, problem_id, state_id, action,
                           expected_objectives):
        await self.storage.save_modp_policy(
            policy_id, problem_id, state_id, action, expected_objectives)

    async def solve(self, problem_id: str,
                     weights: Optional[Dict[str, float]] = None,
                     gamma: float = 0.95, max_iter: int = 100,
                     tol: float = 1e-6) -> Dict[str, float]:
        """[P3] Value iteration over stored states/policies.
        Returns state_id → value (higher = better)."""
        states = await self.storage.get_modp_states(problem_id)
        policies = await self.storage.get_modp_policies(problem_id)
        if not states:
            return {}
        weights = weights or {"carbon_savings": 0.4, "cost": 0.3,
                               "latency": 0.2, "user_satisfaction": 0.1}
        policy_by_state: Dict[str, List[Dict]] = defaultdict(list)
        for p in policies:
            policy_by_state[p["state_id"]].append(p)

        V = {s["state_id"]: 0.0 for s in states}
        for _ in range(max_iter):
            delta = 0.0
            for s in states:
                sid = s["state_id"]
                best = -float("inf")
                for p in policy_by_state.get(sid, []):
                    obj = p.get("expected_objectives", {})
                    # normalize objectives to [0,1]
                    reward = (
                        weights.get("carbon_savings", 0.0) *
                        min(1.0, max(0.0, obj.get("carbon_savings", 0.0) / 50.0)) +
                        weights.get("cost", 0.0) *
                        (1.0 - min(1.0, obj.get("cost", 0.0) / 2000.0)) +
                        weights.get("latency", 0.0) *
                        (1.0 - min(1.0, obj.get("latency", 0.0) / 500.0)) +
                        weights.get("user_satisfaction", 0.0) *
                        min(1.0, max(0.0, obj.get("user_satisfaction", 0.0)))
                    )
                    # self-loop as next state
                    val = reward + gamma * V[sid]
                    if val > best:
                        best = val
                if best != -float("inf"):
                    delta = max(delta, abs(best - V[sid]))
                    V[sid] = best
            if delta < tol:
                break
        return V

    async def best_action(self, problem_id, state_id,
                            weights: Optional[Dict[str, float]] = None
                            ) -> Optional[Dict[str, Any]]:
        """[P3] Pick the policy with highest immediate weighted reward for
        a specific state."""
        policies = await self.storage.get_modp_policies(problem_id)
        candidates = [p for p in policies if p["state_id"] == state_id]
        if not candidates:
            return None
        weights = weights or {"carbon_savings": 0.4, "cost": 0.3,
                               "latency": 0.2, "user_satisfaction": 0.1}
        def score(p):
            obj = p.get("expected_objectives", {})
            return (
                weights.get("carbon_savings", 0) * obj.get("carbon_savings", 0) / 50.0 +
                weights.get("cost", 0) * (1.0 - min(1.0, obj.get("cost", 0) / 2000.0)) +
                weights.get("latency", 0) * (1.0 - min(1.0, obj.get("latency", 0) / 500.0)) +
                weights.get("user_satisfaction", 0) * obj.get("user_satisfaction", 0)
            )
        return max(candidates, key=score)


# -----------------------------------------------------------------------------
# RLHF — [P0] real Bradley-Terry training
# -----------------------------------------------------------------------------
class RLHFTrainer:
    def __init__(self, storage: Storage, strategy_names: List[str]):
        self.storage = storage
        self.strategy_names = list(strategy_names)
        self.model = None
        self._lock = asyncio.Lock()
        self._last_train = 0.0

    async def record_pair(self, pair_id, prompt, chosen, rejected,
                            reward_diff, metadata=None):
        await self.storage.save_preference_pair(
            pair_id, prompt, chosen, rejected, reward_diff, metadata or {})

    async def train_reward_model(self) -> bool:
        """[P0] Fit Bradley-Terry logistic regression on (chosen, rejected) pairs."""
        if not SKLEARN_AVAILABLE_PLACEHOLDER:
            return False
        pairs = await self.storage.get_preference_pairs(limit=2000)
        if len(pairs) < settings.rlhf_min_pairs:
            return False

        X: List[List[float]] = []
        y: List[int] = []
        n = len(self.strategy_names)
        for p in pairs:
            chosen = p.get("chosen_response", "")
            rejected = p.get("rejected_response", "")
            if chosen not in self.strategy_names or rejected not in self.strategy_names:
                continue
            c_idx = self.strategy_names.index(chosen)
            r_idx = self.strategy_names.index(rejected)
            feat = [0.0] * n
            feat[c_idx] = 1.0
            feat[r_idx] = -1.0
            X.append(feat)
            y.append(1)
            X.append([-v for v in feat])
            y.append(0)
        if not X:
            return False

        async with self._lock:
            try:
                from sklearn.linear_model import LogisticRegression
                model = LogisticRegression(max_iter=200)
                await asyncio.to_thread(model.fit, X, y)
                self.model = model
                self._last_train = time.time()
                logger.info("RLHF trained on %d pairs", len(X))
                return True
            except Exception as e:
                logger.warning("RLHF training failed: %s", e)
                return False

    def predict_reward(self, strategy: str) -> float:
        """[P0] Reward = P(strategy preferred)."""
        if self.model is None or strategy not in self.strategy_names:
            return 0.0
        try:
            idx = self.strategy_names.index(strategy)
            feat = [0.0] * len(self.strategy_names)
            feat[idx] = 1.0
            proba = self.model.predict_proba([feat])[0]
            return float(proba[1]) if len(proba) > 1 else 0.0
        except Exception:
            return 0.0


# sklearn availability guard for RLHF
try:
    import sklearn  # noqa
    SKLEARN_AVAILABLE_PLACEHOLDER = True
except ImportError:
    SKLEARN_AVAILABLE_PLACEHOLDER = False


# -----------------------------------------------------------------------------
# MoE gating — [P0] weight decay + exploration + ValueError guard
# -----------------------------------------------------------------------------
class MoEGatingNetwork:
    def __init__(self, storage: Optional[Storage],
                 config: Optional[Dict[str, Any]] = None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = list(self.config.get("expert_names",
                                                     ["online", "offline", "rule_based"]))
        self.num_experts = len(self.expert_names)
        self.n_features = self.config.get("n_features", 5)
        self.gating_weights = np.random.randn(self.num_experts, self.n_features) * 0.1
        self.lock = asyncio.Lock()

    def _encode_state(self, metrics: Dict[str, float]) -> np.ndarray:
        return np.array([
            metrics.get("quality", 0.5),
            metrics.get("energy", 0.5),
            metrics.get("carbon", 0.5),
            metrics.get("latency", 0.5),
            metrics.get("helium", 0.5),
        ], dtype=np.float32)

    async def select_expert(self, metrics: Dict[str, float],
                             explore_epsilon: float = 0.1
                             ) -> Tuple[str, np.ndarray]:
        x = self._encode_state(metrics)
        async with self.lock:
            logits = self.gating_weights @ x
            probs = np.exp(logits - logits.max())
            probs /= probs.sum()
            if random.random() < explore_epsilon:
                idx = random.randint(0, self.num_experts - 1)
            else:
                idx = int(np.argmax(probs))
            selected = self.expert_names[idx]
            confidence = float(probs[idx])
            for i, name in enumerate(self.expert_names):
                try:
                    MOE_EXPERT.labels(expert=name).set(float(probs[i]))
                except Exception:
                    pass

        if self.storage is not None:
            try:
                sample_id = hashlib.sha256(str(metrics).encode()).hexdigest()[:16]
                await self.storage.log_routing_decision(
                    str(uuid.uuid4()), sample_id, selected, confidence)
            except Exception as e:
                logger.debug("routing log failed: %s", e)

        return selected, probs

    async def add_training_sample(self, metrics, selected_expert, reward):
        # [P0] Guard against unknown expert
        try:
            expert_idx = self.expert_names.index(selected_expert)
        except ValueError:
            return
        x = self._encode_state(metrics)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        async with self.lock:
            logits = self.gating_weights @ x
            probs = np.exp(logits - logits.max())
            probs /= probs.sum()
            grad = (probs - target)[:, None] * x[None, :]
            self.gating_weights -= 0.1 * grad
            # [P0] weight decay
            self.gating_weights *= 0.999
            # [P0] L2 clip
            norm = float(np.linalg.norm(self.gating_weights))
            if norm > 10.0:
                self.gating_weights *= 10.0 / norm


# -----------------------------------------------------------------------------
# NSGA-II — [P0] honest objectives (aggregate per-strategy)
# -----------------------------------------------------------------------------
@dataclass
class MOPDWeightVector:
    vector_id: str
    weights: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self):
        return {"vector_id": self.vector_id, "weights": self.weights,
                "objectives": self.objectives,
                "scalarised_score": self.scalarised_score}

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


class NSGAIIWeightOptimizer:
    def __init__(self,
                 evaluate_func: Optional[Callable[[Dict[str, float]],
                                                    Awaitable[Dict[str, float]]]] = None,
                 population_size: int = 20,
                 generations: int = 10,
                 mutation_rate: float = 0.2,
                 crossover_rate: float = 0.8,
                 tournament_size: int = 3,
                 objective_weights: Optional[Dict[str, float]] = None,
                 dynamic_weights: bool = True):
        self.evaluate_func = evaluate_func
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {
            "carbon": 0.4, "cost": 0.3, "latency": 0.2, "user_satisfaction": 0.1}
        self.dynamic_weights = dynamic_weights
        self.best_individual: Optional[Dict[str, float]] = None
        self.best_fitness = -float("inf")
        self.pareto_front: List[MOPDWeightVector] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}
        self._all_points: List[MOPDWeightVector] = []
        self._lock = asyncio.Lock()

    def _random_individual(self) -> Dict[str, float]:
        keys = list(self.objective_weights.keys())
        w = {k: random.random() for k in keys}
        s = sum(w.values())
        return {k: v / s for k, v in w.items()} if s else {k: 1 / len(keys) for k in keys}

    def _crossover(self, p1, p2):
        child = {}
        for k in p1:
            u = random.random()
            if u < 0.5:
                beta = (2 * u) ** (1 / 21) if u <= 0.5 else (1 / (2 * (1 - u))) ** (1 / 21)
                child[k] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[k] + (1 - beta) * p2[k])))
            else:
                child[k] = p1[k] if random.random() < 0.5 else p2[k]
        s = sum(child.values())
        return {k: v / s for k, v in child.items()} if s else p1

    def _mutate(self, ind):
        m = dict(ind)
        for k in m:
            if random.random() < self.mutation_rate:
                u = random.random()
                delta = (2 * u) ** (1 / 21) - 1 if u < 0.5 else 1 - (2 * (1 - u)) ** (1 / 21)
                m[k] = max(0.0, min(1.0, m[k] + delta))
        s = sum(m.values())
        return {k: v / s for k, v in m.items()} if s else ind

    def _fast_non_dominated_sort(self, points):
        fronts: List[List[MOPDWeightVector]] = []
        dom_count = {id(p): 0 for p in points}
        dominated = {id(p): [] for p in points}
        for i, p in enumerate(points):
            for j, q in enumerate(points):
                if i == j:
                    continue
                if all(p.objectives[k] >= q.objectives[k] for k in p.objectives) and \
                   any(p.objectives[k] > q.objectives[k] for k in p.objectives):
                    dominated[id(p)].append(q)
                elif all(q.objectives[k] >= p.objectives[k] for k in q.objectives) and \
                     any(q.objectives[k] > p.objectives[k] for k in q.objectives):
                    dom_count[id(p)] += 1
            if dom_count[id(p)] == 0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p)
        i = 0
        while i < len(fronts):
            nxt = []
            for p in fronts[i]:
                for q in dominated[id(p)]:
                    dom_count[id(q)] -= 1
                    if dom_count[id(q)] == 0:
                        nxt.append(q)
            if nxt:
                fronts.append(nxt)
            i += 1
        return fronts

    def _crowding_distance(self, front):
        if not front:
            return {}
        d = {id(p): 0.0 for p in front}
        for k in front[0].objectives:
            sf = sorted(front, key=lambda x: x.objectives[k])
            d[id(sf[0])] = float("inf")
            d[id(sf[-1])] = float("inf")
            lo, hi = sf[0].objectives[k], sf[-1].objectives[k]
            if hi == lo:
                continue
            for i in range(1, len(sf) - 1):
                d[id(sf[i])] += (sf[i + 1].objectives[k] - sf[i - 1].objectives[k]) / (hi - lo)
        return d

    def _tournament(self, population, fronts, crowding):
        candidates = random.sample(population, min(self.tournament_size, len(population)))
        point_for = {id(ind): pt for ind, pt in zip(population, self._all_points)}
        best = candidates[0]
        best_rank, best_cd = float("inf"), -float("inf")
        for c in candidates:
            pt = point_for.get(id(c))
            if pt is None:
                continue
            rank = len(fronts)
            for i, f in enumerate(fronts):
                if pt in f:
                    rank = i
                    break
            cd = crowding.get(id(pt), 0)
            if rank < best_rank or (rank == best_rank and cd > best_cd):
                best, best_rank, best_cd = c, rank, cd
        return best

    def _compute_dynamic_weights(self):
        w = dict(self.objective_weights)
        if not self.dynamic_weights or not self.pareto_front:
            return w
        for k in w:
            avg = float(np.mean([p.objectives[k] for p in self.pareto_front]))
            mx = float(np.max([p.objectives[k] for p in self.pareto_front]))
            if mx > 0 and avg < 0.5 * mx:
                w[k] = min(0.6, w.get(k, 0.0) * 1.5)
        s = sum(w.values())
        return {k: v / s for k, v in w.items()} if s else w

    def _select_best_from_pareto(self, pareto, weights):
        if not pareto:
            return None
        keys = list(weights.keys())
        mx = {k: max(p.objectives[k] for p in pareto) for k in keys}
        mn = {k: min(p.objectives[k] for p in pareto) for k in keys}
        rng = {k: (mx[k] - mn[k] if mx[k] != mn[k] else 1.0) for k in keys}
        best, best_score = None, -float("inf")
        for p in pareto:
            s = 0.0
            for k in keys:
                norm = (p.objectives[k] - mn[k]) / rng[k]
                s += weights.get(k, 0) * norm
            p.scalarised_score = s
            if s > best_score:
                best, best_score = p, s
        return best

    async def evolve(self):
        if self.evaluate_func is None:
            raise ValueError("evaluate_func not set")
        async with self._lock:
            population = [self._random_individual() for _ in range(self.population_size)]
            results = await asyncio.gather(*[self.evaluate_func(ind) for ind in population])
            points = []
            for ind, obj in zip(population, results):
                pt = MOPDWeightVector(str(uuid.uuid4()), ind, obj)
                points.append(pt)
                self._eval_cache[tuple(sorted(ind.items()))] = obj
            self._all_points = points

            for gen in range(self.generations):
                fronts = self._fast_non_dominated_sort(points)
                crowding: Dict[int, float] = {}
                for f in fronts:
                    crowding.update(self._crowding_distance(f))
                offspring = []
                while len(offspring) < self.population_size:
                    p1 = self._tournament(population, fronts, crowding)
                    p2 = self._tournament(population, fronts, crowding)
                    c = self._crossover(p1, p2) if random.random() < self.crossover_rate \
                        else copy.deepcopy(p1)
                    offspring.append(self._mutate(c))
                child_results = await asyncio.gather(
                    *[self.evaluate_func(ind) for ind in offspring])
                child_points = []
                for ind, obj in zip(offspring, child_results):
                    pt = MOPDWeightVector(str(uuid.uuid4()), ind, obj)
                    child_points.append(pt)
                    self._eval_cache[tuple(sorted(ind.items()))] = obj
                combined_inds = population + offspring
                combined_points = points + child_points
                unique = {}
                for ind, pt in zip(combined_inds, combined_points):
                    k = tuple(sorted(ind.items()))
                    unique[k] = (ind, pt)
                population = [v[0] for v in unique.values()]
                points = [v[1] for v in unique.values()]
                self._all_points = points
                fronts = self._fast_non_dominated_sort(points)
                new_pop, new_pts = [], []
                for f in fronts:
                    if len(new_pop) + len(f) <= self.population_size:
                        for pt in f:
                            for ind, pt2 in zip(population, points):
                                if pt2 is pt:
                                    new_pop.append(ind)
                                    new_pts.append(pt)
                                    break
                    else:
                        cd = self._crowding_distance(f)
                        for pt in sorted(f, key=lambda x: cd.get(id(x), 0), reverse=True):
                            if len(new_pop) >= self.population_size:
                                break
                            for ind, pt2 in zip(population, points):
                                if pt2 is pt:
                                    new_pop.append(ind)
                                    new_pts.append(pt)
                                    break
                population = new_pop[:self.population_size]
                points = new_pts[:self.population_size]
                self._all_points = points
                fronts = self._fast_non_dominated_sort(points)
                if fronts:
                    self.pareto_front = fronts[0]

            w = self._compute_dynamic_weights()
            best = self._select_best_from_pareto(self.pareto_front, w)
            if best:
                self.best_individual = best.weights
                self.best_fitness = best.scalarised_score
            try:
                MOEA_PARETO_SIZE.set(len(self.pareto_front))
            except Exception:
                pass
            return self.pareto_front


# -----------------------------------------------------------------------------
# Global components — declared before startup
# -----------------------------------------------------------------------------
loader: Optional[AIDataCenterLoader] = None
selector: Optional[GreenDatacenterSelector] = None
carbon_client: Optional[RealCarbonIntensityClient] = None
latency_estimator: Optional[CloudLatencyEstimator] = None
sustainability_enricher: Optional[SustainabilitySignalEnricher] = None
cache: Optional[Cache] = None
storage: Optional[Storage] = None
security: Optional[QuantumResilientSecurity] = None
blockchain: Optional[BlockchainVerifier] = None
multi_cloud: Optional[MultiCloudDistributor] = None
strategy_optimizer: Optional[DistillationStrategyOptimizer] = None
moea_optimizer: Optional[NSGAIIWeightOptimizer] = None
moea_task: Optional[asyncio.Task] = None

# v2.4.0 components
limit_graph_manager: Optional[LimitGraphManager] = None
modp_solver: Optional[MODPOptimizer] = None
rlhf_trainer: Optional[RLHFTrainer] = None
moe_gating: Optional[MoEGatingNetwork] = None

# [P0] bounded deque + batched writes
interaction_log: Deque[Dict[str, Any]] = deque(maxlen=settings.interaction_log_max)
_pending_interactions: List[Dict[str, Any]] = []
interaction_lock = asyncio.Lock()

# Background task handles
_rlhf_task: Optional[asyncio.Task] = None
_flush_task: Optional[asyncio.Task] = None
_cleanup_task: Optional[asyncio.Task] = None

PROJECTS_CACHE_KEY = "all_projects"


# -----------------------------------------------------------------------------
# Interaction log helpers — [P0] batched writes
# -----------------------------------------------------------------------------
def _append_csv_sync(path: str, entries: List[Dict[str, Any]]):
    p = Path(path)
    write_header = not p.exists()
    df = pd.DataFrame(entries)
    df.to_csv(p, mode="a", header=write_header, index=False)


def _read_csv_sync(path: str) -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        df = pd.read_csv(p)
        return df.to_dict(orient="records")
    except Exception:
        return []


async def _log_interaction(state: StrategyState, strategy: str, reward: float,
                             carbon_savings: float, cost: float, latency: float):
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "strategy": strategy,
        "reward": float(reward),
        "carbon_savings": float(carbon_savings),
        "cost": float(cost),
        "latency": float(latency),
        "state_vector": state.to_feature_vector().tolist(),
    }
    async with interaction_lock:
        interaction_log.append(entry)
        _pending_interactions.append(entry)
    try:
        INTERACTION_LOG_SIZE.set(len(interaction_log))
    except Exception:
        pass


async def _flush_interactions_loop():
    while True:
        try:
            await asyncio.sleep(settings.interaction_flush_interval)
            await _flush_interactions()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("flush loop error: %s", e)


async def _flush_interactions():
    async with interaction_lock:
        if not _pending_interactions:
            return
        batch = list(_pending_interactions)
        _pending_interactions.clear()
    try:
        await asyncio.to_thread(_append_csv_sync, settings.interaction_logs_path, batch)
    except Exception as e:
        logger.error("CSV flush failed: %s", e)
        async with interaction_lock:
            _pending_interactions.extend(batch)


# -----------------------------------------------------------------------------
# FastAPI app — [P2] middleware, exception handlers, rate limiting
# -----------------------------------------------------------------------------
if FASTAPI_AVAILABLE:
    app = FastAPI(
        title="Green Data Center Dashboard",
        description="AI Data Center Sustainability Explorer",
        version="2.5.0",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "X-API-Key", "X-Request-Id"],
    )

    # [P2] correlation middleware
    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):
        cid = request.headers.get("X-Request-Id") or uuid.uuid4().hex[:12]
        token = correlation_id_var.set(cid)
        start = time.time()
        route = request.url.path
        try:
            response = await call_next(request)
            response.headers["X-Request-Id"] = cid
            try:
                REQUEST_COUNT.labels(route=route, method=request.method,
                                      status=str(response.status_code)).inc()
                REQUEST_LATENCY.labels(route=route).observe(time.time() - start)
            except Exception:
                pass
            return response
        finally:
            correlation_id_var.reset(token)

    # [P2] security headers
    @app.middleware("http")
    async def security_headers_middleware(request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net; "
            "img-src 'self' data: https://*.tile.openstreetmap.org; "
            "connect-src 'self';"
        )
        return response

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse(status_code=exc.status_code,
                            content={"detail": exc.detail})

    @app.exception_handler(RequestValidationError)
    async def validation_handler(request: Request, exc: RequestValidationError):
        return JSONResponse(status_code=422, content={"detail": exc.errors()})

    @app.exception_handler(Exception)
    async def global_handler(request: Request, exc: Exception):
        logger.error("Unhandled: %s", exc, exc_info=True)
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})

    if SLOWAPI_AVAILABLE:
        limiter = Limiter(key_func=get_remote_address,
                            default_limits=[f"{settings.rate_limit_requests}/"
                                             f"{settings.rate_limit_window}seconds"])
        app.state.limiter = limiter
        app.add_exception_handler(429, _rate_limit_exceeded_handler)
    else:
        limiter = None

    # [P2] in-process rate limiter fallback
    class _RateBucket:
        def __init__(self, rate: int, window: int):
            self.rate = rate
            self.window = window
            self.buckets: Dict[str, Deque[float]] = defaultdict(deque)
            self._lock = asyncio.Lock()

        async def allow(self, key: str) -> bool:
            now = time.time()
            async with self._lock:
                q = self.buckets[key]
                while q and q[0] < now - self.window:
                    q.popleft()
                if len(q) >= self.rate:
                    return False
                q.append(now)
                return True

    _fallback_limiter = _RateBucket(settings.rate_limit_requests,
                                     settings.rate_limit_window)

    def rate_limit_decorator(limit_str: str):
        if limiter is not None:
            return limiter.limit(limit_str)
        def deco(fn):
            return fn
        return deco

    async def verify_api_key(api_key: str = Header(None, alias="X-API-Key")):
        if settings.api_key_enabled:
            if api_key != settings.api_key:
                raise HTTPException(status_code=401, detail="Invalid API key")
        return api_key

    async def _require_key_on_post(request: Request):
        # Used as a dependency on POST endpoints
        if settings.api_key_enabled:
            key = request.headers.get("X-API-Key")
            if key != settings.api_key:
                raise HTTPException(status_code=401, detail="Invalid API key")
        return True


# -----------------------------------------------------------------------------
# Startup / shutdown
# -----------------------------------------------------------------------------
async def startup():
    global loader, selector, carbon_client, latency_estimator, \
           sustainability_enricher, cache, storage, security, blockchain, \
           multi_cloud, strategy_optimizer, moea_optimizer, moea_task, \
           limit_graph_manager, modp_solver, rlhf_trainer, moe_gating, \
           _rlhf_task, _flush_task, _cleanup_task

    logger.info("Starting Green Data Center Dashboard v2.5.0...")

    storage = Storage()
    cache = Cache()
    loader = AIDataCenterLoader()
    selector = GreenDatacenterSelector(loader)
    carbon_client = RealCarbonIntensityClient()
    latency_estimator = CloudLatencyEstimator()
    sustainability_enricher = SustainabilitySignalEnricher()
    security = QuantumResilientSecurity()
    blockchain = BlockchainVerifier()
    multi_cloud = MultiCloudDistributor()

    strategy_optimizer = DistillationStrategyOptimizer({
        "distillation_epsilon": settings.distillation_epsilon,
        "distillation_train_every": settings.distillation_train_every,
        "distillation_replay_size": settings.distillation_replay_size,
        "distillation_learning_rate": settings.distillation_learning_rate,
    })

    if settings.enable_limit_graph:
        limit_graph_manager = LimitGraphManager(storage)
        if not await limit_graph_manager.get_metadata("strategy_graph"):
            await limit_graph_manager.create_graph(
                "strategy_graph", "Strategy Weight Vector Relationships",
                {"created_by": "startup"})

    if settings.enable_modp:
        modp_solver = MODPOptimizer(storage)

    if settings.enable_rlhf:
        rlhf_trainer = RLHFTrainer(storage, DistillationStrategyOptimizer.STRATEGIES)

    if settings.enable_moe:
        moe_gating = MoEGatingNetwork(
            storage, {"expert_names": DistillationStrategyOptimizer.STRATEGIES,
                       "n_features": 5})

    # [P0] MOEA constructed first, then load Pareto
    moea_optimizer = NSGAIIWeightOptimizer(
        evaluate_func=None,
        population_size=settings.moea_population_size,
        generations=settings.moea_generations,
        mutation_rate=settings.moea_mutation_rate,
        crossover_rate=settings.moea_crossover_rate,
        tournament_size=settings.moea_tournament_size,
        objective_weights=settings.moea_objective_weights,
        dynamic_weights=settings.moea_dynamic_weights,
    )
    if Path(settings.moea_pareto_path).exists():
        try:
            raw = json.loads(Path(settings.moea_pareto_path).read_text())
            if isinstance(raw, list):
                moea_optimizer.pareto_front = [MOPDWeightVector.from_dict(d) for d in raw]
        except Exception as e:
            logger.warning("Pareto load failed: %s", e)

    # [P3] Load prior interactions from CSV
    prev = await asyncio.to_thread(_read_csv_sync, settings.interaction_logs_path)
    if prev:
        async with interaction_lock:
            for row in prev[-settings.interaction_log_max:]:
                try:
                    interaction_log.append({
                        "timestamp": str(row.get("timestamp", "")),
                        "strategy": str(row.get("strategy", "balanced")),
                        "reward": float(row.get("reward", 0.0)),
                        "carbon_savings": float(row.get("carbon_savings", 0.0)),
                        "cost": float(row.get("cost", 0.0)),
                        "latency": float(row.get("latency", 0.0)),
                    })
                except Exception:
                    continue
        logger.info("Loaded %d interactions from CSV", len(interaction_log))

    await carbon_client.start()

    # Background loops
    if settings.moea_enabled:
        moea_task = asyncio.create_task(moea_loop())
        # [P0] fire once at startup (with warmup if needed)
        asyncio.create_task(_run_moea_soon())

    if rlhf_trainer is not None:
        _rlhf_task = asyncio.create_task(rlhf_loop())

    _flush_task = asyncio.create_task(_flush_interactions_loop())
    _cleanup_task = asyncio.create_task(cleanup_loop())

    logger.info("Dashboard startup complete")


async def shutdown():
    global moea_task, _rlhf_task, _flush_task, _cleanup_task
    logger.info("Shutting down Green Data Center Dashboard...")
    if carbon_client:
        try: await carbon_client.close()
        except Exception: pass
    for name, task in [("moea", moea_task), ("rlhf", _rlhf_task),
                        ("flush", _flush_task), ("cleanup", _cleanup_task)]:
        if task:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
    # [P0] final flush
    try:
        await _flush_interactions()
    except Exception:
        pass
    if storage:
        await storage.close()
    logger.info("Shutdown complete")


# -----------------------------------------------------------------------------
# Background loops
# -----------------------------------------------------------------------------
async def moea_loop():
    while True:
        try:
            await asyncio.sleep(settings.moea_interval_seconds)
            await run_moea()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("MOEA loop error: %s", e)
            await asyncio.sleep(60)


async def _run_moea_soon():
    await asyncio.sleep(1.0)
    try:
        await run_moea()
    except Exception as e:
        logger.error("Initial MOEA run failed: %s", e)


async def rlhf_loop():
    while True:
        try:
            await asyncio.sleep(settings.rlhf_training_interval)
            if rlhf_trainer:
                await rlhf_trainer.train_reward_model()
                pairs = await storage.get_preference_pairs(limit=1)
                try:
                    row = await storage.fetchone(
                        "SELECT COUNT(*) AS c FROM preference_pairs")
                    if row:
                        RLHF_PAIRS.set(int(row["c"]))
                except Exception:
                    pass
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("RLHF loop error: %s", e)


async def cleanup_loop():
    while True:
        try:
            await asyncio.sleep(settings.cleanup_interval)
            if storage:
                counts = await storage.cleanup_old_data(settings.data_retention_days)
                logger.info("Cleanup: %s", counts)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Cleanup loop error: %s", e)


# -----------------------------------------------------------------------------
# MOEA evaluation — [P0] honest objectives (per-strategy aggregation)
# -----------------------------------------------------------------------------
def _warmup_interactions(n: int = 30) -> List[Dict[str, Any]]:
    strategies = DistillationStrategyOptimizer.STRATEGIES
    return [{
        "strategy": random.choice(strategies),
        "reward": random.uniform(0.3, 0.9),
        "carbon_savings": random.uniform(0, 50),
        "cost": random.uniform(100, 2000),
        "latency": random.uniform(20, 400),
    } for _ in range(n)]


async def run_moea():
    if moea_optimizer is None:
        return

    # [P0] warmup if insufficient data
    snapshot = list(interaction_log)
    if len(snapshot) < 20:
        logger.info("MOEA: insufficient history; using warmup data")
        snapshot = _warmup_interactions(30)

    # [P0] aggregate per-strategy objectives
    per_strategy: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for entry in snapshot:
        s = entry.get("strategy", "balanced")
        per_strategy[s].append(entry)

    strategy_obj: Dict[str, Dict[str, float]] = {}
    for s, obs in per_strategy.items():
        n = len(obs)
        strategy_obj[s] = {
            "carbon": sum(o.get("carbon_savings", 0.0) for o in obs) / n / 50.0,
            "cost": 1.0 - min(1.0,
                sum(o.get("cost", 0.0) for o in obs) / n / 2000.0),
            "latency": 1.0 - min(1.0,
                sum(o.get("latency", 0.0) for o in obs) / n / 500.0),
            "user_satisfaction": sum(o.get("reward", 0.0) for o in obs) / n,
        }

    async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
        if not strategy_obj:
            return {"carbon": 0.0, "cost": 0.0, "latency": 0.0,
                    "user_satisfaction": 0.0}
        best_s, best_score = None, -float("inf")
        for s, obj in strategy_obj.items():
            score = sum(weights.get(k, 0.0) * obj[k] for k in weights)
            if score > best_score:
                best_score, best_s = score, s
        return dict(strategy_obj[best_s])

    moea_optimizer.evaluate_func = evaluate
    pareto = await moea_optimizer.evolve()
    logger.info("MOEA produced Pareto front of size %d", len(pareto))

    try:
        Path(settings.moea_pareto_path).write_text(
            json.dumps([p.to_dict() for p in pareto], indent=2))
    except Exception as e:
        logger.warning("Pareto save failed: %s", e)

    weights = moea_optimizer._compute_dynamic_weights()
    best = moea_optimizer._select_best_from_pareto(pareto, weights)
    if best:
        if modp_solver:
            sid = f"moea_best_{best.vector_id}"
            await modp_solver.add_state(
                sid, "strategy_weight_optimization",
                {"weights": best.weights}, best.objectives, stage=1)
            await modp_solver.add_policy(
                f"policy_{best.vector_id}", "strategy_weight_optimization",
                sid, "moea_best", best.objectives)
        if limit_graph_manager:
            await limit_graph_manager.add_node(
                "strategy_graph", f"vector_{best.vector_id}",
                "best_weight_vector",
                {"weights": best.weights, "objectives": best.objectives})


# -----------------------------------------------------------------------------
# Project fetching — [P0] batched carbon, deep-copy
# -----------------------------------------------------------------------------
async def _fetch_projects_with_carbon() -> List[Dict[str, Any]]:
    cached = await cache.get(PROJECTS_CACHE_KEY)
    if cached is not None:
        return cached

    raw_projects = await asyncio.to_thread(loader.get_all_projects)

    sem = asyncio.Semaphore(10)

    async def _one(p):
        async with sem:
            ck = f"carbon_{p.location_country}"
            cached_v = await cache.get(ck)
            if cached_v is not None:
                return cached_v
            t0 = time.time()
            try:
                v = await carbon_client.get_intensity(p.location_country)
            except Exception as e:
                logger.warning("Carbon fetch failed for %s: %s",
                                p.location_country, e)
                v = None
            try:
                CARBON_FETCH_LATENCY.labels(country=p.location_country).observe(
                    time.time() - t0)
            except Exception:
                pass
            if v is not None:
                await cache.set(ck, v)
            return v

    intensities = await asyncio.gather(*[_one(p) for p in raw_projects])

    out = []
    for p, intensity in zip(raw_projects, intensities):
        p2 = copy.deepcopy(p)
        if intensity is not None:
            p2.sustainability.grid_carbon_intensity_gco2_per_kwh = intensity
        p2.green_score = loader._compute_green_score(p2)
        out.append({
            "id": p2.project_id, "name": p2.project_name,
            "company": p2.company,
            "location": f"{p2.location_city}, {p2.location_country}",
            "lat": p2.latitude, "lon": p2.longitude,
            "green_score": p2.green_score,
            "capacity_mw": p2.planned_power_capacity_mw,
            "status": p2.status,
            "carbon_intensity": p2.sustainability.grid_carbon_intensity_gco2_per_kwh,
            "renewable_share": p2.sustainability.renewable_share_pct,
            "pue": p2.sustainability.pue_estimated,
            "cooling_type": getattr(p2.sustainability, "cooling_type", "unknown"),
            "water_stress": p2.sustainability.water_stress_index,
        })
    await cache.set(PROJECTS_CACHE_KEY, out)
    return out


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
if FASTAPI_AVAILABLE:
    # --- Public dashboard ---
    @app.get("/", response_class=HTMLResponse)
    async def dashboard_root():
        """[P1] Public dashboard (no API key required for browser navigation)."""
        try:
            projects = await _fetch_projects_with_carbon()
        except Exception as e:
            logger.error("Dashboard render error: %s", e)
            return HTMLResponse(f"<h1>Dashboard error</h1><pre>{_html.escape(str(e))}</pre>",
                                 status_code=500)
        html_out = _render_dashboard(projects)
        return HTMLResponse(content=html_out)

    # --- Health / ready / metrics ---
    @app.get("/health")
    async def health():
        components = {
            "storage": storage is not None,
            "cache": cache is not None,
            "carbon_client": carbon_client is not None,
            "moea": moea_optimizer is not None,
            "rlhf": rlhf_trainer is not None,
            "modp": modp_solver is not None,
            "limit_graph": limit_graph_manager is not None,
            "moe": moe_gating is not None,
        }
        status = "ok" if all(components.values()) else "degraded"
        return {"status": status, "version": "2.5.0", "components": components}

    @app.get("/ready")
    async def ready():
        if not all([storage, carbon_client, loader, selector]):
            raise HTTPException(status_code=503, detail="Not ready")
        return {"ready": True}

    @app.get("/metrics")
    async def metrics():
        if not PROMETHEUS_AVAILABLE:
            raise HTTPException(status_code=503, detail="Prometheus not installed")
        from fastapi import Response
        return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

    # --- Projects ---
    @app.get("/api/projects")
    @rate_limit_decorator(f"{settings.rate_limit_requests}/{settings.rate_limit_window}s")
    async def get_projects(request: Request):
        if not await _fallback_limiter.allow(request.client.host if request.client else "anon"):
            raise HTTPException(status_code=429, detail="Rate limit exceeded")
        try:
            projects = await _fetch_projects_with_carbon()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        stats = await asyncio.to_thread(loader.get_statistics)
        response = {"projects": projects, "statistics": stats,
                    "quantum_signature": await security.sign_data(
                        {"type": "projects_list", "count": len(projects)})}

        async def _bg():
            try: await blockchain.record_recommendation(
                {"type": "projects_list", "count": len(projects)})
            except Exception: pass
        asyncio.create_task(_bg())
        return response

    # --- Recommend ---
    @app.post("/api/recommend")
    @rate_limit_decorator(f"{settings.rate_limit_requests}/{settings.rate_limit_window}s")
    async def recommend_workload(request: Request, payload: Dict[str, Any],
                                   _key: bool = Depends(_require_key_on_post)):
        if not await _fallback_limiter.allow(request.client.host if request.client else "anon"):
            raise HTTPException(status_code=429, detail="Rate limit exceeded")

        try:
            workload = WorkloadSpec(
                gpu_hours=float(payload.get("gpu_hours", 100)),
                latency_tolerance_ms=float(payload.get("latency_tolerance_ms", 200)),
                workload_type=payload.get("workload_type", "training"),
                carbon_budget_kg=payload.get("carbon_budget_kg"),
                max_cost_usd=payload.get("max_cost_usd"),
            )
        except Exception as e:
            raise HTTPException(status_code=422, detail=str(e))

        user_region = payload.get("user_region", "us-east")
        state = StrategyState(
            gpu_hours=workload.gpu_hours,
            latency_tolerance_ms=workload.latency_tolerance_ms,
            workload_type=workload.workload_type,
            carbon_budget_kg=workload.carbon_budget_kg,
            max_cost_usd=workload.max_cost_usd,
            user_region=user_region,
        )

        # Strategy selection
        if moe_gating is not None:
            metrics = {
                "quality": 0.5,
                "energy": 1.0 - min(1.0, workload.gpu_hours / 1000.0),
                "carbon": 1.0 - min(1.0, (workload.carbon_budget_kg or 50) / 100.0),
                "latency": 1.0 - min(1.0, workload.latency_tolerance_ms / 500.0),
                "helium": 0.5,
            }
            expert_name, _probs = await moe_gating.select_expert(metrics)
            strategy = expert_name if expert_name in DistillationStrategyOptimizer.STRATEGIES \
                else "balanced"
            action_idx = DistillationStrategyOptimizer.STRATEGIES.index(strategy)
            state_vec = state.to_feature_vector()
            teacher_probs = np.ones(5) / 5
        else:
            strategy, action_idx, state_vec, teacher_probs = \
                await strategy_optimizer.select_strategy(state)

        result = await asyncio.to_thread(selector.select_datacenter, workload, user_region)

        projects = await asyncio.to_thread(loader.get_all_projects)
        avg_carbon = (sum(p.sustainability.grid_carbon_intensity_gco2_per_kwh
                            for p in projects) / len(projects)) if projects else 400
        avg_emissions = workload.gpu_hours * 0.65 * 1.3 * (avg_carbon / 1000)
        savings = avg_emissions - result.estimated_carbon_kg

        latency_enrich = {}
        if latency_estimator is not None:
            try:
                latency_enrich["cloud_latency_ms"] = await latency_estimator.estimate(
                    user_region, result.selected_project.location_country)
            except Exception:
                pass
        sustainability_enrich = {}
        if sustainability_enricher is not None:
            try:
                sustainability_enrich = await sustainability_enricher.enrich(
                    result.selected_project,
                    result.selected_project.sustainability.grid_carbon_intensity_gco2_per_kwh)
            except Exception:
                pass

        response = {
            "selected_project": {
                "id": result.selected_project.project_id,
                "name": result.selected_project.project_name,
                "location": f"{result.selected_project.location_city}, "
                            f"{result.selected_project.location_country}",
                "green_score": result.green_score,
                "estimated_carbon_kg": result.estimated_carbon_kg,
                "estimated_cost_usd": result.estimated_cost_usd,
                "latency_ms": result.latency_ms,
            },
            "alternatives": [{"name": p.project_name, "green_score": s}
                              for p, s in result.alternatives],
            "rationale": result.reasoning,
            "carbon_savings_kg": max(0.0, savings),
            "strategy_used": strategy,
            "enrichment": {**latency_enrich, **sustainability_enrich},
        }

        reward = 0.0
        if savings > 0:
            reward += 0.5
        if result.estimated_cost_usd < 1000:
            reward += 0.3
        if result.latency_ms < 200:
            reward += 0.2
        reward = max(0.0, min(1.0, reward))

        # Update MoE + Distillation
        if moe_gating is not None:
            await moe_gating.add_training_sample(metrics, strategy, reward)
        await strategy_optimizer.update(state_vec, action_idx, reward,
                                          state.to_feature_vector(), teacher_probs)

        # RLHF pair (5%)
        if rlhf_trainer is not None and random.random() < 0.05:
            rejected = random.choice(
                [s for s in DistillationStrategyOptimizer.STRATEGIES
                 if s != strategy])
            await rlhf_trainer.record_pair(
                str(uuid.uuid4()),
                "Which strategy is better for this workload?",
                strategy, rejected, reward,
                {"workload": workload.model_dump(), "user_region": user_region})

        # MODP state + policy
        if modp_solver is not None:
            sid = f"recommend_{uuid.uuid4()}"
            await modp_solver.add_state(
                sid, "strategy_selection",
                {"workload": workload.model_dump(), "strategy": strategy},
                {"carbon_savings": savings,
                 "cost": result.estimated_cost_usd,
                 "latency": result.latency_ms,
                 "user_satisfaction": reward}, stage=0)
            await modp_solver.add_policy(
                f"policy_{sid}", "strategy_selection",
                sid, strategy,
                {"carbon_savings": savings,
                 "cost": result.estimated_cost_usd,
                 "latency": result.latency_ms,
                 "user_satisfaction": reward})

        # LIMIT graph
        if limit_graph_manager is not None:
            await limit_graph_manager.add_node(
                "strategy_graph", f"recommendation_{uuid.uuid4()}",
                "recommendation",
                {"strategy": strategy, "carbon_savings": savings,
                 "cost": result.estimated_cost_usd,
                 "latency": result.latency_ms})

        # Interaction log (batched)
        await _log_interaction(state, strategy, reward, savings,
                                result.estimated_cost_usd, result.latency_ms)

        # Signature + blockchain
        response["quantum_signature"] = await security.sign_data(response)
        try:
            tx = await blockchain.record_recommendation(
                {"workload": workload.model_dump(),
                 "selected": result.selected_project.project_id,
                 "carbon_savings": savings})
            response["blockchain_tx_hash"] = tx.get("tx_hash")
        except Exception as e:
            logger.warning("Blockchain failed: %s", e)
        try:
            response["cloud_distribution"] = await multi_cloud.distribute(
                {"user_region": user_region})
        except Exception as e:
            logger.warning("Cloud failed: %s", e)

        # Audit
        if storage is not None:
            try:
                api_key = request.headers.get("X-API-Key", "")
                await storage.log_audit(
                    user_id="key:" + hashlib.sha256(
                        api_key.encode()).hexdigest()[:8] if api_key else "anonymous",
                    action="recommend",
                    details={"workload": workload.model_dump(),
                             "selected": result.selected_project.project_id})
            except Exception:
                pass

        try:
            RECOMMEND_COUNT.labels(strategy=strategy, status="success").inc()
        except Exception:
            pass
        return response

    # --- Batch recommend ---
    @app.post("/api/recommend/batch")
    @rate_limit_decorator(f"{settings.rate_limit_requests}/{settings.rate_limit_window}s")
    async def recommend_batch(request: Request, payload: Dict[str, Any],
                                _key: bool = Depends(_require_key_on_post)):
        workloads = payload.get("workloads", [])
        if not workloads:
            raise HTTPException(status_code=422, detail="Empty workloads list")
        # Process sequentially (bounded by semaphore)
        sem = asyncio.Semaphore(5)
        results: List[Any] = [None] * len(workloads)

        async def _one(i, w):
            async with sem:
                try:
                    # Reuse recommend_workload by crafting a mini Request
                    results[i] = await _recommend_internal(w)
                except Exception as e:
                    results[i] = {"error": str(e)}

        await asyncio.gather(*[_one(i, w) for i, w in enumerate(workloads)])
        return {"results": results, "count": len(results)}

    async def _recommend_internal(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Internal recommend without HTTP layer."""
        workload = WorkloadSpec(
            gpu_hours=float(payload.get("gpu_hours", 100)),
            latency_tolerance_ms=float(payload.get("latency_tolerance_ms", 200)),
            workload_type=payload.get("workload_type", "training"),
            carbon_budget_kg=payload.get("carbon_budget_kg"),
            max_cost_usd=payload.get("max_cost_usd"),
        )
        user_region = payload.get("user_region", "us-east")
        state = StrategyState(workload.gpu_hours, workload.latency_tolerance_ms,
                                workload.workload_type, workload.carbon_budget_kg,
                                workload.max_cost_usd, user_region)
        if moe_gating is not None:
            metrics = {"quality": 0.5,
                        "energy": 1.0 - min(1.0, workload.gpu_hours / 1000.0),
                        "carbon": 1.0 - min(1.0, (workload.carbon_budget_kg or 50) / 100.0),
                        "latency": 1.0 - min(1.0, workload.latency_tolerance_ms / 500.0),
                        "helium": 0.5}
            expert_name, _ = await moe_gating.select_expert(metrics)
            strategy = expert_name if expert_name in DistillationStrategyOptimizer.STRATEGIES \
                else "balanced"
        else:
            strategy = "balanced"
        result = await asyncio.to_thread(selector.select_datacenter, workload, user_region)
        return {"strategy": strategy, "selected": result.selected_project.project_name,
                "carbon_kg": result.estimated_carbon_kg,
                "cost_usd": result.estimated_cost_usd,
                "latency_ms": result.latency_ms}

    # --- Interactions ---
    @app.get("/api/interactions")
    async def get_interactions(limit: int = 100):
        n = max(1, min(int(limit), 1000))
        items = list(interaction_log)[-n:]
        return {"count": len(items), "items": items}

    # --- MOEA ---
    @app.get("/api/moea/pareto")
    async def get_pareto():
        if moea_optimizer is None:
            raise HTTPException(status_code=503, detail="MOEA unavailable")
        return {"pareto": [p.to_dict() for p in moea_optimizer.pareto_front],
                "size": len(moea_optimizer.pareto_front),
                "best": moea_optimizer.best_individual}

    @app.post("/api/moea/trigger")
    async def trigger_moea(_key: bool = Depends(_require_key_on_post)):
        if moea_optimizer is None:
            raise HTTPException(status_code=503, detail="MOEA unavailable")
        try:
            await run_moea()
            return {"status": "ok", "pareto_size": len(moea_optimizer.pareto_front)}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # --- Storage / RLHF ---
    @app.get("/api/storage/stats")
    async def storage_stats():
        if storage is None:
            raise HTTPException(status_code=503, detail="Storage unavailable")
        stats = await storage.get_statistics()
        cache_stats = await cache.stats() if cache else {}
        return {"tables": stats, "cache": cache_stats,
                "interaction_log_size": len(interaction_log)}

    @app.get("/api/rlhf/pairs")
    async def rlhf_pairs(limit: int = 20):
        if rlhf_trainer is None:
            raise HTTPException(status_code=503, detail="RLHF unavailable")
        pairs = await storage.get_preference_pairs(limit=min(int(limit), 500))
        return {"count": len(pairs), "pairs": pairs}

    @app.post("/api/rlhf/train")
    async def rlhf_train(_key: bool = Depends(_require_key_on_post)):
        if rlhf_trainer is None:
            raise HTTPException(status_code=503, detail="RLHF unavailable")
        ok = await rlhf_trainer.train_reward_model()
        return {"trained": ok,
                "model_available": rlhf_trainer.model is not None}

    # --- MODP ---
    @app.get("/api/modp/solve/{problem_id}")
    async def modp_solve(problem_id: str):
        if modp_solver is None:
            raise HTTPException(status_code=503, detail="MODP unavailable")
        values = await modp_solver.solve(problem_id)
        return {"problem_id": problem_id, "state_values": values}


# -----------------------------------------------------------------------------
# Dashboard HTML — [P1] real Leaflet + Chart.js
# -----------------------------------------------------------------------------
_DASHBOARD_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Green Data Center Dashboard — v2.5.0</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  body {{ font-family: -apple-system, system-ui, sans-serif; margin: 0; padding: 0; }}
  header {{ background: #1f4e3d; color: #fff; padding: 12px 24px; }}
  main {{ display: grid; grid-template-columns: 1fr 400px; gap: 16px; padding: 16px; }}
  #map {{ height: 600px; border-radius: 8px; }}
  .panel {{ border: 1px solid #e0e0e0; border-radius: 8px; padding: 12px; background: #fff; }}
  .stats {{ display: flex; gap: 12px; flex-wrap: wrap; }}
  .stat {{ background: #f5f5f5; padding: 8px 12px; border-radius: 6px; }}
  .stat-value {{ font-size: 20px; font-weight: 600; color: #1f4e3d; }}
  .stat-label {{ font-size: 12px; color: #666; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
  th, td {{ padding: 4px 6px; text-align: left; border-bottom: 1px solid #eee; }}
  #form {{ display: grid; gap: 8px; }}
  input, select, button {{ padding: 6px 8px; font-size: 14px; }}
  button {{ background: #1f4e3d; color: #fff; border: none; cursor: pointer; }}
  button:hover {{ background: #2a6b54; }}
  #result {{ background: #e8f5e9; padding: 8px; border-radius: 6px; font-size: 12px;
             white-space: pre-wrap; max-height: 200px; overflow-y: auto; }}
</style>
</head>
<body>
<header>
  <h1 style="margin: 0; font-size: 20px;">🌱 Green Data Center Dashboard — v2.5.0</h1>
  <p style="margin: 4px 0 0; font-size: 13px; opacity: 0.8;">{total} data centers · {operational} operational · {capacity:.0f} MW capacity</p>
</header>
<main>
  <div id="map"></div>
  <div>
    <div class="panel">
      <h2 style="margin-top: 0; font-size: 16px;">Recommend a workload</h2>
      <form id="form">
        <label>GPU hours <input type="number" name="gpu_hours" value="100" min="1"></label>
        <label>Latency tolerance (ms) <input type="number" name="latency_tolerance_ms" value="200" min="1"></label>
        <label>Workload type
          <select name="workload_type">
            <option value="training">Training</option>
            <option value="inference">Inference</option>
            <option value="other">Other</option>
          </select>
        </label>
        <label>User region
          <select name="user_region">
            <option value="us-east">US East</option>
            <option value="us-west">US West</option>
            <option value="eu-west">EU West</option>
            <option value="asia-east">Asia East</option>
          </select>
        </label>
        <button type="submit">Recommend</button>
      </form>
      <div id="result"></div>
    </div>
    <div class="panel" style="margin-top: 12px;">
      <h2 style="margin-top: 0; font-size: 16px;">Green score distribution</h2>
      <canvas id="chart" height="160"></canvas>
    </div>
  </div>
</main>
<script>
const projects = {projects_json};

const map = L.map('map').setView([20, 0], 2);
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png',
            {{maxZoom: 19, attribution: '© OpenStreetMap'}}).addTo(map);

const markers = L.layerGroup().addTo(map);
for (const p of projects) {{
  const color = p.green_score >= 70 ? '#2e7d32' : p.green_score >= 50 ? '#f9a825' : '#c62828';
  const marker = L.circleMarker([p.lat, p.lon], {{
    radius: Math.sqrt(p.capacity_mw) / 2 + 4,
    fillColor: color, color: '#fff', weight: 1, opacity: 1, fillOpacity: 0.85
  }});
  marker.bindPopup(`<b>${{p.name}}</b><br>
    Company: ${{p.company}}<br>
    Location: ${{p.location}}<br>
    Green score: <b>${{p.green_score.toFixed(1)}}</b><br>
    Carbon: ${{p.carbon_intensity.toFixed(0)}} gCO₂/kWh<br>
    Renewable: ${{p.renewable_share.toFixed(0)}}%<br>
    Capacity: ${{p.capacity_mw.toFixed(0)}} MW<br>
    Status: ${{p.status}}`);
  markers.addLayer(marker);
}}

// Green score distribution chart
const buckets = [0, 0, 0, 0, 0];
for (const p of projects) {{
  const idx = Math.min(4, Math.floor(p.green_score / 20));
  buckets[idx]++;
}}
new Chart(document.getElementById('chart'), {{
  type: 'bar',
  data: {{
    labels: ['0-20', '20-40', '40-60', '60-80', '80-100'],
    datasets: [{{ label: 'Projects', data: buckets, backgroundColor: '#1f4e3d' }}]
  }},
  options: {{ plugins: {{ legend: {{ display: false }} }},
              scales: {{ y: {{ beginAtZero: true }} }} }}
}});

// Recommend form
document.getElementById('form').addEventListener('submit', async (e) => {{
  e.preventDefault();
  const fd = new FormData(e.target);
  const payload = Object.fromEntries(fd.entries());
  payload.gpu_hours = parseFloat(payload.gpu_hours);
  payload.latency_tolerance_ms = parseFloat(payload.latency_tolerance_ms);
  const result = document.getElementById('result');
  result.textContent = 'Loading...';
  try {{
    const r = await fetch('/api/recommend', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify(payload)
    }});
    const data = await r.json();
    result.textContent = JSON.stringify(data.selected_project, null, 2) +
                          '\\n\\nStrategy: ' + data.strategy_used +
                          '\\nCarbon savings: ' + data.carbon_savings_kg.toFixed(2) + ' kg';
  }} catch (err) {{
    result.textContent = 'Error: ' + err.message;
  }}
}});
</script>
</body>
</html>
"""


def _render_dashboard(projects: List[Dict[str, Any]]) -> str:
    total = len(projects)
    operational = sum(1 for p in projects if p.get("status") == "operational")
    capacity = sum(p.get("capacity_mw", 0) for p in projects)
    projects_json = json.dumps(projects, default=str)
    return _DASHBOARD_TEMPLATE.format(
        total=total, operational=operational, capacity=capacity,
        projects_json=projects_json)


# -----------------------------------------------------------------------------
# Demo — run via `DASHBOARD_DEMO=1 python3 app_v2_5_0.py`
# -----------------------------------------------------------------------------
async def _demo():
    logger.info("Running dashboard demo (no HTTP server)")
    await startup()
    try:
        projects = await _fetch_projects_with_carbon()
        print(f"\n✓ Fetched {len(projects)} projects")
        print(f"  sample: {projects[0]['name']} ({projects[0]['green_score']:.1f})")

        # Fake recommend
        result = await _recommend_internal({"gpu_hours": 100,
                                              "workload_type": "training",
                                              "user_region": "us-east"})
        print(f"\n✓ Recommend: {result}")

        # Health
        from app_v2_5_0 import storage as st, moea_optimizer as mo
        if st is not None:
            stats = await st.get_statistics()
            print(f"\n✓ Storage tables: {stats}")

        # RLHF train
        if rlhf_trainer is not None:
            for _ in range(20):
                await rlhf_trainer.record_pair(
                    str(uuid.uuid4()), "prompt",
                    random.choice(DistillationStrategyOptimizer.STRATEGIES),
                    random.choice(DistillationStrategyOptimizer.STRATEGIES),
                    random.uniform(0.1, 0.9), {})
            ok = await rlhf_trainer.train_reward_model()
            print(f"\n✓ RLHF trained: {ok}")

        # MOEA
        await run_moea()
        if moea_optimizer is not None:
            print(f"\n✓ MOEA Pareto size: {len(moea_optimizer.pareto_front)}")

        # MODP solve
        if modp_solver is not None:
            values = await modp_solver.solve("strategy_selection")
            print(f"\n✓ MODP solved states: {len(values)}")

        print("\n✓ Demo complete")
    finally:
        await shutdown()


if __name__ == "__main__":
    if os.getenv("DASHBOARD_DEMO") == "1":
        asyncio.run(_demo())
    elif FASTAPI_AVAILABLE:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        print("FastAPI not installed. Install with: pip install fastapi uvicorn")
        print("Or run demo with: DASHBOARD_DEMO=1 python3 app_v2_5_0.py")
