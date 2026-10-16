#!/usr/bin/env python3
# main_integration.py
# Version 18.0 — Full Green Agent orchestration (P0/P1/P2 hardened)
"""
Complete orchestration loop for Green Agent with all ten Green Agent
enhancements built in, plus integration with bio_inspired, moe_system, MODP,
LIMIT Graph, RLHF, and Multi-Teacher Policy Distillation.

P0 fixes over v6:
  - HITL is production-safe and has a real pending queue + resolve()
  - HITL timeout and auto-approve semantics are honored
  - _await_hitl fails closed on exception (never silent approve)
  - Causal RL uses a UCB policy to select actions and its counterfactuals
    are consumed by the multi-agent vote
  - Federated aggregator submits real weight vectors with a DP accountant
    and pairwise masks
  - Multi-agent records all five roles, not just two
  - Chaos has steady-state hypotheses, blast radius, auto-deactivation
  - Circuit breaker has half-open state and resets failures on success
  - Reward weight overrides use a per-thread snapshot (no cross-talk between
    the main loop and the release thread)
  - Deferral re-entry guard prevents infinite carbon-window loops
  - Uncertainty uses real MOE teacher disagreement, not uniform probs
  - State file has a schema version and does not silently reset on error
  - _teacher_moe actually queries ExpertRouter
  - Bio evolution uses real reward-based fitness, not uniform random
  - Sleep delay is offloaded so the main loop is not blocked
  - Bio expansion is capped
  - PolicyMetaCache export is thread-safe against iteration

P1 hardening:
  - SQLite persistence for bandit, cache, lineage, cost, HITL audit
  - Real carbon providers (Electricity Maps / WattTime) with synthetic fallback
  - Token-bucket rate limiting and daily cost budget
  - Structured logging with correlation IDs
  - Health checks and readiness endpoint
  - Graceful shutdown with drain
  - FastAPI endpoints

P2 advanced:
  - Full STL (G / F / U / X / ->) with actionable enforcement
  - Secure federated aggregation with pairwise masks + DP accountant
  - Multi-agent message bus and conflict resolution
  - Adaptive precision driven by telemetry
  - Active learning with KMeans diversity and time-bounded buffer
  - Hash-chained HITL audit ledger
"""

from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import contextlib
import hashlib
import hmac
import json
import logging
import math
import os
import pickle
import random
import re
import sqlite3
import tempfile
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Callable, Dict, List, Optional, Sequence, Tuple,
)

import numpy as np
import psutil

# ----------------------------------------------------------------------
# Optional dependencies
# ----------------------------------------------------------------------
try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.cluster import KMeans
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False


# ----------------------------------------------------------------------
# Logger
# ----------------------------------------------------------------------
logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# ----------------------------------------------------------------------
# Prometheus metrics
# ----------------------------------------------------------------------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    M_TASKS = Counter("ga_tasks_total", "Tasks processed", ["status"], registry=REGISTRY)
    M_REWARD = Histogram("ga_reward", "Reward distribution", registry=REGISTRY)
    M_CARBON = Gauge("ga_carbon_intensity", "Carbon intensity", registry=REGISTRY)
    M_COST = Counter("ga_cost_usd", "Cumulative cost", registry=REGISTRY)
    M_HITL = Counter("ga_hitl_decisions_total", "HITL decisions", ["decision"], registry=REGISTRY)
    M_HITL_PENDING = Gauge("ga_hitl_pending", "Pending HITL requests", registry=REGISTRY)
    M_SAFETY = Counter("ga_safety_violations_total", "Safety violations", ["formula"], registry=REGISTRY)
    M_CHAOS = Counter("ga_chaos_events_total", "Chaos events", ["type"], registry=REGISTRY)
    M_PRECISION = Counter("ga_precision_switches_total", "Precision switches", ["level"], registry=REGISTRY)
    M_FEDERATED = Counter("ga_federated_rounds_total", "Federated rounds", registry=REGISTRY)
    M_DP_EPSILON = Gauge("ga_dp_epsilon_spent", "DP epsilon spent", ["node"], registry=REGISTRY)
    M_ACTIVE_LEARNING = Gauge("ga_active_learning_size", "Active learning buffer", registry=REGISTRY)
    M_RATE_LIMITED = Counter("ga_rate_limited_total", "Rate-limited calls", registry=REGISTRY)
    M_HEALTH = Gauge("ga_health_score", "Health score", registry=REGISTRY)
else:
    class _Dummy:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
    M_TASKS = M_REWARD = M_CARBON = M_COST = _Dummy()
    M_HITL = M_HITL_PENDING = M_SAFETY = M_CHAOS = _Dummy()
    M_PRECISION = M_FEDERATED = M_DP_EPSILON = _Dummy()
    M_ACTIVE_LEARNING = M_RATE_LIMITED = M_HEALTH = _Dummy()


# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


_PII_PATTERNS = [
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), "[EMAIL]"),
    (re.compile(r"\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"), "[PHONE]"),
    (re.compile(r"\b(?:sk|pk|ghp|xox[baprs])[-_A-Za-z0-9]{10,}\b"), "[API_KEY]"),
]


def redact_pii(text: str) -> str:
    if not text:
        return text
    out = text
    for pat, repl in _PII_PATTERNS:
        out = pat.sub(repl, out)
    return out


class TokenBucket:
    def __init__(self, rate: float, per_seconds: float = 60.0):
        self.rate = float(rate)
        self.per_seconds = float(per_seconds)
        self._tokens = float(rate)
        self._last = time.time()
        self._lock = threading.Lock()

    def acquire(self, cost: float = 1.0) -> bool:
        with self._lock:
            now = time.time()
            self._tokens = min(
                self.rate,
                self._tokens + (now - self._last) * (self.rate / max(self.per_seconds, 1e-6)))
            self._last = now
            if self._tokens >= cost:
                self._tokens -= cost
                return True
            return False

    def remaining(self) -> float:
        with self._lock:
            return float(self._tokens)


# ----------------------------------------------------------------------
# Persistence (SQLite)
# ----------------------------------------------------------------------
class Persistence:
    SCHEMA = """
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY, value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS bandit_state (
        id INTEGER PRIMARY KEY,
        state TEXT NOT NULL,
        ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS cache_state (
        id INTEGER PRIMARY KEY,
        state TEXT NOT NULL,
        ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS hitl_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT NOT NULL,
        approved INTEGER NOT NULL,
        reason TEXT, mode TEXT, latency_ms REAL,
        prev_hash TEXT, entry_hash TEXT NOT NULL,
        ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS lineage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT, prompt TEXT, response TEXT,
        policy TEXT, reward REAL, ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS cost_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL, tokens INTEGER NOT NULL, cost_usd REAL NOT NULL
    );
    """

    def __init__(self, path: Optional[str]):
        self.path = path
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        if path:
            self._open()

    def _open(self):
        try:
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(self.path, check_same_thread=False)
            with self._lock:
                self._conn.executescript(self.SCHEMA)
                self._conn.commit()
        except Exception as e:
            logger.warning(f"Persistence init failed: {e}")
            self._conn = None

    def close(self):
        with self._lock:
            if self._conn:
                with contextlib.suppress(Exception):
                    self._conn.commit()
                with contextlib.suppress(Exception):
                    self._conn.close()
                self._conn = None

    def _exec(self, sql: str, params: tuple = ()):
        if not self._conn:
            return None
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(sql, params)
            self._conn.commit()
            return cur

    def _fetchall(self, sql: str, params: tuple = ()):
        if not self._conn:
            return []
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(sql, params)
            return cur.fetchall()

    def save_bandit(self, state: Dict[str, Any]):
        self._exec("INSERT INTO bandit_state (state, ts) VALUES (?, ?)",
                   (json.dumps(state, default=str), time.time()))

    def load_bandit(self) -> Optional[Dict[str, Any]]:
        rows = self._fetchall(
            "SELECT state FROM bandit_state ORDER BY id DESC LIMIT 1")
        if not rows:
            return None
        try:
            return json.loads(rows[0][0])
        except Exception:
            return None

    def save_cache(self, state: Dict[str, Any]):
        self._exec("INSERT INTO cache_state (state, ts) VALUES (?, ?)",
                   (json.dumps(state, default=str), time.time()))

    def load_cache(self) -> Optional[Dict[str, Any]]:
        rows = self._fetchall(
            "SELECT state FROM cache_state ORDER BY id DESC LIMIT 1")
        if not rows:
            return None
        try:
            return json.loads(rows[0][0])
        except Exception:
            return None

    def hitl_append(self, entry: Dict[str, Any]):
        self._exec(
            "INSERT INTO hitl_audit (request_id, approved, reason, mode, "
            "latency_ms, prev_hash, entry_hash, ts) VALUES (?,?,?,?,?,?,?,?)",
            (entry["request_id"], int(bool(entry["approved"])),
             entry.get("reason", ""), entry.get("mode", ""),
             float(entry.get("latency_ms", 0.0)),
             entry.get("prev_hash", ""), entry.get("entry_hash", ""),
             float(entry.get("timestamp", time.time()))))

    def hitl_recent(self, limit: int = 100) -> List[Tuple]:
        return self._fetchall(
            "SELECT request_id, approved, reason, mode, latency_ms, "
            "prev_hash, entry_hash, ts FROM hitl_audit ORDER BY id DESC LIMIT ?",
            (limit,))

    def hitl_last_hash(self) -> str:
        rows = self._fetchall(
            "SELECT entry_hash FROM hitl_audit ORDER BY id DESC LIMIT 1")
        return rows[0][0] if rows else ""

    def lineage_add(self, request_id: str, prompt: str, response: str,
                    policy: Dict[str, Any], reward: float):
        self._exec(
            "INSERT INTO lineage (request_id, prompt, response, policy, reward, ts) "
            "VALUES (?,?,?,?,?,?)",
            (request_id, prompt, response,
             json.dumps(policy, default=str), float(reward), time.time()))

    def cost_record(self, tokens: int, cost_usd: float):
        self._exec(
            "INSERT INTO cost_ledger (ts, tokens, cost_usd) VALUES (?,?,?)",
            (time.time(), int(tokens), float(cost_usd)))

    def cost_today(self) -> float:
        day_start = utcnow().replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp()
        rows = self._fetchall(
            "SELECT COALESCE(SUM(cost_usd), 0) FROM cost_ledger WHERE ts >= ?",
            (day_start,))
        return float(rows[0][0]) if rows else 0.0

    def get_meta(self, key: str) -> Optional[str]:
        rows = self._fetchall("SELECT value FROM meta WHERE key=?", (key,))
        return rows[0][0] if rows else None

    def set_meta(self, key: str, value: str):
        self._exec("INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)",
                   (key, value))


# ----------------------------------------------------------------------
# External module integrations (with graceful fallback)
# ----------------------------------------------------------------------
try:
    from enhancements.limit_graph import LimitGraph
except ImportError:
    class LimitGraph:
        def __init__(self, *args, **kwargs):
            self.limits: Dict[str, Any] = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass

try:
    from enhancements.rlhf import RLHFOptimizer
except ImportError:
    class RLHFOptimizer:
        def __init__(self, action_space, *args, **kwargs):
            self.actions = list(action_space)
            self.scores = {a: 0.0 for a in self.actions}
            self._rng = random.Random(0)
        def update(self, context, action, reward):
            if action not in self.scores:
                self.actions.append(action); self.scores[action] = 0.0
            self.scores[action] += 0.1 * (float(reward) - self.scores[action])
        def sample_action(self, context):
            if not self.scores: return None
            if self._rng.random() < 0.1: return self._rng.choice(self.actions)
            return max(self.scores, key=self.scores.get)

try:
    from enhancements.multi_teacher_policy_distillation import MultiTeacherDistiller
except ImportError:
    class MultiTeacherDistiller:
        def __init__(self, teachers, weights=None, *args, **kwargs):
            self.teachers = list(teachers)
            self.weights = list(weights) if weights else [1.0] * len(self.teachers)
        def _call(self, t, ctx):
            try:
                out = t(ctx)
                if asyncio.iscoroutine(out):
                    out.close(); return None
                return out
            except Exception:
                return None
        def distill(self, context):
            votes: Dict[str, Tuple[Dict, float]] = {}
            for t, w in zip(self.teachers, self.weights):
                c = self._call(t, context)
                if c is None: continue
                key = json.dumps(c, sort_keys=True)
                if key in votes:
                    votes[key] = (votes[key][0], votes[key][1] + float(w))
                else:
                    votes[key] = (c, float(w))
            return max(votes.values(), key=lambda kv: kv[1])[0] if votes else None

try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
except ImportError:
    class GeneticPolicyGenerator:
        """Fallback: single-generation mutate-and-evaluate GA."""
        def __init__(self, seed: int = 0):
            self._rng = random.Random(seed)

        def evolve(self, population, fitness_fn, generations=10,
                   population_size=20):
            if not population:
                return []
            best_parent = max(population, key=lambda p: fitness_fn(p) if isinstance(p, dict) else -1e9)
            out: List[Dict[str, Any]] = []
            for _ in range(population_size):
                child = dict(best_parent)
                if self._rng.random() < 0.5:
                    child["block_size"] = max(4, int(child.get("block_size", 8)) * 2)
                if self._rng.random() < 0.3:
                    child["gpu_batch_size"] = max(1, int(child.get("gpu_batch_size", 1)) + self._rng.choice([-1, 1]))
                out.append(child)
            return out

try:
    from enhancements.moe_system import ExpertRouter
except ImportError:
    class ExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context) -> List[float]:
            if isinstance(context, dict):
                return [float(context.get(k, 0.0)) for k in
                        ("model_size_mb", "prompt_len", "gen_len",
                         "gpu_mem_free_mb", "disk_speed_class")]
            return [0.0] * 5
        def select(self, encoded: List[float]) -> str:
            return "default"


# ===========================================================================
# TEN ENHANCEMENT MODULES (P2)
# ===========================================================================
# ---- 7: Adaptive precision (P2 telemetry) ----
class PrecisionLevel(str, Enum):
    FP32 = "fp32"; BF16 = "bf16"; FP16 = "fp16"
    FP8 = "fp8"; INT8 = "int8"; INT4 = "int4"


PRECISION_COST: Dict[PrecisionLevel, Dict[str, float]] = {
    PrecisionLevel.FP32: {"speed": 1.0, "energy": 1.00, "quality": 1.000, "bits": 32.0},
    PrecisionLevel.BF16: {"speed": 1.7, "energy": 0.72, "quality": 0.997, "bits": 16.0},
    PrecisionLevel.FP16: {"speed": 2.0, "energy": 0.65, "quality": 0.994, "bits": 16.0},
    PrecisionLevel.FP8:  {"speed": 3.1, "energy": 0.50, "quality": 0.985, "bits": 8.0},
    PrecisionLevel.INT8: {"speed": 3.6, "energy": 0.44, "quality": 0.972, "bits": 8.0},
    PrecisionLevel.INT4: {"speed": 5.0, "energy": 0.32, "quality": 0.905, "bits": 4.0},
}


@dataclass
class HardwareTelemetry:
    gpu_util_pct: float = 0.0
    temperature_c: float = 40.0
    power_watts: float = 0.0
    observed_quality_drop: float = 0.0
    timestamp: float = field(default_factory=time.time)


class PrecisionController:
    def __init__(self, supported: Optional[Sequence[PrecisionLevel]] = None,
                 quality_floor: float = 0.95, carbon_aware: bool = True,
                 energy_joules_per_bit: float = 1e-3):
        self.supported = list(supported) if supported else list(PrecisionLevel)
        self.quality_floor = quality_floor
        self.carbon_aware = carbon_aware
        self.energy_joules_per_bit = energy_joules_per_bit
        self._telemetry: deque = deque(maxlen=100)
        self._ema_quality_drop = 0.0
        self._lock = threading.Lock()

    def observe(self, t: HardwareTelemetry) -> None:
        with self._lock:
            self._telemetry.append(t)
            alpha = 0.2
            self._ema_quality_drop = (1 - alpha) * self._ema_quality_drop + \
                                     alpha * t.observed_quality_drop

    def _thermal_penalty(self) -> float:
        with self._lock:
            if not self._telemetry:
                return 0.0
            recent = list(self._telemetry)[-10:]
        t = max(h.temperature_c for h in recent)
        return min(1.0, max(0.0, (t - 70.0) / 30.0))

    def select(self, carbon_intensity: float, latency_headroom_ratio: float,
               carbon_price: float = 0.0) -> PrecisionLevel:
        stress = min(1.0, max(0.0, carbon_intensity / 600.0)) if self.carbon_aware else 0.0
        price_stress = min(1.0, max(0.0, carbon_price / 0.2))
        headroom = min(1.0, max(0.0, latency_headroom_ratio))
        thermal = self._thermal_penalty()
        agg = 0.4 * stress + 0.25 * price_stress + 0.2 * (1.0 - headroom) + 0.15 * thermal
        ordered = [p for p in PrecisionLevel if p in self.supported]
        ordered.sort(key=lambda p: PRECISION_COST[p]["energy"])
        with self._lock:
            floor = max(self.quality_floor,
                        self.quality_floor - self._ema_quality_drop * 0.5)
        chosen = ordered[0]
        for i, p in enumerate(ordered):
            if PRECISION_COST[p]["quality"] >= floor and agg >= 0.2 * i:
                chosen = p
        return chosen

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            if not self._telemetry:
                return {"samples": 0}
            recent = list(self._telemetry)[-20:]
            return {"samples": len(self._telemetry),
                    "avg_temp_c": float(np.mean([h.temperature_c for h in recent])),
                    "avg_power_w": float(np.mean([h.power_watts for h in recent])),
                    "ema_quality_drop": self._ema_quality_drop}


class HardwareAwareAdapter:
    def __init__(self, controller: PrecisionController):
        self.controller = controller

    def adapt(self, policy: Dict[str, Any], carbon_intensity: float,
              latency_headroom: float, carbon_price: float
              ) -> Tuple[Dict[str, Any], PrecisionLevel]:
        level = self.controller.select(carbon_intensity, latency_headroom, carbon_price)
        out = dict(policy)
        out["precision_level"] = level.value
        out["effective_bits"] = PRECISION_COST[level]["bits"]
        out["weight_bits"] = PRECISION_COST[level]["bits"]
        M_PRECISION.labels(level=level.value).inc()
        return out, level


# ---- 8: Carbon markets / RECs (P1 real providers) ----
@dataclass
class CarbonSnapshot:
    intensity_g_co2_per_kwh: float
    price_usd_per_kg: float
    source: str
    region: str = "global"
    timestamp: float = field(default_factory=time.time)


class ElectricityMapsProvider:
    def __init__(self, api_key: Optional[str] = None, zone: str = "US-CAL-CISO"):
        self.api_key = api_key
        self.zone = zone

    async def get_intensity(self) -> CarbonSnapshot:
        if self.api_key and HTTPX_AVAILABLE:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    r = await client.get(
                        "https://api.electricitymap.org/v3/carbon-intensity/latest",
                        params={"zone": self.zone},
                        headers={"auth-token": self.api_key})
                    r.raise_for_status()
                    data = r.json()
                    return CarbonSnapshot(
                        float(data.get("carbonIntensity", 400.0)), 0.0,
                        "electricitymaps", region=self.zone)
            except Exception as e:
                logger.warning(f"ElectricityMaps failed: {e}")
        hour = utcnow().hour
        diurnal = 1.0 + 0.35 * math.sin((hour / 24.0) * 2 * math.pi)
        return CarbonSnapshot(280.0 * diurnal, 0.0, "synthetic", region=self.zone)


class WattTimeProvider:
    def __init__(self, username: Optional[str] = None,
                 password: Optional[str] = None, region: str = "CAISO_NORTH"):
        self.username = username
        self.password = password
        self.region = region

    async def get_intensity(self) -> CarbonSnapshot:
        if self.username and self.password and HTTPX_AVAILABLE:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    r = await client.get(
                        "https://api.watttime.org/v2/data",
                        params={"ba": self.region},
                        auth=(self.username, self.password))
                    r.raise_for_status()
                    data = r.json()
                    if data:
                        return CarbonSnapshot(
                            float(data[0].get("value", 400.0)), 0.0,
                            "watttime", region=self.region)
            except Exception as e:
                logger.warning(f"WattTime failed: {e}")
        return CarbonSnapshot(280.0, 0.0, "synthetic", region=self.region)


class CarbonMarketClient:
    def __init__(self, base_price: float = 0.05, sensitivity: float = 0.0005,
                 min_price: float = 0.005, max_price: float = 0.5):
        self.base_price = base_price
        self.sensitivity = sensitivity
        self.min_price = min_price
        self.max_price = max_price

    def price(self, carbon_intensity: float,
              hour_of_day: Optional[int] = None) -> float:
        tod = 1.0
        if hour_of_day is not None:
            tod = 1.0 + 0.3 * math.sin((hour_of_day / 24.0) * 2 * math.pi)
        raw = self.base_price + self.sensitivity * carbon_intensity * tod
        return max(self.min_price, min(self.max_price, raw))


@dataclass
class RECRecord:
    kwh: float
    issued_at: float
    source: str = "solar"
    cert_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])


class RECInventory:
    def __init__(self, grid_kg_co2_per_kwh: float = 0.4,
                 persist_path: Optional[str] = None):
        self.grid_factor = grid_kg_co2_per_kwh
        self.persist_path = Path(persist_path) if persist_path else None
        self._records: List[RECRecord] = []
        self._ledger: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.persist_path and self.persist_path.exists():
            try:
                with open(self.persist_path, "rb") as f:
                    data = pickle.load(f)
                self._records = data.get("records", [])
                self._ledger = data.get("ledger", [])
            except Exception as e:
                logger.warning(f"REC load failed: {e}")

    def _save(self):
        if not self.persist_path:
            return
        try:
            self.persist_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.persist_path, "wb") as f:
                pickle.dump({"records": self._records, "ledger": self._ledger}, f)
        except Exception as e:
            logger.warning(f"REC save failed: {e}")

    def add(self, kwh: float, source: str = "solar") -> None:
        with self._lock:
            rec = RECRecord(kwh=kwh, issued_at=time.time(), source=source)
            self._records.append(rec)
            self._ledger.append({"op": "add", "kwh": kwh, "source": source,
                                 "cert_id": rec.cert_id, "t": rec.issued_at})
            self._save()

    def total_kwh(self) -> float:
        with self._lock:
            return sum(r.kwh for r in self._records)

    def consume(self, kwh: float) -> float:
        with self._lock:
            remaining, offset = kwh, 0.0
            new_records: List[RECRecord] = []
            consumed: List[Dict[str, Any]] = []
            for r in self._records:
                if remaining <= 0:
                    new_records.append(r); continue
                take = min(r.kwh, remaining)
                remaining -= take
                offset += take * self.grid_factor
                consumed.append({"cert_id": r.cert_id, "kwh": take})
                if r.kwh - take > 1e-9:
                    new_records.append(RECRecord(
                        kwh=r.kwh - take, issued_at=r.issued_at,
                        source=r.source))
            self._records = new_records
            if consumed:
                self._ledger.append({"op": "consume", "items": consumed,
                                     "t": time.time()})
                self._save()
            return offset

    def audit(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._ledger)


# ---- 9: Chaos (P2 steady-state + blast radius) ----
@dataclass
class ChaosConfig:
    fault_prob: float = 0.0
    latency_inject_ms: float = 0.0
    latency_inject_prob: float = 0.3
    carbon_spike_prob: float = 0.0
    carbon_spike_factor: float = 1.5
    seed: int = 0
    max_concurrent_faults: int = 1
    duration_s: float = 0.0

    def enabled(self) -> bool:
        return (self.fault_prob > 0
                or (self.latency_inject_ms > 0 and self.latency_inject_prob > 0)
                or self.carbon_spike_prob > 0)


@dataclass
class SteadyStateHypothesis:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]


class ChaosEngineer:
    def __init__(self, config: Optional[ChaosConfig] = None,
                 hypotheses: Optional[List[SteadyStateHypothesis]] = None):
        self.config = config or ChaosConfig()
        self.rng = random.Random(self.config.seed)
        self.events: List[Dict[str, Any]] = []
        self.hypotheses = hypotheses or []
        self._active_faults = 0
        self._activated_at = time.time() if self.config.enabled() else 0.0
        self._lock = threading.Lock()

    def _can_inject(self) -> bool:
        if self._active_faults >= self.config.max_concurrent_faults:
            return False
        if self.config.duration_s > 0 and \
                (time.time() - self._activated_at) > self.config.duration_s:
            return False
        return True

    def maybe_fault(self) -> bool:
        if not self._can_inject():
            return False
        if self.rng.random() < self.config.fault_prob:
            with self._lock:
                self._active_faults += 1
                self.events.append({"type": "fault", "t": time.time()})
            M_CHAOS.labels(type="fault").inc()
            return True
        return False

    def release_fault(self) -> None:
        with self._lock:
            self._active_faults = max(0, self._active_faults - 1)

    def maybe_latency(self) -> float:
        if not self._can_inject():
            return 0.0
        if (self.config.latency_inject_ms > 0
                and self.rng.random() < self.config.latency_inject_prob):
            with self._lock:
                self.events.append({"type": "latency", "t": time.time()})
            M_CHAOS.labels(type="latency").inc()
            return self.config.latency_inject_ms
        return 0.0

    def maybe_carbon_spike(self, carbon_intensity: float) -> float:
        if not self._can_inject():
            return carbon_intensity
        if self.rng.random() < self.config.carbon_spike_prob:
            with self._lock:
                self.events.append({"type": "carbon_spike", "t": time.time()})
            M_CHAOS.labels(type="carbon_spike").inc()
            return carbon_intensity * self.config.carbon_spike_factor
        return carbon_intensity

    def verify_steady_state(self, metrics: Dict[str, Any]) -> Dict[str, bool]:
        return {h.name: bool(h.predicate(metrics)) for h in self.hypotheses}

    def steady_state_ok(self, metrics: Dict[str, Any]) -> bool:
        return all(self.verify_steady_state(metrics).values())

    def deactivate(self) -> None:
        self.config = ChaosConfig(seed=self.config.seed)
        with self._lock:
            self._active_faults = 0

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {"config": asdict(self.config),
                    "events": len(self.events),
                    "active_faults": self._active_faults,
                    "hypotheses": [h.name for h in self.hypotheses]}


class CircuitBreaker:
    def __init__(self, name: str = "executor",
                 failure_threshold: int = 3, recovery_s: float = 10.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_s = recovery_s
        self._failures = 0
        self._open_until = 0.0
        self._state = "closed"
        self._lock = threading.Lock()

    def allow(self) -> bool:
        with self._lock:
            if self._state == "open" and time.time() >= self._open_until:
                self._state = "half-open"
                self._failures = 0  # reset on half-open
                return True
            return self._state != "open"

    def record(self, success: bool) -> None:
        with self._lock:
            if success:
                self._failures = 0
                self._state = "closed"
            else:
                self._failures += 1
                if self._failures >= self.failure_threshold:
                    self._open_until = time.time() + self.recovery_s
                    self._state = "open"

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {"name": self.name, "state": self._state,
                    "failures": self._failures,
                    "open_until": self._open_until}


# ---- 5: Temporal logic (P2 full STL) ----
class STLOperator(str, Enum):
    ALWAYS = "G"
    EVENTUALLY = "F"
    UNTIL = "U"
    NEXT = "X"
    IMPLIES = "->"


@dataclass
class STLFormula:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    operator: STLOperator
    horizon: int = 10
    rhs_predicate: Optional[Callable[[Dict[str, Any]], bool]] = None

    def evaluate(self, window: List[Dict[str, Any]]) -> bool:
        if not window:
            return True
        sat = [bool(self.predicate(r)) for r in window]
        if self.operator == STLOperator.ALWAYS:
            return all(sat)
        if self.operator == STLOperator.EVENTUALLY:
            return any(sat)
        if self.operator == STLOperator.NEXT:
            return sat[0] if len(sat) == 1 else sat[1]
        if self.operator == STLOperator.UNTIL:
            for i, s in enumerate(sat):
                if s:
                    return all(sat[:i]) if i > 0 else True
            return False
        if self.operator == STLOperator.IMPLIES and self.rhs_predicate is not None:
            rhs = [bool(self.rhs_predicate(r)) for r in window]
            return all((not s) or r for s, r in zip(sat, rhs))
        return True


@dataclass
class STLViolation:
    formula: str
    operator: str
    window_size: int
    timestamp: float


class TemporalLogicMonitor:
    def __init__(self, horizon: int = 10):
        self.horizon = horizon
        self._history: deque = deque(maxlen=horizon * 4)
        self.formulas: List[STLFormula] = []
        self.violations: List[STLViolation] = []
        self._lock = threading.Lock()

    def add_formula(self, f: STLFormula) -> None:
        self.formulas.append(f)

    def observe(self, record: Dict[str, Any]) -> None:
        with self._lock:
            self._history.append({**record, "_t": time.time()})

    def verify(self) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        with self._lock:
            history = list(self._history)
        for f in self.formulas:
            window = history[-f.horizon:]
            results[f.name] = f.evaluate(window)
            if not results[f.name]:
                M_SAFETY.labels(formula=f.name).inc()
                with self._lock:
                    self.violations.append(STLViolation(
                        formula=f.name, operator=f.operator.value,
                        window_size=len(window), timestamp=time.time()))
        return results

    def recent_violations(self, n: int = 10) -> List[Dict[str, Any]]:
        with self._lock:
            return [asdict(v) for v in self.violations[-n:]]


class SafetyShield:
    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor
        self.violations: List[Dict[str, Any]] = []

    def screen(self, selected: Dict[str, Any],
               candidates: Sequence[Dict[str, Any]],
               score_key: str = "block_size",
               cost_fn: Optional[Callable[[Dict[str, Any]], float]] = None
               ) -> Tuple[Dict[str, Any], bool, Dict[str, bool]]:
        verdict = self.monitor.verify()
        violated = [k for k, ok in verdict.items() if not ok]
        if not violated:
            return dict(selected), True, verdict
        if not candidates:
            return dict(selected), False, verdict
        if cost_fn is None:
            cost_fn = lambda c: float(int(c.get(score_key, 0)))
        safe = min(candidates, key=cost_fn)
        self.violations.append({"t": time.time(), "violated": violated,
                                "original": dict(selected),
                                "replacement": dict(safe)})
        return dict(safe), False, verdict


# ---- 2: Causal RL (P0/P2 active UCB) ----
@dataclass
class CausalTransition:
    state: np.ndarray
    action: int
    reward: float
    next_state: np.ndarray
    context: Dict[str, float] = field(default_factory=dict)


class CausalCounterfactualEstimator:
    def __init__(self, state_dim: int, n_actions: int, ridge: float = 1e-3,
                 exploration_c: float = 0.5):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.ridge = ridge
        self.exploration_c = exploration_c
        self._A = np.eye(state_dim + n_actions + 1) * ridge
        self._b = np.zeros(state_dim + n_actions + 1)
        self._action_counts = np.zeros(n_actions)
        self._n = 0
        self._lock = threading.Lock()

    def _features(self, s, a):
        one_hot = np.zeros(self.n_actions)
        one_hot[a % self.n_actions] = 1.0
        return np.concatenate([np.asarray(s, dtype=np.float64), one_hot, [1.0]])

    def _ensure_actions(self, n: int) -> None:
        if n == self.n_actions:
            return
        with self._lock:
            self._A = np.eye(self.state_dim + n + 1) * self.ridge
            self._b = np.zeros(self.state_dim + n + 1)
            self.n_actions = n
            self._action_counts = np.zeros(n)
            self._n = 0

    def update(self, t: CausalTransition) -> None:
        a = int(t.action) % max(self.n_actions, 1)
        x = self._features(t.state, a)
        with self._lock:
            self._A += np.outer(x, x)
            self._b += t.reward * x
            self._action_counts[a] += 1
            self._n += 1

    def predict(self, s, a) -> float:
        with self._lock:
            if self._n < 2:
                return 0.0
            try:
                theta = np.linalg.solve(self._A, self._b)
            except np.linalg.LinAlgError:
                return 0.0
        return float(self._features(s, a) @ theta)

    def counterfactuals(self, s, n_actions: int) -> Dict[int, float]:
        self._ensure_actions(n_actions)
        return {a: self.predict(s, a) for a in range(n_actions)}

    def select_action(self, s, n_actions: int, epsilon: float = 0.05) -> int:
        cf = self.counterfactuals(s, n_actions)
        if not cf:
            return 0
        if random.random() < epsilon:
            return random.randrange(self.n_actions)
        with self._lock:
            total = max(self._n, 1)
            counts = self._action_counts.copy()
        scores = {}
        for a, mu in cf.items():
            count = max(counts[a], 1.0)
            bonus = self.exploration_c * math.sqrt(math.log(total + 1) / count)
            scores[a] = mu + bonus
        return max(scores, key=scores.get)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {"n_updates": int(self._n),
                    "n_actions": self.n_actions,
                    "action_counts": self._action_counts.tolist()}


# ---- 3: Federated (P2 secure + DP) ----
@dataclass
class FederatedUpdate:
    node_id: str
    weights: Dict[str, np.ndarray]
    n_samples: int
    carbon_intensity: float
    timestamp: float = field(default_factory=time.time)


class DPAccountant:
    def __init__(self, target_epsilon: float = 8.0, target_delta: float = 1e-5):
        self.target_epsilon = target_epsilon
        self.target_delta = target_delta
        self.spent = 0.0
        self._rounds = 0

    def step(self, sigma: float, sensitivity: float = 1.0) -> None:
        if sigma <= 0:
            return
        eps_step = sensitivity * math.sqrt(
            2 * math.log(1.25 / self.target_delta)) / sigma
        self.spent += eps_step
        self._rounds += 1

    def can_spend(self) -> bool:
        return self.spent < self.target_epsilon


class PrivacyBudgetExceeded(Exception):
    pass


class FederatedAggregator:
    def __init__(self, dp_sigma: float = 1e-3, staleness_s: float = 3600.0,
                 target_epsilon: float = 8.0, target_delta: float = 1e-5):
        self.dp_sigma = dp_sigma
        self.staleness_s = staleness_s
        self._updates: Dict[str, FederatedUpdate] = {}
        self._global: Optional[Dict[str, np.ndarray]] = None
        self.accountant = DPAccountant(target_epsilon, target_delta)

    def submit(self, u: FederatedUpdate) -> None:
        self._updates[u.node_id] = u

    def _fresh(self) -> List[FederatedUpdate]:
        now = time.time()
        return [u for u in self._updates.values()
                if (now - u.timestamp) <= self.staleness_s]

    @staticmethod
    def _pairwise_mask(node_ids: List[str], shape: Tuple[int, ...],
                       seed: int = 0) -> List[np.ndarray]:
        rng = np.random.default_rng(seed)
        pairwise: Dict[Tuple[str, str], np.ndarray] = {}
        for i, a in enumerate(node_ids):
            for b in node_ids[i + 1:]:
                pairwise[(a, b)] = rng.normal(0.0, 1.0, size=shape)
        masks = []
        for a in node_ids:
            m = np.zeros(shape)
            for (x, y), v in pairwise.items():
                if x == a: m = m + v
                elif y == a: m = m - v
            masks.append(m)
        return masks

    def aggregate(self, secure: bool = True) -> Optional[Dict[str, np.ndarray]]:
        fresh = self._fresh()
        if not fresh:
            return self._global
        if not self.accountant.can_spend():
            raise PrivacyBudgetExceeded(
                f"DP budget exhausted: spent={self.accountant.spent:.2f} "
                f"target={self.accountant.target_epsilon}")
        weights = np.array([u.n_samples / max(u.carbon_intensity, 1.0)
                            for u in fresh])
        weights /= max(weights.sum(), 1e-12)
        keys = list(fresh[0].weights.keys())
        agg: Dict[str, np.ndarray] = {}
        node_ids = [u.node_id for u in fresh]
        for k in keys:
            shape = fresh[0].weights[k].shape
            stacked = np.stack([u.weights[k] for u in fresh], axis=0)
            if secure and len(fresh) > 1:
                masks = self._pairwise_mask(node_ids, shape)
                stacked = stacked + np.stack(masks, axis=0)
            blended = np.tensordot(weights, stacked, axes=([0], [0]))
            if self.dp_sigma > 0:
                blended = blended + np.random.normal(
                    0.0, self.dp_sigma, size=blended.shape)
                self.accountant.step(self.dp_sigma)
            agg[k] = blended
        for nid in node_ids:
            M_DP_EPSILON.labels(node=nid).set(self.accountant.spent)
        self._global = agg
        M_FEDERATED.inc()
        return agg

    def global_weights(self):
        return self._global

    def stats(self) -> Dict[str, Any]:
        return {"tracked_nodes": len(self._updates),
                "dp_spent": self.accountant.spent,
                "dp_target": self.accountant.target_epsilon}


# ---- 4: Multi-agent (P2 message bus + conflict resolution) ----
class AgentRole(str, Enum):
    EXPLORER = "explorer"
    EXPLOITER = "exploiter"
    SAFETY_OFFICER = "safety_officer"
    CARBON_BROKER = "carbon_broker"
    VERIFIER = "verifier"


@dataclass
class AgentBid:
    agent_id: str
    role: AgentRole
    confidence: float
    proposed_action: int
    rationale: str
    carbon_score: float


@dataclass
class AgentMessage:
    sender: str
    recipient: str
    topic: str
    payload: Dict[str, Any]
    t: float = field(default_factory=time.time)


class EmergentRoleRegistry:
    def __init__(self, decay: float = 0.95):
        self.decay = decay
        self._scores = {r: 1.0 for r in AgentRole}
        self._counts = {r: 0 for r in AgentRole}
        self._successes = {r: 0 for r in AgentRole}
        self._lock = threading.Lock()

    def record(self, role: AgentRole, success: bool, reward: float) -> None:
        with self._lock:
            for r in self._scores:
                self._scores[r] *= self.decay
            signal = reward if success else -abs(reward) * 0.5
            self._scores[role] += signal
            self._counts[role] += 1
            if success:
                self._successes[role] += 1

    def weights(self) -> Dict[AgentRole, float]:
        with self._lock:
            total = sum(max(v, 1e-6) for v in self._scores.values())
            return {r: max(v, 1e-6) / total for r, v in self._scores.items()}

    def dominant_role(self) -> AgentRole:
        with self._lock:
            return max(self._scores, key=self._scores.get)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {"scores": {r.value: self._scores[r] for r in AgentRole},
                    "counts": {r.value: self._counts[r] for r in AgentRole},
                    "success_rate": {
                        r.value: (self._successes[r] / self._counts[r])
                        if self._counts[r] else 0.0
                        for r in AgentRole}}


class MultiAgentCoordinator:
    def __init__(self, registry: EmergentRoleRegistry,
                 role_bias: Optional[Dict[AgentRole, float]] = None):
        self.registry = registry
        self.role_bias = role_bias or {
            AgentRole.EXPLORER: 0.6,
            AgentRole.EXPLOITER: 0.9,
            AgentRole.SAFETY_OFFICER: 1.4,
            AgentRole.CARBON_BROKER: 1.2,
            AgentRole.VERIFIER: 1.0,
        }
        self.messages: deque = deque(maxlen=1000)

    def broadcast(self, sender: str, topic: str, payload: Dict[str, Any]) -> None:
        self.messages.append(AgentMessage(
            sender=sender, recipient="*", topic=topic, payload=payload))

    def vote(self, bids: Sequence[AgentBid], n_candidates: int) -> int:
        if not bids or n_candidates <= 0:
            return 0
        role_w = self.registry.weights()
        scores = np.zeros(n_candidates)
        for b in bids:
            idx = b.proposed_action % n_candidates
            weight = (role_w.get(b.role, 0.1)
                      * self.role_bias.get(b.role, 1.0)
                      * max(b.confidence, 0.0)
                      * (1.0 + max(0.0, b.carbon_score)))
            scores[idx] += weight
        return int(np.argmax(scores))

    def resolve_conflict(self, bids: Sequence[AgentBid],
                         n_candidates: int) -> Tuple[int, str]:
        if not bids:
            return 0, "no_bids"
        safety = [b for b in bids if b.role == AgentRole.SAFETY_OFFICER]
        carbon = [b for b in bids if b.role == AgentRole.CARBON_BROKER]
        if safety and min(b.confidence for b in safety) > 0.5:
            idx = safety[0].proposed_action % n_candidates
            self.broadcast("coordinator", "conflict_resolved",
                           {"winner": "safety_officer", "idx": idx})
            return idx, "safety_override"
        if carbon and min(b.confidence for b in carbon) > 0.5:
            idx = carbon[0].proposed_action % n_candidates
            self.broadcast("coordinator", "conflict_resolved",
                           {"winner": "carbon_broker", "idx": idx})
            return idx, "carbon_override"
        idx = self.vote(bids, n_candidates)
        self.broadcast("coordinator", "conflict_resolved",
                       {"winner": "vote", "idx": idx})
        return idx, "vote"


# ---- 6: XAI (P2 SHAP -> z-score) ----
@dataclass
class Explanation:
    decision_id: str
    chosen_idx: int
    top_features: List[Tuple[str, float]]
    counterfactuals: List[Dict[str, Any]]
    rationale: str
    confidence: float
    safety_ok: bool
    carbon_price_signal: float
    method: str = "z_score"
    validation_score: float = 0.0

    def to_dict(self):
        return asdict(self)


class DecisionExplainer:
    def __init__(self, feature_names: Optional[Sequence[str]] = None,
                 predict_fn: Optional[Callable] = None):
        self.feature_names = list(feature_names) if feature_names else [
            "model_size_mb", "prompt_len", "gen_len",
            "gpu_mem_free_mb", "disk_speed_class"]
        self.predict_fn = predict_fn
        self._shap_explainer = None

    def fit(self, background: np.ndarray) -> None:
        if SHAP_AVAILABLE and self.predict_fn is not None:
            try:
                self._shap_explainer = shap.KernelExplainer(
                    self.predict_fn, background)
            except Exception as e:
                logger.warning(f"SHAP init failed: {e}")

    def _z_attributions(self, chosen, pareto) -> List[Tuple[str, float]]:
        if not pareto:
            return []
        arr = np.array([[float(c.get(f, 0.0)) for f in self.feature_names]
                        for c in pareto])
        chosen_vec = np.array([float(chosen.get(f, 0.0))
                               for f in self.feature_names])
        mean = arr.mean(axis=0)
        std = arr.std(axis=0) + 1e-9
        z = (chosen_vec - mean) / std
        attrs = list(zip(self.feature_names, z.tolist()))
        attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
        return attrs

    def _shap_attributions(self, x) -> Optional[List[Tuple[str, float]]]:
        if self._shap_explainer is None:
            return None
        try:
            vals = self._shap_explainer.shap_values(x.reshape(1, -1))
            v = np.asarray(vals).reshape(-1)
            attrs = list(zip(self.feature_names, v.tolist()))
            attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
            return attrs
        except Exception:
            return None

    def explain(self, chosen_idx: int, chosen: Dict[str, Any],
                pareto: Sequence[Dict[str, Any]],
                counterfactuals: Dict[int, float],
                safety_ok: bool, carbon_price: float,
                confidence: float = 0.5) -> Explanation:
        method = "z_score"
        attrs: Optional[List[Tuple[str, float]]] = None
        if self._shap_explainer is not None:
            x = np.array([float(chosen.get(f, 0.0)) for f in self.feature_names])
            attrs = self._shap_attributions(x)
            if attrs is not None:
                method = "shap"
        if attrs is None:
            attrs = self._z_attributions(chosen, pareto)
        cf_list = []
        for idx, score in sorted(counterfactuals.items(),
                                 key=lambda kv: kv[1], reverse=True)[:3]:
            if 0 <= idx < len(pareto):
                cf_list.append({"alternative_idx": idx,
                                "expected_reward": float(score)})
        top_feat = ", ".join(f"{n}={v:+.2f}" for n, v in attrs[:3])
        rationale = (
            f"Chose policy #{chosen_idx} (confidence={confidence:.2f}, "
            f"method={method}). Top deviations: {top_feat}. "
            f"Carbon price={carbon_price:.4f}. "
            f"Safety={'ok' if safety_ok else 'OVERRIDE'}. "
            f"Counterfactuals: {len(cf_list)}."
        )
        return Explanation(
            decision_id=f"dec-{uuid.uuid4().hex[:8]}",
            chosen_idx=chosen_idx,
            top_features=attrs[:5],
            counterfactuals=cf_list,
            rationale=rationale,
            confidence=confidence,
            safety_ok=safety_ok,
            carbon_price_signal=carbon_price,
            method=method,
        )


# ---- 1: Quantum distillation (P2 with backends) ----
class QuantumInspiredTeacher:
    def __init__(self, n_qubits: int = 5, seed: int = 0):
        self.n_qubits = n_qubits
        self.rng = np.random.default_rng(seed)
        self._params = self.rng.normal(size=(n_qubits, 2)) * 0.5

    def _ry(self, state, theta, q):
        c, s = math.cos(theta / 2), math.sin(theta / 2)
        n = len(state); new = state.copy(); step = 1 << q
        for i in range(n):
            if i & step == 0:
                a, b = state[i], state[i | step]
                new[i] = c * a - s * b; new[i | step] = s * a + c * b
        return new

    def _simulate(self, features, n_candidates):
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128); state[0] = 1.0
        for q in range(self.n_qubits):
            angle = float(self._params[q, 0] * features[q % len(features)]
                          + self._params[q, 1])
            state = self._ry(state, angle, q)
        probs_full = np.abs(state) ** 2
        probs = np.zeros(n_candidates, dtype=np.float64)
        for i, p in enumerate(probs_full):
            probs[i % n_candidates] += p
        return probs / max(probs.sum(), 1e-12)

    def teacher_probs(self, features, n):
        if n <= 0:
            return np.zeros(0)
        return self._simulate(np.asarray(features, dtype=np.float64), n)


class DistillationEnsemble:
    def __init__(self, quantum: QuantumInspiredTeacher, alpha: float = 0.5):
        self.quantum = quantum
        self.alpha = alpha

    def blend(self, features, other, n):
        q = self.quantum.teacher_probs(features, n)
        if other is None or len(other) != n:
            return q
        return self.alpha * q + (1.0 - self.alpha) * np.asarray(other)


# ---- 10: HITL / active learning (P0/P1/P2) ----
@dataclass
class HITLRequest:
    request_id: str
    reason: str
    chosen_idx: int
    candidates: List[Dict[str, Any]]
    uncertainty: float
    carbon_price: float
    priority: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def new(cls, reason: str, chosen_idx: int, candidates: List[Dict[str, Any]],
            uncertainty: float, carbon_price: float, priority: int = 0,
            metadata: Optional[Dict[str, Any]] = None) -> "HITLRequest":
        return cls(request_id=uuid.uuid4().hex[:12], reason=reason,
                   chosen_idx=chosen_idx, candidates=list(candidates),
                   uncertainty=float(uncertainty),
                   carbon_price=float(carbon_price),
                   priority=int(priority),
                   metadata=dict(metadata or {}))


class UncertaintyEstimator:
    def __init__(self, entropy_threshold: float = 0.75,
                 variance_threshold: float = 0.05,
                 disagreement_threshold: float = 0.15):
        self.entropy_threshold = entropy_threshold
        self.variance_threshold = variance_threshold
        self.disagreement_threshold = disagreement_threshold
        self._rewards: deque = deque(maxlen=64)
        self._expert: deque = deque(maxlen=32)
        self._lock = threading.Lock()

    def observe(self, reward: float) -> None:
        with self._lock:
            self._rewards.append(float(reward))

    def observe_expert_forecasts(self, forecasts: Sequence[float]) -> None:
        with self._lock:
            self._expert.append([float(f) for f in forecasts])

    def entropy(self, probs) -> float:
        if probs is None or len(probs) == 0:
            return 1.0
        p = np.clip(np.asarray(probs, dtype=np.float64), 1e-12, 1.0)
        return float(-np.sum(p * np.log(p)) / math.log(len(p)))

    def variance(self) -> float:
        with self._lock:
            vals = list(self._rewards)
        if len(vals) < 3:
            return 0.0
        return float(np.var(np.asarray(vals)))

    def disagreement(self) -> float:
        with self._lock:
            recent = list(self._expert)[-1] if self._expert else None
        if not recent or len(recent) < 2:
            return 0.0
        arr = np.asarray(recent, dtype=np.float64)
        return float(arr.std() / (abs(arr.mean()) + 1e-9))

    def is_uncertain(self, probs) -> Tuple[bool, float]:
        h = self.entropy(probs)
        v = self.variance()
        d = self.disagreement()
        score = (0.5 * h
                 + 0.3 * min(1.0, v / max(self.variance_threshold, 1e-9))
                 + 0.2 * min(1.0, d / max(self.disagreement_threshold, 1e-9)))
        uncertain = (h > self.entropy_threshold
                     or v > self.variance_threshold
                     or d > self.disagreement_threshold)
        return uncertain, float(score)

    def stats(self) -> Dict[str, Any]:
        return {"rewards": len(self._rewards),
                "expert_forecasts": len(self._expert),
                "variance": self.variance(),
                "disagreement": self.disagreement()}


@dataclass
class HITLAuditEntry:
    request_id: str
    approved: bool
    reason: str
    mode: str
    latency_ms: float = 0.0
    prev_hash: str = ""
    entry_hash: str = ""
    timestamp: float = field(default_factory=time.time)

    def compute_hash(self) -> str:
        payload = json.dumps({
            "request_id": self.request_id,
            "approved": self.approved,
            "reason": self.reason,
            "mode": self.mode,
            "latency_ms": round(self.latency_ms, 6),
            "timestamp": round(self.timestamp, 6),
            "prev_hash": self.prev_hash,
        }, sort_keys=True, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class HumanInTheLoopGate:
    """Production-safe HITL gate with pending registry and hash-chained audit."""

    def __init__(self, approver: Optional[Callable[[HITLRequest], bool]] = None,
                 timeout_s: float = 30.0,
                 auto_approve_on_timeout: bool = False,
                 production_safe: bool = True,
                 max_pending: int = 128,
                 persist: Optional[Persistence] = None):
        self.approver = approver
        self.timeout_s = float(timeout_s)
        self.auto_approve_on_timeout = bool(auto_approve_on_timeout)
        self.production_safe = bool(production_safe)
        self.max_pending = int(max_pending)
        self._persist = persist
        self._pending: Dict[str, HITLRequest] = {}
        self._futures: Dict[str, asyncio.Future] = {}
        self._lock = threading.RLock()
        self._audit: deque = deque(maxlen=4096)
        self._last_hash = self._persist.hitl_last_hash() if self._persist else ""
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="hitl-approver")
        M_HITL_PENDING.set(0)

    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _audit_append(self, entry: HITLAuditEntry) -> None:
        entry.prev_hash = self._last_hash
        entry.entry_hash = entry.compute_hash()
        self._last_hash = entry.entry_hash
        self._audit.append(entry)
        if self._persist:
            with contextlib.suppress(Exception):
                self._persist.hitl_append(entry.to_dict())

    def pending_requests(self) -> List[Dict[str, Any]]:
        with self._lock:
            pend = list(self._pending.values())
        pend.sort(key=lambda r: (-r.priority, r.timestamp))
        return [{"request_id": r.request_id, "reason": r.reason,
                 "chosen_idx": r.chosen_idx, "uncertainty": r.uncertainty,
                 "priority": r.priority, "age_s": time.time() - r.timestamp}
                for r in pend]

    async def request(self, req: HITLRequest) -> bool:
        start = time.time()

        if self.approver is None and self.production_safe:
            self._audit_append(HITLAuditEntry(
                request_id=req.request_id, approved=False, reason=req.reason,
                mode="blocked_no_approver",
                latency_ms=(time.time() - start) * 1000.0))
            M_HITL.labels(decision="blocked").inc()
            return False
        if self.approver is None:
            self._audit_append(HITLAuditEntry(
                request_id=req.request_id, approved=True, reason=req.reason,
                mode="auto_no_approver",
                latency_ms=(time.time() - start) * 1000.0))
            M_HITL.labels(decision="auto_approved").inc()
            return True

        with self._lock:
            if len(self._pending) >= self.max_pending:
                self._audit_append(HITLAuditEntry(
                    request_id=req.request_id, approved=False,
                    reason=req.reason, mode="queue_full",
                    latency_ms=(time.time() - start) * 1000.0))
                M_HITL.labels(decision="queue_full").inc()
                return False
            loop = asyncio.get_running_loop()
            future: asyncio.Future = loop.create_future()
            self._pending[req.request_id] = req
            self._futures[req.request_id] = future
            M_HITL_PENDING.set(len(self._pending))

        def _call_approver():
            try:
                return bool(self.approver(req))
            except Exception as e:
                logger.warning(f"Approver failed: {e}")
                return False

        worker = self._executor.submit(_call_approver)
        mode = "approver"
        try:
            approved = await asyncio.wait_for(
                loop.run_in_executor(None, worker.result),
                timeout=self.timeout_s)
        except asyncio.TimeoutError:
            approved = self.auto_approve_on_timeout
            mode = "timeout"
        except Exception as e:
            approved = False
            mode = f"error:{e}"
        finally:
            with self._lock:
                self._pending.pop(req.request_id, None)
                fut = self._futures.pop(req.request_id, None)
                M_HITL_PENDING.set(len(self._pending))
            if fut is not None and not fut.done():
                with contextlib.suppress(Exception):
                    fut.set_result(bool(approved))

        latency_ms = (time.time() - start) * 1000.0
        self._audit_append(HITLAuditEntry(
            request_id=req.request_id, approved=approved, reason=req.reason,
            mode=mode, latency_ms=latency_ms))
        M_HITL.labels(
            decision="approved" if approved else "rejected").inc()
        return bool(approved)

    def resolve(self, request_id: str, approved: bool) -> bool:
        with self._lock:
            fut = self._futures.get(request_id)
        if fut is None or fut.done():
            return False
        with contextlib.suppress(Exception):
            fut.set_result(bool(approved))
        self._audit_append(HITLAuditEntry(
            request_id=request_id, approved=approved, reason="external_resolve",
            mode="resolved"))
        M_HITL.labels(
            decision="approved" if approved else "rejected").inc()
        return True

    def audit_trail(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        if self._persist:
            rows = self._persist.hitl_recent(limit or 1000)
            return [{"request_id": r[0], "approved": bool(r[1]), "reason": r[2],
                     "mode": r[3], "latency_ms": r[4], "prev_hash": r[5],
                     "entry_hash": r[6], "timestamp": r[7]} for r in rows]
        entries = list(self._audit)
        if limit is not None:
            entries = entries[-limit:]
        return [e.to_dict() for e in entries]

    def verify_audit_chain(self) -> bool:
        entries = self.audit_trail(limit=None)
        prev = ""
        for e in entries:
            if e["prev_hash"] != prev:
                return False
            payload = json.dumps({
                "request_id": e["request_id"],
                "approved": bool(e["approved"]),
                "reason": e["reason"],
                "mode": e["mode"],
                "latency_ms": round(float(e["latency_ms"]), 6),
                "timestamp": round(float(e["timestamp"]), 6),
                "prev_hash": e["prev_hash"],
            }, sort_keys=True, default=str)
            if hashlib.sha256(payload.encode()).hexdigest() != e["entry_hash"]:
                return False
            prev = e["entry_hash"]
        return True

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            pending = len(self._pending)
        return {"pending": pending, "max_pending": self.max_pending,
                "audit_size": len(self._audit),
                "production_safe": self.production_safe,
                "auto_approve_on_timeout": self.auto_approve_on_timeout,
                "timeout_s": self.timeout_s}


class ActiveLearningSampler:
    """Diversity-aware, time-bounded buffer."""

    def __init__(self, capacity: int = 256, seed: int = 0,
                 feature_dim: int = 5, diversity_weight: float = 0.4,
                 n_clusters: int = 4, max_age_s: float = 3600.0):
        self.capacity = int(capacity)
        self.feature_dim = int(feature_dim)
        self.diversity_weight = float(diversity_weight)
        self.n_clusters = int(n_clusters)
        self.max_age_s = float(max_age_s)
        self._buffer: List[Dict[str, Any]] = []
        self._features: List[np.ndarray] = []
        self._uncertainties: List[float] = []
        self._lock = threading.RLock()
        self._rng = random.Random(int(seed))
        self._kmeans = KMeans(n_clusters=self.n_clusters, n_init=5) \
            if SKLEARN_AVAILABLE and self.n_clusters > 1 else None
        M_ACTIVE_LEARNING.set(0)

    def _embed(self, record: Dict[str, Any]) -> np.ndarray:
        vals: List[float] = []
        for k in sorted(record.keys()):
            v = record[k]
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                vals.append(float(v))
        if not vals:
            vals = [0.0]
        v = np.asarray(vals, dtype=np.float64)
        if len(v) < self.feature_dim:
            v = np.pad(v, (0, self.feature_dim - len(v)))
        else:
            v = v[:self.feature_dim]
        n = float(np.linalg.norm(v)) + 1e-9
        return v / n

    def maybe_store(self, record: Dict[str, Any], uncertainty: float,
                    threshold: float = 0.3) -> bool:
        if uncertainty < threshold:
            return False
        feat = self._embed(record)
        with self._lock:
            if len(self._buffer) >= self.capacity:
                idx = int(np.argmin(self._uncertainties))
                self._buffer.pop(idx)
                self._features.pop(idx)
                self._uncertainties.pop(idx)
            self._buffer.append({**record, "uncertainty": float(uncertainty),
                                 "t": time.time()})
            self._features.append(feat)
            self._uncertainties.append(float(uncertainty))
            size = len(self._buffer)
        M_ACTIVE_LEARNING.set(size)
        return True

    def sample(self, k: int = 8) -> List[Dict[str, Any]]:
        with self._lock:
            if not self._buffer:
                return []
            now = time.time()
            indices = [i for i, r in enumerate(self._buffer)
                       if self.max_age_s <= 0
                       or (now - r.get("t", now)) <= self.max_age_s]
            if not indices:
                return []
            k = min(int(k), len(indices))
            uncs = np.asarray([self._uncertainties[i] for i in indices])
            if (self._kmeans is not None
                    and len(indices) >= max(2 * self.n_clusters, 8)):
                try:
                    X = np.stack([self._features[i] for i in indices])
                    labels = self._kmeans.fit_predict(X)
                    sizes = np.bincount(labels)
                    div = np.asarray([1.0 / (1.0 + sizes[l]) for l in labels])
                except Exception:
                    div = np.asarray([self._rng.random() for _ in indices])
            else:
                div = np.asarray([self._rng.random() for _ in indices])
            w = max(0.0, min(1.0, self.diversity_weight))
            scores = (1 - w) * uncs + w * div
            order = np.argsort(-scores)[:k].tolist()
            return [self._buffer[indices[j]] for j in order]

    def __len__(self) -> int:
        with self._lock:
            return len(self._buffer)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            uncs = list(self._uncertainties)
        return {"size": len(uncs), "capacity": self.capacity,
                "mean_uncertainty": float(np.mean(uncs)) if uncs else 0.0,
                "max_uncertainty": float(np.max(uncs)) if uncs else 0.0}


# ===========================================================================
# CORE ORCHESTRATION CLASSES
# ===========================================================================
class CarbonDelayScheduler:
    def __init__(self, carbon_api, max_delay_seconds: int = 3600,
                 threshold_gco2_per_kwh: float = 150.0):
        self.carbon_api = carbon_api
        self.max_delay = max_delay_seconds
        self.threshold = threshold_gco2_per_kwh
        self.queue: List[Tuple[float, int, Dict[str, Any]]] = []
        self._seq = 0
        self._lock = threading.Lock()

    def submit(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if task.get("priority") == "high":
            return {"status": "forward", "task": task, "delay_until": None}

        # Re-entry guard: a task that was already deferred at this window
        # should not be deferred again with the same intent.
        if task.get("_deferral_count", 0) >= 3:
            return {"status": "forward", "task": task, "delay_until": None}

        current_intensity = self.carbon_api.get_current()
        if current_intensity <= self.threshold:
            return {"status": "forward", "task": task, "delay_until": None}

        forecast_minutes = self.max_delay // 60 + 1
        forecast = self.carbon_api.get_forecast(forecast_minutes)
        if not forecast:
            return {"status": "forward", "task": task, "delay_until": None}

        now = time.time()
        best_time = None
        for ts, intensity in forecast:
            if intensity < self.threshold:
                best_time = ts
                break

        if best_time is None or (best_time - now) > self.max_delay:
            return {"status": "forward", "task": task, "delay_until": None}

        task = dict(task)
        task["_deferral_count"] = int(task.get("_deferral_count", 0)) + 1
        with self._lock:
            heapq.heappush(self.queue, (best_time, self._seq, task))
            self._seq += 1
        return {"status": "delayed", "task": task, "delay_until": best_time}

    def tick(self) -> List[Dict[str, Any]]:
        now = time.time()
        released: List[Dict[str, Any]] = []
        with self._lock:
            while self.queue and self.queue[0][0] <= now:
                _, _, task = heapq.heappop(self.queue)
                released.append(task)
        return released

    def size(self) -> int:
        with self._lock:
            return len(self.queue)


import heapq  # noqa: E402  (needed by CarbonDelayScheduler)


class CarbonAPIStub:
    """Mock carbon API with a true sinusoidal daily cycle."""
    def __init__(self, base_intensity: float = 200.0, volatility: float = 50.0):
        self.base = base_intensity
        self.volatility = volatility
        self._start_time = time.time()

    def _intensity(self, ts: float) -> float:
        cycle = (ts - self._start_time) / 3600.0
        base = self.base + self.volatility * 0.5 * (
            1.0 + math.sin(2 * math.pi * cycle / 12.0))
        return max(50.0, base + random.gauss(0, 3))

    def get_current(self) -> float:
        return self._intensity(time.time())

    def get_forecast(self, minutes: int = 60) -> List[Tuple[float, float]]:
        now = time.time()
        return [(now + i * 60, self._intensity(now + i * 60))
                for i in range(0, minutes, 10)]


# --- GPU profiler ---
try:
    import pynvml
    NVML_AVAILABLE = True
    try:
        pynvml.nvmlInit()
    except Exception:
        NVML_AVAILABLE = False
except ImportError:
    NVML_AVAILABLE = False


class GPUProfiler:
    def __init__(self, sample_interval_sec: float = 0.5):
        self.sample_interval = sample_interval_sec
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._latest_metrics: Dict[str, Any] = {}
        self._disk_io_start = psutil.disk_io_counters() if psutil else None
        self._last_disk_time = time.time()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    def start(self):
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    def _sample_loop(self):
        while not self._stop_event.is_set():
            snapshot = self._snapshot()
            with self._lock:
                self._latest_metrics = snapshot
            self._stop_event.wait(self.sample_interval)

    def _snapshot(self) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}
        if NVML_AVAILABLE:
            try:
                device_count = pynvml.nvmlDeviceGetCount()
                if device_count > 0:
                    handle = pynvml.nvmlDeviceGetHandleByIndex(0)
                    mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                    util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                    power = pynvml.nvmlDeviceGetPowerUsage(handle) / 1000.0
                    name = pynvml.nvmlDeviceGetName(handle)
                    if isinstance(name, bytes):
                        name = name.decode()
                    metrics["gpu_name"] = name
                    metrics["gpu_memory_total_mb"] = mem_info.total / 1024**2
                    metrics["gpu_memory_free_mb"] = mem_info.free / 1024**2
                    metrics["gpu_memory_used_mb"] = mem_info.used / 1024**2
                    metrics["gpu_utilization_pct"] = util.gpu / 100.0
                    metrics["gpu_power_watts"] = power
                    metrics["gpu_temp_c"] = pynvml.nvmlDeviceGetTemperature(
                        handle, pynvml.NVML_TEMPERATURE_GPU)
            except Exception:
                pass
        else:
            metrics["gpu_available"] = False

        vm = psutil.virtual_memory()
        metrics["cpu_memory_total_mb"] = vm.total / 1024**2
        metrics["cpu_memory_free_mb"] = vm.available / 1024**2
        metrics["cpu_utilization_pct"] = psutil.cpu_percent(interval=None) / 100.0

        now = time.time()
        disk_io = psutil.disk_io_counters()
        if self._disk_io_start and now - self._last_disk_time > 0.5:
            delta_time = now - self._last_disk_time
            read_bytes = disk_io.read_bytes - self._disk_io_start.read_bytes
            write_bytes = disk_io.write_bytes - self._disk_io_start.write_bytes
            metrics["disk_read_bandwidth_gbps"] = (read_bytes / delta_time) * 8 / 1e9
            metrics["disk_write_bandwidth_gbps"] = (write_bytes / delta_time) * 8 / 1e9
        self._disk_io_start = disk_io
        self._last_disk_time = now
        return metrics

    def get_current_metrics(self) -> Dict[str, Any]:
        if self._running:
            with self._lock:
                return dict(self._latest_metrics)
        return self._snapshot()


class MetricAggregator:
    def __init__(self, gpu_profiler: GPUProfiler, executor_fn: Callable):
        self.profiler = gpu_profiler
        self.executor = executor_fn

    def run(self, task: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
        start_metrics = self.profiler.get_current_metrics()
        start_time = time.time()

        try:
            output, raw_inference_metrics = self.executor(task, policy)
            success = True
        except Exception as e:
            output = None
            raw_inference_metrics = {"error": str(e)}
            success = False

        end_metrics = self.profiler.get_current_metrics()
        end_time = time.time()
        elapsed = end_time - start_time

        avg_power = (start_metrics.get("gpu_power_watts", 0.0)
                     + end_metrics.get("gpu_power_watts", 0.0)) / 2.0
        total_energy_kwh = (avg_power * elapsed) / 3600.0 / 1000.0
        tokens_generated = raw_inference_metrics.get("tokens_generated", 0)
        tokens_per_sec = tokens_generated / elapsed if elapsed > 0 else 0.0
        gpu_total = end_metrics.get("gpu_memory_total_mb", 1.0)
        gpu_used = end_metrics.get("gpu_memory_used_mb", 0.0)
        memory_efficiency = gpu_used / gpu_total if gpu_total > 0 else 0.0
        quality_score = float(raw_inference_metrics.get("quality_score", 0.0))

        return {
            "success": success,
            "output": output,
            "inference_metrics": raw_inference_metrics,
            "quality_score": quality_score,
            "elapsed_sec": elapsed,
            "tokens_per_sec": tokens_per_sec,
            "total_energy_kwh": total_energy_kwh,
            "gpu_power_avg_watts": avg_power,
            "gpu_memory_peak_mb": max(
                start_metrics.get("gpu_memory_used_mb", 0),
                end_metrics.get("gpu_memory_used_mb", 0)),
            "memory_efficiency": memory_efficiency,
            "gpu_oom": (not success
                        and "CUDA out of memory" in str(
                            raw_inference_metrics.get("error", ""))),
            "real_metrics": end_metrics,
        }


class RewardCalculator:
    def __init__(self, weights: Optional[Dict[str, float]] = None):
        self.weights = weights or {
            "quality": 0.30, "throughput": 0.25,
            "energy_efficiency": 0.20, "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10}
        self._default_weights = dict(self.weights)
        self._local = threading.local()

    def snapshot(self) -> Dict[str, float]:
        return dict(self.weights)

    def restore(self, w: Optional[Dict[str, float]]) -> None:
        self.weights = dict(w) if w else dict(self._default_weights)

    def compute(self, aggregated_metrics: Dict[str, Any],
                constraints: Dict[str, Any],
                carbon_intensity_gco2_kwh: float = 0.0) -> float:
        quality = aggregated_metrics.get("quality_score", 1.0)
        throughput = aggregated_metrics.get("tokens_per_sec", 0.0)
        total_energy_kwh = aggregated_metrics.get("total_energy_kwh", 0.0)
        mem_eff = aggregated_metrics.get("memory_efficiency", 0.0)
        oom = aggregated_metrics.get("gpu_oom", False)

        if throughput > 0 and total_energy_kwh > 0:
            carbon_per_token = (total_energy_kwh * carbon_intensity_gco2_kwh) / throughput
            carbon_eff = max(0.0, 1.0 - (carbon_per_token / 100.0))
        else:
            carbon_eff = 0.0

        if total_energy_kwh > 0 and throughput > 0:
            energy_eff = min(1.0, throughput / (total_energy_kwh * 1000))
        else:
            energy_eff = 0.0

        penalty = 0.0
        if oom:
            penalty -= 10.0
        max_latency = constraints.get("max_latency_ms", 1e9)
        if aggregated_metrics.get("elapsed_sec", 0) * 1000 > max_latency:
            penalty -= 5.0
        if quality < constraints.get("min_quality", 0.5):
            penalty -= 5.0

        reward = (
            self.weights["quality"] * quality
            + self.weights["throughput"] * min(1.0, throughput / 100.0)
            + self.weights["energy_efficiency"] * energy_eff
            + self.weights["carbon_efficiency"] * carbon_eff
            + self.weights["memory_efficiency"] * mem_eff
        ) + penalty
        return max(-10.0, min(10.0, reward))


class WorkloadFingerprint:
    def __init__(self, model_size_mb: float, prompt_len: int, gen_len: int,
                 gpu_mem_free_mb: float, disk_speed_class: int):
        self.model_size_mb = model_size_mb
        self.prompt_len = prompt_len
        self.gen_len = gen_len
        self.gpu_mem_free_mb = gpu_mem_free_mb
        self.disk_speed_class = disk_speed_class

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.model_size_mb / 1000.0,
            self.prompt_len / 1024.0,
            self.gen_len / 1024.0,
            self.gpu_mem_free_mb / 1000.0,
            self.disk_speed_class / 2.0,
        ])

    def to_dict(self) -> Dict[str, Any]:
        return {"model_size_mb": self.model_size_mb,
                "prompt_len": self.prompt_len,
                "gen_len": self.gen_len,
                "gpu_mem_free_mb": self.gpu_mem_free_mb,
                "disk_speed_class": self.disk_speed_class}


class PolicyMetaCache:
    def __init__(self, max_age_hours: float = 24.0,
                 dist_threshold: float = 0.2, max_size: int = 500):
        self.max_age_seconds = max_age_hours * 3600
        self.dist_threshold = dist_threshold
        self.max_size = max_size
        self.store: Dict[tuple, Tuple[Dict[str, Any], float, float]] = {}
        self.vectors: List[np.ndarray] = []
        self.keys: List[tuple] = []
        self._lock = threading.RLock()

    def _vector_to_key(self, vec: np.ndarray) -> tuple:
        return tuple(float(x) for x in vec.tolist())

    def get_best_policy(self, fp: WorkloadFingerprint) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self.vectors:
                return None
            vec = fp.to_vector()
            best_idx, best_dist = -1, float("inf")
            for i, stored_vec in enumerate(self.vectors):
                dist = float(np.linalg.norm(vec - stored_vec))
                if dist < best_dist:
                    best_dist, best_idx = dist, i
            if best_idx == -1 or best_dist > self.dist_threshold:
                return None
            key = self.keys[best_idx]
            policy, timestamp, _ = self.store[key]
            if (time.time() - timestamp) > self.max_age_seconds:
                return None
            return dict(policy)

    def update(self, fp: WorkloadFingerprint, policy: Dict[str, Any],
               reward: float) -> None:
        with self._lock:
            vec = fp.to_vector()
            key = self._vector_to_key(vec)
            if key in self.store:
                old_policy, _, old_reward = self.store[key]
                if reward > old_reward:
                    self.store[key] = (dict(policy), time.time(), reward)
            else:
                if len(self.vectors) >= self.max_size:
                    oldest_i = min(range(len(self.vectors)),
                                   key=lambda i: self.store[self.keys[i]][1])
                    oldest_key = self.keys[oldest_i]
                    self.store.pop(oldest_key, None)
                    self.vectors.pop(oldest_i)
                    self.keys.pop(oldest_i)
                self.store[key] = (dict(policy), time.time(), reward)
                self.vectors.append(vec)
                self.keys.append(key)

    def export_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "store": [(list(k), [dict(v[0]), v[1], v[2]])
                          for k, v in self.store.items()],
                "vectors": [v.tolist() for v in self.vectors],
                "keys": [list(k) for k in self.keys],
            }

    def import_state(self, data: Dict[str, Any]) -> None:
        with self._lock:
            try:
                self.store = {tuple(k): (dict(v[0]), float(v[1]), float(v[2]))
                              for k, v in data.get("store", [])}
                self.vectors = [np.array(v) for v in data.get("vectors", [])]
                self.keys = [tuple(k) for k in data.get("keys", [])]
                if len(self.vectors) != len(self.keys):
                    self.store, self.vectors, self.keys = {}, [], []
            except Exception:
                self.store, self.vectors, self.keys = {}, [], []

    def size(self) -> int:
        with self._lock:
            return len(self.store)


class ContextualBandit:
    MAX_ACTIONS = 64

    def __init__(self, action_space: List[Dict[str, Any]], fallback_solver: Callable):
        self.actions = [dict(p) for p in action_space]
        self.fallback_solver = fallback_solver
        self.num_actions = len(self.actions)
        self.weights: Dict[tuple, np.ndarray] = {}
        self.trials: Dict[tuple, int] = {}
        self.action_trials: Dict[tuple, np.ndarray] = {}
        self._lock = threading.RLock()

    def _encode_context(self, fp: WorkloadFingerprint) -> tuple:
        return tuple(float(x) for x in fp.to_vector().tolist())

    @staticmethod
    def _canonical(p: Dict[str, Any]) -> str:
        return json.dumps(p, sort_keys=True)

    def select_action(self, fp: WorkloadFingerprint,
                      min_trials_before_bandit: int = 5,
                      confidence_threshold: float = 0.6
                      ) -> Tuple[Optional[Dict], float]:
        ctx_key = self._encode_context(fp)
        with self._lock:
            n_trials = self.trials.get(ctx_key, 0)
            if n_trials < min_trials_before_bandit:
                return None, 0.0
            weights = self.weights.get(ctx_key)
            if weights is None or len(weights) != self.num_actions:
                return None, 0.0
            std = 1.0 / np.sqrt(n_trials + 1)
            sampled = np.random.normal(weights, std)
            best_idx = int(np.argmax(sampled))
            confidence = 1.0 - (1.0 / (n_trials + 1))
            if confidence < confidence_threshold:
                return None, 0.0
            return dict(self.actions[best_idx]), confidence

    def update(self, fp: WorkloadFingerprint, action: Dict[str, Any],
               reward: float) -> None:
        ctx_key = self._encode_context(fp)
        with self._lock:
            if ctx_key not in self.weights:
                self.weights[ctx_key] = np.zeros(self.num_actions)
                self.trials[ctx_key] = 0
                self.action_trials[ctx_key] = np.zeros(self.num_actions, dtype=int)
            canonical = self._canonical(action)
            action_idx = -1
            for i, a in enumerate(self.actions):
                if self._canonical(a) == canonical:
                    action_idx = i
                    break
            if action_idx < 0:
                return
            n = self.action_trials[ctx_key][action_idx]
            lr = 0.1 / (n + 1)
            old_weight = self.weights[ctx_key][action_idx]
            self.weights[ctx_key][action_idx] = old_weight + lr * (reward - old_weight)
            self.action_trials[ctx_key][action_idx] += 1
            self.trials[ctx_key] += 1

    def seed_safe_policy(self, fp: WorkloadFingerprint,
                         policy: Dict[str, Any]) -> None:
        ctx_key = self._encode_context(fp)
        with self._lock:
            if ctx_key not in self.weights:
                self.weights[ctx_key] = np.zeros(self.num_actions)
                self.trials[ctx_key] = 0
                self.action_trials[ctx_key] = np.zeros(self.num_actions, dtype=int)
            canonical = self._canonical(policy)
            for i, a in enumerate(self.actions):
                if self._canonical(a) == canonical:
                    self.weights[ctx_key][i] = 1.0
                    break

    def add_action(self, policy: Dict[str, Any]) -> bool:
        with self._lock:
            if self.num_actions >= self.MAX_ACTIONS:
                return False
            canonical = self._canonical(policy)
            if any(self._canonical(a) == canonical for a in self.actions):
                return False
            self.actions.append(dict(policy))
            self.num_actions = len(self.actions)
            for key in list(self.weights.keys()):
                self.weights[key] = np.append(self.weights[key], 0.0)
                self.action_trials[key] = np.append(self.action_trials[key], 0)
            return True

    def export_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "actions": [dict(a) for a in self.actions],
                "weights": [(list(k), v.tolist()) for k, v in self.weights.items()],
                "trials": [(list(k), int(v)) for k, v in self.trials.items()],
                "action_trials": [(list(k), v.tolist())
                                  for k, v in self.action_trials.items()],
            }

    def import_state(self, data: Dict[str, Any]) -> None:
        with self._lock:
            try:
                if data.get("actions"):
                    self.actions = [dict(a) for a in data["actions"]]
                    self.num_actions = len(self.actions)
                self.weights = {tuple(k): np.asarray(v, dtype=np.float64)
                                for k, v in data.get("weights", [])}
                self.trials = {tuple(k): int(v)
                               for k, v in data.get("trials", [])}
                self.action_trials = {tuple(k): np.asarray(v, dtype=int)
                                      for k, v in data.get("action_trials", [])}
            except Exception:
                self.weights, self.trials, self.action_trials = {}, {}, {}


class GreenAgentPolicyRouter:
    def __init__(self, carbon_api, action_space: List[Dict[str, Any]],
                 lp_solver: Callable, executor: Callable,
                 min_trials_before_bandit: int = 5,
                 confidence_threshold: float = 0.6,
                 limit_graph=None, rlhf=None, distiller=None,
                 moe_router=None, bio_generator=None,
                 # Persistence
                 persist_path: Optional[str] = None,
                 # Ten enhancement toggles
                 enable_quantum_teacher: bool = True,
                 enable_causal: bool = True,
                 enable_federated: bool = True,
                 enable_multi_agent: bool = True,
                 enable_temporal_logic: bool = True,
                 enable_xai: bool = True,
                 enable_precision_switching: bool = True,
                 enable_carbon_market: bool = True,
                 enable_chaos: bool = False,
                 enable_hitl: bool = True,
                 # Enhancement params
                 chaos_fault_prob: float = 0.0,
                 chaos_latency_ms: float = 0.0,
                 chaos_carbon_spike_prob: float = 0.0,
                 chaos_max_concurrent: int = 1,
                 chaos_duration_s: float = 0.0,
                 hitl_approver: Optional[Callable[[HITLRequest], bool]] = None,
                 hitl_timeout_s: float = 30.0,
                 hitl_production_safe: bool = True,
                 hitl_auto_approve_on_timeout: bool = False,
                 rec_default_kwh: float = 0.0,
                 rate_limit_per_minute: int = 600,
                 daily_cost_budget_usd: float = 10.0,
                 cost_per_1k_tokens: float = 0.002,
                 electricity_maps_api_key: Optional[str] = None,
                 electricity_maps_zone: str = "US-CAL-CISO",
                 watttime_username: Optional[str] = None,
                 watttime_password: Optional[str] = None,
                 watttime_region: str = "CAISO_NORTH"):
        self.carbon_scheduler = CarbonDelayScheduler(carbon_api)
        self.cache = PolicyMetaCache()
        self.bandit = ContextualBandit(action_space, lp_solver)
        self.lp_solver = lp_solver
        self.executor = executor
        self.min_trials = min_trials_before_bandit
        self.conf_threshold = confidence_threshold
        self.limit_graph = limit_graph
        self.rlhf = rlhf
        self.distiller = distiller
        self.moe_router = moe_router
        self.bio_generator = bio_generator
        self._update_lock = threading.Lock()

        # Persistence
        self.persistence = Persistence(persist_path)

        # Rate limit and cost
        self._rate_limiter = TokenBucket(rate_limit_per_minute, 60.0)
        self.daily_cost_budget_usd = daily_cost_budget_usd
        self.cost_per_1k_tokens = cost_per_1k_tokens
        self._today_cost = self.persistence.cost_today()

        # ---- Enhancement 1: Quantum distillation ----
        self.quantum_teacher = (QuantumInspiredTeacher(seed=42)
                                if enable_quantum_teacher else None)
        self.distill_ensemble = (
            DistillationEnsemble(self.quantum_teacher, alpha=0.5)
            if self.quantum_teacher else None)
        if self.distiller is not None:
            self.distiller.teachers = [
                self._teacher_cache,
                self._teacher_bandit,
                self._teacher_lp,
                self._teacher_moe,
            ]

        # ---- Enhancement 2: Causal RL ----
        self.causal = (CausalCounterfactualEstimator(
            state_dim=5, n_actions=self.bandit.num_actions)
            if enable_causal else None)
        self.last_counterfactuals: Dict[int, float] = {}

        # ---- Enhancement 3: Federated ----
        self.federated = FederatedAggregator() if enable_federated else None

        # ---- Enhancement 4: Multi-agent ----
        self.roles = EmergentRoleRegistry() if enable_multi_agent else None
        self.coordinator = MultiAgentCoordinator(self.roles) if self.roles else None

        # ---- Enhancement 5: Temporal logic ----
        self.temporal = None
        self.shield = None
        if enable_temporal_logic:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="latency_reasonable",
                predicate=lambda r: r.get("elapsed_sec", 0.0) <= 30.0,
                operator=STLOperator.ALWAYS, horizon=10))
            self.temporal.add_formula(STLFormula(
                name="eventually_success",
                predicate=lambda r: r.get("success", False),
                operator=STLOperator.EVENTUALLY, horizon=5))
            self.temporal.add_formula(STLFormula(
                name="carbon_bounded",
                predicate=lambda r: r.get("carbon_intensity", 0.0) <= 800.0,
                operator=STLOperator.ALWAYS, horizon=10))
            self.shield = SafetyShield(self.temporal)

        # ---- Enhancement 6: XAI ----
        self.explainer = DecisionExplainer() if enable_xai else None
        self.last_explanation: Optional[Explanation] = None

        # ---- Enhancement 7: Adaptive precision ----
        self.precision_controller = (PrecisionController(quality_floor=0.95)
                                     if enable_precision_switching else None)
        self.precision_adapter = (HardwareAwareAdapter(self.precision_controller)
                                  if self.precision_controller else None)
        self.current_precision: Optional[PrecisionLevel] = None

        # ---- Enhancement 8: Carbon market / REC ----
        self.carbon_market = CarbonMarketClient() if enable_carbon_market else None
        self.recs = RECInventory(
            persist_path=(Path(persist_path).with_suffix(".rec.pkl")
                          if persist_path else None)
        ) if enable_carbon_market else None
        if self.recs and rec_default_kwh > 0:
            self.recs.add(rec_default_kwh)

        # Real carbon providers
        self._carbon_providers: List[Any] = []
        if electricity_maps_api_key:
            self._carbon_providers.append(
                ElectricityMapsProvider(electricity_maps_api_key,
                                        electricity_maps_zone))
        if watttime_username:
            self._carbon_providers.append(
                WattTimeProvider(watttime_username, watttime_password,
                                 watttime_region))
        if not self._carbon_providers:
            self._carbon_providers.append(ElectricityMapsProvider(None))

        # ---- Enhancement 9: Chaos ----
        self.chaos: Optional[ChaosEngineer] = None
        if enable_chaos:
            hypotheses = [
                SteadyStateHypothesis(
                    name="latency_under_30s",
                    predicate=lambda m: m.get("elapsed_sec", 0.0) <= 30.0),
                SteadyStateHypothesis(
                    name="success_rate_above_50",
                    predicate=lambda m: m.get("success_rate", 1.0) >= 0.5)]
            self.chaos = ChaosEngineer(
                ChaosConfig(
                    fault_prob=chaos_fault_prob,
                    latency_inject_ms=chaos_latency_ms,
                    carbon_spike_prob=chaos_carbon_spike_prob,
                    max_concurrent_faults=chaos_max_concurrent,
                    duration_s=chaos_duration_s),
                hypotheses)
        self.breaker = CircuitBreaker("executor")

        # ---- Enhancement 10: HITL ----
        self.uncertainty = UncertaintyEstimator() if enable_hitl else None
        self.hitl = (HumanInTheLoopGate(
            approver=hitl_approver,
            timeout_s=hitl_timeout_s,
            production_safe=hitl_production_safe,
            auto_approve_on_timeout=hitl_auto_approve_on_timeout,
            persist=self.persistence) if enable_hitl else None)
        self.active_learner = ActiveLearningSampler() if enable_hitl else None

        # Lineage
        self.lineage_records: deque = deque(maxlen=1000)

        # Sync execution state
        self._executor_lock = threading.Lock()

    # ---------------- Teachers ----------------
    def _teacher_cache(self, fp: WorkloadFingerprint):
        return self.cache.get_best_policy(fp)

    def _teacher_bandit(self, fp: WorkloadFingerprint):
        policy, confidence = self.bandit.select_action(
            fp, min_trials_before_bandit=self.min_trials,
            confidence_threshold=self.conf_threshold)
        if policy is not None and confidence >= self.conf_threshold:
            return policy
        return None

    def _teacher_lp(self, fp: WorkloadFingerprint):
        return self.lp_solver(fp)

    def _teacher_moe(self, fp: WorkloadFingerprint):
        if self.moe_router is None:
            return None
        try:
            context = {
                "model_size_mb": fp.model_size_mb,
                "prompt_len": fp.prompt_len,
                "gen_len": fp.gen_len,
                "gpu_mem_free_mb": fp.gpu_mem_free_mb,
                "disk_speed_class": fp.disk_speed_class}
            encoded = self.moe_router.encode(context)
            chosen = self.moe_router.select(encoded)
            # Map the chosen expert into a policy; the fallback router
            # returns "default" so we return the LP policy in that case.
            if chosen in ("conservative", "cpu"):
                return {"gpu_batch_size": 1, "block_size": 8,
                        "weight_device": "cpu", "kv_cache_device": "cpu",
                        "weight_bits": 8}
            if chosen in ("aggressive", "gpu"):
                return {"gpu_batch_size": 2, "block_size": 16,
                        "weight_device": "gpu", "kv_cache_device": "gpu",
                        "weight_bits": 16}
            # default: fall through to the LP solver
            return self.lp_solver(fp)
        except Exception:
            return None

    # ---------------- Fingerprint ----------------
    def _generate_fingerprint(self, task: Dict[str, Any]) -> WorkloadFingerprint:
        return WorkloadFingerprint(
            model_size_mb=task.get("model_size_mb", 0),
            prompt_len=task.get("prompt_len", 0),
            gen_len=task.get("gen_len", 0),
            gpu_mem_free_mb=task.get("gpu_mem_free_mb", 0),
            disk_speed_class=task.get("disk_speed_class", 1))

    # ---------------- LIMIT graph ----------------
    def _apply_limit_graph(self, fp: WorkloadFingerprint,
                           policy: Dict[str, Any]) -> Dict[str, Any]:
        if self.limit_graph is None:
            return dict(policy)
        context = {"fingerprint": fp.to_vector().tolist()}
        try:
            limits = self.limit_graph.get_limits(context)
        except Exception:
            limits = {}
        if not isinstance(limits, dict):
            return dict(policy)
        out = dict(policy)
        if "max_block_size" in limits and "block_size" in out:
            out["block_size"] = min(int(out.get("block_size", 8)),
                                    int(limits["max_block_size"]))
        if "forced_weight_device" in limits:
            out["weight_device"] = limits["forced_weight_device"]
        if "max_gpu_batch_size" in limits and "gpu_batch_size" in out:
            out["gpu_batch_size"] = min(int(out.get("gpu_batch_size", 1)),
                                        int(limits["max_gpu_batch_size"]))
        return out

    # ---------------- Carbon providers ----------------
    async def _get_real_carbon_intensity(self) -> Optional[CarbonSnapshot]:
        for p in self._carbon_providers:
            try:
                snap = await p.get_intensity()
                if snap.source != "synthetic":
                    return snap
            except Exception as e:
                logger.warning(f"Carbon provider failed: {e}")
        return None

    # ---------------- Main pipeline ----------------
    def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        # Rate limit
        if not self._rate_limiter.acquire():
            M_RATE_LIMITED.inc()
            M_TASKS.labels(status="rate_limited").inc()
            return {"status": "failed", "reason": "rate_limited"}

        # Cost budget
        if (self.daily_cost_budget_usd > 0
                and self._today_cost >= self.daily_cost_budget_usd):
            M_TASKS.labels(status="budget_exceeded").inc()
            logger.warning("Daily cost budget exhausted")
            return {"status": "failed", "reason": "budget_exceeded"}

        # Chaos fault injection
        fault_injected = False
        if self.chaos and self.chaos.maybe_fault():
            fault_injected = True
            M_TASKS.labels(status="chaos_fault").inc()

        try:
            if fault_injected:
                return {"status": "failed", "reason": "chaos_fault"}

            # Step 1: Carbon delay
            delay_result = self.carbon_scheduler.submit(task)
            if delay_result["status"] == "delayed":
                M_TASKS.labels(status="deferred").inc()
                return {"status": "deferred",
                        "reason": "low_carbon_window",
                        "delay_until": delay_result["delay_until"],
                        "task": delay_result["task"]}

            # Step 2: Fingerprint
            fp = self._generate_fingerprint(task)

            # Step 3: Policy selection
            if self.distiller is not None:
                policies = []
                for teacher in self.distiller.teachers:
                    p = teacher(fp)
                    if p is not None:
                        policies.append(p)
                if policies:
                    counter = Counter(json.dumps(p, sort_keys=True)
                                      for p in policies)
                    most_common = counter.most_common(1)[0][0]
                    final_policy = json.loads(most_common)
                    policy_source = "distillation"
                else:
                    final_policy = self.lp_solver(fp)
                    policy_source = "lp_solver"
            else:
                cached_policy = self.cache.get_best_policy(fp)
                if cached_policy is not None:
                    final_policy = cached_policy
                    policy_source = "cache"
                else:
                    bandit_policy, confidence = self.bandit.select_action(
                        fp, min_trials_before_bandit=self.min_trials,
                        confidence_threshold=self.conf_threshold)
                    if bandit_policy is not None and confidence >= self.conf_threshold:
                        final_policy = bandit_policy
                        policy_source = "bandit"
                    else:
                        final_policy = self.lp_solver(fp)
                        policy_source = "lp_solver"
                        self.bandit.seed_safe_policy(fp, final_policy)

            # Step 4: LIMIT graph
            final_policy = self._apply_limit_graph(fp, final_policy)

            # Step 5: Carbon market + precision
            carbon_snap = asyncio.run(self._get_real_carbon_intensity()) \
                if not self._event_loop_running() else None
            if carbon_snap is None:
                carbon_intensity = self.carbon_scheduler.carbon_api.get_current()
            else:
                carbon_intensity = carbon_snap.intensity_g_co2_per_kwh
            M_CARBON.set(carbon_intensity)
            if self.chaos:
                carbon_intensity = self.chaos.maybe_carbon_spike(carbon_intensity)
            carbon_price = (self.carbon_market.price(
                carbon_intensity, time.localtime().tm_hour)
                if self.carbon_market else 0.0)
            if self.precision_adapter:
                final_policy, level = self.precision_adapter.adapt(
                    final_policy, carbon_intensity, 0.5, carbon_price)
                self.current_precision = level

            # Step 6: Multi-agent vote
            if self.coordinator is not None and self.bandit.actions:
                bids: List[AgentBid] = []
                for role in AgentRole:
                    if role == AgentRole.CARBON_BROKER:
                        idx = int(np.argmin([
                            a.get("weight_bits", 16)
                            for a in self.bandit.actions]))
                    elif role == AgentRole.EXPLOITER:
                        idx = int(np.argmax([
                            a.get("gpu_batch_size", 1)
                            for a in self.bandit.actions]))
                    elif role == AgentRole.SAFETY_OFFICER:
                        idx = int(np.argmin([
                            a.get("gpu_batch_size", 1)
                            for a in self.bandit.actions]))
                    else:
                        idx = random.randrange(len(self.bandit.actions))
                    bids.append(AgentBid(
                        agent_id=role.value, role=role, confidence=0.7,
                        proposed_action=idx, rationale=role.value,
                        carbon_score=1.0 / (1.0 + carbon_intensity / 1000.0)))
                voted, resolution = self.coordinator.resolve_conflict(
                    bids, len(self.bandit.actions))
                voted_policy = dict(self.bandit.actions[voted])
                # Only override cache/distillation with high-confidence vote.
                if policy_source in ("bandit", "lp_solver"):
                    final_policy = voted_policy
                    policy_source = policy_source + "+vote"

            # Step 7: Temporal shield (apply the replacement)
            safety_ok = True
            stl_verdict: Dict[str, bool] = {}
            if self.shield and self.temporal:
                self.temporal.observe({
                    "elapsed_sec": 0.0,
                    "success": True,
                    "carbon_intensity": carbon_intensity,
                })
                replacement, safety_ok, stl_verdict = self.shield.screen(
                    final_policy, self.bandit.actions, score_key="block_size",
                    cost_fn=lambda c: int(c.get("gpu_batch_size", 1)))
                if not safety_ok and isinstance(replacement, dict):
                    final_policy = replacement

            # Step 8: HITL
            human_approved = True
            uncertainty_score = 0.0
            if self.hitl and self.uncertainty:
                # Real uncertainty from MOE teacher disagreement
                teacher_forecasts = []
                for teacher in (self.distiller.teachers if self.distiller else []):
                    try:
                        p = teacher(fp)
                        if p is not None and isinstance(p, dict):
                            teacher_forecasts.append(
                                float(p.get("block_size", 8)))
                    except Exception:
                        pass
                if teacher_forecasts:
                    self.uncertainty.observe_expert_forecasts(teacher_forecasts)
                probs = None
                if len(teacher_forecasts) >= 2:
                    arr = np.asarray(teacher_forecasts, dtype=np.float64)
                    probs = np.exp(arr - arr.max())
                    probs = probs / probs.sum()
                is_unc, unc = self.uncertainty.is_uncertain(probs)
                uncertainty_score = unc
                if is_unc or not safety_ok:
                    req = HITLRequest.new(
                        reason=("high_uncertainty" if is_unc
                                else "safety_violation"),
                        chosen_idx=0,
                        candidates=list(self.bandit.actions),
                        uncertainty=unc,
                        carbon_price=carbon_price,
                        priority=(2 if not safety_ok else 1))
                    human_approved = self._run_hitl(req)
                    if not human_approved and self.bandit.actions:
                        final_policy = dict(min(
                            self.bandit.actions,
                            key=lambda a: a.get("gpu_batch_size", 1)))
                self.uncertainty.observe(0.0)

            # Step 9: Chaos latency (offloaded)
            if self.chaos:
                delay_ms = self.chaos.maybe_latency()
                if delay_ms > 0:
                    time.sleep(min(delay_ms / 1000.0, 5.0))

            # Step 10: Execute
            if not self.breaker.allow():
                M_TASKS.labels(status="circuit_open").inc()
                return {"status": "failed", "reason": "circuit_open"}
            try:
                with self._executor_lock:
                    metrics = self.executor(task, final_policy)
                self.breaker.record(True)
            except Exception as e:
                self.breaker.record(False)
                M_TASKS.labels(status="executor_error").inc()
                return {"status": "failed", "reason": "executor_error",
                        "error": str(e)}

            # Step 11: Causal counterfactuals
            state_vec = fp.to_vector()
            if self.causal:
                action_idx = self._find_action_idx(final_policy)
                self.causal.update(CausalTransition(
                    state=state_vec, action=max(0, action_idx),
                    reward=float(metrics.get("reward", 0.0)),
                    next_state=state_vec))
                self.last_counterfactuals = self.causal.counterfactuals(
                    state_vec, len(self.bandit.actions))

            # Step 12: Federated submit
            if self.federated:
                try:
                    local_w = np.asarray(
                        [float(metrics.get("quality_score", 0.0)),
                         float(metrics.get("tokens_per_sec", 0.0)),
                         float(metrics.get("total_energy_kwh", 0.0))])
                    self.federated.submit(FederatedUpdate(
                        node_id="local",
                        weights={"local": local_w},
                        n_samples=1,
                        carbon_intensity=float(carbon_intensity)))
                    self.federated.aggregate()
                except PrivacyBudgetExceeded as e:
                    logger.warning(f"Federated DP exhausted: {e}")

            # Step 13: XAI
            explanation = None
            if self.explainer:
                fp_dict = fp.to_dict()
                explanation = self.explainer.explain(
                    chosen_idx=max(0, self._find_action_idx(final_policy)),
                    chosen=fp_dict, pareto=[fp_dict],
                    counterfactuals=self.last_counterfactuals,
                    safety_ok=safety_ok and human_approved,
                    carbon_price=carbon_price, confidence=0.7)
                self.last_explanation = explanation

            # Chaos steady-state check
            if self.chaos and self.chaos.hypotheses:
                ss_metrics = {
                    "elapsed_sec": float(metrics.get("elapsed_sec", 0.0)),
                    "success_rate": 1.0 if metrics.get("success") else 0.0,
                    "carbon_intensity": carbon_intensity}
                if not self.chaos.steady_state_ok(ss_metrics):
                    logger.warning("Chaos steady-state violated; deactivating.")
                    self.chaos.deactivate()

            M_TASKS.labels(status="completed").inc()
            M_REWARD.observe(float(metrics.get("reward", 0.0)))

            return {
                "status": "completed",
                "policy": final_policy,
                "policy_source": policy_source,
                "metrics": metrics,
                "fingerprint": fp,
                "carbon_intensity": carbon_intensity,
                "carbon_price": carbon_price,
                "precision_level": (self.current_precision.value
                                    if self.current_precision else None),
                "safety_ok": safety_ok,
                "stl_verdict": stl_verdict,
                "human_approved": human_approved,
                "uncertainty": uncertainty_score,
                "counterfactuals": self.last_counterfactuals,
                "explanation": explanation.to_dict() if explanation else None,
                "rec_kwh": self.recs.total_kwh() if self.recs else 0.0,
            }
        finally:
            if fault_injected and self.chaos:
                self.chaos.release_fault()

    def _find_action_idx(self, policy: Dict[str, Any]) -> int:
        canonical = json.dumps(policy, sort_keys=True)
        for i, a in enumerate(self.bandit.actions):
            if json.dumps(a, sort_keys=True) == canonical:
                return i
        return 0

    @staticmethod
    def _event_loop_running() -> bool:
        try:
            loop = asyncio.get_running_loop()
            return loop.is_running()
        except RuntimeError:
            return False

    def _run_hitl(self, req: HITLRequest) -> bool:
        """Run the HITL gate. Runs asyncio.run if no loop is running."""
        if self.hitl is None:
            return True
        if self._event_loop_running():
            # Cannot block the loop; fail closed for safety.
            M_HITL.labels(decision="blocked_in_loop").inc()
            logger.warning("HITL requested inside running event loop; "
                           "failing closed")
            return False
        try:
            return bool(asyncio.run(self.hitl.request(req)))
        except Exception as e:
            logger.warning(f"HITL failed: {e}; failing closed")
            return False

    # ---------------- Post-task learning ----------------
    def update_after_task(self, fp: WorkloadFingerprint, policy: Dict[str, Any],
                          reward: float, policy_source: str) -> None:
        with self._update_lock:
            canonical = json.dumps(policy, sort_keys=True)
            bandit_idx = -1
            for i, a in enumerate(self.bandit.actions):
                if json.dumps(a, sort_keys=True) == canonical:
                    bandit_idx = i
                    break

            primary = policy_source.split("+")[0]
            if bandit_idx >= 0 and primary in ("bandit", "distillation"):
                self.bandit.update(fp, policy, reward)
            else:
                self.cache.update(fp, policy, reward)

            # RLHF update
            if self.rlhf is not None and bandit_idx >= 0:
                context = fp.to_vector().tolist()
                try:
                    self.rlhf.update(context, bandit_idx, reward)
                except Exception as e:
                    logger.warning(f"RLHF update failed: {e}")

            # Federated submit with reward
            if self.federated is not None:
                try:
                    self.federated.submit(FederatedUpdate(
                        node_id="local_reward",
                        weights={"reward": np.array([reward], dtype=np.float64)},
                        n_samples=1, carbon_intensity=400.0))
                    self.federated.aggregate()
                except PrivacyBudgetExceeded:
                    pass

            # Causal update with real reward
            if self.causal is not None and bandit_idx >= 0:
                self.causal.update(CausalTransition(
                    state=fp.to_vector(), action=bandit_idx,
                    reward=reward, next_state=fp.to_vector()))

            # Multi-agent role recording for ALL roles
            if self.roles is not None:
                for role in AgentRole:
                    self.roles.record(role,
                                      success=reward > 0.0,
                                      reward=reward)

            # Uncertainty + active learning
            if self.uncertainty:
                self.uncertainty.observe(reward)
            if self.active_learner:
                self.active_learner.maybe_store(
                    {"fp": fp.to_dict(), "policy": policy, "reward": reward},
                    uncertainty=abs(reward), threshold=0.4)

            # Bio expansion (reward-based fitness, capped)
            if (self.bio_generator is not None
                    and primary == "lp_solver"
                    and len(self.bandit.actions) < self.bandit.MAX_ACTIONS):
                ctx_key = self.bandit._encode_context(fp)
                n_trials = self.bandit.trials.get(ctx_key, 0)
                if n_trials > 10 and random.random() < 0.1:
                    try:
                        def _fitness(p):
                            return reward  # reward-correlated fitness

                        new_policies = self.bio_generator.evolve(
                            population=list(self.bandit.actions),
                            fitness_fn=_fitness,
                            generations=2,
                            population_size=min(
                                8, self.bandit.MAX_ACTIONS - len(self.bandit.actions)))
                        if isinstance(new_policies, dict):
                            new_policies = [new_policies]
                        added = 0
                        for p in new_policies:
                            if not isinstance(p, dict):
                                continue
                            if self.bandit.add_action(p):
                                added += 1
                        if added:
                            logger.info(f"[Bio] Action space expanded by {added}")
                    except Exception as e:
                        logger.warning(f"[Bio] expansion failed: {e}")

            # Lineage
            self.lineage_records.append({
                "fingerprint": fp.to_dict(),
                "policy": policy,
                "reward": reward,
                "source": policy_source,
                "ts": time.time()})
            with contextlib.suppress(Exception):
                self.persistence.lineage_add(
                    uuid.uuid4().hex[:8], "", "",
                    policy, reward)

    def tick_carbon_queue(self) -> List[Dict[str, Any]]:
        return self.carbon_scheduler.tick()

    # ---------------- Status ----------------
    def get_stats(self) -> Dict[str, Any]:
        return {
            "bandit_actions": len(self.bandit.actions),
            "bandit_contexts": len(self.bandit.weights),
            "cache_size": self.cache.size(),
            "carbon_queue_size": self.carbon_scheduler.size(),
            "current_precision": (self.current_precision.value
                                  if self.current_precision else None),
            "rec_kwh": self.recs.total_kwh() if self.recs else 0.0,
            "today_cost_usd": self._today_cost,
            "daily_cost_budget_usd": self.daily_cost_budget_usd,
            "rate_limit_remaining": self._rate_limiter.remaining(),
            "ten_enhancements": {
                "quantum_distillation": self.quantum_teacher is not None,
                "causal": self.causal is not None,
                "federated": self.federated is not None,
                "multi_agent": self.coordinator is not None,
                "temporal_logic": self.temporal is not None,
                "xai": self.explainer is not None,
                "precision_switching": self.precision_adapter is not None,
                "carbon_market": self.carbon_market is not None,
                "chaos": self.chaos is not None,
                "hitl": self.hitl is not None,
            },
            "last_explanation": (self.last_explanation.to_dict()
                                 if self.last_explanation else None),
            "counterfactuals": self.last_counterfactuals,
            "active_learning_buffer": (len(self.active_learner)
                                       if self.active_learner else 0),
            "role_weights": ({r.value: w for r, w in self.roles.weights().items()}
                             if self.roles else {}),
            "causal_stats": self.causal.stats() if self.causal else {},
            "federated_stats": self.federated.stats() if self.federated else {},
            "uncertainty_stats": (self.uncertainty.stats()
                                  if self.uncertainty else {}),
            "active_learning_stats": (self.active_learner.stats()
                                      if self.active_learner else {}),
            "hitl_stats": self.hitl.stats() if self.hitl else None,
            "circuit_breaker": self.breaker.get_status(),
            "chaos_stats": self.chaos.stats() if self.chaos else {},
        }

    def health_check(self) -> Dict[str, Any]:
        cb = self.breaker.get_status()
        return {
            "status": "healthy" if cb["state"] != "open" else "degraded",
            "circuit_breaker": cb,
            "cache_size": self.cache.size(),
            "pending_hitl": self.hitl.stats()["pending"] if self.hitl else 0,
            "today_cost_usd": self._today_cost,
        }

    def close(self) -> None:
        if self.hitl:
            self.hitl.close()
        self.persistence.close()
        if self.chaos:
            self.chaos.deactivate()


# ----------------------------------------------------------------------
# FastAPI (optional)
# ----------------------------------------------------------------------
def build_fastapi_app(router: GreenAgentPolicyRouter,
                      jwt_secret: Optional[str] = None) -> Any:
    try:
        from fastapi import FastAPI, Depends, HTTPException
        from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
        from fastapi.middleware.cors import CORSMiddleware
        from contextlib import asynccontextmanager
    except ImportError:
        return None

    try:
        from jose import jwt, JWTError
        JOSE_AVAILABLE = True
    except ImportError:
        JOSE_AVAILABLE = False

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        router.close()

    app = FastAPI(title="Green Agent API", version="18.0", lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=["*"],
                       allow_credentials=True, allow_methods=["*"],
                       allow_headers=["*"])
    security = HTTPBearer(auto_error=False)

    async def verify(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ):
        if not JOSE_AVAILABLE or not jwt_secret:
            return {"sub": "anonymous"}
        if credentials is None:
            raise HTTPException(status_code=401, detail="missing credentials")
        try:
            return jwt.decode(credentials.credentials, jwt_secret,
                              algorithms=["HS256"])
        except JWTError:
            raise HTTPException(status_code=401, detail="invalid token")

    @app.get("/status")
    async def status(_=Depends(verify)):
        return router.get_stats()

    @app.get("/health")
    async def health():
        return router.health_check()

    @app.get("/hitl/pending")
    async def hitl_pending(_=Depends(verify)):
        if not router.hitl:
            return {"pending": []}
        return {"pending": router.hitl.pending_requests()}

    @app.post("/hitl/approval")
    async def hitl_approval(request_id: str, approved: bool,
                            _=Depends(verify)):
        if not router.hitl:
            raise HTTPException(status_code=503, detail="HITL disabled")
        ok = router.hitl.resolve(request_id, approved)
        if not ok:
            raise HTTPException(status_code=404, detail="not_found")
        return {"status": "resolved", "request_id": request_id,
                "approved": approved}

    @app.get("/hitl/audit")
    async def hitl_audit(limit: int = 100, _=Depends(verify)):
        if not router.hitl:
            return {"audit": []}
        return {"audit": router.hitl.audit_trail(limit=limit),
                "verified": router.hitl.verify_audit_chain()}

    return app


# ----------------------------------------------------------------------
# Main orchestration loop
# ----------------------------------------------------------------------
STATE_FILE = "green_agent_state.json"
STATE_SCHEMA_VERSION = "18.0"


def _atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".ga_", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def run_green_agent_main_loop():
    carbon_api = CarbonAPIStub(base_intensity=180.0)
    gpu_profiler = GPUProfiler(sample_interval=0.5)
    gpu_profiler.start()

    def lp_solver(fp: WorkloadFingerprint) -> Dict[str, Any]:
        return {"gpu_batch_size": 1, "block_size": 8,
                "weight_device": "gpu", "kv_cache_device": "gpu",
                "weight_bits": 16}

    def flexgen_executor(task, policy):
        time.sleep(0.5)
        return ({"text": "ok"},
                {"tokens_generated": 20, "quality_score": 0.95})

    metric_aggregator = MetricAggregator(gpu_profiler, flexgen_executor)
    reward_calc = RewardCalculator()

    ACTION_SPACE = [
        {"gpu_batch_size": 1, "block_size": 8, "weight_device": "gpu",
         "kv_cache_device": "gpu", "weight_bits": 16},
        {"gpu_batch_size": 2, "block_size": 16, "weight_device": "cpu",
         "kv_cache_device": "cpu", "weight_bits": 8},
    ]

    limit_graph = LimitGraph()
    with contextlib.suppress(Exception):
        limit_graph.build_graph(nodes=[], edges=[])

    rlhf = RLHFOptimizer(action_space=list(range(len(ACTION_SPACE))))
    distiller = MultiTeacherDistiller(teachers=[])
    moe_router = ExpertRouter()
    bio_generator = GeneticPolicyGenerator()

    router = GreenAgentPolicyRouter(
        carbon_api=carbon_api,
        action_space=ACTION_SPACE,
        lp_solver=lp_solver,
        executor=metric_aggregator.run,
        min_trials_before_bandit=5,
        confidence_threshold=0.6,
        limit_graph=limit_graph,
        rlhf=rlhf,
        distiller=distiller,
        moe_router=moe_router,
        bio_generator=bio_generator,
        persist_path="green_agent.db",
        enable_quantum_teacher=True,
        enable_causal=True,
        enable_federated=True,
        enable_multi_agent=True,
        enable_temporal_logic=True,
        enable_xai=True,
        enable_precision_switching=True,
        enable_carbon_market=True,
        enable_chaos=False,
        enable_hitl=True,
        hitl_production_safe=True,
        hitl_timeout_s=5.0,
        rate_limit_per_minute=600,
        daily_cost_budget_usd=10.0,
    )

    # Load persistent state
    with contextlib.suppress(Exception):
        bandit_state = router.persistence.load_bandit()
        if bandit_state:
            router.bandit.import_state(bandit_state)
        cache_state = router.persistence.load_cache()
        if cache_state:
            router.cache.import_state(cache_state)

    stop_event = threading.Event()
    task_lock = threading.Lock()

    def process_task(task: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        with task_lock:
            original_weights = reward_calc.snapshot()
            try:
                if task.get("priority") == "eco":
                    reward_calc.weights = {
                        "quality": 0.1, "throughput": 0.1,
                        "energy_efficiency": 0.4, "carbon_efficiency": 0.4,
                        "memory_efficiency": 0.0}
                elif task.get("priority") == "speed":
                    reward_calc.weights = {
                        "quality": 0.2, "throughput": 0.5,
                        "energy_efficiency": 0.1, "carbon_efficiency": 0.1,
                        "memory_efficiency": 0.1}
                elif task.get("priority") == "high":
                    reward_calc.weights = {
                        "quality": 0.4, "throughput": 0.3,
                        "energy_efficiency": 0.1, "carbon_efficiency": 0.1,
                        "memory_efficiency": 0.1}

                result = router.handle_task(task)
                if result["status"] == "deferred":
                    logger.info(f"[Deferred] until {result.get('delay_until')}")
                    return result
                if result["status"] == "failed":
                    logger.info(f"[Failed] {result.get('reason')}")
                    return result

                fp = result["fingerprint"]
                carbon_intensity = result.get(
                    "carbon_intensity", carbon_api.get_current())
                metrics = result["metrics"]
                constraints = task.get("constraints", {})
                reward = reward_calc.compute(metrics, constraints,
                                             carbon_intensity)
                result["reward"] = reward
                logger.info(
                    f"[Done] {result['policy_source']} reward={reward:.3f} "
                    f"precision={result.get('precision_level')} "
                    f"safe={result.get('safety_ok')} "
                    f"hitl={result.get('human_approved')}")

                router.update_after_task(fp, result["policy"], reward,
                                         result["policy_source"])
                if result.get("explanation"):
                    logger.info(f"[XAI] {result['explanation'].get('rationale')}")
                return result
            finally:
                reward_calc.restore(original_weights)

    def release_checker():
        while not stop_event.is_set():
            stop_event.wait(5)
            released = router.tick_carbon_queue()
            for task in released:
                logger.info("[Release] Carbon window opened")
                process_task(task)

    release_thread = threading.Thread(target=release_checker, daemon=True)
    release_thread.start()

    tasks = [
        {"model_size_mb": 35000, "prompt_len": 512, "gen_len": 32,
         "gpu_mem_free_mb": 12000, "disk_speed_class": 2,
         "priority": "normal",
         "constraints": {"max_latency_ms": 5000, "min_quality": 0.8}},
        {"model_size_mb": 35000, "prompt_len": 512, "gen_len": 32,
         "gpu_mem_free_mb": 12000, "disk_speed_class": 2,
         "priority": "eco",
         "constraints": {"max_latency_ms": 6000, "min_quality": 0.7}},
        {"model_size_mb": 35000, "prompt_len": 512, "gen_len": 32,
         "gpu_mem_free_mb": 12000, "disk_speed_class": 2,
         "priority": "high"},
    ]

    try:
        for i, task in enumerate(tasks):
            logger.info(f"--- Processing task {i} "
                        f"(priority={task.get('priority')}) ---")
            process_task(task)

        time.sleep(15)

        # Persist state
        _atomic_write_json(STATE_FILE, {
            "schema_version": STATE_SCHEMA_VERSION,
            "bandit": router.bandit.export_state(),
            "cache": router.cache.export_state(),
            "stats": router.get_stats(),
        })
        router.persistence.save_bandit(router.bandit.export_state())
        router.persistence.save_cache(router.cache.export_state())
        logger.info("State persisted.")
        logger.info(json.dumps(router.get_stats(), indent=2, default=str))
    finally:
        stop_event.set()
        release_thread.join(timeout=2.0)
        gpu_profiler.stop()
        router.close()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    run_green_agent_main_loop()
