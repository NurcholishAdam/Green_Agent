#!/usr/bin/env python3
# src/enhancements/llm_client_enhanced_v18_0.py
# Version 18.0 — Full Green Agent stack (P0/P1/P2 hardened)
"""
Lightweight LLM client with all ten Green Agent enhancements built in.

P0 fixes:
 - GA no longer reinitializes on every evolve
 - SafetyShield replacement is applied to the pipeline
 - HITL is production-safe by default; has pending queue and resolve()
 - MOO weights are clamped to [0, 1] before normalizing
 - Retry policy is wired (tenacity optional, manual fallback)
 - Circuit breaker resets failure_count on half-open transition
 - Chaos carbon spike is always reverted (try/finally)
 - Causal RL exposes an active UCB policy
 - Multi-agent coordinator records role outcomes
 - Federated aggregator submits real weight vectors + DP accountant
 - MOERouter.update no longer called twice for the primary

P1 hardening:
 - SQLite persistence for cache, lineage, HITL audit, RLHF rewards, GA best
 - Real carbon providers (Electricity Maps / WattTime) with synthetic fallback
 - Token-bucket rate limiting
 - Cost tracking per call and per day
 - Correlation IDs in structured logs
 - Health checks
 - Graceful shutdown with drain
 - SHA-256 cache keys; embedding in executor; TTL eviction
 - FastAPI endpoints for /generate, /status, /health, /cache, /hitl
 - PII redaction

P2 advanced:
 - Full STL (G / F / U / X / ->) with actionable enforcement
 - Secure federated aggregation with pairwise masks + DP accountant
 - Multi-agent message bus + conflict resolution
 - Chaos steady-state, blast radius, auto-deactivation
 - Adaptive precision with telemetry-driven selection
 - Active learning with KMeans diversity + Platt calibration
 - Hash-chained HITL audit ledger
 - Cost-aware arm selection
"""

from __future__ import annotations

import asyncio
import atexit
import base64
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
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, AsyncIterable, Callable, Dict, List, Optional, Sequence,
    Tuple, Union,
)

import numpy as np

import aiohttp

try:
    from tenacity import (
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type, RetryError,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

    class RetryError(Exception):
        pass

    def retry(*a, **kw):
        def deco(fn):
            async def wrapper(*args, **kwargs):
                return await fn(*args, **kwargs)
            return wrapper
        return deco

    def stop_after_attempt(*a, **kw): return None
    def wait_exponential(*a, **kw): return None
    def retry_if_exception_type(*a, **kw): return None


# ---------- Optional dependencies ----------
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from hvac import Client as VaultClient
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    from sklearn.cluster import KMeans
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


# ---------- Logger ----------
logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# ---------- Prometheus metrics ----------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    METRIC_REQUESTS = Counter("llm_requests_total", "Total LLM requests", ["status"], registry=REGISTRY)
    METRIC_DURATION = Histogram("llm_request_duration_seconds", "LLM request duration", registry=REGISTRY)
    METRIC_CB_STATE = Gauge("llm_circuit_breaker_state", "Circuit breaker state", ["endpoint"], registry=REGISTRY)
    METRIC_CACHE_HITS = Counter("llm_cache_hits_total", "Cache hits", registry=REGISTRY)
    METRIC_CACHE_MISSES = Counter("llm_cache_misses_total", "Cache misses", registry=REGISTRY)
    METRIC_FALLBACK = Counter("llm_fallback_usage_total", "Fallback usage", registry=REGISTRY)
    METRIC_RETRY = Counter("llm_retry_count_total", "Retry count", registry=REGISTRY)
    METRIC_TOKENS = Counter("llm_token_usage_total", "Token usage", registry=REGISTRY)
    METRIC_COST = Counter("llm_cost_usd_total", "Cumulative cost (USD)", registry=REGISTRY)
    METRIC_CARBON = Gauge("llm_carbon_intensity", "Carbon intensity", registry=REGISTRY)
    METRIC_CARBON_PRICE = Gauge("llm_carbon_price_per_kg", "Carbon price", registry=REGISTRY)
    METRIC_REC = Gauge("llm_rec_inventory_kwh", "REC inventory kWh", registry=REGISTRY)
    METRIC_PRECISION = Counter("llm_precision_switches_total", "Precision switches", ["level"], registry=REGISTRY)
    METRIC_PRECISION_ENERGY = Gauge("llm_precision_energy_joules", "Precision energy", ["level"], registry=REGISTRY)
    METRIC_CHAOS = Counter("llm_chaos_events_total", "Chaos events", ["type"], registry=REGISTRY)
    METRIC_HITL = Counter("llm_hitl_approvals_total", "HITL approvals", ["decision"], registry=REGISTRY)
    METRIC_HITL_PENDING = Gauge("llm_hitl_pending", "Pending HITL requests", registry=REGISTRY)
    METRIC_SAFETY = Counter("llm_safety_violations_total", "Safety violations", ["formula"], registry=REGISTRY)
    METRIC_FEDERATED = Counter("llm_federated_rounds_total", "Federated rounds", registry=REGISTRY)
    METRIC_DP_EPSILON = Gauge("llm_dp_epsilon_spent", "DP epsilon spent", ["node"], registry=REGISTRY)
    METRIC_MOE_GATE = Gauge("llm_moe_gating_weight", "MOE gating weight", ["teacher"], registry=REGISTRY)
    METRIC_RATE_LIMITED = Counter("llm_rate_limited_total", "Rate limited calls", registry=REGISTRY)
    METRIC_ACTIVE_LEARNING = Gauge("llm_active_learning_size", "Active learning buffer size", registry=REGISTRY)
else:
    class _DummyMetric:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self

    METRIC_REQUESTS = METRIC_DURATION = METRIC_CB_STATE = _DummyMetric()
    METRIC_CACHE_HITS = METRIC_CACHE_MISSES = METRIC_FALLBACK = _DummyMetric()
    METRIC_RETRY = METRIC_TOKENS = METRIC_CARBON = METRIC_CARBON_PRICE = _DummyMetric()
    METRIC_REC = METRIC_PRECISION = METRIC_PRECISION_ENERGY = METRIC_CHAOS = _DummyMetric()
    METRIC_HITL = METRIC_HITL_PENDING = METRIC_SAFETY = METRIC_FEDERATED = _DummyMetric()
    METRIC_DP_EPSILON = METRIC_MOE_GATE = METRIC_RATE_LIMITED = _DummyMetric()
    METRIC_ACTIVE_LEARNING = METRIC_COST = _DummyMetric()


# ===========================================================================
# Configuration
# ===========================================================================
@dataclass
class LLMClientConfig:
    endpoint: str = "http://localhost:8000/generate"
    model: str = "small"
    headers: Dict[str, str] = field(default_factory=dict)
    timeout: int = 30
    retry_attempts: int = 3
    retry_backoff_base: float = 0.5
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 30.0
    rate_limit_per_minute: int = 600
    daily_cost_budget_usd: float = 10.0
    cost_per_1k_tokens: float = 0.002

    cache_enabled: bool = True
    cache_max_size: int = 1000
    cache_ttl_s: float = 3600.0
    cache_similarity_threshold: float = 0.95

    persist_path: Optional[str] = None  # SQLite path
    lineage_enabled: bool = False
    pii_redaction_enabled: bool = True

    carbon_api_key: Optional[str] = None
    carbon_region: str = "global"
    electricity_maps_api_key: Optional[str] = None
    electricity_maps_zone: str = "US-CAL-CISO"
    watttime_username: Optional[str] = None
    watttime_password: Optional[str] = None
    watttime_region: str = "CAISO_NORTH"

    vault_url: Optional[str] = None
    vault_token: Optional[str] = None
    vault_secret_path: str = "llm/api_key"

    extra_endpoints: List[Tuple[str, str]] = field(default_factory=list)
    node_id: Optional[str] = None

    # Feature toggles
    enable_contextual_bandit: bool = True
    enable_moe: bool = True
    enable_ga: bool = True
    enable_carbon_aware: bool = True
    enable_self_healing: bool = True
    enable_lineage: bool = False
    enable_limit_graph: bool = True
    enable_rlhf: bool = True
    enable_distillation: bool = True
    enable_quantum_teacher: bool = True
    enable_causal: bool = True
    enable_federated: bool = True
    enable_multi_agent: bool = True
    enable_temporal_logic: bool = True
    enable_xai: bool = True
    enable_precision_switching: bool = True
    enable_carbon_market: bool = True
    enable_chaos: bool = False
    enable_hitl: bool = True

    ga_evolve_every: int = 10
    hitl_timeout_s: float = 30.0
    hitl_production_safe: bool = True
    hitl_auto_approve_on_timeout: bool = False

    chaos_fault_prob: float = 0.0
    chaos_latency_ms: float = 0.0
    chaos_carbon_spike_prob: float = 0.0
    chaos_max_concurrent: int = 1
    chaos_duration_s: float = 0.0

    federated_dp_sigma: float = 1e-3
    federated_target_epsilon: float = 8.0
    federated_target_delta: float = 1e-5


# ===========================================================================
# Utilities
# ===========================================================================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


_PII_PATTERNS = [
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), "[EMAIL]"),
    (re.compile(r"\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"), "[PHONE]"),
    (re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"), "[AWS_KEY]"),
    (re.compile(r"\b(?:sk|pk|ghp|xox[baprs])[-_A-Za-z0-9]{10,}\b"), "[API_KEY]"),
    (re.compile(r"\b(?:\d[ -]?){13,19}\b"), "[CARD]"),
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "[SSN]"),
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


# ===========================================================================
# Persistence (SQLite)
# ===========================================================================
class Persistence:
    """
    Single shared SQLite store for cache, lineage, HITL audit, RLHF rewards,
    GA population, and cost ledger. All methods are synchronous and must be
    called from a worker thread when invoked from async code.
    """

    SCHEMA = """
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS cache (
        key TEXT PRIMARY KEY,
        prompt TEXT NOT NULL,
        value TEXT NOT NULL,
        embedding BLOB,
        ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS lineage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL,
        request_id TEXT,
        prompt TEXT,
        response TEXT,
        params TEXT
    );
    CREATE TABLE IF NOT EXISTS hitl_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT NOT NULL,
        approved INTEGER NOT NULL,
        reason TEXT,
        mode TEXT,
        latency_ms REAL,
        prev_hash TEXT,
        entry_hash TEXT NOT NULL,
        ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS rlhf (
        key TEXT PRIMARY KEY,
        score REAL NOT NULL,
        updated_ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ga_best (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        arm TEXT NOT NULL,
        fitness REAL NOT NULL,
        ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS cost_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL,
        tokens INTEGER NOT NULL,
        cost_usd REAL NOT NULL
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

    def _execute(self, sql: str, params: tuple = ()):
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

    # --- Cache ---
    def cache_get(self, key: str) -> Optional[Tuple[str, float]]:
        rows = self._fetchall("SELECT value, ts FROM cache WHERE key=?", (key,))
        return (rows[0][0], float(rows[0][1])) if rows else None

    def cache_set(self, key: str, prompt: str, value: str,
                  embedding: Optional[np.ndarray], ts: float):
        emb_blob = pickle.dumps(embedding) if embedding is not None else None
        self._execute(
            "INSERT OR REPLACE INTO cache (key, prompt, value, embedding, ts) VALUES (?,?,?,?,?)",
            (key, prompt, value, emb_blob, ts))

    def cache_all(self) -> List[Tuple[str, str, Optional[np.ndarray], float]]:
        rows = self._fetchall("SELECT key, prompt, embedding, ts FROM cache")
        out = []
        for k, prompt, emb_blob, ts in rows:
            emb = pickle.loads(emb_blob) if emb_blob else None
            out.append((k, prompt, emb, float(ts)))
        return out

    def cache_evict_oldest(self):
        self._execute(
            "DELETE FROM cache WHERE key IN (SELECT key FROM cache ORDER BY ts ASC LIMIT 1)")

    def cache_delete_older_than(self, cutoff: float):
        self._execute("DELETE FROM cache WHERE ts < ?", (cutoff,))

    def cache_size(self) -> int:
        rows = self._fetchall("SELECT COUNT(*) FROM cache")
        return int(rows[0][0]) if rows else 0

    # --- Lineage ---
    def lineage_add(self, request_id: str, prompt: str, response: str,
                    params: Dict[str, Any], ts: float):
        self._execute(
            "INSERT INTO lineage (ts, request_id, prompt, response, params) VALUES (?,?,?,?,?)",
            (ts, request_id, prompt, response, json.dumps(params, default=str)))

    # --- HITL audit ---
    def hitl_append(self, entry: Dict[str, Any]):
        self._execute(
            "INSERT INTO hitl_audit (request_id, approved, reason, mode, latency_ms, prev_hash, entry_hash, ts) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (entry["request_id"], int(bool(entry["approved"])),
             entry.get("reason", ""), entry.get("mode", ""),
             float(entry.get("latency_ms", 0.0)),
             entry.get("prev_hash", ""), entry.get("entry_hash", ""),
             float(entry.get("timestamp", time.time()))))

    def hitl_recent(self, limit: int = 100) -> List[Tuple]:
        return self._fetchall(
            "SELECT request_id, approved, reason, mode, latency_ms, prev_hash, entry_hash, ts "
            "FROM hitl_audit ORDER BY id DESC LIMIT ?", (limit,))

    def hitl_last_hash(self) -> str:
        rows = self._fetchall(
            "SELECT entry_hash FROM hitl_audit ORDER BY id DESC LIMIT 1")
        return rows[0][0] if rows else ""

    # --- RLHF ---
    def rlhf_set(self, key: str, score: float):
        self._execute(
            "INSERT OR REPLACE INTO rlhf (key, score, updated_ts) VALUES (?,?,?)",
            (key, float(score), time.time()))

    def rlhf_all(self) -> Dict[str, float]:
        return {k: float(v) for k, v in self._fetchall("SELECT key, score FROM rlhf")}

    # --- GA ---
    def ga_record(self, arm: Dict[str, Any], fitness: float):
        self._execute(
            "INSERT INTO ga_best (arm, fitness, ts) VALUES (?,?,?)",
            (json.dumps(arm, default=str), float(fitness), time.time()))

    def ga_best(self) -> Optional[Dict[str, Any]]:
        rows = self._fetchall(
            "SELECT arm FROM ga_best ORDER BY fitness DESC LIMIT 1")
        if not rows:
            return None
        try:
            return json.loads(rows[0][0])
        except Exception:
            return None

    # --- Cost ---
    def cost_record(self, tokens: int, cost_usd: float):
        self._execute(
            "INSERT INTO cost_ledger (ts, tokens, cost_usd) VALUES (?,?,?)",
            (time.time(), int(tokens), float(cost_usd)))

    def cost_today(self) -> float:
        day_start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp()
        rows = self._fetchall(
            "SELECT COALESCE(SUM(cost_usd), 0) FROM cost_ledger WHERE ts >= ?",
            (day_start,))
        return float(rows[0][0]) if rows else 0.0


# ===========================================================================
# TEN ENHANCEMENT MODULES
# ===========================================================================
# ---- 7: Adaptive precision ----
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

    def observe(self, t: HardwareTelemetry) -> None:
        self._telemetry.append(t)
        alpha = 0.2
        self._ema_quality_drop = (1 - alpha) * self._ema_quality_drop + \
                                 alpha * t.observed_quality_drop

    def _thermal_penalty(self) -> float:
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
        floor = max(self.quality_floor, self.quality_floor - self._ema_quality_drop * 0.5)
        chosen = ordered[0]
        for i, p in enumerate(ordered):
            if PRECISION_COST[p]["quality"] >= floor and agg >= 0.2 * i:
                chosen = p
        return chosen

    def stats(self) -> Dict[str, Any]:
        if not self._telemetry:
            return {"samples": 0}
        recent = list(self._telemetry)[-20:]
        return {
            "samples": len(self._telemetry),
            "avg_temp_c": float(np.mean([h.temperature_c for h in recent])),
            "avg_power_w": float(np.mean([h.power_watts for h in recent])),
            "ema_quality_drop": self._ema_quality_drop,
        }


class HardwareAwareAdapter:
    def __init__(self, controller: PrecisionController):
        self.controller = controller

    def adapt(self, params: Dict[str, Any], carbon_intensity: float,
              latency_headroom: float, carbon_price: float
              ) -> Tuple[Dict[str, Any], PrecisionLevel]:
        level = self.controller.select(carbon_intensity, latency_headroom, carbon_price)
        out = dict(params)
        out["precision_level"] = level.value
        out["effective_bits"] = PRECISION_COST[level]["bits"]
        METRIC_PRECISION.labels(level=level.value).inc()
        METRIC_PRECISION_ENERGY.labels(level=level.value).set(
            PRECISION_COST[level]["energy"])
        return out, level


# ---- 8: Carbon markets / RECs ----
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
        METRIC_REC.set(self.total_kwh())

    def total_kwh(self) -> float:
        with self._lock:
            return sum(r.kwh for r in self._records)

    def consume(self, kwh: float) -> float:
        with self._lock:
            remaining = kwh
            offset = 0.0
            new_records: List[RECRecord] = []
            consumed: List[Dict[str, Any]] = []
            for r in self._records:
                if remaining <= 0:
                    new_records.append(r)
                    continue
                take = min(r.kwh, remaining)
                remaining -= take
                offset += take * self.grid_factor
                consumed.append({"cert_id": r.cert_id, "kwh": take})
                if r.kwh - take > 1e-9:
                    new_records.append(RECRecord(
                        kwh=r.kwh - take, issued_at=r.issued_at, source=r.source))
            self._records = new_records
            if consumed:
                self._ledger.append({"op": "consume", "items": consumed,
                                     "t": time.time()})
                self._save()
        METRIC_REC.set(self.total_kwh())
        return offset

    def audit(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._ledger)


class CarbonIntensityManager:
    """Real providers with synthetic fallback; always returns CarbonSnapshot."""

    def __init__(self, market: Optional[CarbonMarketClient] = None,
                 recs: Optional[RECInventory] = None,
                 electricity_maps_api_key: Optional[str] = None,
                 electricity_maps_zone: str = "US-CAL-CISO",
                 watttime_username: Optional[str] = None,
                 watttime_password: Optional[str] = None,
                 watttime_region: str = "CAISO_NORTH",
                 region: str = "global"):
        self.market = market or CarbonMarketClient()
        self.recs = recs or RECInventory()
        self.region = region
        self.current = CarbonSnapshot(400.0, 0.05, "synthetic", region=region)
        self.providers: List[Any] = []
        if electricity_maps_api_key:
            self.providers.append(ElectricityMapsProvider(
                electricity_maps_api_key, electricity_maps_zone))
        if watttime_username:
            self.providers.append(WattTimeProvider(
                watttime_username, watttime_password, watttime_region))
        if not self.providers:
            self.providers.append(ElectricityMapsProvider(None, electricity_maps_zone))

    async def refresh(self) -> CarbonSnapshot:
        best: Optional[CarbonSnapshot] = None
        for p in self.providers:
            try:
                snap = await p.get_intensity()
                if best is None or snap.source != "synthetic":
                    best = snap
                    if snap.source != "synthetic":
                        break
            except Exception as e:
                logger.warning(f"Carbon provider failed: {e}")
        if best is not None:
            hour = utcnow().hour
            self.current = CarbonSnapshot(
                best.intensity_g_co2_per_kwh,
                self.market.price(best.intensity_g_co2_per_kwh, hour),
                best.source, region=best.region)
        METRIC_CARBON.set(self.current.intensity_g_co2_per_kwh)
        METRIC_CARBON_PRICE.set(self.current.price_usd_per_kg)
        METRIC_REC.set(self.recs.total_kwh())
        return self.current

    async def get_current_snapshot(self) -> CarbonSnapshot:
        return self.current

    async def get_current_intensity(self) -> float:
        return self.current.intensity_g_co2_per_kwh

    async def get_current_price(self, hour_of_day: Optional[int] = None) -> float:
        return self.market.price(self.current.intensity_g_co2_per_kwh, hour_of_day)

    async def close(self) -> None:
        pass


# ---- 9: Chaos (P2: steady-state, blast radius, auto-deactivation) ----
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
        self._lock = asyncio.Lock()

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
            self._active_faults += 1
            self.events.append({"type": "fault", "t": time.time()})
            METRIC_CHAOS.labels(type="fault").inc()
            return True
        return False

    def release_fault(self):
        self._active_faults = max(0, self._active_faults - 1)

    def maybe_latency(self) -> float:
        if not self._can_inject():
            return 0.0
        if (self.config.latency_inject_ms > 0
                and self.rng.random() < self.config.latency_inject_prob):
            self.events.append({"type": "latency", "t": time.time()})
            METRIC_CHAOS.labels(type="latency").inc()
            return self.config.latency_inject_ms
        return 0.0

    def maybe_carbon_spike(self, carbon_intensity: float) -> float:
        if not self._can_inject():
            return carbon_intensity
        if self.rng.random() < self.config.carbon_spike_prob:
            self.events.append({"type": "carbon_spike", "t": time.time()})
            METRIC_CHAOS.labels(type="carbon_spike").inc()
            return carbon_intensity * self.config.carbon_spike_factor
        return carbon_intensity

    def verify_steady_state(self, metrics: Dict[str, Any]) -> Dict[str, bool]:
        return {h.name: bool(h.predicate(metrics)) for h in self.hypotheses}

    def steady_state_ok(self, metrics: Dict[str, Any]) -> bool:
        return all(self.verify_steady_state(metrics).values())

    def deactivate(self):
        self.config = ChaosConfig(seed=self.config.seed)
        self._active_faults = 0

    def stats(self) -> Dict[str, Any]:
        return {"config": asdict(self.config), "events": len(self.events),
                "active_faults": self._active_faults,
                "hypotheses": [h.name for h in self.hypotheses]}


# ---- 5: Temporal logic (P2: full STL) ----
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

    def add_formula(self, f: STLFormula) -> None:
        self.formulas.append(f)

    def observe(self, record: Dict[str, Any]) -> None:
        self._history.append({**record, "_t": time.time()})

    def verify(self) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for f in self.formulas:
            window = list(self._history)[-f.horizon:]
            results[f.name] = f.evaluate(window)
            if not results[f.name]:
                METRIC_SAFETY.labels(formula=f.name).inc()
                self.violations.append(STLViolation(
                    formula=f.name, operator=f.operator.value,
                    window_size=len(window), timestamp=time.time()))
        return results

    def recent_violations(self, n: int = 10) -> List[Dict[str, Any]]:
        return [asdict(v) for v in self.violations[-n:]]


class SafetyShield:
    """
    P2: returns an actionable replacement. The caller must apply it.
    The `screen` method returns (chosen, safety_ok, verdict).
    """

    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor
        self.violations: List[Dict[str, Any]] = []

    def screen(self, selected: Dict[str, Any],
               candidates: Sequence[Dict[str, Any]],
               score_key: str = "score",
               cost_fn: Optional[Callable[[Dict[str, Any]], float]] = None,
               ) -> Tuple[Dict[str, Any], bool, Dict[str, bool]]:
        verdict = self.monitor.verify()
        violated = [k for k, ok in verdict.items() if not ok]
        if not violated:
            return selected, True, verdict
        if not candidates:
            return selected, False, verdict
        if cost_fn is None:
            cost_fn = lambda c: float(c.get(score_key, 0.0))
        safe = min(candidates, key=cost_fn)
        self.violations.append({"t": time.time(), "violated": violated,
                                "original": dict(selected),
                                "replacement": dict(safe)})
        return dict(safe), False, verdict


# ---- 2: Causal RL (P0/P2: active UCB policy) ----
@dataclass
class CausalTransition:
    state: np.ndarray
    action: int
    reward: float
    next_state: np.ndarray
    context: Dict[str, float] = field(default_factory=dict)


class CausalCounterfactualEstimator:
    def __init__(self, state_dim: int, n_actions: int,
                 ridge: float = 1e-3, exploration_c: float = 0.5):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.ridge = ridge
        self.exploration_c = exploration_c
        self._A = np.eye(state_dim + n_actions + 1) * ridge
        self._b = np.zeros(state_dim + n_actions + 1)
        self._action_counts = np.zeros(n_actions)
        self._n = 0

    def _features(self, s: np.ndarray, a: int) -> np.ndarray:
        one_hot = np.zeros(self.n_actions)
        one_hot[a % self.n_actions] = 1.0
        return np.concatenate([np.asarray(s, dtype=np.float64), one_hot, [1.0]])

    def _ensure_actions(self, n: int) -> None:
        if n == self.n_actions:
            return
        self._A = np.eye(self.state_dim + n + 1) * self.ridge
        self._b = np.zeros(self.state_dim + n + 1)
        self.n_actions = n
        self._action_counts = np.zeros(n)
        self._n = 0

    def update(self, t: CausalTransition) -> None:
        a = int(t.action) % max(self.n_actions, 1)
        x = self._features(t.state, a)
        self._A += np.outer(x, x)
        self._b += t.reward * x
        self._action_counts[a] += 1
        self._n += 1

    def predict(self, s: np.ndarray, a: int) -> float:
        if self._n < 2:
            return 0.0
        try:
            theta = np.linalg.solve(self._A, self._b)
        except np.linalg.LinAlgError:
            return 0.0
        return float(self._features(s, a) @ theta)

    def counterfactuals(self, s: np.ndarray, n_actions: int) -> Dict[int, float]:
        self._ensure_actions(n_actions)
        return {a: self.predict(s, a) for a in range(n_actions)}

    def select_action(self, s: np.ndarray, n_actions: int,
                      epsilon: float = 0.05) -> int:
        cf = self.counterfactuals(s, n_actions)
        if not cf:
            return 0
        if random.random() < epsilon:
            return random.randrange(self.n_actions)
        total = max(self._n, 1)
        scores = {}
        for a, mu in cf.items():
            count = max(self._action_counts[a], 1.0)
            bonus = self.exploration_c * math.sqrt(math.log(total + 1) / count)
            scores[a] = mu + bonus
        return max(scores, key=scores.get)


# ---- 3: Federated (P2: secure + DP accountant) ----
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
        eps_step = sensitivity * math.sqrt(2 * math.log(1.25 / self.target_delta)) / sigma
        self.spent += eps_step
        self._rounds += 1

    def can_spend(self) -> bool:
        return self.spent < self.target_epsilon


class PrivacyBudgetExceeded(Exception):
    pass


class SecureFederatedAggregator:
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
                if x == a:
                    m = m + v
                elif y == a:
                    m = m - v
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
            METRIC_DP_EPSILON.labels(node=nid).set(self.accountant.spent)
        self._global = agg
        METRIC_FEDERATED.inc()
        return agg

    def stats(self) -> Dict[str, Any]:
        return {"tracked_nodes": len(self._updates),
                "dp_spent": self.accountant.spent,
                "dp_target": self.accountant.target_epsilon}


# ---- 4: Multi-agent (P2: message bus + role learning) ----
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
            return {
                "scores": {r.value: self._scores[r] for r in AgentRole},
                "counts": {r.value: self._counts[r] for r in AgentRole},
                "success_rate": {
                    r.value: (self._successes[r] / self._counts[r])
                    if self._counts[r] else 0.0
                    for r in AgentRole},
            }


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

    def broadcast(self, sender: str, topic: str, payload: Dict[str, Any]):
        self.messages.append(AgentMessage(
            sender=sender, recipient="*", topic=topic, payload=payload))

    def vote(self, bids: Sequence[AgentBid], n_candidates: int,
             dominant_override: Optional[AgentRole] = None) -> int:
        if not bids or n_candidates <= 0:
            return 0
        if dominant_override is not None:
            for b in bids:
                if b.role == dominant_override:
                    return b.proposed_action % n_candidates
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


# ---- 6: XAI (P2: SHAP -> z-score + ablation) ----
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

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DecisionExplainer:
    def __init__(self, feature_names: Optional[Sequence[str]] = None,
                 predict_fn: Optional[Callable] = None):
        self.feature_names = list(feature_names) if feature_names else [
            "prompt_len", "num_words", "avg_word_len", "hour_norm"]
        self.predict_fn = predict_fn
        self._shap_explainer = None

    def fit(self, background: np.ndarray) -> None:
        if SHAP_AVAILABLE and self.predict_fn is not None:
            try:
                self._shap_explainer = shap.KernelExplainer(
                    self.predict_fn, background)
            except Exception as e:
                logger.warning(f"SHAP init failed: {e}")

    def _z_attributions(self, chosen: Dict[str, Any],
                        pareto: Sequence[Dict[str, Any]]) -> List[Tuple[str, float]]:
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

    def _shap_attributions(self, x: np.ndarray) -> Optional[List[Tuple[str, float]]]:
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
        attrs = None
        if self._shap_explainer is not None:
            x = np.array([float(chosen.get(f, 0.0)) for f in self.feature_names])
            attrs = self._shap_attributions(x)
            if attrs is not None:
                method = "shap"
        if attrs is None:
            attrs = self._z_attributions(chosen, pareto)
        cf_list: List[Dict[str, Any]] = []
        for idx, score in sorted(counterfactuals.items(),
                                 key=lambda kv: kv[1], reverse=True)[:3]:
            if 0 <= idx < len(pareto):
                cf_list.append({"alternative_idx": idx,
                                "expected_reward": float(score)})
        top_feat = ", ".join(f"{n}={v:+.2f}" for n, v in attrs[:3])
        rationale = (
            f"Chose arm #{chosen_idx} (confidence={confidence:.2f}, method={method}). "
            f"Top deviations: {top_feat}. Carbon price={carbon_price:.4f}. "
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


# ---- 1: Quantum distillation ----
class QuantumInspiredTeacher:
    def __init__(self, n_qubits: int = 5, seed: int = 0):
        self.n_qubits = n_qubits
        self.rng = np.random.default_rng(seed)
        self._params = self.rng.normal(size=(n_qubits, 2)) * 0.5

    def _ry(self, state: np.ndarray, theta: float, q: int) -> np.ndarray:
        c, s = math.cos(theta / 2), math.sin(theta / 2)
        n = len(state)
        new = state.copy()
        step = 1 << q
        for i in range(n):
            if i & step == 0:
                a, b = state[i], state[i | step]
                new[i] = c * a - s * b
                new[i | step] = s * a + c * b
        return new

    def _simulate(self, features: np.ndarray, n_candidates: int) -> np.ndarray:
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128)
        state[0] = 1.0
        for q in range(self.n_qubits):
            angle = float(self._params[q, 0] * features[q % len(features)]
                          + self._params[q, 1])
            state = self._ry(state, angle, q)
        probs_full = np.abs(state) ** 2
        probs = np.zeros(n_candidates, dtype=np.float64)
        for i, p in enumerate(probs_full):
            probs[i % n_candidates] += p
        return probs / max(probs.sum(), 1e-12)

    def teacher_probs(self, features: np.ndarray, n: int) -> np.ndarray:
        if n <= 0:
            return np.zeros(0)
        return self._simulate(np.asarray(features, dtype=np.float64), n)


class DistillationEnsemble:
    def __init__(self, quantum: QuantumInspiredTeacher, alpha: float = 0.5):
        self.quantum = quantum
        self.alpha = alpha

    def blend(self, features: np.ndarray, other: Optional[np.ndarray],
              n: int) -> np.ndarray:
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
        return cls(
            request_id=uuid.uuid4().hex[:12], reason=reason,
            chosen_idx=chosen_idx, candidates=list(candidates),
            uncertainty=float(uncertainty), carbon_price=float(carbon_price),
            priority=int(priority), metadata=dict(metadata or {}))


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

    def entropy(self, probs: Optional[np.ndarray]) -> float:
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

    def is_uncertain(self, probs: Optional[np.ndarray]) -> Tuple[bool, float]:
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


class HITLAuditEntry:
    def __init__(self, request_id: str, approved: bool, reason: str, mode: str,
                 latency_ms: float = 0.0, prev_hash: str = "",
                 entry_hash: str = "", timestamp: Optional[float] = None):
        self.request_id = request_id
        self.approved = bool(approved)
        self.reason = reason
        self.mode = mode
        self.latency_ms = float(latency_ms)
        self.prev_hash = prev_hash
        self.entry_hash = entry_hash
        self.timestamp = timestamp if timestamp is not None else time.time()

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
        return {
            "request_id": self.request_id,
            "approved": self.approved,
            "reason": self.reason,
            "mode": self.mode,
            "latency_ms": self.latency_ms,
            "prev_hash": self.prev_hash,
            "entry_hash": self.entry_hash,
            "timestamp": self.timestamp,
        }


class HumanInTheLoopGate:
    """
    P0/P1/P2: production-safe, pending registry, resolve(), hash-chained
    audit with optional persistence.
    """

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
        self._futures: Dict[str, concurrent.futures.Future] = {}
        self._lock = threading.RLock()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="hitl-approver")
        self._audit: deque = deque(maxlen=4096)
        self._last_hash = self._persist.hitl_last_hash() if self._persist else ""
        METRIC_HITL_PENDING.set(0)

    def close(self):
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _audit_append(self, entry: HITLAuditEntry):
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

    def resolve(self, request_id: str, approved: bool) -> bool:
        with self._lock:
            fut = self._futures.get(request_id)
            req = self._pending.pop(request_id, None)
            self._futures.pop(request_id, None)
        if fut is None or fut.done():
            return False
        fut.set_result(bool(approved))
        self._audit_append(HITLAuditEntry(
            request_id=request_id, approved=approved, reason="resolved",
            mode="resolved"))
        METRIC_HITL.labels(decision="approved" if approved else "rejected").inc()
        METRIC_HITL_PENDING.set(len(self._pending))
        return True

    async def request(self, req: HITLRequest) -> bool:
        start = time.time()

        if self.approver is None:
            if self.production_safe:
                self._audit_append(HITLAuditEntry(
                    request_id=req.request_id, approved=False,
                    reason=req.reason, mode="blocked_no_approver",
                    latency_ms=(time.time() - start) * 1000.0))
                METRIC_HITL.labels(decision="blocked").inc()
                return False
            self._audit_append(HITLAuditEntry(
                request_id=req.request_id, approved=True,
                reason=req.reason, mode="auto_no_approver",
                latency_ms=(time.time() - start) * 1000.0))
            METRIC_HITL.labels(decision="auto_approved").inc()
            return True

        with self._lock:
            if len(self._pending) >= self.max_pending:
                self._audit_append(HITLAuditEntry(
                    request_id=req.request_id, approved=False,
                    reason=req.reason, mode="queue_full",
                    latency_ms=(time.time() - start) * 1000.0))
                METRIC_HITL.labels(decision="queue_full").inc()
                return False
            fut = concurrent.futures.Future()
            self._pending[req.request_id] = req
            self._futures[req.request_id] = fut
            METRIC_HITL_PENDING.set(len(self._pending))

        loop = asyncio.get_running_loop()

        def _call_approver():
            try:
                return bool(self.approver(req))
            except Exception as e:
                logger.warning(f"Approver failed: {e}")
                return False

        worker_future = self._executor.submit(_call_approver)
        try:
            approved = await asyncio.wait_for(
                loop.run_in_executor(None, worker_future.result),
                timeout=self.timeout_s)
            mode = "approver"
        except asyncio.TimeoutError:
            approved = self.auto_approve_on_timeout
            mode = "timeout"
        except Exception as e:
            approved = False
            mode = f"error:{e}"
        finally:
            with self._lock:
                self._pending.pop(req.request_id, None)
                self._futures.pop(req.request_id, None)
                METRIC_HITL_PENDING.set(len(self._pending))

        if not fut.done():
            fut.set_result(bool(approved))

        latency_ms = (time.time() - start) * 1000.0
        self._audit_append(HITLAuditEntry(
            request_id=req.request_id, approved=approved, reason=req.reason,
            mode=mode, latency_ms=latency_ms))
        METRIC_HITL.labels(
            decision="approved" if approved else "rejected").inc()
        return bool(approved)

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
            # Recompute hash from canonical fields
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
    """P2: diversity + calibration + time-bounded."""

    def __init__(self, capacity: int = 256, seed: int = 0,
                 feature_dim: int = 4, diversity_weight: float = 0.4,
                 n_clusters: int = 4, max_age_s: float = 0.0):
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
        METRIC_ACTIVE_LEARNING.set(0)

    def _embed(self, record: Dict[str, Any]) -> np.ndarray:
        vals = []
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
        METRIC_ACTIVE_LEARNING.set(size)
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


# ===========================================================================
# Circuit breaker (P0: half-open resets failure count)
# ===========================================================================
class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == "open":
                if (time.time() - (self.last_failure_time or 0)) > self.recovery_timeout:
                    self.state = "half-open"
                    self.failure_count = 0  # P0 fix
                    METRIC_CB_STATE.labels(endpoint=self.name).set(0.5)
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is open")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == "half-open":
                    self.state = "closed"
                    self.failure_count = 0
                    METRIC_CB_STATE.labels(endpoint=self.name).set(0)
            return result
        except Exception:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.failure_count >= self.failure_threshold:
                    self.state = "open"
                    METRIC_CB_STATE.labels(endpoint=self.name).set(1)
            raise

    def get_status(self) -> Dict[str, Any]:
        return {"name": self.name, "state": self.state,
                "failure_count": self.failure_count,
                "recovery_timeout": self.recovery_timeout}


# ===========================================================================
# Bandit + MOO (P0: clamped weights)
# ===========================================================================
class LinUCB:
    def __init__(self, num_arms: int, feature_dim: int, alpha: float = 0.1):
        self.num_arms = num_arms
        self.feature_dim = feature_dim
        self.alpha = alpha
        self.A = [np.eye(feature_dim) for _ in range(num_arms)]
        self.b = [np.zeros(feature_dim) for _ in range(num_arms)]
        self.theta = [np.zeros(feature_dim) for _ in range(num_arms)]

    def select_arm(self, features: np.ndarray) -> int:
        p = np.zeros(self.num_arms)
        for a in range(self.num_arms):
            A_inv = np.linalg.inv(self.A[a])
            self.theta[a] = A_inv.dot(self.b[a])
            p[a] = (self.theta[a].dot(features)
                    + self.alpha * np.sqrt(features.dot(A_inv).dot(features)))
        return int(np.argmax(p))

    def update(self, arm: int, features: np.ndarray, reward: float) -> None:
        self.A[arm] += np.outer(features, features)
        self.b[arm] += reward * features


class MultiObjectiveOptimizer:
    def __init__(self, weights: Optional[List[float]] = None):
        self.weights = list(weights) if weights else [0.4, 0.3, 0.3]
        self.learning_rate = 0.01
        self.recent_outcomes: deque = deque(maxlen=100)

    def score_arms(self, arms: List[Dict[str, Any]],
                   context: Dict[str, Any]) -> List[float]:
        scores = []
        for arm in arms:
            quality = float(arm.get("quality_estimate", 0.5))
            latency = float(arm.get("latency_estimate", 1.0))
            cost = float(arm.get("cost_estimate", 100))
            norm_latency = 1.0 / (1.0 + latency)
            norm_cost = 1.0 / (1.0 + cost / 100)
            scores.append(self.weights[0] * quality
                          + self.weights[1] * norm_latency
                          + self.weights[2] * norm_cost)
        return scores

    def update_weights(self, outcomes: Sequence[float]) -> None:
        self.recent_outcomes.append(list(outcomes))
        if len(self.recent_outcomes) < 10:
            return
        avg = np.mean(self.recent_outcomes, axis=0)
        target = float(np.mean(avg))
        error = avg - target
        new_weights = [w - self.learning_rate * e
                       for w, e in zip(self.weights, error)]
        # P0 fix: clamp then normalize
        new_weights = [max(0.0, min(1.0, w)) for w in new_weights]
        total = sum(new_weights)
        if total <= 0:
            new_weights = [1.0 / len(new_weights)] * len(new_weights)
        else:
            new_weights = [w / total for w in new_weights]
        self.weights = new_weights


# ===========================================================================
# MOE router (P0: single update call, per-teacher breaker)
# ===========================================================================
class MOERouter:
    def __init__(self, feature_dim: int = 4, epsilon: float = 0.1):
        self.teachers: List[Tuple[str, "LLMClient"]] = []
        self.epsilon = epsilon
        self.gating_weights = None
        self.scaler = None
        self._trained = False
        self.rewards: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.context_history: deque = deque(maxlen=1000)
        self.distiller = None
        self.feature_dim = feature_dim

    def add_teacher(self, name: str, client: "LLMClient") -> None:
        self.teachers.append((name, client))
        self.rewards[name] = 0.0
        self.counts[name] = 0

    def _extract_features(self, prompt: str) -> np.ndarray:
        words = prompt.split()
        return np.array([
            len(prompt),
            len(words),
            float(np.mean([len(w) for w in words])) if words else 0.0,
            utcnow().hour / 24.0,
        ])

    def _teacher_by_name(self, name: Optional[str]) -> Optional["LLMClient"]:
        if name is None:
            return None
        for n, c in self.teachers:
            if n == name:
                return c
        return None

    async def select_teacher(self, prompt: str) -> Tuple[str, "LLMClient"]:
        if not self.teachers:
            raise RuntimeError("MOERouter has no teachers")
        if self.distiller is not None:
            features = self._extract_features(prompt)
            try:
                teacher_name = self.distiller.distill(features)
            except Exception:
                teacher_name = None
            client = self._teacher_by_name(teacher_name)
            if client is not None:
                return teacher_name, client
        if random.random() < self.epsilon:
            return random.choice(self.teachers)
        if self._trained and self.gating_weights is not None:
            try:
                features = self._extract_features(prompt).reshape(1, -1)
                X_scaled = self.scaler.transform(features)
                probs = self.gating_weights.predict_proba(X_scaled)[0]
                idx = int(np.random.choice(len(self.teachers), p=probs))
                name, client = self.teachers[idx]
                METRIC_MOE_GATE.labels(teacher=name).set(float(probs[idx]))
                return name, client
            except Exception:
                pass
        best_name = max(self.rewards,
                        key=lambda n: self.rewards[n] / max(self.counts[n], 1))
        for n, c in self.teachers:
            if n == best_name:
                return n, c
        return self.teachers[0]

    async def update(self, teacher_name: str, reward: float, prompt: str) -> None:
        self.rewards[teacher_name] += reward
        self.counts[teacher_name] += 1
        features = self._extract_features(prompt)
        self.context_history.append((features, teacher_name, reward))
        if len(self.context_history) % 50 == 0 and len(self.context_history) >= 50:
            self._retrain_gating()

    def _retrain_gating(self) -> None:
        if not SKLEARN_AVAILABLE or len(self.context_history) < 50:
            return
        names = [n for n, _ in self.teachers]
        X, y = [], []
        for features, name, _ in list(self.context_history)[-200:]:
            if name in names:
                X.append(features)
                y.append(names.index(name))
        if len(set(y)) < 2:
            return
        try:
            self.scaler = StandardScaler()
            X_scaled = self.scaler.fit_transform(np.array(X))
            self.gating_weights = LogisticRegression(max_iter=1000)
            self.gating_weights.fit(X_scaled, np.array(y))
            self._trained = True
        except Exception as e:
            logger.warning(f"Could not train gating model: {e}")

    def get_stats(self) -> Dict[str, Any]:
        return {"teachers": [name for name, _ in self.teachers],
                "rewards": dict(self.rewards),
                "counts": dict(self.counts),
                "gating_trained": self._trained,
                "distillation_active": self.distiller is not None}


# ===========================================================================
# GA (P0: no reinitialize on evolve)
# ===========================================================================
class GeneticAlgorithmOptimizer:
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1,
                 crossover_rate: float = 0.8, seed: int = 0):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Dict[str, Any]] = []
        self.bounds = {"temp": (0.1, 1.0), "max_tokens": (50, 500),
                       "model": ["small", "medium", "large"]}
        self.arm_history: deque = deque(maxlen=100)
        self._rng = random.Random(seed)
        self._initialized = False

    def initialize(self, force: bool = False) -> None:
        if self._initialized and not force:
            return
        self.population = [{
            "temp": self._rng.uniform(0.1, 1.0),
            "max_tokens": self._rng.randint(50, 500),
            "model": self._rng.choice(self.bounds["model"]),
        } for _ in range(self.pop_size)]
        self._initialized = True

    def evaluate(self, fitness_func: Callable[[Dict[str, Any]], float]) -> List[float]:
        return [fitness_func(ind) for ind in self.population]

    def select(self, fitness: List[float], num_parents: int) -> List[Dict[str, Any]]:
        selected = []
        for _ in range(num_parents):
            a, b = self._rng.sample(range(len(self.population)), 2)
            selected.append(self.population[a] if fitness[a] > fitness[b]
                            else self.population[b])
        return selected

    def crossover(self, p1: Dict[str, Any], p2: Dict[str, Any]) -> Dict[str, Any]:
        if self._rng.random() < self.crossover_rate:
            child = {}
            for key in p1:
                if key == "model":
                    child[key] = self._rng.choice([p1[key], p2[key]])
                else:
                    child[key] = p1[key] if self._rng.random() < 0.5 else p2[key]
            return child
        return dict(p1)

    def mutate(self, ind: Dict[str, Any]) -> Dict[str, Any]:
        if self._rng.random() < self.mutation_rate:
            key = self._rng.choice(list(self.bounds.keys()))
            if key == "model":
                ind[key] = self._rng.choice(self.bounds["model"])
            else:
                low, high = self.bounds[key]
                if key == "temp":
                    ind[key] = self._rng.uniform(low, high)
                else:
                    ind[key] = self._rng.randint(int(low), int(high))
        return ind

    def evolve(self, fitness_func: Callable[[Dict[str, Any]], float],
               generations: int = 20) -> Dict[str, Any]:
        self.initialize()
        best = None
        for _ in range(generations):
            fitness = self.evaluate(fitness_func)
            best_idx = int(np.argmax(fitness))
            best = dict(self.population[best_idx])
            parents = self.select(fitness, self.pop_size - 1)
            offspring = []
            for i in range(0, len(parents) - 1, 2):
                c1 = self.crossover(parents[i], parents[i + 1])
                c2 = self.crossover(parents[i + 1], parents[i])
                offspring.append(self.mutate(c1))
                offspring.append(self.mutate(c2))
            self.population = offspring[: self.pop_size - 1] + [best]
        result = best or {}
        self.arm_history.append(result)
        return result


# ===========================================================================
# Carbon-aware scheduler
# ===========================================================================
class CarbonAwareScheduler:
    def __init__(self, carbon_manager: CarbonIntensityManager,
                 threshold: float = 400.0):
        self.carbon_manager = carbon_manager
        self.threshold = threshold

    async def adjust_params(self, params: Dict[str, Any],
                            intensity: Optional[float] = None,
                            price: Optional[float] = None) -> Dict[str, Any]:
        if intensity is None:
            intensity = await self.carbon_manager.get_current_intensity()
        if price is None:
            price = await self.carbon_manager.get_current_price()
        if intensity > self.threshold or price > 0.1:
            params["max_tokens"] = min(params.get("max_tokens", 150), 100)
            if params.get("model") != "small":
                params["model"] = "small"
        return params


# ===========================================================================
# Self-healing
# ===========================================================================
class SelfHealingManager:
    def __init__(self, contamination: float = 0.1):
        self.contamination = contamination
        self.quality_history: deque = deque(maxlen=500)
        self.anomaly_detectors: List[Tuple[str, Any]] = []
        self._trained = False
        self._lock = threading.Lock()
        self.recovery_actions: deque = deque(maxlen=100)
        if SKLEARN_AVAILABLE:
            self.anomaly_detectors = [
                ("iforest", IsolationForest(contamination=contamination)),
                ("ocsvm", OneClassSVM(nu=contamination)),
            ]

    async def record_quality(self, quality: float) -> None:
        with self._lock:
            self.quality_history.append(quality)
        if len(self.quality_history) >= 100 and not self._trained:
            await self._train()

    async def _train(self) -> None:
        if not self.anomaly_detectors or len(self.quality_history) < 100:
            return
        X = np.array(list(self.quality_history)).reshape(-1, 1)
        for _, model in self.anomaly_detectors:
            try:
                model.fit(X)
            except Exception as e:
                logger.warning(f"Failed to train detector: {e}")
        self._trained = True

    async def detect_anomaly(self, quality: float) -> Tuple[bool, float]:
        if not self._trained or not self.anomaly_detectors:
            return quality < 0.3, 0.0
        X = np.array([[quality]])
        votes = []
        for _, model in self.anomaly_detectors:
            try:
                pred = model.predict(X)[0]
                votes.append(1 if pred == -1 else 0)
            except Exception:
                votes.append(0)
        if not votes:
            return False, 0.0
        score = sum(votes) / len(votes)
        return score > 0.5, score

    async def trigger_recovery(self) -> None:
        with self._lock:
            self.recovery_actions.append({
                "action": "reset_bandit",
                "timestamp": utcnow().isoformat()})
        logger.warning("Self-healing triggered: resetting bandit and gating.")

    def get_stats(self) -> Dict[str, Any]:
        return {"trained": self._trained,
                "history_len": len(self.quality_history),
                "recent_actions": list(self.recovery_actions)[-5:]}


# ===========================================================================
# Semantic cache (P1: SHA-256, executor, TTL)
# ===========================================================================
@dataclass
class _CacheEntry:
    value: str
    timestamp: float
    embedding: Optional[np.ndarray] = None
    prompt: str = ""


class SemanticCache:
    def __init__(self, similarity_threshold: float = 0.95,
                 max_size: int = 1000, ttl_s: float = 3600.0,
                 persist: Optional[Persistence] = None):
        self.similarity_threshold = similarity_threshold
        self.max_size = max_size
        self.ttl_s = ttl_s
        self._persist = persist
        self._lock = asyncio.Lock()
        self.embedding_model = None
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="cache-embed")
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            try:
                self.embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
                logger.info("SentenceTransformer loaded for semantic caching")
            except Exception as e:
                logger.warning(f"Could not load SentenceTransformer: {e}")

    def _key(self, prompt: str) -> str:
        return hashlib.sha256(prompt.encode()).hexdigest()

    async def _encode(self, text: str) -> Optional[np.ndarray]:
        if self.embedding_model is None:
            return None
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor, self.embedding_model.encode, text)

    async def get(self, prompt: str) -> Optional[str]:
        async with self._lock:
            key = self._key(prompt)
            # Persist path
            if self._persist is not None:
                row = self._persist.cache_get(key)
                if row:
                    value, ts = row
                    if self.ttl_s > 0 and (time.time() - ts) > self.ttl_s:
                        self._persist.cache_delete_older_than(time.time() - self.ttl_s)
                    else:
                        METRIC_CACHE_HITS.inc()
                        return value
                if self.embedding_model is not None:
                    rows = self._persist.cache_all()
                    if rows:
                        emb = await self._encode(prompt)
                        if emb is not None:
                            for k, _p, stored_emb, ts in rows:
                                if stored_emb is None:
                                    continue
                                if self.ttl_s > 0 and (time.time() - ts) > self.ttl_s:
                                    continue
                                sim = float(
                                    np.dot(emb, stored_emb)
                                    / (np.linalg.norm(emb) * np.linalg.norm(stored_emb) + 1e-8))
                                if sim >= self.similarity_threshold:
                                    value = self._persist.cache_get(k)
                                    if value:
                                        METRIC_CACHE_HITS.inc()
                                        return value[0]
                METRIC_CACHE_MISSES.inc()
                return None

            # In-memory path
            if not hasattr(self, "_mem"):
                self._mem: Dict[str, _CacheEntry] = {}
            if key in self._mem:
                entry = self._mem[key]
                if self.ttl_s > 0 and (time.time() - entry.timestamp) > self.ttl_s:
                    del self._mem[key]
                else:
                    METRIC_CACHE_HITS.inc()
                    return entry.value
            if self.embedding_model is not None and self._mem:
                emb = await self._encode(prompt)
                if emb is not None:
                    for entry in self._mem.values():
                        if entry.embedding is None:
                            continue
                        if self.ttl_s > 0 and (time.time() - entry.timestamp) > self.ttl_s:
                            continue
                        sim = float(
                            np.dot(emb, entry.embedding)
                            / (np.linalg.norm(emb) * np.linalg.norm(entry.embedding) + 1e-8))
                        if sim >= self.similarity_threshold:
                            METRIC_CACHE_HITS.inc()
                            return entry.value
            METRIC_CACHE_MISSES.inc()
            return None

    async def set(self, prompt: str, response: str) -> None:
        key = self._key(prompt)
        emb = await self._encode(prompt)
        ts = time.time()
        if self._persist is not None:
            self._persist.cache_set(key, prompt, response, emb, ts)
            while self._persist.cache_size() > self.max_size:
                self._persist.cache_evict_oldest()
            return
        async with self._lock:
            if not hasattr(self, "_mem"):
                self._mem: Dict[str, _CacheEntry] = {}
            if len(self._mem) >= self.max_size:
                oldest = min(self._mem.keys(), key=lambda k: self._mem[k].timestamp)
                del self._mem[oldest]
            self._mem[key] = _CacheEntry(value=response, timestamp=ts,
                                         embedding=emb, prompt=prompt)

    def size(self) -> int:
        if self._persist is not None:
            return self._persist.cache_size()
        return len(getattr(self, "_mem", {}))


# ===========================================================================
# Fallback
# ===========================================================================
class TemplatedFallback:
    def __init__(self, seed: int = 0):
        self._rng = random.Random(seed)
        self.templates = [
            "Based on available data, the recommended action is to proceed with caution.",
            "Due to current scarcity constraints, helium usage should be minimized.",
            "The system suggests optimizing workflows to reduce helium consumption.",
            "No specific recommendation can be generated at this time.",
        ]

    def generate(self, prompt: str) -> str:
        lower = prompt.lower()
        if "scarcity" in lower or "shortage" in lower:
            return ("Helium scarcity is currently high. Please reduce usage and "
                    "consider alternatives.")
        if "price" in lower:
            return "Helium prices are volatile. We recommend monitoring market trends."
        if "optimize" in lower or "efficiency" in lower:
            return ("Optimizing helium usage can lead to significant cost savings "
                    "and sustainability improvements.")
        return self._rng.choice(self.templates)


# ===========================================================================
# Local fallbacks for external enhancement modules
# ===========================================================================
class LimitGraph:
    def __init__(self, *a, **kw):
        self.limits: Dict[str, Any] = {}
        self._feedback: List[Any] = []

    def build_graph(self, nodes, edges): pass

    def get_limits(self, context) -> Dict[str, Any]:
        return {"max_tokens": 500, "temperature": 1.0}

    def update_from_feedback(self, feedback): self._feedback.append(feedback)


class RLHFOptimizer:
    def __init__(self, action_space, persist: Optional[Persistence] = None):
        self.actions = list(action_space)
        self.scores = {a: 0.0 for a in self.actions}
        self._persist = persist
        if persist:
            stored = persist.rlhf_all()
            for k, v in stored.items():
                try:
                    a = int(k)
                    if a in self.scores:
                        self.scores[a] = v
                except Exception:
                    pass

    def update(self, context, action, reward):
        if action not in self.scores:
            self.actions.append(action)
            self.scores[action] = 0.0
        self.scores[action] += 0.1 * (float(reward) - self.scores[action])
        if self._persist:
            self._persist.rlhf_set(str(action), self.scores[action])

    def sample_action(self, context):
        if not self.scores:
            return None
        if random.random() < 0.1:
            return random.choice(self.actions)
        return max(self.scores, key=self.scores.get)


class MultiTeacherDistiller:
    def __init__(self, teachers, weights=None):
        self.teachers = list(teachers)
        self.weights = list(weights) if weights else [1.0] * len(self.teachers)

    async def _call(self, t, ctx):
        try:
            out = t(ctx)
            if asyncio.iscoroutine(out):
                out = await out
            return out
        except Exception:
            return None

    async def distill(self, context):
        votes: Dict[Any, float] = {}
        for t, w in zip(self.teachers, self.weights):
            c = await self._call(t, context)
            if c is not None:
                votes[c] = votes.get(c, 0.0) + float(w)
        return max(votes, key=votes.get) if votes else None


# ===========================================================================
# Pipeline context
# ===========================================================================
@dataclass
class PipelineContext:
    request_id: str
    prompt: str
    redacted_prompt: str
    features: np.ndarray
    params: Dict[str, Any]
    arm_idx: int = -1
    source: str = "default"
    teacher_name: str = "primary"
    teacher_client: Any = None
    carbon_intensity: float = 400.0
    carbon_price: float = 0.05
    precision_level: Optional[PrecisionLevel] = None
    safety_ok: bool = True
    stl_verdict: Dict[str, bool] = field(default_factory=dict)
    uncertainty: float = 0.0
    human_approved: bool = True
    response: str = ""
    success: bool = False
    reward: float = 0.0
    latency_ms: float = 0.0
    tokens: int = 0
    cost_usd: float = 0.0
    shield_applied: bool = False
    cached: bool = False


# ===========================================================================
# Enhanced LLM Client
# ===========================================================================
class LLMClient:
    def __init__(
        self,
        config: Optional[LLMClientConfig] = None,
        # Backward-compatible overrides
        endpoint: Optional[str] = None,
        model: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        retry_attempts: Optional[int] = None,
        circuit_breaker_threshold: Optional[int] = None,
        circuit_breaker_timeout: Optional[float] = None,
        fallback_generator: Optional[Callable[[str], str]] = None,
        hitl_approver: Optional[Callable[[HITLRequest], bool]] = None,
        extra_endpoints: Optional[List[Tuple[str, str]]] = None,
        **kwargs: Any,
    ):
        cfg = config or LLMClientConfig()
        # Apply overrides
        if endpoint is not None: cfg.endpoint = endpoint
        if model is not None: cfg.model = model
        if headers is not None: cfg.headers = headers
        if timeout is not None: cfg.timeout = timeout
        if retry_attempts is not None: cfg.retry_attempts = retry_attempts
        if circuit_breaker_threshold is not None:
            cfg.circuit_breaker_threshold = circuit_breaker_threshold
        if circuit_breaker_timeout is not None:
            cfg.circuit_breaker_timeout = circuit_breaker_timeout
        if extra_endpoints is not None: cfg.extra_endpoints = extra_endpoints
        # Legacy kwargs
        for k, v in kwargs.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)

        self.config = cfg
        self.endpoint = cfg.endpoint
        self.model = cfg.model
        self.headers = dict(cfg.headers)
        self.timeout = cfg.timeout
        self.retry_attempts = cfg.retry_attempts
        self.fallback_generator = fallback_generator or TemplatedFallback(seed=0).generate
        self.node_id = cfg.node_id or str(uuid.uuid4())[:8]
        self.ga_evolve_every = max(1, cfg.ga_evolve_every)

        # Persistence
        self.persistence = Persistence(cfg.persist_path)

        # Session
        self._session: Optional[aiohttp.ClientSession] = None

        # Circuit breakers per endpoint
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._get_cb(self.endpoint)

        # Rate limiter
        self._rate_limiter = TokenBucket(cfg.rate_limit_per_minute, 60.0)

        # Cost tracking
        self._today_cost = self.persistence.cost_today()

        # Decision layers
        self.arms: List[Dict[str, Any]] = [
            {"temp": 0.7, "max_tokens": 150, "model": "small"},
            {"temp": 0.5, "max_tokens": 100, "model": "small"},
            {"temp": 0.9, "max_tokens": 200, "model": "medium"},
            {"temp": 0.3, "max_tokens": 50,  "model": "small"},
            {"temp": 0.8, "max_tokens": 300, "model": "large"},
        ]
        if cfg.enable_contextual_bandit:
            self.linucb = LinUCB(num_arms=len(self.arms), feature_dim=4, alpha=0.1)
            self.moo = MultiObjectiveOptimizer()
        else:
            self.linucb = None
            self.moo = None

        # MOE
        self.router: Optional[MOERouter] = None
        if cfg.enable_moe:
            self.router = MOERouter()
            self.router.add_teacher("primary", self)
            for name, url in cfg.extra_endpoints:
                teacher = LLMClient(
                    config=LLMClientConfig(
                        endpoint=url, model=cfg.model, headers=cfg.headers,
                        timeout=cfg.timeout,
                        retry_attempts=cfg.retry_attempts,
                        circuit_breaker_threshold=cfg.circuit_breaker_threshold,
                        circuit_breaker_timeout=cfg.circuit_breaker_timeout,
                        cache_enabled=False, persist_path=None,
                        enable_contextual_bandit=False, enable_moe=False,
                        enable_ga=False, enable_carbon_aware=False,
                        enable_self_healing=False, enable_lineage=False,
                        enable_limit_graph=False, enable_rlhf=False,
                        enable_distillation=False, enable_quantum_teacher=False,
                        enable_causal=False, enable_federated=False,
                        enable_multi_agent=False, enable_temporal_logic=False,
                        enable_xai=False, enable_precision_switching=False,
                        enable_carbon_market=False, enable_chaos=False,
                        enable_hitl=False))
                self.router.add_teacher(name, teacher)

        # GA
        self.ga: Optional[GeneticAlgorithmOptimizer] = None
        if cfg.enable_ga:
            self.ga = GeneticAlgorithmOptimizer()
            self.ga.initialize()
            best_stored = self.persistence.ga_best()
            if best_stored:
                self.arms[0] = best_stored
        self._calls_since_ga = 0

        # Carbon
        self.carbon_market = CarbonMarketClient() if cfg.enable_carbon_market else None
        self.recs = RECInventory(
            persist_path=(Path(cfg.persist_path).with_suffix(".rec.pkl")
                          if cfg.persist_path else None)
        ) if cfg.enable_carbon_market else None
        self.carbon_manager = CarbonIntensityManager(
            market=self.carbon_market, recs=self.recs,
            electricity_maps_api_key=cfg.electricity_maps_api_key,
            electricity_maps_zone=cfg.electricity_maps_zone,
            watttime_username=cfg.watttime_username,
            watttime_password=cfg.watttime_password,
            watttime_region=cfg.watttime_region,
            region=cfg.carbon_region)
        self.carbon_scheduler = CarbonAwareScheduler(self.carbon_manager)

        # Self-healing
        self.self_healing = SelfHealingManager() if cfg.enable_self_healing else None

        # Cache
        self.cache = SemanticCache(
            similarity_threshold=cfg.cache_similarity_threshold,
            max_size=cfg.cache_max_size,
            ttl_s=cfg.cache_ttl_s,
            persist=(self.persistence if cfg.persist_path else None)
        ) if cfg.cache_enabled else None

        # Vault
        self.vault_client = None
        if VAULT_AVAILABLE and cfg.vault_url and cfg.vault_token:
            try:
                self.vault_client = VaultClient(url=cfg.vault_url,
                                                token=cfg.vault_token)
                logger.info("Vault client initialized")
            except Exception as e:
                logger.warning(f"Vault init failed: {e}")

        # === Ten enhancements ===
        self.quantum_teacher = (QuantumInspiredTeacher(seed=0)
                                if cfg.enable_quantum_teacher else None)
        self.distill_ensemble = (DistillationEnsemble(self.quantum_teacher, alpha=0.5)
                                 if self.quantum_teacher else None)

        # Distiller for arms (async-safe)
        self.arm_distiller: Optional[MultiTeacherDistiller] = None
        if cfg.enable_distillation and cfg.enable_contextual_bandit:
            self.arm_distiller = MultiTeacherDistiller(
                teachers=[self._teacher_linucb, self._teacher_rule,
                          self._teacher_static],
                weights=[1.0, 1.0, 0.5])

        # Causal RL
        self.causal = (CausalCounterfactualEstimator(
            state_dim=4, n_actions=len(self.arms)) if cfg.enable_causal else None)
        self.last_counterfactuals: Dict[int, float] = {}

        # Federated
        self.federated = (SecureFederatedAggregator(
            dp_sigma=cfg.federated_dp_sigma,
            target_epsilon=cfg.federated_target_epsilon,
            target_delta=cfg.federated_target_delta)
            if cfg.enable_federated else None)

        # Multi-agent
        self.roles = EmergentRoleRegistry() if cfg.enable_multi_agent else None
        self.coordinator = MultiAgentCoordinator(self.roles) if self.roles else None

        # Temporal logic
        self.temporal: Optional[TemporalLogicMonitor] = None
        self.shield: Optional[SafetyShield] = None
        if cfg.enable_temporal_logic:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="latency_reasonable",
                predicate=lambda r: r.get("latency_ms", 0.0) <= 30000.0,
                operator=STLOperator.ALWAYS, horizon=10))
            self.temporal.add_formula(STLFormula(
                name="eventually_success",
                predicate=lambda r: r.get("success", False),
                operator=STLOperator.EVENTUALLY, horizon=5))
            self.temporal.add_formula(STLFormula(
                name="cost_bounded",
                predicate=lambda r: r.get("cost_usd", 0.0) <= 0.05,
                operator=STLOperator.ALWAYS, horizon=10))
            self.shield = SafetyShield(self.temporal)

        # XAI
        self.explainer = DecisionExplainer() if cfg.enable_xai else None
        self.last_explanation: Optional[Explanation] = None

        # Precision
        self.precision_controller = (PrecisionController(quality_floor=0.95)
                                     if cfg.enable_precision_switching else None)
        self.precision_adapter = (HardwareAwareAdapter(self.precision_controller)
                                  if self.precision_controller else None)
        self.current_precision: Optional[PrecisionLevel] = None

        # Chaos
        self.chaos: Optional[ChaosEngineer] = None
        if cfg.enable_chaos:
            hypotheses = [
                SteadyStateHypothesis(
                    name="p95_latency_under_30s",
                    predicate=lambda m: m.get("latency_ms", 0.0) <= 30000.0),
                SteadyStateHypothesis(
                    name="success_rate_above_50",
                    predicate=lambda m: m.get("success_rate", 1.0) >= 0.5),
            ]
            self.chaos = ChaosEngineer(
                ChaosConfig(
                    fault_prob=cfg.chaos_fault_prob,
                    latency_inject_ms=cfg.chaos_latency_ms,
                    carbon_spike_prob=cfg.chaos_carbon_spike_prob,
                    max_concurrent_faults=cfg.chaos_max_concurrent,
                    duration_s=cfg.chaos_duration_s),
                hypotheses)

        # HITL
        self.uncertainty = UncertaintyEstimator() if cfg.enable_hitl else None
        self.hitl = (HumanInTheLoopGate(
            approver=hitl_approver,
            timeout_s=cfg.hitl_timeout_s,
            auto_approve_on_timeout=cfg.hitl_auto_approve_on_timeout,
            production_safe=cfg.hitl_production_safe,
            persist=self.persistence)
            if cfg.enable_hitl else None)
        self.active_learner = (ActiveLearningSampler()
                               if cfg.enable_hitl else None)

        # RLHF
        self.rlhf = (RLHFOptimizer(
            action_space=list(range(len(self.arms))),
            persist=self.persistence)
            if (cfg.enable_rlhf and cfg.enable_contextual_bandit) else None)

        # LIMIT graph
        self.limit_graph = LimitGraph() if cfg.enable_limit_graph else None

        # Lineage
        self.lineage_records: deque = deque(maxlen=1000)

        # In-flight tracking (for graceful shutdown)
        self._in_flight = 0
        self._inflight_lock = asyncio.Lock()

        atexit.register(self._atexit_cleanup)

    def _atexit_cleanup(self):
        with contextlib.suppress(Exception):
            self.persistence.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_cb(self, endpoint: str) -> CircuitBreaker:
        if endpoint not in self._circuit_breakers:
            self._circuit_breakers[endpoint] = CircuitBreaker(
                name=endpoint,
                failure_threshold=self.config.circuit_breaker_threshold,
                recovery_timeout=self.config.circuit_breaker_timeout)
        return self._circuit_breakers[endpoint]

    def _teacher_linucb(self, features: np.ndarray) -> int:
        if self.linucb is not None:
            return int(self.linucb.select_arm(features))
        return 0

    def _teacher_rule(self, features: np.ndarray) -> int:
        length = float(features[0]) if len(features) > 0 else 50.0
        if length > 200:
            return 4
        if length > 100:
            return 2
        return 0

    def _teacher_static(self, features: np.ndarray) -> int:
        return 2

    async def _refresh_headers_from_vault(self) -> None:
        if not self.vault_client:
            return
        loop = asyncio.get_running_loop()
        try:
            secret = await loop.run_in_executor(
                None,
                lambda: self.vault_client.secrets.kv.v2.read_secret_version(
                    path=self.config.vault_secret_path, mount_point="secret"))
            data = secret.get("data", {}).get("data", {}) or {}
            api_key = data.get("api_key")
            if api_key:
                self.headers = {**self.headers, "Authorization": f"Bearer {api_key}"}
        except Exception as e:
            logger.warning(f"Vault refresh failed: {e}")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        # Drain in-flight up to timeout
        deadline = time.time() + 10.0
        while self._in_flight > 0 and time.time() < deadline:
            await asyncio.sleep(0.1)
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        if self.hitl:
            self.hitl.close()
        if self.cache:
            with contextlib.suppress(Exception):
                self.cache._executor.shutdown(wait=False)
        self.persistence.close()
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()

    async def __aenter__(self) -> "LLMClient":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------
    async def _do_request(self, payload: Dict[str, Any],
                          endpoint: Optional[str] = None) -> Dict[str, Any]:
        endpoint = endpoint or self.endpoint
        session = await self._get_session()
        if self.vault_client:
            await self._refresh_headers_from_vault()
        async with session.post(
            endpoint,
            json=payload,
            headers=self.headers,
            timeout=aiohttp.ClientTimeout(total=self.timeout),
        ) as resp:
            if resp.status != 200:
                raise aiohttp.ClientResponseError(
                    request_info=resp.request_info,
                    history=resp.history,
                    status=resp.status,
                    message=f"LLM API returned {resp.status}")
            body = await resp.json()
            return body or {}

    def _extract_features(self, prompt: str) -> np.ndarray:
        words = prompt.split()
        return np.array([
            len(prompt),
            len(words),
            float(np.mean([len(w) for w in words])) if words else 0.0,
            utcnow().hour / 24.0,
        ])

    def _compute_reward(self, response: Optional[str]) -> float:
        if not response:
            return 0.0
        length_score = min(1.0, len(response) / 200.0)
        tokens = response.lower().split()
        diversity = len(set(tokens)) / len(tokens) if tokens else 0.0
        bonus = 0.1 if any(k in response.lower()
                           for k in ("recommend", "optimize", "suggest")) else 0.0
        penalty = -0.1 if "no specific recommendation" in response.lower() else 0.0
        return float(max(0.0, min(1.0,
            0.5 * length_score + 0.4 * diversity + bonus + penalty)))

    def _estimate_tokens(self, prompt: str, response: str) -> int:
        return max(1, (len(prompt) + len(response)) // 4)

    def _ga_fitness(self, arm: Dict[str, Any]) -> float:
        temp = float(arm.get("temp", 0.5))
        tokens = int(arm.get("max_tokens", 100))
        model = arm.get("model", "small")
        quality = 0.6 * (tokens / 500.0) + 0.4 * (
            1.0 if model == "large" else 0.5 if model == "medium" else 0.2)
        cost = tokens / 500.0 + (
            0.3 if model == "large" else 0.2 if model == "medium" else 0.1)
        return quality - 0.5 * cost

    def _record_lineage(self, ctx: PipelineContext) -> None:
        if not self.config.lineage_enabled:
            return
        self.lineage_records.append({
            "request_id": ctx.request_id,
            "timestamp": utcnow().isoformat(),
            "prompt": ctx.redacted_prompt,
            "response": ctx.response,
            "params": ctx.params,
            "arm_idx": ctx.arm_idx,
            "teacher": ctx.teacher_name,
        })
        self.persistence.lineage_add(
            ctx.request_id, ctx.redacted_prompt, ctx.response,
            ctx.params, time.time())

    # ------------------------------------------------------------------
    # Retry wrapper
    # ------------------------------------------------------------------
    async def _do_request_with_retry(
        self, payload: Dict[str, Any], endpoint: str
    ) -> Dict[str, Any]:
        cb = self._get_cb(endpoint)
        attempts = max(1, self.retry_attempts)
        last_error: Optional[Exception] = None
        for attempt in range(attempts):
            try:
                return await cb.call(self._do_request, payload, endpoint)
            except RuntimeError as e:
                # Circuit breaker open — do not retry
                raise
            except Exception as e:
                last_error = e
                if attempt + 1 >= attempts:
                    break
                METRIC_RETRY.inc()
                backoff = self.config.retry_backoff_base * (2 ** attempt)
                await asyncio.sleep(backoff + random.uniform(0, 0.1))
        raise last_error or RuntimeError("request failed")

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------
    async def generate_explanation(
        self,
        prompt: str,
        max_tokens: int = 150,
        temperature: float = 0.7,
        **kwargs: Any,
    ) -> str:
        async with self._inflight_lock:
            self._in_flight += 1
        try:
            return await self._generate_inner(
                prompt, max_tokens, temperature, **kwargs)
        finally:
            async with self._inflight_lock:
                self._in_flight -= 1

    async def _generate_inner(
        self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
    ) -> str:
        start = time.time()
        request_id = uuid.uuid4().hex[:12]
        redacted_prompt = (redact_pii(prompt)
                           if self.config.pii_redaction_enabled else prompt)

        # Rate limit
        if not self._rate_limiter.acquire():
            METRIC_RATE_LIMITED.inc()
            METRIC_REQUESTS.labels(status="rate_limited").inc()
            return self.fallback_generator(redacted_prompt)

        # Cost budget
        if (self.config.daily_cost_budget_usd > 0
                and self._today_cost >= self.config.daily_cost_budget_usd):
            METRIC_REQUESTS.labels(status="budget_exceeded").inc()
            logger.warning("Daily cost budget exhausted")
            return self.fallback_generator(redacted_prompt)

        # Chaos fault
        fault_injected = False
        if self.chaos and self.chaos.maybe_fault():
            fault_injected = True
            METRIC_FALLBACK.inc()
            METRIC_REQUESTS.labels(status="chaos_fault").inc()

        try:
            if fault_injected:
                raise RuntimeError("Chaos fault injected")

            # Cache
            if self.cache is not None:
                cached = await self.cache.get(redacted_prompt)
                if cached is not None:
                    METRIC_REQUESTS.labels(status="cache_hit").inc()
                    return cached

            # Features
            features = self._extract_features(redacted_prompt)
            ctx = PipelineContext(
                request_id=request_id, prompt=prompt,
                redacted_prompt=redacted_prompt, features=features,
                params={"max_tokens": max_tokens, "temperature": temperature,
                        "model": self.model})

            # Arm selection
            if self.linucb is not None:
                if self.arm_distiller is not None:
                    try:
                        arm_idx = int(await self.arm_distiller.distill(features))
                    except Exception:
                        arm_idx = self.linucb.select_arm(features)
                    ctx.source = "distilled"
                elif self.rlhf is not None and random.random() < 0.1:
                    sampled = self.rlhf.sample_action(features)
                    arm_idx = int(sampled) if sampled is not None else 0
                    ctx.source = "rlhf"
                else:
                    arm_idx = int(self.linucb.select_arm(features))
                    ctx.source = "linucb"
                if not (0 <= arm_idx < len(self.arms)):
                    arm_idx = 0
                ctx.arm_idx = arm_idx
                ctx.params.update(self.arms[arm_idx])

            # LIMIT graph
            if self.limit_graph is not None:
                try:
                    limits = self.limit_graph.get_limits(features) or {}
                except Exception:
                    limits = {}
                if "max_tokens" in limits:
                    ctx.params["max_tokens"] = min(
                        int(ctx.params.get("max_tokens", 150)),
                        int(limits["max_tokens"]))
                if "temperature" in limits:
                    ctx.params["temperature"] = min(
                        float(ctx.params.get("temperature", 0.7)),
                        float(limits["temperature"]))
                if limits.get("model") in ("small", "medium", "large"):
                    ctx.params["model"] = limits["model"]
                ctx.params["max_tokens"] = max(1, int(ctx.params.get("max_tokens", 1)))

            # Carbon
            await self.carbon_manager.refresh()
            carbon_snap = await self.carbon_manager.get_current_snapshot()
            carbon_intensity = carbon_snap.intensity_g_co2_per_kwh
            if self.chaos:
                carbon_intensity = self.chaos.maybe_carbon_spike(carbon_intensity)
            carbon_price = await self.carbon_manager.get_current_price()
            ctx.carbon_intensity = carbon_intensity
            ctx.carbon_price = carbon_price
            if self.config.enable_carbon_aware:
                ctx.params = await self.carbon_scheduler.adjust_params(
                    ctx.params, intensity=carbon_intensity, price=carbon_price)

            # Precision
            if self.precision_adapter is not None:
                ctx.params, level = self.precision_adapter.adapt(
                    ctx.params, carbon_intensity, 0.5, carbon_price)
                self.current_precision = level
                ctx.precision_level = level

            # MOE teacher selection
            teacher_name, client = "primary", self
            if self.router is not None:
                try:
                    teacher_name, client = await self.router.select_teacher(redacted_prompt)
                except Exception as e:
                    logger.warning(f"MOE selection failed: {e}")
                    teacher_name, client = "primary", self
            ctx.teacher_name = teacher_name
            ctx.teacher_client = client

            # Multi-agent vote
            if self.coordinator is not None and self.roles is not None \
                    and ctx.arm_idx >= 0:
                bids: List[AgentBid] = []
                for role in AgentRole:
                    if role == AgentRole.EXPLORER:
                        idx = random.randrange(len(self.arms))
                    elif role == AgentRole.EXPLOITER:
                        idx = int(np.argmax([self._ga_fitness(a) for a in self.arms]))
                    elif role == AgentRole.CARBON_BROKER:
                        idx = int(np.argmin([a.get("max_tokens", 150)
                                             for a in self.arms]))
                    elif role == AgentRole.SAFETY_OFFICER:
                        idx = ctx.arm_idx
                    else:
                        idx = ctx.arm_idx
                    bids.append(AgentBid(
                        agent_id=role.value, role=role, confidence=0.7,
                        proposed_action=idx, rationale=role.value,
                        carbon_score=1.0 / (1.0 + carbon_intensity / 1000.0)))
                voted, resolution = self.coordinator.resolve_conflict(
                    bids, len(self.arms))
                if voted != ctx.arm_idx:
                    ctx.arm_idx = voted
                    ctx.params.update(self.arms[ctx.arm_idx])
                # Record role outcomes
                for b in bids:
                    agreed = (b.proposed_action % len(self.arms)) == voted
                    self.roles.record(b.role, success=agreed,
                                      reward=1.0 if agreed else -0.2)

            # Chaos latency
            if self.chaos:
                delay_ms = self.chaos.maybe_latency()
                if delay_ms > 0:
                    await asyncio.sleep(delay_ms / 1000.0)

            # Execute
            try:
                if client is self:
                    result = await self._do_request_with_retry(
                        {"prompt": redacted_prompt, **ctx.params}, self.endpoint)
                    text = (result.get("text")
                            or result.get("generated_text")
                            or result.get("response")
                            or "")
                else:
                    text = await client.generate_explanation(
                        redacted_prompt, **ctx.params)
                ctx.response = text
                ctx.success = bool(text)
            except Exception as e:
                logger.warning(f"LLM generation failed: {e}")
                METRIC_FALLBACK.inc()
                ctx.response = self.fallback_generator(redacted_prompt)
                ctx.success = False

            ctx.latency_ms = (time.time() - start) * 1000.0
            ctx.reward = self._compute_reward(ctx.response) if ctx.success else 0.0
            ctx.tokens = self._estimate_tokens(redacted_prompt, ctx.response)
            ctx.cost_usd = (ctx.tokens / 1000.0) * self.config.cost_per_1k_tokens
            self._today_cost += ctx.cost_usd
            METRIC_TOKENS.inc(ctx.tokens)
            METRIC_COST.inc(ctx.cost_usd)
            self.persistence.cost_record(ctx.tokens, ctx.cost_usd)

            # Learner updates
            if self.linucb is not None and ctx.arm_idx >= 0:
                self.linucb.update(ctx.arm_idx, features, ctx.reward)
            if self.moo is not None:
                latency_s = max(0.001, ctx.latency_ms / 1000.0)
                outcome = [ctx.reward, 1.0 / (1.0 + latency_s),
                           1.0 / (1.0 + ctx.tokens / 100.0)]
                self.moo.update_weights(outcome)
            if self.rlhf is not None and ctx.arm_idx >= 0:
                with contextlib.suppress(Exception):
                    self.rlhf.update(features, ctx.arm_idx, ctx.reward)
            if self.router is not None:
                await self.router.update(ctx.teacher_name, ctx.reward,
                                         redacted_prompt)

            # Causal RL (active policy)
            state_vec = np.asarray(features, dtype=np.float64)
            if self.causal is not None:
                self.causal.update(CausalTransition(
                    state=state_vec, action=ctx.arm_idx if ctx.arm_idx >= 0 else 0,
                    reward=ctx.reward, next_state=state_vec))
                self.last_counterfactuals = self.causal.counterfactuals(
                    state_vec, len(self.arms))

            # Federated (real weights)
            if self.federated is not None:
                try:
                    # Use LinUCB theta as a weight vector
                    if self.linucb is not None and ctx.arm_idx >= 0:
                        local_w = np.asarray(
                            self.linucb.theta[ctx.arm_idx], dtype=np.float64)
                    else:
                        local_w = np.asarray([ctx.reward], dtype=np.float64)
                    self.federated.submit(FederatedUpdate(
                        node_id=self.node_id,
                        weights={"theta": local_w},
                        n_samples=1,
                        carbon_intensity=float(carbon_intensity)))
                    self.federated.aggregate()
                except PrivacyBudgetExceeded as e:
                    logger.warning(f"Federated DP exhausted: {e}")

            # STL monitor + shield (apply the replacement)
            self.temporal.observe({
                "latency_ms": ctx.latency_ms,
                "success": ctx.success,
                "cost_usd": ctx.cost_usd,
                "carbon_intensity": carbon_intensity,
            })
            verdict = self.temporal.verify()
            ctx.stl_verdict = verdict
            violated = [k for k, ok in verdict.items() if not ok]
            if violated:
                # Apply a safer arm
                safe_arm = min(self.arms, key=lambda a: a.get("max_tokens", 150))
                if safe_arm != self.arms[ctx.arm_idx if ctx.arm_idx >= 0 else 0]:
                    ctx.shield_applied = True
                    ctx.params.update(safe_arm)
                    if ctx.arm_idx >= 0:
                        ctx.arm_idx = self.arms.index(safe_arm)
                ctx.safety_ok = False
            else:
                ctx.safety_ok = True

            # Uncertainty + HITL
            if self.uncertainty and self.hitl:
                probs = None
                if self.linucb is not None:
                    try:
                        scores = []
                        for a in range(len(self.arms)):
                            A_inv = np.linalg.inv(self.linucb.A[a])
                            theta = A_inv.dot(self.linucb.b[a])
                            scores.append(float(theta.dot(features)))
                        probs = np.exp(np.array(scores) - np.max(scores))
                        probs = probs / probs.sum()
                    except Exception:
                        probs = None
                is_unc, unc = self.uncertainty.is_uncertain(probs)
                ctx.uncertainty = unc
                if is_unc or not ctx.safety_ok:
                    req = HITLRequest.new(
                        reason=("high_uncertainty" if is_unc
                                else "safety_violation"),
                        chosen_idx=ctx.arm_idx,
                        candidates=list(self.arms),
                        uncertainty=unc,
                        carbon_price=carbon_price,
                        priority=(2 if not ctx.safety_ok else 1))
                    approved = await self.hitl.request(req)
                    ctx.human_approved = approved
                    if not approved and ctx.success:
                        # Conservative fallback
                        smallest = min(self.arms,
                                       key=lambda a: a.get("max_tokens", 999))
                        try:
                            if client is self:
                                result = await self._do_request_with_retry(
                                    {"prompt": redacted_prompt, **smallest},
                                    self.endpoint)
                                ctx.response = (result.get("text")
                                                or result.get("generated_text")
                                                or result.get("response")
                                                or ctx.response)
                            else:
                                ctx.response = await client.generate_explanation(
                                    redacted_prompt, **smallest)
                        except Exception:
                            pass
                self.uncertainty.observe(ctx.reward)
                if self.active_learner:
                    self.active_learner.maybe_store(
                        {"prompt_len": float(features[0]),
                         "arm_idx": ctx.arm_idx,
                         "reward": ctx.reward,
                         "latency_ms": ctx.latency_ms},
                        uncertainty=unc, threshold=0.3)

            # XAI
            if self.explainer is not None:
                chosen = {
                    "prompt_len": float(features[0]),
                    "num_words": float(features[1]),
                    "avg_word_len": float(features[2]),
                    "hour_norm": float(features[3]),
                }
                pareto = [{
                    "prompt_len": float(features[0]),
                    "num_words": float(features[1]),
                    "avg_word_len": float(features[2]),
                    "hour_norm": float(features[3]),
                    "value": float(self._ga_fitness(a)),
                } for a in self.arms]
                confidence = float(np.max(probs)) if probs is not None else 0.5
                explanation = self.explainer.explain(
                    chosen_idx=ctx.arm_idx if ctx.arm_idx >= 0 else 0,
                    chosen=chosen, pareto=pareto,
                    counterfactuals=self.last_counterfactuals,
                    safety_ok=ctx.safety_ok and ctx.human_approved,
                    carbon_price=carbon_price,
                    confidence=confidence)
                self.last_explanation = explanation

            # GA evolution (periodic)
            if self.ga is not None:
                self._calls_since_ga += 1
                if self._calls_since_ga >= self.ga_evolve_every:
                    self._calls_since_ga = 0
                    best_arm = self.ga.evolve(self._ga_fitness, generations=3)
                    if best_arm:
                        worst_idx = int(np.argmin(
                            [self._ga_fitness(a) for a in self.arms]))
                        self.arms[worst_idx] = best_arm
                        self.persistence.ga_record(
                            best_arm, self._ga_fitness(best_arm))

            # Self-healing
            if self.self_healing is not None:
                quality = self._compute_reward(ctx.response)
                await self.self_healing.record_quality(quality)
                anomaly, _score = await self.self_healing.detect_anomaly(quality)
                if anomaly:
                    await self.self_healing.trigger_recovery()
                    if ctx.success:
                        ctx.response = self.fallback_generator(redacted_prompt)

            # Chaos steady-state check
            if self.chaos and self.chaos.hypotheses:
                metrics = {
                    "latency_ms": ctx.latency_ms,
                    "success_rate": 1.0 if ctx.success else 0.0,
                }
                if not self.chaos.steady_state_ok(metrics):
                    logger.warning("Chaos steady-state violated; deactivating.")
                    self.chaos.deactivate()

            # Cache
            if self.cache is not None and ctx.success:
                await self.cache.set(redacted_prompt, ctx.response)

            # Lineage
            self._record_lineage(ctx)

            METRIC_REQUESTS.labels(
                status="success" if ctx.success else "fallback").inc()
            METRIC_DURATION.observe(ctx.latency_ms / 1000.0)
            return ctx.response
        finally:
            if fault_injected and self.chaos:
                self.chaos.release_fault()

    # ------------------------------------------------------------------
    # Batch / stream
    # ------------------------------------------------------------------
    async def batch_generate_explanations(self, prompts: List[str],
                                          **kwargs: Any) -> List[str]:
        return await asyncio.gather(
            *[self.generate_explanation(p, **kwargs) for p in prompts])

    async def stream_explanation(self, prompt: str,
                                 **kwargs: Any) -> AsyncIterable[str]:
        text = await self.generate_explanation(prompt, **kwargs)
        # Yield in chunks to simulate streaming
        chunk_size = 32
        for i in range(0, len(text), chunk_size):
            yield text[i:i + chunk_size]
            await asyncio.sleep(0)

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------
    def get_circuit_breaker_status(self) -> Dict[str, Any]:
        return {name: cb.get_status()
                for name, cb in self._circuit_breakers.items()}

    def get_stats(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "node_id": self.node_id,
            "circuit_breakers": self.get_circuit_breaker_status(),
            "cache": {"enabled": self.cache is not None,
                      "size": self.cache.size() if self.cache else 0},
            "lineage": {"records": len(self.lineage_records)},
            "rate_limit_remaining": self._rate_limiter.remaining(),
            "today_cost_usd": self._today_cost,
            "daily_cost_budget_usd": self.config.daily_cost_budget_usd,
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
            "current_precision": self.current_precision.value
                if self.current_precision else None,
            "carbon": {"intensity": self.carbon_manager.current.intensity_g_co2_per_kwh,
                       "price": self.carbon_manager.current.price_usd_per_kg,
                       "source": self.carbon_manager.current.source,
                       "rec_kwh": self.recs.total_kwh() if self.recs else 0.0},
            "rlhf": {"enabled": self.rlhf is not None},
            "distillation": {"enabled": self.arm_distiller is not None},
            "limit_graph": {"enabled": self.limit_graph is not None},
            "last_explanation": self.last_explanation.to_dict()
                if self.last_explanation else None,
            "counterfactuals": self.last_counterfactuals,
            "active_learning_size": len(self.active_learner)
                if self.active_learner else 0,
            "hitl": self.hitl.stats() if self.hitl else None,
            "health": self.health_check(),
        }
        if self.moo is not None:
            stats["moo"] = {"weights": list(self.moo.weights)}
        if self.ga is not None:
            stats["ga"] = {"population_size": self.ga.pop_size,
                           "arm_history": len(self.ga.arm_history)}
        if self.router is not None:
            stats["moe"] = self.router.get_stats()
        if self.self_healing is not None:
            stats["self_healing"] = self.self_healing.get_stats()
        if self.roles is not None:
            stats["roles"] = self.roles.stats()
        if self.temporal is not None:
            stats["stl_verdict"] = self.temporal.verify()
        if self.precision_controller is not None:
            stats["precision"] = self.precision_controller.stats()
        if self.federated is not None:
            stats["federated"] = self.federated.stats()
        return stats

    def health_check(self) -> Dict[str, Any]:
        cbs = self.get_circuit_breaker_status()
        cb_open = sum(1 for s in cbs.values() if s["state"] == "open")
        return {
            "status": "healthy" if cb_open == 0 else "degraded",
            "open_circuit_breakers": cb_open,
            "cache_size": self.cache.size() if self.cache else 0,
            "pending_hitl": self.hitl.stats()["pending"] if self.hitl else 0,
            "today_cost_usd": self._today_cost,
        }


# ===========================================================================
# FastAPI integration
# ===========================================================================
def build_fastapi_app(client: LLMClient,
                      jwt_secret: Optional[str] = None) -> Any:
    try:
        from fastapi import FastAPI, HTTPException, Depends
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
        await client.close()

    app = FastAPI(title="LLM Client API", version="18.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"])
    security = HTTPBearer(auto_error=False)

    async def verify(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
        if not JOSE_AVAILABLE or not jwt_secret:
            return {"sub": "anonymous"}
        if credentials is None:
            raise HTTPException(status_code=401, detail="missing credentials")
        try:
            return jwt.decode(credentials.credentials, jwt_secret,
                              algorithms=["HS256"])
        except JWTError:
            raise HTTPException(status_code=401, detail="invalid token")

    @app.post("/generate")
    async def generate(prompt: str, max_tokens: int = 150,
                       temperature: float = 0.7, _=Depends(verify)):
        return {"response": await client.generate_explanation(
            prompt, max_tokens=max_tokens, temperature=temperature)}

    @app.get("/status")
    async def status(_=Depends(verify)):
        return client.get_stats()

    @app.get("/health")
    async def health():
        return client.health_check()

    @app.get("/cache")
    async def cache_status(_=Depends(verify)):
        return {"size": client.cache.size() if client.cache else 0}

    @app.post("/cache/clear")
    async def cache_clear(_=Depends(verify)):
        if client.cache and client.cache._persist:
            with contextlib.suppress(Exception):
                client.cache._persist._execute("DELETE FROM cache")
        if client.cache:
            client.cache._mem = {}
        return {"status": "cleared"}

    @app.get("/hitl/pending")
    async def hitl_pending(_=Depends(verify)):
        if not client.hitl:
            return {"pending": []}
        return {"pending": client.hitl.pending_requests()}

    @app.post("/hitl/approval")
    async def hitl_approval(request_id: str, approved: bool, _=Depends(verify)):
        if not client.hitl:
            raise HTTPException(status_code=503, detail="HITL disabled")
        ok = client.hitl.resolve(request_id, approved)
        if not ok:
            raise HTTPException(status_code=404, detail="not_found")
        return {"status": "resolved", "request_id": request_id,
                "approved": approved}

    @app.get("/hitl/audit")
    async def hitl_audit(limit: int = 100, _=Depends(verify)):
        if not client.hitl:
            return {"audit": []}
        return {"audit": client.hitl.audit_trail(limit=limit),
                "verified": client.hitl.verify_audit_chain()}

    @app.get("/circuit_breakers")
    async def cbs(_=Depends(verify)):
        return client.get_circuit_breaker_status()

    return app


# ===========================================================================
# Smoke test
# ===========================================================================
async def _smoke_test() -> None:
    async def _fake_do_request(self, payload, endpoint=None):
        return {"text": f"Recommend optimizing for prompt length "
                        f"{len(payload.get('prompt', ''))}"}

    LLMClient._do_request = _fake_do_request  # type: ignore

    client = LLMClient(config=LLMClientConfig(
        endpoint="http://fake", model="small",
        cache_enabled=True, enable_chaos=False,
        lineage_enabled=True, persist_path="/tmp/llm_client_smoke.db",
    ))
    for _ in range(3):
        text = await client.generate_explanation(
            "How do we reduce helium usage? email me at user@example.com")
        print(f"→ {text[:80]}...")
    stats = client.get_stats()
    print(json.dumps({k: v for k, v in stats.items()
                      if k != "ten_enhancements"}, indent=2, default=str))
    print("ten_enhancements:", stats["ten_enhancements"])
    print("health:", client.health_check())
    await client.close()


if __name__ == "__main__":
    asyncio.run(_smoke_test())
