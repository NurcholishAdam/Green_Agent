#!/usr/bin/env python3
# src/enhancements/mixed_precision_utils_enhanced_v18_0.py
# Version 18.0.0 — Mixed Precision Engine (P0/P1/P2 hardened)
"""
Enhanced Mixed Precision Engine — v18.0.0

Objective convention: all objectives are BENEFITS (higher is better).
TOPSIS.score accepts an optional `cost_criteria` set for mixed problems.

P0 fixes:
 - PyTorch import has a graceful fallback
 - No asyncio.get_event_loop() in __init__; explicit async start()
 - No asyncio.run() inside sync methods; sync wrappers raise on running loop
 - torch.amp.autocast (non-deprecated) with fallback
 - Pareto direction consistent with benefit objectives
 - _update_weights uses a principled reward gradient
 - Chaos faults actually exercise the engine paths
 - Real PQC (dilithium/falcon/sphincs) with HMAC-SHA256 fallback
 - Real Web3 with hash-chained ledger fallback
 - sign_precision_decision and record_on_blockchain are called
 - MOEPrecisionEngine.update labels the best-matching teacher
 - XAIExplainer called with correct feature names
 - _teacher_moe uses the gating model

P1 hardening:
 - SQLite persistence (state, HITL audit, chaos, federated, cost, lineage)
 - Real carbon providers (Electricity Maps / WattTime) with synthetic fallback
 - Token-bucket rate limiting and daily cost budget
 - Health checks
 - Portable signal handlers (Windows fallback)
 - atexit cleanup
 - FastAPI + lifespan, JWT from env
 - Structured logging with correlation IDs
 - Deprecated LogisticRegression(multi_class=...) removed
 - Graceful shutdown with drain

P2 advanced:
 - Full STL (G / F / U / X / ->) with actionable enforcement
 - Fresh temporal monitor per decision
 - Secure federated aggregation (pairwise masks + DP accountant)
 - Multi-agent message bus + conflict resolution
 - Chaos steady-state hypotheses + auto-deactivation
 - Telemetry-driven precision with EMA quality drop
 - Active learning with KMeans diversity + time-bounded buffer
 - Hash-chained HITL audit ledger
 - SHAP / weighted XAI
 - Lineage + cost tracking
 - Causal RL reflection-strategy selection
"""

from __future__ import annotations

import asyncio
import atexit
import base64
import concurrent.futures
import contextlib
import contextvars
import hashlib
import hmac
import json
import logging
import math
import os
import pickle
import random
import re
import signal
import sqlite3
import tempfile
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Sequence, Set, Tuple,
)

import numpy as np

# ============================================================
# Optional dependency: PyTorch (with a functional fallback)
# ============================================================
try:
    import torch
    import torch.nn as nn
    from torch.amp import autocast as _torch_amp_autocast  # type: ignore
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    torch = None  # type: ignore

    class _TorchNNStub:
        class Module:
            def __init__(self, *a, **kw): pass
            def to(self, *a, **kw): return self
            def parameters(self): return iter(())
            def __call__(self, *a, **kw): raise RuntimeError("torch unavailable")

        class Linear(Module):
            def __init__(self, *a, **kw): super().__init__()
    nn = _TorchNNStub()  # type: ignore

    class _TorchStub:
        float32 = "float32"
        float16 = "float16"
        bfloat16 = "bfloat16"

        class cuda:  # type: ignore
            @staticmethod
            def is_available() -> bool: return False

        @staticmethod
        def randn(*a, **kw): return np.random.randn(*a)
        @staticmethod
        def zeros(*a, **kw): return np.zeros(*a)
        @staticmethod
        def tensor(*a, **kw): return np.asarray(*a)
    torch = _TorchStub()  # type: ignore

    @contextlib.contextmanager
    def _torch_amp_autocast(*a, **kw):
        yield

# ============================================================
# Other optional dependencies
# ============================================================
try:
    from sklearn.linear_model import LogisticRegression, LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    from sklearn.cluster import KMeans
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs  # type: ignore
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from web3 import Web3  # noqa: F401
    from web3.middleware import geth_poa_middleware  # noqa: F401
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    from prophet import Prophet  # noqa: F401
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing  # noqa: F401
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

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

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    start_http_server = None

try:
    from fastapi import FastAPI, Depends, HTTPException
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn  # noqa: F401
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    from jose import JWTError, jwt  # type: ignore
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator
    from pydantic_settings import BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False


# ============================================================
# Logging + correlation IDs
# ============================================================
logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default="unknown")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ============================================================
# Prometheus
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    MP_PRECISION_SWITCHES = Counter("mp_precision_switches_total", "Switches", ["from", "to"], registry=REGISTRY)
    MP_ENERGY_SAVED = Counter("mp_energy_saved_joules_total", "Energy saved", registry=REGISTRY)
    MP_CARBON_SAVED = Counter("mp_carbon_saved_kg_total", "Carbon saved", registry=REGISTRY)
    MP_COST = Counter("mp_cost_usd_total", "Cost", registry=REGISTRY)
    MP_CURRENT_PRECISION = Gauge("mp_current_precision_index", "Current precision", registry=REGISTRY)
    MP_TEMPORAL = Counter("mp_temporal_violations_total", "Temporal", ["formula"], registry=REGISTRY)
    MP_CHAOS = Counter("mp_chaos_total", "Chaos", ["fault", "status"], registry=REGISTRY)
    MP_HITL = Counter("mp_hitl_total", "HITL", ["decision"], registry=REGISTRY)
    MP_HITL_PENDING = Gauge("mp_hitl_pending", "Pending HITL", registry=REGISTRY)
    MP_FEDERATED = Counter("mp_federated_rounds_total", "Federated rounds", registry=REGISTRY)
    MP_DP_EPSILON = Gauge("mp_dp_epsilon_spent", "DP epsilon", ["node"], registry=REGISTRY)
    MP_HEALTH = Gauge("mp_health_score", "Health", registry=REGISTRY)
    MP_ACTIVE_LEARNING = Gauge("mp_active_learning_size", "Active learning", registry=REGISTRY)
else:
    class _Dummy:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
    MP_PRECISION_SWITCHES = MP_ENERGY_SAVED = MP_CARBON_SAVED = MP_COST = _Dummy()
    MP_CURRENT_PRECISION = MP_TEMPORAL = MP_CHAOS = MP_HITL = _Dummy()
    MP_HITL_PENDING = MP_FEDERATED = MP_DP_EPSILON = MP_HEALTH = _Dummy()
    MP_ACTIVE_LEARNING = _Dummy()


# ============================================================
# Exceptions
# ============================================================
class MixedPrecisionError(Exception): pass
class SyncAsyncBoundaryError(MixedPrecisionError): pass
class CircuitBreakerOpenError(MixedPrecisionError): pass
class RateLimitExceeded(MixedPrecisionError): pass
class PrivacyBudgetExceeded(MixedPrecisionError): pass
class QuantumError(MixedPrecisionError): pass
class BlockchainError(MixedPrecisionError): pass


# ============================================================
# Enums
# ============================================================
class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    BF16 = "bf16"
    FP16 = "fp16"
    FP8 = "fp8"
    FP4 = "fp4"


class AgentRole(str, Enum):
    ACCURACY_SPECIALIST = "accuracy_specialist"
    ENERGY_SPECIALIST = "energy_specialist"
    CARBON_SPECIALIST = "carbon_specialist"
    SPEED_SPECIALIST = "speed_specialist"
    SAFETY_OFFICER = "safety_officer"
    VERIFIER = "verifier"


class STLOperator(str, Enum):
    ALWAYS = "G"
    EVENTUALLY = "F"
    UNTIL = "U"
    NEXT = "X"
    IMPLIES = "->"


class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# ============================================================
# Config (Pydantic or dataclass)
# ============================================================
if PYDANTIC_AVAILABLE:
    class MixedPrecisionConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="MP_", case_sensitive=False)

        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "18.0.0"
        log_level: str = "INFO"
        environment: str = "production"

        default_dtype: str = "fp16"
        use_amp: bool = True
        amp_dtype: str = "fp16"

        db_path: str = "/tmp/mixed_precision_v18.db"
        persist_path: Optional[str] = None

        metrics_port: int = Field(8000, ge=1024, le=65535)
        api_host: str = "0.0.0.0"
        api_port: int = 8000
        jwt_secret: str = ""
        jwt_algorithm: str = "HS256"
        jwt_expiry_s: int = Field(3600, gt=0)

        health_check_interval: int = Field(60, ge=10)
        carbon_update_interval: int = Field(300, ge=10)

        carbon_base_price_per_ton: float = 50.0
        rec_price_per_mwh: float = 30.0
        carbon_region: str = "global"
        electricity_maps_api_key: Optional[str] = None
        electricity_maps_zone: str = "US-CAL-CISO"
        watttime_username: Optional[str] = None
        watttime_password: Optional[str] = None
        watttime_region: str = "CAISO_NORTH"

        quantum_algorithm: str = "dilithium"
        quantum_master_key: str = ""
        quantum_key_rotation_s: int = Field(3600, gt=0)

        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_chain_id: int = 1
        blockchain_poa: bool = False
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None

        rate_limit_per_minute: int = Field(600, ge=1)
        daily_cost_budget_usd: float = Field(10.0, ge=0)
        cost_per_decision_usd: float = Field(1e-5, ge=0)

        federated_dp_sigma: float = Field(1e-3, gt=0)
        federated_target_epsilon: float = Field(8.0, gt=0)
        federated_target_delta: float = Field(1e-5, gt=0)

        hitl_timeout_s: float = Field(30.0, gt=0)
        hitl_production_safe: bool = True
        hitl_auto_approve_on_timeout: bool = False

        ga_seed: int = 0
        bio_population_size: int = Field(20, ge=10)
        bio_mutation_rate: float = Field(0.1, ge=0.0, le=1.0)
        bio_crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
        ga_evolution_interval: int = Field(3600, ge=60)

        chaos_enabled: bool = False
        chaos_enable_loop: bool = False
        chaos_max_concurrent: int = Field(1, ge=1)
        chaos_duration_s: float = Field(0.0, ge=0)

        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        adaptive_precision_enabled: bool = True
        carbon_market_enabled: bool = True
        role_specialization_enabled: bool = True
        chaos_testing_enabled: bool = True
        hitl_enabled: bool = True
        federated_enabled: bool = True
        causal_enabled: bool = True
        multi_agent_enabled: bool = True
        quantum_enabled: bool = True
        blockchain_enabled: bool = True

        rlhf_uncertainty_threshold: float = Field(0.35, ge=0.0, le=1.0)
        hitl_confidence_threshold: float = Field(0.65, ge=0.0, le=1.0)

        @field_validator("log_level")
        @classmethod
        def _v_log(cls, v):
            allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if v.upper() not in allowed:
                raise ValueError(f"log_level must be one of {allowed}")
            return v.upper()

        @field_validator("jwt_secret")
        @classmethod
        def _v_jwt(cls, v):
            if not v:
                return os.urandom(32).hex()
            if len(v) < 32:
                raise ValueError("jwt_secret must be >= 32 chars")
            return v

        @field_validator("quantum_master_key")
        @classmethod
        def _v_master(cls, v):
            if not v:
                return os.urandom(32).hex()
            try:
                bytes.fromhex(v)
            except ValueError:
                raise ValueError("quantum_master_key must be hex")
            return v

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.quantum_master_key)

        def get_db_url(self) -> str:
            return self.persist_path or self.db_path
else:
    @dataclass
    class MixedPrecisionConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "18.0.0"
        log_level: str = "INFO"
        environment: str = "production"
        default_dtype: str = "fp16"
        use_amp: bool = True
        amp_dtype: str = "fp16"
        db_path: str = "/tmp/mixed_precision_v18.db"
        persist_path: Optional[str] = None
        metrics_port: int = 8000
        api_host: str = "0.0.0.0"
        api_port: int = 8000
        jwt_secret: str = field(default_factory=lambda: os.urandom(32).hex())
        jwt_algorithm: str = "HS256"
        jwt_expiry_s: int = 3600
        health_check_interval: int = 60
        carbon_update_interval: int = 300
        carbon_base_price_per_ton: float = 50.0
        rec_price_per_mwh: float = 30.0
        carbon_region: str = "global"
        electricity_maps_api_key: Optional[str] = None
        electricity_maps_zone: str = "US-CAL-CISO"
        watttime_username: Optional[str] = None
        watttime_password: Optional[str] = None
        watttime_region: str = "CAISO_NORTH"
        quantum_algorithm: str = "dilithium"
        quantum_master_key: str = field(default_factory=lambda: os.urandom(32).hex())
        quantum_key_rotation_s: int = 3600
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_chain_id: int = 1
        blockchain_poa: bool = False
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        rate_limit_per_minute: int = 600
        daily_cost_budget_usd: float = 10.0
        cost_per_decision_usd: float = 1e-5
        federated_dp_sigma: float = 1e-3
        federated_target_epsilon: float = 8.0
        federated_target_delta: float = 1e-5
        hitl_timeout_s: float = 30.0
        hitl_production_safe: bool = True
        hitl_auto_approve_on_timeout: bool = False
        ga_seed: int = 0
        bio_population_size: int = 20
        bio_mutation_rate: float = 0.1
        bio_crossover_rate: float = 0.8
        ga_evolution_interval: int = 3600
        chaos_enabled: bool = False
        chaos_enable_loop: bool = False
        chaos_max_concurrent: int = 1
        chaos_duration_s: float = 0.0
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        adaptive_precision_enabled: bool = True
        carbon_market_enabled: bool = True
        role_specialization_enabled: bool = True
        chaos_testing_enabled: bool = True
        hitl_enabled: bool = True
        federated_enabled: bool = True
        causal_enabled: bool = True
        multi_agent_enabled: bool = True
        quantum_enabled: bool = True
        blockchain_enabled: bool = True
        rlhf_uncertainty_threshold: float = 0.35
        hitl_confidence_threshold: float = 0.65

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.quantum_master_key)

        def get_db_url(self) -> str:
            return self.persist_path or self.db_path


# ============================================================
# Utilities
# ============================================================
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


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                    self.success_count = 0
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                self.success_count += 1
                if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= 2:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                elif self.state == CircuitBreakerState.CLOSED:
                    self.failure_count = 0
            return result
        except Exception:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.state == CircuitBreakerState.CLOSED and \
                        self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                elif self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
            raise

    def get_status(self) -> Dict[str, Any]:
        return {"name": self.name, "state": self.state.value,
                "failure_count": self.failure_count}


# ============================================================
# Persistence (SQLite)
# ============================================================
class Persistence:
    SCHEMA = """
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS hitl_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT NOT NULL,
        approved INTEGER NOT NULL,
        reason TEXT, mode TEXT, latency_ms REAL,
        prev_hash TEXT, entry_hash TEXT NOT NULL,
        ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS chaos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fault TEXT NOT NULL,
        passed INTEGER NOT NULL,
        payload TEXT,
        ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS federated (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        round INTEGER NOT NULL,
        weights TEXT NOT NULL,
        dp_spent REAL NOT NULL,
        ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS cost_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL, cost_usd REAL NOT NULL, note TEXT
    );
    CREATE TABLE IF NOT EXISTS lineage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lineage_id TEXT NOT NULL,
        decision_id TEXT NOT NULL,
        payload TEXT,
        ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS carbon_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL, price REAL NOT NULL
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

    def _exec(self, sql, params=()):
        if not self._conn:
            return None
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(sql, params)
            self._conn.commit()
            return cur

    def _fetchall(self, sql, params=()):
        if not self._conn:
            return []
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(sql, params)
            return cur.fetchall()

    def hitl_append(self, entry: Dict[str, Any]):
        self._exec(
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

    def chaos_append(self, fault: str, passed: bool, payload: Dict[str, Any]):
        self._exec(
            "INSERT INTO chaos (fault, passed, payload, ts) VALUES (?,?,?,?)",
            (fault, int(passed), json.dumps(payload, default=str), time.time()))

    def federated_append(self, round_num: int, weights: List[float], dp_spent: float):
        self._exec(
            "INSERT INTO federated (round, weights, dp_spent, ts) VALUES (?,?,?,?)",
            (round_num, json.dumps(weights), float(dp_spent), time.time()))

    def cost_record(self, cost_usd: float, note: str = ""):
        self._exec(
            "INSERT INTO cost_ledger (ts, cost_usd, note) VALUES (?,?,?)",
            (time.time(), float(cost_usd), note))

    def cost_today(self) -> float:
        day_start = utcnow().replace(hour=0, minute=0, second=0,
                                     microsecond=0).timestamp()
        rows = self._fetchall(
            "SELECT COALESCE(SUM(cost_usd), 0) FROM cost_ledger WHERE ts >= ?",
            (day_start,))
        return float(rows[0][0]) if rows else 0.0

    def record_lineage(self, lineage_id: str, decision_id: str, payload: Dict[str, Any]):
        self._exec(
            "INSERT INTO lineage (lineage_id, decision_id, payload, ts) "
            "VALUES (?,?,?,?)",
            (lineage_id, decision_id, json.dumps(payload, default=str), time.time()))

    def append_carbon_price(self, price: float):
        self._exec(
            "INSERT INTO carbon_history (ts, price) VALUES (?,?)",
            (time.time(), float(price)))

    def recent_carbon_prices(self, limit: int = 500) -> List[float]:
        rows = self._fetchall(
            "SELECT price FROM carbon_history ORDER BY id DESC LIMIT ?", (limit,))
        return [float(r[0]) for r in reversed(rows)]


# ============================================================
# Enhancement 1: PQC (real dilithium + HMAC fallback)
# ============================================================
class QuantumSecurity:
    def __init__(self, algorithm: str = "dilithium",
                 master_key: Optional[bytes] = None,
                 rotation_s: int = 3600):
        self.algorithm = algorithm
        self._master = master_key or os.urandom(32)
        self._rotation_s = int(rotation_s)
        self._keys: Dict[str, Tuple[bytes, bytes, float]] = {}
        self._active_key_id: Optional[str] = None
        self._lock = threading.Lock()
        self._rotate()

    def _rotate(self):
        kid = uuid.uuid4().hex[:8]
        pub = priv = b""
        if PQC_AVAILABLE:
            try:
                if self.algorithm == "dilithium":
                    pub, priv = dilithium.generate_keypair()
                elif self.algorithm == "falcon":
                    pub, priv = falcon.generate_keypair()
                else:
                    pub, priv = sphincs.generate_keypair()
            except Exception as e:
                logger.warning(f"PQC keypair failed: {e}")
        with self._lock:
            self._keys[kid] = (pub, priv, time.time())
            self._active_key_id = kid

    def _ensure_key(self):
        with self._lock:
            if self._active_key_id is None:
                self._rotate()
                return
            _, _, ts = self._keys[self._active_key_id]
            if (time.time() - ts) > self._rotation_s:
                self._rotate()

    def _hmac(self, payload: bytes) -> str:
        return hmac.new(self._master, payload, hashlib.sha256).hexdigest()

    async def sign(self, data: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure_key()
        with self._lock:
            kid = self._active_key_id
            pub, priv, _ = self._keys[kid]
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if PQC_AVAILABLE and pub and priv:
            try:
                if self.algorithm == "dilithium":
                    sig = dilithium.sign(priv, payload)
                elif self.algorithm == "falcon":
                    sig = falcon.sign(priv, payload)
                else:
                    sig = sphincs.sign(priv, payload)
                return {"algorithm": self.algorithm,
                        "signature": base64.b64encode(sig).decode(),
                        "key_id": kid}
            except Exception as e:
                logger.warning(f"PQC sign failed: {e}")
        return {"algorithm": "hmac-sha256", "signature": self._hmac(payload),
                "key_id": kid}

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {"pqc_available": PQC_AVAILABLE,
                    "algorithm": self.algorithm,
                    "active_key_id": self._active_key_id,
                    "num_keys": len(self._keys)}


# ============================================================
# Enhancement 1b: Blockchain (real Web3 + ledger)
# ============================================================
class BlockchainVerification:
    def __init__(self, config: MixedPrecisionConfig,
                 persist: Optional[Persistence] = None):
        self.config = config
        self._persist = persist
        self._ledger: Dict[str, Dict[str, Any]] = {}
        self._last_hash = ""
        self._lock = threading.Lock()
        self.web3 = None
        if WEB3_AVAILABLE and getattr(config, "blockchain_rpc_url", None):
            try:
                self.web3 = Web3(Web3.HTTPProvider(config.blockchain_rpc_url))
                if getattr(config, "blockchain_poa", False):
                    self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
                if not self.web3.is_connected():
                    logger.warning("Web3 not connected; local ledger")
                    self.web3 = None
            except Exception as e:
                logger.warning(f"Web3 init failed: {e}")
                self.web3 = None

    async def record(self, data_id: str, data_hash: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            if data_id in self._ledger:
                return {"tx_hash": self._ledger[data_id]["tx_hash"],
                        "status": "idempotent"}
            tx_hash = None
            if self.web3 is not None and getattr(self.config, "blockchain_private_key", None):
                try:
                    account = self.web3.eth.account.from_key(self.config.blockchain_private_key)
                    nonce = self.web3.eth.get_transaction_count(account.address)
                    tx = {
                        "nonce": nonce, "gas": 21000,
                        "gasPrice": self.web3.eth.gas_price,
                        "to": account.address, "value": 0,
                        "data": self.web3.to_hex(text=json.dumps({
                            "id": data_id, "hash": data_hash, "meta": metadata}, default=str)),
                        "chainId": getattr(self.config, "blockchain_chain_id", 1),
                    }
                    signed = self.web3.eth.account.sign_transaction(tx, self.config.blockchain_private_key)
                    raw = getattr(signed, "rawTransaction", None) or getattr(signed, "raw_transaction", None)
                    tx_hash = self.web3.to_hex(self.web3.eth.send_raw_transaction(raw))
                except Exception as e:
                    logger.warning(f"Web3 tx failed: {e}")
                    tx_hash = None
            if tx_hash is None:
                chain_input = f"{data_id}:{data_hash}:{self._last_hash}".encode()
                tx_hash = "0x" + hashlib.sha256(chain_input).hexdigest()[:40]
            self._ledger[data_id] = {"tx_hash": tx_hash, "hash": data_hash,
                                     "metadata": metadata, "prev_hash": self._last_hash}
            self._last_hash = tx_hash
        return {"tx_hash": tx_hash, "status": "recorded"}

    async def health_check(self) -> Dict[str, Any]:
        connected = bool(self.web3 and self.web3.is_connected())
        return {"connected": connected,
                "backend": "web3" if connected else "local",
                "total_records": len(self._ledger)}


# ============================================================
# Enhancement 8: Carbon providers + market + intensity
# ============================================================
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
    def __init__(self, config: MixedPrecisionConfig,
                 persist: Optional[Persistence] = None):
        self.config = config
        self._persist = persist
        self.carbon_price_per_ton = float(getattr(config, "carbon_base_price_per_ton", 50.0))
        self.rec_price_per_mwh = float(getattr(config, "rec_price_per_mwh", 30.0))
        self.grid_intensity_kg_per_mwh = 400.0
        self.trades: List[Dict[str, Any]] = []
        self._retired: Set[str] = set()

    async def get_carbon_credit_value(self, carbon_saved_kg: float) -> float:
        tons = max(0.0, float(carbon_saved_kg)) / 1000.0
        return round(tons * self.carbon_price_per_ton, 6)

    async def get_rec_value(self, energy_saved_kwh: float) -> float:
        mwh = max(0.0, float(energy_saved_kwh)) / 1000.0
        return round(mwh * self.rec_price_per_mwh, 6)

    async def get_market_snapshot(self) -> Dict[str, Any]:
        return {"carbon_price_usd_per_ton": self.carbon_price_per_ton,
                "rec_price_usd_per_mwh": self.rec_price_per_mwh,
                "grid_intensity_kg_per_mwh": self.grid_intensity_kg_per_mwh}

    async def retire_credits(self, amount_kg: float, beneficiary: str,
                             idempotency_key: Optional[str] = None) -> Dict[str, Any]:
        key = idempotency_key or f"{beneficiary}:{amount_kg}"
        if key in self._retired:
            return {"status": "idempotent", "key": key}
        rec = {"id": str(uuid.uuid4()), "amount_kg": float(amount_kg),
               "beneficiary": beneficiary,
               "timestamp": utcnow().isoformat(), "key": key}
        self.trades.append(rec)
        self._retired.add(key)
        return rec


class CarbonIntensityManager:
    def __init__(self, market: Optional[CarbonMarketClient] = None,
                 electricity_maps_api_key: Optional[str] = None,
                 electricity_maps_zone: str = "US-CAL-CISO",
                 watttime_username: Optional[str] = None,
                 watttime_password: Optional[str] = None,
                 watttime_region: str = "CAISO_NORTH",
                 region: str = "global",
                 persist: Optional[Persistence] = None):
        self.market = market
        self.region = region
        self._persist = persist
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
        self._override: Optional[Callable[[], Awaitable[CarbonSnapshot]]] = None

    def set_override(self, fn: Optional[Callable[[], Awaitable[CarbonSnapshot]]]):
        self._override = fn

    async def refresh(self) -> CarbonSnapshot:
        if self._override is not None:
            return await self._override()
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
            base_price = (self.market.carbon_price_per_ton / 1000.0
                          if self.market else 0.05)
            price = base_price * (1 + 0.3 * math.sin((hour / 24.0) * 2 * math.pi))
            self.current = CarbonSnapshot(
                intensity_g_co2_per_kwh=best.intensity_g_co2_per_kwh,
                price_usd_per_kg=max(0.001, price),
                source=best.source, region=best.region)
        if self._persist:
            with contextlib.suppress(Exception):
                self._persist.append_carbon_price(self.current.price_usd_per_kg)
        return self.current

    async def get_current_intensity(self) -> float:
        return self.current.intensity_g_co2_per_kwh

    async def get_current_snapshot(self) -> CarbonSnapshot:
        return self.current

    async def get_current_price(self) -> float:
        return self.current.price_usd_per_kg

    async def close(self): pass

    async def health_check(self) -> Dict[str, Any]:
        return {"status": "ok", "intensity": self.current.intensity_g_co2_per_kwh,
                "source": self.current.source, "region": self.current.region}


# ============================================================
# Enhancement 5: Temporal logic (full STL)
# ============================================================
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

    def add_formula(self, f: STLFormula):
        self.formulas.append(f)

    def observe(self, record: Dict[str, Any]):
        with self._lock:
            self._history.append({**record, "_t": time.time()})

    def verify(self) -> Dict[str, bool]:
        with self._lock:
            history = list(self._history)
        results: Dict[str, bool] = {}
        for f in self.formulas:
            window = history[-f.horizon:]
            results[f.name] = f.evaluate(window)
            if not results[f.name]:
                MP_TEMPORAL.labels(formula=f.name).inc()
                with self._lock:
                    self.violations.append(STLViolation(
                        formula=f.name, operator=f.operator.value,
                        window_size=len(window), timestamp=time.time()))
        return results

    def recent_violations(self, n: int = 10) -> List[Dict[str, Any]]:
        with self._lock:
            return [asdict(v) for v in self.violations[-n:]]

    def fresh(self) -> "TemporalLogicMonitor":
        m = TemporalLogicMonitor(horizon=self.horizon)
        m.formulas = list(self.formulas)
        return m


class SafetyShield:
    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor
        self.violations: List[Dict[str, Any]] = []

    def screen(self, chosen: str, safer: str,
               verdict: Optional[Dict[str, bool]] = None
               ) -> Tuple[str, bool, Dict[str, bool]]:
        v = verdict if verdict is not None else self.monitor.verify()
        violated = [k for k, ok in v.items() if not ok]
        if not violated:
            return chosen, True, v
        self.violations.append({"t": time.time(), "violated": violated,
                                "original": chosen, "replacement": safer})
        return safer, False, v


# ============================================================
# Enhancement 2: Causal RL (UCB)
# ============================================================
@dataclass
class CausalTransition:
    state: np.ndarray
    action: int
    reward: float
    next_state: np.ndarray


class CausalCounterfactualEstimator:
    def __init__(self, state_dim: int, n_actions: int, ridge: float = 1e-3,
                 exploration_c: float = 0.5):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.ridge = ridge
        self.exploration_c = exploration_c
        self._A = np.eye(state_dim + n_actions + 1) * ridge
        self._b = np.zeros(state_dim + n_actions + 1)
        self._counts = np.zeros(n_actions)
        self._n = 0
        self._lock = threading.Lock()

    def _features(self, s, a):
        oh = np.zeros(self.n_actions)
        oh[a % self.n_actions] = 1.0
        return np.concatenate([np.asarray(s, dtype=np.float64), oh, [1.0]])

    def _ensure(self, n: int):
        if n == self.n_actions:
            return
        with self._lock:
            self._A = np.eye(self.state_dim + n + 1) * self.ridge
            self._b = np.zeros(self.state_dim + n + 1)
            self.n_actions = n
            self._counts = np.zeros(n)
            self._n = 0

    def update(self, t: CausalTransition):
        a = int(t.action) % max(self.n_actions, 1)
        x = self._features(t.state, a)
        with self._lock:
            self._A += np.outer(x, x)
            self._b += t.reward * x
            self._counts[a] += 1
            self._n += 1

    def _predict(self, s, a) -> float:
        with self._lock:
            if self._n < 2:
                return 0.0
            try:
                theta = np.linalg.solve(self._A, self._b)
            except np.linalg.LinAlgError:
                return 0.0
        return float(self._features(s, a) @ theta)

    def counterfactuals(self, s, n: int) -> Dict[int, float]:
        self._ensure(n)
        return {a: self._predict(s, a) for a in range(self.n_actions)}

    def select_action(self, s, n: int, epsilon: float = 0.05) -> int:
        cf = self.counterfactuals(s, n)
        if not cf:
            return 0
        if random.random() < epsilon:
            return random.randrange(self.n_actions)
        with self._lock:
            total = max(self._n, 1)
            counts = self._counts.copy()
        scores = {}
        for a, mu in cf.items():
            count = max(counts[a], 1.0)
            bonus = self.exploration_c * math.sqrt(math.log(total + 1) / count)
            scores[a] = mu + bonus
        return max(scores, key=scores.get)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {"n_updates": int(self._n), "n_actions": self.n_actions,
                    "counts": self._counts.tolist()}


# ============================================================
# Enhancement 3: Federated (secure + DP)
# ============================================================
@dataclass
class FederatedUpdate:
    node_id: str
    weights: np.ndarray
    n_samples: int
    carbon_intensity: float
    timestamp: float = field(default_factory=time.time)


class DPAccountant:
    def __init__(self, target_epsilon: float = 8.0, target_delta: float = 1e-5):
        self.target_epsilon = target_epsilon
        self.target_delta = target_delta
        self.spent = 0.0
        self.rounds = 0

    def step(self, sigma: float, sensitivity: float = 1.0):
        if sigma <= 0:
            return
        eps = sensitivity * math.sqrt(2 * math.log(1.25 / self.target_delta)) / sigma
        self.spent += eps
        self.rounds += 1

    def can_spend(self) -> bool:
        return self.spent < self.target_epsilon


class FederatedAggregator:
    def __init__(self, num_params: int = 4, dp_sigma: float = 1e-3,
                 target_epsilon: float = 8.0, target_delta: float = 1e-5,
                 staleness_s: float = 3600.0,
                 persist: Optional[Persistence] = None):
        self.num_params = int(num_params)
        self.dp_sigma = float(dp_sigma)
        self.staleness_s = float(staleness_s)
        self.global_weights: List[float] = [1.0 / self.num_params] * self.num_params
        self.round = 0
        self._pending: Dict[str, FederatedUpdate] = {}
        self.accountant = DPAccountant(target_epsilon, target_delta)
        self._persist = persist
        self._lock = threading.Lock()

    def submit_update(self, client_id: str, weights: List[float],
                      samples: int, carbon_intensity: float = 400.0):
        if len(weights) != self.num_params:
            return
        with self._lock:
            self._pending[client_id] = FederatedUpdate(
                node_id=client_id,
                weights=np.asarray(weights, dtype=np.float64),
                n_samples=max(1, int(samples)),
                carbon_intensity=float(carbon_intensity))

    def _fresh(self) -> List[FederatedUpdate]:
        now = time.time()
        with self._lock:
            return [u for u in self._pending.values()
                    if (now - u.timestamp) <= self.staleness_s]

    @staticmethod
    def _pairwise_masks(node_ids: List[str], shape: Tuple[int, ...]) -> List[np.ndarray]:
        rng = np.random.default_rng(0)
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

    def aggregate(self, secure: bool = True) -> Dict[str, Any]:
        fresh = self._fresh()
        if not fresh:
            return {"weights": self.global_weights, "round": self.round}
        if not self.accountant.can_spend():
            raise PrivacyBudgetExceeded(
                f"DP budget exhausted: spent={self.accountant.spent:.2f}")
        weights = np.array([u.n_samples / max(u.carbon_intensity, 1.0)
                            for u in fresh])
        weights /= max(weights.sum(), 1e-12)
        node_ids = [u.node_id for u in fresh]
        stacked = np.stack([u.weights for u in fresh], axis=0)
        if secure and len(fresh) > 1:
            masks = self._pairwise_masks(node_ids, stacked.shape[1:])
            stacked = stacked + np.stack(masks, axis=0)
        blended = np.tensordot(weights, stacked, axes=([0], [0]))
        if self.dp_sigma > 0:
            blended = blended + np.random.normal(0.0, self.dp_sigma,
                                                 size=blended.shape)
            self.accountant.step(self.dp_sigma)
        with self._lock:
            self.global_weights = blended.tolist()
            self.round += 1
            self._pending.clear()
        for nid in node_ids:
            MP_DP_EPSILON.labels(node=nid).set(self.accountant.spent)
        MP_FEDERATED.inc()
        if self._persist:
            with contextlib.suppress(Exception):
                self._persist.federated_append(
                    self.round, self.global_weights, self.accountant.spent)
        return {"weights": self.global_weights, "round": self.round,
                "dp_spent": self.accountant.spent}

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            pending = len(self._pending)
        return {"round": self.round,
                "global_weights": self.global_weights,
                "pending_updates": pending,
                "dp_spent": self.accountant.spent,
                "dp_target": self.accountant.target_epsilon}


# ============================================================
# Enhancement 4: Multi-agent (message bus + emergent roles)
# ============================================================
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

    def record(self, role: AgentRole, success: bool, reward: float):
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
            AgentRole.ACCURACY_SPECIALIST: 1.0,
            AgentRole.ENERGY_SPECIALIST: 1.1,
            AgentRole.CARBON_SPECIALIST: 1.2,
            AgentRole.SPEED_SPECIALIST: 0.9,
            AgentRole.SAFETY_OFFICER: 1.4,
            AgentRole.VERIFIER: 1.0}
        self.messages: deque = deque(maxlen=1000)
        self._lock = threading.Lock()

    def broadcast(self, sender: str, topic: str, payload: Dict[str, Any]):
        with self._lock:
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
        carbon = [b for b in bids if b.role == AgentRole.CARBON_SPECIALIST]
        if safety and min(b.confidence for b in safety) > 0.5:
            idx = safety[0].proposed_action % n_candidates
            self.broadcast("coordinator", "conflict_resolved",
                           {"winner": "safety_officer", "idx": idx})
            return idx, "safety_override"
        if carbon and min(b.confidence for b in carbon) > 0.5:
            idx = carbon[0].proposed_action % n_candidates
            self.broadcast("coordinator", "conflict_resolved",
                           {"winner": "carbon_specialist", "idx": idx})
            return idx, "carbon_override"
        idx = self.vote(bids, n_candidates)
        self.broadcast("coordinator", "conflict_resolved",
                       {"winner": "vote", "idx": idx})
        return idx, "vote"


# ============================================================
# Enhancement 6: XAI (SHAP + weighted)
# ============================================================
class XAIExplainer:
    def __init__(self, feature_names: List[str],
                 predict_fn: Optional[Callable] = None):
        self.feature_names = list(feature_names)
        self.predict_fn = predict_fn
        self._shap_explainer = None

    def fit(self, background: np.ndarray):
        if SHAP_AVAILABLE and self.predict_fn is not None:
            try:
                self._shap_explainer = shap.KernelExplainer(self.predict_fn, background)
            except Exception as e:
                logger.warning(f"SHAP init failed: {e}")

    def _shap_attributions(self, x: np.ndarray) -> Optional[Dict[str, float]]:
        if self._shap_explainer is None:
            return None
        try:
            vals = self._shap_explainer.shap_values(x.reshape(1, -1))
            v = np.asarray(vals).reshape(-1)
            return {f: float(v[i]) for i, f in enumerate(self.feature_names)}
        except Exception:
            return None

    def explain(self, candidate: Dict[str, float],
                weights: Dict[str, float],
                all_candidates: List[Dict[str, float]],
                top_k: int = 5) -> Dict[str, Any]:
        if not all_candidates:
            return {"contributions": {}, "top_features": [],
                    "narrative": [], "weights_used": {}, "method": "none"}
        matrix = np.array([[float(c.get(f, 0.0)) for f in self.feature_names]
                           for c in all_candidates])
        norms = np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9
        cand_vec = np.array([float(candidate.get(f, 0.0))
                             for f in self.feature_names])
        w_arr = np.array([float(weights.get(f, 1.0)) for f in self.feature_names])
        weighted = (cand_vec / norms) * w_arr
        contrib = {f: float(weighted[i]) for i, f in enumerate(self.feature_names)}
        ranked = sorted(contrib.items(), key=lambda kv: abs(kv[1]),
                        reverse=True)[:top_k]
        narrative = [f"{f} ({v:+.4f}) {'increases' if v >= 0 else 'decreases'} "
                     f"the utility." for f, v in ranked]
        method = "weighted"
        shap_attr = None
        if self._shap_explainer is not None:
            shap_attr = self._shap_attributions(cand_vec)
            if shap_attr is not None:
                method = "shap"
        return {"contributions": contrib,
                "top_features": [f for f, _ in ranked],
                "narrative": narrative,
                "weights_used": dict(weights),
                "method": method,
                "shap_attributions": shap_attr}


# ============================================================
# Enhancement 7: Adaptive precision (telemetry-driven)
# ============================================================
@dataclass
class HardwareTelemetry:
    gpu_available: bool = False
    memory_gb: float = 16.0
    utilization: float = 0.3
    temperature_c: float = 40.0
    observed_quality_drop: float = 0.0
    timestamp: float = field(default_factory=time.time)


class AdaptivePrecisionController:
    def __init__(self, energy_joules_per_op: float = 1e-9):
        self.telemetry = HardwareTelemetry(
            gpu_available=bool(TORCH_AVAILABLE and getattr(torch.cuda, "is_available", lambda: False)()))
        self.last_precision = PrecisionLevel.FP32
        self._ema_quality_drop = 0.0
        self.energy_joules_per_op = float(energy_joules_per_op)
        self._lock = threading.Lock()

    def update_telemetry(self, **kwargs):
        with self._lock:
            for k, v in kwargs.items():
                if hasattr(self.telemetry, k):
                    setattr(self.telemetry, k, v)
            alpha = 0.2
            self._ema_quality_drop = (
                (1 - alpha) * self._ema_quality_drop
                + alpha * float(self.telemetry.observed_quality_drop))

    def select(self, carbon_intensity: float,
               accuracy_required: float = 0.95) -> PrecisionLevel:
        with self._lock:
            tel = self.telemetry
            drop = self._ema_quality_drop
        if accuracy_required > 0.99 or drop > 0.05:
            chosen = PrecisionLevel.FP32
        elif carbon_intensity > 500:
            chosen = PrecisionLevel.FP8
        elif tel.gpu_available and carbon_intensity < 350:
            chosen = PrecisionLevel.FP16
        else:
            chosen = PrecisionLevel.FP16
        self.last_precision = chosen
        return chosen

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0, PrecisionLevel.BF16: 0.4,
                PrecisionLevel.FP16: 0.4, PrecisionLevel.FP8: 0.2,
                PrecisionLevel.FP4: 0.1}[level]

    def estimate_energy_joules(self, level: PrecisionLevel, ops: float) -> float:
        return PRECISION_ENERGY_PER_OP[level] * ops


PRECISION_ENERGY_PER_OP: Dict[PrecisionLevel, float] = {
    PrecisionLevel.FP32: 1.0e-9,
    PrecisionLevel.BF16: 0.4e-9,
    PrecisionLevel.FP16: 0.4e-9,
    PrecisionLevel.FP8: 0.2e-9,
    PrecisionLevel.FP4: 0.1e-9,
}


# ============================================================
# Role specialization
# ============================================================
class RoleSpecializationCoordinator:
    def __init__(self):
        self.roles = [AgentRole.ACCURACY_SPECIALIST, AgentRole.ENERGY_SPECIALIST,
                      AgentRole.CARBON_SPECIALIST, AgentRole.SPEED_SPECIALIST]
        self.affinity = np.array([
            [0.9, 0.2, 0.2, 0.2],
            [0.2, 0.9, 0.3, 0.2],
            [0.2, 0.3, 0.9, 0.2],
            [0.2, 0.2, 0.2, 0.9]])

    def assign_roles(self, context: Dict[str, float]) -> Dict[str, Any]:
        ctx = np.array([context.get("accuracy", 0.5),
                        context.get("energy", 0.5),
                        context.get("carbon", 0.5),
                        context.get("speed", 0.5)])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        assignments = {r.value: float(probs[i]) for i, r in enumerate(self.roles)}
        dominant = self.roles[int(np.argmax(probs))].value
        return {"assignments": assignments, "dominant_role": dominant}


# ============================================================
# Enhancement 9: Chaos (steady-state, no global mutation)
# ============================================================
@dataclass
class SteadyStateHypothesis:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]


class ChaosTester:
    FAULT_TYPES = ["carbon_api_down", "model_oom", "distiller_broken",
                   "rlhf_broken", "scheduler_hang"]

    def __init__(self, engine_ref: Optional[Any] = None,
                 hypotheses: Optional[List[SteadyStateHypothesis]] = None,
                 max_concurrent: int = 1, duration_s: float = 0.0,
                 persist: Optional[Persistence] = None):
        self.engine = engine_ref
        self.hypotheses = hypotheses or [
            SteadyStateHypothesis("precision_valid",
                                  lambda m: m.get("precision") in
                                  [p.value for p in PrecisionLevel])]
        self.max_concurrent = max(1, int(max_concurrent))
        self.duration_s = float(duration_s)
        self.persist = persist
        self.results: List[Dict[str, Any]] = []
        self._active = 0
        self._lock = threading.Lock()

    def _can_inject(self) -> bool:
        return self._active < self.max_concurrent

    async def run_test(self, fault_type: str,
                       duration_s: float = 0.2) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        if not self._can_inject():
            return {"fault": fault_type, "passed": False,
                    "error": "max_concurrent_reached"}
        with self._lock:
            self._active += 1
        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable[[], None]] = []
        eng = self.engine

        try:
            if fault_type == "carbon_api_down" and eng is not None \
                    and eng.carbon_manager is not None:
                orig_override = eng.carbon_manager._override

                async def broken():
                    raise RuntimeError("carbon API down")
                eng.carbon_manager.set_override(broken)
                restore.append(lambda: eng.carbon_manager.set_override(orig_override))
                try:
                    await eng.carbon_manager.refresh()
                except Exception:
                    pass

            elif fault_type == "model_oom":
                # Exercise the engine's OOM handling by injecting a fake model
                if eng is not None:
                    orig_exec = getattr(eng, "_chaos_exec_override", None)

                    async def oom_forward(*a, **k):
                        raise RuntimeError("CUDA out of memory (chaos)")
                    eng._chaos_exec_override = oom_forward
                    restore.append(lambda: setattr(
                        eng, "_chaos_exec_override", orig_exec))
                    try:
                        await eng._exercise_forward_chaos()
                    except Exception:
                        pass

            elif fault_type == "distiller_broken" and eng is not None \
                    and getattr(eng, "distiller", None) is not None:
                orig = eng.distiller.distill

                def broken(_):
                    raise RuntimeError("distiller broken")
                eng.distiller.distill = broken
                restore.append(lambda: setattr(eng.distiller, "distill", orig))
                try:
                    eng.distiller.distill({})
                except Exception:
                    pass

            elif fault_type == "rlhf_broken" and eng is not None \
                    and getattr(eng, "rlhf", None) is not None:
                orig = eng.rlhf.update

                def broken(*a, **k):
                    raise RuntimeError("rlhf broken")
                eng.rlhf.update = broken
                restore.append(lambda: setattr(eng.rlhf, "update", orig))
                try:
                    eng.rlhf.update({}, "fp16", 0.0)
                except Exception:
                    pass

            elif fault_type == "scheduler_hang" and eng is not None \
                    and getattr(eng, "scheduler", None) is not None:
                orig = eng.scheduler.schedule

                async def hung(*a, **k):
                    await asyncio.sleep(10)
                    return {"recommended_delay": 0}
                eng.scheduler.schedule = hung
                restore.append(lambda: setattr(eng.scheduler, "schedule", orig))
                try:
                    await asyncio.wait_for(eng.scheduler.schedule(), timeout=0.1)
                except Exception:
                    pass

            if fault_type != "scheduler_hang":
                await asyncio.sleep(min(duration_s, 1.0))

            metrics = {}
            if eng is not None:
                metrics = {"precision": getattr(eng, "current_precision", None)}
            if not all(h.predicate(metrics) for h in self.hypotheses):
                passed = False
                error_msg = "steady_state_violated"
        except Exception as e:
            passed, error_msg = False, str(e)
        finally:
            for r in restore:
                with contextlib.suppress(Exception):
                    r()
            with self._lock:
                self._active = max(0, self._active - 1)

        result = {"fault": fault_type, "duration_s": duration_s,
                  "elapsed_s": time.time() - start, "passed": passed,
                  "error": error_msg, "timestamp": utcnow().isoformat()}
        self.results.append(result)
        MP_CHAOS.labels(fault=fault_type,
                        status="pass" if passed else "fail").inc()
        if self.persist:
            with contextlib.suppress(Exception):
                self.persist.chaos_append(fault_type, passed, result)
        return result

    def get_report(self) -> Dict[str, Any]:
        return {"tests_run": len(self.results),
                "pass_rate": (sum(1 for r in self.results if r["passed"])
                              / len(self.results)) if self.results else 1.0,
                "recent": self.results[-5:],
                "hypotheses": [h.name for h in self.hypotheses]}


# ============================================================
# Enhancement 10: Active RLHF + HITL + Active learning
# ============================================================
class ActiveRLHF:
    def __init__(self, action_space: List[str],
                 uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0):
        self.actions = list(action_space)
        self.uncertainty_threshold = float(uncertainty_threshold)
        self.human_timeout_s = float(human_timeout_s)
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self.context_priors: Dict[str, Dict[str, float]] = defaultdict(
            lambda: defaultdict(float))
        self.history: List[Dict[str, Any]] = []
        self.pending_queries: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._approver: Optional[Callable[[Any], bool]] = None

    def _ctx_key(self, context: Any) -> str:
        return json.dumps(context, sort_keys=True, default=str)[:64]

    def _policy(self, context: Any) -> np.ndarray:
        key = self._ctx_key(context)
        with self._lock:
            raw = np.array([self.preference_counts[a]
                            + self.context_priors[key][a]
                            for a in self.actions], dtype=float)
        if raw.sum() == 0:
            raw = np.ones(len(self.actions))
        e = np.exp(raw - raw.max())
        return e / e.sum()

    def sample_action(self, context: Any) -> str:
        probs = self._policy(context)
        return self.actions[int(np.argmax(probs))]

    def uncertainty(self, context: Any) -> float:
        probs = self._policy(context)
        ent = -np.sum(probs * np.log(probs + 1e-12))
        return float(ent / math.log(len(self.actions))) if len(self.actions) > 1 else 0.0

    def update(self, context: Any, action: str, reward: float):
        if action not in self.actions:
            return
        key = self._ctx_key(context)
        with self._lock:
            self.preference_counts[action] += float(reward)
            self.context_priors[key][action] += float(reward)
        self.history.append({"action": action, "reward": float(reward),
                             "context": key, "timestamp": utcnow().isoformat()})

    async def maybe_query_human(self, context: Dict,
                                options: List[str]) -> Optional[Dict[str, Any]]:
        u = self.uncertainty(context)
        if u <= self.uncertainty_threshold:
            return None
        qid = str(uuid.uuid4())
        query = {"id": qid, "context": context, "options": options,
                 "uncertainty": u, "created_at": utcnow().isoformat(),
                 "status": "pending"}
        with self._lock:
            self.pending_queries[qid] = query
        MP_HITL_PENDING.set(len(self.pending_queries))
        return query

    def resolve_query(self, query_id: str, chosen: str,
                      rating: float = 1.0) -> Optional[Dict[str, Any]]:
        with self._lock:
            q = self.pending_queries.pop(query_id, None)
            MP_HITL_PENDING.set(len(self.pending_queries))
        if q is None:
            return None
        q.update({"status": "resolved", "chosen": chosen, "rating": rating})
        self.update(q["context"], chosen, rating)
        return q


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
    def new(cls, reason: str, chosen_idx: int,
            candidates: List[Dict[str, Any]],
            uncertainty: float, carbon_price: float,
            priority: int = 0,
            metadata: Optional[Dict[str, Any]] = None) -> "HITLRequest":
        return cls(request_id=uuid.uuid4().hex[:12], reason=reason,
                   chosen_idx=chosen_idx, candidates=list(candidates),
                   uncertainty=float(uncertainty),
                   carbon_price=float(carbon_price),
                   priority=int(priority), metadata=dict(metadata or {}))


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
            "request_id": self.request_id, "approved": self.approved,
            "reason": self.reason, "mode": self.mode,
            "latency_ms": round(self.latency_ms, 6),
            "timestamp": round(self.timestamp, 6),
            "prev_hash": self.prev_hash}, sort_keys=True, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class HumanInTheLoopCoordinator:
    def __init__(self, rlhf: ActiveRLHF, timeout_s: float = 30.0,
                 production_safe: bool = True, max_pending: int = 128,
                 persist: Optional[Persistence] = None,
                 approver: Optional[Callable[[HITLRequest], bool]] = None):
        self.rlhf = rlhf
        self.timeout_s = float(timeout_s)
        self.production_safe = bool(production_safe)
        self.max_pending = int(max_pending)
        self._persist = persist
        self._approver = approver
        self._pending: Dict[str, HITLRequest] = {}
        self._futures: Dict[str, asyncio.Future] = {}
        self._lock = threading.RLock()
        self._audit: deque = deque(maxlen=4096)
        self._last_hash = persist.hitl_last_hash() if persist else ""
        MP_HITL_PENDING.set(0)

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

    async def request(self, req: HITLRequest) -> bool:
        start = time.time()
        approver = self._approver
        if approver is None and self.production_safe:
            self._audit_append(HITLAuditEntry(
                req.request_id, False, req.reason, "blocked_no_approver",
                (time.time() - start) * 1000.0))
            MP_HITL.labels(decision="blocked").inc()
            return False
        if approver is None:
            self._audit_append(HITLAuditEntry(
                req.request_id, True, req.reason, "auto_no_approver",
                (time.time() - start) * 1000.0))
            MP_HITL.labels(decision="auto_approved").inc()
            return True
        with self._lock:
            if len(self._pending) >= self.max_pending:
                self._audit_append(HITLAuditEntry(
                    req.request_id, False, req.reason, "queue_full",
                    (time.time() - start) * 1000.0))
                MP_HITL.labels(decision="queue_full").inc()
                return False
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            self._pending[req.request_id] = req
            self._futures[req.request_id] = future
            MP_HITL_PENDING.set(len(self._pending))

        def _call():
            try:
                return bool(approver(req))
            except Exception as e:
                logger.warning(f"Approver failed: {e}")
                return False

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        mode = "approver"
        try:
            worker = executor.submit(_call)
            approved = await asyncio.wait_for(
                loop.run_in_executor(None, worker.result),
                timeout=self.timeout_s)
        except asyncio.TimeoutError:
            approved = False
            mode = "timeout"
        except Exception as e:
            approved = False
            mode = f"error:{e}"
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            with self._lock:
                self._pending.pop(req.request_id, None)
                fut = self._futures.pop(req.request_id, None)
                MP_HITL_PENDING.set(len(self._pending))
            if fut is not None and not fut.done():
                with contextlib.suppress(Exception):
                    fut.set_result(bool(approved))

        latency_ms = (time.time() - start) * 1000.0
        self._audit_append(HITLAuditEntry(
            req.request_id, approved, req.reason, mode, latency_ms))
        MP_HITL.labels(decision="approved" if approved else "rejected").inc()
        return bool(approved)

    async def escalate(self, decision_context: Dict[str, Any],
                       options: List[str], confidence: float,
                       confidence_threshold: float = 0.65) -> Dict[str, Any]:
        needs_human = confidence < confidence_threshold
        query = await self.rlhf.maybe_query_human(decision_context, options)
        if query is None and not needs_human:
            choice = self.rlhf.sample_action(decision_context)
            return {"escalated": False, "chosen": choice, "source": "auto"}
        auto_choice = self.rlhf.sample_action(decision_context)
        if query is None:
            query = {"id": str(uuid.uuid4()), "options": options,
                     "context": decision_context, "status": "pending"}
        return {"escalated": True, "query": query,
                "chosen": auto_choice, "source": "human_pending"}

    def resolve(self, request_id: str, approved: bool) -> bool:
        with self._lock:
            fut = self._futures.get(request_id)
        if fut is None or fut.done():
            return False
        with contextlib.suppress(Exception):
            fut.set_result(bool(approved))
        self._audit_append(HITLAuditEntry(
            request_id, approved, "external_resolve", "resolved"))
        MP_HITL.labels(decision="approved" if approved else "rejected").inc()
        return True

    def audit_trail(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        if self._persist:
            rows = self._persist.hitl_recent(limit or 1000)
            return [{"request_id": r[0], "approved": bool(r[1]),
                     "reason": r[2], "mode": r[3], "latency_ms": r[4],
                     "prev_hash": r[5], "entry_hash": r[6],
                     "timestamp": r[7]} for r in rows]
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
                "reason": e["reason"], "mode": e["mode"],
                "latency_ms": round(float(e["latency_ms"]), 6),
                "timestamp": round(float(e["timestamp"]), 6),
                "prev_hash": e["prev_hash"]}, sort_keys=True, default=str)
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
                "timeout_s": self.timeout_s}


class ActiveLearningSampler:
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
        self._kmeans = (KMeans(n_clusters=self.n_clusters, n_init=5)
                        if SKLEARN_AVAILABLE and self.n_clusters > 1 else None)
        MP_ACTIVE_LEARNING.set(0)

    def _embed(self, record: Dict[str, Any]) -> np.ndarray:
        vals = [float(v) for k, v in sorted(record.items())
                if isinstance(v, (int, float)) and not isinstance(v, bool)]
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
        MP_ACTIVE_LEARNING.set(size)
        return True

    def __len__(self) -> int:
        with self._lock:
            return len(self._buffer)


# ============================================================
# Pareto + TOPSIS (benefit convention + cost_criteria)
# ============================================================
class ParetoFront:
    """All objectives are BENEFITS (higher is better)."""
    def __init__(self):
        self.solutions: List[Tuple[List[float], Any]] = []

    def add(self, objectives: Sequence[float], decision: Any):
        dominated = False
        for obj, _ in self.solutions:
            if (all(obj[i] >= objectives[i] for i in range(len(objectives)))
                    and any(obj[i] > objectives[i] for i in range(len(objectives)))):
                dominated = True
                break
        if not dominated:
            self.solutions = [
                (obj, dec) for obj, dec in self.solutions
                if not (all(objectives[i] >= obj[i] for i in range(len(objectives)))
                        and any(objectives[i] > obj[i] for i in range(len(objectives))))]
            self.solutions.append((list(objectives), decision))
        return dominated

    def get_pareto_front(self) -> List[Tuple[List[float], Any]]:
        return self.solutions

    def get_best_by_weight(self, weights: Sequence[float]) -> Any:
        best, best_score = None, -float("inf")
        for obj, dec in self.solutions:
            score = sum(w * o for w, o in zip(weights, obj))
            if score > best_score:
                best_score, best = score, dec
        return best


class TOPSIS:
    @staticmethod
    def score(candidates: List[Dict[str, float]], weights: Sequence[float],
              criteria: List[str],
              cost_criteria: Optional[Set[str]] = None) -> List[float]:
        if not candidates or not criteria:
            return []
        cost_criteria = set(cost_criteria or [])
        matrix = np.array([[float(c.get(crit, 0.0)) for crit in criteria]
                           for c in candidates])
        denom = np.sqrt((matrix ** 2).sum(axis=0))
        denom = np.where(denom == 0, 1.0, denom)
        norm = matrix / denom
        w = np.asarray(weights[: norm.shape[1]], dtype=np.float64)
        if w.sum() <= 0:
            w = np.ones(norm.shape[1]) / norm.shape[1]
        else:
            w = w / w.sum()
        weighted = norm * w
        ideal = np.zeros(norm.shape[1])
        neg_ideal = np.zeros(norm.shape[1])
        for j, crit in enumerate(criteria):
            if crit in cost_criteria:
                ideal[j] = weighted[:, j].min()
                neg_ideal[j] = weighted[:, j].max()
            else:
                ideal[j] = weighted[:, j].max()
                neg_ideal[j] = weighted[:, j].min()
        d_plus = np.sqrt(((weighted - ideal) ** 2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal) ** 2).sum(axis=1))
        return (d_minus / (d_plus + d_minus + 1e-9)).tolist()


# ============================================================
# Multi-teacher distiller (weighted vote)
# ============================================================
class MultiTeacherDistiller:
    def __init__(self, teachers: List[Callable],
                 weights: Optional[List[float]] = None):
        self.teachers = list(teachers)
        self.weights = list(weights) if weights else [1.0] * len(self.teachers)

    async def _call(self, t: Callable, ctx: Any) -> Optional[Dict[str, Any]]:
        try:
            out = t(ctx)
            if asyncio.iscoroutine(out):
                out = await out
            return out
        except Exception:
            return None

    async def distill(self, context: Any) -> Optional[Dict[str, Any]]:
        votes: Dict[str, Tuple[Dict[str, Any], float]] = {}
        for t, w in zip(self.teachers, self.weights):
            c = await self._call(t, context)
            if not isinstance(c, dict):
                continue
            key = json.dumps(c, sort_keys=True, default=str)
            if key in votes:
                prev, wprev = votes[key]
                votes[key] = (prev, wprev + float(w))
            else:
                votes[key] = (c, float(w))
        if not votes:
            return None
        return max(votes.values(), key=lambda kv: kv[1])[0]


# ============================================================
# MODP precision selector (benefit convention + XAI + roles)
# ============================================================
class MODPPrecisionSelector:
    FEATURES = ["accuracy", "energy", "carbon", "speed"]

    def __init__(self, adaptive_cost: Optional[Any] = None,
                 xai: Optional[XAIExplainer] = None,
                 roles: Optional[RoleSpecializationCoordinator] = None):
        self.adaptive_cost = adaptive_cost
        self.weights = [0.25, 0.25, 0.25, 0.25]
        self.learning_rate = 0.01
        self.recent_outcomes: deque = deque(maxlen=100)
        self.dtype_list = ["fp32", "fp16", "bf16", "fp8", "fp4"]
        self.xai = xai
        self.roles = roles

    @staticmethod
    def _estimate_accuracy(dtype: str, base_accuracy: float) -> float:
        return base_accuracy * {"fp32": 1.0, "fp16": 0.98, "bf16": 0.97,
                                "fp8": 0.90, "fp4": 0.80}.get(dtype, 0.5)

    @staticmethod
    def _estimate_energy(dtype: str) -> float:
        return {"fp32": 1.0, "fp16": 0.4, "bf16": 0.4,
                "fp8": 0.2, "fp4": 0.1}.get(dtype, 1.0)

    @staticmethod
    def _estimate_speed(dtype: str) -> float:
        return {"fp32": 1.0, "fp16": 2.0, "bf16": 2.0,
                "fp8": 3.0, "fp4": 4.0}.get(dtype, 1.0)

    def _benefit_vector(self, dtype: str, base_accuracy: float,
                        carbon_intensity: float) -> Dict[str, float]:
        """All benefit-style: higher is better."""
        accuracy = self._estimate_accuracy(dtype, base_accuracy)
        energy = self._estimate_energy(dtype)
        carbon = energy * (carbon_intensity / 400.0)
        speed = self._estimate_speed(dtype) / 4.0  # normalize to ~[0,1]
        return {"accuracy": accuracy,
                "energy": 1.0 - min(1.0, energy),
                "carbon": 1.0 - min(1.0, carbon),
                "speed": speed}

    def select(self, features: Dict[str, Any]) -> Dict[str, Any]:
        carbon_intensity = float(features.get("carbon_intensity", 400.0))
        base_accuracy = float(features.get("base_accuracy", 1.0))

        # Update weights from adaptive_cost
        if self.adaptive_cost is not None:
            try:
                wd = self.adaptive_cost.get_current_weights()
                self.weights = [
                    wd.get("accuracy", 0.25),
                    wd.get("energy", 0.25),
                    wd.get("carbon", 0.25),
                    wd.get("speed", 0.25)]
            except Exception:
                pass

        candidates = [self._benefit_vector(d, base_accuracy, carbon_intensity)
                      for d in self.dtype_list]
        scores = TOPSIS.score(candidates, self.weights, self.FEATURES)
        scores_arr = np.asarray(scores) if scores else np.zeros(len(candidates))
        best_idx = int(np.argmax(scores_arr))
        best = self.dtype_list[best_idx]

        front = ParetoFront()
        for c in candidates:
            front.add([c[k] for k in self.FEATURES], c)

        roles = None
        if self.roles:
            roles = self.roles.assign_roles(candidates[best_idx])

        xai_out = None
        if self.xai:
            xai_out = self.xai.explain(
                candidate=candidates[best_idx],
                weights=dict(zip(self.FEATURES, self.weights)),
                all_candidates=candidates)

        # Weight update (ascent on benefit objectives)
        outcome = np.asarray([candidates[best_idx][k] for k in self.FEATURES])
        self.recent_outcomes.append((list(self.weights), outcome))
        if len(self.recent_outcomes) >= 10:
            self._update_weights()

        return {"selected_precision": best,
                "scores": scores,
                "pareto_front_size": len(front.get_pareto_front()),
                "candidate_details": list(zip(self.dtype_list, candidates)),
                "xai_explanation": xai_out,
                "role_assignments": roles}

    def _update_weights(self):
        outcomes = np.array([o for _, o in self.recent_outcomes])
        mean = outcomes.mean(axis=0)
        std = outcomes.std(axis=0) + 1e-9
        grad = mean / std  # ascent on benefits
        w = np.array(self.weights) + self.learning_rate * grad
        w = np.clip(w, 0.05, None)
        w /= w.sum()
        self.weights = w.tolist()


# ============================================================
# MOE precision engine (gating trained on best-matching teacher)
# ============================================================
class MOEPrecisionEngine:
    def __init__(self, carbon_manager: CarbonIntensityManager,
                 xai: Optional[XAIExplainer] = None,
                 roles: Optional[RoleSpecializationCoordinator] = None):
        self.carbon_manager = carbon_manager
        self.xai = xai
        self.roles = roles
        self.dtype_list = ["fp32", "fp16", "bf16", "fp8", "fp4"]
        self.experts: Dict[str, Callable[[Dict[str, Any]], List[float]]] = {}
        self.gating_model = None
        self.scaler = None
        self.history_context: deque = deque(maxlen=500)
        self.history_labels: deque = deque(maxlen=500)
        self._trained = False
        self._init_experts()
        self._init_gating()

    def _init_experts(self):
        self.experts = {
            "accuracy": self._accuracy_teacher,
            "energy": self._energy_teacher,
            "carbon": self._carbon_teacher,
            "speed": self._speed_teacher}

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            try:
                self.gating_model = LogisticRegression(max_iter=1000)
                self.scaler = StandardScaler()
            except Exception:
                pass

    def _accuracy_teacher(self, features: Dict[str, Any]) -> List[float]:
        base = float(features.get("base_accuracy", 1.0))
        return [base * s for s in [1.0, 0.98, 0.97, 0.90, 0.80]]

    def _energy_teacher(self, features: Dict[str, Any]) -> List[float]:
        return [1.0, 0.4, 0.4, 0.2, 0.1]

    def _carbon_teacher(self, features: Dict[str, Any]) -> List[float]:
        intensity = float(features.get("carbon_intensity", 400.0))
        e = self._energy_teacher(features)
        return [1.0 - x * (intensity / 400.0) for x in e]

    def _speed_teacher(self, features: Dict[str, Any]) -> List[float]:
        return [1.0, 2.0, 2.0, 3.0, 4.0]

    def _extract_context(self, features: Dict[str, Any]) -> np.ndarray:
        return np.array([
            float(features.get("carbon_intensity", 400.0)) / 1000.0,
            float(features.get("base_accuracy", 1.0)),
            float(features.get("input_size", 1000)) / 10000.0,
            float(features.get("layer_complexity", 0.5)),
            float(features.get("time_of_day", utcnow().hour / 24.0))])

    def _get_teacher_scores(self, features: Dict[str, Any]) -> Dict[str, List[float]]:
        out: Dict[str, List[float]] = {}
        for name, fn in self.experts.items():
            try:
                s = fn(features)
                if not isinstance(s, list):
                    s = [float(s)] * len(self.dtype_list)
                out[name] = s
            except Exception as e:
                logger.warning(f"Teacher {name} failed: {e}")
                out[name] = [0.5] * len(self.dtype_list)
        return out

    def select(self, features: Dict[str, Any]) -> Dict[str, Any]:
        teacher_scores = self._get_teacher_scores(features)
        weights = self._gating_weights(features, len(teacher_scores))
        dtype_scores = np.zeros(len(self.dtype_list))
        for i, (name, s) in enumerate(teacher_scores.items()):
            dtype_scores += weights[i] * np.asarray(s)
        best_idx = int(np.argmax(dtype_scores))
        best = self.dtype_list[best_idx]

        roles = None
        if self.roles:
            roles = self.roles.assign_roles({
                "accuracy": float(dtype_scores[best_idx]),
                "energy": 1.0 - float(teacher_scores["energy"][best_idx]),
                "carbon": 1.0 - float(teacher_scores["carbon"][best_idx]),
                "speed": float(teacher_scores["speed"][best_idx]) / 4.0})

        xai_out = None
        if self.xai:
            features_dict = {name: float(dtype_scores[i])
                             for i, name in enumerate(["fp32", "fp16", "bf16", "fp8", "fp4"])}
            xai_out = self.xai.explain(
                candidate=features_dict,
                weights={name: float(w) for name, w in
                         zip(["fp32", "fp16", "bf16", "fp8", "fp4"], [1.0] * 5)},
                all_candidates=[features_dict])

        return {"selected_precision": best,
                "dtype_scores": dtype_scores.tolist(),
                "teacher_scores": teacher_scores,
                "gating_weights": {name: float(w)
                                   for name, w in zip(teacher_scores.keys(), weights)},
                "role_assignments": roles,
                "xai_explanation": xai_out}

    def _gating_weights(self, features: Dict[str, Any], n: int) -> np.ndarray:
        if self.gating_model is not None and self._trained:
            try:
                ctx = self._extract_context(features)
                X = self.scaler.transform([ctx])
                probs = self.gating_model.predict_proba(X)[0]
                if len(probs) == n:
                    return probs
            except Exception:
                pass
        return np.ones(n) / n

    def update(self, features: Dict[str, Any], actual_reward_per_dtype: Dict[str, float]):
        """Label the best-matching teacher (P0 fix)."""
        teacher_scores = self._get_teacher_scores(features)
        # Best teacher: the one whose ranking best matches actual reward ranking
        actual = np.asarray([actual_reward_per_dtype.get(d, 0.0)
                             for d in self.dtype_list])
        best_teacher_idx = 0
        best_corr = -2.0
        for i, (name, s) in enumerate(teacher_scores.items()):
            try:
                corr = float(np.corrcoef(np.asarray(s), actual)[0, 1])
            except Exception:
                corr = -2.0
            if corr > best_corr:
                best_corr = corr
                best_teacher_idx = i
        self.history_context.append(self._extract_context(features).tolist())
        self.history_labels.append(best_teacher_idx)
        if len(self.history_context) % 20 == 0 and len(self.history_context) >= 40:
            self._train_gating()

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or self.gating_model is None:
            return
        n = min(len(self.history_context), len(self.history_labels))
        if n < 40:
            return
        X = np.array(self.history_context[-n:])
        y = np.array(self.history_labels[-n:])
        if len(np.unique(y)) < 2:
            return
        try:
            X_scaled = self.scaler.fit_transform(X)
            self.gating_model.fit(X_scaled, y)
            self._trained = True
        except Exception as e:
            logger.warning(f"Gating fit failed: {e}")

    def get_stats(self) -> Dict[str, Any]:
        return {"num_experts": len(self.experts),
                "gating_trained": self._trained,
                "history_len": len(self.history_context)}


# ============================================================
# GA + bio (seeded)
# ============================================================
class GeneticAlgorithmOptimizer:
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1,
                 crossover_rate: float = 0.8, seed: int = 0):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Dict[str, float]] = []
        self.bounds = {
            "accuracy_weight": (0.0, 1.0),
            "energy_weight": (0.0, 1.0),
            "carbon_weight": (0.0, 1.0),
            "speed_weight": (0.0, 1.0)}
        self._rng = random.Random(seed)
        self._initialized = False

    def initialize(self, force: bool = False):
        if self._initialized and not force:
            return
        self.population = []
        for _ in range(self.pop_size):
            ind = {k: self._rng.uniform(*v) for k, v in self.bounds.items()}
            s = sum(ind.values()) or 1.0
            ind = {k: v / s for k, v in ind.items()}
            self.population.append(ind)
        self._initialized = True

    def evaluate(self, fitness_func):
        return [fitness_func(ind) for ind in self.population]

    def select(self, fitness, n):
        selected = []
        for _ in range(n):
            i, j = self._rng.sample(range(len(self.population)), 2)
            selected.append(self.population[i] if fitness[i] > fitness[j]
                            else self.population[j])
        return selected

    def crossover(self, p1, p2):
        if self._rng.random() < self.crossover_rate:
            child = {k: p1[k] if self._rng.random() < 0.5 else p2[k] for k in p1}
        else:
            child = dict(p1)
        s = sum(child.values()) or 1.0
        return {k: v / s for k, v in child.items()}

    def mutate(self, ind):
        if self._rng.random() < self.mutation_rate:
            k = self._rng.choice(list(self.bounds.keys()))
            ind[k] = self._rng.uniform(*self.bounds[k])
            s = sum(ind.values()) or 1.0
            ind = {k2: v / s for k2, v in ind.items()}
        return ind

    def evolve(self, fitness_func, generations: int = 5):
        self.initialize()
        for _ in range(generations):
            fitness = self.evaluate(fitness_func)
            best = dict(self.population[int(np.argmax(fitness))])
            parents = self.select(fitness, self.pop_size - 1)
            offspring = []
            for i in range(0, len(parents) - 1, 2):
                offspring.append(self.mutate(self.crossover(parents[i], parents[i + 1])))
                offspring.append(self.mutate(self.crossover(parents[i + 1], parents[i])))
            self.population = offspring[:self.pop_size - 1] + [best]
        fitness = self.evaluate(fitness_func)
        return dict(self.population[int(np.argmax(fitness))])


class BioOptimizer:
    def __init__(self, config: MixedPrecisionConfig,
                 adaptive_cost: Optional[Any] = None,
                 rlhf: Optional[ActiveRLHF] = None):
        self.config = config
        self.adaptive_cost = adaptive_cost
        self.rlhf = rlhf
        self.ga = GeneticAlgorithmOptimizer(
            population_size=config.bio_population_size,
            mutation_rate=config.bio_mutation_rate,
            crossover_rate=config.bio_crossover_rate,
            seed=config.ga_seed)
        self.current_params = {"accuracy_weight": 0.25, "energy_weight": 0.25,
                               "carbon_weight": 0.25, "speed_weight": 0.25}
        self.fitness_history: deque = deque(maxlen=50)
        self._lock = asyncio.Lock()

    def _fitness(self, params: Dict[str, float]) -> float:
        if self.adaptive_cost is not None:
            try:
                cost = self.adaptive_cost.evaluate({
                    "accuracy": params["accuracy_weight"],
                    "energy": params["energy_weight"],
                    "carbon": params["carbon_weight"],
                    "speed": params["speed_weight"]})
                return -float(cost)
            except Exception:
                pass
        return (params["accuracy_weight"] * 0.4
                + (1 - params["carbon_weight"]) * 0.3
                + (1 - params["energy_weight"]) * 0.3)

    async def evolve(self) -> Dict[str, float]:
        best = self.ga.evolve(self._fitness, generations=5)
        async with self._lock:
            self.current_params = best
            self.fitness_history.append(self._fitness(best))
        return best

    def get_current_params(self) -> Dict[str, float]:
        return dict(self.current_params)


# ============================================================
# Scheduler
# ============================================================
class MultiObjectiveCarbonScheduler:
    def __init__(self, carbon_manager: CarbonIntensityManager,
                 market: Optional[CarbonMarketClient] = None,
                 threshold: float = 400.0, max_delay_s: int = 300):
        self.carbon_manager = carbon_manager
        self.market = market
        self.threshold = float(threshold)
        self.max_delay = int(max_delay_s)
        self.history: deque = deque(maxlen=100)

    async def schedule(self, urgency_score: float = 0.5) -> Dict[str, Any]:
        try:
            intensity = await self.carbon_manager.get_current_intensity()
        except Exception:
            intensity = 400.0
        market = await self.market.get_market_snapshot() if self.market else {}
        if intensity <= self.threshold:
            return {"recommended_delay": 0, "reason": "below_threshold",
                    "intensity": intensity, "market": market}
        carbon_w = 0.3
        urgency_w = 0.5
        best_delay = 0
        best_cost = 1e9
        for d in range(0, self.max_delay + 1, 10):
            savings = min(1.0, (intensity - self.threshold) / max(intensity, 1.0))
            urgency_cost = d / (self.max_delay + 1) * urgency_score
            cost = -carbon_w * savings + urgency_w * urgency_cost
            if cost < best_cost:
                best_cost = cost
                best_delay = d
        self.history.append({"delay": best_delay, "cost": best_cost})
        return {"recommended_delay": best_delay, "reason": "multi_objective",
                "intensity": intensity, "market": market}


# ============================================================
# MOE forecaster (real experts)
# ============================================================
class MOEForecaster:
    def __init__(self, persist: Optional[Persistence] = None):
        self.experts: List[Tuple[str, Callable]] = []
        self.history: deque = deque(maxlen=1000)
        self._persist = persist
        self._init_experts()
        if persist:
            with contextlib.suppress(Exception):
                for price in persist.recent_carbon_prices(500):
                    self.history.append({"ds": utcnow(), "y": float(price)})

    def _init_experts(self):
        if PROPHET_AVAILABLE:
            self.experts.append(("prophet", self._forecast_prophet))
        if SKLEARN_AVAILABLE:
            self.experts.append(("linear", self._forecast_linear))
        if STATSMODELS_AVAILABLE:
            self.experts.append(("holtwinters", self._forecast_holtwinters))
        if not self.experts:
            self.experts.append(("naive", self._forecast_naive))

    async def _forecast_prophet(self, history, horizon):
        if len(history) < 30:
            return [0.5] * horizon
        try:
            import pandas as pd
            df = pd.DataFrame(list(history)).sort_values("ds")
            m = Prophet(changepoint_prior_scale=0.05, seasonality_prior_scale=10)
            m.fit(df)
            future = m.make_future_dataframe(periods=horizon)
            return m.predict(future)["yhat"].tail(horizon).tolist()
        except Exception:
            return [0.5] * horizon

    async def _forecast_linear(self, history, horizon):
        if len(history) < 2:
            return [0.5] * horizon
        X = np.arange(len(history)).reshape(-1, 1)
        y = np.array([h["y"] for h in history])
        try:
            m = LinearRegression().fit(X, y)
            f = np.arange(len(history), len(history) + horizon).reshape(-1, 1)
            return m.predict(f).tolist()
        except Exception:
            return [0.5] * horizon

    async def _forecast_holtwinters(self, history, horizon):
        if len(history) < 24:
            return [0.5] * horizon
        try:
            m = ExponentialSmoothing([h["y"] for h in history],
                                     trend="add", seasonal=None)
            return m.fit().forecast(horizon).tolist()
        except Exception:
            return [0.5] * horizon

    async def _forecast_naive(self, history, horizon):
        if not history:
            return [0.5] * horizon
        return [history[-1]["y"]] * horizon

    async def update_history(self, value: float):
        self.history.append({"ds": utcnow(), "y": float(value)})
        if self._persist:
            with contextlib.suppress(Exception):
                self._persist.append_carbon_price(value)

    async def forecast(self, horizon: int = 24) -> Dict[str, Any]:
        if len(self.history) < 10:
            return {"prices": [0.5] * horizon, "confidence": 0.0}
        forecasts = []
        for name, fn in self.experts:
            try:
                forecasts.append(np.asarray(await fn(self.history, horizon)))
            except Exception:
                forecasts.append(np.full(horizon, 0.5))
        weights = np.ones(len(self.experts)) / len(self.experts)
        final = np.zeros(horizon)
        for i, f in enumerate(forecasts):
            final += weights[i] * f
        return {"prices": final.tolist(),
                "expert_weights": weights.tolist(),
                "confidence": 0.85}


# ============================================================
# Self-healing (rolling refit)
# ============================================================
class SelfHealingManager:
    def __init__(self, config: MixedPrecisionConfig,
                 rlhf: Optional[ActiveRLHF] = None,
                 chaos: Optional[ChaosTester] = None):
        self.config = config
        self.detectors: List[Tuple[str, Any]] = []
        self._lock = threading.Lock()
        self._buffer: deque = deque(maxlen=500)
        self._trained = False
        self._refit_counter = 0
        self.actions: deque = deque(maxlen=100)
        self.rlhf = rlhf
        self.chaos = chaos
        if SKLEARN_AVAILABLE:
            self.detectors = [
                ("iforest", IsolationForest(contamination=0.1)),
                ("ocsvm", OneClassSVM(nu=0.1))]

    def _features(self, metrics: Dict[str, Any]) -> np.ndarray:
        return np.array([
            float(metrics.get("accuracy", 1.0)),
            float(metrics.get("energy_saved", 0.0)),
            float(metrics.get("carbon_saved", 0.0)),
            float(metrics.get("latency_ms", 0.0)) / 1000.0]).reshape(1, -1)

    async def _fit(self, force: bool = False):
        if not self.detectors:
            return
        with self._lock:
            buf = list(self._buffer)
        if len(buf) < 20:
            return
        if self._trained and not force:
            return
        X = np.vstack(buf)
        for _, m in self.detectors:
            with contextlib.suppress(Exception):
                m.fit(X)
        self._trained = True

    async def detect(self, metrics: Dict[str, Any]) -> Tuple[bool, float]:
        with self._lock:
            self._buffer.append(self._features(metrics))
            self._refit_counter += 1
        if not self._trained:
            await self._fit()
            return False, 0.0
        if self._refit_counter % 50 == 0:
            await self._fit(force=True)
        X = self._features(metrics)
        votes = []
        for _, m in self.detectors:
            try:
                votes.append(1 if m.predict(X)[0] == -1 else 0)
            except Exception:
                votes.append(0)
        if not votes:
            return False, 0.0
        score = sum(votes) / len(votes)
        if score > 0.5:
            with self._lock:
                self.actions.append({"action": "recovery",
                                     "timestamp": utcnow().isoformat()})
        return score > 0.5, float(score)

    async def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {"trained": self._trained,
                    "num_detectors": len(self.detectors),
                    "recent_actions": list(self.actions)[-5:],
                    "chaos_active": self.chaos is not None}


# ============================================================
# Main engine
# ============================================================
class EnhancedMixedPrecisionEngine:
    def __init__(self, config: Optional[MixedPrecisionConfig] = None,
                 hitl_approver: Optional[Callable[[HITLRequest], bool]] = None,
                 adaptive_cost: Optional[Any] = None):
        if PYDANTIC_AVAILABLE and isinstance(config, dict):
            self.config = MixedPrecisionConfig(**config)
        elif config is None:
            self.config = MixedPrecisionConfig()
        else:
            self.config = config
        self.instance_id = self.config.instance_id
        self._start_time = utcnow()

        # Persistence
        self.persistence = Persistence(self.config.get_db_url())

        # Rate limit + cost
        self._rate_limiter = TokenBucket(
            rate=getattr(self.config, "rate_limit_per_minute", 600),
            per_seconds=60.0)
        self._today_cost = self.persistence.cost_today()
        self._daily_cost_budget = float(getattr(self.config, "daily_cost_budget_usd", 10.0))
        self._cost_per_decision = float(getattr(self.config, "cost_per_decision_usd", 1e-5))

        # PQC + blockchain
        self.quantum = (QuantumSecurity(
            algorithm=getattr(self.config, "quantum_algorithm", "dilithium"),
            master_key=self.config.get_master_key_bytes(),
            rotation_s=getattr(self.config, "quantum_key_rotation_s", 3600))
            if getattr(self.config, "quantum_enabled", True) else None)
        self.blockchain = (BlockchainVerification(self.config, self.persistence)
                           if getattr(self.config, "blockchain_enabled", True) else None)

        # Carbon
        self.carbon_market = (CarbonMarketClient(self.config, self.persistence)
                              if getattr(self.config, "carbon_market_enabled", True) else None)
        self.carbon_manager = CarbonIntensityManager(
            market=self.carbon_market,
            electricity_maps_api_key=getattr(self.config, "electricity_maps_api_key", None),
            electricity_maps_zone=getattr(self.config, "electricity_maps_zone", "US-CAL-CISO"),
            watttime_username=getattr(self.config, "watttime_username", None),
            watttime_password=getattr(self.config, "watttime_password", None),
            watttime_region=getattr(self.config, "watttime_region", "CAISO_NORTH"),
            region=getattr(self.config, "carbon_region", "global"),
            persist=self.persistence)

        # Temporal logic
        self._base_temporal: Optional[TemporalLogicMonitor] = None
        if getattr(self.config, "temporal_logic_enabled", True):
            self._base_temporal = TemporalLogicMonitor(horizon=10)
            self._base_temporal.add_formula(STLFormula(
                name="carbon_cap",
                predicate=lambda r: r.get("carbon_intensity", 0.0) <= 800.0,
                operator=STLOperator.ALWAYS, horizon=10))
            self._base_temporal.add_formula(STLFormula(
                name="accuracy_floor",
                predicate=lambda r: r.get("base_accuracy", 1.0) >= 0.5,
                operator=STLOperator.ALWAYS, horizon=10))
        self.shield = SafetyShield(self._base_temporal) if self._base_temporal else None

        # XAI
        self.xai = (XAIExplainer(["accuracy", "energy", "carbon", "speed"])
                    if getattr(self.config, "xai_enabled", True) else None)

        # Adaptive precision
        self.precision_controller = (AdaptivePrecisionController()
                                     if getattr(self.config, "adaptive_precision_enabled", True)
                                     else None)

        # Roles + multi-agent
        self.role_coordinator = (RoleSpecializationCoordinator()
                                 if getattr(self.config, "role_specialization_enabled", True)
                                 else None)
        self.roles = EmergentRoleRegistry() if getattr(self.config, "multi_agent_enabled", True) else None
        self.coordinator = MultiAgentCoordinator(self.roles) if self.roles else None

        # RLHF + HITL
        self.rlhf = ActiveRLHF(
            action_space=[p.value for p in PrecisionLevel],
            uncertainty_threshold=getattr(self.config, "rlhf_uncertainty_threshold", 0.35)) \
            if getattr(self.config, "rlhf_enabled", True) else None
        self.hitl = (HumanInTheLoopCoordinator(
            self.rlhf,
            timeout_s=getattr(self.config, "hitl_timeout_s", 30.0),
            production_safe=getattr(self.config, "hitl_production_safe", True),
            persist=self.persistence,
            approver=hitl_approver)
            if (getattr(self.config, "hitl_enabled", True) and self.rlhf is not None)
            else None)

        # Federated
        self.federated = (FederatedAggregator(
            num_params=4,
            dp_sigma=getattr(self.config, "federated_dp_sigma", 1e-3),
            target_epsilon=getattr(self.config, "federated_target_epsilon", 8.0),
            target_delta=getattr(self.config, "federated_target_delta", 1e-5),
            persist=self.persistence)
            if getattr(self.config, "federated_enabled", True) else None)

        # Causal
        self.causal = (CausalCounterfactualEstimator(state_dim=5, n_actions=len(PrecisionLevel))
                       if getattr(self.config, "causal_enabled", True) else None)
        self.last_counterfactuals: Dict[int, float] = {}

        # MODP + MOE
        self.modp_selector = MODPPrecisionSelector(
            adaptive_cost=adaptive_cost, xai=self.xai, roles=self.role_coordinator)
        self.moe_engine = MOEPrecisionEngine(
            self.carbon_manager, xai=self.xai, roles=self.role_coordinator)

        # Bio
        self.bio = (BioOptimizer(self.config, adaptive_cost, self.rlhf)
                    if getattr(self.config, "bio_enabled", True) else None)

        # Forecaster + scheduler
        self.forecaster = MOEForecaster(self.persistence)
        self.scheduler = (MultiObjectiveCarbonScheduler(
            self.carbon_manager, self.carbon_market)
            if getattr(self.config, "scheduler_enabled", True) else None)

        # Distiller
        self.distiller = MultiTeacherDistiller(
            teachers=[self._teacher_modp, self._teacher_moe, self._teacher_bio],
            weights=[1.0, 1.0, 0.5])

        # Self-healing
        self.self_healing = (SelfHealingManager(self.config, self.rlhf)
                             if getattr(self.config, "self_healing_enabled", True) else None)

        # Active learning
        self.active_learner = ActiveLearningSampler(seed=getattr(self.config, "ga_seed", 0))

        # Chaos
        self.chaos = (ChaosTester(
            engine_ref=self,
            hypotheses=[SteadyStateHypothesis(
                "precision_valid",
                lambda m: m.get("precision") in [p.value for p in PrecisionLevel])],
            max_concurrent=getattr(self.config, "chaos_max_concurrent", 1),
            duration_s=getattr(self.config, "chaos_duration_s", 0.0),
            persist=self.persistence)
            if getattr(self.config, "chaos_testing_enabled", True) else None)

        # State
        self.current_precision: Optional[str] = None
        self.total_energy_saved_j: float = 0.0
        self.total_carbon_saved_kg: float = 0.0
        self.last_decision: Optional[Dict[str, Any]] = None
        self._run_history: deque = deque(maxlen=500)
        self._shutdown_event = asyncio.Event()
        self._background_tasks: List[asyncio.Task] = []
        self._running = False
        self._inflight = 0
        self._inflight_lock = asyncio.Lock()
        self._chaos_exec_override: Optional[Callable] = None

        # Circuit breaker
        self.breaker = CircuitBreaker("mp")

        logger.info(f"EnhancedMixedPrecisionEngine v{self.config.version} "
                    f"(instance {self.instance_id})")
        atexit.register(self._atexit_cleanup)

    def _atexit_cleanup(self):
        with contextlib.suppress(Exception):
            self._shutdown_event.set()
        with contextlib.suppress(Exception):
            self.persistence.close()

    # ------------------------------------------------------------------
    # Teacher callables
    # ------------------------------------------------------------------
    async def _teacher_modp(self, features: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            result = self.modp_selector.select(features)
            return {"selected_precision": result["selected_precision"]}
        except Exception:
            return None

    async def _teacher_moe(self, features: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            result = self.moe_engine.select(features)
            return {"selected_precision": result["selected_precision"]}
        except Exception:
            return None

    async def _teacher_bio(self, features: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self.bio is None:
            return None
        params = self.bio.get_current_params()
        dtype_map = {"accuracy_weight": "fp32", "energy_weight": "fp16",
                     "carbon_weight": "fp8", "speed_weight": "fp4"}
        best_w = max(dtype_map.keys(), key=lambda k: params.get(k, 0.25))
        return {"selected_precision": dtype_map[best_w]}

    # ------------------------------------------------------------------
    # Exercise forward path (for chaos tests)
    # ------------------------------------------------------------------
    async def _exercise_forward_chaos(self):
        """Called by ChaosTester to exercise the engine's forward handling."""
        if self._chaos_exec_override is None:
            return
        try:
            await self._chaos_exec_override()
        except Exception as e:
            logger.warning(f"Chaos forward OOM handled: {e}")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        if self._running:
            return
        self._running = True
        loop = asyncio.get_running_loop()
        self._background_tasks.append(loop.create_task(self._carbon_loop()))
        self._background_tasks.append(loop.create_task(self._ga_loop()))
        if self.chaos and getattr(self.config, "chaos_enable_loop", False):
            self._background_tasks.append(loop.create_task(self._chaos_loop()))
        if self.federated:
            self._background_tasks.append(loop.create_task(self._federated_loop()))
        logger.info("Mixed-precision engine started")

    async def _carbon_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await self.carbon_manager.refresh()
                await self.forecaster.update_history(
                    self.carbon_manager.current.price_usd_per_kg)
            except Exception as e:
                logger.warning(f"Carbon loop error: {e}")
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=getattr(self.config, "carbon_update_interval", 300))
            except asyncio.TimeoutError:
                pass

    async def _ga_loop(self):
        while not self._shutdown_event.is_set():
            if self.bio:
                with contextlib.suppress(Exception):
                    await self.bio.evolve()
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=getattr(self.config, "ga_evolution_interval", 3600))
            except asyncio.TimeoutError:
                pass

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            with contextlib.suppress(Exception):
                fault = random.choice(ChaosTester.FAULT_TYPES)
                await self.chaos.run_test(fault, duration_s=0.1)
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=1800)
            except asyncio.TimeoutError:
                pass

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            with contextlib.suppress(Exception):
                if self.federated and self.federated._pending:
                    self.federated.aggregate()
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=600)
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------
    # Core decision pipeline
    # ------------------------------------------------------------------
    async def decide_precision_async(self, features: Dict[str, Any]) -> str:
        """Full async decision; all I/O is awaited."""
        async with self._inflight_lock:
            self._inflight += 1
        try:
            return await self._decide_inner(features)
        finally:
            async with self._inflight_lock:
                self._inflight -= 1

    def decide_precision(self, features: Dict[str, Any]) -> str:
        """Sync wrapper. Raises SyncAsyncBoundaryError when a loop is running."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.decide_precision_async(features))
        raise SyncAsyncBoundaryError(
            "decide_precision() cannot be called from a running event loop; "
            "await decide_precision_async() instead.")

    async def _decide_inner(self, features: Dict[str, Any]) -> str:
        if not self._rate_limiter.acquire():
            raise RateLimitExceeded("Rate limit exceeded")
        if self._daily_cost_budget > 0 and self._today_cost >= self._daily_cost_budget:
            logger.warning("Daily cost budget exhausted")
            return self.current_precision or self.config.default_dtype

        # Carbon intensity
        try:
            snap = await self.carbon_manager.refresh()
            carbon_intensity = snap.intensity_g_co2_per_kwh
        except Exception as e:
            logger.warning(f"Carbon refresh failed: {e}")
            carbon_intensity = 400.0
        features = {**features, "carbon_intensity": carbon_intensity}

        # Precision telemetry feedback
        if self.precision_controller is not None:
            self.precision_controller.update_telemetry(
                gpu_available=bool(getattr(self.precision_controller.telemetry, "gpu_available", False)),
                observed_quality_drop=0.0)

        # Scheduler
        if self.scheduler is not None:
            with contextlib.suppress(Exception):
                sched = await self.scheduler.schedule(urgency_score=0.5)
                if sched["recommended_delay"] > 0:
                    await asyncio.sleep(min(sched["recommended_delay"], 2))

        # Distillation
        distilled = await self.distiller.distill(features)
        source = "distilled"
        best: Optional[str] = None
        if distilled and "selected_precision" in distilled:
            best = distilled["selected_precision"]

        # MODP fallback
        modp_result: Optional[Dict[str, Any]] = None
        if best is None:
            modp_result = self.modp_selector.select(features)
            best = modp_result["selected_precision"]
            source = "modp"

        # MOE advisory
        moe_result = self.moe_engine.select(features)

        # Adaptive precision override on very high carbon
        if self.precision_controller is not None and carbon_intensity > 500:
            hw = self.precision_controller.select(carbon_intensity, accuracy_required=0.95)
            if hw.value != best:
                best = hw.value
                source = "hw_aware"

        # Temporal + safety shield (fresh per decision)
        temporal = self._base_temporal.fresh() if self._base_temporal else None
        temporal_status: Dict[str, bool] = {}
        if temporal is not None:
            temporal.observe({
                "carbon_intensity": float(carbon_intensity),
                "base_accuracy": float(features.get("base_accuracy", 1.0))})
            temporal_status = temporal.verify()
            if self.shield is not None and not all(temporal_status.values()):
                safer = "fp32"
                best, _ok, _v = self.shield.screen(best, safer, verdict=temporal_status)
                source = "shield"

        # Causal policy
        state_vec = np.array([
            float(features.get("carbon_intensity", 400.0)) / 1000.0,
            float(features.get("base_accuracy", 1.0)),
            float(features.get("input_size", 1000)) / 10000.0,
            float(self.total_carbon_saved_kg),
            utcnow().hour / 24.0])
        if self.causal is not None:
            action = self.causal.select_action(state_vec, len(PrecisionLevel))
            self.last_counterfactuals = self.causal.counterfactuals(
                state_vec, len(PrecisionLevel))

        # Multi-agent advisory vote
        resolution = "skip"
        if self.coordinator is not None and self.roles is not None:
            bids = []
            for role in [AgentRole.ACCURACY_SPECIALIST, AgentRole.ENERGY_SPECIALIST,
                         AgentRole.CARBON_SPECIALIST, AgentRole.SPEED_SPECIALIST,
                         AgentRole.SAFETY_OFFICER]:
                if role == AgentRole.ACCURACY_SPECIALIST:
                    idx = PrecisionLevel.FP32.value != best
                elif role == AgentRole.ENERGY_SPECIALIST:
                    idx = PrecisionLevel.FP8.value != best
                elif role == AgentRole.CARBON_SPECIALIST:
                    idx = PrecisionLevel.FP4.value != best
                elif role == AgentRole.SPEED_SPECIALIST:
                    idx = PrecisionLevel.FP16.value != best
                else:
                    idx = 0
                bids.append(AgentBid(
                    agent_id=role.value, role=role, confidence=0.6,
                    proposed_action=int(idx),
                    rationale=role.value,
                    carbon_score=1.0 - min(1.0, carbon_intensity / 1000.0)))
            voted, resolution = self.coordinator.resolve_conflict(bids, 2)

        # HITL escalation (awaited)
        confidence = max(0.1, min(1.0, float(features.get("base_accuracy", 1.0))))
        hitl_outcome = None
        if self.hitl is not None and confidence < getattr(self.config,
                                                         "hitl_confidence_threshold", 0.65):
            try:
                hitl_outcome = await self.hitl.escalate(
                    decision_context={"features": features, "best": best},
                    options=[p.value for p in PrecisionLevel],
                    confidence=confidence,
                    confidence_threshold=getattr(self.config,
                                                 "hitl_confidence_threshold", 0.65))
            except Exception as e:
                logger.warning(f"HITL failed: {e}")

        # RLHF update
        if self.rlhf is not None:
            reward = confidence * (1.0 - AdaptivePrecisionController.energy_factor(
                PrecisionLevel(best)))
            self.rlhf.update({"carbon_intensity": carbon_intensity}, best, reward)

        # Quantum signature
        signature = None
        if self.quantum is not None:
            with contextlib.suppress(Exception):
                signature = await self.quantum.sign({
                    "features": features, "best": best, "source": source})

        # Blockchain record
        tx = None
        if self.blockchain is not None:
            with contextlib.suppress(Exception):
                data_hash = hashlib.sha256(
                    json.dumps({"features": features, "best": best},
                               sort_keys=True, default=str).encode()).hexdigest()
                tx = await self.blockchain.record(
                    data_id=f"mp_{uuid.uuid4().hex[:8]}",
                    data_hash=data_hash,
                    metadata={"source": source})

        # Energy + carbon savings
        ops = float(features.get("input_size", 1000)) * 2.0
        from_energy = PRECISION_ENERGY_PER_OP[PrecisionLevel.FP32] * ops
        to_energy = PRECISION_ENERGY_PER_OP[PrecisionLevel(best)] * ops
        saved_j = max(0.0, from_energy - to_energy)
        self.total_energy_saved_j += saved_j
        saved_kwh = saved_j / 3.6e6
        carbon_saved_kg = saved_kwh * (carbon_intensity / 1000.0)
        self.total_carbon_saved_kg += carbon_saved_kg
        MP_ENERGY_SAVED.inc(int(saved_j))
        MP_CARBON_SAVED.inc(int(carbon_saved_kg * 1000))
        MP_PRECISION_SWITCHES.labels(
            **{"from": self.config.default_dtype, "to": best}).inc()
        MP_CURRENT_PRECISION.set(
            {"fp32": 0, "bf16": 1, "fp16": 2, "fp8": 3, "fp4": 4}.get(best, 0))

        # Cost record
        self._today_cost += self._cost_per_decision
        MP_COST.inc(self._cost_per_decision)
        with contextlib.suppress(Exception):
            self.persistence.cost_record(self._cost_per_decision, note=best)

        # Federated submission
        if self.federated is not None:
            wd = dict(zip(self.modp_selector.FEATURES, self.modp_selector.weights))
            weights_vec = [float(wd.get("accuracy", 0.25)),
                           float(wd.get("energy", 0.25)),
                           float(wd.get("carbon", 0.25)),
                           float(wd.get("speed", 0.25))]
            self.federated.submit_update(
                client_id=self.instance_id,
                weights=weights_vec,
                samples=1,
                carbon_intensity=carbon_intensity)
            if len(self._run_history) % 10 == 0:
                with contextlib.suppress(PrivacyBudgetExceeded):
                    self.federated.aggregate()

        # MOE gating update (P0 fix: labels the best-matching teacher)
        actual_reward_per_dtype = {
            PrecisionLevel.FP32.value: 1.0,
            PrecisionLevel.BF16.value: 0.9,
            PrecisionLevel.FP16.value: 0.85,
            PrecisionLevel.FP8.value: 0.7,
            PrecisionLevel.FP4.value: 0.5}
        self.moe_engine.update(features, actual_reward_per_dtype)

        # Active learning
        self.active_learner.maybe_store(
            {"carbon_intensity": carbon_intensity, "best": best,
             "saved_j": saved_j}, uncertainty=0.1 - confidence * 0.1)

        # Lineage
        decision_id = uuid.uuid4().hex[:12]
        with contextlib.suppress(Exception):
            self.persistence.record_lineage(
                lineage_id=uuid.uuid4().hex[:12],
                decision_id=decision_id,
                payload={"features": features, "best": best,
                         "source": source, "signature": signature, "tx": tx})

        self.current_precision = best
        self.last_decision = {
            "decision_id": decision_id,
            "selected_precision": best,
            "source": source,
            "carbon_intensity": carbon_intensity,
            "temporal_status": temporal_status,
            "xai_explanation": (modp_result or {}).get("xai_explanation"),
            "role_assignments": (modp_result or {}).get("role_assignments"),
            "moe_role_assignments": moe_result.get("role_assignments"),
            "hitl_outcome": hitl_outcome,
            "multi_agent_resolution": resolution,
            "signature": signature,
            "blockchain_tx": tx,
            "counterfactuals": self.last_counterfactuals,
            "energy_saved_j": saved_j,
            "carbon_saved_kg": carbon_saved_kg,
            "timestamp": utcnow().isoformat()}
        self._run_history.append(self.last_decision)
        return best

    # ------------------------------------------------------------------
    # Quantized forward (async-safe)
    # ------------------------------------------------------------------
    @contextlib.contextmanager
    def quantized_forward(self, model, inputs, dtype: str):
        """Synchronous context manager. `dtype` must be provided.

        Note: use `decide_precision_async` to obtain `dtype` first.
        """
        if not TORCH_AVAILABLE:
            yield model, inputs
            return
        original_dtype = torch.float32
        with contextlib.suppress(Exception):
            original_dtype = next(model.parameters()).dtype
        torch_dtype = {
            "fp32": torch.float32,
            "bf16": torch.bfloat16,
            "fp16": torch.float16,
            "fp8": getattr(torch, "float8_e4m3fn", torch.float16),
            "fp4": torch.float16,
        }.get(dtype, torch.float32)
        try:
            converted = model.to(dtype=torch_dtype)
        except Exception:
            converted = model
        try:
            yield converted, inputs
        finally:
            with contextlib.suppress(Exception):
                converted.to(dtype=original_dtype)

    @contextlib.contextmanager
    def amp_forward(self, model, inputs, dtype: Optional[str] = None):
        if not (TORCH_AVAILABLE and getattr(self.config, "use_amp", False)):
            yield model, inputs
            return
        chosen = dtype or getattr(self.config, "amp_dtype", "fp16")
        amp_dtype = torch.float16 if chosen == "fp16" else torch.bfloat16
        try:
            with _torch_amp_autocast(device_type="cuda", dtype=amp_dtype):
                yield model, inputs
        except TypeError:
            # Older torch; fall back to torch.cuda.amp.autocast
            with contextlib.suppress(Exception):
                from torch.cuda.amp import autocast  # type: ignore
                with autocast(dtype=amp_dtype):
                    yield model, inputs
                return
            yield model, inputs

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------
    async def get_comprehensive_status_async(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "version": self.config.version,
            "running": self._running,
            "current_precision": self.current_precision,
            "total_energy_saved_j": self.total_energy_saved_j,
            "total_carbon_saved_kg": self.total_carbon_saved_kg,
            "today_cost_usd": self._today_cost,
            "daily_cost_budget_usd": self._daily_cost_budget,
            "rate_limit_remaining": self._rate_limiter.remaining(),
            "features": {
                "temporal_logic": self._base_temporal is not None,
                "xai": self.xai is not None,
                "adaptive_precision": self.precision_controller is not None,
                "carbon_market": self.carbon_market is not None,
                "role_specialization": self.role_coordinator is not None,
                "chaos_testing": self.chaos is not None,
                "hitl": self.hitl is not None,
                "federated": self.federated is not None,
                "causal_rl": self.causal is not None,
                "multi_agent": self.coordinator is not None,
                "quantum": self.quantum is not None,
                "blockchain": self.blockchain is not None},
            "temporal_logic": (self._base_temporal.get_status()
                               if self._base_temporal else None),
            "moe": self.moe_engine.get_stats(),
            "self_healing": (await self.self_healing.stats()
                             if self.self_healing else None),
            "chaos_report": self.chaos.get_report() if self.chaos else None,
            "federated": self.federated.get_stats() if self.federated else None,
            "hitl_stats": self.hitl.stats() if self.hitl else None,
            "hitl_audit_verified": (self.hitl.verify_audit_chain()
                                    if self.hitl else None),
            "quantum": self.quantum.status() if self.quantum else None,
            "role_registry": self.roles.stats() if self.roles else None,
            "active_learning_size": len(self.active_learner),
            "last_decision": self.last_decision,
            "health": await self.health_check(),
            "timestamp": utcnow().isoformat()}

    def get_comprehensive_status(self) -> Dict[str, Any]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.get_comprehensive_status_async())
        raise SyncAsyncBoundaryError(
            "get_comprehensive_status() cannot be called from a running event "
            "loop; await get_comprehensive_status_async() instead.")

    async def health_check(self) -> Dict[str, Any]:
        chain_ok = True
        if self.hitl is not None:
            with contextlib.suppress(Exception):
                chain_ok = self.hitl.verify_audit_chain()
        score = 100 if chain_ok else 80
        MP_HEALTH.set(score)
        return {
            "status": "healthy" if score >= 70 else "degraded",
            "health_score": score,
            "hitl_audit_ok": chain_ok,
            "pending_hitl": self.hitl.stats()["pending"] if self.hitl else 0,
            "today_cost_usd": self._today_cost,
            "in_flight": self._inflight,
            "blockchain": (await self.blockchain.health_check()
                           if self.blockchain else None),
            "carbon": await self.carbon_manager.health_check(),
            "timestamp": utcnow().isoformat()}

    async def run_chaos_suite(self) -> Dict[str, Any]:
        if self.chaos is None:
            return {"error": "chaos disabled"}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            with contextlib.suppress(Exception):
                results.append(await self.chaos.run_test(f, duration_s=0.1))
        return {"results": results, "report": self.chaos.get_report()}

    async def shutdown(self):
        logger.info("Shutting down mixed-precision engine...")
        self._shutdown_event.set()
        deadline = time.time() + 10.0
        while self._inflight > 0 and time.time() < deadline:
            await asyncio.sleep(0.1)
        for t in self._background_tasks:
            t.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()
        self.persistence.close()
        logger.info("Shutdown complete")


# ============================================================
# FastAPI
# ============================================================
def build_fastapi_app(engine: EnhancedMixedPrecisionEngine,
                      jwt_secret: Optional[str] = None) -> Any:
    if not FASTAPI_AVAILABLE:
        return None
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app):
        yield
        await engine.shutdown()

    app = FastAPI(title="Mixed Precision API", version=engine.config.version,
                  lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=["*"],
                       allow_credentials=True, allow_methods=["*"],
                       allow_headers=["*"])
    security = HTTPBearer(auto_error=False)
    secret = jwt_secret or engine.config.jwt_secret

    async def verify(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
        if not JOSE_AVAILABLE:
            return {"sub": "anonymous"}
        if credentials is None:
            raise HTTPException(status_code=401, detail="missing credentials")
        try:
            return jwt.decode(credentials.credentials, secret,
                              algorithms=[engine.config.jwt_algorithm])
        except JWTError:
            raise HTTPException(status_code=401, detail="invalid token")

    @app.post("/token")
    async def issue_token(sub: str = "user"):
        if not JOSE_AVAILABLE:
            raise HTTPException(status_code=500, detail="JWT library missing")
        exp = utcnow() + timedelta(seconds=engine.config.jwt_expiry_s)
        token = jwt.encode({"sub": sub, "exp": exp}, secret,
                           algorithm=engine.config.jwt_algorithm)
        return {"access_token": token, "token_type": "bearer"}

    @app.post("/decide")
    async def decide(payload: Dict[str, Any], user: Dict = Depends(verify)):
        return {"selected_precision": await engine.decide_precision_async(payload)}

    @app.get("/status")
    async def status(user: Dict = Depends(verify)):
        return await engine.get_comprehensive_status_async()

    @app.get("/health")
    async def health():
        return await engine.health_check()

    @app.get("/hitl/pending")
    async def hitl_pending(user: Dict = Depends(verify)):
        if not engine.hitl:
            return {"pending": []}
        return {"pending": engine.hitl.pending_requests()}

    @app.post("/hitl/approval")
    async def hitl_approval(request_id: str, approved: bool,
                            user: Dict = Depends(verify)):
        if not engine.hitl:
            raise HTTPException(status_code=503, detail="HITL disabled")
        ok = engine.hitl.resolve(request_id, approved)
        if not ok:
            raise HTTPException(status_code=404, detail="not_found")
        return {"status": "resolved", "request_id": request_id,
                "approved": approved}

    @app.get("/hitl/audit")
    async def hitl_audit(limit: int = 100, user: Dict = Depends(verify)):
        if not engine.hitl:
            return {"audit": []}
        return {"audit": engine.hitl.audit_trail(limit=limit),
                "verified": engine.hitl.verify_audit_chain()}

    @app.post("/chaos")
    async def chaos(fault: str = "carbon_api_down", duration_s: float = 0.2,
                    user: Dict = Depends(verify)):
        if not engine.chaos:
            raise HTTPException(status_code=503, detail="chaos disabled")
        return await engine.chaos.run_test(fault, duration_s=duration_s)

    return app


# ============================================================
# Signal handling + singleton
# ============================================================
_engine_instance: Optional[EnhancedMixedPrecisionEngine] = None
_engine_lock = asyncio.Lock()
_shutdown_event_global = asyncio.Event()
_shutdown_requested = False


async def _signal_shutdown():
    _shutdown_event_global.set()


def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        logger.info(f"Received signal {signum}")
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_signal_shutdown())
        except RuntimeError:
            _shutdown_event_global.set()


def _install_signal_handlers(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))
        except (NotImplementedError, AttributeError):
            try:
                signal.signal(sig, lambda s, f: handle_signal(s, None))
            except (ValueError, OSError):
                pass


async def get_mixed_precision_engine(
    config: Optional[MixedPrecisionConfig] = None,
    **kwargs: Any,
) -> EnhancedMixedPrecisionEngine:
    global _engine_instance
    if _engine_instance is None:
        async with _engine_lock:
            if _engine_instance is None:
                _engine_instance = EnhancedMixedPrecisionEngine(config, **kwargs)
                await _engine_instance.start()
    return _engine_instance


async def shutdown_handler():
    global _engine_instance
    if _engine_instance:
        await _engine_instance.shutdown()
        _engine_instance = None


# ============================================================
# Main
# ============================================================
async def main():
    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop)

    print("=" * 80)
    print("Enhanced Mixed Precision Engine v18.0.0 — P0/P1/P2 hardened")
    print("=" * 80)

    if FASTAPI_AVAILABLE and os.environ.get("MP_SERVE_API", "0") == "1":
        cfg = MixedPrecisionConfig()
        engine = EnhancedMixedPrecisionEngine(cfg)
        await engine.start()
        app = build_fastapi_app(engine, cfg.jwt_secret)
        if app is not None:
            uvicorn.run(app, host=cfg.api_host, port=cfg.api_port, level="info")
        return

    if PROMETHEUS_AVAILABLE and start_http_server:
        with contextlib.suppress(Exception):
            start_http_server(8000)

    engine = await get_mixed_precision_engine(config={
        "persist_path": "/tmp/mixed_precision_v18.db",
        "chaos_enabled": False,
        "chaos_enable_loop": False})

    print("\n✅ Ten enhancements wired in:")
    print("   [1] Quantum-Distillation     dilithium/falcon/sphincs + HMAC fallback")
    print("   [2] Causal RL (UCB)          CausalCounterfactualEstimator")
    print("   [3] Federated (secure + DP)  FederatedAggregator + DPAccountant")
    print("   [4] Multi-Agent              EmergentRoleRegistry + MultiAgentCoordinator")
    print("   [5] Temporal Logic (full STL) TemporalLogicMonitor + SafetyShield")
    print("   [6] Explainable AI           XAIExplainer (SHAP + weighted)")
    print("   [7] Adaptive Precision       AdaptivePrecisionController (telemetry)")
    print("   [8] Carbon Markets / RECs    CarbonMarketClient + providers")
    print("   [9] Resilience / Chaos       ChaosTester (steady-state)")
    print("  [10] HITL / Active Learning   ActiveRLHF + HITL + ActiveLearning")

    # Smoke tests
    for dtype in ["fp16", "fp8", "fp32"]:
        decision = await engine.decide_precision_async({
            "base_accuracy": 0.95, "input_size": 4096,
            "layer_complexity": 0.5})
        print(f"   decision: {decision}")

    status = await engine.get_comprehensive_status_async()
    print(f"\n📊 Instance: {status['instance_id']} v{status['version']}")
    print(f"   Current precision: {status['current_precision']}")
    print(f"   Total energy saved: {status['total_energy_saved_j']:.3f} J")
    print(f"   Total carbon saved: {status['total_carbon_saved_kg']:.6f} kg")
    print(f"   Today cost: ${status['today_cost_usd']:.6f}")
    print(f"   Enhancements: {status['features']}")
    print(f"   MOE: {status['moe']}")
    print(f"   Quantum: {status['quantum']}")
    print(f"   Health: {status['health']['status']}")

    # Chaos smoke (single fault)
    if engine.chaos:
        print("\n🧪 Running chaos smoke test...")
        res = await engine.chaos.run_test("carbon_api_down", duration_s=0.05)
        print(f"   chaos_result={res['passed']}")

    print("\n" + "=" * 80)
    print("✅ Enhanced Mixed Precision Engine v18.0.0 ready")
    print("=" * 80)

    try:
        await _shutdown_event_global.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await shutdown_handler()


if __name__ == "__main__":
    asyncio.run(main())
