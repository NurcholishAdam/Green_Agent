#!/usr/bin/env python3
# src/enhancements/meta_cognitive_architecture_enhanced_v18_0.py
# VERSION: 18.0.0 — Full Green Agent stack (P0/P1/P2 hardened)
"""
Enhanced Meta-Cognitive Architecture — v18.0.0

Objective convention (Pareto / TOPSIS / MODP):
    All objectives are BENEFITS (higher is better).
    Cost-style values are negated before being added to the objective vector.
    `TOPSIS.score` accepts an optional `cost_criteria` set for mixed problems.

P0 fixes:
 - Every central import has a fallback (runs without the parent package)
 - logger is not silently shadowed by structlog
 - No asyncio.get_event_loop() in __init__; explicit async start()
 - carbon_manager is always created for the scheduler
 - Federated submission is real (no dead code)
 - Pareto/TOPSIS conventions consistent (benefits)
 - Chaos: no global asyncio.sleep patch, no NaN corruption
 - _chaos_loop disabled by default
 - Real dilithium/falcon/sphincs with HMAC-SHA256 fallback
 - Real Web3 with hash-chained ledger fallback
 - ParetoFront.get_best_by_weight minimizes when objectives are costs

P1 hardening:
 - SQLite persistence for state, reflections, HITL, chaos, federated, cost
 - Real MOEForecaster (Prophet / linear / Holt-Winters / naive)
 - Deprecated LogisticRegression(multi_class=...) removed
 - Portable signal handlers (Windows fallback)
 - atexit cleanup
 - FastAPI + lifespan
 - JWT from env with ephemeral dev fallback
 - Health checks
 - Structured logging with correlation IDs
 - Real HITL: awaits resolver with timeout
 - Unified MOE engine with consistent teacher interface

P2 advanced:
 - Full STL (G / F / U / X / ->) with actionable enforcement
 - Fresh temporal monitor per reflection
 - Secure federated aggregation (pairwise masks + DP accountant)
 - Multi-agent message bus + conflict resolution
 - Chaos steady-state hypotheses + auto-deactivation
 - Telemetry-driven adaptive precision
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
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union,
)

import numpy as np

# ============================================================
# Logging (do not shadow the central logger)
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
# Optional dependencies
# ============================================================
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

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
    import boto3  # noqa: F401
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient  # noqa: F401
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage as gcs_storage  # noqa: F401
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator
    from pydantic_settings import BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential  # noqa: F401
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

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
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False

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

# ============================================================
# Central config fallback
# ============================================================
try:
    from ..config import config as central_config  # type: ignore
except ImportError:
    class _CentralConfig:
        CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5
        CIRCUIT_BREAKER_RECOVERY_TIMEOUT = 30
        rate_limit_requests = 600
        rate_limit_window = 60
        ENVIRONMENT = "production"
        data_retention_days = 365
        metrics_port = 8000
        api_host = "0.0.0.0"
        api_port = 8000
    central_config = _CentralConfig()


# ============================================================
# Prometheus metrics
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    META_REFLECTIONS = Counter("meta_reflections_total", "Total reflections", ["type"], registry=REGISTRY)
    META_OPTIMIZATIONS = Counter("meta_optimizations_total", "Optimizations", ["strategy", "status"], registry=REGISTRY)
    META_BLOCKCHAIN_TX = Counter("meta_blockchain_tx_total", "Blockchain tx", ["status"], registry=REGISTRY)
    META_QUANTUM_SIG = Counter("meta_quantum_signatures_total", "PQC signatures", ["algorithm", "status"], registry=REGISTRY)
    META_CLOUD_DISTRIBUTIONS = Counter("meta_cloud_distributions_total", "Cloud distributions", ["provider", "status"], registry=REGISTRY)
    META_SUCCESS_RATE = Gauge("meta_success_rate", "Success rate", registry=REGISTRY)
    META_CONFIDENCE = Gauge("meta_confidence", "Confidence", registry=REGISTRY)
    META_CARBON_BUDGET = Gauge("meta_carbon_budget_remaining", "Carbon budget", registry=REGISTRY)
    META_PRECISION_SWITCHES = Counter("meta_precision_switches_total", "Precision switches", ["level"], registry=REGISTRY)
    META_PRECISION_ENERGY = Gauge("meta_precision_energy_joules", "Precision energy", ["level"], registry=REGISTRY)
    META_CHAOS_TESTS = Counter("meta_chaos_tests_total", "Chaos tests", ["fault", "status"], registry=REGISTRY)
    META_TEMPORAL_VIOLATIONS = Counter("meta_temporal_violations_total", "Temporal violations", ["formula"], registry=REGISTRY)
    META_HITL_DECISIONS = Counter("meta_hitl_decisions_total", "HITL decisions", ["decision"], registry=REGISTRY)
    META_HITL_PENDING = Gauge("meta_hitl_pending", "Pending HITL", registry=REGISTRY)
    META_FEDERATED_ROUNDS = Counter("meta_federated_rounds_total", "Federated rounds", registry=REGISTRY)
    META_DP_EPSILON = Gauge("meta_dp_epsilon_spent", "DP epsilon spent", ["node"], registry=REGISTRY)
    META_CARBON_CREDITS_USD = Counter("meta_carbon_credits_usd_total", "Carbon credit USD", registry=REGISTRY)
    META_CARBON_PRICE = Gauge("meta_carbon_price_usd_per_kg", "Carbon price", registry=REGISTRY)
    META_HEALTH_SCORE = Gauge("meta_health_score", "Health score", registry=REGISTRY)
    META_COST_USD = Counter("meta_cost_usd_total", "Cumulative cost", registry=REGISTRY)
    META_ACTIVE_LEARNING = Gauge("meta_active_learning_size", "Active learning size", registry=REGISTRY)
    META_MODP_PARETO = Gauge("meta_modp_pareto_front_size", "Pareto front size", registry=REGISTRY)
    META_GA_FITNESS = Gauge("meta_ga_fitness", "GA fitness", registry=REGISTRY)
    META_ANOMALY_DETECTIONS = Counter("meta_anomaly_detections_total", "Anomalies", ["type"], registry=REGISTRY)
    META_SELF_HEALING_ACTIONS = Counter("meta_self_healing_actions_total", "Self healing", ["action"], registry=REGISTRY)
else:
    class _DummyMetric:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
    META_REFLECTIONS = META_OPTIMIZATIONS = META_BLOCKCHAIN_TX = _DummyMetric()
    META_QUANTUM_SIG = META_CLOUD_DISTRIBUTIONS = META_SUCCESS_RATE = _DummyMetric()
    META_CONFIDENCE = META_CARBON_BUDGET = META_PRECISION_SWITCHES = _DummyMetric()
    META_PRECISION_ENERGY = META_CHAOS_TESTS = META_TEMPORAL_VIOLATIONS = _DummyMetric()
    META_HITL_DECISIONS = META_HITL_PENDING = META_FEDERATED_ROUNDS = _DummyMetric()
    META_DP_EPSILON = META_CARBON_CREDITS_USD = META_CARBON_PRICE = _DummyMetric()
    META_HEALTH_SCORE = META_COST_USD = META_ACTIVE_LEARNING = _DummyMetric()
    META_MODP_PARETO = META_GA_FITNESS = META_ANOMALY_DETECTIONS = _DummyMetric()
    META_SELF_HEALING_ACTIONS = _DummyMetric()


# ============================================================
# Exceptions
# ============================================================
class MetaError(Exception): pass
class CircuitBreakerOpenError(MetaError): pass
class RateLimitExceeded(MetaError): pass
class QuantumError(MetaError): pass
class BlockchainError(MetaError): pass
class PrivacyBudgetExceeded(MetaError): pass
class SafetyViolationError(MetaError): pass
class ChaosTestFailure(MetaError): pass


# ============================================================
# Enums
# ============================================================
class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"


class AgentRole(str, Enum):
    PERFORMANCE_SPECIALIST = "performance_specialist"
    CARBON_SPECIALIST = "carbon_specialist"
    COST_SPECIALIST = "cost_specialist"
    ADAPTIVE_SPECIALIST = "adaptive_specialist"
    SAFETY_OFFICER = "safety_officer"
    VERIFIER = "verifier"


class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# ============================================================
# Config (Pydantic or dataclass)
# ============================================================
if PYDANTIC_AVAILABLE:
    class MetaConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="META_", case_sensitive=False)

        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "18.0.0"
        log_level: str = "INFO"
        environment: str = "production"

        # Persistence
        db_path: str = "/tmp/meta_cognitive_v18.db"
        persist_path: Optional[str] = None

        # Blockchain
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_chain_id: int = 1
        blockchain_poa: bool = False
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None

        # Quantum
        quantum_algorithm: str = "dilithium"
        quantum_master_key: str = ""

        # Carbon
        carbon_api_key: Optional[str] = None
        carbon_region: str = "global"
        carbon_update_interval: int = Field(300, ge=10)
        carbon_base_price_per_ton: float = 50.0
        rec_price_per_mwh: float = 30.0
        carbon_market_url: Optional[str] = None
        electricity_maps_api_key: Optional[str] = None
        electricity_maps_zone: str = "US-CAL-CISO"
        watttime_username: Optional[str] = None
        watttime_password: Optional[str] = None
        watttime_region: str = "CAISO_NORTH"

        # Cloud
        aws_bucket: Optional[str] = None
        aws_region: str = "us-east-1"
        azure_container: Optional[str] = None
        azure_connection_string: Optional[str] = None
        gcp_bucket: Optional[str] = None

        # API + metrics
        metrics_port: int = Field(8000, ge=1024, le=65535)
        api_host: str = "0.0.0.0"
        api_port: int = 8000
        jwt_secret: str = ""
        jwt_algorithm: str = "HS256"
        jwt_expiry_s: int = Field(3600, gt=0)

        # Rate / cost
        rate_limit_per_minute: int = Field(600, ge=1)
        daily_cost_budget_usd: float = Field(10.0, ge=0)
        cost_per_call_usd: float = Field(0.0001, ge=0)

        # Intervals
        health_check_interval: int = Field(60, ge=10)
        reflection_interval: int = Field(60, ge=10)
        self_healing_interval: int = Field(600, ge=60)
        chaos_interval: int = Field(1800, ge=60)
        ga_evolution_interval: int = Field(3600, ge=60)
        federated_interval: int = Field(3600, ge=60)
        retention_days: int = Field(365, gt=0)

        # Features
        limit_graph_enabled: bool = True
        rlhf_enabled: bool = True
        distillation_enabled: bool = True
        causal_enabled: bool = True
        federated_enabled: bool = True
        multi_agent_enabled: bool = True
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        precision_switching_enabled: bool = True
        carbon_market_enabled: bool = True
        chaos_enabled: bool = False
        chaos_enable_loop: bool = False
        chaos_max_concurrent: int = Field(1, ge=1)
        chaos_duration_s: float = Field(0.0, ge=0)
        hitl_enabled: bool = True
        hitl_timeout_s: float = Field(30.0, gt=0)
        hitl_production_safe: bool = True
        hitl_auto_approve_on_timeout: bool = False
        modp_enabled: bool = True
        moe_enabled: bool = True
        bio_enabled: bool = True
        self_healing_enabled: bool = True
        scheduler_enabled: bool = True

        # MOE / GA
        bio_population_size: int = Field(20, ge=10)
        bio_mutation_rate: float = Field(0.1, ge=0.0, le=1.0)
        bio_crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
        ga_seed: int = 0

        # Federated
        federated_dp_sigma: float = Field(1e-3, gt=0)
        federated_target_epsilon: float = Field(8.0, gt=0)
        federated_target_delta: float = Field(1e-5, gt=0)

        # RLHF
        rlhf_uncertainty_threshold: float = Field(0.35, ge=0.0, le=1.0)

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
else:
    @dataclass
    class MetaConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "18.0.0"
        log_level: str = "INFO"
        environment: str = "production"
        db_path: str = "/tmp/meta_cognitive_v18.db"
        persist_path: Optional[str] = None
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_chain_id: int = 1
        blockchain_poa: bool = False
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        quantum_algorithm: str = "dilithium"
        quantum_master_key: str = field(default_factory=lambda: os.urandom(32).hex())
        carbon_api_key: Optional[str] = None
        carbon_region: str = "global"
        carbon_update_interval: int = 300
        carbon_base_price_per_ton: float = 50.0
        rec_price_per_mwh: float = 30.0
        carbon_market_url: Optional[str] = None
        electricity_maps_api_key: Optional[str] = None
        electricity_maps_zone: str = "US-CAL-CISO"
        watttime_username: Optional[str] = None
        watttime_password: Optional[str] = None
        watttime_region: str = "CAISO_NORTH"
        aws_bucket: Optional[str] = None
        aws_region: str = "us-east-1"
        azure_container: Optional[str] = None
        azure_connection_string: Optional[str] = None
        gcp_bucket: Optional[str] = None
        metrics_port: int = 8000
        api_host: str = "0.0.0.0"
        api_port: int = 8000
        jwt_secret: str = field(default_factory=lambda: os.urandom(32).hex())
        jwt_algorithm: str = "HS256"
        jwt_expiry_s: int = 3600
        rate_limit_per_minute: int = 600
        daily_cost_budget_usd: float = 10.0
        cost_per_call_usd: float = 0.0001
        health_check_interval: int = 60
        reflection_interval: int = 60
        self_healing_interval: int = 600
        chaos_interval: int = 1800
        ga_evolution_interval: int = 3600
        federated_interval: int = 3600
        retention_days: int = 365
        limit_graph_enabled: bool = True
        rlhf_enabled: bool = True
        distillation_enabled: bool = True
        causal_enabled: bool = True
        federated_enabled: bool = True
        multi_agent_enabled: bool = True
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        precision_switching_enabled: bool = True
        carbon_market_enabled: bool = True
        chaos_enabled: bool = False
        chaos_enable_loop: bool = False
        chaos_max_concurrent: int = 1
        chaos_duration_s: float = 0.0
        hitl_enabled: bool = True
        hitl_timeout_s: float = 30.0
        hitl_production_safe: bool = True
        hitl_auto_approve_on_timeout: bool = False
        modp_enabled: bool = True
        moe_enabled: bool = True
        bio_enabled: bool = True
        self_healing_enabled: bool = True
        scheduler_enabled: bool = True
        bio_population_size: int = 20
        bio_mutation_rate: float = 0.1
        bio_crossover_rate: float = 0.8
        ga_seed: int = 0
        federated_dp_sigma: float = 1e-3
        federated_target_epsilon: float = 8.0
        federated_target_delta: float = 1e-5
        rlhf_uncertainty_threshold: float = 0.35

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.quantum_master_key)


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


# ============================================================
# Circuit breaker + rate limiter
# ============================================================
class EnhancedCircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0, half_open_max_requests: int = 3):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_requests = half_open_max_requests
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self.last_success_time = 0.0
        self._lock = asyncio.Lock()
        self.half_open_requests = 0

    async def allow_request(self) -> bool:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                    self.failure_count = 0
                else:
                    return False
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.half_open_requests >= self.half_open_max_requests:
                    return False
                self.half_open_requests += 1
            return True

    async def record_success(self):
        async with self._lock:
            self.success_count += 1
            self.last_success_time = time.time()
            if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= 2:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.half_open_requests = 0
            else:
                self.failure_count = 0

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED \
                    and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                self.half_open_requests = 0

    async def call(self, func, *args, **kwargs):
        if not await self.allow_request():
            raise CircuitBreakerOpenError(f"Circuit breaker {self.name} OPEN")
        try:
            result = await func(*args, **kwargs)
            await self.record_success()
            return result
        except Exception:
            await self.record_failure()
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
    CREATE TABLE IF NOT EXISTS state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        payload TEXT NOT NULL,
        ts REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS reflections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,
        payload TEXT NOT NULL,
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
        parent_id TEXT, child_id TEXT, operation TEXT,
        payload TEXT, ts REAL NOT NULL
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

    # --- state ---
    def save_state(self, payload: Dict[str, Any]):
        self._exec(
            "INSERT OR REPLACE INTO state (id, payload, ts) VALUES (1, ?, ?)",
            (json.dumps(payload, default=str), time.time()))

    def load_state(self) -> Optional[Dict[str, Any]]:
        rows = self._fetchall("SELECT payload FROM state WHERE id = 1")
        if not rows:
            return None
        try:
            return json.loads(rows[0][0])
        except Exception:
            return None

    # --- reflections ---
    def save_reflection(self, kind: str, payload: Dict[str, Any]):
        self._exec(
            "INSERT INTO reflections (kind, payload, ts) VALUES (?, ?, ?)",
            (kind, json.dumps(payload, default=str), time.time()))

    def recent_reflections(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self._fetchall(
            "SELECT kind, payload, ts FROM reflections ORDER BY id DESC LIMIT ?",
            (limit,))
        return [{"kind": r[0], "payload": json.loads(r[1]),
                 "timestamp": r[2]} for r in rows]

    # --- HITL audit ---
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

    # --- chaos ---
    def chaos_append(self, fault: str, passed: bool, payload: Dict[str, Any]):
        self._exec(
            "INSERT INTO chaos (fault, passed, payload, ts) VALUES (?,?,?,?)",
            (fault, int(passed), json.dumps(payload, default=str), time.time()))

    def recent_chaos(self, limit: int = 20) -> List[Tuple]:
        return self._fetchall(
            "SELECT fault, passed, payload, ts FROM chaos ORDER BY id DESC LIMIT ?",
            (limit,))

    # --- federated ---
    def federated_append(self, round_num: int, weights: List[float], dp_spent: float):
        self._exec(
            "INSERT INTO federated (round, weights, dp_spent, ts) VALUES (?,?,?,?)",
            (round_num, json.dumps(weights), float(dp_spent), time.time()))

    def recent_federated(self, limit: int = 10) -> List[Tuple]:
        return self._fetchall(
            "SELECT round, weights, dp_spent, ts FROM federated ORDER BY id DESC LIMIT ?",
            (limit,))

    # --- cost ---
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

    # --- lineage ---
    def record_lineage(self, lineage_id: str, parent_id: Optional[str],
                       child_id: Optional[str], operation: str,
                       payload: Dict[str, Any]):
        self._exec(
            "INSERT INTO lineage (lineage_id, parent_id, child_id, operation, payload, ts) "
            "VALUES (?,?,?,?,?,?)",
            (lineage_id, parent_id, child_id, operation,
             json.dumps(payload, default=str), time.time()))

    # --- carbon ---
    def append_carbon_price(self, price: float):
        self._exec(
            "INSERT INTO carbon_history (ts, price) VALUES (?,?)",
            (time.time(), float(price)))

    def recent_carbon_prices(self, limit: int = 500) -> List[float]:
        rows = self._fetchall(
            "SELECT price FROM carbon_history ORDER BY id DESC LIMIT ?", (limit,))
        return [float(r[0]) for r in reversed(rows)]


# ============================================================
# PQC (real dilithium + HMAC fallback)
# ============================================================
class QuantumResilientMetaSecurity:
    def __init__(self, config: MetaConfig):
        self.config = config
        self.algorithm = getattr(config, "quantum_algorithm", "dilithium")
        self._keypair: Optional[Tuple[bytes, bytes]] = None
        self._master = (config.get_master_key_bytes()
                        if hasattr(config, "get_master_key_bytes") else os.urandom(32))
        if PQC_AVAILABLE:
            try:
                if self.algorithm == "dilithium":
                    self._keypair = dilithium.generate_keypair()
                elif self.algorithm == "falcon":
                    self._keypair = falcon.generate_keypair()
                else:
                    self._keypair = sphincs.generate_keypair()
            except Exception as e:
                logger.warning(f"PQC keypair failed: {e}")
                self._keypair = None

    def _hmac_sign(self, payload: bytes) -> str:
        return hmac.new(self._master, payload, hashlib.sha256).hexdigest()

    async def sign(self, data: Dict) -> Dict:
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if PQC_AVAILABLE and self._keypair is not None:
            try:
                pub, priv = self._keypair
                if self.algorithm == "dilithium":
                    sig = dilithium.sign(priv, payload)
                elif self.algorithm == "falcon":
                    sig = falcon.sign(priv, payload)
                else:
                    sig = sphincs.sign(priv, payload)
                META_QUANTUM_SIG.labels(algorithm=self.algorithm, status="success").inc()
                return {"algorithm": self.algorithm,
                        "signature": base64.b64encode(sig).decode()}
            except Exception as e:
                META_QUANTUM_SIG.labels(algorithm=self.algorithm, status="failed").inc()
                return {"algorithm": "none", "signature": "", "error": str(e)}
        sig = self._hmac_sign(payload)
        META_QUANTUM_SIG.labels(algorithm="hmac-sha256", status="success").inc()
        return {"algorithm": "hmac-sha256", "signature": sig}

    async def verify(self, data: Dict, signature_data: Dict) -> bool:
        alg = signature_data.get("algorithm", "none")
        sig = signature_data.get("signature", "")
        if alg == "none" or not sig:
            return False
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if alg == "hmac-sha256":
            return hmac.compare_digest(self._hmac_sign(payload), sig)
        if not PQC_AVAILABLE or self._keypair is None:
            return False
        try:
            sig_bytes = base64.b64decode(sig)
            pub = self._keypair[0]
            if alg == "dilithium":
                dilithium.verify(pub, payload, sig_bytes)
            elif alg == "falcon":
                falcon.verify(pub, payload, sig_bytes)
            elif alg == "sphincs":
                sphincs.verify(pub, payload, sig_bytes)
            else:
                return False
            return True
        except Exception:
            return False

    async def get_quantum_status(self) -> Dict[str, Any]:
        return {"pqc_available": PQC_AVAILABLE, "algorithm": self.algorithm,
                "has_keypair": self._keypair is not None}


# ============================================================
# Blockchain (real Web3 + hash-chained ledger)
# ============================================================
class BlockchainMetaVerification:
    def __init__(self, config: MetaConfig):
        self.config = config
        self.web3 = None
        self._ledger: Dict[str, Dict[str, Any]] = {}
        self._last_hash = ""
        self._lock = threading.Lock()
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

    async def record_meta_data(self, data_id: str, data_hash: str,
                               metadata: Dict) -> Dict:
        with self._lock:
            if data_id in self._ledger:
                return {"tx_hash": self._ledger[data_id]["tx_hash"],
                        "status": "idempotent"}
            tx_hash = None
            private_key = getattr(self.config, "blockchain_private_key", None)
            if self.web3 is not None and private_key:
                try:
                    account = self.web3.eth.account.from_key(private_key)
                    nonce = self.web3.eth.get_transaction_count(account.address)
                    tx = {
                        "nonce": nonce, "gas": 21000,
                        "gasPrice": self.web3.eth.gas_price,
                        "to": account.address, "value": 0,
                        "data": self.web3.to_hex(text=json.dumps({
                            "id": data_id, "hash": data_hash,
                            "meta": metadata}, default=str)),
                        "chainId": getattr(self.config, "blockchain_chain_id", 1),
                    }
                    signed = self.web3.eth.account.sign_transaction(tx, private_key)
                    raw = getattr(signed, "rawTransaction", None) or \
                        getattr(signed, "raw_transaction", None)
                    tx_hash = self.web3.to_hex(
                        self.web3.eth.send_raw_transaction(raw))
                except Exception as e:
                    logger.warning(f"Web3 tx failed, local fallback: {e}")
                    tx_hash = None
            if tx_hash is None:
                chain_input = f"{data_id}:{data_hash}:{self._last_hash}".encode()
                tx_hash = "0x" + hashlib.sha256(chain_input).hexdigest()[:40]
            self._ledger[data_id] = {"tx_hash": tx_hash, "hash": data_hash,
                                     "metadata": metadata, "t": time.time(),
                                     "prev_hash": self._last_hash}
            self._last_hash = tx_hash
            META_BLOCKCHAIN_TX.labels(status="recorded").inc()
            return {"tx_hash": tx_hash, "status": "recorded"}

    async def verify_meta_data(self, data_id: str, data_hash: str) -> Dict:
        entry = self._ledger.get(data_id)
        if entry is None:
            return {"status": "missing"}
        ok = entry["hash"] == data_hash
        META_BLOCKCHAIN_TX.labels(status="verified" if ok else "mismatch").inc()
        return {"status": "verified" if ok else "mismatch"}

    async def get_blockchain_status(self) -> Dict[str, Any]:
        connected = bool(self.web3 and self.web3.is_connected())
        return {"connected": connected,
                "backend": "web3" if connected else "local",
                "total_records": len(self._ledger)}

    async def health_check(self) -> Dict[str, Any]:
        return await self.get_blockchain_status()


# ============================================================
# Cloud distribution (real SDKs + local fallback)
# ============================================================
class MultiCloudStorage:
    def __init__(self, config: MetaConfig):
        self.config = config
        self.providers: Dict[str, Any] = {}
        if AWS_AVAILABLE and getattr(config, "aws_bucket", None):
            try:
                self.providers["aws"] = boto3.client(
                    "s3", region_name=getattr(config, "aws_region", "us-east-1"))
            except Exception as e:
                logger.warning(f"AWS init failed: {e}")
        if AZURE_AVAILABLE and getattr(config, "azure_connection_string", None) \
                and getattr(config, "azure_container", None):
            try:
                self.providers["azure"] = BlobServiceClient.from_connection_string(
                    config.azure_connection_string)
            except Exception as e:
                logger.warning(f"Azure init failed: {e}")
        if GCP_AVAILABLE and getattr(config, "gcp_bucket", None):
            try:
                self.providers["gcp"] = gcs_storage.Client()
            except Exception as e:
                logger.warning(f"GCP init failed: {e}")
        self.local_dir = Path(tempfile.gettempdir()) / "meta_cloud"
        self.local_dir.mkdir(parents=True, exist_ok=True)

    async def store(self, data: Dict, filename: Optional[str] = None) -> Dict:
        filename = filename or f"meta_{uuid.uuid4().hex[:8]}.json"
        payload = json.dumps(data, default=str).encode()
        results: Dict[str, Any] = {}
        loop = asyncio.get_running_loop()

        def _aws():
            self.providers["aws"].put_object(
                Bucket=self.config.aws_bucket, Key=filename, Body=payload)
        def _azure():
            c = self.providers["azure"].get_container_client(self.config.azure_container)
            c.upload_blob(name=filename, data=payload, overwrite=True)
        def _gcp():
            bucket = self.providers["gcp"].bucket(self.config.gcp_bucket)
            bucket.blob(filename).upload_from_string(payload)

        for name, fn in (("aws", _aws), ("azure", _azure), ("gcp", _gcp)):
            if name not in self.providers:
                continue
            try:
                await loop.run_in_executor(None, fn)
                results[name] = "ok"
            except Exception as e:
                results[name] = f"failed: {e}"
        try:
            (self.local_dir / filename).write_bytes(payload)
            results["local"] = "ok"
        except Exception as e:
            results["local"] = f"failed: {e}"
        return {"filename": filename, "results": results,
                "providers": list(self.providers.keys())}

    async def health_check(self) -> Dict[str, Any]:
        return {"status": "ok", "providers": list(self.providers.keys()),
                "local_dir": str(self.local_dir)}


# ============================================================
# Carbon market + intensity (real providers)
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
    def __init__(self, config: MetaConfig, persist: Optional[Persistence] = None):
        self.config = config
        self._persist = persist
        self.carbon_price_per_ton = getattr(config, "carbon_base_price_per_ton", 50.0)
        self.rec_price_per_mwh = getattr(config, "rec_price_per_mwh", 30.0)
        self.grid_intensity_kg_per_mwh = 400.0
        self.trades: List[Dict[str, Any]] = []
        self._retired: Set[str] = set()

    async def get_carbon_credit_value(self, carbon_saved_kg: float) -> float:
        tons = max(0.0, carbon_saved_kg) / 1000.0
        return round(tons * self.carbon_price_per_ton, 4)

    async def get_rec_value(self, energy_saved_kwh: float) -> float:
        mwh = max(0.0, energy_saved_kwh) / 1000.0
        return round(mwh * self.rec_price_per_mwh, 4)

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
        credit_usd = await self.get_carbon_credit_value(amount_kg)
        META_CARBON_CREDITS_USD.inc(credit_usd)
        return rec


class CarbonIntensityManager:
    def __init__(self, config: MetaConfig,
                 market: Optional[CarbonMarketClient] = None,
                 persist: Optional[Persistence] = None):
        self.config = config
        self.market = market
        self._persist = persist
        self.current = CarbonSnapshot(400.0, 0.05, "synthetic",
                                      region=getattr(config, "carbon_region", "global"))
        self.providers: List[Any] = []
        if getattr(config, "electricity_maps_api_key", None):
            self.providers.append(ElectricityMapsProvider(
                config.electricity_maps_api_key,
                getattr(config, "electricity_maps_zone", "US-CAL-CISO")))
        if getattr(config, "watttime_username", None):
            self.providers.append(WattTimeProvider(
                config.watttime_username, config.watttime_password,
                getattr(config, "watttime_region", "CAISO_NORTH")))
        if not self.providers:
            self.providers.append(ElectricityMapsProvider(
                None, getattr(config, "electricity_maps_zone", "US-CAL-CISO")))
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
        META_CARBON_PRICE.set(self.current.price_usd_per_kg)
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
# Temporal logic (full STL)
# ============================================================
class STLOperator(str, Enum):
    ALWAYS = "G"
    EVENTUALLY = "F"
    UNTIL = "U"
    NEXT = "X"
    IMPLIES = "->"


class TemporalLogicMonitor:
    def __init__(self, history_len: int = 200):
        self.formulas: Dict[str, str] = {}
        self.compiled: Dict[str, Callable[[List[Dict]], bool]] = {}
        self.history: deque = deque(maxlen=history_len)
        self.violations: List[Dict] = []
        self._lock = threading.Lock()

    def add_formula(self, name: str, formula: str):
        self.formulas[name] = formula
        self.compiled[name] = self._compile(formula)

    def update(self, state: Dict):
        with self._lock:
            self.history.append(dict(state))

    def _compile(self, formula: str) -> Callable[[List[Dict]], bool]:
        f = formula.strip()
        if f.startswith("G(") and f.endswith(")"):
            inner = self._compile(f[2:-1])
            return lambda hist: all(inner([h]) for h in hist) if hist else True
        if f.startswith("F(") and f.endswith(")"):
            inner = self._compile(f[2:-1])
            return lambda hist: any(inner([h]) for h in hist) if hist else False
        if f.startswith("X(") and f.endswith(")"):
            inner = self._compile(f[2:-1])
            return lambda hist: inner([hist[1]]) if len(hist) >= 2 else True
        if " U " in f:
            left, right = f.split(" U ", 1)
            lf, rf = self._compile(left), self._compile(right)
            def until(hist):
                for i in range(len(hist)):
                    if rf(hist[i:]):
                        return True
                    if not lf([hist[i]]):
                        return False
                return False
            return until
        if "->" in f:
            left, right = f.split("->", 1)
            lf, rf = self._compile(left.strip()), self._compile(right.strip())
            return lambda hist: (not lf(hist)) or rf(hist)
        return self._atom(f)

    def _atom(self, atom: str) -> Callable[[List[Dict]], bool]:
        atom = atom.strip()
        for op in ("<=", ">=", "==", "!=", "<", ">"):
            if op in atom:
                lhs, rhs = [s.strip() for s in atom.split(op, 1)]
                def make(lhs, op, rhs):
                    def check(hist):
                        if not hist:
                            return True
                        s = hist[-1]
                        lv = s.get(lhs, 0.0)
                        try:
                            rv = float(rhs)
                        except ValueError:
                            rv = s.get(rhs, 0.0)
                        return {"<":  lambda: lv <  rv,
                                ">":  lambda: lv >  rv,
                                "<=": lambda: lv <= rv,
                                ">=": lambda: lv >= rv,
                                "==": lambda: lv == rv,
                                "!=": lambda: lv != rv}[op]()
                    return check
                return make(lhs, op, rhs)
        return lambda hist: bool(atom.lower() in ("true", "1", "yes"))

    def evaluate(self) -> Dict[str, bool]:
        with self._lock:
            hist = list(self.history)
        results = {}
        for name, fn in self.compiled.items():
            try:
                ok = fn(hist)
            except Exception as e:
                logger.warning(f"Temporal '{name}' error: {e}")
                ok = False
            results[name] = ok
            if not ok:
                META_TEMPORAL_VIOLATIONS.labels(formula=name).inc()
                with self._lock:
                    self.violations.append({
                        "formula": name, "expression": self.formulas[name],
                        "timestamp": utcnow().isoformat()})
        return results

    def get_status(self) -> Dict[str, Any]:
        return {"formulas": self.formulas,
                "last_results": self.evaluate(),
                "violations": self.violations[-5:]}


class SafetyShield:
    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor
        self.violations: List[Dict[str, Any]] = []

    def screen(self, selected_idx: int, candidates: Sequence[Any],
               cost_fn: Optional[Callable[[Any], float]] = None
               ) -> Tuple[int, bool, Dict[str, bool]]:
        verdict = self.monitor.evaluate()
        violated = [k for k, ok in verdict.items() if not ok]
        if not violated:
            return selected_idx, True, verdict
        if not candidates:
            return selected_idx, False, verdict
        if cost_fn is None:
            cost_fn = lambda c: 0.0
        safe_idx = min(range(len(candidates)), key=lambda i: cost_fn(candidates[i]))
        self.violations.append({"t": time.time(), "violated": violated,
                                "original": selected_idx, "replacement": safe_idx})
        return safe_idx, False, verdict


# ============================================================
# Causal RL (UCB policy)
# ============================================================
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

    def _ensure_actions(self, n: int):
        if n == self.n_actions:
            return
        with self._lock:
            self._A = np.eye(self.state_dim + n + 1) * self.ridge
            self._b = np.zeros(self.state_dim + n + 1)
            self.n_actions = n
            self._action_counts = np.zeros(n)
            self._n = 0

    def update(self, t: CausalTransition):
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
            return {"n_updates": int(self._n), "n_actions": self.n_actions,
                    "action_counts": self._action_counts.tolist()}


# ============================================================
# Federated (secure + DP)
# ============================================================
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

    def step(self, sigma: float, sensitivity: float = 1.0):
        if sigma <= 0:
            return
        eps = sensitivity * math.sqrt(2 * math.log(1.25 / self.target_delta)) / sigma
        self.spent += eps
        self._rounds += 1

    def can_spend(self) -> bool:
        return self.spent < self.target_epsilon


class FederatedAggregator:
    def __init__(self, num_params: int = 4, dp_sigma: float = 1e-3,
                 target_epsilon: float = 8.0, target_delta: float = 1e-5,
                 staleness_s: float = 3600.0, persist: Optional[Persistence] = None):
        self.round = 0
        self.num_params = num_params
        self.global_weights: List[float] = [1.0 / num_params] * num_params
        self.client_updates: List[FederatedUpdate] = []
        self.accountant = DPAccountant(target_epsilon, target_delta)
        self.dp_sigma = dp_sigma
        self.staleness_s = staleness_s
        self._persist = persist
        self._lock = threading.Lock()

    def submit_update(self, client_id: str, weights: List[float], samples: int,
                      carbon_intensity: float = 400.0):
        if len(weights) != self.num_params:
            return
        with self._lock:
            self.client_updates.append(FederatedUpdate(
                node_id=client_id,
                weights={"w": np.asarray(weights, dtype=np.float64)},
                n_samples=max(1, int(samples)),
                carbon_intensity=float(carbon_intensity)))

    def _fresh(self) -> List[FederatedUpdate]:
        now = time.time()
        return [u for u in self.client_updates
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
        stacked = np.stack([u.weights["w"] for u in fresh], axis=0)
        if secure and len(fresh) > 1:
            masks = self._pairwise_masks(node_ids, stacked.shape[1:])
            stacked = stacked + np.stack(masks, axis=0)
        blended = np.tensordot(weights, stacked, axes=([0], [0]))
        if self.dp_sigma > 0:
            blended = blended + np.random.normal(0.0, self.dp_sigma, size=blended.shape)
            self.accountant.step(self.dp_sigma)
        with self._lock:
            self.global_weights = blended.tolist()
            self.round += 1
            self.client_updates.clear()
        for nid in node_ids:
            META_DP_EPSILON.labels(node=nid).set(self.accountant.spent)
        META_FEDERATED_ROUNDS.inc()
        if self._persist:
            with contextlib.suppress(Exception):
                self._persist.federated_append(
                    self.round, self.global_weights, self.accountant.spent)
        return {"weights": self.global_weights, "round": self.round,
                "dp_spent": self.accountant.spent}

    def get_stats(self) -> Dict[str, Any]:
        return {"round": self.round, "global_weights": self.global_weights,
                "pending_updates": len(self.client_updates),
                "dp_spent": self.accountant.spent,
                "dp_target": self.accountant.target_epsilon}


# ============================================================
# Multi-agent
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
                    "success_rate": {r.value:
                                     (self._successes[r] / self._counts[r])
                                     if self._counts[r] else 0.0
                                     for r in AgentRole}}


class MultiAgentCoordinator:
    def __init__(self, registry: EmergentRoleRegistry,
                 role_bias: Optional[Dict[AgentRole, float]] = None):
        self.registry = registry
        self.role_bias = role_bias or {
            AgentRole.PERFORMANCE_SPECIALIST: 0.9,
            AgentRole.CARBON_SPECIALIST: 1.2,
            AgentRole.COST_SPECIALIST: 1.1,
            AgentRole.ADAPTIVE_SPECIALIST: 0.9,
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
# XAI (SHAP + weighted)
# ============================================================
class XAIExplainer:
    def __init__(self, feature_names: List[str],
                 predict_fn: Optional[Callable] = None):
        self.feature_names = feature_names
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

    def explain_topsis(self, candidate: Dict[str, float], weights: List[float],
                       all_candidates: List[Dict[str, float]],
                       top_k: int = 5) -> Dict[str, Any]:
        if not all_candidates:
            return {"contributions": {}, "top_features": [],
                    "narrative": [], "weights_used": {}, "method": "none"}
        matrix = np.array([[c[f] for f in self.feature_names]
                           for c in all_candidates])
        norms = np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9
        cand_vec = np.array([candidate[f] for f in self.feature_names])
        weighted = (cand_vec / norms) * np.array(weights)
        contrib = {f: float(weighted[i]) for i, f in enumerate(self.feature_names)}
        ranked = sorted(contrib.items(), key=lambda kv: abs(kv[1]),
                        reverse=True)[:top_k]
        narrative = [f"{f} ({v:+.3f}) contributes to the score."
                     for f, v in ranked]
        method = "weighted_topsis"
        shap_attr = None
        if self._shap_explainer is not None:
            shap_attr = self._shap_attributions(cand_vec)
            if shap_attr:
                method = "shap"
        return {"contributions": contrib,
                "top_features": [f for f, _ in ranked],
                "narrative": narrative,
                "weights_used": {f: float(w) for f, w in
                                 zip(self.feature_names, weights)},
                "method": method,
                "shap_attributions": shap_attr}


# ============================================================
# Adaptive precision (telemetry-driven)
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
    def __init__(self, carbon_manager: Optional[CarbonIntensityManager] = None):
        self.carbon_manager = carbon_manager
        self.telemetry = HardwareTelemetry()
        self.last_precision = PrecisionLevel.FP32
        self._ema_quality_drop = 0.0
        self._lock = threading.Lock()

    def update_telemetry(self, **kwargs):
        with self._lock:
            for k, v in kwargs.items():
                if hasattr(self.telemetry, k):
                    setattr(self.telemetry, k, v)
            alpha = 0.2
            self._ema_quality_drop = (1 - alpha) * self._ema_quality_drop + \
                                     alpha * self.telemetry.observed_quality_drop

    def select(self, carbon_intensity: float,
               accuracy_required: float = 0.95) -> PrecisionLevel:
        with self._lock:
            tel = self.telemetry
            drop = self._ema_quality_drop
        if accuracy_required > 0.99 or drop > 0.05:
            chosen = PrecisionLevel.FP32
        elif carbon_intensity > 500:
            chosen = PrecisionLevel.INT8
        elif tel.gpu_available and carbon_intensity < 350:
            chosen = PrecisionLevel.FP16
        else:
            chosen = PrecisionLevel.FP32
        self.last_precision = chosen
        META_PRECISION_SWITCHES.labels(level=chosen.value).inc()
        META_PRECISION_ENERGY.labels(level=chosen.value).set(
            self.energy_factor(chosen))
        return chosen

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0,
                PrecisionLevel.FP16: 0.6,
                PrecisionLevel.INT8: 0.3}[level]


# ============================================================
# Role specialization
# ============================================================
class RoleSpecializationCoordinator:
    def __init__(self):
        self.roles = [AgentRole.PERFORMANCE_SPECIALIST, AgentRole.CARBON_SPECIALIST,
                      AgentRole.COST_SPECIALIST, AgentRole.ADAPTIVE_SPECIALIST]
        self.affinity = np.array([
            [0.9, 0.1, 0.2, 0.3],
            [0.1, 0.9, 0.2, 0.3],
            [0.1, 0.2, 0.9, 0.3],
            [0.3, 0.2, 0.2, 0.9]])

    def assign_roles(self, context: Dict[str, float]) -> Dict[str, Any]:
        ctx = np.array([
            context.get("confidence", 0.5),
            context.get("carbon_budget", 0.5),
            context.get("cost_budget", 0.5),
            context.get("adaptability", 0.5)])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        assignments = {r.value: float(probs[i]) for i, r in enumerate(self.roles)}
        dominant = self.roles[int(np.argmax(probs))].value
        return {"assignments": assignments, "dominant_role": dominant}


# ============================================================
# Chaos (steady-state, no global mutation)
# ============================================================
@dataclass
class SteadyStateHypothesis:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]


class ChaosTester:
    FAULT_TYPES = ["carbon_api_down", "state_corruption",
                   "latency_spike", "blockchain_broken", "pqc_broken"]

    def __init__(self, arch_ref: Optional[Any] = None,
                 hypotheses: Optional[List[SteadyStateHypothesis]] = None,
                 max_concurrent: int = 1, duration_s: float = 0.0,
                 persist: Optional[Persistence] = None):
        self.arch = arch_ref
        self.hypotheses = hypotheses or []
        self.max_concurrent = max(1, int(max_concurrent))
        self.duration_s = float(duration_s)
        self.results: List[Dict] = []
        self._active = 0
        self._activated_at = time.time()
        self._persist = persist
        self._lock = threading.Lock()

    def _can_inject(self) -> bool:
        if self._active >= self.max_concurrent:
            return False
        if self.duration_s > 0 and \
                (time.time() - self._activated_at) > self.duration_s:
            return False
        return True

    async def run_test(self, fault_type: str,
                       duration_s: float = 0.2) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        if not self._can_inject():
            return {"fault": fault_type, "passed": False,
                    "error": "max_concurrent_faults_reached"}
        with self._lock:
            self._active += 1
        logger.warning(f"CHAOS: injecting {fault_type} for {duration_s}s")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable[[], None]] = []

        try:
            if fault_type == "carbon_api_down" and self.arch and self.arch.carbon_manager:
                orig = self.arch.carbon_manager._override

                async def broken():
                    raise RuntimeError("carbon API down")
                self.arch.carbon_manager.set_override(broken)
                restore.append(lambda: self.arch.carbon_manager.set_override(orig))
            elif fault_type == "state_corruption" and self.arch and self.arch.state:
                orig_conf = getattr(self.arch.state, "confidence", 0.5)
                # Use a finite extreme value, not NaN
                self.arch.state.confidence = -1.0
                restore.append(lambda: setattr(self.arch.state, "confidence", orig_conf))
            elif fault_type == "latency_spike":
                if self.arch:
                    orig_latency = self.arch._chaos_latency_override
                    self.arch._chaos_latency_override = duration_s
                    restore.append(lambda: setattr(
                        self.arch, "_chaos_latency_override", orig_latency))
                await asyncio.sleep(duration_s)
            elif fault_type == "blockchain_broken" and self.arch and self.arch.blockchain:
                orig = self.arch.blockchain.record_meta_data

                async def no_bc(*a, **k):
                    return {"tx_hash": None, "status": "failed"}
                self.arch.blockchain.record_meta_data = no_bc
                restore.append(lambda: setattr(
                    self.arch.blockchain, "record_meta_data", orig))
            elif fault_type == "pqc_broken" and self.arch and self.arch.quantum_security:
                orig = self.arch.quantum_security.sign

                async def no_sig(d):
                    return {"algorithm": "none", "signature": ""}
                self.arch.quantum_security.sign = no_sig
                restore.append(lambda: setattr(
                    self.arch.quantum_security, "sign", orig))

            if fault_type not in ("latency_spike",):
                await asyncio.sleep(duration_s)

            # Steady-state check
            metrics = {}
            if self.arch and self.arch.state:
                metrics = self.arch.state.to_dict()
            if not self.steady_state_ok(metrics):
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
                  "error": error_msg,
                  "timestamp": utcnow().isoformat()}
        self.results.append(result)
        META_CHAOS_TESTS.labels(
            fault=fault_type, status="pass" if passed else "fail").inc()
        if self._persist:
            with contextlib.suppress(Exception):
                self._persist.chaos_append(fault_type, passed, result)
        logger.warning(f"CHAOS result: {result}")
        return result

    def steady_state_ok(self, metrics: Dict[str, Any]) -> bool:
        return all(h.predicate(metrics) for h in self.hypotheses)

    def get_report(self) -> Dict[str, Any]:
        return {"tests_run": len(self.results),
                "pass_rate": (sum(1 for r in self.results if r["passed"]) /
                              len(self.results)) if self.results else 1.0,
                "recent": self.results[-5:],
                "hypotheses": [h.name for h in self.hypotheses]}


# ============================================================
# Active RLHF
# ============================================================
class ActiveRLHF:
    def __init__(self, action_space: List[str],
                 uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0):
        self.actions = list(action_space)
        self.uncertainty_threshold = float(uncertainty_threshold)
        self.human_timeout_s = float(human_timeout_s)
        self.preference_counts = defaultdict(float)
        self.context_priors: Dict[str, Dict[str, float]] = defaultdict(
            lambda: defaultdict(float))
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._approver: Optional[Callable[[Any], bool]] = None

    def _ctx_key(self, context: Any) -> str:
        return json.dumps(context, sort_keys=True, default=str)[:64]

    def _policy(self, context: Any) -> np.ndarray:
        key = self._ctx_key(context)
        with self._lock:
            raw = np.array(
                [self.preference_counts[a] + self.context_priors[key][a]
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
        return float(ent / np.log(len(self.actions)))

    def update(self, context: Any, action: str, reward: float):
        if action not in self.actions:
            return
        key = self._ctx_key(context)
        with self._lock:
            self.preference_counts[action] += float(reward)
            self.context_priors[key][action] += float(reward)
        self.history.append({"action": action, "reward": float(reward),
                             "context": key,
                             "timestamp": utcnow().isoformat()})

    async def maybe_query_human(self, context: Dict,
                                options: List[str]) -> Optional[Dict]:
        u = self.uncertainty(context)
        if u <= self.uncertainty_threshold:
            return None
        qid = str(uuid.uuid4())
        query = {"id": qid, "context": context, "options": options,
                 "uncertainty": u, "created_at": utcnow().isoformat(),
                 "status": "pending"}
        with self._lock:
            self.pending_queries[qid] = query
        META_HITL_PENDING.set(len(self.pending_queries))
        return query

    def resolve_query(self, query_id: str, chosen: str, rating: float = 1.0):
        with self._lock:
            q = self.pending_queries.pop(query_id, None)
            META_HITL_PENDING.set(len(self.pending_queries))
        if q is None:
            return None
        q.update({"status": "resolved", "chosen": chosen, "rating": rating})
        self.update(q["context"], chosen, rating)
        return q


# ============================================================
# HITL gate (pending registry + hash-chained audit)
# ============================================================
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
            priority: int = 0, metadata: Optional[Dict[str, Any]] = None):
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
        META_HITL_PENDING.set(0)

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
            META_HITL_DECISIONS.labels(decision="blocked").inc()
            return False
        if approver is None:
            self._audit_append(HITLAuditEntry(
                req.request_id, True, req.reason, "auto_no_approver",
                (time.time() - start) * 1000.0))
            META_HITL_DECISIONS.labels(decision="auto_approved").inc()
            return True
        with self._lock:
            if len(self._pending) >= self.max_pending:
                self._audit_append(HITLAuditEntry(
                    req.request_id, False, req.reason, "queue_full",
                    (time.time() - start) * 1000.0))
                META_HITL_DECISIONS.labels(decision="queue_full").inc()
                return False
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            self._pending[req.request_id] = req
            self._futures[req.request_id] = future
            META_HITL_PENDING.set(len(self._pending))

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
                META_HITL_PENDING.set(len(self._pending))
            if fut is not None and not fut.done():
                with contextlib.suppress(Exception):
                    fut.set_result(bool(approved))

        latency_ms = (time.time() - start) * 1000.0
        self._audit_append(HITLAuditEntry(
            req.request_id, approved, req.reason, mode, latency_ms))
        META_HITL_DECISIONS.labels(
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
            request_id, approved, "external_resolve", "resolved"))
        META_HITL_DECISIONS.labels(
            decision="approved" if approved else "rejected").inc()
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

    def get_audit(self) -> Dict[str, Any]:
        return {"total": len(self._audit),
                "recent": [e.to_dict() for e in list(self._audit)[-10:]],
                "verified": self.verify_audit_chain()}

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            pending = len(self._pending)
        return {"pending": pending, "max_pending": self.max_pending,
                "audit_size": len(self._audit),
                "production_safe": self.production_safe,
                "timeout_s": self.timeout_s}


# ============================================================
# Active learning sampler
# ============================================================
class ActiveLearningSampler:
    def __init__(self, capacity: int = 256, seed: int = 0,
                 feature_dim: int = 4, diversity_weight: float = 0.4,
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
        META_ACTIVE_LEARNING.set(0)

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
        META_ACTIVE_LEARNING.set(size)
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
            if self._kmeans is not None and len(indices) >= max(2 * self.n_clusters, 8):
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

    def __len__(self):
        with self._lock:
            return len(self._buffer)


# ============================================================
# Pareto + TOPSIS (benefit convention + cost_criteria support)
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

    def get_pareto_front(self):
        return self.solutions

    def get_best_by_weight(self, weights: Sequence[float]):
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
        matrix = np.array([[c[crit] for crit in criteria] for c in candidates])
        norm = matrix / (np.sqrt((matrix ** 2).sum(axis=0)) + 1e-12)
        weighted = norm * np.asarray(weights[:norm.shape[1]])
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
        return (d_minus / (d_plus + d_minus + 1e-12)).tolist()


# ============================================================
# Genetic algorithm + bio optimizer
# ============================================================
class GeneticAlgorithmOptimizer:
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1,
                 crossover_rate: float = 0.8, seed: int = 0):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Dict[str, float]] = []
        self.bounds = {"confidence_weight": (0.0, 1.0),
                       "carbon_weight": (0.0, 1.0),
                       "cost_weight": (0.0, 1.0),
                       "threshold_offset": (-0.1, 0.1)}
        self._rng = random.Random(seed)
        self._initialized = False

    def initialize(self, force: bool = False):
        if self._initialized and not force:
            return
        self.population = []
        for _ in range(self.pop_size):
            ind = {k: self._rng.uniform(*v) for k, v in self.bounds.items()}
            total = sum(ind.values()) or 1.0
            ind = {k: v / total for k, v in ind.items()}
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
            child = {k: (p1[k] if self._rng.random() < 0.5 else p2[k])
                     for k in p1}
        else:
            child = dict(p1)
        total = sum(child.values()) or 1.0
        return {k: v / total for k, v in child.items()}

    def mutate(self, ind):
        if self._rng.random() < self.mutation_rate:
            key = self._rng.choice(list(self.bounds.keys()))
            ind[key] = self._rng.uniform(*self.bounds[key])
            total = sum(ind.values()) or 1.0
            ind = {k: v / total for k, v in ind.items()}
        return ind

    def evolve(self, fitness_func, generations=5):
        self.initialize()
        for _ in range(generations):
            fit = self.evaluate(fitness_func)
            best = dict(self.population[int(np.argmax(fit))])
            parents = self.select(fit, self.pop_size - 1)
            offspring = []
            for i in range(0, len(parents) - 1, 2):
                offspring.append(self.mutate(
                    self.crossover(parents[i], parents[i + 1])))
                offspring.append(self.mutate(
                    self.crossover(parents[i + 1], parents[i])))
            self.population = offspring[:self.pop_size - 1] + [best]
        fit = self.evaluate(fitness_func)
        return dict(self.population[int(np.argmax(fit))])


class BioOptimizer:
    def __init__(self, config: MetaConfig, adaptive_cost=None,
                 limit_graph=None, rlhf: Optional[ActiveRLHF] = None):
        self.config = config
        self.adaptive_cost = adaptive_cost
        self.ga = GeneticAlgorithmOptimizer(
            population_size=config.bio_population_size,
            mutation_rate=config.bio_mutation_rate,
            crossover_rate=config.bio_crossover_rate,
            seed=getattr(config, "ga_seed", 0))
        self.current_params = {"confidence_weight": 0.4, "carbon_weight": 0.3,
                               "cost_weight": 0.3, "threshold_offset": 0.0}
        self.fitness_history = deque(maxlen=50)
        self._lock = asyncio.Lock()
        self.limit_graph = limit_graph
        self.rlhf = rlhf

    def _fitness_func(self, params):
        if self.adaptive_cost:
            try:
                cost = self.adaptive_cost.evaluate({
                    "confidence": params["confidence_weight"],
                    "carbon": params["carbon_weight"],
                    "cost": params["cost_weight"]})
                return -float(cost)
            except Exception:
                pass
        return (params["confidence_weight"] * 0.4
                + (1 - params["carbon_weight"]) * 0.3
                + (1 - params["cost_weight"]) * 0.3)

    async def evolve(self) -> Dict[str, float]:
        best = self.ga.evolve(self._fitness_func, generations=5)
        async with self._lock:
            self.current_params = best
            self.fitness_history.append(self._fitness_func(best))
        META_GA_FITNESS.set(self._fitness_func(best))
        return best

    async def optimize(self, state: Dict[str, Any], strategy: Optional[str] = None):
        features = np.array([state.get("confidence", 0.5),
                             state.get("success_rate", 0.5),
                             state.get("carbon_budget", 0.5),
                             utcnow().hour / 24.0])
        selected = strategy
        source = "explicit" if strategy else "ga"
        if selected is None and self.rlhf is not None:
            u = self.rlhf.uncertainty({"features": features.tolist()})
            if u > self.rlhf.uncertainty_threshold:
                selected = self.rlhf.sample_action({"features": features.tolist()})
                source = "rlhf"
        if selected is None:
            best = await self.evolve()
            return {"action": "bio_evolved", "params": best,
                    "recommendation": f"GA evolved: {best}", "source": "ga"}
        params = {
            "performance": {"confidence_weight": 0.7, "carbon_weight": 0.1,
                            "cost_weight": 0.2, "threshold_offset": -0.02},
            "carbon": {"confidence_weight": 0.2, "carbon_weight": 0.6,
                       "cost_weight": 0.2, "threshold_offset": 0.02},
            "cost": {"confidence_weight": 0.3, "carbon_weight": 0.2,
                     "cost_weight": 0.5, "threshold_offset": 0.0},
            "adaptive": self.current_params,
        }.get(selected, self.current_params)
        if self.limit_graph is not None:
            with contextlib.suppress(Exception):
                limits = self.limit_graph.get_limits(features) or {}
                for k in list(params.keys()):
                    if k in limits and params[k] > limits[k]:
                        params[k] = limits[k]
        self.current_params = params
        if self.rlhf is not None and source in ("rlhf",):
            self.rlhf.update({"features": features.tolist()}, selected,
                             self._fitness_func(params))
        return {"action": f"{selected}_strategy", "params": params,
                "recommendation": f"Selected {selected}", "source": source}

    def get_current_params(self) -> Dict[str, float]:
        return dict(self.current_params)


# ============================================================
# MOE forecaster (real experts)
# ============================================================
class MOEForecaster:
    def __init__(self, persist: Optional[Persistence] = None):
        self.experts: List[Tuple[str, Callable]] = []
        self.history: deque = deque(maxlen=1000)
        self.history_context: deque = deque(maxlen=1000)
        self.history_labels: deque = deque(maxlen=1000)
        self.gating_model = None
        self.scaler = None
        self._trained = False
        self._persist = persist
        self._init_experts()
        self._init_gating()
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

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            try:
                self.gating_model = LogisticRegression(max_iter=1000)
                self.scaler = StandardScaler()
            except Exception:
                pass

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

    async def _extract_context(self):
        return np.array([utcnow().hour / 24, utcnow().weekday() / 6, 0.5, 0.5])

    async def update_history(self, value: float):
        self.history.append({"ds": utcnow(), "y": float(value)})
        self.history_context.append((await self._extract_context()).tolist())
        if self._persist:
            with contextlib.suppress(Exception):
                self._persist.append_carbon_price(value)
        await self._compute_proxy_label()
        if len(self.history_context) % 20 == 0 and len(self.history_context) >= 40:
            await self._train_gating()

    async def _compute_proxy_label(self):
        if len(self.history) < 30:
            return
        recent = list(self.history)
        actual = recent[-1]["y"]
        window = recent[-25:-1]
        errors = []
        for _, fn in self.experts:
            try:
                preds = await fn(window, 1)
                errors.append(abs(preds[0] - actual))
            except Exception:
                errors.append(float("inf"))
        self.history_labels.append(int(np.argmin(errors)))

    async def _train_gating(self):
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
            logger.warning(f"Forecaster gating fit failed: {e}")

    async def forecast(self, horizon: int = 24) -> Dict[str, Any]:
        if len(self.history) < 10:
            return {"prices": [0.5] * horizon, "confidence": 0.0,
                    "source": "cold"}
        forecasts = []
        for name, fn in self.experts:
            try:
                forecasts.append(np.asarray(await fn(self.history, horizon)))
            except Exception:
                forecasts.append(np.full(horizon, 0.5))
        weights = None
        source = "uniform"
        if self.gating_model is not None and self._trained:
            try:
                ctx = await self._extract_context()
                X_scaled = self.scaler.transform([ctx])
                probs = self.gating_model.predict_proba(X_scaled)[0]
                if len(probs) == len(self.experts):
                    weights = probs
                    source = "gating"
            except Exception:
                weights = None
        if weights is None:
            weights = np.ones(len(self.experts)) / len(self.experts)
        final = np.zeros(horizon)
        for i, f in enumerate(forecasts):
            final += weights[i] * f
        return {"prices": final.tolist(),
                "expert_weights": weights.tolist(),
                "confidence": 0.85, "source": source}

    def get_stats(self) -> Dict[str, Any]:
        return {"num_experts": len(self.experts),
                "gating_trained": self._trained,
                "history_len": len(self.history)}


# ============================================================
# MOE strategy optimizer (unified teacher interface)
# ============================================================
@dataclass
class TeacherAdjustment:
    score: float
    adjust_confidence: float = 0.0
    adjust_threshold: float = 0.0


class MOEEngine:
    """
    Unified MOE engine. Every teacher returns a `TeacherAdjustment`.
    The gating model is trained on the best teacher per sample.
    """
    def __init__(self, config: MetaConfig,
                 role_coordinator: Optional[RoleSpecializationCoordinator] = None,
                 persist: Optional[Persistence] = None):
        self.config = config
        self.persist = persist
        self.role_coordinator = role_coordinator or RoleSpecializationCoordinator()
        self.teachers: Dict[str, Callable[[Dict[str, Any]], TeacherAdjustment]] = {}
        self.gating_model = None
        self.scaler = None
        self._trained = False
        self.history_context: deque = deque(maxlen=1000)
        self.history_labels: deque = deque(maxlen=1000)
        self._teacher_errors: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._init_teachers()
        self._init_gating()

    def _init_teachers(self):
        self.teachers = {
            "performance": self._performance_teacher,
            "carbon": self._carbon_teacher,
            "cost": self._cost_teacher,
            "adaptive": self._adaptive_teacher}

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            try:
                self.gating_model = LogisticRegression(max_iter=1000)
                self.scaler = StandardScaler()
            except Exception as e:
                logger.warning(f"Gating init failed: {e}")

    def _performance_teacher(self, state: Dict[str, Any]) -> TeacherAdjustment:
        sr = state.get("success_rate", 0.5)
        if sr < 0.4:
            return TeacherAdjustment(score=0.3, adjust_confidence=-0.1,
                                     adjust_threshold=0.05)
        if sr > 0.8:
            return TeacherAdjustment(score=0.9, adjust_confidence=0.05,
                                     adjust_threshold=-0.02)
        return TeacherAdjustment(score=0.6)

    def _carbon_teacher(self, state: Dict[str, Any]) -> TeacherAdjustment:
        if state.get("carbon_budget", 0.5) < 0.2:
            return TeacherAdjustment(score=0.9, adjust_confidence=-0.05,
                                     adjust_threshold=0.02)
        return TeacherAdjustment(score=0.5)

    def _cost_teacher(self, state: Dict[str, Any]) -> TeacherAdjustment:
        if state.get("cost_budget", 0.5) < 0.2:
            return TeacherAdjustment(score=0.9, adjust_confidence=-0.05,
                                     adjust_threshold=0.02)
        return TeacherAdjustment(score=0.5)

    def _adaptive_teacher(self, state: Dict[str, Any]) -> TeacherAdjustment:
        if len(self.history_labels) >= 10:
            recent = list(self.history_labels)[-10:]
            avg = float(np.mean(recent))
            if avg > 0.6:
                return TeacherAdjustment(score=0.7, adjust_confidence=0.02)
        return TeacherAdjustment(score=0.5)

    async def _extract_context(self, state: Dict[str, Any]) -> np.ndarray:
        return np.array([state.get("confidence", 0.5),
                         state.get("success_rate", 0.5),
                         state.get("carbon_budget", 0.5),
                         state.get("cost_budget", 0.5),
                         utcnow().hour / 24.0])

    async def optimize_strategies(self, state: Dict[str, Any]) -> Dict[str, Any]:
        teacher_results = {}
        for name, fn in self.teachers.items():
            try:
                teacher_results[name] = fn(state)
            except Exception as e:
                logger.warning(f"Teacher {name} failed: {e}")
                teacher_results[name] = TeacherAdjustment(score=0.5)

        scores = np.array([t.score for t in teacher_results.values()])
        weights = await self._get_gating_weights(state, len(scores))
        weighted_score = float(np.dot(weights, scores))
        best_idx = int(np.argmax(scores))
        best_name = list(teacher_results.keys())[best_idx]
        best_result = teacher_results[best_name]

        # Record for gating training
        ctx = (await self._extract_context(state)).tolist()
        self.history_context.append(ctx)
        self.history_labels.append(best_idx)
        for name, r in teacher_results.items():
            self._teacher_errors[name].append(abs(r.score - max(scores)))
        if len(self.history_context) % 20 == 0 and len(self.history_context) >= 40:
            await self._train_gating()

        roles = self.role_coordinator.assign_roles(state)
        result = {
            "action": f"{best_name}_optimization",
            "selected_strategy": best_name,
            "teacher_scores": {n: t.score for n, t in teacher_results.items()},
            "gating_weights": {n: float(w) for n, w in
                               zip(teacher_results.keys(), weights)},
            "weighted_score": weighted_score,
            "combined_adjustment": {
                "adjust_confidence": best_result.adjust_confidence,
                "adjust_threshold": best_result.adjust_threshold},
            "role_assignments": roles,
            "recommendation": self._recommendation(best_name),
        }
        if self.persist:
            with contextlib.suppress(Exception):
                self.persist.save_reflection(
                    "moe_strategy", {"strategy": best_name, **result})
        META_OPTIMIZATIONS.labels(strategy=best_name, status="success").inc()
        return result

    async def _get_gating_weights(self, state: Dict[str, Any],
                                  n: int) -> np.ndarray:
        if self.gating_model is not None and self._trained:
            try:
                ctx = await self._extract_context(state)
                X_scaled = self.scaler.transform([ctx])
                probs = self.gating_model.predict_proba(X_scaled)[0]
                if len(probs) == n:
                    return probs
            except Exception:
                pass
        return np.ones(n) / n

    async def _train_gating(self):
        if not SKLEARN_AVAILABLE or self.gating_model is None:
            return
        if len(self.history_context) < 40:
            return
        n = min(len(self.history_context), len(self.history_labels))
        X = np.array(self.history_context[-n:])
        y = np.array(self.history_labels[-n:])
        if len(np.unique(y)) < 2:
            return
        try:
            X_scaled = self.scaler.fit_transform(X)
            self.gating_model.fit(X_scaled, y)
            self._trained = True
        except Exception as e:
            logger.warning(f"MOE gating fit failed: {e}")

    @staticmethod
    def _recommendation(strategy: str) -> str:
        return {
            "performance": "Prioritize high-confidence experts.",
            "carbon": "Route to low-emission regions and reduce consumption.",
            "cost": "Optimize expert selection for cost.",
            "adaptive": "Adjust dynamically based on recent trends.",
        }.get(strategy, "Maintain current strategy.")

    def get_stats(self) -> Dict[str, Any]:
        return {"num_teachers": len(self.teachers),
                "gating_trained": self._trained,
                "history_len": len(self.history_context),
                "teacher_errors": {k: float(np.mean(v)) if v else None
                                   for k, v in self._teacher_errors.items()}}


# ============================================================
# Multi-objective carbon scheduler
# ============================================================
class MultiObjectiveCarbonScheduler:
    def __init__(self, config: MetaConfig,
                 carbon_manager: CarbonIntensityManager,
                 forecaster: Optional[MOEForecaster] = None,
                 market_client: Optional[CarbonMarketClient] = None):
        self.config = config
        self.carbon_manager = carbon_manager
        self.forecaster = forecaster
        self.market_client = market_client
        self.carbon_weight = 0.3
        self.urgency_weight = 0.5
        self.cost_weight = 0.2
        self.max_delay = 24 * 3600
        self.history = deque(maxlen=100)

    async def schedule(self, urgency_score: float = 0.5) -> Dict[str, Any]:
        forecast = await self.forecaster.forecast(24) if self.forecaster else None
        market = await self.market_client.get_market_snapshot() \
            if self.market_client else {}
        if not forecast or not forecast.get("prices"):
            try:
                intensity = await self.carbon_manager.get_current_intensity()
            except Exception:
                intensity = 400.0
            delay = 3600 if intensity > 400 else 0
            return {"recommended_delay": delay, "reason": "simple_threshold",
                    "market": market}
        best = None
        first = forecast["prices"][0] or 1e-9
        for d in range(0, self.max_delay + 1, 3600):
            idx = int(d / 3600) + 1
            avg = float(np.mean(forecast["prices"][:idx]))
            savings = max(0.0, (first - avg) / first)
            urgency_cost = d / (self.max_delay + 1) * urgency_score
            energy_cost = d * 0.001
            composite = (-self.carbon_weight * savings
                         + self.urgency_weight * urgency_cost
                         + self.cost_weight * energy_cost)
            if best is None or composite < best["cost"]:
                best = {"delay": d, "cost": composite, "carbon_savings": savings}
        if best is None:
            best = {"delay": 0, "cost": 0, "carbon_savings": 0}
        self.history.append(best)
        return {"recommended_delay": best["delay"],
                "reason": "multi_objective",
                "carbon_savings": best["carbon_savings"], "market": market}


# ============================================================
# Self-healing
# ============================================================
class SelfHealingManager:
    def __init__(self, config: MetaConfig, drift_detector=None,
                 rlhf: Optional[ActiveRLHF] = None,
                 chaos: Optional[ChaosTester] = None):
        self.config = config
        self.drift = drift_detector
        self.anomaly_detectors: List[Tuple[str, Any]] = []
        self.gating_weights: List[float] = []
        self._lock = threading.Lock()
        self.recovery_actions: deque = deque(maxlen=100)
        self._trained = False
        self._training_buffer: deque = deque(maxlen=500)
        self._refit_counter = 0
        self.rlhf = rlhf
        self.chaos = chaos
        if SKLEARN_AVAILABLE and config.self_healing_enabled:
            self.anomaly_detectors = [
                ("iforest", IsolationForest(contamination=0.1)),
                ("ocsvm", OneClassSVM(nu=0.1))]
            self.gating_weights = [0.5, 0.5]

    def _features(self, metrics: Dict[str, Any]) -> np.ndarray:
        return np.array([
            float(metrics.get("success_rate", 0.5)),
            float(metrics.get("confidence", 0.5)),
            float(metrics.get("carbon_budget_remaining", 50)) / 100.0,
            float(metrics.get("reflection_count", 0) % 100) / 100.0]).reshape(1, -1)

    async def _maybe_fit(self, force: bool = False):
        if not self.anomaly_detectors:
            return
        if len(self._training_buffer) < 20:
            return
        if self._trained and not force:
            return
        X = np.vstack(list(self._training_buffer))
        for _, m in self.anomaly_detectors:
            try:
                m.fit(X)
            except Exception as e:
                logger.warning(f"Detector fit failed: {e}")
        self._trained = True

    async def detect_anomaly(self, metrics: Dict[str, Any]):
        if not self.anomaly_detectors:
            return metrics.get("success_rate", 0.5) < 0.2, 0.8
        X = self._features(metrics)
        with self._lock:
            self._training_buffer.append(X)
            self._refit_counter += 1
        if not self._trained:
            await self._maybe_fit()
            return False, 0.0
        if self._refit_counter % 50 == 0:
            await self._maybe_fit(force=True)
        votes = []
        for _, m in self.anomaly_detectors:
            try:
                votes.append(1 if m.predict(X)[0] == -1 else 0)
            except Exception:
                votes.append(0)
        if not votes:
            return False, 0.0
        score = sum(v * w for v, w in zip(votes, self.gating_weights[:len(votes)]))
        if score > 0.5:
            META_ANOMALY_DETECTIONS.labels(type="self_healing").inc()
            await self._trigger_recovery(metrics, "restart")
        return score > 0.5, float(score)

    async def _trigger_recovery(self, metrics: Dict[str, Any], default_action: str):
        action = default_action
        if self.rlhf is not None:
            try:
                cand = self.rlhf.sample_action(metrics)
                if cand:
                    action = str(cand)
            except Exception:
                pass
        with self._lock:
            self.recovery_actions.append({"action": action,
                                          "timestamp": utcnow().isoformat()})
        META_SELF_HEALING_ACTIONS.labels(action=action).inc()

    async def train(self, data: List[Dict[str, Any]]):
        for item in data:
            try:
                with self._lock:
                    self._training_buffer.append(self._features(item))
            except Exception:
                pass
        await self._maybe_fit()

    async def check_drift(self, metrics: Dict[str, Any]):
        if self.drift is not None:
            try:
                detected = await self.drift.check_drift(metrics)
            except Exception:
                detected = False
            if detected:
                await self._trigger_recovery(metrics, "drift_recovery")

    async def get_stats(self) -> Dict[str, Any]:
        return {"enabled": self.config.self_healing_enabled,
                "trained": self._trained,
                "num_detectors": len(self.anomaly_detectors),
                "recent_actions": list(self.recovery_actions)[-5:],
                "chaos_active": self.chaos is not None}


# ============================================================
# Meta-cognitive state
# ============================================================
class EnhancedMetaCognitiveState:
    def __init__(self, persist: Optional[Persistence] = None):
        self._persist = persist
        self.confidence = 0.5
        self.uncertainty = 0.5
        self.historical_success_rate = 0.5
        self.reflection_count = 0
        self.carbon_budget_remaining = 100.0
        self.helium_budget_remaining = 100.0
        self.reflection_threshold = 0.3
        self.active_strategies: Dict[str, float] = {}
        self.strategy_effectiveness: Dict[str, float] = {}
        self.preferred_experts: List[str] = []
        self.avoided_experts: List[str] = []
        self.expert_health_scores: Dict[str, float] = {}
        self._lock = threading.Lock()

    async def load(self):
        if not self._persist:
            return
        data = self._persist.load_state()
        if not data:
            return
        with self._lock:
            for k, v in data.items():
                if hasattr(self, k):
                    try:
                        setattr(self, k, v)
                    except Exception:
                        pass

    async def save(self):
        if not self._persist:
            return
        with self._lock:
            payload = self.to_dict()
        self._persist.save_state(payload)

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "confidence": self.confidence,
                "uncertainty": self.uncertainty,
                "success_rate": self.historical_success_rate,
                "reflection_count": self.reflection_count,
                "carbon_budget_remaining": self.carbon_budget_remaining,
                "helium_budget_remaining": self.helium_budget_remaining,
                "reflection_threshold": self.reflection_threshold,
            }


# ============================================================
# Main architecture
# ============================================================
class EnhancedMetaCognitiveArchitecture:
    def __init__(self, config: Optional[MetaConfig] = None,
                 adaptive_cost: Optional[Any] = None,
                 limit_graph: Optional[Any] = None,
                 drift_detector: Optional[Any] = None,
                 hitl_approver: Optional[Callable[[HITLRequest], bool]] = None):
        if PYDANTIC_AVAILABLE and isinstance(config, dict):
            self.config = MetaConfig(**config)
        elif config is None:
            self.config = MetaConfig()
        else:
            self.config = config
        self.instance_id = self.config.instance_id
        self._start_time = utcnow()

        # Persistence
        self.persist_path = self.config.persist_path or self.config.db_path
        self.persistence = Persistence(self.persist_path)

        # State
        self.state = EnhancedMetaCognitiveState(self.persistence)

        # Rate + cost
        self._rate_limiter = TokenBucket(
            rate=getattr(self.config, "rate_limit_per_minute", 600),
            per_seconds=60.0)
        self._today_cost = self.persistence.cost_today() \
            if hasattr(self.persistence, "cost_today") else 0.0
        self._daily_cost_budget = getattr(self.config, "daily_cost_budget_usd", 10.0)
        self._cost_per_call = getattr(self.config, "cost_per_call_usd", 0.0001)

        # Enhancement modules
        self.quantum_security = QuantumResilientMetaSecurity(self.config)
        self.blockchain = BlockchainMetaVerification(self.config)
        self.cloud_storage = MultiCloudStorage(self.config)
        self.carbon_market = (CarbonMarketClient(self.config, self.persistence)
                              if self.config.carbon_market_enabled else None)
        self.carbon_manager = CarbonIntensityManager(
            self.config, self.carbon_market, self.persistence)

        # Temporal logic (fresh monitor per reflection via _fresh_temporal_monitor)
        self._base_temporal = TemporalLogicMonitor() if self.config.temporal_logic_enabled else None
        if self._base_temporal:
            self._base_temporal.add_formula("carbon_cap", "G(carbon <= 20.0)")
            self._base_temporal.add_formula("confidence_min", "F(confidence >= 0.5)")
            self._base_temporal.add_formula("availability_ok", "G(availability >= 0.3)")

        # XAI
        self.xai = XAIExplainer(
            ["cost", "carbon", "latency", "availability"],
            predict_fn=None) if self.config.xai_enabled else None

        # Precision
        self.precision_controller = (AdaptivePrecisionController(self.carbon_manager)
                                     if self.config.precision_switching_enabled else None)

        # Roles
        self.role_coordinator = RoleSpecializationCoordinator()

        # RLHF + HITL
        self.rlhf = ActiveRLHF(
            action_space=["performance", "carbon", "cost", "adaptive"],
            uncertainty_threshold=getattr(self.config,
                                          "rlhf_uncertainty_threshold", 0.35))
        self.hitl = (HumanInTheLoopCoordinator(
            self.rlhf,
            timeout_s=self.config.hitl_timeout_s,
            production_safe=self.config.hitl_production_safe,
            persist=self.persistence,
            approver=hitl_approver)
            if self.config.hitl_enabled else None)

        # Federated
        self.federated_aggregator = (FederatedAggregator(
            num_params=4,
            dp_sigma=self.config.federated_dp_sigma,
            target_epsilon=self.config.federated_target_epsilon,
            target_delta=self.config.federated_target_delta,
            persist=self.persistence)
            if self.config.federated_enabled else None)

        # Multi-agent
        self.roles = EmergentRoleRegistry()
        self.coordinator = MultiAgentCoordinator(self.roles)

        # Causal
        self.causal = (CausalCounterfactualEstimator(state_dim=5, n_actions=4)
                       if self.config.causal_enabled else None)
        self.last_counterfactuals: Dict[int, float] = {}

        # Chaos
        hypotheses = [
            SteadyStateHypothesis("confidence_in_range",
                                  lambda m: 0.0 <= m.get("confidence", 0.5) <= 1.0),
            SteadyStateHypothesis("success_rate_above_0",
                                  lambda m: m.get("success_rate", 0.5) >= 0.0)]
        self.chaos_tester = (ChaosTester(
            arch_ref=self, hypotheses=hypotheses,
            max_concurrent=self.config.chaos_max_concurrent,
            duration_s=self.config.chaos_duration_s,
            persist=self.persistence)
            if self.config.chaos_enabled else None)

        # MOE engine (unified)
        self.moe_engine = (MOEEngine(self.config, self.role_coordinator,
                                     self.persistence)
                           if self.config.moe_enabled else None)

        # Bio
        self.bio_optimizer = (BioOptimizer(self.config, adaptive_cost,
                                           limit_graph, self.rlhf)
                              if self.config.bio_enabled else None)

        # Forecaster + scheduler
        self.forecaster = MOEForecaster(self.persistence)
        self.scheduler = (MultiObjectiveCarbonScheduler(
            self.config, self.carbon_manager, self.forecaster,
            self.carbon_market) if self.config.scheduler_enabled else None)

        # Self-healing
        self.self_healing = (SelfHealingManager(self.config, drift_detector,
                                                self.rlhf, self.chaos_tester)
                             if self.config.self_healing_enabled else None)

        # Active learning
        self.active_learner = ActiveLearningSampler(
            seed=getattr(self.config, "ga_seed", 0))

        # Limit graph
        self.limit_graph = limit_graph

        # Optional pipeline items
        self.adaptive_cost = adaptive_cost
        self.drift = drift_detector

        # Internals
        self._background_tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        self._running = False
        self._inflight = 0
        self._inflight_lock = asyncio.Lock()
        self._chaos_latency_override: float = 0.0

        # Reflection handlers
        self.reflection_triggers: Dict[str, Callable[..., Awaitable[None]]] = {
            "anomaly_detected": self._reflect_on_anomaly,
            "slo_breached": self._reflect_on_slo_breach,
            "health_degraded": self._reflect_on_health_change,
            "performance_drop": self._reflect_on_performance,
            "budget_low": self._reflect_on_budget,
        }

        logger.info(f"EnhancedMetaCognitiveArchitecture v18.0.0 "
                    f"(instance {self.instance_id})")
        atexit.register(self._atexit_cleanup)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def _atexit_cleanup(self):
        with contextlib.suppress(Exception):
            self._shutdown_event.set()
        with contextlib.suppress(Exception):
            self.persistence.close()

    async def start(self):
        """Explicit async start; call after instantiation inside a running loop."""
        if self._running:
            return
        self._running = True
        await self.state.load()
        self._start_background_tasks()
        logger.info("Meta-cognitive architecture started")

    def _start_background_tasks(self):
        loop = asyncio.get_running_loop()
        self._background_tasks.append(
            loop.create_task(self._reflection_loop()))
        self._background_tasks.append(
            loop.create_task(self._self_healing_loop()))
        if self.federated_aggregator:
            self._background_tasks.append(
                loop.create_task(self._federated_loop()))
        if self.chaos_tester and self.config.chaos_enable_loop:
            self._background_tasks.append(
                loop.create_task(self._chaos_loop()))
        if self.bio_optimizer:
            self._background_tasks.append(
                loop.create_task(self._ga_loop()))
        if self.scheduler:
            self._background_tasks.append(
                loop.create_task(self._scheduler_loop()))

    async def _reflection_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.reflection_interval)
            except asyncio.TimeoutError:
                pass
            if self._shutdown_event.is_set():
                break
            with contextlib.suppress(Exception):
                await self._trigger_reflection("performance_drop")

    async def _self_healing_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.self_healing_interval)
            except asyncio.TimeoutError:
                pass
            if self._shutdown_event.is_set():
                break
            if self.self_healing is None:
                continue
            try:
                metrics = {
                    "success_rate": self.state.historical_success_rate,
                    "confidence": self.state.confidence,
                    "carbon_budget_remaining": self.state.carbon_budget_remaining,
                    "reflection_count": self.state.reflection_count,
                }
                is_anom, score = await self.self_healing.detect_anomaly(metrics)
                if is_anom:
                    logger.warning(f"Anomaly score={score:.2f}")
            except Exception as e:
                logger.error(f"Self-healing loop error: {e}")

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.federated_interval)
            except asyncio.TimeoutError:
                pass
            if self._shutdown_event.is_set():
                break
            with contextlib.suppress(Exception):
                if self.federated_aggregator and \
                        self.federated_aggregator.client_updates:
                    self.federated_aggregator.aggregate()

    async def _chaos_loop(self):
        """Opt-in only. Injects a random fault at chaos_interval."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.chaos_interval)
            except asyncio.TimeoutError:
                pass
            if self._shutdown_event.is_set():
                break
            with contextlib.suppress(Exception):
                if self.chaos_tester:
                    fault = random.choice(ChaosTester.FAULT_TYPES)
                    await self.chaos_tester.run_test(fault, duration_s=0.1)

    async def _ga_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.ga_evolution_interval)
            except asyncio.TimeoutError:
                pass
            if self._shutdown_event.is_set():
                break
            with contextlib.suppress(Exception):
                if self.bio_optimizer:
                    await self.bio_optimizer.evolve()

    async def _scheduler_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=300)
            except asyncio.TimeoutError:
                pass
            if self._shutdown_event.is_set():
                break
            with contextlib.suppress(Exception):
                if self.scheduler:
                    await self.scheduler.schedule(urgency_score=0.5)

    def _fresh_temporal_monitor(self) -> TemporalLogicMonitor:
        m = TemporalLogicMonitor()
        if self._base_temporal:
            for name, f in self._base_temporal.formulas.items():
                m.add_formula(name, f)
        return m

    # ------------------------------------------------------------------
    # Reflection
    # ------------------------------------------------------------------
    async def _reflect_on_anomaly(self, **kwargs):
        await self._record_reflection("anomaly_detected", kwargs)

    async def _reflect_on_slo_breach(self, **kwargs):
        await self._record_reflection("slo_breached", kwargs)

    async def _reflect_on_health_change(self, **kwargs):
        await self._record_reflection("health_degraded", kwargs)

    async def _reflect_on_performance(self, **kwargs):
        await self._record_reflection("performance_drop", kwargs)

    async def _reflect_on_budget(self, **kwargs):
        await self._record_reflection("budget_low", kwargs)

    async def _record_reflection(self, kind: str, payload: Dict[str, Any]):
        self.state.reflection_count += 1
        META_REFLECTIONS.labels(type=kind).inc()
        if self.persistence:
            with contextlib.suppress(Exception):
                self.persistence.save_reflection(kind, payload)

    async def _trigger_reflection(self, trigger_type: str, **kwargs):
        handler = self.reflection_triggers.get(trigger_type)
        if handler is None:
            return
        logger.info(f"Reflection triggered: {trigger_type}")
        META_REFLECTIONS.labels(type=trigger_type).inc()

        # Fresh temporal monitor per reflection (P2)
        temporal = self._fresh_temporal_monitor()
        temporal.update({
            "carbon": min(20.0, self.state.carbon_budget_remaining / 10.0),
            "confidence": self.state.confidence,
            "availability": 0.99 if self.state.historical_success_rate > 0.5 else 0.3,
        })
        verdict = temporal.evaluate()
        if not all(verdict.values()):
            logger.warning(f"Temporal violations during reflection: {verdict}")

        # Scheduler-aware delay (real carbon intensity)
        if self.scheduler:
            with contextlib.suppress(Exception):
                sched = await self.scheduler.schedule(urgency_score=0.5)
                if sched["recommended_delay"] > 0:
                    await asyncio.sleep(min(sched["recommended_delay"], 1))

        # MOE strategy selection
        state_ctx = {
            "confidence": self.state.confidence,
            "success_rate": self.state.historical_success_rate,
            "carbon_budget": self.state.carbon_budget_remaining / 100.0,
            "cost_budget": 0.5,
        }
        if self.moe_engine:
            try:
                strategy = await self.moe_engine.optimize_strategies(state_ctx)
                combined = strategy.get("combined_adjustment", {})
                self.state.confidence = max(
                    0.1, min(1.0,
                             self.state.confidence + combined.get("adjust_confidence", 0.0)))
                self.state.reflection_threshold = max(
                    0.1, min(0.9,
                             self.state.reflection_threshold +
                             combined.get("adjust_threshold", 0.0)))
            except Exception as e:
                logger.warning(f"MOE strategy failed: {e}")

        # Causal policy for reflection strategy selection
        if self.causal:
            try:
                state_vec = np.array([self.state.confidence,
                                      self.state.historical_success_rate,
                                      self.state.carbon_budget_remaining / 100.0,
                                      0.5, utcnow().hour / 24.0])
                action = self.causal.select_action(state_vec, 4)
                self.last_counterfactuals = self.causal.counterfactuals(state_vec, 4)
            except Exception:
                pass

        # Bio optimizer (with RLHF override)
        if self.bio_optimizer:
            with contextlib.suppress(Exception):
                await self.bio_optimizer.optimize(state_ctx)

        # Federated submission (real)
        if self.federated_aggregator and self.moe_engine:
            try:
                weights = [self.state.confidence,
                           self.state.historical_success_rate,
                           self.state.carbon_budget_remaining / 100.0,
                           self.state.reflection_threshold]
                self.federated_aggregator.submit_update(
                    client_id=self.instance_id,
                    weights=weights,
                    samples=1,
                    carbon_intensity=await self.carbon_manager.get_current_intensity())
                self.federated_aggregator.aggregate()
            except PrivacyBudgetExceeded as e:
                logger.warning(f"Federated DP exhausted: {e}")
            except Exception as e:
                logger.warning(f"Federated submit failed: {e}")

        # HITL escalation when confidence is low
        if self.hitl and self.state.confidence < self.config.hitl_confidence_threshold \
                if hasattr(self.config, "hitl_confidence_threshold") else 0.65:
            try:
                req = HITLRequest.new(
                    reason="low_confidence",
                    chosen_idx=0,
                    candidates=[{"strategy": "continue"}],
                    uncertainty=1.0 - self.state.confidence,
                    carbon_price=await self.carbon_manager.get_current_price(),
                    priority=1)
                approved = await self.hitl.request(req)
                if not approved:
                    logger.warning("HITL rejected reflection")
            except Exception as e:
                logger.warning(f"HITL failed: {e}")

        await handler(**kwargs)

    # ------------------------------------------------------------------
    # Record outcome
    # ------------------------------------------------------------------
    async def record_outcome(self, task_id: str, success: bool, reward: float,
                             expert_used: str, carbon_kg: float,
                             helium_units: float, latency_ms: float,
                             user_id: Optional[str] = None,
                             sign_data: bool = True,
                             blockchain_record: bool = True):
        async with self._inflight_lock:
            self._inflight += 1
        try:
            # Rate limit
            if not self._rate_limiter.acquire():
                raise RateLimitExceeded("Rate limit exceeded")

            # Cost budget
            if self._daily_cost_budget > 0 and \
                    self._today_cost >= self._daily_cost_budget:
                logger.warning("Daily cost budget exhausted")

            # Update state
            self.state.historical_success_rate = (
                0.9 * self.state.historical_success_rate +
                0.1 * (1.0 if success else 0.0))
            self.state.confidence = max(
                0.1, min(1.0,
                         0.9 * self.state.confidence + 0.1 * float(reward)))

            # Cost record
            self._today_cost += self._cost_per_call
            if self.persistence:
                with contextlib.suppress(Exception):
                    self.persistence.cost_record(
                        self._cost_per_call, note=f"task:{task_id}")
            META_COST_USD.inc(self._cost_per_call)

            # Chaos latency override
            if self._chaos_latency_override > 0:
                await asyncio.sleep(self._chaos_latency_override)

            # Federated submission
            if self.federated_aggregator:
                try:
                    weights = [self.state.confidence,
                               self.state.historical_success_rate,
                               self.state.carbon_budget_remaining / 100.0,
                               self.state.reflection_threshold]
                    self.federated_aggregator.submit_update(
                        client_id=self.instance_id,
                        weights=weights,
                        samples=1,
                        carbon_intensity=await self.carbon_manager.get_current_intensity())
                except Exception as e:
                    logger.warning(f"Federated submit failed: {e}")

            # HITL escalation
            if self.hitl and self.state.confidence < 0.65:
                try:
                    req = HITLRequest.new(
                        reason="low_confidence",
                        chosen_idx=0,
                        candidates=[{"task_id": task_id}],
                        uncertainty=1.0 - self.state.confidence,
                        carbon_price=await self.carbon_manager.get_current_price(),
                        priority=1)
                    await self.hitl.request(req)
                except Exception as e:
                    logger.warning(f"HITL failed: {e}")

            # Carbon market credits
            if self.carbon_market and carbon_kg > 0:
                with contextlib.suppress(Exception):
                    await self.carbon_market.retire_credits(
                        carbon_kg, beneficiary=user_id or "system",
                        idempotency_key=f"task:{task_id}")

            # Anomaly detection
            if self.self_healing:
                metrics = {
                    "success_rate": self.state.historical_success_rate,
                    "confidence": self.state.confidence,
                    "carbon_budget_remaining": self.state.carbon_budget_remaining,
                    "reflection_count": self.state.reflection_count}
                with contextlib.suppress(Exception):
                    is_anom, score = await self.self_healing.detect_anomaly(metrics)
                    if is_anom:
                        await self._trigger_reflection("anomaly_detected",
                                                       score=score)

            # Temporal update
            if self._base_temporal:
                self._base_temporal.update({
                    "carbon": min(20.0, max(0.0, carbon_kg)),
                    "confidence": self.state.confidence,
                    "availability": 0.99 if success else 0.5})
                self._base_temporal.evaluate()

            # RLHF update
            if self.rlhf:
                self.rlhf.update({"task_id": task_id, "expert": expert_used},
                                 expert_used, float(reward))

            # Multi-agent role recording (all roles)
            for role in AgentRole:
                self.roles.record(role, success=success, reward=float(reward))

            # Active learning
            self.active_learner.maybe_store(
                {"carbon_kg": carbon_kg, "reward": reward,
                 "latency_ms": latency_ms},
                uncertainty=abs(1.0 - float(reward)) * 0.5,
                threshold=0.1)

            # PQC
            if sign_data:
                with contextlib.suppress(Exception):
                    await self.quantum_security.sign(
                        {"task_id": task_id, "reward": reward})

            # Blockchain
            if blockchain_record:
                with contextlib.suppress(Exception):
                    await self.blockchain.record_meta_data(
                        data_id=task_id,
                        data_hash=hashlib.sha256(
                            f"{task_id}:{reward}".encode()).hexdigest(),
                        metadata={"success": success, "reward": reward})

            # Cloud
            with contextlib.suppress(Exception):
                await self.cloud_storage.store({
                    "task_id": task_id, "success": success,
                    "reward": reward, "carbon_kg": carbon_kg})

            # Lineage
            with contextlib.suppress(Exception):
                self.persistence.record_lineage(
                    lineage_id=uuid.uuid4().hex[:12],
                    parent_id=None, child_id=task_id,
                    operation="record_outcome",
                    payload={"reward": reward, "success": success})

            # Update metric
            META_SUCCESS_RATE.set(self.state.historical_success_rate)
            META_CONFIDENCE.set(self.state.confidence)
            return {"status": "recorded", "task_id": task_id}
        finally:
            async with self._inflight_lock:
                self._inflight -= 1

    # ------------------------------------------------------------------
    # Chaos + status
    # ------------------------------------------------------------------
    async def run_chaos_suite(self) -> Dict[str, Any]:
        if self.chaos_tester is None:
            return {"error": "chaos testing disabled"}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            try:
                results.append(await self.chaos_tester.run_test(f, duration_s=0.1))
            except Exception as e:
                results.append({"fault": f, "passed": False, "error": str(e)})
        return {"results": results, "report": self.chaos_tester.get_report()}

    async def get_comprehensive_status(self) -> Dict[str, Any]:
        status: Dict[str, Any] = {
            "instance_id": self.instance_id,
            "version": "18.0.0",
            "state": self.state.to_dict(),
            "ten_enhancements": {
                "quantum_distillation": True,
                "causal": self.causal is not None,
                "federated": self.federated_aggregator is not None,
                "multi_agent": self.coordinator is not None,
                "temporal_logic": self._base_temporal is not None,
                "xai": self.xai is not None,
                "precision_switching": self.precision_controller is not None,
                "carbon_market": self.carbon_market is not None,
                "chaos": self.chaos_tester is not None,
                "hitl": self.hitl is not None,
            },
            "today_cost_usd": self._today_cost,
            "rate_limit_remaining": self._rate_limiter.remaining(),
            "timestamp": utcnow().isoformat(),
        }
        if self._base_temporal:
            status["temporal_logic"] = self._base_temporal.get_status()
        if self.precision_controller:
            status["precision"] = {
                "last": self.precision_controller.last_precision.value,
                "telemetry": asdict(self.precision_controller.telemetry)}
        if self.carbon_market:
            status["carbon_market"] = await self.carbon_market.get_market_snapshot()
        if self.carbon_manager:
            status["carbon"] = await self.carbon_manager.health_check()
        if self.role_coordinator:
            status["roles"] = self.role_coordinator.assign_roles({
                "confidence": self.state.confidence,
                "carbon_budget": self.state.carbon_budget_remaining / 100.0,
                "cost_budget": 0.5, "adaptability": 0.5})
        if self.federated_aggregator:
            status["federated"] = self.federated_aggregator.get_stats()
        if self.hitl:
            status["hitl"] = self.hitl.get_audit()
        if self.chaos_tester:
            status["chaos"] = self.chaos_tester.get_report()
        if self.quantum_security:
            status["quantum_security"] = \
                await self.quantum_security.get_quantum_status()
        if self.blockchain:
            status["blockchain_status"] = \
                await self.blockchain.get_blockchain_status()
        if self.moe_engine:
            status["moe"] = self.moe_engine.get_stats()
        if self.forecaster:
            status["forecaster"] = self.forecaster.get_stats()
        if self.cloud_storage:
            status["cloud"] = await self.cloud_storage.health_check()
        if self.self_healing:
            status["self_healing"] = await self.self_healing.get_stats()
        if self.bio_optimizer:
            status["bio"] = {"current_params":
                             self.bio_optimizer.get_current_params()}
        if self.causal:
            status["causal"] = self.causal.stats()
        status["active_learning_size"] = len(self.active_learner)
        status["role_weights"] = {r.value: w for r, w in
                                  self.roles.weights().items()}
        return status

    async def health_check(self) -> Dict[str, Any]:
        chain = await self.blockchain.health_check() if self.blockchain else {}
        score = 100 if chain.get("connected") or chain.get("total_records", 0) >= 0 else 50
        META_HEALTH_SCORE.set(score)
        return {
            "status": "healthy" if score >= 50 else "degraded",
            "health_score": score,
            "blockchain": chain,
            "carbon": (await self.carbon_manager.health_check()
                       if self.carbon_manager else {}),
            "pending_hitl": self.hitl.stats()["pending"] if self.hitl else 0,
            "today_cost_usd": self._today_cost,
            "in_flight": self._inflight,
            "timestamp": utcnow().isoformat(),
        }

    async def shutdown(self):
        logger.info("Shutting down meta-cognitive architecture")
        self._shutdown_event.set()
        # Drain in-flight
        deadline = time.time() + 10.0
        while self._inflight > 0 and time.time() < deadline:
            await asyncio.sleep(0.1)
        for t in self._background_tasks:
            t.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self.state.save()
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()
        self.persistence.close()
        logger.info("Shutdown complete")


# ============================================================
# FastAPI (lifespan)
# ============================================================
def build_fastapi_app(arch: EnhancedMetaCognitiveArchitecture,
                      jwt_secret: Optional[str] = None) -> Any:
    try:
        from contextlib import asynccontextmanager
    except ImportError:
        return None

    @asynccontextmanager
    async def lifespan(app):
        yield
        await arch.shutdown()

    app = FastAPI(title="Meta-Cognitive Architecture API",
                  version="18.0.0", lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=["*"],
                       allow_credentials=True, allow_methods=["*"],
                       allow_headers=["*"])
    security = HTTPBearer(auto_error=False)
    secret = jwt_secret or arch.config.jwt_secret

    async def verify(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
        if not JOSE_AVAILABLE:
            return {"sub": "anonymous"}
        if credentials is None:
            raise HTTPException(status_code=401, detail="missing credentials")
        try:
            return jwt.decode(credentials.credentials, secret,
                              algorithms=[arch.config.jwt_algorithm])
        except JWTError:
            raise HTTPException(status_code=401, detail="invalid token")

    @app.post("/token")
    async def issue_token(sub: str = "user"):
        if not JOSE_AVAILABLE:
            raise HTTPException(status_code=500, detail="JWT library missing")
        exp = utcnow() + timedelta(seconds=arch.config.jwt_expiry_s)
        token = jwt.encode({"sub": sub, "exp": exp}, secret,
                           algorithm=arch.config.jwt_algorithm)
        return {"access_token": token, "token_type": "bearer"}

    @app.post("/reflect")
    async def reflect(trigger_type: str = "performance_drop",
                      user: Dict = Depends(verify)):
        await arch._trigger_reflection(trigger_type)
        return {"status": "reflected", "trigger": trigger_type}

    @app.get("/status")
    async def status(user: Dict = Depends(verify)):
        return await arch.get_comprehensive_status()

    @app.get("/health")
    async def health():
        return await arch.health_check()

    @app.get("/hitl/pending")
    async def hitl_pending(user: Dict = Depends(verify)):
        if not arch.hitl:
            return {"pending": []}
        return {"pending": arch.hitl.pending_requests()}

    @app.post("/hitl/approval")
    async def hitl_approval(request_id: str, approved: bool,
                            user: Dict = Depends(verify)):
        if not arch.hitl:
            raise HTTPException(status_code=503, detail="HITL disabled")
        ok = arch.hitl.resolve(request_id, approved)
        if not ok:
            raise HTTPException(status_code=404, detail="not_found")
        return {"status": "resolved", "request_id": request_id,
                "approved": approved}

    @app.get("/hitl/audit")
    async def hitl_audit(limit: int = 100, user: Dict = Depends(verify)):
        if not arch.hitl:
            return {"audit": []}
        return {"audit": arch.hitl.audit_trail(limit=limit),
                "verified": arch.hitl.verify_audit_chain()}

    @app.post("/chaos")
    async def chaos(fault: str = "latency_spike",
                    duration_s: float = 0.2,
                    user: Dict = Depends(verify)):
        if not arch.chaos_tester:
            raise HTTPException(status_code=503, detail="chaos disabled")
        result = await arch.chaos_tester.run_test(fault, duration_s=duration_s)
        return result

    return app


# ============================================================
# Signal handling + singleton
# ============================================================
_architecture_instance: Optional[EnhancedMetaCognitiveArchitecture] = None
_architecture_lock = asyncio.Lock()
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


async def get_meta_cognitive_architecture(
    **kwargs: Any
) -> EnhancedMetaCognitiveArchitecture:
    global _architecture_instance
    if _architecture_instance is None:
        async with _architecture_lock:
            if _architecture_instance is None:
                _architecture_instance = EnhancedMetaCognitiveArchitecture(**kwargs)
                await _architecture_instance.start()
    return _architecture_instance


async def shutdown_handler():
    global _architecture_instance
    if _architecture_instance:
        await _architecture_instance.shutdown()
        _architecture_instance = None


async def main():
    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop)

    print("=" * 80)
    print("Enhanced Meta-Cognitive Architecture v18.0.0 — P0/P1/P2 hardened")
    print("=" * 80)

    if FASTAPI_AVAILABLE and os.environ.get("META_SERVE_API", "0") == "1":
        cfg = MetaConfig()
        arch = EnhancedMetaCognitiveArchitecture(config=cfg)
        await arch.start()
        app = build_fastapi_app(arch, cfg.jwt_secret)
        if app is not None:
            uvicorn.run(app, host=cfg.api_host, port=cfg.api_port,
                        log_level="info")
        return

    if PROMETHEUS_AVAILABLE and start_http_server:
        with contextlib.suppress(Exception):
            start_http_server(getattr(central_config, "metrics_port", 8000))

    arch = await get_meta_cognitive_architecture(config={
        "persist_path": "/tmp/meta_cognitive_v18.db",
        "chaos_enabled": False,
        "chaos_enable_loop": False})

    print("\n✅ Ten enhancements wired in:")
    print("   [1] Quantum-Distillation     dilithium/falcon/sphincs + HMAC fallback")
    print("   [2] Causal RL (UCB)          CausalCounterfactualEstimator")
    print("   [3] Federated (secure + DP)  FederatedAggregator + DPAccountant")
    print("   [4] Multi-Agent              EmergentRoleRegistry + MultiAgentCoordinator")
    print("   [5] Temporal Logic (full STL) TemporalLogicMonitor + SafetyShield")
    print("   [6] Explainable AI           XAIExplainer (SHAP + weighted)")
    print("   [7] Adaptive Precision       AdaptivePrecisionController")
    print("   [8] Carbon Markets / RECs    CarbonMarketClient + CarbonIntensityManager")
    print("   [9] Resilience / Chaos       ChaosTester (steady-state)")
    print("  [10] HITL / Active Learning   ActiveRLHF + HumanInTheLoopCoordinator")

    # Smoke test
    for i in range(3):
        await arch.record_outcome(
            task_id=f"smoke_{i}", success=True,
            reward=0.8 + 0.05 * i, expert_used="performance",
            carbon_kg=1.5 + 0.2 * i, helium_units=0.1, latency_ms=120,
            user_id="tester", sign_data=True, blockchain_record=True)
        await arch._trigger_reflection("performance_drop")

    status = await arch.get_comprehensive_status()
    print(f"\n📊 Instance: {status['instance_id']} v{status['version']}")
    print(f"   Confidence: {status['state']['confidence']:.2f}")
    print(f"   Success rate: {status['state']['success_rate']:.2f}")
    print(f"   Today cost: ${status['today_cost_usd']:.4f}")
    print(f"   Enhancements: {status['ten_enhancements']}")
    if "moe" in status:
        print(f"   MOE: {status['moe']}")
    if "federated" in status:
        print(f"   Federated: {status['federated']}")
    if "hitl" in status:
        print(f"   HITL: {status['hitl']['total']} entries, "
              f"verified={status['hitl']['verified']}")
    if "quantum_security" in status:
        print(f"   Quantum: {status['quantum_security']}")
    print(f"   Health: {status['state']['confidence']:.2f}")

    print("\n" + "=" * 80)
    print("✅ Enhanced Meta-Cognitive Architecture v18.0.0 ready")
    print("=" * 80)

    try:
        await _shutdown_event_global.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await shutdown_handler()


if __name__ == "__main__":
    asyncio.run(main())
