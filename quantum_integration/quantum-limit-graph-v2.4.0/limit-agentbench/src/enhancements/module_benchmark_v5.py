#!/usr/bin/env python3
# src/enhancements/module_benchmark_enhanced_v18_0.py
# VERSION: 18.0.0 — Full Green Agent stack (P0/P1/P2 hardened)
"""
Green Agent Module Benchmark Suite — v18.0.0

Objective convention: all objectives are BENEFITS (higher is better).
TOPSIS.score accepts an optional `cost_criteria` set for mixed problems.

P0 fixes:
 - Real module registry; benchmarks actually execute registered modules
 - No asyncio.get_event_loop() in __init__; explicit async start()
 - MOE gating trained on real labels
 - MODP _update_weights uses a principled reward gradient
 - Pareto/TOPSIS convention consistent
 - Real PQC (dilithium/falcon/sphincs + HMAC-SHA256 fallback)
 - Real Web3 with hash-chained ledger fallback
 - WebSocket server is actually started and stopped
 - Chaos faults exercise the engine paths
 - HITL pending queue + resolve() + hash-chained audit
 - policy_probs compatible with optional subsystems
 - Feedback buffer preserved on RLHF training failure

P1 hardening:
 - SQLite persistence via aiosqlite (async) with a sync fallback
 - Real carbon providers (Electricity Maps / WattTime) with synthetic fallback
 - Token-bucket rate limiting and daily cost budget
 - Health check
 - Portable signal handlers (Windows fallback)
 - atexit cleanup
 - FastAPI + lifespan, JWT from env
 - Structured logging with correlation IDs
 - Graceful shutdown with drain

P2 advanced:
 - Full STL (G / F / U / X / ->) with actionable enforcement
 - Fresh temporal monitor per benchmark run
 - Secure federated aggregation (pairwise masks + DP accountant)
 - Multi-agent message bus + conflict resolution
 - Chaos steady-state hypotheses + auto-deactivation
 - Telemetry-driven precision with EMA quality drop
 - Active learning with KMeans diversity + time-bounded buffer
 - SHAP / weighted XAI
 - Lineage tracking
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
import statistics
import tempfile
import threading
import time
import traceback
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Protocol,
    Sequence, Set, Tuple,
)

import numpy as np

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
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    from sklearn.linear_model import LogisticRegression, LinearRegression, Ridge
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
    import websockets
    from websockets.server import serve as ws_serve  # type: ignore
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

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
    from prometheus_client import Counter, Gauge, CollectorRegistry, start_http_server
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

audit_logger = logging.getLogger("audit")
if not audit_logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    audit_logger.addHandler(h)
    audit_logger.setLevel(logging.INFO)

correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default="unknown")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ============================================================
# Prometheus metrics
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    BENCHMARK_RUNS = Counter("benchmark_runs_total", "Runs", ["status"], registry=REGISTRY)
    BENCHMARK_MODULES = Gauge("benchmark_modules_total", "Modules", registry=REGISTRY)
    BENCHMARK_SCORE = Gauge("benchmark_avg_score", "Avg score", registry=REGISTRY)
    BENCHMARK_DURATION = Gauge("benchmark_duration_seconds", "Duration", registry=REGISTRY)
    CARBON_INTENSITY = Gauge("benchmark_carbon_intensity", "Carbon intensity", registry=REGISTRY)
    CARBON_PRICE = Gauge("benchmark_carbon_price_usd_per_kg", "Carbon price", registry=REGISTRY)
    MODP_PARETO_SIZE = Gauge("benchmark_modp_pareto_front_size", "Pareto size", registry=REGISTRY)
    MOE_GATING_WEIGHTS = Gauge("benchmark_moe_gating_weights", "MOE gating", ["expert"], registry=REGISTRY)
    GA_FITNESS = Gauge("benchmark_ga_fitness", "GA fitness", registry=REGISTRY)
    SELF_HEALING_ACTIONS = Counter("benchmark_self_healing_actions_total", "SH", ["action"], registry=REGISTRY)
    ANOMALY_DETECTIONS = Counter("benchmark_anomaly_detections_total", "AD", ["type"], registry=REGISTRY)
    LIMIT_GRAPH_EDGES = Gauge("benchmark_limit_graph_edges", "Edges", registry=REGISTRY)
    RLHF_REWARD_MODEL_SCORE = Gauge("benchmark_rlhf_reward_model_score", "RLHF", registry=REGISTRY)
    DISTILLATION_LOSS = Gauge("benchmark_distillation_loss", "Distill loss", registry=REGISTRY)
    TEMPORAL_VIOLATIONS = Counter("benchmark_temporal_violations_total", "TV", ["formula"], registry=REGISTRY)
    CHAOS_TESTS = Counter("benchmark_chaos_tests_total", "CT", ["fault", "status"], registry=REGISTRY)
    HITL_DECISIONS = Counter("benchmark_hitl_decisions_total", "HITL", ["decision"], registry=REGISTRY)
    HITL_PENDING = Gauge("benchmark_hitl_pending", "HP", registry=REGISTRY)
    FEDERATED_ROUNDS = Counter("benchmark_federated_rounds_total", "FR", registry=REGISTRY)
    DP_EPSILON = Gauge("benchmark_dp_epsilon_spent", "DPE", ["node"], registry=REGISTRY)
    CARBON_CREDITS_USD = Counter("benchmark_carbon_credits_usd_total", "CC", registry=REGISTRY)
    XAI_EXPLANATIONS = Counter("benchmark_xai_explanations_total", "XAI", registry=REGISTRY)
    PRECISION_SWITCHES = Counter("benchmark_precision_switches_total", "PS", ["level"], registry=REGISTRY)
    COST_USD = Counter("benchmark_cost_usd_total", "Cost", registry=REGISTRY)
    HEALTH_SCORE = Gauge("benchmark_health_score", "Health", registry=REGISTRY)
    ACTIVE_LEARNING = Gauge("benchmark_active_learning_size", "AL", registry=REGISTRY)
    RATE_LIMITED = Counter("benchmark_rate_limited_total", "RL", registry=REGISTRY)
else:
    class _Dummy:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
    BENCHMARK_RUNS = BENCHMARK_MODULES = BENCHMARK_SCORE = _Dummy()
    BENCHMARK_DURATION = CARBON_INTENSITY = CARBON_PRICE = _Dummy()
    MODP_PARETO_SIZE = MOE_GATING_WEIGHTS = GA_FITNESS = _Dummy()
    SELF_HEALING_ACTIONS = ANOMALY_DETECTIONS = LIMIT_GRAPH_EDGES = _Dummy()
    RLHF_REWARD_MODEL_SCORE = DISTILLATION_LOSS = TEMPORAL_VIOLATIONS = _Dummy()
    CHAOS_TESTS = HITL_DECISIONS = HITL_PENDING = _Dummy()
    FEDERATED_ROUNDS = DP_EPSILON = CARBON_CREDITS_USD = XAI_EXPLANATIONS = _Dummy()
    PRECISION_SWITCHES = COST_USD = HEALTH_SCORE = ACTIVE_LEARNING = _Dummy()
    RATE_LIMITED = _Dummy()


# ============================================================
# Exceptions
# ============================================================
class BenchmarkError(Exception): pass
class SyncAsyncBoundaryError(BenchmarkError): pass
class CircuitBreakerOpenError(BenchmarkError): pass
class RateLimitExceeded(BenchmarkError): pass
class PrivacyBudgetExceeded(BenchmarkError): pass
class QuantumError(BenchmarkError): pass
class BlockchainError(BenchmarkError): pass
class ModuleRegistryError(BenchmarkError): pass


# ============================================================
# Enums
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    BF16 = "bf16"
    FP16 = "fp16"
    FP8 = "fp8"
    FP4 = "fp4"


class AgentRole(str, Enum):
    PERFORMANCE_SPECIALIST = "performance_specialist"
    CARBON_SPECIALIST = "carbon_specialist"
    COST_SPECIALIST = "cost_specialist"
    DIVERSITY_SPECIALIST = "diversity_specialist"
    SAFETY_OFFICER = "safety_officer"
    VERIFIER = "verifier"


class STLOperator(str, Enum):
    ALWAYS = "G"
    EVENTUALLY = "F"
    UNTIL = "U"
    NEXT = "X"
    IMPLIES = "->"


PRECISION_ENERGY_PER_OP: Dict[PrecisionLevel, float] = {
    PrecisionLevel.FP32: 1.0e-9,
    PrecisionLevel.BF16: 0.4e-9,
    PrecisionLevel.FP16: 0.4e-9,
    PrecisionLevel.FP8: 0.2e-9,
    PrecisionLevel.FP4: 0.1e-9,
}


# ============================================================
# Config (Pydantic or dataclass)
# ============================================================
if PYDANTIC_AVAILABLE:
    class MODPConfig(BaseModel):
        enabled: bool = True
        method: str = "topsis"
        weights: List[float] = Field(default_factory=lambda: [0.25, 0.25, 0.25, 0.25])
        adaptive_weights: bool = True
        learning_rate: float = 0.01

    class MOEConfig(BaseModel):
        enabled: bool = True
        num_experts: int = 4
        gating_model: str = "logistic"
        update_interval: int = 3600

    class BioConfig(BaseModel):
        enabled: bool = True
        algorithm: str = "ga"
        population_size: int = 20
        max_iterations: int = 50
        mutation_rate: float = 0.1
        crossover_rate: float = 0.8
        seed: int = 0

    class SchedulerConfig(BaseModel):
        enabled: bool = True
        carbon_threshold: float = 400.0
        max_delay_seconds: int = 300
        urgency_importance: float = 0.5
        carbon_importance: float = 0.3
        cost_importance: float = 0.2

    class SelfHealingConfig(BaseModel):
        enabled: bool = True
        anomaly_contamination: float = 0.1
        health_check_interval: int = 60

    class LimitGraphConfig(BaseModel):
        enabled: bool = True
        max_nodes: int = 100
        update_interval: int = 300

    class RLHFConfig(BaseModel):
        enabled: bool = True
        reward_model: str = "linear"
        training_interval: int = 600
        uncertainty_threshold: float = 0.35

    class DistillationConfig(BaseModel):
        enabled: bool = True
        num_teachers: int = 4
        temperature: float = 2.0
        alpha: float = 0.5
        learning_rate: float = 0.01

    class BenchmarkConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="BENCHMARK_", case_sensitive=False)

        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "18.0.0"
        log_level: str = "INFO"
        environment: str = "production"

        db_path: str = "/tmp/benchmark_v18.db"
        persist_path: Optional[str] = None

        carbon_region: str = "global"
        carbon_update_interval: int = Field(300, ge=10)
        carbon_base_price_per_ton: float = 50.0
        rec_price_per_mwh: float = 30.0
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

        metrics_port: int = Field(8000, ge=1024, le=65535)
        websocket_port: int = Field(8770, ge=1024)
        websocket_host: str = "127.0.0.1"
        api_host: str = "0.0.0.0"
        api_port: int = 8000
        jwt_secret: str = ""
        jwt_algorithm: str = "HS256"
        jwt_expiry_s: int = Field(3600, gt=0)

        health_check_interval: int = Field(60, ge=10)
        auto_optimize_interval: int = Field(1800, ge=60)
        ga_evolution_interval: int = Field(3600, ge=60)
        federated_interval: int = Field(3600, ge=60)
        rlhf_interval: int = Field(600, ge=60)
        distillation_interval: int = Field(300, ge=60)
        chaos_interval: int = Field(1800, ge=60)

        rate_limit_per_minute: int = Field(600, ge=1)
        daily_cost_budget_usd: float = Field(10.0, ge=0)
        cost_per_module_usd: float = Field(1e-5, ge=0)

        federated_dp_sigma: float = Field(1e-3, gt=0)
        federated_target_epsilon: float = Field(8.0, gt=0)
        federated_target_delta: float = Field(1e-5, gt=0)

        hitl_timeout_s: float = Field(30.0, gt=0)
        hitl_production_safe: bool = True
        hitl_auto_approve_on_timeout: bool = False
        hitl_confidence_threshold: float = Field(0.65, ge=0.0, le=1.0)

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

        modp: MODPConfig = Field(default_factory=MODPConfig)
        moe: MOEConfig = Field(default_factory=MOEConfig)
        bio: BioConfig = Field(default_factory=BioConfig)
        scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
        self_healing: SelfHealingConfig = Field(default_factory=SelfHealingConfig)
        limit_graph: LimitGraphConfig = Field(default_factory=LimitGraphConfig)
        rlhf: RLHFConfig = Field(default_factory=RLHFConfig)
        distillation: DistillationConfig = Field(default_factory=DistillationConfig)

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

        def get_db_path(self) -> str:
            return self.persist_path or self.db_path
else:
    @dataclass
    class MODPConfig:
        enabled: bool = True
        method: str = "topsis"
        weights: List[float] = field(default_factory=lambda: [0.25, 0.25, 0.25, 0.25])
        adaptive_weights: bool = True
        learning_rate: float = 0.01

    @dataclass
    class MOEConfig:
        enabled: bool = True
        num_experts: int = 4
        gating_model: str = "logistic"
        update_interval: int = 3600

    @dataclass
    class BioConfig:
        enabled: bool = True
        algorithm: str = "ga"
        population_size: int = 20
        max_iterations: int = 50
        mutation_rate: float = 0.1
        crossover_rate: float = 0.8
        seed: int = 0

    @dataclass
    class SchedulerConfig:
        enabled: bool = True
        carbon_threshold: float = 400.0
        max_delay_seconds: int = 300
        urgency_importance: float = 0.5
        carbon_importance: float = 0.3
        cost_importance: float = 0.2

    @dataclass
    class SelfHealingConfig:
        enabled: bool = True
        anomaly_contamination: float = 0.1
        health_check_interval: int = 60

    @dataclass
    class LimitGraphConfig:
        enabled: bool = True
        max_nodes: int = 100
        update_interval: int = 300

    @dataclass
    class RLHFConfig:
        enabled: bool = True
        reward_model: str = "linear"
        training_interval: int = 600
        uncertainty_threshold: float = 0.35

    @dataclass
    class DistillationConfig:
        enabled: bool = True
        num_teachers: int = 4
        temperature: float = 2.0
        alpha: float = 0.5
        learning_rate: float = 0.01

    @dataclass
    class BenchmarkConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "18.0.0"
        log_level: str = "INFO"
        environment: str = "production"
        db_path: str = "/tmp/benchmark_v18.db"
        persist_path: Optional[str] = None
        carbon_region: str = "global"
        carbon_update_interval: int = 300
        carbon_base_price_per_ton: float = 50.0
        rec_price_per_mwh: float = 30.0
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
        metrics_port: int = 8000
        websocket_port: int = 8770
        websocket_host: str = "127.0.0.1"
        api_host: str = "0.0.0.0"
        api_port: int = 8000
        jwt_secret: str = field(default_factory=lambda: os.urandom(32).hex())
        jwt_algorithm: str = "HS256"
        jwt_expiry_s: int = 3600
        health_check_interval: int = 60
        auto_optimize_interval: int = 1800
        ga_evolution_interval: int = 3600
        federated_interval: int = 3600
        rlhf_interval: int = 600
        distillation_interval: int = 300
        chaos_interval: int = 1800
        rate_limit_per_minute: int = 600
        daily_cost_budget_usd: float = 10.0
        cost_per_module_usd: float = 1e-5
        federated_dp_sigma: float = 1e-3
        federated_target_epsilon: float = 8.0
        federated_target_delta: float = 1e-5
        hitl_timeout_s: float = 30.0
        hitl_production_safe: bool = True
        hitl_auto_approve_on_timeout: bool = False
        hitl_confidence_threshold: float = 0.65
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
        modp: MODPConfig = field(default_factory=MODPConfig)
        moe: MOEConfig = field(default_factory=MOEConfig)
        bio: BioConfig = field(default_factory=BioConfig)
        scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
        self_healing: SelfHealingConfig = field(default_factory=SelfHealingConfig)
        limit_graph: LimitGraphConfig = field(default_factory=LimitGraphConfig)
        rlhf: RLHFConfig = field(default_factory=RLHFConfig)
        distillation: DistillationConfig = field(default_factory=DistillationConfig)

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.quantum_master_key)

        def get_db_path(self) -> str:
            return self.persist_path or self.db_path


# ============================================================
# Module Registry (P0)
# ============================================================
@dataclass
class ModuleSpec:
    name: str
    category: str
    run_fn: Callable[[], Awaitable[Dict[str, Any]]]
    metadata: Dict[str, Any] = field(default_factory=dict)


class ModuleRegistry:
    """
    Registry of modules to be benchmarked. Each module exposes an async
    `run_fn` that returns a dict with at least:
        {"accuracy": float in [0,1], "latency_ms": float,
         "memory_mb": float, "cpu_pct": float,
         "throughput_ops": float, "quality": float in [0,1]}
    Additional keys are passed through to metadata.
    """
    def __init__(self):
        self._modules: Dict[str, ModuleSpec] = {}
        self._lock = threading.Lock()

    def register(self, name: str, category: str,
                 run_fn: Callable[[], Awaitable[Dict[str, Any]]],
                 metadata: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            self._modules[name] = ModuleSpec(
                name=name, category=category, run_fn=run_fn,
                metadata=dict(metadata or {}))

    def unregister(self, name: str) -> None:
        with self._lock:
            self._modules.pop(name, None)

    def names(self) -> List[str]:
        with self._lock:
            return list(self._modules.keys())

    def get(self, name: str) -> Optional[ModuleSpec]:
        with self._lock:
            return self._modules.get(name)

    def all_specs(self) -> List[ModuleSpec]:
        with self._lock:
            return list(self._modules.values())


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
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker {self.name} OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.success_count += 1
                    if self.success_count >= 2:
                        self.state = CircuitBreakerState.CLOSED
                        self.failure_count = 0
                else:
                    self.failure_count = 0
            return result
        except Exception:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.state == CircuitBreakerState.CLOSED \
                        and self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                elif self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
            raise

    def get_status(self) -> Dict[str, Any]:
        return {"name": self.name, "state": self.state.value,
                "failure_count": self.failure_count}


# ============================================================
# Persistence (async via aiosqlite with sync fallback)
# ============================================================
class Persistence:
    SCHEMA = """
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS benchmarks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT UNIQUE,
        avg_score REAL, duration_s REAL,
        modules INTEGER, precision TEXT,
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
        payload TEXT, ts REAL NOT NULL
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
                with contextlib.suppress(Exception): self._conn.commit()
                with contextlib.suppress(Exception): self._conn.close()
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

    # --- async-friendly wrappers via thread pool ---
    async def _async_exec(self, sql, params=()):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._exec, sql, params)

    async def _async_fetchall(self, sql, params=()):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._fetchall, sql, params)

    # --- benchmarks ---
    async def save_benchmark(self, run_id: str, avg_score: float,
                             duration_s: float, modules: int, precision: str):
        await self._async_exec(
            "INSERT OR REPLACE INTO benchmarks (run_id, avg_score, duration_s, modules, precision, ts) "
            "VALUES (?,?,?,?,?,?)",
            (run_id, float(avg_score), float(duration_s), int(modules),
             precision, time.time()))

    # --- hitl ---
    async def hitl_append(self, entry: Dict[str, Any]):
        await self._async_exec(
            "INSERT INTO hitl_audit (request_id, approved, reason, mode, latency_ms, prev_hash, entry_hash, ts) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (entry["request_id"], int(bool(entry["approved"])),
             entry.get("reason", ""), entry.get("mode", ""),
             float(entry.get("latency_ms", 0.0)),
             entry.get("prev_hash", ""), entry.get("entry_hash", ""),
             float(entry.get("timestamp", time.time()))))

    async def hitl_recent(self, limit: int = 100) -> List[Tuple]:
        return await self._async_fetchall(
            "SELECT request_id, approved, reason, mode, latency_ms, prev_hash, entry_hash, ts "
            "FROM hitl_audit ORDER BY id DESC LIMIT ?", (limit,))

    def hitl_last_hash(self) -> str:
        rows = self._fetchall(
            "SELECT entry_hash FROM hitl_audit ORDER BY id DESC LIMIT 1")
        return rows[0][0] if rows else ""

    # --- chaos ---
    async def chaos_append(self, fault: str, passed: bool, payload: Dict[str, Any]):
        await self._async_exec(
            "INSERT INTO chaos (fault, passed, payload, ts) VALUES (?,?,?,?)",
            (fault, int(passed), json.dumps(payload, default=str), time.time()))

    # --- federated ---
    async def federated_append(self, round_num: int, weights: List[float],
                               dp_spent: float):
        await self._async_exec(
            "INSERT INTO federated (round, weights, dp_spent, ts) VALUES (?,?,?,?)",
            (round_num, json.dumps(weights), float(dp_spent), time.time()))

    # --- cost ---
    async def cost_record(self, cost_usd: float, note: str = ""):
        await self._async_exec(
            "INSERT INTO cost_ledger (ts, cost_usd, note) VALUES (?,?,?)",
            (time.time(), float(cost_usd), note))

    def cost_today_sync(self) -> float:
        day_start = utcnow().replace(hour=0, minute=0, second=0,
                                     microsecond=0).timestamp()
        rows = self._fetchall(
            "SELECT COALESCE(SUM(cost_usd), 0) FROM cost_ledger WHERE ts >= ?",
            (day_start,))
        return float(rows[0][0]) if rows else 0.0

    # --- lineage ---
    async def record_lineage(self, lineage_id: str, parent_id: Optional[str],
                             child_id: Optional[str], operation: str,
                             payload: Dict[str, Any]):
        await self._async_exec(
            "INSERT INTO lineage (lineage_id, parent_id, child_id, operation, payload, ts) "
            "VALUES (?,?,?,?,?,?)",
            (lineage_id, parent_id, child_id, operation,
             json.dumps(payload, default=str), time.time()))

    # --- carbon ---
    async def append_carbon_price(self, price: float):
        await self._async_exec(
            "INSERT INTO carbon_history (ts, price) VALUES (?,?)",
            (time.time(), float(price)))


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
            return {"pqc_available": PQC_AVAILABLE, "algorithm": self.algorithm,
                    "active_key_id": self._active_key_id,
                    "num_keys": len(self._keys)}


# ============================================================
# Enhancement 1b: Blockchain (real Web3 + ledger)
# ============================================================
class BlockchainVerification:
    def __init__(self, config: BenchmarkConfig, persist: Optional[Persistence] = None):
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
                    logger.warning("Web3 not connected; using local ledger")
                    self.web3 = None
            except Exception as e:
                logger.warning(f"Web3 init failed: {e}")
                self.web3 = None

    async def record(self, data_id: str, data_hash: str,
                     metadata: Dict[str, Any]) -> Dict[str, Any]:
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
                        "chainId": getattr(self.config, "blockchain_chain_id", 1)}
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
# Enhancement 8: Carbon providers + market
# ============================================================
@dataclass
class CarbonSnapshot:
    intensity_g_co2_per_kwh: float
    price_usd_per_kg: float
    source: str
    region: str = "global"
    timestamp: float = field(default_factory=time.time)


class ElectricityMapsProvider:
    def __init__(self, api_key: Optional[str] = None,
                 zone: str = "US-CAL-CISO"):
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
    def __init__(self, config: BenchmarkConfig, persist: Optional[Persistence] = None):
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
        credit_usd = await self.get_carbon_credit_value(amount_kg)
        CARBON_CREDITS_USD.inc(credit_usd)
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
        CARBON_INTENSITY.set(self.current.intensity_g_co2_per_kwh)
        CARBON_PRICE.set(self.current.price_usd_per_kg)
        if self._persist:
            with contextlib.suppress(Exception):
                await self._persist.append_carbon_price(self.current.price_usd_per_kg)
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
                TEMPORAL_VIOLATIONS.labels(formula=f.name).inc()
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

    def screen(self, chosen: Any, safer: Any,
               verdict: Optional[Dict[str, bool]] = None
               ) -> Tuple[Any, bool, Dict[str, bool]]:
        v = verdict if verdict is not None else self.monitor.verify()
        violated = [k for k, ok in v.items() if not ok]
        if not violated:
            return chosen, True, v
        self.violations.append({"t": time.time(), "violated": violated,
                                "original": str(chosen), "replacement": str(safer)})
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

    async def aggregate(self, secure: bool = True) -> Dict[str, Any]:
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
            DP_EPSILON.labels(node=nid).set(self.accountant.spent)
        FEDERATED_ROUNDS.inc()
        if self._persist:
            with contextlib.suppress(Exception):
                await self._persist.federated_append(
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
# Enhancement 4: Multi-agent
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
            AgentRole.PERFORMANCE_SPECIALIST: 1.0,
            AgentRole.CARBON_SPECIALIST: 1.2,
            AgentRole.COST_SPECIALIST: 1.1,
            AgentRole.DIVERSITY_SPECIALIST: 0.9,
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
        XAI_EXPLANATIONS.inc()
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
    def __init__(self):
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
        PRECISION_SWITCHES.labels(level=chosen.value).inc()
        return chosen

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0, PrecisionLevel.BF16: 0.4,
                PrecisionLevel.FP16: 0.4, PrecisionLevel.FP8: 0.2,
                PrecisionLevel.FP4: 0.1}[level]

    def estimate_energy_joules(self, level: PrecisionLevel, ops: float) -> float:
        return PRECISION_ENERGY_PER_OP[level] * ops


# ============================================================
# Role specialization
# ============================================================
class RoleSpecializationCoordinator:
    def __init__(self):
        self.roles = [AgentRole.PERFORMANCE_SPECIALIST, AgentRole.CARBON_SPECIALIST,
                      AgentRole.COST_SPECIALIST, AgentRole.DIVERSITY_SPECIALIST]
        self.affinity = np.array([
            [0.9, 0.2, 0.2, 0.2],
            [0.2, 0.9, 0.3, 0.2],
            [0.2, 0.3, 0.9, 0.2],
            [0.2, 0.2, 0.2, 0.9]])

    def assign_roles(self, context: Dict[str, float]) -> Dict[str, Any]:
        ctx = np.array([context.get("performance", 0.5),
                        context.get("carbon", 0.5),
                        context.get("cost", 0.5),
                        context.get("diversity", 0.5)])
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
    FAULT_TYPES = ["carbon_api_down", "moe_broken", "scheduler_hang",
                   "distiller_broken", "storage_broken"]

    def __init__(self, runner_ref: Optional[Any] = None,
                 hypotheses: Optional[List[SteadyStateHypothesis]] = None,
                 max_concurrent: int = 1, duration_s: float = 0.0,
                 persist: Optional[Persistence] = None):
        self.runner = runner_ref
        self.hypotheses = hypotheses or [
            SteadyStateHypothesis("score_in_range",
                                  lambda m: 0.0 <= m.get("avg_score", 0.5) <= 1.0)]
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
        r = self.runner

        try:
            if fault_type == "carbon_api_down" and r is not None:
                orig = r.carbon_manager._override

                async def broken():
                    raise RuntimeError("carbon API down")
                r.carbon_manager.set_override(broken)
                restore.append(lambda: r.carbon_manager.set_override(orig))
                try:
                    await r.carbon_manager.refresh()
                except Exception:
                    pass

            elif fault_type == "moe_broken" and r is not None and r.moe_selector:
                orig = r.moe_selector.select_modules

                async def broken(*a, **k):
                    raise RuntimeError("moe broken")
                r.moe_selector.select_modules = broken
                restore.append(lambda: setattr(r.moe_selector, "select_modules", orig))
                try:
                    await r.moe_selector.select_modules([], {})
                except Exception:
                    pass

            elif fault_type == "scheduler_hang" and r is not None and r.scheduler:
                orig = r.scheduler.schedule

                async def hung(*a, **k):
                    await asyncio.sleep(10)
                    return {"recommended_delay": 0}
                r.scheduler.schedule = hung
                restore.append(lambda: setattr(r.scheduler, "schedule", orig))
                try:
                    await asyncio.wait_for(r.scheduler.schedule(), timeout=0.1)
                except Exception:
                    pass

            elif fault_type == "distiller_broken" and r is not None and r.distillation:
                orig = r.distillation.distill

                async def broken(*a, **k):
                    raise RuntimeError("distiller broken")
                r.distillation.distill = broken
                restore.append(lambda: setattr(r.distillation, "distill", orig))
                try:
                    await r.distillation.distill({})
                except Exception:
                    pass

            elif fault_type == "storage_broken" and r is not None and r.persistence:
                # Patch the persistence, not the storage, and keep the replacement async
                orig = r.persistence.save_benchmark

                async def broken(*a, **k):
                    raise RuntimeError("storage broken")
                r.persistence.save_benchmark = broken
                restore.append(lambda: setattr(r.persistence, "save_benchmark", orig))
                try:
                    await r.persistence.save_benchmark("chaos", 0.0, 0.0, 0, "fp32")
                except Exception:
                    pass

            if fault_type != "scheduler_hang":
                await asyncio.sleep(min(duration_s, 1.0))

            metrics = {}
            if r is not None and r.benchmark_history:
                last = r.benchmark_history[-1]
                metrics["avg_score"] = (
                    float(np.mean([x.overall_score for x in last.results]) / 100.0)
                    if last.results else 0.0)
            if not all(h.predicate(metrics) for h in self.hypotheses):
                passed = False
                error_msg = "steady_state_violated"
        except Exception as e:
            passed, error_msg = False, str(e)
        finally:
            for rec in restore:
                with contextlib.suppress(Exception):
                    rec()
            with self._lock:
                self._active = max(0, self._active - 1)

        result = {"fault": fault_type, "duration_s": duration_s,
                  "elapsed_s": time.time() - start, "passed": passed,
                  "error": error_msg, "timestamp": utcnow().isoformat()}
        self.results.append(result)
        CHAOS_TESTS.labels(fault=fault_type,
                           status="pass" if passed else "fail").inc()
        if self.persist:
            with contextlib.suppress(Exception):
                await self.persist.chaos_append(fault_type, passed, result)
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
        self.feedback_buffer: List[Dict[str, Any]] = []
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

    def record_feedback(self, state: Dict, action: str, reward: float):
        with self._lock:
            self.feedback_buffer.append(
                {"state": dict(state), "action": action, "reward": float(reward)})
        self.update(state, action, reward)

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
        HITL_PENDING.set(len(self.pending_queries))
        return query

    def resolve_query(self, query_id: str, chosen: str,
                      rating: float = 1.0) -> Optional[Dict[str, Any]]:
        with self._lock:
            q = self.pending_queries.pop(query_id, None)
            HITL_PENDING.set(len(self.pending_queries))
        if q is None:
            return None
        q.update({"status": "resolved", "chosen": chosen, "rating": rating})
        self.update(q["context"], chosen, rating)
        return q

    async def train_reward_model(self) -> Dict[str, Any]:
        with self._lock:
            buf = list(self.feedback_buffer)
        if len(buf) < 5:
            return {"trained": False, "samples": len(buf)}
        X, y = [], []
        for f in buf:
            s = f["state"]
            X.append([float(s.get("carbon_intensity", 400.0)) / 1000.0,
                      float(s.get("avg_score", 0.5)),
                      float(s.get("cost", 0.5)),
                      float(s.get("diversity", 0.5))])
            y.append(float(f["reward"]))
        X = np.asarray(X)
        y = np.asarray(y)
        if not SKLEARN_AVAILABLE or len(X) < 5:
            # Keep buffer if we can't train
            return {"trained": False, "reason": "insufficient_sklearn"}
        try:
            model = LinearRegression().fit(X, y)
            score = float(model.score(X, y)) if len(X) > 1 else 0.0
            RLHF_REWARD_MODEL_SCORE.set(float(np.mean(y)))
            with self._lock:
                self.feedback_buffer.clear()
            return {"trained": True, "r2": score, "samples": len(X)}
        except Exception as e:
            logger.warning(f"RLHF train failed: {e}")
            # Preserve the buffer for the next round
            return {"trained": False, "reason": str(e)}

    async def get_policy_probs(self, state: Dict) -> List[float]:
        return self._policy(state).tolist()


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
        HITL_PENDING.set(0)

    async def _audit_append(self, entry: HITLAuditEntry):
        entry.prev_hash = self._last_hash
        entry.entry_hash = entry.compute_hash()
        self._last_hash = entry.entry_hash
        self._audit.append(entry)
        if self._persist:
            with contextlib.suppress(Exception):
                await self._persist.hitl_append(entry.to_dict())

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
            await self._audit_append(HITLAuditEntry(
                req.request_id, False, req.reason, "blocked_no_approver",
                (time.time() - start) * 1000.0))
            HITL_DECISIONS.labels(decision="blocked").inc()
            return False
        if approver is None:
            await self._audit_append(HITLAuditEntry(
                req.request_id, True, req.reason, "auto_no_approver",
                (time.time() - start) * 1000.0))
            HITL_DECISIONS.labels(decision="auto_approved").inc()
            return True
        with self._lock:
            if len(self._pending) >= self.max_pending:
                await self._audit_append(HITLAuditEntry(
                    req.request_id, False, req.reason, "queue_full",
                    (time.time() - start) * 1000.0))
                HITL_DECISIONS.labels(decision="queue_full").inc()
                return False
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            self._pending[req.request_id] = req
            self._futures[req.request_id] = future
            HITL_PENDING.set(len(self._pending))

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
                HITL_PENDING.set(len(self._pending))
            if fut is not None and not fut.done():
                with contextlib.suppress(Exception):
                    fut.set_result(bool(approved))

        latency_ms = (time.time() - start) * 1000.0
        await self._audit_append(HITLAuditEntry(
            req.request_id, approved, req.reason, mode, latency_ms))
        HITL_DECISIONS.labels(decision="approved" if approved else "rejected").inc()
        return bool(approved)

    async def escalate(self, decision_context: Dict[str, Any],
                       options: List[str], confidence: float,
                       confidence_threshold: float = 0.65) -> Dict[str, Any]:
        needs_human = confidence < confidence_threshold
        query = await self.rlhf.maybe_query_human(decision_context, options)
        if query is None and not needs_human:
            choice = self.rlhf.sample_action(decision_context)
            return {"escalated": False, "chosen": choice, "source": "auto"}
        if query is None:
            # Build a synthetic request and block on it with timeout
            req = HITLRequest.new(
                reason="low_confidence",
                chosen_idx=0,
                candidates=[{"option": o} for o in options],
                uncertainty=1.0 - confidence,
                carbon_price=float(decision_context.get("carbon_price", 0.0)),
                priority=1)
            approved = await self.request(req)
            choice = options[0] if approved else self.rlhf.sample_action(decision_context)
            return {"escalated": True, "chosen": choice,
                    "source": "human" if approved else "human_rejected"}
        # A query was created; wait for its future
        auto_choice = self.rlhf.sample_action(decision_context)
        return {"escalated": True, "query": query,
                "chosen": auto_choice, "source": "human_pending"}

    async def resolve(self, request_id: str, approved: bool) -> bool:
        with self._lock:
            fut = self._futures.get(request_id)
        if fut is None or fut.done():
            return False
        with contextlib.suppress(Exception):
            fut.set_result(bool(approved))
        await self._audit_append(HITLAuditEntry(
            request_id, approved, "external_resolve", "resolved"))
        HITL_DECISIONS.labels(decision="approved" if approved else "rejected").inc()
        return True

    async def audit_trail(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        if self._persist:
            rows = await self._persist.hitl_recent(limit or 1000)
            return [{"request_id": r[0], "approved": bool(r[1]),
                     "reason": r[2], "mode": r[3], "latency_ms": r[4],
                     "prev_hash": r[5], "entry_hash": r[6],
                     "timestamp": r[7]} for r in rows]
        entries = list(self._audit)
        if limit is not None:
            entries = entries[-limit:]
        return [e.to_dict() for e in entries]

    async def verify_audit_chain(self) -> bool:
        entries = await self.audit_trail(limit=None)
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
        ACTIVE_LEARNING.set(0)

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
        ACTIVE_LEARNING.set(size)
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
# Domain modules
# ============================================================
class GeneticAlgorithmOptimizer:
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1,
                 crossover_rate: float = 0.8, seed: int = 0):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Dict[str, float]] = []
        self.bounds = {
            "performance_weight": (0.0, 1.0),
            "carbon_weight": (0.0, 1.0),
            "cost_weight": (0.0, 1.0),
            "diversity_weight": (0.0, 1.0)}
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
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.ga = GeneticAlgorithmOptimizer(
            population_size=config.bio.population_size,
            mutation_rate=config.bio.mutation_rate,
            crossover_rate=config.bio.crossover_rate,
            seed=config.bio.seed)
        self.current_params = {"performance_weight": 0.25, "carbon_weight": 0.25,
                               "cost_weight": 0.25, "diversity_weight": 0.25}
        self.fitness_history: deque = deque(maxlen=50)
        self._lock = asyncio.Lock()

    def _fitness(self, params: Dict[str, float]) -> float:
        return (params["performance_weight"] * 0.4
                + (1 - params["carbon_weight"]) * 0.3
                + (1 - params["cost_weight"]) * 0.2
                + params["diversity_weight"] * 0.1)

    async def evolve(self) -> Dict[str, float]:
        best = self.ga.evolve(self._fitness, generations=5)
        async with self._lock:
            self.current_params = best
            self.fitness_history.append(self._fitness(best))
        GA_FITNESS.set(self._fitness(best))
        return best

    def get_current_params(self) -> Dict[str, float]:
        return dict(self.current_params)


class MODPStrategyOptimizer:
    FEATURES = ["performance", "carbon", "cost", "diversity"]

    def __init__(self, config: BenchmarkConfig,
                 xai: Optional[XAIExplainer] = None,
                 roles: Optional[RoleSpecializationCoordinator] = None,
                 temporal: Optional[TemporalLogicMonitor] = None):
        self.config = config
        self.candidates = [
            {"name": "performance_focus",
             "performance": 0.8, "carbon": 0.2, "cost": 0.1, "diversity": 0.1},
            {"name": "carbon_focus",
             "performance": 0.2, "carbon": 0.9, "cost": 0.3, "diversity": 0.3},
            {"name": "cost_focus",
             "performance": 0.2, "carbon": 0.3, "cost": 0.9, "diversity": 0.2},
            {"name": "balanced",
             "performance": 0.6, "carbon": 0.6, "cost": 0.6, "diversity": 0.6},
        ]
        self.weights = list(config.modp.weights)
        self.adaptive_weights = config.modp.adaptive_weights
        self.learning_rate = config.modp.learning_rate
        self.recent_outcomes: deque = deque(maxlen=100)
        self.xai = xai
        self.roles = roles
        self.temporal = temporal

    def _benefit_vector(self, cand: Dict[str, Any],
                        carbon_intensity: float) -> Dict[str, float]:
        """All objectives are benefits (higher is better)."""
        ci_factor = min(1.0, max(0.0, carbon_intensity / 800.0))
        carbon_benefit = (1.0 - cand["carbon"]) * (1.0 - ci_factor) + cand["carbon"] * 0.1
        return {
            "performance": float(cand["performance"]),
            "carbon": float(carbon_benefit),
            "cost": 1.0 - float(cand["cost"]),
            "diversity": float(cand["diversity"])}

    async def select_strategy(self, state: Dict[str, Any]) -> Dict[str, Any]:
        carbon_intensity = float(state.get("carbon_intensity", 400.0))
        if self.temporal:
            self.temporal.observe({
                "carbon_intensity": carbon_intensity,
                "avg_score": float(state.get("average_score", 0.5))})
            verdict = self.temporal.verify()
        else:
            verdict = {}

        benefit_vecs = [self._benefit_vector(c, carbon_intensity)
                        for c in self.candidates]
        scores = TOPSIS.score(benefit_vecs, self.weights, self.FEATURES,
                              cost_criteria=None)
        scores_arr = np.asarray(scores) if scores else np.zeros(len(benefit_vecs))
        best_idx = int(np.argmax(scores_arr))

        front = ParetoFront()
        for i, v in enumerate(benefit_vecs):
            front.add([v[k] for k in self.FEATURES], self.candidates[i]["name"])

        explanation = None
        if self.xai:
            explanation = self.xai.explain(
                candidate=benefit_vecs[best_idx],
                weights=dict(zip(self.FEATURES, self.weights)),
                all_candidates=benefit_vecs)

        roles = None
        if self.roles:
            roles = self.roles.assign_roles(benefit_vecs[best_idx])

        outcome = np.asarray([benefit_vecs[best_idx][k] for k in self.FEATURES])
        self.recent_outcomes.append((list(self.weights), outcome))
        if self.adaptive_weights and len(self.recent_outcomes) >= 10:
            self._update_weights()

        MODP_PARETO_SIZE.set(len(front.get_pareto_front()))
        return {"action": "modp_optimization",
                "strategy": self.candidates[best_idx]["name"],
                "weights_used": list(self.weights),
                "scores": scores,
                "pareto_front_size": len(front.get_pareto_front()),
                "xai_explanation": explanation,
                "role_assignments": roles,
                "temporal_status": verdict,
                "recommendation": f"Selected {self.candidates[best_idx]['name']}"}

    def _update_weights(self):
        outcomes = np.array([o for _, o in self.recent_outcomes])
        mean = outcomes.mean(axis=0)
        std = outcomes.std(axis=0) + 1e-9
        grad = mean / std
        w = np.array(self.weights) + self.learning_rate * grad
        w = np.clip(w, 0.05, None)
        w /= w.sum()
        self.weights = w.tolist()


class MOEBenchmarkSelector:
    """MOE gating is trained on real labels (P0 fix)."""
    def __init__(self, config: BenchmarkConfig,
                 roles: Optional[RoleSpecializationCoordinator] = None,
                 xai: Optional[XAIExplainer] = None):
        self.config = config
        self.roles = roles
        self.xai = xai
        self.teacher_names = ["performance", "carbon", "cost", "adaptive"]
        self.gating_model = None
        self.scaler = None
        self.history_context: deque = deque(maxlen=500)
        self.history_labels: deque = deque(maxlen=500)
        self._trained = False
        if SKLEARN_AVAILABLE:
            try:
                self.gating_model = LogisticRegression(max_iter=1000)
                self.scaler = StandardScaler()
            except Exception:
                pass

    def _teacher_scores(self, modules: List[str],
                        features: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
        ci = float(features.get("carbon_intensity", 400.0))
        historical = float(features.get("historical_score", 0.5))
        cost = float(features.get("cost", 0.5))
        diversity = float(features.get("diversity", 0.5))

        perf = {m: 0.5 + 0.3 * historical for m in modules}
        carb = {m: 0.5 * (1.0 - ci / 1000.0) for m in modules}
        cst = {m: 0.5 * (1.0 - cost) for m in modules}
        adapt = {m: 0.5 * diversity + 0.3 * historical for m in modules}
        return {"performance": perf, "carbon": carb,
                "cost": cst, "adaptive": adapt}

    def _extract_context(self, features: Dict[str, Any]) -> np.ndarray:
        return np.array([
            float(features.get("carbon_intensity", 400.0)) / 1000.0,
            float(features.get("historical_score", 0.5)),
            float(features.get("cost", 0.5)),
            float(features.get("diversity", 0.5))])

    def _gating_weights(self, features: Dict[str, Any]) -> np.ndarray:
        n = len(self.teacher_names)
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

    async def select_modules(self, all_modules: List[str],
                             features: Dict[str, Any]) -> Dict[str, Any]:
        if not all_modules:
            return {"selected_modules": [], "teacher_scores": {},
                    "gating_weights": {}, "role_assignments": None,
                    "xai_explanation": None}
        teacher_scores = self._teacher_scores(all_modules, features)
        weights = self._gating_weights(features)
        module_scores: Dict[str, float] = defaultdict(float)
        for i, name in enumerate(self.teacher_names):
            scores = teacher_scores.get(name, {})
            for mod, s in scores.items():
                module_scores[mod] += weights[i] * float(s)
        ranked = sorted(module_scores.items(), key=lambda kv: kv[1], reverse=True)
        selected = [m for m, _ in ranked[:5]]

        for i, name in enumerate(self.teacher_names):
            MOE_GATING_WEIGHTS.labels(expert=name).set(float(weights[i]))

        roles = None
        if self.roles:
            roles = self.roles.assign_roles({
                "performance": float(weights[0]),
                "carbon": float(weights[1]),
                "cost": float(weights[2]),
                "diversity": float(weights[3])})

        xai_out = None
        if self.xai and selected:
            top = selected[0]
            all_dicts = [{"score": float(module_scores[m])} for m in all_modules]
            xai_out = self.xai.explain(
                candidate={"score": float(module_scores[top])},
                weights={"score": 1.0},
                all_candidates=all_dicts)

        return {"selected_modules": selected,
                "teacher_scores": teacher_scores,
                "gating_weights": dict(zip(self.teacher_names, weights.tolist())),
                "role_assignments": roles,
                "xai_explanation": xai_out}

    async def update_gating(self, features: Dict[str, Any],
                            module_scores: Dict[str, float]):
        """P0 fix: label the best-matching teacher based on real outcomes."""
        teacher_scores = self._teacher_scores(list(module_scores.keys()), features)
        # Build actual module ranking
        actual = np.asarray([float(module_scores[m])
                             for m in module_scores.keys()])
        if len(actual) < 2:
            return
        best_teacher_idx = 0
        best_corr = -2.0
        for i, name in enumerate(self.teacher_names):
            scores = teacher_scores.get(name, {})
            t_vec = np.asarray([float(scores.get(m, 0.5))
                                for m in module_scores.keys()])
            try:
                corr = float(np.corrcoef(t_vec, actual)[0, 1])
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
        if self.gating_model is None:
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
        return {"num_teachers": len(self.teacher_names),
                "gating_trained": self._trained,
                "history_len": len(self.history_context)}


class MultiObjectiveCarbonScheduler:
    def __init__(self, carbon_manager: CarbonIntensityManager,
                 market: Optional[CarbonMarketClient] = None,
                 threshold: float = 400.0, max_delay_s: int = 300):
        self.carbon_manager = carbon_manager
        self.market = market
        self.threshold = float(threshold)
        self.max_delay = int(max_delay_s)

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
        return {"recommended_delay": best_delay, "reason": "multi_objective",
                "intensity": intensity, "market": market}


class SelfHealingManager:
    def __init__(self, config: BenchmarkConfig,
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
            float(metrics.get("avg_score", 0.5)),
            float(metrics.get("carbon_intensity", 400.0)) / 1000.0,
            float(metrics.get("module_count", 0)) / 100.0,
            float(metrics.get("duration_seconds", 0.0)) / 1000.0]).reshape(1, -1)

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
            ANOMALY_DETECTIONS.labels(type="self_healing").inc()
            with self._lock:
                self.actions.append({"action": "recovery",
                                     "timestamp": utcnow().isoformat()})
        return score > 0.5, float(score)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {"trained": self._trained,
                    "num_detectors": len(self.detectors),
                    "recent_actions": list(self.actions)[-5:],
                    "chaos_active": self.chaos is not None}


# ============================================================
# WebSocket Server (P0: actually started)
# ============================================================
class WebSocketServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 8770):
        self.host = host
        self.port = int(port)
        self._clients: Set[Any] = set()
        self._server = None
        self._lock = asyncio.Lock()

    async def _handler(self, ws, path=None):
        try:
            async with self._lock:
                self._clients.add(ws)
            async for _ in ws:
                # Ignore inbound messages for now
                pass
        finally:
            async with self._lock:
                self._clients.discard(ws)

    async def start(self):
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("websockets not available; WebSocket server disabled")
            return
        try:
            self._server = await ws_serve(self._handler,
                                          self.host, self.port)
            logger.info(f"WebSocket server listening on ws://{self.host}:{self.port}")
        except Exception as e:
            logger.warning(f"WebSocket server failed to start: {e}")
            self._server = None

    async def stop(self):
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
            self._server = None
        async with self._lock:
            for ws in list(self._clients):
                with contextlib.suppress(Exception):
                    await ws.close()
            self._clients.clear()

    async def broadcast(self, message: Dict[str, Any], topic: str = "default"):
        if not self._clients:
            return
        payload = json.dumps({"topic": topic, **message}, default=str)
        async with self._lock:
            clients = list(self._clients)
        for ws in clients:
            try:
                await ws.send(payload)
            except Exception:
                async with self._lock:
                    self._clients.discard(ws)


# ============================================================
# Data classes
# ============================================================
@dataclass
class BenchmarkResult:
    module_name: str
    category: str
    accuracy_score: float
    performance_score: float
    precision_score: float
    latency_ms: float
    integration_score: float
    overall_score: float
    memory_usage_mb: float
    cpu_usage_pct: float
    p95_latency_ms: float
    throughput_ops_per_sec: float
    data_quality_score: float
    synthetic: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class BenchmarkRun:
    run_id: str
    results: List[BenchmarkResult]
    system_info: Dict[str, Any]
    git_commit: str
    version: str
    data_quality_score: float
    duration_seconds: float
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_distribution: Optional[Dict] = None
    autonomous_optimization: Optional[Dict] = None
    temporal_status: Optional[Dict] = None
    xai_explanation: Optional[Dict] = None
    precision_used: Optional[str] = None
    carbon_credit_value_usd: Optional[float] = None
    rec_value_usd: Optional[float] = None
    role_assignments: Optional[Dict] = None
    chaos_test_passed: Optional[bool] = None
    hitl_outcome: Optional[Dict] = None
    federated_round: Optional[int] = None
    causal_action: Optional[int] = None
    counterfactuals: Optional[Dict[int, float]] = None
    timestamp: datetime = field(default_factory=utcnow)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if isinstance(d.get("timestamp"), datetime):
            d["timestamp"] = d["timestamp"].isoformat()
        return d


# ============================================================
# Main benchmark runner
# ============================================================
class EnhancedBenchmarkRunnerV11:
    def __init__(self, config: Optional[BenchmarkConfig] = None,
                 registry: Optional[ModuleRegistry] = None,
                 hitl_approver: Optional[Callable[[HITLRequest], bool]] = None):
        if PYDANTIC_AVAILABLE and isinstance(config, dict):
            self.config = BenchmarkConfig(**config)
        elif config is None:
            self.config = BenchmarkConfig()
        else:
            self.config = config
        self.instance_id = self.config.instance_id
        self._start_time = utcnow()
        self.registry = registry or ModuleRegistry()

        # Persistence
        self.persistence = Persistence(self.config.get_db_path())
        self._today_cost = self.persistence.cost_today_sync()

        # Rate + cost
        self._rate_limiter = TokenBucket(
            rate=getattr(self.config, "rate_limit_per_minute", 600),
            per_seconds=60.0)
        self._daily_budget = float(getattr(self.config, "daily_cost_budget_usd", 10.0))
        self._cost_per_module = float(getattr(self.config, "cost_per_module_usd", 1e-5))

        # Quantum + blockchain
        self.quantum = (QuantumSecurity(
            algorithm=getattr(self.config, "quantum_algorithm", "dilithium"),
            master_key=self.config.get_master_key_bytes(),
            rotation_s=getattr(self.config, "quantum_key_rotation_s", 3600))
            if getattr(self.config, "quantum_enabled", True) else None)
        self.blockchain = (BlockchainVerification(self.config, self.persistence)
                           if getattr(self.config, "blockchain_enabled", True) else None)

        # Carbon
        self.carbon_market = (CarbonMarketClient(self.config, self.persistence)
                              if getattr(self.config, "carbon_market_enabled", True)
                              else None)
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
        self._base_temporal = (TemporalLogicMonitor(horizon=10)
                               if getattr(self.config, "temporal_logic_enabled", True)
                               else None)
        if self._base_temporal:
            self._base_temporal.add_formula(STLFormula(
                name="carbon_cap",
                predicate=lambda r: r.get("carbon_intensity", 0.0) <= 800.0,
                operator=STLOperator.ALWAYS, horizon=10))
            self._base_temporal.add_formula(STLFormula(
                name="score_min",
                predicate=lambda r: r.get("avg_score", 1.0) >= 0.3,
                operator=STLOperator.ALWAYS, horizon=10))
        self.shield = SafetyShield(self._base_temporal) if self._base_temporal else None

        # XAI
        self.xai = (XAIExplainer(["performance", "carbon", "cost", "diversity"])
                    if getattr(self.config, "xai_enabled", True) else None)

        # Precision
        self.precision_controller = (AdaptivePrecisionController()
                                     if getattr(self.config, "adaptive_precision_enabled", True)
                                     else None)

        # Roles + multi-agent
        self.role_coordinator = (RoleSpecializationCoordinator()
                                 if getattr(self.config, "role_specialization_enabled", True)
                                 else None)
        self.roles = (EmergentRoleRegistry()
                      if getattr(self.config, "multi_agent_enabled", True) else None)
        self.coordinator = MultiAgentCoordinator(self.roles) if self.roles else None

        # RLHF + HITL
        self.rlhf = (ActiveRLHF(
            action_space=["performance_focus", "carbon_focus",
                          "cost_focus", "balanced"],
            uncertainty_threshold=getattr(self.config.rlhf, "uncertainty_threshold", 0.35))
            if getattr(self.config.rlhf, "enabled", True) else None)
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
        self.causal = (CausalCounterfactualEstimator(state_dim=5, n_actions=4)
                       if getattr(self.config, "causal_enabled", True) else None)
        self.last_counterfactuals: Dict[int, float] = {}

        # MODP + MOE
        self.modp_optimizer = MODPStrategyOptimizer(
            self.config, xai=self.xai, roles=self.role_coordinator,
            temporal=self._base_temporal) \
            if getattr(self.config.modp, "enabled", True) else None
        self.moe_selector = MOEBenchmarkSelector(
            self.config, roles=self.role_coordinator, xai=self.xai) \
            if getattr(self.config.moe, "enabled", True) else None

        # Bio
        self.bio = (BioOptimizer(self.config)
                    if getattr(self.config.bio, "enabled", True) else None)

        # Scheduler
        self.scheduler = (MultiObjectiveCarbonScheduler(
            self.carbon_manager, self.carbon_market)
            if getattr(self.config.scheduler, "enabled", True) else None)

        # Self-healing
        self.self_healing = (SelfHealingManager(self.config, self.rlhf)
                             if getattr(self.config.self_healing, "enabled", True)
                             else None)

        # Active learning
        self.active_learner = ActiveLearningSampler(seed=getattr(self.config.bio, "seed", 0))

        # Chaos
        hypotheses = [
            SteadyStateHypothesis("score_in_range",
                                  lambda m: 0.0 <= m.get("avg_score", 0.5) <= 1.0)]
        self.chaos = (ChaosTester(
            runner_ref=self,
            hypotheses=hypotheses,
            max_concurrent=getattr(self.config, "chaos_max_concurrent", 1),
            duration_s=getattr(self.config, "chaos_duration_s", 0.0),
            persist=self.persistence)
            if getattr(self.config, "chaos_testing_enabled", True) else None)

        # WebSocket
        self.websocket = WebSocketServer(
            host=getattr(self.config, "websocket_host", "127.0.0.1"),
            port=getattr(self.config, "websocket_port", 8770))

        # Circuit breaker
        self.breaker = CircuitBreaker("benchmark")

        # State
        self.benchmark_history: deque = deque(maxlen=1000)
        self._history_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._background_tasks: List[asyncio.Task] = []
        self._running = False
        self._inflight = 0
        self._inflight_lock = asyncio.Lock()

        if PROMETHEUS_AVAILABLE and start_http_server:
            with contextlib.suppress(Exception):
                start_http_server(getattr(self.config, "metrics_port", 8000))

        logger.info(f"EnhancedBenchmarkRunnerV11 v{self.config.version} "
                    f"(instance {self.instance_id})")
        atexit.register(self._atexit_cleanup)

    def _atexit_cleanup(self):
        with contextlib.suppress(Exception):
            self._shutdown_event.set()
        with contextlib.suppress(Exception):
            self.persistence.close()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        if self._running:
            return
        self._running = True
        await self.websocket.start()
        loop = asyncio.get_running_loop()
        self._background_tasks.append(loop.create_task(self._carbon_loop()))
        self._background_tasks.append(loop.create_task(self._ga_loop()))
        self._background_tasks.append(loop.create_task(self._rlhf_loop()))
        self._background_tasks.append(loop.create_task(self._federated_loop()))
        if self.chaos and getattr(self.config, "chaos_enable_loop", False):
            self._background_tasks.append(loop.create_task(self._chaos_loop()))
        logger.info("Benchmark runner started")

    async def _carbon_loop(self):
        while not self._shutdown_event.is_set():
            with contextlib.suppress(Exception):
                await self.carbon_manager.refresh()
            try:
                await asyncio.wait_for(self._shutdown_event.wait(),
                                       timeout=getattr(self.config, "carbon_update_interval", 300))
            except asyncio.TimeoutError:
                pass

    async def _ga_loop(self):
        while not self._shutdown_event.is_set():
            if self.bio:
                with contextlib.suppress(Exception):
                    await self.bio.evolve()
            try:
                await asyncio.wait_for(self._shutdown_event.wait(),
                                       timeout=getattr(self.config, "ga_evolution_interval", 3600))
            except asyncio.TimeoutError:
                pass

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            if self.rlhf:
                with contextlib.suppress(Exception):
                    await self.rlhf.train_reward_model()
            try:
                await asyncio.wait_for(self._shutdown_event.wait(),
                                       timeout=getattr(self.config, "rlhf_interval", 600))
            except asyncio.TimeoutError:
                pass

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            with contextlib.suppress(Exception):
                if self.federated and self.federated._pending:
                    await self.federated.aggregate()
            try:
                await asyncio.wait_for(self._shutdown_event.wait(),
                                       timeout=getattr(self.config, "federated_interval", 3600))
            except asyncio.TimeoutError:
                pass

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            if self.chaos:
                with contextlib.suppress(Exception):
                    fault = random.choice(ChaosTester.FAULT_TYPES)
                    await self.chaos.run_test(fault, duration_s=0.1)
            try:
                await asyncio.wait_for(self._shutdown_event.wait(),
                                       timeout=getattr(self.config, "chaos_interval", 1800))
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------
    # Benchmark execution
    # ------------------------------------------------------------------
    async def run_benchmarks(self, module_names: Optional[List[str]] = None,
                             iterations: int = 1,
                             sign_results: bool = True,
                             blockchain_record: bool = True
                             ) -> BenchmarkRun:
        async with self._inflight_lock:
            self._inflight += 1
        try:
            return await self._run_inner(module_names, iterations,
                                         sign_results, blockchain_record)
        finally:
            async with self._inflight_lock:
                self._inflight -= 1

    async def _run_inner(self, module_names, iterations,
                         sign_results, blockchain_record) -> BenchmarkRun:
        run_id = uuid.uuid4().hex[:12]
        correlation_id_var.set(run_id)
        start_time = time.time()

        if not self._rate_limiter.acquire():
            RATE_LIMITED.inc()
            raise RateLimitExceeded("Rate limit exceeded")
        if self._daily_budget > 0 and self._today_cost >= self._daily_budget:
            logger.warning("Daily cost budget exhausted")
            BENCHMARK_RUNS.labels(status="budget_exceeded").inc()
            raise BenchmarkError("Daily cost budget exhausted")

        # 1. Carbon refresh + scheduler
        try:
            snap = await self.carbon_manager.refresh()
            carbon_intensity = snap.intensity_g_co2_per_kwh
        except Exception as e:
            logger.warning(f"Carbon refresh failed: {e}")
            carbon_intensity = 400.0

        if self.scheduler:
            with contextlib.suppress(Exception):
                sched = await self.scheduler.schedule(urgency_score=0.5)
                if sched["recommended_delay"] > 0:
                    await asyncio.sleep(min(sched["recommended_delay"], 2))

        # 2. Precision
        precision = PrecisionLevel.FP32
        if self.precision_controller:
            precision = self.precision_controller.select(
                float(carbon_intensity), accuracy_required=0.95)

        # 3. Module discovery / selection
        all_modules = self.registry.names()
        if not all_modules:
            logger.warning("Module registry is empty; benchmarks will be simulated")
        if module_names is None:
            module_names = all_modules

        # 4. MOE gating (real labels will be added after execution)
        features = {
            "carbon_intensity": carbon_intensity,
            "historical_score": 0.5,
            "cost": 0.5,
            "diversity": 0.5}
        moe_result = None
        if self.moe_selector and module_names:
            moe_result = await self.moe_selector.select_modules(module_names, features)
            module_names = moe_result["selected_modules"]

        # 5. Temporal monitor per run (P0 fix: fresh)
        temporal = self._base_temporal.fresh() if self._base_temporal else None
        temporal.observe({"carbon_intensity": float(carbon_intensity),
                          "avg_score": 0.5}) if temporal else None
        temporal_status = temporal.verify() if temporal else {}

        # 6. Execute benchmarks
        aggregated: List[BenchmarkResult] = []
        module_scores: Dict[str, float] = {}
        for it in range(max(1, iterations)):
            for name in module_names:
                spec = self.registry.get(name)
                if spec is None:
                    # Unknown module; produce a synthetic result but tag it
                    aggregated.append(self._synthetic_result(name, synthetic=True))
                    continue
                try:
                    raw = await asyncio.wait_for(spec.run_fn(), timeout=30.0)
                except Exception as e:
                    logger.warning(f"Module {name} failed: {e}")
                    raw = {"error": str(e), "accuracy": 0.0, "latency_ms": 0.0,
                           "memory_mb": 0.0, "cpu_pct": 0.0,
                           "throughput_ops": 0.0, "quality": 0.0}
                result = self._build_result(name, spec.category, raw)
                aggregated.append(result)
                module_scores[name] = result.overall_score / 100.0

        # 7. Train MOE gating with real labels (P0 fix)
        if self.moe_selector and module_scores:
            with contextlib.suppress(Exception):
                await self.moe_selector.update_gating(features, module_scores)

        # 8. Aggregate
        aggregated = await self._aggregate_results(aggregated)
        avg_score = (float(np.mean([r.overall_score for r in aggregated]))
                     if aggregated else 0.0)

        # 9. MODP
        optimization = None
        if self.modp_optimizer:
            optimization = await self.modp_optimizer.select_strategy({
                "average_score": avg_score / 100.0,
                "carbon_intensity": carbon_intensity,
                "success_rate": 0.5})
        xai_explanation = (moe_result or {}).get("xai_explanation") or \
            (optimization or {}).get("xai_explanation")
        role_assignments = (moe_result or {}).get("role_assignments") or \
            (optimization or {}).get("role_assignments")

        # 10. Causal policy
        causal_action = None
        if self.causal:
            state_vec = np.array([avg_score / 100.0,
                                  carbon_intensity / 1000.0,
                                  len(aggregated) / 20.0, 0.5, 0.5])
            causal_action = self.causal.select_action(state_vec, 4)
            self.last_counterfactuals = self.causal.counterfactuals(state_vec, 4)

        # 11. HITL escalation
        hitl_outcome = None
        confidence = avg_score / 100.0
        if self.hitl and confidence < getattr(self.config, "hitl_confidence_threshold", 0.65):
            with contextlib.suppress(Exception):
                hitl_outcome = await self.hitl.escalate(
                    decision_context={"run_id": run_id,
                                      "avg_score": confidence,
                                      "carbon_intensity": carbon_intensity},
                    options=["accept", "retry", "downgrade"],
                    confidence=confidence,
                    confidence_threshold=getattr(self.config,
                                                 "hitl_confidence_threshold", 0.65))

        # 12. Carbon credits
        credit_value = 0.0
        rec_value = 0.0
        if self.carbon_market:
            try:
                energy_kwh = len(aggregated) * 0.001
                carbon_saved = energy_kwh * carbon_intensity / 1000.0
                credit_value = await self.carbon_market.get_carbon_credit_value(carbon_saved)
                rec_value = await self.carbon_market.get_rec_value(energy_kwh)
            except Exception:
                pass

        # 13. Federated
        federated_round = None
        if self.federated:
            weights_vec = (list(self.modp_optimizer.weights)
                           if self.modp_optimizer else [0.25] * 4)
            self.federated.submit_update(self.instance_id, weights_vec,
                                         samples=max(1, len(aggregated)),
                                         carbon_intensity=carbon_intensity)
            if len(self.benchmark_history) % 5 == 0:
                try:
                    agg = await self.federated.aggregate()
                    federated_round = agg["round"]
                except PrivacyBudgetExceeded as e:
                    logger.warning(f"Federated DP exhausted: {e}")

        # 14. Chaos smoke test
        chaos_passed = None
        if self.chaos and len(self.benchmark_history) % 5 == 0:
            with contextlib.suppress(Exception):
                cr = await self.chaos.run_test("storage_broken", duration_s=0.05)
                chaos_passed = cr["passed"]

        # 15. Cost tracking
        cost = self._cost_per_module * max(1, len(aggregated))
        self._today_cost += cost
        COST_USD.inc(cost)
        with contextlib.suppress(Exception):
            await self.persistence.cost_record(cost, note=run_id)

        # 16. Build run
        run = BenchmarkRun(
            run_id=run_id,
            results=aggregated,
            system_info={},
            git_commit=os.environ.get("GIT_COMMIT", ""),
            version=self.config.version,
            data_quality_score=100.0,
            duration_seconds=time.time() - start_time,
            temporal_status=temporal_status,
            xai_explanation=xai_explanation,
            precision_used=precision.value,
            carbon_credit_value_usd=credit_value,
            rec_value_usd=rec_value,
            role_assignments=role_assignments,
            chaos_test_passed=chaos_passed,
            hitl_outcome=hitl_outcome,
            federated_round=federated_round,
            autonomous_optimization=optimization,
            causal_action=causal_action,
            counterfactuals=dict(self.last_counterfactuals))

        # 17. Quantum signature
        if sign_results and self.quantum:
            with contextlib.suppress(Exception):
                run.quantum_signature = await self.quantum.sign(run.to_dict())

        # 18. Blockchain
        if blockchain_record and self.blockchain:
            with contextlib.suppress(Exception):
                data_hash = hashlib.sha256(
                    json.dumps(run.to_dict(), sort_keys=True, default=str).encode()
                ).hexdigest()
                bc = await self.blockchain.record(
                    data_id=f"benchmark_{run_id}", data_hash=data_hash,
                    metadata={"modules": len(aggregated)})
                run.blockchain_tx_hash = bc["tx_hash"]

        # 19. Persist + history
        async with self._history_lock:
            self.benchmark_history.append(run)
        with contextlib.suppress(Exception):
            await self.persistence.save_benchmark(
                run_id=run_id, avg_score=avg_score,
                duration_s=run.duration_seconds,
                modules=len(aggregated), precision=precision.value)

        # 20. Active learning
        self.active_learner.maybe_store(
            {"avg_score": avg_score, "carbon_intensity": carbon_intensity,
             "modules": len(aggregated)},
            uncertainty=max(0.0, 1.0 - avg_score / 100.0))

        # 21. RLHF feedback
        if self.rlhf:
            reward = confidence * (1.0 - AdaptivePrecisionController.energy_factor(precision))
            with contextlib.suppress(Exception):
                self.rlhf.record_feedback(
                    state={"carbon_intensity": carbon_intensity,
                           "avg_score": confidence,
                           "cost": 0.5, "diversity": 0.5},
                    action="balanced",
                    reward=float(reward))

        # 22. Multi-agent record
        if self.roles:
            reward = confidence
            for role in AgentRole:
                self.roles.record(role, success=avg_score >= 50.0, reward=reward)

        # 23. Metrics
        BENCHMARK_RUNS.labels(status="success").inc()
        BENCHMARK_MODULES.set(len(aggregated))
        BENCHMARK_SCORE.set(avg_score)
        BENCHMARK_DURATION.set(run.duration_seconds)

        # 24. WebSocket broadcast
        with contextlib.suppress(Exception):
            await self.websocket.broadcast({
                "type": "benchmark_completed",
                "run_id": run_id,
                "avg_score": avg_score,
                "modules": len(aggregated),
                "precision": precision.value,
                "carbon_credit_usd": credit_value,
                "timestamp": utcnow().isoformat()}, topic="benchmark")

        # 25. Lineage
        with contextlib.suppress(Exception):
            await self.persistence.record_lineage(
                lineage_id=uuid.uuid4().hex[:12],
                parent_id=None, child_id=run_id,
                operation="benchmark_run",
                payload={"modules": module_names, "avg_score": avg_score})

        logger.info(f"Benchmark {run_id} complete: avg_score={avg_score:.1f}, "
                    f"modules={len(aggregated)}, precision={precision.value}")
        return run

    def _synthetic_result(self, name: str, synthetic: bool = True) -> BenchmarkResult:
        score = random.uniform(0.7, 0.95)
        return BenchmarkResult(
            module_name=name, category="unknown",
            accuracy_score=score, performance_score=score, precision_score=score,
            latency_ms=random.uniform(10, 100), integration_score=score,
            overall_score=score * 100,
            memory_usage_mb=random.uniform(100, 500),
            cpu_usage_pct=random.uniform(20, 80),
            p95_latency_ms=random.uniform(15, 120),
            throughput_ops_per_sec=random.uniform(1000, 5000),
            data_quality_score=0.0, synthetic=synthetic)

    def _build_result(self, name: str, category: str,
                      raw: Dict[str, Any]) -> BenchmarkResult:
        accuracy = float(raw.get("accuracy", 0.0))
        latency = float(raw.get("latency_ms", 0.0))
        memory = float(raw.get("memory_mb", 0.0))
        cpu = float(raw.get("cpu_pct", 0.0))
        throughput = float(raw.get("throughput_ops", 0.0))
        quality = float(raw.get("quality", 0.0))
        integration = float(raw.get("integration", 0.8))
        p95 = float(raw.get("p95_latency_ms", latency * 1.5))
        overall = (0.4 * accuracy + 0.2 * quality + 0.2 * integration + 0.2 *
                   min(1.0, throughput / 5000.0)) * 100.0
        return BenchmarkResult(
            module_name=name, category=category,
            accuracy_score=accuracy, performance_score=quality,
            precision_score=accuracy, latency_ms=latency,
            integration_score=integration, overall_score=overall,
            memory_usage_mb=memory, cpu_usage_pct=cpu,
            p95_latency_ms=p95, throughput_ops_per_sec=throughput,
            data_quality_score=1.0, synthetic=False)

    async def _aggregate_results(self, results: List[BenchmarkResult]
                                 ) -> List[BenchmarkResult]:
        return results

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        if self.rlhf:
            return await self.rlhf.get_policy_probs(state)
        if self.bio:
            p = self.bio.get_current_params()
            return [p["performance_weight"], p["carbon_weight"],
                    p["cost_weight"], p["diversity_weight"]]
        if self.modp_optimizer:
            return list(self.modp_optimizer.weights)
        return [0.25, 0.25, 0.25, 0.25]

    async def get_comprehensive_status(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "version": self.config.version,
            "running": self._running,
            "benchmark_count": len(self.benchmark_history),
            "today_cost_usd": self._today_cost,
            "daily_cost_budget_usd": self._daily_budget,
            "rate_limit_remaining": self._rate_limiter.remaining(),
            "quantum_security": self.quantum.status() if self.quantum else None,
            "blockchain": (await self.blockchain.health_check()
                           if self.blockchain else None),
            "carbon": await self.carbon_manager.health_check(),
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
                "multi_agent": self.coordinator is not None},
            "moe": self.moe_selector.get_stats() if self.moe_selector else None,
            "modp": {"weights": self.modp_optimizer.weights} if self.modp_optimizer else None,
            "bio": {"current_params": self.bio.get_current_params()} if self.bio else None,
            "self_healing": self.self_healing.stats() if self.self_healing else None,
            "chaos": self.chaos.get_report() if self.chaos else None,
            "federated": self.federated.get_stats() if self.federated else None,
            "hitl": (await self.hitl.audit_trail(limit=10)
                     if self.hitl else None),
            "hitl_verified": (await self.hitl.verify_audit_chain()
                              if self.hitl else None),
            "active_learning_size": len(self.active_learner),
            "causal": self.causal.stats() if self.causal else None,
            "role_registry": self.roles.stats() if self.roles else None,
            "health": await self.health_check(),
            "timestamp": utcnow().isoformat()}

    async def health_check(self) -> Dict[str, Any]:
        chain_ok = True
        if self.hitl:
            with contextlib.suppress(Exception):
                chain_ok = await self.hitl.verify_audit_chain()
        score = 100
        if not chain_ok:
            score -= 20
        if self.federated and self.federated.accountant.spent >= \
                self.federated.accountant.target_epsilon:
            score -= 20
        score = max(0, score)
        HEALTH_SCORE.set(score)
        return {
            "status": "healthy" if score >= 70 else "degraded",
            "health_score": score,
            "hitl_audit_ok": chain_ok,
            "pending_hitl": self.hitl.stats()["pending"] if self.hitl else 0,
            "today_cost_usd": self._today_cost,
            "in_flight": self._inflight,
            "timestamp": utcnow().isoformat()}

    async def run_chaos_suite(self) -> Dict[str, Any]:
        if self.chaos is None:
            return {"error": "chaos disabled"}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            with contextlib.suppress(Exception):
                results.append(await self.chaos.run_test(f, duration_s=0.05))
        return {"results": results, "report": self.chaos.get_report()}

    async def shutdown(self):
        logger.info("Shutting down benchmark runner...")
        self._shutdown_event.set()
        deadline = time.time() + 10.0
        while self._inflight > 0 and time.time() < deadline:
            await asyncio.sleep(0.1)
        for t in self._background_tasks:
            t.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self.websocket.stop()
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()
        self.persistence.close()
        logger.info("Shutdown complete")


# ============================================================
# FastAPI
# ============================================================
def build_fastapi_app(runner: EnhancedBenchmarkRunnerV11,
                      jwt_secret: Optional[str] = None) -> Any:
    if not FASTAPI_AVAILABLE:
        return None
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app):
        yield
        await runner.shutdown()

    app = FastAPI(title="Benchmark API", version=runner.config.version,
                  lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=["*"],
                       allow_credentials=True, allow_methods=["*"],
                       allow_headers=["*"])
    security = HTTPBearer(auto_error=False)
    secret = jwt_secret or runner.config.jwt_secret

    async def verify(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
        if not JOSE_AVAILABLE:
            return {"sub": "anonymous"}
        if credentials is None:
            raise HTTPException(status_code=401, detail="missing credentials")
        try:
            return jwt.decode(credentials.credentials, secret,
                              algorithms=[runner.config.jwt_algorithm])
        except JWTError:
            raise HTTPException(status_code=401, detail="invalid token")

    @app.post("/token")
    async def issue_token(sub: str = "user"):
        if not JOSE_AVAILABLE:
            raise HTTPException(status_code=500, detail="JWT library missing")
        exp = utcnow() + timedelta(seconds=runner.config.jwt_expiry_s)
        token = jwt.encode({"sub": sub, "exp": exp}, secret,
                           algorithm=runner.config.jwt_algorithm)
        return {"access_token": token, "token_type": "bearer"}

    @app.post("/benchmark")
    async def benchmark(iterations: int = 1,
                        modules: Optional[List[str]] = None,
                        user: Dict = Depends(verify)):
        run = await runner.run_benchmarks(
            module_names=modules, iterations=iterations)
        return run.to_dict()

    @app.get("/status")
    async def status(user: Dict = Depends(verify)):
        return await runner.get_comprehensive_status()

    @app.get("/health")
    async def health():
        return await runner.health_check()

    @app.get("/hitl/pending")
    async def hitl_pending(user: Dict = Depends(verify)):
        if not runner.hitl:
            return {"pending": []}
        return {"pending": runner.hitl.pending_requests()}

    @app.post("/hitl/approval")
    async def hitl_approval(request_id: str, approved: bool,
                            user: Dict = Depends(verify)):
        if not runner.hitl:
            raise HTTPException(status_code=503, detail="HITL disabled")
        ok = await runner.hitl.resolve(request_id, approved)
        if not ok:
            raise HTTPException(status_code=404, detail="not_found")
        return {"status": "resolved", "request_id": request_id,
                "approved": approved}

    @app.get("/hitl/audit")
    async def hitl_audit(limit: int = 100, user: Dict = Depends(verify)):
        if not runner.hitl:
            return {"audit": []}
        return {"audit": await runner.hitl.audit_trail(limit=limit),
                "verified": await runner.hitl.verify_audit_chain()}

    @app.post("/chaos")
    async def chaos(fault: str = "storage_broken", duration_s: float = 0.1,
                    user: Dict = Depends(verify)):
        if not runner.chaos:
            raise HTTPException(status_code=503, detail="chaos disabled")
        return await runner.chaos.run_test(fault, duration_s=duration_s)

    return app


# ============================================================
# Signal handling + singleton
# ============================================================
_runner_instance: Optional[EnhancedBenchmarkRunnerV11] = None
_runner_lock = asyncio.Lock()
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


async def get_benchmark_runner(
    config: Optional[BenchmarkConfig] = None,
    registry: Optional[ModuleRegistry] = None,
) -> EnhancedBenchmarkRunnerV11:
    global _runner_instance
    if _runner_instance is None:
        async with _runner_lock:
            if _runner_instance is None:
                _runner_instance = EnhancedBenchmarkRunnerV11(config, registry)
                await _runner_instance.start()
    return _runner_instance


async def shutdown_handler():
    global _runner_instance
    if _runner_instance:
        await _runner_instance.shutdown()
        _runner_instance = None


# ============================================================
# Demo registry (used by main only)
# ============================================================
async def _demo_module():
    await asyncio.sleep(0.01)
    return {"accuracy": random.uniform(0.7, 0.98),
            "latency_ms": random.uniform(5, 50),
            "memory_mb": random.uniform(50, 200),
            "cpu_pct": random.uniform(10, 40),
            "throughput_ops": random.uniform(500, 3000),
            "quality": random.uniform(0.6, 0.95),
            "integration": random.uniform(0.7, 1.0)}


def _build_demo_registry() -> ModuleRegistry:
    reg = ModuleRegistry()
    for i in range(5):
        reg.register(name=f"demo_module_{i}", category="demo",
                     run_fn=_demo_module,
                     metadata={"description": f"Demo module {i}"})
    return reg


# ============================================================
# Main
# ============================================================
async def main():
    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop)

    print("=" * 80)
    print("Enhanced Module Benchmark Suite v18.0.0 — P0/P1/P2 hardened")
    print("=" * 80)

    if FASTAPI_AVAILABLE and os.environ.get("BENCHMARK_SERVE_API", "0") == "1":
        cfg = BenchmarkConfig()
        runner = EnhancedBenchmarkRunnerV11(cfg, _build_demo_registry())
        await runner.start()
        app = build_fastapi_app(runner, cfg.jwt_secret)
        if app is not None:
            uvicorn.run(app, host=cfg.api_host, port=cfg.api_port,
                        log_level="info")
        return

    runner = await get_benchmark_runner(
        config={"persist_path": "/tmp/benchmark_v18.db",
                "chaos_enabled": False,
                "chaos_enable_loop": False},
        registry=_build_demo_registry())

    print("\n✅ Ten enhancements wired in:")
    print("   [1] Quantum-Distillation     dilithium/falcon/sphincs + HMAC fallback")
    print("   [2] Causal RL (UCB)          CausalCounterfactualEstimator")
    print("   [3] Federated (secure + DP)  FederatedAggregator + DPAccountant")
    print("   [4] Multi-Agent              EmergentRoleRegistry + MultiAgentCoordinator")
    print("   [5] Temporal Logic (full STL) TemporalLogicMonitor + SafetyShield")
    print("   [6] Explainable AI           XAIExplainer (SHAP + weighted)")
    print("   [7] Adaptive Precision       AdaptivePrecisionController")
    print("   [8] Carbon Markets / RECs    CarbonMarketClient + providers")
    print("   [9] Resilience / Chaos       ChaosTester (steady-state)")
    print("  [10] HITL / Active Learning   ActiveRLHF + HITL + ActiveLearning")

    print(f"\n📊 Registered modules: {runner.registry.names()}")
    print("\n🏃 Running benchmarks...")
    for i in range(2):
        run = await runner.run_benchmarks(iterations=1)
        avg = (np.mean([r.overall_score for r in run.results])
               if run.results else 0.0)
        print(f"   {run.run_id}: modules={len(run.results)} avg_score={avg:.1f} "
              f"precision={run.precision_used} "
              f"credit=${run.carbon_credit_value_usd or 0.0:.4f}")

    print("\n📊 Status:")
    status = await runner.get_comprehensive_status()
    print(json.dumps({
        "instance_id": status["instance_id"],
        "version": status["version"],
        "benchmark_count": status["benchmark_count"],
        "features": status["features"],
        "moe": status["moe"],
        "federated": status["federated"],
        "hitl_verified": status["hitl_verified"],
        "today_cost_usd": status["today_cost_usd"],
        "health": status["health"]["status"],
    }, indent=2, default=str))

    print("\n🧪 Running chaos suite...")
    chaos = await runner.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}")

    print("\n" + "=" * 80)
    print("✅ Enhanced Module Benchmark Suite v18.0.0 — Ready")
    print("=" * 80)

    try:
        await _shutdown_event_global.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await shutdown_handler()


if __name__ == "__main__":
    asyncio.run(main())
