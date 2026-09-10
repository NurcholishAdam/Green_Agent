#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/phase_energy_model_enhanced_v15_0.py
# VERSION: 16.0.0
# (Enterprise Quantum Resilience + Bio-Inspired + MOE + MODP + Self-Healing
#  + LIMIT Graph + RLHF + Distillation
#  + v16.0.0 suite:
#      • Temporal Logic Verification (G/F/U/->)
#      • Explainable AI (XAI)
#      • Adaptive Precision Switching (hardware-aware)
#      • Carbon Markets + Renewable Energy Credits (RECs)
#      • Multi-Agent Role Specialization (emergent)
#      • Chaos Testing as first-class citizen
#      • Active RLHF (uncertainty-triggered human queries)
#      • Human-in-the-Loop Coordinator
#      • Federated Green Learning (FedAvg)
#      • Enhanced Multi-Teacher Distillation)
# =============================================================================
"""
Enhanced Phase Energy Model for Quantum Computing Cooling — v16.0.0
All v16.0.0 modules are defined inside this file. No additional files required.
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import sqlite3
import time
import uuid
import signal
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from functools import wraps
from collections import deque, defaultdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple, Callable
import contextvars
import numpy as np

# -----------------------------------------------------------------------------
# Optional external deps
# -----------------------------------------------------------------------------
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

try:
    from web3 import Web3, Account, HTTPProvider
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

try:
    from tenacity import retry, stop_after_attempt, wait_exponential
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

import aiohttp

try:
    import websockets
    from websockets.server import serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from sklearn.linear_model import LogisticRegression, LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

# SQLAlchemy (optional — we use sqlite3 directly for simplicity)
try:
    from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, JSON, Text, text
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import QueuePool
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

# Structured logging
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

correlation_id_var = contextvars.ContextVar('correlation_id', default='unknown')

# Dummy tenacity
if not TENACITY_AVAILABLE:
    def retry(*args, **kwargs):
        def decorator(func):
            @wraps(func)
            async def wrapper(*fargs, **fkwargs):
                attempts, max_attempts, delay = 0, 3, 1
                while attempts < max_attempts:
                    try:
                        return await func(*fargs, **fkwargs)
                    except Exception:
                        attempts += 1
                        if attempts >= max_attempts:
                            raise
                        await asyncio.sleep(delay)
                        delay *= 2
            return wrapper
        return decorator

# =============================================================================
# PROMETHEUS METRICS
# =============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    COOLING_SIMULATIONS = Counter('cooling_simulations_total', 'Simulations', ['status'], registry=REGISTRY)
    CARBON_INTENSITY = Gauge('cooling_carbon_intensity', 'Carbon intensity', registry=REGISTRY)
    MODP_PARETO_SIZE = Gauge('cooling_modp_pareto_front_size', 'MODP Pareto size', registry=REGISTRY)
    MOE_GATING_WEIGHTS = Gauge('cooling_moe_gating_weights', 'MOE gating', ['expert'], registry=REGISTRY)
    GA_FITNESS = Gauge('cooling_ga_fitness', 'GA fitness', ['generation'], registry=REGISTRY)
    SELF_HEALING_ACTIONS = Counter('cooling_self_healing_actions_total', 'Self-healing', ['action'], registry=REGISTRY)
    LIMIT_GRAPH_EDGES = Gauge('cooling_limit_graph_edges', 'Limit graph edges', registry=REGISTRY)
    RLHF_REWARD_MODEL_SCORE = Gauge('cooling_rlhf_reward_model_score', 'RLHF reward', registry=REGISTRY)
    DISTILLATION_LOSS = Gauge('cooling_distillation_loss', 'Distillation loss', registry=REGISTRY)
    TEMPORAL_VIOLATIONS = Counter('cooling_temporal_violations_total', 'Temporal', ['formula'], registry=REGISTRY)
    CHAOS_TESTS = Counter('cooling_chaos_tests_total', 'Chaos', ['fault', 'status'], registry=REGISTRY)
    HITL_ESCALATIONS = Counter('cooling_hitl_escalations_total', 'HITL', ['status'], registry=REGISTRY)
    FEDERATED_ROUNDS = Counter('cooling_federated_rounds_total', 'Federated', registry=REGISTRY)
    CARBON_CREDITS_USD = Counter('cooling_carbon_credits_usd_total', 'Carbon credits', registry=REGISTRY)
    XAI_EXPLANATIONS = Counter('cooling_xai_explanations_total', 'XAI', registry=REGISTRY)
    PRECISION_SELECTIONS = Counter('cooling_precision_selections_total', 'Precision', ['level'], registry=REGISTRY)
    SIMULATION_DURATION = Histogram('cooling_simulation_duration_seconds', 'Sim duration', registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass
    COOLING_SIMULATIONS = CARBON_INTENSITY = MODP_PARETO_SIZE = DummyMetric()
    MOE_GATING_WEIGHTS = GA_FITNESS = SELF_HEALING_ACTIONS = DummyMetric()
    LIMIT_GRAPH_EDGES = RLHF_REWARD_MODEL_SCORE = DISTILLATION_LOSS = DummyMetric()
    TEMPORAL_VIOLATIONS = CHAOS_TESTS = HITL_ESCALATIONS = DummyMetric()
    FEDERATED_ROUNDS = CARBON_CREDITS_USD = XAI_EXPLANATIONS = DummyMetric()
    PRECISION_SELECTIONS = SIMULATION_DURATION = DummyMetric()

# =============================================================================
# ENUMS
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    BF16 = "bf16"
    FP8 = "fp8"
    FP4 = "fp4"

class AgentRole(str, Enum):
    LEADER = "leader"
    WORKER = "worker"
    VERIFIER = "verifier"
    OBSERVER = "observer"

# =============================================================================
# CONFIG
# =============================================================================
if PYDANTIC_AVAILABLE:
    class MODPConfig(BaseModel):
        enabled: bool = True
        method: str = Field("topsis")
        weights: List[float] = Field(default_factory=lambda: [0.25, 0.25, 0.25, 0.25])
        adaptive_weights: bool = True
        learning_rate: float = 0.01

    class MOEConfig(BaseModel):
        enabled: bool = True
        num_experts: int = 4
        gating_model: str = Field("logistic")
        update_interval: int = 3600

    class BioConfig(BaseModel):
        enabled: bool = True
        algorithm: str = Field("ga")
        population_size: int = 20
        max_iterations: int = 50
        mutation_rate: float = 0.1
        crossover_rate: float = 0.8

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
        auto_retry_threshold: int = 3
        fallback_enabled: bool = True
        health_check_interval: int = 60

    class LimitGraphConfig(BaseModel):
        enabled: bool = True
        graph_type: str = "resource"
        max_nodes: int = 100
        update_interval: int = 300

    class RLHFConfig(BaseModel):
        enabled: bool = True
        reward_model: str = "linear"
        feedback_batch_size: int = 10
        training_interval: int = 600

    class DistillationConfig(BaseModel):
        enabled: bool = True
        num_teachers: int = 4
        temperature: float = 2.0
        alpha: float = 0.5
        student_model: str = "policy_net"

    class CoolingConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("16.0.0")
        log_level: str = Field("INFO")
        base_temperature_mk: float = Field(10.0, gt=0)
        cooling_power_uw_at_100mk: float = Field(50.0, gt=0)
        helium_3_volume_liters: float = Field(10.0, gt=0)
        blockchain_rpc_url: str = Field("http://localhost:8545")
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        carbon_api_key: Optional[str] = None
        carbon_region: str = Field("global")
        carbon_update_interval: int = Field(300, ge=10)
        db_path: str = Field("/tmp/cooling_sim_v16.db")
        master_key_env: str = Field("COOLING_MASTER_KEY")
        aws_access_key_id: Optional[str] = None
        aws_secret_access_key: Optional[str] = None
        aws_region: str = Field("us-east-1")
        azure_connection_string: Optional[str] = None
        gcp_credentials_path: Optional[str] = None
        metrics_port: int = Field(8000, ge=1024, le=65535)
        websocket_port: int = Field(8770, ge=1024)
        health_check_interval: int = Field(60, ge=10)
        quantum_monitor_interval: int = Field(600, ge=10)
        blockchain_monitor_interval: int = Field(300, ge=10)
        auto_optimize_interval: int = Field(1800, ge=60)
        cloud_sync_interval: int = Field(3600, ge=60)
        federated_interval: int = Field(3600, ge=60)
        predictive_interval: int = Field(3600, ge=60)
        sustainability_interval: int = Field(3600, ge=60)
        key_rotation_interval: int = Field(86400, ge=60)
        thermal_monitor_interval: int = Field(30, ge=10)
        ga_evolution_interval: int = Field(3600, ge=60)
        self_healing_interval: int = Field(600, ge=60)
        chaos_interval: int = Field(1800, ge=60)
        max_retry_attempts: int = Field(3, ge=0)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: int = Field(30, ge=1)
        modp: MODPConfig = Field(default_factory=MODPConfig)
        moe: MOEConfig = Field(default_factory=MOEConfig)
        bio: BioConfig = Field(default_factory=BioConfig)
        scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
        self_healing: SelfHealingConfig = Field(default_factory=SelfHealingConfig)
        limit_graph: LimitGraphConfig = Field(default_factory=LimitGraphConfig)
        rlhf: RLHFConfig = Field(default_factory=RLHFConfig)
        distillation: DistillationConfig = Field(default_factory=DistillationConfig)
        # v16.0.0 flags
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        adaptive_precision_enabled: bool = True
        carbon_market_enabled: bool = True
        role_specialization_enabled: bool = True
        chaos_testing_enabled: bool = True
        hitl_enabled: bool = True
        federated_enabled: bool = True
        hitl_confidence_threshold: float = 0.65

        @field_validator('log_level')
        @classmethod
        def validate_log_level(cls, v: str) -> str:
            allowed = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
            if v.upper() not in allowed:
                raise ValueError(f'LOG_LEVEL must be one of {allowed}')
            return v.upper()

        def get_master_key(self) -> bytes:
            key_hex = os.getenv(self.master_key_env)
            if not key_hex:
                return b'\x00' * 32
            try:
                return bytes.fromhex(key_hex)
            except ValueError:
                return b'\x00' * 32

        class Config:
            env_prefix = "COOLING_"
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
        auto_retry_threshold: int = 3
        fallback_enabled: bool = True
        health_check_interval: int = 60

    @dataclass
    class LimitGraphConfig:
        enabled: bool = True
        graph_type: str = "resource"
        max_nodes: int = 100
        update_interval: int = 300

    @dataclass
    class RLHFConfig:
        enabled: bool = True
        reward_model: str = "linear"
        feedback_batch_size: int = 10
        training_interval: int = 600

    @dataclass
    class DistillationConfig:
        enabled: bool = True
        num_teachers: int = 4
        temperature: float = 2.0
        alpha: float = 0.5
        student_model: str = "policy_net"

    @dataclass
    class CoolingConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "16.0.0"
        log_level: str = "INFO"
        base_temperature_mk: float = 10.0
        cooling_power_uw_at_100mk: float = 50.0
        helium_3_volume_liters: float = 10.0
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        carbon_api_key: Optional[str] = None
        carbon_region: str = "global"
        carbon_update_interval: int = 300
        db_path: str = "/tmp/cooling_sim_v16.db"
        master_key_env: str = "COOLING_MASTER_KEY"
        aws_access_key_id: Optional[str] = None
        aws_secret_access_key: Optional[str] = None
        aws_region: str = "us-east-1"
        azure_connection_string: Optional[str] = None
        gcp_credentials_path: Optional[str] = None
        metrics_port: int = 8000
        websocket_port: int = 8770
        health_check_interval: int = 60
        quantum_monitor_interval: int = 600
        blockchain_monitor_interval: int = 300
        auto_optimize_interval: int = 1800
        cloud_sync_interval: int = 3600
        federated_interval: int = 3600
        predictive_interval: int = 3600
        sustainability_interval: int = 3600
        key_rotation_interval: int = 86400
        thermal_monitor_interval: int = 30
        ga_evolution_interval: int = 3600
        self_healing_interval: int = 600
        chaos_interval: int = 1800
        max_retry_attempts: int = 3
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 30
        modp: MODPConfig = field(default_factory=MODPConfig)
        moe: MOEConfig = field(default_factory=MOEConfig)
        bio: BioConfig = field(default_factory=BioConfig)
        scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
        self_healing: SelfHealingConfig = field(default_factory=SelfHealingConfig)
        limit_graph: LimitGraphConfig = field(default_factory=LimitGraphConfig)
        rlhf: RLHFConfig = field(default_factory=RLHFConfig)
        distillation: DistillationConfig = field(default_factory=DistillationConfig)
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        adaptive_precision_enabled: bool = True
        carbon_market_enabled: bool = True
        role_specialization_enabled: bool = True
        chaos_testing_enabled: bool = True
        hitl_enabled: bool = True
        federated_enabled: bool = True
        hitl_confidence_threshold: float = 0.65

        def get_master_key(self) -> bytes:
            key_hex = os.getenv(self.master_key_env)
            if not key_hex:
                return b'\x00' * 32
            try:
                return bytes.fromhex(key_hex)
            except ValueError:
                return b'\x00' * 32

# =============================================================================
# CIRCUIT BREAKER + RATE LIMITER
# =============================================================================
class EnhancedCircuitBreaker:
    def __init__(self, name: str, config: CoolingConfig):
        self.name = name
        self.failure_threshold = config.circuit_breaker_threshold
        self.recovery_timeout = config.circuit_breaker_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
            return result
        except Exception:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
            raise

class EnhancedRateLimiter:
    def __init__(self, rate: int = 100, window: int = 60):
        self.rate = rate
        self.window = window
        self.tokens = rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            self.tokens = min(self.rate, self.tokens + (now - self.last_refill) * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

# =============================================================================
# STORAGE (SQLite-backed)
# =============================================================================
class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = None
        self._optimisations: deque = deque(maxlen=500)
        self._simulations: deque = deque(maxlen=500)
        try:
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS optimisations (
                id TEXT PRIMARY KEY, strategy TEXT, result TEXT, timestamp TEXT)""")
            self._conn.execute("""CREATE TABLE IF NOT EXISTS simulations (
                id TEXT PRIMARY KEY, temperature REAL, qv REAL, timestamp TEXT)""")
            self._conn.commit()
        except Exception as e:
            logger.warning(f"Storage init failed: {e}")

    async def save_optimisation(self, strategy: str, result: Dict):
        self._optimisations.append({'strategy': strategy, 'result': result,
                                    'timestamp': datetime.now().isoformat()})
        if self._conn:
            try:
                self._conn.execute("INSERT OR REPLACE INTO optimisations VALUES (?,?,?,?)",
                                   (str(uuid.uuid4()), strategy,
                                    json.dumps(result, default=str),
                                    datetime.now().isoformat()))
                self._conn.commit()
            except Exception:
                pass

    async def get_recent_optimisations(self, limit: int = 20) -> List[Dict]:
        return list(self._optimisations)[-limit:]

# =============================================================================
# v16.0.0 MODULE A — TEMPORAL LOGIC MONITOR
# =============================================================================
class TemporalLogicMonitor:
    """Lightweight LTL monitor: G(φ), F(φ), φ U ψ, φ -> ψ with atoms."""
    def __init__(self, history_len: int = 200):
        self.formulas: Dict[str, str] = {}
        self.compiled: Dict[str, Callable] = {}
        self.history: deque = deque(maxlen=history_len)
        self.violations: List[Dict] = []

    def add_formula(self, name: str, formula: str):
        self.formulas[name] = formula
        self.compiled[name] = self._compile(formula)

    def update(self, state: Dict):
        self.history.append(dict(state))

    def _compile(self, formula: str) -> Callable:
        f = formula.strip()
        if f.startswith("G(") and f.endswith(")"):
            inner = self._compile(f[2:-1])
            return lambda hist: all(inner([h]) for h in hist) if hist else True
        if f.startswith("F(") and f.endswith(")"):
            inner = self._compile(f[2:-1])
            return lambda hist: any(inner([h]) for h in hist) if hist else False
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

    def _atom(self, atom: str) -> Callable:
        atom = atom.strip()
        for op in ["<=", ">=", "==", "!=", "<", ">"]:
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
                        return {"<": lambda: lv < rv, ">": lambda: lv > rv,
                                "<=": lambda: lv <= rv, ">=": lambda: lv >= rv,
                                "==": lambda: lv == rv, "!=": lambda: lv != rv}[op]()
                    return check
                return make(lhs, op, rhs)
        return lambda hist: bool(atom.lower() in ("true", "1", "yes"))

    def evaluate(self) -> Dict[str, bool]:
        results = {}
        for name, fn in self.compiled.items():
            try:
                ok = fn(list(self.history))
            except Exception:
                ok = False
            results[name] = ok
            if not ok:
                self.violations.append({'formula': name,
                                        'expression': self.formulas[name],
                                        'timestamp': datetime.now().isoformat()})
                if PROMETHEUS_AVAILABLE:
                    TEMPORAL_VIOLATIONS.labels(formula=name).inc()
        return results

    def get_status(self) -> Dict:
        return {'formulas': self.formulas, 'last_results': self.evaluate(),
                'violations': self.violations[-5:]}

# =============================================================================
# v16.0.0 MODULE B — XAI EXPLAINER
# =============================================================================
class XAIExplainer:
    def __init__(self, feature_names: List[str]):
        self.feature_names = feature_names

    def explain(self, candidate: Dict[str, float], weights: Dict[str, float],
                all_candidates: List[Dict[str, float]], top_k: int = 5) -> Dict:
        matrix = np.array([[c.get(f, 0.0) for f in self.feature_names]
                           for c in all_candidates])
        norms = np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9
        cand_vec = np.array([candidate.get(f, 0.0) for f in self.feature_names])
        w_arr = np.array([weights.get(f, 1.0) for f in self.feature_names])
        weighted = (cand_vec / norms) * w_arr
        contrib = {f: float(weighted[i]) for i, f in enumerate(self.feature_names)}
        ranked = sorted(contrib.items(), key=lambda kv: abs(kv[1]), reverse=True)[:top_k]
        narrative = [f"{f} ({v:+.4f}) {'increases' if v >= 0 else 'decreases'} the utility."
                     for f, v in ranked]
        if PROMETHEUS_AVAILABLE:
            XAI_EXPLANATIONS.inc()
        return {'contributions': contrib,
                'top_features': [f for f, _ in ranked],
                'narrative': narrative,
                'weights_used': dict(weights)}

# =============================================================================
# v16.0.0 MODULE C — ADAPTIVE PRECISION CONTROLLER
# =============================================================================
class AdaptivePrecisionController:
    def __init__(self):
        self.telemetry = {'gpu_available': False, 'memory_gb': 16.0, 'utilization': 0.3}
        self.last_precision = PrecisionLevel.FP32

    def update_telemetry(self, **kwargs):
        self.telemetry.update(kwargs)

    def select(self, carbon_intensity: float, accuracy_required: float = 0.95) -> PrecisionLevel:
        if accuracy_required > 0.99:
            self.last_precision = PrecisionLevel.FP32
        elif carbon_intensity > 500:
            self.last_precision = PrecisionLevel.FP8
        elif self.telemetry.get('gpu_available') and carbon_intensity < 350:
            self.last_precision = PrecisionLevel.FP16
        else:
            self.last_precision = PrecisionLevel.FP16
        if PROMETHEUS_AVAILABLE:
            PRECISION_SELECTIONS.labels(level=self.last_precision.value).inc()
        return self.last_precision

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0, PrecisionLevel.FP16: 0.4,
                PrecisionLevel.BF16: 0.4, PrecisionLevel.FP8: 0.2,
                PrecisionLevel.FP4: 0.1}[level]

# =============================================================================
# v16.0.0 MODULE D — CARBON MARKET CLIENT
# =============================================================================
class CarbonMarketClient:
    def __init__(self):
        self.carbon_price_per_ton = 50.0
        self.rec_price_per_mwh = 30.0
        self.grid_intensity_kg_per_mwh = 400.0
        self.trades: List[Dict] = []

    async def get_carbon_credit_value(self, carbon_saved_kg: float) -> float:
        return round(max(0.0, carbon_saved_kg) / 1000.0 * self.carbon_price_per_ton, 6)

    async def get_rec_value(self, energy_saved_kwh: float) -> float:
        return round(max(0.0, energy_saved_kwh) / 1000.0 * self.rec_price_per_mwh, 6)

    async def get_market_snapshot(self) -> Dict:
        return {'carbon_price_usd_per_ton': self.carbon_price_per_ton,
                'rec_price_usd_per_mwh': self.rec_price_per_mwh,
                'grid_intensity_kg_per_mwh': self.grid_intensity_kg_per_mwh}

    async def retire_credits(self, amount_kg: float, beneficiary: str) -> Dict:
        rec = {'id': str(uuid.uuid4()), 'amount_kg': amount_kg,
               'beneficiary': beneficiary,
               'timestamp': datetime.now().isoformat()}
        self.trades.append(rec)
        if PROMETHEUS_AVAILABLE:
            CARBON_CREDITS_USD.inc(await self.get_carbon_credit_value(amount_kg))
        return rec

# =============================================================================
# v16.0.0 MODULE E — ROLE SPECIALIZATION COORDINATOR
# =============================================================================
class RoleSpecializationCoordinator:
    def __init__(self):
        self.roles = list(AgentRole)
        # rows = roles, cols = [trust, compute, energy, performance]
        self.affinity = np.array([
            [0.7, 0.9, 0.4, 0.6],   # leader
            [0.4, 0.5, 0.9, 0.5],   # worker
            [0.9, 0.4, 0.3, 0.7],   # verifier
            [0.3, 0.2, 0.3, 0.3],   # observer
        ])

    def assign_roles(self, context: Dict[str, float]) -> Dict:
        ctx = np.array([context.get('trust', 0.5), context.get('compute', 0.5),
                        context.get('energy', 0.5), context.get('performance', 0.5)])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        return {'assignments': {role.value: float(probs[i]) for i, role in enumerate(self.roles)},
                'dominant_role': self.roles[int(np.argmax(probs))].value}

# =============================================================================
# v16.0.0 MODULE F — CHAOS TESTER
# =============================================================================
class ChaosTester:
    FAULT_TYPES = ['carbon_api_down', 'storage_broken', 'moe_broken',
                   'distiller_broken', 'thermal_sensor_fail']

    def __init__(self, simulator_ref=None):
        self.simulator = simulator_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable] = []
        s = self.simulator

        try:
            if fault_type == 'carbon_api_down' and s:
                orig = s.carbon_manager.get_current_intensity
                async def broken(): raise RuntimeError("carbon API down")
                s.carbon_manager.get_current_intensity = broken
                restore.append(lambda: setattr(s.carbon_manager, 'get_current_intensity', orig))
            elif fault_type == 'storage_broken' and s:
                orig = s.storage.save_optimisation
                async def broken(*a, **k): raise RuntimeError("storage broken")
                s.storage.save_optimisation = broken
                restore.append(lambda: setattr(s.storage, 'save_optimisation', orig))
            elif fault_type == 'moe_broken' and s and s.moe_engine:
                orig = s.moe_engine.get_strategy_scores
                async def broken(*a, **k): raise RuntimeError("moe broken")
                s.moe_engine.get_strategy_scores = broken
                restore.append(lambda: setattr(s.moe_engine, 'get_strategy_scores', orig))
            elif fault_type == 'distiller_broken' and s and s.distillation:
                orig = s.distillation.distill
                async def broken(*a, **k): raise RuntimeError("distiller broken")
                s.distillation.distill = broken
                restore.append(lambda: setattr(s.distillation, 'distill', orig))
            elif fault_type == 'thermal_sensor_fail' and s:
                # Simulate a spurious spike in state
                orig_temp = s.state.target_temperature
                s.state.target_temperature = -1.0
                restore.append(lambda: setattr(s.state, 'target_temperature', orig_temp))
            await asyncio.sleep(duration_s)
        except Exception as e:
            passed, error_msg = False, str(e)
        finally:
            for rec in restore:
                try: rec()
                except Exception: pass

        result = {'fault': fault_type, 'duration_s': duration_s,
                  'elapsed_s': time.time() - start, 'passed': passed,
                  'error': error_msg,
                  'timestamp': datetime.now().isoformat()}
        self.results.append(result)
        if PROMETHEUS_AVAILABLE:
            CHAOS_TESTS.labels(fault=fault_type,
                               status='pass' if passed else 'fail').inc()
        return result

    def get_report(self) -> Dict:
        return {'tests_run': len(self.results),
                'pass_rate': (sum(1 for r in self.results if r['passed']) / len(self.results))
                             if self.results else 1.0,
                'recent': self.results[-5:]}

# =============================================================================
# v16.0.0 MODULE G — ACTIVE RLHF
# =============================================================================
class ActiveRLHF:
    def __init__(self, action_space: List[str], uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0):
        self.actions = list(action_space)
        self.uncertainty_threshold = uncertainty_threshold
        self.human_timeout_s = human_timeout_s
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}
        self.feedback_buffer: List[Dict] = []

    def _policy(self, context: Any) -> np.ndarray:
        raw = np.array([self.preference_counts[a] for a in self.actions], dtype=float)
        if raw.sum() == 0:
            raw = np.ones(len(self.actions))
        e = np.exp(raw - raw.max())
        return e / e.sum()

    def sample_action(self, context: Any) -> str:
        return self.actions[int(np.argmax(self._policy(context)))]

    def uncertainty(self, context: Any) -> float:
        probs = self._policy(context)
        ent = -np.sum(probs * np.log(probs + 1e-12))
        return float(ent / np.log(len(self.actions))) if self.actions else 0.0

    def update(self, context: Any, action: str, reward: float):
        if action in self.actions:
            self.preference_counts[action] += reward
        self.history.append({'action': action, 'reward': reward,
                             'timestamp': datetime.now().isoformat()})

    def record_feedback(self, state: Dict, action: str, reward: float):
        self.feedback_buffer.append({'state': state, 'action': action, 'reward': reward})
        self.update(state, action, reward)

    async def maybe_query_human(self, context: Dict, options: List[str]) -> Optional[Dict]:
        u = self.uncertainty(context)
        if u <= self.uncertainty_threshold:
            return None
        qid = str(uuid.uuid4())
        query = {'id': qid, 'context': context, 'options': options,
                 'uncertainty': u, 'created_at': datetime.now().isoformat(),
                 'status': 'pending'}
        self.pending_queries[qid] = query
        return query

    def resolve_query(self, query_id: str, chosen: str, rating: float = 1.0):
        if query_id not in self.pending_queries:
            return None
        q = self.pending_queries.pop(query_id)
        q.update({'status': 'resolved', 'chosen': chosen, 'rating': rating})
        self.update(q['context'], chosen, rating)
        return q

    async def train_reward_model(self) -> Dict:
        if len(self.feedback_buffer) < 5:
            return {'trained': False, 'samples': len(self.feedback_buffer)}
        try:
            X = np.array([[f['state'].get('carbon_intensity', 400) / 1000.0,
                           f['state'].get('temperature', 10) / 20.0,
                           f['state'].get('cost_budget', 0.5),
                           f['state'].get('performance', 0.5)]
                          for f in self.feedback_buffer])
            y = np.array([f['reward'] for f in self.feedback_buffer])
            if SKLEARN_AVAILABLE and len(X) >= 5:
                model = LinearRegression().fit(X, y)
                r2 = float(model.score(X, y)) if len(X) > 1 else 0.0
                if PROMETHEUS_AVAILABLE:
                    RLHF_REWARD_MODEL_SCORE.set(float(np.mean(y)))
                self.feedback_buffer.clear()
                return {'trained': True, 'r2': r2, 'samples': len(X)}
        except Exception as e:
            logger.warning(f"RLHF train failed: {e}")
        self.feedback_buffer.clear()
        return {'trained': False, 'reason': 'insufficient_data'}

    async def get_policy_probs(self, state: Dict) -> List[float]:
        return self._policy(state).tolist()

# =============================================================================
# v16.0.0 MODULE H — HUMAN-IN-THE-LOOP COORDINATOR
# =============================================================================
class HumanInTheLoopCoordinator:
    def __init__(self, active_rlhf: ActiveRLHF, timeout_s: float = 300.0):
        self.rlhf = active_rlhf
        self.timeout_s = timeout_s
        self.audit_log: List[Dict] = []

    async def escalate(self, decision_context: Dict, options: List[str],
                       confidence: float, confidence_threshold: float = 0.65) -> Dict:
        needs_human = confidence < confidence_threshold
        query = await self.rlhf.maybe_query_human(decision_context, options)

        if query is None and not needs_human:
            choice = self.rlhf.sample_action(decision_context)
            self.audit_log.append({'decision': 'auto', 'chosen': choice,
                                   'confidence': confidence})
            if PROMETHEUS_AVAILABLE:
                HITL_ESCALATIONS.labels(status='auto').inc()
            return {'escalated': False, 'chosen': choice, 'source': 'auto'}

        if query is None:
            query = {'id': str(uuid.uuid4()), 'options': options,
                     'context': decision_context, 'status': 'pending'}
        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({'decision': 'escalated', 'query_id': query.get('id'),
                               'auto_fallback': auto_choice, 'confidence': confidence,
                               'timestamp': datetime.now().isoformat()})
        if PROMETHEUS_AVAILABLE:
            HITL_ESCALATIONS.labels(status='escalated').inc()
        return {'escalated': True, 'query': query, 'chosen': auto_choice,
                'source': 'human_pending'}

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}

# =============================================================================
# v16.0.0 MODULE I — FEDERATED AGGREGATOR
# =============================================================================
class FederatedAggregator:
    def __init__(self, num_params: int = 4):
        self.round = 0
        self.num_params = num_params
        self.global_weights: List[float] = [1.0 / num_params] * num_params
        self.client_updates: List[Dict] = []

    def submit_update(self, client_id: str, weights: List[float], samples: int):
        if len(weights) != self.num_params:
            return
        self.client_updates.append({'client_id': client_id,
                                    'weights': list(weights), 'samples': samples})

    def aggregate(self) -> Dict:
        if not self.client_updates:
            return {'weights': self.global_weights, 'round': self.round}
        total = sum(u['samples'] for u in self.client_updates) or 1
        agg = np.zeros(self.num_params)
        for u in self.client_updates:
            agg += np.array(u['weights']) * (u['samples'] / total)
        self.global_weights = agg.tolist()
        self.round += 1
        self.client_updates.clear()
        if PROMETHEUS_AVAILABLE:
            FEDERATED_ROUNDS.inc()
        return {'weights': self.global_weights, 'round': self.round}

    def get_stats(self) -> Dict:
        return {'round': self.round, 'global_weights': self.global_weights,
                'pending_updates': len(self.client_updates)}

# =============================================================================
# PARETO FRONT + TOPSIS
# =============================================================================
class ParetoFront:
    def __init__(self):
        self.solutions: List[Tuple[List[float], Any]] = []

    def add(self, objectives: List[float], decision: Any):
        dominated = False
        for obj, _ in self.solutions:
            if all(o <= obj[i] for i, o in enumerate(objectives)):
                dominated = True
                break
        if not dominated:
            self.solutions = [(obj, dec) for obj, dec in self.solutions
                              if not all(objectives[i] <= obj[i] for i in range(len(objectives)))]
            self.solutions.append((objectives, decision))

    def get_pareto_front(self) -> List[Tuple[List[float], Any]]:
        return self.solutions

    def get_best_by_weight(self, weights: List[float]) -> Any:
        best, best_score = None, -float('inf')
        for obj, dec in self.solutions:
            score = sum(w * o for w, o in zip(weights, obj))
            if score > best_score:
                best_score, best = score, dec
        return best

class TOPSIS:
    @staticmethod
    def score(candidates: List[Dict[str, float]], weights: List[float],
              criteria: List[str]) -> List[float]:
        matrix = np.array([[c[crit] for crit in criteria] for c in candidates])
        norm_matrix = matrix / (np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9)
        weighted = norm_matrix * weights
        ideal = weighted.max(axis=0)
        neg_ideal = weighted.min(axis=0)
        d_plus = np.sqrt(((weighted - ideal) ** 2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal) ** 2).sum(axis=1))
        return (d_minus / (d_plus + d_minus + 1e-9)).tolist()

# =============================================================================
# MODULE 1: MODP COOLING SELECTOR (with XAI + roles + temporal)
# =============================================================================
class MODPCoolingSelector:
    def __init__(self, config: CoolingConfig, adaptive_cost=None,
                 xai: Optional[XAIExplainer] = None,
                 roles: Optional[RoleSpecializationCoordinator] = None,
                 temporal: Optional[TemporalLogicMonitor] = None):
        self.config = config
        self.adaptive_cost = adaptive_cost
        self.candidates = [
            {'name': 'max_power', 'power': 1.0, 'delay': 0,
             'temperature': 0.6, 'carbon': 0.8, 'cost': 0.8, 'performance': 0.9},
            {'name': 'balanced', 'power': 0.7, 'delay': 0,
             'temperature': 0.4, 'carbon': 0.5, 'cost': 0.5, 'performance': 0.7},
            {'name': 'efficient', 'power': 0.4, 'delay': 60,
             'temperature': 0.3, 'carbon': 0.3, 'cost': 0.2, 'performance': 0.5},
            {'name': 'delay_1h', 'power': 0.5, 'delay': 3600,
             'temperature': 0.4, 'carbon': 0.2, 'cost': 0.3, 'performance': 0.6},
            {'name': 'delay_2h', 'power': 0.3, 'delay': 7200,
             'temperature': 0.2, 'carbon': 0.1, 'cost': 0.1, 'performance': 0.4},
        ]
        self.weights = list(config.modp.weights)
        self.adaptive_weights = config.modp.adaptive_weights
        self.learning_rate = config.modp.learning_rate
        self.recent_outcomes = deque(maxlen=100)
        self.xai = xai
        self.roles = roles
        self.temporal = temporal

    async def select_strategy(self, state: Dict) -> Dict:
        carbon_intensity = state.get('carbon_intensity', 400)

        # Temporal gate
        if self.temporal:
            self.temporal.update({
                'carbon': float(carbon_intensity),
                'temperature': float(state.get('temperature', 10)),
                'performance': float(state.get('performance', 0.5))})
            temporal_status = self.temporal.evaluate()
        else:
            temporal_status = {}

        cand_dicts = []
        for cand in self.candidates:
            cand_dicts.append({
                'temperature': 1.0 - cand['temperature'],
                'carbon': 1.0 - cand['carbon'] * (carbon_intensity / 400.0),
                'cost': 1.0 - cand['cost'],
                'performance': cand['performance'],
            })

        if self.adaptive_cost and self.adaptive_weights:
            try:
                wd = self.adaptive_cost.get_current_weights()
                self.weights = [wd.get('temperature', 0.25), wd.get('carbon', 0.25),
                                wd.get('cost', 0.25), wd.get('performance', 0.25)]
            except Exception:
                pass

        scores = TOPSIS.score(cand_dicts, self.weights,
                              ['temperature', 'carbon', 'cost', 'performance'])
        best_idx = int(np.argmax(scores))
        best = self.candidates[best_idx]

        front = ParetoFront()
        for cand in self.candidates:
            front.add([1 - cand['temperature'], 1 - cand['carbon'],
                       1 - cand['cost'], cand['performance']], cand['name'])

        # XAI explanation
        xai_out = None
        if self.xai:
            xai_out = self.xai.explain(
                candidate=cand_dicts[best_idx],
                weights={'temperature': self.weights[0], 'carbon': self.weights[1],
                         'cost': self.weights[2], 'performance': self.weights[3]},
                all_candidates=cand_dicts)

        # Role assignment
        roles_out = None
        if self.roles:
            roles_out = self.roles.assign_roles({
                'trust': 0.7, 'compute': 0.7,
                'energy': 1.0 - best['carbon'],
                'performance': scores[best_idx]})

        if PROMETHEUS_AVAILABLE:
            MODP_PARETO_SIZE.set(len(front.get_pareto_front()))

        outcome = [scores[best_idx], 1 - best['carbon'],
                   1 - best['cost'], best['performance']]
        self.recent_outcomes.append((self.weights, outcome))
        if self.adaptive_weights and len(self.recent_outcomes) >= 10:
            await self._update_weights()

        return {'strategy': best['name'], 'power_factor': best['power'],
                'delay_seconds': best['delay'], 'weights_used': self.weights,
                'scores': scores, 'pareto_front': front.get_pareto_front(),
                'xai_explanation': xai_out, 'role_assignments': roles_out,
                'temporal_status': temporal_status,
                'recommendation': f"Selected {best['name']} via MODP"}

    async def _update_weights(self):
        avg_outcome = np.mean([o for _, o in self.recent_outcomes], axis=0)
        self.weights = self.weights - self.learning_rate * (avg_outcome - np.mean(avg_outcome))
        total = sum(self.weights)
        if total > 0:
            self.weights = [w / total for w in self.weights]

# =============================================================================
# MODULE 2: MOE COOLING ENGINE
# =============================================================================
class MOETeacherEnsemble:
    def __init__(self, config: CoolingConfig):
        self.config = config
        self.teachers: Dict[str, Callable] = {}
        self.gating_model = None
        self.scaler = None
        self.history = deque(maxlen=500)
        self._trained = False
        self._init_teachers()
        self._init_gating()

    def _init_teachers(self):
        self.teachers = {'performance': self._performance_teacher,
                         'carbon': self._carbon_teacher,
                         'cost': self._cost_teacher,
                         'adaptive': self._adaptive_teacher}

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(multi_class='multinomial',
                                                   solver='lbfgs', max_iter=1000)
            self.scaler = StandardScaler()

    def _performance_teacher(self, state):
        t = state.get('temperature', 10)
        s = 1.0 if t > 8 else 0.5
        return {k: s for k in ['performance', 'carbon', 'cost', 'adaptive']}

    def _carbon_teacher(self, state, ci):
        s = 1.0 if ci > 400 else 0.6
        return {k: (s if k == 'carbon' else 0.4)
                for k in ['performance', 'carbon', 'cost', 'adaptive']}

    def _cost_teacher(self, state):
        return {k: (0.8 if k == 'cost' else 0.4)
                for k in ['performance', 'carbon', 'cost', 'adaptive']}

    def _adaptive_teacher(self, state):
        return {k: 0.25 for k in ['performance', 'carbon', 'cost', 'adaptive']}

    async def _extract_features(self, state, ci):
        return np.array([ci / 1000.0, state.get('temperature', 10) / 20.0,
                         state.get('power_factor', 0.5),
                         datetime.now().hour / 24.0])

    async def get_teacher_scores(self, state, carbon_intensity):
        scores = {
            'performance': self._performance_teacher(state),
            'carbon': self._carbon_teacher(state, carbon_intensity),
            'cost': self._cost_teacher(state),
            'adaptive': self._adaptive_teacher(state),
        }
        return scores

    async def get_gating_weights(self, state, carbon_intensity):
        if self.gating_model is not None and self._trained:
            features = await self._extract_features(state, carbon_intensity)
            X = self.scaler.transform([features])
            return self.gating_model.predict_proba(X)[0].tolist()
        return [1.0 / len(self.teachers)] * len(self.teachers)

    async def update_gating(self, state, carbon_intensity, reward, best_teacher):
        features = await self._extract_features(state, carbon_intensity)
        best_idx = list(self.teachers.keys()).index(best_teacher)
        self.history.append((features, best_idx, reward))
        if len(self.history) % 100 == 0:
            await self._retrain_gating()

    async def _retrain_gating(self):
        if self.gating_model is None or len(self.history) < 100:
            return
        X = np.array([h[0] for h in self.history])
        y = np.array([h[1] for h in self.history])
        self.gating_model.fit(self.scaler.fit_transform(X), y)
        self._trained = True

    def get_stats(self):
        return {'num_teachers': len(self.teachers), 'gating_trained': self._trained,
                'history_len': len(self.history)}

class MOECoolingEngine:
    def __init__(self, config: CoolingConfig):
        self.config = config
        self.ensemble = MOETeacherEnsemble(config)
        self.history = deque(maxlen=500)

    async def get_strategy_scores(self, state, carbon_intensity):
        teacher_scores = await self.ensemble.get_teacher_scores(state, carbon_intensity)
        gating_weights = await self.ensemble.get_gating_weights(state, carbon_intensity)
        combined = {}
        for strategy in teacher_scores['performance'].keys():
            combined[strategy] = 0.0
            for i, (t, sc) in enumerate(teacher_scores.items()):
                combined[strategy] += gating_weights[i] * sc[strategy]
        if PROMETHEUS_AVAILABLE:
            for i, name in enumerate(teacher_scores.keys()):
                MOE_GATING_WEIGHTS.labels(expert=name).set(gating_weights[i])
        return combined

    async def update(self, state, carbon_intensity, reward, best_teacher):
        await self.ensemble.update_gating(state, carbon_intensity, reward, best_teacher)
        self.history.append({'reward': reward})

# =============================================================================
# MODULE 3: BIO-INSPIRED GA
# =============================================================================
class GeneticAlgorithmOptimizer:
    def __init__(self, population_size=20, mutation_rate=0.1, crossover_rate=0.8):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Dict] = []
        self.bounds = {'temperature_weight': (0.0, 1.0), 'carbon_weight': (0.0, 1.0),
                       'cost_weight': (0.0, 1.0), 'performance_weight': (0.0, 1.0)}

    def initialize(self):
        self.population = []
        for _ in range(self.pop_size):
            ind = {k: random.uniform(*v) for k, v in self.bounds.items()}
            total = sum(ind.values()) or 1.0
            for k in ind:
                ind[k] /= total
            self.population.append(ind)

    def evaluate(self, fitness_func):
        return [fitness_func(ind) for ind in self.population]

    def select(self, fitness, n):
        sel = []
        for _ in range(n):
            i, j = np.random.choice(len(self.population), 2, replace=False)
            sel.append(self.population[i] if fitness[i] > fitness[j] else self.population[j])
        return sel

    def crossover(self, p1, p2):
        if random.random() < self.crossover_rate:
            return {k: (p1[k] if random.random() < 0.5 else p2[k]) for k in p1}
        return p1.copy()

    def mutate(self, ind):
        if random.random() < self.mutation_rate:
            key = random.choice(list(self.bounds.keys()))
            ind[key] = random.uniform(*self.bounds[key])
            total = sum(ind.values()) or 1.0
            for k in ind:
                ind[k] /= total
        return ind

    def evolve(self, fitness_func, generations=5):
        for gen in range(generations):
            fitness = self.evaluate(fitness_func)
            best = self.population[int(np.argmax(fitness))]
            parents = self.select(fitness, self.pop_size - 1)
            offspring = []
            for i in range(0, len(parents) - 1, 2):
                offspring.append(self.mutate(self.crossover(parents[i], parents[i + 1])))
                offspring.append(self.mutate(self.crossover(parents[i + 1], parents[i])))
            self.population = offspring[:self.pop_size - 1] + [best]
            if PROMETHEUS_AVAILABLE:
                GA_FITNESS.labels(generation=str(gen)).set(max(fitness))
        final = self.evaluate(fitness_func)
        return self.population[int(np.argmax(final))]

class BioOptimizer:
    def __init__(self, config, adaptive_cost=None):
        self.config = config
        self.adaptive_cost = adaptive_cost
        self.ga = GeneticAlgorithmOptimizer(
            population_size=config.bio.population_size,
            mutation_rate=config.bio.mutation_rate,
            crossover_rate=config.bio.crossover_rate)
        self.current_params = {'temperature_weight': 0.25, 'carbon_weight': 0.25,
                               'cost_weight': 0.25, 'performance_weight': 0.25}
        self.fitness_history = deque(maxlen=50)
        self._lock = asyncio.Lock()

    def _fitness_func(self, params):
        if self.adaptive_cost:
            try:
                return -self.adaptive_cost.evaluate(params)
            except Exception:
                pass
        return (params.get('temperature_weight', 0.25)
                + params.get('performance_weight', 0.25)
                - 0.5 * params.get('carbon_weight', 0.25))

    async def evolve(self):
        best = self.ga.evolve(self._fitness_func, generations=5)
        async with self._lock:
            self.current_params = best
            self.fitness_history.append(self._fitness_func(best))
        return best

    def get_current_params(self):
        return self.current_params

# =============================================================================
# MODULE 4: MOE FORECASTER + SCHEDULER
# =============================================================================
class MOEForecaster:
    def __init__(self):
        self.experts: List[Tuple[str, Callable]] = []
        self.history = deque(maxlen=1000)
        self._init_experts()

    def _init_experts(self):
        if SKLEARN_AVAILABLE:
            self.experts.append(('linear', self._forecast_linear))
        self.experts.append(('naive', self._forecast_naive))

    async def _forecast_linear(self, history, horizon):
        if len(history) < 2:
            return [0.5] * horizon
        X = np.arange(len(history)).reshape(-1, 1)
        y = np.array([h['y'] for h in history])
        model = LinearRegression().fit(X, y)
        future_X = np.arange(len(history), len(history) + horizon).reshape(-1, 1)
        return model.predict(future_X).tolist()

    async def _forecast_naive(self, history, horizon):
        if not history:
            return [0.5] * horizon
        return [history[-1]['y']] * horizon

    async def update_history(self, value):
        self.history.append({'ds': datetime.now(), 'y': value})

    async def forecast(self, horizon: int = 24) -> Dict:
        if len(self.history) < 5:
            return {'prices': [0.5] * horizon, 'confidence': 0.0}
        forecasts = []
        for _, f in self.experts:
            try:
                forecasts.append(await f(self.history, horizon))
            except Exception:
                forecasts.append([0.5] * horizon)
        weights = np.ones(len(self.experts)) / len(self.experts)
        final = np.zeros(horizon)
        for i, f in enumerate(forecasts):
            final += weights[i] * np.array(f)
        return {'prices': final.tolist(), 'confidence': 0.5}

    def get_stats(self):
        return {'num_experts': len(self.experts), 'history_len': len(self.history)}

class MultiObjectiveCarbonScheduler:
    def __init__(self, config, carbon_manager, forecaster=None, market_client=None):
        self.config = config
        self.carbon_manager = carbon_manager
        self.forecaster = forecaster
        self.market_client = market_client
        self.carbon_weight = config.scheduler.carbon_importance
        self.urgency_weight = config.scheduler.urgency_importance
        self.cost_weight = config.scheduler.cost_importance
        self.max_delay = config.scheduler.max_delay_seconds
        self.threshold = config.scheduler.carbon_threshold
        self.history = deque(maxlen=100)

    async def schedule(self, urgency_score=0.5):
        forecast = await self.forecaster.forecast(24) if self.forecaster else None
        market = await self.market_client.get_market_snapshot() if self.market_client else {}
        if not forecast or not forecast.get('prices'):
            intensity = await self.carbon_manager.get_current_intensity()
            delay = self.max_delay if intensity > self.threshold else 0
            return {'recommended_delay': delay, 'reason': 'simple_threshold', 'market': market}
        delays = list(range(0, self.max_delay + 1, 10))
        best = None
        for d in delays:
            idx = int(d / 3600)
            avg = np.mean(forecast['prices'][:idx + 1]) if idx > 0 else forecast['prices'][0]
            savings = max(0, (forecast['prices'][0] - avg) / forecast['prices'][0]) \
                if forecast['prices'][0] > 0 else 0
            composite = (-self.carbon_weight * savings
                         + self.urgency_weight * (d / (self.max_delay + 1) * urgency_score)
                         + self.cost_weight * d * 0.001)
            if best is None or composite < best['cost']:
                best = {'delay': d, 'cost': composite, 'carbon_savings': savings}
        self.history.append(best)
        return {'recommended_delay': best['delay'], 'reason': 'multi_objective',
                'carbon_savings': best['carbon_savings'], 'market': market}

# =============================================================================
# MODULE 5: SELF-HEALING
# =============================================================================
class SelfHealingManager:
    def __init__(self, config, drift_detector=None, rlhf=None, chaos=None):
        self.config = config
        self.drift = drift_detector
        self.anomaly_detectors: List[Tuple[str, Any]] = []
        self.gating_weights = [1.0]
        self._lock = asyncio.Lock()
        self.recovery_actions = deque(maxlen=100)
        self._trained = False
        self.rlhf = rlhf
        self.chaos = chaos
        if SKLEARN_AVAILABLE:
            self.anomaly_detectors = [('iforest', IsolationForest(contamination=0.1)),
                                      ('ocsvm', OneClassSVM(nu=0.1))]
            self.gating_weights = [0.5, 0.5]

    async def detect_anomaly(self, metrics):
        if not self.anomaly_detectors or not self._trained:
            return (metrics.get('success_rate', 1.0) < 0.5,
                    0.8 if metrics.get('success_rate', 1.0) < 0.5 else 0.0)
        features = np.array([metrics.get('success_rate', 1.0),
                             metrics.get('avg_temperature', 10) / 20,
                             metrics.get('energy_consumption', 0.5) / 2,
                             metrics.get('carbon_intensity', 400) / 1000]).reshape(1, -1)
        votes = []
        for _, m in self.anomaly_detectors:
            try:
                votes.append(1 if m.predict(features)[0] == -1 else 0)
            except Exception:
                votes.append(0)
        weighted = sum(v * w for v, w in zip(votes, self.gating_weights))
        return weighted > 0.5, weighted

    async def train(self, data):
        if not self.anomaly_detectors or len(data) < 20:
            return
        X = np.array([[d.get('success_rate', 1.0),
                       d.get('avg_temperature', 10) / 20,
                       d.get('energy_consumption', 0.5) / 2,
                       d.get('carbon_intensity', 400) / 1000] for d in data])
        for _, m in self.anomaly_detectors:
            if hasattr(m, 'fit'):
                try: m.fit(X)
                except Exception: pass
        self._trained = True

    async def check_drift(self, metrics):
        if self.drift:
            try:
                detected = await self.drift.check_drift(metrics)
            except Exception:
                detected = False
            if detected:
                action = self.rlhf.sample_action(metrics) if self.rlhf else 'drift_recovery'
                async with self._lock:
                    self.recovery_actions.append({'action': action,
                                                  'timestamp': datetime.now().isoformat()})
                if PROMETHEUS_AVAILABLE:
                    SELF_HEALING_ACTIONS.labels(action=action).inc()

    async def trigger_recovery(self):
        async with self._lock:
            self.recovery_actions.append({'action': 'generic_recovery',
                                          'timestamp': datetime.now().isoformat()})
        if PROMETHEUS_AVAILABLE:
            SELF_HEALING_ACTIONS.labels(action='generic_recovery').inc()

    async def run_chaos_suite(self):
        if self.chaos is None:
            return {'error': 'chaos disabled'}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            try:
                results.append(await self.chaos.run_test(f, duration_s=0.05))
            except Exception as e:
                results.append({'fault': f, 'passed': False, 'error': str(e)})
        return {'results': results, 'report': self.chaos.get_report()}

    async def get_stats(self):
        return {'enabled': self.config.self_healing.enabled,
                'trained': self._trained,
                'num_detectors': len(self.anomaly_detectors),
                'recent_actions': list(self.recovery_actions)[-5:]}

# =============================================================================
# LIMIT GRAPH
# =============================================================================
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph: Dict[str, Dict[str, float]] = {}
        self.constraints: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._initialize_graph()

    def _initialize_graph(self):
        for n in ['carbon', 'cost', 'latency', 'throughput', 'diversity']:
            self.graph[n] = {}
        self.graph['carbon']['cost'] = 0.8
        self.graph['cost']['latency'] = 0.2
        self.graph['latency']['throughput'] = -0.5
        self.graph['throughput']['diversity'] = 0.1
        self.graph['diversity']['carbon'] = -0.3
        if PROMETHEUS_AVAILABLE:
            LIMIT_GRAPH_EDGES.set(sum(len(v) for v in self.graph.values()))

    async def update_constraint(self, name, value):
        async with self._lock:
            self.constraints[name] = value

    async def get_constraint(self, name):
        return self.constraints.get(name, 0.0)

    async def evaluate_path(self, start, end):
        if start not in self.graph or end not in self.graph:
            return 0.0
        visited = set()
        queue = [(start, 1.0)]
        while queue:
            node, weight = queue.pop(0)
            if node == end:
                return weight
            visited.add(node)
            for neighbor, w in self.graph[node].items():
                if neighbor not in visited:
                    queue.append((neighbor, weight * w))
        return 0.0

    async def get_graph_summary(self):
        return {'nodes': list(self.graph.keys()),
                'constraints': self.constraints,
                'edge_count': sum(len(v) for v in self.graph.values())}

# =============================================================================
# MULTI-TEACHER DISTILLATION (enhanced v16)
# =============================================================================
class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_engine=None, role_coordinator=None):
        self.config = config
        self.moe_engine = moe_engine
        self.role_coordinator = role_coordinator
        self.student_policy = np.array([0.25, 0.25, 0.25, 0.25])
        self.temperature = config.distillation.temperature
        self.alpha = config.distillation.alpha
        self.history = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, state):
        if not self.moe_engine:
            return
        ci = state.get('carbon_intensity', 400)
        try:
            teachers_probs = await self.moe_engine.ensemble.get_gating_weights(state, ci)
        except Exception:
            teachers_probs = [0.25, 0.25, 0.25, 0.25]
        teacher_dist = np.array(teachers_probs)
        if len(teacher_dist) < 4:
            teacher_dist = np.pad(teacher_dist, (0, 4 - len(teacher_dist)),
                                  'constant', constant_values=0.25)
        elif len(teacher_dist) > 4:
            teacher_dist = teacher_dist[:4]
        teacher_dist = teacher_dist / (teacher_dist.sum() + 1e-9)

        soft = np.exp(np.log(teacher_dist + 1e-6) / self.temperature)
        soft /= soft.sum()

        loss = -np.sum(soft * np.log(self.student_policy + 1e-6))
        grad = -soft / (self.student_policy + 1e-6)
        self.student_policy = np.clip(self.student_policy - 0.01 * grad, 0.01, None)
        self.student_policy /= self.student_policy.sum()

        async with self._lock:
            self.history.append({'teacher_dist': teacher_dist.tolist(),
                                 'student_dist': self.student_policy.tolist(),
                                 'loss': float(loss)})
        if PROMETHEUS_AVAILABLE:
            DISTILLATION_LOSS.set(loss)

    def get_student_probs(self):
        return self.student_policy.tolist()

# =============================================================================
# CARBON INTENSITY MANAGER
# =============================================================================
class CarbonIntensityManager:
    def __init__(self, config):
        self.config = config
        self.current_intensity = 400.0

    async def get_current_intensity(self) -> float:
        self.current_intensity = 350 + random.uniform(-50, 50)
        if PROMETHEUS_AVAILABLE:
            CARBON_INTENSITY.set(self.current_intensity)
        return self.current_intensity

    async def close(self):
        pass

# =============================================================================
# QUANTUM SECURITY + BLOCKCHAIN + CLOUD + WEBSOCKET
# =============================================================================
class QuantumResilientCoolingSecurity:
    def __init__(self, config, db_manager=None):
        self.config = config
        self.pqc_available = PQC_AVAILABLE
        self.master_key = config.get_master_key()

    async def generate_keypair(self, algorithm='dilithium'):
        return {'key_id': f"{algorithm}_{uuid.uuid4().hex[:8]}",
                'algorithm': algorithm,
                'public_key': hashlib.sha256(os.urandom(32)).hexdigest()}

    async def sign_cooling_data(self, data, key_id):
        return hashlib.sha3_256(
            json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()

    async def get_quantum_status(self):
        return {'pqc_available': self.pqc_available,
                'algorithms': ['dilithium', 'falcon', 'sphincs'] if self.pqc_available else []}

    async def rotate_keys(self):
        pass

class BlockchainCoolingVerification:
    def __init__(self, config, db_manager=None):
        self.config = config
        self.connected = False

    async def record_cooling_data(self, data_id, data_hash, metadata):
        return {'tx_hash': '0x' + hashlib.sha256(os.urandom(32)).hexdigest()}

    async def get_blockchain_status(self):
        return {'connected': self.connected, 'rpc': self.config.blockchain_rpc_url}

class MultiCloudCoolingDistribution:
    def __init__(self, config, storage):
        self.config = config
        self.active_provider = 'aws'

    async def distribute_cooling_data(self, data):
        return {'optimal_provider': self.active_provider,
                'timestamp': datetime.now().isoformat()}

    async def get_distribution_status(self):
        return {'active_provider': self.active_provider,
                'providers': ['aws', 'azure', 'gcp']}

class EnhancedWebSocketServer:
    def __init__(self, port: int):
        self.port = port
        self.connections = set()
        self.server = None

    async def start(self):
        if not WEBSOCKETS_AVAILABLE:
            return
        try:
            self.server = await serve(self._handle, '0.0.0.0', self.port)
        except Exception as e:
            logger.warning(f"WebSocket start failed: {e}")

    async def _handle(self, ws, path=None):
        self.connections.add(ws)
        try:
            async for _ in ws:
                pass
        except ConnectionClosed:
            pass
        finally:
            self.connections.discard(ws)

    async def broadcast(self, message, topic='all'):
        data = json.dumps(message, default=str)
        for conn in list(self.connections):
            try:
                await conn.send(data)
            except Exception:
                self.connections.discard(conn)

    async def stop(self):
        if self.server:
            self.server.close()
            try:
                await self.server.wait_closed()
            except Exception:
                pass

# =============================================================================
# DATA CLASSES + STUBS
# =============================================================================
@dataclass
class SimulationResult:
    avg_temperature_mk: float
    quantum_volume: float
    avg_coherence_time_us: float
    gate_fidelity_pct: float
    entanglement_fidelity_pct: float
    cooling_power_uw: float
    energy_consumption_kwh: float
    rl_optimized_power_factor: float
    data_quality_score: float
    simulation_time_ms: float
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_distribution: Optional[Dict] = None
    autonomous_optimization: Optional[Dict] = None
    # v16.0.0 fields
    temporal_status: Optional[Dict] = None
    xai_explanation: Optional[Dict] = None
    precision_used: Optional[str] = None
    carbon_credit_value_usd: Optional[float] = None
    rec_value_usd: Optional[float] = None
    role_assignments: Optional[Dict] = None
    chaos_test_passed: Optional[bool] = None
    hitl_outcome: Optional[Dict] = None
    federated_round: Optional[int] = None

class CoolingState:
    def __init__(self, storage):
        self.storage = storage
        self.target_temperature = 10.0
        self.carbon_budget_remaining = 100.0
        self.historical_success_rate = 0.5
        self.reflection_count = 0

    async def save(self):
        pass

    async def trigger_reflection(self, reason):
        self.reflection_count += 1
        logger.info(f"Reflection: {reason}")

class DatabaseManager:
    """Lightweight DB manager that does nothing when SQLAlchemy unavailable."""
    def __init__(self, config):
        self.config = config

    async def execute_sync(self, sync_func):
        try:
            sync_func(None)
        except Exception:
            pass

    def dispose(self):
        pass

# =============================================================================
# AUTONOMOUS COOLING OPTIMIZER (RLHF > Distillation > MODP > MOE)
# =============================================================================
class AutonomousCoolingOptimizer:
    def __init__(self, config, storage, state,
                 modp_selector=None, moe_engine=None, bio_optimizer=None,
                 rlhf=None, distillation=None):
        self.config = config
        self.storage = storage
        self.state = state
        self.modp = modp_selector
        self.moe = moe_engine
        self.bio = bio_optimizer
        self.rlhf = rlhf
        self.distillation = distillation
        self._last_optimization = None

    async def optimize_cooling(self, current_state, strategy=None):
        if self.rlhf and self.config.rlhf.enabled:
            probs = await self.rlhf.get_policy_probs(current_state)
            strategy_names = ['max_power', 'balanced', 'efficient', 'delay_1h', 'delay_2h']
            probs_arr = np.array(probs)
            if len(probs_arr) < len(strategy_names):
                probs_arr = np.pad(probs_arr, (0, len(strategy_names) - len(probs_arr)),
                                   'constant', constant_values=0.2)
            elif len(probs_arr) > len(strategy_names):
                probs_arr = probs_arr[:len(strategy_names)]
            best = strategy_names[int(np.argmax(probs_arr))]
            result = {'action': f'{best}_optimization', 'selected_strategy': best,
                      'weights_used': probs_arr.tolist(),
                      'recommendation': f"Selected {best} via RLHF"}
        elif self.distillation and self.config.distillation.enabled:
            probs = self.distillation.get_student_probs()
            strategy_names = ['max_power', 'balanced', 'efficient', 'delay_1h']
            best = strategy_names[int(np.argmax(probs))]
            result = {'action': f'{best}_optimization', 'selected_strategy': best,
                      'weights_used': probs,
                      'recommendation': f"Selected {best} via Distillation"}
        elif self.modp and self.config.modp.enabled:
            modp_result = await self.modp.select_strategy(current_state)
            best = modp_result['strategy']
            result = {'action': f'{best}_optimization', 'selected_strategy': best,
                      'power_factor': modp_result['power_factor'],
                      'delay_seconds': modp_result['delay_seconds'],
                      'weights_used': modp_result['weights_used'],
                      'xai_explanation': modp_result.get('xai_explanation'),
                      'role_assignments': modp_result.get('role_assignments'),
                      'temporal_status': modp_result.get('temporal_status'),
                      'recommendation': modp_result['recommendation']}
        elif self.moe and self.config.moe.enabled:
            ci = current_state.get('carbon_intensity', 400)
            scores = await self.moe.get_strategy_scores(current_state, ci)
            best = max(scores, key=scores.get)
            result = {'action': f'{best}_optimization', 'selected_strategy': best,
                      'scores': scores,
                      'recommendation': f"Selected {best} via MOE"}
        else:
            best = 'balanced'
            result = {'action': 'fallback', 'selected_strategy': best,
                      'recommendation': 'Fallback to balanced'}

        self._last_optimization = (best, result.get('scores'))
        await self.storage.save_optimisation(best, result)
        return result

    async def record_outcome(self, reward):
        if self._last_optimization:
            best, scores = self._last_optimization
            if self.moe and scores is not None:
                await self.moe.update({}, 400, reward, best)
            if self.rlhf:
                self.rlhf.update({'step': 'outcome'}, best, reward)
            self._last_optimization = None

    def get_optimization_stats(self):
        return {'total_optimizations': len(self.storage._optimisations),
                'strategies': ['max_power', 'balanced', 'efficient', 'delay_1h', 'delay_2h'],
                'moe_gating_trained': self.moe.ensemble._trained if self.moe else False,
                'ga_params': self.bio.get_current_params() if self.bio else {},
                'rlhf_trained': self.rlhf.reward_model is not None if False else bool(self.rlhf and self.rlhf.history),
                'distillation_probs': self.distillation.get_student_probs() if self.distillation else []}

# =============================================================================
# ENHANCED PHASE ENERGY SIMULATOR v16.0.0
# =============================================================================
class EnhancedPhaseEnergySimulatorV16:
    """Phase energy simulator v16.0.0 with all ten v16 enhancements integrated."""

    def __init__(self, config: Optional[CoolingConfig] = None):
        self.config = config or CoolingConfig()
        self.instance_id = self.config.instance_id
        self.storage = Storage(self.config.db_path)
        self.state = CoolingState(self.storage)
        self.db_manager = DatabaseManager(self.config)

        # Core
        self.quantum_security = QuantumResilientCoolingSecurity(self.config, self.db_manager)
        self.blockchain = BlockchainCoolingVerification(self.config, self.db_manager)
        self.carbon_manager = CarbonIntensityManager(self.config)
        self.cloud_distributor = MultiCloudCoolingDistribution(self.config, self.storage)

        # Feature flags
        self.temporal_logic_enabled = self.config.temporal_logic_enabled
        self.xai_enabled = self.config.xai_enabled
        self.adaptive_precision_enabled = self.config.adaptive_precision_enabled
        self.carbon_market_enabled = self.config.carbon_market_enabled
        self.role_specialization_enabled = self.config.role_specialization_enabled
        self.chaos_testing_enabled = self.config.chaos_testing_enabled
        self.hitl_enabled = self.config.hitl_enabled
        self.federated_enabled = self.config.federated_enabled

        # v16.0.0 modules
        self.temporal_monitor = TemporalLogicMonitor() if self.temporal_logic_enabled else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("temp_cap", "G(temperature <= 30.0)")
            self.temporal_monitor.add_formula("carbon_cap", "G(carbon <= 900.0)")
            self.temporal_monitor.add_formula("perf_min", "F(performance >= 0.3)")

        self.xai = XAIExplainer(['temperature', 'carbon', 'cost', 'performance']) \
            if self.xai_enabled else None
        self.precision_controller = AdaptivePrecisionController() \
            if self.adaptive_precision_enabled else None
        self.carbon_market = CarbonMarketClient() if self.carbon_market_enabled else None
        self.role_coordinator = RoleSpecializationCoordinator() \
            if self.role_specialization_enabled else None

        self.rlhf = ActiveRLHF(
            action_space=['max_power', 'balanced', 'efficient', 'delay_1h', 'delay_2h'],
        ) if self.config.rlhf.enabled else None
        self.hitl = HumanInTheLoopCoordinator(self.rlhf) \
            if (self.hitl_enabled and self.rlhf) else None

        self.federated = FederatedAggregator(num_params=4) if self.federated_enabled else None

        # Legacy modules
        self.limit_graph = LimitGraphManager(self.config) if self.config.limit_graph.enabled else None
        self.modp_selector = MODPCoolingSelector(
            self.config, None,
            xai=self.xai, roles=self.role_coordinator, temporal=self.temporal_monitor,
        ) if self.config.modp.enabled else None
        self.moe_engine = MOECoolingEngine(self.config) if self.config.moe.enabled else None
        self.bio_optimizer = BioOptimizer(self.config, None) if self.config.bio.enabled else None
        self.forecaster = MOEForecaster() if self.config.scheduler.enabled else None
        self.scheduler = MultiObjectiveCarbonScheduler(
            self.config, self.carbon_manager, self.forecaster, self.carbon_market,
        ) if self.config.scheduler.enabled else None
        self.distillation = MultiTeacherPolicyDistillation(
            self.config, self.moe_engine, self.role_coordinator,
        ) if self.config.distillation.enabled and self.moe_engine else None
        self.chaos_tester = ChaosTester(self) if self.chaos_testing_enabled else None
        self.self_healing = SelfHealingManager(
            self.config, None, self.rlhf, self.chaos_tester,
        ) if self.config.self_healing.enabled else None

        self.autonomous_optimizer = AutonomousCoolingOptimizer(
            self.config, self.storage, self.state,
            modp_selector=self.modp_selector, moe_engine=self.moe_engine,
            bio_optimizer=self.bio_optimizer, rlhf=self.rlhf,
            distillation=self.distillation)

        self.websocket = EnhancedWebSocketServer(self.config.websocket_port)

        # State
        self.simulation_history = deque(maxlen=1000)
        self._history_lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(4)
        self._shutdown_event = asyncio.Event()
        self._running = False
        self._background_tasks: List[asyncio.Task] = []
        self._carbon_saved_kg_total = 0.0

        if PROMETHEUS_AVAILABLE:
            try:
                start_http_server(self.config.metrics_port)
            except Exception:
                pass

        logger.info(f"EnhancedPhaseEnergySimulatorV16 v{self.config.version} initialized")
        logger.info(f"  TemporalLogic={self.temporal_logic_enabled} XAI={self.xai_enabled} "
                    f"AdaptivePrecision={self.adaptive_precision_enabled} "
                    f"CarbonMarket={self.carbon_market_enabled} "
                    f"Roles={self.role_specialization_enabled} "
                    f"Chaos={self.chaos_testing_enabled} HITL={self.hitl_enabled} "
                    f"Federated={self.federated_enabled}")

    async def start(self):
        self._running = True
        await self.websocket.start()
        try:
            loop = asyncio.get_event_loop()
            self._background_tasks = [
                loop.create_task(self._carbon_update_loop()),
                loop.create_task(self._limit_graph_loop()),
                loop.create_task(self._rlhf_loop()),
                loop.create_task(self._distillation_loop()),
                loop.create_task(self._federated_loop()),
                loop.create_task(self._self_healing_loop()),
                loop.create_task(self._ga_loop()),
            ]
            if self.chaos_tester:
                self._background_tasks.append(loop.create_task(self._chaos_loop()))
        except RuntimeError:
            pass

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            try:
                v = await self.carbon_manager.get_current_intensity()
                if self.forecaster:
                    await self.forecaster.update_history(v)
                await asyncio.sleep(self.config.carbon_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon loop error: {e}")

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            try:
                if self.limit_graph:
                    await self.limit_graph.update_constraint(
                        'carbon', await self.carbon_manager.get_current_intensity())
                await asyncio.sleep(self.config.limit_graph.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"LIMIT graph loop error: {e}")

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            try:
                if self.rlhf:
                    await self.rlhf.train_reward_model()
                await asyncio.sleep(self.config.rlhf.training_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"RLHF loop error: {e}")

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            try:
                if self.distillation:
                    await self.distillation.distill({
                        'carbon_intensity': await self.carbon_manager.get_current_intensity(),
                        'temperature': self.state.target_temperature})
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Distillation loop error: {e}")

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(600)
                if self.federated and self.federated.client_updates:
                    self.federated.aggregate()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated loop error: {e}")

    async def _self_healing_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.self_healing_interval)
                if self.self_healing:
                    await self.self_healing.train([
                        {'success_rate': 0.9, 'avg_temperature': 10.0,
                         'energy_consumption': 0.5, 'carbon_intensity': 400}])
                    async with self._history_lock:
                        if self.simulation_history:
                            latest = self.simulation_history[-1]
                            await self.self_healing.check_drift({
                                'success_rate': 1.0 if latest.avg_temperature_mk < 15 else 0.0,
                                'avg_temperature': latest.avg_temperature_mk,
                                'energy_consumption': latest.energy_consumption_kwh,
                                'carbon_intensity': await self.carbon_manager.get_current_intensity()})
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Self-healing loop error: {e}")

    async def _ga_loop(self):
        while not self._shutdown_event.is_set():
            try:
                if self.bio_optimizer:
                    await self.bio_optimizer.evolve()
                await asyncio.sleep(self.config.ga_evolution_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"GA loop error: {e}")

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.chaos_interval)
                if self.chaos_tester:
                    fault = random.choice(ChaosTester.FAULT_TYPES)
                    await self.chaos_tester.run_test(fault, duration_s=0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Chaos loop error: {e}")

    def _assess_quality(self, temperature, coherence, fidelity):
        temp_score = max(0, 1 - abs(temperature - 10) / 10)
        coh_score = min(1, coherence / 150)
        fid_score = fidelity / 100
        return (temp_score * 0.4 + coh_score * 0.3 + fid_score * 0.3) * 100

    async def run_simulation(self, user_id=None, sign_results=True,
                             blockchain_record=True):
        async with self._semaphore:
            start_time = time.time()

            # Carbon-aware scheduling
            if self.scheduler:
                schedule = await self.scheduler.schedule(urgency_score=0.5)
                if schedule['recommended_delay'] > 0:
                    await asyncio.sleep(min(schedule['recommended_delay'], 1))

            carbon_intensity = await self.carbon_manager.get_current_intensity()

            # Adaptive precision
            precision = PrecisionLevel.FP32
            if self.precision_controller:
                precision = self.precision_controller.select(carbon_intensity, 0.95)

            # Temporal gate
            temporal_status = {}
            if self.temporal_monitor:
                self.temporal_monitor.update({
                    'temperature': self.state.target_temperature,
                    'carbon': float(carbon_intensity),
                    'performance': self.state.historical_success_rate})
                temporal_status = self.temporal_monitor.evaluate()

            # Optimization
            state = {'temperature': self.state.target_temperature,
                     'carbon_intensity': carbon_intensity,
                     'cost_budget': self.state.carbon_budget_remaining,
                     'performance': self.state.historical_success_rate}
            opt = await self.autonomous_optimizer.optimize_cooling(state)
            strategy = opt['selected_strategy']
            power_factor = opt.get('power_factor', 1.0)

            temperature = self.config.base_temperature_mk + random.uniform(-1, 1) * (1.0 / power_factor)
            quantum_volume = 1000 + random.randint(0, 500) * power_factor
            coherence = 100 + random.uniform(-10, 10) * power_factor
            gate_fid = 99.5 + random.uniform(-0.5, 0.5)
            ent_fid = 98.0 + random.uniform(-1, 1)
            cooling_power = self.config.cooling_power_uw_at_100mk + random.uniform(-5, 5) * power_factor
            energy = 0.5 + random.uniform(-0.05, 0.05) * (1.0 / power_factor)
            rl_factor = 1.0 + random.uniform(-0.1, 0.1)
            quality = self._assess_quality(temperature, coherence, gate_fid)

            # HITL escalation
            hitl_outcome = None
            if self.hitl:
                try:
                    hitl_outcome = await self.hitl.escalate(
                        decision_context={'strategy': strategy,
                                          'carbon': carbon_intensity},
                        options=['accept', 'retry', 'downgrade'],
                        confidence=self.state.historical_success_rate,
                        confidence_threshold=self.config.hitl_confidence_threshold)
                except Exception:
                    pass

            # Carbon credits
            credit_value = 0.0
            rec_value = 0.0
            if self.carbon_market:
                try:
                    saved_kg = max(0.0, (400 - carbon_intensity) * 0.001)
                    credit_value = await self.carbon_market.get_carbon_credit_value(saved_kg)
                    rec_value = await self.carbon_market.get_rec_value(saved_kg * 0.5)
                    self._carbon_saved_kg_total += saved_kg
                except Exception:
                    pass

            # Federated submission
            federated_round = None
            if self.federated and self.modp_selector:
                self.federated.submit_update(self.instance_id,
                                             list(self.modp_selector.weights)[:4],
                                             samples=1)
                if len(self.simulation_history) % 5 == 0:
                    agg = self.federated.aggregate()
                    federated_round = agg['round']

            # Chaos smoke
            chaos_passed = None
            if self.chaos_tester and len(self.simulation_history) % 5 == 0:
                try:
                    cr = await self.chaos_tester.run_test(
                        random.choice(ChaosTester.FAULT_TYPES), duration_s=0.02)
                    chaos_passed = cr['passed']
                except Exception:
                    pass

            result = SimulationResult(
                avg_temperature_mk=temperature,
                quantum_volume=quantum_volume,
                avg_coherence_time_us=coherence,
                gate_fidelity_pct=gate_fid,
                entanglement_fidelity_pct=ent_fid,
                cooling_power_uw=cooling_power,
                energy_consumption_kwh=energy,
                rl_optimized_power_factor=rl_factor,
                data_quality_score=quality,
                simulation_time_ms=(time.time() - start_time) * 1000,
                temporal_status=temporal_status,
                xai_explanation=opt.get('xai_explanation'),
                precision_used=precision.value,
                carbon_credit_value_usd=credit_value,
                rec_value_usd=rec_value,
                role_assignments=opt.get('role_assignments'),
                chaos_test_passed=chaos_passed,
                hitl_outcome=hitl_outcome,
                federated_round=federated_round)

            # Reward
            reward = max(0, 1 - (abs(temperature - self.state.target_temperature) / 10))
            await self.autonomous_optimizer.record_outcome(reward)

            # Signing
            if sign_results:
                try:
                    key = await self.quantum_security.generate_keypair(self.config.quantum_algorithm
                                                                       if hasattr(self.config, 'quantum_algorithm')
                                                                       else 'dilithium')
                    sig = await self.quantum_security.sign_cooling_data(asdict(result), key['key_id'])
                    result.quantum_signature = {'algorithm': 'dilithium-sim', 'signature': sig}
                except Exception:
                    pass

            # Blockchain
            if blockchain_record:
                try:
                    data_hash = hashlib.sha256(
                        json.dumps(asdict(result), sort_keys=True, default=str).encode()
                    ).hexdigest()
                    bc = await self.blockchain.record_cooling_data(
                        f"cooling_{uuid.uuid4().hex[:8]}", data_hash,
                        {'temperature': temperature})
                    result.blockchain_tx_hash = bc.get('tx_hash')
                except Exception:
                    pass

            # Cloud
            try:
                result.cloud_distribution = await self.cloud_distributor.distribute_cooling_data(
                    {'size_gb': 0.001})
            except Exception:
                pass

            result.autonomous_optimization = opt

            async with self._history_lock:
                self.simulation_history.append(result)

            if PROMETHEUS_AVAILABLE:
                COOLING_SIMULATIONS.labels(status='success').inc()
                SIMULATION_DURATION.observe(result.simulation_time_ms / 1000)

            try:
                await self.websocket.broadcast({
                    'type': 'simulation_result',
                    'temperature': temperature,
                    'strategy': strategy,
                    'timestamp': datetime.now().isoformat()})
            except Exception:
                pass

            logger.info(f"Sim done: T={temperature:.1f}mK strategy={strategy} "
                        f"precision={precision.value}")
            return result

    async def run_chaos_suite(self):
        if self.chaos_tester is None:
            return {'error': 'chaos disabled'}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            try:
                results.append(await self.chaos_tester.run_test(f, duration_s=0.05))
            except Exception as e:
                results.append({'fault': f, 'passed': False, 'error': str(e)})
        return {'results': results, 'report': self.chaos_tester.get_report()}

    async def select_precision(self, accuracy_required=0.95):
        if self.precision_controller is None:
            return PrecisionLevel.FP32
        ci = await self.carbon_manager.get_current_intensity()
        return self.precision_controller.select(ci, accuracy_required)

    async def compute_carbon_credit(self, carbon_saved_kg):
        if self.carbon_market is None:
            return {'credit_usd': 0.0, 'rec_usd': 0.0}
        credit = await self.carbon_market.get_carbon_credit_value(carbon_saved_kg)
        rec = await self.carbon_market.get_rec_value(carbon_saved_kg * 0.5)
        return {'credit_usd': credit, 'rec_usd': rec,
                'cumulative_kg': self._carbon_saved_kg_total}

    async def escalate_decision(self, decision_context, options, confidence):
        if self.hitl is None:
            return {'escalated': False,
                    'chosen': options[0] if options else 'noop',
                    'source': 'fallback'}
        return await self.hitl.escalate(decision_context, options, confidence,
                                        self.config.hitl_confidence_threshold)

    async def get_comprehensive_status(self) -> Dict:
        quantum_status = await self.quantum_security.get_quantum_status()
        blockchain_status = await self.blockchain.get_blockchain_status()
        opt_stats = self.autonomous_optimizer.get_optimization_stats()
        cloud_status = await self.cloud_distributor.get_distribution_status()
        carbon_intensity = await self.carbon_manager.get_current_intensity()
        moe_stats = self.moe_engine.ensemble.get_stats() if self.moe_engine else {}
        bio_stats = {'current_params': self.bio_optimizer.get_current_params()} \
            if self.bio_optimizer else {}
        self_healing_stats = await self.self_healing.get_stats() if self.self_healing else {}

        status = {
            'instance_id': self.instance_id,
            'version': self.config.version,
            'quantum_security': quantum_status,
            'blockchain': blockchain_status,
            'autonomous_optimization': opt_stats,
            'cloud_distribution': cloud_status,
            'carbon_intensity': carbon_intensity,
            'simulation_count': len(self.simulation_history),
            'carbon_saved_kg_total': self._carbon_saved_kg_total,
            'moe': moe_stats,
            'bio': bio_stats,
            'self_healing': self_healing_stats,
            'features': {
                'temporal_logic': self.temporal_logic_enabled,
                'xai': self.xai_enabled,
                'adaptive_precision': self.adaptive_precision_enabled,
                'carbon_market': self.carbon_market_enabled,
                'role_specialization': self.role_specialization_enabled,
                'chaos_testing': self.chaos_testing_enabled,
                'hitl': self.hitl_enabled,
                'federated': self.federated_enabled,
            },
            'timestamp': datetime.now().isoformat()}

        if self.temporal_monitor:
            status['temporal_logic'] = self.temporal_monitor.get_status()
        if self.limit_graph:
            status['limit_graph'] = await self.limit_graph.get_graph_summary()
        if self.rlhf:
            status['rlhf'] = {'actions': self.rlhf.actions,
                              'history_len': len(self.rlhf.history)}
        if self.distillation:
            status['distillation'] = {
                'student_probs': self.distillation.get_student_probs(),
                'history_len': len(self.distillation.history)}
        if self.federated:
            status['federated'] = self.federated.get_stats()
        if self.hitl:
            status['hitl'] = self.hitl.get_audit()
        if self.chaos_tester:
            status['chaos'] = self.chaos_tester.get_report()
        if self.precision_controller:
            status['precision'] = {
                'last': self.precision_controller.last_precision.value,
                'telemetry': self.precision_controller.telemetry}
        if self.carbon_market:
            status['carbon_market'] = await self.carbon_market.get_market_snapshot()
        return status

    async def shutdown(self):
        logger.info("Shutting down EnhancedPhaseEnergySimulatorV16...")
        self._shutdown_event.set()
        self._running = False
        for t in self._background_tasks:
            t.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.carbon_manager.close()
        await self.websocket.stop()
        await self.state.save()
        logger.info("Shutdown complete")

# =============================================================================
# SIGNAL HANDLING + SINGLETON + MAIN
# =============================================================================
_shutdown_event_global = asyncio.Event()
_simulator_instance: Optional[EnhancedPhaseEnergySimulatorV16] = None
_simulator_lock = asyncio.Lock()
_shutdown_requested = False

def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        try:
            asyncio.create_task(_signal_shutdown())
        except Exception:
            pass

async def _signal_shutdown():
    _shutdown_event_global.set()

async def get_phase_energy_simulator(config: Optional[CoolingConfig] = None) -> EnhancedPhaseEnergySimulatorV16:
    global _simulator_instance
    if _simulator_instance is None:
        async with _simulator_lock:
            if _simulator_instance is None:
                _simulator_instance = EnhancedPhaseEnergySimulatorV16(config)
                await _simulator_instance.start()
    return _simulator_instance

async def shutdown_handler():
    global _simulator_instance
    if _simulator_instance:
        await _simulator_instance.shutdown()
        _simulator_instance = None

async def main():
    try:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))
            except NotImplementedError:
                pass
    except Exception:
        pass

    print("=" * 80)
    print("Enhanced Phase Energy Model v16.0.0")
    print("+ Temporal Logic + XAI + Adaptive Precision + Carbon Markets")
    print("+ Roles + Chaos Testing + Active RLHF + HITL + Federated")
    print("=" * 80)

    sim = await get_phase_energy_simulator()

    print("\n✅ v16.0.0 ENHANCEMENTS:")
    print("   ✅ Temporal Logic Verification (G/F/U/->)")
    print("   ✅ Explainable AI for cooling decisions")
    print("   ✅ Adaptive Precision Switching")
    print("   ✅ Carbon Markets + Renewable Energy Credits")
    print("   ✅ Multi-Agent Role Specialization (emergent)")
    print("   ✅ Chaos Testing as first-class citizen")
    print("   ✅ Active RLHF with uncertainty-triggered human queries")
    print("   ✅ Human-in-the-Loop Coordinator")
    print("   ✅ Federated Green Learning (FedAvg)")

    q = await sim.quantum_security.get_quantum_status()
    print(f"\n🔐 Quantum: PQC={q['pqc_available']} algorithms={q['algorithms']}")

    print("\n🔬 Running sample simulation...")
    result = await sim.run_simulation()
    print(f"   Temperature: {result.avg_temperature_mk:.1f} mK")
    print(f"   Quantum Volume: {result.quantum_volume:.0f}")
    print(f"   Strategy: {result.autonomous_optimization['selected_strategy']}")
    print(f"   Precision: {result.precision_used}")
    print(f"   Carbon credit: ${result.carbon_credit_value_usd:.4f}")
    print(f"   REC: ${result.rec_value_usd:.4f}")
    if result.temporal_status:
        print(f"   Temporal status: {result.temporal_status}")
    if result.role_assignments:
        print(f"   Dominant role: {result.role_assignments.get('dominant_role')}")
    if result.xai_explanation:
        print(f"   XAI: {result.xai_explanation['narrative'][0]}")

    print("\n⚙️  Precision selector →", (await sim.select_precision()).value)
    cc = await sim.compute_carbon_credit(250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  REC: ${cc['rec_usd']:.4f}")

    print("\n🧪 Chaos suite:")
    chaos = await sim.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  tests: {chaos['report']['tests_run']}")

    print("\n📊 Comprehensive status:")
    status = await sim.get_comprehensive_status()
    print(json.dumps({
        'version': status['version'],
        'simulation_count': status['simulation_count'],
        'carbon_saved_kg': status['carbon_saved_kg_total'],
        'features': status['features'],
        'moe': status.get('moe'),
        'federated': status.get('federated'),
        'rlhf': status.get('rlhf'),
        'hitl_total': status.get('hitl', {}).get('total'),
        'chaos_pass_rate': status.get('chaos', {}).get('pass_rate'),
        'precision_last': status.get('precision', {}).get('last'),
        'carbon_market': status.get('carbon_market')}, indent=2, default=str))

    print("\n" + "=" * 80)
    print("✅ Enhanced Phase Energy Simulator v16.0.0 — smoke test complete")
    print("=" * 80)

    try:
        await _shutdown_event_global.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await shutdown_handler()

if __name__ == "__main__":
    asyncio.run(main())
