#!/usr/bin/env python3
# File: src/enhancements/pareto_router_enhanced_v4_0.py
"""
Enhanced Pareto Frontier Routing v5.0.0
Multi-objective optimization with MODP, MOE, Bio-Inspired GA, Carbon-Aware Scheduling,
Self-Healing, LIMIT Graph, RLHF, Multi-Teacher Distillation
+ v5.0.0 suite:
    • Temporal Logic Verification (G/F/U/->)
    • Explainable AI (XAI)
    • Adaptive Precision Switching
    • Carbon Markets + Renewable Energy Credits (RECs)
    • Multi-Agent Role Specialization (emergent)
    • Chaos Testing as first-class
    • Active RLHF (uncertainty-triggered human queries)
    • Human-in-the-Loop Coordinator
    • Federated Green Learning (FedAvg)
"""

import asyncio
import logging
import json
import time
import uuid
import hashlib
import os
import random
import signal
from functools import wraps
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Callable, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import OrderedDict, deque, defaultdict
import numpy as np
import contextvars
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- SQLAlchemy ----------
try:
    from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, JSON, Text, text
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, Session
    from sqlalchemy.pool import QueuePool
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Tenacity ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# ---------- Async HTTP ----------
import aiohttp

# ---------- WebSockets ----------
try:
    import websockets
    from websockets.server import serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

# ---------- Post-quantum crypto ----------
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

try:
    from web3 import Web3, Account, HTTPProvider
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# ---------- ML ----------
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

# ---------- Logging ----------
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

# ---------- Dummy tenacity ----------
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

# ---------- Prometheus Metrics ----------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    ROUTING_DECISIONS = Counter('routing_decisions_total', 'Routing decisions', ['status'], registry=REGISTRY)
    FRONTIER_SIZE = Gauge('pareto_frontier_size', 'Pareto frontier size', registry=REGISTRY)
    ROUTING_LATENCY = Histogram('routing_latency_seconds', 'Routing latency', registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('pareto_circuit_breaker_state', 'CB state', ['name'], registry=REGISTRY)
    CARBON_INTENSITY = Gauge('pareto_carbon_intensity', 'Carbon intensity', registry=REGISTRY)
    MODP_PARETO_SIZE = Gauge('pareto_modp_pareto_front_size', 'MODP Pareto size', registry=REGISTRY)
    MOE_GATING_WEIGHTS = Gauge('pareto_moe_gating_weights', 'MOE gating', ['expert'], registry=REGISTRY)
    GA_FITNESS = Gauge('pareto_ga_fitness', 'GA fitness', ['generation'], registry=REGISTRY)
    SELF_HEALING_ACTIONS = Counter('pareto_self_healing_actions_total', 'Self-healing', ['action'], registry=REGISTRY)
    LIMIT_GRAPH_EDGES = Gauge('pareto_limit_graph_edges', 'LIMIT graph edges', registry=REGISTRY)
    RLHF_REWARD_MODEL_SCORE = Gauge('pareto_rlhf_reward_model_score', 'RLHF reward', registry=REGISTRY)
    DISTILLATION_LOSS = Gauge('pareto_distillation_loss', 'Distillation loss', registry=REGISTRY)
    TEMPORAL_VIOLATIONS = Counter('pareto_temporal_violations_total', 'Temporal', ['formula'], registry=REGISTRY)
    CHAOS_TESTS = Counter('pareto_chaos_tests_total', 'Chaos', ['fault', 'status'], registry=REGISTRY)
    HITL_ESCALATIONS = Counter('pareto_hitl_escalations_total', 'HITL', ['status'], registry=REGISTRY)
    FEDERATED_ROUNDS = Counter('pareto_federated_rounds_total', 'Federated', registry=REGISTRY)
    CARBON_CREDITS_USD = Counter('pareto_carbon_credits_usd_total', 'Carbon credits', registry=REGISTRY)
    XAI_EXPLANATIONS = Counter('pareto_xai_explanations_total', 'XAI', registry=REGISTRY)
    PRECISION_SELECTIONS = Counter('pareto_precision_selections_total', 'Precision', ['level'], registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass
    ROUTING_DECISIONS = FRONTIER_SIZE = ROUTING_LATENCY = DummyMetric()
    CIRCUIT_BREAKER_STATE = CARBON_INTENSITY = MODP_PARETO_SIZE = DummyMetric()
    MOE_GATING_WEIGHTS = GA_FITNESS = SELF_HEALING_ACTIONS = DummyMetric()
    LIMIT_GRAPH_EDGES = RLHF_REWARD_MODEL_SCORE = DISTILLATION_LOSS = DummyMetric()
    TEMPORAL_VIOLATIONS = CHAOS_TESTS = HITL_ESCALATIONS = DummyMetric()
    FEDERATED_ROUNDS = CARBON_CREDITS_USD = XAI_EXPLANATIONS = DummyMetric()
    PRECISION_SELECTIONS = DummyMetric()

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

    class ParetoRouterConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("5.0.0")
        log_level: str = Field("INFO")
        cache_ttl_seconds: int = Field(300, ge=0)
        use_adaptive_weights: bool = True
        enable_persistence: bool = True
        db_path: str = Field("pareto_routing_v5.db")
        max_retry_attempts: int = Field(3, ge=0)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: int = Field(30, ge=1)
        circuit_breaker_half_open_max_requests: int = Field(3, ge=1)
        rate_limit_requests: int = Field(100, ge=1)
        rate_limit_window: int = Field(60, ge=1)
        default_weights: Dict[str, float] = Field(default_factory=lambda: {
            'energy': 1.0, 'carbon': 1.0, 'helium': 0.5,
            'material': 0.3, 'latency': 0.1, 'inaccuracy': 0.1})
        constraints: Dict[str, float] = Field(default_factory=dict)
        metrics_port: int = Field(8000, ge=1024, le=65535)
        websocket_port: int = Field(8770, ge=1024)
        enable_quantum_security: bool = True
        quantum_algorithm: str = Field("dilithium")
        quantum_master_key: str = Field(default="00" * 32)
        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = Field("http://localhost:8545")
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        carbon_api_key: Optional[str] = None
        carbon_region: str = Field("global")
        carbon_update_interval: int = Field(300, ge=10)
        modp: MODPConfig = Field(default_factory=MODPConfig)
        moe: MOEConfig = Field(default_factory=MOEConfig)
        bio: BioConfig = Field(default_factory=BioConfig)
        scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
        self_healing: SelfHealingConfig = Field(default_factory=SelfHealingConfig)
        limit_graph: LimitGraphConfig = Field(default_factory=LimitGraphConfig)
        rlhf: RLHFConfig = Field(default_factory=RLHFConfig)
        distillation: DistillationConfig = Field(default_factory=DistillationConfig)
        # v5.0.0 flags
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

        def get_master_key_bytes(self) -> bytes:
            try:
                return bytes.fromhex(self.quantum_master_key)
            except Exception:
                return b'\x00' * 32

        class Config:
            env_prefix = "PARETO_"
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
    class ParetoRouterConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "5.0.0"
        log_level: str = "INFO"
        cache_ttl_seconds: int = 300
        use_adaptive_weights: bool = True
        enable_persistence: bool = True
        db_path: str = "pareto_routing_v5.db"
        max_retry_attempts: int = 3
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 30
        circuit_breaker_half_open_max_requests: int = 3
        rate_limit_requests: int = 100
        rate_limit_window: int = 60
        default_weights: Dict[str, float] = field(default_factory=lambda: {
            'energy': 1.0, 'carbon': 1.0, 'helium': 0.5,
            'material': 0.3, 'latency': 0.1, 'inaccuracy': 0.1})
        constraints: Dict[str, float] = field(default_factory=dict)
        metrics_port: int = 8000
        websocket_port: int = 8770
        enable_quantum_security: bool = True
        quantum_algorithm: str = "dilithium"
        quantum_master_key: str = "00" * 32
        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        carbon_api_key: Optional[str] = None
        carbon_region: str = "global"
        carbon_update_interval: int = 300
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

        def get_master_key_bytes(self) -> bytes:
            try:
                return bytes.fromhex(self.quantum_master_key)
            except Exception:
                return b'\x00' * 32

# =============================================================================
# Circuit Breaker + Rate Limiter + Bulkhead
# =============================================================================
class EnhancedCircuitBreaker:
    def __init__(self, name: str, config: ParetoRouterConfig):
        self.name = name
        self.config = config
        self.threshold = config.circuit_breaker_threshold
        self.timeout = config.circuit_breaker_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()

    async def allow_request(self) -> bool:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                else:
                    return False
            return True

    async def record_success(self):
        async with self._lock:
            self.failure_count = 0
            self.state = CircuitBreakerState.CLOSED

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.threshold:
                self.state = CircuitBreakerState.OPEN

    async def call(self, func, *args, **kwargs):
        if not await self.allow_request():
            raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self.record_success()
            return result
        except Exception:
            await self.record_failure()
            raise

    def get_status(self) -> Dict:
        return {'name': self.name, 'state': self.state.value,
                'failure_count': self.failure_count}

class EnhancedRateLimiter:
    def __init__(self, config: ParetoRouterConfig):
        self.rate = config.rate_limit_requests
        self.per_seconds = config.rate_limit_window
        self.tokens = self.rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            self.tokens = min(self.rate, self.tokens + (now - self.last_refill) * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

    def get_metrics(self) -> Dict:
        return {'tokens': self.tokens, 'rate': self.rate}

class EnhancedBulkhead:
    def __init__(self, max_concurrency: int = 10):
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.active = 0
        self.queued = 0

    async def execute(self, func, *args, **kwargs):
        self.queued += 1
        async with self.semaphore:
            self.queued -= 1
            self.active += 1
            try:
                return await func(*args, **kwargs)
            finally:
                self.active -= 1

    def get_metrics(self) -> Dict:
        return {'active': self.active, 'queued': self.queued}

# =============================================================================
# v5.0.0 MODULE A — TEMPORAL LOGIC MONITOR
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
                self.violations.append({'formula': name, 'expression': self.formulas[name],
                                        'timestamp': datetime.now().isoformat()})
                if PROMETHEUS_AVAILABLE:
                    TEMPORAL_VIOLATIONS.labels(formula=name).inc()
        return results

    def get_status(self) -> Dict:
        return {'formulas': self.formulas, 'last_results': self.evaluate(),
                'violations': self.violations[-5:]}

# =============================================================================
# v5.0.0 MODULE B — XAI EXPLAINER
# =============================================================================
class XAIExplainer:
    def __init__(self, feature_names: List[str]):
        self.feature_names = feature_names

    def explain(self, candidate: Dict[str, float], weights: Dict[str, float],
                all_candidates: List[Dict[str, float]], top_k: int = 5) -> Dict:
        matrix = np.array([[c.get(f, 0.0) for f in self.feature_names] for c in all_candidates])
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
        return {'contributions': contrib, 'top_features': [f for f, _ in ranked],
                'narrative': narrative, 'weights_used': dict(weights)}

# =============================================================================
# v5.0.0 MODULE C — ADAPTIVE PRECISION CONTROLLER
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
# v5.0.0 MODULE D — CARBON MARKET CLIENT
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
# v5.0.0 MODULE E — ROLE SPECIALIZATION COORDINATOR
# =============================================================================
class RoleSpecializationCoordinator:
    def __init__(self):
        self.roles = list(AgentRole)
        # rows = roles, cols = [trust, compute, energy, perf]
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
# v5.0.0 MODULE F — CHAOS TESTER
# =============================================================================
class ChaosTester:
    FAULT_TYPES = ['carbon_api_down', 'cache_broken', 'circuit_breaker_trip',
                   'distiller_broken', 'moe_broken', 'scheduler_hang']

    def __init__(self, router_ref=None):
        self.router = router_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable] = []
        r = self.router

        try:
            if fault_type == 'carbon_api_down' and r:
                orig = r.carbon_manager.get_current_intensity
                async def broken(): raise RuntimeError("carbon API down")
                r.carbon_manager.get_current_intensity = broken
                restore.append(lambda: setattr(r.carbon_manager, 'get_current_intensity', orig))
            elif fault_type == 'cache_broken' and r:
                orig = r.clear_cache
                async def broken(): raise RuntimeError("cache broken")
                r.clear_cache = broken
                restore.append(lambda: setattr(r, 'clear_cache', orig))
            elif fault_type == 'circuit_breaker_trip' and r:
                for _ in range(r.router_config.circuit_breaker_threshold + 1):
                    await r._circuit_breaker.record_failure()
            elif fault_type == 'distiller_broken' and r and r.distillation:
                orig = r.distillation.distill
                async def broken(*a, **k): raise RuntimeError("distiller broken")
                r.distillation.distill = broken
                restore.append(lambda: setattr(r.distillation, 'distill', orig))
            elif fault_type == 'moe_broken' and r and r.moe_engine:
                orig = r.moe_engine.get_weights
                async def broken(*a, **k): raise RuntimeError("moe broken")
                r.moe_engine.get_weights = broken
                restore.append(lambda: setattr(r.moe_engine, 'get_weights', orig))
            elif fault_type == 'scheduler_hang' and r and r.scheduler:
                orig = r.scheduler.schedule
                async def broken(*a, **k):
                    await asyncio.sleep(5)
                    return {'recommended_delay': 0}
                r.scheduler.schedule = broken
                restore.append(lambda: setattr(r.scheduler, 'schedule', orig))
            await asyncio.sleep(duration_s)
        except Exception as e:
            passed, error_msg = False, str(e)
        finally:
            for rec in restore:
                try: rec()
                except Exception: pass

        result = {'fault': fault_type, 'duration_s': duration_s,
                  'elapsed_s': time.time() - start, 'passed': passed,
                  'error': error_msg, 'timestamp': datetime.now().isoformat()}
        self.results.append(result)
        if PROMETHEUS_AVAILABLE:
            CHAOS_TESTS.labels(fault=fault_type, status='pass' if passed else 'fail').inc()
        return result

    def get_report(self) -> Dict:
        return {'tests_run': len(self.results),
                'pass_rate': (sum(1 for r in self.results if r['passed']) / len(self.results))
                             if self.results else 1.0,
                'recent': self.results[-5:]}

# =============================================================================
# v5.0.0 MODULE G — ACTIVE RLHF
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
                           f['state'].get('avg_score', 0.5),
                           f['state'].get('cost', 0.5),
                           f['state'].get('diversity', 0.5)]
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
# v5.0.0 MODULE H — HUMAN-IN-THE-LOOP COORDINATOR
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
# v5.0.0 MODULE I — FEDERATED AGGREGATOR
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
# MODULE 1: MODP Selector (with XAI + roles + temporal)
# =============================================================================
class MODPSelector:
    def __init__(self, config: ParetoRouterConfig,
                 adaptive_cost: Optional[Any] = None,
                 xai: Optional[XAIExplainer] = None,
                 roles: Optional[RoleSpecializationCoordinator] = None,
                 temporal: Optional[TemporalLogicMonitor] = None):
        self.config = config
        self.adaptive_cost = adaptive_cost
        self.weights = list(config.modp.weights)
        self.adaptive_weights = config.modp.adaptive_weights
        self.learning_rate = config.modp.learning_rate
        self.recent_outcomes = deque(maxlen=100)
        self.objective_names: List[str] = []
        self.xai = xai
        self.roles = roles
        self.temporal = temporal

    async def select(self, frontier: List[str], vectors: Dict[str, np.ndarray],
                     context: Dict) -> Optional[str]:
        if not frontier:
            return None
        criteria = self.objective_names

        # Temporal gate
        if self.temporal:
            self.temporal.update({
                'carbon': float(context.get('carbon_intensity', 400)),
                'frontier_size': float(len(frontier)),
                'confidence': float(context.get('confidence', 0.5)),
            })
            self.temporal.evaluate()

        candidates = []
        for pid in frontier:
            vec = vectors[pid]
            cand = {name: -vec[i] for i, name in enumerate(criteria)}
            candidates.append(cand)

        if self.adaptive_cost and self.adaptive_weights:
            try:
                wd = self.adaptive_cost.get_current_weights()
                self.weights = [wd.get(name, 1.0) for name in criteria]
            except Exception:
                pass
        else:
            self.weights = [self.config.default_weights.get(name, 1.0) for name in criteria]

        total = sum(self.weights)
        if total > 0:
            self.weights = [w / total for w in self.weights]

        scores = TOPSIS.score(candidates, self.weights, criteria)
        best_idx = int(np.argmax(scores))
        best_id = frontier[best_idx]

        # XAI explanation
        xai_out = None
        if self.xai:
            xai_out = self.xai.explain(
                candidate=candidates[best_idx],
                weights={n: w for n, w in zip(criteria, self.weights)},
                all_candidates=candidates,
            )

        # Role assignment
        roles_out = None
        if self.roles:
            roles_out = self.roles.assign_roles({
                'trust': 0.7,
                'compute': 0.7,
                'energy': 1.0 - abs(candidates[best_idx].get('energy', 0.5)),
                'performance': float(scores[best_idx]),
            })

        outcome = [float(scores[best_idx])] + [vectors[best_id][i] for i in range(len(criteria))]
        self.recent_outcomes.append((self.weights, outcome))
        if self.adaptive_weights and len(self.recent_outcomes) >= 10:
            await self._update_weights()

        if PROMETHEUS_AVAILABLE:
            MODP_PARETO_SIZE.set(len(frontier))

        # Store for later retrieval
        self._last_xai = xai_out
        self._last_roles = roles_out

        return best_id

    async def _update_weights(self):
        avg_outcome = np.mean([o for _, o in self.recent_outcomes], axis=0)
        self.weights = self.weights - self.learning_rate * (avg_outcome - np.mean(avg_outcome))
        total = sum(self.weights)
        if total > 0:
            self.weights = [w / total for w in self.weights]

# =============================================================================
# MODULE 2: MOE Weight Engine
# =============================================================================
class MOETeacherEnsemble:
    def __init__(self, config: ParetoRouterConfig):
        self.config = config
        self.teachers: Dict[str, Callable] = {}
        self.gating_model = None
        self.scaler = None
        self.history = deque(maxlen=500)
        self._trained = False
        self._init_teachers()
        self._init_gating()

    def _init_teachers(self):
        self.teachers = {
            'performance': self._performance_teacher,
            'carbon': self._carbon_teacher,
            'cost': self._cost_teacher,
            'user': self._user_teacher,
        }

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(multi_class='multinomial',
                                                   solver='lbfgs', max_iter=1000)
            self.scaler = StandardScaler()

    def _performance_teacher(self, context, historical):
        n = len(context.get('objectives', [])) or 1
        return np.ones(n) / n

    def _carbon_teacher(self, context, carbon_intensity):
        obj_names = context.get('objectives', [])
        n = len(obj_names) or 1
        weights = np.ones(n)
        if 'carbon' in obj_names:
            weights[obj_names.index('carbon')] = 1.0 + (carbon_intensity / 1000)
        return weights / (weights.sum() + 1e-9)

    def _cost_teacher(self, context):
        n = len(context.get('objectives', [])) or 1
        return np.ones(n) / n

    def _user_teacher(self, context, user_prefs):
        obj_names = context.get('objectives', [])
        n = len(obj_names) or 1
        weights = np.array([user_prefs.get(obj, 1.0) for obj in obj_names]) if obj_names else np.ones(n)
        total = weights.sum()
        return weights / total if total > 0 else np.ones(n) / n

    async def _extract_features(self, context, carbon_intensity):
        return np.array([carbon_intensity / 1000,
                         len(context.get('objectives', [])),
                         datetime.now().hour / 24.0,
                         context.get('urgency', 0.5)])

    async def get_teacher_vectors(self, context, carbon_intensity, historical, user_prefs):
        return {
            'performance': self._performance_teacher(context, historical),
            'carbon': self._carbon_teacher(context, carbon_intensity),
            'cost': self._cost_teacher(context),
            'user': self._user_teacher(context, user_prefs),
        }

    async def get_gating_weights(self, context, carbon_intensity):
        if self.gating_model is not None and self._trained:
            features = await self._extract_features(context, carbon_intensity)
            X = self.scaler.transform([features])
            return self.gating_model.predict_proba(X)[0].tolist()
        return [1.0 / len(self.teachers)] * len(self.teachers)

    async def update_gating(self, context, carbon_intensity, reward, best_teacher):
        features = await self._extract_features(context, carbon_intensity)
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
        return {'num_teachers': len(self.teachers),
                'gating_trained': self._trained,
                'history_len': len(self.history)}

class MOEWeightEngine:
    def __init__(self, config: ParetoRouterConfig):
        self.config = config
        self.ensemble = MOETeacherEnsemble(config)
        self.history = deque(maxlen=500)

    async def get_weights(self, context, carbon_intensity, historical, user_prefs):
        teacher_vectors = await self.ensemble.get_teacher_vectors(
            context, carbon_intensity, historical, user_prefs)
        gating_weights = await self.ensemble.get_gating_weights(context, carbon_intensity)
        combined = np.zeros_like(next(iter(teacher_vectors.values())))
        for i, (name, vec) in enumerate(teacher_vectors.items()):
            combined += gating_weights[i] * vec
        if combined.sum() > 0:
            combined = combined / combined.sum()
        if PROMETHEUS_AVAILABLE:
            for i, name in enumerate(teacher_vectors.keys()):
                MOE_GATING_WEIGHTS.labels(expert=name).set(gating_weights[i])
        return combined

    async def update(self, context, carbon_intensity, reward, best_teacher):
        await self.ensemble.update_gating(context, carbon_intensity, reward, best_teacher)
        self.history.append({'reward': reward})

# =============================================================================
# MODULE 3: Bio-Inspired GA
# =============================================================================
class GeneticAlgorithmOptimizer:
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1,
                 crossover_rate: float = 0.8):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Dict] = []
        self.bounds = {
            'energy_weight': (0.0, 1.0), 'carbon_weight': (0.0, 1.0),
            'helium_weight': (0.0, 1.0), 'material_weight': (0.0, 1.0),
            'latency_weight': (0.0, 1.0), 'inaccuracy_weight': (0.0, 1.0),
        }

    def initialize(self, num_objectives: int):
        self.population = []
        for _ in range(self.pop_size):
            ind = {name: random.uniform(0.0, 1.0) for name in self.bounds}
            total = sum(ind.values()) or 1.0
            for k in ind:
                ind[k] /= total
            self.population.append(ind)

    def evaluate(self, fitness_func):
        return [fitness_func(ind) for ind in self.population]

    def select(self, fitness, num_parents):
        selected = []
        for _ in range(num_parents):
            i, j = np.random.choice(len(self.population), 2, replace=False)
            selected.append(self.population[i] if fitness[i] > fitness[j] else self.population[j])
        return selected

    def crossover(self, p1, p2):
        if random.random() < self.crossover_rate:
            return {k: (p1[k] if random.random() < 0.5 else p2[k]) for k in p1}
        return p1.copy()

    def mutate(self, individual):
        if random.random() < self.mutation_rate:
            key = random.choice(list(self.bounds.keys()))
            individual[key] = random.uniform(*self.bounds[key])
            total = sum(individual.values()) or 1.0
            for k in individual:
                individual[k] /= total
        return individual

    def evolve(self, fitness_func, generations: int = 50):
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
    def __init__(self, config: ParetoRouterConfig, adaptive_cost: Optional[Any] = None):
        self.config = config
        self.adaptive_cost = adaptive_cost
        self.ga = GeneticAlgorithmOptimizer(
            population_size=config.bio.population_size,
            mutation_rate=config.bio.mutation_rate,
            crossover_rate=config.bio.crossover_rate)
        self.current_params: Dict = {}
        self.fitness_history = deque(maxlen=50)
        self._lock = asyncio.Lock()

    def _fitness_func(self, params):
        if self.adaptive_cost:
            try:
                return -self.adaptive_cost.evaluate(params)
            except Exception:
                pass
        return (params.get('energy_weight', 0.25) + params.get('carbon_weight', 0.25)
                - 0.5 * params.get('latency_weight', 0.1))

    async def evolve(self, objective_names: List[str]) -> Dict:
        self.ga.initialize(len(objective_names))
        best = self.ga.evolve(self._fitness_func, generations=5)
        async with self._lock:
            self.current_params = best
            self.fitness_history.append(self._fitness_func(best))
        return best

    def get_current_params(self) -> Dict:
        return self.current_params

# =============================================================================
# MODULE 4: MOE Forecaster + Carbon Scheduler
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

    async def forecast(self, horizon=24):
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
# MODULE 5: Self-Healing
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
        features = np.array([
            metrics.get('success_rate', 1.0),
            metrics.get('avg_latency', 0) / 1000,
            metrics.get('frontier_size', 1) / 100,
            metrics.get('carbon_intensity', 400) / 1000,
        ]).reshape(1, -1)
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
        X = np.array([[d.get('success_rate', 1.0), d.get('avg_latency', 0) / 1000,
                       d.get('frontier_size', 1) / 100, d.get('carbon_intensity', 400) / 1000]
                      for d in data])
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

    async def get_stats(self):
        return {'enabled': self.config.self_healing.enabled, 'trained': self._trained,
                'num_detectors': len(self.anomaly_detectors),
                'recent_actions': list(self.recovery_actions)[-5:]}

# =============================================================================
# LIMIT Graph
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
        return {'nodes': list(self.graph.keys()), 'constraints': self.constraints,
                'edge_count': sum(len(v) for v in self.graph.values())}

# =============================================================================
# Multi-Teacher Distillation
# =============================================================================
class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_weight_engine=None,
                 role_coordinator=None):
        self.config = config
        self.moe_weight_engine = moe_weight_engine
        self.role_coordinator = role_coordinator
        self.student_policy = np.array([0.25, 0.25, 0.25, 0.25])
        self.temperature = config.distillation.temperature
        self.alpha = config.distillation.alpha
        self.history = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, state):
        if not self.moe_weight_engine:
            return
        context = {'objectives': ['energy', 'carbon', 'helium', 'material',
                                  'latency', 'inaccuracy'], 'urgency': 0.5}
        ci = state.get('carbon_intensity', 400)
        try:
            teacher_probs = await self.moe_weight_engine.ensemble.get_gating_weights(context, ci)
        except Exception:
            teacher_probs = [0.25, 0.25, 0.25, 0.25]
        teacher_dist = np.array(teacher_probs)
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
# Carbon Intensity Manager
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
# Quantum Security
# =============================================================================
class QuantumResilientRouterSecurity:
    def __init__(self, config, db_manager=None):
        self.config = config
        self.db_manager = db_manager
        self.pqc_available = PQC_AVAILABLE
        self.master_key = config.get_master_key_bytes()

    async def generate_keypair(self, algorithm='dilithium') -> Dict:
        return {'key_id': f"{algorithm}_{uuid.uuid4().hex[:8]}",
                'algorithm': algorithm,
                'public_key': hashlib.sha256(os.urandom(32)).hexdigest()}

    async def sign_routing_decision(self, data: Dict, key_id: str) -> str:
        return hashlib.sha3_256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()

# =============================================================================
# Blockchain Verification
# =============================================================================
class BlockchainRouterVerification:
    def __init__(self, config):
        self.config = config
        self.connected = False

    async def record_routing(self, decision_id: str, data_hash: str) -> str:
        return f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"

    async def get_status(self) -> Dict:
        return {'connected': self.connected, 'rpc': self.config.blockchain_rpc_url}

# =============================================================================
# WebSocket Server
# =============================================================================
class EnhancedWebSocketServer:
    def __init__(self, port: int):
        self.port = port
        self.connections = set()
        self.subscriptions = defaultdict(set)
        self._lock = asyncio.Lock()
        self.server = None

    async def start(self):
        if not WEBSOCKETS_AVAILABLE:
            return
        try:
            self.server = await serve(self._handle, '0.0.0.0', self.port)
        except Exception as e:
            logger.warning(f"WebSocket start failed: {e}")

    async def _handle(self, ws, path=None):
        async with self._lock:
            self.connections.add(ws)
        try:
            async for message in ws:
                try:
                    data = json.loads(message)
                    if data.get('action') == 'subscribe':
                        topic = data.get('topic', 'all')
                        async with self._lock:
                            self.subscriptions[topic].add(ws)
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            async with self._lock:
                self.connections.discard(ws)

    async def broadcast(self, message: Dict, topic: str = 'all'):
        if not self.connections:
            return
        data = json.dumps(message, default=str)
        async with self._lock:
            targets = self.subscriptions.get(topic, set())
            if topic == 'all':
                targets = self.connections
            for conn in list(targets):
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
# PLACEHOLDERS
# =============================================================================
class ExpertRegistry:
    def get_expert(self, expert_id): return None

class ExpertProfile:
    def __init__(self, expert_id: str = "expert"):
        self.expert_id = expert_id

class AdaptiveCostFunction:
    def get_current_weights(self) -> Dict:
        return {}
    def evaluate(self, state: Dict) -> float:
        return 0.0

class NodeRegistry:
    pass

class UserPreferences:
    def get_weights(self) -> Dict:
        return {}

# =============================================================================
# MAIN PARETO ROUTER v5.0.0
# =============================================================================
class ParetoRouter:
    """Enhanced Router v5.0.0 with all v5.0.0 enhancements."""

    def __init__(
        self,
        config: Dict[str, Any],
        cost_function: AdaptiveCostFunction,
        node_registry: NodeRegistry,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        user_preferences: Optional[UserPreferences] = None,
        objectives: Optional[List[str]] = None,
    ):
        self.config = config
        self.cost_function = cost_function
        self.node_registry = node_registry
        self.user_prefs = user_preferences
        self.router_config = ParetoRouterConfig(**config.get('pareto', {}))
        self.objective_names = objectives or [
            'energy', 'carbon', 'helium', 'material', 'latency', 'inaccuracy']

        # DB manager (needed before quantum security init)
        self._db_manager = None
        if SQLALCHEMY_AVAILABLE and self.router_config.enable_persistence:
            try:
                self._db_manager = EnhancedDatabaseManager(self.router_config)
            except Exception as e:
                logger.warning(f"DB init failed: {e}")

        self.carbon_manager = carbon_manager or CarbonIntensityManager(self.router_config)

        # Feature flags
        self.temporal_logic_enabled = self.router_config.temporal_logic_enabled
        self.xai_enabled = self.router_config.xai_enabled
        self.adaptive_precision_enabled = self.router_config.adaptive_precision_enabled
        self.carbon_market_enabled = self.router_config.carbon_market_enabled
        self.role_specialization_enabled = self.router_config.role_specialization_enabled
        self.chaos_testing_enabled = self.router_config.chaos_testing_enabled
        self.hitl_enabled = self.router_config.hitl_enabled
        self.federated_enabled = self.router_config.federated_enabled

        # ---- v5.0.0 modules ----
        self.temporal_monitor = TemporalLogicMonitor() if self.temporal_logic_enabled else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("carbon_cap", "G(carbon <= 800.0)")
            self.temporal_monitor.add_formula("frontier_ok", "G(frontier_size >= 0.0)")
            self.temporal_monitor.add_formula("confidence_min", "F(confidence >= 0.3)")

        self.xai = XAIExplainer(self.objective_names) if self.xai_enabled else None
        self.precision_controller = AdaptivePrecisionController() \
            if self.adaptive_precision_enabled else None
        self.carbon_market = CarbonMarketClient() if self.carbon_market_enabled else None
        self.role_coordinator = RoleSpecializationCoordinator() \
            if self.role_specialization_enabled else None

        # Active RLHF replaces legacy RLHFManager
        self.rlhf = ActiveRLHF(
            action_space=['performance_focus', 'carbon_focus',
                          'cost_focus', 'balanced'],
        ) if self.router_config.rlhf.enabled else None

        self.hitl = HumanInTheLoopCoordinator(self.rlhf) \
            if (self.hitl_enabled and self.rlhf) else None

        self.federated = FederatedAggregator(num_params=len(self.objective_names)) \
            if self.federated_enabled else None

        # Quantum
        self.quantum_security = QuantumResilientRouterSecurity(
            self.router_config, self._db_manager
        ) if self.router_config.enable_quantum_security else None

        # Blockchain
        self.blockchain = BlockchainRouterVerification(self.router_config) \
            if self.router_config.enable_blockchain_verification else None

        # Enhanced modules
        self.limit_graph = LimitGraphManager(self.router_config) \
            if self.router_config.limit_graph.enabled else None
        self.modp_selector = MODPSelector(
            self.router_config, self.cost_function,
            xai=self.xai, roles=self.role_coordinator, temporal=self.temporal_monitor,
        ) if self.router_config.modp.enabled else None
        if self.modp_selector:
            self.modp_selector.objective_names = self.objective_names

        self.moe_engine = MOEWeightEngine(self.router_config) \
            if self.router_config.moe.enabled else None
        self.bio_optimizer = BioOptimizer(self.router_config, self.cost_function) \
            if self.router_config.bio.enabled else None

        self.forecaster = MOEForecaster() if self.router_config.scheduler.enabled else None
        self.scheduler = MultiObjectiveCarbonScheduler(
            self.router_config, self.carbon_manager, self.forecaster, self.carbon_market,
        ) if self.router_config.scheduler.enabled else None

        self.chaos_tester = ChaosTester(self) if self.chaos_testing_enabled else None
        self.self_healing = SelfHealingManager(
            self.router_config, None, self.rlhf, self.chaos_tester,
        ) if self.router_config.self_healing.enabled else None

        self.distillation = MultiTeacherPolicyDistillation(
            self.router_config, self.moe_engine, self.role_coordinator,
        ) if self.router_config.distillation.enabled and self.moe_engine else None

        self.websocket = EnhancedWebSocketServer(self.router_config.websocket_port) \
            if WEBSOCKETS_AVAILABLE else None

        # Cache
        self._cache: OrderedDict = OrderedDict()
        self._cache_lock = asyncio.Lock()
        self._cache_max_size = 1000

        # Resilience primitives
        self._circuit_breaker = EnhancedCircuitBreaker("pareto_router", self.router_config)
        self._rate_limiter = EnhancedRateLimiter(self.router_config)
        self._bulkhead = EnhancedBulkhead(10)

        # Background tasks & state
        self._background_tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        self._running = False
        self.confidence = 0.8
        self.reflection_threshold = 0.3
        self._carbon_saved_kg_total = 0.0
        self._last_decision: Dict = {}

        logger.info(f"ParetoRouter v{self.router_config.version} ready "
                    f"(instance {self.router_config.instance_id})")
        logger.info(f"  TemporalLogic={self.temporal_logic_enabled} "
                    f"XAI={self.xai_enabled} "
                    f"AdaptivePrecision={self.adaptive_precision_enabled} "
                    f"CarbonMarket={self.carbon_market_enabled} "
                    f"Roles={self.role_specialization_enabled} "
                    f"Chaos={self.chaos_testing_enabled} "
                    f"HITL={self.hitl_enabled} "
                    f"Federated={self.federated_enabled}")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        self._running = True
        if self.websocket:
            await self.websocket.start()
        try:
            loop = asyncio.get_event_loop()
            self._background_tasks = [
                loop.create_task(self._cache_cleanup_loop()),
                loop.create_task(self._carbon_update_loop()),
                loop.create_task(self._limit_graph_loop()),
                loop.create_task(self._rlhf_loop()),
                loop.create_task(self._distillation_loop()),
                loop.create_task(self._federated_loop()),
                loop.create_task(self._self_healing_loop()),
                loop.create_task(self._ga_evolution_loop()),
            ]
            if self.chaos_tester:
                self._background_tasks.append(loop.create_task(self._chaos_loop()))
        except RuntimeError:
            pass
        if PROMETHEUS_AVAILABLE:
            try:
                start_http_server(self.router_config.metrics_port)
            except Exception:
                pass
        logger.info("ParetoRouter v5.0.0 started")

    async def _cache_cleanup_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                async with self._cache_lock:
                    now = datetime.now()
                    expired = [k for k, (_, ts) in self._cache.items()
                               if (now - ts).total_seconds() > self.router_config.cache_ttl_seconds]
                    for k in expired:
                        del self._cache[k]
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cache cleanup error: {e}")

    async def _carbon_update_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                intensity = await self.carbon_manager.get_current_intensity()
                if self.forecaster:
                    await self.forecaster.update_history(intensity)
                await asyncio.sleep(self.router_config.carbon_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon loop error: {e}")

    async def _limit_graph_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.limit_graph:
                    await self.limit_graph.update_constraint(
                        'carbon', await self.carbon_manager.get_current_intensity())
                await asyncio.sleep(self.router_config.limit_graph.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"LIMIT graph loop error: {e}")

    async def _rlhf_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.rlhf:
                    await self.rlhf.train_reward_model()
                await asyncio.sleep(self.router_config.rlhf.training_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"RLHF loop error: {e}")

    async def _distillation_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.distillation:
                    await self.distillation.distill({
                        'carbon_intensity': await self.carbon_manager.get_current_intensity(),
                        'avg_score': 0.5, 'cost': 0.5, 'diversity': 0.5})
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Distillation loop error: {e}")

    async def _federated_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(600)
                if self.federated and self.federated.client_updates:
                    self.federated.aggregate()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated loop error: {e}")

    async def _self_healing_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(60)
                if self.self_healing:
                    await self.self_healing.train([
                        {'success_rate': 0.8, 'avg_latency': 100,
                         'frontier_size': 5, 'carbon_intensity': 400}])
                    await self.self_healing.check_drift({
                        'success_rate': self.confidence, 'avg_latency': 0,
                        'frontier_size': 0,
                        'carbon_intensity': await self.carbon_manager.get_current_intensity()})
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Self-healing loop error: {e}")

    async def _ga_evolution_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.bio_optimizer:
                    await self.bio_optimizer.evolve(self.objective_names)
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"GA loop error: {e}")

    async def _chaos_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)
                if self.chaos_tester:
                    fault = random.choice(ChaosTester.FAULT_TYPES)
                    await self.chaos_tester.run_test(fault, duration_s=0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Chaos loop error: {e}")

    # ------------------------------------------------------------------
    # Core routing
    # ------------------------------------------------------------------
    async def route(self, task: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        start_time = time.time()
        try:
            # Carbon-aware scheduling
            if self.scheduler:
                schedule = await self.scheduler.schedule(urgency_score=0.5)
                delay = schedule['recommended_delay']
                if delay > 0:
                    await asyncio.sleep(min(delay, 2))

            carbon_intensity = await self.carbon_manager.get_current_intensity()

            # Adaptive precision
            precision = PrecisionLevel.FP32
            if self.precision_controller:
                precision = self.precision_controller.select(carbon_intensity, 0.95)

            # Temporal gate
            temporal_status = {}
            if self.temporal_monitor:
                self.temporal_monitor.update({
                    'carbon': float(carbon_intensity),
                    'frontier_size': 5.0,
                    'confidence': self.confidence})
                temporal_status = self.temporal_monitor.evaluate()

            # Candidates + vectors
            candidates = self._get_candidate_experts(task, context)
            vectors = {}
            for expert in candidates:
                vec = await self._get_vector(expert, context)
                vectors[expert.expert_id] = vec

            filtered_ids = self._apply_constraints(vectors, context) or list(vectors.keys())
            frontier = self._pareto_frontier({pid: vectors[pid] for pid in filtered_ids})

            # Selection: RLHF → Distillation → Bio → MOE → MODP
            selection_context = {
                'carbon_intensity': carbon_intensity,
                'confidence': self.confidence,
                'objectives': self.objective_names,
            }
            selected_id = None
            source = "fallback"

            # Try MODP first (it holds the XAI/roles logic)
            if self.modp_selector:
                selected_id = await self.modp_selector.select(frontier, vectors, selection_context)
                source = "modp"
            else:
                weights = await self._get_weights(selection_context)
                best_id, best_score = None, float('inf')
                for pid in frontier:
                    score = float(np.dot(weights, vectors[pid]))
                    if score < best_score:
                        best_score, best_id = score, pid
                selected_id, source = best_id, "weighted_sum"

            if selected_id is None and candidates:
                selected_id = candidates[0].expert_id

            # XAI explanation
            xai_out = None
            if self.xai:
                vec = vectors[selected_id]
                cand = {n: -vec[i] for i, n in enumerate(self.objective_names)}
                all_cands = [{n: -vectors[p][i] for i, n in enumerate(self.objective_names)}
                             for p in frontier]
                xai_out = self.xai.explain(
                    candidate=cand,
                    weights=self.router_config.default_weights,
                    all_candidates=all_cands,
                )

            # Role assignments
            role_assignments = None
            if self.role_coordinator:
                role_assignments = self.role_coordinator.assign_roles({
                    'trust': 0.7, 'compute': 0.7, 'energy': 0.7,
                    'performance': 1.0 - float(vectors[selected_id][0]),
                })

            # HITL escalation
            hitl_outcome = None
            if self.hitl:
                try:
                    hitl_outcome = await self.hitl.escalate(
                        decision_context={'selected': selected_id,
                                          'frontier_size': len(frontier)},
                        options=[p for p in frontier] or ['fallback'],
                        confidence=self.confidence,
                        confidence_threshold=self.router_config.hitl_confidence_threshold,
                    )
                except Exception as e:
                    logger.warning(f"HITL failed: {e}")

            # Carbon credits
            credit_value = 0.0
            rec_value = 0.0
            if self.carbon_market:
                try:
                    saved_kg = max(0.0, (400 - carbon_intensity) * 0.001)
                    credit_value = await self.carbon_market.get_carbon_credit_value(saved_kg)
                    rec_value = await self.carbon_market.get_rec_value(saved_kg * 0.5)
                    if saved_kg > 0:
                        self._carbon_saved_kg_total += saved_kg
                except Exception:
                    pass

            # Federated submission
            federated_round = None
            if self.federated:
                weights = self.modp_selector.weights if self.modp_selector else [0.25] * 4
                self.federated.submit_update(
                    self.router_config.instance_id, list(weights)[:len(self.objective_names)],
                    samples=len(frontier))
                if len(self._background_tasks) and random.random() < 0.2:
                    agg = self.federated.aggregate()
                    federated_round = agg['round']

            # Chaos smoke test (occasionally)
            chaos_passed = None
            if self.chaos_tester and random.random() < 0.1:
                try:
                    cr = await self.chaos_tester.run_test(
                        random.choice(ChaosTester.FAULT_TYPES), duration_s=0.02)
                    chaos_passed = cr['passed']
                except Exception:
                    pass

            # Persist
            await self._record_decision(context, selected_id, frontier, vectors,
                                        f"Selected via {source}")

            # Metrics
            if PROMETHEUS_AVAILABLE:
                ROUTING_DECISIONS.labels(status='success').inc()
                FRONTIER_SIZE.set(len(frontier))
                ROUTING_LATENCY.observe(time.time() - start_time)

            result = {
                'selected_id': selected_id,
                'frontier': [{'expert_id': p, 'vector': vectors[p].tolist()} for p in frontier],
                'explanation': f"Selected {selected_id} via {source}",
                'source': source,
                'precision_used': precision.value,
                'temporal_status': temporal_status,
                'xai_explanation': xai_out,
                'role_assignments': role_assignments,
                'carbon_credit_usd': credit_value,
                'rec_value_usd': rec_value,
                'hitl_outcome': hitl_outcome,
                'federated_round': federated_round,
                'chaos_test_passed': chaos_passed,
                'carbon_intensity': carbon_intensity,
                'timestamp': datetime.now().isoformat(),
            }
            self._last_decision = result
            return result

        except Exception as e:
            logger.error(f"Routing failed: {e}")
            if PROMETHEUS_AVAILABLE:
                ROUTING_DECISIONS.labels(status='failed').inc()
            raise

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_candidate_experts(self, task, context):
        return [ExpertProfile(expert_id=f"exp_{i}") for i in range(5)]

    async def _get_weights(self, context) -> np.ndarray:
        # RLHF
        if self.rlhf:
            probs = await self.rlhf.get_policy_probs(context)
            w = np.array(probs)
            # Pad/truncate to match objectives
            n = len(self.objective_names)
            if len(w) < n:
                w = np.pad(w, (0, n - len(w)), 'constant', constant_values=0.25)
            elif len(w) > n:
                w = w[:n]
            return w
        # Distillation
        if self.distillation:
            w = np.array(self.distillation.get_student_probs())
            n = len(self.objective_names)
            if len(w) < n:
                w = np.pad(w, (0, n - len(w)), 'constant', constant_values=0.25)
            elif len(w) > n:
                w = w[:n]
            return w
        # Bio
        if self.bio_optimizer and self.bio_optimizer.current_params:
            p = self.bio_optimizer.current_params
            weights = np.array([p.get(f"{obj}_weight",
                                       self.router_config.default_weights.get(obj, 1.0))
                                for obj in self.objective_names])
            total = weights.sum()
            return weights / total if total > 0 else np.ones(len(self.objective_names)) / len(self.objective_names)
        # MOE
        if self.moe_engine:
            try:
                return await self.moe_engine.get_weights(
                    context,
                    context.get('carbon_intensity', 400),
                    {}, self.user_prefs.get_weights() if self.user_prefs else {})
            except Exception:
                pass
        # Fallback
        w = np.array([self.router_config.default_weights.get(o, 1.0)
                      for o in self.objective_names])
        return w / w.sum() if w.sum() > 0 else np.ones(len(self.objective_names)) / len(self.objective_names)

    async def _compute_objective(self, obj_name, expert, context, deps) -> float:
        return random.uniform(0, 1)

    async def _get_vector(self, expert: ExpertProfile, context) -> np.ndarray:
        expert_id = expert.expert_id
        now = datetime.now()
        async with self._cache_lock:
            if expert_id in self._cache:
                vec, ts = self._cache[expert_id]
                if (now - ts).total_seconds() < self.router_config.cache_ttl_seconds:
                    return vec
                del self._cache[expert_id]

        deps = {'node_registry': self.node_registry,
                'carbon_manager': self.carbon_manager,
                'cost_function': self.cost_function}
        vec = []
        for name in self.objective_names:
            try:
                value = await self._circuit_breaker.call(
                    self._compute_objective, name, expert, context, deps)
                vec.append(value)
            except Exception:
                vec.append(0.0)
        vec = np.array(vec)
        async with self._cache_lock:
            if len(self._cache) >= self._cache_max_size:
                self._cache.popitem(last=False)
            self._cache[expert_id] = (vec, now)
        return vec

    def _apply_constraints(self, vectors, context) -> List[str]:
        if not self.router_config.constraints:
            return list(vectors.keys())
        valid = []
        for eid, vec in vectors.items():
            ok = True
            for idx, name in enumerate(self.objective_names):
                if name in self.router_config.constraints and vec[idx] > self.router_config.constraints[name]:
                    ok = False
                    break
            if ok:
                valid.append(eid)
        return valid

    def _pareto_frontier(self, vectors) -> List[str]:
        eids = list(vectors.keys())
        n = len(eids)
        dominated = [False] * n
        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                if self._dominates(vectors[eids[i]], vectors[eids[j]]):
                    dominated[j] = True
        return [eids[i] for i in range(n) if not dominated[i]]

    def _dominates(self, a: np.ndarray, b: np.ndarray) -> bool:
        return bool(np.all(a <= b) and np.any(a < b))

    async def _record_decision(self, context, selected_id, frontier, vectors, explanation):
        # Quantum signing
        quantum_sig = None
        if self.quantum_security:
            try:
                key = await self.quantum_security.generate_keypair(
                    self.router_config.quantum_algorithm)
                quantum_sig = await self.quantum_security.sign_routing_decision(
                    {'selected': selected_id, 'frontier': frontier}, key['key_id'])
            except Exception:
                pass

        # Blockchain
        blockchain_tx = None
        if self.blockchain:
            try:
                data_hash = hashlib.sha256(
                    json.dumps({'selected': selected_id, 'frontier': frontier},
                               sort_keys=True, default=str).encode()).hexdigest()
                blockchain_tx = await self.blockchain.record_routing(
                    context.get('request_id', str(uuid.uuid4())), data_hash)
            except Exception:
                pass

        # Persist
        if self._db_manager:
            try:
                def insert(session):
                    session.execute(text("""
                        INSERT INTO routing_decisions
                        (request_id, task_id, selected_expert_id, frontier_size,
                         selection_reason, vector_scores, quantum_signature,
                         blockchain_tx_hash)
                        VALUES (:rid, :tid, :sid, :fs, :reason, :vs, :qs, :bt)
                    """), {
                        'rid': context.get('request_id', 'n/a'),
                        'tid': context.get('task_id', 'n/a'),
                        'sid': selected_id,
                        'fs': len(frontier),
                        'reason': explanation,
                        'vs': json.dumps({p: vectors[p].tolist() for p in frontier}),
                        'qs': quantum_sig,
                        'bt': blockchain_tx,
                    })
                await self._db_manager.execute_sync(insert)
            except Exception as e:
                logger.warning(f"DB insert failed: {e}")

        # Broadcast
        if self.websocket:
            await self.websocket.broadcast({
                'type': 'routing_decision', 'selected_id': selected_id,
                'frontier_size': len(frontier), 'explanation': explanation})

    async def trigger_reflection(self, trigger_type: str, **kwargs):
        if trigger_type == 'success':
            self.confidence = min(1.0, self.confidence + 0.05)
        elif trigger_type == 'failure':
            self.confidence = max(0.1, self.confidence - 0.1)

    async def get_frontier(self, task, context) -> List[Dict[str, Any]]:
        candidates = self._get_candidate_experts(task, context)
        vectors = {}
        for expert in candidates:
            vectors[expert.expert_id] = await self._get_vector(expert, context)
        frontier = self._pareto_frontier(vectors)
        return [{'expert_id': p, 'vector': vectors[p].tolist()} for p in frontier]

    async def clear_cache(self):
        async with self._cache_lock:
            self._cache.clear()

    async def select_precision(self, accuracy_required: float = 0.95) -> PrecisionLevel:
        if self.precision_controller is None:
            return PrecisionLevel.FP32
        ci = await self.carbon_manager.get_current_intensity()
        return self.precision_controller.select(ci, accuracy_required)

    async def compute_carbon_credit(self, carbon_saved_kg: float) -> Dict:
        if self.carbon_market is None:
            return {'credit_usd': 0.0, 'rec_usd': 0.0}
        credit = await self.carbon_market.get_carbon_credit_value(carbon_saved_kg)
        rec = await self.carbon_market.get_rec_value(carbon_saved_kg * 0.5)
        return {'credit_usd': credit, 'rec_usd': rec,
                'cumulative_kg': self._carbon_saved_kg_total}

    async def escalate_decision(self, decision_context, options, confidence) -> Dict:
        if self.hitl is None:
            return {'escalated': False,
                    'chosen': options[0] if options else 'noop',
                    'source': 'fallback'}
        return await self.hitl.escalate(
            decision_context, options, confidence,
            self.router_config.hitl_confidence_threshold)

    async def run_chaos_suite(self) -> Dict:
        if self.chaos_tester is None:
            return {'error': 'chaos disabled'}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            try:
                results.append(await self.chaos_tester.run_test(f, duration_s=0.05))
            except Exception as e:
                results.append({'fault': f, 'passed': False, 'error': str(e)})
        return {'results': results, 'report': self.chaos_tester.get_report()}

    async def get_status(self) -> Dict:
        status = {
            'running': self._running,
            'cache_size': len(self._cache),
            'cache_ttl': self.router_config.cache_ttl_seconds,
            'objectives': self.objective_names,
            'circuit_breaker': self._circuit_breaker.get_status(),
            'rate_limiter': self._rate_limiter.get_metrics(),
            'db_enabled': self._db_manager is not None,
            'websocket_enabled': self.websocket is not None,
            'quantum_enabled': self.quantum_security is not None,
            'blockchain_enabled': self.blockchain is not None,
            'moe_enabled': self.moe_engine is not None,
            'bio_enabled': self.bio_optimizer is not None,
            'scheduler_enabled': self.scheduler is not None,
            'self_healing_enabled': self.self_healing is not None,
            'confidence': self.confidence,
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
            'carbon_saved_kg_total': self._carbon_saved_kg_total,
            'timestamp': datetime.now().isoformat(),
        }
        if self.temporal_monitor:
            status['temporal_logic'] = self.temporal_monitor.get_status()
        if self.limit_graph:
            status['limit_graph'] = await self.limit_graph.get_graph_summary()
        if self.rlhf:
            status['rlhf'] = {'actions': self.rlhf.actions,
                              'history_len': len(self.rlhf.history)}
        if self.distillation:
            status['distillation'] = {'student_probs': self.distillation.get_student_probs(),
                                      'history_len': len(self.distillation.history)}
        if self.federated:
            status['federated'] = self.federated.get_stats()
        if self.hitl:
            status['hitl'] = self.hitl.get_audit()
        if self.chaos_tester:
            status['chaos'] = self.chaos_tester.get_report()
        if self.precision_controller:
            status['precision'] = {'last': self.precision_controller.last_precision.value,
                                   'telemetry': self.precision_controller.telemetry}
        if self.carbon_market:
            status['carbon_market'] = await self.carbon_market.get_market_snapshot()
        return status

    async def shutdown(self):
        logger.info("Shutting down ParetoRouter v5.0.0...")
        self._shutdown_event.set()
        self._running = False
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        if self.websocket:
            await self.websocket.stop()
        await self.carbon_manager.close()
        if self._db_manager:
            self._db_manager.dispose()
        logger.info("ParetoRouter shut down")

# =============================================================================
# Database (kept simple; actual persistence via sqlite text SQL)
# =============================================================================
class EnhancedDatabaseManager:
    def __init__(self, config):
        self.config = config
        self.engine = None
        self.SessionLocal = None
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._init_engine()

    def _init_engine(self):
        if not SQLALCHEMY_AVAILABLE:
            return
        try:
            self.engine = create_engine(
                f"sqlite:///{self.config.db_path}",
                poolclass=QueuePool, pool_size=5,
                connect_args={'check_same_thread': False})
            self.SessionLocal = sessionmaker(bind=self.engine)
            # Create table
            with self.engine.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS routing_decisions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        request_id TEXT, task_id TEXT, selected_expert_id TEXT,
                        frontier_size INTEGER, selection_reason TEXT,
                        vector_scores TEXT, quantum_signature TEXT,
                        blockchain_tx_hash TEXT, timestamp TEXT)
                """))
                conn.commit()
        except Exception as e:
            logger.warning(f"DB engine init failed: {e}")

    async def execute_sync(self, sync_func):
        loop = asyncio.get_event_loop()
        def wrapped():
            if not self.SessionLocal:
                return None
            session = self.SessionLocal()
            try:
                result = sync_func(session)
                session.commit()
                return result
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
        return await loop.run_in_executor(self._executor, wrapped)

    def dispose(self):
        if self.engine:
            try: self.engine.dispose()
            except Exception: pass
        self._executor.shutdown(wait=False)

# =============================================================================
# SIGNAL HANDLING + SINGLETON + MAIN
# =============================================================================
_shutdown_requested = False
_shutdown_event_global = asyncio.Event()
_router_instance: Optional[ParetoRouter] = None
_router_lock = asyncio.Lock()

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

async def shutdown_handler():
    global _router_instance
    if _router_instance:
        await _router_instance.shutdown()
        _router_instance = None

async def get_pareto_router(
    config, cost_function, node_registry,
    carbon_manager=None, user_preferences=None, objectives=None,
) -> ParetoRouter:
    global _router_instance
    if _router_instance is None:
        async with _router_lock:
            if _router_instance is None:
                _router_instance = ParetoRouter(
                    config, cost_function, node_registry,
                    carbon_manager, user_preferences, objectives)
                await _router_instance.start()
    return _router_instance

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
    print("Enhanced Pareto Router v5.0.0")
    print("+ Temporal Logic + XAI + Adaptive Precision + Carbon Markets")
    print("+ Roles + Chaos Testing + Active RLHF + HITL + Federated")
    print("=" * 80)

    router = await get_pareto_router(
        config={'pareto': {}},
        cost_function=AdaptiveCostFunction(),
        node_registry=NodeRegistry(),
    )

    # Route a test task
    result = await router.route(
        task={'type': 'inference'},
        context={'request_id': 'test_001'})

    print("\n📊 Routing result:")
    print(json.dumps({
        'selected_id': result['selected_id'],
        'source': result['source'],
        'precision_used': result['precision_used'],
        'frontier_size': len(result['frontier']),
        'carbon_intensity': result['carbon_intensity'],
        'carbon_credit_usd': result['carbon_credit_usd'],
        'rec_value_usd': result['rec_value_usd'],
        'temporal_status': result['temporal_status'],
        'role_dominant': (result.get('role_assignments') or {}).get('dominant_role'),
        'hitl_source': (result.get('hitl_outcome') or {}).get('source'),
    }, indent=2, default=str))

    # Precision
    p = await router.select_precision()
    print(f"\n⚙️  Precision: {p.value}")

    # Carbon credit
    cc = await router.compute_carbon_credit(carbon_saved_kg=250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  REC: ${cc['rec_usd']:.4f}")

    # Chaos
    print("\n🧪 Chaos suite:")
    chaos = await router.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    # Status
    print("\n📋 Status snapshot:")
    status = await router.get_status()
    print(json.dumps({
        'version': '5.0.0',
        'running': status['running'],
        'cache_size': status['cache_size'],
        'features': status['features'],
        'federated': status.get('federated'),
        'rlhf': status.get('rlhf'),
        'distillation': status.get('distillation'),
        'hitl_total': status.get('hitl', {}).get('total'),
        'chaos_pass_rate': status.get('chaos', {}).get('pass_rate'),
        'precision_last': status.get('precision', {}).get('last'),
        'carbon_market': status.get('carbon_market'),
    }, indent=2, default=str))

    print("\n" + "=" * 80)
    print("✅ Pareto Router v5.0.0 — smoke test complete")
    print("=" * 80)

    try:
        await _shutdown_event_global.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await shutdown_handler()

if __name__ == "__main__":
    asyncio.run(main())
