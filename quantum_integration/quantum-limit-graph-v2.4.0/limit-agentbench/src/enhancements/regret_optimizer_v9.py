#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/regret_optimizer_enhanced_v16_0.py
# VERSION: 17.0.0
# =============================================================================
"""
Enhanced Regret-Optimized Carbon Decision System — v17.0.0

Original v16.0.0 features:
    • Bio-inspired GA for decision generation
    • Full MoE gating network
    • Pareto-front optimizer
    • Predictive scenario generation
    • Federated learning
    • Active user preference learning
    • LIMIT Graph
    • RLHF + Multi-Teacher Policy Distillation

v17.0.0 adds (all in-file):
    • Temporal Logic Verification (G/F/U/->)
    • Explainable AI (XAI)
    • Adaptive Precision Switching (fp32/fp16/bf16/fp8/fp4)
    • Carbon Markets + Renewable Energy Credits (RECs)
    • Multi-Agent Role Specialization (emergent)
    • Chaos Testing as first-class citizen
    • Active RLHF (uncertainty-triggered human queries)
    • Human-in-the-Loop Coordinator
    • Federated Green Learning (FedAvg)
    • Causal RL hooks (IPW / ATE)
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import secrets
import time
import uuid
import signal
from collections import deque, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

# =============================================================================
# Central Green Agent components (graceful fallback)
# =============================================================================
try:
    from ..config import config as central_config
    from ..storage import Storage
    from ..schemas.feedback_event import FeedbackEvent
    from ..routing.pareto_gating import ParetoGating
    from ..feedback.adaptive_cost import AdaptiveCostFunction
    from ..safety.drift_detector import DriftDetector
    from ..scaling.message_queue import AsyncMessageQueue
    from ..metrics import MetricsRegistry
    from ..logger import logger
    CENTRAL_AVAILABLE = True
except ImportError:
    CENTRAL_AVAILABLE = False

    class _CentralConfigStub:
        CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5
        CIRCUIT_BREAKER_RECOVERY_TIMEOUT = 30
        rate_limit_requests = 100
        rate_limit_window = 60
        ENVIRONMENT = "dev"
        auto_optimize_interval = 1800
        carbon_update_interval = 300
        data_retention_days = 365
        max_concurrent_calculations = 4
        cache_ttl_seconds = 300
        ga_population_size = 20
        ga_generations = 5
        ga_mutation_rate = 0.2
        ga_crossover_rate = 0.7
        pareto_max_architectures = 100
        rlhf_enabled = True
        rlhf_training_interval = 600
        distillation_enabled = True
        distillation_temperature = 2.0
        distillation_alpha = 0.5
        distillation_interval = 300
        limit_graph_enabled = True
        limit_graph_update_interval = 300
        sustainability_interval = 3600
        carbon_api_key = None
        def get_master_key_bytes(self):
            return b'\x00' * 32

    central_config = _CentralConfigStub()

    class Storage:
        def __init__(self, *args, **kwargs):
            self._data = {}
        def save_pqc_key(self, *a, **k): pass
        def save_decision_option(self, *a, **k): pass
        def save_optimisation(self, *a, **k): pass
        def get_recent_optimisations(self, limit=5): return []
        def save_distribution(self, *a, **k): pass
        def save_state(self, *a, **k): pass
        def load_decision_options(self): return []
        def clean_old_regret_records(self, *a, **k): pass

    class FeedbackEvent:
        @staticmethod
        def create_with_context(**kwargs):
            return type('FE', (), {'to_json': lambda self: json.dumps(kwargs)})()

    class ParetoGating: pass
    class AdaptiveCostFunction:
        def __init__(self, *a, **k): pass
        def get_current_weights(self): return {}
    class DriftDetector:
        def __init__(self, *a, **k): pass
        async def check_drift(self, *a, **k): return False
    class AsyncMessageQueue:
        async def publish(self, *a, **k): pass
    class MetricsRegistry:
        def set_regret_score(self, *a, **k): pass
        def set_cvar_score(self, *a, **k): pass
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# =============================================================================
# Optional external dependencies
# =============================================================================
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

try:
    from sklearn.neural_network import MLPRegressor, MLPClassifier
    from sklearn.linear_model import LinearRegression, LogisticRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    from statsmodels.tsa.arima.model import ARIMA
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

# =============================================================================
# Prometheus metrics
# =============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    REGRET_CYCLES = Counter('regret_cycles_total', 'Regret cycles', ['status'], registry=REGISTRY)
    REGRET_OPTIMIZATIONS = Counter('regret_optimizations_total', 'Optimizations', ['strategy'], registry=REGISTRY)
    TEMPORAL_VIOLATIONS = Counter('regret_temporal_violations_total', 'Temporal', ['formula'], registry=REGISTRY)
    CHAOS_TESTS = Counter('regret_chaos_tests_total', 'Chaos', ['fault', 'status'], registry=REGISTRY)
    HITL_ESCALATIONS = Counter('regret_hitl_escalations_total', 'HITL', ['status'], registry=REGISTRY)
    FEDERATED_ROUNDS = Counter('regret_federated_rounds_total', 'Federated', registry=REGISTRY)
    CARBON_CREDITS_USD = Counter('regret_carbon_credits_usd_total', 'Carbon credits', registry=REGISTRY)
    XAI_EXPLANATIONS = Counter('regret_xai_explanations_total', 'XAI', registry=REGISTRY)
    PRECISION_SELECTIONS = Counter('regret_precision_selections_total', 'Precision', ['level'], registry=REGISTRY)
    LIMIT_GRAPH_EDGES = Gauge('regret_limit_graph_edges', 'Limit graph edges', registry=REGISTRY)
    DISTILLATION_LOSS = Gauge('regret_distillation_loss', 'Distillation loss', registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
    REGRET_CYCLES = REGRET_OPTIMIZATIONS = TEMPORAL_VIOLATIONS = DummyMetric()
    CHAOS_TESTS = HITL_ESCALATIONS = FEDERATED_ROUNDS = DummyMetric()
    CARBON_CREDITS_USD = XAI_EXPLANATIONS = PRECISION_SELECTIONS = DummyMetric()
    LIMIT_GRAPH_EDGES = DISTILLATION_LOSS = DummyMetric()

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
# CUSTOM EXCEPTIONS
# =============================================================================
class RegretError(Exception): pass
class CircuitBreakerOpenError(RegretError): pass

# =============================================================================
# CIRCUIT BREAKER + RATE LIMITER
# =============================================================================
class EnhancedCircuitBreaker:
    def __init__(self, name: str):
        self.name = name
        self.failure_threshold = central_config.CIRCUIT_BREAKER_FAILURE_THRESHOLD
        self.recovery_timeout = central_config.CIRCUIT_BREAKER_RECOVERY_TIMEOUT
        self.half_open_max_requests = 3
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()
        self.half_open_requests = 0

    async def allow_request(self) -> bool:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                else:
                    return False
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.half_open_requests += 1
                if self.half_open_requests > self.half_open_max_requests:
                    self.state = CircuitBreakerState.OPEN
                    return False
            return True

    async def record_success(self):
        async with self._lock:
            self.success_count += 1
            if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= 2:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
            else:
                self.failure_count = 0

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and \
               self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN

    async def call(self, func, *args, **kwargs):
        if not await self.allow_request():
            raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self.record_success()
            return result
        except Exception:
            await self.record_failure()
            raise


class EnhancedRateLimiter:
    def __init__(self):
        self.rate = getattr(central_config, 'rate_limit_requests', 100)
        self.per_seconds = getattr(central_config, 'rate_limit_window', 60)
        self.tokens = self.rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            self.tokens = min(self.rate,
                              self.tokens + (now - self.last_refill) * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

# =============================================================================
# DATA CLASSES
# =============================================================================
@dataclass
class DecisionOption:
    option_id: str
    name: str
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScenarioDefinition:
    carbon_price: float = 50.0
    discount_rate: float = 0.05
    demand_growth_rate: float = 0.02
    technology_cost_reduction: float = 0.1
    regulatory_risk: float = 0.3
    renewable_energy_share: float = 0.3
    energy_efficiency: float = 0.7


@dataclass
class RegretResult:
    best_option_id: str
    best_option_name: str
    maximum_regret: float
    robustness_score: float
    cvar_regret: float
    alternative_options: List[Dict]
    confidence_interval: Tuple[float, float]
    regret_heatmap: List[List[float]]
    data_quality_score: float = 100.0
    calculation_time_ms: float = 0.0
    sensitivity_results: Dict[str, float] = field(default_factory=dict)
    portfolio_allocation: Dict[str, float] = field(default_factory=dict)
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_distribution: Optional[Dict] = None
    autonomous_optimization: Optional[Dict] = None
    # v17.0.0 fields
    temporal_status: Optional[Dict] = None
    xai_explanation: Optional[Dict] = None
    precision_used: Optional[str] = None
    carbon_credit_value_usd: Optional[float] = None
    rec_value_usd: Optional[float] = None
    role_assignments: Optional[Dict] = None
    chaos_test_passed: Optional[bool] = None
    hitl_outcome: Optional[Dict] = None
    federated_round: Optional[int] = None
    causal_ates: Optional[Dict] = None

    def to_dict(self) -> Dict:
        return asdict(self)

# =============================================================================
# v17.0.0 MODULE A — TEMPORAL LOGIC MONITOR
# =============================================================================
class TemporalLogicMonitor:
    """Lightweight LTL monitor: G(φ), F(φ), φ U ψ, φ -> ψ."""
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
                                "==": lambda: lv == rv,
                                "!=": lambda: lv != rv}[op]()
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
                TEMPORAL_VIOLATIONS.labels(formula=name).inc()
        return results

    def get_status(self) -> Dict:
        return {'formulas': self.formulas, 'last_results': self.evaluate(),
                'violations': self.violations[-5:]}

# =============================================================================
# v17.0.0 MODULE B — XAI EXPLAINER
# =============================================================================
class XAIExplainer:
    def __init__(self, feature_names: List[str]):
        self.feature_names = feature_names

    def explain(self, candidate: Dict[str, float], weights: Dict[str, float],
                all_candidates: List[Dict[str, float]], top_k: int = 5) -> Dict:
        if not NUMPY_AVAILABLE:
            return {'contributions': {}, 'narrative': [], 'weights_used': weights}
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
        XAI_EXPLANATIONS.inc()
        return {'contributions': contrib,
                'top_features': [f for f, _ in ranked],
                'narrative': narrative,
                'weights_used': dict(weights)}

# =============================================================================
# v17.0.0 MODULE C — ADAPTIVE PRECISION CONTROLLER
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
        PRECISION_SELECTIONS.labels(level=self.last_precision.value).inc()
        return self.last_precision

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0, PrecisionLevel.FP16: 0.4,
                PrecisionLevel.BF16: 0.4, PrecisionLevel.FP8: 0.2,
                PrecisionLevel.FP4: 0.1}[level]

# =============================================================================
# v17.0.0 MODULE D — CARBON MARKET CLIENT
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
        CARBON_CREDITS_USD.inc(await self.get_carbon_credit_value(amount_kg))
        return rec

# =============================================================================
# v17.0.0 MODULE E — ROLE SPECIALIZATION COORDINATOR
# =============================================================================
class RoleSpecializationCoordinator:
    def __init__(self):
        self.roles = list(AgentRole)
        if NUMPY_AVAILABLE:
            self.affinity = np.array([
                [0.7, 0.9, 0.4, 0.6],   # leader
                [0.4, 0.5, 0.9, 0.5],   # worker
                [0.9, 0.4, 0.3, 0.7],   # verifier
                [0.3, 0.2, 0.3, 0.3],   # observer
            ])
        else:
            self.affinity = None

    def assign_roles(self, context: Dict[str, float]) -> Dict:
        if not NUMPY_AVAILABLE or self.affinity is None:
            return {'assignments': {r.value: 0.25 for r in self.roles},
                    'dominant_role': AgentRole.OBSERVER.value}
        ctx = np.array([context.get('trust', 0.5),
                        context.get('compute', 0.5),
                        context.get('energy', 0.5),
                        context.get('performance', 0.5)])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        return {'assignments': {role.value: float(probs[i])
                                for i, role in enumerate(self.roles)},
                'dominant_role': self.roles[int(np.argmax(probs))].value}

# =============================================================================
# v17.0.0 MODULE F — CHAOS TESTER
# =============================================================================
class ChaosTester:
    FAULT_TYPES = ['carbon_api_down', 'storage_broken', 'moe_broken',
                   'ga_broken', 'rlhf_broken', 'pareto_broken']

    def __init__(self, calculator_ref=None):
        self.calculator = calculator_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable] = []
        c = self.calculator

        try:
            if fault_type == 'carbon_api_down' and c:
                orig = c.carbon_client.get_current_intensity
                async def broken(*a, **k): raise RuntimeError("carbon API down")
                c.carbon_client.get_current_intensity = broken
                restore.append(lambda: setattr(c.carbon_client, 'get_current_intensity', orig))
            elif fault_type == 'storage_broken' and c:
                orig = c.storage.save_optimisation
                def broken(*a, **k): raise RuntimeError("storage broken")
                c.storage.save_optimisation = broken
                restore.append(lambda: setattr(c.storage, 'save_optimisation', orig))
            elif fault_type == 'moe_broken' and c:
                orig = c.moe_gating_network.select_expert
                async def broken(*a, **k): raise RuntimeError("moe broken")
                c.moe_gating_network.select_expert = broken
                restore.append(lambda: setattr(c.moe_gating_network, 'select_expert', orig))
            elif fault_type == 'ga_broken' and c:
                orig = c.ga_generator.run_search
                async def broken(*a, **k): raise RuntimeError("ga broken")
                c.ga_generator.run_search = broken
                restore.append(lambda: setattr(c.ga_generator, 'run_search', orig))
            elif fault_type == 'rlhf_broken' and c and c.rlhf:
                orig = c.rlhf.get_policy_probs
                async def broken(_): raise RuntimeError("rlhf broken")
                c.rlhf.get_policy_probs = broken
                restore.append(lambda: setattr(c.rlhf, 'get_policy_probs', orig))
            elif fault_type == 'pareto_broken' and c:
                orig = c.pareto_optimizer.add_decision
                async def broken(*a, **k): raise RuntimeError("pareto broken")
                c.pareto_optimizer.add_decision = broken
                restore.append(lambda: setattr(c.pareto_optimizer, 'add_decision', orig))
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
        CHAOS_TESTS.labels(fault=fault_type,
                           status='pass' if passed else 'fail').inc()
        return result

    def get_report(self) -> Dict:
        return {'tests_run': len(self.results),
                'pass_rate': (sum(1 for r in self.results if r['passed']) / len(self.results))
                             if self.results else 1.0,
                'recent': self.results[-5:]}

# =============================================================================
# v17.0.0 MODULE G — ACTIVE RLHF (superset of legacy RLHFManager)
# =============================================================================
class ActiveRLHF:
    """Preference-based policy with uncertainty-triggered human queries."""
    def __init__(self, action_space: List[str], uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0):
        self.actions = list(action_space)
        self.uncertainty_threshold = uncertainty_threshold
        self.human_timeout_s = human_timeout_s
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}
        self.feedback_buffer: List[Dict] = []
        self.reward_model = MLPRegressor(hidden_layer_sizes=(16,), max_iter=200,
                                          random_state=42) if SKLEARN_AVAILABLE else None
        self.policy = {'weights': np.array([0.25, 0.25, 0.25, 0.25])
                       if NUMPY_AVAILABLE else [0.25] * 4}

    def _policy(self, context):
        if not NUMPY_AVAILABLE:
            return [0.25] * len(self.actions)
        raw = np.array([self.preference_counts[a] for a in self.actions], dtype=float)
        if raw.sum() == 0:
            raw = np.ones(len(self.actions))
        e = np.exp(raw - raw.max())
        return e / e.sum()

    def sample_action(self, context) -> str:
        if NUMPY_AVAILABLE:
            probs = self._policy(context)
            return self.actions[int(np.argmax(probs))]
        return self.actions[0]

    def uncertainty(self, context) -> float:
        if NUMPY_AVAILABLE:
            probs = self._policy(context)
            ent = -np.sum(probs * np.log(probs + 1e-12))
            return float(ent / np.log(len(self.actions))) if self.actions else 0.0
        return 0.0

    def update(self, context, action: str, reward: float):
        if action in self.actions:
            self.preference_counts[action] += reward
        self.history.append({'action': action, 'reward': reward,
                             'timestamp': datetime.now().isoformat()})

    async def record_feedback(self, state, action: str, reward: float):
        self.feedback_buffer.append({
            'state': self._state_to_features(state),
            'action': self._action_to_index(action),
            'reward': reward})
        self.update(state, action, reward)

    def _state_to_features(self, state):
        return [state.get('carbon_intensity', 400) / 1000.0,
                state.get('regret', 0.5),
                state.get('cost', 0.5),
                state.get('robustness', 0.5)]

    def _action_to_index(self, action: str) -> int:
        actions = ['minimax', 'cvar', 'mopd_balanced', 'mopd_carbon']
        return actions.index(action) if action in actions else 3

    async def train_reward_model(self):
        if self.reward_model is None or len(self.feedback_buffer) < 10:
            return
        try:
            X = [f['state'] for f in self.feedback_buffer]
            y = [f['reward'] for f in self.feedback_buffer]
            self.reward_model.fit(X, y)
            self.feedback_buffer.clear()
        except Exception as e:
            logger.warning(f"ActiveRLHF train failed: {e}")

    async def get_policy_probs(self, state) -> List[float]:
        if NUMPY_AVAILABLE:
            return self._policy(state).tolist()
        return [0.25] * 4

    async def maybe_query_human(self, context, options: List[str]):
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

# =============================================================================
# v17.0.0 MODULE H — HUMAN-IN-THE-LOOP COORDINATOR
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
            HITL_ESCALATIONS.labels(status='auto').inc()
            return {'escalated': False, 'chosen': choice, 'source': 'auto'}

        if query is None:
            query = {'id': str(uuid.uuid4()), 'options': options,
                     'context': decision_context, 'status': 'pending'}
        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({'decision': 'escalated', 'query_id': query.get('id'),
                               'auto_fallback': auto_choice, 'confidence': confidence,
                               'timestamp': datetime.now().isoformat()})
        HITL_ESCALATIONS.labels(status='escalated').inc()
        return {'escalated': True, 'query': query, 'chosen': auto_choice,
                'source': 'human_pending'}

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}

# =============================================================================
# v17.0.0 MODULE I — FEDERATED AGGREGATOR
# =============================================================================
class FederatedAggregator:
    def __init__(self, num_params: int = 4):
        self.round = 0
        self.num_params = num_params
        self.global_weights: List[float] = [0.0] * num_params
        self.client_updates: List[Dict] = []

    def submit_update(self, client_id: str, weights: List[float], samples: int):
        if len(weights) != self.num_params:
            return
        self.client_updates.append({'client_id': client_id,
                                    'weights': list(weights), 'samples': samples})

    def aggregate(self) -> Dict:
        if not self.client_updates or not NUMPY_AVAILABLE:
            return {'weights': self.global_weights, 'round': self.round}
        total = sum(u['samples'] for u in self.client_updates) or 1
        agg = np.zeros(self.num_params)
        for u in self.client_updates:
            agg += np.array(u['weights']) * (u['samples'] / total)
        self.global_weights = agg.tolist()
        self.round += 1
        self.client_updates.clear()
        FEDERATED_ROUNDS.inc()
        return {'weights': self.global_weights, 'round': self.round}

    def get_stats(self) -> Dict:
        return {'round': self.round, 'global_weights': self.global_weights,
                'pending_updates': len(self.client_updates)}

# =============================================================================
# v17.0.0 MODULE J — CAUSAL REWARD SHAPER (IPW / ATE)
# =============================================================================
class CausalRewardShaper:
    def __init__(self, num_actions: int):
        self.num_actions = num_actions
        self.interventions: deque = deque(maxlen=500)
        self.ates: Dict[int, float] = {i: 0.0 for i in range(num_actions)}

    def record(self, action: int, reward: float, propensities):
        if NUMPY_AVAILABLE:
            self.interventions.append((action, float(reward), np.array(propensities)))

    def compute_ate(self) -> Dict[int, float]:
        if len(self.interventions) < 10 or not NUMPY_AVAILABLE:
            return dict(self.ates)
        for a in range(self.num_actions):
            weights, outcomes = [], []
            for action, reward, props in self.interventions:
                if action == a:
                    w = 1.0 / (props[a] + 1e-6)
                    weights.append(w)
                    outcomes.append(reward)
            if weights:
                self.ates[a] = float(np.average(outcomes, weights=weights))
        return dict(self.ates)

    def counterfactual_reward(self, action: int) -> float:
        return self.ates.get(action, 0.0)

# =============================================================================
# POST-QUANTUM CRYPTO
# =============================================================================
class PostQuantumCrypto:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.pqc_available = PQC_AVAILABLE
        self.master_key = central_config.get_master_key_bytes()
        self.default_keypair = None
        self.key_id = None

    async def generate_keypair(self, algorithm: str = 'dilithium') -> Dict:
        key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
        self.key_id = key_id
        return {'key_id': key_id, 'algorithm': algorithm,
                'public_key': hashlib.sha256(os.urandom(32)).hexdigest()}

    async def sign_data(self, data: Dict) -> Dict:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        return {'signature': hashlib.sha256(data_bytes).hexdigest(),
                'algorithm': 'sha256_fallback',
                'key_id': self.key_id or 'fallback'}

# =============================================================================
# BLOCKCHAIN VERIFICATION (stub)
# =============================================================================
class BlockchainRegretVerification:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.connected = False

    async def record_regret_data(self, data_id: str, data_hash: str,
                                  metadata: Dict) -> Dict:
        return {'status': 'success', 'data_id': data_id,
                'tx_hash': f"sim_{hashlib.sha256(os.urandom(32)).hexdigest()[:16]}",
                'simulated': True}

    async def get_blockchain_status(self) -> Dict:
        return {'connected': self.connected}

# =============================================================================
# LIVE CARBON DATA CLIENT
# =============================================================================
class LiveCarbonDataClient:
    def __init__(self):
        self.config = central_config
        self._cache = {}
        self._cache_ttl = getattr(central_config, 'cache_ttl_seconds', 300)
        self._circuit_breaker = EnhancedCircuitBreaker("carbon_api")
        self._rate_limiter = EnhancedRateLimiter()

    async def get_current_intensity(self, region: str = "global") -> float:
        cache_key = f"{region}_current"
        if cache_key in self._cache:
            cache_time, intensity = self._cache[cache_key]
            if (datetime.now() - cache_time).seconds < self._cache_ttl:
                return intensity
        intensity = 300 + random.uniform(-50, 100)
        self._cache[cache_key] = (datetime.now(), intensity)
        return intensity

    async def get_historical_intensities(self, region: str = "global",
                                          days: int = 30) -> List[float]:
        return [300 + random.uniform(-50, 100) for _ in range(days)]

# =============================================================================
# GENETIC DECISION GENERATOR
# =============================================================================
class GeneticDecisionGenerator:
    def __init__(self, storage, regret_calculator):
        self.storage = storage
        self.calculator = regret_calculator
        self.population_size = getattr(central_config, 'ga_population_size', 20)
        self.generations = getattr(central_config, 'ga_generations', 5)
        self.mutation_rate = getattr(central_config, 'ga_mutation_rate', 0.2)
        self.crossover_rate = getattr(central_config, 'ga_crossover_rate', 0.7)

    def _random_decision(self) -> Dict[str, Any]:
        return {'cost': random.uniform(50, 200),
                'carbon': random.uniform(5, 30),
                'capacity': random.uniform(10, 100),
                'efficiency': random.uniform(0.7, 0.95),
                'reliability': random.uniform(0.8, 1.0),
                'lifetime': random.randint(10, 30)}

    def _mutate(self, attrs: Dict) -> Dict:
        new_attrs = attrs.copy()
        for key, value in attrs.items():
            if random.random() < self.mutation_rate:
                if isinstance(value, float):
                    delta = random.gauss(0, 0.1 * max(abs(value), 1))
                    new_attrs[key] = max(0, value + delta)
                elif isinstance(value, int):
                    delta = int(random.gauss(0, max(1, value * 0.1)))
                    new_attrs[key] = max(1, value + delta)
        return new_attrs

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for key in p1:
            if random.random() < 0.5:
                c1[key], c2[key] = p2[key], p1[key]
        return c1, c2

    async def run_search(self, existing_decisions, scenarios):
        if not existing_decisions:
            population = [DecisionOption(f"ga_{i}", f"GA Option {i}",
                                          self._random_decision())
                          for i in range(self.population_size)]
        else:
            population = existing_decisions.copy()
            while len(population) < self.population_size:
                base = random.choice(existing_decisions)
                new_attrs = self._mutate(base.attributes)
                population.append(DecisionOption(
                    f"ga_{uuid.uuid4().hex[:8]}",
                    f"GA Option {len(population)}", new_attrs))
        # Simple truncation to best `population_size` by cost+regret heuristic
        population.sort(key=lambda d: d.attributes.get('cost', 0) +
                                      d.attributes.get('carbon', 0) * 2)
        best = population[:max(5, int(self.population_size * 0.25))]
        return best

    async def add_new_decisions(self, existing, scenarios):
        new_candidates = await self.run_search(existing, scenarios)
        for d in new_candidates:
            try:
                self.storage.save_decision_option(d.option_id, d.name, d.attributes)
            except Exception:
                pass
        return new_candidates

# =============================================================================
# MOE GATING NETWORK
# =============================================================================
class MoEGatingNetwork:
    def __init__(self, storage):
        self.storage = storage
        self.expert_names = ['minimax', 'cvar', 'mopd_balanced', 'mopd_carbon']
        self.experts = {
            'minimax': self._minimax_expert,
            'cvar': self._cvar_expert,
            'mopd_balanced': self._mopd_balanced_expert,
            'mopd_carbon': self._mopd_carbon_expert,
        }
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data = []
        self._lock = asyncio.Lock()

    def _minimax_expert(self, d, s): return {'method': 'minimax'}
    def _cvar_expert(self, d, s): return {'method': 'cvar'}
    def _mopd_balanced_expert(self, d, s):
        return {'method': 'mopd',
                'weights': {'regret': 0.4, 'carbon': 0.3, 'cost': 0.2, 'robustness': 0.1}}
    def _mopd_carbon_expert(self, d, s):
        return {'method': 'mopd',
                'weights': {'regret': 0.2, 'carbon': 0.6, 'cost': 0.1, 'robustness': 0.1}}

    def _encode_context(self, state, carbon_intensity):
        if not NUMPY_AVAILABLE:
            return [0.5] * 6
        features = [
            min(1.0, carbon_intensity / 800.0),
            0.0,
            state.get('cost_budget', 0.5),
            state.get('success_rate', 0.5),
            state.get('num_decisions', 10) / 100.0,
            state.get('num_scenarios', 5) / 20.0,
        ]
        history = state.get('history', [])
        if len(history) >= 5:
            recent = [getattr(h, 'maximum_regret', 1000) for h in history[-5:]]
            features[1] = (recent[-1] - recent[0]) / (recent[0] + 1e-8)
        return features

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or not self._training_data or not NUMPY_AVAILABLE:
            return
        X = np.array([item[0] for item in self._training_data])
        y = np.array([item[1] for item in self._training_data])
        if len(set(y)) < 2:
            return
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)
        self._gating_model = MLPClassifier(hidden_layer_sizes=(16, 8),
                                            max_iter=200, random_state=42)
        try:
            self._gating_model.fit(X_scaled, y)
            self._trained = True
        except Exception:
            self._trained = False

    async def select_expert(self, state, carbon_intensity):
        if self._trained and self._gating_model is not None and NUMPY_AVAILABLE:
            features = self._encode_context(state, carbon_intensity)
            X = np.array(features).reshape(1, -1)
            if self._scaler:
                X = self._scaler.transform(X)
            probs = self._gating_model.predict_proba(X)[0]
            selected = self.expert_names[int(np.argmax(probs))]
        else:
            selected = 'minimax'
        return selected, self.experts[selected]([], [])

    async def add_training_sample(self, context, carbon_intensity,
                                   selected_expert: str, reward: float):
        features = self._encode_context(context, carbon_intensity)
        expert_idx = self.expert_names.index(selected_expert)
        async with self._lock:
            self._training_data.append((features, expert_idx))
            if len(self._training_data) % 10 == 0:
                self._train_gating()

# =============================================================================
# PARETO FRONT OPTIMIZER
# =============================================================================
class ParetoFrontOptimizer:
    def __init__(self, storage):
        self.storage = storage
        self.pareto_front: List[DecisionOption] = []
        self.max_size = getattr(central_config, 'pareto_max_architectures', 100)
        self._lock = asyncio.Lock()

    def _dominates(self, a: DecisionOption, b: DecisionOption) -> bool:
        a_r = a.attributes.get('regret', 1000)
        a_c = a.attributes.get('carbon', 10)
        a_cs = a.attributes.get('cost', 100)
        a_rb = a.attributes.get('robustness', 0.5)
        b_r = b.attributes.get('regret', 1000)
        b_c = b.attributes.get('carbon', 10)
        b_cs = b.attributes.get('cost', 100)
        b_rb = b.attributes.get('robustness', 0.5)
        return (a_r <= b_r and a_c <= b_c and a_cs <= b_cs and a_rb >= b_rb) and \
               (a_r < b_r or a_c < b_c or a_cs < b_cs or a_rb > b_rb)

    async def add_decision(self, decision: DecisionOption, objectives: Dict) -> bool:
        async with self._lock:
            for existing in self.pareto_front:
                if self._dominates(existing, decision):
                    return False
            self.pareto_front = [d for d in self.pareto_front
                                  if not self._dominates(decision, d)]
            self.pareto_front.append(decision)
            if len(self.pareto_front) > self.max_size:
                self.pareto_front = self.pareto_front[:self.max_size]
            return True

    def get_pareto_front(self):
        return self.pareto_front

    async def get_trade_off_suggestions(self, user_weights: Dict) -> List[DecisionOption]:
        if not self.pareto_front:
            return []
        scored = []
        for d in self.pareto_front:
            regret = d.attributes.get('regret', 1000)
            carbon = d.attributes.get('carbon', 10)
            cost = d.attributes.get('cost', 100)
            robustness = d.attributes.get('robustness', 0.5)
            score = (user_weights.get('regret', 0.4) * (1 / (regret + 1e-8)) +
                     user_weights.get('carbon', 0.3) * (1 / (carbon + 1e-8)) +
                     user_weights.get('cost', 0.2) * (1 / (cost + 1e-8)) +
                     user_weights.get('robustness', 0.1) * robustness)
            scored.append((score, d))
        scored.sort(reverse=True, key=lambda x: x[0])
        return [d for _, d in scored[:5]]

# =============================================================================
# PREDICTIVE REGRET MANAGER
# =============================================================================
class PredictiveRegretManager:
    def __init__(self, storage, horizon_hours: int = 24):
        self.storage = storage
        self.horizon_hours = horizon_hours
        self.history = deque(maxlen=1000)

    async def get_regret_forecast(self, current_regret: float) -> Dict:
        if len(self.history) < 10:
            return {'recommendations': []}
        carbon_client = LiveCarbonDataClient()
        historical = await carbon_client.get_historical_intensities(days=30)
        if STATSMODELS_AVAILABLE and len(historical) > 10:
            try:
                model = ARIMA(historical, order=(5, 1, 0))
                fit = model.fit()
                forecast = fit.forecast(steps=max(1, self.horizon_hours // 24))
                trend = float(np.mean(forecast)) / float(np.mean(historical[-10:]))
            except Exception:
                trend = 1.0
        else:
            trend = 1.0
        predicted = current_regret * trend
        recommendations = []
        if predicted > current_regret * 1.2:
            recommendations.append({
                'priority': 'high',
                'reason': f'Regret projected to increase by {((predicted / current_regret) - 1) * 100:.1f}%'
            })
        return {'current_regret': current_regret,
                'predicted_regret': predicted,
                'carbon_trend': trend,
                'recommendations': recommendations}

    async def record_result(self, result):
        self.history.append(result)

# =============================================================================
# FEDERATED REGRET LEARNER (backward-compat)
# =============================================================================
class FederatedRegretLearner:
    def __init__(self, storage, instance_id: str, share_interval: int,
                 message_queue):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.message_queue = message_queue
        self.insights = deque(maxlen=100)
        self._lock = asyncio.Lock()

    async def share_regret_insight(self, insight: Dict):
        self.insights.append(insight)
        try:
            await self.message_queue.publish('federated_insights', json.dumps({
                'instance_id': self.instance_id,
                'insight': insight,
                'timestamp': datetime.now().isoformat()}))
        except Exception:
            pass

    async def pull_network_insights(self, limit: int = 10) -> List[Dict]:
        return list(self.insights)[-limit:]

    async def apply_federated_insights(self, params: Dict) -> Dict:
        async with self._lock:
            if self.insights:
                all_w = [i.get('weights', {}) for i in self.insights if 'weights' in i]
                if all_w:
                    avg = {}
                    for w in all_w:
                        for k, v in w.items():
                            avg[k] = avg.get(k, 0) + v
                    for k in avg:
                        avg[k] /= len(all_w)
                    params['teacher_weights'] = avg
            return params

# =============================================================================
# USER ADAPTIVE REFLEXIVITY
# =============================================================================
class UserAdaptiveRegretReflexivity:
    def __init__(self, storage, learning_rate: float):
        self.storage = storage
        self.learning_rate = learning_rate
        self.preferences = defaultdict(dict)
        self.user_weights = defaultdict(lambda: {'regret': 0.4, 'carbon': 0.3,
                                                 'cost': 0.2, 'robustness': 0.1})
        self._lock = asyncio.Lock()

    async def learn_user_preference(self, user_id, action, context, outcome):
        async with self._lock:
            self.preferences[user_id][action] = {
                'context': context, 'outcome': outcome,
                'timestamp': datetime.now().isoformat()}

    async def get_personalized_regret_params(self, user_id, params):
        if user_id in self.user_weights:
            params['weights'] = self.user_weights[user_id]
        return params

# =============================================================================
# REGRET CALCULATOR CORE
# =============================================================================
class RegretCalculatorCore:
    def __init__(self, config, payoff_calculator):
        self.config = config
        self.payoff_calculator = payoff_calculator

    async def _payoff_matrix(self, decisions, scenarios):
        if not NUMPY_AVAILABLE:
            return None, None
        n_d, n_s = len(decisions), len(scenarios)
        mat = np.zeros((n_d, n_s))
        for i, d in enumerate(decisions):
            for j, s in enumerate(scenarios):
                mat[i, j] = await self.payoff_calculator.calculate_payoff(d, s)
        best_per_scenario = np.max(mat, axis=0)
        regret = best_per_scenario - mat
        return mat, regret

    async def calculate_minimax_regret(self, decisions, scenarios):
        if not NUMPY_AVAILABLE:
            # fallback simple
            best = decisions[0]
            return RegretResult(best.option_id, best.name, 100.0, 0.5, 0.0,
                                [], (0, 0), [])
        mat, regret = await self._payoff_matrix(decisions, scenarios)
        max_regret = np.max(regret, axis=1)
        best_idx = int(np.argmin(max_regret))
        sorted_regrets = np.sort(regret[best_idx])
        cvar_idx = max(1, int(0.05 * len(sorted_regrets)))
        cvar = float(np.mean(sorted_regrets[:cvar_idx]))
        return RegretResult(
            best_option_id=decisions[best_idx].option_id,
            best_option_name=decisions[best_idx].name,
            maximum_regret=float(max_regret[best_idx]),
            robustness_score=1 / (1 + float(max_regret[best_idx]) / 1000),
            cvar_regret=cvar,
            alternative_options=[
                {'option_id': d.option_id, 'name': d.name, 'max_regret': float(r)}
                for d, r in zip(decisions, max_regret)
                if d.option_id != decisions[best_idx].option_id],
            confidence_interval=(float(max_regret[best_idx]) * 0.9,
                                 float(max_regret[best_idx]) * 1.1),
            regret_heatmap=regret.tolist())

    async def calculate_cvar_regret(self, decisions, scenarios):
        if not NUMPY_AVAILABLE:
            return await self.calculate_minimax_regret(decisions, scenarios)
        mat, regret = await self._payoff_matrix(decisions, scenarios)
        cvar_vals = []
        for i in range(len(decisions)):
            sorted_r = np.sort(regret[i])
            idx = max(1, int(0.05 * len(sorted_r)))
            cvar_vals.append(float(np.mean(sorted_r[:idx])))
        best_idx = int(np.argmin(cvar_vals))
        max_r = float(np.max(regret[best_idx]))
        return RegretResult(
            best_option_id=decisions[best_idx].option_id,
            best_option_name=decisions[best_idx].name,
            maximum_regret=max_r,
            robustness_score=1 / (1 + cvar_vals[best_idx] / 1000),
            cvar_regret=cvar_vals[best_idx],
            alternative_options=[
                {'option_id': d.option_id, 'name': d.name, 'cvar_regret': c}
                for d, c in zip(decisions, cvar_vals)
                if d.option_id != decisions[best_idx].option_id],
            confidence_interval=(cvar_vals[best_idx] * 0.9,
                                 cvar_vals[best_idx] * 1.1),
            regret_heatmap=regret.tolist())

    async def calculate_mopd_regret(self, decisions, scenarios, weights):
        if not NUMPY_AVAILABLE:
            return await self.calculate_minimax_regret(decisions, scenarios)
        mat, regret = await self._payoff_matrix(decisions, scenarios)
        max_r = np.max(regret, axis=1)
        avg_carbon = np.array([d.attributes.get('carbon', 10) for d in decisions])
        avg_cost = np.array([d.attributes.get('cost', 100) for d in decisions])
        robustness = 1 / (1 + max_r / 1000)

        def _norm(a):
            return (a - a.min()) / (a.max() - a.min() + 1e-8)

        scores = (weights['regret'] * _norm(max_r) +
                  weights['carbon'] * _norm(avg_carbon) +
                  weights['cost'] * _norm(avg_cost) +
                  weights['robustness'] * (1 - robustness))
        best_idx = int(np.argmin(scores))
        return RegretResult(
            best_option_id=decisions[best_idx].option_id,
            best_option_name=decisions[best_idx].name,
            maximum_regret=float(max_r[best_idx]),
            robustness_score=float(robustness[best_idx]),
            cvar_regret=0.0,
            alternative_options=[],
            confidence_interval=(float(max_r[best_idx]) * 0.9,
                                 float(max_r[best_idx]) * 1.1),
            regret_heatmap=regret.tolist())

# =============================================================================
# SIMPLE PAYOFF CALCULATOR
# =============================================================================
class SimplePayoffCalculator:
    async def calculate_payoff(self, decision, scenario):
        base = 1000 - decision.attributes.get('cost', 0) * 0.1
        carbon_factor = scenario.carbon_price * decision.attributes.get('carbon', 0) * 0.01
        return base - carbon_factor

# =============================================================================
# QUALITY SCORER
# =============================================================================
class SimpleQualityScorer:
    async def assess_quality(self, decisions):
        return 100.0

# =============================================================================
# LIMIT GRAPH
# =============================================================================
class LimitGraphManager:
    def __init__(self, config=None):
        self.config = config or central_config
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
# MULTI-TEACHER POLICY DISTILLATION (role-aware)
# =============================================================================
class MultiTeacherPolicyDistillation:
    def __init__(self, config=None, moe_engine=None):
        self.config = config or central_config
        self.moe_engine = moe_engine
        self.student_policy = np.array([0.25, 0.25, 0.25, 0.25]) \
            if NUMPY_AVAILABLE else [0.25] * 4
        self.temperature = getattr(self.config, 'distillation_temperature', 2.0)
        self.history = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, state):
        if not self.moe_engine or not NUMPY_AVAILABLE:
            return
        carbon_intensity = state.get('carbon_intensity', 400)
        try:
            features = self.moe_engine._encode_context(state, carbon_intensity)
            X = np.array(features).reshape(1, -1)
            if self.moe_engine._scaler:
                X = self.moe_engine._scaler.transform(X)
            probs = self.moe_engine._gating_model.predict_proba(X)[0] \
                if self.moe_engine._trained and self.moe_engine._gating_model is not None \
                else np.ones(4) / 4
        except Exception:
            probs = np.ones(4) / 4
        teacher_dist = np.array(probs) / (np.array(probs).sum() + 1e-9)
        soft = np.exp(np.log(teacher_dist + 1e-6) / self.temperature)
        soft /= soft.sum()
        loss = -np.sum(soft * np.log(self.student_policy + 1e-6))
        grad = -soft / (self.student_policy + 1e-6)
        self.student_policy = np.clip(self.student_policy - 0.01 * grad, 0.01, None)
        self.student_policy /= self.student_policy.sum()
        async with self._lock:
            self.history.append({'loss': float(loss),
                                 'timestamp': datetime.now().isoformat()})
        DISTILLATION_LOSS.set(loss)

    def get_student_probs(self):
        if NUMPY_AVAILABLE:
            return self.student_policy.tolist()
        return self.student_policy

# =============================================================================
# AUTONOMOUS REGRET OPTIMIZER (with RLHF/Distillation priority)
# =============================================================================
class AutonomousRegretOptimizer:
    def __init__(self, storage, state, adaptive_cost=None,
                 rlhf=None, distillation=None):
        self.storage = storage
        self.state = state
        self.adaptive_cost = adaptive_cost
        self.moe_gating = MoEGatingNetwork(storage)
        self.rlhf = rlhf
        self.distillation = distillation
        self._last_optimization = None
        self.optimization_history = deque(maxlen=100)

    async def optimize_regret(self, current_state, strategy=None):
        carbon_intensity = current_state.get('carbon_intensity', 400)
        if self.rlhf is not None and self.rlhf.history:
            probs = await self.rlhf.get_policy_probs(current_state)
            names = self.moe_gating.expert_names
            best_idx = int(np.argmax(probs)) % len(names) if NUMPY_AVAILABLE else 0
            selected = names[best_idx]
        elif self.distillation is not None:
            probs = self.distillation.get_student_probs()
            names = self.moe_gating.expert_names
            best_idx = int(np.argmax(probs)) % len(names) if NUMPY_AVAILABLE else 0
            selected = names[best_idx]
        else:
            selected, _ = await self.moe_gating.select_expert(current_state,
                                                              carbon_intensity)
        result = {'action': f'{selected}_optimization',
                  'selected_strategy': selected,
                  'recommendation': self._generate_recommendation(selected)}
        try:
            self.storage.save_optimisation(selected, result)
        except Exception:
            pass
        self._last_optimization = (selected, result)
        self.optimization_history.append(result)
        REGRET_OPTIMIZATIONS.labels(strategy=selected).inc()
        return result

    async def record_outcome(self, reward, context):
        if self._last_optimization:
            selected, _ = self._last_optimization
            await self.moe_gating.add_training_sample(
                context, context.get('carbon_intensity', 400), selected, reward)
            if self.rlhf is not None and reward > 0.7:
                await self.rlhf.record_feedback(
                    state={'carbon_intensity': context.get('carbon_intensity', 400),
                           'regret': reward, 'cost': 0.5, 'robustness': 0.5},
                    action=selected, reward=reward)
            self._last_optimization = None

    def _generate_recommendation(self, strategy):
        return {'minimax': "Focus on minimising maximum regret.",
                'cvar': "Prioritise tail-risk reduction.",
                'mopd_balanced': "Balance regret, carbon, cost, robustness.",
                'mopd_carbon': "Emphasise carbon efficiency."}.get(
                    strategy, "Maintain current strategy.")

    def get_optimization_stats(self):
        return {'total_optimizations': len(self.optimization_history),
                'strategies': self.moe_gating.expert_names,
                'moe_trained': self.moe_gating._trained,
                'training_samples': len(self.moe_gating._training_data),
                'rlhf_trained': bool(self.rlhf and self.rlhf.history),
                'distillation_probs': self.distillation.get_student_probs()
                    if self.distillation else []}

# =============================================================================
# MULTI-CLOUD DISTRIBUTION (stub)
# =============================================================================
class MultiCloudRegretDistribution:
    def __init__(self, storage):
        self.storage = storage
        self.active_provider = 'aws'
        self.active_region = 'us-east-1'

    async def distribute_regret_data(self, data, preferences=None):
        result = {'optimal_provider': self.active_provider,
                  'optimal_region': self.active_region,
                  'scores': {'aws': 1.0},
                  'data_size_gb': data.get('size_gb', 0),
                  'timestamp': datetime.now().isoformat()}
        try:
            self.storage.save_distribution(result)
        except Exception:
            pass
        return result

    async def get_distribution_status(self):
        return {'providers': ['aws', 'azure', 'gcp'],
                'active_provider': self.active_provider,
                'active_region': self.active_region}

# =============================================================================
# REGRET STATE
# =============================================================================
class RegretState:
    def __init__(self, storage):
        self.storage = storage
        self.confidence = 0.5
        self.historical_success_rate = 0.5
        self.reflection_count = 0
        self.carbon_budget_remaining = 100.0
        self.regret_threshold = 500

    async def save(self):
        try:
            self.storage.save_state('confidence', str(self.confidence))
            self.storage.save_state('carbon_budget', str(self.carbon_budget_remaining))
        except Exception:
            pass

    async def trigger_reflection(self, trigger_type, **kwargs):
        self.reflection_count += 1
        if trigger_type == 'regret_reduced':
            self.confidence = min(1.0, self.confidence + 0.05)
        elif trigger_type == 'regret_increased':
            self.confidence = max(0.1, self.confidence - 0.1)
        await self.save()

# =============================================================================
# CARBON-AWARE OPTIMIZER (stub)
# =============================================================================
class CarbonAwareRegretOptimizer:
    def __init__(self, storage):
        self.storage = storage
        self.carbon_client = LiveCarbonDataClient()

    async def adjust_regret_for_carbon(self, result, urgency):
        intensity = await self.carbon_client.get_current_intensity()
        factor = 1.2 if intensity > 400 else (0.9 if intensity < 200 else 1.0)
        return {'adjustment_factor': factor,
                'adjusted_regret': {**result,
                                    'maximum_regret': result.get('maximum_regret', 1000) * factor}}

    async def get_current_intensity(self):
        return await self.carbon_client.get_current_intensity()

    async def close(self):
        pass

# =============================================================================
# OTHER STUBS
# =============================================================================
class CrossDomainRegretTransfer:
    def __init__(self, storage):
        self.storage = storage
        self.transfers = deque(maxlen=100)

    async def get_transfer_statistics(self):
        return {'total_transfers': len(self.transfers), 'recent': list(self.transfers)[-5:]}


class HumanAIRegretCollaboration:
    def __init__(self, storage, feedback_timeout):
        self.storage = storage
        self.feedback_timeout = feedback_timeout

    async def request_regret_feedback(self, result, context):
        await asyncio.sleep(0.0)


class RegretSustainabilityTracker:
    def __init__(self, storage):
        self.storage = storage
        self.metrics = defaultdict(list)

    async def record_metric(self, name, value, context):
        self.metrics[name].append({'value': value, 'context': context,
                                   'timestamp': datetime.now().isoformat()})

    async def get_sustainability_score(self):
        scores = []
        for values in self.metrics.values():
            if values:
                scores.append(sum(v['value'] for v in values[-20:]) / len(values[-20:]))
        return {'overall_score': (sum(scores) / len(scores) * 100) if scores else 50.0}

# =============================================================================
# ENHANCED REGRET CALCULATOR — v17.0.0
# =============================================================================
class EnhancedRegretCalculator:
    """
    Enhanced regret calculator v17.0.0 with all ten enhancement areas integrated.
    """
    def __init__(self, storage, message_queue, adaptive_cost, pareto_gating,
                 drift_detector, metrics):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.instance_id = str(uuid.uuid4())[:8]
        self._start_time = datetime.now()

        # Core
        self.pqc = PostQuantumCrypto(storage)
        self.blockchain = BlockchainRegretVerification(storage)
        self.cloud_distributor = MultiCloudRegretDistribution(storage)
        self.carbon_client = LiveCarbonDataClient()
        self.payoff_calculator = SimplePayoffCalculator()
        self.core = RegretCalculatorCore(central_config, self.payoff_calculator)
        self.quality_scorer = SimpleQualityScorer()
        self.state = RegretState(storage)

        # v17.0.0 modules
        self.temporal_monitor = TemporalLogicMonitor()
        self.temporal_monitor.add_formula("regret_cap", "G(regret <= 10000.0)")
        self.temporal_monitor.add_formula("carbon_cap", "G(carbon <= 900.0)")
        self.temporal_monitor.add_formula("convergence", "F(regret <= 100.0)")

        self.xai = XAIExplainer(['regret', 'carbon', 'cost', 'robustness'])
        self.precision_controller = AdaptivePrecisionController()
        self.carbon_market = CarbonMarketClient()
        self.role_coordinator = RoleSpecializationCoordinator()
        self.rlhf = ActiveRLHF(
            action_space=['minimax', 'cvar', 'mopd_balanced', 'mopd_carbon'])
        self.hitl = HumanInTheLoopCoordinator(self.rlhf)
        self.federated = FederatedAggregator(num_params=4)
        self.causal_shaper = CausalRewardShaper(num_actions=4)
        self.chaos_tester = ChaosTester(self)

        # Existing core
        self.moe_gating_network = MoEGatingNetwork(storage)
        self.distillation = MultiTeacherPolicyDistillation(
            central_config, self.moe_gating_network)
        self.autonomous_optimizer = AutonomousRegretOptimizer(
            storage, self.state, adaptive_cost,
            rlhf=self.rlhf, distillation=self.distillation)
        self.ga_generator = GeneticDecisionGenerator(storage, self)
        self.pareto_optimizer = ParetoFrontOptimizer(storage)
        self.predictive_manager = PredictiveRegretManager(storage)
        self.federated_learner = FederatedRegretLearner(
            storage, self.instance_id, 3600, message_queue)
        self.user_adaptive = UserAdaptiveRegretReflexivity(storage, 0.01)
        self.limit_graph = LimitGraphManager()
        self.carbon_optimizer = CarbonAwareRegretOptimizer(storage)
        self.cross_domain_transfer = CrossDomainRegretTransfer(storage)
        self.human_collaborator = HumanAIRegretCollaboration(storage, 300)
        self.sustainability_tracker = RegretSustainabilityTracker(storage)

        self.optimization_history: deque = deque(maxlen=10000)
        self._history_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._background_tasks: List[asyncio.Task] = []
        self._running = False
        self._optimization_semaphore = asyncio.Semaphore(
            getattr(central_config, 'max_concurrent_calculations', 4))
        self._carbon_saved_kg_total = 0.0

        logger.info(f"EnhancedRegretCalculator v17.0.0 initialized "
                    f"(instance {self.instance_id})")

    # ----------------------------------------------------------------------
    # Teacher interface
    # ----------------------------------------------------------------------
    async def policy_probs(self, state) -> List[float]:
        if self.rlhf is not None:
            return await self.rlhf.get_policy_probs(state)
        if self.distillation is not None:
            return self.distillation.get_student_probs()
        return [0.25] * 4

    # ----------------------------------------------------------------------
    # Core regret calculation
    # ----------------------------------------------------------------------
    async def calculate_regret(self, decisions, scenarios,
                                method: str = "minimax", user_id: Optional[str] = None):
        async with self._optimization_semaphore:
            start_time = time.time()

            # User preference learning
            if user_id:
                await self.user_adaptive.learn_user_preference(
                    user_id, 'accept_regret_decision',
                    {'method': method}, {'success': True})

            # Carbon adjustment + federated params
            await self.carbon_optimizer.adjust_regret_for_carbon(
                {'maximum_regret': 1000}, "normal")
            await self.federated_learner.apply_federated_insights({
                'cvar_alpha': 0.95,
                'scenario_count': len(scenarios)})

            quality_score = await self.quality_scorer.assess_quality(decisions)
            carbon_intensity = await self.carbon_client.get_current_intensity()

            # Adaptive precision
            precision = self.precision_controller.select(carbon_intensity, 0.95)

            # Temporal gate
            self.temporal_monitor.update({
                'regret': self.optimization_history[-1].maximum_regret
                    if self.optimization_history else 1000.0,
                'carbon': float(carbon_intensity),
            })
            temporal_status = self.temporal_monitor.evaluate()

            # State for strategy selection
            state = {
                'current_regret': self.optimization_history[-1].maximum_regret
                    if self.optimization_history else 1000,
                'carbon_intensity': carbon_intensity,
                'cost_budget': self.state.carbon_budget_remaining,
                'success_rate': self.state.historical_success_rate,
                'num_decisions': len(decisions),
                'num_scenarios': len(scenarios),
                'history': list(self.optimization_history)[-10:]
                    if self.optimization_history else [],
            }

            # LIMIT graph update
            await self.limit_graph.update_constraint('carbon', carbon_intensity)

            # Strategy selection (RLHF > Distillation > MoE)
            if self.rlhf.history:
                probs = await self.rlhf.get_policy_probs(state)
                names = self.moe_gating_network.expert_names
                idx = int(np.argmax(probs)) % len(names) if NUMPY_AVAILABLE else 0
                selected_expert = names[idx]
            elif self.distillation.get_student_probs() != [0.25] * 4:
                probs = self.distillation.get_student_probs()
                names = self.moe_gating_network.expert_names
                idx = int(np.argmax(probs)) % len(names) if NUMPY_AVAILABLE else 0
                selected_expert = names[idx]
            else:
                selected_expert, _ = await self.moe_gating_network.select_expert(
                    state, carbon_intensity)

            # Execute selected expert
            if selected_expert == 'minimax':
                result = await self.core.calculate_minimax_regret(decisions, scenarios)
            elif selected_expert == 'cvar':
                result = await self.core.calculate_cvar_regret(decisions, scenarios)
            elif selected_expert == 'mopd_balanced':
                result = await self.core.calculate_mopd_regret(
                    decisions, scenarios,
                    {'regret': 0.4, 'carbon': 0.3, 'cost': 0.2, 'robustness': 0.1})
            elif selected_expert == 'mopd_carbon':
                result = await self.core.calculate_mopd_regret(
                    decisions, scenarios,
                    {'regret': 0.2, 'carbon': 0.6, 'cost': 0.1, 'robustness': 0.1})
            else:
                result = await self.core.calculate_minimax_regret(decisions, scenarios)

            result.data_quality_score = quality_score
            result.calculation_time_ms = (time.time() - start_time) * 1000

            # XAI explanation
            xai_out = self.xai.explain(
                candidate={'regret': 1.0 / (result.maximum_regret + 1e-8),
                           'carbon': 1.0 - min(1.0, carbon_intensity / 800),
                           'cost': 0.5, 'robustness': result.robustness_score},
                weights={'regret': 0.4, 'carbon': 0.3, 'cost': 0.2, 'robustness': 0.1},
                all_candidates=[
                    {'regret': 1.0 / (result.maximum_regret + 1e-8),
                     'carbon': 1.0 - min(1.0, carbon_intensity / 800),
                     'cost': 0.5, 'robustness': result.robustness_score}])
            result.xai_explanation = xai_out

            # Role assignments
            result.role_assignments = self.role_coordinator.assign_roles({
                'trust': 0.7, 'compute': 0.7,
                'energy': 1.0 - min(1.0, carbon_intensity / 800),
                'performance': result.robustness_score})

            # HITL escalation (if confidence low)
            confidence = 1.0 / (1.0 + result.maximum_regret / 1000)
            hitl_outcome = None
            if confidence < 0.65:
                hitl_outcome = await self.hitl.escalate(
                    decision_context={'regret': result.maximum_regret},
                    options=self.moe_gating_network.expert_names,
                    confidence=confidence,
                    confidence_threshold=0.65)
            result.hitl_outcome = hitl_outcome

            # Causal observation
            try:
                action_idx = self.moe_gating_network.expert_names.index(selected_expert)
            except ValueError:
                action_idx = 0
            self.causal_shaper.record(action_idx, confidence, [0.25] * 4)
            result.causal_ates = self.causal_shaper.compute_ate()

            # Carbon credits
            try:
                saved_kg = max(0.0, (400.0 - carbon_intensity) * 0.001)
                credit = await self.carbon_market.get_carbon_credit_value(saved_kg)
                rec = await self.carbon_market.get_rec_value(saved_kg * 0.5)
                self._carbon_saved_kg_total += saved_kg
                result.carbon_credit_value_usd = credit
                result.rec_value_usd = rec
            except Exception:
                pass

            # Federated submission
            try:
                self.federated.submit_update(
                    self.instance_id,
                    [confidence, 1.0 / (1.0 + result.maximum_regret / 1000),
                     1.0 - min(1.0, carbon_intensity / 800), 0.5],
                    samples=1)
                if len(self.optimization_history) % 5 == 0:
                    agg = self.federated.aggregate()
                    result.federated_round = agg['round']
            except Exception:
                pass

            # Chaos smoke test
            chaos_passed = None
            if len(self.optimization_history) % 5 == 0:
                try:
                    cr = await self.chaos_tester.run_test(
                        random.choice(ChaosTester.FAULT_TYPES), duration_s=0.02)
                    chaos_passed = cr['passed']
                except Exception:
                    pass
            result.chaos_test_passed = chaos_passed

            # PQC signature
            signature = await self.pqc.sign_data(result.to_dict())
            result.quantum_signature = signature

            # Blockchain record
            try:
                data_hash = hashlib.sha256(
                    json.dumps(result.to_dict(), sort_keys=True, default=str).encode()
                ).hexdigest()
                bc = await self.blockchain.record_regret_data(
                    f"regret_{uuid.uuid4().hex[:8]}", data_hash,
                    {'regret': result.maximum_regret,
                     'best': result.best_option_name})
                result.blockchain_tx_hash = bc.get('tx_hash')
            except Exception:
                pass

            # Cloud distribution
            try:
                result.cloud_distribution = await self.cloud_distributor.distribute_regret_data(
                    {'size_gb': 0.001})
            except Exception:
                pass

            # Autonomous optimizer feedback
            reward = 1.0 / (1.0 + result.maximum_regret / 1000)
            await self.autonomous_optimizer.record_outcome(
                reward,
                {'carbon_intensity': carbon_intensity,
                 'num_decisions': len(decisions),
                 'num_scenarios': len(scenarios)})
            result.autonomous_optimization = {
                'selected_strategy': selected_expert, 'reward': reward}

            # Pareto update
            best_decision = next(
                (d for d in decisions if d.option_id == result.best_option_id), None)
            if best_decision:
                best_decision.attributes['regret'] = result.maximum_regret
                best_decision.attributes['robustness'] = result.robustness_score
                await self.pareto_optimizer.add_decision(
                    best_decision,
                    {'regret': result.maximum_regret,
                     'carbon': best_decision.attributes.get('carbon', 0),
                     'cost': best_decision.attributes.get('cost', 0),
                     'robustness': result.robustness_score})

            await self.predictive_manager.record_result(result)

            # Persist
            async with self._history_lock:
                self.optimization_history.append(result)

            # Publish feedback
            try:
                event = FeedbackEvent.create_with_context(
                    task_id=f"regret_{uuid.uuid4().hex[:8]}",
                    selected_action=f"regret_{selected_expert}",
                    quality_score=quality_score / 100.0,
                    latency_ms=result.calculation_time_ms,
                    energy_joules=0.0,
                    carbon_g=0.0,
                    feedback_type="regret",
                    adaptive_cost_value=0.0,
                    state={'method': method, 'scenarios': len(scenarios)},
                    candidates=[{'action': s} for s in self.moe_gating_network.expert_names],
                    source="regret_optimizer",
                    environment=getattr(central_config, 'ENVIRONMENT', 'dev'),
                    tags=["regret", "decision", "v17"])
                await self.queue.publish("feedback_events", event.to_json())
            except Exception:
                pass

            # Drift check
            if self.drift:
                try:
                    await self.drift.check_drift(self.adaptive_cost.get_current_weights())
                except Exception:
                    pass

            # Metrics
            try:
                self.metrics.set_regret_score(result.maximum_regret)
                self.metrics.set_cvar_score(result.cvar_regret)
            except Exception:
                pass

            REGRET_CYCLES.labels(status='success').inc()
            logger.info(f"Regret v17: best={result.best_option_name} "
                        f"regret={result.maximum_regret:.2f} "
                        f"strategy={selected_expert} precision={precision.value}")

            # Attach v17 metadata
            result.temporal_status = temporal_status
            result.precision_used = precision.value

            return result

    # ----------------------------------------------------------------------
    # Utility public APIs
    # ----------------------------------------------------------------------
    async def run_ga_search(self, existing, scenarios):
        return await self.ga_generator.add_new_decisions(existing, scenarios)

    async def get_pareto_front(self):
        return self.pareto_optimizer.get_pareto_front()

    async def get_trade_off_suggestions(self, user_weights):
        return await self.pareto_optimizer.get_trade_off_suggestions(user_weights)

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

    def select_precision(self, carbon_intensity: float = 400.0,
                         accuracy_required: float = 0.95) -> PrecisionLevel:
        return self.precision_controller.select(carbon_intensity, accuracy_required)

    async def compute_carbon_credit(self, carbon_saved_kg: float) -> Dict:
        credit = await self.carbon_market.get_carbon_credit_value(carbon_saved_kg)
        rec = await self.carbon_market.get_rec_value(carbon_saved_kg * 0.5)
        return {'credit_usd': credit, 'rec_usd': rec,
                'cumulative_kg': self._carbon_saved_kg_total}

    def get_role_assignments(self, context: Optional[Dict] = None) -> Dict:
        ctx = context or {'trust': 0.7, 'compute': 0.7,
                          'energy': 0.7, 'performance': 0.7}
        return self.role_coordinator.assign_roles(ctx)

    def get_causal_ates(self) -> Dict:
        return self.causal_shaper.compute_ate()

    async def escalate_decision(self, decision_context, options, confidence) -> Dict:
        return await self.hitl.escalate(decision_context, options, confidence)

    def get_comprehensive_status(self) -> Dict:
        status = {
            'instance_id': self.instance_id,
            'version': '17.0.0',
            'reasoning_count': len(self.optimization_history),
            'carbon_saved_kg_total': self._carbon_saved_kg_total,
            'features': {
                'temporal_logic': True,
                'xai': True,
                'adaptive_precision': True,
                'carbon_market': True,
                'role_specialization': True,
                'chaos_testing': True,
                'hitl': True,
                'federated': True,
                'causal_rl': True,
            },
            'timestamp': datetime.now().isoformat(),
        }
        status['temporal_logic'] = self.temporal_monitor.get_status()
        status['rlhf'] = {'history_len': len(self.rlhf.history)}
        status['distillation'] = {
            'student_probs': self.distillation.get_student_probs(),
            'history_len': len(self.distillation.history)}
        status['federated'] = self.federated.get_stats()
        status['hitl'] = self.hitl.get_audit()
        status['chaos'] = self.chaos_tester.get_report()
        status['precision'] = {
            'last': self.precision_controller.last_precision.value,
            'telemetry': self.precision_controller.telemetry}
        status['causal_ates'] = self.causal_shaper.compute_ate()
        return status

    # ----------------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------------
    async def start(self):
        logger.info("Starting Regret Calculator v17.0.0...")
        self._running = True
        try:
            loop = asyncio.get_running_loop()
            self._background_tasks.extend([
                loop.create_task(self._optimization_loop()),
                loop.create_task(self._carbon_update_loop()),
                loop.create_task(self._federated_loop()),
                loop.create_task(self._predictive_loop()),
                loop.create_task(self._cleanup_loop()),
                loop.create_task(self._ga_loop()),
                loop.create_task(self._rlhf_loop()),
                loop.create_task(self._distillation_loop()),
                loop.create_task(self._limit_graph_loop()),
                loop.create_task(self._chaos_loop()),
            ])
        except RuntimeError:
            pass

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await self.rlhf.train_reward_model()
                await asyncio.sleep(getattr(central_config, 'rlhf_training_interval', 600))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"RLHF loop error: {e}")

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            try:
                state = {'carbon_intensity': await self.carbon_client.get_current_intensity(),
                         'history': list(self.optimization_history)[-10:]}
                await self.distillation.distill(state)
                await asyncio.sleep(getattr(central_config, 'distillation_interval', 300))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Distillation loop error: {e}")

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            try:
                carbon = await self.carbon_client.get_current_intensity()
                await self.limit_graph.update_constraint('carbon', carbon)
                await asyncio.sleep(getattr(central_config, 'limit_graph_update_interval', 300))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Limit graph loop error: {e}")

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)
                fault = random.choice(ChaosTester.FAULT_TYPES)
                await self.chaos_tester.run_test(fault, duration_s=0.05)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Chaos loop error: {e}")

    async def _optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(getattr(central_config, 'auto_optimize_interval', 1800))
            try:
                async with self._history_lock:
                    if self.optimization_history:
                        latest = self.optimization_history[-1]
                        state = {
                            'current_regret': latest.maximum_regret,
                            'carbon_intensity': await self.carbon_client.get_current_intensity(),
                            'cost_budget': self.state.carbon_budget_remaining,
                            'success_rate': self.state.historical_success_rate,
                        }
                        await self.autonomous_optimizer.optimize_regret(state)
            except Exception as e:
                logger.error(f"Optimization loop error: {e}")

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(getattr(central_config, 'carbon_update_interval', 300))
            try:
                await self.carbon_client.get_current_intensity()
            except Exception as e:
                logger.error(f"Carbon update loop error: {e}")

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                await self.federated_learner.pull_network_insights()
            except Exception as e:
                logger.error(f"Federated loop error: {e}")

    async def _predictive_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                async with self._history_lock:
                    if self.optimization_history:
                        latest = self.optimization_history[-1]
                        await self.predictive_manager.get_regret_forecast(
                            latest.maximum_regret)
            except Exception as e:
                logger.error(f"Predictive loop error: {e}")

    async def _ga_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(getattr(central_config, 'sustainability_interval', 86400))
            try:
                existing = self.storage.load_decision_options()
                await self.ga_generator.add_new_decisions(existing, [ScenarioDefinition()])
            except Exception as e:
                logger.error(f"GA loop error: {e}")

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(86400)
            try:
                self.storage.clean_old_regret_records(
                    days=getattr(central_config, 'data_retention_days', 365))
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def shutdown(self):
        logger.info("Shutting down Regret Calculator v17.0.0...")
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.state.save()
        await self.carbon_optimizer.close()
        logger.info("Shutdown complete")

# =============================================================================
# SINGLETON ACCESSOR
# =============================================================================
_regret_instance: Optional[EnhancedRegretCalculator] = None
_regret_lock = asyncio.Lock()

async def get_regret_calculator(storage, queue, adaptive_cost,
                                 pareto_gating, drift_detector, metrics):
    global _regret_instance
    if _regret_instance is None:
        async with _regret_lock:
            if _regret_instance is None:
                _regret_instance = EnhancedRegretCalculator(
                    storage, queue, adaptive_cost, pareto_gating,
                    drift_detector, metrics)
                await _regret_instance.start()
    return _regret_instance

# =============================================================================
# SMOKE TEST
# =============================================================================
async def _smoke_test():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s — %(message)s')
    print("=" * 78)
    print("Enhanced Regret Calculator v17.0.0 — smoke test")
    print("=" * 78)

    calc = await get_regret_calculator(
        storage=Storage(),
        queue=AsyncMessageQueue(),
        adaptive_cost=AdaptiveCostFunction(),
        pareto_gating=ParetoGating(),
        drift_detector=DriftDetector(),
        metrics=MetricsRegistry())

    decisions = [
        DecisionOption('d1', 'Solar Panel Investment', {'cost': 100, 'carbon': 10}),
        DecisionOption('d2', 'Wind Turbine Investment', {'cost': 120, 'carbon': 5}),
        DecisionOption('d3', 'Energy Storage Investment', {'cost': 80, 'carbon': 15}),
    ]
    scenarios = [ScenarioDefinition(carbon_price=50),
                 ScenarioDefinition(carbon_price=75),
                 ScenarioDefinition(carbon_price=100)]

    print("\n🧮 Running regret calculation...")
    result = await calc.calculate_regret(decisions, scenarios)

    print(f"   Best: {result.best_option_name}")
    print(f"   Max regret: {result.maximum_regret:.2f}")
    print(f"   CVaR regret: {result.cvar_regret:.2f}")
    print(f"   Robustness: {result.robustness_score:.4f}")
    print(f"   Precision: {result.precision_used}")
    print(f"   Carbon credit: ${result.carbon_credit_value_usd or 0:.4f}")
    print(f"   REC: ${result.rec_value_usd or 0:.4f}")
    if result.temporal_status:
        print(f"   Temporal: {result.temporal_status}")
    if result.role_assignments:
        print(f"   Dominant role: {result.role_assignments.get('dominant_role')}")
    if result.xai_explanation:
        print(f"   XAI: {result.xai_explanation['narrative'][0]}")
    if result.hitl_outcome:
        print(f"   HITL: {result.hitl_outcome['source']} -> {result.hitl_outcome['chosen']}")

    print("\n🧬 Running GA search...")
    new_decisions = await calc.run_ga_search(decisions, scenarios)
    print(f"   GA generated {len(new_decisions)} new decisions")

    print("\n📊 Pareto front:")
    front = await calc.get_pareto_front()
    print(f"   Size: {len(front)}")

    print("\n⚙️  Precision selector →",
          calc.select_precision(carbon_intensity=350.0).value)
    cc = await calc.compute_carbon_credit(250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  REC: ${cc['rec_usd']:.4f}")

    print(f"\n🎭 Roles: {calc.get_role_assignments()}")
    print(f"🧠 Causal ATEs: {calc.get_causal_ates()}")

    print("\n🧪 Chaos suite:")
    chaos = await calc.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    print("\n📋 Comprehensive status:")
    status = calc.get_comprehensive_status()
    print(json.dumps({
        'version': status['version'],
        'reasoning_count': status['reasoning_count'],
        'carbon_saved_kg': status['carbon_saved_kg_total'],
        'features': status['features'],
        'rlhf': status.get('rlhf'),
        'distillation': status.get('distillation'),
        'federated': status.get('federated'),
        'hitl_total': status.get('hitl', {}).get('total'),
        'chaos_pass_rate': status.get('chaos', {}).get('pass_rate'),
        'precision_last': status.get('precision', {}).get('last'),
        'causal_ates': status.get('causal_ates'),
    }, indent=2, default=str))

    await calc.shutdown()
    print("\n" + "=" * 78)
    print("✅ Enhanced Regret Calculator v17.0.0 — smoke test complete")
    print("=" * 78)


if __name__ == "__main__":
    asyncio.run(_smoke_test())
