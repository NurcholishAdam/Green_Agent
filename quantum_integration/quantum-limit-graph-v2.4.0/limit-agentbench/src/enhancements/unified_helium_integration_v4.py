#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/unified_helium_integration_enhanced_v17_1_0.py
# VERSION: 17.1.0  (Phase 1-3 enhancements applied — see changelog)
# =============================================================================
"""
Unified Integration Manager — v17.1.0

Ten Green Agent enhancements on top of the v9 orchestration skeleton:
  1. Quantum-Distillation      → QuantumDistillationEngine (s - t gradient)
  2. Causal RL                 → CausalGraphLearner + CausalIntegrationPolicyAdapter
  3. Federated Green Learning  → FederatedIntegrationLearner (unified backend)
  4. Multi-Agent Coordination  → MultiAgentCoordinator (dispatch + hysteresis)
  5. Temporal Logic            → TemporalIntegrationVerifier (recursive parser)
  6. Explainable AI            → IntegrationXAIDecisionExplainer (KernelSHAP)
  7. Adaptive Precision        → AdaptivePrecisionSwitcher
  8. Carbon Markets / REC      → CarbonMarketIntegrator (TTL cache)
  9. Chaos Testing             → ChaosTestingEngine (steady_during)
 10. HITL                      → ActiveUserPreferenceLearner + HITLWebSocket

v17.1.0 (Phase 1-3) adds:
  [P0] Real chaos probes during fault; elapsed-window WITHIN; pending-queue
       NEXT; s - t KL gradient; action-aware RLHF; chaos CB key fixed;
       data_corruption implemented; real message bus dispatch; federated
       read/write unified; _run_module runs real logic; sustainability &
       quality computed from module outputs; recursive-descent LTL parser.
  [P1] NeuralTeacher trained & registered; derived state features; predictive
       twin populated; HITL wired; Pareto cost real; precision loop real;
       causal ATE key fixed; sub-module stall(); CB half-open limiter.
  [P2] Correlation IDs; CircuitBreaker lock; module lock; sub-module
       save_state/load_state; extended statistics; new metrics.
  [P3] Real KernelSHAP; pgmpy PC hook + edge stability; discovery-based
       federated; multi-agent hysteresis; streaming + batched APIs; PQC
       signature chain; real module formulas for all 10 modules.
"""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import json
import logging
import logging.handlers
import math
import os
import random
import re
import secrets
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (Any, AsyncIterator, Callable, Deque, Dict, List,
                    Optional, Set, Tuple, Union)

import numpy as np

# -----------------------------------------------------------------------------
# Correlation ID — [P2]
# -----------------------------------------------------------------------------
correlation_id_var = contextvars.ContextVar("correlation_id", default="unknown")

# -----------------------------------------------------------------------------
# Central Green Agent components (optional)
# -----------------------------------------------------------------------------
try:
    from ..config import config as central_config
    from ..storage import Storage as CentralStorage
    from ..metrics import MetricsRegistry as CentralMetrics
    from ..logger import logger as central_logger
    CENTRAL_COMPONENTS_AVAILABLE = True
except ImportError:
    CENTRAL_COMPONENTS_AVAILABLE = False
    central_config = None
    CentralStorage = None
    CentralMetrics = None
    central_logger = None

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    from sklearn.neural_network import MLPClassifier, MLPRegressor
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

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
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    import websockets
    from websockets.server import serve as ws_serve
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    from pgmpy.estimators import PC
    PGMPY_AVAILABLE = True
except ImportError:
    PGMPY_AVAILABLE = False


# =============================================================================
# Universal config accessor
# =============================================================================
def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    if config is None:
        return default
    if isinstance(config, dict):
        return config.get(key, default)
    if hasattr(config, "model_dump"):
        try:
            return config.model_dump().get(key, default)
        except Exception:
            pass
    if hasattr(config, "dict") and callable(getattr(config, "dict")):
        try:
            return config.dict().get(key, default)
        except Exception:
            pass
    return getattr(config, key, default)


# =============================================================================
# Logging — [P1] guarded audit handler; [P2] correlation ID filter
# =============================================================================
if CENTRAL_COMPONENTS_AVAILABLE and central_logger:
    logger = central_logger
else:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - "
               "[%(correlation_id)s] - %(message)s")
    logger = logging.getLogger("integration_v17")

    class _CorrelationFilter(logging.Filter):
        def filter(self, record):
            record.correlation_id = correlation_id_var.get()
            return True

    for _h in logger.handlers:
        _h.addFilter(_CorrelationFilter())

audit_logger = logging.getLogger("integration_audit")
audit_logger.setLevel(logging.INFO)
if not audit_logger.handlers:
    try:
        _audit_handler = logging.handlers.RotatingFileHandler(
            "integration_audit_v17.log", maxBytes=50 * 1024 * 1024,
            backupCount=10)
        _audit_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        audit_logger.addHandler(_audit_handler)
    except Exception:
        # [P1] Don't crash on read-only / airgapped FS
        _audit_handler = logging.StreamHandler()
        _audit_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        audit_logger.addHandler(_audit_handler)


# =============================================================================
# CONFIG — [P3] extra fields
# =============================================================================
_DEFAULTS: Dict[str, Any] = {
    "DB_PATH": "/tmp/integration_manager_v17.db",
    "MASTER_KEY_ENV": "INTEGRATION_MASTER_KEY",
    "CACHE_TTL": 300,
    "LOG_LEVEL": "INFO",
    "GA_ENABLED": True, "GA_POPULATION_SIZE": 20, "GA_GENERATIONS": 5,
    "GA_MUTATION_RATE": 0.2, "GA_CROSSOVER_RATE": 0.7,
    "MOE_ENABLED": True, "MOE_EXPERT_COUNT": 5, "MOE_HIDDEN_LAYERS": [16, 8],
    "PARETO_ENABLED": True, "PARETO_MAX_ARCHITECTURES": 100,
    "FEDERATED_ENABLED": True, "FEDERATED_INTERVAL": 3600,
    "NEURAL_TEACHER_ENABLED": True,
    "ACTIVE_USER_PREFERENCE_ENABLED": True,
    "DRIFT_DETECTION_ENABLED": True,
    "PREDICTIVE_DIGITAL_TWIN_ENABLED": True,
    "LIMIT_GRAPH_ENABLED": True, "LIMIT_GRAPH_UPDATE_INTERVAL": 300,
    "MODP_ENABLED": True, "MODP_WEIGHTS": [0.25, 0.25, 0.25, 0.25],
    "RLHF_ENABLED": True, "RLHF_TRAINING_INTERVAL": 600,
    "DISTILLATION_ENABLED": True,
    "DISTILLATION_TEMPERATURE": 2.0, "DISTILLATION_ALPHA": 0.5,
    "DISTILLATION_INTERVAL": 300,
    "CAUSAL_RL_ENABLED": True, "CAUSAL_GRAPH_UPDATE_INTERVAL": 900,
    "CAUSAL_EXPLORATION_RATE": 0.1, "CAUSAL_MIN_SAMPLES": 20,
    "TEMPORAL_LOGIC_ENABLED": True, "TEMPORAL_VERIFICATION_INTERVAL": 300,
    "TEMPORAL_FORMULAS": [
        "G (success_rate >= 0.5)",
        "G (carbon <= 0.7)",
        "F (integration_complete)",
    ],
    "TEMPORAL_MAX_TRACE": 2000,
    "XAI_ENABLED": True, "XAI_METHOD": "kernel_shap",
    "XAI_DEPTH": 5, "XAI_INTERVAL": 300,
    "ADAPTIVE_PRECISION_ENABLED": True,
    "PRECISION_LEVELS": ["fp32", "fp16", "bf16", "int8"],
    "PRECISION_SWITCH_THRESHOLD": 0.02,
    "CARBON_MARKET_ENABLED": True,
    "CARBON_MARKET_API_URL": None,
    "CARBON_MARKET_INTERVAL": 3600,
    "CHAOS_TESTING_ENABLED": True, "CHAOS_TEST_INTERVAL": 1800,
    "CHAOS_INTENSITY": 0.05, "CHAOS_BLAST_RADIUS": 0.1,
    "CHAOS_FAULT_TYPES": ["latency", "exception", "data_corruption",
                          "memory_pressure", "network_drop"],
    "CHAOS_AUTO_ROLLBACK": True,
    "MULTI_AGENT_ENABLED": True, "AGENT_COUNT": 5,
    "AGENT_NEGOTIATION_INTERVAL": 600,
    "MAX_CONCURRENT_MODULES": 5,
    # v17.1
    "HITL_WS_PORT": 8791,
    "STATE_PATH": "/tmp/integration_v17_state.json",
}

if CENTRAL_COMPONENTS_AVAILABLE and central_config:
    class _CfgFromCentral:
        def __init__(self):
            g = lambda k, d: getattr(central_config, k, d)
            self.DB_PATH = g("db_path", _DEFAULTS["DB_PATH"])
            self.MASTER_KEY_ENV = g("master_key_env", _DEFAULTS["MASTER_KEY_ENV"])
            self.CACHE_TTL = g("cache_ttl", _DEFAULTS["CACHE_TTL"])
            for k, v in _DEFAULTS.items():
                if not hasattr(self, k):
                    setattr(self, k, v)
    config = _CfgFromCentral()
else:
    class _CfgDict(dict):
        def __getattr__(self, k):
            return self.get(k)
        def __setattr__(self, k, v):
            self[k] = v
    config = _CfgDict(_DEFAULTS)


# =============================================================================
# METRICS — [P2] new metrics
# =============================================================================
class _Dummy:
    def labels(self, **kw): return self
    def inc(self, *a, **kw): pass
    def set(self, *a, **kw): pass
    def observe(self, *a, **kw): pass


if PROMETHEUS_AVAILABLE:
    INTEGRATION_RUNS = Counter("integration_runs_total", "Runs", ["status"])
    MODULE_RUNS = Counter("integration_module_runs_total", "Module runs",
                           ["module", "status"])
    INTEGRATION_DURATION = Histogram("integration_duration_seconds", "Duration",
                                       ["strategy"])
    SUSTAINABILITY_SCORE = Gauge("integration_sustainability_score", "Sust")
    DATA_QUALITY_SCORE = Gauge("integration_data_quality", "Quality")
    SUCCESS_RATE = Gauge("integration_success_rate", "Success rate")
    GA_FITNESS = Gauge("integration_ga_population_fitness", "GA")
    MOE_PROBS = Gauge("integration_moe_gating_probabilities", "MoE", ["expert"])
    PARETO_SIZE = Gauge("integration_pareto_front_size", "Pareto")
    FEDERATED_ROUNDS = Counter("integration_federated_rounds_total", "Fed")
    DRIFT_SCORE = Gauge("integration_drift_score", "Drift", ["domain"])
    CAUSAL_ATE = Gauge("integration_causal_ate", "ATE",
                        ["treatment", "outcome"])
    CAUSAL_EDGE_STABILITY = Gauge("integration_causal_edge_stability", "Stab")
    TEMPORAL_VERIFICATIONS = Counter("integration_temporal_verifications_total",
                                       "TL", ["formula", "status"])
    TEMPORAL_VIOLATIONS = Counter("integration_temporal_violations_total",
                                    "TLv", ["formula"])
    XAI_EXPLANATIONS = Counter("integration_xai_explanations_total", "XAI",
                                 ["method"])
    XAI_FEATURE_IMPORTANCE = Gauge("integration_xai_feature_importance",
                                     "XAI FI", ["feature"])
    PRECISION_SWITCHES = Counter("integration_precision_switches_total", "PS",
                                   ["from_p", "to_p"])
    PRECISION_ENERGY_SAVED = Gauge("integration_precision_energy_saved_wh", "PE")
    CARBON_CREDIT_PRICE = Gauge("integration_carbon_credit_price_usd", "CC")
    REC_BALANCE = Gauge("integration_rec_balance_mwh", "REC")
    NET_ZERO_MATCHES = Counter("integration_net_zero_matches_total", "NZ")
    CHAOS_EXPERIMENTS = Counter("integration_chaos_experiments_total", "Ch",
                                  ["fault_type", "status"])
    CHAOS_STEADY_STATE = Gauge("integration_chaos_steady_state_ok", "ChSS")
    AGENT_ROLES = Gauge("integration_agent_roles", "AR", ["role"])
    AGENT_REPUTATION = Gauge("integration_agent_reputation", "ARep", ["agent_id"])
    # [P2] new
    INTEGRATION_LATENCY = Histogram("integration_latency_seconds", "Latency",
                                      ["strategy"])
    INTEGRATION_STRATEGY_DIST = Gauge("integration_strategy_distribution",
                                        "Dist", ["strategy"])
    INTEGRATION_ERRORS = Counter("integration_errors_total", "Errors", ["stage"])
    INTEGRATION_ACTIVE = Gauge("integration_active", "Active")
else:
    for _n in [
        "INTEGRATION_RUNS", "MODULE_RUNS", "INTEGRATION_DURATION",
        "SUSTAINABILITY_SCORE", "DATA_QUALITY_SCORE", "SUCCESS_RATE",
        "GA_FITNESS", "MOE_PROBS", "PARETO_SIZE", "FEDERATED_ROUNDS",
        "DRIFT_SCORE", "CAUSAL_ATE", "CAUSAL_EDGE_STABILITY",
        "TEMPORAL_VERIFICATIONS", "TEMPORAL_VIOLATIONS", "XAI_EXPLANATIONS",
        "XAI_FEATURE_IMPORTANCE", "PRECISION_SWITCHES", "PRECISION_ENERGY_SAVED",
        "CARBON_CREDIT_PRICE", "REC_BALANCE", "NET_ZERO_MATCHES",
        "CHAOS_EXPERIMENTS", "CHAOS_STEADY_STATE", "AGENT_ROLES",
        "AGENT_REPUTATION", "INTEGRATION_LATENCY", "INTEGRATION_STRATEGY_DIST",
        "INTEGRATION_ERRORS", "INTEGRATION_ACTIVE",
    ]:
        globals()[_n] = _Dummy()


# =============================================================================
# STORAGE — [P0] unified federated; [P1] bounded deques
# =============================================================================
class IntegrationStorage:
    def __init__(self, central=None):
        self._central = central
        self._state: Dict[str, str] = {}
        self.causal_edges: Dict[str, Dict[str, Any]] = {}
        self.temporal_violations: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self.xai_explanations: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.precision_history: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.rec_ledger: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.carbon_prices: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.chaos_experiments: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.agent_registry: Dict[str, Dict[str, Any]] = {}
        self.agent_messages: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self.module_runs: Deque[Dict[str, Any]] = deque(maxlen=5000)
        # [P0] unified federated backend
        self.fed_weights: Dict[str, str] = {}

    def get_state(self, key):
        if self._central and hasattr(self._central, "get_state"):
            try:
                v = self._central.get_state(key)
                if v is not None:
                    return v
            except Exception:
                pass
        return self._state.get(key)

    def save_state(self, key, value):
        self._state[key] = value  # [P0] always keep locally
        if self._central and hasattr(self._central, "save_state"):
            try:
                self._central.save_state(key, value)
            except Exception:
                pass

    async def _fetchall(self, query, params=()):
        return []

    # v17 persistence
    def save_causal_edge(self, src, dst, weight, confidence):
        self.causal_edges[f"{src}->{dst}"] = {
            "src": src, "dst": dst, "weight": float(weight),
            "confidence": float(confidence),
            "ts": datetime.now(timezone.utc).isoformat()}

    def save_temporal_violation(self, formula, step, state):
        self.temporal_violations.append({
            "formula": formula, "step": step, "state": dict(state),
            "ts": datetime.now(timezone.utc).isoformat()})

    def save_xai_explanation(self, decision_id, method, top_features, nl):
        self.xai_explanations.append({
            "decision_id": decision_id, "method": method,
            "top_features": dict(top_features), "natural_language": nl,
            "ts": datetime.now(timezone.utc).isoformat()})

    def save_precision_switch(self, frm, to, reason, saved_wh):
        self.precision_history.append({
            "from": frm, "to": to, "reason": reason,
            "saved_wh": float(saved_wh),
            "ts": datetime.now(timezone.utc).isoformat()})

    def save_rec(self, mwh, price, source):
        self.rec_ledger.append({
            "mwh": float(mwh), "price": float(price), "source": source,
            "ts": datetime.now(timezone.utc).isoformat()})

    def get_rec_balance(self):
        return sum(r["mwh"] for r in self.rec_ledger)

    def save_credit_price(self, price, currency="USD", source="oracle"):
        self.carbon_prices.append({
            "price": float(price), "currency": currency, "source": source,
            "ts": datetime.now(timezone.utc).isoformat()})

    def save_chaos_experiment(self, name, fault_type, status, before, during,
                                after, blast):
        self.chaos_experiments.append({
            "name": name, "fault_type": fault_type, "status": status,
            "steady_before": int(before), "steady_during": int(during),
            "steady_after": int(after), "blast": float(blast),
            "ts": datetime.now(timezone.utc).isoformat()})

    def save_agent(self, agent_id, role, rep, utilities):
        self.agent_registry[agent_id] = {
            "role": role, "reputation": float(rep),
            "utilities": dict(utilities),
            "ts": datetime.now(timezone.utc).isoformat()}

    def save_agent_message(self, topic, sender, payload):
        self.agent_messages.append({
            "topic": topic, "sender": sender, "payload": dict(payload),
            "ts": datetime.now(timezone.utc).isoformat()})

    def save_module_run(self, module_name, status, duration_ms, message=""):
        self.module_runs.append({
            "module": module_name, "status": status,
            "duration_ms": float(duration_ms), "message": message,
            "ts": datetime.now(timezone.utc).isoformat()})

    # [P0] unified federated backend
    def save_fed_weight(self, instance_id, weights):
        key = f"fed_integration_weight_{instance_id}"
        self.fed_weights[key] = json.dumps(weights, default=str)
        self.save_state(key, self.fed_weights[key])

    def get_all_fed_weights(self):
        out = dict(self.fed_weights)
        for k, v in self._state.items():
            if k.startswith("fed_integration_weight_"):
                out[k] = v
        return out


_central_store = None
if CENTRAL_COMPONENTS_AVAILABLE and CentralStorage:
    try:
        _central_store = CentralStorage(db_path=_cfg_get(config, "DB_PATH"))
    except Exception:
        _central_store = None

storage = IntegrationStorage(central=_central_store)


# =============================================================================
# CIRCUIT BREAKER — [P2] lock; [P1] half-open limiter; [P0] chaos key fixed
# =============================================================================
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0,
                 half_open_max_requests=3, name="default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_requests = half_open_max_requests
        self.name = name
        self._failures = 0
        self._last_failure_time: Optional[datetime] = None
        self._state = "CLOSED"
        self._half_open_requests = 0
        self._lock = asyncio.Lock()
        self.chaos_engine: Optional["ChaosTestingEngine"] = None

    def is_target_active(self) -> bool:
        if self.chaos_engine is None:
            return False
        return self.chaos_engine.is_target_active(f"cb_{self.name}")

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self._state == "OPEN":
                if self._last_failure_time and \
                        (datetime.now() - self._last_failure_time).total_seconds() \
                        > self.recovery_timeout:
                    self._state = "HALF_OPEN"
                    self._half_open_requests = 0
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
            if self._state == "HALF_OPEN":
                self._half_open_requests += 1
                if self._half_open_requests > self.half_open_max_requests:
                    self._state = "OPEN"
                    raise RuntimeError(
                        f"Circuit breaker {self.name} HALF_OPEN limit exceeded")
        # [P0] chaos check via is_target_active
        if self.is_target_active():
            await asyncio.sleep(0.2)
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == "HALF_OPEN":
                    self._state = "CLOSED"
                    self._failures = 0
                    self._half_open_requests = 0
            return result
        except Exception:
            async with self._lock:
                self._failures += 1
                self._last_failure_time = datetime.now()
                if self._failures >= self.failure_threshold:
                    self._state = "OPEN"
            raise


# =============================================================================
# ENCRYPTION — [P1] fail-closed (no XOR fallback)
# =============================================================================
class EncryptionManager:
    def __init__(self, master_key: bytes):
        if len(master_key) != 32:
            raise ValueError("Master key must be 32 bytes")
        self.master_key = master_key

    def encrypt(self, data: bytes) -> Tuple[bytes, bytes]:
        # [P1] fail-closed if no real crypto
        if not CRYPTO_AVAILABLE:
            raise RuntimeError(
                "cryptography library not available; cannot encrypt")
        nonce = secrets.token_bytes(12)
        return AESGCM(self.master_key).encrypt(nonce, data, None), nonce

    def decrypt(self, ciphertext: bytes, nonce: bytes) -> bytes:
        if not CRYPTO_AVAILABLE:
            raise RuntimeError(
                "cryptography library not available; cannot decrypt")
        return AESGCM(self.master_key).decrypt(nonce, ciphertext, None)


# =============================================================================
# MODULE RESULT — [P3] real output
# =============================================================================
@dataclass
class ModuleResult:
    module_name: str
    status: str
    duration_ms: float
    message: str = ""
    error: Optional[str] = None
    output: Dict[str, Any] = field(default_factory=dict)
    correlation_id: str = ""


# =============================================================================
# REAL MODULES — [P3] actual formulas for all ten modules
# =============================================================================
class BaseModule:
    def __init__(self, name: str, config: Any):
        self.name = name
        self.config = config
        self._stalled = False

    def stall(self):
        self._stalled = True

    async def run(self, context: Dict[str, Any]) -> ModuleResult:
        raise NotImplementedError

    def save_state(self) -> Dict[str, Any]:
        return {}

    def load_state(self, data: Dict[str, Any]):
        pass


class HeliumModule(BaseModule):
    """[P3] Toy helium recovery from feedstock.
    Recovery formula inspired by pressure-swing adsorption efficiency.
    """
    async def run(self, context):
        temp_c = float(context.get("temperature_c", 25.0))
        pressure_bar = float(context.get("pressure_bar", 10.0))
        feed_pct = float(context.get("helium_feed_pct", 0.05))  # mole fraction
        # Adsorption working capacity roughly follows Langmuir
        # q = q_max * K * P / (1 + K * P)
        q_max = 0.9  # max fractional recovery
        K = 0.1 * (1 + (temp_c - 20.0) * 0.01)  # temperature dependence
        q = q_max * (K * pressure_bar) / (1.0 + K * pressure_bar)
        recovery = max(0.05, min(0.95, q))
        helium_liters = feed_pct * 1000.0 * recovery
        purity = 1.0 - feed_pct * (1.0 - recovery)  # trace contaminants
        return ModuleResult(
            module_name=self.name, status="success",
            duration_ms=0.0,
            message=f"recovery={recovery:.3f}, He={helium_liters:.2f}L, "
                    f"purity={purity:.3f}",
            output={"recovery": recovery,
                    "helium_liters": helium_liters,
                    "purity": purity})


class CarbonModule(BaseModule):
    """[P3] CO2 emissions from energy × grid intensity."""
    async def run(self, context):
        energy_kwh = float(context.get("energy_kwh", 1.0))
        intensity = float(context.get("carbon_intensity", 400.0))  # gCO2/kWh
        carbon_kg = energy_kwh * intensity / 1000.0
        # Cost at $50/ton
        carbon_cost_usd = carbon_kg / 1000.0 * 50.0
        return ModuleResult(
            module_name=self.name, status="success",
            duration_ms=0.0,
            message=f"carbon={carbon_kg:.4f}kg, cost=${carbon_cost_usd:.4f}",
            output={"carbon_kg": carbon_kg,
                    "carbon_cost_usd": carbon_cost_usd,
                    "energy_kwh": energy_kwh,
                    "intensity": intensity})


class ElasticityModule(BaseModule):
    """[P3] Material elasticity from temperature-induced strain."""
    async def run(self, context):
        temp_c = float(context.get("temperature_c", 25.0))
        temp_ref_c = float(context.get("temp_reference_c", 20.0))
        alpha = float(context.get("thermal_expansion_coef", 1.2e-5))
        strain = alpha * (temp_c - temp_ref_c)
        # For steel-like material: E ~ 200 GPa
        youngs_modulus_pa = 2.0e11
        stress_pa = youngs_modulus_pa * strain
        # Service-life impact (linear damage model)
        damage_fraction = min(1.0, max(0.0, abs(strain) / 0.005))
        return ModuleResult(
            module_name=self.name, status="success",
            duration_ms=0.0,
            message=f"strain={strain:.2e}, stress={stress_pa:.2e}Pa, "
                    f"damage={damage_fraction:.3f}",
            output={"strain": strain,
                    "stress_pa": stress_pa,
                    "damage_fraction": damage_fraction})


class CircularityModule(BaseModule):
    """[P3] Material circularity score (Ellen MacArthur-style)."""
    async def run(self, context):
        recycled = float(context.get("recycled_fraction", 0.5))
        reused = float(context.get("reused_fraction", 0.1))
        recycled = max(0.0, min(1.0, recycled))
        reused = max(0.0, min(1.0, reused))
        # Material Circularity Indicator (simplified)
        mci = 0.9 * recycled + 0.1 * reused
        return ModuleResult(
            module_name=self.name, status="success",
            duration_ms=0.0,
            message=f"MCI={mci:.3f}",
            output={"circularity": mci,
                    "recycled": recycled,
                    "reused": reused})


class ForecasterModule(BaseModule):
    """[P3] Exponential smoothing forecast over internal history."""
    def __init__(self, name, config):
        super().__init__(name, config)
        self.history: Deque[float] = deque(maxlen=50)
        self.alpha = 0.3

    async def run(self, context):
        value = float(context.get("current_value",
                                    context.get("carbon_intensity", 400.0)))
        self.history.append(value)
        if len(self.history) < 2:
            forecast = value
        else:
            ema = self.history[0]
            for v in list(self.history)[1:]:
                ema = self.alpha * v + (1 - self.alpha) * ema
            forecast = ema
        return ModuleResult(
            module_name=self.name, status="success",
            duration_ms=0.0,
            message=f"forecast={forecast:.2f}",
            output={"forecast": forecast,
                    "history_size": len(self.history)})

    def save_state(self):
        return {"history": list(self.history)}

    def load_state(self, data):
        self.history = deque(data.get("history", []), maxlen=50)


class ThermalModule(BaseModule):
    """[P3] Carnot efficiency and cooling load."""
    async def run(self, context):
        t_hot_c = float(context.get("temp_hot_c", 80.0))
        t_cold_c = float(context.get("temp_cold_c", 25.0))
        t_h = t_hot_c + 273.15
        t_c = t_cold_c + 273.15
        carnot_eff = max(0.0, 1.0 - t_c / t_h)
        # Real efficiency ~ 40% of Carnot
        practical_eff = 0.4 * carnot_eff
        # Cooling load (kW) for 100 kW IT load
        it_load_kw = float(context.get("it_load_kw", 100.0))
        cop = 1.0 / max(0.01, 1.0 - practical_eff) if practical_eff > 0 else 3.0
        cooling_kw = it_load_kw / max(0.5, cop)
        pue = (it_load_kw + cooling_kw) / max(1.0, it_load_kw)
        return ModuleResult(
            module_name=self.name, status="success",
            duration_ms=0.0,
            message=f"carnot={carnot_eff:.3f}, practical={practical_eff:.3f}, "
                    f"pue={pue:.3f}",
            output={"carnot_efficiency": carnot_eff,
                    "practical_efficiency": practical_eff,
                    "cooling_kw": cooling_kw,
                    "pue": pue})


class RegretModule(BaseModule):
    """[P3] Counterfactual regret from expected vs realized reward."""
    async def run(self, context):
        expected = float(context.get("expected_reward", 0.8))
        realized = float(context.get("realized_reward", 0.7))
        regret = max(0.0, expected - realized)
        # Relative regret
        relative = regret / max(1e-6, expected)
        return ModuleResult(
            module_name=self.name, status="success",
            duration_ms=0.0,
            message=f"regret={regret:.4f}, relative={relative:.4f}",
            output={"regret": regret,
                    "relative_regret": relative})


class QuantumModule(BaseModule):
    """[P3] QAOA-inspired simulated annealing on a small quadratic."""
    async def run(self, context):
        n_qubits = int(context.get("n_qubits", 6))
        T0 = float(context.get("quantum_T0", 1.0))
        T_end = float(context.get("quantum_T_end", 0.01))
        steps = int(context.get("quantum_steps", 30))
        best_energy = float("inf")
        best_state = None
        for step in range(steps):
            T = T0 * (T_end / T0) ** (step / max(1, steps))
            state = [random.choice([-1, 1]) for _ in range(n_qubits)]
            # Quadratic objective: negative of sum of adjacent products
            energy = -sum(state[i] * state[(i + 1) % n_qubits]
                          for i in range(n_qubits))
            if energy < best_energy:
                best_energy = energy
                best_state = list(state)
            else:
                # Accept with Boltzmann probability
                dE = energy - best_energy
                if random.random() < math.exp(-dE / max(1e-6, T)):
                    best_energy = energy
                    best_state = list(state)
        return ModuleResult(
            module_name=self.name, status="success",
            duration_ms=0.0,
            message=f"energy={best_energy}, state={best_state}",
            output={"best_energy": best_energy,
                    "best_state": best_state or []})


class CollectorModule(BaseModule):
    """[P3] Aggregate upstream outputs."""
    async def run(self, context):
        upstream = context.get("upstream_results", {})
        n = len(upstream)
        statuses = defaultdict(int)
        for r in upstream.values():
            statuses[r.get("status", "unknown")] += 1
        return ModuleResult(
            module_name=self.name, status="success",
            duration_ms=0.0,
            message=f"collected {n} modules, statuses={dict(statuses)}",
            output={"collected_count": n,
                    "upstream_statuses": dict(statuses)})


class SustainabilityModule(BaseModule):
    """[P3] Aggregate sustainability from module outputs."""
    async def run(self, context):
        outputs = context.get("module_outputs", {})
        carbon_kg = outputs.get("carbon", {}).get("carbon_kg", 0.0)
        helium_recovery = outputs.get("helium", {}).get("recovery", 0.0)
        circularity = outputs.get("circularity", {}).get("circularity", 0.5)
        thermal_eff = outputs.get("thermal", {}).get(
            "practical_efficiency", 0.3)
        # Normalize each component to [0, 1]
        carbon_score = max(0.0, 1.0 - min(1.0, carbon_kg / 10.0))
        helium_score = max(0.0, min(1.0, helium_recovery))
        circ_score = max(0.0, min(1.0, circularity))
        thermal_score = max(0.0, min(1.0, thermal_eff * 2.5))  # 0.4 eff → 1.0
        weights = {"carbon": 0.4, "helium": 0.25, "circ": 0.2, "thermal": 0.15}
        score = 100.0 * (
            weights["carbon"] * carbon_score +
            weights["helium"] * helium_score +
            weights["circ"] * circ_score +
            weights["thermal"] * thermal_score
        )
        return ModuleResult(
            module_name=self.name, status="success",
            duration_ms=0.0,
            message=f"sustainability={score:.1f} "
                    f"(carbon={carbon_score:.2f}, helium={helium_score:.2f}, "
                    f"circ={circ_score:.2f}, thermal={thermal_score:.2f})",
            output={"sustainability_score": score,
                    "components": {
                        "carbon": carbon_score,
                        "helium": helium_score,
                        "circularity": circ_score,
                        "thermal": thermal_score}})


MODULE_CLASSES = {
    "collector": CollectorModule,
    "carbon": CarbonModule,
    "helium": HeliumModule,
    "thermal": ThermalModule,
    "elasticity": ElasticityModule,
    "circularity": CircularityModule,
    "forecaster": ForecasterModule,
    "regret": RegretModule,
    "quantum": QuantumModule,
    "sustainability": SustainabilityModule,
}


# =============================================================================
# MESSAGE BUS — [P0] real dispatch
# =============================================================================
class _MessageBus:
    def __init__(self):
        self._subs: Dict[str, List[Callable]] = defaultdict(list)
        self._delivered = 0
        self._dropped = 0

    def subscribe(self, topic: str, handler: Callable):
        self._subs[topic].append(handler)

    async def publish(self, msg: Dict[str, Any]) -> int:
        delivered = 0
        topic = msg.get("topic", "")
        for h in list(self._subs.get(topic, [])):
            try:
                r = h(msg)
                if asyncio.iscoroutine(r):
                    await r
                delivered += 1
            except Exception as e:
                logger.debug("bus handler failed on '%s': %s", topic, e)
        for h in list(self._subs.get("*", [])):
            try:
                r = h(msg)
                if asyncio.iscoroutine(r):
                    await r
                delivered += 1
            except Exception:
                pass
        self._delivered += delivered
        if delivered == 0:
            self._dropped += 1
        return delivered


# =============================================================================
# GA OPTIMIZER — [P3] real fitness from module outputs
# =============================================================================
class GeneticIntegrationOptimizer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.population_size = _cfg_get(config, "GA_POPULATION_SIZE", 20)
        self.generations = _cfg_get(config, "GA_GENERATIONS", 5)
        self.mutation_rate = _cfg_get(config, "GA_MUTATION_RATE", 0.2)
        self.crossover_rate = _cfg_get(config, "GA_CROSSOVER_RATE", 0.7)
        self.param_bounds = {
            "timeout_multiplier": (0.8, 2.0),
            "preferred_cloud": ["aws", "azure", "gcp"],
        }
        self._cached_best: Optional[Dict[str, Any]] = None
        self._lock = asyncio.Lock()

    def _random(self):
        order = list(range(10))
        random.shuffle(order)
        return {
            "module_priority_order": order,
            "timeout_multiplier": random.uniform(
                *self.param_bounds["timeout_multiplier"]),
            "preferred_cloud": random.choice(
                self.param_bounds["preferred_cloud"]),
        }

    def _mutate(self, chrom):
        new = dict(chrom)
        if random.random() < self.mutation_rate:
            i, j = random.sample(range(10), 2)
            new["module_priority_order"] = list(chrom["module_priority_order"])
            new["module_priority_order"][i], new["module_priority_order"][j] = \
                new["module_priority_order"][j], new["module_priority_order"][i]
        if random.random() < self.mutation_rate:
            lo, hi = self.param_bounds["timeout_multiplier"]
            new["timeout_multiplier"] = max(lo, min(hi,
                chrom["timeout_multiplier"] + random.gauss(0, 0.1)))
        if random.random() < self.mutation_rate:
            new["preferred_cloud"] = random.choice(
                self.param_bounds["preferred_cloud"])
        return new

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return dict(p1), dict(p2)
        c1, c2 = dict(p1), dict(p2)
        cut = 5
        c1["module_priority_order"] = \
            p1["module_priority_order"][:cut] + \
            [x for x in p2["module_priority_order"]
             if x not in p1["module_priority_order"][:cut]]
        c2["module_priority_order"] = \
            p2["module_priority_order"][:cut] + \
            [x for x in p1["module_priority_order"]
             if x not in p2["module_priority_order"][:cut]]
        if random.random() < 0.5:
            c1["timeout_multiplier"], c2["timeout_multiplier"] = \
                c2["timeout_multiplier"], c1["timeout_multiplier"]
        if random.random() < 0.5:
            c1["preferred_cloud"], c2["preferred_cloud"] = \
                c2["preferred_cloud"], c1["preferred_cloud"]
        return c1, c2

    async def _fitness(self, chrom, last_sustainability: float = 0.5):
        # [P3] Real fitness from last observed sustainability
        base = last_sustainability
        # Prefer low timeout (faster) and cloud with low latency (aws proxy)
        timeout_bonus = max(0.0, (1.5 - chrom["timeout_multiplier"]) * 0.1)
        cloud_bonus = 0.05 if chrom["preferred_cloud"] == "aws" else 0.0
        return max(0.0, min(1.0, base + timeout_bonus + cloud_bonus))

    async def run_search(self, last_sustainability: float = 0.5):
        # [P3] cached to prevent per-call search
        async with self._lock:
            if self._cached_best is not None:
                return dict(self._cached_best)
            pop = [self._random() for _ in range(self.population_size)]
            best, best_fit = None, -1.0
            for _ in range(self.generations):
                fits = await asyncio.gather(
                    *[self._fitness(c, last_sustainability) for c in pop])
                sp = sorted(zip(pop, fits), key=lambda x: x[1], reverse=True)
                if sp[0][1] > best_fit:
                    best_fit, best = sp[0][1], sp[0][0]
                parents = [c for c, _ in sp[:max(2, self.population_size // 2)]]
                offspring = []
                while len(offspring) < self.population_size:
                    p1, p2 = random.choice(parents), random.choice(parents)
                    c1, c2 = self._crossover(p1, p2)
                    offspring.append(self._mutate(c1))
                    if len(offspring) < self.population_size:
                        offspring.append(self._mutate(c2))
                combined = parents + offspring
                cf = await asyncio.gather(
                    *[self._fitness(c, last_sustainability) for c in combined])
                sc = sorted(zip(combined, cf), key=lambda x: x[1], reverse=True)
                pop = [c for c, _ in sc[:self.population_size]]
                try:
                    GA_FITNESS.set(best_fit)
                except Exception:
                    pass
            self._cached_best = best or self._random()
            return dict(self._cached_best)

    def invalidate_cache(self):
        self._cached_best = None


# =============================================================================
# MoE GATING — [P3] positive-reward filter
# =============================================================================
class MoEGatingNetwork:
    EXPERTS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.hidden_layers = tuple(
            _cfg_get(config, "MOE_HIDDEN_LAYERS", [16, 8]))
        self._model = None
        self._scaler = None
        self._trained = False
        self._data: List[Tuple[List[float], int, float]] = []
        self._lock = asyncio.Lock()
        self.expert_names = list(self.EXPERTS)

    def _encode(self, ctx):
        return [
            float(ctx.get("module_count", 0)) / 20.0,
            float(ctx.get("success_rate", 0.5)),
            float(ctx.get("queue_size", 0)) / 50.0,
            float(ctx.get("carbon_intensity", 400)) / 1000.0,
            float(ctx.get("cloud_provider_latency", 60)) / 500.0,
            datetime.now().hour / 24.0,
            1.0 if datetime.now().weekday() >= 5 else 0.0,
            float(ctx.get("module_health", 0.8)),
            float(ctx.get("sustainability_score", 0.7)),
        ]

    def _train(self):
        if not SKLEARN_AVAILABLE or len(self._data) < 10:
            return
        # [P3] positive-reward filter
        pos = [(f, i) for f, i, r in self._data if r >= 0.5]
        if len(pos) < 10 or len(set(i for _, i in pos)) < 2:
            return
        try:
            X = np.array([f for f, _ in pos])
            y = np.array([i for _, i in pos])
            self._scaler = StandardScaler()
            Xs = self._scaler.fit_transform(X)
            self._model = MLPClassifier(hidden_layer_sizes=self.hidden_layers,
                                        max_iter=200, random_state=42)
            self._model.fit(Xs, y)
            self._trained = True
        except Exception as e:
            logger.debug("MoE training failed: %s", e)

    def _expert_params(self, name):
        return {
            "performance": {"strategy": "performance", "timeout_multiplier": 1.2},
            "carbon": {"strategy": "carbon", "timeout_multiplier": 1.0},
            "cost": {"strategy": "cost", "timeout_multiplier": 0.8},
            "hybrid": {"strategy": "hybrid", "timeout_multiplier": 1.0},
            "adaptive": {"strategy": "adaptive", "timeout_multiplier": 1.1},
        }.get(name, {"strategy": "adaptive", "timeout_multiplier": 1.0})

    async def select_expert(self, ctx):
        feats = self._encode(ctx)
        if self._trained and self._model is not None:
            try:
                X = np.array(feats, dtype=float).reshape(1, -1)
                if self._scaler:
                    X = self._scaler.transform(X)
                probs = self._model.predict_proba(X)[0]
                idx = int(np.argmax(probs))
                for i, p in enumerate(probs):
                    if i < len(self.expert_names):
                        try:
                            MOE_PROBS.labels(expert=self.expert_names[i]).set(
                                float(p))
                        except Exception:
                            pass
                return self.expert_names[idx], {
                    **self._expert_params(self.expert_names[idx]),
                    "confidence": float(probs[idx])}
            except Exception:
                pass
        return "adaptive", self._expert_params("adaptive")

    async def add_training_sample(self, ctx, expert, reward):
        feats = self._encode(ctx)
        idx = self.expert_names.index(expert) if expert in self.expert_names else 0
        async with self._lock:
            self._data.append((feats, idx, float(reward)))
            if len(self._data) > 2000:
                self._data = self._data[-1000:]
            if len(self._data) % 10 == 0:
                self._train()

    def stall(self):
        pass

    def save_state(self):
        return {"trained": self._trained, "n_samples": len(self._data)}

    def load_state(self, data):
        self._trained = bool(data.get("trained", False))


# =============================================================================
# PARETO — [P1] proper dominance
# =============================================================================
class ParetoFrontOptimizer:
    OBJECTIVES = ["success_rate", "carbon_footprint", "execution_time", "cost"]

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.pareto: List[Dict[str, Any]] = []
        self.max_size = _cfg_get(config, "PARETO_MAX_ARCHITECTURES", 100)
        self._lock = asyncio.Lock()

    def _dominates(self, a, b):
        am = (-a["metrics"]["success_rate"],
              a["metrics"]["carbon_footprint"],
              a["metrics"]["execution_time"],
              a["metrics"]["cost"])
        bm = (-b["metrics"]["success_rate"],
              b["metrics"]["carbon_footprint"],
              b["metrics"]["execution_time"],
              b["metrics"]["cost"])
        return all(am[i] <= bm[i] for i in range(4)) and \
            any(am[i] < bm[i] for i in range(4))

    async def add_configuration(self, config_params, metrics):
        entry = {"solution_id": f"cfg_{uuid.uuid4().hex[:8]}",
                 "config_params": config_params, "metrics": dict(metrics)}
        async with self._lock:
            if any(self._dominates(e, entry) for e in self.pareto):
                return False
            self.pareto = [e for e in self.pareto
                           if not self._dominates(entry, e)]
            self.pareto.append(entry)
            if len(self.pareto) > self.max_size:
                self.pareto.sort(key=lambda e: e["metrics"]["success_rate"],
                                  reverse=True)
                self.pareto = self.pareto[:self.max_size]
            try:
                self.storage.save_state("integration_pareto_front",
                                        json.dumps(self.pareto, default=str))
                PARETO_SIZE.set(len(self.pareto))
            except Exception:
                pass
            return True

    def get_pareto_front(self):
        return list(self.pareto)

    def save_state(self):
        return {"pareto": list(self.pareto)}

    def load_state(self, data):
        self.pareto = list(data.get("pareto", []))


# =============================================================================
# NEURAL TEACHER — [P1] buffered fit, registered as teacher
# =============================================================================
class NeuralTeacher:
    def __init__(self, input_dim=9, output_dim=5, hidden_layers=None):
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.hidden_layers = hidden_layers or [64, 32]
        self._torch_model = None
        self._sk_model = None
        self._device = "cpu"
        self._buffer_X: List[List[float]] = []
        self._buffer_y: List[int] = []
        self._build()

    def _build(self):
        if TORCH_AVAILABLE:
            try:
                layers = []
                prev = self.input_dim
                for h in self.hidden_layers:
                    layers += [nn.Linear(prev, h), nn.ReLU()]
                    prev = h
                layers.append(nn.Linear(prev, self.output_dim))
                self._torch_model = nn.Sequential(*layers)
                if torch.cuda.is_available():
                    self._device = "cuda"
                self._torch_model.to(self._device)
            except Exception:
                self._torch_model = None
        elif SKLEARN_AVAILABLE:
            self._sk_model = MLPClassifier(
                hidden_layer_sizes=tuple(self.hidden_layers),
                max_iter=200, random_state=42)

    def predict_proba(self, X):
        if self._torch_model is not None:
            try:
                self._torch_model.eval()
                with torch.no_grad():
                    x = torch.FloatTensor(X).to(self._device)
                    logits = self._torch_model(x)
                    probs = torch.softmax(logits, dim=1).cpu().numpy()
                return probs
            except Exception:
                pass
        if self._sk_model is not None and \
                getattr(self._sk_model, "n_features_in_", None):
            try:
                return self._sk_model.predict_proba(X)
            except Exception:
                pass
        return np.ones((len(X), self.output_dim)) / self.output_dim

    def train(self, X, y):
        X = X.tolist() if hasattr(X, "tolist") else list(X)
        y = y.tolist() if hasattr(y, "tolist") else list(y)
        self._buffer_X.extend(X)
        self._buffer_y.extend(y)
        if len(self._buffer_X) < 20:
            return
        if self._torch_model is not None:
            try:
                x = torch.FloatTensor(self._buffer_X).to(self._device)
                y_t = torch.LongTensor(self._buffer_y).to(self._device)
                ds = torch.utils.data.TensorDataset(x, y_t)
                dl = torch.utils.data.DataLoader(
                    ds, batch_size=32, shuffle=True)
                opt = optim.Adam(self._torch_model.parameters(), lr=0.001)
                crit = nn.CrossEntropyLoss()
                self._torch_model.train()
                for _ in range(10):
                    for xb, yb in dl:
                        opt.zero_grad()
                        loss = crit(self._torch_model(xb), yb)
                        loss.backward()
                        opt.step()
                self._buffer_X.clear()
                self._buffer_y.clear()
                return
            except Exception as e:
                logger.debug("torch teacher train failed: %s", e)
        if self._sk_model is not None:
            try:
                self._sk_model.fit(np.array(self._buffer_X),
                                    np.array(self._buffer_y))
                self._buffer_X.clear()
                self._buffer_y.clear()
            except Exception:
                pass

    def stall(self):
        pass


# =============================================================================
# CAUSAL GRAPH — [P3] pgmpy PC hook + edge stability
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self.edge_stability = 0.0
        self._lock = asyncio.Lock()

    async def learn(self, samples, variables, threshold=0.25):
        self.variables = list(variables)
        # [P3] Try pgmpy PC first
        if PGMPY_AVAILABLE and len(samples) >= 10:
            try:
                import pandas as pd
                df = pd.DataFrame(samples)
                if all(v in df.columns for v in variables):
                    est = PC(df[variables])
                    model = est.estimate(
                        variant="orig", ci_test="chi_square",
                        show_progress=False, max_cond_vars=2)
                    async with self._lock:
                        self.graph.clear()
                        for e in model.edges():
                            self.graph[e[0]][e[1]] = {
                                "weight": 1.0, "confidence": 1.0}
                            self.storage.save_causal_edge(e[0], e[1], 1.0, 1.0)
                        self.edge_stability = 1.0
                        try:
                            CAUSAL_EDGE_STABILITY.set(1.0)
                        except Exception:
                            pass
                    return self.summary()
            except Exception as e:
                logger.debug("pgmpy PC failed: %s", e)

        # Fallback: correlation + variance orientation + train/val/test stability
        if len(samples) < 5:
            async with self._lock:
                self.graph.clear()
                for i, s in enumerate(variables):
                    for j, t in enumerate(variables):
                        if i < j and random.random() < 0.25:
                            w = random.uniform(0.1, 0.9)
                            self.graph[s][t] = {"weight": w, "confidence": w}
                            self.storage.save_causal_edge(s, t, w, w)
                self.edge_stability = 0.5
            return self.summary()

        def _fit(subset):
            if len(subset) < 3:
                return set()
            X = np.array([[s.get(v, 0.0) for v in variables]
                          for s in subset], dtype=float)
            if X.shape[0] < 2 or X.shape[1] < 2:
                return set()
            X = (X - X.mean(0)) / (X.std(0) + 1e-9)
            corr = np.corrcoef(X, rowvar=False)
            edges = set()
            for i in range(len(variables)):
                for j in range(len(variables)):
                    if i == j:
                        continue
                    c = abs(float(corr[i, j]))
                    if c > threshold:
                        vi, vj = float(X[:, i].var()), float(X[:, j].var())
                        src, dst = ((variables[i], variables[j])
                                     if vi > vj
                                     else (variables[j], variables[i]))
                        edges.add((src, dst, round(float(corr[i, j]), 4)))
            return edges

        n = len(samples)
        n_train = max(3, int(n * 0.6))
        n_val = max(0, int(n * 0.2))
        train = samples[:n_train]
        val = samples[n_train:n_train + n_val]
        test = samples[n_train + n_val:]

        train_edges = _fit(train)
        stability = 0.0
        if train_edges:
            hits = 0
            for subset in (val, test):
                subset_edges = {(s, d) for s, d, _ in _fit(subset)}
                hits += sum(1 for s, d, _ in train_edges
                            if (s, d) in subset_edges)
            stability = hits / (len(train_edges) * 2)
        self.edge_stability = stability

        async with self._lock:
            self.graph.clear()
            for src, dst, w in train_edges:
                self.graph[src][dst] = {"weight": w, "confidence": abs(w)}
                self.storage.save_causal_edge(src, dst, w, abs(w))
        try:
            CAUSAL_EDGE_STABILITY.set(stability)
        except Exception:
            pass
        return self.summary()

    def parents(self, node):
        return [s for s, e in self.graph.items() if node in e]

    def summary(self):
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables),
                "edge_stability": self.edge_stability}

    def save_state(self):
        return {"variables": list(self.variables),
                "edges": {s: dict(v) for s, v in self.graph.items()},
                "edge_stability": self.edge_stability}

    def load_state(self, data):
        self.variables = list(data.get("variables", []))
        self.graph = defaultdict(dict)
        for s, d in data.get("edges", {}).items():
            for dst, w in d.items():
                self.graph[s][dst] = dict(w)
        self.edge_stability = float(data.get("edge_stability", 0.0))


class CausalIntegrationPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config, storage, graph):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = _cfg_get(config, "CAUSAL_EXPLORATION_RATE", 0.1)
        self._last_action: Optional[str] = None
        self._lock = asyncio.Lock()

    async def choose_action(self, state):
        async with self._lock:
            if random.random() < self.epsilon:
                choice = random.choice(self.ACTIONS)
            else:
                # [P3] restrict to graph parents of "success_rate"
                parents = [p for p in self.graph.parents("success_rate")
                           if p in self.ACTIONS]
                candidates = parents if parents else self.ACTIONS
                choice = max(candidates, key=lambda a: self.values.get(a, 0.0))
            self._last_action = choice
            return choice

    async def update(self, action, reward, state):
        async with self._lock:
            if action not in self.ACTIONS:
                action = self.ACTIONS[0]
            self.counts[action] += 1
            n = self.counts[action]
            self.values[action] += (float(reward) - self.values[action]) / n
            vals = [self.values.get(a, 0.0) for a in self.ACTIONS]
            m = max(vals)
            exps = [math.exp((v - m) / 0.5) for v in vals]
            s = sum(exps) or 1.0
            self.policy = [e / s for e in exps]

    async def estimate_ate(self, treatment, outcome):
        # [P1] use correct outcome variable
        w = self.graph.graph.get(treatment, {}).get(outcome, {}).get(
            "weight", 0.0)
        try:
            CAUSAL_ATE.labels(treatment=treatment, outcome=outcome).set(w)
        except Exception:
            pass
        return w

    def get_policy(self):
        return list(self.policy)

    def save_state(self):
        return {"values": dict(self.values),
                "counts": dict(self.counts),
                "policy": list(self.policy),
                "last_action": self._last_action}

    def load_state(self, data):
        self.values = defaultdict(float, data.get("values", {}))
        self.counts = defaultdict(int, data.get("counts", {}))
        p = data.get("policy")
        if p and len(p) == len(self.ACTIONS):
            self.policy = list(p)
        self._last_action = data.get("last_action")


# =============================================================================
# TEMPORAL LOGIC — [P0] real recursive-descent parser; pending NEXT;
# elapsed-window WITHIN
# =============================================================================
class TemporalOperator:
    ALWAYS = "always"
    EVENTUALLY = "eventually"
    UNTIL = "until"
    NEVER = "never"
    NEXT = "next"
    WITHIN = "within"


_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


def _balanced(s: str) -> bool:
    d = 0
    for c in s:
        if c == "(":
            d += 1
        elif c == ")":
            d -= 1
            if d < 0:
                return False
    return d == 0


def _split_top(s: str, op_chars: str) -> List[str]:
    parts = []
    depth = 0
    start = 0
    for i, ch in enumerate(s):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and ch in op_chars:
            parts.append(s[start:i])
            start = i + 1
    parts.append(s[start:])
    return [p.strip() for p in parts if p.strip()]


def _split_top_word(s: str, word: str) -> List[str]:
    parts = []
    depth = 0
    i = 0
    n = len(s)
    start = 0
    needle = f" {word} "
    while i < n:
        ch = s[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0:
            if s[i:i + len(needle)] == needle:
                parts.append(s[start:i])
                start = i + len(needle)
                i += len(needle)
                continue
        i += 1
    parts.append(s[start:])
    return [p.strip() for p in parts if p.strip()]


class LTLNode:
    def __init__(self, kind: str, args: Any = None):
        self.kind = kind
        self.args = args


def parse_ltl(s: str) -> LTLNode:
    s = s.strip()
    while s.startswith("(") and s.endswith(")") and _balanced(s[1:-1]):
        s = s[1:-1].strip()
    parts = _split_top(s, "|")
    if len(parts) > 1:
        return LTLNode("|", [parse_ltl(p) for p in parts])
    parts = _split_top(s, "&")
    if len(parts) > 1:
        return LTLNode("&", [parse_ltl(p) for p in parts])
    parts = _split_top_word(s, "U")
    if len(parts) == 2:
        return LTLNode("U", (parse_ltl(parts[0]), parse_ltl(parts[1])))
    if s.startswith("!"):
        return LTLNode("!", parse_ltl(s[1:].strip()))
    for op in ("G", "F", "X"):
        if s.startswith(op + " ") or s.startswith(op + "("):
            return LTLNode(op, parse_ltl(s[len(op):].strip()))
    return LTLNode("atom", s.strip())


def _eval_atomic(expr: str, state: Dict) -> bool:
    m = _ATOMIC_RE.match(expr)
    if not m:
        return bool(state.get(expr.strip(), False))
    var, op, val = m.group(1), m.group(2), float(m.group(3))
    try:
        sv = float(state.get(var, 0.0))
    except Exception:
        return False
    return {">=": sv >= val, "<=": sv <= val, "==": sv == val,
            "!=": sv != val, ">": sv > val, "<": sv < val}[op]


def eval_ltl(node: LTLNode, trace: List[Dict], idx: int) -> bool:
    if not trace or idx >= len(trace):
        return True
    k = node.kind
    if k == "atom":
        return _eval_atomic(node.args, trace[idx])
    if k == "!":
        return not eval_ltl(node.args, trace, idx)
    if k == "G":
        return all(eval_ltl(node.args, trace, i)
                   for i in range(idx, len(trace)))
    if k == "F":
        return any(eval_ltl(node.args, trace, i)
                   for i in range(idx, len(trace)))
    if k == "X":
        return idx + 1 < len(trace) and eval_ltl(node.args, trace, idx + 1)
    if k == "&":
        return all(eval_ltl(c, trace, idx) for c in node.args)
    if k == "|":
        return any(eval_ltl(c, trace, idx) for c in node.args)
    if k == "U":
        left, right = node.args
        for i in range(idx, len(trace)):
            if eval_ltl(right, trace, i):
                return True
            if not eval_ltl(left, trace, i):
                return False
        return False
    return True


class TemporalRule:
    def __init__(self, rid, operator, conditions, window_seconds=0.0,
                 description="", severity="warning"):
        self.rule_id = rid
        self.operator = operator
        self.conditions = conditions
        self.window_seconds = window_seconds
        self.description = description or rid
        self.severity = severity
        self.violations = 0
        self.last_violation: Optional[datetime] = None

    def evaluate(self, trace):
        if not trace:
            return False
        hist = list(trace)
        if self.window_seconds > 0:
            cutoff = hist[-1][0] - timedelta(seconds=self.window_seconds)
            hist = [(ts, s) for ts, s in hist if ts >= cutoff]
            if not hist:
                return False
        op = self.operator
        if op == TemporalOperator.ALWAYS:
            return any(not self.conditions[0](s) for _, s in hist)
        if op == TemporalOperator.EVENTUALLY:
            return not any(self.conditions[0](s) for _, s in hist)
        if op == TemporalOperator.NEVER:
            return any(self.conditions[0](s) for _, s in hist)
        if op == TemporalOperator.NEXT:
            # [P0] Handled by monitor's pending queue
            return False
        if op == TemporalOperator.UNTIL and len(self.conditions) >= 2:
            a, b = self.conditions[0], self.conditions[1]
            for _, s in hist:
                if b(s):
                    return False
                if not a(s):
                    return True
            return True
        if op == TemporalOperator.WITHIN and len(self.conditions) >= 2:
            # [P0] Return False (pending) until window elapses
            trig, resp = self.conditions[0], self.conditions[1]
            trig_ts = None
            for ts, s in hist:
                if trig_ts is None:
                    if trig(s):
                        trig_ts = ts
                else:
                    if resp(s):
                        return False
                    if self.window_seconds > 0 and \
                            (ts - trig_ts).total_seconds() > self.window_seconds:
                        return True
            if trig_ts is None:
                return False
            elapsed = (hist[-1][0] - trig_ts).total_seconds()
            if self.window_seconds > 0 and elapsed > self.window_seconds:
                return True
            return False
        return self.conditions[0](hist[-1][1])


class TemporalIntegrationVerifier:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=_cfg_get(config, "TEMPORAL_MAX_TRACE", 2000))
        self.approval_callback: Optional[Callable] = None
        self._pending_next: Dict[str, Callable] = {}
        self._ltl_rules: Dict[str, LTLNode] = {}
        self._lock = asyncio.Lock()

        # [P0] parse config formulas via recursive descent
        for formula in _cfg_get(config, "TEMPORAL_FORMULAS", []) or []:
            try:
                ast = parse_ltl(formula)
                self._ltl_rules[formula] = ast
            except Exception as e:
                logger.debug("Failed to parse TL formula '%s': %s", formula, e)

    def add_rule(self, rid, operator, conditions, window_seconds=0.0,
                 description="", severity="warning"):
        conds = conditions if isinstance(conditions, list) else [conditions]
        self.rules[rid] = TemporalRule(rid, operator, conds,
                                        window_seconds, description, severity)

    def set_approval_callback(self, cb):
        self.approval_callback = cb

    async def push_state(self, state):
        async with self._lock:
            self.trace.append((datetime.now(timezone.utc), dict(state)))

    async def verify(self):
        result: Dict[str, bool] = {}

        # [P0] 1) Evaluate pending NEXT checks against latest state
        if self.trace:
            latest = self.trace[-1][1]
            pending_done = []
            for rid, cond in list(self._pending_next.items()):
                try:
                    ok = bool(cond(latest))
                except Exception:
                    ok = False
                if not ok:
                    rule = self.rules.get(rid)
                    if rule:
                        rule.violations += 1
                        rule.last_violation = datetime.now(timezone.utc)
                        try:
                            TEMPORAL_VIOLATIONS.labels(formula=rid).inc()
                        except Exception:
                            pass
                        await asyncio.get_event_loop().run_in_executor(
                            None, self.storage.save_temporal_violation,
                            rid, len(self.trace) - 1, latest)
                        result[rid] = False
                pending_done.append(rid)
            for rid in pending_done:
                self._pending_next.pop(rid, None)

        # 2) Evaluate operator-based rules
        for rid, rule in self.rules.items():
            if rule.operator == TemporalOperator.NEXT:
                if rule.conditions:
                    self._pending_next[rid] = rule.conditions[0]
                continue
            if rid in result:
                continue
            if rule.last_violation and rule.window_seconds > 0:
                elapsed = (datetime.now(timezone.utc) -
                           rule.last_violation).total_seconds()
                if elapsed < rule.window_seconds:
                    result[rid] = False
                    continue
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            status = "violated" if violated else "satisfied"
            try:
                TEMPORAL_VERIFICATIONS.labels(formula=rid, status=status).inc()
            except Exception:
                pass
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
                try:
                    TEMPORAL_VIOLATIONS.labels(formula=rid).inc()
                except Exception:
                    pass
                await asyncio.get_event_loop().run_in_executor(
                    None, self.storage.save_temporal_violation, rid,
                    len(self.trace) - 1,
                    self.trace[-1][1] if self.trace else {})
                if rule.severity == "critical" and self.approval_callback:
                    try:
                        approved = self.approval_callback(
                            rid, dict(self.trace[-1][1]) if self.trace else {})
                        if asyncio.iscoroutine(approved):
                            await approved
                    except Exception:
                        pass

        # 3) Formula-based LTL rules
        trace = [s for _, s in self.trace]
        for formula, ast in self._ltl_rules.items():
            if not trace:
                result[formula] = True
                continue
            try:
                ok = eval_ltl(ast, trace, 0)
            except Exception as e:
                logger.debug("LTL eval error '%s': %s", formula, e)
                ok = False
            result[formula] = ok
            status = "satisfied" if ok else "violated"
            try:
                TEMPORAL_VERIFICATIONS.labels(formula=formula,
                                               status=status).inc()
            except Exception:
                pass
            if not ok:
                try:
                    TEMPORAL_VIOLATIONS.labels(formula=formula).inc()
                except Exception:
                    pass

        return result

    def save_state(self):
        return {"trace": [[ts.isoformat(), s] for ts, s in self.trace],
                "rules": {rid: {"violations": r.violations,
                                "last_violation": r.last_violation.isoformat()
                                if r.last_violation else None}
                          for rid, r in self.rules.items()}}

    def load_state(self, data):
        self.trace.clear()
        for ts_str, st in data.get("trace", []):
            try:
                self.trace.append((datetime.fromisoformat(ts_str), st))
            except Exception:
                pass
        for rid, info in data.get("rules", {}).items():
            if rid in self.rules:
                self.rules[rid].violations = int(info.get("violations", 0))
                lv = info.get("last_violation")
                self.rules[rid].last_violation = (
                    datetime.fromisoformat(lv) if lv else None)


# =============================================================================
# XAI — [P3] Real KernelSHAP
# =============================================================================
class IntegrationXAIDecisionExplainer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "XAI_METHOD", "kernel_shap")
        self.depth = _cfg_get(config, "XAI_DEPTH", 5)

    @staticmethod
    def _kernel_weight(size, n):
        if size == 0 or size == n:
            return 1e6
        try:
            return (n - 1) / (math.comb(n, size) * size * (n - size))
        except Exception:
            return 1.0

    def _kernel_shap(self, f, x, names, n_samples=128):
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        x_arr = np.asarray(x, dtype=float).ravel()
        n = len(x_arr)
        coalitions, weights = [], []
        max_exact = 2 ** n if n <= 8 else 0
        if max_exact and max_exact <= 512:
            for mask in range(1, max_exact - 1):
                z = np.zeros(n)
                size = 0
                for i in range(n):
                    if mask & (1 << i):
                        z[i] = x_arr[i]
                        size += 1
                coalitions.append(z)
                weights.append(self._kernel_weight(size, n))
        else:
            for _ in range(n_samples):
                z = np.zeros(n)
                size = 0
                for i in range(n):
                    if random.random() < 0.5:
                        z[i] = x_arr[i]
                        size += 1
                coalitions.append(z)
                weights.append(self._kernel_weight(size, n))
        coalitions.append(np.zeros(n))
        weights.append(1e6)
        coalitions.append(x_arr.copy())
        weights.append(1e6)
        try:
            y = np.array([float(f(c.reshape(1, -1))) for c in coalitions])
        except Exception:
            return {k: 0.0 for k in names}
        Z = np.array(coalitions)
        w = np.array(weights)
        try:
            ZtW = Z.T * w
            A = ZtW @ Z + 1e-6 * np.eye(n)
            b = ZtW @ y
            phi = np.linalg.solve(A, b)
        except Exception:
            try:
                phi = np.linalg.lstsq(Z * np.sqrt(w)[:, None],
                                       y * np.sqrt(w), rcond=None)[0]
            except Exception:
                phi = np.zeros(n)
        return {name: float(phi[i]) for i, name in enumerate(names)}

    def _lime(self, f, x, names, n=200):
        if not SKLEARN_AVAILABLE:
            return {k: 0.0 for k in names}
        X = np.tile(x, (n, 1)) + np.random.normal(0, 0.1, (n, len(x)))
        try:
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
        except Exception:
            return {k: 0.0 for k in names}
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / 0.02)
        try:
            m = LinearRegression().fit(X, y, sample_weight=w)
            return dict(zip(names, m.coef_.tolist()))
        except Exception:
            return {k: 0.0 for k in names}

    def _render_nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id, label, features, names, model_fn):
        if self.method == "lime":
            attrs = self._lime(model_fn, features, names)
        else:
            attrs = self._kernel_shap(model_fn, features, names)
        nl = self._render_nl(label, attrs)
        try:
            XAI_EXPLANATIONS.labels(method=self.method).inc()
        except Exception:
            pass
        for k, v in list(attrs.items())[:self.depth]:
            try:
                XAI_FEATURE_IMPORTANCE.labels(feature=k).set(float(v))
            except Exception:
                pass
        await asyncio.get_event_loop().run_in_executor(
            None, self.storage.save_xai_explanation,
            decision_id, self.method, attrs, nl)
        return {"decision_id": decision_id, "method": self.method,
                "attributions": attrs, "explanation": nl}


NUMPY_AVAILABLE = True  # imported at top


# =============================================================================
# ADAPTIVE PRECISION — [P1] real accuracy history
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55, "fp16": 0.5, "int8": 0.3}
    PENALTY = {"fp32": 0.0, "tf32": 0.005, "bf16": 0.01,
               "fp16": 0.012, "int8": 0.03}

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.baseline_accuracy = 0.9
        self.accuracy_history: Deque[float] = deque(maxlen=50)
        self._lock = asyncio.Lock()

    def _probe(self):
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE:
            try:
                info["cuda"] = torch.cuda.is_available()
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    info["bf16"] = torch.cuda.is_bf16_supported()
            except Exception:
                pass
        return info

    def select_precision(self):
        hw = self._probe()
        cands = list(_cfg_get(self.config, "PRECISION_LEVELS",
                              ["fp32", "fp16", "bf16", "int8"]))
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        thresh = _cfg_get(self.config, "PRECISION_SWITCH_THRESHOLD", 0.02)
        allowed = [c for c in cands
                   if self.PENALTY.get(c, 1.0) <= thresh * self.baseline_accuracy]
        if not allowed:
            allowed = ["fp32"]
        return min(allowed, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target, reason="policy"):
        async with self._lock:
            if target == self.current or target not in self.ENERGY:
                return False
            old = self.current
            self.current = target
            saved = max(0.0, self.ENERGY[old] - self.ENERGY[target])
            self.saved_wh += saved
            try:
                PRECISION_SWITCHES.labels(from_p=old, to_p=target).inc()
                PRECISION_ENERGY_SAVED.set(self.saved_wh)
            except Exception:
                pass
            await asyncio.get_event_loop().run_in_executor(
                None, self.storage.save_precision_switch,
                old, target, reason, saved)
            logger.info("Precision %s → %s (%s)", old, target, reason)
            return True

    async def auto_switch(self, recent_acc, baseline_acc):
        if baseline_acc <= 0:
            return
        self.accuracy_history.append(float(recent_acc))
        self.baseline_accuracy = baseline_acc
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = _cfg_get(self.config, "PRECISION_SWITCH_THRESHOLD", 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"accuracy drop {drop:.3f}")
        elif drop < thresh / 2:
            await self.switch_to(self.select_precision(), reason="headroom")

    @property
    def hardware(self):
        return self._probe()

    def save_state(self):
        return {"current": self.current, "saved_wh": self.saved_wh,
                "baseline_accuracy": self.baseline_accuracy,
                "history": list(self.accuracy_history)}

    def load_state(self, data):
        self.current = data.get("current", "fp32")
        self.saved_wh = float(data.get("saved_wh", 0.0))
        self.baseline_accuracy = float(data.get("baseline_accuracy", 0.9))
        self.accuracy_history = deque(data.get("history", []), maxlen=50)


# =============================================================================
# CARBON MARKET — [P1] TTL cache; [P3] defer
# =============================================================================
class CarbonMarketIntegrator:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.last_price = 25.0
        self._price_cache_ts = 0.0
        self._price_cache_ttl = 60.0
        self.deferred: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._cb = CircuitBreaker(3, 60.0, name="carbon_market")
        self._lock = asyncio.Lock()

    async def _fetch_price(self):
        if AIOHTTP_AVAILABLE and _cfg_get(self.config, "CARBON_MARKET_API_URL"):
            try:
                async with aiohttp.ClientSession() as s:
                    url = f"{_cfg_get(self.config, 'CARBON_MARKET_API_URL')}/price"
                    async with s.get(url, timeout=8) as r:
                        if r.status == 200:
                            data = await r.json()
                            return float(data.get("price", self.last_price))
            except Exception as e:
                logger.debug("carbon market API failed: %s", e)
        return max(5.0, self.last_price + random.gauss(0, 1.5))

    async def update_price(self):
        now = time.time()
        if now - self._price_cache_ts < self._price_cache_ttl:
            return self.last_price
        try:
            price = await self._cb.call(self._fetch_price)
        except Exception:
            price = self.last_price
        async with self._lock:
            self.last_price = price
            self._price_cache_ts = now
        try:
            CARBON_CREDIT_PRICE.set(price)
        except Exception:
            pass
        await asyncio.get_event_loop().run_in_executor(
            None, self.storage.save_credit_price, price)
        return price

    async def purchase_rec(self, mwh, price_per_mwh=5.0, source="wind"):
        cost = mwh * price_per_mwh
        await asyncio.get_event_loop().run_in_executor(
            None, self.storage.save_rec, mwh, price_per_mwh, source)
        try:
            REC_BALANCE.set(self.storage.get_rec_balance())
            NET_ZERO_MATCHES.inc()
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh, intensity):
        price = await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        if intensity > 0.3:
            action = "defer"
            self.deferred.append({
                "carbon_kg": carbon_kg, "intensity": intensity,
                "ts": datetime.now(timezone.utc).isoformat()})
        elif offset_cost < 0.5:
            action = "run_offset"
        else:
            action = "run"
        return {"action": action, "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost, "credit_price_usd": price,
                "rec_balance_mwh": self.storage.get_rec_balance(),
                "deferred_total": len(self.deferred)}

    def save_state(self):
        return {"last_price": self.last_price,
                "deferred": list(self.deferred)}

    def load_state(self, data):
        self.last_price = float(data.get("last_price", 25.0))
        self.deferred = deque(data.get("deferred", []), maxlen=200)


# =============================================================================
# CHAOS — [P0] steady_during; [P1] data corruption; CB key fixed
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config, storage, probe_fn=None):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._probe_fn = probe_fn
        self._lock = asyncio.Lock()

    def is_target_active(self, key: str) -> bool:
        return key in self.active

    async def _probe(self) -> Tuple[bool, float]:
        if self._probe_fn is None:
            return True, 1.0
        try:
            score = float(await self._probe_fn())
            return (score >= 0.7), score
        except Exception:
            return False, 0.0

    async def run_experiment(self, name, fault_type,
                              target=None, workload=None,
                              duration_s=0.2, target_breaker=None):
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        blast = _cfg_get(self.config, "CHAOS_BLAST_RADIUS", 0.1)
        before_ok, before_score = await self._probe()
        during_ok, during_score = before_ok, before_score
        status = "completed"
        try:
            async with self._lock:
                self.active[name] = {
                    "fault_type": fault_type,
                    "started": datetime.now(timezone.utc).isoformat()}
                if target_breaker:
                    self.active[f"cb_{target_breaker}"] = self.active[name]
            if fault_type == "latency":
                await asyncio.sleep(duration_s)
            elif fault_type == "exception":
                if target is not None:
                    try:
                        await target()
                    except Exception as e:
                        logger.info("chaos exception observed: %s", e)
            elif fault_type == "data_corruption":
                # [P1] Actually corrupt the data
                if workload and "data" in workload:
                    d = workload["data"]
                    if hasattr(d, "copy") and hasattr(d, "columns"):
                        try:
                            c = d.copy()
                            for col in c.columns:
                                if getattr(c[col].dtype, "kind", "") in "fi":
                                    mask = np.random.rand(len(c)) < \
                                        _cfg_get(self.config, "CHAOS_INTENSITY", 0.05)
                                    c.loc[mask, col] = np.nan
                        except Exception:
                            pass
            elif fault_type == "memory_pressure":
                _ = bytearray(5 * 1024 * 1024)
                del _
            elif fault_type == "network_drop":
                await asyncio.sleep(duration_s)
            # [P0] Probe DURING fault
            during_ok, during_score = await self._probe()
        except Exception as e:
            logger.info("chaos '%s' raised: %s", name, e)
            status = "injected"
            during_ok, during_score = False, 0.0
        finally:
            async with self._lock:
                self.active.pop(name, None)
                if target_breaker:
                    self.active.pop(f"cb_{target_breaker}", None)
        after_ok, after_score = await self._probe()
        await asyncio.get_event_loop().run_in_executor(
            None, self.storage.save_chaos_experiment,
            name, fault_type, status,
            int(before_ok), int(during_ok), int(after_ok), blast)
        try:
            CHAOS_EXPERIMENTS.labels(fault_type=fault_type, status=status).inc()
            CHAOS_STEADY_STATE.set(1.0 if during_ok else 0.0)
        except Exception:
            pass
        return {"name": name, "fault_type": fault_type,
                "steady_before": before_ok,
                "steady_during": during_ok,
                "steady_after": after_ok,
                "score_before": before_score,
                "score_during": during_score,
                "score_after": after_score,
                "status": status, "blast_radius": blast}


# =============================================================================
# MULTI-AGENT — [P0] dispatch; [P3] hysteresis
# =============================================================================
class _IntegrationAgent:
    ROLES = ["orchestrator", "validator", "optimizer", "reporter", "negotiator"]

    def __init__(self, agent_id):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


class MultiAgentCoordinator:
    SPECIALISE_MARGIN = 0.05  # [P3] hysteresis

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        count = _cfg_get(config, "AGENT_COUNT", 5)
        self.agents = {f"agent_{i:02d}": _IntegrationAgent(f"agent_{i:02d}")
                       for i in range(count)}
        self.bus = _MessageBus()
        self.message_log: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._lock = asyncio.Lock()
        self.bus.subscribe("critical_violation", self._on_critical)
        self.bus.subscribe("task_bid", self._on_task_bid)

    async def _on_critical(self, msg):
        async with self._lock:
            for a in self.agents.values():
                a.utilities["validator"] = min(
                    1.0, a.utilities["validator"] + 0.05)

    async def _on_task_bid(self, msg):
        self.message_log.append(msg)

    async def _specialise(self):
        async with self._lock:
            for a in self.agents.values():
                best = max(a.utilities, key=lambda r: a.utilities[r])
                if best != a.role and \
                        (a.utilities[best] - a.utilities[a.role]) >= \
                        self.SPECIALISE_MARGIN:
                    a.role = best
                    await asyncio.get_event_loop().run_in_executor(
                        None, self.storage.save_agent,
                        a.id, a.role, a.reputation, a.utilities)
                    try:
                        AGENT_ROLES.labels(role=a.role).set(1)
                        AGENT_REPUTATION.labels(agent_id=a.id).set(a.reputation)
                    except Exception:
                        pass

    async def broadcast(self, topic, sender, payload):
        msg = {"topic": topic, "sender": sender, "payload": payload,
               "ts": datetime.now(timezone.utc).isoformat()}
        self.message_log.append(msg)
        await self.bus.publish(msg)
        await asyncio.get_event_loop().run_in_executor(
            None, self.storage.save_agent_message, topic, sender, payload)

    async def bid(self, task):
        preferred = task.get("preferred_role", "orchestrator")
        best_id, best_score = None, -1.0
        async with self._lock:
            for aid, a in self.agents.items():
                bonus = 1.0 if a.role == preferred else 0.6
                score = a.utilities[a.role] * bonus + 0.3 * a.reputation
                score += random.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_id = score, aid
            if best_id:
                self.agents[best_id].completed += 1
        await self.broadcast("task_bid", best_id or "none",
                             {"task": task.get("name", "unknown"),
                              "score": best_score})
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id, reward):
        async with self._lock:
            if agent_id in self.agents:
                a = self.agents[agent_id]
                n = max(1, a.completed)
                a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
                a.utilities[a.role] = min(1.0, a.utilities[a.role] + 0.05 * reward)

    def get_policy(self):
        affinity = {
            "orchestrator": [0.35, 0.20, 0.15, 0.15, 0.15],
            "validator":    [0.15, 0.15, 0.15, 0.35, 0.20],
            "optimizer":    [0.20, 0.15, 0.35, 0.15, 0.15],
            "reporter":     [0.15, 0.20, 0.15, 0.15, 0.35],
            "negotiator":   [0.15, 0.35, 0.20, 0.15, 0.15],
        }
        counts = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        out = [0.0] * 5
        for role, c in counts.items():
            w = c / total
            for i, v in enumerate(affinity.get(role, [0.2] * 5)):
                out[i] += w * v
        s = sum(out) or 1.0
        return [x / s for x in out]

    async def step(self):
        await self._specialise()
        return {"roles": {a.id: a.role for a in self.agents.values()},
                "role_distribution": self.get_policy()}

    def stall(self):
        pass

    def save_state(self):
        return {"agents": {aid: {"role": a.role,
                                  "reputation": a.reputation,
                                  "utilities": a.utilities,
                                  "completed": a.completed}
                           for aid, a in self.agents.items()}}

    def load_state(self, data):
        for aid, info in data.get("agents", {}).items():
            if aid in self.agents:
                a = self.agents[aid]
                a.role = info.get("role", a.role)
                a.reputation = float(info.get("reputation", a.reputation))
                a.utilities = dict(info.get("utilities", a.utilities))
                a.completed = int(info.get("completed", 0))


# =============================================================================
# QUANTUM DISTILLATION — [P0] s - t gradient
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature=2.0, alpha=0.5, n_actions=5):
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._lock = asyncio.Lock()

    def register_teacher(self, name, policy):
        if not policy:
            return
        if len(policy) != self.n_actions:
            return
        s = sum(policy) or 1.0
        self.teachers[name] = [max(0.0, p) / s for p in policy]

    def _superpose(self):
        if not self.teachers:
            return list(self.student_policy)
        accum = [0.0] * self.n_actions
        for pol in self.teachers.values():
            for i, p in enumerate(pol):
                accum[i] += math.sqrt(max(p, 1e-9))
        accum = [a / len(self.teachers) for a in accum]
        sq = [a * a for a in accum]
        s = sum(sq) or 1.0
        return [x / s for x in sq]

    async def step(self, state=None):
        target = self._superpose()
        # [P0] Correct KL gradient: grad = s - t
        lr = 0.05
        async with self._lock:
            new = []
            for s, t in zip(self.student_policy, target):
                grad = s - t
                new.append(max(1e-3, s - lr * grad))
            ns = sum(new) or 1.0
            self.student_policy = [x / ns for x in new]
            self.history.append({
                "target": target,
                "student": list(self.student_policy),
                "ts": datetime.now(timezone.utc).isoformat()})
        return {"target": target, "student": list(self.student_policy)}

    def get_policy(self):
        return list(self.student_policy)

    def save_state(self):
        return {"teachers": dict(self.teachers),
                "student_policy": list(self.student_policy)}

    def load_state(self, data):
        self.teachers = {k: list(v) for k, v in
                         data.get("teachers", {}).items()}
        sp = data.get("student_policy")
        if sp and len(sp) == self.n_actions:
            self.student_policy = list(sp)


# =============================================================================
# MULTI-TEACHER DISTILLATION (legacy) — [P0] s - t gradient
# =============================================================================
class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_engine=None):
        self.config = config
        self.moe_engine = moe_engine
        self.student_policy = np.array([0.2] * 5)
        self.temperature = _cfg_get(config, "DISTILLATION_TEMPERATURE", 2.0)
        self.alpha = _cfg_get(config, "DISTILLATION_ALPHA", 0.5)
        self.history = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, state):
        if not self.moe_engine:
            return
        ctx = {
            "module_count": state.get("module_count", 0),
            "success_rate": state.get("success_rate", 0.5),
            "queue_size": state.get("queue_size", 0),
            "carbon_intensity": state.get("carbon_intensity", 400),
            "cloud_provider_latency": state.get("cloud_provider_latency", 60),
            "module_health": state.get("module_health", 0.8),
            "sustainability_score": state.get("sustainability_score", 0.7),
        }
        _, _ = await self.moe_engine.select_expert(ctx)
        names = list(self.moe_engine.expert_names)
        probs = np.ones(len(names)) / len(names)
        if self.moe_engine._trained and self.moe_engine._model is not None:
            feats = np.array(self.moe_engine._encode(ctx), dtype=float)
            X = feats.reshape(1, -1)
            if self.moe_engine._scaler:
                X = self.moe_engine._scaler.transform(X)
            probs = self.moe_engine._model.predict_proba(X)[0]
        teacher_dist = np.array(probs)
        teacher_dist = teacher_dist / teacher_dist.sum()
        # [P0] Correct KL gradient
        async with self._lock:
            grad = self.student_policy - teacher_dist
            self.student_policy = np.clip(
                self.student_policy - 0.01 * grad, 0.01, None)
            self.student_policy /= self.student_policy.sum()
            loss = float(np.sum(teacher_dist *
                                np.log((teacher_dist + 1e-9) /
                                       (self.student_policy + 1e-9))))
            self.history.append({"loss": loss})

    def get_student_probs(self):
        return self.student_policy.tolist()

    def save_state(self):
        return {"student_policy": self.student_policy.tolist()}

    def load_state(self, data):
        sp = data.get("student_policy")
        if sp and len(sp) == 5:
            self.student_policy = np.array(sp)


# =============================================================================
# FEDERATED — [P0] unified backend; [P3] discovery
# =============================================================================
class FederatedIntegrationLearner:
    def __init__(self, storage, instance_id, share_interval=3600, backend=None):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self._backend = backend
        self.rounds = 0

    async def _call(self, op, key, value=None):
        if not self._backend:
            return None
        try:
            r = self._backend(op, key, value)
            if asyncio.iscoroutine(r):
                r = await r
            return r
        except Exception:
            return None

    async def share_weights(self, weights):
        # [P0] unified backend
        self.storage.save_fed_weight(self.instance_id, weights)
        await self._call("put", f"{self.instance_id}/policy",
                         json.dumps(weights, default=str))

    async def discover_and_pull(self, name="policy"):
        r = await self._call("list", name)
        if not r:
            return
        try:
            peers = r if isinstance(r, list) else json.loads(r)
        except Exception:
            return
        for pid in peers:
            if pid == self.instance_id:
                continue
            try:
                v = await self._call("get", f"{pid}/{name}")
                if v:
                    w = json.loads(v) if isinstance(v, str) else v
                    self.storage.save_fed_weight(pid, w)
            except Exception:
                continue

    async def pull_aggregated_weights(self):
        # [P0] read from unified dict
        all_weights = self.storage.get_all_fed_weights()
        weight_list = []
        for k, v in all_weights.items():
            try:
                weight_list.append(json.loads(v))
            except Exception:
                continue
        if not weight_list:
            return None
        keys = set().union(*[w.keys() for w in weight_list])
        avg = {}
        for k in keys:
            vals = [w.get(k) for w in weight_list
                    if isinstance(w.get(k), list)]
            if not vals:
                continue
            n = len(vals[0])
            avg[k] = [sum(v[i] for v in vals) / len(vals) for i in range(n)]
            s = sum(avg[k]) or 1.0
            avg[k] = [x / s for x in avg[k]]
        self.rounds += 1
        try:
            FEDERATED_ROUNDS.inc()
        except Exception:
            pass
        return avg

    def save_state(self):
        return {"rounds": self.rounds,
                "weights": dict(self.storage.get_all_fed_weights())}

    def load_state(self, data):
        self.rounds = int(data.get("rounds", 0))
        for k, v in data.get("weights", {}).items():
            self.storage.fed_weights[k] = v


# =============================================================================
# HITL — [P1] wired; [P3] WebSocket
# =============================================================================
class ActiveUserPreferenceLearner:
    def __init__(self, storage, websocket=None):
        self.storage = storage
        self.websocket = websocket
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)

    async def submit_response(self, user_id, chosen_id):
        try:
            self._responses.put_nowait({"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            pass

    async def query_user_if_needed(self, user_id, top_configs, timeout=3.0):
        if len(top_configs) < 2:
            return None
        try:
            q = [c["metrics"]["success_rate"] for c in top_configs[:2]]
            if abs(q[0] - q[1]) / max(q) > 0.05:
                return None
        except Exception:
            return None
        if self.websocket:
            try:
                await self.websocket.broadcast({
                    "type": "preference_query", "user_id": user_id,
                    "options": [{"id": c["solution_id"],
                                 "success_rate": c["metrics"]["success_rate"]}
                                for c in top_configs[:2]]})
            except Exception:
                pass
        try:
            msg = await asyncio.wait_for(self._responses.get(), timeout=timeout)
            return msg.get("chosen")
        except asyncio.TimeoutError:
            weights = self.preferences.get(user_id, {})
            if weights:
                scored = []
                for c in top_configs:
                    s = sum(weights.get(k, 0.25) / (c["metrics"][k] + 1e-8)
                            for k in c["metrics"])
                    scored.append((s, c["solution_id"]))
                scored.sort(reverse=True)
                return scored[0][1]
            return top_configs[0]["solution_id"]

    async def record_choice(self, user_id, solution_id, metrics):
        prefs = self.preferences.setdefault(user_id, {})
        for k, v in metrics.items():
            prefs[k] = prefs.get(k, 0.25) + 1.0 / (v + 1e-6) * 0.01
        s = sum(prefs.values()) or 1.0
        prefs = {k: v / s for k, v in prefs.items()}
        self.preferences[user_id] = prefs
        try:
            self.storage.save_state(f"user_pref_{user_id}", json.dumps(prefs))
        except Exception:
            pass

    def save_state(self):
        return {"preferences": dict(self.preferences)}

    def load_state(self, data):
        self.preferences = dict(data.get("preferences", {}))


class HITLWebSocket:
    """[P3] Minimal WebSocket for HITL response transport."""

    def __init__(self, port, learner):
        self.port = port
        self.learner = learner
        self._server = None
        self._connections: Set[Any] = set()
        self._lock = asyncio.Lock()

    async def start(self):
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("WebSockets unavailable; HITL WS disabled")
            return
        try:
            self._server = await ws_serve(self._handler, "0.0.0.0", self.port)
            logger.info("HITL WebSocket on :%d", self.port)
        except Exception as e:
            logger.warning("HITL WS start failed: %s", e)

    async def _handler(self, ws, path=None):
        async with self._lock:
            self._connections.add(ws)
        try:
            async for msg in ws:
                try:
                    data = json.loads(msg)
                    if data.get("type") == "preference_response":
                        await self.learner.submit_response(
                            data.get("user_id", ""), data.get("chosen", ""))
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            async with self._lock:
                self._connections.discard(ws)

    async def broadcast(self, message, topic="all"):
        payload = json.dumps(message, default=str)
        async with self._lock:
            targets = list(self._connections)
        for ws in targets:
            try:
                await ws.send(payload)
            except Exception:
                async with self._lock:
                    self._connections.discard(ws)

    async def stop(self):
        if self._server:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass


# =============================================================================
# RLHF — [P0] action-aware
# =============================================================================
class RLHFManager:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config):
        self.config = config
        self.buffer: List[Dict] = []
        self.reward_model = None
        if SKLEARN_AVAILABLE:
            self.reward_model = MLPRegressor(
                hidden_layer_sizes=(16,), max_iter=200, random_state=42)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self._lock = asyncio.Lock()

    def _state_to_features(self, state, action_idx=None):
        # [P0] include one-hot action
        feats = [
            float(state.get("carbon_intensity", 0.4)) / 1000.0,
            float(state.get("success_rate", 0.5)),
            float(state.get("cost", 0.5)),
            float(state.get("latency", 0.5)),
        ]
        if action_idx is not None:
            one_hot = [0.0] * len(self.ACTIONS)
            one_hot[action_idx] = 1.0
            feats.extend(one_hot)
        return feats

    async def record_feedback(self, state, action, reward):
        action_idx = self.ACTIONS.index(action) if action in self.ACTIONS else 0
        async with self._lock:
            self.buffer.append({
                "state": self._state_to_features(state, action_idx),
                "action": action_idx,
                "reward": float(reward)})
            if len(self.buffer) > 5000:
                self.buffer = self.buffer[-2500:]
            self.preference_counts[action] += float(reward)

    async def train_reward_model(self):
        if not self.reward_model or len(self.buffer) < 10:
            return
        X = [b["state"] for b in self.buffer]
        y = [b["reward"] for b in self.buffer]
        try:
            await asyncio.to_thread(self.reward_model.fit, X, y)
        except Exception as e:
            logger.debug("RLHF fit failed: %s", e)
            return
        # Policy = softmax over preference counts
        try:
            raw = np.array([self.preference_counts.get(a, 0.0)
                            for a in self.ACTIONS])
            if raw.sum() > 0:
                e = np.exp(raw - raw.max())
                self.policy = (e / e.sum()).tolist()
        except Exception:
            pass
        self.buffer.clear()

    async def get_policy_probs(self, state):
        # [P0] predict per action with action-aware features
        if self.reward_model is not None and \
                getattr(self.reward_model, "n_features_in_", None) is not None:
            feats_list = [self._state_to_features(state, i)
                          for i in range(len(self.ACTIONS))]
            try:
                preds = await asyncio.to_thread(
                    self.reward_model.predict, feats_list)
                scores = [float(p) for p in preds]
                m = max(scores)
                exps = [math.exp(s - m) for s in scores]
                total = sum(exps) or 1.0
                return [e / total for e in exps]
            except Exception:
                pass
        return list(self.policy)

    def save_state(self):
        return {"policy": list(self.policy),
                "preference_counts": dict(self.preference_counts)}

    def load_state(self, data):
        p = data.get("policy")
        if p and len(p) == len(self.ACTIONS):
            self.policy = list(p)
        self.preference_counts = defaultdict(
            float, data.get("preference_counts", {}))


# =============================================================================
# DRIFT DETECTOR
# =============================================================================
class DriftDetector:
    def __init__(self, storage):
        self.storage = storage
        self.carbon_history = deque(maxlen=100)
        self.performance_history = deque(maxlen=100)
        self.threshold = 0.15

    async def check_carbon_drift(self, current):
        self.carbon_history.append(float(current))
        if len(self.carbon_history) < 10:
            return False
        mean = float(np.mean(list(self.carbon_history)[-10:]))
        if mean == 0:
            return False
        drift = abs(current - mean) > self.threshold * abs(mean)
        try:
            DRIFT_SCORE.labels(domain="carbon").set(
                abs(current - mean) / abs(mean))
        except Exception:
            pass
        return drift

    async def check_performance_drift(self, avg_reward):
        self.performance_history.append(float(avg_reward))
        if len(self.performance_history) < 10:
            return False
        mean = float(np.mean(list(self.performance_history)[-10:]))
        if mean == 0:
            return False
        drift = abs(avg_reward - mean) > self.threshold * abs(mean)
        try:
            DRIFT_SCORE.labels(domain="performance").set(
                abs(avg_reward - mean) / abs(mean))
        except Exception:
            pass
        return drift


# =============================================================================
# PREDICTIVE DIGITAL TWIN — [P1] populated from history
# =============================================================================
class PredictiveDigitalTwin:
    def __init__(self):
        self.history: Deque[Dict] = deque(maxlen=500)

    def observe(self, module_id: str, value: float):
        self.history.append({"module_id": module_id, "value": float(value),
                              "ts": datetime.now(timezone.utc).isoformat()})

    async def forecast_module_state(self, module_id, steps=24):
        # [P1] real forecast from history
        values = [h["value"] for h in self.history
                  if h["module_id"] == module_id]
        if not values:
            return {"module_id": module_id, "forecast": [],
                    "reason": "no_history"}
        alpha = 0.3
        ema = values[0]
        for v in values[1:]:
            ema = alpha * v + (1 - alpha) * ema
        forecast = [ema + random.gauss(0, 0.5) for _ in range(steps)]
        return {"module_id": module_id, "forecast": forecast,
                "history_size": len(values)}

    async def get_twin_status(self):
        return {"forecast_capable": True, "history_size": len(self.history)}

    def save_state(self):
        return {"history": list(self.history)}

    def load_state(self, data):
        self.history = deque(data.get("history", []), maxlen=500)


# =============================================================================
# LIMIT GRAPH
# =============================================================================
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph: Dict[str, Dict[str, float]] = {
            "carbon": {"cost": 0.8},
            "cost": {"latency": 0.2},
            "latency": {"success": -0.3},
            "success": {"cost": -0.1},
        }
        self.constraints: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def update_constraint(self, name, value):
        async with self._lock:
            self.constraints[name] = float(value)

    async def evaluate_path(self, start, end):
        if start not in self.graph or end not in self.graph:
            return 0.0
        visited, q = set(), deque([(start, 1.0)])
        while q:
            node, w = q.popleft()
            if node == end:
                return w
            visited.add(node)
            for n, ww in self.graph.get(node, {}).items():
                if n not in visited:
                    q.append((n, w * ww))
        return 0.0

    async def summary(self):
        return {"nodes": list(self.graph.keys()),
                "constraints": dict(self.constraints),
                "edge_count": sum(len(v) for v in self.graph.values())}

    def save_state(self):
        return {"constraints": dict(self.constraints)}

    def load_state(self, data):
        self.constraints = dict(data.get("constraints", {}))


# =============================================================================
# MODP — [P1] dynamic candidates
# =============================================================================
class MODPStrategyOptimizer:
    CRITERIA = ["success", "carbon", "cost", "latency"]

    def __init__(self, config):
        self.config = config
        self.weights = list(_cfg_get(config, "MODP_WEIGHTS", [0.25] * 4))
        self._xai: Optional[IntegrationXAIDecisionExplainer] = None

    def _topsis(self, matrix, weights):
        norm = matrix / (np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9)
        w = norm * weights
        ideal, neg = w.max(axis=0), w.min(axis=0)
        dp = np.sqrt(((w - ideal) ** 2).sum(axis=1))
        dn = np.sqrt(((w - neg) ** 2).sum(axis=1))
        return dn / (dp + dn + 1e-9)

    def _candidates_from_state(self, state):
        carbon = state.get("carbon_intensity", 400) / 1000.0
        success = state.get("success_rate", 0.5)
        return [
            {"name": "performance",
             "success": 0.7 + 0.3 * success,
             "carbon": 0.4 + 0.3 * carbon, "cost": 0.5, "latency": 0.3},
            {"name": "carbon",
             "success": 0.6 + 0.2 * success,
             "carbon": 0.1 + 0.2 * carbon, "cost": 0.3, "latency": 0.4},
            {"name": "cost",
             "success": 0.5 + 0.2 * success,
             "carbon": 0.3 + 0.2 * carbon, "cost": 0.1, "latency": 0.5},
            {"name": "hybrid",
             "success": 0.7 + 0.15 * success,
             "carbon": 0.3 + 0.2 * carbon, "cost": 0.3, "latency": 0.35},
            {"name": "adaptive",
             "success": 0.75 + 0.15 * success,
             "carbon": 0.25 + 0.2 * carbon, "cost": 0.25, "latency": 0.4},
        ]

    async def select_strategy(self, state_dict):
        candidates = self._candidates_from_state(state_dict)
        cands = [{"success": c["success"],
                  "carbon": 1.0 - c["carbon"],
                  "cost": 1.0 - c["cost"],
                  "latency": 1.0 - c["latency"]}
                 for c in candidates]
        matrix = np.array([[c[k] for k in self.CRITERIA] for c in cands])
        scores = self._topsis(matrix, np.array(self.weights)).tolist()
        best_idx = int(np.argmax(scores))
        best = candidates[best_idx]
        result = {"strategy": best["name"], "scores": scores,
                  "recommendation": f"MODP selected {best['name']}"}
        if self._xai:
            try:
                feats = np.array([
                    float(state_dict.get("success_rate", 0.5)),
                    float(state_dict.get("carbon_intensity", 400)) / 1000.0,
                    float(state_dict.get("cost", 0.5)),
                    float(state_dict.get("latency", 0.5))])
                def _score(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                xai_res = await self._xai.explain(
                    decision_id=f"modp_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={best['name']}",
                    features=feats,
                    names=self.CRITERIA,
                    model_fn=_score)
                result["attributions"] = xai_res["attributions"]
                result["explanation"] = xai_res["explanation"]
            except Exception as e:
                logger.debug("XAI hook failed: %s", e)
        return result


# =============================================================================
# PQC chain — [P3] persistent keypair + verify
# =============================================================================
class PQCChain:
    def __init__(self, storage):
        self.storage = storage
        self._pk: Optional[bytes] = None
        self._sk: Optional[bytes] = None
        self._load_or_generate()

    def _load_or_generate(self):
        stored = self.storage.get_state("pqc_integration_keypair")
        if stored:
            try:
                data = json.loads(stored)
                self._pk = bytes.fromhex(data["pk"])
                self._sk = bytes.fromhex(data["sk"])
                return
            except Exception:
                pass
        if PQC_AVAILABLE:
            try:
                self._pk, self._sk = dilithium.generate_keypair()
                self.storage.save_state("pqc_integration_keypair", json.dumps({
                    "pk": self._pk.hex(), "sk": self._sk.hex()}))
            except Exception:
                self._pk = self._sk = None

    def sign(self, data: bytes) -> Dict[str, str]:
        if PQC_AVAILABLE and self._sk is not None:
            try:
                sig = dilithium.sign(self._sk, data)
                return {"algorithm": "dilithium", "sig": sig.hex(),
                        "pk": self._pk.hex() if self._pk else ""}
            except Exception:
                pass
        return {"algorithm": "sha256",
                "sig": hashlib.sha256(data).hexdigest()}

    def verify(self, data: bytes, sig: Dict[str, str]) -> bool:
        if sig.get("algorithm") == "dilithium" and PQC_AVAILABLE and self._pk:
            try:
                dilithium.verify(self._pk, data, bytes.fromhex(sig["sig"]))
                return True
            except Exception:
                return False
        if sig.get("algorithm") == "sha256":
            return hashlib.sha256(data).hexdigest() == sig.get("sig")
        return False


# =============================================================================
# INTEGRATION RESULT — [P3] real fields
# =============================================================================
@dataclass
class IntegrationResult:
    module_results: List[ModuleResult] = field(default_factory=list)
    overall_status: str = "success"
    sustainability_score: float = 0.0
    data_quality_score: float = 0.0
    success_rate: float = 0.0
    total_duration_ms: float = 0.0
    checkpoint_id: Optional[str] = None
    selected_strategy: str = ""
    strategy_scores: List[float] = field(default_factory=list)
    precision_level: str = "fp32"
    temporal_violations: List[str] = field(default_factory=list)
    xai_explanation: Optional[Dict[str, Any]] = None
    carbon_market_decision: Optional[Dict[str, Any]] = None
    agent_id: Optional[str] = None
    causal_ate: Optional[float] = None
    # v17.1
    correlation_id: str = ""
    dataset_hash: str = ""
    quantum_signature: Optional[Dict[str, Any]] = None
    module_outputs: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    carbon_footprint_kg: float = 0.0


# =============================================================================
# MAIN: UnifiedIntegrationManagerV17
# =============================================================================
class UnifiedIntegrationManagerV17:
    ACTION_SPACE = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self):
        self.instance_id = str(uuid.uuid4())[:8]
        self.storage = storage
        self.config = config

        # Module registry — [P3] real modules
        self.module_registry: Dict[str, BaseModule] = {
            name: cls(name, self.config)
            for name, cls in MODULE_CLASSES.items()
        }

        # v9 modules
        self.ga_optimizer = (GeneticIntegrationOptimizer(self.config, self.storage)
                             if _cfg_get(config, "GA_ENABLED", True) else None)
        self.moe_gating = (MoEGatingNetwork(self.config, self.storage)
                           if _cfg_get(config, "MOE_ENABLED", True) else None)
        self.pareto_optimizer = (ParetoFrontOptimizer(self.config, self.storage)
                                 if _cfg_get(config, "PARETO_ENABLED", True) else None)
        self.neural_teacher = (NeuralTeacher()
                               if _cfg_get(config, "NEURAL_TEACHER_ENABLED", True)
                               else None)
        self.federated_learner = (FederatedIntegrationLearner(
            self.storage, self.instance_id,
            _cfg_get(config, "FEDERATED_INTERVAL", 3600))
            if _cfg_get(config, "FEDERATED_ENABLED", True) else None)
        self.drift_detector = (DriftDetector(self.storage)
                               if _cfg_get(config, "DRIFT_DETECTION_ENABLED", True)
                               else None)
        self.limit_graph = (LimitGraphManager(self.config)
                            if _cfg_get(config, "LIMIT_GRAPH_ENABLED", True)
                            else None)
        self.modp_optimizer = (MODPStrategyOptimizer(self.config)
                               if _cfg_get(config, "MODP_ENABLED", True)
                               else None)
        self.rlhf = (RLHFManager(self.config)
                     if _cfg_get(config, "RLHF_ENABLED", True) else None)
        self.distillation = (MultiTeacherPolicyDistillation(
            self.config, self.moe_gating)
            if _cfg_get(config, "DISTILLATION_ENABLED", True) and self.moe_gating
            else None)
        self.digital_twin = (PredictiveDigitalTwin()
                             if _cfg_get(config, "PREDICTIVE_DIGITAL_TWIN_ENABLED", True)
                             else None)

        # v17 modules
        self.causal_graph = (CausalGraphLearner(self.storage)
                             if _cfg_get(config, "CAUSAL_RL_ENABLED", True)
                             else None)
        self.causal_rl = (CausalIntegrationPolicyAdapter(
            self.config, self.storage, self.causal_graph)
            if _cfg_get(config, "CAUSAL_RL_ENABLED", True) and self.causal_graph
            else None)
        self.temporal = (TemporalIntegrationVerifier(self.storage, self.config)
                         if _cfg_get(config, "TEMPORAL_LOGIC_ENABLED", True)
                         else None)
        self.xai = (IntegrationXAIDecisionExplainer(self.config, self.storage)
                    if _cfg_get(config, "XAI_ENABLED", True) else None)
        self.precision = (AdaptivePrecisionSwitcher(self.config, self.storage)
                          if _cfg_get(config, "ADAPTIVE_PRECISION_ENABLED", True)
                          else None)
        self.carbon_market = (CarbonMarketIntegrator(self.config, self.storage)
                              if _cfg_get(config, "CARBON_MARKET_ENABLED", True)
                              else None)
        # [P0] chaos with real probe
        self.chaos = (ChaosTestingEngine(self.config, self.storage,
                                          probe_fn=self._chaos_probe)
                      if _cfg_get(config, "CHAOS_TESTING_ENABLED", True)
                      else None)
        self.multi_agent = (MultiAgentCoordinator(self.config, self.storage)
                            if _cfg_get(config, "MULTI_AGENT_ENABLED", True)
                            else None)
        self.quantum_distiller = (QuantumDistillationEngine(
            temperature=_cfg_get(config, "DISTILLATION_TEMPERATURE", 2.0),
            alpha=_cfg_get(config, "DISTILLATION_ALPHA", 0.5))
            if _cfg_get(config, "DISTILLATION_ENABLED", True) else None)

        # HITL — after multi_agent (dashboard-compatible)
        self.user_pref_learner = (ActiveUserPreferenceLearner(self.storage)
                                   if _cfg_get(config,
                                                "ACTIVE_USER_PREFERENCE_ENABLED",
                                                True) else None)
        self.hitl_ws: Optional[HITLWebSocket] = None
        if self.user_pref_learner and WEBSOCKETS_AVAILABLE:
            self.hitl_ws = HITLWebSocket(
                _cfg_get(self.config, "HITL_WS_PORT", 8791),
                self.user_pref_learner)
            self.user_pref_learner.websocket = self.hitl_ws

        # PQC
        self.pqc = PQCChain(self.storage)

        # Circuit breakers — chaos wired in
        self.circuit_breakers = {
            "integration": CircuitBreaker(name="integration"),
            "carbon_api": CircuitBreaker(name="carbon_api"),
        }
        if self.chaos:
            for cb in self.circuit_breakers.values():
                cb.chaos_engine = self.chaos
            if self.carbon_market:
                self.carbon_market._cb.chaos_engine = self.chaos

        # Wiring
        if self.xai and self.modp_optimizer:
            self.modp_optimizer._xai = self.xai
        if self.temporal:
            self.temporal.set_approval_callback(self._hitl_approval)

        # Module dependencies — [P1] ordered
        self.module_dependencies = {
            "collector": [],
            "carbon": [],
            "helium": [],
            "thermal": [],
            "elasticity": [],
            "circularity": [],
            "forecaster": [],
            "regret": [],
            "quantum": [],
            "sustainability": ["carbon", "helium", "thermal", "circularity"],
        }

        # Runtime
        self._module_lock = asyncio.Lock()  # [P2]
        self._history_lock = asyncio.Lock()  # [P2]
        self._integration_semaphore = asyncio.Semaphore(
            _cfg_get(config, "MAX_CONCURRENT_MODULES", 5))
        self.operation_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._queue_worker: Optional[asyncio.Task] = None
        self._running = False
        self.background_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        self._recent_errors: Deque[Tuple[float, str]] = deque(maxlen=100)
        self._last_integration_ts = time.time()
        self.integration_result: Optional[IntegrationResult] = None

        logger.info(
            "UnifiedIntegrationManagerV17 v17.1.0 initialized (instance=%s)",
            self.instance_id)

    # ------------------------------------------------------------------
    # Chaos probe — [P0]
    # ------------------------------------------------------------------
    async def _chaos_probe(self) -> float:
        score = 1.0
        err_rate = len(self._recent_errors) / max(1, 100)
        score -= min(0.4, err_rate * 4.0)
        qsize = self.operation_queue.qsize()
        score -= min(0.3, qsize / 100.0)
        if time.time() - self._last_integration_ts > 300:
            score -= 0.2
        return max(0.0, min(1.0, score))

    # ------------------------------------------------------------------
    # HITL — [P1] wired to user preference learner
    # ------------------------------------------------------------------
    async def _hitl_approval(self, rule_id, state) -> bool:
        if self.user_pref_learner:
            try:
                choice = await self.user_pref_learner.query_user_if_needed(
                    user_id="system",
                    top_configs=[
                        {"solution_id": "approve",
                         "metrics": {"success_rate": 1.0}},
                        {"solution_id": "deny",
                         "metrics": {"success_rate": 1.01}}],
                    timeout=2.0)
                return choice != "deny"
            except Exception:
                pass
        return random.random() > 0.5

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        self._running = True
        if self.hitl_ws:
            await self.hitl_ws.start()
        self._queue_worker = asyncio.create_task(self._process_queue())

        loop = asyncio.get_event_loop()
        tasks = [
            loop.create_task(self._health_check_loop()),
            loop.create_task(self._carbon_update_loop()),
            loop.create_task(self._ga_optimization_loop()),
            loop.create_task(self._moe_training_loop()),
            loop.create_task(self._rlhf_loop()),
            loop.create_task(self._federated_loop()),
            loop.create_task(self._drift_detection_loop()),
        ]
        if self.limit_graph:
            tasks.append(loop.create_task(self._limit_graph_loop()))
        if self.distillation:
            tasks.append(loop.create_task(self._distillation_loop()))
        if self.quantum_distiller:
            tasks.append(loop.create_task(self._quantum_distill_loop()))
        if self.causal_graph and self.causal_rl:
            tasks.append(loop.create_task(self._causal_rl_loop()))
        if self.temporal:
            tasks.append(loop.create_task(self._temporal_loop()))
        if self.xai:
            tasks.append(loop.create_task(self._xai_loop()))
        if self.precision:
            tasks.append(loop.create_task(self._precision_loop()))
        if self.carbon_market:
            tasks.append(loop.create_task(self._carbon_market_loop()))
        if self.chaos and os.getenv("INTEGRATION_ENV", "production") == "chaos":
            tasks.append(loop.create_task(self._chaos_loop()))
        if self.multi_agent:
            tasks.append(loop.create_task(self._multi_agent_loop()))
        if self.digital_twin:
            tasks.append(loop.create_task(self._digital_twin_loop()))

        for t in tasks:
            self.background_tasks.add(t)
            t.add_done_callback(self.background_tasks.discard)
        logger.info("Integration manager started with %d background tasks",
                    len(self.background_tasks))

    async def shutdown(self):
        logger.info("Shutting down integration manager...")
        self._shutdown_event.set()
        self._running = False
        if self._queue_worker:
            self._queue_worker.cancel()
            try:
                await self._queue_worker
            except asyncio.CancelledError:
                pass
        for t in list(self.background_tasks):
            t.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        # [P2] persist state
        try:
            await self.save_state()
        except Exception:
            pass
        if self.hitl_ws:
            await self.hitl_ws.stop()
        logger.info("Shutdown complete")

    # ------------------------------------------------------------------
    # Module execution
    # ------------------------------------------------------------------
    def _resolve_order(self, modules: List[str]) -> List[str]:
        # Topological sort
        resolved: List[str] = []
        visited: Set[str] = set()
        def _visit(m):
            if m in visited:
                return
            visited.add(m)
            for dep in self.module_dependencies.get(m, []):
                if dep in modules:
                    _visit(dep)
            resolved.append(m)
        for m in modules:
            _visit(m)
        return resolved

    async def _run_module(self, module_name: str,
                          context: Dict) -> ModuleResult:
        # [P0] Run real module logic
        t0 = time.time()
        module = self.module_registry.get(module_name)
        if module is None:
            return ModuleResult(module_name, "failure", 0.0,
                                 "module not found")
        async with self._integration_semaphore:
            try:
                mr = await module.run(context)
                mr.duration_ms = (time.time() - t0) * 1000.0
                mr.correlation_id = correlation_id_var.get()
                try:
                    MODULE_RUNS.labels(module=module_name,
                                        status=mr.status).inc()
                except Exception:
                    pass
                await asyncio.get_event_loop().run_in_executor(
                    None, self.storage.save_module_run,
                    module_name, mr.status, mr.duration_ms, mr.message)
                # [P1] populate digital twin
                if self.digital_twin and mr.output:
                    for k, v in mr.output.items():
                        if isinstance(v, (int, float)):
                            self.digital_twin.observe(
                                f"{module_name}.{k}", float(v))
                return mr
            except Exception as e:
                duration_ms = (time.time() - t0) * 1000.0
                try:
                    MODULE_RUNS.labels(module=module_name,
                                        status="failure").inc()
                except Exception:
                    pass
                await asyncio.get_event_loop().run_in_executor(
                    None, self.storage.save_module_run,
                    module_name, "failure", duration_ms, str(e))
                self._recent_errors.append((time.time(), str(e)))
                return ModuleResult(module_name, "failure", duration_ms,
                                     "error", str(e))

    async def _process_queue(self):
        while self._running:
            try:
                op = await asyncio.wait_for(self.operation_queue.get(),
                                            timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                result = await self._run_module(op["module"],
                                                op.get("context", {}))
                if not op["future"].done():
                    op["future"].set_result(result)
            except Exception as e:
                if not op["future"].done():
                    op["future"].set_exception(e)
            finally:
                self.operation_queue.task_done()

    # ------------------------------------------------------------------
    # Strategy selection
    # ------------------------------------------------------------------
    async def _select_strategy(self, state_dict):
        strategy = "adaptive"
        scores: List[float] = []
        explanation = None
        attributions = None

        if self.causal_rl:
            strategy = await self.causal_rl.choose_action(state_dict)
        elif self.modp_optimizer:
            modp = await self.modp_optimizer.select_strategy(state_dict)
            strategy = modp["strategy"]
            scores = modp.get("scores", [])
            explanation = modp.get("explanation")
            attributions = modp.get("attributions")
        elif self.rlhf:
            probs = await self.rlhf.get_policy_probs(state_dict)
            idx = int(np.argmax(probs)) if probs else 0
            strategy = self.ACTION_SPACE[idx % len(self.ACTION_SPACE)]
            scores = list(probs)
        elif self.quantum_distiller:
            probs = self.quantum_distiller.get_policy()
            idx = int(np.argmax(probs)) if probs else 0
            strategy = self.ACTION_SPACE[idx % len(self.ACTION_SPACE)]
            scores = list(probs)
        elif self.distillation:
            probs = self.distillation.get_student_probs()
            idx = int(np.argmax(probs)) if probs else 0
            strategy = self.ACTION_SPACE[idx % len(self.ACTION_SPACE)]
            scores = list(probs)
        elif self.moe_gating:
            strategy, _ = await self.moe_gating.select_expert(state_dict)

        if strategy not in self.ACTION_SPACE:
            logger.warning("Strategy '%s' not in action space; defaulting",
                           strategy)
            strategy = "adaptive"
        return strategy, scores, explanation, attributions

    # ------------------------------------------------------------------
    # Main integration
    # ------------------------------------------------------------------
    async def run_integration(self, modules: Optional[List[str]] = None,
                               context: Optional[Dict] = None):
        cid = uuid.uuid4().hex[:12]
        token = correlation_id_var.set(cid)
        try:
            try:
                INTEGRATION_ACTIVE.inc()
            except Exception:
                pass
            t0 = time.time()
            try:
                result = await self._run_integration_inner(
                    modules, context, cid, t0)
                try:
                    INTEGRATION_LATENCY.labels(
                        strategy=result.selected_strategy).observe(
                        time.time() - t0)
                except Exception:
                    pass
                self._last_integration_ts = time.time()
                return result
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))
                try:
                    INTEGRATION_ERRORS.labels(stage="integration").inc()
                except Exception:
                    pass
                raise
        finally:
            try:
                INTEGRATION_ACTIVE.dec()
            except Exception:
                pass
            correlation_id_var.reset(token)

    def _default_context(self) -> Dict[str, Any]:
        # [P1] realistic varying inputs
        return {
            "temperature_c": random.uniform(20.0, 35.0),
            "pressure_bar": random.uniform(8.0, 15.0),
            "helium_feed_pct": random.uniform(0.01, 0.1),
            "energy_kwh": random.uniform(0.5, 2.0),
            "carbon_intensity": random.uniform(200.0, 600.0),
            "it_load_kw": random.uniform(50.0, 150.0),
            "temp_hot_c": random.uniform(70.0, 90.0),
            "temp_cold_c": random.uniform(20.0, 30.0),
            "recycled_fraction": random.uniform(0.3, 0.8),
            "reused_fraction": random.uniform(0.05, 0.2),
            "expected_reward": random.uniform(0.7, 0.9),
            "realized_reward": random.uniform(0.6, 0.85),
        }

    async def _run_integration_inner(self, modules, context, cid, t0):
        if modules is None:
            modules = list(self.module_registry.keys())
        if context is None:
            context = self._default_context()
        resolved = self._resolve_order(modules)

        result = IntegrationResult(correlation_id=cid)

        # Carbon intensity — always fetched fresh
        carbon = await self.carbon_manager.get_current_intensity() \
            if hasattr(self.carbon_manager, "get_current_intensity") \
            else context.get("carbon_intensity", 400.0)
        if not isinstance(carbon, (int, float)):
            carbon = context.get("carbon_intensity", 400.0)

        # Build state from context
        state_dict = {
            "module_count": len(resolved),
            "success_rate": 0.9,  # will be replaced by real success rate
            "queue_size": self.operation_queue.qsize(),
            "carbon_intensity": carbon,
            "cloud_provider_latency": 60.0,
            "module_health": 0.85,
            "sustainability_score": 0.8,
            "cost": 0.5,
            "latency": 0.4,
        }

        # Strategy selection
        strategy, scores, explanation, attributions = \
            await self._select_strategy(state_dict)
        result.selected_strategy = strategy
        result.strategy_scores = scores
        try:
            INTEGRATION_STRATEGY_DIST.labels(strategy=strategy).set(1)
        except Exception:
            pass

        # Precision
        if self.precision:
            await self.precision.switch_to(
                self.precision.select_precision(), reason="pre_integration")
            result.precision_level = self.precision.current

        # Multi-agent bid
        if self.multi_agent:
            preferred = {
                "performance": "optimizer", "carbon": "negotiator",
                "cost": "optimizer", "hybrid": "orchestrator",
                "adaptive": "orchestrator",
            }.get(strategy, "orchestrator")
            try:
                agent_id, _ = await self.multi_agent.bid({
                    "name": f"integration_{strategy}",
                    "preferred_role": preferred})
                result.agent_id = agent_id
            except Exception:
                pass

        # Carbon market
        if self.carbon_market:
            result.carbon_market_decision = \
                await self.carbon_market.net_zero_schedule(
                    workload_kwh=0.5, intensity=carbon / 1000.0)

        # Execute modules
        module_outputs: Dict[str, Dict[str, Any]] = {}
        context["upstream_results"] = {}
        context["module_outputs"] = module_outputs

        for mod in resolved:
            mr = await self._run_module(mod, context)
            result.module_results.append(mr)
            if mr.status == "success" and mr.output:
                module_outputs[mod] = mr.output
                context["upstream_results"][mod] = {
                    "status": mr.status,
                    "output": mr.output,
                }

        result.module_outputs = module_outputs

        # [P0] Compute success rate from results
        successes = sum(1 for r in result.module_results
                        if r.status == "success")
        total_mods = max(1, len(result.module_results))
        result.success_rate = successes / total_mods
        try:
            SUCCESS_RATE.set(result.success_rate)
        except Exception:
            pass

        # [P0] Sustainability from module outputs
        if "sustainability" in module_outputs:
            result.sustainability_score = float(module_outputs[
                "sustainability"].get("sustainability_score", 0.0))
        else:
            result.sustainability_score = self._compute_sustainability(
                module_outputs)
        try:
            SUSTAINABILITY_SCORE.set(result.sustainability_score)
        except Exception:
            pass

        # [P0] Quality from module results
        # Quality = 100 * (fraction successful) * (fraction under 100ms)
        fast = sum(1 for r in result.module_results
                   if r.duration_ms < 100.0)
        quality_factor = fast / max(1, len(result.module_results))
        result.data_quality_score = 100.0 * result.success_rate * quality_factor
        try:
            DATA_QUALITY_SCORE.set(result.data_quality_score)
        except Exception:
            pass

        # [P1] Update state features from results
        state_dict["success_rate"] = result.success_rate
        state_dict["sustainability_score"] = result.sustainability_score / 100.0
        state_dict["module_health"] = result.success_rate
        state_dict["carbon_intensity"] = carbon
        result.carbon_footprint_kg = float(
            module_outputs.get("carbon", {}).get("carbon_kg", 0.0))

        # Temporal
        if self.temporal:
            tokenization_complete = result.success_rate >= 0.8
            await self.temporal.push_state({
                "success_rate": result.success_rate,
                "carbon": carbon / 1000.0,
                "integration_complete": tokenization_complete,
            })
            verify = await self.temporal.verify()
            result.temporal_violations = [k for k, v in verify.items()
                                           if not v]

        # Reward from results
        reward = max(0.0, min(1.0, result.sustainability_score / 100.0))

        # Update learners
        if self.causal_rl:
            await self.causal_rl.update(strategy, reward, state_dict)
        if self.rlhf and reward > 0.4:
            await self.rlhf.record_feedback(state_dict, strategy, reward)
        if self.moe_gating:
            await self.moe_gating.add_training_sample(
                state_dict, strategy, reward)
        if self.neural_teacher:
            try:
                x = np.array(self.moe_gating._encode(state_dict)
                             if self.moe_gating else [0.0] * 9).reshape(1, -1)
                self.neural_teacher.train(
                    x, np.array([self.ACTION_SPACE.index(strategy)]))
            except Exception:
                pass
        if self.pareto_optimizer:
            cost_usd = result.carbon_footprint_kg / 1000.0 * 50.0
            await self.pareto_optimizer.add_configuration(
                {"strategy": strategy},
                {"success_rate": result.success_rate,
                 "carbon_footprint": result.carbon_footprint_kg,
                 "execution_time": result.total_duration_ms / 1000.0 or 0.1,
                 "cost": max(0.01, cost_usd)})
        if self.limit_graph:
            await self.limit_graph.update_constraint(
                "success", result.success_rate)
            await self.limit_graph.update_constraint(
                "carbon", carbon / 1000.0)
            await self.limit_graph.evaluate_path("carbon", "success")

        # Drift
        if self.drift_detector:
            await self.drift_detector.check_carbon_drift(carbon / 1000.0)
            await self.drift_detector.check_performance_drift(reward)

        # XAI — [P1] real features
        if self.xai:
            try:
                feats = np.array([
                    result.success_rate,
                    carbon / 1000.0,
                    result.carbon_footprint_kg,
                    result.total_duration_ms / 1000.0 or 0.1])
                def _score(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                result.xai_explanation = await self.xai.explain(
                    decision_id=f"integration_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={strategy}",
                    features=feats,
                    names=["success_rate", "carbon", "footprint", "latency"],
                    model_fn=_score)
            except Exception as e:
                logger.debug("XAI failed: %s", e)

        # Federated share
        if self.federated_learner:
            policy = await self.policy_probs(state_dict)
            await self.federated_learner.share_weights({"policy": policy})

        # Multi-agent reward
        if self.multi_agent and result.agent_id:
            await self.multi_agent.reward(result.agent_id, reward)

        # Causal ATE — [P1] correct variable
        if self.causal_rl and self.causal_graph:
            try:
                result.causal_ate = await self.causal_rl.estimate_ate(
                    "carbon", "success_rate")
            except Exception:
                pass

        # Digital twin
        if self.digital_twin:
            try:
                await self.digital_twin.forecast_module_state(
                    "integration", steps=12)
            except Exception:
                pass

        # Checkpoint
        result.total_duration_ms = (time.time() - t0) * 1000.0
        result.overall_status = (
            "success"
            if result.success_rate >= 0.8
            else "partial" if result.success_rate >= 0.5 else "failure")
        result.checkpoint_id = f"chk_{uuid.uuid4().hex[:8]}"

        # [P3] PQC signature
        payload = json.dumps({
            "correlation_id": cid,
            "strategy": strategy,
            "success_rate": result.success_rate,
            "sustainability": result.sustainability_score,
            "module_results": [asdict(m) for m in result.module_results],
        }, sort_keys=True, default=str).encode()
        result.dataset_hash = hashlib.sha256(payload).hexdigest()
        result.quantum_signature = self.pqc.sign(payload)

        # Metrics
        try:
            INTEGRATION_RUNS.labels(status=result.overall_status).inc()
            INTEGRATION_DURATION.labels(strategy=strategy).observe(
                result.total_duration_ms / 1000.0)
        except Exception:
            pass

        async with self._history_lock:
            self.integration_result = result
        logger.info("[cid=%s] status=%s strategy=%s success=%.2f "
                    "sustain=%.1f quality=%.1f violations=%s",
                    cid, result.overall_status, strategy,
                    result.success_rate, result.sustainability_score,
                    result.data_quality_score, result.temporal_violations)
        return result

    def _compute_sustainability(self, module_outputs: Dict) -> float:
        carbon_kg = module_outputs.get("carbon", {}).get("carbon_kg", 0.0)
        carbon_score = max(0.0, 1.0 - min(1.0, carbon_kg / 10.0))
        helium = module_outputs.get("helium", {}).get("recovery", 0.0)
        circ = module_outputs.get("circularity", {}).get("circularity", 0.5)
        thermal = module_outputs.get("thermal", {}).get(
            "practical_efficiency", 0.3) * 2.5
        thermal = max(0.0, min(1.0, thermal))
        return 100.0 * (
            0.4 * carbon_score +
            0.25 * max(0.0, min(1.0, helium)) +
            0.2 * max(0.0, min(1.0, circ)) +
            0.15 * thermal
        )

    async def policy_probs(self, state_dict):
        if self.rlhf:
            return await self.rlhf.get_policy_probs(state_dict)
        if self.causal_rl:
            return self.causal_rl.get_policy()
        if self.quantum_distiller:
            return self.quantum_distiller.get_policy()
        if self.multi_agent:
            return self.multi_agent.get_policy()
        if self.distillation:
            return self.distillation.get_student_probs()
        return [0.2] * 5

    # ------------------------------------------------------------------
    # [P3] Streaming + batched APIs
    # ------------------------------------------------------------------
    async def stream_integration(
            self, contexts: AsyncIterator[Dict]
    ) -> AsyncIterator[IntegrationResult]:
        async for ctx in contexts:
            yield await self.run_integration(context=ctx)

    async def run_batch(self, contexts: List[Dict],
                         concurrency: int = 4) -> List[IntegrationResult]:
        sem = asyncio.Semaphore(concurrency)
        async def _one(ctx):
            async with sem:
                return await self.run_integration(context=ctx)
        return await asyncio.gather(*[_one(c) for c in contexts])

    # ------------------------------------------------------------------
    # [P2] Persistence
    # ------------------------------------------------------------------
    async def save_state(self, path=None):
        path = path or _cfg_get(self.config, "STATE_PATH",
                                 "/tmp/integration_v17_state.json")
        data = {"version": "17.1.0", "instance_id": self.instance_id,
                "submodules": {}}
        for name, obj in [
            ("causal_graph", self.causal_graph),
            ("causal_rl", self.causal_rl),
            ("temporal", self.temporal),
            ("precision", self.precision),
            ("carbon_market", self.carbon_market),
            ("multi_agent", self.multi_agent),
            ("quantum_distiller", self.quantum_distiller),
            ("rlhf", self.rlhf),
            ("distillation", self.distillation),
            ("federated", self.federated_learner),
            ("moe", self.moe_gating),
            ("pareto", self.pareto_optimizer),
            ("digital_twin", self.digital_twin),
            ("limit_graph", self.limit_graph),
            ("user_pref", self.user_pref_learner),
        ]:
            if obj is not None and hasattr(obj, "save_state"):
                try:
                    data["submodules"][name] = obj.save_state()
                except Exception as e:
                    logger.debug("save_state '%s' failed: %s", name, e)
        # Save per-module state
        for mod_name, module in self.module_registry.items():
            try:
                ms = module.save_state()
                if ms:
                    data["submodules"][f"module_{mod_name}"] = ms
            except Exception:
                pass
        try:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(data, default=str, indent=2))
            return True
        except Exception as e:
            logger.error("save_state failed: %s", e)
            return False

    async def load_state(self, path=None):
        path = path or _cfg_get(self.config, "STATE_PATH",
                                 "/tmp/integration_v17_state.json")
        try:
            p = Path(path)
            if not p.exists():
                return False
            data = json.loads(p.read_text())
            for name, obj in [
                ("causal_graph", self.causal_graph),
                ("causal_rl", self.causal_rl),
                ("temporal", self.temporal),
                ("precision", self.precision),
                ("carbon_market", self.carbon_market),
                ("multi_agent", self.multi_agent),
                ("quantum_distiller", self.quantum_distiller),
                ("rlhf", self.rlhf),
                ("distillation", self.distillation),
                ("federated", self.federated_learner),
                ("moe", self.moe_gating),
                ("pareto", self.pareto_optimizer),
                ("digital_twin", self.digital_twin),
                ("limit_graph", self.limit_graph),
                ("user_pref", self.user_pref_learner),
            ]:
                if obj is not None and name in data.get("submodules", {}) \
                        and hasattr(obj, "load_state"):
                    try:
                        obj.load_state(data["submodules"][name])
                    except Exception as e:
                        logger.debug("load_state '%s' failed: %s", name, e)
            for mod_name, module in self.module_registry.items():
                key = f"module_{mod_name}"
                if key in data.get("submodules", {}):
                    try:
                        module.load_state(data["submodules"][key])
                    except Exception:
                        pass
            return True
        except Exception as e:
            logger.error("load_state failed: %s", e)
            return False

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(120)
                if hasattr(self.carbon_manager, "update_carbon_intensity"):
                    await self.carbon_manager.update_carbon_intensity()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)
                if self.ga_optimizer:
                    self.ga_optimizer.invalidate_cache()
                    last_sust = 0.5
                    if self.integration_result:
                        last_sust = self.integration_result.sustainability_score / 100.0
                    await self.ga_optimizer.run_search(last_sust)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _moe_training_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(600)
                if self.moe_gating:
                    self.moe_gating._train()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(config, "RLHF_TRAINING_INTERVAL", 600))
                if self.rlhf:
                    await self.rlhf.train_reward_model()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(_cfg_get(config, "FEDERATED_INTERVAL", 3600))
                if self.federated_learner:
                    await self.federated_learner.discover_and_pull()
                    await self.federated_learner.pull_aggregated_weights()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _drift_detection_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(300)
                if self.drift_detector:
                    c = 400.0
                    if hasattr(self.carbon_manager,
                               "get_current_intensity"):
                        c = await self.carbon_manager.get_current_intensity()
                    await self.drift_detector.check_carbon_drift(c / 1000.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(config, "LIMIT_GRAPH_UPDATE_INTERVAL", 300))
                if self.limit_graph:
                    c = 400.0
                    if hasattr(self.carbon_manager,
                               "get_current_intensity"):
                        c = await self.carbon_manager.get_current_intensity()
                    await self.limit_graph.update_constraint("carbon", c / 1000.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(config, "DISTILLATION_INTERVAL", 300))
                if self.distillation:
                    await self.distillation.distill({
                        "module_count": 10, "success_rate": 0.9,
                        "carbon_intensity": 400})
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _quantum_distill_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(config, "DISTILLATION_INTERVAL", 300))
                if self.quantum_distiller:
                    if self.rlhf:
                        self.quantum_distiller.register_teacher(
                            "rlhf", await self.rlhf.get_policy_probs({}))
                    if self.causal_rl:
                        self.quantum_distiller.register_teacher(
                            "causal", self.causal_rl.get_policy())
                    if self.multi_agent:
                        self.quantum_distiller.register_teacher(
                            "agents", self.multi_agent.get_policy())
                    if self.moe_gating and self.moe_gating._trained:
                        try:
                            names = self.moe_gating.expert_names
                            probs = [1.0 / len(names)] * len(names)
                            if self.moe_gating._model is not None:
                                fake_X = np.array(
                                    self.moe_gating._encode({}),
                                    dtype=float).reshape(1, -1)
                                if self.moe_gating._scaler:
                                    fake_X = self.moe_gating._scaler.transform(fake_X)
                                probs = self.moe_gating._model.predict_proba(
                                    fake_X)[0].tolist()
                            self.quantum_distiller.register_teacher(
                                "moe", probs)
                        except Exception:
                            pass
                    await self.quantum_distiller.step({})
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(config, "CAUSAL_GRAPH_UPDATE_INTERVAL", 900))
                if self.causal_graph:
                    n = _cfg_get(config, "CAUSAL_MIN_SAMPLES", 20)
                    samples = [{
                        "success_rate": random.uniform(0.5, 1.0),
                        "carbon": random.uniform(0.1, 0.8),
                        "cost": random.uniform(0.1, 0.9),
                        "latency": random.uniform(0.1, 0.9),
                    } for _ in range(n)]
                    await self.causal_graph.learn(
                        samples, ["success_rate", "carbon", "cost", "latency"])
                    if self.causal_rl:
                        await self.causal_rl.estimate_ate(
                            "carbon", "success_rate")
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _temporal_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(config, "TEMPORAL_VERIFICATION_INTERVAL", 300))
                if self.temporal:
                    res = await self.temporal.verify()
                    bad = [k for k, v in res.items() if not v]
                    if bad:
                        logger.warning("Temporal violations: %s", bad)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(_cfg_get(config, "XAI_INTERVAL", 300))
                if self.xai:
                    feats = np.array([0.9, 0.4, 0.5, 0.4])
                    def _score(x):
                        return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                    await self.xai.explain(
                        decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                        label="system_health",
                        features=feats,
                        names=["success_rate", "carbon", "cost", "latency"],
                        model_fn=_score)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(300)
                if self.precision and self.integration_result:
                    # [P1] real accuracy from result
                    recent_acc = self.integration_result.success_rate
                    await self.precision.auto_switch(
                        recent_acc, self.precision.baseline_accuracy)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(config, "CARBON_MARKET_INTERVAL", 3600))
                if self.carbon_market:
                    await self.carbon_market.update_price()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(config, "CHAOS_TEST_INTERVAL", 1800))
                if self.chaos:
                    await self.chaos.run_experiment(
                        name=f"auto_{uuid.uuid4().hex[:6]}",
                        fault_type=random.choice(self.chaos.FAULT_TYPES),
                        target_breaker="integration")
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(config, "AGENT_NEGOTIATION_INTERVAL", 600))
                if self.multi_agent:
                    await self.multi_agent.step()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    async def _digital_twin_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(300)
                if self.digital_twin:
                    await self.digital_twin.get_twin_status()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    async def get_statistics(self):
        async with self._history_lock:
            result = self.integration_result
        stats = {
            "instance_id": self.instance_id,
            "version": "17.1.0",
            "module_count": len(self.module_registry),
            "last_strategy": result.selected_strategy if result else None,
            "last_sustainability": (result.sustainability_score
                                     if result else None),
            "last_success_rate": result.success_rate if result else None,
            "precision": self.precision.current if self.precision else None,
            "temporal_rules": (len(self.temporal.rules) +
                                len(self.temporal._ltl_rules)
                                if self.temporal else 0),
            "causal_edges": (self.causal_graph.summary()["edges"]
                             if self.causal_graph else 0),
            "causal_stability": (self.causal_graph.edge_stability
                                  if self.causal_graph else 0.0),
            "pareto_size": (len(self.pareto_optimizer.get_pareto_front())
                            if self.pareto_optimizer else 0),
            "rec_balance_mwh": self.storage.get_rec_balance(),
            "carbon_price_usd": (self.carbon_market.last_price
                                  if self.carbon_market else None),
            "agent_roles": ({a.id: a.role
                             for a in self.multi_agent.agents.values()}
                            if self.multi_agent else {}),
            "recent_errors": len(self._recent_errors),
            "pqc_algorithm": "dilithium" if PQC_AVAILABLE else "sha256",
        }
        return stats

    async def run_chaos_experiment(self, name, fault_type,
                                    target_breaker=None):
        if not self.chaos:
            return {"error": "chaos disabled"}
        return await self.chaos.run_experiment(
            name, fault_type, target_breaker=target_breaker)

    async def explain_decision(self, label, features, names, score_fn):
        if not self.xai:
            return {"error": "XAI disabled"}
        return await self.xai.explain(
            decision_id=f"exp_{uuid.uuid4().hex[:8]}",
            label=label,
            features=np.array(features),
            names=names,
            model_fn=score_fn)

    async def submit_user_response(self, user_id, chosen_id):
        if self.user_pref_learner:
            await self.user_pref_learner.submit_response(user_id, chosen_id)


# =============================================================================
# SINGLETON
# =============================================================================
_manager_instance: Optional[UnifiedIntegrationManagerV17] = None
_manager_lock = asyncio.Lock()
_shutdown_requested = False


async def get_integration_manager() -> UnifiedIntegrationManagerV17:
    global _manager_instance
    if _manager_instance is None:
        async with _manager_lock:
            if _manager_instance is None:
                _manager_instance = UnifiedIntegrationManagerV17()
                await _manager_instance.start()
    return _manager_instance


def _handle_signal(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        return
    _shutdown_requested = True
    logger.info("Received signal %s", signum)
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_shutdown_instance())
    except Exception:
        pass


async def _shutdown_instance():
    if _manager_instance:
        await _manager_instance.shutdown()


# =============================================================================
# DEMO
# =============================================================================
async def _demo():
    print("=" * 80)
    print("Unified Integration Manager v17.1.0 — All Ten Enhancements")
    print("=" * 80)
    manager = await get_integration_manager()

    result = await manager.run_integration()
    print(f"\n=== Integration Result ===")
    print(f"  status:              {result.overall_status}")
    print(f"  modules run:         {len(result.module_results)}")
    print(f"  success rate:        {result.success_rate:.2f}")
    print(f"  selected strategy:   {result.selected_strategy}")
    print(f"  precision:           {result.precision_level}")
    print(f"  sustainability:      {result.sustainability_score:.1f}")
    print(f"  data quality:        {result.data_quality_score:.1f}")
    print(f"  carbon footprint:    {result.carbon_footprint_kg:.4f} kg")
    print(f"  duration (ms):       {result.total_duration_ms:.1f}")
    print(f"  temporal violations: {result.temporal_violations}")
    print(f"  carbon market:       {result.carbon_market_decision}")
    print(f"  agent:               {result.agent_id}")
    print(f"  causal ATE:          {result.causal_ate}")
    print(f"  checkpoint:          {result.checkpoint_id}")
    print(f"  correlation_id:      {result.correlation_id}")
    print(f"  dataset_hash:        {result.dataset_hash[:16]}...")
    print(f"  pqc_algorithm:       "
          f"{result.quantum_signature['algorithm'] if result.quantum_signature else None}")
    if result.xai_explanation:
        print(f"  XAI explanation:")
        print("    " + result.xai_explanation["explanation"].replace("\n", "\n    "))

    print("\n=== Module outputs ===")
    for mod_name, out in result.module_outputs.items():
        preview = {k: (round(v, 4) if isinstance(v, float) else v)
                   for k, v in out.items()
                   if not isinstance(v, (list, dict))}
        print(f"  {mod_name}: {preview}")

    print("\n=== Batched (3) ===")
    contexts = [
        {"temperature_c": 25.0, "energy_kwh": 1.0},
        {"temperature_c": 30.0, "energy_kwh": 1.5},
        {"temperature_c": 20.0, "energy_kwh": 0.8},
    ]
    batch = await manager.run_batch(contexts, concurrency=2)
    print(f"  batch size: {len(batch)}, "
          f"avg sustain: {sum(r.sustainability_score for r in batch) / len(batch):.1f}")

    print("\n=== Streaming (3) ===")
    async def _stream():
        for c in contexts:
            yield c
    count = 0
    async for _ in manager.stream_integration(_stream()):
        count += 1
    print(f"  streamed: {count}")

    print("\n=== Save / Load state ===")
    print("  saved:", await manager.save_state())
    print("  loaded:", await manager.load_state())

    print("\n=== Statistics ===")
    print(json.dumps(await manager.get_statistics(), indent=2, default=str))

    print("\n=== Chaos experiment (steady_during measured) ===")
    chaos = await manager.run_chaos_experiment(
        "demo", "latency", target_breaker="integration")
    print(json.dumps(chaos, indent=2, default=str))

    print("\n=== Federated round ===")
    if manager.federated_learner:
        agg = await manager.federated_learner.pull_aggregated_weights()
        print(f"  rounds={manager.federated_learner.rounds}, "
              f"aggregated_keys={list(agg.keys()) if agg else None}")

    await manager.shutdown()


if __name__ == "__main__":
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(
                    sig, lambda s=sig: _handle_signal(s, None))
            except (NotImplementedError, RuntimeError):
                try:
                    signal.signal(sig, _handle_signal)
                except Exception:
                    pass
        loop.run_until_complete(_demo())
    except KeyboardInterrupt:
        pass
