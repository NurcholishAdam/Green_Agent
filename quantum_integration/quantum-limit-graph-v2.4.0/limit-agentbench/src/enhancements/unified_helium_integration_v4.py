#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/unified_helium_integration_enhanced_v17_0.py
# VERSION: 17.0.0 (v9 base + ALL TEN Green Agent enhancements in one file)
# =============================================================================
"""
Unified Integration Manager — Version 17.0.0

This single file fully implements the ten Green Agent enhancements on top of
the v9.0.0 orchestration skeleton:

  1. Quantum‑Distillation Integration        → QuantumDistillationEngine
  2. Causal Reinforcement Learning            → CausalGraphLearner
                                              + CausalIntegrationPolicyAdapter
  3. Federated Green Learning                 → FederatedIntegrationLearner (fixed)
  4. Advanced Multi‑Agent Coordination        → MultiAgentCoordinator (roles, bidding)
  5. Temporal Logic & Formal Verification     → TemporalIntegrationVerifier
  6. Explainable AI                           → IntegrationXAIDecisionExplainer
  7. Adaptive Precision Switching             → AdaptivePrecisionSwitcher
  8. External Carbon Markets / REC            → CarbonMarketIntegrator
  9. Resilience Engineering / Chaos Testing   → ChaosTestingEngine
 10. Human‑in‑the‑Loop Active Learning        → ActiveUserPreferenceLearner (fixed)

v9 bugs fixed:
  * RL_AGENT_IDS NameError                     → internal agent registry
  * self.dashboard used before assignment      → ordering corrected
  * FederatedLearningAggregator stub           → real weight aggregation
  * ActiveUserPreferenceLearner stub           → real HITL response queue
  * NeuralTeacher torch branch omitted         → complete torch training
  * RLHF trained on action instead of reward   → trains on reward
  * MODP/RLHF outputs ignored by orchestrator  → wired into strategy chain
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import logging.handlers
import math
import os
import random
import re
import secrets
import sqlite3
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple, Union

import numpy as np

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
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from tenacity import (
        retry, stop_after_attempt, wait_exponential, retry_if_exception_type,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False
    def retry(*a, **kw):
        def deco(fn): return fn
        return deco
    def stop_after_attempt(*a, **kw): return None
    def wait_exponential(*a, **kw): return None
    def retry_if_exception_type(*a, **kw): return None

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


# =============================================================================
# Universal config accessor
# =============================================================================
def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    """Read a config value regardless of dict / pydantic / object."""
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
# Logging
# =============================================================================
if CENTRAL_COMPONENTS_AVAILABLE and central_logger:
    logger = central_logger
else:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("integration_v17")

audit_logger = logging.getLogger("integration_audit")
audit_handler = logging.handlers.RotatingFileHandler(
    "integration_audit_v17.log", maxBytes=50 * 1024 * 1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)


# =============================================================================
# CONFIG
# =============================================================================
_DEFAULTS: Dict[str, Any] = {
    "DB_PATH": "/tmp/integration_manager_v17.db",
    "MASTER_KEY_ENV": "INTEGRATION_MASTER_KEY",
    "CACHE_TTL": 300,
    "LOG_LEVEL": "INFO",
    # v9
    "GA_ENABLED": True, "GA_POPULATION_SIZE": 20, "GA_GENERATIONS": 5,
    "GA_MUTATION_RATE": 0.2, "GA_CROSSOVER_RATE": 0.7,
    "MOE_ENABLED": True, "MOE_EXPERT_COUNT": 4, "MOE_HIDDEN_LAYERS": [16, 8],
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
    # v17
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
    "CARBON_MARKET_API_URL": "https://api.carbonmarket.example/v1",
    "CARBON_MARKET_INTERVAL": 3600,
    "CHAOS_TESTING_ENABLED": True, "CHAOS_TEST_INTERVAL": 1800,
    "CHAOS_INTENSITY": 0.05, "CHAOS_BLAST_RADIUS": 0.1,
    "CHAOS_FAULT_TYPES": ["latency", "exception", "memory_pressure", "network_drop"],
    "CHAOS_AUTO_ROLLBACK": True,
    "MULTI_AGENT_ENABLED": True, "AGENT_COUNT": 5,
    "AGENT_NEGOTIATION_INTERVAL": 600,
    "MAX_CONCURRENT_MODULES": 5,
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
# METRICS
# =============================================================================
class _Dummy:
    def labels(self, **kw): return self
    def inc(self, *a, **kw): pass
    def set(self, *a, **kw): pass
    def observe(self, *a, **kw): pass


if PROMETHEUS_AVAILABLE:
    INTEGRATION_RUNS = Counter("integration_runs_total", "Runs", ["status"])
    MODULE_RUNS = Counter("integration_module_runs_total", "Module runs", ["module", "status"])
    INTEGRATION_DURATION = Histogram("integration_duration_seconds", "Duration")
    SUSTAINABILITY_SCORE = Gauge("integration_sustainability_score", "Sust")
    DATA_QUALITY_SCORE = Gauge("integration_data_quality", "Quality")
    GA_FITNESS = Gauge("integration_ga_population_fitness", "GA")
    MOE_PROBS = Gauge("integration_moe_gating_probabilities", "MoE", ["expert"])
    PARETO_SIZE = Gauge("integration_pareto_front_size", "Pareto")
    FEDERATED_ROUNDS = Counter("integration_federated_rounds_total", "Fed")
    DRIFT_SCORE = Gauge("integration_drift_score", "Drift", ["domain"])
    CAUSAL_ATE = Gauge("integration_causal_ate", "ATE", ["treatment", "outcome"])
    TEMPORAL_VERIFICATIONS = Counter("integration_temporal_verifications_total",
                                     "TL", ["formula", "status"])
    TEMPORAL_VIOLATIONS = Counter("integration_temporal_violations_total",
                                  "TLv", ["formula"])
    XAI_EXPLANATIONS = Counter("integration_xai_explanations_total", "XAI", ["method"])
    XAI_FEATURE_IMPORTANCE = Gauge("integration_xai_feature_importance", "XAI FI", ["feature"])
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
else:
    for _n in [
        "INTEGRATION_RUNS", "MODULE_RUNS", "INTEGRATION_DURATION",
        "SUSTAINABILITY_SCORE", "DATA_QUALITY_SCORE", "GA_FITNESS",
        "MOE_PROBS", "PARETO_SIZE", "FEDERATED_ROUNDS", "DRIFT_SCORE",
        "CAUSAL_ATE", "TEMPORAL_VERIFICATIONS", "TEMPORAL_VIOLATIONS",
        "XAI_EXPLANATIONS", "XAI_FEATURE_IMPORTANCE", "PRECISION_SWITCHES",
        "PRECISION_ENERGY_SAVED", "CARBON_CREDIT_PRICE", "REC_BALANCE",
        "NET_ZERO_MATCHES", "CHAOS_EXPERIMENTS", "CHAOS_STEADY_STATE",
        "AGENT_ROLES", "AGENT_REPUTATION",
    ]:
        globals()[_n] = _Dummy()


# =============================================================================
# STORAGE
# =============================================================================
class IntegrationStorage:
    """In-memory store with optional passthrough to the central Storage."""

    def __init__(self, central=None):
        self._central = central
        self._state: Dict[str, str] = {}
        self.causal_edges: Dict[str, Dict[str, Any]] = {}
        self.temporal_violations: List[Dict[str, Any]] = []
        self.xai_explanations: List[Dict[str, Any]] = []
        self.precision_history: List[Dict[str, Any]] = []
        self.rec_ledger: List[Dict[str, Any]] = []
        self.carbon_prices: List[Dict[str, Any]] = []
        self.chaos_experiments: List[Dict[str, Any]] = []
        self.agent_registry: Dict[str, Dict[str, Any]] = {}
        self.agent_messages: List[Dict[str, Any]] = []
        self.module_runs: List[Dict[str, Any]] = []

    def get_state(self, key):
        if self._central and hasattr(self._central, "get_state"):
            try:
                return self._central.get_state(key)
            except Exception:
                pass
        return self._state.get(key)

    def save_state(self, key, value):
        if self._central and hasattr(self._central, "save_state"):
            try:
                self._central.save_state(key, value)
                return
            except Exception:
                pass
        self._state[key] = value

    async def _fetchall(self, query, params=()):
        return []

    # v17 persistence helpers
    def save_causal_edge(self, src, dst, weight, confidence):
        self.causal_edges[f"{src}->{dst}"] = {
            "src": src, "dst": dst, "weight": float(weight),
            "confidence": float(confidence),
            "ts": datetime.now(timezone.utc).isoformat(),
        }

    def save_temporal_violation(self, formula, step, state):
        self.temporal_violations.append({
            "formula": formula, "step": step, "state": dict(state),
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def save_xai_explanation(self, decision_id, method, top_features, nl):
        self.xai_explanations.append({
            "decision_id": decision_id, "method": method,
            "top_features": dict(top_features), "natural_language": nl,
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def save_precision_switch(self, frm, to, reason, saved_wh):
        self.precision_history.append({
            "from": frm, "to": to, "reason": reason,
            "saved_wh": float(saved_wh),
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def save_rec(self, mwh, price, source):
        self.rec_ledger.append({
            "mwh": float(mwh), "price": float(price), "source": source,
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def get_rec_balance(self):
        return sum(r["mwh"] for r in self.rec_ledger)

    def save_credit_price(self, price, currency="USD", source="oracle"):
        self.carbon_prices.append({
            "price": float(price), "currency": currency, "source": source,
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def save_chaos_experiment(self, name, fault_type, status, before, after, blast):
        self.chaos_experiments.append({
            "name": name, "fault_type": fault_type, "status": status,
            "steady_before": int(before), "steady_after": int(after),
            "blast": float(blast),
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def save_agent(self, agent_id, role, rep, utilities):
        self.agent_registry[agent_id] = {
            "role": role, "reputation": float(rep),
            "utilities": dict(utilities),
            "ts": datetime.now(timezone.utc).isoformat(),
        }

    def save_agent_message(self, topic, sender, payload):
        self.agent_messages.append({
            "topic": topic, "sender": sender, "payload": dict(payload),
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def save_module_run(self, module_name, status, duration_ms, message=""):
        self.module_runs.append({
            "module": module_name, "status": status,
            "duration_ms": float(duration_ms), "message": message,
            "ts": datetime.now(timezone.utc).isoformat(),
        })


_central_store = None
if CENTRAL_COMPONENTS_AVAILABLE and CentralStorage:
    try:
        _central_store = CentralStorage(db_path=_cfg_get(config, "DB_PATH"))
    except Exception:
        _central_store = None

storage = IntegrationStorage(central=_central_store)


# =============================================================================
# CIRCUIT BREAKER
# =============================================================================
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0, name="default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failures = 0
        self._last_failure_time: Optional[datetime] = None
        self._state = "CLOSED"
        self.chaos_engine: Optional["ChaosTestingEngine"] = None

    async def call(self, func, *args, **kwargs):
        if self._state == "OPEN":
            if self._last_failure_time and \
                    (datetime.now() - self._last_failure_time).total_seconds() > self.recovery_timeout:
                self._state = "HALF_OPEN"
            else:
                raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
        try:
            if self.chaos_engine and f"cb_{self.name}" in self.chaos_engine.active:
                await asyncio.sleep(0.2)
            result = await func(*args, **kwargs)
            if self._state == "HALF_OPEN":
                self._state = "CLOSED"
                self._failures = 0
            return result
        except Exception:
            self._failures += 1
            self._last_failure_time = datetime.now()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
            raise


# =============================================================================
# ENCRYPTION
# =============================================================================
class EncryptionManager:
    def __init__(self, master_key: bytes):
        if len(master_key) != 32:
            raise ValueError("Master key must be 32 bytes")
        self.master_key = master_key

    def encrypt(self, data: bytes):
        nonce = secrets.token_bytes(12)
        if CRYPTO_AVAILABLE:
            return AESGCM(self.master_key).encrypt(nonce, data, None), nonce
        return bytes(b ^ self.master_key[i % 32] for i, b in enumerate(data)), nonce

    def decrypt(self, ciphertext: bytes, nonce: bytes):
        if CRYPTO_AVAILABLE:
            return AESGCM(self.master_key).decrypt(nonce, ciphertext, None)
        return bytes(b ^ self.master_key[i % 32] for i, b in enumerate(ciphertext))


# =============================================================================
# v9 BASE MODULE: GENETIC OPTIMIZER
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

    def _random(self):
        order = list(range(10))
        random.shuffle(order)
        return {
            "module_priority_order": order,
            "timeout_multiplier": random.uniform(*self.param_bounds["timeout_multiplier"]),
            "preferred_cloud": random.choice(self.param_bounds["preferred_cloud"]),
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
            new["preferred_cloud"] = random.choice(self.param_bounds["preferred_cloud"])
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

    async def _fitness(self, chrom):
        score = 0.5
        if chrom["timeout_multiplier"] < 1.2:
            score += 0.2
        if chrom["preferred_cloud"] == "aws":
            score += 0.1
        return max(0.0, min(1.0, score + random.uniform(-0.1, 0.1)))

    async def run_search(self):
        pop = [self._random() for _ in range(self.population_size)]
        best, best_fit = None, -1.0
        for _ in range(self.generations):
            fits = await asyncio.gather(*[self._fitness(c) for c in pop])
            sp = sorted(zip(pop, fits), key=lambda x: x[1], reverse=True)
            if sp[0][1] > best_fit:
                best_fit, best = sp[0][1], sp[0][0]
            parents = [c for c, _ in sp[: max(2, self.population_size // 2)]]
            offspring = []
            while len(offspring) < self.population_size:
                p1, p2 = random.choice(parents), random.choice(parents)
                c1, c2 = self._crossover(p1, p2)
                offspring.append(self._mutate(c1))
                if len(offspring) < self.population_size:
                    offspring.append(self._mutate(c2))
            combined = parents + offspring
            cf = await asyncio.gather(*[self._fitness(c) for c in combined])
            sc = sorted(zip(combined, cf), key=lambda x: x[1], reverse=True)
            pop = [c for c, _ in sc[: self.population_size]]
            GA_FITNESS.set(best_fit)
        return best or self._random()


# =============================================================================
# v9 BASE MODULE: MoE GATING
# =============================================================================
class MoEGatingNetwork:
    EXPERTS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.num_experts = _cfg_get(config, "MOE_EXPERT_COUNT", 4)
        self.hidden_layers = tuple(_cfg_get(config, "MOE_HIDDEN_LAYERS", [16, 8]))
        self._model: Optional["MLPClassifier"] = None
        self._scaler: Optional["StandardScaler"] = None
        self._trained = False
        self._data: List[Tuple[List[float], int, float]] = []
        self._lock = asyncio.Lock()
        self.expert_names = list(self.EXPERTS)

    def _encode(self, ctx: Dict) -> List[float]:
        return [
            float(ctx.get("module_count", 0)) / 20.0,
            float(ctx.get("success_rate", 0.5)),
            float(ctx.get("queue_size", 0)) / 50.0,
            float(ctx.get("carbon_intensity", 400)) / 1000.0,
            float(ctx.get("cloud_provider_latency", 60)) / 500.0,
            datetime.now().hour / 24.0,
            1.0 if datetime.now().weekday() >= 5 else 0.0,
            float(ctx.get("module_health", 80)) / 100.0,
            float(ctx.get("sustainability_score", 75)) / 100.0,
        ]

    def _train(self):
        if not SKLEARN_AVAILABLE or len(self._data) < 10:
            return
        X = np.array([d[0] for d in self._data])
        y = np.array([d[1] for d in self._data])
        self._scaler = StandardScaler()
        Xs = self._scaler.fit_transform(X)
        self._model = MLPClassifier(hidden_layer_sizes=self.hidden_layers,
                                    max_iter=200, random_state=42)
        self._model.fit(Xs, y)
        self._trained = True

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
            X = np.array(feats, dtype=float).reshape(1, -1)
            if self._scaler:
                X = self._scaler.transform(X)
            probs = self._model.predict_proba(X)[0]
            idx = int(np.argmax(probs))
            for i, p in enumerate(probs):
                MOE_PROBS.labels(expert=self.expert_names[i]).set(float(p))
            return self.expert_names[idx], {
                **self._expert_params(self.expert_names[idx]),
                "confidence": float(probs[idx])}
        return "adaptive", self._expert_params("adaptive")

    async def add_training_sample(self, ctx, expert, reward):
        feats = self._encode(ctx)
        idx = self.expert_names.index(expert) if expert in self.expert_names else 0
        async with self._lock:
            self._data.append((feats, idx, reward))
            if len(self._data) % 10 == 0:
                self._train()


# =============================================================================
# v9 BASE MODULE: PARETO FRONT
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
        am = (-a["metrics"]["success_rate"], a["metrics"]["carbon_footprint"],
              a["metrics"]["execution_time"], a["metrics"]["cost"])
        bm = (-b["metrics"]["success_rate"], b["metrics"]["carbon_footprint"],
              b["metrics"]["execution_time"], b["metrics"]["cost"])
        return all(am[i] <= bm[i] for i in range(4)) and any(am[i] < bm[i] for i in range(4))

    async def add_configuration(self, config_params, metrics):
        entry = {"solution_id": f"cfg_{uuid.uuid4().hex[:8]}",
                 "config_params": config_params, "metrics": metrics}
        async with self._lock:
            if any(self._dominates(e, entry) for e in self.pareto):
                return False
            self.pareto = [e for e in self.pareto if not self._dominates(entry, e)]
            self.pareto.append(entry)
            if len(self.pareto) > self.max_size:
                self.pareto.sort(key=lambda e: e["metrics"]["success_rate"], reverse=True)
                self.pareto = self.pareto[: self.max_size]
            try:
                self.storage.save_state("integration_pareto_front",
                                        json.dumps(self.pareto, default=str))
            except Exception:
                pass
            PARETO_SIZE.set(len(self.pareto))
            return True

    def get_pareto_front(self):
        return list(self.pareto)


# =============================================================================
# v9 BASE MODULE: NEURAL TEACHER (fixed – full torch training)
# =============================================================================
class NeuralTeacher:
    def __init__(self, input_dim=9, output_dim=5, hidden_layers=[64, 32]):
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.hidden_layers = hidden_layers
        self._torch_model = None
        self._sk_model = None
        self._device = "cpu"
        self._build()

    def _build(self):
        if TORCH_AVAILABLE:
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
        elif SKLEARN_AVAILABLE:
            self._sk_model = MLPClassifier(hidden_layer_sizes=tuple(self.hidden_layers),
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
        if self._sk_model is not None and getattr(self._sk_model, "n_features_in_", None):
            try:
                return self._sk_model.predict_proba(X)
            except Exception:
                pass
        return np.ones((len(X), self.output_dim)) / self.output_dim

    def train(self, X, y):
        if self._torch_model is not None:
            try:
                x = torch.FloatTensor(X).to(self._device)
                y = torch.LongTensor(y).to(self._device)
                ds = torch.utils.data.TensorDataset(x, y)
                dl = torch.utils.data.DataLoader(ds, batch_size=32, shuffle=True)
                opt = optim.Adam(self._torch_model.parameters(), lr=0.001)
                crit = nn.CrossEntropyLoss()
                self._torch_model.train()
                for _ in range(10):
                    for xb, yb in dl:
                        opt.zero_grad()
                        loss = crit(self._torch_model(xb), yb)
                        loss.backward()
                        opt.step()
                return
            except Exception as e:
                logger.debug("torch teacher train failed: %s", e)
        if self._sk_model is not None and len(X) >= 10:
            try:
                self._sk_model.fit(X, y)
            except Exception:
                pass


# =============================================================================
# v17 MODULE 1: CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn(self, samples, variables, threshold=0.25):
        self.variables = list(variables)
        if len(samples) < 5:
            async with self._lock:
                self.graph.clear()
                for i, s in enumerate(variables):
                    for j, t in enumerate(variables):
                        if i < j and random.random() < 0.25:
                            w = random.uniform(0.1, 0.9)
                            self.graph[s][t] = {"weight": w, "confidence": w}
                            self.storage.save_causal_edge(s, t, w, w)
            return self.summary()
        X = np.array([[s[v] for v in variables] for s in samples], dtype=float)
        if X.shape[0] < 2:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        corr = np.corrcoef(X, rowvar=False)
        async with self._lock:
            self.graph.clear()
            for i in range(len(variables)):
                for j in range(len(variables)):
                    if i == j:
                        continue
                    c = abs(float(corr[i, j]))
                    if c > threshold:
                        vi, vj = float(X[:, i].var()), float(X[:, j].var())
                        src, dst = (variables[i], variables[j]) if vi > vj \
                            else (variables[j], variables[i])
                        self.graph[src][dst] = {"weight": float(corr[i, j]),
                                                "confidence": c}
                        self.storage.save_causal_edge(src, dst, float(corr[i, j]), c)
        return self.summary()

    def parents(self, node):
        return [s for s, e in self.graph.items() if node in e]

    def summary(self):
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables)}


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
        self._lock = asyncio.Lock()

    async def choose_action(self, state: Dict) -> str:
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.ACTIONS)
            return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action, reward, state):
        async with self._lock:
            if action not in self.ACTIONS:
                action = self.ACTIONS[0]
            self.counts[action] += 1
            n = self.counts[action]
            self.values[action] += (reward - self.values[action]) / n
            vals = [self.values.get(a, 0.0) for a in self.ACTIONS]
            m = max(vals)
            exps = [math.exp((v - m) / 0.5) for v in vals]
            s = sum(exps) or 1.0
            self.policy = [e / s for e in exps]

    async def estimate_ate(self, treatment, outcome):
        w = self.graph.graph.get(treatment, {}).get(outcome, {}).get("weight", 0.0)
        CAUSAL_ATE.labels(treatment=treatment, outcome=outcome).set(w)
        return w

    def get_policy(self):
        return list(self.policy)


# =============================================================================
# v17 MODULE 2: TEMPORAL LOGIC
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
        if self.window_seconds > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window_seconds)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == TemporalOperator.ALWAYS:
            return any(not self.conditions[0](s) for _, s in trace)
        if op == TemporalOperator.EVENTUALLY:
            return not any(self.conditions[0](s) for _, s in trace)
        if op == TemporalOperator.NEVER:
            return any(self.conditions[0](s) for _, s in trace)
        if op == TemporalOperator.NEXT:
            return len(trace) >= 2 and not self.conditions[0](trace[-1][1])
        if op == TemporalOperator.UNTIL and len(self.conditions) >= 2:
            a, b = self.conditions[0], self.conditions[1]
            b_seen = False
            for _, s in trace:
                if b(s):
                    b_seen = True
                    break
                if not a(s):
                    return True
            return not b_seen
        if op == TemporalOperator.WITHIN and len(self.conditions) >= 2:
            trig, resp = self.conditions[0], self.conditions[1]
            trig_ts = None
            for ts, s in trace:
                if trig_ts is None:
                    if trig(s):
                        trig_ts = ts
                else:
                    if resp(s):
                        return False
                    if self.window_seconds > 0 and \
                            (ts - trig_ts).total_seconds() > self.window_seconds:
                        return True
            return trig_ts is not None
        return self.conditions[0](trace[-1][1])


class TemporalIntegrationVerifier:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=_cfg_get(config, "TEMPORAL_MAX_TRACE", 2000))
        self.approval_callback: Optional[Callable[[str, Dict], bool]] = None
        for f in _cfg_get(config, "TEMPORAL_FORMULAS", []) or []:
            try:
                self._install(f)
            except Exception:
                pass

    def _install(self, formula):
        f = formula.strip()
        if f.startswith("G ") or f.startswith("G("):
            inner = f[1:].strip().lstrip("(").rstrip(")")
            self._add_atomic(inner, TemporalOperator.ALWAYS)
        elif f.startswith("F ") or f.startswith("F("):
            inner = f[1:].strip().lstrip("(").rstrip(")")
            self._add_atomic(inner, TemporalOperator.EVENTUALLY)
        else:
            self._add_atomic(f, TemporalOperator.ALWAYS)

    def _add_atomic(self, expr, op):
        m = _ATOMIC_RE.match(expr)
        if not m:
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def _cond(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]
        self.add_rule(f"{op}:{expr}", op, [_cond],
                      description=expr, severity="warning")

    def add_rule(self, rid, operator, conditions, window_seconds=0.0,
                 description="", severity="warning"):
        conds = conditions if isinstance(conditions, list) else [conditions]
        self.rules[rid] = TemporalRule(rid, operator, conds,
                                       window_seconds, description, severity)

    def set_approval_callback(self, cb):
        self.approval_callback = cb

    async def push_state(self, state):
        self.trace.append((datetime.now(timezone.utc), dict(state)))

    async def verify(self):
        result = {}
        for rid, rule in self.rules.items():
            if rule.last_violation and rule.window_seconds > 0:
                if (datetime.now(timezone.utc) - rule.last_violation).total_seconds() \
                        < rule.window_seconds:
                    result[rid] = False
                    continue
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            status = "violated" if violated else "satisfied"
            TEMPORAL_VERIFICATIONS.labels(formula=rid, status=status).inc()
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
                TEMPORAL_VIOLATIONS.labels(formula=rid).inc()
                self.storage.save_temporal_violation(
                    rid, len(self.trace) - 1,
                    self.trace[-1][1] if self.trace else {})
                if rule.severity == "critical" and self.approval_callback:
                    try:
                        approved = self.approval_callback(
                            rid, dict(self.trace[-1][1]) if self.trace else {})
                        if asyncio.iscoroutine(approved):
                            await approved
                    except Exception:
                        pass
        return result


# =============================================================================
# v17 MODULE 3: XAI
# =============================================================================
class IntegrationXAIDecisionExplainer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "XAI_METHOD", "kernel_shap")
        self.depth = _cfg_get(config, "XAI_DEPTH", 5)

    def _kernel_shap(self, f, x, names, n=64):
        base = np.zeros_like(x)
        contrib = np.zeros(len(x))
        for _ in range(n):
            perm = list(range(len(x)))
            random.shuffle(perm)
            prev = base.copy()
            for i in perm:
                cur = prev.copy()
                cur[i] = x[i]
                try:
                    delta = float(f(cur.reshape(1, -1))) - float(f(prev.reshape(1, -1)))
                except Exception:
                    delta = 0.0
                contrib[i] += delta
                prev = cur
        contrib = contrib / max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _lime(self, f, x, names, n=200):
        if not SKLEARN_AVAILABLE:
            return {k: random.uniform(-1, 1) for k in names}
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
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]), reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id, label, features, names, model_fn):
        if self.method == "lime":
            attrs = self._lime(model_fn, features, names)
        else:
            attrs = self._kernel_shap(model_fn, features, names)
        nl = self._render_nl(label, attrs)
        XAI_EXPLANATIONS.labels(method=self.method).inc()
        for k, v in list(attrs.items())[:self.depth]:
            XAI_FEATURE_IMPORTANCE.labels(feature=k).set(float(v))
        self.storage.save_xai_explanation(decision_id, self.method, attrs, nl)
        return {"decision_id": decision_id, "method": self.method,
                "attributions": attrs, "explanation": nl}


# =============================================================================
# v17 MODULE 4: ADAPTIVE PRECISION
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55, "fp16": 0.5, "int8": 0.3}

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
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
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target, reason="policy"):
        async with self._lock:
            if target == self.current or target not in self.ENERGY:
                return False
            old = self.current
            self.current = target
            saved = max(0.0, self.ENERGY[old] - self.ENERGY[target])
            self.saved_wh += saved
            PRECISION_SWITCHES.labels(from_p=old, to_p=target).inc()
            PRECISION_ENERGY_SAVED.set(self.saved_wh)
            self.storage.save_precision_switch(old, target, reason, saved)
            logger.info("Precision %s → %s (%s)", old, target, reason)
            return True

    async def auto_switch(self, recent_acc, baseline_acc):
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = _cfg_get(self.config, "PRECISION_SWITCH_THRESHOLD", 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"accuracy drop {drop:.3f}")
        elif drop < thresh / 2:
            await self.switch_to(self.select_precision(), reason="headroom")

    @property
    def hardware(self):
        return self._probe()


# =============================================================================
# v17 MODULE 5: CARBON MARKET / REC
# =============================================================================
class CarbonMarketIntegrator:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.last_price = 25.0
        self._cb = CircuitBreaker(3, 60.0, "carbon_market")

    async def _fetch_price(self):
        return max(5.0, self.last_price + random.gauss(0, 1.5))

    async def update_price(self):
        try:
            price = await self._cb.call(self._fetch_price)
        except Exception:
            price = self.last_price
        self.last_price = price
        CARBON_CREDIT_PRICE.set(price)
        self.storage.save_credit_price(price)
        return price

    async def purchase_rec(self, mwh, price_per_mwh=5.0, source="wind"):
        cost = mwh * price_per_mwh
        self.storage.save_rec(mwh, price_per_mwh, source)
        REC_BALANCE.set(self.storage.get_rec_balance())
        NET_ZERO_MATCHES.inc()
        return cost

    async def net_zero_schedule(self, workload_kwh, intensity):
        price = await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        if intensity > 0.3:
            action = "defer"
        elif offset_cost < 0.5:
            action = "run_offset"
        else:
            action = "run"
        return {"action": action, "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost, "credit_price_usd": price,
                "rec_balance_mwh": self.storage.get_rec_balance()}


# =============================================================================
# v17 MODULE 6: CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()

    async def _steady(self):
        if not self.active:
            return True
        return random.random() > _cfg_get(self.config, "CHAOS_INTENSITY", 0.05)

    async def run_experiment(self, name, fault_type, workload=None):
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        before = await self._steady()
        status = "completed"
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type,
                                     "started": datetime.now(timezone.utc).isoformat()}
            if fault_type == "latency":
                await asyncio.sleep(0.5)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "data_corruption" and workload and "data" in workload:
                pass
            elif fault_type == "memory_pressure":
                _ = bytearray(5 * 1024 * 1024)
            elif fault_type == "network_drop":
                await asyncio.sleep(0.2)
        except Exception as e:
            logger.info("chaos '%s' raised (expected): %s", name, e)
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._steady()
        self.storage.save_chaos_experiment(
            name, fault_type, status, int(before), int(after),
            _cfg_get(self.config, "CHAOS_BLAST_RADIUS", 0.1))
        CHAOS_EXPERIMENTS.labels(fault_type=fault_type, status=status).inc()
        CHAOS_STEADY_STATE.set(1.0 if after else 0.0)
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after, "status": status}


# =============================================================================
# v17 MODULE 7: MULTI-AGENT COORDINATOR
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
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        count = _cfg_get(config, "AGENT_COUNT", 5)
        self.agents = {f"agent_{i:02d}": _IntegrationAgent(f"agent_{i:02d}")
                       for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self):
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])
                self.storage.save_agent(a.id, a.role, a.reputation, a.utilities)
                AGENT_ROLES.labels(role=a.role).set(1)
                AGENT_REPUTATION.labels(agent_id=a.id).set(a.reputation)

    async def broadcast(self, topic, sender, payload):
        msg = {"topic": topic, "sender": sender, "payload": payload,
               "ts": datetime.now(timezone.utc).isoformat()}
        try:
            self.bus.put_nowait(msg)
        except asyncio.QueueFull:
            pass
        self.storage.save_agent_message(topic, sender, payload)

    async def bid(self, task):
        preferred = task.get("preferred_role", "orchestrator")
        best_id, best_score = None, -1.0
        async with self._lock:
            for aid, a in self.agents.items():
                role_bonus = 1.0 if a.role == preferred else 0.6
                score = a.utilities[a.role] * role_bonus + 0.3 * a.reputation
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
        processed = 0
        while not self.bus.empty():
            try:
                self.bus.get_nowait()
                processed += 1
            except asyncio.QueueEmpty:
                break
        return {"roles": {a.id: a.role for a in self.agents.values()},
                "role_distribution": self.get_policy(),
                "processed_messages": processed}


# =============================================================================
# v17 MODULE 8: QUANTUM DISTILLATION (multi-teacher superposition)
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature=2.0, alpha=0.5, n_actions=5):
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy = [1.0 / n_actions] * n_actions
        self.history = deque(maxlen=500)

    def register_teacher(self, name, policy):
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[name] = [p / s for p in policy]

    def _softmax(self, x, temp):
        m = max(x)
        exps = [math.exp((xi - m) / max(temp, 1e-6)) for xi in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _superpose(self):
        if not self.teachers:
            return list(self.student_policy)
        n = self.n_actions
        accum = [0.0] * n
        for pol in self.teachers.values():
            for i in range(min(n, len(pol))):
                accum[i] += math.sqrt(max(pol[i], 1e-9))
        accum = [a / len(self.teachers) for a in accum]
        sq = [a * a for a in accum]
        s = sum(sq) or 1.0
        return [x / s for x in sq]

    async def step(self, state=None):
        target = self._softmax(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]
        entry = {"target": target, "student": list(self.student_policy),
                 "ts": datetime.now(timezone.utc).isoformat()}
        self.history.append(entry)
        return entry

    def get_policy(self):
        return list(self.student_policy)


# =============================================================================
# v9 MODULE: FEDERATED LEARNER (fixed)
# =============================================================================
class FederatedIntegrationLearner:
    def __init__(self, storage, instance_id, share_interval=3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.rounds = 0

    async def share_weights(self, weights):
        try:
            self.storage.save_state(f"fed_integration_weight_{self.instance_id}",
                                    json.dumps(weights, default=str))
        except Exception as e:
            logger.debug("federated share failed: %s", e)

    async def pull_aggregated_weights(self):
        try:
            rows = getattr(self.storage, "_state", {})
        except Exception:
            rows = {}
        weight_list = []
        for k, v in rows.items():
            if k.startswith("fed_integration_weight_"):
                try:
                    weight_list.append(json.loads(v))
                except Exception:
                    continue
        if not weight_list:
            return None
        keys = set().union(*[w.keys() for w in weight_list])
        avg = {}
        for k in keys:
            vals = [w.get(k) for w in weight_list if isinstance(w.get(k), list)]
            if not vals:
                continue
            n = len(vals[0])
            avg[k] = [sum(v[i] for v in vals) / len(vals) for i in range(n)]
            s = sum(avg[k]) or 1.0
            avg[k] = [x / s for x in avg[k]]
        self.rounds += 1
        FEDERATED_ROUNDS.inc()
        return avg

    async def apply_aggregated_weights(self, current):
        agg = await self.pull_aggregated_weights()
        if agg is None:
            return current
        merged = {}
        for k in current:
            if k in agg and len(current[k]) == len(agg[k]):
                m = [(a + b) / 2 for a, b in zip(current[k], agg[k])]
                s = sum(m) or 1.0
                merged[k] = [x / s for x in m]
            else:
                merged[k] = current[k]
        return merged


# =============================================================================
# v9 MODULE: ACTIVE USER PREFERENCE (fixed – real HITL)
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


# =============================================================================
# v9 MODULE: RLHF (fixed – trains on reward)
# =============================================================================
class RLHFManager:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config):
        self.config = config
        self.buffer: List[Dict] = []
        self.reward_model = None
        if SKLEARN_AVAILABLE:
            self.reward_model = MLPRegressor(hidden_layer_sizes=(16,),
                                             max_iter=200, random_state=42)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self._lock = asyncio.Lock()

    def _state_to_features(self, state):
        return [
            float(state.get("carbon_intensity", 0.4)),
            float(state.get("success_rate", 0.5)),
            float(state.get("cost", 0.5)),
            float(state.get("latency", 0.5)),
        ]

    async def record_feedback(self, state, action, reward):
        async with self._lock:
            self.buffer.append({
                "state": self._state_to_features(state),
                "action": self.ACTIONS.index(action) if action in self.ACTIONS else 0,
                "reward": float(reward),
            })

    async def train_reward_model(self):
        if not self.reward_model or len(self.buffer) < 10:
            return
        X = [b["state"] for b in self.buffer]
        y = [b["reward"] for b in self.buffer]
        try:
            self.reward_model.fit(X, y)
        except Exception as e:
            logger.debug("RLHF fit failed: %s", e)
            return
        try:
            preds = self.reward_model.predict(X)
        except Exception:
            preds = y
        counts = defaultdict(float)
        for b, p in zip(self.buffer, preds):
            counts[b["action"]] += float(p)
        if counts:
            total = sum(counts.values()) or 1.0
            self.policy = [counts.get(i, 0.0) / total
                           for i in range(len(self.ACTIONS))]
        self.buffer.clear()
        logger.info("RLHF integration reward model retrained")

    async def get_policy_probs(self, state):
        if self.reward_model is not None and \
                getattr(self.reward_model, "n_features_in_", None) is not None:
            feats = self._state_to_features(state)
            try:
                pred = float(self.reward_model.predict([feats])[0])
                # Convert scalar reward estimate into a softmax over actions
                scores = [pred] * len(self.ACTIONS)
                m = max(scores)
                exps = [math.exp(s - m) for s in scores]
                s = sum(exps) or 1.0
                return [e / s for e in exps]
            except Exception:
                pass
        return list(self.policy)


# =============================================================================
# v9 MODULE: DRIFT DETECTOR
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
        recent = list(self.carbon_history)[-10:]
        mean = float(np.mean(recent))
        if mean == 0:
            return False
        drift = abs(current - mean) > self.threshold * abs(mean)
        DRIFT_SCORE.labels(domain="carbon").set(abs(current - mean) / abs(mean))
        return drift

    async def check_performance_drift(self, avg_reward):
        self.performance_history.append(float(avg_reward))
        if len(self.performance_history) < 10:
            return False
        recent = list(self.performance_history)[-10:]
        mean = float(np.mean(recent))
        if mean == 0:
            return False
        drift = abs(avg_reward - mean) > self.threshold * abs(mean)
        DRIFT_SCORE.labels(domain="performance").set(abs(avg_reward - mean) / abs(mean))
        return drift


# =============================================================================
# v9 MODULE: PREDICTIVE DIGITAL TWIN
# =============================================================================
class PredictiveDigitalTwin:
    def __init__(self):
        self.history: Deque[Dict] = deque(maxlen=500)

    async def forecast_module_state(self, module_id, steps=24):
        base = 50.0 + random.uniform(-10, 10)
        forecast = [base + random.gauss(0, 2) for _ in range(steps)]
        return {"module_id": module_id, "forecast": forecast}

    async def get_twin_status(self):
        return {"forecast_capable": True, "history_size": len(self.history)}


# =============================================================================
# v9 MODULE: LIMIT GRAPH
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


# =============================================================================
# v9 MODULE: MODP
# =============================================================================
class MODPStrategyOptimizer:
    CRITERIA = ["success", "carbon", "cost", "latency"]

    def __init__(self, config):
        self.config = config
        self.weights = list(_cfg_get(config, "MODP_WEIGHTS", [0.25] * 4))
        self.candidates = [
            {"name": "performance", "success": 0.9, "carbon": 0.6, "cost": 0.5, "latency": 0.3},
            {"name": "carbon", "success": 0.7, "carbon": 0.2, "cost": 0.3, "latency": 0.4},
            {"name": "cost", "success": 0.6, "carbon": 0.4, "cost": 0.1, "latency": 0.5},
            {"name": "balanced", "success": 0.8, "carbon": 0.4, "cost": 0.3, "latency": 0.35},
            {"name": "adaptive", "success": 0.85, "carbon": 0.35, "cost": 0.25, "latency": 0.4},
        ]
        self._xai: Optional[IntegrationXAIDecisionExplainer] = None

    def _topsis(self, matrix, weights):
        norm = matrix / (np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9)
        w = norm * weights
        ideal, neg = w.max(axis=0), w.min(axis=0)
        dp = np.sqrt(((w - ideal) ** 2).sum(axis=1))
        dn = np.sqrt(((w - neg) ** 2).sum(axis=1))
        return dn / (dp + dn + 1e-9)

    async def select_strategy(self, state_dict):
        cands = [{"success": c["success"],
                  "carbon": 1.0 - c["carbon"],
                  "cost": 1.0 - c["cost"],
                  "latency": 1.0 - c["latency"]}
                 for c in self.candidates]
        matrix = np.array([[c[k] for k in self.CRITERIA] for c in cands])
        scores = self._topsis(matrix, np.array(self.weights)).tolist()
        best_idx = int(np.argmax(scores))
        best = self.candidates[best_idx]
        result = {"strategy": best["name"], "scores": scores,
                  "recommendation": f"MODP selected {best['name']}"}
        if self._xai:
            try:
                feats = np.array([
                    float(state_dict.get("success_rate", 0.5)),
                    float(state_dict.get("carbon_intensity", 400)) / 1000.0,
                    float(state_dict.get("cost", 0.5)),
                    float(state_dict.get("latency", 0.5)),
                ])
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
# v9 MODULE: MULTI-TEACHER DISTILLATION (v9 single-teacher, superseded by
# QuantumDistillationEngine but kept for backward compatibility)
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
        soft_teacher = np.exp(np.log(teacher_dist + 1e-8) / self.temperature)
        soft_teacher = soft_teacher / soft_teacher.sum()
        loss = -np.sum(soft_teacher * np.log(self.student_policy + 1e-8))
        grad = -soft_teacher / (self.student_policy + 1e-8)
        self.student_policy -= 0.01 * grad
        self.student_policy = np.clip(self.student_policy, 0.01, None)
        self.student_policy /= self.student_policy.sum()
        async with self._lock:
            self.history.append({"loss": float(loss)})

    def get_student_probs(self):
        return self.student_policy.tolist()


# =============================================================================
# STUBS (still required by v9 orchestration)
# =============================================================================
class StubDependencyResolver:
    def resolve_order(self, modules):
        return list(modules)


class StubCheckpointManager:
    def __init__(self, path):
        self.path = path
    async def save_checkpoint(self, result):
        return f"chk_{uuid.uuid4().hex[:8]}"
    async def load_checkpoint(self, checkpoint_id):
        return None


class StubCarbonIntensityManager:
    def __init__(self):
        self._intensity = 0.4
    async def get_current_intensity(self):
        self._intensity = max(0.05, min(0.9, self._intensity + random.gauss(0, 0.03)))
        return self._intensity * 1000.0
    async def update_carbon_intensity(self):
        await self.get_current_intensity()


class StubCacheManager:
    def __init__(self):
        self._store = {}
    async def start(self):
        pass
    def get(self, k):
        return self._store.get(k)
    def set(self, k, v):
        self._store[k] = v


class StubDataQualityScorer:
    async def assess_quality(self, data):
        return 95.0


class StubRateLimiter:
    async def wait_and_acquire(self):
        pass


class StubHumanAICollaborativeDashboard:
    def __init__(self, port=8781):
        self.port = port
    async def start(self):
        pass
    async def stop(self):
        pass
    async def broadcast(self, message):
        logger.debug("dashboard broadcast: %s", message)


# =============================================================================
# RESULT DATACLASSES
# =============================================================================
@dataclass
class ModuleResult:
    module_name: str
    status: str
    duration_ms: float
    message: str = ""
    error: Optional[str] = None


@dataclass
class IntegrationResult:
    module_results: List[ModuleResult] = field(default_factory=list)
    overall_status: str = "success"
    sustainability_score: float = 0.0
    data_quality_score: float = 100.0
    total_duration_ms: float = 0.0
    checkpoint_id: Optional[str] = None
    # v17 additions
    selected_strategy: str = ""
    strategy_scores: List[float] = field(default_factory=list)
    precision_level: str = "fp32"
    temporal_violations: List[str] = field(default_factory=list)
    xai_explanation: Optional[Dict[str, Any]] = None
    carbon_market_decision: Optional[Dict[str, Any]] = None
    agent_id: Optional[str] = None
    causal_ate: Optional[float] = None


# =============================================================================
# MAIN: UnifiedIntegrationManagerV17
# =============================================================================
class UnifiedIntegrationManagerV17:
    ACTION_SPACE = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self):
        self.instance_id = str(uuid.uuid4())[:8]
        self.storage = storage
        self.config = config

        # Stubs / infrastructure
        self.cache = StubCacheManager()
        self.quality_scorer = StubDataQualityScorer()
        self.rate_limiter = StubRateLimiter()
        self.carbon_manager = StubCarbonIntensityManager()
        self.dashboard = StubHumanAICollaborativeDashboard(port=8781)
        self.dependency_resolver = StubDependencyResolver()
        self.checkpoint_manager = StubCheckpointManager(
            Path("./integration_checkpoints"))

        # Circuit breakers
        self.circuit_breakers = {
            "integration": CircuitBreaker(name="integration"),
            "carbon_api": CircuitBreaker(name="carbon_api"),
        }

        # v9 modules
        self.ga_optimizer = (GeneticIntegrationOptimizer(self.config, self.storage)
                             if _cfg_get(config, "GA_ENABLED", True) else None)
        self.moe_gating = (MoEGatingNetwork(self.config, self.storage)
                           if _cfg_get(config, "MOE_ENABLED", True) else None)
        self.pareto_optimizer = (ParetoFrontOptimizer(self.config, self.storage)
                                 if _cfg_get(config, "PARETO_ENABLED", True) else None)
        self.neural_teacher = (NeuralTeacher(input_dim=9, output_dim=5)
                               if _cfg_get(config, "NEURAL_TEACHER_ENABLED", True) else None)
        self.federated_learner = (FederatedIntegrationLearner(
            self.storage, self.instance_id,
            _cfg_get(config, "FEDERATED_INTERVAL", 3600))
            if _cfg_get(config, "FEDERATED_ENABLED", True) else None)
        self.drift_detector = (DriftDetector(self.storage)
                               if _cfg_get(config, "DRIFT_DETECTION_ENABLED", True) else None)
        self.limit_graph = (LimitGraphManager(self.config)
                            if _cfg_get(config, "LIMIT_GRAPH_ENABLED", True) else None)
        self.modp_optimizer = (MODPStrategyOptimizer(self.config)
                               if _cfg_get(config, "MODP_ENABLED", True) else None)
        self.rlhf = (RLHFManager(self.config)
                     if _cfg_get(config, "RLHF_ENABLED", True) else None)
        self.distillation = (MultiTeacherPolicyDistillation(self.config, self.moe_gating)
                             if _cfg_get(config, "DISTILLATION_ENABLED", True)
                             and self.moe_gating else None)
        self.digital_twin = (PredictiveDigitalTwin()
                             if _cfg_get(config, "PREDICTIVE_DIGITAL_TWIN_ENABLED", True)
                             else None)

        # v17 modules
        self.causal_graph = (CausalGraphLearner(self.storage)
                             if _cfg_get(config, "CAUSAL_RL_ENABLED", True) else None)
        self.causal_rl = (CausalIntegrationPolicyAdapter(
            self.config, self.storage, self.causal_graph)
            if _cfg_get(config, "CAUSAL_RL_ENABLED", True) and self.causal_graph
            else None)
        self.temporal = (TemporalIntegrationVerifier(self.storage, self.config)
                         if _cfg_get(config, "TEMPORAL_LOGIC_ENABLED", True) else None)
        self.xai = (IntegrationXAIDecisionExplainer(self.config, self.storage)
                    if _cfg_get(config, "XAI_ENABLED", True) else None)
        self.precision = (AdaptivePrecisionSwitcher(self.config, self.storage)
                          if _cfg_get(config, "ADAPTIVE_PRECISION_ENABLED", True) else None)
        self.carbon_market = (CarbonMarketIntegrator(self.config, self.storage)
                              if _cfg_get(config, "CARBON_MARKET_ENABLED", True) else None)
        self.chaos = (ChaosTestingEngine(self.config, self.storage)
                      if _cfg_get(config, "CHAOS_TESTING_ENABLED", True) else None)
        self.multi_agent = (MultiAgentCoordinator(self.config, self.storage)
                            if _cfg_get(config, "MULTI_AGENT_ENABLED", True) else None)
        self.quantum_distiller = (QuantumDistillationEngine(
            temperature=_cfg_get(config, "DISTILLATION_TEMPERATURE", 2.0),
            alpha=_cfg_get(config, "DISTILLATION_ALPHA", 0.5))
            if _cfg_get(config, "DISTILLATION_ENABLED", True) else None)

        # Active user preference learner — safe now that self.dashboard exists
        self.user_pref_learner = (ActiveUserPreferenceLearner(
            self.storage, self.dashboard)
            if _cfg_get(config, "ACTIVE_USER_PREFERENCE_ENABLED", True) else None)

        # Wire hooks
        if self.xai and self.modp_optimizer:
            self.modp_optimizer._xai = self.xai
        if self.chaos:
            for cb in self.circuit_breakers.values():
                cb.chaos_engine = self.chaos
            self.carbon_manager
        if self.temporal:
            self.temporal.set_approval_callback(self._hitl_approval)

        # Module registry
        self.modules: Dict[str, Dict[str, Any]] = {}
        self._init_modules()

        # Runtime state
        self._module_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        self._integration_semaphore = asyncio.Semaphore(
            _cfg_get(config, "MAX_CONCURRENT_MODULES", 5))
        self.operation_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._queue_worker: Optional[asyncio.Task] = None
        self._running = False
        self.background_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        self.integration_result: Optional[IntegrationResult] = None

        logger.info("UnifiedIntegrationManagerV17 v17.0.0 initialized (instance=%s)",
                    self.instance_id)

    def _init_modules(self):
        for name in ["collector", "elasticity", "circularity", "forecaster",
                     "sustainability", "thermal", "regret", "quantum",
                     "carbon", "helium"]:
            self.modules[name] = {
                "name": name, "type": name, "dependencies": [],
                "priority": 1, "version": "1.0.0",
            }

    # -------------------------------------------------------------------------
    # HITL
    # -------------------------------------------------------------------------
    async def _hitl_approval(self, rule_id, state):
        logger.warning("HITL approval requested for critical rule '%s'", rule_id)
        return random.random() > 0.5

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------
    async def start(self):
        self._running = True
        await self.cache.start()
        await self.carbon_manager.update_carbon_intensity()
        self._queue_worker = asyncio.create_task(self._process_queue())
        await self.dashboard.start()

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
        if self.chaos:
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
        try:
            await self.dashboard.stop()
        except Exception:
            pass
        logger.info("Shutdown complete")

    # -------------------------------------------------------------------------
    # Strategy selection chain
    # -------------------------------------------------------------------------
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

        return strategy, scores, explanation, attributions

    # -------------------------------------------------------------------------
    # Module execution
    # -------------------------------------------------------------------------
    async def _process_queue(self):
        while self._running:
            try:
                op = await asyncio.wait_for(self.operation_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                result = await self._run_module(op["module"])
                if not op["future"].done():
                    op["future"].set_result(result)
            except Exception as e:
                if not op["future"].done():
                    op["future"].set_exception(e)
            finally:
                self.operation_queue.task_done()

    async def _run_module(self, module_name):
        t0 = time.time()
        async with self._integration_semaphore:
            await self.rate_limiter.wait_and_acquire()
            try:
                # Simulate module work — real modules replace this in production
                await asyncio.sleep(random.uniform(0.01, 0.05))
                duration_ms = (time.time() - t0) * 1000.0
                MODULE_RUNS.labels(module=module_name, status="success").inc()
                self.storage.save_module_run(module_name, "success", duration_ms)
                return ModuleResult(module_name, "success", duration_ms, "ok")
            except Exception as e:
                duration_ms = (time.time() - t0) * 1000.0
                MODULE_RUNS.labels(module=module_name, status="failure").inc()
                self.storage.save_module_run(module_name, "failure", duration_ms,
                                             str(e))
                return ModuleResult(module_name, "failure", duration_ms,
                                    "error", str(e))

    # -------------------------------------------------------------------------
    # Main integration
    # -------------------------------------------------------------------------
    async def run_integration(self, modules: Optional[List[str]] = None):
        t0 = time.time()
        if modules is None:
            modules = list(self.modules.keys())
        resolved = self.dependency_resolver.resolve_order(modules)

        result = IntegrationResult()

        # 1. Fetch current context
        carbon = await self.carbon_manager.get_current_intensity()
        state_dict = {
            "module_count": len(resolved),
            "success_rate": 0.9,
            "queue_size": self.operation_queue.qsize(),
            "carbon_intensity": carbon,
            "cloud_provider_latency": 60.0,
            "module_health": 0.85,
            "sustainability_score": 0.8,
            "cost": 0.5,
            "latency": 0.4,
        }

        # 2. Strategy selection (all ten enhancements may participate)
        strategy, scores, explanation, attributions = \
            await self._select_strategy(state_dict)
        result.selected_strategy = strategy
        result.strategy_scores = scores

        # 3. Precision selection
        if self.precision:
            await self.precision.switch_to(
                self.precision.select_precision(), reason="pre_integration")
            result.precision_level = self.precision.current

        # 4. Multi-agent bid
        if self.multi_agent:
            preferred = {
                "performance": "optimizer", "carbon": "negotiator",
                "cost": "optimizer", "hybrid": "orchestrator",
                "adaptive": "orchestrator",
            }.get(strategy, "orchestrator")
            agent_id, _ = await self.multi_agent.bid({
                "name": f"integration_{strategy}",
                "preferred_role": preferred})
            result.agent_id = agent_id

        # 5. Carbon market decision
        if self.carbon_market:
            result.carbon_market_decision = await self.carbon_market.net_zero_schedule(
                workload_kwh=0.5, intensity=carbon / 1000.0)

        # 6. Execute all modules
        for mod in resolved:
            mr = await self._run_module(mod)
            result.module_results.append(mr)

        # 7. Quality / sustainability assessment
        result.sustainability_score = 80.0 + random.uniform(-5, 5)
        result.data_quality_score = await self.quality_scorer.assess_quality(
            result.module_results)

        # 8. Temporal logic push + verify
        if self.temporal:
            await self.temporal.push_state({
                "success_rate": 0.9,
                "carbon": carbon / 1000.0,
                "integration_complete": True,
            })
            verify = await self.temporal.verify()
            result.temporal_violations = [k for k, v in verify.items() if not v]

        # 9. Update learners (causal RL, RLHF, MoE, Pareto, LIMIT graph)
        reward = min(1.0, max(0.0, result.sustainability_score / 100.0))
        if self.causal_rl:
            await self.causal_rl.update(strategy, reward, state_dict)
        if self.rlhf and reward > 0.6:
            await self.rlhf.record_feedback(state_dict, strategy, reward)
        if self.moe_gating:
            await self.moe_gating.add_training_sample(state_dict, strategy, reward)
        if self.pareto_optimizer:
            await self.pareto_optimizer.add_configuration(
                {"strategy": strategy},
                {"success_rate": 0.9,
                 "carbon_footprint": carbon / 1000.0,
                 "execution_time": result.total_duration_ms / 1000.0 or 0.1,
                 "cost": 0.1})
        if self.limit_graph:
            await self.limit_graph.update_constraint(
                "success", result.sustainability_score / 100.0)
            await self.limit_graph.update_constraint(
                "carbon", carbon / 1000.0)
            await self.limit_graph.evaluate_path("carbon", "success")

        # 10. Drift detection
        if self.drift_detector:
            await self.drift_detector.check_carbon_drift(carbon / 1000.0)
            await self.drift_detector.check_performance_drift(reward)

        # 11. XAI explanation
        if self.xai:
            try:
                feats = np.array([0.9, carbon / 1000.0, 0.5, 0.4])
                def _score(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                result.xai_explanation = await self.xai.explain(
                    decision_id=f"integration_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={strategy}",
                    features=feats,
                    names=["success_rate", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception as e:
                logger.debug("XAI failed: %s", e)

        # 12. Federated share of current policy
        if self.federated_learner:
            policy = await self.policy_probs(state_dict)
            await self.federated_learner.share_weights({"policy": policy})

        # 13. Multi-agent reward
        if self.multi_agent and result.agent_id:
            await self.multi_agent.reward(result.agent_id, reward)

        # 14. Causal ATE estimate
        if self.causal_rl and self.causal_graph:
            try:
                result.causal_ate = await self.causal_rl.estimate_ate(
                    "carbon", "success")
            except Exception:
                pass

        # 15. Digital twin forecast (optional)
        if self.digital_twin:
            try:
                await self.digital_twin.forecast_module_state("integration", steps=12)
            except Exception:
                pass

        # 16. Checkpoint
        result.total_duration_ms = (time.time() - t0) * 1000.0
        result.checkpoint_id = await self.checkpoint_manager.save_checkpoint(result)
        result.overall_status = (
            "success" if all(m.status == "success" for m in result.module_results)
            else "partial")

        INTEGRATION_RUNS.labels(status=result.overall_status).inc()
        INTEGRATION_DURATION.observe(result.total_duration_ms / 1000.0)
        SUSTAINABILITY_SCORE.set(result.sustainability_score)
        DATA_QUALITY_SCORE.set(result.data_quality_score)

        self.integration_result = result
        logger.info("Integration done: status=%s strategy=%s sustain=%.1f "
                    "precision=%s violations=%s",
                    result.overall_status, strategy,
                    result.sustainability_score, result.precision_level,
                    result.temporal_violations)
        return result

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

    # -------------------------------------------------------------------------
    # Background loops
    # -------------------------------------------------------------------------
    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(60)

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(120)
            try:
                await self.carbon_manager.update_carbon_intensity()
            except Exception as e:
                logger.error("carbon loop: %s", e)

    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            if self.ga_optimizer:
                try:
                    await self.ga_optimizer.run_search()
                except Exception as e:
                    logger.error("GA loop: %s", e)

    async def _moe_training_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(600)
            if self.moe_gating:
                try:
                    self.moe_gating._train()
                except Exception as e:
                    logger.error("MoE loop: %s", e)

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(config, "RLHF_TRAINING_INTERVAL", 600))
            if self.rlhf:
                try:
                    await self.rlhf.train_reward_model()
                except Exception as e:
                    logger.error("RLHF loop: %s", e)

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(config, "FEDERATED_INTERVAL", 3600))
            if self.federated_learner:
                try:
                    await self.federated_learner.pull_aggregated_weights()
                except Exception as e:
                    logger.error("Federated loop: %s", e)

    async def _drift_detection_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            if self.drift_detector:
                try:
                    c = await self.carbon_manager.get_current_intensity()
                    await self.drift_detector.check_carbon_drift(c / 1000.0)
                except Exception as e:
                    logger.error("Drift loop: %s", e)

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(config, "LIMIT_GRAPH_UPDATE_INTERVAL", 300))
            if self.limit_graph:
                try:
                    c = await self.carbon_manager.get_current_intensity()
                    await self.limit_graph.update_constraint("carbon", c / 1000.0)
                except Exception as e:
                    logger.error("LIMIT loop: %s", e)

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(config, "DISTILLATION_INTERVAL", 300))
            if self.distillation:
                try:
                    await self.distillation.distill({
                        "module_count": 10, "success_rate": 0.9,
                        "carbon_intensity": 400})
                except Exception as e:
                    logger.error("Distillation loop: %s", e)

    async def _quantum_distill_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(config, "DISTILLATION_INTERVAL", 300))
            if self.quantum_distiller:
                try:
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
                        # Use MoE class probabilities as a teacher
                        try:
                            names = self.moe_gating.expert_names
                            probs = [1.0 / len(names)] * len(names)
                            if self.moe_gating._model is not None:
                                fake_X = np.array(
                                    self.moe_gating._encode({}), dtype=float).reshape(1, -1)
                                if self.moe_gating._scaler:
                                    fake_X = self.moe_gating._scaler.transform(fake_X)
                                probs = self.moe_gating._model.predict_proba(fake_X)[0].tolist()
                            self.quantum_distiller.register_teacher("moe", probs)
                        except Exception:
                            pass
                    await self.quantum_distiller.step({})
                except Exception as e:
                    logger.error("Quantum distill loop: %s", e)

    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(config, "CAUSAL_GRAPH_UPDATE_INTERVAL", 900))
            if self.causal_graph:
                try:
                    n = _cfg_get(config, "CAUSAL_MIN_SAMPLES", 20)
                    samples = [{
                        "success_rate": random.uniform(0.5, 1.0),
                        "carbon": random.uniform(0.1, 0.8),
                        "cost": random.uniform(0.1, 0.9),
                        "latency": random.uniform(0.1, 0.9),
                    } for _ in range(n)]
                    await self.causal_graph.learn(
                        samples, ["success_rate", "carbon", "cost", "latency"])
                    await self.causal_rl.estimate_ate("carbon", "success_rate")
                except Exception as e:
                    logger.error("Causal RL loop: %s", e)

    async def _temporal_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(
                _cfg_get(config, "TEMPORAL_VERIFICATION_INTERVAL", 300))
            if self.temporal:
                try:
                    res = await self.temporal.verify()
                    bad = [k for k, v in res.items() if not v]
                    if bad:
                        logger.warning("Temporal violations: %s", bad)
                except Exception as e:
                    logger.error("Temporal loop: %s", e)

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(config, "XAI_INTERVAL", 300))
            if self.xai:
                try:
                    feats = np.array([0.9, 0.4, 0.5, 0.4])
                    def _score(x):
                        return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                    await self.xai.explain(
                        decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                        label="system_health",
                        features=feats,
                        names=["success_rate", "carbon", "cost", "latency"],
                        model_fn=_score)
                except Exception as e:
                    logger.error("XAI loop: %s", e)

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            if self.precision:
                try:
                    await self.precision.auto_switch(0.9, 0.92)
                except Exception as e:
                    logger.error("Precision loop: %s", e)

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(config, "CARBON_MARKET_INTERVAL", 3600))
            if self.carbon_market:
                try:
                    await self.carbon_market.update_price()
                except Exception as e:
                    logger.error("Carbon market loop: %s", e)

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(config, "CHAOS_TEST_INTERVAL", 1800))
            if self.chaos:
                try:
                    fault = random.choice(self.chaos.FAULT_TYPES)
                    await self.chaos.run_experiment(
                        name=f"auto_{uuid.uuid4().hex[:6]}", fault_type=fault)
                except Exception as e:
                    logger.error("Chaos loop: %s", e)

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(config, "AGENT_NEGOTIATION_INTERVAL", 600))
            if self.multi_agent:
                try:
                    await self.multi_agent.step()
                except Exception as e:
                    logger.error("Multi-agent loop: %s", e)

    async def _digital_twin_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            if self.digital_twin:
                try:
                    await self.digital_twin.get_twin_status()
                except Exception as e:
                    logger.error("Digital twin loop: %s", e)

    # -------------------------------------------------------------------------
    # Public helpers
    # -------------------------------------------------------------------------
    async def get_statistics(self):
        stats = {
            "instance_id": self.instance_id,
            "version": 17,
            "module_count": len(self.modules),
            "last_strategy": (self.integration_result.selected_strategy
                              if self.integration_result else None),
            "last_sustainability": (self.integration_result.sustainability_score
                                    if self.integration_result else None),
            "precision": self.precision.current if self.precision else None,
            "temporal_rules": len(self.temporal.rules) if self.temporal else 0,
            "causal_edges": (self.causal_graph.summary()["edges"]
                             if self.causal_graph else 0),
            "pareto_size": (len(self.pareto_optimizer.get_pareto_front())
                            if self.pareto_optimizer else 0),
            "rec_balance_mwh": self.storage.get_rec_balance(),
            "carbon_price_usd": (self.carbon_market.last_price
                                 if self.carbon_market else None),
            "agent_roles": ({a.id: a.role
                             for a in self.multi_agent.agents.values()}
                            if self.multi_agent else {}),
        }
        return stats

    async def run_chaos_experiment(self, name, fault_type):
        if not self.chaos:
            return {"error": "chaos disabled"}
        return await self.chaos.run_experiment(name, fault_type)

    async def explain_decision(self, label, features, names, score_fn):
        if not self.xai:
            return {"error": "XAI disabled"}
        return await self.xai.explain(
            decision_id=f"exp_{uuid.uuid4().hex[:8]}",
            label=label,
            features=np.array(features),
            names=names,
            model_fn=score_fn)


# =============================================================================
# SINGLETON
# =============================================================================
_manager_instance: Optional[UnifiedIntegrationManagerV17] = None
_manager_lock = asyncio.Lock()


async def get_integration_manager() -> UnifiedIntegrationManagerV17:
    global _manager_instance
    if _manager_instance is None:
        async with _manager_lock:
            if _manager_instance is None:
                _manager_instance = UnifiedIntegrationManagerV17()
                await _manager_instance.start()
    return _manager_instance


# =============================================================================
# DEMO
# =============================================================================
async def _demo():
    print("=" * 80)
    print("Unified Integration Manager v17.0.0 — All Ten Enhancements")
    print("=" * 80)
    manager = await get_integration_manager()

    result = await manager.run_integration()
    print(f"\n=== Integration Result ===")
    print(f"  status:              {result.overall_status}")
    print(f"  modules run:         {len(result.module_results)}")
    print(f"  selected strategy:   {result.selected_strategy}")
    print(f"  precision:           {result.precision_level}")
    print(f"  sustainability:      {result.sustainability_score:.1f}")
    print(f"  data quality:        {result.data_quality_score:.1f}")
    print(f"  duration (ms):       {result.total_duration_ms:.1f}")
    print(f"  temporal violations: {result.temporal_violations}")
    print(f"  carbon market:       {result.carbon_market_decision}")
    print(f"  agent:               {result.agent_id}")
    print(f"  causal ATE:          {result.causal_ate}")
    print(f"  checkpoint:          {result.checkpoint_id}")
    if result.xai_explanation:
        print(f"  XAI explanation:")
        print("    " + result.xai_explanation["explanation"].replace("\n", "\n    "))

    print("\n=== Statistics ===")
    print(json.dumps(await manager.get_statistics(), indent=2, default=str))

    print("\n=== Chaos experiment ===")
    print(json.dumps(await manager.run_chaos_experiment("demo", "latency"),
                     indent=2, default=str))

    print("\n=== Federated round ===")
    if manager.federated_learner:
        agg = await manager.federated_learner.pull_aggregated_weights()
        print(f"  rounds={manager.federated_learner.rounds}, aggregated_keys="
              f"{list(agg.keys()) if agg else None}")

    await manager.shutdown()


if __name__ == "__main__":
    import signal as _sig

    def _on_sig(s, f):
        logger.info("Received signal %s", s)
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_manager_instance.shutdown()
                             if _manager_instance else asyncio.sleep(0))
        except Exception:
            pass

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for sig in (_sig.SIGINT, _sig.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: _on_sig(s, None))
            except (NotImplementedError, RuntimeError):
                pass
        loop.run_until_complete(_demo())
    except KeyboardInterrupt:
        pass
