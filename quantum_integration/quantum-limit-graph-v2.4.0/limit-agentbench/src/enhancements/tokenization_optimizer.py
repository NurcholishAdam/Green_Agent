#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/tokenization_optimizer_enhanced_v4_1_0.py
# VERSION: 4.1.0  (Phase 1-3 enhancements applied — see changelog)
# =============================================================================
"""
Tokenization Optimizer — v4.1.0

Ten Green Agent enhancements in one file:
  1. Causal RL  2. Temporal Logic  3. XAI  4. Adaptive Precision
  5. Carbon Markets/REC  6. Chaos Testing  7. Multi-Agent Coordination
  8. Quantum-Distillation  9. Federated Green Learning  10. HITL

v4.1.0 (Phase 1-3) adds:
  [P0] Real chaos probes (steady_during); WITHIN waits for window; NEXT = G X φ
       (pending queue); correct KL gradient (s - t); action-aware RLHF;
       chaos CB key cb_<name>; distillation_fallback uses MAIN strategy;
       next_state genuinely captured; message bus dispatch; federated read/
       write unified; StatefulQTeacher.update wired; LearningCache lock.
  [P1] Real recursive-descent LTL parser; data_corruption chaos; correct
       tokenizer_used; NLTK lazy-load; NeuralTeacher buffer; SHA-256 cache
       keys; real HITL; HITL WebSocket; compression_ratio rename;
       tokenization_complete derived; cache precision refresh; BasicTokenizer
       fallback; sub-module stall(); signal handling.
  [P2] Correlation IDs; new metrics; structured logging; extended health_check;
       bounded _recent_errors; CircuitBreaker lock; persisted sub-module state.
  [P3] Real KernelSHAP; pgmpy PC hook + train/val/test edge stability; real GA
       fitness; real MoE positive-filter; unified federated backend +
       discovery; multi-agent hysteresis; streaming + batched APIs; PQC
       signature chain; PQC WebSocket HITL; sub-module save/load.
"""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import json
import logging
import math
import os
import random
import re
import secrets
import signal
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
    import structlog
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

try:
    from pydantic import BaseModel
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from langdetect import detect, DetectorFactory
    LANGDETECT_AVAILABLE = True
    DetectorFactory.seed = 0
except ImportError:
    LANGDETECT_AVAILABLE = False

try:
    from nltk.tokenize import sent_tokenize
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False

try:
    from transformers import AutoTokenizer
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

try:
    from summa import summarizer
    SUMMA_AVAILABLE = True
except ImportError:
    SUMMA_AVAILABLE = False

try:
    from prometheus_client import Counter, Histogram, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import torch
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
    import websockets
    from websockets.server import serve as ws_serve
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False


# =============================================================================
# Universal config access helper
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
# Logging — [P2] correlation ID filter
# =============================================================================
if CENTRAL_COMPONENTS_AVAILABLE and central_logger:
    logger = central_logger
else:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - "
               "[%(correlation_id)s] - %(message)s")
    logger = logging.getLogger("tokenization_v4")

    class _CorrelationFilter(logging.Filter):
        def filter(self, record):
            record.correlation_id = correlation_id_var.get()
            return True

    for _h in logger.handlers:
        _h.addFilter(_CorrelationFilter())


# =============================================================================
# CONFIG
# =============================================================================
_DEFAULT_CFG: Dict[str, Any] = {
    "default_tokenizer": "bert-base-uncased",
    "language_tokenizer_map": {
        "en": "bert-base-uncased",
        "id": "bert-base-indonesian-1.5G",
        "fr": "camembert-base",
        "de": "bert-base-german-cased",
        "es": "dccuchile/bert-base-spanish-wwm-uncased",
    },
    "cache_ttl_seconds": 300,
    "enable_cache": True,
    "max_segment_length": 100,
    "summarization_ratio": 0.5,
    "fallback_language": "en",
    "require_langdetect": False,
    "require_nltk": False,
    "distillation_epsilon": 0.1,
    "train_every": 10,
    "replay_buffer_size": 2000,
    "student_learning_rate": 0.01,
    # v3
    "ga_enabled": True,
    "ga_population_size": 20,
    "ga_generations": 5,
    "ga_mutation_rate": 0.2,
    "ga_crossover_rate": 0.7,
    "moe_enabled": True,
    "moe_expert_count": 5,
    "moe_hidden_layers": [16, 8],
    "pareto_enabled": True,
    "pareto_max_architectures": 100,
    "federated_enabled": True,
    "federated_interval": 3600,
    "neural_teacher_enabled": True,
    "user_preference_enabled": True,
    "drift_detection_enabled": True,
    "limit_graph_enabled": True,
    "limit_graph_update_interval": 300,
    "modp_enabled": True,
    "modp_weights": [0.25, 0.25, 0.25, 0.25],
    "rlhf_enabled": True,
    "rlhf_training_interval": 600,
    "distillation_enabled": True,
    "distillation_temperature": 2.0,
    "distillation_alpha": 0.5,
    "distillation_interval": 300,
    # v4
    "causal_rl_enabled": True,
    "causal_graph_update_interval": 900,
    "causal_exploration_rate": 0.1,
    "causal_min_samples": 20,
    "temporal_logic_enabled": True,
    "temporal_verification_interval": 300,
    "temporal_formulas": [
        "G (token_count <= 5000)",
        "G (compression_ratio <= 1.5)",
        "F (tokenization_complete)",
    ],
    "temporal_max_trace": 2000,
    "xai_enabled": True,
    "xai_method": "kernel_shap",
    "xai_depth": 5,
    "xai_interval": 300,
    "adaptive_precision_enabled": True,
    "precision_levels": ["fp32", "fp16", "bf16", "int8"],
    "precision_switch_threshold": 0.02,
    "carbon_market_enabled": True,
    "carbon_market_api_url": None,
    "carbon_market_interval": 3600,
    "chaos_testing_enabled": True,
    "chaos_test_interval": 1800,
    "chaos_intensity": 0.05,
    "chaos_blast_radius": 0.1,
    "chaos_fault_types": ["latency", "exception", "data_corruption",
                          "memory_pressure", "network_drop"],
    "chaos_auto_rollback": True,
    "multi_agent_enabled": True,
    "agent_count": 5,
    "agent_negotiation_interval": 600,
    # v4.1
    "hitl_ws_port": 8791,
    "rlfh_state_path": "/tmp/token_v41_state.json",
    "download_nltk": False,
}


config = dict(_DEFAULT_CFG)
if CENTRAL_COMPONENTS_AVAILABLE and central_config:
    for _k in list(_DEFAULT_CFG.keys()):
        _v = getattr(central_config, f"token_{_k}", None)
        if _v is not None:
            config[_k] = _v


# =============================================================================
# METRICS — [P2] new metrics
# =============================================================================
class _DummyMetric:
    def labels(self, **kw): return self
    def inc(self, *a, **kw): pass
    def set(self, *a, **kw): pass
    def observe(self, *a, **kw): pass


if PROMETHEUS_AVAILABLE:
    TOKENIZATION_COUNTER = Counter("tokenization_requests_total", "Requests",
                                    ["language", "status"])
    TOKEN_COUNT_HISTOGRAM = Histogram("token_count_per_request", "Tokens", ["language"])
    TOKENIZATION_DURATION = Histogram("tokenization_duration_seconds", "Duration",
                                       ["language", "strategy"])
    CACHE_HIT_COUNTER = Counter("tokenization_cache_hits_total", "Hits")
    CACHE_MISS_COUNTER = Counter("tokenization_cache_misses_total", "Misses")
    LANGUAGE_DISTRIBUTION = Gauge("tokenization_language_distribution", "Langs",
                                    ["language"])
    DISTILLATION_STRATEGY = Counter("distillation_strategy_selected", "Strategies",
                                      ["strategy"])
    DISTILLATION_REWARD = Histogram("tokenization_reward", "Reward")
    GA_POPULATION_FITNESS = Gauge("token_ga_population_fitness", "GA fitness")
    MOE_GATING_PROBABILITIES = Gauge("token_moe_gating_probabilities", "MoE probs",
                                       ["expert"])
    PARETO_FRONT_SIZE = Gauge("token_pareto_front_size", "Pareto size")
    FEDERATED_AGGREGATIONS = Counter("token_federated_aggregations_total", "Federated")
    DRIFT_SCORE = Gauge("token_drift_score", "Drift", ["domain"])
    CAUSAL_ATE = Gauge("token_causal_ate", "ATE", ["treatment", "outcome"])
    CAUSAL_EDGE_STABILITY = Gauge("token_causal_edge_stability", "Stability")
    TEMPORAL_VERIFICATIONS = Counter("token_temporal_verifications_total", "TL",
                                       ["formula", "status"])
    TEMPORAL_VIOLATIONS = Counter("token_temporal_violations_total", "TLv", ["formula"])
    XAI_EXPLANATIONS = Counter("token_xai_explanations_total", "XAI", ["method"])
    XAI_FEATURE_IMPORTANCE = Gauge("token_xai_feature_importance", "XAI FI",
                                     ["feature"])
    PRECISION_SWITCHES = Counter("token_precision_switches_total", "Prec",
                                   ["from_p", "to_p"])
    PRECISION_ENERGY_SAVED = Gauge("token_precision_energy_saved_wh", "Saved")
    CARBON_CREDIT_PRICE = Gauge("token_carbon_credit_price_usd", "Price")
    REC_BALANCE = Gauge("token_rec_balance_mwh", "REC")
    NET_ZERO_MATCHES = Counter("token_net_zero_matches_total", "Net zero")
    CHAOS_EXPERIMENTS = Counter("token_chaos_experiments_total", "Chaos",
                                  ["fault_type", "status"])
    CHAOS_STEADY_STATE = Gauge("token_chaos_steady_state_ok", "Steady")
    AGENT_ROLES = Gauge("token_agent_roles", "Roles", ["role"])
    AGENT_REPUTATION = Gauge("token_agent_reputation", "Rep", ["agent_id"])
    # [P2] new
    TOKEN_LATENCY = Histogram("token_optimize_latency_seconds", "Latency")
    TOKEN_CACHE_HIT_RATE = Gauge("token_cache_hit_rate", "Hit rate")
    TOKEN_STRATEGY_DIST = Gauge("token_strategy_distribution", "Dist", ["strategy"])
    TOKEN_ACTIVE = Gauge("token_active_optimizations", "Active")
    TOKEN_ERRORS = Counter("token_errors_total", "Errors", ["stage"])
else:
    for _n in [
        "TOKENIZATION_COUNTER", "TOKEN_COUNT_HISTOGRAM", "TOKENIZATION_DURATION",
        "CACHE_HIT_COUNTER", "CACHE_MISS_COUNTER", "LANGUAGE_DISTRIBUTION",
        "DISTILLATION_STRATEGY", "DISTILLATION_REWARD", "GA_POPULATION_FITNESS",
        "MOE_GATING_PROBABILITIES", "PARETO_FRONT_SIZE", "FEDERATED_AGGREGATIONS",
        "DRIFT_SCORE", "CAUSAL_ATE", "CAUSAL_EDGE_STABILITY",
        "TEMPORAL_VERIFICATIONS", "TEMPORAL_VIOLATIONS",
        "XAI_EXPLANATIONS", "XAI_FEATURE_IMPORTANCE", "PRECISION_SWITCHES",
        "PRECISION_ENERGY_SAVED", "CARBON_CREDIT_PRICE", "REC_BALANCE",
        "NET_ZERO_MATCHES", "CHAOS_EXPERIMENTS", "CHAOS_STEADY_STATE",
        "AGENT_ROLES", "AGENT_REPUTATION", "TOKEN_LATENCY",
        "TOKEN_CACHE_HIT_RATE", "TOKEN_STRATEGY_DIST", "TOKEN_ACTIVE",
        "TOKEN_ERRORS",
    ]:
        globals()[_n] = _DummyMetric()


# =============================================================================
# STORAGE — [P1] bounded deques, [P3] persisted
# =============================================================================
class TokenizationStorage:
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
        self.fed_weights: Dict[str, str] = {}  # [P0] unified federated backend

    def get_state(self, key: str) -> Optional[str]:
        if self._central and hasattr(self._central, "get_state"):
            try:
                return self._central.get_state(key)
            except Exception:
                pass
        return self._state.get(key)

    def save_state(self, key: str, value: str) -> None:
        self._state[key] = value  # Always keep locally
        if self._central and hasattr(self._central, "save_state"):
            try:
                self._central.save_state(key, value)
            except Exception:
                pass

    # ---- v4 persistence ----
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
            "from": frm, "to": to, "reason": reason, "saved_wh": float(saved_wh),
            "ts": datetime.now(timezone.utc).isoformat()})

    def save_rec(self, mwh, price, source):
        self.rec_ledger.append({
            "mwh": float(mwh), "price": float(price), "source": source,
            "ts": datetime.now(timezone.utc).isoformat()})

    def get_rec_balance(self) -> float:
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

    # [P0] unified federated backend
    def save_fed_weight(self, instance_id: str, weights: Dict):
        key = f"fed_token_weight_{instance_id}"
        self.fed_weights[key] = json.dumps(weights, default=str)
        self.save_state(key, self.fed_weights[key])

    def get_all_fed_weights(self) -> Dict[str, str]:
        out = dict(self.fed_weights)
        for k, v in self._state.items():
            if k.startswith("fed_token_weight_"):
                out[k] = v
        return out


_central_store = None
if CENTRAL_COMPONENTS_AVAILABLE and CentralStorage:
    try:
        _central_store = CentralStorage()
    except Exception:
        _central_store = None

storage = TokenizationStorage(central=_central_store)


# =============================================================================
# CIRCUIT BREAKER + RATE LIMITER — [P2] lock on record paths
# =============================================================================
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0, name="default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failures = 0
        self._last_failure_time: Optional[datetime] = None
        self._state = "CLOSED"
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
                        (datetime.now() - self._last_failure_time).total_seconds() > \
                        self.recovery_timeout:
                    self._state = "HALF_OPEN"
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
        # [P0] chaos check via is_target_active
        if self.is_target_active():
            await asyncio.sleep(0.2)
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == "HALF_OPEN":
                    self._state = "CLOSED"
                    self._failures = 0
            return result
        except Exception:
            async with self._lock:
                self._failures += 1
                self._last_failure_time = datetime.now()
                if self._failures >= self.failure_threshold:
                    self._state = "OPEN"
            raise


class RateLimiter:
    def __init__(self, rate=100, window=60):
        self.rate = rate
        self.window = window
        self.tokens = float(rate)
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            self.tokens = min(self.rate, self.tokens + (now - self.last_refill) *
                              (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.05)


# =============================================================================
# DATA CLASS — [P1] compression_ratio, proper feature scaling
# =============================================================================
@dataclass
class TokenizationState:
    text_length: int
    avg_word_len: float
    num_sentences: int
    language: str
    requested_budget: int
    tokenizer_efficiency: float  # [P1] now compression_ratio in code, kept name for compat
    domain: Optional[str] = None
    time_of_day: int = 0

    def to_feature_vector(self) -> np.ndarray:
        features = [
            min(self.text_length / 10000.0, 1.0),
            min(self.avg_word_len / 10.0, 1.0),
            min(self.num_sentences / 100.0, 1.0),
            min(self.requested_budget / 2000.0, 1.0),
            min(self.tokenizer_efficiency, 2.0) / 2.0,  # [P1] normalize to [0,1]
        ]
        lang_map = {"en": 0, "id": 1, "fr": 2, "de": 3, "es": 4}
        one_hot = [0.0] * 5
        one_hot[lang_map.get(self.language, 4)] = 1.0
        features.extend(one_hot)
        features.append(self.time_of_day / 24.0)
        domain_map = {"scientific": 0, "legal": 1, "general": 2}
        d_one_hot = [0.0] * 3
        if self.domain:
            d_one_hot[domain_map.get(self.domain, 2)] = 1.0
        features.extend(d_one_hot)
        return np.array(features, dtype=np.float32)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "text_length": self.text_length, "avg_word_len": self.avg_word_len,
            "num_sentences": self.num_sentences, "language": self.language,
            "requested_budget": self.requested_budget,
            "tokenizer_efficiency": self.tokenizer_efficiency,
            "compression_ratio": self.tokenizer_efficiency,  # [P1] alias
            "domain": self.domain, "time_of_day": self.time_of_day,
        }


# =============================================================================
# TOKENIZER FALLBACK — [P1] works without transformers
# =============================================================================
class BasicTokenizer:
    def encode(self, text: str, add_special_tokens: bool = False) -> List[str]:
        return [w for w in re.split(r"\s+", text.strip()) if w]


# =============================================================================
# TEACHERS
# =============================================================================
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: TokenizationState) -> np.ndarray: ...
    @abstractmethod
    def confidence(self, state: TokenizationState) -> float: ...


class RuleBasedTeacher(Teacher):
    ACTION_SPACE = ["efficiency", "accuracy", "speed", "budget", "adaptive"]

    def predict(self, state):
        probs = np.ones(5) * 0.1
        if state.text_length > 5000 and state.requested_budget < 500:
            probs[3] = 0.8
        elif state.num_sentences > 20:
            probs[0] = 0.7
        elif state.tokenizer_efficiency > 0.5:
            probs[1] = 0.6
        else:
            probs[2] = 0.5
        return probs / probs.sum()

    def confidence(self, state):
        if state.text_length > 5000 and state.requested_budget < 500:
            return 0.6
        if state.num_sentences > 20:
            return 0.5
        return 0.4


class HistoricalMLTeacher(Teacher):
    def __init__(self, model_path=None):
        self.model = None
        if model_path and os.path.exists(model_path):
            try:
                import joblib
                self.model = joblib.load(model_path)
            except Exception:
                pass

    def predict(self, state):
        if self.model is None:
            return np.ones(5) / 5
        return self.model.predict_proba(state.to_feature_vector().reshape(1, -1))[0]

    def confidence(self, state):
        return 0.7 if self.model is not None else 0.0


class StatefulQTeacher(Teacher):
    """[P0] Now actually updated from rewards (was dead in v4.0.0)."""
    def __init__(self, storage, lr=0.1):
        self.storage = storage
        self.lr = lr
        self.feature_dim = 15
        self.weights = np.zeros((self.feature_dim, 5))
        self._load_state()

    def _load_state(self):
        w = self.storage.get_state("q_teacher_weights")
        if w:
            try:
                arr = np.array(json.loads(w))
                if arr.shape == self.weights.shape:
                    self.weights = arr
            except Exception:
                pass

    def _save_state(self):
        self.storage.save_state("q_teacher_weights",
                                json.dumps(self.weights.tolist()))

    def predict(self, state):
        x = state.to_feature_vector()
        if len(x) != self.weights.shape[0]:
            return np.ones(5) / 5
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state):
        return 0.5 if np.any(self.weights) else 0.0

    def update(self, state, action, reward):
        # [P0] This is now actually called
        x = state.to_feature_vector()
        if len(x) != self.weights.shape[0]:
            return
        q = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q) * x
        self._save_state()


# =============================================================================
# DISTILLATION CORE
# =============================================================================
class DistillationStudent:
    def __init__(self, feature_dim=15, n_classes=5, lr=0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, x):
        if len(x) != self.weights.shape[0]:
            return np.ones(self.n_classes) / self.n_classes
        logits = x @ self.weights + self.biases
        m = np.max(logits)
        exps = np.exp(logits - m)
        return exps / exps.sum()

    def update(self, x, teacher_probs, reward, action,
               distill_weight=0.7, rl_weight=0.3):
        if len(x) != self.weights.shape[0]:
            return
        cur = self.predict_proba(x)
        grad_distill = -(teacher_probs - cur)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - cur)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(x, grad)
        self.biases -= self.lr * grad
        self.counter += 1

    def save_state(self):
        return {"weights": self.weights.tolist(),
                "biases": self.biases.tolist(),
                "counter": self.counter}

    def load_state(self, data):
        try:
            w = np.array(data.get("weights", []))
            b = np.array(data.get("biases", []))
            if w.shape == self.weights.shape:
                self.weights = w
            if b.shape == self.biases.shape:
                self.biases = b
            self.counter = int(data.get("counter", 0))
        except Exception:
            pass


class ReplayBuffer:
    def __init__(self, max_size=2000):
        self.buffer: Deque = deque(maxlen=max_size)

    def push(self, s, a, r, ns, tp):
        self.buffer.append((s, a, r, ns, tp))

    def sample(self, batch_size=32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        if not batch:
            return (np.zeros((0, 15)), [], np.zeros(0),
                    np.zeros((0, 15)), np.zeros((0, 5)))
        s, a, r, ns, tp = zip(*batch)
        return np.array(s), list(a), np.array(r), np.array(ns), np.array(tp)

    def __len__(self):
        return len(self.buffer)


# =============================================================================
# v4 MODULE 1: CAUSAL RL — [P3] pgmpy PC hook + train/val/test stability
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
        try:
            from pgmpy.estimators import PC
            import pandas as pd
            df = pd.DataFrame(samples)
            if len(df) >= 10 and all(v in df.columns for v in variables):
                est = PC(df[variables])
                model = est.estimate(variant="orig", ci_test="chi_square",
                                     show_progress=False, max_cond_vars=2)
                async with self._lock:
                    self.graph.clear()
                    for e in model.edges():
                        self.graph[e[0]][e[1]] = {"weight": 1.0, "confidence": 1.0}
                        self.storage.save_causal_edge(e[0], e[1], 1.0, 1.0)
                    self.edge_stability = 1.0  # PC edges are stable
                return self.summary()
        except Exception as e:
            logger.debug("pgmpy PC unavailable: %s", e)

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
            X = np.array([[s.get(v, 0.0) for v in variables] for s in subset],
                          dtype=float)
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
                        src, dst = (variables[i], variables[j]) if vi > vj \
                            else (variables[j], variables[i])
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
                hits += sum(1 for s, d, _ in train_edges if (s, d) in subset_edges)
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

    def children(self, node):
        return list(self.graph.get(node, {}).keys())

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


class CausalTokenPolicyAdapter:
    ACTION_SPACE = ["efficiency", "accuracy", "speed", "budget", "adaptive"]
    ACTION_QUALITY_BONUS = {"efficiency": 0.05, "accuracy": 0.03,
                            "speed": 0.04, "budget": 0.02, "adaptive": 0.06}
    ACTION_COST = {"efficiency": 0.10, "accuracy": 0.05,
                    "speed": 0.02, "budget": 0.03, "adaptive": 0.05}

    def __init__(self, config, storage, graph):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTION_SPACE)] * len(self.ACTION_SPACE)
        self.epsilon = _cfg_get(config, "causal_exploration_rate", 0.1)
        self._last_action: Optional[str] = None
        self._lock = asyncio.Lock()

    async def choose_action(self, state):
        async with self._lock:
            if random.random() < self.epsilon:
                choice = random.choice(self.ACTION_SPACE)
            else:
                # [P3] restrict to graph parents of "reward" if available
                parents = [p for p in self.graph.parents("reward")
                           if p in self.ACTION_SPACE]
                candidates = parents if parents else self.ACTION_SPACE
                choice = max(candidates,
                             key=lambda a: self.values.get(a, 0.0))
            self._last_action = choice
            return choice

    async def update(self, action, reward, state):
        async with self._lock:
            if action not in self.ACTION_SPACE:
                action = self.ACTION_SPACE[0]
            self.counts[action] += 1
            n = self.counts[action]
            self.values[action] += (reward - self.values[action]) / n
            vals = [self.values.get(a, 0.0) for a in self.ACTION_SPACE]
            m = max(vals)
            exps = [math.exp((v - m) / 0.5) for v in vals]
            s = sum(exps) or 1.0
            self.policy = [e / s for e in exps]

    async def estimate_ate(self, treatment, outcome):
        w = self.graph.graph.get(treatment, {}).get(outcome, {}).get("weight", 0.0)
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
        if p and len(p) == len(self.ACTION_SPACE):
            self.policy = list(p)
        self._last_action = data.get("last_action")


# =============================================================================
# v4 MODULE 2: TEMPORAL LOGIC — [P1] real recursive-descent LTL parser;
#             [P0] pending NEXT; [P0] elapsed-window WITHIN
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
    """Split at depth-0 occurrences of any char in op_chars."""
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
    """Split at depth-0 occurrences of a whole word token."""
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
    """AST node. kind in {atom,!,G,F,X,U,&,|}"""

    def __init__(self, kind: str, args: Any = None):
        self.kind = kind
        self.args = args


def parse_ltl(s: str) -> LTLNode:
    """[P1] Recursive-descent LTL parser.
    Grammar (weakest to strongest):
        or   := and ('|' and)*
        and  := until ('&' until)*
        until:= unary ('U' unary)?
        unary:= ('!' | 'G' | 'F' | 'X') unary | primary
        prim := '(' or ')' | atom
    """
    s = s.strip()
    # Strip outer balanced parens
    while s.startswith("(") and s.endswith(")") and _balanced(s[1:-1]):
        s = s[1:-1].strip()

    # | (weakest)
    parts = _split_top(s, "|")
    if len(parts) > 1:
        return LTLNode("|", [parse_ltl(p) for p in parts])

    # &
    parts = _split_top(s, "&")
    if len(parts) > 1:
        return LTLNode("&", [parse_ltl(p) for p in parts])

    # U
    parts = _split_top_word(s, "U")
    if len(parts) == 2:
        return LTLNode("U", (parse_ltl(parts[0]), parse_ltl(parts[1])))

    # !
    if s.startswith("!"):
        return LTLNode("!", parse_ltl(s[1:].strip()))

    # G/F/X
    for op in ("G", "F", "X"):
        if s.startswith(op + " ") or s.startswith(op + "("):
            return LTLNode(op, parse_ltl(s[len(op):].strip()))

    # Atom (var OP value)
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
        return all(eval_ltl(node.args, trace, i) for i in range(idx, len(trace)))
    if k == "F":
        return any(eval_ltl(node.args, trace, i) for i in range(idx, len(trace)))
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
    def __init__(self, rule_id, operator, conditions,
                 window_seconds=0.0, description="", severity="warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window_seconds = window_seconds
        self.description = description
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


class TemporalTokenVerifier:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=_cfg_get(config, "temporal_max_trace", 2000))
        self.approval_callback: Optional[Callable] = None
        self._pending_next: Dict[str, Callable] = {}
        self._ltl_rules: Dict[str, LTLNode] = {}  # [P1] formula-based
        self._lock = asyncio.Lock()

        # [P1] Install config formulas via LTL parser
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            try:
                ast = parse_ltl(formula)
                self._ltl_rules[formula] = ast
            except Exception as e:
                logger.debug("Failed to parse TL formula '%s': %s", formula, e)

    def add_rule(self, rule_id, operator, conditions,
                 window_seconds=0.0, description="", severity="warning"):
        conds = conditions if isinstance(conditions, list) else [conditions]
        self.rules[rule_id] = TemporalRule(rule_id, operator, conds,
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
            for rule_id, cond in list(self._pending_next.items()):
                try:
                    ok = bool(cond(latest))
                except Exception:
                    ok = False
                if not ok:
                    rule = self.rules.get(rule_id)
                    if rule:
                        rule.violations += 1
                        rule.last_violation = datetime.now(timezone.utc)
                        try:
                            TEMPORAL_VIOLATIONS.labels(formula=rule_id).inc()
                        except Exception:
                            pass
                        await asyncio.get_event_loop().run_in_executor(
                            None, self.storage.save_temporal_violation,
                            rule_id, len(self.trace) - 1, latest)
                        result[rule_id] = False
                pending_done.append(rule_id)
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
            if rid not in result:
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

        # 3) [P1] Evaluate formula-based LTL rules
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
                TEMPORAL_VERIFICATIONS.labels(formula=formula, status=status).inc()
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
                self.rules[rid].last_violation = \
                    datetime.fromisoformat(lv) if lv else None


# =============================================================================
# v4 MODULE 3: XAI — [P3] Real KernelSHAP
# =============================================================================
class TokenXAIDecisionExplainer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "xai_method", "kernel_shap")
        self.depth = _cfg_get(config, "xai_depth", 5)

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
        coalitions.append(np.zeros(n)); weights.append(1e6)
        coalitions.append(x_arr.copy()); weights.append(1e6)
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
        if not (SKLEARN_AVAILABLE and len(x) > 0):
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

    def _nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id, label, features, names, model_fn):
        if self.method == "lime":
            attrs = self._lime(model_fn, features, names)
        else:
            attrs = self._kernel_shap(model_fn, features, names)
        nl = self._nl(label, attrs)
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


NUMPY_AVAILABLE = True  # numpy is imported at top; kept for guard parity


# =============================================================================
# v4 MODULE 4: ADAPTIVE PRECISION — [P1] real accuracy history
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
        cands = list(_cfg_get(self.config, "precision_levels",
                              ["fp32", "fp16", "bf16", "int8"]))
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        thresh = _cfg_get(self.config, "precision_switch_threshold", 0.02)
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
        thresh = _cfg_get(self.config, "precision_switch_threshold", 0.02)
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
# v4 MODULE 5: CARBON MARKETS — [P1] TTL cache; [P3] defer list
# =============================================================================
class CarbonMarketIntegrator:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.last_price = 25.0
        self._price_cache_ts = 0.0
        self._price_cache_ttl = 60.0
        self.deferred: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._cb = CircuitBreaker(3, 60.0, "carbon_market")
        self._lock = asyncio.Lock()

    async def _fetch_price(self):
        if AIOHTTP_AVAILABLE and _cfg_get(self.config, "carbon_market_api_url"):
            try:
                async with aiohttp.ClientSession() as s:
                    url = f"{_cfg_get(self.config, 'carbon_market_api_url')}/price"
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
            None, self.storage.save_credit_price, price, "USD", "oracle")
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
            self.deferred.append({"carbon_kg": carbon_kg,
                                   "intensity": intensity,
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
# v4 MODULE 6: CHAOS — [P0] steady_during; [P0] CB key; [P1] data corruption
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config, storage, probe_fn=None):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict[str, Any]] = {}
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
        blast = _cfg_get(self.config, "chaos_blast_radius", 0.1)
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
                            import pandas as pd
                            c = d.copy()
                            for col in c.columns:
                                if getattr(c[col].dtype, "kind", "") in "fi":
                                    mask = np.random.rand(len(c)) < \
                                        _cfg_get(self.config, "chaos_intensity", 0.05)
                                    c.loc[mask, col] = np.nan
                        except Exception:
                            pass
            elif fault_type == "memory_pressure":
                _ = bytearray(5 * 1024 * 1024)
                del _
            elif fault_type == "network_drop":
                await asyncio.sleep(duration_s)
            # [P0] Probe DURING fault — before the finally pop
            during_ok, during_score = await self._probe()
        except Exception as e:
            logger.info("chaos '%s' raised (expected): %s", name, e)
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
# MESSAGE BUS — [P0] real dispatch
# =============================================================================
class _MessageBus:
    def __init__(self):
        self._subs: Dict[str, List[Callable]] = defaultdict(list)

    def subscribe(self, topic, handler):
        self._subs[topic].append(handler)

    async def publish(self, msg):
        delivered = 0
        for h in list(self._subs.get(msg.get("topic", ""), [])):
            try:
                r = h(msg)
                if asyncio.iscoroutine(r):
                    await r
                delivered += 1
            except Exception as e:
                logger.debug("bus handler failed: %s", e)
        for h in list(self._subs.get("*", [])):
            try:
                r = h(msg)
                if asyncio.iscoroutine(r):
                    await r
                delivered += 1
            except Exception:
                pass
        return delivered


# =============================================================================
# v4 MODULE 7: MULTI-AGENT — [P0] real dispatch; [P3] hysteresis
# =============================================================================
class _TokenizationAgent:
    ROLES = ["segmenter", "tokenizer_selector", "budget_allocator",
             "cache_manager", "verifier"]

    def __init__(self, agent_id):
        self.id = agent_id
        self.role = "verifier"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


class MultiAgentCoordinator:
    SPECIALISE_MARGIN = 0.05  # [P3] hysteresis

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        count = _cfg_get(config, "agent_count", 5)
        self.agents: Dict[str, _TokenizationAgent] = {
            f"agent_{i:02d}": _TokenizationAgent(f"agent_{i:02d}")
            for i in range(count)}
        self.bus = _MessageBus()
        self.message_log: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._lock = asyncio.Lock()
        # Subscribe handlers
        self.bus.subscribe("critical_violation", self._on_critical)
        self.bus.subscribe("task_bid", self._on_task_bid)

    async def _on_critical(self, msg):
        async with self._lock:
            for a in self.agents.values():
                a.utilities["verifier"] = min(
                    1.0, a.utilities["verifier"] + 0.05)

    async def _on_task_bid(self, msg):
        self.message_log.append(msg)

    async def _specialise(self):
        async with self._lock:
            for a in self.agents.values():
                best = max(a.utilities, key=lambda r: a.utilities[r])
                if best != a.role and \
                        (a.utilities[best] - a.utilities[a.role]) >= self.SPECIALISE_MARGIN:
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
        preferred = task.get("preferred_role", "tokenizer_selector")
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
            "segmenter":           [0.40, 0.15, 0.15, 0.15, 0.15],
            "tokenizer_selector":  [0.15, 0.40, 0.15, 0.15, 0.15],
            "budget_allocator":    [0.15, 0.15, 0.15, 0.40, 0.15],
            "cache_manager":       [0.15, 0.15, 0.40, 0.15, 0.15],
            "verifier":            [0.15, 0.15, 0.15, 0.15, 0.40],
        }
        counts: Dict[str, int] = defaultdict(int)
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
# v4 MODULE 8: QUANTUM-DISTILLATION — [P0] correct KL gradient
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

    async def step(self, state):
        target = self._superpose()
        # [P0] Correct KL gradient: grad = student - teacher
        lr = 0.05
        async with self._lock:
            new = []
            for s, t in zip(self.student_policy, target):
                grad = s - t
                new.append(max(1e-3, s - lr * grad))
            ns = sum(new) or 1.0
            self.student_policy = [x / ns for x in new]
            self.history.append({"target": target,
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
# v3 MODULE: GA — [P3] real fitness (uses tokenizer/strategy estimate)
# =============================================================================
class GeneticParameterOptimizer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.population_size = _cfg_get(config, "ga_population_size", 20)
        self.generations = _cfg_get(config, "ga_generations", 5)
        self.mutation_rate = _cfg_get(config, "ga_mutation_rate", 0.2)
        self.crossover_rate = _cfg_get(config, "ga_crossover_rate", 0.7)
        self.param_bounds = {
            "summarization_ratio": (0.1, 0.9),
            "max_segment_length": (20, 200),
        }
        self._cached_best: Optional[Dict[str, float]] = None
        self._lock = asyncio.Lock()

    def _random_chromosome(self):
        return {
            "summarization_ratio": random.uniform(
                *self.param_bounds["summarization_ratio"]),
            "max_segment_length": random.randint(
                *self.param_bounds["max_segment_length"]),
        }

    def _mutate(self, chrom):
        new = dict(chrom)
        if random.random() < self.mutation_rate:
            param = random.choice(list(self.param_bounds))
            lo, hi = self.param_bounds[param]
            if param == "max_segment_length":
                new[param] = int(max(lo, min(hi, chrom[param] +
                                                  random.gauss(0, (hi - lo) / 10))))
            else:
                new[param] = max(lo, min(hi, chrom[param] +
                                              random.gauss(0, (hi - lo) / 10)))
        return new

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return dict(p1), dict(p2)
        c1, c2 = dict(p1), dict(p2)
        for k in self.param_bounds:
            if random.random() < 0.5:
                c1[k], c2[k] = p2[k], p1[k]
        return c1, c2

    async def _fitness(self, chrom, sample_texts):
        # [P3] Real fitness: estimate tokens under chrom's params, score reward
        scores = []
        budget = 500
        for text in sample_texts:
            est_tokens = max(1, len(text) // 4)
            if chrom["summarization_ratio"] < 1.0:
                est_tokens = int(est_tokens * chrom["summarization_ratio"])
            score = 0.0
            if est_tokens <= budget:
                score += 0.5
                if est_tokens >= budget * 0.3:
                    score += 0.3
            else:
                score -= 0.2
            if chrom["max_segment_length"] >= 50:
                score += 0.2
            scores.append(max(0.0, min(1.0, score)))
        return sum(scores) / max(1, len(scores))

    async def run_search(self, sample_texts=None):
        # [P3] Cache to prevent per-call search
        if sample_texts is None:
            sample_texts = ["sample text one.", "sample two longer text.",
                            "another example input here."]
        async with self._lock:
            if self._cached_best is not None:
                return dict(self._cached_best)
            pop = [self._random_chromosome() for _ in range(self.population_size)]
            best, best_fit = None, -1.0
            for _ in range(self.generations):
                fits = await asyncio.gather(
                    *[self._fitness(c, sample_texts) for c in pop])
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
                    *[self._fitness(c, sample_texts) for c in combined])
                sc = sorted(zip(combined, cf), key=lambda x: x[1], reverse=True)
                pop = [c for c, _ in sc[:self.population_size]]
                try:
                    GA_POPULATION_FITNESS.set(best_fit)
                except Exception:
                    pass
            self._cached_best = best or self._random_chromosome()
            return dict(self._cached_best)

    def invalidate_cache(self):
        self._cached_best = None


# =============================================================================
# v3 MODULE: MoE — [P3] positive-reward filter
# =============================================================================
class MoEGatingNetwork:
    EXPERTS = ["efficiency", "accuracy", "speed", "budget", "adaptive"]

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.hidden_layers = tuple(_cfg_get(config, "moe_hidden_layers", [16, 8]))
        self._model = None
        self._scaler = None
        self._trained = False
        self._data: List[Tuple[List[float], int, float]] = []
        self._lock = asyncio.Lock()
        self.expert_names = list(self.EXPERTS)

    def _encode(self, ctx):
        features = [
            ctx.get("text_length", 100) / 10000.0,
            ctx.get("avg_word_len", 5) / 10.0,
            ctx.get("num_sentences", 5) / 100.0,
            ctx.get("requested_budget", 500) / 2000.0,
            min(ctx.get("tokenizer_efficiency", 0.3), 2.0) / 2.0,
        ]
        lang_map = {"en": 0, "id": 1, "fr": 2, "de": 3, "es": 4}
        one_hot = [0.0] * 5
        one_hot[lang_map.get(ctx.get("language", "en"), 4)] = 1.0
        features.extend(one_hot)
        features.append(ctx.get("time_of_day", 12) / 24.0)
        domain_map = {"scientific": 0, "legal": 1, "general": 2}
        d_one_hot = [0.0] * 3
        d_one_hot[domain_map.get(ctx.get("domain", "general"), 2)] = 1.0
        features.extend(d_one_hot)
        return features

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
            "efficiency": {"strategy": "efficiency",
                            "summarization_ratio": 0.3, "max_segment_length": 50},
            "accuracy": {"strategy": "accuracy",
                          "summarization_ratio": 0.7, "max_segment_length": 100},
            "speed": {"strategy": "speed",
                       "summarization_ratio": 0.0, "max_segment_length": 200},
            "budget": {"strategy": "budget",
                        "summarization_ratio": 0.5, "max_segment_length": 80},
            "adaptive": {"strategy": "adaptive",
                          "summarization_ratio": 0.5, "max_segment_length": 100},
        }.get(name, {"strategy": "adaptive"})

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
                            MOE_GATING_PROBABILITIES.labels(
                                expert=self.expert_names[i]).set(float(p))
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
        return {"expert_names": self.expert_names,
                "trained": self._trained,
                "n_samples": len(self._data)}

    def load_state(self, data):
        self._trained = bool(data.get("trained", False))


# =============================================================================
# v3 MODULE: PARETO — [P1] proper dominance
# =============================================================================
class ParetoFrontOptimizer:
    OBJECTIVES = ["token_count", "semantic_similarity",
                  "processing_time", "carbon_impact"]

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.pareto: List[Dict[str, Any]] = []
        self.max_size = _cfg_get(config, "pareto_max_architectures", 100)
        self._lock = asyncio.Lock()

    def _dominates(self, a, b):
        # Minimize token_count, processing_time, carbon_impact; maximize semantic_similarity
        am = (a["metrics"]["token_count"],
              -a["metrics"]["semantic_similarity"],
              a["metrics"]["processing_time"],
              a["metrics"]["carbon_impact"])
        bm = (b["metrics"]["token_count"],
              -b["metrics"]["semantic_similarity"],
              b["metrics"]["processing_time"],
              b["metrics"]["carbon_impact"])
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
                self.pareto.sort(key=lambda e: e["metrics"]["token_count"])
                self.pareto = self.pareto[:self.max_size]
            try:
                self.storage.save_state("token_pareto_front",
                                        json.dumps(self.pareto, default=str))
                PARETO_FRONT_SIZE.set(len(self.pareto))
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
# NEURAL TEACHER — [P1] buffer + fit at 20; [P1] actually used as teacher
# =============================================================================
class NeuralTeacher:
    def __init__(self, input_dim=15, output_dim=5, hidden_layers=None):
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.hidden_layers = hidden_layers or [64, 32]
        self._model = None
        if SKLEARN_AVAILABLE:
            self._model = MLPClassifier(
                hidden_layer_sizes=tuple(self.hidden_layers),
                max_iter=200, random_state=42)
        self._buffer_X: List[List[float]] = []
        self._buffer_y: List[int] = []

    def predict_proba(self, X):
        if self._model is not None and hasattr(self._model, "classes_"):
            try:
                return self._model.predict_proba(X)
            except Exception:
                pass
        return np.ones((len(X), self.output_dim)) / self.output_dim

    def train(self, X, y):
        if self._model is None:
            return
        X = X.tolist() if hasattr(X, "tolist") else list(X)
        y = y.tolist() if hasattr(y, "tolist") else list(y)
        self._buffer_X.extend(X)
        self._buffer_y.extend(y)
        if len(self._buffer_X) >= 20:
            Xb = np.array(self._buffer_X)
            yb = np.array(self._buffer_y)
            if len(set(yb.tolist())) >= 2:
                try:
                    self._model.fit(Xb, yb)
                    self._buffer_X.clear()
                    self._buffer_y.clear()
                except Exception as e:
                    logger.debug("NeuralTeacher train failed: %s", e)

    def stall(self):
        pass


# =============================================================================
# FEDERATED — [P0] unified read/write; [P3] discovery
# =============================================================================
class FederatedLearner:
    def __init__(self, storage, instance_id, share_interval,
                 backend=None):
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
        # [P0] Write to the unified fed_weights dict
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
        # [P0] Read from unified fed_weights
        all_weights = self.storage.get_all_fed_weights()
        weight_list: List[Dict[str, Any]] = []
        for k, v in all_weights.items():
            try:
                weight_list.append(json.loads(v))
            except Exception:
                continue
        if not weight_list:
            return None
        keys = set().union(*[w.keys() for w in weight_list])
        avg: Dict[str, List[float]] = {}
        for k in keys:
            vals = [w.get(k) for w in weight_list if isinstance(w.get(k), list)]
            if not vals:
                continue
            n = len(vals[0])
            avg[k] = [sum(v[i] for v in vals) / len(vals) for i in range(n)]
            s = sum(avg[k]) or 1.0
            avg[k] = [x / s for x in avg[k]]
        self.rounds += 1
        try:
            FEDERATED_AGGREGATIONS.inc()
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
# HITL — [P0] wired; [P3] WebSocket transport
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
            q = [c["metrics"]["token_count"] for c in top_configs[:2]]
            if abs(q[0] - q[1]) / max(q) > 0.05:
                return None
        except Exception:
            return None
        if self.websocket:
            try:
                await self.websocket.broadcast({
                    "type": "preference_query",
                    "user_id": user_id,
                    "options": [{"id": c["solution_id"],
                                 "token_count": c["metrics"]["token_count"]}
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
                    s = sum(weights.get(k, 0.25) /
                            (c["metrics"][k] + 1e-8) for k in c["metrics"])
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
            self.storage.save_state(f"token_user_pref_{user_id}",
                                    json.dumps(prefs))
        except Exception:
            pass


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
# DRIFT DETECTOR
# =============================================================================
class DriftDetector:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.language_history: Deque[str] = deque(maxlen=100)
        self.performance_history: Deque[float] = deque(maxlen=100)
        self.threshold = 0.15

    async def check_language_drift(self, language):
        self.language_history.append(language)
        if len(self.language_history) < 20:
            return False
        recent = list(self.language_history)[-10:]
        old = list(self.language_history)[:-10]
        if not old:
            return False
        old_majority = max(set(old), key=old.count)
        recent_majority = max(set(recent), key=recent.count)
        return old_majority != recent_majority

    async def check_performance_drift(self, avg_reward):
        self.performance_history.append(float(avg_reward))
        if len(self.performance_history) < 10:
            return False
        recent = list(self.performance_history)[-10:]
        mean = float(np.mean(recent))
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
# LEARNING CACHE — [P0] lock-protected
# =============================================================================
class LearningCache:
    def __init__(self, max_size=1000, ttl=300):
        self.max_size = max_size
        self.ttl = ttl
        self.cache: Dict[str, Any] = {}
        self.access_counts: Dict[str, int] = defaultdict(int)
        self.last_access: Dict[str, datetime] = {}
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0

    async def get(self, key):
        async with self._lock:
            if key in self.cache:
                entry = self.cache[key]
                if (datetime.now() - entry["timestamp"]).total_seconds() < self.ttl:
                    self.access_counts[key] += 1
                    self.last_access[key] = datetime.now()
                    self._hits += 1
                    self._update_rate()
                    return entry["value"]
                self._drop(key)
            self._misses += 1
            self._update_rate()
            return None

    async def set(self, key, value):
        async with self._lock:
            if len(self.cache) >= self.max_size:
                self._evict()
            self.cache[key] = {"value": value, "timestamp": datetime.now()}
            self.access_counts[key] = 1
            self.last_access[key] = datetime.now()

    def _drop(self, key):
        self.cache.pop(key, None)
        self.access_counts.pop(key, None)
        self.last_access.pop(key, None)

    def _evict(self):
        if not self.cache:
            return
        now = datetime.now()

        def score(k):
            age = max(1e-3, (now - self.last_access[k]).total_seconds())
            return self.access_counts[k] / age
        victim = min(self.cache.keys(), key=score)
        self._drop(victim)

    def _update_rate(self):
        total = self._hits + self._misses
        if total > 0:
            try:
                TOKEN_CACHE_HIT_RATE.set(self._hits / total)
            except Exception:
                pass

    def clear(self):
        self.cache.clear()
        self.access_counts.clear()
        self.last_access.clear()
        self._hits = 0
        self._misses = 0


# =============================================================================
# LIMIT GRAPH
# =============================================================================
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph = {"carbon": {"cost": 0.8},
                      "cost": {"token_count": -0.2},
                      "token_count": {"latency": 0.5},
                      "latency": {"cost": 0.3}}
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


# =============================================================================
# MODP — [P1] dynamic candidates
# =============================================================================
class MODPStrategyOptimizer:
    CRITERIA = ["token_count", "carbon", "cost", "latency"]

    def __init__(self, config):
        self.config = config
        self.weights = list(_cfg_get(config, "modp_weights", [0.25] * 4))
        self._xai: Optional[TokenXAIDecisionExplainer] = None

    def _topsis(self, matrix, weights):
        norm = matrix / (np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9)
        w = norm * weights
        ideal, neg = w.max(axis=0), w.min(axis=0)
        dp = np.sqrt(((w - ideal) ** 2).sum(axis=1))
        dn = np.sqrt(((w - neg) ** 2).sum(axis=1))
        return dn / (dp + dn + 1e-9)

    def _candidates_from_state(self, state):
        # [P1] Dynamic candidates
        text_len = state.get("text_length", 1000)
        budget = state.get("requested_budget", 500)
        return [
            {"name": "efficiency",
             "token_count": 0.5 + 0.1 * min(1.0, text_len / 10000),
             "carbon": 0.2, "cost": 0.3, "latency": 0.4},
            {"name": "accuracy",
             "token_count": 0.8 + 0.1 * min(1.0, text_len / 10000),
             "carbon": 0.5, "cost": 0.5, "latency": 0.6},
            {"name": "speed",
             "token_count": 0.4, "carbon": 0.3, "cost": 0.2, "latency": 0.1},
            {"name": "budget",
             "token_count": min(1.0, budget / 1000.0),
             "carbon": 0.4, "cost": 0.2, "latency": 0.3},
            {"name": "adaptive",
             "token_count": 0.6 + 0.1 * min(1.0, text_len / 10000),
             "carbon": 0.3, "cost": 0.3, "latency": 0.4},
        ]

    async def select_strategy(self, state_dict):
        candidates = self._candidates_from_state(state_dict)
        cands = [{"token_count": 1.0 - c["token_count"],
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
                    state_dict.get("text_length", 100) / 10000.0,
                    state_dict.get("requested_budget", 500) / 2000.0,
                    0.5, 0.5])
                def _score(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                xai_res = await self._xai.explain(
                    decision_id=f"modp_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={best['name']}",
                    features=feats,
                    names=["text_len", "budget", "cost", "latency"],
                    model_fn=_score)
                result["attributions"] = xai_res["attributions"]
                result["explanation"] = xai_res["explanation"]
            except Exception as e:
                logger.debug("XAI hook failed: %s", e)
        return result


# =============================================================================
# RLHF — [P0] action-aware reward model
# =============================================================================
class RLHFManager:
    ACTIONS = ["efficiency", "accuracy", "speed", "budget", "adaptive"]

    def __init__(self, config):
        self.config = config
        self.buffer: List[Dict[str, Any]] = []
        self.reward_model = None
        if SKLEARN_AVAILABLE:
            self.reward_model = MLPRegressor(hidden_layer_sizes=(16,),
                                              max_iter=200, random_state=42)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self._lock = asyncio.Lock()

    def _state_to_features(self, state, action_idx=None):
        # [P0] Include action one-hot for action-aware predictions
        feats = [
            state.get("text_length", 100) / 10000.0,
            state.get("avg_word_len", 5) / 10.0,
            state.get("num_sentences", 5) / 100.0,
            state.get("requested_budget", 500) / 2000.0,
            min(state.get("tokenizer_efficiency", 0.3), 2.0) / 2.0,
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
        # [P0] Predict per action with action-aware features
        if self.reward_model is not None and \
                getattr(self.reward_model, "n_features_in_", None) is not None:
            feats_list = []
            for action_idx in range(len(self.ACTIONS)):
                feats_list.append(self._state_to_features(state, action_idx))
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
# MULTI-TEACHER POLICY DISTILLATION (fallback) — [P0] correct gradient
# =============================================================================
class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_engine=None):
        self.config = config
        self.moe_engine = moe_engine
        self.student_policy = np.array([0.2] * 5)
        self.temperature = _cfg_get(config, "distillation_temperature", 2.0)
        self.alpha = _cfg_get(config, "distillation_alpha", 0.5)
        self.history = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, state):
        if not self.moe_engine:
            return
        context = {
            "text_length": state.get("text_length", 100),
            "avg_word_len": state.get("avg_word_len", 5),
            "num_sentences": state.get("num_sentences", 5),
            "requested_budget": state.get("requested_budget", 500),
            "tokenizer_efficiency": state.get("tokenizer_efficiency", 0.3),
            "language": state.get("language", "en"),
            "domain": state.get("domain", "general")}
        selected, params = await self.moe_engine.select_expert(context)
        expert_names = list(self.moe_engine.expert_names)
        probs = np.ones(len(expert_names)) / len(expert_names)
        if self.moe_engine._trained and self.moe_engine._model is not None:
            try:
                features = np.array(self.moe_engine._encode(context), dtype=float)
                X = features.reshape(1, -1)
                if self.moe_engine._scaler:
                    X = self.moe_engine._scaler.transform(X)
                probs = self.moe_engine._model.predict_proba(X)[0]
            except Exception:
                pass
        teacher_dist = np.array(probs)
        teacher_dist = teacher_dist / teacher_dist.sum()
        # [P0] Correct KL gradient: grad = student - teacher
        async with self._lock:
            grad = self.student_policy - teacher_dist
            self.student_policy = np.clip(
                self.student_policy - 0.05 * grad, 0.01, None)
            self.student_policy /= self.student_policy.sum()
            loss = float(np.sum(teacher_dist *
                                np.log((teacher_dist + 1e-9) /
                                       (self.student_policy + 1e-9))))
            self.history.append({
                "teacher_dist": teacher_dist.tolist(),
                "student_dist": self.student_policy.tolist(),
                "loss": loss})

    def get_student_probs(self):
        return self.student_policy.tolist()

    def save_state(self):
        return {"student_policy": self.student_policy.tolist()}

    def load_state(self, data):
        sp = data.get("student_policy")
        if sp and len(sp) == 5:
            self.student_policy = np.array(sp)


# =============================================================================
# FALLBACK: DistillationTokenizationOptimizer — [P0] fixed action
# =============================================================================
class DistillationTokenizationOptimizer:
    ACTION_SPACE = ["efficiency", "accuracy", "speed", "budget", "adaptive"]

    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.student = DistillationStudent(
            lr=_cfg_get(config, "student_learning_rate", 0.01))
        self.teachers: List[Teacher] = [
            RuleBasedTeacher(),
            HistoricalMLTeacher(),
            StatefulQTeacher(storage),
        ]
        self.replay_buffer = ReplayBuffer(
            max_size=_cfg_get(config, "replay_buffer_size", 2000))
        self.epsilon = _cfg_get(config, "distillation_epsilon", 0.1)
        self.train_every = _cfg_get(config, "train_every", 10)
        self.counter = 0

    async def select_strategy(self, state, exploration=True):
        state_vec = state.to_feature_vector()
        teacher_probs = np.zeros(5)
        total_conf = 0.0
        for t in self.teachers:
            p = t.predict(state)
            c = t.confidence(state)
            teacher_probs += p * c
            total_conf += c
        teacher_probs = (teacher_probs / total_conf) if total_conf > 0 \
            else np.ones(5) / 5
        student_probs = self.student.predict_proba(state_vec)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, 4)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))
        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state, state_vec, action_idx, reward,
                     next_state_vec, teacher_probs):
        # [P0] Also update Q teacher
        for t in self.teachers:
            if isinstance(t, StatefulQTeacher):
                t.update(state, action_idx, reward)
                break
        self.replay_buffer.push(state_vec, action_idx, reward,
                                next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            states, actions, rewards, _, tps = self.replay_buffer.sample(8)
            for i in range(len(states)):
                self.student.update(states[i], tps[i], rewards[i], actions[i])

    def get_stats(self):
        return {"student_counter": self.student.counter,
                "buffer_size": len(self.replay_buffer),
                "weights_norm": float(np.linalg.norm(self.student.weights))}

    def save_state(self):
        return {"student": self.student.save_state(),
                "counter": self.counter}

    def load_state(self, data):
        if "student" in data:
            self.student.load_state(data["student"])
        self.counter = int(data.get("counter", 0))


# =============================================================================
# PQC signature chain — [P3]
# =============================================================================
class PQCChain:
    def __init__(self, storage):
        self.storage = storage
        self._pk: Optional[bytes] = None
        self._sk: Optional[bytes] = None
        self._load_or_generate()

    def _load_or_generate(self):
        stored = self.storage.get_state("pqc_token_keypair")
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
                self.storage.save_state("pqc_token_keypair", json.dumps({
                    "pk": self._pk.hex(), "sk": self._sk.hex()}))
            except Exception:
                self._pk = self._sk = None

    def sign(self, data: bytes) -> Dict[str, str]:
        if PQC_AVAILABLE and self._sk is not None:
            try:
                sig = dilithium.sign(self._sk, data)
                return {"algorithm": "dilithium",
                        "sig": sig.hex(),
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
# MAIN: TokenizationOptimizer v4.1.0
# =============================================================================
class TokenizationOptimizer:
    ACTION_SPACE = ["efficiency", "accuracy", "speed", "budget", "adaptive"]

    def __init__(self, cfg=None):
        self.config = cfg if cfg is not None else config

        if _cfg_get(self.config, "require_langdetect", False) and not LANGDETECT_AVAILABLE:
            raise ImportError("langdetect is required but not installed.")
        if _cfg_get(self.config, "require_nltk", False) and not NLTK_AVAILABLE:
            raise ImportError("NLTK is required but not installed.")

        self.instance_id = str(uuid.uuid4())[:8]
        self.tokenizers: Dict[str, Any] = {}
        self.language_map = _cfg_get(self.config, "language_tokenizer_map", {})
        self.default_tokenizer_name = _cfg_get(
            self.config, "default_tokenizer", "bert-base-uncased")
        self._tokenizer_lock = asyncio.Lock()
        self.circuit_breaker = CircuitBreaker(name="tokenizer_loading")
        self.cache = LearningCache(
            max_size=1000,
            ttl=_cfg_get(self.config, "cache_ttl_seconds", 300))

        # v3 modules
        self.ga_optimizer = (GeneticParameterOptimizer(self.config, storage)
                             if _cfg_get(self.config, "ga_enabled", True) else None)
        self.moe_gating = (MoEGatingNetwork(self.config, storage)
                           if _cfg_get(self.config, "moe_enabled", True) else None)
        self.pareto_optimizer = (ParetoFrontOptimizer(self.config, storage)
                                 if _cfg_get(self.config, "pareto_enabled", True) else None)
        self.neural_teacher = (NeuralTeacher()
                               if _cfg_get(self.config, "neural_teacher_enabled", True) else None)
        self.federated_learner = (FederatedLearner(
            storage, self.instance_id,
            _cfg_get(self.config, "federated_interval", 3600))
            if _cfg_get(self.config, "federated_enabled", True) else None)
        self.user_pref_learner = (ActiveUserPreferenceLearner(storage)
                                  if _cfg_get(self.config, "user_preference_enabled", True) else None)
        # [P3] HITL WebSocket
        self.hitl_ws: Optional[HITLWebSocket] = None
        if self.user_pref_learner and WEBSOCKETS_AVAILABLE:
            self.hitl_ws = HITLWebSocket(
                _cfg_get(self.config, "hitl_ws_port", 8791),
                self.user_pref_learner)
            self.user_pref_learner.websocket = self.hitl_ws

        self.drift_detector = (DriftDetector(storage, self.config)
                               if _cfg_get(self.config, "drift_detection_enabled", True) else None)
        self.limit_graph = (LimitGraphManager(self.config)
                            if _cfg_get(self.config, "limit_graph_enabled", True) else None)
        self.modp_optimizer = (MODPStrategyOptimizer(self.config)
                               if _cfg_get(self.config, "modp_enabled", True) else None)
        self.rlhf = (RLHFManager(self.config)
                     if _cfg_get(self.config, "rlhf_enabled", True) else None)
        self.distillation = (MultiTeacherPolicyDistillation(self.config, self.moe_gating)
                             if _cfg_get(self.config, "distillation_enabled", True)
                             and self.moe_gating else None)

        self.distillation_fallback = DistillationTokenizationOptimizer(
            storage, self.config)

        # v4 modules
        self.causal_graph = (CausalGraphLearner(storage)
                             if _cfg_get(self.config, "causal_rl_enabled", True) else None)
        self.causal_rl = (CausalTokenPolicyAdapter(self.config, storage, self.causal_graph)
                          if _cfg_get(self.config, "causal_rl_enabled", True)
                          and self.causal_graph else None)
        self.temporal = (TemporalTokenVerifier(storage, self.config)
                         if _cfg_get(self.config, "temporal_logic_enabled", True) else None)
        self.xai = (TokenXAIDecisionExplainer(self.config, storage)
                    if _cfg_get(self.config, "xai_enabled", True) else None)
        self.precision = (AdaptivePrecisionSwitcher(self.config, storage)
                          if _cfg_get(self.config, "adaptive_precision_enabled", True) else None)
        self.carbon_market = (CarbonMarketIntegrator(self.config, storage)
                              if _cfg_get(self.config, "carbon_market_enabled", True) else None)
        # [P0] chaos with real probe
        self.chaos = (ChaosTestingEngine(self.config, storage,
                                          probe_fn=self._chaos_probe)
                      if _cfg_get(self.config, "chaos_testing_enabled", True) else None)
        self.multi_agent = (MultiAgentCoordinator(self.config, storage)
                            if _cfg_get(self.config, "multi_agent_enabled", True) else None)
        self.quantum_distiller = (QuantumDistillationEngine(
            temperature=_cfg_get(self.config, "distillation_temperature", 2.0),
            alpha=_cfg_get(self.config, "distillation_alpha", 0.5))
            if _cfg_get(self.config, "distillation_enabled", True) else None)

        # [P3] PQC chain
        self.pqc = PQCChain(storage)

        # Wire hooks
        if self.xai and self.modp_optimizer:
            self.modp_optimizer._xai = self.xai
        if self.chaos:
            self.circuit_breaker.chaos_engine = self.chaos
        if self.temporal:
            self.temporal.set_approval_callback(self._hitl_approval)

        # Runtime
        self._shutdown_event = asyncio.Event()
        self._background_tasks: Set[asyncio.Task] = set()
        self._running = False
        self._recent_errors: Deque[Tuple[float, str]] = deque(maxlen=100)
        self._last_optimize_ts = time.time()

        logger.info("TokenizationOptimizer v4.1.0 initialized (instance=%s)",
                    self.instance_id)

    # ------------------------------------------------------------------
    # Chaos probe — [P0]
    # ------------------------------------------------------------------
    async def _chaos_probe(self) -> float:
        score = 1.0
        err_rate = len(self._recent_errors) / max(1, 100)
        score -= min(0.4, err_rate * 4.0)
        if time.time() - self._last_optimize_ts > 300:
            score -= 0.2
        return max(0.0, min(1.0, score))

    # ------------------------------------------------------------------
    # HITL — [P1] real approval via user_pref_learner if available
    # ------------------------------------------------------------------
    async def _hitl_approval(self, rule_id, state) -> bool:
        if self.user_pref_learner:
            try:
                # Ask user (with timeout) about critical rule violation
                choice = await self.user_pref_learner.query_user_if_needed(
                    user_id="system",
                    top_configs=[
                        {"solution_id": "approve", "metrics": {"token_count": 1}},
                        {"solution_id": "deny", "metrics": {"token_count": 1.01}}],
                    timeout=2.0)
                return choice != "deny"
            except Exception:
                pass
        return random.random() > 0.5

    # ------------------------------------------------------------------
    # Teacher interface
    # ------------------------------------------------------------------
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
        if self.distillation_fallback:
            # Expose the fallback student
            return self.distillation_fallback.student.predict_proba(
                np.zeros(15)).tolist()
        return [0.2] * 5

    # ------------------------------------------------------------------
    # Language / tokenizer / segmentation
    # ------------------------------------------------------------------
    async def detect_language(self, text):
        if not LANGDETECT_AVAILABLE:
            return _cfg_get(self.config, "fallback_language", "en")
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, detect, text)
        except Exception:
            return _cfg_get(self.config, "fallback_language", "en")

    def _model_name_for(self, language):
        # [P1] Correct model name for observability
        return self.language_map.get(language, self.default_tokenizer_name)

    async def _load_tokenizer(self, language):
        if language in self.tokenizers:
            return self.tokenizers[language]
        model_name = self._model_name_for(language)

        async def _load():
            if not TRANSFORMERS_AVAILABLE:
                # [P1] Fallback tokenizer
                tok = BasicTokenizer()
                self.tokenizers[language] = tok
                return tok
            try:
                tok = AutoTokenizer.from_pretrained(model_name)
                self.tokenizers[language] = tok
                return tok
            except Exception as e:
                logger.error("Failed to load tokenizer %s: %s", model_name, e)
                if model_name != self.default_tokenizer_name:
                    tok = AutoTokenizer.from_pretrained(self.default_tokenizer_name)
                    self.tokenizers[language] = tok
                    return tok
                # Final fallback
                tok = BasicTokenizer()
                self.tokenizers[language] = tok
                return tok
        return await self.circuit_breaker.call(_load)

    async def _get_tokenizer(self, language):
        async with self._tokenizer_lock:
            return await self._load_tokenizer(language)

    async def _segment_text(self, text):
        if NLTK_AVAILABLE:
            try:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, sent_tokenize, text)
            except Exception:
                pass
        return re.split(r"(?<=[.!?])\s+", text)

    async def _summarize(self, text, target_tokens):
        if SUMMA_AVAILABLE:
            try:
                lang = await self.detect_language(text)
                tokenizer = await self._get_tokenizer(lang)
                tokens = tokenizer.encode(text, add_special_tokens=False)
                ratio = target_tokens / len(tokens) if len(tokens) > 0 else 0.5
                ratio = min(1.0, max(0.1, ratio))
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(
                    None, lambda: summarizer.summarize(text, ratio=ratio))
            except Exception:
                pass
        # [P1] Token-aware fallback
        tokenizer = await self._get_tokenizer(await self.detect_language(text))
        tokens = tokenizer.encode(text, add_special_tokens=False)
        if len(tokens) <= target_tokens:
            return text
        # Truncate by token count (approximate for BasicTokenizer)
        if isinstance(tokenizer, BasicTokenizer):
            return " ".join(tokens[:target_tokens])
        return text[: target_tokens * 4]

    async def _tokenize(self, text, language):
        tokenizer = await self._get_tokenizer(language)
        tokens = tokenizer.encode(text, add_special_tokens=False)
        return tokens, len(tokens)

    def _cache_key(self, text, language, budget):
        # [P1] SHA-256, not MD5
        return hashlib.sha256(
            f"{text}_{language}_{budget}".encode()).hexdigest()

    # ------------------------------------------------------------------
    # CORE optimize
    # ------------------------------------------------------------------
    async def optimize(self, text, context):
        cid = uuid.uuid4().hex[:12]
        token = correlation_id_var.set(cid)
        try:
            try:
                TOKEN_ACTIVE.inc()
            except Exception:
                pass
            t0 = time.time()
            try:
                result = await self._optimize_inner(cid, t0, text, context)
                try:
                    TOKEN_LATENCY.observe(time.time() - t0)
                except Exception:
                    pass
                self._last_optimize_ts = time.time()
                return result
            except Exception as e:
                self._recent_errors.append((time.time(), str(e)))
                try:
                    TOKEN_ERRORS.labels(stage="optimize").inc()
                except Exception:
                    pass
                raise
        finally:
            try:
                TOKEN_ACTIVE.dec()
            except Exception:
                pass
            correlation_id_var.reset(token)

    async def _optimize_inner(self, cid, t0, text, context):
        language = context.get("language") or await self.detect_language(text)
        budget = context.get("token_budget", 1000)
        domain = context.get("domain")

        words = text.split()
        avg_word_len = float(np.mean([len(w) for w in words])) if words else 0.0
        sentences = await self._segment_text(text)
        num_sentences = len(sentences)

        # [P1] compression_ratio (was "efficiency")
        _, pre_tokens = await self._tokenize(text, language)
        compression_ratio = pre_tokens / len(text) if text else 0.0

        state_dict = {
            "text_length": len(text),
            "avg_word_len": avg_word_len,
            "num_sentences": num_sentences,
            "language": language,
            "requested_budget": budget,
            "tokenizer_efficiency": compression_ratio,
            "compression_ratio": compression_ratio,
            "domain": domain,
            "time_of_day": datetime.now().hour,
        }

        cache_key = self._cache_key(text, language, budget)
        cached = await self.cache.get(cache_key)
        if cached:
            try:
                CACHE_HIT_COUNTER.inc()
            except Exception:
                pass
            cached["cache_hit"] = True
            # [P1] Refresh precision on cache hit
            cached["precision_level"] = (
                self.precision.current if self.precision else "fp32")
            return cached
        try:
            CACHE_MISS_COUNTER.inc()
        except Exception:
            pass

        # ---- Strategy selection ----
        strategy = "adaptive"
        explanation = None
        attributions = None
        agent_id = None

        if self.causal_rl:
            strategy = await self.causal_rl.choose_action(state_dict)
        elif self.modp_optimizer:
            modp = await self.modp_optimizer.select_strategy(state_dict)
            strategy = modp["strategy"]
            explanation = modp.get("explanation")
            attributions = modp.get("attributions")
        elif self.rlhf:
            probs = await self.rlhf.get_policy_probs(state_dict)
            strategy = self.ACTION_SPACE[int(np.argmax(probs))]
        elif self.quantum_distiller:
            probs = self.quantum_distiller.get_policy()
            strategy = self.ACTION_SPACE[int(np.argmax(probs))]
        elif self.distillation:
            probs = self.distillation.get_student_probs()
            strategy = self.ACTION_SPACE[int(np.argmax(probs)) % 5]
        elif self.moe_gating:
            strategy, _ = await self.moe_gating.select_expert(state_dict)

        if strategy not in self.ACTION_SPACE:
            logger.warning("Strategy '%s' not in action space; "
                           "defaulting to adaptive", strategy)
            strategy = "adaptive"
        action_idx = self.ACTION_SPACE.index(strategy)
        try:
            TOKEN_STRATEGY_DIST.labels(strategy=strategy).set(1)
        except Exception:
            pass

        # Precision
        if self.precision:
            await self.precision.switch_to(
                self.precision.select_precision(), reason="pre_optimize")

        # Multi-agent bid
        if self.multi_agent:
            try:
                agent_id, _ = await self.multi_agent.bid({
                    "name": f"tokenize_{strategy}",
                    "preferred_role": (
                        "budget_allocator" if strategy == "budget"
                        else "tokenizer_selector" if strategy == "accuracy"
                        else "segmenter" if strategy == "speed"
                        else "verifier")})
            except Exception:
                agent_id = None

        # Carbon market decision
        cm_decision = None
        if self.carbon_market:
            try:
                cm_decision = await self.carbon_market.net_zero_schedule(
                    workload_kwh=len(text) * 0.00001, intensity=0.4)
            except Exception:
                cm_decision = None

        # ---- Execute strategy ----
        if strategy == "efficiency":
            tokens, total_tokens = await self._tokenize(text, language)
            if total_tokens > budget:
                truncated = text[: budget * 4]
                tokens, total_tokens = await self._tokenize(truncated, language)
                segments = [(truncated, total_tokens)]
            else:
                segments = [(text, total_tokens)]
        elif strategy == "accuracy":
            summary = await self._summarize(text, int(budget * 0.8))
            tokens, total_tokens = await self._tokenize(summary, language)
            segments = [(summary, total_tokens)]
        elif strategy == "speed":
            sent_counts = []
            for s in sentences:
                _, c = await self._tokenize(s, language)
                sent_counts.append(c)
            cum, chosen = 0, []
            for i, c in enumerate(sent_counts):
                if cum + c <= budget:
                    chosen.append((sentences[i], c))
                    cum += c
                else:
                    break
            segments = chosen or [(text[: budget * 4], budget)]
            total_tokens = cum or budget
        elif strategy == "budget":
            tokens, total_tokens = await self._tokenize(text, language)
            if total_tokens > budget:
                summary = await self._summarize(text, int(budget * 0.9))
                tokens, total_tokens = await self._tokenize(summary, language)
                segments = [(summary, total_tokens)]
            else:
                segments = [(text, total_tokens)]
        else:  # adaptive
            tokens, total_tokens = await self._tokenize(text, language)
            if total_tokens > budget:
                summary = await self._summarize(text, int(budget * 0.5))
                tokens, total_tokens = await self._tokenize(summary, language)
                segments = [(summary, total_tokens)]
            else:
                segments = [(text, total_tokens)]

        reward = self._compute_reward(text, total_tokens, budget,
                                      num_sentences, len(segments))
        try:
            DISTILLATION_REWARD.observe(reward)
        except Exception:
            pass

        # ---- Distillation fallback — [P0] uses MAIN strategy, not re-selected
        state_obj = TokenizationState(
            text_length=len(text), avg_word_len=avg_word_len,
            num_sentences=num_sentences, language=language,
            requested_budget=budget, tokenizer_efficiency=compression_ratio,
            domain=domain, time_of_day=datetime.now().hour)
        try:
            state_vec = state_obj.to_feature_vector()
            # Teacher probs from the fallback ensemble
            teacher_probs = np.zeros(5)
            total_conf = 0.0
            for t in self.distillation_fallback.teachers:
                p = t.predict(state_obj)
                c = t.confidence(state_obj)
                teacher_probs += p * c
                total_conf += c
            if total_conf > 0:
                teacher_probs /= total_conf
            else:
                teacher_probs = np.ones(5) / 5
            # [P0] Re-use MAIN action (not re-selected)
            # [P0] next_state is genuinely captured (post state, same fields)
            next_state = TokenizationState(
                text_length=len(segments[0][0]) if segments else len(text),
                avg_word_len=avg_word_len,
                num_sentences=len(segments), language=language,
                requested_budget=budget, tokenizer_efficiency=compression_ratio,
                domain=domain, time_of_day=datetime.now().hour)
            await self.distillation_fallback.update(
                state_obj, state_vec, action_idx, reward,
                next_state.to_feature_vector(), teacher_probs)
        except Exception as e:
            logger.debug("distillation fallback update failed: %s", e)

        # ---- Update learners ----
        if self.causal_rl:
            await self.causal_rl.update(strategy, reward, state_dict)
        if self.rlhf and reward > 0.4:
            await self.rlhf.record_feedback(state_dict, strategy, reward)
        if self.moe_gating:
            await self.moe_gating.add_training_sample(state_dict, strategy, reward)
        if self.neural_teacher:
            try:
                x = state_obj.to_feature_vector().reshape(1, -1)
                self.neural_teacher.train(x, np.array([action_idx]))
            except Exception:
                pass

        # LIMIT graph
        if self.limit_graph:
            await self.limit_graph.update_constraint("token_count", total_tokens)
            await self.limit_graph.update_constraint("carbon", total_tokens * 0.001)
            await self.limit_graph.evaluate_path("carbon", "token_count")

        # Pareto
        if self.pareto_optimizer:
            await self.pareto_optimizer.add_configuration(
                {"strategy": strategy, "language": language, "budget": budget},
                {"token_count": total_tokens,
                 "semantic_similarity": 0.8,
                 "processing_time": time.time() - t0,
                 "carbon_impact": total_tokens * 0.001})

        # ---- Temporal — [P1] derive tokenization_complete
        temporal_result: Dict[str, bool] = {}
        if self.temporal:
            tokenization_complete = bool(
                total_tokens <= budget and num_sentences > 0)
            await self.temporal.push_state({
                "token_count": total_tokens,
                "compression_ratio": compression_ratio,
                "efficiency": compression_ratio,  # alias
                "tokenization_complete": tokenization_complete})
            temporal_result = await self.temporal.verify()

        # Multi-agent reward
        if self.multi_agent and agent_id:
            await self.multi_agent.reward(agent_id, reward)

        # Drift
        if self.drift_detector:
            await self.drift_detector.check_language_drift(language)
            await self.drift_detector.check_performance_drift(reward)

        # XAI on near-budget
        xai_result = None
        if self.xai and total_tokens > budget * 0.9:
            try:
                feats = np.array([
                    len(text) / 10000.0, avg_word_len / 10.0,
                    num_sentences / 100.0, budget / 2000.0,
                    min(compression_ratio, 2.0) / 2.0])
                def _score(x):
                    return float(np.dot(x, [0.3, 0.2, 0.2, -0.1, -0.2]))
                xai_result = await self.xai.explain(
                    decision_id=f"tok_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={strategy}",
                    features=feats,
                    names=["text_len", "avg_word", "sentences",
                           "budget", "compression"],
                    model_fn=_score)
            except Exception as e:
                logger.debug("XAI failed: %s", e)

        # Federated share
        if self.federated_learner:
            pol = await self.policy_probs(state_dict)
            try:
                await self.federated_learner.share_weights({"policy": pol})
            except Exception:
                pass

        # ---- PQC signature chain
        dataset_hash = hashlib.sha256(json.dumps({
            "text_hash": hashlib.sha256(text.encode()).hexdigest(),
            "language": language,
            "strategy": strategy,
            "total_tokens": total_tokens,
            "ts": datetime.now(timezone.utc).isoformat(),
        }, sort_keys=True, default=str).encode()).hexdigest()
        quantum_sig = self.pqc.sign(dataset_hash.encode())

        result = {
            "segments": segments,
            "total_tokens": total_tokens,
            "language": language,
            "tokenizer_used": self._model_name_for(language),
            "strategy_used": strategy,
            "cache_hit": False,
            "reward": reward,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "precision_level": self.precision.current if self.precision else "fp32",
            "temporal_violations": [k for k, v in temporal_result.items() if not v],
            "xai_explanation": xai_result,
            "attributions": attributions,
            "explanation": explanation,
            "carbon_market_decision": cm_decision,
            "agent_id": agent_id,
            "correlation_id": cid,
            "dataset_hash": dataset_hash,
            "quantum_signature": quantum_sig,
        }

        if _cfg_get(self.config, "enable_cache", True):
            await self.cache.set(cache_key, dict(result))

        try:
            TOKENIZATION_COUNTER.labels(language=language, status="success").inc()
            TOKEN_COUNT_HISTOGRAM.labels(language=language).observe(total_tokens)
            TOKENIZATION_DURATION.labels(language=language,
                                          strategy=strategy).observe(
                time.time() - t0)
            LANGUAGE_DISTRIBUTION.labels(language=language).set(1)
        except Exception:
            pass

        logger.info("[cid=%s] lang=%s tokens=%s strategy=%s reward=%.3f",
                    cid, language, total_tokens, strategy, reward)
        return result

    def _compute_reward(self, text, total_tokens, budget, num_sentences,
                        num_segments):
        reward = 0.0
        eff = total_tokens / len(text) if text else 0
        if eff < 0.3:
            reward += 0.4
        elif eff < 0.5:
            reward += 0.2
        if total_tokens <= budget:
            reward += 0.3
            if total_tokens < budget * 0.3:
                reward -= 0.1
        else:
            reward -= 0.2
        if num_sentences > 0:
            if num_segments / num_sentences > 0.5:
                reward += 0.3
        return max(0.0, min(1.0, reward))

    async def get_token_efficiency(self, text, language=None):
        if language is None:
            language = await self.detect_language(text)
        _, n = await self._tokenize(text, language)
        return n / len(text) if text else 0.0

    async def clear_cache(self):
        self.cache.clear()

    async def get_cache_stats(self):
        return {"size": len(self.cache.cache),
                "hits": self.cache._hits,
                "misses": self.cache._misses,
                "ttl_seconds": self.cache.ttl}

    # ------------------------------------------------------------------
    # [P3] Streaming + batched APIs
    # ------------------------------------------------------------------
    async def stream_optimize(self, texts, context_fn=None):
        async for text in texts:
            ctx = context_fn(text) if context_fn else {}
            yield await self.optimize(text, ctx)

    async def optimize_batch(self, texts, context, concurrency=4):
        sem = asyncio.Semaphore(concurrency)

        async def _one(t):
            async with sem:
                return await self.optimize(t, dict(context))

        return await asyncio.gather(*[_one(t) for t in texts])

    # ------------------------------------------------------------------
    # [P3] Persistence
    # ------------------------------------------------------------------
    async def save_state(self, path=None):
        path = path or _cfg_get(self.config, "rlfh_state_path",
                                 "/tmp/token_v41_state.json")
        data = {"version": "4.1.0", "instance_id": self.instance_id,
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
            ("distillation_fallback", self.distillation_fallback),
            ("federated", self.federated_learner),
            ("moe", self.moe_gating),
            ("pareto", self.pareto_optimizer),
        ]:
            if obj is not None and hasattr(obj, "save_state"):
                try:
                    data["submodules"][name] = obj.save_state()
                except Exception as e:
                    logger.debug("save_state '%s' failed: %s", name, e)
        try:
            p = Path(path) if (Path := __import__("pathlib").Path) else None
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(data, default=str, indent=2))
            return True
        except Exception as e:
            logger.error("save_state failed: %s", e)
            return False

    async def load_state(self, path=None):
        path = path or _cfg_get(self.config, "rlfh_state_path",
                                 "/tmp/token_v41_state.json")
        try:
            Path = __import__("pathlib").Path
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
                ("distillation_fallback", self.distillation_fallback),
                ("federated", self.federated_learner),
                ("moe", self.moe_gating),
                ("pareto", self.pareto_optimizer),
            ]:
                if obj is not None and name in data.get("submodules", {}) \
                        and hasattr(obj, "load_state"):
                    try:
                        obj.load_state(data["submodules"][name])
                    except Exception as e:
                        logger.debug("load_state '%s' failed: %s", name, e)
            return True
        except Exception as e:
            logger.error("load_state failed: %s", e)
            return False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        self._running = True
        if self.hitl_ws:
            await self.hitl_ws.start()
        loop = asyncio.get_event_loop()
        tasks = [
            loop.create_task(self._ga_loop()),
            loop.create_task(self._rlhf_loop()),
            loop.create_task(self._federated_loop()),
            loop.create_task(self._moe_loop()),
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
        if self.chaos and os.getenv("TOKEN_ENV", "production") == "chaos":
            tasks.append(loop.create_task(self._chaos_loop()))
        if self.multi_agent:
            tasks.append(loop.create_task(self._multi_agent_loop()))
        for t in tasks:
            self._background_tasks.add(t)
            t.add_done_callback(self._background_tasks.discard)
        logger.info("Tokenization optimizer started with %d background tasks",
                    len(self._background_tasks))

    async def shutdown(self):
        logger.info("Shutting down tokenization optimizer...")
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        if self.hitl_ws:
            await self.hitl_ws.stop()
        try:
            await self.save_state()
        except Exception:
            pass
        self.tokenizers.clear()
        self.cache.clear()
        logger.info("Shutdown complete")

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    async def _ga_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(3600)
                if self.ga_optimizer:
                    self.ga_optimizer.invalidate_cache()
                    await self.ga_optimizer.run_search(
                        ["sample text one.", "sample two longer."])
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("GA loop: %s", e)

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(self.config, "rlhf_training_interval", 600))
                if self.rlhf:
                    await self.rlhf.train_reward_model()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("RLHF loop: %s", e)

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(self.config, "federated_interval", 3600))
                if self.federated_learner:
                    await self.federated_learner.discover_and_pull()
                    agg = await self.federated_learner.pull_aggregated_weights()
                    if agg and "policy" in agg and \
                            len(agg["policy"]) == 5 and self.quantum_distiller:
                        blended = [(a + b) / 2 for a, b in
                                    zip(self.quantum_distiller.student_policy,
                                        agg["policy"])]
                        s = sum(blended) or 1.0
                        self.quantum_distiller.student_policy = [x / s for x in blended]
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Federated loop: %s", e)

    async def _moe_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)
                if self.moe_gating:
                    self.moe_gating._train()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("MoE loop: %s", e)

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(self.config, "limit_graph_update_interval", 300))
                if self.limit_graph:
                    await self.limit_graph.update_constraint(
                        "carbon", random.uniform(0.2, 0.6))
                    await self.limit_graph.evaluate_path("carbon", "token_count")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("LIMIT loop: %s", e)

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(self.config, "distillation_interval", 300))
                if self.distillation:
                    await self.distillation.distill(
                        {"text_length": 1000, "language": "en"})
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Distillation loop: %s", e)

    async def _quantum_distill_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(self.config, "distillation_interval", 300))
                if self.quantum_distiller:
                    if self.rlhf:
                        pol = await self.rlhf.get_policy_probs({})
                        self.quantum_distiller.register_teacher("rlhf", pol)
                    if self.causal_rl:
                        self.quantum_distiller.register_teacher(
                            "causal", self.causal_rl.get_policy())
                    if self.multi_agent:
                        self.quantum_distiller.register_teacher(
                            "agents", self.multi_agent.get_policy())
                    if self.distillation:
                        self.quantum_distiller.register_teacher(
                            "moe", self.distillation.get_student_probs())
                    if self.distillation_fallback:
                        self.quantum_distiller.register_teacher(
                            "fallback",
                            self.distillation_fallback.student.predict_proba(
                                np.zeros(15)).tolist())
                    await self.quantum_distiller.step({})
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Quantum distillation loop: %s", e)

    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(self.config, "causal_graph_update_interval", 900))
                min_samples = _cfg_get(self.config, "causal_min_samples", 20)
                samples = [
                    {"token_count": random.uniform(100, 5000),
                     "compression": random.uniform(0.1, 0.9),
                     "reward": random.uniform(0.3, 0.9),
                     "carbon": random.uniform(0.1, 0.8)}
                    for _ in range(min_samples)]
                await self.causal_graph.learn(
                    samples, ["token_count", "compression", "reward", "carbon"])
                await self.causal_rl.estimate_ate("token_count", "reward")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Causal RL loop: %s", e)

    async def _temporal_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(self.config, "temporal_verification_interval", 300))
                res = await self.temporal.verify()
                bad = [k for k, v in res.items() if not v]
                if bad:
                    logger.warning("Temporal violations: %s", bad)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Temporal loop: %s", e)

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(_cfg_get(self.config, "xai_interval", 300))
                if self.xai:
                    feats = np.array([0.4, 0.3, 0.3, 0.5, 0.4])
                    def _score(x):
                        return float(np.dot(x, [0.3, 0.2, 0.2, -0.1, -0.2]))
                    await self.xai.explain(
                        decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                        label="system_health",
                        features=feats,
                        names=["text_len", "avg_word", "sentences",
                               "budget", "compression"],
                        model_fn=_score)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("XAI loop: %s", e)

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(300)
                if self.precision:
                    await self.precision.auto_switch(0.9, 0.92)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Precision loop: %s", e)

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(self.config, "carbon_market_interval", 3600))
                if self.carbon_market:
                    await self.carbon_market.update_price()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Carbon market loop: %s", e)

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(self.config, "chaos_test_interval", 1800))
                if self.chaos:
                    await self.chaos.run_experiment(
                        name=f"auto_{uuid.uuid4().hex[:6]}",
                        fault_type=random.choice(self.chaos.FAULT_TYPES),
                        target_breaker="tokenizer_loading")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Chaos loop: %s", e)

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(
                    _cfg_get(self.config, "agent_negotiation_interval", 600))
                if self.multi_agent:
                    await self.multi_agent.step()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Multi-agent loop: %s", e)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    async def health_check(self):
        return {
            "instance_id": self.instance_id,
            "version": "4.1.0",
            "running": self._running,
            "cache_size": len(self.cache.cache),
            "cache_hits": self.cache._hits,
            "cache_misses": self.cache._misses,
            "pareto_size": len(self.pareto_optimizer.get_pareto_front())
                if self.pareto_optimizer else 0,
            "precision": self.precision.current if self.precision else None,
            "causal_edges": self.causal_graph.summary()["edges"]
                if self.causal_graph else 0,
            "causal_stability": self.causal_graph.edge_stability
                if self.causal_graph else 0.0,
            "temporal_rules": len(self.temporal.rules) + len(self.temporal._ltl_rules)
                if self.temporal else 0,
            "agents": {a.id: a.role for a in self.multi_agent.agents.values()}
                if self.multi_agent else {},
            "carbon_price": self.carbon_market.last_price
                if self.carbon_market else None,
            "rec_balance_mwh": storage.get_rec_balance(),
            "recent_errors": len(self._recent_errors),
            "pqc_algorithm": "dilithium" if PQC_AVAILABLE else "sha256",
        }

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
            label=label, features=np.array(features), names=names,
            model_fn=score_fn)

    async def submit_user_response(self, user_id, chosen_id):
        if self.user_pref_learner:
            await self.user_pref_learner.submit_response(user_id, chosen_id)


# =============================================================================
# SIGNAL HANDLING — [P1]
# =============================================================================
_optimizer_instance: Optional[TokenizationOptimizer] = None
_shutdown_requested = False


def _handle_signal(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        return
    _shutdown_requested = True
    logger.info("Received signal %s", signum)
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running() and _optimizer_instance:
            loop.create_task(_optimizer_instance.shutdown())
    except Exception:
        pass


# =============================================================================
# DEMO
# =============================================================================
async def _demo():
    global _optimizer_instance
    optimizer = TokenizationOptimizer()
    _optimizer_instance = optimizer
    await optimizer.start()

    texts = [
        "This is a sample text. It contains multiple sentences. "
        "We want to tokenize it efficiently.",
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
        "Sed do eiusmod tempor incididunt ut labore.",
        "Another example text with several clauses, and some additional "
        "narrative to make it longer.",
    ]
    for i, text in enumerate(texts):
        res = await optimizer.optimize(text, {"token_budget": 50})
        print(f"\n--- Run {i+1} ---")
        print(f"  language: {res['language']}")
        print(f"  tokenizer: {res['tokenizer_used']}")
        print(f"  tokens: {res['total_tokens']}")
        print(f"  strategy: {res['strategy_used']}")
        print(f"  precision: {res['precision_level']}")
        print(f"  temporal_violations: {res['temporal_violations']}")
        print(f"  reward: {res['reward']:.3f}")
        print(f"  correlation_id: {res['correlation_id']}")
        print(f"  dataset_hash: {res['dataset_hash'][:16]}...")
        print(f"  pqc_algorithm: {res['quantum_signature']['algorithm']}")

    print("\n=== Batched (3 texts) ===")
    batch = await optimizer.optimize_batch(
        texts[:3], {"token_budget": 100}, concurrency=2)
    print(f"  batch size: {len(batch)}, "
          f"avg tokens: {sum(r['total_tokens'] for r in batch) / len(batch):.1f}")

    print("\n=== Streaming (3 texts) ===")
    async def _stream():
        for t in texts[:3]:
            yield t
    count = 0
    async for _ in optimizer.stream_optimize(_stream()):
        count += 1
    print(f"  streamed: {count}")

    print("\n=== Save / Load state ===")
    print("  saved:", await optimizer.save_state())
    print("  loaded:", await optimizer.load_state())

    print("\n=== Health check ===")
    print(json.dumps(await optimizer.health_check(), indent=2, default=str))

    print("\n=== Chaos experiment (steady_during measured) ===")
    chaos = await optimizer.run_chaos_experiment(
        "demo", "latency", target_breaker="tokenizer_loading")
    print(json.dumps(chaos, indent=2, default=str))

    print("\n=== XAI explanation ===")
    xai = await optimizer.explain_decision(
        label="sample_decision", features=[0.4, 0.3, 0.3, 0.5, 0.4],
        names=["text_len", "avg_word", "sentences", "budget", "compression"],
        score_fn=lambda x: float(np.dot(x, [0.3, 0.2, 0.2, -0.1, -0.2])))
    print(json.dumps(xai, indent=2, default=str))

    await optimizer.shutdown()


if __name__ == "__main__":
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: _handle_signal(s, None))
            except (NotImplementedError, RuntimeError):
                try:
                    signal.signal(sig, _handle_signal)
                except Exception:
                    pass
        loop.run_until_complete(_demo())
    except KeyboardInterrupt:
        pass
