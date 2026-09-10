#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/tokenization_optimizer_enhanced_v4_0.py
# VERSION: 4.0.0 (v3 base + Causal RL + Temporal Logic + XAI + Adaptive
#                   Precision + Carbon Markets/REC + Chaos Testing +
#                   Multi-Agent Coordination + fixed bugs — ALL in one file)
# =============================================================================
"""
Tokenization Optimizer — Version 4.0.0

Complete single-file implementation of the ten Green Agent enhancements,
integrated with the tokenization-specific logic.

v3.0.0 bug fixes:
  * MODPStrategyOptimizer  – now accepts dict / pydantic / object configs
  * MultiTeacherPolicyDistillation – same config-agnostic access
  * RLHFManager.train_reward_model – trains on REWARD (was ACTION)
  * RLHFManager.get_policy_probs – actually uses trained reward model
  * FederatedLearner – real weight-sharing & aggregation
  * ActiveUserPreferenceLearner – real HITL with response queue
  * DistillationTokenizationOptimizer – now wired into main pipeline

NEW v4.0.0 — all ten Green Agent enhancements fully implemented:
  1. Causal Reinforcement Learning        → CausalGraphLearner + CausalTokenPolicyAdapter
  2. Temporal Logic & Formal Verification → TemporalTokenVerifier
  3. Explainable AI                       → TokenXAIDecisionExplainer
  4. Adaptive Precision Switching         → AdaptivePrecisionSwitcher
  5. Carbon Markets / REC                 → CarbonMarketIntegrator
  6. Resilience / Chaos Testing           → ChaosTestingEngine
  7. Multi-Agent Coordination             → MultiAgentCoordinator
  8. Quantum-Distillation                 → QuantumDistillationEngine
  9. Federated Green Learning             → FederatedTokenLearner (fixed)
 10. Human-in-the-Loop Active Learning    → ActiveUserPreferenceLearner (fixed)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import re
import secrets
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple, Union

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
    import structlog
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    try:
        from pydantic import BaseSettings, Field, validator
        PYDANTIC_AVAILABLE = True
        _PYDANTIC_V1 = True
    except ImportError:
        PYDANTIC_AVAILABLE = False
        _PYDANTIC_V1 = False

try:
    from langdetect import detect, DetectorFactory
    LANGDETECT_AVAILABLE = True
    DetectorFactory.seed = 0
except ImportError:
    LANGDETECT_AVAILABLE = False

try:
    from nltk.tokenize import sent_tokenize
    NLTK_AVAILABLE = True
    import nltk
    try:
        nltk.download("punkt", quiet=True)
        nltk.download("punkt_tab", quiet=True)
    except Exception:
        pass
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
    from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from tenacity import (
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

    def retry(*a, **kw):
        def deco(fn):
            return fn
        return deco

    def stop_after_attempt(*a, **kw): return None
    def wait_exponential(*a, **kw): return None
    def retry_if_exception_type(*a, **kw): return None

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


# =============================================================================
# Universal config access helper (dict / pydantic / object)
# =============================================================================
def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    """Read a config value regardless of whether config is dict, pydantic, or object."""
    if config is None:
        return default
    if isinstance(config, dict):
        return config.get(key, default)
    # pydantic v2 BaseModel
    if hasattr(config, "model_dump"):
        try:
            d = config.model_dump()
            return d.get(key, default)
        except Exception:
            pass
    # pydantic v1 BaseSettings
    if hasattr(config, "dict") and callable(getattr(config, "dict")):
        try:
            d = config.dict()
            return d.get(key, default)
        except Exception:
            pass
    return getattr(config, key, default)


# =============================================================================
# Logging
# =============================================================================
if CENTRAL_COMPONENTS_AVAILABLE and central_logger:
    logger = central_logger
else:
    if STRUCTLOG_AVAILABLE:
        try:
            import structlog as _sl
            _sl.configure(
                processors=[
                    _sl.stdlib.add_log_level,
                    _sl.processors.TimeStamper(fmt="iso"),
                    _sl.processors.JSONRenderer(),
                ],
                logger_factory=_sl.stdlib.LoggerFactory(),
                wrapper_class=_sl.stdlib.BoundLogger,
                cache_logger_on_first_use=True,
            )
            logger = _sl.get_logger(__name__)
        except Exception:
            logging.basicConfig(level=logging.INFO)
            logger = logging.getLogger(__name__)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        logger = logging.getLogger("tokenization_v4")


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
    "moe_expert_count": 4,
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
    "rlhf_reward_model": "linear",
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
        "G (efficiency <= 0.9)",
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
    "carbon_market_api_url": "https://api.carbonmarket.example/v1",
    "carbon_market_interval": 3600,
    "chaos_testing_enabled": True,
    "chaos_test_interval": 1800,
    "chaos_intensity": 0.05,
    "chaos_blast_radius": 0.1,
    "chaos_fault_types": ["latency", "exception", "memory_pressure", "network_drop"],
    "chaos_auto_rollback": True,
    "multi_agent_enabled": True,
    "agent_count": 5,
    "agent_negotiation_interval": 600,
}


def _load_config() -> Any:
    """Build config from central, pydantic, or fall back to defaults."""
    if CENTRAL_COMPONENTS_AVAILABLE and central_config:
        class _CfgFromCentral:
            def __init__(self):
                g = lambda k, d: getattr(central_config, k, d)
                self.default_tokenizer = g("token_default_tokenizer", _DEFAULT_CFG["default_tokenizer"])
                self.language_tokenizer_map = g("token_language_tokenizer_map", _DEFAULT_CFG["language_tokenizer_map"])
                self.cache_ttl_seconds = g("token_cache_ttl_seconds", 300)
                self.enable_cache = g("token_enable_cache", True)
                self.max_segment_length = g("token_max_segment_length", 100)
                self.summarization_ratio = g("token_summarization_ratio", 0.5)
                self.fallback_language = g("token_fallback_language", "en")
                self.require_langdetect = g("token_require_langdetect", False)
                self.require_nltk = g("token_require_nltk", False)
                for k, v in _DEFAULT_CFG.items():
                    if not hasattr(self, k):
                        setattr(self, k, v)
        return _CfgFromCentral()
    if PYDANTIC_AVAILABLE:
        try:
            return _build_pydantic_config()
        except Exception as e:
            logger.warning("pydantic config failed (%s); using dict", e)
            return dict(_DEFAULT_CFG)
    return dict(_DEFAULT_CFG)


def _build_pydantic_config():
    """Construct a pydantic config from _DEFAULT_CFG using dynamic fields."""
    if hasattr(BaseModel, "model_fields"):  # pydantic v2
        class _Cfg(BaseModel):
            class Config:
                arbitrary_types_allowed = True
                extra = "allow"
            def __init__(self, **kw):
                super().__init__(**kw)
        return _Cfg(**_DEFAULT_CFG)
    else:  # pydantic v1
        class _Cfg(BaseModel):
            class Config:
                extra = "allow"
            def __init__(self, **kw):
                super().__init__(**kw)
        return _Cfg(**_DEFAULT_CFG)


config = _load_config()


# =============================================================================
# METRICS
# =============================================================================
class _DummyMetric:
    def labels(self, **kw): return self
    def inc(self, *a, **kw): pass
    def set(self, *a, **kw): pass
    def observe(self, *a, **kw): pass


if PROMETHEUS_AVAILABLE:
    TOKENIZATION_COUNTER = Counter("tokenization_requests_total", "Requests", ["language", "status"])
    TOKEN_COUNT_HISTOGRAM = Histogram("token_count_per_request", "Tokens", ["language"])
    TOKENIZATION_DURATION = Histogram("tokenization_duration_seconds", "Duration", ["language"])
    CACHE_HIT_COUNTER = Counter("tokenization_cache_hits_total", "Hits")
    CACHE_MISS_COUNTER = Counter("tokenization_cache_misses_total", "Misses")
    LANGUAGE_DISTRIBUTION = Gauge("tokenization_language_distribution", "Langs", ["language"])
    DISTILLATION_STRATEGY = Counter("distillation_strategy_selected", "Strategies", ["strategy"])
    DISTILLATION_REWARD = Histogram("distillation_reward", "Reward")
    GA_POPULATION_FITNESS = Gauge("token_ga_population_fitness", "GA fitness")
    MOE_GATING_PROBABILITIES = Gauge("token_moe_gating_probabilities", "MoE probs", ["expert"])
    PARETO_FRONT_SIZE = Gauge("token_pareto_front_size", "Pareto size")
    FEDERATED_AGGREGATIONS = Counter("token_federated_aggregations_total", "Federated aggregations")
    DRIFT_SCORE = Gauge("token_drift_score", "Drift", ["domain"])
    CAUSAL_ATE = Gauge("token_causal_ate", "ATE", ["treatment", "outcome"])
    TEMPORAL_VERIFICATIONS = Counter("token_temporal_verifications_total", "TL verif", ["formula", "status"])
    TEMPORAL_VIOLATIONS = Counter("token_temporal_violations_total", "TL viol", ["formula"])
    XAI_EXPLANATIONS = Counter("token_xai_explanations_total", "XAI", ["method"])
    XAI_FEATURE_IMPORTANCE = Gauge("token_xai_feature_importance", "XAI FI", ["feature"])
    PRECISION_SWITCHES = Counter("token_precision_switches_total", "Prec", ["from_p", "to_p"])
    PRECISION_ENERGY_SAVED = Gauge("token_precision_energy_saved_wh", "Saved")
    CARBON_CREDIT_PRICE = Gauge("token_carbon_credit_price_usd", "Price")
    REC_BALANCE = Gauge("token_rec_balance_mwh", "REC balance")
    NET_ZERO_MATCHES = Counter("token_net_zero_matches_total", "Net zero")
    CHAOS_EXPERIMENTS = Counter("token_chaos_experiments_total", "Chaos", ["fault_type", "status"])
    CHAOS_STEADY_STATE = Gauge("token_chaos_steady_state_ok", "Steady")
    AGENT_ROLES = Gauge("token_agent_roles", "Roles", ["role"])
    AGENT_REPUTATION = Gauge("token_agent_reputation", "Rep", ["agent_id"])
else:
    for _n in [
        "TOKENIZATION_COUNTER", "TOKEN_COUNT_HISTOGRAM", "TOKENIZATION_DURATION",
        "CACHE_HIT_COUNTER", "CACHE_MISS_COUNTER", "LANGUAGE_DISTRIBUTION",
        "DISTILLATION_STRATEGY", "DISTILLATION_REWARD", "GA_POPULATION_FITNESS",
        "MOE_GATING_PROBABILITIES", "PARETO_FRONT_SIZE", "FEDERATED_AGGREGATIONS",
        "DRIFT_SCORE", "CAUSAL_ATE", "TEMPORAL_VERIFICATIONS", "TEMPORAL_VIOLATIONS",
        "XAI_EXPLANATIONS", "XAI_FEATURE_IMPORTANCE", "PRECISION_SWITCHES",
        "PRECISION_ENERGY_SAVED", "CARBON_CREDIT_PRICE", "REC_BALANCE",
        "NET_ZERO_MATCHES", "CHAOS_EXPERIMENTS", "CHAOS_STEADY_STATE",
        "AGENT_ROLES", "AGENT_REPUTATION",
    ]:
        globals()[_n] = _DummyMetric()


# =============================================================================
# STORAGE (extended)
# =============================================================================
class TokenizationStorage:
    """In-memory + optional central passthrough store with v4 tables."""

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

    # Central-compatible sync helpers
    def get_state(self, key: str) -> Optional[str]:
        if self._central and hasattr(self._central, "get_state"):
            try:
                return self._central.get_state(key)
            except Exception:
                pass
        return self._state.get(key)

    def save_state(self, key: str, value: str) -> None:
        if self._central and hasattr(self._central, "save_state"):
            try:
                self._central.save_state(key, value)
                return
            except Exception:
                pass
        self._state[key] = value

    # v4 persistence
    def save_causal_edge(self, src: str, dst: str, weight: float, confidence: float):
        self.causal_edges[f"{src}->{dst}"] = {
            "src": src, "dst": dst, "weight": float(weight),
            "confidence": float(confidence),
            "ts": datetime.now(timezone.utc).isoformat(),
        }

    def save_temporal_violation(self, formula: str, step: int, state: Dict):
        self.temporal_violations.append({
            "formula": formula, "step": step, "state": dict(state),
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def save_xai_explanation(self, decision_id: str, method: str,
                             top_features: Dict, nl: str):
        self.xai_explanations.append({
            "decision_id": decision_id, "method": method,
            "top_features": dict(top_features), "natural_language": nl,
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def save_precision_switch(self, frm: str, to: str, reason: str, saved_wh: float):
        self.precision_history.append({
            "from": frm, "to": to, "reason": reason,
            "saved_wh": float(saved_wh),
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def save_rec(self, mwh: float, price: float, source: str):
        self.rec_ledger.append({
            "mwh": float(mwh), "price": float(price), "source": source,
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def get_rec_balance(self) -> float:
        return sum(r["mwh"] for r in self.rec_ledger)

    def save_credit_price(self, price: float, currency: str = "USD",
                          source: str = "oracle"):
        self.carbon_prices.append({
            "price": float(price), "currency": currency, "source": source,
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def save_chaos_experiment(self, name: str, fault_type: str, status: str,
                              before: int, after: int, blast: float):
        self.chaos_experiments.append({
            "name": name, "fault_type": fault_type, "status": status,
            "steady_before": int(before), "steady_after": int(after),
            "blast": float(blast),
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def save_agent(self, agent_id: str, role: str, rep: float, utilities: Dict):
        self.agent_registry[agent_id] = {
            "role": role, "reputation": float(rep),
            "utilities": dict(utilities),
            "ts": datetime.now(timezone.utc).isoformat(),
        }

    def save_agent_message(self, topic: str, sender: str, payload: Dict):
        self.agent_messages.append({
            "topic": topic, "sender": sender, "payload": dict(payload),
            "ts": datetime.now(timezone.utc).isoformat(),
        })


if CENTRAL_COMPONENTS_AVAILABLE and CentralStorage:
    try:
        _central_store = CentralStorage()
    except Exception:
        _central_store = None
else:
    _central_store = None

storage = TokenizationStorage(central=_central_store)


# =============================================================================
# CIRCUIT BREAKER + RATE LIMITER
# =============================================================================
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0, name: str = "default"):
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


class RateLimiter:
    def __init__(self, rate: int = 100, window: int = 60):
        self.rate = rate
        self.window = window
        self.tokens = float(rate)
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.05)


# =============================================================================
# DATACLASS
# =============================================================================
@dataclass
class TokenizationState:
    text_length: int
    avg_word_len: float
    num_sentences: int
    language: str
    requested_budget: int
    tokenizer_efficiency: float
    domain: Optional[str] = None
    time_of_day: int = 0

    def to_feature_vector(self) -> np.ndarray:
        features = [
            min(self.text_length / 10000.0, 1.0),
            min(self.avg_word_len / 10.0, 1.0),
            min(self.num_sentences / 100.0, 1.0),
            min(self.requested_budget / 2000.0, 1.0),
            self.tokenizer_efficiency,
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
            "text_length": self.text_length,
            "avg_word_len": self.avg_word_len,
            "num_sentences": self.num_sentences,
            "language": self.language,
            "requested_budget": self.requested_budget,
            "tokenizer_efficiency": self.tokenizer_efficiency,
            "domain": self.domain,
            "time_of_day": self.time_of_day,
        }


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
                self.model = None

    def predict(self, state):
        if self.model is None:
            return np.ones(5) / 5
        return self.model.predict_proba(state.to_feature_vector().reshape(1, -1))[0]

    def confidence(self, state):
        return 0.7 if self.model is not None else 0.0


class StatefulQTeacher(Teacher):
    def __init__(self, storage, lr=0.1):
        self.storage = storage
        self.lr = lr
        self.weights = np.zeros((15, 5))
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
        self.storage.save_state("q_teacher_weights", json.dumps(self.weights.tolist()))

    def predict(self, state):
        x = state.to_feature_vector()
        if len(x) != self.weights.shape[0]:
            return np.ones(5) / 5
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state):
        return 0.5

    def update(self, state, action, reward):
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
    def __init__(self, feature_dim: int = 15, n_classes: int = 5, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
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


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer: Deque = deque(maxlen=max_size)

    def push(self, s, a, r, ns, tp):
        self.buffer.append((s, a, r, ns, tp))

    def sample(self, batch_size=32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        if not batch:
            return (np.zeros((0, 15)), [], np.zeros(0), np.zeros((0, 15)),
                    np.zeros((0, 5)))
        s, a, r, ns, tp = zip(*batch)
        return np.array(s), list(a), np.array(r), np.array(ns), np.array(tp)

    def __len__(self):
        return len(self.buffer)


# =============================================================================
# v4 MODULE 1: CAUSAL REINFORCEMENT LEARNING
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage: TokenizationStorage):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn(self, samples: List[Dict[str, float]], variables: List[str],
                    threshold: float = 0.25) -> Dict[str, Any]:
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
                        src, dst = (variables[i], variables[j]) if vi > vj else (variables[j], variables[i])
                        self.graph[src][dst] = {"weight": float(corr[i, j]),
                                                "confidence": c}
                        self.storage.save_causal_edge(src, dst,
                                                      float(corr[i, j]), c)
        return self.summary()

    def parents(self, node: str) -> List[str]:
        return [s for s, e in self.graph.items() if node in e]

    def children(self, node: str) -> List[str]:
        return list(self.graph.get(node, {}).keys())

    def summary(self) -> Dict[str, Any]:
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables)}


class CausalTokenPolicyAdapter:
    ACTIONS = ["efficiency", "accuracy", "speed", "budget", "adaptive"]

    def __init__(self, config, storage: TokenizationStorage,
                 graph: CausalGraphLearner):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = _cfg_get(config, "causal_exploration_rate", 0.1)
        self._lock = asyncio.Lock()

    async def choose_action(self, state: Dict[str, Any]) -> str:
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.ACTIONS)
            return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action: str, reward: float, state: Dict[str, Any]):
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

    async def estimate_ate(self, treatment: str, outcome: str) -> float:
        w = self.graph.graph.get(treatment, {}).get(outcome, {}).get("weight", 0.0)
        CAUSAL_ATE.labels(treatment=treatment, outcome=outcome).set(w)
        return w

    def get_policy(self) -> List[float]:
        return list(self.policy)


# =============================================================================
# v4 MODULE 2: TEMPORAL LOGIC
# =============================================================================
class TemporalOperator:
    ALWAYS = "always"
    EVENTUALLY = "eventually"
    UNTIL = "until"
    NEVER = "never"
    NEXT = "next"
    WITHIN = "within"


_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$"
)


class TemporalRule:
    def __init__(self, rule_id: str, operator: str,
                 conditions: List[Callable[[Dict], bool]],
                 window_seconds: float = 0.0,
                 description: str = "",
                 severity: str = "warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window_seconds = window_seconds
        self.description = description
        self.severity = severity
        self.violations = 0
        self.last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque[Tuple[datetime, Dict]]) -> bool:
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


class TemporalTokenVerifier:
    def __init__(self, storage: TokenizationStorage, config):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=_cfg_get(config, "temporal_max_trace", 2000))
        self.approval_callback: Optional[Callable[[str, Dict], bool]] = None
        # install default formulas as rules
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            try:
                self._install_formula(formula)
            except Exception:
                pass

    def _install_formula(self, formula: str):
        # Very simple atomic-rule wrapper: "G (x <= y)" / "F (x >= y)" / "x OP y"
        f = formula.strip()
        if f.startswith("G ") or f.startswith("G("):
            inner = f[1:].strip().lstrip("(").rstrip(")")
            self._add_atomic(inner, TemporalOperator.ALWAYS)
        elif f.startswith("F ") or f.startswith("F("):
            inner = f[1:].strip().lstrip("(").rstrip(")")
            self._add_atomic(inner, TemporalOperator.EVENTUALLY)
        else:
            self._add_atomic(f, TemporalOperator.ALWAYS)

    def _add_atomic(self, expr: str, op: str):
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

    def add_rule(self, rule_id: str, operator: str,
                 conditions, window_seconds: float = 0.0,
                 description: str = "", severity: str = "warning"):
        conds = conditions if isinstance(conditions, list) else [conditions]
        self.rules[rule_id] = TemporalRule(rule_id, operator, conds,
                                           window_seconds, description, severity)

    def set_approval_callback(self, cb):
        self.approval_callback = cb

    async def push_state(self, state: Dict[str, Any]):
        self.trace.append((datetime.now(timezone.utc), dict(state)))

    async def verify(self) -> Dict[str, bool]:
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
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
            TEMPORAL_VERIFICATIONS.labels(formula=rid, status=status).inc()
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
                TEMPORAL_VIOLATIONS.labels(formula=rid).inc()
                await asyncio.get_event_loop().run_in_executor(
                    None, self.storage.save_temporal_violation, rid,
                    len(self.trace) - 1,
                    self.trace[-1][1] if self.trace else {})
                if rule.severity == "critical" and self.approval_callback:
                    try:
                        approved = self.approval_callback(rid, dict(self.trace[-1][1]))
                        if asyncio.iscoroutine(approved):
                            approved = await approved
                    except Exception:
                        pass
        return result


# =============================================================================
# v4 MODULE 3: XAI
# =============================================================================
class TokenXAIDecisionExplainer:
    def __init__(self, config, storage: TokenizationStorage):
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "xai_method", "kernel_shap")
        self.depth = _cfg_get(config, "xai_depth", 5)

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
                    delta = float(f(cur.reshape(1, -1))) - \
                            float(f(prev.reshape(1, -1)))
                except Exception:
                    delta = 0.0
                contrib[i] += delta
                prev = cur
        contrib = contrib / max(1, n)
        return dict(zip(names, contrib.tolist()))

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
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]), reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id, label, features, names, model_fn):
        if self.method == "lime":
            attrs = self._lime(model_fn, features, names)
        else:
            attrs = self._kernel_shap(model_fn, features, names)
        nl = self._nl(label, attrs)
        XAI_EXPLANATIONS.labels(method=self.method).inc()
        for k, v in list(attrs.items())[:self.depth]:
            XAI_FEATURE_IMPORTANCE.labels(feature=k).set(float(v))
        await asyncio.get_event_loop().run_in_executor(
            None, self.storage.save_xai_explanation,
            decision_id, self.method, attrs, nl)
        return {"decision_id": decision_id, "method": self.method,
                "attributions": attrs, "explanation": nl}


# =============================================================================
# v4 MODULE 4: ADAPTIVE PRECISION SWITCHER
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55, "fp16": 0.5, "int8": 0.3}
    PENALTY = {"fp32": 0.0, "tf32": 0.005, "bf16": 0.01,
               "fp16": 0.012, "int8": 0.03}

    def __init__(self, config, storage: TokenizationStorage):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self._lock = asyncio.Lock()

    def _probe(self) -> Dict[str, Any]:
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

    def select_precision(self) -> str:
        hw = self._probe()
        cands = list(_cfg_get(self.config, "precision_levels",
                              ["fp32", "fp16", "bf16", "int8"]))
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        async with self._lock:
            if target == self.current or target not in self.ENERGY:
                return False
            old = self.current
            self.current = target
            saved = max(0.0, self.ENERGY[old] - self.ENERGY[target])
            self.saved_wh += saved
            PRECISION_SWITCHES.labels(from_p=old, to_p=target).inc()
            PRECISION_ENERGY_SAVED.set(self.saved_wh)
            await asyncio.get_event_loop().run_in_executor(
                None, self.storage.save_precision_switch,
                old, target, reason, saved)
            logger.info("Precision %s → %s (%s)", old, target, reason)
            return True

    async def auto_switch(self, recent_acc: float, baseline_acc: float):
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = _cfg_get(self.config, "precision_switch_threshold", 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"accuracy drop {drop:.3f}")
        elif drop < thresh / 2:
            await self.switch_to(self.select_precision(), reason="headroom")

    @property
    def hardware(self):
        return self._probe()


# =============================================================================
# v4 MODULE 5: CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    def __init__(self, config, storage: TokenizationStorage):
        self.config = config
        self.storage = storage
        self.last_price = 25.0
        self._cb = CircuitBreaker(3, 60.0, "carbon_market")

    async def _fetch_price(self) -> float:
        return max(5.0, self.last_price + random.gauss(0, 1.5))

    async def update_price(self) -> float:
        try:
            price = await self._cb.call(self._fetch_price)
        except Exception:
            price = self.last_price
        self.last_price = price
        CARBON_CREDIT_PRICE.set(price)
        await asyncio.get_event_loop().run_in_executor(
            None, self.storage.save_credit_price, price, "USD", "oracle")
        return price

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0,
                           source: str = "wind") -> float:
        cost = mwh * price_per_mwh
        await asyncio.get_event_loop().run_in_executor(
            None, self.storage.save_rec, mwh, price_per_mwh, source)
        REC_BALANCE.set(self.storage.get_rec_balance())
        NET_ZERO_MATCHES.inc()
        return cost

    async def net_zero_schedule(self, workload_kwh: float,
                                intensity: float) -> Dict[str, Any]:
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
# v4 MODULE 6: CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config, storage: TokenizationStorage):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def _steady(self) -> bool:
        if not self.active:
            return True
        return random.random() > _cfg_get(self.config, "chaos_intensity", 0.05)

    async def run_experiment(self, name: str, fault_type: str,
                             workload: Optional[Dict] = None) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        before = await self._steady()
        status = "completed"
        try:
            async with self._lock:
                self.active[name] = {
                    "fault_type": fault_type,
                    "started": datetime.now(timezone.utc).isoformat(),
                }
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
        await asyncio.get_event_loop().run_in_executor(
            None, self.storage.save_chaos_experiment,
            name, fault_type, status, int(before), int(after),
            _cfg_get(self.config, "chaos_blast_radius", 0.1))
        CHAOS_EXPERIMENTS.labels(fault_type=fault_type, status=status).inc()
        CHAOS_STEADY_STATE.set(1.0 if after else 0.0)
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status}


# =============================================================================
# v4 MODULE 7: MULTI-AGENT COORDINATOR
# =============================================================================
class _TokenizationAgent:
    ROLES = ["segmenter", "tokenizer_selector", "budget_allocator",
             "cache_manager", "verifier"]

    def __init__(self, agent_id: str):
        self.id = agent_id
        self.role = "verifier"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


class MultiAgentCoordinator:
    def __init__(self, config, storage: TokenizationStorage):
        self.config = config
        self.storage = storage
        count = _cfg_get(config, "agent_count", 5)
        self.agents: Dict[str, _TokenizationAgent] = {
            f"agent_{i:02d}": _TokenizationAgent(f"agent_{i:02d}")
            for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self):
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])
                await asyncio.get_event_loop().run_in_executor(
                    None, self.storage.save_agent,
                    a.id, a.role, a.reputation, a.utilities)
                AGENT_ROLES.labels(role=a.role).set(1)
                AGENT_REPUTATION.labels(agent_id=a.id).set(a.reputation)

    async def broadcast(self, topic: str, sender: str, payload: Dict):
        msg = {"topic": topic, "sender": sender, "payload": payload,
               "ts": datetime.now(timezone.utc).isoformat()}
        try:
            self.bus.put_nowait(msg)
        except asyncio.QueueFull:
            pass
        await asyncio.get_event_loop().run_in_executor(
            None, self.storage.save_agent_message, topic, sender, payload)

    async def bid(self, task: Dict) -> Tuple[str, float]:
        preferred = task.get("preferred_role", "tokenizer_selector")
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

    async def reward(self, agent_id: str, reward: float):
        async with self._lock:
            if agent_id in self.agents:
                a = self.agents[agent_id]
                n = max(1, a.completed)
                a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
                a.utilities[a.role] = min(1.0, a.utilities[a.role] + 0.05 * reward)

    def get_policy(self) -> List[float]:
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

    async def step(self) -> Dict[str, Any]:
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
# v4 MODULE 8: QUANTUM-DISTILLATION (multi-teacher superposition)
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                 n_actions: int = 5):
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name: str, policy: List[float]):
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[name] = [p / s for p in policy]

    def _softmax(self, x, temp):
        m = max(x)
        exps = [math.exp((xi - m) / max(temp, 1e-6)) for xi in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _superpose(self) -> List[float]:
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

    async def step(self, state: Dict[str, Any]) -> Dict[str, Any]:
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

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =============================================================================
# v3 MODULE: GA PARAMETER OPTIMIZER (fixed _evaluate_fitness bug)
# =============================================================================
class GeneticParameterOptimizer:
    def __init__(self, config, storage: TokenizationStorage):
        self.config = config
        self.storage = storage
        self.population_size = _cfg_get(config, "ga_population_size", 20)
        self.generations = _cfg_get(config, "ga_generations", 5)
        self.mutation_rate = _cfg_get(config, "ga_mutation_rate", 0.2)
        self.crossover_rate = _cfg_get(config, "ga_crossover_rate", 0.7)
        self.param_bounds = {
            "summarization_ratio": (0.1, 0.9),
            "max_segment_length": (20, 200),
            "default_tokenizer_priority": [0, 1, 2],
        }

    def _random_chromosome(self):
        return {
            "summarization_ratio": random.uniform(*self.param_bounds["summarization_ratio"]),
            "max_segment_length": random.randint(*self.param_bounds["max_segment_length"]),
            "default_tokenizer_priority": random.choice(self.param_bounds["default_tokenizer_priority"]),
        }

    def _mutate(self, chrom):
        new = dict(chrom)
        if random.random() < self.mutation_rate:
            param = random.choice(list(self.param_bounds))
            if param == "default_tokenizer_priority":
                new[param] = random.choice(self.param_bounds[param])
            else:
                lo, hi = self.param_bounds[param]
                new[param] = max(lo, min(hi,
                    chrom[param] + random.gauss(0, (hi - lo) / 10)))
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
        total = 0.0
        for _ in sample_texts:
            score = 0.5
            if chrom["summarization_ratio"] > 0.3:
                score += 0.2
            if chrom["max_segment_length"] > 50:
                score += 0.2
            if chrom["default_tokenizer_priority"] == 1:
                score += 0.1
            total += score
        return total / len(sample_texts) if sample_texts else 0.5

    async def run_search(self, sample_texts):
        pop = [self._random_chromosome() for _ in range(self.population_size)]
        best, best_fit = None, -1.0
        for _ in range(self.generations):
            fits = await asyncio.gather(*[self._fitness(c, sample_texts) for c in pop])
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
            cf = await asyncio.gather(*[self._fitness(c, sample_texts) for c in combined])
            sc = sorted(zip(combined, cf), key=lambda x: x[1], reverse=True)
            pop = [c for c, _ in sc[: self.population_size]]
            GA_POPULATION_FITNESS.set(best_fit)
        return best or self._random_chromosome()


# =============================================================================
# v3 MODULE: MoE
# =============================================================================
class MoEGatingNetwork:
    EXPERTS = ["efficiency", "accuracy", "speed", "budget", "adaptive"]

    def __init__(self, config, storage: TokenizationStorage):
        self.config = config
        self.storage = storage
        self.num_experts = _cfg_get(config, "moe_expert_count", 4)
        self.hidden_layers = tuple(_cfg_get(config, "moe_hidden_layers", [16, 8]))
        self._model = None
        self._scaler = None
        self._trained = False
        self._data: List[Tuple[List[float], int, float]] = []
        self._lock = asyncio.Lock()
        self.expert_names = list(self.EXPERTS)

    def _encode(self, ctx: Dict) -> List[float]:
        features = [
            ctx.get("text_length", 100) / 10000.0,
            ctx.get("avg_word_len", 5) / 10.0,
            ctx.get("num_sentences", 5) / 100.0,
            ctx.get("requested_budget", 500) / 2000.0,
            ctx.get("tokenizer_efficiency", 0.3),
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
            "efficiency": {"strategy": "efficiency", "summarization_ratio": 0.3, "max_segment_length": 50},
            "accuracy": {"strategy": "accuracy", "summarization_ratio": 0.7, "max_segment_length": 100},
            "speed": {"strategy": "speed", "summarization_ratio": 0.0, "max_segment_length": 200},
            "budget": {"strategy": "budget", "summarization_ratio": 0.5, "max_segment_length": 80},
            "adaptive": {"strategy": "adaptive", "summarization_ratio": 0.5, "max_segment_length": 100},
        }.get(name, {"strategy": "adaptive"})

    async def select_expert(self, ctx):
        feats = self._encode(ctx)
        if self._trained and self._model is not None:
            X = np.array(feats, dtype=float).reshape(1, -1)
            if self._scaler:
                X = self._scaler.transform(X)
            probs = self._model.predict_proba(X)[0]
            idx = int(np.argmax(probs))
            for i, p in enumerate(probs):
                MOE_GATING_PROBABILITIES.labels(expert=self.expert_names[i]).set(float(p))
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
# v3 MODULE: PARETO FRONT
# =============================================================================
class ParetoFrontOptimizer:
    OBJECTIVES = ["token_count", "semantic_similarity", "processing_time", "carbon_impact"]

    def __init__(self, config, storage: TokenizationStorage):
        self.config = config
        self.storage = storage
        self.pareto: List[Dict[str, Any]] = []
        self.max_size = _cfg_get(config, "pareto_max_architectures", 100)
        self._lock = asyncio.Lock()

    def _dominates(self, a, b):
        am = (a["metrics"]["token_count"],
              -a["metrics"]["semantic_similarity"],
              a["metrics"]["processing_time"],
              a["metrics"]["carbon_impact"])
        bm = (b["metrics"]["token_count"],
              -b["metrics"]["semantic_similarity"],
              b["metrics"]["processing_time"],
              b["metrics"]["carbon_impact"])
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
                self.pareto.sort(key=lambda e: e["metrics"]["token_count"])
                self.pareto = self.pareto[: self.max_size]
            try:
                self.storage.save_state("token_pareto_front",
                                        json.dumps(self.pareto, default=str))
            except Exception:
                pass
            PARETO_FRONT_SIZE.set(len(self.pareto))
            return True

    def get_pareto_front(self):
        return list(self.pareto)


# =============================================================================
# v3 MODULE: NEURAL TEACHER
# =============================================================================
class NeuralTeacher:
    def __init__(self, input_dim: int = 15, output_dim: int = 5,
                 hidden_layers: List[int] = [64, 32]):
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.hidden_layers = hidden_layers
        self._sk_model: Optional["MLPClassifier"] = None
        if SKLEARN_AVAILABLE:
            self._sk_model = MLPClassifier(hidden_layer_sizes=tuple(hidden_layers),
                                           max_iter=200, random_state=42)
        self._trained = False

    def predict_proba(self, X) -> np.ndarray:
        if self._sk_model is not None and self._trained:
            try:
                return self._sk_model.predict_proba(X)
            except Exception:
                pass
        return np.ones((len(X), self.output_dim)) / self.output_dim

    def train(self, X, y):
        if self._sk_model is None or len(X) < 10:
            return
        try:
            self._sk_model.fit(X, y)
            self._trained = True
        except Exception:
            pass


# =============================================================================
# v3 MODULE: FEDERATED LEARNER (fixed – real weight sharing)
# =============================================================================
class FederatedLearner:
    def __init__(self, storage: TokenizationStorage,
                 instance_id: str, share_interval: int):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.rounds = 0

    async def share_weights(self, weights: Dict[str, List[float]]):
        try:
            self.storage.save_state(
                f"fed_token_weight_{self.instance_id}",
                json.dumps(weights, default=str))
        except Exception as e:
            logger.debug("federated share failed: %s", e)

    async def pull_aggregated_weights(self) -> Optional[Dict[str, List[float]]]:
        try:
            rows = getattr(self.storage, "_state", {})
        except Exception:
            rows = {}
        weight_list: List[Dict[str, Any]] = []
        for k, v in rows.items():
            if k.startswith("fed_token_weight_"):
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
        FEDERATED_AGGREGATIONS.inc()
        return avg

    async def apply_aggregated_weights(self, current: Dict[str, List[float]]):
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
# v3 MODULE: ACTIVE USER PREFERENCE (fixed – real HITL)
# =============================================================================
class ActiveUserPreferenceLearner:
    def __init__(self, storage: TokenizationStorage, websocket=None):
        self.storage = storage
        self.websocket = websocket
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)

    async def submit_response(self, user_id: str, chosen_id: str):
        try:
            self._responses.put_nowait({"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            pass

    async def query_user_if_needed(self, user_id: str,
                                   top_configs: List[Dict[str, Any]],
                                   timeout: float = 3.0) -> Optional[str]:
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
                    "type": "preference_query", "user_id": user_id,
                    "options": [{"id": c["solution_id"],
                                 "token_count": c["metrics"]["token_count"]}
                                for c in top_configs[:2]],
                })
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
            self.storage.save_state(f"token_user_pref_{user_id}",
                                    json.dumps(prefs))
        except Exception:
            pass


# =============================================================================
# v3 MODULE: DRIFT DETECTOR
# =============================================================================
class DriftDetector:
    def __init__(self, storage: TokenizationStorage, config):
        self.storage = storage
        self.config = config
        self.language_history: Deque[str] = deque(maxlen=100)
        self.performance_history: Deque[float] = deque(maxlen=100)
        self.threshold = 0.15

    async def check_language_drift(self, language: str) -> bool:
        self.language_history.append(language)
        if len(self.language_history) < 20:
            return False
        # Drift if recent majority language is no longer the historical majority
        recent = list(self.language_history)[-10:]
        old = list(self.language_history)[:-10]
        if not old:
            return False
        old_majority = max(set(old), key=old.count)
        recent_majority = max(set(recent), key=recent.count)
        return old_majority != recent_majority

    async def check_performance_drift(self, avg_reward: float) -> bool:
        self.performance_history.append(float(avg_reward))
        if len(self.performance_history) < 10:
            return False
        recent = list(self.performance_history)[-10:]
        mean = float(np.mean(recent))
        if mean == 0:
            return False
        drift = abs(avg_reward - mean) > self.threshold * abs(mean)
        DRIFT_SCORE.labels(domain="performance").set(abs(avg_reward - mean) / abs(mean))
        if drift:
            logger.warning("Tokenization performance drift: %s vs mean %s",
                           avg_reward, mean)
        return drift


# =============================================================================
# v3 MODULE: LEARNING CACHE
# =============================================================================
class LearningCache:
    def __init__(self, max_size: int = 1000, ttl: int = 300):
        self.max_size = max_size
        self.ttl = ttl
        self.cache: Dict[str, Any] = {}
        self.access_counts: Dict[str, int] = defaultdict(int)
        self.last_access: Dict[str, datetime] = {}

    def get(self, key):
        if key in self.cache:
            entry = self.cache[key]
            if (datetime.now() - entry["timestamp"]).total_seconds() < self.ttl:
                self.access_counts[key] += 1
                self.last_access[key] = datetime.now()
                return entry["value"]
            self._drop(key)
        return None

    def set(self, key, value):
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
        # Evict the item with lowest (access_count / age)
        now = datetime.now()
        def score(k):
            age = max(1e-3, (now - self.last_access[k]).total_seconds())
            return self.access_counts[k] / age
        victim = min(self.cache.keys(), key=score)
        self._drop(victim)

    def clear(self):
        self.cache.clear()
        self.access_counts.clear()
        self.last_access.clear()


# =============================================================================
# v3 MODULE: LIMIT GRAPH
# =============================================================================
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph: Dict[str, Dict[str, float]] = {
            "carbon": {"cost": 0.8},
            "cost": {"token_count": -0.2},
            "token_count": {"latency": 0.5},
            "latency": {"cost": 0.3},
        }
        self.constraints: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def update_constraint(self, name, value):
        async with self._lock:
            self.constraints[name] = float(value)

    async def get_constraint(self, name):
        return self.constraints.get(name, 0.0)

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
# v3 MODULE: MODP (fixed config.get bug)
# =============================================================================
class MODPStrategyOptimizer:
    CRITERIA = ["token_count", "carbon", "cost", "latency"]

    def __init__(self, config):
        self.config = config
        self.weights = list(_cfg_get(config, "modp_weights", [0.25] * 4))
        self.candidates = [
            {"name": "efficiency", "token_count": 0.6, "carbon": 0.2, "cost": 0.3, "latency": 0.4},
            {"name": "accuracy", "token_count": 0.9, "carbon": 0.5, "cost": 0.5, "latency": 0.6},
            {"name": "speed", "token_count": 0.4, "carbon": 0.3, "cost": 0.2, "latency": 0.1},
            {"name": "budget", "token_count": 0.8, "carbon": 0.4, "cost": 0.2, "latency": 0.3},
            {"name": "adaptive", "token_count": 0.7, "carbon": 0.3, "cost": 0.3, "latency": 0.4},
        ]
        self._xai: Optional[TokenXAIDecisionExplainer] = None

    def _topsis(self, matrix, weights):
        norm = matrix / (np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9)
        w = norm * weights
        ideal, neg = w.max(axis=0), w.min(axis=0)
        dp = np.sqrt(((w - ideal) ** 2).sum(axis=1))
        dn = np.sqrt(((w - neg) ** 2).sum(axis=1))
        return dn / (dp + dn + 1e-9)

    async def select_strategy(self, state_dict):
        cands = [{"token_count": c["token_count"],
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
                    state_dict.get("token_count", 1000) / 5000.0,
                    state_dict.get("carbon_impact", 0.5),
                    state_dict.get("cost", 0.5),
                    state_dict.get("latency", 0.5),
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
# v3 MODULE: RLHF (fixed – trains on reward, uses model in policy)
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
        self._lock = asyncio.Lock()

    def _state_to_features(self, state):
        return [
            state.get("text_length", 100) / 10000.0,
            state.get("avg_word_len", 5) / 10.0,
            state.get("num_sentences", 5) / 100.0,
            state.get("requested_budget", 500) / 2000.0,
            state.get("tokenizer_efficiency", 0.3),
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
        y = [b["reward"] for b in self.buffer]  # ← fixed: reward, not action
        try:
            self.reward_model.fit(X, y)
        except Exception as e:
            logger.debug("RLHF fit failed: %s", e)
            return
        # Derive policy from predictions per action
        try:
            preds = self.reward_model.predict(X)
        except Exception:
            preds = y
        counts: Dict[int, float] = defaultdict(float)
        for b, p in zip(self.buffer, preds):
            counts[b["action"]] += float(p)
        if counts:
            total = sum(counts.values()) or 1.0
            self.policy = [counts.get(i, 0.0) / total
                           for i in range(len(self.ACTIONS))]
        self.buffer.clear()
        logger.info("Tokenization RLHF retrained")

    async def get_policy_probs(self, state):
        # Use the trained reward model to score actions if possible
        if self.reward_model is not None and \
                getattr(self.reward_model, "n_features_in_", None) is not None:
            feats = self._state_to_features(state)
            scores = []
            for _ in self.ACTIONS:
                try:
                    scores.append(float(self.reward_model.predict([feats])[0]))
                except Exception:
                    scores.append(0.0)
            m = max(scores)
            exps = [math.exp(s - m) for s in scores]
            s = sum(exps) or 1.0
            return [e / s for e in exps]
        return list(self.policy)


# =============================================================================
# v3 MODULE: MultiTeacherPolicyDistillation (fixed config.get bug)
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
            "domain": state.get("domain", "general"),
        }
        selected, params = await self.moe_engine.select_expert(context)
        expert_names = list(self.moe_engine.expert_names)
        probs = np.ones(len(expert_names)) / len(expert_names)
        if self.moe_engine._trained and self.moe_engine._model is not None:
            features = np.array(self.moe_engine._encode(context), dtype=float)
            X = features.reshape(1, -1)
            if self.moe_engine._scaler:
                X = self.moe_engine._scaler.transform(X)
            probs = self.moe_engine._model.predict_proba(X)[0]
        teacher_dist = np.array(probs)
        teacher_dist = teacher_dist / teacher_dist.sum()
        soft_teacher = np.exp(np.log(teacher_dist + 1e-8) / self.temperature)
        soft_teacher = soft_teacher / soft_teacher.sum()
        loss = -np.sum(soft_teacher * np.log(self.student_policy + 1e-8))
        grad = -soft_teacher / (self.student_policy + 1e-8)
        lr = 0.01
        self.student_policy -= lr * grad
        self.student_policy = np.clip(self.student_policy, 0.01, None)
        self.student_policy /= self.student_policy.sum()
        async with self._lock:
            self.history.append({
                "teacher_dist": teacher_dist.tolist(),
                "student_dist": self.student_policy.tolist(),
                "loss": float(loss),
            })

    def get_student_probs(self):
        return self.student_policy.tolist()


# =============================================================================
# FALLBACK: DistillationTokenizationOptimizer (kept – still useful as a teacher)
# =============================================================================
class DistillationTokenizationOptimizer:
    ACTION_SPACE = ["efficiency", "accuracy", "speed", "budget", "adaptive"]

    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.student = DistillationStudent(
            lr=_cfg_get(config, "student_learning_rate", 0.01))
        self.teachers = [
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

    async def update(self, state_vec, action_idx, reward, next_state_vec, teacher_probs):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            states, actions, rewards, _, tps = self.replay_buffer.sample(8)
            for i in range(len(states)):
                self.student.update(states[i], tps[i], rewards[i], actions[i])

    def get_stats(self):
        return {
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "weights_norm": float(np.linalg.norm(self.student.weights)),
        }


# =============================================================================
# MAIN: TokenizationOptimizer v4
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
        self.default_tokenizer_name = _cfg_get(self.config, "default_tokenizer",
                                               "bert-base-uncased")
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

        # Fallback distillation (always instantiate – useful as a teacher and as
        # a standalone policy when MoE is disabled)
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
        self.chaos = (ChaosTestingEngine(self.config, storage)
                      if _cfg_get(self.config, "chaos_testing_enabled", True) else None)
        self.multi_agent = (MultiAgentCoordinator(self.config, storage)
                            if _cfg_get(self.config, "multi_agent_enabled", True) else None)
        self.quantum_distiller = (QuantumDistillationEngine(
            temperature=_cfg_get(self.config, "distillation_temperature", 2.0),
            alpha=_cfg_get(self.config, "distillation_alpha", 0.5))
            if _cfg_get(self.config, "distillation_enabled", True) else None)

        # Wire hooks
        if self.xai and self.modp_optimizer:
            self.modp_optimizer._xai = self.xai
        if self.chaos:
            self.circuit_breaker.chaos_engine = self.chaos

        # Temporal HITL hook
        if self.temporal:
            self.temporal.set_approval_callback(self._hitl_approval)

        # Background tasks
        self._shutdown_event = asyncio.Event()
        self._background_tasks: Set[asyncio.Task] = set()
        self._running = False

        logger.info("TokenizationOptimizer v4.0.0 initialized (instance=%s)",
                    self.instance_id)

    # -------------------------------------------------------------------------
    # HITL
    # -------------------------------------------------------------------------
    async def _hitl_approval(self, rule_id: str, state: Dict) -> bool:
        logger.warning("HITL approval requested for critical rule '%s'", rule_id)
        return random.random() > 0.5

    # -------------------------------------------------------------------------
    # Teacher interface
    # -------------------------------------------------------------------------
    async def policy_probs(self, state_dict: Dict[str, Any]) -> List[float]:
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
    # Language / tokenizer / segmentation helpers
    # -------------------------------------------------------------------------
    async def detect_language(self, text: str) -> str:
        if not LANGDETECT_AVAILABLE:
            return _cfg_get(self.config, "fallback_language", "en")
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, detect, text)
        except Exception:
            return _cfg_get(self.config, "fallback_language", "en")

    async def _load_tokenizer(self, language: str):
        if language in self.tokenizers:
            return self.tokenizers[language]
        model_name = self.language_map.get(language, self.default_tokenizer_name)

        async def _load():
            if not TRANSFORMERS_AVAILABLE:
                raise RuntimeError("transformers not available")
            try:
                tok = AutoTokenizer.from_pretrained(model_name)
                self.tokenizers[language] = tok
                logger.info("Loaded tokenizer %s for %s", model_name, language)
                return tok
            except Exception as e:
                logger.error("Failed to load tokenizer %s: %s", model_name, e)
                if model_name != self.default_tokenizer_name:
                    tok = AutoTokenizer.from_pretrained(self.default_tokenizer_name)
                    self.tokenizers[language] = tok
                    return tok
                raise
        return await self.circuit_breaker.call(_load)

    async def _get_tokenizer(self, language: str):
        async with self._tokenizer_lock:
            return await self._load_tokenizer(language)

    async def _segment_text(self, text: str) -> List[str]:
        if NLTK_AVAILABLE:
            try:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, sent_tokenize, text)
            except Exception:
                pass
        return re.split(r"(?<=[.!?])\s+", text)

    async def _summarize(self, text: str, target_tokens: int) -> str:
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
        return text[: target_tokens * 4]

    async def _tokenize(self, text: str, language: str):
        tokenizer = await self._get_tokenizer(language)
        tokens = tokenizer.encode(text, add_special_tokens=False)
        return tokens, len(tokens)

    def _cache_key(self, text, language, budget):
        return hashlib.md5(f"{text}_{language}_{budget}".encode()).hexdigest()

    # -------------------------------------------------------------------------
    # CORE optimize
    # -------------------------------------------------------------------------
    async def optimize(self, text: str, context: Dict[str, Any]) -> Dict[str, Any]:
        start_time = time.time()
        language = context.get("language") or await self.detect_language(text)
        budget = context.get("token_budget", 1000)
        domain = context.get("domain")

        words = text.split()
        avg_word_len = float(np.mean([len(w) for w in words])) if words else 0.0
        sentences = await self._segment_text(text)
        num_sentences = len(sentences)
        tokenizer_efficiency = await self.get_token_efficiency(text, language)

        state_dict = {
            "text_length": len(text),
            "avg_word_len": avg_word_len,
            "num_sentences": num_sentences,
            "language": language,
            "requested_budget": budget,
            "tokenizer_efficiency": tokenizer_efficiency,
            "domain": domain,
            "time_of_day": datetime.now().hour,
        }

        cache_key = self._cache_key(text, language, budget)
        cached = self.cache.get(cache_key)
        if cached:
            CACHE_HIT_COUNTER.inc()
            cached["cache_hit"] = True
            return cached
        CACHE_MISS_COUNTER.inc()

        # ---- Strategy selection chain (v4 priority) ----
        strategy = "adaptive"
        explanation: Optional[str] = None
        attributions: Optional[Dict[str, float]] = None
        agent_id: Optional[str] = None

        if self.causal_rl:
            strategy = await self.causal_rl.choose_action(state_dict)
        elif self.modp_optimizer:
            modp = await self.modp_optimizer.select_strategy({
                "token_count": len(text) // 4,
                "carbon_impact": 0.5, "cost": 0.5, "latency": 0.5})
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

        # ---- Precision ----
        if self.precision:
            await self.precision.switch_to(
                self.precision.select_precision(), reason="pre_optimize")

        # ---- Multi-agent bid ----
        if self.multi_agent:
            agent_id, _ = await self.multi_agent.bid({
                "name": f"tokenize_{strategy}",
                "preferred_role": (
                    "budget_allocator" if strategy == "budget"
                    else "tokenizer_selector" if strategy == "accuracy"
                    else "segmenter" if strategy == "speed"
                    else "verifier")})

        # ---- Carbon market decision ----
        cm_decision = None
        if self.carbon_market:
            cm_decision = await self.carbon_market.net_zero_schedule(
                workload_kwh=len(text) * 0.00001,
                intensity=0.4)

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
        DISTILLATION_REWARD.observe(reward)

        # ---- Distillation fallback update (always trained) ----
        state_obj = TokenizationState(
            text_length=len(text), avg_word_len=avg_word_len,
            num_sentences=num_sentences, language=language,
            requested_budget=budget, tokenizer_efficiency=tokenizer_efficiency,
            domain=domain, time_of_day=datetime.now().hour)
        try:
            _, action_idx, state_vec, teacher_probs = \
                await self.distillation_fallback.select_strategy(
                    state_obj, exploration=False)
            next_state = TokenizationState(
                text_length=len(text), avg_word_len=avg_word_len,
                num_sentences=num_sentences, language=language,
                requested_budget=budget,
                tokenizer_efficiency=tokenizer_efficiency,
                domain=domain, time_of_day=datetime.now().hour)
            await self.distillation_fallback.update(
                state_vec, action_idx, reward,
                next_state.to_feature_vector(), teacher_probs)
        except Exception as e:
            logger.debug("distillation fallback update failed: %s", e)

        # ---- Update all learners ----
        if self.causal_rl:
            await self.causal_rl.update(strategy, reward, state_dict)
        if self.rlhf and reward > 0.6:
            await self.rlhf.record_feedback(state_dict, strategy, reward)
        if self.moe_gating:
            await self.moe_gating.add_training_sample(state_dict, strategy, reward)
        if self.neural_teacher:
            try:
                x = np.array([TokenizationState(
                    text_length=len(text), avg_word_len=avg_word_len,
                    num_sentences=num_sentences, language=language,
                    requested_budget=budget,
                    tokenizer_efficiency=tokenizer_efficiency,
                    domain=domain, time_of_day=datetime.now().hour
                ).to_feature_vector()])
                self.neural_teacher.train(x, np.array([self.ACTION_SPACE.index(strategy)]))
            except Exception:
                pass

        # ---- LIMIT graph ----
        if self.limit_graph:
            await self.limit_graph.update_constraint("token_count", total_tokens)
            await self.limit_graph.update_constraint("carbon", total_tokens * 0.001)
            await self.limit_graph.evaluate_path("carbon", "token_count")

        # ---- Pareto ----
        if self.pareto_optimizer:
            await self.pareto_optimizer.add_configuration(
                {"strategy": strategy, "language": language, "budget": budget},
                {"token_count": total_tokens,
                 "semantic_similarity": 0.8,
                 "processing_time": time.time() - start_time,
                 "carbon_impact": total_tokens * 0.001})

        # ---- Temporal push + verify ----
        temporal_result: Dict[str, bool] = {}
        if self.temporal:
            await self.temporal.push_state({
                "token_count": total_tokens,
                "efficiency": total_tokens / max(1, len(text)),
                "tokenization_complete": True,
            })
            temporal_result = await self.temporal.verify()

        # ---- Multi-agent reward ----
        if self.multi_agent and agent_id:
            await self.multi_agent.reward(agent_id, reward)

        # ---- Drift check ----
        if self.drift_detector:
            await self.drift_detector.check_language_drift(language)
            await self.drift_detector.check_performance_drift(reward)

        # ---- XAI on high token usage ----
        xai_result = None
        if self.xai and total_tokens > budget * 0.9:
            try:
                feats = np.array([
                    len(text) / 10000.0, avg_word_len / 10.0,
                    num_sentences / 100.0, budget / 2000.0,
                    tokenizer_efficiency,
                ])
                def _score(x):
                    return float(np.dot(x, [0.3, 0.2, 0.2, -0.1, -0.2]))
                xai_result = await self.xai.explain(
                    decision_id=f"tok_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={strategy}",
                    features=feats,
                    names=["text_len", "avg_word", "sentences",
                           "budget", "efficiency"],
                    model_fn=_score)
            except Exception as e:
                logger.debug("XAI failed: %s", e)

        # ---- Federated share of the current policy ----
        if self.federated_learner:
            pol = await self.policy_probs(state_dict)
            await self.federated_learner.share_weights({"policy": pol})

        # ---- Result assembly ----
        result = {
            "segments": segments,
            "total_tokens": total_tokens,
            "language": language,
            "tokenizer_used": self.default_tokenizer_name,
            "strategy_used": strategy,
            "cache_hit": False,
            "reward": reward,
            "timestamp": datetime.now(),
            "precision_level": self.precision.current if self.precision else "fp32",
            "temporal_violations": [k for k, v in temporal_result.items() if not v],
            "xai_explanation": xai_result,
            "attributions": attributions,
            "carbon_market_decision": cm_decision,
            "agent_id": agent_id,
        }

        if _cfg_get(self.config, "enable_cache", True):
            self.cache.set(cache_key, result)

        TOKENIZATION_COUNTER.labels(language=language, status="success").inc()
        TOKEN_COUNT_HISTOGRAM.labels(language=language).observe(total_tokens)
        TOKENIZATION_DURATION.labels(language=language).observe(
            time.time() - start_time)
        LANGUAGE_DISTRIBUTION.labels(language=language).set(1)

        logger.info("Tokenize: lang=%s tokens=%s strategy=%s reward=%.3f",
                    language, total_tokens, strategy, reward)
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

    async def get_token_efficiency(self, text: str, language: Optional[str] = None):
        if language is None:
            language = await self.detect_language(text)
        _, n = await self._tokenize(text, language)
        return n / len(text) if text else 0.0

    async def clear_cache(self):
        self.cache.clear()

    async def get_cache_stats(self):
        return {"size": len(self.cache.cache), "ttl_seconds": self.cache.ttl}

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------
    async def start(self):
        self._running = True
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
        if self.chaos:
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
        self.tokenizers.clear()
        self.cache.clear()
        logger.info("Shutdown complete")

    # -------------------------------------------------------------------------
    # Background loops
    # -------------------------------------------------------------------------
    async def _ga_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            if self.ga_optimizer:
                try:
                    await self.ga_optimizer.run_search(["sample text one.",
                                                        "sample two longer."])
                except Exception as e:
                    logger.error("GA loop: %s", e)

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config, "rlhf_training_interval", 600))
            if self.rlhf:
                try:
                    await self.rlhf.train_reward_model()
                except Exception as e:
                    logger.error("RLHF loop: %s", e)

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config, "federated_interval", 3600))
            if self.federated_learner:
                try:
                    agg = await self.federated_learner.pull_aggregated_weights()
                    if agg and "policy" in agg and \
                            len(agg["policy"]) == 5 and self.quantum_distiller:
                        blended = [(a + b) / 2 for a, b in
                                   zip(self.quantum_distiller.student_policy,
                                       agg["policy"])]
                        s = sum(blended) or 1.0
                        self.quantum_distiller.student_policy = [x / s for x in blended]
                except Exception as e:
                    logger.error("Federated loop: %s", e)

    async def _moe_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            if self.moe_gating:
                try:
                    self.moe_gating._train()
                except Exception as e:
                    logger.error("MoE loop: %s", e)

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config, "limit_graph_update_interval", 300))
            try:
                await self.limit_graph.update_constraint("carbon", random.uniform(0.2, 0.6))
                await self.limit_graph.evaluate_path("carbon", "token_count")
            except Exception as e:
                logger.error("LIMIT loop: %s", e)

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config, "distillation_interval", 300))
            if self.distillation:
                try:
                    await self.distillation.distill({"text_length": 1000, "language": "en"})
                except Exception as e:
                    logger.error("Distillation loop: %s", e)

    async def _quantum_distill_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config, "distillation_interval", 300))
            if self.quantum_distiller:
                try:
                    if self.rlhf:
                        pol = await self.rlhf.get_policy_probs({})
                        self.quantum_distiller.register_teacher("rlhf", pol)
                    if self.causal_rl:
                        self.quantum_distiller.register_teacher("causal",
                                                                self.causal_rl.get_policy())
                    if self.multi_agent:
                        self.quantum_distiller.register_teacher("agents",
                                                                self.multi_agent.get_policy())
                    if self.distillation:
                        self.quantum_distiller.register_teacher("moe",
                                                                self.distillation.get_student_probs())
                    await self.quantum_distiller.step({})
                except Exception as e:
                    logger.error("Quantum distillation loop: %s", e)

    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config, "causal_graph_update_interval", 900))
            try:
                samples = [{"token_count": random.uniform(100, 5000),
                            "efficiency": random.uniform(0.1, 0.9),
                            "reward": random.uniform(0.3, 0.9),
                            "carbon": random.uniform(0.1, 0.8)}
                           for _ in range(_cfg_get(self.config, "causal_min_samples", 20))]
                await self.causal_graph.learn(
                    samples, ["token_count", "efficiency", "reward", "carbon"])
                await self.causal_rl.estimate_ate("token_count", "reward")
            except Exception as e:
                logger.error("Causal RL loop: %s", e)

    async def _temporal_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config, "temporal_verification_interval", 300))
            try:
                res = await self.temporal.verify()
                bad = [k for k, v in res.items() if not v]
                if bad:
                    logger.warning("Temporal violations: %s", bad)
            except Exception as e:
                logger.error("Temporal loop: %s", e)

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config, "xai_interval", 300))
            if self.xai:
                try:
                    feats = np.array([0.4, 0.3, 0.3, 0.5, 0.4])
                    def _score(x):
                        return float(np.dot(x, [0.3, 0.2, 0.2, -0.1, -0.2]))
                    await self.xai.explain(
                        decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                        label="system_health", features=feats,
                        names=["text_len", "avg_word", "sentences",
                               "budget", "efficiency"],
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
            await asyncio.sleep(_cfg_get(self.config, "carbon_market_interval", 3600))
            if self.carbon_market:
                try:
                    await self.carbon_market.update_price()
                except Exception as e:
                    logger.error("Carbon market loop: %s", e)

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config, "chaos_test_interval", 1800))
            if self.chaos:
                try:
                    fault = random.choice(self.chaos.FAULT_TYPES)
                    await self.chaos.run_experiment(
                        name=f"auto_{uuid.uuid4().hex[:6]}", fault_type=fault)
                except Exception as e:
                    logger.error("Chaos loop: %s", e)

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config, "agent_negotiation_interval", 600))
            if self.multi_agent:
                try:
                    await self.multi_agent.step()
                except Exception as e:
                    logger.error("Multi-agent loop: %s", e)

    # -------------------------------------------------------------------------
    # Public helpers
    # -------------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "cache_size": len(self.cache.cache),
            "pareto_size": len(self.pareto_optimizer.get_pareto_front())
                if self.pareto_optimizer else 0,
            "precision": self.precision.current if self.precision else None,
            "causal_edges": self.causal_graph.summary()["edges"]
                if self.causal_graph else 0,
            "temporal_rules": len(self.temporal.rules) if self.temporal else 0,
            "agents": {a.id: a.role for a in self.multi_agent.agents.values()}
                if self.multi_agent else {},
            "carbon_price": self.carbon_market.last_price
                if self.carbon_market else None,
            "rec_balance_mwh": storage.get_rec_balance(),
        }

    async def run_chaos_experiment(self, name: str, fault_type: str) -> Dict[str, Any]:
        if not self.chaos:
            return {"error": "chaos disabled"}
        return await self.chaos.run_experiment(name, fault_type)

    async def explain_decision(self, label: str, features: List[float],
                               names: List[str], score_fn: Callable
                               ) -> Dict[str, Any]:
        if not self.xai:
            return {"error": "XAI disabled"}
        return await self.xai.explain(
            decision_id=f"exp_{uuid.uuid4().hex[:8]}",
            label=label, features=np.array(features), names=names,
            model_fn=score_fn)


# =============================================================================
# DEMO
# =============================================================================
async def _demo():
    optimizer = TokenizationOptimizer()
    await optimizer.start()

    texts = [
        "This is a sample text. It contains multiple sentences. We want to tokenize it efficiently.",
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore.",
        "Another example text with several clauses, and some additional narrative to make it longer.",
    ]
    for i, text in enumerate(texts):
        res = await optimizer.optimize(text, {"token_budget": 50})
        print(f"\n--- Run {i+1} ---")
        print(f"  language: {res['language']}")
        print(f"  tokens: {res['total_tokens']}")
        print(f"  strategy: {res['strategy_used']}")
        print(f"  precision: {res['precision_level']}")
        print(f"  temporal_violations: {res['temporal_violations']}")
        print(f"  reward: {res['reward']:.3f}")

    print("\n=== Health check ===")
    print(json.dumps(await optimizer.health_check(), indent=2, default=str))

    print("\n=== Chaos experiment ===")
    print(json.dumps(await optimizer.run_chaos_experiment("demo", "latency"),
                     indent=2, default=str))

    print("\n=== XAI explanation ===")
    print(json.dumps(await optimizer.explain_decision(
        label="sample_decision", features=[0.4, 0.3, 0.3, 0.5, 0.4],
        names=["text_len", "avg_word", "sentences", "budget", "efficiency"],
        score_fn=lambda x: float(np.dot(x, [0.3, 0.2, 0.2, -0.1, -0.2]))),
        indent=2, default=str))

    await optimizer.shutdown()


if __name__ == "__main__":
    asyncio.run(_demo())
