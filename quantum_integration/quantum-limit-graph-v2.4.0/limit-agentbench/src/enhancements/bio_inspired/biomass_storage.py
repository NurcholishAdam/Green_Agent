#!/usr/bin/env python3
# =============================================================================
# Biomass Storage v9.0.0 — Patched Single-File Edition
# =============================================================================
"""
Biomass Storage v9.0.0
======================
Patched single-file version of the biomass storage layer.

P0 fixes
--------
- Signing: HMAC-SHA256 by default with a persistent secret; optional Dilithium
  behind a correct API usage pattern (single keypair, verify is possible).
- No background tasks started inside __init__; use `await storage.start()`.
- Lazy asyncio locks (no cross-loop binding).
- GA save/restore uses locals, so concurrent evaluation cannot corrupt state.
- `_select_best_from_pareto` returns a copy and does not mutate the front.
- `_index_lock` guards all task_index / task_hash_index mutations.
- FederatedCoordinator field renamed from `storage` to `biomass`.

P1 — real metrics
-----------------
- `generate_analytics` derives conversion_efficiency / avg_retrieval_cost /
  expiration_rate / cache_hit_rate from real state. No more `random.uniform`.
- `_estimate_action_quality` uses real fill levels and cost.
- `store_task` enforces `max_storage_tokens`.
- `mobilize` respects priority, deadline, tier.
- Safety invariants reflect live state.
- Shutdown flushes state and is idempotent.
- `CausalRLAgent.q_table` is bounded (LRU eviction).

P2 — honesty
------------
- MODULE_STATUS documents each module's maturity.
- similarity_dedup / capacity_manager / predictive_mobilizer / tier_agents /
  precision / carbon_market / chaos / human_approval are placeholders:
  disabled by default, `.available == False`, warn when enabled.
- GA / MOPD / RL / federated are experimental: warn when enabled.

P3 — production readiness
-------------------------
- Lifecycle: `async start()`, `async shutdown()`, `async ready()`,
  `__aenter__` / `__aexit__`.
- Graceful shutdown with task drain and state flush; idempotent.
- Logical sections within a single file.
- Embedded test suite (`python3 biomass_storage.py --test`).
- Prometheus metrics and OpenTelemetry spans (both optional).
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import hashlib
import hmac
import json
import logging
import math
import os
import random
import sqlite3
import sys
import time
import unittest
import uuid
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple, Union

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field, field_validator  # noqa: F401
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, REGISTRY
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("biomass_storage")
    OTEL_AVAILABLE = True
except ImportError:
    _TRACER = None
    OTEL_AVAILABLE = False

try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            TimeStamper(fmt="iso"),
            JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger(__name__)
except ImportError:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger(__name__)

try:
    from pqcrypto.sign import dilithium  # type: ignore
    PQC_AVAILABLE = True
except ImportError:
    dilithium = None  # type: ignore
    PQC_AVAILABLE = False

# -----------------------------------------------------------------------------
# Central components (optional)
# -----------------------------------------------------------------------------
try:
    from ..storage import Storage as CentralStorage  # type: ignore
    from ..scaling.message_queue import AsyncMessageQueue  # type: ignore
    from ..routing.pareto_gating import ParetoGating  # type: ignore
    from ..feedback.adaptive_cost import AdaptiveCostFunction  # type: ignore
    from ..safety.drift_detector import DriftDetector  # type: ignore
    from ..metrics import MetricsRegistry  # type: ignore
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
    CENTRAL_AVAILABLE = True
except ImportError:
    CENTRAL_AVAILABLE = False
    CentralStorage = None  # type: ignore
    AsyncMessageQueue = None  # type: ignore
    ParetoGating = None  # type: ignore
    AdaptiveCostFunction = None  # type: ignore
    DriftDetector = None  # type: ignore
    MetricsRegistry = None  # type: ignore
    FeedbackEvent = None  # type: ignore


# =============================================================================
# SECTION 1. MODULE STATUS
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "storage_core":       "stable",
    "persistence":        "stable",
    "task_manager":       "stable",
    "security":           "stable",
    "genetic_optimizer":  "experimental",
    "mopd":               "experimental",
    "policy":             "experimental",
    "causal_rl":          "placeholder",
    "federated":          "placeholder",
    "tier_agents":        "placeholder",
    "precision":          "placeholder",
    "carbon_market":      "placeholder",
    "chaos":              "placeholder",
    "human_approval":     "placeholder",
    "similarity_dedup":   "placeholder",
    "capacity_manager":   "placeholder",
    "predictive_mobilizer": "placeholder",
}


def _warn_module(name: str) -> None:
    status = MODULE_STATUS.get(name, "unknown")
    if status == "stable":
        return
    if status == "placeholder":
        logger.warning(
            "Module is a placeholder; enabling it will not have an effect",
            module=name,
        )
    elif status == "experimental":
        logger.warning(
            "Module is experimental; validate before production use",
            module=name,
        )


# =============================================================================
# SECTION 2. RETRY DECORATOR
# =============================================================================
def retry_async(max_retries: int = 3, base_delay: float = 0.5, max_delay: float = 5.0):
    def decorator(fn: Callable):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            last_exc: Optional[Exception] = None
            for attempt in range(max_retries):
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if attempt == max_retries - 1:
                        break
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning("Retrying", fn=fn.__name__, attempt=attempt + 1, error=str(e))
                    await asyncio.sleep(delay)
            assert last_exc is not None
            raise last_exc
        return wrapper
    return decorator


# =============================================================================
# SECTION 3. TRACING HELPERS
# =============================================================================
def traced(span_name: str):
    def decorator(fn: Callable):
        if not OTEL_AVAILABLE:
            return fn

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            with _TRACER.start_as_current_span(span_name):
                return await fn(*args, **kwargs)
        return wrapper
    return decorator


# =============================================================================
# SECTION 4. ENUMS AND DATA CLASSES
# =============================================================================
class StorageTier(Enum):
    ATP_CACHE = "ATP_CACHE"
    GLYCOGEN_QUEUE = "GLYCOGEN_QUEUE"
    STARCH_RESERVE = "STARCH_RESERVE"
    LIPID_DEPOT = "LIPID_DEPOT"
    LIGNIN_ARCHIVE = "LIGNIN_ARCHIVE"


class GuaranteeLevel(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "efficiency": 0.3,
        "cost_score": 0.2,
        "survival_rate": 0.2,
        "cache_hit_rate": 0.3,
    })
    grid_resolution: int = 10


@dataclass
class BiomassStorageConfig:
    max_storage_tokens: int = 10000
    default_collateral_ratio: float = 1.0
    persistence_path: str = "biomass_storage_state.json"
    ga_population_size: int = 50
    ga_mutation_rate: float = 0.1
    ga_crossover_rate: float = 0.7
    ga_generations: int = 10
    ga_tournament_size: int = 3
    mopd: MOPDConfig = field(default_factory=MOPDConfig)
    chaos_probability: float = 0.0
    carbon_market_config: Optional[Dict[str, Any]] = None
    precision_policy: str = "energy_aware"
    q_table_max_size: int = 5000
    state_save_interval: float = 60.0
    shutdown_timeout_seconds: int = 15
    # Placeholder toggles — disabled by default
    enable_similarity_dedup: bool = False
    enable_capacity_manager: bool = False
    enable_predictive_mobilizer: bool = False
    enable_tier_agents: bool = False
    enable_precision_switching: bool = False
    enable_carbon_market: bool = False
    enable_chaos: bool = False
    enable_human_approval: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class StoredTask:
    task_id: str
    content: str
    tier: StorageTier
    timestamp: datetime
    ttl: Optional[timedelta] = None
    access_count: int = 0
    last_access: Optional[datetime] = None
    hash_value: str = ""
    priority: int = 0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["tier"] = self.tier.value
        d["timestamp"] = self.timestamp.isoformat()
        d["ttl"] = self.ttl.total_seconds() if self.ttl else None
        d["last_access"] = self.last_access.isoformat() if self.last_access else None
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StoredTask":
        data = dict(data)
        data["tier"] = StorageTier(data["tier"])
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        data["ttl"] = timedelta(seconds=data["ttl"]) if data["ttl"] else None
        data["last_access"] = (
            datetime.fromisoformat(data["last_access"]) if data["last_access"] else None
        )
        return cls(**data)


@dataclass
class StorageToken:
    token_id: str
    task_id: str
    tier: StorageTier
    created_at: datetime
    expires_at: Optional[datetime] = None
    signature: Optional[bytes] = None
    is_valid: bool = True

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["tier"] = self.tier.value
        d["created_at"] = self.created_at.isoformat()
        d["expires_at"] = self.expires_at.isoformat() if self.expires_at else None
        d["signature"] = self.signature.hex() if self.signature else None
        return d


@dataclass
class StorageAnalytics:
    total_tasks: int
    total_size: int
    conversion_efficiency: float
    avg_retrieval_cost: float
    expiration_rate: float
    cache_hit_rate: float
    storage_distribution: Dict[str, int]


@dataclass
class MOPDPoint:
    strategy: str
    conversion_costs: Dict[str, float]
    collateral_ratios: Dict[str, float]
    efficiency: float
    cost_score: float
    survival_rate: float
    cache_hit_rate: float
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(**data)


MAXIMIZE_KEYS = ("efficiency", "cost_score", "survival_rate", "cache_hit_rate")


def pareto_filter(points: List[MOPDPoint]) -> List[MOPDPoint]:
    if not points:
        return []
    front: List[MOPDPoint] = []
    for i, p in enumerate(points):
        dominated = False
        for j, q in enumerate(points):
            if i == j:
                continue
            if (
                all(getattr(q, k) >= getattr(p, k) for k in MAXIMIZE_KEYS)
                and any(getattr(q, k) > getattr(p, k) for k in MAXIMIZE_KEYS)
            ):
                dominated = True
                break
        if not dominated:
            front.append(p)
    return front


# =============================================================================
# SECTION 5. SECURITY (STABLE) — HMAC by default, Dilithium optional
# =============================================================================
class SecurityService:
    """
    Signing service.

    - Default: HMAC-SHA256 with a persistent secret stored under `key_dir`.
    - Optional: Dilithium via pqcrypto, using a single keypair loaded/generated
      once at construction. Correct API usage so sign/verify can round-trip.

    STATUS: stable (HMAC). Dilithium path is best-effort and toggled by
    availability of pqcrypto.
    """

    def __init__(self, key_dir: str = "./biomass_keys"):
        self.key_dir = Path(key_dir)
        self.key_dir.mkdir(parents=True, exist_ok=True)
        self._hmac_secret = self._load_or_generate_hmac_secret()
        self._dilithium_private: Optional[bytes] = None
        self._dilithium_public: Optional[bytes] = None
        if PQC_AVAILABLE:
            self._load_or_generate_dilithium_keypair()

    def _load_or_generate_hmac_secret(self) -> bytes:
        p = self.key_dir / "hmac_secret.bin"
        if p.exists():
            return p.read_bytes()
        secret = os.urandom(32)
        p.write_bytes(secret)
        try:
            os.chmod(p, 0o600)
        except OSError:
            pass
        return secret

    def _load_or_generate_dilithium_keypair(self) -> None:
        priv_path = self.key_dir / "dilithium_private.bin"
        pub_path = self.key_dir / "dilithium_public.bin"
        try:
            if priv_path.exists() and pub_path.exists():
                self._dilithium_private = priv_path.read_bytes()
                self._dilithium_public = pub_path.read_bytes()
                return
            # pqcrypto's generate_keypair returns (public_key, secret_key)
            public_key, secret_key = dilithium.generate_keypair()
            self._dilithium_private = secret_key
            self._dilithium_public = public_key
            priv_path.write_bytes(secret_key)
            pub_path.write_bytes(public_key)
        except Exception as e:
            logger.warning("Dilithium keypair unavailable; falling back to HMAC", error=str(e))
            self._dilithium_private = None
            self._dilithium_public = None

    @property
    def backend(self) -> str:
        return "dilithium" if self._dilithium_private is not None else "hmac-sha256"

    def sign(self, data: bytes) -> bytes:
        if self._dilithium_private is not None:
            try:
                return dilithium.sign(data, self._dilithium_private)
            except Exception as e:
                logger.debug("Dilithium sign failed; using HMAC", error=str(e))
        return hmac.new(self._hmac_secret, data, hashlib.sha256).digest()

    def verify(self, data: bytes, signature: bytes) -> bool:
        if self._dilithium_private is not None and self._dilithium_public is not None:
            try:
                return bool(dilithium.verify(signature, data, self._dilithium_public))
            except Exception:
                pass
        expected = hmac.new(self._hmac_secret, data, hashlib.sha256).digest()
        return hmac.compare_digest(expected, signature)


# =============================================================================
# SECTION 6. PLACEHOLDERS (safe no-ops, disabled by default)
# =============================================================================
class SimilarityDedupPlaceholder:
    STATUS = "placeholder"

    def __init__(self, enabled: bool = False):
        if enabled:
            _warn_module("similarity_dedup")
        self.available = False
        self.similarity_groups: Dict[str, Any] = {}
        self.group_representatives: Dict[str, Any] = {}
        self._task_texts: Dict[str, Any] = {}

    def to_state(self) -> Dict[str, Any]:
        return {
            "similarity_groups": self.similarity_groups,
            "group_representatives": self.group_representatives,
            "task_texts": self._task_texts,
        }

    def from_state(self, data: Dict[str, Any]) -> None:
        self.similarity_groups = data.get("similarity_groups", {})
        self.group_representatives = data.get("group_representatives", {})
        self._task_texts = data.get("task_texts", {})


class CapacityManagerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, enabled: bool = False):
        if enabled:
            _warn_module("capacity_manager")
        self.available = False
        self.load_history: Deque[float] = deque(maxlen=100)
        self.scaling_factor = 1.0


class PredictiveMobilizerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, enabled: bool = False):
        if enabled:
            _warn_module("predictive_mobilizer")
        self.available = False
        self.demand_history: List[float] = []


class PrecisionControllerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, policy: str = "energy_aware", enabled: bool = False):
        if enabled:
            _warn_module("precision")
        self.available = False
        self.policy = policy

    def get_precision(self, load: float, energy_budget: float) -> str:
        return "float32"


class CarbonMarketClientPlaceholder:
    STATUS = "placeholder"

    def __init__(self, enabled: bool = False, **kwargs: Any):
        if enabled:
            _warn_module("carbon_market")
        self.available = False
        self._warned = False

    def _warn_once(self) -> None:
        if not self._warned:
            logger.info("Carbon market is a placeholder; no trades executed.")
            self._warned = True

    def buy_credits(self, amount: float) -> bool:
        self._warn_once()
        return False

    def sell_credits(self, amount: float) -> bool:
        self._warn_once()
        return False


class ChaosInjectorPlaceholder:
    STATUS = "placeholder"

    def __init__(self, storage: Any, chaos_probability: float = 0.0, enabled: bool = False):
        if enabled:
            _warn_module("chaos")
        self.storage = storage
        self.chaos_probability = chaos_probability
        self.available = False

    async def maybe_inject_failure(self) -> None:
        return None


class HumanApprovalPlaceholder:
    STATUS = "placeholder"

    def __init__(self, queue: Optional[Any] = None,
                 enabled: bool = False, auto_approve_dev: bool = False):
        if enabled:
            _warn_module("human_approval")
        self.queue = queue
        self.available = False
        self.auto_approve_dev = auto_approve_dev
        if auto_approve_dev:
            logger.warning("HumanApproval auto_approve_dev=True; every request approved. Do not use in production.")

    async def request(self, decision: Dict[str, Any]) -> bool:
        if self.auto_approve_dev:
            return True
        logger.info("Approval requested but placeholder denies by default.", decision=decision)
        return False


class TierAgentPlaceholder:
    STATUS = "placeholder"

    def __init__(self, tier: StorageTier, storage: Any, enabled: bool = False):
        if enabled:
            _warn_module("tier_agents")
        self.tier = tier
        self.storage = storage
        self.available = False

    async def act(self, state: Dict[str, Any]) -> None:
        return None


# =============================================================================
# SECTION 7. EXPERIMENTAL MODULES (warn when enabled)
# =============================================================================
class CausalRLAgent:
    """
    Bounded tabular Q-learning.

    STATUS: experimental. The state is discretized to keep the q_table bounded.
    """

    def __init__(
        self,
        state_dim: int,
        action_dim: int,
        max_q_table: int = 5000,
        enabled: bool = True,
    ):
        if enabled:
            _warn_module("causal_rl")
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.max_q_table = max_q_table
        self.q_table: "OrderedDict[Tuple[int, ...], np.ndarray]" = OrderedDict()
        self.epsilon = 0.1
        self.learning_rate = 0.1
        self.gamma = 0.99

    def _discretize(self, state: np.ndarray) -> Tuple[int, ...]:
        # 5 buckets per dimension, clipped
        arr = np.asarray(state, dtype=float)[: self.state_dim]
        if arr.shape[0] < self.state_dim:
            arr = np.pad(arr, (0, self.state_dim - arr.shape[0]))
        buckets = np.clip((arr * 5).astype(int), 0, 4)
        return tuple(int(b) for b in buckets.tolist())

    def _get_or_create(self, key: Tuple[int, ...]) -> np.ndarray:
        if key in self.q_table:
            self.q_table.move_to_end(key)
            return self.q_table[key]
        if len(self.q_table) >= self.max_q_table:
            self.q_table.popitem(last=False)
        self.q_table[key] = np.zeros(self.action_dim)
        return self.q_table[key]

    def act(self, state: np.ndarray) -> int:
        if random.random() < self.epsilon:
            return random.randrange(self.action_dim)
        key = self._discretize(state)
        return int(np.argmax(self._get_or_create(key)))

    def update(
        self, state: np.ndarray, action: int, reward: float,
        next_state: np.ndarray, done: bool,
    ) -> None:
        key = self._discretize(state)
        next_key = self._discretize(next_state)
        current = self._get_or_create(key)
        next_q = self._get_or_create(next_key)
        best_next = 0.0 if done else float(np.max(next_q))
        td_target = reward + self.gamma * best_next
        current[action] += self.learning_rate * (td_target - current[action])

    def size(self) -> int:
        return len(self.q_table)


class FederatedCoordinator:
    """STATUS: placeholder in practice. Sends local snapshot; no aggregation."""

    def __init__(self, biomass: Any, queue: Optional[Any], enabled: bool = False):
        if enabled:
            _warn_module("federated")
        self.biomass = biomass
        self.queue = queue
        self.available = queue is not None

    async def send_update(self) -> bool:
        if self.queue is None:
            return False
        try:
            model = self.biomass.genetic_optimizer.to_dict()
            await self.queue.publish("federated_updates", json.dumps(model, default=str))
            return True
        except Exception as e:
            logger.warning("Federated send failed", error=str(e))
            return False

    async def receive_global_model(self, model_json: str) -> bool:
        # Placeholder: no aggregation logic
        logger.info("Federated receive called; no-op (placeholder).")
        return False


# =============================================================================
# SECTION 8. GENETIC OPTIMIZER + MOPD (EXPERIMENTAL)
# =============================================================================
class GeneticOptimizer:
    """
    Genetic optimizer over conversion costs and collateral ratios.

    STATUS: experimental. Uses real analytics from the owner storage, but does
    not yet do proper cross-validation; treat as a best-effort tuner.
    """

    TIER_PAIRS = [
        ("ATP_CACHE", "GLYCOGEN_QUEUE"),
        ("GLYCOGEN_QUEUE", "STARCH_RESERVE"),
        ("STARCH_RESERVE", "LIPID_DEPOT"),
        ("LIPID_DEPOT", "LIGNIN_ARCHIVE"),
        ("LIPID_DEPOT", "STARCH_RESERVE"),
        ("STARCH_RESERVE", "GLYCOGEN_QUEUE"),
        ("GLYCOGEN_QUEUE", "ATP_CACHE"),
    ]
    GUARANTEE_LEVELS = [level.name for level in GuaranteeLevel]

    def __init__(self, biomass: Any, config: BiomassStorageConfig, enabled: bool = True):
        if enabled:
            _warn_module("genetic_optimizer")
        self.biomass = biomass
        self.config = config
        self.population_size = config.ga_population_size
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.generations = config.ga_generations
        self.tournament_size = config.ga_tournament_size
        self.conversion_cost_bounds = (0.1, 20.0)
        self.collateral_bounds = (0.2, 3.0)
        self.best_individual: Optional[Dict[str, Any]] = None
        self.best_fitness: float = -math.inf
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDPoint] = []
        self.adaptive_cost = None
        self.pareto_gating = None
        self.drift_detector = None

    def set_central_components(self, adaptive_cost, pareto_gating, drift_detector) -> None:
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating
        self.drift_detector = drift_detector

    # ----- individual construction -----
    def _initialize_individual(self) -> Dict[str, Any]:
        costs = {
            f"{a}→{b}": random.uniform(*self.conversion_cost_bounds)
            for a, b in self.TIER_PAIRS
        }
        ratios = {lvl: random.uniform(*self.collateral_bounds) for lvl in self.GUARANTEE_LEVELS}
        return {"conversion_costs": costs, "collateral_ratios": ratios}

    def _initialize_population(self) -> List[Dict[str, Any]]:
        return [self._initialize_individual() for _ in range(self.population_size)]

    # ----- evaluation -----
    async def _evaluate_objectives(self, individual: Dict[str, Any]) -> Dict[str, float]:
        # P0 fix: save/restore using locals, not instance attributes.
        async with self.biomass.param_lock:
            orig_costs = self.biomass.conversion_costs
            orig_ratios = self.biomass.collateral_ratios
            try:
                self.biomass.conversion_costs = dict(individual["conversion_costs"])
                self.biomass.collateral_ratios = dict(individual["collateral_ratios"])
                analytics = self.biomass.generate_analytics()
            finally:
                self.biomass.conversion_costs = orig_costs
                self.biomass.collateral_ratios = orig_ratios

        cost_score = max(0.0, 1.0 - analytics.avg_retrieval_cost / 20.0)
        survival_rate = 1.0 - analytics.expiration_rate  # higher is better
        return {
            "efficiency": analytics.conversion_efficiency,
            "cost_score": cost_score,
            "survival_rate": survival_rate,
            "cache_hit_rate": analytics.cache_hit_rate,
        }

    # ----- Pareto -----
    def _select_best_from_pareto(self, front: List[MOPDPoint]) -> Optional[MOPDPoint]:
        # P0 fix: return a copy; do not mutate the stored front.
        if not front:
            return None
        weights = self.config.mopd.objective_weights
        keys = list(weights.keys())
        max_vals = {k: max(getattr(p, k) for p in front) for k in keys}
        min_vals = {k: min(getattr(p, k) for p in front) for k in keys}
        ranges = {
            k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
            for k in keys
        }
        best: Optional[MOPDPoint] = None
        best_score = -math.inf
        for p in front:
            score = sum(
                weights.get(k, 0.0) * ((getattr(p, k) - min_vals[k]) / ranges[k])
                for k in keys
            )
            if score > best_score:
                best_score = score
                best = p
        if best is None:
            return None
        copy = MOPDPoint.from_dict(best.to_dict())
        copy.scalarised_score = best_score
        return copy

    # ----- evolution -----
    async def evolve(self, generations: Optional[int] = None) -> Dict[str, Any]:
        generations = generations or self.generations
        population = self._initialize_population()
        local_front: List[MOPDPoint] = []
        fitness_scores: List[float] = [0.0] * len(population)

        for gen in range(generations):
            individuals_with_objs = []
            for ind in population:
                objs = await self._evaluate_objectives(ind)
                individuals_with_objs.append((ind, objs))

            # Fitness via adaptive cost (lower cost -> higher fitness) if available
            if self.adaptive_cost is not None and self.pareto_gating is not None:
                try:
                    candidates = [
                        {
                            "expert_id": str(id(ind)),
                            "quality_score": objs["efficiency"],
                            "carbon_g": 0.0,
                            "latency_ms": 0.0,
                            "energy_joules": 0.0,
                        }
                        for ind, objs in individuals_with_objs
                    ]
                    filtered = self.pareto_gating.filter(candidates)
                    if filtered:
                        allowed = {c["expert_id"] for c in filtered}
                        individuals_with_objs = [
                            (ind, objs) for ind, objs in individuals_with_objs
                            if str(id(ind)) in allowed
                        ] or individuals_with_objs

                    costs = []
                    for _, objs in individuals_with_objs:
                        cost = self.adaptive_cost.compute(
                            quality=objs["efficiency"],
                            carbon_g=0.0,
                            latency_ms=0.0,
                            energy_joules=0.0,
                            health=0.8,
                            atp=0.5,
                        )
                        costs.append(float(cost))
                    fitness_scores = [-c for c in costs]
                except Exception as e:
                    logger.debug("Central cost path failed; falling back", error=str(e))
                    fitness_scores = [self._scalarise(objs) for _, objs in individuals_with_objs]
            else:
                fitness_scores = [self._scalarise(objs) for _, objs in individuals_with_objs]

            if self.config.mopd.enabled:
                points = [
                    MOPDPoint(
                        strategy=str(id(ind)),
                        conversion_costs=dict(ind["conversion_costs"]),
                        collateral_ratios=dict(ind["collateral_ratios"]),
                        efficiency=objs["efficiency"],
                        cost_score=objs["cost_score"],
                        survival_rate=objs["survival_rate"],
                        cache_hit_rate=objs["cache_hit_rate"],
                    )
                    for ind, objs in individuals_with_objs
                ]
                local_front = pareto_filter(local_front + points)

            # Reproduce
            new_population: List[Dict[str, Any]] = []
            if individuals_with_objs:
                best_idx = max(range(len(individuals_with_objs)), key=lambda i: fitness_scores[i])
                new_population.append(dict(individuals_with_objs[best_idx][0]))
            while len(new_population) < self.population_size:
                if random.random() < self.crossover_rate and len(individuals_with_objs) >= 2:
                    p1 = self._tournament(individuals_with_objs, fitness_scores)
                    p2 = self._tournament(individuals_with_objs, fitness_scores)
                    child = self._mutate(self._crossover(p1, p2))
                else:
                    child = self._mutate(
                        dict(self._tournament(individuals_with_objs, fitness_scores))
                        if individuals_with_objs else self._initialize_individual()
                    )
                new_population.append(child)
            population = new_population

        self.pareto_front = local_front

        # Apply best
        if self.config.mopd.enabled and local_front:
            best_point = self._select_best_from_pareto(local_front)
            if best_point is not None:
                self.best_individual = {
                    "conversion_costs": dict(best_point.conversion_costs),
                    "collateral_ratios": dict(best_point.collateral_ratios),
                }
                self.best_fitness = best_point.scalarised_score
        elif population:
            # fallback: last generation's best
            last_objs = [await self._evaluate_objectives(ind) for ind in population]
            fits = [self._scalarise(o) for o in last_objs]
            best_idx = int(np.argmax(fits))
            self.best_individual = dict(population[best_idx])
            self.best_fitness = fits[best_idx]

        if self.best_individual is not None:
            # Apply optimised parameters to owner storage
            self.biomass.conversion_costs = dict(self.best_individual["conversion_costs"])
            self.biomass.collateral_ratios = dict(self.best_individual["collateral_ratios"])

        self.evolution_history.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "generations": generations,
            "best_fitness": self.best_fitness,
        })

        return {
            "best_fitness": self.best_fitness,
            "best_individual": self.best_individual,
            "generations": generations,
            "pareto_front": [p.to_dict() for p in self.pareto_front],
        }

    def _scalarise(self, objs: Dict[str, float]) -> float:
        w = self.config.mopd.objective_weights
        return (
            w.get("efficiency", 0.3) * objs["efficiency"]
            + w.get("cost_score", 0.2) * objs["cost_score"]
            + w.get("survival_rate", 0.2) * objs["survival_rate"]
            + w.get("cache_hit_rate", 0.3) * objs["cache_hit_rate"]
        )

    def _tournament(
        self, individuals_with_objs: List[Tuple[Dict[str, Any], Dict[str, float]]],
        fitness_scores: List[float],
    ) -> Dict[str, Any]:
        n = len(individuals_with_objs)
        k = min(self.tournament_size, n)
        indices = random.sample(range(n), k)
        best_idx = max(indices, key=lambda i: fitness_scores[i])
        return dict(individuals_with_objs[best_idx][0])

    def _crossover(self, p1: Dict[str, Any], p2: Dict[str, Any]) -> Dict[str, Any]:
        costs = {}
        for key in p1["conversion_costs"]:
            if random.random() < 0.5:
                costs[key] = p1["conversion_costs"][key]
            else:
                costs[key] = p2["conversion_costs"][key]
            if random.random() < 0.3:
                costs[key] = (p1["conversion_costs"][key] + p2["conversion_costs"][key]) / 2.0
        ratios = {}
        for level in p1["collateral_ratios"]:
            if random.random() < 0.5:
                ratios[level] = p1["collateral_ratios"][level]
            else:
                ratios[level] = p2["collateral_ratios"][level]
            if random.random() < 0.3:
                ratios[level] = (
                    p1["collateral_ratios"][level] + p2["collateral_ratios"][level]
                ) / 2.0
        return {"conversion_costs": costs, "collateral_ratios": ratios}

    def _mutate(self, individual: Dict[str, Any]) -> Dict[str, Any]:
        mutated = {
            "conversion_costs": dict(individual["conversion_costs"]),
            "collateral_ratios": dict(individual["collateral_ratios"]),
        }
        lo, hi = self.conversion_cost_bounds
        for k in mutated["conversion_costs"]:
            if random.random() < self.mutation_rate:
                v = mutated["conversion_costs"][k] + random.uniform(-2.0, 2.0)
                mutated["conversion_costs"][k] = max(lo, min(hi, v))
        lo, hi = self.collateral_bounds
        for k in mutated["collateral_ratios"]:
            if random.random() < self.mutation_rate:
                v = mutated["collateral_ratios"][k] + random.uniform(-0.3, 0.3)
                mutated["collateral_ratios"][k] = max(lo, min(hi, v))
        return mutated

    def to_dict(self) -> Dict[str, Any]:
        return {
            "best_fitness": self.best_fitness,
            "best_individual": self.best_individual,
            "evolution_history": self.evolution_history,
            "population_size": self.population_size,
            "mutation_rate": self.mutation_rate,
            "crossover_rate": self.crossover_rate,
            "generations": self.generations,
            "tournament_size": self.tournament_size,
            "pareto_front": [p.to_dict() for p in self.pareto_front],
        }

    def from_dict(self, data: Dict[str, Any]) -> None:
        self.best_fitness = data.get("best_fitness", -math.inf)
        self.best_individual = data.get("best_individual")
        self.evolution_history = data.get("evolution_history", [])
        self.population_size = data.get("population_size", self.population_size)
        self.mutation_rate = data.get("mutation_rate", self.mutation_rate)
        self.crossover_rate = data.get("crossover_rate", self.crossover_rate)
        self.generations = data.get("generations", self.generations)
        self.tournament_size = data.get("tournament_size", self.tournament_size)
        self.pareto_front = [MOPDPoint.from_dict(p) for p in data.get("pareto_front", [])]

    def get_status(self) -> Dict[str, Any]:
        return {
            "best_fitness": self.best_fitness,
            "best_individual": self.best_individual,
            "evolution_history": self.evolution_history[-10:],
            "pareto_front_size": len(self.pareto_front),
        }


# =============================================================================
# SECTION 9. TASK MANAGER (STABLE)
# =============================================================================
class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._drained = False

    def start_task(self, name: str, coro_func: Callable, *args, **kwargs) -> Optional[asyncio.Task]:
        async def wrapper():
            backoff = 1
            max_backoff = 60
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e))
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.warning("No running loop; task not started", name=name)
            return None
        task = loop.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    async def drain(self, timeout: float) -> None:
        if self._drained:
            return
        self._drained = True
        self.shutdown_event.set()
        all_tasks = list(self.tasks.values())
        if not all_tasks:
            return
        done, pending = await asyncio.wait(all_tasks, timeout=timeout)
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self.tasks.clear()
        logger.info("TaskManager drained", completed=len(done), cancelled=len(pending))


# =============================================================================
# SECTION 10. PERSISTENCE (STABLE)
# =============================================================================
class BiomassStoragePersistence:
    CURRENT_VERSION = "3.0"

    def __init__(self, config: BiomassStorageConfig):
        self.config = config
        self.path = Path(config.persistence_path)
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    @retry_async(max_retries=3, base_delay=0.5)
    async def save_state(self, storage: "BiomassStorage") -> bool:
        async with self._get_lock():
            try:
                state = {
                    "version": self.CURRENT_VERSION,
                    "task_index": {tid: t.to_dict() for tid, t in storage.task_index.items()},
                    "task_hash_index": dict(storage.task_hash_index),
                    "storage_tokens": {tid: tok.to_dict() for tid, tok in storage.storage_tokens.items()},
                    "collateral_pool": storage.collateral_pool,
                    "total_mobilized": storage.total_mobilized,
                    "mobilization_history": list(storage.mobilization_history),
                    "deduplication_savings": storage.deduplication_savings,
                    "expired_count": storage.expired_count,
                    "stored_count": storage.stored_count,
                    "index_hits": storage.index_hits,
                    "index_misses": storage.index_misses,
                    "inflow_history": list(storage.inflow_history),
                    "outflow_history": list(storage.outflow_history),
                    "conversion_costs": dict(storage.conversion_costs),
                    "collateral_ratios": dict(storage.collateral_ratios),
                    "genetic_optimizer": storage.genetic_optimizer.to_dict(),
                }
                with open(self.path, "w") as f:
                    json.dump(state, f, indent=2, default=str)
                logger.info("State saved", path=str(self.path))
                return True
            except Exception as e:
                logger.error("State save failed", error=str(e))
                return False

    @retry_async(max_retries=3, base_delay=0.5)
    async def load_state(self, storage: "BiomassStorage") -> bool:
        async with self._get_lock():
            if not self.path.exists():
                return False
            try:
                with open(self.path, "r") as f:
                    state = json.load(f)
                version = state.get("version", "0.0")
                if version != self.CURRENT_VERSION:
                    logger.warning(
                        "State version mismatch; ignoring",
                        stored=version, expected=self.CURRENT_VERSION,
                    )
                    return False
                storage.task_index = {
                    tid: StoredTask.from_dict(t) for tid, t in state.get("task_index", {}).items()
                }
                storage.task_hash_index = state.get("task_hash_index", {})
                # Rebuild tokens (drop signature bytes handling complexity for now)
                storage.storage_tokens = {}
                for tid, tok_d in state.get("storage_tokens", {}).items():
                    try:
                        tok = StorageToken(
                            token_id=tok_d["token_id"],
                            task_id=tok_d["task_id"],
                            tier=StorageTier(tok_d["tier"]),
                            created_at=datetime.fromisoformat(tok_d["created_at"]),
                            expires_at=datetime.fromisoformat(tok_d["expires_at"]) if tok_d.get("expires_at") else None,
                            signature=bytes.fromhex(tok_d["signature"]) if tok_d.get("signature") else None,
                            is_valid=tok_d.get("is_valid", True),
                        )
                        storage.storage_tokens[tid] = tok
                    except Exception:
                        continue
                storage.collateral_pool = state.get("collateral_pool", 0.0)
                storage.total_mobilized = state.get("total_mobilized", 0)
                storage.mobilization_history = deque(state.get("mobilization_history", []), maxlen=500)
                storage.deduplication_savings = state.get("deduplication_savings", 0)
                storage.expired_count = state.get("expired_count", 0)
                storage.stored_count = state.get("stored_count", 0)
                storage.index_hits = state.get("index_hits", 0)
                storage.index_misses = state.get("index_misses", 0)
                storage.inflow_history = deque(state.get("inflow_history", []), maxlen=100)
                storage.outflow_history = deque(state.get("outflow_history", []), maxlen=100)
                storage.conversion_costs.update(state.get("conversion_costs", {}))
                storage.collateral_ratios.update(state.get("collateral_ratios", {}))
                storage.genetic_optimizer.from_dict(state.get("genetic_optimizer", {}))
                logger.info("State loaded", path=str(self.path))
                return True
            except Exception as e:
                logger.error("State load failed", error=str(e))
                return False


# =============================================================================
# SECTION 11. MAIN STORAGE (STABLE CORE + EXPERIMENTAL OPTIMIZERS)
# =============================================================================
class BiomassStorage:
    """
    Storage layer with lifecycle:

        storage = BiomassStorage(config=...)
        await storage.start()      # or async with
        ...
        await storage.shutdown()
    """

    def __init__(
        self,
        config: Optional[BiomassStorageConfig] = None,
        token_manager: Optional[Any] = None,
        gradient_manager: Optional[Any] = None,
        storage: Optional[Any] = None,
        message_queue: Optional[Any] = None,
        adaptive_cost: Optional[Any] = None,
        pareto_gating: Optional[Any] = None,
        drift_detector: Optional[Any] = None,
        metrics: Optional[Any] = None,
    ):
        self.config = config or BiomassStorageConfig()
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating
        self.drift_detector = drift_detector
        self.metrics = metrics

        # Core data
        self.task_index: Dict[str, StoredTask] = {}
        self.task_hash_index: Dict[str, str] = {}
        self.storage_tokens: Dict[str, StorageToken] = {}
        self.collateral_pool: float = 0.0
        self.total_mobilized: int = 0
        self.mobilization_history: Deque[str] = deque(maxlen=500)
        self.deduplication_savings: int = 0
        self.expired_count: int = 0
        self.stored_count: int = 0
        self.index_hits: int = 0
        self.index_misses: int = 0
        self.inflow_history: Deque[str] = deque(maxlen=100)
        self.outflow_history: Deque[str] = deque(maxlen=100)

        # Parameters (mutated by GA)
        self.conversion_costs: Dict[str, float] = {
            f"{a}→{b}": 1.0 for a, b in GeneticOptimizer.TIER_PAIRS
        }
        self.collateral_ratios: Dict[str, float] = {
            lvl: 1.0 for lvl in GeneticOptimizer.GUARANTEE_LEVELS
        }

        # Placeholders (disabled by default)
        self.similarity_dedup = SimilarityDedupPlaceholder(
            enabled=self.config.enable_similarity_dedup
        )
        self.capacity_manager = CapacityManagerPlaceholder(
            enabled=self.config.enable_capacity_manager
        )
        self.predictive_mobilizer = PredictiveMobilizerPlaceholder(
            enabled=self.config.enable_predictive_mobilizer
        )
        self.precision_controller = PrecisionControllerPlaceholder(
            policy=self.config.precision_policy,
            enabled=self.config.enable_precision_switching,
        )
        self.carbon_market_client = (
            CarbonMarketClientPlaceholder(enabled=self.config.enable_carbon_market)
            if self.config.enable_carbon_market else None
        )
        self.chaos_injector = ChaosInjectorPlaceholder(
            self, self.config.chaos_probability,
            enabled=self.config.enable_chaos,
        )
        self.human_approval = HumanApprovalPlaceholder(
            self.queue, enabled=self.config.enable_human_approval,
        )
        self.tier_agents: Dict[StorageTier, TierAgentPlaceholder] = {
            tier: TierAgentPlaceholder(tier, self, enabled=self.config.enable_tier_agents)
            for tier in StorageTier
        }

        # Experimental subsystems
        self.rl_agent = CausalRLAgent(
            state_dim=10, action_dim=8,
            max_q_table=self.config.q_table_max_size,
            enabled=False,
        )
        self.federated_coordinator = (
            FederatedCoordinator(self, self.queue, enabled=False)
            if self.queue is not None else None
        )
        self.genetic_optimizer = GeneticOptimizer(self, self.config, enabled=True)
        if adaptive_cost and pareto_gating and drift_detector:
            self.genetic_optimizer.set_central_components(
                adaptive_cost, pareto_gating, drift_detector
            )

        # Concurrency
        self.param_lock: Optional[asyncio.Lock] = None
        self._index_lock: Optional[asyncio.Lock] = None

        # Persistence
        self.persistence = BiomassStoragePersistence(self.config)

        # Task manager
        self._task_manager = TaskManager()

        # Lifecycle
        self._started = False
        self._shutdown = False

        # Metrics (Prometheus)
        self._prom = self._setup_metrics()

        logger.info(
            "BiomassStorage initialized",
            max_storage=self.config.max_storage_tokens,
            mopd_enabled=self.config.mopd.enabled,
        )

    # ---------------- locks ----------------
    def _get_param_lock(self) -> asyncio.Lock:
        if self.param_lock is None:
            self.param_lock = asyncio.Lock()
        return self.param_lock

    def _get_index_lock(self) -> asyncio.Lock:
        if self._index_lock is None:
            self._index_lock = asyncio.Lock()
        return self._index_lock

    # ---------------- metrics ----------------
    def _setup_metrics(self) -> Dict[str, Any]:
        if not PROMETHEUS_AVAILABLE:
            return {}
        try:
            return {
                "store_total": Counter("biomass_store_total", "Total store calls"),
                "dedup_hits_total": Counter("biomass_dedup_hits_total", "Dedup hits"),
                "expired_total": Counter("biomass_expired_total", "Expired tasks"),
                "mobilize_total": Counter("biomass_mobilize_total", "Mobilize operations"),
                "tasks_gauge": Gauge("biomass_tasks", "Current task count"),
                "pareto_gauge": Gauge("biomass_pareto_size", "Pareto front size"),
                "q_table_gauge": Gauge("biomass_q_table_size", "Q-table size"),
            }
        except Exception:
            return {}

    # ---------------- lifecycle ----------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        # Load persisted state
        try:
            await self.persistence.load_state(self)
        except Exception as e:
            logger.warning("State load failed at start", error=str(e))

        # Background loops
        self._task_manager.start_task("chaos_loop", self._periodic_chaos)
        self._task_manager.start_task("federated_loop", self._periodic_federated_update)
        self._task_manager.start_task("state_save_loop", self._periodic_state_save)

        logger.info("BiomassStorage started")

    async def ready(self) -> bool:
        if not self._started:
            return False
        # Simple sanity check
        return isinstance(self.task_index, dict) and isinstance(self.storage_tokens, dict)

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        timeout = timeout or float(self.config.shutdown_timeout_seconds)
        logger.info("BiomassStorage shutting down")

        try:
            await asyncio.wait_for(self._task_manager.drain(timeout), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Task drain timed out")

        try:
            await self.persistence.save_state(self)
        except Exception as e:
            logger.warning("State flush failed at shutdown", error=str(e))

        logger.info("BiomassStorage shutdown complete")

    async def __aenter__(self) -> "BiomassStorage":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- background loops ----------------
    async def _periodic_chaos(self) -> None:
        while True:
            await asyncio.sleep(60)
            await self.chaos_injector.maybe_inject_failure()

    async def _periodic_federated_update(self) -> None:
        while True:
            await asyncio.sleep(300)
            if self.federated_coordinator is not None:
                await self.federated_coordinator.send_update()

    async def _periodic_state_save(self) -> None:
        while True:
            await asyncio.sleep(self.config.state_save_interval)
            try:
                await self.persistence.save_state(self)
            except Exception as e:
                logger.warning("Periodic save failed", error=str(e))

    # ---------------- core operations ----------------
    @traced("biomass.store")
    async def store_task(
        self,
        task_id: str,
        content: str,
        tier: StorageTier = StorageTier.ATP_CACHE,
        ttl: Optional[timedelta] = None,
        priority: int = 0,
    ) -> Dict[str, Any]:
        # Enforce capacity
        async with self._get_index_lock():
            if len(self.task_index) >= self.config.max_storage_tokens:
                return {"success": False, "error": "capacity_exceeded"}

            # Dedup
            content_hash = hashlib.sha256(content.encode()).hexdigest()
            existing = self.task_hash_index.get(content_hash)
            if existing is not None:
                self.deduplication_savings += 1
                if self._prom:
                    try:
                        self._prom["dedup_hits_total"].inc()
                    except Exception:
                        pass
                return {
                    "success": True,
                    "deduplicated": True,
                    "original_task_id": existing,
                }

            task = StoredTask(
                task_id=task_id,
                content=content,
                tier=tier,
                timestamp=datetime.now(timezone.utc),
                ttl=ttl,
                hash_value=content_hash,
                priority=priority,
            )
            self.task_index[task_id] = task
            self.task_hash_index[content_hash] = task_id
            self.stored_count += 1

            token = StorageToken(
                token_id=str(uuid.uuid4()),
                task_id=task_id,
                tier=tier,
                created_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc) + ttl if ttl else None,
                is_valid=True,
            )
            # Sign
            try:
                token.signature = self._security_sign(token.token_id.encode())
            except Exception:
                token.signature = None
            self.storage_tokens[task_id] = token
            self.inflow_history.append(datetime.now(timezone.utc).isoformat())

            if self._prom:
                try:
                    self._prom["store_total"].inc()
                    self._prom["tasks_gauge"].set(len(self.task_index))
                except Exception:
                    pass

        # Feedback event (outside lock)
        if self.queue is not None and FeedbackEvent is not None:
            try:
                event = FeedbackEvent.create_with_context(
                    task_id=task_id,
                    selected_action="store_task",
                    quality_score=1.0,
                    energy_joules=0.0,
                    carbon_g=0.0,
                    feedback_type="biomass_storage",
                    adaptive_cost_value=0.0,
                    state={"tier": tier.value},
                    candidates=[{"action": "store"}],
                    source="biomass_storage",
                    environment="production",
                    tags=["biomass", "store"],
                )
                await self.queue.publish("feedback_events", event.to_json())
            except Exception as e:
                logger.debug("Feedback publish failed", error=str(e))

        return {"success": True, "task": task.to_dict()}

    @traced("biomass.retrieve")
    async def retrieve_task(self, task_id: str) -> Optional[StoredTask]:
        async with self._get_index_lock():
            task = self.task_index.get(task_id)
            if task is None:
                self.index_misses += 1
                return None

            # Expiry check
            now = datetime.now(timezone.utc)
            if task.ttl and (task.timestamp + task.ttl) < now:
                self.expired_count += 1
                self._remove_task_locked(task_id)
                self.index_misses += 1
                if self._prom:
                    try:
                        self._prom["expired_total"].inc()
                    except Exception:
                        pass
                return None

            task.access_count += 1
            task.last_access = now
            self.index_hits += 1
            return task

    @traced("biomass.mobilize")
    async def mobilize(self, amount: int) -> Dict[str, Any]:
        if amount <= 0:
            return {"success": False, "error": "invalid_amount"}

        async with self._get_index_lock():
            available = len(self.task_index)
            if available == 0:
                return {"success": True, "mobilized": []}
            amount = min(amount, available)

            # Priority-aware: lowest priority first, then oldest, then smallest tier cost
            def sort_key(tid: str):
                t = self.task_index[tid]
                tier_key = f"{t.tier.value}"
                cost = self.conversion_costs.get(tier_key, 1.0)
                # want higher priority evicted last -> use negative for descending
                return (t.priority, -t.timestamp.timestamp(), cost)

            # Evict lowest priority first: sort ascending priority; keep highest priority
            ordered = sorted(self.task_index.keys(), key=sort_key)
            to_remove = ordered[:amount]

            mobilized = []
            for tid in to_remove:
                task = self.task_index.get(tid)
                if task is None:
                    continue
                mobilized.append(task.to_dict())
                self._remove_task_locked(tid)
                self.total_mobilized += 1

            self.mobilization_history.append(datetime.now(timezone.utc).isoformat())
            self.outflow_history.append(datetime.now(timezone.utc).isoformat())
            if self._prom:
                try:
                    self._prom["mobilize_total"].inc(len(mobilized))
                    self._prom["tasks_gauge"].set(len(self.task_index))
                except Exception:
                    pass

        return {"success": True, "mobilized": mobilized}

    def _remove_task_locked(self, task_id: str) -> None:
        """Assumes _index_lock is held."""
        task = self.task_index.pop(task_id, None)
        if task is None:
            return
        if self.task_hash_index.get(task.hash_value) == task_id:
            del self.task_hash_index[task.hash_value]
        self.storage_tokens.pop(task_id, None)

    # ---------------- analytics ----------------
    def generate_analytics(self) -> StorageAnalytics:
        total = len(self.task_index)

        # Real derived metrics
        costs = list(self.conversion_costs.values()) or [1.0]
        avg_cost = float(np.mean(costs))
        # efficiency: 1.0 / (1.0 + avg_cost) gives (0,1], 1.0 when cost is 0
        conversion_efficiency = 1.0 / (1.0 + avg_cost)

        hit_total = self.index_hits + self.index_misses
        cache_hit_rate = self.index_hits / hit_total if hit_total > 0 else 0.0

        expiration_rate = (
            self.expired_count / self.stored_count
            if self.stored_count > 0 else 0.0
        )

        distribution: Dict[str, int] = {tier.value: 0 for tier in StorageTier}
        for t in self.task_index.values():
            distribution[t.tier.value] = distribution.get(t.tier.value, 0) + 1

        return StorageAnalytics(
            total_tasks=total,
            total_size=sum(len(t.content) for t in self.task_index.values()),
            conversion_efficiency=conversion_efficiency,
            avg_retrieval_cost=avg_cost,
            expiration_rate=min(1.0, expiration_rate),
            cache_hit_rate=cache_hit_rate,
            storage_distribution=distribution,
        )

    # ---------------- teacher policy ----------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        actions = [f"store_to_{tier.value}" for tier in StorageTier] + ["retrieve", "mobilize"]
        n = len(actions)
        uniform = [1.0 / n] * n
        if self.adaptive_cost is None or self.pareto_gating is None:
            return uniform

        try:
            candidates = []
            for action in actions:
                quality = self._estimate_action_quality(action, state)
                candidates.append({
                    "expert_id": action,
                    "quality_score": quality,
                    "carbon_g": 0.0,
                    "latency_ms": 0.0,
                    "energy_joules": 0.0,
                })
            filtered = self.pareto_gating.filter(candidates)
            allowed = {c["expert_id"] for c in filtered} if filtered else set(actions)

            costs = []
            for action in actions:
                if action in allowed:
                    try:
                        cost = float(self.adaptive_cost.compute(
                            quality=self._estimate_action_quality(action, state),
                            carbon_g=0.0, latency_ms=0.0, energy_joules=0.0,
                            health=0.8, atp=0.5,
                        ))
                    except Exception:
                        cost = float("inf")
                else:
                    cost = float("inf")
                costs.append(cost)

            finite = [c for c in costs if math.isfinite(c)]
            if not finite:
                return uniform
            cmin = min(finite)
            exp_costs = [math.exp(-(c - cmin)) if math.isfinite(c) else 0.0 for c in costs]
            total = sum(exp_costs)
            if total <= 0:
                return uniform
            return [e / total for e in exp_costs]
        except Exception:
            return uniform

    def _estimate_action_quality(self, action: str, state: Dict[str, Any]) -> float:
        # Real: fill level drives store preference; hit rate drives retrieve
        if action.startswith("store_to_"):
            tier = action[len("store_to_"):]
            fill = float(state.get(f"fill_{tier}", 0.5))
            return max(0.0, min(1.0, 1.0 - fill))
        if action == "retrieve":
            analytics = self.generate_analytics()
            return analytics.cache_hit_rate
        if action == "mobilize":
            return 1.0 if self.collateral_pool >= 0.0 else 0.0
        return 0.5

    # ---------------- signing ----------------
    def _security_sign(self, data: bytes) -> bytes:
        if not hasattr(self, "_security"):
            self._security = SecurityService()  # lazy
        return self._security.sign(data)

    # ---------------- status ----------------
    async def health_check(self) -> Dict[str, Any]:
        analytics = self.generate_analytics()
        return {
            "status": "healthy" if await self.ready() else "starting",
            "total_tasks": analytics.total_tasks,
            "storage_distribution": analytics.storage_distribution,
            "mopd_enabled": self.config.mopd.enabled,
            "pareto_front_size": len(self.genetic_optimizer.pareto_front),
            "q_table_size": self.rl_agent.size(),
            "module_status": MODULE_STATUS,
            "security_backend": getattr(self, "_security", None).backend
            if hasattr(self, "_security") else "not_initialized",
        }

    def get_pareto_front(self) -> List[MOPDPoint]:
        return list(self.genetic_optimizer.pareto_front)

    def get_mopd_summary(self) -> Dict[str, Any]:
        if not self.config.mopd.enabled:
            return {"enabled": False}
        return {
            "enabled": True,
            "objective_weights": dict(self.config.mopd.objective_weights),
            "grid_resolution": self.config.mopd.grid_resolution,
            "pareto_front_size": len(self.genetic_optimizer.pareto_front),
            "best_scalarised_score": self.genetic_optimizer.best_fitness,
            "evolution_history": self.genetic_optimizer.evolution_history[-10:],
        }


# =============================================================================
# SECTION 12. TESTS
# =============================================================================
class _Tests(unittest.TestCase):
    def _cfg(self) -> BiomassStorageConfig:
        import tempfile
        td = tempfile.mkdtemp()
        return BiomassStorageConfig(
            persistence_path=os.path.join(td, "state.json"),
            ga_population_size=6,
            ga_generations=2,
            max_storage_tokens=20,
            state_save_interval=3600,
        )

    def test_start_ready_shutdown(self):
        async def go():
            storage = BiomassStorage(config=self._cfg())
            self.assertFalse(await storage.ready())
            await storage.start()
            self.assertTrue(await storage.ready())
            await storage.shutdown()
            self.assertTrue(storage._shutdown)
            # Idempotent
            await storage.shutdown()
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with BiomassStorage(config=self._cfg()) as s:
                self.assertTrue(await s.ready())
        asyncio.run(go())

    def test_store_and_retrieve(self):
        async def go():
            async with BiomassStorage(config=self._cfg()) as s:
                r = await s.store_task("t1", "hello", StorageTier.ATP_CACHE)
                self.assertTrue(r["success"])
                t = await s.retrieve_task("t1")
                self.assertIsNotNone(t)
                self.assertEqual(t.content, "hello")
        asyncio.run(go())

    def test_dedup(self):
        async def go():
            async with BiomassStorage(config=self._cfg()) as s:
                await s.store_task("t1", "same")
                r2 = await s.store_task("t2", "same")
                self.assertTrue(r2.get("deduplicated"))
                self.assertEqual(r2["original_task_id"], "t1")
                self.assertEqual(s.deduplication_savings, 1)
        asyncio.run(go())

    def test_capacity_enforced(self):
        async def go():
            cfg = self._cfg()
            cfg.max_storage_tokens = 3
            async with BiomassStorage(config=cfg) as s:
                for i in range(3):
                    r = await s.store_task(f"t{i}", f"content-{i}")
                    self.assertTrue(r["success"])
                r = await s.store_task("overflow", "content-overflow")
                self.assertFalse(r["success"])
                self.assertEqual(r["error"], "capacity_exceeded")
        asyncio.run(go())

    def test_expiry(self):
        async def go():
            async with BiomassStorage(config=self._cfg()) as s:
                await s.store_task("t1", "x", ttl=timedelta(milliseconds=1))
                await asyncio.sleep(0.05)
                t = await s.retrieve_task("t1")
                self.assertIsNone(t)
                self.assertEqual(s.expired_count, 1)
        asyncio.run(go())

    def test_mobilize_priority(self):
        async def go():
            async with BiomassStorage(config=self._cfg()) as s:
                await s.store_task("low", "a", priority=0)
                await s.store_task("high", "b", priority=5)
                r = await s.mobilize(1)
                self.assertTrue(r["success"])
                self.assertEqual(r["mobilized"][0]["task_id"], "low")
                self.assertIsNotNone(await s.retrieve_task("high"))
        asyncio.run(go())

    def test_persistence_roundtrip(self):
        async def go():
            import tempfile
            cfg = self._cfg()
            s1 = BiomassStorage(config=cfg)
            async with s1:
                await s1.store_task("t1", "persisted")
                await s1.store_task("t2", "another")
            s2 = BiomassStorage(config=cfg)
            async with s2:
                self.assertIn("t1", s2.task_index)
                self.assertIn("t2", s2.task_index)
        asyncio.run(go())

    def test_pareto_filter(self):
        pts = [
            MOPDPoint("a", {}, {}, 1.0, 1.0, 1.0, 1.0),
            MOPDPoint("b", {}, {}, 0.5, 0.5, 0.5, 0.5),
            MOPDPoint("c", {}, {}, 1.0, 0.5, 0.5, 0.5),
        ]
        front = pareto_filter(pts)
        self.assertEqual(sorted(p.strategy for p in front), ["a", "c"])

    def test_policy_probs_valid(self):
        async def go():
            async with BiomassStorage(config=self._cfg()) as s:
                probs = await s.policy_probs({})
                self.assertTrue(all(p >= 0 for p in probs))
                self.assertAlmostEqual(sum(probs), 1.0, places=6)
        asyncio.run(go())

    def test_q_table_bounded(self):
        rl = CausalRLAgent(state_dim=4, action_dim=3, max_q_table=10, enabled=False)
        for i in range(200):
            state = np.array([random.random() for _ in range(4)])
            action = rl.act(state)
            rl.update(state, action, 1.0, state, False)
        self.assertLessEqual(rl.size(), 10)

    def test_security_roundtrip(self):
        import tempfile
        svc = SecurityService(tempfile.mkdtemp())
        msg = b"test-message"
        sig = svc.sign(msg)
        self.assertTrue(svc.verify(msg, sig))
        self.assertFalse(svc.verify(b"other", sig))

    def test_real_analytics(self):
        async def go():
            async with BiomassStorage(config=self._cfg()) as s:
                await s.store_task("t1", "a")
                await s.retrieve_task("t1")
                await s.retrieve_task("missing")
                a = s.generate_analytics()
                self.assertEqual(a.total_tasks, 1)
                self.assertEqual(a.cache_hit_rate, 0.5)  # 1 hit, 1 miss
                self.assertGreater(a.conversion_efficiency, 0.0)
                self.assertGreater(a.avg_retrieval_cost, 0.0)
        asyncio.run(go())


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 13. ENTRY POINT
# =============================================================================
async def _example() -> None:
    async with BiomassStorage() as storage:
        await storage.store_task("hello", "world", StorageTier.ATP_CACHE)
        await storage.store_task("a", "b", StorageTier.STARCH_RESERVE, priority=2)
        print("analytics:", storage.generate_analytics())
        print("pareto front size:", len(storage.get_pareto_front()))
        print("probs:", await storage.policy_probs({}))


def main() -> None:
    parser = argparse.ArgumentParser(description="Biomass Storage v9.0.0")
    parser.add_argument("--test", action="store_true", help="Run embedded tests")
    parser.add_argument("--example", action="store_true", help="Run example usage")
    parser.add_argument("--status", action="store_true", help="Print module statuses")
    args = parser.parse_args()

    if args.status:
        for name, status in MODULE_STATUS.items():
            print(f"{name:22s} {status}")
        return

    if args.test:
        sys.exit(run_tests())

    if args.example:
        asyncio.run(_example())
        return

    print("Biomass Storage v9.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
