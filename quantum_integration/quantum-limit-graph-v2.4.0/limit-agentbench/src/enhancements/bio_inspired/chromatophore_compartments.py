#!/usr/bin/env python3
# =============================================================================
# Chromatophore Compartments v8.0.0 — Patched Single-File Edition
# =============================================================================
"""
Chromatophore Compartments v8.0.0
=================================
Patched single-file version of the hierarchical compartment manager.

P0 fixes
--------
- CompartmentConfig fully defined with all referenced fields.
- EventBus implemented (in-process pub/sub).
- RegionAggregator.knowledge_transfer is a real class with add_knowledge.
- Lazy asyncio.Locks (no cross-loop binding).
- No tasks started in __init__; use `await mgr.start()`.
- create_compartment region mapping is correct on failure.
- to_dict / from_dict for CompartmentResource, ChromatophoreCompartment,
  InterCompartmentMarket, KnowledgeTransfer, RegionAggregator.
- Persistence writes real serializable state and can load it back.
- Structure and persistence guarded by dedicated asyncio locks.

P1 — real behavior
------------------
- GA fitness depends on the genome (recomputes per-compartment health).
- policy_probs returns a fixed-length vector over {create, cull, balance}.
- _state_to_features reads real state.
- find_best_compartment uses AdaptiveCostFunction + ParetoGating when available.
- health_check_all skips empty regions.
- ChaosInjector / HumanApproval are honest.
- CausalRLAgent has a bounded, discretized Q-table.
- Shutdown drains tasks, flushes persistence, and is idempotent.

P2 — honesty
------------
- MODULE_STATUS documents each module.
- Placeholders: CausalRLAgent, FederatedCoordinator, PrecisionController,
  CarbonMarketClient, ChaosInjector, HumanApprovalHandler, HealthModel,
  KnowledgeBank. Disabled by default; safe no-ops when enabled.
- Experimental: GeneticOptimizer, MOPD, Homeostatic controller, XAI, Safety
  monitor, Persistence. Warn when enabled.

P3 — production readiness
-------------------------
- Lifecycle: `await start()`, `await shutdown()`, `await ready()`, context
  manager (`async with`).
- Graceful shutdown with task drain and persistence flush.
- Logical sections in a single file.
- Embedded test suite: `python3 compartment_manager.py --test`.
- Prometheus metrics + OpenTelemetry spans (both optional).
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
from collections import OrderedDict, defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("chromatophore_compartments")
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
    "compartment_core":     "stable",
    "circuit_breaker":      "stable",
    "task_manager":         "stable",
    "structure_lock":       "stable",
    "persistence":          "experimental",
    "genetic_optimizer":    "experimental",
    "mopd":                 "experimental",
    "homeostatic":          "experimental",
    "xai":                  "experimental",
    "safety_monitor":       "experimental",
    "causal_rl":            "placeholder",
    "federated":            "placeholder",
    "precision":            "placeholder",
    "carbon_market":        "placeholder",
    "chaos":                "placeholder",
    "human_approval":       "placeholder",
    "health_model":         "placeholder",
    "knowledge_bank":       "placeholder",
}


def _warn_module(name: str) -> None:
    status = MODULE_STATUS.get(name, "unknown")
    if status == "stable":
        return
    if status == "placeholder":
        logger.warning("Module is a placeholder; enabling it has no effect", module=name)
    elif status == "experimental":
        logger.warning("Module is experimental; validate before production use", module=name)


# =============================================================================
# SECTION 2. RETRY + TRACING HELPERS
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
            if last_exc is not None:
                raise last_exc
            return None
        return wrapper
    return decorator


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
# SECTION 3. ENUMS
# =============================================================================
class CompartmentState(Enum):
    GENESIS = "genesis"
    MATURING = "maturing"
    ACTIVE = "active"
    STRESSED = "stressed"
    SENESCENT = "senescent"
    APOPTOTIC = "apoptotic"
    DECOMMISSIONED = "decommissioned"


class MembranePermeability(Enum):
    IMPERMEABLE = "impermeable"
    RESTRICTIVE = "restrictive"
    SELECTIVE = "selective"
    PERMEABLE = "permeable"
    QUANTUM_ENCRYPTED = "quantum_encrypted"


# =============================================================================
# SECTION 4. CONFIGURATION
# =============================================================================
@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "health": 0.3,
        "efficiency": 0.3,
        "token_balance": 0.2,
        "resource_utilization": 0.2,
    })
    grid_resolution: int = 5


@dataclass
class CompartmentConfig:
    # Structure
    max_regions: int = 10
    compartments_per_region: int = 50
    # Persistence
    enable_persistence: bool = True
    persistence_path: str = "compartment_state.json"
    max_retries: int = 3
    retry_base_delay_ms: float = 500.0
    retry_max_delay_ms: float = 5000.0
    # GA / MOPD
    enable_genetic_optimizer: bool = True
    ga_population_size: int = 20
    ga_mutation_rate: float = 0.1
    ga_crossover_rate: float = 0.7
    ga_generations: int = 5
    ga_tournament_size: int = 3
    ga_evolution_interval_hours: float = 6.0
    mopd: MOPDConfig = field(default_factory=MOPDConfig)
    # Homeostatic controller
    target_health: float = 0.7
    target_token_reserve: float = 1000.0
    kp: float = 0.5
    ki: float = 0.1
    kd: float = 0.05
    # Health model
    health_model_path: Optional[str] = None
    health_model_min_samples: int = 100
    health_model_training_interval_seconds: float = 3600.0
    # Circuit breaker
    enable_circuit_breaker: bool = True
    circuit_breaker_db_path: str = "compartment_cb.db"
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout_seconds: float = 60.0
    # Interval
    ecosystem_maintenance_interval_seconds: float = 30.0
    trading_maintenance_interval_seconds: float = 60.0
    # RL
    q_table_max_size: int = 5000
    rl_state_dim: int = 10
    rl_action_dim: int = 3
    # Shutdown
    shutdown_timeout_seconds: int = 15
    # Experimental / placeholder toggles — disabled by default
    enable_causal_rl: bool = False
    enable_federated_learning: bool = False
    enable_safety_monitor: bool = True       # experimental but harmless
    enable_xai: bool = True                  # experimental but harmless
    enable_precision_switching: bool = False
    enable_carbon_market: bool = False
    carbon_market_config: Optional[Dict[str, str]] = None
    enable_chaos: bool = False
    chaos_probability: float = 0.0
    enable_human_approval: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompartmentConfig":
        data = dict(data or {})
        mopd_data = data.pop("mopd", None)
        fields = cls.__dataclass_fields__
        cfg = cls(**{k: v for k, v in data.items() if k in fields})
        if isinstance(mopd_data, dict):
            cfg.mopd = MOPDConfig(**{k: v for k, v in mopd_data.items() if k in MOPDConfig.__dataclass_fields__})
        return cfg

    @classmethod
    def from_env_and_file(cls) -> "CompartmentConfig":
        return cls()


# =============================================================================
# SECTION 5. DATA CLASSES
# =============================================================================
@dataclass
class CompartmentResource:
    cpu_cores: float = 1.0
    memory_mb: float = 256.0
    storage_mb: float = 1024.0
    network_mbps: float = 100.0
    max_tokens: float = 1000.0
    min_cpu_cores: float = 0.5
    max_cpu_cores: float = 4.0
    min_memory_mb: float = 128.0
    max_memory_mb: float = 2048.0
    allocation_scaling: float = 1.0
    last_adjustment: Optional[datetime] = None

    @property
    def utilization(self) -> float:
        # Clamped to [0,1] so it can safely feed policy features
        raw = (self.cpu_cores / self.max_cpu_cores
               + self.memory_mb / self.max_memory_mb
               + self.storage_mb / 4096.0) / 3.0
        return max(0.0, min(1.0, raw))

    def scale_up(self, factor: float = 1.5) -> None:
        self.cpu_cores = min(self.max_cpu_cores, self.cpu_cores * factor)
        self.memory_mb = min(self.max_memory_mb, self.memory_mb * factor)
        self.allocation_scaling *= factor
        self.last_adjustment = datetime.now(timezone.utc)

    def scale_down(self, factor: float = 0.7) -> None:
        self.cpu_cores = max(self.min_cpu_cores, self.cpu_cores * factor)
        self.memory_mb = max(self.min_memory_mb, self.memory_mb * factor)
        self.allocation_scaling *= factor
        self.last_adjustment = datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["last_adjustment"] = self.last_adjustment.isoformat() if self.last_adjustment else None
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompartmentResource":
        data = dict(data or {})
        la = data.get("last_adjustment")
        if isinstance(la, str):
            data["last_adjustment"] = datetime.fromisoformat(la)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class MOPDPoint:
    individual: Dict[str, Any]
    health: float
    efficiency: float
    token_balance: float
    resource_utilization: float
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# =============================================================================
# SECTION 6. SECURITY (STABLE) — HMAC default
# =============================================================================
class SecurityService:
    def __init__(self, key_dir: str = "./compartment_keys"):
        self.key_dir = Path(key_dir)
        self.key_dir.mkdir(parents=True, exist_ok=True)
        self._secret = self._load_or_generate()

    def _load_or_generate(self) -> bytes:
        p = self.key_dir / "hmac_secret.bin"
        if p.exists():
            return p.read_bytes()
        s = os.urandom(32)
        p.write_bytes(s)
        try:
            os.chmod(p, 0o600)
        except OSError:
            pass
        return s

    def sign(self, data: bytes) -> bytes:
        return hmac.new(self._secret, data, hashlib.sha256).digest()

    def verify(self, data: bytes, signature: bytes) -> bool:
        return hmac.compare_digest(self.sign(data), signature)


# =============================================================================
# SECTION 7. EVENT BUS (STABLE)
# =============================================================================
class EventBus:
    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def subscribe(self, event_type: str, callback: Callable) -> None:
        async with self._get_lock():
            self._subscribers[event_type].append(callback)

    async def publish(self, event_type: str, payload: Dict[str, Any]) -> int:
        async with self._get_lock():
            callbacks = list(self._subscribers.get(event_type, []))
        for cb in callbacks:
            try:
                r = cb(event_type, payload)
                if asyncio.iscoroutine(r):
                    await r
            except Exception as e:
                logger.debug("EventBus callback failed", error=str(e))
        return len(callbacks)


# =============================================================================
# SECTION 8. PLACEHOLDERS (safe no-ops)
# =============================================================================
class CausalRLAgentPlaceholder:
    """STATUS: placeholder. Bounded, discretized, uniform policy by default."""
    STATUS = "placeholder"

    def __init__(self, state_dim: int, action_dim: int, max_q_table: int = 5000,
                 enabled: bool = False):
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

    def act(self, state: np.ndarray, explore: bool = True) -> int:
        if explore and random.random() < self.epsilon:
            return random.randrange(self.action_dim)
        key = self._discretize(state)
        return int(np.argmax(self._get_or_create(key)))

    def update(self, state, action, reward, next_state, done) -> None:
        key = self._discretize(state)
        next_key = self._discretize(next_state)
        current = self._get_or_create(key)
        next_q = self._get_or_create(next_key)
        best_next = 0.0 if done else float(np.max(next_q))
        td_target = reward + self.gamma * best_next
        current[action] += self.learning_rate * (td_target - current[action])

    def get_policy_probs(self, state: np.ndarray, temperature: float = 1.0) -> List[float]:
        return [1.0 / self.action_dim] * self.action_dim

    def size(self) -> int:
        return len(self.q_table)


class FederatedCoordinatorPlaceholder:
    STATUS = "placeholder"

    def __init__(self, manager: Any, queue: Optional[Any], enabled: bool = False):
        if enabled:
            _warn_module("federated")
        self.manager = manager
        self.queue = queue
        self.available = False

    async def send_update(self) -> bool:
        return False

    async def receive_global_model(self, model_json: str) -> bool:
        return False


class PrecisionControllerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, policy: str = "energy_aware", enabled: bool = False):
        if enabled:
            _warn_module("precision")
        self.policy = policy
        self.available = False

    def get_precision(self, load: float, energy_budget: float) -> str:
        return "float32"


class CarbonMarketClientPlaceholder:
    STATUS = "placeholder"

    def __init__(self, enabled: bool = False, **kwargs: Any):
        if enabled:
            _warn_module("carbon_market")
        self.available = False

    def buy_credits(self, amount: float) -> bool:
        return False

    def sell_credits(self, amount: float) -> bool:
        return False


class ChaosInjectorPlaceholder:
    STATUS = "placeholder"

    def __init__(self, manager: Any, chaos_probability: float = 0.0,
                 enabled: bool = False):
        if enabled:
            _warn_module("chaos")
        self.manager = manager
        self.chaos_probability = chaos_probability
        self.available = False

    async def maybe_inject_failure(self) -> None:
        return None


class HumanApprovalHandlerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, queue: Optional[Any] = None, enabled: bool = False,
                 auto_approve_dev: bool = False):
        if enabled:
            _warn_module("human_approval")
        self.queue = queue
        self.available = False
        self.auto_approve_dev = auto_approve_dev
        if auto_approve_dev:
            logger.warning("HumanApproval auto_approve_dev=True; do not use in production.")

    async def request_approval(self, decision: Dict[str, Any], timeout: float = 60.0) -> bool:
        if self.auto_approve_dev:
            return True
        return False


class HealthModelPlaceholder:
    """STATUS: placeholder. Predictions are neutral."""
    STATUS = "placeholder"

    def __init__(self, model_path: Optional[str] = None, enabled: bool = False):
        if enabled:
            _warn_module("health_model")
        self.model_path = model_path
        self.history: List[Any] = []
        self.is_trained = False
        self.predictions_cache: Dict[str, Any] = {}
        self.available = False

    async def train(self, force: bool = False) -> Dict[str, Any]:
        return {"status": "placeholder", "samples": len(self.history)}

    async def predict_health(self, compartment_id: str, features: Dict[str, Any]) -> Dict[str, Any]:
        return {"predicted_health": 0.5, "confidence": 0.0}


class KnowledgeBankPlaceholder:
    """STATUS: placeholder. Records are stored but never replayed."""
    STATUS = "placeholder"

    def __init__(self, enabled: bool = False):
        if enabled:
            _warn_module("knowledge_bank")
        self.records: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self.available = False

    async def store(self, knowledge: Dict[str, Any]) -> None:
        self.records.append(knowledge)

    async def replay_to_compartment(self, compartment: Any) -> None:
        return None


# =============================================================================
# SECTION 9. SAFETY MONITOR (EXPERIMENTAL)
# =============================================================================
class SafetyMonitor:
    STATUS = "experimental"

    def __init__(self, enabled: bool = True):
        if enabled:
            _warn_module("safety_monitor")
        self.invariants: List[Tuple[str, Callable[[Dict[str, Any]], bool], str]] = []
        self.available = True

    def add_invariant(self, name: str, fn: Callable[[Dict[str, Any]], bool], description: str) -> None:
        self.invariants.append((name, fn, description))

    def check(self, state: Dict[str, Any]) -> List[str]:
        return [
            f"{name}: {desc}"
            for name, fn, desc in self.invariants
            if not fn(state)
        ]


# =============================================================================
# SECTION 10. XAI (EXPERIMENTAL)
# =============================================================================
class XAIExplainer:
    STATUS = "experimental"

    def __init__(self, enabled: bool = True):
        if enabled:
            _warn_module("xai")
        self.available = True

    def explain_select(self, best: Any, candidates: List[Tuple[Any, float]]) -> str:
        top = candidates[:3]
        return (
            f"Selected {best.compartment_id} with highest score "
            f"{top[0][1]:.2f} over {len(candidates)} candidates."
        )

    def explain_decommission(self, comp: Any) -> str:
        return (
            f"Decommissioned {comp.compartment_id} "
            f"(health={comp.health_score:.2f}, viable={comp.is_viable})."
        )


# =============================================================================
# SECTION 11. COMPARTMENT, MARKET, REGION
# =============================================================================
class MembraneGate:
    def __init__(self) -> None:
        self.permeability = MembranePermeability.SELECTIVE
        self.encryption: Optional[Any] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"permeability": self.permeability.value}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MembraneGate":
        mg = cls()
        try:
            mg.permeability = MembranePermeability(data.get("permeability", "selective"))
        except ValueError:
            pass
        return mg


class ChromatophoreCompartment:
    def __init__(
        self,
        compartment_id: str,
        expert_type: str,
        expert_instance: Optional[Any] = None,
        resources: Optional[CompartmentResource] = None,
    ):
        self.compartment_id = compartment_id
        self.expert_type = expert_type
        self.expert_instance = expert_instance
        self.resources = resources or CompartmentResource()
        self.state = CompartmentState.GENESIS
        self.health_score = 0.8
        self.efficiency_score = 0.7
        self.token_balance = 100.0
        self.success_rate = 0.6
        self.trust_gradient = 0.5
        self.membrane_gate = MembraneGate()
        self.parent_id: Optional[str] = None
        self.is_viable = True
        self.glycogen_queue: List[Any] = []

    def spend_tokens(self, amount: float, reason: str) -> bool:
        if self.token_balance >= amount:
            self.token_balance -= amount
            return True
        return False

    def receive_tokens(self, amount: float, source: str) -> bool:
        self.token_balance += amount
        return True

    def _evaluate_lifecycle(self) -> None:
        if self.health_score < 0.2:
            self.state = CompartmentState.APOPTOTIC
            self.is_viable = False
        elif self.health_score < 0.5:
            self.state = CompartmentState.STRESSED
        else:
            self.state = CompartmentState.ACTIVE

    def prepare_apoptosis(self) -> Tuple[float, Dict[str, Any]]:
        knowledge = {
            "health_score": self.health_score,
            "efficiency_score": self.efficiency_score,
            "expert_type": self.expert_type,
            "token_balance": self.token_balance,
        }
        remaining = self.token_balance
        self.token_balance = 0.0
        return remaining, knowledge

    def to_dict(self) -> Dict[str, Any]:
        return {
            "compartment_id": self.compartment_id,
            "expert_type": self.expert_type,
            "resources": self.resources.to_dict(),
            "state": self.state.value,
            "health_score": self.health_score,
            "efficiency_score": self.efficiency_score,
            "token_balance": self.token_balance,
            "success_rate": self.success_rate,
            "trust_gradient": self.trust_gradient,
            "membrane_gate": self.membrane_gate.to_dict(),
            "parent_id": self.parent_id,
            "is_viable": self.is_viable,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ChromatophoreCompartment":
        data = dict(data or {})
        c = cls(
            compartment_id=data.get("compartment_id", str(uuid.uuid4())),
            expert_type=data.get("expert_type", "default"),
            resources=CompartmentResource.from_dict(data.get("resources", {})),
        )
        try:
            c.state = CompartmentState(data.get("state", "genesis"))
        except ValueError:
            c.state = CompartmentState.GENESIS
        c.health_score = float(data.get("health_score", 0.8))
        c.efficiency_score = float(data.get("efficiency_score", 0.7))
        c.token_balance = float(data.get("token_balance", 100.0))
        c.success_rate = float(data.get("success_rate", 0.6))
        c.trust_gradient = float(data.get("trust_gradient", 0.5))
        c.membrane_gate = MembraneGate.from_dict(data.get("membrane_gate", {}))
        c.parent_id = data.get("parent_id")
        c.is_viable = bool(data.get("is_viable", True))
        return c


class InterCompartmentMarket:
    def __init__(self) -> None:
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.trade_history: List[Dict[str, Any]] = []

    def add_order(self, seller_id: str, buyer_id: str, amount: float, price: float) -> str:
        order_id = f"order_{uuid.uuid4().hex[:8]}"
        self.orders[order_id] = {
            "seller": seller_id,
            "buyer": buyer_id,
            "amount": amount,
            "price": price,
            "status": "open",
        }
        return order_id

    def match_orders(self) -> List[Dict[str, Any]]:
        matches = []
        for order_id, order in self.orders.items():
            if order["status"] == "open":
                order["status"] = "matched"
                self.trade_history.append(dict(order))
                matches.append({
                    "seller": order["seller"],
                    "buyer": order["buyer"],
                    "amount": order["amount"],
                })
        return matches

    def to_dict(self) -> Dict[str, Any]:
        return {"orders": self.orders, "trade_history": self.trade_history}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "InterCompartmentMarket":
        m = cls()
        m.orders = dict((data or {}).get("orders", {}))
        m.trade_history = list((data or {}).get("trade_history", []))
        return m


class KnowledgeTransfer:
    """Simple cross-region knowledge store. Not a placeholder anymore."""
    def __init__(self) -> None:
        self._records: Deque[Dict[str, Any]] = deque(maxlen=500)

    def add_knowledge(self, region_id: str, knowledge: Dict[str, Any]) -> None:
        self._records.append({"region_id": region_id, "knowledge": dict(knowledge)})

    def latest(self, n: int = 10) -> List[Dict[str, Any]]:
        return list(self._records)[-n:]

    def to_dict(self) -> Dict[str, Any]:
        return {"records": list(self._records)}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KnowledgeTransfer":
        kt = cls()
        for r in (data or {}).get("records", []):
            kt._records.append(r)
        return kt


class RegionAggregator:
    def __init__(self, region_id: str, max_compartments: int = 50):
        self.region_id = region_id
        self.max_compartments = max_compartments
        self.compartments: Dict[str, ChromatophoreCompartment] = {}
        self.knowledge_transfer = KnowledgeTransfer()
        self.market = InterCompartmentMarket()

    def add_compartment(self, compartment: ChromatophoreCompartment) -> bool:
        if len(self.compartments) >= self.max_compartments:
            return False
        self.compartments[compartment.compartment_id] = compartment
        return True

    def remove_compartment(self, compartment_id: str) -> None:
        self.compartments.pop(compartment_id, None)

    def get_total_count(self) -> int:
        return len(self.compartments)

    def get_viable_count(self) -> int:
        return sum(1 for c in self.compartments.values() if c.is_viable)

    def health_check(self) -> Optional[float]:
        # Returns None for empty regions so callers can skip them.
        if not self.compartments:
            return None
        return float(np.mean([c.health_score for c in self.compartments.values()]))

    def balance_load_local(self) -> int:
        return 0

    def cull_unhealthy(self) -> List[str]:
        to_remove = [
            cid for cid, comp in self.compartments.items()
            if comp.health_score < 0.2 and not comp.is_viable
        ]
        for cid in to_remove:
            self.compartments.pop(cid, None)
        return to_remove

    def to_dict(self) -> Dict[str, Any]:
        return {
            "region_id": self.region_id,
            "max_compartments": self.max_compartments,
            "compartments": {cid: c.to_dict() for cid, c in self.compartments.items()},
            "knowledge_transfer": self.knowledge_transfer.to_dict(),
            "market": self.market.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RegionAggregator":
        data = dict(data or {})
        r = cls(
            region_id=data.get("region_id", "default"),
            max_compartments=int(data.get("max_compartments", 50)),
        )
        for cid, cdata in (data.get("compartments") or {}).items():
            r.compartments[cid] = ChromatophoreCompartment.from_dict(cdata)
        r.knowledge_transfer = KnowledgeTransfer.from_dict(data.get("knowledge_transfer", {}))
        r.market = InterCompartmentMarket.from_dict(data.get("market", {}))
        return r


# =============================================================================
# SECTION 12. HOMEOSTATIC CONTROLLER (EXPERIMENTAL)
# =============================================================================
class HomeostaticSetpointController:
    STATUS = "experimental"

    def __init__(self, config: CompartmentConfig, enabled: bool = True):
        if enabled:
            _warn_module("homeostatic")
        self.config = config
        self.integral_health = 0.0
        self.integral_token = 0.0
        self.prev_error_health = 0.0
        self.prev_error_token = 0.0

    def compute_adjustment(self, health: float, token_reserve: float) -> Dict[str, float]:
        e_h = self.config.target_health - health
        e_t = self.config.target_token_reserve - token_reserve

        p_h = self.config.kp * e_h
        i_h = self.config.ki * (self.integral_health + e_h)
        d_h = self.config.kd * (e_h - self.prev_error_health)
        self.integral_health += e_h
        self.prev_error_health = e_h

        p_t = self.config.kp * e_t
        i_t = self.config.ki * (self.integral_token + e_t)
        d_t = self.config.kd * (e_t - self.prev_error_token)
        self.integral_token += e_t
        self.prev_error_token = e_t

        spawn_mod = 1.0 + max(0.0, p_h + i_h + d_h) / 10.0
        cull_mod = 1.0 + max(0.0, -p_h - i_h - d_h) / 10.0
        scale_mod = 1.0 + max(0.0, p_t + i_t + d_t) / 1000.0
        return {
            "spawn_rate_modifier": max(0.5, min(1.5, spawn_mod)),
            "cull_aggressiveness_modifier": max(0.5, min(1.5, cull_mod)),
            "resource_scale_modifier": max(0.9, min(1.1, scale_mod)),
        }

    def to_dict(self) -> Dict[str, float]:
        return {
            "integral_health": self.integral_health,
            "integral_token": self.integral_token,
            "prev_error_health": self.prev_error_health,
            "prev_error_token": self.prev_error_token,
        }

    def from_dict(self, data: Dict[str, Any]) -> None:
        self.integral_health = float((data or {}).get("integral_health", 0.0))
        self.integral_token = float((data or {}).get("integral_token", 0.0))
        self.prev_error_health = float((data or {}).get("prev_error_health", 0.0))
        self.prev_error_token = float((data or {}).get("prev_error_token", 0.0))


# =============================================================================
# SECTION 13. GENETIC OPTIMIZER (EXPERIMENTAL) — fitness genome-dependent
# =============================================================================
class GeneticOptimizer:
    STATUS = "experimental"

    def __init__(self, manager: "HierarchicalCompartmentManager",
                 config: CompartmentConfig, enabled: bool = True):
        if enabled:
            _warn_module("genetic_optimizer")
        self.manager = manager
        self.config = config
        self.best_fitness = -math.inf
        self.best_individual: Optional[Dict[str, Any]] = None
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDPoint] = []
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _initialize_individual(self) -> Dict[str, Any]:
        return {
            "health_score_weights": {
                "success_rate": random.uniform(0.1, 0.7),
                "efficiency_score": random.uniform(0.1, 0.7),
                "trust_gradient": random.uniform(0.1, 0.7),
            }
        }

    def _initialize_population(self) -> List[Dict[str, Any]]:
        return [
            self._initialize_individual()
            for _ in range(self.config.ga_population_size)
        ]

    async def _evaluate_individual(self, individual: Dict[str, Any]) -> Dict[str, float]:
        # Genome-dependent: recompute health per compartment using weights
        weights = individual["health_score_weights"]
        healths = []
        efficiencies = []
        tokens = []
        utils = []
        for c in self.manager.compartments.values():
            h = (
                c.success_rate * weights.get("success_rate", 0.4)
                + c.efficiency_score * weights.get("efficiency_score", 0.3)
                + c.trust_gradient * weights.get("trust_gradient", 0.3)
            )
            healths.append(max(0.0, min(1.0, h)))
            efficiencies.append(c.efficiency_score)
            tokens.append(c.token_balance)
            utils.append(c.resources.utilization)

        if not healths:
            return {
                "health": 0.5, "efficiency": 0.5,
                "token_balance": 0.0, "resource_utilization": 0.0,
            }
        return {
            "health": float(np.mean(healths)),
            "efficiency": float(np.mean(efficiencies)),
            "token_balance": float(sum(tokens) / max(1000.0, 1.0)),
            "resource_utilization": float(np.mean(utils)),
        }

    def _scalarise(self, objs: Dict[str, float]) -> float:
        w = self.config.mopd.objective_weights
        return (
            w.get("health", 0.3) * objs["health"]
            + w.get("efficiency", 0.3) * objs["efficiency"]
            + w.get("token_balance", 0.2) * objs["token_balance"]
            + w.get("resource_utilization", 0.2) * objs["resource_utilization"]
        )

    def _filter_pareto(self, points: List[MOPDPoint]) -> List[MOPDPoint]:
        if not points:
            return []
        keys = ["health", "efficiency", "token_balance", "resource_utilization"]
        front: List[MOPDPoint] = []
        for i, p in enumerate(points):
            dominated = False
            for j, q in enumerate(points):
                if i == j:
                    continue
                if (
                    all(getattr(q, k) >= getattr(p, k) for k in keys)
                    and any(getattr(q, k) > getattr(p, k) for k in keys)
                ):
                    dominated = True
                    break
            if not dominated:
                front.append(p)
        return front

    async def evolve(self, generations: Optional[int] = None) -> Dict[str, Any]:
        generations = generations or self.config.ga_generations
        async with self._get_lock():
            population = self._initialize_population()
            local_front: List[MOPDPoint] = []

            for gen in range(generations):
                objs_list = []
                for ind in population:
                    objs = await self._evaluate_individual(ind)
                    objs_list.append(objs)

                fitness = [self._scalarise(o) for o in objs_list]
                if self.config.mopd.enabled:
                    points = [
                        MOPDPoint(
                            individual=dict(ind),
                            health=o["health"],
                            efficiency=o["efficiency"],
                            token_balance=o["token_balance"],
                            resource_utilization=o["resource_utilization"],
                        )
                        for ind, o in zip(population, objs_list)
                    ]
                    local_front = self._filter_pareto(local_front + points)

                # Elitism + reproduction
                new_pop: List[Dict[str, Any]] = []
                best_idx = int(np.argmax(fitness))
                new_pop.append(dict(population[best_idx]))
                while len(new_pop) < self.config.ga_population_size:
                    if random.random() < self.config.ga_crossover_rate and len(population) >= 2:
                        p1 = self._tournament(population, fitness)
                        p2 = self._tournament(population, fitness)
                        child = self._mutate(self._crossover(p1, p2))
                    else:
                        child = self._mutate(dict(self._tournament(population, fitness)))
                    new_pop.append(child)
                population = new_pop

            self.pareto_front = local_front
            if local_front:
                # Select best from Pareto
                w = self.config.mopd.objective_weights
                keys = list(w.keys())
                max_vals = {k: max(getattr(p, k) for p in local_front) for k in keys}
                min_vals = {k: min(getattr(p, k) for p in local_front) for k in keys}
                ranges = {
                    k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
                    for k in keys
                }
                best_score = -math.inf
                for p in local_front:
                    s = sum(
                        w.get(k, 0.0) * ((getattr(p, k) - min_vals[k]) / ranges[k])
                        for k in keys
                    )
                    if s > best_score:
                        best_score = s
                        self.best_individual = dict(p.individual)
                self.best_fitness = best_score

            self.evolution_history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "generations": generations,
                "best_fitness": self.best_fitness,
                "pareto_size": len(self.pareto_front),
            })

            # Apply best weights to manager's compartment params
            if self.best_individual:
                self.manager._compartment_params["health_score_weights"] = dict(
                    self.best_individual["health_score_weights"]
                )

            return {
                "best_fitness": self.best_fitness,
                "best_individual": self.best_individual,
                "generations": generations,
                "pareto_front": [p.to_dict() for p in self.pareto_front],
            }

    def _tournament(self, population: List[Dict[str, Any]], fitness: List[float]) -> Dict[str, Any]:
        n = len(population)
        k = min(self.config.ga_tournament_size, n)
        idxs = random.sample(range(n), k)
        best = max(idxs, key=lambda i: fitness[i])
        return population[best]

    def _crossover(self, p1: Dict[str, Any], p2: Dict[str, Any]) -> Dict[str, Any]:
        child = {"health_score_weights": {}}
        for key in p1["health_score_weights"]:
            if random.random() < 0.5:
                child["health_score_weights"][key] = p1["health_score_weights"][key]
            else:
                child["health_score_weights"][key] = p2["health_score_weights"][key]
        return child

    def _mutate(self, individual: Dict[str, Any]) -> Dict[str, Any]:
        mutant = {"health_score_weights": dict(individual["health_score_weights"])}
        for k in mutant["health_score_weights"]:
            if random.random() < self.config.ga_mutation_rate:
                mutant["health_score_weights"][k] = random.uniform(0.05, 0.9)
        return mutant

    def to_dict(self) -> Dict[str, Any]:
        return {
            "best_fitness": self.best_fitness,
            "best_individual": self.best_individual,
            "evolution_history": self.evolution_history,
            "pareto_front": [p.to_dict() for p in self.pareto_front],
        }

    def from_dict(self, data: Dict[str, Any]) -> None:
        data = data or {}
        self.best_fitness = float(data.get("best_fitness", -math.inf))
        self.best_individual = data.get("best_individual")
        self.evolution_history = list(data.get("evolution_history", []))
        self.pareto_front = [MOPDPoint.from_dict(p) for p in data.get("pareto_front", [])]


# =============================================================================
# SECTION 14. TASK MANAGER (STABLE)
# =============================================================================
class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._drained = False

    def start_task(self, name: str, coro_func: Callable, *args, **kwargs) -> Optional[asyncio.Task]:
        async def wrapper():
            backoff = 1.0
            max_backoff = 60.0
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
# SECTION 15. PERSISTENCE (EXPERIMENTAL) — JSON with real to_dict/from_dict
# =============================================================================
class CompartmentPersistenceManager:
    STATUS = "experimental"
    CURRENT_VERSION = "4.0"

    def __init__(self, config: CompartmentConfig, enabled: bool = True):
        if enabled:
            _warn_module("persistence")
        self.config = config
        self.path = Path(config.persistence_path)
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def save_state(self, manager: "HierarchicalCompartmentManager") -> bool:
        async with self._get_lock():
            try:
                state = {
                    "version": self.CURRENT_VERSION,
                    "config": manager.config.to_dict(),
                    "regions": {rid: r.to_dict() for rid, r in manager.regions.items()},
                    "compartment_to_region": dict(manager.compartment_to_region),
                    "compartments": {cid: c.to_dict() for cid, c in manager.compartments.items()},
                    "global_health": manager.global_health,
                    "total_compartments_created": manager.total_compartments_created,
                    "total_apoptosis_events": manager.total_apoptosis_events,
                    "knowledge_bank": {k: list(v) for k, v in manager.knowledge_bank.items()},
                    "genetic_optimizer": manager.genetic_optimizer.to_dict(),
                    "homeostatic_controller": manager.homeostatic_controller.to_dict(),
                    "_compartment_params": manager._compartment_params,
                }
                with open(self.path, "w") as f:
                    json.dump(state, f, indent=2, default=str)
                logger.info("Compartment state saved", path=str(self.path))
                return True
            except Exception as e:
                logger.error("State save failed", error=str(e))
                return False

    async def load_state(self, manager: "HierarchicalCompartmentManager") -> bool:
        async with self._get_lock():
            if not self.path.exists():
                return False
            try:
                with open(self.path, "r") as f:
                    state = json.load(f)
                version = state.get("version", "0.0")
                if version != self.CURRENT_VERSION:
                    logger.warning("State version mismatch; ignoring", stored=version)
                    return False

                manager.config = CompartmentConfig.from_dict(state.get("config", {}))
                manager.regions = {
                    rid: RegionAggregator.from_dict(rdata)
                    for rid, rdata in (state.get("regions") or {}).items()
                }
                manager.compartment_to_region = dict(state.get("compartment_to_region") or {})
                manager.compartments = {
                    cid: ChromatophoreCompartment.from_dict(cdata)
                    for cid, cdata in (state.get("compartments") or {}).items()
                }
                manager.global_health = float(state.get("global_health", 0.7))
                manager.total_compartments_created = int(state.get("total_compartments_created", 0))
                manager.total_apoptosis_events = int(state.get("total_apoptosis_events", 0))
                manager.knowledge_bank = defaultdict(
                    list,
                    {k: list(v) for k, v in (state.get("knowledge_bank") or {}).items()},
                )
                manager.genetic_optimizer.from_dict(state.get("genetic_optimizer") or {})
                manager.homeostatic_controller.from_dict(state.get("homeostatic_controller") or {})
                manager._compartment_params = state.get(
                    "_compartment_params", manager._compartment_params
                )
                logger.info("Compartment state loaded", path=str(self.path))
                return True
            except Exception as e:
                logger.error("State load failed", error=str(e))
                return False


# =============================================================================
# SECTION 16. CIRCUIT BREAKER (STABLE) — lazy lock, executor I/O
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, name: str, db_path: str, failure_threshold: int = 5,
                 timeout_seconds: float = 60.0):
        self.name = name
        self.db_path = db_path
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure: Optional[datetime] = None
        self._lock: Optional[asyncio.Lock] = None
        self._init_db_sync()
        self._load_sync()

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _init_db_sync(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS circuit_breaker (
                    name TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    failures INTEGER NOT NULL,
                    last_failure TEXT
                )
            """)
            conn.commit()

    def _load_sync(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT state, failures, last_failure FROM circuit_breaker WHERE name = ?",
                (self.name,),
            ).fetchone()
        if row:
            try:
                self._state = CircuitBreakerState(row[0])
            except ValueError:
                self._state = CircuitBreakerState.CLOSED
            self._failure_count = int(row[1])
            self._last_failure = datetime.fromisoformat(row[2]) if row[2] else None

    def _save_sync(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO circuit_breaker
                   (name, state, failures, last_failure) VALUES (?, ?, ?, ?)""",
                (
                    self.name,
                    self._state.value,
                    self._failure_count,
                    self._last_failure.isoformat() if self._last_failure else None,
                ),
            )
            conn.commit()

    async def _persist(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._save_sync)
        except RuntimeError:
            self._save_sync()

    async def call(self, func: Callable, *args, **kwargs):
        lock = self._get_lock()
        async with lock:
            if self._state == CircuitBreakerState.OPEN:
                if self._last_failure and (
                    datetime.now(timezone.utc) - self._last_failure
                ).total_seconds() >= self.timeout_seconds:
                    self._state = CircuitBreakerState.HALF_OPEN
                    await self._persist()
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
        except Exception:
            async with lock:
                self._failure_count += 1
                self._last_failure = datetime.now(timezone.utc)
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                await self._persist()
            raise
        async with lock:
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.CLOSED
                self._failure_count = 0
                await self._persist()
            elif self._failure_count > 0:
                self._failure_count = 0
                await self._persist()
        return result

    def snapshot(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "state": self._state.value,
            "failures": self._failure_count,
        }


# =============================================================================
# SECTION 17. MAIN MANAGER
# =============================================================================
class HierarchicalCompartmentManager:
    """
    Lifecycle:
        mgr = HierarchicalCompartmentManager(...)
        await mgr.start()       # or `async with`
        ...
        await mgr.shutdown()
    """

    def __init__(
        self,
        config: Optional[CompartmentConfig] = None,
        token_manager: Optional[Any] = None,
        gradient_manager: Optional[Any] = None,
        storage: Optional[Any] = None,
        message_queue: Optional[Any] = None,
        adaptive_cost: Optional[Any] = None,
        pareto_gating: Optional[Any] = None,
        drift_detector: Optional[Any] = None,
        metrics: Optional[Any] = None,
    ):
        self.config = config or CompartmentConfig()
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating
        self.drift_detector = drift_detector
        self.metrics = metrics

        # Structure state
        self.regions: Dict[str, RegionAggregator] = {}
        self.compartment_to_region: Dict[str, str] = {}
        self.compartments: Dict[str, ChromatophoreCompartment] = {}
        self.global_health = 0.7
        self.total_compartments_created = 0
        self.total_apoptosis_events = 0
        self.knowledge_bank: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        # Params optimized by GA
        self._compartment_params: Dict[str, Any] = {
            "health_score_weights": {
                "success_rate": 0.4,
                "efficiency_score": 0.3,
                "trust_gradient": 0.3,
            },
        }

        # Lazy locks
        self._structure_lock: Optional[asyncio.Lock] = None
        self._persistence_lock: Optional[asyncio.Lock] = None

        # Subsystems
        self.event_bus = EventBus()
        self.security = SecurityService()
        self.central_health_model = HealthModelPlaceholder(
            model_path=self.config.health_model_path,
            enabled=False,
        )
        self.knowledge_bank_handler = KnowledgeBankPlaceholder(enabled=False)
        self.genetic_optimizer = GeneticOptimizer(self, self.config, enabled=True)
        self.homeostatic_controller = HomeostaticSetpointController(
            self.config, enabled=True,
        )
        self.xai = XAIExplainer(enabled=self.config.enable_xai)

        # Placeholders
        self.rl_agent = CausalRLAgentPlaceholder(
            state_dim=self.config.rl_state_dim,
            action_dim=self.config.rl_action_dim,
            max_q_table=self.config.q_table_max_size,
            enabled=False,
        )
        self.federated = (
            FederatedCoordinatorPlaceholder(self, message_queue, enabled=False)
            if message_queue is not None else None
        )
        self.precision_controller = PrecisionControllerPlaceholder(
            enabled=self.config.enable_precision_switching
        )
        self.carbon_market = (
            CarbonMarketClientPlaceholder(enabled=self.config.enable_carbon_market)
            if self.config.enable_carbon_market else None
        )
        self.chaos_injector = ChaosInjectorPlaceholder(
            self, self.config.chaos_probability,
            enabled=self.config.enable_chaos,
        )
        self.human_approval = HumanApprovalHandlerPlaceholder(
            self.queue, enabled=self.config.enable_human_approval,
        )

        # Safety monitor (experimental)
        self.safety_monitor: Optional[SafetyMonitor] = None
        if self.config.enable_safety_monitor:
            self.safety_monitor = SafetyMonitor(enabled=True)
            self._setup_safety_invariants()

        # Circuit breaker
        self.circuit_breaker = (
            CircuitBreaker(
                name="compartment_manager",
                db_path=self.config.circuit_breaker_db_path,
                failure_threshold=self.config.circuit_breaker_failure_threshold,
                timeout_seconds=self.config.circuit_breaker_timeout_seconds,
            )
            if self.config.enable_circuit_breaker else None
        )

        # Persistence
        self.persistence = (
            CompartmentPersistenceManager(self.config, enabled=True)
            if self.config.enable_persistence else None
        )

        # Task manager
        self._task_manager = TaskManager()
        self._last_global_balance = datetime.now(timezone.utc)

        # Lifecycle
        self._started = False
        self._shutdown = False

        # Metrics
        self._prom = self._setup_metrics()

        self._ensure_region_exists("default")
        logger.info(
            "HierarchicalCompartmentManager initialized",
            mopd=self.config.mopd.enabled,
            persistence=self.persistence is not None,
        )

    # ---------------- locks ----------------
    def _get_structure_lock(self) -> asyncio.Lock:
        if self._structure_lock is None:
            self._structure_lock = asyncio.Lock()
        return self._structure_lock

    def _get_persistence_lock(self) -> asyncio.Lock:
        if self._persistence_lock is None:
            self._persistence_lock = asyncio.Lock()
        return self._persistence_lock

    # ---------------- safety invariants ----------------
    def _setup_safety_invariants(self) -> None:
        assert self.safety_monitor is not None
        max_compartments = self.config.max_regions * self.config.compartments_per_region
        self.safety_monitor.add_invariant(
            "max_compartments",
            lambda s: s.get("total_compartments", 0) <= max_compartments,
            "Too many compartments",
        )
        self.safety_monitor.add_invariant(
            "global_health_min",
            lambda s: s.get("global_health", 0.0) >= 0.1,
            "Global health too low",
        )
        self.safety_monitor.add_invariant(
            "token_balance_non_negative",
            lambda s: s.get("total_tokens", 0.0) >= 0.0,
            "Total tokens negative",
        )

    # ---------------- metrics ----------------
    def _setup_metrics(self) -> Dict[str, Any]:
        if not PROMETHEUS_AVAILABLE:
            return {}
        try:
            return {
                "create_total": Counter("compartment_create_total", "Compartments created"),
                "decommission_total": Counter("compartment_decommission_total", "Compartments decommissioned"),
                "health_gauge": Gauge("compartment_global_health", "Global health"),
                "compartments_gauge": Gauge("compartment_count", "Current compartment count"),
                "pareto_gauge": Gauge("compartment_pareto_size", "Pareto front size"),
            }
        except Exception:
            return {}

    def _update_metrics(self) -> None:
        if not self._prom:
            return
        try:
            self._prom["health_gauge"].set(self.global_health)
            self._prom["compartments_gauge"].set(len(self.compartments))
            self._prom["pareto_gauge"].set(len(self.genetic_optimizer.pareto_front))
        except Exception:
            pass

    # ---------------- lifecycle ----------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        if self.persistence is not None:
            try:
                await self.persistence.load_state(self)
            except Exception as e:
                logger.warning("Initial state load failed", error=str(e))

        self._task_manager.start_task("ecosystem", self._ecosystem_loop)
        self._task_manager.start_task("trading", self._trading_loop)
        self._task_manager.start_task("evolution", self._evolution_loop)
        self._task_manager.start_task("state_save", self._state_save_loop)

        logger.info("HierarchicalCompartmentManager started")

    async def ready(self) -> bool:
        if not self._started:
            return False
        return isinstance(self.compartments, dict) and isinstance(self.regions, dict)

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        timeout = timeout or float(self.config.shutdown_timeout_seconds)
        logger.info("Shutting down HierarchicalCompartmentManager")

        try:
            await asyncio.wait_for(self._task_manager.drain(timeout), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Task drain timed out")

        if self.persistence is not None:
            try:
                await self.persistence.save_state(self)
            except Exception as e:
                logger.warning("Final save failed", error=str(e))

        logger.info("Shutdown complete")

    async def __aenter__(self) -> "HierarchicalCompartmentManager":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- background loops ----------------
    async def _ecosystem_loop(self) -> None:
        while True:
            try:
                await self._ecosystem_tick()
                await asyncio.sleep(self.config.ecosystem_maintenance_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Ecosystem loop error", error=str(e))
                await asyncio.sleep(30)

    async def _ecosystem_tick(self) -> None:
        total_tokens = sum(
            c.token_balance for c in self.compartments.values()
        )
        adjustments = self.homeostatic_controller.compute_adjustment(
            self.global_health, total_tokens
        )
        if adjustments["spawn_rate_modifier"] > 1.05:
            await self.spawn_if_needed()
        if adjustments["cull_aggressiveness_modifier"] > 1.05:
            await self.cull_unhealthy()
        await self.balance_load()
        await self.health_check_all()
        self._update_metrics()

    async def _trading_loop(self) -> None:
        while True:
            try:
                await self._execute_trades()
                await asyncio.sleep(self.config.trading_maintenance_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Trading loop error", error=str(e))
                await asyncio.sleep(60)

    async def _execute_trades(self) -> None:
        async with self._get_structure_lock():
            for region in self.regions.values():
                for match in region.market.match_orders():
                    sid = match["seller"]
                    bid = match["buyer"]
                    amt = match["amount"]
                    if sid in self.compartments and bid in self.compartments:
                        seller = self.compartments[sid]
                        buyer = self.compartments[bid]
                        if seller.spend_tokens(amt, "trade"):
                            buyer.receive_tokens(amt, sid)

    async def _evolution_loop(self) -> None:
        while True:
            try:
                if self.config.enable_genetic_optimizer and len(self.compartments) >= 2:
                    await self.genetic_optimizer.evolve()
                await asyncio.sleep(self.config.ga_evolution_interval_hours * 3600.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Evolution loop error", error=str(e))
                await asyncio.sleep(3600)

    async def _state_save_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(600)
                if self.persistence is not None:
                    await self.persistence.save_state(self)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Periodic save failed", error=str(e))

    # ---------------- structure ----------------
    def _ensure_region_exists(self, region_id: str) -> RegionAggregator:
        if region_id not in self.regions:
            self.regions[region_id] = RegionAggregator(region_id, self.config.compartments_per_region)
        return self.regions[region_id]

    def _pick_region_for(self, expert_type: str) -> Optional[str]:
        # Prefer a region that already hosts the same expert type
        for rid, r in self.regions.items():
            if len(r.compartments) < r.max_compartments and any(
                c.expert_type == expert_type for c in r.compartments.values()
            ):
                return rid
        # Otherwise a region with capacity
        for rid, r in self.regions.items():
            if len(r.compartments) < r.max_compartments:
                return rid
        # Otherwise create a new region if budget allows
        if len(self.regions) < self.config.max_regions:
            new_rid = f"region_{expert_type}_{len(self.regions)}"
            self._ensure_region_exists(new_rid)
            return new_rid
        return None

    def _get_safety_state(self) -> Dict[str, Any]:
        return {
            "total_compartments": len(self.compartments),
            "global_health": self.global_health,
            "total_tokens": sum(c.token_balance for c in self.compartments.values()),
        }

    @traced("compartment.create")
    async def create_compartment(
        self, expert_type: str, expert_instance: Optional[Any] = None,
        resources: Optional[CompartmentResource] = None,
        parent_id: Optional[str] = None, region_id: Optional[str] = None,
    ) -> Optional[ChromatophoreCompartment]:
        async with self._get_structure_lock():
            if self.safety_monitor is not None:
                state = self._get_safety_state()
                state["total_compartments"] += 1
                violations = self.safety_monitor.check(state)
                if violations:
                    logger.warning("Safety violation on create", violations=violations)
                    raise RuntimeError("Safety violation: " + "; ".join(violations))

            if self.human_approval is not None:
                approved = await self.human_approval.request_approval(
                    {"action": "create_compartment", "expert_type": expert_type}
                )
                if not approved:
                    return None

            if region_id is None:
                region_id = self._pick_region_for(expert_type)
                if region_id is None:
                    logger.warning("No region available for new compartment")
                    return None
            self._ensure_region_exists(region_id)

            compartment_id = f"comp_{expert_type}_{uuid.uuid4().hex[:8]}"
            compartment = ChromatophoreCompartment(
                compartment_id=compartment_id,
                expert_type=expert_type,
                expert_instance=expert_instance,
                resources=resources or CompartmentResource(),
            )
            if parent_id:
                compartment.parent_id = parent_id

            # Place the compartment: try target region, then any other
            placed = False
            region = self.regions[region_id]
            if region.add_compartment(compartment):
                placed = True
            else:
                for rid, r in self.regions.items():
                    if rid == region_id:
                        continue
                    if r.add_compartment(compartment):
                        region_id = rid
                        placed = True
                        break
            if not placed:
                logger.warning("Could not place compartment; all regions full")
                return None

            self.compartment_to_region[compartment_id] = region_id
            self.compartments[compartment_id] = compartment
            self.total_compartments_created += 1
            compartment.state = CompartmentState.MATURING

            if self._prom:
                try:
                    self._prom["create_total"].inc()
                except Exception:
                    pass

            await self.event_bus.publish("compartment_created", {
                "compartment_id": compartment_id,
                "region_id": region_id,
                "expert_type": expert_type,
            })

            if self.queue is not None and FeedbackEvent is not None:
                try:
                    event = FeedbackEvent.create_with_context(
                        task_id=f"compartment_create_{compartment_id}",
                        selected_action="create_compartment",
                        quality_score=compartment.health_score,
                        energy_joules=0.0,
                        carbon_g=0.0,
                        feedback_type="compartment",
                        adaptive_cost_value=0.0,
                        state={"compartment_id": compartment_id, "region_id": region_id},
                        candidates=[{"action": "create"}],
                        source="compartment_manager",
                        environment="production",
                        tags=["compartment", "create"],
                    )
                    await self.queue.publish("feedback_events", event.to_json())
                except Exception:
                    pass

            logger.info("Created compartment", id=compartment_id, region=region_id)
            return compartment

    @traced("compartment.decommission")
    async def decommission_compartment(self, compartment_id: str) -> Dict[str, Any]:
        async with self._get_structure_lock():
            compartment = self.compartments.get(compartment_id)
            if compartment is None:
                return {}

            region_id = self.compartment_to_region.get(compartment_id)
            remaining, knowledge = compartment.prepare_apoptosis()
            self.knowledge_bank[compartment.expert_type].append(knowledge)
            if region_id and region_id in self.regions:
                self.regions[region_id].knowledge_transfer.add_knowledge(region_id, knowledge)
                self.regions[region_id].remove_compartment(compartment_id)

            await self.knowledge_bank_handler.store(knowledge)
            self.compartments.pop(compartment_id, None)
            self.compartment_to_region.pop(compartment_id, None)
            self.total_apoptosis_events += 1

            if self._prom:
                try:
                    self._prom["decommission_total"].inc()
                except Exception:
                    pass

            if self.xai is not None:
                logger.info("Decommission", text=self.xai.explain_decommission(compartment))

            await self.event_bus.publish("compartment_decommissioned", {
                "compartment_id": compartment_id,
                "region_id": region_id,
                "remaining_tokens": remaining,
            })
            return knowledge

    # ---------------- balancing & health ----------------
    async def balance_load(self) -> int:
        async with self._get_structure_lock():
            transfers = 0
            for r in self.regions.values():
                transfers += r.balance_load_local()
            if (datetime.now(timezone.utc) - self._last_global_balance).total_seconds() > 60:
                transfers += self._balance_across_regions()
                self._last_global_balance = datetime.now(timezone.utc)
            return transfers

    def _balance_across_regions(self) -> int:
        """Move the largest-loaded compartment from the heaviest region to the lightest."""
        if len(self.regions) < 2:
            return 0
        loads = {
            rid: sum(len(getattr(c, "glycogen_queue", [])) for c in r.compartments.values())
            for rid, r in self.regions.items()
        }
        if not loads:
            return 0
        heaviest = max(loads, key=loads.get)
        lightest = min(loads, key=loads.get)
        if heaviest == lightest:
            return 0
        src = self.regions[heaviest]
        dst = self.regions[lightest]
        if not src.compartments or len(dst.compartments) >= dst.max_compartments:
            return 0
        # Move the compartment with the largest queue
        cid = max(
            src.compartments, key=lambda k: len(getattr(src.compartments[k], "glycogen_queue", []))
        )
        comp = src.compartments.pop(cid)
        dst.add_compartment(comp)
        self.compartment_to_region[cid] = lightest
        logger.info("Moved compartment", id=cid, src=heaviest, dst=lightest)
        return 1

    async def health_check_all(self) -> Dict[str, float]:
        async with self._get_structure_lock():
            scores: Dict[str, float] = {}
            for rid, r in self.regions.items():
                h = r.health_check()
                if h is not None:
                    scores[rid] = h
                for comp in r.compartments.values():
                    comp._evaluate_lifecycle()
            self.global_health = float(np.mean(list(scores.values()))) if scores else 0.5
            return scores

    async def cull_unhealthy(self) -> int:
        async with self._get_structure_lock():
            total = 0
            for r in self.regions.values():
                removed = r.cull_unhealthy()
                for cid in removed:
                    self.compartment_to_region.pop(cid, None)
                    self.compartments.pop(cid, None)
                total += len(removed)
            return total

    async def spawn_if_needed(self) -> None:
        expert_types = {c.expert_type for c in self.compartments.values()}
        for et in expert_types:
            viable = sum(
                1 for c in self.compartments.values()
                if c.expert_type == et and c.is_viable
            )
            if viable < 2:
                try:
                    await self.create_compartment(et)
                except Exception as e:
                    logger.debug("Spawn failed", error=str(e))

    # ---------------- selection & policy ----------------
    @traced("compartment.select")
    async def find_best_compartment(self, expert_type: str, task_complexity: float = 1.0) -> Optional[ChromatophoreCompartment]:
        candidates = [
            c for c in self.compartments.values()
            if c.expert_type == expert_type and c.is_viable
        ]
        if not candidates:
            return None

        # Preferred path: adaptive cost + Pareto gating
        if self.adaptive_cost is not None and self.pareto_gating is not None:
            try:
                cands = [
                    {
                        "expert_id": c.compartment_id,
                        "quality_score": c.health_score,
                        "carbon_g": 0.0,
                        "latency_ms": 0.0,
                        "energy_joules": 0.0,
                        "compartment": c,
                    }
                    for c in candidates
                ]
                filtered = self.pareto_gating.filter(cands)
                if filtered:
                    cands = filtered
                best = None
                best_cost = math.inf
                for entry in cands:
                    try:
                        cost = float(self.adaptive_cost.compute(
                            quality=entry["quality_score"],
                            carbon_g=entry["carbon_g"],
                            latency_ms=entry["latency_ms"],
                            energy_joules=entry["energy_joules"],
                            health=entry["quality_score"],
                            atp=0.5,
                        ))
                    except Exception:
                        cost = math.inf
                    if cost < best_cost:
                        best_cost = cost
                        best = entry["compartment"]
                if best is not None:
                    return best
            except Exception:
                pass

        # Fallback: weighted score
        weights = self._compartment_params.get("health_score_weights", {})
        scored: List[Tuple[ChromatophoreCompartment, float]] = []
        for c in candidates:
            score = (
                c.health_score * weights.get("success_rate", 0.4)
                + c.efficiency_score * weights.get("efficiency_score", 0.3)
                + min(c.token_balance / max(task_complexity * 10.0, 1.0), 1.0)
                * weights.get("trust_gradient", 0.3)
            )
            scored.append((c, score))
        scored.sort(key=lambda x: x[1], reverse=True)
        if self.xai is not None:
            logger.info("Select", text=self.xai.explain_select(scored[0][0], scored))
        return scored[0][0]

    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """
        Fixed-length policy over {create, cull, balance}. Always valid.
        """
        try:
            feats = self._state_to_features(state)
            probs = self.rl_agent.get_policy_probs(feats)
            arr = np.asarray(probs, dtype=float)
            arr = np.clip(arr, 0.0, None)
            total = float(arr.sum())
            if total <= 0 or not np.isfinite(total):
                return [1.0 / self.config.rl_action_dim] * self.config.rl_action_dim
            return (arr / total).tolist()
        except Exception:
            return [1.0 / self.config.rl_action_dim] * self.config.rl_action_dim

    def _state_to_features(self, state: Dict[str, Any]) -> np.ndarray:
        total_tokens = sum(c.token_balance for c in self.compartments.values())
        avg_eff = (
            float(np.mean([c.efficiency_score for c in self.compartments.values()]))
            if self.compartments else 0.5
        )
        return np.array([
            float(state.get("global_health", self.global_health)),
            min(total_tokens / 2000.0, 1.0),
            min(len(self.compartments) / 100.0, 1.0),
            float(state.get("demand", 0.5)),
            avg_eff,
            float(state.get("carbon", 0.0)),
            float(state.get("region_count", len(self.regions)) / max(self.config.max_regions, 1)),
            float(state.get("viable_fraction",
                           sum(1 for c in self.compartments.values() if c.is_viable) / max(len(self.compartments), 1))),
            float(state.get("avg_token", (total_tokens / max(len(self.compartments), 1)) / 1000.0)),
            float(state.get("decommission_rate",
                            self.total_apoptosis_events / max(self.total_compartments_created, 1))),
        ], dtype=float)

    # ---------------- stats ----------------
    async def get_ecosystem_stats(self) -> Dict[str, Any]:
        return {
            "total_compartments": len(self.compartments),
            "viable_compartments": sum(r.get_viable_count() for r in self.regions.values()),
            "global_health": self.global_health,
            "total_regions": len(self.regions),
            "total_created": self.total_compartments_created,
            "total_apoptosis": self.total_apoptosis_events,
            "q_table_size": self.rl_agent.size(),
            "pareto_front_size": len(self.genetic_optimizer.pareto_front),
            "genetic_best_fitness": self.genetic_optimizer.best_fitness,
            "module_status": MODULE_STATUS,
            "circuit_breaker": self.circuit_breaker.snapshot() if self.circuit_breaker else None,
        }

    async def health_report(self) -> Dict[str, Any]:
        return {
            "ready": await self.ready(),
            "global_health": self.global_health,
            "compartments": len(self.compartments),
            "regions": len(self.regions),
        }


# =============================================================================
# SECTION 18. LEGACY COMPAT
# =============================================================================
class CompartmentManager(HierarchicalCompartmentManager):
    def __init__(self, token_manager: Optional[Any] = None):
        super().__init__(
            config=CompartmentConfig(max_regions=5, compartments_per_region=20),
            token_manager=token_manager,
        )


# =============================================================================
# SECTION 19. TESTS
# =============================================================================
class _Tests(unittest.TestCase):
    def _cfg(self) -> CompartmentConfig:
        import tempfile
        td = tempfile.mkdtemp()
        return CompartmentConfig(
            max_regions=3,
            compartments_per_region=5,
            persistence_path=os.path.join(td, "state.json"),
            circuit_breaker_db_path=os.path.join(td, "cb.db"),
            ga_population_size=6,
            ga_generations=2,
            ecosystem_maintenance_interval_seconds=0.2,
            trading_maintenance_interval_seconds=0.5,
        )

    def test_lifecycle(self):
        async def go():
            mgr = HierarchicalCompartmentManager(config=self._cfg())
            self.assertFalse(await mgr.ready())
            await mgr.start()
            self.assertTrue(await mgr.ready())
            await mgr.shutdown()
            self.assertTrue(mgr._shutdown)
            await mgr.shutdown()  # idempotent
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with HierarchicalCompartmentManager(config=self._cfg()) as mgr:
                self.assertTrue(await mgr.ready())
        asyncio.run(go())

    def test_create_and_decommission(self):
        async def go():
            async with HierarchicalCompartmentManager(config=self._cfg()) as mgr:
                c = await mgr.create_compartment("expert_a")
                self.assertIsNotNone(c)
                self.assertEqual(len(mgr.compartments), 1)
                knowledge = await mgr.decommission_compartment(c.compartment_id)
                self.assertIn("health_score", knowledge)
                self.assertEqual(len(mgr.compartments), 0)
        asyncio.run(go())

    def test_max_compartments_invariant(self):
        async def go():
            cfg = self._cfg()
            cfg.max_regions = 1
            cfg.compartments_per_region = 3
            async with HierarchicalCompartmentManager(config=cfg) as mgr:
                for i in range(3):
                    await mgr.create_compartment(f"expert_{i}")
                with self.assertRaises(RuntimeError):
                    await mgr.create_compartment("overflow")
        asyncio.run(go())

    def test_policy_probs_valid(self):
        async def go():
            async with HierarchicalCompartmentManager(config=self._cfg()) as mgr:
                probs = await mgr.policy_probs({})
                self.assertEqual(len(probs), mgr.config.rl_action_dim)
                self.assertTrue(all(p >= 0 for p in probs))
                self.assertAlmostEqual(sum(probs), 1.0, places=6)
        asyncio.run(go())

    def test_q_table_bounded(self):
        rl = CausalRLAgentPlaceholder(state_dim=4, action_dim=3, max_q_table=10)
        for _ in range(200):
            s = np.random.rand(4)
            a = rl.act(s)
            rl.update(s, a, 1.0, s, False)
        self.assertLessEqual(rl.size(), 10)

    def test_knowledge_transfer(self):
        kt = KnowledgeTransfer()
        kt.add_knowledge("r1", {"score": 0.5})
        kt.add_knowledge("r2", {"score": 0.7})
        self.assertEqual(len(kt.latest(10)), 2)
        d = kt.to_dict()
        kt2 = KnowledgeTransfer.from_dict(d)
        self.assertEqual(len(kt2.latest(10)), 2)

    def test_region_roundtrip(self):
        r = RegionAggregator("r1", max_compartments=5)
        comp = ChromatophoreCompartment("c1", "expert_x")
        r.add_compartment(comp)
        r.knowledge_transfer.add_knowledge("r1", {"score": 0.5})
        d = r.to_dict()
        r2 = RegionAggregator.from_dict(d)
        self.assertIn("c1", r2.compartments)
        self.assertEqual(r2.compartments["c1"].expert_type, "expert_x")

    def test_ga_fitness_genome_dependent(self):
        async def go():
            async with HierarchicalCompartmentManager(config=self._cfg()) as mgr:
                for _ in range(5):
                    await mgr.create_compartment("expert_x")
                # Two individuals with very different weights should produce
                # different fitness on the same compartments
                ind_a = {"health_score_weights": {"success_rate": 0.9, "efficiency_score": 0.05, "trust_gradient": 0.05}}
                ind_b = {"health_score_weights": {"success_rate": 0.05, "efficiency_score": 0.05, "trust_gradient": 0.9}}
                oa = await mgr.genetic_optimizer._evaluate_individual(ind_a)
                ob = await mgr.genetic_optimizer._evaluate_individual(ind_b)
                self.assertNotAlmostEqual(oa["health"], ob["health"], places=4)
        asyncio.run(go())

    def test_persistence_roundtrip(self):
        async def go():
            cfg = self._cfg()
            mgr = HierarchicalCompartmentManager(config=cfg)
            async with mgr:
                c = await mgr.create_compartment("expert_a")
                await mgr.persistence.save_state(mgr)
                self.assertTrue(os.path.exists(cfg.persistence_path))

            mgr2 = HierarchicalCompartmentManager(config=cfg)
            async with mgr2:
                await mgr2.persistence.load_state(mgr2)
                self.assertEqual(len(mgr2.compartments), 1)
                # Compartment should still be a real object
                cid = next(iter(mgr2.compartments))
                self.assertIsInstance(mgr2.compartments[cid], ChromatophoreCompartment)
        asyncio.run(go())

    def test_circuit_breaker_transitions(self):
        async def go():
            import tempfile
            td = tempfile.mkdtemp()
            cb = CircuitBreaker("test", os.path.join(td, "cb.db"),
                                failure_threshold=2, timeout_seconds=0.5)

            async def ok():
                return 1

            async def fail():
                raise RuntimeError("boom")

            self.assertEqual(await cb.call(ok), 1)
            for _ in range(2):
                try:
                    await cb.call(fail)
                except RuntimeError:
                    pass
            self.assertEqual(cb.snapshot()["state"], "open")
            with self.assertRaises(RuntimeError):
                await cb.call(ok)
            await asyncio.sleep(0.6)
            self.assertEqual(await cb.call(ok), 1)
            self.assertEqual(cb.snapshot()["state"], "closed")
        asyncio.run(go())

    def test_health_check_skips_empty(self):
        async def go():
            cfg = self._cfg()
            cfg.max_regions = 3
            async with HierarchicalCompartmentManager(config=cfg) as mgr:
                # No compartments -> global health is neutral 0.5
                scores = await mgr.health_check_all()
                self.assertEqual(len(scores), 0)
                self.assertAlmostEqual(mgr.global_health, 0.5, places=2)
        asyncio.run(go())


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 20. ENTRY POINT
# =============================================================================
async def _example() -> None:
    async with HierarchicalCompartmentManager() as mgr:
        await mgr.create_compartment("analyst")
        await mgr.create_compartment("analyst")
        await mgr.create_compartment("retriever")
        await asyncio.sleep(0.5)
        print("stats:", json.dumps(await mgr.get_ecosystem_stats(), indent=2, default=str))
        print("probs:", await mgr.policy_probs({}))


def main() -> None:
    parser = argparse.ArgumentParser(description="Chromatophore Compartments v8.0.0")
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

    print("Chromatophore Compartments v8.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
