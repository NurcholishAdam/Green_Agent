#!/usr/bin/env python3
# =============================================================================
# Bio-Integrated Green Agent v13.0.0
# =============================================================================
"""
Bio-Integrated Green Agent v13.0.0
===================================
Patched single-file version. Focus on correctness, honesty, and lifecycle.

P0 fixes applied
----------------
- CircuitBreaker is always defined (no more NameError when CORE_AVAILABLE).
- Lazy asyncio.Lock in CircuitBreaker (no cross-loop binding).
- RL selector chooses by min cost (AdaptiveCostFunction returns a cost).
- FeedbackEvent now uses create_with_context when available, with a safe fallback.
- Strategy-application methods are guarded by hasattr; safe no-ops otherwise.
- CentralStorage.save_state/load_state are guarded by hasattr.
- No background tasks started inside __init__; use `await agent.start()`.

P1 additions
------------
- _refresh_state() pulls live metrics from services.
- _update_metrics() computes reward; RL selector.update() is called each tick.
- Q-table, state_last_visited, strategy_objectives_history, audit ledger bounded.
- Pareto dominance includes carbon (minimize) with correct direction.

P2 honesty
----------
- Swarm, proactive-healing, drift are placeholders: disabled by default,
  `.available == False`, safe no-ops, warn when enabled.
- MOPD and RL selector are experimental: warn when enabled.

P3 production readiness
-----------------------
- async start(), __aenter__ / __aexit__, ready().
- Graceful shutdown: drain + flush.
- Logical module sections (bio_agent, rl_selector, mopd, swarm, audit, security).
- MODULE_STATUS documentation.
- Embedded test suite: `python3 bio_integrated_agent.py --test`.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
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
import warnings
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple, Union

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

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
    import redis.asyncio as redis_asyncio  # type: ignore
    REDIS_AVAILABLE = True
except ImportError:
    redis_asyncio = None  # type: ignore
    REDIS_AVAILABLE = False

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
# SECTION 0. MODULE STATUS
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "bio_agent":           "stable",
    "circuit_breaker":     "stable",
    "task_manager":        "stable",
    "security":            "experimental",
    "audit":               "experimental",
    "rl_selector":         "experimental",
    "mopd":                "experimental",
    "swarm":               "placeholder",
    "proactive_healing":   "placeholder",
    "drift":               "placeholder",
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
    else:
        logger.warning("Unknown module status", module=name, status=status)


# =============================================================================
# SECTION 1. CIRCUIT BREAKER (STABLE)
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """
    Simple circuit breaker with optional SQLite persistence.

    P0 fixes:
      - Lazily created asyncio.Lock (no cross-loop binding).
      - SQLite I/O offloaded to a thread executor.
      - Idempotent transitions.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_attempts: int = 3,
        storage: Optional[Any] = None,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_attempts = half_open_attempts
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._half_open_attempt_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._lock: Optional[asyncio.Lock] = None
        self.storage = storage
        self._load_state_sync()

    # ---------- lazy lock ----------
    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    # ---------- persistence (sync; called from __init__ or executor) ----------
    def _load_state_sync(self) -> None:
        if not self.storage:
            return
        try:
            getter = getattr(self.storage, "get_circuit_breaker_state", None)
            if getter is None:
                return
            state = getter(self.name)
            if not state:
                return
            self._state = CircuitBreakerState(state["state"])
            self._failure_count = int(state.get("failures", 0))
            lf = state.get("last_failure")
            if lf:
                self._last_failure_time = datetime.fromisoformat(lf)
            self._half_open_attempt_count = int(state.get("half_open_attempts", 0))
        except Exception as e:
            logger.warning("Circuit breaker load failed", name=self.name, error=str(e))

    def _save_state_sync(self) -> None:
        if not self.storage:
            return
        try:
            saver = getattr(self.storage, "save_circuit_breaker_state", None)
            if saver is None:
                return
            saver(
                self.name,
                self._state.value,
                self._failure_count,
                self._last_failure_time.isoformat() if self._last_failure_time else None,
                self._half_open_attempt_count,
            )
        except Exception as e:
            logger.warning("Circuit breaker save failed", name=self.name, error=str(e))

    async def _persist(self) -> None:
        if not self.storage:
            return
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._save_state_sync)
        except RuntimeError:
            # No running loop: fall back to sync write
            self._save_state_sync()

    # ---------- API ----------
    async def call(self, func: Callable, *args, **kwargs):
        lock = self._get_lock()
        async with lock:
            if self._state == CircuitBreakerState.OPEN:
                if (
                    self._last_failure_time
                    and (datetime.now(timezone.utc) - self._last_failure_time).total_seconds()
                    > self.recovery_timeout
                ):
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._half_open_attempt_count = 0
                    await self._persist()
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
            elif self._state == CircuitBreakerState.HALF_OPEN:
                if self._half_open_attempt_count >= self.half_open_attempts:
                    self._state = CircuitBreakerState.OPEN
                    self._last_failure_time = datetime.now(timezone.utc)
                    await self._persist()
                    raise RuntimeError(
                        f"Circuit breaker {self.name} half-open attempts exceeded"
                    )

        try:
            result = await func(*args, **kwargs)
        except Exception:
            async with lock:
                self._failure_count += 1
                self._last_failure_time = datetime.now(timezone.utc)
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    logger.warning(
                        "Circuit breaker opened",
                        name=self.name,
                        failures=self._failure_count,
                    )
                elif self._state == CircuitBreakerState.HALF_OPEN:
                    self._half_open_attempt_count += 1
                await self._persist()
            raise

        async with lock:
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.CLOSED
                self._failure_count = 0
                self._half_open_attempt_count = 0
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
            "half_open_attempts": self._half_open_attempt_count,
            "last_failure": self._last_failure_time.isoformat()
            if self._last_failure_time else None,
        }


# =============================================================================
# SECTION 2. LOCAL STORAGE (STABLE)
# =============================================================================
class LocalCircuitBreakerStorage:
    """SQLite-backed storage for circuit breaker state. Safe for single process."""

    def __init__(self, db_path: str = "agent_storage.db"):
        self.db_path = db_path
        self._init_db_sync()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except sqlite3.Error:
            pass
        return conn

    def _init_db_sync(self) -> None:
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS circuit_breaker (
                    name TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    failures INTEGER NOT NULL,
                    last_failure TEXT,
                    half_open_attempts INTEGER DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_ledger (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    signature TEXT,
                    hash TEXT NOT NULL,
                    prev_hash TEXT
                )
            """)
            conn.commit()

    def save_circuit_breaker_state(
        self, name: str, state: str, failures: int,
        last_failure: Optional[str], half_open_attempts: int,
    ) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO circuit_breaker
                   (name, state, failures, last_failure, half_open_attempts)
                   VALUES (?, ?, ?, ?, ?)""",
                (name, state, failures, last_failure, half_open_attempts),
            )
            conn.commit()

    def get_circuit_breaker_state(self, name: str) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT state, failures, last_failure, half_open_attempts "
                "FROM circuit_breaker WHERE name = ?",
                (name,),
            ).fetchone()
            return dict(row) if row else None

    def append_ledger(
        self, timestamp: str, event_type: str, payload: str,
        signature: Optional[str], hash_: str, prev_hash: Optional[str],
    ) -> int:
        with self._get_conn() as conn:
            cur = conn.execute(
                """INSERT INTO audit_ledger
                   (timestamp, event_type, payload, signature, hash, prev_hash)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (timestamp, event_type, payload, signature, hash_, prev_hash),
            )
            conn.commit()
            return int(cur.lastrowid)

    def read_ledger(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT seq, timestamp, event_type, payload, signature, hash, prev_hash "
                "FROM audit_ledger ORDER BY seq DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def ledger_size(self) -> int:
        with self._get_conn() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM audit_ledger").fetchone()
            return int(row["c"]) if row else 0


# =============================================================================
# SECTION 3. CONFIGURATION (STABLE)
# =============================================================================
if PYDANTIC_AVAILABLE:
    class AgentConfig(BaseModel):
        agent_id: str = Field(default_factory=lambda: f"agent_{uuid.uuid4().hex[:8]}")
        enable_energy_aware_rl: bool = True
        enable_swarm_coordination: bool = False    # placeholder
        enable_multi_objective_rl: bool = True
        enable_proactive_healing: bool = False     # placeholder
        enable_drift_integration: bool = False     # placeholder
        enable_prometheus: bool = False

        rl_learning_rate: float = Field(0.1, ge=0.0, le=1.0)
        rl_discount_factor: float = Field(0.9, ge=0.0, le=1.0)
        rl_epsilon: float = Field(0.1, ge=0.0, le=1.0)
        rl_learning_rate_min: float = Field(0.01, ge=0.0, le=1.0)
        rl_epsilon_min: float = Field(0.01, ge=0.0, le=1.0)

        q_table_max_size: int = Field(5000, ge=100)
        q_table_prune_threshold: float = Field(0.1, ge=0.0, le=1.0)
        strategy_history_max: int = Field(200, ge=10)
        audit_ledger_in_memory_max: int = Field(500, ge=10)

        rl_strategies: List[str] = Field(default_factory=lambda: ["conservative", "balanced", "performance"])
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "energy_efficiency": 0.3,
                "helium_sustainability": 0.25,
                "token_balance": 0.2,
                "health_score": 0.15,
                "carbon_leakage": 0.1,
            }
        )

        strategy_update_interval: float = Field(5.0, ge=0.5)
        state_save_interval: float = Field(60.0, ge=5.0)
        state_save_path: str = "./agent_state.json"
        storage_db_path: str = "./agent_storage.db"
        pqc_key_dir: str = "./pqc_keys"

        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_recovery_timeout: float = 30.0
        circuit_breaker_half_open_attempts: int = 3

        shutdown_timeout_seconds: int = 15

        class Config:
            env_prefix = "AGENT_"
else:
    @dataclass
    class AgentConfig:
        agent_id: str = field(default_factory=lambda: f"agent_{uuid.uuid4().hex[:8]}")
        enable_energy_aware_rl: bool = True
        enable_swarm_coordination: bool = False
        enable_multi_objective_rl: bool = True
        enable_proactive_healing: bool = False
        enable_drift_integration: bool = False
        enable_prometheus: bool = False
        rl_learning_rate: float = 0.1
        rl_discount_factor: float = 0.9
        rl_epsilon: float = 0.1
        rl_learning_rate_min: float = 0.01
        rl_epsilon_min: float = 0.01
        q_table_max_size: int = 5000
        q_table_prune_threshold: float = 0.1
        strategy_history_max: int = 200
        audit_ledger_in_memory_max: int = 500
        rl_strategies: List[str] = field(default_factory=lambda: ["conservative", "balanced", "performance"])
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            "energy_efficiency": 0.3,
            "helium_sustainability": 0.25,
            "token_balance": 0.2,
            "health_score": 0.15,
            "carbon_leakage": 0.1,
        })
        strategy_update_interval: float = 5.0
        state_save_interval: float = 60.0
        state_save_path: str = "./agent_state.json"
        storage_db_path: str = "./agent_storage.db"
        pqc_key_dir: str = "./pqc_keys"
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_recovery_timeout: float = 30.0
        circuit_breaker_half_open_attempts: int = 3
        shutdown_timeout_seconds: int = 15


# =============================================================================
# SECTION 4. MOPD (EXPERIMENTAL)
# =============================================================================
@dataclass
class MOPDPoint:
    strategy: str
    energy_efficiency: float
    helium_sustainability: float
    token_balance: float
    health_score: float
    carbon_leakage: float
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(**data)


MAXIMIZE_KEYS = ("energy_efficiency", "helium_sustainability", "token_balance", "health_score")
MINIMIZE_KEYS = ("carbon_leakage",)


def pareto_filter(points: List[MOPDPoint]) -> List[MOPDPoint]:
    """Non-dominated filter with correct direction for minimization keys."""
    if not points:
        return []
    front: List[MOPDPoint] = []
    for i, p in enumerate(points):
        dominated = False
        for j, q in enumerate(points):
            if i == j:
                continue
            better_or_equal_max = all(getattr(q, k) >= getattr(p, k) for k in MAXIMIZE_KEYS)
            better_or_equal_min = all(getattr(q, k) <= getattr(p, k) for k in MINIMIZE_KEYS)
            strictly_better = (
                any(getattr(q, k) > getattr(p, k) for k in MAXIMIZE_KEYS)
                or any(getattr(q, k) < getattr(p, k) for k in MINIMIZE_KEYS)
            )
            if better_or_equal_max and better_or_equal_min and strictly_better:
                dominated = True
                break
        if not dominated:
            front.append(p)
    return front


# =============================================================================
# SECTION 5. RL SELECTOR (EXPERIMENTAL)
# =============================================================================
class RLStrategySelector:
    """
    Tabular Q-learning with bounded Q-table.
    STATUS: experimental. Warns on enable.
    """

    def __init__(self, config: AgentConfig, enabled: bool = True):
        if enabled:
            _warn_module("rl_selector")
        self.config = config
        self.actions: List[str] = list(config.rl_strategies)
        self.q_table: Dict[str, Dict[str, float]] = {}
        self.learning_rate = config.rl_learning_rate
        self.discount_factor = config.rl_discount_factor
        self.epsilon = config.rl_epsilon
        self.reward_history: deque = deque(maxlen=100)
        self.state_last_visited: Dict[str, float] = {}
        self.strategy_objectives_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=config.strategy_history_max)
        )
        self.pareto_front: List[MOPDPoint] = []
        self.adaptive_cost = None
        self.pareto_gating = None

    def set_central_components(self, adaptive_cost: Any, pareto_gating: Any) -> None:
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating

    def _state_to_key(self, state: Dict[str, float]) -> str:
        load = state.get("system_load", 0.5)
        health = state.get("health_score", 0.8)
        token = state.get("token_balance", 0)
        energy = state.get("energy_intensity", 0.5)
        helium = state.get("helium_level", 0.5)
        carbon = state.get("carbon_leakage_proxy", 0.3)
        alert_count = state.get("alert_count", 0)
        return (
            f"{'h' if load > 0.7 else 'm' if load > 0.4 else 'l'}"
            f"_{'g' if health > 0.7 else 'm' if health > 0.4 else 'p'}"
            f"_{'a' if token > 1000 else 'd' if token > 100 else 's'}"
            f"_{'e' if energy > 0.7 else 'n' if energy > 0.4 else 'i'}"
            f"_{'a' if helium > 0.7 else 'n' if helium > 0.3 else 's'}"
            f"_{'h' if carbon > 0.6 else 'm' if carbon > 0.3 else 'l'}"
            f"_{'m' if alert_count > 2 else 's' if alert_count > 0 else 'n'}"
        )

    def _ensure_state(self, key: str) -> Dict[str, float]:
        if key not in self.q_table:
            self.q_table[key] = {s: 0.0 for s in self.actions}
        return self.q_table[key]

    def _prune_q_table(self) -> None:
        cap = self.config.q_table_max_size
        if len(self.q_table) <= cap:
            return
        # Sort by last visited (oldest first) and drop
        items = sorted(self.state_last_visited.items(), key=lambda kv: kv[1])
        to_drop = len(self.q_table) - cap
        for k, _ in items[:to_drop]:
            self.q_table.pop(k, None)
            self.state_last_visited.pop(k, None)

    def select_action(self, state: Dict[str, float]) -> str:
        key = self._state_to_key(state)
        q_vals = self._ensure_state(key)
        self.state_last_visited[key] = time.time()

        # Prefer MOPD if we have a Pareto front and central components
        if self.adaptive_cost is not None and self.pareto_gating is not None and self.pareto_front:
            candidates = []
            for point in self.pareto_front:
                try:
                    cost = self.adaptive_cost.compute(
                        quality=point.energy_efficiency,
                        carbon_g=point.carbon_leakage * 1000.0,
                        latency_ms=0.0,
                        energy_joules=0.0,
                        health=point.health_score,
                        atp=point.token_balance,
                    )
                    candidates.append((float(cost), point.strategy))
                except Exception:
                    continue
            if candidates:
                # AdaptiveCostFunction returns a cost; lower is better.
                return min(candidates, key=lambda x: x[0])[1]

        # Epsilon-greedy
        if random.random() < self.epsilon:
            action = random.choice(self.actions)
        else:
            max_q = max(q_vals.values())
            best = [a for a, q in q_vals.items() if q == max_q]
            action = random.choice(best)

        self._prune_q_table()
        return action

    def update(
        self,
        state: Dict[str, float],
        action: str,
        reward: float,
        next_state: Dict[str, float],
        objectives: Optional[Dict[str, float]] = None,
    ) -> None:
        key = self._state_to_key(state)
        next_key = self._state_to_key(next_state)
        current_q = self.q_table.setdefault(key, {s: 0.0 for s in self.actions}).get(action, 0.0)
        next_q = self.q_table.setdefault(next_key, {s: 0.0 for s in self.actions})
        next_max_q = max(next_q.values()) if next_q else 0.0
        self.q_table[key][action] = current_q + self.learning_rate * (
            reward + self.discount_factor * next_max_q - current_q
        )
        self.reward_history.append(reward)

        if objectives is not None:
            self.strategy_objectives_history[action].append(objectives)

        self._update_pareto_front()
        self._prune_q_table()

        # Slow epsilon decay
        self.epsilon = max(self.config.rl_epsilon_min, self.epsilon * 0.995)

    def _update_pareto_front(self) -> None:
        if not self.strategy_objectives_history:
            self.pareto_front = []
            return
        points: List[MOPDPoint] = []
        for strategy, objs in self.strategy_objectives_history.items():
            if not objs:
                continue
            avg = {k: float(np.mean([o.get(k, 0.0) for o in objs])) for k in objs[0].keys()}
            points.append(MOPDPoint(
                strategy=strategy,
                energy_efficiency=avg.get("energy_efficiency", 0.0),
                helium_sustainability=avg.get("helium_sustainability", 0.0),
                token_balance=avg.get("token_balance", 0.0),
                health_score=avg.get("health_score", 0.0),
                carbon_leakage=avg.get("carbon_leakage", 1.0),
            ))
        self.pareto_front = pareto_filter(points)

    def get_pareto_front(self) -> List[MOPDPoint]:
        return list(self.pareto_front)

    def q_table_size(self) -> int:
        return len(self.q_table)


# =============================================================================
# SECTION 6. PLACEHOLDERS (SWARM, HEALING, DRIFT)
# =============================================================================
class SwarmCoordinatorPlaceholder:
    """STATUS: placeholder. No Redis, no coordination."""

    STATUS = "placeholder"

    def __init__(self, agent_id: str, enabled: bool = False):
        self.agent_id = agent_id
        self.available = False
        self.peers: Dict[str, Any] = {}
        if enabled:
            _warn_module("swarm")

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def share(self, data: Dict[str, Any]) -> None:
        return None


class ProactiveHealingPlaceholder:
    """STATUS: placeholder. Does not detect or heal."""

    STATUS = "placeholder"

    def __init__(self, health_threshold: float = 0.6, enabled: bool = False):
        self.health_threshold = health_threshold
        self.available = False
        self.actions: deque = deque(maxlen=100)
        if enabled:
            _warn_module("proactive_healing")

    async def evaluate(self, state: Dict[str, Any]) -> List[str]:
        return []


class DriftPlaceholder:
    """STATUS: placeholder. Accepts a central DriftDetector if provided."""

    STATUS = "placeholder"

    def __init__(self, detector: Optional[Any] = None, enabled: bool = False):
        self.detector = detector
        self.available = detector is not None
        self.last_score: float = 0.0
        if enabled and detector is None:
            _warn_module("drift")

    async def check(self, weights: Dict[str, float]) -> float:
        if self.detector is None:
            return 0.0
        try:
            result = await self.detector.check_drift(weights)
            self.last_score = float(result or 0.0)
            return self.last_score
        except Exception as e:
            logger.debug("Drift check failed", error=str(e))
            return 0.0


# =============================================================================
# SECTION 7. SECURITY (EXPERIMENTAL)
# =============================================================================
class SecurityService:
    """
    Signing/verification. Uses Dilithium when available, else SHA-256 HMAC-like
    digest with a per-process secret. Not actually quantum-safe without pqcrypto.
    STATUS: experimental.
    """

    def __init__(self, key_dir: str, enabled: bool = True):
        if enabled:
            _warn_module("security")
        self.key_dir = Path(key_dir)
        self.key_dir.mkdir(parents=True, exist_ok=True)
        self._secret = self._load_or_generate_secret()

    def _load_or_generate_secret(self) -> bytes:
        p = self.key_dir / "hmac_secret.bin"
        if p.exists():
            return p.read_bytes()
        secret = os.urandom(32)
        p.write_bytes(secret)
        return secret

    def sign_data(self, data: Dict[str, Any]) -> str:
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if PQC_AVAILABLE:
            try:
                priv = self._load_or_generate_dilithium()
                return dilithium.sign(payload, priv).hex()
            except Exception:
                pass
        import hmac
        return hmac.new(self._secret, payload, hashlib.sha256).hexdigest()

    def _load_or_generate_dilithium(self) -> bytes:
        p = self.key_dir / "dilithium_private.key"
        if p.exists():
            return p.read_bytes()
        priv, pub = dilithium.generate_keypair()
        p.write_bytes(priv)
        (self.key_dir / "dilithium_public.key").write_bytes(pub)
        return priv

    def verify(self, data: Dict[str, Any], signature: str) -> bool:
        return self.sign_data(data) == signature

    @property
    def backend(self) -> str:
        return "dilithium" if PQC_AVAILABLE else "hmac-sha256"


# =============================================================================
# SECTION 8. AUDIT (EXPERIMENTAL) — bounded, chained, persisted
# =============================================================================
class AuditService:
    """
    Chained, SQLite-backed audit log with bounded in-memory cache.
    STATUS: experimental.
    """

    def __init__(
        self,
        storage: LocalCircuitBreakerStorage,
        security: SecurityService,
        in_memory_max: int = 500,
        enabled: bool = True,
    ):
        if enabled:
            _warn_module("audit")
        self.storage = storage
        self.security = security
        self.in_memory: deque = deque(maxlen=in_memory_max)
        self._lock = asyncio.Lock()

    async def record(
        self, event_type: str, payload: Dict[str, Any], importance: float = 0.5
    ) -> Optional[int]:
        ts = datetime.now(timezone.utc).isoformat()
        signature = self.security.sign_data(payload)
        payload_str = json.dumps(payload, sort_keys=True, default=str)
        prev_hash = None
        # Read latest hash for chain
        try:
            last = self.storage.read_ledger(limit=1)
            if last:
                prev_hash = last[0].get("hash")
        except Exception:
            prev_hash = None
        entry_hash = hashlib.sha256(
            f"{ts}|{event_type}|{payload_str}|{signature}|{prev_hash or ''}".encode()
        ).hexdigest()

        async with self._lock:
            try:
                seq = self.storage.append_ledger(
                    ts, event_type, payload_str, signature, entry_hash, prev_hash
                )
            except Exception as e:
                logger.warning("Audit persist failed", error=str(e))
                seq = None
            self.in_memory.append({
                "seq": seq,
                "timestamp": ts,
                "event_type": event_type,
                "payload": payload,
                "signature": signature,
                "hash": entry_hash,
                "prev_hash": prev_hash,
                "importance": importance,
            })
        return seq

    def recent(self, limit: int = 100) -> List[Dict[str, Any]]:
        return list(self.in_memory)[-limit:]

    def verify_chain(self) -> bool:
        entries = self.storage.read_ledger(limit=10_000)
        # Entries come newest-first; reverse to verify chain
        entries = list(reversed(entries))
        prev = None
        for e in entries:
            expected = hashlib.sha256(
                f"{e['timestamp']}|{e['event_type']}|{e['payload']}|{e['signature']}|{prev or ''}".encode()
            ).hexdigest()
            if expected != e["hash"]:
                return False
            prev = e["hash"]
        return True


# =============================================================================
# SECTION 9. TASK MANAGER (STABLE) — with drain
# =============================================================================
class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.ephemeral: set = set()
        self.shutdown_event = asyncio.Event()
        self._lock: Optional[asyncio.Lock] = None
        self._drained = False

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

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
            logger.warning("No running event loop; task not started", name=name)
            return None
        task = loop.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    def spawn_ephemeral(self, coro) -> Optional[asyncio.Task]:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None
        task = loop.create_task(coro)
        self.ephemeral.add(task)
        task.add_done_callback(self.ephemeral.discard)
        return task

    async def drain(self, timeout: float) -> None:
        if self._drained:
            return
        self._drained = True
        self.shutdown_event.set()
        all_tasks = list(self.tasks.values()) + list(self.ephemeral)
        if not all_tasks:
            return
        done, pending = await asyncio.wait(all_tasks, timeout=timeout)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self.tasks.clear()
        self.ephemeral.clear()
        logger.info("TaskManager drained", completed=len(done), cancelled=len(pending))


# =============================================================================
# SECTION 10. BIO-INTEGRATED AGENT (STABLE ORCHESTRATOR)
# =============================================================================
class BioIntegratedAgent:
    """
    Orchestrator. Lifecycle:

        agent = BioIntegratedAgent(...)
        await agent.start()      # or use async with
        ...
        await agent.shutdown()
    """

    def __init__(
        self,
        config: Optional[Union[AgentConfig, Dict[str, Any]]] = None,
        storage: Optional[Any] = None,
        message_queue: Optional[Any] = None,
        adaptive_cost: Optional[Any] = None,
        pareto_gating: Optional[Any] = None,
        drift_detector: Optional[Any] = None,
        metrics: Optional[Any] = None,
        bio_core: Optional[Any] = None,
    ):
        # Config
        if isinstance(config, dict):
            self.config = AgentConfig(**config) if PYDANTIC_AVAILABLE else AgentConfig(**config)
        elif isinstance(config, AgentConfig):
            self.config = config
        else:
            self.config = AgentConfig() if PYDANTIC_AVAILABLE else AgentConfig()

        # External components
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating
        self.drift_detector = drift_detector
        self.metrics = metrics
        self.bio_core = bio_core

        # Local infrastructure
        self._local_storage = LocalCircuitBreakerStorage(self.config.storage_db_path)
        self.security = SecurityService(self.config.pqc_key_dir)
        self.audit = AuditService(
            self._local_storage, self.security,
            in_memory_max=self.config.audit_ledger_in_memory_max,
        )

        # Circuit breakers
        self._token_circuit = CircuitBreaker(
            "token_service",
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout,
            half_open_attempts=self.config.circuit_breaker_half_open_attempts,
            storage=self._local_storage,
        )
        self._gradient_circuit = CircuitBreaker(
            "gradient_service",
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout,
            half_open_attempts=self.config.circuit_breaker_half_open_attempts,
            storage=self._local_storage,
        )

        # RL selector (experimental)
        self.strategy_selector: Optional[RLStrategySelector] = None
        if self.config.enable_energy_aware_rl or self.config.enable_multi_objective_rl:
            self.strategy_selector = RLStrategySelector(self.config, enabled=True)
            if self.adaptive_cost is not None and self.pareto_gating is not None:
                self.strategy_selector.set_central_components(
                    self.adaptive_cost, self.pareto_gating
                )

        # Placeholders
        self.swarm = SwarmCoordinatorPlaceholder(
            self.config.agent_id,
            enabled=self.config.enable_swarm_coordination,
        )
        self.healing = ProactiveHealingPlaceholder(
            enabled=self.config.enable_proactive_healing,
        )
        self.drift = DriftPlaceholder(
            detector=self.drift_detector,
            enabled=self.config.enable_drift_integration,
        )

        # State
        self.current_strategy: str = "balanced"
        self.state: Dict[str, float] = self._initial_state()
        self.agent_metrics: Dict[str, float] = {
            "strategy_changes": 0,
            "total_reward": 0.0,
            "avg_reward": 0.0,
        }
        self.reward_history: deque = deque(maxlen=100)
        self.correlation_id = uuid.uuid4().hex

        # Lifecycle
        self._task_manager = TaskManager()
        self._started = False
        self._shutdown = False

        # Metrics
        self._prom = self._setup_metrics()

        logger.info(
            "BioIntegratedAgent initialized",
            agent_id=self.config.agent_id,
            correlation_id=self.correlation_id,
            rl_selector=self.strategy_selector is not None,
        )

    # ---------- lifecycle ----------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        # Load state
        await self._load_state()

        # Start swarm (placeholder: no-op)
        await self.swarm.start()

        # Start background tasks
        self._task_manager.start_task(
            "strategy_loop", self._strategy_update_loop
        )
        self._task_manager.start_task(
            "state_save_loop", self._state_save_loop
        )

        logger.info("BioIntegratedAgent started", agent_id=self.config.agent_id)

    async def ready(self) -> bool:
        """Returns True when the agent is started and its core components are ready."""
        if not self._started:
            return False
        if self.strategy_selector is None:
            return False
        # Storage check
        try:
            self._local_storage.ledger_size()
        except Exception:
            return False
        return True

    async def __aenter__(self) -> "BioIntegratedAgent":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        timeout = timeout or float(self.config.shutdown_timeout_seconds)
        logger.info("BioIntegratedAgent shutting down", agent_id=self.config.agent_id)

        # Stop swarm
        try:
            await self.swarm.stop()
        except Exception as e:
            logger.warning("Swarm stop failed", error=str(e))

        # Drain tasks
        try:
            await asyncio.wait_for(self._task_manager.drain(timeout), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Task drain timed out")

        # Flush state
        try:
            await self._save_state()
        except Exception as e:
            logger.warning("State flush failed", error=str(e))

        # Audit shutdown
        try:
            await self.audit.record(
                "shutdown",
                {"agent_id": self.config.agent_id, "correlation_id": self.correlation_id},
                importance=0.4,
            )
        except Exception:
            pass

        logger.info("BioIntegratedAgent shutdown complete", agent_id=self.config.agent_id)

    # ---------- state ----------
    def _initial_state(self) -> Dict[str, float]:
        return {
            "system_load": 0.5,
            "health_score": 0.8,
            "token_balance": 500.0,
            "energy_intensity": 0.5,
            "helium_level": 0.5,
            "carbon_leakage_proxy": 0.3,
            "alert_count": 0,
        }

    async def _refresh_state(self) -> None:
        """
        Pull live metrics from available services. Safe: any missing service
        leaves the corresponding state fields at their current values.
        """
        # Token service via circuit breaker
        if self.queue is not None:
            pass  # queue does not carry state
        token_summary = None
        for provider_name in ("token_manager", "token_service", "token_provider"):
            provider = getattr(self, provider_name, None)
            if provider is None and self.bio_core is not None:
                provider = getattr(self.bio_core, provider_name, None)
            if provider is None:
                continue
            summary_fn = getattr(provider, "get_system_summary", None)
            if summary_fn is None:
                continue
            try:
                async def _call(fn=summary_fn):
                    r = fn()
                    if asyncio.iscoroutine(r):
                        r = await r
                    return r
                token_summary = await self._token_circuit.call(_call)
                break
            except Exception:
                token_summary = None
        if isinstance(token_summary, dict):
            self.state["token_balance"] = float(token_summary.get("total_balance", self.state["token_balance"]))

        # Gradient service
        gradient_strengths = None
        for provider_name in ("gradient_manager", "gradient_service"):
            provider = getattr(self, provider_name, None)
            if provider is None and self.bio_core is not None:
                provider = getattr(self.bio_core, provider_name, None)
            if provider is None:
                continue
            strengths_fn = getattr(provider, "get_field_strengths", None)
            if strengths_fn is None:
                continue
            try:
                async def _call(fn=strengths_fn):
                    r = fn()
                    if asyncio.iscoroutine(r):
                        r = await r
                    return r
                gradient_strengths = await self._gradient_circuit.call(_call)
                break
            except Exception:
                gradient_strengths = None
        if isinstance(gradient_strengths, dict):
            self.state["helium_level"] = float(
                gradient_strengths.get("helium", self.state["helium_level"])
            )
            self.state["carbon_leakage_proxy"] = float(
                1.0 - gradient_strengths.get("carbon", 1.0 - self.state["carbon_leakage_proxy"])
            )

    async def _update_metrics(self, reward: float) -> None:
        self.reward_history.append(reward)
        self.agent_metrics["total_reward"] += reward
        if self.reward_history:
            self.agent_metrics["avg_reward"] = float(np.mean(self.reward_history))
        if self._prom:
            try:
                self._prom["avg_reward"].set(self.agent_metrics["avg_reward"])
                self._prom["strategy_changes"].set(self.agent_metrics["strategy_changes"])
                if self.strategy_selector is not None:
                    self._prom["q_table_size"].set(self.strategy_selector.q_table_size())
                    self._prom["pareto_size"].set(len(self.strategy_selector.pareto_front))
            except Exception:
                pass

    def _objectives_from_state(self, state: Dict[str, float]) -> Dict[str, float]:
        return {
            "energy_efficiency": 1.0 - float(state.get("energy_intensity", 0.5)),
            "helium_sustainability": float(state.get("helium_level", 0.5)),
            "token_balance": min(1.0, float(state.get("token_balance", 0.0)) / 1000.0),
            "health_score": float(state.get("health_score", 0.8)),
            "carbon_leakage": float(state.get("carbon_leakage_proxy", 1.0)),
        }

    async def _compute_reward(self, state: Dict[str, float]) -> float:
        w = self.config.objective_weights
        r = (
            w["energy_efficiency"] * (1.0 - state["energy_intensity"])
            + w["helium_sustainability"] * state["helium_level"]
            + w["token_balance"] * min(1.0, state["token_balance"] / 1000.0)
            + w["health_score"] * state["health_score"]
            + w["carbon_leakage"] * (1.0 - state["carbon_leakage_proxy"])
        )
        return max(0.0, min(1.0, float(r)))

    # ---------- strategy ----------
    async def _strategy_update_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.config.strategy_update_interval)

                # 1. Refresh state from services
                await self._refresh_state()

                # 2. Compute reward and update metrics
                reward = await self._compute_reward(self.state)
                await self._update_metrics(reward)

                # 3. Check drift (placeholder-safe)
                try:
                    drift_score = await self.drift.check(
                        self.adaptive_cost.get_current_weights()
                        if self.adaptive_cost and hasattr(self.adaptive_cost, "get_current_weights")
                        else {}
                    )
                except Exception:
                    drift_score = 0.0

                # 4. RL selector proposes an action
                if self.strategy_selector is None:
                    continue
                action = self.strategy_selector.select_action(self.state)

                # 5. Update Q-table with the reward for the previously-chosen action
                objectives = self._objectives_from_state(self.state)
                self.strategy_selector.update(
                    state=self.state,
                    action=self.current_strategy if self.current_strategy in self.strategy_selector.actions else action,
                    reward=reward,
                    next_state=self.state,
                    objectives=objectives,
                )

                # 6. Apply if changed
                if action != self.current_strategy:
                    old = self.current_strategy
                    self.current_strategy = action
                    self.agent_metrics["strategy_changes"] += 1
                    await self._apply_strategy(action)
                    await self.audit.record(
                        "strategy_change",
                        {"old": old, "new": action, "reward": reward, "drift": drift_score},
                        importance=0.7,
                    )
                    await self._publish_feedback_event(old, action, reward)

                # 7. Proactive healing (placeholder: no-op)
                try:
                    await self.healing.evaluate(self.state)
                except Exception:
                    pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Strategy loop error", error=str(e))
                await asyncio.sleep(5)

    async def _apply_strategy(self, strategy: str) -> None:
        """Apply strategy to available services. Missing methods are no-ops."""
        policy = {
            "conservative": {"rate": 0.5},
            "balanced": {"rate": 1.0},
            "performance": {"rate": 2.0},
        }.get(strategy, {"rate": 1.0})

        # Try known providers; only call methods that exist
        for provider_name in ("token_manager", "token_service", "token_provider"):
            provider = getattr(self, provider_name, None)
            if provider is None and self.bio_core is not None:
                provider = getattr(self.bio_core, provider_name, None)
            if provider is None:
                continue
            fn = getattr(provider, "set_generation_rate", None)
            if fn is None:
                continue
            try:
                r = fn(policy["rate"])
                if asyncio.iscoroutine(r):
                    await r
                break
            except Exception as e:
                logger.debug("set_generation_rate failed", error=str(e))

        # Log application regardless
        logger.info("Strategy applied", strategy=strategy, agent_id=self.config.agent_id)

    async def _publish_feedback_event(self, old: str, new: str, reward: float) -> None:
        if self.queue is None:
            return
        payload = {
            "source": "bio_agent",
            "feedback_type": "routing",
            "task_id": self.config.agent_id,
            "selected_action": new,
            "previous_action": old,
            "reward": reward,
            "correlation_id": self.correlation_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            if FeedbackEvent is not None and hasattr(FeedbackEvent, "create_with_context"):
                event = FeedbackEvent.create_with_context(
                    task_id=self.config.agent_id,
                    selected_action=new,
                    quality_score=reward,
                    energy_joules=0.0,
                    carbon_g=0.0,
                    feedback_type="routing",
                    adaptive_cost_value=reward,
                    state={"old_strategy": old, "new_strategy": new, "state": self.state},
                    candidates=[{"action": s} for s in self.config.rl_strategies],
                    source="bio_agent",
                    environment="production",
                    tags=["bio_agent", "strategy"],
                )
                await self.queue.publish("bio_agent_events", event.to_json())
            else:
                await self.queue.publish("bio_agent_events", json.dumps(payload, default=str))
        except Exception as e:
            logger.warning("Feedback publish failed", error=str(e))

    # ---------- persistence ----------
    async def _save_state(self) -> None:
        data = {
            "_v": 1,
            "agent_id": self.config.agent_id,
            "correlation_id": self.correlation_id,
            "current_strategy": self.current_strategy,
            "state": self.state,
            "agent_metrics": self.agent_metrics,
            "q_table_size": self.strategy_selector.q_table_size() if self.strategy_selector else 0,
            "pareto_size": len(self.strategy_selector.pareto_front) if self.strategy_selector else 0,
        }
        try:
            with open(self.config.state_save_path, "w") as f:
                json.dump(data, f, default=str, indent=2)
        except Exception as e:
            logger.warning("State save failed", error=str(e))

    async def _load_state(self) -> None:
        path = self.config.state_save_path
        if not os.path.exists(path):
            return
        try:
            with open(path, "r") as f:
                data = json.load(f)
            if data.get("_v") != 1:
                logger.warning("Unknown state version; ignoring")
                return
            self.current_strategy = data.get("current_strategy", self.current_strategy)
            self.state = {**self._initial_state(), **data.get("state", {})}
            self.agent_metrics.update(data.get("agent_metrics", {}))
        except Exception as e:
            logger.warning("State load failed", error=str(e))

    async def _state_save_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.config.state_save_interval)
                await self._save_state()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("State save loop error", error=str(e))
                await asyncio.sleep(10)

    # ---------- public API ----------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """Teacher policy over strategies. Always returns a valid probability vector."""
        strategies = self.config.rl_strategies
        n = len(strategies)
        uniform = [1.0 / n] * n
        if self.strategy_selector is None:
            return uniform

        # Try adaptive cost + pareto
        if self.adaptive_cost is not None and self.pareto_gating is not None:
            try:
                objectives = self._objectives_from_state(state)
                costs = []
                for s in strategies:
                    c = self.adaptive_cost.compute(
                        quality=objectives["energy_efficiency"],
                        carbon_g=objectives["carbon_leakage"] * 1000.0,
                        latency_ms=0.0,
                        energy_joules=0.0,
                        health=objectives["health_score"],
                        atp=objectives["token_balance"],
                    )
                    costs.append(float(c))
                # Convert costs to probabilities (lower cost = higher prob)
                costs_arr = np.array(costs)
                exp = np.exp(-(costs_arr - costs_arr.min()))
                probs = exp / exp.sum()
                return probs.tolist()
            except Exception:
                pass

        # Fallback: softmax over Q-values
        key = self.strategy_selector._state_to_key(state)
        q = self.strategy_selector.q_table.get(key)
        if q:
            arr = np.array([q.get(s, 0.0) for s in strategies])
            exp = np.exp(arr - arr.max())
            return (exp / exp.sum()).tolist()
        return uniform

    def get_status(self) -> Dict[str, Any]:
        return {
            "agent_id": self.config.agent_id,
            "correlation_id": self.correlation_id,
            "started": self._started,
            "shutdown": self._shutdown,
            "current_strategy": self.current_strategy,
            "metrics": dict(self.agent_metrics),
            "rl_selector": {
                "enabled": self.strategy_selector is not None,
                "q_table_size": self.strategy_selector.q_table_size() if self.strategy_selector else 0,
                "pareto_size": len(self.strategy_selector.pareto_front) if self.strategy_selector else 0,
            },
            "swarm": {"available": self.swarm.available, "peers": len(self.swarm.peers)},
            "healing": {"available": self.healing.available},
            "drift": {"available": self.drift.available, "last_score": self.drift.last_score},
            "audit": {"ledger_size": self._local_storage.ledger_size()},
            "security_backend": self.security.backend,
            "module_status": MODULE_STATUS,
            "circuit_breakers": {
                "token": self._token_circuit.snapshot(),
                "gradient": self._gradient_circuit.snapshot(),
            },
        }

    # ---------- metrics ----------
    def _setup_metrics(self) -> Dict[str, Any]:
        if not self.config.enable_prometheus or not PROMETHEUS_AVAILABLE:
            return {}
        try:
            return {
                "avg_reward": Gauge("bio_agent_avg_reward", "Rolling average reward"),
                "strategy_changes": Gauge("bio_agent_strategy_changes", "Strategy changes"),
                "q_table_size": Gauge("bio_agent_q_table_size", "Q-table size"),
                "pareto_size": Gauge("bio_agent_pareto_size", "Pareto front size"),
            }
        except Exception:
            return {}


# =============================================================================
# SECTION 11. TESTS
# =============================================================================
class _Tests(unittest.TestCase):
    def _tmp_config(self) -> Dict[str, Any]:
        import tempfile
        td = tempfile.mkdtemp()
        return {
            "state_save_path": os.path.join(td, "state.json"),
            "storage_db_path": os.path.join(td, "storage.db"),
            "pqc_key_dir": os.path.join(td, "keys"),
            "strategy_update_interval": 0.2,
            "state_save_interval": 0.5,
            "enable_prometheus": False,
        }

    def test_start_shutdown_roundtrip(self):
        async def go():
            agent = BioIntegratedAgent(config=self._tmp_config())
            self.assertFalse(await agent.ready())
            await agent.start()
            self.assertTrue(await agent.ready())
            await asyncio.sleep(0.5)
            await agent.shutdown()
            self.assertTrue(agent._shutdown)
        asyncio.run(go())

    def test_async_context_manager(self):
        async def go():
            async with BioIntegratedAgent(config=self._tmp_config()) as agent:
                self.assertTrue(await agent.ready())
        asyncio.run(go())

    def test_q_table_bounded(self):
        cfg = self._tmp_config()
        cfg["q_table_max_size"] = 50
        agent = BioIntegratedAgent(config=cfg)
        sel = agent.strategy_selector
        self.assertIsNotNone(sel)
        for i in range(500):
            state = {
                "system_load": (i % 10) / 10.0,
                "health_score": ((i * 7) % 10) / 10.0,
                "token_balance": float(i * 100),
                "energy_intensity": ((i * 3) % 10) / 10.0,
                "helium_level": ((i * 5) % 10) / 10.0,
                "carbon_leakage_proxy": ((i * 11) % 10) / 10.0,
                "alert_count": i % 5,
            }
            sel.select_action(state)
        self.assertLessEqual(sel.q_table_size(), cfg["q_table_max_size"])

    def test_pareto_filter(self):
        pts = [
            MOPDPoint("a", 1.0, 1.0, 1.0, 1.0, 0.1),
            MOPDPoint("b", 0.5, 0.5, 0.5, 0.5, 0.5),  # dominated
            MOPDPoint("c", 1.0, 1.0, 1.0, 1.0, 0.5),  # dominated
        ]
        front = pareto_filter(pts)
        self.assertEqual([p.strategy for p in front], ["a"])

    def test_policy_probs_valid(self):
        async def go():
            agent = BioIntegratedAgent(config=self._tmp_config())
            try:
                state = agent._initial_state()
                probs = await agent.policy_probs(state)
                self.assertEqual(len(probs), len(agent.config.rl_strategies))
                self.assertTrue(all(p >= 0 for p in probs))
                self.assertAlmostEqual(sum(probs), 1.0, places=6)
            finally:
                await agent.shutdown()
        asyncio.run(go())

    def test_audit_chain(self):
        import tempfile
        async def go():
            td = tempfile.mkdtemp()
            storage = LocalCircuitBreakerStorage(os.path.join(td, "s.db"))
            security = SecurityService(os.path.join(td, "k"))
            audit = AuditService(storage, security, in_memory_max=10)
            for i in range(5):
                await audit.record("test", {"i": i}, importance=0.5)
            self.assertTrue(audit.verify_chain())
            self.assertEqual(len(audit.recent()), 5)
        asyncio.run(go())

    def test_placeholders(self):
        swarm = SwarmCoordinatorPlaceholder("a", enabled=False)
        self.assertFalse(swarm.available)
        healing = ProactiveHealingPlaceholder(enabled=False)
        self.assertFalse(healing.available)
        drift = DriftPlaceholder(enabled=False)
        self.assertFalse(drift.available)

        async def go():
            await swarm.start()
            await swarm.share({"x": 1})
            await swarm.stop()
            self.assertEqual(await healing.evaluate({}), [])
            self.assertEqual(await drift.check({}), 0.0)
        asyncio.run(go())

    def test_circuit_breaker_transitions(self):
        import tempfile
        async def go():
            td = tempfile.mkdtemp()
            storage = LocalCircuitBreakerStorage(os.path.join(td, "cb.db"))
            cb = CircuitBreaker(
                "t", failure_threshold=2, recovery_timeout=0.5,
                half_open_attempts=1, storage=storage,
            )

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

    def test_idempotent_shutdown(self):
        async def go():
            agent = BioIntegratedAgent(config=self._tmp_config())
            await agent.start()
            await agent.shutdown()
            await agent.shutdown()
        asyncio.run(go())


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 12. ENTRY POINT
# =============================================================================
async def _example() -> None:
    config = {
        "state_save_path": "./agent_state.json",
        "storage_db_path": "./agent_storage.db",
        "pqc_key_dir": "./pqc_keys",
    }
    async with BioIntegratedAgent(config=config) as agent:
        await asyncio.sleep(2)
        print(json.dumps(agent.get_status(), indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="Bio-Integrated Green Agent v13.0.0")
    parser.add_argument("--test", action="store_true", help="Run embedded tests")
    parser.add_argument("--example", action="store_true", help="Run example")
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

    print("Bio-Integrated Green Agent v13.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
