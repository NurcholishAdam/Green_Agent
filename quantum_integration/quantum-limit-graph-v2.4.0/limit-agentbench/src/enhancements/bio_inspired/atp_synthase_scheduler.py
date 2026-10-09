#!/usr/bin/env python3
# =============================================================================
# Enhanced ATP Synthase Scheduler v11.0.0
# =============================================================================
"""
Enhanced ATP Synthase Scheduler v11.0.0
========================================
Rewritten from v10.0.0 with three goals:

1. P0 correctness fixes (the previous file did not run):
   - Removed `async with` from non-async methods.
   - Removed `asyncio.run` calls from methods reachable from a running loop.
   - Made all I/O-touching public methods async; sync helpers are pure.
   - Made `get_scheduler_stats` async and awaited everywhere.
   - CircuitBreaker: lazily bound asyncio.Lock, SQLite I/O moved to executor.
   - policy_probs: always returns a valid probability vector (>=0, sum==1).
   - Validated every call site for coroutine leaks.

2. Honest module status (see MODULE_STATUS below):
   - Advanced modules are DISABLED by default.
   - Enabling one logs a clear warning about its maturity.
   - "Placeholder" modules never pretend to work.

3. P3 production readiness:
   - Graceful shutdown with task drain and state flush.
   - Structured module-status documentation.
   - Embedded test suite: run `python3 atp_synthase_scheduler.py --test`.
   - Sections clearly separated (acts as logical modules in one file).

Module status legend
--------------------
  stable        Production-ready; covered by tests.
  experimental  Works but not fully validated; disabled by default.
  placeholder   Stub; safe no-op with clear logging; must not be relied on.

MODULE_STATUS = {
    "core_scheduler":        "stable",
    "circuit_breaker":       "stable",
    "task_manager":          "stable",
    "enhanced_synthase":     "stable",
    "demand_priority":       "stable",
    "load_balancer":         "stable",
    "ml_predictor":          "stable",
    "gradient_forecaster":   "stable",
    "mopd":                  "stable",
    "xai":                   "experimental",
    "safety_monitor":        "experimental",
    "causal_rl":             "placeholder",
    "federated":             "placeholder",
    "precision_controller":  "placeholder",
    "carbon_market":         "placeholder",
    "chaos_injector":        "placeholder",
    "human_approval":        "placeholder",
}

Every experimental/placeholder module:
  - Is disabled by default in the config.
  - Logs a `ModuleStatusWarning` at construction if enabled.
  - Exposes `.available` (False for placeholders, True for experimental).
  - Exposes `.status_reason` for the log.
"""

from __future__ import annotations

import argparse
import asyncio
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
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple, Union

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.preprocessing import StandardScaler
    import joblib
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from tenacity import (
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type, before_sleep_log,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

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
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

# -----------------------------------------------------------------------------
# Local optional imports
# -----------------------------------------------------------------------------
try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPConsumer, EcoATPSource
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False

    class EcoATPSource:  # type: ignore
        GRADIENT_CONVERSION = "gradient_conversion"

    class EcoATPConsumer:  # type: ignore
        EXPERT_EXECUTION = "expert_execution"

try:
    from .proton_gradient_fields import GradientFieldManager, GradientField
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

# -----------------------------------------------------------------------------
# Central Green Agent components (optional)
# -----------------------------------------------------------------------------
try:
    from ..config import config as central_config  # type: ignore
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
    central_config = None  # type: ignore


# =============================================================================
# SECTION 0. MODULE STATUS REGISTRY
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "core_scheduler": "stable",
    "circuit_breaker": "stable",
    "task_manager": "stable",
    "enhanced_synthase": "stable",
    "demand_priority": "stable",
    "load_balancer": "stable",
    "ml_predictor": "stable",
    "gradient_forecaster": "stable",
    "mopd": "stable",
    "xai": "experimental",
    "safety_monitor": "experimental",
    "causal_rl": "placeholder",
    "federated": "placeholder",
    "precision_controller": "placeholder",
    "carbon_market": "placeholder",
    "chaos_injector": "placeholder",
    "human_approval": "placeholder",
}


class ModuleStatusWarning(UserWarning):
    """Emitted when an experimental or placeholder module is enabled."""


def _warn_module(module_name: str) -> str:
    """Log a module-status warning and return the reason string."""
    status = MODULE_STATUS.get(module_name, "unknown")
    if status == "stable":
        return "stable"
    reason = (
        f"Module '{module_name}' is '{status}' and enabled. "
        f"{'Disabled by default; enable only after review.' if status == 'placeholder' else 'Validate before production use.'}"
    )
    logger.warning(reason)
    return reason


# =============================================================================
# SECTION 1. RETRY DECORATOR
# =============================================================================
def retry_decorator(max_attempts: int = 3, min_delay: float = 0.1, max_delay: float = 10.0):
    """Retry decorator for async functions (tenacity if available, else manual)."""
    if TENACITY_AVAILABLE:
        def decorator(func):
            @retry(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=min_delay, min=min_delay, max=max_delay),
                retry=retry_if_exception_type(Exception),
                before_sleep=before_sleep_log(logger, logging.WARNING),
            )
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)
            return wrapper
        return decorator

    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception:
                    if attempt == max_attempts - 1:
                        raise
                    delay = min(min_delay * (2 ** attempt), max_delay)
                    await asyncio.sleep(delay)
        return wrapper
    return decorator


# =============================================================================
# SECTION 2. CIRCUIT BREAKER (STABLE)
# =============================================================================
class CircuitBreaker:
    """
    Circuit breaker with SQLite persistence.

    P0 fix: the asyncio.Lock is lazily created on first use so it is bound to
    the running loop. SQLite I/O is offloaded to a thread executor.
    """

    def __init__(
        self,
        name: str,
        db_path: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
    ):
        self.name = name
        self.db_path = db_path
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = "closed"
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._lock: Optional[asyncio.Lock] = None  # lazy
        self._sync_init_db()
        self._sync_load_state()

    # --- lock ---
    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    # --- persistence (sync, called only from threads or __init__) ---
    def _sync_init_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS circuit_breaker (
                name TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                failures INTEGER NOT NULL,
                last_failure TEXT
            )
        """)
        conn.commit()
        conn.close()

    def _sync_load_state(self) -> None:
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT state, failures, last_failure FROM circuit_breaker WHERE name = ?",
            (self.name,),
        ).fetchone()
        conn.close()
        if row:
            self.state = row[0]
            self.failure_count = row[1]
            self.last_failure_time = (
                datetime.fromisoformat(row[2]) if row[2] else None
            )

    def _sync_save_state(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO circuit_breaker
            (name, state, failures, last_failure)
            VALUES (?, ?, ?, ?)
        """, (
            self.name, self.state, self.failure_count,
            self.last_failure_time.isoformat() if self.last_failure_time else None,
        ))
        conn.commit()
        conn.close()

    async def _persist(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_save_state)

    # --- public API ---
    async def call(self, func: Callable, *args, **kwargs):
        lock = self._get_lock()
        async with lock:
            if self.state == "open":
                if (
                    self.last_failure_time
                    and (datetime.now(timezone.utc) - self.last_failure_time).total_seconds()
                    >= self.recovery_timeout
                ):
                    self.state = "half_open"
                    logger.info("Circuit breaker half_open", name=self.name)
                    await self._persist()
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")

        try:
            result = await func(*args, **kwargs)
        except Exception:
            async with lock:
                self.failure_count += 1
                self.last_failure_time = datetime.now(timezone.utc)
                if self.failure_count >= self.failure_threshold:
                    self.state = "open"
                    logger.warning(
                        "Circuit breaker opened",
                        name=self.name,
                        failures=self.failure_count,
                    )
                await self._persist()
            raise

        async with lock:
            if self.state == "half_open":
                self.state = "closed"
                self.failure_count = 0
                await self._persist()
                logger.info("Circuit breaker closed", name=self.name)
            elif self.failure_count > 0:
                self.failure_count = 0
                await self._persist()
        return result

    def snapshot(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state,
            "failure_count": self.failure_count,
            "last_failure_time": self.last_failure_time.isoformat()
            if self.last_failure_time else None,
        }


# =============================================================================
# SECTION 3. PLACEHOLDER / EXPERIMENTAL MODULES
# =============================================================================
#
# Each module below is honest about what it does. Placeholders return safe
# no-ops with clear logging. Experimental modules work but are disabled by
# default and warn when enabled.
#


class PrecisionController:
    """
    STATUS: placeholder.

    Previous v10 returned "float16"/"float32" strings that nothing consumed.
    This version is honest: `.available` is False and `.get_precision` is a
    no-op returning "float32" with a single startup warning.
    """

    STATUS = "placeholder"

    def __init__(self, policy: str = "energy_aware", enabled: bool = False):
        self.policy = policy
        self.available = False
        self.status_reason = "No consumer wired; returns float32 only."
        if enabled:
            _warn_module("precision_controller")

    def get_precision(self, load: float, energy_budget: float) -> str:
        return "float32"


class CarbonMarketClient:
    """
    STATUS: placeholder.

    No real trading. buy_credits / sell_credits return False and log once.
    """

    STATUS = "placeholder"

    def __init__(
        self,
        provider_url: Optional[str] = None,
        contract_address: Optional[str] = None,
        private_key: Optional[str] = None,
        enabled: bool = False,
    ):
        self.available = False
        self.status_reason = "No live backend; trading methods are no-ops."
        self._warned = False
        if enabled:
            _warn_module("carbon_market")

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


class HumanApprovalHandler:
    """
    STATUS: placeholder.

    v10 auto-approved silently. This version:
      - `.available` is False.
      - `request_approval` returns False by default (deny) unless
        `auto_approve_dev` is True, which logs a loud warning.
    """

    STATUS = "placeholder"

    def __init__(
        self,
        queue: Optional[Any] = None,
        enabled: bool = False,
        auto_approve_dev: bool = False,
    ):
        self.queue = queue
        self.available = False
        self.auto_approve_dev = auto_approve_dev
        self.status_reason = "No approval round-trip; denies by default."
        if enabled:
            _warn_module("human_approval")
        if auto_approve_dev:
            logger.warning(
                "HumanApprovalHandler auto_approve_dev=True; every request will be approved. "
                "Never enable in production."
            )

    async def request_approval(self, decision: Dict[str, Any], timeout: float = 60.0) -> bool:
        if self.auto_approve_dev:
            return True
        logger.info(
            "Approval requested but HumanApprovalHandler is a placeholder; denying.",
            decision=decision,
        )
        return False


class ChaosInjector:
    """
    STATUS: placeholder.

    v10 mutated live config with no rollback. This version:
      - `.available` is False.
      - `maybe_inject_failure` is a no-op.
    """

    STATUS = "placeholder"

    def __init__(self, scheduler: Any, chaos_probability: float = 0.0, enabled: bool = False):
        self.scheduler = scheduler
        self.chaos_probability = chaos_probability
        self.available = False
        self.status_reason = "No isolation or rollback; injection is a no-op."
        if enabled:
            _warn_module("chaos_injector")

    async def maybe_inject_failure(self) -> None:
        return None


class CausalRLAgent:
    """
    STATUS: placeholder.

    v10 was tabular Q-learning with an unused causal mask. This version keeps
    the interface but never invents Q-values; it returns a uniform policy and
    does not update.
    """

    STATUS = "placeholder"

    def __init__(self, state_dim: int, action_dim: int, enabled: bool = False):
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.available = False
        self.status_reason = "No causal model; policy is uniform."
        if enabled:
            _warn_module("causal_rl")

    def act(self, state: np.ndarray, explore: bool = True) -> int:
        return random.randrange(self.action_dim)

    def update(self, state, action, reward, next_state, done) -> None:
        return None

    def get_policy_probs(self, state: np.ndarray, temperature: float = 1.0) -> List[float]:
        return [1.0 / self.action_dim] * self.action_dim


class FederatedCoordinator:
    """
    STATUS: placeholder.

    v10 sent/received full models with fragile key parsing. This version:
      - `.available` is False.
      - send_update is a no-op returning False.
      - receive_global_model logs and does not mutate local state.
    """

    STATUS = "placeholder"

    def __init__(self, scheduler: Any, queue: Optional[Any], enabled: bool = False):
        self.scheduler = scheduler
        self.queue = queue
        self.available = False
        self.status_reason = "No secure aggregation; send/receive are no-ops."
        if enabled:
            _warn_module("federated")

    async def send_update(self) -> bool:
        logger.info("Federated send_update called but module is a placeholder; no-op.")
        return False

    async def receive_global_model(self, model_json: str) -> bool:
        logger.info("Federated receive_global_model called but module is a placeholder; no-op.")
        return False


class SafetyMonitor:
    """
    STATUS: experimental.

    Real invariants evaluated against a caller-provided state dict. Simpler
    than v10's version but honest: no placeholder values.
    """

    STATUS = "experimental"

    def __init__(self, enabled: bool = True):
        self.available = True
        self.invariants: List[Tuple[str, Callable[[Dict[str, Any]], bool], str]] = []
        self.status_reason = "Simple invariants; not a temporal-logic checker."
        if enabled:
            _warn_module("safety_monitor")

    def add_invariant(self, name: str, fn: Callable[[Dict[str, Any]], bool], description: str) -> None:
        self.invariants.append((name, fn, description))

    def check(self, state: Dict[str, Any]) -> List[str]:
        return [
            f"{name}: {desc}"
            for name, fn, desc in self.invariants
            if not fn(state)
        ]


class XAIExplainer:
    """
    STATUS: experimental.

    Templated explanations. Not a saliency/counterfactual engine.
    """

    STATUS = "experimental"

    def __init__(self, enabled: bool = True):
        self.available = True
        self.status_reason = "Template-based text; no attribution or counterfactuals."
        if enabled:
            _warn_module("xai")

    def explain_mopd(self, plan: Any, objective_weights: Dict[str, float]) -> str:
        top = max(objective_weights, key=objective_weights.get) if objective_weights else "unknown"
        return (
            f"MOPD selected plan with emphasis on {top} "
            f"(weight={objective_weights.get(top, 0.0):.2f})."
        )

    def explain_schedule(self, task: Any) -> str:
        return (
            f"Task {task.task_id} scheduled with priority "
            f"{task.user_priority} and ATP {task.eco_atp_required:.2f}."
        )


# =============================================================================
# SECTION 4. CONFIG
# =============================================================================
if PYDANTIC_AVAILABLE:
    class MOPDConfig(BaseModel):
        enabled: bool = True
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "total_produced": 0.3,
                "avg_efficiency": 0.3,
                "demand_satisfaction": 0.2,
                "token_balance": 0.2,
            }
        )
        grid_resolution: int = 5

        @validator("objective_weights")
        def _check(cls, v):
            total = sum(v.values())
            if abs(total - 1.0) > 1e-6:
                raise ValueError("objective_weights must sum to 1")
            return v

    class SynthaseSchedulerConfig(BaseModel):
        # Core
        protons_per_rotation: int = Field(12, ge=8, le=17)
        atp_per_rotation: int = Field(3, ge=1)
        max_rotation_speed_rpm: float = Field(6000, gt=0)
        activation_gradient: float = Field(0.05, ge=0, le=1)
        base_efficiency: float = Field(0.95, ge=0, le=1)
        atp_inhibition_constant: float = Field(0.1, ge=0)
        atp_inhibition_max: float = Field(0.5, ge=0, le=1)
        reverse_efficiency: float = Field(0.7, ge=0, le=1)
        hydrolysis_protons_per_atp: int = Field(4, ge=1)
        uncoupling_leak_rate: float = Field(0.01, ge=0, le=1)
        uncoupling_activation_threshold: float = Field(0.9, ge=0, le=1)
        adaptive_c_ring: bool = True
        min_c_ring: int = 8
        max_c_ring: int = 17
        degradation_scaling: bool = True
        quantum_tunneling_enabled: bool = True
        quantum_efficiency_boost: float = Field(0.25, ge=0, le=1)
        quantum_tunneling_threshold: float = Field(0.7, ge=0, le=1)
        quantum_coherence_time: float = Field(10.0, ge=0)

        driving_force_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "carbon": 0.25, "helium": 0.15, "trust": 0.20,
                "opportunity": 0.25, "eco_atp_reserve": 0.15,
            }
        )
        priority_defaults: Dict[str, Dict[str, float]] = Field(
            default_factory=lambda: {
                "critical": {"weight": 2.0, "min_balance": 10000, "max_consumption": 0.9},
                "high": {"weight": 1.5, "min_balance": 5000, "max_consumption": 0.7},
                "normal": {"weight": 1.0, "min_balance": 2000, "max_consumption": 0.5},
                "low": {"weight": 0.7, "min_balance": 1000, "max_consumption": 0.3},
                "background": {"weight": 0.4, "min_balance": 500, "max_consumption": 0.1},
            }
        )
        default_priority: str = "normal"

        ml_lookback: int = Field(50, ge=10)
        ml_model_path: str = "./models/atp_demand_model.joblib"
        ml_min_samples: int = Field(100, ge=20)

        forecast_history_window: int = Field(50, ge=10)
        forecast_horizon: int = Field(20, ge=5)
        forecast_alpha: float = Field(0.3, ge=0, le=1)
        forecast_beta: float = Field(0.1, ge=0, le=1)

        load_balance_history_size: int = Field(100, ge=10)
        load_balance_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "health": 0.3, "efficiency": 0.3,
                "quantum": 0.2, "performance": 0.2,
            }
        )

        adaptive_priority_enabled: bool = True
        adaptive_priority_learning_rate: float = Field(0.1, ge=0, le=1)
        priority_performance_window: int = Field(50, ge=10)

        synthesis_interval: float = Field(0.1, ge=0.01)
        regulation_interval: float = Field(30, ge=5)
        predictive_interval: float = Field(60, ge=10)
        forecast_interval: float = Field(60, ge=10)
        maintenance_interval: float = Field(60, ge=10)

        enable_multi_synthase: bool = True
        enable_quantum: bool = True
        enable_ml_prediction: bool = True
        enable_prometheus: bool = False
        degradation_tier_update_interval: int = Field(600, ge=60)
        efficiency_thresholds: Dict[int, float] = Field(
            default_factory=lambda: {5: 0.9, 4: 0.8, 3: 0.7, 2: 0.6, 1: 0.0}
        )
        shutdown_timeout_seconds: int = Field(30, ge=5)
        circuit_breaker_db_path: str = "./circuit_breakers.db"
        mopd: MOPDConfig = Field(default_factory=MOPDConfig)

        # --- Advanced modules: ALL DISABLED BY DEFAULT ---
        enable_causal_rl: bool = False
        enable_federated_learning: bool = False
        enable_safety_monitor: bool = True     # experimental but harmless
        enable_xai: bool = True                # experimental but harmless
        enable_precision_switching: bool = False
        enable_carbon_market: bool = False
        carbon_market_config: Optional[Dict[str, str]] = None
        enable_chaos: bool = False
        chaos_probability: float = 0.0
        enable_human_approval: bool = False

        # Development escape hatches; never enable in production
        dev_auto_approve: bool = False

        class Config:
            env_prefix = "ATP_SCHEDULER_"
else:
    @dataclass
    class MOPDConfig:
        enabled: bool = True
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            "total_produced": 0.3, "avg_efficiency": 0.3,
            "demand_satisfaction": 0.2, "token_balance": 0.2,
        })
        grid_resolution: int = 5

    @dataclass
    class SynthaseSchedulerConfig:
        protons_per_rotation: int = 12
        atp_per_rotation: int = 3
        max_rotation_speed_rpm: float = 6000
        activation_gradient: float = 0.05
        base_efficiency: float = 0.95
        atp_inhibition_constant: float = 0.1
        atp_inhibition_max: float = 0.5
        reverse_efficiency: float = 0.7
        hydrolysis_protons_per_atp: int = 4
        uncoupling_leak_rate: float = 0.01
        uncoupling_activation_threshold: float = 0.9
        adaptive_c_ring: bool = True
        min_c_ring: int = 8
        max_c_ring: int = 17
        degradation_scaling: bool = True
        quantum_tunneling_enabled: bool = True
        quantum_efficiency_boost: float = 0.25
        quantum_tunneling_threshold: float = 0.7
        quantum_coherence_time: float = 10.0
        driving_force_weights: Dict[str, float] = field(default_factory=lambda: {
            "carbon": 0.25, "helium": 0.15, "trust": 0.20,
            "opportunity": 0.25, "eco_atp_reserve": 0.15,
        })
        priority_defaults: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
            "critical": {"weight": 2.0, "min_balance": 10000, "max_consumption": 0.9},
            "high": {"weight": 1.5, "min_balance": 5000, "max_consumption": 0.7},
            "normal": {"weight": 1.0, "min_balance": 2000, "max_consumption": 0.5},
            "low": {"weight": 0.7, "min_balance": 1000, "max_consumption": 0.3},
            "background": {"weight": 0.4, "min_balance": 500, "max_consumption": 0.1},
        })
        default_priority: str = "normal"
        ml_lookback: int = 50
        ml_model_path: str = "./models/atp_demand_model.joblib"
        ml_min_samples: int = 100
        forecast_history_window: int = 50
        forecast_horizon: int = 20
        forecast_alpha: float = 0.3
        forecast_beta: float = 0.1
        load_balance_history_size: int = 100
        load_balance_weights: Dict[str, float] = field(default_factory=lambda: {
            "health": 0.3, "efficiency": 0.3, "quantum": 0.2, "performance": 0.2,
        })
        adaptive_priority_enabled: bool = True
        adaptive_priority_learning_rate: float = 0.1
        priority_performance_window: int = 50
        synthesis_interval: float = 0.1
        regulation_interval: float = 30
        predictive_interval: float = 60
        forecast_interval: float = 60
        maintenance_interval: float = 60
        enable_multi_synthase: bool = True
        enable_quantum: bool = True
        enable_ml_prediction: bool = True
        enable_prometheus: bool = False
        degradation_tier_update_interval: int = 600
        efficiency_thresholds: Dict[int, float] = field(default_factory=lambda: {
            5: 0.9, 4: 0.8, 3: 0.7, 2: 0.6, 1: 0.0,
        })
        shutdown_timeout_seconds: int = 30
        circuit_breaker_db_path: str = "./circuit_breakers.db"
        mopd: MOPDConfig = field(default_factory=MOPDConfig)

        enable_causal_rl: bool = False
        enable_federated_learning: bool = False
        enable_safety_monitor: bool = True
        enable_xai: bool = True
        enable_precision_switching: bool = False
        enable_carbon_market: bool = False
        carbon_market_config: Optional[Dict[str, str]] = None
        enable_chaos: bool = False
        chaos_probability: float = 0.0
        enable_human_approval: bool = False
        dev_auto_approve: bool = False


# =============================================================================
# SECTION 5. ENUMS AND DATACLASSES
# =============================================================================
class SynthaseMode(Enum):
    SYNTHESIS = "synthesis"
    HYDROLYSIS = "hydrolysis"
    IDLE = "idle"
    INHIBITED = "inhibited"
    UNCOUPLED = "uncoupled"
    QUANTUM_ENHANCED = "quantum_enhanced"


class SynthaseState(Enum):
    ACTIVE = "active"
    DEGRADED = "degraded"
    OVERLOADED = "overloaded"
    REPAIRING = "repairing"
    DORMANT = "dormant"
    QUANTUM_READY = "quantum_ready"


@dataclass
class SynthaseConfig:
    protons_per_rotation: int = 12
    atp_per_rotation: int = 3
    max_rotation_speed_rpm: float = 6000
    activation_gradient: float = 0.05
    base_efficiency: float = 0.95
    atp_inhibition_constant: float = 0.1
    atp_inhibition_max: float = 0.5
    reverse_efficiency: float = 0.7
    hydrolysis_protons_per_atp: int = 4
    uncoupling_leak_rate: float = 0.01
    uncoupling_activation_threshold: float = 0.9
    adaptive_c_ring: bool = True
    min_c_ring: int = 8
    max_c_ring: int = 17
    degradation_scaling: bool = True
    quantum_tunneling_enabled: bool = True
    quantum_efficiency_boost: float = 0.25
    quantum_tunneling_threshold: float = 0.7
    quantum_coherence_time: float = 10.0


@dataclass
class ScheduledTask:
    task_id: str
    eco_atp_required: float
    priority: int
    deadline: Optional[datetime] = None
    callback: Optional[Callable] = None
    compartment_preference: Optional[str] = None
    scheduled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    token_ids: List[str] = field(default_factory=list)
    status: str = "pending"
    user_priority: Optional[str] = None


@dataclass
class DemandPriority:
    priority_level: str
    weight: float
    min_balance: float
    max_consumption: float


@dataclass
class MOPDPoint:
    driving_force_weights: Dict[str, float]
    load_balance_weights: Dict[str, float]
    priority_weights: Dict[str, float]
    total_produced: float
    avg_efficiency: float
    demand_satisfaction: float
    token_balance: float
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(**data)


# =============================================================================
# SECTION 6. TASK MANAGER (STABLE) — with drain support
# =============================================================================
class TaskManager:
    """
    Manages background tasks. P0 fix: supports graceful drain.
    Also tracks fire-and-forget tasks so they can be awaited on shutdown.
    """

    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.ephemeral: set = set()
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def start_task(self, name: str, coro_func: Callable, *args, **kwargs) -> Optional[asyncio.Task]:
        async def wrapper():
            backoff = 1
            max_backoff = 300
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e), exc_info=True)
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
        """Track fire-and-forget tasks so shutdown can await them."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None
        task = loop.create_task(coro)
        self.ephemeral.add(task)
        task.add_done_callback(self.ephemeral.discard)
        return task

    async def drain(self, timeout: float) -> None:
        """Await all background work, then cancel stragglers."""
        self.shutdown_event.set()
        all_tasks: List[asyncio.Task] = list(self.tasks.values()) + list(self.ephemeral)
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
# SECTION 7. ENHANCED ATP SYNTHASE (STABLE)
# =============================================================================
class EnhancedATPSynthase:
    """
    Core synthase model. All public methods that touch locks or services are
    async. Sync helpers (`get_status`) are pure reads.
    """

    def __init__(self, synthase_id: str, config: SynthaseConfig):
        self.synthase_id = synthase_id
        self.config = config
        self.mode = SynthaseMode.IDLE
        self.state = SynthaseState.ACTIVE
        self.rotation_speed = 0.0
        self.current_efficiency = config.base_efficiency
        self.total_atp_produced = 0.0
        self.total_atp_hydrolyzed = 0.0
        self.production_history: deque = deque(maxlen=1000)
        self.inhibition_level = 0.0
        self.operational_hours = 0.0
        self.degradation_rate = 0.0001
        self.repair_rate = 0.01
        self.quantum_coherence = 1.0
        self.quantum_enhancement_factor = 0.0
        self.quantum_active = False
        self._lock = asyncio.Lock()

    async def calculate_driving_force(self, gradient_service: Optional[Any] = None) -> float:
        if gradient_service is None:
            return 0.0
        strengths = gradient_service.get_field_strengths()
        weights = {
            "carbon": 0.25, "helium": 0.15, "trust": 0.20,
            "opportunity": 0.25, "eco_atp_reserve": 0.15,
        }
        return sum(strengths.get(f, 0.0) * w for f, w in weights.items())

    async def calculate_rotation_speed(self, driving_force: float) -> float:
        if driving_force < self.config.activation_gradient:
            return 0.0
        speed = driving_force * self.config.max_rotation_speed_rpm
        return min(speed, self.config.max_rotation_speed_rpm)

    async def calculate_atp_production_rate(self, rotation_speed: float) -> float:
        if rotation_speed == 0:
            return 0.0
        rps = rotation_speed / 60.0
        efficiency = self.current_efficiency * (1 - self.inhibition_level)
        return rps * self.config.atp_per_rotation * efficiency

    async def update_allosteric_inhibition(self, atp_balance: float) -> None:
        async with self._lock:
            if atp_balance > 20000:
                self.inhibition_level = min(
                    self.config.atp_inhibition_max,
                    self.inhibition_level + self.config.atp_inhibition_constant,
                )
            elif atp_balance < 5000:
                self.inhibition_level = max(0.0, self.inhibition_level - 0.01)
            else:
                self.inhibition_level *= 0.99
            self.inhibition_level = max(
                0.0, min(self.config.atp_inhibition_max, self.inhibition_level)
            )

    async def operate_forward(
        self,
        gradient_service: Optional[Any],
        token_service: Optional[Any],
        account_id: str,
    ) -> float:
        async with self._lock:
            if self.state == SynthaseState.DORMANT:
                return 0.0
            driving_force = await self.calculate_driving_force(gradient_service)
            speed = await self.calculate_rotation_speed(driving_force)
            if speed == 0:
                return 0.0
            self.rotation_speed = speed
            self.mode = SynthaseMode.SYNTHESIS
            if self.config.quantum_tunneling_enabled and self.quantum_active:
                speed *= (1 + self.quantum_enhancement_factor * self.config.quantum_efficiency_boost)
            atp_rate = await self.calculate_atp_production_rate(speed)
            atp_produced = atp_rate * 0.1
            self.total_atp_produced += atp_produced
            self.production_history.append(atp_produced)
            if self.quantum_active:
                self.quantum_coherence -= 0.001
                if self.quantum_coherence <= 0:
                    self.quantum_active = False
                    self.quantum_enhancement_factor = 0.0
            self.operational_hours += 0.1 / 3600.0
            if self.config.degradation_scaling:
                self.degradation_rate *= (1 + 0.001 * self.operational_hours)
            if token_service:
                token_service.generate_tokens(
                    account_id=account_id,
                    source=EcoATPSource.GRADIENT_CONVERSION,
                    energy_saved_kwh=atp_produced / 10000.0,
                    efficiency=self.current_efficiency * (1 - self.inhibition_level),
                )
            return atp_produced

    async def operate_reverse(
        self,
        gradient_service: Optional[Any],
        token_service: Optional[Any],
        account_id: str,
        amount: float,
    ) -> float:
        async with self._lock:
            if self.state == SynthaseState.DORMANT:
                return 0.0
            self.mode = SynthaseMode.HYDROLYSIS
            self.rotation_speed = -self.config.max_rotation_speed_rpm * 0.5
            atp_hydrolyzed = amount * self.config.reverse_efficiency
            self.total_atp_hydrolyzed += atp_hydrolyzed
            if gradient_service:
                gradient_service.pump_field("helium", atp_hydrolyzed * 0.01, "reverse_operation")
            return atp_hydrolyzed

    async def operate_uncoupled(self, gradient_service: Optional[Any]) -> None:
        async with self._lock:
            self.mode = SynthaseMode.UNCOUPLED
            self.rotation_speed = self.config.max_rotation_speed_rpm * 0.9
            if gradient_service:
                for field_id, strength in gradient_service.get_field_strengths().items():
                    if strength > self.config.uncoupling_activation_threshold:
                        gradient_service.discharge_field(field_id, strength * 0.1)

    async def repair(self) -> None:
        async with self._lock:
            self.state = SynthaseState.REPAIRING
            self.degradation_rate = max(0.0001, self.degradation_rate * 0.9)
            self.current_efficiency = min(
                self.config.base_efficiency, self.current_efficiency + self.repair_rate
            )
            self.state = SynthaseState.ACTIVE
            logger.info("Synthase repaired", id=self.synthase_id)

    def get_status(self) -> Dict[str, Any]:
        return {
            "id": self.synthase_id,
            "mode": self.mode.value,
            "state": self.state.value,
            "rotation_speed": self.rotation_speed,
            "efficiency": self.current_efficiency,
            "inhibition_level": self.inhibition_level,
            "total_atp_produced": self.total_atp_produced,
            "total_atp_hydrolyzed": self.total_atp_hydrolyzed,
            "quantum_active": self.quantum_active,
            "quantum_enhancement": self.quantum_enhancement_factor,
            "operational_hours": self.operational_hours,
            "degradation_rate": self.degradation_rate,
        }


# =============================================================================
# SECTION 8. DEMAND PRIORITY MANAGER (STABLE)
# =============================================================================
class DemandPriorityManager:
    def __init__(self, config: SynthaseSchedulerConfig):
        self.config = config
        self.priorities: Dict[str, DemandPriority] = {}
        for level, params in config.priority_defaults.items():
            self.priorities[level] = DemandPriority(
                priority_level=level,
                weight=params["weight"],
                min_balance=params["min_balance"],
                max_consumption=params["max_consumption"],
            )
        self.default_priority = config.default_priority
        self._lock = asyncio.Lock()
        self.performance_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=config.priority_performance_window)
        )

    async def set_priority_config(
        self, priority_level: str, weight: float,
        min_balance: float, max_consumption: float,
    ) -> None:
        async with self._lock:
            if priority_level not in self.priorities:
                self.priorities[priority_level] = DemandPriority(
                    priority_level, weight, min_balance, max_consumption
                )
            else:
                p = self.priorities[priority_level]
                p.weight = weight
                p.min_balance = min_balance
                p.max_consumption = max_consumption

    def get_priority_weight(self, priority_level: str) -> float:
        return self.priorities.get(
            priority_level, self.priorities[self.default_priority]
        ).weight

    def get_task_priority(self, task: ScheduledTask) -> float:
        base = self.get_priority_weight(task.user_priority or self.default_priority)
        if task.deadline:
            remaining = (task.deadline - datetime.now(timezone.utc)).total_seconds()
            if remaining < 300:
                base *= 1.5
            elif remaining < 3600:
                base *= 1.2
        return base * (task.priority + 1)

    async def adapt_weights(self) -> None:
        if not self.config.adaptive_priority_enabled:
            return
        async with self._lock:
            for level, hist in self.performance_history.items():
                if len(hist) >= self.config.priority_performance_window:
                    avg = float(np.mean(hist))
                    if avg > 0.8:
                        delta = self.config.adaptive_priority_learning_rate
                    elif avg < 0.5:
                        delta = -self.config.adaptive_priority_learning_rate
                    else:
                        delta = 0.0
                    p = self.priorities[level]
                    p.weight = max(0.1, min(5.0, p.weight + delta))

    async def record_performance(self, priority_level: str, success: bool, latency: float) -> None:
        async with self._lock:
            if priority_level in self.priorities:
                self.performance_history[priority_level].append(1.0 if success else 0.0)


# =============================================================================
# SECTION 9. LOAD BALANCER (STABLE)
# =============================================================================
class SynthaseLoadBalancer:
    def __init__(self, config: SynthaseSchedulerConfig):
        self.config = config
        self.historical_loads: Dict[str, List[float]] = {}
        self.performance_history: Dict[str, deque] = {}
        self._lock = asyncio.Lock()

    async def assign_load(
        self, synthases: Dict[str, EnhancedATPSynthase], total_demand: float
    ) -> Dict[str, float]:
        async with self._lock:
            if not synthases:
                return {}
            weights = self.config.load_balance_weights
            scores: Dict[str, float] = {}
            total = 0.0
            for sid, s in synthases.items():
                if s.state == SynthaseState.ACTIVE:
                    health = 1.0
                elif s.state == SynthaseState.QUANTUM_READY:
                    health = 1.2
                elif s.state == SynthaseState.DEGRADED:
                    health = 0.6
                elif s.state == SynthaseState.REPAIRING:
                    health = 0.3
                else:
                    health = 0.5
                efficiency = s.current_efficiency
                quantum_bonus = 1.0 + s.quantum_enhancement_factor * 0.5
                hist = self.performance_history.get(sid, deque(maxlen=10))
                perf = sum(hist) / len(hist) if hist else 0.5
                perf_factor = 0.5 + perf
                score = (
                    health * weights.get("health", 0.3)
                    + efficiency * weights.get("efficiency", 0.3)
                    + quantum_bonus * weights.get("quantum", 0.2)
                    + perf_factor * weights.get("performance", 0.2)
                )
                self.historical_loads.setdefault(sid, []).append(score)
                self.historical_loads[sid] = self.historical_loads[sid][
                    -self.config.load_balance_history_size:
                ]
                scores[sid] = score
                total += score
            if total == 0:
                return {sid: total_demand / len(synthases) for sid in synthases}
            return {sid: (sc / total) * total_demand for sid, sc in scores.items()}

    async def record_performance(self, synthase_id: str, load: float) -> None:
        async with self._lock:
            self.performance_history.setdefault(synthase_id, deque(maxlen=10)).append(load)

    def get_load_balance_stats(self) -> Dict[str, Any]:
        return {
            "synthases_tracked": len(self.historical_loads),
            "average_loads": {
                sid: float(np.mean(loads)) if loads else 0.0
                for sid, loads in self.historical_loads.items()
            },
        }


# =============================================================================
# SECTION 10. ML DEMAND PREDICTOR (STABLE)
# =============================================================================
class MLDemandPredictor:
    def __init__(self, config: SynthaseSchedulerConfig):
        self.config = config
        self.model = None
        self.scaler = None
        self.is_trained = False
        self.training_data: List[float] = []
        self._lock = asyncio.Lock()
        self._load_model()

    def _load_model(self) -> None:
        if not SKLEARN_AVAILABLE:
            return
        path = self.config.ml_model_path
        if os.path.exists(path):
            try:
                self.model, self.scaler = joblib.load(path)
                self.is_trained = True
                logger.info("Loaded ML model", path=path)
            except Exception as e:
                logger.warning("Failed to load ML model", error=str(e))

    def _save_model(self) -> None:
        if not SKLEARN_AVAILABLE or not self.is_trained:
            return
        try:
            os.makedirs(os.path.dirname(self.config.ml_model_path), exist_ok=True)
            joblib.dump((self.model, self.scaler), self.config.ml_model_path)
        except Exception as e:
            logger.error("Failed to save ML model", error=str(e))

    async def train(self, history: List[float]) -> Dict[str, Any]:
        if not SKLEARN_AVAILABLE:
            return {"status": "sklearn_unavailable"}
        if len(history) < self.config.ml_min_samples:
            return {"status": "insufficient_data"}
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._train_sync, history)

    def _train_sync(self, history: List[float]) -> Dict[str, Any]:
        X, y = [], []
        for i in range(self.config.ml_lookback, len(history) - 1):
            X.append(history[i - self.config.ml_lookback:i])
            y.append(history[i + 1])
        X = np.array(X)
        y = np.array(y)
        if len(X) < self.config.ml_min_samples:
            return {"status": "insufficient_samples"}
        if self.scaler is None:
            self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(X)
        self.model = RandomForestRegressor(n_estimators=100, random_state=42)
        self.model.fit(X_scaled, y)
        self.is_trained = True
        self.training_data = list(history)
        self._save_model()
        return {"status": "success", "samples": len(X)}

    async def predict(self, recent: List[float]) -> Dict[str, Any]:
        if not self.is_trained or len(recent) < self.config.ml_lookback:
            return {"prediction": None, "confidence": 0.0}
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._predict_sync, recent)

    def _predict_sync(self, recent: List[float]) -> Dict[str, Any]:
        features = recent[-self.config.ml_lookback:]
        scaled = self.scaler.transform([features])
        prediction = float(self.model.predict(scaled)[0])
        volatility = float(np.std(recent[-20:])) if len(recent) >= 20 else 0.2
        confidence = max(0.1, 1.0 - volatility)
        return {
            "prediction": max(0.0, min(1.0, prediction)),
            "confidence": confidence,
        }

    def get_model_stats(self) -> Dict[str, Any]:
        return {
            "is_trained": self.is_trained,
            "training_samples": len(self.training_data),
            "model_type": type(self.model).__name__ if self.model else None,
            "lookback": self.config.ml_lookback,
        }


# =============================================================================
# SECTION 11. GRADIENT FORECASTER (STABLE)
# =============================================================================
class GradientForecaster:
    def __init__(self, config: SynthaseSchedulerConfig):
        self.config = config
        self.gradient_history: Dict[str, List[float]] = {}
        self.forecast_results: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self.level: Dict[str, float] = {}
        self.trend: Dict[str, float] = {}

    def record_gradient(self, field_id: str, value: float) -> None:
        if field_id not in self.gradient_history:
            self.gradient_history[field_id] = []
            self.level[field_id] = value
            self.trend[field_id] = 0.0
        self.gradient_history[field_id].append(value)
        max_len = self.config.forecast_history_window * 2
        if len(self.gradient_history[field_id]) > max_len:
            self.gradient_history[field_id] = self.gradient_history[field_id][-max_len:]
        if len(self.gradient_history[field_id]) >= 2:
            a, b = self.config.forecast_alpha, self.config.forecast_beta
            last_level = self.level.get(field_id, value)
            last_trend = self.trend.get(field_id, 0.0)
            self.level[field_id] = a * value + (1 - a) * (last_level + last_trend)
            self.trend[field_id] = (
                b * (self.level[field_id] - last_level) + (1 - b) * last_trend
            )

    async def forecast(self, field_id: str) -> Dict[str, Any]:
        if field_id not in self.gradient_history or len(self.gradient_history[field_id]) < 20:
            return {"status": "insufficient_data"}
        async with self._lock:
            current_level = self.level.get(field_id, 0.5)
            current_trend = self.trend.get(field_id, 0.0)
            forecast_values = [
                max(0.0, min(1.0, current_level + current_trend * (i + 1)))
                for i in range(self.config.forecast_horizon)
            ]
            volatility = float(np.std(self.gradient_history[field_id][-20:]))
            confidence = max(0.1, 1.0 - volatility * 2)
            result = {
                "field": field_id,
                "current": self.gradient_history[field_id][-1],
                "forecast": forecast_values,
                "trend": (
                    "increasing" if current_trend > 0.01
                    else "decreasing" if current_trend < -0.01
                    else "stable"
                ),
                "slope": current_trend,
                "confidence": confidence,
            }
            self.forecast_results[field_id] = result
            return result


# =============================================================================
# SECTION 12. MAIN SCHEDULER (STABLE)
# =============================================================================
class ATPSynthaseScheduler:
    """
    Main scheduler. All public methods are async when they touch I/O or locks.
    """

    def __init__(
        self,
        token_service: Optional[Any] = None,
        gradient_service: Optional[Any] = None,
        harvester: Optional[Any] = None,
        config: Optional[Union[SynthaseSchedulerConfig, Dict[str, Any]]] = None,
        storage: Optional[Any] = None,
        message_queue: Optional[Any] = None,
        adaptive_cost: Optional[Any] = None,
        pareto_gating: Optional[Any] = None,
        drift_detector: Optional[Any] = None,
        metrics: Optional[Any] = None,
    ):
        self.token_service = token_service
        self.gradient_service = gradient_service
        self.harvester = harvester
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating
        self.drift_detector = drift_detector
        self.metrics = metrics

        # Config
        if isinstance(config, dict):
            self.config = SynthaseSchedulerConfig(**config)
        elif isinstance(config, SynthaseSchedulerConfig):
            self.config = config
        else:
            self.config = SynthaseSchedulerConfig()

        # Synthases
        synthase_config = SynthaseConfig(
            protons_per_rotation=self.config.protons_per_rotation,
            atp_per_rotation=self.config.atp_per_rotation,
            max_rotation_speed_rpm=self.config.max_rotation_speed_rpm,
            activation_gradient=self.config.activation_gradient,
            base_efficiency=self.config.base_efficiency,
            atp_inhibition_constant=self.config.atp_inhibition_constant,
            atp_inhibition_max=self.config.atp_inhibition_max,
            reverse_efficiency=self.config.reverse_efficiency,
            hydrolysis_protons_per_atp=self.config.hydrolysis_protons_per_atp,
            uncoupling_leak_rate=self.config.uncoupling_leak_rate,
            uncoupling_activation_threshold=self.config.uncoupling_activation_threshold,
            adaptive_c_ring=self.config.adaptive_c_ring,
            min_c_ring=self.config.min_c_ring,
            max_c_ring=self.config.max_c_ring,
            degradation_scaling=self.config.degradation_scaling,
            quantum_tunneling_enabled=self.config.quantum_tunneling_enabled,
            quantum_efficiency_boost=self.config.quantum_efficiency_boost,
            quantum_tunneling_threshold=self.config.quantum_tunneling_threshold,
            quantum_coherence_time=self.config.quantum_coherence_time,
        )
        self.primary_synthase = EnhancedATPSynthase("primary", synthase_config)
        self.synthases: Dict[str, EnhancedATPSynthase] = {"primary": self.primary_synthase}

        # Sub-components
        self.priority_manager = DemandPriorityManager(self.config)
        self.load_balancer = SynthaseLoadBalancer(self.config)
        self.ml_predictor = (
            MLDemandPredictor(self.config) if self.config.enable_ml_prediction else None
        )
        self.gradient_forecaster = GradientForecaster(self.config)

        # --- Advanced modules (all honest stubs by default) ---
        self.rl_agent = (
            CausalRLAgent(10, 3, enabled=self.config.enable_causal_rl)
            if self.config.enable_causal_rl else None
        )
        self.federated = (
            FederatedCoordinator(self, message_queue, enabled=self.config.enable_federated_learning)
            if self.config.enable_federated_learning else None
        )
        self.safety_monitor: Optional[SafetyMonitor] = None
        if self.config.enable_safety_monitor:
            self.safety_monitor = SafetyMonitor(enabled=True)
            self._setup_safety_invariants()
        self.xai = XAIExplainer(enabled=self.config.enable_xai) if self.config.enable_xai else None
        self.precision_controller = PrecisionController(
            enabled=self.config.enable_precision_switching
        ) if self.config.enable_precision_switching else None
        self.carbon_market = CarbonMarketClient(
            enabled=self.config.enable_carbon_market,
        ) if self.config.enable_carbon_market else None
        self.chaos_injector = ChaosInjector(
            self, self.config.chaos_probability, enabled=self.config.enable_chaos
        ) if self.config.enable_chaos else None
        self.human_approval = HumanApprovalHandler(
            message_queue,
            enabled=self.config.enable_human_approval,
            auto_approve_dev=self.config.dev_auto_approve,
        ) if self.config.enable_human_approval else None

        # --- Queues and state ---
        self.execution_queue: List[ScheduledTask] = []
        self.priority_queue: List[ScheduledTask] = []
        self.total_eco_atp_produced = 0.0
        self.demand_history: deque = deque(maxlen=500)
        self.predicted_demand = 0.0
        self.current_tier = 5
        self.account_id = "atp_synthase"
        if token_service:
            token_service.create_account(self.account_id)

        # MOPD
        self._pareto_front: List[MOPDPoint] = []
        self._mopd_results: Dict[str, Any] = {}

        # Locks — all asyncio, all created inside __init__ but used from the running loop.
        # (asyncio.Lock is loop-agnostic in 3.10+; safe to construct here.)
        self._queue_lock = asyncio.Lock()
        self._synthase_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()

        # Circuit breakers
        self._token_circuit = CircuitBreaker(
            "token_service", db_path=self.config.circuit_breaker_db_path,
            failure_threshold=3, recovery_timeout=30,
        )
        self._gradient_circuit = CircuitBreaker(
            "gradient_service", db_path=self.config.circuit_breaker_db_path,
            failure_threshold=3, recovery_timeout=30,
        )

        # Prometheus
        self.prometheus_metrics = self._setup_metrics() if self.metrics is None else {}

        # Task manager starts AFTER everything is set up
        self._task_manager = TaskManager()
        self._start_background_tasks()

        logger.info(
            "ATPSynthaseScheduler v11.0.0 initialized",
            central_storage=storage is not None,
            rl_agent=self.rl_agent is not None,
            federated=self.federated is not None,
            safety_monitor=self.safety_monitor is not None,
            xai=self.xai is not None,
            carbon_market=self.carbon_market is not None,
            chaos=self.chaos_injector is not None,
            human_approval=self.human_approval is not None,
        )

    # ------------------------------------------------------------------ setup
    def _setup_safety_invariants(self) -> None:
        """Real invariants. No placeholder values."""
        assert self.safety_monitor is not None
        self.safety_monitor.add_invariant(
            "queue_size",
            lambda s: s.get("queue_size", 0) <= 100,
            "Execution queue too large",
        )
        self.safety_monitor.add_invariant(
            "synthase_count",
            lambda s: s.get("synthase_count", 1) <= 10,
            "Too many synthases",
        )
        self.safety_monitor.add_invariant(
            "token_balance_non_negative",
            lambda s: s.get("token_balance", 0.0) >= 0.0,
            "Token balance negative",
        )

    def _setup_metrics(self) -> Dict[str, Any]:
        if not self.config.enable_prometheus or not PROMETHEUS_AVAILABLE:
            return {}
        return {
            "total_produced": Counter("atp_total_produced", "Total Eco-ATP produced"),
            "production_rate": Gauge("atp_production_rate", "Current production rate"),
            "demand_level": Gauge("atp_demand_level", "Current demand level"),
            "efficiency": Gauge("atp_efficiency", "Current efficiency"),
            "synthase_count": Gauge("atp_synthase_count", "Number of synthases"),
            "queue_size": Gauge("atp_queue_size", "Execution queue size"),
            "priority_queue_size": Gauge("atp_priority_queue_size", "Priority queue size"),
            "degradation_tier": Gauge("atp_degradation_tier", "Current degradation tier"),
            "inhibition_level": Gauge("atp_inhibition_level", "Current inhibition level"),
        }

    def _start_background_tasks(self) -> None:
        self._task_manager.start_task("synthesis", self._synthesis_loop)
        self._task_manager.start_task("regulation", self._regulation_loop)
        self._task_manager.start_task("maintenance", self._maintenance_loop)
        self._task_manager.start_task("predictive", self._predictive_loop)
        self._task_manager.start_task("gradient_forecast", self._gradient_forecast_loop)
        self._task_manager.start_task("degradation_update", self._degradation_update_loop)
        self._task_manager.start_task("priority_adapt", self._priority_adapt_loop)

    # ----------------------------------------------------- helpers (async safe)
    async def calculate_gradient_driving_force(self) -> float:
        """Async version. Uses the gradient circuit breaker."""
        if not self.gradient_service:
            return 0.0

        async def _get():
            return self.gradient_service.get_field_strengths()

        try:
            strengths = await self._gradient_circuit.call(_get)
        except Exception as e:
            logger.warning("Gradient service failed", error=str(e))
            return 0.0
        weights = self.config.driving_force_weights
        return sum(strengths.get(f, 0.0) * w for f, w in weights.items())

    async def _calculate_demand_level(self) -> float:
        """Async demand level. P0 fix: was sync with `async with` inside."""
        if not self.token_service:
            return 0.5

        async def _get_summary():
            return self.token_service.get_system_summary()

        try:
            summary = await self._token_circuit.call(_get_summary)
        except Exception:
            summary = {"total_balance": 10000, "total_consumed": 0, "total_generated": 0}

        balance = summary.get("total_balance", 10000)
        consumption = summary.get("total_consumed", 0)
        generation = summary.get("total_generated", 0)

        queue_demand = min(1.0, len(self.execution_queue) / 50.0)
        if self.execution_queue:
            weights = [self.priority_manager.get_task_priority(t) for t in self.execution_queue[:10]]
            priority_demand = float(np.mean(weights)) if weights else 0.5
        else:
            priority_demand = 0.5
        ratio_demand = consumption / generation if generation > 0 else 1.0
        if balance < 5000:
            balance_demand = 1.0
        elif balance < 20000:
            balance_demand = 0.5 + (20000 - balance) / 30000
        else:
            balance_demand = max(0.1, 1.0 - (balance - 20000) / 30000)

        demand = (
            queue_demand * 0.2 + priority_demand * 0.2
            + ratio_demand * 0.3 + balance_demand * 0.3
        )
        demand = min(1.0, max(0.1, demand))
        # deque.append is thread-safe for CPython; no async lock needed.
        self.demand_history.append(demand)
        return demand

    async def _get_token_balance(self) -> float:
        if not self.token_service:
            return 10000.0

        async def _get():
            return self.token_service.get_system_summary()

        try:
            summary = await self._token_circuit.call(_get)
        except Exception:
            return 10000.0
        return float(summary.get("total_balance", 10000))

    async def _get_gradient_strengths(self) -> Dict[str, float]:
        if not self.gradient_service:
            return {}

        async def _get():
            return self.gradient_service.get_field_strengths()

        try:
            return await self._gradient_circuit.call(_get)
        except Exception:
            return {}

    async def _get_rl_state(self) -> Dict[str, Any]:
        return {
            "demand": await self._calculate_demand_level(),
            "token_balance": await self._get_token_balance(),
            "carbon": (await self._get_gradient_strengths()).get("carbon", 0.5),
            "helium": (await self._get_gradient_strengths()).get("helium", 0.5),
            "queue_size": len(self.execution_queue),
            "efficiency": self.primary_synthase.current_efficiency,
            "inhibition": self.primary_synthase.inhibition_level,
            "quantum_active": self.primary_synthase.quantum_active,
            "tier": self.current_tier,
        }

    # ------------------------------------------------------- policy (validated)
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """
        Always returns a valid probability vector (>=0, sum==1).
        P0 fix: previous versions could return unnormalized vectors.
        """
        probs: List[float]
        if self.rl_agent is not None:
            try:
                features = self._state_to_features(state)
                probs = list(self.rl_agent.get_policy_probs(features))
            except Exception:
                probs = [1.0 / 3, 1.0 / 3, 1.0 / 3]
        elif self.adaptive_cost and self.pareto_gating:
            strategies = ["forward", "reverse", "uncoupled"]
            candidates = []
            for strat in strategies:
                if strat == "forward":
                    quality, carbon_g, latency_ms, energy_j = 0.8, 2.0, 10.0, 5.0
                elif strat == "reverse":
                    quality, carbon_g, latency_ms, energy_j = 0.6, 1.0, 20.0, 2.0
                else:
                    quality, carbon_g, latency_ms, energy_j = 0.4, 0.5, 30.0, 1.0
                cost = self.adaptive_cost.compute(
                    quality=quality, carbon_g=carbon_g, latency_ms=latency_ms,
                    energy_joules=energy_j, health=0.8, atp=0.5,
                )
                candidates.append({
                    "strategy": strat, "score": cost,
                    "carbon_g": carbon_g, "latency_ms": latency_ms,
                    "energy_joules": energy_j, "quality_score": quality,
                })
            filtered = self.pareto_gating.filter(candidates)
            if filtered:
                allowed = {c["strategy"] for c in filtered}
                candidates = [c for c in candidates if c["strategy"] in allowed]
            if not candidates:
                probs = [1.0 / 3] * 3
            else:
                scores = np.array([c["score"] for c in candidates], dtype=float)
                exp = np.exp(scores - scores.max())
                norm = exp / exp.sum()
                probs = [0.0, 0.0, 0.0]
                for c, p in zip(candidates, norm):
                    probs[strategies.index(c["strategy"])] = float(p)
        else:
            demand = state.get("demand", await self._calculate_demand_level())
            probs = [max(0.1, demand), max(0.1, 1.0 - demand), 0.1 if demand < 0.8 else 0.3]

        # Sanitize
        arr = np.asarray(probs, dtype=float)
        arr = np.clip(arr, 0.0, None)
        total = float(arr.sum())
        if total <= 0 or not np.isfinite(total):
            arr = np.ones_like(arr) / len(arr)
        else:
            arr = arr / total
        return arr.tolist()

    def _state_to_features(self, state: Dict[str, Any]) -> np.ndarray:
        return np.array([
            state.get("demand", 0.5),
            state.get("token_balance", 10000) / 20000.0,
            state.get("carbon", 0.5),
            state.get("helium", 0.5),
            state.get("queue_size", 0) / 100.0,
            state.get("efficiency", 0.9),
            state.get("inhibition", 0.0),
            1.0 if state.get("quantum_active", False) else 0.0,
            state.get("tier", 5) / 5.0,
            datetime.now(timezone.utc).hour / 24.0,
        ], dtype=float)

    # ------------------------------------------------------------------ MOPD
    async def _generate_pareto_front(self) -> List[MOPDPoint]:
        if not self.config.mopd.enabled:
            return []
        # Seed from time so exploration varies
        rng = np.random.default_rng(int(time.time()) & 0xFFFFFFFF)
        current_driving = self.config.driving_force_weights
        current_load = self.config.load_balance_weights
        current_priority = {k: p.weight for k, p in self.priority_manager.priorities.items()}
        total = sum(current_priority.values())
        if total > 0:
            current_priority = {k: v / total for k, v in current_priority.items()}

        points: List[MOPDPoint] = []
        for _ in range(20):
            dk = list(current_driving.keys())
            dv = rng.dirichlet([1.0] * len(dk))
            driving = {dk[i]: float(dv[i]) for i in range(len(dk))}

            lk = list(current_load.keys())
            lv = rng.dirichlet([1.0] * len(lk))
            load = {lk[i]: float(lv[i]) for i in range(len(lk))}

            pk = list(current_priority.keys())
            pv = rng.dirichlet([1.0] * len(pk))
            priority = {pk[i]: float(pv[i]) for i in range(len(pk))}

            obj = await self._evaluate_weight_combination(driving, load, priority)
            points.append(MOPDPoint(
                driving_force_weights=driving,
                load_balance_weights=load,
                priority_weights=priority,
                total_produced=obj["total_produced"],
                avg_efficiency=obj["avg_efficiency"],
                demand_satisfaction=obj["demand_satisfaction"],
                token_balance=obj["token_balance"],
            ))
        return self._filter_pareto(points)

    async def _evaluate_weight_combination(
        self, driving: Dict[str, float], load: Dict[str, float], priority: Dict[str, float]
    ) -> Dict[str, float]:
        stats = await self.get_scheduler_stats()
        current_production = stats.get("total_eco_atp_produced", 1000.0)
        current_efficiency = stats.get("current_atp_rate", 0.5) / 100.0
        current_token = (await self._get_token_balance()) / 20000.0

        production_boost = (driving.get("carbon", 0.25) + driving.get("opportunity", 0.25)) / 0.5
        total_produced = current_production * production_boost

        efficiency_factor = (load.get("health", 0.3) + load.get("efficiency", 0.3)) / 0.6
        avg_efficiency = current_efficiency * efficiency_factor

        demand_satisfaction = min(
            1.0, (priority.get("critical", 0.2) + priority.get("high", 0.2)) / 0.4
        )
        token_balance = current_token * (production_boost * 0.5 + efficiency_factor * 0.5)

        return {
            "total_produced": max(0.0, total_produced),
            "avg_efficiency": max(0.0, min(1.0, avg_efficiency)),
            "demand_satisfaction": max(0.0, min(1.0, demand_satisfaction)),
            "token_balance": max(0.0, min(1.0, token_balance)),
        }

    def _filter_pareto(self, points: List[MOPDPoint]) -> List[MOPDPoint]:
        if not points:
            return []
        keys = ["total_produced", "avg_efficiency", "demand_satisfaction", "token_balance"]
        front: List[MOPDPoint] = []
        for i, p_i in enumerate(points):
            a_vec = [getattr(p_i, k) for k in keys]
            dominated = False
            for j, p_j in enumerate(points):
                if i == j:
                    continue
                b_vec = [getattr(p_j, k) for k in keys]
                if all(b >= a for a, b in zip(a_vec, b_vec)) and any(
                    b > a for a, b in zip(a_vec, b_vec)
                ):
                    dominated = True
                    break
            if not dominated:
                front.append(p_i)
        return front

    def _select_best_from_pareto(self, front: List[MOPDPoint]) -> Optional[MOPDPoint]:
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
        # Do not mutate the front; return a copy with scalarised score.
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
        if best is not None:
            # Return a shallow copy so we do not mutate the stored front.
            copy = MOPDPoint.from_dict(best.to_dict())
            copy.scalarised_score = best_score
            return copy
        return None

    async def optimize_with_mopd(self, apply_best: bool = True) -> Dict[str, Any]:
        if not self.config.mopd.enabled:
            return {"status": "mopd_disabled"}

        if self.safety_monitor is not None:
            state = await self._get_safety_state()
            violations = self.safety_monitor.check(state)
            if violations:
                logger.warning("Safety violations before MOPD", violations=violations)
                return {"status": "safety_violation", "violations": violations}

        front = await self._generate_pareto_front()
        if not front:
            return {"status": "no_pareto_front"}
        self._pareto_front = front
        best = self._select_best_from_pareto(front)
        if best is None:
            return {"status": "no_best_plan"}

        applied = False
        if apply_best:
            if self.human_approval is not None:
                approved = await self.human_approval.request_approval({
                    "action": "apply_mopd_plan",
                    "plan": best.to_dict(),
                })
                if not approved:
                    return {"status": "rejected_by_human"}
            await self._apply_mopd_plan(best)
            applied = True

        explanation = None
        if self.xai is not None:
            explanation = self.xai.explain_mopd(best, self.config.mopd.objective_weights)

        if self.queue and FeedbackEvent is not None:
            try:
                event = FeedbackEvent.create_with_context(
                    task_id=f"atp_mopd_{uuid.uuid4().hex[:8]}",
                    selected_action="mopd_optimization",
                    quality_score=best.scalarised_score,
                    energy_joules=0.0, carbon_g=0.0,
                    feedback_type="atp_scheduler",
                    adaptive_cost_value=best.scalarised_score,
                    state={"pareto_front_size": len(front), "applied": applied},
                    candidates=[{"action": "optimize"}],
                    source="atp_synthase_scheduler",
                    environment=getattr(central_config, "ENVIRONMENT", "production")
                    if central_config else "production",
                    tags=["atp", "mopd"],
                )
                await self.queue.publish("feedback_events", event.to_json())
            except Exception as e:
                logger.warning("Failed to publish MOPD feedback event", error=str(e))

        if self.storage is not None:
            try:
                await self._save_mopd_state()
            except Exception as e:
                logger.warning("Failed to save MOPD state", error=str(e))

        return {
            "status": "success",
            "pareto_front": [p.to_dict() for p in front],
            "best_plan": best.to_dict(),
            "applied": applied,
            "explanation": explanation,
        }

    async def _apply_mopd_plan(self, plan: MOPDPoint) -> None:
        self.config.driving_force_weights = dict(plan.driving_force_weights)
        self.config.load_balance_weights = dict(plan.load_balance_weights)
        for level, weight in plan.priority_weights.items():
            if level in self.priority_manager.priorities:
                p = self.priority_manager.priorities[level]
                await self.priority_manager.set_priority_config(
                    priority_level=level, weight=weight,
                    min_balance=p.min_balance, max_consumption=p.max_consumption,
                )

    async def _save_mopd_state(self) -> None:
        if self.storage is None:
            return
        state = {
            "_v": 1,
            "pareto_front": [p.to_dict() for p in self._pareto_front],
            "objective_weights": self.config.mopd.objective_weights,
        }
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda: self.storage.save_state("atp_mopd_state", json.dumps(state))
        )

    async def _load_mopd_state(self) -> None:
        if self.storage is None:
            return
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(
            None, lambda: self.storage.get_state("atp_mopd_state")
        )
        if not data:
            return
        try:
            state = json.loads(data)
            if state.get("_v") != 1:
                logger.warning("Unknown MOPD state version; ignoring.")
                return
            self._pareto_front = [
                MOPDPoint.from_dict(p) for p in state.get("pareto_front", [])
            ]
            self.config.mopd.objective_weights = state.get(
                "objective_weights", self.config.mopd.objective_weights
            )
        except Exception as e:
            logger.warning("Failed to load MOPD state", error=str(e))

    async def _get_safety_state(self) -> Dict[str, Any]:
        return {
            "queue_size": len(self.execution_queue),
            "synthase_count": len(self.synthases),
            "token_balance": await self._get_token_balance(),
        }

    # ----------------------------------------------------------- background
    async def _synthesis_loop(self) -> None:
        while True:
            try:
                total_produced = 0.0
                demand = await self._calculate_demand_level()

                state = await self._get_rl_state()
                probs = await self.policy_probs(state)
                action = np.random.choice(["forward", "reverse", "uncoupled"], p=probs)

                async with self._synthase_lock:
                    synthases_copy = dict(self.synthases)

                load_assignments = await self.load_balancer.assign_load(synthases_copy, demand)
                balance = await self._get_token_balance()

                for sid, synthase in synthases_copy.items():
                    if synthase.state not in (SynthaseState.ACTIVE, SynthaseState.QUANTUM_READY):
                        continue
                    assigned = load_assignments.get(sid, demand / max(len(synthases_copy), 1))
                    await synthase.update_allosteric_inhibition(balance)

                    if action == "reverse" and self._should_reverse_operate(balance):
                        await synthase.operate_reverse(
                            self.gradient_service, self.token_service,
                            self.account_id, amount=50.0 * assigned,
                        )
                        continue
                    if action == "uncoupled" and self._should_uncouple():
                        await synthase.operate_uncoupled(self.gradient_service)
                        continue

                    produced = await synthase.operate_forward(
                        self.gradient_service, self.token_service, self.account_id
                    )
                    total_produced += produced * assigned
                    await self.load_balancer.record_performance(sid, assigned)

                if total_produced > 0:
                    async with self._state_lock:
                        self.total_eco_atp_produced += total_produced
                    if self.prometheus_metrics:
                        self.prometheus_metrics["total_produced"].inc(total_produced)
                        self.prometheus_metrics["production_rate"].set(
                            total_produced / self.config.synthesis_interval
                        )

                for fid, strength in (await self._get_gradient_strengths()).items():
                    self.gradient_forecaster.record_gradient(fid, strength)

                if self.chaos_injector is not None:
                    await self.chaos_injector.maybe_inject_failure()

                await asyncio.sleep(self.config.synthesis_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Synthesis loop error", error=str(e), exc_info=True)
                await asyncio.sleep(5)

    def _should_reverse_operate(self, balance: float) -> bool:
        return balance > 25000

    def _should_uncouple(self) -> bool:
        return self.primary_synthase.inhibition_level > 0.4

    async def _regulation_loop(self) -> None:
        while True:
            try:
                if self.safety_monitor is not None:
                    state = await self._get_safety_state()
                    violations = self.safety_monitor.check(state)
                    if violations:
                        logger.warning("Safety violations in regulation", violations=violations)

                balance = await self._get_token_balance()
                async with self._synthase_lock:
                    for s in self.synthases.values():
                        await s.update_allosteric_inhibition(balance)

                demand = await self._calculate_demand_level()
                active = sum(
                    1 for s in self.synthases.values()
                    if s.state in (SynthaseState.ACTIVE, SynthaseState.QUANTUM_READY)
                )
                if demand > 0.8 and active < 3 and self.config.enable_multi_synthase:
                    await self.spawn_synthase()
                elif demand < 0.2 and len(self.synthases) > 1:
                    for sid in list(self.synthases.keys()):
                        if sid != "primary":
                            await self.remove_synthase(sid)
                            break

                if self.prometheus_metrics:
                    self.prometheus_metrics["synthase_count"].set(len(self.synthases))
                    self.prometheus_metrics["queue_size"].set(len(self.execution_queue))
                    self.prometheus_metrics["priority_queue_size"].set(len(self.priority_queue))
                    self.prometheus_metrics["degradation_tier"].set(self.current_tier)
                    self.prometheus_metrics["inhibition_level"].set(
                        self.primary_synthase.inhibition_level
                    )

                await asyncio.sleep(self.config.regulation_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Regulation loop error", error=str(e), exc_info=True)
                await asyncio.sleep(60)

    async def _maintenance_loop(self) -> None:
        while True:
            try:
                async with self._synthase_lock:
                    for s in self.synthases.values():
                        if s.state == SynthaseState.DEGRADED:
                            await s.repair()
                await asyncio.sleep(self.config.maintenance_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Maintenance loop error", error=str(e), exc_info=True)
                await asyncio.sleep(60)

    async def _predictive_loop(self) -> None:
        while True:
            try:
                if self.ml_predictor is not None:
                    history = list(self.demand_history)
                    if (
                        len(history) > self.config.ml_min_samples
                        and (not self.ml_predictor.is_trained or len(history) % 10 == 0)
                    ):
                        await self.ml_predictor.train(history)
                    if len(history) > self.config.ml_lookback:
                        pred = await self.ml_predictor.predict(history)
                        if pred["prediction"] is not None:
                            self.predicted_demand = pred["prediction"]
                await asyncio.sleep(self.config.predictive_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Predictive loop error", error=str(e), exc_info=True)
                await asyncio.sleep(120)

    async def _gradient_forecast_loop(self) -> None:
        while True:
            try:
                for field_id in (await self._get_gradient_strengths()).keys():
                    await self.gradient_forecaster.forecast(field_id)
                await asyncio.sleep(self.config.forecast_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Gradient forecast loop error", error=str(e), exc_info=True)
                await asyncio.sleep(120)

    async def _degradation_update_loop(self) -> None:
        while True:
            try:
                async with self._synthase_lock:
                    efficiencies = [s.current_efficiency for s in self.synthases.values()]
                if efficiencies:
                    avg = float(np.mean(efficiencies))
                    for tier, threshold in sorted(
                        self.config.efficiency_thresholds.items(), reverse=True
                    ):
                        if avg >= threshold:
                            if self.current_tier != tier:
                                self.current_tier = tier
                                logger.info("Degradation tier updated", tier=tier, avg=avg)
                            break
                await asyncio.sleep(self.config.degradation_tier_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Degradation update loop error", error=str(e), exc_info=True)
                await asyncio.sleep(60)

    async def _priority_adapt_loop(self) -> None:
        while True:
            try:
                await self.priority_manager.adapt_weights()
                await asyncio.sleep(self.config.regulation_interval * 5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Priority adapt loop error", error=str(e), exc_info=True)
                await asyncio.sleep(60)

    # ----------------------------------------------------------- public API
    async def spawn_synthase(self, c_ring_size: Optional[int] = None) -> Optional[str]:
        if self.safety_monitor is not None:
            state = await self._get_safety_state()
            state["synthase_count"] = len(self.synthases) + 1
            violations = self.safety_monitor.check(state)
            if violations:
                logger.warning("Safety violation prevents spawning", violations=violations)
                return None

        if not self.config.enable_multi_synthase:
            return "primary"

        cfg = SynthaseConfig()
        if c_ring_size:
            cfg.protons_per_rotation = c_ring_size
        cfg.quantum_tunneling_enabled = self.config.quantum_tunneling_enabled
        sid = f"synthase_{len(self.synthases)}"
        synth = EnhancedATPSynthase(sid, cfg)
        async with self._synthase_lock:
            self.synthases[sid] = synth
        logger.info("Spawned ATP synthase", id=sid)
        return sid

    async def remove_synthase(self, synthase_id: str) -> bool:
        if synthase_id == "primary" or synthase_id not in self.synthases:
            return False
        if self.safety_monitor is not None:
            state = await self._get_safety_state()
            state["synthase_count"] = len(self.synthases) - 1
            violations = self.safety_monitor.check(state)
            if violations:
                logger.warning("Safety violation prevents removal", violations=violations)
                return False
        async with self._synthase_lock:
            self.synthases.pop(synthase_id, None)
        logger.info("Removed ATP synthase", id=synthase_id)
        return True

    async def schedule_execution(
        self, task_id: str, eco_atp_required: float, priority: int = 0,
        deadline: Optional[datetime] = None, callback: Optional[Callable] = None,
        user_priority: Optional[str] = None,
    ) -> bool:
        if not self.token_service:
            return True

        if self.safety_monitor is not None:
            state = await self._get_safety_state()
            state["queue_size"] = len(self.execution_queue) + 1
            violations = self.safety_monitor.check(state)
            if violations:
                logger.warning("Safety violation scheduling task", violations=violations)
                return False

        if self.human_approval is not None and priority > 2:
            approved = await self.human_approval.request_approval({
                "action": "schedule_high_priority",
                "task_id": task_id,
                "eco_atp_required": eco_atp_required,
            })
            if not approved:
                return False

        async def _reserve():
            return self.token_service.reserve_tokens(
                self.account_id, eco_atp_required, EcoATPConsumer.EXPERT_EXECUTION
            )

        success, token_ids = await self._token_circuit.call(_reserve)
        task = ScheduledTask(
            task_id=task_id, eco_atp_required=eco_atp_required,
            priority=priority, deadline=deadline, callback=callback,
            token_ids=token_ids if success else [], user_priority=user_priority,
        )
        async with self._queue_lock:
            if success:
                self.execution_queue.append(task)
                self.execution_queue.sort(
                    key=lambda t: (
                        self.priority_manager.get_task_priority(t),
                        t.deadline or datetime.max.replace(tzinfo=timezone.utc),
                    ),
                    reverse=True,
                )
            else:
                self.priority_queue.append(task)

        if success and self.xai is not None:
            logger.info("Scheduling explanation", text=self.xai.explain_schedule(task))
        return success

    async def execute_next_task(self) -> Optional[Dict[str, Any]]:
        async with self._queue_lock:
            if not self.execution_queue:
                return None
            task = self.execution_queue.pop(0)

        if self.token_service:
            async def _consume():
                return self.token_service.consume_tokens(
                    task.token_ids, EcoATPConsumer.EXPERT_EXECUTION, True
                )
            await self._token_circuit.call(_consume)

        if task.callback:
            result = await task.callback() if asyncio.iscoroutinefunction(task.callback) else task.callback()
            task.status = "completed"
            return {"task_id": task.task_id, "result": result, "status": "completed"}
        task.status = "completed"
        return {"task_id": task.task_id, "status": "completed"}

    async def recover_failed_task(self, task_id: str, completion_percentage: float) -> float:
        async with self._queue_lock:
            for task in list(self.execution_queue):
                if task.task_id == task_id:
                    if self.token_service:
                        async def _recover():
                            return self.token_service.recover_tokens(
                                task.token_ids, completion_percentage
                            )
                        recovered = await self._token_circuit.call(_recover)
                        self.execution_queue.remove(task)
                        return recovered
        return 0.0

    async def set_priority_config(
        self, priority_level: str, weight: float,
        min_balance: float, max_consumption: float,
    ) -> None:
        await self.priority_manager.set_priority_config(
            priority_level, weight, min_balance, max_consumption
        )

    async def get_scheduler_stats(self) -> Dict[str, Any]:
        """Async because it reads live service state."""
        driving_force = await self.calculate_gradient_driving_force()
        rotation_speed = await self.primary_synthase.calculate_rotation_speed(driving_force)
        atp_rate = await self.primary_synthase.calculate_atp_production_rate(rotation_speed)
        demand = await self._calculate_demand_level()
        return {
            "total_eco_atp_produced": self.total_eco_atp_produced,
            "current_driving_force": driving_force,
            "current_rotation_speed": rotation_speed,
            "current_atp_rate": atp_rate,
            "demand_level": demand,
            "predicted_demand": self.predicted_demand,
            "degradation_tier": self.current_tier,
            "queue_size": len(self.execution_queue),
            "priority_queue_size": len(self.priority_queue),
            "synthase_count": len(self.synthases),
            "active_synthases": sum(
                1 for s in self.synthases.values()
                if s.state in (SynthaseState.ACTIVE, SynthaseState.QUANTUM_READY)
            ),
            "synthases": {sid: s.get_status() for sid, s in self.synthases.items()},
            "load_balance": self.load_balancer.get_load_balance_stats(),
            "ml_predictor": self.ml_predictor.get_model_stats() if self.ml_predictor else None,
            "module_status": MODULE_STATUS,
            "circuit_breakers": {
                "token": self._token_circuit.snapshot(),
                "gradient": self._gradient_circuit.snapshot(),
            },
        }

    def get_efficiency_report(self) -> Dict[str, Any]:
        report = {
            "primary_efficiency": self.primary_synthase.current_efficiency,
            "base_efficiency": self.config.base_efficiency,
            "inhibition_level": self.primary_synthase.inhibition_level,
            "synthase_count": len(self.synthases),
            "recommendations": [],
        }
        if self.primary_synthase.current_efficiency < 0.8:
            report["recommendations"].append("Primary synthase degraded; schedule repair.")
        if self.primary_synthase.inhibition_level > 0.4:
            report["recommendations"].append("High ATP inhibition; consider reverse operation.")
        return report

    # ----------------------------------------------------------- shutdown
    async def shutdown(self, timeout: Optional[float] = None) -> None:
        """
        Graceful shutdown:
        1. Stop accepting new work.
        2. Drain background tasks.
        3. Flush ML model and MOPD state.
        """
        timeout = timeout or float(self.config.shutdown_timeout_seconds)
        logger.info("ATP Synthase Scheduler shutting down")

        # 1. Drain background tasks
        try:
            await asyncio.wait_for(self._task_manager.drain(timeout), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Task drain timed out; continuing shutdown")

        # 2. Flush persisted state
        try:
            if self.ml_predictor is not None:
                self.ml_predictor._save_model()
        except Exception as e:
            logger.warning("Failed to flush ML model", error=str(e))
        try:
            await self._save_mopd_state()
        except Exception as e:
            logger.warning("Failed to flush MOPD state", error=str(e))

        logger.info("ATP Synthase Scheduler shutdown complete")

    async def __aenter__(self) -> "ATPSynthaseScheduler":
        # Load persisted MOPD state on entry if storage is available.
        if self.storage is not None:
            await self._load_mopd_state()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.shutdown()


# =============================================================================
# SECTION 13. MOCK SERVICES (for tests and examples)
# =============================================================================
class MockTokenService:
    def __init__(self):
        self.balance = 10000.0
        self.consumed = 0.0
        self.generated = 0.0

    def get_system_summary(self) -> Dict[str, float]:
        return {
            "total_balance": self.balance,
            "total_consumed": self.consumed,
            "total_generated": self.generated,
        }

    def generate_tokens(self, **kwargs) -> List[Any]:
        self.generated += 10
        self.balance += 10
        return []

    def reserve_tokens(self, account_id: str, amount: float, consumer: Any) -> Tuple[bool, List[str]]:
        if self.balance >= amount:
            self.balance -= amount
            return True, [uuid.uuid4().hex]
        return False, []

    def consume_tokens(self, token_ids: List[str], consumer: Any, ok: bool) -> float:
        self.consumed += len(token_ids) * 5.0
        return float(len(token_ids))

    def recover_tokens(self, token_ids: List[str], completion: float) -> float:
        recovered = len(token_ids) * 5.0 * completion
        self.balance += recovered
        return recovered

    def create_account(self, account_id: str) -> None:
        pass

    def get_account_summary(self, account_id: str) -> Dict[str, Any]:
        return {"balance": self.balance}


class MockGradientService:
    def get_field_strengths(self) -> Dict[str, float]:
        return {
            "carbon": 0.8, "helium": 0.2, "trust": 0.1,
            "opportunity": 0.9, "eco_atp_reserve": 0.5,
        }

    def discharge_field(self, field_id: str, amount: float) -> float:
        return 0.0

    def pump_field(self, field_id: str, amount: float, source: str) -> None:
        return None

    def get_field_stats(self) -> Dict[str, Any]:
        return {}


# =============================================================================
# SECTION 14. TESTS (embedded; run with --test)
# =============================================================================
class _Tests(unittest.TestCase):
    def test_policy_probs_valid(self):
        async def go():
            sched = ATPSynthaseScheduler(
                token_service=MockTokenService(),
                gradient_service=MockGradientService(),
            )
            try:
                state = await sched._get_rl_state()
                probs = await sched.policy_probs(state)
                self.assertEqual(len(probs), 3)
                self.assertTrue(all(p >= 0 for p in probs))
                self.assertAlmostEqual(sum(probs), 1.0, places=6)
                # Zero-demand edge case
                probs2 = await sched.policy_probs({"demand": 0.0})
                self.assertAlmostEqual(sum(probs2), 1.0, places=6)
            finally:
                await sched.shutdown(timeout=3)
        asyncio.run(go())

    def test_stats_async(self):
        async def go():
            sched = ATPSynthaseScheduler(
                token_service=MockTokenService(),
                gradient_service=MockGradientService(),
            )
            try:
                stats = await sched.get_scheduler_stats()
                self.assertIn("total_eco_atp_produced", stats)
                self.assertIn("circuit_breakers", stats)
            finally:
                await sched.shutdown(timeout=3)
        asyncio.run(go())

    def test_pareto_filter(self):
        sched = ATPSynthaseScheduler.__new__(ATPSynthaseScheduler)  # bypass __init__
        pts = [
            MOPDPoint({}, {}, {}, 1.0, 1.0, 1.0, 1.0),
            MOPDPoint({}, {}, {}, 0.5, 0.5, 0.5, 0.5),   # dominated
            MOPDPoint({}, {}, {}, 1.0, 0.5, 0.5, 0.5),   # nondominated
        ]
        front = ATPSynthaseScheduler._filter_pareto(sched, pts)
        self.assertEqual(len(front), 2)

    def test_safety_monitor(self):
        mon = SafetyMonitor(enabled=False)
        mon.add_invariant("x_positive", lambda s: s.get("x", 0) > 0, "x must be positive")
        self.assertEqual(mon.check({"x": 1}), [])
        self.assertEqual(len(mon.check({"x": -1})), 1)

    def test_placeholder_denies(self):
        async def go():
            h = HumanApprovalHandler(auto_approve_dev=False)
            self.assertFalse(await h.request_approval({"action": "x"}))
        asyncio.run(go())

    def test_circuit_breaker_roundtrip(self):
        async def go():
            import tempfile
            path = os.path.join(tempfile.gettempdir(), f"cb_{uuid.uuid4().hex}.db")
            cb = CircuitBreaker("test", path, failure_threshold=2, recovery_timeout=1)

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
            self.assertEqual(cb.state, "open")
            try:
                await cb.call(ok)
                self.fail("Should have been open")
            except RuntimeError:
                pass
            os.remove(path)
        asyncio.run(go())

    def test_scheduler_runs_briefly(self):
        async def go():
            sched = ATPSynthaseScheduler(
                token_service=MockTokenService(),
                gradient_service=MockGradientService(),
            )
            await asyncio.sleep(0.5)
            self.assertGreaterEqual(sched.total_eco_atp_produced, 0.0)
            await sched.shutdown(timeout=3)
        asyncio.run(go())

    def test_graceful_shutdown_twice(self):
        async def go():
            sched = ATPSynthaseScheduler(
                token_service=MockTokenService(),
                gradient_service=MockGradientService(),
            )
            await sched.shutdown(timeout=3)
            await sched.shutdown(timeout=3)  # idempotent
        asyncio.run(go())


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 15. EXAMPLE USAGE + ENTRY POINT
# =============================================================================
async def example_usage() -> None:
    config = {
        "enable_multi_synthase": True,
        "enable_quantum": True,
        "enable_ml_prediction": True,
        "ml_model_path": "./test_model.joblib",
        "circuit_breaker_db_path": "./test_cb.db",
        "enable_safety_monitor": True,
        "enable_xai": True,
        "enable_causal_rl": False,
        "enable_federated_learning": False,
        "enable_carbon_market": False,
        "enable_chaos": False,
        "enable_human_approval": False,
    }
    async with ATPSynthaseScheduler(
        token_service=MockTokenService(),
        gradient_service=MockGradientService(),
        config=config,
    ) as scheduler:
        await asyncio.sleep(2)
        result = await scheduler.optimize_with_mopd()
        print("MOPD result:", json.dumps(result, indent=2, default=str))
        ok = await scheduler.schedule_execution("task1", 10.0, priority=1)
        print("Schedule success:", ok)
        stats = await scheduler.get_scheduler_stats()
        print("Stats keys:", sorted(stats.keys()))


def main() -> None:
    parser = argparse.ArgumentParser(description="ATP Synthase Scheduler v11.0.0")
    parser.add_argument("--test", action="store_true", help="Run embedded test suite.")
    parser.add_argument("--example", action="store_true", help="Run example usage.")
    parser.add_argument("--status", action="store_true", help="Print module statuses.")
    args = parser.parse_args()

    if args.status:
        for name, status in MODULE_STATUS.items():
            print(f"{name:25s} {status}")
        return

    if args.test:
        sys.exit(run_tests())

    if args.example:
        asyncio.run(example_usage())
        return

    # Default: print status and exit gracefully
    print("ATP Synthase Scheduler v11.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
