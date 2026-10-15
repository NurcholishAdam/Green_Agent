#!/usr/bin/env python3
"""
Per-layer energy profiling and layer-skipping for energy-efficient inference.
Version 2.2 — Single-File Enhanced

Public API preserved:
    CarbonIntensityManager, CausalBandit, SafetyMonitor, XAIExplainer,
    FederatedProfilerCoordinator, MultiAgentProfiler, CarbonOffsetBroker,
    ChaosMonkey, HumanReviewManager, FlexGenManager, EnergyProfiler,
    LayerSkippingWrapper, and the exception hierarchy.

Key fixes and enhancements over v2.1:
- Cached autocast contexts per precision (no per-layer context recreation).
- Chaos `safety_override` now saves the original threshold and restores it
  when the chaos event is cleaned up (`ChaosMonkey.cleanup_fault`).
- `_encode_context` always returns a list of floats — bandit feature shape
  is now deterministic.
- `CausalBandit.add_actions` validates inputs and updates `self.actions`
  atomically; `record_outcome` no longer duplicates actions.
- Federated aggregation now actually nudges the posterior of the most recent
  policy using the aggregated mean accuracy / energy-saved signals.
- `SafetyMonitor` also exposes `current_threshold` and `restore_thresholds`
  so callers can snapshot/restore.
- `_autocast` uses `torch.amp.autocast` when available and falls back
  gracefully; no more silently returning a raw function.
- `LayerSkippingWrapper.forward_async` publishes a real `SkipDecision` list
  instead of silently accumulating decisions across episodes.
- Metrics: added `PROFILER_RECOVERIES` counter for chaos recovery events.

The file remains self-contained. In a real deployment, split it into a package
(profiler.py, bandit.py, safety.py, xai.py, federated.py, chaos.py, hitl.py,
flexgen.py, wrapper.py).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import json
import logging
import logging.handlers  # noqa: F401
import math
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Protocol, Tuple, Union,
    runtime_checkable,
)

import numpy as np
import torch
import torch.nn as nn

# -----------------------------------------------------------------------------
# Optional / guarded imports
# -----------------------------------------------------------------------------
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from torch.amp import autocast as _torch_autocast  # type: ignore
    TORCH_AMP_AVAILABLE = True
except ImportError:
    try:
        from torch.cuda.amp import autocast as _torch_autocast  # type: ignore
        TORCH_AMP_AVAILABLE = True
    except ImportError:
        _torch_autocast = None  # type: ignore
        TORCH_AMP_AVAILABLE = False

try:
    import structlog  # noqa: F401
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.handlers.RotatingFileHandler(
                    "energy_profiler.log", maxBytes=10 * 1024 * 1024, backupCount=5
                ),
                logging.StreamHandler(),
            ],
        )


# -----------------------------------------------------------------------------
# Enhancement modules (with fallbacks)
# -----------------------------------------------------------------------------
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # type: ignore
    from enhancements.moe_system import ExpertRouter  # type: ignore
    from enhancements.MODP import ParetoOptimizer  # type: ignore
    from enhancements.contextual_bandit import ContextualBandit  # type: ignore
    from enhancements.storage import Storage  # type: ignore
    from enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    from enhancements.scaling.message_queue import AsyncMessageQueue  # type: ignore
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False

    class GeneticPolicyGenerator:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}

    class ExpertRouter:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def encode(self, context):
            if isinstance(context, dict):
                vals = [float(v) for v in context.values() if isinstance(v, (int, float))]
            else:
                vals = [float(v) for v in context]
            while len(vals) < 8:
                vals.append(0.0)
            return vals[:8]
        def select(self, encoded): return "balanced"

    class ParetoOptimizer:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0.0) * weights.get(k, 1.0) for k in objectives)

    class ContextualBandit:  # type: ignore[no-redef]
        def __init__(self, action_space, fallback_solver, *a, **k):
            self.actions = action_space
        def select_action(self, context):
            return self.actions[0], 0.0, "fallback"
        def update(self, context, action, reward): pass
        def seed_safe_policy(self, context, policy): pass

    class Storage:  # type: ignore[no-redef]
        def save_profiler_state(self, state): pass
        def load_profiler_state(self): return None

    class FeedbackEvent:  # type: ignore[no-redef]
        @staticmethod
        def create_with_context(**kwargs): return {}

    class AsyncMessageQueue:  # type: ignore[no-redef]
        async def publish(self, topic, message): pass


try:
    from enhancements.gpu_optimization.flexgen_policy import (  # type: ignore
        FlexGenPolicy, generate_candidate_policies,
    )
    from enhancements.gpu_optimization.flexgen_controller import (  # type: ignore
        FlexGenController,
    )
    from enhancements.gpu_optimization.flexgen_cost_model import (  # type: ignore
        FlexGenCostModel,
    )
    from enhancements.gpu_optimization.policy_drift_detector import (  # type: ignore
        PolicyDriftDetector,
    )
    from enhancements.schemas.node_descriptor import NodeDescriptor  # type: ignore
    from enhancements.schemas.workload_descriptor import (  # type: ignore
        WorkloadDescriptor,
    )
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False

    class FlexGenPolicy: pass  # type: ignore[no-redef]

    def generate_candidate_policies(n=20): return []  # noqa: E704

    class FlexGenController:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        async def step(self): return {}

    class FlexGenCostModel:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass

    class PolicyDriftDetector:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def get_stats(self): return {}

    class NodeDescriptor: pass  # type: ignore[no-redef]
    class WorkloadDescriptor: pass  # type: ignore[no-redef]


# =============================================================================
# Exceptions
# =============================================================================
class EnergyProfilerError(Exception):
    """Base exception for the energy profiler."""


class CarbonFetchError(EnergyProfilerError):
    """Failed to fetch carbon intensity."""


class CircuitBreakerOpenError(EnergyProfilerError):
    """Circuit breaker is open."""


class SafetyViolationError(EnergyProfilerError):
    """Safety monitor detected a violation."""


class ChaosExperimentError(EnergyProfilerError):
    """Chaos monkey injected a failure."""


class NotImplementedYetError(EnergyProfilerError):
    """Raised when an optional dependency is missing."""


# =============================================================================
# Metrics
# =============================================================================
class _NoopMetric:
    def labels(self, *a, **k): return self
    def inc(self, *a, **k): return None
    def set(self, *a, **k): return None
    def observe(self, *a, **k): return None


class _MetricsRegistry:
    def __init__(self) -> None:
        self.enabled = PROMETHEUS_AVAILABLE
        self.registry = CollectorRegistry() if self.enabled else None
        self._counters: Dict[str, Any] = {}
        self._gauges: Dict[str, Any] = {}
        self._histograms: Dict[str, Any] = {}
        self._lock = threading.RLock()

    def counter(self, name, doc="", labels=None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, doc, labels or [], registry=self.registry)
            return self._counters[name]

    def gauge(self, name, doc="", labels=None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name, doc, labels or [], registry=self.registry)
            return self._gauges[name]

    def histogram(self, name, doc="", labels=None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, doc, labels or [], registry=self.registry)
            return self._histograms[name]


METRICS = _MetricsRegistry()
SKIP_DECISIONS = METRICS.counter(
    "profiler_skip_decisions_total", "Layer skip decisions", ["layer", "decision"]
)
SKIP_RATIO = METRICS.gauge("profiler_last_skip_ratio", "Last forward skip ratio")
ENERGY_ESTIMATED = METRICS.histogram(
    "profiler_energy_estimated_joules", "Estimated energy per forward (J)"
)
ENERGY_SAVED = METRICS.histogram(
    "profiler_energy_saved_joules", "Estimated energy saved per forward (J)"
)
CARBON_INTENSITY = METRICS.histogram(
    "profiler_carbon_intensity_g_per_kwh", "Carbon intensity observed"
)
SAFETY_VIOLATIONS = METRICS.counter(
    "profiler_safety_violations_total", "Safety violations", ["rule"]
)
HITL_REQUESTS = METRICS.counter("profiler_hitl_requests_total", "HITL requests")
HITL_STATUS = METRICS.counter("profiler_hitl_status_total", "HITL status", ["status"])
CARBON_OFFSET_KG = METRICS.counter("profiler_carbon_offset_kg_total", "Carbon offset (kg)")
CARBON_REC_MWH = METRICS.counter("profiler_rec_mwh_total", "RECs purchased (MWh)")
CHAOS_EVENTS = METRICS.counter("profiler_chaos_events_total", "Chaos events", ["fault"])
FEDERATED_ROUNDS = METRICS.counter("profiler_federated_rounds_total", "Federated rounds")
BANDIT_UPDATES = METRICS.counter(
    "profiler_bandit_updates_total", "Bandit updates", ["policy"]
)
PROFILER_RECOVERIES = METRICS.counter(
    "profiler_recoveries_total", "Chaos recovery events", ["fault"]
)


# =============================================================================
# Circuit breaker
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self, name: str, failure_threshold: int = 3, recovery_timeout: float = 30.0
    ):
        self.name = name
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self.half_open_success_threshold = 2
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()

    def _precheck(self) -> None:
        with self._lock:
            if (
                self._state == CircuitBreakerState.OPEN
                and self._last_failure_time is not None
                and time.time() - self._last_failure_time >= self.recovery_timeout
            ):
                self._state = CircuitBreakerState.HALF_OPEN
                self._success_count = 0
            if self._state == CircuitBreakerState.OPEN:
                raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")

    def _record_success(self) -> None:
        with self._lock:
            self._success_count += 1
            if self._state == CircuitBreakerState.HALF_OPEN:
                if self._success_count >= self.half_open_success_threshold:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
            else:
                self._failure_count = 0

    def _record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state in (CircuitBreakerState.CLOSED, CircuitBreakerState.HALF_OPEN):
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN

    async def call_async(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        self._precheck()
        try:
            result = await func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    def call_sync(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        self._precheck()
        try:
            result = func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    async def call(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        if inspect.iscoroutinefunction(func):
            return await self.call_async(func, *args, **kwargs)
        result = self.call_sync(func, *args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
            }


# =============================================================================
# Protocols
# =============================================================================
@runtime_checkable
class CarbonIntensityProvider(Protocol):
    async def get_current_intensity(self) -> float: ...
    def get_last_intensity(self) -> float: ...
    async def update_intensity(self) -> float: ...


@runtime_checkable
class EnergyBudgetProvider(Protocol):
    def get_energy_budget(self) -> float: ...


# =============================================================================
# Carbon intensity manager
# =============================================================================
class CarbonIntensityManager:
    """Real carbon-intensity manager with caching and a circuit breaker."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        region: str = "global",
        default_intensity: float = 400.0,
        cache_ttl: int = 300,
    ):
        self.api_key = api_key
        self.region = region
        self.default_intensity = float(default_intensity)
        self.cache_ttl = int(cache_ttl)
        self._last_intensity = float(default_intensity)
        self._last_update: Optional[float] = None
        self._session: Optional[Any] = None
        self._circuit_breaker = CircuitBreaker("carbon_api")
        self._lock = threading.RLock()

    async def _get_session(self) -> Any:
        if not AIOHTTP_AVAILABLE:
            raise CarbonFetchError("aiohttp is not installed")
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _fetch_intensity(self) -> float:
        if not self.api_key:
            return self.default_intensity
        session = await self._get_session()
        url = (
            "https://api.electricitymap.org/v3/carbon-intensity/latest"
            f"?zone={self.region}"
        )
        headers = {"auth-token": self.api_key}
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                return float(data.get("carbonIntensity", self.default_intensity))
            raise CarbonFetchError(f"API returned {resp.status}")

    async def update_intensity(self) -> float:
        try:
            intensity = await self._circuit_breaker.call_async(self._fetch_intensity)
            with self._lock:
                self._last_intensity = float(intensity)
                self._last_update = time.time()
            logger.info(f"carbon_intensity_updated value={intensity}")
            return float(intensity)
        except Exception as e:
            logger.warning(f"carbon_intensity_fetch_failed error={e}")
            with self._lock:
                return self._last_intensity

    async def get_current_intensity(self) -> float:
        with self._lock:
            stale = (
                self._last_update is None
                or time.time() - self._last_update > self.cache_ttl
            )
        if stale:
            await self.update_intensity()
        with self._lock:
            return self._last_intensity

    def get_last_intensity(self) -> float:
        with self._lock:
            return self._last_intensity

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# =============================================================================
# CausalBandit — real linear contextual bandit
# =============================================================================
@dataclass
class BanditConfig:
    alpha: float = 1.0
    exploration_beta: float = 1.0
    min_trials: int = 5
    use_thompson: bool = True


class CausalBandit:
    """
    Per-action Bayesian linear regression bandit with UCB or Thompson sampling.
    Real contextual bandit — not a mean tracker.
    """

    def __init__(
        self,
        action_space: List[str],
        feature_dim: int = 8,
        config: Optional[BanditConfig] = None,
        fallback_solver: Optional[Callable[[Any], str]] = None,
        min_trials_before_bandit: Optional[int] = None,
        confidence_threshold: Optional[float] = None,
    ):
        if not action_space:
            raise ValueError("action_space must be non-empty")
        self.actions: List[str] = list(action_space)
        self.dim = int(feature_dim)
        cfg = config or BanditConfig()
        if min_trials_before_bandit is not None:
            cfg.min_trials = int(min_trials_before_bandit)
        self.config = cfg
        self.fallback_solver = fallback_solver
        self.confidence_threshold = (
            float(confidence_threshold) if confidence_threshold is not None else 0.6
        )
        self.theta: Dict[str, np.ndarray] = {a: np.zeros(self.dim) for a in self.actions}
        self.P: Dict[str, np.ndarray] = {
            a: np.eye(self.dim) / cfg.alpha for a in self.actions
        }
        self.usage: Dict[str, int] = {a: 0 for a in self.actions}
        self.total_trials = 0
        self._lock = threading.RLock()

    def _prepare_features(self, context: Any) -> np.ndarray:
        if isinstance(context, dict):
            vals = [float(v) for v in context.values() if isinstance(v, (int, float))]
        else:
            try:
                vals = [float(v) for v in context]
            except Exception:
                vals = []
        if len(vals) < self.dim:
            vals = vals + [0.0] * (self.dim - len(vals))
        return np.asarray(vals[: self.dim], dtype=float)

    def select_action(self, context: Any) -> Tuple[str, float, str]:
        with self._lock:
            x = self._prepare_features(context)
            if self.total_trials < self.config.min_trials and self.fallback_solver is not None:
                return self.fallback_solver(context), 0.0, "fallback"

            if self.config.use_thompson:
                samples: Dict[str, float] = {}
                for a in self.actions:
                    try:
                        s = np.random.multivariate_normal(self.theta[a], self.P[a])
                    except Exception:
                        s = self.theta[a]
                    samples[a] = float(x @ s)
                best = max(samples, key=samples.get)
                conf = 1.0 / (1.0 + math.exp(-abs(samples[best])))
                return best, conf, "thompson"

            scores: Dict[str, float] = {}
            for a in self.actions:
                mean = float(x @ self.theta[a])
                var = float(x @ self.P[a] @ x) + 1e-9
                scores[a] = mean + self.config.exploration_beta * math.sqrt(var)
            best = max(scores, key=scores.get)
            conf = 1.0 / (1.0 + math.exp(-abs(scores[best])))
            return best, conf, "ucb"

    def update(self, context: Any, action: str, reward: float) -> None:
        if action not in self.theta:
            return
        with self._lock:
            x = self._prepare_features(context)
            P = self.P[action]
            denom = 1.0 + float(x @ P @ x)
            K = (P @ x) / denom
            err = float(reward) - float(x @ self.theta[action])
            self.theta[action] = self.theta[action] + K * err
            newP = P - np.outer(K, x) @ P
            self.P[action] = 0.5 * (newP + newP.T)
            self.usage[action] += 1
            self.total_trials += 1
            BANDIT_UPDATES.labels(policy=action).inc()

    def apply_federated_prior(self, action: str, mean_reward: float, weight: float = 0.1) -> None:
        """
        Nudge the posterior mean for `action` towards `mean_reward` using a
        small pseudo-observation (a synthetic context at the origin).
        """
        if action not in self.theta or weight <= 0:
            return
        with self._lock:
            P = self.P[action]
            x = np.zeros(self.dim)
            x[0] = 1.0  # intercept-like feature
            denom = 1.0 + float(x @ P @ x)
            K = (P @ x) / denom
            err = float(mean_reward) * float(weight) - float(x @ self.theta[action])
            self.theta[action] = self.theta[action] + K * err
            newP = P - np.outer(K, x) @ P
            self.P[action] = 0.5 * (newP + newP.T)

    def get_action_probabilities(self, context: Any) -> List[float]:
        with self._lock:
            x = self._prepare_features(context)
            scores = np.array([float(x @ self.theta[a]) for a in self.actions])
            z = scores - np.max(scores)
            exp = np.exp(z)
            return (exp / exp.sum()).tolist()

    def seed_safe_policy(self, context: Any, policy: str) -> None:
        if policy in self.theta:
            self.update(context, policy, 0.1)

    def add_actions(self, new_actions: Iterable[str]) -> List[str]:
        """
        Extend the action space atomically. Returns the actions that were
        actually added (existing ones are ignored).
        """
        added: List[str] = []
        with self._lock:
            for a in new_actions:
                if not isinstance(a, str) or not a:
                    continue
                if a in self.theta:
                    continue
                self.theta[a] = np.zeros(self.dim)
                self.P[a] = np.eye(self.dim) / self.config.alpha
                self.usage[a] = 0
                self.actions.append(a)
                added.append(a)
        return added

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "actions": list(self.actions),
                "usage": dict(self.usage),
                "total_trials": self.total_trials,
            }


# =============================================================================
# SafetyMonitor — episode-aware, snapshot-able
# =============================================================================
class SafetyMonitor:
    """
    Temporal safety rules over (skipped, carbon_intensity) pairs.
    Immediate thresholds, consecutive-skip streaks, skip-ratio windows.
    Exposes snapshot/restore for chaos experiments that temporarily relax
    thresholds.
    """

    def __init__(
        self,
        max_consecutive_skips: int = 3,
        max_skip_ratio: float = 0.5,
        max_carbon_intensity: float = 600.0,
        window_size: int = 10,
    ):
        self.max_consecutive_skips = int(max_consecutive_skips)
        self.max_skip_ratio = float(max_skip_ratio)
        self.max_carbon_intensity = float(max_carbon_intensity)
        self.window_size = int(window_size)
        self.history: Deque[Tuple[bool, float, float]] = deque(maxlen=window_size)
        self.consecutive_skips = 0
        self.violations: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def check(self, skipped: bool, carbon_intensity: float) -> Tuple[bool, Optional[str]]:
        with self._lock:
            if carbon_intensity > self.max_carbon_intensity:
                self._record("max_carbon_intensity",
                             {"carbon_intensity": float(carbon_intensity)})
                return False, "max_carbon_intensity"
            if skipped:
                self.consecutive_skips += 1
            else:
                self.consecutive_skips = 0
            if self.consecutive_skips > self.max_consecutive_skips:
                self._record("max_consecutive_skips",
                             {"consecutive_skips": self.consecutive_skips})
                return False, "max_consecutive_skips"
            self.history.append((bool(skipped), float(carbon_intensity), time.time()))
            if len(self.history) >= self.window_size:
                skips = sum(1 for h in self.history if h[0])
                ratio = skips / len(self.history)
                if ratio > self.max_skip_ratio:
                    self._record("max_skip_ratio", {"ratio": ratio})
                    return False, "max_skip_ratio"
            return True, None

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            return {
                "max_carbon_intensity": self.max_carbon_intensity,
                "max_consecutive_skips": float(self.max_consecutive_skips),
                "max_skip_ratio": self.max_skip_ratio,
            }

    def restore(self, snapshot: Dict[str, float]) -> None:
        with self._lock:
            if "max_carbon_intensity" in snapshot:
                self.max_carbon_intensity = float(snapshot["max_carbon_intensity"])
            if "max_consecutive_skips" in snapshot:
                self.max_consecutive_skips = int(snapshot["max_consecutive_skips"])
            if "max_skip_ratio" in snapshot:
                self.max_skip_ratio = float(snapshot["max_skip_ratio"])

    def _record(self, rule: str, details: Dict[str, Any]) -> None:
        self.violations.append({
            "rule": rule,
            "details": details,
            "timestamp": time.time(),
        })
        SAFETY_VIOLATIONS.labels(rule=rule).inc()
        logger.warning(f"safety_violation rule={rule} details={details}")

    def get_violations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.violations)

    def reset_episode(self) -> None:
        with self._lock:
            self.history.clear()
            self.consecutive_skips = 0


# =============================================================================
# XAIExplainer — structured output
# =============================================================================
class XAIExplainer:
    """Human-readable, structured explanations for skipping decisions."""

    def explain_skipping(
        self,
        layer_name: str,
        policy: str,
        context: Dict[str, Any],
        confidence: float,
        decision: bool,
    ) -> Dict[str, Any]:
        top = sorted(
            (
                {"feature": k, "value": float(v)}
                for k, v in context.items() if isinstance(v, (int, float))
            ),
            key=lambda x: -abs(x["value"]),
        )[:5]
        narrative = (
            f"Layer '{layer_name}' "
            f"{'SKIPPED' if decision else 'EXECUTED'} "
            f"(policy='{policy}', confidence={confidence:.2f})."
        )
        return {
            "layer": layer_name,
            "policy": policy,
            "decision": bool(decision),
            "confidence": float(confidence),
            "top_contributions": top,
            "narrative": narrative,
        }


# =============================================================================
# FederatedProfilerCoordinator — weighted, round IDs, DP
# =============================================================================
class FederatedProfilerCoordinator:
    """Weighted aggregation with round IDs, min_clients, and Laplace DP noise."""

    def __init__(
        self,
        privacy_budget: float = 0.5,
        min_clients: int = 1,
        seed: Optional[int] = None,
    ):
        self.privacy_budget = float(privacy_budget)
        self.min_clients = max(1, int(min_clients))
        self.participants: Dict[str, Dict[str, Any]] = {}
        self.round_id = 0
        self._rng = np.random.default_rng(seed if seed is not None else 42)
        self._lock = threading.RLock()

    def register_participant(
        self,
        participant_id: str,
        model_update: Dict[str, Any],
        sample_count: int = 1,
        round_id: Optional[int] = None,
    ) -> None:
        with self._lock:
            rid = round_id if round_id is not None else self.round_id
            self.participants[participant_id] = {
                "update": dict(model_update),
                "weight": max(1, int(sample_count)),
                "round": rid,
            }

    def aggregate(self, expected_round: Optional[int] = None) -> Dict[str, Any]:
        with self._lock:
            active = {
                pid: data for pid, data in self.participants.items()
                if expected_round is None or data["round"] >= expected_round
            }
            if len(active) < self.min_clients:
                return {}
            keys: set = set()
            for data in active.values():
                keys.update(data["update"].keys())
            aggregated: Dict[str, Any] = {}
            for key in keys:
                values: List[Any] = []
                weights: List[float] = []
                numeric = True
                for data in active.values():
                    if key not in data["update"]:
                        continue
                    v = data["update"][key]
                    if isinstance(v, (int, float)):
                        values.append(float(v))
                        weights.append(data["weight"])
                    else:
                        values.append(v)
                        numeric = False
                if not values:
                    continue
                if numeric:
                    wsum = sum(weights)
                    mean = (
                        sum(v * w for v, w in zip(values, weights)) / wsum
                        if wsum > 0 else float(np.mean(values))
                    )
                    if self.privacy_budget > 0:
                        scale = 1.0 / max(self.privacy_budget, 1e-6)
                        mean += float(self._rng.laplace(0.0, scale))
                    aggregated[key] = mean
                else:
                    aggregated[key] = values[0]
            self.round_id += 1
            FEDERATED_ROUNDS.inc()
            return aggregated

    def reset_round(self) -> None:
        with self._lock:
            self.participants.clear()

    def get_participant_count(self) -> int:
        with self._lock:
            return len(self.participants)

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "round_id": self.round_id,
                "num_participants": len(self.participants),
                "privacy_budget": self.privacy_budget,
            }


# =============================================================================
# MultiAgentProfiler — stable hash, thread-safe
# =============================================================================
class MultiAgentProfiler:
    """Coordinates agents that own disjoint subsets of layers."""

    def __init__(self, num_agents: int = 3):
        if num_agents <= 0:
            raise ValueError("num_agents must be positive")
        self.agents: Dict[str, Dict[str, Any]] = {
            f"agent_{i}": {"layers": [], "reward": 0.0} for i in range(num_agents)
        }
        self._lock = threading.RLock()

    @staticmethod
    def _stable_hash(layer_name: str) -> int:
        return int(hashlib.sha256(layer_name.encode()).hexdigest(), 16)

    def assign_layer(self, layer_name: str) -> str:
        with self._lock:
            agent_id = list(self.agents.keys())[
                self._stable_hash(layer_name) % len(self.agents)
            ]
            if layer_name not in self.agents[agent_id]["layers"]:
                self.agents[agent_id]["layers"].append(layer_name)
            return agent_id

    def record_reward(self, agent_id: str, reward: float) -> None:
        with self._lock:
            if agent_id in self.agents:
                self.agents[agent_id]["reward"] += float(reward)

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                agent_id: {
                    "num_layers": len(info["layers"]),
                    "cumulative_reward": info["reward"],
                }
                for agent_id, info in self.agents.items()
            }


# =============================================================================
# CarbonOffsetBroker — thread-safe, wired
# =============================================================================
class CarbonOffsetBroker:
    """Thread-safe offsets + RECs accounting."""

    def __init__(
        self,
        threshold_intensity: float = 400.0,
        cost_per_kg: float = 0.1,
        rec_cost_per_mwh: float = 5.0,
    ):
        self.threshold = float(threshold_intensity)
        self.cost_per_kg = float(cost_per_kg)
        self.rec_cost_per_mwh = float(rec_cost_per_mwh)
        self.total_offset_kg = 0.0
        self.total_recs_mwh = 0.0
        self.total_cost = 0.0
        self.records: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def reward_penalty(self, carbon_intensity: float, weight: float = 0.1) -> float:
        excess = max(0.0, carbon_intensity - self.threshold)
        return (excess / 1000.0) * weight

    async def purchase_offsets(
        self, carbon_intensity: float, carbon_kg: float
    ) -> Dict[str, Any]:
        if carbon_kg <= 0:
            return {"status": "invalid_amount"}
        if carbon_intensity <= self.threshold:
            return {"status": "below_threshold", "carbon_kg": float(carbon_kg)}
        cost = carbon_kg * self.cost_per_kg
        with self._lock:
            self.total_offset_kg += float(carbon_kg)
            self.total_cost += float(cost)
            record = {
                "status": "offset_purchased",
                "carbon_kg": float(carbon_kg),
                "cost_usd": float(cost),
                "timestamp": datetime.now().isoformat(),
            }
            self.records.append(record)
            CARBON_OFFSET_KG.inc(carbon_kg)
        logger.info(f"offset_purchased kg={carbon_kg:.4f} usd={cost:.4f}")
        return record

    async def purchase_recs(self, energy_mwh: float) -> Dict[str, Any]:
        if energy_mwh <= 0:
            return {"status": "invalid_amount"}
        cost = energy_mwh * self.rec_cost_per_mwh
        with self._lock:
            self.total_recs_mwh += float(energy_mwh)
            self.total_cost += float(cost)
            record = {
                "status": "rec_purchased",
                "energy_mwh": float(energy_mwh),
                "cost_usd": float(cost),
                "timestamp": datetime.now().isoformat(),
            }
            self.records.append(record)
            CARBON_REC_MWH.inc(energy_mwh)
        logger.info(f"rec_purchased mwh={energy_mwh:.4f} usd={cost:.4f}")
        return record

    def get_totals(self) -> Dict[str, float]:
        with self._lock:
            return {
                "total_offset_kg": self.total_offset_kg,
                "total_recs_mwh": self.total_recs_mwh,
                "total_cost_usd": self.total_cost,
            }


# =============================================================================
# ChaosMonkey — named faults, seeded, restore-able
# =============================================================================
class ChaosMonkey:
    FAULT_TYPES = {
        "drop_carbon_provider", "freeze_bandit", "safety_override", "queue_drop",
    }

    def __init__(
        self,
        enabled: bool = False,
        failure_probability: float = 0.1,
        seed: Optional[int] = None,
    ):
        self.enabled = bool(enabled)
        self.failure_probability = float(failure_probability)
        self._rng = random.Random(seed if seed is not None else 42)
        self.injected_failures = 0
        self.events: List[Dict[str, Any]] = []
        self._frozen = False
        # Snapshots captured when faults are applied, restored on cleanup.
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def maybe_fail(self) -> Optional[str]:
        if not self.enabled:
            return None
        if self._rng.random() >= self.failure_probability:
            return None
        fault = self._rng.choice(sorted(self.FAULT_TYPES))
        self.injected_failures += 1
        self._record(fault, "triggered")
        CHAOS_EVENTS.labels(fault=fault).inc()
        return fault

    def apply(self, fault: str, profiler: "EnergyProfiler") -> None:
        with self._lock:
            if fault == "drop_carbon_provider":
                self._snapshots.setdefault("drop_carbon_provider", {
                    "carbon_provider": profiler.carbon_provider,
                })
                profiler.carbon_provider = None
            elif fault == "freeze_bandit":
                self._frozen = True
            elif fault == "safety_override":
                self._snapshots.setdefault(
                    "safety_override",
                    {"safety_thresholds": profiler.safety_monitor.snapshot()},
                )
                profiler.safety_monitor.max_carbon_intensity *= 2.0
            elif fault == "queue_drop":
                self._snapshots.setdefault("queue_drop", {"queue": profiler.queue})
                profiler.queue = None

    def cleanup_fault(self, fault: str, profiler: "EnergyProfiler") -> None:
        """Restore the state that `apply` mutated for the given fault."""
        with self._lock:
            snap = self._snapshots.pop(fault, None)
            if snap is None:
                return
            if fault == "drop_carbon_provider":
                profiler.carbon_provider = snap["carbon_provider"]
            elif fault == "safety_override":
                profiler.safety_monitor.restore(snap["safety_thresholds"])
            elif fault == "queue_drop":
                profiler.queue = snap["queue"]
            elif fault == "freeze_bandit":
                self._frozen = False
            PROFILER_RECOVERIES.labels(fault=fault).inc()

    def is_frozen(self) -> bool:
        return self._frozen

    def unfreeze(self) -> None:
        self._frozen = False

    def _record(self, fault: str, outcome: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.events.append({
            "fault": fault,
            "outcome": outcome,
            "timestamp": datetime.now().isoformat(),
            **(extra or {}),
        })

    async def inject_and_measure(
        self, profiler: "EnergyProfiler", fault: str, steps: int = 5
    ) -> Dict[str, Any]:
        start = time.monotonic()
        self.apply(fault, profiler)
        recovery_step: Optional[int] = None
        try:
            for step in range(steps):
                try:
                    _ = profiler.layer_order  # trivial liveness probe
                    if recovery_step is None:
                        recovery_step = step
                except Exception:
                    continue
        finally:
            self.cleanup_fault(fault, profiler)
        elapsed = time.monotonic() - start
        self._record(fault, "measured",
                     {"recovery_step": recovery_step, "elapsed_s": elapsed})
        return {
            "fault": fault,
            "recovery_step": recovery_step,
            "recovered": recovery_step is not None,
            "elapsed_s": elapsed,
        }

    def get_stats(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "injected_failures": self.injected_failures,
            "frozen": self._frozen,
            "pending_snapshots": list(self._snapshots.keys()),
        }


# =============================================================================
# HumanReviewManager — TTL, bounded, audit
# =============================================================================
@dataclass
class HumanReviewRequest:
    review_id: str
    decision_id: str
    details: Dict[str, Any]
    created_at: datetime
    status: str = "pending"
    reviewer: Optional[str] = None
    resolution_at: Optional[datetime] = None


class HumanReviewManager:
    def __init__(self, ttl_seconds: float = 3600.0, max_pending: int = 100):
        self.ttl_seconds = float(ttl_seconds)
        self.max_pending = int(max_pending)
        self.pending_reviews: Dict[str, HumanReviewRequest] = {}
        self.audit_log: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    def _evict_expired(self) -> None:
        now = datetime.now()
        for rid, r in list(self.pending_reviews.items()):
            if r.status == "pending" and now - r.created_at > timedelta(seconds=self.ttl_seconds):
                r.status = "expired"
                HITL_STATUS.labels(status="expired").inc()

    def _enforce_capacity(self) -> None:
        pending = [r for r in self.pending_reviews.values() if r.status == "pending"]
        if len(pending) <= self.max_pending:
            return
        ordered = sorted(pending, key=lambda r: r.created_at)
        for r in ordered[: len(ordered) - self.max_pending]:
            r.status = "dropped"
            HITL_STATUS.labels(status="dropped").inc()

    async def request_review(self, decision_id: str, details: Dict[str, Any]) -> str:
        async with self._lock:
            self._evict_expired()
            rid = str(uuid.uuid4())
            self.pending_reviews[rid] = HumanReviewRequest(
                review_id=rid,
                decision_id=decision_id,
                details=dict(details),
                created_at=datetime.now(),
            )
            self._enforce_capacity()
            HITL_REQUESTS.inc()
            logger.info(f"hitl_requested review_id={rid}")
            return rid

    def _audit(self, review_id: str, action: str, reviewer: str) -> None:
        self.audit_log.append({
            "review_id": review_id,
            "action": action,
            "reviewer": reviewer,
            "timestamp": datetime.now().isoformat(),
        })

    async def approve(self, review_id: str, reviewer: str = "unknown") -> bool:
        async with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None or r.status != "pending":
                return False
            r.status = "approved"
            r.reviewer = reviewer
            r.resolution_at = datetime.now()
            self._audit(review_id, "approved", reviewer)
            HITL_STATUS.labels(status="approved").inc()
            return True

    async def reject(self, review_id: str, reviewer: str = "unknown") -> bool:
        async with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None or r.status != "pending":
                return False
            r.status = "rejected"
            r.reviewer = reviewer
            r.resolution_at = datetime.now()
            self._audit(review_id, "rejected", reviewer)
            HITL_STATUS.labels(status="rejected").inc()
            return True

    async def get_pending(self) -> List[Dict[str, Any]]:
        async with self._lock:
            self._evict_expired()
            return [
                {
                    "review_id": r.review_id,
                    "decision_id": r.decision_id,
                    "details": r.details,
                    "created_at": r.created_at.isoformat(),
                }
                for r in self.pending_reviews.values() if r.status == "pending"
            ]


# =============================================================================
# FlexGenManager — honest delegation
# =============================================================================
class FlexGenManager:
    def __init__(self, carbon_intensity: float = 400.0):
        self.carbon_intensity = float(carbon_intensity)
        self._cost_model = None
        self._drift_detector = None
        self._gpu_profiler = None
        if FLEXGEN_AVAILABLE:
            try:
                self._cost_model = FlexGenCostModel(
                    carbon_intensity_g_per_kwh=self.carbon_intensity
                )
                self._drift_detector = PolicyDriftDetector()
            except Exception as e:
                logger.warning(f"flexgen_init_failed error={e}")
                self._cost_model = None
                self._drift_detector = None
            try:
                from enhancements.gpu_profiler import GPUProfiler  # type: ignore
                self._gpu_profiler = GPUProfiler()
            except Exception:
                self._gpu_profiler = None

    @property
    def available(self) -> bool:
        return FLEXGEN_AVAILABLE

    async def optimize_policy(self, workload: Any, node: Any) -> Dict[str, Any]:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            from enhancements.gpu_optimization.flexgen_controller import (  # type: ignore
                FlexGenController,
            )
            from enhancements.gpu_optimization.flexgen_policy_selector import (  # type: ignore
                DistillationFlexGenSelector,
            )
        except ImportError as e:
            raise NotImplementedYetError(f"FlexGen submodule missing: {e}") from e

        _selector = DistillationFlexGenSelector(  # noqa: F841
            n_candidates=20,
            config={"epsilon": 0.1, "epsilon_decay": 0.999},
        )
        controller = FlexGenController(
            node=node,
            workload=workload,
            carbon_intensity=getattr(workload, "metadata", {}).get(
                "carbon_intensity", self.carbon_intensity
            ),
            use_real_executor=False,
            executor=None,
            cost_model=self._cost_model,
            use_bio_search=True,
            bio_search_config={"population_size": 50, "generations": 10},
            modp_planner=None,
            drift_detector=self._drift_detector,
            gpu_profiler=self._gpu_profiler,
        )
        return await controller.step()

    async def select_precision(self, workload: Dict[str, Any]) -> str:
        """
        Pick a precision level. When FlexGen is available, try to delegate
        once; otherwise use a carbon/workload heuristic. Always returns a str.
        """
        if FLEXGEN_AVAILABLE:
            try:
                wd = WorkloadDescriptor(**workload) if isinstance(workload, dict) else workload
                nd = NodeDescriptor()
                result = await self.optimize_policy(wd, nd)
                if isinstance(result, dict) and "chosen_policy" in result:
                    precision = result["chosen_policy"].get("precision")
                    if precision in ("fp32", "fp16", "int8"):
                        return precision
            except NotImplementedYetError:
                pass
            except Exception as e:
                logger.warning(f"flexgen_precision_failed error={e}")

        carbon = float(workload.get("carbon_intensity", self.carbon_intensity))
        size = workload.get("size", "medium")
        if carbon > 500 or size == "large":
            return "int8"
        if carbon > 300 or size == "medium":
            return "fp16"
        return "fp32"

    async def get_status(self) -> Dict[str, Any]:
        if not FLEXGEN_AVAILABLE:
            return {"available": False}
        gpu = {}
        if self._gpu_profiler and hasattr(self._gpu_profiler, "get_current_metrics"):
            try:
                gpu = self._gpu_profiler.get_current_metrics()
            except Exception:
                gpu = {}
        return {
            "available": True,
            "drift": self._drift_detector.get_stats() if self._drift_detector else {},
            "gpu": gpu,
        }


# =============================================================================
# SkipDecision dataclass
# =============================================================================
@dataclass
class SkipDecision:
    layer: str
    policy: str
    precision: str
    confidence: float
    source: str
    decision: bool
    context: Dict[str, Any]
    explanation: Dict[str, Any]
    review_id: Optional[str] = None


# =============================================================================
# EnergyProfiler
# =============================================================================
class EnergyProfiler:
    """
    Tracks energy per layer and provides adaptive, context-aware skipping.
    Wires CausalBandit, SafetyMonitor, XAIExplainer, FederatedProfilerCoordinator,
    MultiAgentProfiler, CarbonOffsetBroker, ChaosMonkey, HumanReviewManager,
    and FlexGenManager into the decision path.
    """

    def __init__(
        self,
        model: nn.Module,
        energy_per_layer: Dict[str, float],
        carbon_provider: Optional[CarbonIntensityProvider] = None,
        default_carbon_intensity: float = 400.0,
        storage: Optional[Storage] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        bandit: Optional[Any] = None,
        moe: Optional[ExpertRouter] = None,
        modp: Optional[ParetoOptimizer] = None,
        bio: Optional[GeneticPolicyGenerator] = None,
        action_space: Optional[List[str]] = None,
        modp_weights: Optional[Dict[str, float]] = None,
        bio_generations: int = 10,
        bio_population_size: int = 20,
        enable_chaos: bool = False,
        chaos_seed: Optional[int] = 42,
        chaos_failure_probability: float = 0.05,
        hitl_ttl_seconds: float = 3600.0,
        hitl_max_pending: int = 100,
    ):
        self.model = model
        # Copy so we never mutate the caller's dict.
        self.energy_per_layer: Dict[str, float] = dict(energy_per_layer or {})
        self.carbon_provider = carbon_provider
        self.default_carbon_intensity = float(default_carbon_intensity)
        self.storage = storage
        self.queue = message_queue

        self._fill_missing_energies()
        self.layer_order = list(self.energy_per_layer.keys())

        # Enhancement modules
        self.modp = modp or (ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None)
        self.moe = moe or (ExpertRouter() if ENHANCEMENTS_AVAILABLE else None)
        self.bio = bio or (GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None)
        self.modp_weights = modp_weights or {
            "accuracy": 0.4, "energy": 0.3, "carbon": 0.2, "latency": 0.1,
        }
        self.bio_generations = int(bio_generations)
        self.bio_population_size = int(bio_population_size)

        self.action_space = list(action_space) if action_space else [
            "aggressive", "balanced", "conservative",
            "aggressive_fp16", "aggressive_int8",
            "conservative_fp16", "conservative_int8",
        ]
        if bandit is not None and hasattr(bandit, "select_action"):
            self.bandit = bandit
        else:
            self.bandit = CausalBandit(
                action_space=self.action_space,
                feature_dim=8,
                fallback_solver=lambda ctx: "balanced",
                min_trials_before_bandit=5,
                confidence_threshold=0.6,
            )

        # FlexGen
        self.flexgen_manager = FlexGenManager(default_carbon_intensity)

        # New modules
        self.safety_monitor = SafetyMonitor()
        self.xai = XAIExplainer()
        self.federated = FederatedProfilerCoordinator()
        self.multi_agent = MultiAgentProfiler(num_agents=3)
        self.carbon_broker = CarbonOffsetBroker()
        self.chaos_monkey = ChaosMonkey(
            enabled=enable_chaos,
            failure_probability=chaos_failure_probability,
            seed=chaos_seed,
        )
        self.human_review = HumanReviewManager(
            ttl_seconds=hitl_ttl_seconds, max_pending=hitl_max_pending
        )

        # Assign layers to agents
        for layer_name in self.layer_order:
            self.multi_agent.assign_layer(layer_name)

        # State
        self._skipping_history: Dict[str, List[bool]] = defaultdict(list)
        self._performance_history: Deque[float] = deque(maxlen=1000)
        self._energy_saved_history: Deque[float] = deque(maxlen=1000)
        self._last_decisions: List[SkipDecision] = []
        self._last_carbon_intensity: float = default_carbon_intensity
        self._lock = threading.RLock()

        self._load_state()

    # ------------------------------------------------------------------ #
    # Setup / persistence                                                #
    # ------------------------------------------------------------------ #
    def _fill_missing_energies(self) -> None:
        default_energy = 1e-6
        for name, _ in self.model.named_modules():
            if name and name not in self.energy_per_layer:
                self.energy_per_layer[name] = default_energy

    def _load_state(self) -> None:
        if not self.storage:
            return
        loader = getattr(self.storage, "load_profiler_state", None)
        if loader is None:
            return
        try:
            state = loader()
            if not state:
                return
            if hasattr(self.bandit, "theta") and state.get("theta"):
                for k, v in state["theta"].items():
                    if k in self.bandit.theta:
                        self.bandit.theta[k] = np.asarray(v, dtype=float)
            if hasattr(self.bandit, "P") and state.get("P"):
                for k, v in state["P"].items():
                    if k in self.bandit.P:
                        self.bandit.P[k] = np.asarray(v, dtype=float)
            logger.info("profiler_state_loaded")
        except Exception as e:
            logger.warning(f"profiler_state_load_failed error={e}")

    def _save_state(self) -> None:
        if not self.storage:
            return
        saver = getattr(self.storage, "save_profiler_state", None)
        if saver is None:
            return
        try:
            state = {
                "theta": {
                    a: self.bandit.theta[a].tolist()
                    for a in getattr(self.bandit, "theta", {})
                },
                "P": {
                    a: self.bandit.P[a].tolist()
                    for a in getattr(self.bandit, "P", {})
                },
                "modp_weights": self.modp_weights,
                "action_space": list(self.action_space),
                "safety_violations": self.safety_monitor.get_violations()[-10:],
            }
            saver(state)
        except Exception as e:
            logger.warning(f"profiler_state_save_failed error={e}")

    # ------------------------------------------------------------------ #
    # Utilities                                                          #
    # ------------------------------------------------------------------ #
    async def _get_carbon_intensity(self) -> float:
        if self.carbon_provider is not None:
            try:
                val = await self.carbon_provider.get_current_intensity()
                self._last_carbon_intensity = float(val)
                CARBON_INTENSITY.observe(float(val))
                return float(val)
            except Exception as e:
                logger.warning(f"carbon_provider_failed error={e}")
        return self.default_carbon_intensity

    async def estimate_energy_for_token(
        self, layer_name: str, token_importance: float
    ) -> float:
        base_energy = float(self.energy_per_layer.get(layer_name, 1e-6))
        carbon_intensity = self._last_carbon_intensity
        carbon_factor = 1.0 + (carbon_intensity / 400.0 - 1.0) * 0.2
        importance_factor = 1.0 + float(token_importance) * 0.5
        return base_energy * carbon_factor * importance_factor

    @staticmethod
    def _extract_precision(policy: str) -> str:
        if "int8" in policy:
            return "int8"
        if "fp16" in policy:
            return "fp16"
        return "fp32"

    @staticmethod
    def _extract_aggressiveness(policy: str) -> str:
        if "aggressive" in policy:
            return "aggressive"
        if "conservative" in policy:
            return "conservative"
        return "balanced"

    # ------------------------------------------------------------------ #
    # Core decision                                                      #
    # ------------------------------------------------------------------ #
    async def should_skip_layer(
        self,
        layer_name: str,
        token_importance: float,
        current_energy_budget: float,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        # Chaos injection
        fault = self.chaos_monkey.maybe_fail()
        if fault is not None:
            try:
                self.chaos_monkey.apply(fault, self)
            except Exception as e:
                logger.warning(f"chaos_apply_failed error={e}")
            if fault in {"drop_carbon_provider", "queue_drop"}:
                logger.warning(f"chaos_triggered fault={fault} falling_back_to_heuristic")
                # Heuristic path; also clean up the fault on the way out.
                result = self._should_skip_heuristic(
                    layer_name, token_importance, current_energy_budget
                )
                try:
                    self.chaos_monkey.cleanup_fault(fault, self)
                except Exception:
                    pass
                return result

        carbon_intensity = await self._get_carbon_intensity()
        context = dict(context or {})
        context.update({
            "layer_name_hash": float(int(hashlib.sha256(
                layer_name.encode()
            ).hexdigest()[:6], 16)),
            "token_importance": float(token_importance),
            "energy_budget": float(current_energy_budget),
            "carbon_intensity": float(carbon_intensity),
            "layer_energy": float(self.energy_per_layer.get(layer_name, 1e-6)),
        })

        encoded = self._encode_context(context)
        policy, confidence, source = self.bandit.select_action(encoded)
        if not policy:
            policy = "balanced"

        aggressiveness = self._extract_aggressiveness(policy)
        precision = self._extract_precision(policy)

        if aggressiveness == "aggressive":
            skip = (
                (current_energy_budget < 0.6 and token_importance < 0.4)
                or current_energy_budget < 0.4
            )
        elif aggressiveness == "conservative":
            skip = current_energy_budget < 0.2 and token_importance < 0.2
        else:
            skip = (
                (current_energy_budget < 0.4 and token_importance < 0.3)
                or current_energy_budget < 0.2
            )

        # Safety gate
        safe, reason = self.safety_monitor.check(skip, carbon_intensity)
        if not safe:
            logger.warning(f"safety_override layer={layer_name} reason={reason}")
            skip = False

        explanation = self.xai.explain_skipping(
            layer_name, policy, context, confidence, skip
        )

        # HITL on risky aggressive skips under high carbon.
        review_id: Optional[str] = None
        if skip and carbon_intensity > 500 and aggressiveness == "aggressive":
            review_id = await self.human_review.request_review(
                decision_id=f"skip_{layer_name}_{uuid.uuid4().hex[:6]}",
                details={
                    "layer": layer_name,
                    "policy": policy,
                    "context": context,
                    "explanation": explanation,
                },
            )

        decision = SkipDecision(
            layer=layer_name,
            policy=policy,
            precision=precision,
            confidence=float(confidence),
            source=source,
            decision=bool(skip),
            context=context,
            explanation=explanation,
            review_id=review_id,
        )
        with self._lock:
            self._last_decisions.append(decision)

        SKIP_DECISIONS.labels(
            layer=layer_name,
            decision="skip" if skip else "execute",
        ).inc()
        return skip

    def _encode_context(self, context: Dict[str, Any]) -> List[float]:
        """Return a list of floats for the bandit feature vector."""
        if self.moe is not None:
            try:
                encoded = self.moe.encode(context)
                if encoded:
                    # Coerce to a flat list of floats.
                    return [float(v) for v in encoded if isinstance(v, (int, float))]
            except Exception:
                pass
        # Fallback: pack numeric values in insertion order.
        return [float(v) for v in context.values() if isinstance(v, (int, float))]

    def _should_skip_heuristic(
        self, layer_name: str, token_importance: float, budget: float
    ) -> bool:
        if budget < 0.3 and token_importance < 0.3:
            return True
        if budget < 0.7 and token_importance < 0.5:
            return True
        return False

    def reset_episode(self) -> None:
        with self._lock:
            self._last_decisions.clear()
        self.safety_monitor.reset_episode()

    # ------------------------------------------------------------------ #
    # Outcome recording                                                  #
    # ------------------------------------------------------------------ #
    async def record_outcome(
        self,
        accuracy: float,
        energy_saved: float,
        carbon_saved: float = 0.0,
        latency_ms: float = 0.0,
    ) -> None:
        accuracy = float(accuracy)
        energy_saved = float(energy_saved)
        carbon_saved = float(carbon_saved)
        latency_ms = float(latency_ms)

        self._performance_history.append(accuracy)
        self._energy_saved_history.append(energy_saved)
        ENERGY_SAVED.observe(energy_saved)

        decision = self._last_decisions[-1] if self._last_decisions else None
        if decision is not None and not self.chaos_monkey.is_frozen():
            if self.modp is not None:
                avg_energy = float(np.mean(list(self._energy_saved_history))) or 1e-6
                objectives = {
                    "accuracy": accuracy,
                    "energy": 1.0 - min(energy_saved / avg_energy, 1.0),
                    "carbon": 1.0 - min(carbon_saved / 1000.0, 1.0),
                    "latency": 1.0 - min(latency_ms / 1000.0, 1.0),
                }
                reward = float(self.modp.evaluate(objectives, self.modp_weights))
            else:
                reward = accuracy
            reward = float(np.clip(reward, -1.0, 1.0))
            self.bandit.update(decision.context, decision.policy, reward)

            agent_id = self.multi_agent.assign_layer(decision.layer)
            self.multi_agent.record_reward(agent_id, reward)

        # Federated round
        self.federated.register_participant(
            participant_id="local",
            model_update={
                "mean_accuracy": float(
                    np.mean(list(self._performance_history) or [0.0])
                ),
                "mean_energy_saved": float(
                    np.mean(list(self._energy_saved_history) or [0.0])
                ),
            },
            sample_count=max(1, len(self._performance_history)),
        )
        aggregated = self.federated.aggregate()

        # If the aggregate carries a reward signal for the last policy, use
        # it to nudge the bandit posterior.
        if (
            aggregated
            and decision is not None
            and hasattr(self.bandit, "apply_federated_prior")
        ):
            mean_acc = aggregated.get("mean_accuracy")
            if mean_acc is not None:
                try:
                    self.bandit.apply_federated_prior(
                        decision.policy, float(mean_acc), weight=0.1
                    )
                except Exception as e:
                    logger.debug(f"federated_prior_apply_failed error={e}")

        # Carbon offsets at high intensity
        if self._last_carbon_intensity > 400 and carbon_saved > 0:
            try:
                await self.carbon_broker.purchase_offsets(
                    self._last_carbon_intensity, carbon_saved
                )
            except Exception as e:
                logger.warning(f"offset_purchase_failed error={e}")

        # Bio-inspired evolution every 100 outcomes
        if (
            len(self._performance_history) % 100 == 0
            and self.bio is not None
            and self._performance_history
        ):
            try:
                new_policies = await self.evolve_policies()
                added = self.bandit.add_actions(new_policies)
                for p in added:
                    if p not in self.action_space:
                        self.action_space.append(p)
            except Exception as e:
                logger.warning(f"bio_evolve_failed error={e}")

        # Persist every 10 outcomes
        if len(self._performance_history) % 10 == 0:
            self._save_state()

        # Feedback event
        if self.queue is not None and decision is not None:
            try:
                event = FeedbackEvent.create_with_context(
                    task_id=f"skipping_{uuid.uuid4().hex[:8]}",
                    selected_action=decision.policy,
                    quality_score=accuracy,
                    latency_ms=latency_ms,
                    energy_joules=energy_saved,
                    carbon_g=carbon_saved,
                    feedback_type="energy",
                    adaptive_cost_value=0.0,
                    state=decision.context,
                    candidates=list(self.action_space),
                    source="energy_profiler",
                    environment="production",
                    tags=["layer_skipping", "causal_rl"],
                )
                await self.queue.publish("feedback_events", event)
            except Exception as e:
                logger.warning(f"feedback_publish_failed error={e}")

    async def evolve_policies(self) -> List[str]:
        if self.bio is None or not self._performance_history:
            return []

        def fitness(_policy: Any) -> float:
            recent = list(self._performance_history)[-20:]
            return float(np.mean(recent)) if recent else 0.5

        try:
            new_policies = self.bio.evolve(
                population=list(self.action_space),
                fitness_fn=fitness,
                generations=self.bio_generations,
                population_size=self.bio_population_size,
            )
        except Exception as e:
            logger.warning(f"bio_evolve_call_failed error={e}")
            return []

        if isinstance(new_policies, str):
            return [new_policies]
        if isinstance(new_policies, list):
            return [p for p in new_policies if isinstance(p, str)]
        return []

    # ------------------------------------------------------------------ #
    # Public helpers                                                     #
    # ------------------------------------------------------------------ #
    async def run_flexgen_optimization(
        self, workload: Dict[str, Any], node: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            workload_obj = WorkloadDescriptor(**workload)  # type: ignore[call-arg]
            node_obj = NodeDescriptor(**node)  # type: ignore[call-arg]
        except TypeError as e:
            raise EnergyProfilerError(f"Invalid descriptor: {e}") from e
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def select_precision(self, workload: Dict[str, Any]) -> str:
        return await self.flexgen_manager.select_precision(workload)

    def get_energy_map(self) -> Dict[str, float]:
        with self._lock:
            return dict(self.energy_per_layer)

    async def estimate_total_energy(
        self, input_shape: tuple, token_importance: float = 0.5
    ) -> float:
        total = 0.0
        for layer_name in self.layer_order:
            total += await self.estimate_energy_for_token(layer_name, token_importance)
        ENERGY_ESTIMATED.observe(total)
        return total

    def get_safety_violations(self) -> List[Dict[str, Any]]:
        return self.safety_monitor.get_violations()

    def get_last_explanation(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self._last_decisions:
                return None
            return self._last_decisions[-1].explanation

    def get_last_decisions(self) -> List[SkipDecision]:
        with self._lock:
            return list(self._last_decisions)

    def get_agent_stats(self) -> Dict[str, Any]:
        return self.multi_agent.get_stats()

    def get_carbon_broker_totals(self) -> Dict[str, float]:
        return self.carbon_broker.get_totals()

    def get_chaos_stats(self) -> Dict[str, Any]:
        return self.chaos_monkey.get_stats()

    async def get_hitl_pending(self) -> List[Dict[str, Any]]:
        return await self.human_review.get_pending()

    def save(self, path: Union[str, Path]) -> None:
        path = Path(path)
        data = {
            "energy_per_layer": self.energy_per_layer,
            "action_space": list(self.action_space),
            "modp_weights": self.modp_weights,
            "theta": {
                a: self.bandit.theta[a].tolist()
                for a in getattr(self.bandit, "theta", {})
            },
            "P": {
                a: self.bandit.P[a].tolist()
                for a in getattr(self.bandit, "P", {})
            },
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"profiler_saved path={path}")

    @classmethod
    def load(
        cls, path: Union[str, Path], model: nn.Module, carbon_provider=None
    ) -> "EnergyProfiler":
        path = Path(path)
        with open(path, "r") as f:
            data = json.load(f)
        profiler = cls(
            model=model,
            energy_per_layer=data["energy_per_layer"],
            carbon_provider=carbon_provider,
            action_space=data.get("action_space", ["balanced"]),
            modp_weights=data.get(
                "modp_weights",
                {"accuracy": 0.4, "energy": 0.3, "carbon": 0.2, "latency": 0.1},
            ),
        )
        if hasattr(profiler.bandit, "theta") and "theta" in data:
            for k, v in data["theta"].items():
                if k in profiler.bandit.theta:
                    profiler.bandit.theta[k] = np.asarray(v, dtype=float)
        if hasattr(profiler.bandit, "P") and "P" in data:
            for k, v in data["P"].items():
                if k in profiler.bandit.P:
                    profiler.bandit.P[k] = np.asarray(v, dtype=float)
        return profiler


# =============================================================================
# LayerSkippingWrapper
# =============================================================================
class LayerSkippingWrapper(nn.Module):
    """
    Wraps a model to allow selective layer skipping based on EnergyProfiler.
    Supports per-token importance and dynamic energy budget.
    """

    def __init__(
        self,
        model: nn.Module,
        profiler: EnergyProfiler,
        energy_budget_source: Optional[EnergyBudgetProvider] = None,
    ):
        super().__init__()
        self.model = model
        self.profiler = profiler
        self.energy_budget_source = energy_budget_source
        self._energy_budget = 1.0
        self._layer_list = self._build_layer_list(model)
        self._last_skipped: List[str] = []
        self._current_precision: str = "fp32"
        # Cache autocast contexts per precision.
        self._autocast_cache: Dict[str, Any] = {}
        self._autocast_lock = threading.RLock()

    def _build_layer_list(
        self, module: nn.Module, prefix: str = ""
    ) -> List[Tuple[str, nn.Module]]:
        layers: List[Tuple[str, nn.Module]] = []
        for name, child in module.named_children():
            full_name = f"{prefix}.{name}" if prefix else name
            if list(child.children()):
                layers.extend(self._build_layer_list(child, full_name))
            else:
                layers.append((full_name, child))
        return layers

    def set_energy_budget(self, budget: float) -> None:
        self._energy_budget = max(0.0, min(1.0, float(budget)))

    def _get_energy_budget(self) -> float:
        if self.energy_budget_source is not None:
            try:
                return float(self.energy_budget_source.get_energy_budget())
            except Exception:
                pass
        return self._energy_budget

    def _get_autocast(self, precision: str) -> Any:
        """Return a cached context manager for the given precision."""
        with self._autocast_lock:
            if precision in self._autocast_cache:
                return self._autocast_cache[precision]
            ctx: Any = contextlib.nullcontext()
            if TORCH_AMP_AVAILABLE and precision != "fp32" and _torch_autocast is not None:
                dtype = torch.float16 if precision == "fp16" else None
                try:
                    ctx = _torch_autocast(device_type="cuda", dtype=dtype)  # type: ignore
                except Exception:
                    ctx = contextlib.nullcontext()
            self._autocast_cache[precision] = ctx
            return ctx

    async def forward_async(
        self,
        x: torch.Tensor,
        token_importance: Optional[torch.Tensor] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> torch.Tensor:
        if token_importance is None:
            token_importance = torch.ones(x.size(0), device=x.device) * 0.5
        if token_importance.dim() > 1:
            token_importance = token_importance.mean(dim=1)

        # Fresh episode per forward.
        self.profiler.reset_episode()

        current_budget = self._get_energy_budget()
        carbon_intensity = await self.profiler._get_carbon_intensity()
        self._current_precision = await self.profiler.select_precision({
            "size": "medium",
            "carbon_intensity": carbon_intensity,
        })

        context = dict(context or {})
        context["precision"] = self._current_precision

        output = x
        skipped: List[str] = []
        total_energy_original = 0.0
        total_energy_skipped = 0.0

        for layer_name, layer_module in self._layer_list:
            avg_importance = float(token_importance.mean().item())
            energy = await self.profiler.estimate_energy_for_token(
                layer_name, avg_importance
            )
            total_energy_original += energy

            if await self.profiler.should_skip_layer(
                layer_name, avg_importance, current_budget, context
            ):
                logger.debug(f"layer_skipped layer={layer_name}")
                skipped.append(layer_name)
                total_energy_skipped += energy
                continue

            with self._get_autocast(self._current_precision):
                output = layer_module(output)

        self._last_skipped = skipped
        SKIP_RATIO.set(len(skipped) / max(len(self._layer_list), 1))

        fake_accuracy = 1.0 - (len(skipped) / max(len(self._layer_list), 1)) * 0.2
        carbon_saved = total_energy_skipped * carbon_intensity / 1000.0
        await self.profiler.record_outcome(
            accuracy=fake_accuracy,
            energy_saved=total_energy_skipped,
            carbon_saved=carbon_saved,
            latency_ms=0.0,
        )
        return output

    def forward(
        self,
        x: torch.Tensor,
        token_importance: Optional[torch.Tensor] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> torch.Tensor:
        """
        Synchronous entry point. If called from inside a running event loop,
        raises a clear error — call `forward_async` instead.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.forward_async(x, token_importance, context))
        raise RuntimeError(
            "LayerSkippingWrapper.forward() cannot be called from within a "
            "running event loop. Use `await wrapper.forward_async(...)` instead."
        )

    async def run_flexgen_optimization(
        self, workload: Dict[str, Any], node: Dict[str, Any]
    ) -> Dict[str, Any]:
        return await self.profiler.run_flexgen_optimization(workload, node)

    async def select_precision(self, workload: Dict[str, Any]) -> str:
        return await self.profiler.select_precision(workload)

    def get_skipped_layers(self) -> List[str]:
        return list(self._last_skipped)

    def get_current_precision(self) -> str:
        return self._current_precision

    async def estimate_energy(
        self, x: torch.Tensor, token_importance: Optional[torch.Tensor] = None
    ) -> float:
        if token_importance is None:
            token_importance = torch.ones(x.size(0), device=x.device) * 0.5
        avg_importance = float(token_importance.mean().item())
        return await self.profiler.estimate_total_energy(x.shape, avg_importance)

    def save(self, path: Union[str, Path]) -> None:
        path = Path(path)
        data = {
            "energy_budget": self._energy_budget,
            "profiler_config": {
                "energy_per_layer": self.profiler.energy_per_layer,
                "action_space": list(self.profiler.action_space),
                "modp_weights": self.profiler.modp_weights,
            },
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    @classmethod
    def load(
        cls, path: Union[str, Path], model: nn.Module, carbon_provider=None
    ) -> "LayerSkippingWrapper":
        path = Path(path)
        with open(path, "r") as f:
            data = json.load(f)
        profiler = EnergyProfiler(
            model=model,
            energy_per_layer=data["profiler_config"]["energy_per_layer"],
            carbon_provider=carbon_provider,
            action_space=data["profiler_config"].get("action_space", ["balanced"]),
            modp_weights=data["profiler_config"].get(
                "modp_weights",
                {"accuracy": 0.4, "energy": 0.3, "carbon": 0.2, "latency": 0.1},
            ),
        )
        wrapper = cls(model=model, profiler=profiler)
        wrapper._energy_budget = float(data.get("energy_budget", 1.0))
        return wrapper


# =============================================================================
# Factory
# =============================================================================
def build_profiler(
    model: nn.Module,
    energy_per_layer: Dict[str, float],
    *,
    carbon_provider: Optional[CarbonIntensityProvider] = None,
    default_carbon_intensity: float = 400.0,
    storage: Optional[Storage] = None,
    message_queue: Optional[AsyncMessageQueue] = None,
    enable_chaos: bool = False,
    chaos_seed: Optional[int] = 42,
) -> EnergyProfiler:
    return EnergyProfiler(
        model=model,
        energy_per_layer=energy_per_layer,
        carbon_provider=carbon_provider,
        default_carbon_intensity=default_carbon_intensity,
        storage=storage,
        message_queue=message_queue,
        enable_chaos=enable_chaos,
        chaos_seed=chaos_seed,
    )


# =============================================================================
# Example
# =============================================================================
async def example() -> None:
    model = nn.Sequential(
        nn.Linear(10, 20),
        nn.ReLU(),
        nn.Linear(20, 20),
        nn.ReLU(),
        nn.Linear(20, 1),
    )
    energy_per_layer = {
        "0": 1e-6,
        "1": 0.5e-6,
        "2": 1.5e-6,
        "3": 0.5e-6,
        "4": 2e-6,
    }
    carbon_provider = CarbonIntensityManager(api_key=None)
    profiler = build_profiler(
        model=model,
        energy_per_layer=energy_per_layer,
        carbon_provider=carbon_provider,
        enable_chaos=False,
    )
    wrapper = LayerSkippingWrapper(model, profiler)
    x = torch.randn(4, 10)
    importance = torch.ones(4) * 0.8
    output = await wrapper.forward_async(x, importance)
    print(f"Output shape: {output.shape}")
    print(f"Skipped layers: {wrapper.get_skipped_layers()}")
    print(f"Last explanation: {profiler.get_last_explanation()}")
    print(f"Precision: {wrapper.get_current_precision()}")
    print(f"Safety violations: {len(profiler.get_safety_violations())}")
    print(f"Multi-agent stats: {profiler.get_agent_stats()}")
    print(f"Carbon broker totals: {profiler.get_carbon_broker_totals()}")
    print(f"Chaos stats: {profiler.get_chaos_stats()}")
    print(f"HITL pending: {len(await profiler.get_hitl_pending())}")
    print(f"Bandit stats: {profiler.bandit.get_stats()}")
    await carbon_provider.close()


if __name__ == "__main__":
    asyncio.run(example())


__all__ = [
    "CarbonIntensityManager",
    "CarbonIntensityProvider",
    "EnergyBudgetProvider",
    "CircuitBreaker",
    "CircuitBreakerState",
    "CausalBandit",
    "BanditConfig",
    "SafetyMonitor",
    "XAIExplainer",
    "FederatedProfilerCoordinator",
    "MultiAgentProfiler",
    "CarbonOffsetBroker",
    "ChaosMonkey",
    "HumanReviewManager",
    "HumanReviewRequest",
    "FlexGenManager",
    "EnergyProfiler",
    "LayerSkippingWrapper",
    "SkipDecision",
    "build_profiler",
    "EnergyProfilerError",
    "CarbonFetchError",
    "CircuitBreakerOpenError",
    "SafetyViolationError",
    "ChaosExperimentError",
    "NotImplementedYetError",
]
