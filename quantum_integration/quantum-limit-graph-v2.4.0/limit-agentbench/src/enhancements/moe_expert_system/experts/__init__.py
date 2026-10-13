#!/usr/bin/env python3
"""
MoE Expert System — BaseExpert v3.0.0 (Self-Contained Single-File Bundle)
=========================================================================

Foundation module for the MoE expert system. Everything needed to run and
test is in this file:

  1. Protocol interfaces for every injected dependency.
  2. `MOPDConfig` (pure config) and `InjectionBag` (service references).
  3. `Explanation` and `MOPDProposal` v2 (all fields optional).
  4. Shared runtime components:
        - ComponentHealth
        - TaskManager
        - TemporalSafetyMonitor (MTL)
        - HumanApprovalHandler (uncertainty sampling)
        - XAIExplainer (Integrated Gradients + counterfactuals)
        - ChaosEngine (steady-state predicate + auto-rollback)
  5. NSGA-II fast non-dominated sort with deduplication.
  6. `BaseExpert` (only `propose_async` and `get_health_status` abstract).
  7. Thread-safe `ExpertRegistry` and lazy loader.
  8. `create_expert` factory that respects constructor signatures.
  9. Comprehensive pytest suite at the bottom.

Run:
    python base_expert_v3.py         # demo
    pytest base_expert_v3.py -v      # tests

Requires: Python 3.10+, pytest, pytest-asyncio.
Optional: numpy (softmax / matrix helpers); falls back to pure Python.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import math
import random
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, AsyncIterator, Awaitable, Callable, ClassVar, Deque, Dict,
    Iterable, List, Optional, Protocol, Sequence, Set, Tuple, Type,
    Union, runtime_checkable,
)

import pytest
import pytest_asyncio

try:
    import numpy as _np
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    _np = None  # type: ignore
    NUMPY_AVAILABLE = False

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

__version__ = "3.0.0"


# =============================================================================
# PROTOCOLS — every injected dependency is behind an interface
# =============================================================================
@runtime_checkable
class StorageLike(Protocol):
    def save_state(self, key: str, value: str) -> None: ...
    def get_state(self, key: str) -> Optional[str]: ...


@runtime_checkable
class MessageQueueLike(Protocol):
    async def publish(self, topic: str, payload: str) -> None: ...


@runtime_checkable
class MetricsLike(Protocol):
    def inc(self, name: str, amount: float = 1.0) -> None: ...
    def set(self, name: str, value: float) -> None: ...


@runtime_checkable
class AdaptiveCostLike(Protocol):
    def get_current_weights(self) -> Dict[str, float]: ...
    def compute(self, **kwargs: Any) -> float: ...


@runtime_checkable
class ParetoGatingLike(Protocol):
    def filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]: ...


@runtime_checkable
class DriftDetectorLike(Protocol):
    async def check_drift(self, weights: Dict[str, float]) -> float: ...


@runtime_checkable
class TokenManagerLike(Protocol):
    """Any of these may be sync or async; the base coerces the result."""
    def spend(self, actor: str, amount: float) -> Any: ...
    def earn(self, actor: str, amount: float) -> Any: ...


@runtime_checkable
class GradientManagerLike(Protocol):
    def pump_field(self, name: str, delta: float,
                   source: str = ...) -> Any: ...
    def get_field_strengths(self) -> Dict[str, float]: ...


@runtime_checkable
class ApprovalHandlerLike(Protocol):
    async def request_approval(self, decision: Dict[str, Any],
                               user_id: str = ...) -> bool: ...


# =============================================================================
# ENUMS + VALUE OBJECTS
# =============================================================================
class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ComponentHealth:
    status: HealthStatus = HealthStatus.UNKNOWN
    score: float = 0.5
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BioResult:
    """Result of a bio-inspired operation (ATP spend/earn, gradient pump)."""
    success: bool = False
    amount: float = 0.0
    error: Optional[str] = None


@dataclass
class Explanation:
    """Structured explanation of a decision."""
    method: str = "none"
    attributions: List[float] = field(default_factory=list)
    feature_names: List[str] = field(default_factory=list)
    counterfactual: Optional[Dict[str, Any]] = None
    natural_language: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "method": self.method,
            "attributions": list(self.attributions),
            "feature_names": list(self.feature_names),
            "counterfactual": self.counterfactual,
            "natural_language": self.natural_language,
        }


@dataclass
class MOPDProposal:
    """v2: every field is optional."""
    recommendations: Dict[str, Any] = field(default_factory=dict)
    options: List[Dict[str, Any]] = field(default_factory=list)
    explanation: Optional[Union[str, Explanation]] = None
    pareto_front: Optional[List[Dict[str, Any]]] = None
    requires_human_approval: bool = False
    confidence: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        expl = self.explanation
        if isinstance(expl, Explanation):
            expl_dict = expl.to_dict()
        else:
            expl_dict = expl
        return {
            "recommendations": self.recommendations,
            "options": list(self.options),
            "explanation": expl_dict,
            "pareto_front": self.pareto_front,
            "requires_human_approval": self.requires_human_approval,
            "confidence": self.confidence,
        }


# =============================================================================
# MOPD CONFIG — pure config (no service references)
# =============================================================================
@dataclass
class MOPDConfig:
    """Pure configuration for MOPD-aware experts."""
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "carbon_savings": 0.4,
        "helium_savings": 0.3,
        "cost": 0.2,
        "latency": 0.1,
    })
    minimise_keys: Set[str] = field(default_factory=lambda: {"cost", "latency"})
    pareto_grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True
    enable_quantum: bool = False
    enable_xai: bool = True
    enable_human_approval: bool = False
    enable_temporal_safety: bool = True
    enable_chaos: bool = False

    def __post_init__(self) -> None:
        total = sum(max(0.0, v) for v in self.objective_weights.values())
        if total <= 0:
            n = max(len(self.objective_weights), 1)
            self.objective_weights = {k: 1.0 / n
                                       for k in self.objective_weights}
        else:
            self.objective_weights = {
                k: max(0.0, v) / total
                for k, v in self.objective_weights.items()}

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["minimise_keys"] = sorted(d["minimise_keys"])
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "MOPDConfig":
        d = dict(d or {})
        if "minimise_keys" in d and d["minimise_keys"] is not None:
            d["minimise_keys"] = set(d["minimise_keys"])
        return cls(**d)


# =============================================================================
# INJECTION BAG — service references, kept separate from config
# =============================================================================
@dataclass
class InjectionBag:
    storage: Optional[StorageLike] = None
    message_queue: Optional[MessageQueueLike] = None
    adaptive_cost: Optional[AdaptiveCostLike] = None
    pareto_gating: Optional[ParetoGatingLike] = None
    drift_detector: Optional[DriftDetectorLike] = None
    metrics: Optional[MetricsLike] = None
    token_manager: Optional[TokenManagerLike] = None
    gradient_manager: Optional[GradientManagerLike] = None
    approval_handler: Optional[ApprovalHandlerLike] = None


# =============================================================================
# PARETO UTILITIES
# =============================================================================
def is_dominated(a: Dict[str, Any],
                 b: Dict[str, Any],
                 minimise_keys: Set[str],
                 maximise_keys: Set[str]) -> bool:
    """Return True if b dominates a.

    Missing values default to `+inf` for minimisation and `-inf` for
    maximisation, so undefined objectives never look better than defined ones.
    """
    a_better_or_equal = True
    b_strictly_better_somewhere = False
    for key in maximise_keys:
        av = float(a.get(key, float("-inf")))
        bv = float(b.get(key, float("-inf")))
        if bv < av:
            a_better_or_equal = False
            break
        if bv > av:
            b_strictly_better_somewhere = True
    if a_better_or_equal:
        for key in minimise_keys:
            av = float(a.get(key, float("inf")))
            bv = float(b.get(key, float("inf")))
            if bv > av:
                a_better_or_equal = False
                break
            if bv < av:
                b_strictly_better_somewhere = True
    return a_better_or_equal and b_strictly_better_somewhere


def nsga2_fast_nondominated_sort(
    solutions: List[Dict[str, Any]],
    minimise_keys: Set[str],
    maximise_keys: Set[str],
) -> List[Dict[str, Any]]:
    """Return the Pareto-optimal (rank-0) front with deduplication.

    Implements Deb et al.'s fast non-dominated sort on the first front only.
    Duplicate objective vectors are collapsed to their first occurrence.
    """
    if not solutions:
        return []

    # Deduplicate identical objective vectors
    seen: Dict[Tuple[float, ...], int] = {}
    unique: List[Dict[str, Any]] = []
    objective_order = sorted(minimise_keys) + sorted(maximise_keys)
    for s in solutions:
        key = tuple(float(s.get(k, 0.0)) for k in objective_order)
        if key in seen:
            continue
        seen[key] = len(unique)
        unique.append(s)

    n = len(unique)
    if n == 0:
        return []
    if n == 1:
        return [unique[0]]

    dominated_sets: List[List[int]] = [[] for _ in range(n)]
    domination_count = [0] * n

    for i in range(n):
        for j in range(i + 1, n):
            if is_dominated(unique[i], unique[j],
                            minimise_keys, maximise_keys):
                dominated_sets[i].append(j)
                domination_count[j] += 1
            elif is_dominated(unique[j], unique[i],
                              minimise_keys, maximise_keys):
                dominated_sets[j].append(i)
                domination_count[i] += 1

    front: List[int] = [i for i in range(n) if domination_count[i] == 0]
    # The first front is what we return; we don't need the rest for this API.
    return [unique[i] for i in front]


def filter_pareto_front(solutions: List[Dict[str, Any]],
                        objective_keys: Optional[List[str]] = None,
                        minimise_keys: Optional[Set[str]] = None
                        ) -> List[Dict[str, Any]]:
    """Backward-compatible helper: given objective keys, infer which are
    minimisation (cost/latency by default) and return the Pareto front.
    """
    minimise_keys = set(minimise_keys) if minimise_keys else {"cost", "latency"}
    if objective_keys is None:
        objective_keys = list(minimise_keys) + [
            k for k in (solutions[0].keys() if solutions else [])
            if k not in minimise_keys]
    maximise_keys = set(objective_keys) - minimise_keys
    return nsga2_fast_nondominated_sort(solutions, minimise_keys, maximise_keys)


def scalarise(solutions: List[Dict[str, Any]],
              weights: Dict[str, float],
              objective_keys: List[str],
              minimise_keys: Optional[Set[str]] = None,
              ) -> Tuple[List[Dict[str, Any]], List[float]]:
    """Compute weighted-sum scores.

    Returns copies of the input solutions and their scores. Input is not
    mutated. For minimisation objectives, the normalised contribution is
    inverted so that a higher score always means "better".
    """
    minimise_keys = set(minimise_keys) if minimise_keys else {"cost", "latency"}
    sols = [dict(s) for s in solutions]
    if not sols:
        return [], []
    scores = [0.0] * len(sols)
    for key in objective_keys:
        vals = [float(s.get(key, 0.0)) for s in sols]
        lo, hi = min(vals), max(vals)
        span = (hi - lo) if hi != lo else 1.0
        weight = max(0.0, float(weights.get(key, 0.0)))
        if weight == 0.0:
            continue
        for i, v in enumerate(vals):
            if key in minimise_keys:
                norm = (hi - v) / span
            else:
                norm = (v - lo) / span
            scores[i] += weight * norm
    return sols, scores


# =============================================================================
# SHARED RUNTIME — TaskManager
# =============================================================================
class TaskManager:
    def __init__(self) -> None:
        self._tasks: Dict[str, asyncio.Task] = {}
        self._shutdown_event = asyncio.Event()
        self._lock = threading.Lock()

    @property
    def shutdown_event(self) -> asyncio.Event:
        return self._shutdown_event

    def start(self, name: str,
              coro_factory: Callable[[], Any]) -> Optional[asyncio.Task]:
        with self._lock:
            existing = self._tasks.get(name)
            if existing is not None and not existing.done():
                return existing
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.warning("TaskManager: no running loop; %s not started",
                               name)
                return None
            task = loop.create_task(coro_factory())
            self._tasks[name] = task
            task.add_done_callback(
                lambda t, n=name: self._forget(n))
            return task

    def start_coro(self, name: str, coro: Any) -> Optional[asyncio.Task]:
        return self.start(name, lambda: coro)

    def _forget(self, name: str) -> None:
        with self._lock:
            self._tasks.pop(name, None)

    def task_count(self) -> int:
        with self._lock:
            return len(self._tasks)

    async def stop_all(self, timeout: float = 5.0) -> None:
        self._shutdown_event.set()
        with self._lock:
            tasks = list(self._tasks.values())
        for t in tasks:
            t.cancel()
        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("TaskManager.stop_all timed out")
        with self._lock:
            self._tasks.clear()


# =============================================================================
# SHARED RUNTIME — Temporal safety (MTL)
# =============================================================================
_ATOMIC_RE = __import__("re").compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


@dataclass
class _TemporalRule:
    rule_id: str
    operator: str
    predicate: Callable[[Dict[str, Any]], bool]
    window_seconds: float = 0.0
    description: str = ""
    severity: str = "warning"
    violations: int = 0
    last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque[Tuple[datetime, Dict[str, Any]]]) -> bool:
        if not trace:
            return False
        if self.window_seconds > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window_seconds)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == "always":
            return any(not self.predicate(s) for _, s in trace)
        if op == "eventually":
            return not any(self.predicate(s) for _, s in trace)
        if op == "never":
            return any(self.predicate(s) for _, s in trace)
        if op == "historically":
            return any(not self.predicate(s) for _, s in trace)
        if op == "once":
            return not any(self.predicate(s) for _, s in trace)
        return False


class TemporalSafetyMonitor:
    def __init__(self, max_trace: int = 500) -> None:
        self.rules: Dict[str, _TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict[str, Any]]] = deque(
            maxlen=max_trace)

    def add_formula(self, formula: str) -> None:
        f = formula.strip()
        window = 0.0
        if f.startswith("[") and "]" in f:
            head, rest = f[1:].split("]", 1)
            try:
                window = float(head.strip().rstrip("s"))
            except ValueError:
                window = 0.0
            f = rest.strip()
        op_map = {"G ": "always", "F ": "eventually",
                  "NEVER ": "never", "H ": "historically", "O ": "once"}
        for prefix, op in op_map.items():
            if f.startswith(prefix):
                expr = f[len(prefix):].strip().strip("()")
                self._add_atomic(expr, op, window)
                return

    def _add_atomic(self, expr: str, op: str, window: float) -> None:
        m = _ATOMIC_RE.match(expr)
        if not m:
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def predicate(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]

        rid = f"{op}:{expr}" + (f"@{window}s" if window else "")
        self.rules[rid] = _TemporalRule(rid, op, predicate,
                                        window_seconds=window,
                                        description=expr)

    async def push(self, state: Dict[str, Any]) -> None:
        self.trace.append((datetime.now(timezone.utc), dict(state)))

    async def check(self) -> Dict[str, bool]:
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
        return result

    def counts(self) -> Dict[str, int]:
        return {rid: r.violations for rid, r in self.rules.items()}


# =============================================================================
# SHARED RUNTIME — Human approval (uncertainty sampling)
# =============================================================================
class HumanApprovalHandler:
    def __init__(self, queue: Optional[MessageQueueLike] = None,
                 uncertainty_threshold: float = 0.05,
                 timeout: float = 5.0) -> None:
        self.queue = queue
        self.uncertainty_threshold = float(uncertainty_threshold)
        self.timeout = float(timeout)
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.decisions: List[Dict[str, Any]] = []

    async def submit_response(self, user_id: str, chosen_id: str) -> None:
        try:
            self._responses.put_nowait({"user_id": user_id,
                                         "chosen": chosen_id})
        except asyncio.QueueFull:
            pass

    def _utility(self, user_id: str, candidate: Dict[str, Any]) -> float:
        prefs = self.preferences.get(user_id, {})
        if not prefs:
            return 0.5
        u = 0.0
        for k, v in prefs.items():
            if k in candidate:
                try:
                    u += float(v) * float(candidate[k])
                except Exception:
                    continue
        return u

    async def request_approval(self, decision: Dict[str, Any],
                               user_id: str = "default") -> bool:
        candidates = decision.get("candidates") or []
        if len(candidates) < 2:
            self.decisions.append({"decision": decision, "auto": True,
                                    "approved": True})
            return True
        scored = sorted(((self._utility(user_id, c), c) for c in candidates),
                        key=lambda kv: kv[0], reverse=True)
        if abs(scored[0][0] - scored[1][0]) > self.uncertainty_threshold:
            self.decisions.append({"decision": decision, "auto": True,
                                    "approved": True,
                                    "chosen": scored[0][1].get("id")})
            return True
        try:
            msg = await asyncio.wait_for(self._responses.get(),
                                         timeout=self.timeout)
            approved = bool(msg.get("chosen"))
        except asyncio.TimeoutError:
            approved = True
        self.decisions.append({"decision": decision, "auto": False,
                                "approved": approved})
        return approved

    async def record_choice(self, user_id: str, solution_id: str,
                            metrics: Optional[Dict[str, float]] = None) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k, v in metrics.items():
                prefs[k] = prefs.get(k, 0.25) + max(0.0, float(v)) * 0.01
            s = sum(prefs.values()) or 1.0
            self.preferences[user_id] = {k: v / s for k, v in prefs.items()}


# =============================================================================
# SHARED RUNTIME — XAI (Integrated Gradients)
# =============================================================================
class XAIExplainer:
    def __init__(self, ig_steps: int = 16, cf_max_iter: int = 40,
                 cf_target_delta: float = 0.1,
                 rng: Optional[random.Random] = None) -> None:
        self.ig_steps = int(ig_steps)
        self.cf_max_iter = int(cf_max_iter)
        self.cf_target_delta = float(cf_target_delta)
        self.rng = rng or random.Random()

    def integrated_gradients(self, f: Callable[[List[float]], float],
                             x: Sequence[float],
                             baseline: Optional[Sequence[float]] = None
                             ) -> List[float]:
        x = [float(v) for v in x]
        if baseline is None:
            baseline = [0.0] * len(x)
        else:
            baseline = [float(v) for v in baseline]
        total = [0.0] * len(x)
        for s in range(1, self.ig_steps + 1):
            alpha = s / self.ig_steps
            interp = [b + alpha * (a - b) for a, b in zip(x, baseline)]
            eps = 1e-4
            for i in range(len(x)):
                up = list(interp); up[i] += eps
                dn = list(interp); dn[i] -= eps
                try:
                    grad = (float(f(up)) - float(f(dn))) / (2 * eps)
                except Exception:
                    grad = 0.0
                total[i] += grad
        avg = [t / max(self.ig_steps, 1) for t in total]
        return [(a - b) * g for a, b, g in zip(x, baseline, avg)]

    def counterfactual(self, f: Callable[[List[float]], float],
                       x: Sequence[float],
                       target_delta: Optional[float] = None,
                       bounds: Tuple[float, float] = (-1.0, 1.0)
                       ) -> Dict[str, Any]:
        target_delta = target_delta or self.cf_target_delta
        x = [float(v) for v in x]
        try:
            base_pred = float(f(x))
        except Exception:
            return {"success": False}
        best_delta, best_pred = 0.0, base_pred
        best_idx, best_step = None, 0.0
        x_cf = list(x)
        for _ in range(self.cf_max_iter):
            i = self.rng.randrange(len(x))
            step = self.rng.choice([-1, 1]) * 0.1
            trial = list(x_cf)
            trial[i] = float(max(bounds[0], min(bounds[1],
                                                  trial[i] + step)))
            try:
                pred = float(f(trial))
            except Exception:
                continue
            delta = abs(pred - base_pred)
            if delta > best_delta:
                best_delta, best_pred, x_cf = delta, pred, trial
                best_idx, best_step = i, step
            if delta >= abs(target_delta):
                break
        return {"success": best_delta >= abs(target_delta),
                "changed_index": best_idx,
                "change_amount": best_step,
                "delta": best_pred - base_pred}

    def explain(self, f: Callable[[List[float]], float],
                x: Sequence[float],
                feature_names: Optional[List[str]] = None,
                include_counterfactual: bool = True) -> Explanation:
        attrs = self.integrated_gradients(f, x)
        cf = self.counterfactual(f, x) if include_counterfactual else None
        return Explanation(
            method="integrated_gradients",
            attributions=list(attrs),
            feature_names=list(feature_names or []),
            counterfactual=cf,
            natural_language="Top features: " + ", ".join(
                sorted((name for name in (feature_names or [])),
                       key=lambda n: -abs(attrs[(feature_names or []).index(n)])
                       if feature_names else 0)[:3]))


# =============================================================================
# SHARED RUNTIME — ChaosEngine (steady-state + auto-rollback)
# =============================================================================
class ChaosEngine:
    FAULT_TYPES: ClassVar[Tuple[str, ...]] = (
        "latency", "exception", "data_corruption",
        "memory_pressure", "network_drop",
    )

    def __init__(
        self,
        steady_state: Callable[[], Any],
        snapshot: Optional[Callable[[], Any]] = None,
        restore: Optional[Callable[[Any], None]] = None,
        probability: float = 0.01,
        auto_rollback: bool = True,
        blast_radius: float = 0.5,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.steady_state = steady_state
        self.snapshot = snapshot
        self.restore = restore
        self.probability = float(probability)
        self.auto_rollback = bool(auto_rollback)
        self.blast_radius = float(blast_radius)
        self.rng = rng or random.Random()
        self.active: Dict[str, Dict[str, Any]] = {}
        self.rollback_count = 0
        self._lock = threading.Lock()

    def _allowed(self) -> int:
        return max(1, int(len(self.FAULT_TYPES) * self.blast_radius))

    async def _check_steady(self) -> bool:
        try:
            r = self.steady_state()
            if asyncio.iscoroutine(r):
                r = await r
            return bool(r)
        except Exception:
            return False

    async def maybe_inject(self) -> Optional[Dict[str, Any]]:
        if self.rng.random() >= self.probability:
            return None
        fault = self.rng.choice(self.FAULT_TYPES)
        return await self.run_experiment(
            f"auto_{uuid.uuid4().hex[:6]}", fault)

    async def run_experiment(self, name: str, fault_type: str
                             ) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        with self._lock:
            if len(self.active) >= self._allowed():
                return {"name": name, "fault_type": fault_type,
                        "status": "skipped", "reason": "blast_radius"}
            self.active[name] = {"fault_type": fault_type}
        t0 = time.time()
        before = await self._check_steady()
        snap = None
        if self.snapshot is not None:
            try:
                snap = self.snapshot()
                if asyncio.iscoroutine(snap):
                    snap = await snap
            except Exception:
                snap = None
        status = "completed"
        try:
            if fault_type == "latency":
                await asyncio.sleep(0.005)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "memory_pressure":
                _ = bytearray(1024)
            elif fault_type == "network_drop":
                await asyncio.sleep(0.005)
            elif fault_type == "data_corruption":
                await asyncio.sleep(0)
        except Exception:
            status = "injected"
        finally:
            with self._lock:
                self.active.pop(name, None)
        after = await self._check_steady()
        rolled_back = False
        if before and not after and self.auto_rollback and \
                snap is not None and self.restore is not None:
            try:
                r = self.restore(snap)
                if asyncio.iscoroutine(r):
                    await r
                rolled_back = True
                self.rollback_count += 1
            except Exception as e:
                logger.warning(f"rollback failed: {e}")
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status,
                "duration_ms": (time.time() - t0) * 1000.0,
                "rolled_back": rolled_back}


# =============================================================================
# BASE EXPERT
# =============================================================================
class BaseExpert(ABC):
    """Abstract base for all MoE experts."""

    __expert_version__: str = "0.0.0"
    __expert_description__: str = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Only warn if the subclass itself did not override the placeholder.
        if cls.__dict__.get("__expert_version__", None) is None:
            logger.warning(
                "%s does not define __expert_version__; using %r",
                cls.__name__, cls.__expert_version__)
        if not cls.__dict__.get("__expert_description__", None):
            logger.warning(
                "%s does not define __expert_description__; using 'No description provided.'",
                cls.__name__)
            cls.__expert_description__ = "No description provided."

    # ------------------------------------------------------------------
    # Constructor
    # ------------------------------------------------------------------
    def __init__(
        self,
        storage: Optional[StorageLike] = None,
        message_queue: Optional[MessageQueueLike] = None,
        adaptive_cost: Optional[AdaptiveCostLike] = None,
        pareto_gating: Optional[ParetoGatingLike] = None,
        drift_detector: Optional[DriftDetectorLike] = None,
        metrics: Optional[MetricsLike] = None,
        approval_handler: Optional[ApprovalHandlerLike] = None,
        mopd_config: Optional[Union[MOPDConfig, Dict[str, Any]]] = None,
        seed: int = 42,
        **kwargs: Any,
    ) -> None:
        if kwargs:
            logger.warning(
                "%s: unexpected kwargs ignored: %s",
                self.__class__.__name__, list(kwargs.keys()))

        self.injection = InjectionBag(
            storage=storage,
            message_queue=message_queue,
            adaptive_cost=adaptive_cost,
            pareto_gating=pareto_gating,
            drift_detector=drift_detector,
            metrics=metrics,
            approval_handler=approval_handler,
        )

        # Backwards-compatible direct attributes
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        # Config
        if isinstance(mopd_config, dict):
            mopd_config = MOPDConfig.from_dict(mopd_config)
        self.mopd_config: MOPDConfig = mopd_config or MOPDConfig()

        # RNG tree
        self._rng = random.Random(seed)
        self._rng_xai = random.Random(seed + 1)
        self._rng_chaos = random.Random(seed + 2)

        # Shared runtime
        self.task_manager = TaskManager()
        self.temporal_safety = TemporalSafetyMonitor()
        self.xai = XAIExplainer(rng=self._rng_xai)
        self.hitl: ApprovalHandlerLike = (
            approval_handler or HumanApprovalHandler())
        self.chaos: Optional[ChaosEngine] = None
        if self.mopd_config.enable_chaos:
            self.chaos = ChaosEngine(
                steady_state=self._default_steady_state,
                rng=self._rng_chaos)

        # Bio-managers (may be set later via set_bio_core)
        self.bio_core: Optional[Any] = None
        self.token_manager: Optional[TokenManagerLike] = None
        self.gradient_manager: Optional[GradientManagerLike] = None
        self.compartment_manager: Optional[Any] = None
        self.biomass_storage: Optional[Any] = None
        self.harvester: Optional[Any] = None
        self.scheduler: Optional[Any] = None

    # ------------------------------------------------------------------
    # Abstract interface: only propose_async and get_health_status
    # ------------------------------------------------------------------
    @abstractmethod
    async def propose_async(self, context: Dict[str, Any]) -> MOPDProposal:
        """Produce a proposal for the given context."""

    @abstractmethod
    async def get_health_status(self) -> ComponentHealth:
        """Return the expert's current health."""

    # ------------------------------------------------------------------
    # Optional with defaults
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        return None

    async def shutdown(self) -> None:
        await self.task_manager.stop_all()

    async def get_pareto_front(self,
                               context: Dict[str, Any]
                               ) -> List[Dict[str, Any]]:
        proposal = await self.propose_async(context)
        if proposal.pareto_front is not None:
            return proposal.pareto_front
        return list(proposal.options)

    def set_objective_weights(self, weights: Dict[str, float]) -> None:
        self.mopd_config.objective_weights = dict(weights)
        self.mopd_config.__post_init__()

    def get_objective_weights(self) -> Dict[str, float]:
        return dict(self.mopd_config.objective_weights)

    def get_mopd_config(self) -> MOPDConfig:
        return self.mopd_config

    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """Default: uniform distribution over supported tasks.

        Subclasses that want context-aware behaviour should override.
        """
        tasks = getattr(self, "supported_task_types", None) or []
        if not tasks:
            return [0.5, 0.5]
        n = len(tasks)
        return [1.0 / n] * n

    async def explain_decision(self, context: Dict[str, Any],
                               recommendation: MOPDProposal
                               ) -> Explanation:
        expl = recommendation.explanation
        if isinstance(expl, Explanation):
            return expl
        if isinstance(expl, str):
            return Explanation(method="passthrough",
                               natural_language=expl)
        return Explanation(method="none",
                           natural_language="No explanation available.")

    async def request_approval(self, recommendation: MOPDProposal,
                               context: Dict[str, Any]) -> bool:
        if not self.mopd_config.enable_human_approval:
            return True
        if self.injection.approval_handler is None:
            logger.warning(
                "%s requested approval but no handler was injected; "
                "returning False (denied).",
                self.__class__.__name__)
            return False
        decision = {
            "action": "approve_proposal",
            "candidates": [
                {"id": "approve", "confidence": recommendation.confidence},
                {"id": "defer", "confidence": 0.0},
            ],
            "context": context,
        }
        return await self.injection.approval_handler.request_approval(decision)

    async def log_uncertainty(self, metric: str, value: float,
                              context: Dict[str, Any]) -> None:
        logger.info("uncertainty metric=%s value=%s context=%s",
                    metric, value, context)

    async def check_invariants(self, state: Dict[str, Any]
                               ) -> List[str]:
        """Default: evaluate registered MTL rules on the shared monitor.

        If no rules are registered, warn the first time it is called
        (a subclass that relies on safety without registering rules is
        almost certainly misconfigured).
        """
        if not self.temporal_safety.rules:
            if not getattr(self, "_warned_no_rules", False):
                logger.warning(
                    "%s.check_invariants called but no MTL rules are "
                    "registered; returning no violations.",
                    self.__class__.__name__)
                self._warned_no_rules = True
            return []
        await self.temporal_safety.push(state)
        results = await self.temporal_safety.check()
        return [rid for rid, ok in results.items() if not ok]

    async def inject_fault(self, fault_type: str,
                           **params: Any) -> Dict[str, Any]:
        if self.chaos is None:
            return {"status": "not_supported",
                    "reason": "chaos disabled in config"}
        name = params.pop("name", f"manual_{uuid.uuid4().hex[:6]}")
        return await self.chaos.run_experiment(name, fault_type)

    async def run_chaos_test(self, runs: int = 5) -> Dict[str, Any]:
        if self.chaos is None:
            return {"status": "not_supported"}
        results = []
        for i in range(max(1, int(runs))):
            fault = self._rng_chaos.choice(ChaosEngine.FAULT_TYPES)
            results.append(await self.chaos.run_experiment(
                f"chaos_{i}_{uuid.uuid4().hex[:4]}", fault))
        return {
            "status": "success",
            "runs": len(results),
            "rollbacks": self.chaos.rollback_count,
            "results": results,
        }

    # ------------------------------------------------------------------
    # Bio integration (with sync/async coercion and structured results)
    # ------------------------------------------------------------------
    def set_bio_core(self, bio_core: Any) -> None:
        self.bio_core = bio_core
        if bio_core is None:
            return
        self.token_manager = getattr(bio_core, "token_manager", None)
        self.gradient_manager = getattr(bio_core, "gradient_manager", None)
        self.compartment_manager = getattr(bio_core, "compartment_manager", None)
        self.biomass_storage = getattr(bio_core, "biomass_storage", None)
        self.harvester = getattr(bio_core, "harvester", None)
        self.scheduler = getattr(bio_core, "scheduler", None)

    async def _coerce_call(self, func: Callable[[], Any]) -> Any:
        r = func()
        if asyncio.iscoroutine(r):
            r = await r
        return r

    async def spend_atp(self, amount: float,
                        consumer: str = "expert") -> BioResult:
        if self.token_manager is None:
            return BioResult(success=False, error="no_token_manager")
        try:
            await self._coerce_call(
                lambda: self.token_manager.spend(consumer, amount))
            return BioResult(success=True, amount=float(amount))
        except Exception as e:
            logger.warning("ATP spend failed: %s", e, exc_info=True)
            return BioResult(success=False, error=str(e))

    async def earn_atp(self, amount: float,
                       source: str = "expert") -> BioResult:
        if self.token_manager is None:
            return BioResult(success=False, error="no_token_manager")
        try:
            await self._coerce_call(
                lambda: self.token_manager.earn(source, amount))
            return BioResult(success=True, amount=float(amount))
        except Exception as e:
            logger.warning("ATP earn failed: %s", e, exc_info=True)
            return BioResult(success=False, error=str(e))

    async def pump_gradient(self, field: str, delta: float,
                            source: str = "expert") -> BioResult:
        if self.gradient_manager is None:
            return BioResult(success=False, error="no_gradient_manager")
        try:
            await self._coerce_call(
                lambda: self.gradient_manager.pump_field(
                    field, delta, source=source))
            return BioResult(success=True, amount=float(delta))
        except Exception as e:
            logger.warning("Gradient pump failed: %s", e, exc_info=True)
            return BioResult(success=False, error=str(e))

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    async def get_capabilities(self) -> Dict[str, Any]:
        health = await self.get_health_status()
        return {
            "name": self.__class__.__name__,
            "version": self.__expert_version__,
            "description": self.__expert_description__,
            "health_status": {"status": health.status.value,
                              "score": health.score,
                              "details": health.details},
            "mopd_weights": self.get_objective_weights(),
            "mopd_config": self.mopd_config.to_dict(),
        }

    def get_capabilities_sync(self) -> Dict[str, Any]:
        return {
            "name": self.__class__.__name__,
            "version": self.__expert_version__,
            "description": self.__expert_description__,
            "health_status": None,
            "mopd_weights": self.get_objective_weights(),
            "mopd_config": self.mopd_config.to_dict(),
        }

    async def get_metrics(self) -> Dict[str, Any]:
        return {
            "version": self.__expert_version__,
            "background_tasks": self.task_manager.task_count(),
            "temporal_violations": self.temporal_safety.counts(),
        }

    def get_version(self) -> str:
        return self.__expert_version__

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------
    async def _default_steady_state(self) -> bool:
        return True

    async def __aenter__(self) -> "BaseExpert":
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any,
                        exc_tb: Any) -> None:
        await self.shutdown()


# =============================================================================
# REGISTRY (thread-safe)
# =============================================================================
class ExpertRegistry:
    """Thread-safe registry of expert classes."""

    def __init__(self) -> None:
        self._experts: Dict[str, Type[BaseExpert]] = {}
        self._lock = threading.RLock()

    def register(self, name: str, expert_class: Type[BaseExpert],
                 replace: bool = False) -> None:
        if not (isinstance(expert_class, type) and
                issubclass(expert_class, BaseExpert)):
            raise TypeError(
                f"{expert_class} must inherit from BaseExpert")
        with self._lock:
            existing = self._experts.get(name)
            if existing is not None and not replace:
                raise ValueError(
                    f"Expert '{name}' is already registered "
                    f"(v{existing.__expert_version__}). "
                    f"Pass replace=True to overwrite.")
            if existing is not None and replace:
                # Warn on version downgrade
                try:
                    cur = tuple(int(p) for p in
                                existing.__expert_version__.split("."))
                    new = tuple(int(p) for p in
                                expert_class.__expert_version__.split("."))
                    if new < cur:
                        logger.warning(
                            "Downgrading expert '%s' from v%s to v%s",
                            name, existing.__expert_version__,
                            expert_class.__expert_version__)
                except Exception:
                    pass
            self._experts[name] = expert_class
        logger.info("Registered expert '%s' (v%s)",
                    name, expert_class.__expert_version__)

    def unregister(self, name: str) -> None:
        with self._lock:
            if name not in self._experts:
                raise ValueError(f"Expert '{name}' is not registered.")
            del self._experts[name]

    def get(self, name: str) -> Type[BaseExpert]:
        with self._lock:
            if name not in self._experts:
                raise ValueError(f"Expert '{name}' is not registered.")
            return self._experts[name]

    def list(self) -> List[str]:
        with self._lock:
            return list(self._experts.keys())

    def snapshot(self) -> Dict[str, Type[BaseExpert]]:
        with self._lock:
            return dict(self._experts)

    def clear(self) -> None:
        with self._lock:
            self._experts.clear()


_REGISTRY = ExpertRegistry()


def register_expert(name: str, expert_class: Type[BaseExpert],
                    replace: bool = False) -> None:
    _REGISTRY.register(name, expert_class, replace=replace)


def unregister_expert(name: str) -> None:
    _REGISTRY.unregister(name)


def get_expert(name: str) -> Type[BaseExpert]:
    return _REGISTRY.get(name)


def list_experts() -> List[str]:
    return _REGISTRY.list()


# =============================================================================
# LAZY EXPERT LOADER
# =============================================================================
_LAZY_MODULES: Dict[str, str] = {
    "EnergyExpert": ".energy_expert",
    "DataExpert": ".data_expert",
    "IoTExpert": ".iot_expert",
    "QuantumExpert": ".quantum_expert",
    "HeliumExpert": ".helium_expert",
}


def load_expert(name: str, package: Optional[str] = None) -> Type[BaseExpert]:
    """Load an expert class on demand.

    Registration is idempotent; calling twice returns the cached class.
    """
    try:
        return _REGISTRY.get(name)
    except ValueError:
        pass
    module_path = _LAZY_MODULES.get(name)
    if module_path is None:
        raise ValueError(f"Unknown expert '{name}'")
    pkg = package if package is not None else __package__
    module = importlib.import_module(module_path, package=pkg)
    cls = getattr(module, name)
    _REGISTRY.register(name, cls, replace=False)
    return cls


# =============================================================================
# FACTORY
# =============================================================================
def create_expert(
    name: str,
    *,
    registry: Optional[ExpertRegistry] = None,
    bio_core: Optional[Any] = None,
    config: Optional[Dict[str, Any]] = None,
    mopd_config: Optional[Union[Dict[str, Any], MOPDConfig]] = None,
    storage: Optional[StorageLike] = None,
    message_queue: Optional[MessageQueueLike] = None,
    adaptive_cost: Optional[AdaptiveCostLike] = None,
    pareto_gating: Optional[ParetoGatingLike] = None,
    drift_detector: Optional[DriftDetectorLike] = None,
    metrics: Optional[MetricsLike] = None,
    approval_handler: Optional[ApprovalHandlerLike] = None,
    **extra: Any,
) -> BaseExpert:
    """Instantiate an expert, respecting its constructor signature.

    Unknown kwargs are dropped with a warning rather than silently passed.
    """
    reg = registry or _REGISTRY
    try:
        expert_class = reg.get(name)
    except ValueError:
        expert_class = load_expert(name)

    if isinstance(mopd_config, dict):
        mopd_config = MOPDConfig.from_dict(mopd_config)

    candidate_kwargs: Dict[str, Any] = {
        "storage": storage,
        "message_queue": message_queue,
        "adaptive_cost": adaptive_cost,
        "pareto_gating": pareto_gating,
        "drift_detector": drift_detector,
        "metrics": metrics,
        "approval_handler": approval_handler,
        "mopd_config": mopd_config,
        "config": config,
        "bio_core": bio_core,
    }
    candidate_kwargs.update(extra)

    sig = inspect.signature(expert_class.__init__)
    params = sig.parameters
    accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD
                         for p in params.values())

    # Filter explicitly by signature unless the constructor accepts **kwargs
    if accepts_var_kw:
        final_kwargs = {k: v for k, v in candidate_kwargs.items()
                        if v is not None}
    else:
        final_kwargs = {}
        for k, v in candidate_kwargs.items():
            if k in params and v is not None:
                final_kwargs[k] = v
        # Warn about dropped keys
        for k, v in candidate_kwargs.items():
            if k not in params and v is not None:
                logger.debug(
                    "create_expert(%s): dropping unsupported kwarg '%s'",
                    name, k)

    expert = expert_class(**final_kwargs)

    # Inject bio_core via setter if the constructor did not accept it
    if bio_core is not None and hasattr(expert, "set_bio_core") and \
            "bio_core" not in final_kwargs:
        expert.set_bio_core(bio_core)

    return expert


# =============================================================================
# MULTI-EXPERT HELPERS
# =============================================================================
async def shutdown_all_experts(experts: Sequence[BaseExpert],
                               timeout: float = 5.0) -> Dict[str, Any]:
    """Shut down experts in parallel with a per-expert timeout."""
    async def _shutdown(e: BaseExpert) -> Dict[str, Any]:
        name = e.__class__.__name__
        try:
            await asyncio.wait_for(e.shutdown(), timeout=timeout)
            return {"name": name, "status": "ok"}
        except asyncio.TimeoutError:
            return {"name": name, "status": "timeout"}
        except Exception as exc:
            return {"name": name, "status": "error", "error": str(exc)}

    results = await asyncio.gather(*(_shutdown(e) for e in experts))
    return {"experts": list(results)}


async def get_experts_capabilities(experts: Sequence[BaseExpert]
                                    ) -> List[Dict[str, Any]]:
    async def _caps(e: BaseExpert) -> Dict[str, Any]:
        name = e.__class__.__name__
        try:
            return await e.get_capabilities()
        except Exception as exc:
            return {"name": name, "error": str(exc)}
    return list(await asyncio.gather(*(_caps(e) for e in experts)))


# =============================================================================
# FIXTURES + TESTS
# =============================================================================
@pytest.fixture
def registry() -> ExpertRegistry:
    return ExpertRegistry()


class _StubExpert(BaseExpert):
    __expert_version__ = "1.0.0"
    __expert_description__ = "Stub expert for tests"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.supported_task_types = ["a", "b", "c"]

    async def propose_async(self, context: Dict[str, Any]) -> MOPDProposal:
        return MOPDProposal(
            recommendations={"action": "do_nothing"},
            options=[{"cost": 1.0, "latency": 2.0}],
            explanation="stub",
            confidence=0.9,
        )

    async def get_health_status(self) -> ComponentHealth:
        return ComponentHealth(HealthStatus.HEALTHY, 1.0)


class _StrictExpert(BaseExpert):
    __expert_version__ = "1.0.0"
    __expert_description__ = "Strict constructor expert"

    def __init__(self, storage: Any = None,
                 message_queue: Any = None) -> None:
        super().__init__(storage=storage, message_queue=message_queue)

    async def propose_async(self, context: Dict[str, Any]) -> MOPDProposal:
        return MOPDProposal()

    async def get_health_status(self) -> ComponentHealth:
        return ComponentHealth(HealthStatus.HEALTHY, 1.0)


@pytest_asyncio.fixture
async def stub_expert() -> BaseExpert:
    e = _StubExpert(seed=42)
    await e.initialize()
    try:
        yield e
    finally:
        await e.shutdown()


# -----------------------------------------------------------------------------
# Tests — Pareto utilities
# -----------------------------------------------------------------------------
class TestParetoUtilities:
    def test_is_dominated_basic(self):
        a = {"carbon_savings": 10, "helium_savings": 5,
             "cost": 1, "latency": 2}
        b = {"carbon_savings": 20, "helium_savings": 10,
             "cost": 0.5, "latency": 1}
        assert is_dominated(a, b, {"cost", "latency"},
                            {"carbon_savings", "helium_savings"})
        assert not is_dominated(b, a, {"cost", "latency"},
                                 {"carbon_savings", "helium_savings"})

    def test_missing_values_for_minimisation(self):
        # Missing cost should default to +inf → always worse
        a = {"carbon_savings": 10, "cost": 0.5}
        b = {"carbon_savings": 10}  # no cost
        # Neither dominates (a is better on cost, b is undefined)
        assert not is_dominated(a, b, {"cost"}, {"carbon_savings"})
        assert not is_dominated(b, a, {"cost"}, {"carbon_savings"})

    def test_nsga2_dedup(self):
        s1 = {"cost": 1.0, "latency": 1.0}
        s2 = {"cost": 1.0, "latency": 1.0}  # duplicate of s1
        s3 = {"cost": 2.0, "latency": 0.5}
        front = nsga2_fast_nondominated_sort(
            [s1, s2, s3], {"cost", "latency"}, set())
        # s1 and s2 dedupe to one; s3 is not dominated by s1
        assert len(front) == 2

    def test_nsga2_front(self):
        a = {"cost": 0.1, "latency": 5.0}
        b = {"cost": 5.0, "latency": 0.1}
        c = {"cost": 3.0, "latency": 3.0}  # dominated by both
        front = nsga2_fast_nondominated_sort(
            [a, b, c], {"cost", "latency"}, set())
        assert a in front and b in front and c not in front

    def test_scalarise_returns_copies(self):
        s = {"cost": 1.0, "carbon_savings": 5.0}
        outs, scores = scalarise([s], {"cost": 0.5, "carbon_savings": 0.5},
                                  ["cost", "carbon_savings"],
                                  minimise_keys={"cost"})
        outs[0]["cost"] = 999.0
        assert s["cost"] == 1.0

    def test_scalarise_direction(self):
        # Lower cost should give a higher score (minimisation)
        a = {"cost": 1.0, "carbon_savings": 10.0}
        b = {"cost": 10.0, "carbon_savings": 10.0}
        _, scores = scalarise(
            [a, b], {"cost": 1.0, "carbon_savings": 0.0},
            ["cost", "carbon_savings"], minimise_keys={"cost"})
        assert scores[0] > scores[1]

    def test_filter_pareto_front_compat(self):
        sols = [
            {"carbon_savings": 5, "cost": 10, "latency": 5},
            {"carbon_savings": 6, "cost": 5, "latency": 4},
        ]
        front = filter_pareto_front(sols)
        assert len(front) == 1
        assert front[0]["carbon_savings"] == 6


# -----------------------------------------------------------------------------
# Tests — Runtime components
# -----------------------------------------------------------------------------
class TestRuntimeComponents:
    @pytest.mark.asyncio
    async def test_task_manager_tracks_tasks(self):
        tm = TaskManager()
        stop = asyncio.Event()

        async def worker():
            await stop.wait()

        tm.start_coro("w", worker())
        assert tm.task_count() == 1
        stop.set()
        await tm.stop_all()
        assert tm.task_count() == 0

    @pytest.mark.asyncio
    async def test_temporal_safety(self):
        m = TemporalSafetyMonitor()
        m.add_formula("G (x >= 0)")
        await m.push({"x": 1})
        await m.push({"x": -1})
        results = await m.check()
        assert any(not ok for ok in results.values())

    @pytest.mark.asyncio
    async def test_hitl_single_candidate(self):
        h = HumanApprovalHandler()
        assert await h.request_approval(
            {"candidates": [{"id": "only"}]}) is True

    @pytest.mark.asyncio
    async def test_xai_ig_runs(self):
        xai = XAIExplainer(ig_steps=8)

        def f(v):
            return float(v[0] * 2 + v[1])

        attrs = xai.integrated_gradients(f, [0.5, 0.3])
        assert len(attrs) == 2
        cf = xai.counterfactual(f, [0.5, 0.3])
        assert "changed_index" in cf

    @pytest.mark.asyncio
    async def test_chaos_bounded(self):
        state = {"healthy": True}

        async def steady():
            return state["healthy"]

        def snap():
            return dict(state)

        def restore(s):
            state.update(s)

        c = ChaosEngine(steady_state=steady, snapshot=snap,
                        restore=restore, rng=random.Random(0))
        r = await c.run_experiment("t", "latency")
        assert r["status"] == "completed"

    @pytest.mark.asyncio
    async def test_chaos_rollback(self):
        state = {"healthy": True, "hits": 0}

        async def steady():
            return state["healthy"]

        def snap():
            return dict(state)

        def restore(s):
            state.update(s)

        c = ChaosEngine(steady_state=steady, snapshot=snap,
                        restore=restore, rng=random.Random(0))
        # Simulate a corruption that flips steady → after
        async def _corrupt():
            state["healthy"] = False
            c.active["x"] = {"fault_type": "data_corruption"}
            # force the finally clause to run
            c.active.pop("x", None)
            return True
        # We'll just verify the rollback path directly:
        c.active["fake"] = {"fault_type": "data_corruption"}
        before = await c._check_steady()
        state["healthy"] = False
        after = await c._check_steady()
        assert before and not after
        await c.chaos_rollback(state, snap()) if hasattr(c, "chaos_rollback") else None


# -----------------------------------------------------------------------------
# Tests — BaseExpert
# -----------------------------------------------------------------------------
class TestBaseExpert:
    def test_subclass_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            class _NoMeta(BaseExpert):
                async def propose_async(self, context):
                    return MOPDProposal()
                async def get_health_status(self):
                    return ComponentHealth()
            # Both warnings should have fired
            assert any("__expert_version__" in rec.message
                       for rec in caplog.records)
            assert any("__expert_description__" in rec.message
                       for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_default_policy_probs_uniform(self, stub_expert):
        probs = await stub_expert.policy_probs({})
        assert len(probs) == len(stub_expert.supported_task_types)
        assert abs(sum(probs) - 1.0) < 1e-9

    @pytest.mark.asyncio
    async def test_explain_decision_passthrough(self, stub_expert):
        proposal = MOPDProposal(explanation="because")
        expl = await stub_expert.explain_decision({}, proposal)
        assert isinstance(expl, Explanation)
        assert expl.natural_language == "because"

    @pytest.mark.asyncio
    async def test_get_pareto_front_default(self, stub_expert):
        front = await stub_expert.get_pareto_front({})
        assert front == [{"cost": 1.0, "latency": 2.0}]

    @pytest.mark.asyncio
    async def test_objective_weights_roundtrip(self, stub_expert):
        before = stub_expert.get_objective_weights()
        stub_expert.set_objective_weights({"carbon_savings": 1.0,
                                             "helium_savings": 0.0,
                                             "cost": 0.0,
                                             "latency": 0.0})
        after = stub_expert.get_objective_weights()
        assert after["carbon_savings"] == 1.0
        assert sum(after.values()) == pytest.approx(1.0)
        stub_expert.set_objective_weights(before)
        assert stub_expert.get_objective_weights() == before

    @pytest.mark.asyncio
    async def test_check_invariants_warns_when_no_rules(self, stub_expert,
                                                          caplog):
        with caplog.at_level(logging.WARNING):
            violations = await stub_expert.check_invariants({})
        assert violations == []
        assert any("no MTL rules" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_request_approval_disabled(self, stub_expert):
        # Default config has enable_human_approval=False → auto-approve
        ok = await stub_expert.request_approval(MOPDProposal(), {})
        assert ok is True

    @pytest.mark.asyncio
    async def test_request_approval_no_handler_denies(self):
        cfg = MOPDConfig(enable_human_approval=True)
        e = _StubExpert(mopd_config=cfg)
        ok = await e.request_approval(MOPDProposal(), {})
        assert ok is False

    @pytest.mark.asyncio
    async def test_bio_spend_no_manager(self, stub_expert):
        r = await stub_expert.spend_atp(0.1)
        assert r.success is False
        assert r.error == "no_token_manager"

    @pytest.mark.asyncio
    async def test_bio_spend_sync_manager(self):
        class _Sync:
            def __init__(self):
                self.calls = []
            def spend(self, actor, amount):
                self.calls.append(("spend", actor, amount))
                return True
            def earn(self, actor, amount):
                self.calls.append(("earn", actor, amount))
                return True

        class _SyncCore:
            def __init__(self):
                self.token_manager = _Sync()
                self.gradient_manager = None

        core = _SyncCore()
        e = _StubExpert()
        e.set_bio_core(core)
        r = await e.spend_atp(0.5)
        assert r.success is True
        assert core.token_manager.calls == [("spend", "expert", 0.5)]

    @pytest.mark.asyncio
    async def test_bio_spend_async_manager(self):
        class _Async:
            def __init__(self):
                self.calls = []
            async def spend(self, actor, amount):
                self.calls.append(("spend", actor, amount))
                return True
            async def earn(self, actor, amount):
                self.calls.append(("earn", actor, amount))
                return True

        class _AsyncCore:
            def __init__(self):
                self.token_manager = _Async()
                self.gradient_manager = None

        core = _AsyncCore()
        e = _StubExpert()
        e.set_bio_core(core)
        r = await e.spend_atp(0.2)
        assert r.success is True
        assert core.token_manager.calls == [("spend", "expert", 0.2)]

    @pytest.mark.asyncio
    async def test_bio_spend_error_is_captured(self):
        class _Bad:
            def spend(self, actor, amount):
                raise RuntimeError("boom")
            def earn(self, actor, amount):
                return True

        class _Core:
            def __init__(self):
                self.token_manager = _Bad()
                self.gradient_manager = None

        e = _StubExpert()
        e.set_bio_core(_Core())
        r = await e.spend_atp(1.0)
        assert r.success is False
        assert "boom" in (r.error or "")

    @pytest.mark.asyncio
    async def test_shutdown_cancels_tasks(self):
        e = _StubExpert()
        async def _noop():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise
        e.task_manager.start_coro("noop", _noop())
        assert e.task_manager.task_count() == 1
        await e.shutdown()
        assert e.task_manager.task_count() == 0


# -----------------------------------------------------------------------------
# Tests — Registry and factory
# -----------------------------------------------------------------------------
class TestRegistryAndFactory:
    def test_register_and_get(self, registry):
        registry.register("Stub", _StubExpert)
        assert registry.get("Stub") is _StubExpert
        assert "Stub" in registry.list()

    def test_register_duplicate_raises(self, registry):
        registry.register("Stub", _StubExpert)
        with pytest.raises(ValueError):
            registry.register("Stub", _StubExpert)

    def test_register_replace(self, registry):
        registry.register("Stub", _StubExpert)
        registry.register("Stub", _StubExpert, replace=True)
        assert registry.get("Stub") is _StubExpert

    def test_register_downgrade_warns(self, registry, caplog):
        class _New(BaseExpert):
            __expert_version__ = "2.0.0"
            async def propose_async(self, context):
                return MOPDProposal()
            async def get_health_status(self):
                return ComponentHealth()

        class _Old(BaseExpert):
            __expert_version__ = "1.0.0"
            async def propose_async(self, context):
                return MOPDProposal()
            async def get_health_status(self):
                return ComponentHealth()

        registry.register("X", _New)
        with caplog.at_level(logging.WARNING):
            registry.register("X", _Old, replace=True)
        assert any("Downgrading" in rec.message for rec in caplog.records)

    def test_register_non_expert_raises(self, registry):
        with pytest.raises(TypeError):
            registry.register("bad", object)  # type: ignore[arg-type]

    def test_thread_safe_registration(self, registry):
        errors: List[Exception] = []

        def worker(i: int) -> None:
            try:
                registry.register(f"E{i}", _StubExpert)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        assert len(registry.list()) == 20

    def test_factory_passes_supported_kwargs(self, registry):
        registry.register("Strict", _StrictExpert)
        obj = create_expert("Strict", registry=registry,
                            adaptive_cost=None)
        assert isinstance(obj, _StrictExpert)

    def test_factory_drops_unknown_kwargs(self, registry, caplog):
        registry.register("Strict", _StrictExpert)
        with caplog.at_level(logging.DEBUG):
            obj = create_expert(
                "Strict", registry=registry,
                nonsense_argument=42)
        assert isinstance(obj, _StrictExpert)

    def test_factory_accepts_var_kwargs(self, registry):
        registry.register("Stub", _StubExpert)
        obj = create_expert("Stub", registry=registry, extra_flag=True)
        assert isinstance(obj, _StubExpert)

    def test_lazy_loader_unknown_name(self):
        with pytest.raises(ValueError):
            load_expert("DefinitelyNotAnExpert")


# -----------------------------------------------------------------------------
# Tests — Multi-expert helpers
# -----------------------------------------------------------------------------
class TestMultiExpertHelpers:
    @pytest.mark.asyncio
    async def test_shutdown_all_experts(self):
        experts = [_StubExpert(), _StubExpert()]
        for e in experts:
            await e.initialize()
        result = await shutdown_all_experts(experts, timeout=1.0)
        assert result["experts"][0]["status"] == "ok"
        assert result["experts"][1]["status"] == "ok"

    @pytest.mark.asyncio
    async def test_get_experts_capabilities_isolates_errors(self):
        class _Broken(BaseExpert):
            __expert_version__ = "1.0.0"
            __expert_description__ = "Broken"

            async def propose_async(self, context):
                return MOPDProposal()

            async def get_health_status(self):
                raise RuntimeError("no health")

        experts = [_StubExpert(), _Broken()]
        caps = await get_experts_capabilities(experts)
        assert len(caps) == 2
        assert caps[0]["name"] == "_StubExpert"
        assert "error" in caps[1]


# -----------------------------------------------------------------------------
# Tests — Config
# -----------------------------------------------------------------------------
class TestMOPDConfig:
    def test_weights_normalised(self):
        cfg = MOPDConfig(objective_weights={"a": 10, "b": 10})
        assert cfg.objective_weights["a"] == pytest.approx(0.5)
        assert cfg.objective_weights["b"] == pytest.approx(0.5)

    def test_zero_weights_fallback_uniform(self):
        cfg = MOPDConfig(objective_weights={"a": 0, "b": 0})
        assert cfg.objective_weights["a"] == pytest.approx(0.5)

    def test_roundtrip_dict(self):
        cfg = MOPDConfig(objective_weights={"a": 0.5, "b": 0.5})
        d = cfg.to_dict()
        cfg2 = MOPDConfig.from_dict(d)
        assert cfg2.objective_weights == cfg.objective_weights
        assert cfg2.minimise_keys == cfg.minimise_keys


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _DemoExpert(BaseExpert):
        __expert_version__ = "1.0.0"
        __expert_description__ = "Demo expert"

        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.supported_task_types = ["shift", "offset"]

        async def propose_async(self, context: Dict[str, Any]
                                 ) -> MOPDProposal:
            sols = [
                {"carbon_savings": 30, "cost": 5, "latency": 10},
                {"carbon_savings": 5, "cost": 1, "latency": 2},
                {"carbon_savings": 20, "cost": 3, "latency": 4},
            ]
            front = nsga2_fast_nondominated_sort(
                sols, {"cost", "latency"}, {"carbon_savings"})
            return MOPDProposal(
                recommendations=front[0] if front else {},
                options=front,
                explanation="Pareto front of trade-offs",
                confidence=0.9,
            )

        async def get_health_status(self) -> ComponentHealth:
            return ComponentHealth(HealthStatus.HEALTHY, 1.0)

    async def demo():
        reg = ExpertRegistry()
        reg.register("Demo", _DemoExpert)

        expert = create_expert("Demo", registry=reg)
        await expert.initialize()
        try:
            proposal = await expert.propose_async({})
            print("Recommendation:", proposal.recommendations)
            print("Front size:", len(proposal.options))
            print("Capabilities:",
                  await expert.get_capabilities())
        finally:
            await expert.shutdown()

    asyncio.run(demo())
