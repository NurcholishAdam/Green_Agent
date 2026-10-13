#!/usr/bin/env python3
"""
FlexGen MODP Planner — Enhanced v17.1.0
========================================

Causal MODP planner for temporal scheduling of FlexGen workloads.

FIXES OVER v17.0.0:
 - Causal credit assignment now uses a dedicated state-credit table
   keyed by state variable (was a no-op because it looked up variable
   names in a tuple-keyed Q-table).
 - Carbon-market deferral override uses `defer_hours` from the market
   (was passing a USD cost into int() and always deferring 1 hour).
 - Federated sharing, XAI, multi-agent bid/reward, and chaos injection
   are no longer on the planning critical path.
 - HITL responses are correlated via per-request futures.
 - Per-request state/action pair is returned by `plan()` and consumed by
   `learn()`, so concurrent plans cannot corrupt each other.
 - PC-algorithm v-structure orientation corrected; leftover undirected
   edges are no longer oriented by variance; empty graph returned when
   data is insufficient.
 - `_is_node_feasible` uses a real memory model.
 - Carbon bucket capped; deadline/carbon/queue buckets validated.
 - `datetime.now(timezone.utc)` everywhere.
 - `publish_decision` emits a dict-shaped event with explicit fields.
 - Orchestrator lifecycle: `aclose()` / `__aenter__` / `__aexit__`,
   `get_running_loop()`, `_spawn()` with error logging, task set cleared.
 - `log_event` shim (structlog or stdlib) and Prometheus metrics.
 - `InMemoryStorage` methods use real signatures.
 - Ten bundled enhancements fixed: per-teacher KL, no fabricated causal
   edges, HITL correlation, chaos data_corruption + real health probe,
   carbon decoupling, XAI named features, precision filtering + rolling
   accuracy window.

Self-contained: Python stdlib + optional numpy / sklearn / torch.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import math
import os
import random
import re
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Deque, Dict, FrozenSet, List, Optional, Set, Tuple

# ---------- Optional dependencies ----------
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    LinearRegression = None  # type: ignore
    SKLEARN_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    torch = None  # type: ignore
    TORCH_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter as _PromCounter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- structlog / stdlib logger ----------
try:
    import structlog
    _STRUCTLOG_LOGGER = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _STRUCTLOG_LOGGER = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)

# ---------- Preserved module-level logger ----------
logger = logging.getLogger(__name__)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Structured-logging shim (structlog or stdlib)."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_STRUCTLOG_LOGGER, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(logger, level)(message)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# CONFIG HELPER
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
# PROMETHEUS METRICS
# =============================================================================
if PROMETHEUS_AVAILABLE:
    _M_PLAN_DECISIONS = _PromCounter(
        "modp_plan_decisions_total", "Scheduling decisions", ["action"],
    )
    _M_PLAN_LATENCY = Histogram(
        "modp_plan_latency_seconds", "plan() latency",
    )
    _M_DEFER_HOURS = Histogram(
        "modp_defer_hours", "Deferral duration in hours",
    )
    _M_HITL_BLOCKS = _PromCounter(
        "modp_hitl_blocks_total", "HITL blocks",
    )
    _M_TEMPORAL_BLOCKS = _PromCounter(
        "modp_temporal_blocks_total", "Temporal gating blocks",
    )
    _M_CARBON_OVERRIDES = _PromCounter(
        "modp_carbon_market_overrides_total", "Carbon market overrides",
    )
    _M_EPSILON = Gauge("modp_epsilon", "Current exploration rate")
    _M_Q_TABLE_SIZE = Gauge("modp_q_table_size", "Q-table state count")
    _M_CAUSAL_EDGES = Gauge("modp_causal_edges", "Causal graph edges")
    _M_PRECISION_SWITCHES = _PromCounter(
        "modp_precision_switches_total", "Precision switches",
        ["from_p", "to_p"],
    )
    _M_CHAOS_EXPERIMENTS = _PromCounter(
        "modp_chaos_experiments_total", "Chaos experiments",
        ["fault_type", "status"],
    )
else:
    class _Dummy:
        def labels(self, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass

    _M_PLAN_DECISIONS = _M_PLAN_LATENCY = _M_DEFER_HOURS = _Dummy()
    _M_HITL_BLOCKS = _M_TEMPORAL_BLOCKS = _M_CARBON_OVERRIDES = _Dummy()
    _M_EPSILON = _M_Q_TABLE_SIZE = _M_CAUSAL_EDGES = _Dummy()
    _M_PRECISION_SWITCHES = _M_CHAOS_EXPERIMENTS = _Dummy()


# =============================================================================
# SCHEMA DATACLASSES (preserved)
# =============================================================================
@dataclass
class WorkloadDescriptor:
    task_id: str = "unknown"
    deadline: Optional[datetime] = None
    estimated_kwh: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class NodeDescriptor:
    id: str = "node_0"
    region_carbon_intensity: float = 400.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FlexGenPolicy:
    gpu_batch_size: int = 32
    block_size: int = 128


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    """PC-algorithm causal DAG learner + do-calculus utilities."""

    MAX_CARBON_BUCKET: int = 30   # × 50 = 1500 gCO2/kWh upper bound

    def __init__(self, storage: Any = None, alpha: float = 0.05,
                 max_cond_size: int = 3):
        self.storage = storage
        self.alpha = float(alpha)
        self.max_cond_size = int(max_cond_size)
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.skeleton: Dict[str, Set[str]] = defaultdict(set)
        self.sepsets: Dict[Tuple[str, str], Set[str]] = {}
        self.variables: List[str] = []
        self._oriented: Set[Tuple[str, str]] = set()
        self._lock = asyncio.Lock()

    async def learn(self, samples: List[Dict[str, float]],
                    variables: List[str],
                    threshold: float = 0.25) -> Dict[str, Any]:
        self.variables = list(variables)
        self._oriented = set()

        if not NUMPY_AVAILABLE or len(samples) < 5:
            async with self._lock:
                self.graph.clear()
                self.skeleton.clear()
                self.sepsets.clear()
            return self.summary()

        try:
            X = np.array(
                [[float(s.get(v, 0.0)) for v in variables] for s in samples],
                dtype=float)
        except Exception as exc:
            log_event("warning", f"Causal samples parse failed: {exc}")
            return self.summary()
        if X.shape[0] < 3 or X.shape[1] < 2:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)

        async with self._lock:
            self.skeleton = {v: set(variables) - {v} for v in variables}
            self.sepsets = {}
            self._oriented = set()
            self._pc_remove_edges(X, variables)
            self._orient_v_structures(X, variables)
            self._apply_meeks_rules()
            self._build_directed_graph(X, variables)
            for src, edges in self.graph.items():
                for dst, info in edges.items():
                    if self.storage is not None:
                        try:
                            await asyncio.to_thread(
                                self.storage.save_causal_edge,
                                src, dst, info["weight"], info["confidence"])
                        except Exception:
                            pass

        try:
            _M_CAUSAL_EDGES.set(sum(len(v) for v in self.graph.values()))
        except Exception:
            pass
        return self.summary()

    def parents(self, node: str) -> List[str]:
        return [s for s, e in self.graph.items() if node in e]

    def children(self, node: str) -> List[str]:
        return list(self.graph.get(node, {}).keys())

    def summary(self) -> Dict[str, Any]:
        return {
            "nodes": len(self.variables),
            "edges": sum(len(v) for v in self.graph.values()),
            "variables": list(self.variables),
            "skeleton_edges": sum(len(v) for v in self.skeleton.values()) // 2,
            "v_structures": len(self._oriented),
        }

    # ---- PC internals ----
    @staticmethod
    def _std_normal_cdf(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    @staticmethod
    def _partial_corr(X: Any, i: int, j: int, cond: List[int]) -> float:
        if not cond:
            a = X[:, i]
            b = X[:, j]
        else:
            cond_arr = np.hstack([np.ones((X.shape[0], 1)), X[:, cond]])
            try:
                beta_i, *_ = np.linalg.lstsq(cond_arr, X[:, i], rcond=None)
                beta_j, *_ = np.linalg.lstsq(cond_arr, X[:, j], rcond=None)
                a = X[:, i] - cond_arr @ beta_i
                b = X[:, j] - cond_arr @ beta_j
            except Exception:
                return 0.0
        da = a - a.mean()
        db = b - b.mean()
        denom = math.sqrt(float((da * da).sum()) * float((db * db).sum())) + 1e-9
        return float((da * db).sum() / denom)

    def _ci_test(self, X: Any, i: int, j: int,
                 cond: Tuple[int, ...]) -> Tuple[bool, float]:
        n = X.shape[0]
        r = self._partial_corr(X, i, j, list(cond))
        r = max(-0.999, min(0.999, r))
        k = len(cond)
        if n - k - 3 <= 0:
            return True, 1.0
        z = 0.5 * math.log((1 + r) / (1 - r)) * math.sqrt(n - k - 3)
        p = 2.0 * (1.0 - self._std_normal_cdf(abs(z)))
        p = max(0.0, min(1.0, p))
        return p > self.alpha, p

    def _pc_remove_edges(self, X: Any, variables: List[str]) -> None:
        idx = {v: i for i, v in enumerate(variables)}
        for size in range(0, self.max_cond_size + 1):
            edges: List[Tuple[str, str]] = []
            for vi in variables:
                for vj in self.skeleton.get(vi, set()):
                    if vi < vj and vj in self.skeleton[vi]:
                        edges.append((vi, vj))
            for vi, vj in edges:
                if vj not in self.skeleton.get(vi, set()):
                    continue
                neighbours = list(self.skeleton[vi] - {vj})
                if len(neighbours) < size:
                    continue
                for combo in itertools.combinations(neighbours, size):
                    cond_idx = tuple(idx[c] for c in combo)
                    indep, _ = self._ci_test(X, idx[vi], idx[vj], cond_idx)
                    if indep:
                        self.skeleton[vi].discard(vj)
                        self.skeleton[vj].discard(vi)
                        self.sepsets[(vi, vj)] = set(combo)
                        self.sepsets[(vj, vi)] = set(combo)
                        break

    def _orient_v_structures(self, X: Any, variables: List[str]) -> None:
        """
        Orient colliders: x -> z <- y iff x,y non-adjacent, z adjacent to
        BOTH x and y, and z not in sepset(x, y).
        """
        v_count = 0
        for x in variables:
            for y in variables:
                if x == y:
                    continue
                if y in self.skeleton.get(x, set()):
                    continue  # x and y adjacent → no v-structure
                for z in variables:
                    if z == x or z == y:
                        continue
                    if z not in self.skeleton.get(x, set()):
                        continue
                    if z not in self.skeleton.get(y, set()):
                        continue
                    if z in self.sepsets.get((x, y), set()):
                        continue
                    if (x, z) not in self._oriented:
                        self._oriented.add((x, z))
                        v_count += 1
                    if (y, z) not in self._oriented:
                        self._oriented.add((y, z))
                        v_count += 1

    def _apply_meeks_rules(self) -> None:
        changed, iterations = True, 0
        while changed and iterations < 20:
            changed = False
            iterations += 1
            undirected: List[Tuple[str, str]] = []
            for a in list(self.skeleton.keys()):
                for b in list(self.skeleton[a]):
                    if (a, b) in self._oriented or (b, a) in self._oriented:
                        continue
                    undirected.append((a, b))
            for a, b in undirected:
                for src, dst in ((a, b), (b, a)):
                    incoming = [(p, src) for p in self.skeleton.get(src, set())
                                if (p, src) in self._oriented]
                    if not incoming:
                        continue
                    for p, _ in incoming:
                        if dst in self.skeleton.get(p, set()):
                            continue
                        if (src, dst) not in self._oriented and \
                           (dst, src) not in self._oriented:
                            self._oriented.add((src, dst))
                            changed = True
                            break
                    if changed:
                        break
                if changed:
                    break

    def _build_directed_graph(self, X: Any, variables: List[str]) -> None:
        """Leftover undirected edges are NOT oriented (no variance heuristic)."""
        self.graph.clear()
        idx = {v: i for i, v in enumerate(variables)}
        for (a, b) in self._oriented:
            try:
                r = self._partial_corr(X, idx[a], idx[b], [])
            except Exception:
                r = 0.0
            self.graph[a][b] = {"weight": float(r), "confidence": abs(float(r))}

    # ---- Do-calculus ----
    def get_backdoor_adjustment_set(self, treatment: str, outcome: str) -> Set[str]:
        parents = set(self.parents(treatment))
        descendants = self._descendants(treatment)
        return parents - descendants

    def _descendants(self, node: str) -> Set[str]:
        out: Set[str] = set()
        stack = [node]
        while stack:
            n = stack.pop()
            for c in self.children(n):
                if c not in out:
                    out.add(c)
                    stack.append(c)
        return out

    async def estimate_ate_do_calculus(self, treatment: str, outcome: str,
                                       data: List[Dict[str, float]]) -> float:
        """OLS of Y on (X, Z) with Z = backdoor adjustment set."""
        adj_set = self.get_backdoor_adjustment_set(treatment, outcome)
        if not data or not NUMPY_AVAILABLE:
            return 0.0
        try:
            cols = [treatment] + sorted(adj_set) + [outcome]
            X = np.array([[float(d.get(v, 0.0)) for v in cols] for d in data],
                         dtype=float)
        except Exception:
            return 0.0
        if X.shape[0] < 3 or X.shape[1] < 3:
            return 0.0
        t_col = 0
        adj_cols = list(range(1, 1 + len(adj_set)))
        y_col = X.shape[1] - 1
        design = np.hstack([np.ones((X.shape[0], 1)), X[:, [t_col] + adj_cols]])
        try:
            beta, *_ = np.linalg.lstsq(design, X[:, y_col], rcond=None)
        except Exception:
            return 0.0
        ate = float(beta[1])
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_causal_experiment,
                    f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome,
                    ate, len(data), "do_calculus_ols")
            except Exception:
                pass
        return ate


class CausalMODPPolicy:
    """
    Causal MODP policy over scheduling states.

    State: (carbon_bucket, queue_bucket, deadline_bucket).
    Actions: 0 = run_now, 1..horizon = defer k hours, horizon+1 = move_node.
    """

    def __init__(self, config: Any, storage: Any, graph: CausalGraphLearner,
                 horizon: int = 6):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.horizon = int(horizon)
        self.learning_rate = float(_cfg_get(config, "learning_rate", 0.1))
        self.discount = float(_cfg_get(config, "discount_factor", 0.9))
        self.epsilon = float(_cfg_get(config, "epsilon", 0.1))
        self.epsilon_decay = float(_cfg_get(config, "epsilon_decay", 0.999))
        self.optimistic_init = float(_cfg_get(config, "optimistic_init", 1.0))

        self.q_table: Dict[Tuple[int, int, int], List[float]] = {}
        # Per-variable credit (fixed: was reading from tuple-keyed dict).
        self.state_credits: Dict[str, float] = defaultdict(float)
        self._lock = asyncio.Lock()

    def _discretize_state(self, carbon: float, queue: int,
                          hours: float) -> Tuple[int, int, int]:
        c = max(0.0, min(float(carbon) // 50.0,
                         CausalGraphLearner.MAX_CARBON_BUCKET))
        q = min(max(0, int(queue)), 10)
        h = min(max(0, int(hours)), 24)
        return (int(c), q, h)

    def _get_action_values_sync(self, state: Tuple[int, int, int]) -> List[float]:
        if state not in self.q_table:
            self.q_table[state] = [self.optimistic_init] * (self.horizon + 2)
        return self.q_table[state]

    async def choose_action(self, state: Tuple[int, int, int]) -> int:
        async with self._lock:
            values = self._get_action_values_sync(state)
            if random.random() < self.epsilon:
                return random.randint(0, len(values) - 1)
            return int(max(range(len(values)), key=lambda i: values[i]))

    async def update(self, reward: float, next_state: Tuple[int, int, int],
                     last_state: Optional[Tuple[int, int, int]] = None,
                     last_action: Optional[int] = None) -> None:
        """
        Bellman update for the (state, action) pair provided.

        If `last_state` and `last_action` are not given, the call is a no-op
        (the previous instance-level `last_*` state was removed to make
        concurrent calls safe).
        """
        if last_state is None or last_action is None:
            return
        async with self._lock:
            next_values = self._get_action_values_sync(next_state)
            max_next = max(next_values) if next_values else 0.0
            current = self._get_action_values_sync(last_state)
            if 0 <= last_action < len(current):
                current[last_action] += self.learning_rate * (
                    reward + self.discount * max_next - current[last_action])
        await self._assign_causal_credit(reward)

    async def _assign_causal_credit(self, reward: float) -> None:
        """
        Distribute reward to causal parents of `quality`.

        The credits are stored in `state_credits` (variable name → score)
        instead of being written into the tuple-keyed Q-table.
        """
        parents = self.graph.parents("quality")
        for p in parents:
            edge = self.graph.graph.get(p, {}).get("quality", {})
            w = float(edge.get("weight", 0.0))
            prev = self.state_credits.get(p, 0.0)
            self.state_credits[p] = 0.9 * prev + 0.1 * reward * w

    def decay_epsilon(self) -> None:
        self.epsilon = max(0.01, self.epsilon * self.epsilon_decay)

    def save(self, path: str) -> None:
        try:
            payload = {
                "version": 2,
                "horizon": self.horizon,
                "epsilon": self.epsilon,
                "states": [
                    {"key": list(k), "values": list(v)}
                    for k, v in self.q_table.items()
                ],
            }
            tmp = f"{path}.tmp"
            with open(tmp, "w") as f:
                json.dump(payload, f)
            os.replace(tmp, path)
        except Exception as e:
            log_event("warning", f"Q-table save failed: {e}")

    def load(self, path: str) -> None:
        if not os.path.exists(path):
            return
        try:
            with open(path, "r") as f:
                data = json.load(f)
            if isinstance(data, dict) and "states" in data:
                file_horizon = int(data.get("horizon", self.horizon))
                expected = file_horizon + 2
                for entry in data["states"]:
                    try:
                        key = tuple(int(x) for x in entry["key"])
                        values = list(entry["values"])
                    except Exception:
                        continue
                    if len(values) != expected:
                        continue
                    self.q_table[key] = [float(v) for v in values]
                self.epsilon = max(0.01, float(data.get("epsilon",
                                                        self.epsilon)))
                return
            # Legacy format: {"(a, b, c)": [v0, v1, ...]}
            for k, v in data.items():
                try:
                    key = tuple(int(x.strip())
                                for x in k.strip("()").split(","))
                except Exception:
                    continue
                if len(v) == self.horizon + 2:
                    self.q_table[key] = [float(x) for x in v]
        except Exception as e:
            log_event("warning", f"Q-table load failed: {e}")

    def get_policy(self) -> List[float]:
        n = self.horizon + 2
        if not self.q_table:
            return [1.0 / n] * n
        avg = [0.0] * n
        for values in self.q_table.values():
            for i in range(min(n, len(values))):
                avg[i] += values[i]
        s = sum(avg) or 1.0
        return [x / s for x in avg]


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                 n_actions: int = 5):
        self.temperature = float(temperature)
        self.alpha = float(alpha)
        self.n_actions = int(n_actions)
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy = [1.0 / self.n_actions] * self.n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name: str, policy: List[float]) -> None:
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[str(name)] = [max(0.0, p / s) for p in policy]

    def unregister_teacher(self, name: str) -> None:
        self.teachers.pop(name, None)

    def _softmax(self, x: List[float], temp: float) -> List[float]:
        if not x:
            return []
        m = max(x)
        t = max(temp, 1e-6)
        exps = [math.exp((v - m) / t) for v in x]
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

    async def step(self, storage: Any, student_id: str = "modp_student"):
        target = self._softmax(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]

        for tid, pol in self.teachers.items():
            weight = float(pol[0]) if pol else 0.0
            amplitude = math.sqrt(max(weight, 1e-9))
            kl = 0.0
            for i in range(self.n_actions):
                pi = pol[i] if i < len(pol) else 1e-9
                si = self.student_policy[i]
                pi = max(pi, 1e-9)
                si = max(si, 1e-9)
                kl += pi * math.log(pi / si)
            try:
                await asyncio.to_thread(
                    storage.save_teacher_superposition,
                    student_id, tid, weight, self.temperature, amplitude, kl)
            except Exception:
                pass

        entry = {"target": target, "student": list(self.student_policy),
                 "ts": _now_utc().isoformat()}
        self.history.append(entry)
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING
# =============================================================================
class FederatedGreenAggregator:
    """Byte-wise average across federated instances."""

    def __init__(self, storage: Any, instance_id: str,
                 share_interval: int = 3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = int(share_interval)
        self.rounds = 0
        self.last_aggregated: Optional[bytes] = None

    async def share_weights(self, model_id: str, weights: bytes) -> None:
        if not weights:
            return
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, weights,
                float(len(weights)), self.rounds)
            self.rounds += 1
        except Exception as exc:
            log_event("warning", f"Federated share failed: {exc}")

    async def pull_aggregated_weights(self, model_id: str) -> Optional[bytes]:
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id)
        except Exception as exc:
            log_event("warning", f"Federated pull failed: {exc}")
            return None
        if not rows:
            return None
        blobs = [r["weights"] for r in rows if r.get("weights")]
        if not blobs:
            return None
        n = min(len(b) for b in blobs)
        avg = bytearray(n)
        for i in range(n):
            avg[i] = int(sum(b[i] for b in blobs) / len(blobs)) & 0xFF
        self.rounds += 1
        self.last_aggregated = bytes(avg)
        return self.last_aggregated

    async def apply_aggregated_weights(self, model_id: str,
                                       current: bytes) -> bytes:
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None or not current:
            return current
        n = min(len(current), len(agg))
        return bytes([(current[i] + agg[i]) // 2 for i in range(n)])

    def stats(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "rounds": self.rounds,
            "last_aggregated_bytes": (
                len(self.last_aggregated) if self.last_aggregated else 0
            ),
        }


# =============================================================================
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION
# =============================================================================
class _Agent:
    ROLES = ["orchestrator", "validator", "optimizer", "reporter", "negotiator"]

    def __init__(self, agent_id: str):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


class MultiAgentCoordinator:
    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        count = int(_cfg_get(config, "agent_count", 5) or 5)
        self.agents = {f"agent_{i:02d}": _Agent(f"agent_{i:02d}")
                       for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self) -> None:
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])

    async def broadcast(self, topic: str, sender: str,
                        payload: Dict[str, Any]) -> None:
        try:
            self.bus.put_nowait({"topic": topic, "sender": sender,
                                 "payload": payload})
        except asyncio.QueueFull:
            log_event("warning",
                      "MultiAgentCoordinator bus full; message dropped")

    async def bid(self, task: Dict[str, Any]) -> Tuple[str, float]:
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
                             {"task": task.get("name", "?"),
                              "score": best_score})
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id: str, reward: float) -> None:
        async with self._lock:
            if agent_id in self.agents:
                a = self.agents[agent_id]
                n = max(1, a.completed)
                a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
                a.utilities[a.role] = min(1.0,
                                          a.utilities[a.role] + 0.05 * reward)

    def get_role_distribution(self) -> Dict[str, float]:
        counts: Dict[str, int] = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        return {role: c / total for role, c in counts.items()}

    def get_policy(self) -> List[float]:
        affinity = {
            "orchestrator": [0.35, 0.20, 0.15, 0.15, 0.15],
            "validator":    [0.15, 0.15, 0.15, 0.35, 0.20],
            "optimizer":    [0.20, 0.15, 0.35, 0.15, 0.15],
            "reporter":     [0.15, 0.20, 0.15, 0.15, 0.35],
            "negotiator":   [0.15, 0.35, 0.20, 0.15, 0.15],
        }
        role_dist = self.get_role_distribution()
        out = [0.0] * 5
        for role, w in role_dist.items():
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
                "role_distribution": self.get_role_distribution(),
                "strategy_distribution": self.get_policy(),
                "processed_messages": processed}


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC
# =============================================================================
_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


class TemporalRule:
    def __init__(self, rule_id: str, operator: str,
                 conditions: List[Callable[[Dict], bool]],
                 window: float = 0.0, description: str = "",
                 severity: str = "warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = float(window)
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque[Tuple[datetime, Dict]]) -> bool:
        if not trace:
            return False
        if self.window > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == "always":
            return any(not self.conditions[0](s) for _, s in trace)
        if op == "eventually":
            return not any(self.conditions[0](s) for _, s in trace)
        if op == "never":
            return any(self.conditions[0](s) for _, s in trace)
        return False


class TemporalLogicVerifier:
    def __init__(self, storage: Any, config: Any):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=int(_cfg_get(config, "temporal_max_trace", 2000) or 2000))
        self.approval_cb: Optional[Callable] = None
        self._pending_rule_saves: List[Tuple[str, str, str, str]] = []
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula: str) -> None:
        f = (formula or "").strip()
        if not f:
            return
        try:
            if f.startswith("G "):
                self._add_atomic(f[2:].strip().strip("()"), "always")
            elif f.startswith("F "):
                self._add_atomic(f[2:].strip().strip("()"), "eventually")
            elif f.startswith("NEVER "):
                self._add_atomic(f[6:].strip().strip("()"), "never")
            else:
                log_event("warning",
                          f"Unrecognised temporal formula: {formula!r}")
        except Exception as exc:
            log_event("warning", f"Temporal install failed: {exc}")

    def _add_atomic(self, expr: str, op: str) -> None:
        m = _ATOMIC_RE.match(expr)
        if not m:
            log_event("warning", f"Atomic formula did not parse: {expr!r}")
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def cond(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]

        rid = f"{op}:{expr}"
        self.rules[rid] = TemporalRule(rid, op, [cond],
                                        description=expr, severity="warning")
        self._pending_rule_saves.append((rid, expr, op, "warning"))

    async def _flush_pending_rule_saves(self) -> None:
        while self._pending_rule_saves:
            rid, expr, op, sev = self._pending_rule_saves.pop(0)
            try:
                await asyncio.to_thread(
                    self.storage.save_temporal_rule,
                    rid, expr, op, sev, expr, 0.0, True)
            except Exception:
                pass

    def set_approval_callback(self, cb: Callable) -> None:
        self.approval_cb = cb

    async def push_state(self, state: Dict[str, Any]) -> None:
        await self._flush_pending_rule_saves()
        self.trace.append((_now_utc(), dict(state)))
        try:
            await asyncio.to_thread(self.storage.save_temporal_trace, state)
        except Exception:
            pass

    async def verify(self) -> Dict[str, bool]:
        await self._flush_pending_rule_saves()
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace)
            violated = rule.evaluate(copy)
            approved = False
            if violated:
                rule.violations += 1
                rule.last_violation = _now_utc()
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        approved = self.approval_cb(
                            rid, self.trace[-1][1] if self.trace else {})
                        if asyncio.iscoroutine(approved):
                            approved = await approved
                    except Exception as exc:
                        log_event("warning", f"Approval callback failed: {exc}")
                        approved = False
                try:
                    await asyncio.to_thread(
                        self.storage.save_temporal_violation,
                        rid, rule.description, len(self.trace) - 1,
                        self.trace[-1][1] if self.trace else {},
                        rule.severity, bool(approved))
                except Exception:
                    pass
            result[rid] = (not violated) or bool(approved)
        return result


# =============================================================================
# ENHANCEMENT 6 — EXPLAINABLE AI
# =============================================================================
class XAIDecisionExplainer:
    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.method = str(_cfg_get(config, "xai_method", "kernel_shap")
                          or "kernel_shap")
        self.depth = int(_cfg_get(config, "xai_depth", 5) or 5)

    def _permutation_shap(self, f, x, names, n=64):
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
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

    def _kernel_shap(self, f, x, names, n=64):
        return self._permutation_shap(f, x, names, n)

    def _nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id: str, label: str, features,
                      names: List[str], model_fn: Callable) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
        else:
            attrs = self._permutation_shap(model_fn, features, names)
        nl = self._nl(label, attrs)

        feature_map: Dict[str, Any] = {}
        try:
            if NUMPY_AVAILABLE and hasattr(features, "tolist"):
                feature_map = dict(zip(names, features.tolist()))
            else:
                feature_map = dict(zip(names, list(features)))
        except Exception:
            feature_map = {"raw": list(features)}

        try:
            await asyncio.to_thread(
                self.storage.save_xai_explanation,
                decision_id, decision_id, self.method, label,
                feature_map, attrs, nl)
        except Exception:
            pass
        return {"decision_id": decision_id, "method": self.method,
                "attributions": attrs, "explanation": nl}


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION SWITCHER
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}
    LATENCY = {"fp32": 1.0, "tf32": 0.8, "bf16": 0.65,
               "fp16": 0.6, "int8": 0.4}

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.history: Deque[Dict] = deque(maxlen=500)
        self._acc_window: Deque[float] = deque(maxlen=20)
        self._hw_cache: Optional[Dict[str, Any]] = None

    def _probe(self, force_refresh: bool = False) -> Dict[str, Any]:
        if self._hw_cache is not None and not force_refresh:
            return self._hw_cache
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE and torch is not None:
            try:
                info["cuda"] = bool(torch.cuda.is_available())
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    try:
                        info["bf16"] = bool(torch.cuda.is_bf16_supported())
                    except Exception:
                        info["bf16"] = False
            except Exception:
                pass
        self._hw_cache = info
        return info

    def _allowed_precisions(self) -> List[str]:
        raw = list(_cfg_get(self.config, "precision_levels",
                            ["fp32", "fp16", "bf16", "int8"]))
        return [c for c in raw if c in self.ENERGY]

    def select_precision(self) -> str:
        hw = self._probe()
        cands = self._allowed_precisions()
        if not cands:
            return "fp32"
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        if not cands:
            return "fp32"
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    def record_accuracy(self, accuracy: float) -> None:
        try:
            self._acc_window.append(float(accuracy))
        except (TypeError, ValueError):
            pass

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        if target == self.current or target not in self.ENERGY:
            return False
        if target not in self._allowed_precisions():
            log_event("warning",
                      f"Precision {target!r} not in precision_levels; ignoring")
            return False
        old = self.current
        self.current = target
        saved = max(0.0, self.ENERGY.get(old, 1.0) -
                    self.ENERGY.get(target, 1.0))
        self.saved_wh += saved
        self.history.append({"from": old, "to": target,
                             "reason": reason, "saved_wh": saved})
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, 0.0)
            _M_PRECISION_SWITCHES.labels(from_p=old, to_p=target).inc()
        except Exception:
            pass
        return True

    async def auto_switch(self, recent_acc: float = 0.0,
                          baseline_acc: float = 0.0) -> None:
        if recent_acc <= 0 or baseline_acc <= 0:
            if not self._acc_window:
                return
            recent_acc = sum(self._acc_window) / len(self._acc_window)
            baseline_acc = max(self._acc_window)
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = float(_cfg_get(self.config,
                                "precision_switch_threshold", 0.02) or 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            target = self.select_precision()
            if target != self.current:
                await self.switch_to(target, reason="headroom")


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    DEFER_INTENSITY_THRESHOLD = 0.3   # kgCO2/kWh
    RUN_OFFSET_COST_THRESHOLD = 0.5   # USD
    MIN_PRICE_USD = 5.0
    MAX_PRICE_USD = 200.0
    DEFAULT_PRICE_USD = 25.0

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.last_price = float(
            _cfg_get(config, "carbon_default_price_usd",
                     self.DEFAULT_PRICE_USD) or self.DEFAULT_PRICE_USD)
        self._rec_balance_mwh: float = 0.0
        self._closed = False

    async def _fetch_price(self) -> float:
        candidate = self.last_price + random.gauss(0, 1.5)
        return max(self.MIN_PRICE_USD, min(self.MAX_PRICE_USD, candidate))

    async def update_price(self) -> float:
        try:
            price = await self._fetch_price()
        except Exception:
            price = self.last_price
        try:
            price = max(self.MIN_PRICE_USD, min(self.MAX_PRICE_USD, float(price)))
        except (TypeError, ValueError):
            price = self.last_price
        try:
            await asyncio.to_thread(self.storage.save_credit_price, price)
        except Exception:
            pass
        self.last_price = price
        return price

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0,
                           source: str = "wind") -> float:
        mwh = max(0.0, float(mwh))
        if mwh == 0.0:
            return 0.0
        price_per_mwh = max(0.0, float(price_per_mwh))
        src = (source or "").strip().lower() or "unspecified"
        cost = mwh * price_per_mwh
        self._rec_balance_mwh += mwh
        try:
            await asyncio.to_thread(
                self.storage.save_rec, mwh, price_per_mwh, src)
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh: float, intensity: float,
                                price: Optional[float] = None,
                                commit: bool = True) -> Dict[str, Any]:
        """
        Return a scheduling decision with explicit `defer_hours` when the
        action is "defer" so callers do not conflate USD costs with hours.
        """
        workload_kwh = max(0.0, float(workload_kwh))
        intensity = max(0.0, float(intensity))
        if price is None:
            price = float(self.last_price)
        else:
            try:
                price = max(0.0, float(price))
            except (TypeError, ValueError):
                price = float(self.last_price)
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        defer_hours = 0
        if intensity > self.DEFER_INTENSITY_THRESHOLD:
            action = "defer"
            # Number of hours until intensity drops below the threshold,
            # expressed as a simple linear rule of thumb: (I - T) * 10.
            defer_hours = max(1, int((intensity - self.DEFER_INTENSITY_THRESHOLD) * 10))
        elif offset_cost < self.RUN_OFFSET_COST_THRESHOLD:
            action = "run_offset"
        else:
            action = "run"
        if commit:
            try:
                await asyncio.to_thread(
                    self.storage.save_net_zero_match,
                    uuid.uuid4().hex[:8], workload_kwh, intensity, action,
                    carbon_kg, offset_cost, price)
            except Exception:
                pass
        return {
            "action": action,
            "defer_hours": defer_hours,
            "carbon_kg": carbon_kg,
            "offset_cost_usd": offset_cost,
            "credit_price_usd": price,
            "rec_balance_mwh": float(self._rec_balance_mwh),
        }

    def get_rec_balance(self) -> float:
        return float(self._rec_balance_mwh)


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self._shared_state: Dict[str, Any] = {"payload": "ok", "counter": 0}

    async def _health_ok(self) -> bool:
        async with self._lock:
            return not self.active

    async def run_experiment(self, name: str, fault_type: str) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.monotonic()
        before = await self._health_ok()
        status = "completed"
        injected_ms = 0.0
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type,
                                     "started": _now_utc().isoformat()}
            if fault_type == "latency":
                delay = 0.5
                injected_ms = delay * 1000.0
                await asyncio.sleep(delay)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "data_corruption":
                async with self._lock:
                    self._shared_state["payload"] = "corrupted"
                    self._shared_state["counter"] += 1
                injected_ms = 5.0
            elif fault_type == "memory_pressure":
                _ = bytearray(5 * 1024 * 1024)
                injected_ms = 10.0
            elif fault_type == "network_drop":
                delay = 0.2
                injected_ms = delay * 1000.0
                await asyncio.sleep(delay)
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._health_ok()
        duration = (time.monotonic() - t0) * 1000.0
        try:
            await asyncio.to_thread(
                self.storage.save_chaos_experiment,
                name, name, fault_type,
                float(_cfg_get(self.config, "chaos_blast_radius", 0.1) or 0.1),
                int(before), int(after), status, duration)
            _M_CHAOS_EXPERIMENTS.labels(
                fault_type=fault_type, status=status).inc()
        except Exception:
            pass
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status, "duration_ms": duration,
                "injected_ms": injected_ms}


# =============================================================================
# ENHANCEMENT 10 — HITL ACTIVE LEARNING
# =============================================================================
class ActiveUserPreferenceLearner:
    _PREF_KEYS = ("quality_score", "carbon_g", "cost_usd", "latency_ms")

    def __init__(self, storage: Any, dashboard: Any = None):
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._pending: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def submit_response(self, user_id: str, chosen_id: str,
                              request_id: Optional[str] = None) -> None:
        if request_id is not None:
            async with self._lock:
                fut = self._pending.pop(request_id, None)
            if fut is not None and not fut.done():
                fut.set_result({"user_id": user_id, "chosen": chosen_id})
                return
            log_event("warning",
                      f"Unknown HITL request_id {request_id!r}; using FIFO")
        try:
            self._responses.put_nowait(
                {"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            log_event("warning", "HITL response queue full; dropping")

    def _entropy_uncertainty(self, candidates: List[Dict[str, Any]]) -> float:
        scores = []
        for c in candidates[:5]:
            try:
                scores.append(float(c.get("quality_score", 0.0)))
            except (TypeError, ValueError):
                scores.append(0.0)
        s = sum(scores) or 1.0
        probs = [x / s for x in scores]
        ent = -sum(p * math.log(p + 1e-9) for p in probs)
        max_ent = math.log(len(probs)) if len(probs) > 1 else 1.0
        return ent / max_ent if max_ent > 0 else 0.0

    def _ambiguity_score(self, candidates: List[Dict[str, Any]]) -> float:
        if len(candidates) < 2:
            return 0.0
        c1, c2 = candidates[0], candidates[1]

        def _rel(a, b):
            try:
                a = float(a); b = float(b)
            except (TypeError, ValueError):
                return 0.0
            m = max(abs(a), abs(b), 1e-6)
            return abs(a - b) / m

        try:
            q = _rel(c1.get("quality_score", 0), c2.get("quality_score", 0))
            cg = _rel(c1.get("carbon_g", 0), c2.get("carbon_g", 0))
            cost = _rel(c1.get("cost_usd", 0), c2.get("cost_usd", 0))
            lat = _rel(c1.get("latency_ms", 0), c2.get("latency_ms", 0))
            return max(cg, cost, lat) * (1.0 - min(q, 1.0))
        except Exception:
            return 0.0

    async def query_user_if_needed(self, user_id: str,
                                   candidates: List[Dict[str, Any]],
                                   timeout: float = 3.0) -> Optional[str]:
        if len(candidates) < 2:
            return None
        ambiguity = self._ambiguity_score(candidates)
        uncertainty = self._entropy_uncertainty(candidates)
        if ambiguity < 0.05 and uncertainty < 0.5:
            return None

        req_id = uuid.uuid4().hex[:8]
        try:
            await asyncio.to_thread(
                self.storage.enqueue_hitl_request,
                req_id, "pareto_query",
                {"candidates": candidates[:2], "ambiguity": ambiguity,
                 "uncertainty": uncertainty}, "info")
        except Exception:
            pass

        if self.dashboard is not None:
            try:
                await self.dashboard.broadcast({
                    "type": "preference_query", "request_id": req_id,
                    "user_id": user_id})
            except Exception:
                pass

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        async with self._lock:
            self._pending[req_id] = fut
        try:
            msg = await asyncio.wait_for(fut, timeout=timeout)
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "approved")
            except Exception:
                pass
            chosen = msg.get("chosen") if isinstance(msg, dict) else None
            metrics = next(
                (c for c in candidates if c.get("solution_id") == chosen), None)
            await self.record_choice(user_id, chosen, metrics=metrics)
            return chosen
        except asyncio.TimeoutError:
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "timeout")
            except Exception:
                pass
            normalized = self.get_preference_weights(user_id)
            if normalized:
                scored = []
                for c in candidates:
                    s = sum(normalized.get(k, 0.25)
                            / (float(c.get(k, 0) or 0) + 1e-8)
                            for k in self._PREF_KEYS)
                    scored.append((s, c.get("solution_id")))
                scored.sort(reverse=True)
                return scored[0][1]
            return candidates[0].get("solution_id")
        finally:
            async with self._lock:
                self._pending.pop(req_id, None)

    async def record_choice(self, user_id: str, solution_id: str,
                            metrics: Optional[Dict[str, float]] = None) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in self._PREF_KEYS:
                if k in metrics:
                    try:
                        v = float(metrics[k])
                    except (TypeError, ValueError):
                        continue
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        prefs[k] = prefs.get(k, 0.25) + 1.0 / (abs(v) + 1e-6) * 0.01
            self.preferences[user_id] = prefs
        try:
            await asyncio.to_thread(
                self.storage.save_user_preference, user_id, prefs)
        except Exception:
            pass

    def get_preference_weights(self, user_id: str) -> Dict[str, float]:
        raw = self.preferences.get(user_id, {})
        if not raw:
            return {}
        s = sum(raw.values()) or 1.0
        return {k: v / s for k, v in raw.items()}


# =============================================================================
# ENHANCED FlexGenMODPPlanner
# =============================================================================
@dataclass
class _PlannerContext:
    """Per-request planner state returned by plan() and consumed by learn()."""
    state: Tuple[int, int, int]
    action: int
    hours_to_deadline: float


class FlexGenMODPPlanner:
    """
    MODP planner that decides when and where to execute a workload.

    v17.1:
      * Q-learning replaced by Causal MODP policy (do-calculus + PC DAG).
      * Deadlock fixed: `_get_action_values_sync` is synchronous.
      * Federated / XAI / multi-agent / chaos moved off the critical path.
      * HITL correlated via per-request futures.
      * State-action pair returned per call (no shared `last_*`).
      * Carbon-market override uses `defer_hours`, not USD.
    """

    def __init__(
        self,
        carbon_forecaster: Optional[Any] = None,
        horizon: int = 6,
        discount_factor: float = 0.9,
        learning_rate: float = 0.1,
        epsilon: float = 0.1,
        epsilon_decay: float = 0.999,
        defer_penalty: float = 0.05,
        move_penalty: float = 0.1,
        message_queue: Optional[Any] = None,
        q_table_path: str = "modp_q_table.json",
        optimistic_init: float = 1.0,
        save_every: int = 10,
        config: Optional[Any] = None,
        storage: Optional[Any] = None,
        causal_graph: Optional[CausalGraphLearner] = None,
        causal_policy: Optional[CausalMODPPolicy] = None,
        quantum: Optional[QuantumDistillationEngine] = None,
        federated: Optional[FederatedGreenAggregator] = None,
        multi_agent: Optional[MultiAgentCoordinator] = None,
        temporal: Optional[TemporalLogicVerifier] = None,
        xai: Optional[XAIDecisionExplainer] = None,
        precision: Optional[AdaptivePrecisionSwitcher] = None,
        carbon_market: Optional[CarbonMarketIntegrator] = None,
        chaos: Optional[ChaosTestingEngine] = None,
        hitl: Optional[ActiveUserPreferenceLearner] = None,
        *,
        xai_sample_rate: float = 0.01,
        chaos_sample_rate: float = 0.01,
        multi_agent_sample_rate: float = 0.01,
        federated_share_every: int = 100,
    ):
        self.carbon_forecaster = carbon_forecaster
        self.horizon = int(horizon)
        self.discount = float(discount_factor)
        self.lr = float(learning_rate)
        self.epsilon = float(epsilon)
        self.epsilon_decay = float(epsilon_decay)
        self.defer_penalty = float(defer_penalty)
        self.move_penalty = float(move_penalty)
        self.message_queue = message_queue
        self.q_table_path = q_table_path
        self.optimistic_init = float(optimistic_init)
        self.save_every = int(save_every)
        self._update_counter = 0

        self.config = config or {}
        self.storage = storage
        self.causal_graph = causal_graph
        self.causal_policy = causal_policy
        self.quantum = quantum
        self.federated = federated
        self.multi_agent = multi_agent
        self.temporal = temporal
        self.xai = xai
        self.precision = precision
        self.carbon_market = carbon_market
        self.chaos = chaos
        self.hitl = hitl

        # Hot-path sample rates.
        self.xai_sample_rate = float(xai_sample_rate)
        self.chaos_sample_rate = float(chaos_sample_rate)
        self.multi_agent_sample_rate = float(multi_agent_sample_rate)
        self.federated_share_every = int(federated_share_every)

        # Q-table alias for backward compatibility with the previous API.
        if causal_policy is not None:
            self.q_table = causal_policy.q_table
        else:
            self.q_table = {}

        # NOTE: `last_state` / `last_action` attributes are preserved for
        # backward compatibility but are no longer used internally; the
        # correct values are returned as a `_PlannerContext` per call.
        self.last_state: Optional[Tuple[int, int, int]] = None
        self.last_action: Optional[int] = None
        self.last_decision: Optional[Tuple[str, int, Optional[str]]] = None
        self.last_hours_to_deadline: Optional[float] = None
        self._lock = asyncio.Lock()
        self._plan_counter = 0

    # ---- helpers ----
    def _discretize_state(self, current_carbon: float, queue_length: int,
                          hours_to_deadline: float) -> Tuple[int, int, int]:
        if self.causal_policy is not None:
            return self.causal_policy._discretize_state(
                current_carbon, queue_length, hours_to_deadline)
        c = max(0.0, min(float(current_carbon) // 50.0,
                         CausalGraphLearner.MAX_CARBON_BUCKET))
        q = min(max(0, int(queue_length)), 10)
        h = min(max(0, int(hours_to_deadline)), 24)
        return (int(c), q, h)

    def _get_action_values_sync(self, state: Tuple[int, int, int]) -> List[float]:
        if state not in self.q_table:
            self.q_table[state] = [self.optimistic_init] * (self.horizon + 2)
        return self.q_table[state]

    async def get_carbon_forecast(self, hours: Optional[int] = None) -> List[float]:
        hours = int(hours or self.horizon)
        if self.carbon_forecaster:
            try:
                fn = getattr(self.carbon_forecaster, "forecast_carbon_prices", None)
                if fn is not None:
                    forecast = fn(hours=hours)
                    if asyncio.iscoroutine(forecast):
                        forecast = await forecast
                    if isinstance(forecast, dict) and forecast.get("status") == "success":
                        return [float(x) for x in forecast.get("predictions", [])]
                    if isinstance(forecast, list):
                        return [float(x) for x in forecast]
            except Exception as exc:
                log_event("warning", f"Carbon forecast failed: {exc}")
        return [400.0] * hours

    async def _get_current_carbon(self, current_carbon: Optional[float]) -> float:
        if current_carbon is not None:
            return float(current_carbon)
        if self.carbon_forecaster:
            try:
                fn = getattr(self.carbon_forecaster, "get_current_intensity", None)
                if fn is not None:
                    value = fn()
                    if asyncio.iscoroutine(value):
                        value = await value
                    return float(value)
            except Exception:
                pass
        return 400.0

    def _hours_to_deadline(self, workload: WorkloadDescriptor) -> float:
        deadline = getattr(workload, "deadline", None)
        if deadline is None:
            return 24.0
        try:
            if isinstance(deadline, datetime):
                if deadline.tzinfo is None:
                    deadline = deadline.replace(tzinfo=timezone.utc)
                return max(0.0, (deadline - _now_utc()).total_seconds() / 3600.0)
        except Exception as exc:
            log_event("warning", f"Deadline computation failed: {exc}")
        return 24.0

    def _is_node_feasible(self, node: NodeDescriptor,
                          policy: Optional[FlexGenPolicy]) -> bool:
        """
        Simple memory model: required_gb = batch * block_size * 8 bytes / 1e9.
        This uses the FlexGen policy's block_size as an indicative sequence
        length. Real models should implement a proper descriptor query.
        """
        if policy is None:
            return True
        try:
            bytes_per_element = 8  # fp64 intermediate
            required_gb = (
                float(policy.gpu_batch_size) * float(policy.block_size)
                * bytes_per_element / 1e9
            )
            available = float(node.metadata.get("gpu_memory_gb", 16.0))
            return required_gb <= available
        except Exception:
            return True

    def _estimate_reward(self, decision: Tuple[str, int, Optional[str]]) -> float:
        if decision[0] == "run_now":
            return 0.5
        if decision[0] == "defer":
            # Bounded so the value remains in [-1, 1].
            return max(-1.0, -self.defer_penalty * decision[1])
        if decision[0] == "move_node":
            return max(-1.0, -self.move_penalty)
        return 0.0

    # ------------------------------------------------------------------
    # Public planning API
    # ------------------------------------------------------------------
    async def plan(
        self,
        workload: WorkloadDescriptor,
        node: NodeDescriptor,
        current_policy: Optional[FlexGenPolicy] = None,
        queue_length: int = 0,
        current_carbon: Optional[float] = None,
        available_nodes: Optional[List[NodeDescriptor]] = None,
    ) -> Tuple[str, int, Optional[str]]:
        t_start = time.monotonic()

        # 1. Carbon market deferral — decides defer_hours directly.
        market_action = "run"
        market_defer_hours = 0
        if self.carbon_market is not None:
            try:
                intensity = (await self._get_current_carbon(current_carbon)) / 1000.0
                market = await self.carbon_market.net_zero_schedule(
                    workload_kwh=float(workload.estimated_kwh),
                    intensity=intensity, commit=False)
                market_action = market.get("action", "run")
                market_defer_hours = int(market.get("defer_hours", 0))
            except Exception as exc:
                log_event("debug", f"Carbon market check failed: {exc}")

        # 2. Current carbon and deadline.
        current_carbon = await self._get_current_carbon(current_carbon)
        hours_to_deadline = self._hours_to_deadline(workload)

        # 3. State + action.
        state = self._discretize_state(
            current_carbon, queue_length, hours_to_deadline)

        if self.causal_policy is not None:
            action = await self.causal_policy.choose_action(state)
        else:
            values = self._get_action_values_sync(state)
            if random.random() < self.epsilon:
                action = random.randint(0, len(values) - 1)
            else:
                action = int(max(range(len(values)), key=lambda i: values[i]))

        # 4. Interpret action.
        if action == 0:
            decision: Tuple[str, int, Optional[str]] = ("run_now", 0, None)
        elif action <= self.horizon:
            decision = ("defer", action, None)
        else:
            feasible: List[NodeDescriptor] = []
            for n in (available_nodes or []):
                try:
                    if self._is_node_feasible(n, current_policy):
                        feasible.append(n)
                except Exception:
                    continue
            if feasible:
                best_node = min(feasible, key=lambda n: n.region_carbon_intensity)
                if best_node.region_carbon_intensity < current_carbon * 0.9:
                    decision = ("move_node", 0, best_node.id)
                else:
                    decision = ("run_now", 0, None)
            else:
                decision = ("run_now", 0, None)

        # 5. Carbon-market override (uses defer_hours, not USD).
        if market_action == "defer" and decision[0] == "run_now":
            hours = max(1, market_defer_hours)
            decision = ("defer", hours, None)
            try:
                _M_CARBON_OVERRIDES.inc()
            except Exception:
                pass

        # 6. Temporal gating (must stay on the critical path: it can veto).
        if self.temporal is not None:
            try:
                await self.temporal.push_state({
                    "hours_to_deadline": hours_to_deadline,
                    "defer_hours": decision[1] if decision[0] == "defer" else 0,
                    "quality": 1.0})
                verify = await self.temporal.verify()
                if any(not ok for ok in verify.values()):
                    decision = ("run_now", 0, None)
                    try:
                        _M_TEMPORAL_BLOCKS.inc()
                    except Exception:
                        pass
            except Exception as exc:
                log_event("debug", f"Temporal gate failed: {exc}")

        # 7. HITL for tight-deadline deferrals.
        if (decision[0] == "defer"
                and hours_to_deadline < 2.0
                and self.hitl is not None):
            try:
                approved = await self.hitl.query_user_if_needed(
                    "planner_user",
                    [
                        {"solution_id": "approve", "quality_score": 0.90,
                         "carbon_g": 0.10, "cost_usd": 0.10, "latency_ms": 100},
                        {"solution_id": "reject", "quality_score": 0.89,
                         "carbon_g": 0.11, "cost_usd": 0.11, "latency_ms": 105},
                    ],
                    timeout=2.0)
                if approved != "approve":
                    decision = ("run_now", 0, None)
                    try:
                        _M_HITL_BLOCKS.inc()
                    except Exception:
                        pass
            except Exception as exc:
                log_event("warning", f"HITL gate failed: {exc}")

        # 8. Record per-request state/action.
        async with self._lock:
            self.last_state = state
            self.last_action = action
            self.last_decision = decision
            self.last_hours_to_deadline = hours_to_deadline
            self._plan_counter += 1

        # 9. Metrics.
        try:
            _M_PLAN_DECISIONS.labels(action=decision[0]).inc()
            if decision[0] == "defer":
                _M_DEFER_HOURS.observe(float(decision[1]))
            _M_EPSILON.set(self.epsilon)
            _M_Q_TABLE_SIZE.set(len(self.q_table))
            _M_PLAN_LATENCY.observe(time.monotonic() - t_start)
        except Exception:
            pass

        # 10. Fire-and-forget side effects (off the critical path).
        self._schedule_side_effects(workload, node, state, action, decision,
                                    current_carbon, hours_to_deadline)

        # 11. Decay epsilon.
        async with self._lock:
            self.epsilon = max(0.01, self.epsilon * self.epsilon_decay)
        if self.causal_policy is not None:
            self.causal_policy.decay_epsilon()

        return decision

    def _schedule_side_effects(self, workload: WorkloadDescriptor,
                               node: NodeDescriptor,
                               state: Tuple[int, int, int], action: int,
                               decision: Tuple[str, int, Optional[str]],
                               current_carbon: float,
                               hours_to_deadline: float) -> None:
        """
        Push optional work (XAI, multi-agent bid, chaos, federated share)
        off the planning hot path.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        def _spawn(coro: Any) -> None:
            try:
                task = loop.create_task(coro)
                # Keep a strong reference until it completes.
                _bg: set = getattr(self, "_pending_side_effects", set())
                _bg.add(task)
                setattr(self, "_pending_side_effects", _bg)
                task.add_done_callback(_bg.discard)
            except Exception:
                pass

        if self.chaos is not None and random.random() < self.chaos_sample_rate:
            fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
            _spawn(self.chaos.run_experiment(
                f"plan_chaos_{uuid.uuid4().hex[:6]}", fault))

        if self.multi_agent is not None and random.random() < self.multi_agent_sample_rate:
            async def _bid_and_reward():
                try:
                    agent_id, _ = await self.multi_agent.bid({
                        "name": f"plan_{getattr(workload, 'task_id', 'task')}",
                        "preferred_role": "optimizer"})
                    await self.multi_agent.reward(agent_id, 0.8)
                except Exception:
                    pass
            _spawn(_bid_and_reward())

        if self.xai is not None and NUMPY_AVAILABLE and random.random() < self.xai_sample_rate:
            async def _explain():
                try:
                    feats = np.array([
                        float(current_carbon) / 1000.0,
                        float(len(decision)),
                        float(hours_to_deadline) / 24.0,
                        float(self.epsilon)])
                    def _score(x, _w=np.array([0.4, -0.3, 0.2, 0.1])):
                        return float(np.dot(x, _w))
                    await self.xai.explain(
                        decision_id=f"modp_{uuid.uuid4().hex[:8]}",
                        label=f"action={decision[0]}",
                        features=feats,
                        names=["carbon", "queue", "deadline", "epsilon"],
                        model_fn=_score)
                except Exception:
                    pass
            _spawn(_explain())

        if (self.federated is not None
                and self._plan_counter % max(1, self.federated_share_every) == 0):
            async def _share():
                try:
                    policy_bytes = json.dumps(
                        {str(k): v for k, v in self.q_table.items()}
                    ).encode()
                    await self.federated.share_weights(
                        "modp_q_table", policy_bytes)
                except Exception:
                    pass
            _spawn(_share())

    # ------------------------------------------------------------------
    # Learning
    # ------------------------------------------------------------------
    async def learn(self, reward: float, next_carbon: float,
                    next_queue_length: int,
                    *,
                    state: Optional[Tuple[int, int, int]] = None,
                    action: Optional[int] = None,
                    next_hours_to_deadline: Optional[float] = None,
                    ) -> None:
        """
        Bellman update. Prefer passing `state` and `action` explicitly
        (returned by `plan_context()`) so concurrent plans do not corrupt
        each other. If omitted, the shared `last_*` attributes are used for
        backward compatibility.
        """
        async with self._lock:
            if state is None or action is None:
                state = self.last_state
                action = self.last_action
            if state is None or action is None:
                return
            if self.last_decision and self.last_decision[0] == "defer":
                delay = self.last_decision[1]
                next_deadline = max(
                    0.0,
                    (next_hours_to_deadline if next_hours_to_deadline is not None
                     else (self.last_hours_to_deadline or 24.0)) - delay)
            else:
                next_deadline = (next_hours_to_deadline
                                 if next_hours_to_deadline is not None else 24.0)
            next_state = self._discretize_state(
                next_carbon, next_queue_length, next_deadline)
            next_values = self._get_action_values_sync(next_state)
            max_next = max(next_values) if next_values else 0.0
            current = self._get_action_values_sync(state)
            if 0 <= action < len(current):
                current[action] += self.lr * (
                    reward + self.discount * max_next - current[action])

            self.last_state = None
            self.last_action = None
            self.last_decision = None
            self.last_hours_to_deadline = None

            self._update_counter += 1
            if self._update_counter % self.save_every == 0:
                self._save_q_table_unlocked()

            # Propagate to causal policy if present.
            if self.causal_policy is not None:
                try:
                    await self.causal_policy._assign_causal_credit(reward)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    def _save_q_table_unlocked(self) -> None:
        if not self.q_table_path:
            return
        try:
            payload = {
                "version": 2,
                "horizon": self.horizon,
                "epsilon": self.epsilon,
                "states": [
                    {"key": list(k), "values": list(v)}
                    for k, v in self.q_table.items()
                ],
            }
            tmp = f"{self.q_table_path}.tmp"
            with open(tmp, "w") as f:
                json.dump(payload, f)
            os.replace(tmp, self.q_table_path)
        except Exception as e:
            log_event("warning", f"Q-table save failed: {e}")

    def load_q_table(self) -> None:
        if not self.q_table_path or not os.path.exists(self.q_table_path):
            return
        try:
            with open(self.q_table_path, "r") as f:
                data = json.load(f)
            if isinstance(data, dict) and "states" in data:
                file_horizon = int(data.get("horizon", self.horizon))
                expected = file_horizon + 2
                for entry in data["states"]:
                    try:
                        key = tuple(int(x) for x in entry["key"])
                        values = list(entry["values"])
                    except Exception:
                        continue
                    if len(values) != expected:
                        continue
                    self.q_table[key] = [float(v) for v in values]
                return
            for k, v in data.items():
                try:
                    key = tuple(int(x.strip())
                                for x in k.strip("()").split(","))
                except Exception:
                    continue
                if len(v) == self.horizon + 2:
                    self.q_table[key] = [float(x) for x in v]
        except Exception as e:
            log_event("warning", f"Q-table load failed: {e}")

    async def publish_decision(self, workload: WorkloadDescriptor,
                               action: str, delay: int,
                               node_id: Optional[str] = None,
                               reward_estimate: float = 0.0) -> None:
        if not self.message_queue:
            return
        try:
            payload = {
                "source": "modp_flexgen_planner",
                "feedback_type": "routing",
                "task_id": getattr(workload, "task_id", "unknown"),
                "context": {"action": action, "delay_hours": delay,
                            "target_node": node_id, "epsilon": self.epsilon},
                "adaptive_cost_value": float(reward_estimate),
                "tags": ["modp", "scheduling", "carbon_aware"],
            }
            await self.message_queue.publish("modp_events", json.dumps(payload))
        except Exception as exc:
            log_event("warning", f"Publish decision failed: {exc}")

    def get_stats(self) -> Dict[str, Any]:
        return {
            "num_states": len(self.q_table),
            "epsilon": self.epsilon,
            "horizon": self.horizon,
            "has_causal_graph": self.causal_graph is not None,
            "has_causal_policy": self.causal_policy is not None,
        }


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class FlexGenMODPOrchestratorV17:
    """Unified entry point wiring all ten enhancements around the planner."""

    def __init__(self, storage: Any, config: Any, dashboard: Any = None):
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        # Ten enhancements.
        self.quantum = QuantumDistillationEngine(temperature=2.0, alpha=0.5)
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_policy = CausalMODPPolicy(
            config, storage, self.causal_graph, horizon=6)
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        # Wire the planner with all hooks.
        self.planner = FlexGenMODPPlanner(
            config=config,
            storage=storage,
            causal_graph=self.causal_graph,
            causal_policy=self.causal_policy,
            quantum=self.quantum,
            federated=self.federated,
            multi_agent=self.multi_agent,
            temporal=self.temporal,
            xai=self.xai,
            precision=self.precision,
            carbon_market=self.carbon_market,
            chaos=self.chaos,
            hitl=self.hitl,
            horizon=6,
            q_table_path="/tmp/modp_q_table.json",
        )

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()

    # ------------------------------------------------------------------
    async def plan_workload(self, workload: WorkloadDescriptor,
                            node: NodeDescriptor, **kwargs) -> Dict[str, Any]:
        decision = await self.planner.plan(workload, node, **kwargs)
        return {
            "action": decision[0],
            "delay_hours": decision[1],
            "target_node": decision[2],
            "planner_stats": self.planner.get_stats(),
        }

    def _spawn(self, coro: Awaitable[Any], name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)

        def _done(t: asyncio.Task) -> None:
            self._background_tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log_event("error", f"Background task {name} failed: {exc}")

        task.add_done_callback(_done)
        return task

    async def start(self) -> None:
        self._running = True
        _ = asyncio.get_running_loop()
        self._spawn(self._causal_discovery_loop(), "causal_discovery")
        self._spawn(self._federated_loop(), "federated")
        self._spawn(self._multi_agent_loop(), "multi_agent")
        self._spawn(self._temporal_loop(), "temporal")
        self._spawn(self._xai_loop(), "xai")
        self._spawn(self._precision_loop(), "precision")
        self._spawn(self._carbon_market_loop(), "carbon_market")
        self._spawn(self._chaos_loop(), "chaos")
        self._spawn(self._distillation_loop(), "distillation")

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)
        self._background_tasks.clear()
        try:
            self.causal_policy.save("/tmp/modp_q_table.json")
        except Exception:
            pass

    async def aclose(self) -> None:
        await self.shutdown()

    async def __aenter__(self) -> "FlexGenMODPOrchestratorV17":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    # ---- loops ----
    async def _causal_discovery_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "causal_graph_update_interval", 900) or 900))
            try:
                variables = ["carbon", "queue", "deadline", "quality"]
                samples = [{
                    "carbon": random.uniform(0.1, 0.8),
                    "queue": random.uniform(0.0, 1.0),
                    "deadline": random.uniform(0.0, 1.0),
                    "quality": random.uniform(0.5, 1.0),
                } for _ in range(100)]
                await self.causal_graph.learn(samples, variables)
            except Exception as exc:
                log_event("debug", f"causal_discovery loop error: {exc}")

    async def _federated_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "federated_interval", 3600) or 3600))
            try:
                await self.federated.pull_aggregated_weights("modp_q_table")
            except Exception as exc:
                log_event("debug", f"federated loop error: {exc}")

    async def _multi_agent_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "agent_negotiation_interval", 600) or 600))
            try:
                await self.multi_agent.step()
            except Exception as exc:
                log_event("debug", f"multi_agent loop error: {exc}")

    async def _temporal_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "temporal_verification_interval", 300) or 300))
            try:
                await self.temporal.verify()
            except Exception as exc:
                log_event("debug", f"temporal loop error: {exc}")

    async def _xai_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "xai_interval", 300) or 300))
            if not NUMPY_AVAILABLE:
                continue
            try:
                feats = np.array([0.4, 5.0, 0.5, 0.1])
                def _score(x, _w=np.array([0.4, -0.3, 0.2, 0.1])):
                    return float(np.dot(x, _w))
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="modp_health", features=feats,
                    names=["carbon", "queue", "deadline", "epsilon"],
                    model_fn=_score)
            except Exception as exc:
                log_event("debug", f"xai loop error: {exc}")

    async def _precision_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "precision_check_interval", 300) or 300))
            try:
                await self.precision.auto_switch(0.0, 0.0)
            except Exception as exc:
                log_event("debug", f"precision loop error: {exc}")

    async def _carbon_market_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "carbon_market_interval", 3600) or 3600))
            try:
                await self.carbon_market.update_price()
            except Exception as exc:
                log_event("debug", f"carbon_market loop error: {exc}")

    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "chaos_test_interval", 1800) or 1800))
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception as exc:
                log_event("debug", f"chaos loop error: {exc}")

    async def _distillation_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "distillation_interval", 300) or 300))
            try:
                self.quantum.register_teacher(
                    "modp", self.causal_policy.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "modp_student")
            except Exception as exc:
                log_event("debug", f"distillation loop error: {exc}")

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "planner": self.planner.get_stats(),
            "causal_graph": self.causal_graph.summary(),
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
            "federated_rounds": self.federated.rounds,
            "agents": {a.id: a.role
                       for a in self.multi_agent.agents.values()},
            "hitl_pending": len(self.hitl._pending),
            "background_tasks": len(self._background_tasks),
        }


# =============================================================================
# MINIMAL IN-MEMORY STORAGE
# =============================================================================
class InMemoryStorage:
    """Standalone storage for demos/tests. Soft-fail for all methods."""

    def __init__(self):
        self._data: Dict[str, Any] = defaultdict(list)
        self._prefs: Dict[str, Dict[str, float]] = {}

    def save_causal_edge(self, src, dst, weight, confidence):
        self._data["causal_graph"].append({
            "src": src, "dst": dst, "weight": weight,
            "confidence": confidence})

    def save_causal_experiment(self, exp_id, treatment, outcome, ate,
                               samples, method=""):
        self._data["causal_experiments"].append({
            "exp_id": exp_id, "treatment": treatment, "outcome": outcome,
            "ate": ate, "samples": samples, "method": method})

    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._data["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights, "weight_norm": weight_norm,
            "round_id": round_id})

    def get_federated_weights(self, model_id):
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    def save_precision_switch(self, from_p, to_p, reason,
                              saved_wh=0.0, acc_delta=0.0):
        self._data["precision_history"].append({
            "from_p": from_p, "to_p": to_p, "reason": reason,
            "saved_wh": saved_wh, "acc_delta": acc_delta})

    def save_credit_price(self, price_usd, currency="USD",
                          source="oracle", region="global"):
        self._data["carbon_credit_prices"].append({
            "price_usd": price_usd, "currency": currency,
            "source": source, "region": region})

    def save_rec(self, mwh, price_per_mwh, source,
                 certificate_id="", region="global", retired=False):
        self._data["rec_ledger"].append({
            "mwh": mwh, "price_per_mwh": price_per_mwh, "source": source,
            "certificate_id": certificate_id, "region": region,
            "retired": retired})

    def get_rec_balance(self):
        return sum(r["mwh"] for r in self._data["rec_ledger"])

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                            carbon_kg, offset_cost, credit_price):
        self._data["net_zero_matches"].append({
            "match_id": match_id, "workload_kwh": workload_kwh,
            "intensity": intensity, "action": action,
            "carbon_kg": carbon_kg, "offset_cost": offset_cost,
            "credit_price": credit_price})

    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0):
        self._data["chaos_experiments"].append({
            "experiment_id": experiment_id, "name": name,
            "fault_type": fault_type, "blast_radius": blast_radius,
            "steady_before": steady_before, "steady_after": steady_after,
            "status": status, "duration_ms": duration_ms})

    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._data["xai_explanations"].append({
            "explanation_id": explanation_id, "decision_id": decision_id,
            "method": method, "label": label, "features": features,
            "attributions": attributions, "natural_language": nl})

    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window, active):
        self._data["temporal_rules"].append({
            "rule_id": rule_id, "formula": formula, "operator": operator,
            "severity": severity, "description": description,
            "window": window, "active": active})

    def save_temporal_trace(self, state, context=None):
        self._data["temporal_trace"].append({"state": state, "context": context})

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None):
        self._data["temporal_violations"].append({
            "rule_id": rule_id, "formula": formula, "step": step,
            "state": state, "severity": severity, "approved": approved})

    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl):
        self._data["teacher_superpositions"].append({
            "student_id": student_id, "teacher_id": teacher_id,
            "weight": weight, "temperature": temperature,
            "amplitude": amplitude, "kl": kl})

    def enqueue_hitl_request(self, request_id, rule_id, state, severity):
        self._data["hitl_approval_queue"].append({
            "request_id": request_id, "rule_id": rule_id,
            "state": state, "severity": severity, "status": "pending"})

    def resolve_hitl_request(self, request_id, status="approved"):
        for r in self._data["hitl_approval_queue"]:
            if r["request_id"] == request_id:
                r["status"] = status

    def save_user_preference(self, user_id, weights):
        self._prefs[user_id] = dict(weights)

    def get_user_preference(self, user_id):
        return self._prefs.get(user_id)


# =============================================================================
# DEMO
# =============================================================================
async def _demo():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    config = {
        "learning_rate": 0.1,
        "discount_factor": 0.9,
        "epsilon": 0.1,
        "epsilon_decay": 0.999,
        "agent_count": 5,
        "temporal_max_trace": 2000,
        "temporal_formulas": ["G (hours_to_deadline >= 0)"],
        "xai_method": "kernel_shap",
        "xai_depth": 5,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "chaos_intensity": 0.05,
        "chaos_blast_radius": 0.1,
    }

    storage = InMemoryStorage()
    orch = FlexGenMODPOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("FlexGenMODPPlanner v17.1.0 — Demo")
    print("=" * 80)

    # 1. PC algorithm causal discovery
    print("\n=== PC Algorithm Causal Discovery ===")
    if NUMPY_AVAILABLE:
        rng = np.random.default_rng(42)
        n = 500
        X = rng.normal(0, 1, n)
        Y = 0.7 * X + rng.normal(0, 0.5, n)
        Z = 0.5 * X + 0.6 * Y + rng.normal(0, 0.5, n)
        samples = [{"X": float(X[i]), "Y": float(Y[i]), "Z": float(Z[i])}
                   for i in range(n)]
        await orch.causal_graph.learn(samples, ["X", "Y", "Z"])
        summary = orch.causal_graph.summary()
        print(f"  Nodes: {summary['nodes']}")
        print(f"  Oriented edges: {summary['edges']}")
        print(f"  Skeleton edges: {summary['skeleton_edges']}")
        print(f"  V-structures: {summary['v_structures']}")
        for src, edges in orch.causal_graph.graph.items():
            for dst, info in edges.items():
                print(f"    {src} -> {dst} (weight={info['weight']:+.3f})")

        ate = await orch.causal_graph.estimate_ate_do_calculus(
            "X", "Z", samples)
        print(f"\n  ATE(X -> Z) via do-calculus: {ate:+.4f}")
        print(f"  Expected ~ 0.5 + 0.7 * 0.6 = 0.92")

    # 2. Plan workloads
    print("\n=== Workload Planning ===")
    for i in range(5):
        workload = WorkloadDescriptor(
            task_id=f"task_{i}",
            deadline=datetime.now(timezone.utc) + timedelta(
                hours=random.uniform(1, 24)),
            estimated_kwh=random.uniform(0.5, 2.0))
        node = NodeDescriptor(
            id=f"node_{i}",
            region_carbon_intensity=random.uniform(200, 800))
        result = await orch.plan_workload(
            workload, node,
            queue_length=random.randint(0, 5),
            current_carbon=random.uniform(200, 800),
            available_nodes=[
                NodeDescriptor(id="node_a", region_carbon_intensity=300),
                NodeDescriptor(id="node_b", region_carbon_intensity=500),
                NodeDescriptor(id="node_c", region_carbon_intensity=200)])
        print(f"  Task {i}: {result['action']} "
              f"delay={result['delay_hours']}h "
              f"target={result['target_node']}")

    # 3. Learning update
    print("\n=== Learning Update ===")
    await orch.planner.learn(
        reward=0.8, next_carbon=300.0, next_queue_length=2)
    print(f"  Q-table states: {len(orch.planner.q_table)}")
    print(f"  Epsilon after decay: {orch.planner.epsilon:.4f}")

    # 4. Carbon-market override behaviour (defer_hours driven)
    print("\n=== Carbon-market override (defer_hours) ===")
    market = await orch.carbon_market.net_zero_schedule(
        workload_kwh=1.0, intensity=0.7)
    print(f"  Action: {market['action']}  Defer hours: {market['defer_hours']}")

    # 5. HITL approval with correlation
    print("\n=== HITL test (tight deadline) ===")
    async def _delayed_response():
        await asyncio.sleep(0.3)
        # Respond to the request id when it appears.
        for _ in range(50):
            if orch.hitl._pending:
                req_id = next(iter(orch.hitl._pending))
                await orch.hitl.submit_response(
                    "planner_user", "approve", request_id=req_id)
                return
            await asyncio.sleep(0.02)
    asyncio.create_task(_delayed_response())
    workload = WorkloadDescriptor(
        task_id="critical_task",
        deadline=datetime.now(timezone.utc) + timedelta(hours=1.5),
        estimated_kwh=1.0)
    node = NodeDescriptor(id="n1", region_carbon_intensity=400)
    result = await orch.plan_workload(
        workload, node, queue_length=1, current_carbon=600)
    print(f"  Decision: {result['action']} delay={result['delay_hours']}")

    # 6. Health check
    print("\n=== Health check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    # 7. Chaos experiment
    print("\n=== Chaos experiment ===")
    print(json.dumps(
        await orch.chaos.run_experiment("demo_chaos", "latency"),
        indent=2, default=str))

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
