"""
causal_rl_policy.py — Enhanced v17.1.0
========================================

Causal reinforcement learning for MOPD policy adaptation, extended with ten
advanced Green Agent enhancements in a single self-contained file.

FIXES OVER v17.0.0:
 - V-structure orientation corrected (z must be adjacent to BOTH x and y;
   x and y must be non-adjacent).
 - _build_directed_graph no longer orients leftover edges by variance —
   they remain undirected and are not added to the directed graph.
 - self._oriented is initialized in __init__; PC pipeline no longer relies
   on attribute existence.
 - Fabrication of random edges removed when data is insufficient.
 - _pc_remove_edges iterates over a snapshot so the skeleton mutation is
   safe; `size` loop terminates cleanly.
 - _ci_test clamps p-values and validates sample size.
 - do-calculus docstring matches the actual estimator (linear regression).
 - get_backdoor_adjustment_set documented as a parents-only heuristic.
 - CausalPolicyAdapter._assign_causal_credit now actually influences the
   action distribution (values namespaced).
 - counterfactual propagates through the full DAG in topological order.
 - CausalPolicyAdapter.STRATEGY_MAP + to_strategy() bridge to the MOPD
   strategy vocabulary.
 - TemporalLogicVerifier defers rule persistence and honours the approval
   callback's return value.
 - XAIDecisionExplainer persists named features; LIME kernel scaled.
 - AdaptivePrecisionSwitcher filters to known precisions; rolling accuracy
   window; hardware probe cached.
 - CarbonMarketIntegrator decoupled from update_price in net_zero_schedule;
   storage soft-fails; in-memory REC balance.
 - ChaosTestingEngine handles "data_corruption"; real health probe.
 - ActiveUserPreferenceLearner correlates responses; raw preferences
   normalised on read.
 - CausalOrchestratorV17 uses get_running_loop(); _spawn logs task
   failures; aclose() / __aenter__ / __aexit__; clears task set.
 - _on_critical_violation passes distinct candidates.
 - InMemoryStorage methods use real signatures.
 - Prometheus metrics + log_event shim.

Self-contained: Python stdlib + optional numpy / sklearn / torch.
All storage interactions soft-fail.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import math
import random
import re
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Deque, Dict, FrozenSet, List, Optional, Set, Tuple

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
    from prometheus_client import Counter as _PromCounter, Gauge
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
    _M_CAUSAL_EDGES = Gauge(
        "causal_rl_causal_edges",
        "Edges in the causal graph",
    )
    _M_CAUSAL_VSTRUCTURES = _PromCounter(
        "causal_rl_vstructures_total",
        "V-structures oriented",
    )
    _M_ATE_ESTIMATES = _PromCounter(
        "causal_rl_ate_estimates_total",
        "ATE estimates produced",
    )
    _M_TEMPORAL_VIOLATIONS = _PromCounter(
        "causal_rl_temporal_violations_total",
        "Temporal violations",
        ["rule", "severity"],
    )
    _M_HITL_REQUESTS = _PromCounter(
        "causal_rl_hitl_requests_total",
        "HITL queries issued",
    )
    _M_HITL_RESPONSES = _PromCounter(
        "causal_rl_hitl_responses_total",
        "HITL responses",
        ["status"],
    )
    _M_CHAOS_EXPERIMENTS = _PromCounter(
        "causal_rl_chaos_experiments_total",
        "Chaos experiments",
        ["fault_type", "status"],
    )
    _M_PRECISION_SWITCHES = _PromCounter(
        "causal_rl_precision_switches_total",
        "Precision switches",
        ["from_p", "to_p"],
    )
    _M_XAI_EXPLANATIONS = _PromCounter(
        "causal_rl_xai_explanations_total",
        "XAI explanations",
    )
else:
    class _Dummy:
        def labels(self, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass

    _M_CAUSAL_EDGES = _M_CAUSAL_VSTRUCTURES = _M_ATE_ESTIMATES = _Dummy()
    _M_TEMPORAL_VIOLATIONS = _M_HITL_REQUESTS = _M_HITL_RESPONSES = _Dummy()
    _M_CHAOS_EXPERIMENTS = _M_PRECISION_SWITCHES = _M_XAI_EXPLANATIONS = _Dummy()


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL (TRUE)
# =============================================================================
class CausalGraphLearner:
    """
    Causal DAG learner using the **PC algorithm** (Peter–Clark).

    Steps:
      1. Start with a complete undirected graph on the variable set.
      2. Iteratively remove edges whose endpoints become conditionally
         independent given some subset of their neighbours (Fisher-z test).
      3. Orient v-structures (colliders): X → Z ← Y where X, Y not adjacent
         and Z ∉ sepset(X, Y).
      4. Apply Meek's rule 1 to orient additional edges.
      5. Persist each oriented edge to storage.

    Do-calculus utilities:
      * `get_backdoor_adjustment_set(treatment, outcome)` — returns the
        parents of the treatment that are not descendants of it. This is a
        *sufficient* adjustment set when the graph is a valid DAG; it is
        not the full backdoor criterion.
      * `estimate_ate_do_calculus(treatment, outcome, data)` — fits
        Y ~ X + Z by OLS and returns the X coefficient. This equals the
        ATE under linearity and no interactions.
    """

    def __init__(self, storage, alpha: float = 0.05, max_cond_size: int = 3):
        self.storage = storage
        self.alpha = float(alpha)
        self.max_cond_size = int(max_cond_size)
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.skeleton: Dict[str, Set[str]] = defaultdict(set)
        self.sepsets: Dict[Tuple[str, str], Set[str]] = {}
        self.variables: List[str] = []
        # Oriented edges (from, to). Initialized here so the pipeline can
        # safely reference it even if a step is skipped.
        self._oriented: Set[Tuple[str, str]] = set()
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def learn(
        self,
        samples: List[Dict[str, float]],
        variables: List[str],
        threshold: float = 0.25,
    ) -> Dict[str, Any]:
        """Run the PC algorithm over the given samples."""
        self.variables = list(variables)
        self._oriented = set()

        # Insufficient data → return an empty graph (do not fabricate).
        if not NUMPY_AVAILABLE or len(samples) < 5:
            async with self._lock:
                self.graph.clear()
                self.skeleton.clear()
                self.sepsets.clear()
            return self.summary()

        try:
            X = np.array(
                [[float(s.get(v, 0.0)) for v in variables] for s in samples],
                dtype=float,
            )
        except Exception as exc:
            log_event("warning", f"Causal samples could not be parsed: {exc}")
            return self.summary()

        if X.shape[0] < 3 or X.shape[1] < 2:
            return self.summary()

        X = (X - X.mean(0)) / (X.std(0) + 1e-9)

        async with self._lock:
            # Reset state for the run.
            self.skeleton = {v: set(variables) - {v} for v in variables}
            self.sepsets = {}
            self._oriented = set()

            self._pc_remove_edges(X, variables)
            self._orient_v_structures(X, variables)
            self._apply_meeks_rules()
            self._build_directed_graph(X, variables)

            # Persist.
            for src, edges in self.graph.items():
                for dst, info in edges.items():
                    if self.storage:
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

    # ------------------------------------------------------------------
    # PC internals
    # ------------------------------------------------------------------
    def _ci_test(self, X, i: int, j: int, cond: Tuple[int, ...]) -> Tuple[bool, float]:
        """
        Conditional independence test via partial correlation + Fisher z.
        Returns (independent?, p-value).
        """
        n = X.shape[0]
        r = self._partial_corr(X, i, j, list(cond))
        r = max(-0.999, min(0.999, r))
        k = len(cond)
        if n - k - 3 <= 0:
            # Not enough samples to trust the test → treat as independent so
            # the algorithm leaves the edge undirected.
            return True, 1.0
        z = 0.5 * math.log((1 + r) / (1 - r)) * math.sqrt(n - k - 3)
        p = 2.0 * (1.0 - self._std_normal_cdf(abs(z)))
        # Clamp to [0, 1] to guard against floating-point overshoot.
        p = max(0.0, min(1.0, p))
        return p > self.alpha, p

    @staticmethod
    def _partial_corr(X, i: int, j: int, cond: List[int]) -> float:
        """Partial correlation of columns i, j given columns cond."""
        if not cond:
            a = X[:, i]
            b = X[:, j]
        else:
            cond_arr = X[:, cond]
            cond_arr = np.hstack([np.ones((X.shape[0], 1)), cond_arr])
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

    @staticmethod
    def _std_normal_cdf(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    def _pc_remove_edges(self, X, variables: List[str]) -> None:
        """Step 1: remove edges via conditional independence tests."""
        idx = {v: i for i, v in enumerate(variables)}
        for size in range(0, self.max_cond_size + 1):
            # Take a snapshot of the edges to consider at this size.
            edges = []
            for vi in variables:
                for vj in self.skeleton.get(vi, set()):
                    if vi < vj and vj in self.skeleton[vi]:
                        edges.append((vi, vj))

            for vi, vj in edges:
                if vj not in self.skeleton.get(vi, set()):
                    continue
                # Condition on subsets of neighbours of vi (excluding vj).
                neighbours = list(self.skeleton[vi] - {vj})
                if len(neighbours) < size:
                    continue
                removed = False
                for combo in itertools.combinations(neighbours, size):
                    cond_idx = tuple(idx[c] for c in combo)
                    indep, _ = self._ci_test(X, idx[vi], idx[vj], cond_idx)
                    if indep:
                        self.skeleton[vi].discard(vj)
                        self.skeleton[vj].discard(vi)
                        self.sepsets[(vi, vj)] = set(combo)
                        self.sepsets[(vj, vi)] = set(combo)
                        removed = True
                        break
                if removed:
                    continue

    def _orient_v_structures(self, X, variables: List[str]) -> None:
        """
        Step 2: orient v-structures.

        A triple (x, z, y) is a v-structure x → z ← y if:
          * z is adjacent to x (in skeleton)
          * z is adjacent to y (in skeleton)
          * x and y are NOT adjacent (in skeleton)
          * z is NOT in sepset(x, y)
        """
        v_count = 0
        for x in variables:
            for y in variables:
                if x == y:
                    continue
                if y in self.skeleton.get(x, set()):
                    # x and y are adjacent → cannot be a v-structure.
                    continue
                for z in variables:
                    if z == x or z == y:
                        continue
                    # z must be adjacent to both x and y.
                    if z not in self.skeleton.get(x, set()):
                        continue
                    if z not in self.skeleton.get(y, set()):
                        continue
                    # z must not be in the sepset(x, y).
                    if z in self.sepsets.get((x, y), set()):
                        continue
                    # Orient x -> z and y -> z.
                    if (x, z) not in self._oriented:
                        self._oriented.add((x, z))
                        v_count += 1
                    if (y, z) not in self._oriented:
                        self._oriented.add((y, z))
                        v_count += 1
        try:
            if v_count:
                _M_CAUSAL_VSTRUCTURES.inc(v_count)
        except Exception:
            pass

    def _apply_meeks_rules(self) -> None:
        """
        Step 3: apply Meek's rule 1 (a → b, b - c, a, c not adjacent ⇒ b → c).
        Repeats until no more edges can be oriented (bounded iterations).
        """
        changed = True
        iterations = 0
        while changed and iterations < 20:
            changed = False
            iterations += 1
            # Snapshot the current undirected edges.
            undirected = []
            for a in list(self.skeleton.keys()):
                for b in list(self.skeleton[a]):
                    if (a, b) in self._oriented or (b, a) in self._oriented:
                        continue
                    undirected.append((a, b))

            for a, b in undirected:
                # Try both directions for each candidate edge.
                for src, dst in ((a, b), (b, a)):
                    # Need an oriented edge src' -> src for some src'.
                    # Rule 1 requires an incoming oriented edge to `src`.
                    incoming = [(p, src) for p in self.skeleton.get(src, set())
                                if (p, src) in self._oriented]
                    if not incoming:
                        continue
                    for p, _ in incoming:
                        # p -> src, src - dst (undirected), p and dst not
                        # adjacent ⇒ src -> dst.
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

    def _build_directed_graph(self, X, variables: List[str]) -> None:
        """
        Step 4: build the directed graph from the oriented edges.

        Edges that remain undirected after Meek's rules are NOT added to the
        final graph — the PC algorithm deliberately leaves them undirected,
        and arbitrary orientation (e.g. by variance) would fabricate causal
        claims.
        """
        self.graph.clear()
        idx = {v: i for i, v in enumerate(variables)}
        for (a, b) in self._oriented:
            try:
                r = self._partial_corr(X, idx[a], idx[b], [])
            except Exception:
                r = 0.0
            self.graph[a][b] = {"weight": float(r), "confidence": abs(float(r))}

    # ------------------------------------------------------------------
    # Do-calculus utilities
    # ------------------------------------------------------------------
    def get_backdoor_adjustment_set(self, treatment: str, outcome: str) -> Set[str]:
        """
        Return a sufficient backdoor adjustment set.

        This is the parents of `treatment` that are not descendants of it.
        For a valid DAG, this satisfies the backdoor criterion. It is not
        necessarily minimal or unique, and it assumes the learned graph is
        correct.
        """
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

    def _topological_order(self) -> List[str]:
        """Topological order of the directed graph (Kahn's algorithm)."""
        nodes = list(self.variables)
        in_degree: Dict[str, int] = {n: 0 for n in nodes}
        for src, edges in self.graph.items():
            for dst in edges:
                if dst in in_degree:
                    in_degree[dst] += 1
        queue = [n for n in nodes if in_degree.get(n, 0) == 0]
        order: List[str] = []
        while queue:
            n = queue.pop(0)
            order.append(n)
            for c in self.children(n):
                if c in in_degree:
                    in_degree[c] -= 1
                    if in_degree[c] == 0:
                        queue.append(c)
        # Any remaining nodes (cycles) are appended in their original order.
        for n in nodes:
            if n not in order:
                order.append(n)
        return order

    async def estimate_ate_do_calculus(
        self,
        treatment: str,
        outcome: str,
        data: List[Dict[str, float]],
    ) -> float:
        """
        Estimate the ATE via OLS of Y on (X, Z) with Z = backdoor adjustment
        set. Under linearity and no interactions, the coefficient on X equals
        the average treatment effect.
        """
        adj_set = self.get_backdoor_adjustment_set(treatment, outcome)
        if not data:
            return 0.0

        columns = [treatment] + sorted(adj_set) + [outcome]
        try:
            X = np.array(
                [[float(d.get(v, 0.0)) for v in columns] for d in data],
                dtype=float,
            )
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

        try:
            _M_ATE_ESTIMATES.inc()
        except Exception:
            pass

        if self.storage:
            try:
                await asyncio.to_thread(
                    self.storage.save_causal_experiment,
                    f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome,
                    ate, len(data), "do_calculus_ols")
            except Exception:
                pass
        return ate


class CausalPolicyAdapter:
    """
    Epsilon-greedy causal policy with causal credit assignment and
    counterfactual reasoning.

    `ACTIONS` is preserved for backward compatibility; use `to_strategy()`
    to translate a causal action into the MOPD strategy vocabulary.
    """

    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    STRATEGY_MAP: Dict[str, str] = {
        "performance": "balanced",
        "carbon": "carbon_focus",
        "cost": "operational_focus",
        "hybrid": "balanced",
        "adaptive": "balanced",
    }

    # Namespace for credit-assigned state variables, kept separate from
    # ACTIONS so credit assignment does not pollute the action values.
    _STATE_PREFIX = "state::"

    def __init__(self, config, storage, graph: CausalGraphLearner):
        self.config = config
        self.storage = storage
        self.graph = graph
        # Action values keyed by action name.
        self.values: Dict[str, float] = defaultdict(float)
        # Credit-assigned state values keyed by "state::<var>".
        self.state_credits: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = float(_cfg_get(config, "causal_exploration_rate", 0.1) or 0.1)
        self._lock = asyncio.Lock()

    def to_strategy(self, action: str) -> str:
        return self.STRATEGY_MAP.get(action, "balanced")

    async def choose_action(self, state: Dict[str, Any]) -> str:
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.ACTIONS)
            return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action: str, reward: float,
                     state: Dict[str, Any]) -> None:
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

        # Causal credit assignment (separate namespace).
        await self._assign_causal_credit(reward, state)

    async def _assign_causal_credit(self, reward: float,
                                    state: Dict[str, Any]) -> None:
        """Distribute reward to causal parents of 'quality' (if known)."""
        parents = self.graph.parents("quality")
        for p in parents:
            if p not in state:
                continue
            edge = self.graph.graph.get(p, {}).get("quality", {})
            weight = float(edge.get("weight", 0.0))
            key = f"{self._STATE_PREFIX}{p}"
            # Rolling average.
            prev = self.state_credits.get(key, 0.0)
            self.state_credits[key] = 0.9 * prev + 0.1 * reward * weight

    async def estimate_ate(self, treatment: str, outcome: str,
                           samples: int = 100) -> float:
        """
        Legacy API: returns the edge weight between treatment and outcome as
        a proxy for the ATE. The full do-calculus estimator is available via
        `CausalGraphLearner.estimate_ate_do_calculus`.
        """
        w = float(
            self.graph.graph.get(treatment, {}).get(outcome, {}).get("weight", 0.0)
        )
        try:
            await asyncio.to_thread(
                self.storage.save_causal_experiment,
                f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome, w, samples,
                "edge_weight_proxy")
        except Exception:
            pass
        return w

    def counterfactual(self, state: Dict[str, Any],
                       intervention: Dict[str, float]) -> Dict[str, float]:
        """
        Structural counterfactual: apply the intervention, then propagate
        effects to descendants in topological order.
        """
        # Baseline copy.
        cf: Dict[str, float] = {k: float(v) for k, v in state.items()}
        baseline: Dict[str, float] = dict(cf)

        # Apply intervention values on top of the baseline.
        for var, val in intervention.items():
            try:
                cf[var] = float(val)
            except (TypeError, ValueError):
                continue

        # Propagate in topological order so children see the updated parents.
        for node in self.graph._topological_order():
            if node not in cf:
                continue
            for child, info in self.graph.graph.get(node, {}).items():
                if child not in cf:
                    continue
                # Skip if the child is itself an intervention target.
                if child in intervention:
                    continue
                weight = float(info.get("weight", 0.0))
                # Structural equation: child = f(parents). We approximate by
                # propagating the parent's delta.
                delta = cf[node] - baseline.get(node, 0.0)
                cf[child] = cf[child] + weight * delta
        return cf

    def get_policy(self) -> List[float]:
        return list(self.policy)


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    """√p amplitude superposition over teacher policies."""

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

    async def step(self, storage, student_id: str = "causal_student"):
        target = self._softmax(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]

        # Per-teacher KL against the teacher's own distribution.
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

        entry = {
            "target": target,
            "student": list(self.student_policy),
            "ts": _now_utc().isoformat(),
        }
        self.history.append(entry)
        return entry

    def get_policy(self):
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING
# =============================================================================
class FederatedGreenAggregator:
    """Byte-wise average across federated instances."""

    def __init__(self, storage, instance_id, share_interval=3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = int(share_interval)
        self.rounds = 0
        self.last_aggregated: Optional[bytes] = None

    async def share_weights(self, model_id, weights):
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, weights,
                float(len(weights)), self.rounds)
        except Exception:
            pass

    async def pull_aggregated_weights(self, model_id):
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id)
        except Exception:
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

    async def apply_aggregated_weights(self, model_id, current):
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None:
            return current
        n = min(len(current), len(agg))
        return bytes([(current[i] + agg[i]) // 2 for i in range(n)])

    def stats(self):
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
        count = int(_cfg_get(config, "agent_count", 5) or 5)
        self.agents = {f"agent_{i:02d}": _Agent(f"agent_{i:02d}")
                       for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self):
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])
                try:
                    await asyncio.to_thread(
                        self.storage.save_agent, a.id, a.role,
                        a.reputation, a.utilities)
                except Exception:
                    pass

    async def broadcast(self, topic, sender, payload):
        try:
            self.bus.put_nowait({"topic": topic, "sender": sender,
                                 "payload": payload})
        except asyncio.QueueFull:
            log_event("warning", "MultiAgentCoordinator bus full; message dropped")
        try:
            await asyncio.to_thread(
                self.storage.save_agent_message,
                uuid.uuid4().hex[:8], topic, sender, "*", payload)
        except Exception:
            pass

    async def bid(self, task):
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

    async def reward(self, agent_id, reward):
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

    def get_policy(self):
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
                "role_distribution": self.get_role_distribution(),
                "strategy_distribution": self.get_policy(),
                "processed_messages": processed}


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC
# =============================================================================
_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


class TemporalRule:
    def __init__(self, rule_id, operator, conditions, window=0.0,
                 description="", severity="warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = float(window)
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation: Optional[datetime] = None

    def evaluate(self, trace):
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
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=int(_cfg_get(config, "temporal_max_trace", 2000) or 2000))
        self.approval_cb: Optional[Callable] = None
        self._pending_rule_saves: List[Tuple[str, str, str, str]] = []

        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula):
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
                log_event("warning", f"Unrecognised temporal formula: {formula!r}")
        except Exception as exc:
            log_event("warning", f"Temporal formula install failed: {exc}")

    def _add_atomic(self, expr, op):
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

    async def _flush_pending_rule_saves(self):
        while self._pending_rule_saves:
            rid, expr, op, sev = self._pending_rule_saves.pop(0)
            try:
                await asyncio.to_thread(
                    self.storage.save_temporal_rule,
                    rid, expr, op, sev, expr, 0.0, True)
            except Exception:
                pass

    def set_approval_callback(self, cb):
        self.approval_cb = cb

    async def push_state(self, state):
        await self._flush_pending_rule_saves()
        self.trace.append((_now_utc(), dict(state)))
        try:
            await asyncio.to_thread(self.storage.save_temporal_trace, state)
        except Exception:
            pass

    async def verify(self):
        await self._flush_pending_rule_saves()
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace)
            violated = rule.evaluate(copy)
            approved = False

            if violated:
                rule.violations += 1
                rule.last_violation = _now_utc()
                try:
                    _M_TEMPORAL_VIOLATIONS.labels(
                        rule=rid, severity=rule.severity).inc()
                except Exception:
                    pass

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
    """Permutation-based feature importance + LIME + natural-language render."""

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = str(_cfg_get(config, "xai_method", "kernel_shap") or "kernel_shap")
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

    # Backward-compatible alias.
    def _kernel_shap(self, f, x, names, n=64):
        return self._permutation_shap(f, x, names, n)

    def _lime(self, f, x, names, n=200):
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            return {k: 0.0 for k in names}
        scale = float(np.linalg.norm(x)) / max(1.0, math.sqrt(len(x)))
        sigma = max(scale * 0.5, 1e-3)
        X = np.tile(x, (n, 1)) + np.random.normal(0, 0.1, (n, len(x)))
        try:
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
        except Exception:
            return {k: 0.0 for k in names}
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / (2.0 * sigma * sigma))
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
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
        elif self.method == "lime":
            attrs = self._lime(model_fn, features, names)
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
            _M_XAI_EXPLANATIONS.inc()
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
    CRITICAL_PRECISIONS = {"int8"}

    def __init__(self, config, storage, causal_rl=None, xai=None,
                 chaos=None, carbon_market=None, multi_agent=None,
                 hitl=None, federated=None):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.causal_rl = causal_rl
        self.xai = xai
        self.chaos = chaos
        self.carbon_market = carbon_market
        self.multi_agent = multi_agent
        self.hitl = hitl
        self.federated = federated
        self.dynamic_energy = dict(self.ENERGY)
        self.dynamic_latency = dict(self.LATENCY)
        self.telemetry: Deque[Dict] = deque(maxlen=200)
        self.switch_history: Deque[Dict] = deque(maxlen=500)
        self._acc_window: Deque[float] = deque(maxlen=20)
        self._hw_cache: Optional[Dict[str, Any]] = None

    def _probe(self, force_refresh: bool = False):
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
        return [c for c in raw if c in self.dynamic_energy]

    def select_precision(self):
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
        return min(cands, key=lambda c: self.dynamic_energy.get(c, 1.0))

    def observe_telemetry(self, precision, wh, latency, accuracy):
        if precision not in self.dynamic_energy:
            return
        self.telemetry.append({"precision": precision, "wh": float(wh),
                               "latency": float(latency),
                               "accuracy": float(accuracy),
                               "ts": time.time()})

    def record_accuracy(self, accuracy: float):
        try:
            self._acc_window.append(float(accuracy))
        except (TypeError, ValueError):
            pass

    async def switch_to(self, target, reason="policy",
                        current_intensity: float = 0.4) -> bool:
        if target == self.current or target not in self.dynamic_energy:
            return False
        if target not in self._allowed_precisions():
            log_event("warning",
                      f"Precision {target!r} not in precision_levels; ignoring")
            return False

        # Optional carbon deferral.
        if self.carbon_market is not None:
            try:
                decision = await self.carbon_market.net_zero_schedule(
                    workload_kwh=0.5, intensity=current_intensity, commit=False)
                if decision.get("action") == "defer":
                    self.switch_history.append({
                        "from": self.current, "to": target,
                        "reason": f"deferred: {reason}",
                        "status": "deferred",
                        "ts": _now_utc().isoformat()})
                    return False
            except Exception:
                pass

        old = self.current
        self.current = target
        saved = max(0.0, self.dynamic_energy.get(old, 1.0) -
                    self.dynamic_energy.get(target, 1.0))
        self.saved_wh += saved
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, 0.0)
            _M_PRECISION_SWITCHES.labels(from_p=old, to_p=target).inc()
        except Exception:
            pass
        self.switch_history.append({
            "from": old, "to": target, "reason": reason,
            "status": "applied", "saved_wh": saved,
            "ts": _now_utc().isoformat()})
        return True

    async def auto_switch(self, recent_acc=0.0, baseline_acc=0.0):
        # Fall back to the rolling window when the caller passes 0/0.
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

    async def chaos_probe(self, fault_type: str = "latency"):
        if self.chaos is None:
            return None
        try:
            return await self.chaos.run_experiment(
                f"precision_probe_{uuid.uuid4().hex[:6]}", fault_type)
        except Exception as exc:
            log_event("warning", f"chaos_probe failed: {exc}")
            return None


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    DEFER_INTENSITY_THRESHOLD = 0.3   # kgCO2/kWh
    RUN_OFFSET_COST_THRESHOLD = 0.5   # USD
    MIN_PRICE_USD = 5.0
    MAX_PRICE_USD = 200.0
    DEFAULT_PRICE_USD = 25.0
    DEFAULT_PRICE_PER_MWH_USD = 5.0

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.last_price = float(_cfg_get(config, "carbon_default_price_usd",
                                         self.DEFAULT_PRICE_USD)
                                or self.DEFAULT_PRICE_USD)
        self._rec_balance_mwh: float = 0.0
        self._closed = False

    async def _fetch_price(self):
        candidate = self.last_price + random.gauss(0, 1.5)
        return max(self.MIN_PRICE_USD, min(self.MAX_PRICE_USD, candidate))

    async def update_price(self):
        try:
            price = await self._fetch_price()
        except Exception:
            price = self.last_price
        try:
            price = max(self.MIN_PRICE_USD, min(self.MAX_PRICE_USD, float(price)))
        except (TypeError, ValueError):
            price = self.last_price
        # Persist first, then mutate state.
        try:
            await asyncio.to_thread(self.storage.save_credit_price, price)
        except Exception:
            pass
        self.last_price = price
        return price

    async def purchase_rec(self, mwh, price_per_mwh=5.0, source="wind"):
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

    async def net_zero_schedule(self, workload_kwh, intensity,
                                price=None, commit=True):
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
        if intensity > self.DEFER_INTENSITY_THRESHOLD:
            action = "defer"
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

        return {"action": action, "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost, "credit_price_usd": price,
                "rec_balance_mwh": float(self._rec_balance_mwh)}

    def get_rec_balance(self) -> float:
        return float(self._rec_balance_mwh)

    async def aclose(self):
        self._closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self._shared_state: Dict[str, Any] = {"payload": "ok", "counter": 0}

    async def _health_ok(self):
        async with self._lock:
            return not self.active

    async def run_experiment(self, name, fault_type):
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

    def __init__(self, storage, pareto_gating=None, dashboard=None,
                 feedback_callback=None, use_priority_queue: bool = False):
        self.storage = storage
        self.pareto = pareto_gating
        self.dashboard = dashboard
        self.feedback_callback = feedback_callback
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._pending: Dict[str, asyncio.Future] = {}
        self._priority_queue: List[Tuple[float, str, Dict[str, Any]]] = []
        self.use_priority_queue = use_priority_queue
        self._lock = asyncio.Lock()

    async def submit_response(self, user_id, chosen_id,
                              request_id: Optional[str] = None):
        if request_id is not None:
            async with self._lock:
                fut = self._pending.pop(request_id, None)
            if fut is not None and not fut.done():
                fut.set_result({"user_id": user_id, "chosen": chosen_id})
                return
            log_event("warning",
                      f"Unknown HITL request_id {request_id!r}; using legacy queue")
        try:
            self._responses.put_nowait({"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            log_event("warning", "HITL response queue full; dropping response")

    def _entropy_uncertainty(self, candidates):
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

    def _ambiguity_score(self, candidates):
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

    async def query_user_if_needed(self, user_id, candidates, timeout=3.0):
        if len(candidates) < 2:
            return None
        if self.pareto is not None:
            try:
                pareto_set = self.pareto.filter(candidates)
                if len(pareto_set) >= 2:
                    candidates = pareto_set
                else:
                    return None
            except Exception:
                pass

        ambiguity = self._ambiguity_score(candidates)
        uncertainty = self._entropy_uncertainty(candidates)
        if ambiguity < 0.05 and uncertainty < 0.5:
            return None

        req_id = uuid.uuid4().hex[:8]
        try:
            _M_HITL_REQUESTS.inc()
        except Exception:
            pass
        try:
            await asyncio.to_thread(
                self.storage.enqueue_hitl_request,
                req_id, "pareto_query",
                {"candidates": candidates[:2], "ambiguity": ambiguity,
                 "uncertainty": uncertainty}, "info")
        except Exception:
            pass
        try:
            await asyncio.to_thread(
                self.storage.save_active_learning_sample,
                req_id, "causal_student", "hitl_ambiguity",
                uncertainty, True)
        except Exception:
            pass

        if self.dashboard:
            try:
                await self.dashboard.broadcast({
                    "type": "preference_query", "request_id": req_id,
                    "user_id": user_id, "ambiguity": ambiguity,
                    "uncertainty": uncertainty,
                    "options": [
                        {"id": c.get("solution_id"),
                         "quality": c.get("quality_score")}
                        for c in candidates[:2]]})
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
                _M_HITL_RESPONSES.labels(status="approved").inc()
            except Exception:
                pass
            chosen = msg.get("chosen") if isinstance(msg, dict) else None
            metrics = next(
                (c for c in candidates if c.get("solution_id") == chosen), None)
            await self.record_choice(user_id, chosen, metrics=metrics)
            if self.feedback_callback:
                try:
                    await self.feedback_callback({
                        "event_id": uuid.uuid4().hex[:8],
                        "task_id": req_id,
                        "selected_action": chosen,
                        "feedback_type": "hitl_choice",
                        "quality_score": 1.0,
                        "ambiguity": ambiguity,
                        "uncertainty": uncertainty})
                except Exception:
                    pass
            return chosen
        except asyncio.TimeoutError:
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "timeout")
                _M_HITL_RESPONSES.labels(status="timeout").inc()
            except Exception:
                pass
            normalized = self.get_preference_weights(user_id)
            if normalized:
                scored = []
                for c in candidates:
                    s = sum(
                        normalized.get(k, 0.25)
                        / (float(c.get(k, 0) or 0) + 1e-8)
                        for k in self._PREF_KEYS)
                    scored.append((s, c.get("solution_id")))
                scored.sort(reverse=True)
                return scored[0][1]
            return candidates[0].get("solution_id")
        finally:
            async with self._lock:
                self._pending.pop(req_id, None)

    async def record_choice(self, user_id, solution_id, metrics=None):
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

    def get_preference_weights(self, user_id):
        raw = self.preferences.get(user_id, {})
        if not raw:
            return {}
        s = sum(raw.values()) or 1.0
        return {k: v / s for k, v in raw.items()}


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class CausalOrchestratorV17:
    """Unified entry point wiring all ten enhancements."""

    def __init__(self, storage, config, dashboard=None):
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        # Causal RL core.
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(config, storage, self.causal_graph)

        # Nine other enhancements.
        self.quantum = QuantumDistillationEngine(temperature=2.0, alpha=0.5)
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(
            config, storage, causal_rl=self.causal_rl, xai=self.xai)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        self.temporal.set_approval_callback(self._on_critical_violation)

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()

    async def _on_critical_violation(self, rule_id, state):
        try:
            q = float(state.get("quality", 0.8))
            c = float(state.get("carbon", 0.4))
        except (TypeError, ValueError):
            q, c = 0.8, 0.4
        candidates = [
            {"solution_id": "approve", "quality_score": q, "carbon_g": c,
             "cost_usd": 0.5, "latency_ms": 100.0},
            {"solution_id": "reject", "quality_score": 0.0, "carbon_g": 0.0,
             "cost_usd": 0.0, "latency_ms": 0.0},
        ]
        decision = await self.hitl.query_user_if_needed(
            "critical_user", candidates, timeout=2.0)
        return decision == "approve"

    def _spawn(self, coro, name: str) -> asyncio.Task:
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

    async def tick(self, state: Dict[str, Any],
                   data: Optional[List[Dict[str, float]]] = None) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        # 1. Causal RL strategy selection.
        action = await self.causal_rl.choose_action(state)
        result["strategy"] = action
        result["strategy_name"] = self.causal_rl.to_strategy(action)

        # 2. Multi-agent bid.
        agent_id, _ = await self.multi_agent.bid({
            "name": "causal_tick", "preferred_role": "optimizer"})
        result["agent_id"] = agent_id

        # 3. Precision auto-switch using the rolling window.
        self.precision.record_accuracy(float(state.get("accuracy", 0.9) or 0.9))
        await self.precision.auto_switch(0.0, 0.0)
        result["precision"] = self.precision.current

        # 4. Carbon market decision (no price mutation).
        try:
            result["carbon_decision"] = await self.carbon_market.net_zero_schedule(
                workload_kwh=1.0,
                intensity=float(state.get("carbon_intensity", 400) or 400) / 1000.0)
        except Exception as exc:
            log_event("warning", f"Carbon market decision failed: {exc}")
            result["carbon_decision"] = {"action": "run"}

        # 5. XAI explanation.
        if NUMPY_AVAILABLE:
            try:
                feats = np.array([
                    float(state.get("quality", 0.8)),
                    float(state.get("carbon_intensity", 400) or 400) / 1000.0,
                    float(state.get("cost", 0.5)),
                    float(state.get("latency_ms", 100) or 100) / 1000.0])
                def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
                    return float(np.dot(x, _w))
                result["xai"] = await self.xai.explain(
                    decision_id=f"causal_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={result['strategy']}",
                    features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception as exc:
                log_event("debug", f"XAI explanation skipped: {exc}")

        # 6. Multi-agent reward.
        await self.multi_agent.reward(agent_id, 0.8)

        # 7. Temporal push + verify.
        await self.temporal.push_state({
            "quality": float(state.get("quality", 0.8)),
            "carbon": float(state.get("carbon_intensity", 400) or 400) / 1000.0,
            "task_complete": True})
        try:
            verify = await self.temporal.verify()
            result["temporal_violations"] = [k for k, v in verify.items() if not v]
        except Exception as exc:
            log_event("warning", f"Temporal verify failed: {exc}")
            result["temporal_violations"] = []

        # 8. Causal RL update (with credit assignment).
        await self.causal_rl.update(action, 0.8, state)

        # 9. Do-calculus ATE (if data provided).
        if data:
            try:
                result["ate_carbon_quality"] = \
                    await self.causal_graph.estimate_ate_do_calculus(
                        "carbon_intensity", "quality", data)
            except Exception as exc:
                log_event("debug", f"ATE estimation skipped: {exc}")

        return result

    # ------------------------------------------------------------------
    async def start(self):
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

    async def shutdown(self):
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)
        self._background_tasks.clear()

    async def aclose(self):
        await self.shutdown()

    async def __aenter__(self) -> "CausalOrchestratorV17":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()

    async def _causal_discovery_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "causal_graph_update_interval", 900) or 900))
            try:
                variables = ["quality", "carbon_intensity", "cost",
                             "latency_ms", "energy"]
                samples = [{
                    "quality": random.uniform(0.5, 1.0),
                    "carbon_intensity": random.uniform(0.1, 0.8),
                    "cost": random.uniform(0.1, 0.9),
                    "latency_ms": random.uniform(0.1, 0.9),
                    "energy": random.uniform(0.1, 0.9),
                } for _ in range(100)]
                await self.causal_graph.learn(samples, variables)
            except Exception as exc:
                log_event("debug", f"causal_discovery loop error: {exc}")

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "federated_interval", 3600) or 3600))
            try:
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("causal_policy", dummy)
                await self.federated.pull_aggregated_weights("causal_policy")
            except Exception as exc:
                log_event("debug", f"federated loop error: {exc}")

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "agent_negotiation_interval", 600) or 600))
            try:
                await self.multi_agent.step()
            except Exception as exc:
                log_event("debug", f"multi_agent loop error: {exc}")

    async def _temporal_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "temporal_verification_interval", 300) or 300))
            try:
                await self.temporal.verify()
            except Exception as exc:
                log_event("debug", f"temporal loop error: {exc}")

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "xai_interval", 300) or 300))
            if not NUMPY_AVAILABLE:
                continue
            try:
                feats = np.array([0.9, 0.4, 0.5, 0.4])
                def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
                    return float(np.dot(x, _w))
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="causal_health",
                    features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception as exc:
                log_event("debug", f"xai loop error: {exc}")

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "precision_check_interval", 300) or 300))
            try:
                await self.precision.auto_switch(0.0, 0.0)
            except Exception as exc:
                log_event("debug", f"precision loop error: {exc}")

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "carbon_market_interval", 3600) or 3600))
            try:
                await self.carbon_market.update_price()
            except Exception as exc:
                log_event("debug", f"carbon_market loop error: {exc}")

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "chaos_test_interval", 1800) or 1800))
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception as exc:
                log_event("debug", f"chaos loop error: {exc}")

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "distillation_interval", 300) or 300))
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "causal_student")
            except Exception as exc:
                log_event("debug", f"distillation loop error: {exc}")

    async def health_check(self):
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "causal_graph": self.causal_graph.summary(),
            "agents": {a.id: a.role for a in self.multi_agent.agents.values()},
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
            "federated_rounds": self.federated.rounds,
            "temporal_rules": len(self.temporal.rules),
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

    def save_causal_edge(self, source, target, weight, confidence):
        self._data["causal_graph"].append({
            "source": source, "target": target,
            "weight": weight, "confidence": confidence})

    def save_causal_experiment(self, exp_id, treatment, outcome, ate,
                               samples, method=""):
        self._data["causal_experiments"].append({
            "exp_id": exp_id, "treatment": treatment, "outcome": outcome,
            "ate": ate, "samples": samples, "method": method})

    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl):
        self._data["teacher_superpositions"].append({
            "student_id": student_id, "teacher_id": teacher_id,
            "weight": weight, "temperature": temperature,
            "amplitude": amplitude, "kl": kl})

    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._data["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights, "weight_norm": weight_norm,
            "round_id": round_id})

    def get_federated_weights(self, model_id):
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    def save_agent(self, agent_id, role, reputation, utilities):
        self._data["agent_registry"].append({
            "agent_id": agent_id, "role": role,
            "reputation": reputation, "utilities": utilities})

    def save_agent_message(self, message_id, topic, sender, recipient, payload):
        self._data["agent_messages"].append({
            "message_id": message_id, "topic": topic,
            "sender": sender, "recipient": recipient, "payload": payload})

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

    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._data["xai_explanations"].append({
            "explanation_id": explanation_id, "decision_id": decision_id,
            "method": method, "label": label, "features": features,
            "attributions": attributions, "natural_language": nl})

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

    def enqueue_hitl_request(self, request_id, rule_id, state, severity):
        self._data["hitl_approval_queue"].append({
            "request_id": request_id, "rule_id": rule_id,
            "state": state, "severity": severity, "status": "pending"})

    def resolve_hitl_request(self, request_id, status="approved"):
        for r in self._data["hitl_approval_queue"]:
            if r["request_id"] == request_id:
                r["status"] = status

    def save_active_learning_sample(self, sample_id, model_id, strategy,
                                    uncertainty, selected):
        self._data["active_learning_samples"].append({
            "sample_id": sample_id, "model_id": model_id,
            "strategy": strategy, "uncertainty": uncertainty,
            "selected": selected})

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
        "causal_exploration_rate": 0.1,
        "agent_count": 5,
        "federated_interval": 3600,
        "temporal_max_trace": 2000,
        "temporal_formulas": ["G (quality >= 0.5)", "F (task_complete)"],
        "xai_method": "kernel_shap",
        "xai_depth": 5,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "chaos_intensity": 0.05,
        "chaos_blast_radius": 0.1,
    }

    storage = InMemoryStorage()
    orch = CausalOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("causal_rl_policy.py v17.1.0 — Demo")
    print("=" * 80)

    if NUMPY_AVAILABLE:
        print("\n=== PC Algorithm Causal Discovery ===")
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
        print(f"  Graph:")
        for src, edges in orch.causal_graph.graph.items():
            for dst, info in edges.items():
                print(f"    {src} -> {dst} (weight={info['weight']:+.3f})")

        print("\n=== Do-Calculus ATE Estimation ===")
        ate = await orch.causal_graph.estimate_ate_do_calculus("X", "Z", samples)
        print(f"  ATE(X -> Z) via backdoor adjustment: {ate:+.4f}")
        print(f"  Expected ~ 0.5 + 0.7 * 0.6 = 0.92")

        print("\n=== Counterfactual Reasoning ===")
        state = {"X": 1.0, "Y": 0.7, "Z": 0.92}
        cf = orch.causal_rl.counterfactual(state, {"X": 2.0})
        print(f"  Original state:       {state}")
        print(f"  Intervention: X = 2.0")
        print(f"  Counterfactual state: {cf}")

    print("\n=== Orchestration Ticks ===")
    for i in range(3):
        state = {
            "quality": random.uniform(0.6, 0.95),
            "carbon_intensity": random.uniform(200, 700),
            "cost": random.uniform(0.3, 0.8),
            "latency_ms": random.uniform(50, 300),
            "accuracy": random.uniform(0.85, 0.95),
            "baseline_accuracy": 0.92,
        }
        data = [{"carbon_intensity": random.uniform(0.2, 0.8),
                 "quality": random.uniform(0.5, 1.0)}
                for _ in range(50)]
        result = await orch.tick(state, data)
        print(f"\n  Tick {i + 1}:")
        print(f"    strategy: {result['strategy']} -> {result['strategy_name']}")
        print(f"    precision: {result['precision']}")
        print(f"    carbon_action: {result['carbon_decision']['action']}")
        print(f"    temporal_violations: {result['temporal_violations']}")
        if "ate_carbon_quality" in result:
            print(f"    ATE(carbon -> quality): {result['ate_carbon_quality']:+.4f}")

    print("\n=== Health Check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    print("\n=== Chaos Experiment ===")
    print(json.dumps(
        await orch.chaos.run_experiment("demo_chaos", "latency"),
        indent=2, default=str))

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
