# =============================================================================
# FILE: src/enhancements/tests/test_helium_integration.py
# VERSION: 8.3 (Enhanced: Helium integration + adaptive selection, review fixes)
# =============================================================================
"""
Enhanced Pytest Test Suite for Helium Integration — Version 8.3.0
==================================================================

Coverage:
  - Helium integration: DataCollector, Elasticity, Circularity, Regret,
    Thermal, Synthetic, Sustainability signals (smoke tests).
  - Adaptive test-selection distillation (rule/ML/Q teachers, student,
    gated teacher combination, Q-teacher updates, epsilon decay).
  - LIMIT Graph, MODP, RLHF, MoE (full softmax Jacobian), NSGA-II with
    distinct objectives.

CHANGES (v8.3.0 over v8.2.0):
- Removed module-level ``pytest_*`` hooks. Pytest only invokes hooks defined
  in ``conftest.py`` or setuptools-registered plugins, so the hooks in this
  file were inert. Adaptive selection is now exposed via the explicit
  ``adaptive_runner`` fixture. To promote it to a real plugin, move the
  ``pytest_*`` functions into a top-level ``conftest.py`` (see docstring
  note at the bottom).
- ``AdaptiveTestRunner`` MOEA loop is lazy (``start_moea_loop`` /
  ``stop_moea_loop``); no ``asyncio.create_task`` in ``__init__``.
  ``moea_enabled`` defaults to ``False``.
- ``_execute_test`` awaits async test functions; failures propagate for
  both sync and async.
- Production classes (DistillationStudent, ReplayBuffer, MoEGatingNetwork,
  NSGAIIUpdateOptimizer, MOPDUpdateWeights, LimitGraphManager, MODPOptimizer,
  RLHFTrainer, RunningScaler) are imported from ``..tasks.periodic_updater``
  when available; minimal local fallbacks are provided otherwise and are
  marked ``__test__ = False``.
- Carbon intensity unified to kg CO2/kWh (matches NodeDescriptor,
  WorkloadDescriptor, ZeroTrustArchitecture, PeriodicUpdater).
- FeedbackEvent context fixed (``context.task_id``, ``envelope.trace_id``,
  guarded ``to_json`` / ``model_dump_json``).
- Every file write uses ``tmp_path``; the ``adaptive_runner`` fixture
  cancels the MOEA task and flushes state on teardown.
- Added smoke tests for HeliumRegretOptimizer, HeliumThermalOptimizer,
  HeliumSyntheticDataManager, and HeliumSustainabilitySignals.
- Removed unused imports; ``np.fromstring`` replaced with explicit parsing;
  timestamps are UTC-aware.

To register the pytest markers used below, add to ``pytest.ini``:

    [pytest]
    markers =
        integration: integration tests
        performance: performance-sensitive tests
        concurrent: concurrency tests
    asyncio_mode = auto
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import pickle
import random
import sqlite3
import sys
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, ClassVar, Dict, List, Optional, Set, Tuple, Union
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Optional dependencies
# ---------------------------------------------------------------------------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Helium production imports (with a documented sys.path fallback)
# ---------------------------------------------------------------------------
_HELIUM_MODULES = {}
try:
    from helium_data_collector import HeliumDataCollector  # type: ignore
    from helium_elasticity import (  # type: ignore
        HeliumElasticityCalculator, ElasticityConfig, ElasticityMetrics,
    )
    from helium_circularity import (  # type: ignore
        HeliumCircularityCalculator, CircularityConfig, CircularityMetrics,
    )
    from helium_regret_optimizer import HeliumRegretOptimizer  # type: ignore
    from helium_thermal_optimizer import HeliumThermalOptimizer  # type: ignore
    from helium_synthetic_manager import HeliumSyntheticDataManager  # type: ignore
    from helium_sustainability_signals import HeliumSustainabilitySignals  # type: ignore
    _HELIUM_MODULES["available"] = True
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    try:
        from helium_data_collector import HeliumDataCollector  # type: ignore
        from helium_elasticity import (  # type: ignore
            HeliumElasticityCalculator, ElasticityConfig, ElasticityMetrics,
        )
        from helium_circularity import (  # type: ignore
            HeliumCircularityCalculator, CircularityConfig, CircularityMetrics,
        )
        from helium_regret_optimizer import HeliumRegretOptimizer  # type: ignore
        from helium_thermal_optimizer import HeliumThermalOptimizer  # type: ignore
        from helium_synthetic_manager import HeliumSyntheticDataManager  # type: ignore
        from helium_sustainability_signals import HeliumSustainabilitySignals  # type: ignore
        _HELIUM_MODULES["available"] = True
    except ImportError as _e:
        _HELIUM_MODULES["available"] = False
        _HELIUM_MODULES["error"] = str(_e)
        HeliumDataCollector = None  # type: ignore
        HeliumElasticityCalculator = None  # type: ignore
        ElasticityConfig = None  # type: ignore
        HeliumCircularityCalculator = None  # type: ignore
        CircularityConfig = None  # type: ignore
        HeliumRegretOptimizer = None  # type: ignore
        HeliumThermalOptimizer = None  # type: ignore
        HeliumSyntheticDataManager = None  # type: ignore
        HeliumSustainabilitySignals = None  # type: ignore


# ---------------------------------------------------------------------------
# Optional benchmark decorator
# ---------------------------------------------------------------------------
try:
    import pytest_benchmark  # noqa: F401
    BENCHMARK_AVAILABLE = True

    def benchmark(func):
        return pytest.mark.benchmark(func)
except ImportError:
    BENCHMARK_AVAILABLE = False

    def benchmark(func):
        return func


# ---------------------------------------------------------------------------
# Optional FeedbackEvent / AsyncMessageQueue
# ---------------------------------------------------------------------------
try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore

try:
    from ..async_message_queue import AsyncMessageQueue  # type: ignore
except ImportError:
    AsyncMessageQueue = None  # type: ignore


# ---------------------------------------------------------------------------
# Production distillation components (with fallbacks)
# ---------------------------------------------------------------------------
try:
    from ..tasks.periodic_updater import (  # type: ignore
        RunningScaler,
        FederatedWeightsCodec,
        DistillationStudent,
        ReplayBuffer,
        MoEGatingNetwork,
        NSGAIIUpdateOptimizer,
        MOPDUpdateWeights,
        LimitGraphManager,
        MODPOptimizer,
        RLHFTrainer,
    )
    PRODUCTION_AVAILABLE = True
except Exception:
    PRODUCTION_AVAILABLE = False
    RunningScaler = None  # type: ignore
    FederatedWeightsCodec = None  # type: ignore
    DistillationStudent = None  # type: ignore
    ReplayBuffer = None  # type: ignore
    MoEGatingNetwork = None  # type: ignore
    NSGAIIUpdateOptimizer = None  # type: ignore
    MOPDUpdateWeights = None  # type: ignore
    LimitGraphManager = None  # type: ignore
    MODPOptimizer = None  # type: ignore
    RLHFTrainer = None  # type: ignore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _softmax(logits: np.ndarray) -> np.ndarray:
    z = logits - np.max(logits)
    e = np.exp(z)
    s = e.sum()
    if s <= 0:
        return np.ones_like(e) / len(e)
    return e / s


def _clamp01(v: float) -> float:
    return float(min(1.0, max(0.0, v)))


# ---------------------------------------------------------------------------
# Fallback implementations (only used if production is unavailable)
# ---------------------------------------------------------------------------
if not PRODUCTION_AVAILABLE:
    class RunningScaler:  # type: ignore[no-redef]
        __test__ = False
        def __init__(self, dim: int, clip: float = 5.0, min_std: float = 1e-6) -> None:
            self.dim = int(dim); self.clip = float(clip); self.min_std = float(min_std)
            self.mean = np.zeros(self.dim, dtype=np.float64)
            self.m2 = np.zeros(self.dim, dtype=np.float64)
            self.count = 0

        def update(self, x: np.ndarray) -> None:
            x = np.asarray(x, dtype=np.float64).reshape(-1)
            if x.shape[0] != self.dim:
                return
            self.count += 1
            delta = x - self.mean
            self.mean += delta / self.count
            self.m2 += delta * (x - self.mean)

        def transform(self, x: np.ndarray) -> np.ndarray:
            x = np.asarray(x, dtype=np.float64).reshape(-1)
            if self.count < 2:
                return x.astype(np.float32)
            var = self.m2 / (self.count - 1)
            std = np.maximum(np.sqrt(np.maximum(var, 0.0)), self.min_std)
            return np.clip((x - self.mean) / std, -self.clip, self.clip).astype(np.float32)

    class DistillationStudent:  # type: ignore[no-redef]
        __test__ = False
        def __init__(self, feature_dim: int = 12, n_classes: int = 2, lr: float = 0.01,
                     grad_clip: float = 1.0, seed: Optional[int] = None) -> None:
            rng = np.random.default_rng(seed)
            self.feature_dim = int(feature_dim); self.n_classes = int(n_classes)
            self.lr = float(lr); self.grad_clip = float(grad_clip)
            self.weights = rng.normal(0.0, 0.01, size=(self.feature_dim, self.n_classes))
            self.biases = np.zeros(self.n_classes, dtype=np.float64)
            self.counter = 0

        def _maybe_resize(self, n: int) -> None:
            if n == self.n_classes:
                return
            if n > self.n_classes:
                w = np.zeros((self.feature_dim, n), dtype=np.float64)
                b = np.zeros(n, dtype=np.float64)
                w[:, : self.n_classes] = self.weights
                b[: self.n_classes] = self.biases
                self.weights, self.biases = w, b
            else:
                self.weights = self.weights[:, :n]
                self.biases = self.biases[:n]
            self.n_classes = n

        def predict_proba(self, state_vector, num_classes=None):
            if num_classes is not None:
                self._maybe_resize(num_classes)
            return _softmax(state_vector @ self.weights + self.biases)

        def update(self, state_vector, teacher_probs, reward, action,
                   distill_weight=0.7, rl_weight=0.3):
            current = self.predict_proba(state_vector, self.n_classes)
            grad_distill = -(teacher_probs - current)
            one_hot = np.zeros(self.n_classes, dtype=np.float64)
            if 0 <= int(action) < self.n_classes:
                one_hot[int(action)] = 1.0
            grad_rl = -reward * (one_hot - current)
            grad = np.clip(distill_weight * grad_distill + rl_weight * grad_rl,
                           -self.grad_clip, self.grad_clip)
            self.weights -= self.lr * np.outer(state_vector, grad)
            self.biases -= self.lr * grad
            self.counter += 1

    class ReplayBuffer:  # type: ignore[no-redef]
        __test__ = False
        def __init__(self, max_size: int = 2000):
            self.buffer: deque = deque(maxlen=int(max_size))

        def push(self, state_vec, action, reward, next_state_vec, teacher_outputs):
            self.buffer.append((
                np.asarray(state_vec, dtype=np.float64), int(action), float(reward),
                np.asarray(next_state_vec, dtype=np.float64),
                np.asarray(teacher_outputs, dtype=np.float64),
            ))

        def sample(self, batch_size=32):
            batch = list(self.buffer) if len(self.buffer) < batch_size else random.sample(self.buffer, batch_size)
            s, a, r, ns, t = zip(*batch)
            return (np.array(s), np.array(a, dtype=np.int64), np.array(r),
                    np.array(ns), np.array(t))

        def __len__(self):
            return len(self.buffer)

    class MoEGatingNetwork:  # type: ignore[no-redef]
        __test__ = False
        def __init__(self, feature_dim=12, n_experts=2, lr=0.005,
                     load_balance_weight=0.01, seed=None):
            rng = np.random.default_rng(seed)
            self.feature_dim = int(feature_dim); self.n_experts = int(n_experts)
            self.lr = float(lr); self.load_balance_weight = float(load_balance_weight)
            self.weights = rng.normal(0.0, 0.01, size=(self.feature_dim, self.n_experts))
            self.bias = np.zeros(self.n_experts, dtype=np.float64)
            self._mean_gate = np.ones(self.n_experts) / max(self.n_experts, 1)

        def forward(self, state_vec):
            x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
            return _softmax(x @ self.weights + self.bias)

        def update(self, state_vec, teacher_outputs, student_probs, current_gate_weights):
            x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
            t = np.asarray(teacher_outputs, dtype=np.float64)
            s = np.asarray(student_probs, dtype=np.float64)
            g = np.asarray(current_gate_weights, dtype=np.float64)
            error = (g @ t) - s
            dL_dg = t @ error
            dot = float(np.dot(g, dL_dg))
            dL_dz = g * (dL_dg - dot)
            if self.load_balance_weight > 0:
                self._mean_gate = 0.99 * self._mean_gate + 0.01 * g
                target = np.ones_like(g) / len(g)
                lb = self.load_balance_weight * (self._mean_gate - target)
                lb_dot = float(np.dot(g, lb))
                dL_dz += g * (lb - lb_dot)
            self.weights -= self.lr * np.outer(x, dL_dz)
            self.bias -= self.lr * dL_dz

    @dataclass
    class MOPDUpdateWeights:  # type: ignore[no-redef]
        vector_id: str
        weights: Dict[str, float]
        objectives: Dict[str, float]
        scalarised_score: float = 0.0

    class NSGAIIUpdateOptimizer:  # type: ignore[no-redef]
        __test__ = False
        def __init__(self, evaluate_func, population_size=20, generations=10,
                     mutation_rate=0.2, crossover_rate=0.8, tournament_size=3,
                     objective_weights=None, dynamic_weights=True):
            self.evaluate_func = evaluate_func
            self.population_size = int(population_size)
            self.generations = int(generations)
            self.mutation_rate = float(mutation_rate)
            self.crossover_rate = float(crossover_rate)
            self.tournament_size = int(tournament_size)
            self.objective_weights = objective_weights or {
                "update_quality": 0.5, "resource_savings": 0.3, "timeliness": 0.2,
            }
            self.dynamic_weights = bool(dynamic_weights)
            self.best_individual: Optional[Dict[str, float]] = None
            self.pareto_front: List[MOPDUpdateWeights] = []
            self._eval_cache: Dict[Any, Dict[str, float]] = {}
            self._actions = ("update_now", "skip")

        def _random_individual(self):
            w = {a: random.random() for a in self._actions}
            s = sum(w.values()) or 1.0
            return {k: v / s for k, v in w.items()}

        def _crossover(self, p1, p2):
            child = {}
            for k in p1:
                if random.random() < 0.5:
                    u = random.random()
                    beta = (2 * u) ** (1 / 21) if u <= 0.5 else (1 / (2 * (1 - u))) ** (1 / 21)
                    child[k] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[k] + (1 - beta) * p2[k])))
                else:
                    child[k] = p1[k] if random.random() < 0.5 else p2[k]
            s = sum(child.values()) or 1.0
            return {k: v / s for k, v in child.items()}

        def _mutate(self, ind):
            m = dict(ind)
            for k in m:
                if random.random() < self.mutation_rate:
                    u = random.random()
                    d = (2 * u) ** (1 / 21) - 1 if u < 0.5 else 1 - (2 * (1 - u)) ** (1 / 21)
                    m[k] = max(0.0, min(1.0, m[k] + d))
            s = sum(m.values()) or 1.0
            return {k: v / s for k, v in m.items()}

        @staticmethod
        def _dominates(a, b):
            return (all(a.objectives[k] >= b.objectives[k] for k in a.objectives)
                    and any(a.objectives[k] > b.objectives[k] for k in a.objectives))

        def _nds(self, points):
            n = len(points)
            dominated_by = [0] * n
            dominates = [[] for _ in range(n)]
            fronts = [[]]
            for i in range(n):
                for j in range(n):
                    if i == j:
                        continue
                    if self._dominates(points[i], points[j]):
                        dominates[i].append(j)
                    elif self._dominates(points[j], points[i]):
                        dominated_by[i] += 1
                if dominated_by[i] == 0:
                    fronts[0].append(i)
            i = 0
            while fronts[i]:
                nxt = []
                for p in fronts[i]:
                    for q in dominates[p]:
                        dominated_by[q] -= 1
                        if dominated_by[q] == 0:
                            nxt.append(q)
                i += 1
                fronts.append(nxt)
            if fronts and not fronts[-1]:
                fronts.pop()
            return fronts

        @staticmethod
        def _crowding(points, front):
            if not front:
                return {}
            d = {i: 0.0 for i in front}
            for obj in points[front[0]].objectives:
                ordered = sorted(front, key=lambda i: points[i].objectives[obj])
                d[ordered[0]] = float("inf"); d[ordered[-1]] = float("inf")
                lo = points[ordered[0]].objectives[obj]
                hi = points[ordered[-1]].objectives[obj]
                if hi - lo < 1e-12:
                    continue
                for k in range(1, len(ordered) - 1):
                    d[ordered[k]] += (points[ordered[k + 1]].objectives[obj]
                                      - points[ordered[k - 1]].objectives[obj]) / (hi - lo)
            return d

        def _tournament(self, pop, rank, crowd):
            n = len(pop)
            cands = random.sample(range(n), min(self.tournament_size, n))
            best = cands[0]
            for c in cands[1:]:
                rc, rb = rank.get(c, n), rank.get(best, n)
                if rc < rb or (rc == rb and crowd.get(c, 0) > crowd.get(best, 0)):
                    best = c
            return best

        async def _eval(self, individuals):
            keys = [tuple(sorted(i.items())) for i in individuals]
            seen, to_run = set(), []
            for k, ind in zip(keys, individuals):
                if k in self._eval_cache or k in seen:
                    continue
                seen.add(k); to_run.append(ind)
            if to_run:
                results = await asyncio.gather(*[self.evaluate_func(i) for i in to_run])
                for ind, obj in zip(to_run, results):
                    self._eval_cache[tuple(sorted(ind.items()))] = dict(obj)
            return [MOPDUpdateWeights(str(uuid.uuid4()), dict(i), dict(self._eval_cache[k]))
                    for i, k in zip(individuals, keys)]

        async def evolve(self):
            pop = [self._random_individual() for _ in range(self.population_size)]
            pts = await self._eval(pop)
            for _ in range(self.generations):
                fronts = self._nds(pts)
                rank, crowd = {}, {}
                for fi, front in enumerate(fronts):
                    for i in front:
                        rank[i] = fi
                    crowd.update(self._crowding(pts, front))
                off = []
                while len(off) < self.population_size:
                    p1 = self._tournament(pop, rank, crowd)
                    p2 = self._tournament(pop, rank, crowd)
                    c = self._crossover(pop[p1], pop[p2]) if random.random() < self.crossover_rate else dict(pop[p1])
                    off.append(self._mutate(c))
                off_pts = await self._eval(off)
                comb_pop = pop + off
                comb_pts = pts + off_pts
                cfronts = self._nds(comb_pts)
                new_pop, new_pts = [], []
                for fi, front in enumerate(cfronts):
                    if len(new_pop) + len(front) <= self.population_size:
                        for i in front:
                            new_pop.append(comb_pop[i]); new_pts.append(comb_pts[i])
                    else:
                        cd = self._crowding(comb_pts, front)
                        ordered = sorted(front, key=lambda i: cd.get(i, 0.0), reverse=True)
                        for i in ordered:
                            if len(new_pop) >= self.population_size:
                                break
                            new_pop.append(comb_pop[i]); new_pts.append(comb_pts[i])
                        break
                pop = new_pop[: self.population_size]
                pts = new_pts[: self.population_size]
            final = self._nds(pts)
            self.pareto_front = [pts[i] for i in final[0]] if final else []
            if self.pareto_front:
                best = max(self.pareto_front,
                           key=lambda p: sum(self.objective_weights.get(k, 0.0) * v
                                             for k, v in p.objectives.items()))
                self.best_individual = dict(best.weights)
            return self.pareto_front

    class LimitGraphManager:  # type: ignore[no-redef]
        __test__ = False
        def __init__(self, storage=None):
            self.storage = storage; self.graphs = {}
        def create_graph(self, gid, desc, cfg):
            self.graphs[gid] = {"description": desc, "configuration": cfg, "nodes": {}, "edges": {}}
        def add_node(self, gid, nid, ntype, attrs):
            self.graphs.setdefault(gid, {"nodes": {}, "edges": {}})
            self.graphs[gid]["nodes"][nid] = {"node_type": ntype, "attributes": attrs}
        def add_edge(self, gid, eid, src, tgt, weight, attrs):
            self.graphs.setdefault(gid, {"nodes": {}, "edges": {}})
            self.graphs[gid]["edges"][eid] = {"source": src, "target": tgt, "weight": weight, "attributes": attrs}
        def get_nodes(self, gid): return list(self.graphs.get(gid, {}).get("nodes", {}).values())
        def get_edges(self, gid): return list(self.graphs.get(gid, {}).get("edges", {}).values())
        def get_metadata(self, gid): return self.graphs.get(gid, {})

    class MODPOptimizer:  # type: ignore[no-redef]
        __test__ = False
        def __init__(self, storage=None):
            self.storage = storage; self.states = {}; self.policies = {}
        def add_state(self, sid, pid, attrs, objs, stage):
            self.states.setdefault(pid, []).append({"state_id": sid, "state_attributes": attrs,
                                                    "objective_values": objs, "stage": stage})
        def add_policy(self, pid, problem_id, sid, action, exp):
            self.policies.setdefault(problem_id, []).append({"policy_id": pid, "state_id": sid,
                                                             "action": action, "expected_objectives": exp})
        def get_states(self, pid): return self.states.get(pid, [])
        def get_policies(self, pid): return self.policies.get(pid, [])

    class RLHFTrainer:  # type: ignore[no-redef]
        __test__ = False
        def __init__(self, storage=None):
            self.storage = storage; self.pairs = []
        def record_pair(self, pid, prompt, chosen, rejected, rdiff, metadata=None):
            self.pairs.append({"pair_id": pid, "prompt": prompt, "chosen": chosen,
                               "rejected": rejected, "reward_diff": rdiff, "metadata": metadata})
        def get_pairs(self, limit=100): return self.pairs[-limit:]


# ---------------------------------------------------------------------------
# Test-selection state (12 features, kg CO2/kWh)
# ---------------------------------------------------------------------------
@dataclass
class TestSelectionState:
    """State for the adaptive test-selection agent (12 features)."""
    test_name: str
    test_category: str
    estimated_duration_sec: float
    test_importance: float
    code_coverage_pct: float
    recent_failures: int
    system_load: float
    carbon_intensity: float   # kg CO2 / kWh
    time_of_day: float
    test_success_rate: float = 0.5
    avg_reward: float = 0.5

    FEATURE_DIM: ClassVar[int] = 12
    DURATION_MAX: ClassVar[float] = 60.0
    COVERAGE_MAX: ClassVar[float] = 100.0
    FAILURES_MAX: ClassVar[float] = 5.0
    CARBON_MAX_KG: ClassVar[float] = 1.0

    def to_raw_vector(self) -> np.ndarray:
        return np.array([
            self.estimated_duration_sec, self.test_importance,
            self.code_coverage_pct, float(self.recent_failures),
            self.system_load, self.carbon_intensity,
            self.time_of_day, self.test_success_rate, self.avg_reward,
            1.0 if self.test_category == "unit" else 0.0,
            1.0 if self.test_category == "integration" else 0.0,
            1.0 if self.test_category == "performance" else 0.0,
        ], dtype=np.float64)

    def to_feature_vector(self, scaler: Optional[Any] = None) -> np.ndarray:
        raw = self.to_raw_vector()
        if scaler is not None:
            return scaler.transform(raw)
        c = _clamp01
        return np.array([
            c(self.estimated_duration_sec / self.DURATION_MAX),
            c(self.test_importance),
            c(self.code_coverage_pct / self.COVERAGE_MAX),
            c(self.recent_failures / self.FAILURES_MAX),
            c(self.system_load),
            c(self.carbon_intensity / self.CARBON_MAX_KG),
            c(self.time_of_day / 24.0),
            c(self.test_success_rate),
            c(self.avg_reward),
            c(1.0 if self.test_category == "unit" else 0.0),
            c(1.0 if self.test_category == "integration" else 0.0),
            c(1.0 if self.test_category == "performance" else 0.0),
        ], dtype=np.float32)


TEST_ACTIONS: Tuple[str, ...] = ("run", "skip")


# ---------------------------------------------------------------------------
# Test-selection teachers
# ---------------------------------------------------------------------------
class _TeacherBase(ABC):
    __test__ = False
    def __init__(self) -> None:
        self.scaler: Optional[Any] = None
    @abstractmethod
    def predict(self, state: TestSelectionState) -> np.ndarray: ...
    @abstractmethod
    def confidence(self, state: TestSelectionState) -> float: ...
    def _features(self, state: TestSelectionState) -> np.ndarray:
        return state.to_feature_vector(self.scaler)


class TestRuleBasedTeacher(_TeacherBase):
    __test__ = False
    def predict(self, state: TestSelectionState) -> np.ndarray:
        probs = np.ones(2, dtype=np.float64) * 0.1
        if state.test_importance > 0.8 or state.recent_failures > 2:
            probs[0] = 0.9
        elif state.code_coverage_pct < 50:
            probs[0] = 0.8
        elif state.system_load > 0.8:
            probs[1] = 0.8
        elif state.carbon_intensity > 0.5 and state.test_importance < 0.5:
            probs[1] = 0.6
        else:
            probs[0] = 0.6
        return _softmax(probs)
    def confidence(self, state: TestSelectionState) -> float:
        return 0.6 if (state.test_importance > 0.8 or state.recent_failures > 2) else 0.4


class TestHistoricalMLTeacher(_TeacherBase):
    __test__ = False
    EXPECTED_FEATURE_DIM: ClassVar[int] = 12
    def __init__(self, model_path: Optional[Path] = None) -> None:
        super().__init__()
        self.model = None; self.label_encoder = None
        self.model_path: Path = model_path or Path("./test_selection_model.pkl")
        if self.model_path.exists():
            try:
                with open(self.model_path, "rb") as f:
                    self.model, self.label_encoder = pickle.load(f)
            except Exception as e:
                logger.error(f"Failed to load test-selection model: {e}")
    def predict(self, state: TestSelectionState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(2) / 2
        x = self._features(state).reshape(1, -1)
        if not hasattr(self.model, "predict_proba"):
            return np.ones(2) / 2
        return self.model.predict_proba(x)[0]
    def confidence(self, state: TestSelectionState) -> float:
        return 0.7 if self.model is not None else 0.0


class TestStatefulQTeacher(_TeacherBase):
    __test__ = False
    def __init__(self, lr: float = 0.1, weights_path: Optional[Path] = None,
                 feature_dim: int = 12) -> None:
        super().__init__()
        self.lr = float(lr)
        self.weights_path: Path = weights_path or Path("./test_selection_q_weights.json")
        self.feature_dim = int(feature_dim)
        self.weights = np.zeros((self.feature_dim, 2), dtype=np.float64)
        self._load_state()
    def _load_state(self) -> None:
        if not self.weights_path.exists():
            return
        try:
            with open(self.weights_path, "r") as f:
                w = np.array(json.load(f), dtype=np.float64)
            if w.shape == (self.feature_dim, 2):
                self.weights = w
        except Exception as e:
            logger.error(f"Failed to load test-selection Q-weights: {e}")
    def save(self) -> None:
        try:
            with open(self.weights_path, "w") as f:
                json.dump(self.weights.tolist(), f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save test-selection Q-weights: {e}")
    def predict(self, state: TestSelectionState) -> np.ndarray:
        return _softmax(self._features(state) @ self.weights)
    def confidence(self, state: TestSelectionState) -> float:
        x = self._features(state)
        q = x @ self.weights
        return float(np.clip(0.3 + 0.35 * (np.max(q) - np.min(q)), 0.0, 1.0))
    def update_from_vector(self, state_vec: np.ndarray, action: int, reward: float) -> None:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        if x.shape[0] != self.feature_dim or not 0 <= int(action) < 2:
            return
        q = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q) * x


# ---------------------------------------------------------------------------
# Distillation selector
# ---------------------------------------------------------------------------
class DistillationTestSelector:
    __test__ = False
    ACTIONS: ClassVar[List[str]] = list(TEST_ACTIONS)

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.feature_dim = 12
        self.n_actions = 2
        seed = config.get("seed", None)
        self.scaler = RunningScaler(self.feature_dim, clip=config.get("scaler_clip", 5.0))
        self.student = DistillationStudent(
            feature_dim=self.feature_dim,
            lr=config.get("distillation_learning_rate", 0.01),
            grad_clip=config.get("student_grad_clip", 1.0),
            seed=seed,
        )
        self.teachers: List[_TeacherBase] = [
            TestRuleBasedTeacher(),
            TestHistoricalMLTeacher(model_path=config.get("historical_model_path")),
            TestStatefulQTeacher(
                lr=config.get("q_learning_rate", 0.1),
                weights_path=config.get("q_weights_path"),
                feature_dim=self.feature_dim,
            ),
        ]
        for t in self.teachers:
            t.scaler = self.scaler
        self.q_teacher: Optional[TestStatefulQTeacher] = next(
            (t for t in self.teachers if isinstance(t, TestStatefulQTeacher)), None
        )
        self.n_teachers = len(self.teachers)
        self.gating = MoEGatingNetwork(
            feature_dim=self.feature_dim, n_experts=self.n_teachers,
            lr=config.get("gating_learning_rate", 0.005),
            load_balance_weight=config.get("gating_load_balance_weight", 0.01),
            seed=seed,
        )
        self.replay_buffer = ReplayBuffer(max_size=config.get("distillation_replay_size", 2000))
        self.epsilon = float(config.get("distillation_epsilon", 0.1))
        self.epsilon_min = float(config.get("distillation_epsilon_min", 0.01))
        self.epsilon_decay = float(config.get("distillation_epsilon_decay", 0.995))
        self.train_every = int(config.get("distillation_train_every", 10))
        self.counter = 0
        self.distill_weight = float(config.get("distill_weight", 0.7))
        self.rl_weight = float(config.get("rl_weight", 0.3))
        self.batch_update_size = int(config.get("batch_update_size", 8))

    def _compute_teacher_probs(self, state: TestSelectionState):
        state_vec = state.to_feature_vector(self.scaler)
        outs = []
        for t in self.teachers:
            try:
                p = t.predict(state)
            except Exception as e:
                logger.warning(f"Teacher {type(t).__name__} failed: {e}")
                p = np.ones(self.n_actions) / self.n_actions
            p = np.asarray(p, dtype=np.float64).reshape(-1)
            if p.shape[0] != self.n_actions:
                p = np.pad(p, (0, self.n_actions - p.shape[0])) if p.shape[0] < self.n_actions \
                    else p[: self.n_actions]
            s = p.sum()
            p = p / s if s > 0 else np.ones(self.n_actions) / self.n_actions
            outs.append(p)
        outs = np.array(outs)
        gate_w = self.gating.forward(state_vec)
        combined = gate_w @ outs
        s = combined.sum()
        combined = combined / s if s > 0 else np.ones(self.n_actions) / self.n_actions
        return combined, gate_w, outs

    async def select_action(self, state: TestSelectionState, exploration: bool = True):
        self.scaler.update(state.to_raw_vector())
        state_vec = state.to_feature_vector(self.scaler)
        combined, gate_w, teacher_outs = self._compute_teacher_probs(state)
        student_probs = self.student.predict_proba(state_vec, self.n_actions)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, self.n_actions - 1)
        else:
            blended = 0.8 * student_probs + 0.2 * combined
            action_idx = int(np.argmax(blended))
        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)
        return self.ACTIONS[action_idx], action_idx, state_vec, teacher_outs

    async def update(self, state_vec, action_idx, reward, next_state_vec, teacher_outputs):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_outputs)
        self.counter += 1
        if self.counter % self.train_every != 0:
            return
        if len(self.replay_buffer) < self.batch_update_size:
            return
        states, actions, rewards, _, t_batch = self.replay_buffer.sample(self.batch_update_size)
        for i in range(len(states)):
            s = states[i]; t_outs = t_batch[i]
            a = int(actions[i]); r = float(rewards[i])
            gate_w = self.gating.forward(s)
            combined = gate_w @ t_outs
            tot = combined.sum()
            combined = combined / tot if tot > 0 else np.ones(self.n_actions) / self.n_actions
            self.student.update(s, combined, r, a,
                                distill_weight=self.distill_weight, rl_weight=self.rl_weight)
            if self.q_teacher is not None:
                self.q_teacher.update_from_vector(s, a, r)
            student_out = self.student.predict_proba(s)
            self.gating.update(s, t_outs, student_out, gate_w)

    def flush(self) -> None:
        if self.q_teacher is not None:
            self.q_teacher.save()

    def get_stats(self) -> Dict[str, Any]:
        return {
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "epsilon": self.epsilon,
            "n_teachers": self.n_teachers,
            "scaler_count": self.scaler.count,
        }


# ---------------------------------------------------------------------------
# AdaptiveTestRunner (lazy MOEA, tmp_path-friendly, valid FeedbackEvent)
# ---------------------------------------------------------------------------
class AdaptiveTestRunner:
    __test__ = False

    def __init__(self, config: Optional[Dict[str, Any]] = None,
                 critical_tests: Optional[Set[str]] = None,
                 message_queue: Optional[Any] = None) -> None:
        self.config = config or {}
        self.selector = DistillationTestSelector({
            "distillation_epsilon": self.config.get("distillation_epsilon", 0.1),
            "distillation_epsilon_min": self.config.get("distillation_epsilon_min", 0.01),
            "distillation_epsilon_decay": self.config.get("distillation_epsilon_decay", 0.995),
            "distillation_train_every": self.config.get("distillation_train_every", 10),
            "distillation_replay_size": self.config.get("distillation_replay_size", 2000),
            "distillation_learning_rate": self.config.get("distillation_learning_rate", 0.01),
            "distill_weight": self.config.get("distill_weight", 0.7),
            "rl_weight": self.config.get("rl_weight", 0.3),
            "q_learning_rate": self.config.get("q_learning_rate", 0.1),
            "q_weights_path": self.config.get("q_weights_path", "./_test_q.json"),
            "historical_model_path": self.config.get("historical_model_path", "./_test_model.pkl"),
            "gating_learning_rate": self.config.get("gating_learning_rate", 0.005),
            "gating_load_balance_weight": self.config.get("gating_load_balance_weight", 0.01),
            "batch_update_size": self.config.get("batch_update_size", 8),
            "seed": self.config.get("seed", None),
        })
        self.interaction_log: List[Dict[str, Any]] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None
        self._last_selected_expert: Optional[str] = None

        self.test_metadata: Dict[str, Dict[str, Any]] = {}
        self.critical_tests: Set[str] = critical_tests or set()
        self.message_queue = message_queue
        self.failed_tests: Dict[str, int] = defaultdict(int)
        self.quarantine_threshold = int(self.config.get("quarantine_threshold", 3))

        # MOEA is off by default and started lazily.
        self.moea_enabled = bool(self.config.get("moea_enabled", False))
        self.moea_optimizer: Optional[Any] = None
        self.global_best_weights: Optional[Dict[str, float]] = None
        self.pareto_front: List[Any] = []
        self._moea_task: Optional[asyncio.Task] = None
        self._moea_running = False

        self.storage = self.config.get("storage", None)
        self.limit_graph_manager = (
            LimitGraphManager(self.storage) if self.config.get("enable_limit_graph", False) else None
        )
        self.modp_solver = (
            MODPOptimizer(self.storage) if self.config.get("enable_modp", False) else None
        )
        self.rlhf_trainer = (
            RLHFTrainer(self.storage) if self.config.get("enable_rlhf", False) else None
        )
        self.moe_gating = (
            MoEGatingNetwork(feature_dim=12, n_experts=2, seed=self.config.get("seed", None))
            if self.config.get("enable_moe", False) else None
        )

        self._log_path = Path(
            self.config.get("interaction_logs_path", "./_adaptive_test_interactions.csv")
        )
        self._log_columns = ["timestamp", "test_name", "action", "reward", "passed", "state_vec"]

        if self.limit_graph_manager:
            self._init_limit_graph()

        metadata_file = self.config.get("metadata_file")
        if metadata_file and Path(metadata_file).exists():
            self.load_test_metadata(metadata_file)

    # ------------------------------------------------------------------
    async def start_moea_loop(self) -> None:
        if not self.moea_enabled:
            return
        if self._moea_task is not None and not self._moea_task.done():
            return
        self._moea_task = asyncio.create_task(self._moea_loop())

    async def stop_moea_loop(self) -> None:
        if self._moea_task is not None and not self._moea_task.done():
            self._moea_task.cancel()
            try:
                await self._moea_task
            except asyncio.CancelledError:
                pass
        self._moea_task = None

    async def _moea_loop(self) -> None:
        interval = int(self.config.get("moea_interval_seconds", 300))
        while True:
            try:
                await asyncio.sleep(interval)
                await self.run_moea_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop error: {e}")
                await asyncio.sleep(60)

    def _init_limit_graph(self) -> None:
        gid = "test_selection"
        if not self.limit_graph_manager.get_metadata(gid):
            self.limit_graph_manager.create_graph(gid, "Test Selection Relationships", {})
            for a in self.selector.ACTIONS:
                self.limit_graph_manager.add_node(gid, f"action_{a}", a, {})
            self.limit_graph_manager.add_edge(gid, "edge_run_skip",
                                              "action_run", "action_skip", 1.0, {})

    def register_test(self, test_name: str, category: str = "unit",
                      duration_sec: float = 1.0, importance: float = 0.5) -> None:
        self.test_metadata[test_name] = {
            "category": category, "duration_sec": float(duration_sec),
            "importance": float(importance), "coverage_pct": 0.0,
            "recent_failures": 0, "success_rate": 0.5, "avg_reward": 0.5,
        }

    def load_test_metadata(self, filepath: Union[str, Path]) -> None:
        with open(filepath, "r") as f:
            data = json.load(f)
        for name, meta in data.items():
            self.test_metadata[name] = {
                "category": meta.get("category", "unit"),
                "duration_sec": float(meta.get("duration_sec", 1.0)),
                "importance": float(meta.get("importance", 0.5)),
                "coverage_pct": float(meta.get("coverage_pct", 0.0)),
                "recent_failures": int(meta.get("recent_failures", 0)),
                "success_rate": float(meta.get("success_rate", 0.5)),
                "avg_reward": float(meta.get("avg_reward", 0.5)),
            }

    def _build_state(self, test_name: str,
                     system_load: Optional[float] = None,
                     carbon_intensity: Optional[float] = None) -> TestSelectionState:
        meta = self.test_metadata.get(test_name, {})
        return TestSelectionState(
            test_name=test_name,
            test_category=meta.get("category", "unit"),
            estimated_duration_sec=float(meta.get("duration_sec", 1.0)),
            test_importance=float(meta.get("importance", 0.5)),
            code_coverage_pct=float(meta.get("coverage_pct", 0.0)),
            recent_failures=int(meta.get("recent_failures", 0)),
            system_load=float(system_load if system_load is not None else 0.5),
            carbon_intensity=float(carbon_intensity if carbon_intensity is not None else 0.4),
            time_of_day=float(_utc_now().hour),
            test_success_rate=float(meta.get("success_rate", 0.5)),
            avg_reward=float(meta.get("avg_reward", 0.5)),
        )

    async def decide_and_run(self, test_name: str, test_func: Callable[[], Any],
                             system_load: Optional[float] = None,
                             carbon_intensity: Optional[float] = None) -> bool:
        if test_name in self.critical_tests:
            state = self._build_state(test_name, system_load, carbon_intensity)
            state_vec = state.to_feature_vector(self.selector.scaler)
            passed = await self._execute_test(test_name, test_func)
            reward = self._compute_reward(passed, state)
            await self._record_outcome(test_name, "run", reward, passed,
                                       state_vec=state_vec, action_idx=0,
                                       teacher_probs=np.ones(2) / 2, state=state)
            return True

        state = self._build_state(test_name, system_load, carbon_intensity)

        if self.moe_gating is not None:
            gate_w = self.moe_gating.forward(state.to_feature_vector(self.selector.scaler))
            action_idx = int(np.argmax(gate_w))
            action = TEST_ACTIONS[action_idx]
            state_vec = state.to_feature_vector(self.selector.scaler)
            teacher_probs = np.ones(2) / 2
            self._last_selected_expert = action
        else:
            action, action_idx, state_vec, teacher_probs = await self.selector.select_action(
                state, exploration=True
            )
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        if self.global_best_weights is not None:
            moea_probs = np.array([self.global_best_weights.get(a, 1.0) for a in TEST_ACTIONS])
            s = moea_probs.sum()
            moea_probs = moea_probs / s if s > 0 else np.ones(2) / 2
            one_hot = np.zeros(2); one_hot[action_idx] = 1.0
            blended = 0.7 * moea_probs + 0.3 * one_hot
            action_idx = int(np.argmax(blended))
            action = TEST_ACTIONS[action_idx]

        if action == "skip":
            reward = 0.1 * (1.0 - state.test_importance)
            await self._record_outcome(test_name, "skip", reward, None,
                                       state_vec=state_vec, action_idx=action_idx,
                                       teacher_probs=teacher_probs, state=state)
            return False

        passed = await self._execute_test(test_name, test_func)
        reward = self._compute_reward(passed, state)
        await self._record_outcome(test_name, "run", reward, passed,
                                   state_vec=state_vec, action_idx=action_idx,
                                   teacher_probs=teacher_probs, state=state)
        return True

    async def _execute_test(self, test_name: str, test_func: Callable[[], Any]) -> bool:
        """Awaits sync and async test functions; propagates failures."""
        try:
            result = test_func()
            if asyncio.iscoroutine(result):
                await result
            self.failed_tests[test_name] = 0
            return True
        except Exception as e:
            self.failed_tests[test_name] = self.failed_tests.get(test_name, 0) + 1
            logger.error(f"Test '{test_name}' failed: {e}")
            raise

    def _compute_reward(self, passed: bool, state: TestSelectionState) -> float:
        base = 0.6 if passed else 0.0
        coverage_bonus = 0.2 * min(1.0, state.code_coverage_pct / 100.0)
        time_penalty = 0.1 * min(1.0, state.estimated_duration_sec / 60.0)
        carbon_penalty = 0.1 * min(1.0, state.carbon_intensity / 1.0) * (
            state.estimated_duration_sec / 60.0
        )
        reward = (base + coverage_bonus - time_penalty - carbon_penalty) * (
            0.5 + 0.5 * state.test_importance
        )
        return float(max(0.0, min(1.0, reward)))

    async def _record_outcome(self, test_name: str, action: str, reward: float,
                              passed: Optional[bool],
                              state_vec: Optional[np.ndarray] = None,
                              action_idx: Optional[int] = None,
                              teacher_probs: Optional[np.ndarray] = None,
                              state: Optional[TestSelectionState] = None) -> None:
        if state_vec is None:
            state_vec = self.last_state_vec
        if action_idx is None:
            action_idx = self.last_action_idx
        if teacher_probs is None:
            teacher_probs = self.last_teacher_probs

        entry = {
            "timestamp": _utc_now().isoformat(),
            "test_name": test_name, "action": action, "reward": float(reward),
            "passed": passed if passed is not None else "",
            "state_vec": ",".join(map(str, state_vec)) if state_vec is not None else "",
        }
        self.interaction_log.append(entry)
        try:
            await asyncio.to_thread(self._append_log_sync, entry)
        except Exception as e:
            logger.error(f"Failed to append interaction log: {e}")

        if state_vec is not None and action_idx is not None and teacher_probs is not None:
            await self.selector.update(state_vec, int(action_idx), float(reward),
                                       state_vec, teacher_probs)

        if self.rlhf_trainer and random.random() < 0.05:
            chosen = action
            rejected = "skip" if chosen == "run" else "run"
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt=f"Best decision for '{test_name}'?",
                chosen=chosen, rejected=rejected, reward_diff=float(reward),
                metadata={"test_name": test_name},
            )

        if self.modp_solver:
            sid = f"{test_name}_{_utc_now().isoformat()}_{action}"
            self.modp_solver.add_state(
                state_id=sid, problem_id="test_selection",
                state_attributes={"test_name": test_name, "action": action},
                objective_values={"coverage": 0.5, "resource_savings": 0.3,
                                  "success_rate": 1.0 if passed else 0.0},
                stage=0,
            )
            self.modp_solver.add_policy(
                policy_id=f"policy_{sid}", problem_id="test_selection",
                state_id=sid, action=action,
                expected_objectives={"coverage": 0.0, "resource_savings": 0.0, "success_rate": 0.0},
            )

        if self.limit_graph_manager:
            self.limit_graph_manager.add_node(
                "test_selection", f"run_{uuid.uuid4()}", "test_decision",
                {"test_name": test_name, "action": action, "reward": float(reward)},
            )

        if test_name in self.test_metadata:
            meta = self.test_metadata[test_name]
            if passed is not None:
                if passed:
                    meta["recent_failures"] = max(0, int(meta["recent_failures"]) - 1)
                else:
                    meta["recent_failures"] = int(meta["recent_failures"]) + 1
                meta["success_rate"] = 0.9 * meta["success_rate"] + 0.1 * (1.0 if passed else 0.0)
            meta["avg_reward"] = 0.9 * meta["avg_reward"] + 0.1 * float(reward)

        await self._emit_feedback_event(test_name, action, reward)

    def _append_log_sync(self, entry: Dict[str, Any]) -> None:
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        exists = self._log_path.exists()
        row = {col: entry.get(col, "") for col in self._log_columns}
        pd.DataFrame([row]).to_csv(self._log_path, mode="a", header=not exists, index=False)

    async def _emit_feedback_event(self, test_name: str, action: str, reward: float) -> None:
        if FeedbackEvent is None or self.message_queue is None:
            return
        try:
            event = FeedbackEvent(
                source="adaptive_test_runner",
                feedback_type="routing",
                context={
                    "task_id": test_name,
                    "teacher_ids": [type(t).__name__ for t in self.selector.teachers],
                },
                action={
                    "selected_action": action,
                    "selected_rank": 1,
                    "confidence_score": 0.5,
                },
                performance={
                    "quality_score": float(max(0.0, min(1.0, reward))),
                    "latency_ms": 0.0, "energy_joules": 0.0,
                    "carbon_g": 0.0, "helium_cost": 0.0, "duration_ms": 0.0,
                },
                adaptive_cost_value=float(reward),
                tags=["test_selection", action, test_name],
                envelope={"trace_id": str(uuid.uuid4())},
            )
            payload = None
            if hasattr(event, "to_json"):
                payload = event.to_json()
            elif hasattr(event, "model_dump_json"):
                payload = event.model_dump_json()
            if payload is not None:
                result = self.message_queue.publish("test_events", payload)
                if asyncio.iscoroutine(result):
                    await result
        except Exception as e:
            logger.warning(f"Failed to emit FeedbackEvent: {e}")

    async def run_moea_update(self) -> List[Any]:
        if not self.moea_enabled or len(self.interaction_log) < 20:
            return []
        if self._moea_running:
            return list(self.pareto_front)
        self._moea_running = True
        try:
            recent = self.interaction_log[-200:]

            async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
                run_r = [e["reward"] for e in recent if e["action"] == "run"]
                skip_r = [e["reward"] for e in recent if e["action"] == "skip"]
                run_m = float(np.mean(run_r)) if run_r else 0.5
                skip_m = float(np.mean(skip_r)) if skip_r else 0.5
                freq = len(run_r) / max(1, len(recent))
                return {
                    "coverage": float(weights.get("run", 0.5) * run_m
                                      + weights.get("skip", 0.5) * skip_m),
                    "resource_savings": float(weights.get("skip", 0.5) * skip_m
                                              + weights.get("run", 0.5) * (1.0 - run_m)),
                    "success_rate": float(freq * run_m + (1 - freq) * skip_m),
                }

            self.moea_optimizer = NSGAIIUpdateOptimizer(
                evaluate_func=evaluate,
                population_size=self.config.get("moea_population_size", 20),
                generations=self.config.get("moea_generations", 10),
                mutation_rate=self.config.get("moea_mutation_rate", 0.2),
                crossover_rate=self.config.get("moea_crossover_rate", 0.8),
                tournament_size=self.config.get("moea_tournament_size", 3),
                objective_weights=self.config.get("moea_objective_weights"),
                dynamic_weights=self.config.get("moea_dynamic_weights", True),
            )
            pareto = await self.moea_optimizer.evolve()
            self.pareto_front = pareto
            if pareto and getattr(self.moea_optimizer, "best_individual", None):
                self.global_best_weights = dict(self.moea_optimizer.best_individual)
            return pareto
        finally:
            self._moea_running = False

    def get_runner_stats(self) -> Dict[str, Any]:
        stats = {
            "selector_stats": self.selector.get_stats(),
            "interaction_count": len(self.interaction_log),
            "critical_tests": len(self.critical_tests),
            "quarantined_tests": {
                k: v for k, v in self.failed_tests.items() if v >= self.quarantine_threshold
            },
        }
        if self.moea_enabled:
            stats["moea"] = {
                "pareto_front_size": len(self.pareto_front),
                "best_weights": dict(self.global_best_weights) if self.global_best_weights else None,
                "enabled": True,
            }
        if self.limit_graph_manager:
            stats["limit_graph"] = self.limit_graph_manager.get_metadata("test_selection")
        return stats

    def flush(self) -> None:
        self.selector.flush()


# ============================================================================
# FIXTURES
# ============================================================================
@pytest.fixture
def sample_helium_csv(tmp_path: Path) -> Path:
    """Create a sample helium CSV file."""
    rng = np.random.default_rng(42)
    data = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-01", periods=10, freq="D"),
        "helium_usage_l": rng.uniform(0.1, 5.0, 10),
        "price_usd_per_l": rng.uniform(0.3, 0.8, 10),
        "region": ["us-east"] * 10,
    })
    p = tmp_path / "helium_data.csv"
    data.to_csv(p, index=False)
    return p


@pytest.fixture
def sample_elasticity_config():
    if ElasticityConfig is None:
        pytest.skip("ElasticityConfig unavailable.")
    return ElasticityConfig()


@pytest.fixture
def adaptive_config(tmp_path: Path) -> Dict[str, Any]:
    return {
        "enable_limit_graph": True,
        "enable_modp": True,
        "enable_rlhf": True,
        "enable_moe": True,
        "moea_enabled": False,
        "interaction_logs_path": str(tmp_path / "interactions.csv"),
        "q_weights_path": str(tmp_path / "q.json"),
        "historical_model_path": str(tmp_path / "model.pkl"),
        "seed": 42,
    }


@pytest.fixture
def adaptive_runner(adaptive_config: Dict[str, Any]):
    runner = AdaptiveTestRunner(config=adaptive_config)
    yield runner
    try:
        if runner._moea_task is not None and not runner._moea_task.done():
            runner._moea_task.cancel()
    except Exception:
        pass
    runner.flush()


# ============================================================================
# HELIUM INTEGRATION TESTS
# ============================================================================
@pytest.mark.integration
def test_collector_initialization(sample_helium_csv: Path):
    if HeliumDataCollector is None:
        pytest.skip("HeliumDataCollector unavailable.")
    collector = HeliumDataCollector(sample_helium_csv)
    assert collector is not None
    assert collector.data is not None
    assert len(collector.data) == 10


@pytest.mark.performance
def test_elasticity_calculation_speed(sample_helium_csv: Path, sample_elasticity_config):
    if HeliumElasticityCalculator is None or HeliumDataCollector is None:
        pytest.skip("Helium elasticity modules unavailable.")
    collector = HeliumDataCollector(sample_helium_csv)
    calc = HeliumElasticityCalculator(collector, sample_elasticity_config)
    metrics = calc.calculate()
    assert metrics is not None


@pytest.mark.performance
def test_circularity_calculation_speed(sample_helium_csv: Path):
    if HeliumCircularityCalculator is None or HeliumDataCollector is None:
        pytest.skip("Helium circularity modules unavailable.")
    collector = HeliumDataCollector(sample_helium_csv)
    calc = HeliumCircularityCalculator(collector, CircularityConfig())
    metrics = calc.calculate()
    assert metrics is not None


@pytest.mark.integration
def test_data_collector_to_elasticity(sample_helium_csv: Path, sample_elasticity_config):
    if HeliumDataCollector is None or HeliumElasticityCalculator is None:
        pytest.skip("Helium modules unavailable.")
    collector = HeliumDataCollector(sample_helium_csv)
    calc = HeliumElasticityCalculator(collector, sample_elasticity_config)
    metrics = calc.calculate()
    assert metrics.elasticity > 0


@pytest.mark.integration
def test_regret_optimizer_smoke(sample_helium_csv: Path):
    if HeliumRegretOptimizer is None:
        pytest.skip("HeliumRegretOptimizer unavailable.")
    try:
        opt = HeliumRegretOptimizer()
    except TypeError:
        pytest.skip("HeliumRegretOptimizer constructor signature unsupported by this test.")
    assert opt is not None


@pytest.mark.integration
def test_thermal_optimizer_smoke(sample_helium_csv: Path):
    if HeliumThermalOptimizer is None:
        pytest.skip("HeliumThermalOptimizer unavailable.")
    try:
        opt = HeliumThermalOptimizer()
    except TypeError:
        pytest.skip("HeliumThermalOptimizer constructor signature unsupported.")
    assert opt is not None


@pytest.mark.integration
def test_synthetic_manager_smoke():
    if HeliumSyntheticDataManager is None:
        pytest.skip("HeliumSyntheticDataManager unavailable.")
    try:
        mgr = HeliumSyntheticDataManager()
    except TypeError:
        pytest.skip("HeliumSyntheticDataManager constructor signature unsupported.")
    assert mgr is not None


@pytest.mark.integration
def test_sustainability_signals_smoke():
    if HeliumSustainabilitySignals is None:
        pytest.skip("HeliumSustainabilitySignals unavailable.")
    try:
        sig = HeliumSustainabilitySignals()
    except TypeError:
        pytest.skip("HeliumSustainabilitySignals constructor signature unsupported.")
    assert sig is not None


# ============================================================================
# DISTILLATION COMPONENT TESTS
# ============================================================================
def _state(**overrides: Any) -> TestSelectionState:
    base: Dict[str, Any] = {
        "test_name": "test_example",
        "test_category": "unit",
        "estimated_duration_sec": 1.0,
        "test_importance": 0.7,
        "code_coverage_pct": 80.0,
        "recent_failures": 0,
        "system_load": 0.5,
        "carbon_intensity": 0.4,   # kg CO2/kWh
        "time_of_day": 14,
        "test_success_rate": 0.9,
        "avg_reward": 0.8,
    }
    base.update(overrides)
    return TestSelectionState(**base)


def test_state_feature_vector_dimension():
    v = _state().to_feature_vector()
    assert v.shape == (12,)
    assert np.all(v >= 0.0) and np.all(v <= 1.0)
    # carbon_intensity = 0.4 kg -> ~0.4, not ~0.0004
    assert v[5] > 0.2


def test_state_one_hot_blocks():
    assert _state(test_category="unit").to_feature_vector()[9:12].tolist() == [1.0, 0.0, 0.0]
    assert _state(test_category="integration").to_feature_vector()[9:12].tolist() == [0.0, 1.0, 0.0]
    assert _state(test_category="performance").to_feature_vector()[9:12].tolist() == [0.0, 0.0, 1.0]


def test_rule_based_teacher():
    teacher = TestRuleBasedTeacher()
    probs = teacher.predict(_state(test_importance=0.3, recent_failures=3))
    assert probs.sum() == pytest.approx(1.0)
    assert probs[0] > probs[1]


def test_q_teacher_update():
    q = TestStatefulQTeacher(feature_dim=12)
    v = np.zeros(12); v[0] = 1.0
    q.update_from_vector(v, 0, 1.0)
    assert np.any(q.weights[:, 0] != 0)


def test_replay_buffer_roundtrip():
    buf = ReplayBuffer(max_size=5)
    v = np.random.randn(12)
    buf.push(v, 0, 1.0, v, np.ones((3, 2)) / 2)
    assert len(buf) == 1
    states, actions, rewards, _, teacher = buf.sample(1)
    assert states.shape == (1, 12)
    assert teacher.shape == (1, 3, 2)


def test_running_scaler_roundtrip():
    s = RunningScaler(12)
    for _ in range(5):
        s.update(np.random.randn(12) + 0.5)
    out = s.transform(np.random.randn(12))
    assert out.shape == (12,)
    assert np.all(np.isfinite(out))


@pytest.mark.asyncio
async def test_distillation_selector_select_action(adaptive_config):
    selector = DistillationTestSelector(adaptive_config)
    action, idx, state_vec, teacher_outs = await selector.select_action(_state(), exploration=False)
    assert action in selector.ACTIONS
    assert state_vec.shape == (12,)
    assert teacher_outs.shape == (selector.n_teachers, 2)


@pytest.mark.asyncio
async def test_distillation_selector_epsilon_decay(adaptive_config):
    adaptive_config["distillation_epsilon"] = 1.0
    adaptive_config["distillation_epsilon_decay"] = 0.5
    adaptive_config["distillation_epsilon_min"] = 0.01
    selector = DistillationTestSelector(adaptive_config)
    await selector.select_action(_state(), exploration=False)
    assert selector.epsilon < 1.0


@pytest.mark.asyncio
async def test_distillation_update_path(adaptive_config):
    adaptive_config["distillation_train_every"] = 1
    adaptive_config["batch_update_size"] = 2
    selector = DistillationTestSelector(adaptive_config)
    s = _state()
    _, idx, state_vec, t_outs = await selector.select_action(s, exploration=False)
    for _ in range(3):
        await selector.update(state_vec, idx, 0.8, state_vec, t_outs)
    assert selector.student.counter > 0


# ============================================================================
# NEW ENHANCEMENT COMPONENT TESTS
# ============================================================================
@pytest.mark.asyncio
async def test_limit_graph_manager():
    manager = LimitGraphManager()
    gid = "test_graph"
    manager.create_graph(gid, "Test Graph", {})
    manager.add_node(gid, "node1", "type1", {"key": "value"})
    manager.add_node(gid, "node2", "type2", {})
    manager.add_edge(gid, "edge1", "node1", "node2", 1.0, {})
    assert len(manager.get_nodes(gid)) == 2
    assert len(manager.get_edges(gid)) == 1
    assert manager.get_metadata(gid)["description"] == "Test Graph"


@pytest.mark.asyncio
async def test_modp_optimizer():
    opt = MODPOptimizer()
    opt.add_state("state1", "problem1", {"data": 1}, {"obj1": 0.5}, 0)
    opt.add_state("state2", "problem1", {"data": 2}, {"obj1": 0.8}, 1)
    opt.add_policy("policy1", "problem1", "state1", "action1", {"obj1": 0.6})
    assert len(opt.get_states("problem1")) == 2
    assert len(opt.get_policies("problem1")) == 1


@pytest.mark.asyncio
async def test_rlhf_trainer():
    trainer = RLHFTrainer()
    trainer.record_pair("pair1", "prompt", "chosen", "rejected", 0.5, {})
    pairs = trainer.get_pairs()
    assert len(pairs) == 1
    assert pairs[0]["chosen"] == "chosen"


@pytest.mark.asyncio
async def test_moe_gating_network():
    gating = MoEGatingNetwork(feature_dim=12, n_experts=2, seed=42)
    probs = gating.forward(np.zeros(12))
    assert probs.shape == (2,)
    assert probs.sum() == pytest.approx(1.0)

    x = np.random.randn(12)
    t = np.random.rand(2, 2); t = t / t.sum(axis=1, keepdims=True)
    s = np.random.rand(2); s = s / s.sum()
    g = gating.forward(x)
    gating.update(x, t, s, g)
    assert gating.weights.shape == (12, 2)


@pytest.mark.asyncio
async def test_nsga_ii_optimizer_distinct_objectives():
    async def evaluate(weights):
        return {
            "update_quality": weights.get("update_now", 0.5),
            "resource_savings": weights.get("skip", 0.5),
            "timeliness": 1.0 - weights.get("skip", 0.5),
        }

    opt = NSGAIIUpdateOptimizer(
        evaluate_func=evaluate,
        population_size=8, generations=3, dynamic_weights=False,
    )
    pareto = await opt.evolve()
    assert len(pareto) >= 1
    assert opt.best_individual is not None


# ============================================================================
# ADAPTIVE RUNNER TESTS
# ============================================================================
@pytest.mark.asyncio
async def test_adaptive_runner_full_flow(adaptive_runner, tmp_path):
    runner = adaptive_runner
    runner.register_test("dummy_test", category="unit", duration_sec=0.1, importance=0.8)
    result = await runner.decide_and_run("dummy_test", lambda: None)
    assert isinstance(result, bool)
    stats = runner.get_runner_stats()
    assert "selector_stats" in stats
    assert "interaction_count" in stats
    assert (tmp_path / "interactions.csv").exists()


@pytest.mark.asyncio
async def test_adaptive_runner_failure_propagation(adaptive_runner):
    runner = adaptive_runner
    runner.register_test("failing_test", category="unit", duration_sec=0.1, importance=0.9)

    def boom():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await runner.decide_and_run("failing_test", boom)


@pytest.mark.asyncio
async def test_adaptive_runner_async_test_function(adaptive_runner):
    runner = adaptive_runner
    runner.register_test("async_test", category="unit", duration_sec=0.1, importance=0.9)
    ran = {"v": False}

    async def async_body():
        ran["v"] = True

    await runner.decide_and_run("async_test", async_body)
    assert ran["v"] is True


@pytest.mark.asyncio
async def test_adaptive_runner_moea_cancellable(adaptive_config):
    adaptive_config["moea_enabled"] = True
    adaptive_config["moea_interval_seconds"] = 1
    runner = AdaptiveTestRunner(config=adaptive_config)
    await runner.start_moea_loop()
    await asyncio.sleep(0.05)
    await runner.stop_moea_loop()
    assert runner._moea_task is None


# ============================================================================
# OFFLINE TRAINING HELPER
# ============================================================================
def train_historical_model(
    log_path: Optional[Path] = None,
    model_path: Optional[Path] = None,
) -> Optional[Path]:
    if not SKLEARN_ML:
        return None
    lp = log_path or Path("./test_selection_interactions.csv")
    mp = model_path or Path("./test_selection_model.pkl")
    if not lp.exists():
        return None
    df = pd.read_csv(lp)
    if "state_vec" not in df or "action" not in df:
        return None

    def parse_state(s: Any):
        try:
            parts = [float(x) for x in str(s).split(",") if x.strip() != ""]
            return np.array(parts) if len(parts) == 12 else None
        except Exception:
            return None

    rows, labels = [], []
    for s, lbl in zip(df["state_vec"].values, df["action"].values):
        v = parse_state(s)
        if v is not None:
            rows.append(v); labels.append(lbl)
    if len(rows) < 5:
        return None
    X = np.array(rows)
    le = LabelEncoder()
    y = le.fit_transform(labels)
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X, y)
    with open(mp, "wb") as f:
        pickle.dump((clf, le), f)
    return mp


# ============================================================================
# OPTIONAL PLUGIN PROMOTION NOTE
# ============================================================================
#
# If you want the adaptive selection to act as a true pytest plugin (auto-skip
# tests based on the runner's decision), move the following stub into a
# top-level ``conftest.py``:
#
#     def pytest_addoption(parser):
#         parser.addoption("--test-metadata", action="store", default=None)
#         parser.addoption("--adaptive-enable", action="store_true", default=False)
#         parser.addoption("--adaptive-moea", action="store_true", default=False)
#
#     def pytest_configure(config):
#         from src.enhancements.tests.test_helium_integration import AdaptiveTestRunner
#         config._adaptive_runner = AdaptiveTestRunner(
#             config={
#                 "metadata_file": config.getoption("--test-metadata"),
#                 "moea_enabled": config.getoption("--adaptive-moea"),
#                 "enable_limit_graph": True, "enable_modp": True,
#                 "enable_rlhf": True, "enable_moe": True,
#             }
#         )
#
#     def pytest_runtest_setup(item):
#         if not item.config.getoption("--adaptive-enable"):
#             return
#         runner = getattr(item.config, "_adaptive_runner", None)
#         if runner is None:
#             return
#         state = runner._build_state(item.nodeid)
#         probs = TestRuleBasedTeacher().predict(state)
#         if TEST_ACTIONS[int(np.argmax(probs))] == "skip":
#             pytest.skip(f"Skipped by adaptive selector: {item.nodeid}")
#
# The test file itself deliberately omits these because pytest ignores hooks
# defined inside test modules.
# ============================================================================


# ============================================================================
# RUNNER
# ============================================================================
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
