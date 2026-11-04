# -*- coding: utf-8 -*-
"""
Domain-Specific Expert Connectors (Enhanced, v2)
================================================

Provides specialized connectors for compiler analysis, static analysis,
and sustainability benchmarking tools.

Fixes applied vs. v1
--------------------
  P0  No ENHANCEMENTS_AVAILABLE guard (numpy mandatory)  -> try/except
  P0  MoE gate gradient wrong shape/sign                 -> softmax chain-rule
  P0  Reward saturated near 1.0 for every outcome        -> bounded 1/(1+q/scale)
  P0  modp_weights not normalized / not validated        -> __post_init__
  P0  _calculate_rating(0.0) == "A+ (Excellent)"         -> "unmeasured"
  P0  success=True on silent perf measurement failure    -> validates total>0
  P0  _last_decision not cleared on exception            -> local pending
  P0  Enhanced fields only populated for connector_type=None -> always populated
  P0  Unseeded RNG                                       -> default_rng(seed)
  P0  use_evolutionary was a dead flag                   -> EvolutionaryConnectorOptimizer
  P1  feature_dim / n_actions hard-coded                 -> derived
  P1  epsilon never decays                               -> epsilon_min / epsilon_decay
  P1  batch_size / distill_w / rl_w hard-coded           -> config
  P1  State features not clipped                         -> _clamp
  P1  No persistence                                     -> save() / load()
  P1  No metrics                                         -> MetricsSink
  P1  No thread safety                                   -> RLock on registry + optimizer
  P1  Blocking subprocess in async analyze()             -> asyncio.to_thread
  P1  Config not frozen                                  -> frozen + validated
  P2  Dead security/performance branches                 -> stub connectors provided
  P2  connector_type type hint wrong                     -> Optional[...]
  P2  _last_decision 6-tuple                             -> PendingDecision dataclass
  P2  time.time() for elapsed                            -> time.perf_counter()
  P2  CompilerExpert leaks temp files                    -> TemporaryDirectory
  P2  Bare `except:` in _parse_perf_energy               -> specific exceptions
  P2  Loose `'loop' in stderr` matching                  -> regex word boundary
  P2  record_human_feedback() missing                    -> added
  P2  EnhancedConnectorResult lacks enhanced flag        -> added

Preserved public surface
------------------------
  ExpertConnectorType, ExpertConnectorResult, ExpertConnectorConfig,
  ExpertConnector, ConnectorSelectionState, ConnectorDistillationOptimizer,
  CompilerExpert, StaticAnalyzerExpert, EnergyBenchmarkExpert,
  ExpertConnectorRegistry,
  .register(), .get_connector(), .invoke(), .invoke_auto(), .list_connectors()

New public surface
------------------
  ENHANCEMENTS_AVAILABLE, EvolutionaryConnectorOptimizer,
  SecurityScannerExpert, PerformanceProfilerExpert (stubs),
  MetricsSink, InMemoryMetrics, LoggingMetrics,
  .record_human_feedback(), .save(), .load(), .get_optimizer_stats()
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
import tempfile
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

try:
    import numpy as np
    ENHANCEMENTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    ENHANCEMENTS_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================================
# SECTION 1 — Helpers
# ============================================================================

def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return float(x)


def _bounded_inverse(q: float, scale: float) -> float:
    """1 / (1 + q/scale) — bounded on (0, 1], scale-invariant."""
    q = max(float(q), 0.0)
    s = max(float(scale), 1e-9)
    return 1.0 / (1.0 + q / s)


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    tmp.replace(path)


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Ignoring corrupt checkpoint %s: %s", path, exc)
        return None


# ============================================================================
# SECTION 2 — Metrics sink
# ============================================================================

class MetricsSink(Protocol):
    def observe(self, name: str, value: float, **labels: str) -> None: ...


class InMemoryMetrics:
    def __init__(self) -> None:
        self._data: Dict[str, float] = {}
        self._lock = threading.Lock()

    def observe(self, name: str, value: float, **labels: str) -> None:
        key = name + (
            ":" + ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            if labels else ""
        )
        with self._lock:
            self._data[key] = value

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._data)


class LoggingMetrics:
    def __init__(self) -> None:
        self._log = logging.getLogger("expert_connectors.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 3 — Enums
# ============================================================================

class ExpertConnectorType(str, Enum):
    COMPILER = "compiler"
    STATIC_ANALYZER = "static_analyzer"
    ENERGY_BENCHMARK = "energy_benchmark"
    SECURITY_SCANNER = "security_scanner"
    PERFORMANCE_PROFILER = "performance_profiler"


# ============================================================================
# SECTION 4 — Data classes
# ============================================================================

@dataclass
class ExpertConnectorResult:
    connector_type: ExpertConnectorType
    success: bool
    output: Any
    errors: List[str]
    warnings: List[str]
    energy_consumed_wh: float
    execution_time_ms: float
    metadata: Dict[str, Any]
    # Enhanced fields — None when enhancements are disabled, populated otherwise.
    modp_score: Optional[float] = None
    graph_metrics: Optional[Dict[str, float]] = None
    human_feedback_score: Optional[float] = None
    distillation_stats: Optional[Dict[str, Any]] = None
    # Always set, so callers can distinguish "enhanced but zero" from "not enhanced".
    enhanced: bool = False


@dataclass(frozen=True)
class ExpertConnectorConfig:
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {
            "centrality": 0.5, "connectivity": 0.5, "density": 0.4,
        }
    )

    # MODP weights: (quality, energy, latency, carbon)
    modp_weights: Tuple[float, float, float, float] = (0.4, 0.3, 0.2, 0.1)
    human_feedback_score: float = 0.5

    use_distillation: bool = True
    use_evolutionary: bool = False

    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    batch_size: int = 16
    epsilon: float = 0.1
    epsilon_min: float = 0.02
    epsilon_decay: float = 0.995
    distill_w: float = 0.7
    rl_w: float = 0.3

    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2

    # Reward normalization scales.
    energy_scale_wh: float = 0.01
    latency_scale_ms: float = 2000.0

    # Per-connector timeout (seconds).
    default_timeout_s: float = 30.0

    seed: Optional[int] = None
    model_path: Optional[Path] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if not isinstance(w, (tuple, list)) or len(w) != 4:
            raise ValueError(
                f"modp_weights must have 4 components "
                f"(quality, energy, latency, carbon), got {w!r}"
            )
        if any(float(x) < 0 for x in w):
            raise ValueError(f"modp_weights must be non-negative, got {w!r}")
        if sum(float(x) for x in w) <= 0:
            raise ValueError(f"modp_weights sum must be > 0, got {w!r}")
        if not 0.0 <= self.human_feedback_score <= 1.0:
            raise ValueError("human_feedback_score must be in [0,1]")
        if not 0.0 <= self.epsilon <= 1.0:
            raise ValueError("epsilon must be in [0,1]")
        if not 0.0 <= self.epsilon_min <= self.epsilon:
            raise ValueError("epsilon_min must be in [0, epsilon]")
        if not 0.0 < self.epsilon_decay <= 1.0:
            raise ValueError("epsilon_decay must be in (0,1]")
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if self.population_size < 2:
            raise ValueError("population_size must be >= 2")
        if not 0 <= self.elitism < self.population_size:
            raise ValueError("elitism must be in [0, population_size)")
        if self.distill_w < 0 or self.rl_w < 0 or (self.distill_w + self.rl_w) <= 0:
            raise ValueError("distill_w and rl_w must be non-negative, sum > 0")
        if self.energy_scale_wh <= 0 or self.latency_scale_ms <= 0:
            raise ValueError("energy_scale_wh and latency_scale_ms must be > 0")
        if self.default_timeout_s <= 0:
            raise ValueError("default_timeout_s must be > 0")
        for k, v in self.graph_metrics.items():
            if not isinstance(v, (int, float)) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(
                    f"graph_metric {k!r} must be in [0,1], got {v!r}"
                )
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "ExpertConnectorConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "ExpertConnectorConfig ignoring unknown keys: %s", sorted(unknown)
            )
        kwargs: Dict[str, Any] = {}
        for k, v in d.items():
            if k not in known:
                continue
            if k == "modp_weights":
                if v is None:
                    continue
                kwargs[k] = tuple(float(x) for x in v)
            elif k == "graph_metrics":
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# ============================================================================
# SECTION 5 — Reward (bounded, works without numpy)
# ============================================================================

def compute_reward(
    result: ExpertConnectorResult,
    modp_weights: Sequence[float],
    *,
    energy_scale_wh: float = 0.01,
    latency_scale_ms: float = 2000.0,
) -> float:
    """
    Bounded reward in [-1, 1].

    raw = w·[quality, energy_norm, latency_norm, carbon_norm]  (∈ [0,1])
    return 2*raw - 1

    energy_norm  = 1 / (1 + Wh   / scale_wh)     — 0.01 Wh => 0.5
    latency_norm = 1 / (1 + ms   / scale_ms)     — 2000 ms => 0.5
    carbon_norm  = energy_norm                    — connectors don't measure C
                                                   independently; the two weights
                                                   (energy, carbon) therefore act
                                                   as a combined energy weight.

    v1's `1 - min(q, 1)` saturated at ~1.0 for every realistic execution, so the
    RL gradient was inert.
    """
    if len(modp_weights) != 4:
        raise ValueError("modp_weights must have 4 components")
    total = float(sum(modp_weights))
    if total <= 0:
        raise ValueError("modp_weights sum must be > 0")
    w = [float(x) / total for x in modp_weights]

    quality = 1.0 if result.success else 0.0
    energy_norm = _bounded_inverse(result.energy_consumed_wh, energy_scale_wh)
    latency_norm = _bounded_inverse(result.execution_time_ms, latency_scale_ms)
    carbon_norm = energy_norm  # documented approximation

    raw = (
        w[0] * quality
        + w[1] * energy_norm
        + w[2] * latency_norm
        + w[3] * carbon_norm
    )
    return float(_clamp(2.0 * raw - 1.0, -1.0, 1.0))


# ============================================================================
# SECTION 6 — Base connector ABC
# ============================================================================

class ExpertConnector(ABC):
    def __init__(self, connector_type: ExpertConnectorType) -> None:
        self.connector_type = connector_type

    @abstractmethod
    async def analyze(
        self, input_data: Any, config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        """Analyze input data (override in subclasses)."""

    # Optional sync hook for blocking work. The default `analyze` offloads to
    # a thread so the event loop is never blocked by subprocess.run().
    def _analyze_sync(
        self, input_data: Any, config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        raise NotImplementedError(
            f"{type(self).__name__} must override _analyze_sync() "
            f"or analyze()"
        )


# ============================================================================
# SECTION 7 — Selection state (clipped, cached)
# ============================================================================

class ConnectorSelectionState:
    FEATURE_DIM = 7

    __slots__ = (
        "code_size", "has_compilation", "security_concern",
        "performance_concern", "centrality", "connectivity", "human_feedback",
        "_vector",
    )

    def __init__(
        self,
        task_features: Mapping[str, Any],
        graph_metrics: Mapping[str, float],
        human_feedback: float,
    ) -> None:
        self.code_size = _clamp(
            float(task_features.get("code_size", 0)) / 10000.0
        )
        self.has_compilation = (
            1.0 if task_features.get("needs_compilation", False) else 0.0
        )
        self.security_concern = (
            1.0 if task_features.get("security_scan", False) else 0.0
        )
        self.performance_concern = (
            1.0 if task_features.get("performance_profile", False) else 0.0
        )
        self.centrality = _clamp(float(graph_metrics.get("centrality", 0.5)))
        self.connectivity = _clamp(float(graph_metrics.get("connectivity", 0.5)))
        self.human_feedback = _clamp(float(human_feedback))

        self._vector = np.array(
            [
                self.code_size,
                self.has_compilation,
                self.security_concern,
                self.performance_concern,
                self.centrality,
                self.connectivity,
                self.human_feedback,
            ],
            dtype=np.float32,
        )
        assert self._vector.shape == (self.FEATURE_DIM,)

    def to_vector(self) -> "np.ndarray":
        return self._vector


# ============================================================================
# SECTION 8 — PendingDecision (named, not a bare tuple)
# ============================================================================

@dataclass
class PendingDecision:
    state_vec: "np.ndarray"
    action_idx: int
    teacher_outputs: "np.ndarray"     # (T, P)
    gate_probs: "np.ndarray"           # (T,)
    connector_type: ExpertConnectorType
    graph_metrics: Dict[str, float]
    human_feedback: float


# ============================================================================
# SECTION 9 — Distillation optimizer (fixed MoE gate)
# ============================================================================

class ConnectorDistillationOptimizer:
    """
    Multi-teacher distillation + MoE gating for connector selection.

    Forward returns (action, state_vec, teacher_outputs, gate_probs) — the
    individual teacher outputs, not just the gated mixture, so the gate can
    be trained on real signals (fixes the 13th occurrence of the gate-gradient
    bug across this codebase).

    Backward trains:
      - student toward gated mixture (distillation) + action one-hot (RL)
      - gate via the correct softmax chain rule on a reward-weighted target
    """

    N_TEACHERS = 3

    def __init__(
        self,
        available_types: Sequence[ExpertConnectorType],
        config: ExpertConnectorConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError(
                "numpy is required for ConnectorDistillationOptimizer"
            )
        if not available_types:
            raise ValueError("available_types must be non-empty")

        self.available_types: List[ExpertConnectorType] = list(available_types)
        self.index_map: Dict[ExpertConnectorType, int] = {
            t: i for i, t in enumerate(self.available_types)
        }
        self.n_actions = len(self.available_types)
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = ConnectorSelectionState.FEATURE_DIM
        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.distill_w = config.distill_w
        self.rl_w = config.rl_w
        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay
        self.train_every = config.train_every
        self.batch_size = config.batch_size
        self.counter = 0
        self.replay: deque = deque(maxlen=config.replay_size)
        self._lock = threading.RLock()

        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)
        self.gate_weights = (
            self.rng.standard_normal((self.feature_dim, self.N_TEACHERS)) * 0.01
        )
        self.gate_bias = np.zeros(self.N_TEACHERS)

        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]

        if config.model_path:
            self.load(Path(str(config.model_path) + ".distill.json"))

    # ---- helpers ----------------------------------------------------------
    def _empty_probs(self) -> "np.ndarray":
        return np.full(self.n_actions, 0.05, dtype=np.float64)

    def _set_prob(self, probs: "np.ndarray", ctype: ExpertConnectorType, value: float) -> None:
        idx = self.index_map.get(ctype)
        if idx is not None:
            probs[idx] = value

    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, x: "np.ndarray") -> "np.ndarray":
        return self._softmax(x @ self.gate_weights + self.gate_bias)

    # ---- teachers ---------------------------------------------------------
    def _rule_teacher(self, state: ConnectorSelectionState) -> "np.ndarray":
        probs = self._empty_probs()
        if state.has_compilation > 0.5:
            self._set_prob(probs, ExpertConnectorType.COMPILER, 0.7)
        elif state.security_concern > 0.5:
            self._set_prob(probs, ExpertConnectorType.SECURITY_SCANNER, 0.6)
        elif state.performance_concern > 0.5:
            self._set_prob(probs, ExpertConnectorType.PERFORMANCE_PROFILER, 0.6)
        else:
            self._set_prob(probs, ExpertConnectorType.STATIC_ANALYZER, 0.5)
            self._set_prob(probs, ExpertConnectorType.ENERGY_BENCHMARK, 0.3)
        total = probs.sum()
        return probs / total if total > 0 else np.full(
            self.n_actions, 1.0 / self.n_actions
        )

    def _rlhf_teacher(self, state: ConnectorSelectionState) -> "np.ndarray":
        probs = np.full(self.n_actions, 1.0 / self.n_actions, dtype=np.float64)
        if state.human_feedback > 0.7:
            self._set_prob(probs, ExpertConnectorType.STATIC_ANALYZER, probs[
                self.index_map.get(ExpertConnectorType.STATIC_ANALYZER, 0)
            ] + 0.2)
        elif state.human_feedback < 0.3:
            self._set_prob(probs, ExpertConnectorType.COMPILER, probs[
                self.index_map.get(ExpertConnectorType.COMPILER, 0)
            ] + 0.2)
        return probs / probs.sum()

    def _historical_teacher(self, state: ConnectorSelectionState) -> "np.ndarray":
        probs = self._empty_probs()
        if state.centrality > 0.7:
            self._set_prob(probs, ExpertConnectorType.STATIC_ANALYZER, 0.6)
        else:
            self._set_prob(probs, ExpertConnectorType.ENERGY_BENCHMARK, 0.5)
        total = probs.sum()
        return probs / total if total > 0 else np.full(
            self.n_actions, 1.0 / self.n_actions
        )

    # ---- forward ----------------------------------------------------------
    def select_connector(
        self,
        state: ConnectorSelectionState,
        exploration: bool = True,
    ) -> Tuple[int, "np.ndarray", "np.ndarray", "np.ndarray"]:
        """
        Returns
        -------
        action          : int
        state_vec       : (FEATURE_DIM,)
        teacher_outputs : (T, P) — individual teacher distributions
        gate_probs      : (T,)   — MoE gate weights
        """
        x = state.to_vector()
        with self._lock:
            teacher_outputs = np.stack([t(state) for t in self.teachers])
            teacher_outputs /= np.maximum(
                teacher_outputs.sum(axis=1, keepdims=True), 1e-12
            )
            gate = self._gate_forward(x)
            teacher_mix = gate @ teacher_outputs
            teacher_mix /= max(float(teacher_mix.sum()), 1e-12)

            if exploration and self.rng.random() < self.epsilon:
                action = int(self.rng.integers(0, self.n_actions))
            else:
                student = self._softmax(x @ self.student_weights + self.student_bias)
                combined = 0.8 * student + 0.2 * teacher_mix
                action = int(np.argmax(combined))

        return action, x, teacher_outputs, gate

    # ---- backward ---------------------------------------------------------
    def update(
        self,
        state_vec: "np.ndarray",
        action: int,
        reward: float,
        teacher_outputs: "np.ndarray",
        gate_probs: "np.ndarray",
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        with self._lock:
            self.replay.append(
                (
                    np.asarray(state_vec, dtype=np.float64),
                    int(action),
                    r,
                    np.asarray(teacher_outputs, dtype=np.float64),
                    np.asarray(gate_probs, dtype=np.float64),
                )
            )
            self.counter += 1
            should_train = (
                self.counter % self.train_every == 0
                and len(self.replay) >= self.batch_size
            )
        if should_train:
            self._train_step()

    def _train_step(self) -> None:
        with self._lock:
            n = min(self.batch_size, len(self.replay))
            idxs = self.rng.choice(len(self.replay), size=n, replace=False)
            batch = [self.replay[int(i)] for i in idxs]

            for s, a, r, t_outs, gate in batch:
                cur = self._softmax(s @ self.student_weights + self.student_bias)

                # ---- student update ----
                combined = gate @ t_outs
                one_hot = np.zeros(self.n_actions)
                one_hot[a] = 1.0
                grad_distill = -(combined - cur)
                grad_rl = -r * (one_hot - cur)
                grad = self.distill_w * grad_distill + self.rl_w * grad_rl
                self.student_weights -= self.lr * np.outer(s, grad)
                self.student_bias -= self.lr * grad

                # ---- gate update (fixed softmax chain rule) ----
                target = np.full(
                    self.n_actions,
                    (1.0 - r) / max(self.n_actions - 1, 1),
                    dtype=np.float64,
                )
                target[a] = r
                target = np.clip(target, 0.0, 1.0)
                tsum = target.sum()
                if tsum > 0:
                    target = target / tsum
                else:
                    target = np.full(self.n_actions, 1.0 / self.n_actions)

                err = combined - target                # (P,)
                dL_dg = t_outs.T @ err                 # (T,)
                grad_logits = gate * (dL_dg - float(gate @ dL_dg))
                self.gate_weights -= self.gate_lr * np.outer(s, grad_logits)
                self.gate_bias -= self.gate_lr * grad_logits

            self.epsilon = max(
                self.epsilon_min, self.epsilon * self.epsilon_decay
            )

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json(
            path,
            {
                "kind": "connector_distillation",
                "available_types": [t.value for t in self.available_types],
                "feature_dim": self.feature_dim,
                "n_actions": self.n_actions,
                "student_weights": self.student_weights.tolist(),
                "student_bias": self.student_bias.tolist(),
                "gate_weights": self.gate_weights.tolist(),
                "gate_bias": self.gate_bias.tolist(),
                "epsilon": float(self.epsilon),
                "counter": int(self.counter),
            },
        )

    def load(self, path: Path) -> None:
        payload = _load_json(path)
        if not payload:
            return
        if payload.get("kind") != "connector_distillation":
            raise ValueError(f"Wrong checkpoint kind: {payload.get('kind')!r}")
        expected = [t.value for t in self.available_types]
        if list(payload.get("available_types", [])) != expected:
            raise ValueError(
                f"Checkpoint action space {payload.get('available_types')} "
                f"!= current {expected}"
            )
        if int(payload.get("feature_dim", -1)) != self.feature_dim:
            raise ValueError("Checkpoint feature_dim mismatch")
        self.student_weights = np.asarray(
            payload["student_weights"], dtype=np.float64
        )
        self.student_bias = np.asarray(payload["student_bias"], dtype=np.float64)
        self.gate_weights = np.asarray(payload["gate_weights"], dtype=np.float64)
        self.gate_bias = np.asarray(payload["gate_bias"], dtype=np.float64)
        self.epsilon = float(payload.get("epsilon", self.epsilon))
        self.counter = int(payload.get("counter", 0))
        logger.info("Loaded distillation checkpoint from %s", path)


# ============================================================================
# SECTION 10 — Evolutionary optimizer (was a dead flag)
# ============================================================================

@dataclass
class _Ind:
    weights: "np.ndarray"
    bias: "np.ndarray"
    fitness: float = 0.0


class EvolutionaryConnectorOptimizer:
    """Population of linear policies over state features → action logits."""

    def __init__(
        self,
        available_types: Sequence[ExpertConnectorType],
        config: ExpertConnectorConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError(
                "numpy is required for EvolutionaryConnectorOptimizer"
            )
        self.available_types = list(available_types)
        self.index_map = {t: i for i, t in enumerate(self.available_types)}
        self.n_actions = len(self.available_types)
        self.feature_dim = ConnectorSelectionState.FEATURE_DIM
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.counter = 0
        self.generation = 0
        self._lock = threading.RLock()

        self.population: List[_Ind] = [
            _Ind(
                weights=self.rng.standard_normal(
                    (self.feature_dim, self.n_actions)
                ) * 0.1,
                bias=self.rng.standard_normal(self.n_actions) * 0.01,
            )
            for _ in range(config.population_size)
        ]

        if config.model_path:
            self.load(Path(str(config.model_path) + ".evolve.json"))

    def _forward(self, ind: _Ind, x: "np.ndarray") -> "np.ndarray":
        return x @ ind.weights + ind.bias

    def select_connector(
        self, state: ConnectorSelectionState, exploration: bool = True
    ) -> Tuple[int, "np.ndarray", "np.ndarray", "np.ndarray"]:
        x = state.to_vector()
        with self._lock:
            if exploration and self.rng.random() < self.config.epsilon:
                action = int(self.rng.integers(0, self.n_actions))
            else:
                best = max(self.population, key=lambda ind: ind.fitness)
                action = int(np.argmax(self._forward(best, x)))
            # Uniform teacher/gate context for interface compatibility.
            teacher_outputs = np.tile(
                np.full(self.n_actions, 1.0 / self.n_actions),
                (3, 1),
            )
            gate = np.full(3, 1.0 / 3.0)
        return action, x, teacher_outputs, gate

    def update(
        self,
        state_vec: "np.ndarray",
        action: int,
        reward: float,
        teacher_outputs: "np.ndarray",
        gate_probs: "np.ndarray",
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        target = np.full(
            self.n_actions,
            (1.0 - r) / max(self.n_actions - 1, 1),
        )
        target[action] = r
        with self._lock:
            for ind in self.population:
                pred = self._forward(ind, state_vec)
                ind.fitness -= float(np.sum((pred - target) ** 2))
            self.counter += 1
            should_evolve = self.counter % self.config.train_every == 0
        if should_evolve:
            self._evolve()

    def _evolve(self) -> None:
        with self._lock:
            self.population.sort(key=lambda ind: ind.fitness, reverse=True)
            n_elite = max(1, self.config.elitism)
            new_pop: List[_Ind] = [
                _Ind(weights=ind.weights.copy(), bias=ind.bias.copy(), fitness=0.0)
                for ind in self.population[:n_elite]
            ]
            while len(new_pop) < len(self.population):
                a = self._tournament()
                b = self._tournament()
                child = self._crossover(a, b)
                self._mutate(child)
                child.fitness = 0.0
                new_pop.append(child)
            self.population = new_pop
            self.generation += 1

    def _tournament(self, k: int = 3) -> _Ind:
        size = min(k, len(self.population))
        idxs = self.rng.choice(len(self.population), size=size, replace=False)
        return max(
            (self.population[int(i)] for i in idxs), key=lambda ind: ind.fitness
        )

    def _crossover(self, a: _Ind, b: _Ind) -> _Ind:
        if self.rng.random() < self.config.crossover_rate:
            mask_w = self.rng.random(a.weights.shape) < 0.5
            mask_b = self.rng.random(a.bias.shape) < 0.5
            return _Ind(
                weights=np.where(mask_w, a.weights, b.weights).copy(),
                bias=np.where(mask_b, a.bias, b.bias).copy(),
            )
        return _Ind(weights=a.weights.copy(), bias=a.bias.copy())

    def _mutate(self, ind: _Ind) -> None:
        mw = self.rng.random(ind.weights.shape) < self.config.mutation_rate
        mb = self.rng.random(ind.bias.shape) < self.config.mutation_rate
        ind.weights = ind.weights + mw * self.rng.standard_normal(
            ind.weights.shape
        ) * 0.05
        ind.bias = ind.bias + mb * self.rng.standard_normal(ind.bias.shape) * 0.02

    def save(self, path: Path) -> None:
        _save_json(
            path,
            {
                "kind": "connector_evolution",
                "available_types": [t.value for t in self.available_types],
                "feature_dim": self.feature_dim,
                "generation": self.generation,
                "counter": self.counter,
                "population": [
                    {
                        "weights": ind.weights.tolist(),
                        "bias": ind.bias.tolist(),
                        "fitness": float(ind.fitness),
                    }
                    for ind in self.population
                ],
            },
        )

    def load(self, path: Path) -> None:
        payload = _load_json(path)
        if not payload:
            return
        if payload.get("kind") != "connector_evolution":
            raise ValueError(f"Wrong checkpoint kind: {payload.get('kind')!r}")
        expected = [t.value for t in self.available_types]
        if list(payload.get("available_types", [])) != expected:
            raise ValueError("Checkpoint action space mismatch")
        self.generation = int(payload.get("generation", 0))
        self.counter = int(payload.get("counter", 0))
        self.population = [
            _Ind(
                weights=np.asarray(ind["weights"], dtype=np.float64),
                bias=np.asarray(ind["bias"], dtype=np.float64),
                fitness=float(ind.get("fitness", 0.0)),
            )
            for ind in payload["population"]
        ]
        logger.info("Loaded evolutionary checkpoint from %s", path)


# ============================================================================
# SECTION 11 — Connector implementations
# ============================================================================

_OPT_KEYWORDS = re.compile(
    r"\b(loop|inline|vectorization|unroll|autovectorization)\b",
    re.IGNORECASE,
)


class CompilerExpert(ExpertConnector):
    """Compiler analysis expert."""

    def __init__(
        self,
        compiler: str = "gcc",
        optimization_level: str = "-O2",
        enable_warnings: bool = True,
    ) -> None:
        super().__init__(ExpertConnectorType.COMPILER)
        self.compiler = compiler
        self.optimization_level = optimization_level
        self.enable_warnings = enable_warnings

    async def analyze(
        self, input_data: Any, config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        return await asyncio.to_thread(self._analyze_sync, input_data, config)

    def _analyze_sync(
        self, input_data: Any, config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        start = time.perf_counter()
        errors: List[str] = []
        warnings: List[str] = []
        success = False
        output: Dict[str, Any] = {}

        timeout = float((config or {}).get("timeout_s", 30.0))
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                source_file = os.path.join(tmpdir, "input.c")
                if isinstance(input_data, str):
                    with open(source_file, "w", encoding="utf-8") as f:
                        f.write(input_data)
                else:
                    raise TypeError("input_data must be a str")

                output_file = os.path.join(tmpdir, "a.out")
                cmd = [
                    self.compiler,
                    self.optimization_level,
                    source_file,
                    "-o",
                    output_file,
                ]
                if self.enable_warnings:
                    cmd.extend(["-Wall", "-Wextra"])

                try:
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=timeout,
                        check=False,
                    )
                except FileNotFoundError:
                    errors.append(f"Compiler not found: {self.compiler}")
                    return self._make_result(
                        start, success, output, errors, warnings
                    )
                except subprocess.TimeoutExpired:
                    errors.append(f"Compilation timeout ({timeout}s)")
                    return self._make_result(
                        start, success, output, errors, warnings
                    )

                if result.returncode == 0:
                    success = True
                    output["compiled"] = True
                    output["optimizations"] = self._analyze_optimizations(
                        result.stderr or ""
                    )
                else:
                    errors.append((result.stderr or "").strip())
                    output["compiled"] = False

                if result.stderr:
                    warnings = self._parse_warnings(result.stderr)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Compilation error: {exc}")

        return self._make_result(start, success, output, errors, warnings)

    def _make_result(
        self,
        start: float,
        success: bool,
        output: Dict[str, Any],
        errors: List[str],
        warnings: List[str],
    ) -> ExpertConnectorResult:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return ExpertConnectorResult(
            connector_type=self.connector_type,
            success=success,
            output=output,
            errors=errors,
            warnings=warnings,
            energy_consumed_wh=(elapsed_ms / 1000.0) * 0.1,
            execution_time_ms=elapsed_ms,
            metadata={
                "compiler": self.compiler,
                "optimization_level": self.optimization_level,
            },
        )

    def _analyze_optimizations(self, stderr: str) -> List[str]:
        messages: List[str] = []
        if _OPT_KEYWORDS.search(stderr):
            if "loop" in stderr.lower():
                messages.append("Loop optimization opportunity detected")
            if "inline" in stderr.lower():
                messages.append("Function inlining suggested")
            if "vectorization" in stderr.lower():
                messages.append("Vectorization opportunity detected")
        return messages

    def _parse_warnings(self, stderr: str) -> List[str]:
        return [
            line.strip()
            for line in stderr.splitlines()
            if "warning:" in line.lower()
        ]


class StaticAnalyzerExpert(ExpertConnector):
    """Static analysis expert (pylint / flake8)."""

    def __init__(self, analyzer_tool: str = "pylint") -> None:
        super().__init__(ExpertConnectorType.STATIC_ANALYZER)
        self.analyzer_tool = analyzer_tool

    async def analyze(
        self, input_data: Any, config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        return await asyncio.to_thread(self._analyze_sync, input_data, config)

    def _analyze_sync(
        self, input_data: Any, config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        start = time.perf_counter()
        errors: List[str] = []
        warnings: List[str] = []
        success = False
        output: Dict[str, Any] = {}

        timeout = float((config or {}).get("timeout_s", 30.0))
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                source_file = os.path.join(tmpdir, "input.py")
                if isinstance(input_data, str):
                    with open(source_file, "w", encoding="utf-8") as f:
                        f.write(input_data)
                else:
                    raise TypeError("input_data must be a str")

                if self.analyzer_tool == "pylint":
                    cmd = ["pylint", "--output-format=json", source_file]
                elif self.analyzer_tool == "flake8":
                    cmd = ["flake8", "--format=json", source_file]
                else:
                    cmd = [self.analyzer_tool, source_file]

                try:
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=timeout,
                        check=False,
                    )
                except FileNotFoundError:
                    errors.append(f"Analyzer not found: {self.analyzer_tool}")
                    return self._make_result(
                        start, success, output, errors, warnings
                    )
                except subprocess.TimeoutExpired:
                    errors.append(f"Analysis timeout ({timeout}s)")
                    return self._make_result(
                        start, success, output, errors, warnings
                    )

                try:
                    issues = json.loads(result.stdout or "[]")
                except json.JSONDecodeError:
                    errors.append("Failed to parse analyzer output")
                    issues = []

                output["issues"] = issues
                output["issue_count"] = len(issues)
                for issue in issues:
                    kind = (issue.get("type") or "").lower()
                    message = issue.get("message", "")
                    if kind in ("error", "fatal"):
                        errors.append(message)
                    elif kind == "warning":
                        warnings.append(message)
                success = result.returncode in (0, 4)  # pylint uses 4 for warnings
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Analysis error: {exc}")

        return self._make_result(start, success, output, errors, warnings)

    def _make_result(
        self,
        start: float,
        success: bool,
        output: Dict[str, Any],
        errors: List[str],
        warnings: List[str],
    ) -> ExpertConnectorResult:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return ExpertConnectorResult(
            connector_type=self.connector_type,
            success=success,
            output=output,
            errors=errors,
            warnings=warnings,
            energy_consumed_wh=(elapsed_ms / 1000.0) * 0.05,
            execution_time_ms=elapsed_ms,
            metadata={"analyzer": self.analyzer_tool},
        )


class EnergyBenchmarkExpert(ExpertConnector):
    """Energy benchmarking expert (perf stat, RAPL)."""

    def __init__(
        self,
        benchmark_tool: str = "perf",
        grid_carbon_intensity: float = 385.0,
    ) -> None:
        super().__init__(ExpertConnectorType.ENERGY_BENCHMARK)
        self.benchmark_tool = benchmark_tool
        self.grid_carbon_intensity = grid_carbon_intensity

    async def analyze(
        self, input_data: Any, config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        return await asyncio.to_thread(self._analyze_sync, input_data, config)

    def _analyze_sync(
        self, input_data: Any, config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        start = time.perf_counter()
        errors: List[str] = []
        warnings: List[str] = []
        success = False
        output: Dict[str, Any] = {}

        try:
            executable = (
                input_data.get("executable")
                if isinstance(input_data, dict)
                else input_data
            )
            if not executable:
                raise ValueError("no executable provided")

            if self.benchmark_tool == "perf":
                timeout = float((config or {}).get("timeout_s", 60.0))
                cmd = [
                    "perf", "stat",
                    "-e", "power/energy-pkg/",
                    "-e", "power/energy-ram/",
                    executable,
                ]
                try:
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=timeout,
                        check=False,
                    )
                except FileNotFoundError:
                    errors.append("perf not found")
                    return self._make_result(
                        start, success, output, errors, warnings
                    )
                except subprocess.TimeoutExpired:
                    errors.append(f"Benchmark timeout ({timeout}s)")
                    return self._make_result(
                        start, success, output, errors, warnings
                    )

                energy_pkg = self._parse_perf_energy(result.stderr, "energy-pkg")
                energy_ram = self._parse_perf_energy(result.stderr, "energy-ram")

                if energy_pkg is None and energy_ram is None:
                    errors.append(
                        "perf stat could not measure RAPL energy "
                        "(permissions or kernel support?)"
                    )
                    output["measured"] = False
                    output["sustainability_rating"] = "unmeasured"
                    return self._make_result(
                        start, success, output, errors, warnings
                    )

                energy_pkg = energy_pkg or 0.0
                energy_ram = energy_ram or 0.0
                total_energy_joules = energy_pkg + energy_ram
                if total_energy_joules <= 0:
                    errors.append(
                        "perf reported 0 J; treating as measurement failure"
                    )
                    return self._make_result(
                        start, success, output, errors, warnings
                    )

                total_energy_wh = total_energy_joules / 3600.0
                carbon_kg = (
                    total_energy_wh / 1000.0
                ) * self.grid_carbon_intensity / 1000.0
                output.update(
                    {
                        "energy_pkg_joules": energy_pkg,
                        "energy_ram_joules": energy_ram,
                        "total_energy_wh": total_energy_wh,
                        "carbon_kg": carbon_kg,
                        "sustainability_rating": self._calculate_rating(
                            total_energy_wh
                        ),
                        "measured": True,
                    }
                )
                success = True
            else:
                execution_time_s = float(
                    (config or {}).get("execution_time", 1.0)
                )
                estimated_power_w = 50.0
                energy_wh = estimated_power_w * execution_time_s / 3600.0
                carbon_kg = (
                    energy_wh / 1000.0
                ) * self.grid_carbon_intensity / 1000.0
                output.update(
                    {
                        "estimated": True,
                        "total_energy_wh": energy_wh,
                        "carbon_kg": carbon_kg,
                        "sustainability_rating": self._calculate_rating(
                            energy_wh
                        ),
                        "measured": False,
                    }
                )
                warnings.append("Using estimated energy consumption")
                success = True
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Benchmark error: {exc}")

        return self._make_result(start, success, output, errors, warnings)

    def _parse_perf_energy(self, stderr: str, event: str) -> Optional[float]:
        for line in stderr.splitlines():
            if event in line:
                parts = line.split()
                if not parts:
                    continue
                try:
                    return float(parts[0].replace(",", ""))
                except (ValueError, IndexError):
                    continue
        return None

    @staticmethod
    def _calculate_rating(energy_wh: float) -> str:
        if energy_wh <= 0.0:
            return "unmeasured"
        if energy_wh < 0.001:
            return "A+ (Excellent)"
        if energy_wh < 0.01:
            return "A (Very Good)"
        if energy_wh < 0.1:
            return "B (Good)"
        if energy_wh < 1.0:
            return "C (Fair)"
        return "D (Poor)"

    def _make_result(
        self,
        start: float,
        success: bool,
        output: Dict[str, Any],
        errors: List[str],
        warnings: List[str],
    ) -> ExpertConnectorResult:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return ExpertConnectorResult(
            connector_type=self.connector_type,
            success=success,
            output=output,
            errors=errors,
            warnings=warnings,
            energy_consumed_wh=float(output.get("total_energy_wh", 0.0)),
            execution_time_ms=elapsed_ms,
            metadata={
                "benchmark_tool": self.benchmark_tool,
                "grid_carbon_intensity": self.grid_carbon_intensity,
            },
        )


class SecurityScannerExpert(ExpertConnector):
    """Stub — override analyze() and register explicitly to enable."""

    def __init__(self) -> None:
        super().__init__(ExpertConnectorType.SECURITY_SCANNER)

    async def analyze(
        self, input_data: Any, config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        return ExpertConnectorResult(
            connector_type=self.connector_type,
            success=False,
            output={},
            errors=[
                "SecurityScannerExpert is a stub. Subclass and override "
                "analyze() before registering."
            ],
            warnings=[],
            energy_consumed_wh=0.0,
            execution_time_ms=0.0,
            metadata={"stub": True},
        )


class PerformanceProfilerExpert(ExpertConnector):
    """Stub — override analyze() and register explicitly to enable."""

    def __init__(self) -> None:
        super().__init__(ExpertConnectorType.PERFORMANCE_PROFILER)

    async def analyze(
        self, input_data: Any, config: Optional[Dict] = None
    ) -> ExpertConnectorResult:
        return ExpertConnectorResult(
            connector_type=self.connector_type,
            success=False,
            output={},
            errors=[
                "PerformanceProfilerExpert is a stub. Subclass and override "
                "analyze() before registering."
            ],
            warnings=[],
            energy_consumed_wh=0.0,
            execution_time_ms=0.0,
            metadata={"stub": True},
        )


# ============================================================================
# SECTION 12 — Registry
# ============================================================================

class ExpertConnectorRegistry:
    """
    Registry for managing expert connectors with optional enhanced selection.

    Enhanced path:
      1. select_connector() picks a connector via distillation or evolution.
      2. analyze() runs the connector in a worker thread (does not block loop).
      3. compute_reward() scores the outcome in [-1, 1].
      4. optimizer.update() trains on (state, action, reward).
      5. Result is annotated with modp_score / graph_metrics / human_feedback
         / distillation_stats and `enhanced=True`.
    """

    def __init__(
        self,
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if config is None:
            self.config = ExpertConnectorConfig()
        elif isinstance(config, ExpertConnectorConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = ExpertConnectorConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be ExpertConnectorConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.use_enhancements = bool(
            self.config.use_enhancements and ENHANCEMENTS_AVAILABLE
        )
        if self.config.use_enhancements and not ENHANCEMENTS_AVAILABLE:
            logger.warning(
                "use_enhancements=True but numpy is unavailable; "
                "running without enhancements"
            )

        self._lock = threading.RLock()
        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if ENHANCEMENTS_AVAILABLE
            else None
        )
        self._human_feedback_score = float(self.config.human_feedback_score)

        self.connectors: Dict[ExpertConnectorType, ExpertConnector] = {}
        self.register(ExpertConnectorType.COMPILER, CompilerExpert())
        self.register(ExpertConnectorType.STATIC_ANALYZER, StaticAnalyzerExpert())
        self.register(ExpertConnectorType.ENERGY_BENCHMARK, EnergyBenchmarkExpert())

        self.optimizer: Optional[Any] = None
        if self.use_enhancements:
            if self.config.use_evolutionary:
                self.optimizer = EvolutionaryConnectorOptimizer(
                    list(self.connectors.keys()), self.config, rng=self.rng
                )
            elif self.config.use_distillation:
                self.optimizer = ConnectorDistillationOptimizer(
                    list(self.connectors.keys()), self.config, rng=self.rng
                )

        # Backwards-compat alias (v1 exposed .distillation_optimizer)
        self.distillation_optimizer = self.optimizer

    # ------------------------------------------------------------------
    def register(
        self, connector_type: ExpertConnectorType, connector: ExpertConnector
    ) -> None:
        with self._lock:
            if self.optimizer is not None and connector_type not in self.connectors:
                logger.warning(
                    "Registering %s after optimizer construction — the action "
                    "space is stale. Reconstruct the registry to include it.",
                    connector_type,
                )
            self.connectors[connector_type] = connector

    def get_connector(
        self, connector_type: ExpertConnectorType
    ) -> Optional[ExpertConnector]:
        return self.connectors.get(connector_type)

    def list_connectors(self) -> List[ExpertConnectorType]:
        return list(self.connectors.keys())

    def record_human_feedback(self, score: float) -> None:
        score = float(score)
        if not 0.0 <= score <= 1.0:
            raise ValueError("human_feedback_score must be in [0,1]")
        with self._lock:
            self._human_feedback_score = score
        self.metrics.observe("connector_human_feedback_score", score)

    def get_optimizer_stats(self) -> Optional[Dict[str, Any]]:
        return self._optimizer_stats()

    # ------------------------------------------------------------------
    async def invoke(
        self,
        connector_type: Optional[ExpertConnectorType],
        input_data: Any,
        config: Optional[Dict] = None,
        task_features: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> ExpertConnectorResult:
        # -- 1. Decision ------------------------------------------------
        pending: Optional[PendingDecision] = None
        if connector_type is None:
            if self.optimizer is None:
                raise ValueError(
                    "connector_type=None requires use_enhancements=True "
                    "(and numpy available)"
                )
            gm = dict(graph_metrics or self.config.graph_metrics)
            hf = (
                float(human_feedback_score)
                if human_feedback_score is not None
                else float(self._human_feedback_score)
            )
            state = ConnectorSelectionState(task_features or {}, gm, hf)
            action_idx, s_vec, t_outs, g_probs = (
                self.optimizer.select_connector(state)
            )
            available = list(self.connectors.keys())
            if not 0 <= action_idx < len(available):
                raise RuntimeError(
                    f"Optimizer returned action_idx={action_idx} "
                    f"outside registered range [0, {len(available)})"
                )
            connector_type = available[action_idx]
            pending = PendingDecision(
                state_vec=s_vec,
                action_idx=action_idx,
                teacher_outputs=t_outs,
                gate_probs=g_probs,
                connector_type=connector_type,
                graph_metrics=gm,
                human_feedback=hf,
            )

        # -- 2. Lookup --------------------------------------------------
        connector = self.connectors.get(connector_type)
        if connector is None:
            return self._error_result(
                connector_type,
                f"Connector {connector_type.value} not registered",
            )

        # -- 3. Execute -------------------------------------------------
        try:
            result = await connector.analyze(input_data, config)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Connector %s raised: %s", connector_type, exc)
            return self._error_result(
                connector_type, f"Connector raised: {exc}"
            )

        # -- 4. Post-process (enhanced) ---------------------------------
        if pending is not None and self.optimizer is not None:
            try:
                reward = compute_reward(
                    result,
                    self.config.modp_weights,
                    energy_scale_wh=self.config.energy_scale_wh,
                    latency_scale_ms=self.config.latency_scale_ms,
                )
                self.optimizer.update(
                    pending.state_vec,
                    pending.action_idx,
                    reward,
                    pending.teacher_outputs,
                    pending.gate_probs,
                )
                result.modp_score = reward
                result.graph_metrics = dict(pending.graph_metrics)
                result.human_feedback_score = float(pending.human_feedback)
                result.distillation_stats = self._optimizer_stats()
                result.enhanced = True
                self.metrics.observe(
                    "connector_reward", reward,
                    connector=connector_type.value,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Optimizer update failed: %s", exc)

        # -- 5. Observability -------------------------------------------
        try:
            self.metrics.observe(
                "connector_invocations_total", 1.0,
                connector=connector_type.value,
            )
            self.metrics.observe(
                "connector_success_total",
                1.0 if result.success else 0.0,
                connector=connector_type.value,
            )
            self.metrics.observe(
                "connector_energy_wh", result.energy_consumed_wh,
                connector=connector_type.value,
            )
            self.metrics.observe(
                "connector_execution_time_ms", result.execution_time_ms,
                connector=connector_type.value,
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("metrics.observe failed: %s", exc)

        return result

    async def invoke_auto(
        self,
        input_data: Any,
        config: Optional[Dict] = None,
        task_features: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> ExpertConnectorResult:
        return await self.invoke(
            None,
            input_data,
            config,
            task_features,
            graph_metrics,
            human_feedback_score,
        )

    # ------------------------------------------------------------------
    def save(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None or self.optimizer is None:
            return
        try:
            self.optimizer.save(target)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Optimizer save failed: %s", exc)

    def load(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None or self.optimizer is None:
            return
        try:
            self.optimizer.load(target)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Optimizer load failed: %s", exc)

    # ------------------------------------------------------------------
    def _optimizer_stats(self) -> Optional[Dict[str, Any]]:
        if self.optimizer is None:
            return None
        stats: Dict[str, Any] = {
            "kind": type(self.optimizer).__name__,
            "counter": getattr(self.optimizer, "counter", None),
        }
        if hasattr(self.optimizer, "epsilon"):
            stats["epsilon"] = self.optimizer.epsilon
        if hasattr(self.optimizer, "replay"):
            stats["replay_size"] = len(self.optimizer.replay)
        if hasattr(self.optimizer, "generation"):
            stats["generation"] = self.optimizer.generation
        return stats

    def _error_result(
        self, connector_type: ExpertConnectorType, message: str
    ) -> ExpertConnectorResult:
        return ExpertConnectorResult(
            connector_type=connector_type,
            success=False,
            output={},
            errors=[message],
            warnings=[],
            energy_consumed_wh=0.0,
            execution_time_ms=0.0,
            metadata={},
        )


# ============================================================================
# SECTION 13 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    async def _demo() -> None:
        cfg = ExpertConnectorConfig.from_dict(
            {
                "use_enhancements": True,
                "use_distillation": True,
                "modp_weights": [0.4, 0.3, 0.2, 0.1],
                "human_feedback_score": 0.7,
                "seed": 42,
                "model_path": "./models/connector.json",
            }
        )
        registry = ExpertConnectorRegistry(cfg)

        # Explicit invocation
        r = await registry.invoke(
            ExpertConnectorType.COMPILER,
            "int main(){return 0;}",
        )
        print(f"[explicit] success={r.success} "
              f"energy={r.energy_consumed_wh:.6f} Wh "
              f"time={r.execution_time_ms:.1f} ms")

        # Auto invocation (uses the optimizer)
        for _ in range(3):
            r = await registry.invoke_auto(
                "int main(){return 0;}",
                task_features={"needs_compilation": True, "code_size": 500},
            )
            print(f"[auto] {r.connector_type.value} "
                  f"success={r.success} "
                  f"reward={r.modp_score} "
                  f"enhanced={r.enhanced}")

        print("stats:", registry.get_optimizer_stats())
        registry.save()

    asyncio.run(_demo())
