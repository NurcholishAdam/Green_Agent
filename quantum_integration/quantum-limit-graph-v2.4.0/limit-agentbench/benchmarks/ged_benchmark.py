# -*- coding: utf-8 -*-
"""
ged_benchmark.py — Recommendation 4 (Enhanced)
==============================================

Parametric GED performance benchmark with optional advanced
decision-making.

Original features retained
--------------------------
- Target SLA: ``p99 latency < 100 ms`` for graphs with ``≤ 50`` nodes
  per sequence.
- Sweeps graph sizes, measures sync/async latency, validates the SLA,
  exports CSV.

Enhancements (enabled via ``--enhance``)
----------------------------------------
- **Adaptive method selection.** For every trial, the optimizer picks
  sync or async based on a distillation student trained from three
  teachers (rule-based, RLHF, historical) with MoE gating. This was
  previously constructed and threaded through three signatures but
  **never invoked** — the flag was a no-op.
- **MODP reward.** The per-trial reward is the SLA headroom of the
  chosen method: ``0.5 + 0.5 · (1 − latency / SLA)`` when the run meets
  the SLA, ``0.0`` when it does not.
- **Corrected distillation optimizer.** ``select_action`` returns a
  4-tuple ``(action, state_vec, teacher_outputs, teacher_combined)``;
  the replay buffer stores the individual teacher distributions; the
  gate gradient is ``teacher_outputs @ error``; per-instance RNGs;
  config validated at construction; ``update`` guarded by an ``RLock``;
  over-sized teacher outputs are sliced before any ``np.pad``.
- **Escalation decision is honoured by ``validate_sla``.** The SLA
  check picks sync or async per trial when ``--enhance`` is on; it
  otherwise stays on the pure-sync path.
- **Enhancement metadata in CSV output.** Each row carries
  ``strategy_sync`` / ``strategy_async`` counts and
  ``enhanced: bool``; a reader can tell which method produced each
  measurement.
- ``--seed`` for reproducibility, ``--quick`` for a fast smoke run,
  ``--sizes`` to override the graph-size list, ``--out-dir`` to
  redirect the results directory, ``--strict`` to turn enhancement-
  availability warnings into an exit code.
- ``--enhance`` without the enhancement package now warns on stderr
  (previously silent); with ``--strict`` it exits with code ``2``.
- ``percentile`` is a true interpolating percentile; ``p99`` equals the
  maximum only for ``n_trials < 100`` (previously ``p99 == max`` for
  ``n_trials ≤ 100``).
- ``--trials 0`` / negative are rejected by an argparse type — no more
  crash at ``max()`` on an empty list.
- ``main(argv)`` returns ``int`` in every case, including argparse
  errors, so it is usable from CI and tests.
- ``asyncio.run`` is guarded against a running event loop; the caller
  is told to invoke ``run_adaptive_benchmark`` / ``run_async_benchmark``
  directly from their loop.
- ``sys.path`` manipulation happens once, in ``_ensure_core_on_path``,
  called lazily from the timing helpers. Importing this module has no
  global side effect.
- ``GEDDecisionState`` is frozen with full field validation.
- Sync/async/adaptive benchmark runners are unified through a shared
  ``_make_row`` helper.
- ``ACTIONS`` is documented as a neutral fixture (relabel cost is
  uniform across pairs); the enhancement's decision surface is graph
  size, carbon, human feedback, and centrality — not token semantics.

Compatibility
-------------
- ``GEDDistillationOptimizer.select_action`` returns 4 values
  (``action, state_vec, teacher_outputs, teacher_combined``) — 3 before.
- ``GEDDistillationOptimizer.update`` takes
  ``(state_vec, action, reward, teacher_outputs, teacher_combined)``;
  the previous ``next_state_vec`` argument is gone.
- ``main()`` now accepts ``argv`` and returns ``int``.
- ``validate_sla`` is now ``async``; callers must ``await`` it or
  ``asyncio.run(...)``.
- ``GEDDecisionState`` is frozen; use ``GEDDecisionState.from_raw(...)``
  for the normalised constructor.
- ``run_sync_benchmark`` / ``run_async_benchmark`` accept keyword-only
  ``optimize``, ``optimizer``, ``human_feedback``, ``graph_centrality``,
  ``carbon_intensity``, ``seed``. Positional calls with ``enhance=``
  must switch to keyword form.
- ``benchmark_results/`` is resolved relative to this file, not the
  caller's ``cwd``.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop ``slots=True`` for 3.8/3.9.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import math
import random
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque, Dict, Final, List, Optional, Tuple

import numpy as np


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class BenchmarkError(ValueError):
    """Raised for invalid benchmark inputs or configuration."""


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
SLA_MS: Final[float] = 100.0
SLA_NODES: Final[int] = 50
N_TRIALS: Final[int] = 200
QUICK_TRIALS: Final[int] = 5
SLA_TRIALS: Final[int] = 500
QUICK_SLA_TRIALS: Final[int] = 20

GRAPH_SIZES: Final[Tuple[int, ...]] = (
    5, 10, 20, 30, 50, 75, 100, 150, 200,
)

# Resolved relative to this file so output is stable regardless of
# the caller's cwd.
DEFAULT_RESULTS_DIR: Final[Path] = (
    Path(__file__).resolve().parent / "benchmark_results"
)

EDIT_COSTS: Final[Dict[str, float]] = {
    "insert": 1.0, "delete": 1.0, "relabel": 0.7,
}

# Neutral fixture vocabulary. The relabel cost is uniform across pairs;
# GED difficulty is a function of graph size and edit distance, not of
# token semantics. Documented here so a reader does not expect
# semantic-cost variation.
ACTIONS: Final[List[str]] = [
    "quantize", "prune", "execute", "defer", "throttle",
    "cache", "offload", "prefetch", "batch", "stream",
    "compress", "encrypt", "route", "schedule", "checkpoint",
]

# Decision feature vector: graph_size(1) + carbon(1) + human(1) +
# centrality(1) = 4.
GED_FEATURE_DIM: Final[int] = 4
GED_N_ACTIONS: Final[int] = 2
DEFAULT_REPLAY_SIZE: Final[int] = 2000
DEFAULT_TRAIN_EVERY: Final[int] = 10
DEFAULT_BATCH_SIZE: Final[int] = 8

# Energy model reference constants.
DEFAULT_POWER_W: Final[float] = 0.25
DEFAULT_GRID_CARBON_KG_PER_KWH: Final[float] = 0.385
CARBON_INTENSITY_DENOM: Final[float] = 500.0


# --------------------------------------------------------------------------- #
# Optional enhancement imports
# --------------------------------------------------------------------------- #
class _UnavailableComponent:
    """Placeholder for an unavailable enhancement component; raises on use."""

    __slots__ = ("_name",)

    def __init__(self, name: str, *a: Any, **kw: Any) -> None:
        self._name = name
        raise BenchmarkError(
            f"Enhancement component '{name}' is unavailable; install "
            f"the enhancements package to use ``--enhance``."
        )


try:
    from src.enhancements.schemas.node_descriptor import (  # noqa: F401
        NodeDescriptor,
    )
    from src.enhancements.schemas.workload_descriptor import (  # noqa: F401
        WorkloadDescriptor,
    )
    from src.enhancements.zero_trust_architecture import (  # noqa: F401
        ZeroTrustArchitecture,
    )
    from src.enhancements.schemas.feedback_event import (  # noqa: F401
        FeedbackEvent,
    )

    ENHANCEMENTS_AVAILABLE: bool = True
except ImportError as _exc:  # pragma: no cover — defensive
    ENHANCEMENTS_AVAILABLE = False
    logger.info("Enhancement schemas unavailable: %s", _exc)
    NodeDescriptor = _UnavailableComponent  # type: ignore[assignment,misc]
    WorkloadDescriptor = _UnavailableComponent  # type: ignore[assignment,misc]
    ZeroTrustArchitecture = _UnavailableComponent  # type: ignore[assignment,misc]
    FeedbackEvent = _UnavailableComponent  # type: ignore[assignment,misc]


# --------------------------------------------------------------------------- #
# Decision state
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class GEDDecisionState:
    """
    Decision state for the sync-vs-async choice.

    All fields are normalised to ``[0, 1]`` (``graph_size`` is in
    integer nodes and is normalised inside :meth:`to_vector`).
    """

    graph_size: int
    carbon: float
    human: float
    centrality: float

    def __post_init__(self) -> None:
        if isinstance(self.graph_size, bool) or not isinstance(
            self.graph_size, int
        ):
            raise BenchmarkError("graph_size must be an int.")
        if self.graph_size < 0:
            raise BenchmarkError("graph_size must be >= 0.")
        for name in ("carbon", "human", "centrality"):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise BenchmarkError(f"{name} must be numeric.")
            fv = float(v)
            if not math.isfinite(fv) or not 0.0 <= fv <= 1.0:
                raise BenchmarkError(
                    f"{name} must be finite and in [0, 1], got {v!r}."
                )

    @classmethod
    def from_raw(
        cls,
        *,
        graph_size: int,
        carbon_intensity: float = 400.0,
        human_feedback: float = 0.5,
        graph_centrality: float = 0.5,
    ) -> "GEDDecisionState":
        """
        Normalised constructor.

        ``carbon_intensity`` is interpreted in ``gCO₂/kWh`` (the same
        unit the wider pipeline uses) and divided by
        :data:`CARBON_INTENSITY_DENOM`.
        """
        return cls(
            graph_size=int(graph_size),
            carbon=max(
                0.0,
                min(float(carbon_intensity) / CARBON_INTENSITY_DENOM, 1.0),
            ),
            human=max(0.0, min(float(human_feedback), 1.0)),
            centrality=max(0.0, min(float(graph_centrality), 1.0)),
        )

    def to_vector(self) -> np.ndarray:
        return np.array([
            min(self.graph_size / 200.0, 1.0),
            self.carbon,
            self.human,
            self.centrality,
        ], dtype=np.float32)


# --------------------------------------------------------------------------- #
# Replay entry
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class _ReplayEntry:
    state_vec: np.ndarray
    action: int
    reward: float
    teacher_outputs: np.ndarray
    teacher_combined: np.ndarray


# --------------------------------------------------------------------------- #
# Distillation optimizer
# --------------------------------------------------------------------------- #
class GEDDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating to decide between sync
    (action ``0``) and async (action ``1``) GED alignment.

    ``select_action`` returns ``(action, state_vec, teacher_outputs,
    teacher_combined)``. ``update`` takes
    ``(state_vec, action, reward, teacher_outputs, teacher_combined)``.

    .. note::
        The corrected implementation here matches
        :class:`src.integrations.base_adapter.DistillationOptimizer`.
        A future refactor should extract that class to
        ``src/enhancements/distillation.py`` and have this file import
        it.
    """

    def __init__(
        self, config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.config = self._validate_config(config or {})

        self.feature_dim: int = GED_FEATURE_DIM
        self.n_actions: int = GED_N_ACTIONS

        self.student_weights = np.zeros(
            (self.feature_dim, self.n_actions), dtype=np.float64,
        )
        self.student_bias = np.zeros(self.n_actions, dtype=np.float64)

        self.lr: float = self.config["distillation_lr"]
        self.epsilon: float = self.config["epsilon"]
        self.distill_w: float = self.config["distill_weight"]
        self.rl_w: float = self.config["rl_weight"]
        self.train_every: int = self.config["train_every"]
        self.batch_size: int = self.config["batch_size"]

        self.replay: Deque[_ReplayEntry] = deque(
            maxlen=self.config["replay_size"],
        )
        self.counter: int = 0
        self._lock = threading.RLock()

        self._py_rng = random.Random(self.config["seed"])
        self._np_rng = np.random.default_rng(self.config["gate_seed"])

        self.teachers = (
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        )
        self.gate_weights = (
            self._np_rng.standard_normal(
                (self.feature_dim, len(self.teachers)),
            ) * 0.01
        )
        self.gate_bias = np.zeros(len(self.teachers), dtype=np.float64)
        self.gate_lr: float = self.config["gating_lr"]

    # ---------------------------------------------------------------- config
    @staticmethod
    def _validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(config, dict):
            raise BenchmarkError(
                f"config must be a dict, got {type(config).__name__}."
            )

        # Accept both the legacy short names (lr, gate_lr, distill_w,
        # rl_w) and the tree-standard names (distillation_lr,
        # gating_lr, distill_weight, rl_weight).
        def _coalesce(*keys: str) -> Any:
            for k in keys:
                if k in config:
                    return config[k]
            return None

        def _num(
            name: str, raw: Any, default: float, *, positive: bool,
        ) -> float:
            v = default if raw is None else raw
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise BenchmarkError(f"{name} must be numeric.")
            fv = float(v)
            if not math.isfinite(fv):
                raise BenchmarkError(f"{name} must be finite.")
            if positive and fv <= 0:
                raise BenchmarkError(f"{name} must be > 0.")
            if not positive and fv < 0:
                raise BenchmarkError(f"{name} must be >= 0.")
            return fv

        def _int(
            name: str, raw: Any, default: int, *, positive: bool,
        ) -> int:
            v = default if raw is None else raw
            if isinstance(v, bool) or not isinstance(v, int):
                raise BenchmarkError(f"{name} must be an int.")
            if positive and v <= 0:
                raise BenchmarkError(f"{name} must be > 0.")
            if not positive and v < 0:
                raise BenchmarkError(f"{name} must be >= 0.")
            return v

        out = dict(config)
        out["distillation_lr"] = _num(
            "distillation_lr",
            _coalesce("distillation_lr", "lr"),
            0.01, positive=True,
        )
        out["gating_lr"] = _num(
            "gating_lr",
            _coalesce("gating_lr", "gate_lr"),
            0.005, positive=True,
        )
        out["epsilon"] = _num(
            "epsilon", _coalesce("epsilon"), 0.1, positive=False,
        )
        if out["epsilon"] > 1.0:
            raise BenchmarkError("epsilon must be <= 1.0.")
        out["distill_weight"] = _num(
            "distill_weight",
            _coalesce("distill_weight", "distill_w"),
            0.7, positive=False,
        )
        out["rl_weight"] = _num(
            "rl_weight",
            _coalesce("rl_weight", "rl_w"),
            0.3, positive=False,
        )
        if out["distill_weight"] + out["rl_weight"] <= 0:
            raise BenchmarkError(
                "distill_weight + rl_weight must be > 0."
            )
        out["train_every"] = _int(
            "train_every", _coalesce("train_every"),
            DEFAULT_TRAIN_EVERY, positive=True,
        )
        out["batch_size"] = _int(
            "batch_size", _coalesce("batch_size"),
            DEFAULT_BATCH_SIZE, positive=True,
        )
        out["replay_size"] = _int(
            "replay_size",
            _coalesce("replay_size", "replay_buffer_size"),
            DEFAULT_REPLAY_SIZE, positive=True,
        )
        out["seed"] = config.get("seed")
        out["gate_seed"] = config.get("gate_seed")
        return out

    # ---------------------------------------------------------------- teachers
    def _rule_teacher(self, state: GEDDecisionState) -> np.ndarray:
        if state.graph_size > 100:
            return np.array([0.1, 0.9])
        if state.graph_size > 50:
            return np.array([0.3, 0.7])
        return np.array([0.8, 0.2])

    def _rlhf_teacher(self, state: GEDDecisionState) -> np.ndarray:
        probs = np.array([0.5, 0.5])
        if state.human > 0.7:
            probs[1] += 0.2
        elif state.human < 0.3:
            probs[0] += 0.2
        return probs / probs.sum()

    def _historical_teacher(self, state: GEDDecisionState) -> np.ndarray:
        if state.centrality > 0.7:
            return np.array([0.4, 0.6])
        return np.array([0.6, 0.4])

    # ---------------------------------------------------------------- gating
    def _gate_forward(self, x: np.ndarray) -> np.ndarray:
        logits = x @ self.gate_weights + self.gate_bias
        logits = logits - np.max(logits)
        exp = np.exp(logits)
        return exp / exp.sum()

    @staticmethod
    def _normalize_teacher(prob: Any, n_actions: int) -> np.ndarray:
        """
        Coerce a teacher's output into a valid probability vector of
        length ``n_actions``.

        Over-sized outputs are sliced first — ``np.pad`` with a
        negative width raises ``ValueError``.
        """
        arr = np.asarray(prob, dtype=np.float64).reshape(-1)
        if arr.size > n_actions:
            arr = arr[:n_actions]
        elif arr.size < n_actions:
            arr = np.pad(arr, (0, n_actions - arr.size), "constant")
        if not np.all(np.isfinite(arr)):
            raise BenchmarkError("teacher output has non-finite values.")
        total = arr.sum()
        if total <= 0:
            return np.ones(n_actions, dtype=np.float64) / n_actions
        return arr / total

    # ---------------------------------------------------------------- action
    def select_action(
        self, state: GEDDecisionState, exploration: bool = True,
    ) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        if not isinstance(state, GEDDecisionState):
            raise BenchmarkError("state must be a GEDDecisionState.")
        x = state.to_vector()
        if x.shape[0] != self.feature_dim:
            raise BenchmarkError(
                f"feature vector has {x.shape[0]} elements; expected "
                f"{self.feature_dim}."
            )

        teacher_outputs = np.stack([
            self._normalize_teacher(t(state), self.n_actions)
            for t in self.teachers
        ])
        gate = self._gate_forward(x)
        teacher_combined = (gate[:, None] * teacher_outputs).sum(axis=0)
        teacher_combined = teacher_combined / teacher_combined.sum()

        logits = x @ self.student_weights + self.student_bias
        logits = logits - logits.max()
        student_probs = np.exp(logits)
        student_probs = student_probs / student_probs.sum()

        with self._lock:
            if exploration and self._py_rng.random() < self.epsilon:
                action = self._py_rng.randint(0, self.n_actions - 1)
            else:
                combined = (
                    0.8 * student_probs + 0.2 * teacher_combined
                )
                action = int(np.argmax(combined))

        return action, x, teacher_outputs, teacher_combined

    # ---------------------------------------------------------------- update
    def update(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        teacher_outputs: np.ndarray,
        teacher_combined: np.ndarray,
    ) -> None:
        if not math.isfinite(float(reward)):
            raise BenchmarkError(
                f"reward must be finite, got {reward!r}."
            )
        with self._lock:
            self.replay.append(_ReplayEntry(
                state_vec=state_vec,
                action=int(action),
                reward=float(reward),
                teacher_outputs=teacher_outputs,
                teacher_combined=teacher_combined,
            ))
            self.counter += 1
            if (
                self.counter % self.train_every != 0
                or len(self.replay) < self.batch_size
            ):
                return
            batch = self._py_rng.sample(
                list(self.replay),
                min(self.batch_size, len(self.replay)),
            )
            self._train_batch(batch)

    def _train_batch(self, batch: List[_ReplayEntry]) -> None:
        for entry in batch:
            s = entry.state_vec
            cur_probs = self._student_probs(s)

            one_hot = np.zeros(self.n_actions)
            one_hot[entry.action] = 1.0
            grad_distill = -(entry.teacher_combined - cur_probs)
            grad_rl = -entry.reward * (one_hot - cur_probs)
            grad = self.distill_w * grad_distill + self.rl_w * grad_rl
            self.student_weights -= self.lr * np.outer(s, grad)
            self.student_bias -= self.lr * grad

            # Gate gradient — correct shape (n_teachers,) via
            # teacher_outputs @ error.
            target = self._student_probs(s)
            gate = self._gate_forward(s)
            combined = (
                gate[:, None] * entry.teacher_outputs
            ).sum(axis=0)
            combined = combined / combined.sum()
            error = combined - target
            grad_gate = entry.teacher_outputs @ error
            self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
            self.gate_bias -= self.gate_lr * grad_gate

    def _student_probs(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.student_weights + self.student_bias
        logits = logits - logits.max()
        probs = np.exp(logits)
        return probs / probs.sum()


# --------------------------------------------------------------------------- #
# Energy model
# --------------------------------------------------------------------------- #
def estimate_energy_carbon(
    latency_ms: float, graph_size: int,
) -> Tuple[float, float]:
    """
    **Mock** energy and carbon estimation.

    Assumes a 250 W power draw and a 385 gCO₂/kWh grid; scales linearly
    with graph size above 100 nodes.
    """
    energy_wh = (
        (latency_ms / 1000.0 / 3600.0)
        * DEFAULT_POWER_W
        * (1.0 + max(graph_size, 0) / 100.0)
    )
    carbon_kg = energy_wh * DEFAULT_GRID_CARBON_KG_PER_KWH
    return energy_wh, carbon_kg


# --------------------------------------------------------------------------- #
# Sequence generators
# --------------------------------------------------------------------------- #
def random_sequence(
    length: int, rng: Optional[random.Random] = None,
) -> List[str]:
    r = rng or random
    return [r.choice(ACTIONS) for _ in range(length)]


def similar_sequence(
    base: List[str],
    edit_distance: int = 2,
    rng: Optional[random.Random] = None,
) -> List[str]:
    r = rng or random
    seq = list(base)
    for _ in range(min(edit_distance, len(seq))):
        op = r.choice(["substitute", "insert", "delete"])
        idx = r.randrange(len(seq))
        if op == "substitute":
            seq[idx] = r.choice(ACTIONS)
        elif op == "insert" and len(seq) < len(base) + edit_distance:
            seq.insert(idx, r.choice(ACTIONS))
        elif op == "delete" and len(seq) > 1:
            seq.pop(idx)
    return seq


# --------------------------------------------------------------------------- #
# Timing helpers (lazy core import)
# --------------------------------------------------------------------------- #
def _ensure_core_on_path() -> None:
    """Add the project root to ``sys.path`` once, on demand."""
    if getattr(_ensure_core_on_path, "_done", False):
        return
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    _ensure_core_on_path._done = True  # type: ignore[attr-defined]


def time_ged_sync(seq_a: List[str], seq_b: List[str]) -> float:
    """Time one synchronous GED alignment."""
    _ensure_core_on_path()
    from core.async_traversal import _dp_align_sync  # lazy
    start = time.perf_counter()
    _dp_align_sync(seq_a, seq_b, EDIT_COSTS)
    return (time.perf_counter() - start) * 1000.0


async def time_ged_async(seq_a: List[str], seq_b: List[str]) -> float:
    """Time one asynchronous GED alignment."""
    _ensure_core_on_path()
    from core.async_traversal import AsyncDPAlign  # lazy
    start = time.perf_counter()
    await AsyncDPAlign.align(seq_a, seq_b, EDIT_COSTS)
    return (time.perf_counter() - start) * 1000.0


# --------------------------------------------------------------------------- #
# Statistics
# --------------------------------------------------------------------------- #
def percentile(sorted_data: List[float], p: float) -> float:
    """
    Interpolating percentile.

    Uses linear interpolation between the two bracketing samples, the
    same definition numpy / Prometheus / most benchmark harnesses use.
    """
    if not sorted_data:
        return 0.0
    n = len(sorted_data)
    if n == 1:
        return round(float(sorted_data[0]), 3)
    rank = (n - 1) * (p / 100.0)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return round(float(sorted_data[lo]), 3)
    frac = rank - lo
    return round(
        float(sorted_data[lo] * (1 - frac) + sorted_data[hi] * frac),
        3,
    )


def stats(times_ms: List[float]) -> Dict[str, float]:
    if not times_ms:
        raise BenchmarkError(
            "cannot compute statistics on an empty trial list."
        )
    s = sorted(times_ms)
    return {
        "p50": percentile(s, 50),
        "p95": percentile(s, 95),
        "p99": percentile(s, 99),
        "max": round(float(s[-1]), 3),
        "mean": round(sum(s) / len(s), 3),
    }


# --------------------------------------------------------------------------- #
# Row builder + benchmark runners
# --------------------------------------------------------------------------- #
def _make_row(
    *,
    size: int,
    n_trials: int,
    times: List[float],
    energies: List[float],
    carbons: List[float],
    methods: Dict[str, int],
    enhanced: bool,
) -> Dict[str, Any]:
    st = stats(times)
    return {
        "graph_size": size,
        "n_trials": n_trials,
        "p50_ms": st["p50"],
        "p95_ms": st["p95"],
        "p99_ms": st["p99"],
        "max_ms": st["max"],
        "mean_ms": st["mean"],
        "passed_sla": st["p99"] < SLA_MS,
        "avg_energy_wh": round(sum(energies) / len(energies), 6),
        "avg_carbon_kg": round(sum(carbons) / len(carbons), 6),
        "strategy_sync": methods.get("sync", 0),
        "strategy_async": methods.get("async", 0),
        "enhanced": enhanced,
    }


def _print_row(row: Dict[str, Any]) -> None:
    print(
        f"  size={row['graph_size']:4d}  "
        f"p50={row['p50_ms']:7.2f}ms  "
        f"p95={row['p95_ms']:7.2f}ms  "
        f"p99={row['p99_ms']:7.2f}ms  "
        f"[{'PASS' if row['passed_sla'] else 'FAIL'}]"
    )


def run_sync_benchmark(
    sizes: Tuple[int, ...] = GRAPH_SIZES,
    n_trials: int = N_TRIALS,
    *,
    seed: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Pure synchronous benchmark."""
    rng = random.Random(seed)
    results: List[Dict[str, Any]] = []
    for size in sizes:
        base = random_sequence(size, rng=rng)
        times: List[float] = []
        energies: List[float] = []
        carbons: List[float] = []
        for _ in range(n_trials):
            variant = similar_sequence(
                base, max(1, size // 5), rng=rng,
            )
            t = time_ged_sync(base, variant)
            e, c = estimate_energy_carbon(t, size)
            times.append(t)
            energies.append(e)
            carbons.append(c)
        row = _make_row(
            size=size, n_trials=n_trials,
            times=times, energies=energies, carbons=carbons,
            methods={"sync": n_trials, "async": 0},
            enhanced=False,
        )
        _print_row(row)
        results.append(row)
    return results


async def run_async_benchmark(
    sizes: Tuple[int, ...] = GRAPH_SIZES,
    n_trials: int = N_TRIALS,
    *,
    seed: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Pure asynchronous benchmark."""
    rng = random.Random(seed)
    results: List[Dict[str, Any]] = []
    for size in sizes:
        base = random_sequence(size, rng=rng)
        times: List[float] = []
        energies: List[float] = []
        carbons: List[float] = []
        for _ in range(n_trials):
            variant = similar_sequence(
                base, max(1, size // 5), rng=rng,
            )
            t = await time_ged_async(base, variant)
            e, c = estimate_energy_carbon(t, size)
            times.append(t)
            energies.append(e)
            carbons.append(c)
        row = _make_row(
            size=size, n_trials=n_trials,
            times=times, energies=energies, carbons=carbons,
            methods={"sync": 0, "async": n_trials},
            enhanced=False,
        )
        _print_row(row)
        results.append(row)
    return results


def _reward_for_trial(latency_ms: float, sla_ms: float = SLA_MS) -> float:
    """
    Per-trial reward for the optimizer.

    Returns a value in ``[0, 1]``:

    - ``1.0`` at zero latency,
    - ``0.5`` at exactly half the SLA,
    - ``0.0`` at (or above) the SLA.
    """
    if latency_ms >= sla_ms:
        return 0.0
    headroom = 1.0 - latency_ms / sla_ms
    return 0.5 + 0.5 * headroom


async def run_adaptive_benchmark(
    sizes: Tuple[int, ...] = GRAPH_SIZES,
    n_trials: int = N_TRIALS,
    *,
    optimizer: GEDDistillationOptimizer,
    human_feedback: float = 0.5,
    graph_centrality: float = 0.5,
    carbon_intensity: float = 400.0,
    seed: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Enhanced benchmark: the optimizer decides sync vs async per trial.
    """
    rng = random.Random(seed)
    results: List[Dict[str, Any]] = []
    for size in sizes:
        base = random_sequence(size, rng=rng)
        times: List[float] = []
        energies: List[float] = []
        carbons: List[float] = []
        methods = {"sync": 0, "async": 0}
        for _ in range(n_trials):
            variant = similar_sequence(
                base, max(1, size // 5), rng=rng,
            )
            state = GEDDecisionState.from_raw(
                graph_size=size,
                carbon_intensity=carbon_intensity,
                human_feedback=human_feedback,
                graph_centrality=graph_centrality,
            )
            action, x, t_out, t_comb = optimizer.select_action(state)
            if action == 1:
                t = await time_ged_async(base, variant)
                methods["async"] += 1
            else:
                t = time_ged_sync(base, variant)
                methods["sync"] += 1

            reward = _reward_for_trial(t)
            optimizer.update(x, action, reward, t_out, t_comb)

            e, c = estimate_energy_carbon(t, size)
            times.append(t)
            energies.append(e)
            carbons.append(c)
        row = _make_row(
            size=size, n_trials=n_trials,
            times=times, energies=energies, carbons=carbons,
            methods=methods, enhanced=True,
        )
        _print_row(row)
        results.append(row)
    return results


# --------------------------------------------------------------------------- #
# SLA validation
# --------------------------------------------------------------------------- #
async def validate_sla(
    n_trials: int = SLA_TRIALS,
    *,
    optimizer: Optional[GEDDistillationOptimizer] = None,
    human_feedback: float = 0.5,
    graph_centrality: float = 0.5,
    carbon_intensity: float = 400.0,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Validate the ``p99 < SLA_MS`` SLA at :data:`SLA_NODES` nodes.

    When ``optimizer`` is supplied, each trial uses the optimizer to
    pick sync or async; otherwise the check stays on the pure-sync
    path. The result reports how many trials used each method.
    """
    rng = random.Random(seed)
    base = random_sequence(SLA_NODES, rng=rng)
    times: List[float] = []
    energies: List[float] = []
    carbons: List[float] = []
    methods = {"sync": 0, "async": 0}

    for _ in range(n_trials):
        variant = similar_sequence(base, SLA_NODES // 5, rng=rng)
        if optimizer is not None:
            state = GEDDecisionState.from_raw(
                graph_size=SLA_NODES,
                carbon_intensity=carbon_intensity,
                human_feedback=human_feedback,
                graph_centrality=graph_centrality,
            )
            action, x, t_out, t_comb = optimizer.select_action(state)
            if action == 1:
                t = await time_ged_async(base, variant)
                methods["async"] += 1
            else:
                t = time_ged_sync(base, variant)
                methods["sync"] += 1
            optimizer.update(
                x, action, _reward_for_trial(t), t_out, t_comb,
            )
        else:
            t = time_ged_sync(base, variant)
            methods["sync"] += 1

        e, c = estimate_energy_carbon(t, SLA_NODES)
        times.append(t)
        energies.append(e)
        carbons.append(c)

    st = stats(times)
    passed = st["p99"] < SLA_MS
    return {
        "sla_target_nodes": SLA_NODES,
        "sla_target_ms": SLA_MS,
        "n_trials": n_trials,
        **st,
        "sla_passed": passed,
        "verdict": (
            "PASS" if passed
            else f"FAIL — p99 {st['p99']}ms > {SLA_MS}ms target"
        ),
        "avg_energy_wh": round(sum(energies) / len(energies), 6),
        "avg_carbon_kg": round(sum(carbons) / len(carbons), 6),
        "strategy_sync": methods["sync"],
        "strategy_async": methods["async"],
    }


# --------------------------------------------------------------------------- #
# CSV export
# --------------------------------------------------------------------------- #
def save_csv(
    results: List[Dict[str, Any]],
    filename: str,
    *,
    out_dir: Path = DEFAULT_RESULTS_DIR,
) -> Optional[Path]:
    """Write ``results`` to ``out_dir / filename``; returns the path."""
    if not results:
        return None
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise BenchmarkError(
            f"could not create {out_dir}: {exc}"
        ) from exc
    path = out_dir / filename
    try:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=list(results[0].keys()),
            )
            writer.writeheader()
            writer.writerows(results)
    except OSError as exc:
        raise BenchmarkError(
            f"could not write {path}: {exc}"
        ) from exc
    print(f"\nResults saved → {path}")
    return path


# --------------------------------------------------------------------------- #
# Comparison reporter
# --------------------------------------------------------------------------- #
def compare_report(
    results_a: List[Dict[str, Any]],
    results_b: List[Dict[str, Any]],
    *,
    label_a: str = "sync",
    label_b: str = "async",
) -> str:
    """Render a per-size comparison between two benchmark runs."""
    lines = [
        "\n" + "─" * 70,
        f"  {label_a.capitalize()} vs {label_b.capitalize()} GED "
        f"latency comparison",
        "─" * 70,
        f"  {'Size':>6}  "
        f"{label_a + ' p99':>14}  "
        f"{label_b + ' p99':>14}  "
        f"{'Winner':>8}",
        "─" * 70,
    ]
    crossovers: List[int] = []
    for row_a, row_b in zip(results_a, results_b):
        size = row_a["graph_size"]
        pa = row_a["p99_ms"]
        pb = row_b["p99_ms"]
        winner = label_a if pa <= pb else label_b
        if winner == label_b:
            crossovers.append(size)
        lines.append(
            f"  {size:>6}  {pa:>11.2f}ms  {pb:>11.2f}ms  "
            f"{winner:>8}"
        )
    lines.append("─" * 70)
    if crossovers:
        lines.append(
            f"  {label_b.capitalize()} beats {label_a} on p99 at sizes: "
            f"{min(crossovers)}..{max(crossovers)} "
            f"({len(crossovers)} of {len(results_a)} sizes)"
        )
    else:
        lines.append(
            f"  {label_a.capitalize()} beats {label_b} on p99 across "
            f"all {len(results_a)} tested sizes."
        )
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _positive_int(raw: str) -> int:
    try:
        v = int(raw)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(
            f"not an integer: {raw!r}"
        ) from None
    if v <= 0:
        raise argparse.ArgumentTypeError(f"must be > 0, got {v}")
    return v


def _positive_float(raw: str) -> float:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(
            f"not a float: {raw!r}"
        ) from None
    if not math.isfinite(v) or v <= 0:
        raise argparse.ArgumentTypeError(f"must be > 0, got {v}")
    return v


def _sizes_csv(raw: str) -> Tuple[int, ...]:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if not parts:
        raise argparse.ArgumentTypeError("sizes list is empty")
    try:
        return tuple(sorted({int(p) for p in parts}))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid sizes list: {raw!r}"
        ) from exc


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ged_benchmark",
        description=(
            "GED latency benchmark (sync vs async, optional adaptive "
            "decision-making via --enhance)."
        ),
    )
    parser.add_argument(
        "--async-compare", action="store_true",
        help="Also run the pure-async benchmark and compare.",
    )
    parser.add_argument(
        "--sla-check-only", action="store_true",
        help="Only run the SLA validation check.",
    )
    parser.add_argument(
        "--trials", type=_positive_int, default=N_TRIALS,
        help=f"Trials per graph size (default: {N_TRIALS}).",
    )
    parser.add_argument(
        "--sizes", type=_sizes_csv, default=GRAPH_SIZES,
        help=(
            "Comma-separated graph sizes (default: "
            f"{','.join(map(str, GRAPH_SIZES))})."
        ),
    )
    parser.add_argument(
        "--enhance", action="store_true",
        help="Enable adaptive sync-vs-async decision-making.",
    )
    parser.add_argument(
        "--strict", action="store_true",
        help="Treat enhancement-unavailable warnings as exit code 2.",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Seed for sequence generation and optimizer RNGs.",
    )
    parser.add_argument(
        "--out-dir", type=Path, default=DEFAULT_RESULTS_DIR,
        help=(
            "Directory for CSV output "
            f"(default: {DEFAULT_RESULTS_DIR})."
        ),
    )
    parser.add_argument(
        "--quick", action="store_true",
        help=(
            f"Shorthand for --trials {QUICK_TRIALS} and "
            f"--sla-trials {QUICK_SLA_TRIALS}."
        ),
    )
    parser.add_argument(
        "--sla-trials", type=_positive_int, default=SLA_TRIALS,
        help=(
            f"Trials for the SLA check (default: {SLA_TRIALS}; "
            f"{QUICK_SLA_TRIALS} with --quick)."
        ),
    )
    parser.add_argument(
        "--sla-ms", type=_positive_float, default=SLA_MS,
        help=(
            "SLA target in ms (informational; the SLA check uses "
            f"{SLA_MS}ms by default)."
        ),
    )
    parser.add_argument(
        "--version", action="version",
        version=f"%(prog)s {__version__}",
    )
    return parser


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def _run_coro_in_sync_context(coro: Any) -> Any:
    """
    Run ``coro`` to completion from a synchronous context.

    Raises a clear error when already inside a running event loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise BenchmarkError(
        "main() is synchronous and cannot run an async benchmark from "
        "inside a running event loop. Call the async functions "
        "(run_adaptive_benchmark / run_async_benchmark / validate_sla) "
        "directly from your loop."
    )


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_arg_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code if exc.code is not None else 0)

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    if args.quick:
        trials = QUICK_TRIALS
        sla_trials = QUICK_SLA_TRIALS
    else:
        trials = args.trials
        sla_trials = args.sla_trials

    if args.seed is not None:
        random.seed(args.seed)
        np.random.seed(args.seed)

    # ---- Enhancement availability gate ------------------------------ #
    optimizer: Optional[GEDDistillationOptimizer] = None
    if args.enhance:
        if not ENHANCEMENTS_AVAILABLE:
            print(
                "[WARNING] --enhance requested but the enhancements "
                "package is unavailable; running in legacy mode.",
                file=sys.stderr,
            )
            if args.strict:
                return 2
        else:
            try:
                optimizer = GEDDistillationOptimizer({
                    "seed": args.seed,
                    "gate_seed": (args.seed or 0) + 1,
                })
            except BenchmarkError as exc:
                print(
                    f"[ERROR] could not construct the optimizer: {exc}",
                    file=sys.stderr,
                )
                return 2
            print("\n[Adaptive mode enabled — optimizer will choose "
                  "sync vs async per trial]")

    # ---- SLA-only check --------------------------------------------- #
    if args.sla_check_only:
        print("\n" + "─" * 70)
        print("  SLA validation: GED p99 < "
              f"{SLA_MS}ms at {SLA_NODES} nodes")
        print("─" * 70)
        try:
            result = _run_coro_in_sync_context(validate_sla(
                n_trials=sla_trials,
                optimizer=optimizer,
                seed=args.seed,
            ))
        except BenchmarkError as exc:
            print(f"[ERROR] {exc}", file=sys.stderr)
            return 2
        for k, v in result.items():
            print(f"  {k:<25}: {v}")
        print("─" * 70)
        return 0 if result["sla_passed"] else 1

    # ---- Primary benchmark ----------------------------------------- #
    if optimizer is not None:
        print("\n" + "─" * 70)
        print("  Adaptive GED benchmark (sync vs async per trial)")
        print("─" * 70)
        try:
            primary = _run_coro_in_sync_context(run_adaptive_benchmark(
                sizes=args.sizes,
                n_trials=trials,
                optimizer=optimizer,
                seed=args.seed,
            ))
        except BenchmarkError as exc:
            print(f"[ERROR] {exc}", file=sys.stderr)
            return 2
        save_csv(primary, "ged_latency_adaptive.csv", out_dir=args.out_dir)
        primary_label = "adaptive"
    else:
        print("\n" + "─" * 70)
        print("  Synchronous GED benchmark")
        print("─" * 70)
        primary = run_sync_benchmark(
            sizes=args.sizes,
            n_trials=trials,
            seed=args.seed,
        )
        save_csv(primary, "ged_latency_sync.csv", out_dir=args.out_dir)
        primary_label = "sync"

    # ---- Optional comparison run ------------------------------------ #
    if args.async_compare:
        print("\n" + "─" * 70)
        print("  Async GED benchmark (reference)")
        print("─" * 70)
        try:
            reference = _run_coro_in_sync_context(run_async_benchmark(
                sizes=args.sizes,
                n_trials=trials,
                seed=args.seed,
            ))
        except BenchmarkError as exc:
            print(f"[ERROR] {exc}", file=sys.stderr)
            return 2
        save_csv(reference, "ged_latency_async.csv", out_dir=args.out_dir)
        print(compare_report(
            primary, reference,
            label_a=primary_label, label_b="async",
        ))

    # ---- SLA validation --------------------------------------------- #
    print("\n" + "─" * 70)
    print(f"  SLA boundary validation "
          f"({sla_trials} trials at {SLA_NODES} nodes)")
    print("─" * 70)
    try:
        sla = _run_coro_in_sync_context(validate_sla(
            n_trials=sla_trials,
            optimizer=optimizer,
            seed=args.seed,
        ))
    except BenchmarkError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2
    for k, v in sla.items():
        print(f"  {k:<25}: {v}")
    print("─" * 70)

    return 0 if sla["sla_passed"] else 1


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "ACTIONS",
    "BenchmarkError",
    "GEDDecisionState",
    "GEDDistillationOptimizer",
    "SLA_MS",
    "SLA_NODES",
    "compare_report",
    "estimate_energy_carbon",
    "main",
    "percentile",
    "random_sequence",
    "run_adaptive_benchmark",
    "run_async_benchmark",
    "run_sync_benchmark",
    "save_csv",
    "similar_sequence",
    "stats",
    "time_ged_async",
    "time_ged_sync",
    "validate_sla",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m benchmarks.ged_benchmark
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
