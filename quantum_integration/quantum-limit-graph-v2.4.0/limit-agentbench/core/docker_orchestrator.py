# -*- coding: utf-8 -*-
"""
Docker Orchestrator - Independent Execution Manager (Enhanced, v2)
==================================================================

Single-file consolidation of the modularized redesign.

Fixes applied vs. v1
--------------------
  P0  MoE gate gradient (wrong shape/sign)         -> distillation._train_step
  P0  self._last_decision race                     -> local `decision`
  P0  Config mutation                              -> frozen ContainerConfig.with_resources()
  P0  Path traversal                               -> security.sanitize_task_id / safe_join
  P0  docker ps -f label never matched             -> executor adds --label/--name
  P0  Malformed output JSON                        -> orchestrator._read_output
  P1  n_profiles hard-coded                        -> derived from ContainerConfig
  P1  Broken energy normalization                  -> reward.MODPReward 1/(1+q/scale)
  P1  No model persistence                         -> save()/load() on both optimizers
  P1  No observability hooks                       -> MetricsSink protocol
  P1  Input validation                             -> __post_init__ on configs
  P2  Pluggable reward/selector                    -> RewardFn / optimizer interface
  P2  Evolutionary was `pass`                      -> full EvolutionaryResourceOptimizer
  P2  epsilon never decayed                        -> distillation._train_step
  P2  RNG not seeded                               -> np.random.default_rng(seed)

Preserved
---------
  - Original public surface: ContainerConfig, OrchestratorConfig, DockerOrchestrator
  - Original helper functions: create_dockerfile, create_entrypoint (unchanged)
  - Original fields/logic of ContainerConfig, OrchestratorConfig, ResourceAllocationState
"""

from __future__ import annotations

import copy
import json
import logging
import re
import shutil
import subprocess
import time
from collections import deque
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
)

import numpy as np

logger = logging.getLogger(__name__)


# ============================================================================
# SECTION 1 — Security / validation helpers
# ============================================================================

_TASK_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_RESERVED_TASK_IDS = {"", ".", ".."}
_IMAGE_RE = re.compile(r"^[a-z0-9][a-z0-9._/:-]{0,254}$")
_CPU_RE = re.compile(r"^[0-9]+(\.[0-9]+)?$")
_MEM_RE = re.compile(r"^[0-9]+[kmgKMG]?$")
_ALLOWED_NETWORKS = {"none", "host", "bridge"}


def sanitize_task_id(task_id: Any) -> str:
    if not isinstance(task_id, str):
        raise ValueError(f"task_id must be str, got {type(task_id).__name__}")
    if task_id in _RESERVED_TASK_IDS or not _TASK_ID_RE.fullmatch(task_id):
        raise ValueError(
            f"Invalid task_id {task_id!r}: must match [A-Za-z0-9._-]{{1,64}}"
        )
    return task_id


def safe_join(base: Path, task_id: str) -> Path:
    sanitized = sanitize_task_id(task_id)
    base_resolved = Path(base).resolve()
    candidate = (base_resolved / sanitized).resolve()
    if candidate != base_resolved and base_resolved not in candidate.parents:
        raise ValueError(f"task_id {task_id!r} escapes {base}")
    return candidate


# ============================================================================
# SECTION 2 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class ContainerConfig:
    """Docker container configuration (frozen and validated)."""
    image: str
    cpu_limit: str = "2.0"
    memory_limit: str = "4g"
    gpu_limit: Optional[str] = None
    timeout_seconds: int = 300
    # Discrete profiles available to the distillation optimizer
    cpu_options: Tuple[str, ...] = ("1.0", "2.0", "4.0", "8.0")
    memory_options: Tuple[str, ...] = ("2g", "4g", "8g", "16g")
    # Hardening flags
    network: str = "none"
    read_only_rootfs: bool = True
    cap_drop_all: bool = True
    no_new_privileges: bool = True
    rw_workdir: bool = False
    extra_env: Tuple[Tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not _IMAGE_RE.fullmatch(self.image):
            raise ValueError(f"Invalid image reference: {self.image!r}")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")
        if not self.cpu_options:
            raise ValueError("cpu_options must not be empty")
        if len(self.cpu_options) != len(self.memory_options):
            raise ValueError("cpu_options and memory_options must have equal length")
        for c in self.cpu_options:
            if not _CPU_RE.fullmatch(c):
                raise ValueError(f"Invalid cpu option: {c!r}")
        for m in self.memory_options:
            if not _MEM_RE.fullmatch(m):
                raise ValueError(f"Invalid memory option: {m!r}")
        if self.network not in _ALLOWED_NETWORKS and not self.network.startswith("container:"):
            raise ValueError(f"Unsupported network mode: {self.network!r}")
        for k, _ in self.extra_env:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", k):
                raise ValueError(f"Invalid env var name: {k!r}")

    @property
    def n_profiles(self) -> int:
        return len(self.cpu_options)

    def with_resources(self, profile_idx: int) -> "ContainerConfig":
        if not 0 <= profile_idx < self.n_profiles:
            raise IndexError(
                f"profile_idx {profile_idx} out of range [0, {self.n_profiles})"
            )
        return replace(
            self,
            cpu_limit=self.cpu_options[profile_idx],
            memory_limit=self.memory_options[profile_idx],
        )

    def to_docker_args(self) -> List[str]:
        args = ["--cpus", self.cpu_limit, "--memory", self.memory_limit]
        if self.gpu_limit:
            args.extend(["--gpus", self.gpu_limit])
        return args


@dataclass(frozen=True)
class OrchestratorConfig:
    """Configuration for enhanced orchestrator features (frozen and validated)."""
    use_enhancements: bool = False
    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {
            "centrality": 0.5,
            "connectivity": 0.5,
            "density": 0.4,
        }
    )
    modp_weights: Tuple[float, float, float] = (0.4, 0.3, 0.3)
    human_feedback_score: float = 0.5
    use_distillation: bool = True
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    batch_size: int = 16
    epsilon: float = 0.1
    epsilon_min: float = 0.02
    epsilon_decay: float = 0.995
    use_evolutionary: bool = False
    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2
    max_input_bytes: int = 1_000_000
    seed: Optional[int] = None
    model_path: Optional[Path] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if len(w) != 3 or any(x < 0 for x in w) or sum(w) <= 0:
            raise ValueError(f"modp_weights must be 3 non-negative values, got {w!r}")
        if not 0.0 <= self.human_feedback_score <= 1.0:
            raise ValueError("human_feedback_score must be in [0,1]")
        if not 0.0 <= self.epsilon <= 1.0:
            raise ValueError("epsilon must be in [0,1]")
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if self.population_size < 2:
            raise ValueError("population_size must be >= 2")
        if self.elitism >= self.population_size:
            raise ValueError("elitism must be < population_size")
        for k, v in self.graph_metrics.items():
            if not isinstance(v, (int, float)) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(f"graph_metric {k!r} must be in [0,1], got {v!r}")


# ============================================================================
# SECTION 3 — State representation
# ============================================================================

FEATURE_DIM = 7


class ResourceAllocationState:
    """Feature vector for resource selection (bounded / clipped)."""
    __slots__ = (
        "task_type", "input_size", "priority", "has_constraints",
        "graph_centrality", "graph_connectivity", "human_feedback", "features",
    )

    def __init__(
        self,
        task_request: Dict[str, Any],
        graph_metrics: Dict[str, float],
        human_feedback: float,
    ) -> None:
        self.task_type = str(task_request.get("task_type", "unknown"))
        self.input_size = len(str(task_request.get("input_data", {})))
        try:
            self.priority = max(0.0, float(task_request.get("priority", 1)))
        except (TypeError, ValueError):
            self.priority = 1.0
        self.has_constraints = 1.0 if task_request.get("constraints") else 0.0
        self.graph_centrality = float(np.clip(graph_metrics.get("centrality", 0.5), 0, 1))
        self.graph_connectivity = float(np.clip(graph_metrics.get("connectivity", 0.5), 0, 1))
        self.human_feedback = float(np.clip(human_feedback, 0.0, 1.0))
        self.features = np.array(
            [
                min(self.input_size / 10_000.0, 1.0),
                min(self.priority / 10.0, 1.0),
                self.has_constraints,
                self.graph_centrality,
                self.graph_connectivity,
                self.human_feedback,
                min(len(self.task_type) / 50.0, 1.0),
            ],
            dtype=np.float32,
        )
        assert self.features.shape == (FEATURE_DIM,)


# ============================================================================
# SECTION 4 — Pluggable reward (bounded, scale-invariant)
# ============================================================================

class RewardFn(Protocol):
    def __call__(
        self,
        *,
        execution_time: float,
        timeout: float,
        energy_kwh: float,
        carbon_kg: float,
    ) -> float: ...


class MODPReward:
    """
    MODP reward with log-shaped normalization.

    energy_norm  = 1 / (1 + energy_kwh / energy_scale_kwh)
    carbon_norm  = 1 / (1 + carbon_kg  / carbon_scale_kg)
    latency_norm = 1 - min(execution_time / timeout, 1)

    Unlike the original (which saturated near 1 for any realistic execution),
    these terms are bounded on (0, 1] and vary meaningfully across tasks.
    """

    def __init__(
        self,
        weights: Tuple[float, float, float] = (0.4, 0.3, 0.3),
        energy_scale_kwh: float = 1e-3,
        carbon_scale_kg: float = 4e-4,
    ) -> None:
        w = np.asarray(weights, dtype=np.float64)
        if len(w) != 3 or (w < 0).any() or w.sum() <= 0:
            raise ValueError(f"Invalid weights: {weights!r}")
        self.weights = w / w.sum()
        self.energy_scale_kwh = float(energy_scale_kwh)
        self.carbon_scale_kg = float(carbon_scale_kg)

    def __call__(
        self,
        *,
        execution_time: float,
        timeout: float,
        energy_kwh: float,
        carbon_kg: float,
    ) -> float:
        if timeout <= 0:
            raise ValueError("timeout must be > 0")
        latency_norm = 1.0 - min(execution_time / timeout, 1.0)
        energy_norm = 1.0 / (1.0 + energy_kwh / self.energy_scale_kwh)
        carbon_norm = 1.0 / (1.0 + carbon_kg / self.carbon_scale_kg)
        return float(np.dot([energy_norm, latency_norm, carbon_norm], self.weights))


# ============================================================================
# SECTION 5 — Persistence and metrics
# ============================================================================

def save_state(path: Path, **arrays: np.ndarray) -> None:
    payload = {k: np.asarray(v).tolist() for k, v in arrays.items()}
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    tmp.replace(path)


def load_state(path: Path) -> Dict[str, np.ndarray]:
    path = Path(path)
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return {k: np.asarray(v) for k, v in payload.items()}


class MetricsSink(Protocol):
    def observe(self, name: str, value: float, **labels: str) -> None: ...


class InMemoryMetrics:
    """Usable default; also handy for tests and a future /metrics endpoint."""

    def __init__(self) -> None:
        self._data: Dict[str, float] = {}

    def observe(self, name: str, value: float, **labels: str) -> None:
        key = name + (
            ":" + ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            if labels else ""
        )
        self._data[key] = value

    def snapshot(self) -> Dict[str, float]:
        return dict(self._data)


class LoggingMetrics:
    def __init__(self) -> None:
        self._log = logging.getLogger("orchestrator.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 6 — Distillation + MoE optimizer (fixed gate gradient)
# ============================================================================

TeacherFn = Callable[[ResourceAllocationState], np.ndarray]


class ResourceDistillationOptimizer:
    """
    Distillation + MoE gating to select CPU/memory profiles.

    Fixes vs. original:
      - Gate gradient is now computed as t_outs @ err with shape (T,)
        (previously it collapsed to a scalar and broadcast against (D,T)).
      - n_profiles is a constructor argument; teachers resample to any size.
      - epsilon decays after each batch.
      - reward is clipped to [-1, 1] before use.
      - save()/load() added for persistence.
      - Deterministic seeding via config.seed.
    """

    def __init__(
        self,
        n_profiles: int,
        config: OrchestratorConfig,
        rng: Optional[np.random.Generator] = None,
    ) -> None:
        if n_profiles < 1:
            raise ValueError("n_profiles must be >= 1")
        self.n_profiles = int(n_profiles)
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = FEATURE_DIM
        self.student_weights = np.zeros((self.feature_dim, self.n_profiles))
        self.student_bias = np.zeros(self.n_profiles)

        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.distill_w = 0.7
        self.rl_w = 0.3

        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay

        self.replay: deque = deque(maxlen=config.replay_size)
        self.counter = 0
        self.train_every = config.train_every
        self.batch_size = config.batch_size

        self.teachers: List[TeacherFn] = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]

        self.gate_weights = (
            self.rng.standard_normal((self.feature_dim, len(self.teachers))) * 0.01
        )
        self.gate_bias = np.zeros(len(self.teachers))

        if config.model_path:
            self.load(config.model_path)

    # ---- Teachers (always length self.n_profiles) -------------------------
    def _uniform(self) -> np.ndarray:
        return np.ones(self.n_profiles) / self.n_profiles

    def _from_anchors(self, anchors: List[float]) -> np.ndarray:
        arr = np.asarray(anchors, dtype=np.float64)
        if arr.size == 0:
            return self._uniform()
        if arr.size != self.n_profiles:
            x_old = np.linspace(0.0, 1.0, arr.size)
            x_new = np.linspace(0.0, 1.0, self.n_profiles)
            arr = np.interp(x_new, x_old, arr)
        arr = np.maximum(arr, 0.0)
        total = arr.sum()
        return arr / total if total > 0 else self._uniform()

    def _rule_teacher(self, state: ResourceAllocationState) -> np.ndarray:
        p = self.n_profiles
        if state.features[1] > 0.7 or state.features[0] > 0.7:
            anchors = list(np.linspace(0.0, 1.0, max(p, 2)) ** 2)
        elif state.features[1] > 0.3:
            anchors = list(
                np.exp(-((np.arange(p) - (p - 1) / 2) ** 2)
                       / (2 * (max(p, 2) / 4) ** 2))
            )
        else:
            anchors = list(np.linspace(1.0, 0.0, max(p, 2)) ** 2)
        return self._from_anchors(anchors)

    def _rlhf_teacher(self, state: ResourceAllocationState) -> np.ndarray:
        probs = self._uniform().copy()
        if state.human_feedback > 0.7:
            probs[-1] += 0.3
            probs[0] = max(probs[0] - 0.1, 0.0)
        elif state.human_feedback < 0.3:
            probs[0] += 0.3
            probs[-1] = max(probs[-1] - 0.1, 0.0)
        return probs / probs.sum()

    def _historical_teacher(self, state: ResourceAllocationState) -> np.ndarray:
        if state.features[2] > 0.5:
            return self._from_anchors([0.0, 0.1, 0.3, 0.6])
        return self._from_anchors([0.4, 0.4, 0.2, 0.0])

    # ---- Forward ----------------------------------------------------------
    @staticmethod
    def _softmax(logits: np.ndarray) -> np.ndarray:
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, x: np.ndarray) -> np.ndarray:
        return self._softmax(x @ self.gate_weights + self.gate_bias)

    def select_profile(
        self,
        state: ResourceAllocationState,
        exploration: bool = True,
    ) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        """
        Returns
        -------
        action          : int
        state_vec       : (FEATURE_DIM,)
        teacher_outputs : (n_teachers, n_profiles)  individual distributions
        teacher_probs   : (n_profiles,)             gated mixture
        """
        x = state.features
        teacher_outputs = np.stack([t(state) for t in self.teachers])
        gate = self._gate_forward(x)
        teacher_probs = np.sum(gate[:, None] * teacher_outputs, axis=0)
        teacher_probs /= teacher_probs.sum()

        student_probs = self._softmax(x @ self.student_weights + self.student_bias)

        if exploration and self.rng.random() < self.epsilon:
            action = int(self.rng.integers(0, self.n_profiles))
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action = int(np.argmax(combined))

        return action, x, teacher_outputs, teacher_probs

    # ---- Training ---------------------------------------------------------
    def update(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_outputs: np.ndarray,
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        self.replay.append(
            (
                state_vec.astype(np.float64),
                int(action),
                r,
                next_state_vec.astype(np.float64),
                teacher_outputs.astype(np.float64),
            )
        )
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay) >= self.batch_size:
            self._train_step()

    def _train_step(self) -> None:
        n = min(self.batch_size, len(self.replay))
        idxs = self.rng.choice(len(self.replay), size=n, replace=False)
        batch = [self.replay[i] for i in idxs]

        for s, a, r, _ns, t_outs in batch:
            gate = self._gate_forward(s)
            combined_teacher = np.sum(gate[:, None] * t_outs, axis=0)
            cur = self._softmax(s @ self.student_weights + self.student_bias)

            # ---- student update ----
            one_hot = np.zeros(self.n_profiles)
            one_hot[a] = 1.0
            grad_distill = -(combined_teacher - cur)
            grad_rl = -r * (one_hot - cur)
            grad = self.distill_w * grad_distill + self.rl_w * grad_rl

            self.student_weights -= self.lr * np.outer(s, grad)
            self.student_bias -= self.lr * grad

            # ---- gate update (FIXED) ----
            # Target: reward-weighted one-hot. Teachers that already put mass
            # on the rewarded action get their gate weight increased.
            if self.n_profiles > 1:
                target = np.full(
                    self.n_profiles, (1.0 - r) / (self.n_profiles - 1)
                )
            else:
                target = np.ones(1)
            target[a] = r

            err = combined_teacher - target               # (P,)
            grad_gate = t_outs @ err                      # (T,)
            self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
            self.gate_bias -= self.gate_lr * grad_gate

        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    # ---- Persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        save_state(
            path,
            student_weights=self.student_weights,
            student_bias=self.student_bias,
            gate_weights=self.gate_weights,
            gate_bias=self.gate_bias,
            epsilon=np.asarray(self.epsilon),
            counter=np.asarray(self.counter),
            n_profiles=np.asarray(self.n_profiles),
        )

    def load(self, path: Path) -> None:
        data = load_state(path)
        if not data:
            return
        if "n_profiles" in data and int(data["n_profiles"]) != self.n_profiles:
            raise ValueError(
                f"Checkpoint n_profiles={int(data['n_profiles'])} "
                f"does not match optimizer n_profiles={self.n_profiles}"
            )
        self.student_weights = data["student_weights"]
        self.student_bias = data["student_bias"]
        self.gate_weights = data["gate_weights"]
        self.gate_bias = data["gate_bias"]
        self.epsilon = float(data.get("epsilon", self.epsilon))
        self.counter = int(data.get("counter", self.counter))
        logger.info("Loaded distillation checkpoint from %s", path)


# ============================================================================
# SECTION 7 — Evolutionary optimizer (fully implemented)
# ============================================================================

Individual = dict  # {"weights", "bias", "fitness", "n_evaluations"}


class EvolutionaryResourceOptimizer:
    """
    Population-based resource selector. Each individual is a linear policy
    mapping state features -> profile logits. Fitness accumulates reward for
    observations on which the individual's argmax matches the taken action.
    Tournament selection + uniform crossover + Gaussian mutation + elitism.
    """

    def __init__(
        self,
        n_profiles: int,
        config: OrchestratorConfig,
        rng: Optional[np.random.Generator] = None,
    ) -> None:
        if n_profiles < 1:
            raise ValueError("n_profiles must be >= 1")
        self.n_profiles = int(n_profiles)
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = FEATURE_DIM
        self.population: List[Individual] = self._init_population(config.population_size)
        self.generation = 0
        self.evaluations = 0

        if config.model_path:
            self.load(config.model_path)

    def _init_population(self, size: int) -> List[Individual]:
        return [
            {
                "weights": self.rng.standard_normal(
                    (self.feature_dim, self.n_profiles)
                ) * 0.1,
                "bias": self.rng.standard_normal(self.n_profiles) * 0.01,
                "fitness": 0.0,
                "n_evaluations": 0,
            }
            for _ in range(size)
        ]

    def _forward(self, individual: Individual, x: np.ndarray) -> int:
        logits = x @ individual["weights"] + individual["bias"]
        return int(np.argmax(logits))

    def select_profile(
        self,
        state: ResourceAllocationState,
        exploration: bool = True,
    ) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        x = state.features
        if exploration and self.rng.random() < self.config.epsilon:
            action = int(self.rng.integers(0, self.n_profiles))
        else:
            best = max(self.population, key=lambda ind: ind["fitness"])
            action = self._forward(best, x)
        # Uniform teacher outputs to keep the shared interface.
        teacher_outputs = np.tile(
            np.ones(self.n_profiles) / self.n_profiles, (3, 1)
        )
        teacher_probs = np.ones(self.n_profiles) / self.n_profiles
        return action, x, teacher_outputs, teacher_probs

    def update(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_outputs: np.ndarray,
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        for ind in self.population:
            if self._forward(ind, state_vec) == action:
                ind["fitness"] += r
                ind["n_evaluations"] += 1
        self.evaluations += 1
        if self.evaluations % self.config.train_every == 0:
            self.evolve()

    def evolve(self) -> None:
        self.population.sort(key=lambda ind: ind["fitness"], reverse=True)
        n_pop = len(self.population)
        n_elite = min(self.config.elitism, n_pop)
        new_pop: List[Individual] = [
            copy.deepcopy(ind) for ind in self.population[:n_elite]
        ]
        while len(new_pop) < n_pop:
            a = self._tournament()
            b = self._tournament()
            child = self._crossover(a, b)
            self._mutate(child)
            child["fitness"] = 0.0
            child["n_evaluations"] = 0
            new_pop.append(child)
        self.population = new_pop
        self.generation += 1
        logger.debug("Evolved to generation %d", self.generation)

    def _tournament(self, k: int = 3) -> Individual:
        size = min(k, len(self.population))
        idxs = self.rng.choice(len(self.population), size=size, replace=False)
        contestants = [self.population[i] for i in idxs]
        return max(contestants, key=lambda ind: ind["fitness"])

    def _crossover(self, a: Individual, b: Individual) -> Individual:
        if self.rng.random() < self.config.crossover_rate:
            mask_w = self.rng.random(a["weights"].shape) < 0.5
            mask_b = self.rng.random(a["bias"].shape) < 0.5
            return {
                "weights": np.where(mask_w, a["weights"], b["weights"]),
                "bias": np.where(mask_b, a["bias"], b["bias"]),
                "fitness": 0.0,
                "n_evaluations": 0,
            }
        return {
            "weights": a["weights"].copy(),
            "bias": a["bias"].copy(),
            "fitness": 0.0,
            "n_evaluations": 0,
        }

    def _mutate(self, individual: Individual) -> None:
        mw = self.rng.random(individual["weights"].shape) < self.config.mutation_rate
        mb = self.rng.random(individual["bias"].shape) < self.config.mutation_rate
        individual["weights"] = (
            individual["weights"]
            + mw * self.rng.standard_normal(individual["weights"].shape) * 0.1
        )
        individual["bias"] = (
            individual["bias"]
            + mb * self.rng.standard_normal(individual["bias"].shape) * 0.1
        )

    def save(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generation": self.generation,
            "evaluations": self.evaluations,
            "population": [
                {
                    "weights": ind["weights"].tolist(),
                    "bias": ind["bias"].tolist(),
                    "fitness": float(ind["fitness"]),
                }
                for ind in self.population
            ],
        }
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        tmp.replace(path)

    def load(self, path: Path) -> None:
        path = Path(path)
        if not path.exists():
            return
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        self.generation = int(payload["generation"])
        self.evaluations = int(payload["evaluations"])
        self.population = [
            {
                "weights": np.asarray(ind["weights"]),
                "bias": np.asarray(ind["bias"]),
                "fitness": float(ind["fitness"]),
                "n_evaluations": 0,
            }
            for ind in payload["population"]
        ]
        logger.info("Loaded evolutionary checkpoint from %s", path)


# ============================================================================
# SECTION 8 — Hardened Docker executor
# ============================================================================

class DockerExecutor:
    """
    Hardened Docker execution with retries and correct labeling.

    Fixes: --label task_id=..., --name, --cap-drop ALL, --no-new-privileges,
    --read-only + tmpfs, optional rw workdir, image pull-if-missing,
    retries with exponential backoff, deterministic container naming.
    """

    def __init__(
        self,
        work_dir: Path,
        retries: int = 2,
        retry_backoff: float = 0.5,
    ) -> None:
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.retries = retries
        self.retry_backoff = retry_backoff
        self.containers: Dict[str, subprocess.Popen] = {}

    def ensure_image(self, image: str) -> None:
        r = subprocess.run(
            ["docker", "image", "inspect", image],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            return
        logger.info("Pulling image %s", image)
        p = subprocess.run(["docker", "pull", image], capture_output=True, text=True)
        if p.returncode != 0:
            raise RuntimeError(f"Failed to pull image {image}: {p.stderr.strip()}")

    def _build_command(
        self,
        task_id: str,
        config: ContainerConfig,
        image: str,
        task_dir: Path,
        input_file: Path,
        output_file: Path,
    ) -> List[str]:
        cmd: List[str] = [
            "docker", "run", "--rm",
            "--name", f"agent-{task_id}",
            "--label", f"task_id={task_id}",
            "--network", config.network,
        ]
        cmd.extend(config.to_docker_args())

        if config.cap_drop_all:
            cmd.extend(["--cap-drop", "ALL"])
        if config.no_new_privileges:
            cmd.extend(["--security-opt", "no-new-privileges"])
        if config.read_only_rootfs:
            cmd.extend(["--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,size=64m"])

        cmd.extend([
            "-v", f"{input_file.absolute()}:/app/input.json:ro",
            "-v", f"{output_file.absolute()}:/app/output.json:rw",
        ])
        if config.rw_workdir:
            cmd.extend(["-v", f"{task_dir.absolute()}:/app/work:rw"])

        for key, value in config.extra_env:
            cmd.extend(["-e", f"{key}={value}"])
        cmd.extend([
            "-e", "A2A_INPUT=/app/input.json",
            "-e", "A2A_OUTPUT=/app/output.json",
            "-e", "ENABLE_GREEN_METRICS=true",
        ])
        cmd.append(image)
        return cmd

    def run(
        self,
        task_id: str,
        config: ContainerConfig,
        image: str,
        task_dir: Path,
        input_file: Path,
        output_file: Path,
    ) -> Dict[str, Any]:
        self.ensure_image(image)
        cmd = self._build_command(
            task_id, config, image, task_dir, input_file, output_file
        )
        last_err: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            try:
                return self._run_once(cmd, config.timeout_seconds, task_id)
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                logger.warning(
                    "Attempt %d/%d failed: %s", attempt + 1, self.retries + 1, exc
                )
                if attempt < self.retries:
                    time.sleep(self.retry_backoff * (2 ** attempt))
        raise RuntimeError(f"All {self.retries + 1} attempts failed: {last_err}")

    def _run_once(
        self,
        cmd: List[str],
        timeout: int,
        task_id: str,
    ) -> Dict[str, Any]:
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.containers[task_id] = process
        try:
            try:
                stdout, stderr = process.communicate(timeout=timeout)
                exit_code = process.returncode
                timed_out = False
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
                exit_code = -1
                timed_out = True
                stderr = f"Timeout after {timeout}s\n{stderr or ''}"
            return {
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "timed_out": timed_out,
            }
        finally:
            self.containers.pop(task_id, None)

    def cleanup(self, task_id: Optional[str] = None) -> None:
        if task_id:
            proc = self.containers.pop(task_id, None)
            if proc and proc.poll() is None:
                proc.kill()
            subprocess.run(
                ["docker", "rm", "-f", f"agent-{task_id}"],
                capture_output=True, text=True,
            )


# ============================================================================
# SECTION 9 — Orchestrator (main entrypoint)
# ============================================================================

class DockerOrchestrator:
    """
    Docker Orchestrator for Independent Agent Execution with optional
    enhancements.

    Public surface preserved from v1:
        DockerOrchestrator(work_dir, orchestrator_config)
        .execute_task(task_request, config, agent_image=..., ...)
        .cleanup(task_id=None)
        .get_container_stats(task_id)
        .containers  # kept for backwards compat (delegates to executor)
    """

    def __init__(
        self,
        work_dir: Path = Path("./work"),
        orchestrator_config: Optional[OrchestratorConfig] = None,
        reward_fn: Optional[RewardFn] = None,
        metrics: Optional[MetricsSink] = None,
    ) -> None:
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.config = orchestrator_config or OrchestratorConfig()
        self.executor = DockerExecutor(self.work_dir)
        self.reward_fn: RewardFn = reward_fn or MODPReward(
            weights=self.config.modp_weights
        )
        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self._optimizer = None  # type: Optional[Any]

    # Backwards-compat: v1 exposed `self.containers`
    @property
    def containers(self) -> Dict[str, subprocess.Popen]:
        return self.executor.containers

    # ------------------------------------------------------------------
    def _get_optimizer(self, config: ContainerConfig):
        if not self.config.use_enhancements:
            return None
        n = config.n_profiles
        if self._optimizer is not None and self._optimizer.n_profiles == n:
            return self._optimizer
        if self.config.use_evolutionary:
            opt = EvolutionaryResourceOptimizer(n, self.config)
        elif self.config.use_distillation:
            opt = ResourceDistillationOptimizer(n, self.config)
        else:
            return None
        self._optimizer = opt
        return opt

    # ------------------------------------------------------------------
    def execute_task(
        self,
        task_request: Dict[str, Any],
        config: ContainerConfig,
        agent_image: str = "limit-graph-agent:latest",
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        # 1. Validate & sanitize ---------------------------------------
        if not isinstance(task_request, dict):
            raise TypeError("task_request must be a dict")
        task_id = sanitize_task_id(task_request.get("task_id", "unknown"))
        image = agent_image or config.image

        if graph_metrics is None:
            graph_metrics = self.config.graph_metrics
        if human_feedback_score is None:
            human_feedback_score = self.config.human_feedback_score

        # 2. Prepare task dir ------------------------------------------
        task_dir = safe_join(self.work_dir, task_id)
        task_dir.mkdir(parents=True, exist_ok=True)
        input_file = task_dir / "input.json"
        output_file = task_dir / "output.json"

        input_json = json.dumps(task_request, indent=2)
        if len(input_json.encode("utf-8")) > self.config.max_input_bytes:
            raise ValueError(
                f"input exceeds {self.config.max_input_bytes} bytes"
            )
        input_file.write_text(input_json, encoding="utf-8")
        if output_file.exists():
            output_file.unlink()

        # 3. Resource selection (local decision -> thread-safe) --------
        effective_config: ContainerConfig = config
        decision: Optional[Tuple[np.ndarray, int, np.ndarray, np.ndarray]] = None
        optimizer = self._get_optimizer(config)
        if optimizer is not None:
            state = ResourceAllocationState(
                task_request, graph_metrics, human_feedback_score
            )
            action, state_vec, teacher_outputs, teacher_probs = (
                optimizer.select_profile(state)
            )
            effective_config = config.with_resources(action)
            decision = (state_vec, action, teacher_outputs, teacher_probs)

        # 4. Execute ---------------------------------------------------
        start = time.time()
        result = self.executor.run(
            task_id, effective_config, image, task_dir, input_file, output_file
        )
        execution_time = time.time() - start

        # 5. Read output -----------------------------------------------
        response = self._read_output(task_id, output_file, execution_time, result)
        response["container_metadata"] = {
            "image": image,
            "exit_code": result["exit_code"],
            "timed_out": result.get("timed_out", False),
            "execution_time": execution_time,
            "resource_limits": {
                "cpu": effective_config.cpu_limit,
                "memory": effective_config.memory_limit,
            },
            "profile_index": decision[1] if decision else None,
            "graph_metrics": graph_metrics,
            "human_feedback_score": human_feedback_score,
        }

        # 6. Reward + optimizer update ---------------------------------
        if decision is not None and optimizer is not None:
            energy_kwh = execution_time * 1e-4
            carbon_kg = energy_kwh * 0.4
            reward = self.reward_fn(
                execution_time=execution_time,
                timeout=config.timeout_seconds,
                energy_kwh=energy_kwh,
                carbon_kg=carbon_kg,
            )
            state_vec, action, teacher_outputs, _ = decision
            optimizer.update(state_vec, action, reward, state_vec, teacher_outputs)
            response["modp_reward"] = reward
            response["optimizer_stats"] = {
                "kind": type(optimizer).__name__,
                "counter": getattr(
                    optimizer, "counter", getattr(optimizer, "evaluations", None)
                ),
            }

        # 7. Observability ---------------------------------------------
        self.metrics.observe(
            "orchestrator_task_duration_seconds", execution_time,
            task_id=task_id, image=image,
        )
        self.metrics.observe(
            "orchestrator_task_exit_code", float(result["exit_code"]),
            task_id=task_id,
        )
        if decision is not None:
            self.metrics.observe(
                "orchestrator_resource_profile", float(decision[1]),
                task_id=task_id,
            )

        return response

    # ------------------------------------------------------------------
    def _read_output(
        self,
        task_id: str,
        output_file: Path,
        execution_time: float,
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        if output_file.exists():
            try:
                with open(output_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Malformed output for task %s: %s", task_id, exc)
        return {
            "task_id": task_id,
            "status": "failure",
            "error": (result.get("stderr") or "Container failed to produce output")[:1000],
            "execution_time_seconds": execution_time,
        }

    # ------------------------------------------------------------------
    def cleanup(self, task_id: Optional[str] = None) -> None:
        self.executor.cleanup(task_id)
        if task_id:
            task_dir = self.work_dir / sanitize_task_id(task_id)
            if task_dir.exists():
                shutil.rmtree(task_dir)
        else:
            if self.work_dir.exists():
                shutil.rmtree(self.work_dir)
                self.work_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    def get_container_stats(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Works now because the executor sets --label task_id=..."""
        try:
            r = subprocess.run(
                ["docker", "ps", "-q", "-f", f"label=task_id={task_id}"],
                capture_output=True, text=True,
            )
            cid = r.stdout.strip()
            if not cid:
                return None
            s = subprocess.run(
                ["docker", "stats", cid, "--no-stream", "--format", "{{json .}}"],
                capture_output=True, text=True,
            )
            if s.returncode == 0 and s.stdout.strip():
                return json.loads(s.stdout)
        except Exception as exc:  # noqa: BLE001
            logger.debug("stats failed for %s: %s", task_id, exc)
        return None


# ============================================================================
# SECTION 10 — Scaffolding helpers (unchanged behavior, preserved API)
# ============================================================================

def create_dockerfile(output_path: Path = Path("Dockerfile.agent")) -> None:
    """Create Dockerfile for AgentBeats-compliant agent (unchanged)."""
    dockerfile_content = """# AgentBeats-Compliant Agent Container
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY agent/ ./agent/
COPY config/ ./config/
COPY core/a2a_gateway.py ./core/
COPY core/rlhf_feedback_engine.py ./core/
COPY core/green_metrics.py ./core/
COPY docker_entrypoint.py .
ENV PYTHONUNBUFFERED=1
ENV A2A_VERSION=1.1
ENTRYPOINT ["python", "docker_entrypoint.py"]
"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(dockerfile_content)
    logger.info("Dockerfile created: %s", output_path)


def create_entrypoint(output_path: Path = Path("docker_entrypoint.py")) -> None:
    """Create Docker entrypoint script (unchanged)."""
    entrypoint_content = """#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Docker Entrypoint for AgentBeats Agent
Reads A2A input, executes agent, writes A2A output
'''

import os
import json
import sys
from pathlib import Path

from core.a2a_gateway import A2AGateway
from core.rlhf_feedback_engine import RLHFFeedbackEngine
from core.green_metrics import GreenMetricsCollector
from agent.research_agent import ResearchAgent


def main():
    input_file = os.getenv('A2A_INPUT', '/app/input.json')
    output_file = os.getenv('A2A_OUTPUT', '/app/output.json')

    try:
        with open(input_file, 'r') as f:
            task_request = json.load(f)

        gateway = A2AGateway()
        rlhf_engine = RLHFFeedbackEngine()
        green_metrics = GreenMetricsCollector()

        validated_request = gateway.validate_request(task_request)
        green_metrics.start_collection()

        agent = ResearchAgent()
        result = agent.execute(validated_request.input_data)

        metrics = green_metrics.stop_collection()
        feedback = rlhf_engine.analyze_reasoning_trace(
            reasoning_trace=result.get('reasoning_trace', []),
            task_type=validated_request.task_type,
            execution_time=result.get('execution_time', 0),
            success=result.get('success', False)
        )

        response = gateway.create_success_response(
            task_id=validated_request.task_id,
            output=result.get('output', {}),
            execution_time=result.get('execution_time', 0),
            green_metrics=metrics,
            reasoning_trace=result.get('reasoning_trace', []),
            metadata={'rlhf_feedback': feedback}
        )

    except Exception as e:
        response = gateway.create_failure_response(
            task_id=task_request.get('task_id', 'unknown'),
            status='failure',
            error_message=str(e)
        )

    with open(output_file, 'w') as f:
        json.dump(response.to_dict(), f, indent=2)

    sys.exit(0 if response.status.value == 'success' else 1)


if __name__ == '__main__':
    main()
"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(entrypoint_content)
    output_path.chmod(0o755)
    logger.info("Entrypoint created: %s", output_path)


# ============================================================================
# SECTION 11 — CLI / demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = OrchestratorConfig(
        use_enhancements=True,
        use_distillation=True,
        seed=42,
        model_path=Path("./models/distill_v1.json"),
    )
    orch = DockerOrchestrator(work_dir=Path("./work"), orchestrator_config=cfg)

    container = ContainerConfig(
        image="limit-graph-agent:latest",
        timeout_seconds=300,
    )

    resp = orch.execute_task(
        task_request={
            "task_id": "t-001",
            "task_type": "research",
            "priority": 5,
            "input_data": {"q": "hello"},
        },
        config=container,
        graph_metrics={"centrality": 0.7, "connectivity": 0.6},
        human_feedback_score=0.8,
    )
    print(json.dumps(resp, indent=2))

    if orch._optimizer is not None:
        orch._optimizer.save(Path("./models/distill_v1.json"))
