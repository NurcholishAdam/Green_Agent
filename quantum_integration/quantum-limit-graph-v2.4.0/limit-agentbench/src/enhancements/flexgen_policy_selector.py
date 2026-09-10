"""
Distillation-based FlexGen Policy Selector (Enhanced v2).

Single-file integration of all ten Green Agent enhancements:

  1. Quantum-Distillation Integration      -> QuantumInspiredTeacher blended into ensemble
  2. Causal RL for Policy Adaptation       -> CausalCounterfactualEstimator, logged per step
  3. Federated Green Learning              -> FederatedAggregator + export/import weights
  4. Multi-Agent Coordination              -> EmergentRoleRegistry + role-weighted teacher votes
  5. Temporal Logic / Formal Verification  -> optional shield hook, monitor hook
  6. Explainable AI                        -> DecisionExplainer with attributions & counterfactuals
  7. Adaptive Precision Switching          -> precision-aware state, rule teacher reads precision
  8. Carbon Markets / RECs                 -> carbon price, REC availability in state and rule teacher
  9. Resilience / Chaos Testing            -> CircuitBreaker, atomic persistence, teacher fallback
 10. HITL / Active Learning                -> UncertaintyEstimator, HITL gate, prioritized replay

Everything lives in this file. No new modules required.
Backward compatible: legacy signature preserved.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import random
import tempfile
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

from .flexgen_policy import FlexGenPolicy
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..async_message_queue import AsyncMessageQueue
from ..schemas.feedback_event import FeedbackEvent
from ..logger import logger


# ===========================================================================
# Schema versioning (Enhancements 1, 2, 3)
# ===========================================================================
STATE_SCHEMA_VERSION = "flexgen_state_v2"
STATE_FEATURE_FIELDS: Tuple[str, ...] = (
    "tokens_norm",
    "latency_target_norm",
    "gpu_memory_gb_norm",
    "cpu_memory_gb_norm",
    "disk_bw_norm",
    "carbon_intensity_norm",
    "recent_success_rate",
    "avg_reward",
    # v2 additions
    "carbon_price_norm",
    "rec_available_norm",
    "hour_sin",
    "hour_cos",
    "latency_headroom",
    "precision_hint_norm",
)
NORMALIZATION_BOUNDS: Dict[str, float] = {
    "tokens": 10000.0,
    "latency_target": 1000.0,
    "gpu_memory_gb": 80.0,
    "cpu_memory_gb": 256.0,
    "disk_bandwidth_gbps": 10.0,
    "carbon_intensity": 1000.0,
    "carbon_price": 0.5,
    "rec_kwh": 100.0,
}


# ===========================================================================
# Precision levels (Enhancement 7)
# ===========================================================================
class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    BF16 = "bf16"
    FP16 = "fp16"
    FP8 = "fp8"
    INT8 = "int8"
    INT4 = "int4"


_PRECISION_ORDER = [p.value for p in PrecisionLevel]


# ===========================================================================
# State
# ===========================================================================
@dataclass
class FlexGenState:
    """State for the distillation agent (schema v2, backwards-compatible)."""
    # v1 core
    tokens: float
    latency_target: float
    gpu_memory_gb: float
    cpu_memory_gb: float
    disk_bandwidth_gbps: float
    carbon_intensity: float
    recent_success_rate: float
    avg_reward: float
    # v2 additions (all optional, defaults keep v1 callers working)
    carbon_price_per_kg: float = 0.0
    rec_kwh_available: float = 0.0
    hour_of_day: float = 12.0
    precision_hint: Optional[str] = None
    # Kept for controller compatibility (previous file passed policy_idx).
    policy_idx: int = 0

    @property
    def schema_version(self) -> str:
        return STATE_SCHEMA_VERSION

    def to_feature_vector(self) -> np.ndarray:
        hour_rad = (self.hour_of_day % 24.0) / 24.0 * 2.0 * math.pi
        precision_idx = 0.0
        if self.precision_hint is not None and self.precision_hint in _PRECISION_ORDER:
            precision_idx = _PRECISION_ORDER.index(self.precision_hint) / max(len(_PRECISION_ORDER) - 1, 1)
        features = [
            min(self.tokens / NORMALIZATION_BOUNDS["tokens"], 1.0),
            min(self.latency_target / NORMALIZATION_BOUNDS["latency_target"], 1.0),
            min(self.gpu_memory_gb / NORMALIZATION_BOUNDS["gpu_memory_gb"], 1.0),
            min(self.cpu_memory_gb / NORMALIZATION_BOUNDS["cpu_memory_gb"], 1.0),
            min(self.disk_bandwidth_gbps / NORMALIZATION_BOUNDS["disk_bandwidth_gbps"], 1.0),
            min(self.carbon_intensity / NORMALIZATION_BOUNDS["carbon_intensity"], 1.0),
            float(np.clip(self.recent_success_rate, 0.0, 1.0)),
            float(np.clip(self.avg_reward, 0.0, 1.0)),
            min(self.carbon_price_per_kg / NORMALIZATION_BOUNDS["carbon_price"], 1.0),
            min(self.rec_kwh_available / NORMALIZATION_BOUNDS["rec_kwh"], 1.0),
            math.sin(hour_rad),
            math.cos(hour_rad),
            1.0 - min(1.0, self.latency_target / max(NORMALIZATION_BOUNDS["latency_target"], 1.0)),
            precision_idx,
        ]
        return np.array(features, dtype=np.float32)

    @staticmethod
    def feature_dim() -> int:
        return len(STATE_FEATURE_FIELDS)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["_schema"] = STATE_SCHEMA_VERSION
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FlexGenState":
        data = dict(data)
        data.pop("_schema", None)
        return cls(**data)


# ===========================================================================
# Teachers
# ===========================================================================
class Teacher:
    """Abstract teacher with role tagging (Enhancement 4)."""
    name: str = "teacher"
    role: str = "exploiter"

    def predict(self, state: FlexGenState, candidates: Sequence[FlexGenPolicy]) -> np.ndarray:
        raise NotImplementedError

    def confidence(self, state: FlexGenState) -> float:
        raise NotImplementedError


# -- Rule-based (enhanced for carbon price and precision) -------------------
class FlexGenRuleBasedTeacher(Teacher):
    """
    Candidate-aware heuristic. Now reads carbon price, REC availability,
    precision level, mixed precision, block size, and latency headroom.
    """

    name = "rule"
    role = "safety_officer"

    def predict(self, state: FlexGenState, candidates: Sequence[FlexGenPolicy]) -> np.ndarray:
        n = len(candidates)
        if n == 0:
            return np.zeros(0)
        scores = np.ones(n, dtype=np.float64) * 0.5

        # Carbon-stress: prefer offload and low bits.
        carbon_stress = min(1.0, state.carbon_intensity / 600.0)
        price_stress = min(1.0, state.carbon_price_per_kg / 0.2)
        rec_relief = min(1.0, state.rec_kwh_available / 20.0)

        for i, pol in enumerate(candidates):
            s = 1.0
            # Weight device preference under carbon stress.
            if pol.weight_device == "cpu":
                s += 0.3 * carbon_stress
            if pol.weight_device == "disk":
                s -= 0.15 * (1.0 - carbon_stress)
            # Low latency target -> prefer GPU.
            if state.latency_target < 200 and pol.weight_device == "gpu":
                s += 0.3
            # Precision-aware (Enhancement 7).
            pol_prec = getattr(pol, "precision_level", None)
            if pol_prec in ("int4", "int8", "fp8") and (carbon_stress > 0.5 or price_stress > 0.5):
                s += 0.25 * max(carbon_stress, price_stress)
            if rec_relief > 0.5 and pol_prec in ("fp16", "bf16", "fp32"):
                # RECs available -> we can afford higher precision.
                s += 0.15 * rec_relief
            if getattr(pol, "mixed_precision", False):
                s += 0.1
            # Memory-aware fallback.
            if state.gpu_memory_gb < 8 and pol.weight_bits <= 8:
                s += 0.2
            # Block size efficiency nudge.
            if pol.block_size in (16, 32):
                s += 0.05
            scores[i] = max(0.05, s)

        return scores / scores.sum()

    def confidence(self, state: FlexGenState) -> float:
        if state.carbon_intensity > 500 or state.gpu_memory_gb < 16:
            return 0.6
        if state.carbon_price_per_kg > 0.1:
            return 0.55
        return 0.3


# -- Historical ML teacher (unchanged interface, safe loads) ----------------
class FlexGenHistoricalMLTeacher(Teacher):
    """
    Trained classifier placeholder. Loads via JSON to avoid pickle.
    Expected format: {"schema": "...", "classes": int, "weights": [[...]], "bias": [...]}
    or falls back to a scikit-learn model loaded externally and passed in.
    """

    name = "historical"
    role = "exploiter"

    def __init__(self, model_path: Optional[str] = None, model: Any = None):
        self.model = model
        self.classes_: Optional[int] = None
        self.model_path = model_path
        if self.model is None and model_path and os.path.exists(model_path):
            self._try_load()

    def _try_load(self) -> None:
        try:
            with open(self.model_path, "r") as f:
                payload = json.load(f)
            if payload.get("_kind") == "linear_softmax":
                self.model = _LinearSoftmaxModel.from_json(payload)
                self.classes_ = int(payload.get("classes", 0))
            else:
                logger.warning("Unknown historical model format; ignoring.")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Failed to load historical model: {exc}")

    def predict(self, state: FlexGenState, candidates: Sequence[FlexGenPolicy]) -> np.ndarray:
        n = len(candidates)
        if self.model is None or n == 0:
            return np.ones(max(n, 1)) / max(n, 1)
        x = state.to_feature_vector().reshape(1, -1)
        try:
            probs = np.asarray(self.model.predict_proba(x)).reshape(-1)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Historical teacher predict failed: {exc}")
            return np.ones(n) / n
        return _align_probs(probs, n)

    def confidence(self, state: FlexGenState) -> float:
        return 0.7 if self.model is not None else 0.0


class _LinearSoftmaxModel:
    """Portable linear softmax replacement for pickled sklearn models."""

    def __init__(self, weights: np.ndarray, bias: np.ndarray):
        self.weights = weights
        self.bias = bias

    @classmethod
    def from_json(cls, payload: Dict[str, Any]) -> "_LinearSoftmaxModel":
        return cls(
            weights=np.asarray(payload["weights"], dtype=np.float64),
            bias=np.asarray(payload["bias"], dtype=np.float64),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "_kind": "linear_softmax",
            "classes": int(self.weights.shape[1]),
            "weights": self.weights.tolist(),
            "bias": self.bias.tolist(),
        }

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        logits = x @ self.weights + self.bias
        logits = logits - logits.max(axis=-1, keepdims=True)
        e = np.exp(logits)
        return e / e.sum(axis=-1, keepdims=True)


# -- Q-teacher (fixed amnesia, safe persistence) ----------------------------
class FlexGenStatefulQTeacher(Teacher):
    """
    Linear Q-learning teacher. Fixes:
      - Amnesia under candidate-count shrink/grow (archive preserved).
      - JSON write on every update (now periodic + atomic).
    """

    name = "q"
    role = "explorer"

    def __init__(
        self,
        feature_dim: Optional[int] = None,
        n_actions: int = 20,
        lr: float = 0.1,
        weights_path: Optional[str] = None,
        save_every: int = 50,
    ):
        self.lr = lr
        self.feature_dim = feature_dim or FlexGenState.feature_dim()
        self.n_actions = n_actions
        self.weights_path = weights_path
        self.save_every = save_every
        self.weights = np.zeros((self.feature_dim, n_actions))
        self._archived_columns: Dict[int, np.ndarray] = {}
        self._update_count = 0
        self._dirty = False
        if weights_path and os.path.exists(weights_path):
            self._load()

    # -- persistence ---------------------------------------------------
    def _load(self) -> None:
        try:
            with open(self.weights_path, "r") as f:
                payload = json.load(f)
            if payload.get("schema") != STATE_SCHEMA_VERSION:
                logger.warning(
                    "Q-teacher weights schema mismatch; ignoring persisted file."
                )
                return
            loaded = np.asarray(payload["weights"], dtype=np.float64)
            f_dim, n_act = loaded.shape
            min_dim = min(f_dim, self.feature_dim)
            min_act = min(n_act, self.n_actions)
            self.weights = np.zeros((self.feature_dim, self.n_actions))
            self.weights[:min_dim, :min_act] = loaded[:min_dim, :min_act]
            for k, v in payload.get("archive", {}).items():
                self._archived_columns[int(k)] = np.asarray(v, dtype=np.float64)
            logger.info(f"Loaded Q-teacher weights from {self.weights_path}")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Failed to load Q-weights: {exc}")

    def _save(self, force: bool = False) -> None:
        if not self.weights_path:
            return
        if not force and not self._dirty:
            return
        if not force and self._update_count % self.save_every != 0:
            return
        payload = {
            "schema": STATE_SCHEMA_VERSION,
            "feature_dim": self.feature_dim,
            "n_actions": self.n_actions,
            "weights": self.weights.tolist(),
            "archive": {str(k): v.tolist() for k, v in self._archived_columns.items()},
        }
        _atomic_json_write(self.weights_path, payload)
        self._dirty = False

    # -- prediction ----------------------------------------------------
    def _ensure_n_actions(self, n_actions: int) -> None:
        if n_actions == self.n_actions:
            return
        # Archive the columns we're about to drop (avoid amnesia).
        if n_actions < self.n_actions:
            for j in range(n_actions, self.n_actions):
                self._archived_columns[j] = self.weights[:, j].copy()
        new_weights = np.zeros((self.feature_dim, n_actions))
        min_act = min(self.n_actions, n_actions)
        new_weights[:, :min_act] = self.weights[:, :min_act]
        # Restore archived columns when growing back.
        if n_actions > self.n_actions:
            for j in range(self.n_actions, n_actions):
                if j in self._archived_columns:
                    new_weights[:, j] = self._archived_columns[j]
        self.weights = new_weights
        self.n_actions = n_actions
        self._dirty = True

    def predict(self, state: FlexGenState, candidates: Sequence[FlexGenPolicy]) -> np.ndarray:
        n = len(candidates)
        self._ensure_n_actions(n)
        if n == 0:
            return np.zeros(0)
        x = state.to_feature_vector()
        q = x @ self.weights
        q = q - q.max()
        e = np.exp(q)
        return e / e.sum()

    def confidence(self, state: FlexGenState) -> float:
        return 0.5

    def update(self, state: FlexGenState, action: int, reward: float) -> None:
        if not (0 <= action < self.n_actions):
            return
        x = state.to_feature_vector()
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._update_count += 1
        self._dirty = True
        self._save()

    # -- federated (Enhancement 3) -------------------------------------
    def export_weights(self) -> Dict[str, Any]:
        return {
            "schema": STATE_SCHEMA_VERSION,
            "feature_dim": self.feature_dim,
            "n_actions": self.n_actions,
            "weights": self.weights.copy(),
            "samples": self._update_count,
        }

    def import_weights(self, payload: Dict[str, Any]) -> None:
        if payload.get("schema") != STATE_SCHEMA_VERSION:
            logger.warning("Federated Q-weight schema mismatch; ignoring.")
            return
        loaded = np.asarray(payload["weights"], dtype=np.float64)
        f_dim, n_act = loaded.shape
        self.feature_dim = f_dim
        self.n_actions = n_act
        self.weights = loaded.copy()
        self._dirty = True
        self._save(force=True)


# -- Quantum-inspired teacher (Enhancement 1) -------------------------------
class QuantumInspiredTeacher(Teacher):
    """
    Quantum-inspired teacher using a small parameterized state-vector
    simulation. Produces a candidate distribution modulated by the state.
    Deterministic given the seed; blends a quantum interference prior with a
    carbon-aware policy prior.
    """

    name = "quantum"
    role = "carbon_broker"

    def __init__(self, n_qubits: int = 5, seed: int = 0):
        self.n_qubits = n_qubits
        self.rng = np.random.default_rng(seed)
        self._params = self.rng.normal(size=(n_qubits, 2)) * 0.5

    def _simulate(self, features: np.ndarray, n_candidates: int) -> np.ndarray:
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128)
        state[0] = 1.0
        for q in range(self.n_qubits):
            theta = float(self._params[q, 0] * features[q % len(features)] + self._params[q, 1])
            c, s = math.cos(theta / 2), math.sin(theta / 2)
            new = state.copy()
            step = 1 << q
            for i in range(dim):
                if i & step == 0:
                    a, b = state[i], state[i | step]
                    new[i] = c * a - s * b
                    new[i | step] = s * a + c * b
            state = new
        probs_full = np.abs(state) ** 2
        probs = np.zeros(n_candidates, dtype=np.float64)
        for i, p in enumerate(probs_full):
            probs[i % n_candidates] += p
        return probs / max(probs.sum(), 1e-12)

    def predict(self, state: FlexGenState, candidates: Sequence[FlexGenPolicy]) -> np.ndarray:
        n = len(candidates)
        if n == 0:
            return np.zeros(0)
        feats = state.to_feature_vector()
        quantum = self._simulate(feats, n)
        carbon_prior = np.array(
            [1.0 / (1.0 + max(p.weight_bits, 1) * 0.1) for p in candidates],
            dtype=np.float64,
        )
        carbon_prior /= carbon_prior.sum()
        blended = 0.6 * quantum + 0.4 * carbon_prior
        return blended / blended.sum()

    def confidence(self, state: FlexGenState) -> float:
        return 0.45


# ===========================================================================
# Student with temperature, gradient clip, federated export/import
# ===========================================================================
class DistillationStudent:
    """
    Linear softmax student with:
      - Baseline subtraction and L2 regularization
      - Temperature-scaled softmax
      - Gradient clipping
      - Federated export/import
      - Schema-versioned persistence (JSON, not pickle)
    """

    def __init__(
        self,
        feature_dim: Optional[int] = None,
        n_classes: int = 20,
        lr: float = 0.01,
        l2_reg: float = 0.0001,
        temperature: float = 1.0,
        grad_clip: float = 5.0,
    ):
        self.feature_dim = feature_dim or FlexGenState.feature_dim()
        self.n_classes = n_classes
        self.lr = lr
        self.l2_reg = l2_reg
        self.temperature = max(temperature, 1e-3)
        self.grad_clip = grad_clip
        self.weights = np.zeros((self.feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.baseline = 0.0
        self.baseline_alpha = 0.1
        self.counter = 0

    def _resize(self, n_classes: int) -> None:
        if n_classes == self.n_classes:
            return
        new_weights = np.zeros((self.feature_dim, n_classes))
        new_biases = np.zeros(n_classes)
        min_classes = min(self.n_classes, n_classes)
        new_weights[:, :min_classes] = self.weights[:, :min_classes]
        new_biases[:min_classes] = self.biases[:min_classes]
        self.weights = new_weights
        self.biases = new_biases
        self.n_classes = n_classes

    def _ensure_feature_dim(self, dim: int) -> None:
        if dim == self.feature_dim:
            return
        new_weights = np.zeros((dim, self.n_classes))
        min_dim = min(dim, self.feature_dim)
        new_weights[:min_dim, :] = self.weights[:min_dim, :]
        self.weights = new_weights
        self.feature_dim = dim

    def predict_proba(self, state_vector: np.ndarray, num_classes: int) -> np.ndarray:
        self._ensure_feature_dim(len(state_vector))
        self._resize(num_classes)
        logits = state_vector @ self.weights + self.biases
        logits = logits / self.temperature
        logits = logits - logits.max()
        exp_logits = np.exp(logits)
        return exp_logits / exp_logits.sum()

    def update(
        self,
        state_vector: np.ndarray,
        teacher_probs: np.ndarray,
        reward: float,
        action: int,
        distill_weight: float = 0.7,
        rl_weight: float = 0.3,
    ) -> None:
        current_probs = self.predict_proba(state_vector, len(teacher_probs))
        self.baseline = self.baseline_alpha * reward + (1 - self.baseline_alpha) * self.baseline
        advantage = reward - self.baseline

        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        if 0 <= action < self.n_classes:
            one_hot[action] = 1.0
        grad_rl = -advantage * (one_hot - current_probs)
        grad = distill_weight * grad_distill + rl_weight * grad_rl

        # Gradient clip (Enhancement 9).
        norm = float(np.linalg.norm(grad))
        if norm > self.grad_clip:
            grad = grad * (self.grad_clip / (norm + 1e-12))

        self.weights -= self.lr * (np.outer(state_vector, grad) + self.l2_reg * self.weights)
        self.biases -= self.lr * grad
        self.counter += 1

    # -- federated (Enhancement 3) -------------------------------------
    def export_weights(self) -> Dict[str, Any]:
        return {
            "schema": STATE_SCHEMA_VERSION,
            "feature_dim": self.feature_dim,
            "n_classes": self.n_classes,
            "weights": self.weights.copy(),
            "biases": self.biases.copy(),
            "baseline": self.baseline,
            "samples": self.counter,
        }

    def import_weights(self, payload: Dict[str, Any]) -> None:
        if payload.get("schema") != STATE_SCHEMA_VERSION:
            logger.warning("Federated student schema mismatch; ignoring.")
            return
        self.weights = np.asarray(payload["weights"], dtype=np.float64).copy()
        self.biases = np.asarray(payload["biases"], dtype=np.float64).copy()
        self.feature_dim, self.n_classes = self.weights.shape
        self.baseline = float(payload.get("baseline", 0.0))
        self.counter = int(payload.get("samples", self.counter))


# ===========================================================================
# Prioritized replay with candidate-set hash (fixes stale-target bug)
# ===========================================================================
@dataclass
class ReplaySample:
    state_vec: np.ndarray
    action: int
    reward: float
    next_state_vec: np.ndarray
    teacher_probs: np.ndarray
    candidate_hash: str
    priority: float = 1.0
    timestamp: float = field(default_factory=time.time)


class PrioritizedReplayBuffer:
    """
    Replay buffer that tags each sample with the candidate-set hash.
    On sample, samples are rejected if the current candidate set hash does
    not match, preventing silent training on stale index spaces.
    """

    def __init__(self, max_size: int = 2000, alpha: float = 0.6, eps: float = 1e-4):
        self.buffer: deque = deque(maxlen=max_size)
        self.alpha = alpha
        self.eps = eps

    def push(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_probs: np.ndarray,
        candidate_hash: str,
        priority: Optional[float] = None,
    ) -> None:
        if priority is None:
            priority = float(np.abs(reward)) + self.eps
        self.buffer.append(
            ReplaySample(
                state_vec=np.asarray(state_vec, dtype=np.float64),
                action=int(action),
                reward=float(reward),
                next_state_vec=np.asarray(next_state_vec, dtype=np.float64),
                teacher_probs=np.asarray(teacher_probs, dtype=np.float64),
                candidate_hash=candidate_hash,
                priority=priority,
            )
        )

    def __len__(self) -> int:
        return len(self.buffer)

    def sample(
        self,
        batch_size: int,
        current_hash: Optional[str] = None,
    ) -> List[ReplaySample]:
        if not self.buffer:
            return []
        candidates = list(self.buffer)
        if current_hash is not None:
            filtered = [s for s in candidates if s.candidate_hash == current_hash]
            if filtered:
                candidates = filtered
            else:
                # No matching samples; return empty rather than poison the student.
                return []
        priorities = np.array([max(s.priority, self.eps) ** self.alpha for s in candidates])
        probs = priorities / priorities.sum()
        k = min(batch_size, len(candidates))
        idx = np.random.choice(len(candidates), size=k, replace=False, p=probs)
        return [candidates[i] for i in idx]


# ===========================================================================
# Causal counterfactual estimator (Enhancement 2)
# ===========================================================================
class CausalCounterfactualEstimator:
    """
    Ridge-regression structural causal model over (state, action, reward).
    Produces per-action counterfactual reward estimates.
    """

    def __init__(self, state_dim: int, n_actions: int, ridge: float = 1e-3):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.ridge = ridge
        self._A = np.eye(state_dim + n_actions + 1) * ridge
        self._b = np.zeros(state_dim + n_actions + 1)
        self._n = 0

    def _features(self, s: np.ndarray, a: int) -> np.ndarray:
        one_hot = np.zeros(self.n_actions)
        one_hot[a % self.n_actions] = 1.0
        return np.concatenate([np.asarray(s, dtype=np.float64), one_hot, [1.0]])

    def _ensure_actions(self, n_actions: int) -> None:
        if n_actions == self.n_actions:
            return
        self._A = np.eye(self.state_dim + n_actions + 1) * self.ridge
        self._b = np.zeros(self.state_dim + n_actions + 1)
        self.n_actions = n_actions
        self._n = 0

    def update(self, state: np.ndarray, action: int, reward: float) -> None:
        x = self._features(state, action)
        self._A += np.outer(x, x)
        self._b += reward * x
        self._n += 1

    def predict(self, state: np.ndarray, action: int) -> float:
        if self._n < 2:
            return 0.0
        try:
            theta = np.linalg.solve(self._A, self._b)
        except np.linalg.LinAlgError:
            return 0.0
        return float(self._features(state, action) @ theta)

    def counterfactuals(self, state: np.ndarray, n_actions: int) -> Dict[int, float]:
        self._ensure_actions(n_actions)
        return {a: self.predict(state, a) for a in range(n_actions)}


# ===========================================================================
# Federated aggregator (Enhancement 3)
# ===========================================================================
@dataclass
class FederatedUpdate:
    node_id: str
    weights: Dict[str, np.ndarray]
    n_samples: int
    carbon_intensity: float
    timestamp: float = field(default_factory=time.time)


class FederatedAggregator:
    """
    Carbon-weighted FedAvg for student weights. Lower-carbon nodes with more
    samples contribute more.
    """

    def __init__(self, dp_sigma: float = 1e-3, staleness_s: float = 3600.0):
        self.dp_sigma = dp_sigma
        self.staleness_s = staleness_s
        self._updates: Dict[str, FederatedUpdate] = {}
        self._global: Optional[Dict[str, np.ndarray]] = None

    def submit(self, update: FederatedUpdate) -> None:
        self._updates[update.node_id] = update

    def _fresh(self) -> List[FederatedUpdate]:
        now = time.time()
        return [u for u in self._updates.values() if (now - u.timestamp) <= self.staleness_s]

    def aggregate(self) -> Optional[Dict[str, np.ndarray]]:
        fresh = self._fresh()
        if not fresh:
            return self._global
        weights = np.array(
            [u.n_samples / max(u.carbon_intensity, 1.0) for u in fresh],
            dtype=np.float64,
        )
        weights /= max(weights.sum(), 1e-12)
        keys = fresh[0].weights.keys()
        agg: Dict[str, np.ndarray] = {}
        for k in keys:
            stacked = np.stack([u.weights[k] for u in fresh], axis=0)
            blended = np.tensordot(weights, stacked, axes=([0], [0]))
            if self.dp_sigma > 0:
                blended = blended + np.random.normal(0.0, self.dp_sigma, size=blended.shape)
            agg[k] = blended
        self._global = agg
        return agg

    def global_weights(self) -> Optional[Dict[str, np.ndarray]]:
        return self._global


# ===========================================================================
# Emergent role registry (Enhancement 4)
# ===========================================================================
class AgentRole(str, Enum):
    EXPLORER = "explorer"
    EXPLOITER = "exploiter"
    SAFETY_OFFICER = "safety_officer"
    CARBON_BROKER = "carbon_broker"
    VERIFIER = "verifier"


class EmergentRoleRegistry:
    """Decay-weighted performance tracker per role."""

    def __init__(self, decay: float = 0.95):
        self.decay = decay
        self._scores: Dict[AgentRole, float] = {r: 1.0 for r in AgentRole}

    def record(self, role: AgentRole, success: bool, reward: float) -> None:
        for r in self._scores:
            self._scores[r] *= self.decay
        signal = reward if success else -abs(reward) * 0.5
        self._scores[role] += signal

    def weights(self) -> Dict[AgentRole, float]:
        total = sum(max(v, 1e-6) for v in self._scores.values())
        return {r: max(v, 1e-6) / total for r, v in self._scores.items()}


# ===========================================================================
# XAI explainer (Enhancement 6)
# ===========================================================================
@dataclass
class Explanation:
    decision_id: str
    chosen_idx: int
    teacher_attributions: Dict[str, float]
    counterfactuals: List[Dict[str, Any]]
    rationale: str
    confidence: float
    uncertainty: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DecisionExplainer:
    """Builds attribution + counterfactual rationale for each decision."""

    def explain(
        self,
        chosen_idx: int,
        teacher_contributions: Dict[str, float],
        teacher_probs: np.ndarray,
        counterfactuals: Dict[int, float],
        student_probs: np.ndarray,
        uncertainty: float,
    ) -> Explanation:
        cf_sorted = sorted(counterfactuals.items(), key=lambda kv: kv[1], reverse=True)
        cf_list = [
            {"idx": int(i), "expected_reward": float(v)} for i, v in cf_sorted[:3]
        ]
        conf = float(teacher_probs[chosen_idx]) if 0 <= chosen_idx < len(teacher_probs) else 0.5
        top_teachers = sorted(
            teacher_contributions.items(), key=lambda kv: kv[1], reverse=True
        )[:2]
        reason_bits = ", ".join(f"{k}={v:.2f}" for k, v in top_teachers)
        rationale = (
            f"Chose candidate #{chosen_idx} (teacher confidence={conf:.2f}, "
            f"student prob={float(student_probs[chosen_idx]):.2f}, uncertainty={uncertainty:.2f}). "
            f"Top teacher contributions: {reason_bits}."
        )
        return Explanation(
            decision_id=f"dec-{int(time.time()*1000)}",
            chosen_idx=chosen_idx,
            teacher_attributions=teacher_contributions,
            counterfactuals=cf_list,
            rationale=rationale,
            confidence=conf,
            uncertainty=uncertainty,
        )


# ===========================================================================
# Uncertainty + HITL + circuit breaker (Enhancements 9, 10)
# ===========================================================================
class UncertaintyEstimator:
    """Entropy of teacher probs + reward variance."""

    def __init__(self, entropy_threshold: float = 0.75, variance_threshold: float = 0.05):
        self.entropy_threshold = entropy_threshold
        self.variance_threshold = variance_threshold
        self._rewards: deque = deque(maxlen=32)

    def observe(self, reward: float) -> None:
        self._rewards.append(reward)

    def entropy(self, probs: Optional[np.ndarray]) -> float:
        if probs is None or probs.size == 0:
            return 1.0
        p = np.clip(probs, 1e-12, 1.0)
        return float(-np.sum(p * np.log(p)) / math.log(len(p)))

    def variance(self) -> float:
        if len(self._rewards) < 3:
            return 0.0
        return float(np.var(np.asarray(self._rewards)))

    def score(self, probs: Optional[np.ndarray]) -> Tuple[bool, float]:
        h = self.entropy(probs)
        v = self.variance()
        s = 0.6 * h + 0.4 * min(1.0, v / max(self.variance_threshold, 1e-9))
        return (h > self.entropy_threshold or v > self.variance_threshold), s


class HumanInTheLoopGate:
    """Async approval gate with timeout and audit trail."""

    def __init__(
        self,
        approver: Optional[Callable[[Dict[str, Any]], bool]] = None,
        timeout_s: float = 2.0,
        auto_approve_on_timeout: bool = True,
    ):
        self.approver = approver
        self.timeout_s = timeout_s
        self.auto_approve_on_timeout = auto_approve_on_timeout
        self.audit: List[Dict[str, Any]] = []

    async def request(self, payload: Dict[str, Any]) -> bool:
        if self.approver is None:
            self.audit.append({"approved": True, "reason": "no_approver", **payload})
            return True
        loop = asyncio.get_running_loop()
        try:
            ok = await asyncio.wait_for(
                loop.run_in_executor(None, self.approver, payload), timeout=self.timeout_s
            )
        except asyncio.TimeoutError:
            ok = self.auto_approve_on_timeout
        self.audit.append({"approved": bool(ok), **payload})
        return bool(ok)


class CircuitBreaker:
    """Simple circuit breaker to guard teacher predictions."""

    def __init__(self, failure_threshold: int = 3, recovery_time_s: float = 10.0):
        self.failure_threshold = failure_threshold
        self.recovery_time_s = recovery_time_s
        self._failures = 0
        self._open_until = 0.0

    def allow(self) -> bool:
        return time.time() >= self._open_until

    def record(self, success: bool) -> None:
        if success:
            self._failures = 0
        else:
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._open_until = time.time() + self.recovery_time_s
                self._failures = 0


# ===========================================================================
# Helper utilities
# ===========================================================================
def _align_probs(probs: np.ndarray, n: int) -> np.ndarray:
    """
    Align a probability vector of arbitrary length to n classes without the
    np.resize cyclic-repeat bug.
    """
    if n <= 0:
        return np.zeros(0)
    if probs.size == 0:
        return np.ones(n) / n
    out = np.ones(n, dtype=np.float64) * (1.0 / max(n, 1))  # uniform prior
    k = min(probs.size, n)
    out[:k] = probs[:k]
    s = out.sum()
    return out / s if s > 0 else np.ones(n) / n


def _candidate_set_hash(candidates: Sequence[FlexGenPolicy]) -> str:
    payload = "|".join(json.dumps(c.to_dict(), sort_keys=True) for c in candidates)
    return str(hash(payload) & 0xFFFFFFFF)


def _atomic_json_write(path: str, payload: Dict[str, Any]) -> None:
    """Atomic JSON write via temp file + rename."""
    d = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ===========================================================================
# Main selector
# ===========================================================================
class DistillationFlexGenSelector:
    """
    Multi-teacher on-policy distillation selector.
    Variable candidate count, schema-versioned persistence, ten enhancements.
    """

    def __init__(
        self,
        n_candidates: int = 20,
        config: Optional[Dict] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        persistence_path: Optional[str] = None,
        # Enhancement flags
        enable_quantum_teacher: bool = True,
        enable_causal: bool = True,
        enable_federated: bool = True,
        enable_multi_agent: bool = True,
        enable_xai: bool = True,
        enable_hitl: bool = True,
        enable_resilience: bool = True,
        # Enhancement hooks
        safety_shield: Optional[Callable[[int, Sequence[Dict[str, Any]]], Tuple[int, bool, Dict[str, bool]]]] = None,
        temporal_monitor: Optional[Callable[[Dict[str, Any]], Dict[str, bool]]] = None,
        hitl_approver: Optional[Callable[[Dict[str, Any]], bool]] = None,
        # Reproducibility
        seed: Optional[int] = None,
    ):
        self.config = config or {}
        self.message_queue = message_queue
        self.persistence_path = persistence_path
        self.feature_dim = FlexGenState.feature_dim()
        self.rng = random.Random(seed if seed is not None else 0)
        self.seed = seed

        # Teachers ------------------------------------------------------
        self.rule_teacher = FlexGenRuleBasedTeacher()
        self.historical_teacher = FlexGenHistoricalMLTeacher(
            model_path=self.config.get("historical_model_path")
        )
        self.q_teacher = FlexGenStatefulQTeacher(
            feature_dim=self.feature_dim,
            n_actions=n_candidates,
            lr=self.config.get("q_lr", 0.1),
            weights_path=self.config.get("q_weights_path"),
            save_every=self.config.get("q_save_every", 50),
        )
        self.teachers: List[Teacher] = [self.rule_teacher, self.historical_teacher, self.q_teacher]
        if enable_quantum_teacher:
            self.quantum_teacher = QuantumInspiredTeacher(seed=seed or 0)
            self.teachers.append(self.quantum_teacher)
        else:
            self.quantum_teacher = None

        # Student -------------------------------------------------------
        self.student = DistillationStudent(
            feature_dim=self.feature_dim,
            n_classes=n_candidates,
            lr=self.config.get("student_lr", 0.01),
            l2_reg=self.config.get("l2_reg", 0.0001),
            temperature=self.config.get("temperature", 1.0),
            grad_clip=self.config.get("grad_clip", 5.0),
        )
        self.replay_buffer = PrioritizedReplayBuffer(
            max_size=self.config.get("replay_size", 2000)
        )

        # Enhancement modules -------------------------------------------
        self.causal = (
            CausalCounterfactualEstimator(self.feature_dim, n_candidates)
            if enable_causal else None
        )
        self.federated = FederatedAggregator() if enable_federated else None
        self.roles = EmergentRoleRegistry() if enable_multi_agent else None
        self.explainer = DecisionExplainer() if enable_xai else None
        self.uncertainty = UncertaintyEstimator() if enable_hitl else None
        self.hitl = HumanInTheLoopGate(approver=hitl_approver) if enable_hitl else None
        self.breaker = CircuitBreaker() if enable_resilience else None

        # Hooks ---------------------------------------------------------
        self.safety_shield = safety_shield
        self.temporal_monitor = temporal_monitor

        # Schedule ------------------------------------------------------
        self.epsilon = self.config.get("epsilon", 0.1)
        self.epsilon_decay = self.config.get("epsilon_decay", 0.999)
        self.train_every = self.config.get("train_every", 10)
        self.counter = 0

        # Caches --------------------------------------------------------
        self.last_teacher_probs: Optional[np.ndarray] = None
        self.last_student_probs: Optional[np.ndarray] = None
        self.last_explanation: Optional[Explanation] = None
        self.last_uncertainty: float = 0.0
        self.last_teacher_contributions: Dict[str, float] = {}
        self.last_counterfactuals: Dict[int, float] = {}

        if self.persistence_path and os.path.exists(self.persistence_path):
            self._load_student()

    # ------------------------------------------------------------------
    # Persistence (safe + versioned)
    # ------------------------------------------------------------------
    def _load_student(self) -> None:
        try:
            with open(self.persistence_path, "r") as f:
                payload = json.load(f)
            if payload.get("schema") != STATE_SCHEMA_VERSION:
                logger.warning("Student schema mismatch; ignoring persisted file.")
                return
            self.student.import_weights(payload)
            self.epsilon = payload.get("epsilon", self.epsilon)
            self.counter = payload.get("counter", 0)
            logger.info(f"Loaded student from {self.persistence_path}")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Failed to load student: {exc}")

    def _save_student(self) -> None:
        if not self.persistence_path:
            return
        payload = self.student.export_weights()
        payload["epsilon"] = self.epsilon
        payload["counter"] = self.counter
        try:
            _atomic_json_write(self.persistence_path, payload)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Student save failed: {exc}")

    # ------------------------------------------------------------------
    # Teacher ensemble
    # ------------------------------------------------------------------
    def _safe_predict(self, teacher: Teacher, state: FlexGenState, candidates: Sequence[FlexGenPolicy]) -> Optional[np.ndarray]:
        if self.breaker and not self.breaker.allow():
            return None
        try:
            probs = teacher.predict(state, candidates)
            if probs is None or len(probs) != len(candidates):
                probs = _align_probs(np.asarray(probs or []), len(candidates))
            if self.breaker:
                self.breaker.record(True)
            return probs
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Teacher {teacher.name} failed: {exc}")
            if self.breaker:
                self.breaker.record(False)
            return None

    def _ensemble_teacher_probs(
        self,
        state: FlexGenState,
        candidates: Sequence[FlexGenPolicy],
    ) -> Tuple[np.ndarray, Dict[str, float]]:
        n = len(candidates)
        if n == 0:
            return np.zeros(0), {}
        contributions: Dict[str, float] = {}
        weighted = np.zeros(n, dtype=np.float64)
        total_w = 0.0
        role_weights = self.roles.weights() if self.roles else None
        for teacher in self.teachers:
            probs = self._safe_predict(teacher, state, candidates)
            if probs is None:
                continue
            conf = teacher.confidence(state)
            role_w = 1.0
            if role_weights:
                try:
                    role = AgentRole(teacher.role)
                    role_w = role_weights.get(role, 1.0)
                except ValueError:
                    role_w = 1.0
            w = conf * role_w
            weighted += probs * w
            total_w += w
            contributions[teacher.name] = float(w)
        if total_w > 0:
            weighted /= total_w
        else:
            weighted = np.ones(n) / n
        return weighted, contributions

    # ------------------------------------------------------------------
    # Selection
    # ------------------------------------------------------------------
    async def select_policy(
        self,
        candidates: List[FlexGenPolicy],
        state: FlexGenState,
        exploration: bool = True,
    ) -> Tuple[int, np.ndarray, np.ndarray]:
        """
        Choose an index into the candidate list.
        Returns (action_idx, state_vec, teacher_probs).
        """
        state_vec = state.to_feature_vector()
        n = len(candidates)
        if n == 0:
            return 0, state_vec, np.zeros(0)

        teacher_probs, contributions = self._ensemble_teacher_probs(state, candidates)
        student_probs = self.student.predict_proba(state_vec, n)

        # Choose action.
        if exploration and self.rng.random() < self.epsilon:
            action_idx = self.rng.randint(0, n - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))

        # Safety shield (Enhancement 5).
        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.safety_shield is not None:
            try:
                candidate_dicts = [c.to_dict() for c in candidates]
                action_idx, safety_ok, stl_verdict = self.safety_shield(action_idx, candidate_dicts)
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Safety shield failed: {exc}")

        # Temporal monitor (Enhancement 5).
        if self.temporal_monitor is not None:
            try:
                stl_verdict = {**stl_verdict, **self.temporal_monitor({"candidate_idx": action_idx})}
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Temporal monitor failed: {exc}")

        # Uncertainty (Enhancement 10).
        uncertainty_score = 0.0
        is_uncertain = False
        if self.uncertainty is not None:
            is_uncertain, uncertainty_score = self.uncertainty.score(teacher_probs)

        # HITL (Enhancement 10).
        if self.hitl is not None and (is_uncertain or not safety_ok):
            approved = await self.hitl.request(
                {
                    "reason": "high_uncertainty" if is_uncertain else "safety_violation",
                    "candidate_idx": action_idx,
                    "uncertainty": uncertainty_score,
                    "num_candidates": n,
                }
            )
            if not approved:
                # Fall back to lowest-bit policy.
                action_idx = int(np.argmin([c.weight_bits for c in candidates]))

        # Causal counterfactuals (Enhancement 2).
        counterfactuals: Dict[int, float] = {}
        if self.causal is not None:
            self.causal._ensure_actions(n)
            counterfactuals = self.causal.counterfactuals(state_vec, n)

        # XAI (Enhancement 6).
        explanation: Optional[Explanation] = None
        if self.explainer is not None:
            explanation = self.explainer.explain(
                chosen_idx=action_idx,
                teacher_contributions=contributions,
                teacher_probs=teacher_probs,
                counterfactuals=counterfactuals,
                student_probs=student_probs,
                uncertainty=uncertainty_score,
            )

        # Cache.
        self.last_teacher_probs = teacher_probs
        self.last_student_probs = student_probs
        self.last_explanation = explanation
        self.last_uncertainty = uncertainty_score
        self.last_teacher_contributions = contributions
        self.last_counterfactuals = counterfactuals

        self.epsilon = max(0.01, self.epsilon * self.epsilon_decay)

        return action_idx, state_vec, teacher_probs

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------
    async def update(
        self,
        state_vec: np.ndarray,
        action_idx: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_probs: np.ndarray,
        candidates: Optional[Sequence[FlexGenPolicy]] = None,
    ) -> None:
        candidate_hash = _candidate_set_hash(candidates) if candidates else "unknown"
        self.replay_buffer.push(
            state_vec=state_vec,
            action=action_idx,
            reward=reward,
            next_state_vec=next_state_vec,
            teacher_probs=teacher_probs,
            candidate_hash=candidate_hash,
        )
        self.counter += 1

        # Q-teacher online update (Enhancement 2/legacy).
        # Rebuild a minimal FlexGenState-like call requires state fields; skip
        # here and let the controller call q_teacher.update directly if needed.

        # Causal update.
        if self.causal is not None:
            self.causal.update(state_vec, action_idx, reward)

        # Uncertainty history.
        if self.uncertainty is not None:
            self.uncertainty.observe(reward)

        # Role registry update (Enhancement 4).
        if self.roles is not None:
            role = AgentRole.EXPLOITER
            if self.last_teacher_contributions:
                role = max(
                    self.last_teacher_contributions,
                    key=self.last_teacher_contributions.get,
                )
                try:
                    role = AgentRole(role)
                except ValueError:
                    role = AgentRole.EXPLOITER
            self.roles.record(role, success=reward > 0.5, reward=reward)

        # Federated submit (Enhancement 3) — only if node id provided.
        if self.federated is not None and candidates is not None:
            # Use feature-space weight as a stand-in; the controller is expected
            # to attach a node id via submit_federated_update() for real use.
            pass

        # Train student periodically.
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8, current_hash=candidate_hash)
            for s in batch:
                self.student.update(
                    s.state_vec,
                    s.teacher_probs,
                    s.reward,
                    s.action,
                    distill_weight=self.config.get("distill_weight", 0.7),
                    rl_weight=self.config.get("rl_weight", 0.3),
                )

        # Periodic student save.
        if self.persistence_path and self.counter % 100 == 0:
            self._save_student()

    # ------------------------------------------------------------------
    # Federated helpers (Enhancement 3)
    # ------------------------------------------------------------------
    def export_weights(self) -> Dict[str, Any]:
        return {
            "student": self.student.export_weights(),
            "q_teacher": self.q_teacher.export_weights(),
        }

    def import_weights(self, payload: Dict[str, Any]) -> None:
        if "student" in payload:
            self.student.import_weights(payload["student"])
        if "q_teacher" in payload:
            self.q_teacher.import_weights(payload["q_teacher"])

    def submit_federated_update(self, node_id: str, n_samples: int, carbon_intensity: float) -> Optional[Dict[str, np.ndarray]]:
        if self.federated is None:
            return None
        self.federated.submit(
            FederatedUpdate(
                node_id=node_id,
                weights=self.student.export_weights(),
                n_samples=n_samples,
                carbon_intensity=carbon_intensity,
            )
        )
        return self.federated.aggregate()

    def apply_federated_global(self) -> bool:
        if self.federated is None:
            return False
        g = self.federated.global_weights()
        if g is None:
            return False
        self.student.import_weights(g)
        return True

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------
    def get_stats(self) -> Dict[str, Any]:
        return {
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "epsilon": self.epsilon,
            "baseline": self.student.baseline,
            "schema": STATE_SCHEMA_VERSION,
            "feature_dim": self.student.feature_dim,
            "n_classes": self.student.n_classes,
            "last_uncertainty": self.last_uncertainty,
            "teacher_contributions": dict(self.last_teacher_contributions),
            "role_weights": (
                {r.value: w for r, w in self.roles.weights().items()}
                if self.roles else {}
            ),
            "circuit_open": (self.breaker is not None and not self.breaker.allow()),
            "hitl_audit_size": (len(self.hitl.audit) if self.hitl else 0),
            "last_explanation": (
                self.last_explanation.to_dict() if self.last_explanation else None
            ),
            "last_counterfactuals": {
                int(k): float(v) for k, v in self.last_counterfactuals.items()
            },
        }
