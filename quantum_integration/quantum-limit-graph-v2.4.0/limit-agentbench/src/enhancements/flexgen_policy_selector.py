"""
Distillation-based FlexGen Policy Selector (Enhanced v3).

Single-file integration of all ten Green Agent enhancements.

CHANGELOG v2 -> v3
- PrecisionLevel now imported from .flexgen_policy (canonical). Fallback
  local definition only used if the import fails.
- _candidate_set_hash uses SHA-256 over a stable serialization (was Python
  hash(), which is process-salted).
- FlexGenStatefulQTeacher._ensure_n_actions preserves archived columns on
  shrink and restores them on growth; archive is pruned on confirm.
- CausalCounterfactualEstimator._ensure_actions now pads/truncates A and b
  instead of resetting, preserving learned structure across action-count
  changes.
- DistillationStudent.predict_proba is now side-effect free; resize happens
  explicitly via ensure_shape().
- _LinearSoftmaxModel.from_json validates shapes.
- QuantumInspiredTeacher._simulate guards against empty feature vectors and
  short vectors.
- PrioritizedReplayBuffer returns importance-sampling weights alongside the
  batch for unbiased updates.
- FederatedAggregator validates shapes and tracks an RDP budget.
- EmergentRoleRegistry gains epsilon-greedy role sampling and a snapshot API.
- DecisionExplainer produces per-candidate attributions, not just teacher
  weights.
- DistillationFlexGenSelector.update now:
    * applies Q-teacher updates using a lightweight state adapter,
    * falls back to teacher_probs.size when candidates is None,
    * persists both student and Q-teacher,
    * reconstructs FlexGenState-compatible feature updates.
- Non-blocking HITL option: publish the request and return immediately with
  a default decision; the caller can reconcile later.
- get_stats now exposes teacher failures, role snapshot, DP state.
- All original signatures preserved.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import random
import tempfile
import time
from collections import defaultdict, deque
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

# ---------------------------------------------------------------------------
# Canonical PrecisionLevel + PRECISION_ORDER (Enhancement 7).
# Prefer the definition from flexgen_policy to avoid two-enum divergence.
# ---------------------------------------------------------------------------
try:
    from .flexgen_policy import (  # type: ignore
        PrecisionLevel,
        PRECISION_ORDER as _CANONICAL_PRECISION_ORDER,
    )
    _PRECISION_ORDER = [p.value for p in _CANONICAL_PRECISION_ORDER]
except ImportError:  # pragma: no cover
    class PrecisionLevel(str, Enum):
        FP32 = "fp32"
        BF16 = "bf16"
        FP16 = "fp16"
        FP8 = "fp8"
        INT8 = "int8"
        INT4 = "int4"

    _PRECISION_ORDER = [p.value for p in PrecisionLevel]


# ===========================================================================
# Schema versioning
# ===========================================================================
STATE_SCHEMA_VERSION = "flexgen_state_v3"
STATE_FEATURE_FIELDS: Tuple[str, ...] = (
    "tokens_norm",
    "latency_target_norm",
    "gpu_memory_gb_norm",
    "cpu_memory_gb_norm",
    "disk_bw_norm",
    "carbon_intensity_norm",
    "recent_success_rate",
    "avg_reward",
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
# State
# ===========================================================================
@dataclass
class FlexGenState:
    """State for the distillation agent (schema v3)."""
    tokens: float
    latency_target: float
    gpu_memory_gb: float
    cpu_memory_gb: float
    disk_bandwidth_gbps: float
    carbon_intensity: float
    recent_success_rate: float
    avg_reward: float
    # v2+ additions
    carbon_price_per_kg: float = 0.0
    rec_kwh_available: float = 0.0
    hour_of_day: float = 12.0
    precision_hint: Optional[str] = None
    # Controller compatibility
    policy_idx: int = 0

    @property
    def schema_version(self) -> str:
        return STATE_SCHEMA_VERSION

    def to_feature_vector(self) -> np.ndarray:
        hour_rad = (self.hour_of_day % 24.0) / 24.0 * 2.0 * math.pi
        precision_idx = 0.0
        if self.precision_hint is not None and self.precision_hint in _PRECISION_ORDER:
            precision_idx = _PRECISION_ORDER.index(self.precision_hint) / max(
                len(_PRECISION_ORDER) - 1, 1
            )
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

    @classmethod
    def from_feature_vector(cls, vec: np.ndarray) -> "FlexGenState":
        """
        Inverse of to_feature_vector — used for Q-teacher updates that receive
        only the vector. Lossy but structurally consistent.
        """
        v = np.asarray(vec, dtype=np.float64).ravel()
        if v.size < len(STATE_FEATURE_FIELDS):
            v = np.pad(v, (0, len(STATE_FEATURE_FIELDS) - v.size))
        hour_rad = math.atan2(float(v[10]), float(v[11]))
        hour = ((hour_rad / (2 * math.pi)) % 1.0) * 24.0
        precision_idx = int(round(v[13] * max(len(_PRECISION_ORDER) - 1, 1)))
        precision_idx = max(0, min(len(_PRECISION_ORDER) - 1, precision_idx))
        return cls(
            tokens=float(v[0]) * NORMALIZATION_BOUNDS["tokens"],
            latency_target=float(v[1]) * NORMALIZATION_BOUNDS["latency_target"],
            gpu_memory_gb=float(v[2]) * NORMALIZATION_BOUNDS["gpu_memory_gb"],
            cpu_memory_gb=float(v[3]) * NORMALIZATION_BOUNDS["cpu_memory_gb"],
            disk_bandwidth_gbps=float(v[4]) * NORMALIZATION_BOUNDS["disk_bandwidth_gbps"],
            carbon_intensity=float(v[5]) * NORMALIZATION_BOUNDS["carbon_intensity"],
            recent_success_rate=float(v[6]),
            avg_reward=float(v[7]),
            carbon_price_per_kg=float(v[8]) * NORMALIZATION_BOUNDS["carbon_price"],
            rec_kwh_available=float(v[9]) * NORMALIZATION_BOUNDS["rec_kwh"],
            hour_of_day=hour,
            precision_hint=_PRECISION_ORDER[precision_idx],
        )


# ===========================================================================
# Teachers
# ===========================================================================
class Teacher:
    """Abstract teacher with role tagging."""
    name: str = "teacher"
    role: str = "exploiter"

    def predict(self, state: FlexGenState, candidates: Sequence[FlexGenPolicy]) -> np.ndarray:
        raise NotImplementedError

    def confidence(self, state: FlexGenState) -> float:
        raise NotImplementedError


class FlexGenRuleBasedTeacher(Teacher):
    """Candidate-aware heuristic that reads carbon price, REC, precision, and headroom."""

    name = "rule"
    role = "safety_officer"

    def predict(self, state: FlexGenState, candidates: Sequence[FlexGenPolicy]) -> np.ndarray:
        n = len(candidates)
        if n == 0:
            return np.zeros(0)
        scores = np.ones(n, dtype=np.float64) * 0.5

        carbon_stress = min(1.0, state.carbon_intensity / 600.0)
        price_stress = min(1.0, state.carbon_price_per_kg / 0.2)
        rec_relief = min(1.0, state.rec_kwh_available / 20.0)

        for i, pol in enumerate(candidates):
            s = 1.0
            if pol.weight_device == "cpu":
                s += 0.3 * carbon_stress
            if pol.weight_device == "disk":
                s -= 0.15 * (1.0 - carbon_stress)
            if state.latency_target < 200 and pol.weight_device == "gpu":
                s += 0.3
            pol_prec = getattr(pol, "precision_level", None)
            if pol_prec in ("int4", "int8", "fp8") and (carbon_stress > 0.5 or price_stress > 0.5):
                s += 0.25 * max(carbon_stress, price_stress)
            if rec_relief > 0.5 and pol_prec in ("fp16", "bf16", "fp32"):
                s += 0.15 * rec_relief
            if getattr(pol, "mixed_precision", False):
                s += 0.1
            if state.gpu_memory_gb < 8 and pol.weight_bits <= 8:
                s += 0.2
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


class FlexGenHistoricalMLTeacher(Teacher):
    """Loads a portable linear-softmax model from JSON (no pickle)."""

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
    """Portable linear softmax with input-shape validation."""

    def __init__(self, weights: np.ndarray, bias: np.ndarray):
        if weights.ndim != 2:
            raise ValueError("weights must be 2D")
        if bias.ndim != 1 or bias.shape[0] != weights.shape[1]:
            raise ValueError("bias shape must match weights.shape[1]")
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
        if x.shape[-1] != self.weights.shape[0]:
            # Pad or truncate to fit the model. This prevents crashes when the
            # state schema changes between runs.
            d = self.weights.shape[0]
            if x.shape[-1] < d:
                x = np.pad(x, ((0, 0), (0, d - x.shape[-1])))
            else:
                x = x[..., :d]
        logits = x @ self.weights + self.bias
        logits = logits - logits.max(axis=-1, keepdims=True)
        e = np.exp(logits)
        return e / e.sum(axis=-1, keepdims=True)


class FlexGenStatefulQTeacher(Teacher):
    """
    Linear Q-learning teacher with:

      - archive-preserving candidate-count changes,
      - explicit remove_archive() for committed columns,
      - periodic atomic JSON persistence,
      - add_state_update() for controller-side updates that only have the
        feature vector.
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
                logger.warning("Q-teacher schema mismatch; ignoring persisted file.")
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
        if n_actions < self.n_actions:
            # Archive dropped columns so we can restore them if the count grows.
            for j in range(n_actions, self.n_actions):
                self._archived_columns[j] = self.weights[:, j].copy()
        new_weights = np.zeros((self.feature_dim, n_actions))
        min_act = min(self.n_actions, n_actions)
        new_weights[:, :min_act] = self.weights[:, :min_act]
        if n_actions > self.n_actions:
            for j in range(self.n_actions, n_actions):
                if j in self._archived_columns:
                    new_weights[:, j] = self._archived_columns.pop(j)
        self.weights = new_weights
        self.n_actions = n_actions
        self._dirty = True

    def commit_archive(self, keep_top_k: Optional[int] = None) -> int:
        """
        Explicitly discard archived columns. If keep_top_k is provided, keep
        only the K most-recently-archived columns. Returns the number removed.
        """
        if keep_top_k is None:
            removed = len(self._archived_columns)
            self._archived_columns.clear()
            return removed
        keys = sorted(self._archived_columns.keys())
        if len(keys) <= keep_top_k:
            return 0
        to_drop = keys[:-keep_top_k]
        for k in to_drop:
            self._archived_columns.pop(k, None)
        return len(to_drop)

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

    def update_from_vector(self, state_vec: np.ndarray, action: int, reward: float) -> None:
        """Controller-friendly update path that avoids re-encoding the state."""
        self._ensure_n_actions(self.n_actions)  # no-op guard
        if not (0 <= action < self.n_actions):
            return
        x = np.asarray(state_vec, dtype=np.float64).ravel()
        if x.size < self.feature_dim:
            x = np.pad(x, (0, self.feature_dim - x.size))
        elif x.size > self.feature_dim:
            x = x[: self.feature_dim]
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._update_count += 1
        self._dirty = True
        self._save()

    # -- federated -----------------------------------------------------
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


class QuantumInspiredTeacher(Teacher):
    """Quantum-inspired teacher with a small parameterized state-vector simulation."""

    name = "quantum"
    role = "carbon_broker"

    def __init__(self, n_qubits: int = 5, seed: int = 0):
        self.n_qubits = max(1, int(n_qubits))
        self.rng = np.random.default_rng(seed)
        self._params = self.rng.normal(size=(self.n_qubits, 2)) * 0.5

    def _simulate(self, features: np.ndarray, n_candidates: int) -> np.ndarray:
        if n_candidates <= 0:
            return np.zeros(0)
        feat = np.asarray(features, dtype=np.float64).ravel()
        if feat.size == 0:
            feat = np.zeros(self.n_qubits, dtype=np.float64)
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128)
        state[0] = 1.0
        for q in range(self.n_qubits):
            theta = float(
                self._params[q, 0] * feat[q % feat.size] + self._params[q, 1]
            )
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
# Student
# ===========================================================================
class DistillationStudent:
    """
    Linear softmax student with:

      - baseline-subtracted advantage,
      - L2 regularization,
      - temperature-scaled softmax,
      - gradient clipping,
      - federated export/import,
      - side-effect-free predict (v3): shape is resized explicitly.
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

    def ensure_shape(self, feature_dim: int, n_classes: int) -> None:
        """Explicit, side-effect-only-when-needed resize."""
        if feature_dim != self.feature_dim:
            new_w = np.zeros((feature_dim, self.n_classes))
            min_dim = min(feature_dim, self.feature_dim)
            new_w[:min_dim, :] = self.weights[:min_dim, :]
            self.weights = new_w
            self.feature_dim = feature_dim
        if n_classes != self.n_classes:
            new_w = np.zeros((self.feature_dim, n_classes))
            new_b = np.zeros(n_classes)
            min_c = min(self.n_classes, n_classes)
            new_w[:, :min_c] = self.weights[:, :min_c]
            new_b[:min_c] = self.biases[:min_c]
            self.weights = new_w
            self.biases = new_b
            self.n_classes = n_classes

    def predict_proba(self, state_vector: np.ndarray, num_classes: int) -> np.ndarray:
        """
        Side-effect free (v3). If shapes do not match, returns a uniform
        distribution rather than silently mutating the model. Call
        ensure_shape() first to change dimensions.
        """
        x = np.asarray(state_vector, dtype=np.float64).ravel()
        if x.size != self.feature_dim or num_classes != self.n_classes:
            return np.ones(max(num_classes, 1)) / max(num_classes, 1)
        logits = x @ self.weights + self.biases
        logits = logits / self.temperature
        logits = logits - logits.max()
        e = np.exp(logits)
        return e / e.sum()

    def update(
        self,
        state_vector: np.ndarray,
        teacher_probs: np.ndarray,
        reward: float,
        action: int,
        distill_weight: float = 0.7,
        rl_weight: float = 0.3,
    ) -> None:
        n = len(teacher_probs)
        self.ensure_shape(len(state_vector), n)
        current_probs = self.predict_proba(state_vector, n)
        self.baseline = (
            self.baseline_alpha * reward + (1 - self.baseline_alpha) * self.baseline
        )
        advantage = reward - self.baseline

        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(n)
        if 0 <= action < n:
            one_hot[action] = 1.0
        grad_rl = -advantage * (one_hot - current_probs)
        grad = distill_weight * grad_distill + rl_weight * grad_rl

        norm = float(np.linalg.norm(grad))
        if norm > self.grad_clip:
            grad = grad * (self.grad_clip / (norm + 1e-12))

        self.weights -= self.lr * (np.outer(state_vector, grad) + self.l2_reg * self.weights)
        self.biases -= self.lr * grad
        self.counter += 1

    # -- federated -----------------------------------------------------
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
# Prioritized replay with importance sampling
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
    Replay buffer tagged with candidate-set hashes; sampling returns
    importance-sampling weights alongside the batch (v3).
    """

    def __init__(self, max_size: int = 2000, alpha: float = 0.6, eps: float = 1e-4,
                 beta_start: float = 0.4, beta_end: float = 1.0):
        self.buffer: deque = deque(maxlen=max_size)
        self.alpha = alpha
        self.eps = eps
        self.beta_start = beta_start
        self.beta_end = beta_end
        self._sample_count = 0

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
    ) -> Tuple[List[ReplaySample], np.ndarray]:
        if not self.buffer:
            return [], np.zeros(0)
        candidates = list(self.buffer)
        if current_hash is not None:
            filtered = [s for s in candidates if s.candidate_hash == current_hash]
            if filtered:
                candidates = filtered
            else:
                return [], np.zeros(0)
        priorities = np.array([max(s.priority, self.eps) ** self.alpha for s in candidates])
        probs = priorities / priorities.sum()
        k = min(batch_size, len(candidates))
        idx = np.random.choice(len(candidates), size=k, replace=False, p=probs)

        # Importance sampling weights.
        self._sample_count += 1
        beta = min(self.beta_end, self.beta_start +
                   (self.beta_end - self.beta_start) * self._sample_count / 1000.0)
        n = len(candidates)
        is_weights = (n * probs[idx]) ** (-beta)
        is_weights = is_weights / max(is_weights.max(), 1e-12)
        return [candidates[i] for i in idx], is_weights


# ===========================================================================
# Causal counterfactual estimator
# ===========================================================================
class CausalCounterfactualEstimator:
    """
    Ridge-regression structural causal model with padding-based action resize
    (v3) so growing or shrinking candidate sets does not throw away knowledge.
    """

    def __init__(self, state_dim: int, n_actions: int, ridge: float = 1e-3):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.ridge = ridge
        d = state_dim + n_actions + 1
        self._A = np.eye(d) * ridge
        self._b = np.zeros(d)
        self._n = 0

    def _features(self, s: np.ndarray, a: int) -> np.ndarray:
        one_hot = np.zeros(self.n_actions)
        one_hot[a % self.n_actions] = 1.0
        return np.concatenate([np.asarray(s, dtype=np.float64), one_hot, [1.0]])

    def _ensure_actions(self, n_actions: int) -> None:
        if n_actions == self.n_actions:
            return
        old_d = self.state_dim + self.n_actions + 1
        new_d = self.state_dim + n_actions + 1
        new_A = np.eye(new_d) * self.ridge
        new_b = np.zeros(new_d)
        # Copy the state-block, then as many action one-hots as fit.
        state_block = min(self.state_dim, self.state_dim)
        new_A[:state_block, :state_block] = self._A[:state_block, :state_block]
        new_b[:state_block] = self._b[:state_block]
        a_old = self.n_actions
        a_new = n_actions
        a_min = min(a_old, a_new)
        off_old = self.state_dim
        off_new = self.state_dim
        new_A[off_new:off_new + a_min, off_new:off_new + a_min] = \
            self._A[off_old:off_old + a_min, off_old:off_old + a_min]
        new_b[off_new:off_new + a_min] = self._b[off_old:off_old + a_min]
        # Copy intercept (last index).
        new_A[-1, -1] = self._A[-1, -1]
        new_b[-1] = self._b[-1]
        self._A = new_A
        self._b = new_b
        self.n_actions = n_actions

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
# Federated aggregator with shape validation + RDP budget
# ===========================================================================
class SimpleRDPAccountant:
    """RDP accountant for the federated aggregation path."""

    def __init__(self, target_delta: float = 1e-5, max_epsilon: float = 10.0):
        self.target_delta = target_delta
        self.max_epsilon = max_epsilon
        self.steps = 0
        self.orders = [1 + x / 10.0 for x in range(1, 100)]
        self.rdp = np.zeros(len(self.orders))

    def step(self, sigma: float, q: float = 1.0) -> None:
        if sigma <= 0:
            return
        self.steps += 1
        for i, o in enumerate(self.orders):
            self.rdp[i] += (o * q * q) / (2 * sigma * sigma)

    def epsilon(self) -> float:
        if self.steps == 0:
            return 0.0
        eps = [
            self.rdp[i] + math.log(1.0 / self.target_delta) / (self.orders[i] - 1)
            for i in range(len(self.orders)) if self.orders[i] > 1
        ]
        return float(min(eps)) if eps else float("inf")

    def budget_exceeded(self) -> bool:
        return self.epsilon() > self.max_epsilon

    def get_state(self) -> Dict[str, Any]:
        return {
            "steps": self.steps,
            "epsilon": self.epsilon(),
            "delta": self.target_delta,
            "budget_exceeded": self.budget_exceeded(),
        }


@dataclass
class FederatedUpdate:
    node_id: str
    weights: Dict[str, np.ndarray]
    n_samples: int
    carbon_intensity: float
    timestamp: float = field(default_factory=time.time)


class FederatedAggregator:
    """Carbon-weighted FedAvg with shape validation and RDP budget."""

    def __init__(self, dp_sigma: float = 1e-3, staleness_s: float = 3600.0,
                 target_delta: float = 1e-5, max_epsilon: float = 10.0):
        self.dp_sigma = dp_sigma
        self.staleness_s = staleness_s
        self._updates: Dict[str, FederatedUpdate] = {}
        self._global: Optional[Dict[str, np.ndarray]] = None
        self._shapes: Dict[str, Tuple[int, ...]] = {}
        self.accountant = SimpleRDPAccountant(target_delta=target_delta,
                                              max_epsilon=max_epsilon)
        self.rejected: List[Dict[str, Any]] = []

    def submit(self, update: FederatedUpdate) -> bool:
        for k, v in update.weights.items():
            if not isinstance(v, np.ndarray) or v.size == 0:
                return False
            if k in self._shapes and v.shape != self._shapes[k]:
                self.rejected.append({
                    "node_id": update.node_id,
                    "key": k,
                    "expected": self._shapes[k],
                    "got": v.shape,
                    "t": time.time(),
                })
                logger.warning(
                    f"Rejected federated update from {update.node_id}: "
                    f"shape mismatch on {k}"
                )
                return False
            self._shapes.setdefault(k, v.shape)
        if self.accountant.budget_exceeded():
            logger.warning(
                f"DP budget exceeded (eps={self.accountant.epsilon():.3f}); "
                f"skipping federated update"
            )
            return False
        if self.dp_sigma > 0:
            noisy = {
                k: v + np.random.normal(0.0, self.dp_sigma, size=v.shape)
                for k, v in update.weights.items()
            }
            self.accountant.step(self.dp_sigma, q=1.0)
            update = FederatedUpdate(
                node_id=update.node_id, weights=noisy,
                n_samples=update.n_samples,
                carbon_intensity=update.carbon_intensity,
                timestamp=update.timestamp,
            )
        self._updates[update.node_id] = update
        return True

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
            try:
                stacked = np.stack([u.weights[k] for u in fresh], axis=0)
            except (KeyError, ValueError):
                continue
            agg[k] = np.tensordot(weights, stacked, axes=([0], [0]))
        self._global = agg
        return agg

    def global_weights(self) -> Optional[Dict[str, np.ndarray]]:
        return self._global

    def get_dp_state(self) -> Dict[str, Any]:
        return {**self.accountant.get_state(),
                "rejected_updates": len(self.rejected)}


# ===========================================================================
# Emergent role registry
# ===========================================================================
class AgentRole(str, Enum):
    EXPLORER = "explorer"
    EXPLOITER = "exploiter"
    SAFETY_OFFICER = "safety_officer"
    CARBON_BROKER = "carbon_broker"
    VERIFIER = "verifier"


class EmergentRoleRegistry:
    """
    Decay-weighted performance tracker per role with epsilon-greedy sampling
    and full snapshot support (v3).
    """

    def __init__(self, decay: float = 0.95, exploration: float = 0.1,
                 rng: Optional[random.Random] = None):
        self.decay = decay
        self.exploration = exploration
        self.rng = rng or random.Random(0)
        self._scores: Dict[AgentRole, float] = {r: 1.0 for r in AgentRole}
        self._counts: Dict[AgentRole, int] = {r: 0 for r in AgentRole}
        self._successes: Dict[AgentRole, int] = {r: 0 for r in AgentRole}

    def record(self, role: AgentRole, success: bool, reward: float) -> None:
        for r in self._scores:
            self._scores[r] *= self.decay
        signal = abs(reward) if success else -abs(reward) * 0.5
        self._scores[role] += signal
        self._counts[role] += 1
        if success:
            self._successes[role] += 1

    def success_rate(self, role: AgentRole) -> float:
        c = self._counts[role]
        return (self._successes[role] / c) if c > 0 else 0.5

    def weights(self) -> Dict[AgentRole, float]:
        total = sum(max(v, 1e-6) for v in self._scores.values())
        return {r: max(v, 1e-6) / total for r, v in self._scores.items()}

    def dominant_role(self) -> AgentRole:
        return max(self._scores, key=self._scores.get)

    def sample_role(self) -> AgentRole:
        if self.rng.random() < self.exploration:
            return self.rng.choice(list(AgentRole))
        return self.dominant_role()

    def snapshot(self) -> Dict[str, Any]:
        return {
            "scores": {r.value: round(v, 4) for r, v in self._scores.items()},
            "counts": {r.value: c for r, c in self._counts.items()},
            "success_rates": {r.value: round(self.success_rate(r), 4) for r in AgentRole},
            "dominant": self.dominant_role().value,
        }


# ===========================================================================
# XAI explainer
# ===========================================================================
@dataclass
class Explanation:
    decision_id: str
    chosen_idx: int
    teacher_attributions: Dict[str, float]
    per_candidate_attribution: List[Dict[str, Any]]
    counterfactuals: List[Dict[str, Any]]
    rationale: str
    confidence: float
    uncertainty: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DecisionExplainer:
    """
    Produces per-candidate attributions, teacher contributions, counterfactual
    alternatives, and a natural-language rationale (v3).
    """

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

        # v3: per-candidate attribution surface.
        per_cand: List[Dict[str, Any]] = []
        n = len(teacher_probs)
        for i in range(n):
            per_cand.append({
                "idx": i,
                "teacher_prob": float(teacher_probs[i]) if i < n else 0.0,
                "student_prob": float(student_probs[i]) if i < len(student_probs) else 0.0,
                "counterfactual_reward": float(counterfactuals.get(i, 0.0)),
            })

        rationale = (
            f"Chose candidate #{chosen_idx} (teacher confidence={conf:.2f}, "
            f"student prob={float(student_probs[chosen_idx]):.2f}, uncertainty={uncertainty:.2f}). "
            f"Top teacher contributions: {reason_bits}. "
            f"Counterfactuals: {len(cf_list)}."
        )
        return Explanation(
            decision_id=f"dec-{int(time.time()*1000)}",
            chosen_idx=chosen_idx,
            teacher_attributions=teacher_contributions,
            per_candidate_attribution=per_cand,
            counterfactuals=cf_list,
            rationale=rationale,
            confidence=conf,
            uncertainty=uncertainty,
        )


# ===========================================================================
# Uncertainty + HITL + circuit breaker
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
    """
    Async approval gate. Two modes:
      - 'block' (default)   : awaits the approver up to timeout_s.
      - 'publish_only' (v3) : fires the request and returns the default
                              decision immediately. The caller reconciles later.
    """

    def __init__(
        self,
        approver: Optional[Callable[[Dict[str, Any]], bool]] = None,
        timeout_s: float = 2.0,
        auto_approve_on_timeout: bool = True,
        mode: str = "block",
        publisher: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        self.approver = approver
        self.timeout_s = timeout_s
        self.auto_approve_on_timeout = auto_approve_on_timeout
        self.mode = mode
        self.publisher = publisher
        self.audit: List[Dict[str, Any]] = []

    async def request(self, payload: Dict[str, Any]) -> bool:
        if self.mode == "publish_only":
            if self.publisher is not None:
                try:
                    self.publisher(payload)
                except Exception as exc:  # noqa: BLE001
                    logger.error(f"HITL publisher failed: {exc}")
            self.audit.append({"approved": None, "mode": "publish_only", **payload})
            return self.auto_approve_on_timeout

        if self.approver is None:
            self.audit.append({"approved": True, "reason": "no_approver", **payload})
            return True
        loop = asyncio.get_running_loop()
        try:
            ok = await asyncio.wait_for(
                loop.run_in_executor(None, self.approver, payload),
                timeout=self.timeout_s,
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
        self.total_failures = 0
        self.total_successes = 0

    def allow(self) -> bool:
        return time.time() >= self._open_until

    def record(self, success: bool) -> None:
        if success:
            self._failures = 0
            self.total_successes += 1
        else:
            self._failures += 1
            self.total_failures += 1
            if self._failures >= self.failure_threshold:
                self._open_until = time.time() + self.recovery_time_s
                self._failures = 0


# ===========================================================================
# Helpers
# ===========================================================================
def _align_probs(probs: np.ndarray, n: int) -> np.ndarray:
    """Align a probability vector to n classes without cyclic repetition."""
    if n <= 0:
        return np.zeros(0)
    if probs.size == 0:
        return np.ones(n) / n
    out = np.ones(n, dtype=np.float64) * (1.0 / max(n, 1))
    k = min(probs.size, n)
    out[:k] = probs[:k]
    s = out.sum()
    return out / s if s > 0 else np.ones(n) / n


def _candidate_set_hash(candidates: Sequence[FlexGenPolicy]) -> str:
    """Stable SHA-256 over a canonical serialization (v3)."""
    if not candidates:
        return "empty"
    payload = "|".join(json.dumps(c.to_dict(), sort_keys=True) for c in candidates)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


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
    Multi-teacher on-policy distillation selector (v3).
    Variable candidate count, schema-versioned persistence, ten enhancements.
    """

    def __init__(
        self,
        n_candidates: int = 20,
        config: Optional[Dict] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        persistence_path: Optional[str] = None,
        enable_quantum_teacher: bool = True,
        enable_causal: bool = True,
        enable_federated: bool = True,
        enable_multi_agent: bool = True,
        enable_xai: bool = True,
        enable_hitl: bool = True,
        enable_resilience: bool = True,
        safety_shield: Optional[
            Callable[[int, Sequence[Dict[str, Any]]], Tuple[int, bool, Dict[str, bool]]]
        ] = None,
        temporal_monitor: Optional[Callable[[Dict[str, Any]], Dict[str, bool]]] = None,
        hitl_approver: Optional[Callable[[Dict[str, Any]], bool]] = None,
        hitl_mode: str = "block",
        hitl_publisher: Optional[Callable[[Dict[str, Any]], None]] = None,
        seed: Optional[int] = None,
    ):
        self.config = config or {}
        self.message_queue = message_queue
        self.persistence_path = persistence_path
        self.feature_dim = FlexGenState.feature_dim()
        self.rng = random.Random(seed if seed is not None else 0)
        self.seed = seed

        # Teachers
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
        self.teachers: List[Teacher] = [
            self.rule_teacher, self.historical_teacher, self.q_teacher,
        ]
        if enable_quantum_teacher:
            self.quantum_teacher = QuantumInspiredTeacher(seed=seed or 0)
            self.teachers.append(self.quantum_teacher)
        else:
            self.quantum_teacher = None

        # Student + replay
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

        # Enhancement modules
        self.causal = (
            CausalCounterfactualEstimator(self.feature_dim, n_candidates)
            if enable_causal else None
        )
        self.federated = FederatedAggregator() if enable_federated else None
        self.roles = (
            EmergentRoleRegistry(rng=random.Random(seed or 0))
            if enable_multi_agent else None
        )
        self.explainer = DecisionExplainer() if enable_xai else None
        self.uncertainty = UncertaintyEstimator() if enable_hitl else None
        self.hitl = (
            HumanInTheLoopGate(
                approver=hitl_approver,
                mode=hitl_mode,
                publisher=hitl_publisher,
            )
            if enable_hitl else None
        )
        self.breaker = CircuitBreaker() if enable_resilience else None

        # Hooks
        self.safety_shield = safety_shield
        self.temporal_monitor = temporal_monitor

        # Schedule
        self.epsilon = self.config.get("epsilon", 0.1)
        self.epsilon_decay = self.config.get("epsilon_decay", 0.999)
        self.train_every = self.config.get("train_every", 10)
        self.counter = 0

        # Caches
        self.last_teacher_probs: Optional[np.ndarray] = None
        self.last_student_probs: Optional[np.ndarray] = None
        self.last_explanation: Optional[Explanation] = None
        self.last_uncertainty: float = 0.0
        self.last_teacher_contributions: Dict[str, float] = {}
        self.last_counterfactuals: Dict[int, float] = {}

        if self.persistence_path and os.path.exists(self.persistence_path):
            self._load_student()

    # ------------------------------------------------------------------
    # Persistence
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
            if "q_teacher" in payload:
                self.q_teacher.import_weights(payload["q_teacher"])
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
            payload["q_teacher"] = self.q_teacher.export_weights()
        except Exception:
            pass
        try:
            _atomic_json_write(self.persistence_path, payload)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Student save failed: {exc}")

    # ------------------------------------------------------------------
    # Teacher ensemble
    # ------------------------------------------------------------------
    def _safe_predict(
        self,
        teacher: Teacher,
        state: FlexGenState,
        candidates: Sequence[FlexGenPolicy],
    ) -> Optional[np.ndarray]:
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
        state_vec = state.to_feature_vector()
        n = len(candidates)
        if n == 0:
            return 0, state_vec, np.zeros(0)

        teacher_probs, contributions = self._ensemble_teacher_probs(state, candidates)
        student_probs = self.student.predict_proba(state_vec, n)

        if exploration and self.rng.random() < self.epsilon:
            action_idx = self.rng.randint(0, n - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))

        # Safety shield
        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.safety_shield is not None:
            try:
                candidate_dicts = [c.to_dict() for c in candidates]
                action_idx, safety_ok, stl_verdict = self.safety_shield(
                    action_idx, candidate_dicts
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Safety shield failed: {exc}")

        # Temporal monitor
        if self.temporal_monitor is not None:
            try:
                stl_verdict = {
                    **stl_verdict,
                    **self.temporal_monitor({"candidate_idx": action_idx}),
                }
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Temporal monitor failed: {exc}")

        # Uncertainty
        uncertainty_score = 0.0
        is_uncertain = False
        if self.uncertainty is not None:
            is_uncertain, uncertainty_score = self.uncertainty.score(teacher_probs)

        # HITL
        if self.hitl is not None and (is_uncertain or not safety_ok):
            approved = await self.hitl.request({
                "reason": "high_uncertainty" if is_uncertain else "safety_violation",
                "candidate_idx": action_idx,
                "uncertainty": uncertainty_score,
                "num_candidates": n,
            })
            if not approved:
                action_idx = int(np.argmin([c.weight_bits for c in candidates]))

        # Causal counterfactuals
        counterfactuals: Dict[int, float] = {}
        if self.causal is not None:
            self.causal._ensure_actions(n)
            counterfactuals = self.causal.counterfactuals(state_vec, n)

        # XAI
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

        # Cache
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
        # v3: derive candidate hash from teacher_probs when candidates missing.
        if candidates is not None:
            candidate_hash = _candidate_set_hash(candidates)
        else:
            size_hint = len(teacher_probs) if teacher_probs is not None else 0
            candidate_hash = f"size:{size_hint}"

        self.replay_buffer.push(
            state_vec=state_vec,
            action=action_idx,
            reward=reward,
            next_state_vec=next_state_vec,
            teacher_probs=teacher_probs,
            candidate_hash=candidate_hash,
        )
        self.counter += 1

        # Q-teacher online update via the vector-friendly path (v3).
        try:
            self.q_teacher.update_from_vector(state_vec, action_idx, reward)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Q-teacher update failed: {exc}")

        # Causal update
        if self.causal is not None:
            self.causal.update(state_vec, action_idx, reward)

        # Uncertainty history
        if self.uncertainty is not None:
            self.uncertainty.observe(reward)

        # Role registry update
        if self.roles is not None:
            role = AgentRole.EXPLOITER
            if self.last_teacher_contributions:
                top = max(self.last_teacher_contributions,
                          key=self.last_teacher_contributions.get)
                try:
                    role = AgentRole(top)
                except ValueError:
                    role = AgentRole.EXPLOITER
            self.roles.record(role, success=reward > 0.5, reward=reward)

        # Train student periodically (with importance weights)
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch, is_weights = self.replay_buffer.sample(
                8, current_hash=candidate_hash
            )
            for s, w in zip(batch, is_weights if len(is_weights) else []):
                self.student.update(
                    s.state_vec,
                    s.teacher_probs,
                    s.reward * float(w),
                    s.action,
                    distill_weight=self.config.get("distill_weight", 0.7),
                    rl_weight=self.config.get("rl_weight", 0.3),
                )

        # Periodic student save
        if self.persistence_path and self.counter % 100 == 0:
            self._save_student()

    # ------------------------------------------------------------------
    # Federated helpers
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

    def submit_federated_update(
        self, node_id: str, n_samples: int, carbon_intensity: float,
    ) -> Optional[Dict[str, np.ndarray]]:
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
            "role_snapshot": self.roles.snapshot() if self.roles else {},
            "circuit_open": (self.breaker is not None and not self.breaker.allow()),
            "circuit_failures": (self.breaker.total_failures if self.breaker else 0),
            "hitl_audit_size": (len(self.hitl.audit) if self.hitl else 0),
            "federated_dp": (
                self.federated.get_dp_state() if self.federated else {}
            ),
            "last_explanation": (
                self.last_explanation.to_dict() if self.last_explanation else None
            ),
            "last_counterfactuals": {
                int(k): float(v) for k, v in self.last_counterfactuals.items()
            },
        }
