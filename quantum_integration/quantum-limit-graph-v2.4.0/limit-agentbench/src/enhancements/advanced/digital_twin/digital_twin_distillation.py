# src/quantum_integration/digital_twin/digital_twin_distillation.py

"""Distillation fallback: teachers, student, replay buffer, optimizer.

Enhancements over v1:
- Fixed ``temperature`` no-op (now samples from the scaled distribution).
- Class-order-aware historical ML teacher + max-prob confidence.
- Config validation + configurable hyperparameters (mixtures, batch size,
  Q-teacher LR/gamma/TD-clip, gradient clip, min buffer size).
- Replay buffer copies arrays on push; add ``clear`` + serialization.
- Gradient clipping in the student update; guarded probability inputs.
- Full ``to_dict`` / ``from_dict`` for the optimizer (student, Q-teacher,
  replay buffer, counter, epsilon).
- Metric collection (``statistics``) and safer teacher combination.
- Removed unused imports/attributes.
"""

from __future__ import annotations

import logging
import math
import random
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Sequence, Tuple

import numpy as np

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 2

ACTION_SPACE: Tuple[str, ...] = (
    "aggressive_carbon",
    "helium_preservation",
    "circularity_boost",
    "renewable_acceleration",
    "balanced",
)

_FEATURE_NAMES: Tuple[str, ...] = (
    "carbon_emissions", "helium_depletion", "energy_consumption",
    "circularity_index", "biodiversity_impact",
    "carbon_reduction_rate", "helium_reduction_rate", "adoption_rate",
    "shock_size", "recent_success_rate", "avg_roi",
    "circuit_breaker_state", "cache_usage", "scenario_count",
)

_EPS = 1e-12


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _softmax(x: np.ndarray) -> np.ndarray:
    x = np.asarray(x, dtype=np.float64).reshape(-1)
    if x.size == 0:
        return x
    x = x - np.max(x)
    e = np.exp(x)
    s = float(e.sum())
    return e / s if s > 0 else np.full_like(e, 1.0 / e.size)


def _validate_probabilities(p: Any, *, name: str = "probs") -> np.ndarray:
    """Return a sanitized, L1-normalized probability vector of size K."""
    arr = np.asarray(p, dtype=np.float64).reshape(-1)
    if arr.size != len(ACTION_SPACE):
        raise ValueError(
            f"{name}: expected {len(ACTION_SPACE)} classes, got {arr.size}."
        )
    arr = np.where(np.isfinite(arr), arr, 0.0)
    arr = np.clip(arr, 0.0, None)
    s = float(arr.sum())
    if s <= _EPS:
        return np.full(len(ACTION_SPACE), 1.0 / len(ACTION_SPACE))
    return arr / s


def _get_config_value(config: Any, name: str, default: Any) -> Any:
    """Fetch a config value from an object, dict, or return default."""
    if config is None:
        return default
    if isinstance(config, dict):
        val = config.get(name, default)
    else:
        val = getattr(config, name, default)
    return default if val is None else val


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #

@dataclass
class TwinOptimizationState:
    carbon_emissions: float = 0.5
    helium_depletion: float = 0.5
    energy_consumption: float = 0.5
    circularity_index: float = 0.5
    biodiversity_impact: float = 0.5
    carbon_reduction_rate: float = 0.0
    helium_reduction_rate: float = 0.0
    adoption_rate: float = 0.0
    shock_size: float = 0.0
    recent_success_rate: float = 0.5
    avg_roi: float = 0.5
    circuit_breaker_state: float = 0.0
    cache_usage: float = 0.0
    scenario_count: float = 0.0

    def to_feature_vector(self) -> np.ndarray:
        return np.array(
            [getattr(self, name) for name in _FEATURE_NAMES],
            dtype=np.float32,
        )

    @classmethod
    def from_feature_vector(cls, vec: Sequence[float]) -> "TwinOptimizationState":
        if len(vec) != len(_FEATURE_NAMES):
            raise ValueError(
                f"expected {len(_FEATURE_NAMES)} features, got {len(vec)}."
            )
        kwargs = {name: float(v) for name, v in zip(_FEATURE_NAMES, vec)}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Teachers
# --------------------------------------------------------------------------- #

class Teacher(ABC):
    @abstractmethod
    def predict(self, state: TwinOptimizationState) -> np.ndarray: ...

    @abstractmethod
    def confidence(self, state: TwinOptimizationState) -> float: ...


class TwinRuleBasedTeacher(Teacher):
    """Deterministic rule-based teacher."""

    def predict(self, state: TwinOptimizationState) -> np.ndarray:
        probs = np.ones(len(ACTION_SPACE), dtype=np.float64) * 0.1
        if state.carbon_emissions > 0.7:
            probs[0] = 0.8
        elif state.helium_depletion < 0.3:
            probs[1] = 0.7
        elif state.circularity_index < 0.4:
            probs[2] = 0.6
        elif state.energy_consumption > 0.8:
            probs[3] = 0.6
        else:
            probs[4] = 0.5
        return probs / probs.sum()

    def confidence(self, state: TwinOptimizationState) -> float:
        if state.carbon_emissions > 0.7:
            return 0.6
        if state.helium_depletion < 0.3:
            return 0.5
        return 0.4


class TwinHistoricalMLTeacher(Teacher):
    """Best-effort historical ML teacher (sklearn-style, optional).

    Predictions are re-ordered to match ``ACTION_SPACE`` using the
    model's ``classes_`` attribute, so a model trained with a permuted
    label order still produces correctly-mapped probabilities.
    """

    def __init__(self, model_path: Optional[str] = None) -> None:
        self.model: Any = None
        self._class_index: Optional[np.ndarray] = None
        if model_path and Path(model_path).exists():
            self._try_load(model_path)

    def _try_load(self, model_path: str) -> None:
        try:
            import joblib
            model = joblib.load(model_path)
        except Exception as exc:  # noqa: BLE001 - best-effort
            logger.warning("Historical ML teacher disabled: %s", exc)
            return

        classes = getattr(model, "classes_", None)
        if classes is None or len(classes) != len(ACTION_SPACE):
            logger.warning(
                "Historical ML teacher has %s classes; expected %s.",
                None if classes is None else len(classes),
                len(ACTION_SPACE),
            )
            return

        name_to_idx = {name: i for i, name in enumerate(ACTION_SPACE)}
        try:
            reorder = np.array(
                [name_to_idx[c] for c in classes], dtype=np.int64,
            )
        except KeyError as exc:
            logger.warning(
                "Historical ML teacher classes do not match ACTION_SPACE: %s",
                exc,
            )
            return

        self.model = model
        self._class_index = reorder

    def predict(self, state: TwinOptimizationState) -> np.ndarray:
        if self.model is None:
            return np.full(len(ACTION_SPACE), 1.0 / len(ACTION_SPACE))
        try:
            x = state.to_feature_vector().reshape(1, -1)
            proba = np.asarray(self.model.predict_proba(x)[0], dtype=np.float64)
            if (
                self._class_index is not None
                and proba.size == self._class_index.size
            ):
                reordered = np.zeros(len(ACTION_SPACE), dtype=np.float64)
                reordered[self._class_index] = proba
                proba = reordered
            return _validate_probabilities(proba, name="historical_ml_probs")
        except Exception as exc:  # noqa: BLE001 - best-effort
            logger.warning("Historical ML teacher inference failed: %s", exc)
            return np.full(len(ACTION_SPACE), 1.0 / len(ACTION_SPACE))

    def confidence(self, state: TwinOptimizationState) -> float:
        if self.model is None:
            return 0.0
        try:
            x = state.to_feature_vector().reshape(1, -1)
            proba = np.asarray(self.model.predict_proba(x)[0], dtype=np.float64)
            if proba.size == 0:
                return 0.0
            k = len(ACTION_SPACE)
            max_p = float(np.max(proba))
            # Map [1/K, 1] -> [0, 1] for a more informative confidence.
            scaled = (max_p - 1.0 / k) / max(1.0 - 1.0 / k, _EPS)
            return float(np.clip(scaled, 0.0, 1.0))
        except Exception:  # noqa: BLE001 - best-effort
            return 0.0


class TwinStatefulQTeacher(Teacher):
    """Linear Q-teacher with clipped Bellman TD(0) updates.

    Update rule: ``w_a += lr * clip(target - w_a·x, ±td_clip) * x`` where
    ``target = r + gamma * max_a' Q(s', a')`` when ``next_state`` is given.
    """

    def __init__(
        self,
        lr: float = 0.1,
        gamma: float = 0.9,
        td_clip: float = 10.0,
        seed: int = 0,
    ) -> None:
        self.lr = float(lr)
        self.gamma = float(gamma)
        self.td_clip = float(td_clip)
        self.weights = np.zeros((len(_FEATURE_NAMES), len(ACTION_SPACE)))

    def predict(self, state: TwinOptimizationState) -> np.ndarray:
        q = state.to_feature_vector() @ self.weights
        return _softmax(q)

    def confidence(self, state: TwinOptimizationState) -> float:
        return 0.5

    def update(
        self,
        state: TwinOptimizationState,
        action: int,
        reward: float,
        next_state: Optional[TwinOptimizationState] = None,
        gamma: Optional[float] = None,
    ) -> None:
        x = state.to_feature_vector()
        g = self.gamma if gamma is None else float(gamma)
        if next_state is not None:
            q_next = next_state.to_feature_vector() @ self.weights
            target = float(reward) + g * float(np.max(q_next))
        else:
            target = float(reward)
        prediction = float(np.dot(x, self.weights[:, action]))
        td_error = float(np.clip(target - prediction, -self.td_clip, self.td_clip))
        self.weights[:, action] += self.lr * td_error * x

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lr": self.lr,
            "gamma": self.gamma,
            "td_clip": self.td_clip,
            "weights": self.weights.tolist(),
        }

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], *, seed: int = 0,
    ) -> "TwinStatefulQTeacher":
        obj = cls(
            lr=float(data.get("lr", 0.1)),
            gamma=float(data.get("gamma", 0.9)),
            td_clip=float(data.get("td_clip", 10.0)),
            seed=seed,
        )
        w = data.get("weights")
        if w is not None:
            w_arr = np.asarray(w, dtype=np.float64)
            if w_arr.shape == obj.weights.shape:
                obj.weights = w_arr
            else:
                logger.warning(
                    "Discarding Q-teacher weights with wrong shape: %s",
                    w_arr.shape,
                )
        return obj


# --------------------------------------------------------------------------- #
# Student
# --------------------------------------------------------------------------- #

class DistillationStudent:
    def __init__(
        self,
        feature_dim: int = len(_FEATURE_NAMES),
        n_classes: int = len(ACTION_SPACE),
        lr: float = 0.01,
        seed: int = 0,
        grad_clip: float = 1.0,
    ) -> None:
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = float(lr)
        self.counter = 0
        self.grad_clip = float(grad_clip)

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        logits = np.asarray(x, dtype=np.float64) @ self.weights + self.biases
        return _softmax(logits)

    def update(
        self,
        x: np.ndarray,
        teacher_probs: np.ndarray,
        reward: float,
        action: int,
        distill_weight: float = 0.7,
        rl_weight: float = 0.3,
    ) -> None:
        x = np.asarray(x, dtype=np.float64).reshape(-1)
        teacher_probs = _validate_probabilities(teacher_probs, name="teacher_probs")

        current = self.predict_proba(x)
        grad_distill = -(teacher_probs - current)

        one_hot = np.zeros_like(current)
        if 0 <= int(action) < one_hot.size:
            one_hot[int(action)] = 1.0
        grad_rl = -float(reward) * (one_hot - current)

        grad = distill_weight * grad_distill + rl_weight * grad_rl

        if self.grad_clip > 0:
            gnorm = float(np.linalg.norm(grad))
            if gnorm > self.grad_clip:
                grad = grad * (self.grad_clip / gnorm)

        self.weights -= self.lr * np.outer(x, grad)
        self.biases -= self.lr * grad
        self.counter += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "weights": self.weights.tolist(),
            "biases": self.biases.tolist(),
            "lr": self.lr,
            "counter": self.counter,
            "grad_clip": self.grad_clip,
        }

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], *, seed: int = 0,
    ) -> "DistillationStudent":
        obj = cls(
            lr=float(data.get("lr", 0.01)),
            seed=seed,
            grad_clip=float(data.get("grad_clip", 1.0)),
        )
        w = data.get("weights")
        if w is not None:
            w_arr = np.asarray(w, dtype=np.float64)
            if w_arr.shape == obj.weights.shape:
                obj.weights = w_arr
            else:
                logger.warning(
                    "Discarding student weights with wrong shape: %s", w_arr.shape,
                )
        b = data.get("biases")
        if b is not None:
            b_arr = np.asarray(b, dtype=np.float64)
            if b_arr.shape == obj.biases.shape:
                obj.biases = b_arr
            else:
                logger.warning(
                    "Discarding student biases with wrong shape: %s", b_arr.shape,
                )
        obj.counter = int(data.get("counter", 0))
        return obj


# --------------------------------------------------------------------------- #
# Replay buffer
# --------------------------------------------------------------------------- #

class ReplayBuffer:
    def __init__(
        self,
        max_size: int = 2000,
        seed: int = 0,
        feature_dim: int = len(_FEATURE_NAMES),
        n_classes: int = len(ACTION_SPACE),
    ) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be > 0.")
        self.buffer: Deque[Tuple[Any, ...]] = deque(maxlen=int(max_size))
        self._rng = random.Random(seed)
        self.feature_dim = feature_dim
        self.n_classes = n_classes

    def push(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_probs: np.ndarray,
    ) -> None:
        # Copy so callers cannot mutate buffer contents by reference.
        self.buffer.append((
            np.array(state_vec, dtype=np.float32, copy=True),
            int(action),
            float(reward),
            np.array(next_state_vec, dtype=np.float32, copy=True),
            np.array(teacher_probs, dtype=np.float32, copy=True),
        ))

    def sample(self, batch_size: int = 32) -> Tuple[np.ndarray, ...]:
        if not self.buffer:
            raise ValueError("Cannot sample from an empty buffer.")
        n = min(int(batch_size), len(self.buffer))
        batch = self._rng.sample(list(self.buffer), n)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (
            np.stack(states),
            list(actions),
            np.asarray(rewards, dtype=np.float64),
            np.stack(next_states),
            np.stack(teacher_probs),
        )

    def __len__(self) -> int:
        return len(self.buffer)

    def clear(self) -> None:
        self.buffer.clear()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_size": self.buffer.maxlen,
            "items": [
                (s.tolist(), a, r, ns.tolist(), tp.tolist())
                for (s, a, r, ns, tp) in self.buffer
            ],
        }

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], *, seed: int = 0,
    ) -> "ReplayBuffer":
        obj = cls(max_size=int(data.get("max_size", 2000)), seed=seed)
        for item in data.get("items", []):
            s, a, r, ns, tp = item
            obj.push(
                np.asarray(s, dtype=np.float32),
                int(a),
                float(r),
                np.asarray(ns, dtype=np.float32),
                np.asarray(tp, dtype=np.float32),
            )
        return obj


# --------------------------------------------------------------------------- #
# Optimizer
# --------------------------------------------------------------------------- #

@dataclass
class DistillationConfig:
    """Fallback config used when the caller does not supply all fields."""

    distillation_learning_rate: float = 0.01
    distillation_replay_size: int = 2000
    distillation_epsilon: float = 0.1
    distillation_train_every: int = 4
    distill_weight: float = 0.7
    rl_weight: float = 0.3
    # New knobs (defaults preserve prior behavior):
    teacher_mix: float = 0.2
    student_mix: float = 0.8
    batch_size: int = 8
    min_buffer_size: int = 8
    q_learning_rate: float = 0.1
    q_gamma: float = 0.9
    q_td_clip: float = 10.0
    grad_clip: float = 1.0


class DistillationTwinOptimizer:
    """Ensemble of teachers + distilled student + replay buffer."""

    ACTION_SPACE: Tuple[str, ...] = ACTION_SPACE

    def __init__(
        self,
        *,
        config: Any = None,
        seed: int = 0,
        model_path: Optional[str] = None,
    ) -> None:
        self.config = config
        self._rng = random.Random(seed)

        lr = float(_get_config_value(config, "distillation_learning_rate", 0.01))
        replay_size = int(
            _get_config_value(config, "distillation_replay_size", 2000)
        )
        epsilon = float(_get_config_value(config, "distillation_epsilon", 0.1))
        train_every = int(
            _get_config_value(config, "distillation_train_every", 4)
        )
        distill_weight = float(_get_config_value(config, "distill_weight", 0.7))
        rl_weight = float(_get_config_value(config, "rl_weight", 0.3))

        self.teacher_mix = float(_get_config_value(config, "teacher_mix", 0.2))
        self.student_mix = float(_get_config_value(config, "student_mix", 0.8))
        self.batch_size = int(_get_config_value(config, "batch_size", 8))
        self.min_buffer_size = int(
            _get_config_value(config, "min_buffer_size", 8)
        )
        q_lr = float(_get_config_value(config, "q_learning_rate", 0.1))
        q_gamma = float(_get_config_value(config, "q_gamma", 0.9))
        q_td_clip = float(_get_config_value(config, "q_td_clip", 10.0))
        grad_clip = float(_get_config_value(config, "grad_clip", 1.0))

        # ---- Validation ----
        if train_every <= 0:
            raise ValueError("distillation_train_every must be > 0.")
        if not (0.0 <= epsilon <= 1.0):
            raise ValueError("distillation_epsilon must be in [0, 1].")
        if lr <= 0 or q_lr <= 0:
            raise ValueError("learning rates must be positive.")
        if replay_size <= 0:
            raise ValueError("distillation_replay_size must be > 0.")
        if self.batch_size <= 0 or self.min_buffer_size <= 0:
            raise ValueError("batch_size and min_buffer_size must be > 0.")
        if distill_weight < 0 or rl_weight < 0 or (
            distill_weight + rl_weight
        ) <= 0:
            raise ValueError(
                "distill_weight and rl_weight must be non-negative with "
                "positive sum."
            )
        mix_sum = self.student_mix + self.teacher_mix
        if mix_sum <= 0:
            raise ValueError("student_mix + teacher_mix must be > 0.")
        self.student_mix /= mix_sum
        self.teacher_mix /= mix_sum

        # ---- Components ----
        self.student = DistillationStudent(lr=lr, seed=seed, grad_clip=grad_clip)
        self.q_teacher = TwinStatefulQTeacher(
            lr=q_lr, gamma=q_gamma, td_clip=q_td_clip, seed=seed,
        )
        self.teachers: List[Teacher] = [
            TwinRuleBasedTeacher(),
            TwinHistoricalMLTeacher(model_path=model_path),
            self.q_teacher,
        ]
        self.replay_buffer = ReplayBuffer(max_size=replay_size, seed=seed)
        self.epsilon = epsilon
        self.train_every = train_every
        self.distill_weight = distill_weight
        self.rl_weight = rl_weight
        self.counter = 0
        self._last_metrics: Dict[str, float] = {}

    # -- internals ----------------------------------------------------------- #

    def _combine_teacher_probs(
        self, state: TwinOptimizationState,
    ) -> np.ndarray:
        teacher_probs = np.zeros(len(ACTION_SPACE), dtype=np.float64)
        total_conf = 0.0
        for t in self.teachers:
            try:
                raw = t.predict(state)
                p = _validate_probabilities(raw, name=type(t).__name__)
            except Exception as exc:  # noqa: BLE001 - defensive
                logger.warning("Teacher %s predict failed: %s", type(t).__name__, exc)
                continue
            try:
                c = float(t.confidence(state))
            except Exception:  # noqa: BLE001 - defensive
                c = 0.0
            if not math.isfinite(c) or c <= 0:
                continue
            teacher_probs += p * c
            total_conf += c

        if total_conf > 0:
            teacher_probs /= total_conf
            return _validate_probabilities(teacher_probs)
        return np.full(len(ACTION_SPACE), 1.0 / len(ACTION_SPACE))

    # -- public API ---------------------------------------------------------- #

    async def select_strategy(
        self,
        state: TwinOptimizationState,
        *,
        exploration: bool = True,
        temperature: float = 1.0,
    ) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_probs = self._combine_teacher_probs(state)
        student_probs = self.student.predict_proba(state_vec)

        if exploration and self._rng.random() < self.epsilon:
            action_idx = self._rng.randint(0, len(ACTION_SPACE) - 1)
            return ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

        combined = (
            self.student_mix * student_probs + self.teacher_mix * teacher_probs
        )
        combined = _validate_probabilities(combined, name="combined_probs")

        if temperature and temperature > 0 and temperature != 1.0:
            logits = np.log(np.clip(combined, _EPS, None)) / temperature
            combined = _softmax(logits)

        # Sample from the distribution so ``temperature`` actually matters.
        action_idx = int(
            self._rng.choices(
                range(len(ACTION_SPACE)), weights=combined.tolist(), k=1,
            )[0]
        )
        return ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

    async def update(
        self,
        state_vec: np.ndarray,
        action_idx: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_probs: np.ndarray,
    ) -> None:
        teacher_probs = _validate_probabilities(teacher_probs, name="teacher_probs")
        self.replay_buffer.push(
            state_vec, action_idx, reward, next_state_vec, teacher_probs,
        )
        self.counter += 1

        if self.counter % self.train_every != 0:
            return
        if len(self.replay_buffer) < self.min_buffer_size:
            return

        states, actions, rewards, next_states, teacher_probs_batch = (
            self.replay_buffer.sample(self.batch_size)
        )

        for i in range(len(states)):
            self.student.update(
                states[i],
                teacher_probs_batch[i],
                float(rewards[i]),
                int(actions[i]),
                distill_weight=self.distill_weight,
                rl_weight=self.rl_weight,
            )

        for i in range(len(states)):
            self.q_teacher.update(
                TwinOptimizationState.from_feature_vector(states[i]),
                int(actions[i]),
                float(rewards[i]),
                next_state=TwinOptimizationState.from_feature_vector(
                    next_states[i]
                ),
            )

        self._last_metrics = {
            "avg_reward": float(np.mean(rewards)),
            "buffer_size": float(len(self.replay_buffer)),
            "student_counter": float(self.student.counter),
        }

    def statistics(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "teacher_count": len(self.teachers),
            "counter": self.counter,
            "epsilon": self.epsilon,
            "student_mix": self.student_mix,
            "teacher_mix": self.teacher_mix,
            "batch_size": self.batch_size,
            "train_every": self.train_every,
            **self._last_metrics,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "student": self.student.to_dict(),
            "q_teacher": self.q_teacher.to_dict(),
            "replay_buffer": self.replay_buffer.to_dict(),
            "counter": self.counter,
            "epsilon": self.epsilon,
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        *,
        config: Any = None,
        seed: int = 0,
        model_path: Optional[str] = None,
    ) -> "DistillationTwinOptimizer":
        obj = cls(config=config, seed=seed, model_path=model_path)

        if "student" in data:
            obj.student = DistillationStudent.from_dict(data["student"], seed=seed)
        if "q_teacher" in data:
            obj.q_teacher = TwinStatefulQTeacher.from_dict(
                data["q_teacher"], seed=seed,
            )
            # Replace the Q-teacher in the ensemble, preserving order.
            obj.teachers = [
                obj.q_teacher if isinstance(t, TwinStatefulQTeacher) else t
                for t in obj.teachers
            ]
        if "replay_buffer" in data:
            obj.replay_buffer = ReplayBuffer.from_dict(
                data["replay_buffer"], seed=seed,
            )
        obj.counter = int(data.get("counter", 0))
        if "epsilon" in data:
            obj.epsilon = float(data["epsilon"])
        return obj

    def close(self, *, flush: bool = True) -> None:
        # Hook for future persistence / flushing. Kept for API compatibility.
        return None


__all__ = [
    "ACTION_SPACE",
    "SCHEMA_VERSION",
    "DistillationConfig",
    "DistillationStudent",
    "DistillationTwinOptimizer",
    "ReplayBuffer",
    "Teacher",
    "TwinHistoricalMLTeacher",
    "TwinOptimizationState",
    "TwinRuleBasedTeacher",
    "TwinStatefulQTeacher",
]
