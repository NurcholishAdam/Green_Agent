# src/quantum_integration/digital_twin/digital_twin_distillation.py

"""Distillation fallback: teachers, student, replay buffer, optimizer."""

from __future__ import annotations

import logging
import random
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Sequence, Tuple

import numpy as np

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 1

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


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: TwinOptimizationState) -> np.ndarray: ...
    @abstractmethod
    def confidence(self, state: TwinOptimizationState) -> float: ...


class TwinRuleBasedTeacher(Teacher):
    """Deterministic rule-based teacher."""

    def predict(self, state: TwinOptimizationState) -> np.ndarray:
        probs = np.ones(len(ACTION_SPACE)) * 0.1
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
    """Best-effort historical ML teacher (sklearn-based, optional)."""

    def __init__(self, model_path: Optional[str] = None) -> None:
        self.model = None
        if model_path and Path(model_path).exists():
            try:
                import joblib
                self.model = joblib.load(model_path)
                # Verify the model has the expected class count.
                classes = getattr(self.model, "classes_", None)
                if classes is None or len(classes) != len(ACTION_SPACE):
                    logger.warning(
                        "Historical ML teacher has %s classes; expected %s.",
                        None if classes is None else len(classes),
                        len(ACTION_SPACE),
                    )
                    self.model = None
            except Exception as exc:  # noqa: BLE001 - best-effort
                logger.warning("Historical ML teacher disabled: %s", exc)
                self.model = None

    def predict(self, state: TwinOptimizationState) -> np.ndarray:
        if self.model is None:
            return np.ones(len(ACTION_SPACE)) / len(ACTION_SPACE)
        x = state.to_feature_vector().reshape(1, -1)
        proba = np.asarray(self.model.predict_proba(x)[0], dtype=np.float64)
        # Guard against shape drift.
        if proba.size != len(ACTION_SPACE):
            padded = np.zeros(len(ACTION_SPACE), dtype=np.float64)
            padded[: min(proba.size, len(ACTION_SPACE))] = (
                proba[: len(ACTION_SPACE)]
            )
            proba = padded
        s = proba.sum()
        return proba / s if s > 0 else np.ones(len(ACTION_SPACE)) / len(ACTION_SPACE)

    def confidence(self, state: TwinOptimizationState) -> float:
        return 0.7 if self.model is not None else 0.0


class TwinStatefulQTeacher(Teacher):
    """Linear Q-teacher with proper Bellman-free TD-update.

    Uses a bandit-style update ``w += lr * (r - w·x) * x`` on the
    selected action. The update uses ``next_state`` when provided.
    """

    def __init__(self, lr: float = 0.1, seed: int = 0) -> None:
        self.lr = lr
        self.weights = np.zeros((len(_FEATURE_NAMES), len(ACTION_SPACE)))
        self._rng = random.Random(seed)

    def predict(self, state: TwinOptimizationState) -> np.ndarray:
        q = state.to_feature_vector() @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: TwinOptimizationState) -> float:
        return 0.5

    def update(
        self,
        state: TwinOptimizationState,
        action: int,
        reward: float,
        next_state: Optional[TwinOptimizationState] = None,
        gamma: float = 0.9,
    ) -> None:
        x = state.to_feature_vector()
        # Bellman target: reward + gamma * max_a Q(s', a).
        if next_state is not None:
            q_next = next_state.to_feature_vector() @ self.weights
            target = reward + gamma * float(np.max(q_next))
        else:
            target = reward
        prediction = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (target - prediction) * x

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lr": self.lr,
            "weights": self.weights.tolist(),
        }

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], *, seed: int = 0,
    ) -> "TwinStatefulQTeacher":
        obj = cls(lr=float(data.get("lr", 0.1)), seed=seed)
        w = data.get("weights")
        if w is not None:
            obj.weights = np.asarray(w, dtype=np.float64)
        return obj


class DistillationStudent:
    def __init__(
        self,
        feature_dim: int = len(_FEATURE_NAMES),
        n_classes: int = len(ACTION_SPACE),
        lr: float = 0.01,
        seed: int = 0,
    ) -> None:
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.counter = 0
        self._rng = random.Random(seed)

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        logits = x @ self.weights + self.biases
        exp_logits = np.exp(logits - np.max(logits))
        return exp_logits / exp_logits.sum()

    def update(
        self,
        x: np.ndarray,
        teacher_probs: np.ndarray,
        reward: float,
        action: int,
        distill_weight: float = 0.7,
        rl_weight: float = 0.3,
    ) -> None:
        current = self.predict_proba(x)
        grad_distill = -(teacher_probs - current)
        one_hot = np.zeros_like(current)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(x, grad)
        self.biases -= self.lr * grad
        self.counter += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "weights": self.weights.tolist(),
            "biases": self.biases.tolist(),
            "lr": self.lr,
            "counter": self.counter,
        }

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], *, seed: int = 0,
    ) -> "DistillationStudent":
        obj = cls(lr=float(data.get("lr", 0.01)), seed=seed)
        w = data.get("weights")
        if w is not None:
            obj.weights = np.asarray(w, dtype=np.float64)
        b = data.get("biases")
        if b is not None:
            obj.biases = np.asarray(b, dtype=np.float64)
        obj.counter = int(data.get("counter", 0))
        return obj


class ReplayBuffer:
    def __init__(self, max_size: int = 2000, seed: int = 0) -> None:
        self.buffer: Deque[Tuple[Any, ...]] = deque(maxlen=max_size)
        self._rng = random.Random(seed)

    def push(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_probs: np.ndarray,
    ) -> None:
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32) -> Tuple[np.ndarray, ...]:
        if not self.buffer:
            raise ValueError("Cannot sample from an empty buffer.")
        n = min(batch_size, len(self.buffer))
        batch = self._rng.sample(list(self.buffer), n)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (
            np.stack(states),
            list(actions),
            np.asarray(rewards),
            np.stack(next_states),
            np.stack(teacher_probs),
        )

    def __len__(self) -> int:
        return len(self.buffer)


class DistillationTwinOptimizer:
    """Ensemble of teachers + distilled student + replay buffer."""

    ACTION_SPACE: Tuple[str, ...] = ACTION_SPACE

    def __init__(
        self,
        *,
        config: Any,
        seed: int = 0,
        model_path: Optional[str] = None,
    ) -> None:
        self.config = config
        self._rng = random.Random(seed)
        self.student = DistillationStudent(
            lr=config.distillation_learning_rate, seed=seed,
        )
        self.q_teacher = TwinStatefulQTeacher(lr=0.1, seed=seed)
        self.teachers: List[Teacher] = [
            TwinRuleBasedTeacher(),
            TwinHistoricalMLTeacher(model_path=model_path),
            self.q_teacher,
        ]
        self.replay_buffer = ReplayBuffer(
            config.distillation_replay_size, seed=seed,
        )
        self.epsilon = config.distillation_epsilon
        self.train_every = config.distillation_train_every
        self.distill_weight = config.distill_weight
        self.rl_weight = config.rl_weight
        self.counter = 0

    def _combine_teacher_probs(
        self, state: TwinOptimizationState,
    ) -> np.ndarray:
        teacher_probs = np.zeros(len(ACTION_SPACE))
        total_conf = 0.0
        for t in self.teachers:
            p = t.predict(state)
            c = t.confidence(state)
            teacher_probs += p * c
            total_conf += c
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(len(ACTION_SPACE)) / len(ACTION_SPACE)
        return teacher_probs

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
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            if temperature != 1.0 and temperature > 0:
                # Apply temperature-scaled softmax.
                logits = np.log(np.clip(combined, 1e-12, None)) / temperature
                exp = np.exp(logits - np.max(logits))
                combined = exp / exp.sum()
            action_idx = int(np.argmax(combined))
        return ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

    async def update(
        self,
        state_vec: np.ndarray,
        action_idx: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_probs: np.ndarray,
    ) -> None:
        self.replay_buffer.push(
            state_vec, action_idx, reward, next_state_vec, teacher_probs,
        )
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            states, actions, rewards, next_states, teacher_probs_batch = (
                self.replay_buffer.sample(8)
            )
            for i in range(len(states)):
                self.student.update(
                    states[i], teacher_probs_batch[i], float(rewards[i]),
                    actions[i],
                    distill_weight=self.distill_weight,
                    rl_weight=self.rl_weight,
                )
                # Q-teacher Bellman update.
                self.q_teacher.update(
                    TwinOptimizationState.from_feature_vector(states[i]),
                    actions[i],
                    float(rewards[i]),
                    next_state=TwinOptimizationState.from_feature_vector(
                        next_states[i]
                    ),
                )

    def statistics(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "teacher_count": len(self.teachers),
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "student": self.student.to_dict(),
            "q_teacher": self.q_teacher.to_dict(),
            "counter": self.counter,
        }

    def close(self, *, flush: bool = True) -> None:
        return None


__all__ = [
    "ACTION_SPACE",
    "SCHEMA_VERSION",
    "DistillationStudent",
    "DistillationTwinOptimizer",
    "ReplayBuffer",
    "Teacher",
    "TwinHistoricalMLTeacher",
    "TwinOptimizationState",
    "TwinRuleBasedTeacher",
    "TwinStatefulQTeacher",
]
