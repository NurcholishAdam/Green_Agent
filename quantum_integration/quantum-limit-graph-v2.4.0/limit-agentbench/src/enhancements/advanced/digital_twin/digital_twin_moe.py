# src/quantum_integration/digital_twin/digital_twin_moe.py

"""Mixture-of-Experts gating with expert-specific action distributions."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from .digital_twin_distillation import (
    ACTION_SPACE,
    TwinOptimizationState,
    _FEATURE_NAMES,
)
from .digital_twin_errors import DigitalTwinInputError

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 1

_DEFAULT_EXPERT_NAMES: Tuple[str, ...] = (
    "performance", "carbon", "cost", "adaptive",
)


@dataclass
class Expert:
    """Linear expert with a dedicated strategy distribution."""

    name: str
    weights: np.ndarray  # (feature_dim, n_actions)

    def predict(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.weights
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()


@dataclass(frozen=True)
class MoEResult:
    expert_name: str
    gate_probs: Tuple[float, ...]
    action_probs: Tuple[float, ...]
    selected_action: str
    selected_action_idx: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "expert_name": self.expert_name,
            "gate_probs": list(self.gate_probs),
            "action_probs": list(self.action_probs),
            "selected_action": self.selected_action,
            "selected_action_idx": self.selected_action_idx,
        }


class MoEGatingNetwork:
    """Mixture-of-experts gating.

    Each expert owns a strategy distribution. The gating network blends
    the expert distributions by gating weights and returns the combined
    action probabilities.
    """

    def __init__(
        self,
        storage: Optional[Any] = None,
        *,
        num_experts: int = 4,
        seed: int = 0,
        lr: float = 0.1,
        expert_names: Optional[List[str]] = None,
    ) -> None:
        if num_experts < 1:
            raise DigitalTwinInputError("num_experts must be >= 1.")
        names = list(expert_names or _DEFAULT_EXPERT_NAMES)
        if len(names) < num_experts:
            # Extend with synthetic names rather than truncating.
            for i in range(len(names), num_experts):
                names.append(f"expert_{i}")
        if len(names) > num_experts:
            names = names[:num_experts]
        self.storage = storage
        self.num_experts = int(num_experts)
        self.expert_names = tuple(names)
        self.lr = float(lr)
        self._rng = np.random.default_rng(seed)
        self.feature_dim = len(_FEATURE_NAMES)
        self.num_actions = len(ACTION_SPACE)

        # Gating weights: (num_experts, feature_dim)
        self.gating_weights = self._rng.standard_normal(
            (self.num_experts, self.feature_dim),
        )
        # Per-expert action weights: (num_experts, feature_dim, num_actions)
        self.expert_weights = self._rng.standard_normal(
            (self.num_experts, self.feature_dim, self.num_actions),
        ) * 0.1

    # ------------------------------------------------------------------ #
    def _encode_state(self, state: Dict[str, Any]) -> np.ndarray:
        return np.array(
            [float(state.get(name, 0.0)) for name in _FEATURE_NAMES],
            dtype=np.float64,
        )

    def _gate(self, state_vec: np.ndarray) -> np.ndarray:
        logits = self.gating_weights @ state_vec
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def _expert_action_probs(
        self, expert_idx: int, state_vec: np.ndarray,
    ) -> np.ndarray:
        logits = state_vec @ self.expert_weights[expert_idx]
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    # ------------------------------------------------------------------ #
    async def select_expert(
        self,
        state: Dict[str, Any],
        *,
        log_decision: bool = True,
    ) -> MoEResult:
        state_vec = self._encode_state(state)
        gate = self._gate(state_vec)

        expert_action_probs = np.stack([
            self._expert_action_probs(i, state_vec)
            for i in range(self.num_experts)
        ])
        combined = (gate[:, None] * expert_action_probs).sum(axis=0)
        selected_idx = int(np.argmax(combined))

        # The "expert" that produced the winning distribution.
        expert_idx = int(np.argmax(gate))

        if (
            log_decision
            and self.storage
            and hasattr(self.storage, "log_routing_decision")
        ):
            try:
                import hashlib, uuid
                sample_id = hashlib.sha256(
                    str(sorted(state.items())).encode()
                ).hexdigest()[:16]
                self.storage.log_routing_decision(
                    str(uuid.uuid4()),
                    sample_id,
                    self.expert_names[expert_idx],
                    float(gate[expert_idx]),
                )
            except Exception as exc:  # noqa: BLE001 - best-effort
                logger.warning("Failed to log routing decision: %s", exc)

        return MoEResult(
            expert_name=self.expert_names[expert_idx],
            gate_probs=tuple(float(x) for x in gate),
            action_probs=tuple(float(x) for x in combined),
            selected_action=ACTION_SPACE[selected_idx],
            selected_action_idx=selected_idx,
        )

    async def add_training_sample(
        self,
        state: Dict[str, Any],
        selected_expert: str,
        reward: float,
    ) -> None:
        """Online update with reward-scaled gradient.

        A positive reward reinforces the selected expert's gating weight;
        a negative reward pushes it away. Both the gating weights and the
        expert's action weights are updated.
        """
        if selected_expert not in self.expert_names:
            raise DigitalTwinInputError(
                f"unknown expert {selected_expert!r}."
            )
        expert_idx = self.expert_names.index(selected_expert)
        state_vec = self._encode_state(state)

        # Gating gradient.
        gate = self._gate(state_vec)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        grad_gate = (gate - target)[:, None] * state_vec[None, :]
        self.gating_weights -= self.lr * reward * grad_gate

        # Expert action distribution gradient.
        # We use a simple policy-gradient-like update: reward * (one_hot - p) * x.
        action_probs = self._expert_action_probs(expert_idx, state_vec)
        # If the reward is positive, push toward the most probable action;
        # if negative, push away from it.
        best_action = int(np.argmax(action_probs))
        one_hot = np.zeros(self.num_actions)
        one_hot[best_action] = 1.0
        grad_expert = -reward * np.outer(
            state_vec, (one_hot - action_probs),
        )
        self.expert_weights[expert_idx] -= self.lr * grad_expert

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "expert_names": list(self.expert_names),
            "gating_weights": self.gating_weights.tolist(),
            "expert_weights": self.expert_weights.tolist(),
        }

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], *, seed: int = 0,
    ) -> "MoEGatingNetwork":
        names = list(data.get("expert_names") or _DEFAULT_EXPERT_NAMES)
        obj = cls(
            num_experts=len(names),
            seed=seed,
            expert_names=names,
        )
        gw = data.get("gating_weights")
        if gw is not None:
            obj.gating_weights = np.asarray(gw, dtype=np.float64)
        ew = data.get("expert_weights")
        if ew is not None:
            obj.expert_weights = np.asarray(ew, dtype=np.float64)
        return obj

    def statistics(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "num_experts": self.num_experts,
            "expert_names": list(self.expert_names),
            "num_actions": self.num_actions,
        }

    def close(self, *, flush: bool = True) -> None:
        return None


__all__ = ["SCHEMA_VERSION", "Expert", "MoEGatingNetwork", "MoEResult"]
