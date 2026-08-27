"""
Enhanced Mixture‑of‑Experts router for FlexGen policy selection.
Uses a two‑layer MLP gating network with persistence, baseline subtraction,
epsilon annealing, dynamic experts, and transparent event logging.
"""

import logging
from typing import List, Dict, Any, Tuple, Optional, Union
import numpy as np
import random
import json
import os
from pathlib import Path

from ..gpu_optimization.flexgen_policy import FlexGenPolicy
from ..gpu_optimization.flexgen_policy_selector import FlexGenState
from ..gpu_optimization.reward import compute_reward
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..schemas.feedback_event import FeedbackEvent
from ..async_message_queue import AsyncMessageQueue
from ..logger import logger


class FlexGenMoERouter:
    """
    MoE router with a two‑layer MLP gating network.
    """

    def __init__(
        self,
        experts: List[FlexGenPolicy],
        feature_dim: int = 12,
        hidden_dim: int = 64,
        temperature: float = 1.0,
        learning_rate: float = 0.01,
        epsilon: float = 0.1,
        epsilon_decay: float = 0.999,
        message_queue: Optional[AsyncMessageQueue] = None,
        persistence_path: Optional[str] = "moe_router_weights.json",
        use_baseline: bool = True,
        l2_reg: float = 0.0001,
    ):
        """
        Args:
            experts: List of FlexGenPolicy experts.
            feature_dim: Dimension of input state vector.
            hidden_dim: Hidden layer size for MLP.
            temperature: Softmax temperature (higher = more exploration).
            learning_rate: Learning rate for weight updates.
            epsilon: Initial epsilon for ε‑greedy exploration.
            epsilon_decay: Multiplicative decay factor for epsilon after each update.
            message_queue: Optional AsyncMessageQueue for event publishing.
            persistence_path: Path to save/load gating weights (JSON).
            use_baseline: If True, subtract moving average reward baseline.
            l2_reg: L2 regularization coefficient.
        """
        self.experts = experts
        self.n_experts = len(experts)
        self.feature_dim = feature_dim
        self.hidden_dim = hidden_dim
        self.temperature = temperature
        self.lr = learning_rate
        self.epsilon = epsilon
        self.epsilon_decay = epsilon_decay
        self.message_queue = message_queue
        self.persistence_path = persistence_path
        self.use_baseline = use_baseline
        self.l2_reg = l2_reg

        # Two‑layer MLP: input -> hidden (ReLU) -> output
        self.W1 = np.random.randn(feature_dim, hidden_dim) * 0.01
        self.b1 = np.zeros(hidden_dim)
        self.W2 = np.random.randn(hidden_dim, self.n_experts) * 0.01
        self.b2 = np.zeros(self.n_experts)

        # Baseline for variance reduction
        self.baseline = 0.0
        self.baseline_alpha = 0.1  # moving average factor

        self.last_expert_probs = None
        self.last_state_vec = None
        self.step_count = 0

        # Load persisted weights if available
        self._load_weights()

    def _softmax(self, logits: np.ndarray) -> np.ndarray:
        logits = logits / max(self.temperature, 1e-6)
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def _forward(self, state_vec: np.ndarray) -> np.ndarray:
        """Compute logits through MLP."""
        hidden = np.maximum(0, state_vec @ self.W1 + self.b1)  # ReLU
        logits = hidden @ self.W2 + self.b2
        return logits

    def gate(self, state: FlexGenState) -> np.ndarray:
        """
        Compute expert selection probabilities given the state.
        """
        state_vec = state.to_feature_vector()
        # Ensure dimension matches
        if len(state_vec) != self.feature_dim:
            # Resize W1 if needed
            logger.warning(f"State dim {len(state_vec)} != feature_dim {self.feature_dim}; resizing.")
            self.feature_dim = len(state_vec)
            self.W1 = np.random.randn(self.feature_dim, self.hidden_dim) * 0.01
            self.b1 = np.zeros(self.hidden_dim)
        logits = self._forward(state_vec)
        probs = self._softmax(logits)
        self.last_expert_probs = probs
        self.last_state_vec = state_vec
        return probs

    def select_policy(self, state: FlexGenState, exploration: bool = True) -> Tuple[FlexGenPolicy, int]:
        """
        Choose an expert policy index.
        """
        probs = self.gate(state)
        if exploration and random.random() < self.epsilon:
            idx = random.randint(0, self.n_experts - 1)
        else:
            idx = int(np.argmax(probs))
        return self.experts[idx], idx

    def update(self, state: FlexGenState, expert_idx: int, reward: float) -> None:
        """
        Update gating weights using policy gradient with baseline and L2 regularization.
        """
        probs = self.gate(state)  # updates last_state_vec and last_expert_probs
        state_vec = self.last_state_vec

        # Update baseline
        if self.use_baseline:
            self.baseline = self.baseline_alpha * reward + (1 - self.baseline_alpha) * self.baseline
            advantage = reward - self.baseline
        else:
            advantage = reward

        # One‑hot target
        target = np.zeros(self.n_experts)
        target[expert_idx] = 1.0

        # Gradient of log softmax: (target - probs)
        grad_logits = (target - probs) * advantage

        # Backprop through MLP
        hidden = np.maximum(0, state_vec @ self.W1 + self.b1)
        grad_hidden = grad_logits @ self.W2.T
        grad_hidden[hidden <= 0] = 0  # ReLU derivative

        # Update weights with L2 regularization
        self.W2 += self.lr * (np.outer(hidden, grad_logits) - self.l2_reg * self.W2)
        self.b2 += self.lr * grad_logits
        self.W1 += self.lr * (np.outer(state_vec, grad_hidden) - self.l2_reg * self.W1)
        self.b1 += self.lr * grad_hidden

        # Decay epsilon
        self.epsilon = max(0.01, self.epsilon * self.epsilon_decay)

        self.step_count += 1
        logger.debug(f"Updated gating weights, reward={reward:.3f}, advantage={advantage:.3f}, epsilon={self.epsilon:.3f}")

        # Save weights periodically
        if self.persistence_path and self.step_count % 100 == 0:
            self._save_weights()

    async def publish_event(self, workload: WorkloadDescriptor, chosen_policy: FlexGenPolicy, expert_idx: int,
                            metrics: Dict, reward: float) -> None:
        """
        Publish a FeedbackEvent with expert selection details and full gating distribution.
        """
        if not self.message_queue or FeedbackEvent is None:
            return
        # Include full gating distribution in metadata for transparency
        metadata = {
            "gating_probs": self.last_expert_probs.tolist() if self.last_expert_probs is not None else [],
            "expert_idx": expert_idx,
            "num_experts": self.n_experts,
            "epsilon": self.epsilon,
        }
        event = FeedbackEvent(
            source="moe_flexgen_router",
            feedback_type="routing",
            task_id=workload.task_id or "unknown",
            context=metadata,
            action={"selected_action": str(chosen_policy.to_dict()),
                    "selected_rank": expert_idx,
                    "confidence_score": self.last_expert_probs[expert_idx] if self.last_expert_probs is not None else 0.5},
            performance={"quality_score": metrics.get("quality_score", 0.9),
                         "latency_ms": metrics.get("latency_ms", 0),
                         "energy_joules": metrics.get("energy_joules", 0),
                         "carbon_g": metrics.get("carbon_g", 0),
                         "helium_cost": 0,
                         "duration_ms": 0},
            adaptive_cost_value=reward,
            tags=["moe", "flexgen_policy", "expert_selection"],
        )
        await self.message_queue.publish("moe_events", event.to_json())

    def add_expert(self, policy: FlexGenPolicy) -> None:
        """Add a new expert and expand output layer."""
        self.experts.append(policy)
        new_n = len(self.experts)
        # Expand W2 and b2
        new_W2 = np.random.randn(self.hidden_dim, new_n) * 0.01
        new_b2 = np.zeros(new_n)
        # Copy old weights
        new_W2[:, :self.n_experts] = self.W2
        new_b2[:self.n_experts] = self.b2
        self.W2 = new_W2
        self.b2 = new_b2
        self.n_experts = new_n

    def remove_expert(self, index: int) -> None:
        """Remove an expert at the given index."""
        if 0 <= index < self.n_experts:
            del self.experts[index]
            # Remove corresponding column from W2 and b2
            self.W2 = np.delete(self.W2, index, axis=1)
            self.b2 = np.delete(self.b2, index, axis=0)
            self.n_experts -= 1

    def _save_weights(self) -> None:
        """Save gating weights to JSON."""
        if not self.persistence_path:
            return
        data = {
            "W1": self.W1.tolist(),
            "b1": self.b1.tolist(),
            "W2": self.W2.tolist(),
            "b2": self.b2.tolist(),
            "baseline": self.baseline,
            "epsilon": self.epsilon,
            "step_count": self.step_count,
        }
        with open(self.persistence_path, 'w') as f:
            json.dump(data, f)
        logger.info(f"MoE router weights saved to {self.persistence_path}")

    def _load_weights(self) -> None:
        """Load gating weights from JSON if file exists."""
        if not self.persistence_path or not os.path.exists(self.persistence_path):
            return
        try:
            with open(self.persistence_path, 'r') as f:
                data = json.load(f)
            self.W1 = np.array(data["W1"])
            self.b1 = np.array(data["b1"])
            self.W2 = np.array(data["W2"])
            self.b2 = np.array(data["b2"])
            self.baseline = data.get("baseline", 0.0)
            self.epsilon = data.get("epsilon", self.epsilon)
            self.step_count = data.get("step_count", 0)
            # Ensure dimensions match current experts
            if self.W2.shape[1] != self.n_experts:
                logger.warning("Loaded weights have different expert count; reinitializing output layer.")
                self.W2 = np.random.randn(self.hidden_dim, self.n_experts) * 0.01
                self.b2 = np.zeros(self.n_experts)
            logger.info(f"MoE router weights loaded from {self.persistence_path}")
        except Exception as e:
            logger.error(f"Failed to load weights: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Return current router statistics."""
        return {
            "num_experts": self.n_experts,
            "epsilon": self.epsilon,
            "baseline": self.baseline,
            "step_count": self.step_count,
        }
