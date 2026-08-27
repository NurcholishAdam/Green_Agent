# src/enhancements/moe_expert_system/flexgen_moe_router.py
"""
Enhanced Mixture‑of‑Experts router for FlexGen policy selection.
Uses a learned gating network to choose among Pareto‑optimal policies.
"""

import logging
from typing import List, Dict, Any, Tuple, Optional
import numpy as np
import random

from ..gpu_optimization.flexgen_policy import FlexGenPolicy
from ..gpu_optimization.flexgen_policy_selector import FlexGenState
from ..gpu_optimization.reward import compute_reward
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..schemas.feedback_event import FeedbackEvent
from ..async_message_queue import AsyncMessageQueue
from ..logger import logger


class FlexGenMoERouter:
    """
    Gating network that selects a FlexGenPolicy from a set of experts.
    Experts are usually Pareto‑optimal policies from bio_inspired.
    """

    def __init__(
        self,
        experts: List[FlexGenPolicy],
        feature_dim: int = 12,
        hidden_dim: int = 64,
        temperature: float = 0.1,
        learning_rate: float = 0.01,
        message_queue: Optional[AsyncMessageQueue] = None,
    ):
        self.experts = experts
        self.n_experts = len(experts)
        self.temperature = temperature
        self.lr = learning_rate
        self.message_queue = message_queue
        # Simple two‑layer MLP gating
        # For simplicity, use linear softmax; can be extended to MLP
        self.weights = np.random.randn(feature_dim, self.n_experts) * 0.01
        self.last_expert_probs = None

    def _softmax(self, logits: np.ndarray) -> np.ndarray:
        logits = logits / max(self.temperature, 1e-6)
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def gate(self, state: FlexGenState) -> np.ndarray:
        """
        Compute expert selection probabilities given the state.
        """
        state_vec = state.to_feature_vector()
        logits = state_vec @ self.weights
        probs = self._softmax(logits)
        self.last_expert_probs = probs
        return probs

    def select_policy(self, state: FlexGenState, exploration: bool = False) -> Tuple[FlexGenPolicy, int]:
        """
        Choose an expert policy index.
        """
        probs = self.gate(state)
        if exploration and random.random() < 0.1:
            idx = random.randint(0, self.n_experts - 1)
        else:
            idx = int(np.argmax(probs))
        return self.experts[idx], idx

    def update(self, state: FlexGenState, expert_idx: int, reward: float) -> None:
        """
        Update gating weights using simple policy gradient.
        """
        probs = self.gate(state)
        # One‑hot target for expert_idx
        target = np.zeros(self.n_experts)
        target[expert_idx] = 1.0
        grad = (target - probs) * reward
        state_vec = state.to_feature_vector()
        self.weights += self.lr * np.outer(state_vec, grad)
        logger.debug(f"Updated gating weights, reward={reward:.3f}")

    async def publish_event(self, workload: WorkloadDescriptor, chosen_policy: FlexGenPolicy, expert_idx: int,
                            metrics: Dict, reward: float) -> None:
        """
        Publish a FeedbackEvent with expert selection details.
        """
        if not self.message_queue:
            return
        event = FeedbackEvent(
            source="moe_flexgen_router",
            feedback_type="routing",
            task_id=workload.task_id or "unknown",
            context={"expert_idx": expert_idx, "num_experts": self.n_experts},
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
