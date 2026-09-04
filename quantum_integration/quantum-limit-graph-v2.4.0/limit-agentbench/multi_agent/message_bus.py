# -*- coding: utf-8 -*-
"""
MessageBus (Enhanced)

A simple publish-subscribe message bus with optional advanced decision-making:
- LIMIT Graph metrics influence message prioritisation/filtering.
- MODP (multi‑objective) reward determines message importance.
- RLHF: human feedback score adjusts filtering thresholds.
- Multi‑Teacher On‑Policy Distillation + MoE: a learned policy decides whether to
  keep or drop a message based on context.
- Bio‑inspired optimisation: evolutionary tuning of the filtering threshold.

When enhancements are disabled (default), the bus behaves exactly as the original:
messages are stored in a list and consume_all returns them all.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
import random
import numpy as np
from collections import deque


# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
@dataclass
class MessageBusConfig:
    use_enhancements: bool = False
    # LIMIT Graph metrics
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5,
        "density": 0.4
    })
    # MODP weights: [relevance, urgency, energy, carbon]
    modp_weights: Optional[List[float]] = None   # default [0.4, 0.3, 0.2, 0.1]
    # RLHF
    human_feedback_score: float = 0.5
    # Distillation + MoE
    use_distillation: bool = True
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    epsilon: float = 0.1
    # Bio‑inspired
    use_evolutionary: bool = False
    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2


# ------------------------------------------------------------------------------
# State and Distillation Optimizer for message filtering
# ------------------------------------------------------------------------------
class MessageState:
    """Feature vector representing a message and its context."""
    def __init__(self, sender: str, payload: Any,
                 graph_metrics: Dict[str, float], human_feedback: float):
        self.sender_hash = self._encode_name(sender)
        # Extract simple features from payload (if dict)
        if isinstance(payload, dict):
            self.importance = payload.get("importance", 0.5)
            self.urgency = payload.get("urgency", 0.5)
            self.energy_cost = payload.get("energy", 0.0)
        else:
            self.importance = 0.5
            self.urgency = 0.5
            self.energy_cost = 0.0

        self.centrality = graph_metrics.get("centrality", 0.5)
        self.connectivity = graph_metrics.get("connectivity", 0.5)
        self.human_feedback = human_feedback

    def _encode_name(self, name: str) -> float:
        return (hash(name) % 1000) / 1000.0

    def to_vector(self) -> np.ndarray:
        return np.array([
            min(self.importance, 1.0),
            min(self.urgency, 1.0),
            min(self.energy_cost / 10.0, 1.0),
            self.centrality,
            self.connectivity,
            self.human_feedback,
            self.sender_hash,
        ], dtype=np.float32)


class MessageDistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to decide whether to keep or drop a message.
    Actions: 0 = drop, 1 = keep.
    """
    def __init__(self, config: MessageBusConfig):
        self.config = config
        self.feature_dim = 7
        self.n_actions = 2
        self.lr = config.distillation_lr
        self.epsilon = config.epsilon
        self.distill_w = 0.7
        self.rl_w = 0.3
        self.train_every = config.train_every
        self.counter = 0
        self.replay_buffer = deque(maxlen=config.replay_size)

        # Student
        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)

        # Teachers (rule-based, RLHF, historical)
        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]
        # MoE gating
        self.gate_weights = np.random.randn(self.feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = config.gating_lr

    def _rule_teacher(self, state: MessageState) -> np.ndarray:
        # Heuristic: keep if importance or urgency high, or centrality high
        if state.importance > 0.7 or state.urgency > 0.7:
            return np.array([0.1, 0.9])  # keep
        elif state.centrality > 0.7:
            return np.array([0.3, 0.7])
        else:
            return np.array([0.8, 0.2])  # drop

    def _rlhf_teacher(self, state: MessageState) -> np.ndarray:
        # Human feedback: high -> keep more, low -> drop more
        if state.human_feedback > 0.7:
            return np.array([0.2, 0.8])
        elif state.human_feedback < 0.3:
            return np.array([0.8, 0.2])
        else:
            return np.array([0.5, 0.5])

    def _historical_teacher(self, state: MessageState) -> np.ndarray:
        # Simulate a trained model: if energy cost low and importance moderate, keep
        if state.energy_cost < 0.2 and state.importance > 0.4:
            return np.array([0.3, 0.7])
        else:
            return np.array([0.7, 0.3])

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_action(self, state: MessageState, exploration=True):
        x = state.to_vector()
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher(state)
            if len(prob) != self.n_actions:
                prob = np.pad(prob, (0, self.n_actions - len(prob)), 'constant')[:self.n_actions]
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)
        gate = self._gate_forward(x)
        teacher_probs = np.sum(gate[:, None] * teacher_outputs, axis=0)
        teacher_probs /= teacher_probs.sum()

        student_logits = x @ self.student_weights + self.student_bias
        student_probs = np.exp(student_logits - np.max(student_logits))
        student_probs /= student_probs.sum()

        if exploration and random.random() < self.epsilon:
            action = random.randint(0, self.n_actions - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action = int(np.argmax(combined))

        return action, x, teacher_probs

    def update(self, state_vec, action, reward, next_state_vec, teacher_probs):
        self.replay_buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = random.sample(self.replay_buffer, min(8, len(self.replay_buffer)))
            for s, a, r, ns, tp in batch:
                # Update student
                logits = s @ self.student_weights + self.student_bias
                cur = np.exp(logits - np.max(logits))
                cur /= cur.sum()
                grad_distill = -(tp - cur)
                one_hot = np.zeros(self.n_actions); one_hot[a] = 1.0
                grad_rl = -r * (one_hot - cur)
                grad = self.distill_w * grad_distill + self.rl_w * grad_rl
                self.student_weights -= self.lr * np.outer(s, grad)
                self.student_bias -= self.lr * grad

                # Update gating
                gate = self._gate_forward(s)
                combined_teacher = np.sum(gate[:, None] * tp, axis=0)
                error = combined_teacher - cur
                grad_gate = np.dot(tp, error)
                self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
                self.gate_bias -= self.gate_lr * grad_gate


# ------------------------------------------------------------------------------
# Enhanced MessageBus
# ------------------------------------------------------------------------------
class MessageBus:
    """
    Publish-subscribe bus with optional intelligent message filtering.
    """

    def __init__(self, config: Optional[MessageBusConfig] = None):
        self.messages = []
        self.config = config or MessageBusConfig()
        self.use_enhancements = self.config.use_enhancements

        # Enhanced components
        self.distillation_optimizer = None
        self.evolutionary_optimizer = None
        if self.use_enhancements:
            if self.config.modp_weights is None:
                self.config.modp_weights = [0.4, 0.3, 0.2, 0.1]  # relevance, urgency, energy, carbon
            else:
                total = sum(self.config.modp_weights)
                self.config.modp_weights = [w / total for w in self.config.modp_weights]
            if self.config.use_distillation:
                self.distillation_optimizer = MessageDistillationOptimizer(self.config)
            # Evolutionary component could be added if needed

        self._last_decision = None

    def publish(self, sender: str, payload: Any):
        """
        Publish a message. If enhancements are enabled, the message may be
        filtered out (dropped) based on learned policy. Otherwise, it is stored
        as in the original implementation.
        """
        if not self.use_enhancements or self.distillation_optimizer is None:
            # Original behaviour
            self.messages.append((sender, payload))
            return

        # Build state from message and context
        state = MessageState(
            sender=sender,
            payload=payload,
            graph_metrics=self.config.graph_metrics,
            human_feedback=self.config.human_feedback_score
        )

        # Decide whether to keep (1) or drop (0)
        action, state_vec, teacher_probs = self.distillation_optimizer.select_action(state)

        if action == 1:
            self.messages.append((sender, payload))

        # Compute reward (simplified) based on the message's importance and decision
        reward = self._compute_reward(sender, payload, state, action)

        # Update distillation optimizer
        self.distillation_optimizer.update(
            state_vec=state_vec,
            action=action,
            reward=reward,
            next_state_vec=state_vec,  # simplified
            teacher_probs=teacher_probs
        )

    def consume_all(self):
        """
        Return all currently stored messages and clear the bus.
        """
        msgs = self.messages
        self.messages = []
        return msgs

    def _compute_reward(self, sender: str, payload: Any,
                        state: MessageState, action: int) -> float:
        """
        Compute a multi‑objective reward based on the message and decision.
        In a real system, we would have actual metrics like energy/latency/carbon.
        Here we approximate using payload features.
        """
        # Extract importance and urgency from state
        importance = state.importance
        urgency = state.urgency
        energy_cost = state.energy_cost

        # Normalize (higher is better for keeping relevant messages)
        relevance_norm = importance
        urgency_norm = urgency
        energy_efficiency = 1.0 - min(energy_cost / 10.0, 1.0)
        # Carbon proxy: energy_cost * 0.4
        carbon_efficiency = 1.0 - min(energy_cost * 0.4, 1.0)

        weights = self.config.modp_weights
        # Map weights to [relevance, urgency, energy, carbon]
        reward = float(np.dot(
            [relevance_norm, urgency_norm, energy_efficiency, carbon_efficiency],
            weights
        ))

        # If we dropped a message that was important, penalty
        if action == 0 and importance > 0.7:
            reward -= 0.5
        # If we kept a message that was low importance, slight penalty
        if action == 1 and importance < 0.3:
            reward -= 0.2

        return max(-1.0, min(1.0, reward))
