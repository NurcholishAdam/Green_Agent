"""
Distillation-based FlexGen Policy Selector.

Uses multi-teacher on-policy distillation to choose the best offloading
policy from a small set of Pareto-optimal candidates. The state includes
workload, node, carbon intensity, and historical policy performance.
"""

import asyncio
import random
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from collections import deque
import logging

from .flexgen_policy import FlexGenPolicy
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..logger import logger


@dataclass
class FlexGenState:
    """State for the distillation agent."""
    # Workload
    tokens: float
    latency_target: float
    # Node
    gpu_memory_gb: float
    cpu_memory_gb: float
    disk_bandwidth_gbps: float
    # Environment
    carbon_intensity: float
    # Historical
    recent_success_rate: float
    avg_reward: float
    # Policy-specific (one-hot or index of candidate)
    policy_idx: int  # index into candidate list

    def to_feature_vector(self) -> np.ndarray:
        features = [
            min(self.tokens / 10000.0, 1.0),
            min(self.latency_target / 1000.0, 1.0),
            min(self.gpu_memory_gb / 80.0, 1.0),
            min(self.cpu_memory_gb / 256.0, 1.0),
            min(self.disk_bandwidth_gbps / 10.0, 1.0),
            min(self.carbon_intensity / 1000.0, 1.0),
            self.recent_success_rate,
            self.avg_reward,
            self.policy_idx / 20.0,  # placeholder normalization
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base (same pattern as before)
class Teacher:
    def predict(self, state: FlexGenState) -> np.ndarray: ...
    def confidence(self, state: FlexGenState) -> float: ...


class FlexGenRuleBasedTeacher(Teacher):
    """Simple heuristic teacher."""
    def predict(self, state: FlexGenState) -> np.ndarray:
        probs = np.ones(20) * 0.05  # uniform over 20 candidate policies
        # Bias toward policies that offload when carbon high or GPU small
        # (This is a simplified placeholder; in reality we'd have a mapping)
        return probs / probs.sum()

    def confidence(self, state: FlexGenState) -> float:
        return 0.5


class FlexGenHistoricalMLTeacher(Teacher):
    """Placeholder; would load a trained model."""
    def predict(self, state: FlexGenState) -> np.ndarray:
        return np.ones(20) / 20

    def confidence(self, state: FlexGenState) -> float:
        return 0.0  # no model yet


class FlexGenStatefulQTeacher(Teacher):
    """Linear Q-learning teacher."""
    def __init__(self, feature_dim: int = 9, n_actions: int = 20, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((feature_dim, n_actions))

    def predict(self, state: FlexGenState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: FlexGenState) -> float:
        return 0.5

    def update(self, state: FlexGenState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x


# The selector class combining teachers and student
class DistillationFlexGenSelector:
    def __init__(self, n_candidates: int = 20, config: Optional[Dict] = None):
        self.n_candidates = n_candidates
        self.config = config or {}
        self.student = DistillationStudent(feature_dim=9, n_classes=n_candidates)
        self.teachers = [
            FlexGenRuleBasedTeacher(),
            FlexGenHistoricalMLTeacher(),
            FlexGenStatefulQTeacher(feature_dim=9, n_actions=n_candidates),
        ]
        self.replay_buffer = ReplayBuffer(max_size=2000)
        self.epsilon = self.config.get('epsilon', 0.1)
        self.train_every = 10
        self.counter = 0

    async def select_policy(
        self,
        candidates: List[FlexGenPolicy],
        state: FlexGenState,
        exploration: bool = True
    ) -> Tuple[int, np.ndarray, np.ndarray]:
        """
        Choose an index into the candidate list.
        Returns (action_idx, state_vec, teacher_probs).
        """
        state_vec = state.to_feature_vector()
        n = len(candidates)

        teacher_probs = np.zeros(n)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            if len(prob) != n:
                # resize if necessary
                prob = np.resize(prob, n)
                prob = prob / prob.sum()
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(n) / n

        student_probs = self.student.predict_proba(state_vec, n)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, n - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))

        return action_idx, state_vec, teacher_probs

    async def update(self, state_vec, action_idx, reward, next_state_vec, teacher_probs):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, tprobs = batch
            for i in range(len(states)):
                self.student.update(states[i], tprobs[i], rewards[i], actions[i])


# Import shared components (assuming they exist in project)
from .distillation_components import DistillationStudent, ReplayBuffer  # (or redefine locally)
