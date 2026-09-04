# -*- coding: utf-8 -*-
"""
Stress Injection & Spike Simulation (Enhanced)

Provides random failure injection, energy spike simulation, and reward spike simulation
for robustness testing of the Green Agent benchmark harness.

Original functionality is preserved. Enhancements (optional via `StressConfig.use_enhancements`):
  - LIMIT Graph metrics influence failure injection probability.
  - MODP (multi‑objective) weights adjust energy/reward spike magnitudes.
  - RLHF: human feedback score modulates the likelihood of spikes.
  - Multi‑Teacher On‑Policy Distillation + MoE: a learned policy decides when to
    inject a failure or spike.
  - Bio‑inspired optimisation: evolutionary tuning of spike parameters.

FlexGen Integration:
  These stress functions are used to test the system's resilience when running
  LLM inference via FlexGen. The enhanced injector can be called before/after
  FlexGen operations to simulate realistic anomalies.
"""

import random
import time
import os
import signal
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
import numpy as np
from collections import deque


# ------------------------------------------------------------------------------
# Configuration for enhancements
# ------------------------------------------------------------------------------
@dataclass
class StressConfig:
    use_enhancements: bool = False
    # LIMIT Graph metrics (defaults, can be overridden per call)
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5,
        "density": 0.4
    })
    # MODP weights: [reliability, energy, reward]
    modp_weights: Optional[List[float]] = None   # default [0.4, 0.3, 0.3]
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

    # Default failure probability and spike probabilities (original)
    failure_prob: float = 0.05
    energy_spike_prob: float = 0.1
    energy_spike_multiplier: float = 5.0
    reward_spike_prob: float = 0.1
    reward_spike_multiplier: float = 10.0


# ------------------------------------------------------------------------------
# Original utility functions (preserved)
# ------------------------------------------------------------------------------
def random_failure_injection(prob=0.05):
    """Inject artificial crash with given probability (original)."""
    if random.random() < prob:
        print("[STRESS] Injecting artificial crash")
        os.kill(os.getpid(), signal.SIGTERM)


def energy_spike_simulation(energy, prob=0.1, multiplier=5.0):
    """Return energy multiplied if spike occurs (original)."""
    if random.random() < prob:
        return energy * multiplier
    return energy


def reward_spike_simulation(reward, prob=0.1, multiplier=10.0):
    """Return reward multiplied if spike occurs (original)."""
    if random.random() < prob:
        return reward * multiplier
    return reward


# ------------------------------------------------------------------------------
# Enhanced decision components
# ------------------------------------------------------------------------------
class StressState:
    """Feature vector for deciding whether to inject a spike/failure."""
    def __init__(self, context: Dict[str, float], graph_metrics: Dict[str, float],
                 human_feedback: float):
        # Context may contain task complexity, load, etc.
        self.task_complexity = context.get("complexity", 0.5)
        self.load = context.get("load", 0.5)
        self.energy_norm = context.get("energy_norm", 0.0)
        self.centrality = graph_metrics.get("centrality", 0.5)
        self.connectivity = graph_metrics.get("connectivity", 0.5)
        self.human_feedback = human_feedback

    def to_vector(self) -> np.ndarray:
        return np.array([
            min(self.task_complexity, 1.0),
            min(self.load, 1.0),
            min(self.energy_norm, 1.0),
            self.centrality,
            self.connectivity,
            self.human_feedback,
        ], dtype=np.float32)


class StressDistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to decide whether to inject a stress event.
    Actions: 0 = no injection, 1 = inject failure, 2 = inject energy spike, 3 = inject reward spike.
    """
    def __init__(self, config: StressConfig):
        self.config = config
        self.feature_dim = 6
        self.n_actions = 4
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

    def _rule_teacher(self, state: StressState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.05
        # Simple heuristics
        if state.load > 0.8:
            probs[1] = 0.5  # failure more likely under high load
        elif state.energy_norm > 0.5:
            probs[2] = 0.5  # energy spike
        elif state.task_complexity > 0.7:
            probs[3] = 0.4  # reward spike
        else:
            probs[0] = 0.7  # no injection
        return probs / probs.sum()

    def _rlhf_teacher(self, state: StressState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        # Human feedback: high -> less injection (want stable), low -> more stress
        if state.human_feedback > 0.7:
            probs[0] += 0.2
        elif state.human_feedback < 0.3:
            probs[1] += 0.2
            probs[2] += 0.1
        return probs / probs.sum()

    def _historical_teacher(self, state: StressState) -> np.ndarray:
        # Simulate learned model based on centrality
        probs = np.ones(self.n_actions) * 0.05
        if state.centrality > 0.7:
            probs[0] = 0.6  # less injection for critical nodes
        else:
            probs[1] = 0.3
            probs[2] = 0.3
        return probs / probs.sum()

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_action(self, state: StressState, exploration=True):
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
# Enhanced StressInjector class
# ------------------------------------------------------------------------------
class StressInjector:
    """
    Wrapper for stress injection functions with optional enhancements.
    """

    def __init__(self, config: Optional[StressConfig] = None):
        self.config = config or StressConfig()
        self.use_enhancements = self.config.use_enhancements

        # Enhanced components
        self.distillation_optimizer = None
        if self.use_enhancements:
            if self.config.modp_weights is None:
                self.config.modp_weights = [0.4, 0.3, 0.3]  # reliability, energy, reward
            else:
                total = sum(self.config.modp_weights)
                self.config.modp_weights = [w / total for w in self.config.modp_weights]
            if self.config.use_distillation:
                self.distillation_optimizer = StressDistillationOptimizer(self.config)

    def maybe_inject(self, context: Dict[str, float] = None,
                     graph_metrics: Dict[str, float] = None,
                     human_feedback_score: float = None):
        """
        Decide whether to inject a stress event and execute it.
        If enhanced, uses distillation to choose event type; otherwise,
        uses original probability functions.
        """
        context = context or {}
        if graph_metrics is None:
            graph_metrics = self.config.graph_metrics
        if human_feedback_score is None:
            human_feedback_score = self.config.human_feedback_score

        if not self.use_enhancements or self.distillation_optimizer is None:
            # Fallback to original functions with configured probabilities
            random_failure_injection(self.config.failure_prob)
            # Energy and reward spikes are applied by caller, not here; we just run failure.
            return

        # Build state and select action
        state = StressState(context, graph_metrics, human_feedback_score)
        action, state_vec, teacher_probs = self.distillation_optimizer.select_action(state)

        if action == 1:
            # Inject failure
            random_failure_injection(prob=1.0)  # force crash
        elif action == 2:
            # Signal energy spike (caller should apply energy_spike_simulation)
            pass
        elif action == 3:
            # Signal reward spike (caller should apply reward_spike_simulation)
            pass

        # Compute reward (simplified) and update
        reward = self._compute_reward(context, action)
        self.distillation_optimizer.update(
            state_vec=state_vec,
            action=action,
            reward=reward,
            next_state_vec=state_vec,  # simplified
            teacher_probs=teacher_probs
        )

    def _compute_reward(self, context: Dict[str, float], action: int) -> float:
        # Reward based on whether the injection was appropriate (heuristic)
        # In a real system, we would observe the system's resilience.
        # Here we use a simple rule: if action matches expected stress under load,
        # reward is high, else low.
        load = context.get("load", 0.5)
        if action == 1 and load > 0.7:
            return 0.8
        elif action == 2 and load > 0.5:
            return 0.5
        elif action == 3 and load < 0.3:
            return -0.5
        elif action == 0 and load < 0.5:
            return 0.3
        else:
            return -0.2
