# -*- coding: utf-8 -*-
"""
AgentNode (Enhanced)

Represents an agent node capable of executing tasks via a runner (e.g., FlexGen).
When enhancements are enabled, the node uses:
  - LIMIT Graph metrics in its state
  - MODP (multi‑objective) reward calculation
  - RLHF (human feedback) in decision‑making
  - Multi‑Teacher On‑Policy Distillation with MoE gating
  - Bio‑inspired evolutionary optimisation of execution parameters

If enhancements are disabled (default), the node behaves exactly like the original:
it simply calls `runner.run(task)` and returns the result.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List, Tuple
import random
import numpy as np
from collections import deque


# ------------------------------------------------------------------------------
# Configuration for enhancements
# ------------------------------------------------------------------------------
@dataclass
class AgentNodeConfig:
    use_enhancements: bool = False
    # LIMIT Graph metrics
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5,
        "density": 0.4
    })
    # MODP weights: [quality, energy, latency, carbon]
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
# Decision state and distillation optimizer
# ------------------------------------------------------------------------------
class AgentNodeState:
    """Feature vector representing the task and node context."""
    def __init__(self, task: Any, runner_name: str,
                 graph_metrics: Dict[str, float], human_feedback: float):
        # Extract simple features from the task (assuming dict or similar)
        if isinstance(task, dict):
            self.task_size = len(str(task))
            self.task_complexity = task.get("complexity", 0.5)
            self.task_priority = task.get("priority", 0.5)
        else:
            self.task_size = 1
            self.task_complexity = 0.5
            self.task_priority = 0.5

        self.runner_name_encoded = self._encode_name(runner_name)
        self.centrality = graph_metrics.get("centrality", 0.5)
        self.connectivity = graph_metrics.get("connectivity", 0.5)
        self.human_feedback = human_feedback

    def _encode_name(self, name: str) -> float:
        # Simple hash of runner name to a value 0-1
        return (hash(name) % 1000) / 1000.0

    def to_vector(self) -> np.ndarray:
        return np.array([
            min(self.task_size / 10000.0, 1.0),
            self.task_complexity,
            self.task_priority,
            self.centrality,
            self.connectivity,
            self.human_feedback,
            self.runner_name_encoded,
        ], dtype=np.float32)


class DistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to decide an execution strategy.
    Actions: 0 = use runner directly (original), 1 = use runner with low energy mode,
            2 = use runner with high quality mode.
    """
    def __init__(self, config: AgentNodeConfig):
        self.config = config
        self.feature_dim = 7
        self.n_actions = 3
        self.lr = config.distillation_lr
        self.epsilon = config.epsilon
        self.distill_w = 0.7
        self.rl_w = 0.3
        self.train_every = config.train_every
        self.counter = 0
        self.replay_buffer = deque(maxlen=config.replay_size)

        # Student (linear softmax)
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

    def _rule_teacher(self, state: AgentNodeState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.1
        if state.task_complexity > 0.7:
            probs[2] = 0.6   # high quality
        elif state.centrality > 0.7:
            probs[1] = 0.6   # low energy (green)
        else:
            probs[0] = 0.6   # default
        return probs / probs.sum()

    def _rlhf_teacher(self, state: AgentNodeState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback > 0.7:
            probs[2] += 0.2   # prefer quality
        elif state.human_feedback < 0.3:
            probs[1] += 0.2   # prefer green
        return probs / probs.sum()

    def _historical_teacher(self, state: AgentNodeState) -> np.ndarray:
        # Simulate a learned model based on past performance
        probs = np.ones(self.n_actions) * 0.05
        if state.task_priority > 0.8:
            probs[2] = 0.7
        elif state.task_size > 0.6:
            probs[0] = 0.5
            probs[1] = 0.4
        else:
            probs[0] = 0.7
        return probs / probs.sum()

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_strategy(self, state: AgentNodeState, exploration=True) -> Tuple[int, np.ndarray, np.ndarray]:
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
# Enhanced AgentNode
# ------------------------------------------------------------------------------
class AgentNode:
    """
    An agent node that executes tasks via a runner (e.g., FlexGen).
    Optionally uses advanced decision-making to choose execution strategy.
    """

    def __init__(self, name: str, runner: Any,
                 config: Optional[AgentNodeConfig] = None):
        """
        Args:
            name: Node name.
            runner: Object with a `run(task)` method.
            config: Optional AgentNodeConfig for enhancements.
        """
        self.name = name
        self.runner = runner
        self.config = config or AgentNodeConfig()
        self.use_enhancements = self.config.use_enhancements

        # Enhanced components
        self.distillation_optimizer = None
        self.evolutionary_optimizer = None
        if self.use_enhancements:
            if self.config.modp_weights is None:
                self.config.modp_weights = [0.4, 0.3, 0.2, 0.1]  # quality, energy, latency, carbon
            else:
                total = sum(self.config.modp_weights)
                self.config.modp_weights = [w / total for w in self.config.modp_weights]
            if self.config.use_distillation:
                self.distillation_optimizer = DistillationOptimizer(self.config)
            if self.config.use_evolutionary:
                # Placeholder: evolutionary optimizer for modp_weights could be added
                pass

        # Store last decision for learning
        self._last_decision = None

    def execute(self, task: Any) -> Any:
        """
        Execute a task using the runner, optionally selecting a strategy first.
        """
        if not self.use_enhancements or self.distillation_optimizer is None:
            # Original behaviour
            return self.runner.run(task)

        # Build state from task and context
        state = AgentNodeState(
            task=task,
            runner_name=self.name,
            graph_metrics=self.config.graph_metrics,
            human_feedback=self.config.human_feedback_score
        )

        # Select strategy
        strategy_idx, state_vec, teacher_probs = self.distillation_optimizer.select_strategy(state)
        strategy_map = {
            0: "default",
            1: "energy_saving",
            2: "quality_focus",
        }
        chosen_strategy = strategy_map[strategy_idx]

        # Execute based on strategy (we assume runner.run can accept a strategy hint)
        try:
            result = self.runner.run(task, strategy=chosen_strategy)
        except TypeError:
            # If runner doesn't support strategy, call without it
            result = self.runner.run(task)

        # Compute a simple reward (MODP) based on result or metrics from task
        reward = self._compute_reward(result, task, state)

        # Update distillation optimizer
        self.distillation_optimizer.update(
            state_vec=state_vec,
            action=strategy_idx,
            reward=reward,
            next_state_vec=state_vec,  # simplified
            teacher_probs=teacher_probs
        )

        # Optionally update evolutionary optimizer (if present)
        # ...

        return result

    def _compute_reward(self, result: Any, task: Any, state: AgentNodeState) -> float:
        """
        Compute a multi‑objective reward based on the result and context.
        In a real system, metrics like energy/latency would come from the runner.
        Here we approximate using task metadata and result.
        """
        # Extract metrics if available (result may be dict)
        metrics = {}
        if isinstance(result, dict):
            metrics = result.get("metrics", {})
        accuracy = metrics.get("accuracy", 0.8)
        energy_kwh = metrics.get("energy_kwh", 0.001)
        latency_ms = metrics.get("latency_ms", 100)
        carbon_kg = metrics.get("carbon_kg", energy_kwh * 0.4)

        # Normalize (higher is better for accuracy, lower for others)
        accuracy_norm = min(accuracy, 1.0)
        energy_norm = 1.0 - min(energy_kwh / 0.1, 1.0)
        latency_norm = 1.0 - min(latency_ms / 1000.0, 1.0)
        carbon_norm = 1.0 - min(carbon_kg / 0.1, 1.0)

        weights = self.config.modp_weights
        reward = float(np.dot([accuracy_norm, energy_norm, latency_norm, carbon_norm], weights))
        # Add small bonus for following human feedback
        reward += 0.1 * (state.human_feedback - 0.5)
        return max(-1.0, min(1.0, reward))
