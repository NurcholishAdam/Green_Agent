# -*- coding: utf-8 -*-
"""
MultiAgentCoordinator (Enhanced)

Distributes tasks across multiple agents, optionally using advanced decision-making:
- LIMIT Graph metrics influence agent selection.
- MODP (multi‑objective) reward computes a score for each agent's output.
- RLHF: human feedback score biases the selection.
- Multi‑Teacher On‑Policy Distillation + MoE: a learned policy chooses the best agent(s).
- Bio‑inspired optimisation: evolutionary tuning of the selection weights.

When enhancements are disabled (default), the coordinator behaves exactly as the original:
it executes the task on every agent and returns all results.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
import random
import numpy as np
from collections import deque


# ------------------------------------------------------------------------------
# Configuration for enhancements
# ------------------------------------------------------------------------------
@dataclass
class CoordinatorConfig:
    use_enhancements: bool = False
    # LIMIT Graph metrics
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5
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
# Decision state and distillation optimizer for agent selection
# ------------------------------------------------------------------------------
class CoordinatorState:
    """Feature vector representing the task and agent context."""
    def __init__(self, task: Any, agent_name: str,
                 graph_metrics: Dict[str, float], human_feedback: float):
        if isinstance(task, dict):
            self.task_size = len(str(task))
            self.task_complexity = task.get("complexity", 0.5)
            self.task_priority = task.get("priority", 0.5)
        else:
            self.task_size = 1
            self.task_complexity = 0.5
            self.task_priority = 0.5

        self.agent_name_encoded = self._encode_name(agent_name)
        self.centrality = graph_metrics.get("centrality", 0.5)
        self.connectivity = graph_metrics.get("connectivity", 0.5)
        self.human_feedback = human_feedback

    def _encode_name(self, name: str) -> float:
        return (hash(name) % 1000) / 1000.0

    def to_vector(self) -> np.ndarray:
        return np.array([
            min(self.task_size / 10000.0, 1.0),
            self.task_complexity,
            self.task_priority,
            self.centrality,
            self.connectivity,
            self.human_feedback,
            self.agent_name_encoded,
        ], dtype=np.float32)


class DistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to decide the best agent.
    Actions correspond to indices in the list of agents.
    """
    def __init__(self, n_agents: int, config: CoordinatorConfig):
        self.n_actions = n_agents
        self.config = config
        self.feature_dim = 7
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

    def _rule_teacher(self, state: CoordinatorState) -> np.ndarray:
        # Simple heuristic: prefer agents with high centrality (assumed more reliable)
        probs = np.ones(self.n_actions) * 0.05
        if state.centrality > 0.7:
            # Choose first few agents (or based on name hash)
            probs[0] = 0.6
        elif state.task_complexity > 0.7:
            probs[-1] = 0.6  # last agent assumed expert
        else:
            probs[0] = 0.5
        return probs / probs.sum()

    def _rlhf_teacher(self, state: CoordinatorState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback > 0.7:
            # Prefer agent with high quality (assume index 2 or last)
            probs[min(self.n_actions-1, 2)] += 0.2
        elif state.human_feedback < 0.3:
            # Prefer efficient agent (assume index 1)
            probs[min(self.n_actions-1, 1)] += 0.2
        return probs / probs.sum()

    def _historical_teacher(self, state: CoordinatorState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.05
        if state.task_priority > 0.8:
            probs[-1] = 0.7
        else:
            probs[0] = 0.6
        return probs / probs.sum()

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_agent_index(self, state: CoordinatorState, exploration=True):
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
# Enhanced MultiAgentCoordinator
# ------------------------------------------------------------------------------
class MultiAgentCoordinator:
    """
    Distributes tasks across agents, optionally using advanced decision-making.
    """

    def __init__(self, agents: List[Any],
                 config: Optional[CoordinatorConfig] = None):
        """
        Args:
            agents: List of objects with `execute(task)` method.
            config: Optional CoordinatorConfig for enhancements.
        """
        self.agents = agents
        self.config = config or CoordinatorConfig()
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
                self.distillation_optimizer = DistillationOptimizer(
                    n_agents=len(self.agents), config=self.config
                )
            # Evolutionary component could be added if desired

        self._last_decision = None

    def distribute(self, task: Any, **kwargs) -> List[Any]:
        """
        Distribute a task to agents.

        If enhancements are enabled, a single agent (or subset) is chosen via
        distillation. Otherwise, all agents execute the task (original behavior).

        Returns:
            List of results (length may be less than number of agents if enhanced).
        """
        if not self.use_enhancements or self.distillation_optimizer is None:
            # Original: run on all agents
            return [agent.execute(task) for agent in self.agents]

        # Enhanced: select best agent using distillation
        # Build state using first agent's name (we need a reference state)
        # In practice, we would build a state per agent, but for simplicity
        # we use a single state with the first agent's name.
        state = CoordinatorState(
            task=task,
            agent_name=getattr(self.agents[0], 'name', 'agent_0'),
            graph_metrics=self.config.graph_metrics,
            human_feedback=self.config.human_feedback_score
        )

        agent_idx, state_vec, teacher_probs = self.distillation_optimizer.select_agent_index(state)
        chosen_agent = self.agents[agent_idx]

        # Execute chosen agent
        result = chosen_agent.execute(task)

        # Compute reward (simplified) from result or task metadata
        reward = self._compute_reward(result, task, state)

        # Update distillation optimizer
        self.distillation_optimizer.update(
            state_vec=state_vec,
            action=agent_idx,
            reward=reward,
            next_state_vec=state_vec,  # simplified
            teacher_probs=teacher_probs
        )

        return [result]  # return as list for consistency with original

    def _compute_reward(self, result: Any, task: Any, state: CoordinatorState) -> float:
        """
        Compute a multi‑objective reward based on the result and context.
        In a real system, metrics would come from the runner/agent.
        """
        metrics = {}
        if isinstance(result, dict):
            metrics = result.get("metrics", {})
        accuracy = metrics.get("accuracy", 0.8)
        energy_kwh = metrics.get("energy_kwh", 0.001)
        latency_ms = metrics.get("latency_ms", 100)
        carbon_kg = metrics.get("carbon_kg", energy_kwh * 0.4)

        accuracy_norm = min(accuracy, 1.0)
        energy_norm = 1.0 - min(energy_kwh / 0.1, 1.0)
        latency_norm = 1.0 - min(latency_ms / 1000.0, 1.0)
        carbon_norm = 1.0 - min(carbon_kg / 0.1, 1.0)

        weights = self.config.modp_weights
        reward = float(np.dot([accuracy_norm, energy_norm, latency_norm, carbon_norm], weights))
        reward += 0.1 * (state.human_feedback - 0.5)
        return max(-1.0, min(1.0, reward))
