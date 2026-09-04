import torch
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple
from collections import deque
import random


# ------------------------------------------------------------------------------
# Enhanced configuration
# ------------------------------------------------------------------------------
@dataclass
class HealthMonitorConfig:
    use_enhancements: bool = False
    # LIMIT Graph metrics
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5
    })
    # MODP weights: [loss, energy, reward]
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


# ------------------------------------------------------------------------------
# Health decision state and distillation optimizer
# ------------------------------------------------------------------------------
class HealthState:
    def __init__(self, loss: float, energy: float, reward: float,
                 graph_metrics: Dict[str, float], human_feedback: float):
        self.loss_norm = min(abs(loss) / 1e6, 1.0)       # normalized
        self.energy_norm = min(energy / 100.0, 1.0)       # assume max 100
        self.reward_norm = min(abs(reward) / 100.0, 1.0)
        self.centrality = graph_metrics.get("centrality", 0.5)
        self.connectivity = graph_metrics.get("connectivity", 0.5)
        self.human_feedback = human_feedback

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.loss_norm,
            self.energy_norm,
            self.reward_norm,
            self.centrality,
            self.connectivity,
            self.human_feedback,
        ], dtype=np.float32)


class HealthDistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to produce a health score.
    Actions: 0 = healthy, 1 = warning, 2 = critical.
    """
    def __init__(self, config: HealthMonitorConfig):
        self.config = config
        self.feature_dim = 6
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

    def _rule_teacher(self, state: HealthState) -> np.ndarray:
        # Simple thresholds (similar to original)
        if state.loss_norm > 0.8 or state.reward_norm > 0.8:
            return np.array([0.1, 0.2, 0.7])  # critical
        elif state.energy_norm > 0.6 or state.loss_norm > 0.4:
            return np.array([0.2, 0.7, 0.1])  # warning
        else:
            return np.array([0.8, 0.15, 0.05])  # healthy

    def _rlhf_teacher(self, state: HealthState) -> np.ndarray:
        # Human feedback: high -> more tolerant, low -> stricter
        if state.human_feedback > 0.7:
            return np.array([0.7, 0.2, 0.1])
        elif state.human_feedback < 0.3:
            return np.array([0.3, 0.4, 0.3])
        else:
            return np.array([0.5, 0.3, 0.2])

    def _historical_teacher(self, state: HealthState) -> np.ndarray:
        # Simulate learned pattern: centrality influences tolerance
        if state.centrality > 0.7:
            return np.array([0.6, 0.3, 0.1])
        else:
            return np.array([0.4, 0.4, 0.2])

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_health_status(self, state: HealthState, exploration=True):
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
# Enhanced HealthMonitor
# ------------------------------------------------------------------------------
class HealthMonitor:
    """
    Monitors training health with optional advanced decision-making.
    """

    def __init__(self, config: Optional[HealthMonitorConfig] = None):
        self.config = config or HealthMonitorConfig()
        self.use_enhancements = self.config.use_enhancements

        # Enhanced components
        self.distillation_optimizer = None
        if self.use_enhancements:
            if self.config.modp_weights is None:
                self.config.modp_weights = [0.4, 0.3, 0.3]  # loss, energy, reward
            else:
                total = sum(self.config.modp_weights)
                self.config.modp_weights = [w / total for w in self.config.modp_weights]
            if self.config.use_distillation:
                self.distillation_optimizer = HealthDistillationOptimizer(self.config)

    # ------------------------------------------------------------------
    # Original methods (unchanged)
    # ------------------------------------------------------------------
    def check_loss(self, loss):
        if torch.isnan(loss):
            raise RuntimeError("NaN detected in PPO loss")
        if loss.abs() > 1e6:
            raise RuntimeError("Loss explosion detected")

    def check_energy(self, energy, budget):
        if energy > budget * 2:
            print("Warning: extreme energy spike")

    def check_reward(self, reward):
        if abs(reward) > 100:
            print("Warning: abnormal reward magnitude")

    # ------------------------------------------------------------------
    # Enhanced methods (use distillation for holistic health check)
    # ------------------------------------------------------------------
    def comprehensive_check(
        self,
        loss: torch.Tensor,
        energy: float,
        reward: float,
        budget: float,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ):
        """
        Performs all checks and, if enhancements enabled, uses a learned
        policy to determine overall health status (healthy/warning/critical).
        Returns a dict with status and details.
        """
        # Run original checks first (they may raise or print)
        self.check_loss(loss)
        self.check_energy(energy, budget)
        self.check_reward(reward)

        if not self.use_enhancements:
            return {"status": "ok"}

        # Use provided or default metrics/feedback
        if graph_metrics is None:
            graph_metrics = self.config.graph_metrics
        if human_feedback_score is None:
            human_feedback_score = self.config.human_feedback_score

        # Build state from current values
        state = HealthState(
            loss=loss.item() if torch.is_tensor(loss) else float(loss),
            energy=energy,
            reward=reward,
            graph_metrics=graph_metrics,
            human_feedback=human_feedback_score
        )

        # Select status via distillation
        if self.distillation_optimizer:
            action, state_vec, teacher_probs = self.distillation_optimizer.select_health_status(state)
            status_map = {0: "healthy", 1: "warning", 2: "critical"}
            status = status_map[action]

            # Compute reward for updating (use MODP weights)
            # Higher reward means the chosen status was appropriate.
            # In practice, we would use ground truth later. For now, use heuristic:
            # If status is critical and loss/reward extreme, reward high; etc.
            loss_severity = state.loss_norm
            energy_severity = state.energy_norm
            reward_severity = state.reward_norm
            components = np.array([loss_severity, energy_severity, reward_severity])
            # For "healthy" status, we want low severity; for critical, high severity.
            # Reward = match between action and severity.
            if action == 0:  # healthy
                target = 1 - np.mean(components)  # prefer low severity
            elif action == 1:  # warning
                target = np.mean(components) * 0.5 + 0.3
            else:  # critical
                target = np.mean(components)
            reward = float(target)

            self.distillation_optimizer.update(
                state_vec=state_vec,
                action=action,
                reward=reward,
                next_state_vec=state_vec,  # simplified
                teacher_probs=teacher_probs
            )

            return {
                "status": status,
                "distillation_stats": {
                    "student_counter": self.distillation_optimizer.counter,
                    "buffer_size": len(self.distillation_optimizer.replay_buffer)
                }
            }
        else:
            # Fallback to rule-based status
            if loss.abs() > 1e6 or abs(reward) > 100:
                return {"status": "critical"}
            elif energy > budget * 2:
                return {"status": "warning"}
            else:
                return {"status": "healthy"}
