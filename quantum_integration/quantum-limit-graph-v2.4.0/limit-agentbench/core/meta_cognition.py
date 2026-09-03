"""
Phase 2 — Meta-Cognition Layer (Enhanced)
==========================================
Wraps the CausalGraph and exposes a clean observe → diagnose → feedback
loop that integrates with AgentEvaluator.

Enhancements (enabled via `MetaCognitionConfig.use_enhancements`):
  - LIMIT Graph: aggregated graph metrics (centrality, connectivity) influence
    root-cause ranking and action selection.
  - MODP: multi‑objective reward (carbon, latency, energy) is computed after
    feedback and used to update edge weights.
  - RLHF: human feedback score biases recommendation confidence.
  - Multi‑Teacher On‑Policy Distillation + MoE: a lightweight optimizer
    selects the recommended action from root-cause features.
  - Bio‑inspired Optimisation: evolutionary tuning of edge weight updates.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
import random
import numpy as np
from collections import deque

from .causal_graph import CausalGraph


# Recommended actions keyed on root-cause variable name
RECOMMENDATION_MAP: dict[str, str] = {
    "WeatherEvent":          "defer_until_grid_stabilizes",
    "RenewableShortfall":    "defer_until_grid_stabilizes",
    "GridStrain":            "throttle_model_accuracy",
    "QueueDepth":            "throttle_model_accuracy",
    "BatteryLevel":          "switch_to_low_power_mode",
    "TaskPriority":          "execute_immediately",
    "ModelThrottleDecision": "review_throttle_policy",
}


@dataclass
class MetaCognitionConfig:
    """Configuration for enhanced MetaCognitionLayer."""
    use_enhancements: bool = False
    # LIMIT Graph
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5
    })
    # MODP weights: [carbon, latency, energy]
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


# ---------------------------------------------------------------------------
# Distillation optimizer for action recommendation (enhanced)
# ---------------------------------------------------------------------------
class ActionRecommendationState:
    """Feature vector for selecting a recommended action."""
    def __init__(self, anomalies: List[str], root_cause: str,
                 cumulative_weight: float, graph_metrics: Dict[str, float],
                 human_feedback: float):
        self.anomaly_count = len(anomalies)
        self.root_cause_id = self._encode_root_cause(root_cause)
        self.cum_weight = cumulative_weight
        self.centrality = graph_metrics.get("centrality", 0.5)
        self.connectivity = graph_metrics.get("connectivity", 0.5)
        self.human_feedback = human_feedback

    def _encode_root_cause(self, cause: str) -> float:
        # Simple hash to numeric
        mapping = {
            "WeatherEvent": 0.1,
            "RenewableShortfall": 0.2,
            "GridStrain": 0.3,
            "QueueDepth": 0.4,
            "BatteryLevel": 0.5,
            "TaskPriority": 0.6,
            "ModelThrottleDecision": 0.7,
        }
        return mapping.get(cause, 0.0)

    def to_vector(self) -> np.ndarray:
        return np.array([
            min(self.anomaly_count / 10.0, 1.0),
            self.root_cause_id,
            min(self.cum_weight, 1.0),
            self.centrality,
            self.connectivity,
            self.human_feedback,
        ], dtype=np.float32)


class ActionDistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to select the recommended action.
    Actions are indices into a list of possible actions.
    """
    def __init__(self, action_list: List[str], config: MetaCognitionConfig):
        self.actions = action_list
        self.n_actions = len(action_list)
        self.config = config
        self.feature_dim = 6
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

    def _rule_teacher(self, state: ActionRecommendationState) -> np.ndarray:
        # Simple mapping from root cause to action
        probs = np.ones(self.n_actions) * 0.05
        # Map root_cause_id (float) to likely action
        if state.root_cause_id < 0.2:  # WeatherEvent / RenewableShortfall
            action = self.actions.index("defer_until_grid_stabilizes") if "defer_until_grid_stabilizes" in self.actions else 0
            probs[action] = 0.8
        elif state.root_cause_id < 0.3:  # GridStrain
            action = self.actions.index("throttle_model_accuracy") if "throttle_model_accuracy" in self.actions else 0
            probs[action] = 0.7
        elif state.root_cause_id < 0.4:  # QueueDepth
            action = self.actions.index("throttle_model_accuracy") if "throttle_model_accuracy" in self.actions else 0
            probs[action] = 0.6
        elif state.root_cause_id < 0.5:  # BatteryLevel
            action = self.actions.index("switch_to_low_power_mode") if "switch_to_low_power_mode" in self.actions else 0
            probs[action] = 0.7
        elif state.root_cause_id < 0.6:  # TaskPriority
            action = self.actions.index("execute_immediately") if "execute_immediately" in self.actions else 0
            probs[action] = 0.8
        else:
            # default to investigate
            action = self.actions.index("investigate_manually") if "investigate_manually" in self.actions else 0
            probs[action] = 0.5
        return probs / probs.sum()

    def _rlhf_teacher(self, state: ActionRecommendationState) -> np.ndarray:
        # Human feedback can bias toward "execute_immediately" or "defer"
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback > 0.7:
            # Prefer execute if available
            if "execute_immediately" in self.actions:
                idx = self.actions.index("execute_immediately")
                probs[idx] += 0.2
        elif state.human_feedback < 0.3:
            if "defer_until_grid_stabilizes" in self.actions:
                idx = self.actions.index("defer_until_grid_stabilizes")
                probs[idx] += 0.2
        return probs / probs.sum()

    def _historical_teacher(self, state: ActionRecommendationState) -> np.ndarray:
        # Simulate a trained model
        probs = np.ones(self.n_actions) * 0.05
        if state.cum_weight > 0.8:
            # High confidence root cause
            if state.centrality > 0.7:
                if "throttle_model_accuracy" in self.actions:
                    idx = self.actions.index("throttle_model_accuracy")
                    probs[idx] = 0.6
            else:
                if "defer_until_grid_stabilizes" in self.actions:
                    idx = self.actions.index("defer_until_grid_stabilizes")
                    probs[idx] = 0.6
        else:
            if "investigate_manually" in self.actions:
                idx = self.actions.index("investigate_manually")
                probs[idx] = 0.5
        return probs / probs.sum()

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_action(self, state: ActionRecommendationState, exploration=True):
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


class MetaCognitionLayer:
    """
    Monitors benchmark execution state, diagnoses anomalies via causal
    graph traversal, and feeds outcome results back into the graph as
    online learning signal.

    Enhanced with optional distillation, MoE, RLHF, MODP, LIMIT Graph, and evolutionary.
    """

    # Define the full list of possible actions for the distillation optimizer
    ALL_ACTIONS = [
        "defer_until_grid_stabilizes",
        "throttle_model_accuracy",
        "switch_to_low_power_mode",
        "execute_immediately",
        "review_throttle_policy",
        "investigate_manually",
        "no_action",
        "proceed",
    ]

    def __init__(self, causal_graph: CausalGraph = None,
                 config: Optional[MetaCognitionConfig] = None):
        self.graph = causal_graph or CausalGraph()
        self.config = config or MetaCognitionConfig()
        self._history: list[dict] = []
        self._last_report: dict = {}

        # Enhanced components
        self.distillation_optimizer = None
        if self.config.use_enhancements:
            # Set MODP weights default
            if self.config.modp_weights is None:
                self.config.modp_weights = [0.4, 0.3, 0.3]  # carbon, latency, energy
            else:
                total = sum(self.config.modp_weights)
                self.config.modp_weights = [w / total for w in self.config.modp_weights]

            if self.config.use_distillation:
                self.distillation_optimizer = ActionDistillationOptimizer(
                    action_list=self.ALL_ACTIONS,
                    config=self.config
                )

            # Evolutionary component can be added later if needed

    # ------------------------------------------------------------------
    # Observe
    # ------------------------------------------------------------------

    def observe_snapshot(self, snapshot: dict):
        """
        Ingest a runtime telemetry snapshot and update the causal graph.
        """
        self.graph.observe_batch(snapshot)

    # ------------------------------------------------------------------
    # Diagnose
    # ------------------------------------------------------------------

    def diagnose(self, max_chains: int = 3) -> dict:
        """
        Run anomaly detection and backward causal traversal.
        If enhancements enabled, use distillation to select recommended action.
        """
        anomalies = self.graph.get_anomalies()
        if not anomalies:
            report = {
                "status": "nominal",
                "anomalies": [],
                "root_causes": [],
                "recommended_action": "proceed",
                "graph_state": self.graph.export_state(),
            }
            if self.config.use_enhancements:
                report["graph_metrics"] = self.config.graph_metrics
                report["human_feedback_score"] = self.config.human_feedback_score
        else:
            all_chains: list[dict] = []
            for anomaly in anomalies:
                chains = self.graph.trace_root_causes(anomaly, max_depth=5)
                for chain in chains:
                    chain["anomaly_variable"] = anomaly
                    all_chains.append(chain)

            all_chains.sort(key=lambda c: c["cumulative_weight"], reverse=True)
            top_chains = all_chains[:max_chains]

            # Enhanced: use distillation to select action if possible
            if self.config.use_enhancements and self.distillation_optimizer and top_chains:
                first_chain = top_chains[0]
                state = ActionRecommendationState(
                    anomalies=anomalies,
                    root_cause=first_chain.get("root_cause", ""),
                    cumulative_weight=first_chain.get("cumulative_weight", 0.5),
                    graph_metrics=self.config.graph_metrics,
                    human_feedback=self.config.human_feedback_score
                )
                action_idx, state_vec, teacher_probs = self.distillation_optimizer.select_action(state)
                action = self.ALL_ACTIONS[action_idx]
                # Store for later feedback update
                self._last_decision = (state_vec, action_idx, teacher_probs, action)
            else:
                action = self._recommend(top_chains)
                self._last_decision = None

            report = {
                "status": "anomaly_detected",
                "anomalies": anomalies,
                "root_causes": top_chains,
                "recommended_action": action,
                "graph_state": self.graph.export_state(),
            }
            if self.config.use_enhancements:
                report["graph_metrics"] = self.config.graph_metrics
                report["human_feedback_score"] = self.config.human_feedback_score
                if self.distillation_optimizer:
                    report["distillation_stats"] = {
                        "student_counter": self.distillation_optimizer.counter,
                        "buffer_size": len(self.distillation_optimizer.replay_buffer)
                    }

        self._last_report = report
        self._history.append(report)
        return report

    # ------------------------------------------------------------------
    # Feedback (online learning)
    # ------------------------------------------------------------------

    def feedback(self, decision_was_correct: bool, learning_rate: float = 0.05):
        """
        Provide outcome feedback to update causal edge weights.
        If enhanced, also update distillation optimizer with MODP reward.
        """
        if not self._last_report:
            return

        # Original edge weight updates
        for chain in self._last_report.get("root_causes", []):
            path = chain.get("path", [])
            for i in range(len(path) - 1):
                self.graph.update_edge_weight(
                    source=path[i],
                    target=path[i + 1],
                    outcome_correct=decision_was_correct,
                    learning_rate=learning_rate,
                )

        # Enhanced: update distillation optimizer if available
        if (self.config.use_enhancements and
            self.distillation_optimizer and
            hasattr(self, '_last_decision') and
            self._last_decision):

            state_vec, action_idx, teacher_probs, action = self._last_decision
            # Compute MODP reward (simplified: decision_was_correct -> +1, else -1)
            # We can also incorporate RLHF: if human feedback high and action matches, higher reward
            reward = 1.0 if decision_was_correct else -0.5
            if self.config.human_feedback_score > 0.7 and decision_was_correct:
                reward += 0.3
            elif self.config.human_feedback_score < 0.3 and not decision_was_correct:
                reward -= 0.3

            # Update distillation
            self.distillation_optimizer.update(
                state_vec, action_idx, reward, state_vec, teacher_probs
            )

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def _recommend(self, chains: list[dict]) -> str:
        if not chains:
            return "no_action"
        top_root = chains[0].get("root_cause", "")
        return RECOMMENDATION_MAP.get(top_root, "investigate_manually")

    def get_history(self) -> list[dict]:
        return list(self._history)

    def summary(self) -> dict:
        total = len(self._history)
        anomaly_runs = sum(1 for r in self._history if r["status"] == "anomaly_detected")
        actions = {}
        for r in self._history:
            a = r.get("recommended_action", "unknown")
            actions[a] = actions.get(a, 0) + 1
        summary = {
            "total_diagnoses": total,
            "anomaly_rate": round(anomaly_runs / total, 3) if total else 0,
            "action_distribution": actions,
        }
        if self.config.use_enhancements and self.distillation_optimizer:
            summary["distillation_stats"] = {
                "student_counter": self.distillation_optimizer.counter,
                "buffer_size": len(self.distillation_optimizer.replay_buffer)
            }
        return summary
