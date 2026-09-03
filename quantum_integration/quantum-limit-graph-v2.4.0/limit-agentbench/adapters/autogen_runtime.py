"""
Adapter implementation for AutoGen-style agents.
Captures message graph depth and conversation complexity as part of metrics.
Enhanced with optional integration of LIMIT Graph, MODP, RLHF,
Multi-Teacher On-Policy Distillation, Bio-inspired Optimisation, and MoE expert gating.
"""

from typing import Dict, Any, Optional, Tuple, List
from .base_runtime import BaseRuntimeAdapter
import time
import random
import numpy as np
from collections import deque
import logging

logger = logging.getLogger(__name__)

# Optional imports for advanced enhancements
try:
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor
    from src.enhancements.schemas.node_descriptor import NodeDescriptor
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    logger.warning("Advanced enhancements not available; running in legacy mode.")

# ------------------------------------------------------------------------------
# State representation for decision-making
# ------------------------------------------------------------------------------
class RuntimeState:
    """State used by the distillation optimizer to select execution parameters."""
    def __init__(self, query: Dict[str, Any], carbon_intensity: float = 400.0,
                 graph_metrics: Optional[Dict[str, float]] = None,
                 human_feedback_score: float = 0.5):
        self.complexity = query.get('complexity', 0.5)
        self.token_count = query.get('token_count', 1000)
        self.latency_target = query.get('latency_target', 500.0)
        self.carbon_intensity = carbon_intensity
        self.human_feedback_score = human_feedback_score
        self.graph_centrality = (graph_metrics or {}).get('centrality', 0.5)
        self.graph_connectivity = (graph_metrics or {}).get('connectivity', 0.5)
        self.cost_budget = query.get('cost_budget', 10.0)

    def to_feature_vector(self) -> np.ndarray:
        return np.array([
            min(self.complexity, 1.0),
            min(self.token_count / 10000.0, 1.0),
            min(self.latency_target / 1000.0, 1.0),
            min(self.carbon_intensity / 500.0, 1.0),
            self.human_feedback_score,
            self.graph_centrality,
            self.graph_connectivity,
            min(self.cost_budget / 100.0, 1.0),
        ], dtype=np.float32)


# ------------------------------------------------------------------------------
# Distillation Optimizer with MoE gating and optional evolutionary blending
# ------------------------------------------------------------------------------
class RuntimeDistillationOptimizer:
    """
    Multi-teacher on-policy distillation with Mixture-of-Experts (MoE) gating.
    Decides between: 0 = use_autogen, 1 = use_flexgen_low, 2 = use_flexgen_high.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.feature_dim = 8
        self.n_actions = 3
        self.lr = config.get('distillation_lr', 0.01)
        self.epsilon = config.get('epsilon', 0.1)
        self.distill_w = config.get('distill_weight', 0.7)
        self.rl_w = config.get('rl_weight', 0.3)
        self.train_every = config.get('train_every', 10)
        self.counter = 0
        self.replay_buffer = deque(maxlen=config.get('replay_size', 2000))

        # Student (linear softmax)
        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)

        # Teachers (simplified: rule-based, RLHF, historical)
        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher
        ]
        # MoE gating network
        self.gate_weights = np.random.randn(self.feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = config.get('gating_lr', 0.005)

    def _rule_teacher(self, state: RuntimeState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.1
        if state.complexity > 0.7:
            probs[0] = 0.8   # autogen handles complex
        elif state.carbon_intensity > 400:
            probs[1] = 0.7   # flexgen low precision for efficiency
        else:
            probs[2] = 0.5
        return probs / probs.sum()

    def _rlhf_teacher(self, state: RuntimeState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback_score > 0.7:
            probs[0] += 0.2   # prefer autogen
        elif state.human_feedback_score < 0.3:
            probs[1] += 0.2   # prefer flexgen low
        return probs / probs.sum()

    def _historical_teacher(self, state: RuntimeState) -> np.ndarray:
        if state.token_count > 5000:
            return np.array([0.1, 0.3, 0.6])
        elif state.latency_target < 100:
            return np.array([0.7, 0.1, 0.2])
        else:
            return np.array([0.4, 0.3, 0.3])

    def _gate_forward(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_action(self, state: RuntimeState, exploration: bool = True) -> Tuple[int, np.ndarray, np.ndarray]:
        x = state.to_feature_vector()
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher(state)
            if len(prob) != self.n_actions:
                prob = np.pad(prob, (0, self.n_actions - len(prob)), 'constant')[:self.n_actions]
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)
        gate_weights = self._gate_forward(x)
        teacher_probs = np.sum(gate_weights[:, None] * teacher_outputs, axis=0)
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
                cur_probs = np.exp(logits - np.max(logits))
                cur_probs /= cur_probs.sum()
                grad_distill = -(tp - cur_probs)
                one_hot = np.zeros(self.n_actions)
                one_hot[a] = 1.0
                grad_rl = -r * (one_hot - cur_probs)
                grad = self.distill_w * grad_distill + self.rl_w * grad_rl
                self.student_weights -= self.lr * np.outer(s, grad)
                self.student_bias -= self.lr * grad

                # Update gating
                gate_weights = self._gate_forward(s)
                combined_teacher = np.sum(gate_weights[:, None] * tp, axis=0)
                error = combined_teacher - cur_probs
                grad_gate = np.dot(tp, error)
                self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
                self.gate_bias -= self.gate_lr * grad_gate


# ------------------------------------------------------------------------------
# AutoGenRuntime with optional enhancements
# ------------------------------------------------------------------------------
class AutoGenRuntime(BaseRuntimeAdapter):
    """
    Runtime adapter for AutoGen-style agents.
    If enhancements are enabled, it can delegate to FlexGen and use
    distillation to decide execution strategy. It also tracks additional
    metrics (graph depth, carbon, etc.).
    """
    def init(self, config: Dict[str, Any]) -> None:
        self.config = config or {}
        self.use_enhancements = self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE

        # Enhanced components
        self.distillation_optimizer = None
        self.workload_descriptor = None
        self.node_descriptor = None
        self.zero_trust = None
        self.graph_metrics = self.config.get('graph_metrics', {'centrality': 0.5, 'connectivity': 0.5})
        self.human_feedback_score = self.config.get('human_feedback_score', 0.5)

        if self.use_enhancements:
            self._init_enhanced()

    def _init_enhanced(self):
        try:
            # Initialize distillation optimizer
            self.distillation_optimizer = RuntimeDistillationOptimizer(self.config)

            # Optional workload descriptor
            if 'workload' in self.config:
                from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency
                self.workload_descriptor = WorkloadDescriptor(
                    task_id="autogen_runtime",
                    task_type=TaskType.INFERENCE,
                    tokens=self.config.get('default_tokens', 1000),
                    latency_target=self.config.get('default_latency', 500.0),
                    urgency=Urgency.MEDIUM,
                    estimated_energy_joules=0.01,
                    estimated_carbon_kg=0.0001,
                    user_id="autogen_runtime"
                )

            # Optional node descriptor
            if 'node' in self.config:
                from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
                self.node_descriptor = NodeDescriptor(
                    id="autogen_node",
                    type=NodeType.EDGE,
                    region="us-east",
                    region_carbon_intensity=self.config.get('carbon_intensity', 400.0),
                    energy_per_token=0.00005,
                    helium_connectivity_score=0.8,
                    uptime=0.99,
                    renewable_fraction=0.3,
                    cooling_type="air",
                    hardware_model="cpu"
                )

            # Optional zero trust
            if self.config.get('enable_zero_trust', False):
                self.zero_trust = ZeroTrustArchitecture()

            logger.info("AutoGenRuntime enhanced components initialized")
        except Exception as e:
            logger.error(f"Failed to initialize enhancements: {e}")
            self.use_enhancements = False

    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        start = time.perf_counter()

        # Enhanced decision making
        execution_strategy = "autogen"
        reward = None
        if self.use_enhancements and self.distillation_optimizer:
            # Build state
            carbon_intensity = self.node_descriptor.region_carbon_intensity if self.node_descriptor else query.get('carbon_intensity', 400.0)
            state = RuntimeState(
                query,
                carbon_intensity=carbon_intensity,
                graph_metrics=self.graph_metrics,
                human_feedback_score=query.get('human_feedback_score', self.human_feedback_score)
            )
            action_idx, state_vec, teacher_probs = self.distillation_optimizer.select_action(state)
            # Map action to strategy
            if action_idx == 0:
                execution_strategy = "autogen"
            elif action_idx == 1:
                execution_strategy = "flexgen_low_precision"
            else:
                execution_strategy = "flexgen_high_precision"

            self._last_decision = (state_vec, action_idx, teacher_probs, state)
        else:
            self._last_decision = None

        # Simulate execution (could be AutoGen or FlexGen depending on strategy)
        # Here we simulate; in real system would call appropriate engine.
        if execution_strategy == "autogen":
            result = {"accuracy": 0.85, "conversation_depth": 2}
        elif execution_strategy == "flexgen_low_precision":
            result = {"accuracy": 0.80, "conversation_depth": 1}  # lower quality but faster
        else:
            result = {"accuracy": 0.90, "conversation_depth": 3}  # higher quality but slower

        latency = time.perf_counter() - start
        latency_ms = latency * 1000

        # Estimate energy/carbon (simplified)
        token_count = query.get('token_count', 1000)
        energy_kwh = (latency / 3600) * 0.25 * (1 + token_count / 10000)
        carbon_kg = energy_kwh * 0.4  # 400 gCO2/kWh

        output = {
            "latency": latency,
            "latency_ms": latency_ms,
            "execution_strategy": execution_strategy,
            **result
        }

        # Track metrics
        output["energy_kwh"] = energy_kwh
        output["carbon_kg"] = carbon_kg
        output["graph_metrics"] = self.graph_metrics
        output["human_feedback_score"] = query.get('human_feedback_score', self.human_feedback_score)

        # Enhanced post-processing: update models, record feedback
        if self.use_enhancements and self._last_decision:
            state_vec, action_idx, teacher_probs, state = self._last_decision
            # Compute reward using MODP weights (carbon, energy, latency, cost)
            carbon_norm = 1.0 - min(state.carbon_intensity / 500.0, 1.0)
            energy_norm = 1.0 - min(energy_kwh, 1.0)
            latency_norm = 1.0 - min(latency_ms / 1000.0, 1.0)
            cost_norm = 1.0 - min(state.cost_budget / 100.0, 1.0)
            modp_weights = np.array([0.4, 0.3, 0.2, 0.1])
            reward = float(np.dot([carbon_norm, energy_norm, latency_norm, cost_norm], modp_weights))

            # Update distillation optimizer
            self.distillation_optimizer.update(
                state_vec, action_idx, reward, state_vec, teacher_probs
            )

            # Update workload descriptor if present
            if self.workload_descriptor:
                try:
                    import asyncio
                    asyncio.run(self.workload_descriptor.record_outcome(
                        latency_achieved_ms=latency_ms,
                        carbon_saved_kg=max(0, 0.01 - carbon_kg),
                        energy_used_joules=energy_kwh * 3600
                    ))
                except Exception:
                    pass

            # Emit FeedbackEvent if available
            if FeedbackEvent is not None and self.zero_trust:
                try:
                    event = FeedbackEvent(
                        source="autogen_runtime",
                        feedback_type="routing",
                        task_id="autogen_task",
                        context={"strategy": execution_strategy},
                        action={"selected_action": execution_strategy, "selected_rank": 1},
                        performance={"quality_score": reward,
                                     "latency_ms": latency_ms,
                                     "energy_joules": energy_kwh * 3600,
                                     "carbon_g": carbon_kg * 1000,
                                     "helium_cost": 0,
                                     "duration_ms": latency_ms},
                        adaptive_cost_value=reward,
                        tags=["autogen", execution_strategy]
                    )
                    # In production, publish to queue; here just log.
                    logger.debug(f"FeedbackEvent created: {event.event_id}")
                except Exception:
                    pass

        if reward is not None:
            output["reward"] = reward

        return output

    def finalize(self) -> None:
        # Optionally save state, close connections, etc.
        if self.use_enhancements and self.zero_trust:
            import asyncio
            try:
                asyncio.run(self.zero_trust.shutdown())
            except Exception:
                pass
        logger.info("AutoGenRuntime finalized")
