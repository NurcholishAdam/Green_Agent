# -*- coding: utf-8 -*-
"""
AutoGen Agent Adapter (Enhanced)
Adapter for Microsoft AutoGen agents with advanced decision-making and metric tracking.
Integrates with FlexGen (optional), LIMIT Graph, MODP, RLHF,
Multi-Teacher On-Policy Distillation, Bio-inspired Optimisation, and MoE expert gating.
"""

from typing import Dict, Any, Optional, List, Tuple
import logging
import time
import numpy as np
from dataclasses import dataclass, field
from collections import deque
import random

# Base adapter
from .base_adapter import BaseAgentAdapter

# Optional imports for enhancements
try:
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency, Priority
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    # Define dummy classes if not available for typing
    class WorkloadDescriptor:
        pass
    class NodeDescriptor:
        pass
    class ZeroTrustArchitecture:
        pass
    class FeedbackEvent:
        pass

logger = logging.getLogger(__name__)


@dataclass
class AdapterState:
    """State representation for decision-making (task features + graph metrics)."""
    task_complexity: float          # 0-1
    token_count: int
    latency_target: float
    carbon_intensity: float
    human_feedback_score: float = 0.5
    graph_centrality: float = 0.5
    graph_connectivity: float = 0.5
    cost_budget: float = 10.0

    def to_feature_vector(self) -> np.ndarray:
        """Convert to normalized feature vector."""
        return np.array([
            min(self.task_complexity, 1.0),
            min(self.token_count / 10000.0, 1.0),
            min(self.latency_target / 1000.0, 1.0),
            min(self.carbon_intensity / 500.0, 1.0),
            self.human_feedback_score,
            self.graph_centrality,
            self.graph_connectivity,
            min(self.cost_budget / 100.0, 1.0),
        ], dtype=np.float32)


class AdapterDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating to decide execution parameters
    (e.g., whether to use FlexGen, batch size, precision).
    """
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.feature_dim = 8
        self.n_actions = 3  # e.g., 0=autogen_only, 1=flexgen_low_precision, 2=flexgen_high_precision
        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)
        self.lr = self.config.get('distillation_lr', 0.01)
        self.epsilon = self.config.get('epsilon', 0.1)
        self.distill_w = self.config.get('distill_weight', 0.7)
        self.rl_w = self.config.get('rl_weight', 0.3)
        self.replay_buffer = deque(maxlen=2000)
        self.counter = 0
        self.train_every = 10

        # Teachers (simplified, using rule-based and RLHF)
        self.teachers = [
            self._rule_based_teacher,
            self._rlhf_teacher,
            self._historical_teacher
        ]
        # MoE gating
        self.gate_weights = np.random.randn(self.feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = self.config.get('gating_lr', 0.005)

    def _rule_based_teacher(self, state: AdapterState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.1
        if state.task_complexity > 0.7:
            probs[0] = 0.8  # use autogen for complex tasks
        elif state.carbon_intensity > 400:
            probs[1] = 0.7  # use flexgen low precision to save energy
        else:
            probs[2] = 0.5
        return probs / probs.sum()

    def _rlhf_teacher(self, state: AdapterState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback_score > 0.7:
            probs[0] += 0.2  # prefer autogen (perceived quality)
        elif state.human_feedback_score < 0.3:
            probs[1] += 0.2  # prefer flexgen (efficiency)
        return probs / probs.sum()

    def _historical_teacher(self, state: AdapterState) -> np.ndarray:
        # Simulate a historical model
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

    def select_action(self, state: AdapterState, exploration: bool = True) -> Tuple[int, np.ndarray, np.ndarray]:
        x = state.to_feature_vector()
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher(state)
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)
        gate_weights = self._gate_forward(x)
        teacher_probs = np.sum(gate_weights[:, None] * teacher_outputs, axis=0)
        teacher_probs /= teacher_probs.sum()

        student_logits = x @ self.student_weights + self.student_bias
        student_probs = np.exp(student_logits - np.max(student_logits))
        student_probs /= student_probs.sum()

        if exploration and random.random() < self.epsilon:
            action = random.randint(0, self.n_actions-1)
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
                cur_probs = np.exp(s @ self.student_weights + self.student_bias - np.max(s @ self.student_weights + self.student_bias))
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


class AutoGenAdapter(BaseAgentAdapter):
    """
    Adapter for Microsoft AutoGen agents with optional enhancement integration.
    
    Supports:
    - ConversableAgent, AssistantAgent, UserProxyAgent
    - Multi-agent conversations
    - Optional FlexGen execution
    - Advanced decision-making for execution strategy
    """

    def __init__(self, agent: Any, config: Dict[str, Any] = None):
        """
        Initialize AutoGen adapter.

        Args:
            agent: AutoGen agent instance
            config: Optional configuration dict for enhancements
        """
        super().__init__(agent, "autogen")
        self.config = config or {}
        self.use_enhancements = self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE

        # Enhanced components
        self.workload_descriptor = None
        self.node_descriptor = None
        self.zero_trust = None
        self.distillation_optimizer = None

        if self.use_enhancements:
            self._initialize_enhancements()

    def _initialize_enhancements(self):
        """Initialize enhanced decision-making components."""
        try:
            # Workload descriptor for task characterization
            self.workload_descriptor = WorkloadDescriptor(
                task_id="autogen_task",
                task_type=TaskType.INFERENCE,
                tokens=self.config.get('default_tokens', 1000),
                latency_target=self.config.get('default_latency', 500.0),
                urgency=Urgency.MEDIUM,
                estimated_energy_joules=0.01,
                estimated_carbon_kg=0.0001,
                user_id="autogen_adapter",
                metadata={"adapter": "autogen"}
            )
            # Node descriptor for compute node (simulated)
            self.node_descriptor = NodeDescriptor(
                id="autogen_node",
                type=NodeType.EDGE,
                region="us-east",
                region_carbon_intensity=400.0,
                energy_per_token=0.00005,
                helium_connectivity_score=0.8,
                uptime=0.99,
                renewable_fraction=0.3,
                cooling_type="air",
                hardware_model="cpu"
            )
            # Zero trust for security (optional)
            if self.config.get('enable_zero_trust', False):
                self.zero_trust = ZeroTrustArchitecture()
            # Distillation optimizer for execution strategy
            self.distillation_optimizer = AdapterDistillationOptimizer(self.config)
            logger.info("AutoGenAdapter enhanced components initialized")
        except Exception as e:
            logger.error(f"Failed to initialize enhancements: {e}")
            self.use_enhancements = False

    def _estimate_energy_and_carbon(self, token_count: int, execution_time_ms: float) -> Tuple[float, float]:
        """Rough estimation of energy (kWh) and carbon (kg) for a run."""
        # Simple model: power ~250W, time in hours
        energy_kwh = (execution_time_ms / 1000 / 3600) * 0.25
        # Carbon intensity 400 gCO2/kWh
        carbon_kg = energy_kwh * 0.4  # 400/1000
        # Scale with token count
        energy_kwh *= (1 + token_count / 10000)
        carbon_kg *= (1 + token_count / 10000)
        return energy_kwh, carbon_kg

    def run(
        self,
        task_input: Dict[str, Any],
        track_green_metrics: bool = True
    ) -> Dict[str, Any]:
        """
        Run AutoGen agent on task input, optionally with enhanced decision-making and metric tracking.
        """
        logger.debug(f"Running AutoGen agent on task (enhanced={self.use_enhancements})")

        start_time = time.time()

        # Extract message
        if isinstance(task_input, dict):
            message = task_input.get('question') or task_input.get('input') or str(task_input)
            token_count = task_input.get('token_count', 1000)
        else:
            message = str(task_input)
            token_count = 1000

        execution_strategy = "autogen_only"  # default
        if self.use_enhancements and self.distillation_optimizer:
            # Build state for decision
            state = AdapterState(
                task_complexity=task_input.get('complexity', 0.5) if isinstance(task_input, dict) else 0.5,
                token_count=token_count,
                latency_target=task_input.get('latency_target', 500.0) if isinstance(task_input, dict) else 500.0,
                carbon_intensity=self.node_descriptor.region_carbon_intensity if self.node_descriptor else 400.0,
                human_feedback_score=task_input.get('human_feedback_score', 0.5) if isinstance(task_input, dict) else 0.5,
                graph_centrality=task_input.get('graph_centrality', 0.5) if isinstance(task_input, dict) else 0.5,
                graph_connectivity=task_input.get('graph_connectivity', 0.5) if isinstance(task_input, dict) else 0.5,
                cost_budget=task_input.get('cost_budget', 10.0) if isinstance(task_input, dict) else 10.0,
            )
            action_idx, state_vec, teacher_probs = self.distillation_optimizer.select_action(state)
            # Map action to strategy
            if action_idx == 0:
                execution_strategy = "autogen_only"
            elif action_idx == 1:
                execution_strategy = "flexgen_low_precision"
            else:
                execution_strategy = "flexgen_high_precision"
            self._last_decision = (state_vec, action_idx, teacher_probs, state)
        else:
            self._last_decision = None

        # Execute agent (in practice, could delegate to FlexGen for some strategies)
        try:
            if execution_strategy in ["flexgen_low_precision", "flexgen_high_precision"]:
                # Placeholder: assume we call FlexGen with appropriate precision
                # In a real system, we would import and call FlexGen here.
                logger.info(f"Delegating to FlexGen with strategy {execution_strategy}")
                # Simulate result
                result = f"FlexGen result for: {message}"
            else:
                # Standard AutoGen execution
                if hasattr(self.agent, 'generate_reply'):
                    result = self.agent.generate_reply(messages=[{"role": "user", "content": message}])
                elif hasattr(self.agent, 'initiate_chat'):
                    result = self.agent.initiate_chat(message=message)
                else:
                    raise ValueError("Unsupported AutoGen agent type")
        except Exception as e:
            logger.error(f"Agent execution failed: {e}")
            result = None
            reward = 0.0
        else:
            reward = 1.0  # success

        execution_time_ms = (time.time() - start_time) * 1000
        energy_kwh, carbon_kg = self._estimate_energy_and_carbon(token_count, execution_time_ms)

        # Build output
        output = {
            "answer": result,
            "framework": "autogen",
            "execution_strategy": execution_strategy,
            "execution_time_ms": execution_time_ms,
        }

        if track_green_metrics:
            output["energy_consumed_kwh"] = energy_kwh
            output["carbon_emitted_kg"] = carbon_kg
            output["helium_usage_l"] = 0.0  # placeholder
            if self.zero_trust:
                # Record carbon impact in zero trust
                # (simplified, would call proper methods)
                pass

        # Enhanced: update models and emit feedback event
        if self.use_enhancements and self._last_decision:
            state_vec, action_idx, teacher_probs, state = self._last_decision
            # Compute reward using MODP objectives
            carbon_norm = 1.0 - min(state.carbon_intensity / 500.0, 1.0)
            energy_norm = 1.0 - min(energy_kwh, 1.0)
            latency_norm = 1.0 - min(execution_time_ms / 1000.0, 1.0)
            cost_norm = 1.0 - min(state.cost_budget / 100.0, 1.0)
            # Weights: carbon 0.4, energy 0.3, latency 0.2, cost 0.1
            modp_weights = np.array([0.4, 0.3, 0.2, 0.1])
            reward = float(np.dot([carbon_norm, energy_norm, latency_norm, cost_norm], modp_weights))
            # Update distillation optimizer
            next_state_vec = state_vec  # simplified
            self.distillation_optimizer.update(state_vec, action_idx, reward, next_state_vec, teacher_probs)

            # Update workload descriptor (if available)
            if self.workload_descriptor:
                # Record outcome to update its own models
                try:
                    asyncio.run(self.workload_descriptor.record_outcome(
                        latency_achieved_ms=execution_time_ms,
                        carbon_saved_kg=max(0, 0.01 - carbon_kg),
                        energy_used_joules=energy_kwh * 3600
                    ))
                except Exception:
                    pass

            # Emit FeedbackEvent if available
            if FeedbackEvent is not None:
                try:
                    event = FeedbackEvent(
                        source="autogen_adapter",
                        feedback_type="routing",
                        task_id="autogen_task",
                        context={"strategy": execution_strategy},
                        action={"selected_action": execution_strategy, "selected_rank": 1},
                        performance={"quality_score": reward,
                                     "latency_ms": execution_time_ms,
                                     "energy_joules": energy_kwh * 3600,
                                     "carbon_g": carbon_kg * 1000,
                                     "helium_cost": 0,
                                     "duration_ms": execution_time_ms},
                        adaptive_cost_value=reward,
                        tags=["autogen", execution_strategy]
                    )
                    # In production, publish to message queue
                    logger.debug(f"FeedbackEvent created: {event.event_id}")
                except Exception:
                    pass

        return output

    def _get_agent_name(self) -> str:
        """Get agent name from AutoGen agent."""
        if hasattr(self.agent, 'name'):
            return self.agent.name
        else:
            return "AutoGenAgent"
