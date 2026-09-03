# -*- coding: utf-8 -*-
"""
Base Agent Adapter (Enhanced)
Abstract base class for framework-specific adapters with optional integration
of LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy Distillation,
Bio‑inspired Optimisation, and MoE expert gating.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Tuple, List
import logging
import time
import random
import numpy as np
from collections import deque

# Optional imports for enhancements
try:
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    # Dummy classes to avoid NameError if not used
    class WorkloadDescriptor: pass
    class NodeDescriptor: pass
    class ZeroTrustArchitecture: pass
    class FeedbackEvent: pass

logger = logging.getLogger(__name__)


class BaseAgentAdapter(ABC):
    """
    Abstract base class for agent adapters.

    Provides a unified interface for agents from different frameworks.
    If ``use_enhancements`` is True and the enhanced modules are available,
    the adapter will optionally apply decision‑making (via distillation with MoE)
    to choose an execution strategy (e.g., delegate to FlexGen or not),
    compute multi‑objective rewards (MODP), incorporate RLHF feedback,
    and emit ``FeedbackEvent`` for cross‑module learning.
    """

    def __init__(self, agent: Any, framework_name: str,
                 config: Optional[Dict[str, Any]] = None):
        """
        Initialize adapter.

        Args:
            agent: Agent instance from specific framework
            framework_name: Name of the framework
            config: Optional configuration dictionary for enhancements.
        """
        self.agent = agent
        self.framework_name = framework_name
        self.config = config or {}
        self.agent_name = self._get_agent_name()

        # Enhancement flags
        self.use_enhancements = (
            self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE
        )
        self.track_green_metrics = self.config.get('track_green_metrics', True)

        # Enhanced components (lazy initialized)
        self.workload_descriptor = None
        self.node_descriptor = None
        self.zero_trust = None
        self.distillation_optimizer = None

        if self.use_enhancements:
            self._initialize_enhancements()

        logger.info(
            f"Initialized {framework_name} adapter for agent {self.agent_name} "
            f"(enhancements={'on' if self.use_enhancements else 'off'})"
        )

    def _initialize_enhancements(self):
        """Initialize optional enhanced components."""
        try:
            # Distillation optimizer for execution strategy
            self.distillation_optimizer = self._create_distillation_optimizer()

            # Workload descriptor (optional, based on config)
            if self.config.get('use_workload_descriptor', False) and WorkloadDescriptor is not None:
                self.workload_descriptor = WorkloadDescriptor(
                    task_id=self.config.get('task_id', 'default_task'),
                    task_type=TaskType.INFERENCE,
                    tokens=self.config.get('default_tokens', 1000),
                    latency_target=self.config.get('default_latency', 500.0),
                    urgency=Urgency.MEDIUM,
                    estimated_energy_joules=0.01,
                    estimated_carbon_kg=0.0001,
                    user_id=self.agent_name,
                    metadata={"adapter": self.framework_name}
                )

            # Node descriptor (optional)
            if self.config.get('use_node_descriptor', False) and NodeDescriptor is not None:
                self.node_descriptor = NodeDescriptor(
                    id=f"{self.agent_name}_node",
                    type=NodeType.EDGE,
                    region=self.config.get('region', 'us-east'),
                    region_carbon_intensity=self.config.get('carbon_intensity', 400.0),
                    energy_per_token=0.00005,
                    helium_connectivity_score=0.8,
                    uptime=0.99,
                    renewable_fraction=0.3,
                    cooling_type="air",
                    hardware_model="cpu"
                )

            # Zero trust (optional)
            if self.config.get('enable_zero_trust', False):
                self.zero_trust = ZeroTrustArchitecture()

            logger.info("Enhanced components initialized")
        except Exception as e:
            logger.error(f"Failed to initialize enhancements: {e}")
            self.use_enhancements = False
            self.distillation_optimizer = None

    # ------------------------------------------------------------------
    # Distillation optimizer (simplified MoE gating)
    # ------------------------------------------------------------------
    def _create_distillation_optimizer(self):
        """
        Create a distillation optimizer with a linear student, MoE gating,
        and three teachers (rule-based, RLHF, historical).
        Decides among three actions: 0 = use native framework,
        1 = use FlexGen low precision, 2 = use FlexGen high precision.
        """
        class DistillationOptimizer:
            def __init__(self, config):
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

                # MoE gating network
                self.gate_weights = np.random.randn(self.feature_dim, 3) * 0.01
                self.gate_bias = np.zeros(3)
                self.gate_lr = config.get('gating_lr', 0.005)

            def _rule_teacher(self, state_vec):
                # Simplified: uses carbon, complexity, token count
                probs = np.ones(self.n_actions) * 0.1
                # state_vec indices: 0 complexity, 1 token_count, 2 latency_target,
                # 3 carbon_intensity, 4 human_feedback, 5 graph_centrality,
                # 6 graph_connectivity, 7 cost_budget
                if state_vec[0] > 0.7:
                    probs[0] = 0.8
                elif state_vec[3] > 0.4:
                    probs[1] = 0.7
                else:
                    probs[2] = 0.5
                return probs / probs.sum()

            def _rlhf_teacher(self, state_vec):
                probs = np.ones(self.n_actions) / self.n_actions
                if state_vec[4] > 0.7:
                    probs[0] += 0.2
                elif state_vec[4] < 0.3:
                    probs[1] += 0.2
                return probs / probs.sum()

            def _historical_teacher(self, state_vec):
                # Use token count and latency
                if state_vec[1] > 0.5:
                    return np.array([0.1, 0.3, 0.6])
                elif state_vec[2] < 0.1:
                    return np.array([0.7, 0.1, 0.2])
                else:
                    return np.array([0.4, 0.3, 0.3])

            def _gate_forward(self, state_vec):
                logits = state_vec @ self.gate_weights + self.gate_bias
                exp = np.exp(logits - np.max(logits))
                return exp / exp.sum()

            def select_action(self, state_vec, exploration=True):
                teacher_outputs = [
                    self._rule_teacher(state_vec),
                    self._rlhf_teacher(state_vec),
                    self._historical_teacher(state_vec)
                ]
                teacher_outputs = np.array(teacher_outputs)
                gate_weights = self._gate_forward(state_vec)
                teacher_probs = np.sum(gate_weights[:, None] * teacher_outputs, axis=0)
                teacher_probs /= teacher_probs.sum()

                student_logits = state_vec @ self.student_weights + self.student_bias
                student_probs = np.exp(student_logits - np.max(student_logits))
                student_probs /= student_probs.sum()

                if exploration and random.random() < self.epsilon:
                    action = random.randint(0, self.n_actions - 1)
                else:
                    combined = 0.8 * student_probs + 0.2 * teacher_probs
                    action = int(np.argmax(combined))

                return action, state_vec, teacher_probs

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

        return DistillationOptimizer(self.config)

    # ------------------------------------------------------------------
    # State construction and metric estimation helpers
    # ------------------------------------------------------------------
    def _build_state_vec(self, task_input: Dict[str, Any]) -> np.ndarray:
        """Build normalized feature vector from task_input and environment context."""
        # Extract values with defaults
        complexity = float(task_input.get('complexity', 0.5))
        token_count = float(task_input.get('token_count', 1000))
        latency_target = float(task_input.get('latency_target', 500.0))
        carbon_intensity = float(
            task_input.get('carbon_intensity', self.node_descriptor.region_carbon_intensity
                           if self.node_descriptor else 400.0)
        )
        human_feedback = float(task_input.get('human_feedback_score', 0.5))
        graph_metrics = task_input.get('graph_metrics', {})
        centrality = float(graph_metrics.get('centrality', 0.5))
        connectivity = float(graph_metrics.get('connectivity', 0.5))
        cost_budget = float(task_input.get('cost_budget', 10.0))

        # Normalize
        vec = np.array([
            min(complexity, 1.0),
            min(token_count / 10000.0, 1.0),
            min(latency_target / 1000.0, 1.0),
            min(carbon_intensity / 500.0, 1.0),
            human_feedback,
            centrality,
            connectivity,
            min(cost_budget / 100.0, 1.0),
        ], dtype=np.float32)
        return vec

    def _estimate_energy_carbon(self, execution_time_s: float, token_count: int) -> Tuple[float, float]:
        """Rough estimation of energy (kWh) and carbon (kg)."""
        energy_kwh = (execution_time_s / 3600) * 0.25  # 250W
        carbon_kg = energy_kwh * 0.4  # 400 gCO2/kWh
        # Scale with token count
        energy_kwh *= (1 + token_count / 10000)
        carbon_kg *= (1 + token_count / 10000)
        return energy_kwh, carbon_kg

    # ------------------------------------------------------------------
    # Enhanced run pipeline
    # ------------------------------------------------------------------
    def run_with_enhancements(self, task_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the agent using the enhanced decision pipeline.
        This method should be called by subclasses that want to use enhancements.
        It performs:
            1. Build state vector.
            2. Use distillation optimizer to select execution strategy.
            3. Execute the agent (via self._execute).
            4. Compute MODP reward and update models.
            5. Emit FeedbackEvent if configured.
        Returns a dictionary with the agent's output and metrics.
        """
        start_time = time.perf_counter()
        # 1. Build state
        state_vec = self._build_state_vec(task_input)

        # 2. Select action
        action_idx, state_vec, teacher_probs = self.distillation_optimizer.select_action(
            state_vec, exploration=True
        )
        # Map action to strategy
        strategy_map = {
            0: "native",
            1: "flexgen_low_precision",
            2: "flexgen_high_precision",
        }
        strategy = strategy_map.get(action_idx, "native")

        # 3. Execute agent
        # The subclass must implement _execute(strategy, task_input)
        # For simplicity, we assume that the subclass will handle strategy.
        # We can also provide a default _execute that just calls self.agent.
        # We'll call self._execute(task_input, strategy=strategy)
        # If not overridden, we call self.run(task_input) but that would cause recursion.
        # So we require subclasses to implement _execute when using run_with_enhancements.
        # To keep compatibility, we can have a default _execute that calls self.agent
        # but we don't know the framework specifics. So we'll make _execute an abstract method
        # that subclasses must implement if they use run_with_enhancements.
        # However, we can also provide a fallback: if self.agent has a 'generate' or 'run' method,
        # call it. We'll implement a simple dispatch.
        result = self._execute(task_input, strategy=strategy)

        execution_time_s = time.perf_counter() - start_time
        latency_ms = execution_time_s * 1000

        # 4. Compute metrics and reward
        token_count = task_input.get('token_count', 1000)
        energy_kwh, carbon_kg = self._estimate_energy_carbon(execution_time_s, token_count)

        # Build output
        output = {
            "latency": execution_time_s,
            "latency_ms": latency_ms,
            "execution_strategy": strategy,
            **result  # includes agent's own outputs like accuracy
        }

        if self.track_green_metrics:
            output["energy_kwh"] = energy_kwh
            output["carbon_kg"] = carbon_kg
            # Add graph metrics and human feedback for transparency
            graph_metrics = task_input.get('graph_metrics', {})
            output["graph_metrics"] = graph_metrics
            output["human_feedback_score"] = task_input.get('human_feedback_score', 0.5)

        # Compute MODP reward
        # We need carbon_intensity, complexity, etc.
        # Use state_vec components
        carbon_norm = 1.0 - state_vec[3]  # inverted carbon
        energy_norm = 1.0 - min(energy_kwh, 1.0)
        latency_norm = 1.0 - min(latency_ms / 1000.0, 1.0)
        cost_norm = 1.0 - state_vec[7]  # inverted cost (cost_budget normalized)
        # We can optionally incorporate accuracy from result
        accuracy = result.get('accuracy', 0.5)
        # We could use custom MODP weights from config
        modp_weights = self.config.get('modp_weights', [0.4, 0.3, 0.2, 0.1])
        # Ensure sum to 1
        modp_weights = np.array(modp_weights) / np.sum(modp_weights)
        # Combine objectives: carbon, energy, latency, cost, (accuracy optional)
        reward = float(np.dot([carbon_norm, energy_norm, latency_norm, cost_norm], modp_weights))

        # 5. Update distillation optimizer
        next_state_vec = state_vec  # simplified
        self.distillation_optimizer.update(state_vec, action_idx, reward, next_state_vec, teacher_probs)

        # Update workload descriptor if present
        if self.workload_descriptor:
            try:
                # Need to run async method; we'll use asyncio.run if not in async context
                import asyncio
                asyncio.run(self.workload_descriptor.record_outcome(
                    latency_achieved_ms=latency_ms,
                    carbon_saved_kg=max(0, 0.01 - carbon_kg),
                    energy_used_joules=energy_kwh * 3600
                ))
            except Exception as e:
                logger.warning(f"Failed to update workload descriptor: {e}")

        # Emit FeedbackEvent if zero_trust or message queue available
        if self.zero_trust and FeedbackEvent is not None:
            try:
                event = FeedbackEvent(
                    source=f"{self.framework_name}_adapter",
                    feedback_type="routing",
                    task_id=task_input.get('task_id', 'default_task'),
                    context={"strategy": strategy},
                    action={"selected_action": strategy, "selected_rank": 1},
                    performance={"quality_score": reward,
                                 "latency_ms": latency_ms,
                                 "energy_joules": energy_kwh * 3600,
                                 "carbon_g": carbon_kg * 1000,
                                 "helium_cost": 0,
                                 "duration_ms": latency_ms},
                    adaptive_cost_value=reward,
                    tags=[self.framework_name, strategy]
                )
                # In production, publish to queue; here we just log.
                logger.debug(f"FeedbackEvent created: {event.event_id}")
            except Exception as e:
                logger.warning(f"Failed to create FeedbackEvent: {e}")

        output["reward"] = reward
        return output

    # ------------------------------------------------------------------
    # Abstract methods
    # ------------------------------------------------------------------
    @abstractmethod
    def _execute(self, task_input: Dict[str, Any], strategy: str = "native") -> Dict[str, Any]:
        """
        Execute the underlying agent with the given strategy.
        Subclasses must implement this method. The returned dict may contain
        keys like 'accuracy', 'output', etc. which will be merged into the final output.
        """
        pass

    @abstractmethod
    def _get_agent_name(self) -> str:
        """Get agent name from framework-specific agent."""
        pass

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
    def run(self, task_input: Dict[str, Any],
            track_green_metrics: Optional[bool] = None) -> Dict[str, Any]:
        """
        Run agent on task input.
        If enhancements are enabled, uses run_with_enhancements pipeline;
        otherwise calls _execute directly with minimal overhead.
        """
        if track_green_metrics is not None:
            self.track_green_metrics = track_green_metrics

        if self.use_enhancements:
            return self.run_with_enhancements(task_input)
        else:
            # Legacy behavior: just execute and optionally add metrics
            start = time.perf_counter()
            result = self._execute(task_input, strategy="native")
            latency_s = time.perf_counter() - start
            output = {**result, "latency": latency_s, "latency_ms": latency_s * 1000}
            if self.track_green_metrics:
                token_count = task_input.get('token_count', 1000)
                energy_kwh, carbon_kg = self._estimate_energy_carbon(latency_s, token_count)
                output["energy_kwh"] = energy_kwh
                output["carbon_kg"] = carbon_kg
            return output

    def get_metadata(self) -> Dict[str, Any]:
        """Get agent metadata."""
        meta = {
            "agent_name": self.agent_name,
            "framework": self.framework_name,
            "adapter_version": "2.4.2",
            "enhancements_enabled": self.use_enhancements,
        }
        if self.use_enhancements:
            meta["modp_weights"] = self.config.get('modp_weights', None)
            meta["use_zero_trust"] = self.zero_trust is not None
            meta["use_workload_descriptor"] = self.workload_descriptor is not None
            meta["use_node_descriptor"] = self.node_descriptor is not None
        return meta
