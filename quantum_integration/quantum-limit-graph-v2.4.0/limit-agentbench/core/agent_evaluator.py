# -*- coding: utf-8 -*-
"""
Agent Evaluator (Enhanced)
Unified evaluation framework for agents across different frameworks,
with optional integration of LIMIT Graph, MODP, RLHF, Multi‑Teacher
On‑Policy Distillation, Bio‑inspired Optimisation, and MoE expert gating.

The evaluator can now select an execution strategy (native vs FlexGen low/high)
using a distillation optimizer, and calculate multi‑objective rewards.
"""

from typing import Dict, Any, List, Optional, Tuple
import logging
import random
import numpy as np
from collections import deque

from .agentbench_adapter import AgentBenchAdapter
from .green_metrics import GreenMetricsTracker

# Optional imports for enhancements
try:
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    NodeDescriptor = None
    WorkloadDescriptor = None
    ZeroTrustArchitecture = None
    FeedbackEvent = None

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# Enhanced State and Distillation Optimizer
# ------------------------------------------------------------------------------
class EvaluationState:
    """State representation for distillation to choose evaluation strategy."""
    def __init__(self, task: Dict[str, Any], agent: Any,
                 carbon_intensity: float = 400.0,
                 graph_metrics: Optional[Dict[str, float]] = None,
                 human_feedback_score: float = 0.5):
        self.task_complexity = task.get('complexity', 0.5)
        self.task_tokens = task.get('token_count', 1000)
        self.task_latency_target = task.get('latency_target', 500.0)
        self.carbon_intensity = carbon_intensity
        self.graph_centrality = (graph_metrics or {}).get('centrality', 0.5)
        self.graph_connectivity = (graph_metrics or {}).get('connectivity', 0.5)
        self.human_feedback = human_feedback_score
        self.agent_framework = self._framework_onehot(agent)
        self.has_backend = 1.0 if hasattr(agent, 'backend') else 0.0

    def _framework_onehot(self, agent) -> List[float]:
        # Simplified: map common framework names to one-hot (4 dims)
        fw = getattr(agent, 'framework', 'unknown')
        mapping = {
            'autogen': [1,0,0,0],
            'langchain': [0,1,0,0],
            'crewai': [0,0,1,0],
            'limit_graph': [0,0,0,1]
        }
        return mapping.get(fw, [0,0,0,0])

    def to_feature_vector(self) -> np.ndarray:
        return np.array([
            min(self.task_complexity, 1.0),
            min(self.task_tokens / 10000.0, 1.0),
            min(self.task_latency_target / 1000.0, 1.0),
            min(self.carbon_intensity / 500.0, 1.0),
            self.graph_centrality,
            self.graph_connectivity,
            self.human_feedback,
            *self.agent_framework,
            self.has_backend
        ], dtype=np.float32)


class EvaluationDistillationOptimizer:
    """
    Distillation + MoE gating to select evaluation execution strategy.
    Actions: 0 = native agent, 1 = FlexGen low precision, 2 = FlexGen high precision.
    """
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.feature_dim = 11  # 7 + 4 framework onehot
        self.n_actions = 3
        self.lr = self.config.get('distillation_lr', 0.01)
        self.epsilon = self.config.get('epsilon', 0.1)
        self.distill_w = self.config.get('distill_weight', 0.7)
        self.rl_w = self.config.get('rl_weight', 0.3)
        self.train_every = self.config.get('train_every', 10)
        self.counter = 0
        self.replay_buffer = deque(maxlen=self.config.get('replay_size', 2000))

        # Student
        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)

        # Teachers
        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher
        ]
        # MoE gating
        self.gate_weights = np.random.randn(self.feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = self.config.get('gating_lr', 0.005)

    def _rule_teacher(self, state: EvaluationState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.1
        if state.task_complexity > 0.7:
            probs[0] = 0.7  # use native for complex tasks
        elif state.carbon_intensity > 400:
            probs[1] = 0.6  # flexgen low precision to save energy
        else:
            probs[2] = 0.5
        return probs / probs.sum()

    def _rlhf_teacher(self, state: EvaluationState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback > 0.7:
            probs[0] += 0.2  # prefer native (quality)
        elif state.human_feedback < 0.3:
            probs[1] += 0.2  # prefer flexgen low (efficiency)
        return probs / probs.sum()

    def _historical_teacher(self, state: EvaluationState) -> np.ndarray:
        # Simulate trained model
        if state.task_tokens > 5000:
            return np.array([0.1, 0.3, 0.6])
        elif state.task_latency_target < 100:
            return np.array([0.7, 0.1, 0.2])
        else:
            return np.array([0.4, 0.3, 0.3])

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_action(self, state: EvaluationState, exploration=True):
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
# Enhanced AgentEvaluator
# ------------------------------------------------------------------------------
class AgentEvaluator:
    """
    Unified agent evaluation framework with optional enhancements.
    """

    def __init__(
        self,
        grid_region: str = "GLOBAL",
        hardware_profile: str = "default",
        track_green_metrics: bool = True,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize agent evaluator.

        Args:
            grid_region: Grid region for carbon calculations
            hardware_profile: Hardware profile for power estimation
            track_green_metrics: Whether to track energy/carbon
            config: Optional configuration dict for enhancements
        """
        self.grid_region = grid_region
        self.hardware_profile = hardware_profile
        self.track_green_metrics = track_green_metrics
        self.adapter = AgentBenchAdapter()
        self.config = config or {}
        self.use_enhancements = self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE

        # Enhanced components
        self.distillation_optimizer = None
        self.node_descriptor = None
        self.workload_descriptor = None
        self.zero_trust = None

        if self.use_enhancements:
            self.distillation_optimizer = EvaluationDistillationOptimizer(self.config)
            if self.config.get('use_node_descriptor', False) and NodeDescriptor is not None:
                self.node_descriptor = NodeDescriptor(
                    id="evaluator_node",
                    type=NodeType.EDGE if 'NodeType' in globals() else None,
                    region=self.config.get('region', 'us-east'),
                    region_carbon_intensity=self.config.get('carbon_intensity', 400.0),
                    energy_per_token=0.00005,
                    helium_connectivity_score=0.8,
                    uptime=0.99,
                    renewable_fraction=0.3,
                    cooling_type="air",
                    hardware_model="cpu"
                )
            if self.config.get('enable_zero_trust', False):
                self.zero_trust = ZeroTrustArchitecture()
            logger.info("AgentEvaluator enhanced components initialized")

    def evaluate(
        self,
        agent: Any,
        task: Dict[str, Any],
        backend: Optional[str] = None,
        rank: Optional[int] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Evaluate an agent on a task, optionally using enhanced decision-making.

        Args:
            agent: Agent instance to evaluate
            task: Task definition
            backend: Quantum backend (if applicable)
            rank: NSN rank (if applicable)
            graph_metrics: Optional LIMIT Graph metrics
            human_feedback_score: Optional RLHF feedback (0-1)

        Returns:
            Evaluation result with metrics
        """
        logger.info(f"Evaluating agent on task {task.get('task_id', 'unknown')}")

        # Enhanced: use distillation to select execution strategy
        selected_strategy = "native"
        if self.use_enhancements and self.distillation_optimizer:
            if human_feedback_score is None:
                human_feedback_score = self.config.get('human_feedback_score', 0.5)
            if graph_metrics is None:
                graph_metrics = self.config.get('graph_metrics', {})
                if self.node_descriptor and self.node_descriptor.graph_metrics:
                    graph_metrics = {
                        'centrality': self.node_descriptor.graph_metrics.get('centrality', 0.5),
                        'connectivity': self.node_descriptor.graph_metrics.get('connectivity', 0.5)
                    }
            carbon_intensity = self.node_descriptor.region_carbon_intensity if self.node_descriptor else 400.0
            state = EvaluationState(
                task=task,
                agent=agent,
                carbon_intensity=carbon_intensity,
                graph_metrics=graph_metrics,
                human_feedback_score=human_feedback_score
            )
            action_idx, state_vec, teacher_probs = self.distillation_optimizer.select_action(state)
            strategy_map = {0: "native", 1: "flexgen_low_precision", 2: "flexgen_high_precision"}
            selected_strategy = strategy_map[action_idx]
            # Store for later update
            self._last_decision = (state_vec, action_idx, teacher_probs, state, selected_strategy)
        else:
            self._last_decision = None

        # Call adapter with selected strategy (may be ignored by adapter)
        result = self.adapter.evaluate_agent(
            agent=agent,
            task=task,
            track_energy=self.track_green_metrics,
            track_carbon=self.track_green_metrics,
            backend=backend,
            rank=rank,
            execution_strategy=selected_strategy  # extra param, ignored if not supported
        )

        # Calculate sustainability index if metrics available
        if self.track_green_metrics and result.get("metrics"):
            metrics = result["metrics"]
            if all(k in metrics for k in ["accuracy", "energy_kwh", "carbon_co2e_kg"]):
                tracker = GreenMetricsTracker()
                sustainability_index = tracker.get_sustainability_index(
                    accuracy=metrics["accuracy"],
                    energy_kwh=metrics["energy_kwh"],
                    carbon_co2e_kg=metrics["carbon_co2e_kg"]
                )
                result["metrics"]["sustainability_index"] = sustainability_index

        # Enhanced: update distillation optimizer and record feedback
        if self.use_enhancements and self._last_decision:
            state_vec, action_idx, teacher_probs, state, selected_strategy = self._last_decision
            # Compute MODP reward (simplified)
            metrics = result.get("metrics", {})
            accuracy = metrics.get("accuracy", 0.5)
            energy_kwh = metrics.get("energy_kwh", 0.01)
            carbon_kg = metrics.get("carbon_co2e_kg", 0.001)
            latency_ms = metrics.get("latency_ms", 100)

            # Normalize
            carbon_norm = 1.0 - min(carbon_kg / 0.01, 1.0)
            energy_norm = 1.0 - min(energy_kwh, 1.0)
            latency_norm = 1.0 - min(latency_ms / 1000.0, 1.0)
            quality_norm = min(accuracy, 1.0)

            # Use MODP weights from config or defaults
            modp_weights = self.config.get('modp_weights', [0.35, 0.25, 0.2, 0.2])  # quality, energy, latency, carbon
            modp_weights = np.array(modp_weights) / np.sum(modp_weights)
            reward = float(np.dot([quality_norm, energy_norm, latency_norm, carbon_norm], modp_weights))

            # Update distillation optimizer
            self.distillation_optimizer.update(state_vec, action_idx, reward, state_vec, teacher_probs)

            # Add enhanced info to result
            result["execution_strategy"] = selected_strategy
            result["modp_reward"] = reward
            result["graph_metrics"] = graph_metrics
            result["human_feedback_score"] = human_feedback_score
            if self.distillation_optimizer:
                result["distillation_stats"] = {
                    "student_counter": self.distillation_optimizer.counter,
                    "buffer_size": len(self.distillation_optimizer.replay_buffer)
                }

            # Emit FeedbackEvent if zero_trust available
            if self.zero_trust and FeedbackEvent is not None:
                try:
                    event = FeedbackEvent(
                        source="agent_evaluator",
                        feedback_type="routing",
                        task_id=task.get('task_id', 'unknown'),
                        context={"strategy": selected_strategy},
                        action={"selected_action": selected_strategy, "selected_rank": 1},
                        performance={"quality_score": reward,
                                     "latency_ms": latency_ms,
                                     "energy_joules": energy_kwh * 3600,
                                     "carbon_g": carbon_kg * 1000,
                                     "helium_cost": 0,
                                     "duration_ms": latency_ms},
                        adaptive_cost_value=reward,
                        tags=["evaluation", selected_strategy]
                    )
                    logger.debug(f"FeedbackEvent created: {event.event_id}")
                except Exception:
                    pass

        return result

    def evaluate_suite(
        self,
        agent: Any,
        tasks: List[Dict[str, Any]],
        backend: Optional[str] = None,
        rank: Optional[int] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Evaluate an agent on a suite of tasks, optionally with enhancements.
        """
        logger.info(f"Evaluating agent on {len(tasks)} tasks")
        results = []
        for task in tasks:
            result = self.evaluate(agent, task, backend, rank, graph_metrics, human_feedback_score)
            results.append(result)

        aggregated = self._aggregate_results(results)

        suite_result = {
            "agent_name": getattr(agent, 'name', agent.__class__.__name__),
            "framework": getattr(agent, 'framework', 'unknown'),
            "num_tasks": len(tasks),
            "results": results,
            "aggregated_metrics": aggregated
        }

        # Add enhanced summary if enabled
        if self.use_enhancements and self.distillation_optimizer:
            suite_result["distillation_summary"] = {
                "student_counter": self.distillation_optimizer.counter,
                "buffer_size": len(self.distillation_optimizer.replay_buffer)
            }

        return suite_result

    def _aggregate_results(self, results: List[Dict[str, Any]]) -> Dict[str, float]:
        """Aggregate metrics across multiple results."""
        if not results:
            return {}

        metric_values = {}
        for result in results:
            if not result.get("success"):
                continue
            metrics = result.get("metrics", {})
            for key, value in metrics.items():
                if isinstance(value, (int, float)):
                    if key not in metric_values:
                        metric_values[key] = []
                    metric_values[key].append(value)

        aggregated = {}
        for key, values in metric_values.items():
            if values:
                aggregated[f"avg_{key}"] = sum(values) / len(values)
                aggregated[f"min_{key}"] = min(values)
                aggregated[f"max_{key}"] = max(values)

        success_count = sum(1 for r in results if r.get("success"))
        aggregated["success_rate"] = success_count / len(results)

        # If enhanced, aggregate MODP rewards if present
        modp_rewards = [r.get("modp_reward") for r in results if r.get("modp_reward") is not None]
        if modp_rewards:
            aggregated["avg_modp_reward"] = sum(modp_rewards) / len(modp_rewards)

        return aggregated

    def compare_agents(
        self,
        agents: List[Any],
        tasks: List[Dict[str, Any]],
        sort_by: str = "sustainability_index",
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Compare multiple agents on the same tasks.
        """
        logger.info(f"Comparing {len(agents)} agents on {len(tasks)} tasks")

        agent_results = []
        for agent in agents:
            suite_result = self.evaluate_suite(agent, tasks, graph_metrics=graph_metrics,
                                               human_feedback_score=human_feedback_score)
            agent_results.append(suite_result)

        # Sort by specified metric
        sort_key = f"avg_{sort_by}" if sort_by != "success_rate" else "success_rate"
        if sort_key in agent_results[0].get("aggregated_metrics", {}):
            agent_results.sort(
                key=lambda x: x["aggregated_metrics"].get(sort_key, 0),
                reverse=True
            )

        return {
            "num_agents": len(agents),
            "num_tasks": len(tasks),
            "sort_by": sort_by,
            "rankings": agent_results
        }
