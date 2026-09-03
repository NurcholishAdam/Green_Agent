# -*- coding: utf-8 -*-
"""
AgentBench Protocol Adapter (Enhanced)
Provides standardized interface compatible with AgentBench protocol,
with optional integration of LIMIT Graph, MODP, RLHF,
Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, and MoE expert gating.
"""

import json
import hashlib
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import logging
import random
import numpy as np
from collections import deque

logger = logging.getLogger(__name__)

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


# ------------------------------------------------------------------------------
# Enhanced Decision State and Distillation Optimizer
# ------------------------------------------------------------------------------
class EvaluationState:
    """State representation for distillation to decide execution strategy."""
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
    Distillation + MoE gating to select execution strategy.
    Actions: 0 = native, 1 = FlexGen low precision, 2 = FlexGen high precision.
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
            probs[0] = 0.7  # native for complex tasks
        elif state.carbon_intensity > 400:
            probs[1] = 0.6  # flexgen low precision
        else:
            probs[2] = 0.5
        return probs / probs.sum()

    def _rlhf_teacher(self, state: EvaluationState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback > 0.7:
            probs[0] += 0.2
        elif state.human_feedback < 0.3:
            probs[1] += 0.2
        return probs / probs.sum()

    def _historical_teacher(self, state: EvaluationState) -> np.ndarray:
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
# Enhanced AgentBenchAdapter
# ------------------------------------------------------------------------------
class AgentBenchAdapter:
    """
    Adapter for AgentBench protocol compatibility with optional enhancements.
    """

    def __init__(self, protocol_version: str = "1.0",
                 config: Optional[Dict[str, Any]] = None):
        self.protocol_version = protocol_version
        self.config = config or {}
        self.use_enhancements = self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE
        self.task_registry = {}

        # Enhanced components
        self.distillation_optimizer = None
        self.node_descriptor = None
        self.zero_trust = None

        if self.use_enhancements:
            self.distillation_optimizer = EvaluationDistillationOptimizer(self.config)
            if self.config.get('use_node_descriptor', False) and NodeDescriptor is not None:
                self.node_descriptor = NodeDescriptor(
                    id="agentbench_node",
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
            logger.info("AgentBenchAdapter enhanced components initialized")

    def create_task(self, task_id, suite, task_type, input_data,
                    expected_output=None, evaluation_metrics=None,
                    difficulty="medium", timeout_seconds=30) -> Dict[str, Any]:
        """Create a task in AgentBench format, optionally accepting graph metrics."""
        if evaluation_metrics is None:
            evaluation_metrics = ["accuracy", "latency", "energy_kwh", "carbon_co2e_kg"]

        task = {
            "task_id": task_id,
            "suite": suite,
            "type": task_type,
            "difficulty": difficulty,
            "input": input_data,
            "expected_output": expected_output,
            "evaluation": {
                "metrics": evaluation_metrics,
                "timeout_seconds": timeout_seconds
            },
            "protocol_version": self.protocol_version,
            "created_at": datetime.utcnow().isoformat() + "Z"
        }
        self.task_registry[task_id] = task
        logger.info(f"Created task {task_id} in suite {suite}")
        return task

    def evaluate_agent(self, agent, task, track_energy=True, track_carbon=True,
                       backend=None, rank=None,
                       graph_metrics: Optional[Dict[str, float]] = None,
                       human_feedback_score: Optional[float] = None) -> Dict[str, Any]:
        """
        Evaluate an agent on a task, optionally using enhanced decision-making.
        """
        from .green_metrics import GreenMetricsTracker

        task_id = task["task_id"]
        logger.info(f"Evaluating agent on task {task_id}")

        # Enhanced decision if enabled
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
            self._last_decision = (state_vec, action_idx, teacher_probs, state, selected_strategy)
        else:
            self._last_decision = None

        # Green metrics tracking
        tracker = None
        if track_energy or track_carbon:
            tracker = GreenMetricsTracker(track_energy=track_energy, track_carbon=track_carbon)
            tracker.start()

        start_time = datetime.utcnow()
        try:
            # Execute agent (note: could use strategy to decide whether to delegate to FlexGen)
            if selected_strategy in ["flexgen_low_precision", "flexgen_high_precision"]:
                # In a real system, we might call FlexGen; here we just simulate native execution
                # but record the strategy.
                if hasattr(agent, 'run'):
                    output = agent.run(task["input"])
                elif callable(agent):
                    output = agent(task["input"])
                else:
                    raise ValueError("Agent must have 'run' method or be callable")
            else:
                if hasattr(agent, 'run'):
                    output = agent.run(task["input"])
                elif callable(agent):
                    output = agent(task["input"])
                else:
                    raise ValueError("Agent must have 'run' method or be callable")
            success = True
            error = None
        except Exception as e:
            logger.error(f"Agent execution failed: {e}")
            output = None
            success = False
            error = str(e)

        end_time = datetime.utcnow()
        latency_ms = (end_time - start_time).total_seconds() * 1000

        green_metrics = {}
        if tracker:
            tracker.stop()
            green_metrics = tracker.get_metrics()

        accuracy = None
        if task.get("expected_output") and output:
            accuracy = self._calculate_accuracy(output, task["expected_output"])

        result = {
            "task_id": task_id,
            "agent_name": getattr(agent, 'name', agent.__class__.__name__),
            "framework": getattr(agent, 'framework', 'unknown'),
            "success": success,
            "output": output,
            "error": error,
            "metrics": {
                "latency_ms": latency_ms,
                **green_metrics
            },
            "provenance": {
                "hash": self._compute_hash(task, output),
                "timestamp": end_time.isoformat() + "Z",
                "backend": backend,
                "rank": rank
            },
            "protocol_version": self.protocol_version
        }

        if accuracy is not None:
            result["metrics"]["accuracy"] = accuracy

        # Enhanced post-processing
        if self.use_enhancements and self._last_decision:
            state_vec, action_idx, teacher_probs, state, selected_strategy = self._last_decision
            # Compute MODP reward
            metrics = result.get("metrics", {})
            accuracy_val = metrics.get("accuracy", 0.5)
            energy_kwh = metrics.get("energy_kwh", 0.01)
            carbon_kg = metrics.get("carbon_co2e_kg", 0.001)
            latency_val = metrics.get("latency_ms", 100)

            carbon_norm = 1.0 - min(carbon_kg / 0.01, 1.0)
            energy_norm = 1.0 - min(energy_kwh, 1.0)
            latency_norm = 1.0 - min(latency_val / 1000.0, 1.0)
            quality_norm = min(accuracy_val, 1.0)

            modp_weights = self.config.get('modp_weights', [0.35, 0.25, 0.2, 0.2])
            modp_weights = np.array(modp_weights) / np.sum(modp_weights)
            reward = float(np.dot([quality_norm, energy_norm, latency_norm, carbon_norm], modp_weights))

            self.distillation_optimizer.update(state_vec, action_idx, reward, state_vec, teacher_probs)

            result["execution_strategy"] = selected_strategy
            result["modp_reward"] = reward
            result["graph_metrics"] = graph_metrics
            result["human_feedback_score"] = human_feedback_score
            result["distillation_stats"] = {
                "student_counter": self.distillation_optimizer.counter,
                "buffer_size": len(self.distillation_optimizer.replay_buffer)
            }

            # Emit FeedbackEvent if zero_trust available
            if self.zero_trust and FeedbackEvent is not None:
                try:
                    event = FeedbackEvent(
                        source="agentbench_adapter",
                        feedback_type="routing",
                        task_id=task_id,
                        context={"strategy": selected_strategy},
                        action={"selected_action": selected_strategy, "selected_rank": 1},
                        performance={"quality_score": reward,
                                     "latency_ms": latency_val,
                                     "energy_joules": energy_kwh * 3600,
                                     "carbon_g": carbon_kg * 1000,
                                     "helium_cost": 0,
                                     "duration_ms": latency_val},
                        adaptive_cost_value=reward,
                        tags=["agentbench", selected_strategy]
                    )
                    logger.debug(f"FeedbackEvent created: {event.event_id}")
                except Exception:
                    pass

        logger.info(f"Evaluation complete for task {task_id}")
        return result

    # Existing helper methods remain unchanged
    def _calculate_accuracy(self, output, expected) -> float:
        if isinstance(output, dict) and isinstance(expected, dict):
            matches = sum(1 for k, v in expected.items() if k in output and output[k] == v)
            return matches / len(expected) if expected else 0.0
        elif isinstance(output, str) and isinstance(expected, str):
            return 1.0 if output.strip().lower() == expected.strip().lower() else 0.0
        else:
            return 1.0 if output == expected else 0.0

    def _compute_hash(self, task, output) -> str:
        data = json.dumps({
            "task_id": task["task_id"],
            "output": str(output),
            "timestamp": datetime.utcnow().isoformat()
        }, sort_keys=True)
        return hashlib.sha256(data.encode()).hexdigest()

    def validate_result(self, result) -> bool:
        required_fields = ["task_id", "agent_name", "framework", "success", "metrics", "provenance", "protocol_version"]
        return all(field in result for field in required_fields)

    def export_to_json(self, result, filepath):
        with open(filepath, 'w') as f:
            json.dump(result, f, indent=2)
        logger.info(f"Result exported to {filepath}")

    def load_from_json(self, filepath) -> Dict[str, Any]:
        with open(filepath, 'r') as f:
            result = json.load(f)
        logger.info(f"Result loaded from {filepath}")
        return result
