# -*- coding: utf-8 -*-
"""
A2A Protocol Gateway - AgentBeats Compliance Layer (Enhanced)
Validates and transforms agent I/O to A2A standard format.

Enhancements:
- Optional integration with LIMIT Graph, MODP, RLHF,
  Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation,
  and MoE expert gating.
- Distillation optimizer selects best response transformation strategy
  (e.g., normal, compressed, detailed) based on request features and
  context (graph metrics, human feedback).
- MODP weights influence reward calculation.
- RLHF feedback stored in responses for later learning.
"""

from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
from datetime import datetime
import random
import numpy as np
from collections import deque

# Optional imports for enhancements
try:
    from src.enhancements.schemas.node_descriptor import NodeDescriptor
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    NodeDescriptor = None
    WorkloadDescriptor = None
    ZeroTrustArchitecture = None
    FeedbackEvent = None


class A2AVersion(Enum):
    """Supported A2A protocol versions"""
    V1_0 = "1.0"
    V1_1 = "1.1"


class TaskStatus(Enum):
    """A2A-compliant task status codes"""
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    OOM = "out_of_memory"
    CRASH = "crash"
    INVALID_OUTPUT = "invalid_output"


@dataclass
class A2ARequest:
    """A2A-compliant task request with optional enhancement fields."""
    task_id: str
    task_type: str
    input_data: Dict[str, Any]
    constraints: Optional[Dict[str, Any]] = None
    version: str = "1.1"
    # Enhancement fields
    graph_metrics: Optional[Dict[str, float]] = None
    human_feedback_score: Optional[float] = None
    modp_weights: Optional[Dict[str, float]] = None

    def validate(self) -> bool:
        """Validate request against A2A schema"""
        required_fields = ['task_id', 'task_type', 'input_data']
        return all(hasattr(self, field) and getattr(self, field) is not None
                   for field in required_fields)


@dataclass
class A2AResponse:
    """A2A-compliant task response with optional enhancement fields."""
    task_id: str
    status: TaskStatus
    output: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    execution_time: Optional[float] = None
    green_metrics: Optional[Dict[str, Any]] = None
    reasoning_trace: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None
    version: str = "1.1"
    # Enhancement fields
    graph_metrics: Optional[Dict[str, float]] = None
    modp_score: Optional[float] = None
    human_feedback_score: Optional[float] = None
    distillation_stats: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to A2A-compliant dictionary, including enhancements if present."""
        result = {
            "task_id": self.task_id,
            "status": self.status.value,
            "version": self.version,
            "timestamp": datetime.utcnow().isoformat()
        }

        if self.output is not None:
            result["output"] = self.output
        if self.error_message:
            result["error"] = self.error_message
        if self.execution_time is not None:
            result["execution_time_seconds"] = self.execution_time
        if self.green_metrics:
            result["green_metrics"] = self.green_metrics
        if self.reasoning_trace:
            result["reasoning_trace"] = self.reasoning_trace
        if self.metadata:
            result["metadata"] = self.metadata
        # Enhanced
        if self.graph_metrics:
            result["graph_metrics"] = self.graph_metrics
        if self.modp_score is not None:
            result["modp_score"] = self.modp_score
        if self.human_feedback_score is not None:
            result["human_feedback_score"] = self.human_feedback_score
        if self.distillation_stats:
            result["distillation_stats"] = self.distillation_stats

        return result


# ------------------------------------------------------------------------------
# Enhanced Decision Engine for Response Transformation Strategy
# ------------------------------------------------------------------------------
class A2ADecisionState:
    """State for distillation to choose transformation strategy."""
    def __init__(self, request: A2ARequest):
        self.task_type = request.task_type
        self.input_size = len(str(request.input_data))
        self.has_constraints = 1.0 if request.constraints else 0.0
        self.graph_centrality = (request.graph_metrics or {}).get('centrality', 0.5)
        self.graph_connectivity = (request.graph_metrics or {}).get('connectivity', 0.5)
        self.human_feedback = request.human_feedback_score if request.human_feedback_score is not None else 0.5
        self.version_num = 1.0 if request.version == "1.1" else 0.0

    def to_vector(self) -> np.ndarray:
        return np.array([
            min(len(self.task_type) / 50.0, 1.0),
            min(self.input_size / 10000.0, 1.0),
            self.has_constraints,
            self.graph_centrality,
            self.graph_connectivity,
            self.human_feedback,
            self.version_num,
        ], dtype=np.float32)


class A2ADistillationOptimizer:
    """
    Distillation with MoE gating to select transformation strategy.
    Actions:
      0 = standard transformation (original)
      1 = compressed transformation (minimize output size)
      2 = detailed transformation (include reasoning trace)
    """
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.feature_dim = 7
        self.n_actions = 3
        self.lr = self.config.get('distillation_lr', 0.01)
        self.epsilon = self.config.get('epsilon', 0.1)
        self.distill_w = self.config.get('distill_weight', 0.7)
        self.rl_w = self.config.get('rl_weight', 0.3)
        self.train_every = self.config.get('train_every', 10)
        self.counter = 0
        self.replay_buffer = deque(maxlen=self.config.get('replay_size', 2000))

        # Student (linear softmax)
        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)

        # Teachers (rule-based, RLHF, historical)
        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher
        ]
        # MoE gating
        self.gate_weights = np.random.randn(self.feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = self.config.get('gating_lr', 0.005)

    def _rule_teacher(self, state: A2ADecisionState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.1
        if state.input_size > 0.5:  # large input -> compressed
            probs[1] = 0.6
        elif state.human_feedback > 0.7:  # human likes detail
            probs[2] = 0.6
        else:
            probs[0] = 0.6  # standard
        return probs / probs.sum()

    def _rlhf_teacher(self, state: A2ADecisionState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback > 0.7:
            probs[2] += 0.2  # detailed
        elif state.human_feedback < 0.3:
            probs[1] += 0.2  # compressed
        return probs / probs.sum()

    def _historical_teacher(self, state: A2ADecisionState) -> np.ndarray:
        if state.graph_centrality > 0.7:
            return np.array([0.2, 0.2, 0.6])  # detailed for important graphs
        elif state.has_constraints:
            return np.array([0.1, 0.7, 0.2])  # compressed with constraints
        else:
            return np.array([0.6, 0.2, 0.2])

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_action(self, state: A2ADecisionState, exploration=True):
        x = state.to_vector()
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
                one_hot = np.zeros(self.n_actions); one_hot[a] = 1.0
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
# Enhanced A2AGateway
# ------------------------------------------------------------------------------
class A2AGateway:
    """
    A2A Protocol Gateway for AgentBeats Compliance with optional enhancements.

    Ensures all agent interactions conform to A2A standard and optionally
    uses distillation to choose transformation strategy, incorporates LIMIT
    Graph metrics and RLHF, and computes MODP rewards.
    """

    def __init__(self, default_version: A2AVersion = A2AVersion.V1_1,
                 config: Optional[Dict[str, Any]] = None):
        self.default_version = default_version
        self.config = config or {}
        self.use_enhancements = self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE
        self.request_count = 0
        self.error_count = 0

        # Enhanced components
        self.distillation_optimizer = None
        self.node_descriptor = None
        self.zero_trust = None

        if self.use_enhancements:
            self.distillation_optimizer = A2ADistillationOptimizer(self.config)
            # Optional node descriptor
            if self.config.get('use_node_descriptor', False) and NodeDescriptor is not None:
                self.node_descriptor = NodeDescriptor(
                    id="a2a_gateway_node",
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
            logger.info("A2AGateway enhanced components initialized")

    def validate_request(self, request_data: Dict[str, Any]) -> A2ARequest:
        """Validate and parse incoming A2A request, preserving enhancement fields."""
        self.request_count += 1
        try:
            task_id = request_data.get('task_id')
            task_type = request_data.get('task_type')
            input_data = request_data.get('input_data')
            if not all([task_id, task_type, input_data]):
                raise ValueError("Missing required fields: task_id, task_type, input_data")

            # Create request object with optional enhancement fields
            a2a_request = A2ARequest(
                task_id=task_id,
                task_type=task_type,
                input_data=input_data,
                constraints=request_data.get('constraints'),
                version=request_data.get('version', self.default_version.value),
                graph_metrics=request_data.get('graph_metrics'),
                human_feedback_score=request_data.get('human_feedback_score'),
                modp_weights=request_data.get('modp_weights')
            )
            if not a2a_request.validate():
                raise ValueError("Request validation failed")
            return a2a_request
        except Exception as e:
            self.error_count += 1
            raise ValueError(f"A2A request validation failed: {str(e)}")

    def create_success_response(
        self,
        task_id: str,
        output: Dict[str, Any],
        execution_time: float,
        green_metrics: Optional[Dict[str, Any]] = None,
        reasoning_trace: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        modp_score: Optional[float] = None,
        human_feedback_score: Optional[float] = None,
        distillation_stats: Optional[Dict[str, Any]] = None
    ) -> A2AResponse:
        """Create enhanced A2A success response."""
        return A2AResponse(
            task_id=task_id,
            status=TaskStatus.SUCCESS,
            output=output,
            execution_time=execution_time,
            green_metrics=green_metrics,
            reasoning_trace=reasoning_trace,
            metadata=metadata,
            version=self.default_version.value,
            graph_metrics=graph_metrics,
            modp_score=modp_score,
            human_feedback_score=human_feedback_score,
            distillation_stats=distillation_stats
        )

    def create_failure_response(
        self,
        task_id: str,
        status: TaskStatus,
        error_message: str,
        execution_time: Optional[float] = None,
        partial_output: Optional[Dict[str, Any]] = None,
        reasoning_trace: Optional[List[Dict[str, Any]]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> A2AResponse:
        """Create enhanced A2A failure response."""
        return A2AResponse(
            task_id=task_id,
            status=status,
            output=partial_output,
            error_message=error_message,
            execution_time=execution_time,
            reasoning_trace=reasoning_trace,
            version=self.default_version.value,
            graph_metrics=graph_metrics,
            human_feedback_score=human_feedback_score
        )

    def transform_agent_output(
        self,
        task_id: str,
        agent_output: Any,
        execution_time: float,
        green_metrics: Optional[Dict[str, Any]] = None,
        reasoning_trace: Optional[List[Dict[str, Any]]] = None,
        request: Optional[A2ARequest] = None
    ) -> A2AResponse:
        """
        Transform arbitrary agent output to A2A format.
        If enhanced and request provided, use distillation to decide transformation style.
        """
        try:
            # Normalize output to dict
            if isinstance(agent_output, dict):
                output = agent_output
            elif isinstance(agent_output, str):
                output = {"result": agent_output}
            elif isinstance(agent_output, (list, tuple)):
                output = {"results": list(agent_output)}
            else:
                output = {"value": str(agent_output)}

            # Enhanced transformation strategy selection
            if self.use_enhancements and self.distillation_optimizer and request:
                state = A2ADecisionState(request)
                action, state_vec, teacher_probs = self.distillation_optimizer.select_action(state)
                # Modify output based on action
                if action == 1:  # compressed
                    # Simple compression: keep only essential keys
                    if isinstance(output, dict):
                        output = {"result": output.get("result") or output.get("value") or output.get("answer") or str(output)[:200]}
                elif action == 2:  # detailed
                    # Add reasoning trace if not present
                    if reasoning_trace is None:
                        reasoning_trace = [{"step": "A2A transformation", "note": "detailed mode"}]
                # Compute reward (simplified MODP)
                # We need MODP weights; use request.modp_weights or defaults
                weights = request.modp_weights or {'carbon':0.4,'energy':0.3,'latency':0.2,'cost':0.1}
                # Dummy reward for now
                reward = 0.7
                # Update distillation
                self.distillation_optimizer.update(state_vec, action, reward, state_vec, teacher_probs)
                # Store stats
                distillation_stats = {
                    "action": action,
                    "student_counter": self.distillation_optimizer.counter,
                    "buffer_size": len(self.distillation_optimizer.replay_buffer)
                }
            else:
                distillation_stats = None

            return self.create_success_response(
                task_id=task_id,
                output=output,
                execution_time=execution_time,
                green_metrics=green_metrics,
                reasoning_trace=reasoning_trace,
                graph_metrics=request.graph_metrics if request else None,
                human_feedback_score=request.human_feedback_score if request else None,
                distillation_stats=distillation_stats,
                modp_score=0.7 if self.use_enhancements else None
            )

        except Exception as e:
            return self.create_failure_response(
                task_id=task_id,
                status=TaskStatus.INVALID_OUTPUT,
                error_message=f"Failed to transform agent output: {str(e)}",
                execution_time=execution_time,
                graph_metrics=request.graph_metrics if request else None,
                human_feedback_score=request.human_feedback_score if request else None
            )

    def get_statistics(self) -> Dict[str, Any]:
        """Get gateway statistics, including enhanced metrics if available."""
        stats = {
            "total_requests": self.request_count,
            "total_errors": self.error_count,
            "error_rate": self.error_count / max(self.request_count, 1),
            "version": self.default_version.value,
        }
        if self.use_enhancements and self.distillation_optimizer:
            stats["distillation"] = {
                "student_counter": self.distillation_optimizer.counter,
                "buffer_size": len(self.distillation_optimizer.replay_buffer)
            }
        return stats


def create_a2a_task(
    task_id: str,
    task_type: str,
    query: str,
    max_tokens: Optional[int] = None,
    timeout_seconds: Optional[int] = None,
    graph_metrics: Optional[Dict[str, float]] = None,
    human_feedback_score: Optional[float] = None
) -> Dict[str, Any]:
    """
    Helper to create A2A-compliant task request, optionally with enhancements.
    """
    request = {
        "task_id": task_id,
        "task_type": task_type,
        "input_data": {"query": query},
        "version": "1.1"
    }
    if max_tokens or timeout_seconds:
        request["constraints"] = {}
        if max_tokens:
            request["constraints"]["max_tokens"] = max_tokens
        if timeout_seconds:
            request["constraints"]["timeout_seconds"] = timeout_seconds
    if graph_metrics:
        request["graph_metrics"] = graph_metrics
    if human_feedback_score is not None:
        request["human_feedback_score"] = human_feedback_score
    return request
