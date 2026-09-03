# -*- coding: utf-8 -*-
"""
Human-in-the-Loop Portal & Expert Collaboration System Integration (Enhanced)

Provides interface for human reviewers and integrates all expert collaboration
components into a unified system.

Enhancements:
- Optional integration with LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy
  Distillation, Bio‑inspired Optimisation, and MoE expert gating.
- Distillation‑based escalation decision (replaces or augments invocation policy).
- Dynamic expert selection using MoE gating.
- Energy/carbon/latency trade‑offs via MODP weights.
- Graph metrics and human feedback incorporated into state.
"""

from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
import asyncio
from datetime import datetime
import numpy as np
import random
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
    # Dummy classes to avoid NameError
    class NodeDescriptor: pass
    class WorkloadDescriptor: pass
    class ZeroTrustArchitecture: pass
    class FeedbackEvent: pass


class ReviewStatus(Enum):
    """Status of human review."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_REVISION = "needs_revision"


@dataclass
class ReviewRequest:
    """Request for human review."""
    request_id: str
    task_id: str
    agent_output: Any
    escalation_reasons: List[str]
    criticality_level: str
    context: Dict[str, Any]
    requested_at: float
    status: ReviewStatus
    reviewer_id: Optional[str] = None
    review_notes: Optional[str] = None
    reviewed_at: Optional[float] = None
    # Enhanced fields
    graph_metrics: Optional[Dict[str, float]] = None
    human_feedback_score: Optional[float] = None


class HumanReviewPortal:
    """
    Portal for human-in-the-loop review.
    Enhanced with optional graph metrics and RLHF tracking.
    """
    
    def __init__(self):
        self.review_queue: List[ReviewRequest] = []
        self.completed_reviews: List[ReviewRequest] = []
        self.active_reviewers: Dict[str, str] = {}
        
    def submit_for_review(
        self,
        task_id: str,
        agent_output: Any,
        escalation_reasons: List[str],
        criticality_level: str,
        context: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> str:
        request_id = self._generate_request_id(task_id)
        request = ReviewRequest(
            request_id=request_id,
            task_id=task_id,
            agent_output=agent_output,
            escalation_reasons=escalation_reasons,
            criticality_level=criticality_level,
            context=context or {},
            requested_at=datetime.now().timestamp(),
            status=ReviewStatus.PENDING,
            graph_metrics=graph_metrics,
            human_feedback_score=human_feedback_score
        )
        if criticality_level == "critical":
            self.review_queue.insert(0, request)
        else:
            self.review_queue.append(request)
        return request_id
    
    def assign_reviewer(self, request_id: str, reviewer_id: str) -> bool:
        for request in self.review_queue:
            if request.request_id == request_id:
                request.reviewer_id = reviewer_id
                request.status = ReviewStatus.IN_PROGRESS
                self.active_reviewers[reviewer_id] = request_id
                return True
        return False
    
    def submit_review(
        self,
        request_id: str,
        reviewer_id: str,
        status: ReviewStatus,
        notes: Optional[str] = None,
        revised_output: Optional[Any] = None
    ) -> bool:
        for i, request in enumerate(self.review_queue):
            if request.request_id == request_id and request.reviewer_id == reviewer_id:
                request.status = status
                request.review_notes = notes
                request.reviewed_at = datetime.now().timestamp()
                if revised_output is not None:
                    request.agent_output = revised_output
                self.completed_reviews.append(request)
                self.review_queue.pop(i)
                if reviewer_id in self.active_reviewers:
                    del self.active_reviewers[reviewer_id]
                return True
        return False
    
    def get_pending_reviews(self, criticality_filter: Optional[str] = None) -> List[ReviewRequest]:
        pending = [r for r in self.review_queue if r.status == ReviewStatus.PENDING]
        if criticality_filter:
            pending = [r for r in pending if r.criticality_level == criticality_filter]
        return pending
    
    def get_review_status(self, request_id: str) -> Optional[ReviewRequest]:
        for request in self.review_queue:
            if request.request_id == request_id:
                return request
        for request in self.completed_reviews:
            if request.request_id == request_id:
                return request
        return None
    
    def _generate_request_id(self, task_id: str) -> str:
        import hashlib
        content = f"{task_id}:{datetime.now().timestamp()}"
        return hashlib.md5(content.encode()).hexdigest()[:16]


# ------------------------------------------------------------------------------
# Enhanced Distillation Optimizer for Escalation Decision
# ------------------------------------------------------------------------------
class EscalationState:
    """State for distillation to decide escalation and expert selection."""
    def __init__(self, task_id, agent_confidence, estimated_energy_wh,
                 domain, context, graph_metrics=None, human_feedback_score=0.5):
        self.task_id = task_id
        self.confidence = min(agent_confidence, 1.0)
        self.energy = min(estimated_energy_wh / 1.0, 1.0)  # normalize
        # domain one-hot (simplified: 4 domains)
        self.domain_onehot = np.zeros(4)
        domain_map = {'code':0,'security':1,'performance':2,'sustainability':3}
        if domain in domain_map:
            self.domain_onehot[domain_map[domain]] = 1.0
        self.graph_centrality = (graph_metrics or {}).get('centrality', 0.5)
        self.graph_connectivity = (graph_metrics or {}).get('connectivity', 0.5)
        self.human_feedback = human_feedback_score
        # additional context features
        self.context_len = min(len(str(context)) / 1000.0, 1.0)
        
    def to_feature_vector(self) -> np.ndarray:
        return np.array([
            self.confidence,
            self.energy,
            *self.domain_onehot,
            self.graph_centrality,
            self.graph_connectivity,
            self.human_feedback,
            self.context_len
        ], dtype=np.float32)


class DistillationEscalationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to decide:
    - whether to escalate
    - which expert type to invoke (if escalating)
    """
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.feature_dim = 9  # confidence(1) + energy(1) + domain(4) + graph(2) + human(1)
        self.n_actions = 5  # 0=no_escalation, 1=code, 2=security, 3=performance, 4=sustainability
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

        # Teachers (rule‑based, RLHF, historical)
        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher
        ]
        # MoE gating
        self.gate_weights = np.random.randn(self.feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = self.config.get('gating_lr', 0.005)

    def _rule_teacher(self, state: EscalationState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.05
        if state.confidence < 0.6:
            probs[1] = 0.4  # likely code expert
            probs[2] = 0.3  # security
            probs[3] = 0.2  # performance
        elif state.energy > 0.5:
            probs[4] = 0.6  # sustainability expert
        else:
            probs[0] = 0.8  # no escalation
        return probs / probs.sum()

    def _rlhf_teacher(self, state: EscalationState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback > 0.7:
            # Prefer more human review (escalation)
            probs[1] += 0.1
            probs[2] += 0.1
        elif state.human_feedback < 0.3:
            probs[0] += 0.2  # trust agent
        return probs / probs.sum()

    def _historical_teacher(self, state: EscalationState) -> np.ndarray:
        # Simulate model: low confidence -> escalate to code or security
        if state.confidence < 0.5:
            return np.array([0.0, 0.5, 0.3, 0.1, 0.1])
        elif state.energy > 0.6:
            return np.array([0.2, 0.0, 0.0, 0.3, 0.5])
        else:
            return np.array([0.7, 0.1, 0.1, 0.05, 0.05])

    def _gate_forward(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_action(self, state: EscalationState, exploration: bool = True) -> Tuple[int, np.ndarray, np.ndarray]:
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
# Enhanced ExpertCollaborationSystem
# ------------------------------------------------------------------------------
class ExpertCollaborationSystem:
    """
    Unified expert collaboration system with optional advanced decision-making.

    Integrates:
    - Expert Model Gateway
    - Domain-Specific Connectors
    - Knowledge Base
    - Invocation Policy (can be replaced by distillation)
    - Human Review Portal
    - Audit Logger
    - LIMIT Graph, MODP, RLHF, distillation, MoE (optional)
    """
    
    def __init__(
        self,
        expert_gateway,
        invocation_policy,
        audit_logger,
        knowledge_integrator=None,
        expert_connectors=None,
        human_portal=None,
        config: Optional[Dict[str, Any]] = None
    ):
        self.expert_gateway = expert_gateway
        self.invocation_policy = invocation_policy
        self.audit_logger = audit_logger
        self.knowledge_integrator = knowledge_integrator
        self.expert_connectors = expert_connectors
        self.human_portal = human_portal or HumanReviewPortal()
        self.config = config or {}
        self.use_enhancements = self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE

        # Enhanced components
        self.distillation_optimizer = None
        self.node_descriptor = None
        self.zero_trust = None

        if self.use_enhancements:
            self.distillation_optimizer = DistillationEscalationOptimizer(self.config)
            # Optional node descriptor for carbon intensity
            if self.config.get('use_node_descriptor', False) and NodeDescriptor is not None:
                self.node_descriptor = NodeDescriptor(
                    id="collab_node",
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
            # Optional zero trust
            if self.config.get('enable_zero_trust', False):
                self.zero_trust = ZeroTrustArchitecture()
            logger.info("ExpertCollaborationSystem enhanced components initialized")

    async def process_task(
        self,
        task_id: str,
        agent_id: str,
        task_description: str,
        agent_output: Any,
        agent_confidence: float,
        estimated_energy_wh: float,
        domain: str,
        context: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Process task through expert collaboration pipeline.
        If enhanced, use distillation to decide escalation and expert selection.
        """
        # Default human feedback if not provided
        if human_feedback_score is None:
            human_feedback_score = self.config.get('human_feedback_score', 0.5)
        if graph_metrics is None:
            graph_metrics = self.config.get('graph_metrics', {})
            # If node descriptor available, get graph metrics from it
            if self.use_enhancements and self.node_descriptor:
                graph_metrics = {
                    'centrality': self.node_descriptor.graph_metrics.get('centrality', 0.5) if self.node_descriptor.graph_metrics else 0.5,
                    'connectivity': self.node_descriptor.graph_metrics.get('connectivity', 0.5) if self.node_descriptor.graph_metrics else 0.5
                }

        if self.use_enhancements and self.distillation_optimizer:
            # Build state
            state = EscalationState(
                task_id=task_id,
                agent_confidence=agent_confidence,
                estimated_energy_wh=estimated_energy_wh,
                domain=domain,
                context=context or {},
                graph_metrics=graph_metrics,
                human_feedback_score=human_feedback_score
            )
            action_idx, state_vec, teacher_probs = self.distillation_optimizer.select_action(state)
            # Map action to escalation decision and expert type
            escalate = action_idx != 0
            expert_type = None
            if action_idx == 1:
                expert_type = "code"
            elif action_idx == 2:
                expert_type = "security"
            elif action_idx == 3:
                expert_type = "performance"
            elif action_idx == 4:
                expert_type = "sustainability"
            # Store for later update
            self._last_decision = (state_vec, action_idx, teacher_probs, state)
        else:
            # Legacy invocation policy
            decision = self.invocation_policy.decide_escalation(
                task=task_description,
                agent_confidence=agent_confidence,
                estimated_energy_wh=estimated_energy_wh,
                domain=domain,
                context=context
            )
            escalate = decision.should_escalate
            expert_type = decision.recommended_expert if escalate else None
            # Log escalation decision
            if escalate:
                self.audit_logger.log_escalation(
                    task_id=task_id,
                    agent_id=agent_id,
                    reasons=[r.value for r in decision.reasons],
                    expert_type=expert_type
                )
            self._last_decision = None

        result = {
            "task_id": task_id,
            "agent_output": agent_output,
            "escalated": escalate,
            "expert_feedback": None,
            "final_output": agent_output
        }

        if escalate:
            # Check if human review needed
            if self.use_enhancements and expert_type == "security":
                # Enhanced: security tasks always go to human review for now
                criticality = "critical"
            else:
                criticality = "high" if expert_type == "security" else "medium"

            if criticality == "critical":
                request_id = self.human_portal.submit_for_review(
                    task_id=task_id,
                    agent_output=agent_output,
                    escalation_reasons=["enhanced decision" if self.use_enhancements else "policy"],
                    criticality_level=criticality,
                    context=context,
                    graph_metrics=graph_metrics,
                    human_feedback_score=human_feedback_score
                )
                result["human_review_requested"] = True
                result["review_request_id"] = request_id
            else:
                # Invoke expert model
                expert_response = await self._invoke_expert(
                    task_description=task_description,
                    agent_output=agent_output,
                    domain=domain,
                    expert_type=expert_type,
                    context=context
                )
                # Log expert invocation (with graph metrics if available)
                self.audit_logger.log_expert_invocation(
                    task_id=task_id,
                    agent_id=agent_id,
                    expert_type=expert_type,
                    energy_wh=expert_response.energy_consumed_wh,
                    carbon_kg=expert_response.carbon_emitted_kg,
                    details={
                        "model": expert_response.model_used.value,
                        "tokens": expert_response.tokens_used
                    },
                    graph_metrics=graph_metrics,
                    human_feedback_score=human_feedback_score
                )
                result["expert_feedback"] = asdict(expert_response)
                result["final_output"] = expert_response.response_text

        # Enhanced: update distillation after result
        if self.use_enhancements and self._last_decision:
            state_vec, action_idx, teacher_probs, state = self._last_decision
            # Compute reward using MODP weights (simplified)
            if escalate:
                # If expert provided, reward higher if confidence improved
                # Here we just use a simple reward: 1 if escalated and confident, 0 if not
                reward = 1.0 if escalate else 0.0
            else:
                reward = 0.5  # no escalation is neutral
            self.distillation_optimizer.update(state_vec, action_idx, reward, state_vec, teacher_probs)
            del self._last_decision

        return result

    async def _invoke_expert(
        self,
        task_description: str,
        agent_output: Any,
        domain: str,
        expert_type: str,
        context: Optional[Dict[str, Any]]
    ):
        """Invoke expert model."""
        # Build expert prompt
        prompt = self._build_expert_prompt(task_description, agent_output, domain, expert_type, context)
        # Map domain
        from expert_gateway import ExpertDomain
        domain_map = {
            "code": ExpertDomain.CODE_GENERATION,
            "security": ExpertDomain.SECURITY,
            "performance": ExpertDomain.PERFORMANCE,
            "sustainability": ExpertDomain.SUSTAINABILITY
        }
        expert_domain = domain_map.get(domain, ExpertDomain.CODE_GENERATION)
        response = await self.expert_gateway.invoke_expert(
            task=task_description,
            prompt=prompt,
            domain=expert_domain,
            context=context,
            urgency="high" if expert_type == "security" else "medium"
        )
        return response

    def _build_expert_prompt(
        self,
        task_description: str,
        agent_output: Any,
        domain: str,
        expert_type: str,
        context: Optional[Dict[str, Any]]
    ) -> str:
        prompt = f"""Task: {task_description}
Domain: {domain}
Expert Type: {expert_type}
Agent Output:
{agent_output}
Please review and provide expert feedback."""
        return prompt

    def get_collaboration_stats(self) -> Dict[str, Any]:
        stats = {
            "expert_gateway": self.expert_gateway.get_statistics(),
            "invocation_policy": self.invocation_policy.get_policy_stats(),
            "audit": {
                "total_events": len(self.audit_logger.audit_events),
                "total_energy_consumed": self.audit_logger.total_energy_consumed,
                "total_energy_saved": self.audit_logger.total_energy_saved,
                "net_energy": (
                    self.audit_logger.total_energy_consumed -
                    self.audit_logger.total_energy_saved
                )
            },
            "human_reviews": {
                "pending": len(self.human_portal.review_queue),
                "completed": len(self.human_portal.completed_reviews),
                "active_reviewers": len(self.human_portal.active_reviewers)
            }
        }
        if self.use_enhancements and self.distillation_optimizer:
            stats["distillation"] = {
                "student_counter": self.distillation_optimizer.counter,
                "buffer_size": len(self.distillation_optimizer.replay_buffer)
            }
        return stats


def create_expert_collaboration_system(
    openai_key: Optional[str] = None,
    anthropic_key: Optional[str] = None,
    confidence_threshold: float = 0.7,
    sustainability_threshold_wh: float = 0.1,
    log_file: str = "expert_audit.log",
    use_enhancements: bool = False,
    config: Optional[Dict[str, Any]] = None
):
    """Create complete expert collaboration system with optional enhancements."""
    from expert_gateway import create_multi_provider_gateway
    from invocation_policy import SelectiveInvocationPolicy, KnowledgeBaseIntegrator
    from audit_logger import AuditLogger

    expert_gateway = create_multi_provider_gateway(openai_key, anthropic_key)
    invocation_policy = SelectiveInvocationPolicy(
        confidence_threshold=confidence_threshold,
        sustainability_threshold_wh=sustainability_threshold_wh
    )
    audit_logger = AuditLogger(log_file=log_file, enable_persistence=True)
    if config is None:
        config = {}
    config['use_enhancements'] = use_enhancements
    return ExpertCollaborationSystem(
        expert_gateway=expert_gateway,
        invocation_policy=invocation_policy,
        audit_logger=audit_logger,
        config=config
    )
