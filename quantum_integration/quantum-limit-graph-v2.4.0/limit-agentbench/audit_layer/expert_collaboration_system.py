# -*- coding: utf-8 -*-
"""
Human-in-the-Loop Portal & Expert Collaboration System Integration

Provides interface for human reviewers and integrates all expert collaboration
components into a unified system.
"""

from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import asyncio
from datetime import datetime


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


class HumanReviewPortal:
    """
    Portal for human-in-the-loop review.
    
    Features:
    - Task queue management
    - Reviewer assignment
    - Review submission
    - Status tracking
    - Notification system
    """
    
    def __init__(self):
        self.review_queue: List[ReviewRequest] = []
        self.completed_reviews: List[ReviewRequest] = []
        self.active_reviewers: Dict[str, str] = {}  # reviewer_id -> current_task_id
        
    def submit_for_review(
        self,
        task_id: str,
        agent_output: Any,
        escalation_reasons: List[str],
        criticality_level: str,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Submit task for human review.
        
        Args:
            task_id: Task identifier
            agent_output: Agent's output to review
            escalation_reasons: Reasons for escalation
            criticality_level: Criticality level
            context: Additional context
            
        Returns:
            Review request ID
        """
        request_id = self._generate_request_id(task_id)
        
        request = ReviewRequest(
            request_id=request_id,
            task_id=task_id,
            agent_output=agent_output,
            escalation_reasons=escalation_reasons,
            criticality_level=criticality_level,
            context=context or {},
            requested_at=datetime.now().timestamp(),
            status=ReviewStatus.PENDING
        )
        
        # Add to queue (prioritize by criticality)
        if criticality_level == "critical":
            self.review_queue.insert(0, request)
        else:
            self.review_queue.append(request)
        
        return request_id
    
    def assign_reviewer(
        self,
        request_id: str,
        reviewer_id: str
    ) -> bool:
        """Assign reviewer to request."""
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
        """Submit review result."""
        for i, request in enumerate(self.review_queue):
            if request.request_id == request_id and request.reviewer_id == reviewer_id:
                request.status = status
                request.review_notes = notes
                request.reviewed_at = datetime.now().timestamp()
                
                if revised_output is not None:
                    request.agent_output = revised_output
                
                # Move to completed
                self.completed_reviews.append(request)
                self.review_queue.pop(i)
                
                # Clear active reviewer
                if reviewer_id in self.active_reviewers:
                    del self.active_reviewers[reviewer_id]
                
                return True
        
        return False
    
    def get_pending_reviews(
        self,
        criticality_filter: Optional[str] = None
    ) -> List[ReviewRequest]:
        """Get pending reviews."""
        pending = [r for r in self.review_queue if r.status == ReviewStatus.PENDING]
        
        if criticality_filter:
            pending = [r for r in pending if r.criticality_level == criticality_filter]
        
        return pending
    
    def get_review_status(self, request_id: str) -> Optional[ReviewRequest]:
        """Get status of review request."""
        # Check queue
        for request in self.review_queue:
            if request.request_id == request_id:
                return request
        
        # Check completed
        for request in self.completed_reviews:
            if request.request_id == request_id:
                return request
        
        return None
    
    def _generate_request_id(self, task_id: str) -> str:
        """Generate review request ID."""
        import hashlib
        content = f"{task_id}:{datetime.now().timestamp()}"
        return hashlib.md5(content.encode()).hexdigest()[:16]


class ExpertCollaborationSystem:
    """
    Unified expert collaboration system.
    
    Integrates:
    - Expert Model Gateway
    - Domain-Specific Connectors
    - Knowledge Base
    - Invocation Policy
    - Human Review Portal
    - Audit Logger
    """
    
    def __init__(
        self,
        expert_gateway,
        invocation_policy,
        audit_logger,
        knowledge_integrator=None,
        expert_connectors=None,
        human_portal=None
    ):
        self.expert_gateway = expert_gateway
        self.invocation_policy = invocation_policy
        self.audit_logger = audit_logger
        self.knowledge_integrator = knowledge_integrator
        self.expert_connectors = expert_connectors
        self.human_portal = human_portal or HumanReviewPortal()
        
    async def process_task(
        self,
        task_id: str,
        agent_id: str,
        task_description: str,
        agent_output: Any,
        agent_confidence: float,
        estimated_energy_wh: float,
        domain: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process task through expert collaboration pipeline.
        
        Args:
            task_id: Task identifier
            agent_id: Agent identifier
            task_description: Description of task
            agent_output: Agent's initial output
            agent_confidence: Agent's confidence score
            estimated_energy_wh: Estimated energy consumption
            domain: Task domain
            context: Additional context
            
        Returns:
            Result with expert feedback if escalated
        """
        # 1. Check invocation policy
        decision = self.invocation_policy.decide_escalation(
            task=task_description,
            agent_confidence=agent_confidence,
            estimated_energy_wh=estimated_energy_wh,
            domain=domain,
            context=context
        )
        
        # Log escalation decision
        if decision.should_escalate:
            self.audit_logger.log_escalation(
                task_id=task_id,
                agent_id=agent_id,
                reasons=[r.value for r in decision.reasons],
                expert_type=decision.recommended_expert
            )
        
        result = {
            "task_id": task_id,
            "agent_output": agent_output,
            "escalated": decision.should_escalate,
            "decision": asdict(decision),
            "expert_feedback": None,
            "final_output": agent_output
        }
        
        # 2. If escalated, route to appropriate expert
        if decision.should_escalate:
            # Check if human review needed (critical tasks)
            if decision.criticality_level == "critical":
                # Submit for human review
                request_id = self.human_portal.submit_for_review(
                    task_id=task_id,
                    agent_output=agent_output,
                    escalation_reasons=[r.value for r in decision.reasons],
                    criticality_level=decision.criticality_level,
                    context=context
                )
                
                result["human_review_requested"] = True
                result["review_request_id"] = request_id
                
            else:
                # Use expert model
                expert_response = await self._invoke_expert(
                    task_description=task_description,
                    agent_output=agent_output,
                    domain=domain,
                    decision=decision,
                    context=context
                )
                
                # Log expert invocation
                self.audit_logger.log_expert_invocation(
                    task_id=task_id,
                    agent_id=agent_id,
                    expert_type=decision.recommended_expert,
                    energy_wh=expert_response.energy_consumed_wh,
                    carbon_kg=expert_response.carbon_emitted_kg,
                    details={
                        "model": expert_response.model_used.value,
                        "tokens": expert_response.tokens_used
                    }
                )
                
                result["expert_feedback"] = asdict(expert_response)
                result["final_output"] = expert_response.response_text
        
        return result
    
    async def _invoke_expert(
        self,
        task_description: str,
        agent_output: Any,
        domain: str,
        decision,
        context: Optional[Dict[str, Any]]
    ):
        """Invoke expert model."""
        # Build expert prompt
        prompt = self._build_expert_prompt(
            task_description, agent_output, domain, decision, context
        )
        
        # Get domain enum
        from expert_gateway import ExpertDomain
        domain_map = {
            "code": ExpertDomain.CODE_GENERATION,
            "security": ExpertDomain.SECURITY,
            "performance": ExpertDomain.PERFORMANCE,
            "sustainability": ExpertDomain.SUSTAINABILITY
        }
        expert_domain = domain_map.get(domain, ExpertDomain.CODE_GENERATION)
        
        # Invoke expert
        response = await self.expert_gateway.invoke_expert(
            task=task_description,
            prompt=prompt,
            domain=expert_domain,
            context=context,
            urgency="high" if decision.criticality_level == "critical" else "medium"
        )
        
        return response
    
    def _build_expert_prompt(
        self,
        task_description: str,
        agent_output: Any,
        domain: str,
        decision,
        context: Optional[Dict[str, Any]]
    ) -> str:
        """Build prompt for expert model."""
        reasons = ", ".join([r.value for r in decision.reasons])
        
        prompt = f"""Task: {task_description}

Domain: {domain}

Agent Output:
{agent_output}

Escalation Reasons: {reasons}
Criticality: {decision.criticality_level}

Please review the agent's output and provide:
1. Correctness assessment
2. Specific improvements or corrections
3. Security/safety concerns if any
4. Efficiency optimizations if applicable

Your expert review:"""
        
        return prompt
    
    def get_collaboration_stats(self) -> Dict[str, Any]:
        """Get comprehensive collaboration statistics."""
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
        
        return stats


# Convenience function for creating complete system
def create_expert_collaboration_system(
    openai_key: Optional[str] = None,
    anthropic_key: Optional[str] = None,
    confidence_threshold: float = 0.7,
    sustainability_threshold_wh: float = 0.1,
    log_file: str = "expert_audit.log"
):
    """Create complete expert collaboration system."""
    from expert_gateway import create_multi_provider_gateway
    from invocation_policy import SelectiveInvocationPolicy, KnowledgeBaseIntegrator
    from audit_logger import AuditLogger
    
    # Create components
    expert_gateway = create_multi_provider_gateway(
        openai_key=openai_key,
        anthropic_key=anthropic_key
    )
    
    invocation_policy = SelectiveInvocationPolicy(
        confidence_threshold=confidence_threshold,
        sustainability_threshold_wh=sustainability_threshold_wh
    )
    
    audit_logger = AuditLogger(
        log_file=log_file,
        enable_persistence=True
    )
    
    # Create system
    return ExpertCollaborationSystem(
        expert_gateway=expert_gateway,
        invocation_policy=invocation_policy,
        audit_logger=audit_logger
    )
