"""
analytics/expert_collaboration_system.py — Orchestrates the escalation flow.

Wires together:
    * Settings
    * SelectiveInvocationPolicy
    * ExpertGateway
    * AuditLogger
    * HumanReviewPortal (stub here; production version integrates real queue)

Exposes:
    * async process_task(...) — the single entry point
    * get_collaboration_stats() — real metrics for the dashboard
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from analytics.audit_logger import AuditLogger
from policy.invocation_policy import SelectiveInvocationPolicy
from runtime.expert_gateway import ExpertDomain, ExpertGateway
from runtime.settings import Settings

logger = logging.getLogger(__name__)

__all__ = [
    "ReviewStatus", "PendingReview", "HumanReviewPortal",
    "ExpertCollaborationSystem", "create_expert_collaboration_system",
]


class ReviewStatus(str, Enum):
    APPROVED          = "approved"
    REJECTED          = "rejected"
    CHANGES_REQUESTED = "changes_requested"


@dataclass
class PendingReview:
    request_id: str
    task_id: str
    assigned_reviewer_id: str
    created_at_iso: str
    criticality_level: str


@dataclass
class _ReviewRequest:
    request_id: str
    task_id: str
    agent_output: Any
    escalation_reasons: list[str]
    criticality_level: str
    assigned_reviewer_id: str = ""
    status: ReviewStatus | None = None
    notes: str = ""
    created_at_iso: str = ""

    def to_pending(self) -> PendingReview:
        return PendingReview(
            request_id=self.request_id,
            task_id=self.task_id,
            assigned_reviewer_id=self.assigned_reviewer_id,
            created_at_iso=self.created_at_iso,
            criticality_level=self.criticality_level,
        )


class HumanReviewPortal:
    """
    In-memory portal. Production replaces `_requests` with a persistent store.
    Methods are synchronous by design (see §1.2).
    """

    def __init__(self, max_queue_size: int = 100) -> None:
        self._requests: dict[str, _ReviewRequest] = {}
        self._max = max_queue_size

    def submit_for_review(
        self,
        *,
        task_id: str,
        agent_output: Any,
        escalation_reasons: list[str],
        criticality_level: str,
        request_id: str,
    ) -> str:
        if len(self._requests) >= self._max:
            raise RuntimeError(
                f"human review queue full ({self._max}); resolve pending reviews"
            )
        self._requests[request_id] = _ReviewRequest(
            request_id=request_id,
            task_id=task_id,
            agent_output=agent_output,
            escalation_reasons=escalation_reasons,
            criticality_level=criticality_level,
            created_at_iso=datetime.now(timezone.utc).isoformat(),
        )
        return request_id

    def assign_reviewer(self, *, request_id: str, reviewer_id: str) -> bool:
        req = self._requests.get(request_id)
        if req is None:
            return False
        req.assigned_reviewer_id = reviewer_id
        return True

    def submit_review(
        self,
        *,
        request_id: str,
        reviewer_id: str,
        status: ReviewStatus,
        notes: str,
    ) -> dict[str, Any] | None:
        req = self._requests.get(request_id)
        if req is None:
            return None
        if req.assigned_reviewer_id and reviewer_id != req.assigned_reviewer_id:
            raise PermissionError(
                f"reviewer {reviewer_id!r} is not assigned to {request_id!r}"
            )
        req.status = status
        req.notes = notes
        return {
            "request_id": req.request_id,
            "status": req.status.value,
            "reviewer_id": reviewer_id,
            "notes": notes,
        }

    def get_pending_reviews(self) -> list[PendingReview]:
        return [
            r.to_pending()
            for r in self._requests.values()
            if r.status is None
        ]


class ExpertCollaborationSystem:
    def __init__(
        self,
        *,
        settings: Settings,
        audit_logger: AuditLogger,
        gateway: ExpertGateway | None = None,
        policy: SelectiveInvocationPolicy | None = None,
        human_portal: HumanReviewPortal | None = None,
    ) -> None:
        self._settings = settings
        self._audit = audit_logger
        self.gateway = gateway or ExpertGateway(settings)
        self.policy = policy or SelectiveInvocationPolicy(settings)
        self.human_portal = human_portal or HumanReviewPortal()

    async def process_task(
        self,
        *,
        task_id: str,
        agent_id: str,
        task_description: str,
        agent_output: Any,
        agent_confidence: float,
        estimated_energy_wh: float,
        domain: str,
        criticality_level: str | None = None,
        graph_metrics: dict[str, float] | None = None,
        human_feedback_score: float | None = None,
    ) -> dict[str, Any]:
        ts = datetime.now(timezone.utc).isoformat()

        decision = self.policy.decide_escalation(
            task=task_description,
            agent_confidence=agent_confidence,
            estimated_energy_wh=estimated_energy_wh,
            domain=domain,
            criticality_level=criticality_level,
            graph_metrics=graph_metrics,
            human_feedback_score=human_feedback_score,
        )

        self._audit.log_escalation_decision(
            task_id=task_id,
            agent_id=agent_id,
            timestamp_iso=ts,
            should_escalate=decision.should_escalate,
            reasons=[r.value for r in decision.reasons],
            domain=domain,
        )

        if not decision.should_escalate:
            return {
                "task_id": task_id,
                "escalated": False,
                "output": agent_output,
                "decision": {
                    "should_escalate": False,
                    "reasons": [],
                    "modp_score": decision.modp_score,
                },
            }

        # Escalate to expert.
        try:
            expert_resp = await self.gateway.invoke_expert(
                task=task_description,
                prompt=str(agent_output),
                domain=self._coerce_domain(domain),
            )
        except Exception as exc:
            self._audit.log_expert_invocation(
                task_id=task_id, agent_id=agent_id, timestamp_iso=ts,
                provider="?", model="?", prompt=str(agent_output),
                input_tokens=0, output_tokens=0, cost_usd=0.0,
                duration_ms=0.0, domain=domain, error=str(exc),
            )
            return {
                "task_id": task_id,
                "escalated": True,
                "error": str(exc),
                "output": agent_output,
                "decision": {
                    "should_escalate": True,
                    "reasons": [r.value for r in decision.reasons],
                    "modp_score": decision.modp_score,
                },
            }

        self._audit.log_expert_invocation(
            task_id=task_id, agent_id=agent_id, timestamp_iso=ts,
            provider=expert_resp.provider, model=expert_resp.model,
            prompt=str(agent_output),
            input_tokens=expert_resp.input_tokens,
            output_tokens=expert_resp.output_tokens,
            cost_usd=expert_resp.cost_usd,
            duration_ms=expert_resp.duration_ms,
            domain=domain,
            redactions=expert_resp.redactions,
            graph_metrics=graph_metrics,
            human_feedback_score=human_feedback_score,
        )

        return {
            "task_id": task_id,
            "escalated": True,
            "output": agent_output,
            "expert_feedback": {
                "provider": expert_resp.provider,
                "model": expert_resp.model,
                "response_text": expert_resp.response_text,
                "cost_usd": expert_resp.cost_usd,
                "duration_ms": expert_resp.duration_ms,
                "redactions": expert_resp.redactions,
            },
            "decision": {
                "should_escalate": True,
                "reasons": [r.value for r in decision.reasons],
                "modp_score": decision.modp_score,
            },
        }

    def get_collaboration_stats(self) -> dict[str, Any]:
        """Real statistics for the dashboard endpoint."""
        report = self._audit.generate_transparency_report(
            period_start_iso="1970-01-01T00:00:00Z",
            period_end_iso=datetime.now(timezone.utc).isoformat(),
        )
        return {
            "escalation_rate": report.escalation_rate,
            "expert_cost_usd": report.expert_cost_usd,
            "energy_wh_total": report.energy_wh_total,
            "carbon_kg_total": report.carbon_kg_total,
            "budget_remaining_usd": self.gateway.budget_remaining_usd,
            "pending_reviews": len(self.human_portal.get_pending_reviews()),
        }

    @staticmethod
    def _coerce_domain(domain: str) -> ExpertDomain:
        try:
            return ExpertDomain(domain)
        except ValueError:
            return ExpertDomain.OTHER


def create_expert_collaboration_system(
    *,
    settings: Settings | None = None,
    audit_logger: AuditLogger | None = None,
    openai_api_key: str | None = None,
    anthropic_api_key: str | None = None,
) -> ExpertCollaborationSystem:
    """
    Convenience factory. Secrets default to `Settings.read_secret()`, never env.
    """
    s = settings or Settings()
    a = audit_logger or AuditLogger(s.audit_log_file)
    gw = ExpertGateway(s)
    if openai_api_key is not None:
        gw.register_openai(openai_api_key)
    if anthropic_api_key is not None:
        gw.register_anthropic(anthropic_api_key)
    return ExpertCollaborationSystem(
        settings=s, audit_logger=a, gateway=gw,
        policy=SelectiveInvocationPolicy(s),
        human_portal=HumanReviewPortal(),
    )
