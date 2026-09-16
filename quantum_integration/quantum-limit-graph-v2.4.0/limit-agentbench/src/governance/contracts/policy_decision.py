"""
PolicyDecision — the essential contract.

Implements the recommendation's exact dataclass:
    request_id, verdict, policy_version, reasons, obligations,
    risk_level, verification_status, human_approval_required,
    audit_event_id
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class DecisionVerdict(str, Enum):
    """The five possible verdicts a policy decision may carry."""
    ALLOW = "allow"
    DENY = "deny"
    MODIFY = "modify"
    ESCALATE = "escalate"
    FALLBACK = "fallback"


@dataclass
class PolicyDecision:
    """
    Structured verdict for a single proposed action.

    Every runtime action must obtain a PolicyDecision from the
    PolicyDecisionPoint before execution. The decision carries:
        - The verdict (allow / deny / modify / escalate / fallback)
        - The policy version under which it was issued
        - Human-readable reasons
        - Machine-actionable obligations
        - Risk level
        - Verification status
        - Whether human approval is required
        - A reference to the immutable audit event
    """
    request_id: str
    verdict: str
    policy_version: str
    reasons: List[str] = field(default_factory=list)
    obligations: List[str] = field(default_factory=list)
    risk_level: str = "low"
    verification_status: str = "not_required"
    human_approval_required: bool = False
    audit_event_id: str = ""
    modified_request: Optional[Dict[str, Any]] = None
    issued_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def is_allowed(self) -> bool:
        return self.verdict == DecisionVerdict.ALLOW.value

    def is_blocked(self) -> bool:
        return self.verdict == DecisionVerdict.DENY.value

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["issued_at"] = self.issued_at.isoformat()
        return out

    @property
    def obligations_set(self) -> set:
        """Convenience: obligations as a set for membership tests."""
        return set(self.obligations)
