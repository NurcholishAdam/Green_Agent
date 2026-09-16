"""
GovernanceEvent — the immutable audit record.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class GovernanceEventKind(str, Enum):
    DECISION_ISSUED = "decision_issued"
    VERIFICATION_COMPLETED = "verification_completed"
    HUMAN_APPROVED = "human_approved"
    HUMAN_DENIED = "human_denied"
    ESCALATION_RAISED = "escalation_raised"
    ROLLBACK_TRIGGERED = "rollback_triggered"
    EMERGENCY_STOP = "emergency_stop"
    POLICY_UPDATED = "policy_updated"
    OBLIGATION_VIOLATED = "obligation_violated"


@dataclass
class GovernanceEvent:
    """
    Immutable audit event. Every decision, verification, approval, and
    enforcement action produces one of these.
    """
    event_id: str
    kind: str
    request_id: Optional[str] = None
    policy_version: Optional[str] = None
    actor: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        return out
