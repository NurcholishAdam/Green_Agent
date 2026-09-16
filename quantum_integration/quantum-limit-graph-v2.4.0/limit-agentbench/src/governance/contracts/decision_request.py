"""
DecisionRequest — the input to the decision point.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class DecisionRequest:
    """
    A proposed action submitted to the PolicyDecisionPoint.

    Every runtime action must present:
        - A unique request_id
        - The action verb (e.g., "deploy_model", "switch_precision")
        - The full context needed by the policies
        - Optional provenance (run_id, policy_version, agent_id)
    """
    request_id: str
    action: str
    agent_id: Optional[str] = None
    task_id: Optional[str] = None
    run_id: Optional[str] = None
    policy_version: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)
    requested_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["requested_at"] = self.requested_at.isoformat()
        return out
