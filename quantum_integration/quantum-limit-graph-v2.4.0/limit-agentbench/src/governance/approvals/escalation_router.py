"""
EscalationRouter — decides who must approve an escalated decision.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..contracts.policy_decision import PolicyDecision

logger = logging.getLogger(__name__)


@dataclass
class EscalationTarget:
    role: str
    contact: Optional[str] = None
    sla_minutes: Optional[int] = None


class EscalationRouter:
    """
    Route an escalated decision to the appropriate reviewer role.

    Routing rules:
        - LOW       → no escalation
        - MEDIUM    → team_lead
        - HIGH      → safety_officer
        - CRITICAL  → safety_officer + executive (dual control)
    """

    DEFAULT_ROUTING: Dict[str, List[str]] = {
        "low": [],
        "medium": ["team_lead"],
        "high": ["safety_officer"],
        "critical": ["safety_officer", "executive"],
    }

    def __init__(self, routing: Optional[Dict[str, List[str]]] = None):
        self.routing = routing or dict(self.DEFAULT_ROUTING)

    def route(self, decision: PolicyDecision) -> List[EscalationTarget]:
        roles = self.routing.get(decision.risk_level, [])
        return [EscalationTarget(role=r) for r in roles]
