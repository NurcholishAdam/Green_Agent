"""
HumanReviewGate — routes decisions to a human for approval.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from ..contracts.policy_decision import PolicyDecision

logger = logging.getLogger(__name__)


@dataclass
class ReviewRequest:
    request_id: str
    action: str
    reasons: List[str] = field(default_factory=list)
    risk_level: str = "low"
    context: Dict[str, Any] = field(default_factory=dict)
    requested_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class HumanReviewGate:
    """
    Gate that enforces human approval for high-risk decisions.

    The gate is callback-driven: register a callback that returns
    True (approve) or False (deny). If no callback is registered, the
    gate is fail-safe: it denies.
    """

    def __init__(self, *, auto_approve_below: Optional[str] = None):
        self._pending: List[ReviewRequest] = []
        self._callback: Optional[Callable[[ReviewRequest], bool]] = None
        self._auto_approve_below = auto_approve_below
        self._feedback: List[Dict[str, Any]] = []

    def set_callback(self, cb: Callable[[ReviewRequest], bool]) -> None:
        self._callback = cb

    async def require(
        self, decision: PolicyDecision, context: Dict[str, Any],
    ) -> bool:
        req = ReviewRequest(
            request_id=decision.request_id,
            action=context.get("action", "unknown"),
            reasons=list(decision.reasons),
            risk_level=decision.risk_level,
            context=dict(context),
        )
        self._pending.append(req)

        # Auto-approve low risk
        if (
            self._auto_approve_below
            and decision.risk_level == self._auto_approve_below
        ):
            self._pending.remove(req)
            self._feedback.append({"req": req.request_id, "decision": True})
            return True

        # Fail-safe: deny if no callback
        if self._callback is None:
            self._pending.remove(req)
            self._feedback.append({"req": req.request_id, "decision": False})
            return False

        try:
            approved = self._callback(req)
            if hasattr(approved, "__await__"):
                approved = await approved
        except Exception as e:
            logger.warning(f"Human review callback failed: {e}")
            approved = False

        self._pending.remove(req)
        self._feedback.append({"req": req.request_id, "decision": approved})
        return bool(approved)
