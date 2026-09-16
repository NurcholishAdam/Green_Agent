"""
FallbackPolicy — safe fallback behavior when actions are not permitted.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..contracts.decision_request import DecisionRequest

logger = logging.getLogger(__name__)


@dataclass
class FallbackAction:
    """A specific fallback action."""
    action: str
    reason: str = ""
    modified_request: Optional[Dict[str, Any]] = None


class FallbackPolicy:
    """
    Chooses a fallback action for a request.

    The policy is a list of fallback rules. The first rule whose
    `matches` predicate accepts the request is selected.
    """

    def __init__(self):
        self._rules: List[tuple] = []

    def add_rule(
        self, matches, action: str, reason: str = "",
    ) -> None:
        self._rules.append((matches, action, reason))

    async def select(self, request: DecisionRequest) -> FallbackAction:
        for matches, action, reason in self._rules:
            try:
                if matches(request):
                    return FallbackAction(
                        action=action,
                        reason=reason,
                        modified_request=_modify(request, action),
                    )
            except Exception as e:
                logger.warning(f"Fallback rule failed: {e}")
        return FallbackAction(
            action="deny",
            reason="no fallback rule matched",
        )


def _modify(request: DecisionRequest, action: str) -> Dict[str, Any]:
    req = request.to_dict()
    req["context"]["fallback_action"] = action
    return req
