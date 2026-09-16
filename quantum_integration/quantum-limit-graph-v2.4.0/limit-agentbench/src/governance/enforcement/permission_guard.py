"""
PermissionGuard — enforce per-actor permissions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class ActorPermissions:
    actor_id: str
    allowed_actions: Set[str] = field(default_factory=set)
    denied_actions: Set[str] = field(default_factory=set)
    max_risk_level: str = "medium"


class PermissionGuard:
    """
    Guard that enforces per-actor permissions.
    """

    RISK_ORDER = ["low", "medium", "high", "critical"]

    def __init__(self):
        self._actors: Dict[str, ActorPermissions] = {}

    def register(self, permissions: ActorPermissions) -> None:
        self._actors[permissions.actor_id] = permissions

    def check(
        self,
        actor_id: str,
        action: str,
        risk_level: str = "low",
    ) -> bool:
        perm = self._actors.get(actor_id)
        if perm is None:
            return False
        if action in perm.denied_actions:
            return False
        if perm.allowed_actions and action not in perm.allowed_actions:
            return False
        try:
            if (
                self.RISK_ORDER.index(risk_level)
                > self.RISK_ORDER.index(perm.max_risk_level)
            ):
                return False
        except ValueError:
            return False
        return True
