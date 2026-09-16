"""
RollbackController — restore a prior approved state.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class RollbackRecord:
    from_version: str
    to_version: str
    reason: str
    actor: Optional[str] = None
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class RollbackController:
    """
    Roll back to a previously approved state.

    The controller holds a map of state snapshots. Rollback restores
    the snapshot but does not by itself execute actions — the caller
    is responsible for applying the restored state.
    """

    def __init__(self):
        self._snapshots: Dict[str, Any] = {}
        self._history: List[RollbackRecord] = []

    def snapshot(self, version: str, state: Any) -> None:
        self._snapshots[version] = state

    def rollback(
        self,
        from_version: str,
        to_version: str,
        *,
        reason: str,
        actor: Optional[str] = None,
    ) -> Optional[Any]:
        if to_version not in self._snapshots:
            logger.warning(
                f"Rollback target '{to_version}' not found"
            )
            return None
        record = RollbackRecord(
            from_version=from_version,
            to_version=to_version,
            reason=reason,
            actor=actor,
        )
        self._history.append(record)
        logger.info(
            f"Rollback {from_version} → {to_version} ({reason})"
        )
        return self._snapshots[to_version]

    def history(self) -> List[Dict[str, Any]]:
        return [
            {"from": r.from_version, "to": r.to_version,
             "reason": r.reason, "actor": r.actor,
             "at": r.at.isoformat()}
            for r in self._history
        ]
