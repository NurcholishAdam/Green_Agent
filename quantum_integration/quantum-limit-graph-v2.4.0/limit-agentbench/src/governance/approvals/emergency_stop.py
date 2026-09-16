"""
EmergencyStop — halt all execution immediately.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class StopReason:
    reason: str
    actor: Optional[str] = None
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class EmergencyStop:
    """
    Emergency halt gate. When triggered, all `check()` calls return
    False until `reset()` is called.
    """

    def __init__(self):
        self._stopped = False
        self._reasons: List[StopReason] = []
        self._lock = threading.RLock()

    def trigger(self, reason: str, actor: Optional[str] = None) -> None:
        with self._lock:
            self._stopped = True
            self._reasons.append(StopReason(reason=reason, actor=actor))
            logger.warning(f"EMERGENCY STOP: {reason} (actor={actor})")

    def reset(self) -> None:
        with self._lock:
            self._stopped = False
            logger.info("Emergency stop reset")

    def is_stopped(self) -> bool:
        with self._lock:
            return self._stopped

    def reasons(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {"reason": r.reason, "actor": r.actor,
                 "at": r.at.isoformat()}
                for r in self._reasons
            ]
