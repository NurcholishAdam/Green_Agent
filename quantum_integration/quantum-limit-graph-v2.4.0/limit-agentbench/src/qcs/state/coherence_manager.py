"""
CoherenceManager — track coherence-like lifecycle state.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class CoherenceWindow:
    """A bounded lifetime window for a quantum operation."""
    window_id: str
    request_id: str
    created_at: float
    ttl_seconds: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_expired(self) -> bool:
        return (time.monotonic() - self.created_at) > self.ttl_seconds

    def remaining_seconds(self) -> float:
        return max(0.0, self.ttl_seconds - (time.monotonic() - self.created_at))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "window_id": self.window_id,
            "request_id": self.request_id,
            "ttl_seconds": self.ttl_seconds,
            "remaining_seconds": self.remaining_seconds(),
            "metadata": dict(self.metadata),
        }


class CoherenceManager:
    """
    Manages coherence windows analogous to qubit decoherence.

    A coherence window bounds how long a request's intermediate state
    is valid. After TTL expiry, the caller must re-submit. This is
    intentionally analogous to quantum decoherence without claiming
    physical equivalence.
    """

    def __init__(self, *, default_ttl_seconds: float = 60.0):
        self.default_ttl_seconds = default_ttl_seconds
        self._windows: Dict[str, CoherenceWindow] = {}

    def open_window(
        self,
        request_id: str,
        *,
        ttl_seconds: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CoherenceWindow:
        window = CoherenceWindow(
            window_id=uuid.uuid4().hex[:12],
            request_id=request_id,
            created_at=time.monotonic(),
            ttl_seconds=ttl_seconds or self.default_ttl_seconds,
            metadata=dict(metadata or {}),
        )
        self._windows[request_id] = window
        logger.debug(f"Coherence window opened for {request_id}")
        return window

    def refresh(self, request_id: str) -> Optional[CoherenceWindow]:
        window = self._windows.get(request_id)
        if window is None or window.is_expired():
            return None
        window.created_at = time.monotonic()
        return window

    def check(self, request_id: str) -> bool:
        """Return True if the window is still valid."""
        window = self._windows.get(request_id)
        if window is None:
            return False
        if window.is_expired():
            self._windows.pop(request_id, None)
            return False
        return True

    def close(self, request_id: str) -> None:
        self._windows.pop(request_id, None)

    def stats(self) -> Dict[str, Any]:
        active = sum(1 for w in self._windows.values() if not w.is_expired())
        return {"active_windows": active, "total_windows": len(self._windows)}
