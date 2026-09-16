"""
AuditLog — immutable, append-only record of governance events.

Every decision, verification, approval, and enforcement action
produces a GovernanceEvent. The log is append-only; there is no
delete or truncate.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

from ..contracts.governance_event import GovernanceEvent

logger = logging.getLogger(__name__)


class AuditLog:
    """
    Thread-safe append-only audit log.

    Each event is linked to its predecessor by a `prev_hash` to form
    a hash chain, so tampering with any event invalidates all
    subsequent hashes.
    """

    def __init__(self, *, sink_path: Optional[str] = None):
        self._events: List[Dict[str, Any]] = []
        self._lock = threading.RLock()
        self._sink_path = sink_path

    def append(self, event: GovernanceEvent) -> str:
        """Append an event; return its chained content hash."""
        with self._lock:
            payload = event.to_dict()
            prev_hash = (
                self._events[-1]["_chain_hash"] if self._events else "genesis"
            )
            payload["_prev_hash"] = prev_hash
            canonical = json.dumps(payload, sort_keys=True, default=str)
            chain_hash = "sha256:" + hashlib.sha256(
                canonical.encode()
            ).hexdigest()
            payload["_chain_hash"] = chain_hash
            self._events.append(payload)

            if self._sink_path:
                try:
                    with open(self._sink_path, "a") as f:
                        f.write(json.dumps(payload, default=str) + "\n")
                except OSError as e:
                    logger.warning(f"AuditLog sink write failed: {e}")

            return chain_hash

    def get(self, event_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            for e in self._events:
                if e.get("event_id") == event_id:
                    return dict(e)
        return None

    def events(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(e) for e in self._events]

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return iter(self.events())

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)

    def verify_chain(self) -> bool:
        """Return True iff the hash chain is intact."""
        with self._lock:
            prev_hash = "genesis"
            for e in self._events:
                if e.get("_prev_hash") != prev_hash:
                    return False
                payload = {k: v for k, v in e.items() if k != "_chain_hash"}
                canonical = json.dumps(payload, sort_keys=True, default=str)
                expected = "sha256:" + hashlib.sha256(
                    canonical.encode()
                ).hexdigest()
                if e.get("_chain_hash") != expected:
                    return False
                prev_hash = e["_chain_hash"]
        return True
