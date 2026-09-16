"""
StateBuffer — bounded ring buffer for quantum execution state.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Dict, List, Optional


@dataclass
class BufferedEntry:
    """A single buffered state entry."""
    key: str
    value: Any
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "at": self.at.isoformat(),
        }


class StateBuffer:
    """
    Thread-safe bounded ring buffer for transient quantum state.

    Unlike a persistent registry, the buffer discards older entries
    automatically to prevent unbounded growth. This is the correct
    default for coherence-sensitive intermediate state.
    """

    def __init__(
        self,
        *,
        max_entries: int = 1000,
        on_evict: Optional[Callable[[BufferedEntry], None]] = None,
    ):
        self._buffer: Deque[BufferedEntry] = deque(maxlen=max_entries)
        self._lock = threading.RLock()
        self._on_evict = on_evict
        self._evicted_count = 0

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            if (
                self._on_evict is not None
                and len(self._buffer) == self._buffer.maxlen
            ):
                evicted = self._buffer[0]
                self._evicted_count += 1
                try:
                    self._on_evict(evicted)
                except Exception:
                    pass
            self._buffer.append(BufferedEntry(key=key, value=value))

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            for entry in reversed(self._buffer):
                if entry.key == key:
                    return entry.value
        return None

    def get_all(self, key: str) -> List[Any]:
        with self._lock:
            return [e.value for e in self._buffer if e.key == key]

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [e.to_dict() for e in self._buffer]

    def clear(self) -> None:
        with self._lock:
            self._buffer.clear()

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "size": len(self._buffer),
                "max_entries": self._buffer.maxlen,
                "evicted": self._evicted_count,
            }
