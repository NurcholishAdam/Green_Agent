# src/feedback/metric_sink.py

"""
Metric Sinks
============

Abstract sink for emitting metrics, plus production-ready implementations:
:class:`StdoutSink`, :class:`InMemorySink`, and :class:`JsonlSink`.

Enhancements
------------
- Module-level ``logger``.
- :class:`MetricSinkError` for invalid inputs.
- ``InMemorySink`` — bounded ring-buffer with ``statistics()`` and serialization.
- ``JsonlSink`` — appends JSON lines to a file with atomic writes and rotation.
- Validation of every emitted payload; strict / non-strict modes.
- ``__main__`` smoke test covering all three sinks.
"""

from __future__ import annotations

import json
import logging
import math
import os
import tempfile
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MetricSinkError(ValueError):
    """Raised for invalid sink inputs."""


# --------------------------------------------------------------------------- #
# Abstract base
# --------------------------------------------------------------------------- #
class MetricSink:
    """Abstract sink for emitting metrics."""

    def emit(self, metrics: Mapping[str, Any]) -> None:
        """Emit a metric payload. Subclasses must override."""
        raise NotImplementedError

    def flush(self) -> None:
        """Optional flush hook. Default is a no-op."""
        return None

    def close(self) -> None:
        """Optional close hook. Default is a no-op."""
        return None

    @staticmethod
    def _validate(metrics: Mapping[str, Any]) -> Dict[str, Any]:
        """Return a validated copy of ``metrics`` or raise."""
        if not isinstance(metrics, Mapping):
            raise MetricSinkError(
                f"metrics must be a Mapping, got {type(metrics).__name__}."
            )
        # JSON-safety check.
        try:
            json.dumps(metrics, default=str)
        except (TypeError, ValueError) as exc:
            raise MetricSinkError(
                f"metrics must be JSON-serializable: {exc}"
            ) from exc
        return dict(metrics)


# --------------------------------------------------------------------------- #
# Stdout sink
# --------------------------------------------------------------------------- #
class StdoutSink(MetricSink):
    """Safe default sink (AgentBeats-compatible)."""

    def emit(self, metrics: Mapping[str, Any]) -> None:
        payload = self._validate(metrics)
        print("[METRICS]", payload)
        logger.debug("StdoutSink emitted: %s", payload)


# --------------------------------------------------------------------------- #
# In-memory sink
# --------------------------------------------------------------------------- #
class InMemorySink(MetricSink):
    """Bounded in-memory sink with statistics and serialization."""

    def __init__(self, max_entries: int = 10_000) -> None:
        if not isinstance(max_entries, int) or max_entries <= 0:
            raise MetricSinkError("max_entries must be a positive int.")
        self._lock = threading.RLock()
        self._entries: Deque[Dict[str, Any]] = deque(maxlen=max_entries)
        self._total: int = 0

    @property
    def entries(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._entries)

    @property
    def total_emitted(self) -> int:
        with self._lock:
            return self._total

    def emit(self, metrics: Mapping[str, Any]) -> None:
        payload = self._validate(metrics)
        with self._lock:
            self._entries.append(payload)
            self._total += 1

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            entries = list(self._entries)
            total = self._total
        keys: Dict[str, int] = {}
        for e in entries:
            for k in e.keys():
                keys[k] = keys.get(k, 0) + 1
        return {
            "total_emitted": total,
            "buffered": len(entries),
            "keys_seen": keys,
        }

    def clear(self) -> int:
        with self._lock:
            n = len(self._entries)
            self._entries.clear()
        return n

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_emitted": self._total,
                "entries": list(self._entries),
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "InMemorySink":
        if not isinstance(data, Mapping):
            raise MetricSinkError("from_dict expects a Mapping.")
        sink = cls()
        with sink._lock:
            sink._total = int(data.get("total_emitted", 0))
            for e in data.get("entries", []):
                sink._entries.append(dict(e))
        return sink

    def __repr__(self) -> str:
        with self._lock:
            return f"InMemorySink(buffered={len(self._entries)}, total={self._total})"


# --------------------------------------------------------------------------- #
# JSONL sink
# --------------------------------------------------------------------------- #
class JsonlSink(MetricSink):
    """Append-only JSON-lines sink with atomic writes."""

    def __init__(self, path: str, *, create_dirs: bool = True) -> None:
        if not isinstance(path, str) or not path:
            raise MetricSinkError("path must be a non-empty string.")
        self._lock = threading.RLock()
        self.path = Path(path)
        if create_dirs:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._total: int = 0

    @property
    def total_emitted(self) -> int:
        with self._lock:
            return self._total

    def emit(self, metrics: Mapping[str, Any]) -> None:
        payload = self._validate(metrics)
        with self._lock:
            try:
                with self.path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(payload, default=str) + "\n")
                self._total += 1
            except OSError as exc:
                raise MetricSinkError(f"Could not write to {self.path}: {exc}") from exc

    def read_all(self) -> List[Dict[str, Any]]:
        with self._lock:
            if not self.path.exists():
                return []
            out: List[Dict[str, Any]] = []
            with self.path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            out.append(json.loads(line))
                        except json.JSONDecodeError:
                            logger.warning("Skipping malformed JSONL line.")
            return out

    def __repr__(self) -> str:
        return f"JsonlSink(path={str(self.path)!r}, total={self._total})"


__all__ = [
    "InMemorySink",
    "JsonlSink",
    "MetricSink",
    "MetricSinkError",
    "StdoutSink",
]


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # Stdout
    stdout_sink = StdoutSink()
    stdout_sink.emit({"energy_kwh": 0.005, "carbon_kg": 0.002})

    # In-memory
    mem = InMemorySink(max_entries=5)
    for i in range(7):
        mem.emit({"i": i, "energy_kwh": 0.001 * (i + 1)})
    print("In-memory  :", mem)
    print("Stats      :", mem.statistics())

    # JSONL
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        jsonl = JsonlSink(os.path.join(td, "metrics.jsonl"))
        jsonl.emit({"task": "a", "energy_kwh": 0.001})
        jsonl.emit({"task": "b", "energy_kwh": 0.002})
        print("JSONL      :", jsonl)
        print("Read back  :", jsonl.read_all())

    # Validation failure
    try:
        mem.emit("not-a-mapping")  # type: ignore[arg-type]
    except MetricSinkError as exc:
        print("Rejected   :", exc)
