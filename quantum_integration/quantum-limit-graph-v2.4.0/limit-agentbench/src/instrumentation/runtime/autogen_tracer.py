# src/instrumentation/runtime/autogen_tracer.py

"""
AutoGen Tracer
==============

Extracts a message graph from an AutoGen conversation.

Enhancements
------------
- :class:`AutoGenTracerConfig` — frozen, fully validated: rejects ``bool``
  and ``NaN`` for numeric fields, validates ``include_preview`` /
  ``length_strategy`` strictly. ``from_dict`` uses **strict boolean
  parsing** — ``"false"`` raises rather than coercing to ``True`` (the
  string-bool trap). Replaces the silent ``bool(...)`` / ``int(...)`` /
  ``str(...)`` coercions.
- :class:`MessageEdge` — frozen; validates ``timestamp`` (finite,
  non-negative), ``message_length`` and ``edge_index`` (non-negative
  int, ``bool`` rejected), and ``preview`` (``str`` or ``None``,
  truncated to ``MAX_PREVIEW_CHARS = 4096`` as a hard safety ceiling).
  ``from_dict`` no longer coerces ``None`` to ``"None"`` — non-string
  ``sender`` / ``receiver`` / ``preview`` raise.
- **Strict-bool parsing** throughout ``from_dict`` — a JSON payload with
  ``"include_preview": "false"`` raises rather than silently enabling
  preview retention (which would be a PII leak).
- **`from_dict` truncation fix** — when restoring more edges than
  ``config.max_edges`` allows, a warning is logged and ``_edge_counter``
  is adjusted so future ``record()`` calls don't collide with restored
  indices.
- **`length_extract_failures` counter** — ``_extract_length`` failures
  (broken ``__len__`` / ``__repr__``) now increment a counter and log at
  ``warning`` instead of ``debug``; callers can monitor the counter to
  detect a bad message source.
- **`last_edge` property**, **`get_edges_since(ts)` filter**, and
  **per-agent statistics** in ``statistics()``.
- Documented caveats: ``length(edge) == 3`` (legacy tuple shape, not
  field count) and ``length_strategy="auto"`` behaviour for ``bytes``.
- ``__main__`` smoke test extended with strict-bool regression,
  ``None``-sender rejection, and index-collision cases.

Modeling notes
--------------
* ``length(edge)`` returns 3 to preserve the legacy tuple-unpacking
  interface. The dataclass has six fields; use ``asdict(edge)`` or the
  field names for the full record.
* ``length_strategy="auto"`` preserves the original behaviour:
  ``bytes`` are counted as bytes; ``str`` as characters; everything
  else via ``len(str(...))``. Use ``"str"`` for uniform character
  counts, or ``"bytes"`` for uniform UTF-8 byte counts. For a
  ``bytes`` payload, ``auto`` and ``str`` produce different numbers.
* ``MessageEdge.preview`` is capped at ``MAX_PREVIEW_CHARS`` (4096) as
  a hard safety ceiling — independent of ``AutoGenTracerConfig.preview_max_chars``,
  which is applied earlier and is normally smaller.
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import threading
import time
from collections import Counter, deque
from dataclasses import asdict, dataclass, field
from typing import (
    Any,
    Deque,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)

__all__ = [
    "AutoGenTracer",
    "AutoGenTracerConfig",
    "AutoGenTracerError",
    "MessageEdge",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AutoGenTracerError(ValueError):
    """Raised for invalid tracer inputs or configuration."""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _require_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AutoGenTracerError(f"{label} must be an int, not bool")
    if value <= 0:
        raise AutoGenTracerError(f"{label} must be > 0")
    return value


def _require_non_negative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AutoGenTracerError(f"{label} must be an int, not bool")
    if value < 0:
        raise AutoGenTracerError(f"{label} must be >= 0")
    return value


def _require_non_negative_finite_float(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise AutoGenTracerError(f"{label} must be a number, not bool")
    fv = float(value)
    if not math.isfinite(fv):
        raise AutoGenTracerError(f"{label} must be finite")
    if fv < 0:
        raise AutoGenTracerError(f"{label} must be >= 0")
    return fv


def _require_bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise AutoGenTracerError(
            f"{label} must be a bool, got {type(value).__name__}"
        )
    return value


def _strict_bool_from_dict(
    value: Any, label: str, default: bool,
) -> bool:
    """Parse a boolean config field strictly — no ``bool("false")`` trap."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raise AutoGenTracerError(
        f"{label} must be a bool (True/False) or omitted, got "
        f"{type(value).__name__}: {value!r}"
    )


def _strict_int_from_dict(
    value: Any, label: str, default: int,
) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        raise AutoGenTracerError(
            f"{label} must be an int, not bool"
        )
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise AutoGenTracerError(
            f"{label} invalid: {exc}"
        ) from exc


def _strict_str_from_dict(
    value: Any, label: str, default: str,
) -> str:
    if value is None:
        return default
    if not isinstance(value, str):
        raise AutoGenTracerError(
            f"{label} must be a string, got {type(value).__name__}"
        )
    return value


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AutoGenTracerConfig:
    """Tunable parameters for the AutoGen tracer."""

    max_edges: int = 10_000
    include_preview: bool = False
    preview_max_chars: int = 128
    length_strategy: str = "auto"

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "max_edges",
            _require_positive_int(self.max_edges, "max_edges"),
        )
        object.__setattr__(
            self, "preview_max_chars",
            _require_positive_int(
                self.preview_max_chars, "preview_max_chars",
            ),
        )
        object.__setattr__(
            self, "include_preview",
            _require_bool(self.include_preview, "include_preview"),
        )
        if self.length_strategy not in ("auto", "str", "bytes"):
            raise AutoGenTracerError(
                "length_strategy must be 'auto', 'str', or 'bytes'"
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "AutoGenTracerConfig":
        if not isinstance(data, Mapping):
            raise AutoGenTracerError(
                f"AutoGenTracerConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}"
            )
        return cls(
            max_edges=_strict_int_from_dict(
                data.get("max_edges"), "max_edges", 10_000,
            ),
            include_preview=_strict_bool_from_dict(
                data.get("include_preview"),
                "include_preview", False,
            ),
            preview_max_chars=_strict_int_from_dict(
                data.get("preview_max_chars"),
                "preview_max_chars", 128,
            ),
            length_strategy=_strict_str_from_dict(
                data.get("length_strategy"),
                "length_strategy", "auto",
            ),
        )


# --------------------------------------------------------------------------- #
# Edge
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MessageEdge:
    """
    Immutable record of a single message between two agents.

    Unpacks as ``sender, receiver, message_length`` so existing callers
    that iterate over ``get_graph()`` output continue to work.

    ``preview`` is truncated to ``MAX_PREVIEW_CHARS`` (4096) as a hard
    safety ceiling independent of the tracer's ``preview_max_chars``.
    """

    MAX_PREVIEW_CHARS: int = 4096  # class-level safety ceiling

    sender: str
    receiver: str
    message_length: int
    timestamp: float = field(default_factory=time.time)
    edge_index: int = 0
    preview: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.sender, str) or not self.sender:
            raise AutoGenTracerError(
                "sender must be a non-empty string"
            )
        if not isinstance(self.receiver, str) or not self.receiver:
            raise AutoGenTracerError(
                "receiver must be a non-empty string"
            )
        object.__setattr__(
            self, "message_length",
            _require_non_negative_int(
                self.message_length, "message_length",
            ),
        )
        object.__setattr__(
            self, "edge_index",
            _require_non_negative_int(
                self.edge_index, "edge_index",
            ),
        )
        object.__setattr__(
            self, "timestamp",
            _require_non_negative_finite_float(
                self.timestamp, "timestamp",
            ),
        )
        if self.preview is not None:
            if not isinstance(self.preview, str):
                raise AutoGenTracerError(
                    f"preview must be a string or None, got "
                    f"{type(self.preview).__name__}"
                )
            if len(self.preview) > self.MAX_PREVIEW_CHARS:
                truncated = self.preview[: self.MAX_PREVIEW_CHARS]
                logger.warning(
                    "MessageEdge preview truncated from %d to %d chars",
                    len(self.preview), self.MAX_PREVIEW_CHARS,
                )
                object.__setattr__(self, "preview", truncated)

    # -- Tuple-style unpacking ------------------------------------------
    def __iter__(self) -> Iterator[Any]:
        """Yield the legacy 3-tuple: (sender, receiver, message_length)."""
        return iter((
            self.sender, self.receiver, self.message_length,
        ))

    def __len__(self) -> int:
        """Legacy 3-tuple length — NOT the dataclass field count."""
        return 3

    def __getitem__(self, idx: int) -> Any:
        return (
            self.sender, self.receiver, self.message_length,
        )[idx]

    def to_tuple(self) -> Tuple[str, str, int]:
        return (self.sender, self.receiver, self.message_length)

    # -- Serialization --------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "sender": self.sender,
            "receiver": self.receiver,
            "message_length": self.message_length,
            "timestamp": self.timestamp,
            "edge_index": self.edge_index,
            "preview": self.preview,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MessageEdge":
        if not isinstance(data, Mapping):
            raise AutoGenTracerError(
                f"MessageEdge.from_dict expects a Mapping, got "
                f"{type(data).__name__}"
            )

        for required in ("sender", "receiver", "message_length"):
            if required not in data:
                raise AutoGenTracerError(
                    f"MessageEdge.from_dict missing field {required!r}"
                )

        sender = data["sender"]
        if not isinstance(sender, str) or not sender:
            raise AutoGenTracerError(
                f"sender must be a non-empty string, got "
                f"{type(sender).__name__}: {sender!r}"
            )

        receiver = data["receiver"]
        if not isinstance(receiver, str) or not receiver:
            raise AutoGenTracerError(
                f"receiver must be a non-empty string, got "
                f"{type(receiver).__name__}: {receiver!r}"
            )

        try:
            message_length = int(data["message_length"])
        except (TypeError, ValueError) as exc:
            raise AutoGenTracerError(
                f"message_length invalid: {exc}"
            ) from exc

        raw_ts = data.get("timestamp")
        if raw_ts is None:
            timestamp = time.time()
        else:
            if isinstance(raw_ts, bool) or not isinstance(
                raw_ts, numbers.Real
            ):
                raise AutoGenTracerError(
                    f"timestamp must be a number, got "
                    f"{type(raw_ts).__name__}"
                )
            timestamp = float(raw_ts)

        try:
            edge_index = int(data.get("edge_index", 0))
        except (TypeError, ValueError) as exc:
            raise AutoGenTracerError(
                f"edge_index invalid: {exc}"
            ) from exc

        preview = data.get("preview")
        if preview is not None and not isinstance(preview, str):
            raise AutoGenTracerError(
                f"preview must be a string or None, got "
                f"{type(preview).__name__}"
            )

        return cls(
            sender=sender,
            receiver=receiver,
            message_length=message_length,
            timestamp=timestamp,
            edge_index=edge_index,
            preview=preview,
        )

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "MessageEdge":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise AutoGenTracerError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        preview = (
            f", preview={self.preview!r}" if self.preview else ""
        )
        return (
            "MessageEdge("
            f"{self.sender!r} -> {self.receiver!r}, "
            f"len={self.message_length}, idx={self.edge_index}"
            f"{preview})"
        )


# --------------------------------------------------------------------------- #
# Tracer
# --------------------------------------------------------------------------- #
class AutoGenTracer:
    """
    Extracts a message graph from an AutoGen conversation.

    Thread-safe, bounded in memory, and serializable. The original public
    API (``record(sender, receiver, message)`` and ``get_graph()``) is
    preserved.
    """

    def __init__(
        self,
        *,
        config: Optional[AutoGenTracerConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None and not isinstance(
            config, AutoGenTracerConfig
        ):
            raise AutoGenTracerError(
                "config must be an AutoGenTracerConfig instance"
            )
        self._config = config or AutoGenTracerConfig()
        self._strict = _require_bool(strict, "strict")
        self._lock = threading.RLock()

        self._edges: Deque[MessageEdge] = deque(
            maxlen=self._config.max_edges,
        )
        self._edge_counter: int = 0
        self._started_at: float = time.time()
        self._length_extract_failures: int = 0

        logger.debug(
            "AutoGenTracer initialized (max_edges=%d, "
            "include_preview=%s, length_strategy=%s, strict=%s)",
            self._config.max_edges,
            self._config.include_preview,
            self._config.length_strategy,
            self._strict,
        )

    # ---------------------------------------------------------- props
    @property
    def config(self) -> AutoGenTracerConfig:
        return self._config

    @property
    def edge_count(self) -> int:
        with self._lock:
            return len(self._edges)

    @property
    def edges(self) -> List[Tuple[str, str, int]]:
        """Backward-compatible view as a list of 3-tuples (a copy)."""
        with self._lock:
            return [e.to_tuple() for e in self._edges]

    @property
    def last_edge(self) -> Optional[MessageEdge]:
        """The most recently recorded edge, or ``None`` if empty."""
        with self._lock:
            if not self._edges:
                return None
            return self._edges[-1]

    @property
    def length_extract_failures(self) -> int:
        """Number of length-extraction failures since construction."""
        return self._length_extract_failures

    # ---------------------------------------------------------- recording
    def record(
        self, sender: Any, receiver: Any, message: Any,
    ) -> Optional[MessageEdge]:
        """Record one message between two agents."""
        if not isinstance(sender, str) or not sender:
            msg = (
                f"sender must be a non-empty string, got "
                f"{type(sender).__name__}: {sender!r}"
            )
            if self._strict:
                raise AutoGenTracerError(msg)
            logger.warning("%s Skipping.", msg)
            return None

        if not isinstance(receiver, str) or not receiver:
            msg = (
                f"receiver must be a non-empty string, got "
                f"{type(receiver).__name__}: {receiver!r}"
            )
            if self._strict:
                raise AutoGenTracerError(msg)
            logger.warning("%s Skipping.", msg)
            return None

        length = self._extract_length(message)
        preview = self._extract_preview(message)

        with self._lock:
            edge = MessageEdge(
                sender=sender,
                receiver=receiver,
                message_length=length,
                timestamp=time.time(),
                edge_index=self._edge_counter,
                preview=preview,
            )
            self._edges.append(edge)
            self._edge_counter += 1

        logger.debug(
            "Recorded edge %d: %s -> %s (length=%d).",
            edge.edge_index, sender, receiver, length,
        )
        return edge

    # ---------------------------------------------------------- extraction
    def _extract_length(self, message: Any) -> int:
        strategy = self._config.length_strategy

        if message is None:
            return 0

        try:
            if strategy == "str":
                return len(str(message))

            if strategy == "bytes":
                if isinstance(message, bytes):
                    return len(message)
                if isinstance(message, str):
                    return len(message.encode("utf-8"))
                return len(str(message).encode("utf-8"))

            # strategy == "auto"
            if hasattr(message, "__len__"):
                return max(0, int(len(message)))
            return len(str(message))
        except Exception as exc:  # noqa: BLE001
            # Log at warning; increment counter so callers can detect it.
            self._length_extract_failures += 1
            logger.warning(
                "Length extraction failed for %s: %s "
                "(failure count=%d)",
                type(message).__name__,
                exc,
                self._length_extract_failures,
            )
            return 0

    def _extract_preview(self, message: Any) -> Optional[str]:
        if not self._config.include_preview:
            return None
        try:
            text = (
                message
                if isinstance(message, str)
                else str(message)
            )
        except Exception:  # pragma: no cover — defensive
            text = ""
        return text[: self._config.preview_max_chars]

    # ---------------------------------------------------------- views
    def get_graph(self) -> List[Tuple[str, str, int]]:
        """Original ``[(sender, receiver, length), ...]`` shape (a copy)."""
        return self.edges

    def get_edges(self) -> List[MessageEdge]:
        """Richer view — list of :class:`MessageEdge`."""
        with self._lock:
            return list(self._edges)

    def get_edges_since(self, ts: float) -> List[MessageEdge]:
        """Return edges with ``timestamp >= ts`` (a copy)."""
        ts = _require_non_negative_finite_float(ts, "ts")
        with self._lock:
            return [e for e in self._edges if e.timestamp >= ts]

    def to_edge_list(self) -> List[Tuple[str, str, int]]:
        return self.edges

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            edges = list(self._edges)

        if not edges:
            return {
                "edges": 0,
                "unique_agents": 0,
                "unique_senders": 0,
                "unique_receivers": 0,
                "unique_pairs": 0,
                "total_characters": 0,
                "mean_message_length": None,
                "min_message_length": None,
                "max_message_length": None,
                "top_senders": [],
                "top_receivers": [],
                "per_agent": {},
                "length_extract_failures":
                    self._length_extract_failures,
            }

        senders = Counter(e.sender for e in edges)
        receivers = Counter(e.receiver for e in edges)
        agents = set(senders) | set(receivers)
        pairs = {(e.sender, e.receiver) for e in edges}
        lengths = [e.message_length for e in edges]
        total_chars = sum(lengths)

        # Per-agent statistics.
        per_agent: Dict[str, Dict[str, Any]] = {}
        for name in agents:
            sent = [e.message_length for e in edges if e.sender == name]
            recv = [
                e.message_length for e in edges if e.receiver == name
            ]
            per_agent[name] = {
                "as_sender": len(sent),
                "as_receiver": len(recv),
                "mean_sent_length": (
                    sum(sent) / len(sent) if sent else None
                ),
                "mean_received_length": (
                    sum(recv) / len(recv) if recv else None
                ),
            }

        return {
            "edges": len(edges),
            "unique_agents": len(agents),
            "unique_senders": len(senders),
            "unique_receivers": len(receivers),
            "unique_pairs": len(pairs),
            "total_characters": total_chars,
            "mean_message_length": total_chars / len(edges),
            "min_message_length": min(lengths),
            "max_message_length": max(lengths),
            "top_senders": senders.most_common(5),
            "top_receivers": receivers.most_common(5),
            "per_agent": per_agent,
            "length_extract_failures":
                self._length_extract_failures,
        }

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_edges: bool = True) -> int:
        with self._lock:
            removed = len(self._edges)
            if clear_edges:
                self._edges.clear()
            self._edge_counter = 0
            self._started_at = time.time()
            self._length_extract_failures = 0
        logger.debug(
            "AutoGenTracer reset (removed %d edge(s)).", removed,
        )
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "started_at": self._started_at,
                "edge_counter": self._edge_counter,
                "length_extract_failures":
                    self._length_extract_failures,
                "edges": [e.to_dict() for e in self._edges],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AutoGenTracer":
        if not isinstance(data, Mapping):
            raise AutoGenTracerError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}"
            )

        cfg_data = data.get("config") or {}
        if not isinstance(cfg_data, Mapping):
            raise AutoGenTracerError(
                f"config must be a Mapping, got "
                f"{type(cfg_data).__name__}"
            )
        cfg = AutoGenTracerConfig.from_dict(cfg_data)

        strict = _strict_bool_from_dict(
            data.get("strict"), "strict", True,
        )
        tracer = cls(config=cfg, strict=strict)

        raw_edges = data.get("edges") or []
        if not isinstance(raw_edges, list):
            raise AutoGenTracerError(
                f"edges must be a list, got "
                f"{type(raw_edges).__name__}"
            )

        with tracer._lock:
            for entry in raw_edges:
                tracer._edges.append(MessageEdge.from_dict(entry))

            # Index collision guard: if the saved edges exceed max_edges,
            # the deque silently evicted some; adjust counter to avoid
            # collision with future records.
            saved_counter = _strict_int_from_dict(
                data.get("edge_counter"),
                "edge_counter",
                len(tracer._edges),
            )
            max_restored_index = max(
                (e.edge_index for e in tracer._edges), default=-1,
            )
            new_counter = max(saved_counter, max_restored_index + 1)
            if new_counter != saved_counter:
                logger.warning(
                    "from_dict: edge_counter adjusted from %d to %d "
                    "to avoid collision with restored edge_indices",
                    saved_counter, new_counter,
                )
            tracer._edge_counter = new_counter

            tracer._started_at = _require_non_negative_finite_float(
                data.get("started_at", time.time()), "started_at",
            )

            tracer._length_extract_failures = _strict_int_from_dict(
                data.get("length_extract_failures"),
                "length_extract_failures",
                0,
            )

        return tracer

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "AutoGenTracer":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise AutoGenTracerError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.edge_count

    def __iter__(self) -> Iterator[Tuple[str, str, int]]:
        return iter(self.edges)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "AutoGenTracer("
                f"edges={len(self._edges)}, "
                f"max_edges={self._config.max_edges}, "
                f"preview={self._config.include_preview}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # Happy path
    tracer = AutoGenTracer()
    print("repr       :", tracer)
    tracer.record("user_proxy", "assistant", "Hello.")
    tracer.record("assistant", "code_executor", "Executing code...")
    tracer.record("code_executor", "assistant", "Result: 42")

    print("graph      :", tracer.get_graph())
    for sender, receiver, length in tracer.get_graph():
        print(f"  {sender:14s} -> {receiver:14s} (len={length})")

    # Tuple unpacking
    first_edge = tracer.get_edges()[0]
    assert tuple(first_edge) == first_edge.to_tuple()
    print("first edge :", first_edge)

    # last_edge
    assert tracer.last_edge is not None
    print("last edge  :", tracer.last_edge)

    # Shapes
    shapes = AutoGenTracer(strict=False)
    shapes.record("a", "b", "a string message")
    shapes.record("a", "b", b"a bytes message")
    shapes.record("a", "b", ["list", "of", "items"])
    shapes.record("a", "b", {"key": "value"})
    shapes.record("a", "b", None)
    shapes.record("a", "b", 12345)
    shapes.record("a", "b", (i for i in range(5)))
    print("shapes     :", shapes.get_graph())

    # Preview
    preview = AutoGenTracer(
        config=AutoGenTracerConfig(
            include_preview=True, preview_max_chars=20,
        ),
    )
    preview.record(
        "user", "agent",
        "This is a long message that will be truncated.",
    )
    edge = preview.get_edges()[0]
    assert len(edge.preview) == 20
    print("preview    :", edge.preview)

    # Bounded history
    bounded = AutoGenTracer(
        config=AutoGenTracerConfig(max_edges=5),
    )
    for i in range(20):
        bounded.record("a", "b", f"message {i}")
    assert bounded.edge_count == 5
    print(
        "bounded    :", bounded.edge_count,
        "indices:", [e.edge_index for e in bounded.get_edges()],
    )

    # Statistics
    print("stats      :", tracer.statistics())

    # Time-range filter
    now = time.time()
    assert len(tracer.get_edges_since(now - 1.0)) == tracer.edge_count
    assert tracer.get_edges_since(now + 100) == []

    # Serialization
    payload = tracer.to_json()
    restored = AutoGenTracer.from_json(payload)
    assert restored.to_dict() == tracer.to_dict()
    print("Round-trip OK.")

    # Reset
    removed = tracer.reset()
    print(
        "reset      :", removed, "edge(s) removed; new count:",
        tracer.edge_count,
    )

    # Strict-bool trap regression: "false" must NOT enable preview.
    try:
        AutoGenTracerConfig.from_dict(
            {"include_preview": "false"}
        )
    except AutoGenTracerError as exc:
        print("string-bool:", exc)
    else:
        raise AssertionError(
            '"include_preview": "false" must raise'
        )

    # None sender in from_dict must raise (not become "None").
    try:
        MessageEdge.from_dict({
            "sender": None,
            "receiver": "r",
            "message_length": 1,
        })
    except AutoGenTracerError as exc:
        print("null sender:", exc)
    else:
        raise AssertionError("null sender must raise")

    # Index-collision guard on restore.
    big_tracer = AutoGenTracer(
        config=AutoGenTracerConfig(max_edges=3),
    )
    for i in range(10):
        big_tracer.record("a", "b", f"msg {i}")
    small = AutoGenTracer.from_dict(big_tracer.to_dict())
    # The saved edge_counter was 10; restoring into max_edges=3 keeps
    # 3 edges (7..9) but the counter must not collide.
    small.record("a", "b", "new")
    indices = [e.edge_index for e in small.get_edges()]
    assert len(set(indices)) == len(indices), indices
    print("restore idx:", indices)

    # strict must be bool
    try:
        AutoGenTracer(strict="yes")  # type: ignore[arg-type]
    except AutoGenTracerError as exc:
        print("strict     :", exc)

    # Config validation
    bad_configs = (
        dict(max_edges=0),
        dict(max_edges=True),
        dict(preview_max_chars=0),
        dict(preview_max_chars=True),
        dict(include_preview="yes"),
        dict(include_preview=1),
        dict(length_strategy="bogus"),
    )
    for bad in bad_configs:
        try:
            AutoGenTracerConfig(**bad)  # type: ignore[arg-type]
        except AutoGenTracerError as exc:
            print("Rejected   :", exc)
        else:
            raise AssertionError(
                f"config should have rejected {bad}"
            )

    # Bool message_length rejected
    try:
        MessageEdge(
            sender="a", receiver="b", message_length=True,  # type: ignore[arg-type]
        )
    except AutoGenTracerError as exc:
        print("bool len   :", exc)

    # NaN timestamp rejected
    try:
        MessageEdge(
            sender="a", receiver="b", message_length=1,
            timestamp=float("nan"),
        )
    except AutoGenTracerError as exc:
        print("nan ts     :", exc)

    # Preview ceiling on direct construction
    huge = MessageEdge(
        sender="a", receiver="b", message_length=1,
        preview="x" * 10_000,
    )
    assert len(huge.preview) == MessageEdge.MAX_PREVIEW_CHARS
    print("preview cap:", len(huge.preview))

    # Alternative length strategies
    str_len = AutoGenTracer(
        config=AutoGenTracerConfig(length_strategy="str"),
    )
    str_len.record("a", "b", {"k1": 1, "k2": 2})
    print(
        "str length :",
        str_len.get_edges()[0].message_length,
    )

    byte_len = AutoGenTracer(
        config=AutoGenTracerConfig(length_strategy="bytes"),
    )
    byte_len.record("a", "b", "héllo")
    print(
        "byte length:",
        byte_len.get_edges()[0].message_length,
        "(UTF-8 bytes; 'héllo' = 6 bytes)",
    )

    print("\nSmoke test passed.")
