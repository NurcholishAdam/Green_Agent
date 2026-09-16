# src/instrumentation/runtime/autogen_tracer.py

"""
AutoGen Tracer
==============

Extracts a message graph from an AutoGen conversation.

Each ``record()`` call appends a :class:`MessageEdge` describing one message
between two agents. The tracer is designed to be attached to an AutoGen
conversation via a lightweight ``register_reply`` / ``print_received_message``
hook.

Enhancements
------------
- ``AutoGenTracerConfig`` — frozen, validated: bounded edge count, optional
  message preview retention, and a length-extraction strategy.
- ``MessageEdge`` — frozen dataclass with sender, receiver, length, timestamp,
  monotonic edge index, and optional truncated preview. **Supports 3-tuple
  unpacking** so existing ``sender, receiver, length = edge`` code keeps
  working.
- **Bounded history** — ``deque(maxlen=config.max_edges)`` prevents the
  unbounded-growth memory leak of the original.
- **Thread safety** — ``RLock`` guards every mutation; safe under AutoGen's
  default concurrent execution mode.
- **Robust message-length extraction** — handles ``str``, ``bytes``, sized
  containers, unsized objects, generators, and ``None`` without raising.
- **Validation** of ``sender`` / ``receiver`` and of the tracer config;
  strict / non-strict modes.
- ``get_graph()`` returns the original list-of-3-tuples shape; the new
  ``get_edges()`` returns richer :class:`MessageEdge` objects.
- ``statistics()`` — edge count, unique agents, top talkers, total characters,
  message-length distribution.
- ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json`` on the tracer and
  on every dataclass.
- ``reset()`` for conversation reuse.
- Custom ``AutoGenTracerError``; lazy ``%s`` logging; ``__repr__``;
  ``__main__`` smoke test covering every message shape.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import Counter, deque
from dataclasses import asdict, dataclass, field
from typing import (
    Any,
    Deque,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AutoGenTracerError(ValueError):
    """Raised for invalid tracer inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AutoGenTracerConfig:
    """Tunable parameters for the AutoGen tracer."""

    # Maximum number of edges retained. Oldest edges are evicted first.
    max_edges: int = 10_000

    # If True, a truncated copy of the message is retained with each edge.
    # Disabled by default because messages can be large and contain PII.
    include_preview: bool = False

    # Maximum length of the retained preview (characters).
    preview_max_chars: int = 128

    # How to compute ``message_length``:
    #   "auto"   → ``len(message)`` if sized, else ``len(str(message))``
    #              (matches the original behaviour)
    #   "str"    → always ``len(str(message))``
    #   "bytes"  → UTF-8 byte length for ``str``, raw length for ``bytes``
    length_strategy: str = "auto"

    def __post_init__(self) -> None:
        if not isinstance(self.max_edges, int) or self.max_edges <= 0:
            raise AutoGenTracerError("max_edges must be a positive int.")
        if not isinstance(self.preview_max_chars, int) or self.preview_max_chars <= 0:
            raise AutoGenTracerError(
                "preview_max_chars must be a positive int."
            )
        if self.length_strategy not in ("auto", "str", "bytes"):
            raise AutoGenTracerError(
                "length_strategy must be 'auto', 'str', or 'bytes'."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AutoGenTracerConfig":
        if not isinstance(data, Mapping):
            raise AutoGenTracerError(
                f"AutoGenTracerConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            max_edges=int(data.get("max_edges", 10_000)),
            include_preview=bool(data.get("include_preview", False)),
            preview_max_chars=int(data.get("preview_max_chars", 128)),
            length_strategy=str(data.get("length_strategy", "auto")),
        )


# --------------------------------------------------------------------------- #
# Edge
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MessageEdge:
    """
    Immutable record of a single message between two agents.

    Unpacks as ``sender, receiver, message_length`` so existing callers that
    iterate over ``get_graph()`` output continue to work.
    """

    sender: str
    receiver: str
    message_length: int
    timestamp: float = field(default_factory=time.time)
    edge_index: int = 0
    preview: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.sender, str) or not self.sender:
            raise AutoGenTracerError("sender must be a non-empty string.")
        if not isinstance(self.receiver, str) or not self.receiver:
            raise AutoGenTracerError("receiver must be a non-empty string.")
        if not isinstance(self.message_length, int) or self.message_length < 0:
            raise AutoGenTracerError(
                "message_length must be a non-negative int."
            )
        if self.edge_index < 0:
            raise AutoGenTracerError("edge_index must be >= 0.")

    # -- Tuple-style unpacking ------------------------------------------------
    def __iter__(self) -> Iterator[Any]:
        """Yield ``(sender, receiver, message_length)`` — legacy 3-tuple shape."""
        return iter((self.sender, self.receiver, self.message_length))

    def __len__(self) -> int:
        return 3

    def __getitem__(self, idx: int) -> Any:
        return (self.sender, self.receiver, self.message_length)[idx]

    # -- Serialization --------------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_tuple(self) -> Tuple[str, str, int]:
        """Explicit conversion to the original 3-tuple shape."""
        return (self.sender, self.receiver, self.message_length)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MessageEdge":
        if not isinstance(data, Mapping):
            raise AutoGenTracerError(
                f"MessageEdge.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            sender=str(data["sender"]),
            receiver=str(data["receiver"]),
            message_length=int(data["message_length"]),
            timestamp=float(data.get("timestamp", time.time())),
            edge_index=int(data.get("edge_index", 0)),
            preview=data.get("preview"),
        )

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "MessageEdge":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise AutoGenTracerError(f"Invalid JSON payload: {exc}") from exc

    def __repr__(self) -> str:
        preview = f", preview={self.preview!r}" if self.preview else ""
        return (
            "MessageEdge("
            f"{self.sender!r} -> {self.receiver!r}, "
            f"len={self.message_length}, idx={self.edge_index}{preview})"
        )


# --------------------------------------------------------------------------- #
# Tracer
# --------------------------------------------------------------------------- #
class AutoGenTracer:
    """
    Extracts a message graph from an AutoGen conversation.

    Thread-safe, serializable, and bounded in memory. The original public API
    (``record(sender, receiver, message)`` and ``get_graph()``) is preserved;
    new parameters are keyword-only.

    Parameters
    ----------
    config : AutoGenTracerConfig, optional
        Tracer configuration. Defaults to ``AutoGenTracerConfig()``.
    strict : bool, default True
        If True, invalid ``sender`` / ``receiver`` / unsized ``message``
        values raise :class:`AutoGenTracerError`. If False, they are logged
        and coerced to safe defaults.
    """

    def __init__(
        self,
        *,
        config: Optional[AutoGenTracerConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or AutoGenTracerConfig()
        self._strict = bool(strict)
        self._lock = threading.RLock()

        # Bounded internal storage.
        self._edges: Deque[MessageEdge] = deque(maxlen=self._config.max_edges)
        self._edge_counter: int = 0
        self._started_at: float = time.time()

        logger.debug(
            "AutoGenTracer initialized (max_edges=%d, include_preview=%s, "
            "length_strategy=%s, strict=%s)",
            self._config.max_edges,
            self._config.include_preview,
            self._config.length_strategy,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> AutoGenTracerConfig:
        return self._config

    @property
    def edge_count(self) -> int:
        with self._lock:
            return len(self._edges)

    @property
    def edges(self) -> List[Tuple[str, str, int]]:
        """
        Backward-compatible view of the graph as a list of 3-tuples.

        Returns a **copy** so callers cannot mutate the tracer's internal
        state through the returned list.
        """
        with self._lock:
            return [e.to_tuple() for e in self._edges]

    # ---------------------------------------------------------- recording
    def record(
        self, sender: Any, receiver: Any, message: Any
    ) -> Optional[MessageEdge]:
        """
        Record one message between two agents.

        Parameters
        ----------
        sender : str
            Originating agent name. Must be a non-empty string in strict mode.
        receiver : str
            Destination agent name. Must be a non-empty string in strict mode.
        message : Any
            Message payload. Only its length is retained by default. See
            :class:`AutoGenTracerConfig` for preview and length-strategy
            options.

        Returns
        -------
        MessageEdge | None
            The recorded edge, or ``None`` when the input was rejected in
            non-strict mode.
        """
        # ---- Validate sender -------------------------------------------
        if not isinstance(sender, str) or not sender:
            msg = f"sender must be a non-empty string, got {sender!r}."
            if self._strict:
                raise AutoGenTracerError(msg)
            logger.warning("%s Skipping.", msg)
            return None

        # ---- Validate receiver -----------------------------------------
        if not isinstance(receiver, str) or not receiver:
            msg = f"receiver must be a non-empty string, got {receiver!r}."
            if self._strict:
                raise AutoGenTracerError(msg)
            logger.warning("%s Skipping.", msg)
            return None

        # ---- Extract length + preview ----------------------------------
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
            edge.edge_index,
            sender,
            receiver,
            length,
        )
        return edge

    # ---------------------------------------------------------- helpers
    def _extract_length(self, message: Any) -> int:
        """
        Compute a length for ``message`` under the configured strategy.

        Always returns a non-negative int. Never raises.
        """
        strategy = self._config.length_strategy

        if message is None:
            return 0

        if strategy == "str":
            try:
                return len(str(message))
            except Exception:  # pragma: no cover — defensive
                return 0

        if strategy == "bytes":
            try:
                if isinstance(message, bytes):
                    return len(message)
                if isinstance(message, str):
                    return len(message.encode("utf-8"))
                return len(str(message).encode("utf-8"))
            except Exception:  # pragma: no cover — defensive
                return 0

        # strategy == "auto" (original behaviour)
        if hasattr(message, "__len__"):
            try:
                return max(0, int(len(message)))
            except Exception:
                logger.debug(
                    "len() raised on message of type %s; falling back to "
                    "str() length.",
                    type(message).__name__,
                )
        try:
            return len(str(message))
        except Exception:  # pragma: no cover — defensive
            return 0

    def _extract_preview(self, message: Any) -> Optional[str]:
        """Return a truncated preview of ``message`` when enabled."""
        if not self._config.include_preview:
            return None
        try:
            text = message if isinstance(message, str) else str(message)
        except Exception:  # pragma: no cover — defensive
            text = ""
        return text[: self._config.preview_max_chars]

    # ---------------------------------------------------------- graph views
    def get_graph(self) -> List[Tuple[str, str, int]]:
        """
        Return the message graph as the original ``[(sender, receiver, length),
        ...]`` shape. Returns a copy.
        """
        return self.edges

    def get_edges(self) -> List[MessageEdge]:
        """Return the message graph as a list of :class:`MessageEdge`."""
        with self._lock:
            return list(self._edges)

    def to_edge_list(self) -> List[Tuple[str, str, int]]:
        """Alias for :meth:`get_graph` (readability)."""
        return self.edges

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the recorded edges."""
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
            }

        senders = Counter(e.sender for e in edges)
        receivers = Counter(e.receiver for e in edges)
        agents = set(senders) | set(receivers)
        pairs = {(e.sender, e.receiver) for e in edges}

        lengths = [e.message_length for e in edges]
        total_chars = sum(lengths)

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
        }

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_edges: bool = True) -> int:
        """
        Clear the recorded graph and reset the edge counter.

        Returns the number of edges removed.
        """
        with self._lock:
            removed = len(self._edges)
            if clear_edges:
                self._edges.clear()
            self._edge_counter = 0
            self._started_at = time.time()
        logger.debug("AutoGenTracer reset (removed %d edge(s)).", removed)
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "started_at": self._started_at,
                "edge_counter": self._edge_counter,
                "edges": [e.to_dict() for e in self._edges],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AutoGenTracer":
        if not isinstance(data, Mapping):
            raise AutoGenTracerError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = AutoGenTracerConfig.from_dict(cfg_data)
        tracer = cls(config=cfg, strict=bool(data.get("strict", True)))
        with tracer._lock:
            for entry in data.get("edges", []):
                tracer._edges.append(MessageEdge.from_dict(entry))
            tracer._edge_counter = int(
                data.get("edge_counter", len(tracer._edges))
            )
            tracer._started_at = float(data.get("started_at", time.time()))
        return tracer

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "AutoGenTracer":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise AutoGenTracerError(f"Invalid JSON payload: {exc}") from exc

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.edge_count

    def __iter__(self) -> Iterator[Tuple[str, str, int]]:
        """Iterate over the graph as 3-tuples."""
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
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "AutoGenTracer",
    "AutoGenTracerConfig",
    "AutoGenTracerError",
    "MessageEdge",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m instrumentation.runtime.autogen_tracer
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---- Happy path --------------------------------------------------- #
    tracer = AutoGenTracer()
    print("repr       :", tracer)

    tracer.record("user_proxy", "assistant", "Hello, please solve this task.")
    tracer.record("assistant", "code_executor", "Executing code...")
    tracer.record("code_executor", "assistant", "Result: 42")
    tracer.record("assistant", "user_proxy", "Task complete.")

    print("graph      :", tracer.get_graph())
    print("edge count :", tracer.edge_count)

    # ---- Tuple-style unpacking (backward compat) ---------------------- #
    for sender, receiver, length in tracer.get_graph():
        print(f"  {sender:14s} -> {receiver:14s} (len={length})")

    # ---- MessageEdge richer view -------------------------------------- #
    first_edge = tracer.get_edges()[0]
    print("first edge :", first_edge)
    print("as tuple   :", first_edge.to_tuple())
    assert tuple(first_edge) == first_edge.to_tuple()

    # ---- Various message shapes --------------------------------------- #
    shapes = AutoGenTracer(strict=False)
    shapes.record("a", "b", "a string message")           # str
    shapes.record("a", "b", b"a bytes message")           # bytes
    shapes.record("a", "b", ["list", "of", "items"])      # list
    shapes.record("a", "b", {"key": "value", "k2": "v"})  # dict → len=keys
    shapes.record("a", "b", None)                         # None → 0
    shapes.record("a", "b", 12345)                        # int → str length
    shapes.record("a", "b", (i for i in range(5)))        # generator (unsized)
    print("shapes     :", shapes.get_graph())

    # ---- Preview retention -------------------------------------------- #
    preview_tracer = AutoGenTracer(
        config=AutoGenTracerConfig(include_preview=True, preview_max_chars=20)
    )
    preview_tracer.record("user", "agent", "This is a long message that will be truncated.")
    edge = preview_tracer.get_edges()[0]
    print("preview    :", edge.preview)
    assert len(edge.preview) == 20

    # ---- Bounded history ---------------------------------------------- #
    bounded = AutoGenTracer(config=AutoGenTracerConfig(max_edges=5))
    for i in range(20):
        bounded.record("a", "b", f"message {i}")
    print("bounded    :", bounded.edge_count, "(max_edges=5)")
    assert bounded.edge_count == 5
    # The last 5 messages are retained (indices 15..19).
    indices = [e.edge_index for e in bounded.get_edges()]
    print("indices    :", indices)

    # ---- Statistics --------------------------------------------------- #
    print("stats      :", tracer.statistics())

    # ---- Serialization round-trip ------------------------------------- #
    payload = tracer.to_json()
    restored = AutoGenTracer.from_json(payload)
    assert restored.to_dict() == tracer.to_dict()
    print("Round-trip OK.")

    # ---- Reset -------------------------------------------------------- #
    removed = tracer.reset()
    print("reset      :", removed, "edge(s) removed; new count:",
          tracer.edge_count)

    # ---- Validation failures ------------------------------------------ #
    strict = AutoGenTracer(strict=True)
    for bad in (
        ("", "receiver", "msg"),         # empty sender
        (None, "receiver", "msg"),       # None sender
        ("sender", "", "msg"),           # empty receiver
        ("sender", None, "msg"),         # None receiver
    ):
        try:
            strict.record(*bad)
        except AutoGenTracerError as exc:
            print("Rejected   :", exc)

    # Non-strict mode logs and returns None instead of raising.
    lenient = AutoGenTracer(strict=False)
    result = lenient.record("", "receiver", "msg")
    print("lenient    :", result, "(returned None, no edge recorded)")
    assert result is None and lenient.edge_count == 0

    for bad_cfg in (
        dict(max_edges=0),
        dict(preview_max_chars=0),
        dict(length_strategy="bogus"),
    ):
        try:
            AutoGenTracerConfig(**bad_cfg)  # type: ignore[arg-type]
        except AutoGenTracerError as exc:
            print("Rejected cfg:", exc)

    # ---- Alternative length strategies -------------------------------- #
    str_len = AutoGenTracer(
        config=AutoGenTracerConfig(length_strategy="str")
    )
    str_len.record("a", "b", {"k1": 1, "k2": 2})
    print("str length :", str_len.get_edges()[0].message_length,
          "(dict serialized as string)")

    byte_len = AutoGenTracer(
        config=AutoGenTracerConfig(length_strategy="bytes")
    )
    byte_len.record("a", "b", "héllo")   # é is 2 bytes in UTF-8
    print("byte length:", byte_len.get_edges()[0].message_length,
          "(UTF-8 bytes; 'héllo' = 6 bytes)")

    print("\nSmoke test passed.")
