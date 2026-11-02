"""
StateBuffer — bounded key-value ring buffer for transient quantum state.

Naming
------
The primary class is ``KeyValueBuffer``. The name ``StateBuffer`` is
preserved as a **deprecated alias** for backward compatibility, but it
collides with ``qcs.state.state_buffer.StateBuffer`` — a different
abstraction (a queue-based state machine with quantum amplitudes,
priorities, coherence times, and lifecycle transitions). New code should
import ``KeyValueBuffer`` directly to avoid ambiguity.

What this buffer is
-------------------
A bounded key-value cache. ``put(key, value)`` appends an entry;
``get(key)`` returns the most recent value for ``key``. When the buffer
reaches ``config.max_entries``, the oldest entry is evicted (FIFO) and,
if ``on_evict`` is configured, the callback receives it.

What this buffer is NOT
-----------------------
Not a state machine. Not a queue for pending executions. Not durable.
Entries live in memory and are gone when the process exits. For a
queue-based state manager, use ``qcs.state.state_buffer.StateBuffer``.

Design notes
------------
* ``on_evict`` fires **outside** the internal lock. Slow callbacks do not
  block ``put()``/``get()``. Callback failures are logged and counted.
* Optional ``config.background_eviction`` dispatches callbacks to a
  single-worker ``ThreadPoolExecutor``, fully decoupling them from the
  calling thread.
* ``lookup()`` returns a ``LookupResult`` so callers can distinguish
  "not found" from "value is None". ``contains()`` disambiguates further.
* Values are deep-copied on ``put()`` by default (``config.copy_values``)
  so callers cannot mutate buffered state by keeping a reference.
* Optional time-based expiry on read via ``config.max_age_seconds``.

Original API preserved
----------------------
``StateBuffer(max_entries=1000, on_evict=my_callback)`` — same
constructor kwargs; emits a ``DeprecationWarning``.
``put(key, value)`` / ``get(key)`` / ``get_all(key)`` /
``snapshot()`` / ``clear()`` / ``stats()`` — same calls, same return
types (``stats()`` adds keys but preserves the original three).

New API
-------
``KeyValueBuffer(...)``, ``entries()``, ``lookup()``, ``contains()``,
``delete(key)``, ``drain()``, ``health()``, ``close()``,
``__len__``, ``__contains__``.

v2 fixes (see changelog in commit message):
  * Renamed to ``KeyValueBuffer``; ``StateBuffer`` deprecated.
  * ``on_evict`` fired outside the lock; optional background mode.
  * Callback failures logged + counted (were silently swallowed).
  * ``_evicted_count`` counts evictions unconditionally.
  * ``max_entries=0`` rejected at construction.
  * ``lookup()`` disambiguates ``None`` value from missing key.
  * ``BufferedEntry`` frozen; ``at`` normalized to UTC; ``checksum()``.
  * Values deep-copied on ``put`` (``copy_values=True`` default).
  * ``clear()`` silent vs ``drain()`` invoking ``on_evict``.
  * ``delete(key)`` added.
  * Time-based expiry on read via ``config.max_age_seconds``.
  * Metrics, ``health()``, ``close()``, ``__enter__``/``__exit__``.
  * ``__version__``, ``__all__``, ``__repr__``, demo.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import math
import threading
import time
import warnings
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Constants
# =============================================================================

DEFAULT_MAX_ENTRIES: int = 1000
DEFAULT_MAX_KEY_LEN: int = 256
DEFAULT_MAX_VALUE_BYTES: int = 1_048_576    # 1 MiB


# =============================================================================
# Errors
# =============================================================================

class StateBufferError(Exception):
    """Base class for buffer errors (renamed to avoid builtin clash)."""


class StateBufferInputError(StateBufferError, ValueError):
    """Invalid input to a buffer method or the constructor."""


class StateBufferClosedError(StateBufferError):
    """A mutating operation was called after ``close()``."""


# =============================================================================
# Metrics
# =============================================================================

class MetricsRecorder:
    """Drop-in replaceable with Prometheus / OTel."""

    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self._lock = threading.RLock()

    def inc(self, name: str, value: float = 1.0) -> None:
        with self._lock:
            self.counters[name] = self.counters.get(name, 0.0) + value

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            return dict(self.counters)


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _to_jsonable(obj: Any) -> Any:
    """Recursive JSON-safe conversion; raises on unknown types."""
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    raise TypeError(
        f"_to_jsonable: cannot serialize {type(obj).__name__}"
    )


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class BufferConfig:
    """Frozen, validated configuration for ``KeyValueBuffer``."""

    max_entries: int = DEFAULT_MAX_ENTRIES
    max_key_len: int = DEFAULT_MAX_KEY_LEN
    max_value_bytes: int = DEFAULT_MAX_VALUE_BYTES

    # Copy values on put so callers cannot mutate buffered state by
    # keeping a reference. Slower; safer. Default True.
    copy_values: bool = True

    # Reject values that are not JSON-serializable or exceed max_value_bytes.
    # Optional because some callers store non-JSON objects on purpose.
    require_json_values: bool = False

    # Dispatch on_evict callbacks to a background worker thread. Decouples
    # the callback from put()'s latency; useful when the callback is slow.
    background_eviction: bool = False

    # Expire entries on read after this many seconds. None = no expiry.
    max_age_seconds: Optional[float] = None

    def __post_init__(self) -> None:
        if isinstance(self.max_entries, bool) \
                or not isinstance(self.max_entries, int) \
                or self.max_entries < 1:
            raise StateBufferInputError(
                "max_entries must be a positive int"
            )
        if isinstance(self.max_key_len, bool) \
                or not isinstance(self.max_key_len, int) \
                or self.max_key_len < 1:
            raise StateBufferInputError(
                "max_key_len must be a positive int"
            )
        if isinstance(self.max_value_bytes, bool) \
                or not isinstance(self.max_value_bytes, int) \
                or self.max_value_bytes < 1:
            raise StateBufferInputError(
                "max_value_bytes must be a positive int"
            )
        if not isinstance(self.copy_values, bool):
            raise StateBufferInputError("copy_values must be a bool")
        if not isinstance(self.require_json_values, bool):
            raise StateBufferInputError(
                "require_json_values must be a bool"
            )
        if not isinstance(self.background_eviction, bool):
            raise StateBufferInputError(
                "background_eviction must be a bool"
            )
        if self.max_age_seconds is not None:
            if isinstance(self.max_age_seconds, bool) \
                    or not isinstance(self.max_age_seconds, (int, float)) \
                    or not math.isfinite(float(self.max_age_seconds)) \
                    or self.max_age_seconds <= 0:
                raise StateBufferInputError(
                    "max_age_seconds must be a positive finite "
                    "number or None"
                )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Data classes
# =============================================================================

@dataclass(frozen=True)
class LookupResult:
    """Result of ``lookup()`` — distinguishes missing from None."""

    found: bool
    value: Any = None

    def __bool__(self) -> bool:
        return self.found

    def __repr__(self) -> str:
        if self.found:
            return f"LookupResult(found=True, value={self.value!r})"
        return "LookupResult(found=False)"


@dataclass(frozen=True)
class BufferedEntry:
    """Immutable record of one buffered value."""

    key: str
    value: Any
    at: datetime = field(default_factory=_utcnow)

    def __post_init__(self) -> None:
        if not isinstance(self.key, str) or not self.key.strip():
            raise StateBufferInputError(
                "key must be a non-empty string"
            )
        object.__setattr__(self, "at", _ensure_utc(self.at))

    def to_dict(self, *, strict: bool = False) -> Dict[str, Any]:
        """
        Serialize to a JSON-safe dict.

        ``strict=False`` (default) falls back to a placeholder string for
        values that cannot be serialized — ``snapshot()`` never crashes.
        ``strict=True`` raises on unserializable values.
        """
        if strict:
            value = _to_jsonable(self.value)
        else:
            try:
                value = _to_jsonable(self.value)
            except TypeError:
                value = f"<unserializable:{type(self.value).__name__}>"
        return {
            "key": self.key,
            "value": value,
            "at": self.at.isoformat(),
        }

    def checksum(self) -> str:
        payload = json.dumps(
            self.to_dict(strict=False), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            f"BufferedEntry(key={self.key!r}, at={self.at.isoformat()}, "
            f"value_type={type(self.value).__name__})"
        )


# =============================================================================
# The Enhanced KeyValueBuffer
# =============================================================================

class KeyValueBuffer:
    """
    Thread-safe bounded key-value ring buffer.

    See the module docstring for semantics, the eviction callback model,
    and the difference from ``qcs.state.state_buffer.StateBuffer``.
    """

    def __init__(
        self,
        *,
        config: Optional[BufferConfig] = None,
        # Backward-compat kwargs:
        max_entries: Optional[int] = None,
        on_evict: Optional[Callable[[BufferedEntry], None]] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        if config is None:
            cfg_kwargs: Dict[str, Any] = {}
            if max_entries is not None:
                cfg_kwargs["max_entries"] = int(max_entries)
            config = BufferConfig(**cfg_kwargs)
        self.config = config
        self._on_evict = on_evict
        self._metrics = metrics or MetricsRecorder()
        self._lock = threading.RLock()
        self._buffer: Deque[BufferedEntry] = deque(
            maxlen=self.config.max_entries,
        )

        # Counters (all cumulative since construction).
        self._put_count: int = 0
        self._hit_count: int = 0
        self._miss_count: int = 0
        self._expired_count: int = 0
        self._evicted_count: int = 0
        self._eviction_failures: int = 0
        self._started_mono = time.monotonic()

        # Optional background executor for on_evict.
        self._background_executor: Optional[ThreadPoolExecutor] = (
            ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="kvbuf-evict",
            )
            if self.config.background_eviction and on_evict is not None
            else None
        )

        self._closed = False

        if on_evict is not None and _is_coroutine_function(on_evict):
            logger.warning(
                "on_evict is a coroutine function; the buffer is "
                "synchronous. Wrap it with `asyncio.to_thread` at the "
                "call site.",
            )

    # ------------------------------------------------------------------ lifecycle
    def close(self) -> None:
        """Shut down the background eviction executor (if any)."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            ex = self._background_executor
            self._background_executor = None
        if ex is not None:
            ex.shutdown(wait=True)
            logger.debug("KeyValueBuffer closed")

    def __enter__(self) -> "KeyValueBuffer":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ writes
    def put(self, key: str, value: Any) -> Optional[BufferedEntry]:
        """
        Buffer ``value`` under ``key``.

        Returns the evicted entry if the buffer was full (and an entry
        was dropped to make room), else ``None``. The eviction callback
        — if configured — has already been scheduled/fired for the
        returned entry.
        """
        self._validate_key(key)
        self._validate_value(value)
        stored_value = (
            copy.deepcopy(value)
            if self.config.copy_values else value
        )
        entry = BufferedEntry(key=key, value=stored_value)

        evicted: Optional[BufferedEntry] = None
        with self._lock:
            if self._closed:
                raise StateBufferClosedError(
                    "buffer has been closed; instantiate a new one"
                )
            max_len = self._buffer.maxlen
            if max_len is not None and len(self._buffer) >= max_len:
                # The oldest entry will be auto-evicted by append().
                evicted = self._buffer[0]
                self._evicted_count += 1
            self._buffer.append(entry)
            self._put_count += 1

        self._metrics.inc("put_total")
        if evicted is not None:
            self._invoke_on_evict(evicted)
        return evicted

    def delete(self, key: str) -> int:
        """
        Remove all entries with ``key``. Returns the number removed.

        The eviction callback fires per removed entry (outside the lock).
        """
        self._validate_key(key)
        removed: List[BufferedEntry] = []
        with self._lock:
            if self._closed:
                raise StateBufferClosedError("buffer is closed")
            keep = [e for e in self._buffer if e.key != key]
            if len(keep) != len(self._buffer):
                removed = [e for e in self._buffer if e.key == key]
                self._buffer = deque(
                    keep, maxlen=self.config.max_entries,
                )
                self._evicted_count += len(removed)
        for entry in removed:
            self._invoke_on_evict(entry)
        if removed:
            self._metrics.inc("delete_removed_total", len(removed))
        return len(removed)

    def clear(self) -> None:
        """Discard all entries **without** invoking ``on_evict``."""
        with self._lock:
            if self._closed:
                raise StateBufferClosedError("buffer is closed")
            self._buffer.clear()
        self._metrics.inc("clear_total")

    def drain(self) -> Tuple[BufferedEntry, ...]:
        """
        Remove and return every entry; ``on_evict`` fires per entry.

        Use this when evicting entries should be persisted. Use
        ``clear()`` when they should be discarded silently.
        """
        with self._lock:
            if self._closed:
                raise StateBufferClosedError("buffer is closed")
            entries = tuple(self._buffer)
            self._buffer.clear()
            self._evicted_count += len(entries)
        for entry in entries:
            self._invoke_on_evict(entry)
        self._metrics.inc("drain_total", len(entries))
        return entries

    # ------------------------------------------------------------------ reads
    def lookup(self, key: str) -> LookupResult:
        """
        Look up ``key``.

        Returns ``LookupResult(found=True, value=v)`` for a live entry,
        ``LookupResult(found=False)`` otherwise. Distinguishes "missing"
        from "value is None".
        """
        with self._lock:
            now = (
                _utcnow()
                if self.config.max_age_seconds is not None
                else None
            )
            for entry in reversed(self._buffer):
                if entry.key == key:
                    if now is not None and self._is_expired(entry, now):
                        self._expired_count += 1
                        self._miss_count += 1
                        return LookupResult(found=False)
                    self._hit_count += 1
                    return LookupResult(found=True, value=entry.value)
            self._miss_count += 1
        return LookupResult(found=False)

    def get(self, key: str) -> Optional[Any]:
        """
        Return the most recent value for ``key``, or ``None`` if not
        found (or expired).

        Ambiguity: ``get(k) is None`` can mean "value is None" or "not
        found". Use ``lookup()`` or ``contains()`` to disambiguate.
        """
        result = self.lookup(key)
        return result.value if result.found else None

    def contains(self, key: str) -> bool:
        """Return True iff a live (non-expired) entry exists for ``key``."""
        with self._lock:
            now = (
                _utcnow()
                if self.config.max_age_seconds is not None
                else None
            )
            for entry in reversed(self._buffer):
                if entry.key == key:
                    if now is not None and self._is_expired(entry, now):
                        return False
                    return True
        return False

    def get_all(self, key: str) -> List[Any]:
        """Return all (non-expired) values for ``key``, oldest first."""
        with self._lock:
            now = (
                _utcnow()
                if self.config.max_age_seconds is not None
                else None
            )
            return [
                e.value for e in self._buffer
                if e.key == key
                and (now is None or not self._is_expired(e, now))
            ]

    def entries(self) -> Tuple[BufferedEntry, ...]:
        """Return a typed snapshot of all entries (insertion order)."""
        with self._lock:
            return tuple(self._buffer)

    def snapshot(self) -> List[Dict[str, Any]]:
        """
        Return the buffer as a list of dicts (backward-compat shape).

        Values that cannot be JSON-serialized are rendered as
        ``"<unserializable:Type>"``. Use ``entries()`` for typed
        access or ``BufferedEntry.to_dict(strict=True)`` to raise on
        unserializable values.
        """
        with self._lock:
            return [e.to_dict(strict=False) for e in self._buffer]

    # ------------------------------------------------------------------ stats
    def stats(self) -> Dict[str, Any]:
        """
        Cumulative statistics since construction.

        Keys
        ----
        size, max_entries, evicted        — preserved from pre-2.0
        eviction_failures                 — on_evict raised
        puts, hits, misses, expired_on_read
        """
        with self._lock:
            return {
                "size": len(self._buffer),
                "max_entries": self.config.max_entries,
                "evicted": self._evicted_count,
                "eviction_failures": self._eviction_failures,
                "puts": self._put_count,
                "hits": self._hit_count,
                "misses": self._miss_count,
                "expired_on_read": self._expired_count,
            }

    def health(self) -> Dict[str, Any]:
        return {
            "ready": not self._closed,
            "closed": self._closed,
            "size": len(self._buffer) if not self._closed else 0,
            "config": self.config.to_dict(),
            "uptime_seconds": time.monotonic() - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }

    # ------------------------------------------------------------------ dunder
    def __len__(self) -> int:
        with self._lock:
            return len(self._buffer)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        return self.contains(key)

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"KeyValueBuffer("
                f"size={len(self._buffer)}, "
                f"max_entries={self.config.max_entries}, "
                f"evicted={self._evicted_count})"
            )

    # ------------------------------------------------------------------ internals
    def _validate_key(self, key: Any) -> None:
        if not isinstance(key, str) or not key.strip():
            raise StateBufferInputError(
                "key must be a non-empty string"
            )
        if len(key) > self.config.max_key_len:
            raise StateBufferInputError(
                f"key exceeds {self.config.max_key_len} chars"
            )

    def _validate_value(self, value: Any) -> None:
        if not self.config.require_json_values:
            return
        try:
            serialized = json.dumps(_to_jsonable(value))
        except (TypeError, ValueError) as exc:
            raise StateBufferInputError(
                f"value is not JSON-serializable: {exc}"
            ) from exc
        if len(serialized) > self.config.max_value_bytes:
            raise StateBufferInputError(
                f"value size {len(serialized)} bytes exceeds "
                f"max_value_bytes {self.config.max_value_bytes}"
            )

    def _is_expired(self, entry: BufferedEntry, now: datetime) -> bool:
        if self.config.max_age_seconds is None:
            return False
        age = (now - entry.at).total_seconds()
        return age > self.config.max_age_seconds

    def _invoke_on_evict(self, entry: BufferedEntry) -> None:
        """Fire the eviction callback (outside the lock)."""
        if self._on_evict is None:
            return
        # Background path?
        with self._lock:
            ex = self._background_executor
        if ex is not None:
            try:
                ex.submit(self._call_on_evict, entry)
                return
            except RuntimeError:
                # Executor shut down concurrently; fall through to sync.
                logger.debug(
                    "background executor unavailable; "
                    "invoking on_evict synchronously",
                    extra={"key": entry.key},
                )
        self._call_on_evict(entry)

    def _call_on_evict(self, entry: BufferedEntry) -> None:
        try:
            self._on_evict(entry)
        except Exception as exc:
            with self._lock:
                self._eviction_failures += 1
            self._metrics.inc("on_evict_failures_total")
            logger.exception(
                "on_evict raised",
                extra={
                    "key": entry.key,
                    "at": entry.at.isoformat(),
                },
            )


def _is_coroutine_function(fn: Callable[..., Any]) -> bool:
    try:
        import asyncio
        return asyncio.iscoroutinefunction(fn)
    except Exception:
        return False


# =============================================================================
# Deprecated alias
# =============================================================================

class StateBuffer(KeyValueBuffer):
    """
    Deprecated alias for ``KeyValueBuffer``.

    This name collides with ``qcs.state.state_buffer.StateBuffer``
    (the queue-based state machine). If you need that class, import
    from ``qcs.state.state_buffer``. This alias points at the
    key-value ring buffer.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            "StateBuffer is deprecated; use KeyValueBuffer. "
            "For the queue-based state machine, use "
            "qcs.state.state_buffer.StateBuffer.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "BufferedEntry",
    "BufferConfig",
    "DEFAULT_MAX_ENTRIES",
    "DEFAULT_MAX_KEY_LEN",
    "DEFAULT_MAX_VALUE_BYTES",
    "KeyValueBuffer",
    "LookupResult",
    "MetricsRecorder",
    "StateBuffer",
    "StateBufferClosedError",
    "StateBufferError",
    "StateBufferInputError",
)


# =============================================================================
# Demo: python -m qcs.buffer.state_buffer
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json
    from datetime import datetime, timedelta, timezone

    logging.basicConfig(level=logging.INFO)

    print("=== Basic put / get ===")
    buf = KeyValueBuffer(max_entries=5)
    buf.put("k1", "hello")
    buf.put("k2", {"x": 1})
    print(f"  get(k1): {buf.get('k1')!r}")
    print(f"  get(k2): {buf.get('k2')!r}")
    print(f"  len: {len(buf)}")
    print(f"  'k1' in buf: {'k1' in buf}")
    print(f"  'missing' in buf: {'missing' in buf}")

    print("\n=== §1.6 lookup() disambiguates None from missing ===")
    buf.put("none-value", None)
    print(f"  lookup(none-value): {buf.lookup('none-value')!r}")
    print(f"  lookup(missing):    {buf.lookup('missing')!r}")
    print(f"  get(none-value):    {buf.get('none-value')!r}")
    print(f"  contains(none-value): {buf.contains('none-value')}")
    assert buf.lookup("none-value").found
    assert not buf.lookup("missing").found
    assert buf.get("none-value") is None

    print("\n=== §1.2/§1.3/§1.4 Eviction callback fired outside lock ===")
    evicted_keys: List[str] = []
    def on_evict(entry: BufferedEntry) -> None:
        evicted_keys.append(entry.key)
    buf2 = KeyValueBuffer(
        config=BufferConfig(max_entries=3),
        on_evict=on_evict,
    )
    for i in range(5):
        buf2.put(f"e{i}", i)
    print(f"  evicted: {evicted_keys}")
    print(f"  stats:   {buf2.stats()}")
    assert evicted_keys == ["e0", "e1"]
    assert buf2.stats()["evicted"] == 2

    print("\n=== §1.4 Evicted count without on_evict ===")
    buf3 = KeyValueBuffer(config=BufferConfig(max_entries=2))
    for i in range(5):
        buf3.put(f"x{i}", i)
    print(f"  evicted: {buf3.stats()['evicted']}")
    assert buf3.stats()["evicted"] == 3

    print("\n=== §1.3 Callback failure is logged + counted ===")
    def broken_on_evict(entry):
        raise RuntimeError("callback boom")
    buf4 = KeyValueBuffer(
        config=BufferConfig(max_entries=2),
        on_evict=broken_on_evict,
    )
    for i in range(4):
        buf4.put(f"b{i}", i)
    print(f"  eviction_failures: {buf4.stats()['eviction_failures']}")
    assert buf4.stats()["eviction_failures"] == 2

    print("\n=== §1.5 max_entries=0 rejected ===")
    for val in [0, -1, 1.5, True]:
        try:
            KeyValueBuffer(config=BufferConfig(max_entries=val))
            print(f"  max_entries={val}: NOT rejected")
        except Exception as e:
            print(f"  max_entries={val}: {type(e).__name__}")

    print("\n=== §1.7 Frozen BufferedEntry ===")
    entry = buf.entries()[0]
    print(f"  {entry!r}")
    try:
        entry.key = "hacked"  # type: ignore[misc]
    except Exception as e:
        print(f"  entry.key mutation → {type(e).__name__}")

    print("\n=== §1.7 Value deep-copied on put ===")
    original = [1, 2, 3]
    buf.put("mutable", original)
    original.append(4)
    print(f"  after external mutation: {buf.get('mutable')}")
    assert buf.get("mutable") == [1, 2, 3]

    print("\n=== §2.1 Key validation ===")
    for bad_key in ["", "   ", None, 123]:
        try:
            buf.put(bad_key, "v")  # type: ignore[arg-type]
            print(f"  key={bad_key!r}: NOT rejected")
        except StateBufferInputError:
            print(f"  key={bad_key!r}: rejected")
    # Long key
    try:
        buf.put("x" * 500, "v")
    except StateBufferInputError:
        print(f"  long key: rejected")

    print("\n=== §2.2 Optional JSON-value validation ===")
    strict = KeyValueBuffer(
        config=BufferConfig(require_json_values=True),
    )
    strict.put("ok", {"fine": 1})
    print(f"  JSON value accepted")
    try:
        strict.put("bad", object())
    except StateBufferInputError:
        print(f"  object() rejected")
    try:
        strict.put("huge", {"x": "a" * 2_000_000})
    except StateBufferInputError:
        print(f"  oversized value rejected")

    print("\n=== §2.3 BufferedEntry.to_dict ===")
    e = BufferedEntry(key="k", value={"a": 1, "b": [1, 2]})
    print(f"  to_dict: {e.to_dict()}")
    # Unserializable value → graceful fallback
    e2 = BufferedEntry(key="k2", value=object())
    print(f"  unsserializable → {e2.to_dict()['value']}")
    try:
        e2.to_dict(strict=True)
    except TypeError:
        print(f"  strict=True raises ✓")

    print("\n=== §2.4 Typed entries() ===")
    buf.put("a", 1)
    buf.put("b", 2)
    for e in buf.entries():
        print(f"  {e!r}")

    print("\n=== §2.7/§2.8 clear() vs drain() vs delete() ===")
    collected = []
    b = KeyValueBuffer(
        config=BufferConfig(max_entries=10),
        on_evict=lambda e: collected.append(e.key),
    )
    for i in range(5):
        b.put(f"d{i}", i)
    b.delete("d2")
    print(f"  delete(d2): collected={collected}")
    assert "d2" in collected
    collected.clear()
    b.clear()
    print(f"  clear(): collected={collected} (empty expected)")
    assert collected == []
    # Re-populate and drain
    for i in range(3):
        b.put(f"d{i}", i)
    drained = b.drain()
    print(f"  drain(): {[e.key for e in drained]}")
    print(f"  callbacks fired: {collected}")
    assert set(collected) == {"d0", "d1", "d2"}
    assert len(b) == 0

    print("\n=== §3.3 Time-based expiry on read ===")
    fast = KeyValueBuffer(
        config=BufferConfig(max_age_seconds=0.05),
    )
    fast.put("short", "v")
    print(f"  immediately: {fast.get('short')}")
    time.sleep(0.1)
    print(f"  after 100ms: {fast.get('short')}")
    assert fast.get("short") is None
    assert not fast.contains("short")
    print(f"  expired_on_read: {fast.stats()['expired_on_read']}")

    print("\n=== §3.6 Background eviction ===")
    bg_collected = []
    bg = KeyValueBuffer(
        config=BufferConfig(max_entries=2, background_eviction=True),
        on_evict=lambda e: (time.sleep(0.001), bg_collected.append(e.key)),
    )
    for i in range(5):
        bg.put(f"bg{i}", i)
    time.sleep(0.05)  # let the background worker drain
    print(f"  collected in background: {bg_collected}")
    bg.close()
    assert bg_collected == ["bg0", "bg1", "bg2"]

    print("\n=== §3.4 close() prevents further writes ===")
    b_closed = KeyValueBuffer()
    b_closed.close()
    try:
        b_closed.put("k", "v")
    except StateBufferClosedError:
        print(f"  put after close → StateBufferClosedError")

    print("\n=== §3.1/§3.2 stats + health ===")
    print(f"  buf.stats(): {buf.stats()}")
    print(f"  buf.health()['size']: {buf.health()['size']}")
    print(f"  buf.health()['config']: {buf.health()['config']}")

    print("\n=== §3.2 Deprecated StateBuffer alias ===")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        legacy = StateBuffer(max_entries=3)
        assert any(
            issubclass(w.category, DeprecationWarning) for w in caught
        )
    print(f"  StateBuffer() → DeprecationWarning ✓")
    legacy.put("k", "v")
    print(f"  legacy works: get(k) = {legacy.get('k')}")

    print("\n=== §4 Thread safety ===")
    import threading
    shared = KeyValueBuffer(config=BufferConfig(max_entries=100))
    def worker(idx: int) -> None:
        for j in range(50):
            shared.put(f"t{idx}-{j}", j)
            shared.get(f"t{idx}-{j}")
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    s = shared.stats()
    print(f"  puts={s['puts']}, hits={s['hits']}, "
          f"misses={s['misses']}, evicted={s['evicted']}")
    assert s["puts"] == 250
    assert s["hits"] + s["misses"] == 250

    print("\n=== Concurrent eviction callback does NOT block put() ===")
    import time as _time
    slow_called: List[float] = []
    def slow_cb(entry):
        slow_called.append(_time.monotonic())
        _time.sleep(0.1)
    concurrent = KeyValueBuffer(
        config=BufferConfig(
            max_entries=2, background_eviction=True,
        ),
        on_evict=slow_cb,
    )
    t0 = _time.monotonic()
    for i in range(10):
        concurrent.put(f"c{i}", i)
    elapsed = _time.monotonic() - t0
    print(f"  10 puts in {elapsed*1000:.1f}ms "
          f"(slow callback runs in background)")
    concurrent.close()

    print("\n=== Serialization sanity ===")
    snap = buf.snapshot()
    print(f"  snapshot JSON-safe: "
          f"{len(json.dumps(snap))} bytes")
    # Unserializable value doesn't break snapshot
    buf.put("weird", object())
    snap2 = buf.snapshot()
    weird = next(e for e in snap2 if e["key"] == "weird")
    print(f"  unserializable → {weird['value']}")

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
