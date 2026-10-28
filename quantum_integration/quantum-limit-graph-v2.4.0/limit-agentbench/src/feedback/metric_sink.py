# src/feedback/metric_sink.py

"""
Metric Sinks — v2.0.0
======================

Abstract sink for emitting metrics, plus production-ready implementations:
:class:`StdoutSink`, :class:`InMemorySink`, and :class:`JsonlSink`.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``JsonlSink.emit`` now performs **real atomic writes**: each line
        is written to a ``tempfile.NamedTemporaryFile``, ``fsync``'d,
        then appended to the target under a second ``fsync``. The temp
        file is always cleaned up. A crash during ``fsync`` of the temp
        file leaves the target completely untouched. ``tempfile`` is now
        actually used (previously imported but dead).
  * 2.2 ``InMemorySink.to_dict`` records ``max_entries``; ``from_dict``
        restores it. A sink created with ``max_entries=5`` round-trips
        with ``max_entries=5`` (was: silently reset to 10_000).
  * 2.3 ``InMemorySink`` rejects ``bool`` for ``max_entries``. Previously
        ``InMemorySink(max_entries=True)`` became ``deque(maxlen=1)``.
  * 2.4 ``InMemorySink.from_dict`` logs a ``WARNING`` before truncating
        history when the payload exceeds ``max_entries``.
  * 2.5 ``JsonlSink.read_all`` distinguishes "trailing partial line" (no
        final newline — in-progress write) from "malformed middle line"
        (corruption). The former is logged at ``DEBUG`` and skipped; the
        latter is logged at ``WARNING``.
  * 2.6 ``MetricSink._validate`` returns the **JSON-safe** form of the
        payload (round-tripped through ``json.dumps``/``json.loads``
        with a shared ``_json_default``). ``InMemorySink.emit({"ts":
        datetime.now()})`` stores the ISO string, and ``to_dict()`` is
        JSON-serializable without needing ``default=str``.

**P1 robustness / observability**
  * 3.1 ``StdoutSink`` emits JSON (``[METRICS] {"energy": 0.5}``), matching
        ``JsonlSink``'s output format — not Python dict repr.
  * 3.2 ``InMemorySink.to_json`` / ``from_json`` added (symmetry with
        ``to_dict`` / ``from_dict``).
  * 3.3 ``JsonlSink.statistics()`` returns ``total_on_disk``,
        ``total_this_process``, ``buffered``, ``keys_seen``, ``path``
        (parity with ``InMemorySink.statistics()``).
  * 3.4 ``JsonlSink(resume_total=True)`` initializes ``_total`` from the
        existing file's line count. Default remains ``False`` for
        backward compat.
  * 3.5 ``metrics_hook(event, fields)`` on every sink; fires
        ``metric_emitted`` after each successful emit.
  * 3.6 ``__getstate__`` / ``__setstate__`` on both stateful sinks; they
        are now picklable.
  * 3.7 ``__version__`` exported.
  * 3.8 ``MetricSink`` context manager support (``with sink: ...``).
  * 3.10 ``MetricSink`` is an ``ABC`` with ``@abstractmethod emit`` —
        direct construction raises ``TypeError`` at ``__init__`` (not
        ``NotImplementedError`` at first ``emit``).

**P2 polish**
  * ``JsonlSink(fsync=callable)`` hook for test injection (and for
    environments that need a no-op ``fsync``).
  * ``StdoutSink.flush()`` explicitly flushes ``sys.stdout``.

Backward compatible: every v1.0.0 import path, method, and payload works
unchanged. ``MetricSink()`` — the only breaking change — was never usable
directly (its ``emit`` raised ``NotImplementedError``); it now raises
``TypeError`` one step earlier with the same effect.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import threading
from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime, date, time
from pathlib import Path
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Shared JSON serialization helper (P0 2.6)
# --------------------------------------------------------------------------- #
def _json_default(obj: Any) -> Any:
    """
    Best-effort serializer for non-primitive types.

    Handles ``datetime``, ``date``, and ``time`` via ``isoformat()`` so
    they become ISO 8601 strings (matching ``CarbonIntensityUpdate``'s
    conventions). Everything else falls back to ``str``.
    """
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    return str(obj)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MetricSinkError(ValueError):
    """Raised for invalid sink inputs."""


# --------------------------------------------------------------------------- #
# Abstract base (P1 3.10)
# --------------------------------------------------------------------------- #
class MetricSink(ABC):
    """Abstract sink for emitting metrics."""

    def __init__(
        self,
        *,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        if metrics_hook is not None and not callable(metrics_hook):
            raise MetricSinkError("metrics_hook must be callable.")
        self._metrics_hook = metrics_hook

    # ------------------------------------------------------------------ #
    @abstractmethod
    def emit(self, metrics: Mapping[str, Any]) -> None:
        """Emit a metric payload. Subclasses must override."""

    def flush(self) -> None:
        """Optional flush hook. Default is a no-op."""
        return None

    def close(self) -> None:
        """Optional close hook. Default is a no-op."""
        return None

    # P1 3.8: context manager
    def __enter__(self) -> "MetricSink":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    def _emit_metric(
        self, event: str, fields_: Mapping[str, Any],
    ) -> None:
        """P1 3.5: fire the metrics hook, best-effort."""
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields_))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    # ------------------------------------------------------------------ #
    @staticmethod
    def _validate(metrics: Mapping[str, Any]) -> Dict[str, Any]:
        """
        Return the **JSON-safe** form of ``metrics`` (P0 2.6).

        Raises ``MetricSinkError`` when ``metrics`` is not a Mapping or
        cannot be serialized to JSON even with ``_json_default``.
        """
        if not isinstance(metrics, Mapping):
            raise MetricSinkError(
                f"metrics must be a Mapping, "
                f"got {type(metrics).__name__}.")
        try:
            serialized = json.dumps(metrics, default=_json_default)
        except (TypeError, ValueError) as exc:
            raise MetricSinkError(
                f"metrics must be JSON-serializable: {exc}") from exc
        # Round-trip so the returned dict is provably JSON-safe:
        # datetimes become ISO strings, sets become lists, etc. — the
        # same shape every sink hands to downstream consumers.
        return json.loads(serialized)


# --------------------------------------------------------------------------- #
# Stdout sink (P1 3.1)
# --------------------------------------------------------------------------- #
class StdoutSink(MetricSink):
    """Safe default sink (AgentBeats-compatible) that emits JSON."""

    def __init__(
        self,
        *,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        super().__init__(metrics_hook=metrics_hook)

    def emit(self, metrics: Mapping[str, Any]) -> None:
        payload = self._validate(metrics)
        # Emit JSON so downstream log parsers see a consistent format
        # matching JsonlSink (P1 3.1).
        print("[METRICS]", json.dumps(payload))
        logger.debug("StdoutSink emitted: %s", payload)
        self._emit_metric("metric_emitted", {
            "sink": "stdout",
            "keys": sorted(payload.keys()),
        })

    def flush(self) -> None:
        sys.stdout.flush()


# --------------------------------------------------------------------------- #
# In-memory sink (P0 2.2, 2.3, 2.4, 2.6; P1 3.2, 3.6)
# --------------------------------------------------------------------------- #
class InMemorySink(MetricSink):
    """Bounded in-memory sink with statistics and serialization."""

    def __init__(
        self,
        max_entries: int = 10_000,
        *,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        super().__init__(metrics_hook=metrics_hook)
        # P0 2.3: reject bool.
        if isinstance(max_entries, bool) or \
                not isinstance(max_entries, int) or \
                max_entries <= 0:
            raise MetricSinkError(
                "max_entries must be a positive int.")
        self._lock = threading.RLock()
        self._entries: Deque[Dict[str, Any]] = deque(maxlen=max_entries)
        self._total: int = 0

    # ------------------------------------------------------------------ #
    @property
    def entries(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(e) for e in self._entries]

    @property
    def total_emitted(self) -> int:
        with self._lock:
            return self._total

    # ------------------------------------------------------------------ #
    def emit(self, metrics: Mapping[str, Any]) -> None:
        payload = self._validate(metrics)
        with self._lock:
            self._entries.append(payload)
            self._total += 1
            buffered = len(self._entries)
        self._emit_metric("metric_emitted", {
            "sink": "memory",
            "buffered": buffered,
            "keys": sorted(payload.keys()),
        })

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
            "max_entries": self._entries.maxlen,
            "keys_seen": keys,
        }

    def clear(self) -> int:
        with self._lock:
            n = len(self._entries)
            self._entries.clear()
        return n

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "max_entries": self._entries.maxlen,   # P0 2.2
                "total_emitted": self._total,
                "entries": [dict(e) for e in self._entries],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "InMemorySink":
        if not isinstance(data, Mapping):
            raise MetricSinkError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        max_entries = int(data.get("max_entries", 10_000))
        entries = list(data.get("entries", []))

        # P0 2.4: warn before silent truncation.
        if len(entries) > max_entries:
            logger.warning(
                "Restoring %d entries into max_entries=%d; "
                "oldest %d will be evicted.",
                len(entries), max_entries,
                len(entries) - max_entries)

        sink = cls(max_entries=max_entries)
        with sink._lock:
            sink._total = int(data.get("total_emitted", 0))
            # The deque keeps the LAST max_entries when it overflows.
            for e in entries:
                sink._entries.append(dict(e))
        return sink

    # P1 3.2
    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=_json_default, **kwargs)

    @classmethod
    def from_json(cls, payload: str, **kwargs: Any) -> "InMemorySink":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise MetricSinkError(
                f"Invalid JSON payload: {exc}") from exc

    # P1 3.6
    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()

    def __repr__(self) -> str:
        with self._lock:
            return (
                "InMemorySink("
                f"buffered={len(self._entries)}, "
                f"max_entries={self._entries.maxlen}, "
                f"total={self._total})"
            )


# --------------------------------------------------------------------------- #
# JSONL sink (P0 2.1, 2.5; P1 3.3, 3.4, 3.6)
# --------------------------------------------------------------------------- #
class JsonlSink(MetricSink):
    """
    Append-only JSON-lines sink with atomic writes.

    Each ``emit`` writes the payload to a temp file in the target's
    directory, ``fsync``'s it, then appends its contents to the target
    under a second ``fsync``. A crash during the temp ``fsync`` leaves
    the target file untouched.
    """

    def __init__(
        self,
        path: str,
        *,
        create_dirs: bool = True,
        resume_total: bool = False,
        fsync: Optional[Callable[[int], None]] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        super().__init__(metrics_hook=metrics_hook)
        if not isinstance(path, str) or not path:
            raise MetricSinkError(
                "path must be a non-empty string.")
        self._lock = threading.RLock()
        self.path = Path(path)
        if create_dirs:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fsync: Callable[[int], None] = (
            fsync if fsync is not None else os.fsync)
        self._total: int = 0

        # P1 3.4: optionally resume from the existing file's line count.
        if resume_total and self.path.exists():
            try:
                with self.path.open("r", encoding="utf-8") as f:
                    self._total = sum(1 for _ in f)
            except OSError as exc:
                logger.warning(
                    "Could not count existing lines in %s: %s",
                    self.path, exc)

    # ------------------------------------------------------------------ #
    @property
    def total_emitted(self) -> int:
        """Number of emissions made by this process (P1 3.4)."""
        with self._lock:
            return self._total

    # ------------------------------------------------------------------ #
    def emit(self, metrics: Mapping[str, Any]) -> None:
        """
        Append one JSON line atomically.

        P0 2.1: the line is written to a temp file first; if the temp
        ``fsync`` fails, the target is untouched. The temp file is
        always cleaned up.
        """
        payload = self._validate(metrics)
        line = json.dumps(payload) + "\n"
        line_bytes = line.encode("utf-8")

        with self._lock:
            tmp_path: Optional[Path] = None
            try:
                # Step 1: write the line to a temp file in the target's
                # directory. This is the "prepare" step — the target is
                # not touched until it succeeds and fsyncs.
                with tempfile.NamedTemporaryFile(
                    mode="wb",
                    dir=str(self.path.parent),
                    delete=False,
                    prefix=f".{self.path.name}.",
                    suffix=".tmp",
                ) as tmp:
                    tmp.write(line_bytes)
                    tmp.flush()
                    self._fsync(tmp.fileno())
                    tmp_path = Path(tmp.name)

                # Step 2: append the prepared bytes to the target and
                # fsync the target. Even if the OS crashes here, the
                # worst case is the target has an unfsync'd trailing
                # line — the reader skips it (see read_all).
                with self.path.open("ab") as dst:
                    with tmp_path.open("rb") as src:
                        dst.write(src.read())
                    dst.flush()
                    self._fsync(dst.fileno())

                self._total += 1
                self._emit_metric("metric_emitted", {
                    "sink": "jsonl",
                    "path": str(self.path),
                    "keys": sorted(payload.keys()),
                })
            except OSError as exc:
                raise MetricSinkError(
                    f"Could not write to {self.path}: {exc}"
                ) from exc
            finally:
                if tmp_path is not None:
                    try:
                        tmp_path.unlink(missing_ok=True)
                    except OSError:  # pragma: no cover — defensive
                        logger.debug(
                            "Could not remove temp file %s",
                            tmp_path)

    # ------------------------------------------------------------------ #
    def read_all(
        self, *, skip_partial_last: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Read every complete entry from the file (P0 2.5).

        A line without a trailing newline is treated as an in-progress
        write and skipped (``DEBUG``). A malformed line that *did* end
        with a newline is corruption and logged at ``WARNING``.
        """
        with self._lock:
            if not self.path.exists():
                return []
            try:
                raw = self.path.read_text(encoding="utf-8")
            except OSError as exc:
                raise MetricSinkError(
                    f"Could not read {self.path}: {exc}") from exc

        if not raw:
            return []

        has_complete_last = raw.endswith("\n")
        lines = raw.splitlines()

        out: List[Dict[str, Any]] = []
        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            is_last = (i == len(lines) - 1)
            is_partial = is_last and not has_complete_last

            if is_partial and skip_partial_last:
                logger.debug(
                    "Skipping partial last line "
                    "(no trailing newline).")
                continue

            try:
                out.append(json.loads(stripped))
            except json.JSONDecodeError:
                if is_partial:
                    logger.debug(
                        "Skipping malformed partial last line.")
                else:
                    logger.warning(
                        "Malformed JSONL at line %d", i + 1)
        return out

    # ------------------------------------------------------------------ #
    def statistics(self) -> Dict[str, Any]:
        """P1 3.3: parity with InMemorySink.statistics()."""
        entries = self.read_all()
        keys: Dict[str, int] = {}
        for e in entries:
            for k in e.keys():
                keys[k] = keys.get(k, 0) + 1
        with self._lock:
            total_this_process = self._total
        return {
            "total_on_disk": len(entries),
            "total_this_process": total_this_process,
            "buffered": len(entries),
            "keys_seen": keys,
            "path": str(self.path),
        }

    # P1 3.6
    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        state.pop("_fsync", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()
        self._fsync = os.fsync

    def __repr__(self) -> str:
        with self._lock:
            return (
                "JsonlSink("
                f"path={str(self.path)!r}, "
                f"total_this_process={self._total})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "__version__",
    "InMemorySink",
    "JsonlSink",
    "MetricSink",
    "MetricSinkError",
    "StdoutSink",
]


# --------------------------------------------------------------------------- #
# Self-tests
# --------------------------------------------------------------------------- #
def _run_selftests() -> None:
    """Run the module's self-tests."""
    import io
    import pickle
    from contextlib import redirect_stdout

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    class _CaptureHandler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.records: List[logging.LogRecord] = []

        def emit(self, record: logging.LogRecord) -> None:
            self.records.append(record)

    print(f"=== metric_sink v{__version__} self-tests ===\n")

    # ---------------------------------------------------------------- #
    # P1 3.10 — MetricSink is abstract
    # ---------------------------------------------------------------- #
    print("=== P1 3.10: MetricSink is abstract ===")
    try:
        MetricSink()  # type: ignore[abstract]
    except TypeError as exc:
        assert "abstract" in str(exc).lower(), (
            f"P1 3.10 regression: TypeError message lacks "
            f"'abstract': {exc}")
        print(f"  ✓ MetricSink() → TypeError: {exc}")
    else:
        raise AssertionError(
            "P1 3.10 regression: MetricSink() did not raise")

    # ---------------------------------------------------------------- #
    # P1 3.1 — StdoutSink emits JSON
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: StdoutSink emits JSON ===")
    sink = StdoutSink()
    buf = io.StringIO()
    with redirect_stdout(buf):
        sink.emit({"energy": 0.5})
    output = buf.getvalue()
    assert '[METRICS] {"energy": 0.5}' in output, (
        f"P1 3.1 regression: expected '[METRICS] {{\"energy\": 0.5}}' "
        f"in output, got {output!r}")
    assert "'energy'" not in output, (
        f"P1 3.1 regression: Python dict repr leaked into output: "
        f"{output!r}")
    print(f"  ✓ output: {output.strip()}")

    # ---------------------------------------------------------------- #
    # P0 2.3 — bool rejected for max_entries
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: bool rejected for max_entries ===")
    try:
        InMemorySink(max_entries=True)  # type: ignore[arg-type]
    except MetricSinkError as exc:
        print(f"  ✓ InMemorySink(max_entries=True) → "
              f"MetricSinkError: {exc}")
    else:
        raise AssertionError(
            "P0 2.3 regression: bool was accepted as max_entries")

    try:
        InMemorySink(max_entries=False)
    except MetricSinkError:
        print(f"  ✓ InMemorySink(max_entries=False) → "
              f"MetricSinkError")

    # Valid value still works.
    s = InMemorySink(max_entries=5)
    print(f"  ✓ InMemorySink(max_entries=5) accepted")

    # ---------------------------------------------------------------- #
    # P0 2.2 — max_entries round-trip
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: max_entries round-trip ===")
    s = InMemorySink(max_entries=5)
    s.emit({"a": 1})
    payload = s.to_dict()
    assert payload["max_entries"] == 5, (
        f"P0 2.2 regression: to_dict max_entries="
        f"{payload['max_entries']}, expected 5")
    restored = InMemorySink.from_dict(payload)
    assert restored._entries.maxlen == 5, (
        f"P0 2.2 regression: restored maxlen="
        f"{restored._entries.maxlen}")
    print(f"  ✓ InMemorySink(max_entries=5) → to_dict → "
          f"from_dict preserves maxlen=5")

    # ---------------------------------------------------------------- #
    # P0 2.4 — truncation warning
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: truncation warning ===")
    big_payload = {
        "max_entries": 5,
        "total_emitted": 15_000,
        "entries": [{"i": i} for i in range(15_000)],
    }

    handler = _CaptureHandler()
    logger.addHandler(handler)
    old_level = logger.level
    logger.setLevel(logging.WARNING)
    try:
        restored = InMemorySink.from_dict(big_payload)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)

    assert len(restored.entries) == 5, (
        f"P0 2.4 regression: expected 5 entries, "
        f"got {len(restored.entries)}")
    assert restored.entries == [
        {"i": 14995}, {"i": 14996}, {"i": 14997},
        {"i": 14998}, {"i": 14999},
    ], f"P0 2.4 regression: unexpected entries: {restored.entries}"
    warnings = [
        r for r in handler.records
        if r.levelno == logging.WARNING and "Restoring" in r.getMessage()
    ]
    assert warnings, (
        f"P0 2.4 regression: no truncation warning; "
        f"captured: {[r.getMessage() for r in handler.records]}")
    print(f"  ✓ 15000 payload entries → 5 kept, warning logged: "
          f"{warnings[0].getMessage()[:80]}...")
    print(f"  ✓ kept most-recent 5: {restored.entries}")

    # ---------------------------------------------------------------- #
    # P0 2.6 — JSON-safe validation; datetime becomes ISO
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: JSON-safe validation ===")
    s = InMemorySink()
    now = datetime(2024, 6, 15, 12, 0, 0)
    s.emit({"ts": now, "energy": 0.5})

    stored = s.entries[0]
    assert isinstance(stored["ts"], str), (
        f"P0 2.6 regression: ts is {type(stored['ts']).__name__}, "
        f"expected str")
    assert stored["ts"] == now.isoformat(), (
        f"P0 2.6 regression: ts={stored['ts']!r}, "
        f"expected {now.isoformat()!r}")
    print(f"  ✓ InMemorySink.emit({{'ts': datetime}}) stores "
          f"ISO string: {stored['ts']}")

    # to_dict is JSON-serializable without default=str
    try:
        json.dumps(s.to_dict())  # no default= allowed
    except TypeError as exc:
        raise AssertionError(
            f"P0 2.6 regression: to_dict() is not JSON-safe: {exc}")
    print(f"  ✓ to_dict() is JSON-serializable without default=str")

    # ---------------------------------------------------------------- #
    # P0 2.1 — atomic writes via tempfile; simulated crash on fsync
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.1: atomic writes ===")

    with tempfile.TemporaryDirectory() as td:
        target = Path(td) / "metrics.jsonl"

        # Baseline: two successful emits.
        good = JsonlSink(str(target))
        good.emit({"a": 1})
        good.emit({"b": 2})
        entries_before = good.read_all()
        assert entries_before == [{"a": 1}, {"b": 2}]
        print(f"  ✓ baseline: {len(entries_before)} entries written")

        # Broken sink: fsync fails on its first call (temp fsync).
        calls = {"n": 0}

        def failing_fsync(fd: int) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("simulated crash during temp fsync")
            os.fsync(fd)

        broken = JsonlSink(str(target), fsync=failing_fsync)
        try:
            broken.emit({"c": 3})
        except MetricSinkError as exc:
            print(f"  ✓ emit raised MetricSinkError: {exc}")
        else:
            raise AssertionError(
                "P0 2.1 regression: emit did not raise")

        # Target must be unchanged (no partial line).
        entries_after = broken.read_all()
        assert entries_after == entries_before, (
            f"P0 2.1 regression: target changed after failed "
            f"emit. before={entries_before}, after={entries_after}")
        print(f"  ✓ target unchanged: {entries_after}")

        # No orphan temp files.
        orphans = list(Path(td).glob(".metrics.jsonl.*.tmp"))
        assert orphans == [], (
            f"P0 2.1 regression: orphan temp files: {orphans}")
        print(f"  ✓ no orphan temp files")

        # A second emit with a fresh fsync works.
        good2 = JsonlSink(str(target))
        good2.emit({"d": 4})
        final = good2.read_all()
        assert final == [
            {"a": 1}, {"b": 2}, {"d": 4}
        ], f"unexpected final: {final}"
        print(f"  ✓ subsequent emit appends correctly")

    # ---------------------------------------------------------------- #
    # P0 2.5 — partial last line handling
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: partial last line handling ===")

    with tempfile.TemporaryDirectory() as td:
        target = Path(td) / "metrics.jsonl"

        # Write a file with: 2 complete lines + 1 partial (no \n)
        # The partial line "{"c": 3" is incomplete JSON — the classic
        # "crashed mid-write" shape.
        target.write_text(
            '{"a": 1}\n{"b": 2}\n{"c": 3',
            encoding="utf-8",
        )

        sink = JsonlSink(str(target))

        handler = _CaptureHandler()
        logger.addHandler(handler)
        old_level = logger.level
        logger.setLevel(logging.DEBUG)
        try:
            entries = sink.read_all()
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old_level)

        assert entries == [{"a": 1}, {"b": 2}], (
            f"P0 2.5 regression: entries={entries}")
        print(f"  ✓ returned only complete entries: {entries}")

        # No WARNING should be logged for the partial last line.
        warnings = [
            r for r in handler.records if r.levelno == logging.WARNING
        ]
        assert warnings == [], (
            f"P0 2.5 regression: unexpected WARNINGs: "
            f"{[r.getMessage() for r in warnings]}")
        print(f"  ✓ no WARNING logs")

        # A DEBUG log should note the skipped partial line.
        debugs = [
            r for r in handler.records
            if r.levelno == logging.DEBUG and "partial" in
            r.getMessage().lower()
        ]
        assert debugs, (
            f"P0 2.5 regression: no DEBUG log for partial line; "
            f"captured: {[r.getMessage() for r in handler.records]}")
        print(f"  ✓ DEBUG log: {debugs[0].getMessage()}")

        # Malformed *middle* line (has trailing newline) → WARNING.
        target.write_text(
            '{"a": 1}\nnot-json\n{"b": 2}\n',
            encoding="utf-8",
        )
        handler = _CaptureHandler()
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)
        try:
            entries = sink.read_all()
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old_level)

        assert entries == [{"a": 1}, {"b": 2}]
        warnings = [
            r for r in handler.records if r.levelno == logging.WARNING
        ]
        assert warnings, (
            "P0 2.5 regression: malformed middle line not "
            "logged as WARNING")
        print(f"  ✓ malformed middle line logged at WARNING: "
              f"{warnings[0].getMessage()}")

    # ---------------------------------------------------------------- #
    # P1 3.3 — JsonlSink.statistics
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: JsonlSink.statistics ===")

    with tempfile.TemporaryDirectory() as td:
        target = Path(td) / "metrics.jsonl"
        sink = JsonlSink(str(target))
        sink.emit({"a": 1})
        sink.emit({"b": 2})
        sink.emit({"a": 3})

        stats = sink.statistics()
        assert stats["total_on_disk"] == 3, (
            f"P1 3.3 regression: total_on_disk="
            f"{stats['total_on_disk']}")
        assert stats["total_this_process"] == 3, (
            f"P1 3.3 regression: total_this_process="
            f"{stats['total_this_process']}")
        assert stats["buffered"] == 3
        assert stats["keys_seen"] == {"a": 2, "b": 1}, (
            f"P1 3.3 regression: keys_seen={stats['keys_seen']}")
        assert stats["path"] == str(target)
        print(f"  ✓ statistics: total_on_disk={stats['total_on_disk']}, "
              f"keys_seen={stats['keys_seen']}")

    # ---------------------------------------------------------------- #
    # P1 3.6 — pickling
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: pickle round-trip ===")

    # InMemorySink
    s = InMemorySink(max_entries=5)
    s.emit({"a": 1})
    restored = pickle.loads(pickle.dumps(s))
    assert restored.total_emitted == 1
    assert restored.entries == [{"a": 1}]
    assert restored._lock is not None
    assert restored._entries.maxlen == 5
    # Still usable.
    restored.emit({"b": 2})
    assert restored.total_emitted == 2
    print(f"  ✓ InMemorySink pickle round-trip: "
          f"maxlen={restored._entries.maxlen}, total={restored.total_emitted}")

    # JsonlSink
    with tempfile.TemporaryDirectory() as td:
        target = Path(td) / "metrics.jsonl"
        jsonl = JsonlSink(str(target))
        jsonl.emit({"x": 1})
        restored_jsonl = pickle.loads(pickle.dumps(jsonl))
        assert restored_jsonl.total_emitted == 1
        assert restored_jsonl._lock is not None
        # Still usable.
        restored_jsonl.emit({"y": 2})
        assert restored_jsonl.total_emitted == 2
        on_disk = restored_jsonl.read_all()
        assert on_disk == [{"x": 1}, {"y": 2}]
        print(f"  ✓ JsonlSink pickle round-trip: "
              f"total_this_process={restored_jsonl.total_emitted}, "
              f"on_disk={len(on_disk)}")

    # ---------------------------------------------------------------- #
    # Bonus: context managers, metrics_hook, JSON round-trip
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: context managers / metrics_hook / JSON ===")

    events: List = []
    with InMemorySink(
        metrics_hook=lambda ev, f: events.append((ev, f)),
    ) as s:
        s.emit({"energy": 0.5})
        s.emit({"carbon": 0.1})
    assert len(events) == 2
    assert all(ev == "metric_emitted" for ev, _ in events)
    print(f"  ✓ context manager + metrics_hook: "
          f"{len(events)} events")

    s = InMemorySink(max_entries=3)
    s.emit({"a": 1})
    s.emit({"b": 2})
    payload = s.to_json()
    restored = InMemorySink.from_json(payload)
    assert restored.to_dict() == s.to_dict()
    print(f"  ✓ InMemorySink.to_json / from_json round-trip")

    # ---------------------------------------------------------------- #
    # Backward compat: v1.0.0 callers still work
    # ---------------------------------------------------------------- #
    print("\n=== Backward compat: v1.0.0 API ===")

    # StdoutSink()
    stdout_sink = StdoutSink()
    buf = io.StringIO()
    with redirect_stdout(buf):
        stdout_sink.emit({"x": 1})
    assert "[METRICS]" in buf.getvalue()

    # InMemorySink(max_entries=10_000) — positional
    mem = InMemorySink(10)
    mem.emit({"i": 0})
    assert mem.total_emitted == 1
    assert mem.statistics()["max_entries"] == 10

    # JsonlSink(path) — positional
    with tempfile.TemporaryDirectory() as td:
        jsonl = JsonlSink(str(Path(td) / "m.jsonl"))
        jsonl.emit({"x": 1})
        assert jsonl.read_all() == [{"x": 1}]

    # Validation failure (dict not emitted)
    try:
        mem.emit("not-a-mapping")  # type: ignore[arg-type]
    except MetricSinkError as exc:
        assert "Mapping" in str(exc)
        print(f"  ✓ legacy API preserved; "
              f"non-Mapping rejected")

    print("\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
