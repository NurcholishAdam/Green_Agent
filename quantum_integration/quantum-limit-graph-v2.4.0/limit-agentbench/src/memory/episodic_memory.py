# metrics/../memory/episodic_memory.py

"""
Episodic Memory Module

A lightweight, JSON-backed episodic memory store for agent episodes.
Persists each episode as a dict, keeping only the most recent N episodes.

Enhancements
------------
- Thread-safe via ``RLock``.
- Configurable ``max_episodes`` ring-buffer (default 1000, same as legacy).
- Immutable ``EpisodeEntry`` dataclass; auto-timestamps every stored episode.
- Automatic parent-directory creation.
- Atomic file writes (temp + ``os.replace``) to avoid corruption.
- Validation: episode must be a Mapping; corrupt files handled per strict mode.
- Structured serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Custom :class:`EpisodicMemoryError`.
- Context-manager support.
- Lazy ``%s`` logging (no more silent failures), ``__repr__``, and a smoke test.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Defaults & module-level constants (backward-compatible)
# --------------------------------------------------------------------------- #
MEMORY_FILE: str = "memory/memory_store.json"
DEFAULT_MAX_EPISODES: int = 1000
DEFAULT_RECENT_N: int = 10


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EpisodicMemoryError(ValueError):
    """Raised for invalid inputs, configuration, or corrupted memory files."""


# --------------------------------------------------------------------------- #
# Entry
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EpisodeEntry:
    """
    Immutable wrapper for a single stored episode.

    ``payload`` holds the caller's original mapping (defensively copied) with
    a ``timestamp`` and ``stored_at`` field injected for auditing.
    """

    timestamp: str
    stored_at: float
    payload: Mapping[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "stored_at": self.stored_at,
            "payload": dict(self.payload),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EpisodeEntry":
        if not isinstance(data, Mapping):
            raise EpisodicMemoryError(
                f"EpisodeEntry.from_dict expects a Mapping, got {type(data).__name__}."
            )
        # Support both new wrapped form and legacy raw-dict form.
        if "payload" in data and "stored_at" in data:
            return cls(
                timestamp=str(data.get("timestamp", _now_iso())),
                stored_at=float(data["stored_at"]),
                payload=dict(data["payload"]),
            )
        # Legacy: episode dict stored inline.
        payload = dict(data)
        return cls(
            timestamp=str(payload.get("timestamp", _now_iso())),
            stored_at=float(payload.get("stored_at", time.time())),
            payload=payload,
        )


# --------------------------------------------------------------------------- #
# Main class
# --------------------------------------------------------------------------- #
class EpisodicMemory:
    """
    JSON-backed episodic memory store.

    Keeps only the most recent ``max_episodes`` entries on disk. All writes
    are atomic and thread-safe. Corrupt files are handled according to
    ``strict`` mode.
    """

    def __init__(
        self,
        memory_file: str = MEMORY_FILE,
        *,
        max_episodes: int = DEFAULT_MAX_EPISODES,
        strict: bool = False,
        autosave: bool = True,
    ) -> None:
        """
        Parameters
        ----------
        memory_file : str
            Path to the JSON file backing the store. Parent directories are
            created automatically.
        max_episodes : int
            Ring-buffer cap; oldest entries dropped when exceeded. Must be > 0.
        strict : bool
            If True, corrupted files / invalid entries raise
            ``EpisodicMemoryError``. If False (default), they are logged and
            skipped (backward-compatible lenient behavior).
        autosave : bool
            If True (default), ``store`` / ``clear`` persist immediately.
        """
        if not isinstance(max_episodes, int) or max_episodes <= 0:
            raise EpisodicMemoryError(
                f"max_episodes must be a positive int, got {max_episodes!r}."
            )
        if not isinstance(memory_file, str) or not memory_file:
            raise EpisodicMemoryError("memory_file must be a non-empty string.")

        self.memory_file: str = memory_file
        self._max_episodes: int = max_episodes
        self._strict: bool = bool(strict)
        self._autosave: bool = bool(autosave)

        self._lock = threading.RLock()
        self._entries: List[EpisodeEntry] = []
        self._ctx_start: Optional[float] = None

        path = Path(self.memory_file)
        path.parent.mkdir(parents=True, exist_ok=True)

        if not path.exists():
            # Match legacy: create the file with an empty list on first use.
            self._write([])
            logger.debug("Initialized new memory file at %s", path)
        else:
            self._entries = self._read()

        logger.debug(
            "EpisodicMemory ready (file=%s, max_episodes=%d, strict=%s, count=%d)",
            self.memory_file,
            self._max_episodes,
            self._strict,
            len(self._entries),
        )

    # ------------------------------------------------------------------ props
    @property
    def max_episodes(self) -> int:
        return self._max_episodes

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    @property
    def entries(self) -> List[EpisodeEntry]:
        with self._lock:
            return list(self._entries)

    # -------------------------------------------------------------- core API
    def store(self, episode: Mapping[str, Any]) -> EpisodeEntry:
        """
        Append an episode to memory (returns the stored ``EpisodeEntry``).

        The input mapping is defensively copied. A ``timestamp`` field is
        injected (if absent) for auditing, and the entry is wrapped with a
        ``stored_at`` epoch time.
        """
        if not isinstance(episode, Mapping):
            raise EpisodicMemoryError(
                f"episode must be a Mapping, got {type(episode).__name__}."
            )

        payload = dict(episode)
        payload.setdefault("timestamp", _now_iso())
        entry = EpisodeEntry(
            timestamp=str(payload["timestamp"]),
            stored_at=time.time(),
            payload=payload,
        )

        with self._lock:
            self._entries.append(entry)
            if len(self._entries) > self._max_episodes:
                # Ring buffer: drop the oldest entries.
                excess = len(self._entries) - self._max_episodes
                del self._entries[:excess]
            snapshot = [e.to_dict() for e in self._entries]

        if self._autosave:
            self._write(snapshot)

        logger.debug(
            "Stored episode (total=%d, capped at %d)",
            len(snapshot),
            self._max_episodes,
        )
        return entry

    def load_all(self) -> List[Dict[str, Any]]:
        """
        Return all stored episodes as plain dicts (payload form).

        Backward-compatible: callers that previously received a ``list[dict]``
        still do.
        """
        with self._lock:
            return [dict(e.payload) for e in self._entries]

    def get_recent(self, n: int = DEFAULT_RECENT_N) -> List[Dict[str, Any]]:
        """
        Return the ``n`` most recent episodes.

        ``n <= 0`` returns ``[]``. ``n > count`` returns everything.
        """
        if not isinstance(n, int):
            raise EpisodicMemoryError(f"n must be an int, got {type(n).__name__}.")
        if n <= 0:
            return []
        with self._lock:
            return [dict(e.payload) for e in self._entries[-n:]]

    # ----------------------------------------------------------- convenience
    def iter_episodes(self) -> Iterator[Dict[str, Any]]:
        """Iterate over stored episodes (payload form) without loading all at once."""
        with self._lock:
            snapshot = list(self._entries)
        for entry in snapshot:
            yield dict(entry.payload)

    def clear(self, *, autosave: Optional[bool] = None) -> None:
        """Remove all stored episodes."""
        with self._lock:
            self._entries.clear()
        if autosave if autosave is not None else self._autosave:
            self._write([])
        logger.info("EpisodicMemory cleared.")

    def refresh(self) -> int:
        """Re-read the file from disk. Returns the new entry count."""
        with self._lock:
            self._entries = self._read()
            return len(self._entries)

    # -------------------------------------------------------------- internals
    def _read(self) -> List[EpisodeEntry]:
        path = Path(self.memory_file)
        if not path.exists():
            return []

        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            logger.error("Corrupted memory file %s: %s", path, exc)
            if self._strict:
                raise EpisodicMemoryError(
                    f"Corrupted memory file: {path}"
                ) from exc
            logger.warning("Starting with empty memory (non-strict mode).")
            return []
        except OSError as exc:
            logger.error("Could not read memory file %s: %s", path, exc)
            if self._strict:
                raise EpisodicMemoryError(
                    f"Could not read memory file: {path}"
                ) from exc
            return []

        if not isinstance(data, list):
            msg = (
                f"Memory file root must be a list, got {type(data).__name__}."
            )
            if self._strict:
                raise EpisodicMemoryError(msg)
            logger.warning(msg + " Starting with empty memory.")
            return []

        entries: List[EpisodeEntry] = []
        for i, item in enumerate(data):
            try:
                entries.append(EpisodeEntry.from_dict(item))
            except (EpisodicMemoryError, TypeError, ValueError) as exc:
                if self._strict:
                    raise EpisodicMemoryError(
                        f"Malformed entry at index {i}: {exc}"
                    ) from exc
                logger.warning("Skipping malformed entry at index %d: %s", i, exc)

        # Enforce cap after loading (in case file was written externally).
        if len(entries) > self._max_episodes:
            entries = entries[-self._max_episodes:]
        return entries

    def _write(self, payload: List[Dict[str, Any]]) -> None:
        """Atomic write: write to temp file in same dir, then ``os.replace``."""
        path = Path(self.memory_file)
        path.parent.mkdir(parents=True, exist_ok=True)

        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=4)
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except OSError as exc:
            logger.error("Could not write memory file %s: %s", path, exc)
            if self._strict:
                raise EpisodicMemoryError(
                    f"Could not write memory file: {path}"
                ) from exc

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "max_episodes": self._max_episodes,
                "strict": self._strict,
                "entries": [e.to_dict() for e in self._entries],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        memory_file: str = ":memory:",
        autosave: bool = False,
    ) -> "EpisodicMemory":
        if not isinstance(data, Mapping):
            raise EpisodicMemoryError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        mem = cls(
            memory_file=memory_file,
            max_episodes=int(data.get("max_episodes", DEFAULT_MAX_EPISODES)),
            strict=bool(data.get("strict", False)),
            autosave=autosave,
        )
        with mem._lock:
            mem._entries = [
                EpisodeEntry.from_dict(e) for e in data.get("entries", [])
            ]
        return mem

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(
        cls, payload: str, *, memory_file: str = ":memory:"
    ) -> "EpisodicMemory":
        try:
            return cls.from_dict(json.loads(payload), memory_file=memory_file)
        except json.JSONDecodeError as exc:
            raise EpisodicMemoryError(f"Invalid JSON payload: {exc}") from exc

    # ----------------------------------------------------------- context mgr
    def __enter__(self) -> "EpisodicMemory":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped EpisodicMemory session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None

        if exc_type is not None:
            logger.warning(
                "EpisodicMemory context exited with %s after %.4fs; not saving.",
                exc_type.__name__,
                elapsed,
            )
            return
        try:
            with self._lock:
                snapshot = [e.to_dict() for e in self._entries]
            self._write(snapshot)
        except EpisodicMemoryError:
            logger.exception("Failed to persist EpisodicMemory on context exit.")
        finally:
            logger.info(
                "EpisodicMemory context closed cleanly in %.4fs (%d entries).",
                elapsed,
                len(self._entries),
            )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "EpisodicMemory("
            f"file={self.memory_file!r}, "
            f"entries={len(self._entries)}, "
            f"max_episodes={self._max_episodes}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Helpers & public API
# --------------------------------------------------------------------------- #
def _now_iso() -> str:
    return datetime.now().isoformat()


__all__ = [
    "EpisodicMemory",
    "EpisodicMemoryError",
    "EpisodeEntry",
    "MEMORY_FILE",
    "DEFAULT_MAX_EPISODES",
    "DEFAULT_RECENT_N",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m memory.episodic_memory
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    import tempfile as _tf

    tmpdir = _tf.mkdtemp(prefix="episodic_memory_smoke_")
    mem_file = os.path.join(tmpdir, "nested", "memory_store.json")

    mem = EpisodicMemory(memory_file=mem_file, max_episodes=3)

    # Store more episodes than max_episodes to verify ring-buffer.
    for i in range(5):
        mem.store({"episode": i, "reward": 0.1 * i, "tag": f"e{i}"})

    print("Count        :", mem.count, "(capped at 3)")
    print("Recent (2)   :", mem.get_recent(2))
    print("All          :", mem.load_all())
    print("Iter         :", list(mem.iter_episodes()))

    # Serialization round-trip
    payload = mem.to_json()
    restored = EpisodicMemory.from_json(payload, memory_file=":memory:")
    assert restored.to_dict() == mem.to_dict()
    print("Serialization round-trip OK.")

    # Context manager
    with EpisodicMemory(memory_file=os.path.join(tmpdir, "ctx.json")) as scoped:
        scoped.store({"episode": "ctx", "reward": 1.0})
    print("Context-managed save OK.")

    # Validation failure
    for bad in (123, "not-a-mapping", None):
        try:
            mem.store(bad)  # type: ignore[arg-type]
        except EpisodicMemoryError as exc:
            print("Rejected as expected:", exc)
        else:  # pragma: no cover
            raise AssertionError(f"Expected rejection for {bad!r}")

    # Corrupt file behavior (non-strict → logs and starts fresh)
    corrupt_path = os.path.join(tmpdir, "corrupt.json")
    with open(corrupt_path, "w") as f:
        f.write("{not valid json")
    mem2 = EpisodicMemory(memory_file=corrupt_path, strict=False)
    print("Recovered from corrupt file with", mem2.count, "entries.")

    print("Final:", mem)
