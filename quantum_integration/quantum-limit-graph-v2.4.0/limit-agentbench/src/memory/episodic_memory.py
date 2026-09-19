# src/memory/episodic_memory.py

"""
Episodic Memory Module
======================

A lightweight, JSON-backed episodic memory store for agent episodes.
Persists each episode as a dict, keeping only the most recent N episodes.

Enhancements
------------
- ``EpisodeEntry`` — truly immutable: payload wrapped in ``MappingProxyType``,
  hashable, with ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- ``EpisodicMemory`` — thread-safe via a single ``RLock``. Writes occur
  inside the lock so on-disk state can never lag behind in-memory state.
- ``:memory:`` sentinel disables all disk I/O (no stray file is created).
- Atomic file writes (temp file in the same directory + ``os.replace``)
  with retry and linear backoff.
- Non-serializable payloads raise ``EpisodicMemoryInputError`` at
  ``store()`` time — no more silent ``TypeError`` from ``json.dump``.
- ``bool`` rejected for numeric fields; ``NaN``/``inf`` rejected for
  numeric config.
- Structured error hierarchy: ``EpisodicMemoryError`` →
  ``EpisodicMemoryInputError``, ``EpisodicMemoryFileError``,
  ``EpisodicMemoryCorruptionError``.
- Supermemory integration:
  ``to_supermemory_payloads()`` produces dicts that plug directly into
  ``SupermemoryAdapter.remember()`` /
  ``SupermemoryAdapter.remember_many()``; ``from_supermemory_results()``
  does the inverse.
- TTL-aware ``prune()`` keyed on a per-kind ``ttl_seconds`` mapping,
  mirroring ``SupermemoryConfig.ttl_seconds``.
- Truth-level vocabulary and container-tag default, mirroring
  ``SupermemoryConfig``.
- Observability: ``statistics()`` reports counts, errors, last-error
  messages, pruned totals, and uptime.
- ``reset()`` / ``close()`` / context-manager support (reentrant-safe).
- ``__len__`` / ``__contains__`` / ``__iter__`` convenience.
- Full ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- ``__version__`` exported via ``__all__``.
- ``__main__`` smoke test that exercises every new path.

Notes
-----
- Timestamps are emitted as ISO 8601 with a UTC offset. Consumers that
  previously assumed naive local-time strings should switch to
  ``datetime.fromisoformat`` (which handles offsets) or use
  ``_parse_iso_datetime``.
- The validation helpers (``_is_real_int``, ``_is_finite_nonneg``,
  ``_is_positive_finite``) mirror the ones in ``supermemory_config``.
  They are defined locally so this module remains importable without
  the Supermemory SDK.
"""

from __future__ import annotations

import json
import logging
import math
import os
import tempfile
import threading
import time
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "6.0.0"


# --------------------------------------------------------------------------- #
# Backward-compatible module-level constants
# --------------------------------------------------------------------------- #
MEMORY_FILE: str = "memory/memory_store.json"
DEFAULT_MAX_EPISODES: int = 1000
DEFAULT_RECENT_N: int = 10
DEFAULT_WRITE_RETRIES: int = 2

#: Sentinel signalling an in-memory-only store (no disk I/O).
IN_MEMORY_PATH: str = ":memory:"

#: Default accepted truth levels (mirrors ``SupermemoryConfig.truth_levels``).
DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = (
    "measured", "estimated", "simulated", "user-reported",
)

#: Default TTLs per record kind (mirrors ``SupermemoryConfig.ttl_seconds``).
DEFAULT_TTLS: Mapping[str, int] = {
    "decision_outcome": 90 * 24 * 3600,   # 90 days
    "policy":           365 * 24 * 3600,  # 1 year
    "incident":         365 * 24 * 3600,  # 1 year
    "run":              180 * 24 * 3600,  # 180 days
    "grid_forecast":    6 * 3600,         # 6 hours
    "thermal_state":    30 * 60,          # 30 minutes
    "connectivity":     5 * 60,           # 5 minutes
}

#: Default container tag (mirrors ``SupermemoryConfig.default_container_tag``).
DEFAULT_CONTAINER_TAG: str = "org:green-agent"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EpisodicMemoryError(ValueError):
    """Base class for episodic-memory problems."""


class EpisodicMemoryInputError(EpisodicMemoryError):
    """Invalid input to a public API (bad episode, bad n, bad config)."""


class EpisodicMemoryFileError(EpisodicMemoryError):
    """Disk I/O failure (read, write, permission, missing dir)."""


class EpisodicMemoryCorruptionError(EpisodicMemoryError):
    """The backing file is not valid JSON, or entries are malformed."""


# --------------------------------------------------------------------------- #
# Validation helpers — mirror supermemory_config
# --------------------------------------------------------------------------- #
def _is_real_int(value: Any) -> bool:
    """``True`` only for real ints (never for ``bool``)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_nonneg(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _is_positive_finite(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    """Parse an ISO 8601 timestamp; tolerate trailing ``Z`` and epochs."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z") or s.endswith("z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _now_iso() -> str:
    """Return an ISO 8601 timestamp with UTC offset."""
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# Episode entry
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EpisodeEntry:
    """Immutable wrapper around a single stored episode.

    ``payload`` is the caller's original mapping (deep-copied) plus any
    injected metadata. It is exposed as a read-only ``MappingProxyType``.
    """

    timestamp: str
    stored_at: float
    payload: Mapping[str, Any]

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        if not isinstance(self.timestamp, str) or not self.timestamp:
            raise EpisodicMemoryInputError(
                "timestamp must be a non-empty string."
            )
        if not _is_finite_nonneg(self.stored_at):
            raise EpisodicMemoryInputError(
                "stored_at must be a finite non-negative number."
            )
        if not isinstance(self.payload, ABCMapping):
            raise EpisodicMemoryInputError("payload must be a Mapping.")
        object.__setattr__(
            self,
            "payload",
            MappingProxyType(dict(self.payload)),
        )

    # ------------------------------------------------------- accessors
    def get(self, key: str, default: Any = None) -> Any:
        return self.payload.get(key, default)

    @property
    def run_id(self) -> Optional[str]:
        meta = self.payload.get("metadata")
        if isinstance(meta, ABCMapping):
            v = meta.get("run_id")
            if isinstance(v, str) and v:
                return v
        v = self.payload.get("run_id") or self.payload.get("id")
        return v if isinstance(v, str) and v else None

    @property
    def container_tag(self) -> Optional[str]:
        meta = self.payload.get("metadata")
        if isinstance(meta, ABCMapping):
            v = meta.get("container_tag")
            if isinstance(v, str) and v:
                return v
        v = self.payload.get("container_tag")
        return v if isinstance(v, str) and v else None

    @property
    def truth_level(self) -> Optional[str]:
        meta = self.payload.get("metadata")
        if isinstance(meta, ABCMapping):
            v = meta.get("truth_level")
            if isinstance(v, str) and v:
                return v
        v = self.payload.get("truth_level")
        return v if isinstance(v, str) and v else None

    @property
    def kind(self) -> Optional[str]:
        meta = self.payload.get("metadata")
        if isinstance(meta, ABCMapping):
            v = meta.get("kind") or meta.get("record_kind")
            if isinstance(v, str) and v:
                return v
        v = self.payload.get("kind") or self.payload.get("record_kind")
        return v if isinstance(v, str) and v else None

    @property
    def age_seconds(self) -> float:
        return max(0.0, time.time() - self.stored_at)

    # ------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "stored_at": self.stored_at,
            "payload": dict(self.payload),
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(
            self.to_dict(), default=str, indent=indent, sort_keys=True,
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EpisodeEntry":
        if not isinstance(data, ABCMapping):
            raise EpisodicMemoryInputError(
                "EpisodeEntry.from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        # Wrapped form: {"timestamp", "stored_at", "payload"}.
        if "payload" in data and "stored_at" in data:
            payload = data["payload"]
            if not isinstance(payload, ABCMapping):
                raise EpisodicMemoryInputError(
                    "'payload' must be a Mapping."
                )
            return cls(
                timestamp=str(data.get("timestamp") or _now_iso()),
                stored_at=float(data["stored_at"]),
                payload=dict(payload),
            )
        # Legacy form: the entire dict is the payload.
        payload = dict(data)
        return cls(
            timestamp=str(payload.get("timestamp") or _now_iso()),
            stored_at=float(payload.get("stored_at", time.time())),
            payload=payload,
        )

    @classmethod
    def from_json(cls, payload: str) -> "EpisodeEntry":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise EpisodicMemoryInputError(
                f"EpisodeEntry.from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise EpisodicMemoryInputError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data)

    # ------------------------------------------------------- dunder
    def __hash__(self) -> int:
        try:
            payload_hash = hash(tuple(sorted(self.payload.items())))
        except TypeError:
            payload_hash = hash(
                json.dumps(self.payload, default=str, sort_keys=True)
            )
        return hash((self.timestamp, self.stored_at, payload_hash))

    def __repr__(self) -> str:
        return (
            "EpisodeEntry("
            f"timestamp={self.timestamp!r}, "
            f"stored_at={self.stored_at:.3f}, "
            f"keys={sorted(self.payload.keys())})"
        )


# --------------------------------------------------------------------------- #
# Main class
# --------------------------------------------------------------------------- #
class EpisodicMemory:
    """JSON-backed episodic memory store.

    Keeps only the most recent ``max_episodes`` entries. All writes are
    atomic and thread-safe. Corrupt files are handled according to
    ``strict`` mode.

    Parameters
    ----------
    memory_file : str
        Path to the JSON file backing the store. Parent directories are
        created automatically. Pass ``":memory:"`` to disable disk I/O
        entirely.
    max_episodes : int
        Ring-buffer cap; oldest entries dropped when exceeded. Must be
        a positive int (> 0).
    strict : bool
        If True, corrupted files / invalid entries / failed writes
        raise. If False (default), they are logged and skipped.
    autosave : bool
        If True (default), ``store`` / ``clear`` / ``prune`` persist
        immediately.
    write_retries : int
        Number of retries for failed atomic writes (linear backoff).
    ttl_seconds : Mapping[str, int], optional
        Per-kind TTLs (seconds) applied by ``prune()``. Defaults to
        :data:`DEFAULT_TTLS`.
    truth_levels : Iterable[str], optional
        Accepted truth-level vocabulary. Defaults to
        :data:`DEFAULT_TRUTH_LEVELS`.
    default_container_tag : str, optional
        Default container tag used by ``to_supermemory_payloads()``.
    default_truth_level : str, optional
        Truth level assigned to episodes that do not specify one.
    """

    def __init__(
        self,
        memory_file: str = MEMORY_FILE,
        *,
        max_episodes: int = DEFAULT_MAX_EPISODES,
        strict: bool = False,
        autosave: bool = True,
        write_retries: int = DEFAULT_WRITE_RETRIES,
        ttl_seconds: Optional[Mapping[str, int]] = None,
        truth_levels: Optional[Iterable[str]] = None,
        default_container_tag: str = DEFAULT_CONTAINER_TAG,
        default_truth_level: Optional[str] = None,
    ) -> None:
        # ----- validate inputs -----
        if not isinstance(memory_file, str) or not memory_file:
            raise EpisodicMemoryInputError(
                "memory_file must be a non-empty string."
            )
        if not _is_real_int(max_episodes) or max_episodes <= 0:
            raise EpisodicMemoryInputError(
                f"max_episodes must be a positive int (got {max_episodes!r})."
            )
        if not _is_real_int(write_retries) or write_retries < 0:
            raise EpisodicMemoryInputError(
                f"write_retries must be a non-negative int "
                f"(got {write_retries!r})."
            )
        if not isinstance(default_container_tag, str) or not default_container_tag:
            raise EpisodicMemoryInputError(
                "default_container_tag must be a non-empty string."
            )

        # TTLs — validate and freeze.
        ttl_map = dict(DEFAULT_TTLS) if ttl_seconds is None else dict(ttl_seconds)
        for kind, seconds in ttl_map.items():
            if not isinstance(kind, str) or not kind:
                raise EpisodicMemoryInputError(
                    "ttl_seconds keys must be non-empty strings."
                )
            if not _is_real_int(seconds) or seconds <= 0:
                raise EpisodicMemoryInputError(
                    f"ttl_seconds[{kind!r}] must be a positive int "
                    f"(got {seconds!r})."
                )

        # Truth levels — normalize.
        if truth_levels is None:
            levels = tuple(DEFAULT_TRUTH_LEVELS)
        else:
            if isinstance(truth_levels, str) or not isinstance(
                truth_levels, (tuple, list, set, frozenset)
            ):
                raise EpisodicMemoryInputError(
                    "truth_levels must be a sequence of strings."
                )
            normalized: List[str] = []
            seen: set = set()
            for level in truth_levels:
                if not isinstance(level, str) or not level:
                    raise EpisodicMemoryInputError(
                        "truth_levels entries must be non-empty strings."
                    )
                if level in seen:
                    raise EpisodicMemoryInputError(
                        f"truth_levels contains duplicate {level!r}."
                    )
                seen.add(level)
                normalized.append(level)
            if not normalized:
                raise EpisodicMemoryInputError(
                    "truth_levels must be non-empty."
                )
            levels = tuple(normalized)

        if default_truth_level is not None:
            if default_truth_level not in levels:
                raise EpisodicMemoryInputError(
                    f"default_truth_level {default_truth_level!r} not in "
                    f"truth_levels {list(levels)!r}."
                )

        # ----- assign state -----
        self.memory_file: str = memory_file
        self._is_memory_only: bool = (memory_file == IN_MEMORY_PATH)
        self._max_episodes: int = max_episodes
        self._strict: bool = bool(strict)
        self._autosave: bool = bool(autosave)
        self._write_retries: int = write_retries
        self._ttl_seconds: Mapping[str, int] = MappingProxyType(ttl_map)
        self._truth_levels: Tuple[str, ...] = levels
        self._default_container_tag: str = default_container_tag
        self._default_truth_level: Optional[str] = default_truth_level

        self._lock = threading.RLock()
        self._entries: List[EpisodeEntry] = []
        self._ctx_depth: int = 0
        self._ctx_start: Optional[float] = None

        # Counters (guarded by ``_lock``).
        self._write_successes: int = 0
        self._write_errors: int = 0
        self._read_errors: int = 0
        self._total_pruned: int = 0
        self._last_write_error: Optional[str] = None
        self._last_read_error: Optional[str] = None
        self._started_at: float = time.monotonic()

        # ----- initialize backing storage -----
        if not self._is_memory_only:
            path = Path(self.memory_file)
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise EpisodicMemoryFileError(
                    f"could not create parent directory for "
                    f"{self.memory_file!r}: {exc}"
                ) from exc
            if not path.exists():
                self._write([])
                logger.debug("Initialized new memory file at %s", path)
            else:
                self._entries = self._read()

        logger.debug(
            "EpisodicMemory ready "
            "(file=%s, in_memory=%s, max_episodes=%d, strict=%s, count=%d)",
            self.memory_file, self._is_memory_only,
            self._max_episodes, self._strict, len(self._entries),
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

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def autosave(self) -> bool:
        return self._autosave

    @property
    def in_memory(self) -> bool:
        return self._is_memory_only

    @property
    def truth_levels(self) -> Tuple[str, ...]:
        return self._truth_levels

    @property
    def ttl_seconds(self) -> Mapping[str, int]:
        return self._ttl_seconds

    # -------------------------------------------------------------- core API
    def store(
        self,
        episode: Mapping[str, Any],
        *,
        kind: Optional[str] = None,
        truth_level: Optional[str] = None,
        run_id: Optional[str] = None,
        container_tag: Optional[str] = None,
    ) -> EpisodeEntry:
        """Append an episode to memory (returns the stored ``EpisodeEntry``).

        The input mapping is defensively copied. A ``timestamp`` field is
        injected (if absent). Optional keyword arguments populate the
        ``metadata`` sub-mapping using the conventions shared with the
        Supermemory modules (``kind``, ``truth_level``, ``run_id``,
        ``container_tag``).
        """
        if not isinstance(episode, ABCMapping):
            raise EpisodicMemoryInputError(
                f"episode must be a Mapping, got {type(episode).__name__}."
            )

        # Validate truth level up front.
        if truth_level is not None:
            if truth_level not in self._truth_levels:
                raise EpisodicMemoryInputError(
                    f"truth_level {truth_level!r} not in allowed "
                    f"{list(self._truth_levels)!r}."
                )
        else:
            truth_level = self._default_truth_level

        payload: Dict[str, Any] = dict(episode)
        payload.setdefault("timestamp", _now_iso())

        # Metadata injection (shared convention with Supermemory).
        meta = payload.get("metadata")
        if meta is None:
            meta_dict: Dict[str, Any] = {}
        elif isinstance(meta, ABCMapping):
            meta_dict = dict(meta)
        else:
            raise EpisodicMemoryInputError(
                "episode.metadata must be a Mapping when provided."
            )
        if kind is not None:
            meta_dict.setdefault("kind", kind)
        if truth_level is not None:
            meta_dict.setdefault("truth_level", truth_level)
        if run_id is not None:
            meta_dict.setdefault("run_id", run_id)
        if container_tag is not None:
            meta_dict.setdefault("container_tag", container_tag)
        if meta_dict:
            payload["metadata"] = meta_dict

        # Reject non-serializable payloads up front so autosave never
        # partially mutates state.
        try:
            json.dumps(payload)
        except (TypeError, ValueError) as exc:
            raise EpisodicMemoryInputError(
                f"episode payload is not JSON-serializable: {exc}"
            ) from exc

        entry = EpisodeEntry(
            timestamp=str(payload["timestamp"]),
            stored_at=time.time(),
            payload=payload,
        )

        # All state mutation + write happen under the same lock so the
        # on-disk file can never reflect a stale snapshot.
        with self._lock:
            self._entries.append(entry)
            if len(self._entries) > self._max_episodes:
                excess = len(self._entries) - self._max_episodes
                del self._entries[:excess]
            if self._autosave:
                self._write_unlocked([e.to_dict() for e in self._entries])

        logger.debug(
            "Stored episode (total=%d, capped at %d)",
            self.count, self._max_episodes,
        )
        return entry

    def load_all(self) -> List[Dict[str, Any]]:
        """Return all stored episodes as plain dicts (payload form)."""
        with self._lock:
            return [dict(e.payload) for e in self._entries]

    def get_recent(self, n: int = DEFAULT_RECENT_N) -> List[Dict[str, Any]]:
        """Return the ``n`` most recent episodes.

        ``n <= 0`` returns ``[]``. ``n > count`` returns everything.
        """
        if not _is_real_int(n):
            raise EpisodicMemoryInputError(
                f"n must be an int, got {type(n).__name__}."
            )
        if n <= 0:
            return []
        with self._lock:
            return [dict(e.payload) for e in self._entries[-n:]]

    def get(self, run_id: str) -> Optional[EpisodeEntry]:
        """Return the first entry whose ``run_id`` matches, or ``None``."""
        if not isinstance(run_id, str) or not run_id:
            raise EpisodicMemoryInputError(
                "run_id must be a non-empty string."
            )
        with self._lock:
            for entry in self._entries:
                if entry.run_id == run_id:
                    return entry
        return None

    # ----------------------------------------------------------- convenience
    def iter_episodes(self) -> Iterator[Dict[str, Any]]:
        """Iterate over stored episodes (payload form) without loading all."""
        with self._lock:
            snapshot = list(self._entries)
        for entry in snapshot:
            yield dict(entry.payload)

    def clear(self, *, autosave: Optional[bool] = None) -> int:
        """Remove all stored episodes. Returns the number removed."""
        with self._lock:
            removed = len(self._entries)
            if removed == 0:
                return 0
            self._entries.clear()
            do_save = autosave if autosave is not None else self._autosave
            if do_save:
                self._write_unlocked([])
        logger.info("EpisodicMemory cleared (%d entries removed).", removed)
        return removed

    def refresh(self, *, strict: Optional[bool] = None) -> int:
        """Re-read the file from disk. Returns the new entry count."""
        with self._lock:
            self._entries = self._read(strict=strict)
            return len(self._entries)

    def resize(self, max_episodes: int) -> int:
        """Change the ring-buffer cap at runtime. Returns removed count."""
        if not _is_real_int(max_episodes) or max_episodes <= 0:
            raise EpisodicMemoryInputError(
                f"max_episodes must be a positive int (got {max_episodes!r})."
            )
        with self._lock:
            self._max_episodes = max_episodes
            removed = 0
            if len(self._entries) > max_episodes:
                removed = len(self._entries) - max_episodes
                del self._entries[:removed]
                if self._autosave:
                    self._write_unlocked([e.to_dict() for e in self._entries])
        return removed

    # ----------------------------------------------------------- TTL pruning
    def ttl_for(self, kind: str, default: Optional[int] = None) -> int:
        """Return the TTL (seconds) configured for ``kind``.

        If ``kind`` is unknown, ``default`` is returned when provided;
        otherwise an ``EpisodicMemoryInputError`` is raised.
        """
        if kind in self._ttl_seconds:
            return int(self._ttl_seconds[kind])
        if default is None:
            raise EpisodicMemoryInputError(
                f"No TTL configured for kind {kind!r}."
            )
        if not _is_real_int(default) or default <= 0:
            raise EpisodicMemoryInputError(
                "default TTL must be a positive int."
            )
        return int(default)

    def prune(
        self,
        *,
        now: Optional[float] = None,
        autosave: Optional[bool] = None,
    ) -> int:
        """Drop entries whose TTL has expired. Returns number removed.

        Entries without a resolvable ``kind`` are retained (there is no
        TTL to apply). TTLs come from ``ttl_seconds`` (per-kind mapping).
        """
        ts = time.time() if now is None else now
        if not _is_finite_nonneg(ts):
            raise EpisodicMemoryInputError(
                "now must be a finite non-negative number."
            )
        with self._lock:
            kept: List[EpisodeEntry] = []
            removed = 0
            for entry in self._entries:
                kind = entry.kind
                if kind is None:
                    kept.append(entry)
                    continue
                ttl = self._ttl_seconds.get(kind)
                if ttl is None:
                    kept.append(entry)
                    continue
                if ts - entry.stored_at > ttl:
                    removed += 1
                else:
                    kept.append(entry)
            if removed == 0:
                return 0
            self._entries = kept
            self._total_pruned += removed
            do_save = autosave if autosave is not None else self._autosave
            if do_save:
                self._write_unlocked([e.to_dict() for e in self._entries])
        logger.info("EpisodicMemory pruned %d expired entries.", removed)
        return removed

    # -------------------------------------------------- Supermemory bridge
    def to_supermemory_payloads(
        self,
        *,
        container_tag: Optional[str] = None,
        content_key: str = "content",
        default_content: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return payloads shaped for ``SupermemoryAdapter.remember()``.

        Each output dict has ``content``, ``container_tag`` and
        ``metadata``. If an episode lacks ``content``, ``default_content``
        is used when provided; otherwise the full payload is JSON-encoded
        as the content.
        """
        tag = container_tag or self._default_container_tag
        if not isinstance(tag, str) or not tag:
            raise EpisodicMemoryInputError(
                "container_tag must be a non-empty string."
            )
        out: List[Dict[str, Any]] = []
        with self._lock:
            snapshot = list(self._entries)
        for entry in snapshot:
            payload = dict(entry.payload)
            content = payload.get(content_key)
            if content is None:
                if default_content is not None:
                    content = default_content
                else:
                    content = json.dumps(payload, default=str)

            meta = payload.get("metadata")
            meta_dict = dict(meta) if isinstance(meta, ABCMapping) else {}
            meta_dict.setdefault("observed_at", entry.timestamp)
            meta_dict.setdefault("stored_at", entry.stored_at)
            if entry.run_id:
                meta_dict.setdefault("run_id", entry.run_id)
            if entry.truth_level:
                meta_dict.setdefault("truth_level", entry.truth_level)
            if entry.kind:
                meta_dict.setdefault("kind", entry.kind)
            meta_dict.setdefault("container_tag", tag)

            out.append({
                "content": str(content),
                "container_tag": tag,
                "metadata": meta_dict,
            })
        return out

    def to_supermemory_payload(self, entry: EpisodeEntry) -> Dict[str, Any]:
        """Convert a single ``EpisodeEntry`` to a Supermemory payload."""
        if not isinstance(entry, EpisodeEntry):
            raise EpisodicMemoryInputError(
                "to_supermemory_payload expects an EpisodeEntry."
            )
        payload = dict(entry.payload)
        content = payload.get("content")
        if content is None:
            content = json.dumps(payload, default=str)
        meta = payload.get("metadata")
        meta_dict = dict(meta) if isinstance(meta, ABCMapping) else {}
        meta_dict.setdefault("observed_at", entry.timestamp)
        meta_dict.setdefault("stored_at", entry.stored_at)
        if entry.run_id:
            meta_dict.setdefault("run_id", entry.run_id)
        if entry.truth_level:
            meta_dict.setdefault("truth_level", entry.truth_level)
        if entry.kind:
            meta_dict.setdefault("kind", entry.kind)
        meta_dict.setdefault("container_tag", self._default_container_tag)
        return {
            "content": str(content),
            "container_tag": self._default_container_tag,
            "metadata": meta_dict,
        }

    @classmethod
    def from_supermemory_results(
        cls,
        results: Iterable[Mapping[str, Any]],
        *,
        memory_file: str = IN_MEMORY_PATH,
        max_episodes: int = DEFAULT_MAX_EPISODES,
        autosave: bool = False,
    ) -> "EpisodicMemory":
        """Build a store from a sequence of Supermemory-style results.

        Best-effort: preserves ``content``, ``metadata`` and
        ``container_tag`` where present. Entries without a Mapping shape
        are skipped.
        """
        mem = cls(
            memory_file=memory_file,
            max_episodes=max_episodes,
            autosave=autosave,
        )
        for r in results:
            if not isinstance(r, ABCMapping):
                continue
            payload = dict(r)
            meta = payload.get("metadata")
            if isinstance(meta, ABCMapping):
                payload["metadata"] = dict(meta)
            try:
                mem.store(payload)
            except EpisodicMemoryError as exc:
                logger.warning(
                    "from_supermemory_results: skipping malformed entry: %s",
                    exc,
                )
        return mem

    # -------------------------------------------------------------- internals
    def _read(self, *, strict: Optional[bool] = None) -> List[EpisodeEntry]:
        if self._is_memory_only:
            return []
        use_strict = self._strict if strict is None else bool(strict)
        path = Path(self.memory_file)
        if not path.exists():
            return []

        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            with self._lock:
                self._read_errors += 1
                self._last_read_error = str(exc)
            logger.error("Corrupted memory file %s: %s", path, exc)
            if use_strict:
                raise EpisodicMemoryCorruptionError(
                    f"Corrupted memory file: {path}"
                ) from exc
            logger.warning(
                "Starting with empty memory (non-strict mode)."
            )
            return []
        except OSError as exc:
            with self._lock:
                self._read_errors += 1
                self._last_read_error = str(exc)
            logger.error("Could not read memory file %s: %s", path, exc)
            if use_strict:
                raise EpisodicMemoryFileError(
                    f"Could not read memory file: {path}"
                ) from exc
            return []

        if not isinstance(data, list):
            msg = (
                f"Memory file root must be a list, got "
                f"{type(data).__name__}."
            )
            with self._lock:
                self._read_errors += 1
                self._last_read_error = msg
            if use_strict:
                raise EpisodicMemoryCorruptionError(msg)
            logger.warning(msg + " Starting with empty memory.")
            return []

        entries: List[EpisodeEntry] = []
        for i, item in enumerate(data):
            try:
                entries.append(EpisodeEntry.from_dict(item))
            except (EpisodicMemoryError, TypeError, ValueError) as exc:
                with self._lock:
                    self._read_errors += 1
                    self._last_read_error = str(exc)
                if use_strict:
                    raise EpisodicMemoryCorruptionError(
                        f"Malformed entry at index {i}: {exc}"
                    ) from exc
                logger.warning(
                    "Skipping malformed entry at index %d: %s", i, exc,
                )

        # Enforce cap after loading.
        if len(entries) > self._max_episodes:
            entries = entries[-self._max_episodes:]
        return entries

    def _write_unlocked(self, payload: List[Dict[str, Any]]) -> bool:
        """Caller must hold ``self._lock``. Returns True on success."""
        if self._is_memory_only:
            self._write_successes += 1
            return True

        path = Path(self.memory_file)
        attempts = self._write_retries + 1
        last_exc: Optional[BaseException] = None

        for attempt in range(1, attempts + 1):
            try:
                self._atomic_write_once(payload, path)
                self._write_successes += 1
                self._last_write_error = None
                return True
            except (TypeError, ValueError) as exc:
                # Payload not JSON-serializable — retrying is pointless.
                self._write_errors += 1
                self._last_write_error = str(exc)
                logger.error("EpisodicMemory payload not serializable: %s", exc)
                if self._strict:
                    raise EpisodicMemoryInputError(
                        f"payload is not JSON-serializable: {exc}"
                    ) from exc
                return False
            except OSError as exc:
                last_exc = exc
                logger.warning(
                    "EpisodicMemory write attempt %d/%d failed: %s",
                    attempt, attempts, exc,
                )
                if attempt < attempts:
                    time.sleep(0.25 * attempt)

        self._write_errors += 1
        self._last_write_error = str(last_exc)
        logger.error(
            "EpisodicMemory write failed after %d attempt(s): %s",
            attempts, last_exc,
        )
        if self._strict:
            raise EpisodicMemoryFileError(
                f"write failed after {attempts} attempts: {last_exc}"
            ) from last_exc
        return False

    @staticmethod
    def _atomic_write_once(
        payload: List[Dict[str, Any]], path: Path,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp", dir=str(path.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=4)
            os.replace(tmp_path, path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "max_episodes": self._max_episodes,
                "strict": self._strict,
                "autosave": self._autosave,
                "write_retries": self._write_retries,
                "ttl_seconds": dict(self._ttl_seconds),
                "truth_levels": list(self._truth_levels),
                "default_container_tag": self._default_container_tag,
                "default_truth_level": self._default_truth_level,
                "entries": [e.to_dict() for e in self._entries],
            }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(
            self.to_dict(), default=str, indent=indent, sort_keys=True,
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        memory_file: str = IN_MEMORY_PATH,
        autosave: bool = False,
    ) -> "EpisodicMemory":
        if not isinstance(data, ABCMapping):
            raise EpisodicMemoryInputError(
                "EpisodicMemory.from_dict expects a Mapping."
            )
        mem = cls(
            memory_file=memory_file,
            max_episodes=int(data.get("max_episodes", DEFAULT_MAX_EPISODES)),
            strict=bool(data.get("strict", False)),
            autosave=autosave,
            write_retries=int(
                data.get("write_retries", DEFAULT_WRITE_RETRIES)
            ),
            ttl_seconds=data.get("ttl_seconds"),
            truth_levels=data.get("truth_levels"),
            default_container_tag=data.get(
                "default_container_tag", DEFAULT_CONTAINER_TAG,
            ),
            default_truth_level=data.get("default_truth_level"),
        )
        raw_entries = data.get("entries", [])
        if not isinstance(raw_entries, list):
            raise EpisodicMemoryInputError(
                "'entries' must be a list."
            )
        restored: List[EpisodeEntry] = []
        for i, e in enumerate(raw_entries):
            try:
                restored.append(EpisodeEntry.from_dict(e))
            except (EpisodicMemoryError, TypeError, ValueError) as exc:
                if mem._strict:
                    raise EpisodicMemoryCorruptionError(
                        f"Malformed entry at index {i}: {exc}"
                    ) from exc
                logger.warning(
                    "from_dict: skipping malformed entry at index %d: %s",
                    i, exc,
                )
        with mem._lock:
            mem._entries = restored
            if len(mem._entries) > mem._max_episodes:
                del mem._entries[: len(mem._entries) - mem._max_episodes]
        return mem

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        memory_file: str = IN_MEMORY_PATH,
        autosave: bool = False,
    ) -> "EpisodicMemory":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise EpisodicMemoryInputError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise EpisodicMemoryInputError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(
            data, memory_file=memory_file, autosave=autosave,
        )

    # --------------------------------------------------------------- stats
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "file": self.memory_file,
                "in_memory": self._is_memory_only,
                "count": len(self._entries),
                "max_episodes": self._max_episodes,
                "strict": self._strict,
                "autosave": self._autosave,
                "write_retries": self._write_retries,
                "write_successes": self._write_successes,
                "write_errors": self._write_errors,
                "read_errors": self._read_errors,
                "total_pruned": self._total_pruned,
                "last_write_error": self._last_write_error,
                "last_read_error": self._last_read_error,
                "ttl_kinds": sorted(self._ttl_seconds.keys()),
                "truth_levels": list(self._truth_levels),
                "uptime_seconds": time.monotonic() - self._started_at,
            }

    def reset(self, *, clear_entries: bool = True) -> int:
        """Reset counters (and optionally entries).

        Returns the number of entries removed.
        """
        with self._lock:
            removed = len(self._entries) if clear_entries else 0
            if clear_entries:
                self._entries.clear()
            self._write_successes = 0
            self._write_errors = 0
            self._read_errors = 0
            self._total_pruned = 0
            self._last_write_error = None
            self._last_read_error = None
            self._started_at = time.monotonic()
        return removed

    # ---------------------------------------------------------- lifecycle
    def close(self) -> None:
        """Flush pending state to disk. Safe to call multiple times."""
        if self._is_memory_only:
            return
        try:
            with self._lock:
                self._write_unlocked([e.to_dict() for e in self._entries])
        except EpisodicMemoryError:
            logger.exception("close() failed to persist EpisodicMemory.")

    # ---------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.count

    def __contains__(self, item: object) -> bool:
        if not isinstance(item, str):
            return False
        with self._lock:
            for entry in self._entries:
                if entry.run_id == item:
                    return True
                # Also allow matching by memory id (Supermemory-style).
                if str(entry.payload.get("id") or "") == item:
                    return True
        return False

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return self.iter_episodes()

    def __enter__(self) -> "EpisodicMemory":
        with self._lock:
            if self._ctx_depth == 0:
                self._ctx_start = time.perf_counter()
            self._ctx_depth += 1
        logger.debug(
            "Entering scoped EpisodicMemory session (depth=%d).",
            self._ctx_depth,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            self._ctx_depth -= 1
            if self._ctx_depth > 0:
                return  # still nested
            started = self._ctx_start
            self._ctx_start = None
        elapsed = (
            time.perf_counter() - started
            if started is not None
            else 0.0
        )

        if exc_type is not None:
            logger.warning(
                "EpisodicMemory context exited with %s after %.4fs; "
                "not saving.",
                exc_type.__name__, elapsed,
            )
            return

        if self._is_memory_only:
            logger.info(
                "EpisodicMemory in-memory context closed cleanly in "
                "%.4fs (%d entries).",
                elapsed, self.count,
            )
            return

        try:
            with self._lock:
                self._write_unlocked([e.to_dict() for e in self._entries])
        except EpisodicMemoryError:
            logger.exception(
                "Failed to persist EpisodicMemory on context exit."
            )
        finally:
            logger.info(
                "EpisodicMemory context closed cleanly in %.4fs (%d entries).",
                elapsed, self.count,
            )

    def __repr__(self) -> str:
        with self._lock:
            return (
                "EpisodicMemory("
                f"file={self.memory_file!r}, "
                f"entries={len(self._entries)}, "
                f"max_episodes={self._max_episodes}, "
                f"strict={self._strict}, "
                f"in_memory={self._is_memory_only})"
            )


__all__ = [
    "EpisodicMemory",
    "EpisodicMemoryError",
    "EpisodicMemoryInputError",
    "EpisodicMemoryFileError",
    "EpisodicMemoryCorruptionError",
    "EpisodeEntry",
    "MEMORY_FILE",
    "IN_MEMORY_PATH",
    "DEFAULT_MAX_EPISODES",
    "DEFAULT_RECENT_N",
    "DEFAULT_WRITE_RETRIES",
    "DEFAULT_TRUTH_LEVELS",
    "DEFAULT_TTLS",
    "DEFAULT_CONTAINER_TAG",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.episodic_memory
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    import tempfile as _tf

    tmpdir = _tf.mkdtemp(prefix="episodic_memory_smoke_")
    mem_file = os.path.join(tmpdir, "nested", "memory_store.json")

    # ------------------------------------------------------------ basic
    mem = EpisodicMemory(memory_file=mem_file, max_episodes=3)
    print("repr         :", mem)

    for i in range(5):
        mem.store({
            "episode": i, "reward": 0.1 * i, "tag": f"e{i}",
            "content": f"episode {i} on arm_edge",
        }, kind="run", truth_level="measured", run_id=f"run-{i:03d}")

    print("Count        :", mem.count, "(capped at 3)")
    print("Recent (2)   :", mem.get_recent(2))
    print("Iter         :", [e.get("episode") for e in mem])
    assert len(mem) == 3
    assert "run-004" in mem
    assert "run-000" not in mem

    # --------------------------------------------------- frozen entry
    entry = mem.entries[0]
    try:
        entry.payload["episode"] = 999  # type: ignore[index]
    except TypeError:
        print("frozen entry : OK")
    hash(entry)
    print("hashable     : OK")

    # --------------------------------------------------- non-serializable
    try:
        mem.store({"bad": object()})
    except EpisodicMemoryInputError as exc:
        print("reject bad   :", exc)

    # --------------------------------------------------- :memory: mode
    im = EpisodicMemory(memory_file=IN_MEMORY_PATH)
    im.store({"content": "x"}, kind="run", run_id="r1")
    assert im.count == 1
    assert not Path(IN_MEMORY_PATH).exists()
    print("in-memory    : OK (no stray file)")

    # --------------------------------------------------- stats / reset
    print("statistics   :", {
        k: v for k, v in mem.statistics().items()
        if k not in ("truth_levels", "ttl_kinds")
    })
    assert mem.reset(clear_entries=False) == 0
    print("reset(0)     : OK")

    # --------------------------------------------------- TTL pruning
    mem2 = EpisodicMemory(
        memory_file=IN_MEMORY_PATH, max_episodes=100,
        ttl_seconds={"run": 1, "policy": 3600},
    )
    mem2.store({"content": "old run"}, kind="run", run_id="old")
    mem2.store({"content": "new policy"}, kind="policy", run_id="new")
    time.sleep(1.1)
    removed = mem2.prune()
    print("pruned       :", removed, "| remaining:", mem2.count)
    assert removed == 1
    assert mem2.get("new") is not None
    assert mem2.get("old") is None

    # --------------------------------------------------- Supermemory bridge
    payloads = mem.to_supermemory_payloads(container_tag="org:green-agent")
    assert all({"content", "container_tag", "metadata"} <= set(p) for p in payloads)
    print("sm payloads  :", len(payloads), "->", payloads[0]["metadata"].get("run_id"))

    recovered = EpisodicMemory.from_supermemory_results(payloads)
    print("sm reverse   :", recovered.count, "entries")

    # --------------------------------------------------- Serialization
    payload = mem.to_json()
    restored = EpisodicMemory.from_json(payload)
    assert restored.to_dict() == mem.to_dict()
    print("serialize RT : OK")

    # --------------------------------------------------- Corruption
    corrupt_path = os.path.join(tmpdir, "corrupt.json")
    with open(corrupt_path, "w") as f:
        f.write("{not valid json")
    lenient = EpisodicMemory(memory_file=corrupt_path, strict=False)
    print("recover      :", lenient.count, "entries")
    try:
        EpisodicMemory(memory_file=corrupt_path, strict=True)
    except EpisodicMemoryCorruptionError as exc:
        print("strict read  : OK ->", exc)

    # --------------------------------------------------- Validation
    for bad in (
        lambda: mem.store(123),                        # type: ignore[arg-type]
        lambda: mem.store({"x": 1}, truth_level="bogus"),
        lambda: EpisodicMemory(max_episodes=0),
        lambda: EpisodicMemory(max_episodes=True),
        lambda: EpisodicMemory(write_retries=-1),
        lambda: EpisodicMemory(ttl_seconds={"policy": 0}),
        lambda: EpisodicMemory(truth_levels=("a", "a")),
        lambda: EpisodicMemory(default_truth_level="bogus"),
        lambda: mem.get_recent("nope"),                # type: ignore[arg-type]
    ):
        try:
            bad()
        except EpisodicMemoryError as exc:
            print("Rejected     :", exc)

    # --------------------------------------------------- Context manager
    with EpisodicMemory(memory_file=os.path.join(tmpdir, "ctx.json")) as scoped:
        scoped.store({"content": "ctx", "reward": 1.0}, run_id="ctx-1")
    print("ctx mgr      : OK")

    print("\nSmoke test passed.")
