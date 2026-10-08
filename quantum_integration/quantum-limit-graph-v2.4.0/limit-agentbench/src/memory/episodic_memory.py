# src/memory/episodic_memory.py

"""
Episodic Memory Module
======================

A lightweight, JSON-backed episodic memory store for agent episodes.
Persists each episode as a dict, keeping only the most recent N episodes.

Enhancements
------------
- ``EpisodeEntry`` — **truly** immutable: payload recursively wrapped in
  ``MappingProxyType`` / tuples; hashable via a recursive ``_hashable()``
  helper; carries ``schema_version`` and a non-serialized monotonic
  timestamp for TTL math; exposes ``to_memory_dict()`` /
  ``to_episode_payload()`` bridges for the pipeline.
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
  ``SupermemoryAdapter.remember()`` / ``remember_many()``;
  ``from_supermemory_results()`` does the inverse and preserves the
  source's ``observed_at`` so a round trip is lossless.
- TTL-aware ``prune()`` keyed on a per-kind ``ttl_seconds`` mapping,
  mirroring ``SupermemoryConfig.ttl_seconds``. Uses monotonic time for
  in-process records (NTP-safe) and falls back to wall clock for records
  loaded from disk.
- Truth-level vocabulary and container-tag default, mirroring
  ``SupermemoryConfig``.
- Observability: ``statistics()`` reports counts, errors, unified
  ``last_error``, pruned totals, and uptime.
- ``reset()`` / ``close()`` / sync + async context-manager support
  (reentrant-safe).
- ``__len__`` / ``__contains__`` / ``__getitem__`` / ``__iter__`` plus
  ``contains(record_id, kind=..., container_tag=...)`` for filtered
  lookups.
- ``store_many()`` / ``store_async()`` / ``load_all_async()`` /
  ``from_config()`` / ``from_pipeline()`` / ``from_memory_dicts()``
  helpers.
- Full ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- ``__version__`` and ``SCHEMA_VERSION`` exported via ``__all__``.
- ``__main__`` smoke test that exercises every new path.

Notes
-----
- Timestamps are emitted as ISO 8601 with a UTC offset. Consumers that
  previously assumed naive local-time strings should switch to
  ``datetime.fromisoformat`` (which handles offsets) or use
  ``_parse_iso_datetime``.
- The validation helpers (``_is_real_int``, ``_is_finite_nonneg``,
  ``_is_positive_finite``, ``_deep_freeze``, ``_hashable``) mirror the
  ones used in the patched ``bounded_recall`` module. They are defined
  locally so this module remains importable without the Supermemory SDK.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import tempfile
import threading
import time
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field
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

__version__ = "6.1.0"

#: Version of the episodic-memory contract itself.
SCHEMA_VERSION: int = 1


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
DEFAULT_TTLS: Mapping[str, int] = MappingProxyType({
    "decision_outcome": 90 * 24 * 3600,   # 90 days
    "policy":           365 * 24 * 3600,  # 1 year
    "incident":         365 * 24 * 3600,  # 1 year
    "run":              180 * 24 * 3600,  # 180 days
    "grid_forecast":    6 * 3600,         # 6 hours
    "thermal_state":    30 * 60,          # 30 minutes
    "connectivity":     5 * 60,           # 5 minutes
})

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
# Validation + freeze helpers — mirror bounded_recall
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
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
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


def _deep_freeze(value: Any, *, depth: int = 0) -> Any:
    """Recursively wrap mappings in ``MappingProxyType`` and sequences in
    tuples. Used to make ``EpisodeEntry`` truly immutable.
    """
    if depth > 32:
        return value
    if isinstance(value, ABCMapping):
        return MappingProxyType({
            str(k): _deep_freeze(v, depth=depth + 1)
            for k, v in value.items()
        })
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(v, depth=depth + 1) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_deep_freeze(v, depth=depth + 1) for v in value)
    return value


def _hashable(value: Any, *, depth: int = 0) -> Any:
    """Convert nested mappings to hashable tuples; leaves scalars alone."""
    if depth > 32:
        return "<truncated>"
    if isinstance(value, ABCMapping):
        return tuple(sorted(
            (str(k), _hashable(v, depth=depth + 1))
            for k, v in value.items()
        ))
    if isinstance(value, (list, tuple)):
        return tuple(_hashable(v, depth=depth + 1) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_hashable(v, depth=depth + 1) for v in value)
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    try:
        hash(value)
        return value
    except TypeError:
        return repr(value)


def _safe_meta(entry_or_payload: Any) -> Mapping[str, Any]:
    """Return ``entry_or_payload['metadata']`` if it's a Mapping, else ``{}``."""
    if isinstance(entry_or_payload, EpisodeEntry):  # type: ignore[name-defined]
        container = entry_or_payload.payload
    elif isinstance(entry_or_payload, ABCMapping):
        container = entry_or_payload
    else:
        return {}
    meta = container.get("metadata")
    if not isinstance(meta, ABCMapping):
        return {}
    return meta


# --------------------------------------------------------------------------- #
# Episode entry
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EpisodeEntry:
    """Immutable wrapper around a single stored episode.

    ``payload`` is the caller's original mapping recursively deep-copied
    and wrapped in ``MappingProxyType`` (with nested mappings similarly
    frozen). All accessors return read-only views.
    """

    timestamp: str
    stored_at: float
    payload: Mapping[str, Any]
    schema_version: int = SCHEMA_VERSION
    #: Monotonic baseline used for in-process TTL math. Excluded from
    #: serialization (0.0 means "fall back to wall clock").
    _mono_stored_at: float = field(default=0.0, compare=False, repr=False)

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
        if not _is_finite_nonneg(self._mono_stored_at):
            raise EpisodicMemoryInputError(
                "_mono_stored_at must be a finite non-negative number."
            )
        if not isinstance(self.payload, ABCMapping):
            raise EpisodicMemoryInputError("payload must be a Mapping.")
        object.__setattr__(
            self,
            "payload",
            _deep_freeze(dict(self.payload)),
        )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise EpisodicMemoryInputError(
                "schema_version must be a positive int."
            )

    # ------------------------------------------------------- accessors
    @property
    def id(self) -> str:
        """Stable identifier: ``run_id`` if available, else the timestamp."""
        rid = self.run_id
        if rid is not None:
            return rid
        pid = self.payload.get("id")
        if isinstance(pid, str) and pid:
            return pid
        return f"episode:{self.timestamp}"

    def get(self, key: str, default: Any = None) -> Any:
        return self.payload.get(key, default)

    @property
    def run_id(self) -> Optional[str]:
        meta = _safe_meta(self)
        v = meta.get("run_id")
        if isinstance(v, str) and v:
            return v
        v = self.payload.get("run_id") or self.payload.get("id")
        return v if isinstance(v, str) and v else None

    @property
    def container_tag(self) -> Optional[str]:
        meta = _safe_meta(self)
        v = meta.get("container_tag")
        if isinstance(v, str) and v:
            return v
        v = self.payload.get("container_tag")
        return v if isinstance(v, str) and v else None

    @property
    def truth_level(self) -> Optional[str]:
        meta = _safe_meta(self)
        v = meta.get("truth_level")
        if isinstance(v, str) and v:
            return v
        v = self.payload.get("truth_level")
        return v if isinstance(v, str) and v else None

    @property
    def kind(self) -> Optional[str]:
        meta = _safe_meta(self)
        v = meta.get("kind") or meta.get("record_kind")
        if isinstance(v, str) and v:
            return v
        v = self.payload.get("kind") or self.payload.get("record_kind")
        return v if isinstance(v, str) and v else None

    @property
    def age_seconds(self) -> float:
        return max(0.0, time.time() - self.stored_at)

    # ------------------------------------------------------- TTL
    def is_expired(
        self,
        ttl_seconds: int,
        *,
        now_mono: Optional[float] = None,
        now_wall: Optional[float] = None,
    ) -> bool:
        """Return True if the entry outlived ``ttl_seconds``.

        Prefers the monotonic baseline when present; falls back to wall
        clock for entries reconstructed via ``from_dict``.
        """
        if ttl_seconds <= 0:
            return False
        if self._mono_stored_at > 0:
            mono = time.monotonic() if now_mono is None else now_mono
            return (mono - self._mono_stored_at) >= ttl_seconds
        wall = time.time() if now_wall is None else now_wall
        return (wall - self.stored_at) >= ttl_seconds

    # ------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
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
                schema_version=int(data.get("schema_version", SCHEMA_VERSION)),
            )
        # Legacy form: the entire dict is the payload.
        payload = dict(data)
        return cls(
            timestamp=str(payload.get("timestamp") or _now_iso()),
            stored_at=float(payload.get("stored_at", time.time())),
            payload=payload,
            schema_version=SCHEMA_VERSION,
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

    # ------------------------------------------------------- bridges
    def _render_content(self) -> str:
        meta = _safe_meta(self)
        lines = [f"Episode {self.id} (kind={self.kind or 'unknown'})"]
        if self.truth_level:
            lines.append(f"  truth_level: {self.truth_level}")
        if meta.get("container_tag"):
            lines.append(f"  container_tag: {meta['container_tag']}")
        lines.append(f"  timestamp: {self.timestamp}")
        lines.append(
            f"  payload_keys: {sorted(str(k) for k in self.payload.keys())}"
        )
        return "\n".join(lines)

    def _metadata(self, *, container_tag: str) -> Dict[str, Any]:
        meta = dict(_safe_meta(self))
        meta.setdefault("kind", self.kind or "episode")
        meta.setdefault("record_id", self.id)
        if self.run_id:
            meta.setdefault("run_id", self.run_id)
        if self.truth_level:
            meta.setdefault("truth_level", self.truth_level)
        meta.setdefault("container_tag", container_tag)
        meta.setdefault("observed_at", self.timestamp)
        meta.setdefault("stored_at", self.stored_at)
        meta.setdefault("schema_version", self.schema_version)
        return meta

    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
        content_key: str = "content",
    ) -> Dict[str, Any]:
        """Return a payload shaped for ``EpisodicMemory.store`` /
        ``SupermemoryAdapter.remember``.
        """
        tag = container_tag or self.container_tag or DEFAULT_CONTAINER_TAG
        if not isinstance(tag, str) or not tag:
            raise EpisodicMemoryInputError(
                "container_tag must be a non-empty string."
            )
        if content is None:
            raw_content = self.payload.get(content_key)
            if isinstance(raw_content, str) and raw_content:
                content = raw_content
            else:
                content = self._render_content()
        return {
            "content": content,
            "container_tag": tag,
            "metadata": self._metadata(container_tag=tag),
        }

    def to_memory_dict(self) -> Dict[str, Any]:
        """Return ``{"id", "content", "metadata"}`` for ``BoundedRecall``."""
        return {
            "id": self.id,
            "content": self._render_content(),
            "metadata": self._metadata(
                container_tag=self.container_tag or DEFAULT_CONTAINER_TAG,
            ),
        }

    # ------------------------------------------------------- dunder
    def __hash__(self) -> int:
        return hash((
            self.timestamp,
            self.stored_at,
            _hashable(self.payload),
            self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            "EpisodeEntry("
            f"timestamp={self.timestamp!r}, "
            f"stored_at={self.stored_at:.3f}, "
            f"kind={self.kind!r}, "
            f"keys={sorted(str(k) for k in self.payload.keys())})"
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
        Default container tag used by ``to_supermemory_payloads()`` and
        auto-populated into entry metadata at ``store()`` time.
    default_truth_level : str, optional
        Truth level assigned to episodes that do not specify one.
    persist_truncation_on_read : bool
        If True, ``_read()`` rewrites the file when it trims entries to
        ``max_episodes``. Default False (in-memory only).
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
        persist_truncation_on_read: bool = False,
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
        if not isinstance(persist_truncation_on_read, bool):
            raise EpisodicMemoryInputError(
                "persist_truncation_on_read must be a bool."
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
        self._persist_truncation_on_read: bool = persist_truncation_on_read

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
        self._last_error: Optional[str] = None
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
                if self._persist_truncation_on_read:
                    with self._lock:
                        self._write_unlocked(
                            [e.to_dict() for e in self._entries]
                        )

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

    @property
    def default_container_tag(self) -> str:
        return self._default_container_tag

    # -------------------------------------------------------------- core API
    def store(
        self,
        episode: Mapping[str, Any],
        *,
        kind: Optional[str] = None,
        truth_level: Optional[str] = None,
        run_id: Optional[str] = None,
        container_tag: Optional[str] = None,
        timestamp: Optional[str] = None,
        stored_at: Optional[float] = None,
    ) -> EpisodeEntry:
        """Append an episode to memory (returns the stored ``EpisodeEntry``).

        The input mapping is defensively copied. A ``timestamp`` field is
        injected if absent; a caller-supplied ``timestamp`` (either via
        the payload or the ``timestamp=`` kwarg) is preserved. Optional
        keyword arguments populate the ``metadata`` sub-mapping using the
        conventions shared with the Supermemory modules.
        """
        if not isinstance(episode, ABCMapping):
            raise EpisodicMemoryInputError(
                f"episode must be a Mapping, got {type(episode).__name__}."
            )

        # ---- validate optional metadata args ----
        for name, val in (
            ("kind", kind),
            ("run_id", run_id),
            ("container_tag", container_tag),
        ):
            if val is not None and (not isinstance(val, str) or not val):
                raise EpisodicMemoryInputError(
                    f"{name} must be None or a non-empty string."
                )
        if timestamp is not None:
            if not isinstance(timestamp, str) or not timestamp:
                raise EpisodicMemoryInputError(
                    "timestamp must be None or a non-empty string."
                )
        if stored_at is not None:
            if not _is_finite_nonneg(stored_at):
                raise EpisodicMemoryInputError(
                    "stored_at must be None or a finite number >= 0."
                )

        # ---- truth level ----
        if truth_level is not None:
            if truth_level not in self._truth_levels:
                raise EpisodicMemoryInputError(
                    f"truth_level {truth_level!r} not in allowed "
                    f"{list(self._truth_levels)!r}."
                )
        else:
            truth_level = self._default_truth_level

        # ---- payload prep ----
        payload: Dict[str, Any] = dict(episode)
        if timestamp is not None:
            payload["timestamp"] = timestamp
        payload.setdefault("timestamp", _now_iso())

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
        # Always populate container_tag: caller arg > existing meta > default.
        resolved_tag = (
            container_tag
            or meta_dict.get("container_tag")
            or self._default_container_tag
        )
        if not isinstance(resolved_tag, str) or not resolved_tag:
            raise EpisodicMemoryInputError(
                "container_tag must be a non-empty string."
            )
        meta_dict.setdefault("container_tag", resolved_tag)
        meta_dict.setdefault("schema_version", SCHEMA_VERSION)
        if meta_dict:
            payload["metadata"] = meta_dict

        # Reject non-serializable payloads up front.
        try:
            json.dumps(payload, default=str)
        except (TypeError, ValueError) as exc:
            raise EpisodicMemoryInputError(
                f"episode payload is not JSON-serializable: {exc}"
            ) from exc

        wall_stored_at = time.time() if stored_at is None else float(stored_at)
        mono_stored_at = time.monotonic()

        entry = EpisodeEntry(
            timestamp=str(payload["timestamp"]),
            stored_at=wall_stored_at,
            payload=payload,
            schema_version=SCHEMA_VERSION,
            _mono_stored_at=mono_stored_at,
        )

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

    def store_many(
        self,
        episodes: Iterable[Mapping[str, Any]],
        *,
        stop_on_error: bool = False,
    ) -> List[EpisodeEntry]:
        """Store several episodes. Returns the stored entries."""
        out: List[EpisodeEntry] = []
        for idx, ep in enumerate(episodes):
            try:
                out.append(self.store(ep))
            except EpisodicMemoryError as exc:
                if stop_on_error:
                    raise
                logger.warning("store_many[%d] failed: %s", idx, exc)
                with self._lock:
                    self._last_error = f"store_many[{idx}]: {exc}"
        return out

    async def store_async(
        self,
        episode: Mapping[str, Any],
        **kwargs: Any,
    ) -> EpisodeEntry:
        return await asyncio.to_thread(self.store, episode, **kwargs)

    def load_all(self) -> List[Mapping[str, Any]]:
        """Return all stored episodes as read-only mappings."""
        with self._lock:
            snapshot = list(self._entries)
        return [_deep_freeze(dict(e.payload)) for e in snapshot]

    async def load_all_async(self) -> List[Mapping[str, Any]]:
        return await asyncio.to_thread(self.load_all)

    def get_recent(self, n: int = DEFAULT_RECENT_N) -> List[Mapping[str, Any]]:
        """Return the ``n`` most recent episodes as read-only mappings.

        ``n <= 0`` returns ``[]``. ``n > count`` returns everything.
        """
        if not _is_real_int(n):
            raise EpisodicMemoryInputError(
                f"n must be an int, got {type(n).__name__}."
            )
        if n <= 0:
            return []
        with self._lock:
            snapshot = list(self._entries[-n:])
        return [_deep_freeze(dict(e.payload)) for e in snapshot]

    def get_recent_entries(
        self, n: int = DEFAULT_RECENT_N,
    ) -> List[EpisodeEntry]:
        """Return the ``n`` most recent ``EpisodeEntry`` objects."""
        if not _is_real_int(n):
            raise EpisodicMemoryInputError(
                f"n must be an int, got {type(n).__name__}."
            )
        if n <= 0:
            return []
        with self._lock:
            return list(self._entries[-n:])

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

    def __getitem__(self, run_id: str) -> EpisodeEntry:
        """Look up an entry by ``run_id``; raises ``KeyError`` if missing."""
        entry = self.get(run_id)
        if entry is None:
            raise KeyError(f"No episode with run_id={run_id!r}.")
        return entry

    def contains(
        self,
        record_id: Optional[str] = None,
        *,
        kind: Optional[str] = None,
        container_tag: Optional[str] = None,
        truth_level: Optional[str] = None,
    ) -> bool:
        """Return True if any stored entry matches the filters.

        ``record_id`` (positional) matches against ``run_id`` or ``id``.
        All filters are optional; passing none returns ``True`` if the
        store is non-empty.
        """
        if record_id is not None and (
            not isinstance(record_id, str) or not record_id
        ):
            raise EpisodicMemoryInputError(
                "record_id must be None or a non-empty string."
            )
        with self._lock:
            for entry in self._entries:
                if record_id is not None and (
                    entry.run_id != record_id
                    and str(entry.payload.get("id") or "") != record_id
                ):
                    continue
                if kind is not None and entry.kind != kind:
                    continue
                if (
                    container_tag is not None
                    and entry.container_tag != container_tag
                ):
                    continue
                if truth_level is not None and entry.truth_level != truth_level:
                    continue
                return True
        return False

    def count_by_kind(self) -> Dict[str, int]:
        with self._lock:
            out: Dict[str, int] = {}
            for entry in self._entries:
                k = entry.kind or "unknown"
                out[k] = out.get(k, 0) + 1
            return out

    def count_by_container_tag(self) -> Dict[str, int]:
        with self._lock:
            out: Dict[str, int] = {}
            for entry in self._entries:
                k = entry.container_tag or "unknown"
                out[k] = out.get(k, 0) + 1
            return out

    def count_by_truth_level(self) -> Dict[str, int]:
        with self._lock:
            out: Dict[str, int] = {}
            for entry in self._entries:
                k = entry.truth_level or "unknown"
                out[k] = out.get(k, 0) + 1
            return out

    # ----------------------------------------------------------- convenience
    def iter_episodes(self) -> Iterator[Mapping[str, Any]]:
        """Iterate over stored episodes (read-only mapping form)."""
        with self._lock:
            snapshot = list(self._entries)
        for entry in snapshot:
            yield _deep_freeze(dict(entry.payload))

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
        now_mono: Optional[float] = None,
        now_wall: Optional[float] = None,
        autosave: Optional[bool] = None,
    ) -> int:
        """Drop entries whose TTL has expired. Returns number removed.

        Entries without a resolvable ``kind`` are retained (there is no
        TTL to apply). TTLs come from ``ttl_seconds`` (per-kind mapping).
        Uses monotonic time for in-process records; falls back to wall
        clock for records loaded from disk.
        """
        if now_mono is None:
            now_mono = time.monotonic()
        elif not _is_finite_nonneg(now_mono):
            raise EpisodicMemoryInputError(
                "now_mono must be a finite non-negative number."
            )
        if now_wall is None:
            now_wall = time.time()
        elif not _is_finite_nonneg(now_wall):
            raise EpisodicMemoryInputError(
                "now_wall must be a finite non-negative number."
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
                if entry.is_expired(
                    ttl, now_mono=now_mono, now_wall=now_wall,
                ):
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

        Each output dict carries the entry's own container tag when it
        has one; otherwise the override; otherwise the store default.
        """
        override = container_tag
        if override is not None and (
            not isinstance(override, str) or not override
        ):
            raise EpisodicMemoryInputError(
                "container_tag must be None or a non-empty string."
            )
        out: List[Dict[str, Any]] = []
        with self._lock:
            snapshot = list(self._entries)
        for entry in snapshot:
            # Resolve per-entry tag: entry meta > override > default.
            entry_tag = entry.container_tag or override or self._default_container_tag
            if not isinstance(entry_tag, str) or not entry_tag:
                entry_tag = self._default_container_tag

            payload = dict(entry.payload)
            content = payload.get(content_key)
            if content is None:
                if default_content is not None:
                    content = default_content
                else:
                    content = json.dumps(payload, default=str)

            meta_dict = dict(_safe_meta(entry))
            meta_dict.setdefault("observed_at", entry.timestamp)
            meta_dict.setdefault("stored_at", entry.stored_at)
            if entry.run_id:
                meta_dict.setdefault("run_id", entry.run_id)
            if entry.truth_level:
                meta_dict.setdefault("truth_level", entry.truth_level)
            if entry.kind:
                meta_dict.setdefault("kind", entry.kind)
            meta_dict.setdefault("record_id", entry.id)
            meta_dict["container_tag"] = entry_tag
            meta_dict.setdefault("schema_version", entry.schema_version)

            out.append({
                "content": str(content),
                "container_tag": entry_tag,
                "metadata": meta_dict,
            })
        return out

    def to_supermemory_payload(self, entry: EpisodeEntry) -> Dict[str, Any]:
        """Convert a single ``EpisodeEntry`` to a Supermemory payload."""
        if not isinstance(entry, EpisodeEntry):
            raise EpisodicMemoryInputError(
                "to_supermemory_payload expects an EpisodeEntry."
            )
        return entry.to_episode_payload()

    def to_memory_dicts(self) -> List[Dict[str, Any]]:
        """Return entries shaped for ``BoundedRecall`` / ``RecallBundle``."""
        with self._lock:
            snapshot = list(self._entries)
        return [e.to_memory_dict() for e in snapshot]

    @classmethod
    def from_supermemory_results(
        cls,
        results: Iterable[Mapping[str, Any]],
        *,
        memory_file: str = IN_MEMORY_PATH,
        max_episodes: int = DEFAULT_MAX_EPISODES,
        autosave: bool = False,
        strict: bool = False,
    ) -> "EpisodicMemory":
        """Build a store from a sequence of Supermemory-style results.

        Preserves the source's ``observed_at`` / ``timestamp`` so a
        Supermemory→Episodic→Supermemory round trip is lossless.
        """
        mem = cls(
            memory_file=memory_file,
            max_episodes=max_episodes,
            autosave=autosave,
            strict=strict,
        )
        for r in results:
            if not isinstance(r, ABCMapping):
                continue
            payload = dict(r)
            meta = payload.get("metadata")
            if isinstance(meta, ABCMapping):
                payload["metadata"] = dict(meta)
                meta_dict = payload["metadata"]
            else:
                meta_dict = {}

            # Preserve the source's observed_at (or timestamp) as the
            # new entry's top-level timestamp.
            source_ts = (
                meta_dict.get("observed_at")
                or meta_dict.get("timestamp")
                or payload.get("timestamp")
            )
            if isinstance(source_ts, str) and source_ts:
                payload["timestamp"] = source_ts

            source_tag = (
                meta_dict.get("container_tag")
                or payload.get("container_tag")
            )
            kind = meta_dict.get("kind")
            truth_level = meta_dict.get("truth_level")
            run_id = meta_dict.get("run_id")

            kwargs: Dict[str, Any] = {}
            if isinstance(source_tag, str) and source_tag:
                kwargs["container_tag"] = source_tag
            if isinstance(kind, str) and kind:
                kwargs["kind"] = kind
            if isinstance(truth_level, str) and truth_level:
                kwargs["truth_level"] = truth_level
            if isinstance(run_id, str) and run_id:
                kwargs["run_id"] = run_id

            try:
                mem.store(payload, **kwargs)
            except EpisodicMemoryError as exc:
                logger.warning(
                    "from_supermemory_results: skipping malformed entry: %s",
                    exc,
                )
        return mem

    @classmethod
    def from_memory_dicts(
        cls,
        entries: Iterable[Mapping[str, Any]],
        *,
        memory_file: str = IN_MEMORY_PATH,
        max_episodes: int = DEFAULT_MAX_EPISODES,
        autosave: bool = False,
        strict: bool = False,
    ) -> "EpisodicMemory":
        """Build a store from ``{"id", "content", "metadata"}`` shapes."""
        mem = cls(
            memory_file=memory_file,
            max_episodes=max_episodes,
            autosave=autosave,
            strict=strict,
        )
        for e in entries:
            if not isinstance(e, ABCMapping):
                continue
            meta = e.get("metadata")
            meta_dict = dict(meta) if isinstance(meta, ABCMapping) else {}
            payload: Dict[str, Any] = dict(meta_dict)
            payload["content"] = e.get("content")
            if e.get("id") is not None:
                payload.setdefault("run_id", e["id"])
            try:
                mem.store(
                    payload,
                    kind=meta_dict.get("kind"),
                    truth_level=meta_dict.get("truth_level"),
                    container_tag=meta_dict.get("container_tag"),
                    run_id=meta_dict.get("run_id"),
                )
            except EpisodicMemoryError as exc:
                logger.warning(
                    "from_memory_dicts: skipping malformed entry: %s", exc,
                )
        return mem

    # ---------------------------------------------------- constructors
    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any],
        *,
        memory_file: str = IN_MEMORY_PATH,
        autosave: bool = False,
        auto_load: bool = False,
    ) -> "EpisodicMemory":
        """Build a store from a config mapping (the ``to_dict()`` shape)."""
        if not isinstance(config, ABCMapping):
            raise EpisodicMemoryInputError(
                "config must be a Mapping."
            )
        return cls(
            memory_file=memory_file,
            max_episodes=int(config.get("max_episodes", DEFAULT_MAX_EPISODES)),
            strict=bool(config.get("strict", False)),
            autosave=autosave,
            write_retries=int(
                config.get("write_retries", DEFAULT_WRITE_RETRIES)
            ),
            ttl_seconds=config.get("ttl_seconds"),
            truth_levels=config.get("truth_levels"),
            default_container_tag=str(
                config.get("default_container_tag", DEFAULT_CONTAINER_TAG)
            ),
            default_truth_level=config.get("default_truth_level"),
            persist_truncation_on_read=bool(
                config.get("persist_truncation_on_read", False)
            ),
        )

    @classmethod
    def from_pipeline(
        cls,
        pipeline: Any,
        **kwargs: Any,
    ) -> "EpisodicMemory":
        """Return ``pipeline.episodic`` if present, else build a fresh one."""
        episodic = getattr(pipeline, "episodic", None)
        if isinstance(episodic, cls):
            return episodic
        return cls(**kwargs)

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
                self._last_error = str(exc)
            logger.error("Corrupted memory file %s: %s", path, exc)
            if use_strict:
                raise EpisodicMemoryCorruptionError(
                    f"Corrupted memory file: {path}"
                ) from exc
            logger.warning("Starting with empty memory (non-strict mode).")
            return []
        except OSError as exc:
            with self._lock:
                self._read_errors += 1
                self._last_read_error = str(exc)
                self._last_error = str(exc)
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
                self._last_error = msg
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
                    self._last_error = str(exc)
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
                self._write_errors += 1
                self._last_write_error = str(exc)
                self._last_error = str(exc)
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
        self._last_error = str(last_exc)
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
                json.dump(payload, f, indent=4, default=str)
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
                "schema_version": SCHEMA_VERSION,
                "max_episodes": self._max_episodes,
                "strict": self._strict,
                "autosave": self._autosave,
                "write_retries": self._write_retries,
                "ttl_seconds": dict(self._ttl_seconds),
                "truth_levels": list(self._truth_levels),
                "default_container_tag": self._default_container_tag,
                "default_truth_level": self._default_truth_level,
                "persist_truncation_on_read": self._persist_truncation_on_read,
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
        auto_load: bool = False,
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
            default_container_tag=str(
                data.get("default_container_tag", DEFAULT_CONTAINER_TAG)
            ),
            default_truth_level=data.get("default_truth_level"),
            persist_truncation_on_read=bool(
                data.get("persist_truncation_on_read", False)
            ),
        )
        raw_entries = data.get("entries", [])
        if not isinstance(raw_entries, list):
            raise EpisodicMemoryInputError("'entries' must be a list.")
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
        auto_load: bool = False,
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
            data, memory_file=memory_file,
            autosave=autosave, auto_load=auto_load,
        )

    # --------------------------------------------------------------- stats
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "schema_version": SCHEMA_VERSION,
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
                "last_error": self._last_error,
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
            self._last_error = None
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
        return self.contains(item)

    def __iter__(self) -> Iterator[Mapping[str, Any]]:
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
                "EpisodicMemory context closed cleanly in %.4fs "
                "(%d entries).",
                elapsed, self.count,
            )

    async def __aenter__(self) -> "EpisodicMemory":
        return self.__enter__()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.__exit__(exc_type, exc, tb)

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
    "SCHEMA_VERSION",
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

    # ------------------------------------------------------------ 1. Basic
    mem = EpisodicMemory(memory_file=mem_file, max_episodes=3)
    print("repr         :", mem)

    for i in range(5):
        mem.store({
            "episode": i, "reward": 0.1 * i, "tag": f"e{i}",
            "content": f"episode {i} on arm_edge",
        }, kind="run", truth_level="measured", run_id=f"run-{i:03d}")

    print("Count        :", mem.count, "(capped at 3)")
    assert len(mem) == 3
    assert "run-004" in mem
    assert "run-000" not in mem

    # ------------------------------------------------------------ 2. Deep freeze (bug 1)
    entry = mem.entries[0]
    assert entry.payload["metadata"]["kind"] == "run"
    try:
        entry.payload["metadata"]["kind"] = "hacked"  # type: ignore[index]
    except TypeError:
        print("deep-frozen  : OK")
    else:
        raise AssertionError("nested metadata should be frozen")

    # ------------------------------------------------------------ 3. Hash (bug 2)
    h1 = hash(entry)
    h2 = hash(entry)
    assert h1 == h2
    {entry}  # must not raise
    print("hashable     : OK")

    # ------------------------------------------------------------ 4. Return types (bug 3)
    loaded = mem.load_all()
    try:
        loaded[0]["metadata"]["kind"] = "hacked"  # type: ignore[index]
    except TypeError:
        print("load_all ro  : OK")
    else:
        raise AssertionError("load_all should return frozen views")
    recent = mem.get_recent(2)
    try:
        recent[0]["metadata"]["container_tag"] = "hacked"  # type: ignore[index]
    except TypeError:
        print("get_recent ro: OK")
    else:
        raise AssertionError("get_recent should return frozen views")

    # ------------------------------------------------------------ 5. store() validation (bug 4)
    for bad_kwargs in (
        dict(kind=123),
        dict(run_id=""),
        dict(container_tag=b""),
    ):
        try:
            mem.store({"x": 1}, **bad_kwargs)  # type: ignore[arg-type]
        except EpisodicMemoryInputError as exc:
            print("reject meta  :", exc)
        else:
            raise AssertionError(f"expected rejection for {bad_kwargs!r}")

    # ------------------------------------------------------------ 6. container_tag auto-populate (bug 7)
    mem.store({"x": 1}, run_id="auto-tag")
    auto_entry = mem.get("auto-tag")
    assert auto_entry is not None
    assert auto_entry.container_tag == DEFAULT_CONTAINER_TAG
    print("auto tag     : OK")

    # ------------------------------------------------------------ 7. to_supermemory_payloads per-entry tag (bug 5)
    tagged_mem = EpisodicMemory(
        memory_file=IN_MEMORY_PATH, autosave=False,
    )
    tagged_mem.store(
        {"content": "tagged", "x": 1},
        container_tag="org:custom",
    )
    payloads = tagged_mem.to_supermemory_payloads()
    assert payloads[0]["container_tag"] == "org:custom"
    assert payloads[0]["metadata"]["container_tag"] == "org:custom"
    print("per-entry tag: OK")

    # ------------------------------------------------------------ 8. from_supermemory_results preserves observed_at (bug 6)
    source_ts = "2024-01-01T00:00:00+00:00"
    src_payload = {
        "content": "x",
        "container_tag": "org:src",
        "metadata": {
            "observed_at": source_ts,
            "container_tag": "org:src",
            "kind": "run",
            "run_id": "orig-1",
        },
    }
    restored = EpisodicMemory.from_supermemory_results([src_payload])
    restored_entry = restored.get("orig-1")
    assert restored_entry is not None
    assert restored_entry.timestamp == source_ts
    print("observed_at preserved : OK")

    # ------------------------------------------------------------ 9. SCHEMA_VERSION (bug 8)
    assert SCHEMA_VERSION == 1
    assert entry.schema_version == SCHEMA_VERSION
    assert mem.statistics()["schema_version"] == SCHEMA_VERSION
    print("schema ver   : OK")

    # ------------------------------------------------------------ 10. Non-serializable
    try:
        mem.store({"bad": object()})
    except EpisodicMemoryInputError as exc:
        print("reject bad   :", exc)

    # ------------------------------------------------------------ 11. :memory: mode
    im = EpisodicMemory(memory_file=IN_MEMORY_PATH)
    im.store({"content": "x"}, kind="run", run_id="r1")
    assert im.count == 1
    assert not Path(IN_MEMORY_PATH).exists()
    print("in-memory    : OK (no stray file)")

    # ------------------------------------------------------------ 12. TTL pruning
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

    # ------------------------------------------------------------ 13. Bridges
    bridge_entry = mem.entries[0]
    md = bridge_entry.to_memory_dict()
    assert {"id", "content", "metadata"} <= set(md)
    ep = bridge_entry.to_episode_payload()
    assert {"content", "container_tag", "metadata"} <= set(ep)
    assert ep["metadata"]["schema_version"] == SCHEMA_VERSION
    print("bridges      : OK")

    # ------------------------------------------------------------ 14. store_many
    many = EpisodicMemory(memory_file=IN_MEMORY_PATH, autosave=False)
    stored = many.store_many([
        {"content": "a", "run_id": "a1"},
        {"content": "b", "run_id": "b1"},
    ])
    assert len(stored) == 2
    assert many.count == 2
    print("store_many   : OK")

    # ------------------------------------------------------------ 15. from_config / from_pipeline
    cfg = mem.to_dict()
    rebuilt = EpisodicMemory.from_config(cfg)
    assert rebuilt.max_episodes == mem.max_episodes
    class _FakePipeline:
        episodic = mem
    recovered = EpisodicMemory.from_pipeline(_FakePipeline())
    assert recovered is mem
    print("from_*       : OK")

    # ------------------------------------------------------------ 16. contains / count helpers
    assert mem.contains(kind="run")
    assert mem.contains(container_tag=DEFAULT_CONTAINER_TAG)
    assert mem.contains(truth_level="measured")
    cbt = mem.count_by_kind()
    assert "run" in cbt
    cbc = mem.count_by_container_tag()
    assert DEFAULT_CONTAINER_TAG in cbc
    cbtr = mem.count_by_truth_level()
    assert "measured" in cbtr
    print("filters      : OK")

    # ------------------------------------------------------------ 17. __getitem__
    first_entry = mem.entries[0]
    assert mem[first_entry.run_id] is first_entry
    try:
        mem["nope"]
    except KeyError:
        print("__getitem__  : OK")

    # ------------------------------------------------------------ 18. Serialization
    payload = mem.to_json()
    restored_mem = EpisodicMemory.from_json(payload)
    assert restored_mem.to_dict() == mem.to_dict()
    print("serialize RT : OK")

    # ------------------------------------------------------------ 19. Async
    async def _async_path():
        async with EpisodicMemory(memory_file=IN_MEMORY_PATH) as m:
            await m.store_async({"content": "async"}, run_id="async-1")
            loaded = await m.load_all_async()
            return loaded

    async_loaded = asyncio.run(_async_path())
    assert len(async_loaded) == 1
    print("async        : OK")

    # ------------------------------------------------------------ 20. Statistics / reset
    stats = mem.statistics()
    print("statistics   :", {
        k: v for k, v in stats.items()
        if k not in ("truth_levels", "ttl_kinds")
    })
    assert "last_error" in stats
    assert mem.reset(clear_entries=False) == 0
    print("reset(0)     : OK")

    # ------------------------------------------------------------ 21. Corruption
    corrupt_path = os.path.join(tmpdir, "corrupt.json")
    with open(corrupt_path, "w") as f:
        f.write("{not valid json")
    lenient = EpisodicMemory(memory_file=corrupt_path, strict=False)
    print("recover      :", lenient.count, "entries")
    try:
        EpisodicMemory(memory_file=corrupt_path, strict=True)
    except EpisodicMemoryCorruptionError as exc:
        print("strict read  : OK ->", exc)

    # ------------------------------------------------------------ 22. Validation
    for bad in (
        lambda: mem.store(123),                          # type: ignore[arg-type]
        lambda: mem.store({"x": 1}, truth_level="bogus"),
        lambda: EpisodicMemory(max_episodes=0),
        lambda: EpisodicMemory(max_episodes=True),
        lambda: EpisodicMemory(write_retries=-1),
        lambda: EpisodicMemory(ttl_seconds={"policy": 0}),
        lambda: EpisodicMemory(truth_levels=("a", "a")),
        lambda: EpisodicMemory(default_truth_level="bogus"),
        lambda: EpisodicMemory(persist_truncation_on_read="yes"),
        lambda: mem.get_recent("nope"),                  # type: ignore[arg-type]
        lambda: mem.contains(record_id=""),
    ):
        try:
            bad()
        except EpisodicMemoryError as exc:
            print("Rejected     :", exc)

    # ------------------------------------------------------------ 23. Context manager
    with EpisodicMemory(memory_file=os.path.join(tmpdir, "ctx.json")) as scoped:
        scoped.store({"content": "ctx", "reward": 1.0}, run_id="ctx-1")
        with scoped:
            scoped.store({"content": "nested"}, run_id="ctx-2")
    print("ctx mgr      : OK (reentrant)")

    print("\nSmoke test passed.")
