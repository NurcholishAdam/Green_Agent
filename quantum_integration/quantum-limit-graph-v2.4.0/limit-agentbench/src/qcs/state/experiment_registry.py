"""
ExperimentRegistry — persistent index of quantum experiments.

A thread-safe, optionally-persistent registry of ``ExperimentRecord``
entries. Records are **immutable facts**: once persisted, a record's
identity fields (``experiment_id``, ``task_id``, ``request_id``,
``algorithm``, ``backend``, ``created_at``) cannot be changed. The
mutable-in-practice fields (status, quality, resource metrics) can be
updated via ``update()``, which appends a new state to the log rather
than mutating the old record.

Persistence format
------------------
Two formats are supported and auto-detected on load:

  * **Append-only event log (default, JSONL).** Each operation is one
    line::

        {"op":"put","record":{...}}
        {"op":"delete","experiment_id":"..."}

    Appends are O(1), crash-safe (partial last line is skipped on load),
    and no full rewrite is required after the initial write.

  * **Single-JSON snapshot (legacy).** A file like
    ``{"records": [...]}``. Loaded transparently; the next write
    switches to append-only.

``compact()`` rewrites the log as a fresh snapshot of the current state,
via a temp file + ``os.replace`` (atomic on POSIX).

Backward compatibility
----------------------
The pre-2.0 API is preserved:

  * ``ExperimentRegistry(path=...)`` — same.
  * ``registry.record(rec) -> None`` — same; now raises on persist
    failure.
  * ``registry.list(task_id=..., status=..., limit=...) -> List[Dict]``
    — same return shape.
  * ``registry.count() -> int`` — same.

New in v2:

  * ``get()``, ``delete()``, ``update()``, ``clear()``, ``records()``,
    ``compact()``, ``load()``, ``health()``.
  * Typed exceptions: ``DuplicateExperimentError``,
    ``ExperimentNotFoundError``, ``PersistenceError``.
  * Typed ``ExperimentRecord`` (frozen, validated, ``status`` is
    ``ExecutionStatus``).
  * Append-only log; atomic compaction; tolerant load with diagnostics.

v2 fixes (see changelog in commit message):
  * Persistence is no longer O(n²); appends are O(1).
  * Concurrent writes no longer corrupt the file.
  * Serialization failures no longer destroy existing records.
  * ``record()`` raises when a write fails (was silent).
  * ``_load()`` reports skipped records (was silent).
  * ``list(limit=0)`` returns ``[]`` (was the whole registry).
  * ``ExperimentRecord`` frozen; ``extra`` frozen.
  * ``status`` is ``ExecutionStatus``; typos raise.
  * Field-level validation added.
  * Duplicate ``experiment_id`` rejected.
  * Legacy ``{"records": ...}`` files still load.
  * Metrics, ``health()``, structured logging.
  * ``__version__``, ``__all__``, demo.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import threading
import time
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Optional contract import (fall back to a local enum if not present)
# =============================================================================

try:  # pragma: no cover — the enhanced contract is the intended dependency
    from ..contracts.execution_result import ExecutionStatus
except Exception:  # pragma: no cover — original contracts or standalone use
    class ExecutionStatus(str, Enum):  # type: ignore[no-redef]
        SUBMITTED = "submitted"
        QUEUED = "queued"
        RUNNING = "running"
        COMPLETED = "completed"
        FAILED = "failed"
        FALLBACK = "fallback"
        CANCELLED = "cancelled"
        BUDGET_EXCEEDED = "budget_exceeded"


# =============================================================================
# Constants
# =============================================================================

MAX_ID_LEN: int = 256
MAX_EXTRA_ENTRIES: int = 64
MAX_EXTRA_VALUE_LEN: int = 4096
DEFAULT_MAX_RECORDS: int = 100_000
COMPACT_THRESHOLD_RATIO: float = 3.0   # compact when file > 3× live size


# =============================================================================
# Errors
# =============================================================================

class RegistryError(Exception):
    """Base class for ExperimentRegistry errors."""


class PersistenceError(RegistryError):
    """The underlying file could not be read or written."""


class DuplicateExperimentError(RegistryError):
    """A record with the same ``experiment_id`` already exists."""

    def __init__(self, experiment_id: str) -> None:
        self.experiment_id = experiment_id
        super().__init__(
            f"an experiment with experiment_id={experiment_id!r} "
            f"is already registered"
        )


class ExperimentNotFoundError(RegistryError):
    """The requested ``experiment_id`` is not in the registry."""

    def __init__(self, experiment_id: str) -> None:
        self.experiment_id = experiment_id
        super().__init__(
            f"experiment_id={experiment_id!r} not found"
        )


class RegistryInputError(RegistryError, ValueError):
    """Invalid input to the registry or a record."""


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class RegistryConfig:
    """Frozen, validated configuration for ``ExperimentRegistry``."""

    durable: bool = True                # fsync on append
    autoload: bool = True               # load in __init__
    max_records: int = DEFAULT_MAX_RECORDS
    allow_replace_on_record: bool = False
    auto_compact_threshold: float = COMPACT_THRESHOLD_RATIO

    def __post_init__(self) -> None:
        for fname in ("durable", "autoload", "allow_replace_on_record"):
            if not isinstance(getattr(self, fname), bool):
                raise RegistryInputError(f"{fname} must be a bool")
        if isinstance(self.max_records, bool) \
                or not isinstance(self.max_records, int) \
                or self.max_records <= 0:
            raise RegistryInputError(
                "max_records must be a positive int"
            )
        if self.auto_compact_threshold <= 0:
            raise RegistryInputError(
                "auto_compact_threshold must be > 0"
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Metrics
# =============================================================================

class MetricsRecorder:
    """Drop-in replaceable with Prometheus / OTel."""

    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _key(name: str, labels: Mapping[str, Any]) -> str:
        if not labels:
            return name
        parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{parts}}}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.counters[k] = self.counters.get(k, 0.0) + value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.histograms.setdefault(k, []).append(float(value))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            hist: Dict[str, Dict[str, float]] = {}
            for k, vals in self.histograms.items():
                if not vals:
                    continue
                s = sorted(vals)
                hist[k] = {
                    "count": len(vals),
                    "mean": sum(vals) / len(vals),
                    "p50": s[len(s) // 2],
                    "p99": s[min(len(s) - 1, int(len(s) * 0.99))],
                    "max": s[-1],
                }
            return {
                "counters": dict(self.counters),
                "histograms": hist,
            }


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


def _is_nonempty_str(v: Any) -> bool:
    return isinstance(v, str) and bool(v.strip())


def _is_finite_number(v: Any) -> bool:
    return (
        not isinstance(v, bool)
        and isinstance(v, (int, float))
        and math.isfinite(float(v))
    )


def _freeze_mapping(m: Any) -> Mapping[str, Any]:
    if m is None:
        return MappingProxyType({})
    if isinstance(m, MappingProxyType):
        return m
    if not isinstance(m, Mapping):
        raise RegistryInputError(
            f"extra must be a Mapping; got {type(m).__name__}"
        )
    return MappingProxyType(dict(m))


def _to_jsonable(obj: Any) -> Any:
    """
    Recursive JSON-safe conversion. Raises ``TypeError`` on unknown types
    — silently stringifying audit records is worse than failing.
    """
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    raise TypeError(
        f"_to_jsonable: cannot serialize type {type(obj).__name__}"
    )


# =============================================================================
# ExperimentRecord
# =============================================================================

@dataclass(frozen=True)
class ExperimentRecord:
    """
    Immutable record of a single experiment.

    Identity fields (``experiment_id``, ``task_id``, ``request_id``,
    ``algorithm``, ``backend``, ``created_at``) never change after
    construction. Operational fields (``status``, ``quality``,
    ``shots_used``, ...) can be changed via ``ExperimentRegistry.update``,
    which creates a *new* record with the same identity.
    """

    # --- Identity ---
    experiment_id: str
    task_id: str
    request_id: str
    algorithm: str
    backend: Optional[str]
    status: ExecutionStatus
    created_at: datetime = field(default_factory=_utcnow)

    # --- Operational (mutable via registry.update) ---
    quality: Optional[float] = None
    shots_used: int = 0
    queue_seconds: float = 0.0
    runtime_seconds: float = 0.0
    energy_kwh: float = 0.0
    co2e_kg: float = 0.0
    signature: Optional[str] = None
    policy_version: Optional[str] = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    # --- Internal ---
    _skip_validation: bool = field(
        default=False, repr=False, compare=False, hash=False,
    )

    def __post_init__(self) -> None:
        # --- Enum coercion ---
        if not isinstance(self.status, ExecutionStatus):
            try:
                object.__setattr__(
                    self, "status", ExecutionStatus(self.status),
                )
            except (ValueError, KeyError):
                allowed = sorted(s.value for s in ExecutionStatus)
                raise RegistryInputError(
                    f"status must be one of {allowed}; "
                    f"got {self.status!r}"
                ) from None

        # --- UTC ---
        object.__setattr__(
            self, "created_at", _ensure_utc(self.created_at),
        )

        # --- Freeze extra ---
        object.__setattr__(
            self, "extra", _freeze_mapping(self.extra),
        )

        if self._skip_validation:
            return

        # --- Identity validation ---
        for fname in (
            "experiment_id", "task_id", "request_id", "algorithm",
        ):
            v = getattr(self, fname)
            if not _is_nonempty_str(v):
                raise RegistryInputError(
                    f"{fname} must be a non-empty string"
                )
            if len(v) > MAX_ID_LEN:
                raise RegistryInputError(
                    f"{fname} exceeds {MAX_ID_LEN} chars"
                )
        if self.backend is not None and not _is_nonempty_str(self.backend):
            raise RegistryInputError(
                "backend must be a non-empty string or None"
            )

        # --- Numeric validation ---
        if self.quality is not None:
            if not _is_finite_number(self.quality):
                raise RegistryInputError(
                    "quality must be a finite number or None"
                )
            if not (0.0 <= float(self.quality) <= 1.0):
                raise RegistryInputError(
                    f"quality must be in [0, 1]; got {self.quality}"
                )
        if isinstance(self.shots_used, bool) \
                or not isinstance(self.shots_used, int) \
                or self.shots_used < 0:
            raise RegistryInputError(
                "shots_used must be a non-negative int"
            )
        for fname in ("queue_seconds", "runtime_seconds", "energy_kwh", "co2e_kg"):
            v = getattr(self, fname)
            if not _is_finite_number(v) or float(v) < 0:
                raise RegistryInputError(
                    f"{fname} must be a non-negative finite number"
                )

        # --- Extra bounds ---
        if len(self.extra) > MAX_EXTRA_ENTRIES:
            raise RegistryInputError(
                f"extra has {len(self.extra)} entries "
                f"(max {MAX_EXTRA_ENTRIES})"
            )
        for k, v in self.extra.items():
            if not isinstance(k, str):
                raise RegistryInputError("extra keys must be strings")
            if isinstance(v, str) and len(v) > MAX_EXTRA_VALUE_LEN:
                raise RegistryInputError(
                    f"extra[{k!r}] exceeds {MAX_EXTRA_VALUE_LEN} chars"
                )

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "task_id": self.task_id,
            "request_id": self.request_id,
            "algorithm": self.algorithm,
            "backend": self.backend,
            "status": self.status.value,
            "quality": self.quality,
            "shots_used": self.shots_used,
            "queue_seconds": self.queue_seconds,
            "runtime_seconds": self.runtime_seconds,
            "energy_kwh": self.energy_kwh,
            "co2e_kg": self.co2e_kg,
            "signature": self.signature,
            "policy_version": self.policy_version,
            "created_at": self.created_at.isoformat(),
            "extra": _to_jsonable(dict(self.extra)),
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(
        cls,
        d: Mapping[str, Any],
        *,
        validate: bool = True,
    ) -> "ExperimentRecord":
        if not isinstance(d, Mapping):
            raise RegistryInputError(
                f"d must be a Mapping; got {type(d).__name__}"
            )
        known = {f.name for f in fields(cls)} - {"_skip_validation"}
        unknown = set(d.keys()) - known
        if unknown:
            raise RegistryInputError(
                f"unknown fields in ExperimentRecord dict: "
                f"{sorted(unknown)}"
            )
        cleaned: Dict[str, Any] = dict(d)
        ts = cleaned.get("created_at")
        if isinstance(ts, str):
            cleaned["created_at"] = datetime.fromisoformat(ts)
        elif ts is None:
            # Legacy files may lack created_at entirely; use epoch.
            cleaned["created_at"] = datetime.fromtimestamp(
                0, tz=timezone.utc,
            )
            logger.warning(
                "ExperimentRecord missing created_at; using epoch"
            )
        if not validate:
            cleaned["_skip_validation"] = True
        return cls(**cleaned)

    def checksum(self) -> str:
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def with_updates(self, **changes: Any) -> "ExperimentRecord":
        """Return a copy with the given fields replaced (re-validated)."""
        return replace(self, **changes)

    def __repr__(self) -> str:
        return (
            f"ExperimentRecord("
            f"id={self.experiment_id!r}, "
            f"task={self.task_id!r}, "
            f"status={self.status.value}, "
            f"quality={self.quality!r})"
        )


# =============================================================================
# Updatable fields
# =============================================================================

_UPDATABLE_FIELDS: FrozenSet[str] = frozenset({
    "status", "quality", "shots_used",
    "queue_seconds", "runtime_seconds",
    "energy_kwh", "co2e_kg",
    "signature", "policy_version", "extra",
})


# =============================================================================
# The Enhanced ExperimentRegistry
# =============================================================================

class ExperimentRegistry:
    """
    Thread-safe, optionally-persistent registry of experiments.

    See the module docstring for the persistence format and API.
    """

    def __init__(
        self,
        *,
        path: Optional[Path] = None,
        config: Optional[RegistryConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
        autoload: Optional[bool] = None,
    ) -> None:
        self.config = config or RegistryConfig(
            autoload=autoload if autoload is not None else True,
        )
        if autoload is not None and autoload != self.config.autoload:
            # Caller explicitly overrode the config; honor it.
            self.config = replace(self.config, autoload=autoload)

        self._path: Optional[Path] = Path(path) if path else None
        self._metrics = metrics or MetricsRecorder()
        self._lock = threading.RLock()

        # In-memory index.
        self._records_by_id: Dict[str, ExperimentRecord] = {}
        self._order: List[str] = []
        self._by_task_id: Dict[str, Set[str]] = {}
        self._by_status: Dict[ExecutionStatus, Set[str]] = {}

        # Diagnostics.
        self._load_errors: List[str] = []
        self._load_count: int = 0
        self._persist_failures: int = 0
        self._ops_appended: int = 0
        self._started_mono = time.monotonic()

        if self.config.autoload:
            self.load()

    # ------------------------------------------------------------------ persistence
    def load(self) -> int:
        """
        (Re)load records from the persisted file. Idempotent — clears the
        in-memory state before loading. Returns the number of records
        loaded; populates ``_load_errors`` for any skipped entries.
        """
        self._load_errors.clear()
        self._load_count = 0
        with self._lock:
            self._records_by_id.clear()
            self._order.clear()
            self._by_task_id.clear()
            self._by_status.clear()

            if self._path is None or not self._path.exists():
                return 0

            try:
                content = self._path.read_text(encoding="utf-8")
            except Exception as exc:
                self._load_errors.append(f"read_failed:{exc}")
                logger.warning(
                    "experiment registry could not be read",
                    extra={"path": str(self._path)},
                    exc_info=True,
                )
                return 0

            if not content.strip():
                return 0

            # Detect format: try single-JSON first.
            try:
                parsed = json.loads(content)
            except json.JSONDecodeError:
                parsed = None

            if isinstance(parsed, dict) and "op" not in parsed:
                loaded = self._load_snapshot_locked(parsed)
            else:
                loaded = self._load_jsonl_locked(content)

            self._load_count = loaded
            logger.info(
                "experiment registry loaded",
                extra={
                    "path": str(self._path),
                    "loaded": loaded,
                    "skipped": len(self._load_errors),
                },
            )
            return loaded

    def _load_snapshot_locked(self, data: Mapping[str, Any]) -> int:
        entries = data.get("records", [])
        if not isinstance(entries, list):
            self._load_errors.append("snapshot_records_not_list")
            return 0
        loaded = 0
        for i, entry in enumerate(entries):
            try:
                rec = ExperimentRecord.from_dict(entry)
                self._apply_put_locked(rec, allow_replace=True)
                loaded += 1
            except Exception as exc:
                self._load_errors.append(f"record_{i}:{exc}")
        return loaded

    def _load_jsonl_locked(self, content: str) -> int:
        loaded = 0
        for lineno, raw in enumerate(content.splitlines(), start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                op = json.loads(line)
            except json.JSONDecodeError as exc:
                self._load_errors.append(f"line_{lineno}_parse:{exc}")
                continue
            try:
                self._apply_op_locked(op)
                loaded += 1
            except Exception as exc:
                self._load_errors.append(f"line_{lineno}_apply:{exc}")
        return loaded

    def _apply_op_locked(self, op: Mapping[str, Any]) -> None:
        kind = op.get("op")
        if kind == "put":
            rec_data = op.get("record")
            if not isinstance(rec_data, Mapping):
                raise RegistryInputError("put op missing 'record'")
            rec = ExperimentRecord.from_dict(rec_data)
            self._apply_put_locked(rec, allow_replace=True)
        elif kind == "delete":
            experiment_id = op.get("experiment_id")
            if not _is_nonempty_str(experiment_id):
                raise RegistryInputError("delete op missing experiment_id")
            self._apply_delete_locked(experiment_id)
        else:
            raise RegistryInputError(f"unknown op {kind!r}")

    def _append_op_locked(self, op: Mapping[str, Any]) -> None:
        """Append one operation to the on-disk log. Raises on failure."""
        if self._path is None:
            return
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            line = json.dumps(op, separators=(",", ":"), default=_strict_default) + "\n"
            with open(self._path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                if self.config.durable:
                    os.fsync(f.fileno())
        except Exception as exc:
            self._persist_failures += 1
            self._metrics.inc("persist_failures_total")
            logger.error(
                "experiment registry append failed",
                extra={"path": str(self._path), "op": op.get("op")},
                exc_info=True,
            )
            raise PersistenceError(
                f"failed to append to {self._path}: {exc}"
            ) from exc
        self._ops_appended += 1
        self._metrics.inc("ops_appended_total", op=str(op.get("op")))

    # ------------------------------------------------------------------ mutation
    def record(
        self, rec: ExperimentRecord, *, replace: Optional[bool] = None,
    ) -> None:
        """
        Add a record. Raises ``DuplicateExperimentError`` if a record
        with the same ``experiment_id`` exists (unless ``replace=True``
        or ``config.allow_replace_on_record=True``).
        """
        if not isinstance(rec, ExperimentRecord):
            raise RegistryInputError(
                f"record() requires an ExperimentRecord; "
                f"got {type(rec).__name__}"
            )
        replace_effective = (
            replace if replace is not None
            else self.config.allow_replace_on_record
        )
        with self._lock:
            existing = self._records_by_id.get(rec.experiment_id)
            if existing is not None and not replace_effective:
                raise DuplicateExperimentError(rec.experiment_id)

            # Append first: if it fails, in-memory state stays untouched.
            self._append_op_locked({
                "op": "put",
                "record": rec.to_dict(),
            })

            self._apply_put_locked(rec, allow_replace=True)
            self._maybe_evict_locked()

        self._metrics.inc(
            "records_total",
            action="replace" if existing is not None else "add",
        )

    def update(
        self, experiment_id: str, **changes: Any,
    ) -> ExperimentRecord:
        """
        Update operational fields of an existing record.

        Allowed fields: status, quality, shots_used, queue_seconds,
        runtime_seconds, energy_kwh, co2e_kg, signature, policy_version,
        extra. Identity fields cannot be updated.
        """
        if not _is_nonempty_str(experiment_id):
            raise RegistryInputError(
                "experiment_id must be a non-empty string"
            )
        unknown = set(changes) - _UPDATABLE_FIELDS
        if unknown:
            raise RegistryInputError(
                f"cannot update fields {sorted(unknown)}; "
                f"updatable: {sorted(_UPDATABLE_FIELDS)}"
            )
        with self._lock:
            existing = self._records_by_id.get(experiment_id)
            if existing is None:
                raise ExperimentNotFoundError(experiment_id)
            updated = replace(existing, **changes)
            self._append_op_locked({
                "op": "put",
                "record": updated.to_dict(),
            })
            self._apply_put_locked(updated, allow_replace=True)
        self._metrics.inc("records_total", action="update")
        return updated

    def delete(self, experiment_id: str) -> bool:
        """Delete a record. Returns True if it existed."""
        if not _is_nonempty_str(experiment_id):
            raise RegistryInputError(
                "experiment_id must be a non-empty string"
            )
        with self._lock:
            if experiment_id not in self._records_by_id:
                return False
            self._append_op_locked({
                "op": "delete",
                "experiment_id": experiment_id,
            })
            self._apply_delete_locked(experiment_id)
        self._metrics.inc("records_total", action="delete")
        return True

    def clear(self) -> None:
        """Remove all records (in memory and on disk)."""
        with self._lock:
            # Append deletes for everything so the log is honest.
            if self._path is not None:
                for experiment_id in list(self._records_by_id.keys()):
                    self._append_op_locked({
                        "op": "delete",
                        "experiment_id": experiment_id,
                    })
            self._records_by_id.clear()
            self._order.clear()
            self._by_task_id.clear()
            self._by_status.clear()
        self._metrics.inc("clears_total")

    # ------------------------------------------------------------------ queries
    def get(self, experiment_id: str) -> Optional[ExperimentRecord]:
        with self._lock:
            return self._records_by_id.get(experiment_id)

    def require(self, experiment_id: str) -> ExperimentRecord:
        with self._lock:
            rec = self._records_by_id.get(experiment_id)
        if rec is None:
            raise ExperimentNotFoundError(experiment_id)
        return rec

    def records(
        self,
        *,
        task_id: Optional[str] = None,
        status: Optional[Union[ExecutionStatus, str]] = None,
        limit: int = 100,
        sort_by: Optional[str] = None,
        ascending: bool = True,
    ) -> List[ExperimentRecord]:
        """
        Return typed ``ExperimentRecord`` objects.

        ``limit=0`` returns ``[]``; ``limit<0`` raises.

        ``sort_by=None`` preserves insertion order (last ``limit`` in
        insertion order — matching the pre-2.0 behavior). Providing
        ``sort_by`` returns the first ``limit`` records after sorting.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise RegistryInputError(
                "limit must be a non-negative int"
            )

        with self._lock:
            ids: Optional[Set[str]] = None
            if task_id is not None:
                ids = set(self._by_task_id.get(task_id, ()))
            if status is not None:
                status_enum = (
                    status if isinstance(status, ExecutionStatus)
                    else ExecutionStatus(status)
                )
                status_ids = set(self._by_status.get(status_enum, ()))
                ids = status_ids if ids is None else (ids & status_ids)

            if ids is None:
                base_ids = list(self._order)
            else:
                base_ids = [i for i in self._order if i in ids]

            recs = [
                self._records_by_id[i]
                for i in base_ids if i in self._records_by_id
            ]

            if limit == 0:
                return []

            if sort_by is None:
                return recs[-limit:]

            if sort_by not in ExperimentRecord.__dataclass_fields__:
                raise RegistryInputError(
                    f"unknown sort_by field {sort_by!r}"
                )
            recs.sort(key=lambda r: getattr(r, sort_by), reverse=not ascending)
            return recs[:limit]

    def list(
        self,
        *,
        task_id: Optional[str] = None,
        status: Optional[Union[ExecutionStatus, str]] = None,
        limit: int = 100,
        sort_by: Optional[str] = None,
        ascending: bool = True,
    ) -> List[Dict[str, Any]]:
        """Backward-compatible wrapper around ``records()``."""
        return [
            r.to_dict()
            for r in self.records(
                task_id=task_id, status=status, limit=limit,
                sort_by=sort_by, ascending=ascending,
            )
        ]

    def count(self) -> int:
        with self._lock:
            return len(self._records_by_id)

    def __len__(self) -> int:
        return self.count()

    def __contains__(self, experiment_id: object) -> bool:
        if not isinstance(experiment_id, str):
            return False
        with self._lock:
            return experiment_id in self._records_by_id

    # ------------------------------------------------------------------ compaction
    def compact(self) -> int:
        """
        Rewrite the log as a fresh snapshot of the current state,
        removing tombstones and superseded entries. Atomic: writes to a
        temp file and ``os.replace``s it. Returns the number of live
        records written.
        """
        if self._path is None:
            return 0
        with self._lock:
            tmp = self._path.with_suffix(self._path.suffix + ".tmp")
            tmp.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    for experiment_id in self._order:
                        rec = self._records_by_id.get(experiment_id)
                        if rec is None:
                            continue
                        op = {"op": "put", "record": rec.to_dict()}
                        f.write(
                            json.dumps(op, separators=(",", ":"),
                                       default=_strict_default)
                            + "\n"
                        )
                    f.flush()
                    if self.config.durable:
                        os.fsync(f.fileno())
                os.replace(tmp, self._path)
            except Exception as exc:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
                raise PersistenceError(
                    f"compaction failed: {exc}"
                ) from exc
            live = len(self._records_by_id)
        self._metrics.inc("compactions_total")
        logger.info(
            "experiment registry compacted",
            extra={"live_records": live, "path": str(self._path)},
        )
        return live

    # ------------------------------------------------------------------ health
    def health(self) -> Dict[str, Any]:
        with self._lock:
            n = len(self._records_by_id)
            file_size = (
                self._path.stat().st_size
                if self._path is not None and self._path.exists()
                else 0
            )
        return {
            "ready": True,
            "path": str(self._path) if self._path else None,
            "records": n,
            "records_loaded_on_startup": self._load_count,
            "load_errors": list(self._load_errors),
            "persist_failures": self._persist_failures,
            "ops_appended": self._ops_appended,
            "file_size_bytes": file_size,
            "config": self.config.to_dict(),
            "uptime_seconds": time.monotonic() - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }

    # ------------------------------------------------------------------ internals
    def _apply_put_locked(
        self, rec: ExperimentRecord, *, allow_replace: bool,
    ) -> None:
        existing = self._records_by_id.get(rec.experiment_id)
        if existing is not None:
            if not allow_replace:
                raise DuplicateExperimentError(rec.experiment_id)
            self._unindex_locked(existing)
        else:
            self._order.append(rec.experiment_id)
        self._records_by_id[rec.experiment_id] = rec
        self._index_locked(rec)

    def _apply_delete_locked(self, experiment_id: str) -> None:
        existing = self._records_by_id.pop(experiment_id, None)
        if existing is None:
            return
        self._unindex_locked(existing)
        try:
            self._order.remove(experiment_id)
        except ValueError:
            pass

    def _index_locked(self, rec: ExperimentRecord) -> None:
        self._by_task_id.setdefault(rec.task_id, set()).add(rec.experiment_id)
        self._by_status.setdefault(rec.status, set()).add(rec.experiment_id)

    def _unindex_locked(self, rec: ExperimentRecord) -> None:
        task_set = self._by_task_id.get(rec.task_id)
        if task_set is not None:
            task_set.discard(rec.experiment_id)
            if not task_set:
                self._by_task_id.pop(rec.task_id, None)
        status_set = self._by_status.get(rec.status)
        if status_set is not None:
            status_set.discard(rec.experiment_id)
            if not status_set:
                self._by_status.pop(rec.status, None)

    def _maybe_evict_locked(self) -> None:
        while len(self._records_by_id) > self.config.max_records:
            oldest = self._order[0] if self._order else None
            if oldest is None:
                break
            logger.warning(
                "evicting experiment record (max_records reached)",
                extra={
                    "experiment_id": oldest,
                    "max_records": self.config.max_records,
                },
            )
            self._apply_delete_locked(oldest)
            self._metrics.inc("evictions_total")

    def __repr__(self) -> str:
        return (
            f"ExperimentRegistry("
            f"path={str(self._path) if self._path else None!r}, "
            f"records={self.count()})"
        )


# =============================================================================
# Strict JSON default (raises on unknown types)
# =============================================================================

def _strict_default(o: Any) -> Any:
    raise TypeError(
        f"_strict_default: cannot serialize {type(o).__name__}"
    )


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "DuplicateExperimentError",
    "ExecutionStatus",
    "ExperimentNotFoundError",
    "ExperimentRecord",
    "ExperimentRegistry",
    "MetricsRecorder",
    "PersistenceError",
    "RegistryConfig",
    "RegistryError",
    "RegistryInputError",
)


# =============================================================================
# Demo: python -m qcs.state.experiment_registry
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import tempfile
    from pathlib import Path

    logging.basicConfig(level=logging.INFO)

    def _mk(
        i: int, *,
        task: str = "task-1",
        status: ExecutionStatus = ExecutionStatus.COMPLETED,
        quality: Optional[float] = 0.9,
    ) -> ExperimentRecord:
        return ExperimentRecord(
            experiment_id=f"exp-{i:04d}",
            task_id=task,
            request_id=f"req-{i:04d}",
            algorithm="vqe",
            backend="ibmq_quito",
            status=status,
            quality=quality,
            shots_used=1000,
            queue_seconds=1.5,
            runtime_seconds=3.2,
            energy_kwh=0.05,
            co2e_kg=0.02,
            extra={"note": f"run-{i}"},
        )

    print("=== In-memory: record / get / list / count ===")
    reg = ExperimentRegistry()
    for i in range(5):
        reg.record(_mk(i))
    print(f"  count: {reg.count()}")
    assert reg.count() == 5
    print(f"  get(exp-0002): {reg.get('exp-0002')}")
    print(f"  contains exp-0002: {'exp-0002' in reg}")
    print(f"  records(task_id='task-1'): {len(reg.records(task_id='task-1'))}")

    print("\n=== §1.4 limit=0 returns [] ===")
    assert reg.list(limit=0) == []
    print(f"  list(limit=0): {reg.list(limit=0)}")
    try:
        reg.list(limit=-1)
    except RegistryInputError as e:
        print(f"  list(limit=-1): {type(e).__name__}")

    print("\n=== §2.2 status is ExecutionStatus ===")
    rec = reg.get("exp-0000")
    print(f"  {rec!r}")
    print(f"  type(rec.status): {type(rec.status).__name__}")
    assert isinstance(rec.status, ExecutionStatus)

    print("\n=== §2.4 duplicate experiment_id rejected ===")
    try:
        reg.record(_mk(0))
    except DuplicateExperimentError as e:
        print(f"  {type(e).__name__}: {e.experiment_id}")
    # Replace is allowed when requested.
    reg.record(_mk(0, quality=0.95), replace=True)
    print(f"  after replace: quality={reg.get('exp-0000').quality}")

    print("\n=== §3.2 update() ===")
    updated = reg.update(
        "exp-0001",
        status=ExecutionStatus.FAILED,
        quality=None,
    )
    print(f"  updated: {updated!r}")
    assert updated.status == ExecutionStatus.FAILED

    print("\n=== §2.1 immutability ===")
    try:
        rec.status = ExecutionStatus.FAILED  # type: ignore[misc]
    except Exception as e:
        print(f"  status mutation → {type(e).__name__}")
    try:
        rec.extra["injected"] = True  # type: ignore[index]
    except TypeError:
        print(f"  extra mutation → TypeError")

    print("\n=== Persistence round-trip ===")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "registry.jsonl"
        reg1 = ExperimentRegistry(path=path)
        for i in range(10):
            reg1.record(_mk(i))
        print(f"  file size: {path.stat().st_size} bytes")
        print(f"  first line: {path.read_text().splitlines()[0][:80]}…")

        # Reload in a fresh instance.
        reg2 = ExperimentRegistry(path=path)
        print(f"  reloaded: {reg2.count()} records")
        assert reg2.count() == 10
        print(f"  health: loaded={reg2.health()['records_loaded_on_startup']}, "
              f"errors={reg2.health()['load_errors']}")

        print("\n=== §1.6 Corrupt line tolerated on load ===")
        # Append a corrupted line.
        with open(path, "a") as f:
            f.write('{"op": "put", "record": {BROKEN}\n')
        reg3 = ExperimentRegistry(path=path)
        print(f"  reloaded after corruption: {reg3.count()} records")
        print(f"  load_errors: {reg3.health()['load_errors']}")
        assert reg3.count() == 10  # corrupt line skipped

        print("\n=== §3.2 delete + update round-trip ===")
        reg2.delete("exp-0005")
        reg2.update("exp-0006", quality=0.99)
        print(f"  after delete+update: count={reg2.count()}")
        reg4 = ExperimentRegistry(path=path)
        assert reg4.count() == 9
        assert reg4.get("exp-0005") is None
        assert reg4.get("exp-0006").quality == 0.99
        print(f"  reloaded state preserved ✓")

        print("\n=== §1.3 compact() ===")
        before = path.stat().st_size
        live = reg4.compact()
        after = path.stat().st_size
        print(f"  compacted: {live} records, {before} → {after} bytes")
        assert after <= before

        print("\n=== Legacy snapshot format loads ===")
        legacy_path = Path(td) / "legacy.json"
        legacy_path.write_text(json.dumps({
            "records": [
                _mk(100).to_dict(),
                _mk(101).to_dict(),
            ]
        }))
        reg_legacy = ExperimentRegistry(path=legacy_path)
        print(f"  legacy loaded: {reg_legacy.count()} records")
        assert reg_legacy.count() == 2

    print("\n=== §3.2 clear() ===")
    reg.clear()
    print(f"  after clear: {reg.count()}")
    assert reg.count() == 0

    print("\n=== §3.3 Index: filter by task_id and status ===")
    reg_idx = ExperimentRegistry()
    for i in range(20):
        reg_idx.record(_mk(
            i,
            task="task-A" if i % 2 == 0 else "task-B",
            status=(
                ExecutionStatus.COMPLETED if i % 3 == 0
                else ExecutionStatus.FAILED
            ),
        ))
    task_a = reg_idx.records(task_id="task-A")
    print(f"  task-A: {len(task_a)}")
    failed = reg_idx.records(status=ExecutionStatus.FAILED)
    print(f"  failed: {len(failed)}")
    both = reg_idx.records(
        task_id="task-A", status=ExecutionStatus.COMPLETED,
    )
    print(f"  task-A + completed: {len(both)}")

    print("\n=== §3.4 sort ===")
    top = reg_idx.records(
        sort_by="quality", ascending=False, limit=3,
    )
    print(f"  top 3 by quality: "
          f"{[(r.experiment_id, r.quality) for r in top]}")

    print("\n=== §2.3 field validation ===")
    for label, kwargs in [
        ("negative shots", dict(shots_used=-1)),
        ("quality > 1", dict(quality=1.5)),
        ("empty task_id", dict(task_id="")),
        ("bad status", dict(status="complet")),
    ]:
        base = _mk(999).to_dict()
        base.update(kwargs)
        try:
            ExperimentRecord.from_dict(base)
            print(f"  {label}: NOT rejected")
        except Exception as e:
            print(f"  {label}: {type(e).__name__}")

    print("\n=== §3.10 record() rejects wrong type ===")
    try:
        reg_idx.record("not a record")  # type: ignore[arg-type]
    except RegistryInputError as e:
        print(f"  {type(e).__name__}")

    print("\n=== §2.9 max_records eviction ===")
    small = ExperimentRegistry(
        config=RegistryConfig(max_records=3),
    )
    for i in range(10):
        small.record(_mk(i))
    print(f"  after 10 records (max 3): {small.count()}")
    assert small.count() == 3
    print(f"  oldest gone: {small.get('exp-0000') is None}")
    print(f"  newest kept: {small.get('exp-0009') is not None}")

    print("\n=== §4.x Thread safety ===")
    import threading
    reg_ts = ExperimentRegistry()
    def worker(idx: int) -> None:
        for j in range(50):
            reg_ts.record(_mk(idx * 100 + j))
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  final count: {reg_ts.count()} (expected 250)")
    assert reg_ts.count() == 250

    print("\n=== §3.9 health() ===")
    h = reg_ts.health()
    print(f"  records: {h['records']}, "
          f"persist_failures: {h['persist_failures']}")

    print("\n=== Concurrent persistence (atomicity) ===")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "conc.jsonl"
        reg_c = ExperimentRegistry(path=path)
        def worker_c(idx: int) -> None:
            for j in range(20):
                reg_c.record(_mk(idx * 100 + j, task=f"t-{idx}"))
        threads = [threading.Thread(target=worker_c, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        reloaded = ExperimentRegistry(path=path)
        print(f"  reloaded: {reloaded.count()} (expected 100)")
        assert reloaded.count() == 100

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
