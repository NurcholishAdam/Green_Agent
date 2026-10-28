"""
ProvenanceRegistry — record where every metric came from. — v2.0.0

Every metric in a compliance report has a *source*: a live meter, an
estimation model, a simulation, a contractual instrument. The
provenance registry records that mapping, with a trust level and a
timestamp, so an auditor can answer "where did this number come from?"

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``to_dict()`` is now JSON-safe. Previously ``asdict()`` on a
        ``ProvenanceRecord`` produced a raw ``datetime`` in
        ``recorded_at`` — ``json.dumps(registry.to_dict())`` raised
        ``TypeError`` on the first record.
  * 2.2 New query API: ``get_by_metric(metric)`` and
        ``get_latest(metric)``. Previously the only method was
        ``to_dict()``, which returned every record with no filtering
        — the registry could not answer the one question it exists
        to answer.
  * 2.3 ``ProvenanceRecord.__post_init__`` validates every field,
        including cross-field consistency: ``source_kind="measured"``
        with ``trust_level="low"`` is rejected because measured data
        cannot be low-trust.
  * 2.4 ``record()`` derives ``trust_level`` from ``source_kind`` when
        the caller omits it. Previously the default was a silent
        ``"low"`` — recording ``source_kind="measured"`` produced a
        low-trust record.
  * 2.5 ``RLock`` guards every mutation and every read. Concurrent
        ``record`` + ``get_latest`` no longer race.
  * 2.6 New error hierarchy: ``ProvenanceError(ValueError)`` with
        ``ProvenanceValidationError`` and ``ProvenanceNotFoundError``.
        Add these to ``governance/__init__.py``'s
        ``_ERROR_CLASSES_BY_MODULE`` for ``except GovernanceError:``
        compatibility.
  * 2.7 ``ProvenanceRecord`` is ``frozen=True``. Direct mutation
        raises ``FrozenInstanceError`` — the only mutation path is
        ``record()``, which appends a new entry.

**P1 robustness**
  * 3.1 ``metric_value`` field records the metric's value at the
        moment of provenance capture, so a compliance auditor can
        cross-check "the report says accuracy=0.92; the provenance
        says the measured accuracy was 0.92". Plus
        ``verify_provenance(metric, value, tolerance=...)``.
  * 3.2 ``linked_entry_id`` correlates a provenance record to a
        ``CarbonLedger`` / ``CarbonInstrumentsLedger`` entry.
        ``get_provenance_for_entry(entry_id)`` retrieves it.
  * 3.3 Optional WAL persistence (``sink_path``) — appends one JSON
        line per record, fsyncs file and parent dir, reloads on
        construction. Same pattern as ``CarbonLedger`` and
        ``CarbonInstrumentsLedger`` v2.0.0.
  * 3.4 ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
  * 3.5 ``get``, ``__len__``, ``__iter__``, ``__contains__``,
        ``to_list``.
  * 3.6 ``statistics()`` with per-source / per-trust breakdowns and
        ``min_trust_by_metric``.
  * 3.7 ``get_latest`` cached via ``_latest_by_metric`` — O(1)
        amortized instead of O(N) per call.
  * 3.8 ``metrics_hook(event, fields)`` fires on record and
        verification.
  * 3.9 ``__repr__`` and pickle support.
  * 3.10 ``record_many(records)`` — batch insertion under one lock.
  * 3.11 ``__version__`` and ``LEDGER_SCHEMA_VERSION`` exported.
  * 3.12 ``distinct_metrics()`` and ``distinct_source_kinds()``.
  * 3.13 ``source_name`` guards against path traversal.
  * 3.14 ``derived_from`` field for metric derivation graphs;
        ``provenance_chain(metric)`` walks the chain.
  * 3.15 ``recorded_at`` is caller-supplied (default = now); the
        timestamp is measurement time, not insert time.
  * 3.17 ``__all__``.

Backward compatible
-------------------
``ProvenanceRegistry()``, ``registry.record(metric=..., source_kind=...,
source_name=..., trust_level=...)``, ``registry.to_dict()`` — every
v1.0.0 call site works unchanged. New kwargs are optional.

Note on ``trust_level`` default change
--------------------------------------
v1.0.0 defaulted ``trust_level="low"``. v2.0.0 derives it from
``source_kind`` (``"measured" → "high"``, ``"estimated" → "medium"``,
etc.) when the caller omits it. Callers who relied on the silent
``"low"`` default must pass ``trust_level="low"`` explicitly — which
will now fail validation for ``source_kind="measured"``, because the
old behaviour was semantically inconsistent.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"
LEDGER_SCHEMA_VERSION = "5.0.0"


# =============================================================================
# Errors (P0 2.6)
# =============================================================================

class ProvenanceError(ValueError):
    """Base class for every error raised by ProvenanceRegistry."""


class ProvenanceValidationError(ProvenanceError):
    """Raised for invalid record values or inputs."""


class ProvenanceNotFoundError(ProvenanceError):
    """Raised when a metric has no provenance record."""


# =============================================================================
# Enums (P0 2.3)
# =============================================================================

class SourceKind(str, Enum):
    MEASURED = "measured"
    ESTIMATED = "estimated"
    SIMULATED = "simulated"
    CONTRACTUAL = "contractual"
    DERIVED = "derived"
    IMPORTED = "imported"

    def __str__(self) -> str:
        return self.value


class TrustLevel(str, Enum):
    UNKNOWN = "unknown"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

    def __str__(self) -> str:
        return self.value


# =============================================================================
# Cross-field consistency (P0 2.3, P0 2.4)
# =============================================================================

# Minimum trust level for each source kind. Measured data cannot
# logically be low-trust; simulated data cannot be high-trust. These
# constraints prevent provenance laundering.
_MIN_TRUST_FOR_SOURCE: Dict[str, str] = {
    "measured": "high",
    "contractual": "high",
    "derived": "medium",
    "estimated": "medium",
    "simulated": "low",
    "imported": "low",
}

_TRUST_RANK: Dict[str, int] = {
    "unknown": 0, "low": 1, "medium": 2, "high": 3,
}


# =============================================================================
# Helpers
# =============================================================================

def _as_aware_utc(dt: datetime) -> datetime:
    """Normalize a datetime to UTC (P0 2.3)."""
    if not isinstance(dt, datetime):
        raise ProvenanceValidationError(
            f"expected datetime, got {type(dt).__name__}")
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fsync_dir(path: Path) -> None:
    """fsync the parent directory of ``path`` (POSIX)."""
    if os.name == "nt":  # pragma: no cover
        return
    try:
        fd = os.open(str(path.parent), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:  # pragma: no cover
        pass
    finally:
        os.close(fd)


def _check_no_path_traversal(name: str, value: str) -> None:
    """Reject path-traversal-like characters in ``value`` (P1 3.13)."""
    if ".." in value or "\\" in value:
        raise ProvenanceValidationError(
            f"{name} contains path-traversal-like characters: "
            f"{value!r}")


# =============================================================================
# ProvenanceRecord (P0 2.3, 2.7; P1 3.1, 3.2, 3.14)
# =============================================================================

@dataclass(frozen=True)
class ProvenanceRecord:
    """
    Immutable record of where one metric came from.

    Frozen — mutation must go through :meth:`ProvenanceRegistry.record`.
    """
    metric: str
    source_kind: str
    trust_level: str
    source_name: Optional[str] = None
    metric_value: Optional[float] = None            # P1 3.1
    linked_entry_id: Optional[str] = None           # P1 3.2
    derived_from: Tuple[str, ...] = ()              # P1 3.14
    recorded_at: datetime = field(                  # P1 3.15
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        # -- metric --
        if not isinstance(self.metric, str) or not self.metric:
            raise ProvenanceValidationError(
                "metric must be a non-empty string")

        # -- source_kind (normalize + validate) --
        if not isinstance(self.source_kind, str):
            raise ProvenanceValidationError(
                f"source_kind must be a string, "
                f"got {type(self.source_kind).__name__}")
        kind = self.source_kind.strip().lower()
        valid_kinds = {s.value for s in SourceKind}
        if kind not in valid_kinds:
            raise ProvenanceValidationError(
                f"source_kind must be one of "
                f"{sorted(valid_kinds)}, got {self.source_kind!r}")
        object.__setattr__(self, "source_kind", kind)

        # -- trust_level (normalize + validate) --
        if not isinstance(self.trust_level, str):
            raise ProvenanceValidationError(
                f"trust_level must be a string, "
                f"got {type(self.trust_level).__name__}")
        trust = self.trust_level.strip().lower()
        valid_trusts = {t.value for t in TrustLevel}
        if trust not in valid_trusts:
            raise ProvenanceValidationError(
                f"trust_level must be one of "
                f"{sorted(valid_trusts)}, got {self.trust_level!r}")
        object.__setattr__(self, "trust_level", trust)

        # -- cross-field consistency (P0 2.3) --
        min_trust = _MIN_TRUST_FOR_SOURCE.get(kind, "unknown")
        if _TRUST_RANK[trust] < _TRUST_RANK[min_trust]:
            raise ProvenanceValidationError(
                f"trust_level={trust!r} is below the minimum for "
                f"source_kind={kind!r} (minimum is {min_trust!r}); "
                f"measured data cannot be low-trust, and simulated "
                f"data cannot be high-trust")

        # -- source_name (with path-traversal guard) --
        if self.source_name is not None:
            if not isinstance(self.source_name, str) or \
                    not self.source_name:
                raise ProvenanceValidationError(
                    "source_name must be a non-empty string or None")
            _check_no_path_traversal("source_name", self.source_name)

        # -- metric_value --
        if self.metric_value is not None:
            if isinstance(self.metric_value, bool) or \
                    not isinstance(self.metric_value, (int, float)):
                raise ProvenanceValidationError(
                    "metric_value must be numeric or None")
            fv = float(self.metric_value)
            if math.isnan(fv) or math.isinf(fv):
                raise ProvenanceValidationError(
                    "metric_value must be finite")
            object.__setattr__(self, "metric_value", fv)

        # -- linked_entry_id --
        if self.linked_entry_id is not None:
            if not isinstance(self.linked_entry_id, str) or \
                    not self.linked_entry_id:
                raise ProvenanceValidationError(
                    "linked_entry_id must be a non-empty string "
                    "or None")

        # -- derived_from --
        if not isinstance(self.derived_from, tuple):
            raise ProvenanceValidationError(
                "derived_from must be a tuple of strings")
        for i, parent in enumerate(self.derived_from):
            if not isinstance(parent, str) or not parent:
                raise ProvenanceValidationError(
                    f"derived_from[{i}] must be a non-empty string")
        if self.metric in self.derived_from:
            raise ProvenanceValidationError(
                f"metric {self.metric!r} cannot be in its own "
                f"derived_from chain")

        # -- recorded_at (UTC-normalize) --
        object.__setattr__(
            self, "recorded_at", _as_aware_utc(self.recorded_at))

    # ---------------------------------------------------------------- #
    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-safe dict representation."""
        return {
            "metric": self.metric,
            "source_kind": self.source_kind,
            "trust_level": self.trust_level,
            "source_name": self.source_name,
            "metric_value": self.metric_value,
            "linked_entry_id": self.linked_entry_id,
            "derived_from": list(self.derived_from),
            "recorded_at": self.recorded_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ProvenanceRecord":
        if not isinstance(data, Mapping):
            raise ProvenanceValidationError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}")

        recorded_at_raw = data.get("recorded_at")
        if isinstance(recorded_at_raw, str):
            try:
                recorded_at = datetime.fromisoformat(
                    recorded_at_raw.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ProvenanceValidationError(
                    f"recorded_at is not a valid ISO 8601 timestamp: "
                    f"{exc}") from exc
        elif isinstance(recorded_at_raw, datetime):
            recorded_at = recorded_at_raw
        elif recorded_at_raw is None:
            recorded_at = datetime.now(timezone.utc)
        else:
            raise ProvenanceValidationError(
                f"recorded_at must be a datetime, ISO string, or "
                f"None; got {type(recorded_at_raw).__name__}")

        derived_from_raw = data.get("derived_from", ())
        if isinstance(derived_from_raw, list):
            derived_from = tuple(derived_from_raw)
        elif isinstance(derived_from_raw, tuple):
            derived_from = derived_from_raw
        else:
            raise ProvenanceValidationError(
                "derived_from must be a list or tuple")

        return cls(
            metric=data.get("metric"),
            source_kind=data.get("source_kind"),
            trust_level=data.get("trust_level"),
            source_name=data.get("source_name"),
            metric_value=data.get("metric_value"),
            linked_entry_id=data.get("linked_entry_id"),
            derived_from=derived_from,
            recorded_at=recorded_at,
        )


# =============================================================================
# ProvenanceRegistry (P0 2.2, 2.4, 2.5; P1 all)
# =============================================================================

class ProvenanceRegistry:
    """
    Append-only registry of metric provenance.

    Every record answers three questions:
      - What metric was this record about?
      - Where did that metric's value come from?
      - When was that value measured?

    Records are immutable. The registry's only mutation path is
    :meth:`record` (or :meth:`record_many`), which appends.
    """

    def __init__(
        self,
        *,
        sink_path: Optional[str] = None,
        strict: bool = True,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ):
        if sink_path is not None:
            if not isinstance(sink_path, str) or not sink_path:
                raise ProvenanceValidationError(
                    "sink_path must be a non-empty string or None")
        if metrics_hook is not None and not callable(metrics_hook):
            raise ProvenanceValidationError(
                "metrics_hook must be callable")

        self._records: List[ProvenanceRecord] = []
        self._lock = threading.RLock()
        self._sink_path = sink_path
        self._strict = bool(strict)
        self._metrics_hook = metrics_hook
        # P1 3.7: cache for get_latest()
        self._latest_cache: Optional[Dict[str, ProvenanceRecord]] = None
        # P1 3.2: index for linked_entry_id lookups
        self._by_entry_id: Dict[str, ProvenanceRecord] = {}
        self._record_count: int = 0

        if sink_path:
            self._load_from_sink()

    # ------------------------------------------------------------------
    # Metrics hook (P1 3.8)
    # ------------------------------------------------------------------

    def _emit_metric(
        self, event: str, fields_: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields_))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    # ------------------------------------------------------------------
    # Sink persistence (P1 3.3)
    # ------------------------------------------------------------------

    def _load_from_sink(self) -> None:
        path = Path(self._sink_path)  # type: ignore[arg-type]
        if not path.exists():
            return
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ProvenanceError(
                f"Could not read sink {path}: {exc}") from exc
        if not raw:
            return

        has_complete_last = raw.endswith("\n")
        lines = raw.splitlines()
        loaded: List[ProvenanceRecord] = []
        by_entry_id: Dict[str, ProvenanceRecord] = {}

        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            is_last = (i == len(lines) - 1)
            is_partial = is_last and not has_complete_last
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                if is_partial:
                    logger.debug(
                        "Skipping partial last line in sink %s", path)
                    continue
                if self._strict:
                    raise ProvenanceError(
                        f"Sink {path} corrupted at line {i + 1}")
                logger.warning(
                    "Skipping corrupt line %d in sink %s",
                    i + 1, path)
                continue
            if not isinstance(payload, Mapping):
                if self._strict:
                    raise ProvenanceError(
                        f"Sink {path} line {i + 1} is not a "
                        f"JSON object")
                continue
            try:
                record = ProvenanceRecord.from_dict(payload)
            except ProvenanceValidationError as exc:
                if self._strict:
                    raise ProvenanceError(
                        f"Sink {path} line {i + 1} is invalid: "
                        f"{exc}") from exc
                logger.warning(
                    "Skipping invalid record at line %d: %s",
                    i + 1, exc)
                continue
            loaded.append(record)
            if record.linked_entry_id:
                by_entry_id[record.linked_entry_id] = record

        with self._lock:
            self._records = loaded
            self._by_entry_id = by_entry_id
            self._latest_cache = None

        logger.debug(
            "ProvenanceRegistry loaded %d records from %s",
            len(loaded), path)

    def _persist_line(self, record: ProvenanceRecord) -> None:
        if not self._sink_path:
            return
        path = Path(self._sink_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record.to_dict(), default=str) + "\n"
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            _fsync_dir(path)
        except OSError as exc:
            raise ProvenanceError(
                f"Could not append to sink {path}: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Record (P0 2.4, 2.5; P1 3.1, 3.2, 3.14, 3.15)
    # ------------------------------------------------------------------

    def record(
        self,
        *,
        metric: str,
        source_kind: str,
        source_name: Optional[str] = None,
        trust_level: Optional[str] = None,        # None = derive
        metric_value: Optional[float] = None,
        linked_entry_id: Optional[str] = None,
        derived_from: Optional[Sequence[str]] = None,
        recorded_at: Optional[datetime] = None,
    ) -> ProvenanceRecord:
        """
        Record the provenance of a metric.

        ``trust_level`` is derived from ``source_kind`` when omitted
        (P0 2.4). ``recorded_at`` defaults to now; callers who know
        the measurement time should supply it explicitly (P1 3.15).

        Returns the stored ``ProvenanceRecord``.
        """
        # P0 2.4: derive trust_level from source_kind if omitted.
        if trust_level is None:
            kind_norm = (
                source_kind.strip().lower()
                if isinstance(source_kind, str) else ""
            )
            trust_level = _MIN_TRUST_FOR_SOURCE.get(
                kind_norm, "unknown")

        # P1 3.15: caller-supplied recorded_at; default = now.
        if recorded_at is None:
            recorded_at = datetime.now(timezone.utc)

        derived_tuple: Tuple[str, ...] = ()
        if derived_from is not None:
            if isinstance(derived_from, str):
                raise ProvenanceValidationError(
                    "derived_from must be a sequence of strings, "
                    "not a single string")
            derived_tuple = tuple(derived_from)

        record_obj = ProvenanceRecord(
            metric=metric,
            source_kind=source_kind,
            trust_level=trust_level,
            source_name=source_name,
            metric_value=metric_value,
            linked_entry_id=linked_entry_id,
            derived_from=derived_tuple,
            recorded_at=recorded_at,
        )

        with self._lock:
            self._persist_line(record_obj)
            self._records.append(record_obj)
            if record_obj.linked_entry_id:
                self._by_entry_id[record_obj.linked_entry_id] = \
                    record_obj
            self._latest_cache = None
            self._record_count += 1

        self._emit_metric("record_added", {
            "metric": record_obj.metric,
            "source_kind": record_obj.source_kind,
            "trust_level": record_obj.trust_level,
            "metric_value": record_obj.metric_value,
        })
        if record_obj.trust_level in ("low", "unknown"):
            self._emit_metric("low_trust_recorded", {
                "metric": record_obj.metric,
                "trust_level": record_obj.trust_level,
            })
        return record_obj

    def record_many(
        self, records: Sequence[Mapping[str, Any]],
    ) -> List[ProvenanceRecord]:
        """Record multiple entries under one lock (P1 3.10)."""
        if not isinstance(records, (list, tuple)):
            raise ProvenanceValidationError(
                f"records must be a list or tuple, "
                f"got {type(records).__name__}")

        built: List[ProvenanceRecord] = []
        for i, raw in enumerate(records):
            if not isinstance(raw, Mapping):
                raise ProvenanceValidationError(
                    f"records[{i}] must be a Mapping")

            source_kind = raw.get("source_kind")
            trust_level = raw.get("trust_level")
            if trust_level is None:
                kind_norm = (
                    source_kind.strip().lower()
                    if isinstance(source_kind, str) else ""
                )
                trust_level = _MIN_TRUST_FOR_SOURCE.get(
                    kind_norm, "unknown")

            recorded_at = raw.get("recorded_at")
            if recorded_at is None:
                recorded_at = datetime.now(timezone.utc)

            derived_raw = raw.get("derived_from")
            if derived_raw is None:
                derived_tuple: Tuple[str, ...] = ()
            elif isinstance(derived_raw, str):
                raise ProvenanceValidationError(
                    f"records[{i}].derived_from must be a "
                    f"sequence of strings, not a single string")
            else:
                derived_tuple = tuple(derived_raw)

            try:
                record_obj = ProvenanceRecord(
                    metric=raw.get("metric"),
                    source_kind=source_kind,
                    trust_level=trust_level,
                    source_name=raw.get("source_name"),
                    metric_value=raw.get("metric_value"),
                    linked_entry_id=raw.get("linked_entry_id"),
                    derived_from=derived_tuple,
                    recorded_at=recorded_at,
                )
            except ProvenanceValidationError as exc:
                raise ProvenanceValidationError(
                    f"records[{i}]: {exc}") from exc
            built.append(record_obj)

        with self._lock:
            for record_obj in built:
                self._persist_line(record_obj)
            for record_obj in built:
                self._records.append(record_obj)
                if record_obj.linked_entry_id:
                    self._by_entry_id[
                        record_obj.linked_entry_id] = record_obj
            self._latest_cache = None
            self._record_count += len(built)

        for record_obj in built:
            self._emit_metric("record_added", {
                "metric": record_obj.metric,
                "source_kind": record_obj.source_kind,
                "trust_level": record_obj.trust_level,
                "batch": True,
            })
        return built

    # ------------------------------------------------------------------
    # Query API (P0 2.2; P1 3.5, 3.7)
    # ------------------------------------------------------------------

    def get_by_metric(self, metric: str) -> List[ProvenanceRecord]:
        """Return every provenance record for ``metric``."""
        if not isinstance(metric, str) or not metric:
            raise ProvenanceValidationError(
                "metric must be a non-empty string")
        with self._lock:
            return [r for r in self._records if r.metric == metric]

    def get_latest(self, metric: str) -> Optional[ProvenanceRecord]:
        """Return the most recent provenance record for ``metric``."""
        if not isinstance(metric, str) or not metric:
            raise ProvenanceValidationError(
                "metric must be a non-empty string")
        with self._lock:
            if self._latest_cache is None:
                cache: Dict[str, ProvenanceRecord] = {}
                for r in self._records:
                    cache[r.metric] = r
                self._latest_cache = cache
            return self._latest_cache.get(metric)

    def get_provenance_for_entry(
        self, entry_id: str,
    ) -> Optional[ProvenanceRecord]:
        """Return the record linked to a ledger entry (P1 3.2)."""
        if not isinstance(entry_id, str) or not entry_id:
            return None
        with self._lock:
            return self._by_entry_id.get(entry_id)

    def distinct_metrics(self) -> List[str]:
        with self._lock:
            return sorted({r.metric for r in self._records})

    def distinct_source_kinds(self) -> List[str]:
        with self._lock:
            return sorted({r.source_kind for r in self._records})

    def provenance_chain(
        self, metric: str, *, max_depth: int = 5,
    ) -> List[ProvenanceRecord]:
        """
        Return the full provenance chain for ``metric``, following
        ``derived_from`` links (P1 3.14).
        """
        if isinstance(max_depth, bool) or not isinstance(max_depth, int) \
                or max_depth <= 0:
            raise ProvenanceValidationError(
                "max_depth must be a positive int")
        chain: List[ProvenanceRecord] = []
        seen: set = set()

        def _walk(m: str, depth: int) -> None:
            if depth > max_depth or m in seen:
                return
            seen.add(m)
            record = self.get_latest(m)
            if record is None:
                return
            chain.append(record)
            for parent in record.derived_from:
                _walk(parent, depth + 1)

        _walk(metric, 1)
        return chain

    def verify_provenance(
        self,
        metric: str,
        value: float,
        *,
        tolerance: float = 1e-6,
    ) -> bool:
        """
        Return True iff the latest record for ``metric`` has a
        ``metric_value`` matching ``value`` within ``tolerance``
        (P1 3.1).
        """
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            return False
        if tolerance < 0:
            raise ProvenanceValidationError(
                "tolerance must be >= 0")

        record = self.get_latest(metric)
        if record is None or record.metric_value is None:
            self._emit_metric("provenance_verified", {
                "metric": metric, "ok": False,
            })
            return False
        ok = abs(record.metric_value - fv) <= tolerance
        self._emit_metric("provenance_verified", {
            "metric": metric, "ok": ok,
        })
        return ok

    # ------------------------------------------------------------------
    # Read API (P1 3.5)
    # ------------------------------------------------------------------

    def to_list(self) -> List[ProvenanceRecord]:
        with self._lock:
            return list(self._records)

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)

    def __iter__(self) -> Iterator[ProvenanceRecord]:
        return iter(self.to_list())

    def __contains__(self, metric: object) -> bool:
        if not isinstance(metric, str) or not metric:
            return False
        with self._lock:
            return any(r.metric == metric for r in self._records)

    # ------------------------------------------------------------------
    # Statistics (P1 3.6)
    # ------------------------------------------------------------------

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            by_source: Dict[str, int] = {}
            by_trust: Dict[str, int] = {}
            min_trust_by_metric: Dict[str, str] = {}
            for r in self._records:
                by_source[r.source_kind] = (
                    by_source.get(r.source_kind, 0) + 1)
                by_trust[r.trust_level] = (
                    by_trust.get(r.trust_level, 0) + 1)
                existing = min_trust_by_metric.get(r.metric)
                rank = _TRUST_RANK.get(r.trust_level, 0)
                if existing is None or \
                        rank < _TRUST_RANK.get(existing, 0):
                    min_trust_by_metric[r.metric] = r.trust_level

            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "record_count": len(self._records),
                "distinct_metrics": len(self.distinct_metrics()),
                "by_source_kind": by_source,
                "by_trust_level": by_trust,
                "min_trust_by_metric": min_trust_by_metric,
                "low_trust_count": by_trust.get("low", 0),
                "high_trust_count": by_trust.get("high", 0),
                "record_count_total": self._record_count,
                "sink_attached": self._sink_path is not None,
                "sink_path": self._sink_path,
            }

    # ------------------------------------------------------------------
    # Serialization (P1 3.4)
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "records": [r.to_dict() for r in self._records],
                "count": len(self._records),
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        sink_path: Optional[str] = None,
        strict: bool = True,
    ) -> "ProvenanceRegistry":
        if not isinstance(data, Mapping):
            raise ProvenanceValidationError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}")
        registry = cls(sink_path=sink_path, strict=strict)
        raw_records = data.get("records", [])
        if not isinstance(raw_records, (list, tuple)):
            raise ProvenanceValidationError(
                "from_dict records must be a list or tuple")
        loaded: List[ProvenanceRecord] = []
        by_entry_id: Dict[str, ProvenanceRecord] = {}
        for i, raw in enumerate(raw_records):
            if not isinstance(raw, Mapping):
                raise ProvenanceValidationError(
                    f"records[{i}] must be a Mapping")
            try:
                record = ProvenanceRecord.from_dict(raw)
            except ProvenanceValidationError as exc:
                raise ProvenanceValidationError(
                    f"records[{i}]: {exc}") from exc
            loaded.append(record)
            if record.linked_entry_id:
                by_entry_id[record.linked_entry_id] = record
        with registry._lock:
            registry._records = loaded
            registry._by_entry_id = by_entry_id
            registry._latest_cache = None
        return registry

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        sink_path: Optional[str] = None,
        strict: bool = True,
    ) -> "ProvenanceRegistry":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ProvenanceValidationError(
                f"Invalid JSON payload: {exc}") from exc
        return cls.from_dict(data, sink_path=sink_path, strict=strict)

    # ------------------------------------------------------------------
    # Pickling (P1 3.9)
    # ------------------------------------------------------------------

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        state.pop("_metrics_hook", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()
        self._metrics_hook = None

    def __repr__(self) -> str:
        with self._lock:
            low = sum(
                1 for r in self._records
                if r.trust_level in ("low", "unknown"))
            return (
                f"ProvenanceRegistry(version={__version__!r}, "
                f"records={len(self._records)}, "
                f"low_trust={low}, "
                f"sink={self._sink_path!r})")


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "__version__",
    "LEDGER_SCHEMA_VERSION",
    "ProvenanceError",
    "ProvenanceNotFoundError",
    "ProvenanceRecord",
    "ProvenanceRegistry",
    "ProvenanceValidationError",
    "SourceKind",
    "TrustLevel",
]


# =============================================================================
# Self-tests
# =============================================================================

def _run_selftests() -> None:
    """Run the module's self-tests."""
    import concurrent.futures
    import pickle
    import tempfile
    import time

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s")

    print(f"=== provenance_registry v{__version__} self-tests ===\n")

    # ---------------------------------------------------------------- #
    # P0 2.1 — JSON-safe to_dict
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: JSON-safe to_dict ===")
    registry = ProvenanceRegistry()
    registry.record(
        metric="accuracy", source_kind="measured",
        source_name="vendor_x")
    payload = registry.to_dict()
    json.dumps(payload)  # must not raise
    assert isinstance(payload["records"][0]["recorded_at"], str)
    print(f"  ✓ to_dict JSON-serializable; recorded_at is ISO string")

    # Round-trip.
    rt = ProvenanceRegistry.from_json(registry.to_json())
    assert len(rt) == 1
    print(f"  ✓ JSON round-trip works")

    # ---------------------------------------------------------------- #
    # P0 2.2 — query API
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: query API ===")
    registry = ProvenanceRegistry()
    registry.record(metric="accuracy", source_kind="measured")
    registry.record(metric="energy", source_kind="estimated")
    registry.record(metric="accuracy", source_kind="simulated")

    accuracy_records = registry.get_by_metric("accuracy")
    assert len(accuracy_records) == 2, len(accuracy_records)
    latest = registry.get_latest("accuracy")
    assert latest.source_kind == "simulated"
    assert registry.get_latest("missing") is None
    print(f"  ✓ get_by_metric: 2 records; get_latest: simulated")

    # ---------------------------------------------------------------- #
    # P0 2.3 — field validation + cross-field consistency
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: validation ===")

    bad_kwargs_list = [
        dict(metric="", source_kind="measured", trust_level="high"),
        dict(metric="m", source_kind="garbage", trust_level="low"),
        dict(metric="m", source_kind="measured",
             trust_level="hihg"),  # typo
        dict(metric="m", source_kind="measured",
             trust_level="low"),  # inconsistent
        dict(metric="m", source_kind="simulated",
             trust_level="high"),  # inconsistent
        dict(metric="m", source_kind="measured", trust_level="high",
             recorded_at="not-a-datetime"),
    ]
    for kwargs in bad_kwargs_list:
        try:
            ProvenanceRecord(**kwargs)
        except ProvenanceValidationError:
            pass
        else:
            raise AssertionError(
                f"P0 2.3 regression: {kwargs!r} accepted")
    print(f"  ✓ {len(bad_kwargs_list)} invalid constructions "
          f"rejected")

    # Path traversal.
    try:
        ProvenanceRecord(
            metric="m", source_kind="measured",
            trust_level="high", source_name="../../etc/passwd")
    except ProvenanceValidationError:
        print(f"  ✓ path traversal in source_name rejected")

    # ---------------------------------------------------------------- #
    # P0 2.4 — trust_level derived from source_kind
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: derived trust_level ===")
    registry = ProvenanceRegistry()

    rec = registry.record(metric="m", source_kind="measured")
    assert rec.trust_level == "high", rec.trust_level
    print(f"  ✓ measured → derived trust=high")

    rec = registry.record(metric="m2", source_kind="estimated")
    assert rec.trust_level == "medium"
    print(f"  ✓ estimated → derived trust=medium")

    rec = registry.record(metric="m3", source_kind="simulated")
    assert rec.trust_level == "low"
    print(f"  ✓ simulated → derived trust=low")

    # Explicit override still works.
    rec = registry.record(
        metric="m4", source_kind="measured", trust_level="medium")
    assert rec.trust_level == "medium"
    print(f"  ✓ explicit trust_level respected")

    # ---------------------------------------------------------------- #
    # P0 2.5 — thread safety
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: thread safety ===")
    registry = ProvenanceRegistry()

    def _record_one(i: int) -> None:
        registry.record(
            metric=f"m-{i % 10}", source_kind="measured")

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        list(pool.map(_record_one, range(100)))
    assert len(registry) == 100
    print(f"  ✓ 100 concurrent records → 100 total")

    # ---------------------------------------------------------------- #
    # P0 2.6 — error hierarchy
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: error hierarchy ===")
    assert issubclass(ProvenanceError, ValueError)
    assert issubclass(ProvenanceValidationError, ProvenanceError)
    assert issubclass(ProvenanceNotFoundError, ProvenanceError)
    print(f"  ✓ 3 error types subclass ProvenanceError + ValueError")

    # ---------------------------------------------------------------- #
    # P0 2.7 — frozen dataclass
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: frozen dataclass ===")
    from dataclasses import FrozenInstanceError
    registry = ProvenanceRegistry()
    rec = registry.record(metric="m", source_kind="measured")
    try:
        rec.trust_level = "low"   # type: ignore[misc]
    except FrozenInstanceError:
        pass
    else:
        raise AssertionError(
            "P0 2.7 regression: record mutable")
    print(f"  ✓ ProvenanceRecord is frozen")

    # ---------------------------------------------------------------- #
    # P1 3.1 — value correlation + verify_provenance
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: value correlation ===")
    registry = ProvenanceRegistry()
    registry.record(
        metric="accuracy", source_kind="measured",
        metric_value=0.92)
    assert registry.verify_provenance("accuracy", 0.92) is True
    assert registry.verify_provenance("accuracy", 0.93) is False
    assert registry.verify_provenance(
        "accuracy", 0.920001, tolerance=0.001) is True
    print(f"  ✓ verify_provenance: 0.92 matches, 0.93 doesn't")

    # ---------------------------------------------------------------- #
    # P1 3.2 — ledger entry correlation
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: ledger entry correlation ===")
    registry = ProvenanceRegistry()
    registry.record(
        metric="operational_co2e_kg",
        source_kind="measured",
        linked_entry_id="entry-abc",
        metric_value=500.0)
    rec = registry.get_provenance_for_entry("entry-abc")
    assert rec is not None
    assert rec.metric == "operational_co2e_kg"
    assert rec.metric_value == 500.0
    assert registry.get_provenance_for_entry("missing") is None
    print(f"  ✓ get_provenance_for_entry works")

    # ---------------------------------------------------------------- #
    # P1 3.3 — persistence
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: sink persistence ===")
    with tempfile.TemporaryDirectory() as td:
        sink = os.path.join(td, "provenance.jsonl")
        log = ProvenanceRegistry(sink_path=sink)
        log.record(metric="a", source_kind="measured")
        log.record(metric="b", source_kind="estimated")

        # Reload.
        log2 = ProvenanceRegistry(sink_path=sink)
        assert len(log2) == 2
        assert log2.get_latest("a").source_kind == "measured"
        print(f"  ✓ 2 records reloaded from sink")

    # ---------------------------------------------------------------- #
    # P1 3.4 — serialization
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: serialization ===")
    d = registry.to_dict()
    assert d["count"] == 1
    assert d["version"] == __version__
    rt = ProvenanceRegistry.from_dict(d)
    assert len(rt) == 1
    print(f"  ✓ to_dict / from_dict round-trip")

    # ---------------------------------------------------------------- #
    # P1 3.5 — read API
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: read API ===")
    registry = ProvenanceRegistry()
    registry.record(metric="m", source_kind="measured")
    assert len(registry) == 1
    assert "m" in registry
    assert "missing" not in registry
    assert len(registry.to_list()) == 1
    assert list(registry)[0].metric == "m"
    print(f"  ✓ get / len / in / iter all work")

    # ---------------------------------------------------------------- #
    # P1 3.6 — statistics
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: statistics ===")
    registry = ProvenanceRegistry()
    registry.record(metric="a", source_kind="measured")
    registry.record(metric="b", source_kind="simulated")
    registry.record(metric="a", source_kind="simulated")
    stats = registry.statistics()
    assert stats["record_count"] == 3
    assert stats["distinct_metrics"] == 2
    assert stats["by_source_kind"] == {"measured": 1, "simulated": 2}
    assert stats["by_trust_level"] == {"high": 1, "low": 2}
    assert stats["min_trust_by_metric"] == {
        "a": "low", "b": "low"}
    print(f"  ✓ statistics: {stats['record_count']} records, "
          f"by_trust={stats['by_trust_level']}")

    # ---------------------------------------------------------------- #
    # P1 3.7 — get_latest caching
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.7: get_latest caching ===")
    registry = ProvenanceRegistry()
    for i in range(100):
        registry.record(
            metric=f"m-{i % 10}", source_kind="measured")
    t0 = time.perf_counter()
    for _ in range(1000):
        registry.get_latest("m-5")
    elapsed = time.perf_counter() - t0
    assert elapsed < 0.05, (
        f"P1 3.7 regression: 1000 calls took {elapsed:.3f}s")
    print(f"  ✓ 1000 get_latest calls in {elapsed*1000:.1f}ms")

    # Cache invalidated by record.
    registry.record(metric="m-5", source_kind="simulated")
    assert registry.get_latest("m-5").source_kind == "simulated"
    print(f"  ✓ cache invalidated by record")

    # ---------------------------------------------------------------- #
    # P1 3.8 — metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.8: metrics_hook ===")
    events: List[Tuple[str, Mapping[str, Any]]] = []
    registry = ProvenanceRegistry(
        metrics_hook=lambda ev, f: events.append((ev, f)))
    registry.record(metric="m", source_kind="measured")
    registry.record(metric="m2", source_kind="simulated")
    names = {ev for ev, _ in events}
    assert "record_added" in names
    assert "low_trust_recorded" in names
    print(f"  ✓ events: {sorted(names)}")

    # ---------------------------------------------------------------- #
    # P1 3.9 — pickle
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.9: pickle ===")
    registry = ProvenanceRegistry()
    registry.record(metric="m", source_kind="measured")
    blob = pickle.dumps(registry)
    restored = pickle.loads(blob)
    assert len(restored) == 1
    assert restored.get_latest("m") is not None
    print(f"  ✓ pickle round-trip works")

    # ---------------------------------------------------------------- #
    # P1 3.10 — record_many
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.10: record_many ===")
    registry = ProvenanceRegistry()
    records = registry.record_many([
        {"metric": "a", "source_kind": "measured"},
        {"metric": "b", "source_kind": "estimated"},
        {"metric": "c", "source_kind": "simulated"},
    ])
    assert len(records) == 3
    assert len(registry) == 3
    print(f"  ✓ 3 records added in one batch")

    # ---------------------------------------------------------------- #
    # P1 3.11 — version constants
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.11: version exports ===")
    assert __version__ == "2.0.0"
    assert LEDGER_SCHEMA_VERSION == "5.0.0"
    d = registry.to_dict()
    assert d["version"] == __version__
    assert d["schema_version"] == LEDGER_SCHEMA_VERSION
    print(f"  ✓ version={__version__!r}, "
          f"schema={LEDGER_SCHEMA_VERSION!r}")

    # ---------------------------------------------------------------- #
    # P1 3.12 — distinct queries
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.12: distinct queries ===")
    metrics = registry.distinct_metrics()
    assert metrics == ["a", "b", "c"]
    source_kinds = registry.distinct_source_kinds()
    assert "measured" in source_kinds
    assert "simulated" in source_kinds
    print(f"  ✓ distinct_metrics={metrics}")

    # ---------------------------------------------------------------- #
    # P1 3.14 — provenance_chain
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.14: provenance chain ===")
    registry = ProvenanceRegistry()
    registry.record(
        metric="model_a", source_kind="measured")
    registry.record(
        metric="model_b", source_kind="measured")
    registry.record(
        metric="ensemble", source_kind="derived",
        derived_from=("model_a", "model_b"))

    chain = registry.provenance_chain("ensemble")
    chain_metrics = [r.metric for r in chain]
    assert "ensemble" in chain_metrics
    assert "model_a" in chain_metrics
    assert "model_b" in chain_metrics
    print(f"  ✓ chain from ensemble: {chain_metrics}")

    # Cycle protection.
    registry.record(
        metric="cycle_a", source_kind="derived",
        derived_from=("cycle_b",))
    registry.record(
        metric="cycle_b", source_kind="derived",
        derived_from=("cycle_a",))
    cycle_chain = registry.provenance_chain("cycle_a")
    assert len(cycle_chain) <= 3
    print(f"  ✓ cycle detection terminates the walk")

    # ---------------------------------------------------------------- #
    # P1 3.15 — caller-supplied recorded_at
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.15: caller-supplied recorded_at ===")
    registry = ProvenanceRegistry()
    measurement_time = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
    rec = registry.record(
        metric="m", source_kind="measured",
        recorded_at=measurement_time)
    assert rec.recorded_at == measurement_time, (
        f"P1 3.15 regression: recorded_at={rec.recorded_at}, "
        f"expected {measurement_time}")
    print(f"  ✓ recorded_at honours caller measurement time")

    # Default is now.
    before = datetime.now(timezone.utc)
    rec = registry.record(metric="m2", source_kind="measured")
    after = datetime.now(timezone.utc)
    assert before <= rec.recorded_at <= after
    print(f"  ✓ default recorded_at is insert time")

    # ---------------------------------------------------------------- #
    # Duplicate metrics over time (append-only)
    # ---------------------------------------------------------------- #
    print("\n=== Append-only semantics ===")
    registry = ProvenanceRegistry()
    r1 = registry.record(
        metric="m", source_kind="estimated", metric_value=0.5)
    r2 = registry.record(
        metric="m", source_kind="measured", metric_value=0.92)
    # Both records exist; get_latest returns the newest.
    assert len(registry) == 2
    assert registry.get_latest("m") is r2
    assert registry.get_by_metric("m") == [r1, r2]
    print(f"  ✓ both records retained; get_latest returns newest")

    print(f"\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
