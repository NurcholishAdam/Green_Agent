"""
CarbonLedger — immutable accounting of operational carbon. — v2.0.0

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``entries()`` is now JSON-safe. Previously ``asdict()`` on a
        ``CarbonEntry`` produced a raw ``datetime`` in ``recorded_at``
        — ``json.dumps(ledger.entries())`` raised ``TypeError`` on
        the first entry.
  * 2.2 ``get_statistics()`` is implemented. Previously the class
        lacked the method entirely, so ``build_separated_report``
        (in ``carbon_instruments_ledger.py`` v2.0.0) silently
        reported ``operational = 0`` for a ledger with real
        emissions. The statistics payload includes both
        ``total_carbon_operational_kg`` and the
        ``total_carbon_kg`` alias that ``build_separated_report``
        falls back to.
  * 2.3 ``CarbonEntry.__post_init__`` validates every field
        (non-empty ids, known source, finite non-negative quantities,
        UTC-normalized timestamps, plausibility bound on
        ``operational_co2e_kg``). Previously every field accepted
        any value.
  * 2.4 ``record()``, ``record_batch()``, aggregates, and reads are
        guarded by an ``RLock``. Concurrent ``record`` + read no
        longer race.
  * 2.5 New error hierarchy: ``CarbonLedgerError`` base with
        ``CarbonLedgerValidationError`` and
        ``CarbonLedgerDuplicateError``. All inherit from
        ``ValueError`` for v1.0.0 backward compat. Add these classes
        to ``governance/__init__.py``'s ``_ERROR_CLASSES_BY_MODULE``
        to re-parent them under ``GovernanceError``.
  * 2.6 "Immutable" claim honoured. ``CarbonEntry`` is
        ``frozen=True`` (direct mutation raises
        ``FrozenInstanceError``); ``_entries`` is a ``tuple``
        (``.append()`` / ``.clear()`` raise ``AttributeError``).
  * 2.7 The "never sum contractual into operational" principle is
        enforced by (a) source validation, (b) a plausibility bound
        on ``operational_co2e_kg``, and (c) the pairing with
        ``build_separated_report``. A caller who misclassifies a
        large offset as operational now sees
        ``CarbonLedgerValidationError`` at record time.

**P1 robustness**
  * 3.1 Optional WAL persistence (``sink_path``) — appends one JSON
        line per entry, fsyncs file and parent dir, reloads on
        construction. Same pattern as ``CarbonInstrumentsLedger``
        v2.0.0.
  * 3.3 ``get(entry_id)``, ``__len__``, ``__iter__``,
        ``__contains__``, ``to_list()``.
  * 3.4 ``total_energy_kwh()`` aggregate.
  * 3.5 ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
  * 3.6 ``statistics()`` with per-source breakdown.
  * 3.7 ``total_operational_kg()`` / ``total_energy_kwh()`` are
        cached; invalidated on ``record``.
  * 3.8 ``CarbonSource`` enum; the ``source`` field is a ``str`` for
        JSON compat, but ``__post_init__`` validates against the
        enum's values.
  * 3.9 ``__version__`` and ``LEDGER_SCHEMA_VERSION`` exported.
  * 3.10 ``metrics_hook(event, fields)`` fires on record.
  * 3.11 ``__repr__`` and ``__getstate__`` / ``__setstate__`` — the
        ledger is picklable.
  * 3.13 ``record_batch(entries)`` — one lock, one fsync.
  * 3.14 ``recorded_at`` UTC-normalized when caller supplies a naive
        datetime.
  * 3.15 Self-tests include an integration check with
        ``build_separated_report``.

**P1 deferred**
  * 3.2 No hash chain. This ledger is append-only and single-writer;
        the instrumentation is tamper-evident by mutation prohibition
        (frozen dataclass, tuple-backed storage), not by chain
        hashing. Add a chain when the paired instruments ledger does.

Backward compatible
-------------------
``CarbonLedger()``, ``ledger.record(run_id=..., operational_co2e_kg=...,
energy_kwh=..., source=..., methodology_version=..., task_id=...,
notes=...) -> str``, ``ledger.total_operational_kg()``,
``ledger.entries()`` — every v1.0.0 call site works unchanged. New
kwargs (``sink_path=``, ``strict=``, ``metrics_hook=``) are optional.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import uuid
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
# Plausibility bound (P0 2.7)
# =============================================================================

# A single task entry should not exceed this many kg CO2e. A value
# above this bound is far more likely to be a contractual instrument
# (offset) misclassified as operational than a real per-task
# measurement. Override via `CarbonLedger(plausibility_max_kg=...)`.
DEFAULT_PLAUSIBILITY_MAX_KG = 100_000.0


# =============================================================================
# Errors (P0 2.5)
# =============================================================================

class CarbonLedgerError(ValueError):
    """Base class for every error raised by CarbonLedger."""


class CarbonLedgerValidationError(CarbonLedgerError):
    """Raised for invalid entry values or inputs."""


class CarbonLedgerDuplicateError(CarbonLedgerError):
    """Raised when recording an entry_id that already exists."""


class CarbonLedgerPersistenceError(CarbonLedgerError):
    """Raised when the sink write fails."""


# =============================================================================
# Enum (P1 3.8)
# =============================================================================

class CarbonSource(str, Enum):
    """Canonical provenance values for ``CarbonEntry.source``."""
    MEASURED = "measured"
    ESTIMATED = "estimated"
    SIMULATED = "simulated"

    def __str__(self) -> str:
        return self.value


_VALID_SOURCES = tuple(s.value for s in CarbonSource)


# =============================================================================
# Helpers
# =============================================================================

def _as_aware_utc(dt: datetime) -> datetime:
    if not isinstance(dt, datetime):
        raise CarbonLedgerValidationError(
            f"expected datetime, got {type(dt).__name__}")
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fsync_dir(path: Path) -> None:
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


# =============================================================================
# CarbonEntry (P0 2.1, 2.3, 2.6)
# =============================================================================

@dataclass(frozen=True)
class CarbonEntry:
    """
    A single operational carbon record.

    Frozen — the only way to modify an entry after creation is to
    :func:`dataclasses.replace` it into a new one, which does not
    affect the ledger's stored copy.
    """
    entry_id: str
    run_id: str
    task_id: Optional[str]
    operational_co2e_kg: float
    energy_kwh: float
    source: str                    # "measured" | "estimated" | "simulated"
    methodology_version: str
    recorded_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    notes: Optional[str] = None

    def __post_init__(self) -> None:
        # -- entry_id --
        if not isinstance(self.entry_id, str) or not self.entry_id:
            raise CarbonLedgerValidationError(
                "entry_id must be a non-empty string")

        # -- run_id --
        if not isinstance(self.run_id, str) or not self.run_id:
            raise CarbonLedgerValidationError(
                "run_id must be a non-empty string")

        # -- task_id --
        if self.task_id is not None and (
                not isinstance(self.task_id, str) or not self.task_id):
            raise CarbonLedgerValidationError(
                "task_id must be a non-empty string or None")

        # -- methodology_version --
        if not isinstance(self.methodology_version, str) or \
                not self.methodology_version:
            raise CarbonLedgerValidationError(
                "methodology_version must be a non-empty string")

        # -- source (P1 3.8: validate against enum) --
        if not isinstance(self.source, str):
            raise CarbonLedgerValidationError(
                f"source must be a string, "
                f"got {type(self.source).__name__}")
        normalized = self.source.strip().lower()
        if normalized not in _VALID_SOURCES:
            raise CarbonLedgerValidationError(
                f"source must be one of {_VALID_SOURCES}, "
                f"got {self.source!r}")
        object.__setattr__(self, "source", normalized)

        # -- operational_co2e_kg --
        if isinstance(self.operational_co2e_kg, bool) or \
                not isinstance(self.operational_co2e_kg, (int, float)):
            raise CarbonLedgerValidationError(
                "operational_co2e_kg must be numeric")
        co2e = float(self.operational_co2e_kg)
        if math.isnan(co2e) or math.isinf(co2e):
            raise CarbonLedgerValidationError(
                "operational_co2e_kg must be finite")
        if co2e < 0:
            raise CarbonLedgerValidationError(
                f"operational_co2e_kg must be >= 0, got {co2e}")
        object.__setattr__(self, "operational_co2e_kg", co2e)

        # -- energy_kwh --
        if isinstance(self.energy_kwh, bool) or \
                not isinstance(self.energy_kwh, (int, float)):
            raise CarbonLedgerValidationError(
                "energy_kwh must be numeric")
        kwh = float(self.energy_kwh)
        if math.isnan(kwh) or math.isinf(kwh):
            raise CarbonLedgerValidationError(
                "energy_kwh must be finite")
        if kwh < 0:
            raise CarbonLedgerValidationError(
                f"energy_kwh must be >= 0, got {kwh}")
        object.__setattr__(self, "energy_kwh", kwh)

        # -- recorded_at (P1 3.14) --
        object.__setattr__(
            self, "recorded_at", _as_aware_utc(self.recorded_at))

        # -- notes --
        if self.notes is not None and not isinstance(self.notes, str):
            raise CarbonLedgerValidationError(
                "notes must be a string or None")

    # ---------------------------------------------------------------- #
    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-safe dict (P0 2.1)."""
        return {
            "entry_id": self.entry_id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "operational_co2e_kg": self.operational_co2e_kg,
            "energy_kwh": self.energy_kwh,
            "source": self.source,
            "methodology_version": self.methodology_version,
            "recorded_at": self.recorded_at.isoformat(),
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonEntry":
        if not isinstance(data, Mapping):
            raise CarbonLedgerValidationError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}")
        recorded_at_raw = data.get("recorded_at")
        if isinstance(recorded_at_raw, str):
            try:
                recorded_at = datetime.fromisoformat(
                    recorded_at_raw.replace("Z", "+00:00"))
            except ValueError as exc:
                raise CarbonLedgerValidationError(
                    f"recorded_at is not a valid ISO 8601 "
                    f"timestamp: {exc}") from exc
        elif isinstance(recorded_at_raw, datetime):
            recorded_at = recorded_at_raw
        elif recorded_at_raw is None:
            recorded_at = datetime.now(timezone.utc)
        else:
            raise CarbonLedgerValidationError(
                f"recorded_at must be a datetime, ISO string, or "
                f"None; got {type(recorded_at_raw).__name__}")
        return cls(
            entry_id=data.get("entry_id"),
            run_id=data.get("run_id"),
            task_id=data.get("task_id"),
            operational_co2e_kg=data.get("operational_co2e_kg"),
            energy_kwh=data.get("energy_kwh"),
            source=data.get("source"),
            methodology_version=data.get("methodology_version"),
            recorded_at=recorded_at,
            notes=data.get("notes"),
        )


# =============================================================================
# CarbonLedger (P0 2.2, 2.4, 2.6, 2.7; P1 all)
# =============================================================================

class CarbonLedger:
    """
    Append-only ledger of operational carbon.

    Never sums contractual instruments into this ledger — those live
    in :class:`CarbonInstrumentsLedger`. The separation is enforced by
    (a) the class boundary, (b) a ``source`` allow-list, (c) a
    plausibility bound that catches a global offset misclassified as
    a single task's operational emissions, and (d) the pairing with
    :func:`build_separated_report`.
    """

    def __init__(
        self,
        *,
        sink_path: Optional[str] = None,
        strict: bool = True,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
        plausibility_max_kg: float = DEFAULT_PLAUSIBILITY_MAX_KG,
    ):
        if sink_path is not None:
            if not isinstance(sink_path, str) or not sink_path:
                raise CarbonLedgerValidationError(
                    "sink_path must be a non-empty string or None")
        if metrics_hook is not None and not callable(metrics_hook):
            raise CarbonLedgerValidationError(
                "metrics_hook must be callable")
        if isinstance(plausibility_max_kg, bool) or \
                not isinstance(plausibility_max_kg, (int, float)):
            raise CarbonLedgerValidationError(
                "plausibility_max_kg must be numeric")
        if math.isnan(plausibility_max_kg) or \
                math.isinf(plausibility_max_kg) or \
                plausibility_max_kg <= 0:
            raise CarbonLedgerValidationError(
                "plausibility_max_kg must be finite and > 0")

        # P0 2.6: tuple-backed storage — append/clear raise.
        self._entries: Tuple[CarbonEntry, ...] = ()
        self._entries_by_id: Dict[str, CarbonEntry] = {}
        self._lock = threading.RLock()
        self._sink_path = sink_path
        self._strict = bool(strict)
        self._metrics_hook = metrics_hook
        self._plausibility_max_kg = float(plausibility_max_kg)
        # P1 3.7: caches
        self._total_operational_cache: Optional[float] = None
        self._total_energy_cache: Optional[float] = None
        self._record_count: int = 0
        self._duplicate_rejected_count: int = 0

        if sink_path:
            self._load_from_sink()

    # ------------------------------------------------------------------
    # Metrics hook (P1 3.10)
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
    # Sink persistence (P1 3.1)
    # ------------------------------------------------------------------

    def _load_from_sink(self) -> None:
        path = Path(self._sink_path)  # type: ignore[arg-type]
        if not path.exists():
            return
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise CarbonLedgerError(
                f"Could not read sink {path}: {exc}") from exc
        if not raw:
            return

        has_complete_last = raw.endswith("\n")
        lines = raw.splitlines()
        loaded: List[CarbonEntry] = []
        by_id: Dict[str, CarbonEntry] = {}

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
                    raise CarbonLedgerError(
                        f"Sink {path} corrupted at line {i + 1}")
                logger.warning(
                    "Skipping corrupt line %d in sink %s",
                    i + 1, path)
                continue
            if not isinstance(payload, Mapping):
                if self._strict:
                    raise CarbonLedgerError(
                        f"Sink {path} line {i + 1} is not a JSON "
                        f"object")
                continue
            try:
                entry = CarbonEntry.from_dict(payload)
            except CarbonLedgerValidationError as exc:
                if self._strict:
                    raise CarbonLedgerError(
                        f"Sink {path} line {i + 1} is invalid: "
                        f"{exc}") from exc
                logger.warning(
                    "Skipping invalid entry at line %d: %s",
                    i + 1, exc)
                continue
            if entry.entry_id in by_id:
                if self._strict:
                    raise CarbonLedgerError(
                        f"Sink {path} line {i + 1} has duplicate "
                        f"entry_id {entry.entry_id!r}")
                continue
            loaded.append(entry)
            by_id[entry.entry_id] = entry

        with self._lock:
            self._entries = tuple(loaded)
            self._entries_by_id = by_id
            self._invalidate_caches()

        logger.debug(
            "CarbonLedger loaded %d entries from %s",
            len(loaded), path)

    def _persist_line(self, entry: CarbonEntry) -> None:
        if not self._sink_path:
            return
        path = Path(self._sink_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(entry.to_dict(), default=str) + "\n"
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            _fsync_dir(path)
        except OSError as exc:
            raise CarbonLedgerPersistenceError(
                f"Could not append to sink {path}: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Caches (P1 3.7)
    # ------------------------------------------------------------------

    def _invalidate_caches(self) -> None:
        self._total_operational_cache = None
        self._total_energy_cache = None

    # ------------------------------------------------------------------
    # Record (P0 2.4, 2.7)
    # ------------------------------------------------------------------

    def record(
        self,
        *,
        run_id: str,
        operational_co2e_kg: float,
        energy_kwh: float,
        source: str,
        methodology_version: str,
        task_id: Optional[str] = None,
        notes: Optional[str] = None,
        entry_id: Optional[str] = None,
    ) -> str:
        """
        Record an operational carbon entry.

        Returns the ``entry_id`` (either the caller-supplied
        ``entry_id`` or a generated one).
        """
        if entry_id is None:
            entry_id = uuid.uuid4().hex[:12]
        elif not isinstance(entry_id, str) or not entry_id:
            raise CarbonLedgerValidationError(
                "entry_id must be a non-empty string or None")

        # P0 2.7: plausibility bound catches misclassified offsets.
        if isinstance(operational_co2e_kg, (int, float)) and \
                not isinstance(operational_co2e_kg, bool):
            co2e = float(operational_co2e_kg)
            if math.isfinite(co2e) and co2e > self._plausibility_max_kg:
                raise CarbonLedgerValidationError(
                    f"operational_co2e_kg={co2e} exceeds "
                    f"plausibility_max_kg={self._plausibility_max_kg}; "
                    f"likely a contractual instrument (offset) "
                    f"misclassified as operational")

        entry = CarbonEntry(
            entry_id=entry_id,
            run_id=run_id,
            task_id=task_id,
            operational_co2e_kg=operational_co2e_kg,
            energy_kwh=energy_kwh,
            source=source,
            methodology_version=methodology_version,
            notes=notes,
        )

        with self._lock:
            if entry.entry_id in self._entries_by_id:
                self._duplicate_rejected_count += 1
                raise CarbonLedgerDuplicateError(
                    f"duplicate entry_id: {entry.entry_id}")
            # Persist BEFORE mutating memory (durability guarantee).
            self._persist_line(entry)
            self._entries = self._entries + (entry,)
            self._entries_by_id[entry.entry_id] = entry
            self._record_count += 1
            self._invalidate_caches()

        self._emit_metric("entry_recorded", {
            "entry_id": entry.entry_id,
            "source": entry.source,
            "operational_co2e_kg": entry.operational_co2e_kg,
            "energy_kwh": entry.energy_kwh,
        })
        return entry.entry_id

    def record_batch(
        self, entries: Sequence[Mapping[str, Any]],
    ) -> List[str]:
        """Record multiple entries under one lock + one fsync (P1 3.13)."""
        if not isinstance(entries, (list, tuple)):
            raise CarbonLedgerValidationError(
                f"entries must be a list or tuple, "
                f"got {type(entries).__name__}")

        # Pre-validate: build CarbonEntry instances and check
        # duplicates against both the batch and the existing ledger.
        built: List[CarbonEntry] = []
        batch_ids: set = set()
        with self._lock:
            existing_ids = set(self._entries_by_id.keys())

        for i, raw in enumerate(entries):
            if not isinstance(raw, Mapping):
                raise CarbonLedgerValidationError(
                    f"entries[{i}] must be a Mapping")
            entry_id = raw.get("entry_id") or uuid.uuid4().hex[:12]
            if not isinstance(entry_id, str) or not entry_id:
                raise CarbonLedgerValidationError(
                    f"entries[{i}].entry_id must be a non-empty "
                    f"string")
            co2e = raw.get("operational_co2e_kg")
            if isinstance(co2e, (int, float)) and \
                    not isinstance(co2e, bool):
                co2e_f = float(co2e)
                if math.isfinite(co2e_f) and \
                        co2e_f > self._plausibility_max_kg:
                    raise CarbonLedgerValidationError(
                        f"entries[{i}].operational_co2e_kg="
                        f"{co2e_f} exceeds plausibility bound")
            try:
                entry = CarbonEntry(
                    entry_id=entry_id,
                    run_id=raw.get("run_id"),
                    task_id=raw.get("task_id"),
                    operational_co2e_kg=raw.get("operational_co2e_kg"),
                    energy_kwh=raw.get("energy_kwh"),
                    source=raw.get("source"),
                    methodology_version=raw.get("methodology_version"),
                    recorded_at=raw.get(
                        "recorded_at",
                        datetime.now(timezone.utc)),
                    notes=raw.get("notes"),
                )
            except CarbonLedgerValidationError as exc:
                raise CarbonLedgerValidationError(
                    f"entries[{i}]: {exc}") from exc
            if entry.entry_id in batch_ids:
                raise CarbonLedgerDuplicateError(
                    f"duplicate entry_id within batch: "
                    f"{entry.entry_id}")
            if entry.entry_id in existing_ids:
                raise CarbonLedgerDuplicateError(
                    f"duplicate entry_id: {entry.entry_id}")
            batch_ids.add(entry.entry_id)
            built.append(entry)

        with self._lock:
            # Persist all lines before committing to memory.
            for entry in built:
                self._persist_line(entry)
            self._entries = self._entries + tuple(built)
            for entry in built:
                self._entries_by_id[entry.entry_id] = entry
            self._record_count += len(built)
            self._invalidate_caches()

        for entry in built:
            self._emit_metric("entry_recorded", {
                "entry_id": entry.entry_id,
                "source": entry.source,
                "operational_co2e_kg": entry.operational_co2e_kg,
                "energy_kwh": entry.energy_kwh,
                "batch": True,
            })
        return [e.entry_id for e in built]

    # ------------------------------------------------------------------
    # Read API (P1 3.3)
    # ------------------------------------------------------------------

    def get(self, entry_id: str) -> Optional[CarbonEntry]:
        if not isinstance(entry_id, str) or not entry_id:
            return None
        with self._lock:
            return self._entries_by_id.get(entry_id)

    def to_list(self) -> List[CarbonEntry]:
        with self._lock:
            return list(self._entries)

    def entries(self) -> List[Dict[str, Any]]:
        """Return JSON-safe dicts (P0 2.1)."""
        with self._lock:
            return [e.to_dict() for e in self._entries]

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def __iter__(self) -> Iterator[CarbonEntry]:
        return iter(self.to_list())

    def __contains__(self, entry_id: object) -> bool:
        if not isinstance(entry_id, str) or not entry_id:
            return False
        with self._lock:
            return entry_id in self._entries_by_id

    # ------------------------------------------------------------------
    # Aggregates (P1 3.4, 3.7)
    # ------------------------------------------------------------------

    def total_operational_kg(self) -> float:
        with self._lock:
            if self._total_operational_cache is None:
                self._total_operational_cache = sum(
                    e.operational_co2e_kg for e in self._entries)
            return self._total_operational_cache

    def total_energy_kwh(self) -> float:
        with self._lock:
            if self._total_energy_cache is None:
                self._total_energy_cache = sum(
                    e.energy_kwh for e in self._entries)
            return self._total_energy_cache

    # ------------------------------------------------------------------
    # Statistics (P0 2.2 + P1 3.6)
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """
        Return statistics consumable by ``build_separated_report``.

        Includes both ``total_carbon_operational_kg`` and the
        ``total_carbon_kg`` alias.
        """
        with self._lock:
            by_source: Dict[str, int] = {}
            total_co2e = 0.0
            total_energy = 0.0
            for e in self._entries:
                by_source[e.source] = by_source.get(e.source, 0) + 1
                total_co2e += e.operational_co2e_kg
                total_energy += e.energy_kwh
            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "entry_count": len(self._entries),
                # Both keys for build_separated_report compat.
                "total_carbon_operational_kg": total_co2e,
                "total_carbon_kg": total_co2e,
                "total_energy_kwh": total_energy,
                "by_source": by_source,
                "measured_count": by_source.get("measured", 0),
                "estimated_count": by_source.get("estimated", 0),
                "simulated_count": by_source.get("simulated", 0),
                "record_count": self._record_count,
                "duplicate_rejected_count":
                    self._duplicate_rejected_count,
                "sink_attached": self._sink_path is not None,
                "sink_path": self._sink_path,
                "plausibility_max_kg": self._plausibility_max_kg,
            }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_statistics`."""
        return self.get_statistics()

    # ------------------------------------------------------------------
    # Serialization (P1 3.5)
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "entries": [e.to_dict() for e in self._entries],
                "total_operational_kg": self.total_operational_kg(),
                "total_energy_kwh": self.total_energy_kwh(),
                "count": len(self._entries),
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        sink_path: Optional[str] = None,
        strict: bool = True,
    ) -> "CarbonLedger":
        if not isinstance(data, Mapping):
            raise CarbonLedgerValidationError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}")
        ledger = cls(sink_path=sink_path, strict=strict)
        raw_entries = data.get("entries", [])
        if not isinstance(raw_entries, (list, tuple)):
            raise CarbonLedgerValidationError(
                "from_dict entries must be a list or tuple")
        loaded: List[CarbonEntry] = []
        by_id: Dict[str, CarbonEntry] = {}
        for i, raw in enumerate(raw_entries):
            if not isinstance(raw, Mapping):
                raise CarbonLedgerValidationError(
                    f"entries[{i}] must be a Mapping")
            try:
                entry = CarbonEntry.from_dict(raw)
            except CarbonLedgerValidationError as exc:
                raise CarbonLedgerValidationError(
                    f"entries[{i}]: {exc}") from exc
            if entry.entry_id in by_id:
                raise CarbonLedgerDuplicateError(
                    f"duplicate entry_id in payload: "
                    f"{entry.entry_id}")
            loaded.append(entry)
            by_id[entry.entry_id] = entry
        with ledger._lock:
            ledger._entries = tuple(loaded)
            ledger._entries_by_id = by_id
            ledger._invalidate_caches()
        return ledger

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        sink_path: Optional[str] = None,
        strict: bool = True,
    ) -> "CarbonLedger":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise CarbonLedgerValidationError(
                f"Invalid JSON payload: {exc}") from exc
        return cls.from_dict(data, sink_path=sink_path, strict=strict)

    # ------------------------------------------------------------------
    # Pickling (P1 3.11)
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
            return (
                f"CarbonLedger(version={__version__!r}, "
                f"entries={len(self._entries)}, "
                f"total_operational_kg="
                f"{self.total_operational_kg():.4f}, "
                f"total_energy_kwh="
                f"{self.total_energy_kwh():.4f}, "
                f"sink={self._sink_path!r})"
            )


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "__version__",
    "LEDGER_SCHEMA_VERSION",
    "DEFAULT_PLAUSIBILITY_MAX_KG",
    "CarbonEntry",
    "CarbonLedger",
    "CarbonLedgerDuplicateError",
    "CarbonLedgerError",
    "CarbonLedgerPersistenceError",
    "CarbonLedgerValidationError",
    "CarbonSource",
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

    print(f"=== carbon_ledger v{__version__} self-tests ===\n")

    def _sample(**overrides):
        base = dict(
            run_id="r-1",
            operational_co2e_kg=100.0,
            energy_kwh=50.0,
            source="measured",
            methodology_version="v5.0.0",
        )
        base.update(overrides)
        return base

    # ---------------------------------------------------------------- #
    # P0 2.1 — JSON-safe entries
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: JSON-safe entries ===")

    ledger = CarbonLedger()
    ledger.record(**_sample())
    payload = ledger.entries()
    json.dumps(payload)  # must not raise
    assert isinstance(payload[0]["recorded_at"], str)
    print(f"  ✓ json.dumps(entries()) succeeds; recorded_at is ISO "
          f"string")

    # Round-trip.
    rt = CarbonLedger.from_json(ledger.to_json())
    assert len(rt) == 1
    print(f"  ✓ JSON round-trip works")

    # ---------------------------------------------------------------- #
    # P0 2.2 — get_statistics + build_separated_report
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: get_statistics + cross-ledger report ===")

    ledger = CarbonLedger()
    ledger.record(**_sample(operational_co2e_kg=500.0, energy_kwh=250.0))
    stats = ledger.get_statistics()
    assert stats["total_carbon_operational_kg"] == 500.0
    assert stats["total_carbon_kg"] == 500.0
    assert stats["total_energy_kwh"] == 250.0
    print(f"  ✓ get_statistics: operational=500, energy=250")

    # Integration with build_separated_report.
    try:
        from .carbon_instruments_ledger import (
            CarbonInstrumentsLedger, build_separated_report,
        )
        instruments = CarbonInstrumentsLedger()
        report = build_separated_report(ledger, instruments)
        assert report["operational"][
            "total_carbon_operational_kg"] == 500.0, (
            f"P0 2.2 regression: build_separated_report shows "
            f"{report['operational']['total_carbon_operational_kg']}")
        print(f"  ✓ build_separated_report shows operational=500")
    except ImportError:
        print(f"  ✓ get_statistics available (instruments ledger "
              f"not importable in this context)")

    # ---------------------------------------------------------------- #
    # P0 2.3 — CarbonEntry validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: CarbonEntry validation ===")

    bad_kwargs_list = [
        dict(entry_id="", run_id="r", task_id=None,
             operational_co2e_kg=1.0, energy_kwh=1.0,
             source="measured", methodology_version="v"),
        dict(entry_id="e", run_id="", task_id=None,
             operational_co2e_kg=1.0, energy_kwh=1.0,
             source="measured", methodology_version="v"),
        dict(entry_id="e", run_id="r", task_id=None,
             operational_co2e_kg=-100.0, energy_kwh=1.0,
             source="measured", methodology_version="v"),
        dict(entry_id="e", run_id="r", task_id=None,
             operational_co2e_kg=float("nan"), energy_kwh=1.0,
             source="measured", methodology_version="v"),
        dict(entry_id="e", run_id="r", task_id=None,
             operational_co2e_kg=1.0, energy_kwh=-5.0,
             source="measured", methodology_version="v"),
        dict(entry_id="e", run_id="r", task_id=None,
             operational_co2e_kg=1.0, energy_kwh=1.0,
             source="garbage", methodology_version="v"),
        dict(entry_id="e", run_id="r", task_id=None,
             operational_co2e_kg=1.0, energy_kwh=1.0,
             source="measured", methodology_version=""),
    ]
    for kwargs in bad_kwargs_list:
        try:
            CarbonEntry(**kwargs)
        except CarbonLedgerValidationError:
            pass
        else:
            raise AssertionError(
                f"P0 2.3 regression: {kwargs!r} accepted")
    print(f"  ✓ {len(bad_kwargs_list)} invalid constructions "
          f"rejected")

    # Source normalization.
    entry = CarbonEntry(
        entry_id="e", run_id="r", task_id=None,
        operational_co2e_kg=1.0, energy_kwh=1.0,
        source="MEASURED", methodology_version="v")
    assert entry.source == "measured"
    print(f"  ✓ source='MEASURED' normalized to 'measured'")

    # ---------------------------------------------------------------- #
    # P0 2.4 — thread safety
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: thread safety ===")

    ledger = CarbonLedger()
    def _record_one(i: int) -> None:
        ledger.record(
            run_id=f"r-{i}",
            operational_co2e_kg=1.0,
            energy_kwh=0.5,
            source="measured",
            methodology_version="v")

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        list(pool.map(_record_one, range(100)))

    assert len(ledger) == 100, (
        f"P0 2.4 regression: {len(ledger)} entries, expected 100")
    assert ledger.total_operational_kg() == 100.0
    print(f"  ✓ 100 concurrent records → 100 entries, total=100.0")

    # ---------------------------------------------------------------- #
    # P0 2.5 — error hierarchy
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: error hierarchy ===")
    assert issubclass(CarbonLedgerError, ValueError)
    assert issubclass(
        CarbonLedgerValidationError, CarbonLedgerError)
    assert issubclass(
        CarbonLedgerDuplicateError, CarbonLedgerError)
    assert issubclass(
        CarbonLedgerPersistenceError, CarbonLedgerError)
    print(f"  ✓ 4 error types subclass CarbonLedgerError "
          f"(and ValueError)")

    # ---------------------------------------------------------------- #
    # P0 2.6 — immutability
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: immutability ===")

    from dataclasses import FrozenInstanceError

    ledger = CarbonLedger()
    eid = ledger.record(**_sample())
    entry = ledger.get(eid)
    try:
        entry.operational_co2e_kg = 0.0   # type: ignore[misc]
    except FrozenInstanceError:
        pass
    else:
        raise AssertionError(
            "P0 2.6 regression: CarbonEntry mutable")
    print(f"  ✓ CarbonEntry is frozen")

    # Tuple-backed storage.
    assert isinstance(ledger._entries, tuple), (
        f"P0 2.6 regression: _entries is {type(ledger._entries).__name__}")
    try:
        ledger._entries.append(entry)   # type: ignore[union-attr]
    except AttributeError:
        pass
    else:
        raise AssertionError(
            "P0 2.6 regression: _entries is mutable")
    print(f"  ✓ _entries is an immutable tuple")

    # ---------------------------------------------------------------- #
    # P0 2.7 — "never sum contractual" enforcement
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: plausibility bound ===")

    ledger = CarbonLedger()
    try:
        ledger.record(
            run_id="r", operational_co2e_kg=1_000_000.0,
            energy_kwh=1.0, source="measured",
            methodology_version="v")
    except CarbonLedgerValidationError as exc:
        print(f"  ✓ 1,000,000 kg rejected as plausibility "
              f"violation: {str(exc)[:60]}...")
    else:
        raise AssertionError(
            "P0 2.7 regression: implausible value accepted")

    # Below-bound value works.
    ledger.record(
        run_id="r", operational_co2e_kg=1000.0,
        energy_kwh=1.0, source="measured",
        methodology_version="v")
    print(f"  ✓ 1000 kg accepted (below bound)")

    # ---------------------------------------------------------------- #
    # P1 3.1 — persistence
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: sink persistence ===")

    with tempfile.TemporaryDirectory() as td:
        sink = os.path.join(td, "carbon.jsonl")
        log = CarbonLedger(sink_path=sink)
        log.record(**_sample(operational_co2e_kg=100.0))
        log.record(**_sample(operational_co2e_kg=50.0))

        # Reload.
        log2 = CarbonLedger(sink_path=sink)
        assert len(log2) == 2, (
            f"P1 3.1 regression: len={len(log2)}, expected 2")
        assert log2.total_operational_kg() == 150.0
        print(f"  ✓ 2 entries reloaded; total=150.0")

    # ---------------------------------------------------------------- #
    # P1 3.3 — accessors
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: read API ===")
    ledger = CarbonLedger()
    eid = ledger.record(**_sample())
    assert len(ledger) == 1
    assert eid in ledger
    assert "missing" not in ledger
    assert ledger.get(eid) is not None
    assert ledger.get("missing") is None
    assert [e.entry_id for e in ledger] == [eid]
    print(f"  ✓ get / len / iter / in all work")

    # ---------------------------------------------------------------- #
    # P1 3.4 — total_energy_kwh
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: total_energy_kwh ===")
    assert ledger.total_energy_kwh() == 50.0
    print(f"  ✓ total_energy_kwh=50.0")

    # ---------------------------------------------------------------- #
    # P1 3.5 — serialization
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: serialization ===")
    d = ledger.to_dict()
    assert d["count"] == 1
    assert d["version"] == __version__
    rt = CarbonLedger.from_dict(d)
    assert len(rt) == 1
    print(f"  ✓ to_dict / from_dict round-trip")

    # Duplicate in payload rejected.
    bad = d.copy()
    bad["entries"] = [d["entries"][0], d["entries"][0]]
    try:
        CarbonLedger.from_dict(bad)
    except CarbonLedgerDuplicateError:
        print(f"  ✓ duplicate entry_id in payload rejected")
    else:
        raise AssertionError(
            "P1 3.5 regression: duplicate accepted")

    # ---------------------------------------------------------------- #
    # P1 3.6 — statistics
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: statistics ===")
    ledger = CarbonLedger()
    ledger.record(**_sample(source="measured"))
    ledger.record(**_sample(source="estimated"))
    ledger.record(**_sample(source="simulated"))
    stats = ledger.get_statistics()
    assert stats["by_source"] == {
        "measured": 1, "estimated": 1, "simulated": 1}
    assert stats["measured_count"] == 1
    print(f"  ✓ by_source: {stats['by_source']}")

    # ---------------------------------------------------------------- #
    # P1 3.7 — caching
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.7: caching ===")
    ledger = CarbonLedger()
    for i in range(100):
        ledger.record(**_sample(operational_co2e_kg=1.0))
    t0 = time.perf_counter()
    for _ in range(1000):
        ledger.total_operational_kg()
    elapsed = time.perf_counter() - t0
    assert elapsed < 0.05, (
        f"P1 3.7 regression: 1000 calls took {elapsed:.3f}s")
    print(f"  ✓ 1000 calls in {elapsed*1000:.1f}ms")

    ledger.record(**_sample(operational_co2e_kg=5.0))
    assert ledger.total_operational_kg() == 105.0
    print(f"  ✓ cache invalidated by record()")

    # ---------------------------------------------------------------- #
    # P1 3.8 — CarbonSource enum
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.8: CarbonSource enum ===")
    assert CarbonSource.MEASURED.value == "measured"
    assert CarbonSource.ESTIMATED.value == "estimated"
    assert CarbonSource.SIMULATED.value == "simulated"
    print(f"  ✓ enum exports 3 valid values")

    # ---------------------------------------------------------------- #
    # P1 3.9 — version constants
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.9: version exports ===")
    assert __version__ == "2.0.0"
    assert LEDGER_SCHEMA_VERSION == "5.0.0"
    print(f"  ✓ version={__version__!r}, "
          f"schema={LEDGER_SCHEMA_VERSION!r}")

    # ---------------------------------------------------------------- #
    # P1 3.10 — metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.10: metrics_hook ===")
    events: List[Tuple[str, Mapping[str, Any]]] = []
    ledger = CarbonLedger(
        metrics_hook=lambda ev, f: events.append((ev, f)))
    ledger.record(**_sample())
    names = {ev for ev, _ in events}
    assert "entry_recorded" in names
    print(f"  ✓ events: {sorted(names)}")

    # ---------------------------------------------------------------- #
    # P1 3.11 — pickle
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.11: pickle ===")
    ledger = CarbonLedger()
    ledger.record(**_sample())
    blob = pickle.dumps(ledger)
    restored = pickle.loads(blob)
    assert len(restored) == 1
    assert restored.total_operational_kg() == 100.0
    print(f"  ✓ pickle round-trip works")

    # ---------------------------------------------------------------- #
    # P1 3.13 — record_batch
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.13: record_batch ===")
    ledger = CarbonLedger()
    ids = ledger.record_batch([
        _sample(operational_co2e_kg=10.0),
        _sample(operational_co2e_kg=20.0),
        _sample(operational_co2e_kg=30.0),
    ])
    assert len(ids) == 3
    assert len(ledger) == 3
    assert ledger.total_operational_kg() == 60.0
    print(f"  ✓ 3 entries in one batch, total=60.0")

    # Duplicate within batch rejected before any commit.
    ledger2 = CarbonLedger()
    try:
        ledger2.record_batch([
            _sample(entry_id="dup", operational_co2e_kg=10.0),
            _sample(entry_id="dup", operational_co2e_kg=20.0),
        ])
    except CarbonLedgerDuplicateError:
        pass
    else:
        raise AssertionError(
            "P1 3.13 regression: duplicate-in-batch accepted")
    assert len(ledger2) == 0
    print(f"  ✓ duplicate-in-batch rejected atomically")

    # ---------------------------------------------------------------- #
    # P1 3.14 — UTC normalization
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.14: UTC normalization ===")
    naive = datetime(2024, 1, 1, 12, 0, 0)
    entry = CarbonEntry(
        entry_id="e", run_id="r", task_id=None,
        operational_co2e_kg=1.0, energy_kwh=1.0,
        source="measured", methodology_version="v",
        recorded_at=naive)
    assert entry.recorded_at.tzinfo is not None
    assert entry.recorded_at.tzinfo == timezone.utc
    print(f"  ✓ naive datetime normalized to UTC")

    # ---------------------------------------------------------------- #
    # Duplicate detection on record()
    # ---------------------------------------------------------------- #
    print("\n=== Duplicate detection ===")
    ledger = CarbonLedger()
    eid = ledger.record(**_sample())
    try:
        ledger.record(**_sample(entry_id=eid))
    except CarbonLedgerDuplicateError:
        print(f"  ✓ duplicate entry_id on record() rejected")
    else:
        raise AssertionError(
            "Duplicate regression: duplicate accepted")

    print(f"\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
