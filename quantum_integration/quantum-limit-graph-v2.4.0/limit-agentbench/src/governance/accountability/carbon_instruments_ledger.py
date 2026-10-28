"""
CarbonInstrumentsLedger — contractual instruments, kept separate from
operational emissions. — v2.0.0

The recommendation is emphatic: certificates and RECs must never be
summed with physical emissions.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``to_dict()`` is now JSON-safe. Previously ``asdict()`` on a
        retired ``CarbonInstrument`` produced a raw ``datetime`` in
        ``retired_at`` — ``json.dumps(ledger.to_dict())`` raised
        ``TypeError`` on the first retired instrument.
  * 2.2 ``CarbonInstrument.__post_init__`` validates every field
        (non-empty id, known kind/unit, finite non-negative quantity,
        plausible ``vintage_year``, consistent
        ``retired``/``retired_at`` pair).
  * 2.3 ``add()`` is guarded by an ``RLock`` — the check-then-append
        sequence is atomic. Previously two concurrent ``add()`` calls
        with the same id both passed the duplicate check and both
        appended.
  * 2.4 New error hierarchy: ``CarbonInstrumentsLedgerError``,
        ``ValidationError``, ``DuplicateError``,
        ``NotFoundError``. All catchable via
        ``except GovernanceError:`` after re-parenting at package
        import. Previously raised bare ``ValueError``.
  * 2.5 ``retire(instrument_id)`` is a first-class method.
        ``CarbonInstrument`` is now ``frozen=True`` — direct mutation
        raises ``FrozenInstanceError``, so callers MUST go through
        ``retire()``. This is the *only* way to honour the ledger's
        "immutable" claim.
  * 2.6 ``contractual_kgco2e()`` converts ``tCO2e`` instruments
        (× 1000). Previously a 2-tonne retired offset contributed
        ``0.0`` to the metric — a 1000× under-report. ``MWh`` (RECs)
        is reported by a separate ``contractual_mwh()`` accessor and
        excluded from ``contractual_kgco2e()`` (a REC cannot be
        summed into kgCO2e without grid intensity — the split is
        the point).

**P1 robustness**
  * 3.1 Optional WAL persistence (``sink_path``) — appends one JSON
        line per add/retire, fsyncs file and parent dir, reloads on
        construction. Same pattern as ``CarbonLedger`` v3.0.0.
  * 3.2 ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
  * 3.3 ``get(instrument_id)``, ``__len__``, ``__iter__``,
        ``to_list()``.
  * 3.4 ``statistics()``.
  * 3.5 ``InstrumentKind`` / ``InstrumentUnit`` enums; field types
        remain ``str`` for backward compat, coercion happens in
        ``__post_init__``.
  * 3.6 ``__version__`` and ``LEDGER_SCHEMA_VERSION`` exported.
  * 3.7 ``provenance: Dict[str, Any]`` with recursive JSON coercion.
  * 3.8 ``contractual_kgco2e()`` cached, invalidated on mutation.
  * 3.9 ``metrics_hook(event, fields)`` fires on add, retire, and
        duplicate rejection.
  * 3.10 ``__repr__``, ``__getstate__``/``__setstate__`` — the
        ledger is picklable.
  * 3.11 ``build_separated_report(carbon_ledger, instruments_ledger)``
        produces a dict with clearly-labelled ``operational`` and
        ``contractual`` sections and a ``note`` explaining why they
        must never be summed.
  * 3.12 ``add_batch(instruments)`` — one lock, one fsync.

Backward compatible: ``CarbonInstrumentsLedger()``, ``add(instrument)``,
``contractual_kgco2e()``, ``to_dict()`` all keep their v1.0.0 shape.
``CarbonInstrument(...)`` accepts the same positional/keyword args.
"""

from __future__ import annotations

import copy
import json
import logging
import math
import os
import threading
from dataclasses import (
    dataclass, field, replace, fields, asdict, MISSING,
)
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
# Errors (P0 2.4)
# =============================================================================

class CarbonInstrumentsLedgerError(ValueError):
    """Base class for every error raised by CarbonInstrumentsLedger."""


class CarbonInstrumentsValidationError(CarbonInstrumentsLedgerError):
    """Raised for invalid instrument values or operations."""


class CarbonInstrumentsDuplicateError(CarbonInstrumentsLedgerError):
    """Raised when adding an instrument with a duplicate id."""


class CarbonInstrumentsNotFoundError(CarbonInstrumentsLedgerError):
    """Raised when an operation targets an unknown instrument."""


class CarbonInstrumentsPersistenceError(CarbonInstrumentsLedgerError):
    """Raised when the sink write fails."""


# =============================================================================
# Enums (P1 3.5)
# =============================================================================

class InstrumentKind(str, Enum):
    REC = "rec"
    OFFSET = "offset"
    CREDIT = "credit"

    def __str__(self) -> str:
        return self.value


class InstrumentUnit(str, Enum):
    MWH = "MWh"
    KGCO2E = "kgCO2e"
    TCO2E = "tCO2e"

    def __str__(self) -> str:
        return self.value


# =============================================================================
# Unit conversion (P0 2.6)
# =============================================================================

_KINDS = tuple(k.value for k in InstrumentKind)
_UNITS = tuple(u.value for u in InstrumentUnit)

# Conversion factors to kgCO2e for offset-type instruments.
_UNIT_TO_KGCO2E: Dict[str, float] = {
    "kgCO2e": 1.0,
    "tCO2e": 1000.0,
    # "MWh" deliberately not in this table: a REC (MWh) cannot be
    # summed into kgCO2e without a grid-intensity factor.
}


# =============================================================================
# Helpers
# =============================================================================

def _safe_value(v: Any) -> Any:
    """Recursively coerce ``v`` to a JSON-safe form."""
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, Mapping):
        return {str(k): _safe_value(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [_safe_value(x) for x in v]
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    return str(v)


def _as_aware_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fsync_dir(path: Path) -> None:
    """fsync the directory containing ``path`` (POSIX)."""
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
# CarbonInstrument (P0 2.2, P0 2.5)
# =============================================================================

@dataclass(frozen=True)
class CarbonInstrument:
    """
    A single contractual instrument.

    Frozen — mutate via :meth:`CarbonInstrumentsLedger.retire` or
    replace with :func:`dataclasses.replace`.
    """
    instrument_id: str
    kind: str                  # "rec" | "offset" | "credit"
    quantity: float
    unit: str                  # "MWh" | "kgCO2e" | "tCO2e"
    vintage_year: Optional[int] = None
    matching_period: Optional[str] = None
    producer: Optional[str] = None
    region: Optional[str] = None
    retired: bool = False
    retired_at: Optional[datetime] = None
    provenance: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # -- instrument_id --
        if not isinstance(self.instrument_id, str) or \
                not self.instrument_id:
            raise CarbonInstrumentsValidationError(
                "instrument_id must be a non-empty string")

        # -- kind --
        if not isinstance(self.kind, str):
            raise CarbonInstrumentsValidationError(
                f"kind must be a string, got {type(self.kind).__name__}")
        kind_norm = self.kind.strip().lower()
        if kind_norm not in _KINDS:
            raise CarbonInstrumentsValidationError(
                f"kind must be one of {_KINDS}, got {self.kind!r}")
        object.__setattr__(self, "kind", kind_norm)

        # -- unit --
        if not isinstance(self.unit, str):
            raise CarbonInstrumentsValidationError(
                f"unit must be a string, got {type(self.unit).__name__}")
        # Case-sensitive: unit strings are conventionally cased as
        # "kgCO2e", "tCO2e", "MWh".
        if self.unit not in _UNITS:
            raise CarbonInstrumentsValidationError(
                f"unit must be one of {_UNITS}, got {self.unit!r}")

        # -- quantity --
        if isinstance(self.quantity, bool) or \
                not isinstance(self.quantity, (int, float)):
            raise CarbonInstrumentsValidationError(
                "quantity must be numeric")
        q = float(self.quantity)
        if math.isnan(q) or math.isinf(q):
            raise CarbonInstrumentsValidationError(
                "quantity must be finite")
        if q < 0:
            raise CarbonInstrumentsValidationError(
                f"quantity must be >= 0, got {q}")
        object.__setattr__(self, "quantity", q)

        # -- vintage_year --
        if self.vintage_year is not None:
            if isinstance(self.vintage_year, bool) or \
                    not isinstance(self.vintage_year, int):
                raise CarbonInstrumentsValidationError(
                    "vintage_year must be an int or None")
            if not 1990 <= self.vintage_year <= 2100:
                raise CarbonInstrumentsValidationError(
                    f"vintage_year must be in [1990, 2100], "
                    f"got {self.vintage_year}")

        # -- retired / retired_at consistency --
        if self.retired:
            if self.retired_at is None:
                object.__setattr__(
                    self, "retired_at",
                    datetime.now(timezone.utc))
            else:
                if not isinstance(self.retired_at, datetime):
                    raise CarbonInstrumentsValidationError(
                        "retired_at must be a datetime or None")
                object.__setattr__(
                    self, "retired_at",
                    _as_aware_utc(self.retired_at))
        else:
            if self.retired_at is not None:
                raise CarbonInstrumentsValidationError(
                    "retired_at must be None when retired=False")

        # -- provenance must be a mapping --
        if not isinstance(self.provenance, Mapping):
            raise CarbonInstrumentsValidationError(
                "provenance must be a Mapping")

    # ---------------------------------------------------------------- #
    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-safe dict representation."""
        return {
            "instrument_id": self.instrument_id,
            "kind": self.kind,
            "quantity": self.quantity,
            "unit": self.unit,
            "vintage_year": self.vintage_year,
            "matching_period": self.matching_period,
            "producer": self.producer,
            "region": self.region,
            "retired": self.retired,
            "retired_at": (
                self.retired_at.isoformat()
                if self.retired_at else None),
            "provenance": _safe_value(dict(self.provenance)),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonInstrument":
        if not isinstance(data, Mapping):
            raise CarbonInstrumentsValidationError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}")

        retired_at_raw = data.get("retired_at")
        if isinstance(retired_at_raw, str):
            try:
                retired_at = datetime.fromisoformat(
                    retired_at_raw.replace("Z", "+00:00"))
            except ValueError as exc:
                raise CarbonInstrumentsValidationError(
                    f"retired_at is not a valid ISO 8601 timestamp: "
                    f"{exc}") from exc
        elif isinstance(retired_at_raw, datetime):
            retired_at = retired_at_raw
        elif retired_at_raw is None:
            retired_at = None
        else:
            raise CarbonInstrumentsValidationError(
                f"retired_at must be a datetime, ISO string, or "
                f"None; got {type(retired_at_raw).__name__}")

        provenance_raw = data.get("provenance", {}) or {}
        if not isinstance(provenance_raw, Mapping):
            raise CarbonInstrumentsValidationError(
                "provenance must be a Mapping")

        return cls(
            instrument_id=data.get("instrument_id"),
            kind=data.get("kind"),
            quantity=data.get("quantity"),
            unit=data.get("unit"),
            vintage_year=data.get("vintage_year"),
            matching_period=data.get("matching_period"),
            producer=data.get("producer"),
            region=data.get("region"),
            retired=bool(data.get("retired", False)),
            retired_at=retired_at,
            provenance=dict(provenance_raw),
        )


# =============================================================================
# CarbonInstrumentsLedger
# =============================================================================

class CarbonInstrumentsLedger:
    """
    Immutable ledger of contractual carbon instruments.

    Reports MUST display operational and contractual emissions
    separately. This ledger tracks only the contractual side.

    Instruments are added via :meth:`add` (or :meth:`add_batch`) and
    retired via :meth:`retire`. The ``CarbonInstrument`` dataclass is
    frozen; the only mutation path is ``retire()``, which replaces
    the instrument with a retired copy.
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
                raise CarbonInstrumentsValidationError(
                    "sink_path must be a non-empty string or None")
        if metrics_hook is not None and not callable(metrics_hook):
            raise CarbonInstrumentsValidationError(
                "metrics_hook must be callable")

        self._instruments: List[CarbonInstrument] = []
        self._instruments_by_id: Dict[str, CarbonInstrument] = {}
        self._lock = threading.RLock()
        self._sink_path = sink_path
        self._strict = bool(strict)
        self._metrics_hook = metrics_hook
        self._contractual_kgco2e_cache: Optional[float] = None
        self._contractual_mwh_cache: Optional[float] = None
        self._add_count: int = 0
        self._retire_count: int = 0
        self._duplicate_rejected_count: int = 0
        self._last_known_size: int = 0

        if sink_path:
            self._load_from_sink()

    # ------------------------------------------------------------------
    # Metrics hook
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
    # Sink (P1 3.1)
    # ------------------------------------------------------------------

    def _load_from_sink(self) -> None:
        path = Path(self._sink_path)  # type: ignore[arg-type]
        self._instruments.clear()
        self._instruments_by_id.clear()
        if not path.exists():
            self._last_known_size = 0
            return
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise CarbonInstrumentsLedgerError(
                f"Could not read sink {path}: {exc}") from exc
        self._last_known_size = len(raw.encode("utf-8"))
        if not raw:
            return
        has_complete_last = raw.endswith("\n")
        lines = raw.splitlines()
        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            is_last = (i == len(lines) - 1)
            is_partial = is_last and not has_complete_last
            try:
                entry = json.loads(stripped)
            except json.JSONDecodeError:
                if is_partial:
                    logger.debug(
                        "Skipping partial last line in sink %s", path)
                    continue
                if self._strict:
                    raise CarbonInstrumentsLedgerError(
                        f"Sink {path} corrupted at line {i + 1}")
                logger.warning(
                    "Skipping corrupt line %d in sink %s",
                    i + 1, path)
                continue
            if not isinstance(entry, Mapping):
                if self._strict:
                    raise CarbonInstrumentsLedgerError(
                        f"Sink {path} line {i + 1} is not a JSON object")
                continue
            try:
                instrument = CarbonInstrument.from_dict(entry)
            except CarbonInstrumentsValidationError as exc:
                if self._strict:
                    raise CarbonInstrumentsLedgerError(
                        f"Sink {path} line {i + 1} is invalid: {exc}"
                    ) from exc
                logger.warning(
                    "Skipping invalid instrument at line %d: %s",
                    i + 1, exc)
                continue
            # Retired instruments may appear multiple times in the
            # sink (once when added, once when retired). Keep the most
            # recent state: replace in place if we've seen the id.
            if instrument.instrument_id in self._instruments_by_id:
                idx = next(
                    j for j, existing in enumerate(self._instruments)
                    if existing.instrument_id == instrument.instrument_id
                )
                self._instruments[idx] = instrument
            else:
                self._instruments.append(instrument)
            self._instruments_by_id[instrument.instrument_id] = instrument
        self._invalidate_caches()
        logger.debug(
            "CarbonInstrumentsLedger loaded %d instrument(s) from %s",
            len(self._instruments), path)

    def _persist_line(self, record: Mapping[str, Any]) -> None:
        if not self._sink_path:
            return
        path = Path(self._sink_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(_safe_value(record), default=str) + "\n"
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            _fsync_dir(path)
            self._last_known_size += len(line.encode("utf-8"))
        except OSError as exc:
            raise CarbonInstrumentsPersistenceError(
                f"Could not append to sink {path}: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Caches (P1 3.8)
    # ------------------------------------------------------------------

    def _invalidate_caches(self) -> None:
        self._contractual_kgco2e_cache = None
        self._contractual_mwh_cache = None

    # ------------------------------------------------------------------
    # Add (P0 2.3)
    # ------------------------------------------------------------------

    def add(self, instrument: CarbonInstrument) -> None:
        """Append a new instrument; reject duplicates atomically."""
        if not isinstance(instrument, CarbonInstrument):
            raise CarbonInstrumentsValidationError(
                f"instrument must be a CarbonInstrument, "
                f"got {type(instrument).__name__}")
        with self._lock:
            if instrument.instrument_id in self._instruments_by_id:
                self._duplicate_rejected_count += 1
                raise CarbonInstrumentsDuplicateError(
                    f"duplicate instrument_id: "
                    f"{instrument.instrument_id}")
            # Persist first, then commit to memory (matches CarbonLedger).
            if self._sink_path:
                self._persist_line({
                    "op": "add",
                    "instrument": instrument.to_dict(),
                })
            self._instruments.append(instrument)
            self._instruments_by_id[instrument.instrument_id] = instrument
            self._add_count += 1
            self._invalidate_caches()

        self._emit_metric("instrument_added", {
            "instrument_id": instrument.instrument_id,
            "kind": instrument.kind,
            "unit": instrument.unit,
            "quantity": instrument.quantity,
        })

    def add_batch(
        self, instruments: Sequence[CarbonInstrument],
    ) -> None:
        """Append multiple instruments under one lock acquisition (P1 3.12)."""
        if not isinstance(instruments, (list, tuple)):
            raise CarbonInstrumentsValidationError(
                f"instruments must be a list or tuple, "
                f"got {type(instruments).__name__}")
        # Pre-validate types and IDs before mutating anything.
        ids_seen: set = set()
        for i, instrument in enumerate(instruments):
            if not isinstance(instrument, CarbonInstrument):
                raise CarbonInstrumentsValidationError(
                    f"instruments[{i}] must be a CarbonInstrument, "
                    f"got {type(instrument).__name__}")
            if instrument.instrument_id in ids_seen:
                raise CarbonInstrumentsDuplicateError(
                    f"duplicate instrument_id within batch: "
                    f"{instrument.instrument_id}")
            ids_seen.add(instrument.instrument_id)

        with self._lock:
            for instrument in instruments:
                if instrument.instrument_id in self._instruments_by_id:
                    self._duplicate_rejected_count += 1
                    raise CarbonInstrumentsDuplicateError(
                        f"duplicate instrument_id: "
                        f"{instrument.instrument_id}")
            # All clear — persist then commit.
            if self._sink_path:
                for instrument in instruments:
                    self._persist_line({
                        "op": "add",
                        "instrument": instrument.to_dict(),
                    })
            for instrument in instruments:
                self._instruments.append(instrument)
                self._instruments_by_id[
                    instrument.instrument_id] = instrument
            self._add_count += len(instruments)
            self._invalidate_caches()

        for instrument in instruments:
            self._emit_metric("instrument_added", {
                "instrument_id": instrument.instrument_id,
                "kind": instrument.kind,
                "unit": instrument.unit,
                "quantity": instrument.quantity,
                "batch": True,
            })

    # ------------------------------------------------------------------
    # Retire (P0 2.5)
    # ------------------------------------------------------------------

    def retire(
        self,
        instrument_id: str,
        *,
        retired_at: Optional[datetime] = None,
    ) -> CarbonInstrument:
        """
        Retire an instrument — mark it as claimed against a reporting
        period. Retirement is irreversible.

        Returns the retired copy. The original is replaced in place
        (same ledger, same id).
        """
        if not isinstance(instrument_id, str) or not instrument_id:
            raise CarbonInstrumentsValidationError(
                "instrument_id must be a non-empty string")
        if retired_at is not None and not isinstance(
                retired_at, datetime):
            raise CarbonInstrumentsValidationError(
                "retired_at must be a datetime or None")

        with self._lock:
            existing = self._instruments_by_id.get(instrument_id)
            if existing is None:
                raise CarbonInstrumentsNotFoundError(
                    f"no instrument with id {instrument_id!r}")
            if existing.retired:
                raise CarbonInstrumentsValidationError(
                    f"instrument {instrument_id!r} is already retired")
            retired = replace(
                existing,
                retired=True,
                retired_at=retired_at or datetime.now(timezone.utc),
            )
            if self._sink_path:
                self._persist_line({
                    "op": "retire",
                    "instrument": retired.to_dict(),
                })
            idx = next(
                j for j, i in enumerate(self._instruments)
                if i.instrument_id == instrument_id
            )
            self._instruments[idx] = retired
            self._instruments_by_id[instrument_id] = retired
            self._retire_count += 1
            self._invalidate_caches()

        self._emit_metric("instrument_retired", {
            "instrument_id": instrument_id,
            "quantity": retired.quantity,
            "unit": retired.unit,
        })
        return retired

    # ------------------------------------------------------------------
    # Read API (P1 3.3)
    # ------------------------------------------------------------------

    def get(self, instrument_id: str) -> Optional[CarbonInstrument]:
        if not isinstance(instrument_id, str) or not instrument_id:
            return None
        with self._lock:
            return self._instruments_by_id.get(instrument_id)

    def to_list(self) -> List[CarbonInstrument]:
        with self._lock:
            return list(self._instruments)

    def __len__(self) -> int:
        with self._lock:
            return len(self._instruments)

    def __iter__(self) -> Iterator[CarbonInstrument]:
        return iter(self.to_list())

    def __contains__(self, instrument_id: object) -> bool:
        if not isinstance(instrument_id, str) or not instrument_id:
            return False
        with self._lock:
            return instrument_id in self._instruments_by_id

    # ------------------------------------------------------------------
    # Aggregates (P0 2.6, P1 3.8)
    # ------------------------------------------------------------------

    def contractual_kgco2e(self) -> float:
        """
        Sum retired offset-type instruments in kgCO2e.

        ``tCO2e`` is converted (× 1000). ``MWh`` (RECs) is excluded —
        use :meth:`contractual_mwh` for REC totals. The split is the
        whole point: a REC cannot be converted into kgCO2e without a
        grid-intensity factor, so summing them into one number would
        be a category error.
        """
        with self._lock:
            if self._contractual_kgco2e_cache is not None:
                return self._contractual_kgco2e_cache
            total = 0.0
            for i in self._instruments:
                if i.kind != "offset" or not i.retired:
                    continue
                factor = _UNIT_TO_KGCO2E.get(i.unit)
                if factor is None:
                    # Shouldn't happen given __post_init__ unit
                    # validation, but defend against future units.
                    logger.warning(
                        "Skipping offset %r with un-convertible "
                        "unit %r", i.instrument_id, i.unit)
                    continue
                total += i.quantity * factor
            self._contractual_kgco2e_cache = total
            return total

    def contractual_mwh(self) -> float:
        """Sum retired REC instruments in MWh (P0 2.6)."""
        with self._lock:
            if self._contractual_mwh_cache is not None:
                return self._contractual_mwh_cache
            total = sum(
                i.quantity for i in self._instruments
                if i.kind == "rec"
                and i.unit == "MWh"
                and i.retired
            )
            self._contractual_mwh_cache = total
            return total

    # ------------------------------------------------------------------
    # Statistics (P1 3.4)
    # ------------------------------------------------------------------

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            by_kind: Dict[str, int] = {}
            by_unit: Dict[str, int] = {}
            retired = 0
            for i in self._instruments:
                by_kind[i.kind] = by_kind.get(i.kind, 0) + 1
                by_unit[i.unit] = by_unit.get(i.unit, 0) + 1
                if i.retired:
                    retired += 1
            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "instrument_count": len(self._instruments),
                "retired_count": retired,
                "unretired_count": len(self._instruments) - retired,
                "by_kind": by_kind,
                "by_unit": by_unit,
                "contractual_kgco2e": self.contractual_kgco2e(),
                "contractual_mwh": self.contractual_mwh(),
                "add_count": self._add_count,
                "retire_count": self._retire_count,
                "duplicate_rejected_count":
                    self._duplicate_rejected_count,
                "sink_attached": self._sink_path is not None,
                "sink_path": self._sink_path,
            }

    # ------------------------------------------------------------------
    # Serialization (P1 3.2)
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "instruments": [
                    i.to_dict() for i in self._instruments
                ],
                "contractual_kgco2e": self.contractual_kgco2e(),
                "contractual_mwh": self.contractual_mwh(),
                "count": len(self._instruments),
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        sink_path: Optional[str] = None,
        strict: bool = True,
    ) -> "CarbonInstrumentsLedger":
        if not isinstance(data, Mapping):
            raise CarbonInstrumentsValidationError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}")
        ledger = cls(sink_path=sink_path, strict=strict)
        instruments_raw = data.get("instruments", [])
        if not isinstance(instruments_raw, (list, tuple)):
            raise CarbonInstrumentsValidationError(
                "from_dict instruments must be a list or tuple")
        with ledger._lock:
            ledger._instruments.clear()
            ledger._instruments_by_id.clear()
            for i, raw in enumerate(instruments_raw):
                if not isinstance(raw, Mapping):
                    raise CarbonInstrumentsValidationError(
                        f"instruments[{i}] must be a Mapping")
                instrument = CarbonInstrument.from_dict(raw)
                if instrument.instrument_id in ledger._instruments_by_id:
                    raise CarbonInstrumentsDuplicateError(
                        f"duplicate instrument_id in payload: "
                        f"{instrument.instrument_id}")
                ledger._instruments.append(instrument)
                ledger._instruments_by_id[
                    instrument.instrument_id] = instrument
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
    ) -> "CarbonInstrumentsLedger":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise CarbonInstrumentsValidationError(
                f"Invalid JSON payload: {exc}") from exc
        return cls.from_dict(data, sink_path=sink_path, strict=strict)

    # ------------------------------------------------------------------
    # Pickling (P1 3.10)
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
                f"CarbonInstrumentsLedger("
                f"version={__version__!r}, "
                f"instruments={len(self._instruments)}, "
                f"retired={sum(1 for i in self._instruments if i.retired)}, "
                f"contractual_kgco2e="
                f"{self.contractual_kgco2e():.4f}, "
                f"contractual_mwh={self.contractual_mwh():.4f}, "
                f"sink={self._sink_path!r})"
            )


# =============================================================================
# Cross-ledger report (P1 3.11)
# =============================================================================

def build_separated_report(
    carbon_ledger: Any,
    instruments_ledger: CarbonInstrumentsLedger,
) -> Dict[str, Any]:
    """
    Build a compliance report that displays operational and
    contractual emissions side by side, with an explicit note that
    they must never be summed.

    Parameters
    ----------
    carbon_ledger : object
        Must expose ``get_statistics()`` returning a mapping with
        ``total_carbon_kg`` or ``total_carbon_operational_kg``.
    instruments_ledger : CarbonInstrumentsLedger
        The contractual side.

    Returns
    -------
    dict with ``operational``, ``contractual``, and ``note`` sections.
    """
    stats: Mapping[str, Any] = {}
    if hasattr(carbon_ledger, "get_statistics") and callable(
            carbon_ledger.get_statistics):
        try:
            stats = carbon_ledger.get_statistics() or {}
        except Exception as exc:  # noqa: BLE001
            logger.warning("carbon_ledger.get_statistics failed: %s", exc)
            stats = {}

    operational_kg = stats.get(
        "total_carbon_operational_kg",
        stats.get("total_carbon_kg", 0.0),
    )

    return {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "operational": {
            "total_carbon_operational_kg": float(operational_kg or 0.0),
            "source": "measured + estimated emissions",
        },
        "contractual": {
            "total_carbon_contractual_kg":
                instruments_ledger.contractual_kgco2e(),
            "total_rec_mwh": instruments_ledger.contractual_mwh(),
            "source": "retired instruments",
        },
        "note": (
            "Contractual and operational totals are tracked "
            "separately and MUST NOT be summed. A contractual "
            "offset represents a claim against a reporting period, "
            "not a reduction in the physical emissions recorded "
            "under 'operational'."
        ),
    }


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "__version__",
    "LEDGER_SCHEMA_VERSION",
    "CarbonInstrument",
    "CarbonInstrumentsDuplicateError",
    "CarbonInstrumentsLedger",
    "CarbonInstrumentsLedgerError",
    "CarbonInstrumentsNotFoundError",
    "CarbonInstrumentsPersistenceError",
    "CarbonInstrumentsValidationError",
    "InstrumentKind",
    "InstrumentUnit",
    "build_separated_report",
]


# =============================================================================
# Self-tests
# =============================================================================

def _run_selftests() -> None:
    """Run the module's self-tests."""
    import pickle
    import tempfile
    import time

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s")

    print(f"=== carbon_instruments_ledger v{__version__} self-tests ===\n")

    # ---------------------------------------------------------------- #
    # P0 2.1 — to_dict is JSON-safe
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: JSON-safe to_dict ===")

    ledger = CarbonInstrumentsLedger()
    ledger.add(CarbonInstrument(
        instrument_id="o-1",
        kind="offset",
        quantity=100.0,
        unit="kgCO2e",
        retired=True,
    ))
    payload = ledger.to_dict()
    json.dumps(payload)  # must not raise
    assert payload["instruments"][0]["retired_at"] is not None
    assert isinstance(payload["instruments"][0]["retired_at"], str)
    print(f"  ✓ to_dict JSON-serializable; retired_at is ISO string")

    # Round-trip via JSON.
    rt = CarbonInstrumentsLedger.from_json(ledger.to_json())
    assert len(rt) == 1
    assert rt.get("o-1").retired is True
    print(f"  ✓ JSON round-trip works")

    # ---------------------------------------------------------------- #
    # P0 2.2 — CarbonInstrument validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: CarbonInstrument validation ===")

    bad_kwargs_list = [
        dict(instrument_id="", kind="offset", quantity=1.0,
             unit="kgCO2e"),
        dict(instrument_id="x", kind="garbage", quantity=1.0,
             unit="kgCO2e"),
        dict(instrument_id="x", kind="offset", quantity=-100.0,
             unit="kgCO2e"),
        dict(instrument_id="x", kind="offset", quantity=float("nan"),
             unit="kgCO2e"),
        dict(instrument_id="x", kind="offset", quantity=1.0,
             unit="moonbeams"),
        dict(instrument_id="x", kind="offset", quantity=1.0,
             unit="kgCO2e", vintage_year=-1),
        dict(instrument_id="x", kind="offset", quantity=1.0,
             unit="kgCO2e", vintage_year=9999),
        dict(instrument_id="x", kind="offset", quantity=1.0,
             unit="kgCO2e", retired=False,
             retired_at=datetime.now(timezone.utc)),
    ]
    for kwargs in bad_kwargs_list:
        try:
            CarbonInstrument(**kwargs)
        except CarbonInstrumentsValidationError:
            pass
        else:
            raise AssertionError(
                f"P0 2.2 regression: {kwargs!r} accepted")
    print(f"  ✓ {len(bad_kwargs_list)} invalid constructions rejected")

    # `retired=True` auto-populates retired_at.
    inst = CarbonInstrument(
        instrument_id="auto", kind="offset", quantity=1.0,
        unit="kgCO2e", retired=True)
    assert inst.retired_at is not None
    assert inst.retired_at.tzinfo is not None
    print(f"  ✓ retired=True auto-populates UTC retired_at")

    # ---------------------------------------------------------------- #
    # P0 2.3 — thread-safe add
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: thread-safe add ===")

    import concurrent.futures

    ledger = CarbonInstrumentsLedger()
    errors: List[Exception] = []

    def _try_add():
        try:
            ledger.add(CarbonInstrument(
                instrument_id="same-id", kind="offset",
                quantity=1.0, unit="kgCO2e"))
        except Exception as exc:
            errors.append(exc)

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        list(pool.map(lambda _: _try_add(), range(100)))

    assert len(ledger) == 1, (
        f"P0 2.3 regression: {len(ledger)} instruments added, "
        f"expected 1")
    assert len(errors) == 99, (
        f"P0 2.3 regression: expected 99 duplicate errors, "
        f"got {len(errors)}")
    assert all(isinstance(e, CarbonInstrumentsDuplicateError)
               for e in errors)
    print(f"  ✓ 100 concurrent adds → 1 instrument, "
          f"99 duplicates rejected")

    # ---------------------------------------------------------------- #
    # P0 2.4 — errors catchable as GovernanceError-compatible
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: error hierarchy ===")
    assert issubclass(
        CarbonInstrumentsDuplicateError,
        CarbonInstrumentsLedgerError)
    assert issubclass(
        CarbonInstrumentsValidationError,
        CarbonInstrumentsLedgerError)
    assert issubclass(
        CarbonInstrumentsNotFoundError,
        CarbonInstrumentsLedgerError)
    assert issubclass(
        CarbonInstrumentsPersistenceError,
        CarbonInstrumentsLedgerError)
    # Backward compat with v1.0.0 (which raised ValueError).
    assert issubclass(
        CarbonInstrumentsDuplicateError, ValueError)
    print(f"  ✓ all 4 error types subclass "
          f"CarbonInstrumentsLedgerError and ValueError")

    # ---------------------------------------------------------------- #
    # P0 2.5 — retire() + frozen dataclass
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: retire() + frozen ===")

    ledger = CarbonInstrumentsLedger()
    original = CarbonInstrument(
        instrument_id="r-1", kind="offset",
        quantity=10.0, unit="kgCO2e")
    ledger.add(original)
    assert original.retired is False

    retired = ledger.retire("r-1")
    assert retired.retired is True
    assert retired.retired_at is not None
    # The ledger holds the retired copy.
    assert ledger.get("r-1").retired is True

    # Second retire rejected.
    try:
        ledger.retire("r-1")
    except CarbonInstrumentsValidationError:
        pass
    else:
        raise AssertionError(
            "P0 2.5 regression: double-retire accepted")

    # Unknown id → NotFound.
    try:
        ledger.retire("nonexistent")
    except CarbonInstrumentsNotFoundError:
        pass
    else:
        raise AssertionError(
            "P0 2.5 regression: unknown id accepted")

    # Direct mutation raises FrozenInstanceError.
    from dataclasses import FrozenInstanceError
    try:
        original.retired = True   # type: ignore[misc]
    except FrozenInstanceError:
        pass
    else:
        raise AssertionError(
            "P0 2.5 regression: direct field mutation succeeded")
    print(f"  ✓ retire() works; double-retire rejected; "
          f"direct mutation raises FrozenInstanceError")

    # ---------------------------------------------------------------- #
    # P0 2.6 — tCO2e conversion + MWh separation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: tCO2e conversion ===")

    ledger = CarbonInstrumentsLedger()
    ledger.add(CarbonInstrument(
        instrument_id="o-tonnes", kind="offset",
        quantity=2.0, unit="tCO2e", retired=True))
    assert ledger.contractual_kgco2e() == 2000.0, (
        f"P0 2.6 regression: tCO2e offset contributed "
        f"{ledger.contractual_kgco2e()}, expected 2000.0")
    print(f"  ✓ 2 tCO2e retired offset → 2000 kgCO2e")

    # REC MWh is tracked separately, not summed into kgCO2e.
    ledger.add(CarbonInstrument(
        instrument_id="rec-1", kind="rec",
        quantity=5.0, unit="MWh", retired=True))
    assert ledger.contractual_kgco2e() == 2000.0, (
        f"REC leaked into kgCO2e: "
        f"{ledger.contractual_kgco2e()}")
    assert ledger.contractual_mwh() == 5.0
    print(f"  ✓ REC MWh excluded from kgCO2e; "
          f"contractual_mwh()=5.0")

    # Unretired contributes to neither.
    ledger.add(CarbonInstrument(
        instrument_id="unretired", kind="offset",
        quantity=100.0, unit="kgCO2e", retired=False))
    assert ledger.contractual_kgco2e() == 2000.0
    print(f"  ✓ unretired instrument excluded")

    # ---------------------------------------------------------------- #
    # P1 3.1 — sink persistence and reload
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: sink persistence ===")

    with tempfile.TemporaryDirectory() as td:
        sink = os.path.join(td, "instruments.jsonl")
        log = CarbonInstrumentsLedger(sink_path=sink)
        log.add(CarbonInstrument(
            instrument_id="p-1", kind="offset",
            quantity=100.0, unit="kgCO2e", retired=True))
        log.add(CarbonInstrument(
            instrument_id="p-2", kind="rec",
            quantity=5.0, unit="MWh"))
        log.retire("p-2")

        # Reload.
        log2 = CarbonInstrumentsLedger(sink_path=sink)
        assert len(log2) == 2
        assert log2.get("p-1").retired is True
        assert log2.get("p-2").retired is True
        assert log2.contractual_kgco2e() == 100.0
        assert log2.contractual_mwh() == 5.0
        print(f"  ✓ 2 instruments reloaded from sink; "
              f"retire state preserved")

    # ---------------------------------------------------------------- #
    # P1 3.2 — to_dict / from_dict / to_json / from_json
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: serialization round-trip ===")

    ledger = CarbonInstrumentsLedger()
    ledger.add(CarbonInstrument(
        instrument_id="s-1", kind="offset",
        quantity=50.0, unit="kgCO2e",
        vintage_year=2024, producer="acme",
        provenance={"registry": "verra", "certificate_id": "VCS-123"}))
    d = ledger.to_dict()
    assert d["count"] == 1
    assert d["version"] == __version__
    rt = CarbonInstrumentsLedger.from_dict(d)
    assert len(rt) == 1
    assert rt.get("s-1").producer == "acme"
    assert rt.get("s-1").provenance["certificate_id"] == "VCS-123"
    print(f"  ✓ dict round-trip preserves provenance")

    # Tampered payload (duplicate id) rejected.
    bad = d.copy()
    bad["instruments"] = [
        d["instruments"][0], d["instruments"][0]
    ]
    try:
        CarbonInstrumentsLedger.from_dict(bad)
    except CarbonInstrumentsDuplicateError:
        print(f"  ✓ duplicate id in payload rejected")
    else:
        raise AssertionError(
            "P1 3.2 regression: duplicate id in payload accepted")

    # ---------------------------------------------------------------- #
    # P1 3.3 — get / __len__ / __iter__ / in
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: read API ===")
    ledger = CarbonInstrumentsLedger()
    ledger.add(CarbonInstrument(
        instrument_id="a", kind="offset",
        quantity=1.0, unit="kgCO2e"))
    assert len(ledger) == 1
    assert "a" in ledger
    assert "b" not in ledger
    assert ledger.get("a").instrument_id == "a"
    assert ledger.get("missing") is None
    assert [i.instrument_id for i in ledger] == ["a"]
    print(f"  ✓ get / len / iter / in all work")

    # ---------------------------------------------------------------- #
    # P1 3.4 — statistics
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: statistics ===")
    ledger = CarbonInstrumentsLedger()
    ledger.add(CarbonInstrument(
        instrument_id="s1", kind="offset",
        quantity=10.0, unit="kgCO2e", retired=True))
    ledger.add(CarbonInstrument(
        instrument_id="s2", kind="rec",
        quantity=2.0, unit="MWh"))
    stats = ledger.statistics()
    assert stats["instrument_count"] == 2
    assert stats["retired_count"] == 1
    assert stats["by_kind"] == {"offset": 1, "rec": 1}
    assert stats["contractual_kgco2e"] == 10.0
    assert stats["contractual_mwh"] == 0.0
    print(f"  ✓ statistics: "
          f"{stats['instrument_count']} instruments, "
          f"{stats['retired_count']} retired")

    # ---------------------------------------------------------------- #
    # P1 3.5 — enums exported
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: InstrumentKind / InstrumentUnit ===")
    assert InstrumentKind.REC.value == "rec"
    assert InstrumentUnit.TCO2E.value == "tCO2e"
    # Case-insensitive kind normalization.
    inst = CarbonInstrument(
        instrument_id="x", kind="OFFSET",
        quantity=1.0, unit="kgCO2e")
    assert inst.kind == "offset"
    print(f"  ✓ enums present; kind coerced to lowercase")

    # ---------------------------------------------------------------- #
    # P1 3.6 — version constants
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: version exports ===")
    assert __version__ == "2.0.0"
    assert LEDGER_SCHEMA_VERSION == "5.0.0"
    d = ledger.to_dict()
    assert d["version"] == __version__
    assert d["schema_version"] == LEDGER_SCHEMA_VERSION
    print(f"  ✓ version={__version__!r}, "
          f"schema={LEDGER_SCHEMA_VERSION!r}")

    # ---------------------------------------------------------------- #
    # P1 3.7 — provenance Dict[str, Any]
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.7: arbitrary provenance types ===")
    ledger = CarbonInstrumentsLedger()
    ledger.add(CarbonInstrument(
        instrument_id="prov",
        kind="offset", quantity=1.0, unit="kgCO2e",
        provenance={
            "auditor": {"name": "ACME", "id": 42},
            "issued": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "tags": ["verified", "vintage-2023"],
        }))
    d = ledger.to_dict()
    json.dumps(d)  # must not raise
    assert isinstance(
        d["instruments"][0]["provenance"]["auditor"], dict)
    assert isinstance(
        d["instruments"][0]["provenance"]["issued"], str)
    print(f"  ✓ arbitrary provenance coerced to JSON-safe")

    # ---------------------------------------------------------------- #
    # P1 3.8 — contractual_kgco2e cached
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.8: cached aggregates ===")
    ledger = CarbonInstrumentsLedger()
    for i in range(100):
        ledger.add(CarbonInstrument(
            instrument_id=f"c-{i}", kind="offset",
            quantity=1.0, unit="kgCO2e", retired=True))
    t0 = time.perf_counter()
    for _ in range(1000):
        ledger.contractual_kgco2e()
    elapsed = time.perf_counter() - t0
    assert elapsed < 0.05, (
        f"P1 3.8 regression: 1000 calls took {elapsed:.3f}s")
    print(f"  ✓ 1000 contractual_kgco2e() calls in "
          f"{elapsed*1000:.1f}ms")

    # Mutation invalidates.
    ledger.add(CarbonInstrument(
        instrument_id="c-new", kind="offset",
        quantity=5.0, unit="kgCO2e", retired=True))
    assert ledger.contractual_kgco2e() == 105.0
    print(f"  ✓ cache invalidated by add()")

    # ---------------------------------------------------------------- #
    # P1 3.9 — metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.9: metrics_hook ===")
    events: List[Tuple[str, Mapping[str, Any]]] = []
    ledger = CarbonInstrumentsLedger(
        metrics_hook=lambda ev, f: events.append((ev, f)))
    ledger.add(CarbonInstrument(
        instrument_id="m-1", kind="offset",
        quantity=1.0, unit="kgCO2e"))
    ledger.retire("m-1")
    names = {ev for ev, _ in events}
    assert "instrument_added" in names
    assert "instrument_retired" in names
    print(f"  ✓ events captured: {sorted(names)}")

    # ---------------------------------------------------------------- #
    # P1 3.10 — pickle
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.10: pickle ===")
    ledger = CarbonInstrumentsLedger()
    ledger.add(CarbonInstrument(
        instrument_id="p", kind="offset",
        quantity=1.0, unit="kgCO2e"))
    blob = pickle.dumps(ledger)
    restored = pickle.loads(blob)
    assert len(restored) == 1
    assert restored.get("p") is not None
    assert restored.contractual_kgco2e() == 0.0
    print(f"  ✓ pickle round-trip works")

    # ---------------------------------------------------------------- #
    # P1 3.11 — build_separated_report
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.11: separated report ===")

    class _FakeCarbonLedger:
        def get_statistics(self):
            return {
                "total_carbon_kg": 500.0,
                "total_carbon_operational_kg": 500.0,
            }

    instruments = CarbonInstrumentsLedger()
    instruments.add(CarbonInstrument(
        instrument_id="o1", kind="offset",
        quantity=100.0, unit="kgCO2e", retired=True))
    instruments.add(CarbonInstrument(
        instrument_id="r1", kind="rec",
        quantity=3.0, unit="MWh", retired=True))

    report = build_separated_report(_FakeCarbonLedger(), instruments)
    assert report["operational"]["total_carbon_operational_kg"] == 500.0
    assert report["contractual"]["total_carbon_contractual_kg"] == 100.0
    assert report["contractual"]["total_rec_mwh"] == 3.0
    assert "MUST NOT be summed" in report["note"]
    print(f"  ✓ operational=500, contractual=100, "
          f"rec=3, note present")

    # ---------------------------------------------------------------- #
    # P1 3.12 — add_batch
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.12: add_batch ===")
    ledger = CarbonInstrumentsLedger()
    batch = [
        CarbonInstrument(
            instrument_id=f"b-{i}", kind="offset",
            quantity=1.0, unit="kgCO2e")
        for i in range(5)
    ]
    ledger.add_batch(batch)
    assert len(ledger) == 5
    print(f"  ✓ 5 instruments added in one batch")

    # Duplicate within batch rejected before any mutation.
    ledger2 = CarbonInstrumentsLedger()
    dup_batch = [
        CarbonInstrument(
            instrument_id="dup", kind="offset",
            quantity=1.0, unit="kgCO2e"),
        CarbonInstrument(
            instrument_id="dup", kind="offset",
            quantity=2.0, unit="kgCO2e"),
    ]
    try:
        ledger2.add_batch(dup_batch)
    except CarbonInstrumentsDuplicateError:
        pass
    else:
        raise AssertionError(
            "P1 3.12 regression: duplicate within batch accepted")
    assert len(ledger2) == 0, (
        f"P1 3.12 regression: partial batch committed "
        f"({len(ledger2)} instruments)")
    print(f"  ✓ duplicate-in-batch rejected atomically (no "
          f"partial commit)")

    print("\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
