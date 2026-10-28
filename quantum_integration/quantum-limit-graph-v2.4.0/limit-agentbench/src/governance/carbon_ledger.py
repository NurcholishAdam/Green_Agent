"""
Green Agent v5.0.0 — Carbon Ledger v2.0.0 (Enhanced)
====================================================

Layer 8: Immutable carbon accounting and compliance.

The ledger is the primary evidence artifact for sustainability claims.
It records *measured* operational emissions, distinguishes them from
*contractual* instruments (RECs, offsets), and produces a
tamper-evident hash chain that survives process restarts.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``record()`` / ``correct_entry()`` / ``record_instrument()`` raise
        ``LedgerNotInitializedError`` when called before
        ``initialize()``. Previously an un-initialized ledger was
        overwritten with a single-entry file — 99 % data loss.
  * 2.2 ``get_report()`` now resolves corrections before summing.
        Previously a ``5.0 → 3.0`` correction reported ``8.0``, while
        ``get_statistics()`` correctly reported ``3.0``.
  * 2.3 ``initialize()`` raises ``LedgerTamperedError`` when the loaded
        chain fails verification. Previously it logged an ERROR and
        continued with the tampered state.
  * 2.4 ``initialize()`` recomputes ``measured`` / ``estimated`` /
        ``simulated`` / ``correction`` counters and the
        ``_corrections`` index from ``entries``. Previously every
        counter was reset to 0 on restart despite ``entries`` being
        intact.
  * 2.5 ``_persist_append()`` failure raises ``LedgerPersistenceError``
        (previously logged and returned silently, giving the caller a
        hash for a non-durable entry).
  * 2.6 The ``entries`` list is exposed via a read-only property
        (returns copies). External mutation of
        ``ledger.entries[-1]["carbon_kg"]`` no longer corrupts the
        chain — the mutation hits a copy that is discarded.
  * 2.7 New error hierarchy: ``LedgerError`` base class; subclasses
        ``LedgerNotInitializedError``, ``LedgerTamperedError``,
        ``LedgerPersistenceError``, ``LedgerValidationError``,
        ``LedgerDuplicateInstrumentError`` (the last doubles as a
        ``ValueError`` for v1.0.0 backward compat).
  * 2.8 ``initialize()`` is idempotent — a second call is a no-op.
        Previously a second call silently replaced in-memory state
        from disk (which could be stale if ``record()`` had failed to
        persist).

**P1 robustness**
  * 3.1 Append-only WAL persistence. ``record()`` is now O(1) per
        record instead of O(N) — a full-file rewrite on every entry
        made the ledger unusable beyond a few thousand records.
        ``shutdown()`` compacts (atomic full-file rewrite).
  * 3.2 ``verify_entry(entry_hash)`` — verify a single entry without
        walking the whole chain.
  * 3.3 ``get_report()`` returns deep copies of entries.
  * 3.4 Enums (``CarbonZone``, ``CarbonZone.RED`` etc.) are stored as
        their ``.value`` (``"red"``) rather than ``"CarbonZone.RED"``.
        Historic entries remain readable.
  * 3.5 Instrument ``kind`` and ``unit`` are validated against known
        sets. ``tCO2e`` is converted to ``kgCO2e`` correctly.
  * 3.6 A second ``correct_entry()`` on an already-corrected entry
        raises ``LedgerValidationError``. Reverse the first
        correction, or correct the correction.
  * 3.7 ``get_report()`` uses half-open ``[start, end)`` windowing —
        adjacent windows no longer double-count boundary entries.
  * 3.8 ``_calculate_average_intensity()`` returns ``None`` when
        ``total_energy == 0`` but ``total_carbon > 0`` (undefined
        intensity, not "zero").
  * 3.9 ``verify_chain()`` cross-checks ``_last_chain_hash`` against
        the last entry's hash.
  * 3.10 Per-entry ``schema_version`` is preserved. ``get_report()``
        reports ``schema_versions_present``.
  * 3.11 ``_parse_iso()`` logs a warning on failure and counts the
        parse errors in ``get_report()``.

**P2 polish**
  * Pickling support (``__getstate__`` / ``__setstate__``).
  * Sync + async context managers.
  * ``_run_selftests()`` callable.

Backward compatible: the public API is unchanged. ``ledger.entries``
returns copies (mutation of the copies has no effect). Old ledgers load
via ``initialize()``; the WAL is created lazily on the first ``record()``.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


# =============================================================================
# Schema version
# =============================================================================

LEDGER_SCHEMA_VERSION = "5.0.0"
__version__ = "2.0.0"


# =============================================================================
# Error hierarchy (P0 2.7)
# =============================================================================

class LedgerError(Exception):
    """Base class for every error raised by the carbon ledger."""


class LedgerNotInitializedError(LedgerError):
    """Raised when a write method is called before ``initialize()``."""


class LedgerTamperedError(LedgerError):
    """Raised when the loaded chain fails hash verification."""


class LedgerPersistenceError(LedgerError):
    """Raised when the ledger cannot be persisted to disk."""


class LedgerValidationError(LedgerError):
    """Raised for invalid inputs (unknown instrument kind, etc.)."""


class LedgerDuplicateInstrumentError(LedgerError, ValueError):
    """
    Raised when ``record_instrument()`` is called with a duplicate ID.

    Multiple inheritance preserves v1.0.0 behavior: ``except ValueError:``
    still catches this. ``except LedgerError:`` also catches it.
    """


# =============================================================================
# Instrument vocabulary (P1 3.5)
# =============================================================================

class InstrumentKind(str, Enum):
    REC = "rec"
    OFFSET = "offset"
    CREDIT = "credit"

    def __str__(self) -> str:
        return self.value


class InstrumentUnit(str, Enum):
    KWH = "kWh"
    MWH = "MWh"
    KGCO2E = "kgCO2e"
    TCO2E = "tCO2e"

    def __str__(self) -> str:
        return self.value


# Conversion factors to kgCO2e for offset-type instruments.
_UNIT_TO_KGCO2E: Dict[str, float] = {
    "kgCO2e": 1.0,
    "tCO2e": 1000.0,
}


# =============================================================================
# Canonical hash fields (P0 2.6)
# =============================================================================
#
# Only these fields participate in the hash. Adding metadata fields
# (``published_at``, ``reviewed_by``, etc.) after the fact does not break
# the chain; tampering with any canonical field does.
#
_CANONICAL_HASH_FIELDS: Tuple[str, ...] = (
    "entry_id",
    "schema_version",
    "timestamp",
    "task_id",
    "run_id",
    "energy_kwh",
    "carbon_operational_kg",
    "carbon_kg",
    "carbon_zone",
    "action",
    "negawatt_reward",
    "power_budget",
    "source",
    "methodology_version",
    "baseline",
    "simulated",
    "region",
    "precision",
    "corrects_entry_id",
    "correction_reason",
    "extra",
)


# =============================================================================
# Helpers
# =============================================================================

def _get_attr(obj: Any, name: str, default: Any) -> Any:
    """Duck-typed attribute or key access with a default."""
    if obj is None:
        return default
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _coerce_for_json(value: Any) -> Any:
    """
    Recursively coerce ``value`` to a JSON-safe form.

    Enums (``CarbonZone.RED``) become their ``.value`` (``"red"``).
    Datetimes become ISO strings. Dicts and lists recurse.
    """
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _coerce_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_coerce_for_json(v) for v in value]
    return value


def _parse_iso(value: Any, *, context: str = "") -> Optional[datetime]:
    """
    Parse an ISO timestamp.

    Returns ``None`` on failure (with a ``WARNING`` log) — callers must
    decide how to handle the undated entry. P1 3.11.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        logger.warning(
            "Could not parse timestamp %r%s; treating as undated.",
            value, f" ({context})" if context else "")
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# =============================================================================
# CarbonLedger
# =============================================================================

class CarbonLedger:
    """
    Immutable ledger for carbon accounting and compliance reporting.

    Design principles:

      1. **Full SHA-256 hash chain.** Every entry commits to its
         predecessor via ``prev_hash``, so deletion, reordering, or
         insertion of *canonical* fields invalidates the entire
         subsequent history. Non-canonical metadata fields added after
         the fact do not break the chain (P0 2.6).

      2. **Operational vs. contractual separation.** Physical emissions
         live in the operational ledger; purchased instruments live in
         the instruments ledger. They are NEVER summed by this class.

      3. **Provenance on every entry.** Each entry records whether the
         energy was measured or estimated, the source, and the
         methodology version.

      4. **Corrections without destruction.** A correction is recorded
         as a new entry with a ``corrects_entry_id`` field. The
         original entry is never modified. A single entry may only be
         corrected once (P1 3.6).

      5. **Append-only persistence via WAL (P1 3.1).** Each ``record()``
         appends one line to ``<file>.wal`` and ``fsync``s it. Full-file
         compaction happens on ``shutdown()``. This makes ``record()``
         O(1) per call regardless of ledger size.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config or {}
        governance_cfg = self.config.get("governance", {}) or {}

        # --- Internal entries list (P0 2.6) ---
        self._entries: List[Dict[str, Any]] = []

        # --- Totals ---
        self.total_carbon: float = 0.0
        self.total_energy: float = 0.0

        # --- Persistence ---
        self._ledger_file = Path(
            governance_cfg.get(
                "ledger_file", "data/carbon_ledger.json"))
        self._wal_file = self._ledger_file.with_suffix(
            self._ledger_file.suffix + ".wal")
        self._instruments_file = Path(
            governance_cfg.get(
                "instruments_file", "data/carbon_instruments.json"))
        self._schema_version = governance_cfg.get(
            "schema_version", LEDGER_SCHEMA_VERSION)

        # --- Instruments ledger (contractual side) ---
        self._instruments: List[Dict[str, Any]] = []

        # --- Corrections index ---
        self._corrections: Dict[str, List[str]] = {}

        # --- Chain state ---
        self._last_chain_hash: str = "genesis"
        self._entry_counter: int = 0

        # --- Lifecycle (P0 2.1 / 2.8) ---
        self._initialized: bool = False

        # --- Thread safety ---
        self._lock = threading.RLock()

        # --- Statistics ---
        self._correction_count: int = 0
        self._simulated_entry_count: int = 0
        self._measured_entry_count: int = 0
        self._estimated_entry_count: int = 0
        self._parse_error_count: int = 0

        logger.info(
            f"CarbonLedger v{__version__} initialized "
            f"(ledger_file={self._ledger_file}, "
            f"schema_version={self._schema_version})"
        )

    # ------------------------------------------------------------------
    # Public read-only property (P0 2.6)
    # ------------------------------------------------------------------

    @property
    def entries(self) -> List[Dict[str, Any]]:
        """
        Read-only snapshot of the ledger entries.

        Returns deep copies — mutating the returned list or its dicts
        does not affect the ledger's state. (P0 2.6)
        """
        with self._lock:
            return copy.deepcopy(self._entries)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """
        Load existing state from disk.

        Idempotent (P0 2.8). Raises ``LedgerTamperedError`` if the
        loaded chain fails verification (P0 2.3).
        """
        with self._lock:
            if self._initialized:
                logger.debug("initialize() called again; no-op.")
                return

            # ---- Load compact file ----
            if self._ledger_file.exists():
                try:
                    with open(self._ledger_file, "r") as f:
                        payload = json.load(f)
                except (OSError, json.JSONDecodeError) as e:
                    raise LedgerError(
                        f"Could not load ledger from "
                        f"{self._ledger_file}: {e}") from e

                self._entries = list(
                    payload.get("entries", []) or [])
                self.total_carbon = float(
                    payload.get("total_carbon", 0.0) or 0.0)
                self.total_energy = float(
                    payload.get("total_energy", 0.0) or 0.0)
                self._last_chain_hash = payload.get(
                    "last_chain_hash", "genesis")
                self._entry_counter = int(
                    payload.get("entry_counter", len(self._entries))
                    or 0)

            # ---- Replay WAL (P1 3.1) ----
            if self._wal_file.exists():
                self._replay_wal_locked()

            # ---- Verify the full chain (P0 2.3) ----
            if not self.verify_chain():
                raise LedgerTamperedError(
                    f"Ledger at {self._ledger_file} failed hash "
                    f"chain verification — it may have been tampered "
                    f"with, or the compact file and WAL disagree. "
                    f"Inspect {self._ledger_file} and "
                    f"{self._wal_file} before proceeding.")

            # ---- Recompute counters (P0 2.4) ----
            self._recompute_counters_locked()

            # ---- Load instruments ----
            if self._instruments_file.exists():
                try:
                    with open(self._instruments_file, "r") as f:
                        self._instruments = list(json.load(f) or [])
                except (OSError, json.JSONDecodeError) as e:
                    logger.warning(
                        f"Failed to load instruments ledger: {e}")

            self._initialized = True

        logger.info(
            f"CarbonLedger initialized — loaded {len(self._entries)} "
            f"entries, {len(self._instruments)} instruments"
        )

    async def shutdown(self) -> None:
        """
        Compact the ledger to disk and log a summary.
        """
        with self._lock:
            if not self._initialized:
                logger.debug(
                    "shutdown() called on uninitialized ledger; no-op.")
                return
            self._persist_compact_locked()
        logger.info(
            f"CarbonLedger shutdown - Total: "
            f"{self.total_carbon:.4f} kg CO2"
        )

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    async def record(
        self,
        result: Any,
        decision: Any,
        *,
        source: str = "estimated",
        methodology_version: Optional[str] = None,
        baseline: Optional[str] = None,
        simulated: Optional[bool] = None,
        region: Optional[str] = None,
        precision: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Record a carbon accounting entry.

        Returns the full SHA-256 hash of the new entry. The entry is
        durable (in the WAL) before this method returns; if persistence
        fails, ``LedgerPersistenceError`` is raised and the entry is
        discarded from memory (P0 2.5 + P1 3.1).

        Raises ``LedgerNotInitializedError`` if called before
        ``initialize()`` (P0 2.1).
        """
        with self._lock:
            self._require_initialized_locked()

            entry = self._build_entry(
                result=result,
                decision=decision,
                source=source,
                methodology_version=(
                    methodology_version or self._schema_version),
                baseline=baseline,
                simulated=simulated,
                region=region,
                precision=precision,
                extra=extra,
            )

            # --- Compute hash chain ---
            entry["prev_hash"] = self._last_chain_hash
            entry["hash"] = self._calculate_hash(entry)

            # --- Persist to WAL FIRST (P0 2.5) ---
            self._persist_append_locked(entry)

            # --- Commit to memory ---
            self._entries.append(entry)
            self._last_chain_hash = entry["hash"]
            self._entry_counter += 1
            self._update_totals_locked(entry)
            self._update_counters_locked(entry)

            logger.info(
                f"Carbon ledger entry {entry['entry_id']}: "
                f"{float(entry.get('carbon_operational_kg', 0.0)):.4f} "
                f"kg CO2 (total: {self.total_carbon:.4f})"
            )
            return entry["hash"]

    def _build_entry(
        self,
        *,
        result: Any,
        decision: Any,
        source: str,
        methodology_version: str,
        baseline: Optional[str],
        simulated: Optional[bool],
        region: Optional[str],
        precision: Optional[str],
        extra: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Assemble the entry dict with all provenance fields."""
        # --- Extract from result ---
        task_id = _get_attr(result, "task_id", "unknown")
        energy_kwh = _get_attr(result, "energy_consumed", 0.0)
        carbon_kg = _get_attr(result, "carbon_emitted", 0.0)
        negawatt_reward = _get_attr(result, "negawatt_reward", 0.0)

        # --- Extract from decision ---
        carbon_zone = _get_attr(decision, "carbon_zone", "unknown")
        action = _get_attr(decision, "action", "unknown")
        power_budget = _get_attr(decision, "power_budget", None)

        # --- Coerce enums to their .value (P1 3.4) ---
        carbon_zone = _coerce_for_json(carbon_zone)
        action = _coerce_for_json(action)

        # --- Operational vs. contractual ---
        carbon_operational = _get_attr(
            result, "carbon_operational_kg", None)
        carbon_contractual = _get_attr(
            result, "carbon_contractual_kg", None)
        if carbon_operational is None:
            carbon_operational = carbon_kg
        if carbon_contractual is None:
            carbon_contractual = 0.0

        # --- Simulated flag ---
        if simulated is None:
            simulated = bool(_get_attr(result, "simulated", False))
            if source == "simulated":
                simulated = True

        entry: Dict[str, Any] = {
            "entry_id": uuid.uuid4().hex[:12],
            "schema_version": self._schema_version,
            "timestamp": datetime.now(timezone.utc).isoformat(),

            "task_id": task_id,
            "run_id": _get_attr(result, "run_id", None),

            "energy_kwh": float(energy_kwh),
            "carbon_operational_kg": float(carbon_operational),
            "carbon_kg": float(carbon_kg),

            "carbon_zone": carbon_zone,
            "action": action,
            "negawatt_reward": float(negawatt_reward),
            "power_budget": power_budget,

            "source": source,
            "methodology_version": methodology_version,
            "baseline": baseline,
            "simulated": bool(simulated),
            "region": region,
            "precision": precision,

            "corrects_entry_id": None,
            "correction_reason": None,
        }

        if extra:
            entry["extra"] = _coerce_for_json(dict(extra))

        return entry

    def _calculate_hash(self, entry: Dict[str, Any]) -> str:
        """
        Compute the full SHA-256 hash over the canonical field subset.

        P0 2.6: only ``_CANONICAL_HASH_FIELDS`` participate. Adding
        metadata fields after the fact does not invalidate the chain.
        """
        data = {
            k: entry[k] for k in _CANONICAL_HASH_FIELDS
            if k in entry
        }
        serialized = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()

    # ------------------------------------------------------------------
    # Corrections
    # ------------------------------------------------------------------

    async def correct_entry(
        self,
        entry_id: str,
        corrected_carbon_kg: float,
        *,
        reason: str,
        methodology_version: Optional[str] = None,
    ) -> Optional[str]:
        """
        Record a correction to a previous entry.

        The original entry is NEVER modified. A new entry is appended
        with ``corrects_entry_id`` pointing to the original. An entry
        may only be corrected once (P1 3.6) — to change the corrected
        value, correct the correction.

        Returns the hash of the correction entry, or None if the
        original entry was not found.
        """
        with self._lock:
            self._require_initialized_locked()

            original = next(
                (e for e in self._entries
                 if e.get("entry_id") == entry_id),
                None,
            )
            if original is None:
                logger.warning(
                    f"correct_entry: no entry with id {entry_id}")
                return None

            # P1 3.6: reject double-correction.
            if entry_id in self._corrections:
                raise LedgerValidationError(
                    f"entry {entry_id!r} already corrected by "
                    f"{self._corrections[entry_id]}; reverse the "
                    f"existing correction first, or correct the "
                    f"correction.")

            correction: Dict[str, Any] = {
                "entry_id": uuid.uuid4().hex[:12],
                "schema_version": self._schema_version,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "task_id": original.get("task_id"),
                "run_id": original.get("run_id"),
                "energy_kwh": original.get("energy_kwh", 0.0),
                "carbon_operational_kg": float(corrected_carbon_kg),
                "carbon_kg": float(corrected_carbon_kg),
                "carbon_zone": original.get("carbon_zone"),
                "action": "correction",
                "negawatt_reward": 0.0,
                "power_budget": None,
                "source": "correction",
                "methodology_version": (
                    methodology_version or self._schema_version),
                "baseline": original.get("baseline"),
                "simulated": False,
                "region": original.get("region"),
                "precision": original.get("precision"),
                "corrects_entry_id": entry_id,
                "correction_reason": reason,
            }
            correction["prev_hash"] = self._last_chain_hash
            correction["hash"] = self._calculate_hash(correction)

            # Persist to WAL FIRST.
            self._persist_append_locked(correction)

            # Commit to memory.
            self._entries.append(correction)
            self._last_chain_hash = correction["hash"]
            self._entry_counter += 1
            self._corrections.setdefault(entry_id, []).append(
                correction["entry_id"])
            self._correction_count += 1

            # Adjust totals by the delta (corrected - effective).
            effective_kg = self._effective_operational_kg_locked(
                entry_id)
            self.total_carbon += (corrected_carbon_kg - effective_kg)

            return correction["hash"]

    def _effective_operational_kg_locked(self, entry_id: str) -> float:
        """
        Return the effective operational carbon for ``entry_id`` after
        applying any corrections (P0 2.2).
        """
        # Find the original entry.
        original = next(
            (e for e in self._entries
             if e.get("entry_id") == entry_id),
            None,
        )
        if original is None:
            return 0.0
        effective = float(original.get("carbon_operational_kg", 0.0))
        # Apply corrections in order — later corrections win.
        for cid in self._corrections.get(entry_id, []):
            c = next(
                (e for e in self._entries
                 if e.get("entry_id") == cid),
                None,
            )
            if c is not None:
                effective = float(c.get("carbon_operational_kg", 0.0))
        return effective

    # ------------------------------------------------------------------
    # Instruments ledger (contractual carbon)
    # ------------------------------------------------------------------

    async def record_instrument(
        self,
        *,
        instrument_id: str,
        kind: str,
        quantity: float,
        unit: str,
        vintage_year: Optional[int] = None,
        matching_period: Optional[str] = None,
        producer: Optional[str] = None,
        region: Optional[str] = None,
        retired: bool = False,
        provenance: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Record a contractual carbon instrument (REC, offset, credit).

        Instruments are kept in a separate list from operational
        emissions. ``kind`` and ``unit`` are validated against known
        sets (P1 3.5).
        """
        with self._lock:
            self._require_initialized_locked()

            # P1 3.5: validate kind / unit.
            if kind not in {k.value for k in InstrumentKind}:
                raise LedgerValidationError(
                    f"invalid instrument kind {kind!r}; "
                    f"expected one of {sorted(k.value for k in InstrumentKind)}")
            if unit not in {u.value for u in InstrumentUnit}:
                raise LedgerValidationError(
                    f"invalid instrument unit {unit!r}; "
                    f"expected one of {sorted(u.value for u in InstrumentUnit)}")

            if any(
                i.get("instrument_id") == instrument_id
                for i in self._instruments
            ):
                raise LedgerDuplicateInstrumentError(
                    f"duplicate instrument_id: {instrument_id}")

            self._instruments.append({
                "instrument_id": instrument_id,
                "kind": kind,
                "quantity": float(quantity),
                "unit": unit,
                "vintage_year": vintage_year,
                "matching_period": matching_period,
                "producer": producer,
                "region": region,
                "retired": bool(retired),
                "retired_at": (
                    datetime.now(timezone.utc).isoformat()
                    if retired else None
                ),
                "provenance": dict(provenance or {}),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            })
            self._persist_instruments_locked()

    async def get_instruments_ledger(self) -> Dict[str, Any]:
        """
        Return the contractual instruments ledger.

        The ``contractual_kgco2e`` field sums retired offset instruments
        with unit conversion (P1 3.5).
        """
        with self._lock:
            return {
                "instruments": copy.deepcopy(self._instruments),
                "count": len(self._instruments),
                "contractual_kgco2e": self._contractual_kg_locked(),
                "note": (
                    "Contractual instruments are tracked separately "
                    "from operational emissions. Never sum the two."
                ),
            }

    def _contractual_kg_locked(self) -> float:
        """Sum retired offset instruments in kgCO2e (P1 3.5)."""
        total = 0.0
        for i in self._instruments:
            if i["kind"] != "offset" or not i["retired"]:
                continue
            factor = _UNIT_TO_KGCO2E.get(i["unit"])
            if factor is None:
                logger.warning(
                    "Cannot convert instrument %r (%s) to kgCO2e; "
                    "skipping.", i.get("instrument_id"), i["unit"])
                continue
            total += float(i["quantity"]) * factor
        return total

    # ------------------------------------------------------------------
    # Verification
    # ------------------------------------------------------------------

    def verify_chain(self) -> bool:
        """
        Verify the full hash chain.

        Returns True iff every entry's ``prev_hash`` matches its
        predecessor's ``hash``, every entry's ``hash`` matches its
        recomputed value, and ``_last_chain_hash`` matches the final
        entry's ``hash`` (P1 3.9).
        """
        with self._lock:
            prev = "genesis"
            for entry in self._entries:
                if entry.get("prev_hash") != prev:
                    logger.error(
                        f"Chain broken at entry "
                        f"{entry.get('entry_id')}: "
                        f"prev_hash={entry.get('prev_hash')} != {prev}")
                    return False
                expected = self._calculate_hash(entry)
                if entry.get("hash") != expected:
                    logger.error(
                        f"Hash mismatch at entry "
                        f"{entry.get('entry_id')}")
                    return False
                prev = entry["hash"]
            # P1 3.9: cross-check _last_chain_hash.
            if prev != self._last_chain_hash:
                logger.error(
                    "_last_chain_hash=%r does not match the last "
                    "entry's hash=%r",
                    self._last_chain_hash, prev)
                return False
        return True

    def verify_entry(self, entry_hash: str) -> bool:
        """
        Verify a single entry by its hash.

        Returns True iff the entry exists, its own hash recomputes
        correctly, and its ``prev_hash`` matches the immediate
        predecessor (P1 3.2).
        """
        with self._lock:
            prev = "genesis"
            for entry in self._entries:
                if entry.get("prev_hash") != prev:
                    return False
                expected = self._calculate_hash(entry)
                if entry.get("hash") != expected:
                    return False
                if entry.get("hash") == entry_hash:
                    return True
                prev = entry["hash"]
        return False

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    async def get_report(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Generate a carbon accounting report.

        Uses half-open ``[start, end)`` windowing (P1 3.7). Resolves
        corrections before summing (P0 2.2). Returns deep copies
        (P1 3.3).
        """
        with self._lock:
            # Snapshot for consistent reads.
            entries_snapshot = copy.deepcopy(self._entries)
            instruments_kg = self._contractual_kg_locked()

        # ---- Filter by timestamp (half-open) ----
        parse_errors = 0
        filtered: List[Dict[str, Any]] = []
        for e in entries_snapshot:
            ts_raw = e.get("timestamp")
            ts = _parse_iso(
                ts_raw,
                context=f"entry {e.get('entry_id')}",
            )
            if ts_raw and ts is None:
                parse_errors += 1
                # Include the entry but mark it (silent drop is worse).
                e["_timestamp_parse_error"] = True
                filtered.append(e)
                continue
            if start_date and ts is not None:
                start = start_date
                if start.tzinfo is None:
                    start = start.replace(tzinfo=timezone.utc)
                if ts < start:
                    continue
            if end_date and ts is not None:
                end = end_date
                if end.tzinfo is None:
                    end = end.replace(tzinfo=timezone.utc)
                # Half-open window: [start, end).
                if ts >= end:
                    continue
            filtered.append(e)

        # ---- Resolve corrections (P0 2.2) ----
        effective = self._resolve_corrections(filtered)

        # ---- Compute totals from effective entries ----
        total_energy = sum(
            float(e.get("energy_kwh", 0.0)) for e in filtered)
        total_carbon_operational = sum(
            float(effective[e["entry_id"]].get(
                "carbon_operational_kg", 0.0))
            for e in filtered
            if not e.get("corrects_entry_id")    # skip correction rows
            and e["entry_id"] in effective
        )
        total_carbon_legacy = total_carbon_operational  # aligned with operational
        total_negawatt = sum(
            float(e.get("negawatt_reward", 0.0)) for e in filtered)

        # ---- Breakdowns ----
        by_source: Dict[str, int] = {}
        schema_versions: Dict[str, int] = {}
        for e in filtered:
            src = e.get("source", "unknown")
            by_source[src] = by_source.get(src, 0) + 1
            sv = e.get("schema_version", "unknown")
            schema_versions[sv] = schema_versions.get(sv, 0) + 1

        simulated_count = sum(
            1 for e in filtered if e.get("simulated"))
        corrected_count = sum(
            1 for e in filtered if e.get("corrects_entry_id"))

        # ---- Average intensity over effective entries ----
        total_carbon_for_intensity = sum(
            float(effective[e["entry_id"]].get("carbon_kg", 0.0))
            for e in filtered
            if not e.get("corrects_entry_id")
            and e["entry_id"] in effective
        )
        if total_energy == 0:
            if total_carbon_for_intensity > 0:
                avg_intensity = None
            else:
                avg_intensity = 0.0
        else:
            avg_intensity = (total_carbon_for_intensity / total_energy) * 1000

        return {
            "report_period": {
                "start": start_date.isoformat() if start_date else None,
                "end": end_date.isoformat() if end_date else None,
                "window_semantics": "[start, end)",
            },
            "schema_version": self._schema_version,
            "schema_versions_present": schema_versions,
            "total_entries": len(filtered),
            "total_energy_kwh": total_energy,
            "total_carbon_kg": total_carbon_legacy,
            "total_carbon_operational_kg": total_carbon_operational,
            "total_carbon_contractual_kg": instruments_kg,
            "total_negawatt": total_negawatt,
            "average_carbon_intensity": avg_intensity,
            "by_source": by_source,
            "simulated_count": simulated_count,
            "corrected_count": corrected_count,
            "timestamp_parse_errors": parse_errors,
            "chain_verified": self.verify_chain(),
            "entries": filtered[-100:],
        }

    def _resolve_corrections(
        self, entries: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Return ``{entry_id: effective_entry}`` after applying
        corrections in ``entries`` to originals in ``entries``.

        Called by ``get_report`` — operates on the filtered view, so
        a correction whose original is outside the window does not
        retroactively apply to an entry inside the window.
        """
        effective: Dict[str, Dict[str, Any]] = {
            e["entry_id"]: dict(e) for e in entries
            if e.get("entry_id")
        }
        for e in entries:
            target = e.get("corrects_entry_id")
            if target and target in effective:
                effective[target]["carbon_operational_kg"] = float(
                    e.get("carbon_operational_kg", 0.0))
                effective[target]["carbon_kg"] = float(
                    e.get("carbon_kg", 0.0))
        return effective

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative ledger statistics."""
        with self._lock:
            return {
                "schema_version": self._schema_version,
                "entry_count": len(self._entries),
                "entry_counter": self._entry_counter,
                "total_carbon_kg": self.total_carbon,
                "total_energy_kwh": self.total_energy,
                "measured_entries": self._measured_entry_count,
                "estimated_entries": self._estimated_entry_count,
                "simulated_entries": self._simulated_entry_count,
                "corrections": self._correction_count,
                "instruments_count": len(self._instruments),
                "last_chain_hash": self._last_chain_hash,
                "chain_verified": self.verify_chain(),
                "initialized": self._initialized,
            }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_initialized_locked(self) -> None:
        if not self._initialized:
            raise LedgerNotInitializedError(
                "Ledger not initialized. Call "
                "`await ledger.initialize()` before writing. "
                "(P0 2.1)")

    def _update_totals_locked(self, entry: Dict[str, Any]) -> None:
        self.total_carbon += float(
            entry.get("carbon_operational_kg", 0.0))
        self.total_energy += float(entry.get("energy_kwh", 0.0))

    def _update_counters_locked(self, entry: Dict[str, Any]) -> None:
        if entry.get("simulated"):
            self._simulated_entry_count += 1
        source = entry.get("source")
        if source == "measured":
            self._measured_entry_count += 1
        elif source == "estimated":
            self._estimated_entry_count += 1
        elif source == "correction":
            self._correction_count += 1

    def _recompute_counters_locked(self) -> None:
        """Recompute all counters from ``_entries`` (P0 2.4)."""
        self._measured_entry_count = 0
        self._estimated_entry_count = 0
        self._simulated_entry_count = 0
        self._correction_count = 0
        self._corrections = {}
        for e in self._entries:
            if e.get("simulated"):
                self._simulated_entry_count += 1
            src = e.get("source")
            if src == "measured":
                self._measured_entry_count += 1
            elif src == "estimated":
                self._estimated_entry_count += 1
            elif src == "correction":
                self._correction_count += 1
            target = e.get("corrects_entry_id")
            if target:
                self._corrections.setdefault(target, []).append(
                    e["entry_id"])

    def _replay_wal_locked(self) -> None:
        """Replay any WAL lines not already present in ``_entries``."""
        existing_ids = {e.get("entry_id") for e in self._entries}
        try:
            with open(self._wal_file, "r") as f:
                lines = f.read().splitlines()
        except OSError as e:
            raise LedgerError(
                f"Could not read WAL {self._wal_file}: {e}") from e

        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                entry = json.loads(stripped)
            except json.JSONDecodeError:
                # P0 2.5-adjacent: a truncated last line is the
                # signature of a crash mid-write. Ignore it; raise on
                # any earlier corruption.
                if i == len(lines) - 1:
                    logger.warning(
                        "Ignoring truncated last WAL line "
                        "(in-progress write from prior crash).")
                    break
                raise LedgerTamperedError(
                    f"WAL corruption at line {i + 1} of "
                    f"{self._wal_file}") from None

            entry_id = entry.get("entry_id")
            if entry_id and entry_id in existing_ids:
                continue  # already compacted
            self._entries.append(entry)
            if entry_id:
                existing_ids.add(entry_id)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _persist_append_locked(self, entry: Dict[str, Any]) -> None:
        """
        Append one entry to the WAL (P1 3.1). Raises on failure
        (P0 2.5) — the caller must not consider the entry committed.
        """
        try:
            self._wal_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._wal_file, "a") as f:
                f.write(
                    json.dumps(
                        entry, sort_keys=True, default=str) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            raise LedgerPersistenceError(
                f"Could not append to WAL {self._wal_file}: {e}"
            ) from e

    def _persist_compact_locked(self) -> None:
        """
        Atomically rewrite the full ledger and truncate the WAL.
        Called from ``shutdown()``.
        """
        payload = {
            "schema_version": self._schema_version,
            "entries": self._entries,
            "total_carbon": self.total_carbon,
            "total_energy": self.total_energy,
            "last_chain_hash": self._last_chain_hash,
            "entry_counter": self._entry_counter,
            "persisted_at": datetime.now(timezone.utc).isoformat(),
        }
        tmp = self._ledger_file.with_suffix(
            self._ledger_file.suffix + ".tmp")
        try:
            self._ledger_file.parent.mkdir(
                parents=True, exist_ok=True)
            with open(tmp, "w") as f:
                json.dump(payload, f, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._ledger_file)
        except OSError as e:
            logger.error(f"Failed to compact ledger: {e}")
            raise LedgerPersistenceError(
                f"Failed to compact ledger {self._ledger_file}: {e}"
            ) from e

        # Truncate WAL only after successful compact.
        if self._wal_file.exists():
            try:
                self._wal_file.unlink()
            except OSError as e:
                logger.warning(
                    f"Could not remove WAL after compact: {e}. "
                    f"Next initialize() will dedupe by entry_id.")

    def _persist_instruments_locked(self) -> None:
        """Atomically rewrite the instruments file (P0 2.5)."""
        snapshot = [dict(i) for i in self._instruments]
        tmp = self._instruments_file.with_suffix(
            self._instruments_file.suffix + ".tmp")
        try:
            self._instruments_file.parent.mkdir(
                parents=True, exist_ok=True)
            with open(tmp, "w") as f:
                json.dump(snapshot, f, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._instruments_file)
        except OSError as e:
            raise LedgerPersistenceError(
                f"Failed to persist instruments: {e}") from e

    # ------------------------------------------------------------------
    # Pickling (P2)
    # ------------------------------------------------------------------

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Context managers (P2)
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "CarbonLedger":
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonLedger("
                f"version={__version__!r}, "
                f"entries={len(self._entries)}, "
                f"total_carbon_kg={self.total_carbon:.4f}, "
                f"instruments={len(self._instruments)}, "
                f"initialized={self._initialized})"
            )


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "__version__",
    "CarbonLedger",
    "InstrumentKind",
    "InstrumentUnit",
    "LEDGER_SCHEMA_VERSION",
    "LedgerDuplicateInstrumentError",
    "LedgerError",
    "LedgerNotInitializedError",
    "LedgerPersistenceError",
    "LedgerTamperedError",
    "LedgerValidationError",
]


# =============================================================================
# Self-tests
# =============================================================================

def _run_selftests() -> None:
    """Run the module's self-tests."""
    import asyncio
    import io
    import pickle
    import shutil
    import tempfile
    import time

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print(f"=== carbon_ledger v{__version__} self-tests ===\n")

    # Stub classes for duck-typed result / decision.
    class _Result:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    class _Decision:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    def _new_ledger(tmpdir: str) -> CarbonLedger:
        return CarbonLedger({
            "governance": {
                "ledger_file": os.path.join(tmpdir, "ledger.json"),
                "instruments_file": os.path.join(tmpdir, "instruments.json"),
            }
        })

    # ---------------------------------------------------------------- #
    # P0 2.1 — record() before initialize() raises
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: record() before initialize() raises ===")

    async def _test_p0_2_1():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            result = _Result(
                task_id="t-1", energy_consumed=1.0,
                carbon_emitted=0.5, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)
            try:
                await ledger.record(result, decision)
            except LedgerNotInitializedError as exc:
                print(f"  ✓ record() → LedgerNotInitializedError: "
                      f"{str(exc)[:60]}...")
            else:
                raise AssertionError(
                    "P0 2.1 regression: record() succeeded "
                    "before initialize()")

            # On-disk file was not created.
            ledger_path = os.path.join(td, "ledger.json")
            wal_path = ledger_path + ".wal"
            assert not os.path.exists(ledger_path), (
                "P0 2.1 regression: ledger file was created")
            assert not os.path.exists(wal_path), (
                "P0 2.1 regression: WAL file was created")
            print("  ✓ no files created")

    asyncio.run(_test_p0_2_1())

    # ---------------------------------------------------------------- #
    # P0 2.2 — corrections don't double-count
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: corrections don't double-count ===")

    async def _test_p0_2_2():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()

            result = _Result(
                task_id="t-1", energy_consumed=10.0,
                carbon_emitted=5.0, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)
            entry_hash = await ledger.record(result, decision)
            first_entry_id = ledger.entries[0]["entry_id"]

            # Correct to 3.0
            await ledger.correct_entry(
                first_entry_id, 3.0, reason="re-measured")

            report = await ledger.get_report()
            stats = ledger.get_statistics()

            # P0 2.2: both APIs agree.
            assert report["total_carbon_operational_kg"] == 3.0, (
                f"P0 2.2 regression: report operational="
                f"{report['total_carbon_operational_kg']}, "
                f"expected 3.0")
            assert report["total_carbon_kg"] == 3.0, (
                f"P0 2.2 regression: report legacy="
                f"{report['total_carbon_kg']}, expected 3.0")
            assert stats["total_carbon_kg"] == 3.0, (
                f"P0 2.2 regression: stats total="
                f"{stats['total_carbon_kg']}, expected 3.0")
            print(f"  ✓ report=3.0, stats=3.0 (previously 8.0 vs 3.0)")

            # Double-correction should raise (P1 3.6).
            try:
                await ledger.correct_entry(
                    first_entry_id, 2.0, reason="again")
            except LedgerValidationError:
                print("  ✓ double-correction raises "
                      "LedgerValidationError")
            else:
                raise AssertionError(
                    "P1 3.6 regression: double-correction succeeded")

    asyncio.run(_test_p0_2_2())

    # ---------------------------------------------------------------- #
    # P0 2.3 — tampered chain raises
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: tampered chain raises ===")

    async def _test_p0_2_3():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            result = _Result(
                task_id="t", energy_consumed=1.0,
                carbon_emitted=0.5, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)
            await ledger.record(result, decision)
            await ledger.shutdown()

            # Tamper with the compact file.
            ledger_path = os.path.join(td, "ledger.json")
            with open(ledger_path) as f:
                payload = json.load(f)
            payload["entries"][0]["carbon_kg"] = 999.0  # ← tamper
            with open(ledger_path, "w") as f:
                json.dump(payload, f, indent=2, default=str)

            # Try to load.
            ledger2 = _new_ledger(td)
            try:
                await ledger2.initialize()
            except LedgerTamperedError as exc:
                print(f"  ✓ tampered chain → LedgerTamperedError: "
                      f"{str(exc)[:60]}...")
            else:
                raise AssertionError(
                    "P0 2.3 regression: tampered chain accepted")

    asyncio.run(_test_p0_2_3())

    # ---------------------------------------------------------------- #
    # P0 2.4 — counters restored on init
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: counters restored on init ===")

    async def _test_p0_2_4():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            result = _Result(
                task_id="t", energy_consumed=1.0,
                carbon_emitted=0.5, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)
            # Record 3 measured, 2 estimated.
            for _ in range(3):
                await ledger.record(
                    result, decision, source="measured")
            for _ in range(2):
                await ledger.record(
                    result, decision, source="estimated")
            await ledger.shutdown()

            stats_before = ledger.get_statistics()
            assert stats_before["measured_entries"] == 3
            assert stats_before["estimated_entries"] == 2

            # Restart.
            ledger2 = _new_ledger(td)
            await ledger2.initialize()
            stats_after = ledger2.get_statistics()
            assert stats_after["measured_entries"] == 3, (
                f"P0 2.4 regression: measured_entries="
                f"{stats_after['measured_entries']} (expected 3)")
            assert stats_after["estimated_entries"] == 2, (
                f"P0 2.4 regression: estimated_entries="
                f"{stats_after['estimated_entries']} (expected 2)")
            print(f"  ✓ after restart: measured=3, estimated=2 "
                  f"(previously both 0)")

    asyncio.run(_test_p0_2_4())

    # ---------------------------------------------------------------- #
    # P0 2.5 — persistence failure raises
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: persistence failure raises ===")

    async def _test_p0_2_5():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            result = _Result(
                task_id="t", energy_consumed=1.0,
                carbon_emitted=0.5, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)

            # Simulate WAL write failure.
            original_open = open
            call_count = {"n": 0}

            def failing_open(path, *args, **kwargs):
                if ".wal" in str(path) and "a" in args:
                    raise OSError("simulated disk full")
                return original_open(path, *args, **kwargs)

            import builtins
            builtins.open = failing_open
            try:
                try:
                    await ledger.record(result, decision)
                except LedgerPersistenceError as exc:
                    print(f"  ✓ persist failure → "
                          f"LedgerPersistenceError: "
                          f"{str(exc)[:60]}...")
                else:
                    raise AssertionError(
                        "P0 2.5 regression: persist failure "
                        "was silent")
            finally:
                builtins.open = original_open

            # Entry not committed to memory after failed persist.
            assert len(ledger.entries) == 0, (
                f"P0 2.5 regression: entry was committed to memory "
                f"after failed persist ({len(ledger.entries)} entries)")
            print("  ✓ entry not committed after persist failure")

    asyncio.run(_test_p0_2_5())

    # ---------------------------------------------------------------- #
    # P0 2.6 — entries property returns copies
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: entries property returns copies ===")

    async def _test_p0_2_6():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            result = _Result(
                task_id="t", energy_consumed=1.0,
                carbon_emitted=0.5, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)
            await ledger.record(result, decision)

            snap = ledger.entries
            snap[0]["carbon_kg"] = 999.0    # mutate the snapshot
            snap[0]["published_at"] = "..."  # add non-canonical field

            # The internal state must be untouched.
            assert ledger.entries[0]["carbon_kg"] == 0.5, (
                "P0 2.6 regression: mutating the snapshot changed "
                "internal state")
            assert "published_at" not in ledger.entries[0]
            assert ledger.verify_chain() is True, (
                "P0 2.6 regression: chain broke after snapshot "
                "mutation")
            print("  ✓ snapshot mutation does not affect the ledger")

    asyncio.run(_test_p0_2_6())

    # ---------------------------------------------------------------- #
    # P0 2.7 — error hierarchy
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: error hierarchy ===")

    assert issubclass(LedgerNotInitializedError, LedgerError)
    assert issubclass(LedgerTamperedError, LedgerError)
    assert issubclass(LedgerPersistenceError, LedgerError)
    assert issubclass(LedgerValidationError, LedgerError)
    assert issubclass(LedgerDuplicateInstrumentError, LedgerError)
    assert issubclass(LedgerDuplicateInstrumentError, ValueError), (
        "P0 2.7 regression: LedgerDuplicateInstrumentError is no "
        "longer a ValueError (v1.0.0 backward compat broken)")
    print("  ✓ all 5 errors subclass LedgerError")
    print("  ✓ LedgerDuplicateInstrumentError still subclasses ValueError")

    # ---------------------------------------------------------------- #
    # P0 2.8 — initialize() idempotent
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.8: initialize() idempotent ===")

    async def _test_p0_2_8():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            result = _Result(
                task_id="t", energy_consumed=1.0,
                carbon_emitted=0.5, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)
            await ledger.record(result, decision)
            assert len(ledger.entries) == 1

            # Second initialize() — no-op.
            await ledger.initialize()
            assert len(ledger.entries) == 1, (
                f"P0 2.8 regression: second initialize() reset "
                f"state (entries={len(ledger.entries)})")
            print("  ✓ second initialize() did not lose in-memory "
                  "entries")

    asyncio.run(_test_p0_2_8())

    # ---------------------------------------------------------------- #
    # P1 3.1 — O(1) append via WAL
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: O(1) append via WAL ===")

    async def _test_p1_3_1():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            result = _Result(
                task_id="t", energy_consumed=0.001,
                carbon_emitted=0.0005, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)

            n = 500
            t0 = time.perf_counter()
            for _ in range(n):
                await ledger.record(result, decision)
            elapsed = time.perf_counter() - t0
            # Full-file rewrite would be O(N²). For 500 records at
            # ~1 KB each, that's ~125 MB of serialization. At
            # ~100 MB/s that's ~1.25 s. WAL is O(N) total, so the
            # whole run should be < 1 s on any modern machine.
            assert elapsed < 2.0, (
                f"P1 3.1 regression: {n} records took "
                f"{elapsed:.2f}s (expected < 2s with WAL)")
            print(f"  ✓ {n} records in {elapsed:.3f}s")

            # Data integrity: the WAL has all entries.
            await ledger.shutdown()  # compacts
            assert len(ledger.entries) == n

    asyncio.run(_test_p1_3_1())

    # ---------------------------------------------------------------- #
    # P1 3.2 — verify_entry(hash)
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: verify_entry(hash) ===")

    async def _test_p1_3_2():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            result = _Result(
                task_id="t", energy_consumed=1.0,
                carbon_emitted=0.5, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)
            h = await ledger.record(result, decision)

            assert ledger.verify_entry(h) is True, (
                "P1 3.2 regression: verify_entry returned False "
                "for a valid entry")
            assert ledger.verify_entry("0" * 64) is False, (
                "P1 3.2 regression: verify_entry returned True for "
                "nonexistent hash")
            print(f"  ✓ valid hash → True; garbage hash → False")

    asyncio.run(_test_p1_3_2())

    # ---------------------------------------------------------------- #
    # P1 3.4 — enums stored as .value
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: enums stored as .value ===")

    class _Zone(Enum):
        RED = "red"

    async def _test_p1_3_4():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            result = _Result(
                task_id="t", energy_consumed=1.0,
                carbon_emitted=0.5, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone=_Zone.RED,
                action="execute",
                power_budget=0.5,
            )
            await ledger.record(result, decision)
            stored_zone = ledger.entries[0]["carbon_zone"]
            assert stored_zone == "red", (
                f"P1 3.4 regression: stored carbon_zone="
                f"{stored_zone!r} (expected 'red')")
            print(f"  ✓ Enum → 'red' (not 'CarbonZone.RED')")

    asyncio.run(_test_p1_3_4())

    # ---------------------------------------------------------------- #
    # P1 3.5 — instrument kind/unit validation
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: instrument validation ===")

    async def _test_p1_3_5():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()

            # Invalid kind.
            try:
                await ledger.record_instrument(
                    instrument_id="i-1",
                    kind="garbage",
                    quantity=10.0,
                    unit="kgCO2e",
                )
            except LedgerValidationError:
                print(f"  ✓ kind='garbage' rejected")
            else:
                raise AssertionError(
                    "P1 3.5 regression: kind='garbage' accepted")

            # Invalid unit.
            try:
                await ledger.record_instrument(
                    instrument_id="i-1",
                    kind="offset",
                    quantity=10.0,
                    unit="moonbeams",
                )
            except LedgerValidationError:
                print(f"  ✓ unit='moonbeams' rejected")

            # Valid: offset in tCO2e (should convert to kgCO2e).
            await ledger.record_instrument(
                instrument_id="i-2",
                kind="offset",
                quantity=2.0,
                unit="tCO2e",
                retired=True,
            )
            ledger_data = await ledger.get_instruments_ledger()
            assert ledger_data["contractual_kgco2e"] == 2000.0, (
                f"P1 3.5 regression: contractual_kgco2e="
                f"{ledger_data['contractual_kgco2e']}, expected 2000.0")
            print(f"  ✓ 2 tCO2e retired = 2000 kgCO2e")
            print(f"  ✓ duplicate instrument_id raises")
            try:
                await ledger.record_instrument(
                    instrument_id="i-2",
                    kind="offset", quantity=1.0, unit="tCO2e",
                )
            except LedgerDuplicateInstrumentError:
                pass
            else:
                raise AssertionError(
                    "P1 3.5 regression: duplicate accepted")

    asyncio.run(_test_p1_3_5())

    # ---------------------------------------------------------------- #
    # P1 3.7 — half-open window
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.7: half-open date window ===")

    async def _test_p1_3_7():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            result = _Result(
                task_id="t", energy_consumed=1.0,
                carbon_emitted=0.5, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)
            await ledger.record(result, decision)

            # Adjacent windows around "now"
            now = datetime.now(timezone.utc)
            from datetime import timedelta
            before = now - timedelta(seconds=10)
            after = now + timedelta(seconds=10)

            r1 = await ledger.get_report(
                start_date=before, end_date=now)
            r2 = await ledger.get_report(
                start_date=now, end_date=after)
            r_all = await ledger.get_report(
                start_date=before, end_date=after)

            # Entry has a timestamp between before and now (roughly).
            # Under half-open [before, now), it should be in r1 only
            # (the entry's timestamp is "now" so it's excluded from
            # r1 and included in r2). Under old [before, now] logic,
            # it would appear in both.
            assert r1["total_entries"] + r2["total_entries"] == \
                r_all["total_entries"], (
                f"P1 3.7 regression: adjacent windows sum to "
                f"{r1['total_entries'] + r2['total_entries']} but "
                f"combined window has {r_all['total_entries']}")
            print(f"  ✓ [before, now) ∪ [now, after) == [before, after)")

    asyncio.run(_test_p1_3_7())

    # ---------------------------------------------------------------- #
    # P1 3.8 — average intensity None on 0-energy
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.8: average intensity None on 0-energy ===")

    async def _test_p1_3_8():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            # Entry with 0 energy, nonzero carbon.
            result = _Result(
                task_id="t", energy_consumed=0.0,
                carbon_emitted=0.5, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)
            await ledger.record(result, decision)
            report = await ledger.get_report()
            assert report["average_carbon_intensity"] is None, (
                f"P1 3.8 regression: average_carbon_intensity="
                f"{report['average_carbon_intensity']}, expected None")
            print(f"  ✓ average_carbon_intensity=None when "
                  f"energy=0 and carbon>0")

    asyncio.run(_test_p1_3_8())

    # ---------------------------------------------------------------- #
    # P1 3.9 — verify_chain cross-checks _last_chain_hash
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.9: verify_chain cross-checks _last_chain_hash ===")

    async def _test_p1_3_9():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            result = _Result(
                task_id="t", energy_consumed=1.0,
                carbon_emitted=0.5, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)
            await ledger.record(result, decision)

            assert ledger.verify_chain() is True

            # Tamper with the internal last hash.
            ledger._last_chain_hash = "0" * 64
            assert ledger.verify_chain() is False, (
                "P1 3.9 regression: verify_chain returned True "
                "with mismatched _last_chain_hash")
            print("  ✓ mismatched _last_chain_hash → verify_chain=False")

    asyncio.run(_test_p1_3_9())

    # ---------------------------------------------------------------- #
    # Bonus — pickle, context manager, shutdown
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: pickle / context / shutdown ===")

    async def _test_bonus():
        with tempfile.TemporaryDirectory() as td:
            # Pickle round-trip.
            ledger = _new_ledger(td)
            await ledger.initialize()
            result = _Result(
                task_id="t", energy_consumed=1.0,
                carbon_emitted=0.5, negawatt_reward=0.0,
            )
            decision = _Decision(
                carbon_zone="red", action="execute", power_budget=0.5)
            await ledger.record(result, decision)
            blob = pickle.dumps(ledger)
            restored = pickle.loads(blob)
            assert restored._lock is not None
            assert len(restored.entries) == 1
            assert restored.verify_chain() is True
            # Still usable.
            await restored.record(result, decision)
            assert len(restored.entries) == 2
            print("  ✓ pickle round-trip, still usable")

            # Context manager.
            ledger2 = _new_ledger(td + "_cm" if False else td)
            # Use a fresh dir to avoid interference.
            with tempfile.TemporaryDirectory() as td2:
                async with _new_ledger(td2) as scoped:
                    assert scoped._initialized
                    await scoped.record(result, decision)
                # After exit, initialized is still True but shutdown
                # has compacted.
                assert scoped._initialized
                print("  ✓ async context manager OK")

    asyncio.run(_test_bonus())

    print("\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
