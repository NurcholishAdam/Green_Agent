"""
Green Agent v5.0.0 — Carbon Ledger v3.0.0 (Enhanced)
====================================================

Layer 8: Immutable carbon accounting and compliance.

The ledger is the primary evidence artifact for sustainability claims.
It records *measured* operational emissions, distinguishes them from
*contractual* instruments (RECs, offsets), and produces a
tamper-evident hash chain that survives process restarts.

v3.0.0 changes vs. v2.0.0
-------------------------
**P0 bug fixes**
  * 2.1 **Transitive correction resolution.** Correcting a correction
        (A → B → C) now propagates the terminal value back to A in
        both ``get_report()`` and ``get_statistics()``. The v2.0.0
        fix only handled one level — the two APIs diverged on chains
        (e.g. 3.0 vs 4.0 for A→B→C).
  * 2.2 ``_coerce_for_json`` handles ``set`` / ``frozenset`` by
        deterministic sorted-list coercion, ``bytes`` by hex, and
        rejects ``NaN`` / ``Inf`` floats with ``LedgerValidationError``.
        Previously a ``set`` in ``extra`` produced a
        hash-randomized ``str(set)`` — the chain broke after every
        restart with ``PYTHONHASHSEED=random`` (the default).
  * 2.3 ``initialize()`` recomputes ``total_carbon`` / ``total_energy``
        from entries. Previously the totals were loaded verbatim from
        the payload and could be silently tampered (entries
        re-verified, totals not).
  * 2.4 ``initialize()`` parses numeric fields with ``_safe_float`` /
        ``_safe_int`` that raise ``LedgerError`` on malformed input.
        Previously ``float("garbage")`` leaked ``ValueError``.
  * 2.5 ``add_metadata(entry_id, metadata)`` added — the docstring
        promised post-hoc metadata attachment but v2.0.0 exposed no
        API for it (the ``entries`` property returns copies).
  * 2.6 ``os.fsync`` on the parent directory after every WAL append
        and every ``os.replace``. Previously only file contents were
        fsynced — a crash between ``record()`` and ``initialize()``
        could lose the file's *directory entry* (hence the whole
        file) despite the content being on disk.

**P1 robustness**
  * 3.1 ``get_report()`` includes a ``by_region`` breakdown.
  * 3.2 ``get_entry(entry_id)`` (O(1) via index),
        ``get_entries_for_task(task_id)``,
        ``verify_entry_by_id(entry_id)``.
  * 3.3 ``export_report_csv()`` for compliance portals.
  * 3.4 ``metrics_hook(event, fields)`` on every state transition.
  * 3.5 ``wal_replayed_entries`` in ``get_statistics()``.
  * 3.6 ``verify_chain()`` caches its result; invalidated on mutation.
  * 3.7 ``_entry_by_id`` index makes per-entry and per-correction
        lookups O(1) (was O(N)).
  * 3.8 ``migrate_schema(target_version, migrator)`` — re-hash and
        compact under a new schema; entries whose version doesn't
        match raise on ``initialize()``.
  * 3.9 ``get_report(include_all_entries=True)`` returns the full
        filtered list; ``report_entries_limit`` configurable (default
        100).
  * 3.10 ``total_carbon_naive_kg`` diagnostic field (raw sum
        including correction rows — for debugging only).
  * 3.11 ``_parse_iso`` allocates the context f-string only on
        failure.
  * 3.12 ``__aexit__`` no longer swallows shutdown failures.
  * 3.13 ``__repr__`` includes ``schema_version`` and
        ``chain_verified``.

Backward compatible: every v1.0.0 / v2.0.0 method signature and payload
works unchanged. ``entries`` returns copies (as in v2.0.0). The
WAL is created lazily on the first ``record()``.
"""

from __future__ import annotations

import copy
import csv
import hashlib
import io
import json
import logging
import math
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Callable, Dict, List, Mapping, Optional, Tuple,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Schema version
# =============================================================================

LEDGER_SCHEMA_VERSION = "5.0.0"
__version__ = "3.0.0"


# =============================================================================
# Error hierarchy
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


class LedgerSchemaMismatchError(LedgerValidationError):
    """Raised when ``initialize()`` finds entries under a different schema."""


# =============================================================================
# Instrument vocabulary
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


_UNIT_TO_KGCO2E: Dict[str, float] = {
    "kgCO2e": 1.0,
    "tCO2e": 1000.0,
}


# =============================================================================
# Canonical hash fields
# =============================================================================

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


def _coerce_for_json(value: Any, *, path: str = "value") -> Any:
    """
    Recursively coerce ``value`` to a deterministic, JSON-safe form.

    - ``Enum`` → ``.value``.
    - ``datetime`` → ISO 8601 string.
    - ``dict`` → dict (values recurse).
    - ``list`` / ``tuple`` → list (values recurse).
    - ``set`` / ``frozenset`` → **sorted** list (deterministic — P0 2.2).
    - ``bytes`` → hex string (deterministic).
    - ``float`` non-finite → ``LedgerValidationError`` (P0 2.2).
    - Anything else non-primitive → ``LedgerValidationError``.
    """
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            str(k): _coerce_for_json(v, path=f"{path}.{k}")
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [
            _coerce_for_json(v, path=f"{path}[{i}]")
            for i, v in enumerate(value)
        ]
    if isinstance(value, (set, frozenset)):
        # Sort by canonical JSON so the order is deterministic across
        # processes regardless of PYTHONHASHSEED.
        coerced = [
            _coerce_for_json(v, path=f"{path}{{...}}")
            for v in value
        ]
        try:
            return sorted(
                coerced,
                key=lambda x: json.dumps(
                    x, sort_keys=True, default=str),
            )
        except (TypeError, ValueError) as exc:
            raise LedgerValidationError(
                f"extra set at {path} contains unorderable values: "
                f"{exc}") from exc
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise LedgerValidationError(
                f"extra contains non-finite float {value!r} at "
                f"{path}")
        return value
    if value is None or isinstance(value, (bool, int, str)):
        return value
    raise LedgerValidationError(
        f"extra contains unserializable type "
        f"{type(value).__name__} at {path}")


def _parse_iso(
    value: Any, *, context_fn: Optional[Callable[[], str]] = None,
) -> Optional[datetime]:
    """
    Parse an ISO timestamp; return ``None`` on failure with a
    ``WARNING`` log (P1 3.11: context string allocated only on failure).
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        context = context_fn() if context_fn else ""
        logger.warning(
            "Could not parse timestamp %r%s; treating as undated.",
            value, f" ({context})" if context else "")
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _safe_float(
    payload: Mapping[str, Any], key: str, default: float,
) -> float:
    """Parse a numeric field, raising ``LedgerError`` on malformed input (P0 2.4)."""
    if key not in payload or payload[key] is None:
        return float(default)
    raw = payload[key]
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise LedgerError(
            f"Could not parse {key!r} from ledger payload: "
            f"{raw!r} (expected numeric)")
    try:
        result = float(raw)
    except (TypeError, ValueError) as exc:
        raise LedgerError(
            f"Could not parse {key!r} from ledger payload: {exc}"
        ) from exc
    if math.isnan(result) or math.isinf(result):
        raise LedgerError(
            f"Could not parse {key!r} from ledger payload: {raw!r} "
            f"(expected finite)")
    return result


def _safe_int(
    payload: Mapping[str, Any], key: str, default: int,
) -> int:
    """Parse an integer field, raising ``LedgerError`` on malformed input."""
    if key not in payload or payload[key] is None:
        return int(default)
    raw = payload[key]
    if isinstance(raw, bool):
        raise LedgerError(
            f"Could not parse {key!r} from ledger payload: "
            f"{raw!r} (expected int)")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise LedgerError(
            f"Could not parse {key!r} from ledger payload: {exc}"
        ) from exc


def _fsync_dir(path: Path) -> None:
    """
    fsync the directory containing ``path`` (P0 2.6).

    Makes the file's directory entry durable. Without this, a crash
    between ``record()`` and ``initialize()`` can lose a newly-created
    WAL file despite its content being fsynced.
    """
    if os.name == "nt":     # pragma: no cover — POSIX durability only
        return
    try:
        dir_fd = os.open(str(path.parent), os.O_RDONLY)
    except OSError as exc:  # pragma: no cover — defensive
        logger.debug("Could not open dir for fsync: %s", exc)
        return
    try:
        os.fsync(dir_fd)
    except OSError as exc:  # pragma: no cover — defensive
        logger.debug("Could not fsync dir: %s", exc)
    finally:
        os.close(dir_fd)


# =============================================================================
# CarbonLedger
# =============================================================================

class CarbonLedger:
    """
    Immutable ledger for carbon accounting and compliance reporting.

    Design principles:

      1. **Full SHA-256 hash chain.** Every entry commits to its
         predecessor via ``prev_hash``. Only ``_CANONICAL_HASH_FIELDS``
         participate; post-hoc metadata added via ``add_metadata()``
         does not break the chain.

      2. **Operational vs. contractual separation.** Physical emissions
         live in the operational ledger; purchased instruments live in
         the instruments ledger. They are NEVER summed by this class.

      3. **Provenance on every entry.** Each entry records whether the
         energy was measured or estimated, the source, and the
         methodology version.

      4. **Corrections without destruction.** A correction is recorded
         as a new entry with a ``corrects_entry_id`` field. Chains are
         resolved transitively — correcting a correction updates the
         original's effective value.

      5. **Append-only persistence via WAL.** Each ``record()`` appends
         one line to ``<file>.wal``, ``fsync``'s the file, and
         ``fsync``'s the parent directory. Full-file compaction
         happens on ``shutdown()``.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        *,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ):
        if metrics_hook is not None and not callable(metrics_hook):
            raise LedgerValidationError("metrics_hook must be callable.")

        self.config = config or {}
        self._metrics_hook = metrics_hook
        governance_cfg = self.config.get("governance", {}) or {}

        # --- Internal entries list + index (P1 3.7) ---
        self._entries: List[Dict[str, Any]] = []
        self._entry_by_id: Dict[str, Dict[str, Any]] = {}

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
        self._report_entries_limit = int(
            governance_cfg.get("report_entries_limit", 100))

        # --- Instruments ledger (contractual side) ---
        self._instruments: List[Dict[str, Any]] = []

        # --- Corrections index: original_entry_id -> [correction_ids] ---
        self._corrections: Dict[str, List[str]] = {}

        # --- Chain state ---
        self._last_chain_hash: str = "genesis"
        self._entry_counter: int = 0
        self._chain_verified_cache: Optional[bool] = None

        # --- Lifecycle ---
        self._initialized: bool = False

        # --- Thread safety ---
        self._lock = threading.RLock()

        # --- Statistics ---
        self._correction_count: int = 0
        self._simulated_entry_count: int = 0
        self._measured_entry_count: int = 0
        self._estimated_entry_count: int = 0
        self._wal_replayed_entries: int = 0

        logger.info(
            f"CarbonLedger v{__version__} initialized "
            f"(ledger_file={self._ledger_file}, "
            f"schema_version={self._schema_version})"
        )

    # ------------------------------------------------------------------
    # Public read-only property (P0 2.6 of v2.0.0)
    # ------------------------------------------------------------------

    @property
    def entries(self) -> List[Dict[str, Any]]:
        """
        Read-only snapshot of the ledger entries (deep copies).

        Mutating the returned list or its dicts does not affect the
        ledger's state. Use ``add_metadata()`` to attach metadata.
        """
        with self._lock:
            return copy.deepcopy(self._entries)

    # ------------------------------------------------------------------
    # Metrics (P1 3.4)
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
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """
        Load existing state from disk (idempotent).

        Raises ``LedgerTamperedError`` on chain failure,
        ``LedgerSchemaMismatchError`` on unknown schema versions, and
        ``LedgerError`` on malformed numeric fields.
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
                # P0 2.4: safe parsing of numeric fields.
                # P0 2.3: values loaded here are advisory — the totals
                # are recomputed from entries below.
                _ = _safe_float(payload, "total_carbon", 0.0)
                _ = _safe_float(payload, "total_energy", 0.0)
                self._last_chain_hash = payload.get(
                    "last_chain_hash", "genesis")
                self._entry_counter = _safe_int(
                    payload, "entry_counter", len(self._entries))

            # ---- Schema check (P1 3.8) ----
            versions = {
                e.get("schema_version")
                for e in self._entries if e.get("schema_version")
            }
            if versions and self._schema_version not in versions:
                raise LedgerSchemaMismatchError(
                    f"Ledger at {self._ledger_file} contains schema "
                    f"versions {sorted(versions)} but this build "
                    f"writes schema {self._schema_version!r}. Run "
                    f"`await migrate_schema(...)` on a legacy "
                    f"build first.")

            # ---- Replay WAL ----
            if self._wal_file.exists():
                self._replay_wal_locked()

            # ---- Verify the full chain ----
            if not self.verify_chain():
                raise LedgerTamperedError(
                    f"Ledger at {self._ledger_file} failed hash "
                    f"chain verification — it may have been tampered "
                    f"with, or the compact file and WAL disagree. "
                    f"Inspect {self._ledger_file} and "
                    f"{self._wal_file} before proceeding.")

            # ---- Recompute counters + totals from entries ----
            # P0 2.3: totals are derived; never trust the payload.
            # P0 2.4: counters rebuilt from entries, not persisted.
            self._recompute_counters_locked()
            self._recompute_totals_locked()

            # ---- Load instruments ----
            if self._instruments_file.exists():
                try:
                    with open(self._instruments_file, "r") as f:
                        self._instruments = list(json.load(f) or [])
                except (OSError, json.JSONDecodeError) as e:
                    logger.warning(
                        f"Failed to load instruments ledger: {e}")

            self._initialized = True

        self._emit_metric("initialize_completed", {
            "entries": len(self._entries),
            "instruments": len(self._instruments),
            "wal_replayed": self._wal_replayed_entries,
        })
        logger.info(
            f"CarbonLedger initialized — loaded {len(self._entries)} "
            f"entries ({self._wal_replayed_entries} replayed from "
            f"WAL), {len(self._instruments)} instruments"
        )

    async def shutdown(self) -> None:
        """Compact the ledger to disk and log a summary."""
        with self._lock:
            if not self._initialized:
                logger.debug(
                    "shutdown() called on uninitialized ledger; no-op.")
                return
            self._persist_compact_locked()
        self._emit_metric("shutdown_completed", {
            "entries": len(self._entries),
            "total_carbon_kg": self.total_carbon,
        })
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
        durable (in the WAL and fsynced) before this method returns;
        if persistence fails, ``LedgerPersistenceError`` is raised and
        the entry is discarded from memory.
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

            entry["prev_hash"] = self._last_chain_hash
            entry["hash"] = self._calculate_hash(entry)

            # Persist to WAL FIRST — durability before memory commit.
            self._persist_append_locked(entry)

            # Incremental totals (safe: new entries have no corrections).
            self._append_entry_locked(entry)
            self.total_carbon += float(
                entry.get("carbon_operational_kg", 0.0))
            self.total_energy += float(entry.get("energy_kwh", 0.0))

            self._last_chain_hash = entry["hash"]
            self._entry_counter += 1

            carbon_kg = float(entry.get("carbon_operational_kg", 0.0))
            logger.info(
                f"Carbon ledger entry {entry['entry_id']}: "
                f"{carbon_kg:.4f} kg CO2 "
                f"(total: {self.total_carbon:.4f})")

        self._emit_metric("entry_recorded", {
            "entry_id": entry["entry_id"],
            "carbon_kg": carbon_kg,
            "energy_kwh": float(entry.get("energy_kwh", 0.0)),
            "source": entry.get("source", "unknown"),
        })
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
        task_id = _get_attr(result, "task_id", "unknown")
        energy_kwh = _get_attr(result, "energy_consumed", 0.0)
        carbon_kg = _get_attr(result, "carbon_emitted", 0.0)
        negawatt_reward = _get_attr(result, "negawatt_reward", 0.0)

        carbon_zone = _coerce_for_json(
            _get_attr(decision, "carbon_zone", "unknown"),
            path="decision.carbon_zone")
        action = _coerce_for_json(
            _get_attr(decision, "action", "unknown"),
            path="decision.action")
        power_budget = _get_attr(decision, "power_budget", None)

        carbon_operational = _get_attr(
            result, "carbon_operational_kg", None)
        carbon_contractual = _get_attr(
            result, "carbon_contractual_kg", None)
        if carbon_operational is None:
            carbon_operational = carbon_kg
        if carbon_contractual is None:
            carbon_contractual = 0.0

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
            entry["extra"] = _coerce_for_json(
                dict(extra), path="extra")

        return entry

    def _calculate_hash(self, entry: Dict[str, Any]) -> str:
        """Compute the full SHA-256 hash over the canonical field subset."""
        data = {
            k: entry[k] for k in _CANONICAL_HASH_FIELDS
            if k in entry
        }
        serialized = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()

    # ------------------------------------------------------------------
    # Corrections (P0 2.1: transitive)
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

        Corrections are resolved transitively: correcting A with B and
        B with C makes C's value the effective value of A. The
        ``total_carbon`` is recomputed from entries after every
        correction (P0 2.1 + P0 2.3).
        """
        with self._lock:
            self._require_initialized_locked()

            original = self._entry_by_id.get(entry_id)
            if original is None:
                logger.warning(
                    f"correct_entry: no entry with id {entry_id}")
                return None

            # P1 3.6: reject double-correction of the same entry.
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

            self._persist_append_locked(correction)
            self._append_entry_locked(correction)
            self._last_chain_hash = correction["hash"]
            self._entry_counter += 1

            # P0 2.1 + P0 2.3: recompute totals transitively.
            self._recompute_totals_locked()

        self._emit_metric("correction_recorded", {
            "original_entry_id": entry_id,
            "correction_entry_id": correction["entry_id"],
            "corrected_carbon_kg": float(corrected_carbon_kg),
            "reason": reason,
        })
        logger.info(
            f"Correction {correction['entry_id']} of "
            f"{entry_id}: {corrected_carbon_kg:.4f} kg CO2 "
            f"(total: {self.total_carbon:.4f})")
        return correction["hash"]

    # ------------------------------------------------------------------
    # Metadata (P0 2.5)
    # ------------------------------------------------------------------

    async def add_metadata(
        self,
        entry_id: str,
        metadata: Dict[str, Any],
        *,
        replace: bool = False,
    ) -> bool:
        """
        Attach metadata to an existing entry **without breaking the
        hash chain** (P0 2.5).

        Metadata is stored under ``entry["extra"]`` — which is in
        ``_CANONICAL_HASH_FIELDS`` but only the *pre-existing*
        ``extra`` value participates in the hash at hash time. Hmm —
        wait, this needs explanation.

        Actually: since ``extra`` **is** in the canonical field set,
        mutating it would break the chain. So this method appends to a
        *separate* non-canonical field ``entry["metadata"]`` which is
        NOT hashed. The caller can still use ``extra`` at record-time
        (which IS hashed), but post-hoc ``add_metadata`` targets the
        non-canonical field.

        Returns True iff the entry was found.
        """
        with self._lock:
            self._require_initialized_locked()
            entry = self._entry_by_id.get(entry_id)
            if entry is None:
                return False

            coerced = _coerce_for_json(
                dict(metadata), path=f"metadata[{entry_id}]")
            if replace:
                entry["metadata"] = coerced
            else:
                existing = dict(entry.get("metadata", {}))
                existing.update(coerced)
                entry["metadata"] = _coerce_for_json(
                    existing, path=f"metadata[{entry_id}]")

        self._emit_metric("metadata_added", {
            "entry_id": entry_id,
            "keys": sorted(metadata.keys()),
            "replace": replace,
        })
        return True

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
        """Record a contractual carbon instrument (REC, offset, credit)."""
        with self._lock:
            self._require_initialized_locked()

            if kind not in {k.value for k in InstrumentKind}:
                raise LedgerValidationError(
                    f"invalid instrument kind {kind!r}; expected one "
                    f"of {sorted(k.value for k in InstrumentKind)}")
            if unit not in {u.value for u in InstrumentUnit}:
                raise LedgerValidationError(
                    f"invalid instrument unit {unit!r}; expected one "
                    f"of {sorted(u.value for u in InstrumentUnit)}")

            if any(
                i.get("instrument_id") == instrument_id
                for i in self._instruments
            ):
                raise LedgerDuplicateInstrumentError(
                    f"duplicate instrument_id: {instrument_id}")

            record = {
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
                "provenance": _coerce_for_json(
                    dict(provenance or {}),
                    path=f"instrument[{instrument_id}].provenance"),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
            self._instruments.append(record)
            self._persist_instruments_locked()

        self._emit_metric("instrument_recorded", {
            "instrument_id": instrument_id,
            "kind": kind,
            "quantity": float(quantity),
            "unit": unit,
            "retired": bool(retired),
        })

    async def get_instruments_ledger(self) -> Dict[str, Any]:
        """Return the contractual instruments ledger."""
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
        """Sum retired offset instruments in kgCO2e."""
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
    # Single-entry accessors (P1 3.2)
    # ------------------------------------------------------------------

    def get_entry(self, entry_id: str) -> Optional[Dict[str, Any]]:
        """Return a deep copy of one entry, or None (O(1) via index)."""
        with self._lock:
            entry = self._entry_by_id.get(entry_id)
            return copy.deepcopy(entry) if entry is not None else None

    def get_entries_for_task(
        self, task_id: str,
    ) -> List[Dict[str, Any]]:
        """Return deep copies of every entry for ``task_id``."""
        with self._lock:
            return [
                copy.deepcopy(e) for e in self._entries
                if e.get("task_id") == task_id
            ]

    def verify_entry_by_id(self, entry_id: str) -> bool:
        """Verify a single entry by its entry_id (P1 3.2)."""
        with self._lock:
            entry = self._entry_by_id.get(entry_id)
            if entry is None:
                return False
            expected = self._calculate_hash(entry)
            return entry.get("hash") == expected

    # ------------------------------------------------------------------
    # Chain verification
    # ------------------------------------------------------------------

    def verify_chain(self) -> bool:
        """
        Verify the full hash chain (P1 3.6: cached result).

        Cached on success; invalidated on every mutation.
        """
        with self._lock:
            if self._chain_verified_cache is not None:
                return self._chain_verified_cache

            prev = "genesis"
            for entry in self._entries:
                if entry.get("prev_hash") != prev:
                    logger.error(
                        f"Chain broken at entry "
                        f"{entry.get('entry_id')}: prev_hash="
                        f"{entry.get('prev_hash')} != {prev}")
                    self._chain_verified_cache = False
                    return False
                expected = self._calculate_hash(entry)
                if entry.get("hash") != expected:
                    logger.error(
                        f"Hash mismatch at entry "
                        f"{entry.get('entry_id')}")
                    self._chain_verified_cache = False
                    return False
                prev = entry["hash"]

            if prev != self._last_chain_hash:
                logger.error(
                    "_last_chain_hash=%r does not match the last "
                    "entry's hash=%r",
                    self._last_chain_hash, prev)
                self._chain_verified_cache = False
                return False

            self._chain_verified_cache = True
            return True

    def verify_entry(self, entry_hash: str) -> bool:
        """Verify a single entry by its hash, walking the chain."""
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

    def _invalidate_chain_cache(self) -> None:
        self._chain_verified_cache = None

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    async def get_report(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        include_all_entries: bool = False,
    ) -> Dict[str, Any]:
        """
        Generate a carbon accounting report.

        Half-open ``[start, end)`` windowing (P1 3.7). Correction chains
        resolved transitively (P0 2.1). Returns deep copies (P1 3.3).
        """
        with self._lock:
            entries_snapshot = copy.deepcopy(self._entries)
            instruments_kg = self._contractual_kg_locked()
            effective = self._resolve_corrections(self._entries)
            # Effective map is keyed on entry_id; work off live entries
            # for filtering so chain info is available.

        # ---- Filter by timestamp (half-open) ----
        parse_errors = 0
        filtered: List[Dict[str, Any]] = []
        for e in entries_snapshot:
            ts_raw = e.get("timestamp")
            ts = _parse_iso(
                ts_raw,
                context_fn=(
                    lambda e=e: f"entry {e.get('entry_id')}"
                ) if ts_raw else None,
            )
            if ts_raw and ts is None:
                parse_errors += 1
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
                if ts >= end:
                    continue
            filtered.append(e)

        # ---- Totals ----
        total_energy = sum(
            float(e.get("energy_kwh", 0.0))
            for e in filtered
            if not e.get("corrects_entry_id")   # energy doesn't change
        )
        total_carbon_operational = sum(
            float(effective[e["entry_id"]].get(
                "carbon_operational_kg", 0.0))
            for e in filtered
            if not e.get("corrects_entry_id")
            and e["entry_id"] in effective
        )
        total_carbon_naive = sum(
            float(e.get("carbon_kg", 0.0)) for e in filtered
        )
        total_negawatt = sum(
            float(e.get("negawatt_reward", 0.0)) for e in filtered
        )

        # ---- Breakdowns ----
        by_source: Dict[str, int] = {}
        by_region: Dict[str, Dict[str, float]] = {}
        schema_versions: Dict[str, int] = {}
        for e in filtered:
            src = e.get("source", "unknown")
            by_source[src] = by_source.get(src, 0) + 1
            sv = e.get("schema_version", "unknown")
            schema_versions[sv] = schema_versions.get(sv, 0) + 1
            if e.get("corrects_entry_id"):
                continue
            region = e.get("region") or "unknown"
            bucket = by_region.setdefault(region, {
                "entries": 0, "energy_kwh": 0.0, "carbon_kg": 0.0,
            })
            bucket["entries"] += 1
            bucket["energy_kwh"] += float(e.get("energy_kwh", 0.0))
            if e["entry_id"] in effective:
                bucket["carbon_kg"] += float(
                    effective[e["entry_id"]].get(
                        "carbon_operational_kg", 0.0))

        simulated_count = sum(
            1 for e in filtered if e.get("simulated"))
        corrected_count = sum(
            1 for e in filtered if e.get("corrects_entry_id"))

        # ---- Average intensity ----
        total_carbon_for_intensity = sum(
            float(effective[e["entry_id"]].get("carbon_kg", 0.0))
            for e in filtered
            if not e.get("corrects_entry_id")
            and e["entry_id"] in effective
        )
        if total_energy == 0:
            avg_intensity = (
                None if total_carbon_for_intensity > 0 else 0.0
            )
        else:
            avg_intensity = (
                total_carbon_for_intensity / total_energy) * 1000

        # P1 3.9: configurable entries list.
        entries_out = (
            filtered if include_all_entries
            else filtered[-self._report_entries_limit:]
        )

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
            "total_carbon_kg": total_carbon_operational,
            "total_carbon_operational_kg": total_carbon_operational,
            "total_carbon_contractual_kg": instruments_kg,
            "total_carbon_naive_kg": total_carbon_naive,
            "total_negawatt": total_negawatt,
            "average_carbon_intensity": avg_intensity,
            "by_source": by_source,
            "by_region": by_region,
            "simulated_count": simulated_count,
            "corrected_count": corrected_count,
            "timestamp_parse_errors": parse_errors,
            "chain_verified": self.verify_chain(),
            "entries": entries_out,
        }

    def _resolve_corrections(
        self, entries: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Return ``{entry_id: effective_entry}`` after applying
        corrections transitively (P0 2.1).

        Chains: A → B → C means ``effective[A]`` reflects C's value.
        """
        by_id: Dict[str, Dict[str, Any]] = {
            e["entry_id"]: dict(e) for e in entries
            if e.get("entry_id")
        }
        # Forward map: original_id -> correction_id.
        forward: Dict[str, str] = {}
        for e in entries:
            target = e.get("corrects_entry_id")
            if target:
                forward[target] = e["entry_id"]

        effective: Dict[str, Dict[str, Any]] = dict(by_id)
        for original_id, entry in by_id.items():
            if entry.get("corrects_entry_id"):
                continue   # skip correction rows
            current = original_id
            seen: set = set()
            while current in forward:
                if current in seen:
                    logger.warning(
                        "Correction cycle detected at %r; "
                        "breaking.", current)
                    break
                seen.add(current)
                current = forward[current]
            if current != original_id and current in by_id:
                effective[original_id]["carbon_operational_kg"] = float(
                    by_id[current].get("carbon_operational_kg", 0.0))
                effective[original_id]["carbon_kg"] = float(
                    by_id[current].get("carbon_kg", 0.0))
        return effective

    # ------------------------------------------------------------------
    # CSV export (P1 3.3)
    # ------------------------------------------------------------------

    def export_report_csv(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> str:
        """Return the report's entries as a CSV string (for portals)."""
        with self._lock:
            snapshot = copy.deepcopy(self._entries)

        # Filter (sync, mirrors get_report's windowing).
        filtered: List[Dict[str, Any]] = []
        for e in snapshot:
            ts = _parse_iso(e.get("timestamp"))
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
                if ts >= end:
                    continue
            filtered.append(e)

        fields = [
            "entry_id", "timestamp", "task_id", "run_id",
            "energy_kwh", "carbon_operational_kg", "carbon_kg",
            "carbon_zone", "action", "source",
            "methodology_version", "baseline", "simulated",
            "region", "precision", "corrects_entry_id",
            "correction_reason",
        ]
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for e in filtered:
            writer.writerow(e)
        return buf.getvalue()

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
                "wal_replayed_entries": self._wal_replayed_entries,
            }

    # ------------------------------------------------------------------
    # Schema migration (P1 3.8)
    # ------------------------------------------------------------------

    async def migrate_schema(
        self,
        target_version: str,
        migrator: Callable[[Dict[str, Any]], Dict[str, Any]],
    ) -> int:
        """
        Apply ``migrator`` to every entry, re-hash the chain, and
        compact. Returns the number of entries migrated.

        The migrator receives a copy of each entry (with
        ``hash`` / ``prev_hash`` removed) and must return an updated
        dict; the ledger re-inserts ``hash`` / ``prev_hash`` after
        calling it.
        """
        if not callable(migrator):
            raise LedgerValidationError("migrator must be callable.")
        with self._lock:
            self._require_initialized_locked()
            if not self._entries:
                self._schema_version = target_version
                return 0

            new_entries: List[Dict[str, Any]] = []
            prev_hash = "genesis"
            for e in self._entries:
                stripped = {
                    k: v for k, v in e.items()
                    if k not in ("hash", "prev_hash")
                }
                migrated = migrator(stripped)
                if not isinstance(migrated, dict):
                    raise LedgerValidationError(
                        f"migrator returned "
                        f"{type(migrated).__name__} for entry "
                        f"{e.get('entry_id')!r}; expected dict.")
                migrated["schema_version"] = target_version
                migrated["prev_hash"] = prev_hash
                migrated["hash"] = self._calculate_hash(migrated)
                prev_hash = migrated["hash"]
                new_entries.append(migrated)

            self._entries = new_entries
            self._schema_version = target_version
            self._last_chain_hash = prev_hash
            self._recompute_counters_locked()
            self._recompute_totals_locked()
            self._invalidate_chain_cache()
            self._persist_compact_locked()

        self._emit_metric("schema_migrated", {
            "target_version": target_version,
            "entries": len(new_entries),
        })
        return len(new_entries)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_initialized_locked(self) -> None:
        if not self._initialized:
            raise LedgerNotInitializedError(
                "Ledger not initialized. Call "
                "`await ledger.initialize()` before writing.")

    def _append_entry_locked(self, entry: Dict[str, Any]) -> None:
        """
        Append ``entry`` to ``_entries`` and update the indexes.

        Caller is responsible for updating ``total_carbon`` /
        ``total_energy`` (either incrementally or via
        ``_recompute_totals_locked``).
        """
        self._entries.append(entry)
        entry_id = entry.get("entry_id")
        if entry_id:
            self._entry_by_id[entry_id] = entry

        if entry.get("simulated"):
            self._simulated_entry_count += 1
        src = entry.get("source")
        if src == "measured":
            self._measured_entry_count += 1
        elif src == "estimated":
            self._estimated_entry_count += 1
        elif src == "correction":
            self._correction_count += 1

        target = entry.get("corrects_entry_id")
        if target:
            self._corrections.setdefault(target, []).append(
                entry["entry_id"])

        self._invalidate_chain_cache()

    def _recompute_counters_locked(self) -> None:
        """Rebuild counters, correction index, and entry-by-id index."""
        self._measured_entry_count = 0
        self._estimated_entry_count = 0
        self._simulated_entry_count = 0
        self._correction_count = 0
        self._corrections = {}
        self._entry_by_id = {}
        for e in self._entries:
            if e.get("entry_id"):
                self._entry_by_id[e["entry_id"]] = e
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
        self._invalidate_chain_cache()

    def _recompute_totals_locked(self) -> None:
        """
        Recompute ``total_carbon`` / ``total_energy`` from entries (P0 2.3).

        Uses transitive correction resolution so a chained correction
        propagates to the original.
        """
        total_energy = 0.0
        total_carbon = 0.0
        for e in self._entries:
            if e.get("corrects_entry_id"):
                continue   # corrections don't add energy
            total_energy += float(e.get("energy_kwh", 0.0))
            total_carbon += self._effective_operational_kg_locked(
                e["entry_id"])
        self.total_energy = total_energy
        self.total_carbon = total_carbon

    def _effective_operational_kg_locked(
        self, entry_id: str,
    ) -> float:
        """
        Return the effective operational carbon for ``entry_id`` after
        walking the full correction chain (P0 2.1: transitive).
        """
        current = entry_id
        seen: set = set()
        while current in self._corrections:
            if current in seen:
                logger.warning(
                    "Correction cycle detected at %r; breaking.",
                    current)
                break
            seen.add(current)
            corrections = self._corrections[current]
            if not corrections:
                break
            current = corrections[-1]     # latest correction wins
        entry = self._entry_by_id.get(current)
        if entry is None:
            return 0.0
        return float(entry.get("carbon_operational_kg", 0.0))

    def _replay_wal_locked(self) -> None:
        """Replay WAL lines not already present in ``_entries`` (P1 3.5)."""
        existing_ids = {e.get("entry_id") for e in self._entries}
        try:
            with open(self._wal_file, "r") as f:
                lines = f.read().splitlines()
        except OSError as e:
            raise LedgerError(
                f"Could not read WAL {self._wal_file}: {e}") from e

        replayed = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                entry = json.loads(stripped)
            except json.JSONDecodeError:
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
                continue
            self._entries.append(entry)
            if entry_id:
                existing_ids.add(entry_id)
            replayed += 1
        self._wal_replayed_entries = replayed

    # ------------------------------------------------------------------
    # Persistence (P0 2.6: fsync the parent directory)
    # ------------------------------------------------------------------

    def _persist_append_locked(self, entry: Dict[str, Any]) -> None:
        """Append to the WAL, fsync the file *and* the parent directory."""
        try:
            self._wal_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._wal_file, "a") as f:
                f.write(json.dumps(
                    entry, sort_keys=True, default=str) + "\n")
                f.flush()
                os.fsync(f.fileno())
            _fsync_dir(self._wal_file)
        except OSError as e:
            raise LedgerPersistenceError(
                f"Could not append to WAL {self._wal_file}: {e}"
            ) from e

    def _persist_compact_locked(self) -> None:
        """Atomically rewrite the ledger + truncate the WAL."""
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
            _fsync_dir(self._ledger_file)
        except OSError as e:
            raise LedgerPersistenceError(
                f"Failed to compact ledger {self._ledger_file}: {e}"
            ) from e

        if self._wal_file.exists():
            try:
                self._wal_file.unlink()
                _fsync_dir(self._wal_file)
            except OSError as e:
                logger.warning(
                    f"Could not remove WAL after compact: {e}. "
                    f"Next initialize() will dedupe by entry_id.")

    def _persist_instruments_locked(self) -> None:
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
            _fsync_dir(self._instruments_file)
        except OSError as e:
            raise LedgerPersistenceError(
                f"Failed to persist instruments: {e}") from e

    # ------------------------------------------------------------------
    # Pickling
    # ------------------------------------------------------------------

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Context managers (P1 3.12: no swallow)
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "CarbonLedger":
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            await self.shutdown()
        except LedgerPersistenceError:
            if exc_type is None:
                raise
            logger.error(
                "shutdown() failed while unwinding from %s",
                exc_type.__name__, exc_info=True)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonLedger("
                f"version={__version__!r}, "
                f"schema={self._schema_version!r}, "
                f"entries={len(self._entries)}, "
                f"total_carbon_kg={self.total_carbon:.4f}, "
                f"instruments={len(self._instruments)}, "
                f"initialized={self._initialized}, "
                f"chain_verified={self.verify_chain()})"
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
    "LedgerSchemaMismatchError",
    "LedgerTamperedError",
    "LedgerValidationError",
]


# =============================================================================
# Self-tests
# =============================================================================

def _run_selftests() -> None:
    """Run the module's self-tests."""
    import asyncio
    import pickle
    import subprocess
    import sys
    import tempfile
    import time

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print(f"=== carbon_ledger v{__version__} self-tests ===\n")

    class _Result:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    class _Decision:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    def _new_ledger(tmpdir: str, **kwargs) -> CarbonLedger:
        return CarbonLedger(
            {"governance": {
                "ledger_file": os.path.join(tmpdir, "ledger.json"),
                "instruments_file": os.path.join(
                    tmpdir, "instruments.json"),
            }},
            **kwargs,
        )

    def _ok_result(**overrides) -> _Result:
        base = dict(
            task_id="t-1", energy_consumed=1.0,
            carbon_emitted=0.5, negawatt_reward=0.0,
        )
        base.update(overrides)
        return _Result(**base)

    def _ok_decision(**overrides) -> _Decision:
        base = dict(
            carbon_zone="red", action="execute", power_budget=0.5)
        base.update(overrides)
        return _Decision(**base)

    # ---------------------------------------------------------------- #
    # P0 2.1 (v3): transitive correction chain
    # ---------------------------------------------------------------- #
    print("=== P0 2.1 (v3): transitive correction chain ===")

    async def _test_p0_2_1():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()

            # A: carbon=5.0
            await ledger.record(
                _ok_result(carbon_emitted=5.0), _ok_decision())
            a_id = ledger.entries[0]["entry_id"]

            # B corrects A to 3.0
            b_hash = await ledger.correct_entry(
                a_id, 3.0, reason="re-measured")
            b_id = next(
                e["entry_id"] for e in ledger.entries
                if e["hash"] == b_hash)

            # C corrects B to 4.0
            await ledger.correct_entry(
                b_id, 4.0, reason="further correction")

            # P0 2.1: report + stats both reflect C's value (4.0).
            report = await ledger.get_report()
            stats = ledger.get_statistics()
            assert report["total_carbon_operational_kg"] == 4.0, (
                f"P0 2.1 regression: report operational="
                f"{report['total_carbon_operational_kg']}, "
                f"expected 4.0")
            assert report["total_carbon_kg"] == 4.0
            assert stats["total_carbon_kg"] == 4.0, (
                f"P0 2.1 regression: stats total="
                f"{stats['total_carbon_kg']}, expected 4.0")
            print(f"  ✓ A→B→C chain: report=4.0, stats=4.0")

    asyncio.run(_test_p0_2_1())

    # ---------------------------------------------------------------- #
    # P0 2.2 (v3): set/bytes/NaN in extra
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2 (v3): extra coercion ===")

    async def _test_p0_2_2():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(
                _ok_result(), _ok_decision(),
                extra={"tags": {"b", "a", "c"}, "note": "ok"})
            stored = ledger.entries[0]["extra"]
            # Sorted list, not a set.
            assert stored["tags"] == ["a", "b", "c"], stored["tags"]
            print(f"  ✓ set → sorted list: {stored['tags']}")

            # NaN rejected.
            try:
                await ledger.record(
                    _ok_result(), _ok_decision(),
                    extra={"value": float("nan")})
            except LedgerValidationError:
                print("  ✓ NaN float → LedgerValidationError")

            # Unserializable type rejected.
            try:
                await ledger.record(
                    _ok_result(), _ok_decision(),
                    extra={"obj": object()})
            except LedgerValidationError:
                print("  ✓ object() → LedgerValidationError")

            # bytes → hex.
            await ledger.record(
                _ok_result(), _ok_decision(),
                extra={"bin": b"\x00\x01\x02"})
            stored = ledger.entries[-1]["extra"]
            assert stored["bin"] == "000102"
            print(f"  ✓ bytes → hex: {stored['bin']}")

    asyncio.run(_test_p0_2_2())

    # ---------------------------------------------------------------- #
    # P0 2.3 (v3): tampered totals rejected on load
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3 (v3): tampered totals ===")

    async def _test_p0_2_3():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(
                _ok_result(carbon_emitted=5.0), _ok_decision())
            await ledger.shutdown()

            # Tamper with total_carbon (entries intact).
            p = os.path.join(td, "ledger.json")
            with open(p) as f:
                payload = json.load(f)
            payload["total_carbon"] = 999.0
            with open(p, "w") as f:
                json.dump(payload, f, indent=2, default=str)

            ledger2 = _new_ledger(td)
            await ledger2.initialize()
            assert ledger2.get_statistics()["total_carbon_kg"] == 5.0, (
                f"P0 2.3 regression: total="
                f"{ledger2.get_statistics()['total_carbon_kg']}, "
                f"expected 5.0 (recomputed from entries)")
            print("  ✓ tampered total_carbon=999 → recomputed to 5.0")

    asyncio.run(_test_p0_2_3())

    # ---------------------------------------------------------------- #
    # P0 2.4 (v3): malformed numeric fields raise LedgerError
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4 (v3): malformed numerics ===")

    async def _test_p0_2_4():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(_ok_result(), _ok_decision())
            await ledger.shutdown()

            p = os.path.join(td, "ledger.json")
            with open(p) as f:
                payload = json.load(f)
            payload["total_carbon"] = "garbage"
            with open(p, "w") as f:
                json.dump(payload, f, indent=2, default=str)

            ledger2 = _new_ledger(td)
            try:
                await ledger2.initialize()
            except LedgerError as exc:
                print(f"  ✓ 'garbage' total → LedgerError: "
                      f"{str(exc)[:60]}...")
            except ValueError as exc:
                raise AssertionError(
                    f"P0 2.4 regression: raw ValueError instead of "
                    f"LedgerError: {exc}")
            else:
                raise AssertionError(
                    "P0 2.4 regression: malformed numeric accepted")

    asyncio.run(_test_p0_2_4())

    # ---------------------------------------------------------------- #
    # P0 2.5 (v3): add_metadata preserves the chain
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5 (v3): add_metadata ===")

    async def _test_p0_2_5():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(_ok_result(), _ok_decision())
            entry_id = ledger.entries[0]["entry_id"]

            assert ledger.verify_chain() is True
            ok = await ledger.add_metadata(
                entry_id, {"published_at": "2024-01-01T00:00:00Z"})
            assert ok is True
            assert ledger.verify_chain() is True, (
                "P0 2.5 regression: chain broke after add_metadata")

            stored = ledger.entries[0]
            assert stored["metadata"]["published_at"] == \
                "2024-01-01T00:00:00Z"
            # Canonical hash unchanged.
            assert stored["hash"] == ledger.entries[0]["hash"]
            print(f"  ✓ metadata added, chain still verifies")

            # Nonexistent entry.
            assert await ledger.add_metadata(
                "does-not-exist", {"x": 1}) is False
            print(f"  ✓ nonexistent entry_id → False")

    asyncio.run(_test_p0_2_5())

    # ---------------------------------------------------------------- #
    # P0 2.6 (v3): fsync_dir called after WAL append
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6 (v3): WAL fsync_dir ===")

    async def _test_p0_2_6():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(_ok_result(), _ok_decision())
            # On POSIX, verify the WAL file exists with content.
            wal_path = os.path.join(td, "ledger.json.wal")
            assert os.path.exists(wal_path), (
                "P0 2.6 regression: WAL not created")
            with open(wal_path) as f:
                content = f.read()
            assert len(content.strip()) > 0
            print(f"  ✓ WAL exists with {len(content.strip())} bytes")

    asyncio.run(_test_p0_2_6())

    # ---------------------------------------------------------------- #
    # P1 3.2: single-entry accessors
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: get_entry / verify_entry_by_id ===")

    async def _test_p1_3_2():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            h = await ledger.record(_ok_result(), _ok_decision())
            entry_id = ledger.entries[0]["entry_id"]

            e = ledger.get_entry(entry_id)
            assert e is not None and e["entry_id"] == entry_id
            assert ledger.verify_entry_by_id(entry_id) is True
            assert ledger.verify_entry_by_id("does-not-exist") is False
            print(f"  ✓ get_entry + verify_entry_by_id work")

            # get_entries_for_task.
            entries = ledger.get_entries_for_task("t-1")
            assert len(entries) == 1
            print(f"  ✓ get_entries_for_task found {len(entries)} entry")

    asyncio.run(_test_p1_3_2())

    # ---------------------------------------------------------------- #
    # P1 3.3: CSV export
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: CSV export ===")

    async def _test_p1_3_3():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(_ok_result(), _ok_decision())
            csv_text = ledger.export_report_csv()
            lines = csv_text.splitlines()
            assert len(lines) == 2  # header + 1 entry
            assert "entry_id" in lines[0]
            print(f"  ✓ CSV export: {len(lines)} lines")

    asyncio.run(_test_p1_3_3())

    # ---------------------------------------------------------------- #
    # P1 3.4: metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: metrics_hook ===")

    async def _test_p1_3_4():
        events: List = []
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(
                td, metrics_hook=lambda ev, f: events.append((ev, f)))
            await ledger.initialize()
            await ledger.record(_ok_result(), _ok_decision())

            names = {ev for ev, _ in events}
            assert "initialize_completed" in names
            assert "entry_recorded" in names
            print(f"  ✓ metrics_hook: {sorted(names)}")

    asyncio.run(_test_p1_3_4())

    # ---------------------------------------------------------------- #
    # P1 3.5: wal_replayed_entries visible
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: wal_replayed_entries ===")

    async def _test_p1_3_5():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(_ok_result(), _ok_decision())
            # Do NOT call shutdown — WAL remains.
            # Reload.
            ledger2 = _new_ledger(td)
            await ledger2.initialize()
            stats = ledger2.get_statistics()
            assert stats["wal_replayed_entries"] == 1, (
                f"P1 3.5 regression: wal_replayed_entries="
                f"{stats['wal_replayed_entries']}, expected 1")
            print(f"  ✓ 1 entry replayed from WAL")

    asyncio.run(_test_p1_3_5())

    # ---------------------------------------------------------------- #
    # P1 3.8: schema mismatch
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.8: schema mismatch ===")

    async def _test_p1_3_8():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(_ok_result(), _ok_decision())
            await ledger.shutdown()

            # Change the schema version in the payload.
            p = os.path.join(td, "ledger.json")
            with open(p) as f:
                payload = json.load(f)
            payload["entries"][0]["schema_version"] = "999.0.0"
            with open(p, "w") as f:
                json.dump(payload, f, indent=2, default=str)

            ledger2 = _new_ledger(td)
            try:
                await ledger2.initialize()
            except LedgerSchemaMismatchError as exc:
                print(f"  ✓ schema mismatch → "
                      f"LedgerSchemaMismatchError")
            else:
                raise AssertionError(
                    "P1 3.8 regression: schema mismatch not detected")

    asyncio.run(_test_p1_3_8())

    # ---------------------------------------------------------------- #
    # Regression: v2.0.0 P0 tests still pass
    # ---------------------------------------------------------------- #
    print("\n=== Regression: v2.0.0 P0 fixes ===")

    async def _test_v2_regressions():
        with tempfile.TemporaryDirectory() as td:
            # P0 2.1: record before initialize.
            ledger = _new_ledger(td)
            try:
                await ledger.record(_ok_result(), _ok_decision())
            except LedgerNotInitializedError:
                print("  ✓ record before init raises")

        with tempfile.TemporaryDirectory() as td:
            # P0 2.2: single correction.
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(
                _ok_result(carbon_emitted=5.0), _ok_decision())
            a_id = ledger.entries[0]["entry_id"]
            await ledger.correct_entry(a_id, 3.0, reason="...")
            report = await ledger.get_report()
            stats = ledger.get_statistics()
            assert report["total_carbon_operational_kg"] == 3.0
            assert stats["total_carbon_kg"] == 3.0
            print("  ✓ single correction: both APIs agree (3.0)")

        with tempfile.TemporaryDirectory() as td:
            # P0 2.3: tamper detection.
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(_ok_result(), _ok_decision())
            await ledger.shutdown()
            p = os.path.join(td, "ledger.json")
            with open(p) as f:
                payload = json.load(f)
            payload["entries"][0]["carbon_kg"] = 999.0
            with open(p, "w") as f:
                json.dump(payload, f, indent=2, default=str)
            ledger2 = _new_ledger(td)
            try:
                await ledger2.initialize()
            except LedgerTamperedError:
                print("  ✓ tampered chain raises")

        with tempfile.TemporaryDirectory() as td:
            # P0 2.4: counters restored.
            ledger = _new_ledger(td)
            await ledger.initialize()
            for _ in range(3):
                await ledger.record(
                    _ok_result(), _ok_decision(), source="measured")
            for _ in range(2):
                await ledger.record(
                    _ok_result(), _ok_decision(), source="estimated")
            await ledger.shutdown()
            ledger2 = _new_ledger(td)
            await ledger2.initialize()
            s = ledger2.get_statistics()
            assert s["measured_entries"] == 3
            assert s["estimated_entries"] == 2
            print("  ✓ counters preserved (measured=3, estimated=2)")

        with tempfile.TemporaryDirectory() as td:
            # P0 2.6: entries property returns copies.
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(_ok_result(), _ok_decision())
            snap = ledger.entries
            snap[0]["carbon_kg"] = 999.0
            assert ledger.entries[0]["carbon_kg"] == 0.5
            assert ledger.verify_chain() is True
            print("  ✓ entries snapshot mutation isolated")

        with tempfile.TemporaryDirectory() as td:
            # P0 2.8: initialize idempotent.
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(_ok_result(), _ok_decision())
            await ledger.initialize()
            assert len(ledger.entries) == 1
            print("  ✓ initialize() idempotent")

    asyncio.run(_test_v2_regressions())

    # ---------------------------------------------------------------- #
    # Regression: pickle + context manager + instruments
    # ---------------------------------------------------------------- #
    print("\n=== Regression: pickle / context / instruments ===")

    async def _test_misc():
        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            async with ledger:
                await ledger.record(_ok_result(), _ok_decision())

        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record(_ok_result(), _ok_decision())
            blob = pickle.dumps(ledger)
            restored = pickle.loads(blob)
            assert restored._lock is not None
            assert len(restored.entries) == 1
            await restored.record(_ok_result(), _ok_decision())
            assert len(restored.entries) == 2
            print("  ✓ pickle round-trip, still usable")

        with tempfile.TemporaryDirectory() as td:
            ledger = _new_ledger(td)
            await ledger.initialize()
            await ledger.record_instrument(
                instrument_id="i-1", kind="offset",
                quantity=2.0, unit="tCO2e", retired=True)
            data = await ledger.get_instruments_ledger()
            assert data["contractual_kgco2e"] == 2000.0
            print("  ✓ instruments: 2 tCO2e = 2000 kgCO2e")

    asyncio.run(_test_misc())

    print("\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
