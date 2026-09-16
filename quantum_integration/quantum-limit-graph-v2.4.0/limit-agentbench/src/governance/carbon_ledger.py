"""
Green Agent v5.0.0 - Carbon Ledger (Enhanced)
==============================================

Layer 8: Immutable carbon accounting and compliance.

The ledger is the primary evidence artifact for sustainability claims.
It records *measured* operational emissions, distinguishes them from
*contractual* instruments (RECs, offsets), and produces a
tamper-evident hash chain that survives process restarts.

Original API preserved:
    ledger = CarbonLedger(config)
    await ledger.initialize()
    hash_ = await ledger.record(result, decision)
    report = await ledger.get_report(start_date, end_date)
    await ledger.shutdown()

Enhanced API:
    entry_id = await ledger.record(
        result, decision,
        source="codecarbon",
        methodology_version="v5.0.0",
        baseline="FP32-2025Q4",
    )
    await ledger.correct_entry(entry_id, corrected_kg, reason="...")
    instruments = await ledger.get_instruments_ledger()
    chain_ok = ledger.verify_chain()
    stats = ledger.get_statistics()
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# =============================================================================
# Schema version
# =============================================================================

LEDGER_SCHEMA_VERSION = "5.0.0"


# =============================================================================
# CarbonLedger
# =============================================================================

class CarbonLedger:
    """
    Immutable ledger for carbon accounting and compliance reporting.

    Design principles (per the governance recommendation):

      1. **Full SHA-256 hash chain.** Every entry commits to its
         predecessor, so deletion, reordering, or insertion invalidates
         the entire subsequent history.

      2. **Operational vs. contractual separation.** Physical emissions
         live in `entries`; purchased instruments live in
         `_instruments`. They are NEVER summed.

      3. **Provenance on every entry.** Each entry records whether the
         energy was measured or estimated, the source, and the
         methodology version.

      4. **Corrections without destruction.** A correction is recorded
         as a new entry with a `corrects_entry_id` field. The original
         entry is never modified.

      5. **Persistence.** The ledger is written to disk on every
         `record()` so entries survive process restarts.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the ledger.

        Args:
            config: Full Green Agent config. The ledger uses
                `config['governance']['ledger_file']` for persistence
                and `config['governance']['schema_version']` for the
                schema version. Both default sensibly.
        """
        self.config = config or {}
        governance_cfg = self.config.get("governance", {}) or {}

        # --- Public fields (preserved) ---
        self.entries: List[Dict[str, Any]] = []
        self.total_carbon: float = 0.0
        self.total_energy: float = 0.0

        # --- Persistence ---
        self._ledger_file = Path(
            governance_cfg.get("ledger_file", "data/carbon_ledger.json")
        )
        self._instruments_file = Path(
            governance_cfg.get(
                "instruments_file", "data/carbon_instruments.json"
            )
        )
        self._schema_version = governance_cfg.get(
            "schema_version", LEDGER_SCHEMA_VERSION
        )

        # --- Instruments ledger (contractual side) ---
        self._instruments: List[Dict[str, Any]] = []

        # --- Corrections index: original_entry_id -> [correction_ids] ---
        self._corrections: Dict[str, List[str]] = {}

        # --- Chain state ---
        self._last_chain_hash: str = "genesis"
        self._entry_counter: int = 0

        # --- Thread safety ---
        self._lock = threading.RLock()

        # --- Statistics ---
        self._correction_count: int = 0
        self._simulated_entry_count: int = 0
        self._measured_entry_count: int = 0
        self._estimated_entry_count: int = 0

        logger.info(
            f"CarbonLedger initialized (ledger_file={self._ledger_file}, "
            f"schema_version={self._schema_version})"
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """
        Load existing entries from disk if the ledger file exists.

        The original implementation was a stub. This version reads the
        full entries list and re-verifies the hash chain before
        accepting the loaded state.
        """
        if self._ledger_file.exists():
            try:
                with open(self._ledger_file, "r") as f:
                    payload = json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                logger.error(
                    f"Failed to load ledger from {self._ledger_file}: {e}"
                )
                payload = {}
            else:
                self.entries = list(payload.get("entries", []) or [])
                self.total_carbon = float(
                    payload.get("total_carbon", 0.0) or 0.0
                )
                self.total_energy = float(
                    payload.get("total_energy", 0.0) or 0.0
                )
                self._last_chain_hash = payload.get(
                    "last_chain_hash", "genesis"
                )
                self._entry_counter = int(
                    payload.get("entry_counter", len(self.entries)) or 0
                )

                if not self.verify_chain():
                    logger.error(
                        "Loaded ledger failed hash chain verification — "
                        "the file may have been tampered with"
                    )

        # --- Load instruments ledger ---
        if self._instruments_file.exists():
            try:
                with open(self._instruments_file, "r") as f:
                    self._instruments = list(json.load(f) or [])
            except (OSError, json.JSONDecodeError) as e:
                logger.warning(
                    f"Failed to load instruments ledger: {e}"
                )

        logger.info(
            f"CarbonLedger initialized — loaded {len(self.entries)} "
            f"entries, {len(self._instruments)} instruments"
        )

    async def shutdown(self) -> None:
        """
        Persist the ledger to disk and log a summary.

        The original implementation was a stub. This version writes
        the full state atomically.
        """
        self._persist()
        logger.info(
            f"CarbonLedger shutdown - Total: {self.total_carbon:.4f} kg CO2"
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

        Args:
            result: A `UnifiedResult` (or duck-typed object) with
                `task_id`, `energy_consumed`, `carbon_emitted`,
                `negawatt_reward`.
            decision: An `ExecutionDecision` (or duck-typed object)
                with `carbon_zone`, `action`, `power_budget`.
            source: `"measured"`, `"estimated"`, or `"simulated"`.
            methodology_version: The methodology used to compute carbon.
            baseline: The comparison baseline (e.g. "FP32-2025Q4").
            simulated: Override the simulated flag if the source
                disagrees.
            region: Grid region (for provenance).
            precision: Model precision (for provenance).
            extra: Optional extra fields to attach.

        Returns:
            The full hash of the newly appended entry, for verification.

        The entry is appended to the in-memory list, committed to the
        hash chain, and persisted to disk in the same call — so entries
        survive process restarts without waiting for shutdown().
        """
        with self._lock:
            entry = self._build_entry(
                result=result,
                decision=decision,
                source=source,
                methodology_version=(
                    methodology_version or self._schema_version
                ),
                baseline=baseline,
                simulated=simulated,
                region=region,
                precision=precision,
                extra=extra,
            )

            # --- Compute hash chain ---
            entry["prev_hash"] = self._last_chain_hash
            entry["hash"] = self._calculate_hash(entry)
            self._last_chain_hash = entry["hash"]

            # --- Append ---
            self.entries.append(entry)
            self._entry_counter += 1

            # --- Update totals ---
            carbon_kg = float(entry.get("carbon_operational_kg", 0.0))
            energy_kwh = float(entry.get("energy_kwh", 0.0))
            self.total_carbon += carbon_kg
            self.total_energy += energy_kwh

            # --- Update source counters ---
            if entry.get("simulated"):
                self._simulated_entry_count += 1
            if source == "measured":
                self._measured_entry_count += 1
            elif source == "estimated":
                self._estimated_entry_count += 1

            # --- Persist immediately ---
            self._persist()

            logger.info(
                f"Carbon ledger entry {entry['entry_id']}: "
                f"{carbon_kg:.4f} kg CO2 "
                f"(total: {self.total_carbon:.4f})"
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
        # --- Duck-typed extraction of result fields ---
        task_id = _get_attr(result, "task_id", "unknown")
        energy_kwh = _get_attr(result, "energy_consumed", 0.0)
        carbon_kg = _get_attr(result, "carbon_emitted", 0.0)
        negawatt_reward = _get_attr(result, "negawatt_reward", 0.0)

        # --- Duck-typed extraction of decision fields ---
        carbon_zone = _get_attr(decision, "carbon_zone", "unknown")
        action = _get_attr(decision, "action", "unknown")
        power_budget = _get_attr(decision, "power_budget", None)

        # --- Separated operational vs. contractual carbon ---
        carbon_operational = _get_attr(
            result, "carbon_operational_kg", None,
        )
        carbon_contractual = _get_attr(
            result, "carbon_contractual_kg", None,
        )
        # Fall back to the combined field if separated values are absent
        if carbon_operational is None:
            carbon_operational = carbon_kg
        if carbon_contractual is None:
            carbon_contractual = 0.0

        # --- Simulated flag ---
        if simulated is None:
            simulated = bool(_get_attr(result, "simulated", False))
            if source == "simulated":
                simulated = True

        # --- Entry ID ---
        entry_id = uuid.uuid4().hex[:12]

        entry: Dict[str, Any] = {
            # --- Identity ---
            "entry_id": entry_id,
            "schema_version": self._schema_version,

            # --- Timing ---
            "timestamp": datetime.now(timezone.utc).isoformat(),

            # --- Task ---
            "task_id": task_id,
            "run_id": _get_attr(result, "run_id", None),

            # --- Energy / carbon (operational, physical) ---
            "energy_kwh": float(energy_kwh),
            "carbon_operational_kg": float(carbon_operational),
            # Preserved for backward compatibility:
            "carbon_kg": float(carbon_kg),

            # --- Decision ---
            "carbon_zone": carbon_zone,
            "action": action,
            "negawatt_reward": float(negawatt_reward),
            "power_budget": power_budget,

            # --- Provenance ---
            "source": source,
            "methodology_version": methodology_version,
            "baseline": baseline,
            "simulated": bool(simulated),
            "region": region,
            "precision": precision,

            # --- Correction (filled by correct_entry) ---
            "corrects_entry_id": None,
            "correction_reason": None,
        }

        if extra:
            entry["extra"] = dict(extra)

        return entry

    def _calculate_hash(self, entry: Dict) -> str:
        """
        Calculate the **full** SHA-256 hash of an entry.

        Unlike the original implementation, this returns the complete
        64-character hex digest — not a truncated 16-character prefix.
        """
        data = {k: v for k, v in entry.items() if k != "hash"}
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

        The original entry is **never modified**. Instead, a new entry
        is appended that carries `corrects_entry_id` pointing to the
        original. Downstream reports that need the corrected value
        should follow the correction chain.

        Returns the hash of the correction entry, or None if the
        original entry was not found.
        """
        with self._lock:
            original = next(
                (e for e in self.entries if e.get("entry_id") == entry_id),
                None,
            )
            if original is None:
                logger.warning(
                    f"correct_entry: no entry with id {entry_id}"
                )
                return None

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
                    methodology_version or self._schema_version
                ),
                "baseline": original.get("baseline"),
                "simulated": False,
                "region": original.get("region"),
                "precision": original.get("precision"),
                "corrects_entry_id": entry_id,
                "correction_reason": reason,
            }
            correction["prev_hash"] = self._last_chain_hash
            correction["hash"] = self._calculate_hash(correction)
            self._last_chain_hash = correction["hash"]

            self.entries.append(correction)
            self._entry_counter += 1
            self._corrections.setdefault(entry_id, []).append(
                correction["entry_id"]
            )
            self._correction_count += 1

            # Adjust totals by the delta (corrected - original)
            original_kg = float(
                original.get("carbon_operational_kg", 0.0)
            )
            self.total_carbon += (corrected_carbon_kg - original_kg)

            self._persist()
            return correction["hash"]

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

        Instruments are kept in a **separate** list from operational
        emissions. They are never summed with `entries`.
        """
        with self._lock:
            if any(
                i.get("instrument_id") == instrument_id
                for i in self._instruments
            ):
                raise ValueError(
                    f"duplicate instrument_id: {instrument_id}"
                )
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
            self._persist_instruments()

    async def get_instruments_ledger(self) -> Dict[str, Any]:
        """Return the contractual instruments ledger."""
        with self._lock:
            contractual_kg = sum(
                i["quantity"] for i in self._instruments
                if i["kind"] == "offset"
                and i["unit"] == "kgCO2e"
                and i["retired"]
            )
            return {
                "instruments": [dict(i) for i in self._instruments],
                "count": len(self._instruments),
                "contractual_kgco2e": contractual_kg,
                "note": (
                    "Contractual instruments are tracked separately "
                    "from operational emissions. Never sum the two."
                ),
            }

    # ------------------------------------------------------------------
    # Verification
    # ------------------------------------------------------------------

    def verify_chain(self) -> bool:
        """
        Verify the full hash chain.

        Returns True iff every entry's `prev_hash` matches the previous
        entry's `hash` and every entry's `hash` matches its recomputed
        value.
        """
        with self._lock:
            prev = "genesis"
            for entry in self.entries:
                if entry.get("prev_hash") != prev:
                    logger.error(
                        f"Chain broken at entry {entry.get('entry_id')}: "
                        f"prev_hash={entry.get('prev_hash')} != {prev}"
                    )
                    return False
                expected = self._calculate_hash(entry)
                if entry.get("hash") != expected:
                    logger.error(
                        f"Hash mismatch at entry {entry.get('entry_id')}"
                    )
                    return False
                prev = entry["hash"]
        return True

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

        Backward-compatible: same signature, same core fields
        (`report_period`, `total_entries`, `total_energy_kwh`,
        `total_carbon_kg`, `total_negawatt`, `average_carbon_intensity`,
        `entries`).

        Enhanced: adds `total_carbon_operational_kg`,
        `total_carbon_contractual_kg`, `by_source`, `simulated_count`,
        `corrected_count`, and a `chain_verified` flag.
        """
        with self._lock:
            filtered = list(self.entries)

        if start_date:
            filtered = [
                e for e in filtered
                if _parse_iso(e.get("timestamp")) >= start_date
            ]
        if end_date:
            filtered = [
                e for e in filtered
                if _parse_iso(e.get("timestamp")) <= end_date
            ]

        # --- Totals ---
        total_energy = sum(
            float(e.get("energy_kwh", 0.0)) for e in filtered
        )
        total_carbon_operational = sum(
            float(e.get("carbon_operational_kg", 0.0)) for e in filtered
        )
        total_carbon_legacy = sum(
            float(e.get("carbon_kg", 0.0)) for e in filtered
        )
        total_negawatt = sum(
            float(e.get("negawatt_reward", 0.0)) for e in filtered
        )

        # --- By-source breakdown ---
        by_source: Dict[str, int] = {}
        for e in filtered:
            src = e.get("source", "unknown")
            by_source[src] = by_source.get(src, 0) + 1

        # --- Simulated and correction counts ---
        simulated_count = sum(
            1 for e in filtered if e.get("simulated")
        )
        corrected_count = sum(
            1 for e in filtered if e.get("corrects_entry_id")
        )

        return {
            "report_period": {
                "start": start_date.isoformat() if start_date else None,
                "end": end_date.isoformat() if end_date else None,
            },
            "schema_version": self._schema_version,
            "total_entries": len(filtered),
            "total_energy_kwh": total_energy,
            "total_carbon_kg": total_carbon_legacy,
            "total_carbon_operational_kg": total_carbon_operational,
            "total_carbon_contractual_kg": self._contractual_kg(),
            "total_negawatt": total_negawatt,
            "average_carbon_intensity": self._calculate_average_intensity(
                filtered
            ),
            "by_source": by_source,
            "simulated_count": simulated_count,
            "corrected_count": corrected_count,
            "chain_verified": self.verify_chain(),
            "entries": filtered[-100:],
        }

    def _calculate_average_intensity(
        self, entries: List[Dict],
    ) -> float:
        """
        Calculate average carbon intensity from entries.

        Backward-compatible: same computation on `energy_kwh` and
        `carbon_kg`.
        """
        if not entries:
            return 0.0
        total_energy = sum(
            float(e.get("energy_kwh", 0.0)) for e in entries
        )
        total_carbon = sum(
            float(e.get("carbon_kg", 0.0)) for e in entries
        )
        if total_energy == 0:
            return 0.0
        return (total_carbon / total_energy) * 1000  # gCO2/kWh

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative ledger statistics."""
        with self._lock:
            return {
                "schema_version": self._schema_version,
                "entry_count": len(self.entries),
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
            }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _persist(self) -> None:
        """
        Write the ledger to disk atomically.

        Uses a temp file + rename so a crash mid-write cannot corrupt
        the ledger.
        """
        with self._lock:
            payload = {
                "schema_version": self._schema_version,
                "entries": self.entries,
                "total_carbon": self.total_carbon,
                "total_energy": self.total_energy,
                "last_chain_hash": self._last_chain_hash,
                "entry_counter": self._entry_counter,
                "persisted_at": datetime.now(timezone.utc).isoformat(),
            }
        try:
            self._ledger_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._ledger_file.with_suffix(
                self._ledger_file.suffix + ".tmp"
            )
            with open(tmp, "w") as f:
                json.dump(payload, f, indent=2, default=str)
            os.replace(tmp, self._ledger_file)
        except OSError as e:
            logger.error(f"Failed to persist ledger: {e}")

    def _persist_instruments(self) -> None:
        with self._lock:
            snapshot = list(self._instruments)
        try:
            self._instruments_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._instruments_file, "w") as f:
                json.dump(snapshot, f, indent=2, default=str)
        except OSError as e:
            logger.error(f"Failed to persist instruments: {e}")

    def _contractual_kg(self) -> float:
        with self._lock:
            return sum(
                float(i["quantity"]) for i in self._instruments
                if i["kind"] == "offset"
                and i["unit"] == "kgCO2e"
                and i["retired"]
            )


# =============================================================================
# Helpers
# =============================================================================

def _get_attr(obj: Any, name: str, default: Any) -> Any:
    """Duck-typed attribute or key access with a default."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _parse_iso(value: Any) -> datetime:
    """Parse an ISO timestamp; return epoch on failure."""
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return datetime.min.replace(tzinfo=timezone.utc)
