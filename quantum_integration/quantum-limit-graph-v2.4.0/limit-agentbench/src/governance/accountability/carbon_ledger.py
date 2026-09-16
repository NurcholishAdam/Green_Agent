"""
CarbonLedger — immutable accounting of operational carbon.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class CarbonEntry:
    entry_id: str
    run_id: str
    task_id: Optional[str]
    operational_co2e_kg: float
    energy_kwh: float
    source: str                # "measured" | "estimated" | "simulated"
    methodology_version: str
    recorded_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    notes: Optional[str] = None


class CarbonLedger:
    """
    Append-only ledger of operational carbon.

    Never sums contractual instruments into this ledger — those live
    in CarbonInstrumentsLedger.
    """

    def __init__(self):
        self._entries: List[CarbonEntry] = []

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
    ) -> str:
        entry_id = uuid.uuid4().hex[:12]
        self._entries.append(CarbonEntry(
            entry_id=entry_id,
            run_id=run_id,
            task_id=task_id,
            operational_co2e_kg=float(operational_co2e_kg),
            energy_kwh=float(energy_kwh),
            source=source,
            methodology_version=methodology_version,
            notes=notes,
        ))
        return entry_id

    def total_operational_kg(self) -> float:
        return sum(e.operational_co2e_kg for e in self._entries)

    def entries(self) -> List[Dict[str, Any]]:
        return [asdict(e) for e in self._entries]
