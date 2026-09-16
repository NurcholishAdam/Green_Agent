"""
CarbonInstrumentsLedger — contractual instruments, kept separate from
operational emissions.

The recommendation is emphatic: certificates and RECs must never be
summed with physical emissions.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class CarbonInstrument:
    """A single contractual instrument."""
    instrument_id: str
    kind: str                  # "rec" | "offset" | "credit"
    quantity: float
    unit: str                  # "MWh" | "kgCO2e"
    vintage_year: Optional[int] = None
    matching_period: Optional[str] = None
    producer: Optional[str] = None
    region: Optional[str] = None
    retired: bool = False
    retired_at: Optional[datetime] = None
    provenance: Dict[str, str] = field(default_factory=dict)


class CarbonInstrumentsLedger:
    """
    Immutable ledger of contractual carbon instruments.

    Reports MUST display operational and contractual emissions
    separately. This ledger tracks only the contractual side.
    """

    def __init__(self):
        self._instruments: List[CarbonInstrument] = []

    def add(self, instrument: CarbonInstrument) -> None:
        if any(
            i.instrument_id == instrument.instrument_id
            for i in self._instruments
        ):
            raise ValueError(
                f"duplicate instrument_id: {instrument.instrument_id}"
            )
        self._instruments.append(instrument)

    def contractual_kgco2e(self) -> float:
        return sum(
            i.quantity for i in self._instruments
            if i.kind == "offset" and i.unit == "kgCO2e" and i.retired
        )

    def to_dict(self) -> Dict:
        return {
            "instruments": [asdict(i) for i in self._instruments],
            "contractual_kgco2e": self.contractual_kgco2e(),
            "count": len(self._instruments),
        }
