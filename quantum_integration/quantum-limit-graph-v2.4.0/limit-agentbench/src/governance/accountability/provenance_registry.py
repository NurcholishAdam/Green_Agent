"""
ProvenanceRegistry — record where every metric came from.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class ProvenanceRecord:
    metric: str
    source_kind: str             # "measured" | "estimated" | "simulated" | ...
    source_name: Optional[str]
    trust_level: str
    recorded_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class ProvenanceRegistry:
    def __init__(self):
        self._records: List[ProvenanceRecord] = []

    def record(
        self,
        *,
        metric: str,
        source_kind: str,
        source_name: Optional[str] = None,
        trust_level: str = "low",
    ) -> None:
        self._records.append(ProvenanceRecord(
            metric=metric,
            source_kind=source_kind,
            source_name=source_name,
            trust_level=trust_level,
        ))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "records": [asdict(r) for r in self._records],
            "count": len(self._records),
        }
