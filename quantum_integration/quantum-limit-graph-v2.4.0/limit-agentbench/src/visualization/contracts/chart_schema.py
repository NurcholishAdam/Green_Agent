"""
Chart schema — the data-source declaration every visual must carry.

Implements the recommendation's requirement:
    "Every visual should declare its data source, time range, units,
     baseline, system/policy version, and whether a value is measured,
     estimated, simulated, or externally supplied."
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class DataSource(Enum):
    """Where a visual's data came from."""
    MEASURED = "measured"
    ESTIMATED = "estimated"
    SIMULATED = "simulated"
    FEDERATED = "federated"
    EXTERNAL = "external"
    UNKNOWN = "unknown"


class TrustLevel(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class TradeoffKind(Enum):
    """The trade-off pairs the folder knows how to render."""
    ACCURACY_ENERGY = "accuracy_energy"
    ACCURACY_CARBON = "accuracy_carbon"
    LATENCY_ENERGY = "latency_energy"
    CARBON_ENERGY = "carbon_energy"
    QUALITY_LATENCY = "quality_latency"
    PRECISION_ENERGY = "precision_energy"
    QUANTUM_CLASSICAL = "quantum_classical"


@dataclass
class ChartContext:
    """
    The provenance banner every visual must attach to itself.

    Rendered as a subtitle or footer so a reader cannot mistake a
    simulated plot for a measured one.
    """
    source: str = DataSource.UNKNOWN.value
    trust_level: str = TrustLevel.LOW.value
    system_version: Optional[str] = None
    policy_version: Optional[str] = None
    run_id: Optional[str] = None
    time_range_start: Optional[datetime] = None
    time_range_end: Optional[datetime] = None
    units: Dict[str, str] = field(default_factory=dict)
    baseline: Optional[str] = None
    simulated: bool = False
    notes: Optional[str] = None

    def to_subtitle(self) -> str:
        """One-line summary for the plot's subtitle."""
        parts: List[str] = []
        if self.system_version:
            parts.append(f"sys={self.system_version}")
        if self.policy_version:
            parts.append(f"policy={self.policy_version}")
        parts.append(f"source={self.source}")
        if self.simulated:
            parts.append("⚠ simulated")
        if self.baseline:
            parts.append(f"baseline={self.baseline}")
        return " · ".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        if self.time_range_start:
            out["time_range_start"] = self.time_range_start.isoformat()
        if self.time_range_end:
            out["time_range_end"] = self.time_range_end.isoformat()
        return out
