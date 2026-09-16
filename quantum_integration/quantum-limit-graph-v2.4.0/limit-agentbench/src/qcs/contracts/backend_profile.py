"""
BackendProfile — capability description of a quantum backend.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional


class BackendKind(str, Enum):
    SIMULATOR = "simulator"
    HARDWARE = "hardware"
    EMULATOR = "emulator"
    CLOUD_SIMULATOR = "cloud_simulator"


class QubitTechnology(str, Enum):
    SUPERCONDUCTING = "superconducting"
    TRAPPED_ION = "trapped_ion"
    PHOTONIC = "photonic"
    NEUTRAL_ATOM = "neutral_atom"
    SIMULATED = "simulated"
    UNKNOWN = "unknown"


@dataclass
class BackendProfile:
    """
    Capabilities and constraints of a quantum backend.
    """
    backend_id: str
    kind: str
    technology: str = QubitTechnology.UNKNOWN.value
    num_qubits: int = 0
    max_shots_per_job: int = 100_000
    queue_depth: int = 0
    avg_queue_seconds: float = 0.0
    gate_fidelity: Optional[float] = None
    readout_fidelity: Optional[float] = None
    coherence_time_us: Optional[float] = None
    cost_per_shot: float = 0.0
    energy_kwh_per_shot: float = 0.0
    vendor: Optional[str] = None
    region: Optional[str] = None
    online: bool = True
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def can_handle(self, required_qubits: int, required_shots: int) -> bool:
        return (
            self.online
            and self.num_qubits >= required_qubits
            and self.max_shots_per_job >= required_shots
        )
