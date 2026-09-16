"""
Encoding strategies for quantum state preparation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


class EncodingStrategy:
    """Base class for encoding strategies."""

    name: str = "base"

    def encode(self, tokens: List[float], n_qubits: int) -> Dict[str, Any]:
        raise NotImplementedError

    def describe(self) -> Dict[str, Any]:
        return {"name": self.name, "n_qubits": 0}


@dataclass
class AmplitudeEncoding(EncodingStrategy):
    """Encode tokens as normalized amplitudes."""
    name: str = "amplitude"

    def encode(self, tokens: List[float], n_qubits: int) -> Dict[str, Any]:
        import math
        size = 2 ** n_qubits
        amps = [float(t) for t in tokens[:size]]
        while len(amps) < size:
            amps.append(0.0)
        norm = math.sqrt(sum(a * a for a in amps)) or 1.0
        return {
            "amplitudes": [a / norm for a in amps],
            "n_qubits": n_qubits,
        }

    def describe(self) -> Dict[str, Any]:
        return {"name": self.name, "type": "dense"}


@dataclass
class AngleEncoding(EncodingStrategy):
    """Encode tokens as rotation angles on individual qubits."""
    name: str = "angle"

    def encode(self, tokens: List[float], n_qubits: int) -> Dict[str, Any]:
        import math
        angles = [
            2.0 * math.atan(float(t)) % math.pi
            for t in tokens[:n_qubits]
        ]
        while len(angles) < n_qubits:
            angles.append(0.0)
        return {"phase_angles": angles, "n_qubits": n_qubits}

    def describe(self) -> Dict[str, Any]:
        return {"name": self.name, "type": "angle"}
