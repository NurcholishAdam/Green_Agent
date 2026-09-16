"""
NoiseCharacterization — estimate backend noise and its impact.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional


@dataclass
class NoiseEstimate:
    """Estimated noise characteristics of a backend."""
    gate_error_rate: Optional[float] = None
    readout_error_rate: Optional[float] = None
    coherence_time_us: Optional[float] = None
    expected_fidelity: Optional[float] = None
    confidence: float = 0.5
    notes: list = None

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        if out["notes"] is None:
            out["notes"] = []
        return out


class NoiseCharacterization:
    """
    Estimate per-job fidelity from a backend profile.

    `expected_fidelity` is computed as the product of gate fidelity
    raised to the depth of the circuit, times readout fidelity per qubit.
    """

    def __init__(
        self,
        *,
        default_gate_fidelity: float = 0.99,
        default_readout_fidelity: float = 0.98,
    ):
        self.default_gate_fidelity = default_gate_fidelity
        self.default_readout_fidelity = default_readout_fidelity

    def estimate(
        self,
        *,
        backend_gate_fidelity: Optional[float],
        backend_readout_fidelity: Optional[float],
        circuit_depth: int,
        n_qubits: int,
        coherence_time_us: Optional[float] = None,
    ) -> NoiseEstimate:
        gate = backend_gate_fidelity or self.default_gate_fidelity
        readout = backend_readout_fidelity or self.default_readout_fidelity
        gate_fidelity = gate ** max(1, circuit_depth)
        readout_fidelity = readout ** max(1, n_qubits)
        expected = gate_fidelity * readout_fidelity

        notes = []
        if expected < 0.5:
            notes.append("expected fidelity below 50% — high noise")

        return NoiseEstimate(
            gate_error_rate=1.0 - gate,
            readout_error_rate=1.0 - readout,
            coherence_time_us=coherence_time_us,
            expected_fidelity=expected,
            confidence=0.7 if coherence_time_us else 0.5,
            notes=notes,
        )

    def exceeds_threshold(
        self, estimate: NoiseEstimate, *, min_fidelity: float,
    ) -> bool:
        return (estimate.expected_fidelity or 0.0) < min_fidelity
