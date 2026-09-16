"""
Classical-to-quantum token translation.

Converts classical token streams into quantum-compatible representations.
"""

from __future__ import annotations

import hashlib
import logging
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


@dataclass
class TranslationResult:
    """Result of translating classical tokens."""
    amplitudes: List[float] = field(default_factory=list)
    phase_angles: List[float] = field(default_factory=list)
    n_qubits: int = 0
    encoding_name: str = "amplitude"
    metadata: Dict[str, Any] = field(default_factory=dict)
    translation_hash: str = ""


class ClassicalTokenTranslator:
    """
    Translate a sequence of classical tokens (integers or floats) into a
    normalized amplitude vector of size 2^n for quantum state preparation.

    The translator is deterministic: identical input yields identical
    output, with a SHA-256 hash of the translation included for
    reproducibility.
    """

    def __init__(self, *, max_qubits: int = 16, normalize: bool = True):
        if max_qubits < 1 or max_qubits > 30:
            raise ValueError("max_qubits must be between 1 and 30")
        self.max_qubits = max_qubits
        self.normalize = normalize

    def translate(
        self,
        tokens: Sequence[float],
        *,
        encoding: str = "amplitude",
    ) -> TranslationResult:
        """
        Translate a token sequence into a quantum state representation.

        Args:
            tokens: Classical numeric sequence.
            encoding: `"amplitude"` (default) or `"angle"`.

        Returns:
            A `TranslationResult` with amplitudes or phase angles.
        """
        if not tokens:
            return TranslationResult(
                amplitudes=[],
                n_qubits=0,
                encoding_name=encoding,
                translation_hash=_hash_tokens([]),
            )

        n_qubits = self._required_qubits(len(tokens))
        size = 2 ** n_qubits

        if encoding == "amplitude":
            amplitudes = self._amplitude_encode(tokens, size)
            result = TranslationResult(
                amplitudes=amplitudes,
                n_qubits=n_qubits,
                encoding_name="amplitude",
            )
        elif encoding == "angle":
            angles = self._angle_encode(tokens)
            result = TranslationResult(
                phase_angles=angles,
                n_qubits=len(angles),
                encoding_name="angle",
            )
        else:
            raise ValueError(f"unknown encoding '{encoding}'")

        result.translation_hash = _hash_tokens(list(tokens))
        result.metadata["original_length"] = len(tokens)
        return result

    def _required_qubits(self, n: int) -> int:
        bits = max(1, math.ceil(math.log2(max(2, n))))
        return min(bits, self.max_qubits)

    def _amplitude_encode(
        self, tokens: Sequence[float], size: int,
    ) -> List[float]:
        # Zero-pad to size, then normalize
        amplitudes = [float(t) for t in tokens[:size]]
        while len(amplitudes) < size:
            amplitudes.append(0.0)
        if self.normalize:
            norm = math.sqrt(sum(a * a for a in amplitudes))
            if norm > 0:
                amplitudes = [a / norm for a in amplitudes]
        return amplitudes

    def _angle_encode(self, tokens: Sequence[float]) -> List[float]:
        # Map each token to [0, π] via arctan
        return [
            2.0 * math.atan(float(t)) % math.pi
            for t in tokens[:self.max_qubits]
        ]


def _hash_tokens(tokens: List[float]) -> str:
    payload = ",".join(f"{t:.12g}" for t in tokens)
    return "sha256:" + hashlib.sha256(payload.encode()).hexdigest()
