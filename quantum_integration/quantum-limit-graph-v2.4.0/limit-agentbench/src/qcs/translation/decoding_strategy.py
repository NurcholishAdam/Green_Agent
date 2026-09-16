"""
Decoding strategies for quantum measurement outcomes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence


class DecodingStrategy:
    """Base class for decoding strategies."""

    name: str = "base"

    def decode(
        self, counts: Dict[str, int], *,
        problem_size: int = 0,
    ) -> List[Any]:
        raise NotImplementedError


@dataclass
class SampleDecoding(DecodingStrategy):
    """Decode by returning the most-frequent bitstrings."""
    name: str = "sample"

    def decode(
        self, counts: Dict[str, int], *,
        problem_size: int = 0,
    ) -> List[Any]:
        ranked = sorted(
            counts.items(), key=lambda kv: kv[1], reverse=True,
        )
        return [
            {"bitstring": k, "count": v, "probability": v / max(1, sum(counts.values()))}
            for k, v in ranked
        ]


@dataclass
class ArgmaxDecoding(DecodingStrategy):
    """Decode by returning only the single most-frequent bitstring."""
    name: str = "argmax"

    def decode(
        self, counts: Dict[str, int], *,
        problem_size: int = 0,
    ) -> List[Any]:
        if not counts:
            return []
        best = max(counts.items(), key=lambda kv: kv[1])
        return [{"bitstring": best[0], "count": best[1]}]


@dataclass
class ExpectationDecoding(DecodingStrategy):
    """Decode by computing expectation values from measurement samples."""
    name: str = "expectation"

    def decode(
        self, counts: Dict[str, int], *,
        problem_size: int = 0,
    ) -> List[Any]:
        total = sum(counts.values()) or 1
        expectation = 0.0
        for bitstring, count in counts.items():
            value = sum(int(b) for b in bitstring if b in "01")
            expectation += value * (count / total)
        return [{"expectation": expectation, "n_shots": total}]
