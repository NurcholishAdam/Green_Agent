"""
Decoding strategies for quantum measurement outcomes.

Takes a ``{bitstring: count}`` histogram from a quantum backend (or a
simulated run of a ``TranslationResult``) and turns it into structured
``DecodingResult`` objects.

Bitstring convention
--------------------
A bitstring of length ``n`` is written most-significant-qubit-first, i.e.
it is the binary representation of the basis-state index ``i`` with the
qubit-0 bit on the right:
    "0…0101"  ==  index 5  ==  |q[n-1]=0 … q[2]=1 q[1]=0 q[0]=1⟩
This matches ``format(i, f"0{n}b")`` and is the convention used by
``counts_from_amplitudes()`` below.

Version: 2.0.0
"""

from __future__ import annotations

import logging
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
)

__all__ = [
    "VERSION",
    "StrategyName",
    "DecodingError",
    "InvalidBitstringError",
    "EmptyHistogramError",
    "DecodingResult",
    "DecodingStrategy",
    "SampleDecoding",
    "ArgmaxDecoding",
    "ExpectationDecoding",
    "ObservableDecoding",
    "get_strategy",
    "counts_from_amplitudes",
]

logger = logging.getLogger(__name__)

VERSION = "2.0.0"

StrategyName = Literal["sample", "argmax", "expectation", "observable"]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class DecodingError(ValueError):
    """Base error for all decoding failures."""


class InvalidBitstringError(DecodingError):
    """A bitstring is not composed solely of '0'/'1' or has the wrong length."""


class EmptyHistogramError(DecodingError):
    """The histogram is empty or has non-positive total weight."""


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DecodingResult:
    """
    A single decoded outcome.

    Strategies populate whichever fields make sense for them:

    - ``SampleDecoding``       → ``bitstring``, ``count``, ``probability``
    - ``ArgmaxDecoding``       → ``bitstring``, ``count``, ``probability``
    - ``ExpectationDecoding``  → ``expectation``, ``count`` (== n_shots)
    - ``ObservableDecoding``   → ``expectation``, ``count`` (== n_shots)

    ``extra`` carries strategy-specific metadata (e.g. the observable name,
    tie-break information) without changing the schema.
    """

    bitstring: Optional[str] = None
    count: int = 0
    probability: Optional[float] = None
    expectation: Optional[float] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """JSON-safe dict, omitting unset (None) fields."""
        out: Dict[str, Any] = {"count": self.count}
        if self.bitstring is not None:
            out["bitstring"] = self.bitstring
        if self.probability is not None:
            out["probability"] = self.probability
        if self.expectation is not None:
            out["expectation"] = self.expectation
        if self.extra:
            out["extra"] = dict(self.extra)
        return out


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_bitstring(bs: Any, problem_size: Optional[int]) -> int:
    """
    Validate ``bs`` and return its Hamming weight.

    Raises
    ------
    InvalidBitstringError
        - non-string or empty
        - contains characters other than '0' / '1'
        - length != problem_size (when problem_size is not None)
    """
    if not isinstance(bs, str) or not bs:
        raise InvalidBitstringError(
            f"bitstring must be a non-empty str, got {bs!r}"
        )
    for ch in bs:
        if ch not in "01":
            raise InvalidBitstringError(
                f"bitstring {bs!r} contains non-'01' character {ch!r}"
            )
    if problem_size is not None and len(bs) != problem_size:
        raise InvalidBitstringError(
            f"bitstring {bs!r} has length {len(bs)}, "
            f"expected problem_size={problem_size}"
        )
    return bs.count("1")


def _validate_counts(
    counts: Dict[str, int], problem_size: Optional[int]
) -> int:
    """
    Validate the histogram, return total shot count.

    Raises
    ------
    EmptyHistogramError if the histogram is empty or has non-positive total.
    InvalidBitstringError on any invalid key.
    DecodingError on non-integer / negative counts.
    """
    if not counts:
        raise EmptyHistogramError("histogram is empty")
    total = 0
    for bs, c in counts.items():
        _validate_bitstring(bs, problem_size)
        if not isinstance(c, int) or isinstance(c, bool):
            raise DecodingError(
                f"count for {bs!r} must be a non-bool int, got {type(c).__name__}"
            )
        if c < 0:
            raise DecodingError(f"count for {bs!r} is negative ({c})")
        total += c
    if total <= 0:
        raise EmptyHistogramError(
            "histogram has non-positive total count; cannot decode"
        )
    return total


# ---------------------------------------------------------------------------
# Base class + registry
# ---------------------------------------------------------------------------

_STRATEGIES: Dict[str, type["DecodingStrategy"]] = {}


class DecodingStrategy(ABC):
    """
    Base class for decoding strategies.

    Subclasses declare ``name: ClassVar[str]`` and are automatically
    registered. Use :func:`get_strategy` to instantiate by name.
    """

    name: ClassVar[str] = "base"

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls.name != "base":
            if cls.name in _STRATEGIES:
                raise DecodingError(
                    f"strategy name {cls.name!r} is already registered by "
                    f"{_STRATEGIES[cls.name].__name__}"
                )
            _STRATEGIES[cls.name] = cls

    @abstractmethod
    def decode(
        self,
        counts: Dict[str, int],
        *,
        problem_size: Optional[int] = None,
    ) -> List[DecodingResult]:
        """Decode a histogram into structured results."""
        ...


def get_strategy(name: StrategyName) -> DecodingStrategy:
    """
    Instantiate a registered strategy by name.

    Raises
    ------
    DecodingError
        - unknown strategy name
        - strategy requires constructor arguments (e.g. ``observable``)
    """
    try:
        cls = _STRATEGIES[name]
    except KeyError:
        raise DecodingError(
            f"unknown strategy {name!r}; known: {sorted(_STRATEGIES)}"
        ) from None
    try:
        return cls()
    except TypeError as exc:
        raise DecodingError(
            f"strategy {name!r} requires constructor arguments "
            f"({cls.__name__}); instantiate it directly"
        ) from exc


# ---------------------------------------------------------------------------
# Concrete strategies
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SampleDecoding(DecodingStrategy):
    """
    Return the full histogram (or the top-K) ranked by descending count.

    Ties are broken by descending bitstring (deterministic regardless of
    insertion order in the input dict).
    """

    name: ClassVar[str] = "sample"

    top_k: Optional[int] = None

    def __post_init__(self) -> None:
        if self.top_k is not None and self.top_k < 1:
            raise DecodingError(f"top_k must be >= 1 or None, got {self.top_k}")

    def decode(
        self,
        counts: Dict[str, int],
        *,
        problem_size: Optional[int] = None,
    ) -> List[DecodingResult]:
        total = _validate_counts(counts, problem_size)
        ranked = sorted(
            counts.items(),
            key=lambda kv: (-kv[1], kv[0]),  # count desc, then bitstring desc
        )
        if self.top_k is not None:
            ranked = ranked[: self.top_k]
        return [
            DecodingResult(
                bitstring=bs,
                count=c,
                probability=c / total,
            )
            for bs, c in ranked
        ]


@dataclass(frozen=True)
class ArgmaxDecoding(DecodingStrategy):
    """
    Return the single most-frequent bitstring.

    Ties are broken by the lexicographically larger bitstring for
    determinism.
    """

    name: ClassVar[str] = "argmax"

    def decode(
        self,
        counts: Dict[str, int],
        *,
        problem_size: Optional[int] = None,
    ) -> List[DecodingResult]:
        total = _validate_counts(counts, problem_size)
        best_bs, best_count = max(
            counts.items(),
            key=lambda kv: (kv[1], kv[0]),  # count desc, then bitstring desc
        )
        return [
            DecodingResult(
                bitstring=best_bs,
                count=best_count,
                probability=best_count / total,
            )
        ]


@dataclass(frozen=True)
class ExpectationDecoding(DecodingStrategy):
    """
    Expected Hamming weight (number of '1' bits) over the shot distribution.

    Returns a single result carrying ``expectation`` and ``count == n_shots``.
    Empty / zero-total histograms raise :class:`EmptyHistogramError` rather
    than fabricating a shot.
    """

    name: ClassVar[str] = "expectation"

    def decode(
        self,
        counts: Dict[str, int],
        *,
        problem_size: Optional[int] = None,
    ) -> List[DecodingResult]:
        total = _validate_counts(counts, problem_size)
        weighted = 0
        for bs, c in counts.items():
            # _validate_counts already checked the bitstring; recount here.
            weighted += bs.count("1") * c
        return [
            DecodingResult(
                expectation=weighted / total,
                count=total,
                extra={"observable": "hamming_weight"},
            )
        ]


@dataclass(frozen=True)
class ObservableDecoding(DecodingStrategy):
    """
    Expectation of a user-supplied diagonal observable.

    ``observable(bitstring) -> float`` maps each measured bitstring to an
    eigenvalue. For Pauli-Z observables on a subset of qubits this is
    typically a sum of ``±1`` contributions; for hardware-agnostic objectives
    it can be any real-valued scoring function.

    Not registered under :func:`get_strategy` because it requires the
    ``observable`` argument; instantiate it directly.
    """

    name: ClassVar[str] = "observable"

    observable: Callable[[str], float]
    label: Optional[str] = None

    def decode(
        self,
        counts: Dict[str, int],
        *,
        problem_size: Optional[int] = None,
    ) -> List[DecodingResult]:
        total = _validate_counts(counts, problem_size)
        weighted = 0.0
        for bs, c in counts.items():
            try:
                val = float(self.observable(bs))
            except Exception as exc:
                raise DecodingError(
                    f"observable raised on bitstring {bs!r}: {exc}"
                ) from exc
            weighted += val * c
        extra = {"observable": self.label or "custom"}
        return [
            DecodingResult(
                expectation=weighted / total,
                count=total,
                extra=extra,
            )
        ]


# Exclude ObservableDecoding from name-based construction — it needs args.
_STRATEGIES.pop("observable", None)


# ---------------------------------------------------------------------------
# Bridge from TranslationResult / raw amplitudes
# ---------------------------------------------------------------------------

def counts_from_amplitudes(
    amplitudes: Sequence[float],
    *,
    shots: int = 1024,
    seed: Optional[int] = None,
) -> Dict[str, int]:
    """
    Sample a bitstring histogram from an amplitude vector.

    Parameters
    ----------
    amplitudes:
        Length must be a power of two (``2**n_qubits``). Values are squared
        to form sampling probabilities; they need not be normalised.
    shots:
        Number of samples to draw.
    seed:
        Optional RNG seed for reproducibility.

    Returns
    -------
    Dict[str, int]
        ``{bitstring: count}`` in the MSB-first convention documented at
        the top of the module.

    Raises
    ------
    DecodingError
        If ``amplitudes`` has non-power-of-two length, contains non-finite
        values, or has zero total probability.
    """
    n_amps = len(amplitudes)
    if n_amps < 2 or (n_amps & (n_amps - 1)) != 0:
        raise DecodingError(
            f"amplitude vector length must be a power of two >= 2, got {n_amps}"
        )
    if shots < 0:
        raise DecodingError(f"shots must be non-negative, got {shots}")

    probs: List[float] = []
    for i, a in enumerate(amplitudes):
        pa = float(a) ** 2
        if pa != pa or pa in (float("inf"), float("-inf")):
            raise DecodingError(
                f"amplitude[{i}] produced non-finite probability: {a!r}"
            )
        probs.append(pa)

    total = sum(probs)
    if total <= 0.0:
        raise DecodingError("all amplitudes are zero; cannot sample")

    n_qubits = n_amps.bit_length() - 1
    bitstrings = [format(i, f"0{n_qubits}b") for i in range(n_amps)]

    rng = random.Random(seed)
    picks = rng.choices(bitstrings, weights=probs, k=shots)

    out: Dict[str, int] = {}
    for bs in picks:
        out[bs] = out.get(bs, 0) + 1
    return out


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    histogram = {"00": 5, "01": 10, "10": 10, "11": 3}

    print("=== SampleDecoding (top_k=2) ===")
    for r in SampleDecoding(top_k=2).decode(histogram, problem_size=2):
        print(" ", r.to_dict())

    print("=== ArgmaxDecoding ===")
    for r in ArgmaxDecoding().decode(histogram, problem_size=2):
        print(" ", r.to_dict())

    print("=== ExpectationDecoding (Hamming weight) ===")
    for r in ExpectationDecoding().decode(histogram, problem_size=2):
        print(" ", r.to_dict())

    print("=== ObservableDecoding: P(q0 == 1) ===")
    # In our MSB-first convention, rightmost char is qubit 0.
    obs = ObservableDecoding(
        observable=lambda bs: 1.0 if bs[-1] == "1" else 0.0,
        label="P(q0=1)",
    )
    for r in obs.decode(histogram, problem_size=2):
        print(" ", r.to_dict())

    print("=== Registry ===")
    for name in ("sample", "argmax", "expectation"):
        print(f"  {name}: {type(get_strategy(name)).__name__}")
    try:
        get_strategy("observable")  # type: ignore[arg-type]
    except DecodingError as e:
        print(f"  observable: {e}")

    print("=== counts_from_amplitudes ===")
    # 2-qubit Bell state |00> + |11> / sqrt(2)
    amps = [0.70710678, 0.0, 0.0, 0.70710678]
    sampled = counts_from_amplitudes(amps, shots=100, seed=42)
    print("  sampled:", sampled)
    print("  argmax:", ArgmaxDecoding().decode(sampled, problem_size=2)[0].to_dict())
