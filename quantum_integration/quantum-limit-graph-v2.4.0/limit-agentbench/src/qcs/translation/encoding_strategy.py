"""
Encoding strategies for quantum state preparation.

Bitstring / qubit ordering convention (shared with
``classical_to_quantum`` and ``decoding_strategies``): a bitstring of
length ``n`` is MSB-first — ``format(i, f"0{n}b")`` — so qubit 0 is the
rightmost character.

Version: 2.0.0
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, List, Literal, Optional, Sequence

__all__ = [
    "VERSION",
    "EncodingName",
    "EncodingError",
    "TokenError",
    "TruncationError",
    "MemoryBudgetError",
    "EncodingSpec",
    "EncodingResult",
    "EncodingStrategy",
    "AmplitudeEncoding",
    "AngleEncoding",
    "get_encoder",
]

logger = logging.getLogger(__name__)

VERSION = "2.0.0"

EncodingName = Literal["amplitude", "angle"]
_VALID_ENCODINGS = ("amplitude", "angle")

_MAX_QUBITS = 30
_PY_FLOAT_BYTES = 32                         # float object + list slot
_DEFAULT_MAX_STATE_BYTES = 256 * 1024 * 1024  # 256 MiB


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class EncodingError(ValueError):
    """Base error for all encoding failures."""


class TokenError(EncodingError):
    """Invalid token value (NaN/inf, bool, string, non-numeric)."""


class TruncationError(EncodingError):
    """Token stream exceeds the qubit capacity."""


class MemoryBudgetError(EncodingError):
    """Projected state vector exceeds the memory budget."""


# ---------------------------------------------------------------------------
# Token coercion
# ---------------------------------------------------------------------------

def _coerce_tokens(tokens: Sequence[Any]) -> List[float]:
    """
    Validate and canonicalise tokens.

    - Rejects ``bool``, ``str``, ``bytes``.
    - Rejects non-finite values (``NaN``, ``±inf``).
    - Canonicalises ``-0.0`` to ``0.0`` so hashes are stable.
    - Accepts any object convertible via ``float()``.
    """
    if isinstance(tokens, (str, bytes, bytearray)):
        raise TokenError(
            "tokens must be a numeric sequence, not a string/bytes"
        )
    out: List[float] = []
    for i, tok in enumerate(tokens):
        if isinstance(tok, (bool, str, bytes, bytearray)):
            raise TokenError(
                f"token[{i}] must be numeric, got {type(tok).__name__}"
            )
        try:
            val = float(tok)
        except (TypeError, ValueError) as exc:
            raise TokenError(
                f"token[{i}] is not numeric: {tok!r}"
            ) from exc
        if not math.isfinite(val):
            raise TokenError(f"token[{i}] is not finite: {tok!r}")
        out.append(0.0 if val == 0.0 else val)
    return out


# ---------------------------------------------------------------------------
# Hash
# ---------------------------------------------------------------------------

def _hash_state(
    tokens: List[float],
    *,
    encoding: EncodingName,
    n_qubits: int,
    truncated: bool = False,
    version: str = VERSION,
) -> str:
    """SHA-256 over everything that determines the resulting state."""
    payload = {
        "v": version,
        "encoding": encoding,
        "n_qubits": n_qubits,
        "truncated": truncated,
        "tokens": [repr(t) for t in tokens],
    }
    blob = json.dumps(
        payload, sort_keys=True, separators=(",", ":")
    ).encode()
    return "sha256:" + hashlib.sha256(blob).hexdigest()


# ---------------------------------------------------------------------------
# Spec (capability description)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EncodingSpec:
    """Static, class-level description of an encoder."""

    name: str
    kind: Literal["dense", "angle"]
    description: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "description": self.description,
        }


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EncodingResult:
    """
    Encoded quantum state plus metadata and reproducibility hash.

    Exactly one of ``amplitudes`` / ``phase_angles`` is populated, matching
    ``encoding_name``. Invariants:

    - ``encoding_name == "amplitude"`` → ``len(amplitudes) == 2**n_qubits``,
      ``phase_angles`` empty.
    - ``encoding_name == "angle"``     → ``len(phase_angles) == n_qubits``,
      ``amplitudes`` empty.

    ``metadata["encoded_length"]`` (number of tokens actually stored, post
    truncation) and ``metadata["input_length"]`` (number of tokens passed
    in) let :meth:`decode` recover the original stream without guessing.

    ``to_dict()`` returns the legacy ``{"amplitudes": …, "n_qubits": …}``
    shape for callers migrating from the old API.
    """

    amplitudes: List[float] = field(default_factory=list)
    phase_angles: List[float] = field(default_factory=list)
    n_qubits: int = 0
    encoding_name: EncodingName = "amplitude"
    metadata: Dict[str, Any] = field(default_factory=dict)
    state_hash: str = ""

    def __post_init__(self) -> None:
        if self.encoding_name == "amplitude":
            if self.phase_angles:
                raise EncodingError(
                    "amplitude result cannot carry phase_angles"
                )
            if self.amplitudes:
                expected = 1 << self.n_qubits
                if len(self.amplitudes) != expected:
                    raise EncodingError(
                        f"amplitude length {len(self.amplitudes)} != "
                        f"2**n_qubits ({expected})"
                    )
        elif self.encoding_name == "angle":
            if self.amplitudes:
                raise EncodingError(
                    "angle result cannot carry amplitudes"
                )
            if self.phase_angles and len(self.phase_angles) != self.n_qubits:
                raise EncodingError(
                    f"len(phase_angles) ({len(self.phase_angles)}) "
                    f"must equal n_qubits ({self.n_qubits})"
                )
        else:
            raise EncodingError(
                f"unknown encoding_name {self.encoding_name!r}"
            )

    # ---- state inspection --------------------------------------------

    @property
    def is_valid_state(self) -> bool:
        """True iff ``amplitudes`` forms an L2-normalised state vector."""
        if self.encoding_name != "amplitude" or not self.amplitudes:
            return False
        norm_sq = sum(a * a for a in self.amplitudes)
        return math.isclose(norm_sq, 1.0, rel_tol=1e-9, abs_tol=1e-12)

    # ---- decode -------------------------------------------------------

    def decode(self) -> List[float]:
        """
        Recover the original token stream.

        Returns ``metadata["encoded_length"]`` tokens (the ones that actually
        made it into the state; truncated tokens are not recovered).

        Raises
        ------
        EncodingError if the result lacks the metadata required for decoding.
        """
        if self.encoding_name == "amplitude":
            return self._decode_amplitude()
        return self._decode_angle()

    def _decode_amplitude(self) -> List[float]:
        meta = self.metadata
        if "encoded_length" not in meta or "original_norm" not in meta:
            raise EncodingError(
                "result lacks decode metadata (encoded_length / "
                "original_norm); was it produced by encode()?"
            )
        n = int(meta["encoded_length"])
        if n == 0:
            return []
        if meta.get("zero_norm_fallback"):
            return [0.0] * n
        norm = float(meta["original_norm"])
        normalized = bool(meta.get("normalized", True))
        if normalized:
            return [float(self.amplitudes[i]) * norm for i in range(n)]
        return [float(self.amplitudes[i]) for i in range(n)]

    def _decode_angle(self) -> List[float]:
        n = int(self.metadata.get("encoded_length", self.n_qubits))
        if n == 0:
            return []
        # theta = atan(t) + pi/2  =>  t = tan(theta - pi/2)
        return [
            math.tan(float(self.phase_angles[i]) - math.pi / 2.0)
            for i in range(n)
        ]

    # ---- legacy compatibility ----------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Return the pre-enhancement ``Dict[str, Any]`` shape."""
        out: Dict[str, Any] = {"n_qubits": self.n_qubits}
        if self.encoding_name == "amplitude":
            out["amplitudes"] = list(self.amplitudes)
        else:
            out["phase_angles"] = list(self.phase_angles)
        return out


# ---------------------------------------------------------------------------
# Base class + registry
# ---------------------------------------------------------------------------

_ENCODERS: Dict[str, type["EncodingStrategy"]] = {}


class EncodingStrategy(ABC):
    """
    Base class for encoding strategies.

    Subclasses declare a ``spec: ClassVar[EncodingSpec]`` and are
    automatically registered under ``spec.name``. Use :func:`get_encoder`
    to instantiate by name.

    Parameters
    ----------
    on_truncate:
        ``"error"`` (default) / ``"warn"`` / ``"truncate"`` — what to do
        when ``len(tokens)`` exceeds capacity (``2**n_qubits`` for
        amplitude, ``n_qubits`` for angle).
    on_zero_norm:
        ``"zero_state"`` (default — emit ``|0…0⟩``) / ``"error"``.
    max_state_bytes:
        Memory budget for the state vector (default 256 MiB).
    """

    spec: ClassVar[EncodingSpec]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        spec = getattr(cls, "spec", None)
        if spec is not None and spec.name != "base":
            if spec.name in _ENCODERS:
                raise EncodingError(
                    f"encoder name {spec.name!r} is already registered "
                    f"by {_ENCODERS[spec.name].__name__}"
                )
            _ENCODERS[spec.name] = cls

    def __init__(
        self,
        *,
        on_truncate: Literal["error", "warn", "truncate"] = "error",
        on_zero_norm: Literal["zero_state", "error"] = "zero_state",
        max_state_bytes: int = _DEFAULT_MAX_STATE_BYTES,
    ) -> None:
        if on_truncate not in ("error", "warn", "truncate"):
            raise ValueError(
                f"unknown on_truncate policy {on_truncate!r}"
            )
        if on_zero_norm not in ("zero_state", "error"):
            raise ValueError(
                f"unknown on_zero_norm policy {on_zero_norm!r}"
            )
        if max_state_bytes < _PY_FLOAT_BYTES:
            raise ValueError("max_state_bytes is too small")
        self.on_truncate = on_truncate
        self.on_zero_norm = on_zero_norm
        self.max_state_bytes = max_state_bytes

    # ---- public API --------------------------------------------------

    @abstractmethod
    def encode(
        self,
        tokens: Sequence[Any],
        n_qubits: int,
    ) -> EncodingResult:
        """Encode ``tokens`` into an :class:`EncodingResult`."""
        ...

    def describe(self) -> Dict[str, Any]:
        """Return the strategy's static description."""
        return self.spec.as_dict()

    # ---- shared helpers ----------------------------------------------

    def _validate_n_qubits(self, n: int) -> int:
        if isinstance(n, bool) or not isinstance(n, int):
            raise EncodingError(
                f"n_qubits must be an int, got {type(n).__name__}"
            )
        if not 1 <= n <= _MAX_QUBITS:
            raise EncodingError(
                f"n_qubits must be between 1 and {_MAX_QUBITS}, got {n}"
            )
        return n

    def _handle_truncation(
        self, n_tokens: int, capacity: int, encoding: str
    ) -> None:
        msg = (
            f"{encoding} encoding: token stream of length {n_tokens} "
            f"exceeds capacity {capacity}"
        )
        if self.on_truncate == "error":
            raise TruncationError(msg)
        if self.on_truncate == "warn":
            logger.warning("%s — truncating", msg)
        else:
            logger.debug("%s — truncating", msg)

    def _check_budget(self, size: int) -> None:
        projected = size * _PY_FLOAT_BYTES
        if projected > self.max_state_bytes:
            raise MemoryBudgetError(
                f"state vector of {size} amplitudes "
                f"(~{projected / 2**20:.1f} MiB) exceeds budget of "
                f"{self.max_state_bytes / 2**20:.1f} MiB"
            )


# ---------------------------------------------------------------------------
# Amplitude encoding
# ---------------------------------------------------------------------------

class AmplitudeEncoding(EncodingStrategy):
    """
    Encode tokens as L2-normalised amplitudes padded to ``2**n_qubits``.

    Padding uses ``0.0`` (no amplitude). A zero-norm input produces the
    ``|0…0⟩`` basis state by default, or raises with
    ``on_zero_norm="error"``.
    """

    spec: ClassVar[EncodingSpec] = EncodingSpec(
        name="amplitude",
        kind="dense",
        description=(
            "Tokens become L2-normalised amplitudes padded to 2**n_qubits."
        ),
    )

    def encode(
        self,
        tokens: Sequence[Any],
        n_qubits: int,
    ) -> EncodingResult:
        n_qubits = self._validate_n_qubits(n_qubits)
        coerced_full = _coerce_tokens(tokens)
        input_length = len(coerced_full)

        size = 1 << n_qubits
        truncated = input_length > size
        if truncated:
            self._handle_truncation(input_length, size, "amplitude")
        coerced = coerced_full[:size]
        encoded_length = len(coerced)

        self._check_budget(size)

        amps = list(coerced)
        if len(amps) < size:
            amps.extend([0.0] * (size - len(amps)))

        norm = math.sqrt(sum(a * a for a in amps))
        zero_norm = (norm == 0.0)
        if zero_norm:
            if self.on_zero_norm == "error":
                raise EncodingError(
                    "token stream has zero norm; not a valid state"
                )
            amps = [0.0] * size
            amps[0] = 1.0   # documented convention: |0…0⟩
            norm = 1.0
        else:
            amps = [a / norm for a in amps]

        return EncodingResult(
            amplitudes=amps,
            n_qubits=n_qubits,
            encoding_name="amplitude",
            metadata={
                "input_length": input_length,
                "encoded_length": encoded_length,
                "truncated": truncated,
                "original_norm": norm,
                "normalized": True,
                "zero_norm_fallback": zero_norm,
            },
            state_hash=_hash_state(
                coerced_full,
                encoding="amplitude",
                n_qubits=n_qubits,
                truncated=truncated,
            ),
        )


# ---------------------------------------------------------------------------
# Angle encoding
# ---------------------------------------------------------------------------

class AngleEncoding(EncodingStrategy):
    """
    Encode tokens as RY rotation angles in ``(0, π)``.

    Mapping: ``theta = atan(t) + π/2`` (invertible: ``t = tan(theta - π/2)``).
    Unused qubits are padded with ``angle = 0.0`` (identity rotation);
    :meth:`EncodingResult.decode` skips them via ``metadata["encoded_length"]``.
    """

    spec: ClassVar[EncodingSpec] = EncodingSpec(
        name="angle",
        kind="angle",
        description=(
            "Tokens become RY rotation angles via atan(t) + π/2; "
            "unused qubits are padded with identity rotations."
        ),
    )

    def encode(
        self,
        tokens: Sequence[Any],
        n_qubits: int,
    ) -> EncodingResult:
        n_qubits = self._validate_n_qubits(n_qubits)
        coerced_full = _coerce_tokens(tokens)
        input_length = len(coerced_full)

        truncated = input_length > n_qubits
        if truncated:
            self._handle_truncation(input_length, n_qubits, "angle")
        coerced = coerced_full[:n_qubits]
        encoded_length = len(coerced)

        # theta = atan(t) + pi/2, strictly in (0, pi) for finite t
        angles = [math.atan(t) + math.pi / 2.0 for t in coerced]
        if len(angles) < n_qubits:
            angles.extend([0.0] * (n_qubits - len(angles)))

        return EncodingResult(
            phase_angles=angles,
            n_qubits=n_qubits,
            encoding_name="angle",
            metadata={
                "input_length": input_length,
                "encoded_length": encoded_length,
                "truncated": truncated,
                "padding_angle": 0.0,
            },
            state_hash=_hash_state(
                coerced_full,
                encoding="angle",
                n_qubits=n_qubits,
                truncated=truncated,
            ),
        )


# ---------------------------------------------------------------------------
# Registry accessor
# ---------------------------------------------------------------------------

def get_encoder(
    name: EncodingName,
    **kwargs: Any,
) -> EncodingStrategy:
    """
    Instantiate a registered encoder by name.

    Parameters
    ----------
    name:
        ``"amplitude"`` or ``"angle"``.
    **kwargs:
        Forwarded to the encoder's ``__init__`` (``on_truncate``,
        ``on_zero_norm``, ``max_state_bytes``).

    Raises
    ------
    EncodingError if the name is unknown.
    """
    try:
        cls = _ENCODERS[name]
    except KeyError:
        raise EncodingError(
            f"unknown encoder {name!r}; known: {sorted(_ENCODERS)}"
        ) from None
    return cls(**kwargs)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Amplitude ------------------------------------------------------
    amp = AmplitudeEncoding()
    r = amp.encode([1.0, 2.0, 3.0], n_qubits=2)
    print("=== AmplitudeEncoding ===")
    print("  describe:", amp.describe())
    print("  n_qubits:", r.n_qubits, "| valid:", r.is_valid_state)
    print("  amplitudes:", [round(a, 4) for a in r.amplitudes])
    print("  round-trip:", r.decode())
    print("  hash:", r.state_hash[:23], "…")

    # --- Angle ----------------------------------------------------------
    ang = AngleEncoding()
    r2 = ang.encode([1.0, -2.0, 3.0], n_qubits=5)
    print("\n=== AngleEncoding ===")
    print("  describe:", ang.describe())
    print("  n_qubits:", r2.n_qubits, "| encoded:", r2.metadata["encoded_length"])
    print("  angles:", [round(a, 4) for a in r2.phase_angles])
    print("  round-trip:", [round(x, 4) for x in r2.decode()])
    print("  hash:", r2.state_hash[:23], "…")

    # --- Registry -------------------------------------------------------
    print("\n=== Registry ===")
    for name in ("amplitude", "angle"):
        print(f"  {name}: {type(get_encoder(name)).__name__}")

    # --- Edge cases -----------------------------------------------------
    print("\n=== Edge cases ===")
    z = amp.encode([0.0, 0.0], n_qubits=1)
    print("  zero-norm:", z.amplitudes, "| flag:",
          z.metadata["zero_norm_fallback"])

    try:
        amp.encode([1.0] * 10, n_qubits=2)
    except TruncationError as e:
        print("  truncation:", e)

    try:
        ang.encode([1.0], n_qubits=0)
    except EncodingError as e:
        print("  n_qubits:", e)

    try:
        amp.encode([float("nan")], n_qubits=1)
    except TokenError as e:
        print("  token:", e)

    # --- Legacy dict compat --------------------------------------------
    print("\n=== Legacy to_dict() ===")
    print("  ", r.to_dict())
