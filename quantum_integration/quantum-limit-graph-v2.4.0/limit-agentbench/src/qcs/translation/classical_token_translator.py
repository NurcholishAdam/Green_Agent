"""
Classical-to-quantum token translation — module + test suite in one file.

Usage
-----
    from classical_to_quantum import ClassicalTokenTranslator

    t = ClassicalTokenTranslator()
    r = t.translate([1.0, -2.0, 3.0])
    r.is_valid_state         # True
    t.decode(r)              # [1.0, -2.0, 3.0]
    r.to_qasm()              # OpenQASM 2.0 string

Test suite (requires pytest + hypothesis):
    pip install pytest hypothesis
    pytest -v classical_to_quantum.py

Version: 3.1.0
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Sequence

__all__ = [
    "VERSION",
    "Encoding",
    "TranslationError",
    "EncodingError",
    "TokenError",
    "TruncationError",
    "MemoryBudgetError",
    "TranslationResult",
    "ClassicalTokenTranslator",
]

logger = logging.getLogger(__name__)

VERSION = "3.1.0"

Encoding = Literal["amplitude", "angle"]
_VALID_ENCODINGS = ("amplitude", "angle")

# ---------------------------------------------------------------------------
# Optional NumPy acceleration
# ---------------------------------------------------------------------------

try:  # pragma: no cover
    import numpy as _np  # type: ignore
    _HAS_NUMPY = True
except ImportError:  # pragma: no cover
    _np = None  # type: ignore
    _HAS_NUMPY = False

_NUMPY_THRESHOLD = 12       # n_qubits strictly above this uses NumPy
_PY_FLOAT_BYTES = 32        # float object + list slot
_NP_FLOAT_BYTES = 8         # float64
_DEFAULT_MAX_STATE_BYTES = 256 * 1024 * 1024  # 256 MiB


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class TranslationError(ValueError):
    """Base error for all translation failures."""


class EncodingError(TranslationError):
    """Unknown or unsupported encoding name."""


class TokenError(TranslationError):
    """Invalid token value (NaN/inf, bool, string, non-numeric)."""


class TruncationError(TranslationError):
    """Token stream exceeds the configured qubit budget."""


class MemoryBudgetError(TranslationError):
    """The projected state vector exceeds the configured memory budget."""


# ---------------------------------------------------------------------------
# Token coercion
# ---------------------------------------------------------------------------

def _coerce_tokens(tokens: Sequence[Any]) -> List[float]:
    """
    Validate and canonicalise a token sequence.

    - Rejects ``bool``, ``str``, ``bytes`` (common silent bugs).
    - Rejects non-finite values (``NaN``, ``±inf``).
    - Canonicalises ``-0.0`` to ``0.0`` so hashes are stable.
    - Accepts any object convertible via ``float()``.
    """
    if isinstance(tokens, (str, bytes, bytearray)):
        raise TokenError(
            "tokens must be a sequence of numbers, not a string/bytes"
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
            raise TokenError(f"token[{i}] is not numeric: {tok!r}") from exc
        if not math.isfinite(val):
            raise TokenError(f"token[{i}] is not finite: {tok!r}")
        if val == 0.0:
            val = 0.0  # canonicalise -0.0
        out.append(val)
    return out


# ---------------------------------------------------------------------------
# Hash
# ---------------------------------------------------------------------------

def _hash_translation(
    tokens: List[float],
    *,
    encoding: Encoding,
    normalize: bool,
    max_qubits: int,
    truncation_policy: str,
    version: str = VERSION,
) -> str:
    """SHA-256 over everything that determines the resulting state."""
    payload = {
        "v": version,
        "encoding": encoding,
        "normalize": normalize,
        "max_qubits": max_qubits,
        "truncation_policy": truncation_policy,
        "tokens": [repr(t) for t in tokens],
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(blob).hexdigest()


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class TranslationResult:
    """
    Result of translating classical tokens.

    ``amplitudes`` may be ``list[float]`` (pure-Python path) or
    ``numpy.ndarray`` (accelerated path, ``n_qubits > 12``).

    Invariants (enforced in ``__post_init__``):

    - ``encoding_name == "amplitude"`` → ``len(amplitudes) == 2**n_qubits``,
      ``phase_angles`` must be empty.
    - ``encoding_name == "angle"``     → ``len(phase_angles) == n_qubits``,
      ``amplitudes`` must be empty.
    """

    amplitudes: List[float] = field(default_factory=list)
    phase_angles: List[float] = field(default_factory=list)
    n_qubits: int = 0
    encoding_name: Encoding = "amplitude"
    metadata: Dict[str, Any] = field(default_factory=dict)
    translation_hash: str = ""

    def __post_init__(self) -> None:
        if self.encoding_name == "amplitude":
            if self.phase_angles:
                raise TranslationError(
                    "amplitude result cannot carry phase_angles"
                )
            if self.amplitudes:
                expected = 1 << self.n_qubits
                if len(self.amplitudes) != expected:
                    raise TranslationError(
                        f"amplitude length {len(self.amplitudes)} != "
                        f"2**n_qubits ({expected})"
                    )
        elif self.encoding_name == "angle":
            if self.amplitudes:
                raise TranslationError(
                    "angle result cannot carry amplitudes"
                )
            if self.phase_angles and len(self.phase_angles) != self.n_qubits:
                raise TranslationError(
                    f"n_qubits ({self.n_qubits}) must equal "
                    f"len(phase_angles) ({len(self.phase_angles)})"
                )
        else:
            raise TranslationError(
                f"unknown encoding_name {self.encoding_name!r}"
            )

    # ---- state inspection --------------------------------------------

    @property
    def is_valid_state(self) -> bool:
        """True iff ``amplitudes`` is an L2-normalised state vector."""
        if self.encoding_name != "amplitude" or not self.amplitudes:
            return False
        if _HAS_NUMPY and isinstance(self.amplitudes, _np.ndarray):
            norm_sq = float(_np.dot(self.amplitudes, self.amplitudes))
        else:
            norm_sq = sum(a * a for a in self.amplitudes)
        return math.isclose(norm_sq, 1.0, rel_tol=1e-9, abs_tol=1e-12)

    # ---- exporters ---------------------------------------------------

    def to_qasm(self) -> str:
        """OpenQASM 2.0 program preparing this state."""
        if self.encoding_name == "amplitude":
            return self._to_qasm_amplitude()
        return self._to_qasm_angle()

    def _to_qasm_amplitude(self) -> str:
        amp_str = ", ".join(repr(float(a)) for a in self.amplitudes)
        return (
            'OPENQASM 2.0;\n'
            'include "qelib1.inc";\n'
            f'// ClassicalTokenTranslator v{VERSION} - amplitude encoding\n'
            f'// translation_hash: {self.translation_hash}\n'
            f'qreg q[{self.n_qubits}];\n'
            f'initialize({amp_str}) q;\n'
        )

    def _to_qasm_angle(self) -> str:
        lines = [
            'OPENQASM 2.0;',
            'include "qelib1.inc";',
            f'// ClassicalTokenTranslator v{VERSION} - angle encoding',
            f'// translation_hash: {self.translation_hash}',
            f'qreg q[{self.n_qubits}];',
        ]
        for i, angle in enumerate(self.phase_angles):
            lines.append(f'ry({repr(float(angle))}) q[{i}];')
        return "\n".join(lines) + "\n"

    def to_qiskit(self) -> Any:
        """Return a ``qiskit.QuantumCircuit`` preparing this state."""
        try:
            from qiskit import QuantumCircuit  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "qiskit is required for TranslationResult.to_qiskit(); "
                "install with `pip install qiskit`"
            ) from exc

        qc = QuantumCircuit(self.n_qubits)
        if self.encoding_name == "amplitude":
            qc.initialize(list(self.amplitudes), list(range(self.n_qubits)))
        else:
            for i, angle in enumerate(self.phase_angles):
                qc.ry(float(angle), i)
        return qc

    def to_braket(self) -> Any:
        """
        Return an ``amazon_braket_sdk`` ``Circuit`` for angle-encoded states.

        Amplitude-encoded states raise ``NotImplementedError`` — Braket's
        gate model has no native arbitrary-amplitude init.
        """
        try:
            from braket.circuits import Circuit  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "amazon-braket-sdk is required for "
                "TranslationResult.to_braket(); install with "
                "`pip install amazon-braket-sdk`"
            ) from exc

        if self.encoding_name == "amplitude":
            raise NotImplementedError(
                "Braket's gate model does not support arbitrary amplitude "
                "initialization; use to_qiskit() or to_qasm() instead."
            )
        circuit = Circuit()
        for i, angle in enumerate(self.phase_angles):
            circuit.ry(i, float(angle))
        return circuit


# ---------------------------------------------------------------------------
# Translator
# ---------------------------------------------------------------------------

class ClassicalTokenTranslator:
    """
    Translate classical token streams to/from quantum representations.

    Modes
    -----
    - ``"amplitude"``: pads to ``2**n_qubits`` and L2-normalises when
      ``normalize=True``.
    - ``"angle"``: ``theta = atan(t) + pi/2`` — invertible and strictly in
      ``(0, pi)``.

    Parameters
    ----------
    max_qubits:
        Hard cap on qubit count. ``1..30``.
    normalize:
        L2-normalise amplitude vectors.
    on_truncate:
        ``"error"`` (default) / ``"warn"`` / ``"truncate"``.
    on_zero_norm:
        ``"zero_state"`` (default, emits ``|0…0⟩``) / ``"error"``.
    max_state_bytes:
        Memory budget for the state vector (default 256 MiB).
    """

    def __init__(
        self,
        *,
        max_qubits: int = 16,
        normalize: bool = True,
        on_truncate: Literal["error", "warn", "truncate"] = "error",
        on_zero_norm: Literal["zero_state", "error"] = "zero_state",
        max_state_bytes: int = _DEFAULT_MAX_STATE_BYTES,
    ) -> None:
        if not 1 <= max_qubits <= 30:
            raise ValueError("max_qubits must be between 1 and 30")
        if on_truncate not in ("error", "warn", "truncate"):
            raise ValueError(f"unknown on_truncate policy {on_truncate!r}")
        if on_zero_norm not in ("zero_state", "error"):
            raise ValueError(f"unknown on_zero_norm policy {on_zero_norm!r}")
        if max_state_bytes < _PY_FLOAT_BYTES:
            raise ValueError("max_state_bytes is too small")

        self.max_qubits = max_qubits
        self.normalize = normalize
        self.on_truncate = on_truncate
        self.on_zero_norm = on_zero_norm
        self.max_state_bytes = max_state_bytes

    # ---- encode ------------------------------------------------------

    def translate(
        self,
        tokens: Sequence[Any],
        *,
        encoding: Encoding = "amplitude",
    ) -> TranslationResult:
        if encoding not in _VALID_ENCODINGS:
            raise EncodingError(f"unknown encoding {encoding!r}")

        coerced = _coerce_tokens(tokens)
        if not coerced:
            return self._empty_result(encoding)

        if encoding == "amplitude":
            return self._translate_amplitude(coerced)
        return self._translate_angle(coerced)

    # ---- decode ------------------------------------------------------

    def decode(self, result: TranslationResult) -> List[float]:
        """Recover the original token stream from a ``TranslationResult``."""
        if result.encoding_name == "amplitude":
            return self._decode_amplitude(result)
        if result.encoding_name == "angle":
            return self._decode_angle(result)
        raise TranslationError(
            f"cannot decode result with encoding {result.encoding_name!r}"
        )

    def _decode_amplitude(self, result: TranslationResult) -> List[float]:
        meta = result.metadata
        if "original_length" not in meta or "original_norm" not in meta:
            raise TranslationError(
                "result lacks decode metadata (original_length / "
                "original_norm); was it produced by translate()?"
            )
        n = int(meta["original_length"])
        if n == 0:
            return []
        if meta.get("zero_norm_fallback"):
            return [0.0] * n

        norm = float(meta["original_norm"])
        normalized = bool(meta.get("normalized", True))
        out: List[float] = []
        for i in range(n):
            a = float(result.amplitudes[i])
            if normalized:
                a *= norm
            out.append(a)
        return out

    def _decode_angle(self, result: TranslationResult) -> List[float]:
        # theta = atan(t) + pi/2  =>  t = tan(theta - pi/2)
        return [
            math.tan(float(a) - math.pi / 2.0) for a in result.phase_angles
        ]

    # ---- internal ----------------------------------------------------

    def _empty_result(self, encoding: Encoding) -> TranslationResult:
        return TranslationResult(
            n_qubits=0,
            encoding_name=encoding,
            metadata={"original_length": 0, "truncated": False},
            translation_hash=self._hash([], encoding),
        )

    def _translate_amplitude(self, tokens: List[float]) -> TranslationResult:
        n_qubits = self._required_qubits(len(tokens))
        size = 1 << n_qubits
        truncated = len(tokens) > size
        if truncated:
            self._handle_truncation(len(tokens), size, "amplitude")

        amplitudes, norm, zero_norm = self._amplitude_encode(tokens, size)

        result = TranslationResult(
            amplitudes=amplitudes,
            n_qubits=n_qubits,
            encoding_name="amplitude",
            metadata={
                "original_length": len(tokens),
                "original_norm": norm,
                "truncated": truncated,
                "normalized": self.normalize,
                "zero_norm_fallback": zero_norm,
                "used_numpy": (
                    _HAS_NUMPY and isinstance(amplitudes, _np.ndarray)
                ),
            },
        )
        result.translation_hash = self._hash(tokens, "amplitude")
        return result

    def _translate_angle(self, tokens: List[float]) -> TranslationResult:
        capacity = self.max_qubits
        truncated = len(tokens) > capacity
        if truncated:
            self._handle_truncation(len(tokens), capacity, "angle")

        angles = self._angle_encode(tokens)
        result = TranslationResult(
            phase_angles=angles,
            n_qubits=len(angles),
            encoding_name="angle",
            metadata={
                "original_length": len(tokens),
                "truncated": truncated,
            },
        )
        result.translation_hash = self._hash(tokens, "angle")
        return result

    def _hash(self, tokens: List[float], encoding: Encoding) -> str:
        return _hash_translation(
            tokens,
            encoding=encoding,
            normalize=self.normalize,
            max_qubits=self.max_qubits,
            truncation_policy=self.on_truncate,
        )

    def _required_qubits(self, n: int) -> int:
        bits = max(1, (max(2, n) - 1).bit_length())
        return min(bits, self.max_qubits)

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

    def _amplitude_encode(self, tokens: List[float], size: int) -> Any:
        n_qubits = size.bit_length() - 1
        use_numpy = _HAS_NUMPY and n_qubits > _NUMPY_THRESHOLD
        bytes_per = _NP_FLOAT_BYTES if use_numpy else _PY_FLOAT_BYTES
        projected = size * bytes_per
        if projected > self.max_state_bytes:
            raise MemoryBudgetError(
                f"state vector of {size} amplitudes "
                f"(~{projected / 2**20:.1f} MiB) exceeds budget of "
                f"{self.max_state_bytes / 2**20:.1f} MiB"
            )
        if use_numpy:  # pragma: no cover
            return self._amplitude_encode_numpy(tokens, size)
        return self._amplitude_encode_python(tokens, size)

    def _amplitude_encode_python(self, tokens, size):
        amps = list(tokens[:size])
        if len(amps) < size:
            amps.extend([0.0] * (size - len(amps)))

        norm = math.sqrt(sum(a * a for a in amps))
        zero_norm = norm == 0.0
        if zero_norm:
            if self.on_zero_norm == "error":
                raise TranslationError(
                    "token stream has zero norm; not a valid state"
                )
            amps[0] = 1.0  # documented convention: |0...0>
            norm = 1.0
        elif self.normalize:
            amps = [a / norm for a in amps]
        return amps, norm, zero_norm

    def _amplitude_encode_numpy(self, tokens, size):  # pragma: no cover
        arr = _np.zeros(size, dtype=_np.float64)
        n = min(len(tokens), size)
        arr[:n] = tokens[:n]
        norm = float(_np.linalg.norm(arr))
        zero_norm = norm == 0.0
        if zero_norm:
            if self.on_zero_norm == "error":
                raise TranslationError(
                    "token stream has zero norm; not a valid state"
                )
            arr = _np.zeros(size, dtype=_np.float64)
            arr[0] = 1.0
            norm = 1.0
        elif self.normalize:
            arr = arr / norm
        return arr, norm, zero_norm

    def _angle_encode(self, tokens: List[float]) -> List[float]:
        # theta in (0, pi); invertible: t = tan(theta - pi/2)
        n = min(len(tokens), self.max_qubits)
        return [math.atan(tokens[i]) + math.pi / 2.0 for i in range(n)]


# ===========================================================================
# TEST SUITE — only defined when pytest + hypothesis are available
# ===========================================================================

try:
    import pytest
    from hypothesis import HealthCheck, given, settings
    from hypothesis import strategies as st
    _HAS_TEST_DEPS = True
except ImportError:  # pragma: no cover
    _HAS_TEST_DEPS = False


if _HAS_TEST_DEPS:

    # ---- strategies -----------------------------------------------------

    finite_floats = st.floats(
        allow_nan=False, allow_infinity=False, width=64,
    )
    round_trip_floats = st.floats(
        min_value=-1e10, max_value=1e10,
        allow_nan=False, allow_infinity=False, width=64,
    )

    def _make_nonzero(xs):
        if any(x != 0.0 for x in xs):
            return xs
        return [1.0] + list(xs[1:])

    nonzero_tokens = st.lists(
        finite_floats, min_size=1, max_size=50,
    ).map(_make_nonzero)

    nonzero_tokens_bounded = st.lists(
        round_trip_floats, min_size=1, max_size=50,
    ).map(_make_nonzero)

    DEFAULT_SETTINGS = settings(
        max_examples=100,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )

    # ---- encoding validation -------------------------------------------

    class TestEncodingValidation:
        def test_empty_input_with_unknown_encoding_raises(self):
            t = ClassicalTokenTranslator()
            with pytest.raises(EncodingError):
                t.translate([], encoding="bogus")  # type: ignore[arg-type]

        def test_empty_input_valid_encoding_returns_empty(self):
            t = ClassicalTokenTranslator()
            for enc in ("amplitude", "angle"):
                r = t.translate([], encoding=enc)  # type: ignore[arg-type]
                assert r.n_qubits == 0
                assert r.encoding_name == enc
                assert r.metadata["original_length"] == 0
                assert r.translation_hash.startswith("sha256:")

    # ---- token validation ----------------------------------------------

    class TestTokenValidation:
        @pytest.mark.parametrize("bad", [
            [float("nan")], [float("inf")], [float("-inf")],
            [True, False], ["1.5"], [b"x"], [object()],
        ])
        def test_rejects_invalid_tokens(self, bad):
            t = ClassicalTokenTranslator()
            with pytest.raises(TokenError):
                t.translate(bad)

        def test_rejects_string_as_whole_tokens(self):
            t = ClassicalTokenTranslator()
            with pytest.raises(TokenError):
                t.translate("123")  # type: ignore[arg-type]

        def test_accepts_fraction_decimal(self):
            from decimal import Decimal
            from fractions import Fraction
            t = ClassicalTokenTranslator()
            r = t.translate([Fraction(1, 2), Decimal("0.25")])
            assert r.is_valid_state

        def test_negative_zero_canonicalised(self):
            t = ClassicalTokenTranslator()
            a = t.translate([0.0, 1.0]).translation_hash
            b = t.translate([-0.0, 1.0]).translation_hash
            assert a == b

    # ---- truncation ----------------------------------------------------

    class TestTruncation:
        def test_error_policy(self):
            t = ClassicalTokenTranslator(max_qubits=2)  # capacity 4
            with pytest.raises(TruncationError):
                t.translate([1.0] * 5)

        def test_truncate_policy(self):
            t = ClassicalTokenTranslator(max_qubits=2, on_truncate="truncate")
            r = t.translate([1.0] * 5)
            assert r.metadata["truncated"] is True
            assert r.n_qubits == 2
            assert r.metadata["original_length"] == 5

        def test_warn_policy(self, caplog):
            import logging as _logging
            t = ClassicalTokenTranslator(max_qubits=2, on_truncate="warn")
            with caplog.at_level(_logging.WARNING):
                t.translate([1.0] * 5)
            assert any("truncat" in m.lower() for m in caplog.messages)

    # ---- zero-norm -----------------------------------------------------

    class TestZeroNorm:
        def test_zero_state_policy(self):
            t = ClassicalTokenTranslator()
            r = t.translate([0.0, 0.0, 0.0])
            assert r.metadata["zero_norm_fallback"] is True
            assert r.amplitudes[0] == 1.0
            assert all(a == 0.0 for a in r.amplitudes[1:])
            assert r.is_valid_state

        def test_error_policy(self):
            t = ClassicalTokenTranslator(on_zero_norm="error")
            with pytest.raises(TranslationError):
                t.translate([0.0, 0.0])

    # ---- amplitude encoding property tests ----------------------------

    class TestAmplitudeEncoding:
        @DEFAULT_SETTINGS
        @given(tokens=nonzero_tokens)
        def test_valid_state(self, tokens):
            t = ClassicalTokenTranslator()
            r = t.translate(tokens)
            assert r.is_valid_state
            assert len(r.amplitudes) == 2 ** r.n_qubits

        @DEFAULT_SETTINGS
        @given(tokens=nonzero_tokens)
        def test_n_qubits_matches_size(self, tokens):
            t = ClassicalTokenTranslator()
            r = t.translate(tokens)
            assert 1 << r.n_qubits == len(r.amplitudes)

        @DEFAULT_SETTINGS
        @given(tokens=nonzero_tokens)
        def test_normalization_matches_metadata(self, tokens):
            t = ClassicalTokenTranslator()
            r = t.translate(tokens)
            if r.metadata.get("zero_norm_fallback"):
                return
            norm = r.metadata["original_norm"]
            expected_len = r.metadata["original_length"]
            for i in range(expected_len):
                assert math.isclose(
                    r.amplitudes[i] * norm, tokens[i],
                    rel_tol=1e-9, abs_tol=1e-12,
                )

    # ---- angle encoding property tests --------------------------------

    class TestAngleEncoding:
        @DEFAULT_SETTINGS
        @given(tokens=nonzero_tokens_bounded)
        def test_angles_in_open_unit_interval_pi(self, tokens):
            t = ClassicalTokenTranslator()
            r = t.translate(tokens, encoding="angle")
            for a in r.phase_angles:
                assert 0.0 < a < math.pi

    # ---- decode round-trip property tests -----------------------------

    class TestDecodeRoundTrip:
        @DEFAULT_SETTINGS
        @given(tokens=nonzero_tokens_bounded)
        def test_amplitude_round_trip(self, tokens):
            t = ClassicalTokenTranslator()
            r = t.translate(tokens)
            decoded = t.decode(r)
            assert len(decoded) == len(tokens)
            for a, b in zip(decoded, tokens):
                assert math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12)

        @DEFAULT_SETTINGS
        @given(tokens=nonzero_tokens_bounded)
        def test_amplitude_round_trip_unnormalized(self, tokens):
            t = ClassicalTokenTranslator(normalize=False)
            r = t.translate(tokens)
            decoded = t.decode(r)
            assert len(decoded) == len(tokens)
            for a, b in zip(decoded, tokens):
                assert math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12)

        @DEFAULT_SETTINGS
        @given(tokens=nonzero_tokens_bounded)
        def test_angle_round_trip(self, tokens):
            t = ClassicalTokenTranslator()
            r = t.translate(tokens, encoding="angle")
            decoded = t.decode(r)
            assert len(decoded) == r.n_qubits
            for a, b in zip(decoded, tokens[:r.n_qubits]):
                assert math.isclose(a, b, rel_tol=1e-5, abs_tol=1e-12)

        def test_amplitude_decode_requires_metadata(self):
            t = ClassicalTokenTranslator()
            r = TranslationResult(
                amplitudes=[1.0, 0.0],
                n_qubits=1,
                encoding_name="amplitude",
            )
            with pytest.raises(TranslationError):
                t.decode(r)

        def test_empty_round_trip(self):
            t = ClassicalTokenTranslator()
            for enc in ("amplitude", "angle"):
                r = t.translate([], encoding=enc)  # type: ignore[arg-type]
                assert t.decode(r) == []

    # ---- hash ----------------------------------------------------------

    class TestHash:
        @DEFAULT_SETTINGS
        @given(tokens=nonzero_tokens)
        def test_hash_stability(self, tokens):
            t = ClassicalTokenTranslator()
            assert (
                t.translate(tokens).translation_hash
                == t.translate(tokens).translation_hash
            )

        @DEFAULT_SETTINGS
        @given(tokens=nonzero_tokens)
        def test_hash_distinguishes_encoding(self, tokens):
            t = ClassicalTokenTranslator(on_truncate="truncate")
            a = t.translate(tokens, encoding="amplitude").translation_hash
            b = t.translate(tokens, encoding="angle").translation_hash
            assert a != b

        @DEFAULT_SETTINGS
        @given(tokens=nonzero_tokens)
        def test_hash_distinguishes_normalization_flag(self, tokens):
            a = ClassicalTokenTranslator(normalize=True).translate(
                tokens, encoding="amplitude",
            ).translation_hash
            b = ClassicalTokenTranslator(normalize=False).translate(
                tokens, encoding="amplitude",
            ).translation_hash
            assert a != b

        def test_hash_covers_version(self):
            h1 = _hash_translation(
                [1.0], encoding="amplitude", normalize=True,
                max_qubits=16, truncation_policy="error", version="a",
            )
            h2 = _hash_translation(
                [1.0], encoding="amplitude", normalize=True,
                max_qubits=16, truncation_policy="error", version="b",
            )
            assert h1 != h2

    # ---- memory budget -------------------------------------------------

    class TestMemoryBudget:
        def test_budget_exceeded_raises(self):
            t = ClassicalTokenTranslator(
                max_qubits=20,
                max_state_bytes=1024 * 1024,
                on_truncate="truncate",
            )
            with pytest.raises(MemoryBudgetError):
                t.translate([1.0] * (2 ** 19))

    # ---- exporters -----------------------------------------------------

    class TestExporters:
        def _amp(self):
            t = ClassicalTokenTranslator()
            return t.translate([1.0, 2.0, 3.0])

        def _angle(self):
            t = ClassicalTokenTranslator()
            return t.translate([1.0, 2.0, 3.0], encoding="angle")

        def test_qasm_amplitude(self):
            qasm = self._amp().to_qasm()
            assert qasm.startswith("OPENQASM 2.0;")
            assert "initialize(" in qasm
            assert "qreg q[" in qasm
            assert f"v{VERSION}" in qasm

        def test_qasm_angle(self):
            qasm = self._angle().to_qasm()
            assert qasm.startswith("OPENQASM 2.0;")
            assert qasm.count("ry(") == self._angle().n_qubits
            assert "initialize(" not in qasm

        def test_qiskit_amplitude(self):
            try:
                import qiskit  # noqa: F401
            except ImportError:
                pytest.skip("qiskit not installed")
            r = self._amp()
            qc = r.to_qiskit()
            assert qc.num_qubits == r.n_qubits

        def test_qiskit_angle(self):
            try:
                import qiskit  # noqa: F401
            except ImportError:
                pytest.skip("qiskit not installed")
            r = self._angle()
            qc = r.to_qiskit()
            assert qc.num_qubits == r.n_qubits

        def test_braket_angle(self):
            try:
                from braket.circuits import Circuit  # noqa: F401
            except ImportError:
                pytest.skip("amazon-braket-sdk not installed")
            r = self._angle()
            circ = r.to_braket()
            assert len(list(circ.instructions)) == r.n_qubits

        def test_braket_amplitude_unsupported(self):
            try:
                from braket.circuits import Circuit  # noqa: F401
            except ImportError:
                pytest.skip("amazon-braket-sdk not installed")
            with pytest.raises(NotImplementedError):
                self._amp().to_braket()

    # ---- numpy fast path -----------------------------------------------

    @pytest.mark.skipif(not _HAS_NUMPY, reason="numpy not installed")
    class TestNumpyPath:
        def test_returns_ndarray_above_threshold(self):
            import numpy as np
            t = ClassicalTokenTranslator(max_qubits=14)
            tokens = list(range(1, 2 ** 13 + 1))
            r = t.translate(tokens)
            assert r.n_qubits == 13
            assert isinstance(r.amplitudes, np.ndarray)
            assert r.is_valid_state

        def test_returns_list_at_or_below_threshold(self):
            t = ClassicalTokenTranslator(max_qubits=14)
            tokens = list(range(1, 2 ** 12 + 1))
            r = t.translate(tokens)
            assert r.n_qubits == 12
            assert isinstance(r.amplitudes, list)
            assert r.is_valid_state

        def test_numpy_and_python_paths_agree(self):
            import numpy as np
            import sys as _sys
            mod = _sys.modules[__name__]
            tokens = list(range(1, 2 ** 12 + 1))
            r_py = ClassicalTokenTranslator(max_qubits=14).translate(tokens)
            old = mod._NUMPY_THRESHOLD
            try:
                mod._NUMPY_THRESHOLD = 10
                r_np = ClassicalTokenTranslator(max_qubits=14).translate(tokens)
            finally:
                mod._NUMPY_THRESHOLD = old
            assert isinstance(r_np.amplitudes, np.ndarray)
            np.testing.assert_allclose(
                np.asarray(r_py.amplitudes), r_np.amplitudes, atol=1e-12,
            )

    # ---- TranslationResult invariants ---------------------------------

    class TestResultInvariants:
        def test_mixed_modes_rejected(self):
            with pytest.raises(TranslationError):
                TranslationResult(
                    amplitudes=[1.0, 0.0],
                    phase_angles=[0.5],
                    n_qubits=1,
                    encoding_name="amplitude",
                )

        def test_wrong_amplitude_length_rejected(self):
            with pytest.raises(TranslationError):
                TranslationResult(
                    amplitudes=[1.0, 0.0, 0.0],
                    n_qubits=1,
                    encoding_name="amplitude",
                )


# ===========================================================================
# __main__ — demo, plus optional test runner
# ===========================================================================

if __name__ == "__main__":
    import sys

    if "--test" in sys.argv:
        if not _HAS_TEST_DEPS:
            sys.exit(
                "pytest and hypothesis required for --test mode; "
                "install with `pip install pytest hypothesis`"
            )
        sys.exit(pytest.main([__file__, "-v"]))

    # Demo
    logging.basicConfig(level=logging.INFO)
    t = ClassicalTokenTranslator(max_qubits=8)

    r = t.translate([1.0, -2.0, 3.0])
    print("amplitude:", r.n_qubits, "qubits | valid:", r.is_valid_state)
    print("  amplitudes:", [round(a, 4) for a in r.amplitudes])
    print("  hash:", r.translation_hash[:23], "…")
    print("  round-trip:", t.decode(r))

    r2 = t.translate([1.0, -2.0, 3.0], encoding="angle")
    print("angle:", r2.n_qubits, "qubits")
    print("  angles:", [round(a, 4) for a in r2.phase_angles])
    print("  round-trip:", t.decode(r2))

    print("--- qasm (amplitude) ---")
    print(r.to_qasm())
    print("--- qasm (angle) ---")
    print(r2.to_qasm())

    if _HAS_TEST_DEPS:
        print(f"Run tests:  pytest -v {__file__}")
        print(f"     or:    python {__file__} --test")
    else:
        print("Install test deps to run tests: pip install pytest hypothesis")
