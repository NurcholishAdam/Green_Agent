"""
NoiseCharacterization — estimate backend noise and its impact.

Given a backend's gate fidelity, readout fidelity, and coherence time,
plus a circuit's depth, qubit count, and (optionally) gate time, this
module produces a `NoiseEstimate` — an expected end-to-end fidelity and
the derived error rates.

What the model captures
-----------------------
``expected_fidelity`` is the product of:

  * **Gate penalty** — ``gate_fidelity ** n_gates``, where ``n_gates`` is
    ``total_gate_count`` if provided (accurate), else ``circuit_depth``
    as a lower-bound proxy (documented in ``notes``).
  * **Readout penalty** — ``readout_fidelity ** n_qubits``, assuming
    independent per-qubit readout errors. A note documents the
    independence assumption when ``n_qubits > 1``.
  * **Coherence penalty** — ``exp(-circuit_duration_us / coherence_time_us)``,
    applied only when both ``coherence_time_us`` and ``gate_time_ns`` are
    provided. This is the exponential dephasing model (a reasonable
    first-order approximation; real hardware has T1/T2 dynamics the
    model doesn't capture).

What the model does NOT capture
-------------------------------
  * Crosstalk between adjacent qubits.
  * Correlated readout errors (shared readout lines).
  * Gate-dependent error rates (some gates are noisier than others).
  * State-prep error.

A caller requiring tighter bounds should treat this estimate as a
*lower bound* on noise — the true error rate is at least as high.

Original API preserved:
    nc = NoiseCharacterization()
    est = nc.estimate(
        backend_gate_fidelity=..., backend_readout_fidelity=...,
        circuit_depth=..., n_qubits=..., coherence_time_us=...,
    )
    est.expected_fidelity / est.gate_error_rate / est.confidence

v2 fixes (see changelog in commit message):
  * No more ``gate or default`` (0.0 is no longer replaced by the
    default). ``used_defaults`` names which inputs fell back.
  * Coherence penalty is now applied — the previous version stored
    coherence time but ignored it in the fidelity calculation.
  * ``exceeds_threshold`` renamed to ``needs_fallback`` (the previous
    name returned the opposite of what it said); the old name remains
    as a deprecation alias.
  * ``total_gate_count`` parameter added — ``gate ** depth`` was
    underestimating noise on dense circuits.
  * ``circuit_depth`` / ``n_qubits`` / ``coherence_time_us`` validated.
  * ``notes: list = None`` → ``Tuple[str, ...] = ()``.
  * ``NoiseEstimate`` frozen; ``__post_init__`` validates.
  * ``NoiseConfig`` dataclass; constructor validates.
  * ``min_fidelity`` validated in ``needs_fallback``.
  * ``to_dict`` / ``checksum`` / ``estimate_from_profile`` / health.
  * Structured logging, metrics, ``__version__``, ``__all__``, demo.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
import warnings
from dataclasses import asdict, dataclass, field, fields
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Constants
# =============================================================================

CONFIDENCE_BASELINE = 0.4
CONFIDENCE_CEILING = 0.95
CONFIDENCE_FLOOR = 0.1


# =============================================================================
# Errors
# =============================================================================

class NoiseCharacterizationError(Exception):
    """Base class for NoiseCharacterization errors."""


class NoiseCharacterizationInputError(NoiseCharacterizationError, ValueError):
    """Invalid inputs to a method or the constructor."""


# =============================================================================
# Metrics
# =============================================================================

class MetricsRecorder:
    """Drop-in replaceable with Prometheus / OTel."""

    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _key(name: str, labels: Mapping[str, Any]) -> str:
        if not labels:
            return name
        parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{parts}}}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.counters[k] = self.counters.get(k, 0.0) + value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.histograms.setdefault(k, []).append(float(value))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            hist: Dict[str, Dict[str, float]] = {}
            for k, vals in self.histograms.items():
                if not vals:
                    continue
                s = sorted(vals)
                hist[k] = {
                    "count": len(vals),
                    "mean": sum(vals) / len(vals),
                    "p50": s[len(s) // 2],
                    "p99": s[min(len(s) - 1, int(len(s) * 0.99))],
                    "max": s[-1],
                }
            return {
                "counters": dict(self.counters),
                "histograms": hist,
            }


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class NoiseConfig:
    """Frozen, validated configuration for ``NoiseCharacterization``."""

    default_gate_fidelity: float = 0.99
    default_readout_fidelity: float = 0.98

    def __post_init__(self) -> None:
        for fname in (
            "default_gate_fidelity", "default_readout_fidelity",
        ):
            v = getattr(self, fname)
            if isinstance(v, bool) \
                    or not isinstance(v, (int, float)) \
                    or not math.isfinite(float(v)):
                raise NoiseCharacterizationInputError(
                    f"{fname} must be a finite number"
                )
            if not (0.0 < float(v) <= 1.0):
                raise NoiseCharacterizationInputError(
                    f"{fname} must be in (0, 1]; got {v!r}"
                )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Result
# =============================================================================

@dataclass(frozen=True)
class NoiseEstimate:
    """
    Immutable estimate of a backend's noise for a specific circuit.

    Fields
    ------
    gate_error_rate, readout_error_rate
        Per-gate and per-qubit error rates used in the estimate.
    gate_fidelity_used, readout_fidelity_used
        The fidelity values that actually entered the calculation —
        equal to the backend's values when provided, the config
        defaults otherwise.
    used_defaults
        Names of the inputs that fell back to config defaults. Empty
        when the caller provided every input.
    expected_fidelity
        The end-to-end expected fidelity, product of gate, readout,
        and coherence penalties.
    coherence_penalty
        The multiplicative dephasing factor, or ``None`` if not applied
        (``coherence_time_us`` or ``gate_time_ns`` was missing).
    estimated_duration_us
        The circuit duration used in the dephasing calculation, or
        ``None`` if not computed.
    n_gates_used
        The gate count that entered the gate-fidelity calculation.
    confidence
        A rough confidence signal in ``[0, 1]``, higher when more of
        the inputs were known.
    notes
        Human-readable observations. Empty when nothing notable.
    """

    gate_error_rate: Optional[float] = None
    readout_error_rate: Optional[float] = None
    coherence_time_us: Optional[float] = None

    gate_fidelity_used: Optional[float] = None
    readout_fidelity_used: Optional[float] = None
    used_defaults: FrozenSet[str] = frozenset()

    expected_fidelity: Optional[float] = None
    coherence_penalty: Optional[float] = None
    estimated_duration_us: Optional[float] = None
    n_gates_used: Optional[int] = None

    confidence: float = 0.5
    notes: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # --- Coerce used_defaults / notes ---
        if not isinstance(self.used_defaults, frozenset):
            object.__setattr__(
                self, "used_defaults", frozenset(self.used_defaults),
            )
        if not isinstance(self.notes, tuple):
            object.__setattr__(self, "notes", tuple(self.notes))

        # --- Probability-like fields in [0, 1] ---
        for fname in (
            "gate_error_rate", "readout_error_rate",
            "gate_fidelity_used", "readout_fidelity_used",
            "expected_fidelity", "coherence_penalty",
        ):
            v = getattr(self, fname)
            if v is None:
                continue
            if not isinstance(v, (int, float)) \
                    or isinstance(v, bool) \
                    or not math.isfinite(float(v)):
                raise NoiseCharacterizationInputError(
                    f"{fname} must be a finite number or None"
                )
            if not (0.0 <= float(v) <= 1.0):
                raise NoiseCharacterizationInputError(
                    f"{fname} must be in [0, 1] or None; got {v!r}"
                )

        # --- Coherence / duration must be positive or None ---
        for fname in ("coherence_time_us", "estimated_duration_us"):
            v = getattr(self, fname)
            if v is not None:
                if not isinstance(v, (int, float)) \
                        or not math.isfinite(float(v)) \
                        or float(v) <= 0:
                    raise NoiseCharacterizationInputError(
                        f"{fname} must be > 0 or None"
                    )

        # --- n_gates_used non-negative or None ---
        if self.n_gates_used is not None:
            if isinstance(self.n_gates_used, bool) \
                    or not isinstance(self.n_gates_used, int) \
                    or self.n_gates_used < 0:
                raise NoiseCharacterizationInputError(
                    "n_gates_used must be a non-negative int or None"
                )

        # --- Confidence in [0, 1] ---
        if not isinstance(self.confidence, (int, float)) \
                or isinstance(self.confidence, bool) \
                or not math.isfinite(float(self.confidence)):
            raise NoiseCharacterizationInputError(
                "confidence must be a finite number"
            )
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise NoiseCharacterizationInputError(
                f"confidence must be in [0, 1]; got {self.confidence!r}"
            )

        # --- Notes must be strings ---
        for i, note in enumerate(self.notes):
            if not isinstance(note, str):
                raise NoiseCharacterizationInputError(
                    f"notes[{i}] must be a string; "
                    f"got {type(note).__name__}"
                )

    # ------------------------------------------------------------------ helpers
    def needs_fallback(self, *, min_fidelity: float) -> bool:
        """
        Whether the estimate is below the given fidelity threshold.

        Returns True when we should fall back to classical execution —
        either because the fidelity is low or because it wasn't measured
        (conservative). Prefer this over the deprecated
        ``NoiseCharacterization.exceeds_threshold``.
        """
        if not (0.0 <= float(min_fidelity) <= 1.0):
            raise NoiseCharacterizationInputError(
                "min_fidelity must be in [0, 1]"
            )
        if self.expected_fidelity is None:
            return True
        return self.expected_fidelity < min_fidelity

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "gate_error_rate": self.gate_error_rate,
            "readout_error_rate": self.readout_error_rate,
            "coherence_time_us": self.coherence_time_us,
            "gate_fidelity_used": self.gate_fidelity_used,
            "readout_fidelity_used": self.readout_fidelity_used,
            "used_defaults": sorted(self.used_defaults),
            "expected_fidelity": self.expected_fidelity,
            "coherence_penalty": self.coherence_penalty,
            "estimated_duration_us": self.estimated_duration_us,
            "n_gates_used": self.n_gates_used,
            "confidence": self.confidence,
            "notes": list(self.notes),
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "NoiseEstimate":
        if not isinstance(d, Mapping):
            raise NoiseCharacterizationInputError(
                f"d must be a Mapping; got {type(d).__name__}"
            )
        known = {f.name for f in fields(cls)}
        unknown = set(d.keys()) - known
        if unknown:
            raise NoiseCharacterizationInputError(
                f"unknown fields in NoiseEstimate dict: "
                f"{sorted(unknown)}"
            )
        cleaned: Dict[str, Any] = dict(d)
        if isinstance(cleaned.get("used_defaults"), (list, set)):
            cleaned["used_defaults"] = frozenset(cleaned["used_defaults"])
        if isinstance(cleaned.get("notes"), list):
            cleaned["notes"] = tuple(cleaned["notes"])
        return cls(**cleaned)

    def checksum(self) -> str:
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            f"NoiseEstimate("
            f"fidelity={self.expected_fidelity!r}, "
            f"confidence={self.confidence:.2f}, "
            f"gate_fid={self.gate_fidelity_used!r}, "
            f"readout_fid={self.readout_fidelity_used!r}, "
            f"coherence_penalty={self.coherence_penalty!r})"
        )


# =============================================================================
# The Enhanced NoiseCharacterization
# =============================================================================

class NoiseCharacterization:
    """
    Estimate per-job fidelity from a backend profile.

    See the module docstring for the model and its limitations.
    """

    def __init__(
        self,
        *,
        default_gate_fidelity: float = 0.99,
        default_readout_fidelity: float = 0.98,
        config: Optional[NoiseConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        # Backward-compatible kwargs override config defaults.
        if config is None:
            config = NoiseConfig(
                default_gate_fidelity=default_gate_fidelity,
                default_readout_fidelity=default_readout_fidelity,
            )
        self.config = config
        self.default_gate_fidelity = config.default_gate_fidelity
        self.default_readout_fidelity = config.default_readout_fidelity
        self._metrics = metrics or MetricsRecorder()
        self._started_mono = time.monotonic()

    # ------------------------------------------------------------------ estimate
    def estimate(
        self,
        *,
        backend_gate_fidelity: Optional[float],
        backend_readout_fidelity: Optional[float],
        circuit_depth: int,
        n_qubits: int,
        coherence_time_us: Optional[float] = None,
        total_gate_count: Optional[int] = None,
        gate_time_ns: Optional[float] = None,
    ) -> NoiseEstimate:
        """
        Estimate the end-to-end fidelity for a circuit on a backend.

        Parameters
        ----------
        backend_gate_fidelity : Optional[float]
            Per-gate fidelity in [0, 1], or ``None`` to use the config
            default. A value of ``0.0`` is honored (a completely broken
            gate); the config default is used only when this is ``None``.
        backend_readout_fidelity : Optional[float]
            Per-qubit readout fidelity in [0, 1], or ``None``.
        circuit_depth : int
            Circuit depth, used as the gate count when
            ``total_gate_count`` is not supplied. Must be >= 0.
        n_qubits : int
            Qubit count. Must be >= 1.
        coherence_time_us : Optional[float]
            Backend coherence time in microseconds. When combined with
            ``gate_time_ns``, a dephasing penalty is applied.
        total_gate_count : Optional[int]
            Total number of gates in the circuit. When supplied, this is
            more accurate than ``circuit_depth`` as the exponent for the
            gate-fidelity term. Must be >= 0.
        gate_time_ns : Optional[float]
            Duration of a single gate in nanoseconds, used to compute the
            circuit duration for the coherence penalty. Must be > 0.
        """
        # --- Input validation ---
        self._validate_inputs(
            backend_gate_fidelity=backend_gate_fidelity,
            backend_readout_fidelity=backend_readout_fidelity,
            circuit_depth=circuit_depth,
            n_qubits=n_qubits,
            coherence_time_us=coherence_time_us,
            total_gate_count=total_gate_count,
            gate_time_ns=gate_time_ns,
        )

        # --- Resolve gate / readout, tracking which used defaults ---
        used_defaults: List[str] = []
        if backend_gate_fidelity is None:
            gate_fidelity_used = float(self.config.default_gate_fidelity)
            used_defaults.append("gate_fidelity")
        else:
            gate_fidelity_used = float(backend_gate_fidelity)

        if backend_readout_fidelity is None:
            readout_fidelity_used = float(self.config.default_readout_fidelity)
            used_defaults.append("readout_fidelity")
        else:
            readout_fidelity_used = float(backend_readout_fidelity)

        # --- Gate penalty: gate_fidelity ** n_gates ---
        notes: List[str] = []
        if total_gate_count is not None:
            n_gates = int(total_gate_count)
        else:
            n_gates = int(circuit_depth)
            if circuit_depth > 0:
                notes.append(
                    "gate-count approximated by circuit_depth; supply "
                    "total_gate_count for a tighter bound"
                )

        if n_gates <= 0:
            gate_component = 1.0
        else:
            gate_component = gate_fidelity_used ** n_gates

        # --- Readout penalty: readout_fidelity ** n_qubits ---
        readout_component = readout_fidelity_used ** int(n_qubits)
        if n_qubits > 1 and readout_fidelity_used < 1.0:
            notes.append(
                "readout assumes independent per-qubit errors; "
                "correlated readout will be worse"
            )

        # --- Coherence penalty: exp(-duration_us / coherence_time_us) ---
        coherence_penalty: Optional[float] = None
        estimated_duration_us: Optional[float] = None
        if (
            coherence_time_us is not None
            and gate_time_ns is not None
            and n_gates > 0
        ):
            estimated_duration_us = (
                n_gates * float(gate_time_ns) / 1000.0
            )
            coherence_penalty = math.exp(
                -estimated_duration_us / float(coherence_time_us)
            )
            notes.append(
                f"coherence penalty applied: "
                f"duration={estimated_duration_us:.2f}us / "
                f"coherence={coherence_time_us:.2f}us "
                f"→ ×{coherence_penalty:.3f}"
            )
        elif coherence_time_us is not None and gate_time_ns is None:
            notes.append(
                "coherence_time_us provided but gate_time_ns is "
                "missing; coherence penalty not applied"
            )

        # --- Combine ---
        expected = gate_component * readout_component
        if coherence_penalty is not None:
            expected *= coherence_penalty
        # Numerically clamp to [0, 1] in case of float edge cases.
        expected = max(0.0, min(1.0, expected))

        # --- Notes for extreme cases ---
        if expected < 0.5:
            notes.append("expected fidelity below 50% — high noise")
        if expected < 0.01:
            notes.append("expected fidelity below 1% — circuit infeasible")

        # --- Confidence: derived from which inputs were known ---
        confidence = self._compute_confidence(
            gate_provided=backend_gate_fidelity is not None,
            readout_provided=backend_readout_fidelity is not None,
            coherence_provided=(
                coherence_time_us is not None and coherence_time_us > 0
            ),
            gate_time_provided=gate_time_ns is not None,
            total_gate_count_provided=total_gate_count is not None,
        )

        # --- Build result ---
        result = NoiseEstimate(
            gate_error_rate=1.0 - gate_fidelity_used,
            readout_error_rate=1.0 - readout_fidelity_used,
            coherence_time_us=(
                float(coherence_time_us)
                if coherence_time_us is not None else None
            ),
            gate_fidelity_used=gate_fidelity_used,
            readout_fidelity_used=readout_fidelity_used,
            used_defaults=frozenset(used_defaults),
            expected_fidelity=expected,
            coherence_penalty=coherence_penalty,
            estimated_duration_us=estimated_duration_us,
            n_gates_used=n_gates,
            confidence=confidence,
            notes=tuple(notes),
        )

        # --- Metrics ---
        self._metrics.inc(
            "estimate_total",
            used_defaults=str(len(used_defaults)),
        )
        self._metrics.observe("expected_fidelity", expected)
        self._metrics.observe("confidence", confidence)
        logger.debug(
            "noise estimate",
            extra={
                "expected_fidelity": expected,
                "confidence": confidence,
                "n_gates_used": n_gates,
                "used_defaults": list(used_defaults),
            },
        )
        return result

    # ------------------------------------------------------------------ threshold
    def needs_fallback(
        self,
        estimate: NoiseEstimate,
        *,
        min_fidelity: float,
    ) -> bool:
        """
        Whether this estimate is below ``min_fidelity`` — i.e., the
        backend should fall back to classical execution.

        Returns ``True`` when:
          * ``expected_fidelity`` is below ``min_fidelity``, or
          * ``expected_fidelity`` is ``None`` (conservative).

        Raises ``NoiseCharacterizationInputError`` when ``min_fidelity``
        is out of range.
        """
        if not (0.0 <= float(min_fidelity) <= 1.0):
            raise NoiseCharacterizationInputError(
                "min_fidelity must be in [0, 1]"
            )
        if estimate.expected_fidelity is None:
            return True
        return estimate.expected_fidelity < float(min_fidelity)

    def exceeds_threshold(
        self,
        estimate: NoiseEstimate,
        *,
        min_fidelity: float,
    ) -> bool:
        """
        Deprecated alias for ``needs_fallback``.

        The old name was misleading: it returns ``True`` when the
        fidelity is **below** the threshold, i.e., when the estimate
        exceeds the noise threshold. Use ``needs_fallback`` in new code.
        """
        warnings.warn(
            "exceeds_threshold is deprecated; use needs_fallback. "
            "The method returns True when the fidelity is BELOW the "
            "threshold, which does not match the name.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.needs_fallback(estimate, min_fidelity=min_fidelity)

    # ------------------------------------------------------------------ profile bridge
    def estimate_from_profile(
        self,
        profile: Any,
        *,
        circuit_depth: int,
        n_qubits: int,
        total_gate_count: Optional[int] = None,
        gate_time_ns: Optional[float] = None,
    ) -> NoiseEstimate:
        """
        Build an estimate from a ``BackendProfile`` (original or enhanced).

        Uses the profile's `effective_gate_fidelity`,
        `readout_fidelity`, and `effective_coherence_us` when available,
        falling back to `gate_fidelity_2q` / `gate_fidelity` and
        `t2_coherence_us` / `coherence_time_us` for the original profile.
        Missing fields are passed through as ``None`` and fall back to
        the config defaults inside ``estimate``.
        """
        gate = _profile_attr(profile, (
            "effective_gate_fidelity", "gate_fidelity_2q",
            "gate_fidelity",
        ))
        readout = _profile_attr(profile, ("readout_fidelity",))
        coherence = _profile_attr(profile, (
            "effective_coherence_us", "t2_coherence_us",
            "coherence_time_us",
        ))
        return self.estimate(
            backend_gate_fidelity=gate,
            backend_readout_fidelity=readout,
            circuit_depth=circuit_depth,
            n_qubits=n_qubits,
            coherence_time_us=coherence,
            total_gate_count=total_gate_count,
            gate_time_ns=gate_time_ns,
        )

    # ------------------------------------------------------------------ health
    def health(self) -> Dict[str, Any]:
        return {
            "ready": True,
            "config": self.config.to_dict(),
            "uptime_seconds": time.monotonic() - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }

    # ------------------------------------------------------------------ internals
    def _validate_inputs(
        self,
        *,
        backend_gate_fidelity: Any,
        backend_readout_fidelity: Any,
        circuit_depth: Any,
        n_qubits: Any,
        coherence_time_us: Any,
        total_gate_count: Any,
        gate_time_ns: Any,
    ) -> None:
        for fname, v in (
            ("backend_gate_fidelity", backend_gate_fidelity),
            ("backend_readout_fidelity", backend_readout_fidelity),
        ):
            if v is None:
                continue
            if isinstance(v, bool) \
                    or not isinstance(v, (int, float)) \
                    or not math.isfinite(float(v)):
                raise NoiseCharacterizationInputError(
                    f"{fname} must be a finite number or None"
                )
            if not (0.0 <= float(v) <= 1.0):
                raise NoiseCharacterizationInputError(
                    f"{fname} must be in [0, 1] or None; got {v!r}"
                )

        if isinstance(circuit_depth, bool) \
                or not isinstance(circuit_depth, int) \
                or circuit_depth < 0:
            raise NoiseCharacterizationInputError(
                f"circuit_depth must be an int >= 0; "
                f"got {circuit_depth!r}"
            )
        if isinstance(n_qubits, bool) \
                or not isinstance(n_qubits, int) \
                or n_qubits < 1:
            raise NoiseCharacterizationInputError(
                f"n_qubits must be an int >= 1; got {n_qubits!r}"
            )
        if coherence_time_us is not None:
            if not isinstance(coherence_time_us, (int, float)) \
                    or isinstance(coherence_time_us, bool) \
                    or not math.isfinite(float(coherence_time_us)) \
                    or float(coherence_time_us) <= 0:
                raise NoiseCharacterizationInputError(
                    "coherence_time_us must be a positive finite "
                    "number or None"
                )
        if total_gate_count is not None:
            if isinstance(total_gate_count, bool) \
                    or not isinstance(total_gate_count, int) \
                    or total_gate_count < 0:
                raise NoiseCharacterizationInputError(
                    "total_gate_count must be a non-negative int or None"
                )
        if gate_time_ns is not None:
            if not isinstance(gate_time_ns, (int, float)) \
                    or isinstance(gate_time_ns, bool) \
                    or not math.isfinite(float(gate_time_ns)) \
                    or float(gate_time_ns) <= 0:
                raise NoiseCharacterizationInputError(
                    "gate_time_ns must be a positive finite number or None"
                )

    def _compute_confidence(
        self,
        *,
        gate_provided: bool,
        readout_provided: bool,
        coherence_provided: bool,
        gate_time_provided: bool,
        total_gate_count_provided: bool,
    ) -> float:
        confidence = CONFIDENCE_BASELINE
        if gate_provided:
            confidence += 0.20
        if readout_provided:
            confidence += 0.15
        if coherence_provided:
            confidence += 0.10
        if total_gate_count_provided:
            confidence += 0.10
        if gate_time_provided:
            confidence += 0.05
        return max(
            CONFIDENCE_FLOOR,
            min(CONFIDENCE_CEILING, confidence),
        )


# =============================================================================
# Helpers
# =============================================================================

def _profile_attr(profile: Any, names: Sequence[str]) -> Optional[float]:
    """Return the first numeric attribute from ``names`` on ``profile``."""
    for name in names:
        v = getattr(profile, name, None)
        if v is None:
            continue
        if isinstance(v, bool):
            continue
        if not isinstance(v, (int, float)):
            continue
        if not math.isfinite(float(v)):
            continue
        return float(v)
    return None


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "CONFIDENCE_BASELINE",
    "CONFIDENCE_CEILING",
    "CONFIDENCE_FLOOR",
    "MetricsRecorder",
    "NoiseCharacterization",
    "NoiseCharacterizationError",
    "NoiseCharacterizationInputError",
    "NoiseConfig",
    "NoiseEstimate",
)


# =============================================================================
# Demo: python -m qcs.execution.noise_characterization
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json

    logging.basicConfig(level=logging.INFO)

    print("=== Happy path ===")
    nc = NoiseCharacterization()
    est = nc.estimate(
        backend_gate_fidelity=0.999,
        backend_readout_fidelity=0.985,
        circuit_depth=100,
        n_qubits=8,
    )
    print(f"  {est!r}")
    print(f"  expected_fidelity:  {est.expected_fidelity:.4f}")
    print(f"  gate_error_rate:    {est.gate_error_rate:.4f}")
    print(f"  readout_error_rate: {est.readout_error_rate:.4f}")
    print(f"  confidence:         {est.confidence:.2f}")
    print(f"  used_defaults:      {sorted(est.used_defaults)}")
    print(f"  checksum:           {est.checksum()[:16]}…")

    print("\n=== §1.1 gate_fidelity=0.0 does NOT become the default ===")
    est_zero = nc.estimate(
        backend_gate_fidelity=0.0,
        backend_readout_fidelity=0.98,
        circuit_depth=10,
        n_qubits=4,
    )
    print(f"  gate_fidelity_used: {est_zero.gate_fidelity_used}")
    print(f"  expected_fidelity:  {est_zero.expected_fidelity}")
    print(f"  used_defaults:      {sorted(est_zero.used_defaults)}")
    assert est_zero.gate_fidelity_used == 0.0
    assert est_zero.expected_fidelity == 0.0
    assert "gate_fidelity" not in est_zero.used_defaults

    print("\n=== §1.1 gate_fidelity=None uses the default (and says so) ===")
    est_none = nc.estimate(
        backend_gate_fidelity=None,
        backend_readout_fidelity=0.98,
        circuit_depth=10,
        n_qubits=4,
    )
    print(f"  gate_fidelity_used: {est_none.gate_fidelity_used}")
    print(f"  used_defaults:      {sorted(est_none.used_defaults)}")
    assert "gate_fidelity" in est_none.used_defaults

    print("\n=== §1.2 Coherence penalty IS now applied ===")
    est_coh = nc.estimate(
        backend_gate_fidelity=0.999,
        backend_readout_fidelity=0.99,
        circuit_depth=100,
        n_qubits=4,
        coherence_time_us=10.0,
        gate_time_ns=100.0,   # 100 gates × 100 ns = 10 µs = 1× coherence
    )
    print(f"  estimated_duration_us: {est_coh.estimated_duration_us}")
    print(f"  coherence_penalty:     {est_coh.coherence_penalty:.4f}")
    print(f"  expected_fidelity:     {est_coh.expected_fidelity:.4f}")
    # Without coherence:
    est_nocoh = nc.estimate(
        backend_gate_fidelity=0.999, backend_readout_fidelity=0.99,
        circuit_depth=100, n_qubits=4,
    )
    print(f"  without coherence penalty: {est_nocoh.expected_fidelity:.4f}")
    assert est_coh.expected_fidelity < est_nocoh.expected_fidelity

    print("\n=== §2.2 total_gate_count is more accurate than depth ===")
    # Same depth, different gate counts → different estimates.
    est_dense = nc.estimate(
        backend_gate_fidelity=0.99,
        backend_readout_fidelity=0.99,
        circuit_depth=8, total_gate_count=28,  # 8-qubit GHZ has ~28 gates
        n_qubits=8,
    )
    est_sparse = nc.estimate(
        backend_gate_fidelity=0.99,
        backend_readout_fidelity=0.99,
        circuit_depth=8, n_qubits=8,
    )
    print(f"  28-gate fidelity:  {est_dense.expected_fidelity:.4f}")
    print(f"  depth-only proxy:  {est_sparse.expected_fidelity:.4f}")
    print(f"  dense notes:       {est_dense.notes}")
    assert est_dense.expected_fidelity < est_sparse.expected_fidelity

    print("\n=== §1.3 needs_fallback vs deprecated exceeds_threshold ===")
    good = nc.estimate(
        backend_gate_fidelity=0.999, backend_readout_fidelity=0.99,
        circuit_depth=10, n_qubits=4,
    )
    bad = nc.estimate(
        backend_gate_fidelity=0.90, backend_readout_fidelity=0.90,
        circuit_depth=200, n_qubits=20,
    )
    print(f"  good → needs_fallback(0.9):  "
          f"{nc.needs_fallback(good, min_fidelity=0.9)}")
    print(f"  bad  → needs_fallback(0.9):  "
          f"{nc.needs_fallback(bad, min_fidelity=0.9)}")
    assert not nc.needs_fallback(good, min_fidelity=0.9)
    assert nc.needs_fallback(bad, min_fidelity=0.9)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = nc.exceeds_threshold(bad, min_fidelity=0.9)
        assert any(
            issubclass(w.category, DeprecationWarning) for w in caught
        )
    print("  exceeds_threshold emitted DeprecationWarning ✓")

    print("\n=== §2.5 min_fidelity validation ===")
    for label, val in [
        ("-0.1", -0.1), ("1.5", 1.5), ("nan", float("nan")),
    ]:
        try:
            nc.needs_fallback(good, min_fidelity=val)
            print(f"  min_fidelity={label}: NOT rejected")
        except NoiseCharacterizationInputError as e:
            print(f"  min_fidelity={label}: {type(e).__name__}")

    print("\n=== §2.1 depth / n_qubits validation ===")
    for label, kwargs in [
        ("negative depth", dict(circuit_depth=-1, n_qubits=4)),
        ("zero qubits", dict(circuit_depth=10, n_qubits=0)),
    ]:
        try:
            nc.estimate(
                backend_gate_fidelity=0.99,
                backend_readout_fidelity=0.98,
                **kwargs,
            )
            print(f"  {label}: NOT rejected")
        except NoiseCharacterizationInputError:
            print(f"  {label}: rejected")

    print("\n=== §2.1 depth=0 skips gate penalty ===")
    est_noop = nc.estimate(
        backend_gate_fidelity=0.5,
        backend_readout_fidelity=1.0,
        circuit_depth=0, n_qubits=1,
    )
    print(f"  no-gate circuit: {est_noop.expected_fidelity}")
    assert est_noop.expected_fidelity == 1.0

    print("\n=== §1.5 Confidence tracks inputs ===")
    for label, kwargs in [
        ("all defaults", dict(
            backend_gate_fidelity=None, backend_readout_fidelity=None,
            circuit_depth=10, n_qubits=4)),
        ("all provided", dict(
            backend_gate_fidelity=0.99, backend_readout_fidelity=0.98,
            circuit_depth=10, n_qubits=4, coherence_time_us=100.0,
            gate_time_ns=100.0, total_gate_count=100)),
    ]:
        est_c = nc.estimate(**kwargs)
        print(f"  {label}: confidence={est_c.confidence:.2f}")

    print("\n=== §2.6 Constructor validation ===")
    for label, kwargs in [
        ("gate fidelity > 1", dict(default_gate_fidelity=1.5)),
        ("gate fidelity 0", dict(default_gate_fidelity=0.0)),
        ("gate fidelity negative", dict(default_gate_fidelity=-0.5)),
        ("gate fidelity nan", dict(default_gate_fidelity=float("nan"))),
    ]:
        try:
            NoiseCharacterization(**kwargs)
            print(f"  {label}: NOT rejected")
        except NoiseCharacterizationInputError:
            print(f"  {label}: rejected")

    print("\n=== §2.3 notes is a tuple ===")
    assert isinstance(est.notes, tuple)
    try:
        est.notes.append("x")  # type: ignore[attr-defined]
    except AttributeError:
        print("  notes.append → AttributeError ✓")

    print("\n=== §2.4 NoiseEstimate is immutable ===")
    try:
        est.expected_fidelity = 0.99  # type: ignore[misc]
    except Exception as e:
        print(f"  expected_fidelity mutation → {type(e).__name__}")

    print("\n=== §3.5 used_defaults distinguishes measured from configured ===")
    print(f"  all-defaults → {sorted(est_none.used_defaults)}")
    print(f"  all-provided → {sorted(est.used_defaults)}")
    assert "gate_fidelity" not in est.used_defaults
    assert "gate_fidelity" in est_none.used_defaults

    print("\n=== §2.7 Readout independence note appears when expected ===")
    readout_note = any(
        "independent" in n for n in est.notes
    )
    print(f"  note present for n_qubits=8: {readout_note}")
    assert readout_note
    single_qubit = nc.estimate(
        backend_gate_fidelity=0.99, backend_readout_fidelity=0.98,
        circuit_depth=10, n_qubits=1,
    )
    assert not any("independent" in n for n in single_qubit.notes)

    print("\n=== §3.3 estimate_from_profile bridge ===")
    class _MockProfile:
        """Stands in for BackendProfile — supports both original and enhanced."""
        backend_id = "mock-qpu"
        effective_gate_fidelity = 0.999
        readout_fidelity = 0.99
        effective_coherence_us = 100.0
    est_prof = nc.estimate_from_profile(
        _MockProfile(),
        circuit_depth=50, n_qubits=4,
        gate_time_ns=100.0,
    )
    print(f"  from profile: {est_prof!r}")
    assert est_prof.gate_fidelity_used == 0.999

    print("\n=== §3.1 Serialization ===")
    d = est.to_dict()
    print(f"  keys: {sorted(d.keys())}")
    print(f"  JSON: {len(json.dumps(d, default=str))} bytes")
    restored = NoiseEstimate.from_dict(d)
    print(f"  round-trip: {restored == est}")
    assert restored == est
    try:
        NoiseEstimate.from_dict({**d, "extra": 1})
    except NoiseCharacterizationInputError:
        print("  strict from_dict rejects unknown fields ✓")

    print("\n=== §3.3 Health ===")
    print(f"  ready: {nc.health()['ready']}")
    print(f"  config: {nc.health()['config']}")

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
