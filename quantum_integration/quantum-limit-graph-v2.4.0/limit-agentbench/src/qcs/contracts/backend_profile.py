"""
BackendProfile — capability description of a quantum backend.

A ``BackendProfile`` is an **immutable contract**: once constructed, its
fields cannot change. Callers that hold a reference (routers, monitors,
schedulers) can rely on it not mutating underneath them.

Use ``with_updates(...)`` / ``with_queue_status(...)`` / ``refreshed_from(...)``
to obtain a new instance with specific fields replaced.

Use ``can_handle(qubits, shots, ...)`` / ``check_handle(requirements)`` to
ask whether this backend can service a request. The returned ``HandleCheck``
carries a boolean, the primary failure reason (if any), the full list of
failures, and a list of constraints that could not be verified from unknown
profile data — so callers can distinguish "definitely can't" from
"probably can, but we don't have enough data to be sure".

The ``online`` field is a **liveness** signal (the backend's API is
reachable). It is not a usability signal — a backend can be ``online=True``
while having a 24-hour queue, degraded fidelity, or exhausted quota. Use
``can_handle(...)`` with the constraints that matter to you.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _coerce_enum(enum_cls: type, value: Any, field_name: str) -> Enum:
    """Accept either an enum member or a string value; validate."""
    if isinstance(value, enum_cls):
        return value
    try:
        return enum_cls(value)
    except ValueError:
        allowed = sorted(e.value for e in enum_cls)
        raise ValueError(
            f"{field_name} must be one of {allowed}; got {value!r}"
        ) from None


def _normalize_region(region: Optional[str]) -> Optional[str]:
    """Normalize to uppercase, hyphen-separated form."""
    if region is None:
        return None
    if not isinstance(region, str):
        raise ValueError(
            f"region must be a string or None; got {type(region).__name__}"
        )
    cleaned = region.strip().upper().replace("_", "-").replace(" ", "-")
    return cleaned or None


def _freeze_mapping(m: Any) -> Mapping[str, Any]:
    if m is None:
        return MappingProxyType({})
    if isinstance(m, MappingProxyType):
        return m
    if not isinstance(m, Mapping):
        raise TypeError(
            f"metadata must be a Mapping; got {type(m).__name__}"
        )
    return MappingProxyType(dict(m))


def _to_jsonable(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


# =============================================================================
# Enums
# =============================================================================

class BackendKind(str, Enum):
    """
    Category of the backend, used for queue and cost modeling.

    - ``SIMULATOR``: local, no queue, no cost, deterministic.
    - ``CLOUD_SIMULATOR``: cloud-hosted simulator; queues and costs apply.
    - ``EMULATOR``: noisy simulator that models a specific hardware device.
    - ``HARDWARE``: real quantum hardware.
    """

    SIMULATOR = "simulator"
    HARDWARE = "hardware"
    EMULATOR = "emulator"
    CLOUD_SIMULATOR = "cloud_simulator"


class QubitTechnology(str, Enum):
    """Underlying physical (or simulated) qubit technology."""

    SUPERCONDUCTING = "superconducting"
    TRAPPED_ION = "trapped_ion"
    PHOTONIC = "photonic"
    NEUTRAL_ATOM = "neutral_atom"
    SIMULATED = "simulated"
    UNKNOWN = "unknown"


# =============================================================================
# HandleCheck — rich result of capability checks
# =============================================================================

@dataclass(frozen=True, eq=False)
class HandleCheck:
    """
    Outcome of ``can_handle`` / ``check_handle``.

    ``bool(check)`` matches ``check.can_handle``, so callers doing
    ``if profile.can_handle(...)`` continue to work unchanged.

    ``== True`` / ``== False`` also work for callers that compared to bool.
    """

    can_handle: bool
    reason: str = "ok"
    failures: Tuple[str, ...] = ()
    # Constraints that could not be verified because the profile field is
    # unknown (e.g. queue depth is None). Not treated as failures.
    unknown: Tuple[str, ...] = ()

    def __bool__(self) -> bool:
        return self.can_handle

    def __eq__(self, other: object) -> bool:
        if isinstance(other, bool):
            return self.can_handle == other
        if isinstance(other, HandleCheck):
            return (
                self.can_handle, self.reason,
                self.failures, self.unknown,
            ) == (
                other.can_handle, other.reason,
                other.failures, other.unknown,
            )
        return NotImplemented

    def __hash__(self) -> int:
        return hash(
            (self.can_handle, self.reason, self.failures, self.unknown)
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "can_handle": self.can_handle,
            "reason": self.reason,
            "failures": list(self.failures),
            "unknown": list(self.unknown),
        }


# =============================================================================
# BackendRequirements — a request spec
# =============================================================================

@dataclass(frozen=True)
class BackendRequirements:
    """
    A request spec that a ``BackendProfile`` is checked against.

    ``qubits`` and ``shots`` are required; the rest default to "no
    constraint". Fields set to ``None`` mean "I don't care", NOT "must
    match None".
    """

    qubits: int
    shots: int

    # --- Fidelity and coherence ---
    min_gate_fidelity: Optional[float] = None
    min_readout_fidelity: Optional[float] = None
    min_coherence_us: Optional[float] = None

    # --- Queue and cost ---
    max_queue_seconds: Optional[float] = None
    max_cost_usd: Optional[float] = None
    max_energy_kwh: Optional[float] = None

    # --- Region / hardware ---
    allowed_regions: Optional[Tuple[str, ...]] = None
    requires_hardware: bool = False

    # --- Circuit-specific ---
    required_gates: Tuple[str, ...] = ()
    max_circuit_depth: Optional[int] = None

    def __post_init__(self) -> None:
        if self.allowed_regions is not None and \
                not isinstance(self.allowed_regions, tuple):
            object.__setattr__(
                self, "allowed_regions",
                tuple(self.allowed_regions),
            )
        if not isinstance(self.required_gates, tuple):
            object.__setattr__(
                self, "required_gates", tuple(self.required_gates),
            )


# =============================================================================
# BackendProfile
# =============================================================================

@dataclass(frozen=True)
class BackendProfile:
    """
    Immutable capability description of a quantum backend.

    Construction validates every field; invalid inputs raise ``ValueError``
    (not silent corruption downstream). ``kind`` and ``technology`` accept
    either enum members or their string values — strings are coerced and
    validated.

    Every modification must go through a copy method
    (``with_updates`` / ``with_queue_status`` / ``refreshed_from``).
    """

    # --- Identity ---
    backend_id: str
    kind: BackendKind
    technology: QubitTechnology = QubitTechnology.UNKNOWN
    vendor: Optional[str] = None
    region: Optional[str] = None

    # --- Capacity ---
    num_qubits: int = 0
    max_shots_per_job: int = 100_000
    max_circuit_depth: Optional[int] = None
    supported_gates: Tuple[str, ...] = ()
    connectivity: Optional[str] = None
    native_two_qubit_gate: Optional[str] = None

    # --- Dynamic status ---
    online: bool = True
    queue_depth: Optional[int] = None
    avg_queue_seconds: Optional[float] = None
    last_refreshed_at: Optional[datetime] = None

    # --- Fidelity ---
    # Deprecated: use gate_fidelity_1q / gate_fidelity_2q where possible.
    gate_fidelity: Optional[float] = None
    gate_fidelity_1q: Optional[float] = None
    gate_fidelity_2q: Optional[float] = None
    readout_fidelity: Optional[float] = None

    # --- Coherence ---
    # Deprecated: use t1/t2.
    coherence_time_us: Optional[float] = None
    t1_coherence_us: Optional[float] = None
    t2_coherence_us: Optional[float] = None

    # --- Cost / energy ---
    cost_per_shot: float = 0.0
    energy_kwh_per_shot: float = 0.0

    # --- Free-form ---
    notes: Tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------ init
    def __post_init__(self) -> None:
        # --- Coerce enums ---
        object.__setattr__(
            self, "kind",
            _coerce_enum(BackendKind, self.kind, "kind"),
        )
        object.__setattr__(
            self, "technology",
            _coerce_enum(QubitTechnology, self.technology, "technology"),
        )

        # --- Identity ---
        if not isinstance(self.backend_id, str) or not self.backend_id.strip():
            raise ValueError(
                "backend_id must be a non-empty string"
            )

        # --- Integers ---
        if not isinstance(self.num_qubits, int) or self.num_qubits < 0:
            raise ValueError(
                f"num_qubits must be a non-negative int; got {self.num_qubits!r}"
            )
        if not isinstance(self.max_shots_per_job, int) or self.max_shots_per_job <= 0:
            raise ValueError(
                "max_shots_per_job must be a positive int"
            )
        if self.queue_depth is not None:
            if not isinstance(self.queue_depth, int) or self.queue_depth < 0:
                raise ValueError(
                    "queue_depth must be a non-negative int or None"
                )
        if self.max_circuit_depth is not None:
            if not isinstance(self.max_circuit_depth, int) or self.max_circuit_depth <= 0:
                raise ValueError(
                    "max_circuit_depth must be a positive int or None"
                )

        # --- Fidelity in [0, 1] ---
        for fname in (
            "gate_fidelity", "gate_fidelity_1q",
            "gate_fidelity_2q", "readout_fidelity",
        ):
            v = getattr(self, fname)
            if v is not None:
                if not isinstance(v, (int, float)):
                    raise ValueError(
                        f"{fname} must be a number or None; got {type(v).__name__}"
                    )
                if not (0.0 <= float(v) <= 1.0):
                    raise ValueError(
                        f"{fname} must be in [0, 1] or None; got {v!r}"
                    )

        # --- Non-negative floats ---
        if self.avg_queue_seconds is not None:
            if not isinstance(self.avg_queue_seconds, (int, float)):
                raise ValueError(
                    "avg_queue_seconds must be a number or None"
                )
            if self.avg_queue_seconds < 0:
                raise ValueError(
                    "avg_queue_seconds must be >= 0 or None"
                )
        for fname in ("cost_per_shot", "energy_kwh_per_shot"):
            v = getattr(self, fname)
            if not isinstance(v, (int, float)):
                raise ValueError(f"{fname} must be a number")
            if v < 0:
                raise ValueError(f"{fname} must be >= 0")

        # --- Coherence: positive or None ---
        for fname in (
            "coherence_time_us", "t1_coherence_us", "t2_coherence_us",
        ):
            v = getattr(self, fname)
            if v is not None:
                if not isinstance(v, (int, float)):
                    raise ValueError(f"{fname} must be a number or None")
                if v <= 0:
                    raise ValueError(f"{fname} must be > 0 or None")

        # --- Cross-field: hardware cannot be simulated ---
        if self.kind == BackendKind.HARDWARE and \
                self.technology == QubitTechnology.SIMULATED:
            raise ValueError(
                "kind=hardware cannot have technology=simulated"
            )

        # --- Region normalization ---
        object.__setattr__(
            self, "region", _normalize_region(self.region),
        )

        # --- Freeze sequences / metadata ---
        if not isinstance(self.notes, tuple):
            object.__setattr__(self, "notes", tuple(self.notes))
        if not isinstance(self.supported_gates, tuple):
            object.__setattr__(
                self, "supported_gates", tuple(self.supported_gates),
            )
        if not isinstance(self.metadata, MappingProxyType):
            object.__setattr__(
                self, "metadata", _freeze_mapping(self.metadata),
            )

        # --- last_refreshed_at → UTC ---
        if self.last_refreshed_at is not None:
            object.__setattr__(
                self, "last_refreshed_at",
                _ensure_utc(self.last_refreshed_at),
            )

    # ------------------------------------------------------------------ properties
    @property
    def effective_gate_fidelity(self) -> Optional[float]:
        """Best gate fidelity available (prefers 2q if present — worst case)."""
        if self.gate_fidelity_2q is not None:
            return self.gate_fidelity_2q
        if self.gate_fidelity is not None:
            return self.gate_fidelity
        return None

    @property
    def effective_coherence_us(self) -> Optional[float]:
        """Best coherence time available (prefers t2 — depth-limiting)."""
        if self.t2_coherence_us is not None:
            return self.t2_coherence_us
        if self.coherence_time_us is not None:
            return self.coherence_time_us
        return None

    # ------------------------------------------------------------------ estimates
    def estimate_cost_usd(self, shots: int) -> float:
        """Upper bound on cost for a job of ``shots`` shots."""
        if shots < 0:
            raise ValueError("shots must be >= 0")
        return float(shots) * float(self.cost_per_shot)

    def estimate_energy_kwh(self, shots: int) -> float:
        """Upper bound on energy for a job of ``shots`` shots."""
        if shots < 0:
            raise ValueError("shots must be >= 0")
        return float(shots) * float(self.energy_kwh_per_shot)

    def estimated_circuit_fidelity(
        self, n_1q_gates: int, n_2q_gates: int,
    ) -> Optional[float]:
        """
        Rough upper bound on circuit fidelity: f1^n1 * f2^n2.

        Returns ``None`` if the required fidelity data is missing. This
        ignores cross-talk, state-prep error, and readout error
        accumulation — treat it as an order-of-magnitude guide only.
        """
        if n_1q_gates < 0 or n_2q_gates < 0:
            raise ValueError("gate counts must be >= 0")
        f1 = (self.gate_fidelity_1q
              if self.gate_fidelity_1q is not None
              else self.gate_fidelity)
        f2 = (self.gate_fidelity_2q
              if self.gate_fidelity_2q is not None
              else self.gate_fidelity)
        if f1 is None or f2 is None:
            return None
        return (f1 ** n_1q_gates) * (f2 ** n_2q_gates)

    # ------------------------------------------------------------------ freshness
    def is_stale(self, max_age_seconds: float = 300.0) -> bool:
        if max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be > 0")
        if self.last_refreshed_at is None:
            return True
        age = (_utcnow() - self.last_refreshed_at).total_seconds()
        return age > max_age_seconds

    # ------------------------------------------------------------------ capability
    def can_handle(
        self,
        required_qubits: int,
        required_shots: int,
        *,
        min_gate_fidelity: Optional[float] = None,
        min_readout_fidelity: Optional[float] = None,
        min_coherence_us: Optional[float] = None,
        max_queue_seconds: Optional[float] = None,
        max_cost_usd: Optional[float] = None,
        max_energy_kwh: Optional[float] = None,
        allowed_regions: Optional[Sequence[str]] = None,
        requires_hardware: bool = False,
        required_gates: Optional[Sequence[str]] = None,
        max_circuit_depth: Optional[int] = None,
    ) -> HandleCheck:
        """
        Check whether this backend can service a request.

        The old two-argument ``can_handle(q, s)`` form returns a
        ``HandleCheck`` whose ``bool()`` matches the previous semantics:
        True iff ``online and num_qubits >= q and max_shots_per_job >= s``.
        The richer form evaluates additional constraints and reports
        those it could not verify in ``HandleCheck.unknown``.
        """
        req = BackendRequirements(
            qubits=int(required_qubits),
            shots=int(required_shots),
            min_gate_fidelity=min_gate_fidelity,
            min_readout_fidelity=min_readout_fidelity,
            min_coherence_us=min_coherence_us,
            max_queue_seconds=max_queue_seconds,
            max_cost_usd=max_cost_usd,
            max_energy_kwh=max_energy_kwh,
            allowed_regions=(
                tuple(allowed_regions) if allowed_regions else None
            ),
            requires_hardware=bool(requires_hardware),
            required_gates=(
                tuple(required_gates) if required_gates else ()
            ),
            max_circuit_depth=max_circuit_depth,
        )
        return self.check_handle(req)

    def check_handle(self, req: BackendRequirements) -> HandleCheck:
        """Rich capability check against a ``BackendRequirements``."""
        failures: List[str] = []
        unknown: List[str] = []

        # --- Always-checked core constraints ---
        if req.qubits < 0:
            failures.append(f"invalid:required_qubits:{req.qubits}")
        if req.shots < 0:
            failures.append(f"invalid:required_shots:{req.shots}")
        if not self.online:
            failures.append("offline")
        if self.num_qubits < req.qubits:
            failures.append(
                f"insufficient_qubits:{self.num_qubits}<{req.qubits}"
            )
        if self.max_shots_per_job < req.shots:
            failures.append(
                f"insufficient_shots:{self.max_shots_per_job}<{req.shots}"
            )

        # --- Hardware requirement ---
        if req.requires_hardware and self.kind != BackendKind.HARDWARE:
            failures.append(f"hardware_required:{self.kind.value}")

        # --- Gate fidelity ---
        if req.min_gate_fidelity is not None:
            eff = self.effective_gate_fidelity
            if eff is None:
                unknown.append("gate_fidelity_unknown")
            elif eff < req.min_gate_fidelity:
                failures.append(
                    f"low_gate_fidelity:{eff:.3f}"
                    f"<{req.min_gate_fidelity:.3f}"
                )

        # --- Readout fidelity ---
        if req.min_readout_fidelity is not None:
            if self.readout_fidelity is None:
                unknown.append("readout_fidelity_unknown")
            elif self.readout_fidelity < req.min_readout_fidelity:
                failures.append(
                    f"low_readout_fidelity:{self.readout_fidelity:.3f}"
                    f"<{req.min_readout_fidelity:.3f}"
                )

        # --- Coherence ---
        if req.min_coherence_us is not None:
            eff_coh = self.effective_coherence_us
            if eff_coh is None:
                unknown.append("coherence_unknown")
            elif eff_coh < req.min_coherence_us:
                failures.append(
                    f"short_coherence:{eff_coh:.1f}us"
                    f"<{req.min_coherence_us:.1f}us"
                )

        # --- Queue ---
        if req.max_queue_seconds is not None:
            if self.avg_queue_seconds is None:
                unknown.append("queue_unknown")
            elif self.avg_queue_seconds > req.max_queue_seconds:
                failures.append(
                    f"queue_too_long:{self.avg_queue_seconds:.1f}s"
                    f">{req.max_queue_seconds:.1f}s"
                )

        # --- Cost ---
        if req.max_cost_usd is not None:
            est = self.estimate_cost_usd(req.shots)
            if est > req.max_cost_usd:
                failures.append(
                    f"cost_over_budget:${est:.4f}>${req.max_cost_usd:.4f}"
                )

        # --- Energy ---
        if req.max_energy_kwh is not None:
            est = self.estimate_energy_kwh(req.shots)
            if est > req.max_energy_kwh:
                failures.append(
                    f"energy_over_budget:{est:.4f}>{req.max_energy_kwh:.4f}"
                )

        # --- Region ---
        if req.allowed_regions is not None:
            if self.region is None:
                unknown.append("region_unknown")
            elif self.region not in req.allowed_regions:
                failures.append(f"region_not_allowed:{self.region}")

        # --- Required gates ---
        if req.required_gates:
            missing = [
                g for g in req.required_gates
                if g not in self.supported_gates
            ]
            if missing:
                failures.append(f"missing_gates:{','.join(missing)}")

        # --- Max circuit depth ---
        if req.max_circuit_depth is not None:
            if self.max_circuit_depth is None:
                unknown.append("circuit_depth_unknown")
            elif self.max_circuit_depth < req.max_circuit_depth:
                failures.append(
                    f"insufficient_circuit_depth:"
                    f"{self.max_circuit_depth}<{req.max_circuit_depth}"
                )

        return HandleCheck(
            can_handle=not failures,
            reason="ok" if not failures else failures[0],
            failures=tuple(failures),
            unknown=tuple(unknown),
        )

    # ------------------------------------------------------------------ copy helpers
    def with_updates(self, **changes: Any) -> "BackendProfile":
        """Return a copy with the given fields replaced and re-validated."""
        return replace(self, **changes)

    def with_queue_status(
        self,
        queue_depth: Optional[int],
        avg_queue_seconds: Optional[float],
        *,
        online: Optional[bool] = None,
        refreshed_at: Optional[datetime] = None,
    ) -> "BackendProfile":
        """Update dynamic queue / liveness fields; keeps static fields."""
        return replace(
            self,
            queue_depth=queue_depth,
            avg_queue_seconds=avg_queue_seconds,
            online=self.online if online is None else online,
            last_refreshed_at=(
                refreshed_at if refreshed_at is not None else _utcnow()
            ),
        )

    def refreshed_from(self, other: "BackendProfile") -> "BackendProfile":
        """
        Return a copy with dynamic fields updated from ``other``.

        Static fields (technology, vendor, region, num_qubits,
        supported_gates, connectivity, ...) are kept from ``self``.
        Dynamic fields (queue, liveness, fidelity, coherence) are taken
        from ``other``, falling back to ``self`` when ``other`` has None.
        """
        if other.backend_id != self.backend_id:
            raise ValueError(
                f"backend_id mismatch: {self.backend_id!r} vs "
                f"{other.backend_id!r}"
            )
        return replace(
            self,
            online=other.online,
            queue_depth=(
                other.queue_depth
                if other.queue_depth is not None else self.queue_depth
            ),
            avg_queue_seconds=(
                other.avg_queue_seconds
                if other.avg_queue_seconds is not None
                else self.avg_queue_seconds
            ),
            gate_fidelity=(
                other.gate_fidelity
                if other.gate_fidelity is not None
                else self.gate_fidelity
            ),
            gate_fidelity_1q=(
                other.gate_fidelity_1q
                if other.gate_fidelity_1q is not None
                else self.gate_fidelity_1q
            ),
            gate_fidelity_2q=(
                other.gate_fidelity_2q
                if other.gate_fidelity_2q is not None
                else self.gate_fidelity_2q
            ),
            readout_fidelity=(
                other.readout_fidelity
                if other.readout_fidelity is not None
                else self.readout_fidelity
            ),
            t1_coherence_us=(
                other.t1_coherence_us
                if other.t1_coherence_us is not None
                else self.t1_coherence_us
            ),
            t2_coherence_us=(
                other.t2_coherence_us
                if other.t2_coherence_us is not None
                else self.t2_coherence_us
            ),
            last_refreshed_at=(
                other.last_refreshed_at
                if other.last_refreshed_at is not None
                else _utcnow()
            ),
        )

    # ------------------------------------------------------------------ consistency
    def validate_consistency(self) -> List[str]:
        """
        Return a list of soft warnings (empty on success).

        Unlike ``__post_init__``, this does not raise — it surfaces
        suspicious-but-not-impossible combinations.
        """
        warnings_: List[str] = []

        if self.kind == BackendKind.HARDWARE:
            if self.vendor is None:
                warnings_.append("hardware_backend_without_vendor")
            if self.region is None:
                warnings_.append("hardware_backend_without_region")
            if self.gate_fidelity is None \
                    and self.gate_fidelity_2q is None:
                warnings_.append("hardware_backend_without_fidelity")

        if self.queue_depth is not None \
                and self.queue_depth > 0 \
                and self.avg_queue_seconds is not None \
                and self.avg_queue_seconds == 0.0:
            warnings_.append("queue_depth_positive_but_zero_wait")

        if self.gate_fidelity_1q is not None \
                and self.gate_fidelity_2q is not None \
                and self.gate_fidelity_2q > self.gate_fidelity_1q:
            warnings_.append("2q_fidelity_exceeds_1q_unusual")

        if self.kind == BackendKind.SIMULATOR \
                and self.technology != QubitTechnology.SIMULATED:
            warnings_.append(
                f"simulator_with_non_simulated_technology:"
                f"{self.technology.value}"
            )

        if self.online and self.num_qubits == 0 \
                and self.kind == BackendKind.HARDWARE:
            warnings_.append("online_hardware_with_zero_qubits")

        return warnings_

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        """JSON-friendly dict — enums as strings, tuples as lists."""
        return {
            "backend_id": self.backend_id,
            "kind": self.kind.value,
            "technology": self.technology.value,
            "vendor": self.vendor,
            "region": self.region,
            "num_qubits": self.num_qubits,
            "max_shots_per_job": self.max_shots_per_job,
            "max_circuit_depth": self.max_circuit_depth,
            "supported_gates": list(self.supported_gates),
            "connectivity": self.connectivity,
            "native_two_qubit_gate": self.native_two_qubit_gate,
            "online": self.online,
            "queue_depth": self.queue_depth,
            "avg_queue_seconds": self.avg_queue_seconds,
            "last_refreshed_at": (
                self.last_refreshed_at.isoformat()
                if self.last_refreshed_at else None
            ),
            "gate_fidelity": self.gate_fidelity,
            "gate_fidelity_1q": self.gate_fidelity_1q,
            "gate_fidelity_2q": self.gate_fidelity_2q,
            "readout_fidelity": self.readout_fidelity,
            "coherence_time_us": self.coherence_time_us,
            "t1_coherence_us": self.t1_coherence_us,
            "t2_coherence_us": self.t2_coherence_us,
            "cost_per_shot": self.cost_per_shot,
            "energy_kwh_per_shot": self.energy_kwh_per_shot,
            "notes": list(self.notes),
            "metadata": _to_jsonable(self.metadata),
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "BackendProfile":
        """Construct from ``to_dict()`` output. Rejects unknown fields."""
        known = {f.name for f in fields(cls)}
        unknown = set(d.keys()) - known
        if unknown:
            raise ValueError(
                f"unknown fields in BackendProfile dict: {sorted(unknown)}"
            )
        cleaned = dict(d)
        if isinstance(cleaned.get("last_refreshed_at"), str):
            cleaned["last_refreshed_at"] = datetime.fromisoformat(
                cleaned["last_refreshed_at"]
            )
        for fname in ("notes", "supported_gates"):
            v = cleaned.get(fname)
            if v is not None and not isinstance(v, tuple):
                cleaned[fname] = tuple(v)
        return cls(**cleaned)

    # ------------------------------------------------------------------ repr
    def __repr__(self) -> str:
        return (
            f"BackendProfile(id={self.backend_id!r}, "
            f"kind={self.kind.value}, "
            f"tech={self.technology.value}, "
            f"qubits={self.num_qubits}, "
            f"online={self.online})"
        )


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "BackendKind",
    "BackendProfile",
    "BackendRequirements",
    "HandleCheck",
    "QubitTechnology",
)


# =============================================================================
# Smoke test: python -m qcs.contracts.backend_profile
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    print("=== Construction & validation ===")
    profile = BackendProfile(
        backend_id="ibmq_quito",
        kind="hardware",  # string accepted, coerced
        technology="superconducting",
        num_qubits=5,
        max_shots_per_job=20_000,
        vendor="IBM",
        region="us-ca",  # normalized to US-CA
        gate_fidelity_1q=0.9995,
        gate_fidelity_2q=0.985,
        readout_fidelity=0.97,
        t2_coherence_us=80.0,
        cost_per_shot=0.00035,
        energy_kwh_per_shot=0.000002,
        supported_gates=("cx", "u1", "u2", "u3", "id", "x"),
        max_circuit_depth=200,
        connectivity="heavy-hex",
    )
    print(f"  {profile!r}")
    print(f"  kind enum:       {profile.kind}  ({type(profile.kind).__name__})")
    print(f"  region norm:     {profile.region}")
    print(f"  fidelity(2q):    {profile.effective_gate_fidelity}")
    print(f"  coherence(t2):   {profile.effective_coherence_us} us")

    print("\n=== Immutability ===")
    try:
        profile.num_qubits = 0  # type: ignore[misc]
    except Exception as e:
        print(f"  num_qubits mutation → {type(e).__name__}")
    try:
        profile.notes = ("x",)  # type: ignore[misc]
    except Exception as e:
        print(f"  notes mutation     → {type(e).__name__}")

    print("\n=== Validation errors ===")
    for label, kwargs in [
        ("negative qubits", {"backend_id": "x", "kind": "simulator", "num_qubits": -1}),
        ("bad fidelity",    {"backend_id": "x", "kind": "simulator", "gate_fidelity_1q": 1.5}),
        ("bad kind",        {"backend_id": "x", "kind": "hardware_"}),
        ("hw + simulated",  {"backend_id": "x", "kind": "hardware", "technology": "simulated"}),
        ("negative cost",   {"backend_id": "x", "kind": "simulator", "cost_per_shot": -1}),
    ]:
        try:
            BackendProfile(**kwargs)
            print(f"  {label}: NOT rejected")
        except ValueError as e:
            print(f"  {label}: {e}")

    print("\n=== can_handle ===")
    req_ok = profile.can_handle(4, 1000)
    print(f"  basic(4,1000):  can_handle={req_ok.can_handle}  "
          f"reason={req_ok.reason}")
    assert bool(req_ok) is True
    assert req_ok == True  # backward-compat

    req_fail = profile.can_handle(10, 1000)
    print(f"  basic(10,1000): can_handle={req_fail.can_handle}  "
          f"reason={req_fail.reason}")
    assert bool(req_fail) is False
    assert req_fail == False

    req_fidelity = profile.can_handle(
        4, 1000, min_gate_fidelity=0.999,
    )
    print(f"  fidelity>=0.999:{req_fidelity.can_handle}  "
          f"reason={req_fidelity.reason}")

    req_cost = profile.can_handle(
        4, 1000, max_cost_usd=0.0001,
    )
    print(f"  cost<=0.0001:   {req_cost.can_handle}  "
          f"reason={req_cost.reason}")

    req_region = profile.can_handle(
        4, 1000, allowed_regions=("EU-DE",),
    )
    print(f"  region EU-DE:   {req_region.can_handle}  "
          f"reason={req_region.reason}")

    req_hw = BackendProfile(
        backend_id="sim1", kind="simulator", num_qubits=20,
    ).can_handle(4, 1000, requires_hardware=True)
    print(f"  requires_hw:    {req_hw.can_handle}  reason={req_hw.reason}")

    print("\n=== Unknowns (profile lacks queue data) ===")
    bare = BackendProfile(
        backend_id="bare", kind="hardware", technology="trapped_ion",
        num_qubits=11, max_shots_per_job=100_000, vendor="IonQ",
        region="US-MD",
    )
    u = bare.can_handle(
        4, 1000, max_queue_seconds=60.0,
    )
    print(f"  can_handle: {u.can_handle}  "
          f"failures={u.failures}  unknown={u.unknown}")

    print("\n=== Estimates ===")
    print(f"  cost 10k shots:    ${profile.estimate_cost_usd(10_000):.4f}")
    print(f"  energy 10k shots:  "
          f"{profile.estimate_energy_kwh(10_000):.6f} kWh")
    print(f"  circuit fidelity (100×1Q, 50×2Q): "
          f"{profile.estimated_circuit_fidelity(100, 50):.4f}")

    print("\n=== Copy methods ===")
    refreshed = profile.with_queue_status(
        queue_depth=42, avg_queue_seconds=180.0,
    )
    print(f"  with_queue_status → {refreshed!r}  "
          f"(original untouched: queue={profile.queue_depth})")
    assert profile.queue_depth is None
    assert refreshed.queue_depth == 42

    print(f"  is_stale (original):  {profile.is_stale()}")
    print(f"  is_stale (refreshed): {refreshed.is_stale()}")

    print("\n=== refreshed_from ===")
    newer = BackendProfile(
        backend_id="ibmq_quito",
        kind="hardware",
        technology="superconducting",
        num_qubits=5,
        max_shots_per_job=20_000,
        vendor="IBM",
        region="US-CA",
        gate_fidelity_2q=0.97,      # degraded
        queue_depth=500,
        avg_queue_seconds=2400.0,
    )
    merged = profile.refreshed_from(newer)
    print(f"  static kept:   vendor={merged.vendor}, tech={merged.technology.value}")
    print(f"  dynamic merged: queue={merged.queue_depth}, "
          f"fidelity2q={merged.gate_fidelity_2q}")

    print("\n=== Consistency warnings ===")
    hw_no_vendor = BackendProfile(
        backend_id="h1", kind="hardware", technology="superconducting",
        num_qubits=5,
    )
    print(f"  warnings: {hw_no_vendor.validate_consistency()}")

    print("\n=== Serialization round-trip ===")
    d = profile.to_dict()
    import json
    print(f"  to_dict JSON-safe: {json.dumps(d)[:80]}…")
    restored = BackendProfile.from_dict(d)
    assert restored == profile, "round-trip mismatch"
    print(f"  round-trip:        OK")

    try:
        BackendProfile.from_dict({**d, "extra_field": 1})
    except ValueError as e:
        print(f"  strict from_dict:  {e}")

    print("\n=== __all__ / __version__ ===")
    print(f"  version: {__version__}")
    print(f"  exports: {__all__}")

    print("\nSmoke test passed.")
