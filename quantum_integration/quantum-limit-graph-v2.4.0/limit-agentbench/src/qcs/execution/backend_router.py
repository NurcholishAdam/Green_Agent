"""
BackendRouter — select the best backend for a quantum job.

Given a set of registered ``BackendProfile`` objects and a routing
request (qubits, shots, preference, budget, region constraints), the
router:

  1. Filters by capability (delegating to ``BackendProfile.check_handle``
     when available, ``can_handle`` otherwise).
  2. Filters by preference (simulator / hardware / auto).
  3. Filters by queue, cost, region, and staleness.
  4. Scores remaining candidates on a configurable linear combination
     of fidelity, queue, and cost.
  5. Returns the winner with its full ``HandleCheck``, the unknowns the
     profile couldn't verify, and a structured rationale.

A request that matches no candidate returns
``fallback_to_classical=True`` with a typed ``RoutingRejectionReason``
that distinguishes "no simulators registered" from "all simulators
failed cost" — the caller can act accordingly.

Original API preserved:
    router = BackendRouter()
    router.register(profile)
    decision = router.select(
        required_qubits=4, required_shots=1000, preference="auto",
    )
    decision.backend_id / decision.rationale /
    decision.rejected / decision.fallback_to_classical

v2 fixes (see changelog in commit message):
  * No crash on ``BackendProfile.avg_queue_seconds=None``.
  * Uses ``BackendProfile.check_handle(BackendRequirements)`` when
    available, gaining fidelity, coherence, circuit-depth, gate, and
    region filtering.
  * Uses ``effective_gate_fidelity`` for scoring (2q-aware).
  * ``max_cost_usd=0`` now means "zero dollars" (was "unlimited").
  * ``max_cost_per_job`` constructor arg is now wired to the default.
  * Deterministic tie-breaking.
  * Frozen, typed ``RoutingDecision``; rejected entries are typed
    ``RejectedBackend`` objects with ``__getitem__`` dict-compat.
  * ``BackendRouterConfig`` for all tuning constants.
  * Thread-safe; bounded registry; ``unregister`` / ``clear`` / ``get``.
  * ``unknowns``, ``score``, ``stale``, ``requirements``,
    ``handle_check``, and ``reason`` surfaced on the decision.
  * Structured rationale; ``to_dict`` / ``checksum``.
  * ``health()`` for readiness probes.
  * ``__version__``, ``__all__``, demo.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field, fields, replace
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
    Union,
)

from ..contracts.backend_profile import BackendKind, BackendProfile
from ..contracts.execution_request import BackendPreference

# Optional imports — the enhanced BackendProfile exposes these; the
# original does not. We tolerate both.
try:
    from ..contracts.backend_profile import (  # type: ignore[attr-defined]
        BackendRequirements,
        HandleCheck,
    )
    _HAS_BACKEND_REQUIREMENTS = True
except ImportError:  # pragma: no cover — original contract
    BackendRequirements = None  # type: ignore[assignment]
    HandleCheck = None  # type: ignore[assignment]
    _HAS_BACKEND_REQUIREMENTS = False

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Constants
# =============================================================================

MAX_REGISTRY_SIZE: int = 10_000
DEFAULT_QUEUE_SCALE_SECONDS: float = 600.0
DEFAULT_COST_SCALE_USD_PER_SHOT: float = 0.01
DEFAULT_SIMULATOR_BONUS: float = 0.1
DEFAULT_UNKNOWN_FIDELITY: float = 0.5
DEFAULT_PROFILE_MAX_AGE_SECONDS: float = 3600.0


# =============================================================================
# Errors
# =============================================================================

class RoutingError(Exception):
    """Base class for BackendRouter errors."""


class RoutingInputError(RoutingError, ValueError):
    """Invalid inputs to ``select`` or the constructor."""


# =============================================================================
# Enums
# =============================================================================

class RoutingRejectionReason(str, Enum):
    """Typed reason codes for rejected candidates."""

    INSUFFICIENT_CAPABILITIES = "insufficient_capabilities"
    INSUFFICIENT_QUBITS = "insufficient_qubits"
    INSUFFICIENT_SHOTS = "insufficient_shots"
    INSUFFICIENT_FIDELITY = "insufficient_fidelity"
    INSUFFICIENT_COHERENCE = "insufficient_coherence"
    INSUFFICIENT_CIRCUIT_DEPTH = "insufficient_circuit_depth"
    MISSING_GATES = "missing_gates"

    OFFLINE = "offline"
    QUEUE_UNKNOWN = "queue_unknown"
    QUEUE_TOO_LONG = "queue_too_long"
    COST_OVER_BUDGET = "cost_over_budget"
    PREFERENCE_MISMATCH = "preference_mismatch"
    REGION_NOT_ALLOWED = "region_not_allowed"
    STALE_PROFILE = "stale_profile"
    LOWER_SCORE = "lower_score"
    OTHER = "other"


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
class BackendRouterConfig:
    """All tunable constants for ``BackendRouter``."""

    # --- Preference / scoring ---
    prefer_simulator: bool = True
    simulator_bonus: float = DEFAULT_SIMULATOR_BONUS
    queue_scale_seconds: float = DEFAULT_QUEUE_SCALE_SECONDS
    cost_scale_usd_per_shot: float = DEFAULT_COST_SCALE_USD_PER_SHOT
    unknown_fidelity_value: float = DEFAULT_UNKNOWN_FIDELITY

    # --- Cost ---
    # Default max cost per job in USD; None = no default cap. Callers
    # can override via select(max_cost_usd=...).
    max_cost_per_job_usd: Optional[float] = None

    # --- Freshness ---
    require_fresh: bool = False
    max_profile_age_seconds: float = DEFAULT_PROFILE_MAX_AGE_SECONDS

    # --- Registry ---
    max_registry_size: int = MAX_REGISTRY_SIZE

    def __post_init__(self) -> None:
        for fname in (
            "simulator_bonus", "queue_scale_seconds",
            "cost_scale_usd_per_shot", "unknown_fidelity_value",
            "max_profile_age_seconds",
        ):
            v = getattr(self, fname)
            if not isinstance(v, (int, float)) \
                    or isinstance(v, bool) \
                    or not math.isfinite(float(v)):
                raise RoutingInputError(
                    f"{fname} must be a finite number"
                )
        if self.queue_scale_seconds <= 0:
            raise RoutingInputError(
                "queue_scale_seconds must be > 0"
            )
        if self.cost_scale_usd_per_shot <= 0:
            raise RoutingInputError(
                "cost_scale_usd_per_shot must be > 0"
            )
        if not (0.0 <= self.unknown_fidelity_value <= 1.0):
            raise RoutingInputError(
                "unknown_fidelity_value must be in [0, 1]"
            )
        if self.max_cost_per_job_usd is not None:
            v = self.max_cost_per_job_usd
            if not isinstance(v, (int, float)) or v < 0:
                raise RoutingInputError(
                    "max_cost_per_job_usd must be >= 0 or None"
                )
        if self.max_registry_size <= 0:
            raise RoutingInputError("max_registry_size must be > 0")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Rejected candidate record
# =============================================================================

@dataclass(frozen=True)
class RejectedBackend:
    """
    A backend that was considered and rejected, with a typed reason.

    Backward compatible with the original dict shape:
        r["backend"] → backend_id
        r["reason"]  → human-readable detail
        r["reason_code"] → typed enum value (new)
    """

    backend_id: str
    reason: RoutingRejectionReason
    detail: str = ""

    def __getitem__(self, key: str) -> Any:
        if key == "backend":
            return self.backend_id
        if key == "reason":
            return self.detail or self.reason.value
        if key == "reason_code":
            return self.reason.value
        raise KeyError(key)

    def __contains__(self, key: str) -> bool:
        return key in ("backend", "reason", "reason_code")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "backend": self.backend_id,
            "reason": self.detail or self.reason.value,
            "reason_code": self.reason.value,
        }


# =============================================================================
# Routing decision
# =============================================================================

@dataclass(frozen=True)
class RoutingDecision:
    """
    Immutable routing recommendation.

    Backward-compatible attributes:
        backend_id: Optional[str]
        rationale: str
        rejected: Tuple[RejectedBackend, ...]  (dict-compat via __getitem__)
        fallback_to_classical: bool
    """

    backend_id: Optional[str]
    rationale: str
    rejected: Tuple[RejectedBackend, ...] = ()
    fallback_to_classical: bool = False

    # --- New: structured fields ---
    reason: Optional[RoutingRejectionReason] = None
    score: Optional[float] = None
    stale: bool = False
    unknowns: Tuple[str, ...] = ()
    requirements: Mapping[str, Any] = field(default_factory=dict)
    handle_check: Mapping[str, Any] = field(default_factory=dict)
    rationale_dict: Mapping[str, Any] = field(default_factory=dict)
    decided_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        # Freeze mappings
        for fname in (
            "requirements", "handle_check", "rationale_dict",
        ):
            v = getattr(self, fname)
            if not isinstance(v, MappingProxyType):
                object.__setattr__(self, fname, _freeze_mapping(v))
        if not isinstance(self.unknowns, tuple):
            object.__setattr__(self, "unknowns", tuple(self.unknowns))
        if not isinstance(self.rejected, tuple):
            object.__setattr__(
                self, "rejected",
                tuple(
                    r if isinstance(r, RejectedBackend)
                    else RejectedBackend(
                        backend_id=str(r.get("backend", "")),
                        reason=RoutingRejectionReason.OTHER,
                        detail=str(r.get("reason", "")),
                    )
                    for r in self.rejected
                ),
            )
        # UTC
        if self.decided_at.tzinfo is None:
            object.__setattr__(
                self, "decided_at",
                self.decided_at.replace(tzinfo=timezone.utc),
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "backend_id": self.backend_id,
            "rationale": self.rationale,
            "rejected": [r.to_dict() for r in self.rejected],
            "fallback_to_classical": self.fallback_to_classical,
            "reason": self.reason.value if self.reason else None,
            "score": self.score,
            "stale": self.stale,
            "unknowns": list(self.unknowns),
            "requirements": dict(self.requirements),
            "handle_check": dict(self.handle_check),
            "rationale_dict": dict(self.rationale_dict),
            "decided_at": self.decided_at.isoformat(),
        }

    def checksum(self) -> str:
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        if self.backend_id:
            return (
                f"RoutingDecision(backend={self.backend_id!r}, "
                f"score={self.score!r}, stale={self.stale}, "
                f"n_rejected={len(self.rejected)})"
            )
        return (
            f"RoutingDecision(fallback_to_classical=True, "
            f"reason={self.reason.value if self.reason else None!r})"
        )


def _freeze_mapping(m: Any) -> Mapping[str, Any]:
    if m is None:
        return MappingProxyType({})
    if isinstance(m, MappingProxyType):
        return m
    if not isinstance(m, Mapping):
        return MappingProxyType({})
    return MappingProxyType(dict(m))


# =============================================================================
# Router
# =============================================================================

class BackendRouter:
    """
    Selects the best backend given a request and a registry of backends.

    Selection order:
      1. Filter by capabilities (delegated to ``check_handle`` when
         available, ``can_handle`` otherwise).
      2. Filter by preference (simulator / hardware / auto).
      3. Filter by queue, cost, region, staleness.
      4. Score remaining candidates on fidelity, queue, and cost.
      5. Return the winner with structured rationale.

    Thread-safe; bounded registry; deterministic tie-breaking.
    """

    def __init__(
        self,
        *,
        prefer_simulator: bool = True,
        max_cost_per_job: Optional[float] = None,
        config: Optional[BackendRouterConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        # Backward-compatible kwargs override config defaults.
        if config is None:
            config = BackendRouterConfig(
                prefer_simulator=bool(prefer_simulator),
                max_cost_per_job_usd=(
                    float(max_cost_per_job)
                    if max_cost_per_job is not None else None
                ),
            )
        self.config = config
        self.prefer_simulator = config.prefer_simulator
        self.max_cost_per_job = config.max_cost_per_job_usd
        self._metrics = metrics or MetricsRecorder()

        self._lock = threading.RLock()
        self._registry: Dict[str, BackendProfile] = {}
        self._started_mono = time.monotonic()

    # ------------------------------------------------------------------ registry
    def register(self, profile: BackendProfile) -> None:
        if profile is None or not getattr(profile, "backend_id", None):
            raise RoutingInputError(
                "profile must have a non-empty backend_id"
            )
        with self._lock:
            if (
                profile.backend_id not in self._registry
                and len(self._registry) >= self.config.max_registry_size
            ):
                raise RoutingError(
                    f"registry full ({self.config.max_registry_size})"
                )
            self._registry[profile.backend_id] = profile
        logger.info(
            "registered backend",
            extra={
                "backend_id": profile.backend_id,
                "kind": getattr(profile, "kind", None),
            },
        )

    def unregister(self, backend_id: str) -> bool:
        with self._lock:
            if backend_id in self._registry:
                del self._registry[backend_id]
                return True
        return False

    def clear(self) -> None:
        with self._lock:
            self._registry.clear()

    def get(self, backend_id: str) -> Optional[BackendProfile]:
        with self._lock:
            return self._registry.get(backend_id)

    def list_backends(self) -> Tuple[BackendProfile, ...]:
        with self._lock:
            return tuple(self._registry.values())

    # ------------------------------------------------------------------ select
    def select(
        self,
        *,
        required_qubits: int,
        required_shots: int,
        preference: Union[BackendPreference, str] = BackendPreference.AUTO,
        max_queue_seconds: float = 300.0,
        max_cost: Optional[float] = None,
        # New optional constraints, forwarded into BackendRequirements.
        min_gate_fidelity: Optional[float] = None,
        min_readout_fidelity: Optional[float] = None,
        min_coherence_us: Optional[float] = None,
        max_circuit_depth: Optional[int] = None,
        required_gates: Optional[Sequence[str]] = None,
        allowed_regions: Optional[Sequence[str]] = None,
        requires_hardware: bool = False,
        # Optional override for the freshness config.
        require_fresh: Optional[bool] = None,
    ) -> RoutingDecision:
        """
        Select the best backend, or return ``fallback_to_classical=True``.

        All constraints except qubits/shots are optional. When the enhanced
        ``BackendProfile`` is in use, ``min_gate_fidelity``, ``min_coherence_us``,
        ``max_circuit_depth``, ``required_gates``, and ``allowed_regions`` are
        checked against the profile. On the original profile, only the
        capability + queue + cost + preference checks are available.
        """
        t_start = time.monotonic()
        self._validate_select_inputs(
            required_qubits=required_qubits,
            required_shots=required_shots,
            max_queue_seconds=max_queue_seconds,
            max_cost=max_cost,
        )
        pref = _coerce_preference(preference)
        effective_max_cost = (
            float(max_cost) if max_cost is not None
            else self.config.max_cost_per_job_usd
        )
        fresh_required = (
            require_fresh if require_fresh is not None
            else self.config.require_fresh
        )

        # Build a BackendRequirements once (if supported) — reused for
        # every candidate.
        requirements_obj = self._build_requirements(
            required_qubits=required_qubits,
            required_shots=required_shots,
            max_queue_seconds=max_queue_seconds,
            max_cost_usd=effective_max_cost,
            min_gate_fidelity=min_gate_fidelity,
            min_readout_fidelity=min_readout_fidelity,
            min_coherence_us=min_coherence_us,
            max_circuit_depth=max_circuit_depth,
            required_gates=required_gates,
            allowed_regions=allowed_regions,
            requires_hardware=requires_hardware,
        )

        candidates: List[Tuple[BackendProfile, float]] = []
        rejected: List[RejectedBackend] = []
        unknowns: List[str] = []
        pref_seen = False
        region_seen = False

        with self._lock:
            snapshot = list(self._registry.values())

        for profile in snapshot:
            # --- Capability check ---
            cap = self._check_capabilities(
                profile, requirements_obj,
                required_qubits=required_qubits,
                required_shots=required_shots,
                max_queue_seconds=max_queue_seconds,
                effective_max_cost=effective_max_cost,
                min_gate_fidelity=min_gate_fidelity,
                min_coherence_us=min_coherence_us,
                max_circuit_depth=max_circuit_depth,
                required_gates=required_gates,
                allowed_regions=allowed_regions,
                requires_hardware=requires_hardware,
            )
            if cap.rejected is not None:
                rejected.append(cap.rejected)
                continue
            for u in cap.unknowns:
                if u not in unknowns:
                    unknowns.append(u)

            # --- Preference match ---
            if not self._preference_matches(profile, pref):
                if _matches_pref_kind(profile, pref):
                    pref_seen = True
                rejected.append(RejectedBackend(
                    backend_id=profile.backend_id,
                    reason=RoutingRejectionReason.PREFERENCE_MISMATCH,
                    detail=f"preference {pref.value!r} mismatch",
                ))
                continue
            if _matches_pref_kind(profile, pref):
                pref_seen = True

            # --- Region track ---
            if allowed_regions is not None:
                region = getattr(profile, "region", None)
                if region in allowed_regions:
                    region_seen = True

            # --- Staleness ---
            stale = self._is_stale(profile)
            if fresh_required and stale:
                rejected.append(RejectedBackend(
                    backend_id=profile.backend_id,
                    reason=RoutingRejectionReason.STALE_PROFILE,
                    detail=(
                        f"last refreshed "
                        f"{_profile_age_seconds(profile):.0f}s ago "
                        f"(max {self.config.max_profile_age_seconds:.0f}s)"
                    ),
                ))
                continue

            # --- Score ---
            score = self._score(
                profile,
                required_shots=required_shots,
                required_qubits=required_qubits,
            )
            candidates.append((profile, score))

        # --- No candidates: build fallback decision ---
        if not candidates:
            reason = self._no_candidate_reason(
                rejected, pref_seen, region_seen,
            )
            decision = RoutingDecision(
                backend_id=None,
                rationale=f"no backend satisfied constraints ({reason.value})",
                rejected=tuple(rejected),
                fallback_to_classical=True,
                reason=reason,
                unknowns=tuple(unknowns),
                requirements=self._requirements_to_dict(requirements_obj),
            )
            self._record_metrics(t_start, decision)
            return decision

        # --- Deterministic tie-break: highest score, then backend_id ---
        best_profile, best_score = max(
            candidates,
            key=lambda pair: (pair[1], pair[0].backend_id),
        )

        for profile, score in candidates:
            if profile is best_profile:
                continue
            rejected.append(RejectedBackend(
                backend_id=profile.backend_id,
                reason=RoutingRejectionReason.LOWER_SCORE,
                detail=(
                    f"score={score:.4f} < {best_score:.4f} "
                    f"({best_profile.backend_id})"
                ),
            ))

        best_stale = self._is_stale(best_profile)
        handle_check_dict = self._handle_check_dict(best_profile, requirements_obj)
        rationale_dict = self._build_rationale_dict(
            best_profile=best_profile,
            best_score=best_score,
            required_qubits=required_qubits,
            required_shots=required_shots,
            effective_max_cost=effective_max_cost,
            unknowns=unknowns,
        )
        rationale_str = (
            f"selected {best_profile.backend_id!r} "
            f"({_kind_value(best_profile)}; "
            f"fidelity={rationale_dict['fidelity']:.3f}, "
            f"queue={rationale_dict['queue_seconds']}, "
            f"cost=${rationale_dict['cost_usd']:.4f}, "
            f"score={best_score:.3f})"
        )
        decision = RoutingDecision(
            backend_id=best_profile.backend_id,
            rationale=rationale_str,
            rejected=tuple(rejected),
            fallback_to_classical=False,
            score=best_score,
            stale=best_stale,
            unknowns=tuple(unknowns),
            requirements=self._requirements_to_dict(requirements_obj),
            handle_check=handle_check_dict,
            rationale_dict=rationale_dict,
        )
        self._record_metrics(t_start, decision)
        logger.info(
            "routing decision",
            extra={
                "selected": decision.backend_id,
                "score": decision.score,
                "stale": decision.stale,
                "n_rejected": len(decision.rejected),
                "unknowns": list(decision.unknowns),
            },
        )
        return decision

    # ------------------------------------------------------------------ health
    def health(self) -> Dict[str, Any]:
        with self._lock:
            profiles = list(self._registry.values())
        online = 0
        stale = 0
        now = time.monotonic()
        for p in profiles:
            if getattr(p, "online", False):
                online += 1
            if self._is_stale(p):
                stale += 1
        return {
            "ready": bool(profiles),
            "backends_registered": len(profiles),
            "backends_online": online,
            "backends_stale": stale,
            "uptime_seconds": now - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }

    # ------------------------------------------------------------------ internals
    def _validate_select_inputs(
        self,
        *,
        required_qubits: Any,
        required_shots: Any,
        max_queue_seconds: Any,
        max_cost: Any,
    ) -> None:
        if not isinstance(required_qubits, int) \
                or isinstance(required_qubits, bool) \
                or required_qubits < 0:
            raise RoutingInputError(
                f"required_qubits must be a non-negative int; "
                f"got {required_qubits!r}"
            )
        if not isinstance(required_shots, int) \
                or isinstance(required_shots, bool) \
                or required_shots < 1:
            raise RoutingInputError(
                f"required_shots must be an int >= 1; "
                f"got {required_shots!r}"
            )
        if not isinstance(max_queue_seconds, (int, float)) \
                or isinstance(max_queue_seconds, bool) \
                or max_queue_seconds < 0:
            raise RoutingInputError(
                "max_queue_seconds must be >= 0"
            )
        if max_cost is not None:
            if not isinstance(max_cost, (int, float)) \
                    or isinstance(max_cost, bool) \
                    or max_cost < 0:
                raise RoutingInputError(
                    "max_cost must be >= 0 or None"
                )

    def _build_requirements(
        self,
        *,
        required_qubits: int,
        required_shots: int,
        max_queue_seconds: float,
        max_cost_usd: Optional[float],
        min_gate_fidelity: Optional[float],
        min_readout_fidelity: Optional[float],
        min_coherence_us: Optional[float],
        max_circuit_depth: Optional[int],
        required_gates: Optional[Sequence[str]],
        allowed_regions: Optional[Sequence[str]],
        requires_hardware: bool,
    ) -> Optional[Any]:
        if not _HAS_BACKEND_REQUIREMENTS:
            return None
        try:
            return BackendRequirements(  # type: ignore[misc]
                qubits=required_qubits,
                shots=required_shots,
                min_gate_fidelity=min_gate_fidelity,
                min_readout_fidelity=min_readout_fidelity,
                min_coherence_us=min_coherence_us,
                max_queue_seconds=max_queue_seconds,
                max_cost_usd=max_cost_usd,
                allowed_regions=(
                    tuple(allowed_regions)
                    if allowed_regions else None
                ),
                requires_hardware=requires_hardware,
                required_gates=(
                    tuple(required_gates) if required_gates else ()
                ),
                max_circuit_depth=max_circuit_depth,
            )
        except Exception as exc:
            logger.warning(
                "could not construct BackendRequirements: %s; "
                "falling back to per-check validation",
                exc,
            )
            return None

    @dataclass(frozen=True)
    class _CapabilityCheck:
        rejected: Optional[RejectedBackend] = None
        unknowns: Tuple[str, ...] = ()

    def _check_capabilities(
        self,
        profile: BackendProfile,
        requirements_obj: Optional[Any],
        *,
        required_qubits: int,
        required_shots: int,
        max_queue_seconds: float,
        effective_max_cost: Optional[float],
        min_gate_fidelity: Optional[float],
        min_coherence_us: Optional[float],
        max_circuit_depth: Optional[int],
        required_gates: Optional[Sequence[str]],
        allowed_regions: Optional[Sequence[str]],
        requires_hardware: bool,
    ) -> "BackendRouter._CapabilityCheck":
        # --- Preferred path: check_handle with BackendRequirements ---
        if (
            requirements_obj is not None
            and hasattr(profile, "check_handle")
        ):
            try:
                hc = profile.check_handle(requirements_obj)
            except Exception as exc:
                logger.warning(
                    "check_handle raised for %s: %s",
                    profile.backend_id, exc,
                )
                return BackendRouter._CapabilityCheck(
                    rejected=RejectedBackend(
                        backend_id=profile.backend_id,
                        reason=RoutingRejectionReason.OTHER,
                        detail=f"check_handle raised: {exc}",
                    ),
                )
            if not hc.can_handle:
                reason = _rejection_reason_from_handle_check(hc)
                return BackendRouter._CapabilityCheck(
                    rejected=RejectedBackend(
                        backend_id=profile.backend_id,
                        reason=reason,
                        detail=hc.reason or "capability check failed",
                    ),
                )
            return BackendRouter._CapabilityCheck(
                unknowns=tuple(hc.unknown),
            )

        # --- Fallback path: can_handle(qubits, shots) + manual checks ---
        ok = self._call_can_handle(
            profile, required_qubits, required_shots,
        )
        if not ok:
            return BackendRouter._CapabilityCheck(
                rejected=RejectedBackend(
                    backend_id=profile.backend_id,
                    reason=RoutingRejectionReason.INSUFFICIENT_CAPABILITIES,
                    detail="insufficient qubits or shots",
                ),
            )

        # Offline
        if getattr(profile, "online", True) is False:
            return BackendRouter._CapabilityCheck(
                rejected=RejectedBackend(
                    backend_id=profile.backend_id,
                    reason=RoutingRejectionReason.OFFLINE,
                    detail="backend is offline",
                ),
            )

        unknowns: List[str] = []

        # Queue
        queue = getattr(profile, "avg_queue_seconds", None)
        if queue is None:
            unknowns.append("queue_unknown")
        elif float(queue) > max_queue_seconds:
            return BackendRouter._CapabilityCheck(
                rejected=RejectedBackend(
                    backend_id=profile.backend_id,
                    reason=RoutingRejectionReason.QUEUE_TOO_LONG,
                    detail=(
                        f"queue {queue}s > budget {max_queue_seconds}s"
                    ),
                ),
            )

        # Cost
        if effective_max_cost is not None:
            cost_per_shot = float(getattr(profile, "cost_per_shot", 0.0) or 0.0)
            job_cost = cost_per_shot * required_shots
            if job_cost > effective_max_cost:
                return BackendRouter._CapabilityCheck(
                    rejected=RejectedBackend(
                        backend_id=profile.backend_id,
                        reason=RoutingRejectionReason.COST_OVER_BUDGET,
                        detail=(
                            f"cost ${job_cost:.4f} > "
                            f"budget ${effective_max_cost:.4f}"
                        ),
                    ),
                )

        # Fidelity / coherence / depth / gates / regions (fallback)
        if min_gate_fidelity is not None:
            f = _profile_fidelity_value(profile, self.config)
            if f is None:
                unknowns.append("gate_fidelity_unknown")
            elif f < min_gate_fidelity:
                return BackendRouter._CapabilityCheck(
                    rejected=RejectedBackend(
                        backend_id=profile.backend_id,
                        reason=RoutingRejectionReason.INSUFFICIENT_FIDELITY,
                        detail=(
                            f"fidelity {f:.3f} < "
                            f"{min_gate_fidelity:.3f}"
                        ),
                    ),
                )

        if min_coherence_us is not None:
            coh = _profile_coherence_value(profile)
            if coh is None:
                unknowns.append("coherence_unknown")
            elif coh < min_coherence_us:
                return BackendRouter._CapabilityCheck(
                    rejected=RejectedBackend(
                        backend_id=profile.backend_id,
                        reason=RoutingRejectionReason.INSUFFICIENT_COHERENCE,
                        detail=(
                            f"coherence {coh:.1f}us < "
                            f"{min_coherence_us:.1f}us"
                        ),
                    ),
                )

        if max_circuit_depth is not None:
            depth = getattr(profile, "max_circuit_depth", None)
            if depth is None:
                unknowns.append("circuit_depth_unknown")
            elif depth < max_circuit_depth:
                return BackendRouter._CapabilityCheck(
                    rejected=RejectedBackend(
                        backend_id=profile.backend_id,
                        reason=RoutingRejectionReason.INSUFFICIENT_CIRCUIT_DEPTH,
                        detail=(
                            f"depth {depth} < {max_circuit_depth}"
                        ),
                    ),
                )

        if required_gates:
            supported = set(getattr(profile, "supported_gates", ()) or ())
            missing = [g for g in required_gates if g not in supported]
            if missing:
                return BackendRouter._CapabilityCheck(
                    rejected=RejectedBackend(
                        backend_id=profile.backend_id,
                        reason=RoutingRejectionReason.MISSING_GATES,
                        detail=f"missing gates: {','.join(missing)}",
                    ),
                )

        if allowed_regions is not None:
            region = getattr(profile, "region", None)
            if region is None:
                unknowns.append("region_unknown")
            elif region not in allowed_regions:
                return BackendRouter._CapabilityCheck(
                    rejected=RejectedBackend(
                        backend_id=profile.backend_id,
                        reason=RoutingRejectionReason.REGION_NOT_ALLOWED,
                        detail=f"region {region!r} not in {list(allowed_regions)}",
                    ),
                )

        if requires_hardware:
            kind = getattr(profile, "kind", None)
            if not _kind_is(kind, BackendKind.HARDWARE):
                return BackendRouter._CapabilityCheck(
                    rejected=RejectedBackend(
                        backend_id=profile.backend_id,
                        reason=RoutingRejectionReason.PREFERENCE_MISMATCH,
                        detail="requires_hardware=True but kind is not hardware",
                    ),
                )

        return BackendRouter._CapabilityCheck(
            unknowns=tuple(unknowns),
        )

    def _call_can_handle(
        self, profile: BackendProfile, qubits: int, shots: int,
    ) -> bool:
        """Call can_handle tolerantly: supports bool or HandleCheck return."""
        try:
            result = profile.can_handle(qubits, shots)
        except Exception as exc:
            logger.warning(
                "can_handle raised for %s: %s",
                getattr(profile, "backend_id", "?"), exc,
            )
            return False
        # Enhanced profile returns HandleCheck; original returns bool.
        if hasattr(result, "can_handle"):
            return bool(result.can_handle)
        return bool(result)

    def _preference_matches(
        self, profile: BackendProfile, preference: BackendPreference,
    ) -> bool:
        if preference == BackendPreference.AUTO:
            return True
        if preference == BackendPreference.SIMULATOR:
            kind = getattr(profile, "kind", None)
            return (
                _kind_is(kind, BackendKind.SIMULATOR)
                or _kind_is(kind, BackendKind.CLOUD_SIMULATOR)
                or _kind_is(kind, BackendKind.EMULATOR)
            )
        if preference == BackendPreference.HARDWARE:
            return _kind_is(getattr(profile, "kind", None), BackendKind.HARDWARE)
        return False

    def _score(
        self,
        profile: BackendProfile,
        *,
        required_shots: int,
        required_qubits: int,
    ) -> float:
        """Deterministic score: higher is better."""
        fidelity = _profile_fidelity_value(profile, self.config) or 0.0

        queue = getattr(profile, "avg_queue_seconds", None)
        if queue is None:
            queue_penalty = 0.0   # unknown → no penalty (conservative)
        else:
            queue_penalty = min(
                1.0,
                float(queue) / self.config.queue_scale_seconds,
            )

        cost_per_shot = float(getattr(profile, "cost_per_shot", 0.0) or 0.0)
        job_cost = cost_per_shot * required_shots
        cost_penalty = min(
            1.0,
            job_cost / (
                self.config.cost_scale_usd_per_shot * max(required_shots, 1)
            ) if required_shots > 0 else 0.0,
        )

        simulator_bonus = 0.0
        if (
            self.config.prefer_simulator
            and _kind_is(getattr(profile, "kind", None), BackendKind.SIMULATOR)
        ):
            simulator_bonus = self.config.simulator_bonus

        return fidelity - queue_penalty - cost_penalty + simulator_bonus

    def _is_stale(self, profile: BackendProfile) -> bool:
        """Stale = last_refreshed_at is None or older than threshold."""
        if hasattr(profile, "is_stale"):
            try:
                return bool(profile.is_stale(self.config.max_profile_age_seconds))
            except Exception:
                pass
        last = getattr(profile, "last_refreshed_at", None)
        if last is None:
            return True
        age = (_utcnow() - last).total_seconds()
        return age > self.config.max_profile_age_seconds

    def _handle_check_dict(
        self,
        profile: BackendProfile,
        requirements_obj: Optional[Any],
    ) -> Mapping[str, Any]:
        if requirements_obj is None or not hasattr(profile, "check_handle"):
            return MappingProxyType({})
        try:
            hc = profile.check_handle(requirements_obj)
        except Exception:
            return MappingProxyType({})
        return MappingProxyType({
            "can_handle": bool(getattr(hc, "can_handle", True)),
            "reason": getattr(hc, "reason", "ok"),
            "failures": list(getattr(hc, "failures", ())),
            "unknown": list(getattr(hc, "unknown", ())),
        })

    def _build_rationale_dict(
        self,
        *,
        best_profile: BackendProfile,
        best_score: float,
        required_qubits: int,
        required_shots: int,
        effective_max_cost: Optional[float],
        unknowns: List[str],
    ) -> Mapping[str, Any]:
        queue = getattr(best_profile, "avg_queue_seconds", None)
        cost_per_shot = float(
            getattr(best_profile, "cost_per_shot", 0.0) or 0.0
        )
        return MappingProxyType({
            "backend_id": best_profile.backend_id,
            "kind": _kind_value(best_profile),
            "technology": str(getattr(best_profile, "technology", "unknown")),
            "fidelity": _profile_fidelity_value(
                best_profile, self.config,
            ) or self.config.unknown_fidelity_value,
            "queue_seconds": float(queue) if queue is not None else None,
            "cost_per_shot": cost_per_shot,
            "cost_usd": cost_per_shot * required_shots,
            "score": best_score,
            "required_qubits": required_qubits,
            "required_shots": required_shots,
            "max_cost_usd": effective_max_cost,
            "unknowns": list(unknowns),
        })

    def _requirements_to_dict(
        self, req: Optional[Any],
    ) -> Mapping[str, Any]:
        if req is None:
            return MappingProxyType({})
        try:
            return MappingProxyType(asdict(req))
        except Exception:
            return MappingProxyType({})

    def _no_candidate_reason(
        self,
        rejected: List[RejectedBackend],
        pref_seen: bool,
        region_seen: bool,
    ) -> RoutingRejectionReason:
        """
        Pick the most informative reason for a "no candidate" decision.

        If we saw a backend whose kind matched the preference but it was
        rejected for other reasons, the top-level reason is the most
        common rejection reason, not PREFERENCE_MISMATCH.
        """
        if not rejected:
            return RoutingRejectionReason.INSUFFICIENT_CAPABILITIES
        counts: Dict[RoutingRejectionReason, int] = {}
        for r in rejected:
            counts[r.reason] = counts.get(r.reason, 0) + 1
        # If every rejection is preference mismatch AND no matching-kind
        # backend was even considered, report PREFERENCE_MISMATCH.
        if (
            all(r.reason == RoutingRejectionReason.PREFERENCE_MISMATCH
                for r in rejected)
            and not pref_seen
        ):
            return RoutingRejectionReason.PREFERENCE_MISMATCH
        if (
            all(r.reason == RoutingRejectionReason.REGION_NOT_ALLOWED
                for r in rejected)
            and not region_seen
        ):
            return RoutingRejectionReason.REGION_NOT_ALLOWED
        # Otherwise return the most common reason.
        return max(counts.items(), key=lambda kv: (kv[1], kv[0].value))[0]

    def _record_metrics(
        self, t_start: float, decision: RoutingDecision,
    ) -> None:
        elapsed_ms = (time.monotonic() - t_start) * 1000.0
        self._metrics.observe("select_latency_ms", elapsed_ms)
        if decision.backend_id:
            self._metrics.inc(
                "select_total", result="selected",
                backend=decision.backend_id,
            )
        else:
            self._metrics.inc(
                "select_total", result="fallback",
                reason=(
                    decision.reason.value if decision.reason else "unknown"
                ),
            )
        for r in decision.rejected:
            self._metrics.inc(
                "rejected_total", reason=r.reason.value,
            )


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_preference(value: Any) -> BackendPreference:
    if isinstance(value, BackendPreference):
        return value
    try:
        return BackendPreference(value)
    except (ValueError, KeyError):
        allowed = sorted(e.value for e in BackendPreference)
        raise RoutingInputError(
            f"preference must be one of {allowed}; got {value!r}"
        ) from None


def _kind_value(profile: BackendProfile) -> str:
    kind = getattr(profile, "kind", None)
    if kind is None:
        return "unknown"
    if hasattr(kind, "value"):
        return str(kind.value)
    return str(kind)


def _kind_is(kind: Any, target: BackendKind) -> bool:
    """True if kind is (or equals the value of) the target enum member."""
    if kind is None:
        return False
    if kind == target:
        return True
    if kind == target.value:
        return True
    return False


def _matches_pref_kind(
    profile: BackendProfile, pref: BackendPreference,
) -> bool:
    """Whether the profile's kind matches the preference (ignoring other filters)."""
    kind = getattr(profile, "kind", None)
    if pref == BackendPreference.AUTO:
        return True
    if pref == BackendPreference.SIMULATOR:
        return (
            _kind_is(kind, BackendKind.SIMULATOR)
            or _kind_is(kind, BackendKind.CLOUD_SIMULATOR)
            or _kind_is(kind, BackendKind.EMULATOR)
        )
    if pref == BackendPreference.HARDWARE:
        return _kind_is(kind, BackendKind.HARDWARE)
    return False


def _profile_fidelity_value(
    profile: BackendProfile,
    config: BackendRouterConfig,
) -> Optional[float]:
    """Prefer effective_gate_fidelity, then 2q, then gate, then None."""
    eff = getattr(profile, "effective_gate_fidelity", None)
    if eff is not None:
        try:
            return float(eff)
        except (TypeError, ValueError):
            pass
    for fname in ("gate_fidelity_2q", "gate_fidelity"):
        v = getattr(profile, fname, None)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return None


def _profile_coherence_value(profile: BackendProfile) -> Optional[float]:
    eff = getattr(profile, "effective_coherence_us", None)
    if eff is not None:
        try:
            return float(eff)
        except (TypeError, ValueError):
            pass
    for fname in ("t2_coherence_us", "coherence_time_us"):
        v = getattr(profile, fname, None)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return None


def _profile_age_seconds(profile: BackendProfile) -> float:
    last = getattr(profile, "last_refreshed_at", None)
    if last is None:
        return float("inf")
    return (_utcnow() - last).total_seconds()


def _rejection_reason_from_handle_check(hc: Any) -> RoutingRejectionReason:
    """Map a HandleCheck's `reason` string to our enum."""
    reason = str(getattr(hc, "reason", "") or "").lower()
    mapping = {
        "insufficient_qubits": RoutingRejectionReason.INSUFFICIENT_QUBITS,
        "insufficient_shots": RoutingRejectionReason.INSUFFICIENT_SHOTS,
        "insufficient_circuit_depth": RoutingRejectionReason.INSUFFICIENT_CIRCUIT_DEPTH,
        "low_gate_fidelity": RoutingRejectionReason.INSUFFICIENT_FIDELITY,
        "low_readout_fidelity": RoutingRejectionReason.INSUFFICIENT_FIDELITY,
        "short_coherence": RoutingRejectionReason.INSUFFICIENT_COHERENCE,
        "queue_too_long": RoutingRejectionReason.QUEUE_TOO_LONG,
        "cost_over_budget": RoutingRejectionReason.COST_OVER_BUDGET,
        "energy_over_budget": RoutingRejectionReason.COST_OVER_BUDGET,
        "region_not_allowed": RoutingRejectionReason.REGION_NOT_ALLOWED,
        "missing_gates": RoutingRejectionReason.MISSING_GATES,
        "offline": RoutingRejectionReason.OFFLINE,
        "hardware_required": RoutingRejectionReason.PREFERENCE_MISMATCH,
    }
    for prefix, code in mapping.items():
        if reason.startswith(prefix):
            return code
    return RoutingRejectionReason.INSUFFICIENT_CAPABILITIES


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "BackendRouter",
    "BackendRouterConfig",
    "MetricsRecorder",
    "RejectedBackend",
    "RoutingDecision",
    "RoutingError",
    "RoutingInputError",
    "RoutingRejectionReason",
)


# =============================================================================
# Smoke test: python -m qcs.execution.backend_router
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    def _mk_sim(sim_id: str, fidelity: Optional[float] = 0.99,
                queue: Optional[float] = 2.0,
                cost: float = 0.0001,
                region: str = "US-CA"):
        return BackendProfile(
            backend_id=sim_id, kind="simulator",
            technology="simulated",
            num_qubits=32, max_shots_per_job=1_000_000,
            gate_fidelity_2q=fidelity,
            avg_queue_seconds=queue,
            cost_per_shot=cost,
            region=region,
            online=True,
        )

    def _mk_hw(hw_id: str, fidelity: Optional[float] = 0.97,
               queue: Optional[float] = 60.0,
               cost: float = 0.001,
               region: str = "US-CA"):
        return BackendProfile(
            backend_id=hw_id, kind="hardware",
            technology="superconducting",
            num_qubits=27, max_shots_per_job=200_000,
            gate_fidelity_2q=fidelity,
            avg_queue_seconds=queue,
            cost_per_shot=cost,
            region=region,
            vendor="IBM",
            online=True,
        )

    print("=== Registration & basic selection ===")
    router = BackendRouter()
    router.register(_mk_sim("sim-A"))
    router.register(_mk_hw("hw-A"))
    d = router.select(required_qubits=8, required_shots=1000)
    print(f"  {d!r}")
    print(f"  rationale: {d.rationale}")
    print(f"  unknowns: {d.unknowns}")
    assert d.backend_id is not None
    assert not d.fallback_to_classical

    print("\n=== §1.1 Enhanced profile with avg_queue_seconds=None ===")
    router_none = BackendRouter()
    profile_none = _mk_sim("sim-none", queue=None)
    router_none.register(profile_none)
    d_none = router_none.select(required_qubits=8, required_shots=1000)
    print(f"  {d_none!r}")
    print(f"  unknowns: {d_none.unknowns}")
    assert d_none.backend_id == "sim-none"
    assert "queue_unknown" in d_none.unknowns

    print("\n=== §1.2 Fidelity filtering via check_handle ===")
    router_fid = BackendRouter()
    router_fid.register(_mk_sim("sim-low", fidelity=0.85))
    router_fid.register(_mk_sim("sim-high", fidelity=0.99))
    d_fid = router_fid.select(
        required_qubits=8, required_shots=1000,
        min_gate_fidelity=0.95,
    )
    print(f"  {d_fid!r}")
    print(f"  selected: {d_fid.backend_id}")
    if _HAS_BACKEND_REQUIREMENTS:
        assert d_fid.backend_id == "sim-high"
        # Verify the low-fidelity backend was rejected with a typed reason.
        low_rej = [r for r in d_fid.rejected if r.backend_id == "sim-low"]
        print(f"  low-fidelity rejected as: "
              f"{low_rej[0].reason.value if low_rej else None}")
        assert low_rej and low_rej[0].reason in (
            RoutingRejectionReason.INSUFFICIENT_FIDELITY,
            RoutingRejectionReason.LOWER_SCORE,
        )

    print("\n=== §1.3 effective_gate_fidelity scoring ===")
    router_eff = BackendRouter()
    # Backend with only 2q fidelity (gate_fidelity is None on enhanced).
    router_eff.register(_mk_sim("sim-2q", fidelity=0.99))
    d_eff = router_eff.select(required_qubits=8, required_shots=1000)
    print(f"  selected: {d_eff.backend_id}")
    print(f"  rationale_dict fidelity: {d_eff.rationale_dict.get('fidelity')}")
    assert d_eff.rationale_dict["fidelity"] >= 0.9  # not 0.5

    print("\n=== §1.4 max_cost=0 rejects any non-zero cost ===")
    router_cost = BackendRouter()
    router_cost.register(_mk_sim("sim-paid", cost=0.001))
    router_cost.register(_mk_sim("sim-free", cost=0.0))
    d_cost = router_cost.select(
        required_qubits=8, required_shots=1000, max_cost=0.0,
    )
    print(f"  {d_cost!r}")
    assert d_cost.backend_id == "sim-free"
    # With no free backend, max_cost=0 falls back.
    router_cost2 = BackendRouter()
    router_cost2.register(_mk_sim("sim-paid", cost=0.001))
    d_cost2 = router_cost2.select(
        required_qubits=8, required_shots=1000, max_cost=0.0,
    )
    print(f"  only paid: fallback={d_cost2.fallback_to_classical}, "
          f"reason={d_cost2.reason.value if d_cost2.reason else None}")
    assert d_cost2.fallback_to_classical
    assert d_cost2.reason == RoutingRejectionReason.COST_OVER_BUDGET

    print("\n=== §1.5 max_cost_per_job config default ===")
    router_cfg = BackendRouter(max_cost_per_job=0.0001)
    router_cfg.register(_mk_sim("sim-paid", cost=0.001))
    d_cfg = router_cfg.select(required_qubits=8, required_shots=1000)
    print(f"  fallback={d_cfg.fallback_to_classical}")
    assert d_cfg.fallback_to_classical  # 0.001*1000=1.0 > 0.0001

    print("\n=== §2.1 Preference coercion ===")
    router_pref = BackendRouter()
    router_pref.register(_mk_sim("s1"))
    router_pref.register(_mk_hw("h1"))
    d_pref = router_pref.select(
        required_qubits=8, required_shots=1000, preference="hardware",
    )
    print(f"  preference='hardware' → {d_pref.backend_id}")
    assert d_pref.backend_id == "h1"
    try:
        router_pref.select(
            required_qubits=8, required_shots=1000, preference="hardwar",
        )
    except RoutingInputError as e:
        print(f"  preference='hardwar' rejected: {e}")

    print("\n=== §2.3 Immutability ===")
    try:
        d.backend_id = "hacked"  # type: ignore[misc]
    except Exception as e:
        print(f"  backend_id mutation → {type(e).__name__}")

    print("\n=== §2.4 Typed rejections with dict-compat ===")
    router_rej = BackendRouter()
    router_rej.register(_mk_hw("hw-only", cost=0.001))
    d_rej = router_rej.select(
        required_qubits=8, required_shots=1000,
        preference="simulator",
    )
    print(f"  fallback={d_rej.fallback_to_classical}")
    print(f"  reason: {d_rej.reason.value if d_rej.reason else None}")
    assert d_rej.fallback_to_classical
    assert d_rej.reason == RoutingRejectionReason.PREFERENCE_MISMATCH
    r = d_rej.rejected[0]
    print(f"  rejected[0]['backend'] = {r['backend']}")
    print(f"  rejected[0]['reason'] = {r['reason']}")
    print(f"  rejected[0]['reason_code'] = {r['reason_code']}")

    print("\n=== §2.6 Deterministic tie-breaking ===")
    p1 = _mk_sim("zzz", fidelity=0.99, queue=2.0, cost=0.0001)
    p2 = _mk_sim("aaa", fidelity=0.99, queue=2.0, cost=0.0001)
    router_t1 = BackendRouter()
    router_t1.register(p1)
    router_t1.register(p2)
    d_t1 = router_t1.select(required_qubits=8, required_shots=1000)
    router_t2 = BackendRouter()
    router_t2.register(p2)
    router_t2.register(p1)
    d_t2 = router_t2.select(required_qubits=8, required_shots=1000)
    print(f"  order 1: {d_t1.backend_id}")
    print(f"  order 2: {d_t2.backend_id}")
    assert d_t1.backend_id == d_t2.backend_id == "zzz"

    print("\n=== §3.3 Stale profile handling ===")
    stale_profile = replace(
        _mk_sim("stale"),
        last_refreshed_at=None,
    )
    router_stale = BackendRouter(require_fresh=True)
    router_stale.register(stale_profile)
    d_stale = router_stale.select(required_qubits=8, required_shots=1000)
    print(f"  fallback={d_stale.fallback_to_classical}")
    print(f"  reason={d_stale.reason.value if d_stale.reason else None}")
    assert d_stale.fallback_to_classical
    assert d_stale.reason == RoutingRejectionReason.STALE_PROFILE

    print("\n=== §3.4 Region filter ===")
    router_region = BackendRouter()
    router_region.register(_mk_sim("us-ca", region="US-CA"))
    router_region.register(_mk_sim("eu-de", region="EU-DE"))
    d_region = router_region.select(
        required_qubits=8, required_shots=1000,
        allowed_regions=("EU-DE",),
    )
    print(f"  selected: {d_region.backend_id}")
    assert d_region.backend_id == "eu-de"

    print("\n=== §3.8 Serialization / checksum ===")
    d_dict = d.to_dict()
    print(f"  keys: {sorted(d_dict.keys())}")
    print(f"  checksum: {d.checksum()[:16]}…")
    assert isinstance(d_dict["rejected"], list)

    print("\n=== §2.12 Health ===")
    print(f"  {router.health()}")

    print("\n=== Registry management ===")
    assert router.get("sim-A") is not None
    assert router.unregister("sim-A") is True
    assert router.get("sim-A") is None
    print(f"  registry after unregister: "
          f"{[p.backend_id for p in router.list_backends()]}")
    router.clear()
    assert len(router.list_backends()) == 0

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
