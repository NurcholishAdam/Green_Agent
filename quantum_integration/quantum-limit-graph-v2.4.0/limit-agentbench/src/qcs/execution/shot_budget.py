"""
ShotBudget — track and enforce shot allocation across phases.

A ``ShotBudget`` allocates a total shot count across three named phases
and a catch-all:

    exploratory  — small-shots exploration
    mitigation   — error-mitigation overhead
    reserved     — held back for retries / emergencies
    production   — everything left over

The allocation is a *policy* — a split of a given total. Usage tracking
is a separate operation via ``track_usage``; the class does not
maintain mutable ledgers.

Truncation
----------
Each phase's shots are computed as ``int(total * pct)``. The fractional
remainder is absorbed by ``production`` (which is computed as
``total - exploratory - mitigation - reserved``), so the sum invariant
``exploratory + mitigation + reserved + production == total`` always
holds. The percentage fields on the allocation let a caller reconstruct
the intended split exactly.

Validation
----------
``ShotBudgetConfig`` validates each percentage independently *and*
their sum. ``ShotBudgetAllocation`` validates non-negative fields and
the sum invariant. Neither accepts negative, NaN, or out-of-range
values — the previous ``abs(sum) > 1.0`` check let negative percentages
flow through and produce negative shot counts.

Original API preserved:
    budget = ShotBudget()
    allocation = budget.allocate(total_shots=1000)
    budget.can_proceed(requested=200, allocated=allocation.production_shots)
    allocation.to_dict()

v2 fixes (see changelog in commit message):
  * Per-field percentage validation (abs(sum) > 1.0 allowed negatives).
  * total_shots validated (was unvalidated; -1 produced negative fields).
  * ``max(0, production)`` clamp removed (was a silent-corruption
    masking; correct arithmetic now guarantees non-negative).
  * ``ShotBudgetAllocation`` frozen; ``__post_init__`` enforces the sum
    invariant.
  * ``can_proceed`` validates its arguments (was True for negative
    ``requested``).
  * Percentages recorded on the allocation; ``from_dict`` / ``from_percentages``
    added.
  * ``ShotBudgetConfig`` dataclass; metrics; health; __version__;
    __all__; demo.
  * ``to_dict`` returns legacy keys plus canonical keys; ``from_dict``
    accepts either.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field, fields
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Constants
# =============================================================================

MAX_TOTAL_SHOTS: int = 10**12  # one trillion; far above any real job


# =============================================================================
# Errors
# =============================================================================

class ShotBudgetError(Exception):
    """Base class for ShotBudget errors."""


class ShotBudgetInputError(ShotBudgetError, ValueError):
    """Invalid inputs to a ShotBudget method or the constructor."""


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
class ShotBudgetConfig:
    """Frozen, validated configuration for ``ShotBudget``."""

    exploratory_pct: float = 0.2
    mitigation_pct: float = 0.3
    reserved_pct: float = 0.05

    def __post_init__(self) -> None:
        # --- Per-field validation ---
        for fname in (
            "exploratory_pct", "mitigation_pct", "reserved_pct",
        ):
            v = getattr(self, fname)
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ShotBudgetInputError(
                    f"{fname} must be a number; "
                    f"got {type(v).__name__}"
                )
            if not math.isfinite(float(v)):
                raise ShotBudgetInputError(
                    f"{fname} must be finite; got {v!r}"
                )
            if not (0.0 <= float(v) <= 1.0):
                raise ShotBudgetInputError(
                    f"{fname} must be in [0, 1]; got {v!r}"
                )
        # --- Sum validation ---
        total_pct = (
            float(self.exploratory_pct)
            + float(self.mitigation_pct)
            + float(self.reserved_pct)
        )
        if total_pct > 1.0 + 1e-9:
            raise ShotBudgetInputError(
                f"exploratory_pct + mitigation_pct + reserved_pct "
                f"must not exceed 1.0; got {total_pct:.6f}"
            )

    @property
    def production_pct(self) -> float:
        """The implied production percentage (>= 0)."""
        return max(
            0.0,
            1.0 - float(self.exploratory_pct)
            - float(self.mitigation_pct)
            - float(self.reserved_pct),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Allocation
# =============================================================================

@dataclass(frozen=True)
class ShotBudgetAllocation:
    """
    Immutable record of a shot allocation plan.

    The four shot-count fields are non-negative integers that sum to
    ``total_shots``. Percentages, when present, record the policy that
    produced the allocation so an audit can reconstruct the split.

    Hand-construction is allowed but enforces the sum invariant; use
    ``ShotBudget.allocate`` for the normal path, or
    ``ShotBudgetAllocation.from_dict`` to deserialize an audit record.
    """

    total_shots: int
    exploratory_shots: int = 0
    production_shots: int = 0
    mitigation_shots: int = 0
    reserved_shots: int = 0

    # --- Optional: which percentages produced this allocation ---
    exploratory_pct: Optional[float] = None
    mitigation_pct: Optional[float] = None
    reserved_pct: Optional[float] = None

    # --- Internal escape hatch for audit reconstruction ---
    _skip_validation: bool = field(
        default=False, repr=False, compare=False, hash=False,
    )

    def __post_init__(self) -> None:
        # --- Int fields ---
        for fname in (
            "total_shots", "exploratory_shots", "production_shots",
            "mitigation_shots", "reserved_shots",
        ):
            v = getattr(self, fname)
            if isinstance(v, bool) or not isinstance(v, int):
                raise ShotBudgetInputError(
                    f"{fname} must be an int; got {type(v).__name__}"
                )
            if v < 0:
                raise ShotBudgetInputError(
                    f"{fname} must be >= 0; got {v}"
                )

        # --- Percentage fields (optional) ---
        for fname in (
            "exploratory_pct", "mitigation_pct", "reserved_pct",
        ):
            v = getattr(self, fname)
            if v is None:
                continue
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ShotBudgetInputError(
                    f"{fname} must be a number or None"
                )
            if not math.isfinite(float(v)):
                raise ShotBudgetInputError(
                    f"{fname} must be finite or None"
                )
            if not (0.0 <= float(v) <= 1.0):
                raise ShotBudgetInputError(
                    f"{fname} must be in [0, 1] or None; got {v!r}"
                )

        if self._skip_validation:
            return

        # --- Sum invariant ---
        if self.total_shots <= 0:
            raise ShotBudgetInputError(
                f"total_shots must be > 0; got {self.total_shots}"
            )
        summed = (
            self.exploratory_shots + self.production_shots
            + self.mitigation_shots + self.reserved_shots
        )
        if summed != self.total_shots:
            raise ShotBudgetInputError(
                f"allocation sum {summed} != total_shots "
                f"{self.total_shots} "
                f"(exploratory={self.exploratory_shots}, "
                f"mitigation={self.mitigation_shots}, "
                f"reserved={self.reserved_shots}, "
                f"production={self.production_shots})"
            )

    # ------------------------------------------------------------------ helper
    @property
    def production_pct(self) -> Optional[float]:
        """
        The production percentage implied by the other three, or None
        if percentages weren't recorded.
        """
        pcts = (
            self.exploratory_pct,
            self.mitigation_pct,
            self.reserved_pct,
        )
        if any(p is None for p in pcts):
            return None
        return max(0.0, 1.0 - sum(float(p) for p in pcts))  # type: ignore[arg-type]

    def to_dict(self) -> Dict[str, Any]:
        """Legacy keys first for backward compat, then canonical keys."""
        return {
            # Legacy keys (produced by the pre-2.0 `to_dict`):
            "total": self.total_shots,
            "exploratory": self.exploratory_shots,
            "production": self.production_shots,
            "mitigation": self.mitigation_shots,
            "reserved": self.reserved_shots,
            # Canonical keys (matching the dataclass field names):
            "total_shots": self.total_shots,
            "exploratory_shots": self.exploratory_shots,
            "production_shots": self.production_shots,
            "mitigation_shots": self.mitigation_shots,
            "reserved_shots": self.reserved_shots,
            # New metadata:
            "exploratory_pct": self.exploratory_pct,
            "mitigation_pct": self.mitigation_pct,
            "reserved_pct": self.reserved_pct,
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(
        cls,
        d: Mapping[str, Any],
        *,
        validate: bool = True,
    ) -> "ShotBudgetAllocation":
        """
        Build from a dict produced by ``to_dict()`` or the legacy
        ``{total, exploratory, production, mitigation, reserved}``
        shape.
        """
        if not isinstance(d, Mapping):
            raise ShotBudgetInputError(
                f"d must be a Mapping; got {type(d).__name__}"
            )

        def _get(short: str, long: str, default: Any = None) -> Any:
            if long in d:
                return d[long]
            if short in d:
                return d[short]
            return default

        cleaned: Dict[str, Any] = {
            "total_shots": _get("total", "total_shots"),
            "exploratory_shots": _get("exploratory", "exploratory_shots", 0),
            "production_shots": _get("production", "production_shots", 0),
            "mitigation_shots": _get("mitigation", "mitigation_shots", 0),
            "reserved_shots": _get("reserved", "reserved_shots", 0),
            "exploratory_pct": d.get("exploratory_pct"),
            "mitigation_pct": d.get("mitigation_pct"),
            "reserved_pct": d.get("reserved_pct"),
        }
        if cleaned["total_shots"] is None:
            raise ShotBudgetInputError(
                "dict must contain 'total' or 'total_shots'"
            )
        if not validate:
            cleaned["_skip_validation"] = True
        return cls(**cleaned)

    @classmethod
    def from_percentages(
        cls,
        total_shots: int,
        *,
        exploratory_pct: float = 0.2,
        mitigation_pct: float = 0.3,
        reserved_pct: float = 0.05,
    ) -> "ShotBudgetAllocation":
        """
        Convenience constructor that runs the same validation and
        truncation logic as ``ShotBudget().allocate``.
        """
        return ShotBudget(
            exploratory_pct=exploratory_pct,
            mitigation_pct=mitigation_pct,
            reserved_pct=reserved_pct,
        ).allocate(total_shots)

    def checksum(self) -> str:
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            f"ShotBudgetAllocation("
            f"total={self.total_shots}, "
            f"exploratory={self.exploratory_shots}, "
            f"mitigation={self.mitigation_shots}, "
            f"reserved={self.reserved_shots}, "
            f"production={self.production_shots})"
        )


# =============================================================================
# Usage tracker
# =============================================================================

@dataclass(frozen=True)
class ShotUsage:
    """Snapshot of how many shots of an allocation have been used."""

    allocated: int
    used: int
    remaining: int
    overspent_by: int
    ratio_used: Optional[float]  # used/allocated, None if allocated==0

    def __post_init__(self) -> None:
        for fname in ("allocated", "used", "remaining", "overspent_by"):
            v = getattr(self, fname)
            if isinstance(v, bool) or not isinstance(v, int) or v < 0:
                raise ShotBudgetInputError(
                    f"{fname} must be a non-negative int"
                )
        # Consistency check on the arithmetic.
        expected_remaining = max(0, self.allocated - self.used)
        expected_overspent = max(0, self.used - self.allocated)
        if self.remaining != expected_remaining:
            raise ShotBudgetInputError(
                f"remaining ({self.remaining}) inconsistent with "
                f"allocated ({self.allocated}) and used ({self.used})"
            )
        if self.overspent_by != expected_overspent:
            raise ShotBudgetInputError(
                f"overspent_by ({self.overspent_by}) inconsistent with "
                f"allocated ({self.allocated}) and used ({self.used})"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allocated": self.allocated,
            "used": self.used,
            "remaining": self.remaining,
            "overspent_by": self.overspent_by,
            "ratio_used": self.ratio_used,
        }

    def __repr__(self) -> str:
        return (
            f"ShotUsage(allocated={self.allocated}, "
            f"used={self.used}, remaining={self.remaining}, "
            f"overspent_by={self.overspent_by})"
        )


# =============================================================================
# The Enhanced ShotBudget
# =============================================================================

class ShotBudget:
    """
    Allocates shots across exploratory, mitigation, reserved, and
    production phases.

    The instance is stateless (the config never changes); usage is
    tracked *externally* via ``track_usage``. The class itself is
    thread-safe and reentrant.
    """

    def __init__(
        self,
        *,
        exploratory_pct: float = 0.2,
        mitigation_pct: float = 0.3,
        reserved_pct: float = 0.05,
        config: Optional[ShotBudgetConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        if config is None:
            config = ShotBudgetConfig(
                exploratory_pct=exploratory_pct,
                mitigation_pct=mitigation_pct,
                reserved_pct=reserved_pct,
            )
        self.config = config
        self.exploratory_pct = float(config.exploratory_pct)
        self.mitigation_pct = float(config.mitigation_pct)
        self.reserved_pct = float(config.reserved_pct)
        self._metrics = metrics or MetricsRecorder()
        self._started_mono = time.monotonic()

    # ------------------------------------------------------------------ allocate
    def allocate(self, total_shots: int) -> ShotBudgetAllocation:
        """
        Produce an allocation of ``total_shots`` across the four phases.

        Truncation: each named phase gets ``int(total * pct)``; the
        remainder is absorbed by ``production``. The four shot fields
        always sum to ``total_shots``.
        """
        self._validate_total_shots(total_shots)

        exploratory = int(total_shots * self.exploratory_pct)
        mitigation = int(total_shots * self.mitigation_pct)
        reserved = int(total_shots * self.reserved_pct)
        # With validated percentages in [0,1] summing to <= 1, the
        # expression below is provably non-negative — no clamp needed.
        production = total_shots - exploratory - mitigation - reserved

        allocation = ShotBudgetAllocation(
            total_shots=total_shots,
            exploratory_shots=exploratory,
            production_shots=production,
            mitigation_shots=mitigation,
            reserved_shots=reserved,
            exploratory_pct=self.exploratory_pct,
            mitigation_pct=self.mitigation_pct,
            reserved_pct=self.reserved_pct,
        )

        self._metrics.inc("allocations_total")
        self._metrics.observe(
            "allocation_total_shots", float(total_shots),
        )
        logger.debug(
            "allocated shots",
            extra={
                "total_shots": total_shots,
                "exploratory": exploratory,
                "mitigation": mitigation,
                "reserved": reserved,
                "production": production,
            },
        )
        return allocation

    # ------------------------------------------------------------------ usage
    def track_usage(
        self, allocation: ShotBudgetAllocation, used: int,
    ) -> ShotUsage:
        """
        Return a ``ShotUsage`` summarizing the given allocation and
        consumption. The allocation is *not* mutated (it's frozen);
        the caller is responsible for persisting the usage record.
        """
        if not isinstance(allocation, ShotBudgetAllocation):
            raise ShotBudgetInputError(
                "allocation must be a ShotBudgetAllocation"
            )
        if isinstance(used, bool) or not isinstance(used, int):
            raise ShotBudgetInputError(
                f"used must be an int; got {type(used).__name__}"
            )
        if used < 0:
            raise ShotBudgetInputError(
                f"used must be >= 0; got {used}"
            )

        allocated = allocation.total_shots
        remaining = max(0, allocated - used)
        overspent_by = max(0, used - allocated)
        ratio_used = (used / allocated) if allocated > 0 else None

        usage = ShotUsage(
            allocated=allocated,
            used=used,
            remaining=remaining,
            overspent_by=overspent_by,
            ratio_used=ratio_used,
        )
        if overspent_by > 0:
            self._metrics.inc("usage_overspent")
            logger.warning(
                "shot usage exceeded allocation",
                extra={
                    "allocated": allocated,
                    "used": used,
                    "overspent_by": overspent_by,
                },
            )
        return usage

    # ------------------------------------------------------------------ capability
    def can_proceed(self, requested: int, allocated: int) -> bool:
        """
        Return True if ``requested`` shots fit within ``allocated``.

        Kept for backward compat. Validates both arguments (the
        pre-2.0 version returned True for ``requested=-1``).
        Use ``fits_within`` for the clearer, allocation-aware variant.
        """
        self._validate_shots("requested", requested)
        self._validate_shots("allocated", allocated)
        return requested <= allocated

    def fits_within(
        self, requested: int, allocation: ShotBudgetAllocation,
    ) -> bool:
        """
        Return True if ``requested`` shots fit within ``allocation``.

        The allocation is authoritative: its sum invariant has been
        validated at construction, so ``requested`` is checked against
        ``allocation.total_shots``.
        """
        if not isinstance(allocation, ShotBudgetAllocation):
            raise ShotBudgetInputError(
                "allocation must be a ShotBudgetAllocation"
            )
        self._validate_shots("requested", requested)
        return requested <= allocation.total_shots

    # ------------------------------------------------------------------ health
    def health(self) -> Dict[str, Any]:
        return {
            "ready": True,
            "config": self.config.to_dict(),
            "production_pct": self.config.production_pct,
            "uptime_seconds": time.monotonic() - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }

    # ------------------------------------------------------------------ internals
    @staticmethod
    def _validate_total_shots(total_shots: Any) -> None:
        if isinstance(total_shots, bool) or not isinstance(total_shots, int):
            raise ShotBudgetInputError(
                f"total_shots must be an int; "
                f"got {type(total_shots).__name__}"
            )
        if total_shots <= 0:
            raise ShotBudgetInputError(
                f"total_shots must be > 0; got {total_shots}"
            )
        if total_shots > MAX_TOTAL_SHOTS:
            raise ShotBudgetInputError(
                f"total_shots={total_shots} exceeds MAX_TOTAL_SHOTS "
                f"({MAX_TOTAL_SHOTS})"
            )

    @staticmethod
    def _validate_shots(name: str, v: Any) -> None:
        if isinstance(v, bool) or not isinstance(v, int):
            raise ShotBudgetInputError(
                f"{name} must be an int; got {type(v).__name__}"
            )
        if v < 0:
            raise ShotBudgetInputError(
                f"{name} must be >= 0; got {v}"
            )


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "MAX_TOTAL_SHOTS",
    "MetricsRecorder",
    "ShotBudget",
    "ShotBudgetAllocation",
    "ShotBudgetConfig",
    "ShotBudgetError",
    "ShotBudgetInputError",
    "ShotUsage",
)


# =============================================================================
# Demo: python -m qcs.execution.shot_budget
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json

    logging.basicConfig(level=logging.INFO)

    print("=== Happy path (default 20/30/5 split) ===")
    budget = ShotBudget()
    alloc = budget.allocate(1000)
    print(f"  {alloc!r}")
    print(f"  sum = "
          f"{alloc.exploratory_shots + alloc.mitigation_shots + alloc.reserved_shots + alloc.production_shots}")
    assert (
        alloc.exploratory_shots + alloc.mitigation_shots
        + alloc.reserved_shots + alloc.production_shots
    ) == 1000
    print(f"  exploratory_pct: {alloc.exploratory_pct}")
    print(f"  production_pct (implied): {alloc.production_pct}")
    print(f"  checksum: {alloc.checksum()[:16]}…")

    print("\n=== §1.1 Negative percentage rejected ===")
    for label, kwargs in [
        ("negative exploratory",
         dict(exploratory_pct=-0.5, mitigation_pct=0.3, reserved_pct=0.05)),
        ("negative reserved",
         dict(exploratory_pct=0.4, mitigation_pct=0.4, reserved_pct=-0.3)),
        ("exploratory > 1",
         dict(exploratory_pct=1.5)),
        ("sum > 1",
         dict(exploratory_pct=0.5, mitigation_pct=0.5, reserved_pct=0.1)),
        ("NaN percentage",
         dict(exploratory_pct=float("nan"))),
        ("string percentage",
         dict(exploratory_pct="0.2")),
    ]:
        try:
            ShotBudget(**kwargs)
            print(f"  {label}: NOT rejected")
        except ShotBudgetInputError as e:
            print(f"  {label}: {type(e).__name__}")

    print("\n=== §1.2 total_shots validation ===")
    for label, val in [
        ("negative", -1),
        ("zero", 0),
        ("string", "1000"),
        ("bool True", True),
        ("exceeds MAX_TOTAL_SHOTS", MAX_TOTAL_SHOTS + 1),
    ]:
        try:
            budget.allocate(val)  # type: ignore[arg-type]
            print(f"  {label}: NOT rejected")
        except ShotBudgetInputError:
            print(f"  {label}: rejected")

    print("\n=== §1.3 Sum invariant holds for many totals ===")
    for total in (1, 2, 3, 7, 99, 100, 1000, 999_999):
        alloc = budget.allocate(total)
        s = (
            alloc.exploratory_shots + alloc.mitigation_shots
            + alloc.reserved_shots + alloc.production_shots
        )
        assert s == total, f"sum mismatch for {total}: got {s}"
    print("  sum invariant holds for all tested totals ✓")

    print("\n=== §1.4 ShotBudgetAllocation immutability + invariant ===")
    try:
        alloc.total_shots = 0  # type: ignore[misc]
    except Exception as e:
        print(f"  mutation → {type(e).__name__}")
    try:
        ShotBudgetAllocation(total_shots=1000)
    except ShotBudgetInputError as e:
        print(f"  incomplete allocation rejected: {str(e)[:60]}…")
    try:
        ShotBudgetAllocation(
            total_shots=100,
            exploratory_shots=-5,
            production_shots=105,
        )
    except ShotBudgetInputError:
        print(f"  negative field rejected ✓")

    print("\n=== §1.5 can_proceed / fits_within validation ===")
    for label, args in [
        ("negative requested", (-1, 100)),
        ("negative allocated", (10, -100)),
        ("bool requested", (True, 100)),
    ]:
        try:
            budget.can_proceed(*args)  # type: ignore[arg-type]
            print(f"  {label}: NOT rejected")
        except ShotBudgetInputError:
            print(f"  {label}: rejected")

    print("  can_proceed(200, 1000) =", budget.can_proceed(200, 1000))
    print("  fits_within(200, alloc) =", budget.fits_within(200, alloc))
    print("  fits_within(2000, alloc) =", budget.fits_within(2000, alloc))

    print("\n=== §2.1 Usage tracking ===")
    usage = budget.track_usage(alloc, used=300)
    print(f"  {usage!r}")
    print(f"  remaining: {usage.remaining}")
    print(f"  ratio_used: {usage.ratio_used:.4f}")
    usage_over = budget.track_usage(alloc, used=1500)
    print(f"  overspent_by: {usage_over.overspent_by}")

    print("\n=== §2.4 Truncation flows to production ===")
    small = ShotBudget(
        exploratory_pct=0.33, mitigation_pct=0.33, reserved_pct=0.33,
    ).allocate(99)
    print(f"  total=99, 33/33/33 split:")
    print(f"    exploratory: {small.exploratory_shots} "
          f"(ideal {99 * 0.33:.2f})")
    print(f"    mitigation: {small.mitigation_shots}")
    print(f"    reserved: {small.reserved_shots}")
    print(f"    production: {small.production_shots} "
          f"(absorbs the truncation remainder)")
    assert (
        small.exploratory_shots + small.mitigation_shots
        + small.reserved_shots + small.production_shots
    ) == 99

    print("\n=== §2.7 from_dict / from_percentages ===")
    d = alloc.to_dict()
    print(f"  to_dict keys: {sorted(d.keys())}")
    restored = ShotBudgetAllocation.from_dict(d)
    print(f"  round-trip: {restored == alloc}")
    assert restored == alloc

    # Legacy dict shape (only short keys):
    legacy_dict = {
        "total": 1000,
        "exploratory": 200,
        "production": 450,
        "mitigation": 300,
        "reserved": 50,
    }
    legacy = ShotBudgetAllocation.from_dict(legacy_dict)
    print(f"  legacy dict accepted: {legacy!r}")

    by_pct = ShotBudgetAllocation.from_percentages(
        500, exploratory_pct=0.1, mitigation_pct=0.2, reserved_pct=0.05,
    )
    print(f"  from_percentages: {by_pct!r}")

    print("\n=== §3.2 Health ===")
    h = budget.health()
    print(f"  ready: {h['ready']}")
    print(f"  production_pct: {h['production_pct']}")
    print(f"  config: {h['config']}")

    print("\n=== §2.5 Direct construction requires sum invariant ===")
    manual = ShotBudgetAllocation(
        total_shots=100,
        exploratory_shots=20,
        mitigation_shots=30,
        reserved_shots=5,
        production_shots=45,
    )
    print(f"  manual: {manual!r}")

    print("\n=== §1.4 _skip_validation escape hatch (audit reconstruction) ===")
    bad = ShotBudgetAllocation(
        total_shots=1000, exploratory_shots=1,
        _skip_validation=True,
    )
    print(f"  reconstructed with skipped validation: "
          f"sum={bad.exploratory_shots + bad.production_shots + bad.mitigation_shots + bad.reserved_shots}")

    print("\n=== Config ===")
    cfg = ShotBudgetConfig(
        exploratory_pct=0.1, mitigation_pct=0.4, reserved_pct=0.1,
    )
    print(f"  config: {cfg.to_dict()}")
    print(f"  production_pct: {cfg.production_pct}")
    try:
        ShotBudgetConfig(exploratory_pct=-0.1)
    except ShotBudgetInputError as e:
        print(f"  negative pct rejected: {type(e).__name__}")

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
