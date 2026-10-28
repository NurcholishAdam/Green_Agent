# src/decision/carbon_aware_decision_core.py

"""
Carbon-Aware Decision Core (Extended) — v2.0.0
==============================================

Dual-axis decision engine that combines:

- **Carbon awareness** — zones derived from grid carbon intensity (gCO2/kWh).
- **Helium awareness** — zones derived from GPU helium dependency and global
  helium supply status.

The engine emits an :class:`ExecutionDecision` describing the chosen action
(``execute_full`` / ``execute_throttled`` / ``execute_minimal`` / ``defer``)
and the allowed power budget.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 Non-deferrable workloads in a red/critical band now get
        ``execute_minimal`` (was incorrectly deferred).
  * 2.2 ``from_dict`` delegates to ``DecisionConfig.from_legacy_dict`` so new
        config fields default correctly and are not silently dropped.
  * 2.3 Power budgets are now stored in ``DecisionConfig`` and shared across
        the carbon+helium path and the carbon-only path (previously they
        disagreed: 0.5 vs 0.6, 0.2 vs 0.3).
  * 2.4 ``_scarcity_level_str`` strips whitespace and logs unknown levels
        instead of silently treating them as ``normal``.
  * 3   **Safety override**: a critical axis (carbon CRITICAL or helium
        HELIUM_CRITICAL) forces defer/minimal even when the weighted sum would
        otherwise not reach ``band_red``. This is the single biggest correctness
        fix — previously ``GREEN + HELIUM_CRITICAL`` yielded ``execute_throttled``.

**P1 robustness / observability**
  * 2.5 ``__enter__`` / ``__exit__`` take the RLock; scope stats are captured.
  * 4.1 ``priority`` now influences the decision: high-priority + would-defer
        degrades to ``execute_minimal`` rather than deferring.
  * 4.3 Stable ``ReasonCode`` enum for machine consumption; the free-text
        ``reasoning`` is preserved.
  * 4.4 Pluggable ``metrics_hook(event, fields)`` for Prometheus / OTel.
  * 4.11 Scope stats via ``scope_decision_count`` inside a ``with`` block.

**P2 enhancements**
  * 4.2 ``helium_intensive_hardware`` routing — a workload targeting an
        HBM-class accelerator inherits an implicit dependency floor if no
        explicit ``helium_profile`` is supplied.
  * 4.5 ``DecisionConfig.from_profile("conservative"|"balanced"|"aggressive")``.
  * 4.6 ``dry_run=True`` on ``make_decision`` skips history + metrics.
  * 4.7 ``statistics(window=...)`` (last N decisions) and ``trend()`` helpers.
  * 4.8 ``deferred_until`` on the decision (horizon configurable).
  * 4.9 Optional carbon hysteresis (``hysteresis_pct``) to prevent flapping.
  * 4.16 Self-tests at the bottom of the file exercise every P0 fix.

Compatibility
-------------
The public surface (``CarbonZone``, ``HeliumZone``, ``ExecutionDecision``,
``DecisionConfig``, ``CarbonAwareDecisionCore``, ``CarbonDecisionError``) is
unchanged. ``__init__(config=None)`` and ``make_decision(...)`` keep their
signatures; new arguments are keyword-only.

Thread-safe, serializable, bounded in memory.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional, Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CarbonDecisionError(ValueError):
    """Raised for invalid inputs, configuration, or decision failures."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class CarbonZone(Enum):
    """Carbon intensity zones (gCO2/kWh)."""

    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"
    CRITICAL = "critical"


class HeliumZone(Enum):
    """Helium scarcity zones for workload execution."""

    HELIUM_GREEN = "helium_green"        # Normal supply, all workloads OK
    HELIUM_YELLOW = "helium_yellow"      # Constrained, throttle high-dependency
    HELIUM_RED = "helium_red"            # Severe shortage, defer high-dependency
    HELIUM_CRITICAL = "helium_critical"  # No helium, block GPU workloads


class ReasonCode(Enum):
    """
    Stable machine-readable reason codes.

    The free-text ``reasoning`` field remains for humans; ``reason_code`` is
    the contract for dashboards, metrics, and automated retry policies.
    """

    # Carbon + helium combined path
    GREEN_FULL = "green_full"
    YELLOW_THROTTLED = "yellow_throttled"
    RED_DEFERRED = "red_deferred"
    RED_MINIMAL = "red_minimal"
    CRITICAL_DEFERRED = "critical_deferred"
    CRITICAL_MINIMAL = "critical_minimal"
    HELIUM_SAFETY_DEFERRED = "helium_safety_deferred"
    HELIUM_SAFETY_MINIMAL = "helium_safety_minimal"
    CARBON_SAFETY_DEFERRED = "carbon_safety_deferred"
    CARBON_SAFETY_MINIMAL = "carbon_safety_minimal"
    PRIORITY_OVERRIDE_MINIMAL = "priority_override_minimal"

    # Carbon-only fallback
    CARBON_ONLY_GREEN_FULL = "carbon_only_green_full"
    CARBON_ONLY_YELLOW_THROTTLED = "carbon_only_yellow_throttled"
    CARBON_ONLY_RED_DEFERRED = "carbon_only_red_deferred"
    CARBON_ONLY_RED_MINIMAL = "carbon_only_red_minimal"
    CARBON_ONLY_CRITICAL_DEFERRED = "carbon_only_critical_deferred"


# --------------------------------------------------------------------------- #
# Default policy profiles (P2 4.5)
# --------------------------------------------------------------------------- #
_PROFILE_PRESETS: Dict[str, Dict[str, Any]] = {
    "conservative": dict(
        band_green=0.5, band_yellow=1.0, band_red=1.8,
        hysteresis_pct=0.10, priority_critical_threshold=2,
    ),
    "balanced": dict(),  # matches the module defaults
    "aggressive": dict(
        band_green=1.5, band_yellow=2.5, band_red=3.5,
        hysteresis_pct=0.02, priority_critical_threshold=1,
    ),
}


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DecisionConfig:
    """
    Tunable parameters for the carbon-aware decision engine.

    Centralizes carbon thresholds, helium thresholds, multi-objective weights,
    scarcity-driven dependency cutoffs, power budgets, and policy knobs.
    """

    # ---- Carbon thresholds (gCO2/kWh) — must be strictly increasing. ---- #
    carbon_green_threshold: float = 50.0
    carbon_yellow_threshold: float = 200.0
    carbon_red_threshold: float = 400.0

    # ---- Helium thresholds (dependency scores) — must be in (0, 1]. ---- #
    helium_yellow_threshold: float = 0.6
    helium_red_threshold: float = 0.8
    helium_critical_threshold: float = 0.95

    # Scarcity-level dependency cutoffs used when supply is constrained.
    helium_severe_critical_cutoff: float = 0.5
    helium_severe_red_cutoff: float = 0.3

    # Feature toggle.
    helium_aware_enabled: bool = True

    # ---- Decision weights (must sum to 1.0). ---- #
    carbon_weight: float = 0.6
    helium_weight: float = 0.4

    # ---- Composite-score decision band boundaries. ---- #
    band_green: float = 1.0
    band_yellow: float = 1.8
    band_red: float = 2.5

    # ---- Power budgets (P0 2.3: shared across both paths). ---- #
    power_full: float = 1.0
    power_throttled: float = 0.5
    power_minimal: float = 0.2
    power_defer: float = 0.0

    # ---- Priority handling (P1 4.1). ---- #
    priority_critical_threshold: int = 2  # priority <= this is "critical for company"

    # ---- Deferral metadata (P2 4.8). ---- #
    deferral_horizon_hours: float = 2.0

    # ---- Carbon hysteresis (P2 4.9). ---- #
    hysteresis_pct: float = 0.0  # 0.0 = disabled; e.g. 0.05 = 5 % margin

    # ---- Helium-intensive hardware (P2 4.2). ---- #
    helium_intensive_hardware: Tuple[str, ...] = (
        "a100", "h100", "h200", "b200", "gh200", "mi300",
    )
    helium_intensive_default_dependency: float = 0.8

    # ---- History ring-buffer size. ---- #
    max_history: int = 1000

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        # Carbon thresholds
        for name in (
            "carbon_green_threshold",
            "carbon_yellow_threshold",
            "carbon_red_threshold",
        ):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or value <= 0:
                raise CarbonDecisionError(f"{name} must be > 0 (got {value}).")
        if not (
            self.carbon_green_threshold
            < self.carbon_yellow_threshold
            < self.carbon_red_threshold
        ):
            raise CarbonDecisionError(
                "carbon thresholds must be strictly increasing: "
                "green < yellow < red."
            )

        # Helium thresholds
        for name in (
            "helium_yellow_threshold",
            "helium_red_threshold",
            "helium_critical_threshold",
        ):
            value = getattr(self, name)
            if not 0.0 < value <= 1.0:
                raise CarbonDecisionError(
                    f"{name} must be in (0, 1] (got {value})."
                )
        if not (
            self.helium_yellow_threshold
            < self.helium_red_threshold
            < self.helium_critical_threshold
        ):
            raise CarbonDecisionError(
                "helium thresholds must be strictly increasing: "
                "yellow < red < critical."
            )

        # Scarcity cutoffs
        for name in (
            "helium_severe_critical_cutoff",
            "helium_severe_red_cutoff",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise CarbonDecisionError(
                    f"{name} must be in [0, 1] (got {value})."
                )
        if self.helium_severe_red_cutoff > self.helium_severe_critical_cutoff:
            raise CarbonDecisionError(
                "helium_severe_red_cutoff must be <= helium_severe_critical_cutoff."
            )

        # Weights
        for name in ("carbon_weight", "helium_weight"):
            value = getattr(self, name)
            if value < 0:
                raise CarbonDecisionError(f"{name} must be >= 0 (got {value}).")
        weight_sum = self.carbon_weight + self.helium_weight
        if abs(weight_sum - 1.0) > 1e-6:
            raise CarbonDecisionError(
                f"carbon_weight + helium_weight must sum to 1.0 "
                f"(got {weight_sum:.6f})."
            )

        # Decision bands
        if not (self.band_green < self.band_yellow < self.band_red):
            raise CarbonDecisionError(
                "decision bands must be strictly increasing: "
                "band_green < band_yellow < band_red."
            )

        # Power budgets
        for name in ("power_full", "power_throttled",
                     "power_minimal", "power_defer"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise CarbonDecisionError(
                    f"{name} must be in [0, 1] (got {value})."
                )
        if not (self.power_full > self.power_throttled
                >= self.power_minimal >= self.power_defer):
            raise CarbonDecisionError(
                "power budgets must satisfy "
                "full > throttled >= minimal >= defer."
            )

        # Priority
        if self.priority_critical_threshold < 0:
            raise CarbonDecisionError(
                "priority_critical_threshold must be >= 0."
            )

        # Deferral horizon
        if self.deferral_horizon_hours < 0:
            raise CarbonDecisionError(
                "deferral_horizon_hours must be >= 0."
            )

        # Hysteresis
        if not 0.0 <= self.hysteresis_pct < 0.5:
            raise CarbonDecisionError(
                "hysteresis_pct must be in [0, 0.5)."
            )

        # Helium-intensive hardware
        if not 0.0 <= self.helium_intensive_default_dependency <= 1.0:
            raise CarbonDecisionError(
                "helium_intensive_default_dependency must be in [0, 1]."
            )

        if self.max_history <= 0:
            raise CarbonDecisionError("max_history must be > 0.")

    # ------------------------------------------------------- convenience
    @classmethod
    def from_legacy_dict(
        cls, config: Optional[Mapping[str, Any]]
    ) -> "DecisionConfig":
        """
        Build a :class:`DecisionConfig` from a flat config mapping.

        Only keys that match a dataclass field are consumed; unknown keys are
        ignored. This future-proofs deserialization — new fields added to
        ``DecisionConfig`` are picked up automatically instead of being
        silently defaulted (P0 2.2).
        """
        cfg = dict(config or {})
        valid = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in cfg.items() if k in valid}
        # ``helium_intensive_hardware`` arrives from JSON as a list
        hw = kwargs.get("helium_intensive_hardware")
        if isinstance(hw, list):
            kwargs["helium_intensive_hardware"] = tuple(hw)
        return cls(**kwargs)

    @classmethod
    def from_profile(
        cls, profile: str, **overrides: Any
    ) -> "DecisionConfig":
        """
        Construct a config from a named policy profile, with optional overrides.

        Available profiles: ``"conservative"``, ``"balanced"``, ``"aggressive"``.
        """
        key = profile.strip().lower()
        if key not in _PROFILE_PRESETS:
            raise CarbonDecisionError(
                f"Unknown policy profile {profile!r}. "
                f"Choose from: {sorted(_PROFILE_PRESETS)}."
            )
        merged: Dict[str, Any] = dict(_PROFILE_PRESETS[key])
        merged.update(overrides)
        return cls(**merged)


# --------------------------------------------------------------------------- #
# Decision result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ExecutionDecision:
    """
    Immutable execution decision with helium awareness.

    ``timestamp`` is auto-populated with a timezone-aware UTC datetime when
    not provided.
    """

    action: str
    power_budget: float
    carbon_zone: CarbonZone
    helium_zone: Optional[HeliumZone] = None
    helium_aware_flag: bool = False
    reasoning: str = ""
    target_hardware: Optional[str] = None
    reason_code: Optional[str] = None                      # P1 4.3
    deferred_until: Optional[datetime] = None              # P2 4.8
    carbon_estimate_at_retry: Optional[float] = None       # P2 4.8
    priority: Optional[int] = None                         # P1 4.1 (audit)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.action, str) or not self.action:
            raise CarbonDecisionError("action must be a non-empty string.")
        if not isinstance(self.carbon_zone, CarbonZone):
            raise CarbonDecisionError(
                "carbon_zone must be a CarbonZone instance."
            )
        if self.helium_zone is not None and not isinstance(
            self.helium_zone, HeliumZone
        ):
            raise CarbonDecisionError(
                "helium_zone must be a HeliumZone or None."
            )
        if not isinstance(self.power_budget, (int, float)):
            raise CarbonDecisionError("power_budget must be numeric.")
        pb = float(self.power_budget)
        if math.isnan(pb) or math.isinf(pb):
            raise CarbonDecisionError("power_budget must be finite.")
        object.__setattr__(self, "power_budget", max(0.0, min(1.0, pb)))

        if not isinstance(self.timestamp, datetime):
            raise CarbonDecisionError("timestamp must be a datetime.")
        if self.timestamp.tzinfo is None:
            object.__setattr__(
                self, "timestamp",
                self.timestamp.replace(tzinfo=timezone.utc),
            )

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "power_budget": self.power_budget,
            "carbon_zone": self.carbon_zone.value,
            "helium_zone": self.helium_zone.value if self.helium_zone else None,
            "helium_aware_flag": self.helium_aware_flag,
            "reasoning": self.reasoning,
            "target_hardware": self.target_hardware,
            "reason_code": self.reason_code,
            "deferred_until": (
                self.deferred_until.isoformat()
                if self.deferred_until else None
            ),
            "carbon_estimate_at_retry": self.carbon_estimate_at_retry,
            "priority": self.priority,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExecutionDecision":
        if not isinstance(data, Mapping):
            raise CarbonDecisionError(
                f"ExecutionDecision.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )

        # Timestamp
        ts = data.get("timestamp")
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)

        # Carbon zone
        cz_raw = data["carbon_zone"]
        carbon_zone = cz_raw if isinstance(cz_raw, CarbonZone) \
            else CarbonZone(cz_raw)

        # Helium zone
        hz_raw = data.get("helium_zone")
        if hz_raw is None:
            helium_zone = None
        elif isinstance(hz_raw, HeliumZone):
            helium_zone = hz_raw
        else:
            helium_zone = HeliumZone(hz_raw)

        # Deferred until
        du_raw = data.get("deferred_until")
        if isinstance(du_raw, datetime):
            deferred_until = du_raw
        elif isinstance(du_raw, str) and du_raw:
            deferred_until = datetime.fromisoformat(du_raw)
            if not deferred_until.tzinfo:
                deferred_until = deferred_until.replace(tzinfo=timezone.utc)
        else:
            deferred_until = None

        # reason_code accepts both str and ReasonCode
        rc = data.get("reason_code")
        if isinstance(rc, ReasonCode):
            rc = rc.value

        return cls(
            action=str(data["action"]),
            power_budget=float(data["power_budget"]),
            carbon_zone=carbon_zone,
            helium_zone=helium_zone,
            helium_aware_flag=bool(data.get("helium_aware_flag", False)),
            reasoning=str(data.get("reasoning", "")),
            target_hardware=data.get("target_hardware"),
            reason_code=rc,
            deferred_until=deferred_until,
            carbon_estimate_at_retry=(
                float(data["carbon_estimate_at_retry"])
                if data.get("carbon_estimate_at_retry") is not None
                else None
            ),
            priority=(
                int(data["priority"])
                if data.get("priority") is not None else None
            ),
            timestamp=timestamp,
        )


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #
class _DefaultWorkloadProfile:
    """Minimal fallback profile matching the original attribute contract."""

    deferrable: bool = True
    priority: int = 5
    target_hardware: Optional[str] = None
    helium_profile: Optional[Any] = None


class _ImplicitHeliumProfile:
    """A minimal helium profile generated for helium-intensive hardware."""

    __slots__ = ("dependency_score",)

    def __init__(self, dependency_score: float) -> None:
        self.dependency_score = dependency_score


# --------------------------------------------------------------------------- #
# Decision core
# --------------------------------------------------------------------------- #
class CarbonAwareDecisionCore:
    """
    Enhanced decision core with dual-axis carbon + helium awareness.

    Thread-safe, serializable, and bounded in memory. The original public
    API (``__init__(config=None)`` and ``make_decision(...)``) is preserved;
    new parameters are keyword-only with backward-compatible defaults.
    """

    _CARBON_SCORES: Mapping[CarbonZone, int] = {
        CarbonZone.GREEN: 0,
        CarbonZone.YELLOW: 1,
        CarbonZone.RED: 2,
        CarbonZone.CRITICAL: 3,
    }
    _HELIUM_SCORES: Mapping[HeliumZone, int] = {
        HeliumZone.HELIUM_GREEN: 0,
        HeliumZone.HELIUM_YELLOW: 1,
        HeliumZone.HELIUM_RED: 2,
        HeliumZone.HELIUM_CRITICAL: 3,
    }
    _KNOWN_SCARCITY = frozenset(
        {"normal", "caution", "critical", "severe"}
    )
    _ZONE_ORDER: Tuple[CarbonZone, ...] = (
        CarbonZone.GREEN, CarbonZone.YELLOW,
        CarbonZone.RED, CarbonZone.CRITICAL,
    )

    # ------------------------------------------------------------------ #
    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        decision_config: Optional[DecisionConfig] = None,
        strict: bool = True,
        metrics_hook: Optional[Callable[[str, Mapping[str, Any]], None]] = None,
    ) -> None:
        """
        Parameters
        ----------
        config : dict, optional
            Legacy flat config dict. Recognized keys are forwarded to
            :class:`DecisionConfig`.
        decision_config : DecisionConfig, optional
            Typed configuration. When provided, ``config`` is ignored.
        strict : bool, default True
            If True, invalid inputs raise :class:`CarbonDecisionError`.
            If False, they are logged and coerced to safe defaults.
        metrics_hook : callable, optional
            Called as ``hook(event_name, fields_dict)`` after every successful
            decision (or preview). Exceptions from the hook are swallowed.
        """
        if decision_config is not None:
            self._config = decision_config
        else:
            self._config = DecisionConfig.from_legacy_dict(config)

        self._strict: bool = bool(strict)
        self._metrics_hook = metrics_hook

        # Legacy attribute preserved for callers that inspect it.
        self.config: Dict[str, Any] = dict(config or {})

        # Legacy attributes (kept in sync with the typed config).
        self.carbon_thresholds: Dict[str, float] = {
            "green": self._config.carbon_green_threshold,
            "yellow": self._config.carbon_yellow_threshold,
            "red": self._config.carbon_red_threshold,
        }
        self.helium_thresholds: Dict[str, float] = {
            "yellow": self._config.helium_yellow_threshold,
            "red": self._config.helium_red_threshold,
            "critical": self._config.helium_critical_threshold,
        }
        self.helium_aware_enabled: bool = self._config.helium_aware_enabled
        self.weights: Dict[str, float] = {
            "carbon": self._config.carbon_weight,
            "helium": self._config.helium_weight,
        }

        self._lock = threading.RLock()
        self._history: Deque[ExecutionDecision] = deque(
            maxlen=self._config.max_history
        )

        # Hysteresis / scope bookkeeping.
        self._last_carbon_zone: Optional[CarbonZone] = None
        self._ctx_start: Optional[float] = None
        self._scope_history_start: Optional[int] = None

        logger.debug(
            "CarbonAwareDecisionCore v%s initialized "
            "(helium_aware=%s, weights=(%.2f, %.2f), strict=%s, "
            "hysteresis=%.0f%%)",
            __version__,
            self.helium_aware_enabled,
            self.weights["carbon"],
            self.weights["helium"],
            self._strict,
            self._config.hysteresis_pct * 100.0,
        )

    # ------------------------------------------------------------------ props
    @property
    def decision_config(self) -> DecisionConfig:
        return self._config

    @property
    def history_size(self) -> int:
        with self._lock:
            return len(self._history)

    @property
    def history(self) -> List[ExecutionDecision]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- public API
    def make_decision(
        self,
        workload_profile: Any,
        carbon_intensity: float,
        helium_supply_status: Optional[Any] = None,
        *,
        dry_run: bool = False,
    ) -> ExecutionDecision:
        """
        Make an execution decision considering both carbon and helium.

        Parameters
        ----------
        workload_profile : object
            Must expose ``deferrable`` (bool, default True) and ``priority``
            (int, default 5). May expose ``helium_profile.dependency_score``
            (float in [0, 1]) and ``target_hardware`` (str).
        carbon_intensity : float
            Grid carbon intensity in gCO2/kWh. Must be finite and >= 0.
        helium_supply_status : object, optional
            Must expose ``scarcity_level`` (an ``Enum`` with a ``.value``
            attribute, or a plain string).
        dry_run : bool, default False
            If True, the decision is computed and returned but not recorded
            in history and not sent to the metrics hook.

        Returns
        -------
        ExecutionDecision
        """
        # ---- Validate carbon intensity ------------------------------- #
        ci = self._validate_carbon_intensity(carbon_intensity)

        # ---- Validate workload profile ------------------------------- #
        if workload_profile is None:
            if self._strict:
                raise CarbonDecisionError(
                    "workload_profile must not be None.")
            logger.warning("workload_profile is None; using defaults.")
            workload_profile = _DefaultWorkloadProfile()

        priority = self._read_priority(workload_profile)
        deferrable = bool(getattr(workload_profile, "deferrable", True))
        target_hardware = getattr(workload_profile, "target_hardware", None)

        # ---- Carbon zone (with hysteresis) --------------------------- #
        carbon_zone = self._get_carbon_zone(ci)

        # ---- Helium zone (with hardware boost) ----------------------- #
        helium_zone: Optional[HeliumZone] = None
        helium_aware = False
        effective_dependency: Optional[float] = None
        if self.helium_aware_enabled and helium_supply_status is not None:
            helium_profile = getattr(
                workload_profile, "helium_profile", None)
            # P2 4.2: synthesize a profile if hardware implies helium use
            if helium_profile is None and self._is_helium_intensive_hardware(
                target_hardware
            ):
                helium_profile = _ImplicitHeliumProfile(
                    self._config.helium_intensive_default_dependency
                )
            if helium_profile is not None:
                try:
                    helium_zone, effective_dependency = (
                        self._get_helium_zone_and_dep(
                            helium_profile,
                            helium_supply_status,
                            target_hardware=target_hardware,
                        )
                    )
                    helium_aware = True
                    if self.weights["helium"] <= 0.0:
                        logger.warning(
                            "helium_aware=True but helium_weight=0; the "
                            "helium axis is informational only."
                        )
                except Exception as exc:  # noqa: BLE001
                    if self._strict:
                        raise CarbonDecisionError(
                            f"Failed to compute helium zone: {exc}"
                        ) from exc
                    logger.warning(
                        "Helium zone computation failed (%s); "
                        "falling back to carbon-only decision.", exc)

        # ---- Combine ------------------------------------------------ #
        if helium_aware and helium_zone is not None:
            action, power, reasoning, reason_code = self._combine_decisions(
                carbon_zone, helium_zone, workload_profile,
            )
        else:
            action, power, reasoning, reason_code = \
                self._carbon_only_decision(carbon_zone, workload_profile)

        # ---- Deferral metadata (P2 4.8) ----------------------------- #
        deferred_until: Optional[datetime] = None
        if action == "defer" and self._config.deferral_horizon_hours > 0:
            deferred_until = datetime.now(timezone.utc) + timedelta(
                hours=self._config.deferral_horizon_hours
            )

        decision = ExecutionDecision(
            action=action,
            power_budget=power,
            carbon_zone=carbon_zone,
            helium_zone=helium_zone,
            helium_aware_flag=helium_aware,
            reasoning=reasoning,
            target_hardware=target_hardware,
            reason_code=reason_code.value,
            deferred_until=deferred_until,
            carbon_estimate_at_retry=None,
            priority=priority,
        )

        # ---- Record / log ------------------------------------------ #
        if not dry_run:
            with self._lock:
                self._history.append(decision)
                self._last_carbon_zone = carbon_zone

            logger.info(
                "Decision: action=%s power=%.2f carbon=%s helium=%s "
                "aware=%s priority=%s code=%s reason=%s",
                decision.action,
                decision.power_budget,
                decision.carbon_zone.value,
                (decision.helium_zone.value
                 if decision.helium_zone else "n/a"),
                decision.helium_aware_flag,
                priority,
                decision.reason_code,
                decision.reasoning,
            )
            self._emit_metric("decision", {
                "action": decision.action,
                "carbon_zone": decision.carbon_zone.value,
                "helium_zone": (
                    decision.helium_zone.value
                    if decision.helium_zone else "n/a"
                ),
                "reason_code": decision.reason_code,
                "priority": priority,
                "deferred": decision.action == "defer",
            })
        else:
            logger.debug(
                "Dry-run decision: action=%s carbon=%s helium=%s",
                decision.action,
                decision.carbon_zone.value,
                (decision.helium_zone.value
                 if decision.helium_zone else "n/a"),
            )

        return decision

    # -------------------------------------------------------- validation
    def _validate_carbon_intensity(self, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"carbon_intensity must be numeric, "
                f"got {type(value).__name__}."
            )
            if self._strict:
                raise CarbonDecisionError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0

        fvalue = float(value)
        if math.isnan(fvalue) or math.isinf(fvalue):
            msg = f"carbon_intensity must be finite, got {value!r}."
            if self._strict:
                raise CarbonDecisionError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0

        if fvalue < 0:
            msg = f"carbon_intensity must be >= 0, got {fvalue}."
            if self._strict:
                raise CarbonDecisionError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        return fvalue

    @staticmethod
    def _read_priority(workload_profile: Any) -> int:
        raw = getattr(workload_profile, "priority", 5)
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 5

    def _is_helium_intensive_hardware(
        self, target_hardware: Optional[str]
    ) -> bool:
        if not target_hardware:
            return False
        return target_hardware.strip().lower() in {
            h.lower() for h in self._config.helium_intensive_hardware
        }

    # -------------------------------------------------------- carbon zone
    def _carbon_zone_raw(self, carbon_intensity: float) -> CarbonZone:
        """Stateless carbon zone assignment (pre-hysteresis)."""
        th = self.carbon_thresholds
        if carbon_intensity < th["green"]:
            return CarbonZone.GREEN
        if carbon_intensity < th["yellow"]:
            return CarbonZone.YELLOW
        if carbon_intensity < th["red"]:
            return CarbonZone.RED
        return CarbonZone.CRITICAL

    def _get_carbon_zone(self, carbon_intensity: float) -> CarbonZone:
        """
        Determine the carbon zone with optional hysteresis (P2 4.9).

        When ``hysteresis_pct > 0`` and a previous zone exists, intensity must
        exceed the target boundary by ``hysteresis_pct`` before the zone flips.
        """
        h = self._config.hysteresis_pct
        last = self._last_carbon_zone
        if h <= 0.0 or last is None:
            return self._carbon_zone_raw(carbon_intensity)

        bounds = [
            self.carbon_thresholds["green"],
            self.carbon_thresholds["yellow"],
            self.carbon_thresholds["red"],
        ]
        idx = self._ZONE_ORDER.index(last)

        # Climb up while intensity clears the next boundary with margin.
        while idx < 3 and carbon_intensity >= bounds[idx] * (1.0 + h):
            idx += 1
        # Fall down while intensity clears the previous boundary with margin.
        while idx > 0 and carbon_intensity < bounds[idx - 1] * (1.0 - h):
            idx -= 1
        return self._ZONE_ORDER[idx]

    # -------------------------------------------------------- helium zone
    @staticmethod
    def _scarcity_level_str(helium_supply_status: Any) -> str:
        """
        Return the scarcity level as a normalized, validated lowercase string.

        Whitespace is stripped; unknown levels are logged and treated as
        ``normal`` (P0 2.4).
        """
        if helium_supply_status is None:
            return "normal"
        raw = getattr(helium_supply_status, "scarcity_level", None)
        if raw is None:
            return "normal"
        if hasattr(raw, "value"):
            raw = raw.value
        level = str(raw).strip().lower()
        if level not in CarbonAwareDecisionCore._KNOWN_SCARCITY:
            logger.warning(
                "Unknown helium scarcity level %r; treating as normal.",
                level,
            )
            return "normal"
        return level

    def _get_helium_zone(
        self,
        helium_profile: Any,
        helium_supply_status: Any,
    ) -> HeliumZone:
        """Backward-compatible convenience wrapper."""
        zone, _ = self._get_helium_zone_and_dep(
            helium_profile, helium_supply_status, target_hardware=None,
        )
        return zone

    def _get_helium_zone_and_dep(
        self,
        helium_profile: Any,
        helium_supply_status: Any,
        target_hardware: Optional[str] = None,
    ) -> Tuple[HeliumZone, float]:
        """
        Determine helium zone and the effective dependency used to compute it.

        The effective dependency is ``max(explicit_dependency,
        hardware_implied_dependency)`` where the hardware-implied dependency
        only applies if ``target_hardware`` is helium-intensive (P2 4.2).
        """
        dependency = getattr(helium_profile, "dependency_score", None)
        if dependency is None:
            raise CarbonDecisionError(
                "helium_profile must expose 'dependency_score'."
            )
        if isinstance(dependency, bool) or not isinstance(
            dependency, (int, float)
        ):
            raise CarbonDecisionError(
                f"dependency_score must be numeric, "
                f"got {type(dependency).__name__}."
            )
        dep = float(dependency)
        if math.isnan(dep) or math.isinf(dep) or not 0.0 <= dep <= 1.0:
            raise CarbonDecisionError(
                f"dependency_score must be in [0, 1], got {dependency!r}."
            )
        if self._is_helium_intensive_hardware(target_hardware):
            dep = max(dep, self._config.helium_intensive_default_dependency)

        level = self._scarcity_level_str(helium_supply_status)
        th = self.helium_thresholds
        cfg = self._config

        if level == "severe":
            if dep > cfg.helium_severe_critical_cutoff:
                return HeliumZone.HELIUM_CRITICAL, dep
            if dep > cfg.helium_severe_red_cutoff:
                return HeliumZone.HELIUM_RED, dep
            return HeliumZone.HELIUM_YELLOW, dep

        if level == "critical":
            if dep >= th["critical"]:
                return HeliumZone.HELIUM_CRITICAL, dep
            if dep >= th["red"]:
                return HeliumZone.HELIUM_RED, dep
            if dep >= th["yellow"]:
                return HeliumZone.HELIUM_YELLOW, dep
            return HeliumZone.HELIUM_GREEN, dep

        if level == "caution":
            if dep >= th["critical"]:
                return HeliumZone.HELIUM_RED, dep
            if dep >= th["red"]:
                return HeliumZone.HELIUM_YELLOW, dep
            return HeliumZone.HELIUM_GREEN, dep

        # Normal
        if dep >= th["critical"]:
            return HeliumZone.HELIUM_YELLOW, dep
        return HeliumZone.HELIUM_GREEN, dep

    # -------------------------------------------------------- combine
    def _power_for(self, action: str) -> float:
        return {
            "execute_full": self._config.power_full,
            "execute_throttled": self._config.power_throttled,
            "execute_minimal": self._config.power_minimal,
            "defer": self._config.power_defer,
        }.get(action, 0.0)

    def _is_priority_critical(self, priority: int) -> bool:
        return priority <= self._config.priority_critical_threshold

    def _combine_decisions(
        self,
        carbon_zone: CarbonZone,
        helium_zone: HeliumZone,
        workload_profile: Any,
    ) -> Tuple[str, float, str, ReasonCode]:
        """
        Combine carbon and helium decisions.

        Decision order:
          1. **Safety override** (P0 3): any critical axis → defer/minimal.
          2. **Priority override** (P1 4.1): high-priority + would-defer
             workloads degrade to minimal rather than defer.
          3. **Weighted composite** with ``band_green`` / ``band_yellow`` /
             ``band_red`` thresholds.
        """
        cfg = self._config
        deferrable = bool(getattr(workload_profile, "deferrable", True))
        priority = self._read_priority(workload_profile)
        priority_critical = self._is_priority_critical(priority)

        # ---- Safety override (P0 3) -------------------------------- #
        helium_critical = (helium_zone == HeliumZone.HELIUM_CRITICAL)
        carbon_critical = (carbon_zone == CarbonZone.CRITICAL)
        if helium_critical or carbon_critical:
            if not deferrable:
                code = (ReasonCode.HELIUM_SAFETY_MINIMAL
                        if helium_critical
                        else ReasonCode.CARBON_SAFETY_MINIMAL)
                return (
                    "execute_minimal",
                    self._power_for("execute_minimal"),
                    f"Critical axis (non-deferrable): "
                    f"Carbon={carbon_zone.value}, "
                    f"Helium={helium_zone.value}",
                    code,
                )
            if priority_critical:
                code = (ReasonCode.HELIUM_SAFETY_MINIMAL
                        if helium_critical
                        else ReasonCode.CARBON_SAFETY_MINIMAL)
                return (
                    "execute_minimal",
                    self._power_for("execute_minimal"),
                    f"Critical axis but priority={priority} ≤ "
                    f"{cfg.priority_critical_threshold}",
                    code,
                )
            code = (ReasonCode.HELIUM_SAFETY_DEFERRED
                    if helium_critical
                    else ReasonCode.CARBON_SAFETY_DEFERRED)
            return (
                "defer",
                self._power_for("defer"),
                f"Critical axis (deferrable): "
                f"Carbon={carbon_zone.value}, "
                f"Helium={helium_zone.value}",
                code,
            )

        # ---- Weighted composite ----------------------------------- #
        carbon_score = self._CARBON_SCORES[carbon_zone]
        helium_score = self._HELIUM_SCORES[helium_zone]
        combined = (
            carbon_score * self.weights["carbon"]
            + helium_score * self.weights["helium"]
        )

        if combined >= cfg.band_red:
            # P0 2.1: non-deferrable ⇒ minimal (was previously deferred)
            if not deferrable:
                return (
                    "execute_minimal",
                    self._power_for("execute_minimal"),
                    f"Critical constraints but non-deferrable "
                    f"(combined={combined:.2f}): "
                    f"Carbon={carbon_zone.value}, "
                    f"Helium={helium_zone.value}",
                    ReasonCode.CRITICAL_MINIMAL,
                )
            if priority_critical:
                return (
                    "execute_minimal",
                    self._power_for("execute_minimal"),
                    f"Critical constraints but priority={priority} ≤ "
                    f"{cfg.priority_critical_threshold}",
                    ReasonCode.PRIORITY_OVERRIDE_MINIMAL,
                )
            return (
                "defer",
                self._power_for("defer"),
                f"Critical constraints: Carbon={carbon_zone.value}, "
                f"Helium={helium_zone.value}",
                ReasonCode.CRITICAL_DEFERRED,
            )

        if combined >= cfg.band_yellow:
            if deferrable:
                if priority_critical:
                    # Would defer, but high priority → minimal
                    return (
                        "execute_minimal",
                        self._power_for("execute_minimal"),
                        f"Red zone but priority={priority} ≤ "
                        f"{cfg.priority_critical_threshold}",
                        ReasonCode.PRIORITY_OVERRIDE_MINIMAL,
                    )
                return (
                    "defer",
                    self._power_for("defer"),
                    f"Red zone - deferring task due to "
                    f"{carbon_zone.value}/{helium_zone.value}",
                    ReasonCode.RED_DEFERRED,
                )
            return (
                "execute_minimal",
                self._power_for("execute_minimal"),
                "Red zone but non-deferrable - minimal execution",
                ReasonCode.RED_MINIMAL,
            )

        if combined >= cfg.band_green:
            return (
                "execute_throttled",
                self._power_for("execute_throttled"),
                "Yellow zone - throttled execution",
                ReasonCode.YELLOW_THROTTLED,
            )

        return (
            "execute_full",
            self._power_for("execute_full"),
            "Green zone - full execution",
            ReasonCode.GREEN_FULL,
        )

    # -------------------------------------------------------- carbon-only
    def _carbon_only_decision(
        self,
        carbon_zone: CarbonZone,
        workload_profile: Any,
    ) -> Tuple[str, float, str, ReasonCode]:
        """Fallback carbon-only decision logic."""
        deferrable = bool(getattr(workload_profile, "deferrable", True))
        priority = self._read_priority(workload_profile)
        priority_critical = self._is_priority_critical(priority)

        if carbon_zone == CarbonZone.GREEN:
            return (
                "execute_full",
                self._power_for("execute_full"),
                "Green carbon zone",
                ReasonCode.CARBON_ONLY_GREEN_FULL,
            )
        if carbon_zone == CarbonZone.YELLOW:
            return (
                "execute_throttled",
                self._power_for("execute_throttled"),
                "Yellow carbon zone",
                ReasonCode.CARBON_ONLY_YELLOW_THROTTLED,
            )
        if carbon_zone == CarbonZone.RED:
            if not deferrable:
                return (
                    "execute_minimal",
                    self._power_for("execute_minimal"),
                    "Red carbon zone but non-deferrable",
                    ReasonCode.CARBON_ONLY_RED_MINIMAL,
                )
            if priority_critical:
                return (
                    "execute_minimal",
                    self._power_for("execute_minimal"),
                    f"Red carbon zone but priority={priority} ≤ "
                    f"{self._config.priority_critical_threshold}",
                    ReasonCode.PRIORITY_OVERRIDE_MINIMAL,
                )
            return (
                "defer",
                self._power_for("defer"),
                "Red carbon zone - deferring",
                ReasonCode.CARBON_ONLY_RED_DEFERRED,
            )
        # CRITICAL
        if not deferrable:
            return (
                "execute_minimal",
                self._power_for("execute_minimal"),
                "Critical carbon zone but non-deferrable",
                ReasonCode.CARBON_ONLY_RED_MINIMAL,
            )
        return (
            "defer",
            self._power_for("defer"),
            "Critical carbon zone",
            ReasonCode.CARBON_ONLY_CRITICAL_DEFERRED,
        )

    # -------------------------------------------------------- metrics
    def _emit_metric(self, event: str, fields: Mapping[str, Any]) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook failed", exc_info=True)

    # -------------------------------------------------------- statistics
    def statistics(self, window: Optional[int] = None) -> Dict[str, Any]:
        """
        Return aggregate statistics over the recorded decisions.

        ``window`` (P2 4.7): if provided, restrict to the last ``window``
        decisions. Also includes a per-reason-code breakdown (P1 4.3).
        """
        with self._lock:
            history = list(self._history)
        if window is not None:
            history = history[-window:]

        if not history:
            return {
                "count": 0,
                "by_action": {},
                "by_carbon_zone": {},
                "by_helium_zone": {},
                "by_reason_code": {},
                "helium_aware_ratio": None,
                "mean_power_budget": None,
                "deferred_ratio": None,
            }

        by_action: Dict[str, int] = {}
        by_carbon: Dict[str, int] = {}
        by_helium: Dict[str, int] = {}
        by_reason: Dict[str, int] = {}
        for d in history:
            by_action[d.action] = by_action.get(d.action, 0) + 1
            by_carbon[d.carbon_zone.value] = (
                by_carbon.get(d.carbon_zone.value, 0) + 1
            )
            if d.helium_zone is not None:
                by_helium[d.helium_zone.value] = (
                    by_helium.get(d.helium_zone.value, 0) + 1
                )
            if d.reason_code is not None:
                by_reason[d.reason_code] = (
                    by_reason.get(d.reason_code, 0) + 1
                )

        powers = [d.power_budget for d in history]
        helium_aware = sum(1 for d in history if d.helium_aware_flag)
        deferred = sum(1 for d in history if d.action == "defer")

        return {
            "count": len(history),
            "by_action": by_action,
            "by_carbon_zone": by_carbon,
            "by_helium_zone": by_helium,
            "by_reason_code": by_reason,
            "helium_aware_ratio": helium_aware / len(history),
            "mean_power_budget": sum(powers) / len(powers),
            "deferred_ratio": deferred / len(history),
        }

    def trend(self, split: int = 10) -> Dict[str, Any]:
        """
        Compare the first and last halves of the recent history (P2 4.7).

        Useful for spotting a rising defer ratio during a carbon spike.
        """
        with self._lock:
            history = list(self._history)
        if len(history) < 2:
            return {"available": False, "reason": "insufficient_history"}
        n = min(split, len(history))
        first = history[:n]
        last = history[-n:]

        def _defer_ratio(xs: List[ExecutionDecision]) -> float:
            return sum(1 for d in xs if d.action == "defer") / max(1, len(xs))

        def _mean_power(xs: List[ExecutionDecision]) -> float:
            return sum(d.power_budget for d in xs) / max(1, len(xs))

        return {
            "available": True,
            "window": n,
            "defer_ratio_delta": _defer_ratio(last) - _defer_ratio(first),
            "mean_power_delta": _mean_power(last) - _mean_power(first),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset in-memory state; optionally clear decision history."""
        with self._lock:
            self._last_carbon_zone = None
            if clear_history:
                self._history.clear()
        logger.debug(
            "CarbonAwareDecisionCore reset (clear_history=%s)",
            clear_history,
        )

    # -------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "config": asdict(self._config),
                "strict": self._strict,
                "history": [d.to_dict() for d in self._history],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonAwareDecisionCore":
        if not isinstance(data, Mapping):
            raise CarbonDecisionError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        # P0 2.2: delegate to from_legacy_dict so new config fields
        # automatically use their declared defaults.
        cfg = DecisionConfig.from_legacy_dict(data.get("config") or {})
        core = cls(
            decision_config=cfg,
            strict=bool(data.get("strict", True)),
        )
        with core._lock:
            for entry in data.get("history", []):
                core._history.append(ExecutionDecision.from_dict(entry))
        return core

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "CarbonAwareDecisionCore":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise CarbonDecisionError(
                f"Invalid JSON payload: {exc}") from exc

    # -------------------------------------------------------- context manager
    def __enter__(self) -> "CarbonAwareDecisionCore":
        with self._lock:
            self._ctx_start = time.perf_counter()
            self._scope_history_start = len(self._history)
        logger.debug("Entering scoped carbon-aware decision session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (self._ctx_start
                     if self._ctx_start is not None
                     else time.perf_counter())
            elapsed = time.perf_counter() - start
            self._ctx_start = None
            scope_start = self._scope_history_start
            self._scope_history_start = None
            total = len(self._history)
        scope_count = total - (scope_start or 0)

        if exc_type is not None:
            logger.warning(
                "Decision-core scope exited with %s after %.4fs "
                "(%d decision(s)).",
                exc_type.__name__, elapsed, scope_count,
            )
            return
        logger.info(
            "Decision-core scope closed in %.4fs (%d decision(s)).",
            elapsed, scope_count,
        )

    def scope_decision_count(self) -> int:
        """
        Number of decisions recorded since the current ``with`` block started.

        Returns ``0`` outside a ``with`` block.
        """
        with self._lock:
            if self._scope_history_start is None:
                return 0
            return len(self._history) - self._scope_history_start

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonAwareDecisionCore("
                f"version={__version__!r}, "
                f"helium_aware={self.helium_aware_enabled}, "
                f"weights=(carbon={self.weights['carbon']:.2f}, "
                f"helium={self.weights['helium']:.2f}), "
                f"decisions={len(self._history)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "CarbonZone",
    "HeliumZone",
    "ReasonCode",
    "ExecutionDecision",
    "CarbonAwareDecisionCore",
    "DecisionConfig",
    "CarbonDecisionError",
]


# --------------------------------------------------------------------------- #
# Local smoke test + P0 regression assertions
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    from dataclasses import dataclass

    @dataclass
    class _HeliumProfile:
        dependency_score: float

    @dataclass
    class _Workload:
        deferrable: bool = True
        priority: int = 5
        target_hardware: Optional[str] = "a100"
        helium_profile: Optional[_HeliumProfile] = None

    class _Scarcity(Enum):
        NORMAL = "normal"
        CAUTION = "caution"
        CRITICAL = "critical"
        SEVERE = "severe"

    @dataclass
    class _Supply:
        scarcity_level: _Scarcity

    # ------------------------------------------------------------ #
    # 1. Smoke test scenarios
    # ------------------------------------------------------------ #
    core = CarbonAwareDecisionCore()

    scenarios = [
        # label,             carbon, dep, scarcity, deferrable
        ("green-normal",       20.0, 0.10, _Scarcity.NORMAL,   True),
        ("yellow-caution",    120.0, 0.65, _Scarcity.CAUTION,  True),
        ("red-critical",      350.0, 0.85, _Scarcity.CRITICAL, True),
        ("critical-severe",   450.0, 0.90, _Scarcity.SEVERE,   True),
        ("red-nondeferrable", 350.0, 0.85, _Scarcity.CRITICAL, False),
        ("carbon-only",        20.0, None, None,               True),
    ]

    print(f"{'scenario':<22} {'action':<18} {'power':<6} "
          f"{'carbon':<9} {'helium':<16} {'code':<30}")
    print("-" * 110)
    for label, ci, dep, level, deferrable in scenarios:
        wp = _Workload(
            deferrable=deferrable,
            helium_profile=(
                _HeliumProfile(dep) if dep is not None else None
            ),
        )
        supply = _Supply(level) if level is not None else None
        d = core.make_decision(wp, ci, supply)
        print(
            f"{label:<22} {d.action:<18} {d.power_budget:<6.2f} "
            f"{d.carbon_zone.value:<9} "
            f"{(d.helium_zone.value if d.helium_zone else '-'):<16} "
            f"{(d.reason_code or '-'):<30}"
        )

    print("\nStatistics:", core.statistics())
    print("Trend:", core.trend())

    # ------------------------------------------------------------ #
    # 2. P0 regression: non-deferrable + critical ⇒ minimal (not defer)
    # ------------------------------------------------------------ #
    d = core.make_decision(
        _Workload(deferrable=False,
                  helium_profile=_HeliumProfile(0.99)),
        carbon_intensity=450.0,
        helium_supply_status=_Supply(_Scarcity.CRITICAL),
    )
    assert d.action == "execute_minimal", (
        f"P0 2.1 regression: expected minimal, got {d.action}")
    assert d.reason_code in (
        "carbon_safety_minimal", "helium_safety_minimal"
    ), f"unexpected reason_code: {d.reason_code}"
    print("\n✓ P0 2.1 fixed: non-deferrable + critical ⇒ execute_minimal")

    # ------------------------------------------------------------ #
    # 3. P0 safety override: GREEN + HELIUM_CRITICAL ⇒ defer (not throttled)
    # ------------------------------------------------------------ #
    d = core.make_decision(
        _Workload(deferrable=True,
                  helium_profile=_HeliumProfile(0.99)),
        carbon_intensity=20.0,
        helium_supply_status=_Supply(_Scarcity.CRITICAL),
    )
    assert d.action == "defer", (
        f"P0 3 regression: expected defer, got {d.action}")
    assert d.reason_code == "helium_safety_deferred", d.reason_code
    print("✓ P0 3 fixed: GREEN + HELIUM_CRITICAL ⇒ defer "
          "(safety override)")

    # ------------------------------------------------------------ #
    # 4. P0 2.4: scarcity normalization (whitespace + unknown)
    # ------------------------------------------------------------ #
    @dataclass
    class _RawSupply:
        scarcity_level: str

    # Leading/trailing whitespace should normalize
    d = core.make_decision(
        _Workload(helium_profile=_HeliumProfile(0.99)),
        carbon_intensity=20.0,
        helium_supply_status=_RawSupply("  CRITICAL  "),
    )
    assert d.helium_zone == HeliumZone.HELIUM_CRITICAL, (
        f"P0 2.4 regression: got {d.helium_zone}")
    # Unknown level should fall back to normal, not crash
    d = core.make_decision(
        _Workload(helium_profile=_HeliumProfile(0.99)),
        carbon_intensity=20.0,
        helium_supply_status=_RawSupply("EMERGENCY"),
    )
    assert d.helium_zone == HeliumZone.HELIUM_YELLOW, (
        f"P0 2.4 regression: unknown level should map to normal regime, "
        f"got {d.helium_zone}")
    print("✓ P0 2.4 fixed: scarcity level whitespace normalized; "
          "unknown levels logged + treated as normal")

    # ------------------------------------------------------------ #
    # 5. P0 2.3: power budget consistency across paths
    # ------------------------------------------------------------ #
    # Carbon-only path
    d_carbon = core.make_decision(
        _Workload(deferrable=True, helium_profile=None),
        carbon_intensity=350.0,
        helium_supply_status=None,
    )
    # Combined path (helium yellow but weighted sum still in red band)
    d_combined = core.make_decision(
        _Workload(deferrable=True,
                  helium_profile=_HeliumProfile(0.85)),
        carbon_intensity=350.0,
        helium_supply_status=_Supply(_Scarcity.CRITICAL),
    )
    assert d_carbon.action == d_combined.action == "defer"
    assert d_carbon.power_budget == d_combined.power_budget == 0.0
    print("✓ P0 2.3 fixed: power budget is consistent across "
          "carbon-only and combined paths")

    # ------------------------------------------------------------ #
    # 6. P1 4.1: priority influences decision
    # ------------------------------------------------------------ #
    high_prio = core.make_decision(
        _Workload(deferrable=True, priority=1,
                  helium_profile=_HeliumProfile(0.85)),
        carbon_intensity=350.0,
        helium_supply_status=_Supply(_Scarcity.CRITICAL),
    )
    assert high_prio.action == "execute_minimal", (
        f"P1 4.1 regression: expected minimal for high-priority, "
        f"got {high_prio.action}")
    assert high_prio.reason_code == "priority_override_minimal"
    print("✓ P1 4.1: high-priority workloads degrade to minimal "
          "instead of deferring")

    # ------------------------------------------------------------ #
    # 7. P2 4.6: dry_run skips history
    # ------------------------------------------------------------ #
    before = core.history_size
    _ = core.make_decision(
        _Workload(), carbon_intensity=20.0, dry_run=True,
    )
    assert core.history_size == before, "dry_run should not record history"
    print("✓ P2 4.6: dry_run skips history append")

    # ------------------------------------------------------------ #
    # 8. P2 4.8: deferred_until populated when action == defer
    # ------------------------------------------------------------ #
    d = core.make_decision(
        _Workload(), carbon_intensity=450.0,
    )
    assert d.action == "defer" and d.deferred_until is not None
    print(f"✓ P2 4.8: deferred_until={d.deferred_until.isoformat()}")

    # ------------------------------------------------------------ #
    # 9. P2 4.9: hysteresis prevents flapping
    # ------------------------------------------------------------ #
    hcore = CarbonAwareDecisionCore(
        decision_config=DecisionConfig(hysteresis_pct=0.05),
    )
    _ = hcore.make_decision(_Workload(), carbon_intensity=20.0)
    # 51 is technically yellow, but with 5% hysteresis the green→yellow
    # boundary is 52.5, so we should stay GREEN.
    d = hcore.make_decision(_Workload(), carbon_intensity=51.0)
    assert d.carbon_zone == CarbonZone.GREEN, (
        f"P2 4.9 regression: hysteresis should keep us in GREEN, "
        f"got {d.carbon_zone}")
    # 53 should cross
    d = hcore.make_decision(_Workload(), carbon_intensity=53.0)
    assert d.carbon_zone == CarbonZone.YELLOW
    # 48 should return to green
    d = hcore.make_decision(_Workload(), carbon_intensity=48.0)
    assert d.carbon_zone == CarbonZone.GREEN
    print("✓ P2 4.9: hysteresis prevents carbon-zone flapping")

    # ------------------------------------------------------------ #
    # 10. P2 4.2: helium-intensive hardware boosts dependency
    # ------------------------------------------------------------ #
    d = core.make_decision(
        _Workload(target_hardware="h100",
                  helium_profile=_HeliumProfile(0.1)),  # low explicit dep
        carbon_intensity=20.0,
        helium_supply_status=_Supply(_Scarcity.CRITICAL),
    )
    # Effective dep should be 0.8 (intensive default) ⇒ HELIUM_RED
    assert d.helium_zone == HeliumZone.HELIUM_RED, (
        f"P2 4.2 regression: got {d.helium_zone}")
    print("✓ P2 4.2: h100 implicit dependency floor applied")

    # ------------------------------------------------------------ #
    # 11. P2 4.5: policy profiles
    # ------------------------------------------------------------ #
    conservative = DecisionConfig.from_profile("conservative")
    aggressive = DecisionConfig.from_profile("aggressive")
    assert conservative.band_red < aggressive.band_red
    print("✓ P2 4.5: policy profiles load correctly")

    # ------------------------------------------------------------ #
    # 12. Serialization round-trip
    # ------------------------------------------------------------ #
    payload = core.to_json()
    restored = CarbonAwareDecisionCore.from_json(payload)
    assert restored.to_dict() == core.to_dict()
    print("✓ Serialization round-trip (with new fields) OK")

    # ------------------------------------------------------------ #
    # 13. P1 4.11: scope stats
    # ------------------------------------------------------------ #
    scoped = CarbonAwareDecisionCore()
    with scoped:
        scoped.make_decision(_Workload(), 20.0)
        scoped.make_decision(_Workload(), 20.0)
        assert scoped.scope_decision_count() == 2
    print("✓ P1 4.11: scope_decision_count tracks decisions inside `with`")

    # ------------------------------------------------------------ #
    # 14. P0 2.2: from_dict defaults are picked up for new fields
    # ------------------------------------------------------------ #
    legacy_payload = {
        "config": {
            "carbon_green_threshold": 40.0,
            "carbon_yellow_threshold": 180.0,
            "carbon_red_threshold": 380.0,
            # Omit all the new fields → from_legacy_dict must supply defaults
        },
        "strict": True,
        "history": [],
    }
    restored = CarbonAwareDecisionCore.from_dict(legacy_payload)
    assert restored.decision_config.carbon_green_threshold == 40.0
    assert restored.decision_config.hysteresis_pct == 0.0  # default
    assert restored.decision_config.deferral_horizon_hours == 2.0  # default
    print("✓ P0 2.2 fixed: from_dict applies new-field defaults correctly")

    # ------------------------------------------------------------ #
    # 15. Validation failures still raise
    # ------------------------------------------------------------ #
    valid_wp = _Workload()
    for bad in (
        dict(carbon_weight=0.7, helium_weight=0.7),
        dict(carbon_green_threshold=100, carbon_yellow_threshold=50),
    ):
        try:
            CarbonAwareDecisionCore(bad)
        except CarbonDecisionError as exc:
            print(f"✓ Rejected config as expected: {exc}")
        else:
            raise AssertionError(f"Expected config rejection: {bad!r}")

    for bad_ci in (float("nan"), float("inf"), -1.0, "not-a-number"):
        try:
            core.make_decision(valid_wp, bad_ci)  # type: ignore[arg-type]
        except CarbonDecisionError:
            pass
        else:
            raise AssertionError(f"Expected rejection for {bad_ci!r}")
    print("✓ Validation failures still raise CarbonDecisionError")

    # Non-strict mode never raises
    lenient = CarbonAwareDecisionCore(strict=False)
    _ = lenient.make_decision(valid_wp, float("nan"))
    print("✓ Non-strict mode handled invalid input gracefully")

    print("\n✓ All smoke + regression tests passed.")
