# src/decision/carbon_aware_decision_core.py

"""
Carbon-Aware Decision Core (Extended)

Dual-axis decision engine that combines:

- **Carbon awareness** — zones derived from grid carbon intensity (gCO2/kWh).
- **Helium awareness** — zones derived from GPU helium dependency and global
  helium supply status.

The engine emits an :class:`ExecutionDecision` describing the chosen action
(``execute_full`` / ``execute_throttled`` / ``execute_minimal`` / ``defer``)
and the allowed power budget.

Enhancements
------------
- Weights validated to sum to 1.0; thresholds validated to be ordered.
- Robust handling of ``scarcity_level`` as either an ``Enum`` or a string.
- Configurable helium-scarcity dependency cutoffs (no more magic ``0.5``/``0.3``).
- Frozen :class:`ExecutionDecision` with an auto-populated UTC timestamp.
- Full validation of inputs (``carbon_intensity`` finite and >= 0; weights in
  ``[0, 1]``; ``power_budget`` clamped to ``[0, 1]``).
- Thread-safe via ``RLock``.
- Bounded decision-history ring-buffer with ``statistics()``.
- Structured serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Sync context manager for scoped decision batches.
- Custom :class:`CarbonDecisionError`.
- Lazy ``%s`` logging, ``__repr__``, and a smoke test under ``__main__``.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


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


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DecisionConfig:
    """
    Tunable parameters for the carbon-aware decision engine.

    Centralizes carbon thresholds, helium thresholds, multi-objective weights,
    and the scarcity-driven dependency cutoffs used by helium zoning.
    """

    # Carbon thresholds (gCO2/kWh) — must be strictly increasing.
    carbon_green_threshold: float = 50.0
    carbon_yellow_threshold: float = 200.0
    carbon_red_threshold: float = 400.0

    # Helium thresholds (dependency scores) — must be in (0, 1].
    helium_yellow_threshold: float = 0.6
    helium_red_threshold: float = 0.8
    helium_critical_threshold: float = 0.95

    # Scarcity-level dependency cutoffs used when supply is constrained.
    # In `severe` scarcity, dependencies above these values trigger the
    # corresponding helium zones regardless of the configured thresholds.
    helium_severe_critical_cutoff: float = 0.5
    helium_severe_red_cutoff: float = 0.3

    # Feature toggle.
    helium_aware_enabled: bool = True

    # Decision weights (must sum to 1.0).
    carbon_weight: float = 0.6
    helium_weight: float = 0.4

    # Composite-score decision band boundaries.
    band_green: float = 1.0     # score <  band_green                → green
    band_yellow: float = 1.8    # score <  band_yellow               → yellow
    band_red: float = 2.5       # score <  band_red                  → red
                                # score >= band_red                  → critical

    # History ring-buffer size.
    max_history: int = 1000

    def __post_init__(self) -> None:
        # Carbon thresholds: positive, strictly increasing.
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

        # Helium thresholds: in (0, 1], strictly increasing.
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

        # Scarcity cutoffs: in [0, 1], critical >= red.
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

        # Weights: non-negative and sum to 1.
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

        # Decision bands: increasing.
        if not (self.band_green < self.band_yellow < self.band_red):
            raise CarbonDecisionError(
                "decision bands must be strictly increasing: "
                "band_green < band_yellow < band_red."
            )

        if self.max_history <= 0:
            raise CarbonDecisionError("max_history must be > 0.")

    # ----- convenience builders ------------------------------------------
    @classmethod
    def from_legacy_dict(cls, config: Optional[Mapping[str, Any]]) -> "DecisionConfig":
        """
        Build a :class:`DecisionConfig` from the original flat config dict.

        Recognized keys (all optional):
        ``carbon_green_threshold``, ``carbon_yellow_threshold``,
        ``carbon_red_threshold``, ``helium_yellow_threshold``,
        ``helium_red_threshold``, ``helium_critical_threshold``,
        ``helium_aware_enabled``, ``carbon_weight``, ``helium_weight``.
        """
        cfg = dict(config or {})
        kwargs: Dict[str, Any] = {}
        for key in (
            "carbon_green_threshold",
            "carbon_yellow_threshold",
            "carbon_red_threshold",
            "helium_yellow_threshold",
            "helium_red_threshold",
            "helium_critical_threshold",
            "helium_severe_critical_cutoff",
            "helium_severe_red_cutoff",
            "helium_aware_enabled",
            "carbon_weight",
            "helium_weight",
            "band_green",
            "band_yellow",
            "band_red",
            "max_history",
        ):
            if key in cfg:
                kwargs[key] = cfg[key]
        return cls(**kwargs)


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

    action: str                                     # 'execute_full' | 'execute_throttled' | 'execute_minimal' | 'defer'
    power_budget: float                             # 0.0 to 1.0
    carbon_zone: CarbonZone
    helium_zone: Optional[HeliumZone] = None
    helium_aware_flag: bool = False
    reasoning: str = ""
    target_hardware: Optional[str] = None
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
        # Clamp silently rather than rejecting — matches legacy behavior on
        # the boundary 0.0 / 1.0 while still protecting against NaN/Inf.
        object.__setattr__(self, "power_budget", max(0.0, min(1.0, pb)))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "power_budget": self.power_budget,
            "carbon_zone": self.carbon_zone.value,
            "helium_zone": self.helium_zone.value if self.helium_zone else None,
            "helium_aware_flag": self.helium_aware_flag,
            "reasoning": self.reasoning,
            "target_hardware": self.target_hardware,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExecutionDecision":
        if not isinstance(data, Mapping):
            raise CarbonDecisionError(
                f"ExecutionDecision.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        ts = data.get("timestamp")
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)

        helium_raw = data.get("helium_zone")
        helium = HeliumZone(helium_raw) if helium_raw else None

        return cls(
            action=str(data["action"]),
            power_budget=float(data["power_budget"]),
            carbon_zone=CarbonZone(data["carbon_zone"]),
            helium_zone=helium,
            helium_aware_flag=bool(data.get("helium_aware_flag", False)),
            reasoning=str(data.get("reasoning", "")),
            target_hardware=data.get("target_hardware"),
            timestamp=timestamp,
        )


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

    # Score mappings for zone combination.
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

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        decision_config: Optional[DecisionConfig] = None,
        strict: bool = True,
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
        """
        if decision_config is not None:
            self._config = decision_config
        else:
            self._config = DecisionConfig.from_legacy_dict(config)

        self._strict: bool = bool(strict)
        # Preserve the legacy ``self.config`` attribute so callers that
        # inspect it keep working.
        self.config: Dict[str, Any] = dict(config or {})

        # Legacy attributes preserved for backward compatibility.
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
        self._ctx_start: Optional[float] = None

        logger.debug(
            "CarbonAwareDecisionCore initialized "
            "(helium_aware=%s, weights=(%.2f, %.2f), strict=%s)",
            self.helium_aware_enabled,
            self.weights["carbon"],
            self.weights["helium"],
            self._strict,
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
            attribute, or a plain string). Recognized values: ``'severe'``,
            ``'critical'``, ``'caution'``, anything else treated as normal.

        Returns
        -------
        ExecutionDecision
        """
        # ---- Validate carbon intensity -------------------------------
        ci = self._validate_carbon_intensity(carbon_intensity)

        # ---- Validate workload profile -------------------------------
        if workload_profile is None:
            if self._strict:
                raise CarbonDecisionError("workload_profile must not be None.")
            logger.warning("workload_profile is None; using defaults.")
            workload_profile = _DefaultWorkloadProfile()

        # ---- Carbon zone ---------------------------------------------
        carbon_zone = self._get_carbon_zone(ci)

        # ---- Helium zone (if enabled + data available) ---------------
        helium_zone: Optional[HeliumZone] = None
        helium_aware = False
        if self.helium_aware_enabled and helium_supply_status is not None:
            helium_profile = getattr(workload_profile, "helium_profile", None)
            if helium_profile is not None:
                try:
                    helium_zone = self._get_helium_zone(
                        helium_profile, helium_supply_status
                    )
                    helium_aware = True
                except Exception as exc:
                    if self._strict:
                        raise CarbonDecisionError(
                            f"Failed to compute helium zone: {exc}"
                        ) from exc
                    logger.warning(
                        "Helium zone computation failed (%s); "
                        "falling back to carbon-only decision.",
                        exc,
                    )

        # ---- Combine -----------------------------------------------
        if helium_aware and helium_zone is not None:
            final_action, power_budget, reasoning = self._combine_decisions(
                carbon_zone, helium_zone, workload_profile
            )
        else:
            final_action, power_budget, reasoning = self._carbon_only_decision(
                carbon_zone, workload_profile
            )

        decision = ExecutionDecision(
            action=final_action,
            power_budget=power_budget,
            carbon_zone=carbon_zone,
            helium_zone=helium_zone,
            helium_aware_flag=helium_aware,
            reasoning=reasoning,
            target_hardware=getattr(workload_profile, "target_hardware", None),
        )

        with self._lock:
            self._history.append(decision)

        logger.info(
            "Decision: action=%s power=%.2f carbon=%s helium=%s aware=%s "
            "reason=%s",
            decision.action,
            decision.power_budget,
            decision.carbon_zone.value,
            decision.helium_zone.value if decision.helium_zone else "n/a",
            decision.helium_aware_flag,
            decision.reasoning,
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

    # -------------------------------------------------------- carbon zone
    def _get_carbon_zone(self, carbon_intensity: float) -> CarbonZone:
        """Determine carbon zone based on intensity (gCO2/kWh)."""
        if carbon_intensity < self.carbon_thresholds["green"]:
            return CarbonZone.GREEN
        if carbon_intensity < self.carbon_thresholds["yellow"]:
            return CarbonZone.YELLOW
        if carbon_intensity < self.carbon_thresholds["red"]:
            return CarbonZone.RED
        return CarbonZone.CRITICAL

    # -------------------------------------------------------- helium zone
    @staticmethod
    def _scarcity_level_str(helium_supply_status: Any) -> str:
        """Return the scarcity level as a lowercase string."""
        raw = getattr(helium_supply_status, "scarcity_level", None)
        if raw is None:
            return "normal"
        # Enum with .value
        if hasattr(raw, "value"):
            raw = raw.value
        return str(raw).lower()

    def _get_helium_zone(
        self,
        helium_profile: Any,
        helium_supply_status: Any,
    ) -> HeliumZone:
        """
        Determine the helium zone from the workload dependency and the global
        helium supply status.

        Two regimes:

        - **Threshold regime** (``caution`` / ``critical`` / normal) — uses
          the configured ``helium_thresholds`` to map dependency to a zone.
        - **Severe regime** — overrides thresholds with the more aggressive
          ``helium_severe_*_cutoff`` values, because in severe scarcity even
          moderate dependency is problematic.
        """
        dependency = getattr(helium_profile, "dependency_score", None)
        if dependency is None:
            raise CarbonDecisionError(
                "helium_profile must expose 'dependency_score'."
            )
        if isinstance(dependency, bool) or not isinstance(dependency, (int, float)):
            raise CarbonDecisionError(
                f"dependency_score must be numeric, got {type(dependency).__name__}."
            )
        dep = float(dependency)
        if math.isnan(dep) or math.isinf(dep) or not 0.0 <= dep <= 1.0:
            raise CarbonDecisionError(
                f"dependency_score must be in [0, 1], got {dependency!r}."
            )

        level = self._scarcity_level_str(helium_supply_status)
        th = self.helium_thresholds
        cfg = self._config

        if level == "severe":
            # Severe regime: aggressive cutoffs.
            if dep > cfg.helium_severe_critical_cutoff:
                return HeliumZone.HELIUM_CRITICAL
            if dep > cfg.helium_severe_red_cutoff:
                return HeliumZone.HELIUM_RED
            return HeliumZone.HELIUM_YELLOW

        if level == "critical":
            if dep >= th["critical"]:
                return HeliumZone.HELIUM_CRITICAL
            if dep >= th["red"]:
                return HeliumZone.HELIUM_RED
            if dep >= th["yellow"]:
                return HeliumZone.HELIUM_YELLOW
            return HeliumZone.HELIUM_GREEN

        if level == "caution":
            if dep >= th["critical"]:
                return HeliumZone.HELIUM_RED
            if dep >= th["red"]:
                return HeliumZone.HELIUM_YELLOW
            return HeliumZone.HELIUM_GREEN

        # Normal supply: only very high dependency warrants a warning zone.
        if dep >= th["critical"]:
            return HeliumZone.HELIUM_YELLOW
        return HeliumZone.HELIUM_GREEN

    # -------------------------------------------------------- combine
    def _combine_decisions(
        self,
        carbon_zone: CarbonZone,
        helium_zone: HeliumZone,
        workload_profile: Any,
    ) -> Tuple[str, float, str]:
        """
        Combine carbon and helium decisions using the configured weights.

        Conservative philosophy: the composite score reflects the average
        constraint level, and the decision band picks the corresponding
        action. Deferrable + red ⇒ defer; non-deferrable + red ⇒ minimal.
        """
        cfg = self._config
        carbon_score = self._CARBON_SCORES[carbon_zone]
        helium_score = self._HELIUM_SCORES[helium_zone]

        combined = (
            carbon_score * self.weights["carbon"]
            + helium_score * self.weights["helium"]
        )

        deferrable = bool(getattr(workload_profile, "deferrable", True))

        if combined >= cfg.band_red:
            return (
                "defer",
                0.0,
                f"Critical constraints: Carbon={carbon_zone.value}, "
                f"Helium={helium_zone.value}",
            )

        if combined >= cfg.band_yellow:
            if deferrable:
                return (
                    "defer",
                    0.0,
                    f"Red zone - deferring task due to "
                    f"{carbon_zone.value}/{helium_zone.value}",
                )
            return (
                "execute_minimal",
                0.2,
                "Red zone but non-deferrable - minimal execution",
            )

        if combined >= cfg.band_green:
            return (
                "execute_throttled",
                0.5,
                "Yellow zone - throttled execution",
            )

        return ("execute_full", 1.0, "Green zone - full execution")

    # -------------------------------------------------------- carbon-only
    def _carbon_only_decision(
        self,
        carbon_zone: CarbonZone,
        workload_profile: Any,
    ) -> Tuple[str, float, str]:
        """Fallback carbon-only decision logic."""
        deferrable = bool(getattr(workload_profile, "deferrable", True))

        if carbon_zone == CarbonZone.GREEN:
            return "execute_full", 1.0, "Green carbon zone"
        if carbon_zone == CarbonZone.YELLOW:
            return "execute_throttled", 0.6, "Yellow carbon zone"
        if carbon_zone == CarbonZone.RED:
            if deferrable:
                return "defer", 0.0, "Red carbon zone - deferring"
            return (
                "execute_minimal",
                0.3,
                "Red carbon zone but non-deferrable",
            )
        # CRITICAL
        return "defer", 0.0, "Critical carbon zone"

    # -------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the recorded decisions."""
        with self._lock:
            history = list(self._history)

        if not history:
            return {
                "count": 0,
                "by_action": {},
                "by_carbon_zone": {},
                "by_helium_zone": {},
                "helium_aware_ratio": None,
                "mean_power_budget": None,
                "deferred_ratio": None,
            }

        by_action: Dict[str, int] = {}
        by_carbon: Dict[str, int] = {}
        by_helium: Dict[str, int] = {}
        for d in history:
            by_action[d.action] = by_action.get(d.action, 0) + 1
            by_carbon[d.carbon_zone.value] = by_carbon.get(d.carbon_zone.value, 0) + 1
            if d.helium_zone is not None:
                by_helium[d.helium_zone.value] = by_helium.get(d.helium_zone.value, 0) + 1

        powers = [d.power_budget for d in history]
        helium_aware = sum(1 for d in history if d.helium_aware_flag)
        deferred = sum(1 for d in history if d.action == "defer")

        return {
            "count": len(history),
            "by_action": by_action,
            "by_carbon_zone": by_carbon,
            "by_helium_zone": by_helium,
            "helium_aware_ratio": helium_aware / len(history),
            "mean_power_budget": sum(powers) / len(powers),
            "deferred_ratio": deferred / len(history),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset in-memory state; optionally clear decision history."""
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug("CarbonAwareDecisionCore reset (clear_history=%s)", clear_history)

    # -------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
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
        cfg_data = dict(data.get("config", {}) or {})
        cfg = DecisionConfig(
            carbon_green_threshold=float(cfg_data.get("carbon_green_threshold", 50.0)),
            carbon_yellow_threshold=float(cfg_data.get("carbon_yellow_threshold", 200.0)),
            carbon_red_threshold=float(cfg_data.get("carbon_red_threshold", 400.0)),
            helium_yellow_threshold=float(cfg_data.get("helium_yellow_threshold", 0.6)),
            helium_red_threshold=float(cfg_data.get("helium_red_threshold", 0.8)),
            helium_critical_threshold=float(cfg_data.get("helium_critical_threshold", 0.95)),
            helium_severe_critical_cutoff=float(
                cfg_data.get("helium_severe_critical_cutoff", 0.5)
            ),
            helium_severe_red_cutoff=float(
                cfg_data.get("helium_severe_red_cutoff", 0.3)
            ),
            helium_aware_enabled=bool(cfg_data.get("helium_aware_enabled", True)),
            carbon_weight=float(cfg_data.get("carbon_weight", 0.6)),
            helium_weight=float(cfg_data.get("helium_weight", 0.4)),
            band_green=float(cfg_data.get("band_green", 1.0)),
            band_yellow=float(cfg_data.get("band_yellow", 1.8)),
            band_red=float(cfg_data.get("band_red", 2.5)),
            max_history=int(cfg_data.get("max_history", 1000)),
        )
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
            raise CarbonDecisionError(f"Invalid JSON payload: {exc}") from exc

    # -------------------------------------------------------- context manager
    def __enter__(self) -> "CarbonAwareDecisionCore":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped carbon-aware decision session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "Decision-core scope exited with %s after %.4fs.",
                exc_type.__name__,
                elapsed,
            )
            return
        logger.info(
            "Decision-core scope closed in %.4fs (%d decision(s)).",
            elapsed,
            len(self._history),
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonAwareDecisionCore("
                f"helium_aware={self.helium_aware_enabled}, "
                f"weights=(carbon={self.weights['carbon']:.2f}, "
                f"helium={self.weights['helium']:.2f}), "
                f"decisions={len(self._history)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Default workload profile (used when strict=False and profile is None)
# --------------------------------------------------------------------------- #
class _DefaultWorkloadProfile:
    """Minimal fallback profile matching the original attribute contract."""

    deferrable: bool = True
    priority: int = 5
    target_hardware: Optional[str] = None
    helium_profile: Optional[Any] = None


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "CarbonZone",
    "HeliumZone",
    "ExecutionDecision",
    "CarbonAwareDecisionCore",
    "DecisionConfig",
    "CarbonDecisionError",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m decision.carbon_aware_decision_core
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Fake workload profile / helium profile / supply status ------- #
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

    core = CarbonAwareDecisionCore()

    scenarios = [
        # (label, carbon, dep, scarcity, deferrable)
        ("green-normal",        20.0, 0.10, _Scarcity.NORMAL,   True),
        ("yellow-caution",     120.0, 0.65, _Scarcity.CAUTION,  True),
        ("red-critical",       350.0, 0.85, _Scarcity.CRITICAL, True),
        ("critical-severe",    450.0, 0.90, _Scarcity.SEVERE,   True),
        ("red-nondeferrable",  350.0, 0.85, _Scarcity.CRITICAL, False),
        ("carbon-only",         20.0, None, None,               True),
    ]

    print(f"{'scenario':<22} {'action':<18} {'power':<6} "
          f"{'carbon':<9} {'helium':<16} {'aware':<6}")
    print("-" * 90)
    for label, ci, dep, level, deferrable in scenarios:
        wp = _Workload(
            deferrable=deferrable,
            helium_profile=_HeliumProfile(dep) if dep is not None else None,
        )
        supply = _Supply(level) if level is not None else None
        d = core.make_decision(wp, ci, supply)
        print(
            f"{label:<22} {d.action:<18} {d.power_budget:<6.2f} "
            f"{d.carbon_zone.value:<9} "
            f"{(d.helium_zone.value if d.helium_zone else '-'):<16} "
            f"{d.helium_aware_flag!s:<6}"
        )

    print("\nStatistics:", core.statistics())

    # ---- Serialization round-trip ----------------------------------- #
    payload = core.to_json()
    restored = CarbonAwareDecisionCore.from_json(payload)
    assert restored.to_dict() == core.to_dict()
    print("Serialization round-trip OK.")

    # ---- Context manager -------------------------------------------- #
    with CarbonAwareDecisionCore() as scoped:
        scoped.make_decision(_Workload(), 20.0)
    print("Context-managed decision OK.")

    # ---- Validation failures ---------------------------------------- #
    valid_wp = _Workload()
    for bad in (
        # config with weights not summing to 1
        dict(carbon_weight=0.7, helium_weight=0.7),
        # config with non-increasing carbon thresholds
        dict(carbon_green_threshold=100, carbon_yellow_threshold=50),
    ):
        try:
            CarbonAwareDecisionCore(bad)
        except CarbonDecisionError as exc:
            print("Rejected config as expected:", exc)
        else:  # pragma: no cover
            raise AssertionError(f"Expected config rejection: {bad!r}")

    for bad_ci in (float("nan"), float("inf"), -1.0, "not-a-number"):
        try:
            core.make_decision(valid_wp, bad_ci)  # type: ignore[arg-type]
        except CarbonDecisionError as exc:
            print("Rejected carbon_intensity as expected:", exc)
        else:  # pragma: no cover
            raise AssertionError(f"Expected rejection for {bad_ci!r}")

    # Non-strict mode should never raise.
    lenient = CarbonAwareDecisionCore(strict=False)
    _ = lenient.make_decision(valid_wp, float("nan"))
    print("Non-strict mode handled invalid input gracefully.")

    print("\nSmoke test passed.")
