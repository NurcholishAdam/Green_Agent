# src/interpretation/workload_interpreter.py

"""
Workload Interpreter (v5.1)
===========================

Converts raw task JSON into a structured :class:`WorkloadProfile`, including
helium-dependency analysis.

Fixes in v5.1 (see review)
--------------------------
  * P0 — Carbon is recomputed after the energy penalty. Previously every
         high-dependency workload under-reported carbon by 17 %+ because
         carbon was derived from pre-penalty energy.
  * P0 — ``helium_config`` is now read back in ``from_dict``. Previously
         it was written to ``to_dict`` and silently discarded on restore.
  * P0 — ``config=`` dict honours *every* ``InterpretationConfig`` field.
         Previously every key except ``enable_helium_analysis`` was
         silently ignored.
  * P0 — All numeric coercion goes through ``_safe_int`` / ``_safe_float``
         / ``_coerce_mapping``; malformed input raises
         ``InterpretationError`` rather than leaking ``ValueError`` /
         ``TypeError`` / ``AttributeError`` / ``KeyError``.
  * P0 — ``x or default`` patterns replaced with explicit ``is None``
         checks so ``cpu_cores=0`` and ``priority=0`` are no longer
         silently rewritten.
  * P1 — ``math.isfinite`` validation on ``energy_estimate_kwh`` /
         ``carbon_estimate_kg``; NaN and Inf were previously accepted.
  * P1 — ``helium_profile: []`` now raises instead of silently becoming
         ``None``.
  * P1 — ``enable_helium_analysis`` de-duplicated. It is a property
         backed by ``_config``; assignment updates both.
  * P1 — ``statistics()`` gains ``min_carbon_kg`` / ``max_carbon_kg`` /
         ``helium_mean_score`` with a consistent empty-history shape.
  * P1 — ``from_dict`` warns when restoring more history entries than
         ``max_history`` can hold (previously silently truncated).
  * P1 — ``helium_penalty_applied`` and ``base_energy_kwh`` are now
         recorded on the profile for observability.
  * P2 — ``analyze_batch()``; ``WorkloadProfile.key()`` / ``carbon_level()``
         / ``helium_level()`` / ``compare()``; ``CarbonLevel`` enum;
         ``schema_version`` in ``to_dict``.
  * P3 — Dead ``time`` import removed; ``json.dumps(default=str)`` removed;
         ``__main__`` smoke test asserts the carbon/energy invariant and
         the helium_config round-trip.

Public API (backward compatible)
--------------------------------
    InterpretationConfig, InterpretationError
    CarbonLevel
    WorkloadProfile, WorkloadInterpreter
"""

from __future__ import annotations

import json
import logging
import math
import threading
from collections import deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional, Tuple

from .helium_profile import (
    HardwareType,
    HeliumDependencyLevel,
    HeliumProfile,
    HeliumProfileConfig,
    HeliumProfileError,
)

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

__all__ = [
    "CarbonLevel",
    "InterpretationConfig",
    "InterpretationError",
    "WorkloadInterpreter",
    "WorkloadProfile",
    "__version__",
]


# =============================================================================
# Safe coercion helpers
# =============================================================================

def _safe_int(
    value: Any,
    *,
    field: str,
    default: int = 0,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    """Coerce `value` to int, raising `InterpretationError` on any problem."""
    if value is None or value == "":
        result = default
    elif isinstance(value, bool):
        raise InterpretationError(
            f"{field} must be an integer, got bool {value!r}"
        )
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise InterpretationError(
                f"{field} must be finite, got {value!r}"
            )
        if not value.is_integer():
            raise InterpretationError(
                f"{field} must be an integer, got {value!r}"
            )
        result = int(value)
    else:
        try:
            result = int(value)
        except (TypeError, ValueError) as exc:
            raise InterpretationError(
                f"{field} must be an integer, got {value!r} "
                f"({type(value).__name__})"
            ) from exc
    if min_value is not None and result < min_value:
        raise InterpretationError(
            f"{field} must be >= {min_value}, got {result!r}"
        )
    if max_value is not None and result > max_value:
        raise InterpretationError(
            f"{field} must be <= {max_value}, got {result!r}"
        )
    return result


def _safe_float(
    value: Any,
    *,
    field: str,
    default: float = 0.0,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    """Coerce `value` to a finite float, raising `InterpretationError` otherwise."""
    if value is None or value == "":
        result = default
    else:
        try:
            result = float(value)
        except (TypeError, ValueError) as exc:
            raise InterpretationError(
                f"{field} must be a number, got {value!r} "
                f"({type(value).__name__})"
            ) from exc
        if not math.isfinite(result):
            raise InterpretationError(
                f"{field} must be finite, got {value!r}"
            )
    if min_value is not None and result < min_value:
        raise InterpretationError(
            f"{field} must be >= {min_value}, got {result!r}"
        )
    if max_value is not None and result > max_value:
        raise InterpretationError(
            f"{field} must be <= {max_value}, got {result!r}"
        )
    return result


def _coerce_mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    """Return `value` if it's a Mapping, `{}` if None, else raise."""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise InterpretationError(
            f"{field} must be a Mapping (or None), got "
            f"{type(value).__name__}"
        )
    return value


def _parse_timestamp(value: Any) -> datetime:
    """Parse datetime / ISO-8601 str (accepts 'Z' on all Python versions)."""
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            ts = datetime.fromisoformat(s)
        except ValueError as exc:
            raise InterpretationError(
                f"timestamp is not ISO-8601: {value!r}"
            ) from exc
    elif value is None:
        return datetime.now(timezone.utc)
    else:
        raise InterpretationError(
            f"timestamp must be datetime or ISO string, got "
            f"{type(value).__name__}"
        )
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


# =============================================================================
# Errors & enums
# =============================================================================

class InterpretationError(ValueError):
    """Raised for invalid interpretation inputs or configuration."""


class CarbonLevel(Enum):
    """Bucketed carbon profile of a workload (drives deferral decisions)."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class InterpretationConfig:
    """Tunable parameters for workload interpretation."""
    enable_helium_analysis: bool = True
    model_size_scale_gb: float = 100.0
    data_volume_scale_gb: float = 1000.0
    base_energy_kwh: float = 0.1
    energy_per_gpu_kwh: float = 0.3
    carbon_intensity_kg_per_kwh: float = 0.4
    high_dependency_cutoff: float = 0.8
    high_dependency_energy_penalty: float = 1.2
    max_history: int = 1000
    # Carbon bucketing (used by `WorkloadProfile.carbon_level()`)
    low_carbon_kg: float = 0.5
    high_carbon_kg: float = 2.0

    def __post_init__(self) -> None:
        for name in (
            "model_size_scale_gb", "data_volume_scale_gb",
            "base_energy_kwh", "carbon_intensity_kg_per_kwh",
        ):
            val = getattr(self, name)
            if not math.isfinite(val) or val <= 0:
                raise InterpretationError(
                    f"{name} must be finite and > 0, got {val!r}"
                )
        for name in (
            "energy_per_gpu_kwh", "low_carbon_kg", "high_carbon_kg",
        ):
            val = getattr(self, name)
            if not math.isfinite(val) or val < 0:
                raise InterpretationError(
                    f"{name} must be finite and >= 0, got {val!r}"
                )
        if not 0.0 <= self.high_dependency_cutoff <= 1.0:
            raise InterpretationError(
                f"high_dependency_cutoff must be in [0, 1], "
                f"got {self.high_dependency_cutoff!r}"
            )
        if self.high_dependency_energy_penalty < 1.0:
            raise InterpretationError(
                f"high_dependency_energy_penalty must be >= 1.0, "
                f"got {self.high_dependency_energy_penalty!r}"
            )
        if self.max_history <= 0:
            raise InterpretationError(
                f"max_history must be > 0, got {self.max_history!r}"
            )
        if self.low_carbon_kg >= self.high_carbon_kg:
            raise InterpretationError(
                f"low_carbon_kg ({self.low_carbon_kg!r}) must be < "
                f"high_carbon_kg ({self.high_carbon_kg!r})"
            )


# =============================================================================
# WorkloadProfile
# =============================================================================

@dataclass
class WorkloadProfile:
    """Enhanced workload profile with helium awareness."""
    task_id: str
    complexity_score: float
    energy_estimate_kwh: float
    carbon_estimate_kg: float
    resource_requirements: Dict[str, Any]
    deferrable: bool
    priority: int
    helium_profile: Optional[HeliumProfile] = None
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    source: str = "workload_interpreter"
    # Observability fields (P1 #11)
    base_energy_kwh: Optional[float] = None
    helium_penalty_applied: bool = False

    def __post_init__(self) -> None:
        # Task ID
        if not isinstance(self.task_id, str) or not self.task_id:
            raise InterpretationError(
                "task_id must be a non-empty string."
            )

        # Numeric validation — explicit isfinite catches NaN / Inf (P1 #6)
        if not isinstance(self.complexity_score, (int, float)) or not math.isfinite(
            float(self.complexity_score)
        ):
            raise InterpretationError(
                f"complexity_score must be finite, "
                f"got {self.complexity_score!r}"
            )
        if not 0.0 <= self.complexity_score <= 1.0:
            raise InterpretationError(
                f"complexity_score must be in [0, 1], "
                f"got {self.complexity_score!r}"
            )
        for name in ("energy_estimate_kwh", "carbon_estimate_kg"):
            val = getattr(self, name)
            if not isinstance(val, (int, float)) or not math.isfinite(float(val)):
                raise InterpretationError(
                    f"{name} must be finite, got {val!r}"
                )
            if val < 0:
                raise InterpretationError(
                    f"{name} must be >= 0, got {val!r}"
                )

        if self.base_energy_kwh is not None:
            if not math.isfinite(self.base_energy_kwh):
                raise InterpretationError(
                    f"base_energy_kwh must be finite, "
                    f"got {self.base_energy_kwh!r}"
                )
            if self.base_energy_kwh < 0:
                raise InterpretationError(
                    f"base_energy_kwh must be >= 0, "
                    f"got {self.base_energy_kwh!r}"
                )
        if self.helium_penalty_applied and self.base_energy_kwh is None:
            raise InterpretationError(
                "helium_penalty_applied=True requires base_energy_kwh to "
                "record the pre-penalty energy."
            )

        # Priority
        if not 1 <= self.priority <= 10:
            raise InterpretationError(
                f"priority must be in [1, 10], got {self.priority!r}"
            )

        # Resource requirements
        if not isinstance(self.resource_requirements, dict):
            raise InterpretationError(
                "resource_requirements must be a dict."
            )

        # Helium profile (if present, must be a HeliumProfile)
        if self.helium_profile is not None and not isinstance(
            self.helium_profile, HeliumProfile
        ):
            raise InterpretationError(
                f"helium_profile must be a HeliumProfile or None, "
                f"got {type(self.helium_profile).__name__}"
            )

        # UTC timestamp
        if self.timestamp.tzinfo is None:
            self.timestamp = self.timestamp.replace(tzinfo=timezone.utc)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            "task_id": self.task_id,
            "complexity_score": self.complexity_score,
            "energy_estimate_kwh": self.energy_estimate_kwh,
            "carbon_estimate_kg": self.carbon_estimate_kg,
            "resource_requirements": dict(self.resource_requirements),
            "deferrable": self.deferrable,
            "priority": self.priority,
            "helium_profile": (
                self.helium_profile.to_dict()
                if self.helium_profile else None
            ),
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "base_energy_kwh": self.base_energy_kwh,
            "helium_penalty_applied": self.helium_penalty_applied,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "WorkloadProfile":
        if not isinstance(data, Mapping):
            raise InterpretationError(
                f"from_dict expects a Mapping, got {type(data).__name__}"
            )
        for required in (
            "task_id", "complexity_score", "energy_estimate_kwh",
            "carbon_estimate_kg",
        ):
            if required not in data:
                raise InterpretationError(
                    f"from_dict: missing required key {required!r}"
                )

        # Fix P1 #7: distinguish "absent / null" from "present but empty"
        hp: Optional[HeliumProfile]
        if "helium_profile" in data and data["helium_profile"] is not None:
            hp = HeliumProfile.from_dict(data["helium_profile"])
        else:
            hp = None

        rr_raw = data.get("resource_requirements")
        rr = dict(rr_raw) if isinstance(rr_raw, Mapping) else {}

        return cls(
            task_id=str(data["task_id"]),
            complexity_score=_safe_float(
                data["complexity_score"], field="complexity_score",
            ),
            energy_estimate_kwh=_safe_float(
                data["energy_estimate_kwh"], field="energy_estimate_kwh",
            ),
            carbon_estimate_kg=_safe_float(
                data["carbon_estimate_kg"], field="carbon_estimate_kg",
            ),
            resource_requirements=rr,
            deferrable=bool(data.get("deferrable", True)),
            priority=_safe_int(
                data.get("priority"), field="priority", default=5,
                min_value=1,
            ),
            helium_profile=hp,
            timestamp=_parse_timestamp(data.get("timestamp")),
            source=str(data.get("source", "workload_interpreter")),
            base_energy_kwh=(
                _safe_float(data["base_energy_kwh"],
                            field="base_energy_kwh")
                if data.get("base_energy_kwh") is not None else None
            ),
            helium_penalty_applied=bool(
                data.get("helium_penalty_applied", False)
            ),
        )

    def to_json(self, **kwargs: Any) -> str:
        # Every field is primitive in `to_dict()`, so no `default=str`.
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "WorkloadProfile":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise InterpretationError(f"Invalid JSON: {exc}") from exc
        if not isinstance(data, Mapping):
            raise InterpretationError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}"
            )
        return cls.from_dict(data)

    # ------------------------------------------------------------------
    # Analysis helpers
    # ------------------------------------------------------------------

    def carbon_level(
        self, config: Optional[InterpretationConfig] = None,
    ) -> CarbonLevel:
        """Bucket the profile's carbon estimate into LOW / MEDIUM / HIGH."""
        cfg = config or InterpretationConfig()
        if self.carbon_estimate_kg >= cfg.high_carbon_kg:
            return CarbonLevel.HIGH
        if self.carbon_estimate_kg >= cfg.low_carbon_kg:
            return CarbonLevel.MEDIUM
        return CarbonLevel.LOW

    def helium_level(self) -> Optional[HeliumDependencyLevel]:
        """Bucket the helium dependency score, or `None` if no analysis ran."""
        hp = self.helium_profile
        if hp is None:
            return None
        # Prefer the enhanced `HeliumProfile.level()` if available.
        level_fn = getattr(hp, "level", None)
        if callable(level_fn):
            try:
                return level_fn()  # type: ignore[no-any-return]
            except TypeError:
                pass
        # Fallback bucketing (matches HeliumProfileConfig defaults).
        score = hp.dependency_score
        if score >= 0.90:
            return HeliumDependencyLevel.CRITICAL
        if score >= 0.70:
            return HeliumDependencyLevel.HIGH
        if score >= 0.40:
            return HeliumDependencyLevel.MODERATE
        if score >= 0.10:
            return HeliumDependencyLevel.LOW
        return HeliumDependencyLevel.NEGLIGIBLE

    def key(self) -> Tuple[Any, ...]:
        """Deterministic identity tuple (excludes timestamp)."""
        helium_key: Optional[Tuple[Any, ...]] = None
        if self.helium_profile is not None:
            k_fn = getattr(self.helium_profile, "key", None)
            if callable(k_fn):
                try:
                    helium_key = k_fn()  # type: ignore[no-any-return]
                except Exception:  # noqa: BLE001 — defensive
                    helium_key = None
            if helium_key is None:
                helium_key = (
                    self.helium_profile.dependency_score,
                    self.helium_profile.hardware_type.value,
                    self.helium_profile.gpu_count,
                )
        return (
            self.task_id,
            self.complexity_score,
            self.energy_estimate_kwh,
            self.carbon_estimate_kg,
            self.deferrable,
            self.priority,
            helium_key,
            self.base_energy_kwh,
            self.helium_penalty_applied,
        )

    def compare(self, other: "WorkloadProfile") -> Dict[str, Any]:
        """Return a structured comparison between two profiles."""
        return {
            "task_id_self": self.task_id,
            "task_id_other": other.task_id,
            "complexity_delta": other.complexity_score - self.complexity_score,
            "energy_delta_kwh": (
                other.energy_estimate_kwh - self.energy_estimate_kwh
            ),
            "carbon_delta_kg": (
                other.carbon_estimate_kg - self.carbon_estimate_kg
            ),
            "priority_delta": other.priority - self.priority,
            "self_carbon_level": self.carbon_level().value,
            "other_carbon_level": other.carbon_level().value,
            "self_helium_level": (
                self.helium_level().value
                if self.helium_level() else None
            ),
            "other_helium_level": (
                other.helium_level().value
                if other.helium_level() else None
            ),
        }

    def __repr__(self) -> str:
        helium = (
            f"{self.helium_profile.dependency_score:.3f}"
            if self.helium_profile else "n/a"
        )
        return (
            "WorkloadProfile("
            f"task_id={self.task_id!r}, "
            f"complexity={self.complexity_score:.3f}, "
            f"energy={self.energy_estimate_kwh:.4f}kWh, "
            f"carbon={self.carbon_estimate_kg:.4f}kg, "
            f"priority={self.priority}, "
            f"helium={helium})"
        )


# =============================================================================
# WorkloadInterpreter
# =============================================================================

class WorkloadInterpreter:
    """Enhanced workload interpreter with helium dependency scoring."""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        interpretation_config: Optional[InterpretationConfig] = None,
        helium_config: Optional[HeliumProfileConfig] = None,
        strict: bool = True,
        strict_config: bool = False,
    ) -> None:
        if interpretation_config is not None:
            self._config = interpretation_config
        else:
            cfg = dict(config or {})
            known = set(InterpretationConfig.__dataclass_fields__)
            unknown = set(cfg) - known
            if unknown:
                msg = (
                    f"Unknown InterpretationConfig keys "
                    f"(ignored): {sorted(unknown)}. "
                    f"Known keys: {sorted(known)}."
                )
                if strict_config:
                    raise InterpretationError(msg)
                logger.warning(msg)
            kwargs = {k: v for k, v in cfg.items() if k in known}
            self._config = InterpretationConfig(**kwargs)

        self._helium_config = helium_config or HeliumProfileConfig()
        self._strict = bool(strict)
        # Preserved for legacy callers that inspect `interp.config`.
        self.config: Dict[str, Any] = dict(config or {})

        self._lock = threading.RLock()
        self._history: Deque[WorkloadProfile] = deque(
            maxlen=self._config.max_history
        )

        logger.debug(
            "WorkloadInterpreter initialized "
            "(helium_analysis=%s, strict=%s)",
            self.enable_helium_analysis,
            self._strict,
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def config_typed(self) -> InterpretationConfig:
        return self._config

    @property
    def helium_config(self) -> HeliumProfileConfig:
        return self._helium_config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def history(self) -> List[WorkloadProfile]:
        with self._lock:
            return list(self._history)

    # Fix P1 #8: single source of truth. Reading returns `_config`'s value;
    # assigning rebuilds `_config` via `dataclasses.replace` so it stays in
    # sync (previously it was a duplicated instance attribute).
    @property
    def enable_helium_analysis(self) -> bool:
        return self._config.enable_helium_analysis

    @enable_helium_analysis.setter
    def enable_helium_analysis(self, value: bool) -> None:
        self._config = replace(self._config, enable_helium_analysis=bool(value))

    # ------------------------------------------------------------------
    # Analysis
    # ------------------------------------------------------------------

    def analyze_task(
        self,
        task_json: Mapping[str, Any],
        *,
        record: bool = True,
    ) -> WorkloadProfile:
        """Analyze incoming task and create workload profile with helium metrics."""
        if not isinstance(task_json, Mapping):
            msg = (
                f"task_json must be a Mapping, "
                f"got {type(task_json).__name__}."
            )
            if self._strict:
                raise InterpretationError(msg)
            logger.warning("%s Using empty mapping.", msg)
            task_json = {}

        complexity_score = self._calculate_complexity(task_json)
        energy_estimate = self._estimate_energy(task_json)
        carbon_estimate = self._estimate_carbon(energy_estimate)
        resource_reqs = self._extract_resource_requirements(task_json)
        priority = _safe_int(
            task_json.get("priority"),
            field="priority",
            default=5,
            min_value=1,
            max_value=10,
        )
        deferrable = bool(task_json.get("deferrable", True))

        profile = WorkloadProfile(
            task_id=str(task_json.get("task_id", "unknown")),
            complexity_score=complexity_score,
            energy_estimate_kwh=energy_estimate,
            carbon_estimate_kg=carbon_estimate,
            resource_requirements=resource_reqs,
            deferrable=deferrable,
            priority=priority,
        )

        if self.enable_helium_analysis:
            try:
                profile.helium_profile = HeliumProfile.from_task_config(
                    task_json, config=self._helium_config,
                )
            except HeliumProfileError as exc:
                logger.warning("Helium analysis failed: %s", exc)
                if self._strict:
                    raise InterpretationError(
                        f"Helium analysis failed: {exc}"
                    ) from exc
            else:
                # Fix P0 #1: recompute carbon after the penalty.
                if (
                    profile.helium_profile is not None
                    and profile.helium_profile.dependency_score
                    > self._config.high_dependency_cutoff
                ):
                    pre_penalty = profile.energy_estimate_kwh
                    profile.base_energy_kwh = pre_penalty
                    profile.energy_estimate_kwh = (
                        pre_penalty * self._config.high_dependency_energy_penalty
                    )
                    profile.carbon_estimate_kg = self._estimate_carbon(
                        profile.energy_estimate_kwh
                    )
                    profile.helium_penalty_applied = True

        if record:
            with self._lock:
                self._history.append(profile)

        logger.debug(
            "Analyzed task %s: complexity=%.3f energy=%.4f carbon=%.4f "
            "penalty=%s",
            profile.task_id,
            profile.complexity_score,
            profile.energy_estimate_kwh,
            profile.carbon_estimate_kg,
            profile.helium_penalty_applied,
        )
        return profile

    def analyze_batch(
        self,
        tasks: Iterable[Mapping[str, Any]],
        *,
        record: bool = True,
    ) -> List[WorkloadProfile]:
        """Interpret a batch of task configs in one pass."""
        return [self.analyze_task(t, record=record) for t in tasks]

    # ------------------------------------------------------------------
    # Private helpers (config-driven, coercion hardened)
    # ------------------------------------------------------------------

    def _calculate_complexity(self, task_json: Mapping) -> float:
        model_config = _coerce_mapping(
            task_json.get("model_config"), field="model_config",
        )
        model_size = _safe_float(
            model_config.get("size_gb"), field="model_config.size_gb",
        )
        data_volume = _safe_float(
            task_json.get("data_volume_gb"), field="data_volume_gb",
        )
        return min(
            1.0,
            (model_size / self._config.model_size_scale_gb)
            + (data_volume / self._config.data_volume_scale_gb),
        )

    def _estimate_energy(self, task_json: Mapping) -> float:
        hardware_req = _coerce_mapping(
            task_json.get("hardware_requirements"),
            field="hardware_requirements",
        )
        gpu_count = _safe_int(
            hardware_req.get("gpu_count"),
            field="hardware_requirements.gpu_count",
            min_value=0,
        )
        return (
            self._config.base_energy_kwh
            + gpu_count * self._config.energy_per_gpu_kwh
        )

    def _estimate_carbon(self, energy_kwh: float) -> float:
        return energy_kwh * self._config.carbon_intensity_kg_per_kwh

    def _extract_resource_requirements(
        self, task_json: Mapping
    ) -> Dict[str, Any]:
        hw = _coerce_mapping(
            task_json.get("hardware_requirements"),
            field="hardware_requirements",
        )
        cpu_cores = _safe_int(
            hw.get("cpu_cores"),
            field="hardware_requirements.cpu_cores",
            default=2,
            min_value=0,
        )
        memory_gb = _safe_float(
            hw.get("memory_gb"),
            field="hardware_requirements.memory_gb",
            default=8.0,
            min_value=0.0,
        )
        gpu_count = _safe_int(
            hw.get("gpu_count"),
            field="hardware_requirements.gpu_count",
            default=0,
            min_value=0,
        )
        return {
            "cpu_cores": cpu_cores,
            "memory_gb": memory_gb,
            "gpu_count": gpu_count,
        }

    # ------------------------------------------------------------------
    # Statistics / maintenance
    # ------------------------------------------------------------------

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            history = list(self._history)
        n = len(history)
        if n == 0:
            return {
                "interpreted": 0,
                "mean_complexity": None,
                "mean_energy_kwh": None,
                "mean_carbon_kg": None,
                "min_carbon_kg": None,
                "max_carbon_kg": None,
                "helium_analyzed": 0,
                "helium_mean_score": None,
            }
        energies = [p.energy_estimate_kwh for p in history]
        carbons = [p.carbon_estimate_kg for p in history]
        helium_scores = [
            p.helium_profile.dependency_score
            for p in history if p.helium_profile is not None
        ]
        return {
            "interpreted": n,
            "mean_complexity": (
                sum(p.complexity_score for p in history) / n
            ),
            "mean_energy_kwh": sum(energies) / n,
            "mean_carbon_kg": sum(carbons) / n,
            "min_carbon_kg": min(carbons),
            "max_carbon_kg": max(carbons),
            "helium_analyzed": len(helium_scores),
            "helium_mean_score": (
                sum(helium_scores) / len(helium_scores)
                if helium_scores else None
            ),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """
        Reset the interpreter.

        Note:
            Statistics are derived from `_history`, not counters. This method
            does not (and cannot) "reset" statistics independently — clearing
            the history clears everything.
        """
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug(
            "WorkloadInterpreter reset (clear_history=%s)", clear_history
        )

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "schema_version": __version__,
                "config": asdict(self._config),
                "helium_config": asdict(self._helium_config),
                "strict": self._strict,
                "enable_helium_analysis": self.enable_helium_analysis,
                "history": [p.to_dict() for p in self._history],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "WorkloadInterpreter":
        if not isinstance(data, Mapping):
            raise InterpretationError(
                f"from_dict expects a Mapping, got {type(data).__name__}"
            )

        # --- InterpretationConfig (all known fields) ---
        cfg_data = dict(data.get("config", {}) or {})
        known = set(InterpretationConfig.__dataclass_fields__)
        cfg_kwargs = {k: v for k, v in cfg_data.items() if k in known}
        interp_cfg = InterpretationConfig(**cfg_kwargs)

        # --- HeliumProfileConfig (fix P0 #2: read back) ---
        hc_data = dict(data.get("helium_config", {}) or {})
        hc_known = set(HeliumProfileConfig.__dataclass_fields__)
        hc_kwargs = {k: v for k, v in hc_data.items() if k in hc_known}
        helium_cfg = (
            HeliumProfileConfig(**hc_kwargs) if hc_kwargs
            else HeliumProfileConfig()
        )

        interp = cls(
            interpretation_config=interp_cfg,
            helium_config=helium_cfg,
            strict=bool(data.get("strict", True)),
        )

        # --- History (fix P1 #10: warn on truncation) ---
        entries = list(data.get("history", []) or [])
        if len(entries) > interp._config.max_history:
            logger.warning(
                "Restoring %d history entries into a maxlen=%d deque; "
                "the oldest %d will be dropped.",
                len(entries),
                interp._config.max_history,
                len(entries) - interp._config.max_history,
            )
        with interp._lock:
            for entry in entries:
                interp._history.append(WorkloadProfile.from_dict(entry))
        return interp

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "WorkloadInterpreter":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise InterpretationError(f"Invalid JSON: {exc}") from exc
        if not isinstance(data, Mapping):
            raise InterpretationError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}"
            )
        return cls.from_dict(data)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "WorkloadInterpreter("
                f"helium={self.enable_helium_analysis}, "
                f"history={len(self._history)}, "
                f"strict={self._strict})"
            )


# =============================================================================
# Smoke test
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    interp = WorkloadInterpreter()
    print("repr       :", interp)

    task = {
        "task_id": "t-1",
        "hardware_requirements": {
            "gpu_count": 8, "cpu_cores": 16, "memory_gb": 64,
        },
        "model_config": {"size_gb": 120, "cpu_compatible": False},
        "estimated_training_hours": 200,
        "priority": 3,
        "data_volume_gb": 500,
    }
    profile = interp.analyze_task(task)
    print("profile    :", profile)
    print("helium     :", profile.helium_profile)
    print("carbon lvl :", profile.carbon_level().value)
    print("helium lvl :", (
        profile.helium_level().value
        if profile.helium_level() else None
    ))
    print("stats      :", interp.statistics())

    # --- P0 #1 regression: carbon tracks energy after penalty ---
    assert profile.helium_penalty_applied is True
    assert profile.base_energy_kwh is not None
    expected_carbon = (
        profile.energy_estimate_kwh
        * interp.config_typed.carbon_intensity_kg_per_kwh
    )
    assert math.isclose(
        profile.carbon_estimate_kg, expected_carbon, rel_tol=1e-9,
    ), (
        f"Carbon {profile.carbon_estimate_kg} does not track energy "
        f"{profile.energy_estimate_kwh}: expected {expected_carbon}"
    )
    print(f"Penalty applied: base={profile.base_energy_kwh:.4f}, "
          f"penalized={profile.energy_estimate_kwh:.4f}, "
          f"carbon={profile.carbon_estimate_kg:.4f}")

    # --- P0 #2 regression: helium_config round-trips ---
    from .helium_profile import HeliumProfileConfig as _HC
    interp2 = WorkloadInterpreter(
        helium_config=_HC(large_model_gb=10.0, gpu_cluster_threshold=2),
    )
    restored = WorkloadInterpreter.from_json(interp2.to_json())
    assert restored.helium_config == interp2.helium_config
    print("helium_config round-trip OK")

    # --- WorkloadProfile round-trip ---
    restored_profile = WorkloadProfile.from_json(profile.to_json())
    assert restored_profile.key() == profile.key()
    print("WorkloadProfile round-trip OK")

    # --- P0 #3 regression: unknown config keys are not silently dropped ---
    try:
        WorkloadInterpreter(
            config={"totally_made_up": 42}, strict_config=True,
        )
    except InterpretationError as exc:
        print(f"Unknown config key rejected: {exc}")
    else:
        raise AssertionError("Unknown config key was silently accepted!")

    # --- P0 #4 regression: input errors raise InterpretationError ---
    for bad in (
        {"task_id": "bad", "hardware_requirements": {"gpu_count": "five"}},
        {"task_id": "bad", "model_config": "not-a-dict"},
        {"task_id": "bad", "data_volume_gb": float("nan")},
    ):
        try:
            interp.analyze_task(bad)
        except InterpretationError as exc:
            print(f"Rejected {list(bad)}: {exc}")
        else:
            raise AssertionError(f"{bad!r} was accepted!")

    # --- P0 #5 regression: cpu_cores=0 is respected, not masked ---
    p_zero = interp.analyze_task({
        "task_id": "zero-cores",
        "hardware_requirements": {"cpu_cores": 0},
    }, record=False)
    assert p_zero.resource_requirements["cpu_cores"] == 0, (
        f"cpu_cores=0 was silently masked to "
        f"{p_zero.resource_requirements['cpu_cores']}"
    )
    print("cpu_cores=0 respected")

    # --- P1 #6 regression: NaN / Inf rejected at WorkloadProfile level ---
    try:
        WorkloadProfile(
            task_id="nan-test",
            complexity_score=0.5,
            energy_estimate_kwh=float("nan"),
            carbon_estimate_kg=1.0,
            resource_requirements={},
            deferrable=True,
            priority=5,
        )
    except InterpretationError as exc:
        print(f"NaN energy rejected: {exc}")
    else:
        raise AssertionError("NaN energy was accepted!")

    # --- P1 #7 regression: empty helium_profile dict raises ---
    try:
        WorkloadProfile.from_dict({
            "task_id": "x",
            "complexity_score": 0.5,
            "energy_estimate_kwh": 1.0,
            "carbon_estimate_kg": 0.4,
            "helium_profile": {},
        })
    except (InterpretationError, HeliumProfileError) as exc:
        print(f"Empty helium_profile rejected: {exc}")
    else:
        raise AssertionError("Empty helium_profile was accepted!")

    # --- P1 #8 regression: enable_helium_analysis stays in sync ---
    interp_sync = WorkloadInterpreter()
    assert interp_sync.enable_helium_analysis is True
    interp_sync.enable_helium_analysis = False
    assert interp_sync.config_typed.enable_helium_analysis is False
    assert interp_sync.to_dict()["enable_helium_analysis"] is False
    assert interp_sync.to_dict()["config"]["enable_helium_analysis"] is False
    print("enable_helium_analysis sync OK")

    # --- Original interpreter round-trip ---
    payload = interp.to_json()
    restored_interp = WorkloadInterpreter.from_json(payload)
    assert restored_interp.to_dict() == interp.to_dict()
    print("Interpreter round-trip OK.")

    print("\nAll smoke checks passed.")
