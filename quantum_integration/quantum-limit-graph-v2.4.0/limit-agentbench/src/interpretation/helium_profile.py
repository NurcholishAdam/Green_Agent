# src/interpretation/helium_profile.py

"""
Helium Profile (v5.1)
=====================

Helium-dependency modelling for workloads. Classifies hardware into helium
footprints and derives a :class:`HeliumProfile` from a task config.

Enhancements in v5.1 (see review)
---------------------------------
  * P0 — `math.isfinite` checks reject NaN/Inf on every numeric field
         (previously four float fields accepted NaN and Inf silently).
  * P0 — `from_dict` accepts `dataclasses.asdict()` output via
         `HardwareType.from_any` (previously raised `ValueError` on the
         `"HardwareType.GPU_CLUSTER"` repr that `asdict()` produces).
  * P0 — All numeric coercion goes through `_safe_int` / `_safe_float`,
         which raise `HeliumProfileError` instead of leaking
         `ValueError` / `TypeError` / `AttributeError` / `KeyError`.
  * P0 — Stale "LRU cache on from_task_config" claim removed from the
         docstring; a `key()` method provides an explicit deduplication
         primitive instead.
  * P1 — `EDGE` is now reachable when `gpu_count > 0` (precedence fixed:
         `edge_deployment` wins over GPU count).
  * P1 — `HeliumDependencyLevel` is actually used via a `level()` method
         driven by configurable thresholds on `HeliumProfileConfig`.
  * P1 — `estimated_helium_impact` / `scarcity_tolerance` are validated
         for internal consistency (derived from `dependency_score`).
  * P1 — ISO-8601 timestamps accept `Z` suffix on every Python version.
  * P1 — Nested mappings validated; non-Mapping inputs rejected with a
         clear `HeliumProfileError`.
  * P1 — `gpu_cluster_threshold` boundary documented (`>`, exclusive).
  * P2 — `schema_version` embedded in `to_dict`; `default=str` removed
         from `json.dumps` (round-trip is now lossless or raises).
  * P2 — New helpers: `level()`, `key()`, `compare()`,
         `recommend_alternatives()`, `HeliumProfile.create()`.
  * P3 — Dead `lru_cache` import removed; `HeliumProfile` uses
         `slots=True`; footprint table cached at module level.

Public API (backward compatible)
--------------------------------
    HeliumProfileError
    HeliumDependencyLevel, HardwareType
    HeliumProfileConfig
    HeliumProfile
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

__all__ = [
    "HardwareType",
    "HeliumDependencyLevel",
    "HeliumProfile",
    "HeliumProfileConfig",
    "HeliumProfileError",
    "__version__",
]


# =============================================================================
# Errors
# =============================================================================

class HeliumProfileError(ValueError):
    """Raised for invalid helium profile inputs."""


# =============================================================================
# Enums
# =============================================================================

class HeliumDependencyLevel(Enum):
    """Helium dependency classification for workloads (derived from score)."""
    CRITICAL = "critical"       # Large GPU/TPU training clusters, Quantum
    HIGH = "high"               # Single GPU training, inference servers
    MODERATE = "moderate"       # CPU-only but data-intensive
    LOW = "low"                 # Basic CPU inference
    NEGLIGIBLE = "negligible"   # Edge devices, quantized models


class HardwareType(Enum):
    """Hardware types with their helium footprints."""
    GPU_CLUSTER = "gpu_cluster"
    SINGLE_GPU = "single_gpu"
    TPU = "tpu"
    CPU_ONLY = "cpu"
    QUANTUM = "quantum"
    EDGE = "edge"

    @property
    def helium_footprint(self) -> float:
        """Helium dependency score (0.0 to 1.0)."""
        return _HELIUM_FOOTPRINTS[self]

    @classmethod
    def from_any(cls, value: Any) -> "HardwareType":
        """
        Coerce `value` (enum, string value, string name, or asdict repr)
        into a `HardwareType`. Accepts `"gpu_cluster"`, `"GPU_CLUSTER"`,
        and `"HardwareType.GPU_CLUSTER"` forms.
        """
        if isinstance(value, cls):
            return value
        s = str(value).strip()
        # Strip "HardwareType." prefix emitted by repr() / asdict()
        if "." in s:
            s = s.rsplit(".", 1)[-1]
        # 1) Match by value ("gpu_cluster")
        try:
            return cls(s)
        except ValueError:
            pass
        # 2) Match by name ("GPU_CLUSTER")
        try:
            return cls[s.upper()]
        except KeyError:
            pass
        raise HeliumProfileError(
            f"Unknown HardwareType {value!r}. Expected one of "
            f"{[m.value for m in cls]} (values) or "
            f"{[m.name for m in cls]} (names)."
        )


# Module-level footprint table avoids rebuilding the dict on every access.
_HELIUM_FOOTPRINTS: Dict[HardwareType, float] = {
    HardwareType.GPU_CLUSTER: 0.95,
    HardwareType.QUANTUM: 0.99,
    HardwareType.TPU: 0.85,
    HardwareType.SINGLE_GPU: 0.75,
    HardwareType.CPU_ONLY: 0.10,
    HardwareType.EDGE: 0.05,
}


# =============================================================================
# Safe coercion helpers
# =============================================================================

def _safe_int(value: Any, *, field: str, default: int = 0) -> int:
    """Coerce `value` to int, raising `HeliumProfileError` on any problem."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        # bool is technically an int, but a bool gpu_count is almost always
        # a caller bug worth surfacing.
        raise HeliumProfileError(
            f"{field} must be an integer, got bool {value!r}"
        )
    if isinstance(value, float):
        if not math.isfinite(value):
            raise HeliumProfileError(
                f"{field} must be finite, got {value!r}"
            )
        if not value.is_integer():
            raise HeliumProfileError(
                f"{field} must be an integer, got {value!r}"
            )
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise HeliumProfileError(
            f"{field} must be an integer, got {value!r} "
            f"({type(value).__name__})"
        ) from exc


def _safe_float(value: Any, *, field: str, default: float = 0.0) -> float:
    """Coerce `value` to a finite float, raising `HeliumProfileError` otherwise."""
    if value is None or value == "":
        return default
    try:
        fv = float(value)
    except (TypeError, ValueError) as exc:
        raise HeliumProfileError(
            f"{field} must be a number, got {value!r} "
            f"({type(value).__name__})"
        ) from exc
    if not math.isfinite(fv):
        raise HeliumProfileError(
            f"{field} must be finite, got {value!r}"
        )
    return fv


def _coerce_mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    """Return `value` if it's a Mapping, `{}` if None, else raise."""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise HeliumProfileError(
            f"{field} must be a Mapping (or None), got "
            f"{type(value).__name__}"
        )
    return value


def _parse_timestamp(value: Any) -> datetime:
    """Parse a timestamp from `datetime`, ISO-8601 str (with or without Z)."""
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            ts = datetime.fromisoformat(s)
        except ValueError as exc:
            raise HeliumProfileError(
                f"timestamp is not ISO-8601: {value!r}"
            ) from exc
    elif value is None:
        return datetime.now(timezone.utc)
    else:
        raise HeliumProfileError(
            f"timestamp must be datetime or ISO string, got "
            f"{type(value).__name__}"
        )
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class HeliumProfileConfig:
    """
    Tunable parameters for helium profiling.

    Notes:
        ``gpu_cluster_threshold`` is *exclusive*: a workload with exactly
        that many GPUs classifies as ``SINGLE_GPU``; one more classifies
        as ``GPU_CLUSTER``.
    """
    large_model_gb: float = 50.0
    large_model_adjustment: float = 1.3
    long_training_hours: float = 100.0
    long_training_adjustment: float = 1.2
    gpu_cluster_threshold: int = 4
    # Level thresholds — a profile's `level()` is the highest bucket whose
    # threshold is <= dependency_score.
    critical_threshold: float = 0.90
    high_threshold: float = 0.70
    moderate_threshold: float = 0.40
    low_threshold: float = 0.10

    def __post_init__(self) -> None:
        for name in ("large_model_gb", "long_training_hours"):
            val = getattr(self, name)
            if not math.isfinite(val) or val <= 0:
                raise HeliumProfileError(
                    f"{name} must be finite and > 0, got {val!r}"
                )
        for name in (
            "large_model_adjustment", "long_training_adjustment",
        ):
            val = getattr(self, name)
            if not math.isfinite(val) or val < 1.0:
                raise HeliumProfileError(
                    f"{name} must be finite and >= 1.0, got {val!r}"
                )
        if self.gpu_cluster_threshold < 1:
            raise HeliumProfileError(
                f"gpu_cluster_threshold must be >= 1, "
                f"got {self.gpu_cluster_threshold!r}"
            )
        for name in (
            "critical_threshold", "high_threshold",
            "moderate_threshold", "low_threshold",
        ):
            val = getattr(self, name)
            if not math.isfinite(val) or not 0.0 <= val <= 1.0:
                raise HeliumProfileError(
                    f"{name} must be finite and in [0, 1], got {val!r}"
                )
        # Enforce monotonic thresholds so `level()` is unambiguous.
        for a, b in (
            ("low_threshold", "moderate_threshold"),
            ("moderate_threshold", "high_threshold"),
            ("high_threshold", "critical_threshold"),
        ):
            if getattr(self, a) >= getattr(self, b):
                raise HeliumProfileError(
                    f"{a} must be < {b} "
                    f"({getattr(self, a)!r} vs {getattr(self, b)!r})"
                )


_DEFAULT_CONFIG = HeliumProfileConfig()


# =============================================================================
# Profile
# =============================================================================

@dataclass(slots=True)
class HeliumProfile:
    """
    Helium dependency profile for a workload.

    Fields:
        dependency_score:        0.0–1.0, base footprint × adjustments.
        hardware_type:           Classified hardware category.
        estimated_helium_impact: `dependency_score * 100` — validated for
                                 consistency in `__post_init__`.
        scarcity_tolerance:      `1 - dependency_score` — validated likewise.
        ...
    """
    dependency_score: float
    hardware_type: HardwareType
    estimated_helium_impact: float
    scarcity_tolerance: float
    gpu_count: int = 0
    memory_bandwidth_gbs: float = 0.0
    estimated_training_hours: float = 0.0
    model_size_gb: float = 0.0
    can_use_distilled_model: bool = False
    can_run_on_cpu: bool = False
    has_quantized_version: bool = False
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:
        # Range checks
        if not 0.0 <= self.dependency_score <= 1.0:
            raise HeliumProfileError(
                f"dependency_score must be in [0, 1], "
                f"got {self.dependency_score!r}"
            )
        if not 0.0 <= self.scarcity_tolerance <= 1.0:
            raise HeliumProfileError(
                f"scarcity_tolerance must be in [0, 1], "
                f"got {self.scarcity_tolerance!r}"
            )
        if self.gpu_count < 0:
            raise HeliumProfileError(
                f"gpu_count must be >= 0, got {self.gpu_count!r}"
            )

        # Finiteness + non-negativity — this is the fix for P0 #1: NaN and
        # Inf are rejected by `math.isfinite`.
        for name in (
            "estimated_helium_impact", "memory_bandwidth_gbs",
            "estimated_training_hours", "model_size_gb",
        ):
            val = getattr(self, name)
            if not math.isfinite(val):
                raise HeliumProfileError(
                    f"{name} must be finite, got {val!r}"
                )
            if val < 0:
                raise HeliumProfileError(
                    f"{name} must be >= 0, got {val!r}"
                )

        # Derived-field consistency (P1)
        expected_impact = self.dependency_score * 100.0
        if not math.isclose(
            self.estimated_helium_impact, expected_impact,
            rel_tol=1e-9, abs_tol=1e-9,
        ):
            raise HeliumProfileError(
                f"estimated_helium_impact={self.estimated_helium_impact!r} "
                f"is inconsistent with dependency_score="
                f"{self.dependency_score!r} (expected {expected_impact!r})"
            )
        expected_tol = 1.0 - self.dependency_score
        if not math.isclose(
            self.scarcity_tolerance, expected_tol,
            rel_tol=1e-9, abs_tol=1e-9,
        ):
            raise HeliumProfileError(
                f"scarcity_tolerance={self.scarcity_tolerance!r} "
                f"is inconsistent with dependency_score="
                f"{self.dependency_score!r} (expected {expected_tol!r})"
            )

        # Timestamp timezone (in-place normalisation; slots + mutable class)
        if self.timestamp.tzinfo is None:
            self.timestamp = self.timestamp.replace(tzinfo=timezone.utc)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def create(
        cls,
        *,
        dependency_score: float,
        hardware_type: HardwareType,
        gpu_count: int = 0,
        memory_bandwidth_gbs: float = 0.0,
        estimated_training_hours: float = 0.0,
        model_size_gb: float = 0.0,
        can_use_distilled_model: bool = False,
        can_run_on_cpu: bool = False,
        has_quantized_version: bool = False,
        timestamp: Optional[datetime] = None,
    ) -> "HeliumProfile":
        """
        Convenience constructor that derives `estimated_helium_impact` and
        `scarcity_tolerance` from `dependency_score` so callers can't create
        an inconsistent profile by hand.
        """
        return cls(
            dependency_score=dependency_score,
            hardware_type=hardware_type,
            estimated_helium_impact=dependency_score * 100.0,
            scarcity_tolerance=1.0 - dependency_score,
            gpu_count=gpu_count,
            memory_bandwidth_gbs=memory_bandwidth_gbs,
            estimated_training_hours=estimated_training_hours,
            model_size_gb=model_size_gb,
            can_use_distilled_model=can_use_distilled_model,
            can_run_on_cpu=can_run_on_cpu,
            has_quantized_version=has_quantized_version,
            timestamp=timestamp or datetime.now(timezone.utc),
        )

    @classmethod
    def from_task_config(
        cls,
        task_config: Mapping[str, Any],
        *,
        config: Optional[HeliumProfileConfig] = None,
    ) -> "HeliumProfile":
        """
        Create a HeliumProfile from a task configuration.

        Raises:
            HeliumProfileError: on malformed config or bad numeric fields.
        """
        if not isinstance(task_config, Mapping):
            raise HeliumProfileError(
                f"task_config must be a Mapping, got "
                f"{type(task_config).__name__}"
            )
        cfg = config or _DEFAULT_CONFIG

        hardware_req = _coerce_mapping(
            task_config.get("hardware_requirements"),
            field="hardware_requirements",
        )
        model_config = _coerce_mapping(
            task_config.get("model_config"),
            field="model_config",
        )

        gpu_count = _safe_int(
            hardware_req.get("gpu_count"), field="hardware_requirements.gpu_count",
        )
        tpu_accelerator = bool(hardware_req.get("tpu_accelerator", False))
        quantum_circuit = bool(task_config.get("quantum_circuit", False))
        edge_deployment = bool(task_config.get("edge_deployment", False))

        # Precedence (fix P1 #6): `edge_deployment` is checked BEFORE
        # `gpu_count` so an edge device with a GPU is classified as EDGE
        # (footprint 0.05) rather than SINGLE_GPU (0.75).
        if quantum_circuit:
            hardware_type = HardwareType.QUANTUM
        elif tpu_accelerator:
            hardware_type = HardwareType.TPU
        elif edge_deployment:
            hardware_type = HardwareType.EDGE
        elif gpu_count > cfg.gpu_cluster_threshold:
            hardware_type = HardwareType.GPU_CLUSTER
        elif gpu_count > 0:
            hardware_type = HardwareType.SINGLE_GPU
        else:
            hardware_type = HardwareType.CPU_ONLY

        base_score = hardware_type.helium_footprint
        model_size_gb = _safe_float(
            model_config.get("size_gb"), field="model_config.size_gb",
        )
        training_hours = _safe_float(
            task_config.get("estimated_training_hours"),
            field="estimated_training_hours",
        )
        memory_bandwidth_gbs = _safe_float(
            hardware_req.get("memory_bandwidth_gbs"),
            field="hardware_requirements.memory_bandwidth_gbs",
        )

        adjustment = 1.0
        if model_size_gb > cfg.large_model_gb:
            adjustment *= cfg.large_model_adjustment
        if training_hours > cfg.long_training_hours:
            adjustment *= cfg.long_training_adjustment

        dependency_score = min(1.0, base_score * adjustment)

        return cls.create(
            dependency_score=dependency_score,
            hardware_type=hardware_type,
            gpu_count=gpu_count,
            memory_bandwidth_gbs=memory_bandwidth_gbs,
            estimated_training_hours=training_hours,
            model_size_gb=model_size_gb,
            can_use_distilled_model=bool(
                model_config.get("distilled_available", False)
            ),
            can_run_on_cpu=bool(model_config.get("cpu_compatible", False)),
            has_quantized_version=bool(
                model_config.get("quantized_available", False)
            ),
        )

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            "dependency_score": self.dependency_score,
            "hardware_type": self.hardware_type.value,
            "estimated_helium_impact": self.estimated_helium_impact,
            "scarcity_tolerance": self.scarcity_tolerance,
            "gpu_count": self.gpu_count,
            "memory_bandwidth_gbs": self.memory_bandwidth_gbs,
            "estimated_training_hours": self.estimated_training_hours,
            "model_size_gb": self.model_size_gb,
            "can_use_distilled_model": self.can_use_distilled_model,
            "can_run_on_cpu": self.can_run_on_cpu,
            "has_quantized_version": self.has_quantized_version,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "HeliumProfile":
        if not isinstance(data, Mapping):
            raise HeliumProfileError(
                f"from_dict expects a Mapping, got {type(data).__name__}"
            )
        for required in ("dependency_score", "hardware_type"):
            if required not in data:
                raise HeliumProfileError(
                    f"from_dict: missing required key {required!r}"
                )

        dep = _safe_float(data["dependency_score"],
                          field="dependency_score")
        # Accept `asdict()` output where hardware_type is the enum itself
        # or its repr string, in addition to the canonical `.value` form.
        hardware_type = HardwareType.from_any(data["hardware_type"])

        # Derive impact/tolerance if absent, else validate consistency.
        impact = data.get("estimated_helium_impact")
        impact = (dep * 100.0 if impact is None
                  else _safe_float(impact, field="estimated_helium_impact"))
        tol = data.get("scarcity_tolerance")
        tol = (1.0 - dep if tol is None
               else _safe_float(tol, field="scarcity_tolerance"))

        return cls(
            dependency_score=dep,
            hardware_type=hardware_type,
            estimated_helium_impact=impact,
            scarcity_tolerance=tol,
            gpu_count=_safe_int(data.get("gpu_count"), field="gpu_count"),
            memory_bandwidth_gbs=_safe_float(
                data.get("memory_bandwidth_gbs"),
                field="memory_bandwidth_gbs",
            ),
            estimated_training_hours=_safe_float(
                data.get("estimated_training_hours"),
                field="estimated_training_hours",
            ),
            model_size_gb=_safe_float(
                data.get("model_size_gb"), field="model_size_gb",
            ),
            can_use_distilled_model=bool(
                data.get("can_use_distilled_model", False)
            ),
            can_run_on_cpu=bool(data.get("can_run_on_cpu", False)),
            has_quantized_version=bool(
                data.get("has_quantized_version", False)
            ),
            timestamp=_parse_timestamp(data.get("timestamp")),
        )

    def to_json(self, **kwargs: Any) -> str:
        # Note: `default=str` removed (P2). Every field is primitive in
        # `to_dict()`, so any non-serialisable value indicates a bug.
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "HeliumProfile":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise HeliumProfileError(f"Invalid JSON: {exc}") from exc
        if not isinstance(data, Mapping):
            raise HeliumProfileError(
                f"JSON payload must decode to an object, got "
                f"{type(data).__name__}"
            )
        return cls.from_dict(data)

    # ------------------------------------------------------------------
    # Analysis helpers
    # ------------------------------------------------------------------

    def level(
        self, config: Optional[HeliumProfileConfig] = None,
    ) -> HeliumDependencyLevel:
        """Bucketed dependency level (drives scheduling decisions)."""
        cfg = config or _DEFAULT_CONFIG
        s = self.dependency_score
        if s >= cfg.critical_threshold:
            return HeliumDependencyLevel.CRITICAL
        if s >= cfg.high_threshold:
            return HeliumDependencyLevel.HIGH
        if s >= cfg.moderate_threshold:
            return HeliumDependencyLevel.MODERATE
        if s >= cfg.low_threshold:
            return HeliumDependencyLevel.LOW
        return HeliumDependencyLevel.NEGLIGIBLE

    def key(self) -> Tuple[Any, ...]:
        """
        Deterministic identity tuple (excludes `timestamp`). Useful for
        deduplication and cache keys without relying on `__hash__` (which
        is intentionally absent — profiles are mutable).
        """
        return (
            self.dependency_score,
            self.hardware_type.value,
            self.estimated_helium_impact,
            self.scarcity_tolerance,
            self.gpu_count,
            self.memory_bandwidth_gbs,
            self.estimated_training_hours,
            self.model_size_gb,
            self.can_use_distilled_model,
            self.can_run_on_cpu,
            self.has_quantized_version,
        )

    def compare(self, other: "HeliumProfile") -> Dict[str, Any]:
        """Return a structured comparison between two profiles."""
        if self.dependency_score > 0:
            ratio: Optional[float] = (
                other.dependency_score / self.dependency_score
            )
        else:
            ratio = None
        return {
            "self_score": self.dependency_score,
            "other_score": other.dependency_score,
            "delta": other.dependency_score - self.dependency_score,
            "ratio": ratio,
            "self_hw": self.hardware_type.value,
            "other_hw": other.hardware_type.value,
            "same_hw": self.hardware_type is other.hardware_type,
            "other_is_cheaper": other.dependency_score < self.dependency_score,
        }

    def recommend_alternatives(
        self, config: Optional[HeliumProfileConfig] = None,
    ) -> List[str]:
        """Suggest concrete ways to reduce helium dependency for this workload."""
        cfg = config or _DEFAULT_CONFIG
        suggestions: List[str] = []

        level = self.level(cfg)
        if level in (
            HeliumDependencyLevel.CRITICAL, HeliumDependencyLevel.HIGH,
        ):
            if not self.can_use_distilled_model:
                suggestions.append(
                    "Enable distillation to reduce model size and compute."
                )
            if not self.has_quantized_version:
                suggestions.append(
                    "Produce a quantized (int8/int4) build to shrink footprint."
                )
            if (
                not self.can_run_on_cpu
                and level is HeliumDependencyLevel.HIGH
            ):
                suggestions.append(
                    "Investigate a CPU fallback for this workload."
                )

        if self.gpu_count > cfg.gpu_cluster_threshold:
            suggestions.append(
                f"Reduce GPU count from {self.gpu_count} to "
                f"<={cfg.gpu_cluster_threshold} to leave cluster tier."
            )
        if self.model_size_gb > cfg.large_model_gb:
            suggestions.append(
                f"Reduce model size from {self.model_size_gb:.1f} GB to "
                f"<={cfg.large_model_gb:.0f} GB."
            )
        if self.estimated_training_hours > cfg.long_training_hours:
            suggestions.append(
                f"Shorten training from "
                f"{self.estimated_training_hours:.0f} h to "
                f"<={cfg.long_training_hours:.0f} h."
            )
        return suggestions

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            "HeliumProfile("
            f"dep={self.dependency_score:.3f}, "
            f"hw={self.hardware_type.value}, "
            f"impact={self.estimated_helium_impact:.1f}, "
            f"scarcity_tol={self.scarcity_tolerance:.3f}, "
            f"gpus={self.gpu_count})"
        )


# =============================================================================
# Smoke test
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    cfg_task = {
        "hardware_requirements": {"gpu_count": 8, "tpu_accelerator": False},
        "model_config": {"size_gb": 120, "cpu_compatible": False,
                         "distilled_available": True},
        "estimated_training_hours": 200,
    }
    p = HeliumProfile.from_task_config(cfg_task)
    print("Profile   :", p)
    print("Level     :", p.level().value)
    print("Dict      :", p.to_dict())
    print("Suggestions:", p.recommend_alternatives())

    # Round-trip via JSON
    payload = p.to_json()
    assert HeliumProfile.from_json(payload).to_dict() == p.to_dict()

    # Round-trip via asdict() — regression for P0 #3
    from dataclasses import asdict as _asdict
    rehydrated = HeliumProfile.from_dict(_asdict(p))
    assert rehydrated.key() == p.key()
    print("Round-trip OK (JSON + asdict).")

    # Regression: nan / inf must be rejected
    for bad_field, bad_val in [
        ("estimated_helium_impact", float("nan")),
        ("model_size_gb", float("inf")),
    ]:
        kwargs = dict(
            dependency_score=0.5,
            hardware_type=HardwareType.SINGLE_GPU,
            estimated_helium_impact=50.0,
            scarcity_tolerance=0.5,
        )
        kwargs[bad_field] = bad_val
        try:
            HeliumProfile(**kwargs)
        except HeliumProfileError as exc:
            print(f"Rejected {bad_field}={bad_val!r}: {exc}")
        else:
            raise AssertionError(f"{bad_field}={bad_val!r} was accepted!")
    print("All smoke checks passed.")
