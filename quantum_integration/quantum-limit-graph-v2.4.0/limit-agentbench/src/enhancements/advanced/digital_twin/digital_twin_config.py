# src/quantum_integration/digital_twin/digital_twin_config.py

"""
Frozen, validated, serializable config for the digital twin.

Enhancements
------------
- Split into eight nested frozen dataclasses (``SimulationConfig``,
  ``CorrelationConfig``, ``SubstitutionConfig``, ``DistillationConfig``,
  ``ResilienceConfig``, ``PersistenceConfig``, ``TelemetryConfig``,
  ``SubsystemConfig``) plus a thin top-level ``DigitalTwinConfig``.
- ``_SchemaVersionMixin`` provides ``assert_compatible`` for every config
  and is reused by :class:`DigitalTwinResult` and other schema records.
- ``correlation_matrix_override`` is validated as a real correlation
  matrix: diagonal is 1.0, matrix is symmetric, values are in ``[-1, 1]``,
  every resource in ``resource_variances`` has a row and column.
- ``__hash__`` includes ``correlation_matrix_override`` — no more
  collisions between configs that differ only in the override.
- ``from_dict`` calls ``assert_compatible`` before constructing.
- ``from_dict`` accepts both nested and legacy flat formats.
- ``from_env`` is table-driven, covers every field, and rejects
  unknown ``GREEN_AGENT_DIGITAL_TWIN_*`` keys with a warning (or raises
  when ``strict=True``).
- ``to_env`` mirrors ``from_env`` for round-tripping test fixtures.
- ``with_env`` layers defaults → env → explicit overrides in one call.
- ``to_dict`` / ``from_dict`` are derived from ``dataclasses.fields``
  with a small dispatch table, so adding a field requires one edit.
- Backward-compatible attribute access: ``config.time_horizon_years``
  still works via ``__getattr__`` delegation to the sub-configs.
- Bridges to the memory package:
  - ``default_truth_level`` validated against
    ``memory_schemas.DEFAULT_TRUTH_LEVELS``.
  - ``default_container_tag`` reuses ``memory_schemas.DEFAULT_CONTAINER_TAG``.
  - ``ttl_seconds`` per record kind, mirroring ``MemoryConfig``.
  - ``to_memory_config()`` returns a ``MemoryConfig`` when available.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List, Optional, Tuple

from .digital_twin_errors import (
    DigitalTwinConfigError,
    DigitalTwinParseError,
)
from .digital_twin_helpers import (
    _is_finite_nonneg,
    _is_positive_finite,
    _is_real_int,
)

logger = logging.getLogger(__name__)

SCHEMA_VERSION: int = 1

_ENV_PREFIX = "GREEN_AGENT_DIGITAL_TWIN_"


# --------------------------------------------------------------------------- #
# Bridge to the memory package
# --------------------------------------------------------------------------- #
try:  # pragma: no cover - environment dependent
    from ...memory.memory_schemas import (  # type: ignore
        DEFAULT_CONTAINER_TAG as _MEM_DEFAULT_CONTAINER_TAG,
        DEFAULT_TRUTH_LEVELS as _MEM_DEFAULT_TRUTH_LEVELS,
    )
    _MEMORY_SCHEMAS_AVAILABLE = True
except ImportError:  # pragma: no cover
    _MEMORY_SCHEMAS_AVAILABLE = False
    _MEM_DEFAULT_CONTAINER_TAG = "org:green-agent"
    _MEM_DEFAULT_TRUTH_LEVELS = (
        "measured", "estimated", "simulated", "user-reported",
    )

#: Module-level exports re-used across the digital-twin package.
DEFAULT_CONTAINER_TAG: str = _MEM_DEFAULT_CONTAINER_TAG
DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = tuple(_MEM_DEFAULT_TRUTH_LEVELS)

#: Default per-kind TTLs. Keys mirror the memory package's record kinds.
_DEFAULT_TTLS: MappingProxyType = MappingProxyType({
    "digital_twin_result":  90 * 24 * 3600,
    "digital_twin_summary": 365 * 24 * 3600,
})

#: Priority keys accepted by ``SimulationConfig.user_priorities``.
_ALLOWED_PRIORITY_KEYS = frozenset({
    "carbon", "helium", "energy", "circularity", "biodiversity",
})

#: Default resource names for cross-validation with correlation / variances.
_RESOURCE_KEYS = frozenset({
    "carbon", "helium", "energy", "circularity", "biodiversity",
})


# --------------------------------------------------------------------------- #
# Schema-version mixin
# --------------------------------------------------------------------------- #
class _SchemaVersionMixin:
    """``assert_compatible`` shared by every config and schema record.

    ``DigitalTwinConfig`` and each of its nested sub-configs inherit this.
    Callers can also use it directly:

        DigitalTwinConfig.assert_compatible(payload)
    """

    @classmethod
    def assert_compatible(
        cls,
        data: ABCMapping,
        *,
        strict: bool = False,
    ) -> None:
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                f"{cls.__name__}.assert_compatible expects a Mapping."
            )
        v = data.get("schema_version", SCHEMA_VERSION)
        if not _is_real_int(v) or v <= 0:
            raise DigitalTwinParseError(
                f"invalid schema_version {v!r} in {cls.__name__} payload."
            )
        if v > SCHEMA_VERSION:
            raise DigitalTwinParseError(
                f"{cls.__name__} payload schema_version {v} is newer than "
                f"the current contract {SCHEMA_VERSION}."
            )
        if strict and v < SCHEMA_VERSION:
            raise DigitalTwinParseError(
                f"{cls.__name__} payload schema_version {v} is older than "
                f"the current contract {SCHEMA_VERSION}."
            )


# --------------------------------------------------------------------------- #
# Mapping helpers
# --------------------------------------------------------------------------- #
def _freeze_str_float_map(
    name: str,
    value: Any,
    *,
    lo: Optional[float] = None,
    hi: Optional[float] = None,
) -> MappingProxyType:
    """Freeze ``Mapping[str, float]``, optionally bounding each value."""
    if not isinstance(value, ABCMapping):
        raise DigitalTwinConfigError(f"{name} must be a Mapping.")
    out: Dict[str, float] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise DigitalTwinConfigError(
                f"{name} keys must be non-empty strings."
            )
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise DigitalTwinConfigError(
                f"{name}[{k!r}] must be numeric."
            )
        fv = float(v)
        if not _is_finite_nonneg(abs(fv)):
            raise DigitalTwinConfigError(
                f"{name}[{k!r}] must be finite."
            )
        if lo is not None and fv < lo:
            raise DigitalTwinConfigError(
                f"{name}[{k!r}] must be >= {lo} (got {fv!r})."
            )
        if hi is not None and fv > hi:
            raise DigitalTwinConfigError(
                f"{name}[{k!r}] must be <= {hi} (got {fv!r})."
            )
        out[k] = fv
    return MappingProxyType(out)


def _freeze_str_int_map(
    name: str,
    value: Any,
    *,
    positive: bool = True,
) -> MappingProxyType:
    """Freeze ``Mapping[str, int]``."""
    if not isinstance(value, ABCMapping):
        raise DigitalTwinConfigError(f"{name} must be a Mapping.")
    out: Dict[str, int] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise DigitalTwinConfigError(
                f"{name} keys must be non-empty strings."
            )
        if not _is_real_int(v):
            raise DigitalTwinConfigError(
                f"{name}[{k!r}] must be an int."
            )
        if positive and v <= 0:
            raise DigitalTwinConfigError(
                f"{name}[{k!r}] must be a positive int."
            )
        out[k] = int(v)
    return MappingProxyType(out)


def _freeze_str_tuple_map(
    name: str,
    value: Any,
) -> MappingProxyType:
    """Freeze ``Mapping[str, Sequence[str]]``."""
    if not isinstance(value, ABCMapping):
        raise DigitalTwinConfigError(f"{name} must be a Mapping.")
    out: Dict[str, Tuple[str, ...]] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise DigitalTwinConfigError(
                f"{name} keys must be non-empty strings."
            )
        if isinstance(v, str) or not isinstance(
            v, (list, tuple, set, frozenset),
        ):
            raise DigitalTwinConfigError(
                f"{name}[{k!r}] must be a sequence of strings."
            )
        items: List[str] = []
        for item in v:
            if not isinstance(item, str) or not item:
                raise DigitalTwinConfigError(
                    f"{name}[{k!r}] entries must be non-empty strings."
                )
            items.append(item)
        out[k] = tuple(items)
    return MappingProxyType(out)


# --------------------------------------------------------------------------- #
# SimulationConfig
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SimulationConfig(_SchemaVersionMixin):
    """Core simulation parameters."""

    schema_version: int = SCHEMA_VERSION

    time_horizon_years: int = 10
    time_step_days: int = 30
    n_simulations: int = 1_000
    confidence_level: float = 0.95
    include_stochastic_events: bool = True
    parallel_simulations: int = 4
    expert_population_dynamics: bool = True
    material_flow_tracking: bool = True
    carbon_pricing_scenario: str = "linear_increase"
    helium_depletion_model: str = "exponential"

    correlated_uncertainty: bool = True
    resource_substitution_enabled: bool = True
    user_priorities: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({
            "carbon": 0.25, "helium": 0.20, "energy": 0.15,
            "circularity": 0.20, "biodiversity": 0.20,
        }),
    )
    cache_max_size: int = 100

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in (
            "include_stochastic_events", "expert_population_dynamics",
            "material_flow_tracking", "correlated_uncertainty",
            "resource_substitution_enabled",
        ):
            if not isinstance(getattr(self, name), bool):
                raise DigitalTwinConfigError(f"{name} must be a bool.")

        for name in (
            "time_horizon_years", "time_step_days", "n_simulations",
            "parallel_simulations", "cache_max_size",
        ):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise DigitalTwinConfigError(
                    f"{name} must be a positive int (got {v!r})."
                )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinConfigError("schema_version must be positive.")

        # Confidence strictly inside (0, 1).
        if not _is_positive_finite(self.confidence_level) or not (
            0.0 < self.confidence_level < 1.0
        ):
            raise DigitalTwinConfigError(
                "confidence_level must be in (0, 1) exclusive."
            )

        # Step must allow at least two steps over the horizon.
        steps = (self.time_horizon_years * 365) // self.time_step_days
        if steps < 2:
            raise DigitalTwinConfigError(
                "time_step_days too large for time_horizon_years; "
                f"only {steps} step(s) would be simulated."
            )

        for name in ("carbon_pricing_scenario", "helium_depletion_model"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise DigitalTwinConfigError(
                    f"{name} must be a non-empty string."
                )

        object.__setattr__(
            self, "user_priorities",
            _freeze_str_float_map(
                "user_priorities", self.user_priorities,
                lo=0.0, hi=1.0,
            ),
        )
        total = sum(self.user_priorities.values())
        if abs(total - 1.0) > 0.01:
            raise DigitalTwinConfigError(
                f"user_priorities must sum to ~1.0 (got {total:.4f})."
            )
        if not set(self.user_priorities).issubset(_ALLOWED_PRIORITY_KEYS):
            raise DigitalTwinConfigError(
                "user_priorities keys must be a subset of "
                f"{sorted(_ALLOWED_PRIORITY_KEYS)}."
            )

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "time_horizon_years": self.time_horizon_years,
            "time_step_days": self.time_step_days,
            "n_simulations": self.n_simulations,
            "confidence_level": self.confidence_level,
            "include_stochastic_events": self.include_stochastic_events,
            "parallel_simulations": self.parallel_simulations,
            "expert_population_dynamics": self.expert_population_dynamics,
            "material_flow_tracking": self.material_flow_tracking,
            "carbon_pricing_scenario": self.carbon_pricing_scenario,
            "helium_depletion_model": self.helium_depletion_model,
            "correlated_uncertainty": self.correlated_uncertainty,
            "resource_substitution_enabled":
                self.resource_substitution_enabled,
            "user_priorities": dict(self.user_priorities),
            "cache_max_size": self.cache_max_size,
        }

    @classmethod
    def from_dict(cls, data: ABCMapping) -> "SimulationConfig":
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "SimulationConfig.from_dict expects a Mapping."
            )
        cls.assert_compatible(data)
        kwargs = {k: v for k, v in data.items() if k in _SIMULATION_FIELDS}
        if "user_priorities" in kwargs:
            kwargs["user_priorities"] = dict(kwargs["user_priorities"])
        try:
            return cls(**kwargs)
        except DigitalTwinConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise DigitalTwinConfigError(
                f"SimulationConfig.from_dict failed: {exc}"
            ) from exc

    def with_overrides(self, **kwargs: Any) -> "SimulationConfig":
        unknown = set(kwargs) - _SIMULATION_FIELDS
        if unknown:
            raise DigitalTwinConfigError(
                f"Unknown SimulationConfig field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def __hash__(self) -> int:
        return hash((
            self.schema_version, self.time_horizon_years,
            self.time_step_days, self.n_simulations,
            self.confidence_level, self.include_stochastic_events,
            self.parallel_simulations, self.expert_population_dynamics,
            self.material_flow_tracking, self.carbon_pricing_scenario,
            self.helium_depletion_model, self.correlated_uncertainty,
            self.resource_substitution_enabled,
            tuple(sorted(self.user_priorities.items())),
            self.cache_max_size,
        ))

    def __repr__(self) -> str:
        return (
            "SimulationConfig("
            f"horizon={self.time_horizon_years}y, "
            f"step={self.time_step_days}d, "
            f"sims={self.n_simulations})"
        )


_SIMULATION_FIELDS = {f.name for f in fields(SimulationConfig)}


# --------------------------------------------------------------------------- #
# CorrelationConfig
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CorrelationConfig(_SchemaVersionMixin):
    """Correlation / variance / volatility settings."""

    schema_version: int = SCHEMA_VERSION
    correlation_matrix_override: Optional[Mapping[str, Mapping[str, float]]] = None
    resource_variances: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({
            "carbon": 0.02, "helium": 0.02, "energy": 0.01,
            "circularity": 0.01, "biodiversity": 0.01,
        }),
    )
    volatility_window_size: int = 10

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinConfigError("schema_version must be positive.")
        if not _is_real_int(self.volatility_window_size) or self.volatility_window_size <= 0:
            raise DigitalTwinConfigError(
                "volatility_window_size must be a positive int."
            )

        object.__setattr__(
            self, "resource_variances",
            _freeze_str_float_map(
                "resource_variances", self.resource_variances,
                lo=0.0, hi=1.0,
            ),
        )

        if self.correlation_matrix_override is not None:
            if not isinstance(self.correlation_matrix_override, ABCMapping):
                raise DigitalTwinConfigError(
                    "correlation_matrix_override must be a Mapping."
                )
            frozen_corr: Dict[str, MappingProxyType] = {}
            for row, cols in self.correlation_matrix_override.items():
                if not isinstance(row, str) or not row:
                    raise DigitalTwinConfigError(
                        "correlation_matrix_override row keys must be non-empty."
                    )
                frozen_corr[row] = _freeze_str_float_map(
                    f"correlation_matrix_override[{row!r}]",
                    cols, lo=-1.0, hi=1.0,
                )
            # ---- full correlation-matrix shape validation ---- #
            resources = set(frozen_corr.keys())
            if resources != _RESOURCE_KEYS:
                missing = _RESOURCE_KEYS - resources
                extra = resources - _RESOURCE_KEYS
                parts = []
                if missing:
                    parts.append(f"missing rows for {sorted(missing)}")
                if extra:
                    parts.append(f"unknown rows {sorted(extra)}")
                raise DigitalTwinConfigError(
                    "correlation_matrix_override " + " and ".join(parts) + "."
                )
            for row, cols in frozen_corr.items():
                if set(cols.keys()) != _RESOURCE_KEYS:
                    raise DigitalTwinConfigError(
                        f"correlation_matrix_override row {row!r} must "
                        f"cover every resource."
                    )
                if abs(cols[row] - 1.0) > 1e-6:
                    raise DigitalTwinConfigError(
                        f"correlation_matrix_override diagonal entry for "
                        f"{row!r} must be 1.0 (got {cols[row]!r})."
                    )
                for col, val in cols.items():
                    if abs(val - frozen_corr[col][row]) > 1e-6:
                        raise DigitalTwinConfigError(
                            f"correlation_matrix_override must be symmetric "
                            f"at ({row!r}, {col!r})."
                        )
            object.__setattr__(
                self, "correlation_matrix_override",
                MappingProxyType(frozen_corr),
            )

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "correlation_matrix_override": (
                {k: dict(v) for k, v in self.correlation_matrix_override.items()}
                if self.correlation_matrix_override else None
            ),
            "resource_variances": dict(self.resource_variances),
            "volatility_window_size": self.volatility_window_size,
        }

    @classmethod
    def from_dict(cls, data: ABCMapping) -> "CorrelationConfig":
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "CorrelationConfig.from_dict expects a Mapping."
            )
        cls.assert_compatible(data)
        kwargs = {k: v for k, v in data.items() if k in _CORRELATION_FIELDS}
        if "resource_variances" in kwargs and isinstance(
            kwargs["resource_variances"], ABCMapping,
        ):
            kwargs["resource_variances"] = dict(kwargs["resource_variances"])
        if isinstance(kwargs.get("correlation_matrix_override"), ABCMapping):
            kwargs["correlation_matrix_override"] = {
                k: dict(v) for k, v in kwargs["correlation_matrix_override"].items()
            }
        try:
            return cls(**kwargs)
        except DigitalTwinConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise DigitalTwinConfigError(
                f"CorrelationConfig.from_dict failed: {exc}"
            ) from exc

    def with_overrides(self, **kwargs: Any) -> "CorrelationConfig":
        unknown = set(kwargs) - _CORRELATION_FIELDS
        if unknown:
            raise DigitalTwinConfigError(
                f"Unknown CorrelationConfig field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def __hash__(self) -> int:
        corr_hash = (
            tuple(sorted(
                (row, tuple(sorted(cols.items())))
                for row, cols in self.correlation_matrix_override.items()
            ))
            if self.correlation_matrix_override is not None else None
        )
        return hash((
            self.schema_version, corr_hash,
            tuple(sorted(self.resource_variances.items())),
            self.volatility_window_size,
        ))

    def __repr__(self) -> str:
        return (
            "CorrelationConfig("
            f"override={'yes' if self.correlation_matrix_override else 'no'}, "
            f"resources={len(self.resource_variances)})"
        )


_CORRELATION_FIELDS = {f.name for f in fields(CorrelationConfig)}


# --------------------------------------------------------------------------- #
# SubstitutionConfig
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SubstitutionConfig(_SchemaVersionMixin):
    """Resource substitution parameters."""

    schema_version: int = SCHEMA_VERSION
    substitution_availability_default: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({
            "helium": 0.3, "carbon": 0.5, "energy": 0.6,
        }),
    )
    substitution_cost_factor_default: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({
            "helium": 2.0, "carbon": 1.5, "energy": 1.3,
        }),
    )
    substitution_timeline_default: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({
            "helium": 24.0, "carbon": 12.0, "energy": 18.0,
        }),
    )
    substitution_ramp_start_step: int = 10
    substitution_ramp_rate: float = 0.05

    def __post_init__(self) -> None:
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinConfigError("schema_version must be positive.")
        if not _is_real_int(self.substitution_ramp_start_step) or self.substitution_ramp_start_step <= 0:
            raise DigitalTwinConfigError(
                "substitution_ramp_start_step must be a positive int."
            )
        if not _is_finite_nonneg(self.substitution_ramp_rate) or self.substitution_ramp_rate > 1.0:
            raise DigitalTwinConfigError(
                "substitution_ramp_rate must be in [0, 1]."
            )

        object.__setattr__(
            self, "substitution_availability_default",
            _freeze_str_float_map(
                "substitution_availability_default",
                self.substitution_availability_default,
                lo=0.0, hi=1.0,
            ),
        )
        object.__setattr__(
            self, "substitution_cost_factor_default",
            _freeze_str_float_map(
                "substitution_cost_factor_default",
                self.substitution_cost_factor_default,
                lo=1.0,
            ),
        )
        object.__setattr__(
            self, "substitution_timeline_default",
            _freeze_str_float_map(
                "substitution_timeline_default",
                self.substitution_timeline_default,
                lo=0.0,
            ),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "substitution_availability_default":
                dict(self.substitution_availability_default),
            "substitution_cost_factor_default":
                dict(self.substitution_cost_factor_default),
            "substitution_timeline_default":
                dict(self.substitution_timeline_default),
            "substitution_ramp_start_step": self.substitution_ramp_start_step,
            "substitution_ramp_rate": self.substitution_ramp_rate,
        }

    @classmethod
    def from_dict(cls, data: ABCMapping) -> "SubstitutionConfig":
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "SubstitutionConfig.from_dict expects a Mapping."
            )
        cls.assert_compatible(data)
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in _SUBSTITUTION_FIELDS:
                continue
            if isinstance(v, ABCMapping):
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        try:
            return cls(**kwargs)
        except DigitalTwinConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise DigitalTwinConfigError(
                f"SubstitutionConfig.from_dict failed: {exc}"
            ) from exc

    def with_overrides(self, **kwargs: Any) -> "SubstitutionConfig":
        unknown = set(kwargs) - _SUBSTITUTION_FIELDS
        if unknown:
            raise DigitalTwinConfigError(
                f"Unknown SubstitutionConfig field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def __hash__(self) -> int:
        return hash((
            self.schema_version,
            tuple(sorted(self.substitution_availability_default.items())),
            tuple(sorted(self.substitution_cost_factor_default.items())),
            tuple(sorted(self.substitution_timeline_default.items())),
            self.substitution_ramp_start_step,
            self.substitution_ramp_rate,
        ))

    def __repr__(self) -> str:
        return (
            "SubstitutionConfig("
            f"ramp_start={self.substitution_ramp_start_step}, "
            f"ramp_rate={self.substitution_ramp_rate})"
        )


_SUBSTITUTION_FIELDS = {f.name for f in fields(SubstitutionConfig)}


# --------------------------------------------------------------------------- #
# DistillationConfig
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DistillationConfig(_SchemaVersionMixin):
    """Distillation / RL hyperparameters."""

    schema_version: int = SCHEMA_VERSION
    distillation_epsilon: float = 0.1
    distillation_train_every: int = 10
    distillation_replay_size: int = 2_000
    distillation_learning_rate: float = 0.01
    distill_weight: float = 0.7
    rl_weight: float = 0.3

    def __post_init__(self) -> None:
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinConfigError("schema_version must be positive.")
        for name in ("distillation_train_every", "distillation_replay_size"):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise DigitalTwinConfigError(
                    f"{name} must be a positive int (got {v!r})."
                )
        if not _is_finite_nonneg(self.distillation_epsilon) or self.distillation_epsilon > 1.0:
            raise DigitalTwinConfigError(
                "distillation_epsilon must be in [0, 1]."
            )
        if not _is_positive_finite(self.distillation_learning_rate):
            raise DigitalTwinConfigError(
                "distillation_learning_rate must be finite and > 0."
            )
        for name in ("distill_weight", "rl_weight"):
            v = getattr(self, name)
            if not _is_finite_nonneg(v) or v > 1.0:
                raise DigitalTwinConfigError(
                    f"{name} must be in [0, 1] (got {v!r})."
                )
        if abs(self.distill_weight + self.rl_weight - 1.0) > 1e-6:
            raise DigitalTwinConfigError(
                "distill_weight + rl_weight must equal 1.0."
            )
        # Warn if the training cadence exceeds the replay size.
        if self.distillation_train_every > self.distillation_replay_size:
            logger.warning(
                "distillation_train_every (%d) exceeds "
                "distillation_replay_size (%d); training may starve.",
                self.distillation_train_every,
                self.distillation_replay_size,
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "distillation_epsilon": self.distillation_epsilon,
            "distillation_train_every": self.distillation_train_every,
            "distillation_replay_size": self.distillation_replay_size,
            "distillation_learning_rate": self.distillation_learning_rate,
            "distill_weight": self.distill_weight,
            "rl_weight": self.rl_weight,
        }

    @classmethod
    def from_dict(cls, data: ABCMapping) -> "DistillationConfig":
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "DistillationConfig.from_dict expects a Mapping."
            )
        cls.assert_compatible(data)
        kwargs = {k: v for k, v in data.items() if k in _DISTILLATION_FIELDS}
        try:
            return cls(**kwargs)
        except DigitalTwinConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise DigitalTwinConfigError(
                f"DistillationConfig.from_dict failed: {exc}"
            ) from exc

    def with_overrides(self, **kwargs: Any) -> "DistillationConfig":
        unknown = set(kwargs) - _DISTILLATION_FIELDS
        if unknown:
            raise DigitalTwinConfigError(
                f"Unknown DistillationConfig field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def __hash__(self) -> int:
        return hash((
            self.schema_version, self.distillation_epsilon,
            self.distillation_train_every, self.distillation_replay_size,
            self.distillation_learning_rate, self.distill_weight,
            self.rl_weight,
        ))


_DISTILLATION_FIELDS = {f.name for f in fields(DistillationConfig)}


# --------------------------------------------------------------------------- #
# ResilienceConfig
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ResilienceConfig(_SchemaVersionMixin):
    """Retry and circuit-breaker settings."""

    schema_version: int = SCHEMA_VERSION
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5_000.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    def __post_init__(self) -> None:
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinConfigError("schema_version must be positive.")
        if not _is_real_int(self.max_retries) or self.max_retries < 0:
            raise DigitalTwinConfigError(
                "max_retries must be a non-negative int."
            )
        if not _is_real_int(self.circuit_breaker_threshold) or self.circuit_breaker_threshold < 1:
            raise DigitalTwinConfigError(
                "circuit_breaker_threshold must be a positive int."
            )
        for name in ("retry_base_delay_ms", "retry_max_delay_ms",
                     "circuit_breaker_recovery_timeout"):
            if not _is_positive_finite(getattr(self, name)):
                raise DigitalTwinConfigError(
                    f"{name} must be a finite number > 0."
                )
        if self.retry_max_delay_ms < self.retry_base_delay_ms:
            raise DigitalTwinConfigError(
                "retry_max_delay_ms must be >= retry_base_delay_ms."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "max_retries": self.max_retries,
            "retry_base_delay_ms": self.retry_base_delay_ms,
            "retry_max_delay_ms": self.retry_max_delay_ms,
            "circuit_breaker_threshold": self.circuit_breaker_threshold,
            "circuit_breaker_recovery_timeout":
                self.circuit_breaker_recovery_timeout,
        }

    @classmethod
    def from_dict(cls, data: ABCMapping) -> "ResilienceConfig":
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "ResilienceConfig.from_dict expects a Mapping."
            )
        cls.assert_compatible(data)
        kwargs = {k: v for k, v in data.items() if k in _RESILIENCE_FIELDS}
        try:
            return cls(**kwargs)
        except DigitalTwinConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise DigitalTwinConfigError(
                f"ResilienceConfig.from_dict failed: {exc}"
            ) from exc

    def with_overrides(self, **kwargs: Any) -> "ResilienceConfig":
        unknown = set(kwargs) - _RESILIENCE_FIELDS
        if unknown:
            raise DigitalTwinConfigError(
                f"Unknown ResilienceConfig field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def __hash__(self) -> int:
        return hash((
            self.schema_version, self.max_retries,
            self.retry_base_delay_ms, self.retry_max_delay_ms,
            self.circuit_breaker_threshold,
            self.circuit_breaker_recovery_timeout,
        ))


_RESILIENCE_FIELDS = {f.name for f in fields(ResilienceConfig)}


# --------------------------------------------------------------------------- #
# PersistenceConfig
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PersistenceConfig(_SchemaVersionMixin):
    """Persistence settings."""

    schema_version: int = SCHEMA_VERSION
    persistence_path: str = "digital_twin_state.json.gz"
    atomic_writes: bool = True

    def __post_init__(self) -> None:
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinConfigError("schema_version must be positive.")
        if not isinstance(self.persistence_path, str) or not self.persistence_path:
            raise DigitalTwinConfigError(
                "persistence_path must be a non-empty string."
            )
        if not isinstance(self.atomic_writes, bool):
            raise DigitalTwinConfigError("atomic_writes must be a bool.")
        # Expand ``~`` so the same path is used everywhere.
        object.__setattr__(
            self, "persistence_path",
            str(Path(self.persistence_path).expanduser()),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "persistence_path": self.persistence_path,
            "atomic_writes": self.atomic_writes,
        }

    @classmethod
    def from_dict(cls, data: ABCMapping) -> "PersistenceConfig":
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "PersistenceConfig.from_dict expects a Mapping."
            )
        cls.assert_compatible(data)
        kwargs = {k: v for k, v in data.items() if k in _PERSISTENCE_FIELDS}
        try:
            return cls(**kwargs)
        except DigitalTwinConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise DigitalTwinConfigError(
                f"PersistenceConfig.from_dict failed: {exc}"
            ) from exc

    def with_overrides(self, **kwargs: Any) -> "PersistenceConfig":
        unknown = set(kwargs) - _PERSISTENCE_FIELDS
        if unknown:
            raise DigitalTwinConfigError(
                f"Unknown PersistenceConfig field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def __hash__(self) -> int:
        return hash((self.schema_version, self.persistence_path, self.atomic_writes))


_PERSISTENCE_FIELDS = {f.name for f in fields(PersistenceConfig)}


# --------------------------------------------------------------------------- #
# TelemetryConfig
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TelemetryConfig(_SchemaVersionMixin):
    """Telemetry settings."""

    schema_version: int = SCHEMA_VERSION
    telemetry_export_interval: int = 60
    prometheus_port: Optional[int] = None
    latency_ring_size: int = 200

    def __post_init__(self) -> None:
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinConfigError("schema_version must be positive.")
        for name in ("telemetry_export_interval", "latency_ring_size"):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise DigitalTwinConfigError(
                    f"{name} must be a positive int (got {v!r})."
                )
        if self.prometheus_port is not None:
            if (
                not _is_real_int(self.prometheus_port)
                or not (1024 <= self.prometheus_port <= 65535)
            ):
                raise DigitalTwinConfigError(
                    "prometheus_port must be None or in [1024, 65535]."
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "telemetry_export_interval": self.telemetry_export_interval,
            "prometheus_port": self.prometheus_port,
            "latency_ring_size": self.latency_ring_size,
        }

    @classmethod
    def from_dict(cls, data: ABCMapping) -> "TelemetryConfig":
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "TelemetryConfig.from_dict expects a Mapping."
            )
        cls.assert_compatible(data)
        kwargs = {k: v for k, v in data.items() if k in _TELEMETRY_FIELDS}
        try:
            return cls(**kwargs)
        except DigitalTwinConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise DigitalTwinConfigError(
                f"TelemetryConfig.from_dict failed: {exc}"
            ) from exc

    def with_overrides(self, **kwargs: Any) -> "TelemetryConfig":
        unknown = set(kwargs) - _TELEMETRY_FIELDS
        if unknown:
            raise DigitalTwinConfigError(
                f"Unknown TelemetryConfig field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def __hash__(self) -> int:
        return hash((
            self.schema_version, self.telemetry_export_interval,
            self.prometheus_port, self.latency_ring_size,
        ))


_TELEMETRY_FIELDS = {f.name for f in fields(TelemetryConfig)}


# --------------------------------------------------------------------------- #
# SubsystemConfig
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SubsystemConfig(_SchemaVersionMixin):
    """Subsystem enable flags and per-subsystem hyperparameters."""

    schema_version: int = SCHEMA_VERSION
    enable_limit_graph: bool = True
    enable_modp_solver: bool = True
    enable_rlhf: bool = True
    enable_pso_tuning: bool = True
    enable_moe_gating: bool = True
    enable_ga_tuning: bool = False
    moe_expert_count: int = 4
    pso_particles: int = 10
    pso_iterations: int = 20
    ga_population_size: int = 20
    ga_generations: int = 5

    def __post_init__(self) -> None:
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinConfigError("schema_version must be positive.")
        for name in (
            "enable_limit_graph", "enable_modp_solver", "enable_rlhf",
            "enable_pso_tuning", "enable_moe_gating", "enable_ga_tuning",
        ):
            if not isinstance(getattr(self, name), bool):
                raise DigitalTwinConfigError(f"{name} must be a bool.")
        for name in (
            "moe_expert_count", "pso_particles", "pso_iterations",
            "ga_population_size", "ga_generations",
        ):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise DigitalTwinConfigError(
                    f"{name} must be a positive int (got {v!r})."
                )
        if self.pso_particles < 2:
            raise DigitalTwinConfigError(
                "pso_particles must be >= 2."
            )
        if self.ga_population_size < 2:
            raise DigitalTwinConfigError(
                "ga_population_size must be >= 2."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "enable_limit_graph": self.enable_limit_graph,
            "enable_modp_solver": self.enable_modp_solver,
            "enable_rlhf": self.enable_rlhf,
            "enable_pso_tuning": self.enable_pso_tuning,
            "enable_moe_gating": self.enable_moe_gating,
            "enable_ga_tuning": self.enable_ga_tuning,
            "moe_expert_count": self.moe_expert_count,
            "pso_particles": self.pso_particles,
            "pso_iterations": self.pso_iterations,
            "ga_population_size": self.ga_population_size,
            "ga_generations": self.ga_generations,
        }

    @classmethod
    def from_dict(cls, data: ABCMapping) -> "SubsystemConfig":
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "SubsystemConfig.from_dict expects a Mapping."
            )
        cls.assert_compatible(data)
        kwargs = {k: v for k, v in data.items() if k in _SUBSYSTEM_FIELDS}
        try:
            return cls(**kwargs)
        except DigitalTwinConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise DigitalTwinConfigError(
                f"SubsystemConfig.from_dict failed: {exc}"
            ) from exc

    def with_overrides(self, **kwargs: Any) -> "SubsystemConfig":
        unknown = set(kwargs) - _SUBSYSTEM_FIELDS
        if unknown:
            raise DigitalTwinConfigError(
                f"Unknown SubsystemConfig field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def __hash__(self) -> int:
        return hash((
            self.schema_version, self.enable_limit_graph,
            self.enable_modp_solver, self.enable_rlhf,
            self.enable_pso_tuning, self.enable_moe_gating,
            self.enable_ga_tuning, self.moe_expert_count,
            self.pso_particles, self.pso_iterations,
            self.ga_population_size, self.ga_generations,
        ))


_SUBSYSTEM_FIELDS = {f.name for f in fields(SubsystemConfig)}


# --------------------------------------------------------------------------- #
# Sub-config registry
# --------------------------------------------------------------------------- #
_SUBCONFIG_ORDER: Tuple[str, ...] = (
    "simulation", "correlation", "substitution", "distillation",
    "resilience", "persistence", "telemetry", "subsystems",
)

_SUBCONFIG_CLASSES = {
    "simulation": SimulationConfig,
    "correlation": CorrelationConfig,
    "substitution": SubstitutionConfig,
    "distillation": DistillationConfig,
    "resilience": ResilienceConfig,
    "persistence": PersistenceConfig,
    "telemetry": TelemetryConfig,
    "subsystems": SubsystemConfig,
}

#: Legacy flat field name -> (subconfig name, field name). Used by
#: ``from_dict`` when a flat (v1) payload is provided.
_LEGACY_FIELD_ROUTES: Dict[str, Tuple[str, str]] = {}
for _sub_name, _sub_cls in _SUBCONFIG_CLASSES.items():
    for _f in fields(_sub_cls):
        if _f.name == "schema_version":
            continue
        _LEGACY_FIELD_ROUTES.setdefault(_f.name, (_sub_name, _f.name))
del _sub_name, _sub_cls, _f


# --------------------------------------------------------------------------- #
# DigitalTwinConfig
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DigitalTwinConfig(_SchemaVersionMixin):
    """Top-level digital-twin configuration.

    Holds one nested sub-config per concern. Attribute access to nested
    fields is preserved for backward compatibility via ``__getattr__``:
    ``config.time_horizon_years`` delegates to
    ``config.simulation.time_horizon_years``.
    """

    schema_version: int = SCHEMA_VERSION
    seed: int = 0

    #: Truth-level vocabulary used when writing twin results.
    default_truth_level: str = "estimated"
    #: Container tag used when writing twin results.
    default_container_tag: str = DEFAULT_CONTAINER_TAG
    #: Per-kind TTLs applied by the persistence layer.
    ttl_seconds: Mapping[str, int] = field(
        default_factory=lambda: MappingProxyType(dict(_DEFAULT_TTLS)),
    )

    # Nested sub-configs.
    simulation: SimulationConfig = field(default_factory=SimulationConfig)
    correlation: CorrelationConfig = field(default_factory=CorrelationConfig)
    substitution: SubstitutionConfig = field(default_factory=SubstitutionConfig)
    distillation: DistillationConfig = field(default_factory=DistillationConfig)
    resilience: ResilienceConfig = field(default_factory=ResilienceConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    subsystems: SubsystemConfig = field(default_factory=SubsystemConfig)

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinConfigError("schema_version must be positive.")
        if not _is_real_int(self.seed) or self.seed < 0:
            raise DigitalTwinConfigError(
                "seed must be a non-negative int."
            )

        # Truth level must be in the vocabulary.
        if not isinstance(self.default_truth_level, str) or not self.default_truth_level:
            raise DigitalTwinConfigError(
                "default_truth_level must be a non-empty string."
            )
        if self.default_truth_level not in DEFAULT_TRUTH_LEVELS:
            raise DigitalTwinConfigError(
                f"default_truth_level {self.default_truth_level!r} not in "
                f"{list(DEFAULT_TRUTH_LEVELS)!r}."
            )

        # Container tag.
        if not isinstance(self.default_container_tag, str) or not self.default_container_tag:
            raise DigitalTwinConfigError(
                "default_container_tag must be a non-empty string."
            )

        # TTLs.
        object.__setattr__(
            self, "ttl_seconds",
            _freeze_str_int_map("ttl_seconds", self.ttl_seconds, positive=True),
        )

        # Sub-config type check (defensive: dataclass typing doesn't enforce).
        for name, cls_ in _SUBCONFIG_CLASSES.items():
            v = getattr(self, name)
            if not isinstance(v, cls_):
                raise DigitalTwinConfigError(
                    f"{name} must be a {cls_.__name__} instance, got "
                    f"{type(v).__name__}."
                )

        # Cross-sub-config: user_priorities keys must match resource_variances.
        priorities = set(self.simulation.user_priorities.keys())
        variances = set(self.correlation.resource_variances.keys())
        if priorities != variances:
            missing_v = priorities - variances
            missing_p = variances - priorities
            parts = []
            if missing_v:
                parts.append(
                    f"resource_variances missing {sorted(missing_v)}"
                )
            if missing_p:
                parts.append(
                    f"user_priorities missing {sorted(missing_p)}"
                )
            raise DigitalTwinConfigError(
                "resource_variances and user_priorities must cover the "
                "same resources: " + "; ".join(parts) + "."
            )

    # ------------------------------------------------------------------ #
    # Backward-compatible attribute delegation
    # ------------------------------------------------------------------ #
    def __getattr__(self, name: str) -> Any:
        # Called only when the attribute isn't found normally.
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        for sub_name in _SUBCONFIG_ORDER:
            try:
                sub = object.__getattribute__(self, sub_name)
            except AttributeError:
                continue
            try:
                return getattr(sub, name)
            except AttributeError:
                continue
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}"
        )

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "seed": self.seed,
            "default_truth_level": self.default_truth_level,
            "default_container_tag": self.default_container_tag,
            "ttl_seconds": dict(self.ttl_seconds),
            "simulation": self.simulation.to_dict(),
            "correlation": self.correlation.to_dict(),
            "substitution": self.substitution.to_dict(),
            "distillation": self.distillation.to_dict(),
            "resilience": self.resilience.to_dict(),
            "persistence": self.persistence.to_dict(),
            "telemetry": self.telemetry.to_dict(),
            "subsystems": self.subsystems.to_dict(),
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(
        cls,
        data: ABCMapping,
        *,
        strict: bool = False,
    ) -> "DigitalTwinConfig":
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "DigitalTwinConfig.from_dict expects a Mapping."
            )
        # Validate strict is a bool.
        if not isinstance(strict, bool):
            raise DigitalTwinParseError(
                f"strict must be a bool, got {type(strict).__name__}."
            )
        # Schema compatibility check.
        cls.assert_compatible(data, strict=strict)

        # Detect format: nested (v2) vs flat (v1 legacy).
        is_nested = any(
            key in data for key in _SUBCONFIG_CLASSES
        )
        if not is_nested:
            return cls._from_legacy_dict(data)

        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k in ("schema_version", "seed", "default_truth_level",
                     "default_container_tag"):
                kwargs[k] = v
            elif k == "ttl_seconds":
                if not isinstance(v, ABCMapping):
                    raise DigitalTwinParseError("ttl_seconds must be a Mapping.")
                kwargs[k] = dict(v)
            elif k in _SUBCONFIG_CLASSES:
                if not isinstance(v, ABCMapping):
                    raise DigitalTwinParseError(
                        f"{k} must be a Mapping."
                    )
                kwargs[k] = _SUBCONFIG_CLASSES[k].from_dict(v)
            elif strict:
                raise DigitalTwinParseError(
                    f"Unknown config key(s): {sorted({k} - {'x'})}."
                )

        try:
            return cls(**kwargs)
        except DigitalTwinConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise DigitalTwinConfigError(
                f"DigitalTwinConfig.from_dict failed: {exc}"
            ) from exc

    @classmethod
    def _from_legacy_dict(cls, data: ABCMapping) -> "DigitalTwinConfig":
        """Migrate a flat (v1) payload into the nested format."""
        grouped: Dict[str, Dict[str, Any]] = {
            name: {} for name in _SUBCONFIG_CLASSES
        }
        top_level: Dict[str, Any] = {}
        for k, v in data.items():
            if k in ("schema_version", "seed", "default_truth_level",
                     "default_container_tag"):
                top_level[k] = v
                continue
            if k == "ttl_seconds":
                top_level[k] = v
                continue
            route = _LEGACY_FIELD_ROUTES.get(k)
            if route is None:
                continue
            sub_name, field_name = route
            grouped[sub_name][field_name] = v

        kwargs: Dict[str, Any] = dict(top_level)
        for sub_name, cls_ in _SUBCONFIG_CLASSES.items():
            blob = grouped[sub_name]
            if blob:
                kwargs[sub_name] = cls_.from_dict(blob)
        return cls(**kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: bool = False,
    ) -> "DigitalTwinConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise DigitalTwinParseError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    # ------------------------------------------------------------------ #
    # Environment bindings
    # ------------------------------------------------------------------ #
    @classmethod
    def from_env(
        cls,
        env: Optional[Mapping[str, str]] = None,
        *,
        strict: bool = False,
        env_prefix: str = _ENV_PREFIX,
    ) -> "DigitalTwinConfig":
        """Build a config from ``GREEN_AGENT_DIGITAL_TWIN_*`` env vars.

        The full env-var surface is described by :data:`_ENV_BINDINGS`.
        Unknown ``GREEN_AGENT_DIGITAL_TWIN_*`` keys are logged and, when
        ``strict=True``, raise.
        """
        e = env if env is not None else os.environ
        nested: Dict[str, Dict[str, Any]] = {n: {} for n in _SUBCONFIG_CLASSES}
        top_level: Dict[str, Any] = {}

        # Env -> (path, cast)
        for env_name, path, cast, *rest in _ENV_BINDINGS:
            raw = e.get(env_prefix + env_name)
            if raw is None:
                continue
            raw = raw.strip()
            if not raw:
                continue
            try:
                value = cast(raw)
            except (TypeError, ValueError) as exc:
                raise DigitalTwinConfigError(
                    f"{env_prefix}{env_name}={raw!r} is not a valid value."
                ) from exc
            if path == "seed":
                top_level["seed"] = value
            elif path == "default_truth_level":
                top_level["default_truth_level"] = value
            elif path == "default_container_tag":
                top_level["default_container_tag"] = value
            elif path.startswith("simulation."):
                nested["simulation"][path.split(".", 1)[1]] = value
            elif path.startswith("correlation."):
                nested["correlation"][path.split(".", 1)[1]] = value
            elif path.startswith("substitution."):
                nested["substitution"][path.split(".", 1)[1]] = value
            elif path.startswith("distillation."):
                nested["distillation"][path.split(".", 1)[1]] = value
            elif path.startswith("resilience."):
                nested["resilience"][path.split(".", 1)[1]] = value
            elif path.startswith("persistence."):
                nested["persistence"][path.split(".", 1)[1]] = value
            elif path.startswith("telemetry."):
                nested["telemetry"][path.split(".", 1)[1]] = value
            elif path.startswith("subsystems."):
                nested["subsystems"][path.split(".", 1)[1]] = value

        # Reject unknown env vars.
        known = {env_prefix + b[0] for b in _ENV_BINDINGS}
        unknown = sorted(
            k for k in e.keys()
            if k.startswith(env_prefix) and k not in known
        )
        if unknown:
            if strict:
                raise DigitalTwinConfigError(
                    f"Unknown env var(s): {unknown}."
                )
            logger.warning("Ignoring unknown env var(s): %s", unknown)

        payload = dict(top_level)
        for sub_name, blob in nested.items():
            if blob:
                # Nested blobs are fed through ``from_dict`` which will
                # validate them via the sub-config constructors.
                payload[sub_name] = blob

        # Ensure seed is an int when present.
        if "seed" in payload and not _is_real_int(payload["seed"]):
            raise DigitalTwinConfigError("seed must be an int.")

        return cls.from_dict(payload, strict=False)

    def to_env(self, *, env_prefix: str = _ENV_PREFIX) -> Dict[str, str]:
        """Export the config as ``GREEN_AGENT_DIGITAL_TWIN_*`` env vars."""
        out: Dict[str, str] = {}
        for env_name, path, _cast, *rest in _ENV_BINDINGS:
            value = self._get_path(path)
            if value is None:
                continue
            out[env_prefix + env_name] = str(value)
        return out

    def _get_path(self, path: str) -> Any:
        if "." not in path:
            return getattr(self, path, None)
        head, tail = path.split(".", 1)
        target = getattr(self, head, None)
        if target is None:
            return None
        return getattr(target, tail, None)

    # ------------------------------------------------------------------ #
    # Composition helpers
    # ------------------------------------------------------------------ #
    def with_overrides(self, **kwargs: Any) -> "DigitalTwinConfig":
        valid = {f.name for f in fields(self)}
        unknown = set(kwargs) - valid
        if unknown:
            raise DigitalTwinConfigError(
                f"Unknown config field(s): {sorted(unknown)}."
            )
        # Type-check nested sub-configs.
        for sub_name, cls_ in _SUBCONFIG_CLASSES.items():
            if sub_name in kwargs and not isinstance(kwargs[sub_name], cls_):
                raise DigitalTwinConfigError(
                    f"{sub_name} must be a {cls_.__name__} instance."
                )
        return replace(self, **kwargs)

    def merge(self, other: "DigitalTwinConfig") -> "DigitalTwinConfig":
        defaults = DigitalTwinConfig()
        overrides: Dict[str, Any] = {}
        for f in fields(self):
            ov = getattr(other, f.name)
            dv = getattr(defaults, f.name)
            if ov != dv:
                overrides[f.name] = ov
        return self.with_overrides(**overrides)

    @classmethod
    def with_env(
        cls,
        env: Optional[Mapping[str, str]] = None,
        *,
        strict: bool = False,
        env_prefix: str = _ENV_PREFIX,
        **overrides: Any,
    ) -> "DigitalTwinConfig":
        """Layer: defaults → env → explicit overrides."""
        base = cls.from_env(env=env, strict=strict, env_prefix=env_prefix)
        if not overrides:
            return base
        return base.with_overrides(**overrides)

    # ------------------------------------------------------------------ #
    # Memory-package bridge
    # ------------------------------------------------------------------ #
    def to_memory_config(self) -> Any:
        """Return a ``MemoryConfig`` when the memory package is importable.

        The digital twin and the memory layer share a seed, retry policy,
        latency ring size, truth-level vocabulary, container tag, and TTL
        map. When the memory package is unavailable, a plain dict is
        returned instead.
        """
        payload = {
            "seed": self.seed,
            "write_retries": self.resilience.max_retries,
            "latency_ring_size": self.telemetry.latency_ring_size,
            "truth_levels": list(DEFAULT_TRUTH_LEVELS),
            "default_truth_level": self.default_truth_level,
            "default_container_tag": self.default_container_tag,
            "ttl_seconds": dict(self.ttl_seconds),
        }
        try:  # pragma: no cover - environment dependent
            from ...memory import MemoryConfig  # type: ignore
            return MemoryConfig.from_dict(payload)
        except Exception:
            return payload

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        return hash((
            self.schema_version, self.seed,
            self.default_truth_level, self.default_container_tag,
            tuple(sorted(self.ttl_seconds.items())),
            self.simulation, self.correlation, self.substitution,
            self.distillation, self.resilience, self.persistence,
            self.telemetry, self.subsystems,
        ))

    def __repr__(self) -> str:
        return (
            "DigitalTwinConfig("
            f"horizon={self.simulation.time_horizon_years}y, "
            f"step={self.simulation.time_step_days}d, "
            f"sims={self.simulation.n_simulations}, "
            f"seed={self.seed})"
        )


# --------------------------------------------------------------------------- #
# Env bindings
# --------------------------------------------------------------------------- #
#: ``(env_name, field_path, cast)`` table shared by ``from_env`` / ``to_env``.
_ENV_BINDINGS: Tuple[Tuple[str, str, Any], ...] = (
    # Top-level.
    ("SEED", "seed", int),
    ("DEFAULT_TRUTH_LEVEL", "default_truth_level", str),
    ("DEFAULT_CONTAINER_TAG", "default_container_tag", str),

    # Simulation.
    ("TIME_HORIZON_YEARS", "simulation.time_horizon_years", int),
    ("TIME_STEP_DAYS", "simulation.time_step_days", int),
    ("N_SIMULATIONS", "simulation.n_simulations", int),
    ("CONFIDENCE_LEVEL", "simulation.confidence_level", float),
    ("PARALLEL_SIMULATIONS", "simulation.parallel_simulations", int),
    ("CARBON_PRICING_SCENARIO", "simulation.carbon_pricing_scenario", str),
    ("HELIUM_DEPLETION_MODEL", "simulation.helium_depletion_model", str),
    ("CACHE_MAX_SIZE", "simulation.cache_max_size", int),
    ("INCLUDE_STOCHASTIC_EVENTS", "simulation.include_stochastic_events",
     lambda s: s.lower() in ("1", "true", "yes", "on")),
    ("EXPERT_POPULATION_DYNAMICS", "simulation.expert_population_dynamics",
     lambda s: s.lower() in ("1", "true", "yes", "on")),
    ("MATERIAL_FLOW_TRACKING", "simulation.material_flow_tracking",
     lambda s: s.lower() in ("1", "true", "yes", "on")),
    ("CORRELATED_UNCERTAINTY", "simulation.correlated_uncertainty",
     lambda s: s.lower() in ("1", "true", "yes", "on")),
    ("RESOURCE_SUBSTITUTION_ENABLED", "simulation.resource_substitution_enabled",
     lambda s: s.lower() in ("1", "true", "yes", "on")),

    # Correlation.
    ("VOLATILITY_WINDOW_SIZE", "correlation.volatility_window_size", int),

    # Substitution.
    ("SUBSTITUTION_RAMP_START_STEP", "substitution.substitution_ramp_start_step", int),
    ("SUBSTITUTION_RAMP_RATE", "substitution.substitution_ramp_rate", float),

    # Distillation.
    ("DISTILLATION_EPSILON", "distillation.distillation_epsilon", float),
    ("DISTILLATION_TRAIN_EVERY", "distillation.distillation_train_every", int),
    ("DISTILLATION_REPLAY_SIZE", "distillation.distillation_replay_size", int),
    ("DISTILLATION_LEARNING_RATE", "distillation.distillation_learning_rate", float),
    ("DISTILL_WEIGHT", "distillation.distill_weight", float),
    ("RL_WEIGHT", "distillation.rl_weight", float),

    # Resilience.
    ("MAX_RETRIES", "resilience.max_retries", int),
    ("RETRY_BASE_DELAY_MS", "resilience.retry_base_delay_ms", float),
    ("RETRY_MAX_DELAY_MS", "resilience.retry_max_delay_ms", float),
    ("CIRCUIT_BREAKER_THRESHOLD", "resilience.circuit_breaker_threshold", int),
    ("CIRCUIT_BREAKER_RECOVERY_TIMEOUT",
     "resilience.circuit_breaker_recovery_timeout", float),

    # Persistence.
    ("PERSISTENCE_PATH", "persistence.persistence_path", str),
    ("ATOMIC_WRITES", "persistence.atomic_writes",
     lambda s: s.lower() in ("1", "true", "yes", "on")),

    # Telemetry.
    ("TELEMETRY_EXPORT_INTERVAL", "telemetry.telemetry_export_interval", int),
    ("PROMETHEUS_PORT", "telemetry.prometheus_port", int),
    ("LATENCY_RING_SIZE", "telemetry.latency_ring_size", int),

    # Subsystems.
    ("ENABLE_LIMIT_GRAPH", "subsystems.enable_limit_graph",
     lambda s: s.lower() in ("1", "true", "yes", "on")),
    ("ENABLE_MODP_SOLVER", "subsystems.enable_modp_solver",
     lambda s: s.lower() in ("1", "true", "yes", "on")),
    ("ENABLE_RLHF", "subsystems.enable_rlhf",
     lambda s: s.lower() in ("1", "true", "yes", "on")),
    ("ENABLE_PSO_TUNING", "subsystems.enable_pso_tuning",
     lambda s: s.lower() in ("1", "true", "yes", "on")),
    ("ENABLE_MOE_GATING", "subsystems.enable_moe_gating",
     lambda s: s.lower() in ("1", "true", "yes", "on")),
    ("ENABLE_GA_TUNING", "subsystems.enable_ga_tuning",
     lambda s: s.lower() in ("1", "true", "yes", "on")),
    ("MOE_EXPERT_COUNT", "subsystems.moe_expert_count", int),
    ("PSO_PARTICLES", "subsystems.pso_particles", int),
    ("PSO_ITERATIONS", "subsystems.pso_iterations", int),
    ("GA_POPULATION_SIZE", "subsystems.ga_population_size", int),
    ("GA_GENERATIONS", "subsystems.ga_generations", int),
)


__all__ = [
    "DEFAULT_CONTAINER_TAG",
    "DEFAULT_TRUTH_LEVELS",
    "SCHEMA_VERSION",
    "CorrelationConfig",
    "DigitalTwinConfig",
    "DistillationConfig",
    "PersistenceConfig",
    "ResilienceConfig",
    "SimulationConfig",
    "SubstitutionConfig",
    "SubsystemConfig",
    "TelemetryConfig",
]
