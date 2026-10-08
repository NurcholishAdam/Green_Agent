# src/quantum_integration/digital_twin/digital_twin_config.py

"""Frozen, validated, serializable config for the digital twin."""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field, fields, replace
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .digital_twin_errors import DigitalTwinConfigError
from .digital_twin_helpers import (
    _is_finite_nonneg,
    _is_positive_finite,
    _is_real_int,
)

SCHEMA_VERSION: int = 1

_ENV_PREFIX = "GREEN_AGENT_DIGITAL_TWIN_"


def _freeze_str_float_map(name: str, value: Any) -> Mapping[str, float]:
    if not isinstance(value, Mapping):
        raise DigitalTwinConfigError(f"{name} must be a Mapping.")
    out: Dict[str, float] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise DigitalTwinConfigError(
                f"{name} keys must be non-empty strings."
            )
        if not _is_finite_nonneg(v):
            raise DigitalTwinConfigError(
                f"{name}[{k!r}] must be finite and >= 0."
            )
        out[k] = float(v)
    return MappingProxyType(out)


def _freeze_str_str_map(name: str, value: Any) -> Mapping[str, str]:
    if not isinstance(value, Mapping):
        raise DigitalTwinConfigError(f"{name} must be a Mapping.")
    out: Dict[str, str] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k or not isinstance(v, str) or not v:
            raise DigitalTwinConfigError(
                f"{name} entries must be non-empty strings."
            )
        out[k] = v
    return MappingProxyType(out)


def _freeze_str_int_map(name: str, value: Any) -> Mapping[str, int]:
    if not isinstance(value, Mapping):
        raise DigitalTwinConfigError(f"{name} must be a Mapping.")
    out: Dict[str, int] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise DigitalTwinConfigError(
                f"{name} keys must be non-empty strings."
            )
        if not _is_real_int(v) or v <= 0:
            raise DigitalTwinConfigError(
                f"{name}[{k!r}] must be a positive int."
            )
        out[k] = int(v)
    return MappingProxyType(out)


_ALLOWED_PRIORITY_KEYS = frozenset({
    "carbon", "helium", "energy", "circularity", "biodiversity",
})


@dataclass(frozen=True)
class DigitalTwinConfig:
    """Frozen configuration for the digital twin simulation."""

    schema_version: int = SCHEMA_VERSION
    seed: int = 0

    # Core simulation
    time_horizon_years: int = 10
    time_step_days: int = 30
    n_simulations: int = 1000
    confidence_level: float = 0.95
    include_stochastic_events: bool = True
    parallel_simulations: int = 4
    expert_population_dynamics: bool = True
    material_flow_tracking: bool = True
    carbon_pricing_scenario: str = "linear_increase"
    helium_depletion_model: str = "exponential"

    # Enhanced features
    correlated_uncertainty: bool = True
    resource_substitution_enabled: bool = True
    user_priorities: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({
            "carbon": 0.25, "helium": 0.20, "energy": 0.15,
            "circularity": 0.20, "biodiversity": 0.20,
        }),
    )
    cache_max_size: int = 100

    # Retry and circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # Persistence
    persistence_path: str = "digital_twin_state.json.gz"
    atomic_writes: bool = True

    # Telemetry
    telemetry_export_interval: int = 60
    prometheus_port: Optional[int] = None
    latency_ring_size: int = 200

    # Correlation matrix override
    correlation_matrix_override: Optional[Mapping[str, Mapping[str, float]]] = None

    # Substitution model parameters
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

    resource_variances: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({
            "carbon": 0.02, "helium": 0.02, "energy": 0.01,
            "circularity": 0.01, "biodiversity": 0.01,
        }),
    )
    volatility_window_size: int = 10

    # Distillation parameters
    distillation_epsilon: float = 0.1
    distillation_train_every: int = 10
    distillation_replay_size: int = 2000
    distillation_learning_rate: float = 0.01
    distill_weight: float = 0.7
    rl_weight: float = 0.3

    # Subsystem flags
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

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        # Booleans
        for name in (
            "include_stochastic_events", "expert_population_dynamics",
            "material_flow_tracking", "correlated_uncertainty",
            "resource_substitution_enabled", "atomic_writes",
            "enable_limit_graph", "enable_modp_solver", "enable_rlhf",
            "enable_pso_tuning", "enable_moe_gating", "enable_ga_tuning",
        ):
            if not isinstance(getattr(self, name), bool):
                raise DigitalTwinConfigError(f"{name} must be a bool.")

        # Positive ints
        for name in (
            "time_horizon_years", "time_step_days", "n_simulations",
            "parallel_simulations", "cache_max_size",
            "circuit_breaker_threshold", "telemetry_export_interval",
            "latency_ring_size", "volatility_window_size",
            "distillation_train_every", "distillation_replay_size",
            "moe_expert_count", "pso_particles", "pso_iterations",
            "ga_population_size", "ga_generations",
            "substitution_ramp_start_step",
        ):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise DigitalTwinConfigError(
                    f"{name} must be a positive int (got {v!r})."
                )

        if not _is_real_int(self.max_retries) or self.max_retries < 0:
            raise DigitalTwinConfigError(
                "max_retries must be a non-negative int."
            )
        if not _is_real_int(self.seed) or self.seed < 0:
            raise DigitalTwinConfigError(
                "seed must be a non-negative int."
            )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinConfigError(
                "schema_version must be a positive int."
            )

        # Floats
        for name in (
            "retry_base_delay_ms", "retry_max_delay_ms",
            "circuit_breaker_recovery_timeout",
            "substitution_ramp_rate",
        ):
            if not _is_positive_finite(getattr(self, name)):
                raise DigitalTwinConfigError(
                    f"{name} must be a finite number > 0."
                )
        if self.retry_max_delay_ms < self.retry_base_delay_ms:
            raise DigitalTwinConfigError(
                "retry_max_delay_ms must be >= retry_base_delay_ms."
            )
        if not (0.0 <= self.confidence_level <= 1.0):
            raise DigitalTwinConfigError(
                "confidence_level must be in [0, 1]."
            )
        if not (0.0 <= self.distillation_epsilon <= 1.0):
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
                    f"{name} must be finite in [0, 1] (got {v!r})."
                )
        if abs(self.distill_weight + self.rl_weight - 1.0) > 1e-6:
            raise DigitalTwinConfigError(
                "distill_weight + rl_weight must equal 1.0."
            )

        if self.prometheus_port is not None:
            if not _is_real_int(self.prometheus_port) or self.prometheus_port < 1024:
                raise DigitalTwinConfigError(
                    "prometheus_port must be None or >= 1024."
                )

        if not isinstance(self.carbon_pricing_scenario, str) or not self.carbon_pricing_scenario:
            raise DigitalTwinConfigError(
                "carbon_pricing_scenario must be a non-empty string."
            )
        if not isinstance(self.helium_depletion_model, str) or not self.helium_depletion_model:
            raise DigitalTwinConfigError(
                "helium_depletion_model must be a non-empty string."
            )
        if not isinstance(self.persistence_path, str) or not self.persistence_path:
            raise DigitalTwinConfigError(
                "persistence_path must be a non-empty string."
            )

        # Frozen mappings
        object.__setattr__(
            self, "user_priorities",
            _freeze_str_float_map("user_priorities", self.user_priorities),
        )
        prio_sum = sum(self.user_priorities.values())
        if abs(prio_sum - 1.0) > 0.01:
            raise DigitalTwinConfigError(
                f"user_priorities must sum to ~1.0 (got {prio_sum:.4f})."
            )
        if not set(self.user_priorities).issubset(_ALLOWED_PRIORITY_KEYS):
            raise DigitalTwinConfigError(
                f"user_priorities keys must be a subset of "
                f"{sorted(_ALLOWED_PRIORITY_KEYS)}."
            )

        object.__setattr__(
            self, "substitution_availability_default",
            _freeze_str_float_map(
                "substitution_availability_default",
                self.substitution_availability_default,
            ),
        )
        object.__setattr__(
            self, "substitution_cost_factor_default",
            _freeze_str_float_map(
                "substitution_cost_factor_default",
                self.substitution_cost_factor_default,
            ),
        )
        object.__setattr__(
            self, "substitution_timeline_default",
            _freeze_str_float_map(
                "substitution_timeline_default",
                self.substitution_timeline_default,
            ),
        )
        object.__setattr__(
            self, "resource_variances",
            _freeze_str_float_map("resource_variances", self.resource_variances),
        )
        if self.correlation_matrix_override is not None:
            if not isinstance(self.correlation_matrix_override, Mapping):
                raise DigitalTwinConfigError(
                    "correlation_matrix_override must be a Mapping."
                )
            frozen_corr: Dict[str, Mapping[str, float]] = {}
            for row, cols in self.correlation_matrix_override.items():
                if not isinstance(row, str) or not row:
                    raise DigitalTwinConfigError(
                        "correlation_matrix_override row keys must be non-empty."
                    )
                frozen_corr[row] = _freeze_str_float_map(
                    f"correlation_matrix_override[{row!r}]", cols,
                )
            object.__setattr__(
                self, "correlation_matrix_override",
                MappingProxyType(frozen_corr),
            )

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "seed": self.seed,
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
            "resource_substitution_enabled": self.resource_substitution_enabled,
            "user_priorities": dict(self.user_priorities),
            "cache_max_size": self.cache_max_size,
            "max_retries": self.max_retries,
            "retry_base_delay_ms": self.retry_base_delay_ms,
            "retry_max_delay_ms": self.retry_max_delay_ms,
            "circuit_breaker_threshold": self.circuit_breaker_threshold,
            "circuit_breaker_recovery_timeout": self.circuit_breaker_recovery_timeout,
            "persistence_path": self.persistence_path,
            "atomic_writes": self.atomic_writes,
            "telemetry_export_interval": self.telemetry_export_interval,
            "prometheus_port": self.prometheus_port,
            "latency_ring_size": self.latency_ring_size,
            "correlation_matrix_override": (
                {k: dict(v) for k, v in self.correlation_matrix_override.items()}
                if self.correlation_matrix_override else None
            ),
            "substitution_availability_default": dict(self.substitution_availability_default),
            "substitution_cost_factor_default": dict(self.substitution_cost_factor_default),
            "substitution_timeline_default": dict(self.substitution_timeline_default),
            "substitution_ramp_start_step": self.substitution_ramp_start_step,
            "substitution_ramp_rate": self.substitution_ramp_rate,
            "resource_variances": dict(self.resource_variances),
            "volatility_window_size": self.volatility_window_size,
            "distillation_epsilon": self.distillation_epsilon,
            "distillation_train_every": self.distillation_train_every,
            "distillation_replay_size": self.distillation_replay_size,
            "distillation_learning_rate": self.distillation_learning_rate,
            "distill_weight": self.distill_weight,
            "rl_weight": self.rl_weight,
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

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = False,
    ) -> "DigitalTwinConfig":
        if not isinstance(data, Mapping):
            raise DigitalTwinConfigError(
                "DigitalTwinConfig.from_dict expects a Mapping."
            )
        valid = {f.name for f in fields(cls)}
        unknown = set(data) - valid
        if strict and unknown:
            raise DigitalTwinConfigError(
                f"Unknown config key(s): {sorted(unknown)}."
            )
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k in (
                "user_priorities",
                "substitution_availability_default",
                "substitution_cost_factor_default",
                "substitution_timeline_default",
                "resource_variances",
            ):
                if not isinstance(v, Mapping):
                    raise DigitalTwinConfigError(f"{k} must be a Mapping.")
                kwargs[k] = dict(v)
            elif k == "correlation_matrix_override":
                if v is not None and not isinstance(v, Mapping):
                    raise DigitalTwinConfigError(
                        "correlation_matrix_override must be a Mapping or None."
                    )
                kwargs[k] = (
                    {rk: dict(rv) for rk, rv in v.items()}
                    if v is not None else None
                )
            else:
                kwargs[k] = v
        try:
            return cls(**kwargs)
        except DigitalTwinConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise DigitalTwinConfigError(
                f"failed to build DigitalTwinConfig: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls, payload: str, *, strict: bool = False,
    ) -> "DigitalTwinConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise DigitalTwinConfigError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, Mapping):
            raise DigitalTwinConfigError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    @classmethod
    def from_env(
        cls,
        env: Optional[Mapping[str, str]] = None,
        *,
        strict: bool = False,
    ) -> "DigitalTwinConfig":
        e: Mapping[str, str] = env if env is not None else os.environ
        p = _ENV_PREFIX
        raw: Dict[str, Any] = {}

        def _raw(name: str) -> Optional[str]:
            v = e.get(name)
            if v is None:
                return None
            v = v.strip()
            return v or None

        for name, cast in (
            ("TIME_HORIZON_YEARS", int), ("TIME_STEP_DAYS", int),
            ("N_SIMULATIONS", int), ("PARALLEL_SIMULATIONS", int),
            ("MAX_RETRIES", int), ("CIRCUIT_BREAKER_THRESHOLD", int),
            ("TELEMETRY_EXPORT_INTERVAL", int), ("SEED", int),
        ):
            if (v := _raw(p + name)) is not None:
                try:
                    raw[name.lower()] = cast(v)
                except ValueError as exc:
                    raise DigitalTwinConfigError(
                        f"{p}{name}={v!r} is not a valid value."
                    ) from exc
        for name, cast in (
            ("CONFIDENCE_LEVEL", float),
            ("RETRY_BASE_DELAY_MS", float),
            ("RETRY_MAX_DELAY_MS", float),
            ("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", float),
            ("DISTILLATION_LEARNING_RATE", float),
            ("DISTILL_WEIGHT", float),
            ("RL_WEIGHT", float),
        ):
            if (v := _raw(p + name)) is not None:
                try:
                    raw[name.lower()] = cast(v)
                except ValueError as exc:
                    raise DigitalTwinConfigError(
                        f"{p}{name}={v!r} is not a valid value."
                    ) from exc
        if (v := _raw(p + "PERSISTENCE_PATH")) is not None:
            raw["persistence_path"] = v
        return cls.from_dict(raw, strict=strict)

    def with_overrides(self, **kwargs: Any) -> "DigitalTwinConfig":
        valid = {f.name for f in fields(self)}
        unknown = set(kwargs) - valid
        if unknown:
            raise DigitalTwinConfigError(
                f"Unknown config field(s): {sorted(unknown)}."
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
    def assert_compatible(
        cls, data: Mapping[str, Any], *, strict: bool = False,
    ) -> None:
        from .digital_twin_errors import DigitalTwinParseError
        if not isinstance(data, Mapping):
            raise DigitalTwinParseError(
                "assert_compatible expects a Mapping."
            )
        v = data.get("schema_version", SCHEMA_VERSION)
        if not _is_real_int(v) or v <= 0:
            raise DigitalTwinParseError(
                f"invalid schema_version {v!r}."
            )
        if v > SCHEMA_VERSION:
            raise DigitalTwinParseError(
                f"payload schema_version {v} is newer than {SCHEMA_VERSION}."
            )
        if strict and v < SCHEMA_VERSION:
            raise DigitalTwinParseError(
                f"payload schema_version {v} is older than {SCHEMA_VERSION}."
            )

    def __hash__(self) -> int:
        return hash((
            self.schema_version, self.seed,
            self.time_horizon_years, self.time_step_days,
            self.n_simulations, self.confidence_level,
            self.include_stochastic_events, self.parallel_simulations,
            self.expert_population_dynamics, self.material_flow_tracking,
            self.carbon_pricing_scenario, self.helium_depletion_model,
            self.correlated_uncertainty, self.resource_substitution_enabled,
            tuple(sorted(self.user_priorities.items())),
            self.cache_max_size, self.max_retries,
            self.retry_base_delay_ms, self.retry_max_delay_ms,
            self.circuit_breaker_threshold,
            self.circuit_breaker_recovery_timeout,
            self.persistence_path, self.atomic_writes,
            self.telemetry_export_interval, self.prometheus_port,
            self.latency_ring_size,
            tuple(sorted(self.substitution_availability_default.items())),
            tuple(sorted(self.substitution_cost_factor_default.items())),
            tuple(sorted(self.substitution_timeline_default.items())),
            self.substitution_ramp_start_step, self.substitution_ramp_rate,
            tuple(sorted(self.resource_variances.items())),
            self.volatility_window_size,
            self.distillation_epsilon, self.distillation_train_every,
            self.distillation_replay_size, self.distillation_learning_rate,
            self.distill_weight, self.rl_weight,
            self.enable_limit_graph, self.enable_modp_solver,
            self.enable_rlhf, self.enable_pso_tuning,
            self.enable_moe_gating, self.enable_ga_tuning,
            self.moe_expert_count, self.pso_particles, self.pso_iterations,
            self.ga_population_size, self.ga_generations,
        ))

    def __repr__(self) -> str:
        return (
            "DigitalTwinConfig("
            f"horizon={self.time_horizon_years}y, "
            f"step={self.time_step_days}d, "
            f"sims={self.n_simulations}, "
            f"seed={self.seed})"
        )


__all__ = ["DigitalTwinConfig", "SCHEMA_VERSION"]
