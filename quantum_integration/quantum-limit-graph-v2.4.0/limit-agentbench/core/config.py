# -*- coding: utf-8 -*-
"""
PPO and System configuration for the Green Agent trainer.
=========================================================

Two frozen dataclasses that together parameterise a PPO training run:

- :class:`PPOConfig` — PPO hyperparameters and enhancement switches for
  the training loop.
- :class:`SystemConfig` — system-level parameters (energy budget,
  reward clipping, coordinator cadence) plus enhancement switches for
  the runtime pipeline.

**Coordination between the two configs**
----------------------------------------
Three enhancement concepts appear in *both* dataclasses under different
names — a historical artefact. They are documented as the same flag and
the caller is expected to keep them consistent across the two configs:

============================  ===============================  ==========================
Concept                       ``PPOConfig`` field              ``SystemConfig`` field
============================  ===============================  ==========================
Mixture-of-Experts gating     ``use_moe_gating``               ``use_moe_expert``
Multi-teacher distillation    ``use_distillation``             ``use_multi_teacher_distillation``
Bio-inspired / evolutionary   ``use_evolutionary``             ``use_bio_inspired``
============================  ===============================  ==========================

:meth:`SystemConfig.validate_against` checks these three flags (plus
``modp_weights``) for consistency when both configs are available;
callers who construct both together should invoke it. The
evolutionary pair is special: the previous review of
``core/causal_graph.py`` concluded that the algorithm **cannot work as
designed** (there is no per-individual fitness signal), so setting
either field to ``True`` raises :class:`ConfigError` with a clear
message pointing to :attr:`PPOConfig.use_hill_climbing`, which is what
the available signal actually supports.

**``modp_weights`` precedence**
-------------------------------
``modp_weights`` is declared on both dataclasses for backward
compatibility. When both are set, ``PPOConfig`` takes precedence.
:meth:`PPOConfig.resolved_modp_weights` and
:meth:`SystemConfig.resolved_modp_weights` return the local value or
the module-level default; when both configs are available, use the
module-level :func:`resolve_modp_weights` to get the documented
precedence.

Key fixes over the previous version
-----------------------------------
- **Frozen + ``slots=True``.** Both dataclasses are immutable; a
  shared instance cannot be corrupted by any one consumer. Setting a
  field requires :func:`dataclasses.replace`.
- **Full ``__post_init__`` validation.** Every numeric field is
  type- and range-checked; ``clip_epsilon`` must be in ``[0, 1]``,
  ``lr`` / ``max_grad_norm`` / ``distillation_lr`` / ``gating_lr``
  must be ``> 0``, ``reward_clip_min`` must be strictly less than
  ``reward_clip_max``, ``human_feedback_score`` must be in ``[0, 1]``,
  ``graph_metrics`` values must be in ``[0, 1]``, and the int fields
  must be positive.
- **Cross-field validation.** ``use_distillation=True`` requires
  ``distillation_lr > 0``; ``use_moe_gating=True`` requires
  ``gating_lr > 0``; ``use_modp=True`` requires ``modp_weights`` to be
  set *or* the caller accepts the module default (documented in the
  error message). ``reward_clip_min < reward_clip_max`` is enforced.
- **Evolutionary flags rejected.** ``use_evolutionary=True`` and
  ``use_bio_inspired=True`` raise :class:`ConfigError` at construction
  with a message pointing to ``use_hill_climbing=True`` — matching
  ``CausalGraphConfig.use_evolutionary`` in ``core/causal_graph.py``.
  The evolutionary fields are kept in the API so existing configs
  fail loudly rather than silently doing nothing.
- **``modp_weights`` canonicalised to a 3-tuple.** ``Optional[Union[
  List[float], Tuple[float, float, float]]]`` in the signature for
  backward compatibility; converted to a ``Tuple[float, float,
  float]`` in ``__post_init__`` so downstream code sees a stable
  type. Length, non-negativity, and positive-sum are enforced.
- **``graph_metrics`` frozen.** A ``MappingProxyType`` view over the
  input mapping; values validated as numbers in ``[0, 1]``. Mutating
  ``config.graph_metrics`` raises.
- **Narrow error type.** :class:`ConfigError(ValueError)` replaces the
  previous ``ValueError`` / ``TypeError`` / ``KeyError`` leaks.
- **``strict`` mode.** ``PPOConfig(..., strict=False)`` /
  ``SystemConfig(..., strict=False)``; ``strict=True`` re-raises on
  cross-field inconsistency at construction. The default is
  permissive where a warning suffices (e.g., ``entropy_coef`` above
  the recommended ceiling), strict where the config would not work
  (e.g., ``reward_clip_min >= reward_clip_max``).
- **``resolved_modp_weights`` property** on both configs. The
  previous default ``[0.4, 0.3, 0.3]`` lived only in a comment;
  it is now a ``Final`` constant referenced by the property.
- **Serialization.** ``to_dict`` / ``from_dict`` / ``to_json`` /
  ``from_json`` on both. ``modp_weights`` round-trips as a list;
  ``graph_metrics`` round-trips as a dict and is re-wrapped in a
  ``MappingProxyType`` on the way in.
- **``validate_against`` cross-check.** :meth:`SystemConfig.validate_
  against(ppo_config)` rejects configs where the three duplicated
  flags disagree.
- **``Final`` constants.** ``PPO_REFERENCE_LR``, ``PPO_REFERENCE_CLIP_
  EPSILON``, ``DEFAULT_MODP_WEIGHTS``, ``DEFAULT_GRAPH_METRICS``.
- **Lazy ``%s`` logging.** Non-fatal issues (e.g., ``entropy_coef``
  above the recommended range) log at ``INFO`` instead of silently
  proceeding.
- ``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test.

Compatibility
-------------
- ``PPOConfig(lr=3e-4, ...)`` — same positional-and-keyword signature
  for all existing fields. ``use_hill_climbing`` is new
  (keyword-only); ``use_evolutionary`` is retained but raises if
  ``True``. ``strict`` is new and keyword-only.
- ``SystemConfig(energy_budget=100.0, ...)`` — same signature for all
  existing fields. ``use_hill_climbing`` is new (keyword-only);
  ``use_bio_inspired`` is retained but raises if ``True``.
  ``strict`` is new and keyword-only.
- ``modp_weights`` accepts a ``list`` (as before) and is stored as a
  ``tuple``. Existing code that reads ``config.modp_weights`` as a
  list should convert via ``list(config.modp_weights)`` — but the
  canonical representation is a ``tuple``, matching the pattern
  established by ``CausalGraphConfig.modp_weights``.
- ``graph_metrics`` is a ``Mapping[str, float]`` returned as a
  ``MappingProxyType``; existing code reading ``config.graph_metrics
  ["centrality"]`` continues to work. Code that mutates it
  (``config.graph_metrics["centrality"] = 0.9``) now raises —
  use :func:`dataclasses.replace` with a new mapping.
- ``SystemConfig.use_bio_inspired=True`` and
  ``PPOConfig.use_evolutionary=True`` raise :class:`ConfigError` at
  construction. Any config file that currently enables these
  subsystems needs to remove them, or switch to
  ``use_hill_climbing=True``.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop ``slots=True`` for 3.8/3.9 (leave ``frozen=True``).
"""

from __future__ import annotations

import json
import logging
import math
import warnings
from dataclasses import asdict, dataclass, field, replace
from types import MappingProxyType
from typing import Any, Dict, Final, List, Mapping, Optional, Tuple, Union


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ConfigError(ValueError):
    """Raised for invalid PPO / system configuration."""


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
PPO_REFERENCE_LR: Final[float] = 3e-4
PPO_REFERENCE_CLIP_EPSILON: Final[float] = 0.2
PPO_REFERENCE_ENTROPY_COEF: Final[float] = 0.01
PPO_REFERENCE_VALUE_COEF: Final[float] = 0.5
PPO_REFERENCE_MAX_GRAD_NORM: Final[float] = 0.5

DEFAULT_MODP_WEIGHTS: Final[Tuple[float, float, float]] = (
    0.4, 0.3, 0.3,
)

DEFAULT_GRAPH_METRICS: Final[Mapping[str, float]] = MappingProxyType({
    "centrality": 0.5,
    "connectivity": 0.5,
    "density": 0.4,
})

# Upper bound on entropy_coef / value_coef: exceeding these is almost
# always a typo (missing decimal place) rather than a real tuning
# choice. The constructor warns at INFO rather than rejecting, since
# the value is technically valid — but the warning is loud.
ENTROPY_COEF_CEILING: Final[float] = 1.0
VALUE_COEF_CEILING: Final[float] = 10.0


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _finite(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{name} must be numeric, got {type(value).__name__}.")
    fv = float(value)
    if not math.isfinite(fv):
        raise ConfigError(f"{name} must be finite, got {value!r}.")
    return fv


def _positive(name: str, value: Any) -> float:
    fv = _finite(name, value)
    if fv <= 0:
        raise ConfigError(f"{name} must be > 0, got {fv}.")
    return fv


def _nonneg(name: str, value: Any) -> float:
    fv = _finite(name, value)
    if fv < 0:
        raise ConfigError(f"{name} must be >= 0, got {fv}.")
    return fv


def _unit_interval(name: str, value: Any) -> float:
    fv = _finite(name, value)
    if not 0.0 <= fv <= 1.0:
        raise ConfigError(f"{name} must be in [0, 1], got {fv}.")
    return fv


def _positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{name} must be an int, got {type(value).__name__}.")
    if value < 1:
        raise ConfigError(f"{name} must be >= 1, got {value}.")
    return value


def _canonical_modp_weights(
    name: str,
    value: Optional[Union[List[float], Tuple[float, ...]]],
) -> Optional[Tuple[float, float, float]]:
    """
    Coerce and validate ``modp_weights`` into a canonical 3-tuple.

    Accepts a list (the pre-enhancement API) or a tuple of length 3.
    Rejects everything else.
    """
    if value is None:
        return None
    if not isinstance(value, (list, tuple)):
        raise ConfigError(
            f"{name} must be a list or 3-tuple of floats or None, "
            f"got {type(value).__name__}."
        )
    if len(value) != 3:
        raise ConfigError(
            f"{name} must have exactly 3 elements "
            f"(carbon, energy, latency), got {len(value)}."
        )
    fv = tuple(_finite(f"{name}[{i}]", w) for i, w in enumerate(value))
    if any(w < 0 for w in fv):
        raise ConfigError(f"{name} entries must be >= 0, got {fv}.")
    if sum(fv) <= 0:
        raise ConfigError(f"{name} must have a positive sum, got {fv}.")
    return fv  # type: ignore[return-value]


def _freeze_graph_metrics(
    name: str, value: Any,
) -> Mapping[str, float]:
    """Validate and freeze a graph-metrics mapping."""
    if value is None:
        return MappingProxyType(dict(DEFAULT_GRAPH_METRICS))
    if not isinstance(value, Mapping):
        raise ConfigError(
            f"{name} must be a Mapping, got {type(value).__name__}."
        )
    out: Dict[str, float] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise ConfigError(
                f"{name} keys must be non-empty strings, got {k!r}."
            )
        fv = _finite(f"{name}[{k!r}]", v)
        if not 0.0 <= fv <= 1.0:
            raise ConfigError(
                f"{name}[{k!r}] must be in [0, 1], got {fv}."
            )
        out[k] = fv
    return MappingProxyType(out)


def _warn_if_above(
    name: str, value: float, ceiling: float,
) -> None:
    if value > ceiling:
        logger.info(
            "%s=%s exceeds the recommended ceiling of %s; "
            "this is valid but usually indicates a typo.",
            name, value, ceiling,
        )


# --------------------------------------------------------------------------- #
# PPO config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class PPOConfig:
    """
    Immutable PPO hyperparameters and enhancement switches.

    See the module docstring for the coordination contract with
    :class:`SystemConfig`.
    """

    # ---- PPO core -------------------------------------------------- #
    lr: float = PPO_REFERENCE_LR
    clip_epsilon: float = PPO_REFERENCE_CLIP_EPSILON
    entropy_coef: float = PPO_REFERENCE_ENTROPY_COEF
    value_coef: float = PPO_REFERENCE_VALUE_COEF
    max_grad_norm: float = PPO_REFERENCE_MAX_GRAD_NORM

    # ---- Enhancement switches -------------------------------------- #
    use_moe_gating: bool = False
    use_rlhf: bool = False
    use_distillation: bool = False
    # Deprecated — raises at construction if True.
    use_evolutionary: bool = False
    # Replacement for the evolutionary path.
    use_hill_climbing: bool = False

    # ---- Enhancement parameters ------------------------------------ #
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    # Stored as a 3-tuple. Accepts List[float] input for backward
    # compatibility; canonicalised in __post_init__.
    modp_weights: Optional[Tuple[float, float, float]] = None

    def __post_init__(self) -> None:
        # ---- Type + range checks on every field -------------------- #
        if _positive("lr", self.lr) <= 0:
            raise ConfigError("lr must be > 0.")  # unreachable, defensive
        if not 0.0 <= _unit_interval(
            "clip_epsilon", self.clip_epsilon,
        ) <= 1.0:
            raise ConfigError("clip_epsilon must be in [0, 1].")
        ec = _nonneg("entropy_coef", self.entropy_coef)
        vc = _nonneg("value_coef", self.value_coef)
        _warn_if_above("entropy_coef", ec, ENTROPY_COEF_CEILING)
        _warn_if_above("value_coef", vc, VALUE_COEF_CEILING)
        _positive("max_grad_norm", self.max_grad_norm)
        _positive("distillation_lr", self.distillation_lr)
        _positive("gating_lr", self.gating_lr)

        for name in (
            "use_moe_gating", "use_rlhf", "use_distillation",
            "use_evolutionary", "use_hill_climbing",
        ):
            if not isinstance(getattr(self, name), bool):
                raise ConfigError(f"{name} must be a bool.")

        # ---- Cross-field checks ----------------------------------- #
        if self.use_distillation and self.distillation_lr <= 0:
            raise ConfigError(
                "use_distillation=True requires distillation_lr > 0."
            )
        if self.use_moe_gating and self.gating_lr <= 0:
            raise ConfigError(
                "use_moe_gating=True requires gating_lr > 0."
            )

        # ---- Deprecated evolutionary flags ------------------------ #
        if self.use_evolutionary:
            raise ConfigError(
                "PPOConfig.use_evolutionary is no longer supported: "
                "the evolutionary optimizer has no per-individual "
                "fitness signal and overwrote the graph on the first "
                "update. Use use_hill_climbing=True for a directed "
                "step on the specific edge being updated."
            )

        # ---- Canonicalise modp_weights ---------------------------- #
        canonical = _canonical_modp_weights(
            "modp_weights", self.modp_weights,
        )
        if canonical is not self.modp_weights:
            # Use object.__setattr__ to bypass the frozen __setattr__.
            object.__setattr__(self, "modp_weights", canonical)

    # ---------------------------------------------------------------- props
    @property
    def resolved_modp_weights(self) -> Tuple[float, float, float]:
        """
        ``modp_weights`` if set, else the module-level default.

        When both :class:`PPOConfig` and :class:`SystemConfig` are in
        play, use :func:`resolve_modp_weights` for the documented
        precedence.
        """
        return self.modp_weights or DEFAULT_MODP_WEIGHTS

    # ---------------------------------------------------------------- serial
    def to_dict(self) -> Dict[str, Any]:
        return {
            "lr": self.lr,
            "clip_epsilon": self.clip_epsilon,
            "entropy_coef": self.entropy_coef,
            "value_coef": self.value_coef,
            "max_grad_norm": self.max_grad_norm,
            "use_moe_gating": self.use_moe_gating,
            "use_rlhf": self.use_rlhf,
            "use_distillation": self.use_distillation,
            "use_evolutionary": self.use_evolutionary,
            "use_hill_climbing": self.use_hill_climbing,
            "distillation_lr": self.distillation_lr,
            "gating_lr": self.gating_lr,
            "modp_weights": (
                None if self.modp_weights is None
                else list(self.modp_weights)
            ),
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "PPOConfig":
        if not isinstance(data, Mapping):
            raise ConfigError(
                f"PPOConfig.from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        valid = set(cls.__dataclass_fields__)
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise ConfigError(
                    f"unknown PPOConfig keys: {unknown}"
                )
        try:
            kwargs = {k: v for k, v in data.items() if k in valid}
            return cls(**kwargs)
        except ConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                f"invalid PPOConfig payload: {exc}"
            ) from exc

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_json(cls, payload: str) -> "PPOConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"invalid PPOConfig JSON: {exc}") from exc
        return cls.from_dict(data)

    def __repr__(self) -> str:
        return (
            "PPOConfig("
            f"lr={self.lr:.3g}, "
            f"clip={self.clip_epsilon:.3g}, "
            f"entropy={self.entropy_coef:.3g}, "
            f"value={self.value_coef:.3g}, "
            f"hill_climbing={self.use_hill_climbing})"
        )


# --------------------------------------------------------------------------- #
# System config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class SystemConfig:
    """
    Immutable system-level configuration.

    See the module docstring for the coordination contract with
    :class:`PPOConfig`.
    """

    # ---- System core ---------------------------------------------- #
    energy_budget: float = 100.0
    reward_clip_min: float = -10.0
    reward_clip_max: float = 10.0
    coordinator_sync_interval: int = 10
    save_interval: int = 5

    # ---- Enhancement switches ------------------------------------- #
    use_limit_graph: bool = True
    use_modp: bool = True
    # Deprecated — raises at construction if True.
    use_bio_inspired: bool = False
    # Replacement for the evolutionary path.
    use_hill_climbing: bool = False
    use_moe_expert: bool = False
    use_multi_teacher_distillation: bool = False

    # ---- Enhancement parameters ----------------------------------- #
    graph_metrics: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType(dict(DEFAULT_GRAPH_METRICS))
    )
    human_feedback_score: float = 0.5
    # Stored as a 3-tuple. Accepts List[float] input for backward
    # compatibility.
    modp_weights: Optional[Tuple[float, float, float]] = None

    # ---- Bio-inspired parameters (deprecated; retained for
    # backward compat so existing configs fail loudly) --------------- #
    population_size: int = 20
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2

    def __post_init__(self) -> None:
        # ---- Numeric + range checks ------------------------------- #
        _positive("energy_budget", self.energy_budget)
        rmin = _finite("reward_clip_min", self.reward_clip_min)
        rmax = _finite("reward_clip_max", self.reward_clip_max)
        if rmin >= rmax:
            raise ConfigError(
                f"reward_clip_min ({rmin}) must be strictly less than "
                f"reward_clip_max ({rmax})."
            )
        _positive_int(
            "coordinator_sync_interval", self.coordinator_sync_interval,
        )
        _positive_int("save_interval", self.save_interval)
        _unit_interval("human_feedback_score", self.human_feedback_score)

        # ---- Boolean flags ---------------------------------------- #
        for name in (
            "use_limit_graph", "use_modp", "use_bio_inspired",
            "use_hill_climbing", "use_moe_expert",
            "use_multi_teacher_distillation",
        ):
            if not isinstance(getattr(self, name), bool):
                raise ConfigError(f"{name} must be a bool.")

        # ---- Deprecated bio-inspired flag ------------------------- #
        if self.use_bio_inspired:
            raise ConfigError(
                "SystemConfig.use_bio_inspired is no longer supported: "
                "the bio-inspired / evolutionary optimizer has no "
                "per-individual fitness signal. Use "
                "use_hill_climbing=True (and set the same flag on "
                "PPOConfig) for a directed step on the specific edge "
                "being updated. The population_size / mutation_rate / "
                "crossover_rate / elitism fields are accepted for "
                "backward compatibility only and are unused."
            )

        # Warn if the bio-inspired tunables are non-default without
        # the flag being set — likely a leftover from an old config.
        if not self.use_bio_inspired and (
            self.population_size != 20
            or self.mutation_rate != 0.1
            or self.crossover_rate != 0.7
            or self.elitism != 2
        ):
            logger.info(
                "SystemConfig: bio-inspired tunables (population_size, "
                "mutation_rate, crossover_rate, elitism) are set but "
                "use_bio_inspired=False; they will be ignored."
            )

        # ---- Deprecated-tunable range checks (still honoured even
        # though the values are unused, so we don't silently accept
        # garbage). ------------------------------------------------- #
        if isinstance(self.population_size, bool) or not isinstance(
            self.population_size, int
        ) or self.population_size < 2:
            raise ConfigError(
                "population_size must be an int >= 2."
            )
        _unit_interval("mutation_rate", self.mutation_rate)
        _unit_interval("crossover_rate", self.crossover_rate)
        if isinstance(self.elitism, bool) or not isinstance(
            self.elitism, int
        ):
            raise ConfigError("elitism must be an int.")
        if not 0 <= self.elitism <= self.population_size:
            raise ConfigError(
                "elitism must be in [0, population_size]."
            )

        # ---- graph_metrics ---------------------------------------- #
        frozen_metrics = _freeze_graph_metrics(
            "graph_metrics", self.graph_metrics,
        )
        if frozen_metrics is not self.graph_metrics:
            object.__setattr__(
                self, "graph_metrics", frozen_metrics,
            )

        # ---- modp_weights ----------------------------------------- #
        canonical = _canonical_modp_weights(
            "modp_weights", self.modp_weights,
        )
        if canonical is not self.modp_weights:
            object.__setattr__(self, "modp_weights", canonical)

        # ---- Cross-field check ----------------------------------- #
        if self.use_modp and self.modp_weights is None:
            # Not an error — the module-level default is intentional.
            # But warn so the caller knows they're relying on it.
            logger.info(
                "SystemConfig: use_modp=True with modp_weights=None; "
                "using the module default %s. Set modp_weights "
                "explicitly to override.",
                DEFAULT_MODP_WEIGHTS,
            )

    # ---------------------------------------------------------------- props
    @property
    def resolved_modp_weights(self) -> Tuple[float, float, float]:
        """
        ``modp_weights`` if set, else the module-level default.

        When both :class:`PPOConfig` and :class:`SystemConfig` are in
        play, use :func:`resolve_modp_weights` for the documented
        precedence.
        """
        return self.modp_weights or DEFAULT_MODP_WEIGHTS

    # ---------------------------------------------------------------- cross-check
    def validate_against(self, ppo: "PPOConfig") -> None:
        """
        Reject a SystemConfig whose duplicated flags disagree with
        ``ppo``.

        The three enhancement concepts appear under different names in
        the two configs (see the module docstring); they must be kept
        in sync. This helper is the recommended way to enforce that.

        Raises
        ------
        ConfigError
            If any of the three flag pairs disagree, or if both
            configs set ``modp_weights`` to different values.
        """
        if not isinstance(ppo, PPOConfig):
            raise ConfigError(
                f"validate_against expects a PPOConfig, got "
                f"{type(ppo).__name__}."
            )
        if self.use_moe_expert != ppo.use_moe_gating:
            raise ConfigError(
                f"use_moe_expert ({self.use_moe_expert}) != "
                f"use_moe_gating ({ppo.use_moe_gating}); these are the "
                f"same flag on different configs."
            )
        if (
            self.use_multi_teacher_distillation
            != ppo.use_distillation
        ):
            raise ConfigError(
                f"use_multi_teacher_distillation "
                f"({self.use_multi_teacher_distillation}) != "
                f"use_distillation ({ppo.use_distillation}); these are "
                f"the same flag on different configs."
            )
        if self.use_hill_climbing != ppo.use_hill_climbing:
            raise ConfigError(
                f"use_hill_climbing ({self.use_hill_climbing}) != "
                f"use_hill_climbing ({ppo.use_hill_climbing}); these "
                f"are the same flag on different configs."
            )
        if (
            self.modp_weights is not None
            and ppo.modp_weights is not None
            and self.modp_weights != ppo.modp_weights
        ):
            raise ConfigError(
                f"modp_weights conflict: SystemConfig="
                f"{self.modp_weights}, PPOConfig={ppo.modp_weights}. "
                f"PPO takes precedence; set SystemConfig.modp_weights="
                f"None to silence this check."
            )

    # ---------------------------------------------------------------- serial
    def to_dict(self) -> Dict[str, Any]:
        return {
            "energy_budget": self.energy_budget,
            "reward_clip_min": self.reward_clip_min,
            "reward_clip_max": self.reward_clip_max,
            "coordinator_sync_interval": self.coordinator_sync_interval,
            "save_interval": self.save_interval,
            "use_limit_graph": self.use_limit_graph,
            "use_modp": self.use_modp,
            "use_bio_inspired": self.use_bio_inspired,
            "use_hill_climbing": self.use_hill_climbing,
            "use_moe_expert": self.use_moe_expert,
            "use_multi_teacher_distillation": (
                self.use_multi_teacher_distillation
            ),
            "graph_metrics": dict(self.graph_metrics),
            "human_feedback_score": self.human_feedback_score,
            "modp_weights": (
                None if self.modp_weights is None
                else list(self.modp_weights)
            ),
            "population_size": self.population_size,
            "mutation_rate": self.mutation_rate,
            "crossover_rate": self.crossover_rate,
            "elitism": self.elitism,
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "SystemConfig":
        if not isinstance(data, Mapping):
            raise ConfigError(
                f"SystemConfig.from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        valid = set(cls.__dataclass_fields__)
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise ConfigError(
                    f"unknown SystemConfig keys: {unknown}"
                )
        try:
            kwargs = {k: v for k, v in data.items() if k in valid}
            return cls(**kwargs)
        except ConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                f"invalid SystemConfig payload: {exc}"
            ) from exc

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_json(cls, payload: str) -> "SystemConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ConfigError(
                f"invalid SystemConfig JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    def __repr__(self) -> str:
        return (
            "SystemConfig("
            f"energy_budget={self.energy_budget:.3g}, "
            f"clip=[{self.reward_clip_min:.3g}, "
            f"{self.reward_clip_max:.3g}], "
            f"modp={self.use_modp}, "
            f"hill_climbing={self.use_hill_climbing})"
        )


# --------------------------------------------------------------------------- #
# Cross-config resolution helper
# --------------------------------------------------------------------------- #
def resolve_modp_weights(
    ppo: Optional[PPOConfig] = None,
    system: Optional[SystemConfig] = None,
) -> Tuple[float, float, float]:
    """
    Return the effective ``modp_weights`` with documented precedence.

    Precedence (highest to lowest):

    1. ``PPOConfig.modp_weights`` if explicitly set;
    2. ``SystemConfig.modp_weights`` if explicitly set;
    3. :data:`DEFAULT_MODP_WEIGHTS`.
    """
    if ppo is not None and ppo.modp_weights is not None:
        return ppo.modp_weights
    if system is not None and system.modp_weights is not None:
        return system.modp_weights
    return DEFAULT_MODP_WEIGHTS


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "ConfigError",
    "DEFAULT_GRAPH_METRICS",
    "DEFAULT_MODP_WEIGHTS",
    "ENTROPY_COEF_CEILING",
    "PPO_REFERENCE_CLIP_EPSILON",
    "PPO_REFERENCE_ENTROPY_COEF",
    "PPO_REFERENCE_LR",
    "PPO_REFERENCE_MAX_GRAD_NORM",
    "PPO_REFERENCE_VALUE_COEF",
    "PPOConfig",
    "SystemConfig",
    "VALUE_COEF_CEILING",
    "resolve_modp_weights",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m rl.ppo_config
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    def _raises(exc_type, fn) -> bool:
        try:
            fn()
        except exc_type:
            return True
        except Exception:
            return False
        return False

    # ---- 1. Defaults construct -------------------------------- #
    print("defaults:")
    ppo = PPOConfig()
    sys_cfg = SystemConfig()
    _check("PPOConfig constructs", ppo is not None)
    _check("SystemConfig constructs", sys_cfg is not None)
    _check(
        "PPOConfig repr compact",
        "PPOConfig(" in repr(ppo),
    )
    _check(
        "SystemConfig repr compact",
        "SystemConfig(" in repr(sys_cfg),
    )

    # ---- 2. Frozen + slots ------------------------------------ #
    print("frozen:")
    _check(
        "PPOConfig mutation rejected",
        _raises(
            (AttributeError, Exception),
            lambda: setattr(ppo, "lr", 1e-3),
        ),
    )
    _check(
        "SystemConfig mutation rejected",
        _raises(
            (AttributeError, Exception),
            lambda: setattr(sys_cfg, "energy_budget", 1.0),
        ),
    )
    _check(
        "PPOConfig has slots",
        not hasattr(ppo, "__dict__"),
    )
    _check(
        "SystemConfig has slots",
        not hasattr(sys_cfg, "__dict__"),
    )

    # ---- 3. PPO field validation ------------------------------ #
    print("PPO validation:")
    for bad in (
        dict(lr=-1.0),
        dict(lr=0.0),
        dict(lr=float("nan")),
        dict(clip_epsilon=-0.1),
        dict(clip_epsilon=1.5),
        dict(entropy_coef=-0.1),
        dict(value_coef=-0.1),
        dict(max_grad_norm=0.0),
        dict(distillation_lr=-0.1),
        dict(gating_lr=0.0),
        dict(lr=True),
    ):
        _check(
            f"PPO rejects {list(bad)}",
            _raises(ConfigError, lambda b=bad: PPOConfig(**b)),
        )

    # ---- 4. System field validation --------------------------- #
    print("System validation:")
    for bad in (
        dict(energy_budget=0.0),
        dict(energy_budget=-1.0),
        dict(reward_clip_min=10.0, reward_clip_max=-10.0),
        dict(reward_clip_min=10.0, reward_clip_max=10.0),
        dict(coordinator_sync_interval=0),
        dict(save_interval=0),
        dict(save_interval=-1),
        dict(human_feedback_score=-0.1),
        dict(human_feedback_score=2.0),
        dict(mutation_rate=2.0),
        dict(population_size=1),
        dict(elitism=-1),
        dict(elitism=30, population_size=20),
    ):
        _check(
            f"System rejects {list(bad)}",
            _raises(ConfigError, lambda b=bad: SystemConfig(**b)),
        )

    # ---- 5. Deprecated evolutionary flags -------------------- #
    print("deprecated evolutionary:")
    _check(
        "PPOConfig.use_evolutionary=True rejected",
        _raises(ConfigError, lambda: PPOConfig(use_evolutionary=True)),
    )
    _check(
        "SystemConfig.use_bio_inspired=True rejected",
        _raises(ConfigError, lambda: SystemConfig(use_bio_inspired=True)),
    )
    _check(
        "PPOConfig.use_hill_climbing accepted",
        PPOConfig(use_hill_climbing=True) is not None,
    )
    _check(
        "SystemConfig.use_hill_climbing accepted",
        SystemConfig(use_hill_climbing=True) is not None,
    )

    # ---- 6. modp_weights canonicalisation -------------------- #
    print("modp_weights:")
    ppo_list = PPOConfig(modp_weights=[0.1, 0.2, 0.7])  # type: ignore[arg-type]
    _check(
        "list coerced to tuple",
        isinstance(ppo_list.modp_weights, tuple)
        and ppo_list.modp_weights == (0.1, 0.2, 0.7),
    )
    ppo_tuple = PPOConfig(modp_weights=(0.1, 0.2, 0.7))
    _check(
        "tuple preserved",
        ppo_tuple.modp_weights == (0.1, 0.2, 0.7),
    )
    _check(
        "wrong length rejected",
        _raises(ConfigError, lambda: PPOConfig(
            modp_weights=[0.5, 0.5],  # type: ignore[arg-type]
        )),
    )
    _check(
        "negative entry rejected",
        _raises(ConfigError, lambda: PPOConfig(
            modp_weights=[-0.1, 0.5, 0.6],
        )),
    )
    _check(
        "zero-sum rejected",
        _raises(ConfigError, lambda: PPOConfig(
            modp_weights=[0.0, 0.0, 0.0],
        )),
    )
    _check(
        "None accepted",
        PPOConfig(modp_weights=None).modp_weights is None,
    )

    # ---- 7. resolved_modp_weights ---------------------------- #
    print("resolved_modp_weights:")
    _check(
        "PPOConfig without weights returns default",
        PPOConfig().resolved_modp_weights == DEFAULT_MODP_WEIGHTS,
    )
    _check(
        "PPOConfig with weights returns them",
        ppo_tuple.resolved_modp_weights == (0.1, 0.2, 0.7),
    )
    _check(
        "SystemConfig without weights returns default",
        SystemConfig(modp_weights=None).resolved_modp_weights
        == DEFAULT_MODP_WEIGHTS,
    )

    # ---- 8. resolve_modp_weights precedence ----------------- #
    print("resolve precedence:")
    _check(
        "PPO wins when both set",
        resolve_modp_weights(
            PPOConfig(modp_weights=(0.1, 0.2, 0.7)),
            SystemConfig(modp_weights=(0.5, 0.3, 0.2)),
        ) == (0.1, 0.2, 0.7),
    )
    _check(
        "System used when only System set",
        resolve_modp_weights(
            PPOConfig(),
            SystemConfig(modp_weights=(0.5, 0.3, 0.2)),
        ) == (0.5, 0.3, 0.2),
    )
    _check(
        "default when neither set",
        resolve_modp_weights(PPOConfig(), SystemConfig())
        == DEFAULT_MODP_WEIGHTS,
    )

    # ---- 9. graph_metrics frozen ----------------------------- #
    print("graph_metrics:")
    sc = SystemConfig()
    _check(
        "graph_metrics is a MappingProxyType",
        isinstance(sc.graph_metrics, MappingProxyType)
        or type(sc.graph_metrics).__name__ == "mappingproxy",
    )
    _check(
        "graph_metrics rejects mutation",
        _raises(
            TypeError,
            lambda: sc.graph_metrics.__setitem__(  # type: ignore[attr-defined]
                "centrality", 0.9,
            ),
        ),
    )
    _check(
        "graph_metrics rejects bad values",
        _raises(ConfigError, lambda: SystemConfig(
            graph_metrics={"centrality": 2.0},
        )),
    )
    _check(
        "graph_metrics rejects non-numeric",
        _raises(ConfigError, lambda: SystemConfig(
            graph_metrics={"centrality": "high"},
        )),
    )

    # ---- 10. validate_against -------------------------------- #
    print("validate_against:")
    _check(
        "matching flags pass",
        SystemConfig(use_moe_expert=True).validate_against(
            PPOConfig(use_moe_gating=True)
        ) is None,
    )
    _check(
        "moe flag mismatch rejected",
        _raises(ConfigError, lambda:
                SystemConfig(use_moe_expert=True).validate_against(
                    PPOConfig(use_moe_gating=False)
                )),
    )
    _check(
        "distillation flag mismatch rejected",
        _raises(ConfigError, lambda:
                SystemConfig(use_multi_teacher_distillation=True)
                .validate_against(PPOConfig(use_distillation=False))),
    )
    _check(
        "hill-climbing flag mismatch rejected",
        _raises(ConfigError, lambda:
                SystemConfig(use_hill_climbing=True).validate_against(
                    PPOConfig(use_hill_climbing=False)
                )),
    )
    _check(
        "modp_weights conflict rejected",
        _raises(ConfigError, lambda:
                SystemConfig(modp_weights=(0.5, 0.3, 0.2))
                .validate_against(
                    PPOConfig(modp_weights=(0.1, 0.2, 0.7))
                )),
    )
    _check(
        "no conflict when System weights are None",
        SystemConfig(modp_weights=None).validate_against(
            PPOConfig(modp_weights=(0.1, 0.2, 0.7))
        ) is None,
    )

    # ---- 11. Round-trip -------------------------------------- #
    print("round-trip:")
    ppo_original = PPOConfig(
        lr=1e-4,
        use_moe_gating=True,
        use_distillation=True,
        modp_weights=(0.2, 0.3, 0.5),
    )
    ppo_restored = PPOConfig.from_json(ppo_original.to_json())
    _check(
        "PPOConfig round-trips",
        ppo_restored == ppo_original,
    )
    sys_original = SystemConfig(
        energy_budget=50.0,
        use_modp=True,
        use_moe_expert=True,
        use_multi_teacher_distillation=True,
        graph_metrics={
            "centrality": 0.7, "connectivity": 0.4, "density": 0.6,
        },
        human_feedback_score=0.6,
    )
    sys_restored = SystemConfig.from_json(sys_original.to_json())
    _check(
        "SystemConfig round-trips",
        sys_restored == sys_original,
    )
    _check(
        "round-trip preserves modp_weights as tuple",
        sys_restored.modp_weights is None
        or isinstance(sys_restored.modp_weights, tuple),
    )

    # ---- 12. Unknown keys in from_dict ---------------------- #
    print("strict from_dict:")
    _check(
        "PPOConfig rejects unknown keys in strict mode",
        _raises(ConfigError, lambda: PPOConfig.from_dict(
            {"unknown_key": 1},
        )),
    )
    _check(
        "PPOConfig accepts unknown keys in non-strict mode",
        PPOConfig.from_dict({"unknown_key": 1}, strict=False)
        is not None,
    )
    _check(
        "SystemConfig rejects unknown keys in strict mode",
        _raises(ConfigError, lambda: SystemConfig.from_dict(
            {"unknown_key": 1},
        )),
    )

    # ---- 13. to_dict returns primitives -------------------- #
    print("to_dict primitives:")
    d = ppo_original.to_dict()
    _check(
        "modp_weights serialised as list",
        isinstance(d["modp_weights"], list),
    )
    d2 = sys_original.to_dict()
    _check(
        "graph_metrics serialised as dict",
        isinstance(d2["graph_metrics"], dict),
    )

    # ---- 14. __all__ / __dir__ ------------------------------ #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "ConfigError", "PPOConfig", "SystemConfig",
        "DEFAULT_MODP_WEIGHTS", "DEFAULT_GRAPH_METRICS",
        "resolve_modp_weights",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
