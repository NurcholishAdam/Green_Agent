# src/quantum_integration/digital_twin/digital_twin_schemas.py

"""Enums and frozen record dataclasses for the digital twin."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .digital_twin_errors import (
    DigitalTwinInputError,
    DigitalTwinParseError,
)
from .digital_twin_helpers import (
    _deep_freeze,
    _hashable,
    _is_real_int,
    _parse_iso_datetime,
    _to_plain,
)

SCHEMA_VERSION: int = 1
DEFAULT_CONTAINER_TAG: str = "org:green-agent"
DEFAULT_TRUTH_LEVEL: str = "estimated"
DEFAULT_KIND: str = "digital_twin_result"


class SimulationScenario(str, Enum):
    POLICY_CHANGE = "policy_change"
    MARKET_SHOCK = "market_shock"
    RESOURCE_DEPLETION = "resource_depletion"
    TECHNOLOGY_ADOPTION = "technology_adoption"
    REGULATORY_CHANGE = "regulatory_change"
    CLIMATE_EVENT = "climate_event"
    POLICY_AND_TECHNOLOGY = "policy_and_technology"
    MARKET_AND_REGULATORY = "market_and_regulatory"
    RESOURCE_AND_CLIMATE = "resource_and_climate"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def values(cls) -> Tuple[str, ...]:
        return tuple(m.value for m in cls)

    @classmethod
    def coerce(cls, value: Any) -> "SimulationScenario":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(value)
            except ValueError as exc:
                raise DigitalTwinInputError(
                    f"scenario {value!r} is not one of {list(cls.values())}."
                ) from exc
        raise DigitalTwinInputError(
            f"scenario must be a str or SimulationScenario, got "
            f"{type(value).__name__}."
        )


@dataclass(frozen=True)
class DigitalTwinResult:
    """Immutable result of one scenario simulation."""

    scenario_id: str
    scenario_type: SimulationScenario
    observed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    metrics: Mapping[str, Any] = field(default_factory=dict)
    projections: Mapping[str, Tuple[float, ...]] = field(default_factory=dict)
    confidence_intervals: Mapping[str, Tuple[float, float]] = field(
        default_factory=dict,
    )
    risk_factors: Tuple[str, ...] = ()
    recommendations: Tuple[Mapping[str, Any], ...] = ()
    sustainability_score: float = 0.0
    interdependent_factors: Tuple[str, ...] = ()
    substitution_effects: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict,
    )
    weighted_score: float = 0.0
    strategy_used: str = "balanced"
    reward: float = 0.0
    container_tag: str = DEFAULT_CONTAINER_TAG
    truth_level: str = DEFAULT_TRUTH_LEVEL
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name in ("scenario_id", "strategy_used", "container_tag", "truth_level"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise DigitalTwinInputError(
                    f"{name} must be a non-empty string."
                )
        object.__setattr__(
            self, "scenario_type",
            SimulationScenario.coerce(self.scenario_type),
        )
        if not isinstance(self.observed_at, datetime):
            raise DigitalTwinInputError("observed_at must be a datetime.")
        if self.observed_at.tzinfo is None:
            object.__setattr__(
                self, "observed_at",
                self.observed_at.replace(tzinfo=timezone.utc),
            )
        for name in ("sustainability_score", "weighted_score", "reward"):
            v = getattr(self, name)
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                raise DigitalTwinInputError(f"{name} must be numeric.")
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinInputError("schema_version must be positive.")

        object.__setattr__(self, "metrics", _deep_freeze(dict(self.metrics)))
        object.__setattr__(
            self, "projections",
            _deep_freeze({k: tuple(v) for k, v in self.projections.items()}),
        )
        object.__setattr__(
            self, "confidence_intervals",
            _deep_freeze({
                k: (float(v[0]), float(v[1]))
                for k, v in self.confidence_intervals.items()
            }),
        )
        object.__setattr__(self, "risk_factors", tuple(str(r) for r in self.risk_factors))
        object.__setattr__(
            self, "recommendations",
            tuple(_deep_freeze(dict(r)) for r in self.recommendations),
        )
        object.__setattr__(
            self, "interdependent_factors",
            tuple(str(f) for f in self.interdependent_factors),
        )
        object.__setattr__(
            self, "substitution_effects",
            _deep_freeze({
                k: _to_plain(v) if isinstance(v, Mapping) else v
                for k, v in self.substitution_effects.items()
            }),
        )

    # ------------------------------------------------------------------ #
    @property
    def id(self) -> str:
        return self.scenario_id

    @property
    def kind(self) -> str:
        return DEFAULT_KIND

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "scenario_id": self.scenario_id,
            "scenario_type": self.scenario_type.value,
            "observed_at": self.observed_at.isoformat(),
            "metrics": _to_plain(self.metrics),
            "projections": {k: list(v) for k, v in self.projections.items()},
            "confidence_intervals": {
                k: list(v) for k, v in self.confidence_intervals.items()
            },
            "risk_factors": list(self.risk_factors),
            "recommendations": [_to_plain(r) for r in self.recommendations],
            "sustainability_score": self.sustainability_score,
            "interdependent_factors": list(self.interdependent_factors),
            "substitution_effects": _to_plain(self.substitution_effects),
            "weighted_score": self.weighted_score,
            "strategy_used": self.strategy_used,
            "reward": self.reward,
            "container_tag": self.container_tag,
            "truth_level": self.truth_level,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DigitalTwinResult":
        if not isinstance(data, Mapping):
            raise DigitalTwinParseError(
                "DigitalTwinResult.from_dict expects a Mapping."
            )
        try:
            scenario_id = data["scenario_id"]
            scenario_type = data["scenario_type"]
        except KeyError as exc:
            raise DigitalTwinParseError(
                f"DigitalTwinResult missing key {exc.args[0]!r}."
            ) from exc
        observed = (
            _parse_iso_datetime(data.get("observed_at"))
            or datetime.now(timezone.utc)
        )
        projections = {
            k: tuple(float(v) for v in vals)
            for k, vals in (data.get("projections") or {}).items()
        }
        intervals = {
            k: (float(vals[0]), float(vals[1]))
            for k, vals in (data.get("confidence_intervals") or {}).items()
        }
        return cls(
            scenario_id=str(scenario_id),
            scenario_type=scenario_type,
            observed_at=observed,
            metrics=dict(data.get("metrics") or {}),
            projections=projections,
            confidence_intervals=intervals,
            risk_factors=tuple(data.get("risk_factors") or ()),
            recommendations=tuple(data.get("recommendations") or ()),
            sustainability_score=float(data.get("sustainability_score", 0.0)),
            interdependent_factors=tuple(data.get("interdependent_factors") or ()),
            substitution_effects=dict(data.get("substitution_effects") or {}),
            weighted_score=float(data.get("weighted_score", 0.0)),
            strategy_used=str(data.get("strategy_used", "balanced")),
            reward=float(data.get("reward", 0.0)),
            container_tag=str(data.get("container_tag", DEFAULT_CONTAINER_TAG)),
            truth_level=str(data.get("truth_level", DEFAULT_TRUTH_LEVEL)),
            schema_version=int(data.get("schema_version", SCHEMA_VERSION)),
        )

    @classmethod
    def from_json(cls, payload: str) -> "DigitalTwinResult":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise DigitalTwinParseError(
                f"DigitalTwinResult.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    def _render_content(self) -> str:
        return (
            f"Digital Twin result {self.scenario_id} "
            f"({self.scenario_type.value}) — strategy={self.strategy_used}, "
            f"sustainability={self.sustainability_score:.3f}, "
            f"weighted={self.weighted_score:.3f}, reward={self.reward:+.3f}"
        )

    def _metadata(self, *, container_tag: str) -> Dict[str, Any]:
        return {
            "kind": DEFAULT_KIND,
            "type": DEFAULT_KIND,
            "record_id": self.id,
            "scenario_id": self.scenario_id,
            "scenario_type": self.scenario_type.value,
            "strategy_used": self.strategy_used,
            "sustainability_score": self.sustainability_score,
            "weighted_score": self.weighted_score,
            "reward": self.reward,
            "truth_level": self.truth_level,
            "container_tag": container_tag,
            "observed_at": self.observed_at.isoformat(),
            "schema_version": self.schema_version,
            "risk_factors": list(self.risk_factors),
        }

    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
        truth_level: Optional[str] = None,
    ) -> Dict[str, Any]:
        tag = container_tag or self.container_tag
        if not isinstance(tag, str) or not tag:
            raise DigitalTwinInputError(
                "container_tag must be a non-empty string."
            )
        meta = self._metadata(container_tag=tag)
        if truth_level is not None:
            if not isinstance(truth_level, str) or not truth_level:
                raise DigitalTwinInputError(
                    "truth_level must be None or a non-empty string."
                )
            meta["truth_level"] = truth_level
        return {
            "content": content if content is not None else self._render_content(),
            "container_tag": tag,
            "metadata": meta,
        }

    def to_memory_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "content": self._render_content(),
            "metadata": self._metadata(container_tag=self.container_tag),
        }

    def __hash__(self) -> int:
        return hash((
            self.scenario_id, self.scenario_type, self.observed_at,
            _hashable(self.metrics),
            _hashable(self.projections),
            _hashable(self.confidence_intervals),
            self.risk_factors, self.recommendations,
            self.sustainability_score, self.interdependent_factors,
            _hashable(self.substitution_effects), self.weighted_score,
            self.strategy_used, self.reward, self.container_tag,
            self.truth_level, self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"DigitalTwinResult(id={self.scenario_id!r}, "
            f"type={self.scenario_type.value!r}, "
            f"score={self.sustainability_score:.3f})"
        )


@dataclass(frozen=True)
class ResourceProjection:
    """Immutable projection of a resource level over time."""

    resource_type: str
    current_level: float
    projected_levels: Tuple[float, ...]
    depletion_year: Optional[int] = None
    confidence_lower: Tuple[float, ...] = ()
    confidence_upper: Tuple[float, ...] = ()
    substitution_availability: float = 0.0
    substitution_cost_factor: float = 1.0
    substitution_timeline: Tuple[float, ...] = ()
    alternative_resources: Tuple[str, ...] = ()
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.resource_type, str) or not self.resource_type:
            raise DigitalTwinInputError(
                "resource_type must be a non-empty string."
            )
        if not isinstance(self.projected_levels, tuple):
            object.__setattr__(
                self, "projected_levels",
                tuple(float(x) for x in self.projected_levels),
            )
        if self.confidence_lower and not isinstance(self.confidence_lower, tuple):
            object.__setattr__(
                self, "confidence_lower",
                tuple(float(x) for x in self.confidence_lower),
            )
        if self.confidence_upper and not isinstance(self.confidence_upper, tuple):
            object.__setattr__(
                self, "confidence_upper",
                tuple(float(x) for x in self.confidence_upper),
            )
        if self.substitution_timeline and not isinstance(
            self.substitution_timeline, tuple,
        ):
            object.__setattr__(
                self, "substitution_timeline",
                tuple(float(x) for x in self.substitution_timeline),
            )
        if not isinstance(self.alternative_resources, tuple):
            object.__setattr__(
                self, "alternative_resources",
                tuple(str(x) for x in self.alternative_resources),
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "resource_type": self.resource_type,
            "current_level": self.current_level,
            "projected_levels": list(self.projected_levels),
            "depletion_year": self.depletion_year,
            "confidence_lower": list(self.confidence_lower),
            "confidence_upper": list(self.confidence_upper),
            "substitution_availability": self.substitution_availability,
            "substitution_cost_factor": self.substitution_cost_factor,
            "substitution_timeline": list(self.substitution_timeline),
            "alternative_resources": list(self.alternative_resources),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ResourceProjection":
        if not isinstance(data, Mapping):
            raise DigitalTwinParseError(
                "ResourceProjection.from_dict expects a Mapping."
            )
        return cls(
            resource_type=str(data["resource_type"]),
            current_level=float(data.get("current_level", 0.0)),
            projected_levels=tuple(
                float(x) for x in (data.get("projected_levels") or ())
            ),
            depletion_year=data.get("depletion_year"),
            confidence_lower=tuple(
                float(x) for x in (data.get("confidence_lower") or ())
            ),
            confidence_upper=tuple(
                float(x) for x in (data.get("confidence_upper") or ())
            ),
            substitution_availability=float(
                data.get("substitution_availability", 0.0)
            ),
            substitution_cost_factor=float(
                data.get("substitution_cost_factor", 1.0)
            ),
            substitution_timeline=tuple(
                float(x) for x in (data.get("substitution_timeline") or ())
            ),
            alternative_resources=tuple(
                data.get("alternative_resources") or ()
            ),
            schema_version=int(data.get("schema_version", SCHEMA_VERSION)),
        )

    def __hash__(self) -> int:
        return hash((
            self.resource_type, self.current_level, self.projected_levels,
            self.depletion_year, self.confidence_lower, self.confidence_upper,
            self.substitution_availability, self.substitution_cost_factor,
            self.substitution_timeline, self.alternative_resources,
            self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"ResourceProjection(type={self.resource_type!r}, "
            f"level={self.current_level:.3f}, "
            f"depletion_year={self.depletion_year})"
        )


__all__ = [
    "DEFAULT_CONTAINER_TAG",
    "DEFAULT_KIND",
    "DEFAULT_TRUTH_LEVEL",
    "SCHEMA_VERSION",
    "DigitalTwinResult",
    "ResourceProjection",
    "SimulationScenario",
]
