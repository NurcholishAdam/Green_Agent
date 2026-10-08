# src/quantum_integration/digital_twin/digital_twin_schemas.py

"""Enums and frozen record dataclasses for the digital twin.

Records are immutable once constructed. Mappings and sequences are
recursively frozen into ``MappingProxyType`` / tuples so callers cannot
mutate the record's internal state. ``__hash__`` uses ``_hashable`` to
project frozen mappings into deterministic hashable tuples.

Numeric validation
------------------
All numeric fields are validated to be finite (``bool`` rejected,
``NaN`` / ``inf`` rejected). Structural fields are validated: mappings
must be mappings, ``confidence_intervals`` values must be length-2
finite pairs, and ``recommendations`` entries must be mappings.

Schema versions
---------------
Payloads carrying a ``schema_version`` newer than
:data:`SCHEMA_VERSION` are rejected by ``from_dict`` / ``from_json``.
Older payloads are accepted because no field has been added or removed
between v1 and v2; v2 only adds stricter validation.

Parsing
-------
``from_dict`` / ``from_json`` accept ``strict=True`` to raise on
unparseable ``observed_at`` values. In non-strict mode, unparseable
timestamps are logged and replaced with the current time.
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

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

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 2
DEFAULT_CONTAINER_TAG: str = "org:green-agent"
DEFAULT_TRUTH_LEVEL: str = "estimated"
DEFAULT_KIND: str = "digital_twin_result"


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #

def _is_finite_number(value: Any) -> bool:
    """Return ``True`` iff ``value`` is a finite ``int``/``float``.

    ``bool`` is rejected because it is a subclass of ``int``.
    """
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _require_finite_float(name: str, value: Any) -> float:
    """Return ``value`` as a finite ``float``, else raise."""
    if not _is_finite_number(value):
        raise DigitalTwinInputError(f"{name} must be a finite number.")
    return float(value)


def _require_nonempty_str(name: str, value: Any) -> str:
    """Return ``value`` if it is a non-empty string, else raise."""
    if not isinstance(value, str) or not value:
        raise DigitalTwinInputError(f"{name} must be a non-empty string.")
    return value


def _require_mapping(name: str, value: Any) -> Mapping[str, Any]:
    """Return ``value`` if it is a Mapping, else raise."""
    if not isinstance(value, ABCMapping):
        raise DigitalTwinInputError(f"{name} must be a Mapping.")
    return value


def _coerce_finite_tuple(name: str, values: Any) -> Tuple[float, ...]:
    """Return a tuple of finite floats from ``values``.

    Accepts any iterable (except ``str``/``bytes``, which would iterate
    characters/bytes). Values are validated individually and the index
    of the offending entry is included in the error message.
    """
    if values is None:
        return ()
    if isinstance(values, (str, bytes)):
        raise DigitalTwinInputError(
            f"{name} must be a sequence, not {type(values).__name__}."
        )
    try:
        items = list(values)
    except TypeError as exc:
        raise DigitalTwinInputError(
            f"{name} must be a sequence of finite numbers."
        ) from exc
    return tuple(
        _require_finite_float(f"{name}[{i}]", v)
        for i, v in enumerate(items)
    )


def _coerce_confidence_pair(name: str, value: Any) -> Tuple[float, float]:
    """Return a length-2 tuple of finite floats from ``value``."""
    if isinstance(value, (str, bytes)) or not hasattr(value, "__len__"):
        raise DigitalTwinInputError(
            f"{name} must be a 2-element sequence of finite numbers."
        )
    try:
        n = len(value)
    except TypeError as exc:
        raise DigitalTwinInputError(
            f"{name} must be a 2-element sequence of finite numbers."
        ) from exc
    if n != 2:
        raise DigitalTwinInputError(
            f"{name} must have exactly 2 elements, got {n}."
        )
    lo = _require_finite_float(f"{name}[0]", value[0])
    hi = _require_finite_float(f"{name}[1]", value[1])
    return (lo, hi)


def _check_schema_version(data: Mapping[str, Any]) -> int:
    """Return the declared ``schema_version``, rejecting newer payloads.

    Raises :class:`DigitalTwinParseError` on missing/invalid versions
    that are non-numeric or newer than :data:`SCHEMA_VERSION`.
    """
    raw = data.get("schema_version", SCHEMA_VERSION)
    if isinstance(raw, bool):
        raise DigitalTwinParseError(
            f"schema_version must be an integer, got {raw!r}."
        )
    try:
        v = int(raw)
    except (TypeError, ValueError) as exc:
        raise DigitalTwinParseError(
            f"schema_version must be an integer, got {raw!r}."
        ) from exc
    if v <= 0:
        raise DigitalTwinParseError(
            f"schema_version must be positive, got {v}."
        )
    if v > SCHEMA_VERSION:
        raise DigitalTwinParseError(
            f"schema_version {v} is newer than supported "
            f"({SCHEMA_VERSION})."
        )
    return v


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #

class SimulationScenario(str, Enum):
    """Enumerated simulation scenarios."""

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

    def __repr__(self) -> str:
        return f"SimulationScenario.{self.name}"

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
                    f"scenario {value!r} is not one of "
                    f"{list(cls.values())}."
                ) from exc
        raise DigitalTwinInputError(
            f"scenario must be a str or SimulationScenario, got "
            f"{type(value).__name__}."
        )


# --------------------------------------------------------------------------- #
# DigitalTwinResult
# --------------------------------------------------------------------------- #

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
        # ---- String fields -------------------------------------------- #
        for name in (
            "scenario_id", "strategy_used", "container_tag", "truth_level",
        ):
            _require_nonempty_str(name, getattr(self, name))

        # ---- Enum coercion -------------------------------------------- #
        object.__setattr__(
            self, "scenario_type",
            SimulationScenario.coerce(self.scenario_type),
        )

        # ---- Timestamp normalization ---------------------------------- #
        if not isinstance(self.observed_at, datetime):
            raise DigitalTwinInputError("observed_at must be a datetime.")
        if self.observed_at.tzinfo is None:
            object.__setattr__(
                self, "observed_at",
                self.observed_at.replace(tzinfo=timezone.utc),
            )

        # ---- Numeric fields (finite) ---------------------------------- #
        object.__setattr__(
            self, "sustainability_score",
            _require_finite_float(
                "sustainability_score", self.sustainability_score,
            ),
        )
        object.__setattr__(
            self, "weighted_score",
            _require_finite_float("weighted_score", self.weighted_score),
        )
        object.__setattr__(
            self, "reward",
            _require_finite_float("reward", self.reward),
        )

        # ---- Schema version ------------------------------------------- #
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinInputError(
                "schema_version must be a positive integer."
            )

        # ---- metrics: mapping ----------------------------------------- #
        _require_mapping("metrics", self.metrics)
        object.__setattr__(
            self, "metrics", _deep_freeze(dict(self.metrics)),
        )

        # ---- projections: mapping[str, tuple[float, ...]] ------------- #
        _require_mapping("projections", self.projections)
        proj_frozen: Dict[str, Tuple[float, ...]] = {}
        for k, v in self.projections.items():
            proj_frozen[str(k)] = _coerce_finite_tuple(
                f"projections[{k!r}]", v,
            )
        object.__setattr__(
            self, "projections", _deep_freeze(proj_frozen),
        )

        # ---- confidence_intervals: mapping[str, (float, float)] ------- #
        _require_mapping(
            "confidence_intervals", self.confidence_intervals,
        )
        ci_frozen: Dict[str, Tuple[float, float]] = {}
        for k, v in self.confidence_intervals.items():
            ci_frozen[str(k)] = _coerce_confidence_pair(
                f"confidence_intervals[{k!r}]", v,
            )
        object.__setattr__(
            self, "confidence_intervals", _deep_freeze(ci_frozen),
        )

        # ---- risk_factors: tuple[str, ...] ---------------------------- #
        object.__setattr__(
            self, "risk_factors",
            tuple(str(r) for r in self.risk_factors),
        )

        # ---- recommendations: tuple[Mapping, ...] --------------------- #
        recs: List[Mapping[str, Any]] = []
        for i, r in enumerate(self.recommendations):
            if not isinstance(r, ABCMapping):
                raise DigitalTwinInputError(
                    f"recommendations[{i}] must be a Mapping."
                )
            recs.append(_deep_freeze(dict(r)))
        object.__setattr__(self, "recommendations", tuple(recs))

        # ---- interdependent_factors: tuple[str, ...] ------------------ #
        object.__setattr__(
            self, "interdependent_factors",
            tuple(str(f) for f in self.interdependent_factors),
        )

        # ---- substitution_effects: mapping[str, mapping] -------------- #
        _require_mapping(
            "substitution_effects", self.substitution_effects,
        )
        object.__setattr__(
            self, "substitution_effects",
            _deep_freeze(dict(self.substitution_effects)),
        )

    # -- read-only projections ------------------------------------------ #

    @property
    def id(self) -> str:
        return self.scenario_id

    @property
    def kind(self) -> str:
        return DEFAULT_KIND

    # -- serialization -------------------------------------------------- #

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
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> "DigitalTwinResult":
        """Rebuild a result from a dict payload.

        Parameters
        ----------
        data:
            The mapping produced by :meth:`to_dict` (or compatible).
        strict:
            When ``True``, an unparseable ``observed_at`` raises
            :class:`DigitalTwinParseError`. When ``False`` (default),
            unparseable timestamps are logged and replaced with the
            current UTC time.

        Raises
        ------
        DigitalTwinParseError
            On missing required keys, schema version mismatch, or
            malformed payload.
        """
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "DigitalTwinResult.from_dict expects a Mapping."
            )

        # Schema-version guard (rejects newer payloads).
        v = _check_schema_version(data)

        try:
            scenario_id = data["scenario_id"]
            scenario_type = data["scenario_type"]
        except KeyError as exc:
            raise DigitalTwinParseError(
                f"DigitalTwinResult missing key {exc.args[0]!r}."
            ) from exc

        # Timestamp: strict raises, non-strict warns and substitutes.
        raw_observed = data.get("observed_at")
        observed = _parse_iso_datetime(raw_observed)
        if observed is None:
            if raw_observed is not None:
                if strict:
                    raise DigitalTwinParseError(
                        f"invalid observed_at: {raw_observed!r}."
                    )
                logger.warning(
                    "DigitalTwinResult.from_dict: unparseable "
                    "observed_at %r; substituting current time.",
                    raw_observed,
                )
            observed = datetime.now(timezone.utc)

        # Scenario type: convert input error into parse error.
        try:
            scenario_type_enum = SimulationScenario.coerce(scenario_type)
        except DigitalTwinInputError as exc:
            raise DigitalTwinParseError(str(exc)) from exc

        # Projections: per-key error context.
        projections: Dict[str, Tuple[float, ...]] = {}
        raw_proj = data.get("projections") or {}
        if not isinstance(raw_proj, ABCMapping):
            raise DigitalTwinParseError(
                "projections must be a Mapping."
            )
        for k, vals in raw_proj.items():
            try:
                projections[str(k)] = _coerce_finite_tuple(
                    f"projections[{k!r}]", vals,
                )
            except DigitalTwinInputError as exc:
                raise DigitalTwinParseError(str(exc)) from exc

        # Confidence intervals: per-key error context.
        intervals: Dict[str, Tuple[float, float]] = {}
        raw_ci = data.get("confidence_intervals") or {}
        if not isinstance(raw_ci, ABCMapping):
            raise DigitalTwinParseError(
                "confidence_intervals must be a Mapping."
            )
        for k, vals in raw_ci.items():
            try:
                intervals[str(k)] = _coerce_confidence_pair(
                    f"confidence_intervals[{k!r}]", vals,
                )
            except DigitalTwinInputError as exc:
                raise DigitalTwinParseError(str(exc)) from exc

        # Construct; wrap input errors as parse errors.
        try:
            return cls(
                scenario_id=str(scenario_id),
                scenario_type=scenario_type_enum,
                observed_at=observed,
                metrics=dict(data.get("metrics") or {}),
                projections=projections,
                confidence_intervals=intervals,
                risk_factors=tuple(data.get("risk_factors") or ()),
                recommendations=tuple(data.get("recommendations") or ()),
                sustainability_score=float(
                    data.get("sustainability_score", 0.0)
                ),
                interdependent_factors=tuple(
                    data.get("interdependent_factors") or ()
                ),
                substitution_effects=dict(
                    data.get("substitution_effects") or {}
                ),
                weighted_score=float(data.get("weighted_score", 0.0)),
                strategy_used=str(data.get("strategy_used", "balanced")),
                reward=float(data.get("reward", 0.0)),
                container_tag=str(
                    data.get("container_tag", DEFAULT_CONTAINER_TAG)
                ),
                truth_level=str(
                    data.get("truth_level", DEFAULT_TRUTH_LEVEL)
                ),
                schema_version=v,
            )
        except DigitalTwinInputError as exc:
            raise DigitalTwinParseError(str(exc)) from exc
        except (TypeError, ValueError) as exc:
            raise DigitalTwinParseError(str(exc)) from exc

    @classmethod
    def from_json(
        cls, payload: str, *, strict: bool = False,
    ) -> "DigitalTwinResult":
        """Parse a JSON payload. See :meth:`from_dict` for ``strict``."""
        if not isinstance(payload, str):
            raise DigitalTwinParseError(
                "DigitalTwinResult.from_json expects a string."
            )
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise DigitalTwinParseError(
                f"DigitalTwinResult.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data, strict=strict)

    # -- rendering / metadata ------------------------------------------ #

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
        """Return an episode-shaped payload for downstream consumers.

        ``container_tag`` overrides ``self.container_tag`` in the emitted
        metadata; the record itself is unchanged. ``truth_level``
        overrides the value in the emitted metadata only.
        """
        tag = container_tag or self.container_tag
        _require_nonempty_str("container_tag", tag)
        meta = self._metadata(container_tag=tag)
        if truth_level is not None:
            _require_nonempty_str("truth_level", truth_level)
            meta["truth_level"] = truth_level
        return {
            "content": (
                content if content is not None else self._render_content()
            ),
            "container_tag": tag,
            "metadata": meta,
        }

    def to_memory_dict(self) -> Dict[str, Any]:
        """Return a memory-store-shaped payload."""
        return {
            "id": self.id,
            "content": self._render_content(),
            "metadata": self._metadata(container_tag=self.container_tag),
        }

    # -- hash / repr ---------------------------------------------------- #

    def __hash__(self) -> int:
        # Frozen mappings are MappingProxyType and therefore unhashable;
        # ``_hashable`` projects them into deterministic hashable tuples.
        # Same for each recommendation entry.
        return hash((
            self.scenario_id,
            self.scenario_type,
            self.observed_at,
            _hashable(self.metrics),
            _hashable(self.projections),
            _hashable(self.confidence_intervals),
            self.risk_factors,
            tuple(_hashable(r) for r in self.recommendations),
            self.sustainability_score,
            self.interdependent_factors,
            _hashable(self.substitution_effects),
            self.weighted_score,
            self.strategy_used,
            self.reward,
            self.container_tag,
            self.truth_level,
            self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"DigitalTwinResult(id={self.scenario_id!r}, "
            f"type={self.scenario_type.value!r}, "
            f"strategy={self.strategy_used!r}, "
            f"score={self.sustainability_score:.3f})"
        )


# --------------------------------------------------------------------------- #
# ResourceProjection
# --------------------------------------------------------------------------- #

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
        # ---- String field --------------------------------------------- #
        if not isinstance(self.resource_type, str) or not self.resource_type:
            raise DigitalTwinInputError(
                "resource_type must be a non-empty string."
            )

        # ---- Numeric fields (finite) ---------------------------------- #
        object.__setattr__(
            self, "current_level",
            _require_finite_float("current_level", self.current_level),
        )
        object.__setattr__(
            self, "substitution_availability",
            _require_finite_float(
                "substitution_availability",
                self.substitution_availability,
            ),
        )
        object.__setattr__(
            self, "substitution_cost_factor",
            _require_finite_float(
                "substitution_cost_factor",
                self.substitution_cost_factor,
            ),
        )

        # ---- depletion_year: int or None ------------------------------ #
        if self.depletion_year is not None:
            if isinstance(self.depletion_year, bool) or not isinstance(
                self.depletion_year, int,
            ):
                raise DigitalTwinInputError(
                    "depletion_year must be an int or None."
                )

        # ---- Sequence fields (finite-float tuples) -------------------- #
        projected = _coerce_finite_tuple(
            "projected_levels", self.projected_levels,
        )
        lower = _coerce_finite_tuple(
            "confidence_lower", self.confidence_lower,
        )
        upper = _coerce_finite_tuple(
            "confidence_upper", self.confidence_upper,
        )
        timeline = _coerce_finite_tuple(
            "substitution_timeline", self.substitution_timeline,
        )
        object.__setattr__(self, "projected_levels", projected)
        object.__setattr__(self, "confidence_lower", lower)
        object.__setattr__(self, "confidence_upper", upper)
        object.__setattr__(self, "substitution_timeline", timeline)

        # ---- Cross-length checks -------------------------------------- #
        if lower and len(lower) != len(projected):
            raise DigitalTwinInputError(
                f"confidence_lower length {len(lower)} must match "
                f"projected_levels length {len(projected)}."
            )
        if upper and len(upper) != len(projected):
            raise DigitalTwinInputError(
                f"confidence_upper length {len(upper)} must match "
                f"projected_levels length {len(projected)}."
            )

        # ---- alternative_resources: tuple[str, ...] ------------------- #
        object.__setattr__(
            self, "alternative_resources",
            tuple(str(x) for x in self.alternative_resources),
        )

        # ---- Schema version ------------------------------------------- #
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise DigitalTwinInputError(
                "schema_version must be a positive integer."
            )

    # -- serialization -------------------------------------------------- #

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

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> "ResourceProjection":
        """Rebuild from a dict payload.

        ``strict`` is accepted for API symmetry with
        :meth:`DigitalTwinResult.from_dict`. There is currently no
        lenient path that silently substitutes values in this record, so
        ``strict`` has no behavioral effect. It is preserved so callers
        can forward the flag uniformly.
        """
        if not isinstance(data, ABCMapping):
            raise DigitalTwinParseError(
                "ResourceProjection.from_dict expects a Mapping."
            )

        v = _check_schema_version(data)

        if "resource_type" not in data:
            raise DigitalTwinParseError(
                "ResourceProjection missing key 'resource_type'."
            )

        try:
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
                    float(x) for x in (
                        data.get("substitution_timeline") or ()
                    )
                ),
                alternative_resources=tuple(
                    data.get("alternative_resources") or ()
                ),
                schema_version=v,
            )
        except DigitalTwinInputError as exc:
            raise DigitalTwinParseError(str(exc)) from exc
        except (TypeError, ValueError) as exc:
            raise DigitalTwinParseError(str(exc)) from exc

    @classmethod
    def from_json(
        cls, payload: str, *, strict: bool = False,
    ) -> "ResourceProjection":
        """Parse a JSON payload. See :meth:`from_dict`."""
        if not isinstance(payload, str):
            raise DigitalTwinParseError(
                "ResourceProjection.from_json expects a string."
            )
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise DigitalTwinParseError(
                f"ResourceProjection.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data, strict=strict)

    # -- hash / repr ---------------------------------------------------- #

    def __hash__(self) -> int:
        return hash((
            self.resource_type,
            self.current_level,
            self.projected_levels,
            self.depletion_year,
            self.confidence_lower,
            self.confidence_upper,
            self.substitution_availability,
            self.substitution_cost_factor,
            self.substitution_timeline,
            self.alternative_resources,
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
