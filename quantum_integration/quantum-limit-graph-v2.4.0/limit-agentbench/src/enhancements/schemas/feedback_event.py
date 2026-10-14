"""
Canonical MOPD event schema (v2.3.0)
====================================
Used by all feedback collectors, adaptive cost functions, and audit trails.

Enhancements (v2.3.0 over v2.2):
- Fixed DB and JSON round-trip bugs (metadata was silently dropped; caller
  dicts were mutated by ``from_db_dict`` and ``coerce_dicts``).
- Unified feedback-type registry: ``DEFAULT_FEEDBACK_TYPES`` plus subclass
  extension via ``allowed_feedback_types: ClassVar`` plus environment
  extension via ``MOPD_EXTRA_FEEDBACK_TYPES``.
- UTC-aware timestamps (``datetime.now(timezone.utc)``).
- Explicit per-feedback-type contracts (``FEEDBACK_TYPE_CONTRACTS``).
- Tighter types: ``PreferencePair`` model for RLHF; structured types for
  expert outputs, decision-process state, and graph metrics.
- Consistent identifier validation (``min_length=1`` on all IDs).
- Optional strict extras mode via ``MOPD_STRICT_EXTRA=1``.
- ``validate_assignment=True`` on the top-level event.
- ``FeedbackEvent.migrate`` + ``register_migration`` decorator.
- ``FeedbackEvent.validate_for_audit`` helper.
- Non-recursive JSON decode to avoid accidentally decoding user strings.
- Built-in tests at the bottom of the file.

All previous features retained.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    FrozenSet,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants (single source of truth)
# ---------------------------------------------------------------------------

SCHEMA_VERSION: str = "2.3.0"

DEFAULT_FEEDBACK_TYPES: FrozenSet[str] = frozenset({
    "routing",
    "distillation",
    "energy",
    "carbon",
    "helium",
    "rlhf",
    "graph",
    "expert",
    "evolutionary",
})

# For each feedback_type, the dotted paths that MUST be non-None.
FEEDBACK_TYPE_CONTRACTS: Dict[str, List[str]] = {
    "helium":       ["performance.helium_cost"],
    "rlhf":         ["rlhf"],
    "graph":        ["graph"],
    "distillation": ["learning"],   # additionally requires learning.* non-None
    "energy":       ["performance.energy_joules"],
    "carbon":       ["performance.carbon_g"],
}

# Fields that get flattened to `<field>_<subkey>` columns in DB form.
_FLATTENED_FIELDS: Tuple[str, ...] = (
    "context",
    "action",
    "performance",
    "learning",
    "envelope",
)

# Fields that carry JSON-encoded payloads in DB form.
_JSON_FIELDS: FrozenSet[str] = frozenset({
    "tags",
    "multi_objective",
    "custom_metrics",
    "metadata",
    "evolutionary",
    "expert",
    "decision_process",
    "rlhf",
    "graph",
    "context_teacher_ids",
})

_STRICT_EXTRA: bool = os.environ.get("MOPD_STRICT_EXTRA", "0") == "1"


def _utc_now() -> datetime:
    """Timezone-aware UTC now."""
    return datetime.now(timezone.utc)


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _env_extra_feedback_types() -> FrozenSet[str]:
    """Feedback types allow-listed via ``MOPD_EXTRA_FEEDBACK_TYPES``."""
    raw = os.environ.get("MOPD_EXTRA_FEEDBACK_TYPES", "")
    return frozenset(t.strip() for t in raw.split(",") if t.strip())


def _json_decode_once(value: Any) -> Any:
    """
    Decode a JSON string once. Does not recurse into the decoded payload,
    so user strings that merely look like JSON are not silently transformed.
    """
    if isinstance(value, str):
        s = value.lstrip()
        if s.startswith("{") or s.startswith("["):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
    return value


# ---------------------------------------------------------------------------
# Migration registry
# ---------------------------------------------------------------------------

_MIGRATIONS: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {}


def register_migration(from_version: str) -> Callable:
    """
    Register a migration function for a specific ``schema_version``.

    The function must accept a dict and return a dict with an updated
    ``schema_version``. Chain migrations until ``SCHEMA_VERSION`` is reached.
    """
    def deco(fn: Callable[[Dict[str, Any]], Dict[str, Any]]) -> Callable:
        _MIGRATIONS[from_version] = fn
        return fn
    return deco


@register_migration("2.0")
def _migrate_2_0_to_2_1(data: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(data)
    data.setdefault("learning", {})
    data["schema_version"] = "2.1"
    return data


@register_migration("2.1")
def _migrate_2_1_to_2_2(data: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(data)
    data.setdefault("envelope", {})
    data["schema_version"] = "2.2"
    return data


@register_migration("2.2")
def _migrate_2_2_to_2_3(data: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(data)
    data.setdefault("envelope", {})
    data.setdefault("metadata", {})
    data["schema_version"] = SCHEMA_VERSION
    return data


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------

class MetricValue(BaseModel):
    """A single metric with name, value, and optional direction/weight."""
    model_config = ConfigDict(extra="allow")

    name: str = Field(..., min_length=1)
    value: float
    direction: Optional[Literal["max", "min"]] = None
    weight: Optional[float] = Field(None, description="Relative weight; defaults to 1.0 by convention")


class ActionDetails(BaseModel):
    """Details about the selected action."""
    model_config = ConfigDict(extra="allow")

    selected_action: str = Field(..., min_length=1)
    selected_rank: Optional[int] = Field(None, ge=1, description="Rank among candidates")
    confidence_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    candidate_count: Optional[int] = Field(None, ge=1)
    candidate_actions: Optional[List[str]] = None

    @model_validator(mode="after")
    def _check_candidate_count(self) -> "ActionDetails":
        if self.candidate_actions is not None:
            if self.candidate_count is None:
                # derive for convenience
                object.__setattr__(self, "candidate_count", len(self.candidate_actions))
            elif self.candidate_count != len(self.candidate_actions):
                raise ValueError(
                    "candidate_count must match len(candidate_actions) when both provided"
                )
        return self


class PerformanceMetrics(BaseModel):
    """Performance and resource metrics."""
    model_config = ConfigDict(extra="allow")

    quality_score: float = Field(ge=0.0, le=1.0)
    latency_ms: float = Field(ge=0.0)
    energy_joules: float = Field(ge=0.0)
    carbon_g: float = Field(ge=0.0)
    helium_cost: Optional[float] = Field(None, ge=0.0)
    duration_ms: Optional[float] = Field(None, ge=0.0)
    resource_usage: Dict[str, float] = Field(default_factory=dict)

    @field_validator("resource_usage")
    @classmethod
    def _check_resource_usage(cls, v: Dict[str, float]) -> Dict[str, float]:
        bad = {k: val for k, val in v.items() if val < 0}
        if bad:
            raise ValueError(f"resource_usage values must be non-negative: {bad}")
        return v


class LearningMetrics(BaseModel):
    """Metrics related to learning / distillation."""
    model_config = ConfigDict(extra="allow")

    distillation_loss: Optional[float] = Field(None, ge=0.0)
    multi_teacher_loss: Optional[float] = Field(None, ge=0.0,
                                                description="Aggregated loss across all teachers")
    teacher_individual_losses: Optional[Dict[str, float]] = Field(
        None, description="Per-teacher distillation loss"
    )


class ContextInfo(BaseModel):
    """Execution context."""
    model_config = ConfigDict(extra="allow")

    task_id: str = Field(..., min_length=1)
    model_id: Optional[str] = None
    policy_version: Optional[str] = None
    teacher_ids: List[str] = Field(default_factory=list)


class EvolutionaryInfo(BaseModel):
    """Fields specific to bio-inspired optimization."""
    model_config = ConfigDict(extra="allow")

    generation: Optional[int] = Field(None, ge=0)
    population_id: Optional[str] = None
    parent_ids: Optional[List[str]] = None
    fitness_vector: Optional[List[float]] = None
    selection_pressure: Optional[float] = Field(None, ge=0.0)
    mutation_rate: Optional[float] = Field(None, ge=0.0, le=1.0)
    crossover_rate: Optional[float] = Field(None, ge=0.0, le=1.0)


class ExpertInfo(BaseModel):
    """Fields specific to mixture-of-experts."""
    model_config = ConfigDict(extra="allow")

    expert_id: Optional[str] = None
    gate_weights: Optional[Dict[str, float]] = None
    expert_outputs: Optional[Dict[str, List[float]]] = Field(
        None, description="Expert id -> output vector (typically a probability distribution)"
    )
    load_balancing_loss: Optional[float] = Field(None, ge=0.0)
    expert_utilization: Optional[Dict[str, float]] = Field(
        None, description="Fraction of tokens routed to each expert"
    )


class DecisionProcessInfo(BaseModel):
    """Fields specific to MODP / sequential decision making."""
    model_config = ConfigDict(extra="allow")

    state: Optional[Union[Dict[str, Any], List[float]]] = None
    next_state: Optional[Union[Dict[str, Any], List[float]]] = None
    reward_vector: Optional[List[float]] = None
    done: Optional[bool] = None
    policy_vector: Optional[List[float]] = Field(None,
                                                 description="Output policy distribution")
    value_estimate: Optional[float] = Field(None,
                                            description="Value function estimate for the state")


class PreferencePair(BaseModel):
    """Chosen/rejected response pair for RLHF."""
    model_config = ConfigDict(extra="allow")

    chosen: str = Field(..., min_length=1)
    rejected: str = Field(..., min_length=1)
    chosen_score: Optional[float] = None
    rejected_score: Optional[float] = None


class RLHFInfo(BaseModel):
    """Fields specific to RLHF."""
    model_config = ConfigDict(extra="allow")

    reward_model_id: Optional[str] = None
    human_feedback_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    preference_pair: Optional[PreferencePair] = None
    reward_model_score: Optional[float] = None
    kl_penalty: Optional[float] = Field(None, ge=0.0,
                                        description="KL divergence penalty applied during RLHF")


class GraphInfo(BaseModel):
    """Fields specific to LIMIT Graph integration."""
    model_config = ConfigDict(extra="allow")

    graph_id: Optional[str] = None
    node_id: Optional[str] = None
    edge_id: Optional[str] = None
    graph_embedding: Optional[List[float]] = Field(None,
                                                   description="Embedding of the graph state")
    graph_metrics: Optional[Dict[str, float]] = Field(
        None, description="Graph metrics (e.g., centrality, density)"
    )


class EventEnvelope(BaseModel):
    """Envelope fields for distributed tracing and lifecycle."""
    model_config = ConfigDict(extra="allow")

    priority: int = Field(0, ge=0, le=10, description="0=low, 10=high")
    retention_seconds: Optional[int] = Field(None, ge=0)
    trace_id: Optional[str] = None
    parent_event_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Main event
# ---------------------------------------------------------------------------

class FeedbackEvent(BaseModel):
    """Standardized feedback event for Green Agent (v2.3 schema)."""
    model_config = ConfigDict(
        extra="allow",
        validate_assignment=True,
    )

    # Class-level allow-list. Subclasses may override.
    allowed_feedback_types: ClassVar[FrozenSet[str]] = DEFAULT_FEEDBACK_TYPES

    # -- Schema version ------------------------------------------------------
    schema_version: str = Field(SCHEMA_VERSION, description="Schema version for compatibility")

    # -- Core identifiers ----------------------------------------------------
    event_id: str = Field(default_factory=_new_uuid, min_length=1)
    timestamp: datetime = Field(default_factory=_utc_now)
    correlation_id: str = Field(default_factory=_new_uuid, min_length=1)
    source: str = Field(..., min_length=1, description="Component that emitted the event")
    environment: str = Field("unknown", min_length=1,
                             description="Deployment environment (e.g. production, staging, test)")
    agent_version: Optional[str] = None

    # -- Envelope ------------------------------------------------------------
    envelope: EventEnvelope = Field(default_factory=EventEnvelope)

    # -- Core blocks ---------------------------------------------------------
    context: ContextInfo
    action: ActionDetails
    performance: PerformanceMetrics
    learning: LearningMetrics = Field(default_factory=LearningMetrics)

    # -- Multi-objective vector ---------------------------------------------
    multi_objective: List[MetricValue] = Field(default_factory=list)

    # -- Flexible metrics ----------------------------------------------------
    custom_metrics: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # -- Feedback type -------------------------------------------------------
    feedback_type: str = Field(..., min_length=1)

    # -- Adaptive cost -------------------------------------------------------
    adaptive_cost_value: float

    # -- Tags ----------------------------------------------------------------
    tags: List[str] = Field(default_factory=list)

    # -- Optional module-specific information -------------------------------
    evolutionary: Optional[EvolutionaryInfo] = None
    expert: Optional[ExpertInfo] = None
    decision_process: Optional[DecisionProcessInfo] = None
    rlhf: Optional[RLHFInfo] = None
    graph: Optional[GraphInfo] = None

    # ==================================================================
    # Validators
    # ==================================================================
    @field_validator("feedback_type")
    @classmethod
    def _validate_feedback_type(cls, v: str) -> str:
        base = getattr(cls, "allowed_feedback_types", DEFAULT_FEEDBACK_TYPES)
        allowed = set(base) | set(_env_extra_feedback_types())
        if v not in allowed:
            raise ValueError(
                f"feedback_type must be one of {sorted(allowed)}; got {v!r}"
            )
        return v

    @model_validator(mode="before")
    @classmethod
    def _coerce_dicts(cls, values: Any) -> Any:
        """
        Coerce dicts into their Pydantic sub-models. Never mutates the input.
        """
        if not isinstance(values, dict):
            return values
        new_values = dict(values)
        sub_models: Dict[str, Any] = {
            "envelope": EventEnvelope,
            "context": ContextInfo,
            "action": ActionDetails,
            "performance": PerformanceMetrics,
            "learning": LearningMetrics,
            "evolutionary": EvolutionaryInfo,
            "expert": ExpertInfo,
            "decision_process": DecisionProcessInfo,
            "rlhf": RLHFInfo,
            "graph": GraphInfo,
        }
        for field, model_cls in sub_models.items():
            if field in new_values and isinstance(new_values[field], dict):
                new_values[field] = model_cls(**new_values[field])
        if "multi_objective" in new_values and isinstance(new_values["multi_objective"], list):
            new_values["multi_objective"] = [
                MetricValue(**item) if isinstance(item, dict) else item
                for item in new_values["multi_objective"]
            ]
        # Encode datetimes to ISO so that ``datetime.now()`` (naive) is handled too.
        ts = new_values.get("timestamp")
        if isinstance(ts, datetime) and ts.tzinfo is None:
            new_values["timestamp"] = ts.replace(tzinfo=timezone.utc)
        return new_values

    @model_validator(mode="after")
    def _validate_consistency(self) -> "FeedbackEvent":
        ft = self.feedback_type
        if ft == "helium" and self.performance.helium_cost is None:
            raise ValueError("performance.helium_cost required when feedback_type='helium'")
        if ft == "rlhf" and self.rlhf is None:
            raise ValueError("rlhf required when feedback_type='rlhf'")
        if ft == "graph" and self.graph is None:
            raise ValueError("graph required when feedback_type='graph'")
        if ft == "distillation" and not self._learning_has_content():
            raise ValueError(
                "learning.{distillation_loss,multi_teacher_loss,teacher_individual_losses} "
                "required when feedback_type='distillation'"
            )
        if ft == "energy" and self.performance.energy_joules <= 0:
            raise ValueError("performance.energy_joules > 0 required when feedback_type='energy'")
        if ft == "carbon" and self.performance.carbon_g <= 0:
            raise ValueError("performance.carbon_g > 0 required when feedback_type='carbon'")
        return self

    @model_validator(mode="after")
    def _handle_extras(self) -> "FeedbackEvent":
        extra = self.model_extra or {}
        if extra:
            if _STRICT_EXTRA:
                raise ValueError(f"unexpected extra fields: {sorted(extra.keys())}")
            logger.debug("FeedbackEvent has extra fields: %s", sorted(extra.keys()))
        return self

    # ------------------------------------------------------------------
    def _learning_has_content(self) -> bool:
        lm = self.learning
        if lm is None:
            return False
        return any([
            lm.distillation_loss is not None,
            lm.multi_teacher_loss is not None,
            lm.teacher_individual_losses is not None,
        ])

    # ==================================================================
    # Serialization
    # ==================================================================
    def to_db_dict(self) -> Dict[str, Any]:
        """
        Convert to a flat dict for SQLite insertion.

        - ``timestamp`` becomes a UTC POSIX float.
        - Declared sub-models (``context``, ``action``, ...) are flattened
          to ``<field>_<subkey>`` columns.
        - Extra (undeclared) fields are stored under a single ``extra_json``
          column so that they round-trip losslessly.
        - Any remaining dict/list values are JSON-encoded.
        """
        data = self.model_dump(mode="json")

        # Timestamp -> UTC POSIX float
        ts = self.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        data["timestamp"] = ts.timestamp()

        # Extract extras (undeclared top-level keys) into a single column
        declared = set(type(self).model_fields.keys())
        extras = {k: v for k, v in data.items() if k not in declared}
        for k in list(extras.keys()):
            del data[k]
        if extras:
            data["extra_json"] = json.dumps(extras)

        # Flatten declared sub-models
        for field in _FLATTENED_FIELDS:
            sub = data.pop(field, None)
            if isinstance(sub, dict):
                for k, v in sub.items():
                    data[f"{field}_{k}"] = v

        # JSON-encode remaining dict/list payloads
        for k, v in list(data.items()):
            if isinstance(v, (dict, list)):
                data[k] = json.dumps(v)

        return data

    @classmethod
    def from_db_dict(cls, data: Dict[str, Any]) -> "FeedbackEvent":
        """Reconstruct from a DB dict (flattened fields, POSIX timestamp)."""
        data = dict(data)  # never mutate the caller's dict

        # 1. Extract extras blob
        extras: Dict[str, Any] = {}
        if "extra_json" in data:
            raw = data.pop("extra_json")
            if isinstance(raw, str):
                try:
                    extras = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    extras = {}
            elif isinstance(raw, dict):
                extras = raw

        # 2. Rebuild declared sub-models from prefixed columns
        nested: Dict[str, Dict[str, Any]] = {}
        for key in list(data.keys()):
            for field in _FLATTENED_FIELDS:
                prefix = f"{field}_"
                if key.startswith(prefix):
                    nested.setdefault(field, {})[key[len(prefix):]] = data.pop(key)
                    break

        # 3. One-level JSON decode on top-level values
        for k, v in list(data.items()):
            data[k] = _json_decode_once(v)

        # 4. One-level JSON decode on flattened sub-values
        for field, sub in nested.items():
            for k, v in list(sub.items()):
                sub[k] = _json_decode_once(v)

        # 5. Merge nested dicts back into the top-level
        for field, sub in nested.items():
            data[field] = sub

        # 6. Restore timestamp
        if isinstance(data.get("timestamp"), (int, float)):
            data["timestamp"] = datetime.fromtimestamp(data["timestamp"], tz=timezone.utc)

        # 7. Merge extras back
        for k, v in extras.items():
            data.setdefault(k, v)

        return cls(**data)

    def to_json(self) -> str:
        """Serialize to JSON (datetime as ISO 8601 with timezone)."""
        return self.model_dump_json(mode="json")

    @classmethod
    def from_json(cls, json_str: str) -> "FeedbackEvent":
        """Deserialize from JSON."""
        return cls.model_validate_json(json_str)

    @classmethod
    def migrate(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply registered migrations until ``SCHEMA_VERSION`` is reached.
        Returns a new dict; does not mutate the input.
        """
        data = dict(data)
        seen: set = set()
        while True:
            v = data.get("schema_version", SCHEMA_VERSION)
            if v == SCHEMA_VERSION or v in seen or v not in _MIGRATIONS:
                return data
            seen.add(v)
            data = _MIGRATIONS[v](data)

    # ==================================================================
    # Helpers
    # ==================================================================
    def set_metadata(self, key: str, value: Any) -> None:
        self.metadata[key] = value

    def get_metadata(self, key: str, default: Any = None) -> Any:
        return self.metadata.get(key, default)

    def add_tag(self, tag: str) -> None:
        if tag not in self.tags:
            self.tags.append(tag)

    def add_objective(
        self,
        name: str,
        value: float,
        direction: Optional[Literal["max", "min"]] = None,
        weight: Optional[float] = None,
    ) -> None:
        self.multi_objective.append(
            MetricValue(name=name, value=value, direction=direction, weight=weight)
        )

    def add_custom_metric(self, key: str, value: Any) -> None:
        self.custom_metrics[key] = value

    def get_custom_metric(self, key: str, default: Any = None) -> Any:
        return self.custom_metrics.get(key, default)

    # ------------------------------------------------------------------
    def validate_for_audit(self) -> List[str]:
        """
        Return a list of *advisory* warnings that do not prevent construction
        but which an audit pipeline may wish to flag.
        """
        warnings: List[str] = []
        if not self.envelope.trace_id:
            warnings.append("envelope.trace_id is missing")
        if not self.envelope.parent_event_id:
            warnings.append("envelope.parent_event_id is missing")
        if self.correlation_id == self.event_id:
            warnings.append("correlation_id equals event_id (no shared correlation set)")
        if self.environment in ("", "unknown"):
            warnings.append("environment is not explicitly set")
        if not self.context.policy_version:
            warnings.append("context.policy_version is missing")
        if self.feedback_type in ("routing", "distillation") and not self.context.teacher_ids:
            warnings.append(
                f"context.teacher_ids is empty for feedback_type={self.feedback_type!r}"
            )
        if self.adaptive_cost_value < 0:
            warnings.append("adaptive_cost_value is negative")
        if self.feedback_type == "distillation" and not self._learning_has_content():
            warnings.append("distillation feedback has no learning metrics")
        return warnings


__all__ = [
    "SCHEMA_VERSION",
    "DEFAULT_FEEDBACK_TYPES",
    "FEEDBACK_TYPE_CONTRACTS",
    "MetricValue",
    "ActionDetails",
    "PerformanceMetrics",
    "LearningMetrics",
    "ContextInfo",
    "EvolutionaryInfo",
    "ExpertInfo",
    "DecisionProcessInfo",
    "PreferencePair",
    "RLHFInfo",
    "GraphInfo",
    "EventEnvelope",
    "FeedbackEvent",
    "register_migration",
]


# ============================================================================
# Tests
# ============================================================================

def _make_routing_event(**overrides: Any) -> "FeedbackEvent":
    base: Dict[str, Any] = {
        "source": "test",
        "context": {"task_id": "t1", "teacher_ids": ["a", "b"]},
        "action": {"selected_action": "route_a"},
        "performance": {
            "quality_score": 0.9,
            "latency_ms": 10.0,
            "energy_joules": 1.0,
            "carbon_g": 0.5,
        },
        "feedback_type": "routing",
        "adaptive_cost_value": 0.42,
    }
    base.update(overrides)
    return FeedbackEvent(**base)


def _test_utc_timestamp() -> None:
    ev = _make_routing_event()
    assert ev.timestamp.tzinfo is not None, "timestamp must be timezone-aware"


def _test_roundtrip_json() -> None:
    ev = _make_routing_event(tags=["a", "b"], metadata={"foo": {"nested": [1, 2, 3]}})
    s = ev.to_json()
    ev2 = FeedbackEvent.from_json(s)
    assert ev2.source == "test"
    assert ev2.metadata == {"foo": {"nested": [1, 2, 3]}}
    assert ev2.tags == ["a", "b"]
    assert ev2.context.teacher_ids == ["a", "b"]


def _test_roundtrip_db() -> None:
    ev = _make_routing_event(
        tags=["a", "b"],
        metadata={"foo": {"nested": [1, 2, 3]}},
        custom_metrics={"note": "keep me as a string"},
    )
    ev.add_objective("quality", 0.9, direction="max", weight=1.0)

    d = ev.to_db_dict()
    snapshot = dict(d)

    ev2 = FeedbackEvent.from_db_dict(d)
    assert ev2.source == "test"
    assert ev2.context.task_id == "t1"
    assert ev2.context.teacher_ids == ["a", "b"]
    assert ev2.metadata == {"foo": {"nested": [1, 2, 3]}}
    assert ev2.custom_metrics["note"] == "keep me as a string"
    assert ev2.tags == ["a", "b"]
    assert len(ev2.multi_objective) == 1
    assert ev2.multi_objective[0].name == "quality"
    assert ev2.multi_objective[0].direction == "max"

    # Must not mutate caller's dict
    assert d == snapshot, "from_db_dict mutated its input dict"


def _test_roundtrip_extras() -> None:
    ev = _make_routing_event()
    # 'weird_field' is an undeclared extra; extra='allow' permits it.
    ev = FeedbackEvent.model_validate({
        **ev.model_dump(mode="json"),
        "weird_field": {"nested": True},
    })
    d = ev.to_db_dict()
    assert "extra_json" in d
    ev2 = FeedbackEvent.from_db_dict(d)
    assert ev2.model_extra.get("weird_field") == {"nested": True}


def _test_coerce_dicts_does_not_mutate_input() -> None:
    raw = {
        "source": "test",
        "context": {"task_id": "t1"},
        "action": {"selected_action": "a"},
        "performance": {
            "quality_score": 0.5,
            "latency_ms": 1.0,
            "energy_joules": 1.0,
            "carbon_g": 0.1,
        },
        "feedback_type": "routing",
        "adaptive_cost_value": 0.1,
    }
    snapshot = json.loads(json.dumps(raw))
    FeedbackEvent(**raw)
    assert raw == snapshot, "FeedbackEvent(**raw) must not mutate the input dict"


def _test_consistency_contracts() -> None:
    # helium without helium_cost
    try:
        _make_routing_event(
            feedback_type="helium",
            performance={
                "quality_score": 0.5,
                "latency_ms": 1.0,
                "energy_joules": 1.0,
                "carbon_g": 0.1,
            },
        )
    except Exception:
        pass
    else:
        raise AssertionError("helium without helium_cost should fail")

    # distillation without learning content
    try:
        _make_routing_event(feedback_type="distillation")
    except Exception:
        pass
    else:
        raise AssertionError("distillation without learning content should fail")

    # energy with zero joules
    try:
        _make_routing_event(
            feedback_type="energy",
            performance={
                "quality_score": 0.5,
                "latency_ms": 1.0,
                "energy_joules": 0.0,
                "carbon_g": 0.1,
            },
        )
    except Exception:
        pass
    else:
        raise AssertionError("energy with zero joules should fail")


def _test_subclass_feedback_type_extension() -> None:
    class CustomEvent(FeedbackEvent):
        allowed_feedback_types: ClassVar[FrozenSet[str]] = (
            DEFAULT_FEEDBACK_TYPES | frozenset({"custom"})
        )

    ev = CustomEvent(
        source="test",
        context={"task_id": "t1"},
        action={"selected_action": "a"},
        performance={
            "quality_score": 0.5,
            "latency_ms": 1.0,
            "energy_joules": 1.0,
            "carbon_g": 0.1,
        },
        feedback_type="custom",
        adaptive_cost_value=0.1,
    )
    assert ev.feedback_type == "custom"

    # Base class must still reject it
    try:
        _make_routing_event(feedback_type="custom")
    except Exception:
        pass
    else:
        raise AssertionError("base FeedbackEvent should reject unknown feedback_type")


def _test_env_feedback_type_extension() -> None:
    os.environ["MOPD_EXTRA_FEEDBACK_TYPES"] = "special"
    try:
        ev = _make_routing_event(feedback_type="special")
        assert ev.feedback_type == "special"
    finally:
        os.environ.pop("MOPD_EXTRA_FEEDBACK_TYPES", None)


def _test_audit_warnings() -> None:
    ev = _make_routing_event()
    warnings = ev.validate_for_audit()
    assert any("trace_id" in w for w in warnings), warnings
    assert any("environment" in w for w in warnings), warnings


def _test_migration_registry() -> None:
    legacy = {
        "schema_version": "2.0",
        "source": "test",
        "context": {"task_id": "t1"},
        "action": {"selected_action": "a"},
        "performance": {
            "quality_score": 0.5,
            "latency_ms": 1.0,
            "energy_joules": 1.0,
            "carbon_g": 0.1,
        },
        "feedback_type": "routing",
        "adaptive_cost_value": 0.1,
    }
    migrated = FeedbackEvent.migrate(legacy)
    assert migrated["schema_version"] == SCHEMA_VERSION
    ev = FeedbackEvent(**migrated)
    assert ev.schema_version == SCHEMA_VERSION


def _test_preference_pair_coercion() -> None:
    ev = _make_routing_event(
        feedback_type="rlhf",
        rlhf={
            "reward_model_id": "rm-1",
            "human_feedback_score": 0.8,
            "preference_pair": {
                "chosen": "response A",
                "rejected": "response B",
                "chosen_score": 0.9,
                "rejected_score": 0.2,
            },
        },
    )
    assert isinstance(ev.rlhf.preference_pair, PreferencePair)
    d = ev.to_db_dict()
    ev2 = FeedbackEvent.from_db_dict(d)
    assert ev2.rlhf.preference_pair.chosen == "response A"


def _test_validate_assignment() -> None:
    ev = _make_routing_event()
    try:
        ev.action.confidence_score = 2.0  # out of range
    except Exception:
        return
    raise AssertionError("validate_assignment=True should reject out-of-range assignments")


def _run_all_tests() -> None:
    _test_utc_timestamp()
    _test_roundtrip_json()
    _test_roundtrip_db()
    _test_roundtrip_extras()
    _test_coerce_dicts_does_not_mutate_input()
    _test_consistency_contracts()
    _test_subclass_feedback_type_extension()
    _test_env_feedback_type_extension()
    _test_audit_warnings()
    _test_migration_registry()
    _test_preference_pair_coercion()
    _test_validate_assignment()
    print("All FeedbackEvent tests passed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _run_all_tests()
