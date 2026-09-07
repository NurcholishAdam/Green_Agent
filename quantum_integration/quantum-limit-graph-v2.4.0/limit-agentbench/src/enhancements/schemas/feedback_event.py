"""
Canonical MOPD event schema.
Used by all feedback collectors, adaptive cost functions, and audit trails.

Enhanced with:
- Logical sub-models for better organization.
- Multi-objective vector support.
- Flexible metrics and resource usage.
- Optional fields for bio_inspired, moe_system, MODP, RLHF, and Graph.
- Improved serialization using Pydantic's JSON mode.
- Envelope fields (priority, retention, trace).
- Discriminated event types for future extensibility.
- Accept dicts for sub-model fields (coerced automatically).
"""

from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from datetime import datetime
from typing import Optional, Dict, Any, List, Union, Literal
import uuid
import json


class MetricValue(BaseModel):
    """A single metric with name, value, and optional direction."""
    name: str
    value: float
    direction: Optional[Literal['max', 'min']] = None
    weight: Optional[float] = None


class ActionDetails(BaseModel):
    """Details about the selected action."""
    selected_action: str = Field(..., min_length=1)
    selected_rank: Optional[int] = Field(None, ge=1, description="Rank among candidates")
    confidence_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    candidate_count: Optional[int] = Field(None, ge=1)
    candidate_actions: Optional[List[str]] = None


class PerformanceMetrics(BaseModel):
    """Performance and resource metrics."""
    quality_score: float = Field(ge=0.0, le=1.0)
    latency_ms: float = Field(ge=0.0)
    energy_joules: float = Field(ge=0.0)
    carbon_g: float = Field(ge=0.0)
    helium_cost: Optional[float] = Field(None, ge=0.0)
    duration_ms: Optional[float] = Field(None, ge=0.0)
    resource_usage: Dict[str, float] = Field(default_factory=dict)


class LearningMetrics(BaseModel):
    """Metrics related to learning/distillation."""
    distillation_loss: Optional[float] = Field(None, ge=0.0)
    multi_teacher_loss: Optional[float] = Field(None, ge=0.0, description="Aggregated loss across all teachers")
    teacher_individual_losses: Optional[Dict[str, float]] = Field(None, description="Per‑teacher distillation loss")


class ContextInfo(BaseModel):
    """Execution context."""
    task_id: str
    model_id: Optional[str] = None
    policy_version: Optional[str] = None
    teacher_ids: List[str] = Field(default_factory=list)


class EvolutionaryInfo(BaseModel):
    """Fields specific to bio_inspired optimization."""
    generation: Optional[int] = Field(None, ge=0)
    population_id: Optional[str] = None
    parent_ids: Optional[List[str]] = None
    fitness_vector: Optional[List[float]] = None
    selection_pressure: Optional[float] = Field(None, ge=0.0)
    mutation_rate: Optional[float] = Field(None, ge=0.0, le=1.0)
    crossover_rate: Optional[float] = Field(None, ge=0.0, le=1.0)


class ExpertInfo(BaseModel):
    """Fields specific to mixture-of-experts."""
    expert_id: Optional[str] = None
    gate_weights: Optional[Dict[str, float]] = None
    expert_outputs: Optional[Dict[str, Any]] = None
    load_balancing_loss: Optional[float] = Field(None, ge=0.0)
    expert_utilization: Optional[Dict[str, float]] = Field(None, description="Fraction of tokens routed to each expert")


class DecisionProcessInfo(BaseModel):
    """Fields specific to MODP / sequential decision making."""
    state: Optional[Any] = None
    next_state: Optional[Any] = None
    reward_vector: Optional[List[float]] = None
    done: Optional[bool] = None
    policy_vector: Optional[List[float]] = Field(None, description="Output policy distribution")
    value_estimate: Optional[float] = Field(None, description="Value function estimate for the state")


class RLHFInfo(BaseModel):
    """Fields specific to RLHF (Reinforcement Learning from Human Feedback)."""
    reward_model_id: Optional[str] = None
    human_feedback_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    preference_pair: Optional[Dict[str, Any]] = Field(None, description="Chosen and rejected responses")
    reward_model_score: Optional[float] = Field(None, description="Score from the reward model")
    kl_penalty: Optional[float] = Field(None, ge=0.0, description="KL divergence penalty applied during RLHF")


class GraphInfo(BaseModel):
    """Fields specific to LIMIT Graph integration."""
    graph_id: Optional[str] = None
    node_id: Optional[str] = None
    edge_id: Optional[str] = None
    graph_embedding: Optional[List[float]] = Field(None, description="Embedding of the graph state")
    graph_metrics: Optional[Dict[str, float]] = Field(None, description="Graph metrics (e.g., centrality, density)")


class EventEnvelope(BaseModel):
    """Envelope fields for distributed tracing and lifecycle."""
    priority: int = Field(0, ge=0, le=10, description="0=low, 10=high")
    retention_seconds: Optional[int] = Field(None, ge=0)
    trace_id: Optional[str] = None
    parent_event_id: Optional[str] = None


class FeedbackEvent(BaseModel):
    """Standardized feedback event for Green Agent (v2.2 schema)."""
    model_config = ConfigDict(extra='allow')  # allow additional fields for flexibility

    # Schema version
    schema_version: str = Field("2.2", description="Schema version for compatibility")

    # Core identifiers
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.now)
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source: str = Field(..., description="Component that emitted the event")
    environment: str = Field("production", description="Deployment environment")
    agent_version: Optional[str] = None

    # Envelope
    envelope: EventEnvelope = Field(default_factory=EventEnvelope)

    # Context
    context: ContextInfo

    # Action details
    action: ActionDetails

    # Performance metrics
    performance: PerformanceMetrics

    # Learning metrics
    learning: LearningMetrics = Field(default_factory=LearningMetrics)

    # Multi-objective vector
    multi_objective: List[MetricValue] = Field(default_factory=list)

    # Flexible metrics
    custom_metrics: Dict[str, Any] = Field(default_factory=dict)

    # Feedback type
    feedback_type: str

    # Adaptive cost result
    adaptive_cost_value: float

    # Tags for flexible filtering
    tags: List[str] = Field(default_factory=list)

    # Metadata for future extensions
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Optional module-specific information
    evolutionary: Optional[EvolutionaryInfo] = None
    expert: Optional[ExpertInfo] = None
    decision_process: Optional[DecisionProcessInfo] = None
    rlhf: Optional[RLHFInfo] = None
    graph: Optional[GraphInfo] = None

    # Class-level allowed feedback types (can be extended via subclassing or environment)
    allowed_feedback_types: set = {"routing", "distillation", "energy", "carbon", "helium", "rlhf", "graph"}

    @field_validator('feedback_type')
    @classmethod
    def validate_feedback_type(cls, v: str) -> str:
        # Allow additional types from environment variable or subclass
        extra_allowed = set()
        # Check if subclass has extended the set
        if hasattr(cls, 'allowed_feedback_types'):
            allowed = cls.allowed_feedback_types
        else:
            allowed = {"routing", "distillation", "energy", "carbon", "helium", "rlhf", "graph"}
        if v not in allowed:
            raise ValueError(f"feedback_type must be one of {allowed}")
        return v

    @model_validator(mode='after')
    def validate_consistency(self) -> 'FeedbackEvent':
        if self.feedback_type == "helium" and self.performance.helium_cost is None:
            raise ValueError("helium_cost must be provided when feedback_type='helium'")
        if self.feedback_type == "rlhf" and self.rlhf is None:
            raise ValueError("rlhf info must be provided when feedback_type='rlhf'")
        if self.feedback_type == "graph" and self.graph is None:
            raise ValueError("graph info must be provided when feedback_type='graph'")
        return self

    @model_validator(mode='before')
    @classmethod
    def coerce_dicts(cls, values: Any) -> Any:
        """
        Convert dicts provided for sub-model fields into the corresponding Pydantic models.
        This allows callers to pass dicts instead of model instances.
        """
        if not isinstance(values, dict):
            return values
        # Map field names to their model classes
        sub_models = {
            'envelope': EventEnvelope,
            'context': ContextInfo,
            'action': ActionDetails,
            'performance': PerformanceMetrics,
            'learning': LearningMetrics,
            'evolutionary': EvolutionaryInfo,
            'expert': ExpertInfo,
            'decision_process': DecisionProcessInfo,
            'rlhf': RLHFInfo,
            'graph': GraphInfo,
            # Note: multi_objective is a list of MetricValue, handle separately if needed
        }
        for field, model_cls in sub_models.items():
            if field in values and isinstance(values[field], dict):
                values[field] = model_cls(**values[field])
        # Handle multi_objective if it contains dicts
        if 'multi_objective' in values and isinstance(values['multi_objective'], list):
            new_list = []
            for item in values['multi_objective']:
                if isinstance(item, dict):
                    new_list.append(MetricValue(**item))
                else:
                    new_list.append(item)
            values['multi_objective'] = new_list
        return values

    # --------------------------------------------------------------------------
    # Serialization helpers
    # --------------------------------------------------------------------------
    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dict for SQLite insertion (timestamp as float, nested models flattened)."""
        data = self.model_dump(mode='json', exclude={'timestamp'})
        data['timestamp'] = self.timestamp.timestamp()
        # Flatten nested structures for easier querying
        for field in ['context', 'action', 'performance', 'learning', 'envelope']:
            sub = data.pop(field)
            data.update({f"{field}_{k}": v for k, v in sub.items()})
        # Convert complex fields to JSON strings for storage
        json_fields = [
            'context_teacher_ids', 'tags', 'multi_objective', 'custom_metrics',
            'evolutionary', 'expert', 'decision_process', 'rlhf', 'graph'
        ]
        for field in json_fields:
            if field in data and data[field] is not None:
                data[field] = json.dumps(data[field])
        return data

    @classmethod
    def from_db_dict(cls, data: Dict[str, Any]) -> "FeedbackEvent":
        """Reconstruct from a DB dict (flattened fields, float timestamp)."""
        # Rebuild nested dicts
        nested = {}
        for field in ['context', 'action', 'performance', 'learning', 'envelope']:
            sub = {}
            prefix = f"{field}_"
            for k, v in list(data.items()):
                if k.startswith(prefix):
                    sub[k[len(prefix):]] = v
                    del data[k]
            nested[field] = sub
        # Parse JSON fields
        json_fields = [
            'context_teacher_ids', 'tags', 'multi_objective', 'custom_metrics',
            'evolutionary', 'expert', 'decision_process', 'rlhf', 'graph'
        ]
        for field in json_fields:
            if field in data and isinstance(data[field], str):
                data[field] = json.loads(data[field])
        # Convert timestamp
        if 'timestamp' in data and isinstance(data['timestamp'], (int, float)):
            data['timestamp'] = datetime.fromtimestamp(data['timestamp'])
        # Merge nested back into main data
        data.update(nested)
        # The validator will coerce dicts to models
        return cls(**data)

    def to_json(self) -> str:
        """Serialize to JSON (datetime as ISO string, nested as-is)."""
        return self.model_dump_json(mode='json')

    @classmethod
    def from_json(cls, json_str: str) -> "FeedbackEvent":
        """Deserialize from JSON."""
        return cls.model_validate_json(json_str)

    # --------------------------------------------------------------------------
    # Helper methods
    # --------------------------------------------------------------------------
    def set_metadata(self, key: str, value: Any) -> None:
        self.metadata[key] = value

    def get_metadata(self, key: str, default: Any = None) -> Any:
        return self.metadata.get(key, default)

    def add_tag(self, tag: str) -> None:
        if tag not in self.tags:
            self.tags.append(tag)

    def add_objective(self, name: str, value: float, direction: Optional[str] = None,
                      weight: Optional[float] = None) -> None:
        self.multi_objective.append(
            MetricValue(name=name, value=value, direction=direction, weight=weight)
        )

    def add_custom_metric(self, key: str, value: Any) -> None:
        self.custom_metrics[key] = value

    def get_custom_metric(self, key: str, default: Any = None) -> Any:
        return self.custom_metrics.get(key, default)
