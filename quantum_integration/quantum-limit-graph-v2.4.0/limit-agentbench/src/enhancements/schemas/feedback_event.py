"""
Canonical MOPD event schema.
Used by all feedback collectors, adaptive cost functions, and audit trails.
"""
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime
from typing import Optional, Dict, Any, List
import uuid
import json

class FeedbackEvent(BaseModel):
    """Standardized feedback event for Green Agent (v2.0 schema)."""
    
    # Schema version
    schema_version: str = Field("2.0", description="Schema version for compatibility")
    
    # Core identifiers
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.now)
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source: str = Field(..., description="Component that emitted the event")
    environment: str = Field("production", description="Deployment environment")
    agent_version: Optional[str] = None
    
    # Context
    task_id: str
    model_id: Optional[str] = None
    policy_version: Optional[str] = None
    teacher_ids: List[str] = Field(default_factory=list)
    
    # Action details
    selected_action: str
    selected_rank: Optional[int] = Field(None, ge=1, description="Rank among candidates")
    confidence_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    candidate_count: Optional[int] = Field(None, ge=1)
    
    # Performance metrics
    quality_score: float = Field(ge=0.0, le=1.0)
    latency_ms: float = Field(ge=0.0)
    energy_joules: float = Field(ge=0.0)
    carbon_g: float = Field(ge=0.0)
    helium_cost: Optional[float] = Field(None, ge=0.0)
    duration_ms: Optional[float] = Field(None, ge=0.0)
    
    # Learning / distillation
    distillation_loss: Optional[float] = Field(None, ge=0.0)
    
    # Resource usage (typed)
    resource_usage: Dict[str, float] = Field(default_factory=dict)
    
    # Feedback type
    feedback_type: str  # "routing", "distillation", "energy", "carbon", "helium"
    
    # Adaptive cost result
    adaptive_cost_value: float
    
    # Tags for flexible filtering
    tags: List[str] = Field(default_factory=list)
    
    # Metadata for future extensions
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator('feedback_type')
    @classmethod
    def validate_feedback_type(cls, v: str) -> str:
        allowed = {"routing", "distillation", "energy", "carbon", "helium"}
        if v not in allowed:
            raise ValueError(f"feedback_type must be one of {allowed}")
        return v

    @model_validator(mode='after')
    def check_helium_consistency(self) -> 'FeedbackEvent':
        if self.feedback_type == "helium" and self.helium_cost is None:
            raise ValueError("helium_cost must be provided when feedback_type='helium'")
        return self

    @field_validator('selected_action')
    @classmethod
    def validate_selected_action(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("selected_action cannot be empty")
        return v.strip()

    # --------------------------------------------------------------------------
    # Serialization helpers
    # --------------------------------------------------------------------------
    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dict for SQLite insertion (timestamp as float)."""
        data = self.model_dump()
        # Convert datetime to float timestamp
        data['timestamp'] = self.timestamp.timestamp()
        # Convert list fields to JSON strings for storage
        for field in ['teacher_ids', 'tags']:
            data[field] = json.dumps(data[field])
        # Convert resource_usage and metadata to JSON strings
        data['resource_usage'] = json.dumps(data['resource_usage'])
        data['metadata'] = json.dumps(data['metadata'])
        return data

    @classmethod
    def from_db_dict(cls, data: Dict[str, Any]) -> "FeedbackEvent":
        """Reconstruct from a DB dict (float timestamp)."""
        # Convert timestamp back to datetime
        if 'timestamp' in data and isinstance(data['timestamp'], (int, float)):
            data['timestamp'] = datetime.fromtimestamp(data['timestamp'])
        # Parse JSON fields
        for field in ['teacher_ids', 'tags']:
            if field in data and isinstance(data[field], str):
                data[field] = json.loads(data[field])
        if 'resource_usage' in data and isinstance(data['resource_usage'], str):
            data['resource_usage'] = json.loads(data['resource_usage'])
        if 'metadata' in data and isinstance(data['metadata'], str):
            data['metadata'] = json.loads(data['metadata'])
        return cls(**data)

    def to_json(self) -> str:
        """Serialize to JSON (datetime as ISO string)."""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> "FeedbackEvent":
        """Deserialize from JSON."""
        return cls.model_validate_json(json_str)

    # --------------------------------------------------------------------------
    # Helper to add structured metadata
    # --------------------------------------------------------------------------
    def set_metadata(self, key: str, value: Any) -> None:
        self.metadata[key] = value

    def get_metadata(self, key: str, default: Any = None) -> Any:
        return self.metadata.get(key, default)

    def add_tag(self, tag: str) -> None:
        if tag not in self.tags:
            self.tags.append(tag)
