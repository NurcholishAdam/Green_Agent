"""
Canonical MOPD event schema.
Used by all feedback collectors, adaptive cost functions, and audit trails.
"""
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional, Dict, Any
import uuid

class FeedbackEvent(BaseModel):
    """Standardized feedback event for Green Agent."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.now)
    task_id: str
    model_id: Optional[str] = None
    teacher_id: Optional[str] = None
    selected_action: str
    quality_score: float = Field(ge=0.0, le=1.0)
    latency_ms: float = Field(ge=0.0)
    energy_joules: float = Field(ge=0.0)
    carbon_g: float = Field(ge=0.0)
    helium_cost: Optional[float] = None
    resource_usage: Dict[str, float] = Field(default_factory=dict)
    distillation_loss: Optional[float] = None
    feedback_type: str  # "routing", "distillation", "energy", "carbon"
    adaptive_cost_value: float
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator('feedback_type')
    @classmethod
    def validate_type(cls, v: str) -> str:
        allowed = {"routing", "distillation", "energy", "carbon", "helium"}
        if v not in allowed:
            raise ValueError(f"feedback_type must be one of {allowed}")
        return v

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dict for SQLite insertion."""
        return self.model_dump()
