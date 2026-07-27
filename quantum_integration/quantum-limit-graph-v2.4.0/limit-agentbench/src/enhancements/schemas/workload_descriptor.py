from pydantic import BaseModel, Field
from typing import Literal, Optional

class WorkloadDescriptor(BaseModel):
    """Descriptor for a workload/task."""
    task_type: Literal["training", "inference", "edge_sensing", "federated_round"]
    tokens: int = Field(..., ge=1)
    latency_target: float = Field(..., description="milliseconds")
    sector_emission_factor: Optional[float] = Field(None, description="kg CO₂ per $ revenue, if applicable")
    bio_mode: Literal["photosynthetic", "chemotactic", "none"] = "none"
    priority: Literal["accuracy", "green", "balanced"] = "balanced"

    class Config:
        schema_extra = {
            "example": {
                "task_type": "inference",
                "tokens": 512,
                "latency_target": 200.0,
                "sector_emission_factor": 0.03,
                "bio_mode": "photosynthetic",
                "priority": "green"
            }
        }
