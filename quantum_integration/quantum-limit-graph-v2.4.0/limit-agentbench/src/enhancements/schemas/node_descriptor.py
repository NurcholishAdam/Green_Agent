from pydantic import BaseModel, Field, validator
from typing import Optional, Literal

class NodeDescriptor(BaseModel):
    """Descriptor for a compute node (edge, hotspot, cloud, lab)."""
    id: str
    type: Literal["edge", "hotspot", "cloud", "lab"]
    region: str
    region_carbon_intensity: float = Field(..., description="kg CO₂/kWh")
    energy_per_token: float = Field(..., description="Joules per token")
    helium_connectivity_score: float = Field(0.5, ge=0, le=1)
    material_footprint_id: Optional[str] = None
    uptime: float = 1.0
    renewable_fraction: float = 0.0

    @validator('region_carbon_intensity')
    def intensity_positive(cls, v):
        if v < 0:
            raise ValueError('carbon intensity must be non-negative')
        return v

    class Config:
        schema_extra = {
            "example": {
                "id": "node-123",
                "type": "edge",
                "region": "us-east",
                "region_carbon_intensity": 0.42,
                "energy_per_token": 0.00005,
                "helium_connectivity_score": 0.92,
                "material_footprint_id": "gpu-a100"
            }
        }
