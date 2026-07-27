# src/enhancements/schemas/node_descriptor.py
"""
Enhanced Node Descriptor v2.0.0
================================
Defines the structure of a compute node (edge, hotspot, cloud, lab, on_prem, vm, container)
with comprehensive fields for sustainability-aware routing, including energy, carbon,
helium, material, performance, cooling, and operational metrics.

Features:
- Expanded node types as Enum.
- Fields for performance (FLOPs, memory, storage), cooling, location, cost, health.
- Sustainability metrics: carbon intensity, helium connectivity, renewable fraction.
- Material footprint reference.
- Versioning and metadata extension.
- Helper methods for cost estimation and health evaluation.
- Pydantic validation with custom validators.
- Full docstrings and type hints.
"""

from enum import Enum
from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, field_validator

# ============================================================================
# Enums for node types and cooling types
# ============================================================================

class NodeType(str, Enum):
    """Enumeration of supported node types."""
    EDGE = "edge"
    HOTSPOT = "hotspot"
    CLOUD = "cloud"
    LAB = "lab"
    ON_PREM = "on_prem"
    VM = "vm"
    CONTAINER = "container"

class CoolingType(str, Enum):
    """Cooling system types."""
    AIR = "air"
    LIQUID = "liquid"
    CRYOGENIC = "cryogenic"
    NONE = "none"

class MaintenanceStatus(str, Enum):
    """Maintenance status of the node."""
    OPERATIONAL = "operational"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"
    OFFLINE = "offline"

# ============================================================================
# NodeDescriptor (Enhanced)
# ============================================================================

class NodeDescriptor(BaseModel):
    """
    Descriptor for a compute node, providing all necessary information
    for sustainability-aware routing and resource allocation.

    Fields:
        id: Unique node identifier.
        type: Type of the node.
        region: Geographic region (e.g., "us-east").
        region_carbon_intensity: Carbon intensity in kg CO₂/kWh.
        energy_per_token: Energy consumption per token (Joules).
        helium_connectivity_score: Connectivity score (0-1) for helium-based nodes.
        material_footprint_id: Reference to material footprint catalog (optional).
        uptime: Fraction of time the node is available (0-1).
        renewable_fraction: Fraction of energy from renewable sources (0-1).
        flops: Estimated FLOPs per second (optional).
        memory_gb: Available memory in GB (optional).
        storage_gb: Available storage in GB (optional).
        cooling_type: Cooling system type.
        network_latency_ms: Average network latency in milliseconds (optional).
        cost_per_hour_usd: Cost per hour in USD (optional).
        maintenance_status: Current maintenance status.
        last_updated: Timestamp of last update.
        hardware_model: Hardware model identifier (optional).
        manufacturer: Hardware manufacturer (optional).
        location_lat: Latitude of the node (optional).
        location_lon: Longitude of the node (optional).
        availability_zone: Cloud availability zone (optional).
        carbon_intensity_source: Source of carbon intensity data (optional).
        efficiency_score: Overall sustainability efficiency score (0-1, optional).
        version: Schema version.
        metadata: Additional custom data.
    """

    # Core identification
    id: str = Field(..., description="Unique node identifier")

    # Node type and location
    type: NodeType = Field(..., description="Type of the node")
    region: str = Field(..., description="Geographic region")
    location_lat: Optional[float] = Field(None, description="Latitude of the node")
    location_lon: Optional[float] = Field(None, description="Longitude of the node")
    availability_zone: Optional[str] = Field(None, description="Cloud availability zone")

    # Sustainability metrics
    region_carbon_intensity: float = Field(..., ge=0, description="kg CO₂/kWh")
    carbon_intensity_source: Optional[str] = Field(None, description="Source of carbon intensity data")
    energy_per_token: float = Field(..., gt=0, description="Joules per token")
    helium_connectivity_score: float = Field(0.5, ge=0, le=1, description="Connectivity score for helium-based nodes")
    material_footprint_id: Optional[str] = Field(None, description="Reference to material footprint catalog")
    renewable_fraction: float = Field(0.0, ge=0, le=1, description="Fraction of energy from renewables")
    efficiency_score: Optional[float] = Field(None, ge=0, le=1, description="Overall sustainability efficiency score")

    # Performance and resources
    flops: Optional[float] = Field(None, gt=0, description="Estimated FLOPs per second")
    memory_gb: Optional[float] = Field(None, gt=0, description="Available memory in GB")
    storage_gb: Optional[float] = Field(None, gt=0, description="Available storage in GB")

    # Cooling and network
    cooling_type: CoolingType = Field(CoolingType.AIR, description="Cooling system type")
    network_latency_ms: Optional[float] = Field(None, gt=0, description="Average network latency in milliseconds")

    # Operational metrics
    uptime: float = Field(1.0, ge=0, le=1, description="Fraction of time the node is available")
    cost_per_hour_usd: Optional[float] = Field(None, ge=0, description="Cost per hour in USD")
    maintenance_status: MaintenanceStatus = Field(MaintenanceStatus.OPERATIONAL, description="Maintenance status")
    last_updated: datetime = Field(default_factory=datetime.utcnow, description="Timestamp of last update")

    # Hardware information
    hardware_model: Optional[str] = Field(None, description="Hardware model identifier")
    manufacturer: Optional[str] = Field(None, description="Hardware manufacturer")

    # Schema version & extensibility
    version: str = Field("2.0.0", description="Schema version")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional custom data")

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator('region_carbon_intensity')
    @classmethod
    def validate_carbon_intensity(cls, v: float) -> float:
        if v < 0:
            raise ValueError("region_carbon_intensity must be non‑negative")
        return v

    @field_validator('energy_per_token')
    @classmethod
    def validate_energy_per_token(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("energy_per_token must be positive")
        return v

    @field_validator('location_lat')
    @classmethod
    def validate_latitude(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-90 <= v <= 90):
            raise ValueError("latitude must be between -90 and 90")
        return v

    @field_validator('location_lon')
    @classmethod
    def validate_longitude(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-180 <= v <= 180):
            raise ValueError("longitude must be between -180 and 180")
        return v

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def compute_energy_cost(self, tokens: int) -> float:
        """
        Compute the total energy cost for a workload with given tokens.

        Args:
            tokens: Number of tokens.

        Returns:
            Total energy in Joules.
        """
        return self.energy_per_token * tokens

    def compute_carbon_cost(self, energy_joules: float) -> float:
        """
        Compute the carbon cost for a given energy consumption.

        Args:
            energy_joules: Energy in Joules.

        Returns:
            Carbon emissions in kg CO₂.
        """
        # Convert Joules to kWh (1 J = 2.7778e-7 kWh)
        energy_kwh = energy_joules * 2.7778e-7
        return energy_kwh * self.region_carbon_intensity

    def get_health_score(self) -> float:
        """
        Compute a health score for the node (0-1, higher is better).

        Factors considered:
            - uptime
            - maintenance_status
            - efficiency_score (if available)
        """
        base = self.uptime
        if self.maintenance_status == MaintenanceStatus.OPERATIONAL:
            base *= 1.0
        elif self.maintenance_status == MaintenanceStatus.DEGRADED:
            base *= 0.7
        elif self.maintenance_status == MaintenanceStatus.MAINTENANCE:
            base *= 0.3
        else:  # OFFLINE
            base *= 0.0
        if self.efficiency_score is not None:
            base *= self.efficiency_score
        return max(0.0, min(1.0, base))

    def is_available(self) -> bool:
        """Return True if the node is operational or degraded."""
        return self.maintenance_status in (MaintenanceStatus.OPERATIONAL, MaintenanceStatus.DEGRADED)

    def to_dict(self, exclude_none: bool = False) -> Dict[str, Any]:
        """
        Convert the descriptor to a dictionary.

        Args:
            exclude_none: If True, omit fields with None value.

        Returns:
            Dictionary representation.
        """
        data = self.model_dump()
        if exclude_none:
            return {k: v for k, v in data.items() if v is not None}
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NodeDescriptor":
        """Create a NodeDescriptor from a dictionary."""
        return cls(**data)

    # ------------------------------------------------------------------
    # Configuration for Pydantic
    # ------------------------------------------------------------------
    class Config:
        schema_extra = {
            "example": {
                "id": "node-123",
                "type": "edge",
                "region": "us-east",
                "location_lat": 40.7128,
                "location_lon": -74.0060,
                "availability_zone": "us-east-1a",
                "region_carbon_intensity": 0.42,
                "carbon_intensity_source": "OS-Climate",
                "energy_per_token": 0.00005,
                "helium_connectivity_score": 0.92,
                "material_footprint_id": "gpu-a100",
                "renewable_fraction": 0.3,
                "efficiency_score": 0.85,
                "flops": 1.5e12,
                "memory_gb": 64,
                "storage_gb": 1024,
                "cooling_type": "liquid",
                "network_latency_ms": 50.0,
                "uptime": 0.99,
                "cost_per_hour_usd": 2.50,
                "maintenance_status": "operational",
                "hardware_model": "A100",
                "manufacturer": "NVIDIA",
                "version": "2.0.0",
                "metadata": {"owner": "team-alpha", "environment": "production"}
            }
        }


# ============================================================================
# Convenience factory
# ============================================================================

def create_node_descriptor(
    id: str,
    node_type: NodeType,
    region: str,
    region_carbon_intensity: float,
    energy_per_token: float,
    **kwargs
) -> NodeDescriptor:
    """
    Factory function to create a NodeDescriptor with sensible defaults.

    Args:
        id: Node identifier.
        node_type: Type of the node.
        region: Geographic region.
        region_carbon_intensity: Carbon intensity (kg CO₂/kWh).
        energy_per_token: Energy per token (Joules).
        **kwargs: Additional fields.

    Returns:
        A populated NodeDescriptor.
    """
    return NodeDescriptor(
        id=id,
        type=node_type,
        region=region,
        region_carbon_intensity=region_carbon_intensity,
        energy_per_token=energy_per_token,
        **kwargs
    )


# ============================================================================
# Example usage (if run directly)
# ============================================================================
if __name__ == "__main__":
    # Demonstrate creation and usage
    node = NodeDescriptor(
        id="node-001",
        type=NodeType.EDGE,
        region="us-east",
        region_carbon_intensity=0.42,
        energy_per_token=0.00005,
        helium_connectivity_score=0.92,
        uptime=0.98,
        renewable_fraction=0.4,
        cooling_type=CoolingType.LIQUID,
        hardware_model="A100",
        metadata={"rack": "R12"}
    )
    print(node.model_dump_json(indent=2))
    print(f"Health score: {node.get_health_score():.2f}")
    print(f"Energy cost for 512 tokens: {node.compute_energy_cost(512)} J")
    print(f"Carbon cost for that energy: {node.compute_carbon_cost(node.compute_energy_cost(512)):.4f} kg CO₂")
