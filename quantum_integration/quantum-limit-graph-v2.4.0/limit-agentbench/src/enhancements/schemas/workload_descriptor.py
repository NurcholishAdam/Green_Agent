# src/enhancements/schemas/workload_descriptor.py
"""
Enhanced Workload Descriptor v2.0.0
====================================
Defines the structure of a workload/task with comprehensive fields for
sustainability-aware routing, including energy, carbon, helium, data size,
model size, user/tenant information, and versioning.

Features:
- Expanded task types as Enum.
- Fields for estimated energy, carbon, helium, data/model sizes.
- User/tenant and tracing IDs.
- Versioning and metadata extension.
- Helper methods for cost estimation.
- Pydantic validation with custom validators.
- Full docstrings and type hints.
"""

from enum import Enum
from typing import Optional, Dict, Any, Literal
from datetime import datetime
from pydantic import BaseModel, Field, field_validator

# ============================================================================
# Enums for task types and urgency
# ============================================================================

class TaskType(str, Enum):
    """Enumeration of supported task types."""
    TRAINING = "training"
    INFERENCE = "inference"
    EDGE_SENSING = "edge_sensing"
    FEDERATED_ROUND = "federated_round"
    MULTIMODAL = "multimodal"
    DATA_PROCESSING = "data_processing"
    QUERY = "query"
    INFERENCE_BATCH = "inference_batch"
    TRAINING_DISTRIBUTED = "training_distributed"
    EDGE_COMPUTE = "edge_compute"

class Urgency(str, Enum):
    """Urgency levels for task prioritization."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class Priority(str, Enum):
    """Priority profiles for routing."""
    ACCURACY = "accuracy"
    GREEN = "green"
    BALANCED = "balanced"

class BioMode(str, Enum):
    """Bio-inspired operation modes."""
    PHOTOSYNTHETIC = "photosynthetic"
    CHEMOTACTIC = "chemotactic"
    NONE = "none"

# ============================================================================
# WorkloadDescriptor (Enhanced)
# ============================================================================

class WorkloadDescriptor(BaseModel):
    """
    Descriptor for a workload/task, providing all necessary information
    for sustainability-aware routing and resource allocation.

    Fields:
        task_id: Unique identifier for the task (optional).
        correlation_id: Correlation ID for tracing (optional).
        task_type: Type of the task.
        tokens: Number of tokens (for language tasks).
        latency_target: Target latency in milliseconds.
        deadline: Hard deadline (optional).
        urgency: Urgency level (default: MEDIUM).
        sector_emission_factor: kg CO₂ per $ revenue (optional).
        bio_mode: Bio-inspired mode (default: NONE).
        priority: Routing priority profile (default: BALANCED).
        estimated_energy_joules: Estimated energy consumption (optional).
        estimated_carbon_kg: Estimated carbon emissions (optional).
        helium_units: Estimated helium usage units (optional).
        data_size_bytes: Size of input data in bytes (optional).
        model_size_bytes: Size of model parameters (optional).
        user_id: User identifier for multi‑tenant support (optional).
        tenant_id: Tenant identifier (optional).
        version: Schema version (default: "2.0.0").
        metadata: Dictionary for additional custom data (optional).
    """

    # Core identification
    task_id: Optional[str] = Field(None, description="Unique task identifier")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for tracing")

    # Task characteristics
    task_type: TaskType = Field(..., description="Type of the workload")
    tokens: int = Field(..., ge=1, description="Number of tokens (for language tasks)")
    latency_target: float = Field(..., gt=0, description="Target latency in milliseconds")
    deadline: Optional[datetime] = Field(None, description="Hard deadline for the task")
    urgency: Urgency = Field(Urgency.MEDIUM, description="Urgency level")

    # Sustainability & cost
    sector_emission_factor: Optional[float] = Field(
        None,
        ge=0,
        description="kg CO₂ per $ revenue, if applicable"
    )
    estimated_energy_joules: Optional[float] = Field(
        None,
        ge=0,
        description="Estimated energy consumption in Joules"
    )
    estimated_carbon_kg: Optional[float] = Field(
        None,
        ge=0,
        description="Estimated carbon emissions in kg CO₂"
    )
    helium_units: Optional[float] = Field(
        None,
        ge=0,
        description="Estimated helium usage units"
    )

    # Resource sizing
    data_size_bytes: Optional[int] = Field(None, ge=0, description="Input data size in bytes")
    model_size_bytes: Optional[int] = Field(None, ge=0, description="Model size in bytes")

    # Multi‑tenant & user
    user_id: Optional[str] = Field(None, description="User identifier")
    tenant_id: Optional[str] = Field(None, description="Tenant identifier")

    # Bio‑inspired mode
    bio_mode: BioMode = Field(BioMode.NONE, description="Bio‑inspired operation mode")

    # Routing priority
    priority: Priority = Field(Priority.BALANCED, description="Routing priority profile")

    # Schema version & extensibility
    version: str = Field("2.0.0", description="Schema version")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional custom data")

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator('sector_emission_factor')
    @classmethod
    def validate_sector_emission_factor(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v < 0:
            raise ValueError("sector_emission_factor must be non‑negative")
        return v

    @field_validator('latency_target')
    @classmethod
    def validate_latency_target(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("latency_target must be positive")
        return v

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def compute_energy_cost(self, energy_per_token: float) -> float:
        """
        Compute the total energy cost for this workload.

        Args:
            energy_per_token: Energy per token (Joules) from a NodeDescriptor.

        Returns:
            Total energy in Joules.
        """
        return energy_per_token * self.tokens

    def compute_carbon_cost(self, carbon_intensity_kg_per_kwh: float) -> float:
        """
        Compute the carbon cost for this workload.

        Args:
            carbon_intensity_kg_per_kwh: Carbon intensity in kg CO₂/kWh.

        Returns:
            Carbon emissions in kg CO₂.
        """
        # Convert energy to kWh (1 J = 2.7778e-7 kWh)
        energy_kwh = self.tokens * 0.00001  # placeholder; actual should come from node
        return energy_kwh * carbon_intensity_kg_per_kwh

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
    def from_dict(cls, data: Dict[str, Any]) -> "WorkloadDescriptor":
        """Create a WorkloadDescriptor from a dictionary."""
        return cls(**data)

    def is_critical(self) -> bool:
        """Return True if the task urgency is CRITICAL."""
        return self.urgency == Urgency.CRITICAL

    def is_high_priority(self) -> bool:
        """Return True if urgency is HIGH or CRITICAL."""
        return self.urgency in (Urgency.HIGH, Urgency.CRITICAL)

    # ------------------------------------------------------------------
    # Configuration for Pydantic
    # ------------------------------------------------------------------
    class Config:
        schema_extra = {
            "example": {
                "task_id": "task-12345",
                "correlation_id": "corr-67890",
                "task_type": "inference",
                "tokens": 512,
                "latency_target": 200.0,
                "deadline": "2025-12-31T23:59:59Z",
                "urgency": "medium",
                "sector_emission_factor": 0.03,
                "estimated_energy_joules": 0.05,
                "estimated_carbon_kg": 0.0002,
                "helium_units": 0.001,
                "data_size_bytes": 10240,
                "model_size_bytes": 5242880,
                "user_id": "user-007",
                "tenant_id": "tenant-acme",
                "bio_mode": "photosynthetic",
                "priority": "green",
                "version": "2.0.0",
                "metadata": {"source": "api-gateway", "region": "us-east"}
            }
        }


# ============================================================================
# Convenience factory
# ============================================================================

def create_workload_descriptor(
    task_type: TaskType,
    tokens: int,
    latency_target: float,
    **kwargs
) -> WorkloadDescriptor:
    """
    Factory function to create a WorkloadDescriptor with sensible defaults.

    Args:
        task_type: Type of the task.
        tokens: Number of tokens.
        latency_target: Target latency in ms.
        **kwargs: Additional fields.

    Returns:
        A populated WorkloadDescriptor.
    """
    return WorkloadDescriptor(
        task_type=task_type,
        tokens=tokens,
        latency_target=latency_target,
        **kwargs
    )


# ============================================================================
# Example usage (if run directly)
# ============================================================================
if __name__ == "__main__":
    # Demonstrate creation and usage
    wl = WorkloadDescriptor(
        task_id="task-001",
        task_type=TaskType.INFERENCE,
        tokens=1024,
        latency_target=150.0,
        urgency=Urgency.HIGH,
        priority=Priority.GREEN,
        bio_mode=BioMode.PHOTOSYNTHETIC,
        estimated_energy_joules=0.1,
        estimated_carbon_kg=0.0005,
        user_id="user-123",
        metadata={"region": "eu-west"}
    )
    print(wl.model_dump_json(indent=2))
    print(f"Is critical? {wl.is_critical()}")
    print(f"Energy cost: {wl.compute_energy_cost(0.00005)} J")
