from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from typing import Optional, Dict, List, Literal
from enum import Enum
import os
import logging

logger = logging.getLogger(__name__)

# ==============================================
# Enums for compression methods and quantization
# ==============================================

class CompressionMethod(str, Enum):
    STRUCTURED_PRUNING = "structured_pruning"
    UNSTRUCTURED_PRUNING = "unstructured_pruning"
    INT8_QUANT = "int8_quant"
    HYBRID = "hybrid"
    SVD = "svd"

class QuantizationMethod(str, Enum):
    INT8_DYNAMIC = "int8_dynamic"
    INT8_STATIC = "int8_static"
    FP16 = "fp16"

# ==============================================
# MOPD Configuration (NEW)
# ==============================================

class MOPDConfig(BaseModel):
    """
    Configuration for Multi‑Objective Pareto Decision (MOPD) in model compression.
    """
    enabled: bool = Field(
        True,
        description="Enable MOPD‑aware compression selection"
    )
    objective_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            'accuracy': 0.4,
            'energy': 0.3,
            'carbon': 0.2,
            'material': 0.1,
        },
        description="Weights for scalarising Pareto front (must sum to 1)"
    )
    grid_resolution: int = Field(
        5,
        ge=2,
        description="Number of discrete points for sampling (e.g., for weight combinations)"
    )
    enable_cost_benefit: bool = Field(True)
    enable_predictive: bool = Field(True)

    @model_validator(mode='after')
    def check_weights(self):
        """Ensure objective weights sum to 1."""
        total = sum(self.objective_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"MOPD objective weights must sum to 1, got {total}")
        return self

# ==============================================
# Enhanced Sustainability Configuration
# ==============================================

class SustainabilityConfig(BaseModel):
    """
    Configuration for sustainability‑aware model compression.
    All fields can be overridden via environment variables with prefix SUSTAINABILITY_.
    """

    # ========== General ==========
    energy_threshold_joules: float = Field(
        5.0,
        ge=0,
        description="Energy per inference in Joules; compression triggered if exceeded"
    )
    accuracy_drop_tolerance: float = Field(
        0.02,
        ge=0,
        le=1,
        description="Max allowable absolute accuracy drop after compression"
    )

    # ========== Hardware & Energy ==========
    hardware_profile: str = Field(
        "default",
        description="Name of the hardware profile to use (must exist in hardware_profiles)"
    )
    hardware_profiles: Dict[str, float] = Field(
        default_factory=lambda: {
            'default': 0.5e-12,
            'gpu': 0.3e-12,
            'cpu': 0.5e-12,
            'tpu': 0.2e-12,
        },
        description="Mapping of profile names to energy per MAC (Joules)"
    )
    energy_normalization_max_joules: Optional[float] = Field(
        None,
        description="Maximum energy per inference used for normalization; if None, uses 2× energy_threshold_joules"
    )

    # ========== Fitness Scoring ==========
    fitness_accuracy_weight: float = Field(0.6, ge=0, le=1)
    fitness_energy_weight: float = Field(0.4, ge=0, le=1)
    fitness_carbon_weight: float = Field(0.1, ge=0, le=1)
    fitness_material_weight: float = Field(0.05, ge=0, le=1)
    compression_bonus: float = Field(0.05, ge=0, le=0.5)

    # ========== Adaptive Weights ==========
    use_adaptive_weights: bool = Field(
        True,
        description="Use AdaptiveCostFunction weights if available"
    )
    adaptive_learning_rate: float = Field(
        0.01,
        gt=0,
        le=1,
        description="Learning rate for adaptive weight updates"
    )

    # ========== Compression Strategies ==========
    pruning_sparsity: float = Field(0.3, ge=0, le=1)
    structured_pruning_enabled: bool = Field(True)
    quantization_method: QuantizationMethod = Field(QuantizationMethod.INT8_DYNAMIC)
    hybrid_pruning_sparsity: float = Field(0.2, ge=0, le=1)
    svd_rank_factor: float = Field(
        0.5,
        gt=0,
        le=1,
        description="Fraction of original rank to keep in SVD"
    )
    compression_strategy_priority: List[CompressionMethod] = Field(
        default=[
            CompressionMethod.STRUCTURED_PRUNING,
            CompressionMethod.UNSTRUCTURED_PRUNING,
            CompressionMethod.INT8_QUANT,
            CompressionMethod.HYBRID,
            CompressionMethod.SVD,
        ],
        description="Order in which compression methods are tried"
    )

    # ========== Carbon & Sustainability ==========
    carbon_aware_enabled: bool = Field(True)
    carbon_offset_enabled: bool = Field(False)
    carbon_intensity_api_key: Optional[str] = Field(
        None,
        description="API key for Electricity Map (required if carbon_aware_enabled)"
    )
    carbon_region: str = Field("global", description="Region code for carbon intensity API")

    # ========== Persistence ==========
    compressed_model_dir: str = Field("./compressed_models")
    history_db_path: str = Field("./compression_history.db")
    history_retention_days: int = Field(365, ge=0)

    # ========== Telemetry & Logging ==========
    log_compression_events: bool = Field(True)
    export_metrics: bool = Field(True)
    prometheus_port: Optional[int] = Field(
        9090,
        ge=1024,
        le=65535,
        description="Port for Prometheus metrics; required if export_metrics=True"
    )

    # ========== Integration Hooks ==========
    enable_anomaly_trigger: bool = Field(True)
    enable_predictive_maintenance: bool = Field(True)
    enable_auto_recompress: bool = Field(True)
    recompress_interval_seconds: int = Field(3600, ge=60)

    # ========== Versioning ==========
    version: str = Field("1.1", description="Configuration schema version")

    # ========== MOPD Configuration (NEW) ==========
    mopd: MOPDConfig = Field(
        default_factory=MOPDConfig,
        description="Multi‑Objective Pareto Decision settings"
    )

    # Pydantic v2 configuration
    model_config = ConfigDict(env_prefix="SUSTAINABILITY_")

    # ---------- Validation ----------
    @model_validator(mode='after')
    def validate_carbon_api_key(self):
        """Ensure API key is provided when carbon awareness is enabled."""
        if self.carbon_aware_enabled and not self.carbon_intensity_api_key:
            raise ValueError("carbon_intensity_api_key is required when carbon_aware_enabled is True")
        return self

    @model_validator(mode='after')
    def validate_prometheus_port(self):
        """Ensure Prometheus port is set when metrics export is enabled."""
        if self.export_metrics and self.prometheus_port is None:
            raise ValueError("prometheus_port is required when export_metrics is True")
        return self

    @model_validator(mode='after')
    def validate_recompress_interval(self):
        """Ensure recompression interval is reasonable when auto‑recompress is enabled."""
        if self.enable_auto_recompress and self.recompress_interval_seconds < 60:
            raise ValueError("recompress_interval_seconds must be at least 60 when enable_auto_recompress is True")
        return self

    @model_validator(mode='after')
    def ensure_hardware_profile_exists(self):
        """Ensure the selected hardware profile is defined in hardware_profiles."""
        if self.hardware_profile not in self.hardware_profiles:
            raise ValueError(f"hardware_profile '{self.hardware_profile}' not found in hardware_profiles")
        return self

    @model_validator(mode='after')
    def normalize_fitness_weights(self):
        """
        Normalize fitness weights to sum to 1 and warn if they don't.
        The original weights are preserved; the normalized versions are used in fitness scoring.
        """
        weights = [
            self.fitness_accuracy_weight,
            self.fitness_energy_weight,
            self.fitness_carbon_weight,
            self.fitness_material_weight,
        ]
        total = sum(weights)
        if abs(total - 1.0) > 1e-6:
            logger.warning(
                f"Fitness weights sum to {total:.4f}, not 1. They will be normalized automatically."
            )
        return self

    # ---------- Utility Methods ----------
    def get_energy_per_mac(self) -> float:
        """Get the energy per MAC for the selected hardware profile."""
        return self.hardware_profiles.get(self.hardware_profile, self.hardware_profiles['default'])

    def get_energy_normalization_max(self) -> float:
        """Get the max energy value used for normalization."""
        if self.energy_normalization_max_joules is not None:
            return self.energy_normalization_max_joules
        return self.energy_threshold_joules * 2.0

    def normalized_fitness_weights(self) -> Dict[str, float]:
        """Return fitness weights that sum to 1, based on the configured values."""
        weights = {
            'accuracy': self.fitness_accuracy_weight,
            'energy': self.fitness_energy_weight,
            'carbon': self.fitness_carbon_weight,
            'material': self.fitness_material_weight,
        }
        total = sum(weights.values())
        if total > 0:
            return {k: v / total for k, v in weights.items()}
        return weights

    def get_mopd_weights(self) -> Dict[str, float]:
        """Return the MOPD objective weights (for Pareto selection)."""
        return self.mopd.objective_weights

    def is_mopd_enabled(self) -> bool:
        """Return whether MOPD is enabled."""
        return self.mopd.enabled

    # ---------- Serialization Helpers ----------
    def to_dict(self) -> Dict:
        """Export configuration as a dictionary (excluding None values)."""
        return self.model_dump(exclude_none=True)

    @classmethod
    def from_dict(cls, data: Dict) -> "SustainabilityConfig":
        """Create a config instance from a dictionary."""
        return cls(**data)

    @classmethod
    def from_env(cls) -> "SustainabilityConfig":
        """Load configuration from environment variables (prefix SUSTAINABILITY_)."""
        return cls()

# ==============================================
# Convenience exports
# ==============================================

__all__ = [
    "SustainabilityConfig",
    "CompressionMethod",
    "QuantizationMethod",
    "MOPDConfig",
]
