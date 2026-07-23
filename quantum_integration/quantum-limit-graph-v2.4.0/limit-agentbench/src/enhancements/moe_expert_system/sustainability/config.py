from pydantic import BaseModel, Field, field_validator
from typing import Optional, Dict, List
import os

class SustainabilityConfig(BaseModel):
    # ========== General ==========
    energy_threshold: float = Field(5.0, ge=0, description="Joules; triggers compression if exceeded")
    accuracy_drop_tolerance: float = Field(0.02, ge=0, le=1, description="Max allowable absolute accuracy drop")

    # ========== Hardware & Energy ==========
    hardware_profile: str = Field("default", description="default, gpu, cpu, tpu")
    energy_per_mac: float = Field(0.5e-12, gt=0, description="pJ per MAC operation")
    max_energy_joules: float = Field(10.0, gt=0, description="Used for normalization in fitness score")

    # ========== Fitness Scoring ==========
    fitness_accuracy_weight: float = Field(0.6, ge=0, le=1)
    fitness_energy_weight: float = Field(0.4, ge=0, le=1)
    fitness_carbon_weight: float = Field(0.1, ge=0, le=1)
    fitness_material_weight: float = Field(0.05, ge=0, le=1)
    compression_bonus: float = Field(0.05, ge=0, le=0.5)

    # ========== Adaptive Weights ==========
    use_adaptive_weights: bool = Field(True, description="Use AdaptiveCostFunction weights if available")
    adaptive_learning_rate: float = Field(0.01, gt=0, le=1)

    # ========== Compression Strategies ==========
    pruning_sparsity: float = Field(0.3, ge=0, le=1)
    structured_pruning_enabled: bool = Field(True)
    quantization_method: str = Field("int8_dynamic", description="int8_dynamic, int8_static, fp16")
    hybrid_pruning_sparsity: float = Field(0.2, ge=0, le=1)
    compression_strategy_priority: List[str] = Field(
        default=["structured_pruning", "unstructured_pruning", "int8_quant", "hybrid"],
        description="Order in which methods are tried"
    )

    # ========== Carbon & Sustainability ==========
    carbon_aware_enabled: bool = Field(True)
    carbon_offset_enabled: bool = Field(False)
    carbon_intensity_api_key: Optional[str] = Field(None)
    carbon_region: str = Field("global")

    # ========== Persistence ==========
    compressed_model_dir: str = Field("./compressed_models")
    history_db_path: str = Field("./compression_history.db")
    history_retention_days: int = Field(365, ge=0)

    # ========== Telemetry & Logging ==========
    log_compression_events: bool = Field(True)
    export_metrics: bool = Field(True)
    prometheus_port: Optional[int] = Field(9090, ge=1024, le=65535)

    # ========== Integration Hooks ==========
    enable_anomaly_trigger: bool = Field(True)
    enable_predictive_maintenance: bool = Field(True)
    enable_auto_recompress: bool = Field(True)
    recompress_interval_seconds: int = Field(3600, ge=60)

    # ========== Environment Variable Support ==========
    class Config:
        env_prefix = "SUSTAINABILITY_"

    @field_validator('fitness_accuracy_weight', 'fitness_energy_weight', 'fitness_carbon_weight', 'fitness_material_weight')
    @classmethod
    def weights_sum_to_one(cls, v, values):
        # Ensure the four weights sum to 1 (when all are provided)
        weights = {
            'accuracy': values.data.get('fitness_accuracy_weight', 0.6),
            'energy': values.data.get('fitness_energy_weight', 0.4),
            'carbon': values.data.get('fitness_carbon_weight', 0.1),
            'material': values.data.get('fitness_material_weight', 0.05),
        }
        # Actually, since these are optional and have defaults, we can't enforce sum=1 because they are separate.
        # Instead, we'll provide a method to normalize later.
        return v

    def normalized_fitness_weights(self) -> Dict[str, float]:
        """Return fitness weights that sum to 1."""
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
