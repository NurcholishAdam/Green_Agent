# sustainability/__init__.py
"""
Enhanced Sustainability-Aware Model Compression and Pruning Module
Single-file drop-in for Green_Agent MoE system.

Includes:
- Pydantic configuration
- Adaptive fitness scorer with carbon, material, and adaptive weights
- Expert profile with sustainability metrics
- Integration with AdaptiveCostFunction
- Factory function for easy setup
"""

import torch
import torch.nn.utils.prune as prune
from torch.quantization import quantize_dynamic
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List
import logging
import os
import json
import asyncio
import sqlite3
import hashlib
import time
import copy
from datetime import datetime
from pathlib import Path
import numpy as np

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- thop for FLOPs ----------
try:
    from thop import profile
    THOP_AVAILABLE = True
except ImportError:
    THOP_AVAILABLE = False

# ---------- Logging ----------
logger = logging.getLogger(__name__)

# ==============================================
# 1. CONFIGURATION (Pydantic validated)
# ==============================================

class SustainabilityConfig(BaseModel):
    """Configuration for sustainability‑aware compression and scoring."""
    # Triggers compression if full inference energy exceeds this (Joules)
    energy_threshold: float = Field(5.0, ge=0)
    # Max allowable accuracy drop (absolute difference)
    accuracy_drop_tolerance: float = Field(0.02, ge=0, le=1)
    # Energy estimation coefficient (pJ per MAC operation)
    energy_per_mac: float = Field(0.5e-12, gt=0)
    # Fitness weighting (defaults if no adaptive cost function)
    fitness_accuracy_weight: float = Field(0.6, ge=0, le=1)
    fitness_energy_weight: float = Field(0.4, ge=0, le=1)
    # Additional weights for carbon and material (will be normalized)
    fitness_carbon_weight: float = Field(0.1, ge=0, le=1)
    fitness_material_weight: float = Field(0.05, ge=0, le=1)
    # Max energy for normalization (Joules)
    max_energy_joules: float = Field(10.0, gt=0)
    # Compression bonus
    compression_bonus: float = Field(0.05, ge=0, le=0.5)
    # Pruning sparsity levels
    pruning_sparsity: float = Field(0.3, ge=0, le=1)
    hybrid_pruning_sparsity: float = Field(0.2, ge=0, le=1)
    # Hardware profile (for energy estimation)
    hardware_profile: str = Field("default")
    # Compression storage directory
    compressed_model_dir: str = Field("./compressed_models")
    # History database path
    history_db_path: str = Field("./compression_history.db")
    # Re‑compression interval (seconds); 0 = disabled
    recompress_interval: int = Field(0, ge=0)
    # Whether to trigger compression on anomaly
    anomaly_trigger_enabled: bool = True
    # Whether to use adaptive weights from AdaptiveCostFunction
    use_adaptive_weights: bool = True

    @field_validator('fitness_accuracy_weight', 'fitness_energy_weight', 'fitness_carbon_weight', 'fitness_material_weight')
    @classmethod
    def weights_non_negative(cls, v):
        if v < 0:
            raise ValueError("Weights must be non-negative")
        return v

    class Config:
        env_prefix = "SUSTAINABILITY_"

# Global config instance (can be overridden)
SUSTAINABILITY_CONFIG = SustainabilityConfig()

# ==============================================
# 2. EXPERT PROFILE EXTENSION
# ==============================================

@dataclass
class SustainabilityAwareExpertProfile:
    """
    Extended ExpertProfile with sustainability metrics.
    """
    expert_id: str
    model_path: Optional[str] = None

    compressed_flag: bool = False
    compression_method: Optional[str] = None
    energy_per_inference_full: float = float('inf')
    energy_per_inference_compressed: Optional[float] = None
    accuracy_full: float = 0.0
    accuracy_compressed: Optional[float] = None
    sustainability_fitness_score: float = 0.0
    # New fields
    carbon_savings_kg: float = 0.0
    material_index: float = 0.0
    last_compressed_at: Optional[datetime] = None
    compression_history: List[Dict] = field(default_factory=list)


# ==============================================
# 3. ENHANCED SUSTAINABILITY FITNESS SCORER
# ==============================================

class SustainabilityFitnessScorer:
    """
    Computes a multi‑objective sustainability fitness score for an expert.
    Higher score = more sustainable (accurate, energy‑efficient, low carbon, low material impact).

    The score is a weighted sum of normalized components:
        - accuracy (higher is better)
        - energy efficiency (lower energy per inference is better)
        - carbon savings (kg CO₂ saved vs. uncompressed baseline)
        - material index (lower is better)

    Weights can be provided via config or dynamically from an AdaptiveCostFunction.
    """

    def __init__(
        self,
        config: Optional[SustainabilityConfig] = None,
        adaptive_cost_function: Optional[Any] = None,  # AdaptiveCostFunction
    ):
        """
        Args:
            config: SustainabilityConfig instance (if None, uses global SUSTAINABILITY_CONFIG)
            adaptive_cost_function: If provided and config.use_adaptive_weights is True,
                weights will be taken from this function's `weights` dictionary.
        """
        self.config = config or SUSTAINABILITY_CONFIG
        self.adaptive_cost = adaptive_cost_function

        # Mapping of component names to adaptive cost function keys
        self._adaptive_map = {
            'accuracy': 'zeta',   # 1 - accuracy (inverted in the cost function)
            'energy': 'alpha',    # energy weight
            'carbon': 'beta',     # carbon weight
            'material': 'delta',  # material weight
        }

    def compute(self, profile: SustainabilityAwareExpertProfile) -> float:
        """
        Compute the sustainability fitness score for an expert.

        Returns:
            float in [0, 1] (higher is better)
        """
        # 1. Gather metrics
        acc = profile.accuracy_compressed if profile.compressed_flag else profile.accuracy_full
        energy = profile.energy_per_inference_compressed if profile.compressed_flag else profile.energy_per_inference_full
        carbon_savings = getattr(profile, 'carbon_savings_kg', 0.0)
        material_index = getattr(profile, 'material_index', 0.0)

        # 2. Validate and handle missing values
        if acc is None:
            logger.warning(f"Expert {profile.expert_id} has no accuracy; defaulting to 0.5")
            acc = 0.5
        if energy is None or energy == float('inf'):
            logger.warning(f"Expert {profile.expert_id} has no energy data; defaulting to max energy")
            energy = self.config.max_energy_joules

        # 3. Normalize components to [0, 1] (higher = better)
        # Accuracy: already in [0, 1]
        # Energy: 0 = max (bad), so invert: 1 - (energy / max_energy)
        energy_norm = max(0.0, 1.0 - (energy / self.config.max_energy_joules))
        # Carbon savings: cap at 0.1 kg CO₂ (scale to 0-1)
        carbon_norm = min(1.0, carbon_savings / 0.1)
        # Material index: assume index in [0, 1]; invert so lower is better.
        material_norm = max(0.0, 1.0 - material_index)

        # 4. Get weights (adaptive or fixed)
        weights = self._get_weights()

        # 5. Compute weighted sum
        score = (
            weights['accuracy'] * acc +
            weights['energy'] * energy_norm +
            weights['carbon'] * carbon_norm +
            weights['material'] * material_norm
        )

        # 6. Add compression bonus (reward for being compressed)
        if profile.compressed_flag:
            score += self.config.compression_bonus

        # 7. Clamp to [0, 1]
        score = max(0.0, min(1.0, score))

        # 8. Store in profile for later use
        profile.sustainability_fitness_score = score
        return score

    def _get_weights(self) -> Dict[str, float]:
        """
        Return current weights.
        If adaptive_cost is available and use_adaptive_weights is True,
        map the adaptive keys to component names and normalize.
        Otherwise, use the fixed weights from config.
        """
        if self.adaptive_cost and self.config.use_adaptive_weights:
            # Map adaptive keys to component names
            adaptive_weights = self.adaptive_cost.weights
            mapped = {}
            for comp, adaptive_key in self._adaptive_map.items():
                # If the adaptive key exists, use it; otherwise fallback to config.
                mapped[comp] = adaptive_weights.get(adaptive_key, getattr(self.config, f'fitness_{comp}_weight', 0.0))
            # Normalize to sum to 1
            total = sum(mapped.values())
            if total > 0:
                for k in mapped:
                    mapped[k] /= total
            return mapped
        else:
            # Use fixed weights from config
            weights = {
                'accuracy': self.config.fitness_accuracy_weight,
                'energy': self.config.fitness_energy_weight,
                'carbon': self.config.fitness_carbon_weight,
                'material': self.config.fitness_material_weight,
            }
            total = sum(weights.values())
            if total > 0:
                for k in weights:
                    weights[k] /= total
            return weights

# ==============================================
# 4. CORE COMPRESSOR (abridged – kept for completeness)
# ==============================================

class SustainabilityCompressor:
    def __init__(self, model: torch.nn.Module, profile: SustainabilityAwareExpertProfile,
                 config: SustainabilityConfig = None,
                 telemetry=None, carbon_manager=None,
                 history_manager=None, storage=None):
        self.model = model
        self.profile = profile
        self.config = config or SUSTAINABILITY_CONFIG
        # ... (other attributes, see previous full implementation)
        # For brevity, we omit the full compressor; it would be identical to the enhanced version.
        # We include a placeholder to show it belongs.
        pass

    # (All compression methods go here)
    pass

# ==============================================
# 5. MLOPS PIPELINE EXTENSION (abridged)
# ==============================================

class MLOpsPipelineExtension:
    def __init__(self, pipeline: Any, config: SustainabilityConfig = None,
                 telemetry=None, carbon_manager=None, anomaly_detector=None):
        # ... (similar to previous)
        pass

# ==============================================
# 6. ROUTER INTEGRATION (using the enhanced scorer)
# ==============================================

class SustainabilityAwareRouter:
    def __init__(self, base_router: Any, scorer: Optional[SustainabilityFitnessScorer] = None):
        self.base_router = base_router
        self.scorer = scorer or SustainabilityFitnessScorer()

    def route(self, query: Any, required_accuracy: float = 0.90) -> Any:
        candidates = self.base_router.get_all_experts(query)

        valid_candidates = []
        for exp_id, profile in candidates:
            acc = profile.accuracy_compressed if profile.compressed_flag else profile.accuracy_full
            if acc >= required_accuracy:
                valid_candidates.append((exp_id, profile))

        if not valid_candidates:
            return self.base_router.route(query)

        # Compute scores using the enhanced scorer
        for exp_id, profile in valid_candidates:
            self.scorer.compute(profile)

        best_exp_id, best_profile = max(valid_candidates, key=lambda x: x[1].sustainability_fitness_score)

        if best_profile.compressed_flag:
            return self.base_router.load_compressed_model(best_exp_id)
        else:
            return self.base_router.load_full_model(best_exp_id)

# ==============================================
# 7. CONVENIENCE FACTORY
# ==============================================

def create_sustainability_module(
    pipeline: Any,
    config: Optional[SustainabilityConfig] = None,
    adaptive_cost_function: Optional[Any] = None,
    telemetry: Optional[Any] = None,
    carbon_manager: Optional[Any] = None,
    anomaly_detector: Optional[Any] = None,
):
    """
    Factory to create all sustainability components with the enhanced fitness scorer.
    """
    config = config or SUSTAINABILITY_CONFIG
    scorer = SustainabilityFitnessScorer(config, adaptive_cost_function)
    # ... (create compressor, pipeline extension, etc.)
    return {
        'scorer': scorer,
        'router': SustainabilityAwareRouter,
        # other components
    }

# ==============================================
# 8. EXPORTS
# ==============================================

__all__ = [
    "SUSTAINABILITY_CONFIG",
    "SustainabilityAwareExpertProfile",
    "SustainabilityCompressor",
    "SustainabilityFitnessScorer",
    "MLOpsPipelineExtension",
    "SustainabilityAwareRouter",
    "create_sustainability_module",
]

# ==============================================
# 9. EXAMPLE USAGE
# ==============================================

if __name__ == "__main__":
    # Demo of the enhanced scorer
    config = SustainabilityConfig()
    scorer = SustainabilityFitnessScorer(config)

    # Mock profile
    profile = SustainabilityAwareExpertProfile(expert_id="expert_1")
    profile.accuracy_full = 0.92
    profile.energy_per_inference_full = 8.0
    profile.carbon_savings_kg = 0.05
    profile.material_index = 0.2
    profile.compressed_flag = True
    profile.accuracy_compressed = 0.90
    profile.energy_per_inference_compressed = 4.0

    score = scorer.compute(profile)
    print(f"Sustainability fitness score: {score:.4f}")
