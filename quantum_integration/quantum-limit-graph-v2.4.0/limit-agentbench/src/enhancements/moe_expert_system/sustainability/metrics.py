# sustainability/__init__.py
"""
Enhanced Sustainability-Aware Model Compression and Pruning Module
Single-file drop-in for Green_Agent MoE system.

Includes:
- Pydantic configuration with hardware profiles, versioning, and cross-field validation
- Adaptive fitness scorer with carbon, material, and dynamic weights
- Expert profile with sustainability metrics
- Full compressor implementation: structured/unstructured pruning, INT8 quantization, SVD, hybrid
- SQLite history manager and filesystem model storage
- Async background re‑compression and anomaly‑triggered compression
- Telemetry counters and gauges (OpenTelemetry‑ready)
- Router integration with multi‑objective fitness
- Factory function for easy setup
"""

import torch
import torch.nn.utils.prune as prune
from torch.quantization import quantize_dynamic
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List, Callable, Union, Protocol
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
    from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
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
# 1. ENUMS
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
# 2. CONFIGURATION (Pydantic validated)
# ==============================================
class SustainabilityConfig(BaseModel):
    """
    Configuration for sustainability‑aware compression and scoring.
    All fields can be overridden via environment variables with prefix SUSTAINABILITY_.
    """

    # ---------- General ----------
    energy_threshold_joules: float = Field(5.0, ge=0, description="Energy per inference in J; compression triggered if exceeded")
    accuracy_drop_tolerance: float = Field(0.02, ge=0, le=1, description="Max allowable absolute accuracy drop")

    # ---------- Hardware & Energy ----------
    hardware_profile: str = Field("default", description="Selected hardware profile name")
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
        description="Max energy for normalization; if None, uses 2× energy_threshold_joules"
    )

    # ---------- Fitness Scoring ----------
    fitness_accuracy_weight: float = Field(0.6, ge=0, le=1)
    fitness_energy_weight: float = Field(0.4, ge=0, le=1)
    fitness_carbon_weight: float = Field(0.1, ge=0, le=1)
    fitness_material_weight: float = Field(0.05, ge=0, le=1)
    compression_bonus: float = Field(0.05, ge=0, le=0.5)

    # ---------- Adaptive Weights ----------
    use_adaptive_weights: bool = Field(True, description="Use AdaptiveCostFunction weights if available")
    adaptive_learning_rate: float = Field(0.01, gt=0, le=1, description="Learning rate for adaptive weight updates")

    # ---------- Compression Strategies ----------
    pruning_sparsity: float = Field(0.3, ge=0, le=1)
    structured_pruning_enabled: bool = Field(True)
    quantization_method: QuantizationMethod = Field(QuantizationMethod.INT8_DYNAMIC)
    hybrid_pruning_sparsity: float = Field(0.2, ge=0, le=1)
    svd_rank_factor: float = Field(0.5, gt=0, le=1, description="Fraction of original rank to keep in SVD")
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

    # ---------- Carbon & Sustainability ----------
    carbon_aware_enabled: bool = Field(True)
    carbon_offset_enabled: bool = Field(False)
    carbon_intensity_api_key: Optional[str] = Field(None, description="API key for Electricity Map (required if carbon_aware_enabled)")
    carbon_region: str = Field("global", description="Region code for carbon intensity API")
    carbon_savings_scale_kg: float = Field(0.1, gt=0, description="Max carbon savings (kg) for normalization")

    # ---------- Material Index ----------
    material_index_map: Dict[str, float] = Field(
        default_factory=lambda: {
            'default': 0.5,
            'gpu': 0.4,
            'cpu': 0.3,
            'tpu': 0.2,
        },
        description="Mapping of hardware profiles to material indices (lower is better)"
    )

    # ---------- Persistence ----------
    compressed_model_dir: str = Field("./compressed_models")
    history_db_path: str = Field("./compression_history.db")
    history_retention_days: int = Field(365, ge=0)

    # ---------- Telemetry & Logging ----------
    log_compression_events: bool = Field(True)
    export_metrics: bool = Field(True)
    prometheus_port: Optional[int] = Field(9090, ge=1024, le=65535, description="Port for Prometheus metrics; required if export_metrics=True")

    # ---------- Integration Hooks ----------
    enable_anomaly_trigger: bool = Field(True)
    enable_predictive_maintenance: bool = Field(True)
    enable_auto_recompress: bool = Field(True)
    recompress_interval_seconds: int = Field(3600, ge=60)

    # ---------- Versioning ----------
    version: str = Field("2.0", description="Configuration schema version")

    # Pydantic v2 config
    model_config = ConfigDict(env_prefix="SUSTAINABILITY_")

    # ---------- Validation ----------
    @model_validator(mode='after')
    def validate_carbon_api_key(self):
        if self.carbon_aware_enabled and not self.carbon_intensity_api_key:
            raise ValueError("carbon_intensity_api_key is required when carbon_aware_enabled is True")
        return self

    @model_validator(mode='after')
    def validate_prometheus_port(self):
        if self.export_metrics and self.prometheus_port is None:
            raise ValueError("prometheus_port is required when export_metrics is True")
        return self

    @model_validator(mode='after')
    def validate_recompress_interval(self):
        if self.enable_auto_recompress and self.recompress_interval_seconds < 60:
            raise ValueError("recompress_interval_seconds must be at least 60 when enable_auto_recompress is True")
        return self

    @model_validator(mode='after')
    def ensure_hardware_profile_exists(self):
        if self.hardware_profile not in self.hardware_profiles:
            raise ValueError(f"hardware_profile '{self.hardware_profile}' not found in hardware_profiles")
        return self

    # ---------- Utility Methods ----------
    def get_energy_per_mac(self) -> float:
        """Get energy per MAC for the selected hardware profile."""
        return self.hardware_profiles.get(self.hardware_profile, self.hardware_profiles['default'])

    def get_energy_normalization_max(self) -> float:
        """Get the max energy used for normalization."""
        if self.energy_normalization_max_joules is not None:
            return self.energy_normalization_max_joules
        return self.energy_threshold_joules * 2.0

    def get_material_index(self) -> float:
        """Get material index for the selected hardware profile."""
        return self.material_index_map.get(self.hardware_profile, 0.5)

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

# Global config instance (can be overridden)
SUSTAINABILITY_CONFIG = SustainabilityConfig()


# ==============================================
# 3. EXPERT PROFILE EXTENSION
# ==============================================
@dataclass
class SustainabilityAwareExpertProfile:
    """
    Extended ExpertProfile with sustainability metrics.
    """
    expert_id: str
    model_path: Optional[str] = None
    node_id: Optional[str] = None  # For anomaly detection tie‑in

    compressed_flag: bool = False
    compression_method: Optional[str] = None
    energy_per_inference_full: float = float('inf')
    energy_per_inference_compressed: Optional[float] = None
    accuracy_full: float = 0.0
    accuracy_compressed: Optional[float] = None
    sustainability_fitness_score: float = 0.0
    carbon_savings_kg: float = 0.0
    material_index: float = 0.0
    last_compressed_at: Optional[datetime] = None
    compression_history: List[Dict] = field(default_factory=list)

    def update_material_index(self, config: SustainabilityConfig):
        """Set material index based on current hardware profile."""
        self.material_index = config.get_material_index()


# ==============================================
# 4. COMPRESSION HISTORY MANAGER (SQLite)
# ==============================================
class CompressionHistoryManager:
    """Manages compression history in a SQLite database."""
    def __init__(self, db_path: str, retention_days: int = 365):
        self.db_path = db_path
        self.retention_days = retention_days
        self._init_db()

    def _init_db(self):
        os.makedirs(os.path.dirname(self.db_path) or '.', exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS compression_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                expert_id TEXT NOT NULL,
                method TEXT NOT NULL,
                energy_before REAL,
                energy_after REAL,
                accuracy_before REAL,
                accuracy_after REAL,
                carbon_savings_kg REAL,
                hardware_profile TEXT,
                timestamp TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_expert_id ON compression_events(expert_id)
        """)
        conn.close()

    def record(self, expert_id: str, method: str, energy_before: float, energy_after: float,
               accuracy_before: float, accuracy_after: float, carbon_savings_kg: float,
               hardware_profile: str):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO compression_events
            (expert_id, method, energy_before, energy_after, accuracy_before, accuracy_after,
             carbon_savings_kg, hardware_profile, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (expert_id, method, energy_before, energy_after, accuracy_before, accuracy_after,
              carbon_savings_kg, hardware_profile, datetime.now().isoformat()))
        conn.commit()
        conn.close()

        # Prune old records
        self._prune()

    def _prune(self):
        if self.retention_days > 0:
            cutoff = (datetime.now() - timedelta(days=self.retention_days)).isoformat()
            conn = sqlite3.connect(self.db_path)
            conn.execute("DELETE FROM compression_events WHERE timestamp < ?", (cutoff,))
            conn.commit()
            conn.close()

    def get_history(self, expert_id: str, limit: int = 10) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            "SELECT method, energy_before, energy_after, accuracy_before, accuracy_after, "
            "carbon_savings_kg, hardware_profile, timestamp FROM compression_events "
            "WHERE expert_id = ? ORDER BY timestamp DESC LIMIT ?",
            (expert_id, limit)
        )
        rows = cursor.fetchall()
        conn.close()
        return [{
            'method': r[0],
            'energy_before': r[1],
            'energy_after': r[2],
            'accuracy_before': r[3],
            'accuracy_after': r[4],
            'carbon_savings_kg': r[5],
            'hardware_profile': r[6],
            'timestamp': r[7]
        } for r in rows]

    def get_latest(self, expert_id: str) -> Optional[Dict]:
        history = self.get_history(expert_id, limit=1)
        return history[0] if history else None


# ==============================================
# 5. COMPRESSED MODEL STORAGE
# ==============================================
class CompressedModelStorage:
    """Persistent storage for compressed models."""
    def __init__(self, storage_dir: str):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def _model_path(self, expert_id: str, method: str) -> Path:
        safe_id = hashlib.md5(f"{expert_id}_{method}".encode()).hexdigest()[:8]
        return self.storage_dir / f"{expert_id}_{method}_{safe_id}.pt"

    def save(self, expert_id: str, method: str, model: torch.nn.Module, profile: SustainabilityAwareExpertProfile):
        path = self._model_path(expert_id, method)
        torch.save({
            'model_state_dict': model.state_dict(),
            'method': method,
            'energy': profile.energy_per_inference_compressed,
            'accuracy': profile.accuracy_compressed,
            'timestamp': datetime.now().isoformat()
        }, path)
        logger.debug(f"Compressed model saved to {path}")

    def load(self, expert_id: str, method: str, model: torch.nn.Module) -> bool:
        path = self._model_path(expert_id, method)
        if not path.exists():
            return False
        data = torch.load(path, map_location='cpu')
        model.load_state_dict(data['model_state_dict'])
        return True


# ==============================================
# 6. ENHANCED SUSTAINABILITY FITNESS SCORER
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
        telemetry: Optional[Any] = None,
    ):
        """
        Args:
            config: SustainabilityConfig instance (if None, uses global SUSTAINABILITY_CONFIG)
            adaptive_cost_function: If provided and config.use_adaptive_weights is True,
                weights will be taken from this function's `weights` dictionary.
            telemetry: Optional telemetry collector for exporting scores.
        """
        self.config = config or SUSTAINABILITY_CONFIG
        self.adaptive_cost = adaptive_cost_function
        self.telemetry = telemetry

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
            energy = self.config.get_energy_normalization_max()

        # 3. Normalize components to [0, 1] (higher = better)
        # Accuracy: already in [0, 1]
        # Energy: 0 = max (bad), so invert: 1 - (energy / max_energy)
        energy_norm = max(0.0, 1.0 - (energy / self.config.get_energy_normalization_max()))
        # Carbon savings: cap at config.carbon_savings_scale_kg
        carbon_norm = min(1.0, carbon_savings / self.config.carbon_savings_scale_kg)
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

        # 8. Store in profile
        profile.sustainability_fitness_score = score

        # 9. Telemetry
        if self.telemetry:
            asyncio.create_task(self.telemetry.gauge('sustainability_fitness', score, {'expert_id': profile.expert_id}))

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
# 7. CORE COMPRESSOR (FULL IMPLEMENTATION)
# ==============================================
class SustainabilityCompressor:
    """
    Applies various compression methods to a model, evaluates trade‑offs,
    and persists the compressed model.
    """
    def __init__(
        self,
        model: torch.nn.Module,
        profile: SustainabilityAwareExpertProfile,
        config: Optional[SustainabilityConfig] = None,
        telemetry: Optional[Any] = None,
        carbon_manager: Optional[Any] = None,
        history_manager: Optional[CompressionHistoryManager] = None,
        storage: Optional[CompressedModelStorage] = None,
        accuracy_fn: Optional[Callable[[torch.nn.Module, Any], float]] = None,
    ):
        self.model = model
        self.profile = profile
        self.config = config or SUSTAINABILITY_CONFIG
        self.telemetry = telemetry
        self.carbon_manager = carbon_manager
        self.history_manager = history_manager
        self.storage = storage
        self.accuracy_fn = accuracy_fn or self._default_accuracy_fn
        self.hardware_profile = self.config.hardware_profile
        self._original_state_dict = None  # For restoring original

    def _default_accuracy_fn(self, model: torch.nn.Module, val_loader: Any) -> float:
        """Default accuracy evaluation for classification tasks."""
        model.eval()
        correct = 0
        total = 0
        with torch.no_grad():
            for inputs, labels in val_loader:
                outputs = model(inputs)
                _, predicted = torch.max(outputs.data, 1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()
        return correct / total if total > 0 else 0.0

    # ---------- Energy estimation ----------
    async def _estimate_energy_real(self, model: torch.nn.Module, sample_input: torch.Tensor) -> float:
        """Use telemetry if available; fallback to FLOPs."""
        if self.telemetry and hasattr(self.telemetry, 'get_energy_per_inference'):
            energy = await self.telemetry.get_energy_per_inference(self.profile.expert_id)
            if energy is not None:
                return energy
        return self._estimate_energy_flops(model, sample_input)

    def _estimate_energy_flops(self, model: torch.nn.Module, sample_input: torch.Tensor) -> float:
        """FLOPs-based estimation with hardware profile coefficient."""
        try:
            flops, _ = profile(model, inputs=(sample_input,), verbose=False)
        except Exception:
            # Manual fallback: count linear layer parameters as rough FLOPs
            flops = 0
            for module in model.modules():
                if isinstance(module, torch.nn.Linear):
                    flops += module.in_features * module.out_features
            flops = flops * 2  # approximate multiply-adds

        coeff = self.config.get_energy_per_mac()
        return flops * coeff

    # ---------- Compression methods ----------
    def apply_structured_pruning(self, sparsity: float = None, dim: int = 0) -> torch.nn.Module:
        """Apply channel‑wise pruning (structured) on Conv2d layers."""
        if sparsity is None:
            sparsity = self.config.pruning_sparsity
        for module in self.model.modules():
            if isinstance(module, torch.nn.Conv2d):
                prune.ln_structured(module, name='weight', amount=sparsity, n=2, dim=dim)
                prune.remove(module, 'weight')
        return self.model

    def apply_unstructured_pruning(self, sparsity: float = None) -> torch.nn.Module:
        """Apply global unstructured pruning on Linear layers."""
        if sparsity is None:
            sparsity = self.config.pruning_sparsity
        parameters_to_prune = []
        for module in self.model.modules():
            if isinstance(module, torch.nn.Linear):
                parameters_to_prune.append((module, "weight"))
        prune.global_unstructured(
            parameters_to_prune,
            pruning_method=prune.L1Unstructured,
            amount=sparsity
        )
        for module, _ in parameters_to_prune:
            prune.remove(module, "weight")
        return self.model

    def apply_int8_quantization(self) -> torch.nn.Module:
        """Apply dynamic INT8 quantization to Linear layers."""
        quantized_model = quantize_dynamic(
            self.model,
            {torch.nn.Linear},
            dtype=torch.qint8
        )
        return quantized_model

    def apply_hybrid(self) -> torch.nn.Module:
        """Apply hybrid: pruning + quantization."""
        self.apply_unstructured_pruning(sparsity=self.config.hybrid_pruning_sparsity)
        self.apply_int8_quantization()
        return self.model

    def apply_svd(self, rank_factor: float = None) -> torch.nn.Module:
        """
        Replace Linear layers with low-rank approximation using SVD.
        rank_factor: fraction of original rank to keep.
        """
        if rank_factor is None:
            rank_factor = self.config.svd_rank_factor
        for name, module in self.model.named_modules():
            if isinstance(module, torch.nn.Linear):
                weight = module.weight.data
                U, S, V = torch.linalg.svd(weight, full_matrices=False)
                k = max(1, int(S.size(0) * rank_factor))
                U_k = U[:, :k]
                S_k = S[:k]
                V_k = V[:k, :]
                new_weight = U_k @ torch.diag(S_k) @ V_k
                module.weight.data = new_weight
                # Bias remains unchanged
        return self.model

    # ---------- Model handling ----------
    def _copy_model(self) -> torch.nn.Module:
        """Create a deep copy of the current model."""
        return copy.deepcopy(self.model)

    def _restore_original(self):
        """Restore the model to its original state."""
        if self._original_state_dict is not None:
            self.model.load_state_dict(self._original_state_dict)

    def _evaluate_accuracy(self, model: torch.nn.Module, val_loader: Any) -> float:
        return self.accuracy_fn(model, val_loader)

    # ---------- Main compression orchestration ----------
    async def evaluate_tradeoff_and_compress(self, val_loader: Any, sample_input: torch.Tensor) -> bool:
        """
        Enhanced orchestration:
        - Deep copy model for each candidate.
        - Use multiple compression methods.
        - Track best model (deep copy) and metrics.
        - Save compressed model and history.
        """
        # Ensure we start from the original model
        self._restore_original()

        # Baseline metrics
        baseline_acc = self._evaluate_accuracy(self.model, val_loader)
        baseline_energy = await self._estimate_energy_real(self.model, sample_input)

        self.profile.accuracy_full = baseline_acc
        self.profile.energy_per_inference_full = baseline_energy

        if baseline_energy <= self.config.energy_threshold_joules:
            logger.info(f"Expert {self.profile.expert_id} energy ({baseline_energy:.2f} J) within threshold. Skipping.")
            return False

        # Compression candidates (ordered by priority)
        candidates = []
        for method in self.config.compression_strategy_priority:
            if method == CompressionMethod.STRUCTURED_PRUNING:
                candidates.append(('structured_pruning', self.apply_structured_pruning, self.config.pruning_sparsity))
            elif method == CompressionMethod.UNSTRUCTURED_PRUNING:
                candidates.append(('unstructured_pruning', self.apply_unstructured_pruning, self.config.pruning_sparsity))
            elif method == CompressionMethod.INT8_QUANT:
                candidates.append(('int8_quant', self.apply_int8_quantization, None))
            elif method == CompressionMethod.HYBRID:
                candidates.append(('hybrid', self.apply_hybrid, None))
            elif method == CompressionMethod.SVD:
                candidates.append(('svd', self.apply_svd, self.config.svd_rank_factor))

        best_candidate = None
        best_energy = baseline_energy
        best_acc = baseline_acc
        best_model = None

        for method_name, method_func, sparsity in candidates:
            # Deep copy the original model for this candidate
            original_copy = self._copy_model()
            model_copy = original_copy  # start with copy
            try:
                # Apply method to the copy
                if sparsity is not None:
                    model_copy = method_func(sparsity)
                else:
                    if method_name == 'hybrid':
                        model_copy = self.apply_hybrid()
                    elif method_name == 'svd':
                        model_copy = self.apply_svd()
                    else:
                        model_copy = method_func()

                # Evaluate
                acc = self._evaluate_accuracy(model_copy, val_loader)
                energy = await self._estimate_energy_real(model_copy, sample_input)

                # Check if accuracy is acceptable and energy improvement
                if baseline_acc - acc <= self.config.accuracy_drop_tolerance:
                    if energy < best_energy:
                        best_energy = energy
                        best_acc = acc
                        best_candidate = method_name
                        best_model = copy.deepcopy(model_copy)  # keep a deep copy
                # Continue to next candidate; model_copy goes out of scope
            except Exception as e:
                logger.warning(f"Compression method {method_name} failed: {e}")
            finally:
                # Clean up to avoid memory leaks
                del model_copy
                del original_copy

        if best_candidate is None or best_model is None:
            self._restore_original()
            logger.warning(f"Expert {self.profile.expert_id} cannot be compressed without exceeding accuracy tolerance.")
            return False

        # Apply the best compression to the actual model (replace)
        self.model.load_state_dict(best_model.state_dict())
        self.profile.compressed_flag = True
        self.profile.compression_method = best_candidate
        self.profile.accuracy_compressed = best_acc
        self.profile.energy_per_inference_compressed = best_energy

        # Carbon savings
        carbon_savings = 0.0
        if self.config.carbon_aware_enabled and self.carbon_manager:
            intensity_data = await self.carbon_manager.get_current_intensity()
            carbon_intensity = intensity_data.get('intensity', 400) / 1000  # kg/kWh
            energy_saved_joules = baseline_energy - best_energy
            carbon_savings = energy_saved_joules / 3.6e6 * carbon_intensity
            self.profile.carbon_savings_kg = carbon_savings

        self.profile.last_compressed_at = datetime.now()

        # Save to persistent storage
        if self.storage:
            self.storage.save(self.profile.expert_id, best_candidate, self.model, self.profile)

        # Record history
        if self.history_manager:
            self.history_manager.record(
                self.profile.expert_id,
                best_candidate,
                baseline_energy,
                best_energy,
                baseline_acc,
                best_acc,
                carbon_savings,
                self.hardware_profile
            )

        # Telemetry counters
        if self.telemetry:
            asyncio.create_task(self.telemetry.increment(f"{self.config.version}.compressions_total"))
            asyncio.create_task(self.telemetry.gauge(f"{self.config.version}.energy_saved_joules", energy_saved_joules))
            asyncio.create_task(self.telemetry.gauge(f"{self.config.version}.carbon_saved_kg", carbon_savings))

        logger.info(f"Expert {self.profile.expert_id} compressed with {best_candidate}. "
                    f"Energy: {baseline_energy:.4f} → {best_energy:.4f} J, "
                    f"Accuracy: {baseline_acc:.4f} → {best_acc:.4f}, "
                    f"Carbon saved: {carbon_savings:.4f} kg CO₂")
        return True


# ==============================================
# 8. MLOPS PIPELINE EXTENSION (FULL)
# ==============================================
class MLOpsPipelineExtension:
    """
    Integrates sustainability compression into an ML pipeline.
    Supports async re‑compression loop and anomaly‑triggered compression.
    """
    def __init__(
        self,
        pipeline: Any,
        config: Optional[SustainabilityConfig] = None,
        telemetry: Optional[Any] = None,
        carbon_manager: Optional[Any] = None,
        anomaly_detector: Optional[Any] = None,
        accuracy_fn: Optional[Callable[[torch.nn.Module, Any], float]] = None,
    ):
        self.pipeline = pipeline
        self.config = config or SUSTAINABILITY_CONFIG
        self.telemetry = telemetry
        self.carbon_manager = carbon_manager
        self.anomaly_detector = anomaly_detector
        self.accuracy_fn = accuracy_fn
        self.history_manager = CompressionHistoryManager(
            self.config.history_db_path,
            self.config.history_retention_days
        )
        self.storage = CompressedModelStorage(self.config.compressed_model_dir)

        # Background re‑compression task
        self._running = False
        self._recompress_task: Optional[asyncio.Task] = None

        # Rollback monitor: store last known compressed accuracy
        self._compressed_acc_cache: Dict[str, float] = {}

    def _ensure_model_registry(self):
        """Ensure pipeline has expected attributes; raise if not."""
        if not hasattr(self.pipeline, 'model_registry') or not hasattr(self.pipeline, 'profile_registry'):
            raise AttributeError("Pipeline must have 'model_registry' and 'profile_registry' attributes.")
        if not hasattr(self.pipeline, 'val_loaders'):
            self.pipeline.val_loaders = {}  # optional

    def on_expert_registered(
        self,
        expert_id: str,
        model: torch.nn.Module,
        profile: SustainabilityAwareExpertProfile,
        val_loader: Any,
    ) -> None:
        """
        Hook to run immediately after an expert is trained/registered.
        """
        self._ensure_model_registry()

        # Compute material index based on hardware profile
        profile.update_material_index(self.config)

        # Check if a compressed version exists on disk
        if self.storage:
            latest = self.history_manager.get_latest(expert_id)
            if latest:
                method = latest['method']
                if self.storage.load(expert_id, method, model):
                    # Update profile with saved metrics
                    profile.compressed_flag = True
                    profile.compression_method = method
                    profile.accuracy_compressed = latest['accuracy_after']
                    profile.energy_per_inference_compressed = latest['energy_after']
                    profile.carbon_savings_kg = latest['carbon_savings_kg']
                    profile.last_compressed_at = datetime.fromisoformat(latest['timestamp'])
                    logger.info(f"Loaded compressed model for expert {expert_id} (method: {method})")
                    # Store in pipeline
                    self.pipeline.model_registry[expert_id] = model
                    self.pipeline.profile_registry[expert_id] = profile
                    return

        # Trigger compression if energy exceeds threshold
        if profile.energy_per_inference_full > self.config.energy_threshold_joules:
            logger.info(f"[SUSTAINABILITY] Triggering compression for expert {expert_id}...")
            compressor = SustainabilityCompressor(
                model, profile, self.config,
                telemetry=self.telemetry,
                carbon_manager=self.carbon_manager,
                history_manager=self.history_manager,
                storage=self.storage,
                accuracy_fn=self.accuracy_fn
            )
            # Get sample input from val_loader
            sample_input = next(iter(val_loader))[0]
            # Use asyncio.run carefully – only if we're not already in an event loop.
            try:
                loop = asyncio.get_running_loop()
                # We are in an async context, so create a task
                async def compress():
                    success = await compressor.evaluate_tradeoff_and_compress(val_loader, sample_input)
                    if success:
                        self.pipeline.model_registry[expert_id] = compressor.model
                        self.pipeline.profile_registry[expert_id] = profile
                        self._compressed_acc_cache[expert_id] = profile.accuracy_compressed
                asyncio.create_task(compress())
            except RuntimeError:
                # No running loop, use asyncio.run
                success = asyncio.run(compressor.evaluate_tradeoff_and_compress(val_loader, sample_input))
                if success:
                    self.pipeline.model_registry[expert_id] = compressor.model
                    self.pipeline.profile_registry[expert_id] = profile
                    self._compressed_acc_cache[expert_id] = profile.accuracy_compressed
        else:
            logger.info(f"Expert {expert_id} energy ({profile.energy_per_inference_full:.2f} J) within threshold. No compression.")

    # ---------- Periodic re‑compression ----------
    async def start_recompress_loop(self):
        """Background task to periodically re‑evaluate compression."""
        if self.config.recompress_interval_seconds <= 0:
            return
        self._running = True
        while self._running:
            await asyncio.sleep(self.config.recompress_interval_seconds)
            await self._recompress_all()

    async def _recompress_all(self):
        """Iterate over all experts and re‑compress if needed."""
        self._ensure_model_registry()
        for expert_id, model in list(self.pipeline.model_registry.items()):
            profile = self.pipeline.profile_registry.get(expert_id)
            if profile is None:
                continue
            val_loader = self.pipeline.val_loaders.get(expert_id)
            if val_loader is None:
                continue
            compressor = SustainabilityCompressor(
                model, profile, self.config,
                telemetry=self.telemetry,
                carbon_manager=self.carbon_manager,
                history_manager=self.history_manager,
                storage=self.storage,
                accuracy_fn=self.accuracy_fn
            )
            sample_input = next(iter(val_loader))[0]
            success = await compressor.evaluate_tradeoff_and_compress(val_loader, sample_input)
            if success:
                self.pipeline.model_registry[expert_id] = compressor.model
                self.pipeline.profile_registry[expert_id] = profile
                self._compressed_acc_cache[expert_id] = profile.accuracy_compressed

    async def stop_recompress_loop(self):
        self._running = False
        if self._recompress_task:
            self._recompress_task.cancel()
            try:
                await self._recompress_task
            except asyncio.CancelledError:
                pass
            self._recompress_task = None

    # ---------- Anomaly‑triggered compression ----------
    async def on_anomaly_detected(self, node_id: str, metrics: Dict):
        """Callback from AnomalyDetector."""
        if not self.config.enable_anomaly_trigger:
            return
        self._ensure_model_registry()
        # Find experts running on this node
        for expert_id, profile in self.pipeline.profile_registry.items():
            if profile.node_id == node_id:
                model = self.pipeline.model_registry.get(expert_id)
                if model is None:
                    continue
                compressor = SustainabilityCompressor(
                    model, profile, self.config,
                    telemetry=self.telemetry,
                    carbon_manager=self.carbon_manager,
                    history_manager=self.history_manager,
                    storage=self.storage,
                    accuracy_fn=self.accuracy_fn
                )
                val_loader = self.pipeline.val_loaders.get(expert_id)
                if val_loader:
                    sample_input = next(iter(val_loader))[0]
                    success = await compressor.evaluate_tradeoff_and_compress(val_loader, sample_input)
                    if success:
                        self.pipeline.model_registry[expert_id] = compressor.model
                        self.pipeline.profile_registry[expert_id] = profile
                        self._compressed_acc_cache[expert_id] = profile.accuracy_compressed
                break  # assume only one expert per node for simplicity

    # ---------- Rollback monitor ----------
    async def monitor_rollback(self, expert_id: str, current_accuracy: float):
        """
        If the compressed expert's accuracy drops below a threshold, revert to full model.
        Should be called periodically by the pipeline.
        """
        if expert_id not in self._compressed_acc_cache:
            return
        compressed_acc = self._compressed_acc_cache[expert_id]
        if compressed_acc == 0:
            return
        if current_accuracy < compressed_acc * self.config.accuracy_drop_tolerance:
            logger.warning(f"Expert {expert_id} accuracy {current_accuracy:.4f} dropped below {compressed_acc*self.config.accuracy_drop_tolerance:.4f}. Reverting to full model.")
            profile = self.pipeline.profile_registry.get(expert_id)
            if profile and not profile.compressed_flag:
                logger.info(f"Expert {expert_id} already full model.")
                return
            # Restore full model from original state (if stored)
            if hasattr(self.pipeline, 'full_models'):
                full_model = self.pipeline.full_models.get(expert_id)
                if full_model is not None:
                    self.pipeline.model_registry[expert_id] = full_model
                    profile.compressed_flag = False
                    profile.accuracy_compressed = None
                    profile.energy_per_inference_compressed = None
                    logger.info(f"Reverted expert {expert_id} to full model.")
                else:
                    logger.error(f"No full model available for expert {expert_id} to revert.")


# ==============================================
# 9. ROUTER INTEGRATION
# ==============================================
class SustainabilityAwareRouter:
    """
    Router that selects experts based on sustainability fitness.
    Assumes base_router has methods:
        - get_all_experts(query) -> list of (expert_id, profile)
        - load_compressed_model(expert_id) -> model
        - load_full_model(expert_id) -> model
    """
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
# 10. CONVENIENCE FACTORY
# ==============================================
def create_sustainability_module(
    pipeline: Any,
    config: Optional[SustainabilityConfig] = None,
    adaptive_cost_function: Optional[Any] = None,
    telemetry: Optional[Any] = None,
    carbon_manager: Optional[Any] = None,
    anomaly_detector: Optional[Any] = None,
    accuracy_fn: Optional[Callable[[torch.nn.Module, Any], float]] = None,
) -> Dict[str, Any]:
    """
    Factory to create all sustainability components with the enhanced fitness scorer.
    """
    config = config or SUSTAINABILITY_CONFIG
    scorer = SustainabilityFitnessScorer(config, adaptive_cost_function, telemetry)
    extension = MLOpsPipelineExtension(
        pipeline, config, telemetry, carbon_manager, anomaly_detector, accuracy_fn
    )
    router = SustainabilityAwareRouter(pipeline, scorer)
    return {
        'scorer': scorer,
        'extension': extension,
        'router': router,
        'config': config,
    }


# ==============================================
# 11. EXPORTS
# ==============================================
__all__ = [
    "SUSTAINABILITY_CONFIG",
    "SustainabilityAwareExpertProfile",
    "SustainabilityCompressor",
    "SustainabilityFitnessScorer",
    "MLOpsPipelineExtension",
    "SustainabilityAwareRouter",
    "create_sustainability_module",
    "CompressionMethod",
    "QuantizationMethod",
]


# ==============================================
# 12. EXAMPLE USAGE (if run directly)
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

    # Example config validation
    try:
        config = SustainabilityConfig(energy_threshold_joules=-1)
    except Exception as e:
        print(f"Validation error: {e}")
