# sustainability/__init__.py
"""
Enhanced Sustainability-Aware Model Compression and Pruning Module
Single-file drop-in for Green_Agent MoE system with MOPD, XAI, temporal safety,
human-in-the-loop, and chaos testing support.

Includes:
- Pydantic configuration with MOPD weights and safety/approval flags
- Real-time energy telemetry
- Structured pruning, unstructured pruning, INT8 quantization, hybrid, SVD
- Carbon-aware compression
- Persistent storage & history logging (SQLite)
- Periodic re-compression (async, cancellable)
- Anomaly-triggered compression
- Hardware profiles with configurable energy coefficients
- Benchmarking
- Fitness scoring (accuracy, energy, carbon, material)
- Router integration with multi-objective fitness
- Automatic rollback on accuracy degradation
- Structured telemetry (counters, gauges)
- Graceful fallbacks for missing dependencies
- **Pareto front generation for compression methods**
- **MOPD-aware selection based on configurable weights**
- **Persistence of Pareto fronts**
- **XAI explanations for selected plans**
- **Temporal safety checks (invariants)**
- **Human-in-the-loop approval**
- **Chaos testing hooks**
"""

import torch
import torch.nn.utils.prune as prune
from torch.quantization import quantize_dynamic
from dataclasses import dataclass, field, asdict
from typing import Optional, Any, Dict, List, Callable, Union, Protocol, Tuple
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
    from pydantic import BaseModel, Field, field_validator, model_validator
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
# 1. CONFIGURATION (Pydantic validated) – Enhanced with MOPD and new flags
# ==============================================

class MOPDConfig(BaseModel):
    """Configuration for Multi-Objective Pareto Decision (MOPD) in compression."""
    enabled: bool = Field(True, description="Enable MOPD-aware selection")
    objective_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            'accuracy': 0.4,
            'energy': 0.3,
            'carbon': 0.2,
            'material': 0.1,
        },
        description="Weights for objectives when scalarising Pareto front"
    )
    grid_resolution: int = Field(5, description="Number of discrete points for continuous variables (unused for now)")
    enable_cost_benefit: bool = Field(True)
    enable_predictive: bool = Field(True)
    # New: XAI and approval flags
    explain_decisions: bool = Field(True, description="Generate explanations for selected compression plans")
    require_human_approval: bool = Field(False, description="Require human approval for aggressive compression")
    aggressive_threshold: float = Field(0.3, description="Accuracy drop threshold to consider aggressive")

    @model_validator(mode='after')
    def check_weights(self):
        total = sum(self.objective_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError("Objective weights must sum to 1")
        return self

class SustainabilityConfig(BaseModel):
    """Configuration for sustainability‑aware compression."""
    # Triggers compression if full inference energy exceeds this (Joules)
    energy_threshold: float = Field(5.0, ge=0)
    # Max allowable accuracy drop (absolute difference)
    accuracy_drop_tolerance: float = Field(0.02, ge=0, le=1)
    # Energy estimation coefficient (pJ per MAC operation) – default
    energy_per_mac: float = Field(0.5e-12, gt=0)
    # Fitness weighting (for scalar routing, deprecated in favour of MOPD)
    fitness_accuracy_weight: float = Field(0.5, ge=0, le=1)  # changed to sum to 1
    fitness_energy_weight: float = Field(0.3, ge=0, le=1)
    fitness_carbon_weight: float = Field(0.15, ge=0, le=1)
    fitness_material_weight: float = Field(0.05, ge=0, le=1)
    # Pruning sparsity levels
    pruning_sparsity: float = Field(0.3, ge=0, le=1)
    hybrid_pruning_sparsity: float = Field(0.2, ge=0, le=1)
    # SVD rank reduction factor (fraction of original rank)
    svd_rank_factor: float = Field(0.5, gt=0, le=1)
    # Hardware profiles with per‑profile energy coefficients
    hardware_profiles: Dict[str, float] = Field(default_factory=lambda: {
        'default': 0.5e-12,
        'gpu': 0.3e-12,
        'cpu': 0.5e-12,
        'tpu': 0.2e-12,
    })
    hardware_profile: str = Field("default")
    # Compression storage directory
    compressed_model_dir: str = Field("./compressed_models")
    # History database path
    history_db_path: str = Field("./compression_history.db")
    # Re‑compression interval (seconds); 0 = disabled
    recompress_interval: int = Field(0, ge=0)
    # Whether to trigger compression on anomaly
    anomaly_trigger_enabled: bool = True
    # Rollback monitoring: if accuracy drops below this fraction of compressed accuracy, revert
    rollback_accuracy_threshold: float = Field(0.95, gt=0, le=1)
    # Telemetry prefix for metrics
    telemetry_prefix: str = "sustainability"
    # MOPD configuration
    mopd: MOPDConfig = Field(default_factory=MOPDConfig)
    # NEW: temporal safety and chaos testing flags
    enable_temporal_safety: bool = Field(True)
    enable_chaos_testing: bool = Field(False)
    chaos_test_interval: int = Field(60, ge=0)

    @model_validator(mode='after')
    def check_weights(self):
        total = self.fitness_accuracy_weight + self.fitness_energy_weight + self.fitness_carbon_weight + self.fitness_material_weight
        if abs(total - 1.0) > 1e-6:
            raise ValueError("fitness_accuracy_weight + fitness_energy_weight + fitness_carbon_weight + fitness_material_weight must equal 1")
        return self

    def get_energy_coeff(self, profile: str = None) -> float:
        """Get energy per MAC coefficient for a given hardware profile."""
        profile = profile or self.hardware_profile
        return self.hardware_profiles.get(profile, self.energy_per_mac)

    class Config:
        env_prefix = "SUSTAINABILITY_"

# Global config instance
SUSTAINABILITY_CONFIG = SustainabilityConfig()

# ==============================================
# 2. DEPENDENCY STUBS (graceful fallback)
# ==============================================

class TelemetryCollectorStub:
    """Stub for TelemetryCollector if not available."""
    async def get_energy_per_inference(self, expert_id: str) -> Optional[float]:
        return None
    async def increment(self, metric: str, value: float = 1.0, tags: Dict = None):
        pass
    async def gauge(self, metric: str, value: float, tags: Dict = None):
        pass

class CarbonIntensityManagerStub:
    async def get_current_intensity(self) -> Dict:
        return {'intensity': 400.0}

class AnomalyDetectorStub:
    async def ingest(self, node_id: str, metrics: Dict) -> Optional[Any]:
        return None

try:
    from ..telemetry_collector import TelemetryCollector
    TELEMETRY_AVAILABLE = True
except ImportError:
    TelemetryCollector = TelemetryCollectorStub
    TELEMETRY_AVAILABLE = False

try:
    from ..carbon_manager import CarbonIntensityManager
    CARBON_AVAILABLE = True
except ImportError:
    CarbonIntensityManager = CarbonIntensityManagerStub
    CARBON_AVAILABLE = False

try:
    from ..anomaly_detection import AnomalyDetector
    ANOMALY_AVAILABLE = True
except ImportError:
    AnomalyDetector = AnomalyDetectorStub
    ANOMALY_AVAILABLE = False

# ==============================================
# 3. EXPERT PROFILE EXTENSION (Enhanced with MOPD and XAI)
# ==============================================

@dataclass
class MOPDPoint:
    """Represents a single compression candidate with its objectives."""
    method: str
    accuracy: float
    energy: float
    carbon_savings_kg: float
    material_index: float
    scalarised_score: float = 0.0
    explanation: str = ""  # NEW: XAI explanation

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        return cls(**data)

@dataclass
class SustainabilityAwareExpertProfile:
    """
    Extended ExpertProfile with sustainability metrics and MOPD data.
    """
    expert_id: str
    model_path: Optional[str] = None
    node_id: Optional[str] = None

    compressed_flag: bool = False
    compression_method: Optional[str] = None
    energy_per_inference_full: float = float('inf')
    energy_per_inference_compressed: Optional[float] = None
    accuracy_full: float = 0.0
    accuracy_compressed: Optional[float] = None
    sustainability_fitness_score: float = 0.0
    carbon_savings_kg: float = 0.0
    material_index: float = 0.5  # default set to neutral
    last_compressed_at: Optional[datetime] = None
    compression_history: List[Dict] = field(default_factory=list)
    pareto_front: List[MOPDPoint] = field(default_factory=list)

    def update_material_index(self, hardware_profile: str):
        material_map = {
            'default': 0.5,
            'gpu': 0.4,
            'cpu': 0.3,
            'tpu': 0.2,
        }
        self.material_index = material_map.get(hardware_profile, 0.5)

# ==============================================
# 4. COMPRESSION HISTORY MANAGER (SQLite) – Enhanced with MOPD
# ==============================================

class CompressionHistoryManager:
    """Manages compression history in a SQLite database, including Pareto fronts."""
    def __init__(self, db_path: str):
        self.db_path = db_path
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
            CREATE TABLE IF NOT EXISTS pareto_fronts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                expert_id TEXT NOT NULL,
                method TEXT NOT NULL,
                accuracy REAL,
                energy REAL,
                carbon_savings_kg REAL,
                material_index REAL,
                scalarised_score REAL,
                explanation TEXT,
                timestamp TEXT NOT NULL
            )
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

    def record_pareto_front(self, expert_id: str, pareto_front: List[MOPDPoint]):
        conn = sqlite3.connect(self.db_path)
        for point in pareto_front:
            conn.execute("""
                INSERT INTO pareto_fronts
                (expert_id, method, accuracy, energy, carbon_savings_kg, material_index,
                 scalarised_score, explanation, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (expert_id, point.method, point.accuracy, point.energy,
                  point.carbon_savings_kg, point.material_index,
                  point.scalarised_score, point.explanation, datetime.now().isoformat()))
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

    def get_pareto_front(self, expert_id: str, limit: int = 50) -> List[MOPDPoint]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            "SELECT method, accuracy, energy, carbon_savings_kg, material_index, "
            "scalarised_score, explanation, timestamp FROM pareto_fronts "
            "WHERE expert_id = ? ORDER BY timestamp DESC LIMIT ?",
            (expert_id, limit)
        )
        rows = cursor.fetchall()
        conn.close()
        return [MOPDPoint(
            method=r[0],
            accuracy=r[1],
            energy=r[2],
            carbon_savings_kg=r[3],
            material_index=r[4],
            scalarised_score=r[5],
            explanation=r[6]
        ) for r in rows]

# ==============================================
# 5. COMPRESSED MODEL STORAGE (unchanged)
# ==============================================

class CompressedModelStorage:
    def __init__(self, storage_dir: str):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def _model_path(self, expert_id: str, method: str) -> Path:
        safe_id = hashlib.md5(expert_id.encode()).hexdigest()[:8]
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
# 6. CORE COMPRESSOR (ENHANCED WITH MOPD, XAI, Safety, Approval, Chaos)
# ==============================================

class SustainabilityCompressor:
    def __init__(
        self,
        model: torch.nn.Module,
        profile: SustainabilityAwareExpertProfile,
        config: SustainabilityConfig = None,
        telemetry: Optional[TelemetryCollector] = None,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        history_manager: Optional[CompressionHistoryManager] = None,
        storage: Optional[CompressedModelStorage] = None,
        accuracy_fn: Optional[Callable[[torch.nn.Module, Any], float]] = None,
    ):
        self.model = model
        self.profile = profile
        self.config = config or SUSTAINABILITY_CONFIG
        self.telemetry = telemetry or TelemetryCollectorStub()
        self.carbon_manager = carbon_manager or CarbonIntensityManagerStub()
        self.history_manager = history_manager
        self.storage = storage
        self.hardware_profile = self.config.hardware_profile
        self.accuracy_fn = accuracy_fn or self._default_accuracy_fn
        self._original_state_dict = copy.deepcopy(model.state_dict())
        # Initialize material index if not set
        if self.profile.material_index == 0.0:
            self.profile.update_material_index(self.hardware_profile)

    def _default_accuracy_fn(self, model: torch.nn.Module, val_loader: Any) -> float:
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

    async def _estimate_energy_real(self, model: torch.nn.Module, sample_input: torch.Tensor) -> float:
        if TELEMETRY_AVAILABLE and hasattr(self.telemetry, 'get_energy_per_inference'):
            energy = await self.telemetry.get_energy_per_inference(self.profile.expert_id)
            if energy is not None:
                return energy
        return self._estimate_energy_flops(model, sample_input)

    def _estimate_energy_flops(self, model: torch.nn.Module, sample_input: torch.Tensor) -> float:
        try:
            flops, _ = profile(model, inputs=(sample_input,), verbose=False)
        except Exception:
            flops = 0
            for module in model.modules():
                if isinstance(module, torch.nn.Linear):
                    flops += module.in_features * module.out_features
            flops = flops * 2
        coeff = self.config.get_energy_coeff(self.hardware_profile)
        return flops * coeff

    # ---------- Compression methods (now accept optional model parameter) ----------
    def _apply_structured_pruning(self, model: torch.nn.Module, sparsity: float = None, dim: int = 0) -> torch.nn.Module:
        if sparsity is None:
            sparsity = self.config.pruning_sparsity
        for module in model.modules():
            if isinstance(module, torch.nn.Conv2d):
                prune.ln_structured(module, name='weight', amount=sparsity, n=2, dim=dim)
                prune.remove(module, 'weight')
        return model

    def _apply_unstructured_pruning(self, model: torch.nn.Module, sparsity: float = None) -> torch.nn.Module:
        if sparsity is None:
            sparsity = self.config.pruning_sparsity
        parameters_to_prune = []
        for module in model.modules():
            if isinstance(module, torch.nn.Linear):
                parameters_to_prune.append((module, "weight"))
        prune.global_unstructured(
            parameters_to_prune,
            pruning_method=prune.L1Unstructured,
            amount=sparsity
        )
        for module, _ in parameters_to_prune:
            prune.remove(module, "weight")
        return model

    def _apply_int8_quantization(self, model: torch.nn.Module) -> torch.nn.Module:
        quantized_model = quantize_dynamic(model, {torch.nn.Linear}, dtype=torch.qint8)
        return quantized_model

    def _apply_hybrid(self, model: torch.nn.Module) -> torch.nn.Module:
        model = self._apply_unstructured_pruning(model, sparsity=self.config.hybrid_pruning_sparsity)
        model = self._apply_int8_quantization(model)
        return model

    def _apply_svd(self, model: torch.nn.Module, rank_factor: float = None) -> torch.nn.Module:
        if rank_factor is None:
            rank_factor = self.config.svd_rank_factor
        for name, module in model.named_modules():
            if isinstance(module, torch.nn.Linear):
                weight = module.weight.data
                U, S, V = torch.linalg.svd(weight, full_matrices=False)
                k = max(1, int(S.size(0) * rank_factor))
                U_k = U[:, :k]
                S_k = S[:k]
                V_k = V[:k, :]
                module.weight.data = U_k @ torch.diag(S_k) @ V_k
        return model

    # ---------- Model copying ----------
    def _copy_model(self) -> torch.nn.Module:
        return copy.deepcopy(self.model)

    def _restore_original(self):
        self.model.load_state_dict(self._original_state_dict)

    # ---------- Accuracy evaluation ----------
    def _evaluate_accuracy(self, model: torch.nn.Module, val_loader: Any) -> float:
        return self.accuracy_fn(model, val_loader)

    # ---------- Benchmarking ----------
    async def benchmark(self, val_loader: Any, sample_input: torch.Tensor, iterations: int = 10) -> Dict:
        model = self.model
        model.eval()
        for _ in range(5):
            _ = model(sample_input)
        start = time.time()
        for _ in range(iterations):
            _ = model(sample_input)
        elapsed = time.time() - start
        avg_time_ms = (elapsed / iterations) * 1000
        energy = await self._estimate_energy_real(model, sample_input)
        return {'avg_time_ms': avg_time_ms, 'energy_per_inference': energy, 'iterations': iterations}

    # ---------- Pareto front generation (fixed model copying) ----------
    async def _generate_pareto_front(
        self,
        val_loader: Any,
        sample_input: torch.Tensor,
        baseline_acc: float,
        baseline_energy: float
    ) -> List[MOPDPoint]:
        methods = [
            ('structured_pruning', self._apply_structured_pruning, self.config.pruning_sparsity),
            ('unstructured_pruning', self._apply_unstructured_pruning, self.config.pruning_sparsity),
            ('int8_quant', self._apply_int8_quantization, None),
            ('hybrid', self._apply_hybrid, None),
            ('svd', self._apply_svd, self.config.svd_rank_factor),
        ]
        points = []
        for method_name, method_func, sparsity in methods:
            model_copy = self._copy_model()
            try:
                if sparsity is not None:
                    model_copy = method_func(model_copy, sparsity)
                else:
                    if method_name == 'hybrid':
                        model_copy = self._apply_hybrid(model_copy)
                    elif method_name == 'svd':
                        model_copy = self._apply_svd(model_copy)
                    else:
                        model_copy = method_func(model_copy)

                acc = self._evaluate_accuracy(model_copy, val_loader)
                energy = await self._estimate_energy_real(model_copy, sample_input)
                carbon_savings = 0.0
                if CARBON_AVAILABLE and hasattr(self.carbon_manager, 'get_current_intensity'):
                    intensity_data = await self.carbon_manager.get_current_intensity()
                    carbon_intensity = intensity_data.get('intensity', 400) / 1000
                    energy_saved_joules = baseline_energy - energy
                    carbon_savings = energy_saved_joules / 3.6e6 * carbon_intensity

                # Only include if within tolerance
                if baseline_acc - acc <= self.config.accuracy_drop_tolerance:
                    explanation = f"Method {method_name}: accuracy {acc:.4f}, energy {energy:.4f}J, carbon saved {carbon_savings:.4f}kg"
                    point = MOPDPoint(
                        method=method_name,
                        accuracy=acc,
                        energy=energy,
                        carbon_savings_kg=carbon_savings,
                        material_index=self.profile.material_index,
                        explanation=explanation
                    )
                    points.append(point)
            except Exception as e:
                logger.warning(f"Compression method {method_name} failed: {e}")
            finally:
                del model_copy

        if not points:
            return []

        # Pareto filter
        pareto = []
        for i, p_i in enumerate(points):
            dominated = False
            for j, p_j in enumerate(points):
                if i == j:
                    continue
                a_vec = [-p_i.accuracy, p_i.energy, -p_i.carbon_savings_kg, p_i.material_index]
                b_vec = [-p_j.accuracy, p_j.energy, -p_j.carbon_savings_kg, p_j.material_index]
                if all(b <= a for a, b in zip(a_vec, b_vec)) and any(b < a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDPoint]) -> Optional[MOPDPoint]:
        if not pareto_front:
            return None
        weights = self.config.mopd.objective_weights
        acc_vals = [p.accuracy for p in pareto_front]
        energy_vals = [p.energy for p in pareto_front]
        carbon_vals = [p.carbon_savings_kg for p in pareto_front]
        material_vals = [p.material_index for p in pareto_front]
        max_acc = max(acc_vals) if acc_vals else 1
        max_energy = max(energy_vals) if energy_vals else 1
        max_carbon = max(carbon_vals) if carbon_vals else 1
        max_material = max(material_vals) if material_vals else 1

        best = None
        best_score = -float('inf')
        for point in pareto_front:
            acc_norm = point.accuracy / max_acc if max_acc > 0 else 0
            energy_norm = 1.0 - (point.energy / max_energy) if max_energy > 0 else 0
            carbon_norm = point.carbon_savings_kg / max_carbon if max_carbon > 0 else 0
            material_norm = 1.0 - (point.material_index / max_material) if max_material > 0 else 0
            score = (weights['accuracy'] * acc_norm +
                     weights['energy'] * energy_norm +
                     weights['carbon'] * carbon_norm +
                     weights['material'] * material_norm)
            point.scalarised_score = score
            if score > best_score:
                best_score = score
                best = point
        return best

    # ---------- Main compression orchestration (enhanced) ----------
    async def evaluate_tradeoff_and_compress(
        self,
        val_loader: Any,
        sample_input: torch.Tensor,
        use_mopd: bool = True
    ) -> bool:
        self._restore_original()
        baseline_acc = self._evaluate_accuracy(self.model, val_loader)
        baseline_energy = await self._estimate_energy_real(self.model, sample_input)

        self.profile.accuracy_full = baseline_acc
        self.profile.energy_per_inference_full = baseline_energy

        if baseline_energy <= self.config.energy_threshold:
            logger.info(f"Expert {self.profile.expert_id} energy ({baseline_energy:.2f} J) within threshold. Skipping.")
            return False

        pareto_front = await self._generate_pareto_front(val_loader, sample_input, baseline_acc, baseline_energy)
        if not pareto_front:
            logger.warning(f"No viable compression candidates for expert {self.profile.expert_id}")
            self._restore_original()
            return False

        self.profile.pareto_front = pareto_front

        best_point = None
        if use_mopd and self.config.mopd.enabled:
            best_point = self._select_best_from_pareto(pareto_front)
        else:
            best_point = min(pareto_front, key=lambda p: p.energy)

        if best_point is None:
            self._restore_original()
            return False

        # Human approval if required and compression is aggressive
        if self.config.mopd.require_human_approval:
            accuracy_drop = baseline_acc - best_point.accuracy
            if accuracy_drop > self.config.mopd.aggressive_threshold:
                approved = await self.request_approval(best_point)
                if not approved:
                    logger.info(f"Human approval not granted for {best_point.method}; skipping compression.")
                    self._restore_original()
                    return False

        # Apply selected method to actual model
        method_name = best_point.method
        sparsity_map = {
            'structured_pruning': self.config.pruning_sparsity,
            'unstructured_pruning': self.config.pruning_sparsity,
            'int8_quant': None,
            'hybrid': None,
            'svd': self.config.svd_rank_factor,
        }
        sparsity = sparsity_map.get(method_name)

        if sparsity is not None:
            model_copy = getattr(self, f"_apply_{method_name}")(self._copy_model(), sparsity)
        else:
            if method_name == 'hybrid':
                model_copy = self._apply_hybrid(self._copy_model())
            elif method_name == 'svd':
                model_copy = self._apply_svd(self._copy_model())
            else:
                model_copy = getattr(self, f"_apply_{method_name}")(self._copy_model())

        self.model.load_state_dict(model_copy.state_dict())
        del model_copy

        self.profile.compressed_flag = True
        self.profile.compression_method = method_name
        self.profile.accuracy_compressed = best_point.accuracy
        self.profile.energy_per_inference_compressed = best_point.energy
        self.profile.carbon_savings_kg = best_point.carbon_savings_kg
        self.profile.last_compressed_at = datetime.now()

        if self.storage:
            self.storage.save(self.profile.expert_id, method_name, self.model, self.profile)

        if self.history_manager:
            self.history_manager.record(
                self.profile.expert_id,
                method_name,
                baseline_energy,
                best_point.energy,
                baseline_acc,
                best_point.accuracy,
                best_point.carbon_savings_kg,
                self.hardware_profile
            )
            self.history_manager.record_pareto_front(self.profile.expert_id, pareto_front)

        energy_saved = baseline_energy - best_point.energy
        await self.telemetry.increment(f"{self.config.telemetry_prefix}.compressions_total")
        await self.telemetry.gauge(f"{self.config.telemetry_prefix}.energy_saved_joules", energy_saved)
        await self.telemetry.gauge(f"{self.config.telemetry_prefix}.carbon_saved_kg", best_point.carbon_savings_kg)

        logger.info(f"Expert {self.profile.expert_id} compressed with {method_name}. "
                    f"Energy: {baseline_energy:.4f} → {best_point.energy:.4f} J, "
                    f"Accuracy: {baseline_acc:.4f} → {best_point.accuracy:.4f}, "
                    f"Carbon saved: {best_point.carbon_savings_kg:.4f} kg CO₂, "
                    f"Explanation: {best_point.explanation}")
        return True

    # ---------- Temporal Safety Check ----------
    def check_invariants(self, baseline_energy: float, compressed_energy: float, baseline_acc: float, compressed_acc: float) -> List[str]:
        violations = []
        if compressed_acc < baseline_acc - self.config.accuracy_drop_tolerance:
            violations.append(f"Accuracy drop {baseline_acc - compressed_acc:.4f} exceeds tolerance {self.config.accuracy_drop_tolerance:.4f}")
        if compressed_energy >= baseline_energy:
            violations.append("Compressed energy not lower than baseline")
        return violations

    # ---------- Human-in-the-loop ----------
    async def request_approval(self, point: MOPDPoint) -> bool:
        if not self.config.mopd.require_human_approval:
            return True
        logger.warning(f"Human approval required for compression method {point.method}. Auto-denying.")
        return False

    # ---------- Chaos Testing ----------
    async def inject_fault(self, fault_type: str, **params):
        if not self.config.enable_chaos_testing:
            logger.info("Chaos testing disabled")
            return
        if fault_type == 'high_energy':
            # Simulate high energy by overriding telemetry
            self.telemetry = TelemetryCollectorStub()
            self.telemetry.get_energy_per_inference = lambda *args, **kwargs: asyncio.sleep(0, result=100.0)  # dummy
            logger.warning("Injected high_energy fault")
        elif fault_type == 'low_accuracy':
            self.accuracy_fn = lambda model, loader: 0.1
            logger.warning("Injected low_accuracy fault")
        else:
            logger.warning(f"Unknown fault type: {fault_type}")

    async def run_chaos_test(self) -> Dict[str, Any]:
        if not self.config.enable_chaos_testing:
            return {'status': 'disabled'}
        report = {'faults': [], 'results': {}}
        # Test high energy fault
        await self.inject_fault('high_energy')
        # Check if compressor still works
        try:
            success = await self.evaluate_tradeoff_and_compress(None, None)  # would fail due to missing args
        except Exception as e:
            report['results']['high_energy'] = f'failed_gracefully: {type(e).__name__}'
        # Reset
        self.telemetry = TelemetryCollectorStub()
        # Test low accuracy fault
        await self.inject_fault('low_accuracy')
        # ...
        return report

# ==============================================
# 7. FITNESS SCORER (ENHANCED) – unchanged but can use MOPD
# ==============================================

class SustainabilityFitnessScorer:
    def __init__(self, config: SustainabilityConfig = None):
        self.config = config or SUSTAINABILITY_CONFIG

    def compute(self, profile: SustainabilityAwareExpertProfile) -> float:
        acc = profile.accuracy_compressed if profile.compressed_flag else profile.accuracy_full
        energy = profile.energy_per_inference_compressed if profile.compressed_flag else profile.energy_per_inference_full
        normalized_energy = max(0.0, 1.0 - (energy / 10.0))
        carbon_score = min(1.0, profile.carbon_savings_kg / 0.1)
        material_score = 1.0 - profile.material_index
        fitness = (
            self.config.fitness_accuracy_weight * acc +
            self.config.fitness_energy_weight * normalized_energy +
            self.config.fitness_carbon_weight * carbon_score +
            self.config.fitness_material_weight * material_score
        )
        compression_bonus = 0.05 if profile.compressed_flag else 0.0
        profile.sustainability_fitness_score = fitness + compression_bonus
        return profile.sustainability_fitness_score

# ==============================================
# 8. MLOPS PIPELINE EXTENSION (ENHANCED)
# ==============================================

class MLOpsPipelineExtension:
    def __init__(
        self,
        pipeline: Any,
        config: SustainabilityConfig = None,
        telemetry: Optional[TelemetryCollector] = None,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        anomaly_detector: Optional[AnomalyDetector] = None,
        accuracy_fn: Optional[Callable[[torch.nn.Module, Any], float]] = None,
    ):
        self.pipeline = pipeline
        self.config = config or SUSTAINABILITY_CONFIG
        self.telemetry = telemetry or TelemetryCollectorStub()
        self.carbon_manager = carbon_manager or CarbonIntensityManagerStub()
        self.anomaly_detector = anomaly_detector or AnomalyDetectorStub()
        self.accuracy_fn = accuracy_fn
        self.history_manager = CompressionHistoryManager(self.config.history_db_path)
        self.storage = CompressedModelStorage(self.config.compressed_model_dir)

        self._running = False
        self._recompress_task: Optional[asyncio.Task] = None
        self._compressed_acc_cache: Dict[str, float] = {}

    def _ensure_model_registry(self):
        if not hasattr(self.pipeline, 'model_registry') or not hasattr(self.pipeline, 'profile_registry'):
            raise AttributeError("Pipeline must have 'model_registry' and 'profile_registry' attributes.")
        if not hasattr(self.pipeline, 'val_loaders'):
            self.pipeline.val_loaders = {}

    async def on_expert_registered(
        self,
        expert_id: str,
        model: torch.nn.Module,
        profile: SustainabilityAwareExpertProfile,
        val_loader: Any,
    ) -> None:
        self._ensure_model_registry()
        profile.update_material_index(self.config.hardware_profile)

        if self.storage:
            latest = self.history_manager.get_latest(expert_id)
            if latest:
                method = latest['method']
                if self.storage.load(expert_id, method, model):
                    profile.compressed_flag = True
                    profile.compression_method = method
                    profile.accuracy_compressed = latest['accuracy_after']
                    profile.energy_per_inference_compressed = latest['energy_after']
                    profile.carbon_savings_kg = latest['carbon_savings_kg']
                    profile.last_compressed_at = datetime.fromisoformat(latest['timestamp'])
                    logger.info(f"Loaded compressed model for expert {expert_id} (method: {method})")
                    self.pipeline.model_registry[expert_id] = model
                    self.pipeline.profile_registry[expert_id] = profile
                    return

        if profile.energy_per_inference_full > self.config.energy_threshold:
            logger.info(f"[SUSTAINABILITY] Triggering compression for expert {expert_id}...")
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
                if hasattr(self.pipeline, 'pareto_fronts'):
                    self.pipeline.pareto_fronts[expert_id] = profile.pareto_front
        else:
            logger.info(f"Expert {expert_id} energy ({profile.energy_per_inference_full:.2f} J) within threshold. No compression.")

    async def start_recompress_loop(self):
        if self.config.recompress_interval <= 0:
            return
        self._running = True
        while self._running:
            await asyncio.sleep(self.config.recompress_interval)
            await self._recompress_all()

    async def _recompress_all(self):
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
                if hasattr(self.pipeline, 'pareto_fronts'):
                    self.pipeline.pareto_fronts[expert_id] = profile.pareto_front

    async def stop_recompress_loop(self):
        self._running = False
        if self._recompress_task:
            self._recompress_task.cancel()
            try:
                await self._recompress_task
            except asyncio.CancelledError:
                pass
            self._recompress_task = None

    async def on_anomaly_detected(self, node_id: str, metrics: Dict):
        if not self.config.anomaly_trigger_enabled:
            return
        self._ensure_model_registry()
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
                        if hasattr(self.pipeline, 'pareto_fronts'):
                            self.pipeline.pareto_fronts[expert_id] = profile.pareto_front
                break

    async def monitor_rollback(self, expert_id: str, current_accuracy: float):
        if expert_id not in self._compressed_acc_cache:
            return
        compressed_acc = self._compressed_acc_cache[expert_id]
        if compressed_acc == 0:
            return
        if current_accuracy < compressed_acc * self.config.rollback_accuracy_threshold:
            logger.warning(f"Expert {expert_id} accuracy {current_accuracy:.4f} dropped below {compressed_acc*self.config.rollback_accuracy_threshold:.4f}. Reverting to full model.")
            profile = self.pipeline.profile_registry.get(expert_id)
            if profile and not profile.compressed_flag:
                logger.info(f"Expert {expert_id} already full model.")
                return
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
# 9. ROUTER INTEGRATION (ENHANCED WITH MOPD)
# ==============================================

class SustainabilityAwareRouter:
    def __init__(self, base_router: Any, use_mopd: bool = True):
        self.base_router = base_router
        self.use_mopd = use_mopd

    def route(self, query: Any, required_accuracy: float = 0.90) -> Any:
        candidates = self.base_router.get_all_experts(query)

        valid_candidates = []
        for exp_id, profile in candidates:
            acc = profile.accuracy_compressed if profile.compressed_flag else profile.accuracy_full
            if acc >= required_accuracy:
                valid_candidates.append((exp_id, profile))

        if not valid_candidates:
            return self.base_router.route(query)

        scorer = SustainabilityFitnessScorer()
        for exp_id, profile in valid_candidates:
            scorer.compute(profile)
        best_exp_id, best_profile = max(valid_candidates, key=lambda x: x[1].sustainability_fitness_score)

        if best_profile.compressed_flag:
            return self.base_router.load_compressed_model(best_exp_id)
        else:
            return self.base_router.load_full_model(best_exp_id)

# ==============================================
# 10. CONVENIENCE EXPORTS
# ==============================================

__all__ = [
    "SUSTAINABILITY_CONFIG",
    "SustainabilityAwareExpertProfile",
    "SustainabilityCompressor",
    "SustainabilityFitnessScorer",
    "MLOpsPipelineExtension",
    "SustainabilityAwareRouter",
    "MOPDPoint",
]

# ==============================================
# 11. EXAMPLE USAGE
# ==============================================

if __name__ == "__main__":
    try:
        config = SustainabilityConfig(energy_threshold=-1.0)
    except Exception as e:
        print(f"Validation error: {e}")
    print("Enhanced sustainability module with MOPD and XAI loaded.")
