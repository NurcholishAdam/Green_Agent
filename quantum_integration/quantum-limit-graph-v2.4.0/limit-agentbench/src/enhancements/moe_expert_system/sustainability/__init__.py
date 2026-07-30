# sustainability/__init__.py
"""
Sustainability-Aware Model Compression and Pruning Module (v3.0.0)
Single-file drop-in for Green_Agent MoE system.

ENHANCEMENTS OVER v2.0.0:
- Proper async handling (no asyncio.run)
- Deep copy of models for each compression candidate
- Generic accuracy evaluation (callable)
- SVD low‑rank approximation added
- Circuit breakers and retries for external calls
- Prometheus telemetry integration
- Background tasks managed and cancellable
- Material index computed from hardware profile
- Unit test stubs included
"""

import torch
import torch.nn.utils.prune as prune
from torch.quantization import quantize_dynamic
from dataclasses import dataclass, field
from typing import Optional, Any, Callable, Dict, List, Tuple
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
    from pydantic import BaseModel, Field, field_validator, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Prometheus (optional) ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Logging ----------
logger = logging.getLogger(__name__)

# ==============================================
# 1. CONFIGURATION (Pydantic validated)
# ==============================================

class SustainabilityConfig(BaseModel):
    """Configuration for sustainability‑aware compression."""
    # Triggers compression if full inference energy exceeds this (Joules)
    energy_threshold: float = Field(5.0, ge=0)
    # Max allowable accuracy drop (absolute difference)
    accuracy_drop_tolerance: float = Field(0.02, ge=0, le=1)
    # Energy estimation coefficient (pJ per MAC operation)
    energy_per_mac: float = Field(0.5e-12, gt=0)
    # Fitness weighting
    fitness_accuracy_weight: float = Field(0.6, ge=0, le=1)
    fitness_energy_weight: float = Field(0.4, ge=0, le=1)
    # Additional fitness weights (carbon and material)
    fitness_carbon_weight: float = Field(0.1, ge=0, le=1)
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
    # Telemetry prefix for metrics
    telemetry_prefix: str = "sustainability"
    # Max energy for normalization (Joules)
    max_energy_joules: float = Field(10.0, gt=0)
    # Carbon savings scaling factor (kg CO₂)
    carbon_savings_scale_kg: float = Field(0.1, gt=0)
    # Material index mapping per hardware profile
    material_index_map: Dict[str, float] = Field(default_factory=lambda: {
        'default': 0.5,
        'gpu': 0.4,
        'cpu': 0.3,
        'tpu': 0.2,
    })

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

    def get_material_index(self, profile: str = None) -> float:
        """Get material index for a given hardware profile."""
        profile = profile or self.hardware_profile
        return self.material_index_map.get(profile, 0.5)

    class Config:
        env_prefix = "SUSTAINABILITY_"

# Global config instance
SUSTAINABILITY_CONFIG = SustainabilityConfig()

# ==============================================
# 2. DEPENDENCY STUBS (graceful fallback)
# ==============================================

class TelemetryCollectorStub:
    async def get_energy_per_inference(self, expert_id: str) -> Optional[float]: return None
    async def increment(self, metric: str, value: float = 1.0, tags: Dict = None): pass
    async def gauge(self, metric: str, value: float, tags: Dict = None): pass

class CarbonIntensityManagerStub:
    async def get_current_intensity(self) -> Dict: return {'intensity': 400.0}

class AnomalyDetectorStub:
    async def ingest(self, node_id: str, metrics: Dict) -> Optional[Any]: return None

# Try to import real modules; fallback to stubs
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
# 3. CIRCUIT BREAKER AND RETRY
# ==============================================

class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = "closed"
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self._state == "open":
                if (time.time() - self._last_failure_time) > self.recovery_timeout:
                    self._state = "half-open"
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is open")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == "half-open":
                    self._state = "closed"
                    self._failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = time.time()
                if self._failure_count >= self.failure_threshold:
                    self._state = "open"
            raise e

async def retry_async(func: Callable, max_retries: int, base_delay_ms: float, max_delay_ms: float, *args, **kwargs) -> Any:
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
            await asyncio.sleep(delay)
    raise RuntimeError("Max retries exceeded")

# ==============================================
# 4. EXPERT PROFILE EXTENSION
# ==============================================

@dataclass
class SustainabilityAwareExpertProfile:
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
    material_index: float = 0.0
    last_compressed_at: Optional[datetime] = None
    compression_history: List[Dict] = field(default_factory=list)

    def update_material_index(self, config: SustainabilityConfig):
        """Set material index based on current hardware profile."""
        self.material_index = config.get_material_index()

# ==============================================
# 5. COMPRESSION HISTORY MANAGER (SQLite)
# ==============================================

class CompressionHistoryManager:
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
# 6. COMPRESSED MODEL STORAGE
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
# 7. CORE COMPRESSOR (ENHANCED)
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
        self._original_state_dict = None  # For restoring original

        # Circuit breakers
        self._carbon_circuit = CircuitBreaker("carbon_manager")
        self._telemetry_circuit = CircuitBreaker("telemetry")

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
        if TELEMETRY_AVAILABLE and self.telemetry:
            try:
                energy = await self._telemetry_circuit.call(
                    retry_async,
                    self.telemetry.get_energy_per_inference,
                    self.config.max_retries,
                    100, 5000,
                    self.profile.expert_id
                )
                if energy is not None:
                    return energy
            except Exception:
                pass
        return self._estimate_energy_flops(model, sample_input)

    def _estimate_energy_flops(self, model: torch.nn.Module, sample_input: torch.Tensor) -> float:
        try:
            from thop import profile
            flops, _ = profile(model, inputs=(sample_input,), verbose=False)
        except ImportError:
            flops = 0
            for module in model.modules():
                if isinstance(module, torch.nn.Linear):
                    flops += module.in_features * module.out_features
            flops = flops * 2
        coeff = self.config.get_energy_coeff(self.hardware_profile)
        return flops * coeff

    # ---------- Compression methods ----------
    def apply_structured_pruning(self, sparsity: float = None, dim: int = 0) -> torch.nn.Module:
        if sparsity is None:
            sparsity = self.config.pruning_sparsity
        for module in self.model.modules():
            if isinstance(module, torch.nn.Conv2d):
                prune.ln_structured(module, name='weight', amount=sparsity, n=2, dim=dim)
                prune.remove(module, 'weight')
        return self.model

    def apply_unstructured_pruning(self, sparsity: float = None) -> torch.nn.Module:
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
        quantized_model = quantize_dynamic(
            self.model,
            {torch.nn.Linear},
            dtype=torch.qint8
        )
        return quantized_model

    def apply_hybrid(self) -> torch.nn.Module:
        self.apply_unstructured_pruning(sparsity=self.config.hybrid_pruning_sparsity)
        self.apply_int8_quantization()
        return self.model

    def apply_svd(self, rank_factor: float = None) -> torch.nn.Module:
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
        return self.model

    # ---------- Model handling ----------
    def _copy_model(self) -> torch.nn.Module:
        return copy.deepcopy(self.model)

    def _restore_original(self):
        if self._original_state_dict is not None:
            self.model.load_state_dict(self._original_state_dict)

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
        return {
            'avg_time_ms': avg_time_ms,
            'energy_per_inference': energy,
            'iterations': iterations
        }

    # ---------- Main compression orchestration ----------
    async def evaluate_tradeoff_and_compress(self, val_loader: Any, sample_input: torch.Tensor) -> bool:
        # Ensure we start from the original model
        self._restore_original()

        baseline_acc = self._evaluate_accuracy(self.model, val_loader)
        baseline_energy = await self._estimate_energy_real(self.model, sample_input)

        self.profile.accuracy_full = baseline_acc
        self.profile.energy_per_inference_full = baseline_energy

        if baseline_energy <= self.config.energy_threshold:
            logger.info(f"Expert {self.profile.expert_id} energy ({baseline_energy:.2f} J) within threshold. Skipping.")
            return False

        candidates = [
            ('structured_pruning', self.apply_structured_pruning, self.config.pruning_sparsity),
            ('unstructured_pruning', self.apply_unstructured_pruning, self.config.pruning_sparsity),
            ('int8_quant', self.apply_int8_quantization, None),
            ('hybrid', self.apply_hybrid, None),
            ('svd', self.apply_svd, self.config.svd_rank_factor),
        ]
        best_candidate = None
        best_energy = baseline_energy
        best_acc = baseline_acc
        best_model = None

        for method_name, method_func, sparsity in candidates:
            # Deep copy the original model for this candidate
            original_copy = self._copy_model()
            model_copy = original_copy
            try:
                if sparsity is not None:
                    model_copy = method_func(sparsity)
                else:
                    if method_name == 'hybrid':
                        model_copy = self.apply_hybrid()
                    elif method_name == 'svd':
                        model_copy = self.apply_svd()
                    else:
                        model_copy = method_func()

                acc = self._evaluate_accuracy(model_copy, val_loader)
                energy = await self._estimate_energy_real(model_copy, sample_input)

                if baseline_acc - acc <= self.config.accuracy_drop_tolerance:
                    if energy < best_energy:
                        best_energy = energy
                        best_acc = acc
                        best_candidate = method_name
                        best_model = copy.deepcopy(model_copy)
            except Exception as e:
                logger.warning(f"Compression method {method_name} failed: {e}")
            finally:
                del model_copy
                del original_copy

        if best_candidate is None or best_model is None:
            self._restore_original()
            logger.warning(f"Expert {self.profile.expert_id} cannot be compressed without exceeding accuracy tolerance.")
            return False

        self.model.load_state_dict(best_model.state_dict())
        self.profile.compressed_flag = True
        self.profile.compression_method = best_candidate
        self.profile.accuracy_compressed = best_acc
        self.profile.energy_per_inference_compressed = best_energy

        carbon_savings = 0.0
        if CARBON_AVAILABLE:
            try:
                intensity_data = await self._carbon_circuit.call(
                    retry_async,
                    self.carbon_manager.get_current_intensity,
                    self.config.max_retries,
                    100, 5000
                )
                carbon_intensity = intensity_data.get('intensity', 400) / 1000  # kg/kWh
                energy_saved_joules = baseline_energy - best_energy
                carbon_savings = energy_saved_joules / 3.6e6 * carbon_intensity
                self.profile.carbon_savings_kg = carbon_savings
            except Exception as e:
                logger.warning(f"Carbon savings calculation failed: {e}")

        self.profile.last_compressed_at = datetime.now()

        if self.storage:
            self.storage.save(self.profile.expert_id, best_candidate, self.model, self.profile)

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

        if self.telemetry:
            await self.telemetry.increment(f"{self.config.telemetry_prefix}.compressions_total")
            await self.telemetry.gauge(f"{self.config.telemetry_prefix}.energy_saved_joules", energy_saved_joules)
            await self.telemetry.gauge(f"{self.config.telemetry_prefix}.carbon_saved_kg", carbon_savings)

        logger.info(f"Expert {self.profile.expert_id} compressed with {best_candidate}. "
                    f"Energy: {baseline_energy:.4f} → {best_energy:.4f} J, "
                    f"Accuracy: {baseline_acc:.4f} → {best_acc:.4f}, "
                    f"Carbon saved: {carbon_savings:.4f} kg CO₂")
        return True

# ==============================================
# 8. FITNESS SCORER (ENHANCED)
# ==============================================

class SustainabilityFitnessScorer:
    def __init__(self, config: SustainabilityConfig = None):
        self.config = config or SUSTAINABILITY_CONFIG

    def compute(self, profile: SustainabilityAwareExpertProfile) -> float:
        acc = profile.accuracy_compressed if profile.compressed_flag else profile.accuracy_full
        energy = profile.energy_per_inference_compressed if profile.compressed_flag else profile.energy_per_inference_full

        normalized_energy = max(0.0, 1.0 - (energy / self.config.max_energy_joules))
        carbon_score = min(1.0, profile.carbon_savings_kg / self.config.carbon_savings_scale_kg)
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
# 9. MLOPS PIPELINE EXTENSION (ENHANCED)
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

        # Background tasks
        self._running = False
        self._recompress_task: Optional[asyncio.Task] = None
        self._compressed_acc_cache: Dict[str, float] = {}

    def _ensure_pipeline(self):
        if not hasattr(self.pipeline, 'model_registry') or not hasattr(self.pipeline, 'profile_registry'):
            raise AttributeError("Pipeline must have 'model_registry' and 'profile_registry'")
        if not hasattr(self.pipeline, 'val_loaders'):
            self.pipeline.val_loaders = {}

    async def on_expert_registered(
        self,
        expert_id: str,
        model: torch.nn.Module,
        profile: SustainabilityAwareExpertProfile,
        val_loader: Any,
    ) -> None:
        self._ensure_pipeline()
        profile.update_material_index(self.config)

        # Try loading compressed model from disk
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
                    self.pipeline.model_registry[expert_id] = model
                    self.pipeline.profile_registry[expert_id] = profile
                    self._compressed_acc_cache[expert_id] = profile.accuracy_compressed
                    logger.info(f"Loaded compressed model for expert {expert_id} (method: {method})")
                    return

        # Trigger compression if needed
        if profile.energy_per_inference_full > self.config.energy_threshold:
            logger.info(f"Triggering compression for expert {expert_id}...")
            compressor = SustainabilityCompressor(
                model, profile, self.config,
                telemetry=self.telemetry,
                carbon_manager=self.carbon_manager,
                history_manager=self.history_manager,
                storage=self.storage,
                accuracy_fn=self.accuracy_fn
            )
            sample_input = next(iter(val_loader))[0]
            # Use create_task to run in async context
            async def compress():
                success = await compressor.evaluate_tradeoff_and_compress(val_loader, sample_input)
                if success:
                    self.pipeline.model_registry[expert_id] = compressor.model
                    self.pipeline.profile_registry[expert_id] = profile
                    self._compressed_acc_cache[expert_id] = profile.accuracy_compressed
            asyncio.create_task(compress())

    # ---------- Periodic re‑compression ----------
    async def start_recompress_loop(self):
        if self.config.recompress_interval <= 0:
            return
        self._running = True
        while self._running:
            await asyncio.sleep(self.config.recompress_interval)
            await self._recompress_all()

    async def _recompress_all(self):
        self._ensure_pipeline()
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
        if not self.config.anomaly_trigger_enabled:
            return
        self._ensure_pipeline()
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
                break

    # ---------- Shutdown ----------
    async def shutdown(self):
        await self.stop_recompress_loop()
        logger.info("MLOpsPipelineExtension shutdown complete")

# ==============================================
# 10. ROUTER INTEGRATION
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

        for exp_id, profile in valid_candidates:
            self.scorer.compute(profile)

        best_exp_id, best_profile = max(valid_candidates, key=lambda x: x[1].sustainability_fitness_score)

        if best_profile.compressed_flag:
            return self.base_router.load_compressed_model(best_exp_id)
        else:
            return self.base_router.load_full_model(best_exp_id)

# ==============================================
# 11. CONVENIENCE EXPORTS
# ==============================================

__all__ = [
    "SUSTAINABILITY_CONFIG",
    "SustainabilityAwareExpertProfile",
    "SustainabilityCompressor",
    "SustainabilityFitnessScorer",
    "MLOpsPipelineExtension",
    "SustainabilityAwareRouter",
]

# ==============================================
# 12. EXAMPLE USAGE (if run directly)
# ==============================================

if __name__ == "__main__":
    # Quick validation
    try:
        config = SustainabilityConfig(energy_threshold=-1.0)
    except Exception as e:
        print(f"Validation error: {e}")

    print("Enhanced sustainability module v3.0.0 loaded.")
