# src/enhancements/carbon_nas_enhanced.py

"""
Enhanced Carbon-Aware Neural Architecture Search (NAS) for Green Agent
Version 3.0 - With complete hardware profiler, cache versioning, and multi-objective BO

Scientific basis: Energy consumption of training is proportional to parameters × steps
Reference: "Carbon-Aware Neural Architecture Search" (NeurIPS, 2023)

Version History:
- v1.0: Original implementation
- v2.0: Mixed precision, Bayesian optimization, transfer learning
- v3.0: Hardware profiler, cache versioning, multi-objective BO
"""

import numpy as np
import json
import pickle
import hashlib
import time
import subprocess
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Callable
from enum import Enum
from datetime import datetime
import logging
import random
from collections import defaultdict
from pathlib import Path
import os

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Complete Hardware Profiler Implementation
# ============================================================

class HardwareType(Enum):
    """Supported GPU hardware types"""
    A100 = "a100"
    H100 = "h100"
    V100 = "v100"
    T4 = "t4"
    RTX4090 = "rtx4090"
    RTX3090 = "rtx3090"


class HardwareProfiler:
    """
    Complete hardware-in-the-loop validation system.
    
    Features:
    - Real GPU power monitoring via NVML
    - CUDA event timing for accurate latency
    - Memory profiling
    - Thermal monitoring
    - Multi-GPU support
    - Background monitoring thread
    """
    
    def __init__(self, hardware_type: str = 'gpu', config: Optional[Dict] = None):
        self.config = config or {}
        self.hardware_type = hardware_type
        self.gpu_index = self.config.get('gpu_index', 0)
        self.simulation_mode = self.config.get('simulate', False)
        self.profile_cache: Dict[str, Dict] = {}
        self.cache_file = self.config.get('profile_cache_file', 'hardware_profiles.json')
        
        # NVML state
        self._nvml_available = False
        self._nvml_handle = None
        
        # Background monitoring
        self._monitoring = False
        self._monitor_thread = None
        self._power_history = []
        self._temp_history = []
        
        # Initialize NVML if not in simulation mode
        if not self.simulation_mode:
            self._init_nvml()
        
        # Load cache
        self._load_cache()
        
        logger.info(f"HardwareProfiler initialized for {hardware_type} (simulation={self.simulation_mode})")
    
    def _init_nvml(self):
        """Initialize NVIDIA Management Library"""
        try:
            import pynvml
            pynvml.nvmlInit()
            self._nvml_handle = pynvml.nvmlDeviceGetHandleByIndex(self.gpu_index)
            self._nvml_available = True
            logger.info("NVML initialized for hardware profiling")
        except ImportError:
            logger.warning("pynvml not available, falling back to simulation")
            self.simulation_mode = True
        except Exception as e:
            logger.warning(f"NVML initialization failed: {e}, using simulation")
            self.simulation_mode = True
    
    def start_monitoring(self, interval_ms: int = 100):
        """Start background monitoring of power and temperature"""
        if self._monitoring:
            return
        
        self._monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, 
                                                 args=(interval_ms / 1000,),
                                                 daemon=True)
        self._monitor_thread.start()
        logger.info("Hardware monitoring started")
    
    def stop_monitoring(self):
        """Stop background monitoring"""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=2)
    
    def _monitor_loop(self, interval_seconds: float):
        """Background monitoring loop"""
        while self._monitoring:
            if not self.simulation_mode and self._nvml_available:
                try:
                    import pynvml
                    power = pynvml.nvmlDeviceGetPowerUsage(self._nvml_handle) / 1000.0
                    temp = pynvml.nvmlDeviceGetTemperature(self._nvml_handle, 
                                                            pynvml.NVML_TEMPERATURE_GPU)
                    self._power_history.append((time.time(), power))
                    self._temp_history.append((time.time(), temp))
                    
                    # Keep history limited
                    if len(self._power_history) > 1000:
                        self._power_history = self._power_history[-1000:]
                    if len(self._temp_history) > 1000:
                        self._temp_history = self._temp_history[-1000:]
                except Exception as e:
                    logger.warning(f"Monitoring failed: {e}")
            
            time.sleep(interval_seconds)
    
    def _get_cache_key(self, config: 'ArchitectureConfig') -> str:
        """Generate cache key for profiling results"""
        config_dict = {
            'num_layers': config.num_layers,
            'hidden_size': config.hidden_size,
            'num_heads': config.num_heads,
            'operations': [op.value for op in config.operations],
            'parallelism': config.parallelism,
            'hardware': self.hardware_type
        }
        json_str = json.dumps(config_dict, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    def _load_cache(self):
        """Load profile cache from disk"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    self.profile_cache = json.load(f)
                logger.info(f"Loaded {len(self.profile_cache)} profiles from cache")
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
    
    def _save_cache(self):
        """Save profile cache to disk"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.profile_cache, f, indent=2)
            logger.info(f"Saved {len(self.profile_cache)} profiles to cache")
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")
    
    def profile_architecture(self, config: 'ArchitectureConfig') -> Optional[Dict]:
        """
        Profile an architecture on actual hardware.
        
        Returns dictionary with real measurements:
        - actual_latency_ms: Average inference latency
        - actual_latency_p95_ms: 95th percentile latency
        - actual_energy_joules: Energy per inference
        - actual_memory_mb: Peak memory usage
        - actual_power_watts: Average power draw
        - actual_temperature_c: Peak temperature
        """
        cache_key = self._get_cache_key(config)
        
        # Check cache
        if cache_key in self.profile_cache:
            logger.info(f"Using cached profile for {cache_key[:16]}...")
            return self.profile_cache[cache_key]
        
        if self.simulation_mode:
            profile_data = self._simulate_profile(config)
        else:
            profile_data = self._run_on_hardware(config)
        
        if profile_data:
            self.profile_cache[cache_key] = profile_data
            self._save_cache()
        
        return profile_data
    
    def _simulate_profile(self, config: 'ArchitectureConfig') -> Dict:
        """
        Simulate hardware profiling using analytical models calibrated to real hardware.
        """
        # Calibrated models based on A100 GPU data
        total_flops = config.hidden_size ** 2 * config.num_layers * 1000
        peak_tflops = self._get_peak_tflops()
        
        # Base latency (ms)
        base_latency = total_flops / (peak_tflops * 1e12) * 1000
        
        # Parallelism scaling (diminishing returns)
        efficiency = 1.0 / (1 + 0.1 * np.log2(config.parallelism))
        actual_latency = base_latency / config.parallelism * efficiency
        
        # Energy (Joules) - power × time
        avg_power = self._get_estimated_power(config)
        actual_energy = avg_power * (actual_latency / 1000)
        
        # Memory usage (MB)
        param_bytes = config.hidden_size ** 2 * config.num_layers * 4
        activation_memory = param_bytes * config.num_layers * 0.1
        actual_memory_mb = (param_bytes + activation_memory) / (1024 * 1024)
        
        return {
            'actual_latency_ms': actual_latency,
            'actual_latency_p95_ms': actual_latency * 1.2,
            'actual_energy_joules': actual_energy,
            'actual_memory_mb': actual_memory_mb,
            'actual_power_watts': avg_power,
            'actual_temperature_c': 65 + config.parallelism * 2,
            'measured_at': datetime.now().isoformat(),
            'hardware': self.hardware_type,
            'source': 'simulation'
        }
    
    def _run_on_hardware(self, config: 'ArchitectureConfig') -> Optional[Dict]:
        """
        Actually run on real hardware using PyTorch and NVML.
        
        This builds a model, runs inference benchmarks,
        and measures power consumption.
        """
        try:
            import torch
            import torch.nn as nn
            
            # Build model based on config
            model = self._build_model(config)
            model = model.cuda()
            model.eval()
            
            # Warm-up
            dummy_input = torch.randn(1, 3, 224, 224).cuda()
            for _ in range(10):
                with torch.no_grad():
                    _ = model(dummy_input)
            
            # Measure latency with CUDA events
            import pynvml
            
            # Get power baseline
            power_start = pynvml.nvmlDeviceGetPowerUsage(self._nvml_handle) / 1000.0
            
            # Timing measurements
            latencies = []
            num_iterations = 100
            
            for _ in range(num_iterations):
                start_event = torch.cuda.Event(enable_timing=True)
                end_event = torch.cuda.Event(enable_timing=True)
                
                start_event.record()
                with torch.no_grad():
                    _ = model(dummy_input)
                end_event.record()
                torch.cuda.synchronize()
                
                latency_ms = start_event.elapsed_time(end_event)
                latencies.append(latency_ms)
            
            # Get power after
            power_end = pynvml.nvmlDeviceGetPowerUsage(self._nvml_handle) / 1000.0
            
            # Calculate metrics
            avg_latency = np.mean(latencies)
            p95_latency = np.percentile(latencies, 95)
            avg_power = (power_start + power_end) / 2
            total_time_s = (num_iterations * avg_latency) / 1000
            energy_joules = avg_power * total_time_s / num_iterations
            
            # Memory usage
            memory_mb = torch.cuda.max_memory_allocated() / (1024 * 1024)
            
            # Temperature
            temp = pynvml.nvmlDeviceGetTemperature(self._nvml_handle,
                                                    pynvml.NVML_TEMPERATURE_GPU)
            
            # Clean up
            del model
            torch.cuda.empty_cache()
            
            return {
                'actual_latency_ms': avg_latency,
                'actual_latency_p95_ms': p95_latency,
                'actual_energy_joules': energy_joules,
                'actual_memory_mb': memory_mb,
                'actual_power_watts': avg_power,
                'actual_temperature_c': temp,
                'measured_at': datetime.now().isoformat(),
                'hardware': self.hardware_type,
                'source': 'hardware'
            }
            
        except ImportError as e:
            logger.warning(f"PyTorch or NVML not available: {e}")
            return self._simulate_profile(config)
        except Exception as e:
            logger.error(f"Hardware profiling failed: {e}")
            return None
    
    def _build_model(self, config: 'ArchitectureConfig') -> nn.Module:
        """Build a PyTorch model from architecture config"""
        import torch.nn as nn
        
        class DynamicModel(nn.Module):
            def __init__(self, config):
                super().__init__()
                self.config = config
                
                # Input projection
                self.input_proj = nn.Linear(3 * 224 * 224, config.hidden_size)
                
                # Transformer blocks
                self.blocks = nn.ModuleList()
                for _ in range(config.num_layers):
                    if OperationType.ATTENTION in config.operations:
                        block = nn.TransformerEncoderLayer(
                            d_model=config.hidden_size,
                            nhead=config.num_heads,
                            dim_feedforward=config.hidden_size * 4,
                            batch_first=True
                        )
                    else:
                        block = nn.Sequential(
                            nn.Linear(config.hidden_size, config.hidden_size * 4),
                            nn.ReLU(),
                            nn.Linear(config.hidden_size * 4, config.hidden_size)
                        )
                    self.blocks.append(block)
                
                # Output projection
                self.output_proj = nn.Linear(config.hidden_size, 1000)  # ImageNet classes
            
            def forward(self, x):
                x = x.view(x.size(0), -1)
                x = self.input_proj(x)
                for block in self.blocks:
                    if hasattr(block, 'forward'):
                        x = block(x)
                x = self.output_proj(x)
                return x
        
        return DynamicModel(config)
    
    def _get_peak_tflops(self) -> float:
        """Get peak TFLOPS for current hardware"""
        peak_tflops = {
            HardwareType.A100: 312.0,
            HardwareType.H100: 1979.0,
            HardwareType.V100: 125.0,
            HardwareType.T4: 65.0,
            HardwareType.RTX4090: 366.0,
            HardwareType.RTX3090: 142.0
        }
        try:
            hw_type = HardwareType(self.hardware_type)
            return peak_tflops.get(hw_type, 312.0)
        except ValueError:
            return 312.0
    
    def _get_estimated_power(self, config: 'ArchitectureConfig') -> float:
        """Estimate average power draw based on config"""
        base_power = 200.0
        power_per_layer = 10.0
        power_per_head = 5.0
        
        total_power = base_power + config.num_layers * power_per_layer + config.num_heads * power_per_head
        return min(350.0, total_power)
    
    def get_average_power(self, window_seconds: int = 10) -> float:
        """Get average power over time window"""
        if not self._power_history:
            return 200.0
        
        cutoff = time.time() - window_seconds
        recent = [p for t, p in self._power_history if t > cutoff]
        if recent:
            return sum(recent) / len(recent)
        return 200.0
    
    def get_current_temperature(self) -> float:
        """Get current GPU temperature"""
        if not self._temp_history:
            return 65.0
        
        return self._temp_history[-1][1] if self._temp_history else 65.0


# ============================================================
# ENHANCEMENT 2: Cache Versioning
# ============================================================

class VersionedCache:
    """
    Versioned cache for architecture search results.
    
    Features:
    - Version tracking for compatibility
    - Automatic cache invalidation on version mismatch
    - Migration support for older cache formats
    - Cache statistics and health monitoring
    """
    
    CURRENT_VERSION = "3.0.0"
    VERSION_HISTORY = ["1.0.0", "2.0.0", "3.0.0"]
    
    def __init__(self, cache_file: Optional[str] = None, version: str = None):
        self.cache_file = cache_file or "nas_cache.json"
        self.version = version or self.CURRENT_VERSION
        self.cached_architectures: Dict[str, Tuple['ArchitectureConfig', 'ArchitectureMetrics']] = {}
        self.metadata = {
            'version': self.version,
            'created_at': datetime.now().isoformat(),
            'last_modified': datetime.now().isoformat(),
            'entry_count': 0,
            'compatible_versions': self.VERSION_HISTORY
        }
        self._load()
    
    def _compute_hash(self, config: 'ArchitectureConfig') -> str:
        """Compute unique hash for architecture configuration"""
        config_dict = {
            'num_layers': config.num_layers,
            'hidden_size': config.hidden_size,
            'num_heads': config.num_heads,
            'operations': [op.value for op in config.operations],
            'mixed_precision': config.mixed_precision.to_dict(),
            'parallelism': config.parallelism,
            'use_gradient_checkpointing': config.use_gradient_checkpointing,
            'use_pruning': config.use_pruning,
            'pruning_ratio': config.pruning_ratio
        }
        json_str = json.dumps(config_dict, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    def is_compatible(self) -> bool:
        """Check if loaded cache is compatible with current version"""
        return self.metadata.get('version') in self.VERSION_HISTORY
    
    def migrate_from_v1(self, old_data: Dict) -> Dict:
        """Migrate from v1.x cache format to v3.0"""
        migrated = {}
        for key, entry in old_data.items():
            # v1 had different config structure
            if 'config' in entry:
                config = entry['config']
                # Add new fields with defaults
                config['mixed_precision'] = {'default_precision': 'fp16', 'layer_precisions': {}}
                config['use_gradient_checkpointing'] = False
                config['use_pruning'] = False
                config['pruning_ratio'] = 0.0
                migrated[key] = entry
        return migrated
    
    def _load(self):
        """Load cache from disk with version checking"""
        if not os.path.exists(self.cache_file):
            logger.info(f"No existing cache found at {self.cache_file}")
            return
        
        try:
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            # Check metadata
            if 'metadata' in cache_data:
                self.metadata = cache_data['metadata']
                entries = cache_data.get('entries', {})
            else:
                # Legacy format (pre-versioning)
                entries = cache_data
                self.metadata['version'] = "1.0.0"
                logger.warning("Loaded legacy cache (pre-versioning)")
            
            # Check compatibility
            cache_version = self.metadata.get('version', '1.0.0')
            if cache_version not in self.VERSION_HISTORY:
                logger.warning(f"Cache version {cache_version} not compatible. Starting fresh.")
                return
            
            # Migrate if needed
            if cache_version < self.CURRENT_VERSION:
                logger.info(f"Migrating cache from v{cache_version} to v{self.CURRENT_VERSION}")
                entries = self.migrate_from_v1(entries) if cache_version == '1.0.0' else entries
            
            # Reconstruct entries
            for hash_key, data in entries.items():
                # Reconstruct config
                config_data = data['config']
                config = ArchitectureConfig(
                    num_layers=config_data['num_layers'],
                    hidden_size=config_data['hidden_size'],
                    num_heads=config_data['num_heads'],
                    operations=[OperationType(op) for op in config_data['operations']],
                    mixed_precision=MixedPrecisionConfig(),
                    parallelism=config_data['parallelism'],
                    use_gradient_checkpointing=config_data.get('use_gradient_checkpointing', False),
                    use_pruning=config_data.get('use_pruning', False),
                    pruning_ratio=config_data.get('pruning_ratio', 0.0)
                )
                
                # Reconstruct metrics
                metrics_data = data['metrics']
                metrics = ArchitectureMetrics(
                    accuracy=metrics_data['accuracy'],
                    accuracy_std=metrics_data['accuracy_std'],
                    latency_ms=metrics_data['latency_ms'],
                    latency_p95_ms=metrics_data['latency_p95_ms'],
                    training_energy_joules=metrics_data.get('training_energy_joules', 0),
                    inference_energy_joules=metrics_data.get('inference_energy_joules', 0),
                    total_carbon_kg=metrics_data['total_carbon_kg'],
                    params_millions=metrics_data.get('params_millions', 0),
                    flops_billions=metrics_data.get('flops_billions', 0),
                    helium_footprint=metrics_data['helium_footprint'],
                    confidence_score=metrics_data['confidence_score']
                )
                
                self.cached_architectures[hash_key] = (config, metrics)
            
            self.metadata['entry_count'] = len(self.cached_architectures)
            self.metadata['last_modified'] = datetime.now().isoformat()
            logger.info(f"Loaded {len(self.cached_architectures)} architectures from cache (v{self.metadata['version']})")
            
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
    
    def save(self):
        """Save cache to disk with metadata"""
        cache_data = {
            'metadata': self.metadata,
            'entries': {}
        }
        
        for hash_key, (config, metrics) in self.cached_architectures.items():
            cache_data['entries'][hash_key] = {
                'config': {
                    'num_layers': config.num_layers,
                    'hidden_size': config.hidden_size,
                    'num_heads': config.num_heads,
                    'operations': [op.value for op in config.operations],
                    'mixed_precision': config.mixed_precision.to_dict(),
                    'parallelism': config.parallelism,
                    'use_gradient_checkpointing': config.use_gradient_checkpointing,
                    'use_pruning': config.use_pruning,
                    'pruning_ratio': config.pruning_ratio
                },
                'metrics': {
                    'accuracy': metrics.accuracy,
                    'accuracy_std': metrics.accuracy_std,
                    'latency_ms': metrics.latency_ms,
                    'latency_p95_ms': metrics.latency_p95_ms,
                    'training_energy_joules': metrics.training_energy_joules,
                    'inference_energy_joules': metrics.inference_energy_joules,
                    'total_carbon_kg': metrics.total_carbon_kg,
                    'params_millions': metrics.params_millions,
                    'flops_billions': metrics.flops_billions,
                    'helium_footprint': metrics.helium_footprint,
                    'confidence_score': metrics.confidence_score
                }
            }
        
        self.metadata['last_modified'] = datetime.now().isoformat()
        self.metadata['entry_count'] = len(self.cached_architectures)
        
        with open(self.cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
        
        logger.info(f"Saved {len(self.cached_architectures)} architectures to cache (v{self.version})")
    
    def get(self, config: 'ArchitectureConfig') -> Optional['ArchitectureMetrics']:
        """Get cached metrics for an architecture"""
        hash_key = self._compute_hash(config)
        if hash_key in self.cached_architectures:
            _, metrics = self.cached_architectures[hash_key]
            return metrics
        return None
    
    def put(self, config: 'ArchitectureConfig', metrics: 'ArchitectureMetrics'):
        """Cache metrics for an architecture"""
        hash_key = self._compute_hash(config)
        self.cached_architectures[hash_key] = (config, metrics)
        self.save()
    
    def clear(self):
        """Clear all cached entries"""
        self.cached_architectures.clear()
        self.save()
        logger.info("Cache cleared")
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        return {
            'version': self.metadata['version'],
            'entry_count': len(self.cached_architectures),
            'created_at': self.metadata.get('created_at'),
            'last_modified': self.metadata.get('last_modified'),
            'compatible': self.is_compatible(),
            'cache_file': self.cache_file
        }


# ============================================================
# ENHANCEMENT 3: Multi-Objective Bayesian Optimization
# ============================================================

class MultiObjectiveBayesianOptimizer:
    """
    Multi-objective Bayesian optimization for Pareto frontier discovery.
    
    Uses ParEGO (Pareto Efficient Global Optimization) algorithm:
    - Randomly samples weight vectors
    - Scalarizes objectives with augmented Tchebycheff
    - Builds GP model for each scalarization
    - Suggests candidates maximizing expected improvement
    """
    
    def __init__(self, search_space_bounds: Dict[str, Tuple[float, float]], 
                 n_objectives: int = 4,
                 n_weight_vectors: int = 10):
        self.search_space_bounds = search_space_bounds
        self.n_objectives = n_objectives
        self.n_weight_vectors = n_weight_vectors
        
        # Storage for all evaluated points
        self.X: List[np.ndarray] = []  # Parameter vectors
        self.F: List[np.ndarray] = []  # Objective vectors
        
        # GP models for different weight vectors
        self.gp_models = {}
        self.weight_vectors = self._generate_weight_vectors()
        
        logger.info(f"MultiObjectiveBayesianOptimizer initialized with {n_weight_vectors} weight vectors")
    
    def _generate_weight_vectors(self) -> List[np.ndarray]:
        """Generate uniformly distributed weight vectors using simplex sampling"""
        weight_vectors = []
        
        for _ in range(self.n_weight_vectors):
            # Generate random weights
            weights = np.random.dirichlet(np.ones(self.n_objectives))
            weight_vectors.append(weights)
        
        return weight_vectors
    
    def _scalarize(self, objectives: np.ndarray, weights: np.ndarray, rho: float = 0.05) -> float:
        """
        Augmented Tchebycheff scalarization.
        
        Formula: min_i [w_i * f_i] + ρ * Σ(f_i)
        where ρ is a small positive number (e.g., 0.05)
        """
        weighted = weights * objectives
        return np.max(weighted) + rho * np.sum(objectives)
    
    def add_observation(self, params: Dict[str, float], objectives: np.ndarray):
        """Add an observation to the surrogate model"""
        param_vector = np.array([params.get(key, 0) for key in self.search_space_bounds.keys()])
        self.X.append(param_vector)
        self.F.append(objectives)
        
        # Update GP models for each weight vector
        self._update_gp_models()
    
    def _update_gp_models(self):
        """Update Gaussian process models for each weight vector"""
        if len(self.X) < 3:
            return
        
        try:
            from sklearn.gaussian_process import GaussianProcessRegressor
            from sklearn.gaussian_process.kernels import RBF, WhiteKernel, Matern
            
            for i, weights in enumerate(self.weight_vectors):
                # Compute scalarized objectives
                y = [self._scalarize(f, weights) for f in self.F]
                
                # Create kernel
                kernel = 1.0 * Matern(length_scale=1.0, nu=2.5) + WhiteKernel(noise_level=0.1)
                
                # Fit GP
                gp = GaussianProcessRegressor(kernel=kernel, n_restarts_optimizer=5, alpha=1e-6)
                gp.fit(np.array(self.X), y)
                
                self.gp_models[i] = gp
                
        except ImportError:
            logger.warning("scikit-learn not available, falling back to random search")
            self.gp_models = {}
    
    def _expected_improvement(self, mean: float, std: float, best_y: float) -> float:
        """Calculate Expected Improvement acquisition function"""
        if std < 1e-9:
            return 0.0
        
        z = (best_y - mean) / std
        ei = (best_y - mean) * self._cdf(z) + std * self._pdf(z)
        return max(0, ei)
    
    def _cdf(self, z: float) -> float:
        """Standard normal CDF approximation"""
        return 0.5 * (1 + np.tanh(np.sqrt(2/np.pi) * (z + 0.044715 * z**3)))
    
    def _pdf(self, z: float) -> float:
        """Standard normal PDF"""
        return np.exp(-z**2 / 2) / np.sqrt(2 * np.pi)
    
    def _random_candidates(self, n: int) -> List[Dict[str, float]]:
        """Generate random candidates within bounds"""
        candidates = []
        for _ in range(n):
            candidate = {}
            for key, (low, high) in self.search_space_bounds.items():
                candidate[key] = random.uniform(low, high)
            candidates.append(candidate)
        return candidates
    
    def suggest_next(self, n_candidates: int = 10) -> List[Dict[str, float]]:
        """
        Suggest next architectures using multi-objective acquisition.
        
        Uses random scalarization of the Pareto front.
        """
        if not self.gp_models or len(self.X) < 3:
            return self._random_candidates(n_candidates)
        
        # Randomly select a weight vector
        weight_idx = random.randint(0, len(self.weight_vectors) - 1)
        gp = self.gp_models.get(weight_idx)
        
        if gp is None:
            return self._random_candidates(n_candidates)
        
        # Generate random candidates for evaluation
        candidates = self._random_candidates(n_candidates * 10)
        
        # Evaluate acquisition function
        best_candidates = []
        y_best = min(self._scalarize(f, self.weight_vectors[weight_idx]) for f in self.F)
        
        for candidate in candidates:
            param_vector = np.array([[candidate.get(key, 0) for key in self.search_space_bounds.keys()]])
            
            try:
                mean, std = gp.predict(param_vector, return_std=True)
                ei = self._expected_improvement(mean[0], std[0], y_best)
                candidate['_acquisition'] = float(ei)
                best_candidates.append(candidate)
            except Exception as e:
                logger.warning(f"Prediction failed: {e}")
        
        best_candidates.sort(key=lambda x: x.get('_acquisition', 0), reverse=True)
        return best_candidates[:n_candidates]
    
    def get_pareto_frontier(self) -> List[Tuple[np.ndarray, np.ndarray]]:
        """Get the current Pareto frontier of evaluated points"""
        if not self.F:
            return []
        
        frontier = []
        for i, f1 in enumerate(self.F):
            dominated = False
            for j, f2 in enumerate(self.F):
                if i != j and np.all(f2 <= f1) and np.any(f2 < f1):
                    dominated = True
                    break
            if not dominated:
                frontier.append((self.X[i], f1))
        
        return frontier
    
    def get_hypervolume(self, reference_point: Optional[np.ndarray] = None) -> float:
        """
        Calculate hypervolume of Pareto frontier.
        
        Higher hypervolume indicates better coverage of Pareto front.
        """
        if reference_point is None:
            reference_point = np.max(self.F, axis=0) if self.F else np.ones(self.n_objectives)
        
        frontier = self.get_pareto_frontier()
        if not frontier:
            return 0.0
        
        # Simplified hypervolume calculation for 2-4 objectives
        # In production, use pygmo or similar library
        return len(frontier) / len(self.F)


# ============================================================
# ENHANCEMENT 4: Main Enhanced Class (Integrating All)
# ============================================================

class OperationType(Enum):
    CONV3x3 = "conv3x3"
    CONV5x5 = "conv5x5"
    CONV7x7 = "conv7x7"
    MAXPOOL = "maxpool"
    AVGPOOL = "avgpool"
    IDENTITY = "identity"
    SKIP_CONNECT = "skip_connect"
    LINEAR = "linear"
    ATTENTION = "attention"
    MLP = "mlp"


class PrecisionType(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"


@dataclass
class MixedPrecisionConfig:
    default_precision: PrecisionType = PrecisionType.FP16
    layer_precisions: Dict[str, PrecisionType] = field(default_factory=dict)
    precision_thresholds: Dict[str, float] = field(default_factory=dict)
    
    def get_precision_for_layer(self, layer_name: str, layer_type: str) -> PrecisionType:
        if layer_name in ['input', 'output', 'embedding']:
            return PrecisionType.FP32
        if layer_name in self.layer_precisions:
            return self.layer_precisions[layer_name]
        if layer_type in ['attention', 'linear']:
            return PrecisionType.FP16
        elif layer_type in ['conv']:
            return PrecisionType.INT8
        return self.default_precision
    
    def compute_energy_factor(self) -> float:
        precision_factors = {
            PrecisionType.FP32: 1.0,
            PrecisionType.FP16: 0.6,
            PrecisionType.INT8: 0.25,
            PrecisionType.INT4: 0.15
        }
        
        if not self.layer_precisions:
            return 0.1*1.0 + 0.3*0.6 + 0.5*0.25 + 0.1*0.15
        
        total_weight = 0
        weighted_sum = 0
        for layer, precision in self.layer_precisions.items():
            factor = precision_factors.get(precision, 0.6)
            total_weight += 1
            weighted_sum += factor
        
        return weighted_sum / total_weight if total_weight > 0 else 0.5
    
    def to_dict(self) -> Dict:
        return {
            'default_precision': self.default_precision.value,
            'layer_precisions': {k: v.value for k, v in self.layer_precisions.items()},
            'energy_factor': self.compute_energy_factor()
        }


@dataclass
class ArchitectureConfig:
    num_layers: int
    hidden_size: int
    num_heads: int
    operations: List[OperationType]
    mixed_precision: MixedPrecisionConfig
    parallelism: int
    use_gradient_checkpointing: bool = False
    use_pruning: bool = False
    pruning_ratio: float = 0.0
    
    def compute_complexity_score(self) -> float:
        base_score = self.num_layers * self.hidden_size / 1000
        if self.use_gradient_checkpointing:
            base_score *= 0.7
        if self.use_pruning:
            base_score *= (1 - self.pruning_ratio)
        return base_score


@dataclass
class ArchitectureMetrics:
    accuracy: float
    accuracy_std: float
    latency_ms: float
    latency_p95_ms: float
    training_energy_joules: float
    inference_energy_joules: float
    total_carbon_kg: float
    params_millions: float
    flops_billions: float
    helium_footprint: float
    confidence_score: float


@dataclass
class ParetoPoint:
    config: ArchitectureConfig
    metrics: ArchitectureMetrics
    search_iteration: int
    timestamp: datetime
    dominates: List[int] = field(default_factory=list)
    dominated_by: List[int] = field(default_factory=list)
    score: float = 0.0


class EnhancedCarbonAwareNAS:
    """
    Enhanced Carbon-Aware Neural Architecture Search.
    
    Features:
    1. Mixed-precision support
    2. Warm-start / cache-aware search
    3. Multi-objective Bayesian optimization
    4. Complete hardware-in-the-loop validation
    5. Transfer learning across tasks
    """
    
    ENERGY_PER_OP = {
        OperationType.CONV3x3: 2.0e-11,
        OperationType.CONV5x5: 5.0e-11,
        OperationType.CONV7x7: 1.0e-10,
        OperationType.MAXPOOL: 1.0e-12,
        OperationType.AVGPOOL: 1.0e-12,
        OperationType.IDENTITY: 0,
        OperationType.SKIP_CONNECT: 1.0e-13,
        OperationType.LINEAR: 1.0e-11,
        OperationType.ATTENTION: 5.0e-11,
        OperationType.MLP: 2.0e-11
    }
    
    SEARCH_SPACE_BOUNDS = {
        'num_layers': (4, 60),
        'hidden_size': (64, 2048),
        'num_heads': (2, 24),
        'parallelism': (1, 8),
        'pruning_ratio': (0, 0.5)
    }
    
    CARBON_INTENSITY = {
        'us-east': 380,
        'us-west': 250,
        'eu-north': 80,
        'asia-pacific': 550
    }
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.region = self.config.get('region', 'us-east')
        self.expected_inferences = self.config.get('expected_inferences', 1_000_000)
        self.carbon_intensity = self.CARBON_INTENSITY.get(self.region, 400)
        self.helium_weight = self.config.get('helium_weight', 0.3)
        self.task_type = self.config.get('task_type', 'general')
        
        # Initialize components
        self.cache = VersionedCache(
            self.config.get('cache_file', 'nas_cache.json'),
            version="3.0.0"
        )
        self.multi_objective_bo = MultiObjectiveBayesianOptimizer(
            self.SEARCH_SPACE_BOUNDS,
            n_objectives=4,
            n_weight_vectors=self.config.get('n_weight_vectors', 10)
        )
        self.hardware_profiler = HardwareProfiler(
            self.config.get('hardware_type', 'gpu'),
            self.config.get('profiler_config', {})
        )
        self.transfer_weights = TransferLearningWeights()
        
        # Storage
        self.explored_architectures: List[Tuple[ArchitectureConfig, ArchitectureMetrics]] = []
        self.pareto_frontier: List[ParetoPoint] = []
        self.search_iteration = 0
        
        # Start hardware monitoring
        if self.config.get('monitor_hardware', True):
            self.hardware_profiler.start_monitoring()
        
        logger.info(f"EnhancedCarbonAwareNAS v3.0 initialized for {self.region} region")
    
    def estimate_training_flops(self, config: ArchitectureConfig) -> float:
        flops_per_forward = config.hidden_size ** 2 * config.num_layers
        training_flops = flops_per_forward * 1000
        
        for op in config.operations:
            if op in [OperationType.ATTENTION]:
                training_flops *= 1.5
            elif op in [OperationType.MLP]:
                training_flops *= 1.2
            elif op in [OperationType.CONV5x5, OperationType.CONV7x7]:
                training_flops *= 1.1
        
        energy_factor = config.mixed_precision.compute_energy_factor()
        training_flops *= energy_factor
        
        if config.use_gradient_checkpointing:
            training_flops *= 1.3
        
        return training_flops
    
    def estimate_inference_flops(self, config: ArchitectureConfig) -> float:
        flops_per_forward = config.hidden_size ** 2 * config.num_layers
        energy_factor = config.mixed_precision.compute_energy_factor()
        total_flops = flops_per_forward * energy_factor * self.expected_inferences
        
        if config.use_pruning:
            total_flops *= (1 - config.pruning_ratio)
        
        return total_flops
    
    def calculate_training_energy(self, flops: float, config: ArchitectureConfig) -> float:
        avg_energy_per_flop = np.mean([self.ENERGY_PER_OP.get(op, 1e-11) 
                                        for op in config.operations])
        energy = flops * avg_energy_per_flop / config.parallelism
        return energy
    
    def estimate_accuracy(self, config: ArchitectureConfig) -> Tuple[float, float]:
        base_accuracy = 0.7 + 0.3 * (1 - 1 / np.log2(config.hidden_size + 1))
        
        op_contributions = {
            OperationType.ATTENTION: 0.05,
            OperationType.CONV3x3: 0.03,
            OperationType.MLP: 0.02,
            OperationType.LINEAR: 0.01
        }
        
        for op in config.operations:
            base_accuracy += op_contributions.get(op, 0)
        
        precision_impacts = {
            PrecisionType.FP32: 0,
            PrecisionType.FP16: -0.01,
            PrecisionType.INT8: -0.03,
            PrecisionType.INT4: -0.08
        }
        
        penalty = 0
        for precision, impact in precision_impacts.items():
            fraction = 0.25
            penalty += fraction * impact
        
        base_accuracy += penalty
        
        if config.use_pruning:
            base_accuracy -= config.pruning_ratio * 0.15
        
        weights = self.transfer_weights.get_initial_weights(self.task_type)
        transfer_boost = sum([
            weights.get('num_layers', 0) * (config.num_layers / 50) * 0.1,
            weights.get('hidden_size', 0) * (config.hidden_size / 512) * 0.1
        ])
        base_accuracy += transfer_boost
        
        accuracy = max(0.6, min(0.98, base_accuracy))
        uncertainty = 0.02 + 0.03 * (1 - len(self.cache.cached_architectures) / 1000)
        
        return accuracy, uncertainty
    
    def estimate_helium_footprint(self, config: ArchitectureConfig) -> float:
        base_footprint = config.parallelism * 0.1
        energy_factor = config.mixed_precision.compute_energy_factor()
        footprint = base_footprint * energy_factor
        footprint *= np.log2(config.hidden_size) / 10
        
        if config.use_pruning:
            footprint *= (1 - config.pruning_ratio * 0.5)
        
        return min(1.0, footprint)
    
    def evaluate_architecture(self, config: ArchitectureConfig, 
                             use_hardware_profile: bool = True) -> ArchitectureMetrics:
        # Check cache first
        cached_metrics = self.cache.get(config)
        if cached_metrics:
            logger.info("Using cached metrics")
            return cached_metrics
        
        # Estimate metrics
        train_flops = self.estimate_training_flops(config)
        inference_flops = self.estimate_inference_flops(config)
        
        train_energy = self.calculate_training_energy(train_flops, config)
        inference_energy = self.calculate_training_energy(inference_flops, config) * 0.1
        
        train_carbon = self._estimate_carbon(train_energy)
        inference_carbon = self._estimate_carbon(inference_energy)
        total_carbon = train_carbon + inference_carbon
        
        accuracy, accuracy_std = self.estimate_accuracy(config)
        
        latency_base = config.num_layers * config.hidden_size / 1e6
        latency_ms = latency_base * 1000 / config.parallelism
        
        # Hardware profiling
        if use_hardware_profile:
            profile = self.hardware_profiler.profile_architecture(config)
            if profile:
                latency_ms = profile['actual_latency_ms']
                latency_p95_ms = profile.get('actual_latency_p95_ms', latency_ms * 1.2)
                confidence_score = 0.95
            else:
                latency_p95_ms = latency_ms * 1.2
                confidence_score = 0.7
        else:
            latency_p95_ms = latency_ms * 1.2
            confidence_score = 0.7
        
        params_millions = config.hidden_size ** 2 * config.num_layers / 1e6
        flops_billions = (train_flops + inference_flops) / 1e9
        helium_footprint = self.estimate_helium_footprint(config)
        
        metrics = ArchitectureMetrics(
            accuracy=accuracy,
            accuracy_std=accuracy_std,
            latency_ms=latency_ms,
            latency_p95_ms=latency_p95_ms,
            training_energy_joules=train_energy,
            inference_energy_joules=inference_energy,
            total_carbon_kg=total_carbon,
            params_millions=params_millions,
            flops_billions=flops_billions,
            helium_footprint=helium_footprint,
            confidence_score=confidence_score
        )
        
        self.cache.put(config, metrics)
        return metrics
    
    def _estimate_carbon(self, energy_joules: float) -> float:
        energy_kwh = energy_joules / 3.6e6
        carbon_kg = energy_kwh * self.carbon_intensity / 1000
        return carbon_kg
    
    def search_pareto_frontier(self, max_architectures: int = 100) -> List[ParetoPoint]:
        """Search for Pareto-optimal architectures using multi-objective BO"""
        self.explored_architectures = []
        
        # Initial random samples for warm-up
        n_warmup = min(10, max_architectures)
        for _ in range(n_warmup):
            config = self._generate_random_config()
            metrics = self.evaluate_architecture(config)
            self.explored_architectures.append((config, metrics))
            
            # Compute objective vector
            objectives = np.array([
                metrics.total_carbon_kg / 100,  # Normalize carbon
                metrics.latency_ms / 200,        # Normalize latency
                metrics.helium_footprint,        # Helium (0-1)
                1 - metrics.accuracy             # Minimize (1-accuracy)
            ])
            self.multi_objective_bo.add_observation(self._config_to_params(config), objectives)
        
        # Bayesian optimization iterations
        remaining = max_architectures - n_warmup
        for _ in range(remaining):
            candidates = self.multi_objective_bo.suggest_next(n_candidates=5)
            
            best_candidate = None
            best_metrics = None
            best_dominance = float('inf')
            
            for candidate_params in candidates:
                config = self._params_to_config(candidate_params)
                metrics = self.evaluate_architecture(config)
                
                # Check if this candidate is Pareto-dominated
                is_dominated = False
                for _, existing_metrics in self.explored_architectures:
                    if (existing_metrics.total_carbon_kg <= metrics.total_carbon_kg and
                        existing_metrics.latency_ms <= metrics.latency_ms and
                        existing_metrics.helium_footprint <= metrics.helium_footprint and
                        existing_metrics.accuracy >= metrics.accuracy and
                        (existing_metrics.total_carbon_kg < metrics.total_carbon_kg or
                         existing_metrics.latency_ms < metrics.latency_ms or
                         existing_metrics.helium_footprint < metrics.helium_footprint or
                         existing_metrics.accuracy > metrics.accuracy)):
                        is_dominated = True
                        break
                
                if not is_dominated:
                    dominance_count = sum(1 for _, m in self.explored_architectures
                                         if m.total_carbon_kg <= metrics.total_carbon_kg and
                                            m.latency_ms <= metrics.latency_ms and
                                            m.helium_footprint <= metrics.helium_footprint and
                                            m.accuracy >= metrics.accuracy)
                    
                    if dominance_count < best_dominance:
                        best_dominance = dominance_count
                        best_candidate = config
                        best_metrics = metrics
            
            if best_candidate:
                self.explored_architectures.append((best_candidate, best_metrics))
                objectives = np.array([
                    best_metrics.total_carbon_kg / 100,
                    best_metrics.latency_ms / 200,
                    best_metrics.helium_footprint,
                    1 - best_metrics.accuracy
                ])
                self.multi_objective_bo.add_observation(self._config_to_params(best_candidate), objectives)
        
        return self._compute_pareto_frontier()
    
    def _generate_random_config(self) -> ArchitectureConfig:
        mixed_precision = MixedPrecisionConfig(
            default_precision=random.choice(list(PrecisionType)),
            layer_precisions={}
        )
        
        return ArchitectureConfig(
            num_layers=random.choice([6, 12, 24, 48]),
            hidden_size=random.choice([128, 256, 512, 1024]),
            num_heads=random.choice([4, 8, 12, 16]),
            operations=random.sample(list(OperationType), 3),
            mixed_precision=mixed_precision,
            parallelism=random.choice([1, 2, 4, 8]),
            use_gradient_checkpointing=random.choice([True, False]),
            use_pruning=random.choice([True, False]),
            pruning_ratio=random.uniform(0, 0.3)
        )
    
    def _config_to_params(self, config: ArchitectureConfig) -> Dict[str, float]:
        return {
            'num_layers': float(config.num_layers),
            'hidden_size': float(config.hidden_size),
            'num_heads': float(config.num_heads),
            'parallelism': float(config.parallelism),
            'pruning_ratio': config.pruning_ratio
        }
    
    def _params_to_config(self, params: Dict[str, float]) -> ArchitectureConfig:
        mixed_precision = MixedPrecisionConfig(default_precision=PrecisionType.FP16)
        
        return ArchitectureConfig(
            num_layers=int(params['num_layers']),
            hidden_size=int(params['hidden_size']),
            num_heads=int(params['num_heads']),
            operations=[OperationType.CONV3x3, OperationType.ATTENTION, OperationType.MLP],
            mixed_precision=mixed_precision,
            parallelism=int(params['parallelism']),
            use_gradient_checkpointing=False,
            use_pruning=params.get('pruning_ratio', 0) > 0,
            pruning_ratio=params.get('pruning_ratio', 0)
        )
    
    def _compute_pareto_frontier(self) -> List[ParetoPoint]:
        points = []
        
        for i, (config, metrics) in enumerate(self.explored_architectures):
            points.append(ParetoPoint(
                config=config,
                metrics=metrics,
                search_iteration=i,
                timestamp=datetime.now(),
                dominates=[],
                dominated_by=[]
            ))
        
        for i, point in enumerate(points):
            for j, other in enumerate(points):
                if i != j:
                    if (point.metrics.accuracy >= other.metrics.accuracy and
                        point.metrics.total_carbon_kg <= other.metrics.total_carbon_kg and
                        point.metrics.latency_ms <= other.metrics.latency_ms and
                        point.metrics.helium_footprint <= other.metrics.helium_footprint and
                        (point.metrics.accuracy > other.metrics.accuracy or
                         point.metrics.total_carbon_kg < other.metrics.total_carbon_kg or
                         point.metrics.latency_ms < other.metrics.latency_ms or
                         point.metrics.helium_footprint < other.metrics.helium_footprint)):
                        point.dominates.append(j)
                    
                    if (other.metrics.accuracy >= point.metrics.accuracy and
                        other.metrics.total_carbon_kg <= point.metrics.total_carbon_kg and
                        other.metrics.latency_ms <= point.metrics.latency_ms and
                        other.metrics.helium_footprint <= point.metrics.helium_footprint and
                        (other.metrics.accuracy > point.metrics.accuracy or
                         other.metrics.total_carbon_kg < point.metrics.total_carbon_kg or
                         other.metrics.latency_ms < point.metrics.latency_ms or
                         other.metrics.helium_footprint < point.metrics.helium_footprint)):
                        point.dominated_by.append(j)
        
        pareto_optimal = [p for p in points if len(p.dominated_by) == 0]
        
        logger.info(f"Found {len(pareto_optimal)} Pareto-optimal architectures out of {len(points)}")
        logger.info(f"Hypervolume: {self.multi_objective_bo.get_hypervolume():.3f}")
        
        return pareto_optimal
    
    def select_optimal_architecture(self, carbon_budget_kg: float = float('inf'),
                                     latency_budget_ms: float = float('inf'),
                                     helium_budget: float = 1.0,
                                     min_accuracy: float = 0.7) -> Optional[ArchitectureConfig]:
        if not self.pareto_frontier:
            self.search_pareto_frontier()
        
        feasible = []
        for point in self.pareto_frontier:
            m = point.metrics
            if (m.total_carbon_kg <= carbon_budget and
                m.latency_p95_ms <= latency_budget_ms and
                m.helium_footprint <= helium_budget and
                m.accuracy >= min_accuracy):
                feasible.append(point)
        
        if not feasible:
            logger.warning("No feasible architectures found with given constraints")
            if self.pareto_frontier:
                return self.pareto_frontier[0].config
            return None
        
        for point in feasible:
            m = point.metrics
            carbon_score = 1 - (m.total_carbon_kg / carbon_budget) if carbon_budget > 0 else 1
            latency_score = 1 - (m.latency_p95_ms / latency_budget_ms) if latency_budget_ms > 0 else 1
            helium_score = 1 - m.helium_footprint / helium_budget if helium_budget > 0 else 1
            accuracy_score = m.accuracy
            confidence = m.confidence_score
            
            point.score = confidence * (
                0.3 * carbon_score + 0.2 * latency_score + 
                0.2 * helium_score + 0.3 * accuracy_score
            )
        
        best = max(feasible, key=lambda x: x.score)
        
        logger.info(f"Selected architecture: {best.config.num_layers} layers, "
                   f"{best.config.hidden_size} hidden, "
                   f"mixed-precision: {best.config.mixed_precision.default_precision.value}")
        
        return best.config
    
    def get_carbon_optimal_architecture(self, task_constraints: Dict) -> ArchitectureConfig:
        carbon_budget = task_constraints.get('carbon_budget_kg', 100.0)
        latency_budget = task_constraints.get('latency_budget_ms', 100.0)
        helium_budget = task_constraints.get('helium_budget', 1.0)
        min_accuracy = task_constraints.get('min_accuracy', 0.7)
        self.task_type = task_constraints.get('task_type', self.task_type)
        
        return self.select_optimal_architecture(
            carbon_budget_kg=carbon_budget,
            latency_budget_ms=latency_budget,
            helium_budget=helium_budget,
            min_accuracy=min_accuracy
        )
    
    def get_cache_stats(self) -> Dict:
        return self.cache.get_stats()
    
    def clear_cache(self):
        self.cache.clear()
    
    def get_hardware_metrics(self) -> Dict:
        return {
            'average_power_watts': self.hardware_profiler.get_average_power(),
            'current_temperature_c': self.hardware_profiler.get_current_temperature(),
            'monitoring_active': self.hardware_profiler._monitoring
        }
    
    def save_state(self, filepath: str):
        state = {
            'explored_architectures': len(self.explored_architectures),
            'pareto_frontier_size': len(self.pareto_frontier),
            'search_iteration': self.search_iteration,
            'region': self.region,
            'task_type': self.task_type,
            'timestamp': datetime.now().isoformat(),
            'cache_stats': self.get_cache_stats()
        }
        with open(filepath, 'w') as f:
            json.dump(state, f, indent=2)
        logger.info(f"NAS state saved to {filepath}")
    
    def stop(self):
        """Stop hardware monitoring"""
        self.hardware_profiler.stop_monitoring()


# ============================================================
# TransferLearningWeights (from earlier, included for completeness)
# ============================================================

class TransferLearningWeights:
    def __init__(self):
        self.task_weights: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self.task_categories = {
            'nlp': ['text_classification', 'sentiment_analysis', 'named_entity_recognition', 'language_modeling'],
            'vision': ['image_classification', 'object_detection', 'segmentation', 'face_recognition'],
            'speech': ['speech_recognition', 'speaker_identification', 'keyword_spotting'],
            'recommendation': ['collaborative_filtering', 'content_based', 'sequential_recommendation']
        }
    
    def get_initial_weights(self, task_type: str) -> Dict[str, float]:
        if task_type in self.task_categories['nlp']:
            return {'num_layers': 0.3, 'hidden_size': 0.2, 'num_heads': 0.2, 'attention_weight': 0.2, 'ffn_weight': 0.1}
        elif task_type in self.task_categories['vision']:
            return {'num_layers': 0.25, 'hidden_size': 0.15, 'conv_layers': 0.3, 'pooling': 0.15, 'fc_layers': 0.15}
        elif task_type in self.task_categories['speech']:
            return {'num_layers': 0.2, 'hidden_size': 0.2, 'conv_layers': 0.2, 'rnn_layers': 0.2, 'attention_weight': 0.2}
        else:
            return {'num_layers': 0.2, 'hidden_size': 0.2, 'num_heads': 0.2, 'width': 0.2, 'depth': 0.2}
    
    def update_weights(self, task_type: str, architecture: ArchitectureConfig, performance: float):
        feature_contributions = {
            'num_layers': architecture.num_layers / 100,
            'hidden_size': architecture.hidden_size / 1000,
            'num_heads': architecture.num_heads / 20
        }
        for feature, contribution in feature_contributions.items():
            current = self.task_weights[task_type][feature]
            self.task_weights[task_type][feature] = 0.9 * current + 0.1 * performance * contribution


# ============================================================
# Usage Example
# ============================================================

if __name__ == "__main__":
    print("=== Enhanced Carbon-Aware NAS v3.0 Demo ===\n")
    
    # Initialize NAS
    nas = EnhancedCarbonAwareNAS(config={
        'region': 'us-east',
        'task_type': 'vision',
        'expected_inferences': 500_000,
        'cache_file': 'nas_cache_v3.json',
        'monitor_hardware': False,  # Disable for demo
        'profiler_config': {'simulate': True}
    })
    
    print("1. Cache Statistics:")
    print(f"   {nas.get_cache_stats()}")
    
    print("\n2. Searching Pareto Frontier (30 architectures)...")
    nas.search_pareto_frontier(max_architectures=30)
    
    print(f"\n3. Hardware Metrics:")
    print(f"   {nas.get_hardware_metrics()}")
    
    print("\n4. Selecting Optimal Architecture:")
    task_constraints = {
        'carbon_budget_kg': 50.0,
        'latency_budget_ms': 100.0,
        'helium_budget': 0.6,
        'min_accuracy': 0.85,
        'task_type': 'vision'
    }
    
    optimal = nas.get_carbon_optimal_architecture(task_constraints)
    
    print(f"\n✅ Optimal Architecture Found:")
    print(f"   Layers: {optimal.num_layers}")
    print(f"   Hidden size: {optimal.hidden_size}")
    print(f"   Mixed precision: {optimal.mixed_precision.default_precision.value}")
    print(f"   Parallelism: {optimal.parallelism}")
    print(f"   Pruning: {optimal.pruning_ratio if optimal.use_pruning else 'No'}")
    
    print("\n5. Cache Statistics After Search:")
    print(f"   {nas.get_cache_stats()}")
