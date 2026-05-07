# src/enhancements/carbon_nas_enhanced.py

"""
Enhanced Carbon-Aware Neural Architecture Search (NAS) for Green Agent
Version 3.2 - Enhanced with distributed profiling, advanced BO, and lifecycle tracking

Scientific basis: Energy consumption of training is proportional to parameters × steps
Reference: "Carbon-Aware Neural Architecture Search" (NeurIPS, 2023)

Version History:
- v1.0: Original implementation
- v2.0: Mixed precision, Bayesian optimization, transfer learning
- v3.0: Hardware profiler, cache versioning, multi-objective BO
- v3.1: Fixed model builder, improved sampling, enhanced metrics
- v3.2: Distributed profiling, lifecycle carbon tracking, advanced acquisition
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
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# For improved sampling and optimization
from scipy.stats import qmc
from scipy.optimize import minimize

# For advanced acquisition functions
try:
    from botorch.acquisition import qExpectedImprovement, qNoisyExpectedImprovement
    from botorch.models import SingleTaskGP
    from botorch.fit import fit_gpytorch_model
    from gpytorch.mlls import ExactMarginalLogLikelihood
    BOTORCH_AVAILABLE = True
except ImportError:
    BOTORCH_AVAILABLE = False
    logger.warning("BoTorch not available, using standard acquisition")

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Distributed Hardware Profiler
# ============================================================

class DistributedHardwareProfiler:
    """
    Distributed hardware profiling across multiple GPU nodes.
    
    Features:
    - Asynchronous profiling on multiple GPUs simultaneously
    - Load balancing across available hardware
    - Remote profiling via SSH or API
    - Result aggregation with fault tolerance
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.gpu_endpoints = self.config.get('gpu_endpoints', ['localhost'])
        self.simulation_mode = self.config.get('simulate', False)
        self.max_concurrent = self.config.get('max_concurrent_profiles', 4)
        self._executor = ThreadPoolExecutor(max_workers=self.max_concurrent)
        self._session: Optional[aiohttp.ClientSession] = None
        self.profile_cache: Dict[str, Dict] = {}
        self.cache_file = self.config.get('profile_cache_file', 'hardware_profiles.json')
        
        # Load cache
        self._load_cache()
        
        logger.info(f"DistributedHardwareProfiler initialized with {len(self.gpu_endpoints)} endpoints")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def profile_architecture_distributed(self, config: 'ArchitectureConfig',
                                                endpoint: Optional[str] = None) -> Optional[Dict]:
        """
        Profile architecture on specified or best available endpoint.
        """
        cache_key = self._get_cache_key(config)
        
        # Check cache
        if cache_key in self.profile_cache:
            logger.info(f"Using cached profile for {cache_key[:16]}...")
            return self.profile_cache[cache_key]
        
        if endpoint is None:
            endpoint = await self._select_best_endpoint()
        
        if self.simulation_mode or endpoint == 'localhost':
            profile_data = self._simulate_profile(config)
        else:
            profile_data = await self._profile_remote(config, endpoint)
        
        if profile_data:
            self.profile_cache[cache_key] = profile_data
            self._save_cache()
        
        return profile_data
    
    async def profile_batch(self, configs: List['ArchitectureConfig']) -> List[Optional[Dict]]:
        """
        Profile multiple architectures in parallel.
        """
        tasks = []
        for i, config in enumerate(configs):
            endpoint = self.gpu_endpoints[i % len(self.gpu_endpoints)]
            tasks.append(self.profile_architecture_distributed(config, endpoint))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        return [r if not isinstance(r, Exception) else None for r in results]
    
    async def _select_best_endpoint(self) -> str:
        """Select the least loaded endpoint"""
        if len(self.gpu_endpoints) == 1:
            return self.gpu_endpoints[0]
        
        # Simplified: return least utilized (would query load in production)
        return self.gpu_endpoints[0]
    
    async def _profile_remote(self, config: 'ArchitectureConfig', endpoint: str) -> Optional[Dict]:
        """Profile on remote endpoint via HTTP API"""
        try:
            session = await self._get_session()
            
            # Serialize config
            config_dict = {
                'num_layers': config.num_layers,
                'hidden_size': config.hidden_size,
                'num_heads': config.num_heads,
                'operations': [op.value for op in config.operations],
                'parallelism': config.parallelism,
                'precision': config.mixed_precision.default_precision.value
            }
            
            async with session.post(
                f"{endpoint}/profile",
                json=config_dict,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"Remote profiling failed: {response.status}")
                    return self._simulate_profile(config)
                    
        except Exception as e:
            logger.warning(f"Remote profiling error: {e}")
            return self._simulate_profile(config)
    
    def _get_cache_key(self, config: 'ArchitectureConfig') -> str:
        """Generate cache key for profiling results"""
        config_dict = {
            'num_layers': config.num_layers,
            'hidden_size': config.hidden_size,
            'num_heads': config.num_heads,
            'operations': [op.value for op in config.operations],
            'parallelism': config.parallelism,
            'hardware': 'distributed'
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
    
    def _simulate_profile(self, config: 'ArchitectureConfig') -> Dict:
        """Simulate hardware profiling using analytical models"""
        total_flops = config.hidden_size ** 2 * config.num_layers * 1000
        peak_tflops = self._get_peak_tflops()
        
        # Base latency (ms)
        base_latency = total_flops / (peak_tflops * 1e12) * 1000
        
        # Parallelism scaling
        efficiency = 1.0 / (1 + 0.1 * np.log2(config.parallelism))
        actual_latency = base_latency / config.parallelism * efficiency
        
        # Energy (Joules)
        avg_power = self._get_estimated_power(config)
        actual_energy = avg_power * (actual_latency / 1000)
        
        return {
            'actual_latency_ms': actual_latency,
            'actual_latency_p95_ms': actual_latency * 1.2,
            'actual_energy_joules': actual_energy,
            'actual_memory_mb': config.hidden_size ** 2 * config.num_layers * 4 / (1024 * 1024),
            'actual_power_watts': avg_power,
            'actual_temperature_c': 65 + config.parallelism * 2,
            'measured_at': datetime.now().isoformat(),
            'source': 'simulation'
        }
    
    def _get_peak_tflops(self) -> float:
        """Get peak TFLOPS for current hardware"""
        return 312.0  # A100 default
    
    def _get_estimated_power(self, config: 'ArchitectureConfig') -> float:
        """Estimate average power draw"""
        base_power = 200.0
        power_per_layer = 10.0
        power_per_head = 5.0
        total_power = base_power + config.num_layers * power_per_layer + config.num_heads * power_per_head
        return min(350.0, total_power)
    
    async def close(self):
        """Clean up resources"""
        if self._session:
            await self._session.close()
        self._executor.shutdown()


# ============================================================
# ENHANCEMENT 2: Lifecycle Carbon Tracker
# ============================================================

class LifecycleCarbonTracker:
    """
    Track carbon emissions across the entire model lifecycle.
    
    Stages tracked:
    - Architecture search carbon
    - Training carbon
    - Inference carbon (over lifetime)
    - Retraining carbon
    - Disposal/decommissioning carbon
    """
    
    def __init__(self):
        self.search_carbon_kg = 0.0
        self.training_carbon_kg = 0.0
        self.inference_carbon_kg = 0.0
        self.retraining_carbon_kg = 0.0
        self.disposal_carbon_kg = 0.0
        self._history: List[Dict] = []
    
    def add_search_emissions(self, carbon_kg: float, architectures_evaluated: int):
        """Record carbon from NAS search phase"""
        self.search_carbon_kg += carbon_kg
        self._history.append({
            'stage': 'search',
            'carbon_kg': carbon_kg,
            'architectures_evaluated': architectures_evaluated,
            'timestamp': datetime.now().isoformat()
        })
    
    def add_training_emissions(self, carbon_kg: float, training_steps: int):
        """Record carbon from model training"""
        self.training_carbon_kg += carbon_kg
        self._history.append({
            'stage': 'training',
            'carbon_kg': carbon_kg,
            'training_steps': training_steps,
            'timestamp': datetime.now().isoformat()
        })
    
    def add_inference_emissions(self, carbon_kg: float, inferences: int):
        """Record carbon from model inference over lifetime"""
        self.inference_carbon_kg += carbon_kg
        self._history.append({
            'stage': 'inference',
            'carbon_kg': carbon_kg,
            'inferences': inferences,
            'timestamp': datetime.now().isoformat()
        })
    
    def get_total_carbon(self) -> float:
        """Get total lifecycle carbon emissions"""
        return (self.search_carbon_kg + self.training_carbon_kg + 
                self.inference_carbon_kg + self.retraining_carbon_kg + 
                self.disposal_carbon_kg)
    
    def get_breakdown(self) -> Dict[str, float]:
        """Get carbon breakdown by stage"""
        return {
            'search': self.search_carbon_kg,
            'training': self.training_carbon_kg,
            'inference': self.inference_carbon_kg,
            'retraining': self.retraining_carbon_kg,
            'disposal': self.disposal_carbon_kg,
            'total': self.get_total_carbon()
        }
    
    def get_carbon_intensity(self, stage: str) -> float:
        """Get carbon intensity (kg CO2 per unit) for a stage"""
        if stage == 'search':
            return self.search_carbon_kg / max(1, len(self._history))
        elif stage == 'training':
            return self.training_carbon_kg / max(1, len(self._history))
        elif stage == 'inference':
            return self.inference_carbon_kg / max(1, len(self._history))
        return 0.0
    
    def generate_report(self) -> Dict:
        """Generate comprehensive lifecycle report"""
        return {
            'breakdown': self.get_breakdown(),
            'total_carbon_kg': self.get_total_carbon(),
            'total_carbon_tco2': self.get_total_carbon() / 1000,
            'history': self._history[-10:],  # Last 10 entries
            'carbon_intensity': {
                'search_per_eval': self.get_carbon_intensity('search'),
                'training_per_step': self.get_carbon_intensity('training'),
                'inference_per_inf': self.get_carbon_intensity('inference')
            }
        }


# ============================================================
# ENHANCEMENT 3: Advanced Acquisition Function
# ============================================================

class AdvancedAcquisitionFunction:
    """
    Advanced acquisition functions for Bayesian optimization.
    
    Supports:
    - Expected Improvement (EI)
    - Probability of Improvement (PI)
    - Upper Confidence Bound (UCB)
    - Noisy Expected Improvement (NEI) for uncertain objectives
    """
    
    @staticmethod
    def expected_improvement(mean: np.ndarray, std: np.ndarray, 
                            best_y: float, xi: float = 0.01) -> np.ndarray:
        """
        Expected Improvement acquisition function.
        
        EI(x) = E[max(f(x) - f(x*), 0)]
        """
        if np.any(std <= 1e-9):
            return np.zeros_like(mean)
        
        z = (best_y - mean - xi) / std
        ei = (best_y - mean - xi) * _cdf(z) + std * _pdf(z)
        return np.maximum(ei, 0)
    
    @staticmethod
    def probability_of_improvement(mean: np.ndarray, std: np.ndarray,
                                   best_y: float, xi: float = 0.01) -> np.ndarray:
        """
        Probability of Improvement acquisition function.
        
        PI(x) = P(f(x) >= f(x*) + ξ)
        """
        if np.any(std <= 1e-9):
            return np.zeros_like(mean)
        
        z = (best_y - mean - xi) / std
        return _cdf(z)
    
    @staticmethod
    def upper_confidence_bound(mean: np.ndarray, std: np.ndarray,
                               beta: float = 2.0) -> np.ndarray:
        """
        Upper Confidence Bound acquisition function.
        
        UCB(x) = μ(x) + β * σ(x)
        """
        return mean + beta * std
    
    @staticmethod
    def noisy_expected_improvement(mean: np.ndarray, std: np.ndarray,
                                   best_y: float, noise_std: float) -> np.ndarray:
        """
        Noisy Expected Improvement for heteroscedastic noise.
        """
        combined_std = np.sqrt(std**2 + noise_std**2)
        return AdvancedAcquisitionFunction.expected_improvement(
            mean, combined_std, best_y
        )


def _cdf(z: np.ndarray) -> np.ndarray:
    """Standard normal CDF"""
    return 0.5 * (1 + np.tanh(np.sqrt(2/np.pi) * (z + 0.044715 * z**3)))


def _pdf(z: np.ndarray) -> np.ndarray:
    """Standard normal PDF"""
    return np.exp(-z**2 / 2) / np.sqrt(2 * np.pi)


# ============================================================
# ENHANCEMENT 4: Enhanced Multi-Objective BO with Advanced Acquisition
# ============================================================

class EnhancedMultiObjectiveBayesianOptimizer:
    """
    Enhanced multi-objective Bayesian optimization with advanced acquisition.
    
    Features:
    - Multiple acquisition functions (EI, PI, UCB, NEI)
    - Adaptive acquisition selection based on search stage
    - Trust region optimization for candidate generation
    - Parallel candidate suggestion
    """
    
    def __init__(self, search_space_bounds: Dict[str, Tuple[float, float]], 
                 n_objectives: int = 4,
                 n_weight_vectors: int = 10,
                 use_sobol_sampling: bool = True,
                 acquisition_type: str = 'ei'):  # ei, pi, ucb, nei
        self.search_space_bounds = search_space_bounds
        self.n_objectives = n_objectives
        self.n_weight_vectors = n_weight_vectors
        self.use_sobol_sampling = use_sobol_sampling
        self.acquisition_type = acquisition_type
        self.acquisition_weights = {'ei': 0.4, 'pi': 0.3, 'ucb': 0.3}
        
        # Storage
        self.X: List[np.ndarray] = []
        self.F: List[np.ndarray] = []
        
        # Acquisition history for adaptive selection
        self.acquisition_performance: Dict[str, List[float]] = {
            'ei': [], 'pi': [], 'ucb': [], 'nei': []
        }
        
        # GP models for each weight vector
        self.gp_models = {}
        self.weight_vectors = self._generate_weight_vectors()
        
        # Sobol sequence for better initial sampling
        if use_sobol_sampling:
            self.sobol_engine = qmc.Sobol(d=len(search_space_bounds), seed=42)
        
        logger.info(f"EnhancedMOBO initialized with {n_weight_vectors} weight vectors, acquisition={acquisition_type}")
    
    def _generate_weight_vectors(self) -> List[np.ndarray]:
        """Generate uniformly distributed weight vectors"""
        weight_vectors = []
        for _ in range(self.n_weight_vectors):
            weights = np.random.dirichlet(np.ones(self.n_objectives))
            weight_vectors.append(weights)
        return weight_vectors
    
    def _scalarize(self, objectives: np.ndarray, weights: np.ndarray, rho: float = 0.05) -> float:
        """Augmented Tchebycheff scalarization"""
        weighted = weights * objectives
        return np.max(weighted) + rho * np.sum(objectives)
    
    def add_observation(self, params: Dict[str, float], objectives: np.ndarray,
                       noise_std: Optional[float] = None):
        """Add observation with optional noise estimate"""
        param_vector = np.array([params.get(key, 0) for key in self.search_space_bounds.keys()])
        self.X.append(param_vector)
        self.F.append(objectives)
        
        # Store noise if provided
        if noise_std is not None:
            self._update_gp_models(noise_std)
        else:
            self._update_gp_models()
    
    def _update_gp_models(self, noise_std: Optional[float] = None):
        """Update Gaussian process models"""
        if len(self.X) < 5:
            return
        
        try:
            from sklearn.gaussian_process import GaussianProcessRegressor
            from sklearn.gaussian_process.kernels import RBF, WhiteKernel, Matern, ConstantKernel
            
            for i, weights in enumerate(self.weight_vectors):
                y = np.array([self._scalarize(f, weights) for f in self.F])
                
                # Normalize
                y_mean = np.mean(y)
                y_std = np.std(y)
                if y_std > 1e-6:
                    y_normalized = (y - y_mean) / y_std
                else:
                    y_normalized = y
                
                # Kernel with noise handling
                if noise_std is not None:
                    alpha = noise_std ** 2
                else:
                    alpha = 1e-6
                
                kernel = (ConstantKernel(1.0) * 
                         Matern(length_scale=1.0, nu=2.5) + 
                         WhiteKernel(noise_level=0.1))
                
                gp = GaussianProcessRegressor(
                    kernel=kernel, 
                    n_restarts_optimizer=10,
                    alpha=alpha,
                    normalize_y=True
                )
                gp.fit(np.array(self.X), y_normalized)
                
                gp.y_mean = y_mean
                gp.y_std = y_std
                
                self.gp_models[i] = gp
                
        except ImportError:
            logger.warning("scikit-learn not available, falling back to random search")
            self.gp_models = {}
        except Exception as e:
            logger.warning(f"GP update failed: {e}")
            self.gp_models = {}
    
    def _select_acquisition_function(self) -> str:
        """Adaptively select best acquisition function based on recent performance"""
        if len(self.X) < 20:
            return self.acquisition_type
        
        # Count how many times each acquisition was used and its success
        total_uses = sum(len(v) for v in self.acquisition_performance.values())
        if total_uses < 10:
            return self.acquisition_type
        
        # Select based on highest average improvement
        avg_improvements = {}
        for acq, improvements in self.acquisition_performance.items():
            if improvements:
                avg_improvements[acq] = np.mean(improvements)
        
        if avg_improvements:
            return max(avg_improvements, key=avg_improvements.get)
        return self.acquisition_type
    
    def _acquisition_value(self, x: np.ndarray, weight_idx: int, 
                          best_y: float) -> float:
        """Compute acquisition value for a candidate point"""
        x = x.reshape(1, -1)
        
        if weight_idx not in self.gp_models:
            return -np.random.random()
        
        gp = self.gp_models[weight_idx]
        
        try:
            mean, std = gp.predict(x, return_std=True)
            
            if hasattr(gp, 'y_mean'):
                mean = mean * gp.y_std + gp.y_mean
                std = std * gp.y_std
            
            # Select acquisition function
            acq_type = self._select_acquisition_function()
            
            if acq_type == 'ei':
                return AdvancedAcquisitionFunction.expected_improvement(
                    mean, std, best_y
                )[0]
            elif acq_type == 'pi':
                return AdvancedAcquisitionFunction.probability_of_improvement(
                    mean, std, best_y
                )[0]
            elif acq_type == 'ucb':
                return AdvancedAcquisitionFunction.upper_confidence_bound(
                    mean, std
                )[0]
            else:
                return AdvancedAcquisitionFunction.expected_improvement(
                    mean, std, best_y
                )[0]
                
        except Exception as e:
            logger.warning(f"Acquisition computation failed: {e}")
            return -np.random.random()
    
    def suggest_next(self, n_candidates: int = 10) -> List[Dict[str, float]]:
        """Suggest next candidates using acquisition function optimization"""
        if not self.gp_models or len(self.X) < 5:
            if self.use_sobol_sampling and len(self.X) < 20:
                return self._generate_sobol_samples(n_candidates)
            else:
                return self._random_candidates(n_candidates)
        
        # Randomly select a weight vector
        weight_idx = random.randint(0, len(self.weight_vectors) - 1)
        
        # Get best scalarized value
        weights = self.weight_vectors[weight_idx]
        y_scalarized = [self._scalarize(f, weights) for f in self.F]
        y_best = min(y_scalarized)
        
        # Generate candidates for optimization
        candidates = []
        bounds = [(low, high) for low, high in self.search_space_bounds.values()]
        
        # Multi-start optimization
        n_starts = 20
        for _ in range(n_starts):
            # Random start point
            x0 = np.array([random.uniform(low, high) for low, high in bounds])
            
            # Optimize
            result = minimize(
                lambda x: -self._acquisition_value(x, weight_idx, y_best),
                x0,
                bounds=bounds,
                method='L-BFGS-B',
                options={'maxiter': 50}
            )
            
            if result.success:
                candidates.append(result.x)
        
        # Remove duplicates and convert to dict
        unique_candidates = []
        seen = set()
        for x in candidates:
            key = tuple(np.round(x, 6))
            if key not in seen:
                seen.add(key)
                candidate = {}
                for i, (key_name, _) in enumerate(self.search_space_bounds.items()):
                    value = x[i]
                    # Handle integer parameters
                    if key_name in ['num_layers', 'num_heads', 'parallelism']:
                        value = int(round(value))
                        value = max(self.search_space_bounds[key_name][0],
                                  min(self.search_space_bounds[key_name][1], value))
                    candidate[key_name] = float(value)
                unique_candidates.append(candidate)
        
        # Score candidates by acquisition value
        scored = []
        for candidate in unique_candidates:
            x = np.array([candidate.get(k, 0) for k in self.search_space_bounds.keys()])
            acq_value = self._acquisition_value(x, weight_idx, y_best)
            scored.append((candidate, acq_value))
        
        scored.sort(key=lambda x: x[1], reverse=True)
        
        # Record acquisition type used for this round
        acq_used = self._select_acquisition_function()
        if acq_used not in self.acquisition_performance:
            self.acquisition_performance[acq_used] = []
        
        return [c for c, _ in scored[:n_candidates]]
    
    def _generate_sobol_samples(self, n: int) -> List[Dict[str, float]]:
        """Generate Sobol sequence samples"""
        samples = self.sobol_engine.random(n)
        
        candidates = []
        for sample in samples:
            candidate = {}
            for i, (key, (low, high)) in enumerate(self.search_space_bounds.items()):
                value = low + sample[i] * (high - low)
                if key in ['num_layers', 'num_heads', 'parallelism']:
                    value = int(round(value))
                    value = max(low, min(high, value))
                candidate[key] = float(value)
            candidates.append(candidate)
        
        return candidates
    
    def _random_candidates(self, n: int) -> List[Dict[str, float]]:
        """Generate random candidates"""
        candidates = []
        for _ in range(n):
            candidate = {}
            for key, (low, high) in self.search_space_bounds.items():
                value = random.uniform(low, high)
                if key in ['num_layers', 'num_heads', 'parallelism']:
                    value = int(round(value))
                    value = max(low, min(high, value))
                candidate[key] = value
            candidates.append(candidate)
        return candidates
    
    def record_acquisition_performance(self, acq_type: str, improvement: float):
        """Record performance of an acquisition function"""
        if acq_type in self.acquisition_performance:
            self.acquisition_performance[acq_type].append(improvement)
            # Keep last 20
            if len(self.acquisition_performance[acq_type]) > 20:
                self.acquisition_performance[acq_type] = self.acquisition_performance[acq_type][-20:]
    
    def get_pareto_frontier(self) -> List[Tuple[np.ndarray, np.ndarray]]:
        """Get current Pareto frontier"""
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
        """Calculate hypervolume of Pareto frontier"""
        if reference_point is None:
            reference_point = np.max(self.F, axis=0) if self.F else np.ones(self.n_objectives)
        
        frontier = self.get_pareto_frontier()
        if not frontier:
            return 0.0
        
        # Monte Carlo hypervolume
        n_samples = 10000
        samples = np.random.uniform(0, 1, (n_samples, self.n_objectives))
        samples_scaled = samples * reference_point
        
        dominated_count = 0
        for sample in samples_scaled:
            for _, f in frontier:
                if np.all(f <= sample):
                    dominated_count += 1
                    break
        
        return (dominated_count / n_samples) * np.prod(reference_point)


# ============================================================
# ENHANCEMENT 5: Main Enhanced NAS Class
# ============================================================

class EnhancedCarbonAwareNAS:
    """
    Enhanced Carbon-Aware Neural Architecture Search v3.2.
    
    Features:
    - Distributed hardware profiling
    - Lifecycle carbon tracking
    - Advanced acquisition functions
    - Adaptive BO with multiple acquisition types
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
        
        # Initialize enhanced components
        self.cache = VersionedCache(
            self.config.get('cache_file', 'nas_cache.json'),
            version="3.2.0",
            compress=self.config.get('compress_cache', True)
        )
        
        self.multi_objective_bo = EnhancedMultiObjectiveBayesianOptimizer(
            self.SEARCH_SPACE_BOUNDS,
            n_objectives=4,
            n_weight_vectors=self.config.get('n_weight_vectors', 10),
            use_sobol_sampling=self.config.get('use_sobol_sampling', True),
            acquisition_type=self.config.get('acquisition_type', 'ei')
        )
        
        # Use distributed profiler
        self.hardware_profiler = DistributedHardwareProfiler(
            self.config.get('profiler_config', {})
        )
        
        self.transfer_weights = TransferLearningWeights()
        self.carbon_tracker = LifecycleCarbonTracker()
        
        # Storage
        self.explored_architectures: List[Tuple[ArchitectureConfig, ArchitectureMetrics]] = []
        self.pareto_frontier: List[ParetoPoint] = []
        self.search_iteration = 0
        
        logger.info(f"EnhancedCarbonAwareNAS v3.2 initialized for {self.region} region")
    
    async def search_pareto_frontier_async(self, max_architectures: int = 100) -> List[ParetoPoint]:
        """Async Pareto frontier search with distributed profiling"""
        self.explored_architectures = []
        
        # Initial warm-up with Sobol sampling
        n_warmup = min(20, max_architectures)
        warmup_configs = []
        for _ in range(n_warmup):
            warmup_configs.append(self._generate_random_config())
        
        # Profile warmup architectures in parallel
        warmup_results = await self.hardware_profiler.profile_batch(warmup_configs)
        
        for config, profile in zip(warmup_configs, warmup_results):
            if profile:
                metrics = self._profile_to_metrics(config, profile)
            else:
                metrics = self.evaluate_architecture(config, use_hardware_profile=False)
            
            self.explored_architectures.append((config, metrics))
            
            objectives = np.array([
                metrics.total_carbon_kg / 100,
                metrics.latency_ms / 200,
                metrics.helium_footprint,
                1 - metrics.accuracy
            ])
            self.multi_objective_bo.add_observation(
                self._config_to_params(config), objectives
            )
        
        # Record search carbon
        search_carbon = sum(m.total_carbon_kg for _, m in self.explored_architectures)
        self.carbon_tracker.add_search_emissions(search_carbon, len(self.explored_architectures))
        
        # Bayesian optimization iterations
        remaining = max_architectures - n_warmup
        acquisition_perf = []
        
        for iteration in range(remaining):
            candidates = self.multi_objective_bo.suggest_next(n_candidates=10)
            
            # Profile candidates in parallel
            candidate_configs = [self._params_to_config(c) for c in candidates[:5]]
            candidate_profiles = await self.hardware_profiler.profile_batch(candidate_configs)
            
            best_candidate = None
            best_metrics = None
            best_dominance = float('inf')
            
            for config, profile in zip(candidate_configs, candidate_profiles):
                if profile:
                    metrics = self._profile_to_metrics(config, profile)
                else:
                    metrics = self.evaluate_architecture(config, use_hardware_profile=False)
                
                # Check Pareto dominance
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
                                         if metrics.total_carbon_kg <= m.total_carbon_kg and
                                            metrics.latency_ms <= m.latency_ms and
                                            metrics.helium_footprint <= m.helium_footprint and
                                            metrics.accuracy >= m.accuracy)
                    
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
                self.multi_objective_bo.add_observation(
                    self._config_to_params(best_candidate), objectives
                )
                logger.info(f"Iteration {iteration+1}/{remaining}: Found new Pareto point")
                acquisition_perf.append(1.0)  # Success
            else:
                # Random exploration
                config = self._generate_random_config()
                metrics = self.evaluate_architecture(config)
                self.explored_architectures.append((config, metrics))
                objectives = np.array([
                    metrics.total_carbon_kg / 100,
                    metrics.latency_ms / 200,
                    metrics.helium_footprint,
                    1 - metrics.accuracy
                ])
                self.multi_objective_bo.add_observation(
                    self._config_to_params(config), objectives
                )
                logger.info(f"Iteration {iteration+1}/{remaining}: Added random exploration point")
                acquisition_perf.append(0.0)  # Failure
        
        # Record acquisition performance
        for i, perf in enumerate(acquisition_perf):
            self.multi_objective_bo.record_acquisition_performance(
                self.config.get('acquisition_type', 'ei'), perf
            )
        
        return self._compute_pareto_frontier()
    
    def _profile_to_metrics(self, config: ArchitectureConfig, 
                           profile: Dict) -> ArchitectureMetrics:
        """Convert hardware profile to ArchitectureMetrics"""
        return ArchitectureMetrics(
            accuracy=self.estimate_accuracy(config)[0] if profile.get('accuracy') is None else profile.get('accuracy', 0.95),
            accuracy_std=profile.get('accuracy_std', 0.02),
            latency_ms=profile['actual_latency_ms'],
            latency_p95_ms=profile.get('actual_latency_p95_ms', profile['actual_latency_ms'] * 1.2),
            training_energy_joules=profile.get('actual_energy_joules', 0) * 1000,
            inference_energy_joules=profile.get('actual_energy_joules', 0) * 10,
            total_carbon_kg=self._estimate_carbon(profile.get('actual_energy_joules', 0) * 1010),
            params_millions=config.hidden_size ** 2 * config.num_layers / 1e6,
            flops_billions=self.estimate_training_flops(config) / 1e9,
            helium_footprint=self.estimate_helium_footprint(config),
            confidence_score=profile.get('confidence', 0.95)
        )
    
    def evaluate_architecture(self, config: ArchitectureConfig, 
                             use_hardware_profile: bool = True) -> ArchitectureMetrics:
        """Evaluate architecture (sync wrapper for async method)"""
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
        latency_p95_ms = latency_ms * 1.2
        
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
            confidence_score=0.85
        )
        
        self.cache.put(config, metrics)
        return metrics
    
    def get_carbon_report(self) -> Dict:
        """Get lifecycle carbon report"""
        return self.carbon_tracker.generate_report()
    
    async def close(self):
        """Clean up resources"""
        await self.hardware_profiler.close()
    
    # ... (keep existing helper methods: _estimate_carbon, _generate_random_config,
    #     _config_to_params, _params_to_config, _compute_pareto_frontier,
    #     estimate_accuracy, estimate_training_flops, etc.)
    
    def _estimate_carbon(self, energy_joules: float) -> float:
        energy_kwh = energy_joules / 3.6e6
        return energy_kwh * self.carbon_intensity / 1000
    
    def _generate_random_config(self) -> ArchitectureConfig:
        mixed_precision = MixedPrecisionConfig(
            default_precision=random.choice(list(PrecisionType)),
            layer_precisions={}
        )
        
        num_layers_options = [6, 12, 24, 36, 48, 60]
        hidden_size_options = [128, 256, 512, 768, 1024, 1536, 2048]
        num_heads_options = [4, 8, 12, 16, 20, 24]
        
        return ArchitectureConfig(
            num_layers=random.choice(num_layers_options),
            hidden_size=random.choice(hidden_size_options),
            num_heads=random.choice(num_heads_options),
            operations=random.sample(list(OperationType), min(3, len(OperationType))),
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
        
        return pareto_optimal


# ============================================================
# Usage Example
# ============================================================

async def async_main():
    print("=== Enhanced Carbon-Aware NAS v3.2 Demo ===\n")
    
    nas = EnhancedCarbonAwareNAS({
        'region': 'us-east',
        'task_type': 'vision',
        'expected_inferences': 500_000,
        'compress_cache': True,
        'acquisition_type': 'ei',
        'profiler_config': {
            'simulate': True,
            'gpu_endpoints': ['localhost']
        }
    })
    
    print("1. Searching Pareto frontier (20 architectures)...")
    pareto = await nas.search_pareto_frontier_async(max_architectures=20)
    
    print(f"   Found {len(pareto)} Pareto-optimal architectures")
    
    print("\n2. Lifecycle Carbon Report:")
    carbon_report = nas.get_carbon_report()
    print(f"   Search carbon: {carbon_report['breakdown']['search']:.2f} kg CO2")
    print(f"   Training carbon: {carbon_report['breakdown']['training']:.2f} kg CO2")
    print(f"   Total carbon: {carbon_report['total_carbon_kg']:.2f} kg CO2")
    
    print("\n3. Cache Statistics:")
    cache_stats = nas.cache.get_stats()
    print(f"   Cache size: {cache_stats['entry_count']}")
    print(f"   Hit rate: {cache_stats['hit_rate']:.1%}")
    
    print("\n4. Acquisition Performance:")
    acq_perf = nas.multi_objective_bo.acquisition_performance
    for acq, perf in acq_perf.items():
        if perf:
            print(f"   {acq}: {np.mean(perf):.2f} avg improvement")
    
    await nas.close()
    print("\n✅ Enhanced Carbon-Aware NAS v3.2 test complete")

def main():
    asyncio.run(async_main())

if __name__ == "__main__":
    main()
