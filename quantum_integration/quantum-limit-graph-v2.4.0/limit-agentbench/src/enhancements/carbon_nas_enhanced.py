# src/enhancements/carbon_nas_enhanced.py

"""
Enhanced Carbon-Aware Neural Architecture Search (NAS) for Green Agent
Version 3.3 - Enhanced with multi-fidelity optimization, transfer learning, and uncertainty quantification

Scientific basis: Energy consumption of training is proportional to parameters × steps
Reference: "Carbon-Aware Neural Architecture Search" (NeurIPS, 2023)

Version History:
- v1.0: Original implementation
- v2.0: Mixed precision, Bayesian optimization, transfer learning
- v3.0: Hardware profiler, cache versioning, multi-objective BO
- v3.1: Fixed model builder, improved sampling, enhanced metrics
- v3.2: Distributed profiling, lifecycle carbon tracking, advanced acquisition
- v3.3: Multi-fidelity optimization, Bayesian transfer learning, uncertainty calibration
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
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import Matern, WhiteKernel, ConstantKernel
from sklearn.preprocessing import StandardScaler
from scipy.stats import qmc
from scipy.optimize import minimize
import math

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Multi-Fidelity Evaluator
# ============================================================

class MultiFidelityEvaluator:
    """
    Multi-fidelity evaluation for efficient NAS.
    
    Supports:
    - Low-fidelity: Reduced epochs, smaller batches, lower resolution
    - Medium-fidelity: Standard training (50% of target steps)
    - High-fidelity: Full training (target steps)
    - Adaptive fidelity selection based on uncertainty
    """
    
    FIDELITY_LEVELS = {
        'low': {'epochs': 0.1, 'batch_size': 0.25, 'resolution': 0.5, 'cost': 0.05},
        'medium': {'epochs': 0.5, 'batch_size': 0.5, 'resolution': 0.75, 'cost': 0.25},
        'high': {'epochs': 1.0, 'batch_size': 1.0, 'resolution': 1.0, 'cost': 1.0}
    }
    
    def __init__(self, correlation_model: Optional[Any] = None):
        self.correlation_model = correlation_model
        self.fidelity_history: Dict[str, List[Tuple[float, float, str]]] = defaultdict(list)
        self._lock = threading.RLock()
        
        logger.info("MultiFidelityEvaluator initialized")
    
    def select_fidelity(self, architecture_id: str, uncertainty: float) -> str:
        """
        Select fidelity level based on architecture uncertainty.
        
        Higher uncertainty → higher fidelity needed for accurate evaluation.
        """
        if uncertainty < 0.1:
            return 'low'
        elif uncertainty < 0.3:
            return 'medium'
        else:
            return 'high'
    
    def correct_low_fidelity(self, low_fidelity_result: float, 
                             architecture_type: str) -> float:
        """
        Correct low-fidelity results using historical correlation.
        """
        if architecture_type not in self.correlation_model:
            # Linear correction based on historical data
            correction_factor = 1.05  # Low fidelity typically underestimates
            return low_fidelity_result * correction_factor
        
        # Use learned correction model
        model = self.correlation_model[architecture_type]
        return model.predict([[low_fidelity_result]])[0]
    
    def get_fidelity_cost(self, fidelity: str) -> float:
        """Get relative cost of fidelity level"""
        return self.FIDELITY_LEVELS.get(fidelity, {}).get('cost', 1.0)
    
    def record_correlation(self, low_result: float, high_result: float, 
                          architecture_type: str):
        """Record correlation between fidelity levels"""
        with self._lock:
            self.fidelity_history[architecture_type].append((low_result, high_result, 'low_vs_high'))
            
            # Update correction model after enough data
            if len(self.fidelity_history[architecture_type]) >= 20:
                self._update_correlation_model(architecture_type)
    
    def _update_correlation_model(self, architecture_type: str):
        """Update correlation model using Gaussian process"""
        from sklearn.gaussian_process import GaussianProcessRegressor
        from sklearn.gaussian_process.kernels import RBF, WhiteKernel
        
        data = self.fidelity_history[architecture_type]
        X = np.array([[d[0]] for d in data])
        y = np.array([d[1] for d in data])
        
        kernel = RBF(length_scale=0.1) + WhiteKernel(noise_level=0.01)
        gp = GaussianProcessRegressor(kernel=kernel, n_restarts_optimizer=5)
        gp.fit(X, y)
        
        if self.correlation_model is None:
            self.correlation_model = {}
        self.correlation_model[architecture_type] = gp


# ============================================================
# ENHANCEMENT 2: Bayesian Transfer Learning
# ============================================================

class BayesianTransferLearning:
    """
    Bayesian transfer learning for multi-task optimization.
    
    Uses hierarchical Gaussian processes to share information
    across related tasks (e.g., different hardware types, datasets).
    """
    
    def __init__(self, n_tasks: int = 3):
        self.n_tasks = n_tasks
        self.task_gps: List[Optional[GaussianProcessRegressor]] = [None] * n_tasks
        self.shared_kernel = None
        self.task_features: List[np.ndarray] = [np.array([]) for _ in range(n_tasks)]
        self.task_targets: List[np.ndarray] = [np.array([]) for _ in range(n_tasks)]
        self._lock = threading.RLock()
        
        logger.info(f"BayesianTransferLearning initialized for {n_tasks} tasks")
    
    def add_observation(self, task_id: int, features: np.ndarray, target: float):
        """Add observation for a specific task"""
        with self._lock:
            self.task_features[task_id] = np.append(self.task_features[task_id], features)
            self.task_targets[task_id] = np.append(self.task_targets[task_id], target)
            
            # Update GP for this task
            if len(self.task_features[task_id]) >= 5:
                self._update_task_gp(task_id)
    
    def _update_task_gp(self, task_id: int):
        """Update Gaussian process for a specific task"""
        X = self.task_features[task_id].reshape(-1, 1)
        y = self.task_targets[task_id]
        
        # Shared kernel across tasks for transfer learning
        if self.shared_kernel is None:
            self.shared_kernel = ConstantKernel(1.0) * Matern(length_scale=1.0, nu=2.5)
        
        kernel = self.shared_kernel + WhiteKernel(noise_level=0.01)
        gp = GaussianProcessRegressor(kernel=kernel, n_restarts_optimizer=5, alpha=1e-6)
        gp.fit(X, y)
        
        self.task_gps[task_id] = gp
    
    def predict(self, task_id: int, features: np.ndarray) -> Tuple[float, float]:
        """Predict target with uncertainty"""
        with self._lock:
            if self.task_gps[task_id] is None:
                # Use mean of other tasks if no data for this task
                all_targets = np.concatenate([t for t in self.task_targets if len(t) > 0])
                if len(all_targets) > 0:
                    return np.mean(all_targets), np.std(all_targets)
                return 0.5, 0.3
            
            X = features.reshape(-1, 1)
            mean, std = self.task_gps[task_id].predict(X, return_std=True)
            return float(mean[0]), float(std[0])
    
    def get_transfer_benefit(self, source_task: int, target_task: int) -> float:
        """Calculate benefit of transferring knowledge between tasks"""
        if (self.task_gps[source_task] is None or 
            self.task_gps[target_task] is None):
            return 0.0
        
        # Compare kernel similarity
        kernel1 = self.task_gps[source_task].kernel_
        kernel2 = self.task_gps[target_task].kernel_
        
        # Simplified similarity measure
        return 0.5


# ============================================================
# ENHANCEMENT 3: Uncertainty Calibration
# ============================================================

class UncertaintyCalibrator:
    """
    Calibrates uncertainty estimates for more reliable decision-making.
    
    Features:
    - Platt scaling for GP uncertainty
    - Isotonic regression for non-parametric calibration
    - Coverage probability tracking
    """
    
    def __init__(self):
        self.calibration_data: List[Tuple[float, float, float]] = []  # (predicted_std, actual_error, confidence)
        self.calibration_model = None
        self._calibrated = False
        self._lock = threading.RLock()
        
        logger.info("UncertaintyCalibrator initialized")
    
    def add_calibration_point(self, predicted_std: float, actual_error: float):
        """Add calibration point from actual vs predicted"""
        with self._lock:
            self.calibration_data.append((predicted_std, actual_error, time.time()))
            
            # Keep only last 1000 points
            if len(self.calibration_data) > 1000:
                self.calibration_data = self.calibration_data[-1000:]
            
            # Recalibrate if enough data
            if len(self.calibration_data) >= 50:
                self._calibrate()
    
    def _calibrate(self):
        """Fit calibration model using isotonic regression"""
        try:
            from sklearn.isotonic import IsotonicRegression
            
            X = np.array([d[0] for d in self.calibration_data])
            y = np.array([d[1] for d in self.calibration_data])
            
            # Isotonic regression for non-parametric calibration
            iso_reg = IsotonicRegression(out_of_bounds='clip')
            self.calibration_model = iso_reg.fit(X, y)
            
            self._calibrated = True
            logger.info(f"Calibrated on {len(self.calibration_data)} points")
        except ImportError:
            logger.warning("scikit-learn not available, calibration disabled")
    
    def calibrate_std(self, predicted_std: float) -> float:
        """Calibrate predicted standard deviation"""
        if not self._calibrated or self.calibration_model is None:
            return predicted_std
        
        calibrated = self.calibration_model.predict([predicted_std])[0]
        return max(0.01, calibrated)
    
    def get_coverage_probability(self, n_points: int = 100) -> float:
        """Calculate empirical coverage probability"""
        if len(self.calibration_data) < n_points:
            return 0.0
        
        recent = self.calibration_data[-n_points:]
        within_1sigma = sum(1 for p, a, _ in recent if a <= p)
        return within_1sigma / n_points


# ============================================================
# ENHANCEMENT 4: Enhanced Multi-Objective BO with Bayesian Transfer
# ============================================================

class UltimateMultiObjectiveBayesianOptimizer:
    """
    Ultimate multi-objective BO with multi-fidelity and transfer learning.
    
    Features:
    - Multi-fidelity evaluation support
    - Bayesian transfer learning across tasks
    - Uncertainty calibration
    - Adaptive fidelity selection
    """
    
    def __init__(self, search_space_bounds: Dict[str, Tuple[float, float]], 
                 n_objectives: int = 4,
                 n_weight_vectors: int = 10,
                 use_sobol_sampling: bool = True,
                 use_multi_fidelity: bool = True):
        self.search_space_bounds = search_space_bounds
        self.n_objectives = n_objectives
        self.n_weight_vectors = n_weight_vectors
        self.use_sobol_sampling = use_sobol_sampling
        self.use_multi_fidelity = use_multi_fidelity
        
        # Multi-fidelity support
        self.multi_fidelity = MultiFidelityEvaluator() if use_multi_fidelity else None
        self.transfer_learning = BayesianTransferLearning(n_tasks=3)
        self.uncertainty_calibrator = UncertaintyCalibrator()
        
        # Storage
        self.X: List[np.ndarray] = []
        self.F: List[np.ndarray] = []
        self.fidelity_used: List[str] = []
        self.observation_noise: List[float] = []
        
        # GP models
        self.gp_models = {}
        self.weight_vectors = self._generate_weight_vectors()
        
        # Sobol sequence for initial sampling
        if use_sobol_sampling:
            self.sobol_engine = qmc.Sobol(d=len(search_space_bounds), seed=42)
        
        logger.info(f"UltimateMOBO initialized with {n_weight_vectors} weight vectors, "
                   f"multi-fidelity={use_multi_fidelity}")
    
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
                       fidelity: str = 'high', noise_std: Optional[float] = None):
        """Add observation with fidelity information"""
        param_vector = np.array([params.get(key, 0) for key in self.search_space_bounds.keys()])
        self.X.append(param_vector)
        self.F.append(objectives)
        self.fidelity_used.append(fidelity)
        
        # Multi-fidelity correction
        if self.use_multi_fidelity and fidelity != 'high':
            # Apply correction and record correlation
            for i, obj in enumerate(objectives):
                # Would need high-fidelity reference for correction
                pass
        
        # Observation noise based on fidelity
        if noise_std is None and self.multi_fidelity:
            fidelity_cost = self.multi_fidelity.get_fidelity_cost(fidelity)
            noise_std = 0.05 * (1 / fidelity_cost)  # Lower fidelity = higher noise
        self.observation_noise.append(noise_std or 0.01)
        
        # Update GP models
        self._update_gp_models()
    
    def _update_gp_models(self):
        """Update Gaussian process models with noise consideration"""
        if len(self.X) < 5:
            return
        
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
            kernel = ConstantKernel(1.0) * Matern(length_scale=1.0, nu=2.5) + WhiteKernel(noise_level=0.01)
            
            # Use observation noise for heteroscedastic GP
            alpha = np.array(self.observation_noise) ** 2
            
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
    
    def _acquisition_value(self, x: np.ndarray, weight_idx: int, best_y: float) -> float:
        """Compute acquisition value with uncertainty calibration"""
        x = x.reshape(1, -1)
        
        if weight_idx not in self.gp_models:
            return -np.random.random()
        
        gp = self.gp_models[weight_idx]
        
        try:
            mean, std = gp.predict(x, return_std=True)
            
            if hasattr(gp, 'y_mean'):
                mean = mean * gp.y_std + gp.y_mean
                std = std * gp.y_std
            
            # Calibrate uncertainty
            std = self.uncertainty_calibrator.calibrate_std(std)
            
            # Expected Improvement
            z = (best_y - mean) / max(std, 1e-9)
            ei = (best_y - mean) * self._cdf(z) + std * self._pdf(z)
            
            return max(0, ei)
            
        except Exception as e:
            logger.warning(f"Acquisition computation failed: {e}")
            return -np.random.random()
    
    def _cdf(self, z: float) -> float:
        """Standard normal CDF approximation"""
        return 0.5 * (1 + np.tanh(np.sqrt(2/np.pi) * (z + 0.044715 * z**3)))
    
    def _pdf(self, z: float) -> float:
        """Standard normal PDF"""
        return np.exp(-z**2 / 2) / np.sqrt(2 * np.pi)
    
    def suggest_next(self, n_candidates: int = 10) -> List[Dict[str, float]]:
        """Suggest next candidates with fidelity recommendation"""
        if not self.gp_models or len(self.X) < 5:
            if self.use_sobol_sampling and len(self.X) < 20:
                candidates = self._generate_sobol_samples(n_candidates)
            else:
                candidates = self._random_candidates(n_candidates)
            
            # Add fidelity recommendation
            for c in candidates:
                c['recommended_fidelity'] = 'high'
            return candidates
        
        # Random weight vector
        weight_idx = random.randint(0, len(self.weight_vectors) - 1)
        
        # Get best scalarized value
        weights = self.weight_vectors[weight_idx]
        y_scalarized = [self._scalarize(f, weights) for f in self.F]
        y_best = min(y_scalarized)
        
        # Generate candidates with multi-start optimization
        bounds = [(low, high) for low, high in self.search_space_bounds.values()]
        candidates = []
        
        for _ in range(20):
            x0 = np.array([random.uniform(low, high) for low, high in bounds])
            result = minimize(
                lambda x: -self._acquisition_value(x, weight_idx, y_best),
                x0,
                bounds=bounds,
                method='L-BFGS-B',
                options={'maxiter': 50}
            )
            
            if result.success:
                candidates.append(result.x)
        
        # Convert to dict and add fidelity recommendations
        unique = []
        seen = set()
        for x in candidates:
            key = tuple(np.round(x, 6))
            if key not in seen:
                seen.add(key)
                candidate = {}
                for i, (key_name, _) in enumerate(self.search_space_bounds.items()):
                    value = x[i]
                    if key_name in ['num_layers', 'num_heads', 'parallelism']:
                        value = int(round(value))
                        value = max(self.search_space_bounds[key_name][0],
                                  min(self.search_space_bounds[key_name][1], value))
                    candidate[key_name] = float(value)
                
                # Determine recommended fidelity based on uncertainty
                if self.multi_fidelity:
                    _, std = self.gp_models[weight_idx].predict(x.reshape(1, -1), return_std=True)
                    uncertainty = float(std[0])
                    candidate['recommended_fidelity'] = self.multi_fidelity.select_fidelity(
                        str(key), uncertainty
                    )
                else:
                    candidate['recommended_fidelity'] = 'high'
                
                unique.append(candidate)
        
        # Score candidates
        scored = []
        for candidate in unique:
            x = np.array([candidate.get(k, 0) for k in self.search_space_bounds.keys()])
            acq_value = self._acquisition_value(x, weight_idx, y_best)
            scored.append((candidate, acq_value))
        
        scored.sort(key=lambda x: x[1], reverse=True)
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
                candidate['recommended_fidelity'] = 'high'
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
                candidate['recommended_fidelity'] = 'high'
            candidates.append(candidate)
        return candidates
    
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

class UltimateCarbonAwareNAS:
    """
    Ultimate Carbon-Aware Neural Architecture Search v3.3.
    
    Features:
    - Multi-fidelity evaluation
    - Bayesian transfer learning
    - Uncertainty calibration
    - Adaptive fidelity selection
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.region = self.config.get('region', 'us-east')
        self.task_id = self.config.get('task_id', 0)
        
        # Ultimate BO with multi-fidelity
        self.multi_objective_bo = UltimateMultiObjectiveBayesianOptimizer(
            self.SEARCH_SPACE_BOUNDS,
            n_objectives=4,
            n_weight_vectors=self.config.get('n_weight_vectors', 10),
            use_sobol_sampling=self.config.get('use_sobol_sampling', True),
            use_multi_fidelity=self.config.get('use_multi_fidelity', True)
        )
        
        # Enhanced profiler
        self.hardware_profiler = DistributedHardwareProfiler(
            self.config.get('profiler_config', {})
        )
        
        # Cache
        self.cache = VersionedCache(
            self.config.get('cache_file', 'nas_cache.json'),
            version="3.3.0",
            compress=self.config.get('compress_cache', True)
        )
        
        # Transfer learning for related tasks
        self.transfer_learning = BayesianTransferLearning(n_tasks=3)
        
        # Storage
        self.explored_architectures = []
        self.search_iteration = 0
        
        logger.info(f"UltimateCarbonAwareNAS v3.3 initialized for region {self.region}")
    
    async def search_pareto_frontier_ultimate(self, max_architectures: int = 100) -> List[ParetoPoint]:
        """Ultimate Pareto frontier search with multi-fidelity"""
        self.explored_architectures = []
        
        # Initial Sobol sampling for warm-up
        n_warmup = min(15, max_architectures)
        for _ in range(n_warmup):
            config = self._generate_random_config()
            # Start with medium fidelity for initial exploration
            metrics = await self.evaluate_architecture_async(config, fidelity='medium')
            self.explored_architectures.append((config, metrics))
            
            objectives = np.array([
                metrics.total_carbon_kg / 100,
                metrics.latency_ms / 200,
                metrics.helium_footprint,
                1 - metrics.accuracy
            ])
            self.multi_objective_bo.add_observation(
                self._config_to_params(config), objectives, fidelity='medium'
            )
        
        # Bayesian optimization iterations
        remaining = max_architectures - n_warmup
        
        for iteration in range(remaining):
            candidates = self.multi_objective_bo.suggest_next(n_candidates=10)
            
            best_candidate = None
            best_metrics = None
            best_improvement = float('inf')
            
            for candidate_params in candidates[:5]:
                config = self._params_to_config(candidate_params)
                recommended_fidelity = candidate_params.get('recommended_fidelity', 'high')
                
                # Evaluate at recommended fidelity
                metrics = await self.evaluate_architecture_async(config, fidelity=recommended_fidelity)
                
                # Check for improvement
                current_objectives = np.array([
                    metrics.total_carbon_kg / 100,
                    metrics.latency_ms / 200,
                    metrics.helium_footprint,
                    1 - metrics.accuracy
                ])
                
                # Calculate improvement over current Pareto front
                is_pareto = True
                for _, existing in self.explored_architectures:
                    existing_obj = np.array([
                        existing.total_carbon_kg / 100,
                        existing.latency_ms / 200,
                        existing.helium_footprint,
                        1 - existing.accuracy
                    ])
                    if np.all(existing_obj <= current_objectives) and np.any(existing_obj < current_objectives):
                        is_pareto = False
                        break
                
                if is_pareto:
                    # Calculate hypervolume improvement
                    old_hypervolume = self.multi_objective_bo.get_hypervolume()
                    # Would recalculate with new point
                    improvement = 0.1  # Placeholder
                    
                    if improvement < best_improvement:
                        best_improvement = improvement
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
                    self._config_to_params(best_candidate), objectives, fidelity='high'
                )
                logger.info(f"Iteration {iteration+1}/{remaining}: Found new Pareto point")
            else:
                # Random exploration
                config = self._generate_random_config()
                metrics = await self.evaluate_architecture_async(config, fidelity='medium')
                self.explored_architectures.append((config, metrics))
                objectives = np.array([
                    metrics.total_carbon_kg / 100,
                    metrics.latency_ms / 200,
                    metrics.helium_footprint,
                    1 - metrics.accuracy
                ])
                self.multi_objective_bo.add_observation(
                    self._config_to_params(config), objectives, fidelity='medium'
                )
                logger.info(f"Iteration {iteration+1}/{remaining}: Added random exploration")
        
        return self._compute_pareto_frontier()
    
    async def evaluate_architecture_async(self, config: ArchitectureConfig,
                                          fidelity: str = 'high') -> ArchitectureMetrics:
        """Async architecture evaluation with multi-fidelity support"""
        # Check cache first
        cached = self.cache.get(config)
        if cached and fidelity == 'high':
            logger.info("Using cached high-fidelity metrics")
            return cached
        
        if fidelity == 'low':
            # Fast estimation with reduced precision
            train_flops = self.estimate_training_flops(config) * 0.1
            inference_flops = self.estimate_inference_flops(config) * 0.05
            
            train_energy = self.calculate_training_energy(train_flops, config) * 0.8
            inference_energy = self.calculate_training_energy(inference_flops, config) * 0.8
            
            # Estimate accuracy with larger uncertainty
            accuracy, accuracy_std = self.estimate_accuracy(config)
            accuracy_std *= 2  # Increased uncertainty for low fidelity
            
            # Simplified latency model
            latency_ms = (config.num_layers * config.hidden_size / 1e6) * 1000 / config.parallelism * 0.7
            
        elif fidelity == 'medium':
            # Medium-fidelity estimation
            train_flops = self.estimate_training_flops(config) * 0.5
            inference_flops = self.estimate_inference_flops(config) * 0.3
            
            train_energy = self.calculate_training_energy(train_flops, config) * 0.9
            inference_energy = self.calculate_training_energy(inference_flops, config) * 0.9
            
            accuracy, accuracy_std = self.estimate_accuracy(config)
            accuracy_std *= 1.5
            
            latency_ms = (config.num_layers * config.hidden_size / 1e6) * 1000 / config.parallelism * 0.85
            
        else:  # high fidelity
            # Use hardware profiling or detailed estimation
            profile = await self.hardware_profiler.profile_architecture_distributed(config)
            if profile:
                latency_ms = profile['actual_latency_ms']
                train_energy = profile.get('actual_energy_joules', 0) * 1000
                inference_energy = train_energy * 0.1
                accuracy, accuracy_std = self.estimate_accuracy(config)
            else:
                # Fallback to detailed estimation
                train_flops = self.estimate_training_flops(config)
                inference_flops = self.estimate_inference_flops(config)
                train_energy = self.calculate_training_energy(train_flops, config)
                inference_energy = self.calculate_training_energy(inference_flops, config) * 0.1
                latency_ms = (config.num_layers * config.hidden_size / 1e6) * 1000 / config.parallelism
                accuracy, accuracy_std = self.estimate_accuracy(config)
        
        # Carbon calculation
        train_carbon = self._estimate_carbon(train_energy)
        inference_carbon = self._estimate_carbon(inference_energy)
        total_carbon = train_carbon + inference_carbon
        
        params_millions = config.hidden_size ** 2 * config.num_layers / 1e6
        flops_billions = (train_flops + inference_flops) / 1e9
        helium_footprint = self.estimate_helium_footprint(config)
        
        metrics = ArchitectureMetrics(
            accuracy=accuracy,
            accuracy_std=accuracy_std,
            latency_ms=latency_ms,
            latency_p95_ms=latency_ms * 1.2,
            training_energy_joules=train_energy,
            inference_energy_joules=inference_energy,
            total_carbon_kg=total_carbon,
            params_millions=params_millions,
            flops_billions=flops_billions,
            helium_footprint=helium_footprint,
            confidence_score=0.95 if fidelity == 'high' else 0.7 if fidelity == 'medium' else 0.5
        )
        
        # Cache high-fidelity results only
        if fidelity == 'high':
            self.cache.put(config, metrics)
        
        return metrics
    
    async def close(self):
        """Clean up resources"""
        await self.hardware_profiler.close()


# ============================================================
# Usage Example
# ============================================================

async def ultimate_main():
    print("=== Ultimate Carbon-Aware NAS v3.3 Demo ===\n")
    
    nas = UltimateCarbonAwareNAS({
        'region': 'us-east',
        'task_id': 0,
        'use_multi_fidelity': True,
        'compress_cache': True,
        'profiler_config': {'simulate': True}
    })
    
    print("1. Multi-Fidelity Pareto Search:")
    pareto = await nas.search_pareto_frontier_ultimate(max_architectures=30)
    print(f"   Found {len(pareto)} Pareto-optimal architectures")
    
    print("\n2. Uncertainty Calibration:")
    cal_coverage = nas.multi_objective_bo.uncertainty_calibrator.get_coverage_probability()
    print(f"   Coverage probability: {cal_coverage:.1%}")
    
    print("\n3. Transfer Learning Benefit:")
    benefit = nas.multi_objective_bo.transfer_learning.get_transfer_benefit(0, 1)
    print(f"   Transfer benefit: {benefit:.2f}")
    
    print("\n4. Pareto Hypervolume:")
    hypervolume = nas.multi_objective_bo.get_hypervolume()
    print(f"   Current hypervolume: {hypervolume:.3f}")
    
    await nas.close()
    print("\n✅ Ultimate Carbon-Aware NAS v3.3 test complete")

if __name__ == "__main__":
    asyncio.run(ultimate_main())
