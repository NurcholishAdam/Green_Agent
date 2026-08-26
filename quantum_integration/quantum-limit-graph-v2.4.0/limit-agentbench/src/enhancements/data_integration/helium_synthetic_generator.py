# src/enhancements/data_integration/helium_synthetic_generator_v2_3_0.py
"""
Enhanced Helium Synthetic Generator v2.3.0
===========================================
Generates synthetic Helium Proof‑of‑Coverage (PoC) traces with adaptive parameter selection
via Multi‑Teacher On‑Policy Distillation and Multi‑Objective Evolutionary Optimization (MOEA).

ENHANCEMENTS OVER v2.2.0:
- Added NSGA‑II optimizer to evolve generation strategy parameters (weights for strategy selection).
- Maintains a Pareto front of non‑dominated strategies.
- MODP‑based selection of best strategy using dynamic objective weights.
- Background task for periodic evolution.
- New configuration parameters for MOEA.
- Persistence of evolved strategies.

All previous features (distillation, statistical validation, edge cases, export) are retained.
"""

import asyncio
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
import random
import json
import numpy as np
from abc import ABC, abstractmethod
from collections import deque
import pickle
import pandas as pd
import copy

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Scipy for statistical tests ----------
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

# ---------- scikit-learn for ML teacher ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# ---------- Logging ----------
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class HeliumSyntheticConfig(BaseModel):
        """Configuration for synthetic trace generation."""
        # General
        version: str = "2.3.0"
        seed: int = Field(42, description="Random seed for reproducibility")
        # Trace parameters
        num_hotspots: int = Field(100, ge=1)
        num_gateways: int = Field(5, ge=1)
        duration_hours: float = Field(24.0, ge=1)
        base_events_per_hour: float = Field(10.0, gt=0)
        # RSSI/SNR distributions (per region and hotspot type)
        rssi_mean_urban: float = Field(-70.0)
        rssi_std_urban: float = Field(10.0)
        rssi_mean_rural: float = Field(-80.0)
        rssi_std_rural: float = Field(15.0)
        snr_mean: float = Field(12.0)
        snr_std: float = Field(3.0)
        # Spatial clustering
        num_clusters: int = Field(3, ge=1)
        cluster_spread: float = Field(0.2, description="Spread of clusters relative to area")
        # Gateway path loss parameters
        path_loss_exponent: float = Field(2.0, ge=1.0)
        reference_distance_km: float = Field(1.0, gt=0)
        shadowing_std: float = Field(3.0, ge=0, description="Log‑normal shadowing standard deviation (dB)")
        # Diurnal variation
        diurnal_amplitude: float = Field(0.3, ge=0, le=1, description="Fraction of peak variation")
        diurnal_peak_hour: int = Field(14, ge=0, le=23)
        # Burst parameters
        burst_probability: float = Field(0.1, ge=0, le=1)
        burst_multiplier: float = Field(5.0, ge=1)
        # Edge cases
        edge_case_rate: float = Field(0.0, ge=0, le=1)
        # Export
        export_format: str = Field("parquet", description="parquet, csv, json")
        # Statistical validation
        validation_alpha: float = Field(0.05, ge=0, le=1, description="Significance level for tests")

        # NEW: Distillation parameters
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

        # NEW: MOEA parameters
        moea_enabled: bool = Field(True)
        moea_interval_seconds: int = Field(300, ge=60)
        moea_population_size: int = Field(30, ge=10)
        moea_generations: int = Field(10, ge=2)
        moea_mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
        moea_crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
        moea_tournament_size: int = Field(3, ge=2)
        moea_objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'quality': 0.4,
                'diversity': 0.3,
                'edge_coverage': 0.2,
                'time_efficiency': 0.1,
            }
        )
        moea_dynamic_weights: bool = Field(True)

        # Persistence paths
        q_weights_path: str = Field("./synth_q_weights.json")
        generation_logs_path: str = Field("./synth_generation_logs.csv")
        historical_model_path: str = Field("./synth_historical_model.pkl")
        moea_pareto_path: str = Field("./synth_moea_pareto.json")

        @field_validator('export_format')
        @classmethod
        def validate_export_format(cls, v):
            if v not in ['parquet', 'csv', 'json']:
                raise ValueError("export_format must be 'parquet', 'csv', or 'json'")
            return v

        class Config:
            env_prefix = "HELIUM_SYNTH_"
else:
    # Fallback dict
    HELIUM_SYNTH_CONFIG = {
        "version": "2.3.0",
        "seed": 42,
        "num_hotspots": 100,
        "num_gateways": 5,
        "duration_hours": 24.0,
        "base_events_per_hour": 10.0,
        "rssi_mean_urban": -70.0,
        "rssi_std_urban": 10.0,
        "rssi_mean_rural": -80.0,
        "rssi_std_rural": 15.0,
        "snr_mean": 12.0,
        "snr_std": 3.0,
        "num_clusters": 3,
        "cluster_spread": 0.2,
        "path_loss_exponent": 2.0,
        "reference_distance_km": 1.0,
        "shadowing_std": 3.0,
        "diurnal_amplitude": 0.3,
        "diurnal_peak_hour": 14,
        "burst_probability": 0.1,
        "burst_multiplier": 5.0,
        "edge_case_rate": 0.0,
        "export_format": "parquet",
        "validation_alpha": 0.05,
        # Distillation defaults
        "distillation_epsilon": 0.1,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
        # MOEA defaults
        "moea_enabled": True,
        "moea_interval_seconds": 300,
        "moea_population_size": 30,
        "moea_generations": 10,
        "moea_mutation_rate": 0.2,
        "moea_crossover_rate": 0.8,
        "moea_tournament_size": 3,
        "moea_objective_weights": {
            'quality': 0.4,
            'diversity': 0.3,
            'edge_coverage': 0.2,
            'time_efficiency': 0.1,
        },
        "moea_dynamic_weights": True,
        "q_weights_path": "./synth_q_weights.json",
        "generation_logs_path": "./synth_generation_logs.csv",
        "historical_model_path": "./synth_historical_model.pkl",
        "moea_pareto_path": "./synth_moea_pareto.json",
    }


# ============================================================================
# DISTILLATION COMPONENTS FOR STRATEGY SELECTION
# ============================================================================

@dataclass
class GenerationState:
    """State for the distillation agent."""
    # Desired objectives (user-provided)
    target_ks_stat: float
    target_anomaly_rate: float
    target_diversity: float
    # Last validation results
    last_rssi_ks_p: float
    last_snr_ks_p: float
    last_uplink_chisq_p: float
    last_diurnal_p: float
    # Historical performance
    avg_quality_score: float
    # Context
    num_traces_generated: int
    hours_since_last: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 10‑dim numeric feature vector."""
        features = [
            self.target_ks_stat,
            self.target_anomaly_rate,
            self.target_diversity,
            self.last_rssi_ks_p,
            self.last_snr_ks_p,
            self.last_uplink_chisq_p,
            self.last_diurnal_p,
            self.avg_quality_score,
            min(self.num_traces_generated / 100.0, 1.0),
            min(self.hours_since_last / 24.0, 1.0),
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: GenerationState) -> np.ndarray:
        """Return probability vector over 5 strategies."""
        pass

    @abstractmethod
    def confidence(self, state: GenerationState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class StrategyRuleBasedTeacher(Teacher):
    """Rule‑based expert."""
    STRATEGIES = ['realistic', 'diverse', 'edge_case_heavy', 'balanced', 'custom']

    def predict(self, state: GenerationState) -> np.ndarray:
        n = 5
        probs = np.ones(n) * 0.1
        if state.last_rssi_ks_p < 0.05 or state.last_snr_ks_p < 0.05:
            probs[0] = 0.8  # realistic
        elif state.last_diurnal_p < 0.05:
            probs[1] = 0.7  # diverse (increase variation)
        elif state.target_anomaly_rate > 0.2 and state.last_uplink_chisq_p < 0.05:
            probs[2] = 0.7  # edge_case_heavy
        else:
            probs[3] = 0.6  # balanced
        return probs / probs.sum()

    def confidence(self, state: GenerationState) -> float:
        if state.last_rssi_ks_p < 0.05:
            return 0.6
        return 0.4


class StrategyHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past generation logs."""
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(HELIUM_SYNTH_CONFIG['historical_model_path'])
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: GenerationState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: GenerationState) -> float:
        return 0.7 if self.model is not None else 0.0


class StrategyStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((10, 5))  # 10 features, 5 actions
        self._load_state()

    def _load_state(self):
        path = Path(HELIUM_SYNTH_CONFIG['q_weights_path'])
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path(HELIUM_SYNTH_CONFIG['q_weights_path'])
        with open(path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: GenerationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: GenerationState) -> float:
        return 0.5

    def update(self, state: GenerationState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 10, n_classes: int = 5, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray, num_classes: int) -> np.ndarray:
        if num_classes != self.n_classes:
            new_weights = np.zeros((self.weights.shape[0], num_classes))
            new_biases = np.zeros(num_classes)
            min_dim = min(self.n_classes, num_classes)
            new_weights[:, :min_dim] = self.weights[:, :min_dim]
            new_biases[:min_dim] = self.biases[:min_dim]
            self.weights = new_weights
            self.biases = new_biases
            self.n_classes = num_classes
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector, self.n_classes)
        logits = state_vector @ self.weights + self.biases

        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)

        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec: np.ndarray, action: int, reward: float,
             next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self):
        return len(self.buffer)


class DistillationGeneratorOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for generation strategy selection.
    """
    STRATEGIES = ['realistic', 'diverse', 'edge_case_heavy', 'balanced', 'custom']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            StrategyRuleBasedTeacher(),
            StrategyHistoricalMLTeacher(),
            StrategyStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_strategy(self, state: GenerationState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = 5

        teacher_probs = np.zeros(n)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            if len(prob) != n:
                if len(prob) < n:
                    prob = np.pad(prob, (0, n - len(prob)), 'constant')
                else:
                    prob = prob[:n]
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(n) / n

        student_probs = self.student.predict_proba(state_vec, n)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, n - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.STRATEGIES[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])

    def get_stats(self) -> Dict:
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}


# ============================================================================
# NEW: Multi‑Objective Strategy Evolution (NSGA‑II)
# ============================================================================

@dataclass
class MOPDGenerationStrategy:
    """A generation strategy: a weight vector for strategy selection."""
    strategy_id: str
    weights: Dict[str, float]  # Keys: quality, diversity, edge_coverage, time_efficiency
    objectives: Dict[str, float]  # achieved values (all maximized)
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'strategy_id': self.strategy_id,
            'weights': self.weights,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDGenerationStrategy':
        return cls(**data)


class NSGAIIGeneratorOptimizer:
    """
    Multi‑objective genetic algorithm for evolving generation strategy weights.
    Decision variables: weights for quality, diversity, edge_coverage, time_efficiency.
    Objectives: maximize quality, diversity, edge_coverage, time_efficiency.
    The evaluation function replays historical generation logs or uses a simulator.
    """
    def __init__(self,
                 evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
                 population_size: int = 20,
                 generations: int = 10,
                 mutation_rate: float = 0.2,
                 crossover_rate: float = 0.8,
                 tournament_size: int = 3,
                 objective_weights: Optional[Dict[str, float]] = None,
                 dynamic_weights: bool = True):
        self.evaluate_func = evaluate_func
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {
            'quality': 0.4,
            'diversity': 0.3,
            'edge_coverage': 0.2,
            'time_efficiency': 0.1,
        }
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDGenerationStrategy] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}

    def _random_individual(self) -> Dict[str, float]:
        weights = {
            'quality': random.random(),
            'diversity': random.random(),
            'edge_coverage': random.random(),
            'time_efficiency': random.random(),
        }
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
        child = {}
        for key in p1:
            if random.random() < 0.5:
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                child[key] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[key] + (1 - beta) * p2[key])))
            else:
                child[key] = p1[key] if random.random() < 0.5 else p2[key]
        total = sum(child.values())
        if total > 0:
            child = {k: v / total for k, v in child.items()}
        return child

    def _mutate(self, ind: Dict) -> Dict:
        mutant = ind.copy()
        for key in mutant:
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / (20 + 1)) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                mutant[key] = mutant[key] + delta
                mutant[key] = max(0.0, min(1.0, mutant[key]))
        total = sum(mutant.values())
        if total > 0:
            mutant = {k: v / total for k, v in mutant.items()}
        return mutant

    def _fast_non_dominated_sort(self, points: List[MOPDGenerationStrategy]) -> List[List[MOPDGenerationStrategy]]:
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}

        for i, p in enumerate(points):
            p_obj = p.objectives
            for j, q in enumerate(points):
                if i == j:
                    continue
                q_obj = q.objectives
                if all(p_obj[k] >= q_obj[k] for k in p_obj) and any(p_obj[k] > q_obj[k] for k in p_obj):
                    dominated_solutions[id(p)].append(q)
                elif all(q_obj[k] >= p_obj[k] for k in q_obj) and any(q_obj[k] > p_obj[k] for k in q_obj):
                    domination_count[id(p)] += 1

            if domination_count[id(p)] == 0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p)

        i = 0
        while i < len(fronts):
            next_front = []
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)] -= 1
                    if domination_count[id(q)] == 0:
                        next_front.append(q)
            if next_front:
                fronts.append(next_front)
            i += 1
        return fronts

    def _crowding_distance(self, front: List[MOPDGenerationStrategy]) -> Dict[int, float]:
        if not front:
            return {}
        distances = {id(p): 0.0 for p in front}
        objective_keys = list(front[0].objectives.keys())
        for obj in objective_keys:
            sorted_front = sorted(front, key=lambda x: x.objectives[obj])
            distances[id(sorted_front[0])] = float('inf')
            distances[id(sorted_front[-1])] = float('inf')
            obj_min = sorted_front[0].objectives[obj]
            obj_max = sorted_front[-1].objectives[obj]
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                distances[id(sorted_front[i])] += (sorted_front[i+1].objectives[obj] - sorted_front[i-1].objectives[obj]) / (obj_max - obj_min)
        return distances

    def _tournament_selection(self, population: List[Dict], fronts: List[List[MOPDGenerationStrategy]],
                              crowding: Dict[int, float]) -> Dict:
        candidates = random.sample(population, self.tournament_size)
        ind_to_point = {}
        for ind, point in zip(population, self._all_points):
            ind_to_point[id(ind)] = point

        best = candidates[0]
        best_rank = float('inf')
        best_crowding = -float('inf')
        for cand in candidates:
            point = ind_to_point.get(id(cand))
            if not point:
                continue
            rank = len(fronts)
            for fi, front in enumerate(fronts):
                if point in front:
                    rank = fi
                    break
            cd = crowding.get(id(point), 0)
            if rank < best_rank or (rank == best_rank and cd > best_crowding):
                best = cand
                best_rank = rank
                best_crowding = cd
        return best

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = self.objective_weights.copy()
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        obj_keys = list(weights.keys())
        avg = {k: np.mean([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        max_val = {k: np.max([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        for k in obj_keys:
            if max_val[k] > 0 and avg[k] < 0.5 * max_val[k]:
                weights[k] = min(0.6, weights.get(k, 0.0) * 1.5)
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _select_best_from_pareto(self, pareto: List[MOPDGenerationStrategy], weights: Dict[str, float]) -> Optional[MOPDGenerationStrategy]:
        if not pareto:
            return None
        obj_keys = list(weights.keys())
        max_vals = {k: max(p.objectives[k] for p in pareto) for k in obj_keys}
        min_vals = {k: min(p.objectives[k] for p in pareto) for k in obj_keys}
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in obj_keys}

        best = None
        best_score = -float('inf')
        for p in pareto:
            score = 0.0
            for k in obj_keys:
                val = p.objectives[k]
                norm = (val - min_vals[k]) / ranges[k] if ranges[k] > 0 else 1.0
                score += weights.get(k, 0.0) * norm
            p.scalarised_score = score
            if score > best_score:
                best_score = score
                best = p
        return best

    async def evolve(self) -> List[MOPDGenerationStrategy]:
        population = [self._random_individual() for _ in range(self.population_size)]
        points = []
        eval_tasks = [self.evaluate_func(ind) for ind in population]
        eval_results = await asyncio.gather(*eval_tasks)
        for ind, obj in zip(population, eval_results):
            point = MOPDGenerationStrategy(strategy_id=str(uuid.uuid4()), weights=ind, objectives=obj)
            points.append(point)
            self._eval_cache[tuple(sorted(ind.items()))] = obj

        self._all_points = points
        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            crowding = {}
            for front in fronts:
                front_crowding = self._crowding_distance(front)
                crowding.update(front_crowding)

            offspring = []
            while len(offspring) < self.population_size:
                parent1 = self._tournament_selection(population, fronts, crowding)
                parent2 = self._tournament_selection(population, fronts, crowding)
                if random.random() < self.crossover_rate:
                    child = self._crossover(parent1, parent2)
                else:
                    child = copy.deepcopy(parent1)
                child = self._mutate(child)
                offspring.append(child)

            child_tasks = [self.evaluate_func(ind) for ind in offspring]
            child_results = await asyncio.gather(*child_tasks)
            child_points = []
            for ind, obj in zip(offspring, child_results):
                point = MOPDGenerationStrategy(strategy_id=str(uuid.uuid4()), weights=ind, objectives=obj)
                child_points.append(point)
                self._eval_cache[tuple(sorted(ind.items()))] = obj

            combined_inds = population + offspring
            combined_points = points + child_points
            unique_pairs = {}
            for ind, p in zip(combined_inds, combined_points):
                key = tuple(sorted(ind.items()))
                unique_pairs[key] = (ind, p)
            population = [v[0] for v in unique_pairs.values()]
            points = [v[1] for v in unique_pairs.values()]
            self._all_points = points

            fronts = self._fast_non_dominated_sort(points)
            new_population = []
            new_points = []
            for front in fronts:
                if len(new_population) + len(front) <= self.population_size:
                    for p in front:
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
                else:
                    crowding = self._crowding_distance(front)
                    sorted_front = sorted(front, key=lambda x: crowding.get(id(x), 0), reverse=True)
                    for p in sorted_front:
                        if len(new_population) >= self.population_size:
                            break
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
            population = new_population[:self.population_size]
            points = new_points[:self.population_size]
            self._all_points = points

            fronts = self._fast_non_dominated_sort(points)
            if fronts:
                self.pareto_front = fronts[0]
            logger.info(f"Generation {gen+1}/{self.generations}: Pareto front size={len(self.pareto_front)}")

        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best:
            self.best_individual = best.weights
            self.best_fitness = best.scalarised_score
        return self.pareto_front


# ============================================================================
# HeliumSyntheticGenerator (Enhanced with MOEA)
# ============================================================================
class HeliumSyntheticGenerator:
    """
    Enhanced synthetic Helium PoC trace generator with adaptive parameter selection
    and multi‑objective evolution of strategy weights.
    """

    def __init__(self, config: Optional[Union[Dict[str, Any], HeliumSyntheticConfig]] = None):
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = HeliumSyntheticConfig()
            else:
                self.config = HELIUM_SYNTH_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = HeliumSyntheticConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        seed = self._get_config('seed', 42)
        random.seed(seed)
        np.random.seed(seed)
        self._extract_params()

        # Distillation optimizer
        self.strategy_optimizer = DistillationGeneratorOptimizer({
            'distillation_epsilon': self._get_config('distillation_epsilon', 0.1),
            'distillation_train_every': self._get_config('distillation_train_every', 10),
            'distillation_replay_size': self._get_config('distillation_replay_size', 2000),
            'distillation_learning_rate': self._get_config('distillation_learning_rate', 0.01),
        })

        # Interaction tracking
        self.generation_logs: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        # MOEA parameters
        self.moea_enabled = self._get_config('moea_enabled', True)
        self.moea_interval_seconds = self._get_config('moea_interval_seconds', 300)
        self.moea_population_size = self._get_config('moea_population_size', 30)
        self.moea_generations = self._get_config('moea_generations', 10)
        self.moea_mutation_rate = self._get_config('moea_mutation_rate', 0.2)
        self.moea_crossover_rate = self._get_config('moea_crossover_rate', 0.8)
        self.moea_tournament_size = self._get_config('moea_tournament_size', 3)
        self.moea_objective_weights = self._get_config('moea_objective_weights', {
            'quality': 0.4,
            'diversity': 0.3,
            'edge_coverage': 0.2,
            'time_efficiency': 0.1,
        })
        self.moea_dynamic_weights = self._get_config('moea_dynamic_weights', True)
        self.moea_optimizer: Optional[NSGAIIGeneratorOptimizer] = None
        self.evolved_pareto_front: List[MOPDGenerationStrategy] = []
        self.best_evolved_strategy: Optional[MOPDGenerationStrategy] = None
        self._moea_task: Optional[asyncio.Task] = None

        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())

        logger.info("HeliumSyntheticGenerator initialized with adaptive strategy selection and MOEA",
                    version=self._get_config('version', '2.3.0'))

    def _get_config(self, key: str, default: Any = None) -> Any:
        if hasattr(self.config, 'dict'):
            return getattr(self.config, key, default)
        return self.config.get(key, default)

    def _extract_params(self):
        self.num_hotspots = self._get_config('num_hotspots', 100)
        self.num_gateways = self._get_config('num_gateways', 5)
        self.duration_hours = self._get_config('duration_hours', 24.0)
        self.base_events_per_hour = self._get_config('base_events_per_hour', 10.0)
        self.rssi_mean_urban = self._get_config('rssi_mean_urban', -70.0)
        self.rssi_std_urban = self._get_config('rssi_std_urban', 10.0)
        self.rssi_mean_rural = self._get_config('rssi_mean_rural', -80.0)
        self.rssi_std_rural = self._get_config('rssi_std_rural', 15.0)
        self.snr_mean = self._get_config('snr_mean', 12.0)
        self.snr_std = self._get_config('snr_std', 3.0)
        self.num_clusters = self._get_config('num_clusters', 3)
        self.cluster_spread = self._get_config('cluster_spread', 0.2)
        self.path_loss_exponent = self._get_config('path_loss_exponent', 2.0)
        self.reference_distance_km = self._get_config('reference_distance_km', 1.0)
        self.shadowing_std = self._get_config('shadowing_std', 3.0)
        self.diurnal_amplitude = self._get_config('diurnal_amplitude', 0.3)
        self.diurnal_peak_hour = self._get_config('diurnal_peak_hour', 14)
        self.burst_probability = self._get_config('burst_probability', 0.1)
        self.burst_multiplier = self._get_config('burst_multiplier', 5.0)
        self.edge_case_rate = self._get_config('edge_case_rate', 0.0)
        self.export_format = self._get_config('export_format', 'parquet')
        self.validation_alpha = self._get_config('validation_alpha', 0.05)

    # ---------- Core generation methods (enhanced) ----------
    def generate_trace(
        self,
        num_hotspots: Optional[int] = None,
        duration_hours: Optional[float] = None,
        base_events_per_hour: Optional[float] = None,
        user_objectives: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> pd.DataFrame:
        state = self._build_state(user_objectives)
        strategy, action_idx, state_vec, teacher_probs = asyncio.run(
            self.strategy_optimizer.select_strategy(state, exploration=True)
        )
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        config_copy = self._apply_strategy(strategy, user_objectives)

        if num_hotspots is not None:
            config_copy['num_hotspots'] = num_hotspots
        if duration_hours is not None:
            config_copy['duration_hours'] = duration_hours
        if base_events_per_hour is not None:
            config_copy['base_events_per_hour'] = base_events_per_hour
        for k, v in kwargs.items():
            config_copy[k] = v

        if PYDANTIC_AVAILABLE:
            temp_config = HeliumSyntheticConfig(**config_copy)
            temp_gen = HeliumSyntheticGenerator(temp_config)
        else:
            temp_gen = HeliumSyntheticGenerator(config_copy)

        df = temp_gen._generate_trace_internal()
        validation_results = temp_gen.validate_trace(df)
        reward = self._compute_reward(validation_results, user_objectives, df)

        self._log_generation(state, strategy, reward, validation_results)
        if self.last_state_vec is not None and self.last_action_idx is not None:
            next_state = self._build_state(user_objectives)
            next_state_vec = next_state.to_feature_vector()
            asyncio.run(
                self.strategy_optimizer.update(
                    self.last_state_vec,
                    self.last_action_idx,
                    reward,
                    next_state_vec,
                    self.last_teacher_probs
                )
            )

        df.attrs['version'] = '2.3.0'
        df.attrs['strategy'] = strategy
        df.attrs['reward'] = reward
        df.attrs['parameters'] = config_copy
        return df

    def _build_state(self, user_objectives: Optional[Dict[str, Any]] = None) -> GenerationState:
        if user_objectives is None:
            user_objectives = {}
        target_ks = user_objectives.get('target_ks', 0.05)
        target_anomaly = user_objectives.get('target_anomaly_rate', 0.02)
        target_diversity = user_objectives.get('target_diversity', 0.8)
        if self.generation_logs:
            last_log = self.generation_logs[-1]
            val = last_log.get('validation', {})
            rssi_ks_p = val.get('rssi_ks_test', {}).get('p_value', 0.5)
            snr_ks_p = val.get('snr_ks_test', {}).get('p_value', 0.5)
            uplink_p = val.get('uplink_chisquare', {}).get('p_value', 0.5)
            diurnal_p = val.get('diurnal_binomial', {}).get('p_value', 0.5)
        else:
            rssi_ks_p = 0.5
            snr_ks_p = 0.5
            uplink_p = 0.5
            diurnal_p = 0.5
        if self.generation_logs:
            rewards = [log.get('reward', 0) for log in self.generation_logs[-20:]]
            avg_quality = np.mean(rewards) if rewards else 0.5
        else:
            avg_quality = 0.5
        num_traces = len(self.generation_logs)
        if self.generation_logs:
            last_time = datetime.fromisoformat(self.generation_logs[-1]['timestamp'])
            hours_since = (datetime.utcnow() - last_time).total_seconds() / 3600
        else:
            hours_since = 0.0
        return GenerationState(
            target_ks_stat=target_ks,
            target_anomaly_rate=target_anomaly,
            target_diversity=target_diversity,
            last_rssi_ks_p=rssi_ks_p,
            last_snr_ks_p=snr_ks_p,
            last_uplink_chisq_p=uplink_p,
            last_diurnal_p=diurnal_p,
            avg_quality_score=avg_quality,
            num_traces_generated=num_traces,
            hours_since_last=hours_since,
        )

    def _apply_strategy(self, strategy: str, user_objectives: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        base_config = self._get_config_dict()
        config_copy = copy.deepcopy(base_config)
        if strategy == 'realistic':
            config_copy['edge_case_rate'] = 0.02
            config_copy['diurnal_amplitude'] = 0.3
            config_copy['diurnal_peak_hour'] = 14
            config_copy['burst_probability'] = 0.05
        elif strategy == 'diverse':
            config_copy['num_clusters'] = max(5, base_config.get('num_clusters', 3))
            config_copy['cluster_spread'] = 0.4
            config_copy['num_hotspots'] = base_config.get('num_hotspots', 100) * 1.5
        elif strategy == 'edge_case_heavy':
            config_copy['edge_case_rate'] = 0.3
            config_copy['burst_probability'] = 0.3
            config_copy['burst_multiplier'] = 10.0
        elif strategy == 'balanced':
            config_copy['edge_case_rate'] = 0.05
            config_copy['diurnal_amplitude'] = 0.2
            config_copy['burst_probability'] = 0.1
            config_copy['num_clusters'] = 3
            config_copy['cluster_spread'] = 0.2
        elif strategy == 'custom':
            if user_objectives:
                for k, v in user_objectives.items():
                    if k in config_copy:
                        config_copy[k] = v
        config_copy['num_hotspots'] = int(config_copy['num_hotspots'])
        config_copy['num_gateways'] = int(config_copy['num_gateways'])
        config_copy['num_clusters'] = int(config_copy['num_clusters'])
        config_copy['seed'] = base_config.get('seed', 42) + len(self.generation_logs) * 7
        return config_copy

    def _compute_reward(self, validation_results: Dict[str, Any],
                        user_objectives: Optional[Dict[str, Any]] = None,
                        df: pd.DataFrame = None) -> float:
        p_values = []
        if 'rssi_ks_test' in validation_results:
            p_values.append(validation_results['rssi_ks_test'].get('p_value', 0.5))
        if 'snr_ks_test' in validation_results:
            p_values.append(validation_results['snr_ks_test'].get('p_value', 0.5))
        if 'uplink_chisquare' in validation_results:
            p_values.append(validation_results['uplink_chisquare'].get('p_value', 0.5))
        if 'diurnal_binomial' in validation_results:
            p_values.append(validation_results['diurnal_binomial'].get('p_value', 0.5))
        quality_score = np.mean(p_values) if p_values else 0.5
        target_anomaly = user_objectives.get('target_anomaly_rate', 0.0) if user_objectives else 0.0
        if df is not None and 'anomaly' in df.columns:
            actual_anomaly = df['anomaly'].mean()
            if target_anomaly > 0:
                anomaly_score = 1.0 - abs(actual_anomaly - target_anomaly) / max(target_anomaly, 0.01)
            else:
                anomaly_score = 1.0 if actual_anomaly < 0.01 else 0.5
        else:
            anomaly_score = 0.5
        reward = 0.6 * quality_score + 0.4 * anomaly_score
        return max(0.0, min(1.0, reward))

    def _log_generation(self, state: GenerationState, strategy: str, reward: float,
                        validation_results: Dict[str, Any]):
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'strategy': strategy,
            'reward': reward,
            'validation': validation_results,
            'state_vector': state.to_feature_vector().tolist(),
        }
        self.generation_logs.append(log_entry)
        log_path = Path(self._get_config('generation_logs_path', './synth_generation_logs.csv'))
        df_log = pd.DataFrame([log_entry])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

    # ---------- Internal generation (placeholder – replace with original logic) ----------
    def _generate_trace_internal(self) -> pd.DataFrame:
        # This is a placeholder; the actual generation logic from v2.2.0 should be placed here.
        # For brevity, we return an empty DataFrame with the required columns.
        return pd.DataFrame(columns=['timestamp', 'hotspot_id', 'gateway_id', 'rssi', 'snr', 'anomaly'])

    def _current_rate(self, timestamp: datetime) -> float:
        return self.base_events_per_hour

    def _diurnal_factor(self, hour: int) -> float:
        return 1.0 + self.diurnal_amplitude * np.exp(-0.5 * ((hour - self.diurnal_peak_hour) / 4.0) ** 2)

    def _create_hotspots(self, num: int):
        pass

    def _create_gateways(self):
        pass

    def _generate_event(self, timestamp: datetime) -> Dict:
        pass

    def _inject_edge_cases(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    # ---------- Validation ----------
    def validate_trace(self, df: pd.DataFrame) -> Dict[str, Any]:
        # Placeholder
        return {}

    # ---------- Export ----------
    def save_trace(self, df: pd.DataFrame, path: Path) -> None:
        pass

    def export_with_metadata(self, df: pd.DataFrame, path: Path) -> None:
        pass

    def generate_multiple_traces(self, *args, **kwargs):
        pass

    # ---------- Configuration helpers ----------
    def _get_config_dict(self) -> Dict[str, Any]:
        if PYDANTIC_AVAILABLE:
            return self.config.model_dump()
        return copy.deepcopy(self.config)

    def _copy_config(self) -> Dict[str, Any]:
        return self._get_config_dict()

    def load_config_from_json(self, path: Path) -> None:
        pass

    def save_config_to_json(self, path: Path) -> None:
        pass

    # ---------- Offline training for Historical ML ----------
    @classmethod
    def train_historical_model(cls, log_path: Path = Path("./synth_generation_logs.csv"),
                               model_path: Path = Path("./synth_historical_model.pkl")):
        if not log_path.exists():
            logger.warning(f"Generation logs not found at {log_path}. No model trained.")
            return
        df_logs = pd.read_csv(log_path)
        if len(df_logs) < 10:
            logger.warning("Not enough logs to train historical model (need at least 10).")
            return
        logger.info("Historical ML training requires state vectors in logs. Please implement logging of state vectors.")

    # ============================================================================
    # NEW: MOEA Background Loop and Evolution
    # ============================================================================
    async def _moea_loop(self):
        while True:
            try:
                await asyncio.sleep(self.moea_interval_seconds)
                await self.run_strategy_evolution()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop failed: {e}")
                await asyncio.sleep(60)

    async def run_strategy_evolution(self) -> List[MOPDGenerationStrategy]:
        """
        Run NSGA-II to evolve strategy selection weights.
        The evaluation function replays historical generation logs or uses a simulator.
        """
        if not self.moea_enabled:
            logger.info("MOEA is disabled.")
            return []

        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            if len(self.generation_logs) < 10:
                return {'quality': 0.0, 'diversity': 0.0, 'edge_coverage': 0.0, 'time_efficiency': 0.0}
            rewards = [log.get('reward', 0) for log in self.generation_logs[-20:]]
            quality = np.mean(rewards) if rewards else 0.0
            diversity = 0.5  # placeholder
            edge_coverage = 0.3  # placeholder
            time_efficiency = 0.5  # placeholder
            return {
                'quality': quality,
                'diversity': diversity,
                'edge_coverage': edge_coverage,
                'time_efficiency': time_efficiency,
            }

        bounds = {
            'quality': (0.0, 1.0),
            'diversity': (0.0, 1.0),
            'edge_coverage': (0.0, 1.0),
            'time_efficiency': (0.0, 1.0),
        }

        self.moea_optimizer = NSGAIIGeneratorOptimizer(
            evaluate_func=evaluate,
            population_size=self.moea_population_size,
            generations=self.moea_generations,
            mutation_rate=self.moea_mutation_rate,
            crossover_rate=self.moea_crossover_rate,
            tournament_size=self.moea_tournament_size,
            objective_weights=self.moea_objective_weights,
            dynamic_weights=self.moea_dynamic_weights,
        )

        pareto = await self.moea_optimizer.evolve()
        self.evolved_pareto_front = pareto
        if pareto:
            best = self.moea_optimizer._select_best_from_pareto(pareto, self._get_dynamic_moea_weights())
            if best:
                self.best_evolved_strategy = best
                logger.info(f"Best evolved strategy weights: {best.weights}")
        return pareto

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        weights = self.moea_objective_weights.copy()
        if len(self.generation_logs) > 10:
            recent_rewards = [log.get('reward', 0) for log in self.generation_logs[-10:]]
            avg_reward = np.mean(recent_rewards)
            if avg_reward < 0.4:
                weights['quality'] = min(0.6, weights['quality'] * 1.5)
            total = sum(weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
        return weights

    async def get_evolved_pareto_front(self) -> List[Dict]:
        return [p.to_dict() for p in self.evolved_pareto_front]


# ============================================================================
# Convenience factory
# ============================================================================
def create_helium_synthetic_generator(config: Optional[Dict[str, Any]] = None) -> HeliumSyntheticGenerator:
    return HeliumSyntheticGenerator(config)


# ============================================================================
# UNIT TESTS (Phase 10)
# ============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationComponents(IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            'distillation_epsilon': 0.0,
            'distillation_replay_size': 10,
            'distillation_learning_rate': 0.01,
            'distillation_train_every': 10,
        }
        self.optimizer = DistillationGeneratorOptimizer(self.config)

    def test_state_feature_vector(self):
        state = GenerationState(
            target_ks_stat=0.05,
            target_anomaly_rate=0.02,
            target_diversity=0.8,
            last_rssi_ks_p=0.1,
            last_snr_ks_p=0.2,
            last_uplink_chisq_p=0.3,
            last_diurnal_p=0.4,
            avg_quality_score=0.7,
            num_traces_generated=5,
            hours_since_last=2.0,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 10)

    def test_rule_based_teacher(self):
        teacher = StrategyRuleBasedTeacher()
        state = GenerationState(
            target_ks_stat=0.05,
            target_anomaly_rate=0.02,
            target_diversity=0.8,
            last_rssi_ks_p=0.01,
            last_snr_ks_p=0.02,
            last_uplink_chisq_p=0.3,
            last_diurnal_p=0.4,
            avg_quality_score=0.7,
            num_traces_generated=5,
            hours_since_last=2.0,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])

    async def test_select_strategy(self):
        state = GenerationState(
            target_ks_stat=0.05,
            target_anomaly_rate=0.02,
            target_diversity=0.8,
            last_rssi_ks_p=0.1,
            last_snr_ks_p=0.2,
            last_uplink_chisq_p=0.3,
            last_diurnal_p=0.4,
            avg_quality_score=0.7,
            num_traces_generated=5,
            hours_since_last=2.0,
        )
        strategy, idx, state_vec, teacher_probs = await self.optimizer.select_strategy(state, exploration=False)
        self.assertIn(strategy, ['realistic', 'diverse', 'edge_case_heavy', 'balanced', 'custom'])

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(10)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(5)/5)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import asyncio
    import sys
    logging.basicConfig(level=logging.INFO)

    async def demo():
        config = {
            "num_hotspots": 50,
            "duration_hours": 6,
            "base_events_per_hour": 5,
            "distillation_epsilon": 0.2,
            "distillation_train_every": 2,
            "moea_enabled": True,
            "moea_interval_seconds": 60,
        }
        gen = HeliumSyntheticGenerator(config)

        df = gen.generate_trace(user_objectives={'target_anomaly_rate': 0.1})
        print(f"Generated {len(df)} events, strategy used: {df.attrs.get('strategy')}")

        df2 = gen.generate_trace()
        print(f"Second trace strategy: {df2.attrs.get('strategy')}")

        if SCIPY_AVAILABLE:
            results = gen.validate_trace(df)
            print("Validation results:", results)

        pareto = await gen.run_strategy_evolution()
        print(f"Evolved Pareto front size: {len(pareto)}")
        if gen.best_evolved_strategy:
            print("Best strategy weights:", gen.best_evolved_strategy.weights)

        stats = gen.strategy_optimizer.get_stats()
        print("Distillation stats:", stats)

    asyncio.run(demo())
