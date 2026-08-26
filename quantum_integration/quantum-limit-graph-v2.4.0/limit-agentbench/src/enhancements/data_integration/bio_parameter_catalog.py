# src/enhancements/data_integration/bio_parameter_catalog_v2_2_0.py
"""
Enhanced Bio‑Parameter Catalog v2.3.0
======================================
Curated catalog of organism‑like efficiency profiles with adaptive recommendation
via Multi‑Teacher On‑Policy Distillation, plus Multi‑Objective Evolutionary Optimization (MOEA)
to evolve new profiles that trade off multiple objectives.

ENHANCEMENTS OVER v2.2.0:
- Added NSGA‑II optimizer to evolve profile parameters (photosynthetic efficiency,
  resilience, carbon fixation, helium affinity).
- Maintains a Pareto front of non‑dominated profiles.
- MODP‑based selection of best profile using dynamic objective weights.
- Background task for periodic evolution.
- New configuration parameters for MOEA.
- Persistence of evolved profiles.

All previous features (distillation, CRUD, search, export/import, file watcher) are retained.
"""

import json
import time
import threading
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timezone
import hashlib
import logging
from collections import deque
import random
import numpy as np
from abc import ABC, abstractmethod
import pickle
import pandas as pd
import asyncio
import copy

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

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
# Data Models (Pydantic or dataclass fallback)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class OrganismProfile(BaseModel):
        """Profile for an organism type."""
        photosynthetic_efficiency: float = Field(0.5, ge=0, le=1)
        resilience_to_stress: float = Field(0.5, ge=0, le=1)
        carbon_fixation_rate: float = Field(0.5, ge=0, le=1)
        helium_affinity: float = Field(0.5, ge=0, le=1)

        @field_validator('photosynthetic_efficiency', 'resilience_to_stress', 'carbon_fixation_rate', 'helium_affinity')
        @classmethod
        def validate_range(cls, v):
            if not 0 <= v <= 1:
                raise ValueError("Value must be between 0 and 1")
            return v

    class CatalogMetadata(BaseModel):
        version: str = "2.3.0"
        last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
        source: str = "manual"
        hash: Optional[str] = None

    class BioParameterCatalogData(BaseModel):
        metadata: CatalogMetadata = Field(default_factory=CatalogMetadata)
        organism_types: Dict[str, OrganismProfile] = Field(default_factory=dict)

else:
    from dataclasses import dataclass, field

    @dataclass
    class OrganismProfile:
        photosynthetic_efficiency: float = 0.5
        resilience_to_stress: float = 0.5
        carbon_fixation_rate: float = 0.5
        helium_affinity: float = 0.5

        def __post_init__(self):
            for attr in ['photosynthetic_efficiency', 'resilience_to_stress', 'carbon_fixation_rate', 'helium_affinity']:
                val = getattr(self, attr)
                if not 0 <= val <= 1:
                    raise ValueError(f"{attr} must be between 0 and 1")

    @dataclass
    class CatalogMetadata:
        version: str = "2.3.0"
        last_updated: Optional[datetime] = field(default_factory=lambda: datetime.now(timezone.utc))
        source: str = "manual"
        hash: Optional[str] = None

    @dataclass
    class BioParameterCatalogData:
        metadata: CatalogMetadata = field(default_factory=CatalogMetadata)
        organism_types: Dict[str, OrganismProfile] = field(default_factory=dict)


# ============================================================================
# File Watcher (optional)
# ============================================================================
class FileWatcher:
    """Simple file watcher that polls for changes."""
    def __init__(self, file_path: Path, callback: callable, interval: float = 5.0):
        self.file_path = file_path
        self.callback = callback
        self.interval = interval
        self.last_mtime = file_path.stat().st_mtime if file_path.exists() else 0
        self.running = False
        self.thread = None

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._poll, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()

    def _poll(self):
        while self.running:
            try:
                if self.file_path.exists():
                    mtime = self.file_path.stat().st_mtime
                    if mtime != self.last_mtime:
                        self.last_mtime = mtime
                        self.callback()
            except Exception as e:
                logger.error("FileWatcher error", error=str(e))
            time.sleep(self.interval)


# ============================================================================
# DISTILLATION COMPONENTS FOR PROFILE SELECTION
# ============================================================================

@dataclass
class ProfileSelectionState:
    """State for the distillation agent."""
    # Environmental
    carbon_intensity: float
    helium_scarcity: float
    temperature: float
    humidity: float
    # Task requirements
    required_efficiency: float
    required_resilience: float
    required_carbon_fixation: float
    required_helium_affinity: float
    # Historical performance
    avg_success_score: float
    # Time context
    hour_of_day: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 10‑dim numeric feature vector."""
        features = [
            min(self.carbon_intensity / 1000.0, 1.0),
            self.helium_scarcity,
            min(self.temperature / 50.0, 1.0),
            min(self.humidity / 100.0, 1.0),
            self.required_efficiency,
            self.required_resilience,
            self.required_carbon_fixation,
            self.required_helium_affinity,
            self.avg_success_score,
            self.hour_of_day / 24.0,
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: ProfileSelectionState) -> np.ndarray:
        """Return probability vector over available organism types."""
        pass

    @abstractmethod
    def confidence(self, state: ProfileSelectionState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class ProfileRuleBasedTeacher(Teacher):
    """Rule‑based expert."""
    def __init__(self, catalog: 'BioParameterCatalog'):
        self.catalog = catalog

    def predict(self, state: ProfileSelectionState) -> np.ndarray:
        available = self.catalog.list_organism_types()
        n = len(available)
        probs = np.ones(n) * 0.1
        # Heuristics: map state to a recommended type
        if state.carbon_intensity > 700:
            if 'low_carbon' in available:
                idx = available.index('low_carbon')
                probs[idx] = 0.8
        elif state.helium_scarcity > 0.6:
            if 'high_robustness' in available:
                idx = available.index('high_robustness')
                probs[idx] = 0.7
        elif state.required_efficiency > 0.8:
            if 'high_efficiency' in available:
                idx = available.index('high_efficiency')
                probs[idx] = 0.7
        else:
            probs[:] = 1.0 / n
        return probs / probs.sum()

    def confidence(self, state: ProfileSelectionState) -> float:
        if state.carbon_intensity > 700:
            return 0.6
        return 0.4


class ProfileHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past interactions."""
    def __init__(self, catalog: 'BioParameterCatalog', model_path: Optional[Path] = None):
        self.catalog = catalog
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path("./profile_historical_model.pkl")
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: ProfileSelectionState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            available = self.catalog.list_organism_types()
            return np.ones(len(available)) / len(available)
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        # We need to align probabilities with current catalog order.
        # For simplicity, we return probs for the classes the model knows.
        # In a real system, we'd map to current available types.
        return probs

    def confidence(self, state: ProfileSelectionState) -> float:
        return 0.7 if self.model is not None else 0.0


class ProfileStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, catalog: 'BioParameterCatalog', lr: float = 0.1):
        self.catalog = catalog
        self.lr = lr
        self.weights = {}  # organism_type -> weight vector
        self._load_state()

    def _load_state(self):
        path = Path("./profile_q_weights.json")
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                for k, v in data.items():
                    self.weights[k] = np.array(v)
                logger.info(f"Loaded Q‑weights for {len(self.weights)} types")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path("./profile_q_weights.json")
        data = {k: v.tolist() for k, v in self.weights.items()}
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)

    def predict(self, state: ProfileSelectionState) -> np.ndarray:
        available = self.catalog.list_organism_types()
        n = len(available)
        q_values = np.zeros(n)
        for i, org_type in enumerate(available):
            if org_type in self.weights:
                q_values[i] = np.dot(state.to_feature_vector(), self.weights[org_type])
            else:
                q_values[i] = 0.0
        # Softmax
        exp_q = np.exp(q_values - np.max(q_values))
        return exp_q / exp_q.sum()

    def confidence(self, state: ProfileSelectionState) -> float:
        return 0.5

    def update(self, state: ProfileSelectionState, organism_type: str, reward: float):
        if organism_type not in self.weights:
            self.weights[organism_type] = np.zeros(10)  # feature dim
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[organism_type])
        self.weights[organism_type] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 10, n_classes: int = 5, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray, num_classes: int) -> np.ndarray:
        # Resize if number of classes changed
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

        # Distillation gradient
        grad_distill = -(teacher_probs - current_probs)

        # Policy gradient
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


class DistillationProfileOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for organism profile selection.
    """
    def __init__(self, catalog: 'BioParameterCatalog', config: Dict[str, Any]):
        self.catalog = catalog
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            ProfileRuleBasedTeacher(catalog),
            ProfileHistoricalMLTeacher(catalog),
            ProfileStatefulQTeacher(catalog)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_profile(self, state: ProfileSelectionState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        available = self.catalog.list_organism_types()
        if not available:
            raise ValueError("No organism types available")
        state_vec = state.to_feature_vector()

        # Ensemble teachers
        teacher_probs = np.zeros(len(available))
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            # Align length
            if len(prob) != len(available):
                if len(prob) < len(available):
                    prob = np.pad(prob, (0, len(available) - len(prob)), 'constant')
                else:
                    prob = prob[:len(available)]
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(len(available)) / len(available)

        student_probs = self.student.predict_proba(state_vec, len(available))

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, len(available) - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return available[action_idx], action_idx, state_vec, teacher_probs

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
# NEW: Multi‑Objective Profile Evolution (NSGA‑II)
# ============================================================================

@dataclass
class MOPDProfilePoint:
    """A profile with its objective vector (all maximized)."""
    profile_id: str
    parameters: Dict[str, float]  # 4 parameters
    objectives: Dict[str, float]  # e.g., {'efficiency': 0.8, 'resilience': 0.9, ...}
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'profile_id': self.profile_id,
            'parameters': self.parameters,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDProfilePoint':
        return cls(**data)


class NSGAIIProfileOptimizer:
    """
    NSGA‑II for evolving organism profile parameters.
    Decision variables: 4 continuous parameters in [0,1].
    Objectives: maximize efficiency, resilience, carbon_fixation, helium_affinity.
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
            'efficiency': 0.25,
            'resilience': 0.25,
            'carbon_fixation': 0.25,
            'helium_affinity': 0.25,
        }
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDProfilePoint] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}

    def _random_individual(self) -> Dict[str, float]:
        return {
            'photosynthetic_efficiency': random.random(),
            'resilience_to_stress': random.random(),
            'carbon_fixation_rate': random.random(),
            'helium_affinity': random.random(),
        }

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
        child = {}
        for key in p1:
            if random.random() < 0.5:
                # SBX
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                child[key] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[key] + (1 - beta) * p2[key])))
            else:
                child[key] = p1[key] if random.random() < 0.5 else p2[key]
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
        return mutant

    def _fast_non_dominated_sort(self, points: List[MOPDProfilePoint]) -> List[List[MOPDProfilePoint]]:
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

    def _crowding_distance(self, front: List[MOPDProfilePoint]) -> Dict[int, float]:
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

    def _tournament_selection(self, population: List[Dict], fronts: List[List[MOPDProfilePoint]],
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

    def _select_best_from_pareto(self, pareto: List[MOPDProfilePoint], weights: Dict[str, float]) -> Optional[MOPDProfilePoint]:
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

    async def evolve(self) -> List[MOPDProfilePoint]:
        population = [self._random_individual() for _ in range(self.population_size)]
        points = []
        eval_tasks = [self.evaluate_func(ind) for ind in population]
        eval_results = await asyncio.gather(*eval_tasks)
        for ind, obj in zip(population, eval_results):
            point = MOPDProfilePoint(
                profile_id=str(uuid.uuid4()),
                parameters=ind,
                objectives=obj
            )
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
                point = MOPDProfilePoint(
                    profile_id=str(uuid.uuid4()),
                    parameters=ind,
                    objectives=obj
                )
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
            self.best_individual = best.parameters
            self.best_fitness = best.scalarised_score
        return self.pareto_front


# ============================================================================
# Enhanced BioParameterCatalog (with Distillation + Evolution)
# ============================================================================
class BioParameterCatalog:
    """
    Enhanced catalog of organism‑like efficiency profiles with adaptive recommendation
    and multi‑objective evolution of new profiles.
    """

    def __init__(
        self,
        catalog_path: Path = Path("./bio_parameters.json"),
        auto_reload: bool = False,
        validate_on_load: bool = True,
        # Distillation parameters
        distillation_epsilon: float = 0.1,
        distillation_train_every: int = 10,
        distillation_replay_size: int = 2000,
        distillation_learning_rate: float = 0.01,
        # MOEA parameters
        moea_enabled: bool = True,
        moea_population_size: int = 20,
        moea_generations: int = 5,
        moea_mutation_rate: float = 0.2,
        moea_crossover_rate: float = 0.8,
        moea_tournament_size: int = 3,
        moea_objective_weights: Optional[Dict[str, float]] = None,
        moea_dynamic_weights: bool = True,
        moea_interval_seconds: int = 300,
    ):
        self.catalog_path = catalog_path
        self.auto_reload = auto_reload
        self.validate_on_load = validate_on_load
        self._lock = threading.RLock()
        self._data: Optional[BioParameterCatalogData] = None
        self._file_watcher: Optional[FileWatcher] = None

        # Distillation optimizer
        self.distillation_config = {
            'distillation_epsilon': distillation_epsilon,
            'distillation_train_every': distillation_train_every,
            'distillation_replay_size': distillation_replay_size,
            'distillation_learning_rate': distillation_learning_rate,
        }
        self.profile_optimizer = DistillationProfileOptimizer(self, self.distillation_config)

        # Interaction tracking
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        # MOEA parameters
        self.moea_enabled = moea_enabled
        self.moea_population_size = moea_population_size
        self.moea_generations = moea_generations
        self.moea_mutation_rate = moea_mutation_rate
        self.moea_crossover_rate = moea_crossover_rate
        self.moea_tournament_size = moea_tournament_size
        self.moea_objective_weights = moea_objective_weights or {
            'efficiency': 0.25,
            'resilience': 0.25,
            'carbon_fixation': 0.25,
            'helium_affinity': 0.25,
        }
        self.moea_dynamic_weights = moea_dynamic_weights
        self.moea_interval_seconds = moea_interval_seconds
        self.moea_optimizer: Optional[NSGAIIProfileOptimizer] = None
        self.evolved_pareto_front: List[MOPDProfilePoint] = []
        self.best_evolved_profile: Optional[MOPDProfilePoint] = None
        self._moea_task: Optional[asyncio.Task] = None

        # Load initial data
        self._load()

        # Start file watcher if requested
        if auto_reload:
            self._file_watcher = FileWatcher(
                catalog_path, self.reload_from_disk, interval=5.0
            )
            self._file_watcher.start()

        # Start MOEA background task if enabled
        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())

        logger.info("BioParameterCatalog initialized with adaptive recommendation and MOEA", path=str(catalog_path))

    # ---------- Core loading/saving ----------
    def _load(self):
        if self.catalog_path.exists():
            with open(self.catalog_path, 'r') as f:
                raw = json.load(f)
            if self.validate_on_load and PYDANTIC_AVAILABLE:
                try:
                    self._data = BioParameterCatalogData(**raw)
                except ValidationError as e:
                    logger.error("Validation failed, using defaults", error=str(e))
                    self._reset_to_defaults()
            else:
                metadata_dict = raw.get('metadata', {})
                if 'last_updated' in metadata_dict and isinstance(metadata_dict['last_updated'], str):
                    try:
                        metadata_dict['last_updated'] = datetime.fromisoformat(metadata_dict['last_updated'])
                    except ValueError:
                        metadata_dict['last_updated'] = datetime.now(timezone.utc)
                metadata = CatalogMetadata(**metadata_dict)
                organism_types = {}
                for k, v in raw.get('organism_types', {}).items():
                    try:
                        organism_types[k] = OrganismProfile(**v)
                    except ValueError as e:
                        logger.warning(f"Invalid profile for {k}, skipping: {e}")
                self._data = BioParameterCatalogData(metadata, organism_types)
        else:
            self._reset_to_defaults()
            self.save()

    def _reset_to_defaults(self):
        default_organisms = {
            "high_efficiency": OrganismProfile(
                photosynthetic_efficiency=0.8,
                resilience_to_stress=0.6,
                carbon_fixation_rate=0.9,
                helium_affinity=0.7,
            ),
            "high_robustness": OrganismProfile(
                photosynthetic_efficiency=0.5,
                resilience_to_stress=0.9,
                carbon_fixation_rate=0.6,
                helium_affinity=0.5,
            ),
            "low_carbon": OrganismProfile(
                photosynthetic_efficiency=0.7,
                resilience_to_stress=0.5,
                carbon_fixation_rate=0.4,
                helium_affinity=0.3,
            ),
        }
        metadata = CatalogMetadata(
            version="2.3.0",
            last_updated=datetime.now(timezone.utc),
            source="default",
            hash=self._compute_hash(default_organisms),
        )
        self._data = BioParameterCatalogData(metadata, default_organisms)

    def reload_from_disk(self):
        with self._lock:
            logger.info("Reloading catalog from disk")
            self._load()

    def save(self):
        with self._lock:
            self._data.metadata.last_updated = datetime.now(timezone.utc)
            self._data.metadata.hash = self._compute_hash(self._data.organism_types)

            if PYDANTIC_AVAILABLE:
                data = self._data.model_dump(mode='json')
            else:
                data = {
                    "metadata": {
                        "version": self._data.metadata.version,
                        "last_updated": self._data.metadata.last_updated.isoformat() if self._data.metadata.last_updated else None,
                        "source": self._data.metadata.source,
                        "hash": self._data.metadata.hash,
                    },
                    "organism_types": {
                        k: {
                            "photosynthetic_efficiency": v.photosynthetic_efficiency,
                            "resilience_to_stress": v.resilience_to_stress,
                            "carbon_fixation_rate": v.carbon_fixation_rate,
                            "helium_affinity": v.helium_affinity,
                        }
                        for k, v in self._data.organism_types.items()
                    }
                }
            with open(self.catalog_path, 'w') as f:
                json.dump(data, f, indent=2)

    def _compute_hash(self, organism_types: Dict) -> str:
        if PYDANTIC_AVAILABLE:
            serializable = {k: v.model_dump() for k, v in organism_types.items()}
        else:
            serializable = {
                k: {
                    "photosynthetic_efficiency": v.photosynthetic_efficiency,
                    "resilience_to_stress": v.resilience_to_stress,
                    "carbon_fixation_rate": v.carbon_fixation_rate,
                    "helium_affinity": v.helium_affinity,
                }
                for k, v in organism_types.items()
            }
        content = json.dumps(serializable, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    # ---------- Public query methods ----------
    def get_parameters(self, organism_type: str) -> Dict[str, float]:
        with self._lock:
            profile = self._data.organism_types.get(organism_type)
            if profile:
                if PYDANTIC_AVAILABLE:
                    return profile.model_dump()
                else:
                    return {
                        'photosynthetic_efficiency': profile.photosynthetic_efficiency,
                        'resilience_to_stress': profile.resilience_to_stress,
                        'carbon_fixation_rate': profile.carbon_fixation_rate,
                        'helium_affinity': profile.helium_affinity,
                    }
            return {}

    def list_organism_types(self) -> List[str]:
        with self._lock:
            return list(self._data.organism_types.keys())

    def search(self, **filters) -> List[str]:
        with self._lock:
            results = []
            for name, profile in self._data.organism_types.items():
                match = True
                for key, value in filters.items():
                    if '__' in key:
                        field, op = key.split('__', 1)
                    else:
                        field, op = key, 'eq'
                    attr = getattr(profile, field, None)
                    if attr is None:
                        match = False
                        break
                    if op == 'eq':
                        if attr != value:
                            match = False
                            break
                    elif op == 'ne':
                        if attr == value:
                            match = False
                            break
                    elif op == 'gte':
                        if attr < value:
                            match = False
                            break
                    elif op == 'lte':
                        if attr > value:
                            match = False
                            break
                    elif op == 'gt':
                        if attr <= value:
                            match = False
                            break
                    elif op == 'lt':
                        if attr >= value:
                            match = False
                            break
                    else:
                        match = False
                        break
                if match:
                    results.append(name)
            return results

    # ---------- CRUD operations ----------
    def add_organism_type(self, name: str, profile: Dict[str, float]) -> bool:
        if not name or not name.strip():
            logger.error("Organism type name cannot be empty")
            return False

        with self._lock:
            if PYDANTIC_AVAILABLE:
                try:
                    validated = OrganismProfile(**profile)
                except ValidationError as e:
                    logger.error("Invalid profile", error=str(e))
                    return False
            else:
                required = ['photosynthetic_efficiency', 'resilience_to_stress', 'carbon_fixation_rate', 'helium_affinity']
                for key in required:
                    if key not in profile:
                        logger.error(f"Missing required key: {key}")
                        return False
                try:
                    validated = OrganismProfile(**profile)
                except ValueError as e:
                    logger.error("Invalid profile", error=str(e))
                    return False
            self._data.organism_types[name] = validated
            self.save()
            return True

    def remove_organism_type(self, name: str) -> bool:
        with self._lock:
            if name in self._data.organism_types:
                del self._data.organism_types[name]
                self.save()
                return True
            return False

    def get_metadata(self) -> Dict[str, Any]:
        with self._lock:
            meta = self._data.metadata
            return {
                'version': meta.version,
                'last_updated': meta.last_updated.isoformat() if meta.last_updated else None,
                'source': meta.source,
                'hash': meta.hash,
                'count': len(self._data.organism_types),
            }

    # ---------- Export/import ----------
    def export_catalog(self, path: Path) -> None:
        metadata = self._data.metadata
        if PYDANTIC_AVAILABLE:
            data = self._data.model_dump(mode='json')
        else:
            data = {
                "metadata": {
                    "version": metadata.version,
                    "last_updated": metadata.last_updated.isoformat() if metadata.last_updated else None,
                    "source": metadata.source,
                    "hash": metadata.hash,
                },
                "organism_types": {
                    k: {
                        "photosynthetic_efficiency": v.photosynthetic_efficiency,
                        "resilience_to_stress": v.resilience_to_stress,
                        "carbon_fixation_rate": v.carbon_fixation_rate,
                        "helium_affinity": v.helium_affinity,
                    }
                    for k, v in self._data.organism_types.items()
                }
            }
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Catalog exported to {path}")

    def import_catalog(self, path: Path, merge: bool = False) -> int:
        with open(path, 'r') as f:
            raw = json.load(f)

        if PYDANTIC_AVAILABLE:
            try:
                imported = BioParameterCatalogData(**raw)
            except ValidationError as e:
                logger.error("Imported catalog validation failed", error=str(e))
                return 0
        else:
            metadata_dict = raw.get('metadata', {})
            if 'last_updated' in metadata_dict and isinstance(metadata_dict['last_updated'], str):
                try:
                    metadata_dict['last_updated'] = datetime.fromisoformat(metadata_dict['last_updated'])
                except ValueError:
                    metadata_dict['last_updated'] = datetime.now(timezone.utc)
            metadata = CatalogMetadata(**metadata_dict)
            organism_types = {}
            for k, v in raw.get('organism_types', {}).items():
                try:
                    organism_types[k] = OrganismProfile(**v)
                except ValueError as e:
                    logger.warning(f"Invalid profile for {k}, skipping: {e}")
            imported = BioParameterCatalogData(metadata, organism_types)

        with self._lock:
            if merge:
                self._data.organism_types.update(imported.organism_types)
                self._data.metadata.last_updated = datetime.now(timezone.utc)
                self._data.metadata.source = "imported"
                self.save()
            else:
                self._data = imported
                self.save()
        return len(imported.organism_types)

    # ---------- NEW: Adaptive recommendation (distillation) ----------
    def build_state(self, context: Dict[str, Any]) -> ProfileSelectionState:
        if self.interaction_log:
            success_scores = [entry.get('reward', 0) for entry in self.interaction_log[-50:]]
            avg_success = np.mean(success_scores) if success_scores else 0.5
        else:
            avg_success = 0.5

        return ProfileSelectionState(
            carbon_intensity=context.get('carbon_intensity', 400.0),
            helium_scarcity=context.get('helium_scarcity', 0.5),
            temperature=context.get('temperature', 25.0),
            humidity=context.get('humidity', 50.0),
            required_efficiency=context.get('required_efficiency', 0.5),
            required_resilience=context.get('required_resilience', 0.5),
            required_carbon_fixation=context.get('required_carbon_fixation', 0.5),
            required_helium_affinity=context.get('required_helium_affinity', 0.5),
            avg_success_score=avg_success,
            hour_of_day=datetime.now().hour,
        )

    async def recommend_profile(self, context: Dict[str, Any], exploration: bool = True) -> Tuple[str, Dict[str, float]]:
        state = self.build_state(context)
        organism_type, action_idx, state_vec, teacher_probs = await self.profile_optimizer.select_profile(state, exploration=exploration)
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs
        params = self.get_parameters(organism_type)
        return organism_type, params

    def record_outcome(self, organism_type: str, performance: float, user_rating: Optional[float] = None):
        if user_rating is not None:
            reward = 0.7 * performance + 0.3 * user_rating
        else:
            reward = performance
        reward = max(0.0, min(1.0, reward))

        self.interaction_log.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'organism_type': organism_type,
            'performance': performance,
            'user_rating': user_rating,
            'reward': reward,
        })
        log_path = Path("./profile_interactions.csv")
        df_log = pd.DataFrame([self.interaction_log[-1]])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

        if self.last_state_vec is not None and self.last_action_idx is not None:
            current_state = self.build_state({})
            next_state_vec = current_state.to_feature_vector()
            asyncio.run(
                self.profile_optimizer.update(
                    self.last_state_vec,
                    self.last_action_idx,
                    reward,
                    next_state_vec,
                    self.last_teacher_probs
                )
            )

    # ---------- Offline training for Historical ML ----------
    @classmethod
    def train_historical_model(cls, log_path: Path = Path("./profile_interactions.csv"), model_path: Path = Path("./profile_historical_model.pkl")):
        if not log_path.exists():
            logger.warning(f"Interaction logs not found at {log_path}. No model trained.")
            return

        df_logs = pd.read_csv(log_path)
        if len(df_logs) < 10:
            logger.warning("Not enough logs to train historical model (need at least 10).")
            return

        logger.info("Historical ML training requires state vectors in logs. Please implement logging of state vectors.")

    # ---------- NEW: MOEA Integration ----------
    async def _moea_loop(self):
        while True:
            try:
                await asyncio.sleep(self.moea_interval_seconds)
                await self.run_profile_evolution()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop failed: {e}")
                await asyncio.sleep(60)

    async def run_profile_evolution(self) -> List[MOPDProfilePoint]:
        """
        Run NSGA-II to evolve new organism profiles (parameter vectors).
        The objectives are simply the four profile parameters (higher is better).
        The evolved profiles can be added to the catalog or used for recommendation.
        """
        if not self.moea_enabled:
            logger.info("MOEA is disabled.")
            return []

        async def evaluate(params: Dict[str, float]) -> Dict[str, float]:
            # In this simple version, the objectives equal the parameters.
            # In a real system, you could simulate performance under current environmental conditions.
            return {
                'efficiency': params['photosynthetic_efficiency'],
                'resilience': params['resilience_to_stress'],
                'carbon_fixation': params['carbon_fixation_rate'],
                'helium_affinity': params['helium_affinity'],
            }

        self.moea_optimizer = NSGAIIProfileOptimizer(
            evaluate_func=evaluate,
            population_size=self.moea_population_size,
            generations=self.moea_generations,
            mutation_rate=self.moea_mutation_rate,
            crossover_rate=self.moea_crossover_rate,
            tournament_size=self.moea_tournament_size,
            objective_weights=self._get_dynamic_moea_weights(),
            dynamic_weights=self.moea_dynamic_weights,
        )

        pareto = await self.moea_optimizer.evolve()
        self.evolved_pareto_front = pareto

        if pareto:
            best = self.moea_optimizer._select_best_from_pareto(
                pareto,
                self._get_dynamic_moea_weights()
            )
            if best:
                self.best_evolved_profile = best
                logger.info(f"Best evolved profile params: {best.parameters}")
                # Add the best profile to the catalog (optional)
                self.add_organism_type(
                    f"evolved_{best.profile_id[:8]}",
                    best.parameters
                )
        return pareto

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        # Could be adapted based on environmental conditions, but for simplicity return static.
        return self.moea_objective_weights.copy()

    async def get_evolved_pareto_front(self) -> List[Dict]:
        return [p.to_dict() for p in self.evolved_pareto_front]

    def get_evolved_profile(self, profile_id: str) -> Optional[Dict]:
        for p in self.evolved_pareto_front:
            if p.profile_id == profile_id:
                return p.parameters
        return None

    # ---------- Cleanup ----------
    def close(self):
        if self._file_watcher:
            self._file_watcher.stop()
        if self._moea_task:
            self._moea_task.cancel()
        logger.info("BioParameterCatalog closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ============================================================================
# Convenience factory
# ============================================================================
def create_bio_catalog(
    catalog_path: Path = Path("./bio_parameters.json"),
    auto_reload: bool = False,
    **kwargs,
) -> BioParameterCatalog:
    return BioParameterCatalog(catalog_path, auto_reload, **kwargs)


# ============================================================================
# UNIT TESTS (Phase 10)
# ============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationAndEvolution(IsolatedAsyncioTestCase):
    def setUp(self):
        self.catalog = BioParameterCatalog(catalog_path=Path("./test_bio_parameters.json"), auto_reload=False)
        if not self.catalog.list_organism_types():
            self.catalog.add_organism_type("test_type", {
                "photosynthetic_efficiency": 0.5,
                "resilience_to_stress": 0.5,
                "carbon_fixation_rate": 0.5,
                "helium_affinity": 0.5,
            })

    def test_state_feature_vector(self):
        state = ProfileSelectionState(
            carbon_intensity=400.0,
            helium_scarcity=0.5,
            temperature=25.0,
            humidity=50.0,
            required_efficiency=0.6,
            required_resilience=0.7,
            required_carbon_fixation=0.8,
            required_helium_affinity=0.9,
            avg_success_score=0.5,
            hour_of_day=12,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 10)

    def test_rule_based_teacher(self):
        teacher = ProfileRuleBasedTeacher(self.catalog)
        state = ProfileSelectionState(
            carbon_intensity=800.0, helium_scarcity=0.5, temperature=25.0, humidity=50.0,
            required_efficiency=0.5, required_resilience=0.5,
            required_carbon_fixation=0.5, required_helium_affinity=0.5,
            avg_success_score=0.5, hour_of_day=12,
        )
        probs = teacher.predict(state)
        self.assertEqual(len(probs), len(self.catalog.list_organism_types()))

    async def test_select_profile(self):
        optimizer = DistillationProfileOptimizer(self.catalog, {'distillation_epsilon': 0.0, 'distillation_replay_size': 10, 'distillation_learning_rate': 0.01, 'distillation_train_every': 10})
        state = ProfileSelectionState(
            carbon_intensity=400.0, helium_scarcity=0.5, temperature=25.0, humidity=50.0,
            required_efficiency=0.6, required_resilience=0.7,
            required_carbon_fixation=0.8, required_helium_affinity=0.9,
            avg_success_score=0.5, hour_of_day=12,
        )
        profile, idx, _, _ = await optimizer.select_profile(state, exploration=False)
        self.assertIn(profile, self.catalog.list_organism_types())

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(10)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(3)/3)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)

    async def test_profile_evolution(self):
        # Run evolution with small parameters for testing
        self.catalog.moea_population_size = 5
        self.catalog.moea_generations = 2
        pareto = await self.catalog.run_profile_evolution()
        self.assertGreater(len(pareto), 0)
        self.assertIsNotNone(self.catalog.best_evolved_profile)

    def tearDown(self):
        if Path("./test_bio_parameters.json").exists():
            Path("./test_bio_parameters.json").unlink()


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    async def demo():
        # Create catalog with evolution enabled
        catalog = create_bio_catalog(
            auto_reload=False,
            moea_population_size=10,
            moea_generations=3,
        )

        print("Organism types before evolution:", catalog.list_organism_types())

        # Recommend profile based on context
        context = {
            'carbon_intensity': 800,
            'helium_scarcity': 0.5,
            'temperature': 30,
            'humidity': 60,
            'required_efficiency': 0.9,
        }
        organism_type, params = await catalog.recommend_profile(context, exploration=True)
        print(f"Recommended: {organism_type}")
        print(f"Parameters: {params}")

        # Record outcome
        catalog.record_outcome(organism_type, performance=0.85, user_rating=0.9)

        # Run profile evolution manually (also runs in background)
        pareto = await catalog.run_profile_evolution()
        print(f"Evolved Pareto front size: {len(pareto)}")
        print("Organism types after evolution:", catalog.list_organism_types())

        stats = catalog.profile_optimizer.get_stats()
        print("Distillation stats:", stats)

        catalog.close()

    asyncio.run(demo())
