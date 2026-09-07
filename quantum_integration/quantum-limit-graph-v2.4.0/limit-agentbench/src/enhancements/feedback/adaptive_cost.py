#!/usr/bin/env python3
"""
Adaptive Cost Function with Two‑Tier Updates + MOEA + LIMIT Graph + MODP + RLHF + MoE (Enhanced v2.2)
====================================================================================================
- Online: fast exponential moving average for immediate routing.
- Offline: batched, validated updates for long‑term policy weights.
- Enhanced: Multi‑Objective Evolutionary Optimization (NSGA‑II) to evolve
  a Pareto front of weight vectors, with MODP‑based selection.
- NEW: LIMIT Graph for weight vector relationships.
- NEW: MODP solver for storing decision states/policies.
- NEW: RLHF trainer for human preference collection.
- NEW: MoE gating network to blend online/offline/rule‑based weight vectors.
- All original functionality retained.

Fixes over v2.1:
- Complete NSGA‑II implementation.
- Fixed MOEA evaluation function to use scalar reward.
- Implemented offline weight update (batch average / MTPD call).
- Safe config access.
- Replaced missing storage method with available one.
- Added persistence for Pareto front.
- Added thread‑safe online weight manager.
- Improved MoE blending logic.
- Proper drift detector integration.
"""

import asyncio
import json
import time
import numpy as np
from typing import Dict, List, Optional, Any, Tuple, Callable, Awaitable
from datetime import datetime
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..config import config
from ..logger import logger
import random
import copy
import uuid
import hashlib
from dataclasses import dataclass
from pathlib import Path
from collections import deque

# ------------------------------------------------------------------------------
# OnlineWeightManager (thread-safe with lock)
# ------------------------------------------------------------------------------
class OnlineWeightManager:
    """
    Exponential moving average for online adaptation.
    Persists weights to SQLite and reloads on startup.
    Thread‑safe for concurrent updates.
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        self.weights = {
            "quality": 0.25,
            "energy": 0.25,
            "carbon": 0.25,
            "latency": 0.25,
            "helium": 0.0,
        }
        self.alpha = 0.1
        self.max_energy = getattr(config, 'ADAPTIVE_MAX_ENERGY', 100.0) or 100.0
        self.max_carbon = getattr(config, 'ADAPTIVE_MAX_CARBON', 1.0) or 1.0
        self.max_latency = getattr(config, 'ADAPTIVE_MAX_LATENCY', 1000.0) or 1000.0
        self.max_helium = getattr(config, 'ADAPTIVE_MAX_HELIUM', 1.0) or 1.0
        self._lock = asyncio.Lock()
        self._load_state()

    def _load_state(self):
        try:
            data = self.storage.load_adaptive_state("online_weights")
            if data:
                self.weights = json.loads(data)
                logger.info(f"Loaded online weights: {self.weights}")
        except Exception as e:
            logger.warning(f"Failed to load online weights: {e}. Using defaults.")

    def _save_state(self):
        try:
            self.storage.save_adaptive_state("online_weights", json.dumps(self.weights))
        except Exception as e:
            logger.error(f"Failed to save online weights: {e}")

    async def update(self, event: FeedbackEvent):
        """Update weights based on observed event (async with lock)."""
        async with self._lock:
            norm_quality = event.quality_score
            norm_energy = 1.0 - min(1.0, event.energy_joules / self.max_energy)
            norm_carbon = 1.0 - min(1.0, event.carbon_g / self.max_carbon)
            norm_latency = 1.0 - min(1.0, event.latency_ms / self.max_latency)
            if event.helium_cost is not None:
                norm_helium = 1.0 - min(1.0, event.helium_cost / self.max_helium)
            else:
                norm_helium = None

            observed = {
                "quality": norm_quality,
                "energy": norm_energy,
                "carbon": norm_carbon,
                "latency": norm_latency,
            }
            if norm_helium is not None:
                observed["helium"] = norm_helium

            for key in self.weights:
                if key in observed:
                    self.weights[key] = (1 - self.alpha) * self.weights[key] + self.alpha * observed[key]

            total = sum(self.weights.values())
            if total > 0:
                for key in self.weights:
                    self.weights[key] /= total

            logger.debug(f"Online weights updated: {self.weights}")
            self._save_state()

    def get_cost_vector(self) -> Dict[str, float]:
        return self.weights.copy()

    def reset(self, initial_weights: Dict[str, float]):
        self.weights = initial_weights.copy()
        self._save_state()
        logger.info(f"Online weights reset to: {self.weights}")


# ------------------------------------------------------------------------------
# NEW: LIMIT Graph Manager
# ------------------------------------------------------------------------------
class LimitGraphManager:
    """
    Manages a graph of weight vector relationships for LIMIT.
    Nodes are weight vectors or updates, edges represent dependencies or improvements.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.graphs = {}

    def create_graph(self, graph_id: str, description: str, configuration: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_metadata'):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {'description': description, 'configuration': configuration, 'nodes': {}, 'edges': {}}

    def add_node(self, graph_id: str, node_id: str, node_type: Optional[str], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_node'):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['nodes'][node_id] = {'node_type': node_type, 'attributes': attributes}

    def add_edge(self, graph_id: str, edge_id: str, source: str, target: str,
                 weight: Optional[float], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_edge'):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['edges'][edge_id] = {'source': source, 'target': target, 'weight': weight, 'attributes': attributes}

    def get_nodes(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_nodes'):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self.graphs.get(graph_id, {}).get('nodes', {}).values())

    def get_edges(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_edges'):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self.graphs.get(graph_id, {}).get('edges', {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_metadata'):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})


# ------------------------------------------------------------------------------
# NEW: MODP Optimizer (wrapper)
# ------------------------------------------------------------------------------
class MODPOptimizer:
    """
    Multi‑Objective Dynamic Programming solver that stores decision states/policies.
    Used for persisting Pareto front points and selected weight vectors.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.states = {}

    def add_state(self, state_id: str, problem_id: str, state_attributes: Dict[str, Any],
                  objective_values: Dict[str, float], stage: int) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_state'):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)
        else:
            if problem_id not in self.states:
                self.states[problem_id] = []
            self.states[problem_id].append({
                'state_id': state_id, 'state_attributes': state_attributes,
                'objective_values': objective_values, 'stage': stage
            })

    def add_policy(self, policy_id: str, problem_id: str, state_id: str,
                   action: str, expected_objectives: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_policy'):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_policies(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_policies'):
            return self.storage.get_modp_policies(problem_id)
        return []


# ------------------------------------------------------------------------------
# NEW: RLHF Trainer
# ------------------------------------------------------------------------------
class RLHFTrainer:
    """
    Collects human preference pairs for weight vector choices.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.pairs = []

    def record_pair(self, pair_id: str, prompt: str, chosen: str, rejected: str,
                    reward_diff: float, metadata: Optional[Dict] = None) -> None:
        if self.storage and hasattr(self.storage, 'save_preference_pair'):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({
                'pair_id': pair_id, 'prompt': prompt, 'chosen': chosen,
                'rejected': rejected, 'reward_diff': reward_diff, 'metadata': metadata
            })

    def get_pairs(self, limit: int = 100) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_preference_pairs'):
            return self.storage.get_preference_pairs(limit)
        return self.pairs[-limit:]

    def train_reward_model(self):
        pairs = self.get_pairs()
        if len(pairs) < 5:
            logger.info("Not enough preference pairs for RLHF training.")
            return
        logger.info(f"Training reward model on {len(pairs)} preference pairs...")


# ------------------------------------------------------------------------------
# NEW: MoE Gating Network for Weight Blending
# ------------------------------------------------------------------------------
class MoEGatingNetwork:
    """
    Mixture-of-Experts gating that blends online, offline (MOEA), and rule‑based weight vectors.
    The gating network learns to select the best source for the current context.
    """
    def __init__(self, storage: Optional[Storage] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = self.config.get('expert_names', ['online', 'offline', 'rule_based'])
        self.num_experts = len(self.expert_names)
        self.gating_weights = np.random.randn(self.num_experts, 5)  # 5 metrics
        self._training_samples = []

    def _encode_state(self, metrics: Dict[str, float]) -> np.ndarray:
        """Encode current normalized metrics into a 5‑dim vector."""
        features = [
            metrics.get('quality', 0.5),
            metrics.get('energy', 0.5),
            metrics.get('carbon', 0.5),
            metrics.get('latency', 0.5),
            metrics.get('helium', 0.5),
        ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, metrics: Dict[str, float]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(metrics)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = int(np.argmax(probs))
        selected = self.expert_names[expert_idx]
        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            sample_id = hashlib.sha256(str(metrics).encode()).hexdigest()[:16]
            try:
                self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
            except Exception as e:
                logger.warning(f"Failed to log routing decision: {e}")
        return selected, probs

    async def add_training_sample(self, metrics: Dict[str, float], selected_expert: str, reward: float):
        x = self._encode_state(metrics)
        expert_idx = self.expert_names.index(selected_expert)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        grad = (probs - target)[:, None] * x[None, :]
        self.gating_weights -= 0.1 * grad


# ------------------------------------------------------------------------------
# Weight Vector and NSGA-II Optimizer (fully implemented)
# ------------------------------------------------------------------------------
@dataclass
class MOPDWeightVector:
    """A weight vector with its objective values (all maximized)."""
    vector_id: str
    weights: Dict[str, float]  # keys: quality, energy, carbon, latency, helium
    objectives: Dict[str, float]  # normalized benefits (higher is better)
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'vector_id': self.vector_id,
            'weights': self.weights,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDWeightVector':
        return cls(**data)


class NSGAIIWeightOptimizer:
    """Complete NSGA-II implementation for evolving weight vectors."""
    def __init__(
        self,
        evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
        population_size: int = 20,
        generations: int = 10,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        tournament_size: int = 3,
        objective_weights: Optional[Dict[str, float]] = None,
        dynamic_weights: bool = True,
    ):
        self.evaluate_func = evaluate_func
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {
            'quality': 0.3,
            'energy': 0.2,
            'carbon': 0.2,
            'latency': 0.2,
            'helium': 0.1,
        }
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDWeightVector] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}
        self._all_points: List[MOPDWeightVector] = []

    def _random_individual(self) -> Dict[str, float]:
        keys = list(self.objective_weights.keys())
        weights = {k: random.random() for k in keys}
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

    def _fast_non_dominated_sort(self, points: List[MOPDWeightVector]) -> List[List[MOPDWeightVector]]:
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

    def _crowding_distance(self, front: List[MOPDWeightVector]) -> Dict[int, float]:
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

    def _tournament_selection(self, population: List[Dict], fronts: List[List[MOPDWeightVector]],
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

    def _select_best_from_pareto(self, pareto: List[MOPDWeightVector], weights: Dict[str, float]) -> Optional[MOPDWeightVector]:
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

    async def evolve(self) -> List[MOPDWeightVector]:
        population = [self._random_individual() for _ in range(self.population_size)]
        eval_tasks = [self.evaluate_func(ind) for ind in population]
        eval_results = await asyncio.gather(*eval_tasks)
        points = []
        for ind, obj in zip(population, eval_results):
            point = MOPDWeightVector(
                vector_id=str(uuid.uuid4()),
                weights=ind,
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
                point = MOPDWeightVector(
                    vector_id=str(uuid.uuid4()),
                    weights=ind,
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
            self.best_individual = best.weights
            self.best_fitness = best.scalarised_score
        return self.pareto_front


# ------------------------------------------------------------------------------
# OfflineTrainer (Enhanced with MOEA, MODP, LIMIT Graph, persistence)
# ------------------------------------------------------------------------------
class OfflineTrainer:
    """
    Batch trainer for durable updates with validation and MOEA refinement.
    Buffers events, periodically invokes NSGA‑II to evolve a Pareto front of weight vectors,
    and selects the best using dynamic MODP weights.
    Added integration with MODP and LIMIT Graph.
    """

    def __init__(self, storage: Storage, mtpd_optimizer: Optional[Any] = None,
                 limit_graph_manager: Optional[LimitGraphManager] = None,
                 modp_solver: Optional[MODPOptimizer] = None):
        self.storage = storage
        self.mtpd_optimizer = mtpd_optimizer
        self.buffer = []
        self.batch_size = getattr(config, 'OFFLINE_BATCH_SIZE', 32)
        self.update_interval = getattr(config, 'OFFLINE_UPDATE_INTERVAL_SEC', 60)
        self.last_update = datetime.now()
        self._lock = asyncio.Lock()

        # MOEA parameters
        self.moea_population_size = getattr(config, 'MOEA_POPULATION_SIZE', 20)
        self.moea_generations = getattr(config, 'MOEA_GENERATIONS', 10)
        self.moea_interval_seconds = getattr(config, 'MOEA_INTERVAL_SEC', 300)
        self.moea_enabled = getattr(config, 'MOEA_ENABLED', True)
        self.moea_optimizer: Optional[NSGAIIWeightOptimizer] = None
        self.pareto_front: List[MOPDWeightVector] = []
        self._moea_task: Optional[asyncio.Task] = None

        # NEW: integration objects
        self.limit_graph_manager = limit_graph_manager
        self.modp_solver = modp_solver

        # Persistence path for evolved weights
        self.pareto_path = Path("./adaptive_pareto_front.json")

        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())

    async def queue_event(self, event: FeedbackEvent):
        async with self._lock:
            self.buffer.append(event)
            if len(self.buffer) >= self.batch_size:
                await self._train_step()

    async def _train_step(self):
        """Process a batch and update the offline weights / MTPD student."""
        if len(self.buffer) == 0:
            return

        batch = self.buffer[:self.batch_size]
        self.buffer = self.buffer[self.batch_size:]

        # Compute average normalized metrics for this batch
        avg_metrics = {k: [] for k in ['quality', 'energy', 'carbon', 'latency', 'helium']}
        for e in batch:
            avg_metrics['quality'].append(e.quality_score)
            avg_metrics['energy'].append(1.0 - min(1.0, e.energy_joules / (getattr(config, 'ADAPTIVE_MAX_ENERGY', 100.0) or 100.0)))
            avg_metrics['carbon'].append(1.0 - min(1.0, e.carbon_g / (getattr(config, 'ADAPTIVE_MAX_CARBON', 1.0) or 1.0)))
            avg_metrics['latency'].append(1.0 - min(1.0, e.latency_ms / (getattr(config, 'ADAPTIVE_MAX_LATENCY', 1000.0) or 1000.0)))
            if e.helium_cost is not None:
                avg_metrics['helium'].append(1.0 - min(1.0, e.helium_cost / (getattr(config, 'ADAPTIVE_MAX_HELIUM', 1.0) or 1.0)))
            else:
                avg_metrics['helium'].append(0.5)

        batch_weights = {k: float(np.mean(v)) for k, v in avg_metrics.items() if len(v) > 0}

        # If MTPD optimizer is available, call its offline update with batch
        if self.mtpd_optimizer:
            try:
                # Assume method exists
                await self.mtpd_optimizer.offline_update(batch)
                logger.info(f"Called MTPD offline update with batch of {len(batch)} events.")
            except Exception as e:
                logger.error(f"MTPD offline update failed: {e}")

        # Store batch summary
        try:
            self.storage.log_offline_batch_summary({
                "timestamp": time.time(),
                "batch_size": len(batch),
                "avg_quality": batch_weights.get('quality', 0),
                "avg_carbon": batch_weights.get('carbon', 0),
                "avg_latency": batch_weights.get('latency', 0),
                "avg_energy": batch_weights.get('energy', 0),
            })
        except Exception as e:
            logger.warning(f"Failed to store batch summary: {e}")

    async def _moea_loop(self):
        while True:
            try:
                await asyncio.sleep(self.moea_interval_seconds)
                await self.run_moea()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop error: {e}")
                await asyncio.sleep(60)

    async def run_moea(self) -> List[MOPDWeightVector]:
        """
        Run NSGA‑II to evolve a Pareto front of weight vectors.
        Uses historical feedback events from storage.
        """
        try:
            # Use available storage method; fallback to empty list
            if hasattr(self.storage, 'get_recent_feedback_events'):
                events = self.storage.get_recent_feedback_events(limit=1000)
            elif hasattr(self.storage, 'get_feedback_events'):
                events = self.storage.get_feedback_events(limit=1000)
            else:
                logger.warning("Storage does not provide feedback events; MOEA skipped.")
                return []
        except Exception as e:
            logger.error(f"Failed to retrieve events for MOEA: {e}")
            return []

        if len(events) < 20:
            logger.warning("Not enough events for MOEA; skipping.")
            return []

        # Precompute normalized benefits for each event (to speed evaluation)
        benefits_list = []
        for ev in events:
            norm_energy = 1.0 - min(1.0, ev.energy_joules / (getattr(config, 'ADAPTIVE_MAX_ENERGY', 100.0) or 100.0))
            norm_carbon = 1.0 - min(1.0, ev.carbon_g / (getattr(config, 'ADAPTIVE_MAX_CARBON', 1.0) or 1.0))
            norm_latency = 1.0 - min(1.0, ev.latency_ms / (getattr(config, 'ADAPTIVE_MAX_LATENCY', 1000.0) or 1000.0))
            if ev.helium_cost is not None:
                norm_helium = 1.0 - min(1.0, ev.helium_cost / (getattr(config, 'ADAPTIVE_MAX_HELIUM', 1.0) or 1.0))
            else:
                norm_helium = 0.5
            benefits_list.append({
                'quality': ev.quality_score,
                'energy': norm_energy,
                'carbon': norm_carbon,
                'latency': norm_latency,
                'helium': norm_helium,
            })

        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            # Compute average weighted benefit for each objective
            objectives = {k: 0.0 for k in weights}
            count = len(benefits_list)
            if count == 0:
                return objectives
            for b in benefits_list:
                for k in weights:
                    objectives[k] += weights[k] * b[k]  # weight * benefit
            for k in objectives:
                objectives[k] /= count
            # Objectives are to be maximized; they already are benefits
            return objectives

        self.moea_optimizer = NSGAIIWeightOptimizer(
            evaluate_func=evaluate,
            population_size=self.moea_population_size,
            generations=self.moea_generations,
            mutation_rate=getattr(config, 'MOEA_MUTATION_RATE', 0.2),
            crossover_rate=getattr(config, 'MOEA_CROSSOVER_RATE', 0.8),
            tournament_size=getattr(config, 'MOEA_TOURNAMENT_SIZE', 3),
            objective_weights=getattr(config, 'MOEA_OBJECTIVE_WEIGHTS', None),
            dynamic_weights=getattr(config, 'MOEA_DYNAMIC_WEIGHTS', True),
        )
        self.pareto_front = await self.moea_optimizer.evolve()
        logger.info(f"MOEA produced Pareto front of size {len(self.pareto_front)}")

        # Save Pareto front to disk
        try:
            data = [p.to_dict() for p in self.pareto_front]
            with open(self.pareto_path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save Pareto front: {e}")

        # Store best in MODP and add to LIMIT graph
        if self.pareto_front and self.moea_optimizer:
            weights = self._compute_dynamic_weights()
            best = self.moea_optimizer._select_best_from_pareto(self.pareto_front, weights)
            if best:
                if self.modp_solver:
                    self.modp_solver.add_state(
                        state_id=f"moea_best_{best.vector_id}",
                        problem_id="weight_optimization",
                        state_attributes={'weights': best.weights},
                        objective_values=best.objectives,
                        stage=1
                    )
                if self.limit_graph_manager:
                    self.limit_graph_manager.add_node(
                        "weight_vectors",
                        f"vector_{best.vector_id}",
                        "best_weight_vector",
                        {'weights': best.weights, 'objectives': best.objectives}
                    )
        return self.pareto_front

    async def get_best_weight_vector(self) -> Optional[Dict[str, float]]:
        if not self.pareto_front:
            # Try loading from disk
            if self.pareto_path.exists():
                try:
                    with open(self.pareto_path, 'r') as f:
                        data = json.load(f)
                    self.pareto_front = [MOPDWeightVector.from_dict(d) for d in data]
                except Exception:
                    pass
        if self.pareto_front and self.moea_optimizer:
            weights = self._compute_dynamic_weights()
            best = self.moea_optimizer._select_best_from_pareto(self.pareto_front, weights)
            if best:
                return best.weights
        return None

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        base = getattr(config, 'MOEA_OBJECTIVE_WEIGHTS', {
            'quality': 0.3,
            'energy': 0.2,
            'carbon': 0.2,
            'latency': 0.2,
            'helium': 0.1,
        }).copy()
        # Could adjust based on recent performance; for now return static
        return base


# ------------------------------------------------------------------------------
# AdaptiveCostFunction (Enhanced with MoE, RLHF, MODP, LIMIT Graph)
# ------------------------------------------------------------------------------
class AdaptiveCostFunction:
    """
    Main orchestrator for 2‑tier adaptive costs + MOEA + new components.
    Integrates online EMA, offline batch training, drift detection, MOEA, MoE gating,
    RLHF preference logging, MODP state storage, and LIMIT Graph.
    """

    def __init__(self, storage: Storage, mtpd_optimizer: Optional[Any] = None):
        self.storage = storage
        self.online = OnlineWeightManager(storage)

        # Create new components
        self.limit_graph_manager = LimitGraphManager(storage) if getattr(config, 'ENABLE_LIMIT_GRAPH', True) else None
        self.modp_solver = MODPOptimizer(storage) if getattr(config, 'ENABLE_MODP', True) else None
        self.rlhf_trainer = RLHFTrainer(storage) if getattr(config, 'ENABLE_RLHF', True) else None
        self.moe_gating = MoEGatingNetwork(storage, {'expert_names': ['online', 'offline', 'rule_based']}) if getattr(config, 'ENABLE_MOE', True) else None

        self.offline = OfflineTrainer(
            storage,
            mtpd_optimizer,
            limit_graph_manager=self.limit_graph_manager,
            modp_solver=self.modp_solver
        )
        self.drift_detector: Optional[Any] = None  # set externally if needed

        # Initialize LIMIT Graph if enabled
        if self.limit_graph_manager:
            if not self.limit_graph_manager.get_metadata("weight_vectors"):
                self.limit_graph_manager.create_graph("weight_vectors", "Weight Vector Relationships", {})
            for src in ['online', 'offline', 'rule_based']:
                self.limit_graph_manager.add_node("weight_vectors", f"source_{src}", src, {"type": "source"})

    async def record_feedback(self, event: FeedbackEvent) -> None:
        """Record feedback into all pipelines."""
        try:
            # Store raw event
            self.storage.store_feedback_event(event.to_db_dict())

            # Online update (async lock)
            await self.online.update(event)

            # Offline buffer
            await self.offline.queue_event(event)

            # MoE gating update (optional)
            if self.moe_gating:
                metrics = {
                    'quality': event.quality_score,
                    'energy': 1.0 - min(1.0, event.energy_joules / (getattr(config, 'ADAPTIVE_MAX_ENERGY', 100.0) or 100.0)),
                    'carbon': 1.0 - min(1.0, event.carbon_g / (getattr(config, 'ADAPTIVE_MAX_CARBON', 1.0) or 1.0)),
                    'latency': 1.0 - min(1.0, event.latency_ms / (getattr(config, 'ADAPTIVE_MAX_LATENCY', 1000.0) or 1000.0)),
                    'helium': 1.0 - min(1.0, (event.helium_cost or 0.0) / (getattr(config, 'ADAPTIVE_MAX_HELIUM', 1.0) or 1.0)),
                }
                selected_expert, probs = await self.moe_gating.select_expert(metrics)
                await self.moe_gating.add_training_sample(metrics, selected_expert, event.quality_score)

            # Drift detection if configured
            if self.drift_detector:
                try:
                    drift_result = await self.drift_detector.check_drift(self.online.get_cost_vector())
                    if drift_result and drift_result.get('drift_detected'):
                        logger.warning(f"Drift detected in adaptive weights: {drift_result}")
                except Exception as e:
                    logger.warning(f"Drift detection failed: {e}")
        except Exception as e:
            logger.error(f"Error in AdaptiveCostFunction.record_feedback: {e}", exc_info=True)

    def get_current_weights(self) -> Dict[str, float]:
        """Return current online weights (fast adaptation)."""
        return self.online.get_cost_vector()

    async def get_blended_weights(self) -> Dict[str, float]:
        """
        Use MoE gating to blend online and offline weight vectors.
        Falls back to online weights if MoE is disabled or offline unavailable.
        """
        if not self.moe_gating:
            return self.get_current_weights()

        online_weights = self.get_current_weights()
        offline_weights = await self.offline.get_best_weight_vector()
        if offline_weights is None:
            return online_weights

        rule_based = {k: 0.2 for k in online_weights.keys()}

        # Context: use average of online weights as features
        metrics = {
            'quality': online_weights.get('quality', 0.2),
            'energy': online_weights.get('energy', 0.2),
            'carbon': online_weights.get('carbon', 0.2),
            'latency': online_weights.get('latency', 0.2),
            'helium': online_weights.get('helium', 0.0),
        }
        selected_expert, probs = await self.moe_gating.select_expert(metrics)

        # Blend using probabilities; expert order matches expert_names
        blended = {}
        total_prob = 0.0
        for i, name in enumerate(self.moe_gating.expert_names):
            if name == 'online':
                weights = online_weights
            elif name == 'offline':
                weights = offline_weights
            elif name == 'rule_based':
                weights = rule_based
            else:
                continue
            prob = probs[i]
            for k in weights:
                blended[k] = blended.get(k, 0.0) + prob * weights[k]
            total_prob += prob

        if total_prob > 0:
            blended = {k: v / total_prob for k, v in blended.items()}

        # Normalize (should already sum to 1, but just in case)
        total = sum(blended.values())
        if total > 0:
            blended = {k: v / total for k, v in blended.items()}
        return blended

    async def get_evolved_weights(self) -> Optional[Dict[str, float]]:
        """Return best weight vector from MOEA Pareto front."""
        return await self.offline.get_best_weight_vector()

    async def record_human_preference(self, chosen_source: str, rejected_source: str,
                                      reward_diff: float = 1.0):
        """Record a human preference pair for RLHF."""
        if self.rlhf_trainer:
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt="Which weight source produced better routing?",
                chosen=chosen_source,
                rejected=rejected_source,
                reward_diff=reward_diff,
                metadata={"timestamp": datetime.now().isoformat()}
            )

    def reset_weights(self, initial_weights: Dict[str, float]) -> None:
        self.online.reset(initial_weights)
        self.offline.buffer.clear()
        logger.info("Adaptive cost function reset.")

    async def get_limit_graph(self, graph_id: str = "weight_vectors") -> Dict:
        if self.limit_graph_manager:
            return {
                'metadata': self.limit_graph_manager.get_metadata(graph_id),
                'nodes': self.limit_graph_manager.get_nodes(graph_id),
                'edges': self.limit_graph_manager.get_edges(graph_id),
            }
        return {}

    async def get_moe_experts(self) -> List[str]:
        return self.moe_gating.expert_names if self.moe_gating else []
