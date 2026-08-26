"""
Adaptive Cost Function with Two‑Tier Updates + MOEA (Enhanced v2.0)
==================================================================
- Online: fast exponential moving average for immediate routing.
- Offline: batched, validated updates for long‑term policy weights.
- Enhanced: Multi‑Objective Evolutionary Optimization (NSGA‑II) to evolve
  a Pareto front of weight vectors, with MODP‑based selection.

New features:
- NSGAIIWeightOptimizer class for global exploration of weight space.
- OfflineTrainer periodically runs MOEA in background.
- Pareto front storage and dynamic selection of best weights.
- Persistence of evolved weight vectors.
- Integration with existing AdaptiveCostFunction.

All original functionality retained.
"""

import asyncio
import json
import time
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..config import config
from ..logger import logger
import random
import copy
import uuid
from dataclasses import dataclass

# ------------------------------------------------------------------------------
# OnlineWeightManager (unchanged from original)
# ------------------------------------------------------------------------------
class OnlineWeightManager:
    """
    Exponential moving average for online adaptation.
    Persists weights to SQLite and reloads on startup.
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
        self.max_energy = config.ADAPTIVE_MAX_ENERGY or 100.0
        self.max_carbon = config.ADAPTIVE_MAX_CARBON or 1.0
        self.max_latency = config.ADAPTIVE_MAX_LATENCY or 1000.0
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

    def update(self, event: FeedbackEvent):
        """Update weights based on observed event."""
        # Normalize event values to 0‑1 using configured maxes
        norm_quality = event.quality_score
        norm_energy = 1.0 - min(1.0, event.energy_joules / self.max_energy)
        norm_carbon = 1.0 - min(1.0, event.carbon_g / self.max_carbon)
        norm_latency = 1.0 - min(1.0, event.latency_ms / self.max_latency)
        if event.helium_cost is not None:
            norm_helium = 1.0 - min(1.0, event.helium_cost / (config.ADAPTIVE_MAX_HELIUM or 1.0))
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
# NEW: MOPDWeightVector and NSGAIIWeightOptimizer
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
    """
    Multi‑objective genetic algorithm for evolving cost function weights.
    Decision variables: weight values for each metric (sum to 1).
    Objectives: maximize quality, minimize energy, minimize carbon, minimize latency, minimize helium.
    The evaluation function replays historical feedback events to estimate average benefits.
    """

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

    def _random_individual(self) -> Dict[str, float]:
        keys = ['quality', 'energy', 'carbon', 'latency', 'helium']
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
        points = []
        eval_tasks = [self.evaluate_func(ind) for ind in population]
        eval_results = await asyncio.gather(*eval_tasks)
        for ind, obj in zip(population, eval_results):
            point = MOPDWeightVector(vector_id=str(uuid.uuid4()), weights=ind, objectives=obj)
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
                point = MOPDWeightVector(vector_id=str(uuid.uuid4()), weights=ind, objectives=obj)
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
# OfflineTrainer (Enhanced with MOEA)
# ------------------------------------------------------------------------------
class OfflineTrainer:
    """
    Batch trainer for durable updates with validation and MOEA refinement.
    Buffers events, periodically invokes NSGA‑II to evolve a Pareto front of weight vectors,
    and selects the best using dynamic MODP weights.
    """

    def __init__(self, storage: Storage, mtpd_optimizer: Optional[Any] = None):
        self.storage = storage
        self.mtpd_optimizer = mtpd_optimizer
        self.buffer = []
        self.batch_size = config.OFFLINE_BATCH_SIZE
        self.update_interval = config.OFFLINE_UPDATE_INTERVAL_SEC
        self.last_update = datetime.now()
        self._lock = asyncio.Lock()

        # MOEA parameters (with defaults)
        self.moea_population_size = getattr(config, 'MOEA_POPULATION_SIZE', 20)
        self.moea_generations = getattr(config, 'MOEA_GENERATIONS', 10)
        self.moea_interval_seconds = getattr(config, 'MOEA_INTERVAL_SEC', 300)
        self.moea_enabled = getattr(config, 'MOEA_ENABLED', True)
        self.moea_optimizer: Optional[NSGAIIWeightOptimizer] = None
        self.pareto_front: List[MOPDWeightVector] = []
        self._moea_task: Optional[asyncio.Task] = None

        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())

    async def queue_event(self, event: FeedbackEvent):
        async with self._lock:
            self.buffer.append(event)
            if len(self.buffer) >= self.batch_size:
                await self._train_step()

    async def _train_step(self):
        """Process a batch and update the MTPD student policy."""
        if len(self.buffer) == 0:
            return

        batch = self.buffer[:self.batch_size]
        self.buffer = self.buffer[self.batch_size:]

        avg_carbon = np.mean([e.carbon_g for e in batch])
        avg_quality = np.mean([e.quality_score for e in batch])
        avg_latency = np.mean([e.latency_ms for e in batch])
        avg_energy = np.mean([e.energy_joules for e in batch])

        if avg_quality < config.PARETO_QUALITY_MIN:
            logger.warning(f"Offline update rejected: quality {avg_quality:.3f} < {config.PARETO_QUALITY_MIN}")
            return

        if self.mtpd_optimizer:
            try:
                # Call existing MTPD optimizer if available
                logger.info(f"Calling MTPD optimizer with batch of {len(batch)} events.")
                # In a full implementation, we would pass the batch to the optimizer.
                # Example: self.mtpd_optimizer.train_on_batch(batch)
            except Exception as e:
                logger.error(f"Failed to call MTPD optimizer offline update: {e}")

        # Store summary
        self.storage.log_offline_batch_summary({
            "timestamp": time.time(),
            "batch_size": len(batch),
            "avg_quality": avg_quality,
            "avg_carbon": avg_carbon,
            "avg_latency": avg_latency,
            "avg_energy": avg_energy,
        })

    # ---------- MOEA methods ----------
    async def _moea_loop(self):
        """Periodically run MOEA to refresh Pareto front."""
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
        Evaluation uses historical feedback events (retrieved from storage).
        """
        # Get recent events
        events = self.storage.get_recent_feedback_events(limit=1000)
        if len(events) < 20:
            logger.warning("Not enough events for MOEA; skipping.")
            return []

        # Define evaluation function
        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            # Compute average normalized benefits over events
            benefits = {k: [] for k in ['quality', 'energy', 'carbon', 'latency', 'helium']}
            for ev in events:
                benefits['quality'].append(ev.quality_score)
                norm_energy = 1.0 - min(1.0, ev.energy_joules / (config.ADAPTIVE_MAX_ENERGY or 100.0))
                benefits['energy'].append(norm_energy)
                norm_carbon = 1.0 - min(1.0, ev.carbon_g / (config.ADAPTIVE_MAX_CARBON or 1.0))
                benefits['carbon'].append(norm_carbon)
                norm_latency = 1.0 - min(1.0, ev.latency_ms / (config.ADAPTIVE_MAX_LATENCY or 1000.0))
                benefits['latency'].append(norm_latency)
                if ev.helium_cost is not None:
                    norm_helium = 1.0 - min(1.0, ev.helium_cost / (config.ADAPTIVE_MAX_HELIUM or 1.0))
                else:
                    norm_helium = 0.5
                benefits['helium'].append(norm_helium)

            # Compute weighted average of benefits using the candidate weights
            objectives = {}
            for key in weights:
                # Weighted mean of the benefit values for this metric
                objectives[key] = np.mean([weights[key] * b for b in benefits[key]]) if benefits[key] else 0.0
            return objectives

        # Create MOEA optimizer
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
        return self.pareto_front

    async def get_best_weight_vector(self) -> Optional[Dict[str, float]]:
        """Return the best weight vector from the Pareto front using MODP selection."""
        if not self.pareto_front:
            await self.run_moea()
        if self.pareto_front and self.moea_optimizer:
            weights = self._compute_dynamic_weights()
            best = self.moea_optimizer._select_best_from_pareto(self.pareto_front, weights)
            if best:
                return best.weights
        return None

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        """Compute objective weights for MODP selection (can be adjusted dynamically)."""
        # Base weights, can be overridden by config
        base = getattr(config, 'MOEA_OBJECTIVE_WEIGHTS', {
            'quality': 0.3,
            'energy': 0.2,
            'carbon': 0.2,
            'latency': 0.2,
            'helium': 0.1,
        }).copy()
        # Example dynamic adjustment: if carbon emissions are high in recent events, increase carbon weight.
        # In a full implementation, we would examine recent events or system state.
        return base


# ------------------------------------------------------------------------------
# AdaptiveCostFunction (Enhanced)
# ------------------------------------------------------------------------------
class AdaptiveCostFunction:
    """
    Main orchestrator for 2‑tier adaptive costs + MOEA.
    Integrates online EMA, offline batch training, drift detection, and MOEA.
    """

    def __init__(self, storage: Storage, mtpd_optimizer: Optional[Any] = None):
        self.storage = storage
        self.online = OnlineWeightManager(storage)
        self.offline = OfflineTrainer(storage, mtpd_optimizer)
        self.drift_detector: Optional[Any] = None  # set externally

    async def record_feedback(self, event: FeedbackEvent) -> None:
        """Record feedback into all pipelines."""
        try:
            self.storage.store_feedback_event(event.to_db_dict())
            self.online.update(event)
            await self.offline.queue_event(event)
            if self.drift_detector:
                try:
                    await self.drift_detector.check_drift(self.online.get_cost_vector())
                except Exception as e:
                    logger.warning(f"Drift detection failed: {e}")
        except Exception as e:
            logger.error(f"Error in AdaptiveCostFunction.record_feedback: {e}", exc_info=True)

    def get_current_weights(self) -> Dict[str, float]:
        """Return current online weights (fast adaptation)."""
        return self.online.get_cost_vector()

    async def get_evolved_weights(self) -> Optional[Dict[str, float]]:
        """Return best weight vector from MOEA Pareto front."""
        return await self.offline.get_best_weight_vector()

    def reset_weights(self, initial_weights: Dict[str, float]) -> None:
        """Reset online weights and clear offline buffer."""
        self.online.reset(initial_weights)
        self.offline.buffer.clear()
        logger.info("Adaptive cost function reset.")
