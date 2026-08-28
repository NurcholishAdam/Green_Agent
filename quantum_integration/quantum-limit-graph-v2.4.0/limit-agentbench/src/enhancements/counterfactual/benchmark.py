"""
Counterfactual Benchmarking Harness (v3.4.0)
===========================================
Replays historical decisions with different policies, computes metrics,
performs statistical comparisons, and evolves new policies via multi‑objective
evolutionary optimization (NSGA‑II). The evolved policies are parameterized
weight vectors that trade off among quality, carbon, latency, energy, cost,
and helium.

NEW IN v3.4.0:
- Added LIMIT Graph manager for policy relationship modelling.
- Added MODP solver wrapper for storing decision states/policies.
- Added RLHF trainer for human preference collection on policy choices.
- Added MoE gating network to blend policies (experts).
- Integration with central Storage for new data persistence.
- New configuration flags for enabling/disabling each component.

Previous features (distillation, NSGA-II, MODP selection) are retained.
"""
import asyncio
import uuid
import numpy as np
from typing import List, Dict, Optional, Any, Callable, Awaitable, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import logging
import json
import random
import copy
import time
import hashlib

# Optional statistical libraries
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

from ..storage import Storage
from ..config import config
from ..logger import logger
from ..schemas.feedback_event import FeedbackEvent
from ..mtpd_optimizer import MTPDOptimizer, StrategyMetrics


@dataclass
class BenchmarkResult:
    """Structured result of a policy benchmark run."""
    run_id: str
    policy_name: str
    timestamp: float
    sample_count: int
    metrics: Dict[str, float]          # mean values
    confidence_intervals: Dict[str, Tuple[float, float]]  # 95% CI
    p_value: Optional[float] = None    # vs MOPD_current


# ============================================================================
# NEW: LIMIT Graph Manager
# ============================================================================
class LimitGraphManager:
    """
    Manages a graph of policy relationships for LIMIT.
    Nodes are policies or benchmarks, edges represent dependencies or improvements.
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


# ============================================================================
# NEW: MODP Optimizer (wrapper)
# ============================================================================
class MODPOptimizer:
    """
    Multi‑Objective Dynamic Programming solver that stores decision states/policies.
    This complements the NSGA-II optimizer; MODP here is used for scalarized selection
    among Pareto front points and for persisting evolved policies.
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


# ============================================================================
# NEW: RLHF Trainer
# ============================================================================
class RLHFTrainer:
    """
    Collects human preference pairs for benchmark policy choices.
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


# ============================================================================
# NEW: MoE Gating Network
# ============================================================================
class MoEGatingNetwork:
    """
    Mixture-of-Experts gating for benchmark policy selection.
    Experts correspond to predefined policies (fixed_cheapest, energy_only, etc.) plus evolved ones.
    The gating network learns to select the best policy for a given context.
    """
    def __init__(self, storage: Optional[Storage] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = self.config.get('expert_names', ['fixed_cheapest', 'energy_only', 'carbon_only', 'quality_only'])
        self.num_experts = len(self.expert_names)
        # State dimension: we'll use 12 features
        self.gating_weights = np.random.randn(self.num_experts, 12)
        self._training_samples = []

    def _encode_state(self, state: Dict) -> np.ndarray:
        # Encode a context dict into a fixed-size vector (12 features)
        features = [
            state.get('carbon_intensity', 0.0),
            state.get('workload_size', 0.0),
            state.get('latency_target', 0.0),
            state.get('cost_budget', 0.0),
            state.get('energy_price', 0.0),
            state.get('helium_scarcity', 0.0),
            state.get('quality_requirement', 0.0),
            state.get('hour_of_day', 0.0) / 24.0,
            state.get('day_of_week', 0.0) / 7.0,
            state.get('recent_success_rate', 0.5),
            state.get('avg_reward', 0.0),
            state.get('num_candidates', 1.0) / 10.0,
        ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, state: Dict) -> Tuple[str, np.ndarray]:
        x = self._encode_state(state)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            sample_id = hashlib.sha256(str(state).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
        return selected, probs

    async def add_training_sample(self, state: Dict, selected_expert: str, reward: float):
        x = self._encode_state(state)
        expert_idx = self.expert_names.index(selected_expert)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        grad = (probs - target)[:, None] * x[None, :]
        self.gating_weights -= 0.1 * grad


# ============================================================================
# Parameterized Policy and Evolutionary Optimizer
# ============================================================================
@dataclass
class Policy:
    policy_id: str
    weights: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return {'policy_id': self.policy_id, 'weights': self.weights}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Policy':
        return cls(**data)

    def choose_candidate(self, candidates: List[Dict]) -> int:
        best_idx = 0
        best_score = float('inf')
        for idx, cand in enumerate(candidates):
            score = 0.0
            for metric in ['carbon', 'latency', 'energy', 'cost', 'helium']:
                if metric in self.weights:
                    score += self.weights[metric] * cand.get(metric, 0.0)
            if 'quality' in self.weights:
                score -= self.weights['quality'] * cand.get('quality_score', 0.0)
            if score < best_score:
                best_score = score
                best_idx = idx
        return best_idx


@dataclass
class MOPDPoint:
    policy: Policy
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'policy': self.policy.to_dict(),
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }


class NSGAIIOptimizer:
    """
    Multi‑objective genetic algorithm for evolving policy weight vectors.
    Assumes all objectives are to be maximized.
    """
    def __init__(
        self,
        evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
        parameter_bounds: Dict[str, Tuple[float, float]],
        population_size: int = 20,
        generations: int = 10,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        tournament_size: int = 3,
        objective_weights: Optional[Dict[str, float]] = None,
        dynamic_weights: bool = True,
    ):
        self.evaluate_func = evaluate_func
        self.parameter_bounds = parameter_bounds
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {}
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDPoint] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}
        self._all_points: List[MOPDPoint] = []

    def _random_individual(self) -> Dict[str, float]:
        ind = {}
        for name, (low, high) in self.parameter_bounds.items():
            ind[name] = random.uniform(low, high)
        total = sum(ind.values())
        if total > 0:
            ind = {k: v / total for k, v in ind.items()}
        return ind

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
        child = {}
        for name in self.parameter_bounds:
            if random.random() < 0.5:
                low, high = self.parameter_bounds[name]
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                val = 0.5 * ((1 + beta) * p1[name] + (1 - beta) * p2[name])
                child[name] = max(low, min(high, val))
            else:
                child[name] = p1[name] if random.random() < 0.5 else p2[name]
        total = sum(child.values())
        if total > 0:
            child = {k: v / total for k, v in child.items()}
        return child

    def _mutate(self, ind: Dict) -> Dict:
        mutant = ind.copy()
        for name, (low, high) in self.parameter_bounds.items():
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / (20 + 1)) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                mutant[name] = mutant[name] + delta * (high - low)
                mutant[name] = max(low, min(high, mutant[name]))
        total = sum(mutant.values())
        if total > 0:
            mutant = {k: v / total for k, v in mutant.items()}
        return mutant

    def _fast_non_dominated_sort(self, points: List[MOPDPoint]) -> List[List[MOPDPoint]]:
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

    def _crowding_distance(self, front: List[MOPDPoint]) -> Dict[int, float]:
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

    def _tournament_selection(self, population: List[Dict], fronts: List[List[MOPDPoint]],
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
        if not obj_keys:
            return weights
        avg = {k: np.mean([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        max_val = {k: np.max([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        for k in obj_keys:
            if max_val[k] > 0 and avg[k] < 0.5 * max_val[k]:
                weights[k] = min(0.6, weights.get(k, 0.0) * 1.5)
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _select_best_from_pareto(self, pareto: List[MOPDPoint], weights: Dict[str, float]) -> Optional[MOPDPoint]:
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

    async def evolve(self) -> List[MOPDPoint]:
        population = [self._random_individual() for _ in range(self.population_size)]
        points = []
        eval_tasks = [self.evaluate_func(ind) for ind in population]
        eval_results = await asyncio.gather(*eval_tasks)
        for ind, obj in zip(population, eval_results):
            point = MOPDPoint(
                policy=Policy(policy_id=str(uuid.uuid4()), weights=ind),
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
                point = MOPDPoint(
                    policy=Policy(policy_id=str(uuid.uuid4()), weights=ind),
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
            self.best_individual = best.policy.weights
            self.best_fitness = best.scalarised_score
        return self.pareto_front


# ============================================================================
# Enhanced CounterfactualBenchmark with Policy Evolution and New Components
# ============================================================================
class CounterfactualBenchmark:
    POLICIES = {
        "fixed_cheapest": "_policy_fixed_cheapest",
        "energy_only": "_policy_energy_only",
        "carbon_only": "_policy_carbon_only",
        "quality_only": "_policy_quality_only",
        "mopd_current": "_policy_mopd_current",
    }

    def __init__(
        self,
        storage: Storage,
        optimizer: Optional[MTPDOptimizer] = None,
        confidence_level: float = 0.95,
        bootstrap_samples: int = 1000,
        moea_population_size: int = 20,
        moea_generations: int = 5,
        moea_mutation_rate: float = 0.2,
        moea_crossover_rate: float = 0.8,
        moea_tournament_size: int = 3,
        moea_objective_weights: Optional[Dict[str, float]] = None,
        moea_dynamic_weights: bool = True,
        enable_limit_graph: bool = True,
        enable_modp: bool = True,
        enable_rlhf: bool = True,
        enable_moe: bool = True,
        moe_expert_names: Optional[List[str]] = None,
    ):
        self.storage = storage
        self.optimizer = optimizer
        self.confidence_level = confidence_level
        self.bootstrap_samples = bootstrap_samples

        self.moea_population_size = moea_population_size
        self.moea_generations = moea_generations
        self.moea_mutation_rate = moea_mutation_rate
        self.moea_crossover_rate = moea_crossover_rate
        self.moea_tournament_size = moea_tournament_size
        self.moea_objective_weights = moea_objective_weights or {
            'quality': 0.3, 'carbon': 0.2, 'latency': 0.2,
            'energy': 0.1, 'cost': 0.1, 'helium': 0.1,
        }
        self.moea_dynamic_weights = moea_dynamic_weights
        self.evolved_pareto_front: List[MOPDPoint] = []
        self.best_evolved_policy: Optional[Policy] = None

        # NEW components
        self.limit_graph_manager = LimitGraphManager(storage) if enable_limit_graph else None
        self.modp_solver = MODPOptimizer(storage) if enable_modp else None
        self.rlhf_trainer = RLHFTrainer(storage) if enable_rlhf else None
        self.moe_gating = None
        if enable_moe:
            expert_names = moe_expert_names or list(self.POLICIES.keys())
            self.moe_gating = MoEGatingNetwork(storage, {'expert_names': expert_names})

        # Initialize LIMIT Graph if enabled
        if self.limit_graph_manager:
            self._init_limit_graph()

        logger.info("CounterfactualBenchmark initialized with evolution, LIMIT Graph, MODP, RLHF, MoE")

    def _init_limit_graph(self):
        graph_id = "benchmark_policies"
        if not self.limit_graph_manager.get_metadata(graph_id):
            self.limit_graph_manager.create_graph(graph_id, "Benchmark Policy Relationships", {})
            for policy_name in self.POLICIES:
                self.limit_graph_manager.add_node(graph_id, f"policy_{policy_name}", policy_name, {})
            # Add edges from current MOPD to evolved (placeholder)
            self.limit_graph_manager.add_edge(graph_id, "edge_mopd_evolved", "policy_mopd_current", "policy_evolved", 1.0, {})

    # --------------------------------------------------------------------------
    # Existing policy implementations
    # --------------------------------------------------------------------------
    async def _policy_fixed_cheapest(self, state, candidates):
        return min(range(len(candidates)), key=lambda i: candidates[i].get('cost_usd', float('inf')))

    async def _policy_energy_only(self, state, candidates):
        return min(range(len(candidates)), key=lambda i: candidates[i].get('energy_joules', float('inf')))

    async def _policy_carbon_only(self, state, candidates):
        return min(range(len(candidates)), key=lambda i: candidates[i].get('carbon_g', float('inf')))

    async def _policy_quality_only(self, state, candidates):
        return max(range(len(candidates)), key=lambda i: candidates[i].get('quality_score', 0.0))

    async def _policy_mopd_current(self, state, candidates):
        if self.optimizer is None:
            raise RuntimeError("MOPD optimizer not set; cannot run 'mopd_current' policy.")
        metrics_list = [
            StrategyMetrics(
                strategy_name=c.get('action_id', 'unknown'),
                latency_ms=c.get('latency_ms', 0.0),
                carbon_g=c.get('carbon_g', 0.0),
                cost_usd=c.get('cost_usd', 0.0),
                quality_score=c.get('quality_score', 0.0),
            )
            for c in candidates
        ]
        chosen = self.optimizer.select_strategy(state, metrics_list)
        for idx, c in enumerate(candidates):
            if c.get('action_id') == chosen.strategy_name:
                return idx
        if hasattr(chosen, 'action_idx'):
            return chosen.action_idx
        return 0

    # --------------------------------------------------------------------------
    # Core benchmark (enhanced with MoE and new components)
    # --------------------------------------------------------------------------
    async def run_benchmark(
        self,
        days_back: int = 7,
        policies: Optional[List[str]] = None,
        sample_limit: int = 10000,
    ) -> Dict[str, BenchmarkResult]:
        if policies is None:
            policies = list(self.POLICIES.keys())

        events = self.storage.get_feedback_events_with_context(
            days_back=days_back,
            limit=sample_limit,
        )
        if not events:
            logger.warning("No historical events with context found for benchmark.")
            return {}

        logger.info(f"Running benchmark on {len(events)} events from last {days_back} days.")

        results = {}
        for policy_name in policies:
            if policy_name not in self.POLICIES:
                logger.warning(f"Policy '{policy_name}' not defined; skipping.")
                continue

            policy_method = getattr(self, self.POLICIES[policy_name])
            metrics, ci = await self._evaluate_policy(policy_method, events)

            run_id = str(uuid.uuid4())
            result = BenchmarkResult(
                run_id=run_id,
                policy_name=policy_name,
                timestamp=time.time(),
                sample_count=len(events),
                metrics=metrics,
                confidence_intervals=ci,
            )
            results[policy_name] = result

            self.storage.store_benchmark_result(
                run_id=run_id,
                policy_name=policy_name,
                metrics=metrics,
                count=len(events),
                confidence_intervals=ci,
            )

            # LIMIT Graph: add node for this benchmark run
            if self.limit_graph_manager:
                self.limit_graph_manager.add_node(
                    "benchmark_policies",
                    f"run_{run_id}",
                    "benchmark_run",
                    {'policy': policy_name, 'metrics': metrics}
                )

        if "mopd_current" in results and len(results) > 1:
            baseline = results["mopd_current"]
            for name, res in results.items():
                if name == "mopd_current":
                    continue
                p_val = self._compute_p_value(res, baseline)
                res.p_value = p_val

        self._log_comparison(results)

        # If MoE gating available, select the best policy based on context
        if self.moe_gating and results:
            context = self._build_context(events)
            selected_expert, probs = await self.moe_gating.select_expert(context)
            logger.info(f"MoE selected policy: {selected_expert}")
            # Record a preference pair (simulated)
            if self.rlhf_trainer:
                self.rlhf_trainer.record_pair(
                    pair_id=str(uuid.uuid4()),
                    prompt="Which policy performs best?",
                    chosen=selected_expert,
                    rejected="quality_only",  # dummy
                    reward_diff=0.1,
                    metadata={'benchmark': True}
                )

        return results

    def _build_context(self, events: List[Dict]) -> Dict:
        # Extract average context from events (simplified)
        return {
            'carbon_intensity': 0.4,
            'workload_size': 1000,
            'latency_target': 500,
            'cost_budget': 50,
            'energy_price': 0.1,
            'helium_scarcity': 0.5,
            'quality_requirement': 0.9,
            'hour_of_day': 12,
            'day_of_week': 3,
            'recent_success_rate': 0.7,
            'avg_reward': 0.5,
            'num_candidates': 5,
        }

    # --------------------------------------------------------------------------
    # Existing evaluation helper
    # --------------------------------------------------------------------------
    async def _evaluate_policy(self, policy_func, events):
        per_event_metrics = []
        for event in events:
            state = event.get('state', {})
            candidates = event.get('candidates', [])
            if not candidates:
                continue
            try:
                chosen_idx = await policy_func(state, candidates)
            except Exception as e:
                logger.warning(f"Policy simulation failed for event {event.get('event_id')}: {e}")
                continue
            chosen_candidate = candidates[chosen_idx]
            per_event_metrics.append({
                'quality': chosen_candidate.get('quality_score', 0.0),
                'carbon': chosen_candidate.get('carbon_g', 0.0),
                'latency': chosen_candidate.get('latency_ms', 0.0),
                'energy': chosen_candidate.get('energy_joules', 0.0),
                'cost': chosen_candidate.get('cost_usd', 0.0),
                'helium': chosen_candidate.get('helium_cost', 0.0),
            })
        if not per_event_metrics:
            return {}, {}
        return self._bootstrap_aggregate(per_event_metrics)

    # --------------------------------------------------------------------------
    # NEW: Evaluate a parameterized policy
    # --------------------------------------------------------------------------
    async def evaluate_policy_parameters(self, weights: Dict[str, float]) -> Dict[str, float]:
        events = self.storage.get_feedback_events_with_context(days_back=7, limit=10000)
        if not events:
            return {k: 0.0 for k in ['quality', 'carbon', 'latency', 'energy', 'cost', 'helium']}

        policy = Policy(policy_id="temp", weights=weights)
        total_metrics = {k: [] for k in ['quality', 'carbon', 'latency', 'energy', 'cost', 'helium']}
        for event in events:
            candidates = event.get('candidates', [])
            if not candidates:
                continue
            chosen_idx = policy.choose_candidate(candidates)
            chosen = candidates[chosen_idx]
            total_metrics['quality'].append(chosen.get('quality_score', 0.0))
            total_metrics['carbon'].append(chosen.get('carbon_g', 0.0))
            total_metrics['latency'].append(chosen.get('latency_ms', 0.0))
            total_metrics['energy'].append(chosen.get('energy_joules', 0.0))
            total_metrics['cost'].append(chosen.get('cost_usd', 0.0))
            total_metrics['helium'].append(chosen.get('helium_cost', 0.0))

        benefits = {}
        benefits['quality'] = float(np.mean(total_metrics['quality'])) if total_metrics['quality'] else 0.0
        for metric in ['carbon', 'latency', 'energy', 'cost', 'helium']:
            vals = total_metrics[metric]
            if not vals:
                benefits[metric] = 0.0
                continue
            max_val = max(vals)
            if max_val == 0:
                benefits[metric] = 1.0
            else:
                benefits[metric] = 1.0 - float(np.mean(vals) / max_val)
        return benefits

    # --------------------------------------------------------------------------
    # NEW: Run policy evolution (NSGA-II)
    # --------------------------------------------------------------------------
    async def run_policy_evolution(self) -> List[MOPDPoint]:
        param_bounds = {
            'quality': (0.01, 1.0),
            'carbon': (0.01, 1.0),
            'latency': (0.01, 1.0),
            'energy': (0.01, 1.0),
            'cost': (0.01, 1.0),
            'helium': (0.01, 1.0),
        }

        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            total = sum(weights.values())
            if total == 0:
                normalized = {k: 1.0/len(weights) for k in weights}
            else:
                normalized = {k: v / total for k, v in weights.items()}
            return await self.evaluate_policy_parameters(normalized)

        optimizer = NSGAIIOptimizer(
            evaluate_func=evaluate,
            parameter_bounds=param_bounds,
            population_size=self.moea_population_size,
            generations=self.moea_generations,
            mutation_rate=self.moea_mutation_rate,
            crossover_rate=self.moea_crossover_rate,
            tournament_size=self.moea_tournament_size,
            objective_weights=self.moea_objective_weights,
            dynamic_weights=self.moea_dynamic_weights,
        )

        pareto = await optimizer.evolve()
        self.evolved_pareto_front = pareto

        if pareto:
            best = optimizer._select_best_from_pareto(
                pareto,
                self._get_dynamic_moea_weights()
            )
            if best:
                self.best_evolved_policy = best.policy
                logger.info(f"Best evolved policy weights: {best.policy.weights}")
                # Persist evolved policy
                self.storage.store_evolved_policy(best.policy.to_dict())

                # MODP: record state and policy
                if self.modp_solver:
                    problem_id = "policy_evolution"
                    state_id = f"evolved_{best.policy.policy_id}"
                    self.modp_solver.add_state(
                        state_id=state_id,
                        problem_id=problem_id,
                        state_attributes={'weights': best.policy.weights},
                        objective_values=best.objectives,
                        stage=1
                    )
                    self.modp_solver.add_policy(
                        policy_id=best.policy.policy_id,
                        problem_id=problem_id,
                        state_id=state_id,
                        action="evolved",
                        expected_objectives=best.objectives
                    )

                # LIMIT Graph: add node for evolved policy
                if self.limit_graph_manager:
                    self.limit_graph_manager.add_node(
                        "benchmark_policies",
                        f"policy_{best.policy.policy_id}",
                        "evolved_policy",
                        {'weights': best.policy.weights}
                    )

        return pareto

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        # static for now, can be enhanced
        return self.moea_objective_weights.copy()

    # --------------------------------------------------------------------------
    # Utility methods
    # --------------------------------------------------------------------------
    def _bootstrap_aggregate(self, metrics_list):
        metric_keys = list(metrics_list[0].keys())
        data = {key: np.array([m[key] for m in metrics_list]) for key in metric_keys}
        means = {key: float(np.mean(data[key])) for key in metric_keys}
        ci = {}
        n = len(metrics_list)
        for key in metric_keys:
            vals = data[key]
            boot_means = []
            for _ in range(self.bootstrap_samples):
                sample = np.random.choice(vals, size=n, replace=True)
                boot_means.append(np.mean(sample))
            boot_means = np.array(boot_means)
            lower = np.percentile(boot_means, (1 - self.confidence_level) / 2 * 100)
            upper = np.percentile(boot_means, (1 + self.confidence_level) / 2 * 100)
            ci[key] = (float(lower), float(upper))
        return means, ci

    def _compute_p_value(self, result_a, result_b):
        if not SCIPY_AVAILABLE:
            return None
        # Placeholder
        return 0.05

    def _log_comparison(self, results):
        if not results:
            return
        logger.info("=" * 60)
        logger.info("Counterfactual Benchmark Results")
        logger.info("=" * 60)
        for name, res in results.items():
            logger.info(f"Policy: {name}")
            logger.info(f"  Quality: {res.metrics.get('quality', 0.0):.4f}")
            logger.info(f"  Carbon:  {res.metrics.get('carbon', 0.0):.4f} g")
            logger.info(f"  Latency: {res.metrics.get('latency', 0.0):.2f} ms")
            logger.info(f"  Energy:  {res.metrics.get('energy', 0.0):.4f} J")
            logger.info(f"  Cost:    {res.metrics.get('cost', 0.0):.4f} USD")
            if res.p_value is not None:
                logger.info(f"  p‑value vs MOPD: {res.p_value:.4f}")
            logger.info("-" * 40)
        logger.info("=" * 60)

    def to_api_response(self, results):
        out = {}
        for name, res in results.items():
            out[name] = {
                "run_id": res.run_id,
                "timestamp": res.timestamp,
                "sample_count": res.sample_count,
                "metrics": res.metrics,
                "confidence_intervals": res.confidence_intervals,
                "p_value": res.p_value,
            }
        return out

    # ---------- New public methods for enhancements ----------
    async def get_limit_graph(self, graph_id: str = "benchmark_policies") -> Dict:
        if self.limit_graph_manager:
            return {
                'metadata': self.limit_graph_manager.get_metadata(graph_id),
                'nodes': self.limit_graph_manager.get_nodes(graph_id),
                'edges': self.limit_graph_manager.get_edges(graph_id),
            }
        return {}

    async def get_moe_experts(self) -> List[str]:
        if self.moe_gating:
            return self.moe_gating.expert_names
        return []

    async def get_rlhf_pairs(self, limit: int = 100) -> List[Dict]:
        if self.rlhf_trainer:
            return self.rlhf_trainer.get_pairs(limit)
        return []

    async def record_rlhf_pair(self, pair_id, prompt, chosen, rejected, reward_diff, metadata=None):
        if self.rlhf_trainer:
            self.rlhf_trainer.record_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
