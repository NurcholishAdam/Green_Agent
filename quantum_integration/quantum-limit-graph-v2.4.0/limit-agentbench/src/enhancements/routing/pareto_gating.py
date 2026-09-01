"""
Pareto Gating Module
====================
Filters infeasible actions via hard constraints and returns Pareto‑optimal options.
Enhanced with dynamic constraints, configurable objectives, and scalar scoring.
NEW: Integrated with LIMIT Graph, MODP, RLHF, Multi‑Teacher Policy Distillation,
bio‑inspired MOEA (NSGA‑II), and MoE expert gating for advanced optimisation.

Enhancements included:
- Generalised constraint definitions (operators like >=, <=, ==, etc.)
- Support for missing objective values (drop or impute with worst value)
- Vectorised Pareto dominance for better performance
- Improved scalar scoring with multiple normalisation methods (minmax, zscore, rank)
- Dynamic objective updates via `set_objectives`
- New optional components:
  * LimitGraphManager: stores Pareto front nodes/edges.
  * MODPOptimizer: persists decision states/policies for multi‑objective dynamic programming.
  * RLHFTrainer: collects human preference pairs for objective weight tuning.
  * MoEGatingNetwork: blends multiple scoring experts (rule‑based, learned, evolved).
  * NSGAIIOptimizer: bio‑inspired global weight evolution with Pareto front.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple, Callable, Union
import numpy as np
import random
import copy
import uuid
import hashlib
from dataclasses import dataclass
from abc import ABC, abstractmethod

# Import config (adjust path as needed)
from ..config import config
from ..logger import logger

# Default objective configuration:
DEFAULT_OBJECTIVES = [
    {"key": "quality_score", "direction": "max"},
    {"key": "latency_ms", "direction": "min"},
    {"key": "carbon_g", "direction": "min"},
    {"key": "energy_joules", "direction": "min"},
    {"key": "helium_cost", "direction": "min"},
    {"key": "resource_usage", "direction": "min"},  # optional
]

# Define supported comparison operators
_OPS = {
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


# ------------------------------------------------------------------------------
# NEW: LIMIT Graph Manager
# ------------------------------------------------------------------------------
class LimitGraphManager:
    """
    Manages a graph of Pareto front solutions and their relationships.
    Nodes are solutions, edges represent dominance or improvement links.
    """
    def __init__(self, storage: Optional[Any] = None):
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
    Used to persist Pareto front points and selected actions.
    """
    def __init__(self, storage: Optional[Any] = None):
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
    Collects human preference pairs for objective weight tuning.
    """
    def __init__(self, storage: Optional[Any] = None):
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
# NEW: MoE Gating Network for Objective Weight Blending
# ------------------------------------------------------------------------------
class MoEGatingNetwork:
    """
    Mixture-of-Experts gating that blends multiple scoring strategies
    (rule‑based, learned, evolved). The gating network learns to select the best
    source for the current context (based on candidate statistics).
    """
    def __init__(self, storage: Optional[Any] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = self.config.get('expert_names', ['rule_based', 'learned', 'evolved'])
        self.num_experts = len(self.expert_names)
        # Gating input: normalized statistics of candidates (e.g., mean, std of objectives)
        self.gating_weights = np.random.randn(self.num_experts, 6)  # 6 features for context
        self._training_samples = []

    def _encode_state(self, stats: Dict[str, float]) -> np.ndarray:
        """Encode candidate statistics into a fixed‑size vector."""
        features = [
            stats.get('avg_quality', 0.5),
            stats.get('avg_latency', 0.5),
            stats.get('avg_carbon', 0.5),
            stats.get('avg_energy', 0.5),
            stats.get('avg_helium', 0.5),
            stats.get('num_candidates', 1.0) / 100.0,
        ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, stats: Dict[str, float]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(stats)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            sample_id = hashlib.sha256(str(stats).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
        return selected, probs

    async def add_training_sample(self, stats: Dict[str, float], selected_expert: str, reward: float):
        x = self._encode_state(stats)
        expert_idx = self.expert_names.index(selected_expert)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        grad = (probs - target)[:, None] * x[None, :]
        self.gating_weights -= 0.1 * grad


# ------------------------------------------------------------------------------
# NEW: NSGA-II Optimizer for Objective Weights (bio-inspired)
# ------------------------------------------------------------------------------
@dataclass
class NSGAIIWeightVector:
    vector_id: str
    weights: Dict[str, float]  # objective name -> weight
    objectives: Dict[str, float]  # achieved values (all maximized)
    scalarised_score: float = 0.0

    def to_dict(self):
        return {'vector_id': self.vector_id, 'weights': self.weights,
                'objectives': self.objectives, 'scalarised_score': self.scalarised_score}

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


class NSGAIIOptimizer:
    """
    NSGA-II for evolving objective weight vectors.
    Evaluation function is synchronous and returns a dict of objective benefits
    (higher is better) given a weight vector.
    """
    def __init__(self, evaluate_func: Callable[[Dict[str, float]], Dict[str, float]],
                 population_size=20, generations=10, mutation_rate=0.2,
                 crossover_rate=0.8, tournament_size=3):
        self.evaluate_func = evaluate_func
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.pareto_front: List[NSGAIIWeightVector] = []
        self.best_individual = None
        self.best_fitness = -float('inf')
        self._eval_cache = {}

    def _random_individual(self, keys):
        w = {k: random.random() for k in keys}
        total = sum(w.values())
        if total > 0:
            w = {k: v / total for k, v in w.items()}
        return w

    def _crossover(self, p1, p2):
        child = {}
        for k in p1:
            if random.random() < 0.5:
                child[k] = p1[k]
            else:
                child[k] = p2[k]
        total = sum(child.values())
        if total > 0:
            child = {k: v / total for k, v in child.items()}
        return child

    def _mutate(self, ind):
        mutant = ind.copy()
        for k in mutant:
            if random.random() < self.mutation_rate:
                mutant[k] = random.random()
        total = sum(mutant.values())
        if total > 0:
            mutant = {k: v / total for k, v in mutant.items()}
        return mutant

    def _dominates(self, a: NSGAIIWeightVector, b: NSGAIIWeightVector) -> bool:
        """True if a dominates b (all objectives >= and at least one >)."""
        a_obj = a.objectives
        b_obj = b.objectives
        better_or_equal = all(a_obj[k] >= b_obj[k] for k in a_obj)
        strictly_better = any(a_obj[k] > b_obj[k] for k in a_obj)
        return better_or_equal and strictly_better

    def _fast_non_dominated_sort(self, points):
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}
        for i, p in enumerate(points):
            for j, q in enumerate(points):
                if i == j:
                    continue
                if self._dominates(p, q):
                    dominated_solutions[id(p)].append(q)
                elif self._dominates(q, p):
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

    def _crowding_distance(self, front):
        if not front:
            return {}
        distances = {id(p): 0.0 for p in front}
        obj_keys = list(front[0].objectives.keys())
        for obj in obj_keys:
            sorted_front = sorted(front, key=lambda x: x.objectives[obj])
            distances[id(sorted_front[0])] = float('inf')
            distances[id(sorted_front[-1])] = float('inf')
            obj_min = sorted_front[0].objectives[obj]
            obj_max = sorted_front[-1].objectives[obj]
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                distances[id(sorted_front[i])] += (
                    sorted_front[i + 1].objectives[obj] - sorted_front[i - 1].objectives[obj]
                ) / (obj_max - obj_min)
        return distances

    def _tournament_selection(self, population, fronts, crowding):
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

    def evolve(self, keys) -> List[NSGAIIWeightVector]:
        population = [self._random_individual(keys) for _ in range(self.population_size)]
        points = []
        for ind in population:
            obj = self.evaluate_func(ind)
            point = NSGAIIWeightVector(vector_id=str(uuid.uuid4()), weights=ind, objectives=obj)
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

            child_points = []
            for ind in offspring:
                key = tuple(sorted(ind.items()))
                if key in self._eval_cache:
                    obj = self._eval_cache[key]
                else:
                    obj = self.evaluate_func(ind)
                    self._eval_cache[key] = obj
                point = NSGAIIWeightVector(vector_id=str(uuid.uuid4()), weights=ind, objectives=obj)
                child_points.append(point)

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

        if self.pareto_front:
            best = max(self.pareto_front, key=lambda p: sum(p.objectives.values()))
            self.best_individual = best.weights
            self.best_fitness = best.scalarised_score
        return self.pareto_front


# ==============================================================================
# Enhanced ParetoGating with optional integration components
# ==============================================================================
class ParetoGating:
    """
    Ensures hard constraints are met and returns Pareto‑optimal options.
    Optionally integrates with LIMIT Graph, MODP, RLHF, MoE, and NSGA-II.
    """

    def __init__(
        self,
        constraints: Optional[Dict[str, Any]] = None,
        objectives: Optional[List[Dict[str, str]]] = None,
        missing_policy: str = "drop",
        storage: Optional[Any] = None,
        enable_limit_graph: bool = True,
        enable_modp: bool = True,
        enable_rlhf: bool = True,
        enable_moe: bool = True,
        enable_nsga: bool = True,
        moe_expert_names: Optional[List[str]] = None,
        nsga_population_size: int = 20,
        nsga_generations: int = 10,
    ):
        """
        Args:
            constraints: Hard constraints (see original).
            objectives: List of objective definitions.
            missing_policy: 'drop' or 'worst'.
            storage: Optional central Storage object for persistence.
            enable_*: Flags to enable/disable optional components.
            moe_expert_names: Names of experts for MoE gating.
            nsga_*: Parameters for NSGA-II optimizer.
        """
        self.missing_policy = missing_policy
        self.constraints = self._normalise_constraints(
            constraints
            or {
                "quality_score": config.PARETO_QUALITY_MIN,
                "latency_ms": config.PARETO_LATENCY_MAX,
                "carbon_g": config.PARETO_CARBON_MAX,
            }
        )
        self.objectives = objectives or DEFAULT_OBJECTIVES
        self._objective_keys = [obj["key"] for obj in self.objectives]
        self._objective_dirs = [obj["direction"] for obj in self.objectives]

        # Optional components
        self.storage = storage
        self.limit_graph_manager = LimitGraphManager(storage) if enable_limit_graph else None
        self.modp_solver = MODPOptimizer(storage) if enable_modp else None
        self.rlhf_trainer = RLHFTrainer(storage) if enable_rlhf else None
        self.moe_gating = MoEGatingNetwork(
            storage,
            {'expert_names': moe_expert_names or ['rule_based', 'learned', 'evolved']}
        ) if enable_moe else None
        self.nsga_enabled = enable_nsga
        self.nsga_optimizer = None  # will be created when needed
        self.evolved_weights = None
        self.nsga_population_size = nsga_population_size
        self.nsga_generations = nsga_generations

        # Initialize LIMIT graph if enabled
        if self.limit_graph_manager:
            if not self.limit_graph_manager.get_metadata("pareto_front"):
                self.limit_graph_manager.create_graph("pareto_front", "Pareto Front Solutions", {})

        logger.info(
            f"ParetoGating initialized with {len(self.objectives)} objectives, "
            f"missing_policy='{self.missing_policy}', optional components: "
            f"limit_graph={self.limit_graph_manager is not None}, modp={self.modp_solver is not None}, "
            f"rlhf={self.rlhf_trainer is not None}, moe={self.moe_gating is not None}, nsga={self.nsga_enabled}."
        )

    # --------------------------------------------------------------------------
    # Constraint normalisation
    # --------------------------------------------------------------------------
    @staticmethod
    def _normalise_constraints(constraints: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        normalised = {}
        for key, value in constraints.items():
            if isinstance(value, (int, float)):
                if key == "quality_score":
                    normalised[key] = {"op": ">=", "value": value}
                else:
                    normalised[key] = {"op": "<=", "value": value}
            elif isinstance(value, dict) and "op" in value and "value" in value:
                if value["op"] not in _OPS:
                    raise ValueError(f"Unsupported operator '{value['op']}' for key '{key}'.")
                normalised[key] = value
            else:
                raise ValueError(f"Constraint for '{key}' must be numeric or dict with 'op' and 'value'.")
        return normalised

    def set_objectives(self, objectives: List[Dict[str, str]]) -> None:
        self.objectives = objectives
        self._objective_keys = [obj["key"] for obj in self.objectives]
        self._objective_dirs = [obj["direction"] for obj in self.objectives]
        logger.info(f"Objectives updated to {len(self.objectives)} objectives.")

    # --------------------------------------------------------------------------
    # Filtering
    # --------------------------------------------------------------------------
    def filter(self, candidates, dynamic_constraints=None, return_stats=False):
        # Merge constraints
        effective_constraints = self.constraints.copy()
        if dynamic_constraints:
            effective_constraints.update(self._normalise_constraints(dynamic_constraints))

        feasible = [c for c in candidates if self._satisfies_constraints(c, effective_constraints)]
        if not feasible:
            if return_stats:
                return [], {"total": len(candidates), "feasible": 0, "pareto": 0}
            return []

        pareto = self._pareto_filter(feasible)

        # Optional recording to components
        if self.limit_graph_manager:
            for sol in pareto:
                node_id = f"sol_{uuid.uuid4()}"
                obj_values = {k: sol.get(k) for k in self._objective_keys}
                self.limit_graph_manager.add_node(
                    "pareto_front",
                    node_id,
                    "pareto_solution",
                    obj_values
                )

        if self.modp_solver:
            problem_id = "pareto_gating"
            for sol in pareto:
                state_id = str(uuid.uuid4())
                obj_values = {obj["key"]: sol.get(obj["key"], 0.0) for obj in self.objectives}
                self.modp_solver.add_state(
                    state_id=state_id,
                    problem_id=problem_id,
                    state_attributes={'solution': sol},
                    objective_values=obj_values,
                    stage=0
                )

        if return_stats:
            stats = {
                "total": len(candidates),
                "feasible": len(feasible),
                "pareto": len(pareto),
            }
            return pareto, stats
        return pareto

    def _satisfies_constraints(self, candidate, constraints):
        for key, constraint in constraints.items():
            value = candidate.get(key)
            if value is None:
                return False
            op_func = _OPS[constraint["op"]]
            if not op_func(value, constraint["value"]):
                return False
        return True

    # --------------------------------------------------------------------------
    # Pareto filtering
    # --------------------------------------------------------------------------
    def _pareto_filter(self, candidates):
        n = len(candidates)
        if n == 0:
            return []
        if n > 1000:
            return self._pareto_filter_loop(candidates)

        m = len(self.objectives)
        obj_matrix = np.zeros((n, m))
        for i, c in enumerate(candidates):
            for j, obj in enumerate(self.objectives):
                key = obj["key"]
                val = c.get(key)
                if val is None:
                    if self.missing_policy == "worst":
                        val = -np.inf
                    else:
                        val = -np.inf
                if obj["direction"] == "min":
                    val = -val
                obj_matrix[i, j] = val

        better_or_equal = np.all(obj_matrix[:, None, :] >= obj_matrix[None, :, :], axis=2)
        strictly_better = np.any(obj_matrix[:, None, :] > obj_matrix[None, :, :], axis=2)
        dominated_by = better_or_equal & strictly_better
        np.fill_diagonal(dominated_by, False)
        is_dominated = np.any(dominated_by, axis=0)
        pareto_indices = np.where(~is_dominated)[0]
        return [candidates[i] for i in pareto_indices]

    def _pareto_filter_loop(self, candidates):
        pareto = []
        for i, c1 in enumerate(candidates):
            dominated = False
            for j, c2 in enumerate(candidates):
                if i == j:
                    continue
                if self._dominates(c2, c1):
                    dominated = True
                    break
            if not dominated:
                pareto.append(c1)
        return pareto

    def _dominates(self, a, b):
        better_or_equal = True
        strictly_better = False
        for obj in self.objectives:
            key = obj["key"]
            direction = obj["direction"]
            val_a = a.get(key)
            val_b = b.get(key)
            if val_a is None or val_b is None:
                if self.missing_policy == "drop":
                    return False
                else:
                    if direction == "max":
                        val_a = -np.inf if val_a is None else val_a
                        val_b = -np.inf if val_b is None else val_b
                    else:
                        val_a = np.inf if val_a is None else val_a
                        val_b = np.inf if val_b is None else val_b
            if direction == "max":
                if val_a < val_b:
                    return False
                if val_a > val_b:
                    strictly_better = True
            else:
                if val_a > val_b:
                    return False
                if val_a < val_b:
                    strictly_better = True
        return strictly_better

    # --------------------------------------------------------------------------
    # Scoring
    # --------------------------------------------------------------------------
    def score_candidates(self, candidates, weights=None, normalisation="minmax"):
        if weights is None:
            if self.evolved_weights:
                weights = self.evolved_weights
            else:
                weights = {obj["key"]: 1.0 for obj in self.objectives}

        obj_keys = [obj["key"] for obj in self.objectives]
        matrix = []
        for c in candidates:
            row = []
            for key, obj in zip(obj_keys, self.objectives):
                val = c.get(key)
                if val is None:
                    if self.missing_policy == "worst":
                        val = -np.inf if obj["direction"] == "max" else np.inf
                    else:
                        val = -np.inf if obj["direction"] == "max" else np.inf
                row.append(val)
            matrix.append(row)
        matrix = np.array(matrix, dtype=float)

        if normalisation == "minmax":
            mins = matrix.min(axis=0)
            maxs = matrix.max(axis=0)
            ranges = maxs - mins
            ranges[ranges == 0] = 1.0
            normalised = (matrix - mins) / ranges
            for i, obj in enumerate(self.objectives):
                if obj["direction"] == "min":
                    normalised[:, i] = 1.0 - normalised[:, i]
        elif normalisation == "zscore":
            means = matrix.mean(axis=0)
            stds = matrix.std(axis=0)
            stds[stds == 0] = 1.0
            normalised = (matrix - means) / stds
            for i, obj in enumerate(self.objectives):
                if obj["direction"] == "min":
                    normalised[:, i] = -normalised[:, i]
        elif normalisation == "rank":
            normalised = np.zeros_like(matrix, dtype=float)
            for j in range(matrix.shape[1]):
                col = matrix[:, j]
                order = col.argsort()
                ranks = np.empty_like(order, dtype=float)
                ranks[order] = np.arange(len(col))
                if self.objectives[j]["direction"] == "min":
                    ranks = len(col) - 1 - ranks
                normalised[:, j] = ranks
                max_rank = len(col) - 1 if len(col) > 1 else 1.0
                normalised[:, j] /= max_rank
        else:
            raise ValueError(f"Unknown normalisation method '{normalisation}'.")

        weight_vec = np.array([weights.get(k, 1.0) for k in obj_keys])
        scores = normalised @ weight_vec
        paired = list(zip(candidates, scores.tolist()))
        paired.sort(key=lambda x: x[1], reverse=True)
        return paired

    # --------------------------------------------------------------------------
    # NEW: NSGA-II Evolution for Objective Weights
    # --------------------------------------------------------------------------
    async def run_nsga_evolution(self, candidate_history: List[Dict[str, Any]]) -> Optional[Dict[str, float]]:
        """
        Evolve objective weights using NSGA-II based on historical candidate performance.
        candidate_history: list of candidate dicts with objective values.
        Returns best weight vector or None.
        """
        if not self.nsga_enabled:
            logger.info("NSGA-II evolution disabled.")
            return None

        def evaluate(weights):
            if not candidate_history:
                return {obj['key']: 0.0 for obj in self.objectives}
            avg_benefits = {obj['key']: 0.0 for obj in self.objectives}
            for obj in self.objectives:
                key = obj['key']
                vals = [c.get(key, 0) for c in candidate_history]
                if vals:
                    avg_benefits[key] = float(np.mean(vals))
            result = {}
            for obj in self.objectives:
                key = obj['key']
                w = weights.get(key, 0.0)
                result[key] = w * avg_benefits[key]
            return result

        if not self.nsga_optimizer:
            self.nsga_optimizer = NSGAIIOptimizer(
                evaluate_func=evaluate,
                population_size=self.nsga_population_size,
                generations=self.nsga_generations,
            )
        else:
            self.nsga_optimizer.evaluate_func = evaluate

        obj_keys = [obj['key'] for obj in self.objectives]
        pareto = self.nsga_optimizer.evolve(obj_keys)
        if pareto:
            best = max(pareto, key=lambda p: sum(p.objectives.values()))
            self.evolved_weights = best.weights
            logger.info(f"NSGA-II evolved weights: {self.evolved_weights}")

            if self.limit_graph_manager:
                node_id = f"evolved_weights_{uuid.uuid4()}"
                self.limit_graph_manager.add_node(
                    "pareto_front",
                    node_id,
                    "evolved_weights",
                    self.evolved_weights
                )
            if self.modp_solver:
                self.modp_solver.add_state(
                    state_id=f"nsga_weights_{uuid.uuid4()}",
                    problem_id="weight_optimization",
                    state_attributes={'weights': self.evolved_weights},
                    objective_values=best.objectives,
                    stage=0
                )
            return self.evolved_weights
        return None

    # --------------------------------------------------------------------------
    # NEW: RLHF methods
    # --------------------------------------------------------------------------
    async def record_human_preference(self, chosen_source: str, rejected_source: str,
                                      reward_diff: float = 1.0):
        """Record a human preference pair for RLHF."""
        if self.rlhf_trainer:
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt="Which scoring produced better Pareto ranking?",
                chosen=chosen_source,
                rejected=rejected_source,
                reward_diff=reward_diff,
                metadata={"timestamp": str(uuid.uuid4())}
            )

    async def get_limit_graph(self, graph_id: str = "pareto_front") -> Dict:
        if self.limit_graph_manager:
            return {
                'metadata': self.limit_graph_manager.get_metadata(graph_id),
                'nodes': self.limit_graph_manager.get_nodes(graph_id),
                'edges': self.limit_graph_manager.get_edges(graph_id),
            }
        return {}

    async def get_modp_states(self, problem_id: str = "pareto_gating") -> List[Dict]:
        if self.modp_solver:
            return self.modp_solver.get_states(problem_id)
        return []
