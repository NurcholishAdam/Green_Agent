"""
Evolutionary policy search for FlexGen policies.
Uses a simple genetic algorithm to evolve candidate policies,
evaluated via the cost model or real execution.
"""

import random
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import asdict
import numpy as np

from .flexgen_policy import FlexGenPolicy
from .flexgen_cost_model import FlexGenCostModel
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..pareto_gating import ParetoGating
from ..logger import logger


class BioPolicySearch:
    def __init__(
        self,
        node: NodeDescriptor,
        workload: WorkloadDescriptor,
        cost_model: FlexGenCostModel,
        population_size: int = 50,
        generations: int = 10,
        mutation_rate: float = 0.2,
    ):
        self.node = node
        self.workload = workload
        self.cost_model = cost_model
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.population: List[FlexGenPolicy] = []
        self.pareto = ParetoGating(
            objectives=[
                {"key": "latency_ms", "direction": "min"},
                {"key": "energy_joules", "direction": "min"},
                {"key": "carbon_g", "direction": "min"},
            ]
        )

    def _random_policy(self) -> FlexGenPolicy:
        return FlexGenPolicy(
            gpu_batch_size=random.choice([1, 2, 4, 8]),
            block_size=random.choice([8, 16, 32, 64]),
            weight_device=random.choice(["gpu", "cpu", "disk"]),
            activation_device=random.choice(["gpu", "cpu"]),
            kv_cache_device=random.choice(["gpu", "cpu", "disk"]),
            weight_bits=random.choice([4, 8, 16]),
            kv_cache_bits=random.choice([4, 8, 16]),
            cpu_attention=random.random() < 0.3,
            overlap_io_compute=random.random() < 0.7,
        )

    def _evaluate(self, policy: FlexGenPolicy) -> Dict[str, float]:
        est = self.cost_model.estimate(policy, self.node, self.workload)
        return {
            "latency_ms": est.total_latency_ms,
            "energy_joules": est.total_energy_joules,
            "carbon_g": est.total_carbon_g,
            "gpu_memory_gb": est.peak_gpu_memory_gb,
            "policy": policy,
        }

    def _mutate(self, policy: FlexGenPolicy) -> FlexGenPolicy:
        new_policy = FlexGenPolicy(**policy.to_dict())
        if random.random() < self.mutation_rate:
            new_policy.gpu_batch_size = random.choice([1, 2, 4, 8])
        if random.random() < self.mutation_rate:
            new_policy.block_size = random.choice([8, 16, 32, 64])
        if random.random() < self.mutation_rate:
            new_policy.weight_device = random.choice(["gpu", "cpu", "disk"])
        if random.random() < self.mutation_rate:
            new_policy.kv_cache_device = random.choice(["gpu", "cpu", "disk"])
        if random.random() < self.mutation_rate:
            new_policy.weight_bits = random.choice([4, 8, 16])
        if random.random() < self.mutation_rate:
            new_policy.cpu_attention = not new_policy.cpu_attention
        return new_policy

    def run(self) -> List[FlexGenPolicy]:
        """Run evolutionary search and return Pareto-optimal policies."""
        # Initialize population
        self.population = [self._random_policy() for _ in range(self.population_size)]

        for gen in range(self.generations):
            # Evaluate all policies
            evaluated = [self._evaluate(p) for p in self.population]
            # Keep non-dominated as parents
            pareto_set = self.pareto.filter(evaluated)
            if not pareto_set:
                pareto_set = evaluated[:10]
            parents = [item["policy"] for item in pareto_set]

            # Generate offspring via mutation and crossover
            offspring = []
            while len(offspring) < self.population_size:
                parent = random.choice(parents)
                child = self._mutate(parent)
                offspring.append(child)

            self.population = offspring
            logger.info(f"Generation {gen}: population {len(self.population)}")

        # Final evaluation
        final_evaluated = [self._evaluate(p) for p in self.population]
        final_pareto = self.pareto.filter(final_evaluated)
        return [item["policy"] for item in final_pareto] if final_pareto else self.population[:5]
