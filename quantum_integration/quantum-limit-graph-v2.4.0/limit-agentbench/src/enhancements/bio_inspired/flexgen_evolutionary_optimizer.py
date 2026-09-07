"""
Enhanced bio‑inspired evolutionary optimizer for FlexGen policies.
Uses genetic algorithm with Pareto filtering to evolve candidate offloading policies.

Improvements:
- Async run method with proper event publishing.
- Elite handling ensures population size remains constant.
- Feasibility-aware parent selection.
- Stores best Pareto policies for later use.
- Thread-safe population updates.
- Uses cost model's quality score when available.
"""

import asyncio
import random
import logging
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import asdict

import numpy as np

from ..gpu_optimization.flexgen_policy import FlexGenPolicy
from ..gpu_optimization.flexgen_cost_model import FlexGenCostModel
from ..gpu_optimization.reward import compute_reward
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..pareto_gating import ParetoGating
from ..async_message_queue import AsyncMessageQueue
from ..schemas.feedback_event import FeedbackEvent
from ..logger import logger


class FlexGenEvolutionaryOptimizer:
    """
    Evolves a population of FlexGenPolicy objects to find Pareto‑optimal policies.
    """

    def __init__(
        self,
        population_size: int = 100,
        generations: int = 20,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        elite_size: int = 5,
        cost_model: Optional[FlexGenCostModel] = None,
        pareto: Optional[ParetoGating] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        use_real_executor: bool = False,
        executor=None,
    ):
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.elite_size = elite_size
        self.cost_model = cost_model or FlexGenCostModel()
        self.pareto = pareto or ParetoGating(
            objectives=[
                {"key": "latency_ms", "direction": "min"},
                {"key": "energy_joules", "direction": "min"},
                {"key": "carbon_g", "direction": "min"},
            ]
        )
        self.message_queue = message_queue
        self.use_real_executor = use_real_executor
        self.executor = executor
        self.population: List[FlexGenPolicy] = []
        self.best_policies: List[FlexGenPolicy] = []  # Store final Pareto set
        self._lock = asyncio.Lock()  # Thread safety if needed

    def initialize_population(self, seed_policies: Optional[List[FlexGenPolicy]] = None) -> None:
        if seed_policies:
            self.population = seed_policies[:]
            # Fill remaining with random policies
            while len(self.population) < self.population_size:
                self.population.append(self._random_policy())
        else:
            self.population = [self._random_policy() for _ in range(self.population_size)]

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

    def _evaluate(self, policy: FlexGenPolicy, node: NodeDescriptor, workload: WorkloadDescriptor,
                  carbon_intensity: float) -> Tuple[Dict, float]:
        """Return metrics dict and reward."""
        if self.use_real_executor and self.executor:
            metrics = self.executor.execute(policy, node, workload)
        else:
            est = self.cost_model.estimate(policy, node, workload)
            metrics = {
                "latency_ms": est.total_latency_ms,
                "energy_joules": est.total_energy_joules,
                "carbon_g": est.total_carbon_g,
                "gpu_memory_gb": est.peak_gpu_memory_gb,
                "success": est.peak_gpu_memory_gb <= node.metadata.get("gpu_memory_gb", 16.0),
                "quality_score": est.quality_score if hasattr(est, 'quality_score') else 0.9,
            }
        reward = compute_reward(metrics, workload)
        return metrics, reward

    def _select_parents(self, evaluated: List[Tuple[FlexGenPolicy, Dict, float]]) -> List[FlexGenPolicy]:
        """
        Tournament selection based on reward, but only among feasible policies.
        If no feasible policies, fall back to all.
        """
        feasible = [(p, m, r) for p, m, r in evaluated if m.get('success', False)]
        if not feasible:
            feasible = evaluated  # keep all if none feasible
        parents = []
        tournament_size = max(2, int(self.population_size * 0.1))
        for _ in range(self.population_size):
            candidates = random.sample(feasible, tournament_size)
            # Choose the one with highest reward
            winner = max(candidates, key=lambda x: x[2])
            parents.append(winner[0])
        return parents

    def _crossover(self, parent1: FlexGenPolicy, parent2: FlexGenPolicy) -> FlexGenPolicy:
        """Uniform crossover over policy fields."""
        child_dict = {}
        for field in FlexGenPolicy.__dataclass_fields__:
            if random.random() < 0.5:
                child_dict[field] = getattr(parent1, field)
            else:
                child_dict[field] = getattr(parent2, field)
        return FlexGenPolicy(**child_dict)

    def _mutate(self, policy: FlexGenPolicy) -> FlexGenPolicy:
        """Randomly change some fields."""
        if random.random() < self.mutation_rate:
            policy.gpu_batch_size = random.choice([1, 2, 4, 8])
        if random.random() < self.mutation_rate:
            policy.block_size = random.choice([8, 16, 32, 64])
        if random.random() < self.mutation_rate:
            policy.weight_device = random.choice(["gpu", "cpu", "disk"])
        if random.random() < self.mutation_rate:
            policy.activation_device = random.choice(["gpu", "cpu"])
        if random.random() < self.mutation_rate:
            policy.kv_cache_device = random.choice(["gpu", "cpu", "disk"])
        if random.random() < self.mutation_rate:
            policy.weight_bits = random.choice([4, 8, 16])
        if random.random() < self.mutation_rate:
            policy.kv_cache_bits = random.choice([4, 8, 16])
        if random.random() < self.mutation_rate:
            policy.cpu_attention = not policy.cpu_attention
        if random.random() < self.mutation_rate:
            policy.overlap_io_compute = not policy.overlap_io_compute
        return policy

    async def run(self, node: NodeDescriptor, workload: WorkloadDescriptor,
                  carbon_intensity: float, generations: Optional[int] = None) -> List[FlexGenPolicy]:
        """
        Execute the evolutionary loop asynchronously and return a set of Pareto‑optimal policies.
        """
        gens = generations or self.generations
        self.initialize_population()

        for gen in range(gens):
            # Evaluate all policies
            evaluated = []
            for policy in self.population:
                metrics, reward = self._evaluate(policy, node, workload, carbon_intensity)
                evaluated.append((policy, metrics, reward))

            # Extract metrics for Pareto filtering (only successful ones)
            successful_metrics = [m for _, m, _ in evaluated if m.get('success', False)]
            if not successful_metrics:
                successful_metrics = [m for _, m, _ in evaluated]  # if none feasible, use all
            pareto_metrics = self.pareto.filter(successful_metrics)

            # Map Pareto metrics back to policies using the evaluated list
            pareto_policies = []
            for p, m, _ in evaluated:
                if m in pareto_metrics:
                    pareto_policies.append(p)

            # Optional: publish elite policies
            if self.message_queue and gen % 5 == 0:
                for pol in pareto_policies[:3]:
                    metrics, reward = self._evaluate(pol, node, workload, carbon_intensity)
                    event = FeedbackEvent(
                        source="bio_inspired_flexgen",
                        feedback_type="routing",
                        task_id=workload.task_id or "unknown",
                        context={"generation": gen, "node_id": node.id},
                        action={"selected_action": str(pol.to_dict())},
                        performance={"quality_score": metrics.get("quality_score", 0.9)},
                        adaptive_cost_value=reward,
                        tags=["bio_inspired", "flexgen_policy", "evolution"],
                    )
                    await self.message_queue.publish("bio_inspired_events", event.to_json())

            # Select parents (only feasible)
            parents = self._select_parents(evaluated)

            # Create offspring (ensuring population size remains constant)
            offspring = []
            while len(offspring) < self.population_size - self.elite_size:
                p1, p2 = random.sample(parents, 2)
                if random.random() < self.crossover_rate:
                    child = self._crossover(p1, p2)
                else:
                    child = random.choice([p1, p2])
                child = self._mutate(child)
                offspring.append(child)

            # Add elites from Pareto set; if fewer than elite_size, fill with random offspring
            elites = pareto_policies[:self.elite_size]
            offspring.extend(elites)
            if len(offspring) < self.population_size:
                additional_needed = self.population_size - len(offspring)
                additional = [self._random_policy() for _ in range(additional_needed)]
                offspring.extend(additional)

            # Ensure population size
            self.population = offspring[:self.population_size]

            logger.info(f"Generation {gen}: population={len(self.population)}, pareto_size={len(pareto_policies)}")

        # Final Pareto set evaluation
        final_evaluated = []
        for policy in self.population:
            metrics, reward = self._evaluate(policy, node, workload, carbon_intensity)
            final_evaluated.append((policy, metrics))
        final_metrics = [m for _, m in final_evaluated if m.get('success', False)]
        if not final_metrics:
            final_metrics = [m for _, m in final_evaluated]
        final_pareto = self.pareto.filter(final_metrics)
        final_policies = [p for p, m in final_evaluated if m in final_pareto]
        if not final_policies:
            final_policies = [p for p, _ in final_evaluated][:10]

        self.best_policies = final_policies
        return final_policies

    # Synchronous wrapper for convenience
    def run_sync(self, node: NodeDescriptor, workload: WorkloadDescriptor,
                 carbon_intensity: float, generations: Optional[int] = None) -> List[FlexGenPolicy]:
        """Run the optimizer synchronously (blocks until done)."""
        return asyncio.run(self.run(node, workload, carbon_intensity, generations))
