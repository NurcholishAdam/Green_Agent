"""
Evolutionary policy search for FlexGen policies (Enhanced).
Uses a genetic algorithm with crossover, elitism, and scalar reward to evolve
candidate policies, evaluated via cost model or real execution. Integrates
with other Green Agent modules (ParetoGating, AsyncMessageQueue, FeedbackEvent,
reward computation) for agentic closed-loop sustainability-aware orchestration.
"""

import random
import logging
from typing import List, Dict, Any, Tuple, Optional, Callable
from dataclasses import asdict
import numpy as np

from .flexgen_policy import FlexGenPolicy
from .flexgen_cost_model import FlexGenCostModel
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..pareto_gating import ParetoGating
from ..async_message_queue import AsyncMessageQueue
from ..schemas.feedback_event import FeedbackEvent
from ..logger import logger

# Optional reward function import (if defined elsewhere)
try:
    from ..gpu_optimization.reward import compute_reward
except ImportError:
    # Fallback simple reward
    def compute_reward(metrics: Dict[str, Any], workload: WorkloadDescriptor) -> float:
        """
        Default reward: quality, latency satisfaction, energy efficiency, carbon efficiency.
        """
        weights = {'quality': 0.3, 'throughput': 0.25, 'energy': 0.2, 'carbon': 0.15, 'memory': 0.1}
        latency_score = max(0.0, 1.0 - metrics['latency_ms'] / max(workload.latency_target, 1.0))
        energy_score = max(0.0, 1.0 - metrics['energy_joules'] / 100.0)  # normalize arbitrary
        carbon_score = max(0.0, 1.0 - metrics['carbon_g'] / 10.0)
        memory_score = 1.0 if metrics.get('success', True) else 0.0
        quality = metrics.get('quality_score', 0.9)
        reward = (weights['quality'] * quality +
                  weights['throughput'] * latency_score +
                  weights['energy'] * energy_score +
                  weights['carbon'] * carbon_score +
                  weights['memory'] * memory_score)
        return max(0.0, min(1.0, reward))


class BioPolicySearch:
    """
    Enhanced evolutionary optimizer for FlexGen policies.

    Features:
    - Crossover and elitism.
    - Evaluation via cost model or real/mock executor.
    - Scalar reward for selection pressure.
    - FeedbackEvent emission via AsyncMessageQueue.
    - Population diversity monitoring for drift detection.
    - Adaptive mutation rate.
    - Infeasible policy handling.
    - Integration with MoE/MODP by returning Pareto set and logging.
    """

    def __init__(
        self,
        node: NodeDescriptor,
        workload: WorkloadDescriptor,
        cost_model: FlexGenCostModel,
        population_size: int = 50,
        generations: int = 10,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        elite_size: int = 5,
        use_real_executor: bool = False,
        executor: Optional[Callable] = None,
        carbon_intensity: float = 400.0,
        message_queue: Optional[AsyncMessageQueue] = None,
        drift_threshold: float = 0.3,
        diversity_threshold: float = 0.05,
    ):
        """
        Args:
            node: Compute node descriptor.
            workload: Workload descriptor.
            cost_model: Cost model for evaluation (if not using real executor).
            population_size: Number of policies in each generation.
            generations: Number of evolutionary generations.
            mutation_rate: Initial mutation probability per gene.
            crossover_rate: Probability of performing crossover.
            elite_size: Number of top policies preserved each generation.
            use_real_executor: If True, use executor instead of cost model.
            executor: Callable(policy, node, workload) -> metrics dict.
            carbon_intensity: Current carbon intensity (gCO2/kWh).
            message_queue: Optional AsyncMessageQueue for event logging.
            drift_threshold: Drift detection threshold (distance from average policy).
            diversity_threshold: Minimum population diversity before increasing mutation.
        """
        self.node = node
        self.workload = workload
        self.cost_model = cost_model
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.elite_size = min(elite_size, population_size)
        self.use_real_executor = use_real_executor
        self.executor = executor
        self.carbon_intensity = carbon_intensity
        self.message_queue = message_queue
        self.drift_threshold = drift_threshold
        self.diversity_threshold = diversity_threshold

        self.population: List[FlexGenPolicy] = []
        self.pareto = ParetoGating(
            objectives=[
                {"key": "latency_ms", "direction": "min"},
                {"key": "energy_joules", "direction": "min"},
                {"key": "carbon_g", "direction": "min"},
            ]
        )
        self.best_policy: Optional[FlexGenPolicy] = None
        self.best_reward: float = -1.0
        self.generation_history: List[Dict[str, Any]] = []  # for drift detection

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

    def _evaluate(self, policy: FlexGenPolicy) -> Tuple[Dict[str, Any], float]:
        """
        Evaluate a policy and return (metrics, reward).
        """
        if self.use_real_executor and self.executor is not None:
            metrics = self.executor(policy, self.node, self.workload)
        else:
            est = self.cost_model.estimate(policy, self.node, self.workload)
            metrics = {
                "latency_ms": est.total_latency_ms,
                "energy_joules": est.total_energy_joules,
                "carbon_g": est.total_carbon_g,
                "gpu_memory_gb": est.peak_gpu_memory_gb,
                "success": est.peak_gpu_memory_gb <= self.node.metadata.get("gpu_memory_gb", 16.0),
                "quality_score": 0.9,  # assume fixed for now
            }
        reward = compute_reward(metrics, self.workload)
        return metrics, reward

    def _crossover(self, parent1: FlexGenPolicy, parent2: FlexGenPolicy) -> FlexGenPolicy:
        """Uniform crossover over all fields."""
        child_dict = {}
        for field_name in FlexGenPolicy.__dataclass_fields__:
            if random.random() < 0.5:
                child_dict[field_name] = getattr(parent1, field_name)
            else:
                child_dict[field_name] = getattr(parent2, field_name)
        return FlexGenPolicy(**child_dict)

    def _mutate(self, policy: FlexGenPolicy, mutation_rate: Optional[float] = None) -> FlexGenPolicy:
        """Mutate a policy with the given rate (or self.mutation_rate)."""
        if mutation_rate is None:
            mutation_rate = self.mutation_rate
        new_policy = FlexGenPolicy(**policy.to_dict())
        if random.random() < mutation_rate:
            new_policy.gpu_batch_size = random.choice([1, 2, 4, 8])
        if random.random() < mutation_rate:
            new_policy.block_size = random.choice([8, 16, 32, 64])
        if random.random() < mutation_rate:
            new_policy.weight_device = random.choice(["gpu", "cpu", "disk"])
        if random.random() < mutation_rate:
            new_policy.activation_device = random.choice(["gpu", "cpu"])
        if random.random() < mutation_rate:
            new_policy.kv_cache_device = random.choice(["gpu", "cpu", "disk"])
        if random.random() < mutation_rate:
            new_policy.weight_bits = random.choice([4, 8, 16])
        if random.random() < mutation_rate:
            new_policy.kv_cache_bits = random.choice([4, 8, 16])
        if random.random() < mutation_rate:
            new_policy.cpu_attention = not new_policy.cpu_attention
        if random.random() < mutation_rate:
            new_policy.overlap_io_compute = not new_policy.overlap_io_compute
        return new_policy

    def _select_parents(self, evaluated: List[Tuple[FlexGenPolicy, Dict, float]]) -> List[FlexGenPolicy]:
        """
        Tournament selection based on reward.
        """
        parents = []
        tournament_size = max(2, int(self.population_size * 0.1))
        for _ in range(self.population_size):
            candidates = random.sample(evaluated, tournament_size)
            winner = max(candidates, key=lambda x: x[2])  # highest reward
            parents.append(winner[0])
        return parents

    def _compute_diversity(self) -> float:
        """Average pairwise Euclidean distance between policy feature vectors."""
        if len(self.population) < 2:
            return 1.0
        vectors = [self._policy_to_vector(p) for p in self.population]
        dists = []
        for i in range(len(vectors)):
            for j in range(i + 1, len(vectors)):
                dists.append(np.linalg.norm(np.array(vectors[i]) - np.array(vectors[j])))
        return float(np.mean(dists)) if dists else 0.0

    def _policy_to_vector(self, policy: FlexGenPolicy) -> List[float]:
        """Convert policy to a fixed-length numeric vector for distance calculations."""
        vec = [
            policy.gpu_batch_size / 8.0,
            policy.block_size / 64.0,
            1.0 if policy.weight_device == 'gpu' else 0.0,
            1.0 if policy.weight_device == 'cpu' else 0.0,
            1.0 if policy.activation_device == 'gpu' else 0.0,
            1.0 if policy.kv_cache_device == 'gpu' else 0.0,
            1.0 if policy.kv_cache_device == 'cpu' else 0.0,
            policy.weight_bits / 16.0,
            policy.kv_cache_bits / 16.0,
            1.0 if policy.cpu_attention else 0.0,
            1.0 if policy.overlap_io_compute else 0.0,
        ]
        return vec

    def _detect_drift(self, new_population: List[FlexGenPolicy]) -> bool:
        """
        Simple drift detection: compare new population centroid to previous one.
        Returns True if shift exceeds threshold.
        """
        if not self.generation_history:
            return False
        prev_vec = self.generation_history[-1]
        curr_vec = np.mean([self._policy_to_vector(p) for p in new_population], axis=0)
        dist = np.linalg.norm(curr_vec - np.array(prev_vec))
        return dist > self.drift_threshold

    async def _publish_event(self, policy: FlexGenPolicy, metrics: Dict[str, Any], reward: float, generation: int):
        """Publish a FeedbackEvent for the selected policy."""
        if not self.message_queue:
            return
        event = FeedbackEvent(
            source="bio_policy_search",
            feedback_type="routing",
            task_id=self.workload.task_id or "unknown",
            context={
                "generation": generation,
                "node_id": self.node.id,
                "carbon_intensity": self.carbon_intensity,
                "population_size": self.population_size,
            },
            action={"selected_action": str(policy.to_dict()),
                    "selected_rank": generation,
                    "confidence_score": 0.5},
            performance={"quality_score": metrics.get("quality_score", 0.9),
                         "latency_ms": metrics.get("latency_ms", 0),
                         "energy_joules": metrics.get("energy_joules", 0),
                         "carbon_g": metrics.get("carbon_g", 0),
                         "helium_cost": 0,
                         "duration_ms": 0},
            adaptive_cost_value=reward,
            tags=["bio_inspired", "flexgen_policy", "evolution"],
        )
        await self.message_queue.publish("bio_inspired_events", event.to_json())

    def run(self) -> List[FlexGenPolicy]:
        """Run evolutionary search and return Pareto‑optimal policies."""
        # Initialize population
        self.population = [self._random_policy() for _ in range(self.population_size)]

        for gen in range(self.generations):
            # Evaluate all policies
            evaluated = []
            for policy in self.population:
                metrics, reward = self._evaluate(policy)
                evaluated.append((policy, metrics, reward))

            # Track best policy
            best_in_gen = max(evaluated, key=lambda x: x[2])
            if best_in_gen[2] > self.best_reward:
                self.best_reward = best_in_gen[2]
                self.best_policy = best_in_gen[0]

            # Build Pareto set from successful policies
            successful_metrics = [m for _, m, _ in evaluated if m.get('success', False)]
            if not successful_metrics:
                successful_metrics = [m for _, m, _ in evaluated]
            pareto_metrics = self.pareto.filter(successful_metrics)

            # Map back to policies
            pareto_policies = []
            for m in pareto_metrics:
                for p, pm, r in evaluated:
                    if pm == m:
                        pareto_policies.append(p)
                        break

            # Publish best policy of this generation (async)
            if self.message_queue:
                import asyncio
                asyncio.run(self._publish_event(best_in_gen[0], best_in_gen[1], best_in_gen[2], gen))

            # Store generation centroid for drift detection
            centroid = np.mean([self._policy_to_vector(p) for p in self.population], axis=0)
            self.generation_history.append(centroid.tolist())

            # Compute diversity and adjust mutation rate
            diversity = self._compute_diversity()
            if diversity < self.diversity_threshold:
                # Increase mutation to promote exploration
                current_mutation = min(0.5, self.mutation_rate * 1.5)
            else:
                current_mutation = self.mutation_rate

            # Selection
            parents = self._select_parents(evaluated)

            # Create offspring
            offspring = []
            # Elitism: preserve top policies
            elite_candidates = sorted(evaluated, key=lambda x: x[2], reverse=True)[:self.elite_size]
            offspring.extend([p for p, _, _ in elite_candidates])

            while len(offspring) < self.population_size:
                p1, p2 = random.sample(parents, 2)
                if random.random() < self.crossover_rate:
                    child = self._crossover(p1, p2)
                else:
                    child = random.choice([p1, p2])
                child = self._mutate(child, current_mutation)
                offspring.append(child)

            # Replace population
            self.population = offspring[:self.population_size]

            # Check for drift (optional logging)
            if gen > 0 and self._detect_drift(self.population):
                logger.warning(f"Generation {gen}: population drift detected.")

            logger.info(
                f"Generation {gen}: best reward={self.best_reward:.3f}, "
                f"pareto_size={len(pareto_policies)}, diversity={diversity:.3f}"
            )

        # Final evaluation of last population
        final_evaluated = []
        for policy in self.population:
            metrics, reward = self._evaluate(policy)
            final_evaluated.append((policy, metrics))
        final_metrics = [m for _, m in final_evaluated if m.get('success', False)]
        if not final_metrics:
            final_metrics = [m for _, m in final_evaluated]
        final_pareto = self.pareto.filter(final_metrics)
        final_policies = [p for p, m in final_evaluated if m in final_pareto]
        if not final_policies:
            final_policies = [p for p, _ in final_evaluated][:10]

        return final_policies
