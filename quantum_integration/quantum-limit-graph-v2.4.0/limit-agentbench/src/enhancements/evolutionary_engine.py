# File: src/enhancements/evolutionary_engine.py
"""
Evolutionary Engine for Green Agent v1.0.0
Manages the lifecycle of experts using sustainability‑aware fitness.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from collections import deque
import numpy as np

# Import existing modules (adjust paths as needed)
from ..expert_registry import ExpertRegistry, ExpertProfile
from ..digital_twin import DigitalTwin
from ..mlops_pipeline import MLOpsPipeline
from ..database.manager import DatabaseManager
from ..task_manager import TaskManager
from .sustainability_cost import SustainabilityCostFunction

logger = logging.getLogger(__name__)

class EvolutionaryEngine:
    """
    Periodic evolutionary engine that:
    - Computes fitness (accuracy / cost) for all experts.
    - Prunes low‑fitness experts (with low plasticity).
    - Merges redundant experts (high similarity, low redundancy).
    - Spawns new experts when a domain gap is detected.
    """
    def __init__(
        self,
        config: Dict[str, Any],
        registry: ExpertRegistry,
        cost_function: SustainabilityCostFunction,
        digital_twin: DigitalTwin,
        mlops: MLOpsPipeline,
        db_manager: DatabaseManager,
        task_manager: TaskManager,
    ):
        self.config = config
        self.registry = registry
        self.cost_function = cost_function
        self.digital_twin = digital_twin
        self.mlops = mlops
        self.db_manager = db_manager
        self.task_manager = task_manager

        self._fitness_history = deque(maxlen=100)
        self._lock = asyncio.Lock()
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # Tunable thresholds (can be config‑driven)
        self.prune_threshold = config.get('prune_threshold', 0.2)
        self.merge_similarity_threshold = config.get('merge_similarity_threshold', 0.85)
        self.spawn_gap_threshold = config.get('spawn_gap_threshold', 0.3)

    async def start(self, interval_seconds: int = 3600):
        """Start the evolution loop."""
        self._running = True
        self._task = asyncio.create_task(self._evolution_loop(interval_seconds))
        logger.info("EvolutionaryEngine started")

    async def _evolution_loop(self, interval: int):
        while self._running:
            try:
                await self._evolve()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Evolution loop error: {e}")
                await asyncio.sleep(60)

    async def _evolve(self):
        """Run one full evolution cycle."""
        # 1. Get all active experts
        experts = self.registry.get_all_active_experts()
        if not experts:
            return

        # 2. Compute fitness for each expert
        fitness_scores = {}
        for expert in experts:
            # Use a generic context (could be derived from recent workload stats)
            context = {"task_type": "general", "token_count": 100}
            cost = await self.cost_function.compute(expert, context)
            accuracy = expert.accuracy_score if expert.accuracy_score else 0.5
            fitness = accuracy / (cost + 1e-8)
            fitness_scores[expert.expert_id] = fitness

        async with self._lock:
            # 3. Prune low‑fitness experts
            to_prune = [
                eid for eid, fit in fitness_scores.items()
                if fit < self.prune_threshold and not self._is_critical(eid)
            ]
            for eid in to_prune:
                await self.registry.deprecate_expert(eid, reason="evolutionary_prune")
                logger.info(f"Pruned expert {eid} (fitness {fitness_scores[eid]:.3f})")

            # 4. Merge similar experts
            to_merge = self._find_similar_experts(experts, fitness_scores)
            for eid_a, eid_b in to_merge:
                merged_id = await self._merge_experts(eid_a, eid_b)
                if merged_id:
                    logger.info(f"Merged experts {eid_a} and {eid_b} into {merged_id}")

            # 5. Spawn new experts if domain gap is detected
            gap = await self._detect_domain_gap(experts, fitness_scores)
            if gap > self.spawn_gap_threshold:
                new_expert_id = await self._spawn_expert(gap)
                if new_expert_id:
                    logger.info(f"Spawned new expert {new_expert_id} due to domain gap {gap:.3f}")

    def _is_critical(self, expert_id: str) -> bool:
        """Check if expert is critical (e.g., high usage, unique capability)."""
        expert = self.registry.get_expert(expert_id)
        if not expert:
            return False
        # Example: usage_count > 100 or domain has few alternatives
        return expert.usage_count > 100

    def _find_similar_experts(
        self,
        experts: List[ExpertProfile],
        fitness: Dict[str, float]
    ) -> List[Tuple[str, str]]:
        """Return pairs of experts that are similar and can be merged."""
        pairs = []
        for i, e1 in enumerate(experts):
            for e2 in experts[i+1:]:
                if (e1.domain == e2.domain and
                    abs(fitness[e1.expert_id] - fitness[e2.expert_id]) < 0.1):
                    pairs.append((e1.expert_id, e2.expert_id))
        # Limit merges per cycle to avoid excessive changes
        return pairs[:5]

    async def _merge_experts(self, expert_a_id: str, expert_b_id: str) -> Optional[str]:
        """Merge two experts into one via weight averaging or distillation."""
        # Placeholder – in reality use ML model merging
        merged = await self.mlops.merge_models(expert_a_id, expert_b_id)
        if not merged:
            return None
        profile = ExpertProfile(
            expert_id=merged['id'],
            expert_name=f"Merged_{expert_a_id}_{expert_b_id}",
            domain=self.registry.get_expert(expert_a_id).domain,
            accuracy_score=merged['accuracy'],
            efficiency_score=(
                self.registry.get_expert(expert_a_id).efficiency_score +
                self.registry.get_expert(expert_b_id).efficiency_score
            ) / 2,
            sustainability_score=merged.get('sustainability_score', 0.5)
        )
        success, _ = await self.registry.register_expert(profile, validate=False, auto_certify=True)
        if success:
            # Deprecate originals
            await self.registry.deprecate_expert(expert_a_id, replacement=profile.expert_id)
            await self.registry.deprecate_expert(expert_b_id, replacement=profile.expert_id)
            return profile.expert_id
        return None

    async def _detect_domain_gap(self, experts: List[ExpertProfile], fitness: Dict[str, float]) -> float:
        """Measure gap between current expert coverage and expected domain distribution."""
        forecast = await self.digital_twin.forecast_domain_distribution()
        if not forecast:
            return 0.0
        current = {e.domain.value: sum(1 for e2 in experts if e2.domain == e.domain) for e in experts}
        gap = 0.0
        for domain, expected in forecast.items():
            actual = current.get(domain, 0)
            if expected > 0 and actual == 0:
                gap += 1.0
        return gap / len(forecast)

    async def _spawn_expert(self, gap: float) -> Optional[str]:
        """Create a new expert for an under‑represented domain."""
        new_expert = await self.mlops.spawn_expert(gap)
        if not new_expert:
            return None
        profile = ExpertProfile(
            expert_id=new_expert['id'],
            expert_name=f"Spawned_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            domain=new_expert['domain'],
            accuracy_score=new_expert['accuracy'],
            efficiency_score=0.8,
            sustainability_score=new_expert.get('sustainability_score', 0.5)
        )
        success, _ = await self.registry.register_expert(profile, validate=False, auto_certify=True)
        return profile.expert_id if success else None

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("EvolutionaryEngine stopped")
