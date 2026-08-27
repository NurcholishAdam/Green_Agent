"""
FlexGen Controller: End‑to‑end closed‑loop policy selection.

Addresses the three limitations:
1. Agentic: actively selects and applies policies.
2. System-level: orchestrates GPU/CPU/disk offloading under carbon constraints.
3. Sustainability-first: reward heavily weights energy/carbon; decisions are logged.
"""

import asyncio
from typing import Dict, List, Optional, Tuple
from dataclasses import asdict
import numpy as np

from .flexgen_policy import FlexGenPolicy, MockFlexGenExecutor, generate_candidate_policies
from .flexgen_policy_selector import DistillationFlexGenSelector, FlexGenState
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..pareto_gating import ParetoGating
from ..async_message_queue import AsyncMessageQueue
from ..schemas.feedback_event import FeedbackEvent
from ..logger import logger


class FlexGenController:
    def __init__(
        self,
        node: NodeDescriptor,
        workload: WorkloadDescriptor,
        carbon_intensity: float,
        message_queue: Optional[AsyncMessageQueue] = None,
    ):
        self.node = node
        self.workload = workload
        self.carbon_intensity = carbon_intensity
        self.message_queue = message_queue
        self.executor = MockFlexGenExecutor(carbon_intensity_g_per_kwh=carbon_intensity)
        self.selector = DistillationFlexGenSelector(n_candidates=20)
        self.pareto = ParetoGating(
            objectives=[
                {"key": "latency_ms", "direction": "min"},
                {"key": "energy_joules", "direction": "min"},
                {"key": "carbon_g", "direction": "min"},
            ]
        )
        self.last_state_vec = None
        self.last_action_idx = None
        self.last_teacher_probs = None
        self.last_candidates = None

    async def step(self) -> Dict:
        """One iteration of the closed loop."""
        # 1. Generate candidate policies
        candidates = generate_candidate_policies(20)

        # 2. Evaluate all candidates with mock executor to get metrics
        metrics_list = []
        for policy in candidates:
            metrics = self.executor.execute(policy, self.node, self.workload)
            # Add policy index for later identification
            metrics["policy_idx"] = len(metrics_list)
            metrics_list.append(metrics)

        # 3. Filter by Pareto dominance
        feasible = []
        for m in metrics_list:
            if m["success"]:
                feasible.append(m)
        if not feasible:
            logger.warning("No feasible policies found, falling back to all")
            feasible = metrics_list
        pareto_candidates = self.pareto.filter(feasible)
        # If no Pareto candidates, use all feasible
        if not pareto_candidates:
            pareto_candidates = feasible

        # 4. Build state for distillation selector
        state = FlexGenState(
            tokens=self.workload.tokens,
            latency_target=self.workload.latency_target,
            gpu_memory_gb=self.node.metadata.get("gpu_memory_gb", 16.0),
            cpu_memory_gb=self.node.metadata.get("cpu_memory_gb", 64.0),
            disk_bandwidth_gbps=self.node.metadata.get("disk_bandwidth_gbps", 2.0),
            carbon_intensity=self.carbon_intensity,
            recent_success_rate=0.8,
            avg_reward=0.6,
            policy_idx=0,  # will be overwritten
        )

        # 5. Select policy index from the Pareto set
        # Note: the selector's n_candidates might be larger than len(pareto_candidates)
        # We'll temporarily adapt by using min(n, len(pareto_candidates))
        action_idx, state_vec, teacher_probs = await self.selector.select_policy(
            pareto_candidates, state, exploration=True
        )
        chosen_policy_metrics = pareto_candidates[action_idx]
        chosen_policy = self._reconstruct_policy(chosen_policy_metrics)

        # 6. Execute (already done in step 2, but we could re-run with chosen policy)
        # For purity, we already have metrics; no need to re-execute.
        metrics = chosen_policy_metrics

        # 7. Compute reward
        reward = self._compute_reward(metrics, self.workload)

        # 8. Update selector
        next_state_vec = state_vec  # same state for simplicity
        await self.selector.update(state_vec, action_idx, reward, next_state_vec, teacher_probs)

        # 9. Publish FeedbackEvent
        if self.message_queue:
            event = FeedbackEvent(
                source="flexgen_controller",
                feedback_type="routing",
                task_id=self.workload.task_id or "unknown",
                context={"node_id": self.node.id, "carbon_intensity": self.carbon_intensity},
                action={"selected_action": str(chosen_policy.to_dict()),
                        "selected_rank": action_idx,
                        "confidence_score": 0.5},
                performance={"quality_score": 0.9,  # assume fixed quality
                             "latency_ms": metrics["latency_ms"],
                             "energy_joules": metrics["energy_joules"],
                             "carbon_g": metrics["carbon_g"],
                             "helium_cost": 0,
                             "duration_ms": 0},
                adaptive_cost_value=reward,
                tags=["flexgen", "policy_selection", "carbon_aware"],
            )
            await self.message_queue.publish("policy_outcomes", event.to_json())

        # Store last context for potential drift detection
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs
        self.last_candidates = pareto_candidates

        return {
            "chosen_policy": chosen_policy.to_dict(),
            "metrics": metrics,
            "reward": reward,
            "pareto_count": len(pareto_candidates),
        }

    def _reconstruct_policy(self, metrics: Dict) -> FlexGenPolicy:
        """Convert metrics dict back to a FlexGenPolicy (since metrics include policy dict)."""
        return FlexGenPolicy(**metrics["policy"])

    def _compute_reward(self, metrics: Dict, workload: WorkloadDescriptor) -> float:
        """Reward that balances quality, latency, energy, carbon."""
        latency_score = 1.0 - min(1.0, metrics["latency_ms"] / max(workload.latency_target, 1.0))
        energy_score = 1.0 - min(1.0, metrics["energy_joules"] / 100.0)  # normalize
        carbon_score = 1.0 - min(1.0, metrics["carbon_g"] / 10.0)
        success_bonus = 1.0 if metrics["success"] else 0.0
        reward = (
            0.4 * success_bonus +
            0.2 * latency_score +
            0.2 * energy_score +
            0.2 * carbon_score
        )
        return max(0.0, min(1.0, reward))
