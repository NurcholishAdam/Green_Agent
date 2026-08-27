"""
FlexGen Controller: End‑to‑end closed‑loop policy selection (enhanced).

Addresses the three limitations:
1. Agentic: actively selects and applies policies (now with optional real executor).
2. System-level: orchestrates GPU/CPU/disk offloading under carbon constraints (supports MODP deferral and node-specific carbon).
3. Sustainability-first: reward heavily weights energy/carbon; decisions are logged with rich context.

New enhancements:
- Pluggable executor (mock, real HF connector, or cost model)
- Bio-inspired candidate generation
- Shared reward function
- MODP integration (defer/move decisions)
- Policy drift detection
- Node-specific carbon intensity
- Selector persistence (optional)
- Richer FeedbackEvent with teacher probabilities and Pareto size
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import asdict
import numpy as np

from .flexgen_policy import FlexGenPolicy, MockFlexGenExecutor, generate_candidate_policies
from .flexgen_policy_selector import DistillationFlexGenSelector, FlexGenState
from .flexgen_cost_model import FlexGenCostModel
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..pareto_gating import ParetoGating
from ..async_message_queue import AsyncMessageQueue
from ..schemas.feedback_event import FeedbackEvent
from ..logger import logger

# Optional imports (if modules exist)
try:
    from ..gpu_optimization.reward import compute_reward
except ImportError:
    # Fallback reward
    def compute_reward(metrics: Dict, workload: WorkloadDescriptor) -> float:
        latency_score = 1.0 - min(1.0, metrics['latency_ms'] / max(workload.latency_target, 1.0))
        energy_score = 1.0 - min(1.0, metrics['energy_joules'] / 100.0)
        carbon_score = 1.0 - min(1.0, metrics['carbon_g'] / 10.0)
        success_bonus = 1.0 if metrics.get('success', False) else 0.0
        return 0.4 * success_bonus + 0.2 * latency_score + 0.2 * energy_score + 0.2 * carbon_score

try:
    from .bio_policy_search import BioPolicySearch
except ImportError:
    BioPolicySearch = None

try:
    from ..modp.flexgen_modp_planner import FlexGenMODPPlanner
except ImportError:
    FlexGenMODPPlanner = None

try:
    from .policy_drift_detector import PolicyDriftDetector
except ImportError:
    PolicyDriftDetector = None

try:
    from .gpu_profiler import GPUProfiler
except ImportError:
    GPUProfiler = None


class FlexGenController:
    def __init__(
        self,
        node: NodeDescriptor,
        workload: WorkloadDescriptor,
        carbon_intensity: float,
        message_queue: Optional[AsyncMessageQueue] = None,
        use_real_executor: bool = False,
        executor: Optional[Any] = None,
        use_cost_model_prefilter: bool = False,
        cost_model: Optional[FlexGenCostModel] = None,
        use_bio_search: bool = False,
        bio_search_config: Optional[Dict] = None,
        modp_planner: Optional[FlexGenMODPPlanner] = None,
        drift_detector: Optional[PolicyDriftDetector] = None,
        gpu_profiler: Optional[GPUProfiler] = None,
        selector_persistence_path: Optional[str] = None,
    ):
        """
        Args:
            node: Compute node descriptor.
            workload: Workload descriptor.
            carbon_intensity: Default carbon intensity if node lacks metadata.
            message_queue: Optional AsyncMessageQueue for event publishing.
            use_real_executor: If True, use provided executor (real/mock) instead of MockFlexGenExecutor.
            executor: Callable(policy, node, workload) -> metrics dict.
            use_cost_model_prefilter: If True, pre-filter candidates using cost model before execution.
            cost_model: Cost model instance for prefiltering (defaults to FlexGenCostModel).
            use_bio_search: If True, use BioPolicySearch to evolve candidates.
            bio_search_config: Config dict for BioPolicySearch.
            modp_planner: Optional MODP planner for temporal decisions.
            drift_detector: Optional PolicyDriftDetector for monitoring.
            gpu_profiler: Optional GPUProfiler for real metrics.
            selector_persistence_path: Path to save/load distillation selector weights.
        """
        self.node = node
        self.workload = workload
        self.carbon_intensity = carbon_intensity
        self.message_queue = message_queue
        self.use_real_executor = use_real_executor
        self.executor = executor if executor else MockFlexGenExecutor(carbon_intensity_g_per_kwh=carbon_intensity)
        self.use_cost_model_prefilter = use_cost_model_prefilter
        self.cost_model = cost_model or FlexGenCostModel(carbon_intensity_g_per_kwh=carbon_intensity)
        self.use_bio_search = use_bio_search
        self.bio_search_config = bio_search_config or {}
        self.modp_planner = modp_planner
        self.drift_detector = drift_detector
        self.gpu_profiler = gpu_profiler

        # Update executor carbon intensity per node if possible
        if hasattr(self.executor, 'carbon_intensity'):
            self.executor.carbon_intensity = node.metadata.get("region_carbon_intensity", carbon_intensity)

        # Initialize selector with dynamic support
        self.selector = DistillationFlexGenSelector(
            n_candidates=20,  # will be adjusted dynamically
            persistence_path=selector_persistence_path
        )

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

    def _get_node_carbon_intensity(self) -> float:
        """Get carbon intensity for the node, falling back to default."""
        return self.node.metadata.get("region_carbon_intensity", self.carbon_intensity)

    async def _execute_policy(self, policy: FlexGenPolicy) -> Dict[str, Any]:
        """Execute a policy using the configured executor and optionally measure real metrics."""
        if self.use_real_executor and self.executor:
            # If using real executor, we may also collect GPU metrics before/after
            gpu_before = None
            if self.gpu_profiler:
                gpu_before = self.gpu_profiler.get_gpu_metrics()
            metrics = await self.executor(policy, self.node, self.workload)
            if self.gpu_profiler:
                gpu_after = self.gpu_profiler.get_gpu_metrics()
                # Add energy/carbon based on real power if not already present
                if 'energy_joules' not in metrics and gpu_before and gpu_after:
                    avg_power = (gpu_before.get('gpu_power_watts', 65) + gpu_after.get('gpu_power_watts', 65)) / 2
                    latency_s = metrics.get('latency_ms', 0) / 1000.0
                    metrics['energy_joules'] = avg_power * latency_s
                    metrics['carbon_g'] = (metrics['energy_joules'] / 3.6e6) * self._get_node_carbon_intensity()
            return metrics
        else:
            # Use mock executor
            metrics = self.executor.execute(policy, self.node, self.workload)
            return metrics

    async def _generate_candidates(self) -> List[FlexGenPolicy]:
        """Generate candidate policies, optionally using bio search or cost model prefiltering."""
        if self.use_bio_search and BioPolicySearch is not None:
            # Use bio-inspired search (synchronous; run in executor to avoid blocking)
            bio = BioPolicySearch(
                node=self.node,
                workload=self.workload,
                cost_model=self.cost_model,
                **self.bio_search_config
            )
            # BioPolicySearch.run() is synchronous; call directly (could be optimized later)
            candidates = bio.run()
            logger.info(f"Bio search produced {len(candidates)} candidates.")
            return candidates
        else:
            # Generate random candidates
            candidates = generate_candidate_policies(20)
            if self.use_cost_model_prefilter and self.cost_model:
                # Pre-filter using cost model: evaluate each and keep feasible and cheap
                estimates = []
                for policy in candidates:
                    est = self.cost_model.estimate(policy, self.node, self.workload)
                    if est.peak_gpu_memory_gb <= self.node.metadata.get("gpu_memory_gb", 16):
                        estimates.append((policy, est))
                # Sort by weighted cost and keep top N
                estimates.sort(key=lambda x: (x[1].total_latency_ms + x[1].total_energy_joules))
                candidates = [p for p, _ in estimates[:10]]
            return candidates

    async def step(self) -> Dict:
        """One iteration of the closed loop."""
        # 0. Check MODP planner for temporal decision
        if self.modp_planner:
            modp_action, delay, target_node = await self.modp_planner.plan(
                self.workload, self.node, None, queue_length=0, current_carbon=self._get_node_carbon_intensity()
            )
            if modp_action == "defer":
                logger.info(f"MODP suggests deferring for {delay} hours; controller will defer.")
                return {
                    "action": "defer",
                    "delay_hours": delay,
                    "reason": "modp_deferral",
                }
            elif modp_action == "move_node" and target_node:
                # In a multi-node setup, we would switch node here. For simplicity, just log.
                logger.info(f"MODP suggests moving to node {target_node}; not implemented in single-node controller.")
                # Continue with current node for now.

        # 1. Generate candidate policies
        candidates = await self._generate_candidates()

        # 2. Evaluate all candidates
        metrics_list = []
        for idx, policy in enumerate(candidates):
            metrics = await self._execute_policy(policy)
            metrics["policy_idx"] = idx
            metrics["policy"] = policy.to_dict()
            metrics_list.append(metrics)

        # 3. Filter by Pareto dominance
        feasible = [m for m in metrics_list if m.get("success", False)]
        if not feasible:
            logger.warning("No feasible policies found, falling back to all")
            feasible = metrics_list
        pareto_candidates = self.pareto.filter(feasible)
        if not pareto_candidates:
            pareto_candidates = feasible

        # 4. Build state for selector
        state = FlexGenState(
            tokens=self.workload.tokens,
            latency_target=self.workload.latency_target,
            gpu_memory_gb=self.node.metadata.get("gpu_memory_gb", 16.0),
            cpu_memory_gb=self.node.metadata.get("cpu_memory_gb", 64.0),
            disk_bandwidth_gbps=self.node.metadata.get("disk_bandwidth_gbps", 2.0),
            carbon_intensity=self._get_node_carbon_intensity(),
            recent_success_rate=0.8,
            avg_reward=0.6,
            policy_idx=0,
        )

        # 5. Select policy index from the Pareto set
        # The selector may need to handle variable number of candidates; we'll create a temporary selector view
        # In our enhanced selector, `select_policy` expects a list of policies; we pass the Pareto list.
        action_idx, state_vec, teacher_probs = await self.selector.select_policy(
            pareto_candidates, state, exploration=True
        )
        chosen_policy_metrics = pareto_candidates[action_idx]
        chosen_policy = FlexGenPolicy(**chosen_policy_metrics["policy"])

        # 6. Execute already done; use metrics from chosen
        metrics = chosen_policy_metrics

        # 7. Compute reward using shared function
        reward = compute_reward(metrics, self.workload)

        # 8. Update selector
        next_state_vec = state_vec
        await self.selector.update(state_vec, action_idx, reward, next_state_vec, teacher_probs)

        # 9. Policy drift detection
        drift_detected = False
        if self.drift_detector:
            self.drift_detector.add_policy(chosen_policy.to_dict(), reward=reward)
            if self.drift_detector.detect_drift():
                logger.warning("Policy drift detected; consider rollback.")
                drift_detected = True

        # 10. Publish FeedbackEvent
        if self.message_queue:
            event = FeedbackEvent(
                source="flexgen_controller",
                feedback_type="routing",
                task_id=self.workload.task_id or "unknown",
                context={
                    "node_id": self.node.id,
                    "carbon_intensity": self._get_node_carbon_intensity(),
                    "pareto_size": len(pareto_candidates),
                    "executor_type": "real" if self.use_real_executor else "mock",
                    "drift_detected": drift_detected,
                    "teacher_probs": teacher_probs.tolist() if teacher_probs is not None else [],
                },
                action={"selected_action": str(chosen_policy.to_dict()),
                        "selected_rank": action_idx,
                        "confidence_score": teacher_probs[action_idx] if teacher_probs is not None else 0.5},
                performance={"quality_score": metrics.get("quality_score", 0.9),
                             "latency_ms": metrics["latency_ms"],
                             "energy_joules": metrics["energy_joules"],
                             "carbon_g": metrics["carbon_g"],
                             "helium_cost": 0,
                             "duration_ms": 0},
                adaptive_cost_value=reward,
                tags=["flexgen", "policy_selection", "carbon_aware", "executor_" + ("real" if self.use_real_executor else "mock")],
            )
            await self.message_queue.publish("policy_outcomes", event.to_json())

        # Store last context
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs
        self.last_candidates = pareto_candidates

        return {
            "chosen_policy": chosen_policy.to_dict(),
            "metrics": metrics,
            "reward": reward,
            "pareto_count": len(pareto_candidates),
            "drift_detected": drift_detected,
        }

    def _reconstruct_policy(self, metrics: Dict) -> FlexGenPolicy:
        """Convert metrics dict back to a FlexGenPolicy."""
        return FlexGenPolicy(**metrics["policy"])

    def _compute_reward(self, metrics: Dict, workload: WorkloadDescriptor) -> float:
        """Use shared reward function."""
        return compute_reward(metrics, workload)
