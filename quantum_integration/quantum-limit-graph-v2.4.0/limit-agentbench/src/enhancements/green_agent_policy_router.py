"""
green_agent_policy_router.py

Main orchestration class that integrates:
  - CarbonDelayScheduler
  - PolicyMetaCache
  - ContextualBandit
  - Fallback LP solver
"""
import time
from typing import Dict, Any, Optional

# Assume these are available in the enhancements folder
from .carbon_delay_scheduler import CarbonDelayScheduler
from .policy_meta_cache import PolicyMetaCache, WorkloadFingerprint
from .contextual_bandit import ContextualBandit


class GreenAgentPolicyRouter:
    """
    Routes tasks to the best policy using a three‑tier decision system:
    1. Carbon‑aware delay (if non‑urgent)
    2. Meta‑cache (nearest historical policy)
    3. Contextual Bandit (online learning)
    4. Fallback LP solver (stable Phase 2)
    """
    def __init__(
        self,
        carbon_api,
        action_space: list,
        lp_solver,
        executor,
        min_trials_before_bandit: int = 5,
        confidence_threshold: float = 0.6,
    ):
        """
        Args:
            carbon_api: API object for carbon intensity.
            action_space: List of candidate policy dicts.
            lp_solver: Callable that takes a WorkloadFingerprint and returns a policy.
            executor: The Phase 3 FlexGen executor.
            min_trials_before_bandit: Safety gate for bandit.
            confidence_threshold: Minimum confidence to use bandit.
        """
        self.carbon_scheduler = CarbonDelayScheduler(carbon_api)
        self.cache = PolicyMetaCache()
        self.bandit = ContextualBandit(action_space, lp_solver)
        self.lp_solver = lp_solver
        self.executor = executor
        self.min_trials = min_trials_before_bandit
        self.conf_threshold = confidence_threshold

    def _generate_fingerprint(self, task: Dict[str, Any]) -> WorkloadFingerprint:
        """Extract key parameters from task."""
        return WorkloadFingerprint(
            model_size_mb=task.get("model_size_mb", 0),
            prompt_len=task.get("prompt_len", 0),
            gen_len=task.get("gen_len", 0),
            gpu_mem_free_mb=task.get("gpu_mem_free_mb", 0),
            disk_speed_class=task.get("disk_speed_class", 1),  # default to SSD
        )

    def _calculate_reward(self, metrics: Dict[str, Any], constraints: Dict[str, Any]) -> float:
        """
        Example reward function. In practice you would use your Phase 1 metrics.
        """
        quality = metrics.get("quality_score", 1.0)
        throughput = metrics.get("tokens_per_sec", 0)
        energy_eff = metrics.get("energy_efficiency", 0.5)  # placeholder
        carbon_eff = metrics.get("carbon_efficiency", 0.5)

        # Penalties for violating constraints
        penalty = 0.0
        if metrics.get("gpu_oom", False):
            penalty -= 10.0
        if metrics.get("latency_ms", 0) > constraints.get("max_latency_ms", 1e9):
            penalty -= 5.0

        reward = (
            0.35 * quality +
            0.25 * (throughput / 100.0) +   # scale to ~0-1
            0.20 * energy_eff +
            0.15 * carbon_eff +
            0.05 * (metrics.get("memory_efficiency", 0))
        ) + penalty
        return max(-10.0, min(10.0, reward))  # clamp

    def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main entry point. Returns a dict with 'policy', 'metrics', and 'status'.
        """
        # ---- Step 1: Carbon delay ----
        delay_result = self.carbon_scheduler.submit(task)
        if delay_result["status"] == "delayed":
            return {
                "status": "deferred",
                "reason": "low_carbon_window",
                "delay_until": delay_result["delay_until"],
            }

        # ---- Step 2: Generate fingerprint ----
        fp = self._generate_fingerprint(task)

        # ---- Step 3: Try cache ----
        cached_policy = self.cache.get_best_policy(fp)
        if cached_policy is not None:
            final_policy = cached_policy
            policy_source = "cache"
        else:
            # ---- Step 4: Bandit selection ----
            bandit_policy, confidence = self.bandit.select_action(
                fp,
                min_trials_before_bandit=self.min_trials,
                confidence_threshold=self.conf_threshold
            )
            if bandit_policy is not None and confidence >= self.conf_threshold:
                final_policy = bandit_policy
                policy_source = "bandit"
            else:
                # ---- Step 5: Fallback to LP solver ----
                final_policy = self.lp_solver(fp)
                policy_source = "lp_solver"
                # Seed bandit with this safe policy (so it can learn faster)
                self.bandit.seed_safe_policy(fp, final_policy)

        # ---- Step 6: Execute (Phase 3) ----
        # The executor must accept (task, policy) and return real metrics.
        metrics = self.executor.run(task, final_policy)

        # ---- Step 7: Calculate reward and update ----
        constraints = task.get("constraints", {})
        reward = self._calculate_reward(metrics, constraints)

        # Update the appropriate learning component
        if policy_source == "bandit":
            self.bandit.update(fp, final_policy, reward)
        elif policy_source == "lp_solver" or policy_source == "cache":
            # Update cache with the result (whether from LP or cache)
            # For cache, we refresh timestamp even if the same policy.
            self.cache.update(fp, final_policy, reward)

        # Return result
        return {
            "status": "completed",
            "policy": final_policy,
            "policy_source": policy_source,
            "metrics": metrics,
            "reward": reward,
        }

    def tick_carbon_queue(self) -> list:
        """
        Call this periodically (e.g., every minute) to release delayed tasks.
        Returns a list of tasks to be processed.
        """
        return self.carbon_scheduler.tick()
