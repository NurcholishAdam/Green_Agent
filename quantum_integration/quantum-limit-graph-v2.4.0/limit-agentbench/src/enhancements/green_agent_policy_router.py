"""
green_agent_policy_router.py

Enhanced orchestration class that integrates:
  - CarbonDelayScheduler (with MODP, bio, MoE)
  - PolicyMetaCache (with MoE enrichment)
  - ContextualBandit (with MODP, bio expansion, MoE)
  - GPUProfiler and MetricAggregator for real metrics
  - MODP for multi‑objective reward
  - Bio‑inspired action expansion
  - MoE context encoding
  - Persistence of all state
  - Unified logging and statistics
"""

import time
import json
import os
import logging
from typing import Dict, Any, Optional, List, Callable

# Import enhanced modules (all in the enhancements folder)
from .carbon_delay_scheduler import CarbonDelayScheduler
from .policy_meta_cache import PolicyMetaCache, WorkloadFingerprint
from .contextual_bandit import ContextualBandit
from .gpu_profiler import GPUProfiler
from .metric_aggregator import MetricAggregator
from .reward_calculator import RewardCalculator

# Optionally import bio, moe, MODP (with fallbacks)
try:
    from .bio_inspired import GeneticPolicyGenerator
except ImportError:
    class GeneticPolicyGenerator:
        def generate_policies(self, current_policies, n=2):
            return []  # stub

try:
    from .moe_system import ExpertRouter
except ImportError:
    class ExpertRouter:
        def encode(self, task, hardware_state):
            # Default: use fingerprint vector
            return task.get("fingerprint", [0.0]*5)

try:
    from .MODP import ParetoOptimizer
except ImportError:
    class ParetoOptimizer:
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)


class GreenAgentPolicyRouter:
    """
    Enhanced router with:
    - Carbon‑aware delay (using enhanced CarbonDelayScheduler)
    - Meta‑cache with MoE‑enriched fingerprints
    - Contextual Bandit with MODP reward and bio‑inspired expansion
    - Real‑time metrics from GPUProfiler
    - MODP for multi‑objective reward calculation
    - Persistence of all learned state
    - Comprehensive statistics and logging
    """

    def __init__(
        self,
        carbon_api,
        action_space: List[Dict[str, Any]],
        lp_solver: Callable,
        executor: Callable,
        modp_weights: Optional[Dict[str, float]] = None,
        moe_router: Optional[Any] = None,
        bio_generator: Optional[Any] = None,
        min_trials_before_bandit: int = 5,
        confidence_threshold: float = 0.6,
        persistence_file: str = "router_state.json",
        enable_profiler: bool = True,
    ):
        """
        Args:
            carbon_api: Object for carbon intensity (get_current, get_forecast).
            action_space: Initial list of candidate policy dicts.
            lp_solver: Callable that takes a WorkloadFingerprint and returns a policy.
            executor: Phase 3 FlexGen executor (or MetricAggregator run method).
            modp_weights: Weights for MODP objectives (quality, throughput, energy, carbon, memory).
            moe_router: MoE encoder for context enrichment.
            bio_generator: Bio‑inspired policy generator for action expansion.
            min_trials_before_bandit: Safety gate for bandit.
            confidence_threshold: Minimum confidence to use bandit.
            persistence_file: File to save/load state.
            enable_profiler: Whether to start GPUProfiler.
        """
        self.logger = logging.getLogger(__name__)

        # ---- Instantiate enhanced sub‑modules ----
        self.carbon_scheduler = CarbonDelayScheduler(carbon_api)
        self.cache = PolicyMetaCache()  # will be enhanced with MoE if available
        self.bandit = ContextualBandit(
            action_space=action_space,
            fallback_solver=lp_solver,
            modp_weights=modp_weights,
            moe_router=moe_router,
            bio_generator=bio_generator,
        )
        self.lp_solver = lp_solver
        self.executor = executor
        self.min_trials = min_trials_before_bandit
        self.conf_threshold = confidence_threshold
        self.persistence_file = persistence_file

        # ---- MODP reward calculator ----
        self.modp_weights = modp_weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy_efficiency": 0.20,
            "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10,
        }
        self.reward_calc = RewardCalculator(weights=self.modp_weights)

        # ---- MoE router ----
        self.moe = moe_router if moe_router else ExpertRouter()

        # ---- Bio‑inspired generator ----
        self.bio = bio_generator if bio_generator else GeneticPolicyGenerator()

        # ---- Profiler (for real metrics) ----
        self.profiler = None
        if enable_profiler:
            self.profiler = GPUProfiler()
            self.profiler.start()
            self.metric_aggregator = MetricAggregator(self.profiler, self.executor)
        else:
            self.metric_aggregator = None

        # ---- Statistics ----
        self.stats = {
            "total_tasks": 0,
            "delayed": 0,
            "forwarded": 0,
            "cache_hits": 0,
            "bandit_decisions": 0,
            "lp_fallbacks": 0,
            "bio_expansions": 0,
            "total_reward": 0.0,
        }

        # ---- Load persistent state ----
        self._load_state()

    # --------------------- Persistence ---------------------
    def _load_state(self):
        """Load router, cache, bandit, and carbon scheduler state from JSON."""
        if not os.path.exists(self.persistence_file):
            return
        try:
            with open(self.persistence_file, "r") as f:
                data = json.load(f)
                # Restore cache
                if "cache" in data:
                    self.cache.store = {tuple(k): tuple(v) for k, v in data["cache"]}
                    self.cache.vectors = [np.array(v) for v in data["cache_vectors"]]
                    self.cache.keys = [tuple(k) for k in data["cache_keys"]]
                # Restore bandit state (weights, covariances, trials, action_space)
                if "bandit" in data:
                    self.bandit.state.action_weights = {
                        tuple(k): np.array(v) for k, v in data["bandit"]["weights"]
                    }
                    self.bandit.state.action_covariances = {
                        tuple(k): np.array(v) for k, v in data["bandit"]["covariances"]
                    }
                    self.bandit.state.action_trials = {
                        tuple(k): v for k, v in data["bandit"]["trials"]
                    }
                    self.bandit.state.action_space = data["bandit"]["action_space"]
                # Restore carbon scheduler queue
                if "carbon_queue" in data:
                    for item in data["carbon_queue"]:
                        heapq.heappush(self.carbon_scheduler.queue,
                                       (item["time"], item["task"]))
                # Restore stats
                if "stats" in data:
                    self.stats.update(data["stats"])
            self.logger.info("Loaded state from %s", self.persistence_file)
        except Exception as e:
            self.logger.warning("Failed to load state: %s", e)

    def _save_state(self):
        """Save current state to JSON."""
        try:
            data = {
                "cache": [
                    (list(k), (list(v[0]), v[1], v[2])) for k, v in self.cache.store.items()
                ],
                "cache_vectors": [v.tolist() for v in self.cache.vectors],
                "cache_keys": [list(k) for k in self.cache.keys],
                "bandit": {
                    "weights": [
                        (list(k), v.tolist()) for k, v in self.bandit.state.action_weights.items()
                    ],
                    "covariances": [
                        (list(k), v.tolist()) for k, v in self.bandit.state.action_covariances.items()
                    ],
                    "trials": [
                        (list(k), v) for k, v in self.bandit.state.action_trials.items()
                    ],
                    "action_space": self.bandit.state.action_space,
                },
                "carbon_queue": [
                    {"time": t, "task": task} for t, task in self.carbon_scheduler.queue
                ],
                "stats": self.stats,
            }
            with open(self.persistence_file, "w") as f:
                json.dump(data, f)
            self.logger.debug("State saved.")
        except Exception as e:
            self.logger.warning("Failed to save state: %s", e)

    # --------------------- Fingerprint Generation (with MoE) ---------------------
    def _generate_fingerprint(self, task: Dict[str, Any]) -> WorkloadFingerprint:
        """Generate a fingerprint enriched by MoE context if available."""
        # Basic fingerprint from task attributes
        fp = WorkloadFingerprint(
            model_size_mb=task.get("model_size_mb", 0),
            prompt_len=task.get("prompt_len", 0),
            gen_len=task.get("gen_len", 0),
            gpu_mem_free_mb=task.get("gpu_mem_free_mb", 0),
            disk_speed_class=task.get("disk_speed_class", 1),
        )
        # Add hardware state if profiler is running
        if self.profiler:
            hw_state = self.profiler.get_current_metrics()
            task["hardware_state"] = hw_state  # store for later use
            # Use MoE to enrich the fingerprint
            context_vector = self.moe.encode(task, hw_state)
            # We could append context_vector to the fingerprint, but for now we store it in task
            task["moe_context"] = context_vector
        return fp

    # --------------------- Task Processing ---------------------
    def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main entry point with enhanced decision flow.
        """
        self.stats["total_tasks"] += 1

        # ---- Step 1: Carbon delay ----
        delay_result = self.carbon_scheduler.submit(task)
        if delay_result["status"] == "delayed":
            self.stats["delayed"] += 1
            return {
                "status": "deferred",
                "reason": delay_result.get("reason", "low_carbon_window"),
                "delay_until": delay_result["delay_until"],
            }

        self.stats["forwarded"] += 1

        # ---- Step 2: Generate fingerprint ----
        fp = self._generate_fingerprint(task)

        # ---- Step 3: Try cache (with MoE‑enriched fingerprint) ----
        cached_policy = self.cache.get_best_policy(fp)
        if cached_policy is not None:
            self.stats["cache_hits"] += 1
            final_policy = cached_policy
            policy_source = "cache"
        else:
            # ---- Step 4: Bandit selection ----
            bandit_policy, confidence = self.bandit.select_action(
                task,  # passes full task so bandit can use MoE context
                min_trials_before_bandit=self.min_trials,
                confidence_threshold=self.conf_threshold,
            )
            if bandit_policy is not None and confidence >= self.conf_threshold:
                self.stats["bandit_decisions"] += 1
                final_policy = bandit_policy
                policy_source = "bandit"
            else:
                # ---- Step 5: Fallback to LP solver ----
                self.stats["lp_fallbacks"] += 1
                final_policy = self.lp_solver(fp)
                policy_source = "lp_solver"
                # Seed bandit with this safe policy
                self.bandit.seed_safe_policy(task, final_policy, reward=1.0)

        # ---- Step 6: Execute (using MetricAggregator if available) ----
        if self.metric_aggregator:
            metrics = self.metric_aggregator.run(task, final_policy)
        else:
            # Fallback: executor returns raw metrics
            metrics = self.executor(task, final_policy)

        # ---- Step 7: Compute reward using MODP ----
        constraints = task.get("constraints", {})
        # Get carbon intensity for carbon efficiency
        carbon_intensity = self.carbon_scheduler.carbon_api.get_current()
        reward = self.reward_calc.compute(metrics, constraints, carbon_intensity)

        self.stats["total_reward"] += reward

        # ---- Step 8: Update learning components ----
        if policy_source == "bandit":
            self.bandit.update(task, final_policy, metrics)
        elif policy_source in ("cache", "lp_solver"):
            self.cache.update(fp, final_policy, reward)

        # ---- Step 9: Feed back to carbon scheduler ----
        # If the task was delayed, we could also adjust scheduler parameters.
        # We pass the reward to the carbon scheduler for bio‑inspired adaptation.
        self.carbon_scheduler.report_reward(task, reward)

        # ---- Step 10: Trigger bio‑inspired expansion if needed ----
        # If bandit confidence is low and we have enough trials, expand action space
        ctx_key = self.bandit._encode_context(task)
        n_trials = self.bandit.state.action_trials.get(ctx_key, 0)
        if n_trials > 10 and policy_source == "lp_solver":
            # Bandit is still falling back after many trials -> expand
            new_policies = self.bio.generate_policies(self.bandit.state.action_space, n=2)
            if new_policies:
                self.stats["bio_expansions"] += 1
                for p in new_policies:
                    if p not in self.bandit.state.action_space:
                        self.bandit.state.action_space.append(p)
                        # Extend bandit's internal structures for all contexts
                        for key in self.bandit.state.action_weights:
                            new_mean = np.append(self.bandit.state.action_weights[key], 0.0)
                            new_cov = np.zeros((len(self.bandit.state.action_weights[key])+1,
                                               len(self.bandit.state.action_weights[key])+1))
                            new_cov[:len(self.bandit.state.action_weights[key]),
                                    :len(self.bandit.state.action_weights[key])] = \
                                self.bandit.state.action_covariances[key]
                            new_cov[-1, -1] = 0.1
                            self.bandit.state.action_weights[key] = new_mean
                            self.bandit.state.action_covariances[key] = new_cov
                            self.bandit.state.action_rewards[key].append(0.0)
                self.logger.info("Bio‑inspired expansion: added %d new policies", len(new_policies))

        # ---- Save state periodically ----
        if self.stats["total_tasks"] % 10 == 0:
            self._save_state()

        return {
            "status": "completed",
            "policy": final_policy,
            "policy_source": policy_source,
            "metrics": metrics,
            "reward": reward,
        }

    def tick_carbon_queue(self) -> list:
        """Release delayed tasks whose time has come."""
        return self.carbon_scheduler.tick()

    def get_stats(self) -> Dict[str, Any]:
        """Return current statistics."""
        return {
            **self.stats,
            "cache_size": len(self.cache.store),
            "bandit_action_space_size": len(self.bandit.state.action_space),
            "carbon_queue_size": len(self.carbon_scheduler.queue),
            "bandit_num_contexts": len(self.bandit.state.action_weights),
        }

    def shutdown(self):
        """Stop profiler and save state."""
        if self.profiler:
            self.profiler.stop()
        self._save_state()
        self.logger.info("Shutdown complete.")

    def __del__(self):
        self.shutdown()


# ----------------------------------------------------------------------
# Example usage (simplified)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    # Dummy components for demonstration
    from .carbon_api_stub import CarbonAPIStub
    carbon_api = CarbonAPIStub()

    def lp_solver(fp):
        return {"gpu_batch_size": 1, "block_size": 8}

    def executor(task, policy):
        time.sleep(0.5)
        return {"tokens_generated": 20, "quality_score": 0.95}

    action_space = [
        {"gpu_batch_size": 1, "block_size": 8, "weight_device": "gpu"},
        {"gpu_batch_size": 2, "block_size": 16, "weight_device": "cpu"},
    ]

    router = GreenAgentPolicyRouter(
        carbon_api=carbon_api,
        action_space=action_space,
        lp_solver=lp_solver,
        executor=executor,
        persistence_file="router_test.json",
    )

    # Simulate a task
    task = {"model_size_mb": 35000, "prompt_len": 512, "gen_len": 32,
            "gpu_mem_free_mb": 12000, "disk_speed_class": 2}
    result = router.handle_task(task)
    print(result)
    print(router.get_stats())

    # Shutdown
    router.shutdown()
