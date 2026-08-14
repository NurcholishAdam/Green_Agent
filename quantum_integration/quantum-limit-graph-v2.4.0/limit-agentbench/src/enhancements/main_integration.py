"""
main_integration.py

Complete orchestration loop for the Green Agent with Enhancements 1, 2, 5.
"""
import time
import threading
from collections import deque

# Import all enhancements
from enhancements.carbon_delay_scheduler import CarbonDelayScheduler
from enhancements.carbon_api_stub import CarbonAPIStub
from enhancements.gpu_profiler import GPUProfiler
from enhancements.metric_aggregator import MetricAggregator
from enhancements.reward_calculator import RewardCalculator
from enhancements.green_agent_policy_router import GreenAgentPolicyRouter
from enhancements.policy_meta_cache import WorkloadFingerprint


def run_green_agent_main_loop():
    # ---- Setup ----
    carbon_api = CarbonAPIStub(base_intensity=180.0)
    gpu_profiler = GPUProfiler(sample_interval=0.5)
    gpu_profiler.start()

    # Your actual Phase 2 LP solver (stub here)
    def lp_solver(fp: WorkloadFingerprint):
        return {"gpu_batch_size": 1, "block_size": 8, "weight_device": "gpu"}

    # Your actual Phase 3 Executor (wrapped by MetricAggregator)
    def flexgen_executor(task, policy):
        # Simulate inference (replace with real FlexGen call)
        time.sleep(0.5)
        return {"tokens_generated": 20, "quality_score": 0.95}, {}

    metric_aggregator = MetricAggregator(gpu_profiler, flexgen_executor)
    reward_calc = RewardCalculator()

    ACTION_SPACE = [
        {"gpu_batch_size": 1, "block_size": 8, "weight_device": "gpu", "kv_cache_device": "gpu", "weight_bits": 16},
        {"gpu_batch_size": 2, "block_size": 16, "weight_device": "cpu", "kv_cache_device": "cpu", "weight_bits": 8},
    ]

    # ---- Instantiate Router ----
    router = GreenAgentPolicyRouter(
        carbon_api=carbon_api,
        action_space=ACTION_SPACE,
        lp_solver=lp_solver,
        executor=metric_aggregator.run,  # Pass the wrapped executor
        min_trials_before_bandit=5,      # Start safe, lower later
        confidence_threshold=0.6,
    )

    # ---- Task Queue for delayed tasks ----
    pending_queue = deque()

    def process_task(task):
        result = router.handle_task(task)
        if result["status"] == "deferred":
            print(f"[Deferred] Task delayed until {result['delay_until']}")
            pending_queue.append(result)  # store for later retry
        else:
            print(f"[Done] Policy: {result['policy_source']}, Reward: {result['reward']:.3f}")
            # Optionally update constraints/log based on metrics
        return result

    # ---- Background thread to release delayed tasks ----
    def release_checker():
        while True:
            time.sleep(30)  # check every 30 seconds
            released = router.tick_carbon_queue()
            for task in released:
                print(f"[Release] Carbon window opened, re-submitting task")
                process_task(task)

    release_thread = threading.Thread(target=release_checker, daemon=True)
    release_thread.start()

    # ---- Simulate incoming tasks ----
    tasks = [
        {"model_size_mb": 35000, "prompt_len": 512, "gen_len": 32,
         "gpu_mem_free_mb": 12000, "disk_speed_class": 2, "priority": "normal",
         "constraints": {"max_latency_ms": 5000, "min_quality": 0.8}},
        {"model_size_mb": 35000, "prompt_len": 512, "gen_len": 32,
         "gpu_mem_free_mb": 12000, "disk_speed_class": 2, "priority": "high"},
    ]

    for i, task in enumerate(tasks):
        print(f"\n--- Processing task {i} ---")
        process_task(task)

    # Simulate running for a while to see carbon delay kick in
    time.sleep(65)

    # ---- Shutdown ----
    gpu_profiler.stop()
    print("Shutdown complete.")


if __name__ == "__main__":
    run_green_agent_main_loop()
