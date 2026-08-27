"""
main_integration.py

Complete orchestration loop for Green Agent with Enhancements 1, 2, 5,
plus integration with bio_inspired, moe_system, MODP, LIMIT Graph, RLHF,
and Multi‑Teacher Policy Distillation.

All classes are defined within this single file for easy deployment.
"""
import time
import threading
import heapq
import json
import os
import random
from collections import deque
from typing import Dict, Any, List, Optional, Tuple, Callable

import numpy as np
import psutil

# ----------------------------------------------------------------------
# 1. External module integrations (with graceful fallback)
# ----------------------------------------------------------------------
try:
    from enhancements.limit_graph import LimitGraph
except ImportError:
    class LimitGraph:
        def __init__(self, *args, **kwargs): self.limits = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass

try:
    from enhancements.rlhf import RLHFOptimizer
except ImportError:
    class RLHFOptimizer:
        def __init__(self, action_space, *args, **kwargs): self.actions = action_space
        def update(self, context, action, reward): pass
        def sample_action(self, context): return self.actions[0] if self.actions else None

try:
    from enhancements.multi_teacher_policy_distillation import MultiTeacherDistiller
except ImportError:
    class MultiTeacherDistiller:
        def __init__(self, teachers, *args, **kwargs): self.teachers = teachers
        def distill(self, context): return self.teachers[0](context) if self.teachers else None

try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
except ImportError:
    class GeneticPolicyGenerator:
        def __init__(self, *args, **kwargs): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}

try:
    from enhancements.moe_system import ExpertRouter
except ImportError:
    class ExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context): return [0.0]*5
        def select(self, encoded): return "default"

# ----------------------------------------------------------------------
# 2. Core Enhancement Classes (all defined here)
# ----------------------------------------------------------------------

# --------------------- CarbonDelayScheduler ---------------------
class CarbonDelayScheduler:
    """Schedules tasks to run during lower‑carbon periods."""
    def __init__(self, carbon_api, max_delay_seconds=3600, threshold_gco2_per_kwh=150.0):
        self.carbon_api = carbon_api
        self.max_delay = max_delay_seconds
        self.threshold = threshold_gco2_per_kwh
        self.queue = []  # min‑heap of (scheduled_timestamp, task)

    def submit(self, task):
        if task.get("priority") == "high":
            return {"status": "forward", "task": task, "delay_until": None}

        current_intensity = self.carbon_api.get_current()
        if current_intensity <= self.threshold:
            return {"status": "forward", "task": task, "delay_until": None}

        forecast_minutes = self.max_delay // 60 + 1
        forecast = self.carbon_api.get_forecast(forecast_minutes)
        if not forecast:
            return {"status": "forward", "task": task, "delay_until": None}

        now = time.time()
        best_time = None
        for ts, intensity in forecast:
            if intensity < self.threshold:
                best_time = ts
                break

        if best_time is None or (best_time - now) > self.max_delay:
            return {"status": "forward", "task": task, "delay_until": None}

        heapq.heappush(self.queue, (best_time, task))
        return {"status": "delayed", "task": task, "delay_until": best_time}

    def tick(self):
        now = time.time()
        released = []
        while self.queue and self.queue[0][0] <= now:
            _, task = heapq.heappop(self.queue)
            released.append(task)
        return released


# --------------------- CarbonAPIStub ---------------------
class CarbonAPIStub:
    """Mock carbon intensity API for testing."""
    def __init__(self, base_intensity=200.0, volatility=50.0):
        self.base = base_intensity
        self.volatility = volatility
        self._start_time = time.time()

    def get_current(self):
        cycle = (time.time() - self._start_time) / 3600.0
        intensity = self.base + self.volatility * (0.5 * (1 + (2 * 3.14159 * cycle / 12))) + random.gauss(0, 5)
        return max(50.0, intensity)

    def get_forecast(self, minutes=60):
        now = time.time()
        forecast = []
        for i in range(0, minutes, 10):
            ts = now + i * 60
            cycle = (ts - self._start_time) / 3600.0
            intensity = self.base + self.volatility * (0.5 * (1 + (2 * 3.14159 * cycle / 12))) + random.gauss(0, 5)
            forecast.append((ts, max(50.0, intensity)))
        return forecast


# --------------------- GPUProfiler ---------------------
try:
    import pynvml
    NVML_AVAILABLE = True
    pynvml.nvmlInit()
except (ImportError, pynvml.NVMLError):
    NVML_AVAILABLE = False

class GPUProfiler:
    """Collects GPU, CPU, and Disk metrics."""
    def __init__(self, sample_interval_sec=0.5):
        self.sample_interval = sample_interval_sec
        self._running = False
        self._thread = None
        self._latest_metrics = {}
        self._disk_io_start = psutil.disk_io_counters()
        self._last_disk_time = time.time()

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    def _sample_loop(self):
        while self._running:
            self._latest_metrics = self._snapshot()
            time.sleep(self.sample_interval)

    def _snapshot(self):
        metrics = {}
        if NVML_AVAILABLE:
            try:
                device_count = pynvml.nvmlDeviceGetCount()
                if device_count > 0:
                    handle = pynvml.nvmlDeviceGetHandleByIndex(0)
                    mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                    util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                    power = pynvml.nvmlDeviceGetPowerUsage(handle) / 1000.0
                    metrics["gpu_name"] = pynvml.nvmlDeviceGetName(handle)
                    metrics["gpu_memory_total_mb"] = mem_info.total / 1024**2
                    metrics["gpu_memory_free_mb"] = mem_info.free / 1024**2
                    metrics["gpu_memory_used_mb"] = mem_info.used / 1024**2
                    metrics["gpu_utilization_pct"] = util.gpu / 100.0
                    metrics["gpu_power_watts"] = power
                    metrics["gpu_temp_c"] = pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU)
            except Exception:
                pass
        else:
            metrics["gpu_available"] = False

        vm = psutil.virtual_memory()
        metrics["cpu_memory_total_mb"] = vm.total / 1024**2
        metrics["cpu_memory_free_mb"] = vm.available / 1024**2
        metrics["cpu_utilization_pct"] = psutil.cpu_percent(interval=None) / 100.0

        now = time.time()
        disk_io = psutil.disk_io_counters()
        if self._disk_io_start and now - self._last_disk_time > 0.5:
            delta_time = now - self._last_disk_time
            read_bytes = disk_io.read_bytes - self._disk_io_start.read_bytes
            write_bytes = disk_io.write_bytes - self._disk_io_start.write_bytes
            metrics["disk_read_bandwidth_gbps"] = (read_bytes / delta_time) * 8 / 1e9
            metrics["disk_write_bandwidth_gbps"] = (write_bytes / delta_time) * 8 / 1e9
        self._disk_io_start = disk_io
        self._last_disk_time = now
        return metrics

    def get_current_metrics(self):
        if self._running:
            return self._latest_metrics.copy()
        return self._snapshot()


# --------------------- MetricAggregator ---------------------
class MetricAggregator:
    """Wraps the FlexGen executor and captures real‑time metrics."""
    def __init__(self, gpu_profiler: GPUProfiler, executor_fn: Callable):
        self.profiler = gpu_profiler
        self.executor = executor_fn

    def run(self, task: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
        start_metrics = self.profiler.get_current_metrics()
        start_time = time.time()
        start_gpu_energy = start_metrics.get("gpu_power_watts", 0.0) * start_time

        try:
            output, raw_inference_metrics = self.executor(task, policy)
            success = True
        except Exception as e:
            output = None
            raw_inference_metrics = {"error": str(e)}
            success = False

        end_metrics = self.profiler.get_current_metrics()
        end_time = time.time()

        elapsed = end_time - start_time
        avg_power = (start_metrics.get("gpu_power_watts", 0.0) + end_metrics.get("gpu_power_watts", 0.0)) / 2.0
        total_energy_kwh = (avg_power * elapsed) / 3600.0 / 1000.0

        tokens_generated = raw_inference_metrics.get("tokens_generated", 0)
        tokens_per_sec = tokens_generated / elapsed if elapsed > 0 else 0.0

        gpu_total = end_metrics.get("gpu_memory_total_mb", 1.0)
        gpu_used = end_metrics.get("gpu_memory_used_mb", 0.0)
        memory_efficiency = gpu_used / gpu_total if gpu_total > 0 else 0.0

        return {
            "success": success,
            "output": output,
            "inference_metrics": raw_inference_metrics,
            "elapsed_sec": elapsed,
            "tokens_per_sec": tokens_per_sec,
            "total_energy_kwh": total_energy_kwh,
            "gpu_power_avg_watts": avg_power,
            "gpu_memory_peak_mb": max(start_metrics.get("gpu_memory_used_mb", 0),
                                      end_metrics.get("gpu_memory_used_mb", 0)),
            "memory_efficiency": memory_efficiency,
            "gpu_oom": (not success and "CUDA out of memory" in str(raw_inference_metrics.get("error", ""))),
            "real_metrics": end_metrics
        }


# --------------------- RewardCalculator ---------------------
class RewardCalculator:
    def __init__(self, weights: Dict[str, float] = None):
        self.weights = weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy_efficiency": 0.20,
            "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10,
        }

    def compute(self, aggregated_metrics: Dict[str, Any],
                constraints: Dict[str, Any],
                carbon_intensity_gco2_kwh: float = 0.0) -> float:
        quality = aggregated_metrics.get("quality_score", 1.0)
        throughput = aggregated_metrics.get("tokens_per_sec", 0.0)
        total_energy_kwh = aggregated_metrics.get("total_energy_kwh", 0.0)
        mem_eff = aggregated_metrics.get("memory_efficiency", 0.0)
        oom = aggregated_metrics.get("gpu_oom", False)

        if throughput > 0 and total_energy_kwh > 0:
            carbon_per_token = (total_energy_kwh * carbon_intensity_gco2_kwh) / throughput
            carbon_eff = max(0.0, 1.0 - (carbon_per_token / 100.0))
        else:
            carbon_eff = 0.0

        if total_energy_kwh > 0 and throughput > 0:
            energy_eff = min(1.0, throughput / (total_energy_kwh * 1000))
        else:
            energy_eff = 0.0

        penalty = 0.0
        if oom:
            penalty -= 10.0
        max_latency = constraints.get("max_latency_ms", 1e9)
        if aggregated_metrics.get("elapsed_sec", 0) * 1000 > max_latency:
            penalty -= 5.0
        if quality < constraints.get("min_quality", 0.5):
            penalty -= 5.0

        reward = (
            self.weights["quality"] * quality +
            self.weights["throughput"] * min(1.0, throughput / 100.0) +
            self.weights["energy_efficiency"] * energy_eff +
            self.weights["carbon_efficiency"] * carbon_eff +
            self.weights["memory_efficiency"] * mem_eff
        ) + penalty
        return max(-10.0, min(10.0, reward))


# --------------------- WorkloadFingerprint ---------------------
class WorkloadFingerprint:
    def __init__(self, model_size_mb: float, prompt_len: int, gen_len: int,
                 gpu_mem_free_mb: float, disk_speed_class: int):
        self.model_size_mb = model_size_mb
        self.prompt_len = prompt_len
        self.gen_len = gen_len
        self.gpu_mem_free_mb = gpu_mem_free_mb
        self.disk_speed_class = disk_speed_class

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.model_size_mb / 1000.0,
            self.prompt_len / 1024.0,
            self.gen_len / 1024.0,
            self.gpu_mem_free_mb / 1000.0,
            self.disk_speed_class / 2.0,
        ])


# --------------------- PolicyMetaCache ---------------------
class PolicyMetaCache:
    def __init__(self, max_age_hours: float = 24.0, dist_threshold: float = 0.2):
        self.max_age_seconds = max_age_hours * 3600
        self.dist_threshold = dist_threshold
        self.store = {}  # key: tuple(vector) -> (policy, timestamp, avg_reward)
        self.vectors = []
        self.keys = []

    def _vector_to_key(self, vec: np.ndarray) -> tuple:
        return tuple(vec.tolist())

    def get_best_policy(self, fp: WorkloadFingerprint) -> Optional[Dict[str, Any]]:
        vec = fp.to_vector()
        if not self.vectors:
            return None

        best_idx = -1
        best_dist = float('inf')
        for i, stored_vec in enumerate(self.vectors):
            dist = np.linalg.norm(vec - stored_vec)
            if dist < best_dist:
                best_dist = dist
                best_idx = i

        if best_idx == -1 or best_dist > self.dist_threshold:
            return None

        key = self.keys[best_idx]
        policy, timestamp, avg_reward = self.store[key]
        if (time.time() - timestamp) > self.max_age_seconds:
            return None
        return policy

    def update(self, fp: WorkloadFingerprint, policy: Dict[str, Any], reward: float):
        vec = fp.to_vector()
        key = self._vector_to_key(vec)
        if key in self.store:
            old_policy, old_ts, old_reward = self.store[key]
            if reward > old_reward:
                self.store[key] = (policy, time.time(), reward)
        else:
            self.store[key] = (policy, time.time(), reward)
            self.vectors.append(vec)
            self.keys.append(key)


# --------------------- ContextualBandit ---------------------
class ContextualBandit:
    def __init__(self, action_space: List[Dict[str, Any]], fallback_solver: Callable):
        self.actions = action_space
        self.fallback_solver = fallback_solver
        self.num_actions = len(action_space)
        self.weights = {}  # context key -> np.ndarray
        self.trials = {}   # context key -> int

    def _encode_context(self, fp: WorkloadFingerprint) -> tuple:
        return tuple(fp.to_vector().tolist())

    def select_action(self, fp: WorkloadFingerprint,
                      min_trials_before_bandit: int = 5,
                      confidence_threshold: float = 0.6) -> Tuple[Optional[Dict], float]:
        ctx_key = self._encode_context(fp)
        n_trials = self.trials.get(ctx_key, 0)

        if n_trials < min_trials_before_bandit:
            return None, 0.0

        weights = self.weights[ctx_key]
        std = 1.0 / np.sqrt(n_trials + 1)
        sampled = np.random.normal(weights, std)
        best_idx = np.argmax(sampled)

        confidence = 1.0 - (1.0 / (n_trials + 1))
        if confidence < confidence_threshold:
            return None, 0.0
        return self.actions[best_idx], confidence

    def update(self, fp: WorkloadFingerprint, action: Dict[str, Any], reward: float):
        ctx_key = self._encode_context(fp)
        if ctx_key not in self.weights:
            self.weights[ctx_key] = np.zeros(self.num_actions)
            self.trials[ctx_key] = 0

        try:
            action_idx = self.actions.index(action)
        except ValueError:
            return

        n = self.trials[ctx_key]
        lr = 0.1 / (n + 1)
        old_weight = self.weights[ctx_key][action_idx]
        self.weights[ctx_key][action_idx] = old_weight + lr * (reward - old_weight)
        self.trials[ctx_key] += 1

    def seed_safe_policy(self, fp: WorkloadFingerprint, policy: Dict[str, Any]):
        ctx_key = self._encode_context(fp)
        if ctx_key not in self.weights:
            self.weights[ctx_key] = np.zeros(self.num_actions)
            self.trials[ctx_key] = 0
        for i, act in enumerate(self.actions):
            if act == policy:
                self.weights[ctx_key][i] = 1.0
                break


# --------------------- GreenAgentPolicyRouter (Enhanced) ---------------------
class GreenAgentPolicyRouter:
    def __init__(self, carbon_api, action_space: list, lp_solver: Callable,
                 executor: Callable, min_trials_before_bandit: int = 5,
                 confidence_threshold: float = 0.6,
                 # New optional modules
                 limit_graph=None, rlhf=None, distiller=None,
                 moe_router=None, bio_generator=None):
        self.carbon_scheduler = CarbonDelayScheduler(carbon_api)
        self.cache = PolicyMetaCache()
        self.bandit = ContextualBandit(action_space, lp_solver)
        self.lp_solver = lp_solver
        self.executor = executor
        self.min_trials = min_trials_before_bandit
        self.conf_threshold = confidence_threshold

        # New modules
        self.limit_graph = limit_graph
        self.rlhf = rlhf
        self.distiller = distiller
        self.moe_router = moe_router
        self.bio_generator = bio_generator

        # For distillation, teachers are functions returning a policy
        if self.distiller is not None:
            self.distiller.teachers = [
                self._teacher_cache,
                self._teacher_bandit,
                self._teacher_lp,
                self._teacher_moe,   # optional, may return None
            ]

    def _teacher_cache(self, fp: WorkloadFingerprint):
        return self.cache.get_best_policy(fp)

    def _teacher_bandit(self, fp: WorkloadFingerprint):
        policy, confidence = self.bandit.select_action(
            fp,
            min_trials_before_bandit=self.min_trials,
            confidence_threshold=self.conf_threshold
        )
        return policy if policy is not None and confidence >= self.conf_threshold else None

    def _teacher_lp(self, fp: WorkloadFingerprint):
        return self.lp_solver(fp)

    def _teacher_moe(self, fp: WorkloadFingerprint):
        """Use MoE expert to suggest a policy (if available)."""
        if self.moe_router is None:
            return None
        # For demonstration, just return the LP policy or a random action from bandit's action space.
        # In a real system, the MoE would map context to an expert's recommended policy.
        # Here we return a default policy based on the fingerprint's model size.
        if fp.model_size_mb > 30000:
            return {"gpu_batch_size": 2, "block_size": 16, "weight_device": "cpu", "kv_cache_device": "cpu", "weight_bits": 8}
        else:
            return {"gpu_batch_size": 1, "block_size": 8, "weight_device": "gpu", "kv_cache_device": "gpu", "weight_bits": 16}

    def _generate_fingerprint(self, task: Dict[str, Any]) -> WorkloadFingerprint:
        return WorkloadFingerprint(
            model_size_mb=task.get("model_size_mb", 0),
            prompt_len=task.get("prompt_len", 0),
            gen_len=task.get("gen_len", 0),
            gpu_mem_free_mb=task.get("gpu_mem_free_mb", 0),
            disk_speed_class=task.get("disk_speed_class", 1),
        )

    def _apply_limit_graph(self, fp: WorkloadFingerprint, policy: Dict) -> Dict:
        """Apply LIMIT Graph constraints to a policy."""
        if self.limit_graph is None:
            return policy
        context = {"fingerprint": fp.to_vector().tolist()}
        limits = self.limit_graph.get_limits(context)
        # Example constraint: block_size max
        if "max_block_size" in limits and "block_size" in policy:
            policy["block_size"] = min(policy["block_size"], limits["max_block_size"])
        # Example: force weight_device if specified
        if "forced_weight_device" in limits:
            policy["weight_device"] = limits["forced_weight_device"]
        return policy

    def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        # Step 1: Carbon delay
        delay_result = self.carbon_scheduler.submit(task)
        if delay_result["status"] == "deferred":
            return {
                "status": "deferred",
                "reason": "low_carbon_window",
                "delay_until": delay_result["delay_until"],
            }

        # Step 2: Fingerprint
        fp = self._generate_fingerprint(task)

        # Step 3: Policy selection via distillation (if available)
        if self.distiller is not None:
            # Collect non‑None policies from teachers
            policies = []
            for teacher in self.distiller.teachers:
                p = teacher(fp)
                if p is not None:
                    policies.append(p)
            if policies:
                # For simplicity, choose the first policy; in a real distiller, we would weight them.
                # Here we mimic distillation by selecting the most common or simply the first.
                # We'll use a simple voting: choose the policy that appears most frequently.
                from collections import Counter
                policy_counter = Counter(json.dumps(p, sort_keys=True) for p in policies)
                most_common = policy_counter.most_common(1)[0][0]
                final_policy = json.loads(most_common)
                policy_source = "distillation"
            else:
                final_policy = self.lp_solver(fp)
                policy_source = "lp_solver"
        else:
            # Step 4: Try cache
            cached_policy = self.cache.get_best_policy(fp)
            if cached_policy is not None:
                final_policy = cached_policy
                policy_source = "cache"
            else:
                # Step 5: Bandit
                bandit_policy, confidence = self.bandit.select_action(
                    fp, min_trials_before_bandit=self.min_trials,
                    confidence_threshold=self.conf_threshold
                )
                if bandit_policy is not None and confidence >= self.conf_threshold:
                    final_policy = bandit_policy
                    policy_source = "bandit"
                else:
                    # Step 6: LP solver
                    final_policy = self.lp_solver(fp)
                    policy_source = "lp_solver"
                    self.bandit.seed_safe_policy(fp, final_policy)

        # Step 7: Apply LIMIT Graph constraints
        final_policy = self._apply_limit_graph(fp, final_policy)

        # Step 8: Execute
        metrics = self.executor(task, final_policy)

        # Return result; reward will be computed in main loop
        return {
            "status": "completed",
            "policy": final_policy,
            "policy_source": policy_source,
            "metrics": metrics,
            "fingerprint": fp,
        }

    def update_after_task(self, fp: WorkloadFingerprint, policy: Dict, reward: float,
                          policy_source: str):
        """Update learning components after reward is known."""
        # Update bandit
        if policy_source in ("bandit", "distillation"):
            # If distillation was used, the policy may not match any bandit action exactly,
            # so we skip if not found; else update.
            if policy in self.bandit.actions:
                self.bandit.update(fp, policy, reward)
            else:
                # For distillation, we could update the closest action, but we skip for simplicity.
                pass
        elif policy_source in ("cache", "lp_solver"):
            self.cache.update(fp, policy, reward)

        # Update RLHF if available
        if self.rlhf is not None:
            context = fp.to_vector().tolist()
            action_index = self.bandit.actions.index(policy) if policy in self.bandit.actions else -1
            if action_index >= 0:
                self.rlhf.update(context, action_index, reward)

        # Bio‑inspired expansion
        if self.bio_generator is not None and policy_source == "lp_solver":
            ctx_key = self.bandit._encode_context(fp)
            n_trials = self.bandit.trials.get(ctx_key, 0)
            if n_trials > 10:
                # Generate new policies via bio‑inspired evolution
                new_actions = self.bio_generator.evolve(
                    population=self.bandit.actions,
                    fitness_fn=lambda p: random.uniform(0,1),  # placeholder
                    generations=2,
                    population_size=len(self.bandit.actions)*2
                )
                # Add new actions that are not already present
                for new_policy in new_actions:
                    if new_policy not in self.bandit.actions:
                        self.bandit.actions.append(new_policy)
                        # Extend bandit weights for new action
                        for key in self.bandit.weights:
                            self.bandit.weights[key] = np.append(
                                self.bandit.weights[key], 0.0
                            )
                        # (In real implementation, we'd also update covariance, etc.)
                print("[Bio] Action space expanded with new policies.")

    def tick_carbon_queue(self) -> list:
        return self.carbon_scheduler.tick()


# ----------------------------------------------------------------------
# 3. Main Orchestration Loop (with all enhancements)
# ----------------------------------------------------------------------

STATE_FILE = "green_agent_state.json"

def run_green_agent_main_loop():
    # ---- Setup ----
    carbon_api = CarbonAPIStub(base_intensity=180.0)
    gpu_profiler = GPUProfiler(sample_interval=0.5)
    gpu_profiler.start()

    def lp_solver(fp: WorkloadFingerprint):
        return {"gpu_batch_size": 1, "block_size": 8, "weight_device": "gpu"}

    def flexgen_executor(task, policy):
        time.sleep(0.5)
        return {"tokens_generated": 20, "quality_score": 0.95}, {}

    metric_aggregator = MetricAggregator(gpu_profiler, flexgen_executor)
    reward_calc = RewardCalculator()

    ACTION_SPACE = [
        {"gpu_batch_size": 1, "block_size": 8, "weight_device": "gpu", "kv_cache_device": "gpu", "weight_bits": 16},
        {"gpu_batch_size": 2, "block_size": 16, "weight_device": "cpu", "kv_cache_device": "cpu", "weight_bits": 8},
    ]

    # ---- Instantiate new modules ----
    limit_graph = LimitGraph()
    # Build a simple graph with constraints (example)
    limit_graph.build_graph(nodes=[], edges=[])
    # We can set some default limits via update_from_feedback or set_limits, but for demo we assume get_limits returns {} initially.

    rlhf = RLHFOptimizer(action_space=list(range(len(ACTION_SPACE))))

    distiller = MultiTeacherDistiller(teachers=[])  # teachers set inside router

    moe_router = ExpertRouter()  # may be a stub

    bio_generator = GeneticPolicyGenerator()  # may be a stub

    router = GreenAgentPolicyRouter(
        carbon_api=carbon_api,
        action_space=ACTION_SPACE,
        lp_solver=lp_solver,
        executor=metric_aggregator.run,
        min_trials_before_bandit=5,
        confidence_threshold=0.6,
        limit_graph=limit_graph,
        rlhf=rlhf,
        distiller=distiller,
        moe_router=moe_router,
        bio_generator=bio_generator,
    )

    # Load persistent state if exists
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
            router.bandit.weights = {tuple(k): np.array(v) for k, v in state["weights"]}
            router.bandit.trials = {tuple(k): v for k, v in state["trials"]}
        print(f"Loaded state from {STATE_FILE}")

    def process_task(task):
        # Adjust reward weights based on task priority (MODP integration)
        if task.get("priority") == "eco":
            reward_calc.weights = {
                "quality": 0.1,
                "throughput": 0.1,
                "energy_efficiency": 0.4,
                "carbon_efficiency": 0.4,
                "memory_efficiency": 0.0,
            }
        elif task.get("priority") == "speed":
            reward_calc.weights = {
                "quality": 0.2,
                "throughput": 0.5,
                "energy_efficiency": 0.1,
                "carbon_efficiency": 0.1,
                "memory_efficiency": 0.1,
            }

        # MoE routing (optional)
        # (In a real system, we might use moe_router here to alter task or provide context.
        #  For now, it's just a placeholder.)

        result = router.handle_task(task)
        if result["status"] == "deferred":
            print(f"[Deferred] Task delayed until {result['delay_until']}")
            return result

        # Compute reward
        fp = result["fingerprint"]
        carbon_intensity = carbon_api.get_current()
        metrics = result["metrics"]
        constraints = task.get("constraints", {})
        reward = reward_calc.compute(metrics, constraints, carbon_intensity)
        result["reward"] = reward
        print(f"[Done] Policy: {result['policy_source']}, Reward: {reward:.3f}")

        # Update learning components
        router.update_after_task(fp, result["policy"], reward, result["policy_source"])

        return result

    def release_checker():
        while True:
            time.sleep(30)
            released = router.tick_carbon_queue()
            for task in released:
                print(f"[Release] Carbon window opened, re-submitting task")
                process_task(task)

    release_thread = threading.Thread(target=release_checker, daemon=True)
    release_thread.start()

    # Simulate tasks
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

    time.sleep(65)

    # Persist state
    with open(STATE_FILE, "w") as f:
        state = {
            "weights": [([float(x) for x in k], v.tolist()) for k, v in router.bandit.weights.items()],
            "trials": [([float(x) for x in k], v) for k, v in router.bandit.trials.items()]
        }
        json.dump(state, f)
    gpu_profiler.stop()
    print("Shutdown complete. State persisted.")


if __name__ == "__main__":
    run_green_agent_main_loop()
