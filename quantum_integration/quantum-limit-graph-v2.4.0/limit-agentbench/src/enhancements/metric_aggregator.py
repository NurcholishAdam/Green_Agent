"""
metric_aggregator.py

Enhanced wrapper for the FlexGen executor that captures accurate energy, latency,
and throughput metrics, and integrates with MODP, bio_inspired, moe_system,
LIMIT Graph, RLHF, and Multi‑Teacher Policy Distillation.

Features:
- MODP utility computation (multi‑objective reward).
- Bio‑inspired fitness evaluation.
- MoE context vector generation.
- LIMIT Graph constraint enforcement.
- RLHF preference updates.
- Multi‑Teacher Policy Distillation for policy refinement.
- Streaming callbacks for real‑time notification.
- Enhanced derived metrics using the profiler's built‑in methods.
"""

import time
from typing import Dict, Any, Callable, List, Optional

from .gpu_profiler import GPUProfiler

# Optional imports with fallback stubs
try:
    from .MODP import ParetoOptimizer
except ImportError:
    class ParetoOptimizer:
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)

try:
    from .bio_inspired import FitnessEvaluator
except ImportError:
    class FitnessEvaluator:
        def evaluate(self, metrics, policy):
            return 0.0

try:
    from .moe_system import ContextEncoder
except ImportError:
    class ContextEncoder:
        def encode(self, metrics):
            return [metrics.get("gpu_utilization_pct", 0),
                    metrics.get("cpu_utilization_pct", 0),
                    metrics.get("gpu_memory_used_mb", 0) / 1000]

try:
    from .limit_graph import LimitGraph
except ImportError:
    class LimitGraph:
        def __init__(self, *args, **kwargs): self.limits = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass

try:
    from .rlhf import RLHFOptimizer
except ImportError:
    class RLHFOptimizer:
        def __init__(self, action_space, *args, **kwargs): self.actions = action_space
        def update(self, context, action, reward): pass
        def sample_action(self, context): return self.actions[0] if self.actions else None

try:
    from .multi_teacher_policy_distillation import MultiTeacherDistiller
except ImportError:
    class MultiTeacherDistiller:
        def __init__(self, teachers, *args, **kwargs): self.teachers = teachers
        def distill(self, context): return self.teachers[0](context) if self.teachers else None


class MetricAggregator:
    """
    Enhanced metric aggregator with MODP, bio, MoE, LIMIT Graph, RLHF,
    and Multi‑Teacher Policy Distillation integration.
    """

    def __init__(
        self,
        gpu_profiler: GPUProfiler,
        executor_fn: Callable,
        modp_weights: Optional[Dict[str, float]] = None,
        bio_evaluator: Optional[Any] = None,
        moe_encoder: Optional[Any] = None,
        enable_callbacks: bool = True,
        limit_graph: Optional[Any] = None,
        rlhf_optimizer: Optional[Any] = None,
        distiller: Optional[Any] = None,
    ):
        """
        Args:
            gpu_profiler: Enhanced GPUProfiler instance.
            executor_fn: The underlying FlexGen executor.
            modp_weights: Weights for MODP objectives.
            bio_evaluator: Fitness evaluator for bio‑inspired module.
            moe_encoder: Context encoder for MoE module.
            enable_callbacks: Whether to trigger callbacks after each run.
            limit_graph: Optional LIMIT Graph instance for constraint enforcement.
            rlhf_optimizer: Optional RLHF optimizer for preference updates.
            distiller: Optional MultiTeacherDistiller for policy refinement.
        """
        self.profiler = gpu_profiler
        self.executor = executor_fn

        # MODP
        self.modp = ParetoOptimizer()
        self.modp_weights = modp_weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy_efficiency": 0.20,
            "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10,
        }

        # Bio
        self.bio = bio_evaluator if bio_evaluator else FitnessEvaluator()

        # MoE
        self.moe = moe_encoder if moe_encoder else ContextEncoder()

        # New modules
        self.limit_graph = limit_graph if limit_graph else LimitGraph()
        self.rlhf = rlhf_optimizer  # can be None
        self.distiller = distiller

        # If distiller is None, create one with default teachers based on available modules
        if self.distiller is None:
            teachers = [
                self._teacher_modp_policy,
                self._teacher_bio_policy,
                self._teacher_moe_policy,
            ]
            self.distiller = MultiTeacherDistiller(teachers)

        # Callbacks
        self._callbacks = []  # list of (callback_fn, cooldown, last_call)
        self.enable_callbacks = enable_callbacks

        # Store last metrics for possible RLHF update
        self._last_metrics = None

    # --------------------- Teacher functions for distillation ---------------------
    def _teacher_modp_policy(self, context: Dict) -> Dict:
        """
        Teacher based on MODP: adjusts policy to favor high utility.
        Context should contain 'task' and 'policy'.
        """
        task = context.get('task', {})
        policy = context.get('policy', {}).copy()
        # Example: if carbon_intensity high, reduce max_tokens
        carbon = task.get('carbon_intensity_gco2_kwh', 200)
        if carbon > 300:
            policy['max_tokens'] = min(policy.get('max_tokens', 150), 100)
        return policy

    def _teacher_bio_policy(self, context: Dict) -> Dict:
        """
        Teacher based on bio‑inspired fitness.
        """
        task = context.get('task', {})
        policy = context.get('policy', {}).copy()
        # Use a simple rule based on model size
        if task.get('model_size_mb', 0) > 30000:
            policy['gpu_batch_size'] = 2
        return policy

    def _teacher_moe_policy(self, context: Dict) -> Dict:
        """
        Teacher based on MoE context.
        """
        # We don't have actual metrics yet, so use placeholders
        task = context.get('task', {})
        policy = context.get('policy', {}).copy()
        # Example: adjust based on prompt length
        if task.get('prompt_len', 0) > 512:
            policy['block_size'] = min(policy.get('block_size', 8), 4)
        return policy

    # --------------------- LIMIT Graph enforcement ---------------------
    def _apply_limit_graph(self, task: Dict, policy: Dict) -> Dict:
        """Apply LIMIT Graph constraints to the policy."""
        if self.limit_graph is None:
            return policy
        context = {
            'task': task,
            'policy': policy,
            'model_size_mb': task.get('model_size_mb', 0),
            'prompt_len': task.get('prompt_len', 0),
            'gpu_mem_free_mb': task.get('gpu_mem_free_mb', 0),
        }
        limits = self.limit_graph.get_limits(context)
        # Clamp numeric policy fields based on limits
        for key, val in limits.items():
            if key == 'max_gpu_batch_size' and 'gpu_batch_size' in policy:
                policy['gpu_batch_size'] = min(policy['gpu_batch_size'], val)
            elif key == 'max_block_size' and 'block_size' in policy:
                policy['block_size'] = min(policy['block_size'], val)
            elif key == 'max_max_tokens' and 'max_tokens' in policy:
                policy['max_tokens'] = min(policy['max_tokens'], val)
            # Add more mappings as needed
        return policy

    # --------------------- Distillation-based policy selection ---------------------
    def _select_policy_with_distillation(self, task: Dict, policy: Dict) -> Dict:
        """Use Multi‑Teacher Distillation to refine the policy."""
        if self.distiller is None or not self.distiller.teachers:
            return policy
        context = {'task': task, 'policy': policy}
        refined_policy = self.distiller.distill(context)
        if refined_policy is None:
            return policy
        # Merge with original (only override keys present in refined)
        merged = policy.copy()
        merged.update(refined_policy)
        return merged

    # --------------------- Callback System ---------------------
    def register_callback(
        self, callback: Callable[[Dict[str, Any]], None], cooldown: float = 0.1
    ):
        """Register a function to be called after each run with aggregated metrics."""
        self._callbacks.append((callback, cooldown, 0.0))

    def _trigger_callbacks(self, metrics: Dict[str, Any]):
        """Call registered callbacks, respecting cooldowns."""
        now = time.time()
        for i, (cb, cd, last) in enumerate(self._callbacks):
            if now - last >= cd:
                try:
                    cb(metrics)
                except Exception as e:
                    print(f"Callback error: {e}")
                self._callbacks[i] = (cb, cd, now)

    # --------------------- Core Run Method ---------------------
    def run(self, task: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute inference and capture real metrics.
        Returns aggregated metrics dictionary.
        """
        # ---- Refine policy using distillation + LIMIT Graph ----
        refined_policy = self._select_policy_with_distillation(task, policy)
        refined_policy = self._apply_limit_graph(task, refined_policy)

        # ---- Pre‑execution snapshot ----
        start_metrics = self.profiler.get_current_metrics()
        start_time = time.time()

        # ---- Execute inference ----
        try:
            output, raw_inference_metrics = self.executor(task, refined_policy)
            success = True
        except Exception as e:
            output = None
            raw_inference_metrics = {"error": str(e)}
            success = False

        # ---- Post‑execution snapshot ----
        end_metrics = self.profiler.get_current_metrics()
        end_time = time.time()

        # ---- Compute derived metrics ----
        elapsed_sec = end_time - start_time
        tokens_generated = raw_inference_metrics.get("tokens_generated", 0)
        tokens_per_sec = tokens_generated / elapsed_sec if elapsed_sec > 0 else 0.0

        gpu_energy_joules = end_metrics.get("energy_gpu_joules", 0.0) - start_metrics.get("energy_gpu_joules", 0.0)
        cpu_energy_joules = end_metrics.get("energy_cpu_joules", 0.0) - start_metrics.get("energy_cpu_joules", 0.0)
        total_energy_kwh = (gpu_energy_joules + cpu_energy_joules) / 3600.0 / 1000.0

        gpu_total = end_metrics.get("gpu_memory_total_mb", 1.0)
        gpu_used = end_metrics.get("gpu_memory_used_mb", 0.0)
        memory_efficiency = gpu_used / gpu_total if gpu_total > 0 else 0.0

        avg_gpu_power = (start_metrics.get("gpu_power_watts", 0.0) + end_metrics.get("gpu_power_watts", 0.0)) / 2.0

        carbon_intensity = task.get("carbon_intensity_gco2_kwh", 200.0)
        carbon_kg = total_energy_kwh * carbon_intensity / 1000.0

        metrics = {
            "success": success,
            "output": output,
            "inference_metrics": raw_inference_metrics,
            "elapsed_sec": elapsed_sec,
            "tokens_per_sec": tokens_per_sec,
            "total_energy_kwh": total_energy_kwh,
            "gpu_energy_joules": gpu_energy_joules,
            "cpu_energy_joules": cpu_energy_joules,
            "gpu_power_avg_watts": avg_gpu_power,
            "gpu_memory_peak_mb": max(start_metrics.get("gpu_memory_used_mb", 0),
                                      end_metrics.get("gpu_memory_used_mb", 0)),
            "memory_efficiency": memory_efficiency,
            "carbon_kg": carbon_kg,
            "gpu_oom": (not success and "CUDA out of memory" in str(raw_inference_metrics.get("error", ""))),
            "start_metrics": start_metrics,
            "end_metrics": end_metrics,
            "energy_efficiency": end_metrics.get("energy_efficiency", 0.0),
            "carbon_efficiency": end_metrics.get("carbon_efficiency", 0.0),
            "quality_score": raw_inference_metrics.get("quality_score", 1.0),
            # Include the refined policy that was actually used
            "refined_policy": refined_policy,
        }

        # Store last metrics
        self._last_metrics = metrics

        # ---- RLHF update (if available) ----
        if self.rlhf is not None:
            # Build context from metrics and task
            context = {
                'task': task,
                'metrics': metrics,
                'refined_policy': refined_policy,
            }
            # Compute reward as MODP utility (or simple scalar)
            reward = self.compute_modp_utility(metrics) if success else -1.0
            # Action is the refined policy encoded as string
            action = str(sorted(refined_policy.items()))
            self.rlhf.update(context, action, reward)

        # ---- LIMIT Graph feedback (if available) ----
        if self.limit_graph is not None:
            feedback = {
                'task': task,
                'policy': refined_policy,
                'metrics': metrics,
                'success': success,
            }
            self.limit_graph.update_from_feedback(feedback)

        # ---- Trigger callbacks ----
        if self.enable_callbacks:
            self._trigger_callbacks(metrics)

        return metrics

    # --------------------- Integration Interfaces ---------------------
    def compute_modp_utility(self, metrics: Optional[Dict[str, Any]] = None) -> float:
        """
        Compute a scalar utility using MODP weights from the given metrics.
        If no metrics provided, uses the most recent run's metrics (if available).
        """
        if metrics is None:
            if self._last_metrics is None:
                raise ValueError("Must provide metrics or run the executor first.")
            metrics = self._last_metrics
        objectives = {
            "quality": metrics.get("quality_score", 1.0),
            "throughput": metrics.get("tokens_per_sec", 0.0) / 100.0,
            "energy_efficiency": metrics.get("energy_efficiency", 0.0),
            "carbon_efficiency": metrics.get("carbon_efficiency", 0.0),
            "memory_efficiency": metrics.get("memory_efficiency", 0.0),
        }
        return self.modp.evaluate(objectives, self.modp_weights)

    def compute_bio_fitness(self, metrics: Dict[str, Any], policy: Dict[str, Any]) -> float:
        """
        Delegate to the bio‑inspired fitness evaluator.
        """
        return self.bio.evaluate(metrics, policy)

    def get_moe_context(self, metrics: Dict[str, Any]) -> List[float]:
        """
        Generate a context vector for the MoE router.
        """
        return self.moe.encode(metrics)

    # --------------------- Utility ---------------------
    def get_last_run_metrics(self) -> Optional[Dict[str, Any]]:
        """
        Return the metrics from the most recent run (if stored).
        """
        return self._last_metrics

    def get_current_policy(self) -> Optional[Dict[str, Any]]:
        """Return the last refined policy used (if any)."""
        if self._last_metrics:
            return self._last_metrics.get("refined_policy")
        return None
