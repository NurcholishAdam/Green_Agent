#!/usr/bin/env python3
"""
Enhanced FlexGen-style block scheduler (v2.5.0)
================================================
Simulates layer-wise offloading and asynchronous transfer for Green Agent.
Integrates with FlexGenPolicy, NodeDescriptor, WorkloadDescriptor, cost model,
AsyncMessageQueue, FeedbackEvent, and reward computation.

FIXES OVER v2.0:
- Relative imports wrapped with fallbacks; module imports cleanly even when
  optional siblings are missing.
- Symmetric memory accounting:
    * CPU-side weight memory is now released at the end of each block.
    * GPU memory is only decremented for tensors that were actually added.
    * Current GPU/CPU memory is clamped to >= 0.
- KV-cache disk path charges both legs (disk -> cpu, cpu -> gpu).
- Activations on CPU are charged a cpu -> gpu transfer before compute.
- Energy model separates GPU, CPU, and disk contributions.
- Error path returns a fixed schema with inf metrics (not zeros).
- Input validation for block_size, weight_bits, kv_cache_bits, batch size.
- `self.tokens` is consistently derived from the workload when present,
  otherwise from `batch_size * seq_len`.
- Fast-path feasibility check for obviously infeasible policies.
- `publish_events` flag; non-blocking fire-and-forget publishing.
- `model_size_gb`, `num_layers`, `hidden_dim`, `seq_len` configurable.
- Prometheus metrics (optional).
- `seed`, `overlap_efficiency` configurable.
- `get_stats()` helper.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter as _PromCounter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- structlog / stdlib logger ----------
try:
    import structlog
    _logger = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _logger = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Logging shim that works with structlog or stdlib logging."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_logger, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(_logger, level)(message)


# ---------- Relative imports with fallbacks ----------
try:
    from .flexgen_policy import FlexGenPolicy  # type: ignore
except ImportError:  # pragma: no cover
    FlexGenPolicy = Any  # type: ignore

try:
    from ..schemas.node_descriptor import NodeDescriptor  # type: ignore
except ImportError:  # pragma: no cover
    NodeDescriptor = Any  # type: ignore

try:
    from ..schemas.workload_descriptor import WorkloadDescriptor  # type: ignore
except ImportError:  # pragma: no cover
    WorkloadDescriptor = Any  # type: ignore

try:
    from ..async_message_queue import AsyncMessageQueue  # type: ignore
except ImportError:  # pragma: no cover
    AsyncMessageQueue = Any  # type: ignore

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:  # pragma: no cover
    FeedbackEvent = Any  # type: ignore

try:
    from ..logger import logger  # type: ignore
except ImportError:  # pragma: no cover
    logger = _logger  # type: ignore


# ---------- Reward ----------
try:
    from ..gpu_optimization.reward import compute_reward  # type: ignore
except ImportError:  # pragma: no cover
    def compute_reward(metrics: Dict[str, Any], workload: Any) -> float:
        """Fallback reward: quality, latency satisfaction, energy, carbon, memory."""
        weights = {
            "quality": 0.3,
            "throughput": 0.25,
            "energy": 0.2,
            "carbon": 0.15,
            "memory": 0.1,
        }
        latency_target = max(float(getattr(workload, "latency_target", 1.0) or 1.0), 1.0)
        latency_ms = float(metrics.get("latency_ms", latency_target) or latency_target)
        energy_joules = float(metrics.get("energy_joules", 100.0) or 100.0)
        carbon_g = float(metrics.get("carbon_g", 10.0) or 10.0)

        latency_score = max(0.0, 1.0 - latency_ms / latency_target)
        energy_score = max(0.0, 1.0 - energy_joules / 100.0)
        carbon_score = max(0.0, 1.0 - carbon_g / 10.0)
        memory_score = 1.0 if metrics.get("success", True) else 0.0
        quality = float(metrics.get("quality_score", 0.9))

        reward = (
            weights["quality"] * quality
            + weights["throughput"] * latency_score
            + weights["energy"] * energy_score
            + weights["carbon"] * carbon_score
            + weights["memory"] * memory_score
        )
        return max(0.0, min(1.0, reward))


# ==============================================================================
# Helpers
# ==============================================================================
def _as_float(
    value: Any,
    default: float,
    name: Optional[str] = None,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        log_event("warning", f"Invalid {name or 'value'}: {value!r}; using {default}")
        return default
    if min_val is not None and v < min_val:
        log_event("warning", f"{name or 'value'} {v} < {min_val}; clamping to {min_val}")
        return min_val
    if max_val is not None and v > max_val:
        log_event("warning", f"{name or 'value'} {v} > {max_val}; clamping to {max_val}")
        return max_val
    return v


def _as_int(
    value: Any,
    default: int,
    name: Optional[str] = None,
    min_val: Optional[int] = None,
) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        log_event("warning", f"Invalid {name or 'value'}: {value!r}; using {default}")
        return default
    if min_val is not None and v < min_val:
        log_event("warning", f"{name or 'value'} {v} < {min_val}; clamping to {min_val}")
        return min_val
    return v


# Module-level Prometheus metrics (created once to avoid duplicate registration).
if PROMETHEUS_AVAILABLE:
    _M_SIMULATIONS = _PromCounter(
        "block_scheduler_simulations_total",
        "BlockScheduler simulations",
        ["success"],
    )
    _M_LATENCY = Histogram(
        "block_scheduler_latency_seconds",
        "Simulated latency",
    )
    _M_ENERGY = Histogram(
        "block_scheduler_energy_joules",
        "Simulated energy",
    )
    _M_CARBON = Histogram(
        "block_scheduler_carbon_g",
        "Simulated carbon",
    )
    _M_PEAK_GPU = Gauge(
        "block_scheduler_peak_gpu_memory_gb",
        "Peak GPU memory",
    )
    _M_PEAK_CPU = Gauge(
        "block_scheduler_peak_cpu_memory_gb",
        "Peak CPU memory",
    )
    _M_TRANSFER = Histogram(
        "block_scheduler_transfer_seconds",
        "Transfer time",
    )
    _M_COMPUTE = Histogram(
        "block_scheduler_compute_seconds",
        "Compute time",
    )
    _M_PUBLISH_FAILURES = _PromCounter(
        "block_scheduler_publish_failures_total",
        "Failed event publishes",
    )
    _M_ERRORS = _PromCounter(
        "block_scheduler_errors_total",
        "Simulation errors",
    )
else:  # pragma: no cover
    _M_SIMULATIONS = _M_LATENCY = _M_ENERGY = _M_CARBON = None
    _M_PEAK_GPU = _M_PEAK_CPU = _M_TRANSFER = _M_COMPUTE = None
    _M_PUBLISH_FAILURES = _M_ERRORS = None


# ==============================================================================
# BlockScheduler
# ==============================================================================
class BlockScheduler:
    """
    Simulates the zig-zag block schedule for a given policy and hardware.
    Produces metrics that can be used for policy evaluation and learning.
    """

    def __init__(
        self,
        policy: FlexGenPolicy,
        node: NodeDescriptor,
        workload: WorkloadDescriptor,
        carbon_intensity: float = 400.0,
        message_queue: Optional[AsyncMessageQueue] = None,
        *,
        # Model architecture (overridable)
        num_layers: int = 32,
        hidden_dim: int = 4096,
        seq_len: int = 512,
        model_size_gb_fp16: float = 14.0,
        # Behaviour flags
        overlap_efficiency: float = 0.5,
        publish_events: bool = True,
        seed: Optional[int] = None,
        enable_prometheus: bool = True,
    ):
        self.policy = policy
        self.node = node
        self.workload = workload
        self.carbon_intensity = _as_float(
            carbon_intensity, 400.0, name="carbon_intensity", min_val=0.0
        )
        self.message_queue = message_queue
        self.publish_events = bool(publish_events)
        self._enable_prometheus = bool(enable_prometheus) and PROMETHEUS_AVAILABLE

        self._seed = seed if seed is not None else random.randint(0, 2**31 - 1)
        self._rng = random.Random(self._seed)

        # --- Policy validation ---
        self.block_size = _as_int(
            getattr(policy, "block_size", 8), 8, name="block_size", min_val=1
        )
        self.weight_bits = _as_int(
            getattr(policy, "weight_bits", 16), 16, name="weight_bits", min_val=1
        )
        self.kv_cache_bits = _as_int(
            getattr(policy, "kv_cache_bits", 16), 16, name="kv_cache_bits", min_val=1
        )
        self.gpu_batch_size = _as_int(
            getattr(policy, "gpu_batch_size", 1), 1, name="gpu_batch_size", min_val=1
        )

        # --- Node capabilities (safe access) ---
        md = self._extract_node_metadata(node)
        self.gpu_memory_gb = _as_float(md.get("gpu_memory_gb", 16.0), 16.0, "gpu_memory_gb", min_val=0.0)
        self.cpu_memory_gb = _as_float(md.get("cpu_memory_gb", 64.0), 64.0, "cpu_memory_gb", min_val=0.0)
        self.gpu_cpu_bandwidth_gbps = _as_float(
            md.get("gpu_cpu_bandwidth_gbps", 12.0), 12.0, "gpu_cpu_bandwidth_gbps", min_val=1e-6
        )
        self.disk_bandwidth_gbps = _as_float(
            md.get("disk_bandwidth_gbps", 2.0), 2.0, "disk_bandwidth_gbps", min_val=1e-6
        )

        # --- Model architecture (overridable) ---
        self.num_layers = _as_int(num_layers, 32, "num_layers", min_val=1)
        self.hidden_dim = _as_int(hidden_dim, 4096, "hidden_dim", min_val=1)
        self.seq_len = _as_int(seq_len, 512, "seq_len", min_val=1)

        # Model size scales inversely with quantization bits.
        self.model_size_gb = max(0.0, float(model_size_gb_fp16)) * (16.0 / self.weight_bits)

        # --- Tokens ---
        # If the workload supplies an explicit token count, use it. Otherwise
        # use the per-forward-pass token count (batch * seq_len).
        wl_tokens = _as_int(getattr(workload, "tokens", 0) or 0, 0, "workload.tokens", min_val=0)
        if wl_tokens > 0:
            self.tokens = wl_tokens
        else:
            self.tokens = self.gpu_batch_size * self.seq_len

        # --- KV cache ---
        bytes_per_elem = self.kv_cache_bits / 8.0
        self.kv_cache_gb = (
            self.gpu_batch_size
            * self.seq_len
            * self.hidden_dim
            * self.num_layers
            * 2  # key + value
            * bytes_per_elem
        ) / 1e9

        self.activation_gb_per_token = 0.0001
        self.num_blocks = max(1, math.ceil(self.num_layers / self.block_size))

        # Peaks (public, preserved from v2.0).
        self.peak_gpu_mem_gb: float = 0.0
        self.peak_cpu_mem_gb: float = 0.0

        # Overlap
        self.overlap_efficiency = _as_float(
            overlap_efficiency, 0.5, "overlap_efficiency", min_val=0.0, max_val=1.0
        )

        # Bookkeeping for non-blocking publishes.
        self._pending_publish_tasks: set = set()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_node_metadata(node: Any) -> Dict[str, Any]:
        md = getattr(node, "metadata", None)
        if md is None:
            return {}
        if isinstance(md, dict):
            return md
        # Try object attribute access
        out: Dict[str, Any] = {}
        for key in (
            "gpu_memory_gb",
            "cpu_memory_gb",
            "gpu_cpu_bandwidth_gbps",
            "disk_bandwidth_gbps",
        ):
            if hasattr(md, key):
                out[key] = getattr(md, key)
        return out

    @staticmethod
    def _policy_dict(policy: Any) -> Dict[str, Any]:
        if hasattr(policy, "to_dict") and callable(policy.to_dict):
            try:
                return dict(policy.to_dict())
            except Exception:
                pass
        if is_dataclass(policy):
            return asdict(policy)
        return dict(getattr(policy, "__dict__", {}))

    def _transfer_time(self, size_gb: float, src: str, dst: str) -> float:
        """Estimate transfer time between devices in seconds."""
        if src == dst or size_gb <= 0:
            return 0.0
        if (src == "gpu" and dst == "cpu") or (src == "cpu" and dst == "gpu"):
            bandwidth = self.gpu_cpu_bandwidth_gbps
        elif src == "disk" or dst == "disk":
            bandwidth = self.disk_bandwidth_gbps
        else:
            bandwidth = self.gpu_cpu_bandwidth_gbps
        if bandwidth <= 0:
            bandwidth = 1.0
        return size_gb / bandwidth

    def _compute_block_time(self, block_layers: int, batch_size: int) -> float:
        """Estimate compute time for a block in seconds."""
        base_flops_per_token = 7e9
        total_tokens = batch_size * self.seq_len
        flops = block_layers * base_flops_per_token * total_tokens
        gpu_throughput = 30e12 * (self.weight_bits / 16.0)
        if gpu_throughput <= 0:
            gpu_throughput = 1.0
        return flops / gpu_throughput

    def _compute_energy(
        self,
        compute_time: float,
        transfer_time_gpu_cpu: float,
        transfer_time_disk: float,
    ) -> Tuple[float, float, float]:
        """
        Dynamic power model.
        Returns (energy_gpu_j, energy_cpu_j, energy_disk_j).
        """
        if self.policy.weight_device == "gpu":
            gpu_power = 250.0
        elif self.policy.weight_device == "cpu":
            gpu_power = 50.0
        else:
            gpu_power = 30.0
        cpu_power = 70.0
        disk_power = 15.0

        energy_gpu = gpu_power * compute_time
        energy_cpu = cpu_power * transfer_time_gpu_cpu
        energy_disk = disk_power * transfer_time_disk
        return energy_gpu, energy_cpu, energy_disk

    def _fast_path_infeasible(self) -> Optional[Dict[str, Any]]:
        """
        Return a failure dict if the policy is obviously infeasible, otherwise None.
        """
        if self.policy.weight_device == "gpu" and self.model_size_gb > self.gpu_memory_gb:
            return self._failure_metrics(
                f"model_size_gb ({self.model_size_gb:.3f}) > gpu_memory_gb ({self.gpu_memory_gb:.3f})"
            )
        if self.policy.kv_cache_device == "gpu" and self.kv_cache_gb > self.gpu_memory_gb:
            return self._failure_metrics(
                f"kv_cache_gb ({self.kv_cache_gb:.3f}) > gpu_memory_gb ({self.gpu_memory_gb:.3f})"
            )
        return None

    def _failure_metrics(self, reason: str) -> Dict[str, Any]:
        """Fixed-schema failure metrics. Uses inf where appropriate."""
        return {
            "success": False,
            "error": reason,
            "latency_ms": float("inf"),
            "energy_joules": float("inf"),
            "carbon_g": float("inf"),
            "gpu_memory_used_gb": 0.0,
            "cpu_memory_used_gb": 0.0,
            "throughput_tokens_per_s": 0.0,
            "quality_score": 0.0,
            "policy": self._policy_dict(self.policy),
            "num_tokens": self.tokens,
        }

    # ------------------------------------------------------------------
    # publishing
    # ------------------------------------------------------------------
    async def _publish(self, metrics: Dict[str, Any], reward: float) -> None:
        if self.message_queue is None:
            return
        try:
            policy_dict = self._policy_dict(self.policy)
            event = FeedbackEvent(
                source="block_scheduler",
                feedback_type="routing",
                task_id=getattr(self.workload, "task_id", None) or None,
                context={
                    "node_id": getattr(self.node, "id", None),
                    "policy": str(policy_dict),
                    "block_size": self.block_size,
                    "carbon_intensity": self.carbon_intensity,
                },
                action={
                    "selected_action": str(policy_dict),
                    "selected_rank": 0,
                    "confidence_score": 1.0,
                },
                performance={
                    "quality_score": float(metrics.get("quality_score", 0.0) or 0.0),
                    "latency_ms": float(metrics.get("latency_ms", 0.0) or 0.0),
                    "energy_joules": float(metrics.get("energy_joules", 0.0) or 0.0),
                    "carbon_g": float(metrics.get("carbon_g", 0.0) or 0.0),
                    "helium_cost": 0.0,
                    "duration_ms": 0.0,
                },
                adaptive_cost_value=float(reward),
                tags=["block_scheduler", "flexgen", "inference"],
            )
            payload = (
                event.to_json()
                if hasattr(event, "to_json") and callable(event.to_json)
                else json.dumps(event.to_dict() if hasattr(event, "to_dict") else {})
            )
            await self.message_queue.publish("inference_events", payload)
        except Exception as exc:
            log_event("warning", f"Failed to publish FeedbackEvent: {exc}")
            if self._enable_prometheus and _M_PUBLISH_FAILURES is not None:
                try:
                    _M_PUBLISH_FAILURES.inc()
                except Exception:
                    pass

    def _spawn_publish(self, metrics: Dict[str, Any], reward: float) -> None:
        if not self.publish_events or self.message_queue is None:
            return
        try:
            task = asyncio.create_task(self._publish(metrics, reward))
        except RuntimeError:
            return
        self._pending_publish_tasks.add(task)
        task.add_done_callback(self._pending_publish_tasks.discard)

    async def drain_publishes(self) -> None:
        """Await any pending publish tasks. Call before shutdown if desired."""
        if not self._pending_publish_tasks:
            return
        pending = list(self._pending_publish_tasks)
        await asyncio.gather(*pending, return_exceptions=True)
        self._pending_publish_tasks.clear()

    # ------------------------------------------------------------------
    # main
    # ------------------------------------------------------------------
    async def run_inference(self, inputs: Any = None) -> Dict[str, Any]:
        """
        Simulate the full inference with block scheduling.
        Returns a dict with metrics.
        """
        # `inputs` is reserved for future use (e.g. real activations).
        _ = inputs

        # Fast-path: obviously infeasible.
        fast_fail = self._fast_path_infeasible()
        if fast_fail is not None:
            if self._enable_prometheus and _M_SIMULATIONS is not None:
                try:
                    _M_SIMULATIONS.labels(success="False").inc()
                except Exception:
                    pass
            log_event(
                "info",
                "BlockScheduler fast-path failure",
                reason=fast_fail["error"],
            )
            return fast_fail

        try:
            current_gpu_mem = 0.0
            current_cpu_mem = 0.0
            total_transfer_time_gpu_cpu = 0.0
            total_transfer_time_disk = 0.0
            total_compute_time = 0.0

            for block_idx in range(self.num_blocks):
                block_layers = min(
                    self.block_size,
                    self.num_layers - block_idx * self.block_size,
                )
                if block_layers <= 0:
                    break

                weight_size_gb = (self.model_size_gb / self.num_layers) * block_layers
                activation_gb = (
                    block_layers * self.gpu_batch_size * self.activation_gb_per_token
                )
                kv_cache_block_gb = self.kv_cache_gb / self.num_blocks

                # ---------- Weights ----------
                if self.policy.weight_device == "gpu":
                    # Resident: accumulate on GPU.
                    current_gpu_mem += weight_size_gb
                elif self.policy.weight_device == "cpu":
                    current_cpu_mem += weight_size_gb
                    total_transfer_time_gpu_cpu += self._transfer_time(
                        weight_size_gb, "cpu", "gpu"
                    )
                elif self.policy.weight_device == "disk":
                    total_transfer_time_disk += self._transfer_time(
                        weight_size_gb, "disk", "cpu"
                    )
                    current_cpu_mem += weight_size_gb
                    total_transfer_time_gpu_cpu += self._transfer_time(
                        weight_size_gb, "cpu", "gpu"
                    )

                # ---------- Activations ----------
                if self.policy.activation_device == "gpu":
                    current_gpu_mem += activation_gb
                else:
                    current_cpu_mem += activation_gb
                    total_transfer_time_gpu_cpu += self._transfer_time(
                        activation_gb, "cpu", "gpu"
                    )

                # ---------- KV cache ----------
                if self.policy.kv_cache_device == "gpu":
                    current_gpu_mem += kv_cache_block_gb
                elif self.policy.kv_cache_device == "cpu":
                    current_cpu_mem += kv_cache_block_gb
                    total_transfer_time_gpu_cpu += self._transfer_time(
                        kv_cache_block_gb, "cpu", "gpu"
                    )
                elif self.policy.kv_cache_device == "disk":
                    total_transfer_time_disk += self._transfer_time(
                        kv_cache_block_gb, "disk", "cpu"
                    )
                    current_cpu_mem += kv_cache_block_gb
                    total_transfer_time_gpu_cpu += self._transfer_time(
                        kv_cache_block_gb, "cpu", "gpu"
                    )

                # ---------- Compute ----------
                total_compute_time += self._compute_block_time(
                    block_layers, self.gpu_batch_size
                )

                # ---------- Peaks ----------
                self.peak_gpu_mem_gb = max(self.peak_gpu_mem_gb, current_gpu_mem)
                self.peak_cpu_mem_gb = max(self.peak_cpu_mem_gb, current_cpu_mem)

                # ---------- Release at end of block (symmetric!) ----------
                # Activations are always transient.
                if self.policy.activation_device == "gpu":
                    current_gpu_mem -= activation_gb
                else:
                    current_cpu_mem -= activation_gb

                # KV cache is transient per block.
                if self.policy.kv_cache_device == "gpu":
                    current_gpu_mem -= kv_cache_block_gb
                else:
                    current_cpu_mem -= kv_cache_block_gb

                # Weights: transient unless gpu-resident.
                if self.policy.weight_device == "cpu":
                    current_cpu_mem -= weight_size_gb
                elif self.policy.weight_device == "disk":
                    current_cpu_mem -= weight_size_gb

                # Clamp to >= 0.
                current_gpu_mem = max(0.0, current_gpu_mem)
                current_cpu_mem = max(0.0, current_cpu_mem)

            # ---------- Feasibility ----------
            success = (
                self.peak_gpu_mem_gb <= self.gpu_memory_gb
                and self.peak_cpu_mem_gb <= self.cpu_memory_gb
            )

            # ---------- Timing ----------
            total_transfer_time = total_transfer_time_gpu_cpu + total_transfer_time_disk
            if self.policy.overlap_io_compute:
                total_time = (
                    total_compute_time
                    + total_transfer_time * (1.0 - self.overlap_efficiency)
                )
            else:
                total_time = total_compute_time + total_transfer_time

            total_tokens = self.tokens
            throughput = total_tokens / total_time if total_time > 0 else 0.0

            # ---------- Energy & carbon ----------
            e_gpu, e_cpu, e_disk = self._compute_energy(
                total_compute_time,
                total_transfer_time_gpu_cpu,
                total_transfer_time_disk,
            )
            energy_j = e_gpu + e_cpu + e_disk
            energy_kwh = energy_j / 3.6e6
            carbon_g = energy_kwh * self.carbon_intensity
            latency_ms = total_time * 1000.0

            metrics: Dict[str, Any] = {
                "success": success,
                "latency_ms": latency_ms,
                "energy_joules": energy_j,
                "carbon_g": carbon_g,
                "gpu_memory_used_gb": self.peak_gpu_mem_gb,
                "cpu_memory_used_gb": self.peak_cpu_mem_gb,
                "throughput_tokens_per_s": throughput,
                "quality_score": 0.9,
                "policy": self._policy_dict(self.policy),
                "num_tokens": total_tokens,
                # Extra diagnostics (harmless; callers can ignore them).
                "transfer_time_gpu_cpu_s": total_transfer_time_gpu_cpu,
                "transfer_time_disk_s": total_transfer_time_disk,
                "compute_time_s": total_compute_time,
                "energy_gpu_j": e_gpu,
                "energy_cpu_j": e_cpu,
                "energy_disk_j": e_disk,
            }

            # ---------- Publish (non-blocking) ----------
            if self.publish_events and self.message_queue is not None:
                try:
                    reward = float(compute_reward(metrics, self.workload))
                    self._spawn_publish(metrics, reward)
                except Exception as exc:
                    log_event("warning", f"Failed to enqueue FeedbackEvent: {exc}")

            # ---------- Metrics ----------
            if self._enable_prometheus:
                try:
                    _M_SIMULATIONS.labels(success=str(success)).inc()
                    _M_LATENCY.observe(total_time)
                    _M_ENERGY.observe(energy_j)
                    _M_CARBON.observe(carbon_g)
                    _M_PEAK_GPU.set(self.peak_gpu_mem_gb)
                    _M_PEAK_CPU.set(self.peak_cpu_mem_gb)
                    _M_TRANSFER.observe(total_transfer_time)
                    _M_COMPUTE.observe(total_compute_time)
                except Exception:
                    pass

            log_event(
                "info",
                "BlockScheduler inference completed",
                success=success,
                latency_ms=round(latency_ms, 2),
                energy_j=round(energy_j, 2),
                carbon_g=round(carbon_g, 6),
                peak_gpu_mem_gb=round(self.peak_gpu_mem_gb, 3),
                peak_cpu_mem_gb=round(self.peak_cpu_mem_gb, 3),
            )
            return metrics

        except Exception as exc:
            log_event("error", f"BlockScheduler simulation failed: {exc}")
            if self._enable_prometheus and _M_ERRORS is not None:
                try:
                    _M_ERRORS.inc()
                    _M_SIMULATIONS.labels(success="False").inc()
                except Exception:
                    pass
            return self._failure_metrics(f"simulation error: {exc}")

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------
    def get_stats(self) -> Dict[str, Any]:
        return {
            "block_size": self.block_size,
            "num_blocks": self.num_blocks,
            "num_layers": self.num_layers,
            "hidden_dim": self.hidden_dim,
            "seq_len": self.seq_len,
            "weight_bits": self.weight_bits,
            "kv_cache_bits": self.kv_cache_bits,
            "gpu_batch_size": self.gpu_batch_size,
            "model_size_gb": self.model_size_gb,
            "kv_cache_gb": self.kv_cache_gb,
            "tokens": self.tokens,
            "gpu_memory_gb": self.gpu_memory_gb,
            "cpu_memory_gb": self.cpu_memory_gb,
            "peak_gpu_mem_gb": self.peak_gpu_mem_gb,
            "peak_cpu_mem_gb": self.peak_cpu_mem_gb,
            "seed": self._seed,
            "carbon_intensity": self.carbon_intensity,
        }


# ==============================================================================
# Example usage
# ==============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    from dataclasses import dataclass

    @dataclass
    class _FakePolicy:
        block_size: int = 8
        weight_bits: int = 16
        kv_cache_bits: int = 16
        gpu_batch_size: int = 4
        weight_device: str = "cpu"
        activation_device: str = "gpu"
        kv_cache_device: str = "gpu"
        cpu_attention: bool = False
        overlap_io_compute: bool = True

        def to_dict(self):
            return asdict(self)

    class _FakeNode:
        id = "node-1"
        metadata = {
            "gpu_memory_gb": 24.0,
            "cpu_memory_gb": 128.0,
            "gpu_cpu_bandwidth_gbps": 12.0,
            "disk_bandwidth_gbps": 2.0,
        }

    class _FakeWorkload:
        task_id = "task-1"
        latency_target = 500.0
        tokens = 512

    async def _demo() -> None:
        policy = _FakePolicy()
        node = _FakeNode()
        workload = _FakeWorkload()

        scheduler = BlockScheduler(policy, node, workload, seed=42)
        metrics = await scheduler.run_inference()
        print("Metrics:", json.dumps(metrics, indent=2, default=str))
        print("Stats:", scheduler.get_stats())

        await scheduler.drain_publishes()

    asyncio.run(_demo())
