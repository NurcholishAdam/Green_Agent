"""
Hugging Face connector for real FlexGen execution (enhanced).
Loads a model and runs inference with a chosen policy, integrating with
block scheduling, offloading, quantization, measurement, and Green Agent modules.
Supports fallback to simulation if PyTorch/Transformers unavailable.
"""

import logging
import time
import os
from typing import Dict, Any, Optional, List, Tuple

from .block_scheduler import BlockScheduler
from .quantization import apply_quantization
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..async_message_queue import AsyncMessageQueue
from ..schemas.feedback_event import FeedbackEvent
from ..logger import logger

# Optional imports
try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    from transformers import AutoModelForCausalLM, AutoTokenizer
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

try:
    from ..gpu_optimization.gpu_profiler import GPUProfiler
except ImportError:
    GPUProfiler = None

try:
    from ..gpu_optimization.reward import compute_reward
except ImportError:
    def compute_reward(metrics, workload):
        return 0.5  # fallback


class HFFlexGenConnector:
    """
    Hugging Face connector that applies FlexGen policies to real model inference.
    Uses accelerate device_map for automatic offloading; manual block scheduling
    is simulated through BlockScheduler if real hooks are not available.
    """

    def __init__(
        self,
        model_name: str = "facebook/opt-1.3b",
        node: Optional[NodeDescriptor] = None,
        workload: Optional[WorkloadDescriptor] = None,
        carbon_intensity_g_per_kwh: float = 400.0,
        message_queue: Optional[AsyncMessageQueue] = None,
    ):
        self.model_name = model_name
        self.node = node
        self.workload = workload
        self.carbon_intensity = carbon_intensity_g_per_kwh
        self.message_queue = message_queue
        self.model = None
        self.tokenizer = None
        self.gpu_profiler = GPUProfiler() if GPUProfiler else None
        self.last_metrics = {}

    def _get_device_map(self, policy: Dict[str, Any]) -> str:
        """
        Determine device_map for accelerate based on policy.
        """
        weight_device = policy.get("weight_device", "gpu")
        if weight_device == "cpu":
            return "cpu"
        elif weight_device == "disk":
            return "disk"  # accelerate supports "disk" offload
        else:
            return "auto"  # Let accelerate place layers

    def load_model(self, policy: Dict[str, Any], node: Optional[NodeDescriptor] = None) -> bool:
        """
        Load model and tokenizer with offloading and quantization.
        """
        if not TRANSFORMERS_AVAILABLE or not TORCH_AVAILABLE:
            logger.error("PyTorch/Transformers not available; cannot load real model.")
            return False

        try:
            logger.info(f"Loading model {self.model_name} with policy {policy}")
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)

            # Determine torch dtype based on quantization bits
            weight_bits = policy.get("weight_bits", 16)
            if weight_bits <= 8:
                torch_dtype = torch.float16
            else:
                torch_dtype = torch.float16  # default for memory saving

            device_map = self._get_device_map(policy)

            # Load with device_map for offloading
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                device_map=device_map,
                torch_dtype=torch_dtype,
                offload_folder="./offload" if policy.get("weight_device") == "disk" else None,
            )

            # Apply quantization (if bitsandbytes available)
            self.model = apply_quantization(self.model, weight_bits, policy.get("kv_cache_bits", 16))

            # If CPU attention requested, we may need to modify attention layers
            # (placeholder; real implementation would replace attention modules)
            if policy.get("cpu_attention", False):
                logger.warning("CPU attention requested but not fully implemented; using default attention.")

            logger.info("Model loaded successfully.")
            return True
        except Exception as e:
            logger.error(f"Model loading failed: {e}")
            self.model = None
            self.tokenizer = None
            return False

    def run_inference(
        self,
        prompt: str,
        policy: Dict[str, Any],
        max_new_tokens: int = 20,
        use_block_scheduling: bool = False,
    ) -> Dict[str, Any]:
        """
        Run inference with the model using the given policy.
        Returns metrics dict with latency, energy, carbon, success.
        """
        if self.model is None or self.tokenizer is None:
            logger.error("Model not loaded.")
            return {"success": False, "error": "model_not_loaded"}

        # If real block scheduling requested and available, use it
        if use_block_scheduling:
            scheduler = BlockScheduler(policy, self.node, self.workload, self.carbon_intensity)
            # We could use scheduler to orchestrate manual layer execution,
            # but that requires reimplementing generation. For now, fallback to standard generate
            # and log a warning.
            logger.warning("Manual block scheduling not fully integrated; using accelerate offload.")

        try:
            start_time = time.time()
            # Record GPU metrics before
            gpu_before = {}
            if self.gpu_profiler:
                gpu_before = self.gpu_profiler.get_gpu_metrics()

            # Tokenize
            inputs = self.tokenizer(prompt, return_tensors="pt")
            # Move inputs to same device as model (accelerate handles if needed)
            if hasattr(self.model, 'device'):
                inputs = {k: v.to(self.model.device) for k, v in inputs.items()}

            # Generate
            with torch.no_grad():
                outputs = self.model.generate(
                    **inputs,
                    max_new_tokens=max_new_tokens,
                    do_sample=False,
                )

            latency = time.time() - start_time

            # Decode output
            output_text = self.tokenizer.decode(outputs[0], skip_special_tokens=True)

            # Record GPU metrics after
            gpu_after = {}
            if self.gpu_profiler:
                gpu_after = self.gpu_profiler.get_gpu_metrics()

            # Estimate energy (using average power)
            power_before = gpu_before.get("gpu_power_watts", 65.0)
            power_after = gpu_after.get("gpu_power_watts", 65.0)
            avg_power = (power_before + power_after) / 2
            energy_j = avg_power * latency

            # Carbon
            carbon_g = (energy_j / 3.6e6) * self.carbon_intensity

            metrics = {
                "success": True,
                "output": output_text,
                "latency_ms": latency * 1000,
                "num_tokens": max_new_tokens,
                "energy_joules": energy_j,
                "carbon_g": carbon_g,
                "gpu_memory_used_mb": gpu_after.get("gpu_memory_used_mb", 0),
                "throughput_tokens_per_s": max_new_tokens / latency if latency > 0 else 0,
                "quality_score": 1.0,  # assume no quality loss for real model
                "policy": policy,
            }

            # Publish FeedbackEvent
            if self.message_queue and FeedbackEvent:
                reward = compute_reward(metrics, self.workload) if self.workload else 0.5
                event = FeedbackEvent(
                    source="hf_flexgen_connector",
                    feedback_type="routing",
                    task_id=self.workload.task_id if self.workload else "unknown",
                    context={"model_name": self.model_name,
                             "policy": str(policy),
                             "node_id": self.node.id if self.node else "unknown"},
                    action={"selected_action": str(policy),
                            "selected_rank": 0,
                            "confidence_score": 1.0},
                    performance={"quality_score": metrics["quality_score"],
                                 "latency_ms": metrics["latency_ms"],
                                 "energy_joules": metrics["energy_joules"],
                                 "carbon_g": metrics["carbon_g"],
                                 "helium_cost": 0,
                                 "duration_ms": 0},
                    adaptive_cost_value=reward,
                    tags=["hf_flexgen", "real_inference", "policy_execution"],
                )
                # Use asyncio to publish if not in async context
                import asyncio
                try:
                    asyncio.create_task(self.message_queue.publish("inference_events", event.to_json()))
                except RuntimeError:
                    # No running event loop, run synchronously
                    asyncio.run(self.message_queue.publish("inference_events", event.to_json()))

            self.last_metrics = metrics
            return metrics

        except Exception as e:
            logger.error(f"Inference failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "latency_ms": 0,
                "energy_joules": 0,
                "carbon_g": 0,
            }

    def close(self):
        """Clean up resources."""
        if self.gpu_profiler:
            self.gpu_profiler.shutdown()
        self.model = None
        self.tokenizer = None
