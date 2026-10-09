#!/usr/bin/env python3
"""
Enhanced Hugging Face connector for real FlexGen execution (v2.5.0).
===================================================================
Loads a model and runs inference with a chosen policy, integrating with
block scheduling, offloading, quantization, measurement, and Green Agent
modules. Falls back to simulation if PyTorch/Transformers are unavailable.

FIXES OVER v2.0:
- Relative imports wrapped in try/except with fallbacks.
- `device_map="disk"` replaced with a supported CPU device map + offload_folder.
- Feedback publishing is async-safe; sync path uses asyncio.run only when
  no loop is running, otherwise schedules a tracked task.
- Inference failure returns `success=False` with `inf` metrics (was silently
  substituting simulated success).
- `weight_bits`/`kv_cache_bits`/`group_size` validated (None-safe).
- GPU profiler sync calls refuse to run inside a live loop and log the reason.
- Actual generated-token count is read from the output tensor.
- `pad_token_id`/`eos_token_id` set on the tokenizer and passed to generate.
- `asyncio.get_running_loop()` used instead of deprecated `get_event_loop()`.
- `torch.cuda.OutOfMemoryError` handled distinctly from other errors.
- Energy uses the profiler's cumulative delta when available.
- `close()` frees CUDA cache and cleans up the offload folder.
- `__aenter__` / `__aexit__` / `aclose()` added.
- Optional Prometheus metrics.
- `use_block_scheduling=True` logs a warning when it is requested but not
  implemented (previously a silent no-op).
- `**kwargs` no longer ignored — reserved keys are documented and honoured.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import shutil
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Dict, Optional

# ---------- Prometheus (optional) ----------
try:
    from prometheus_client import Counter as _PromCounter, Histogram
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
    from .block_scheduler import BlockScheduler  # type: ignore
except ImportError:  # pragma: no cover
    BlockScheduler = Any  # type: ignore

try:
    from .quantization import apply_quantization  # type: ignore
except ImportError:  # pragma: no cover
    def apply_quantization(model: Any, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError(
            "apply_quantization is unavailable: the `quantization` module "
            "could not be imported."
        )

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


# ---------- Optional torch / transformers ----------
try:
    import torch  # type: ignore
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    torch = None  # type: ignore
    TORCH_AVAILABLE = False

try:
    from transformers import AutoModelForCausalLM, AutoTokenizer  # type: ignore
    TRANSFORMERS_AVAILABLE = True
except ImportError:  # pragma: no cover
    AutoModelForCausalLM = None  # type: ignore
    AutoTokenizer = None  # type: ignore
    TRANSFORMERS_AVAILABLE = False

try:
    from ..gpu_optimization.gpu_profiler import GPUProfiler  # type: ignore
except ImportError:  # pragma: no cover
    GPUProfiler = None  # type: ignore

try:
    from ..gpu_optimization.reward import compute_reward  # type: ignore
except ImportError:  # pragma: no cover
    def compute_reward(metrics: Dict[str, Any], workload: Any) -> float:
        """Fallback reward based on latency only."""
        return max(0.0, min(1.0, 1.0 - metrics.get("latency_ms", 500) / 1000.0))


# ---------- Prometheus metrics (module scope: single registration) ----------
if PROMETHEUS_AVAILABLE:
    _M_INFERENCES = _PromCounter(
        "hf_connector_inferences_total",
        "Hugging Face connector inferences",
        ["simulated", "success"],
    )
    _M_INFERENCE_SECONDS = Histogram(
        "hf_connector_inference_seconds",
        "Wall-clock inference time",
    )
    _M_ENERGY = Histogram(
        "hf_connector_energy_joules",
        "Estimated inference energy",
    )
    _M_CARBON = Histogram(
        "hf_connector_carbon_g",
        "Estimated inference carbon",
    )
    _M_LOAD_SECONDS = Histogram(
        "hf_connector_load_seconds",
        "Model load time",
    )
    _M_PUBLISH_FAILURES = _PromCounter(
        "hf_connector_publish_failures_total",
        "Failed FeedbackEvent publishes",
    )
else:  # pragma: no cover
    _M_INFERENCES = _M_INFERENCE_SECONDS = _M_ENERGY = _M_CARBON = None
    _M_LOAD_SECONDS = _M_PUBLISH_FAILURES = None


# ---------- Helpers ----------
def _as_int(
    value: Any,
    default: int,
    name: Optional[str] = None,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None,
) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        log_event("warning", f"Invalid {name or 'value'}: {value!r}; using {default}")
        return default
    if min_val is not None and v < min_val:
        return min_val
    if max_val is not None and v > max_val:
        return max_val
    return v


def _as_float(
    value: Any,
    default: float,
    name: Optional[str] = None,
    min_val: Optional[float] = None,
) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        log_event("warning", f"Invalid {name or 'value'}: {value!r}; using {default}")
        return default
    if not math.isfinite(v):
        log_event("warning", f"Non-finite {name or 'value'}: {v}; using {default}")
        return default
    if min_val is not None and v < min_val:
        return min_val
    return v


def _policy_dict(policy: Any) -> Dict[str, Any]:
    """Return a plain dict representation of a policy."""
    if isinstance(policy, dict):
        return dict(policy)
    if hasattr(policy, "to_dict") and callable(getattr(policy, "to_dict")):
        try:
            return dict(policy.to_dict())
        except Exception:
            pass
    if is_dataclass(policy) and not isinstance(policy, type):
        try:
            return asdict(policy)
        except Exception:
            pass
    try:
        return dict(getattr(policy, "__dict__", {}) or {})
    except Exception:
        return {}


# ==============================================================================
# HFFlexGenConnector
# ==============================================================================
class HFFlexGenConnector:
    """
    Hugging Face connector that applies FlexGen policies to real model inference.

    Uses accelerate device_map for automatic offloading. Falls back to a
    simulation mode if PyTorch / Transformers are unavailable, or if the real
    model fails to load.
    """

    MIN_MAX_NEW_TOKENS = 1
    MAX_MAX_NEW_TOKENS = 4096

    def __init__(
        self,
        model_name: str = "facebook/opt-1.3b",
        node: Optional[NodeDescriptor] = None,
        workload: Optional[WorkloadDescriptor] = None,
        carbon_intensity_g_per_kwh: float = 400.0,
        message_queue: Optional[AsyncMessageQueue] = None,
        **kwargs: Any,
    ):
        """
        Args:
            model_name: Hugging Face model identifier.
            node: Optional NodeDescriptor for hardware metadata.
            workload: Optional WorkloadDescriptor for reward computation.
            carbon_intensity_g_per_kwh: Grid intensity for carbon estimation.
            message_queue: Optional AsyncMessageQueue for FeedbackEvent publishing.

        Reserved kwargs (optional, all keyword-only):
            seed: RNG seed for future sampling support.
            strict: If True, raise instead of logging on unsupported options.
            enable_prometheus: Enable Prometheus metric recording.
            offload_dir: Directory for disk offload (default "./offload").
            publish_events: If False, never publish FeedbackEvents.
        """
        self.model_name: str = str(model_name or "facebook/opt-1.3b")
        self.node = node
        self.workload = workload
        self.carbon_intensity: float = _as_float(
            carbon_intensity_g_per_kwh, 400.0,
            name="carbon_intensity_g_per_kwh", min_val=0.0,
        )
        self.message_queue = message_queue

        self._seed = kwargs.get("seed")
        self._strict = bool(kwargs.get("strict", False))
        self._enable_prometheus = bool(kwargs.get("enable_prometheus", True)) and PROMETHEUS_AVAILABLE
        self._offload_dir = Path(kwargs.get("offload_dir", "./offload"))
        self._publish_events = bool(kwargs.get("publish_events", True))

        # Public state (preserved from v2.0)
        self.model: Any = None
        self.tokenizer: Any = None
        self.gpu_profiler: Any = None
        self.last_metrics: Dict[str, Any] = {}
        self.simulation_mode: bool = not (TRANSFORMERS_AVAILABLE and TORCH_AVAILABLE)

        # Internal state
        self._closed: bool = False
        self._load_policy: Optional[Dict[str, Any]] = None
        self._pending_publish_tasks: set = set()

        # Create the profiler if available.
        if GPUProfiler is not None:
            try:
                self.gpu_profiler = GPUProfiler(
                    carbon_intensity_g_per_kwh=self.carbon_intensity,
                    message_queue=None,  # publishing is handled by the connector
                )
            except Exception as exc:
                log_event("warning", f"Failed to create GPUProfiler: {exc}")
                self.gpu_profiler = None

        if self.simulation_mode:
            log_event(
                "warning",
                "PyTorch/Transformers not available; using simulation mode.",
            )
        else:
            log_event("info", f"Real model inference enabled for {self.model_name}")

    # ------------------------------------------------------------------
    # Device map / quantization helpers
    # ------------------------------------------------------------------
    def _resolve_device_map(
        self, policy_dict: Dict[str, Any]
    ) -> tuple[Any, Optional[str]]:
        """
        Translate the policy's weight_device into an accelerate device_map
        and an optional offload folder.

        accelerate does NOT accept the string "disk". Disk offload is
        signalled by a CPU device map plus `offload_folder`.
        """
        weight_device = str(policy_dict.get("weight_device", "gpu") or "gpu").lower()
        if weight_device == "disk":
            return {"": "cpu"}, str(self._offload_dir)
        if weight_device == "cpu":
            return {"": "cpu"}, None
        return "auto", None

    def _apply_cpu_attention(self, model: Any, policy_dict: Dict[str, Any]) -> Any:
        """Placeholder for CPU-attention replacement."""
        if policy_dict.get("cpu_attention", False):
            msg = (
                "CPU attention requested but not fully implemented; "
                "using default attention."
            )
            if self._strict:
                raise NotImplementedError(msg)
            log_event("warning", msg)
        return model

    def _select_torch_dtype(
        self, node: Optional[NodeDescriptor], weight_bits: int
    ) -> Any:
        """Choose a torch dtype based on hardware capabilities."""
        if not TORCH_AVAILABLE:
            return None
        try:
            if torch.cuda.is_available():
                if hasattr(torch.cuda, "is_bf16_supported") and torch.cuda.is_bf16_supported():
                    return torch.bfloat16
                return torch.float16
        except Exception:
            pass
        return torch.float32

    # ------------------------------------------------------------------
    # Model loading
    # ------------------------------------------------------------------
    def load_model(
        self,
        policy: Dict[str, Any],
        node: Optional[NodeDescriptor] = None,
    ) -> bool:
        """
        Load model and tokenizer with offloading and quantization.
        Returns True on success, False on failure. Simulation mode returns True
        without loading anything.
        """
        if self._closed:
            log_event("warning", "load_model called after close()")
            return False

        policy_dict = _policy_dict(policy)
        self._load_policy = policy_dict

        # ---- Simulation mode ----
        if self.simulation_mode:
            log_event("info", "Simulation mode: model loading simulated.")
            self.last_metrics = {"model_loaded": True, "simulated": True}
            return True

        if not (TRANSFORMERS_AVAILABLE and TORCH_AVAILABLE):
            log_event("error", "PyTorch/Transformers not available; cannot load real model.")
            return False

        try:
            start = time.monotonic()

            weight_bits = _as_int(policy_dict.get("weight_bits"), 16, "weight_bits", min_val=1)
            kv_bits = _as_int(policy_dict.get("kv_cache_bits"), 16, "kv_cache_bits", min_val=1)
            group_size = _as_int(policy_dict.get("group_size"), 64, "group_size", min_val=1)

            # Tokenizer
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            if getattr(self.tokenizer, "pad_token_id", None) is None:
                eos = getattr(self.tokenizer, "eos_token_id", None)
                if eos is not None:
                    self.tokenizer.pad_token_id = eos

            # dtype
            torch_dtype = self._select_torch_dtype(node or self.node, weight_bits)

            # Device map + offload folder
            device_map, offload_folder = self._resolve_device_map(policy_dict)
            if offload_folder:
                Path(offload_folder).mkdir(parents=True, exist_ok=True)

            load_kwargs: Dict[str, Any] = {
                "device_map": device_map,
            }
            if torch_dtype is not None:
                load_kwargs["torch_dtype"] = torch_dtype
            if offload_folder:
                load_kwargs["offload_folder"] = offload_folder
                load_kwargs["offload_state_dict"] = True

            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name, **load_kwargs,
            )

            # Quantization (only if requested)
            if weight_bits < 16 or kv_bits < 16:
                try:
                    self.model = apply_quantization(
                        self.model, weight_bits, kv_bits, group_size,
                    )
                except Exception as exc:
                    log_event("warning", f"Quantization failed: {exc}")
                    if self._strict:
                        raise

            # CPU attention placeholder
            self.model = self._apply_cpu_attention(self.model, policy_dict)

            # Evaluation mode
            try:
                if hasattr(self.model, "eval"):
                    self.model.eval()
            except Exception:
                pass

            elapsed = time.monotonic() - start
            if self._enable_prometheus and _M_LOAD_SECONDS is not None:
                try:
                    _M_LOAD_SECONDS.observe(elapsed)
                except Exception:
                    pass
            log_event("info", f"Model loaded successfully in {elapsed:.2f}s")
            return True

        except Exception as exc:
            log_event("error", f"Model loading failed: {exc}")
            self.model = None
            self.tokenizer = None
            self.simulation_mode = True
            log_event("info", "Falling back to simulation mode due to load failure.")
            return False

    # ------------------------------------------------------------------
    # Simulation
    # ------------------------------------------------------------------
    def _simulate_inference(
        self,
        prompt: str,
        policy_dict: Dict[str, Any],
        max_new_tokens: int,
    ) -> Dict[str, Any]:
        """Generate synthetic metrics based on policy and workload."""
        weight_device = str(policy_dict.get("weight_device", "gpu") or "gpu").lower()
        base_latency = 0.5
        if weight_device == "cpu":
            base_latency = 2.0
        elif weight_device == "disk":
            base_latency = 3.0

        bits = _as_int(policy_dict.get("weight_bits"), 16, "weight_bits", min_val=1)
        if bits <= 4:
            base_latency *= 0.6
        elif bits <= 8:
            base_latency *= 0.8
        if policy_dict.get("cpu_attention", False):
            base_latency *= 1.5

        latency = base_latency * max_new_tokens
        power = 100.0 if weight_device == "cpu" else 250.0
        energy = power * latency
        carbon = (energy / 3.6e6) * self.carbon_intensity

        return {
            "success": True,
            "output": prompt + " [simulated]",
            "latency_ms": latency * 1000.0,
            "num_tokens": int(max_new_tokens),
            "energy_joules": float(energy),
            "carbon_g": float(carbon),
            "gpu_memory_used_mb": 0,
            "throughput_tokens_per_s": (
                max_new_tokens / latency if latency > 0 else 0.0
            ),
            "quality_score": 0.95,
            "policy": dict(policy_dict),
            "simulated": True,
        }

    # ------------------------------------------------------------------
    # Generation
    # ------------------------------------------------------------------
    def _generate(
        self,
        prompt: str,
        policy_dict: Dict[str, Any],
        max_new_tokens: int,
    ) -> tuple[str, float, int]:
        """
        Run the actual generation. Returns (output_text, latency_seconds,
        actual_num_generated_tokens).
        """
        inputs = self.tokenizer(prompt, return_tensors="pt")

        # Move inputs to the model's primary device if there is one.
        # With device_map="auto" accelerate handles placement via hooks;
        # we only move inputs when the model exposes a single device.
        try:
            device = getattr(self.model, "device", None)
            if device is not None:
                inputs = {k: v.to(device) for k, v in inputs.items()}
        except Exception:
            pass

        gen_kwargs: Dict[str, Any] = {
            "max_new_tokens": int(max_new_tokens),
            "do_sample": False,
        }
        pad_id = getattr(self.tokenizer, "pad_token_id", None)
        eos_id = getattr(self.tokenizer, "eos_token_id", None)
        if pad_id is not None:
            gen_kwargs["pad_token_id"] = pad_id
        if eos_id is not None:
            gen_kwargs["eos_token_id"] = eos_id

        start = time.monotonic()
        with torch.no_grad():
            outputs = self.model.generate(**inputs, **gen_kwargs)
        latency = time.monotonic() - start

        # Count actual generated tokens (input prefix excluded).
        input_len = 0
        try:
            if isinstance(inputs, dict) and "input_ids" in inputs:
                input_len = int(inputs["input_ids"].shape[1])
        except Exception:
            input_len = 0
        try:
            output_len = int(outputs.shape[1])
        except Exception:
            output_len = input_len + int(max_new_tokens)
        num_tokens = max(0, output_len - input_len)
        if num_tokens == 0:
            num_tokens = int(max_new_tokens)

        output_text = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        return output_text, latency, num_tokens

    # ------------------------------------------------------------------
    # Metric assembly
    # ------------------------------------------------------------------
    def _assemble_real_metrics(
        self,
        prompt: str,
        output_text: str,
        policy_dict: Dict[str, Any],
        max_new_tokens: int,
        latency: float,
        num_tokens: int,
        gpu_before: Dict[str, Any],
        gpu_after: Dict[str, Any],
    ) -> Dict[str, Any]:
        # Energy: prefer the profiler's cumulative delta if available.
        energy_before = float(gpu_before.get("energy_joules", 0.0) or 0.0)
        energy_after = float(gpu_after.get("energy_joules", 0.0) or 0.0)
        if energy_after > energy_before:
            energy = energy_after - energy_before
        else:
            # Fallback: two-point average of instantaneous power.
            power_before = float(gpu_before.get("gpu_power_watts", 65.0) or 65.0)
            power_after = float(gpu_after.get("gpu_power_watts", 65.0) or 65.0)
            avg_power = (power_before + power_after) / 2.0
            energy = avg_power * latency

        carbon = (energy / 3.6e6) * self.carbon_intensity
        throughput = num_tokens / latency if latency > 0 else 0.0

        return {
            "success": True,
            "output": output_text,
            "latency_ms": latency * 1000.0,
            "num_tokens": int(num_tokens),
            "energy_joules": float(energy),
            "carbon_g": float(carbon),
            "gpu_memory_used_mb": int(gpu_after.get("gpu_memory_used_mb", 0) or 0),
            "throughput_tokens_per_s": float(throughput),
            "quality_score": 1.0,
            "policy": dict(policy_dict),
            "simulated": False,
        }

    def _failure_metrics(
        self,
        reason: str,
        policy_dict: Optional[Dict[str, Any]] = None,
        is_oom: bool = False,
    ) -> Dict[str, Any]:
        return {
            "success": False,
            "output": "",
            "latency_ms": float("inf"),
            "num_tokens": 0,
            "energy_joules": float("inf"),
            "carbon_g": float("inf"),
            "gpu_memory_used_mb": 0,
            "throughput_tokens_per_s": 0.0,
            "quality_score": 0.0,
            "policy": dict(policy_dict or {}),
            "simulated": False,
            "error": "cuda_oom" if is_oom else reason,
        }

    # ------------------------------------------------------------------
    # GPU profiler integration
    # ------------------------------------------------------------------
    def _collect_gpu_metrics_sync(self) -> Dict[str, Any]:
        if self.gpu_profiler is None:
            return {}
        # Do not call the profiler's sync API from a running loop.
        try:
            asyncio.get_running_loop()
            # Loop is running; skip to avoid RuntimeError inside the profiler.
            return {}
        except RuntimeError:
            pass
        try:
            return self.gpu_profiler.get_gpu_metrics() or {}
        except Exception as exc:
            log_event("warning", f"GPU profiler sync call failed: {exc}")
            return {}

    async def _collect_gpu_metrics_async(self) -> Dict[str, Any]:
        if self.gpu_profiler is None:
            return {}
        try:
            metrics = await self.gpu_profiler.get_all_gpu_metrics()
            return metrics[0] if metrics else {}
        except Exception as exc:
            log_event("warning", f"GPU profiler async call failed: {exc}")
            return {}

    # ------------------------------------------------------------------
    # Public synchronous inference
    # ------------------------------------------------------------------
    def run_inference_sync(
        self,
        prompt: str,
        policy: Dict[str, Any],
        max_new_tokens: int = 20,
        use_block_scheduling: bool = False,
    ) -> Dict[str, Any]:
        """Synchronous inference (falls back to simulation if needed)."""
        if self._closed:
            return self._failure_metrics("connector_closed")

        if not isinstance(prompt, str) or not prompt:
            return self._failure_metrics("empty_prompt")

        max_new_tokens = _as_int(
            max_new_tokens, 20, "max_new_tokens",
            min_val=self.MIN_MAX_NEW_TOKENS,
            max_val=self.MAX_MAX_NEW_TOKENS,
        )
        policy_dict = _policy_dict(policy)

        if use_block_scheduling:
            log_event(
                "warning",
                "use_block_scheduling=True is not implemented; "
                "running default inference.",
            )

        # Simulation path
        if self.simulation_mode or self.model is None:
            metrics = self._simulate_inference(prompt, policy_dict, max_new_tokens)
            metrics["use_block_scheduling_requested"] = bool(use_block_scheduling)
            self.last_metrics = metrics
            self._record_inference_metrics(metrics)
            return metrics

        # Real path
        gpu_before = self._collect_gpu_metrics_sync()

        try:
            output_text, latency, num_tokens = self._generate(
                prompt, policy_dict, max_new_tokens,
            )
        except Exception as exc:
            return self._handle_generation_failure(
                exc, policy_dict, gpu_before,
            )

        gpu_after = self._collect_gpu_metrics_sync()

        metrics = self._assemble_real_metrics(
            prompt, output_text, policy_dict, max_new_tokens,
            latency, num_tokens, gpu_before, gpu_after,
        )
        metrics["use_block_scheduling_requested"] = bool(use_block_scheduling)
        self.last_metrics = metrics
        self._record_inference_metrics(metrics)
        self._publish_feedback_sync(metrics, policy_dict)
        return metrics

    def _handle_generation_failure(
        self,
        exc: BaseException,
        policy_dict: Dict[str, Any],
        gpu_before: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Free CUDA cache, log, and return a fixed-schema failure metrics dict."""
        is_oom = False
        if TORCH_AVAILABLE and torch is not None:
            cuda_mod = getattr(torch, "cuda", None)
            oom_cls = getattr(cuda_mod, "OutOfMemoryError", None) if cuda_mod else None
            if oom_cls is not None and isinstance(exc, oom_cls):
                is_oom = True
            try:
                if cuda_mod is not None and cuda_mod.is_available():
                    cuda_mod.empty_cache()
            except Exception:
                pass

        log_event(
            "error",
            f"Inference failed ({'OOM' if is_oom else 'error'}): {exc}",
        )
        metrics = self._failure_metrics(str(exc), policy_dict, is_oom=is_oom)
        self.last_metrics = metrics
        self._record_inference_metrics(metrics)
        return metrics

    # ------------------------------------------------------------------
    # Public asynchronous inference
    # ------------------------------------------------------------------
    async def run_inference(
        self,
        prompt: str,
        policy: Dict[str, Any],
        max_new_tokens: int = 20,
        use_block_scheduling: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronous inference; never blocks the event loop."""
        if self._closed:
            return self._failure_metrics("connector_closed")

        if not isinstance(prompt, str) or not prompt:
            return self._failure_metrics("empty_prompt")

        max_new_tokens = _as_int(
            max_new_tokens, 20, "max_new_tokens",
            min_val=self.MIN_MAX_NEW_TOKENS,
            max_val=self.MAX_MAX_NEW_TOKENS,
        )
        policy_dict = _policy_dict(policy)

        if use_block_scheduling:
            log_event(
                "warning",
                "use_block_scheduling=True is not implemented; "
                "running default inference.",
            )

        # Simulation path
        if self.simulation_mode or self.model is None:
            metrics = self._simulate_inference(prompt, policy_dict, max_new_tokens)
            metrics["use_block_scheduling_requested"] = bool(use_block_scheduling)
            self.last_metrics = metrics
            self._record_inference_metrics(metrics)
            await self._publish_feedback_async(metrics, policy_dict)
            return metrics

        # Real path
        gpu_before = await self._collect_gpu_metrics_async()

        try:
            output_text, latency, num_tokens = await asyncio.to_thread(
                self._generate, prompt, policy_dict, max_new_tokens,
            )
        except Exception as exc:
            return self._handle_generation_failure(exc, policy_dict, gpu_before)

        gpu_after = await self._collect_gpu_metrics_async()

        metrics = self._assemble_real_metrics(
            prompt, output_text, policy_dict, max_new_tokens,
            latency, num_tokens, gpu_before, gpu_after,
        )
        metrics["use_block_scheduling_requested"] = bool(use_block_scheduling)
        self.last_metrics = metrics
        self._record_inference_metrics(metrics)
        await self._publish_feedback_async(metrics, policy_dict)
        return metrics

    # ------------------------------------------------------------------
    # Feedback publishing
    # ------------------------------------------------------------------
    def _build_feedback_event(
        self, metrics: Dict[str, Any], policy_dict: Dict[str, Any]
    ) -> Optional[Any]:
        if (
            not self._publish_events
            or self.message_queue is None
            or FeedbackEvent is None
        ):
            return None

        # Reward
        reward = 0.0
        try:
            if self.workload is not None:
                reward = float(compute_reward(metrics, self.workload))
            else:
                reward = 0.5
        except Exception as exc:
            log_event("warning", f"Reward computation failed: {exc}")
            reward = 0.0

        try:
            task_id = None
            if self.workload is not None:
                task_id = getattr(self.workload, "task_id", None)

            node_id = getattr(self.node, "id", None) if self.node else None

            event = FeedbackEvent(
                source="hf_flexgen_connector",
                feedback_type="routing",
                task_id=task_id,
                context={
                    "model_name": self.model_name,
                    "policy": str(policy_dict),
                    "node_id": node_id,
                    "simulation": bool(metrics.get("simulated", False)),
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
                tags=["hf_flexgen", "inference", "policy_execution"],
            )
            return event
        except Exception as exc:
            log_event("warning", f"Failed to build FeedbackEvent: {exc}")
            self._inc_publish_failure()
            return None

    @staticmethod
    def _serialize_event(event: Any) -> str:
        # Preferred: to_json
        try:
            if hasattr(event, "to_json") and callable(event.to_json):
                return event.to_json()
        except Exception:
            pass
        # Fallback: to_dict + json.dumps
        try:
            if hasattr(event, "to_dict") and callable(event.to_dict):
                return json.dumps(event.to_dict(), default=str)
        except Exception:
            pass
        # Last resort: str()
        return str(event)

    def _inc_publish_failure(self) -> None:
        if self._enable_prometheus and _M_PUBLISH_FAILURES is not None:
            try:
                _M_PUBLISH_FAILURES.inc()
            except Exception:
                pass

    async def _publish_feedback_async(
        self, metrics: Dict[str, Any], policy_dict: Dict[str, Any]
    ) -> None:
        event = self._build_feedback_event(metrics, policy_dict)
        if event is None:
            return
        payload = self._serialize_event(event)
        try:
            await self.message_queue.publish("inference_events", payload)
        except Exception as exc:
            log_event("warning", f"Failed to publish FeedbackEvent: {exc}")
            self._inc_publish_failure()

    def _publish_feedback_sync(
        self, metrics: Dict[str, Any], policy_dict: Dict[str, Any]
    ) -> None:
        event = self._build_feedback_event(metrics, policy_dict)
        if event is None:
            return
        payload = self._serialize_event(event)

        # Are we inside a running loop?
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # No loop: run once on a throwaway loop.
            try:
                asyncio.run(self.message_queue.publish("inference_events", payload))
            except Exception as exc:
                log_event("warning", f"Failed to publish FeedbackEvent (sync/run): {exc}")
                self._inc_publish_failure()
            return

        # Loop is running: schedule and track.
        try:
            task = asyncio.create_task(
                self.message_queue.publish("inference_events", payload)
            )
        except Exception as exc:
            log_event("warning", f"Failed to schedule publish: {exc}")
            self._inc_publish_failure()
            return
        self._pending_publish_tasks.add(task)
        task.add_done_callback(self._pending_publish_tasks.discard)
        task.add_done_callback(self._log_deferred_publish_result)

    def _log_deferred_publish_result(self, task: "asyncio.Task[Any]") -> None:
        try:
            exc = task.exception()
        except (asyncio.CancelledError, Exception):
            return
        if exc is not None:
            log_event("warning", f"Deferred publish failed: {exc}")
            self._inc_publish_failure()

    async def drain_publishes(self) -> None:
        """Await any in-flight publish tasks."""
        if not self._pending_publish_tasks:
            return
        pending = list(self._pending_publish_tasks)
        await asyncio.gather(*pending, return_exceptions=True)
        self._pending_publish_tasks.clear()

    # ------------------------------------------------------------------
    # Prometheus helpers
    # ------------------------------------------------------------------
    def _record_inference_metrics(self, metrics: Dict[str, Any]) -> None:
        if not self._enable_prometheus:
            return
        try:
            simulated = str(bool(metrics.get("simulated", False)))
            success = str(bool(metrics.get("success", False)))
            if _M_INFERENCES is not None:
                _M_INFERENCES.labels(simulated=simulated, success=success).inc()
            if metrics.get("success"):
                lat_ms = metrics.get("latency_ms", float("inf"))
                if _M_INFERENCE_SECONDS is not None and math.isfinite(lat_ms):
                    _M_INFERENCE_SECONDS.observe(float(lat_ms) / 1000.0)
                en = metrics.get("energy_joules", float("inf"))
                if _M_ENERGY is not None and math.isfinite(en):
                    _M_ENERGY.observe(float(en))
                cg = metrics.get("carbon_g", float("inf"))
                if _M_CARBON is not None and math.isfinite(cg):
                    _M_CARBON.observe(float(cg))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        """Clean up resources: profiler, model, CUDA cache, offload folder."""
        if self._closed:
            return
        self._closed = True

        if self.gpu_profiler is not None:
            try:
                self.gpu_profiler.shutdown()
            except Exception as exc:
                log_event("warning", f"GPU profiler shutdown failed: {exc}")
            self.gpu_profiler = None

        # Drop model references so the GC can reclaim them.
        self.model = None
        self.tokenizer = None

        if TORCH_AVAILABLE and torch is not None:
            try:
                cuda_mod = getattr(torch, "cuda", None)
                if cuda_mod is not None and cuda_mod.is_available():
                    cuda_mod.empty_cache()
            except Exception:
                pass

        # Clean up the offload folder.
        try:
            if self._offload_dir.exists():
                shutil.rmtree(self._offload_dir, ignore_errors=True)
        except Exception:
            pass

    async def aclose(self) -> None:
        """Async-safe shutdown: drains pending publishes, then calls close()."""
        await self.drain_publishes()
        self.close()

    async def __aenter__(self) -> "HFFlexGenConnector":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()


# ==============================================================================
# Example usage
# ==============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def _demo() -> None:
        connector = HFFlexGenConnector()
        try:
            policy = {
                "weight_device": "gpu",
                "activation_device": "gpu",
                "kv_cache_device": "gpu",
                "weight_bits": 8,
                "kv_cache_bits": 8,
                "gpu_batch_size": 1,
            }
            ok = connector.load_model(policy)
            print(f"load_model: {ok}, simulation={connector.simulation_mode}")

            metrics = await connector.run_inference(
                "Hello world, this is a test.",
                policy,
                max_new_tokens=16,
            )
            print(json.dumps(
                {k: v for k, v in metrics.items() if k != "output"},
                indent=2, default=str,
            ))
        finally:
            await connector.aclose()

    asyncio.run(_demo())
