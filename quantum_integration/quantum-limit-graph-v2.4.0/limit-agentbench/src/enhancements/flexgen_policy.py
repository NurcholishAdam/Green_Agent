"""
FlexGen-style Policy and Mock Executor for Green Agent GPU Orchestration.
Enhanced v3 — substrate for all ten Green Agent enhancements:

  1. Quantum-Distillation Integration      -> versioned feature schema, teacher-ready vectors
  2. Causal RL for Policy Adaptation       -> state/outcome feature separations, counterfactual hooks
  3. Federated Green Learning              -> serializable, versioned policy vectors
  4. Multi-Agent Coordination              -> policy bid representation, role-tagged metrics
  5. Temporal Logic / Formal Verification  -> time-indexed provenance records
  6. Explainable AI                        -> per-decision attribution surface
  7. Adaptive Precision Switching          -> runtime precision override + mixed precision
  8. Carbon Markets / RECs                 -> carbon price input, REC offset output
  9. Resilience / Chaos Testing            -> fault, latency, carbon-spike injection
 10. HITL / Active Learning                -> uncertainty on every metric, decision records

CHANGELOG v2 -> v3
- Removed dead model_size_gb computation (was computed twice).
- Unified bits accounting: effective_weight_bits drives model_size_gb; effective_kv_bits drives kv_cache.
- activation_gb is now derived from layers * seq * hidden (was a magic constant).
- Quality is no longer double-penalized: precision_explicit short-circuits legacy bit penalties.
- _failed_result is JSON-safe: uses FAILED_SENTINEL (1e12) and -1.0 sentinels, never inf/nan.
- _failed_result resolves a valid precision level even on failure (was returning "unknown").
- _contrib caps SNR at SNR_CAP to avoid Infinity in JSON.
- ChaosInjector.maybe_carbon_spike returns (value, spiked); caller records the event once.
- generate_grid_policies pruning accounts for KV cache and activation memory.
- PRECISION_ORDER + PRECISION_INDEX for stable vector encoding.
- to_vector(enforce_version=True) rejects precision levels outside PRECISION_ORDER.
- MockFlexGenExecutor._role_efficiency honors ExecutionContext.role_tag.
- Mixed precision now blends 75/25 (weight vs KV share) instead of 50/50.
- Carbon price penalty and latency-variance penalty added to compute_reward.
- Disk IOPS check added to CPU -> disk fallback.
- Throughput is split into prefill_tokens_per_s and decode_tokens_per_s.
- effective_speedup emitted for post-hoc calibration.
- Backward compatible: all original signatures still work.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor


# ===========================================================================
# Feature / vector schema versioning (Enhancements 1, 2, 3)
# ===========================================================================
POLICY_VECTOR_VERSION = "flexgen_policy_vec_v3"
POLICY_VECTOR_FIELDS: Tuple[str, ...] = (
    # normalized scalars
    "gpu_batch_size_norm",
    "block_size_norm",
    # weight device one-hot
    "weight_device_gpu",
    "weight_device_cpu",
    "weight_device_disk",
    # activation device one-hot
    "activation_device_gpu",
    "activation_device_cpu",
    # kv cache device one-hot
    "kv_cache_device_gpu",
    "kv_cache_device_cpu",
    "kv_cache_device_disk",
    # precision
    "weight_bits_norm",
    "kv_cache_bits_norm",
    # flags
    "cpu_attention",
    "overlap_io_compute",
    # extended precision / runtime
    "precision_level_norm",
    "mixed_precision_flag",
)
FEATURE_VECTOR_DIM = len(POLICY_VECTOR_FIELDS)

# Finite sentinels so JSON serialization never encounters inf/nan.
FAILED_SENTINEL = 1e12
SNR_CAP = 1e9


# ===========================================================================
# Precision levels (Enhancement 7) — CANONICAL definition.
# The controller module MUST import these instead of redefining its own.
# ===========================================================================
class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    BF16 = "bf16"
    FP16 = "fp16"
    FP8 = "fp8"
    INT8 = "int8"
    INT4 = "int4"


# Explicit ordering for deterministic vector encoding.
PRECISION_ORDER: Tuple[PrecisionLevel, ...] = (
    PrecisionLevel.FP32,
    PrecisionLevel.BF16,
    PrecisionLevel.FP16,
    PrecisionLevel.FP8,
    PrecisionLevel.INT8,
    PrecisionLevel.INT4,
)
PRECISION_INDEX: Dict[PrecisionLevel, int] = {p: i for i, p in enumerate(PRECISION_ORDER)}


# Per-level hardware-aware cost model.
# speed    : multiplicative throughput vs FP32
# energy   : multiplicative energy vs FP32
# quality  : relative model quality
# memory   : multiplicative memory footprint vs FP32
# bits     : nominal bit-width used for weight / KV accounting
PRECISION_COST: Dict[PrecisionLevel, Dict[str, float]] = {
    PrecisionLevel.FP32: {"speed": 1.0, "energy": 1.00, "quality": 1.000, "memory": 1.00, "bits": 32.0},
    PrecisionLevel.BF16: {"speed": 1.7, "energy": 0.72, "quality": 0.997, "memory": 0.50, "bits": 16.0},
    PrecisionLevel.FP16: {"speed": 2.0, "energy": 0.65, "quality": 0.994, "memory": 0.50, "bits": 16.0},
    PrecisionLevel.FP8:  {"speed": 3.1, "energy": 0.50, "quality": 0.985, "memory": 0.25, "bits": 8.0},
    PrecisionLevel.INT8: {"speed": 3.6, "energy": 0.44, "quality": 0.972, "memory": 0.25, "bits": 8.0},
    PrecisionLevel.INT4: {"speed": 5.0, "energy": 0.32, "quality": 0.905, "memory": 0.125, "bits": 4.0},
}


# Allowed discrete values for classic fields.
ALLOWED_DEVICES = {"gpu", "cpu", "disk"}
ALLOWED_ACTIVATION_DEVICES = {"gpu", "cpu"}
ALLOWED_BITS = {4, 8, 16}


# ===========================================================================
# Policy dataclass
# ===========================================================================
@dataclass
class FlexGenPolicy:
    """Policy variables controlling offloading, batching, and quantization."""
    gpu_batch_size: int = 1
    block_size: int = 16
    weight_device: str = "gpu"
    activation_device: str = "gpu"
    kv_cache_device: str = "gpu"
    weight_bits: int = 16
    kv_cache_bits: int = 16
    cpu_attention: bool = False
    overlap_io_compute: bool = True
    # Enhancement 7: optional runtime precision hint. If set, executor treats
    # this as the effective precision floor / ceiling depending on mode.
    precision_level: Optional[str] = None
    # Enhancement 7: mixed precision — weights and KV can differ.
    mixed_precision: bool = False

    def __post_init__(self):
        if self.weight_device not in ALLOWED_DEVICES:
            raise ValueError(f"weight_device must be one of {ALLOWED_DEVICES}")
        if self.activation_device not in ALLOWED_ACTIVATION_DEVICES:
            raise ValueError(f"activation_device must be one of {ALLOWED_ACTIVATION_DEVICES}")
        if self.kv_cache_device not in ALLOWED_DEVICES:
            raise ValueError(f"kv_cache_device must be one of {ALLOWED_DEVICES}")
        if self.weight_bits not in ALLOWED_BITS:
            raise ValueError(f"weight_bits must be one of {ALLOWED_BITS}")
        if self.kv_cache_bits not in ALLOWED_BITS:
            raise ValueError(f"kv_cache_bits must be one of {ALLOWED_BITS}")
        if self.gpu_batch_size < 1:
            raise ValueError("gpu_batch_size must be >= 1")
        if self.block_size < 1:
            raise ValueError("block_size must be >= 1")
        if self.precision_level is not None:
            try:
                PrecisionLevel(self.precision_level)
            except ValueError as exc:
                raise ValueError(
                    f"precision_level must be one of {[p.value for p in PrecisionLevel]}"
                ) from exc

    # -- serialization -------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FlexGenPolicy":
        return cls(**data)

    # -- vector --------------------------------------------------------
    def to_vector(
        self,
        precision_level: Optional[PrecisionLevel] = None,
        enforce_version: bool = True,
    ) -> List[float]:
        """
        Return a normalized numeric vector (schema-versioned).
        Schema: POLICY_VECTOR_FIELDS, version POLICY_VECTOR_VERSION.

        If `enforce_version` is True, a provided precision_level must belong
        to PRECISION_ORDER. This prevents silently re-encoding persisted
        vectors from an older schema.
        """
        lvl = precision_level
        if lvl is None and self.precision_level is not None:
            lvl = PrecisionLevel(self.precision_level)

        if enforce_version and lvl is not None and lvl not in PRECISION_ORDER:
            raise ValueError(
                f"precision_level {lvl} not in canonical PRECISION_ORDER "
                f"({[p.value for p in PRECISION_ORDER]})"
            )

        if lvl is not None:
            bits = PRECISION_COST[lvl]["bits"]
            w_bits_norm = bits / 32.0
            kv_bits_norm = bits / 32.0
            lvl_norm = PRECISION_INDEX[lvl] / max(len(PRECISION_ORDER) - 1, 1)
        else:
            w_bits_norm = self.weight_bits / 32.0
            kv_bits_norm = self.kv_cache_bits / 32.0
            lvl_norm = 0.0

        return [
            self.gpu_batch_size / 8.0,
            self.block_size / 64.0,
            1.0 if self.weight_device == "gpu" else 0.0,
            1.0 if self.weight_device == "cpu" else 0.0,
            1.0 if self.weight_device == "disk" else 0.0,
            1.0 if self.activation_device == "gpu" else 0.0,
            1.0 if self.activation_device == "cpu" else 0.0,
            1.0 if self.kv_cache_device == "gpu" else 0.0,
            1.0 if self.kv_cache_device == "cpu" else 0.0,
            1.0 if self.kv_cache_device == "disk" else 0.0,
            w_bits_norm,
            kv_bits_norm,
            1.0 if self.cpu_attention else 0.0,
            1.0 if self.overlap_io_compute else 0.0,
            lvl_norm,
            1.0 if self.mixed_precision else 0.0,
        ]

    def vector_schema(self) -> Dict[str, Any]:
        """Schema descriptor for downstream feature consumers (Enhancement 1/3)."""
        return {
            "version": POLICY_VECTOR_VERSION,
            "fields": list(POLICY_VECTOR_FIELDS),
            "dim": FEATURE_VECTOR_DIM,
            "precision_order": [p.value for p in PRECISION_ORDER],
        }

    def policy_hash(self) -> str:
        """Deterministic short hash for federated aggregation keys."""
        payload = json.dumps(self.to_dict(), sort_keys=True).encode()
        return hashlib.sha256(payload).hexdigest()[:16]


# ===========================================================================
# Chaos / resilience (Enhancement 9)
# ===========================================================================
@dataclass
class ChaosConfig:
    fault_prob: float = 0.0
    latency_inject_ms: float = 0.0
    latency_inject_prob: float = 0.3
    carbon_spike_prob: float = 0.0
    carbon_spike_factor: float = 1.5
    seed: int = 0

    def enabled(self) -> bool:
        return (
            self.fault_prob > 0
            or (self.latency_inject_ms > 0 and self.latency_inject_prob > 0)
            or self.carbon_spike_prob > 0
        )


class ChaosInjector:
    """Deterministic chaos injector for resilience testing."""

    def __init__(self, config: Optional[ChaosConfig] = None):
        self.config = config or ChaosConfig()
        self.rng = random.Random(self.config.seed)
        self.events: List[Dict[str, Any]] = []

    def maybe_fault(self) -> bool:
        if self.rng.random() < self.config.fault_prob:
            self.events.append({"type": "fault", "t": time.time()})
            return True
        return False

    def maybe_latency(self) -> float:
        if (
            self.config.latency_inject_ms > 0
            and self.rng.random() < self.config.latency_inject_prob
        ):
            self.events.append({"type": "latency", "t": time.time()})
            return self.config.latency_inject_ms
        return 0.0

    def maybe_carbon_spike(self, carbon_intensity: float) -> Tuple[float, bool]:
        """
        v3: returns (new_intensity, spiked_flag) so the caller records the
        event exactly once, avoiding duplicate entries in the log.
        """
        if self.rng.random() < self.config.carbon_spike_prob:
            return carbon_intensity * self.config.carbon_spike_factor, True
        return carbon_intensity, False


# ===========================================================================
# Runtime execution context (Enhancements 7, 8, 9, 10)
# ===========================================================================
@dataclass
class ExecutionContext:
    """Optional per-call context allowing runtime overrides."""
    carbon_intensity_g_per_kwh: Optional[float] = None
    carbon_price_per_kg: float = 0.0
    rec_kwh_available: float = 0.0
    rec_grid_factor_kg_per_kwh: float = 0.4
    hour_of_day: Optional[int] = None
    precision_override: Optional[PrecisionLevel] = None
    deterministic_seed: Optional[int] = None
    role_tag: Optional[str] = None  # Enhancement 4 — multi-agent tag


# ===========================================================================
# Mock executor
# ===========================================================================
class MockFlexGenExecutor:
    """
    Simulates execution of a FlexGen policy on a given node and workload.

    Produces:
      - point estimates: latency, throughput (prefill + decode), energy,
        carbon, quality, memory
      - uncertainty: latency_std_ms, carbon_std_g, energy_std_j (Enhancement 10)
      - market signals: carbon_price_cost, rec_offset_kg, net_carbon_g (Enhancement 8)
      - provenance: execution_id, timestamp, node_id, workload_id (Enhancement 5)
      - explainability: attribution surface (Enhancement 6)
      - runtime precision actually used (Enhancement 7)
      - chaos events applied (Enhancement 9)
      - role-aware efficiency modifier (Enhancement 4)
    """

    def __init__(
        self,
        carbon_intensity_g_per_kwh: float = 400.0,
        chaos: Optional[ChaosConfig] = None,
        enable_uncertainty: bool = True,
        enable_provenance: bool = True,
    ):
        self.carbon_intensity = carbon_intensity_g_per_kwh
        self.chaos = ChaosInjector(chaos) if (chaos and chaos.enabled()) else None
        self.enable_uncertainty = enable_uncertainty
        self.enable_provenance = enable_provenance
        # Deterministic noise per policy hash (so repeated runs are stable).
        self._noise_cache: Dict[str, float] = {}

    # -- helpers -------------------------------------------------------
    def _stable_noise(self, key: str, scale: float) -> float:
        """Stable per-key pseudo-noise in [-scale, scale]."""
        if key not in self._noise_cache:
            h = int(hashlib.sha256(key.encode()).hexdigest(), 16)
            self._noise_cache[key] = ((h % 10_000) / 10_000.0) * 2.0 - 1.0
        return self._noise_cache[key] * scale

    def _resolve_precision(
        self,
        policy: FlexGenPolicy,
        context: Optional[ExecutionContext],
    ) -> Tuple[PrecisionLevel, bool]:
        """
        Resolve effective precision. Priority:
          1. Explicit context.precision_override
          2. policy.precision_level
          3. Legacy weight_bits mapping
        Returns (level, mixed_precision_flag).
        """
        if context and context.precision_override is not None:
            return context.precision_override, bool(policy.mixed_precision)
        if policy.precision_level is not None:
            return PrecisionLevel(policy.precision_level), bool(policy.mixed_precision)
        if policy.weight_bits >= 16:
            return PrecisionLevel.FP16, bool(policy.mixed_precision)
        if policy.weight_bits == 8:
            return PrecisionLevel.INT8, bool(policy.mixed_precision)
        return PrecisionLevel.INT4, bool(policy.mixed_precision)

    def _effective_bits(
        self,
        policy: FlexGenPolicy,
        level: PrecisionLevel,
        mixed: bool,
        precision_explicit: bool,
    ) -> Tuple[float, float]:
        """
        Returns (weight_bits, kv_bits) for memory accounting.

        Rules (v3):
          - Explicit precision, non-mixed      : both use the level's bits.
          - Explicit precision, mixed          : weights use level bits,
                                                 KV keeps explicit kv_cache_bits.
          - Precision inferred from weight_bits: weights use effective bits,
                                                 KV keeps explicit kv_cache_bits.
        """
        level_bits = PRECISION_COST[level]["bits"]
        if precision_explicit:
            if mixed:
                return level_bits, float(policy.kv_cache_bits)
            return level_bits, level_bits
        return level_bits, float(policy.kv_cache_bits)

    def _quantization_speedup(
        self, level: PrecisionLevel, mixed: bool
    ) -> Tuple[float, float]:
        """
        Return (speed_multiplier, quality_multiplier) for a given precision.
        Mixed precision blends 75 % target + 25 % FP16 to reflect that only
        weight-side compute is quantized while KV stays higher precision.
        """
        base = PRECISION_COST[level]
        speed = base["speed"]
        quality = base["quality"]
        if mixed:
            ref = PRECISION_COST[PrecisionLevel.FP16]
            speed = 0.75 * speed + 0.25 * ref["speed"]
            quality = 0.75 * quality + 0.25 * ref["quality"]
        return speed, quality

    def _role_efficiency(self, context: Optional[ExecutionContext]) -> float:
        """
        Small deterministic efficiency modifier per role tag (v3).
        Role tags come from the multi-agent coordinator (Enhancement 4).
        """
        if not context or not context.role_tag:
            return 1.0
        tag = context.role_tag.lower()
        table = {
            "safety_officer": 0.95,
            "carbon_broker": 0.98,
            "verifier": 1.00,
            "exploiter": 1.02,
            "explorer": 1.05,
        }
        return table.get(tag, 1.0)

    # -- main ----------------------------------------------------------
    def execute(
        self,
        policy: FlexGenPolicy,
        node: NodeDescriptor,
        workload: WorkloadDescriptor,
        context: Optional[ExecutionContext] = None,
    ) -> Dict[str, Any]:
        exec_id = str(uuid.uuid4())
        t_start = time.time()

        # -- Chaos hook: hard fault (Enhancement 9) ------------------
        if self.chaos and self.chaos.maybe_fault():
            return self._failed_result(
                policy, exec_id, t_start,
                reason="chaos_fault",
                chaos_events=self.chaos.events[-1:],
            )

        # -- Node specs ---------------------------------------------
        gpu_memory_gb = float(node.metadata.get("gpu_memory_gb", 16.0))
        cpu_memory_gb = float(node.metadata.get("cpu_memory_gb", 64.0))
        gpu_flops_tflops = float(node.metadata.get("gpu_flops_tflops", 30.0))
        gpu_cpu_bw_gbps = float(node.metadata.get("gpu_cpu_bandwidth_gbps", 12.0))
        disk_bw_gbps = float(node.metadata.get("disk_bandwidth_gbps", 2.0))
        disk_iops = float(node.metadata.get("disk_iops", 5000.0))
        gpu_max_power_w = float(node.metadata.get("gpu_max_power_w", 250.0))
        cpu_max_power_w = float(node.metadata.get("cpu_max_power_w", 100.0))
        gpu_idle_power_w = float(node.metadata.get("gpu_idle_power_w", 20.0))
        cpu_idle_power_w = float(node.metadata.get("cpu_idle_power_w", 10.0))

        # -- Model params -------------------------------------------
        model_params = workload.metadata.get("model_params", {})
        num_layers = int(model_params.get("num_layers", 32))
        hidden_dim = int(model_params.get("hidden_dim", 4096))
        params_billions = float(model_params.get("params_billions", 7))
        seq_len_prompt = int(workload.tokens)
        max_new_tokens = int(
            model_params.get("max_new_tokens",
                             workload.metadata.get("max_new_tokens", 32))
        )

        # -- Precision resolution (Enhancement 7) -------------------
        precision_explicit = (
            (context is not None and context.precision_override is not None)
            or policy.precision_level is not None
        )
        precision_level, mixed = self._resolve_precision(policy, context)
        speed_mult, quality_mult = self._quantization_speedup(precision_level, mixed)
        speed_mult *= self._role_efficiency(context)

        eff_weight_bits, eff_kv_bits = self._effective_bits(
            policy, precision_level, mixed, precision_explicit
        )

        # -- Memory model -------------------------------------------
        # v3: single, consistent bits accounting.
        model_size_gb = params_billions * (eff_weight_bits / 8.0)

        bytes_per_kv = eff_kv_bits / 8.0
        kv_prompt_gb = (
            policy.gpu_batch_size * seq_len_prompt * 2 * hidden_dim * num_layers * bytes_per_kv
        ) / 1e9
        kv_decode_gb = (
            policy.gpu_batch_size * max_new_tokens * 2 * hidden_dim * num_layers * bytes_per_kv
        ) / 1e9
        kv_cache_gb = kv_prompt_gb + kv_decode_gb

        # v3: activation memory derived from layers * seq * hidden.
        # 2 bytes per activation element, 4x workspace factor.
        activation_gb = (
            policy.gpu_batch_size * seq_len_prompt * hidden_dim * num_layers * 2 * 4
        ) / 1e9

        weight_on_gpu = policy.weight_device == "gpu"
        kv_on_gpu = policy.kv_cache_device == "gpu"
        activation_on_gpu = policy.activation_device == "gpu"

        peak_gpu_mem_gb = (
            (model_size_gb if weight_on_gpu else 0)
            + (kv_cache_gb if kv_on_gpu else 0)
            + (activation_gb if activation_on_gpu else 0)
        )
        peak_cpu_mem_gb = (
            (model_size_gb if policy.weight_device == "cpu" else 0)
            + (kv_cache_gb if policy.kv_cache_device == "cpu" else 0)
            + (activation_gb if policy.activation_device == "cpu" else 0)
        )
        disk_io_gb = (
            (model_size_gb if policy.weight_device == "disk" else 0)
            + (kv_cache_gb if policy.kv_cache_device == "disk" else 0)
        )

        # -- Feasibility with fallback (Enhancement 7) --------------
        fallback_applied = False
        effective_weight_device = policy.weight_device
        effective_kv_device = policy.kv_cache_device

        gpu_ok = peak_gpu_mem_gb <= gpu_memory_gb
        cpu_ok = peak_cpu_mem_gb <= cpu_memory_gb
        if not (gpu_ok and cpu_ok):
            if policy.weight_device == "cpu" and not cpu_ok:
                effective_weight_device = "disk"
                fallback_applied = True
            if policy.kv_cache_device == "cpu" and not cpu_ok:
                effective_kv_device = "disk"
                fallback_applied = True
            peak_gpu_mem_gb = (
                (model_size_gb if effective_weight_device == "gpu" else 0)
                + (kv_cache_gb if effective_kv_device == "gpu" else 0)
                + (activation_gb if activation_on_gpu else 0)
            )
            peak_cpu_mem_gb = (
                (model_size_gb if effective_weight_device == "cpu" else 0)
                + (kv_cache_gb if effective_kv_device == "cpu" else 0)
                + (activation_gb if not activation_on_gpu else 0)
            )
            disk_io_gb = (
                (model_size_gb if effective_weight_device == "disk" else 0)
                + (kv_cache_gb if effective_kv_device == "disk" else 0)
            )
            gpu_ok = peak_gpu_mem_gb <= gpu_memory_gb
            cpu_ok = peak_cpu_mem_gb <= cpu_memory_gb

        # v3: disk IOPS sanity check when we fall back to disk.
        if (effective_weight_device == "disk" or effective_kv_device == "disk") and disk_iops <= 0:
            return self._failed_result(
                policy, exec_id, t_start,
                reason="disk_iops_unavailable",
                chaos_events=[],
            )

        success = gpu_ok and cpu_ok

        # -- Transfer time ------------------------------------------
        transfer_time_s = 0.0
        if effective_weight_device != "gpu":
            if effective_weight_device == "disk":
                transfer_time_s += model_size_gb / max(disk_bw_gbps, 1e-9)
            transfer_time_s += model_size_gb / max(gpu_cpu_bw_gbps, 1e-9)
        if effective_kv_device != "gpu":
            if effective_kv_device == "disk":
                transfer_time_s += kv_cache_gb / max(disk_bw_gbps, 1e-9)
            transfer_time_s += kv_cache_gb / max(gpu_cpu_bw_gbps, 1e-9)

        # -- Compute time -------------------------------------------
        flops_per_token = num_layers * 8 * hidden_dim * hidden_dim
        total_tokens = seq_len_prompt + max_new_tokens
        total_flops = flops_per_token * total_tokens * policy.gpu_batch_size

        batch_efficiency = 0.6 + 0.4 * min(1.0, policy.gpu_batch_size / 8.0)
        block_efficiency = 0.85 + 0.15 * min(1.0, policy.block_size / 32.0)
        gpu_flops = gpu_flops_tflops * 1e12 * batch_efficiency * block_efficiency
        gpu_flops *= speed_mult
        compute_time_s = total_flops / gpu_flops if gpu_flops > 0 else 0.0

        if policy.cpu_attention:
            cpu_flops_tflops = float(node.metadata.get("cpu_flops_tflops", 0.5))
            cpu_attn_flops = 2 * total_tokens * policy.gpu_batch_size * hidden_dim * num_layers
            cpu_attn_flops_s = cpu_flops_tflops * 1e12
            compute_time_s += cpu_attn_flops / max(cpu_attn_flops_s, 1e-9)

        # -- Total time with idle-aware energy ----------------------
        if policy.overlap_io_compute:
            total_time_s = max(compute_time_s, transfer_time_s)
        else:
            total_time_s = compute_time_s + transfer_time_s

        # Chaos latency injection (Enhancement 9).
        if self.chaos:
            injected = self.chaos.maybe_latency()
            if injected > 0:
                total_time_s += injected / 1000.0

        latency_ms = total_time_s * 1000.0

        # Throughput split (v3): prefill vs decode.
        prefill_tokens = seq_len_prompt * policy.gpu_batch_size
        decode_tokens = max_new_tokens * policy.gpu_batch_size
        prefill_tps = prefill_tokens / total_time_s if total_time_s > 0 else 0.0
        decode_tps = decode_tokens / total_time_s if total_time_s > 0 else 0.0
        throughput_tokens_per_s = (
            (prefill_tokens + decode_tokens) / total_time_s if total_time_s > 0 else 0.0
        )

        # -- Energy: active + idle during overlap -------------------
        gpu_util = min(1.0, compute_time_s / total_time_s) if total_time_s > 0 else 0.5
        cpu_util = min(1.0, transfer_time_s / total_time_s) if total_time_s > 0 else 0.5
        gpu_active_w = gpu_idle_power_w + gpu_util * (gpu_max_power_w - gpu_idle_power_w)
        cpu_active_w = cpu_idle_power_w + cpu_util * (cpu_max_power_w - cpu_idle_power_w)

        gpu_idle_window_s = max(0.0, total_time_s - compute_time_s)
        cpu_idle_window_s = max(0.0, total_time_s - transfer_time_s)

        energy_j = (
            gpu_active_w * compute_time_s
            + gpu_idle_power_w * gpu_idle_window_s
            + cpu_active_w * transfer_time_s
            + cpu_idle_power_w * cpu_idle_window_s
        )
        # Precision energy multiplier (Enhancement 7).
        energy_j *= PRECISION_COST[precision_level]["energy"]

        # -- Carbon: time-varying intensity + chaos (Enhancement 8/9)
        base_intensity = self.carbon_intensity
        if context and context.carbon_intensity_g_per_kwh is not None:
            base_intensity = context.carbon_intensity_g_per_kwh

        chaos_events: List[Dict[str, Any]] = []
        if self.chaos:
            spiked, was_spiked = self.chaos.maybe_carbon_spike(base_intensity)
            if was_spiked:
                chaos_events.append({"type": "carbon_spike", "value": spiked})
                base_intensity = spiked

        energy_kwh = energy_j / 3.6e6
        gross_carbon_g = energy_kwh * base_intensity

        # -- REC offset (Enhancement 8) -----------------------------
        rec_offset_kg = 0.0
        rec_kwh_used = 0.0
        if context and context.rec_kwh_available > 0:
            rec_kwh_used = min(energy_kwh, context.rec_kwh_available)
            rec_offset_kg = rec_kwh_used * context.rec_grid_factor_kg_per_kwh
        net_carbon_g = max(0.0, gross_carbon_g - rec_offset_kg * 1000.0)

        # -- Carbon price cost (Enhancement 8) ----------------------
        carbon_price_cost = 0.0
        if context and context.carbon_price_per_kg > 0:
            carbon_price_cost = (net_carbon_g / 1000.0) * context.carbon_price_per_kg

        # -- Quality: single source of penalty (v3) -----------------
        if precision_explicit:
            # Precision level fully determines quality; do not double-penalize.
            quality = quality_mult
        else:
            quality = 1.0
            if policy.weight_bits <= 4:
                quality *= 0.85
            elif policy.weight_bits <= 8:
                quality *= 0.95
            if policy.kv_cache_bits <= 4:
                quality *= 0.90
            elif policy.kv_cache_bits <= 8:
                quality *= 0.97
        quality = max(0.0, min(1.0, quality))

        # -- Uncertainty (Enhancements 6, 10) -----------------------
        if self.enable_uncertainty:
            ph = policy.policy_hash()
            latency_std = abs(self._stable_noise(ph + ":lat", 0.08)) * latency_ms + 0.5
            energy_std = abs(self._stable_noise(ph + ":ene", 0.10)) * energy_j + 0.1
            carbon_std = abs(self._stable_noise(ph + ":car", 0.12)) * net_carbon_g + 0.05
            quality_std = abs(self._stable_noise(ph + ":qua", 0.02)) + 0.005
        else:
            latency_std = energy_std = carbon_std = quality_std = 0.0

        # -- XAI attribution surface (Enhancement 6) ----------------
        attribution = {
            "latency_ms": self._contrib(latency_ms, latency_std),
            "energy_joules": self._contrib(energy_j, energy_std),
            "carbon_g": self._contrib(net_carbon_g, carbon_std),
            "quality_score": self._contrib(quality, quality_std),
            "memory_pressure": self._contrib(
                peak_gpu_mem_gb / max(gpu_memory_gb, 1e-9), 0.02
            ),
            "transfer_share": self._contrib(
                transfer_time_s / max(total_time_s, 1e-9), 0.02
            ),
        }

        # -- Provenance record (Enhancement 5) ----------------------
        provenance = None
        if self.enable_provenance:
            provenance = {
                "execution_id": exec_id,
                "timestamp": t_start,
                "node_id": getattr(node, "id", "unknown"),
                "workload_id": getattr(workload, "task_id", "unknown"),
                "policy_hash": policy.policy_hash(),
                "role_tag": context.role_tag if context else None,
                "precision_level": precision_level.value,
                "mixed_precision": mixed,
                "fallback_applied": fallback_applied,
                "chaos_events": chaos_events,
                "vector_schema": POLICY_VECTOR_VERSION,
            }

        return {
            # core metrics
            "success": success,
            "gpu_memory_used_gb": peak_gpu_mem_gb,
            "cpu_memory_used_gb": peak_cpu_mem_gb,
            "disk_io_gb": disk_io_gb,
            "latency_ms": latency_ms,
            "throughput_tokens_per_s": throughput_tokens_per_s,
            "prefill_tokens_per_s": prefill_tps,
            "decode_tokens_per_s": decode_tps,
            "energy_joules": energy_j,
            "carbon_g": net_carbon_g,
            "gross_carbon_g": gross_carbon_g,
            "quality_score": quality,
            # uncertainty
            "latency_std_ms": latency_std,
            "energy_std_j": energy_std,
            "carbon_std_g": carbon_std,
            "quality_std": quality_std,
            # market / REC
            "carbon_price_cost": carbon_price_cost,
            "rec_offset_kg": rec_offset_kg,
            "rec_kwh_used": rec_kwh_used,
            # precision
            "precision_level": precision_level.value,
            "mixed_precision": mixed,
            "effective_bits": eff_weight_bits,
            "effective_kv_bits": eff_kv_bits,
            "effective_speedup": speed_mult,
            # resilience / chaos
            "fallback_applied": fallback_applied,
            "chaos_events": chaos_events,
            # provenance / XAI
            "provenance": provenance,
            "attribution": attribution,
            # introspection
            "policy": policy.to_dict(),
        }

    # -- helpers -------------------------------------------------------
    @staticmethod
    def _contrib(value: float, std: float) -> Dict[str, float]:
        """Small per-feature contribution record for XAI consumers."""
        if std > 1e-12:
            snr = value / std
            snr = max(-SNR_CAP, min(SNR_CAP, snr))
        else:
            snr = SNR_CAP if value != 0 else 0.0
        return {"value": float(value), "std": float(std), "snr": float(snr)}

    def _failed_result(
        self,
        policy: FlexGenPolicy,
        exec_id: str,
        t_start: float,
        reason: str,
        chaos_events: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        v3: JSON-safe failure record. Uses FAILED_SENTINEL (1e12) instead of
        inf and -1.0 instead of nan so downstream serialization never chokes.
        """
        # Resolve a valid precision string even on failure.
        try:
            if policy.precision_level is not None:
                resolved = PrecisionLevel(policy.precision_level).value
            elif policy.weight_bits >= 16:
                resolved = PrecisionLevel.FP16.value
            elif policy.weight_bits == 8:
                resolved = PrecisionLevel.INT8.value
            else:
                resolved = PrecisionLevel.INT4.value
        except Exception:
            resolved = PrecisionLevel.FP32.value

        return {
            "success": False,
            "gpu_memory_used_gb": -1.0,
            "cpu_memory_used_gb": -1.0,
            "disk_io_gb": 0.0,
            "latency_ms": FAILED_SENTINEL,
            "throughput_tokens_per_s": 0.0,
            "prefill_tokens_per_s": 0.0,
            "decode_tokens_per_s": 0.0,
            "energy_joules": FAILED_SENTINEL,
            "carbon_g": FAILED_SENTINEL,
            "gross_carbon_g": FAILED_SENTINEL,
            "quality_score": 0.0,
            "latency_std_ms": 0.0,
            "energy_std_j": 0.0,
            "carbon_std_g": 0.0,
            "quality_std": 0.0,
            "carbon_price_cost": 0.0,
            "rec_offset_kg": 0.0,
            "rec_kwh_used": 0.0,
            "precision_level": resolved,
            "mixed_precision": bool(policy.mixed_precision),
            "effective_bits": float(policy.weight_bits),
            "effective_kv_bits": float(policy.kv_cache_bits),
            "effective_speedup": 1.0,
            "fallback_applied": False,
            "chaos_events": chaos_events or [],
            "provenance": {
                "execution_id": exec_id,
                "timestamp": t_start,
                "reason": reason,
                "policy_hash": policy.policy_hash(),
            },
            "attribution": {},
            "policy": policy.to_dict(),
        }


# ===========================================================================
# Candidate generators
# ===========================================================================
def generate_candidate_policies(n: int = 20, seed: Optional[int] = None) -> List[FlexGenPolicy]:
    """Generate diverse candidates via random sampling. Deterministic if seed given."""
    rng = random.Random(seed) if seed is not None else random
    policies: List[FlexGenPolicy] = []
    for _ in range(n):
        policies.append(
            FlexGenPolicy(
                gpu_batch_size=rng.choice([1, 2, 4, 8]),
                block_size=rng.choice([8, 16, 32, 64]),
                weight_device=rng.choice(["gpu", "cpu", "disk"]),
                activation_device=rng.choice(["gpu", "cpu"]),
                kv_cache_device=rng.choice(["gpu", "cpu", "disk"]),
                weight_bits=rng.choice([4, 8, 16]),
                kv_cache_bits=rng.choice([4, 8, 16]),
                cpu_attention=rng.random() < 0.3,
                overlap_io_compute=rng.random() < 0.7,
                precision_level=rng.choice(
                    [None] + [p.value for p in PrecisionLevel]
                ),
                mixed_precision=rng.random() < 0.25,
            )
        )
    return policies


def generate_grid_policies(
    node: Optional[NodeDescriptor] = None,
    workload: Optional[WorkloadDescriptor] = None,
    max_policies: int = 128,
) -> List[FlexGenPolicy]:
    """
    Deterministic structured grid. Node/workload, when provided, prune
    infeasible combinations up front (v3: KV cache and activation are
    also included in the feasibility check).
    """
    policies: List[FlexGenPolicy] = []
    for batch in [1, 2, 4, 8]:
        for weight_device in ["gpu", "cpu", "disk"]:
            for kv_device in ["gpu", "cpu", "disk"]:
                for bits in [4, 8, 16]:
                    policies.append(
                        FlexGenPolicy(
                            gpu_batch_size=batch,
                            block_size=16,
                            weight_device=weight_device,
                            activation_device="gpu",
                            kv_cache_device=kv_device,
                            weight_bits=bits,
                            kv_cache_bits=bits,
                            cpu_attention=False,
                            overlap_io_compute=True,
                            mixed_precision=False,
                        )
                    )
    # Node/workload pruning (deterministic, not random).
    if node is not None and workload is not None:
        gpu_mem = float(node.metadata.get("gpu_memory_gb", 16.0))
        cpu_mem = float(node.metadata.get("cpu_memory_gb", 64.0))
        params_b = float(workload.metadata.get("model_params", {}).get("params_billions", 7))
        layers = int(workload.metadata.get("model_params", {}).get("num_layers", 32))
        hidden = int(workload.metadata.get("model_params", {}).get("hidden_dim", 4096))
        seq = int(workload.tokens)
        max_new = int(workload.metadata.get("max_new_tokens", 32))

        feasible: List[FlexGenPolicy] = []
        for p in policies:
            approx_model_gb = params_b * (p.weight_bits / 8.0)
            approx_kv_gb = (
                p.gpu_batch_size * (seq + max_new) * 2 * hidden * layers * (p.kv_cache_bits / 8.0)
            ) / 1e9
            approx_act_gb = (
                p.gpu_batch_size * seq * hidden * layers * 2 * 4
            ) / 1e9

            w_gpu = p.weight_device == "gpu"
            kv_gpu = p.kv_cache_device == "gpu"
            act_gpu = p.activation_device == "gpu"

            gpu_mem_req = (
                (approx_model_gb if w_gpu else 0)
                + (approx_kv_gb if kv_gpu else 0)
                + (approx_act_gb if act_gpu else 0)
            )
            cpu_mem_req = (
                (approx_model_gb if p.weight_device == "cpu" else 0)
                + (approx_kv_gb if p.kv_cache_device == "cpu" else 0)
                + (approx_act_gb if not act_gpu else 0)
            )
            if gpu_mem_req > gpu_mem:
                continue
            if cpu_mem_req > cpu_mem:
                continue
            feasible.append(p)
        policies = feasible
    return policies[:max_policies]


def generate_heuristic_policies(
    node: Optional[NodeDescriptor] = None,
    workload: Optional[WorkloadDescriptor] = None,
) -> List[FlexGenPolicy]:
    """
    Node/workload-aware heuristics. Emits low-latency, low-memory,
    high-throughput, and balanced candidates tuned to the actual memory budget.
    """
    gpu_mem = float(node.metadata.get("gpu_memory_gb", 16.0)) if node else 16.0
    cpu_mem = float(node.metadata.get("cpu_memory_gb", 64.0)) if node else 64.0
    params_b = (
        float(workload.metadata.get("model_params", {}).get("params_billions", 7))
        if workload else 7.0
    )
    model_fp16_gb = params_b * 2.0

    policies: List[FlexGenPolicy] = []

    # Low-latency: prefer GPU if it fits, else CPU with FP16.
    if model_fp16_gb <= gpu_mem * 0.8:
        policies.append(
            FlexGenPolicy(
                gpu_batch_size=2, block_size=16,
                weight_device="gpu", activation_device="gpu", kv_cache_device="gpu",
                weight_bits=16, kv_cache_bits=16,
                cpu_attention=False, overlap_io_compute=True,
                precision_level=PrecisionLevel.FP16.value,
            )
        )

    # Low-memory: CPU weights, CPU KV, INT8.
    if model_fp16_gb * 0.25 <= cpu_mem * 0.8:
        policies.append(
            FlexGenPolicy(
                gpu_batch_size=4, block_size=32,
                weight_device="cpu", activation_device="cpu", kv_cache_device="cpu",
                weight_bits=8, kv_cache_bits=8,
                cpu_attention=True, overlap_io_compute=True,
                precision_level=PrecisionLevel.INT8.value,
            )
        )

    # High throughput: disk offload, INT4, overlapped.
    policies.append(
        FlexGenPolicy(
            gpu_batch_size=8, block_size=64,
            weight_device="disk", activation_device="gpu", kv_cache_device="disk",
            weight_bits=4, kv_cache_bits=4,
            cpu_attention=False, overlap_io_compute=True,
            precision_level=PrecisionLevel.INT4.value,
        )
    )

    # Balanced: weights CPU, KV GPU, FP8 mixed.
    policies.append(
        FlexGenPolicy(
            gpu_batch_size=4, block_size=32,
            weight_device="cpu", activation_device="gpu", kv_cache_device="gpu",
            weight_bits=8, kv_cache_bits=8,
            cpu_attention=False, overlap_io_compute=True,
            precision_level=PrecisionLevel.FP8.value,
            mixed_precision=True,
        )
    )
    return policies


# ===========================================================================
# Reward (workload-relative, carbon-price-aware)
# ===========================================================================
def compute_reward(
    metrics: Dict[str, Any],
    workload: WorkloadDescriptor,
    baseline: Optional[Dict[str, float]] = None,
) -> float:
    """
    Workload-relative reward with:

      - latency score (relative to workload target)
      - energy score (relative to baseline or estimated budget)
      - carbon score (relative to baseline or estimated budget)
      - success bonus
      - quality term
      - v3: carbon price cost penalty (capped)
      - v3: latency-variance penalty (capped)
    """
    latency_target = max(float(workload.latency_target), 1.0)
    latency_score = max(0.0, 1.0 - float(metrics.get("latency_ms", 0.0)) / latency_target)

    if baseline and baseline.get("energy_joules", 0) > 0:
        ref_energy = baseline["energy_joules"]
    else:
        ref_energy = max(50.0, workload.tokens * 5.0)
    if baseline and baseline.get("carbon_g", 0) > 0:
        ref_carbon = baseline["carbon_g"]
    else:
        ref_carbon = max(1.0, ref_energy / 3.6e6 * 500.0)  # ~500 g/kWh

    energy_score = max(0.0, 1.0 - float(metrics.get("energy_joules", 0.0)) / ref_energy)
    carbon_score = max(0.0, 1.0 - float(metrics.get("carbon_g", 0.0)) / ref_carbon)

    success_bonus = 1.0 if metrics.get("success", False) else 0.0
    quality = float(metrics.get("quality_score", 0.9))

    reward = (
        0.30 * quality
        + 0.25 * latency_score
        + 0.20 * energy_score
        + 0.15 * carbon_score
        + 0.10 * success_bonus
    )

    # v3: carbon price penalty. Small but non-negligible.
    cpc = float(metrics.get("carbon_price_cost", 0.0))
    if cpc > 0:
        reward -= min(0.10, cpc)

    # v3: latency-variance penalty. High-variance policies are less trustworthy.
    latency_std = float(metrics.get("latency_std_ms", 0.0))
    if latency_std > 0 and latency_target > 0:
        reward -= min(0.05, 0.5 * latency_std / latency_target)

    return max(0.0, min(1.0, reward))


# ===========================================================================
# Convenience helpers for downstream enhancements
# ===========================================================================
def policy_feature_matrix(policies: Sequence[FlexGenPolicy]) -> List[List[float]]:
    """Batch-to-matrix for distillation/causal/federated consumers."""
    return [p.to_vector() for p in policies]


def policy_schema() -> Dict[str, Any]:
    """Expose the versioned vector schema to controller-side modules."""
    return {
        "version": POLICY_VECTOR_VERSION,
        "fields": list(POLICY_VECTOR_FIELDS),
        "dim": FEATURE_VECTOR_DIM,
        "precision_order": [p.value for p in PRECISION_ORDER],
        "precision_cost": {k.value: v for k, v in PRECISION_COST.items()},
    }
