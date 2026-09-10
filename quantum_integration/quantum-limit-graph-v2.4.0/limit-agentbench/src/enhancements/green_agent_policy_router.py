"""
green_agent_policy_router.py — Enhanced v2

Single-file integration of all ten Green Agent enhancements:

  1. Quantum-Distillation Integration      QuantumInspiredTeacher + DistillationEnsemble
  2. Causal RL for Policy Adaptation       CausalCounterfactualEstimator
  3. Federated Green Learning              FederatedAggregator
  4. Multi-Agent Coordination              AgentRole + EmergentRoleRegistry + MultiAgentCoordinator
  5. Temporal Logic / Formal Verification  STLFormula + TemporalLogicMonitor + SafetyShield
  6. Explainable AI                        DecisionExplainer + Explanation
  7. Adaptive Precision Switching          PrecisionController + HardwareAwareAdapter
  8. Carbon Markets / RECs                 CarbonMarketClient + RECInventory
  9. Resilience / Chaos                    ChaosEngineer
 10. HITL / Active Learning                UncertaintyEstimator + HumanInTheLoopGate + ActiveLearningSampler

Bug fixes over v1:
  - ContextualBandit is defined locally with one canonical API
  - select_action called with one signature everywhere
  - Bandit internals exposed via public methods (add_action, seed_action, trials, encode_context)
  - MoE.encode shimmed to accept 1 or 2 args
  - _generate_fingerprint no longer mutates the input task
  - Persistence is atomic (tmp + rename) with a lock and no __del__ I/O
  - Bio expansion guarded with try/except and dict-hashable checks
  - Heap tie-break uses a monotonic counter (no dict comparison)
  - Metric/Reward interfaces normalized through local fallbacks
  - Optional GC: atexit + context manager, not __del__
  - Statistics counters actually count each addition
  - health_check() added
"""

from __future__ import annotations

import atexit
import hashlib
import heapq
import itertools
import json
import logging
import math
import os
import random
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

# ============================================================
# Local fallback sub-modules (router must run even without them)
# ============================================================


# -- CarbonDelayScheduler fallback ---------------------------------
@dataclass
class _DelayedTask:
    time: float
    task: Dict[str, Any]
    seq: int = 0


class CarbonDelayScheduler:
    """
    Carbon-aware delay scheduler. Delays non-critical tasks when carbon
    is above a threshold and a forecast window looks cleaner.
    """

    def __init__(self, carbon_api, threshold: float = 400.0, max_delay_s: float = 300.0):
        self.carbon_api = carbon_api
        self.threshold = threshold
        self.max_delay_s = max_delay_s
        self.queue: List[_DelayedTask] = []
        self._seq = itertools.count()
        self._rewards: List[float] = []

    def submit(self, task: Dict[str, Any]) -> Dict[str, Any]:
        critical = bool(task.get("critical", False))
        current = float(self.carbon_api.get_current())
        if critical or current <= self.threshold:
            return {"status": "immediate"}
        forecast = getattr(self.carbon_api, "get_forecast", None)
        delay_until = time.time() + self.max_delay_s
        if callable(forecast):
            try:
                points = forecast()
                if points:
                    best_t = min(
                        range(len(points)),
                        key=lambda i: points[i],
                    )
                    delay_until = time.time() + min(self.max_delay_s, float(best_t) * 60.0)
            except Exception:
                pass
        item = _DelayedTask(time=delay_until, task=task, seq=next(self._seq))
        heapq.heappush(self.queue, (item.time, item.seq, item))
        return {
            "status": "delayed",
            "reason": "high_carbon",
            "delay_until": delay_until,
        }

    def tick(self) -> List[Dict[str, Any]]:
        now = time.time()
        released: List[Dict[str, Any]] = []
        while self.queue and self.queue[0][0] <= now:
            _, _, item = heapq.heappop(self.queue)
            released.append(item.task)
        return released

    def report_reward(self, task: Dict[str, Any], reward: float) -> None:
        self._rewards.append(float(reward))

    def stats(self) -> Dict[str, Any]:
        return {"queue_size": len(self.queue), "rewards_recorded": len(self._rewards)}


# -- PolicyMetaCache fallback --------------------------------------
@dataclass(frozen=True)
class WorkloadFingerprint:
    model_size_mb: float
    prompt_len: int
    gen_len: int
    gpu_mem_free_mb: float
    disk_speed_class: int

    def to_tuple(self) -> Tuple[Any, ...]:
        return (
            float(self.model_size_mb),
            int(self.prompt_len),
            int(self.gen_len),
            float(self.gpu_mem_free_mb),
            int(self.disk_speed_class),
        )


class PolicyMetaCache:
    """Simple policy cache keyed by fingerprint, with reward tracking."""

    def __init__(self, max_size: int = 512):
        self.max_size = max_size
        self.store: Dict[Tuple, Tuple[Dict, float, int]] = {}
        self._lock = threading.Lock()

    def get_best_policy(self, fp: WorkloadFingerprint) -> Optional[Dict]:
        with self._lock:
            entry = self.store.get(fp.to_tuple())
            if entry is None:
                return None
            return dict(entry[0])

    def update(self, fp: WorkloadFingerprint, policy: Dict, reward: float) -> None:
        key = fp.to_tuple()
        with self._lock:
            prev = self.store.get(key)
            if prev is None:
                self.store[key] = (dict(policy), float(reward), 1)
            else:
                prev_policy, prev_reward, count = prev
                count += 1
                avg = prev_reward + (float(reward) - prev_reward) / count
                if reward >= prev_reward:
                    self.store[key] = (dict(policy), avg, count)
                else:
                    self.store[key] = (prev_policy, avg, count)
            if len(self.store) > self.max_size:
                # Evict oldest by insertion order
                self.store.pop(next(iter(self.store)))

    def __len__(self):
        return len(self.store)


# -- ContextualBandit (local, canonical API) -----------------------
class ContextualBandit:
    """
    Linear UCB-style contextual bandit over a policy action space.

    Canonical API:
      - select_action(context, min_trials=..., confidence_threshold=...) -> (policy|None, confidence)
      - update(context, policy, reward)
      - seed_action(context, policy, reward)
      - add_action(policy)
      - get_trials(context) -> int
      - encode_context(context) -> hashable key
    """

    def __init__(
        self,
        action_space: Sequence[Dict[str, Any]],
        fallback_solver: Optional[Callable] = None,
        modp_weights: Optional[Dict[str, float]] = None,
        moe_router: Optional[Any] = None,
        bio_generator: Optional[Any] = None,
        feature_dim: int = 8,
        alpha: float = 0.3,
    ):
        self.action_space: List[Dict[str, Any]] = [dict(p) for p in action_space]
        self.fallback_solver = fallback_solver
        self.modp_weights = modp_weights or {}
        self.moe_router = moe_router
        self.bio_generator = bio_generator
        self.feature_dim = feature_dim
        self.alpha = alpha

        # Per-context per-action statistics
        self._weights: Dict[Tuple, np.ndarray] = {}
        self._covariances: Dict[Tuple, np.ndarray] = {}
        self._trials: Dict[Tuple, int] = {}
        self._rewards: Dict[Tuple, List[float]] = {}
        self._policy_index: Dict[str, int] = {}
        self._lock = threading.Lock()
        self._rebuild_index()

    # -- internals ---------------------------------------------------
    def _rebuild_index(self) -> None:
        self._policy_index = {self._canonical(p): i for i, p in enumerate(self.action_space)}

    @staticmethod
    def _canonical(policy: Dict[str, Any]) -> str:
        return json.dumps(policy, sort_keys=True)

    def encode_context(self, context: Dict[str, Any]) -> Tuple:
        """Public context encoder (was _encode_context)."""
        fp = context.get("fingerprint")
        parts: List[Any] = []
        if isinstance(fp, WorkloadFingerprint):
            parts.extend(fp.to_tuple())
        elif isinstance(fp, (tuple, list)):
            parts.extend(list(fp))
        elif isinstance(fp, dict):
            parts.extend(sorted(fp.values(), key=lambda x: str(x)))
        hw = context.get("hardware_state") or {}
        parts.append(round(float(hw.get("gpu_utilization_pct", 0.0)), 2))
        parts.append(round(float(hw.get("gpu_power_watts", 0.0)), 1))
        return tuple(parts)

    def _features(self, context: Dict[str, Any]) -> np.ndarray:
        fp = context.get("fingerprint")
        v: List[float] = []
        if isinstance(fp, WorkloadFingerprint):
            v = list(fp.to_tuple())
        elif isinstance(fp, (tuple, list)):
            v = [float(x) for x in fp]
        elif isinstance(fp, dict):
            v = [float(x) for x in fp.values() if isinstance(x, (int, float))]
        hw = context.get("hardware_state") or {}
        v.append(float(hw.get("gpu_utilization_pct", 0.0)))
        v.append(float(hw.get("gpu_power_watts", 0.0)))
        arr = np.asarray(v, dtype=np.float64)
        if arr.size < self.feature_dim:
            arr = np.pad(arr, (0, self.feature_dim - arr.size))
        return arr[: self.feature_dim]

    def _ensure_context(self, key: Tuple) -> None:
        n = len(self.action_space)
        if key not in self._weights:
            self._weights[key] = np.zeros((self.feature_dim, n))
            self._covariances[key] = np.eye(self.feature_dim) * 1.0
            self._trials[key] = 0
            self._rewards[key] = [0.0] * n
        else:
            # Handle action space growth
            cur_n = self._weights[key].shape[1]
            if cur_n < n:
                pad_w = np.zeros((self.feature_dim, n - cur_n))
                self._weights[key] = np.hstack([self._weights[key], pad_w])
                self._covariances[key] = np.eye(self.feature_dim) * 1.0
                self._rewards[key] = self._rewards[key] + [0.0] * (n - cur_n)

    # -- public API --------------------------------------------------
    def select_action(
        self,
        context: Dict[str, Any],
        min_trials_before_bandit: int = 0,
        confidence_threshold: float = 0.0,
        **kwargs,
    ) -> Tuple[Optional[Dict], float]:
        key = self.encode_context(context)
        with self._lock:
            self._ensure_context(key)
            n = len(self.action_space)
            trials = self._trials[key]
            if trials < min_trials_before_bandit:
                return None, 0.0
            x = self._features(context)
            w = self._weights[key]
            mean = x @ w
            # Upper confidence bound
            try:
                cov = self._covariances[key]
                ucb = self.alpha * np.sqrt(np.maximum(np.diag(x @ cov @ x.T) if x.ndim == 1 else 0.0, 0.0))
                ucb = np.full(n, float(self.alpha * np.linalg.norm(x)))
            except Exception:
                ucb = np.full(n, float(self.alpha))
            scores = mean + ucb
            best = int(np.argmax(scores))
            confidence = float(1.0 / (1.0 + math.exp(-scores[best])))
            if confidence < confidence_threshold:
                return None, confidence
            return dict(self.action_space[best]), confidence

    def update(self, context: Dict[str, Any], policy: Dict[str, Any], reward: float) -> None:
        key = self.encode_context(context)
        with self._lock:
            self._ensure_context(key)
            idx = self._policy_index.get(self._canonical(policy))
            if idx is None:
                self.add_action(policy)
                idx = self._policy_index[self._canonical(policy)]
            x = self._features(context)
            w = self._weights[key]
            if idx >= w.shape[1]:
                pad_w = np.zeros((self.feature_dim, idx + 1 - w.shape[1]))
                self._weights[key] = np.hstack([w, pad_w])
                w = self._weights[key]
                self._rewards[key] = self._rewards[key] + [0.0] * (idx + 1 - len(self._rewards[key]))
            pred = float(x @ w[:, idx])
            w[:, idx] += 0.05 * (float(reward) - pred) * x
            self._rewards[key][idx] = float(reward)
            self._trials[key] += 1
            # Simple covariance update
            cov = self._covariances[key]
            denom = 1.0 + float(x @ cov @ x)
            if denom > 1e-9:
                cov -= np.outer(cov @ x, cov @ x) / denom

    def seed_action(self, context: Dict[str, Any], policy: Dict[str, Any], reward: float = 1.0) -> None:
        self.add_action(policy)
        self.update(context, policy, reward)

    def add_action(self, policy: Dict[str, Any]) -> int:
        canonical = self._canonical(policy)
        if canonical in self._policy_index:
            return self._policy_index[canonical]
        with self._lock:
            self.action_space.append(dict(policy))
            idx = len(self.action_space) - 1
            self._policy_index[canonical] = idx
            for key in list(self._weights.keys()):
                w = self._weights[key]
                if w.shape[1] <= idx:
                    pad_w = np.zeros((self.feature_dim, idx + 1 - w.shape[1]))
                    self._weights[key] = np.hstack([w, pad_w])
                    self._rewards[key] = self._rewards[key] + [0.0] * (idx + 1 - len(self._rewards[key]))
            return idx

    def get_trials(self, context: Dict[str, Any]) -> int:
        key = self.encode_context(context)
        return int(self._trials.get(key, 0))

    def get_trials_for_policy(self, context: Dict[str, Any], policy: Dict[str, Any]) -> int:
        key = self.encode_context(context)
        idx = self._policy_index.get(self._canonical(policy))
        if idx is None:
            return 0
        return int(self._rewards.get(key, [0.0] * (idx + 1))[idx])

    def export_state(self) -> Dict[str, Any]:
        return {
            "action_space": [dict(p) for p in self.action_space],
            "weights": [(list(k), v.tolist()) for k, v in self._weights.items()],
            "covariances": [(list(k), v.tolist()) for k, v in self._covariances.items()],
            "trials": [(list(k), v) for k, v in self._trials.items()],
            "rewards": [(list(k), v) for k, v in self._rewards.items()],
        }

    def import_state(self, data: Dict[str, Any]) -> None:
        if not data:
            return
        with self._lock:
            self.action_space = [dict(p) for p in data.get("action_space", self.action_space)]
            self._rebuild_index()
            self._weights = {tuple(k): np.asarray(v, dtype=np.float64) for k, v in data.get("weights", [])}
            self._covariances = {tuple(k): np.asarray(v, dtype=np.float64) for k, v in data.get("covariances", [])}
            self._trials = {tuple(k): int(v) for k, v in data.get("trials", [])}
            self._rewards = {tuple(k): list(v) for k, v in data.get("rewards", [])}


# -- MetricAggregator + RewardCalculator + GPUProfiler fallbacks ---
class _NullProfiler:
    def get_current_metrics(self) -> Dict[str, Any]:
        return {}

    def start(self): pass
    def stop(self): pass


class GPUProfiler:
    """Fallback profiler if the real one isn't available."""

    def __init__(self, **kwargs):
        self._running = False

    def get_current_metrics(self) -> Dict[str, Any]:
        return {
            "gpu_utilization_pct": random.random(),
            "gpu_power_watts": 200.0 + random.random() * 50,
            "cpu_utilization_pct": random.random(),
            "timestamp": time.time(),
        }

    def start(self): self._running = True
    def stop(self): self._running = False
    def health_check(self): return {"status": "ok" if self._running else "degraded"}


class MetricAggregator:
    def __init__(self, profiler, executor):
        self.profiler = profiler
        self.executor = executor

    def run(self, task: Dict, policy: Dict) -> Dict[str, Any]:
        before = {}
        try:
            before = self.profiler.get_current_metrics()
        except Exception:
            pass
        start = time.time()
        try:
            metrics = self.executor(task, policy)
        except TypeError:
            # Maybe the executor expects (policy, task)
            metrics = self.executor(policy, task)
        duration_s = time.time() - start
        try:
            after = self.profiler.get_current_metrics()
        except Exception:
            after = {}
        if isinstance(metrics, dict):
            metrics.setdefault("duration_s", duration_s)
            metrics.setdefault("timestamp", time.time())
            if "energy_joules" not in metrics and before and after:
                avg_power = (float(before.get("gpu_power_watts", 0)) + float(after.get("gpu_power_watts", 0))) / 2.0
                metrics["energy_joules"] = avg_power * duration_s
        return metrics


class RewardCalculator:
    def __init__(self, weights: Optional[Dict[str, float]] = None, carbon_intensity_default: float = 400.0):
        self.weights = weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy_efficiency": 0.20,
            "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10,
        }
        self.carbon_intensity_default = carbon_intensity_default

    def compute(
        self,
        metrics: Dict[str, Any],
        constraints: Optional[Dict[str, Any]] = None,
        carbon_intensity: Optional[float] = None,
    ) -> float:
        quality = float(metrics.get("quality_score", 0.9))
        throughput = float(metrics.get("tokens_per_sec", 0.0))
        energy = float(metrics.get("energy_joules", 0.0))
        carbon = float(carbon_intensity if carbon_intensity is not None else self.carbon_intensity_default)

        # Normalize sub-scores into [0,1]
        throughput_score = min(1.0, throughput / 100.0) if throughput > 0 else 0.5
        energy_score = 1.0 - min(1.0, energy / 100.0) if energy > 0 else 0.5
        carbon_kg = max(1e-6, (energy / 3.6e6) * carbon / 1000.0)
        carbon_score = min(1.0, 1.0 / (1.0 + carbon_kg * 10.0))
        mem_used = float(metrics.get("gpu_memory_used_mb", 0.0))
        mem_total = float(metrics.get("gpu_memory_total_mb", 1.0)) or 1.0
        mem_score = max(0.0, 1.0 - mem_used / mem_total)

        score = (
            self.weights.get("quality", 0.3) * quality
            + self.weights.get("throughput", 0.25) * throughput_score
            + self.weights.get("energy_efficiency", 0.2) * energy_score
            + self.weights.get("carbon_efficiency", 0.15) * carbon_score
            + self.weights.get("memory_efficiency", 0.1) * mem_score
        )
        # Constraint penalties
        if constraints:
            if constraints.get("max_latency_ms") and metrics.get("latency_ms", 0) > constraints["max_latency_ms"]:
                score -= 0.3
            if constraints.get("max_carbon_g") and metrics.get("carbon_g", 0) > constraints["max_carbon_g"]:
                score -= 0.3
        return float(max(0.0, min(1.0, score)))


# ============================================================
# ============================================================
# TEN ENHANCEMENT MODULES (self-contained)
# ============================================================
# ============================================================


# ------------------------------------------------------------
# Enhancement 7: Adaptive Precision Switching
# ------------------------------------------------------------
class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    BF16 = "bf16"
    FP16 = "fp16"
    FP8 = "fp8"
    INT8 = "int8"
    INT4 = "int4"


PRECISION_COST = {
    PrecisionLevel.FP32: {"speed": 1.0, "energy": 1.00, "quality": 1.000, "memory": 1.00, "bits": 32.0},
    PrecisionLevel.BF16: {"speed": 1.7, "energy": 0.72, "quality": 0.997, "memory": 0.50, "bits": 16.0},
    PrecisionLevel.FP16: {"speed": 2.0, "energy": 0.65, "quality": 0.994, "memory": 0.50, "bits": 16.0},
    PrecisionLevel.FP8:  {"speed": 3.1, "energy": 0.50, "quality": 0.985, "memory": 0.25, "bits": 8.0},
    PrecisionLevel.INT8: {"speed": 3.6, "energy": 0.44, "quality": 0.972, "memory": 0.25, "bits": 8.0},
    PrecisionLevel.INT4: {"speed": 5.0, "energy": 0.32, "quality": 0.905, "memory": 0.125, "bits": 4.0},
}


class PrecisionController:
    def __init__(self, supported: Optional[Sequence[PrecisionLevel]] = None,
                 quality_floor: float = 0.95, carbon_aware: bool = True):
        self.supported = list(supported) if supported else list(PrecisionLevel)
        self.quality_floor = quality_floor
        self.carbon_aware = carbon_aware

    def select(self, carbon_intensity: float, latency_headroom_ratio: float,
               carbon_price: float = 0.0) -> PrecisionLevel:
        stress = min(1.0, max(0.0, carbon_intensity / 600.0)) if self.carbon_aware else 0.0
        price_stress = min(1.0, max(0.0, carbon_price / 0.2))
        headroom = min(1.0, max(0.0, latency_headroom_ratio))
        agg = 0.5 * stress + 0.3 * price_stress + 0.2 * (1.0 - headroom)
        ordered = [p for p in PrecisionLevel if p in self.supported]
        ordered.sort(key=lambda p: PRECISION_COST[p]["energy"])
        chosen = ordered[0]
        for i, p in enumerate(ordered):
            if PRECISION_COST[p]["quality"] >= self.quality_floor and agg >= 0.25 * i:
                chosen = p
        return chosen


class HardwareAwareAdapter:
    def __init__(self, controller: PrecisionController):
        self.controller = controller

    def adapt(self, policy: Dict[str, Any], carbon_intensity: float,
              latency_headroom: float, carbon_price: float) -> Tuple[Dict[str, Any], PrecisionLevel]:
        level = self.controller.select(carbon_intensity, latency_headroom, carbon_price)
        out = dict(policy)
        out["precision_level"] = level.value
        out["effective_bits"] = PRECISION_COST[level]["bits"]
        return out, level


# ------------------------------------------------------------
# Enhancement 8: Carbon Markets / RECs
# ------------------------------------------------------------
class CarbonMarketClient:
    def __init__(self, base_price: float = 0.05, sensitivity: float = 0.0005):
        self.base_price = base_price
        self.sensitivity = sensitivity

    def price(self, carbon_intensity: float, hour_of_day: Optional[int] = None) -> float:
        tod = 1.0
        if hour_of_day is not None:
            tod = 1.0 + 0.3 * math.sin((hour_of_day / 24.0) * 2 * math.pi)
        return self.base_price + self.sensitivity * carbon_intensity * tod


@dataclass
class RECRecord:
    kwh: float
    issued_at: float
    source: str = "solar"


class RECInventory:
    def __init__(self, grid_kg_co2_per_kwh: float = 0.4):
        self.grid_factor = grid_kg_co2_per_kwh
        self._records: List[RECRecord] = []

    def add(self, kwh: float, source: str = "solar") -> None:
        self._records.append(RECRecord(kwh=kwh, issued_at=time.time(), source=source))

    def total_kwh(self) -> float:
        return sum(r.kwh for r in self._records)

    def consume(self, kwh: float) -> float:
        remaining = kwh
        offset = 0.0
        new_records: List[RECRecord] = []
        for r in self._records:
            if remaining <= 0:
                new_records.append(r)
                continue
            take = min(r.kwh, remaining)
            remaining -= take
            offset += take * self.grid_factor
            if r.kwh - take > 1e-9:
                new_records.append(RECRecord(kwh=r.kwh - take, issued_at=r.issued_at, source=r.source))
        self._records = new_records
        return offset


# ------------------------------------------------------------
# Enhancement 9: Resilience / Chaos
# ------------------------------------------------------------
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


class ChaosEngineer:
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
        if self.config.latency_inject_ms > 0 and self.rng.random() < self.config.latency_inject_prob:
            self.events.append({"type": "latency", "t": time.time()})
            return self.config.latency_inject_ms
        return 0.0

    def maybe_carbon_spike(self, carbon_intensity: float) -> float:
        if self.rng.random() < self.config.carbon_spike_prob:
            self.events.append({"type": "carbon_spike", "t": time.time()})
            return carbon_intensity * self.config.carbon_spike_factor
        return carbon_intensity


class CircuitBreaker:
    """Minimal circuit breaker around external calls."""

    def __init__(self, name: str, failure_threshold: int = 3, recovery_s: float = 10.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_s = recovery_s
        self._failures = 0
        self._open_until = 0.0

    def allow(self) -> bool:
        return time.time() >= self._open_until

    def record(self, success: bool) -> None:
        if success:
            self._failures = 0
        else:
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._open_until = time.time() + self.recovery_s
                self._failures = 0


# ------------------------------------------------------------
# Enhancement 5: Temporal Logic / Formal Verification
# ------------------------------------------------------------
class STLOperator(str, Enum):
    ALWAYS = "G"
    EVENTUALLY = "F"
    UNTIL = "U"


@dataclass
class STLFormula:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    operator: STLOperator
    horizon: int = 10


class TemporalLogicMonitor:
    def __init__(self, horizon: int = 10):
        self.horizon = horizon
        self._history: List[Dict[str, Any]] = []
        self.formulas: List[STLFormula] = []

    def add_formula(self, f: STLFormula) -> None:
        self.formulas.append(f)

    def observe(self, record: Dict[str, Any]) -> None:
        self._history.append(record)
        max_len = self.horizon * 4
        if len(self._history) > max_len:
            self._history = self._history[-max_len:]

    def verify(self) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for f in self.formulas:
            window = self._history[-f.horizon:]
            if not window:
                results[f.name] = True
                continue
            sat = [bool(f.predicate(r)) for r in window]
            if f.operator == STLOperator.ALWAYS:
                results[f.name] = all(sat)
            elif f.operator in (STLOperator.EVENTUALLY, STLOperator.UNTIL):
                results[f.name] = any(sat)
            else:
                results[f.name] = True
        return results


class SafetyShield:
    """Rewrites unsafe policy selections to the safest candidate."""

    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor
        self.violations: List[Dict[str, Any]] = []

    def screen(
        self,
        selected_policy: Dict[str, Any],
        candidates: Sequence[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], bool, Dict[str, bool]]:
        verdict = self.monitor.verify()
        violated = [k for k, ok in verdict.items() if not ok]
        if not violated:
            return selected_policy, True, verdict
        if not candidates:
            return selected_policy, False, verdict
        # Fallback = smallest gpu_batch_size among candidates
        safe = min(candidates, key=lambda p: int(p.get("gpu_batch_size", 1)))
        self.violations.append({"t": time.time(), "violated": violated})
        return dict(safe), False, verdict


# ------------------------------------------------------------
# Enhancement 2: Causal RL
# ------------------------------------------------------------
@dataclass
class CausalTransition:
    state: np.ndarray
    action: int
    reward: float
    next_state: np.ndarray
    context: Dict[str, float] = field(default_factory=dict)


class CausalCounterfactualEstimator:
    def __init__(self, state_dim: int, n_actions: int, ridge: float = 1e-3):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.ridge = ridge
        self._A = np.eye(state_dim + n_actions + 1) * ridge
        self._b = np.zeros(state_dim + n_actions + 1)
        self._n = 0

    def _features(self, s, a):
        one_hot = np.zeros(self.n_actions)
        one_hot[a % self.n_actions] = 1.0
        return np.concatenate([np.asarray(s, dtype=np.float64), one_hot, [1.0]])

    def _ensure_actions(self, n):
        if n == self.n_actions:
            return
        self._A = np.eye(self.state_dim + n + 1) * self.ridge
        self._b = np.zeros(self.state_dim + n + 1)
        self.n_actions = n
        self._n = 0

    def update(self, t: CausalTransition) -> None:
        x = self._features(t.state, t.action)
        self._A += np.outer(x, x)
        self._b += t.reward * x
        self._n += 1

    def predict(self, s, a):
        if self._n < 2:
            return 0.0
        try:
            theta = np.linalg.solve(self._A, self._b)
        except np.linalg.LinAlgError:
            return 0.0
        return float(self._features(s, a) @ theta)

    def counterfactuals(self, s, n_actions):
        self._ensure_actions(n_actions)
        return {a: self.predict(s, a) for a in range(n_actions)}


# ------------------------------------------------------------
# Enhancement 3: Federated Green Learning
# ------------------------------------------------------------
@dataclass
class FederatedUpdate:
    node_id: str
    weights: Dict[str, np.ndarray]
    n_samples: int
    carbon_intensity: float
    timestamp: float = field(default_factory=time.time)


class FederatedAggregator:
    def __init__(self, dp_sigma: float = 1e-3, staleness_s: float = 3600.0):
        self.dp_sigma = dp_sigma
        self.staleness_s = staleness_s
        self._updates: Dict[str, FederatedUpdate] = {}
        self._global: Optional[Dict[str, np.ndarray]] = None

    def submit(self, u: FederatedUpdate) -> None:
        self._updates[u.node_id] = u

    def _fresh(self):
        now = time.time()
        return [u for u in self._updates.values() if (now - u.timestamp) <= self.staleness_s]

    def aggregate(self) -> Optional[Dict[str, np.ndarray]]:
        fresh = self._fresh()
        if not fresh:
            return self._global
        weights = np.array([u.n_samples / max(u.carbon_intensity, 1.0) for u in fresh])
        weights /= max(weights.sum(), 1e-12)
        keys = fresh[0].weights.keys()
        agg = {}
        for k in keys:
            stacked = np.stack([u.weights[k] for u in fresh], axis=0)
            blended = np.tensordot(weights, stacked, axes=([0], [0]))
            if self.dp_sigma > 0:
                blended = blended + np.random.normal(0.0, self.dp_sigma, size=blended.shape)
            agg[k] = blended
        self._global = agg
        return agg

    def global_weights(self):
        return self._global


# ------------------------------------------------------------
# Enhancement 4: Multi-Agent Coordination
# ------------------------------------------------------------
class AgentRole(str, Enum):
    EXPLORER = "explorer"
    EXPLOITER = "exploiter"
    SAFETY_OFFICER = "safety_officer"
    CARBON_BROKER = "carbon_broker"
    VERIFIER = "verifier"


@dataclass
class AgentBid:
    agent_id: str
    role: AgentRole
    confidence: float
    proposed_action: int
    rationale: str
    carbon_score: float


class EmergentRoleRegistry:
    def __init__(self, decay: float = 0.95):
        self.decay = decay
        self._scores = {r: 1.0 for r in AgentRole}
        self._counts = {r: 0 for r in AgentRole}

    def record(self, role: AgentRole, success: bool, reward: float) -> None:
        for r in self._scores:
            self._scores[r] *= self.decay
        signal = reward if success else -abs(reward) * 0.5
        self._scores[role] += signal
        self._counts[role] += 1

    def weights(self) -> Dict[AgentRole, float]:
        total = sum(max(v, 1e-6) for v in self._scores.values())
        return {r: max(v, 1e-6) / total for r, v in self._scores.items()}

    def dominant_role(self) -> AgentRole:
        return max(self._scores, key=self._scores.get)


class MultiAgentCoordinator:
    def __init__(self, registry: EmergentRoleRegistry,
                 role_bias: Optional[Dict[AgentRole, float]] = None):
        self.registry = registry
        self.role_bias = role_bias or {
            AgentRole.EXPLORER: 0.6,
            AgentRole.EXPLOITER: 0.9,
            AgentRole.SAFETY_OFFICER: 1.2,
            AgentRole.CARBON_BROKER: 1.1,
            AgentRole.VERIFIER: 1.0,
        }

    def vote(self, bids: Sequence[AgentBid], n_candidates: int) -> int:
        if not bids or n_candidates <= 0:
            return 0
        role_w = self.registry.weights()
        scores = np.zeros(n_candidates)
        for b in bids:
            idx = b.proposed_action % n_candidates
            weight = (
                role_w.get(b.role, 0.1)
                * self.role_bias.get(b.role, 1.0)
                * max(b.confidence, 0.0)
                * (1.0 + b.carbon_score)
            )
            scores[idx] += weight
        return int(np.argmax(scores))


# ------------------------------------------------------------
# Enhancement 6: XAI
# ------------------------------------------------------------
@dataclass
class Explanation:
    decision_id: str
    chosen_idx: int
    top_features: List[Tuple[str, float]]
    counterfactuals: List[Dict[str, Any]]
    rationale: str
    confidence: float
    safety_ok: bool
    carbon_price_signal: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DecisionExplainer:
    def __init__(self, feature_names: Optional[Sequence[str]] = None):
        self.feature_names = list(feature_names) if feature_names else [
            "quality_score", "tokens_per_sec", "energy_joules",
            "carbon_g", "gpu_memory_used_mb",
        ]

    def _attributions(self, chosen: Dict[str, Any], pareto: Sequence[Dict[str, Any]]):
        if not pareto:
            return []
        arr = np.array([[float(c.get(f, 0.0)) for f in self.feature_names] for c in pareto])
        chosen_vec = np.array([float(chosen.get(f, 0.0)) for f in self.feature_names])
        mean = arr.mean(axis=0)
        std = arr.std(axis=0) + 1e-9
        z = (chosen_vec - mean) / std
        attrs = list(zip(self.feature_names, z.tolist()))
        attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
        return attrs

    def explain(
        self,
        chosen_idx: int,
        chosen: Dict[str, Any],
        pareto: Sequence[Dict[str, Any]],
        teacher_probs: Optional[np.ndarray],
        counterfactuals: Dict[int, float],
        safety_ok: bool,
        carbon_price: float,
        confidence: float = 0.5,
    ) -> Explanation:
        attrs = self._attributions(chosen, pareto)
        cf_list = []
        for idx, score in sorted(counterfactuals.items(), key=lambda kv: kv[1], reverse=True)[:3]:
            if 0 <= idx < len(pareto):
                cf_list.append({
                    "alternative_idx": idx,
                    "expected_reward": float(score),
                })
        top_feat = ", ".join(f"{n}={v:+.2f}" for n, v in attrs[:3])
        rationale = (
            f"Chose policy #{chosen_idx} (confidence={confidence:.2f}). "
            f"Top deviations: {top_feat}. Carbon price={carbon_price:.4f}. "
            f"Safety={'ok' if safety_ok else 'OVERRIDE'}."
        )
        return Explanation(
            decision_id=f"dec-{uuid.uuid4().hex[:8]}",
            chosen_idx=chosen_idx,
            top_features=attrs[:5],
            counterfactuals=cf_list,
            rationale=rationale,
            confidence=confidence,
            safety_ok=safety_ok,
            carbon_price_signal=carbon_price,
        )


# ------------------------------------------------------------
# Enhancement 1: Quantum-Distillation
# ------------------------------------------------------------
class QuantumInspiredTeacher:
    def __init__(self, n_qubits: int = 5, seed: int = 0):
        self.n_qubits = n_qubits
        self.rng = np.random.default_rng(seed)
        self._params = self.rng.normal(size=(n_qubits, 2)) * 0.5

    def _ry(self, state, theta, q):
        c, s = math.cos(theta / 2), math.sin(theta / 2)
        n = len(state)
        new = state.copy()
        step = 1 << q
        for i in range(n):
            if i & step == 0:
                a, b = state[i], state[i | step]
                new[i] = c * a - s * b
                new[i | step] = s * a + c * b
        return new

    def _simulate(self, features: np.ndarray, n_candidates: int) -> np.ndarray:
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128)
        state[0] = 1.0
        for q in range(self.n_qubits):
            angle = float(self._params[q, 0] * features[q % len(features)] + self._params[q, 1])
            state = self._ry(state, angle, q)
        probs_full = np.abs(state) ** 2
        probs = np.zeros(n_candidates, dtype=np.float64)
        for i, p in enumerate(probs_full):
            probs[i % n_candidates] += p
        return probs / max(probs.sum(), 1e-12)

    def teacher_probs(self, features: np.ndarray, n: int) -> np.ndarray:
        if n <= 0:
            return np.zeros(0)
        return self._simulate(np.asarray(features, dtype=np.float64), n)


class DistillationEnsemble:
    def __init__(self, quantum: QuantumInspiredTeacher, alpha: float = 0.5):
        self.quantum = quantum
        self.alpha = alpha

    def blend(self, features: np.ndarray, other: Optional[np.ndarray], n: int) -> np.ndarray:
        q = self.quantum.teacher_probs(features, n)
        if other is None or len(other) != n:
            return q
        return self.alpha * q + (1.0 - self.alpha) * np.asarray(other)


class MultiTeacherDistiller:
    """Local multi-teacher distiller with weighted soft vote."""

    def __init__(self, teachers: Sequence[Callable], weights: Optional[Sequence[float]] = None):
        self.teachers = list(teachers)
        self.weights = list(weights) if weights else [1.0] * len(self.teachers)

    def _safe(self, fn, ctx):
        try:
            out = fn(ctx)
            if asyncio_iscoroutine(out):
                out.close()
                return None
            return out
        except Exception:
            return None

    def distill(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        votes: Dict[str, Tuple[Dict, float]] = {}
        for t, w in zip(self.teachers, self.weights):
            choice = self._safe(t, context)
            if choice is None:
                continue
            key = json.dumps(choice, sort_keys=True)
            if key in votes:
                votes[key] = (votes[key][0], votes[key][1] + w)
            else:
                votes[key] = (choice, w)
        if not votes:
            return None
        return max(votes.values(), key=lambda kv: kv[1])[0]


def asyncio_iscoroutine(obj) -> bool:
    try:
        import asyncio
        return asyncio.iscoroutine(obj)
    except Exception:
        return False


# ------------------------------------------------------------
# Enhancement 10: HITL / Active Learning
# ------------------------------------------------------------
@dataclass
class HITLRequest:
    request_id: str
    reason: str
    chosen_idx: int
    candidates: List[Dict[str, Any]]
    uncertainty: float
    carbon_price: float


class UncertaintyEstimator:
    def __init__(self, entropy_threshold: float = 0.75, variance_threshold: float = 0.05):
        self.entropy_threshold = entropy_threshold
        self.variance_threshold = variance_threshold
        self._rewards: List[float] = []

    def observe(self, reward: float) -> None:
        self._rewards.append(float(reward))
        if len(self._rewards) > 32:
            self._rewards = self._rewards[-32:]

    def entropy(self, probs: Optional[np.ndarray]) -> float:
        if probs is None or len(probs) == 0:
            return 1.0
        p = np.clip(np.asarray(probs, dtype=np.float64), 1e-12, 1.0)
        return float(-np.sum(p * np.log(p)) / math.log(len(p)))

    def variance(self) -> float:
        if len(self._rewards) < 3:
            return 0.0
        return float(np.var(np.asarray(self._rewards)))

    def is_uncertain(self, probs: Optional[np.ndarray]) -> Tuple[bool, float]:
        h = self.entropy(probs)
        v = self.variance()
        score = 0.6 * h + 0.4 * min(1.0, v / max(self.variance_threshold, 1e-9))
        return (h > self.entropy_threshold or v > self.variance_threshold), score


class HumanInTheLoopGate:
    def __init__(self, approver: Optional[Callable[[HITLRequest], bool]] = None,
                 timeout_s: float = 2.0, auto_approve_on_timeout: bool = True):
        self.approver = approver
        self.timeout_s = timeout_s
        self.auto_approve_on_timeout = auto_approve_on_timeout
        self.audit: List[Dict[str, Any]] = []

    async def request(self, req: HITLRequest) -> bool:
        if self.approver is None:
            self.audit.append({"request_id": req.request_id, "approved": True,
                               "reason": "no_approver"})
            return True
        import asyncio
        loop = asyncio.get_running_loop()
        try:
            approved = await asyncio.wait_for(
                loop.run_in_executor(None, self.approver, req),
                timeout=self.timeout_s,
            )
        except asyncio.TimeoutError:
            approved = self.auto_approve_on_timeout
        self.audit.append({"request_id": req.request_id, "approved": bool(approved),
                           "reason": req.reason})
        return bool(approved)


class ActiveLearningSampler:
    def __init__(self, capacity: int = 256):
        self.capacity = capacity
        self._buffer: List[Dict[str, Any]] = []

    def maybe_store(self, record: Dict[str, Any], uncertainty: float,
                    threshold: float = 0.5) -> bool:
        if uncertainty < threshold:
            return False
        if len(self._buffer) >= self.capacity:
            self._buffer.pop(0)
        self._buffer.append({**record, "uncertainty": uncertainty, "t": time.time()})
        return True

    def sample(self, k: int = 8) -> List[Dict[str, Any]]:
        if not self._buffer:
            return []
        k = min(k, len(self._buffer))
        return random.sample(self._buffer, k)

    def __len__(self):
        return len(self._buffer)


# ============================================================
# ============================================================
# GreenAgentPolicyRouter
# ============================================================
# ============================================================


class GreenAgentPolicyRouter:
    """
    Enhanced router with all ten Green Agent enhancements hosted in-file.
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
        enable_limit_graph: bool = True,
        enable_rlhf: bool = True,
        enable_distillation: bool = True,
        # Ten enhancement flags
        enable_quantum_teacher: bool = True,
        enable_causal: bool = True,
        enable_federated: bool = True,
        enable_multi_agent: bool = True,
        enable_temporal_logic: bool = True,
        enable_xai: bool = True,
        enable_precision_switching: bool = True,
        enable_carbon_market: bool = True,
        enable_chaos: bool = False,
        enable_hitl: bool = True,
        # Tunables
        carbon_threshold: float = 400.0,
        chaos_fault_prob: float = 0.0,
        chaos_latency_ms: float = 0.0,
        chaos_carbon_spike_prob: float = 0.0,
        rec_default_kwh: float = 0.0,
        hitl_approver: Optional[Callable[[HITLRequest], bool]] = None,
        hitl_timeout_s: float = 2.0,
    ):
        self.logger = logging.getLogger(__name__)
        self.lp_solver = lp_solver
        self.executor = executor
        self.min_trials = min_trials_before_bandit
        self.conf_threshold = confidence_threshold
        self.persistence_file = persistence_file
        self._save_lock = threading.Lock()
        self._save_counter = 0

        # ---- Core modules ----
        self.carbon_scheduler = CarbonDelayScheduler(
            carbon_api, threshold=carbon_threshold
        )
        self.cache = PolicyMetaCache()
        self.bandit = ContextualBandit(
            action_space=action_space,
            fallback_solver=lp_solver,
            modp_weights=modp_weights,
            moe_router=moe_router,
            bio_generator=bio_generator,
        )
        self.modp_weights = modp_weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy_efficiency": 0.20,
            "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10,
        }
        self.reward_calc = RewardCalculator(weights=self.modp_weights)
        self.moe = moe_router if moe_router else _NoMoERouter()
        self.bio = bio_generator if bio_generator else _NoBioGenerator()

        # ---- Profiler ----
        self.profiler = GPUProfiler() if enable_profiler else None
        if self.profiler:
            self.profiler.start()
            self.metric_aggregator = MetricAggregator(self.profiler, self.executor)
        else:
            self.metric_aggregator = None

        # ---- LIMIT Graph (simple constraint holder) ----
        self.limit_graph = _SimpleLimitGraph() if enable_limit_graph else None

        # ---- RLHF ----
        self.rlhf = _SimpleRLHF(
            action_space=[json.dumps(p, sort_keys=True) for p in action_space]
        ) if enable_rlhf else None

        # ---- Enhancement 1: Quantum-Distillation ----
        self.quantum_teacher = (
            QuantumInspiredTeacher(seed=42) if enable_quantum_teacher else None
        )
        self.distill_ensemble = (
            DistillationEnsemble(self.quantum_teacher, alpha=0.5)
            if self.quantum_teacher else None
        )

        # ---- Enhancement 2: Causal ----
        self.causal = (
            CausalCounterfactualEstimator(state_dim=8, n_actions=max(4, len(action_space)))
            if enable_causal else None
        )
        self.last_counterfactuals: Dict[int, float] = {}

        # ---- Enhancement 3: Federated ----
        self.federated = FederatedAggregator() if enable_federated else None

        # ---- Enhancement 4: Multi-Agent ----
        self.roles = EmergentRoleRegistry() if enable_multi_agent else None
        self.coordinator = MultiAgentCoordinator(self.roles) if self.roles else None

        # ---- Enhancement 5: Temporal Logic ----
        self.temporal = None
        self.shield = None
        if enable_temporal_logic:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="no_oversized_batch",
                predicate=lambda r: int(r.get("gpu_batch_size", 1)) <= 64,
                operator=STLOperator.ALWAYS,
                horizon=10,
            ))
            self.shield = SafetyShield(self.temporal)

        # ---- Enhancement 6: XAI ----
        self.explainer = DecisionExplainer() if enable_xai else None
        self.last_explanation: Optional[Explanation] = None

        # ---- Enhancement 7: Precision ----
        self.precision_controller = (
            PrecisionController(quality_floor=0.95)
            if enable_precision_switching else None
        )
        self.precision_adapter = (
            HardwareAwareAdapter(self.precision_controller)
            if self.precision_controller else None
        )
        self.current_precision: Optional[PrecisionLevel] = None

        # ---- Enhancement 8: Carbon market / RECs ----
        self.carbon_market = (
            CarbonMarketClient() if enable_carbon_market else None
        )
        self.recs = RECInventory() if enable_carbon_market else None
        if self.recs and rec_default_kwh > 0:
            self.recs.add(rec_default_kwh)

        # ---- Enhancement 9: Chaos ----
        self.chaos = (
            ChaosEngineer(ChaosConfig(
                fault_prob=chaos_fault_prob,
                latency_inject_ms=chaos_latency_ms,
                carbon_spike_prob=chaos_carbon_spike_prob,
            )) if enable_chaos else None
        )
        self.breaker = CircuitBreaker("executor") if enable_chaos else None

        # ---- Enhancement 10: HITL ----
        self.uncertainty = UncertaintyEstimator() if enable_hitl else None
        self.hitl = (
            HumanInTheLoopGate(approver=hitl_approver, timeout_s=hitl_timeout_s)
            if enable_hitl else None
        )
        self.active_learner = ActiveLearningSampler() if enable_hitl else None

        # ---- Distiller (with the ten-enhancement quantum teacher) ----
        self.distiller = None
        if enable_distillation:
            self.distiller = MultiTeacherDistiller(
                teachers=[
                    self._teacher_cache,
                    self._teacher_bandit,
                    self._teacher_lp,
                    self._teacher_precision,
                ],
                weights=[1.5, 1.0, 1.0, 0.5],
            )

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
            "distillation_decisions": 0,
            "rlhf_updates": 0,
            "limit_graph_enforced": 0,
            "precision_switches": 0,
            "causal_updates": 0,
            "federated_submits": 0,
            "role_updates": 0,
            "safety_violations": 0,
            "hitl_requests": 0,
            "hitl_approved": 0,
            "chaos_faults": 0,
            "chaos_latency_ms": 0.0,
            "carbon_rec_offset_kg": 0.0,
        }

        # ---- Load persistent state ----
        self._load_state()
        atexit.register(self._atexit_save)

    # ------------------------------------------------------------------
    # Context manager / cleanup
    # ------------------------------------------------------------------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.shutdown()

    def _atexit_save(self):
        try:
            self._save_state()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Persistence (atomic)
    # ------------------------------------------------------------------
    def _load_state(self) -> None:
        if not os.path.exists(self.persistence_file):
            return
        try:
            with open(self.persistence_file, "r") as f:
                data = json.load(f)
            if "cache" in data:
                self.cache.store = {
                    tuple(k): (dict(v[0]), float(v[1]), int(v[2]))
                    for k, v in data["cache"]
                }
            if "bandit" in data:
                self.bandit.import_state(data["bandit"])
            if "carbon_queue" in data:
                for item in data["carbon_queue"]:
                    self.carbon_scheduler.queue.append(
                        _DelayedTask(
                            time=float(item["time"]),
                            task=dict(item["task"]),
                            seq=next(self.carbon_scheduler._seq),
                        )
                    )
                heapq.heapify(self.carbon_scheduler.queue)
            if "stats" in data:
                for k, v in data["stats"].items():
                    if k in self.stats:
                        self.stats[k] = v
            self.logger.info("Loaded state from %s", self.persistence_file)
        except Exception as e:
            self.logger.warning("Failed to load state: %s", e)

    def _save_state(self) -> None:
        with self._save_lock:
            try:
                data = {
                    "cache": [
                        (list(k), (dict(v[0]), float(v[1]), int(v[2])))
                        for k, v in self.cache.store.items()
                    ],
                    "bandit": self.bandit.export_state(),
                    "carbon_queue": [
                        {"time": t, "task": task}
                        for (t, _, task) in self.carbon_scheduler.queue
                    ],
                    "stats": self.stats,
                }
                d = os.path.dirname(os.path.abspath(self.persistence_file)) or "."
                fd, tmp = tempfile.mkstemp(dir=d, prefix=".router_", suffix=".json")
                try:
                    with os.fdopen(fd, "w") as f:
                        json.dump(data, f, default=str)
                    os.replace(tmp, self.persistence_file)
                except Exception:
                    try:
                        os.unlink(tmp)
                    except OSError:
                        pass
                    raise
            except Exception as e:
                self.logger.warning("Failed to save state: %s", e)

    def _maybe_save(self) -> None:
        self._save_counter += 1
        if self._save_counter % 10 == 0:
            self._save_state()

    # ------------------------------------------------------------------
    # Fingerprint
    # ------------------------------------------------------------------
    def _generate_fingerprint(self, task: Dict[str, Any]) -> Tuple[WorkloadFingerprint, Dict[str, Any]]:
        """Return (fingerprint, extra_context). Does NOT mutate the task."""
        fp = WorkloadFingerprint(
            model_size_mb=float(task.get("model_size_mb", 0)),
            prompt_len=int(task.get("prompt_len", 0)),
            gen_len=int(task.get("gen_len", 0)),
            gpu_mem_free_mb=float(task.get("gpu_mem_free_mb", 0)),
            disk_speed_class=int(task.get("disk_speed_class", 1)),
        )
        extra: Dict[str, Any] = {}
        if self.profiler:
            try:
                hw_state = self.profiler.get_current_metrics()
            except Exception:
                hw_state = {}
            extra["hardware_state"] = hw_state
            extra["moe_context"] = self._safe_moe_encode(task, hw_state)
        return fp, extra

    def _safe_moe_encode(self, task: Dict[str, Any], hw_state: Dict[str, Any]) -> Any:
        """Call moe.encode with 1 or 2 args, whichever it accepts."""
        try:
            return self.moe.encode(task, hw_state)
        except TypeError:
            try:
                return self.moe.encode(task)
            except Exception:
                return None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Teachers
    # ------------------------------------------------------------------
    def _teacher_cache(self, context: Dict[str, Any]):
        fp = context.get("fingerprint")
        if isinstance(fp, WorkloadFingerprint):
            return self.cache.get_best_policy(fp)
        return None

    def _teacher_bandit(self, context: Dict[str, Any]):
        policy, conf = self.bandit.select_action(
            context,
            min_trials_before_bandit=self.min_trials,
            confidence_threshold=self.conf_threshold,
        )
        if policy is not None and conf >= self.conf_threshold:
            return policy
        return None

    def _teacher_lp(self, context: Dict[str, Any]):
        fp = context.get("fingerprint")
        if isinstance(fp, WorkloadFingerprint):
            try:
                return self.lp_solver(fp)
            except Exception:
                return None
        return None

    def _teacher_precision(self, context: Dict[str, Any]):
        """Precision teacher: proposes a policy tuned to current carbon stress."""
        if not self.precision_adapter:
            return None
        carbon = self._current_carbon()
        price = self._current_carbon_price()
        headroom = 0.5
        policy, _ = self.precision_adapter.adapt({}, carbon, headroom, price)
        return policy

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _current_carbon(self) -> float:
        try:
            carbon = float(self.carbon_scheduler.carbon_api.get_current())
        except Exception:
            carbon = 400.0
        if self.chaos:
            carbon = self.chaos.maybe_carbon_spike(carbon)
        return carbon

    def _current_carbon_price(self) -> float:
        if not self.carbon_market:
            return 0.0
        return self.carbon_market.price(self._current_carbon(), time.localtime().tm_hour)

    def _build_state_vec(self, context: Dict[str, Any]) -> np.ndarray:
        fp = context.get("fingerprint")
        v = list(fp.to_tuple()) if isinstance(fp, WorkloadFingerprint) else [0.0] * 5
        hw = context.get("hardware_state") or {}
        v.append(float(hw.get("gpu_utilization_pct", 0.0)))
        v.append(float(hw.get("gpu_power_watts", 0.0)) / 400.0)
        v.append(float(self._current_carbon()) / 1000.0)
        arr = np.asarray(v, dtype=np.float64)
        if arr.size < 8:
            arr = np.pad(arr, (0, 8 - arr.size))
        return arr[:8]

    # ------------------------------------------------------------------
    # Multi-agent bid generation
    # ------------------------------------------------------------------
    def _generate_bids(self, context: Dict[str, Any], candidates: Sequence[Dict[str, Any]]) -> List[AgentBid]:
        if not self.coordinator or not self.roles or not candidates:
            return []
        carbon = self._current_carbon()
        bids: List[AgentBid] = []
        for role in AgentRole:
            if role == AgentRole.CARBON_BROKER:
                idx = min(range(len(candidates)), key=lambda i: int(candidates[i].get("gpu_batch_size", 1)))
            elif role == AgentRole.EXPLOITER:
                idx = max(range(len(candidates)), key=lambda i: int(candidates[i].get("gpu_batch_size", 1)))
            elif role == AgentRole.EXPLORER:
                idx = random.randrange(len(candidates))
            elif role == AgentRole.SAFETY_OFFICER:
                idx = min(range(len(candidates)), key=lambda i: int(candidates[i].get("gpu_batch_size", 1)))
            else:
                idx = 0
            bids.append(AgentBid(
                agent_id=role.value,
                role=role,
                confidence=0.7,
                proposed_action=idx,
                rationale=role.value,
                carbon_score=1.0 / (1.0 + carbon / 1000.0),
            ))
        return bids

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------
    def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        self.stats["total_tasks"] += 1

        # Chaos fault injection at entry
        if self.chaos and self.chaos.maybe_fault():
            self.stats["chaos_faults"] += 1
            return {
                "status": "failed",
                "reason": "chaos_fault",
                "policy": None,
                "policy_source": None,
                "metrics": {},
                "reward": 0.0,
            }

        # Step 1: carbon delay
        try:
            delay_result = self.carbon_scheduler.submit(task)
        except Exception as e:
            self.logger.warning("Carbon scheduler failed: %s", e)
            delay_result = {"status": "immediate"}

        if delay_result.get("status") == "delayed":
            self.stats["delayed"] += 1
            return {
                "status": "deferred",
                "reason": delay_result.get("reason", "high_carbon"),
                "delay_until": delay_result.get("delay_until"),
            }

        self.stats["forwarded"] += 1

        # Step 2: fingerprint
        fp, extra = self._generate_fingerprint(task)
        context: Dict[str, Any] = {"task": task, "fingerprint": fp, **extra}

        # Step 3: select policy
        final_policy: Optional[Dict[str, Any]] = None
        policy_source: Optional[str] = None
        bandit_confidence = 0.0
        distill_confidence = 0.0

        cached = self._teacher_cache(context)
        if cached is not None:
            final_policy = cached
            policy_source = "cache"
            self.stats["cache_hits"] += 1
        else:
            if self.distiller is not None:
                distilled = self.distiller.distill(context)
                if distilled is not None:
                    final_policy = distilled
                    policy_source = "distillation"
                    distill_confidence = 0.7
                    self.stats["distillation_decisions"] += 1

            if final_policy is None:
                bandit_policy, bandit_confidence = self.bandit.select_action(
                    context,
                    min_trials_before_bandit=self.min_trials,
                    confidence_threshold=self.conf_threshold,
                )
                if bandit_policy is not None and bandit_confidence >= self.conf_threshold:
                    final_policy = bandit_policy
                    policy_source = "bandit"
                    self.stats["bandit_decisions"] += 1
                else:
                    try:
                        final_policy = self.lp_solver(fp)
                    except Exception as e:
                        self.logger.warning("LP solver failed: %s", e)
                        final_policy = dict(self.bandit.action_space[0]) if self.bandit.action_space else {}
                    policy_source = "lp_solver"
                    self.stats["lp_fallbacks"] += 1
                    self.bandit.seed_action(context, final_policy, reward=1.0)

        # Step 4: multi-agent vote
        if self.coordinator is not None and self.bandit.action_space:
            bids = self._generate_bids(context, self.bandit.action_space)
            voted = self.coordinator.vote(bids, len(self.bandit.action_space))
            voted_policy = dict(self.bandit.action_space[voted])
            # Only override if we're not using a high-confidence cached policy.
            if policy_source != "cache":
                final_policy = voted_policy
                policy_source = (policy_source or "vote") + "+vote"

        # Step 5: temporal safety shield
        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.shield and self.temporal:
            self.temporal.observe({
                "gpu_batch_size": int(final_policy.get("gpu_batch_size", 1)),
            })
            final_policy, safety_ok, stl_verdict = self.shield.screen(
                final_policy, self.bandit.action_space
            )
            if not safety_ok:
                self.stats["safety_violations"] += 1

        # Step 6: precision switching
        precision_level = None
        if self.precision_adapter:
            carbon = self._current_carbon()
            price = self._current_carbon_price()
            final_policy, precision_level = self.precision_adapter.adapt(
                final_policy, carbon, 0.5, price
            )
            self.current_precision = precision_level
            self.stats["precision_switches"] += 1

        # Step 7: LIMIT Graph constraints
        if self.limit_graph is not None:
            try:
                limits = self.limit_graph.get_limits(context)
                if "max_gpu_batch_size" in limits:
                    max_batch = int(limits["max_gpu_batch_size"])
                    if int(final_policy.get("gpu_batch_size", 0)) > max_batch:
                        final_policy = dict(final_policy)
                        final_policy["gpu_batch_size"] = max_batch
                        self.stats["limit_graph_enforced"] += 1
            except Exception as e:
                self.logger.warning("LIMIT Graph failed: %s", e)

        # Step 8: HITL for low-confidence or safety violations
        human_approved = True
        uncertainty_score = 0.0
        if self.hitl and self.uncertainty:
            probs = np.ones(len(self.bandit.action_space)) / max(1, len(self.bandit.action_space))
            is_unc, uncertainty_score = self.uncertainty.is_uncertain(probs)
            if is_unc or not safety_ok:
                self.stats["hitl_requests"] += 1
                req = HITLRequest(
                    request_id=uuid.uuid4().hex[:8],
                    reason="high_uncertainty" if is_unc else "safety_violation",
                    chosen_idx=0,
                    candidates=list(self.bandit.action_space),
                    uncertainty=uncertainty_score,
                    carbon_price=self._current_carbon_price(),
                )
                human_approved = self._await_hitl(req)
                if human_approved:
                    self.stats["hitl_approved"] += 1
                else:
                    # Fall back to the smallest batch candidate.
                    final_policy = min(
                        self.bandit.action_space,
                        key=lambda p: int(p.get("gpu_batch_size", 1)),
                    )

        # Step 9: chaos latency around execution
        if self.chaos:
            delay_ms = self.chaos.maybe_latency()
            if delay_ms > 0:
                self.stats["chaos_latency_ms"] += delay_ms
                time.sleep(delay_ms / 1000.0)

        # Step 10: execute
        metrics: Dict[str, Any] = {}
        try:
            if self.metric_aggregator:
                metrics = self.metric_aggregator.run(task, final_policy)
            else:
                metrics = self.executor(task, final_policy)
                if not isinstance(metrics, dict):
                    metrics = {"raw": metrics}
        except Exception as e:
            self.logger.warning("Executor failed: %s", e)
            metrics = {"error": str(e), "quality_score": 0.0}
            if self.breaker:
                self.breaker.record(False)
        else:
            if self.breaker:
                self.breaker.record(True)

        # Step 11: reward with carbon market
        constraints = task.get("constraints", {}) or {}
        carbon_intensity = self._current_carbon()
        reward = self.reward_calc.compute(metrics, constraints, carbon_intensity)

        if self.carbon_market:
            price = self._current_carbon_price()
            reward -= price * float(metrics.get("carbon_g", 0.0)) / 1000.0
        reward = max(0.0, min(1.0, reward))

        self.stats["total_reward"] += reward

        # REC offset
        if self.recs:
            energy_j = float(metrics.get("energy_joules", 0.0))
            if energy_j > 0 and self.recs.total_kwh() > 0:
                kwh = energy_j / 3.6e6
                offset = self.recs.consume(min(kwh, self.recs.total_kwh()))
                self.stats["carbon_rec_offset_kg"] += offset
                if "carbon_g" in metrics:
                    metrics["carbon_g"] = max(0.0, float(metrics["carbon_g"]) - offset * 1000.0)

        # Step 12: learn
        if policy_source and policy_source in ("bandit", "distillation", "lp_solver", "vote"):
            self.bandit.update(context, final_policy, reward)
        self.cache.update(fp, final_policy, reward)

        if self.rlhf is not None:
            policy_key = json.dumps(final_policy, sort_keys=True)
            try:
                self.rlhf.update(context, policy_key, reward)
                self.stats["rlhf_updates"] += 1
            except Exception:
                pass

        if self.limit_graph is not None:
            try:
                self.limit_graph.update_from_feedback({
                    "context": context,
                    "policy": final_policy,
                    "reward": reward,
                })
            except Exception:
                pass

        # Causal update
        if self.causal is not None:
            state_vec = self._build_state_vec(context)
            action_idx = 0
            try:
                canonical = json.dumps(final_policy, sort_keys=True)
                for i, p in enumerate(self.bandit.action_space):
                    if json.dumps(p, sort_keys=True) == canonical:
                        action_idx = i
                        break
            except Exception:
                pass
            self.causal.update(CausalTransition(
                state=state_vec, action=action_idx, reward=reward,
                next_state=state_vec,
            ))
            self.last_counterfactuals = self.causal.counterfactuals(
                state_vec, len(self.bandit.action_space)
            )
            self.stats["causal_updates"] += 1

        # Federated submit
        if self.federated is not None:
            vec = np.array([reward, float(metrics.get("quality_score", 0.0)),
                            float(metrics.get("energy_joules", 0.0)) / 100.0],
                           dtype=np.float64)
            self.federated.submit(FederatedUpdate(
                node_id="router",
                weights={"local": vec},
                n_samples=1,
                carbon_intensity=carbon_intensity,
            ))
            self.federated.aggregate()
            self.stats["federated_submits"] += 1

        # Role registry update
        if self.roles is not None:
            self.roles.record(
                AgentRole.CARBON_BROKER if "precision" in final_policy else AgentRole.EXPLOITER,
                success=reward > 0.5,
                reward=reward,
            )
            self.stats["role_updates"] += 1

        # Uncertainty + active learning
        if self.uncertainty:
            self.uncertainty.observe(reward)
        if self.active_learner:
            self.active_learner.maybe_store(
                {"policy": final_policy, "reward": reward},
                uncertainty=uncertainty_score,
                threshold=0.4,
            )

        # XAI
        if self.explainer:
            explainer_input = dict(metrics)
            explainer_input.setdefault("quality_score", 0.9)
            explanation = self.explainer.explain(
                chosen_idx=0,
                chosen=explainer_input,
                pareto=[dict(metrics) for _ in self.bandit.action_space] or [explainer_input],
                teacher_probs=None,
                counterfactuals=self.last_counterfactuals,
                safety_ok=safety_ok and human_approved,
                carbon_price=self._current_carbon_price(),
                confidence=max(bandit_confidence, distill_confidence, 0.5),
            )
            self.last_explanation = explanation

        # Feed back to carbon scheduler
        self.carbon_scheduler.report_reward(task, reward)

        # Bio expansion (guarded)
        self._maybe_bio_expand(context, policy_source)

        # Periodic save
        self._maybe_save()

        return {
            "status": "completed",
            "policy": final_policy,
            "policy_source": policy_source,
            "metrics": metrics,
            "reward": reward,
            "precision_level": precision_level.value if precision_level else None,
            "safety_ok": safety_ok,
            "stl_verdict": stl_verdict,
            "human_approved": human_approved,
            "uncertainty": uncertainty_score,
            "counterfactuals": self.last_counterfactuals,
            "explanation": self.last_explanation.to_dict() if self.last_explanation else None,
        }

    def _await_hitl(self, req: HITLRequest) -> bool:
        """Run the HITL gate synchronously (best-effort)."""
        try:
            import asyncio
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop and loop.is_running():
                # We're inside a running loop; fire-and-forget would be unsafe.
                # Default to auto-approval.
                return True
            return asyncio.run(self.hitl.request(req))
        except Exception:
            return True

    def _maybe_bio_expand(self, context: Dict[str, Any], policy_source: Optional[str]) -> None:
        if not self.bio or policy_source != "lp_solver":
            return
        try:
            n_trials = self.bandit.get_trials(context)
        except Exception:
            return
        if n_trials <= 10:
            return
        try:
            new_policies = self.bio.generate_policies(self.bandit.action_space, n=2)
        except Exception:
            return
        if not new_policies:
            return
        added = 0
        for p in new_policies:
            try:
                if not isinstance(p, dict):
                    continue
                self.bandit.add_action(p)
                added += 1
            except Exception:
                continue
        if added:
            self.stats["bio_expansions"] += added
            self.logger.info("Bio-inspired expansion added %d policies", added)

    # ------------------------------------------------------------------
    # Public utility
    # ------------------------------------------------------------------
    def tick_carbon_queue(self) -> List[Dict[str, Any]]:
        return self.carbon_scheduler.tick()

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self.stats,
            "cache_size": len(self.cache.store),
            "bandit_action_space_size": len(self.bandit.action_space),
            "carbon_queue_size": len(self.carbon_scheduler.queue),
            "bandit_num_contexts": len(self.bandit._weights),
            "distillation_active": self.distiller is not None,
            "rlhf_active": self.rlhf is not None,
            "limit_graph_active": self.limit_graph is not None,
            "quantum_teacher_active": self.quantum_teacher is not None,
            "causal_active": self.causal is not None,
            "federated_active": self.federated is not None,
            "multi_agent_active": self.coordinator is not None,
            "temporal_logic_active": self.temporal is not None,
            "xai_active": self.explainer is not None,
            "precision_switching_active": self.precision_adapter is not None,
            "carbon_market_active": self.carbon_market is not None,
            "chaos_active": self.chaos is not None,
            "hitl_active": self.hitl is not None,
            "current_precision": self.current_precision.value if self.current_precision else None,
            "rec_kwh_available": self.recs.total_kwh() if self.recs else 0.0,
            "last_explanation": self.last_explanation.to_dict() if self.last_explanation else None,
            "last_counterfactuals": {int(k): float(v) for k, v in self.last_counterfactuals.items()},
        }

    def health_check(self) -> Dict[str, Any]:
        components: Dict[str, Dict[str, Any]] = {
            "cache": {"status": "ok"},
            "bandit": {"status": "ok", "actions": len(self.bandit.action_space)},
            "carbon_scheduler": {"status": "ok",
                                 "queue_size": len(self.carbon_scheduler.queue)},
        }
        if self.profiler and hasattr(self.profiler, "health_check"):
            try:
                components["profiler"] = self.profiler.health_check()
            except Exception as e:
                components["profiler"] = {"status": "unhealthy", "error": str(e)}
        checkable = [c for c in components.values() if c.get("status") != "unavailable"]
        if not checkable:
            overall, score = "degraded", 0
        else:
            ok = all(c.get("status") in ("ok", "healthy") for c in checkable)
            overall, score = ("healthy", 100) if ok else ("degraded", 50)
        return {"status": overall, "score": score, "components": components}

    def shutdown(self) -> None:
        if self.profiler:
            try:
                self.profiler.stop()
            except Exception:
                pass
        try:
            self._save_state()
        except Exception:
            pass
        self.logger.info("Router shutdown complete.")


# ============================================================
# Minimal fallback helpers
# ============================================================
class _NoMoERouter:
    def encode(self, task, hw_state=None):
        return [0.0] * 5


class _NoBioGenerator:
    def generate_policies(self, current_policies, n=2):
        return []


class _SimpleLimitGraph:
    def __init__(self):
        self.limits: Dict[str, Any] = {}
        self._feedback: List[Dict[str, Any]] = []

    def build_graph(self, nodes, edges):
        pass

    def get_limits(self, context):
        # Default constraint: cap batch size
        return {"max_gpu_batch_size": self.limits.get("max_gpu_batch_size", 8)}

    def update_from_feedback(self, feedback):
        self._feedback.append(feedback)
        if len(self._feedback) > 1000:
            self._feedback = self._feedback[-1000:]


class _SimpleRLHF:
    def __init__(self, action_space: Sequence[str]):
        self.actions = list(action_space)
        self.scores = {a: 0.0 for a in self.actions}

    def update(self, context, action, reward):
        if action not in self.scores:
            self.actions.append(action)
            self.scores[action] = 0.0
        self.scores[action] += 0.1 * (float(reward) - self.scores[action])

    def sample_action(self, context):
        if not self.actions:
            return None
        return max(self.scores, key=self.scores.get)


# ============================================================
# Example usage
# ============================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _CarbonAPIStub:
        def get_current(self):
            return 250.0 + random.random() * 300.0

        def get_forecast(self):
            return [500.0, 450.0, 300.0, 220.0, 200.0]

    def lp_solver(fp):
        return {"gpu_batch_size": 1, "block_size": 8, "weight_device": "gpu"}

    def executor(task, policy):
        time.sleep(0.01)
        return {
            "tokens_generated": 20,
            "tokens_per_sec": 90.0,
            "quality_score": 0.95,
            "energy_joules": 5.0,
            "gpu_memory_used_mb": 4000.0,
            "gpu_memory_total_mb": 16000.0,
            "latency_ms": 100.0,
        }

    action_space = [
        {"gpu_batch_size": 1, "block_size": 8, "weight_device": "gpu"},
        {"gpu_batch_size": 2, "block_size": 16, "weight_device": "cpu"},
        {"gpu_batch_size": 4, "block_size": 32, "weight_device": "disk"},
    ]

    with GreenAgentPolicyRouter(
        carbon_api=_CarbonAPIStub(),
        action_space=action_space,
        lp_solver=lp_solver,
        executor=executor,
        persistence_file="router_test.json",
        enable_chaos=False,
    ) as router:
        for _ in range(5):
            task = {
                "model_size_mb": 35000,
                "prompt_len": 512,
                "gen_len": 32,
                "gpu_mem_free_mb": 12000,
                "disk_speed_class": 2,
            }
            result = router.handle_task(task)
            print(json.dumps({
                "status": result.get("status"),
                "policy_source": result.get("policy_source"),
                "reward": result.get("reward"),
                "precision": result.get("precision_level"),
                "safety_ok": result.get("safety_ok"),
                "human_approved": result.get("human_approved"),
            }, default=str))

        print(json.dumps(router.get_stats(), indent=2, default=str))
        print(json.dumps(router.health_check(), indent=2, default=str))
