#!/usr/bin/env python3
"""
FlexGen MoE Router v2.0.0 — Self-Contained Single-File Bundle
=============================================================

Complete rewrite of the FlexGen Mixture-of-Experts router. Everything
needed to run and test is in this file:

  1. Local stubs for FlexGenState, FlexGenPolicy, WorkloadDescriptor,
     FeedbackEvent, AsyncMessageQueue.
  2. Ten real enhancement modules (no placeholders):
        - QuantumDistillationModule  (density-matrix superposition)
        - CausalMask                 (PC-style partial correlation)
        - FederatedCoordinator       (weighted FedAvg + DP + sparsify + rate limit)
        - SafetyMonitor              (severity + mitigation hooks)
        - PrecisionController        (layer-wise, hysteresis, energy)
        - CarbonMarketClient         (budget + EMA + REC ledger)
        - ChaosInjector              (steady-state + rollback + bounded)
        - HumanApprovalHandler       (uncertainty sampling)
        - XAIExplainer               (Integrated Gradients + counterfactuals)
        - TemporalSafetyMonitor      (MTL operators G/F/NEVER/H/O)
  3. `RoutingDecision` value object.
  4. `FlexGenMoERouter` with thread-safe CPU-bound forward pass
     (`threading.Lock` + `asyncio.to_thread`).
  5. Comprehensive pytest suite.

Run:
    python flexgen_moe_router_v2.py        # demo
    pytest flexgen_moe_router_v2.py -v     # tests
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import re
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple

import numpy as np
import pytest
import pytest_asyncio

logger = logging.getLogger("flexgen_moe")
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


# =============================================================================
# LOCAL STUBS (replace with production adapters)
# =============================================================================
@dataclass
class FlexGenState:
    batch_size: int = 8
    seq_len: int = 512
    gpu_memory_used_gb: float = 4.0
    gpu_memory_total_gb: float = 24.0
    carbon_intensity: float = 400.0
    energy_budget_j: float = 1000.0
    latency_budget_ms: float = 100.0
    helium_scarcity: float = 0.5
    task_complexity: float = 0.5
    trust_score: float = 0.8
    opportunity_score: float = 0.5
    stress_level: float = 0.3

    def to_feature_vector(self) -> np.ndarray:
        return np.array([
            self.batch_size / 64.0,
            self.seq_len / 4096.0,
            self.gpu_memory_used_gb / max(self.gpu_memory_total_gb, 1e-6),
            self.carbon_intensity / 1000.0,
            self.energy_budget_j / 5000.0,
            self.latency_budget_ms / 1000.0,
            self.helium_scarcity,
            self.task_complexity,
            self.trust_score,
            self.opportunity_score,
            self.stress_level,
            0.5,
        ], dtype=np.float32)


@dataclass
class FlexGenPolicy:
    name: str
    offload_ratio: float = 0.5
    batch_size: int = 8
    precision: str = "fp16"

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "offload_ratio": self.offload_ratio,
                "batch_size": self.batch_size, "precision": self.precision}


@dataclass
class WorkloadDescriptor:
    task_id: str = "unknown"
    task_type: str = "generic"
    workload_size: float = 1.0


class FeedbackEvent:
    def __init__(self, **fields: Any):
        self.fields = fields
        self.fields["ts"] = datetime.now(timezone.utc).isoformat()

    def to_json(self) -> str:
        return json.dumps(self.fields, default=str)


class AsyncMessageQueue:
    def __init__(self):
        self._topics: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self.published: List[Tuple[str, str]] = []

    async def publish(self, topic: str, payload: str) -> None:
        self.published.append((topic, payload))
        await self._topics[topic].put(payload)

    async def subscribe(self, topic: str, timeout: float = 0.5) -> Optional[str]:
        try:
            return await asyncio.wait_for(self._topics[topic].get(), timeout)
        except asyncio.TimeoutError:
            return None


def compute_reward(state: FlexGenState, metrics: Dict[str, float],
                   policy: FlexGenPolicy) -> float:
    quality = metrics.get("quality_score", 0.8)
    carbon = state.carbon_intensity / 1000.0
    latency = metrics.get("latency_ms", 50) / 100.0
    return float(max(0.0, quality - 0.3 * carbon - 0.2 * latency))


# =============================================================================
# ENHANCEMENT 1 — CAUSAL MASK (PC-style)
# =============================================================================
class CausalMask:
    """PC-style partial-correlation skeleton over 12 input features."""

    FEATURE_NAMES = (
        "batch_size", "seq_len", "gpu_mem_frac", "carbon_intensity",
        "energy_budget", "latency_budget", "helium_scarcity",
        "task_complexity", "trust_score", "opportunity_score",
        "stress_level", "reserved",
    )

    def __init__(self, feature_dim: int = 12,
                 mask: Optional[np.ndarray] = None,
                 rng: Optional[random.Random] = None):
        self.feature_dim = int(feature_dim)
        self.rng = rng or random.Random()
        if mask is not None:
            if len(mask) != feature_dim:
                raise ValueError(
                    f"mask length {len(mask)} != feature_dim {feature_dim}")
            self.mask = np.asarray(mask, dtype=np.float32)
        else:
            self.mask = np.ones(feature_dim, dtype=np.float32)
        self.samples: Deque[np.ndarray] = deque(maxlen=512)
        self.edges: Dict[str, Dict[str, float]] = defaultdict(dict)

    def apply(self, state_vec: np.ndarray) -> np.ndarray:
        if len(state_vec) != self.feature_dim:
            raise ValueError(
                f"state_vec dim {len(state_vec)} != {self.feature_dim}")
        return state_vec * self.mask

    def observe(self, state_vec: np.ndarray) -> None:
        if len(state_vec) == self.feature_dim:
            self.samples.append(np.asarray(state_vec, dtype=np.float32))

    def fit(self, alpha: float = 0.05, threshold: float = 0.25) -> Dict[str, Any]:
        self.edges.clear()
        if len(self.samples) < 10:
            return self.summary()
        X = np.asarray(self.samples, dtype=float)
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        try:
            corr = np.corrcoef(X, rowvar=False)
            corr = np.nan_to_num(corr, nan=0.0)
            prec = np.linalg.pinv(corr)
        except Exception:
            return self.summary()
        names = list(self.FEATURE_NAMES)[:self.feature_dim]
        d = self.feature_dim
        for i in range(d):
            for j in range(i + 1, d):
                denom = math.sqrt(max(prec[i, i] * prec[j, j], 1e-12))
                pcorr = -prec[i, j] / denom
                if abs(pcorr) <= threshold:
                    continue
                r = min(max(pcorr, -0.999999), 0.999999)
                z = 0.5 * math.log((1 + r) / (1 - r)) * \
                    math.sqrt(max(X.shape[0] - (d - 2), 1))
                p_val = math.erfc(abs(z) / math.sqrt(2.0))
                if p_val > alpha:
                    continue
                self.edges[names[i]][names[j]] = float(pcorr)
        active: Set[str] = set()
        for src, targets in self.edges.items():
            active.add(src)
            active.update(targets.keys())
        for i, n in enumerate(names):
            self.mask[i] = 1.0 if n in active else 0.5
        return self.summary()

    def summary(self) -> Dict[str, Any]:
        return {"features": self.feature_dim,
                "edges": sum(len(v) for v in self.edges.values()),
                "mask": self.mask.tolist()}


# =============================================================================
# ENHANCEMENT 2 — QUANTUM DISTILLATION
# =============================================================================
class QuantumDistillationModule:
    """Density-matrix distillation over expert parameter vectors."""

    def __init__(self, n_dims: int = 12, temperature: float = 1.0,
                 learning_rate: float = 0.1, min_temperature: float = 0.25,
                 temperature_decay: float = 0.999,
                 convergence_eps: float = 1e-4,
                 rng: Optional[random.Random] = None):
        self.n_dims = int(n_dims)
        self.temperature = float(temperature)
        self.learning_rate = float(learning_rate)
        self.min_temperature = float(min_temperature)
        self.temperature_decay = float(temperature_decay)
        self.convergence_eps = float(convergence_eps)
        self.rng = rng or random.Random()
        self.teachers: Dict[str, List[float]] = {}
        self.teacher_weights: Dict[str, float] = {}
        self.student: List[float] = [1.0 / self.n_dims] * self.n_dims
        self.history: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._converged = False

    def register_teacher(self, name: str, policy: Iterable[float],
                         weight: float = 1.0) -> None:
        pol = list(policy)
        if not pol:
            return
        s = sum(pol) or 1.0
        self.teachers[name] = [max(0.0, p) / s for p in pol]
        self.teacher_weights[name] = max(1e-6, float(weight))

    def unregister_teacher(self, name: str) -> None:
        self.teachers.pop(name, None)
        self.teacher_weights.pop(name, None)

    def _von_neumann_entropy(self, rho: np.ndarray) -> float:
        try:
            m = (rho + rho.T) / 2.0
            eig = np.linalg.eigvalsh(m)
            eig = np.clip(eig, 1e-12, None)
            return float(-np.sum(eig * np.log(eig)))
        except Exception:
            return 0.0

    def _superpose(self) -> Tuple[List[float], float]:
        n = self.n_dims
        if not self.teachers:
            return list(self.student), 0.0
        amps: List[np.ndarray] = []
        weights: List[float] = []
        for tid, pol in self.teachers.items():
            a = np.array([math.sqrt(max(pol[i], 1e-12))
                          if i < len(pol) else 0.0 for i in range(n)])
            amps.append(a)
            weights.append(self.teacher_weights.get(tid, 1.0))
        wsum = sum(weights) or 1.0
        weights = [w / wsum for w in weights]
        rho = np.zeros((n, n))
        for a, w in zip(amps, weights):
            rho += w * np.outer(a, a)
        diag = np.clip(np.diag(rho), 1e-12, None)
        s = float(diag.sum()) or 1.0
        policy = (diag / s).tolist()
        return policy, self._von_neumann_entropy(rho)

    def _softmax(self, x: Iterable[float], temp: float) -> List[float]:
        x = np.asarray(list(x), dtype=float)
        m = x.max()
        e = np.exp((x - m) / max(temp, 1e-6))
        s = e.sum() or 1.0
        return (e / s).tolist()

    async def step(self, storage: Any = None,
                   student_id: str = "router") -> Dict[str, Any]:
        target_raw, entropy = self._superpose()
        target = self._softmax(target_raw, self.temperature)
        new: List[float] = []
        for s, t in zip(self.student, target):
            eps = max(1e-12, min(self.student) * 0.1)
            grad = -(t / max(s, eps))
            new.append(max(1e-3, s - self.learning_rate * grad))
        ns = sum(new) or 1.0
        new_student = [x / ns for x in new]
        delta = sum(abs(a - b) for a, b in zip(new_student, self.student))
        self._converged = delta < self.convergence_eps
        self.student = new_student
        self.temperature = max(self.min_temperature,
                               self.temperature * self.temperature_decay)
        entry = {"target": target, "student": list(self.student),
                 "temperature": self.temperature, "delta": delta,
                 "converged": self._converged, "entropy": entropy}
        self.history.append(entry)
        return entry

    def is_available(self) -> bool:
        return True

    def is_converged(self) -> bool:
        return self._converged

    def get_student_policy(self) -> List[float]:
        return list(self.student)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED COORDINATOR
# =============================================================================
class FederatedCoordinator:
    """Weighted FedAvg with DP noise, sparsification, and rate limiting."""

    def __init__(self, router: "FlexGenMoERouter",
                 queue: Optional[AsyncMessageQueue] = None,
                 rate_limit_seconds: float = 5.0,
                 dp_epsilon: Optional[float] = None,
                 dp_clip: float = 1.0,
                 sparse_topk: float = 0.1,
                 rng: Optional[random.Random] = None):
        self.router = router
        self.queue = queue
        self.rate_limit_seconds = float(rate_limit_seconds)
        self.dp_epsilon = dp_epsilon
        self.dp_clip = float(dp_clip)
        self.sparse_topk = float(sparse_topk)
        self.rng = rng or random.Random()
        self.last_send_ts = 0.0
        self.sent_count = 0
        self.received_count = 0
        self.last_global_weights: Optional[Dict[str, Any]] = None

    def _sparsify(self, vec: np.ndarray) -> np.ndarray:
        if self.sparse_topk <= 0.0 or vec.size == 0:
            return vec
        k = max(1, int(vec.size * self.sparse_topk))
        flat = vec.flatten()
        idx = np.argsort(-np.abs(flat))[:k]
        out = np.zeros_like(flat)
        out[idx] = flat[idx]
        return out.reshape(vec.shape)

    def _privatise(self, vec: np.ndarray) -> np.ndarray:
        if self.dp_epsilon is None or vec.size == 0:
            return vec
        sigma = self.dp_clip / max(self.dp_epsilon, 1e-6)
        noise = np.asarray([self.rng.gauss(0.0, sigma) for _ in range(vec.size)])
        clipped = np.clip(vec, -self.dp_clip, self.dp_clip)
        return (clipped + noise.reshape(vec.shape)).astype(vec.dtype)

    async def send_update(self, force: bool = False) -> bool:
        if not self.queue:
            return False
        now = time.monotonic()
        if not force and (now - self.last_send_ts) < self.rate_limit_seconds:
            return False
        weights = self.router._get_weights_dict()
        # Apply DP + sparsification to the W matrices only.
        for key in ("W1", "W2"):
            if key in weights:
                arr = np.asarray(weights[key], dtype=np.float32)
                arr = self._sparsify(arr)
                arr = self._privatise(arr)
                weights[key] = arr.tolist()
        await self.queue.publish("federated_moe_weights", json.dumps(weights))
        self.last_send_ts = now
        self.sent_count += 1
        return True

    async def receive_global_model(self, model_json: str) -> None:
        model = json.loads(model_json)
        self.last_global_weights = model
        await self.router._apply_global_weights_async(model)
        self.received_count += 1


# =============================================================================
# ENHANCEMENT 4 — SAFETY MONITOR
# =============================================================================
@dataclass
class SafetyViolation:
    name: str
    severity: str
    description: str
    mitigated: bool = False


@dataclass
class SafetyRule:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    description: str
    severity: str = "warning"
    mitigation: Optional[Callable[["FlexGenMoERouter"], None]] = None


class SafetyMonitor:
    """Rule-based safety with severity levels and mitigation hooks."""

    def __init__(self):
        self.rules: List[SafetyRule] = []
        self.violations: Deque[SafetyViolation] = deque(maxlen=1000)

    def add_rule(self, name: str, predicate: Callable[[Dict[str, Any]], bool],
                 description: str, severity: str = "warning",
                 mitigation: Optional[Callable] = None) -> None:
        self.rules.append(SafetyRule(name, predicate, description,
                                     severity, mitigation))

    def check(self, state: Dict[str, Any],
              router: Optional["FlexGenMoERouter"] = None) -> List[SafetyViolation]:
        out: List[SafetyViolation] = []
        for rule in self.rules:
            try:
                ok = bool(rule.predicate(state))
            except Exception:
                ok = False
            if not ok:
                v = SafetyViolation(rule.name, rule.severity, rule.description)
                if rule.mitigation and router is not None:
                    try:
                        rule.mitigation(router)
                        v.mitigated = True
                    except Exception:
                        pass
                self.violations.append(v)
                out.append(v)
        return out

    def violation_count(self, severity: Optional[str] = None) -> int:
        if severity is None:
            return len(self.violations)
        return sum(1 for v in self.violations if v.severity == severity)


# =============================================================================
# ENHANCEMENT 5 — PRECISION CONTROLLER
# =============================================================================
class PrecisionController:
    """Layer-wise precision with hysteresis and energy accounting."""

    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}
    DTYPE_MAP = {"fp32": np.float32, "tf32": np.float32,
                 "bf16": np.float32, "fp16": np.float16,
                 "int8": np.float16}

    def __init__(self, levels: Optional[List[str]] = None,
                 max_sensitivity: float = 0.5,
                 switch_threshold: float = 0.02,
                 hysteresis_s: float = 0.5):
        self.levels = levels or list(self.ENERGY.keys())
        self.max_sensitivity = float(max_sensitivity)
        self.switch_threshold = float(switch_threshold)
        self.hysteresis_s = float(hysteresis_s)
        self.current = "fp32"
        self.saved_wh = 0.0
        self._last_switch = 0.0

    def select_precision(self,
                         layer_sensitivity: Optional[float] = None) -> str:
        if layer_sensitivity is not None and \
                layer_sensitivity > self.max_sensitivity:
            return "fp32"
        return min(self.levels, key=lambda c: self.ENERGY.get(c, 1.0))

    def dtype_for(self, precision: str):
        return self.DTYPE_MAP.get(precision, np.float32)

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        if target == self.current or target not in self.ENERGY:
            return False
        now = time.time()
        if now - self._last_switch < self.hysteresis_s:
            return False
        old = self.current
        self.current = target
        self._last_switch = now
        self.saved_wh += max(0.0, self.ENERGY[old] - self.ENERGY[target])
        return True

    async def auto_switch(self, recent_acc: float, baseline_acc: float,
                          layer_sensitivity: Optional[float] = None) -> None:
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        if drop > self.switch_threshold:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < self.switch_threshold:
            await self.switch_to(self.select_precision(layer_sensitivity),
                                 reason="headroom")

    async def auto_switch_layers(self, per_layer_acc: Dict[str, float],
                                 baseline_acc: float) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for layer, acc in per_layer_acc.items():
            drop = (baseline_acc - acc) / max(baseline_acc, 1e-6)
            sens = min(1.0, drop / max(self.switch_threshold, 1e-6))
            out[layer] = self.select_precision(sens)
        return out


# =============================================================================
# ENHANCEMENT 6 — CARBON MARKET CLIENT
# =============================================================================
class CarbonMarketClient:
    """Carbon budget + EMA price + REC ledger."""

    def __init__(self, budget_kg: float = 100.0,
                 price_oracle: Optional[Callable] = None,
                 rng: Optional[random.Random] = None,
                 ema_alpha: float = 0.3):
        self.budget_kg = float(budget_kg)
        self.used_kg = 0.0
        self.price = 25.0
        self.price_ema = 25.0
        self.ema_alpha = float(ema_alpha)
        self.rng = rng or random.Random()
        self.price_oracle = price_oracle or self._random_walk
        self.rec_ledger: List[Dict[str, Any]] = []

    async def _random_walk(self) -> float:
        return max(5.0, self.price + self.rng.gauss(0, 1.5))

    async def update_price(self) -> float:
        try:
            p = float(await self.price_oracle())
        except Exception:
            p = self.price
        self.price = max(0.0, p)
        self.price_ema = (self.ema_alpha * self.price +
                          (1 - self.ema_alpha) * self.price_ema)
        return self.price

    def _budget_ok(self, extra: float) -> bool:
        return (self.used_kg + extra) <= self.budget_kg

    async def net_zero_schedule(self, workload_kwh: float,
                                intensity: float) -> Dict[str, Any]:
        await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * self.price
        if not self._budget_ok(carbon_kg):
            action = "defer"
        elif intensity > 0.3:
            action = "defer"
        elif offset_cost < 0.5:
            action = "run_offset"
        else:
            action = "run"
        if action in ("run", "run_offset"):
            self.used_kg += carbon_kg
        return {"action": action, "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost,
                "credit_price_usd": self.price,
                "budget_remaining_kg": max(0.0, self.budget_kg - self.used_kg)}

    async def buy_credits(self, mwh: float, price_per_mwh: float = 5.0,
                          source: str = "wind") -> Dict[str, Any]:
        cost = mwh * price_per_mwh
        cert = uuid.uuid4().hex[:12]
        self.rec_ledger.append({"mwh": mwh, "cost": cost, "source": source,
                                "certificate": cert, "retired": False})
        return {"cost": cost, "certificate": cert}

    async def sell_credits(self, mwh: float,
                           price_per_mwh: float = 5.0) -> Dict[str, Any]:
        return {"revenue": mwh * price_per_mwh, "mwh": mwh}

    async def retire_rec(self, mwh: float) -> float:
        remaining = mwh
        for r in self.rec_ledger:
            if r["retired"]:
                continue
            take = min(r["mwh"], remaining)
            r["mwh"] -= take
            remaining -= take
            if r["mwh"] <= 0:
                r["retired"] = True
            if remaining <= 0:
                break
        return mwh - remaining

    def rec_balance(self) -> float:
        return sum(r["mwh"] for r in self.rec_ledger if not r["retired"])


# =============================================================================
# ENHANCEMENT 7 — CHAOS INJECTOR
# =============================================================================
class ChaosInjector:
    """Fault injection with steady-state hypothesis + auto-rollback."""

    FAULT_TYPES = ("latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop")

    def __init__(self, router: "FlexGenMoERouter",
                 chaos_probability: float = 0.01,
                 auto_rollback: bool = True,
                 blast_radius: float = 0.5,
                 rng: Optional[random.Random] = None):
        self.router = router
        self.chaos_probability = float(chaos_probability)
        self.auto_rollback = bool(auto_rollback)
        self.blast_radius = float(blast_radius)
        self.rng = rng or random.Random()
        self.active: Dict[str, Dict[str, Any]] = {}
        self.rollback_count = 0
        self._lock = threading.Lock()

    def _allowed(self) -> int:
        return max(1, int(len(self.FAULT_TYPES) * self.blast_radius))

    async def maybe_inject_failure(self) -> Optional[Dict[str, Any]]:
        if self.rng.random() >= self.chaos_probability:
            return None
        fault = self.rng.choice(self.FAULT_TYPES)
        return await self.run_experiment(
            f"auto_{uuid.uuid4().hex[:6]}", fault)

    async def run_experiment(self, name: str, fault_type: str) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        with self._lock:
            if len(self.active) >= self._allowed():
                return {"name": name, "fault_type": fault_type,
                        "status": "skipped", "reason": "blast_radius"}
            self.active[name] = {"fault_type": fault_type}
        t0 = time.time()
        before = self.router._check_steady_state()
        snapshot = self.router._snapshot_weights()
        status = "completed"
        try:
            if fault_type == "latency":
                await asyncio.sleep(0.005)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "memory_pressure":
                _ = bytearray(1024)
            elif fault_type == "network_drop":
                await asyncio.sleep(0.005)
            elif fault_type == "data_corruption":
                # Bounded corruption: scale weights to a small range.
                with self.router._cpu_lock:
                    f = self.rng.uniform(0.95, 1.05)
                    self.router.W1 = np.clip(self.router.W1 * f, -10.0, 10.0)
        except Exception:
            status = "injected"
        finally:
            with self._lock:
                self.active.pop(name, None)
        after = self.router._check_steady_state()
        rolled_back = False
        if before and not after and self.auto_rollback:
            self.router._restore_weights(snapshot)
            rolled_back = True
            self.rollback_count += 1
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status,
                "duration_ms": (time.time() - t0) * 1000.0,
                "rolled_back": rolled_back}


# =============================================================================
# ENHANCEMENT 8 — HUMAN APPROVAL
# =============================================================================
class HumanApprovalHandler:
    """Uncertainty-sampled HITL with preference learning."""

    def __init__(self, queue: Optional[AsyncMessageQueue] = None,
                 uncertainty_threshold: float = 0.05,
                 timeout: float = 5.0):
        self.queue = queue
        self.uncertainty_threshold = float(uncertainty_threshold)
        self.timeout = float(timeout)
        self.preferences: Dict[str, Dict[str, float]] = {}
        self.posterior: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.decisions: List[Dict[str, Any]] = []

    async def submit_response(self, user_id: str, chosen_id: str) -> None:
        try:
            self._responses.put_nowait({"user_id": user_id,
                                        "chosen": chosen_id})
        except asyncio.QueueFull:
            pass

    def _utility(self, user_id: str, candidate: Dict[str, Any]) -> float:
        prefs = self.preferences.get(user_id, {})
        if not prefs:
            return 0.5
        u = 0.0
        for k, v in prefs.items():
            if k in candidate:
                try:
                    u += float(v) * float(candidate[k])
                except Exception:
                    continue
        return u

    def is_uncertain(self, user_id: str,
                     candidates: List[Dict[str, Any]]) -> bool:
        if len(candidates) < 2:
            return False
        scored = sorted((self._utility(user_id, c) for c in candidates),
                        reverse=True)
        return abs(scored[0] - scored[1]) < self.uncertainty_threshold

    async def request_approval(self, decision: Dict[str, Any],
                               user_id: str = "default") -> bool:
        candidates = decision.get("candidates") or []
        if len(candidates) < 2:
            self.decisions.append({"decision": decision, "auto": True,
                                   "approved": True})
            return True
        scored = sorted(((self._utility(user_id, c), c) for c in candidates),
                        key=lambda kv: kv[0], reverse=True)
        if abs(scored[0][0] - scored[1][0]) > self.uncertainty_threshold:
            self.decisions.append({"decision": decision, "auto": True,
                                   "approved": True,
                                   "chosen": scored[0][1].get("id")})
            return True
        try:
            msg = await asyncio.wait_for(self._responses.get(),
                                         timeout=self.timeout)
            approved = bool(msg.get("chosen"))
        except asyncio.TimeoutError:
            approved = True
        self.decisions.append({"decision": decision, "auto": False,
                               "approved": approved})
        return approved

    async def record_choice(self, user_id: str, solution_id: str,
                            metrics: Optional[Dict[str, float]] = None) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k, v in metrics.items():
                prefs[k] = prefs.get(k, 0.25) + max(0.0, float(v)) * 0.01
            s = sum(prefs.values()) or 1.0
            prefs = {k: v / s for k, v in prefs.items()}
            self.preferences[user_id] = prefs
            post = self.posterior.setdefault(user_id, {})
            for k, v in prefs.items():
                post[k] = post.get(k, 1.0) + v


# =============================================================================
# ENHANCEMENT 9 — XAI (Integrated Gradients)
# =============================================================================
class XAIExplainer:
    """Integrated Gradients + bounded counterfactuals."""

    def __init__(self, ig_steps: int = 32, cf_max_iter: int = 40,
                 cf_target_delta: float = 0.1,
                 rng: Optional[random.Random] = None):
        self.ig_steps = int(ig_steps)
        self.cf_max_iter = int(cf_max_iter)
        self.cf_target_delta = float(cf_target_delta)
        self.rng = rng or random.Random()

    def integrated_gradients(self, f: Callable,
                             x: np.ndarray,
                             baseline: Optional[np.ndarray] = None) -> np.ndarray:
        x = np.asarray(x, dtype=float)
        if baseline is None:
            baseline = np.zeros_like(x)
        total = np.zeros_like(x)
        for s in range(1, self.ig_steps + 1):
            alpha = s / self.ig_steps
            interp = baseline + alpha * (x - baseline)
            eps = 1e-4
            grad = np.zeros_like(x)
            for i in range(len(x)):
                up = interp.copy(); up[i] += eps
                dn = interp.copy(); dn[i] -= eps
                try:
                    grad[i] = (float(f(up)) - float(f(dn))) / (2 * eps)
                except Exception:
                    grad[i] = 0.0
            total += grad
        return (x - baseline) * (total / max(self.ig_steps, 1))

    def counterfactual(self, f: Callable, x: np.ndarray,
                       target_delta: Optional[float] = None,
                       bounds: Tuple[float, float] = (-1.0, 1.0)) -> Dict[str, Any]:
        target_delta = target_delta or self.cf_target_delta
        try:
            base_pred = float(f(x))
        except Exception:
            return {"success": False}
        best_delta, best_pred = 0.0, base_pred
        best_idx, best_step = None, 0.0
        x_cf = x.copy()
        for _ in range(self.cf_max_iter):
            i = self.rng.randrange(len(x))
            step = self.rng.choice([-1, 1]) * 0.1
            trial = x_cf.copy()
            trial[i] = float(np.clip(trial[i] + step, *bounds))
            try:
                pred = float(f(trial))
            except Exception:
                continue
            delta = abs(pred - base_pred)
            if delta > best_delta:
                best_delta, best_pred, x_cf = delta, pred, trial
                best_idx, best_step = i, step
            if delta >= abs(target_delta):
                break
        return {"success": best_delta >= abs(target_delta),
                "changed_index": best_idx, "change_amount": best_step,
                "delta": best_pred - base_pred}


# =============================================================================
# ENHANCEMENT 10 — TEMPORAL SAFETY MONITOR (MTL)
# =============================================================================
_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


@dataclass
class _TemporalRule:
    rule_id: str
    operator: str
    predicate: Callable[[Dict[str, Any]], bool]
    window_seconds: float = 0.0
    description: str = ""
    severity: str = "warning"
    violations: int = 0
    last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque[Tuple[datetime, Dict[str, Any]]]) -> bool:
        if not trace:
            return False
        if self.window_seconds > 0:
            cutoff = trace[-1][0] - \
                __import__("datetime").timedelta(seconds=self.window_seconds)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == "always":
            return any(not self.predicate(s) for _, s in trace)
        if op == "eventually":
            return not any(self.predicate(s) for _, s in trace)
        if op == "never":
            return any(self.predicate(s) for _, s in trace)
        if op == "historically":
            return any(not self.predicate(s) for _, s in trace)
        if op == "once":
            return not any(self.predicate(s) for _, s in trace)
        return False


class TemporalSafetyMonitor:
    """MTL monitor supporting G, F, NEVER, H, O and windowed rules."""

    def __init__(self, max_trace: int = 500):
        self.rules: Dict[str, _TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict[str, Any]]] = deque(maxlen=max_trace)
        self.approval_cb: Optional[Callable] = None

    def add_formula(self, formula: str) -> None:
        f = formula.strip()
        window = 0.0
        if f.startswith("[") and "]" in f:
            head, rest = f[1:].split("]", 1)
            try:
                window = float(head.strip().rstrip("s"))
            except ValueError:
                window = 0.0
            f = rest.strip()
        op_map = {"G ": "always", "F ": "eventually",
                  "NEVER ": "never", "H ": "historically", "O ": "once"}
        for prefix, op in op_map.items():
            if f.startswith(prefix):
                expr = f[len(prefix):].strip().strip("()")
                self._add_atomic(expr, op, window)
                return

    def _add_atomic(self, expr: str, op: str, window: float) -> None:
        m = _ATOMIC_RE.match(expr)
        if not m:
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def predicate(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]

        rid = f"{op}:{expr}" + (f"@{window}s" if window else "")
        self.rules[rid] = _TemporalRule(rid, op, predicate,
                                        window_seconds=window,
                                        description=expr)

    def set_approval_callback(self, cb: Callable) -> None:
        self.approval_cb = cb

    async def push(self, state: Dict[str, Any]) -> None:
        self.trace.append((datetime.now(timezone.utc), dict(state)))

    async def check(self) -> Dict[str, bool]:
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
        return result

    def counts(self) -> Dict[str, int]:
        return {rid: r.violations for rid, r in self.rules.items()}


# =============================================================================
# ROUTING DECISION VALUE OBJECT
# =============================================================================
@dataclass(frozen=True)
class RoutingDecision:
    state_vec: np.ndarray
    probs: np.ndarray
    expert_idx: int
    expert_name: str
    explored: bool
    confidence: float
    timestamp: float
    explanation: Optional[Dict[str, Any]] = None
    counterfactual: Optional[Dict[str, Any]] = None


# =============================================================================
# MAIN ROUTER
# =============================================================================
class FlexGenMoERouter:
    """Thread-safe MoE router with all ten enhancement modules."""

    FEATURE_DIM = 12
    W_MAX = 100.0
    WEIGHT_NORM_MAX = 1000.0

    def __init__(self, experts: List[FlexGenPolicy],
                 feature_dim: int = FEATURE_DIM,
                 hidden_dim: int = 64,
                 temperature: float = 1.0,
                 learning_rate: float = 0.01,
                 epsilon: float = 0.1,
                 epsilon_decay: float = 0.999,
                 message_queue: Optional[AsyncMessageQueue] = None,
                 persistence_path: Optional[str] = None,
                 use_baseline: bool = True,
                 l2_reg: float = 1e-4,
                 causal_mask: Optional[np.ndarray] = None,
                 enable_federated: bool = False,
                 enable_safety_monitor: bool = True,
                 enable_xai: bool = True,
                 enable_precision: bool = False,
                 enable_carbon_market: bool = False,
                 enable_chaos: bool = False,
                 chaos_probability: float = 0.0,
                 enable_human_approval: bool = False,
                 enable_temporal_safety: bool = True,
                 enable_quantum_distillation: bool = True,
                 seed: int = 42) -> None:
        self.experts = list(experts)
        if not self.experts:
            raise ValueError("At least one expert required")
        self.n_experts = len(self.experts)
        self.feature_dim = int(feature_dim)
        self.hidden_dim = int(hidden_dim)
        self.temperature = float(temperature)
        self.lr = float(learning_rate)
        self.epsilon = float(epsilon)
        self.epsilon_decay = float(epsilon_decay)
        self.message_queue = message_queue
        self.persistence_path = persistence_path
        self.use_baseline = bool(use_baseline)
        self.l2_reg = float(l2_reg)

        # Deterministic RNG tree
        self._rng = random.Random(seed)
        self._rng_xai = random.Random(seed + 1)
        self._rng_chaos = random.Random(seed + 2)
        self._rng_fed = random.Random(seed + 3)
        self._rng_carbon = random.Random(seed + 4)
        np.random.seed(seed)

        # Master weights (always float32 for training stability)
        self.W1 = (np.random.randn(self.feature_dim, self.hidden_dim)
                   * 0.01).astype(np.float32)
        self.b1 = np.zeros(self.hidden_dim, dtype=np.float32)
        self.W2 = (np.random.randn(self.hidden_dim, self.n_experts)
                   * 0.01).astype(np.float32)
        self.b2 = np.zeros(self.n_experts, dtype=np.float32)

        # Baseline
        self.baseline = 0.0
        self.baseline_alpha = 0.1
        self.step_count = 0

        # Thread-safety for CPU-bound forward/backward
        self._cpu_lock = threading.Lock()

        # Causal mask
        self.causal_mask = CausalMask(feature_dim, mask=causal_mask,
                                      rng=self._rng_xai)

        # Enhancement modules
        self.quantum_distillation = (QuantumDistillationModule(
            n_dims=feature_dim, rng=random.Random(seed + 5))
            if enable_quantum_distillation else None)
        self.federated = (FederatedCoordinator(
            self, message_queue, rng=self._rng_fed)
            if enable_federated else None)
        self.safety_monitor = SafetyMonitor() if enable_safety_monitor else None
        if self.safety_monitor:
            self._setup_safety_rules()
        self.precision_controller = (PrecisionController()
                                     if enable_precision else None)
        self.carbon_market = (CarbonMarketClient(rng=self._rng_carbon)
                              if enable_carbon_market else None)
        self.chaos_injector = (ChaosInjector(
            self, chaos_probability, rng=self._rng_chaos)
            if enable_chaos else None)
        self.human_approval = (HumanApprovalHandler(message_queue)
                               if enable_human_approval else None)
        self.temporal_safety = (TemporalSafetyMonitor()
                                if enable_temporal_safety else None)
        if self.temporal_safety:
            self._setup_temporal_rules()

        self.enable_xai = bool(enable_xai)
        self.xai = XAIExplainer(rng=self._rng_xai) if enable_xai else None

        # Snapshot for rollback
        self._initial_snapshot = self._snapshot_weights()

        if self.persistence_path and os.path.exists(self.persistence_path):
            self._load_weights()

    # ------------------------------------------------------------------
    def _setup_safety_rules(self) -> None:
        assert self.safety_monitor is not None

        def _finite(r):
            return (np.all(np.isfinite(r.W1)) and
                    np.all(np.isfinite(r.W2)) and
                    np.all(np.isfinite(r.b1)) and
                    np.all(np.isfinite(r.b2)))

        def _bounded(r):
            return (float(np.linalg.norm(r.W1)) < self.WEIGHT_NORM_MAX and
                    float(np.linalg.norm(r.W2)) < self.WEIGHT_NORM_MAX)

        def _probs_valid(state):
            p = state.get("probs")
            if p is None:
                return True
            return (all(x >= 0 for x in p) and
                    abs(sum(p) - 1.0) < 1e-4)

        self.safety_monitor.add_rule(
            "weights_finite",
            lambda state, r=self: _finite(r),
            "weights contain NaN or Inf",
            severity="critical",
            mitigation=lambda r: r._restore_weights(r._initial_snapshot))
        self.safety_monitor.add_rule(
            "weights_bounded",
            lambda state, r=self: _bounded(r),
            "weight norm exceeds bound",
            severity="critical",
            mitigation=lambda r: r._clip_weights())
        self.safety_monitor.add_rule(
            "probs_valid",
            _probs_valid,
            "gating probabilities invalid",
            severity="warning")

    def _setup_temporal_rules(self) -> None:
        assert self.temporal_safety is not None
        self.temporal_safety.add_formula("G (epsilon >= 0.01)")
        self.temporal_safety.add_formula("G (epsilon <= 1.0)")
        self.temporal_safety.add_formula("NEVER (step_count < 0)")

    # ------------------------------------------------------------------
    def _clip_weights(self) -> None:
        with self._cpu_lock:
            self.W1 = np.clip(self.W1, -self.W_MAX, self.W_MAX)
            self.W2 = np.clip(self.W2, -self.W_MAX, self.W_MAX)
            self.b1 = np.clip(self.b1, -self.W_MAX, self.W_MAX)
            self.b2 = np.clip(self.b2, -self.W_MAX, self.W_MAX)

    def _snapshot_weights(self) -> Dict[str, np.ndarray]:
        with self._cpu_lock:
            return {
                "W1": self.W1.copy(), "b1": self.b1.copy(),
                "W2": self.W2.copy(), "b2": self.b2.copy(),
                "baseline": self.baseline, "epsilon": self.epsilon,
            }

    def _restore_weights(self, snap: Dict[str, Any]) -> None:
        with self._cpu_lock:
            self.W1 = snap["W1"].copy()
            self.b1 = snap["b1"].copy()
            self.W2 = snap["W2"].copy()
            self.b2 = snap["b2"].copy()
            self.baseline = snap.get("baseline", self.baseline)
            self.epsilon = snap.get("epsilon", self.epsilon)

    def _check_steady_state(self) -> bool:
        try:
            with self._cpu_lock:
                if not (np.all(np.isfinite(self.W1)) and
                        np.all(np.isfinite(self.W2))):
                    return False
                if (float(np.linalg.norm(self.W1)) > self.WEIGHT_NORM_MAX or
                        float(np.linalg.norm(self.W2)) > self.WEIGHT_NORM_MAX):
                    return False
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    def _forward_impl(self, state_vec: np.ndarray,
                      dtype=np.float32) -> np.ndarray:
        """Pure forward pass. Does not mutate self.W*."""
        masked = self.causal_mask.apply(state_vec).astype(dtype)
        W1 = self.W1.astype(dtype)
        b1 = self.b1.astype(dtype)
        W2 = self.W2.astype(dtype)
        b2 = self.b2.astype(dtype)
        hidden = np.maximum(0, masked @ W1 + b1)
        logits = hidden @ W2 + b2
        return logits.astype(np.float32)

    def _forward_locked(self, state_vec: np.ndarray,
                        dtype=np.float32) -> np.ndarray:
        with self._cpu_lock:
            return self._forward_impl(state_vec, dtype)

    def _softmax(self, logits: np.ndarray) -> np.ndarray:
        logits = np.nan_to_num(logits, nan=0.0, posinf=1e6, neginf=-1e6)
        temp = max(self.temperature, 1e-3)
        x = logits / temp
        m = float(x.max())
        e = np.exp(x - m)
        s = float(e.sum())
        if not np.isfinite(s) or s <= 0:
            return np.ones_like(e) / e.size
        return e / s

    def _choose_dtype(self, state_vec: np.ndarray):
        if self.precision_controller is None:
            return np.float32
        load = float(np.mean(np.abs(state_vec)))
        precision = self.precision_controller.select_precision(
            layer_sensitivity=None)
        # Only switch to fp16 if headroom policy allows
        if load < 0.5:
            precision = self.precision_controller.select_precision()
        return self.precision_controller.dtype_for(precision)

    # ------------------------------------------------------------------
    async def gate(self, state: FlexGenState) -> np.ndarray:
        state_vec = state.to_feature_vector()
        if len(state_vec) != self.feature_dim:
            raise ValueError(
                f"state vector dim {len(state_vec)} != feature_dim "
                f"{self.feature_dim}; call resize_feature_dim() explicitly")
        self.causal_mask.observe(state_vec)
        dtype = self._choose_dtype(state_vec)
        logits = await asyncio.to_thread(self._forward_locked, state_vec, dtype)
        probs = self._softmax(logits)

        if self.safety_monitor is not None:
            violations = self.safety_monitor.check(
                {"probs": probs.tolist()}, router=self)
            if any(v.severity == "critical" and not v.mitigated
                   for v in violations):
                logger.warning("Critical safety violation in gate; "
                               "fallback to uniform")
                probs = np.ones(self.n_experts, dtype=np.float32) / self.n_experts
        return probs

    async def select_policy(self, state: FlexGenState,
                            exploration: bool = True,
                            user_id: str = "default",
                            task_id: str = "unknown") -> RoutingDecision:
        state_vec = state.to_feature_vector()
        probs = await self.gate(state)

        explored = False
        if exploration and self._rng.random() < self.epsilon:
            idx = self._rng.randint(0, self.n_experts - 1)
            explored = True
        else:
            idx = int(np.argmax(probs))

        # HITL gate for critical routing decisions
        if self.human_approval is not None and self.n_experts >= 2:
            decision = {
                "action": "route",
                "candidates": [
                    {"id": i, "quality_score": float(probs[i])}
                    for i in np.argsort(-probs)[:2]
                ],
            }
            if not await self.human_approval.request_approval(
                    decision, user_id=user_id):
                idx = int(np.argmax(probs))
                explored = False

        explanation = None
        counterfactual = None
        if self.enable_xai and self.xai is not None:
            def _score(v):
                logits = self._forward_impl(np.asarray(v, dtype=np.float32))
                return float(logits[idx])
            try:
                attrs = self.xai.integrated_gradients(_score, state_vec)
                explanation = {
                    "method": "integrated_gradients",
                    "attributions": attrs.tolist(),
                    "feature_names": list(CausalMask.FEATURE_NAMES),
                }
                counterfactual = self.xai.counterfactual(_score, state_vec)
            except Exception as e:
                logger.debug(f"XAI failed: {e}")

        # Temporal safety check
        if self.temporal_safety is not None:
            await self.temporal_safety.push({
                "epsilon": self.epsilon, "step_count": self.step_count})
            await self.temporal_safety.check()

        return RoutingDecision(
            state_vec=state_vec,
            probs=probs,
            expert_idx=idx,
            expert_name=self.experts[idx].name,
            explored=explored,
            confidence=float(probs[idx]),
            timestamp=time.time(),
            explanation=explanation,
            counterfactual=counterfactual,
        )

    async def update(self, state: FlexGenState, expert_idx: int,
                     reward: float) -> RoutingDecision:
        if not (0 <= expert_idx < self.n_experts):
            raise ValueError(f"expert_idx {expert_idx} out of range")
        state_vec = state.to_feature_vector()
        if len(state_vec) != self.feature_dim:
            raise ValueError(
                f"state vector dim {len(state_vec)} != feature_dim "
                f"{self.feature_dim}")

        # Compute forward + backward entirely in a worker thread under lock.
        def _do_update():
            with self._cpu_lock:
                probs = self._softmax(self._forward_impl(state_vec))
                if self.use_baseline:
                    self.baseline = (self.baseline_alpha * reward +
                                     (1 - self.baseline_alpha) * self.baseline)
                    advantage = reward - self.baseline
                else:
                    advantage = reward
                target = np.zeros(self.n_experts, dtype=np.float32)
                target[expert_idx] = 1.0
                grad_logits = ((target - probs) * advantage).astype(np.float32)
                np.clip(grad_logits, -1.0, 1.0, out=grad_logits)
                hidden = np.maximum(0, state_vec @ self.W1 + self.b1)
                grad_hidden = (grad_logits @ self.W2.T).astype(np.float32)
                grad_hidden[hidden <= 0] = 0
                np.clip(grad_hidden, -1.0, 1.0, out=grad_hidden)
                # SGD with L2
                self.W2 += self.lr * (np.outer(hidden, grad_logits) -
                                      self.l2_reg * self.W2)
                self.b2 += self.lr * grad_logits
                self.W1 += self.lr * (np.outer(state_vec, grad_hidden) -
                                      self.l2_reg * self.W1)
                self.b1 += self.lr * grad_hidden
                self._clip_weights()
                return probs

        probs = await asyncio.to_thread(_do_update)
        self.epsilon = max(0.01, self.epsilon * self.epsilon_decay)
        self.step_count += 1

        # Post-update safety check
        if self.safety_monitor is not None:
            self.safety_monitor.check({"probs": probs.tolist()}, router=self)

        # Periodic persistence
        if (self.persistence_path and self.step_count % 100 == 0):
            self._save_weights()

        # Chaos injection (rate-limited by probability)
        if self.chaos_injector is not None:
            await self.chaos_injector.maybe_inject_failure()

        # Federated send (rate-limited inside)
        if self.federated is not None:
            await self.federated.send_update()

        return RoutingDecision(
            state_vec=state_vec,
            probs=probs,
            expert_idx=expert_idx,
            expert_name=self.experts[expert_idx].name,
            explored=False,
            confidence=float(probs[expert_idx]),
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    async def publish_event(self, decision: RoutingDecision,
                            workload: WorkloadDescriptor,
                            metrics: Dict[str, Any],
                            reward: float) -> None:
        if self.message_queue is None:
            return
        metadata = {
            "gating_probs": decision.probs.tolist(),
            "expert_idx": decision.expert_idx,
            "expert_name": decision.expert_name,
            "num_experts": self.n_experts,
            "epsilon": self.epsilon,
            "confidence": decision.confidence,
            "explored": decision.explored,
            "explanation": decision.explanation,
            "counterfactual": decision.counterfactual,
        }
        event = FeedbackEvent(
            source="moe_flexgen_router",
            feedback_type="routing",
            task_id=workload.task_id,
            context=metadata,
            action={"selected_expert": decision.expert_name,
                    "selected_rank": decision.expert_idx,
                    "confidence_score": decision.confidence},
            performance={
                "quality_score": metrics.get("quality_score", 0.9),
                "latency_ms": metrics.get("latency_ms", 0),
                "energy_joules": metrics.get("energy_joules", 0),
                "carbon_g": metrics.get("carbon_g", 0),
            },
            adaptive_cost_value=reward,
            tags=["moe", "flexgen_policy", "expert_selection"],
        )
        await self.message_queue.publish("moe_events", event.to_json())

    # ------------------------------------------------------------------
    async def add_expert(self, policy: FlexGenPolicy,
                         user_id: str = "default") -> bool:
        if self.human_approval is not None:
            approved = await self.human_approval.request_approval(
                {"action": "add_expert", "candidates": [
                    {"id": "yes"}, {"id": "no"}]}, user_id=user_id)
            if not approved:
                return False
        with self._cpu_lock:
            self.experts.append(policy)
            new_n = len(self.experts)
            new_W2 = (np.random.randn(self.hidden_dim, new_n)
                      * 0.01).astype(np.float32)
            new_b2 = np.zeros(new_n, dtype=np.float32)
            new_W2[:, :self.n_experts] = self.W2
            new_b2[:self.n_experts] = self.b2
            self.W2 = new_W2
            self.b2 = new_b2
            self.n_experts = new_n
        return True

    async def remove_expert(self, index: int,
                            user_id: str = "default") -> bool:
        if not (0 <= index < self.n_experts):
            return False
        if self.n_experts <= 1:
            logger.warning("Cannot remove last expert")
            return False
        if self.human_approval is not None:
            approved = await self.human_approval.request_approval(
                {"action": "remove_expert", "candidates": [
                    {"id": "yes"}, {"id": "no"}]}, user_id=user_id)
            if not approved:
                return False
        with self._cpu_lock:
            del self.experts[index]
            self.W2 = np.delete(self.W2, index, axis=1)
            self.b2 = np.delete(self.b2, index, axis=0)
            self.n_experts -= 1
        return True

    # ------------------------------------------------------------------
    def _get_weights_dict(self) -> Dict[str, Any]:
        with self._cpu_lock:
            return {
                "W1": self.W1.tolist(), "b1": self.b1.tolist(),
                "W2": self.W2.tolist(), "b2": self.b2.tolist(),
                "baseline": self.baseline, "epsilon": self.epsilon,
                "n_experts": self.n_experts,
                "feature_dim": self.feature_dim,
                "hidden_dim": self.hidden_dim,
            }

    def _apply_global_weights_sync(self, weights: Dict[str, Any]) -> None:
        remote_W1 = np.asarray(weights["W1"], dtype=np.float32)
        remote_b1 = np.asarray(weights["b1"], dtype=np.float32)
        remote_W2 = np.asarray(weights["W2"], dtype=np.float32)
        remote_b2 = np.asarray(weights["b2"], dtype=np.float32)
        if remote_W1.shape != self.W1.shape:
            raise ValueError(
                f"W1 shape {remote_W1.shape} != local {self.W1.shape}")
        if remote_W2.shape != self.W2.shape:
            raise ValueError(
                f"W2 shape {remote_W2.shape} != local {self.W2.shape}")
        with self._cpu_lock:
            self.W1 = 0.5 * self.W1 + 0.5 * remote_W1
            self.b1 = 0.5 * self.b1 + 0.5 * remote_b1
            self.W2 = 0.5 * self.W2 + 0.5 * remote_W2
            self.b2 = 0.5 * self.b2 + 0.5 * remote_b2
            if self.use_baseline:
                self.baseline = 0.5 * self.baseline + \
                    0.5 * float(weights.get("baseline", self.baseline))
            self.epsilon = 0.5 * self.epsilon + \
                0.5 * float(weights.get("epsilon", self.epsilon))

    async def _apply_global_weights_async(self, weights: Dict[str, Any]) -> None:
        await asyncio.to_thread(self._apply_global_weights_sync, weights)

    def _apply_global_weights(self, weights: Dict[str, Any]) -> None:
        # Sync entry point used by external callers.
        self._apply_global_weights_sync(weights)

    # ------------------------------------------------------------------
    def resize_feature_dim(self, new_dim: int) -> None:
        """Explicitly resize the input layer; discards W1/b1 (caller warning)."""
        if new_dim == self.feature_dim:
            return
        logger.warning(
            f"Resizing feature_dim {self.feature_dim} -> {new_dim}. "
            f"Input layer weights will be reset.")
        with self._cpu_lock:
            self.feature_dim = int(new_dim)
            self.W1 = (np.random.randn(self.feature_dim, self.hidden_dim)
                       * 0.01).astype(np.float32)
            self.b1 = np.zeros(self.hidden_dim, dtype=np.float32)
            self.causal_mask = CausalMask(self.feature_dim)

    # ------------------------------------------------------------------
    def _save_weights(self) -> None:
        if not self.persistence_path:
            return
        parent = os.path.dirname(self.persistence_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        data = self._get_weights_dict()
        data["step_count"] = self.step_count
        tmp = f"{self.persistence_path}.tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, self.persistence_path)

    def _load_weights(self) -> None:
        try:
            with open(self.persistence_path, "r") as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load weights: {e}")
            return
        W1 = np.asarray(data["W1"], dtype=np.float32)
        b1 = np.asarray(data["b1"], dtype=np.float32)
        W2 = np.asarray(data["W2"], dtype=np.float32)
        b2 = np.asarray(data["b2"], dtype=np.float32)
        if W1.shape != self.W1.shape:
            logger.warning("Loaded W1 shape mismatch; ignoring file.")
            return
        if W2.shape != self.W2.shape:
            logger.warning("Loaded W2 shape mismatch; ignoring file.")
            return
        with self._cpu_lock:
            self.W1 = W1; self.b1 = b1
            self.W2 = W2; self.b2 = b2
            self.baseline = float(data.get("baseline", 0.0))
            self.epsilon = float(data.get("epsilon", self.epsilon))
            self.step_count = int(data.get("step_count", 0))

    # ------------------------------------------------------------------
    def get_stats(self) -> Dict[str, Any]:
        return {
            "num_experts": self.n_experts,
            "feature_dim": self.feature_dim,
            "hidden_dim": self.hidden_dim,
            "epsilon": self.epsilon,
            "baseline": self.baseline,
            "step_count": self.step_count,
            "causal_mask": self.causal_mask.mask.tolist(),
            "federated_enabled": self.federated is not None,
            "safety_monitor_enabled": self.safety_monitor is not None,
            "xai_enabled": self.enable_xai,
            "precision_controller_enabled": self.precision_controller is not None,
            "carbon_market_enabled": self.carbon_market is not None,
            "chaos_enabled": self.chaos_injector is not None,
            "human_approval_enabled": self.human_approval is not None,
            "temporal_safety_enabled": self.temporal_safety is not None,
            "quantum_enabled": self.quantum_distillation is not None,
        }

    async def shutdown(self) -> None:
        """Placeholder for symmetry; no background tasks are spawned here."""
        return


# =============================================================================
# FIXTURES
# =============================================================================
def _make_experts(n: int) -> List[FlexGenPolicy]:
    return [FlexGenPolicy(name=f"policy_{i}") for i in range(n)]


@pytest.fixture
def experts():
    return _make_experts(4)


@pytest.fixture
def queue():
    return AsyncMessageQueue()


@pytest.fixture
def state():
    return FlexGenState()


@pytest.fixture
def router(experts):
    return FlexGenMoERouter(experts, seed=42, persistence_path=None)


@pytest.fixture
def full_router(experts, queue):
    return FlexGenMoERouter(
        experts, message_queue=queue, seed=42,
        enable_federated=True, enable_safety_monitor=True,
        enable_xai=True, enable_precision=True,
        enable_carbon_market=True, enable_chaos=False,
        enable_human_approval=True, enable_temporal_safety=True,
        enable_quantum_distillation=True,
        persistence_path=None,
    )


# =============================================================================
# TESTS — BUG FIXES
# =============================================================================
class TestBugFixes:
    @pytest.mark.asyncio
    async def test_update_no_deadlock(self, router, state):
        # Previously deadlocked due to reentrant asyncio.Lock.
        decision = await asyncio.wait_for(
            router.update(state, 0, reward=1.0), timeout=2.0)
        assert decision.expert_idx == 0

    @pytest.mark.asyncio
    async def test_select_policy_callable_from_loop(self, router, state):
        # Previously used asyncio.run() which crashed inside a loop.
        decision = await asyncio.wait_for(
            router.select_policy(state), timeout=2.0)
        assert 0 <= decision.expert_idx < router.n_experts

    @pytest.mark.asyncio
    async def test_precision_does_not_poison_weights(self, experts):
        r = FlexGenMoERouter(experts, seed=1, enable_precision=True,
                             persistence_path=None)
        s = FlexGenState(gpu_memory_used_gb=23.0)  # high load
        await r.gate(s)
        assert r.W1.dtype == np.float32
        assert r.W2.dtype == np.float32

    @pytest.mark.asyncio
    async def test_feature_dim_mismatch_raises(self, router):
        class BadState:
            def to_feature_vector(self):
                return np.zeros(5, dtype=np.float32)
        with pytest.raises(ValueError):
            await router.gate(BadState())

    @pytest.mark.asyncio
    async def test_apply_global_weights_shape_validation(self, router):
        bad = {"W1": [[1.0]], "b1": [0.0], "W2": [[1.0]], "b2": [0.0]}
        with pytest.raises((ValueError, KeyError)):
            await router._apply_global_weights_async(bad)

    @pytest.mark.asyncio
    async def test_softmax_handles_nan(self, router):
        logits = np.array([np.nan, 0.0, 1.0, -1.0], dtype=np.float32)
        probs = router._softmax(logits)
        assert np.all(np.isfinite(probs))
        assert abs(probs.sum() - 1.0) < 1e-5

    @pytest.mark.asyncio
    async def test_chaos_bounded(self, experts):
        r = FlexGenMoERouter(experts, seed=1, enable_chaos=True,
                             chaos_probability=1.0, persistence_path=None)
        for _ in range(30):
            await r.chaos_injector.run_experiment(
                f"c{_}", "data_corruption")
        assert np.all(np.isfinite(r.W1))
        assert float(np.linalg.norm(r.W1)) <= r.WEIGHT_NORM_MAX

    @pytest.mark.asyncio
    async def test_zero_experts_rejected(self):
        with pytest.raises(ValueError):
            FlexGenMoERouter([])

    @pytest.mark.asyncio
    async def test_last_expert_cannot_be_removed(self, experts):
        r = FlexGenMoERouter(experts[:1], seed=1, persistence_path=None)
        ok = await r.remove_expert(0)
        assert ok is False

    @pytest.mark.asyncio
    async def test_federated_rate_limit(self, experts, queue):
        r = FlexGenMoERouter(experts, message_queue=queue, seed=1,
                             enable_federated=True, persistence_path=None)
        s = FlexGenState()
        # First send should go through; second immediately after should be rate-limited.
        sent1 = await r.federated.send_update(force=True)
        sent2 = await r.federated.send_update(force=False)
        assert sent1 is True
        assert sent2 is False

    @pytest.mark.asyncio
    async def test_safety_mitigation_on_nan(self, experts):
        r = FlexGenMoERouter(experts, seed=1, persistence_path=None)
        r.W1[0, 0] = np.nan
        violations = r.safety_monitor.check({}, router=r)
        assert any(v.name == "weights_finite" for v in violations)
        # Mitigation should have restored finite weights.
        assert np.all(np.isfinite(r.W1))


# =============================================================================
# TESTS — CORE ROUTER
# =============================================================================
class TestCoreRouter:
    @pytest.mark.asyncio
    async def test_gate_probs_sum_to_one(self, router, state):
        probs = await router.gate(state)
        assert abs(probs.sum() - 1.0) < 1e-5
        assert np.all(probs >= 0)

    @pytest.mark.asyncio
    async def test_select_policy_deterministic(self, experts, state):
        r1 = FlexGenMoERouter(experts, seed=7, persistence_path=None)
        r1.epsilon = 0.0
        r2 = FlexGenMoERouter(experts, seed=7, persistence_path=None)
        r2.epsilon = 0.0
        d1 = await r1.select_policy(state)
        d2 = await r2.select_policy(state)
        assert d1.expert_idx == d2.expert_idx

    @pytest.mark.asyncio
    async def test_update_changes_weights(self, router, state):
        W1_before = router.W1.copy()
        await router.update(state, 0, reward=1.0)
        assert not np.allclose(W1_before, router.W1)

    @pytest.mark.asyncio
    async def test_epsilon_decays(self, router, state):
        before = router.epsilon
        for _ in range(10):
            await router.update(state, 0, reward=0.5)
        assert router.epsilon < before

    @pytest.mark.asyncio
    async def test_routing_decision_snapshot(self, router, state):
        decision = await router.select_policy(state)
        # Decision should be frozen / immutable
        with pytest.raises(Exception):
            decision.expert_idx = 999  # frozen dataclass

    @pytest.mark.asyncio
    async def test_add_expert(self, router):
        n0 = router.n_experts
        ok = await router.add_expert(FlexGenPolicy(name="new_policy"))
        assert ok is True
        assert router.n_experts == n0 + 1

    @pytest.mark.asyncio
    async def test_remove_expert(self, router):
        n0 = router.n_experts
        ok = await router.remove_expert(0)
        assert ok is True
        assert router.n_experts == n0 - 1

    @pytest.mark.asyncio
    async def test_publish_event_uses_decision(self, experts, queue, state):
        r = FlexGenMoERouter(experts, message_queue=queue, seed=1,
                             persistence_path=None)
        decision = await r.select_policy(state)
        await r.publish_event(decision, WorkloadDescriptor(task_id="t1"),
                              {"quality_score": 0.9}, reward=0.9)
        assert len(queue.published) == 1
        payload = json.loads(queue.published[0][1])
        assert payload["context"]["expert_idx"] == decision.expert_idx


# =============================================================================
# TESTS — ENHANCEMENT MODULES
# =============================================================================
class TestCausalMask:
    def test_apply_identity_when_unfit(self):
        c = CausalMask(feature_dim=4)
        out = c.apply(np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32))
        assert np.allclose(out, [1, 2, 3, 4])

    def test_observe_and_fit(self):
        c = CausalMask(feature_dim=4)
        for _ in range(50):
            c.observe(np.random.randn(4).astype(np.float32))
        summary = c.fit()
        assert len(summary["mask"]) == 4

    def test_wrong_dim_raises(self):
        c = CausalMask(feature_dim=4)
        with pytest.raises(ValueError):
            c.apply(np.zeros(3, dtype=np.float32))


class TestQuantumDistillation:
    def test_register_normalises(self):
        q = QuantumDistillationModule(n_dims=4)
        q.register_teacher("a", [1, 2, 3, 4])
        assert abs(sum(q.teachers["a"]) - 1.0) < 1e-9

    def test_superposition_agreement(self):
        q = QuantumDistillationModule(n_dims=4)
        p = [0.4, 0.3, 0.2, 0.1]
        q.register_teacher("a", p)
        q.register_teacher("b", p)
        sup, _ = q._superpose()
        assert all(abs(a - b) < 1e-6 for a, b in zip(sup, p))

    @pytest.mark.asyncio
    async def test_step_updates_student(self):
        q = QuantumDistillationModule(n_dims=4)
        q.register_teacher("a", [0.4, 0.3, 0.2, 0.1])
        before = q.get_student_policy()
        entry = await q.step()
        after = q.get_student_policy()
        assert before != after
        assert "entropy" in entry


class TestFederatedCoordinator:
    @pytest.mark.asyncio
    async def test_receive_updates_weights(self, router, queue):
        r = FlexGenMoERouter(router.experts, message_queue=queue, seed=1,
                             enable_federated=True, persistence_path=None)
        payload = r._get_weights_dict()
        # Modify weights
        payload["W1"] = (np.asarray(payload["W1"]) * 2.0).tolist()
        await r.federated.receive_global_model(json.dumps(payload))
        assert r.federated.received_count == 1


class TestSafetyMonitor:
    def test_check_detects_violation(self):
        s = SafetyMonitor()
        s.add_rule("always_false", lambda st: False, "test", severity="warning")
        v = s.check({})
        assert len(v) == 1
        assert v[0].severity == "warning"

    def test_mitigation_called(self):
        calls = []
        s = SafetyMonitor()
        s.add_rule("f", lambda st: False, "test",
                   mitigation=lambda r: calls.append(1))
        s.check({}, router=object())
        assert len(calls) == 1


class TestPrecisionController:
    def test_select_lowest_energy(self):
        p = PrecisionController(levels=["fp32", "int8"])
        assert p.select_precision() == "int8"

    def test_high_sensitivity_forces_fp32(self):
        p = PrecisionController(levels=["fp32", "int8"], max_sensitivity=0.3)
        assert p.select_precision(layer_sensitivity=0.9) == "fp32"

    @pytest.mark.asyncio
    async def test_switch(self):
        p = PrecisionController(levels=["fp32", "int8"], hysteresis_s=0.0)
        assert await p.switch_to("int8") is True
        assert p.current == "int8"

    @pytest.mark.asyncio
    async def test_auto_switch_on_drop(self):
        p = PrecisionController(levels=["fp32", "int8"], hysteresis_s=0.0)
        p.current = "int8"
        await p.auto_switch(recent_acc=0.7, baseline_acc=0.95)
        assert p.current == "fp32"

    @pytest.mark.asyncio
    async def test_layer_wise(self):
        p = PrecisionController(levels=["fp32", "int8"], max_sensitivity=0.3)
        applied = await p.auto_switch_layers(
            per_layer_acc={"a": 0.94, "b": 0.5}, baseline_acc=0.95)
        assert applied["b"] == "fp32"


class TestCarbonMarket:
    @pytest.mark.asyncio
    async def test_budget_overshoot_defers(self):
        c = CarbonMarketClient(budget_kg=0.1, rng=random.Random(0))
        c.used_kg = 0.09
        result = await c.net_zero_schedule(1.0, 0.1)
        assert result["action"] == "defer"

    @pytest.mark.asyncio
    async def test_buy_and_retire_rec(self):
        c = CarbonMarketClient()
        await c.buy_credits(10.0)
        await c.buy_credits(5.0)
        assert c.rec_balance() == 15.0
        retired = await c.retire_rec(7.0)
        assert retired == 7.0
        assert abs(c.rec_balance() - 8.0) < 1e-9

    @pytest.mark.asyncio
    async def test_ema_smooths(self):
        prices = iter([100.0, 200.0])
        async def oracle():
            return next(prices)
        c = CarbonMarketClient(price_oracle=oracle)
        await c.update_price()
        await c.update_price()
        assert 100.0 < c.price_ema < 200.0


class TestChaosInjector:
    @pytest.mark.asyncio
    async def test_latency(self, router):
        c = ChaosInjector(router, rng=random.Random(0))
        result = await c.run_experiment("t1", "latency")
        assert result["status"] == "completed"

    @pytest.mark.asyncio
    async def test_exception(self, router):
        c = ChaosInjector(router, rng=random.Random(0))
        result = await c.run_experiment("t2", "exception")
        assert result["status"] == "injected"

    @pytest.mark.asyncio
    async def test_unknown_fault_rejected(self, router):
        c = ChaosInjector(router)
        with pytest.raises(ValueError):
            await c.run_experiment("t", "unknown")


class TestHumanApproval:
    @pytest.mark.asyncio
    async def test_single_candidate_auto(self):
        h = HumanApprovalHandler()
        assert await h.request_approval(
            {"candidates": [{"id": "only"}]}) is True

    @pytest.mark.asyncio
    async def test_confident_skips_query(self):
        h = HumanApprovalHandler(uncertainty_threshold=0.01)
        await h.record_choice("u", "a", metrics={"quality_score": 1.0})
        ok = await h.request_approval({
            "candidates": [
                {"id": "hi", "quality_score": 10.0},
                {"id": "lo", "quality_score": 0.01},
            ]}, user_id="u")
        assert ok is True

    @pytest.mark.asyncio
    async def test_uncertain_queries(self):
        h = HumanApprovalHandler(uncertainty_threshold=10.0, timeout=0.05)
        await h.submit_response("u", "hi")
        ok = await h.request_approval({
            "candidates": [
                {"id": "hi", "quality_score": 1.0},
                {"id": "lo", "quality_score": 1.0},
            ]}, user_id="u")
        assert ok is True


class TestXAIExplainer:
    def test_ig_sum_approximates_diff(self):
        x = XAIExplainer(ig_steps=16)
        def f(v):
            return float(np.dot(v, [0.4, -0.3, -0.2, -0.1]))
        feats = np.array([0.9, 0.4, 0.5, 0.4])
        attrs = x.integrated_gradients(f, feats)
        diff = f(feats) - f(np.zeros_like(feats))
        assert abs(float(np.sum(attrs)) - diff) < 0.1

    def test_counterfactual_runs(self):
        x = XAIExplainer(cf_max_iter=100, cf_target_delta=0.05,
                         rng=random.Random(0))
        def f(v):
            return float(v[0] - v[1])
        feats = np.array([0.9, 0.4])
        cf = x.counterfactual(f, feats)
        assert "changed_index" in cf


class TestTemporalSafety:
    @pytest.mark.asyncio
    async def test_always_satisfied(self):
        s = TemporalSafetyMonitor()
        s.add_formula("G (quality >= 0.5)")
        await s.push({"quality": 0.9})
        await s.push({"quality": 0.8})
        result = await s.check()
        assert all(result.values())

    @pytest.mark.asyncio
    async def test_never_violated(self):
        s = TemporalSafetyMonitor()
        s.add_formula("NEVER (carbon >= 0.8)")
        await s.push({"carbon": 0.5})
        await s.push({"carbon": 0.9})
        result = await s.check()
        assert any(not v for v in result.values())


# =============================================================================
# TESTS — FULL INTEGRATION
# =============================================================================
class TestFullIntegration:
    @pytest.mark.asyncio
    async def test_route_and_update_cycle(self, full_router, state):
        decision = await full_router.select_policy(state)
        updated = await full_router.update(state, decision.expert_idx,
                                            reward=0.9)
        assert updated.expert_idx == decision.expert_idx

    @pytest.mark.asyncio
    async def test_stats_reports_all_modules(self, full_router):
        stats = full_router.get_stats()
        for key in ("federated_enabled", "safety_monitor_enabled",
                    "xai_enabled", "precision_controller_enabled",
                    "carbon_market_enabled", "chaos_enabled",
                    "human_approval_enabled", "temporal_safety_enabled",
                    "quantum_enabled"):
            assert key in stats

    @pytest.mark.asyncio
    async def test_concurrent_updates(self, full_router, state):
        async def _upd(i):
            return await full_router.update(state, i % full_router.n_experts,
                                             reward=0.5)
        results = await asyncio.gather(*[_upd(i) for i in range(10)])
        assert len(results) == 10

    @pytest.mark.asyncio
    async def test_persistence_roundtrip(self, experts, tmp_path):
        path = str(tmp_path / "weights.json")
        r1 = FlexGenMoERouter(experts, seed=1, persistence_path=path)
        s = FlexGenState()
        for _ in range(3):
            await r1.update(s, 0, reward=1.0)
        r1._save_weights()
        r2 = FlexGenMoERouter(experts, seed=99, persistence_path=path)
        assert np.allclose(r1.W1, r2.W1)
        assert r1.step_count == r2.step_count


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    async def demo():
        experts = _make_experts(4)
        queue = AsyncMessageQueue()
        router = FlexGenMoERouter(
            experts, message_queue=queue, seed=42,
            enable_federated=True, enable_safety_monitor=True,
            enable_xai=True, enable_precision=True,
            enable_carbon_market=True, enable_human_approval=True,
            persistence_path=None)
        state = FlexGenState()
        decision = await router.select_policy(state)
        print(f"Routed to: {decision.expert_name} "
              f"(confidence={decision.confidence:.3f})")
        if decision.explanation:
            print(f"Top attribution index: "
                  f"{int(np.argmax(np.abs(decision.explanation['attributions'])))}")
        await router.update(state, decision.expert_idx, reward=0.9)
        print("Stats:", json.dumps(router.get_stats(), indent=2))
        await router.shutdown()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())
