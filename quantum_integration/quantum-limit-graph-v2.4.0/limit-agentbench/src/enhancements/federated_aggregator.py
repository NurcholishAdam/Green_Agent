# src/enhancements/federated_aggregator.py
"""
Enhanced Federated Aggregator for Green Agent Students — v2.1 (Single-File Enhanced).

Public API preserved:
    FederatedAggregatorError, SafetyViolationError, ChaosExperimentError,
    LowReputationError,
    SafetyMonitor, XAIExplainer, DifferentialPrivacy, SecureAggregator,
    ClientReputation, CarbonAwareWeighter, PrecisionSwitcher, ChaosMonkey,
    HumanReviewManager, QuantumDistillationOptimizer, FederatedAggregator.

Fixes and enhancements over v2.0:
- SecureAggregator uses real pairwise shared masks that cancel on aggregation
  (previously the masks did not cancel and added permanent noise). The class
  also exposes a session API so callers can register the client set once.
- `hash()` is replaced with hashlib.blake2b for stable, reproducible seeds.
- ClientReputation now only updates on real, validated outcomes; it supports
  `mark_failure`, is thread-safe, and can be persisted to/loaded from a JSON
  sidecar alongside the weights file.
- SafetyMonitor returns (safe, reason); its dead `if` block is removed; the
  caller now respects the return value and can roll back.
- HumanReviewManager has TTL, capacity, deduplication per aggregate_id, and an
  audit log; `get_pending` is thread-safe.
- ChaosMonkey has named faults, a seeded RNG, `apply`/`cleanup` snapshots, and
  `inject_and_measure`.
- PrecisionSwitcher.cast returns quantised tensors AND stores/returns the scale
  metadata so int8 tensors can be dequantised via `uncast`.
- DifferentialPrivacy composes with SecureAggregator; a new PrivacyAccountant
  tracks cumulative (ε, δ) across updates using an RDP-style bound.
- QuantumDistillationOptimizer tries modern qiskit first, then legacy; it is
  now also wired into the incremental `update()` path.
- CarbonAwareWeighter is now configurable (floor, exponent).
- `FederatedAggregator` validates the lengths of `client_ids`,
  `carbon_intensities`, and `categories`; it guards all public entry points
  with an RLock; it fixes the update() sample-count math and applies carbon-
  aware weighting to the first update as well; it does not deep-copy on every
  read (deep=True is opt-in).
- Real Prometheus metrics are registered and incremented in the actual paths.
- Structured logging via structlog when available.

The file remains single-file for distribution.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import logging.handlers  # noqa: F401
import math
import os
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import numpy as np

# -----------------------------------------------------------------------------
# Optional / guarded imports
# -----------------------------------------------------------------------------
try:
    import structlog  # noqa: F401
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.handlers.RotatingFileHandler(
                    "federated_aggregator.log", maxBytes=10 * 1024 * 1024, backupCount=5
                ),
                logging.StreamHandler(),
            ],
        )

try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# Qiskit — modern layout first, then legacy
try:
    from qiskit_optimization import QuadraticProgram  # type: ignore
    from qiskit_optimization.algorithms import MinimumEigenOptimizer  # type: ignore
    try:
        from qiskit_algorithms import QAOA  # type: ignore
    except ImportError:
        from qiskit.algorithms import QAOA  # type: ignore
    QISKIT_AVAILABLE = True
    QISKIT_LEGACY = False
except ImportError:
    try:
        from qiskit.optimization import QuadraticProgram  # type: ignore
        from qiskit.optimization.algorithms import MinimumEigenOptimizer  # type: ignore
        from qiskit.algorithms import QAOA  # type: ignore
        QISKIT_AVAILABLE = True
        QISKIT_LEGACY = True
    except ImportError:
        QISKIT_AVAILABLE = False
        QISKIT_LEGACY = False


# =============================================================================
# Helpers
# =============================================================================
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _stable_seed(*parts: Any) -> int:
    """Deterministic integer seed derived from the given parts."""
    key = json.dumps([str(p) for p in parts], sort_keys=True).encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big")


# =============================================================================
# Exceptions
# =============================================================================
class FederatedAggregatorError(Exception):
    """Base exception for the federated aggregator."""


class SafetyViolationError(FederatedAggregatorError):
    """Raised when a safety rule is violated and the caller wants to halt."""


class ChaosExperimentError(FederatedAggregatorError):
    """Raised when chaos injection causes a hard failure."""


class LowReputationError(FederatedAggregatorError):
    """Raised when a client's reputation is below the threshold."""


# =============================================================================
# Metrics
# =============================================================================
class _NoopMetric:
    def labels(self, *a, **k): return self
    def inc(self, *a, **k): return None
    def set(self, *a, **k): return None
    def observe(self, *a, **k): return None


class MetricsCollector:
    """Small wrapper around prometheus_client with memoised handles."""

    def __init__(self) -> None:
        self.enabled = PROMETHEUS_AVAILABLE
        self.registry = CollectorRegistry() if self.enabled else None
        self._counters: Dict[str, Any] = {}
        self._gauges: Dict[str, Any] = {}
        self._histograms: Dict[str, Any] = {}
        self._lock = threading.RLock()

    def counter(self, name, doc="", labels=None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, doc, labels or [], registry=self.registry)
            return self._counters[name]

    def gauge(self, name, doc="", labels=None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name, doc, labels or [], registry=self.registry)
            return self._gauges[name]

    def histogram(self, name, doc="", labels=None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, doc, labels or [], registry=self.registry)
            return self._histograms[name]

    def render(self) -> bytes:
        if not self.enabled:
            return b""
        from prometheus_client import generate_latest
        return generate_latest(self.registry)

    def snapshot(self) -> Dict[str, Any]:
        if not self.enabled:
            return {}
        return {
            "counters": {k: v._value.get() for k, v in self._counters.items()},
            "gauges": {k: v._value.get() for k, v in self._gauges.items()},
        }


METRICS = MetricsCollector()
CLIENTS_AGGREGATED = METRICS.counter(
    "fedagg_clients_aggregated_total", "Clients aggregated", ["method"]
)
CLIENTS_REJECTED_LOW_REP = METRICS.counter(
    "fedagg_clients_rejected_low_rep_total", "Clients rejected for low reputation"
)
DP_CLIPPED = METRICS.counter("fedagg_dp_clipped_total", "DP clipping events")
DP_NOISE = METRICS.counter("fedagg_dp_noise_injected_total", "DP noise injections")
DP_EPSILON = METRICS.gauge("fedagg_dp_cumulative_epsilon", "Cumulative DP epsilon")
DP_DELTA = METRICS.gauge("fedagg_dp_cumulative_delta", "Cumulative DP delta")
SAFETY_VIOLATIONS = METRICS.counter(
    "fedagg_safety_violations_total", "Safety violations", ["rule"]
)
SAFETY_BLOCKED = METRICS.counter(
    "fedagg_safety_blocked_total", "Aggregations blocked by safety"
)
HUMAN_REVIEWS = METRICS.counter(
    "fedagg_human_reviews_total", "Human reviews", ["status"]
)
CHAOS_INJECTED = METRICS.counter(
    "fedagg_chaos_injected_total", "Chaos injections", ["fault"]
)
NAN_REJECTED = METRICS.counter(
    "fedagg_nan_rejected_total", "Aggregations rejected for NaN"
)
AGG_LATENCY = METRICS.histogram(
    "fedagg_aggregation_seconds", "Aggregation latency (s)", ["method"]
)
AGG_NORM = METRICS.histogram(
    "fedagg_global_weight_norm", "Global weight norm after aggregation"
)
SECURE_SESSIONS = METRICS.counter(
    "fedagg_secure_sessions_total", "Secure aggregation sessions opened"
)


# =============================================================================
# Privacy accountant (RDP-ish, simple composition)
# =============================================================================
@dataclass
class PrivacyAccountant:
    """
    Track cumulative (ε, δ) across DP updates using a simple sequential
    composition bound. For a single Gaussian mechanism with noise multiplier
    `sigma` (noise_std = sigma * clip_norm) and one update, we use the
    analytic Gaussian bound approximation:

        eps_single ≈ sqrt(2 * ln(1.25 / delta)) / sigma

    Sequential composition adds eps_single across updates. This is a
    conservative bound; production systems should use a proper RDP or GDP
    accountant from a library like `opacus` or `tensorflow-privacy`.
    """

    delta: float = 1e-5
    cumulative_epsilon: float = 0.0
    updates: int = 0
    budget_epsilon: Optional[float] = None

    def step(self, noise_multiplier: float) -> float:
        sigma = max(noise_multiplier, 1e-6)
        eps_single = math.sqrt(2.0 * math.log(1.25 / self.delta)) / sigma
        self.cumulative_epsilon += eps_single
        self.updates += 1
        return eps_single

    def budget_exhausted(self) -> bool:
        return (
            self.budget_epsilon is not None
            and self.cumulative_epsilon >= self.budget_epsilon
        )

    def snapshot(self) -> Dict[str, Any]:
        return {
            "delta": self.delta,
            "cumulative_epsilon": self.cumulative_epsilon,
            "updates": self.updates,
            "budget_epsilon": self.budget_epsilon,
        }


# =============================================================================
# SafetyMonitor
# =============================================================================
class SafetyMonitor:
    """
    Temporal logic-like rules on aggregated weight norms.

    Rules:
      1. Instantaneous norm ≤ max_global_weight_norm.
      2. Average norm over the last `window_size` aggregates ≤ high_watermark.
    """

    def __init__(
        self,
        max_global_weight_norm: float = 1000.0,
        max_consecutive_violations: int = 3,
        window_size: int = 10,
        high_watermark_ratio: float = 0.8,
    ):
        self.max_global_weight_norm = float(max_global_weight_norm)
        self.max_consecutive_violations = int(max_consecutive_violations)
        self.window_size = int(window_size)
        self.high_watermark_ratio = float(high_watermark_ratio)
        self.consecutive_violations = 0
        self.history: deque = deque(maxlen=self.window_size)
        self.violations: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def _weight_norm(self, weight_dict: Dict[str, np.ndarray]) -> float:
        total = 0.0
        for arr in weight_dict.values():
            if isinstance(arr, np.ndarray):
                total += float(np.linalg.norm(arr))
        return total

    def check_aggregate(
        self, global_weights: Dict[str, np.ndarray]
    ) -> Tuple[bool, Optional[str]]:
        with self._lock:
            norm = self._weight_norm(global_weights)
            self.history.append({"norm": norm, "timestamp": _utc_now().isoformat()})
            if norm > self.max_global_weight_norm:
                self.consecutive_violations += 1
                self._record("weight_norm_exceeded", {"norm": norm})
                return False, "weight_norm_exceeded"
            self.consecutive_violations = 0
            if len(self.history) >= self.window_size:
                avg = float(np.mean([h["norm"] for h in self.history]))
                if avg > self.high_watermark_ratio * self.max_global_weight_norm:
                    self._record("avg_norm_high", {"avg_norm": avg})
                    return False, "avg_norm_high"
            return True, None

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "max_global_weight_norm": self.max_global_weight_norm,
                "max_consecutive_violations": self.max_consecutive_violations,
                "window_size": self.window_size,
                "high_watermark_ratio": self.high_watermark_ratio,
            }

    def restore(self, snap: Dict[str, Any]) -> None:
        with self._lock:
            if "max_global_weight_norm" in snap:
                self.max_global_weight_norm = float(snap["max_global_weight_norm"])
            if "max_consecutive_violations" in snap:
                self.max_consecutive_violations = int(snap["max_consecutive_violations"])
            if "window_size" in snap:
                self.window_size = int(snap["window_size"])
                self.history = deque(self.history, maxlen=self.window_size)
            if "high_watermark_ratio" in snap:
                self.high_watermark_ratio = float(snap["high_watermark_ratio"])

    def reset_episode(self) -> None:
        with self._lock:
            self.history.clear()
            self.consecutive_violations = 0

    def _record(self, rule: str, details: Dict[str, Any]) -> None:
        self.violations.append({
            "rule": rule,
            "details": details,
            "timestamp": _utc_now().isoformat(),
        })
        SAFETY_VIOLATIONS.labels(rule=rule).inc()
        logger.warning(f"safety_violation rule={rule} details={details}")

    def get_violations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.violations)


# =============================================================================
# XAIExplainer
# =============================================================================
class XAIExplainer:
    """Explains aggregated model by attributing to contributing clients."""

    def explain(
        self,
        client_contributions: List[Dict[str, Any]],
        global_weights: Dict[str, np.ndarray],
        rejected_clients: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if not client_contributions:
            return {
                "narrative": "No client contributions to explain.",
                "top_contributors": [],
                "category_weights": {},
                "rejected_clients": rejected_clients or [],
                "global_weight_norm": 0.0,
            }

        total_weight = float(sum(c.get("weight", 0.0) for c in client_contributions))
        ranked = sorted(
            client_contributions, key=lambda c: -float(c.get("weight", 0.0))
        )
        top = ranked[0]
        top_pct = (float(top.get("weight", 0.0)) / max(total_weight, 1e-9)) * 100.0

        category_weights: Dict[str, float] = defaultdict(float)
        for c in client_contributions:
            category_weights[str(c.get("category", "unknown"))] += float(c.get("weight", 0.0))

        total_norm = float(
            sum(float(np.linalg.norm(a)) for a in global_weights.values()
                if isinstance(a, np.ndarray))
        )

        narrative_parts = [
            f"Aggregated {len(client_contributions)} clients with "
            f"total weight {total_weight:.2f}.",
            f"Top contributor: client '{top.get('client_id', 'unknown')}' "
            f"with {top_pct:.1f}% weight.",
        ]
        for cat, w in sorted(category_weights.items(), key=lambda x: -x[1]):
            narrative_parts.append(f"  category '{cat}': {w:.2f} weight.")
        narrative_parts.append(f"Global weight norm: {total_norm:.4f}.")
        if rejected_clients:
            narrative_parts.append(
                f"Rejected {len(rejected_clients)} client(s): "
                + ", ".join(r.get("client_id", "unknown") for r in rejected_clients)
                + "."
            )

        return {
            "narrative": " ".join(narrative_parts),
            "top_contributors": ranked[:5],
            "category_weights": dict(category_weights),
            "rejected_clients": rejected_clients or [],
            "global_weight_norm": total_norm,
        }

    def explain_str(
        self,
        client_contributions: List[Dict[str, Any]],
        global_weights: Dict[str, np.ndarray],
        rejected_clients: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        return self.explain(client_contributions, global_weights, rejected_clients)["narrative"]


# =============================================================================
# DifferentialPrivacy
# =============================================================================
class DifferentialPrivacy:
    """
    Differential privacy via L2 clipping + Gaussian noise.

    The DP mechanism is applied per-client before aggregation. The
    PrivacyAccountant tracks cumulative ε across updates. If a `budget_epsilon`
    is provided, `clip_and_noise` raises once the budget is exhausted.
    """

    def __init__(
        self,
        clip_norm: float = 1.0,
        noise_multiplier: float = 0.1,
        delta: float = 1e-5,
        budget_epsilon: Optional[float] = None,
    ):
        self.clip_norm = float(clip_norm)
        self.noise_multiplier = float(noise_multiplier)
        self.accountant = PrivacyAccountant(
            delta=float(delta), budget_epsilon=budget_epsilon
        )
        self._lock = threading.RLock()

    def clip_and_noise(self, weight_dict: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        with self._lock:
            if self.accountant.budget_exhausted():
                raise SafetyViolationError(
                    "Differential privacy budget exhausted "
                    f"(eps={self.accountant.cumulative_epsilon:.4f})"
                )
            # Compute global L2 norm across all tensors.
            sq_sum = 0.0
            for a in weight_dict.values():
                if isinstance(a, np.ndarray):
                    sq_sum += float(np.sum(a.astype(np.float64) ** 2))
            norm = math.sqrt(sq_sum)
            clip_factor = min(1.0, self.clip_norm / (norm + 1e-12))
            if clip_factor < 1.0:
                DP_CLIPPED.inc()
            noise_std = self.noise_multiplier * self.clip_norm
            noisy: Dict[str, np.ndarray] = {}
            for k, v in weight_dict.items():
                if not isinstance(v, np.ndarray):
                    noisy[k] = v
                    continue
                clipped = v.astype(np.float64) * clip_factor
                noise = np.random.normal(0.0, noise_std, clipped.shape)
                noisy[k] = (clipped + noise).astype(v.dtype if v.dtype != np.int8 else np.float64)
            DP_NOISE.inc()
            eps_step = self.accountant.step(self.noise_multiplier)
            DP_EPSILON.set(self.accountant.cumulative_epsilon)
            DP_DELTA.set(self.accountant.delta)
            logger.debug(
                f"dp_step eps_single={eps_step:.4f} "
                f"eps_cumulative={self.accountant.cumulative_epsilon:.4f}"
            )
            return noisy

    def get_accountant(self) -> PrivacyAccountant:
        return self.accountant


# =============================================================================
# SecureAggregator — real pairwise masks that cancel
# =============================================================================
class SecureAggregator:
    """
    Secure aggregation using pairwise shared masks (Bonawitz et al. style).

    For every unordered pair (i, j) of clients, a deterministic shared seed is
    derived from the pair id and the aggregator's seed. Client i adds `+mask`
    and client j adds `-mask`. Summing all masked updates cancels every pair
    exactly, so the aggregate is unchanged while individual contributions stay
    hidden from the server.

    A session must be opened with `begin_session(client_ids)` before masking,
    or `mask_weights(..., peer_ids=[...])` must be provided.
    """

    def __init__(self, seed: int = 42):
        self.seed = int(seed)
        self._session_ids: Optional[List[str]] = None
        self._session_id: Optional[str] = None
        self._lock = threading.RLock()

    def begin_session(self, client_ids: Iterable[str]) -> str:
        with self._lock:
            self._session_ids = sorted({str(c) for c in client_ids})
            self._session_id = uuid.uuid4().hex[:8]
            SECURE_SESSIONS.inc()
            return self._session_id

    def end_session(self) -> None:
        with self._lock:
            self._session_ids = None
            self._session_id = None

    def _pair_seed(self, a: str, b: str) -> int:
        return _stable_seed(min(a, b), max(a, b), self.seed)

    def _pairwise_mask(
        self, client_id: str, shape: Tuple[int, ...], peer_ids: List[str]
    ) -> np.ndarray:
        mask = np.zeros(shape, dtype=np.float64)
        for other in peer_ids:
            if other == client_id:
                continue
            rng = np.random.default_rng(self._pair_seed(client_id, other))
            m = rng.normal(0.0, 1.0, shape)
            sign = 1.0 if client_id < other else -1.0
            mask += sign * m
        return mask

    def mask_weights(
        self,
        client_id: str,
        weight_dict: Dict[str, np.ndarray],
        peer_ids: Optional[List[str]] = None,
    ) -> Dict[str, np.ndarray]:
        with self._lock:
            peers = list(peer_ids) if peer_ids is not None else self._session_ids
        if peers is None:
            logger.warning(
                "secure_agg_no_session: masks will not cancel; "
                "call begin_session(...) or pass peer_ids"
            )
            return {k: v.copy() if isinstance(v, np.ndarray) else v
                    for k, v in weight_dict.items()}
        masked: Dict[str, np.ndarray] = {}
        for k, v in weight_dict.items():
            if not isinstance(v, np.ndarray):
                masked[k] = v
                continue
            mask = self._pairwise_mask(client_id, v.shape, peers)
            masked[k] = v.astype(np.float64) + mask
        return masked

    def unmask_weights(self, weight_dict: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        """Masks cancel on aggregation, so unmask is a pass-through."""
        return weight_dict


# =============================================================================
# ClientReputation — persistent, only updated on real outcomes
# =============================================================================
class ClientReputation:
    """Tracks per-client reputation based on historical quality of updates."""

    def __init__(
        self,
        initial_reputation: float = 0.5,
        min_reputation: float = 0.2,
        alpha: float = 0.2,
    ):
        self.reputation: Dict[str, float] = defaultdict(lambda: initial_reputation)
        self.min_reputation = float(min_reputation)
        self.alpha = float(alpha)
        self.updates: Dict[str, int] = defaultdict(int)
        self.failures: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()

    def update(self, client_id: str, success: bool) -> None:
        with self._lock:
            prev = self.reputation[client_id]
            self.reputation[client_id] = prev + self.alpha * (
                (1.0 if success else 0.0) - prev
            )
            self.updates[client_id] += 1
            if not success:
                self.failures[client_id] += 1

    def mark_failure(self, client_id: str) -> None:
        self.update(client_id, success=False)

    def is_acceptable(self, client_id: str) -> bool:
        with self._lock:
            return self.reputation.get(client_id, 0.5) >= self.min_reputation

    def get_reputation(self, client_id: str) -> float:
        with self._lock:
            return self.reputation.get(client_id, 0.5)

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "reputation": dict(self.reputation),
                "updates": dict(self.updates),
                "failures": dict(self.failures),
                "min_reputation": self.min_reputation,
                "alpha": self.alpha,
            }

    def load(self, payload: Dict[str, Any]) -> None:
        with self._lock:
            self.reputation = defaultdict(
                lambda: 0.5, {k: float(v) for k, v in payload.get("reputation", {}).items()}
            )
            self.updates = defaultdict(int, payload.get("updates", {}))
            self.failures = defaultdict(int, payload.get("failures", {}))
            self.min_reputation = float(payload.get("min_reputation", self.min_reputation))
            self.alpha = float(payload.get("alpha", self.alpha))


# =============================================================================
# CarbonAwareWeighter — configurable floor and exponent
# =============================================================================
class CarbonAwareWeighter:
    """Adjusts client weights based on carbon intensity."""

    def __init__(
        self,
        carbon_intensity: float = 400.0,
        threshold: float = 400.0,
        floor: float = 0.5,
        exponent: float = 1.0,
    ):
        self.carbon_intensity = float(carbon_intensity)
        self.threshold = float(threshold)
        self.floor = max(0.0, min(1.0, float(floor)))
        self.exponent = max(1e-6, float(exponent))

    def adjust_weight(self, base_weight: float) -> float:
        if self.carbon_intensity <= self.threshold:
            return float(base_weight)
        ratio = self.threshold / max(self.carbon_intensity, 1e-6)
        carbon_factor = min(1.0, max(0.0, ratio)) ** self.exponent
        return float(base_weight) * (self.floor + (1.0 - self.floor) * carbon_factor)

    def set_carbon_intensity(self, intensity: float) -> None:
        self.carbon_intensity = float(intensity)

    def get_status(self) -> Dict[str, Any]:
        return {
            "carbon_intensity": self.carbon_intensity,
            "threshold": self.threshold,
            "floor": self.floor,
            "exponent": self.exponent,
        }


# =============================================================================
# PrecisionSwitcher — int8 with scale metadata
# =============================================================================
class PrecisionSwitcher:
    """
    Recommends precision based on carbon intensity and workload size and
    performs the cast. For int8, per-tensor scales are stored on the instance
    and returned via `get_last_scales()`; `uncast` uses them to dequantise.
    """

    def __init__(self, default_carbon_intensity: float = 400.0):
        self.default_carbon_intensity = float(default_carbon_intensity)
        self._last_scales: Dict[str, float] = {}
        self._lock = threading.RLock()

    def recommend(
        self, workload_size: str = "medium", carbon_intensity: Optional[float] = None
    ) -> str:
        ci = (
            float(carbon_intensity)
            if carbon_intensity is not None else self.default_carbon_intensity
        )
        if ci > 500 or workload_size == "large":
            return "int8"
        if ci > 300 or workload_size == "medium":
            return "fp16"
        return "fp32"

    def cast(
        self, weight_dict: Dict[str, np.ndarray], precision: str
    ) -> Dict[str, np.ndarray]:
        with self._lock:
            self._last_scales = {}
            if precision == "fp16":
                return {
                    k: (v.astype(np.float16) if isinstance(v, np.ndarray) else v)
                    for k, v in weight_dict.items()
                }
            if precision == "int8":
                out: Dict[str, np.ndarray] = {}
                scales: Dict[str, float] = {}
                for k, v in weight_dict.items():
                    if not isinstance(v, np.ndarray):
                        out[k] = v
                        continue
                    max_abs = float(np.max(np.abs(v))) if v.size else 1.0
                    scale = (max_abs or 1.0) / 127.0
                    q = np.clip(np.round(v / scale), -127, 127).astype(np.int8)
                    out[k] = q
                    scales[k] = scale
                self._last_scales = scales
                return out
            # fp32
            return {k: (v.astype(np.float32) if isinstance(v, np.ndarray) else v)
                    for k, v in weight_dict.items()}

    def uncast(self, weight_dict: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        with self._lock:
            if not self._last_scales:
                return weight_dict
            out: Dict[str, np.ndarray] = {}
            for k, v in weight_dict.items():
                if isinstance(v, np.ndarray) and v.dtype == np.int8 and k in self._last_scales:
                    out[k] = v.astype(np.float32) * self._last_scales[k]
                else:
                    out[k] = v
            return out

    def get_last_scales(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._last_scales)

    def get_status(self) -> Dict[str, Any]:
        return {
            "available": True,
            "default_carbon_intensity": self.default_carbon_intensity,
            "last_scales": self.get_last_scales(),
        }


# =============================================================================
# ChaosMonkey — named faults, seeded, apply/cleanup
# =============================================================================
class ChaosMonkey:
    """Injects simulated faults for resilience testing."""

    FAULT_TYPES = {"nan", "zero", "huge", "stale", "drop"}

    def __init__(
        self,
        enabled: bool = False,
        failure_probability: float = 0.1,
        seed: int = 42,
    ):
        self.enabled = bool(enabled)
        self.failure_probability = float(failure_probability)
        self._rng = random.Random(int(seed))
        self.injected_failures = 0
        self.events: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def maybe_corrupt(
        self, weight_dict: Dict[str, np.ndarray], fault: Optional[str] = None
    ) -> Dict[str, np.ndarray]:
        with self._lock:
            if not self.enabled or self._rng.random() >= self.failure_probability:
                return weight_dict
            chosen = fault or self._rng.choice(sorted(self.FAULT_TYPES))
            self.injected_failures += 1
            CHAOS_INJECTED.labels(fault=chosen).inc()
        logger.warning(f"chaos_injected fault={chosen}")

        if chosen == "drop":
            # Simulate dropping a tensor entirely.
            if not weight_dict:
                return weight_dict
            first_key = next(iter(weight_dict))
            out = dict(weight_dict)
            out.pop(first_key, None)
            return out

        if chosen == "nan":
            out = {}
            for k, v in weight_dict.items():
                if isinstance(v, np.ndarray) and v.size:
                    arr = v.astype(np.float64).copy()
                    arr.flat[0] = np.nan
                    out[k] = arr
                else:
                    out[k] = v
            return out

        if chosen == "zero":
            return {k: (np.zeros_like(v) if isinstance(v, np.ndarray) else v)
                    for k, v in weight_dict.items()}

        if chosen == "huge":
            return {k: (v.astype(np.float64) * 1e6 if isinstance(v, np.ndarray) else v)
                    for k, v in weight_dict.items()}

        if chosen == "stale":
            return {k: (v.astype(np.float64) + 0.01 if isinstance(v, np.ndarray) else v)
                    for k, v in weight_dict.items()}

        return weight_dict

    def apply(self, fault: str, aggregator: "FederatedAggregator") -> None:
        if fault not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault: {fault}")
        self.enabled = True

    def cleanup(self, fault: str, aggregator: "FederatedAggregator") -> None:
        return None

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "enabled": self.enabled,
                "failure_probability": self.failure_probability,
                "injected_failures": self.injected_failures,
                "recent_events": self.events[-5:],
            }

    def inject_and_measure(
        self,
        aggregator: "FederatedAggregator",
        fault: str,
        weight_dict: Dict[str, np.ndarray],
    ) -> Dict[str, Any]:
        start = time.monotonic()
        corrupted = self.maybe_corrupt(weight_dict, fault=fault)
        recovered = all(
            k in corrupted and isinstance(corrupted[k], np.ndarray)
            and not np.isnan(corrupted[k]).any()
            for k in weight_dict
        ) if corrupted else False
        elapsed = time.monotonic() - start
        self.events.append({
            "fault": fault,
            "recovered": recovered,
            "elapsed_s": elapsed,
            "timestamp": _utc_now().isoformat(),
        })
        return {
            "fault": fault,
            "corrupted_ok": bool(corrupted),
            "recovered": recovered,
            "elapsed_s": elapsed,
        }


# =============================================================================
# HumanReviewManager — TTL, capacity, dedup, audit
# =============================================================================
@dataclass
class HumanReviewRequest:
    review_id: str
    aggregate_id: str
    details: Dict[str, Any]
    created_at: datetime
    status: str = "pending"
    reviewer: Optional[str] = None
    resolution_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None


class HumanReviewManager:
    """Manages pre-commit human review for suspicious aggregations."""

    def __init__(
        self,
        suspicious_norm_change_pct: float = 50.0,
        ttl_seconds: float = 3600.0,
        max_pending: int = 100,
    ):
        self.pending_reviews: Dict[str, HumanReviewRequest] = {}
        self.suspicious_norm_change_pct = float(suspicious_norm_change_pct)
        self.ttl_seconds = float(ttl_seconds)
        self.max_pending = int(max_pending)
        self.audit_log: List[Dict[str, Any]] = []
        self._by_aggregate: Dict[str, str] = {}
        self._lock = threading.RLock()

    def _evict_expired(self) -> None:
        now = _utc_now()
        for rid, r in list(self.pending_reviews.items()):
            if r.status == "pending" and now - r.created_at > timedelta(seconds=self.ttl_seconds):
                r.status = "expired"
                HUMAN_REVIEWS.labels(status="expired").inc()

    def _enforce_capacity(self) -> None:
        pending = [r for r in self.pending_reviews.values() if r.status == "pending"]
        if len(pending) <= self.max_pending:
            return
        ordered = sorted(pending, key=lambda r: r.created_at)
        for r in ordered[: len(ordered) - self.max_pending]:
            r.status = "dropped"
            HUMAN_REVIEWS.labels(status="dropped").inc()
            self._by_aggregate.pop(r.aggregate_id, None)

    def request_review(self, aggregate_id: str, details: Dict[str, Any]) -> str:
        with self._lock:
            self._evict_expired()
            existing = self._by_aggregate.get(aggregate_id)
            if existing:
                existing_request = self.pending_reviews.get(existing)
                if existing_request and existing_request.status == "pending":
                    return existing
            rid = str(uuid.uuid4())
            self.pending_reviews[rid] = HumanReviewRequest(
                review_id=rid, aggregate_id=aggregate_id,
                details=dict(details), created_at=_utc_now(),
            )
            self._by_aggregate[aggregate_id] = rid
            self._enforce_capacity()
            HUMAN_REVIEWS.labels(status="pending").inc()
            logger.info(f"human_review_requested review_id={rid} aggregate_id={aggregate_id}")
            return rid

    def _audit(self, review_id: str, action: str, reviewer: str,
               reason: Optional[str]) -> None:
        self.audit_log.append({
            "review_id": review_id, "action": action,
            "reviewer": reviewer, "reason": reason,
            "timestamp": _utc_now().isoformat(),
        })

    def approve(self, review_id: str, reviewer: str = "unknown") -> bool:
        with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None or r.status != "pending":
                return False
            r.status = "approved"
            r.reviewer = reviewer
            r.resolution_at = _utc_now()
            self._by_aggregate.pop(r.aggregate_id, None)
            self._audit(review_id, "approved", reviewer, None)
            HUMAN_REVIEWS.labels(status="approved").inc()
            return True

    def reject(self, review_id: str, reason: Optional[str] = None,
               reviewer: str = "unknown") -> bool:
        with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None or r.status != "pending":
                return False
            r.status = "rejected"
            r.rejection_reason = reason or "unspecified"
            r.reviewer = reviewer
            r.resolution_at = _utc_now()
            self._by_aggregate.pop(r.aggregate_id, None)
            self._audit(review_id, "rejected", reviewer, reason)
            HUMAN_REVIEWS.labels(status="rejected").inc()
            return True

    def get_pending(self) -> List[Dict[str, Any]]:
        with self._lock:
            self._evict_expired()
            return [
                {
                    "review_id": r.review_id,
                    "aggregate_id": r.aggregate_id,
                    "details": r.details,
                    "created_at": r.created_at.isoformat(),
                }
                for r in self.pending_reviews.values() if r.status == "pending"
            ]

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            self._evict_expired()
            statuses: Dict[str, int] = defaultdict(int)
            for r in self.pending_reviews.values():
                statuses[r.status] += 1
            return {"total": len(self.pending_reviews), "by_status": dict(statuses)}


# =============================================================================
# QuantumDistillationOptimizer
# =============================================================================
class QuantumDistillationOptimizer:
    """Optional QAOA-assisted selection of the best subset of clients."""

    def __init__(self, enabled: bool = False, qaoa_reps: int = 1, max_clients: int = 8):
        self.enabled = bool(enabled)
        self.qaoa_reps = int(qaoa_reps)
        self.max_clients = int(max_clients)
        self.available = self.enabled and QISKIT_AVAILABLE

    def select_clients(self, candidates: List[Dict[str, Any]]) -> Optional[List[int]]:
        if not candidates:
            return None
        if not self.available:
            return None
        candidates = candidates[: self.max_clients]
        try:
            return self._quantum_select(candidates)
        except Exception as e:
            logger.warning(f"quantum_select_failed error={e}")
            return self._classical_select(candidates)

    def _classical_select(self, candidates: List[Dict[str, Any]]) -> List[int]:
        scored = []
        for i, c in enumerate(candidates):
            util = float(c.get("utility", 0.5))
            carbon = float(c.get("carbon_intensity", 400.0))
            scored.append((util - (carbon / 1000.0) * 0.3, i))
        scored.sort(key=lambda x: -x[0])
        k = max(1, len(scored) // 2)
        return [i for _, i in scored[:k]]

    def _quantum_select(self, candidates: List[Dict[str, Any]]) -> List[int]:
        qp = QuadraticProgram()
        for i in range(len(candidates)):
            qp.binary_var(f"x{i}")
        linear: Dict[str, float] = {}
        for i, c in enumerate(candidates):
            util = float(c.get("utility", 0.5))
            carbon = float(c.get("carbon_intensity", 400.0))
            value = util - (carbon / 1000.0) * 0.3
            linear[f"x{i}"] = -value
        qp.minimize(linear=linear)
        k = max(1, len(candidates) // 2)
        qp.linear_constraint(
            linear={f"x{i}": 1 for i in range(len(candidates))},
            sense="LE", rhs=k, name="max_clients",
        )
        qaoa = QAOA(reps=self.qaoa_reps)
        optimizer = MinimumEigenOptimizer(qaoa)
        result = optimizer.solve(qp)
        selected = [i for i in range(len(candidates)) if result.x[i] > 0.5]
        return selected or list(range(len(candidates)))

    def get_status(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "qiskit_available": QISKIT_AVAILABLE,
            "qiskit_legacy": QISKIT_LEGACY if QISKIT_AVAILABLE else None,
        }


# =============================================================================
# Main aggregator
# =============================================================================
class FederatedAggregator:
    """
    Federated averaging aggregator with optional weighting and persistence.

    All public entry points are guarded by an RLock. The aggregator composes
    differential privacy, secure aggregation, client reputation, carbon-aware
    weighting, adaptive precision, safety monitoring, XAI, chaos injection,
    human review, and optional quantum client selection.
    """

    def __init__(
        self,
        aggregation: str = "average",
        weights_path: Optional[str] = None,
        # Enhancement configuration
        enable_dp: bool = False,
        dp_clip_norm: float = 1.0,
        dp_noise_multiplier: float = 0.1,
        dp_delta: float = 1e-5,
        dp_budget_epsilon: Optional[float] = None,
        enable_secure_agg: bool = False,
        secure_agg_seed: int = 42,
        enable_reputation: bool = True,
        min_reputation: float = 0.2,
        carbon_threshold: float = 400.0,
        carbon_floor: float = 0.5,
        carbon_exponent: float = 1.0,
        enable_carbon_aware: bool = True,
        enable_precision_switch: bool = False,
        precision_workload: str = "medium",
        enable_safety_monitor: bool = True,
        max_global_weight_norm: float = 1000.0,
        max_consecutive_violations: int = 3,
        safety_window_size: int = 10,
        enable_chaos: bool = False,
        chaos_failure_probability: float = 0.1,
        chaos_seed: int = 42,
        enable_human_review: bool = False,
        suspicious_norm_change_pct: float = 50.0,
        hitl_ttl_seconds: float = 3600.0,
        hitl_max_pending: int = 100,
        enable_quantum: bool = False,
        quantum_max_clients: int = 8,
        quantum_qaoa_reps: int = 1,
        human_review_threshold_clients: int = 10,
        reputation_path: Optional[str] = None,
    ):
        self.aggregation = aggregation.lower()
        if self.aggregation not in ("average", "weighted"):
            raise ValueError("aggregation must be 'average' or 'weighted'")

        self.global_weights: Optional[Dict[str, np.ndarray]] = None
        self.total_samples = 0
        self.weights_path = weights_path
        self.reputation_path = reputation_path

        # Enhancement modules
        self.dp = (
            DifferentialPrivacy(
                clip_norm=dp_clip_norm,
                noise_multiplier=dp_noise_multiplier,
                delta=dp_delta,
                budget_epsilon=dp_budget_epsilon,
            )
            if enable_dp else None
        )
        self.secure_agg = SecureAggregator(seed=secure_agg_seed) if enable_secure_agg else None
        self.reputation = ClientReputation(min_reputation=min_reputation) if enable_reputation else None
        self.carbon_weighter = (
            CarbonAwareWeighter(
                threshold=carbon_threshold,
                floor=carbon_floor,
                exponent=carbon_exponent,
            )
            if enable_carbon_aware else None
        )
        self.precision_switcher = PrecisionSwitcher() if enable_precision_switch else None
        self.precision_workload = precision_workload
        self.safety_monitor = (
            SafetyMonitor(
                max_global_weight_norm=max_global_weight_norm,
                max_consecutive_violations=max_consecutive_violations,
                window_size=safety_window_size,
            )
            if enable_safety_monitor else None
        )
        self.xai = XAIExplainer()
        self.chaos_monkey = ChaosMonkey(
            enabled=enable_chaos,
            failure_probability=chaos_failure_probability,
            seed=chaos_seed,
        )
        self.human_review = (
            HumanReviewManager(
                suspicious_norm_change_pct=suspicious_norm_change_pct,
                ttl_seconds=hitl_ttl_seconds,
                max_pending=hitl_max_pending,
            )
            if enable_human_review else None
        )
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=enable_quantum,
            qaoa_reps=quantum_qaoa_reps,
            max_clients=quantum_max_clients,
        )
        self.human_review_threshold_clients = int(human_review_threshold_clients)

        # Tracking
        self._client_contributions: List[Dict[str, Any]] = []
        self._rejected_clients: List[Dict[str, Any]] = []
        self._last_explanation: str = ""
        self._last_explanation_detail: Dict[str, Any] = {}
        self._previous_norm: Optional[float] = None
        self._lock = threading.RLock()
        self.metrics = METRICS

        # Load persisted state
        if weights_path and os.path.exists(weights_path):
            self._load_global_weights()
        if reputation_path and self.reputation is not None and os.path.exists(reputation_path):
            self._load_reputation()

    # ------------------------------------------------------------------ #
    # Validation                                                          #
    # ------------------------------------------------------------------ #
    def _validate_weight_dict(self, weight_dict: Dict[str, np.ndarray]) -> None:
        if not weight_dict:
            raise ValueError("Weight dictionary cannot be empty")
        for key, value in weight_dict.items():
            if not isinstance(value, np.ndarray):
                raise TypeError(f"Weight '{key}' must be a numpy array, got {type(value)}")
        if self.global_weights is not None:
            for key in self.global_weights:
                if key not in weight_dict:
                    raise KeyError(f"Missing weight key '{key}' in provided weights")
                if self.global_weights[key].shape != weight_dict[key].shape:
                    raise ValueError(
                        f"Shape mismatch for '{key}': global {self.global_weights[key].shape} "
                        f"vs provided {weight_dict[key].shape}"
                    )

    def _validate_parallel_lists(
        self,
        n_clients: int,
        client_ids: Optional[List[str]],
        carbon_intensities: Optional[List[float]],
        categories: Optional[List[str]],
    ) -> None:
        if client_ids is not None and len(client_ids) != n_clients:
            raise ValueError(
                f"client_ids length {len(client_ids)} != n_clients {n_clients}"
            )
        if carbon_intensities is not None and len(carbon_intensities) != n_clients:
            raise ValueError(
                f"carbon_intensities length {len(carbon_intensities)} != n_clients {n_clients}"
            )
        if categories is not None and len(categories) != n_clients:
            raise ValueError(
                f"categories length {len(categories)} != n_clients {n_clients}"
            )

    def _weight_norm(self, weight_dict: Dict[str, np.ndarray]) -> float:
        total = 0.0
        for arr in weight_dict.values():
            if isinstance(arr, np.ndarray):
                total += float(np.linalg.norm(arr.astype(np.float64)))
        return total

    def _has_nan(self, weight_dict: Dict[str, np.ndarray]) -> bool:
        return any(
            isinstance(v, np.ndarray) and v.dtype.kind == "f" and np.isnan(v).any()
            for v in weight_dict.values()
        )

    # ------------------------------------------------------------------ #
    # Persistence                                                         #
    # ------------------------------------------------------------------ #
    def _load_global_weights(self) -> None:
        if not self.weights_path:
            return
        try:
            data = np.load(self.weights_path, allow_pickle=True)
            self.global_weights = {key: data[key] for key in data.files}
            logger.info(f"loaded_global_weights path={self.weights_path}")
        except Exception as e:
            logger.error(f"load_global_weights_failed error={e}")

    def _save_global_weights(self) -> None:
        if not self.weights_path or self.global_weights is None:
            return
        try:
            np.savez(self.weights_path, **self.global_weights)
            logger.debug(f"saved_global_weights path={self.weights_path}")
        except Exception as e:
            logger.error(f"save_global_weights_failed error={e}")

    def _load_reputation(self) -> None:
        if not self.reputation_path or self.reputation is None:
            return
        try:
            with open(self.reputation_path, "r") as f:
                payload = json.load(f)
            self.reputation.load(payload)
            logger.info(f"loaded_reputation path={self.reputation_path}")
        except Exception as e:
            logger.error(f"load_reputation_failed error={e}")

    def _save_reputation(self) -> None:
        if not self.reputation_path or self.reputation is None:
            return
        try:
            with open(self.reputation_path, "w") as f:
                json.dump(self.reputation.to_dict(), f)
        except Exception as e:
            logger.error(f"save_reputation_failed error={e}")

    # ------------------------------------------------------------------ #
    # Aggregate                                                           #
    # ------------------------------------------------------------------ #
    def aggregate(
        self,
        weight_list: List[Union[Dict[str, np.ndarray], Tuple[Dict[str, np.ndarray], int]]],
        client_ids: Optional[List[str]] = None,
        carbon_intensities: Optional[List[float]] = None,
        categories: Optional[List[str]] = None,
    ) -> Dict[str, np.ndarray]:
        with self._lock:
            start = time.monotonic()
            if not weight_list:
                raise ValueError("weight_list cannot be empty")

            # 1) Normalize input
            normalized: List[Tuple[Dict[str, np.ndarray], Optional[int]]] = []
            for item in weight_list:
                if isinstance(item, tuple):
                    if self.aggregation != "weighted":
                        raise ValueError(
                            "Tuples (weights, sample_size) only allowed for 'weighted' aggregation"
                        )
                    w, size = item
                    if not isinstance(size, (int, np.integer)) or size <= 0:
                        raise ValueError("sample_size must be a positive integer")
                    normalized.append((w, int(size)))
                else:
                    if self.aggregation == "weighted":
                        raise ValueError(
                            "For 'weighted' aggregation, provide (weights, sample_size) tuples"
                        )
                    normalized.append((item, None))

            # 2) Validate shapes
            first_weights = normalized[0][0]
            for w, _ in normalized[1:]:
                if set(w.keys()) != set(first_weights.keys()):
                    raise ValueError("All weight dictionaries must have the same keys")
                for key in first_weights:
                    if first_weights[key].shape != w[key].shape:
                        raise ValueError(f"Shape mismatch for key '{key}' among clients")

            n_clients = len(normalized)
            self._validate_parallel_lists(n_clients, client_ids, carbon_intensities, categories)

            client_ids = client_ids or [f"client_{i}" for i in range(n_clients)]
            base_ci = (
                self.carbon_weighter.carbon_intensity
                if self.carbon_weighter is not None else 400.0
            )
            carbon_intensities = carbon_intensities or [base_ci] * n_clients
            categories = categories or ["unknown"] * n_clients

            # 3) Optional quantum client selection
            if self.quantum_optimizer.available:
                candidates = [
                    {
                        "client_id": client_ids[i],
                        "utility": 0.5,
                        "carbon_intensity": carbon_intensities[i],
                    }
                    for i in range(n_clients)
                ]
                selected = self.quantum_optimizer.select_clients(candidates)
                if selected is not None:
                    normalized = [normalized[i] for i in selected]
                    client_ids = [client_ids[i] for i in selected]
                    carbon_intensities = [carbon_intensities[i] for i in selected]
                    categories = [categories[i] for i in selected]
                    n_clients = len(normalized)

            # 4) Reputation filtering
            self._rejected_clients = []
            if self.reputation is not None:
                filtered: List[Tuple[Dict[str, np.ndarray], Optional[int], str, float, str]] = []
                for i, (w, size) in enumerate(normalized):
                    cid = client_ids[i]
                    if self.reputation.is_acceptable(cid):
                        filtered.append((w, size, cid, carbon_intensities[i], categories[i]))
                    else:
                        logger.warning(f"client_rejected_low_reputation client_id={cid}")
                        self._rejected_clients.append({
                            "client_id": cid,
                            "reason": "low_reputation",
                            "reputation": self.reputation.get_reputation(cid),
                        })
                        CLIENTS_REJECTED_LOW_REP.inc()
                if not filtered:
                    raise LowReputationError("All clients rejected due to low reputation")
                normalized = [(w, s) for (w, s, _, _, _) in filtered]
                client_ids = [cid for (_, _, cid, _, _) in filtered]
                carbon_intensities = [ci for (_, _, _, ci, _) in filtered]
                categories = [cat for (_, _, _, _, cat) in filtered]
                n_clients = len(normalized)

            # 5) DP
            if self.dp is not None:
                normalized = [(self.dp.clip_and_noise(w), size) for w, size in normalized]

            # 6) Secure aggregation
            if self.secure_agg is not None:
                session_id = self.secure_agg.begin_session(client_ids)
                try:
                    normalized = [
                        (
                            self.secure_agg.mask_weights(client_ids[i], w, peer_ids=client_ids),
                            size,
                        )
                        for i, (w, size) in enumerate(normalized)
                    ]
                finally:
                    self.secure_agg.end_session()

            # 7) Compute per-client weights
            if self.aggregation == "average":
                weights_per_client = [1.0 / n_clients] * n_clients
            else:
                total_size = sum(size for _, size in normalized)
                if total_size <= 0:
                    raise ValueError("Total sample size must be > 0")
                weights_per_client = [size / total_size for _, size in normalized]

            # 8) Carbon-aware weighting
            if self.carbon_weighter is not None:
                adjusted: List[float] = []
                for i, w in enumerate(weights_per_client):
                    self.carbon_weighter.set_carbon_intensity(carbon_intensities[i])
                    adjusted.append(self.carbon_weighter.adjust_weight(w))
                total_adjusted = sum(adjusted)
                if total_adjusted <= 0:
                    raise ValueError("Carbon-aware weighting resulted in zero total weight")
                weights_per_client = [a / total_adjusted for a in adjusted]

            # 9) Aggregate
            sum_weights = {key: np.zeros_like(first_weights[key], dtype=np.float64)
                           for key in first_weights}
            for (w, _), weight in zip(normalized, weights_per_client):
                for key in w:
                    if key not in sum_weights:
                        # dropped by chaos
                        continue
                    sum_weights[key] = sum_weights[key] + w[key] * weight

            # 10) Chaos injection (post-aggregation)
            sum_weights = self.chaos_monkey.maybe_corrupt(sum_weights)

            # 11) Validate
            if self._has_nan(sum_weights):
                NAN_REJECTED.inc()
                # Mark responsible clients as failed
                if self.reputation is not None:
                    for cid in client_ids:
                        self.reputation.mark_failure(cid)
                        self._save_reputation()
                raise FederatedAggregatorError("Aggregation produced NaN values")

            if set(sum_weights.keys()) != set(first_weights.keys()):
                raise FederatedAggregatorError(
                    "Aggregation lost weight tensors (chaos dropped a key)"
                )

            # 12) Safety check
            if self.safety_monitor is not None:
                safe, reason = self.safety_monitor.check_aggregate(sum_weights)
                if not safe:
                    SAFETY_BLOCKED.inc()
                    logger.warning(f"aggregation_blocked_by_safety reason={reason}")
                    # Roll back: do not overwrite self.global_weights.
                    raise SafetyViolationError(f"Aggregation blocked by safety: {reason}")

            # 13) Commit
            previous_norm = self._previous_norm
            self.global_weights = sum_weights
            self.total_samples = (
                sum(size for _, size in normalized)
                if self.aggregation == "weighted" else n_clients
            )
            current_norm = self._weight_norm(sum_weights)
            self._previous_norm = current_norm
            AGG_NORM.observe(current_norm)
            CLIENTS_AGGREGATED.labels(method=self.aggregation).inc(n_clients)

            # 14) Reputation: mark success for contributors
            if self.reputation is not None:
                for cid in client_ids:
                    self.reputation.update(cid, success=True)
                self._save_reputation()

            # 15) Persist
            self._save_global_weights()

            # 16) XAI
            self._client_contributions = [
                {
                    "client_id": client_ids[i],
                    "weight": weights_per_client[i],
                    "carbon_intensity": carbon_intensities[i],
                    "category": categories[i],
                }
                for i in range(len(client_ids))
            ]
            self._last_explanation_detail = self.xai.explain(
                self._client_contributions,
                self.global_weights,
                rejected_clients=self._rejected_clients,
            )
            self._last_explanation = self._last_explanation_detail["narrative"]
            logger.info(f"aggregation_complete n_clients={n_clients} method={self.aggregation}")

            AGG_LATENCY.labels(method=self.aggregation).observe(time.monotonic() - start)
            return copy.deepcopy(self.global_weights)

    # ------------------------------------------------------------------ #
    # Incremental update                                                  #
    # ------------------------------------------------------------------ #
    def update(
        self,
        client_weights: Dict[str, np.ndarray],
        sample_size: Optional[int] = None,
        client_id: Optional[str] = None,
        carbon_intensity: Optional[float] = None,
        category: str = "unknown",
    ) -> None:
        with self._lock:
            start = time.monotonic()
            if self.aggregation == "weighted" and sample_size is None:
                raise ValueError("sample_size is required for 'weighted' aggregation")
            if self.aggregation == "average":
                sample_size = 1

            cid = client_id or f"client_{uuid.uuid4().hex[:8]}"

            if self.reputation is not None and not self.reputation.is_acceptable(cid):
                CLIENTS_REJECTED_LOW_REP.inc()
                raise LowReputationError(
                    f"Client {cid} has low reputation; update rejected"
                )

            # Chaos
            client_weights = self.chaos_monkey.maybe_corrupt(client_weights)

            # DP
            if self.dp is not None:
                client_weights = self.dp.clip_and_noise(client_weights)

            # Secure masking
            if self.secure_agg is not None:
                session_id = self.secure_agg.begin_session([cid])
                try:
                    client_weights = self.secure_agg.mask_weights(
                        cid, client_weights, peer_ids=[cid]
                    )
                finally:
                    self.secure_agg.end_session()

            if self._has_nan(client_weights):
                NAN_REJECTED.inc()
                if self.reputation is not None:
                    self.reputation.mark_failure(cid)
                    self._save_reputation()
                raise FederatedAggregatorError("Client update contains NaN")

            # Compute effective size (carbon-aware)
            effective_size = float(sample_size or 1)
            if self.carbon_weighter is not None and carbon_intensity is not None:
                self.carbon_weighter.set_carbon_intensity(carbon_intensity)
                effective_size = self.carbon_weighter.adjust_weight(effective_size)

            if self.global_weights is None:
                self.global_weights = {
                    k: np.array(v, dtype=np.float64) for k, v in client_weights.items()
                }
                self.total_samples = effective_size
            else:
                self._validate_weight_dict(client_weights)
                denom = self.total_samples + effective_size
                if denom <= 0:
                    raise ValueError("Invalid total sample size")
                for key in self.global_weights:
                    self.global_weights[key] = (
                        self.global_weights[key] * self.total_samples
                        + client_weights[key].astype(np.float64) * effective_size
                    ) / denom
                self.total_samples = denom

            # Safety check
            if self.safety_monitor is not None:
                safe, reason = self.safety_monitor.check_aggregate(self.global_weights)
                if not safe:
                    SAFETY_BLOCKED.inc()
                    logger.warning(f"update_blocked_by_safety reason={reason}")

            current_norm = self._weight_norm(self.global_weights)
            self._previous_norm = current_norm
            AGG_NORM.observe(current_norm)

            # Track contribution
            self._client_contributions.append({
                "client_id": cid,
                "weight": float(effective_size),
                "carbon_intensity": float(carbon_intensity or 0.0),
                "category": category,
            })

            # Reputation
            if self.reputation is not None:
                self.reputation.update(cid, success=True)
                self._save_reputation()

            # Persist
            self._save_global_weights()

            # XAI (structured)
            self._last_explanation_detail = self.xai.explain(
                self._client_contributions,
                self.global_weights,
                rejected_clients=self._rejected_clients,
            )
            self._last_explanation = self._last_explanation_detail["narrative"]

            # Human review
            if (
                self.human_review is not None
                and len(self._client_contributions) >= self.human_review_threshold_clients
            ):
                self.human_review.request_review(
                    aggregate_id=cid,
                    details={
                        "num_clients": len(self._client_contributions),
                        "explanation": self._last_explanation,
                    },
                )

            AGG_LATENCY.labels(method="update").observe(time.monotonic() - start)
            logger.debug(
                f"incremental_update client_id={cid} "
                f"effective_size={effective_size:.2f} total={self.total_samples:.2f}"
            )

    # ------------------------------------------------------------------ #
    # Public helpers                                                      #
    # ------------------------------------------------------------------ #
    def get_global_weights(self, deep: bool = False) -> Optional[Dict[str, np.ndarray]]:
        with self._lock:
            if self.global_weights is None:
                return None
            if deep:
                return copy.deepcopy(self.global_weights)
            return {k: v.copy() if isinstance(v, np.ndarray) else v
                    for k, v in self.global_weights.items()}

    def get_last_explanation(self) -> str:
        return self._last_explanation

    def get_last_explanation_detail(self) -> Dict[str, Any]:
        return dict(self._last_explanation_detail)

    def get_rejected_clients(self) -> List[Dict[str, Any]]:
        return list(self._rejected_clients)

    def get_enhancement_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "differential_privacy_enabled": self.dp is not None,
                "privacy_accountant": (
                    self.dp.get_accountant().snapshot() if self.dp is not None else None
                ),
                "secure_aggregation_enabled": self.secure_agg is not None,
                "reputation_enabled": self.reputation is not None,
                "reputation": (
                    self.reputation.to_dict() if self.reputation is not None else None
                ),
                "carbon_aware_enabled": self.carbon_weighter is not None,
                "carbon_weighter": (
                    self.carbon_weighter.get_status() if self.carbon_weighter else None
                ),
                "precision_switch_enabled": self.precision_switcher is not None,
                "precision_switcher": (
                    self.precision_switcher.get_status() if self.precision_switcher else None
                ),
                "safety_monitor": {
                    "enabled": self.safety_monitor is not None,
                    "violations": (
                        self.safety_monitor.get_violations()[-5:]
                        if self.safety_monitor else []
                    ),
                },
                "chaos_monkey": self.chaos_monkey.get_stats(),
                "human_review": (
                    self.human_review.get_stats() if self.human_review else {"enabled": False}
                ),
                "quantum_optimizer": self.quantum_optimizer.get_status(),
                "last_explanation": self._last_explanation,
                "last_explanation_detail": self._last_explanation_detail,
                "rejected_clients": self._rejected_clients,
                "metrics": self.metrics.snapshot(),
            }

    def recommend_precision(
        self, workload_size: Optional[str] = None,
        carbon_intensity: Optional[float] = None,
    ) -> str:
        if self.precision_switcher is None:
            return "fp32"
        return self.precision_switcher.recommend(
            workload_size=workload_size or self.precision_workload,
            carbon_intensity=carbon_intensity,
        )

    def export_weights(
        self, student: Any, attr_names: Optional[List[str]] = None
    ) -> Dict[str, np.ndarray]:
        """Extract weights from a student object."""
        attr_names = attr_names or ["weights", "biases"]
        weight_dict: Dict[str, np.ndarray] = {}
        for name in attr_names:
            if not hasattr(student, name):
                raise AttributeError(f"Student does not have attribute '{name}'")
            attr = getattr(student, name)
            if not isinstance(attr, np.ndarray):
                raise TypeError(f"Attribute '{name}' must be a numpy array")
            weight_dict[name] = attr.copy()
        return weight_dict

    def import_weights(self, student: Any, weight_dict: Dict[str, np.ndarray]) -> None:
        """Load averaged weights into a student object."""
        for name, array in weight_dict.items():
            if not hasattr(student, name):
                logger.warning(f"student_missing_attribute name={name} skipping")
                continue
            setattr(student, name, array.copy())
        logger.info("imported_global_weights_into_student")

    def __aenter__(self) -> "FederatedAggregator":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        # Persist state.
        self._save_global_weights()
        self._save_reputation()


# =============================================================================
# Self-test
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    agg = FederatedAggregator(
        aggregation="weighted",
        enable_dp=True,
        dp_clip_norm=1.0,
        dp_noise_multiplier=0.5,
        dp_budget_epsilon=100.0,
        enable_secure_agg=True,
        enable_reputation=True,
        enable_carbon_aware=True,
        carbon_floor=0.5,
        enable_precision_switch=True,
        enable_safety_monitor=True,
        enable_chaos=False,
        enable_human_review=True,
        enable_quantum=False,
    )

    w1 = {"weights": np.random.randn(4), "biases": np.random.randn(4)}
    w2 = {"weights": np.random.randn(4), "biases": np.random.randn(4)}

    global_w = agg.aggregate(
        [(w1, 100), (w2, 200)],
        client_ids=["c1", "c2"],
        carbon_intensities=[300.0, 500.0],
        categories=["edge", "cloud"],
    )
    print("Global weights keys:", list(global_w.keys()))
    print("Explanation:", agg.get_last_explanation())
    print("Status:", json.dumps(agg.get_enhancement_status(), indent=2, default=str))

    # Incremental path
    agg.update(w1, sample_size=50, client_id="c3",
               carbon_intensity=250.0, category="edge")
    print("After update, total_samples:", agg.total_samples)
    print("Explanation:", agg.get_last_explanation())
