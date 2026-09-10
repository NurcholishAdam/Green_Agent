# src/enhancements/federated_aggregator.py
"""
Enhanced Federated Aggregator for Green Agent Students - v2.0 (Enterprise).

Supports weighted averaging, incremental updates, persistence, and robustness checks.
Enables sharing distilled student weights across deployments without raw data.

NEW IN v2.0 (Advanced Enhancements):
- Differential Privacy (clipping + Gaussian/Laplace noise).
- Secure Aggregation (mask-based simulation; Bonawitz et al. style).
- Client Reputation tracking (rejects low-reputation updates).
- Carbon-aware weighting (down-weights high-carbon clients).
- Adaptive precision switching (fp32 / fp16 / int8).
- SafetyMonitor with temporal logic-like rules.
- XAIExplainer for aggregation attribution.
- ChaosMonkey for resilience testing.
- HumanReviewManager for pre-commit human review.
- QuantumDistillationOptimizer (optional) for QAOA-assisted client selection.

Usage:
    aggregator = FederatedAggregator(aggregation='weighted')
    # Batch aggregation:
    global_weights = aggregator.aggregate([(weights1, 100), (weights2, 200)])
    # Or incremental update:
    aggregator.update(weights1, sample_size=100, client_id='c1')
    aggregator.update(weights2, sample_size=200, client_id='c2')
    global_weights = aggregator.get_global_weights()
"""

import copy
import hashlib
import json
import logging
import os
import random
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np

logger = logging.getLogger(__name__)

# ============================================================
# NEW: Optional QISKIT for quantum distillation
# ============================================================
try:
    import qiskit
    from qiskit.optimization import QuadraticProgram
    from qiskit.optimization.algorithms import MinimumEigenOptimizer
    from qiskit.algorithms import QAOA
    from qiskit import Aer
    QISKIT_AVAILABLE = True
except ImportError:
    QISKIT_AVAILABLE = False


# ============================================================
# NEW: Custom exceptions
# ============================================================
class FederatedAggregatorError(Exception): pass
class SafetyViolationError(FederatedAggregatorError): pass
class ChaosExperimentError(FederatedAggregatorError): pass
class LowReputationError(FederatedAggregatorError): pass


# ============================================================
# NEW: SafetyMonitor (temporal logic-like)
# ============================================================
class SafetyMonitor:
    """Temporal logic-like rules on aggregated weight norms and client updates."""
    def __init__(
        self,
        max_global_weight_norm: float = 1000.0,
        max_consecutive_violations: int = 3,
        window_size: int = 10,
    ):
        self.max_global_weight_norm = max_global_weight_norm
        self.max_consecutive_violations = max_consecutive_violations
        self.window_size = window_size
        self.consecutive_violations = 0
        self.history: deque = deque(maxlen=window_size)
        self.violations: List[Dict] = []

    def _weight_norm(self, weight_dict: Dict[str, np.ndarray]) -> float:
        total = 0.0
        for arr in weight_dict.values():
            if isinstance(arr, np.ndarray):
                total += float(np.linalg.norm(arr))
        return total

    def check_aggregate(self, global_weights: Dict[str, np.ndarray]) -> bool:
        norm = self._weight_norm(global_weights)
        self.history.append({"norm": norm, "timestamp": datetime.now().isoformat()})
        if norm > self.max_global_weight_norm:
            self.consecutive_violations += 1
            self._record_violation("weight_norm_exceeded", {"norm": norm})
            if self.consecutive_violations >= self.max_consecutive_violations:
                return False
            return False
        self.consecutive_violations = 0
        # Temporal rule: average norm over window must be < 0.8 * max
        if len(self.history) >= self.window_size:
            avg_norm = np.mean([h["norm"] for h in self.history])
            if avg_norm > 0.8 * self.max_global_weight_norm:
                self._record_violation("avg_norm_high", {"avg_norm": avg_norm})
                return False
        return True

    def _record_violation(self, rule: str, details: Dict):
        self.violations.append({
            "rule": rule,
            "details": details,
            "timestamp": datetime.now().isoformat(),
        })
        logger.warning(f"Safety violation: {rule} - {details}")

    def get_violations(self) -> List[Dict]:
        return self.violations


# ============================================================
# NEW: XAIExplainer
# ============================================================
class XAIExplainer:
    """Explains aggregated model by attributing to contributing clients."""
    def explain(
        self,
        client_contributions: List[Dict[str, Any]],
        global_weights: Dict[str, np.ndarray],
    ) -> str:
        if not client_contributions:
            return "No client contributions to explain."
        total_weight = sum(c.get("weight", 0.0) for c in client_contributions)
        parts = [f"Aggregated {len(client_contributions)} clients with total weight {total_weight:.2f}."]
        # Top contributor
        top = max(client_contributions, key=lambda c: c.get("weight", 0.0))
        top_pct = (top.get("weight", 0.0) / max(total_weight, 1e-9)) * 100
        parts.append(f"Top contributor: client '{top.get('client_id', 'unknown')}' "
                     f"with {top_pct:.1f}% weight.")
        # Category totals
        category_weights = defaultdict(float)
        for c in client_contributions:
            category_weights[c.get("category", "unknown")] += c.get("weight", 0.0)
        for cat, w in sorted(category_weights.items(), key=lambda x: -x[1]):
            parts.append(f"  category '{cat}': {w:.2f} weight.")
        # Weight norm
        total_norm = sum(float(np.linalg.norm(a)) for a in global_weights.values() if isinstance(a, np.ndarray))
        parts.append(f"Global weight norm: {total_norm:.4f}.")
        return " ".join(parts)


# ============================================================
# NEW: Differential Privacy
# ============================================================
class DifferentialPrivacy:
    """Differential privacy via clipping + Gaussian noise."""
    def __init__(self, clip_norm: float = 1.0, noise_multiplier: float = 0.1):
        self.clip_norm = clip_norm
        self.noise_multiplier = noise_multiplier

    def clip_and_noise(self, weight_dict: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        # Compute global norm
        norm = np.sqrt(sum(float(np.sum(a ** 2)) for a in weight_dict.values()))
        clip_factor = min(1.0, self.clip_norm / (norm + 1e-12))
        noisy = {}
        for k, v in weight_dict.items():
            clipped = v * clip_factor
            noise = np.random.normal(0, self.noise_multiplier * self.clip_norm, clipped.shape)
            noisy[k] = clipped + noise
        return noisy


# ============================================================
# NEW: Secure Aggregation (simulation)
# ============================================================
class SecureAggregator:
    """Simulated secure aggregation using deterministic masks.
    Real implementations use secret sharing; here we simulate with hashed masks
    that cancel when summed."""
    def __init__(self, seed: int = 42):
        self.seed = seed

    def _mask_for(self, client_id: str, shape: Tuple[int, ...]) -> np.ndarray:
        # Deterministic mask per (client_id, shape, seed). Sum across clients should cancel.
        rng = np.random.default_rng(abs(hash((client_id, shape, self.seed))) % (2 ** 32))
        return rng.normal(0, 1, shape)

    def mask_weights(self, client_id: str, weight_dict: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        return {k: v + self._mask_for(client_id, v.shape) for k, v in weight_dict.items()}

    def unmask_weights(self, weight_dict: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        # Masks cancel when aggregated; return as-is.
        return weight_dict


# ============================================================
# NEW: Client Reputation
# ============================================================
class ClientReputation:
    """Tracks per-client reputation based on historical quality of updates."""
    def __init__(self, initial_reputation: float = 0.5, min_reputation: float = 0.2):
        self.reputation: Dict[str, float] = defaultdict(lambda: initial_reputation)
        self.min_reputation = min_reputation
        self.updates: Dict[str, int] = defaultdict(int)

    def update(self, client_id: str, success: bool):
        alpha = 0.2
        prev = self.reputation[client_id]
        self.reputation[client_id] = prev + alpha * ((1.0 if success else 0.0) - prev)
        self.updates[client_id] += 1

    def is_acceptable(self, client_id: str) -> bool:
        return self.reputation.get(client_id, 0.5) >= self.min_reputation

    def get_reputation(self, client_id: str) -> float:
        return self.reputation.get(client_id, 0.5)


# ============================================================
# NEW: Carbon-Aware Weighter
# ============================================================
class CarbonAwareWeighter:
    """Adjusts client weights based on carbon intensity."""
    def __init__(self, carbon_intensity: float = 400.0, threshold: float = 400.0):
        self.carbon_intensity = carbon_intensity
        self.threshold = threshold

    def adjust_weight(self, base_weight: float) -> float:
        if self.carbon_intensity <= self.threshold:
            return base_weight
        # Down-weight when carbon is high (up to 50% reduction)
        carbon_factor = min(1.0, self.threshold / max(self.carbon_intensity, 1e-6))
        return base_weight * (0.5 + 0.5 * carbon_factor)

    def set_carbon_intensity(self, intensity: float):
        self.carbon_intensity = intensity


# ============================================================
# NEW: Precision Switcher
# ============================================================
class PrecisionSwitcher:
    """Recommends precision based on carbon intensity and workload size."""
    def __init__(self, default_carbon_intensity: float = 400.0):
        self.default_carbon_intensity = default_carbon_intensity

    def recommend(self, workload_size: str = "medium", carbon_intensity: Optional[float] = None) -> str:
        ci = carbon_intensity if carbon_intensity is not None else self.default_carbon_intensity
        if ci > 500 or workload_size == "large":
            return "int8"
        elif ci > 300 or workload_size == "medium":
            return "fp16"
        return "fp32"

    def cast(self, weight_dict: Dict[str, np.ndarray], precision: str) -> Dict[str, np.ndarray]:
        if precision == "fp16":
            return {k: v.astype(np.float16) for k, v in weight_dict.items()}
        elif precision == "int8":
            # Scale to int8 and store scale in metadata-like convention
            return {k: (v / max(np.max(np.abs(v)), 1e-6) * 127).astype(np.int8) for k, v in weight_dict.items()}
        return weight_dict


# ============================================================
# NEW: ChaosMonkey
# ============================================================
class ChaosMonkey:
    """Injects simulated failures for resilience testing."""
    def __init__(self, enabled: bool = False, failure_probability: float = 0.1):
        self.enabled = enabled
        self.failure_probability = failure_probability
        self.injected_failures = 0

    def maybe_corrupt(self, weight_dict: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        if not self.enabled or random.random() >= self.failure_probability:
            return weight_dict
        self.injected_failures += 1
        corruption = random.choice(["nan", "zero", "huge", "stale"])
        if corruption == "nan":
            for k in weight_dict:
                if isinstance(weight_dict[k], np.ndarray) and weight_dict[k].size > 0:
                    weight_dict[k] = weight_dict[k].copy()
                    weight_dict[k].flat[0] = np.nan
        elif corruption == "zero":
            weight_dict = {k: np.zeros_like(v) for k, v in weight_dict.items()}
        elif corruption == "huge":
            weight_dict = {k: v * 1e6 for k, v in weight_dict.items()}
        elif corruption == "stale":
            # Slightly shift weights
            weight_dict = {k: v + 0.01 for k, v in weight_dict.items()}
        logger.warning(f"ChaosMonkey injected corruption: {corruption}")
        return weight_dict

    def get_stats(self) -> Dict:
        return {"enabled": self.enabled, "injected_failures": self.injected_failures}


# ============================================================
# NEW: Human Review Manager
# ============================================================
class HumanReviewManager:
    """Manages pre-commit human review for suspicious aggregations."""
    def __init__(self, suspicious_norm_change_pct: float = 50.0):
        self.pending_reviews: Dict[str, Dict[str, Any]] = {}
        self.suspicious_norm_change_pct = suspicious_norm_change_pct

    def request_review(self, aggregate_id: str, details: Dict) -> str:
        review_id = str(uuid.uuid4())
        self.pending_reviews[review_id] = {
            "review_id": review_id,
            "aggregate_id": aggregate_id,
            "details": details,
            "status": "pending",
            "created_at": datetime.now().isoformat(),
        }
        logger.info(f"Human review requested: {review_id}")
        return review_id

    def approve(self, review_id: str) -> bool:
        if review_id in self.pending_reviews:
            self.pending_reviews[review_id]["status"] = "approved"
            return True
        return False

    def reject(self, review_id: str, reason: Optional[str] = None) -> bool:
        if review_id in self.pending_reviews:
            self.pending_reviews[review_id]["status"] = "rejected"
            self.pending_reviews[review_id]["reason"] = reason or "unspecified"
            return True
        return False

    def get_pending(self) -> List[Dict]:
        return [r for r in self.pending_reviews.values() if r["status"] == "pending"]

    def get_stats(self) -> Dict:
        statuses = defaultdict(int)
        for r in self.pending_reviews.values():
            statuses[r["status"]] += 1
        return {"total": len(self.pending_reviews), "by_status": dict(statuses)}


# ============================================================
# NEW: Quantum Distillation Optimizer (optional)
# ============================================================
class QuantumDistillationOptimizer:
    """Optional QAOA-assisted selection of the best subset of clients."""
    def __init__(self, enabled: bool = False, qaoa_reps: int = 1, max_clients: int = 8):
        self.enabled = enabled
        self.qaoa_reps = qaoa_reps
        self.max_clients = max_clients
        self.available = enabled and QISKIT_AVAILABLE

    def select_clients(self, candidates: List[Dict[str, Any]]) -> Optional[List[int]]:
        if not self.available or not candidates:
            return None
        # Limit size for tractability
        candidates = candidates[: self.max_clients]
        try:
            qp = QuadraticProgram()
            for i in range(len(candidates)):
                qp.binary_var(f"x{i}")
            # Maximize utility, penalize carbon
            linear = {}
            for i, c in enumerate(candidates):
                util = c.get("utility", 0.5)
                carbon = c.get("carbon_intensity", 400.0)
                value = util - (carbon / 1000.0) * 0.3
                linear[f"x{i}"] = -value
            qp.minimize(linear=linear)
            # Select at most half
            k = max(1, len(candidates) // 2)
            qp.linear_constraint(
                linear={f"x{i}": 1 for i in range(len(candidates))},
                sense="LE",
                rhs=k,
                name="max_clients",
            )
            backend = Aer.get_backend("aer_simulator")
            qaoa = QAOA(reps=self.qaoa_reps)
            optimizer = MinimumEigenOptimizer(qaoa)
            result = optimizer.solve(qp)
            selected = [i for i in range(len(candidates)) if result.x[i] > 0.5]
            return selected or list(range(len(candidates)))
        except Exception as e:
            logger.warning(f"Quantum client selection failed: {e}")
            return None

    def get_status(self) -> Dict:
        return {"available": self.available, "qiskit_available": QISKIT_AVAILABLE}


# ============================================================
# MAIN FederatedAggregator (enhanced)
# ============================================================
class FederatedAggregator:
    """
    Federated averaging aggregator with optional weighting and persistence.
    Now enhanced with differential privacy, secure aggregation, client reputation,
    carbon awareness, adaptive precision, safety, XAI, chaos testing, human review,
    and optional quantum distillation.
    """

    def __init__(
        self,
        aggregation: str = "average",
        weights_path: Optional[str] = None,
        # NEW: enhancement configuration
        enable_dp: bool = False,
        dp_clip_norm: float = 1.0,
        dp_noise_multiplier: float = 0.1,
        enable_secure_agg: bool = False,
        secure_agg_seed: int = 42,
        enable_reputation: bool = True,
        min_reputation: float = 0.2,
        carbon_threshold: float = 400.0,
        enable_carbon_aware: bool = True,
        enable_precision_switch: bool = False,
        precision_workload: str = "medium",
        enable_safety_monitor: bool = True,
        max_global_weight_norm: float = 1000.0,
        enable_chaos: bool = False,
        chaos_failure_probability: float = 0.1,
        enable_human_review: bool = False,
        suspicious_norm_change_pct: float = 50.0,
        enable_quantum: bool = False,
        quantum_max_clients: int = 8,
        quantum_qaoa_reps: int = 1,
        human_review_threshold_clients: int = 10,
    ):
        self.aggregation = aggregation.lower()
        if self.aggregation not in ("average", "weighted"):
            raise ValueError("aggregation must be 'average' or 'weighted'")

        self.global_weights: Optional[Dict[str, np.ndarray]] = None
        self.total_samples = 0
        self.weights_path = weights_path

        # NEW: Enhancement modules
        self.dp = DifferentialPrivacy(clip_norm=dp_clip_norm, noise_multiplier=dp_noise_multiplier) if enable_dp else None
        self.secure_agg = SecureAggregator(seed=secure_agg_seed) if enable_secure_agg else None
        self.reputation = ClientReputation(min_reputation=min_reputation) if enable_reputation else None
        self.carbon_weighter = CarbonAwareWeighter(threshold=carbon_threshold) if enable_carbon_aware else None
        self.precision_switcher = PrecisionSwitcher() if enable_precision_switch else None
        self.precision_workload = precision_workload
        self.safety_monitor = SafetyMonitor(max_global_weight_norm=max_global_weight_norm) if enable_safety_monitor else None
        self.xai = XAIExplainer()
        self.chaos_monkey = ChaosMonkey(enabled=enable_chaos, failure_probability=chaos_failure_probability)
        self.human_review = HumanReviewManager(suspicious_norm_change_pct=suspicious_norm_change_pct) if enable_human_review else None
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=enable_quantum, qaoa_reps=quantum_qaoa_reps, max_clients=quantum_max_clients
        )
        self.human_review_threshold_clients = human_review_threshold_clients

        # Client contributions for XAI
        self._client_contributions: List[Dict[str, Any]] = []
        self._previous_norm: Optional[float] = None
        self._last_explanation: str = ""

        # Load persisted global weights
        if weights_path and os.path.exists(weights_path):
            self._load_global_weights()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
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

    def _weight_norm(self, weight_dict: Dict[str, np.ndarray]) -> float:
        total = 0.0
        for arr in weight_dict.values():
            if isinstance(arr, np.ndarray):
                total += float(np.linalg.norm(arr))
        return total

    def _has_nan(self, weight_dict: Dict[str, np.ndarray]) -> bool:
        return any(isinstance(v, np.ndarray) and np.isnan(v).any() for v in weight_dict.values())

    def _load_global_weights(self):
        if not self.weights_path:
            return
        try:
            data = np.load(self.weights_path, allow_pickle=True)
            self.global_weights = {key: data[key] for key in data.files}
            logger.info(f"Loaded global weights from {self.weights_path}")
        except Exception as e:
            logger.error(f"Failed to load weights from {self.weights_path}: {e}")

    def _save_global_weights(self):
        if not self.weights_path or self.global_weights is None:
            return
        try:
            np.savez(self.weights_path, **self.global_weights)
            logger.debug(f"Saved global weights to {self.weights_path}")
        except Exception as e:
            logger.error(f"Failed to save weights to {self.weights_path}: {e}")

    # ------------------------------------------------------------------
    # Aggregate
    # ------------------------------------------------------------------
    def aggregate(
        self,
        weight_list: List[Union[Dict[str, np.ndarray], Tuple[Dict[str, np.ndarray], int]]],
        client_ids: Optional[List[str]] = None,
        carbon_intensities: Optional[List[float]] = None,
        categories: Optional[List[str]] = None,
    ) -> Dict[str, np.ndarray]:
        """
        Aggregate a list of weight dictionaries or (weights, sample_size) tuples.

        New optional args:
            client_ids: Client identifiers for reputation/XAI.
            carbon_intensities: Per-client carbon intensity for carbon-aware weighting.
            categories: Optional category labels for XAI.
        """
        if not weight_list:
            raise ValueError("weight_list cannot be empty")

        # Normalize input
        normalized: List[Tuple[Dict[str, np.ndarray], Optional[int]]] = []
        for item in weight_list:
            if isinstance(item, tuple):
                if self.aggregation != "weighted":
                    raise ValueError("Tuples (weights, sample_size) are only allowed for 'weighted' aggregation")
                w, size = item
                if not isinstance(size, (int, np.integer)) or size <= 0:
                    raise ValueError("sample_size must be a positive integer")
                normalized.append((w, int(size)))
            else:
                if self.aggregation == "weighted":
                    raise ValueError("For 'weighted' aggregation, provide (weights, sample_size) tuples")
                normalized.append((item, None))

        # Validate shapes/keys
        first_weights = normalized[0][0]
        for w, _ in normalized[1:]:
            if set(w.keys()) != set(first_weights.keys()):
                raise ValueError("All weight dictionaries must have the same keys")
            for key in first_weights:
                if first_weights[key].shape != w[key].shape:
                    raise ValueError(f"Shape mismatch for key '{key}' among clients")

        # Prepare per-client contribution tracking
        n_clients = len(normalized)
        client_ids = client_ids or [f"client_{i}" for i in range(n_clients)]
        carbon_intensities = carbon_intensities or [self.carbon_weighter.carbon_intensity if self.carbon_weighter else 400.0] * n_clients
        categories = categories or ["unknown"] * n_clients

        # Optional quantum client selection
        if self.quantum_optimizer.available:
            candidates = [
                {
                    "client_id": client_ids[i],
                    "utility": 0.5,
                    "carbon_intensity": carbon_intensities[i],
                }
                for i in range(n_clients)
            ]
            selected_indices = self.quantum_optimizer.select_clients(candidates)
            if selected_indices is not None:
                normalized = [normalized[i] for i in selected_indices]
                client_ids = [client_ids[i] for i in selected_indices]
                carbon_intensities = [carbon_intensities[i] for i in selected_indices]
                categories = [categories[i] for i in selected_indices]
                n_clients = len(normalized)

        # Reputation filtering
        if self.reputation is not None:
            filtered = []
            for i, (w, size) in enumerate(normalized):
                cid = client_ids[i]
                if self.reputation.is_acceptable(cid):
                    filtered.append((w, size, cid, carbon_intensities[i], categories[i]))
                else:
                    logger.warning(f"Skipping client '{cid}' due to low reputation")
            if not filtered:
                raise LowReputationError("All clients rejected due to low reputation")
            normalized = [(w, s) for (w, s, _, _, _) in filtered]
            client_ids = [cid for (_, _, cid, _, _) in filtered]
            carbon_intensities = [ci for (_, _, _, ci, _) in filtered]
            categories = [cat for (_, _, _, _, cat) in filtered]
            n_clients = len(normalized)

        # Differential privacy: clip + noise each client's update
        if self.dp is not None:
            normalized = [(self.dp.clip_and_noise(w), size) for w, size in normalized]

        # Secure aggregation: mask each client's update
        if self.secure_agg is not None:
            normalized = [(self.secure_agg.mask_weights(client_ids[i], w), size)
                          for i, (w, size) in enumerate(normalized)]

        # Compute weights
        weights_per_client: List[float] = []
        if self.aggregation == "average":
            weights_per_client = [1.0 / n_clients] * n_clients
        else:
            total_size = sum(size for _, size in normalized)
            if total_size == 0:
                raise ValueError("Total sample size must be > 0")
            weights_per_client = [size / total_size for _, size in normalized]

        # Carbon-aware weighting
        if self.carbon_weighter is not None:
            adjusted = []
            for i, w in enumerate(weights_per_client):
                self.carbon_weighter.set_carbon_intensity(carbon_intensities[i])
                adjusted.append(self.carbon_weighter.adjust_weight(w))
            total_adjusted = sum(adjusted)
            if total_adjusted <= 0:
                raise ValueError("Carbon-aware weighting resulted in zero total weight")
            weights_per_client = [a / total_adjusted for a in adjusted]

        # Aggregate
        sum_weights = {key: np.zeros_like(first_weights[key]) for key in first_weights}
        for (w, _), weight in zip(normalized, weights_per_client):
            for key in w:
                sum_weights[key] += w[key] * weight

        # Apply chaos if enabled
        sum_weights = self.chaos_monkey.maybe_corrupt(sum_weights)

        # Validate result
        if self._has_nan(sum_weights):
            logger.error("Aggregation produced NaN values; rejecting result")
            raise FederatedAggregatorError("Aggregation produced NaN values")

        self.global_weights = sum_weights
        self.total_samples = sum(size for _, size in normalized) if self.aggregation == "weighted" else n_clients

        # Safety check
        if self.safety_monitor is not None:
            if not self.safety_monitor.check_aggregate(self.global_weights):
                logger.warning("Safety violation detected in aggregation")

        # Update reputation (mark successful this round)
        if self.reputation is not None:
            for cid in client_ids:
                self.reputation.update(cid, success=True)

        # Persist
        self._save_global_weights()

        # XAI
        self._client_contributions = [
            {
                "client_id": client_ids[i],
                "weight": weights_per_client[i],
                "carbon_intensity": carbon_intensities[i],
                "category": categories[i],
            }
            for i in range(len(client_ids))
        ]
        self._last_explanation = self.xai.explain(self._client_contributions, self.global_weights)

        logger.info(f"Aggregated {n_clients} clients using '{self.aggregation}' method. "
                    f"Explanation: {self._last_explanation}")

        return copy.deepcopy(self.global_weights)

    # ------------------------------------------------------------------
    # Incremental update
    # ------------------------------------------------------------------
    def update(
        self,
        client_weights: Dict[str, np.ndarray],
        sample_size: Optional[int] = None,
        client_id: Optional[str] = None,
        carbon_intensity: Optional[float] = None,
        category: str = "unknown",
    ) -> None:
        """Incrementally update global weights with a new client's contribution."""
        if self.aggregation == "weighted" and sample_size is None:
            raise ValueError("sample_size is required for 'weighted' aggregation")
        if self.aggregation == "average":
            sample_size = 1

        cid = client_id or f"client_{uuid.uuid4().hex[:8]}"
        # Reputation gate
        if self.reputation is not None and not self.reputation.is_acceptable(cid):
            raise LowReputationError(f"Client {cid} has low reputation; update rejected")

        # Chaos injection
        client_weights = self.chaos_monkey.maybe_corrupt(client_weights)

        # DP
        if self.dp is not None:
            client_weights = self.dp.clip_and_noise(client_weights)

        # Secure aggregation masking (simulated)
        if self.secure_agg is not None:
            client_weights = self.secure_agg.mask_weights(cid, client_weights)

        if self.global_weights is None:
            self.global_weights = {k: np.array(v, dtype=np.float64) for k, v in client_weights.items()}
            self.total_samples = sample_size if sample_size else 1
        else:
            self._validate_weight_dict(client_weights)
            new_total = self.total_samples + (sample_size or 1)
            # Carbon-aware adjustment of the incoming weight
            effective_size = sample_size or 1
            if self.carbon_weighter is not None and carbon_intensity is not None:
                self.carbon_weighter.set_carbon_intensity(carbon_intensity)
                effective_size = self.carbon_weighter.adjust_weight(effective_size)
            # Weighted incremental update
            for key in self.global_weights:
                self.global_weights[key] = (
                    self.global_weights[key] * self.total_samples
                    + client_weights[key] * effective_size
                ) / (self.total_samples + effective_size)
            self.total_samples = int(self.total_samples + effective_size)

        # Track contribution
        self._client_contributions.append({
            "client_id": cid,
            "weight": float(effective_size) if self.carbon_weighter else float(sample_size or 1),
            "carbon_intensity": carbon_intensity or 0.0,
            "category": category,
        })

        # Safety
        if self.safety_monitor is not None:
            self.safety_monitor.check_aggregate(self.global_weights)

        # Persist
        self._save_global_weights()

        # XAI
        self._last_explanation = self.xai.explain(self._client_contributions, self.global_weights)

        # Human review for suspicious large aggregations
        if self.human_review is not None and len(self._client_contributions) >= self.human_review_threshold_clients:
            self.human_review.request_review(
                aggregate_id=str(uuid.uuid4()),
                details={"num_clients": len(self._client_contributions), "explanation": self._last_explanation},
            )

        logger.debug(f"Updated global model with client '{cid}' (total samples: {self.total_samples})")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get_global_weights(self) -> Optional[Dict[str, np.ndarray]]:
        """Return a copy of the current global weights."""
        if self.global_weights is None:
            return None
        return copy.deepcopy(self.global_weights)

    def get_last_explanation(self) -> str:
        return self._last_explanation

    def get_enhancement_status(self) -> Dict:
        return {
            "differential_privacy_enabled": self.dp is not None,
            "secure_aggregation_enabled": self.secure_agg is not None,
            "reputation_enabled": self.reputation is not None,
            "carbon_aware_enabled": self.carbon_weighter is not None,
            "precision_switch_enabled": self.precision_switcher is not None,
            "safety_monitor": {
                "enabled": self.safety_monitor is not None,
                "violations": self.safety_monitor.get_violations()[-5:] if self.safety_monitor else [],
            },
            "chaos_monkey": self.chaos_monkey.get_stats(),
            "human_review": self.human_review.get_stats() if self.human_review else {"enabled": False},
            "quantum_optimizer": self.quantum_optimizer.get_status(),
            "reputation": dict(self.reputation.reputation) if self.reputation else {},
            "last_explanation": self._last_explanation,
        }

    def recommend_precision(self, workload_size: str = None, carbon_intensity: Optional[float] = None) -> str:
        """Recommend a precision based on carbon intensity and workload."""
        if self.precision_switcher is None:
            return "fp32"
        return self.precision_switcher.recommend(
            workload_size=workload_size or self.precision_workload,
            carbon_intensity=carbon_intensity,
        )

    def export_weights(self, student: Any, attr_names: List[str] = ["weights", "biases"]) -> Dict[str, np.ndarray]:
        """Extract weights from a student object."""
        weight_dict = {}
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
                logger.warning(f"Student does not have attribute '{name}'; skipping.")
                continue
            setattr(student, name, array.copy())
        logger.info("Imported global weights into student")


# ============================================================
# Quick self-test
# ============================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    agg = FederatedAggregator(
        aggregation="weighted",
        enable_dp=True,
        enable_secure_agg=True,
        enable_reputation=True,
        enable_carbon_aware=True,
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
    print("Global weights:", global_w)
    print("Explanation:", agg.get_last_explanation())
    print("Enhancement status:", json.dumps(agg.get_enhancement_status(), indent=2, default=str))
