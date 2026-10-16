"""
federated.py — Federated green learning (Enhancement 3).

Carbon-weighted FedAvg with shape validation, per-client Gaussian noise,
and an RDP accountant.
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


class SimpleRDPAccountant:
    """Minimal RDP accountant for the subsampled Gaussian mechanism."""

    def __init__(self, target_delta: float = 1e-5, max_epsilon: float = 10.0):
        self.target_delta = target_delta
        self.max_epsilon = max_epsilon
        self.steps = 0
        self.orders = [1 + x / 10.0 for x in range(1, 100)]
        self.rdp = np.zeros(len(self.orders))

    def step(self, sigma: float, q: float = 1.0) -> None:
        if sigma <= 0:
            return
        self.steps += 1
        for i, o in enumerate(self.orders):
            self.rdp[i] += (o * q * q) / (2 * sigma * sigma)

    def epsilon(self) -> float:
        if self.steps == 0:
            return 0.0
        eps = [
            self.rdp[i] + math.log(1.0 / self.target_delta) / (self.orders[i] - 1)
            for i in range(len(self.orders)) if self.orders[i] > 1
        ]
        return float(min(eps)) if eps else float("inf")

    def budget_exceeded(self) -> bool:
        return self.epsilon() > self.max_epsilon

    def get_state(self) -> Dict[str, Any]:
        return {
            "steps": self.steps,
            "epsilon": self.epsilon(),
            "delta": self.target_delta,
            "budget_exceeded": self.budget_exceeded(),
        }


@dataclass
class FederatedUpdate:
    node_id: str
    weights: Dict[str, np.ndarray]
    n_samples: int
    carbon_intensity: float
    timestamp: float = field(default_factory=time.time)


class FederatedAggregator:
    """Carbon-weighted FedAvg with DP noise and shape validation."""

    def __init__(self, dp_sigma: float = 1e-3, staleness_s: float = 3600.0,
                 target_delta: float = 1e-5, max_epsilon: float = 10.0):
        self.dp_sigma = dp_sigma
        self.staleness_s = staleness_s
        self._updates: Dict[str, FederatedUpdate] = {}
        self._global: Optional[Dict[str, np.ndarray]] = None
        self._lock = threading.Lock()
        self._shapes: Dict[str, Tuple[int, ...]] = {}
        self.rejected: List[Dict[str, Any]] = []
        self.accountant = SimpleRDPAccountant(target_delta=target_delta,
                                              max_epsilon=max_epsilon)

    def submit(self, u: FederatedUpdate) -> bool:
        with self._lock:
            for k, v in u.weights.items():
                if not isinstance(v, np.ndarray) or v.size == 0:
                    return False
                if k in self._shapes and v.shape != self._shapes[k]:
                    self.rejected.append({
                        "node_id": u.node_id, "key": k,
                        "expected": self._shapes[k], "got": v.shape, "t": time.time(),
                    })
                    return False
                self._shapes.setdefault(k, v.shape)
            if self.accountant.budget_exceeded():
                return False
            if self.dp_sigma > 0:
                noisy = {k: v + np.random.normal(0.0, self.dp_sigma, size=v.shape)
                         for k, v in u.weights.items()}
                self.accountant.step(self.dp_sigma, q=1.0)
                u = FederatedUpdate(
                    node_id=u.node_id, weights=noisy,
                    n_samples=u.n_samples, carbon_intensity=u.carbon_intensity,
                    timestamp=u.timestamp,
                )
            self._updates[u.node_id] = u
            return True

    def _fresh(self) -> List[FederatedUpdate]:
        now = time.time()
        return [u for u in self._updates.values() if (now - u.timestamp) <= self.staleness_s]

    def aggregate(self) -> Optional[Dict[str, np.ndarray]]:
        with self._lock:
            fresh = self._fresh()
            if not fresh:
                return self._global
            weights = np.array(
                [u.n_samples / max(u.carbon_intensity, 1.0) for u in fresh],
                dtype=np.float64,
            )
            weights /= max(weights.sum(), 1e-12)
            keys = fresh[0].weights.keys()
            agg: Dict[str, np.ndarray] = {}
            for k in keys:
                try:
                    stacked = np.stack([u.weights[k] for u in fresh], axis=0)
                except (KeyError, ValueError):
                    continue
                agg[k] = np.tensordot(weights, stacked, axes=([0], [0]))
            self._global = agg
            return agg

    def global_weights(self) -> Optional[Dict[str, np.ndarray]]:
        return self._global

    def get_dp_state(self) -> Dict[str, Any]:
        return {**self.accountant.get_state(), "rejected_updates": len(self.rejected)}
