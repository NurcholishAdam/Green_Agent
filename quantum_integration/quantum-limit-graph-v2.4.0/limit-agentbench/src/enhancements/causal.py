"""
causal.py — Causal RL for policy adaptation (Enhancement 2).

Ridge-regression SCM with action-space padding. Sufficient statistics are
preserved across candidate-count changes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import numpy as np


@dataclass
class CausalTransition:
    state: np.ndarray
    action: int
    reward: float
    next_state: np.ndarray
    context: Dict[str, float] = field(default_factory=dict)


class CausalCounterfactualEstimator:
    """Ridge SCM with padding-based action resize and cached solve."""

    def __init__(self, state_dim: int, n_actions: int, ridge: float = 1e-3):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.ridge = ridge
        d = state_dim + n_actions + 1
        self._A = np.eye(d) * ridge
        self._b = np.zeros(d)
        self._n = 0
        self._theta: Optional[np.ndarray] = None
        self._dirty = True

    def _features(self, s: np.ndarray, a: int) -> np.ndarray:
        one_hot = np.zeros(self.n_actions)
        one_hot[a % self.n_actions] = 1.0
        s = np.asarray(s, dtype=np.float64).ravel()[: self.state_dim]
        if s.size < self.state_dim:
            s = np.pad(s, (0, self.state_dim - s.size))
        return np.concatenate([s, one_hot, [1.0]])

    def _ensure_actions(self, n_actions: int) -> None:
        if n_actions == self.n_actions:
            return
        new_d = self.state_dim + n_actions + 1
        new_A = np.eye(new_d) * self.ridge
        new_b = np.zeros(new_d)
        new_A[: self.state_dim, : self.state_dim] = self._A[: self.state_dim, : self.state_dim]
        new_b[: self.state_dim] = self._b[: self.state_dim]
        a_min = min(self.n_actions, n_actions)
        old_off = self.state_dim
        new_off = self.state_dim
        new_A[new_off:new_off + a_min, new_off:new_off + a_min] = \
            self._A[old_off:old_off + a_min, old_off:old_off + a_min]
        new_b[new_off:new_off + a_min] = self._b[old_off:old_off + a_min]
        new_A[-1, -1] = self._A[-1, -1]
        new_b[-1] = self._b[-1]
        self._A = new_A
        self._b = new_b
        self.n_actions = n_actions
        self._dirty = True

    def update(self, t: CausalTransition) -> None:
        x = self._features(t.state, t.action)
        self._A += np.outer(x, x)
        self._b += t.reward * x
        self._n += 1
        self._dirty = True

    def _solve(self) -> None:
        if not self._dirty:
            return
        try:
            self._theta = np.linalg.solve(self._A, self._b)
        except np.linalg.LinAlgError:
            self._theta = None
        self._dirty = False

    def predict(self, s: np.ndarray, a: int) -> float:
        if self._n < 2:
            return 0.0
        self._solve()
        if self._theta is None:
            return 0.0
        return float(self._features(s, a) @ self._theta)

    def counterfactuals(self, s: np.ndarray, n_actions: int) -> Dict[int, float]:
        self._ensure_actions(n_actions)
        return {a: self.predict(s, a) for a in range(n_actions)}

    def best_counterfactual(self, s: np.ndarray, n_actions: int):
        cf = self.counterfactuals(s, n_actions)
        if not cf:
            return -1, 0.0
        best = max(cf, key=cf.get)
        return best, cf[best]

    def export_state(self) -> Dict[str, Any]:
        return {
            "schema": "causal_scm_v3",
            "state_dim": self.state_dim,
            "n_actions": self.n_actions,
            "A": self._A.tolist(),
            "b": self._b.tolist(),
            "n": self._n,
        }

    def import_state(self, payload: Dict[str, Any]) -> bool:
        if payload.get("schema") != "causal_scm_v3":
            return False
        try:
            A = np.asarray(payload["A"], dtype=np.float64)
            b = np.asarray(payload["b"], dtype=np.float64)
            if A.ndim != 2 or b.ndim != 1 or A.shape[0] != A.shape[1] or A.shape[0] != b.size:
                return False
            self._A = A
            self._b = b
            self._n = int(payload.get("n", 0))
            self.state_dim = int(payload["state_dim"])
            self.n_actions = int(payload["n_actions"])
            self._dirty = True
            return True
        except Exception:
            return False
