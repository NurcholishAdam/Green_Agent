"""
quantum.py — Quantum-inspired distillation (Enhancement 1).

Small state-vector simulation blended with a carbon-aware prior.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Optional, Sequence

import numpy as np


class QuantumInspiredTeacher:
    """Deterministic state-vector simulation."""

    def __init__(self, n_qubits: int = 5, seed: int = 0):
        self.n_qubits = max(1, int(n_qubits))
        self.rng = np.random.default_rng(seed)
        self._params = self.rng.normal(size=(self.n_qubits, 2)) * 0.5

    def _ry(self, state: np.ndarray, theta: float, q: int) -> np.ndarray:
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

    def _simulate(self, features: np.ndarray, n: int) -> np.ndarray:
        feat = np.asarray(features, dtype=np.float64).ravel()
        if feat.size == 0:
            feat = np.zeros(self.n_qubits, dtype=np.float64)
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128)
        state[0] = 1.0
        for q in range(self.n_qubits):
            angle = float(self._params[q, 0] * feat[q % feat.size] + self._params[q, 1])
            state = self._ry(state, angle, q)
        probs_full = np.abs(state) ** 2
        probs = np.zeros(n, dtype=np.float64)
        for i, p in enumerate(probs_full):
            probs[i % n] += p
        return probs / max(probs.sum(), 1e-12)

    def teacher_probs(self, features: np.ndarray, n_candidates: int) -> np.ndarray:
        if n_candidates <= 0:
            return np.zeros(0)
        return self._simulate(np.asarray(features, dtype=np.float64), n_candidates)

    def teacher_probs_from_candidates(
        self,
        features: np.ndarray,
        candidates: Sequence[Dict[str, Any]],
    ) -> np.ndarray:
        n = len(candidates)
        if n == 0:
            return np.zeros(0)
        raw = self._simulate(features, n)
        carbon_prior = np.array(
            [1.0 / (1.0 + float(c.get("carbon_g", 1.0))) for c in candidates],
            dtype=np.float64,
        )
        carbon_prior /= max(carbon_prior.sum(), 1e-12)
        blended = 0.6 * raw + 0.4 * carbon_prior
        return blended / max(blended.sum(), 1e-12)


class DistillationEnsemble:
    """Blends a quantum teacher with an existing probability vector."""

    def __init__(self, quantum: QuantumInspiredTeacher, alpha: float = 0.5):
        self.quantum = quantum
        self.alpha = alpha

    def blend(self, features: np.ndarray, other_probs: Optional[np.ndarray], n: int) -> np.ndarray:
        q = self.quantum.teacher_probs(features, n)
        if other_probs is None or len(other_probs) != n:
            return q
        return self.alpha * q + (1.0 - self.alpha) * np.asarray(other_probs)
