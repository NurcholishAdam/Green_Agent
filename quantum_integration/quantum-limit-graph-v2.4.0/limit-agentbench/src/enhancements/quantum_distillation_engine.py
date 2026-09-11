"""
quantum_distillation_engine.py
Quantum-inspired multi-teacher distillation.
"""
from __future__ import annotations
import asyncio
import math
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional

import numpy as np


class QuantumDistillationEngine:
    """
    Multi-teacher superposition:
        amplitude_k   = sqrt(softmax_k)
        target_k      = amplitude_k^2 / sum(amplitude^2)
    """

    def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                 n_actions: int = 5):
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy: List[float] = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    # ------------------------------------------------------------------
    # Teacher registration
    # ------------------------------------------------------------------
    def register_teacher(self, name: str, policy: List[float]) -> None:
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[name] = [p / s for p in policy]

    # ------------------------------------------------------------------
    # Superposition
    # ------------------------------------------------------------------
    def _softmax(self, x: List[float], temp: float) -> List[float]:
        m = max(x)
        exps = [math.exp((v - m) / max(temp, 1e-6)) for v in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _superpose(self) -> List[float]:
        if not self.teachers:
            return list(self.student_policy)
        n = self.n_actions
        accum = [0.0] * n
        for pol in self.teachers.values():
            for i in range(min(n, len(pol))):
                accum[i] += math.sqrt(max(pol[i], 1e-9))
        accum = [a / len(self.teachers) for a in accum]
        sq = [a * a for a in accum]
        s = sum(sq) or 1.0
        return [x / s for x in sq]

    # ------------------------------------------------------------------
    # Training step (KL-style gradient on the student)
    # ------------------------------------------------------------------
    async def step(self, storage, student_id: str = "mopd_student") -> Dict[str, Any]:
        target = self._softmax(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]

        # Persist each teacher superposition weight
        for tid, pol in self.teachers.items():
            weight = pol[0] if pol else 0.0
            amplitude = math.sqrt(max(weight, 1e-9))
            kl = sum(
                t * math.log(max(t, 1e-9) / max(s, 1e-9))
                for t, s in zip(target, self.student_policy)
            )
            await asyncio.to_thread(
                storage.save_teacher_superposition,
                student_id, tid, weight, self.temperature, amplitude, kl,
            )

        entry = {"target": target, "student": list(self.student_policy),
                 "ts": datetime.now(timezone.utc).isoformat()}
        self.history.append(entry)
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)
