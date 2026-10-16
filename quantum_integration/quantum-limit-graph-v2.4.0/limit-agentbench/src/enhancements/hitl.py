"""
hitl.py — Human-in-the-loop and active learning (Enhancement 10).
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np


@dataclass
class HITLRequest:
    request_id: str
    reason: str
    metric_name: str
    value: float
    uncertainty: float
    carbon_price: float
    timestamp: float = field(default_factory=time.time)


class UncertaintyEstimator:
    """Rolling-window statistics per metric."""

    def __init__(self, window: int = 32):
        self.window = window
        self._history: Dict[str, deque] = {}

    def observe(self, metrics: Dict[str, Any]) -> None:
        for k, v in metrics.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                d = self._history.setdefault(k, deque(maxlen=self.window))
                d.append(float(v))

    def std(self, key: str) -> float:
        d = self._history.get(key)
        if d is None or len(d) < 3:
            return 0.0
        return float(np.std(np.asarray(d, dtype=np.float64)))

    def mean(self, key: str) -> float:
        d = self._history.get(key)
        if d is None or not d:
            return 0.0
        return float(np.mean(np.asarray(d, dtype=np.float64)))

    def z_score(self, key: str, value: float) -> float:
        s = self.std(key)
        if s < 1e-9:
            return 0.0
        return (value - self.mean(key)) / s

    def is_anomalous(self, key: str, value: float, z_thresh: float = 3.0) -> Tuple[bool, float]:
        z = abs(self.z_score(key, value))
        return z > z_thresh, z

    def composite_uncertainty(self, keys: Sequence[str]) -> float:
        vals = [self.std(k) for k in keys if self.std(k) > 0]
        return float(np.mean(vals)) if vals else 0.0

    def snapshot_stats(self) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        for k, d in self._history.items():
            if len(d) < 2:
                continue
            arr = np.asarray(d, dtype=np.float64)
            out[k] = {"mean": float(arr.mean()), "std": float(arr.std())}
        return out


class HumanInTheLoopGate:
    """Sync approval gate with audit trail."""

    def __init__(
        self,
        approver: Optional[Callable[[HITLRequest], bool]] = None,
        timeout_s: float = 2.0,
        auto_approve_on_timeout: bool = True,
    ):
        self.approver = approver
        self.timeout_s = timeout_s
        self.auto_approve_on_timeout = auto_approve_on_timeout
        self.audit: deque = deque(maxlen=1024)

    def request_sync(self, req: HITLRequest) -> bool:
        if self.approver is None:
            self.audit.append({"request_id": req.request_id, "approved": True,
                               "reason": "no_approver"})
            return True
        try:
            approved = bool(self.approver(req))
        except Exception as e:
            self.audit.append({"request_id": req.request_id, "approved": False,
                               "error": str(e)})
            return False
        self.audit.append({"request_id": req.request_id, "approved": approved,
                           "reason": req.reason})
        return approved


class ActiveLearningSampler:
    """Uncertainty-prioritized anomaly buffer."""

    def __init__(self, capacity: int = 256):
        self.capacity = capacity
        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def maybe_store(self, record: Dict[str, Any], uncertainty: float,
                    threshold: float = 0.5) -> bool:
        if uncertainty < threshold:
            return False
        with self._lock:
            if len(self._buffer) >= self.capacity:
                self._buffer.pop(0)
            self._buffer.append({**record, "uncertainty": uncertainty, "t": time.time()})
        return True

    def sample(self, k: int = 8) -> List[Dict[str, Any]]:
        with self._lock:
            if not self._buffer:
                return []
            k = min(k, len(self._buffer))
            return random.sample(self._buffer, k)

    def __len__(self) -> int:
        with self._lock:
            return len(self._buffer)
