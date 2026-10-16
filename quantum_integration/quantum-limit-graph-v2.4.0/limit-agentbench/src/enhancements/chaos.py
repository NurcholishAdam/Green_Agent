"""
chaos.py — Resilience and chaos testing (Enhancement 9).

Bounded event log. Deterministic per seed.
"""

from __future__ import annotations

import random
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class ChaosConfig:
    fault_prob: float = 0.0
    latency_inject_ms: float = 0.0
    latency_inject_prob: float = 0.3
    carbon_spike_prob: float = 0.0
    carbon_spike_factor: float = 1.5
    sensor_noise_std: float = 0.0
    seed: int = 0

    def enabled(self) -> bool:
        return (
            self.fault_prob > 0
            or (self.latency_inject_ms > 0 and self.latency_inject_prob > 0)
            or self.carbon_spike_prob > 0
            or self.sensor_noise_std > 0
        )


class ChaosEngineer:
    """Deterministic chaos injector with a bounded event log."""

    def __init__(self, config: Optional[ChaosConfig] = None, max_events: int = 4096):
        self.config = config or ChaosConfig()
        self.rng = random.Random(self.config.seed)
        self.events: deque = deque(maxlen=max_events)

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

    def apply_sensor_noise(self, value: float) -> float:
        if self.config.sensor_noise_std > 0:
            return value + self.rng.gauss(0.0, self.config.sensor_noise_std)
        return value

    def recent_events(self, k: int = 16) -> List[Dict[str, Any]]:
        return list(self.events)[-k:]
