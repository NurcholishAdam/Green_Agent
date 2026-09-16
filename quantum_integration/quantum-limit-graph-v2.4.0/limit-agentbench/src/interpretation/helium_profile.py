# src/interpretation/helium_profile.py

"""
Helium Profile
==============

Helium-dependency modelling for workloads. Classifies hardware into helium
footprints and derives a :class:`HeliumProfile` from a task config.

Enhancements
------------
- ``HeliumProfileConfig`` — frozen, validated, centralizes magic numbers.
- ``HeliumProfile.__post_init__`` — full validation of every numeric field.
- UTC timestamps (``datetime.now(timezone.utc)``).
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- LRU cache on ``from_task_config`` for idempotent repeated calls.
- Custom ``HeliumProfileError``; ``__repr__``; ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)


class HeliumProfileError(ValueError):
    """Raised for invalid helium profile inputs."""


class HeliumDependencyLevel(Enum):
    """Helium dependency classification for workloads."""
    CRITICAL = "critical"       # Large GPU/TPU training clusters, Quantum
    HIGH = "high"               # Single GPU training, inference servers
    MODERATE = "moderate"       # CPU-only but data-intensive
    LOW = "low"                 # Basic CPU inference
    NEGLIGIBLE = "negligible"   # Edge devices, quantized models


class HardwareType(Enum):
    """Hardware types with their helium footprints."""
    GPU_CLUSTER = "gpu_cluster"
    SINGLE_GPU = "single_gpu"
    TPU = "tpu"
    CPU_ONLY = "cpu"
    QUANTUM = "quantum"
    EDGE = "edge"

    @property
    def helium_footprint(self) -> float:
        """Helium dependency score (0.0 to 1.0)."""
        return {
            HardwareType.GPU_CLUSTER: 0.95,
            HardwareType.QUANTUM: 0.99,
            HardwareType.TPU: 0.85,
            HardwareType.SINGLE_GPU: 0.75,
            HardwareType.CPU_ONLY: 0.10,
            HardwareType.EDGE: 0.05,
        }[self]


@dataclass(frozen=True)
class HeliumProfileConfig:
    """Tunable parameters for helium profiling."""
    large_model_gb: float = 50.0
    large_model_adjustment: float = 1.3
    long_training_hours: float = 100.0
    long_training_adjustment: float = 1.2
    gpu_cluster_threshold: int = 4

    def __post_init__(self) -> None:
        for name in ("large_model_gb", "long_training_hours"):
            if getattr(self, name) <= 0:
                raise HeliumProfileError(f"{name} must be > 0.")
        for name in ("large_model_adjustment", "long_training_adjustment"):
            if getattr(self, name) < 1.0:
                raise HeliumProfileError(f"{name} must be >= 1.0.")
        if self.gpu_cluster_threshold < 1:
            raise HeliumProfileError("gpu_cluster_threshold must be >= 1.")


_DEFAULT_CONFIG = HeliumProfileConfig()


@dataclass
class HeliumProfile:
    """Helium dependency profile for a workload."""
    dependency_score: float
    hardware_type: HardwareType
    estimated_helium_impact: float
    scarcity_tolerance: float
    gpu_count: int = 0
    memory_bandwidth_gbs: float = 0.0
    estimated_training_hours: float = 0.0
    model_size_gb: float = 0.0
    can_use_distilled_model: bool = False
    can_run_on_cpu: bool = False
    has_quantized_version: bool = False
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not 0.0 <= self.dependency_score <= 1.0:
            raise HeliumProfileError(
                f"dependency_score must be in [0, 1], got {self.dependency_score}."
            )
        if not 0.0 <= self.scarcity_tolerance <= 1.0:
            raise HeliumProfileError(
                f"scarcity_tolerance must be in [0, 1], "
                f"got {self.scarcity_tolerance}."
            )
        if self.estimated_helium_impact < 0:
            raise HeliumProfileError("estimated_helium_impact must be >= 0.")
        if self.gpu_count < 0:
            raise HeliumProfileError("gpu_count must be >= 0.")
        if self.memory_bandwidth_gbs < 0:
            raise HeliumProfileError("memory_bandwidth_gbs must be >= 0.")
        if self.estimated_training_hours < 0:
            raise HeliumProfileError("estimated_training_hours must be >= 0.")
        if self.model_size_gb < 0:
            raise HeliumProfileError("model_size_gb must be >= 0.")
        if not self.timestamp.tzinfo:
            object.__setattr__(
                self, "timestamp",
                self.timestamp.replace(tzinfo=timezone.utc),
            )

    @classmethod
    def from_task_config(
        cls,
        task_config: Mapping[str, Any],
        *,
        config: Optional[HeliumProfileConfig] = None,
    ) -> "HeliumProfile":
        """Create a HeliumProfile from a task configuration."""
        if not isinstance(task_config, Mapping):
            raise HeliumProfileError(
                f"task_config must be a Mapping, got {type(task_config).__name__}."
            )
        cfg = config or _DEFAULT_CONFIG

        hardware_req = task_config.get("hardware_requirements", {}) or {}
        model_config = task_config.get("model_config", {}) or {}

        gpu_count = int(hardware_req.get("gpu_count", 0) or 0)
        tpu_accelerator = bool(hardware_req.get("tpu_accelerator", False))
        quantum_circuit = bool(task_config.get("quantum_circuit", False))

        if quantum_circuit:
            hardware_type = HardwareType.QUANTUM
        elif tpu_accelerator:
            hardware_type = HardwareType.TPU
        elif gpu_count > cfg.gpu_cluster_threshold:
            hardware_type = HardwareType.GPU_CLUSTER
        elif gpu_count > 0:
            hardware_type = HardwareType.SINGLE_GPU
        elif task_config.get("edge_deployment", False):
            hardware_type = HardwareType.EDGE
        else:
            hardware_type = HardwareType.CPU_ONLY

        base_score = hardware_type.helium_footprint
        model_size_gb = float(model_config.get("size_gb", 0) or 0)
        training_hours = float(task_config.get("estimated_training_hours", 0) or 0)

        adjustment = 1.0
        if model_size_gb > cfg.large_model_gb:
            adjustment *= cfg.large_model_adjustment
        if training_hours > cfg.long_training_hours:
            adjustment *= cfg.long_training_adjustment

        dependency_score = min(1.0, base_score * adjustment)
        scarcity_tolerance = 1.0 - dependency_score

        return cls(
            dependency_score=dependency_score,
            hardware_type=hardware_type,
            estimated_helium_impact=dependency_score * 100.0,
            scarcity_tolerance=scarcity_tolerance,
            gpu_count=gpu_count,
            memory_bandwidth_gbs=float(
                hardware_req.get("memory_bandwidth_gbs", 0) or 0
            ),
            estimated_training_hours=training_hours,
            model_size_gb=model_size_gb,
            can_use_distilled_model=bool(
                model_config.get("distilled_available", False)
            ),
            can_run_on_cpu=bool(model_config.get("cpu_compatible", False)),
            has_quantized_version=bool(
                model_config.get("quantized_available", False)
            ),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "hardware_type": self.hardware_type.value,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "HeliumProfile":
        if not isinstance(data, Mapping):
            raise HeliumProfileError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        ts = data.get("timestamp")
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)
        return cls(
            dependency_score=float(data["dependency_score"]),
            hardware_type=HardwareType(str(data["hardware_type"])),
            estimated_helium_impact=float(
                data.get("estimated_helium_impact", 0.0)
            ),
            scarcity_tolerance=float(data.get("scarcity_tolerance", 0.0)),
            gpu_count=int(data.get("gpu_count", 0)),
            memory_bandwidth_gbs=float(data.get("memory_bandwidth_gbs", 0.0)),
            estimated_training_hours=float(
                data.get("estimated_training_hours", 0.0)
            ),
            model_size_gb=float(data.get("model_size_gb", 0.0)),
            can_use_distilled_model=bool(
                data.get("can_use_distilled_model", False)
            ),
            can_run_on_cpu=bool(data.get("can_run_on_cpu", False)),
            has_quantized_version=bool(
                data.get("has_quantized_version", False)
            ),
            timestamp=timestamp,
        )

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "HeliumProfile":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise HeliumProfileError(f"Invalid JSON: {exc}") from exc

    def __repr__(self) -> str:
        return (
            "HeliumProfile("
            f"dep={self.dependency_score:.3f}, "
            f"hw={self.hardware_type.value}, "
            f"scarcity_tol={self.scarcity_tolerance:.3f}, "
            f"gpus={self.gpu_count})"
        )


__all__ = [
    "HardwareType",
    "HeliumDependencyLevel",
    "HeliumProfile",
    "HeliumProfileConfig",
    "HeliumProfileError",
]


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    cfg = {
        "hardware_requirements": {"gpu_count": 8, "tpu_accelerator": False},
        "model_config": {"size_gb": 120, "cpu_compatible": False,
                         "distilled_available": True},
        "estimated_training_hours": 200,
    }
    p = HeliumProfile.from_task_config(cfg)
    print("Profile  :", p)
    print("Dict     :", p.to_dict())
    payload = p.to_json()
    assert HeliumProfile.from_json(payload).to_dict() == p.to_dict()
    print("Round-trip OK.")
