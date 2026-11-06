"""
src/types.py — every schema referenced by the four delivered modules.

Each dataclass validates its fields in `__post_init__`; invalid values raise
`ValueError` — no silent fallbacks, no `None` sentinels where a range exists.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

__all__ = [
    "TaskKind", "ModelArch", "DecisionType", "Method", "Precision",
    "WorkloadProfile", "DataOptimizationResult", "Decision",
    "CarbonBudget", "BenchmarkRecord", "LeaderboardEntry",
]

_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


# --- Enums ---------------------------------------------------------------

class TaskKind(str, Enum):
    FINE_TUNING = "fine_tuning"
    INFERENCE   = "inference"
    AGENT       = "agent"
    BENCHMARK   = "benchmark"


class ModelArch(str, Enum):
    TRANSFORMER = "transformer"
    CNN         = "cnn"
    RNN         = "rnn"
    HYBRID      = "hybrid"


class DecisionType(str, Enum):
    EXECUTE_NOW       = "execute_now"
    DEFER_TO_OPTIMAL  = "defer_to_optimal"
    ROUTE_TO_NODE     = "route_to_node"
    REJECT_BUDGET     = "reject_budget"


class Method(str, Enum):
    FULL_FINETUNE = "full_finetune"
    LORA          = "lora"
    ADAPTER       = "adapter"
    QUANTIZE      = "quantize"


class Precision(str, Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"


# --- Dataclasses ---------------------------------------------------------

@dataclass(frozen=True, slots=True)
class WorkloadProfile:
    """Returned by WorkloadInterpreter.interpret. See report §1.1."""
    task_id: str
    task_type: TaskKind
    model_architecture: ModelArch
    dataset_size: int
    estimated_energy_kwh: float
    estimated_flops: float
    dag_steps: int
    optimization_potential: float
    candidates: tuple[str, ...]

    def __post_init__(self) -> None:
        if not _ID_RE.match(self.task_id):
            raise ValueError(f"invalid task_id: {self.task_id!r}")
        if self.dataset_size < 0:
            raise ValueError("dataset_size must be ≥ 0")
        if self.estimated_energy_kwh < 0:
            raise ValueError("estimated_energy_kwh must be ≥ 0")
        if self.estimated_flops < 0:
            raise ValueError("estimated_flops must be ≥ 0")
        if self.dag_steps < 0:
            raise ValueError("dag_steps must be ≥ 0")
        if not (0.0 <= self.optimization_potential <= 1.0):
            raise ValueError("optimization_potential must be in [0, 1]")
        if not all(isinstance(c, str) and c for c in self.candidates):
            raise ValueError("candidates must be non-empty strings")

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["task_type"] = self.task_type.value
        d["model_architecture"] = self.model_architecture.value
        d["candidates"] = list(self.candidates)
        return d


@dataclass(frozen=True, slots=True)
class DataOptimizationResult:
    """Returned by SyntheticDataOptimizer.optimize. See report §1.2."""
    original_size: int
    optimized_size: int
    compression_ratio: float
    synthetic_added: int
    quality_retention: float
    estimated_energy_saved_kwh: float
    strategies_applied: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.original_size <= 0:
            raise ValueError("original_size must be > 0")
        if self.optimized_size < 0:
            raise ValueError("optimized_size must be ≥ 0")
        if self.optimized_size > self.original_size:
            raise ValueError("optimized_size cannot exceed original_size")
        expected = (
            self.original_size / self.optimized_size
            if self.optimized_size else float("inf")
        )
        if abs(self.compression_ratio - expected) > 1e-3:
            raise ValueError(
                f"compression_ratio ({self.compression_ratio}) does not match "
                f"original/optimized ({expected:.3f})"
            )
        if self.synthetic_added < 0:
            raise ValueError("synthetic_added must be ≥ 0")
        if not (0.0 <= self.quality_retention <= 1.0):
            raise ValueError("quality_retention must be in [0, 1]")
        if self.estimated_energy_saved_kwh < 0:
            raise ValueError("estimated_energy_saved_kwh must be ≥ 0")

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["strategies_applied"] = list(self.strategies_applied)
        return d


@dataclass(frozen=True, slots=True)
class Decision:
    """Returned by CarbonAwareDecisionCore.make_decision. See report §1.3."""
    decision_type: DecisionType
    when_iso: str | None
    where_node: str | None
    how_method: Method
    reasoning: str
    carbon_saved_kg: float
    carbon_saved_pct: float
    use_flexgen: bool
    flexgen_precision: Precision | None

    def __post_init__(self) -> None:
        if self.decision_type is DecisionType.DEFER_TO_OPTIMAL and not self.when_iso:
            raise ValueError("DEFER_TO_OPTIMAL requires when_iso")
        if self.decision_type is DecisionType.ROUTE_TO_NODE and not self.where_node:
            raise ValueError("ROUTE_TO_NODE requires where_node")
        if self.carbon_saved_kg < 0:
            raise ValueError("carbon_saved_kg must be ≥ 0")
        if not (0.0 <= self.carbon_saved_pct <= 1.0):
            raise ValueError("carbon_saved_pct must be in [0, 1]")
        if self.use_flexgen and self.flexgen_precision is None:
            raise ValueError("use_flexgen=True requires flexgen_precision")
        if not self.use_flexgen and self.flexgen_precision is not None:
            raise ValueError("use_flexgen=False must not set flexgen_precision")

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["decision_type"] = self.decision_type.value
        d["how_method"] = self.how_method.value
        d["flexgen_precision"] = (
            self.flexgen_precision.value if self.flexgen_precision else None
        )
        return d


@dataclass(frozen=True, slots=True)
class CarbonBudget:
    """Hard limits passed to CarbonAwareDecisionCore."""
    max_energy_kwh: float
    max_carbon_kg: float
    max_latency_ms: int

    def __post_init__(self) -> None:
        if self.max_energy_kwh <= 0:
            raise ValueError("max_energy_kwh must be > 0")
        if self.max_carbon_kg <= 0:
            raise ValueError("max_carbon_kg must be > 0")
        if self.max_latency_ms <= 0:
            raise ValueError("max_latency_ms must be > 0")


@dataclass(frozen=True, slots=True)
class BenchmarkRecord:
    """One row stored in BenchmarkIntelligence."""
    model_name: str
    accuracy: float
    energy_kwh: float
    carbon_kgco2e: float
    latency_ms: float
    recorded_at_iso: str
    modp_score: float | None = None

    def __post_init__(self) -> None:
        if not _ID_RE.match(self.model_name):
            raise ValueError(f"invalid model_name: {self.model_name!r}")
        if not (0.0 <= self.accuracy <= 1.0):
            raise ValueError("accuracy must be in [0, 1]")
        if self.energy_kwh < 0:
            raise ValueError("energy_kwh must be ≥ 0")
        if self.carbon_kgco2e < 0:
            raise ValueError("carbon_kgco2e must be ≥ 0")
        if self.latency_ms < 0:
            raise ValueError("latency_ms must be ≥ 0")
        if self.modp_score is not None and not (0.0 <= self.modp_score <= 1.0):
            raise ValueError("modp_score must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class LeaderboardEntry:
    """One row returned by BenchmarkIntelligence.get_leaderboard."""
    model_name: str
    accuracy: float
    energy_kwh: float
    carbon_kgco2e: float
    latency_ms: float
    efficiency_score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
