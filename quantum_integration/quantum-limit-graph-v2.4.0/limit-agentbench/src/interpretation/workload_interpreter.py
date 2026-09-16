# src/interpretation/workload_interpreter.py

"""
Workload Interpreter
====================

Converts raw task JSON into a structured :class:`WorkloadProfile`, including
helium-dependency analysis.

Enhancements
------------
- ``InterpretationConfig`` — frozen, validated, centralizes magic numbers.
- ``WorkloadProfile.__post_init__`` — full validation.
- UTC timestamps.
- ``RLock``-guarded ``WorkloadInterpreter`` with bounded history and stats.
- Strict / non-strict modes; custom ``InterpretationError``.
- Serialization on both ``WorkloadProfile`` and ``WorkloadInterpreter``.
- Lazy ``%s`` logging; ``__repr__``; ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional

from .helium_profile import (
    HeliumProfile,
    HeliumProfileConfig,
    HeliumProfileError,
    HardwareType,
)

logger = logging.getLogger(__name__)


class InterpretationError(ValueError):
    """Raised for invalid interpretation inputs or configuration."""


@dataclass(frozen=True)
class InterpretationConfig:
    """Tunable parameters for workload interpretation."""
    enable_helium_analysis: bool = True
    model_size_scale_gb: float = 100.0
    data_volume_scale_gb: float = 1000.0
    base_energy_kwh: float = 0.1
    energy_per_gpu_kwh: float = 0.3
    carbon_intensity_kg_per_kwh: float = 0.4
    high_dependency_cutoff: float = 0.8
    high_dependency_energy_penalty: float = 1.2
    max_history: int = 1000

    def __post_init__(self) -> None:
        for name in (
            "model_size_scale_gb", "data_volume_scale_gb",
            "base_energy_kwh", "carbon_intensity_kg_per_kwh",
        ):
            if getattr(self, name) <= 0:
                raise InterpretationError(f"{name} must be > 0.")
        if self.energy_per_gpu_kwh < 0:
            raise InterpretationError("energy_per_gpu_kwh must be >= 0.")
        if not 0.0 <= self.high_dependency_cutoff <= 1.0:
            raise InterpretationError(
                "high_dependency_cutoff must be in [0, 1]."
            )
        if self.high_dependency_energy_penalty < 1.0:
            raise InterpretationError(
                "high_dependency_energy_penalty must be >= 1.0."
            )
        if self.max_history <= 0:
            raise InterpretationError("max_history must be > 0.")


@dataclass
class WorkloadProfile:
    """Enhanced workload profile with helium awareness."""
    task_id: str
    complexity_score: float
    energy_estimate_kwh: float
    carbon_estimate_kg: float
    resource_requirements: Dict[str, Any]
    deferrable: bool
    priority: int
    helium_profile: Optional[HeliumProfile] = None
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    source: str = "workload_interpreter"

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, str) or not self.task_id:
            raise InterpretationError("task_id must be a non-empty string.")
        if not 0.0 <= self.complexity_score <= 1.0:
            raise InterpretationError(
                f"complexity_score must be in [0, 1], got {self.complexity_score}."
            )
        if self.energy_estimate_kwh < 0:
            raise InterpretationError("energy_estimate_kwh must be >= 0.")
        if self.carbon_estimate_kg < 0:
            raise InterpretationError("carbon_estimate_kg must be >= 0.")
        if not 1 <= self.priority <= 10:
            raise InterpretationError(
                f"priority must be in [1, 10], got {self.priority}."
            )
        if not isinstance(self.resource_requirements, dict):
            raise InterpretationError("resource_requirements must be a dict.")
        if not self.timestamp.tzinfo:
            object.__setattr__(
                self, "timestamp",
                self.timestamp.replace(tzinfo=timezone.utc),
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "complexity_score": self.complexity_score,
            "energy_estimate_kwh": self.energy_estimate_kwh,
            "carbon_estimate_kg": self.carbon_estimate_kg,
            "resource_requirements": dict(self.resource_requirements),
            "deferrable": self.deferrable,
            "priority": self.priority,
            "helium_profile": (
                self.helium_profile.to_dict() if self.helium_profile else None
            ),
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "WorkloadProfile":
        if not isinstance(data, Mapping):
            raise InterpretationError(
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
        hp_raw = data.get("helium_profile")
        hp = HeliumProfile.from_dict(hp_raw) if hp_raw else None
        return cls(
            task_id=str(data["task_id"]),
            complexity_score=float(data["complexity_score"]),
            energy_estimate_kwh=float(data["energy_estimate_kwh"]),
            carbon_estimate_kg=float(data["carbon_estimate_kg"]),
            resource_requirements=dict(data.get("resource_requirements", {})),
            deferrable=bool(data.get("deferrable", True)),
            priority=int(data.get("priority", 5)),
            helium_profile=hp,
            timestamp=timestamp,
            source=str(data.get("source", "workload_interpreter")),
        )

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "WorkloadProfile":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise InterpretationError(f"Invalid JSON: {exc}") from exc

    def __repr__(self) -> str:
        helium = (
            f"{self.helium_profile.dependency_score:.3f}"
            if self.helium_profile else "n/a"
        )
        return (
            "WorkloadProfile("
            f"task_id={self.task_id!r}, "
            f"complexity={self.complexity_score:.3f}, "
            f"energy={self.energy_estimate_kwh:.4f}kWh, "
            f"carbon={self.carbon_estimate_kg:.4f}kg, "
            f"priority={self.priority}, "
            f"helium={helium})"
        )


class WorkloadInterpreter:
    """Enhanced workload interpreter with helium dependency scoring."""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        interpretation_config: Optional[InterpretationConfig] = None,
        helium_config: Optional[HeliumProfileConfig] = None,
        strict: bool = True,
    ) -> None:
        if interpretation_config is not None:
            self._config = interpretation_config
        else:
            cfg = dict(config or {})
            kwargs: Dict[str, Any] = {}
            if "enable_helium_analysis" in cfg:
                kwargs["enable_helium_analysis"] = bool(
                    cfg["enable_helium_analysis"]
                )
            self._config = InterpretationConfig(**kwargs)

        self._helium_config = helium_config or HeliumProfileConfig()
        self._strict = bool(strict)
        # Legacy attribute preserved.
        self.config: Dict[str, Any] = dict(config or {})
        self.enable_helium_analysis: bool = self._config.enable_helium_analysis

        self._lock = threading.RLock()
        self._history: Deque[WorkloadProfile] = deque(
            maxlen=self._config.max_history
        )

        logger.debug(
            "WorkloadInterpreter initialized "
            "(helium_analysis=%s, strict=%s)",
            self.enable_helium_analysis,
            self._strict,
        )

    @property
    def config_typed(self) -> InterpretationConfig:
        return self._config

    @property
    def history(self) -> List[WorkloadProfile]:
        with self._lock:
            return list(self._history)

    def analyze_task(
        self,
        task_json: Mapping[str, Any],
        *,
        record: bool = True,
    ) -> WorkloadProfile:
        """Analyze incoming task and create workload profile with helium metrics."""
        if not isinstance(task_json, Mapping):
            msg = (
                f"task_json must be a Mapping, got {type(task_json).__name__}."
            )
            if self._strict:
                raise InterpretationError(msg)
            logger.warning("%s Using empty mapping.", msg)
            task_json = {}

        complexity_score = self._calculate_complexity(task_json)
        energy_estimate = self._estimate_energy(task_json)
        carbon_estimate = self._estimate_carbon(energy_estimate)
        resource_reqs = self._extract_resource_requirements(task_json)
        priority = int(task_json.get("priority", 5) or 5)
        deferrable = bool(task_json.get("deferrable", True))

        profile = WorkloadProfile(
            task_id=str(task_json.get("task_id", "unknown")),
            complexity_score=complexity_score,
            energy_estimate_kwh=energy_estimate,
            carbon_estimate_kg=carbon_estimate,
            resource_requirements=resource_reqs,
            deferrable=deferrable,
            priority=priority,
        )

        if self.enable_helium_analysis:
            try:
                profile.helium_profile = HeliumProfile.from_task_config(
                    task_json, config=self._helium_config
                )
                if (
                    profile.helium_profile.dependency_score
                    > self._config.high_dependency_cutoff
                ):
                    profile.energy_estimate_kwh *= (
                        self._config.high_dependency_energy_penalty
                    )
            except HeliumProfileError as exc:
                logger.warning("Helium analysis failed: %s", exc)
                if self._strict:
                    raise InterpretationError(
                        f"Helium analysis failed: {exc}"
                    ) from exc

        if record:
            with self._lock:
                self._history.append(profile)

        logger.debug(
            "Analyzed task %s: complexity=%.3f energy=%.4f carbon=%.4f",
            profile.task_id,
            profile.complexity_score,
            profile.energy_estimate_kwh,
            profile.carbon_estimate_kg,
        )
        return profile

    # --------------- private helpers (unchanged behavior, config-driven) ------
    def _calculate_complexity(self, task_json: Mapping) -> float:
        model_size = float(
            (task_json.get("model_config") or {}).get("size_gb", 0) or 0
        )
        data_volume = float(task_json.get("data_volume_gb", 0) or 0)
        return min(
            1.0,
            (model_size / self._config.model_size_scale_gb)
            + (data_volume / self._config.data_volume_scale_gb),
        )

    def _estimate_energy(self, task_json: Mapping) -> float:
        hardware_req = task_json.get("hardware_requirements") or {}
        gpu_count = int(hardware_req.get("gpu_count", 0) or 0)
        return (
            self._config.base_energy_kwh
            + gpu_count * self._config.energy_per_gpu_kwh
        )

    def _estimate_carbon(self, energy_kwh: float) -> float:
        return energy_kwh * self._config.carbon_intensity_kg_per_kwh

    def _extract_resource_requirements(self, task_json: Mapping) -> Dict:
        hw = task_json.get("hardware_requirements") or {}
        return {
            "cpu_cores": int(hw.get("cpu_cores", 2) or 2),
            "memory_gb": float(hw.get("memory_gb", 8) or 8),
            "gpu_count": int(hw.get("gpu_count", 0) or 0),
        }

    # ------------------------------- statistics / serialization ---------------
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            history = list(self._history)
        if not history:
            return {
                "interpreted": 0,
                "mean_complexity": None,
                "mean_energy_kwh": None,
                "mean_carbon_kg": None,
                "helium_analyzed": 0,
            }
        return {
            "interpreted": len(history),
            "mean_complexity": sum(p.complexity_score for p in history)
            / len(history),
            "mean_energy_kwh": sum(p.energy_estimate_kwh for p in history)
            / len(history),
            "mean_carbon_kg": sum(p.carbon_estimate_kg for p in history)
            / len(history),
            "helium_analyzed": sum(
                1 for p in history if p.helium_profile is not None
            ),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug("WorkloadInterpreter reset (clear_history=%s)", clear_history)

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "helium_config": asdict(self._helium_config),
                "strict": self._strict,
                "enable_helium_analysis": self.enable_helium_analysis,
                "history": [p.to_dict() for p in self._history],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "WorkloadInterpreter":
        if not isinstance(data, Mapping):
            raise InterpretationError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = InterpretationConfig(
            enable_helium_analysis=bool(
                cfg_data.get("enable_helium_analysis", True)
            ),
            model_size_scale_gb=float(cfg_data.get("model_size_scale_gb", 100.0)),
            data_volume_scale_gb=float(cfg_data.get("data_volume_scale_gb", 1000.0)),
            base_energy_kwh=float(cfg_data.get("base_energy_kwh", 0.1)),
            energy_per_gpu_kwh=float(cfg_data.get("energy_per_gpu_kwh", 0.3)),
            carbon_intensity_kg_per_kwh=float(
                cfg_data.get("carbon_intensity_kg_per_kwh", 0.4)
            ),
            high_dependency_cutoff=float(
                cfg_data.get("high_dependency_cutoff", 0.8)
            ),
            high_dependency_energy_penalty=float(
                cfg_data.get("high_dependency_energy_penalty", 1.2)
            ),
            max_history=int(cfg_data.get("max_history", 1000)),
        )
        interp = cls(interpretation_config=cfg,
                     strict=bool(data.get("strict", True)))
        with interp._lock:
            for entry in data.get("history", []):
                interp._history.append(WorkloadProfile.from_dict(entry))
        return interp

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "WorkloadInterpreter":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise InterpretationError(f"Invalid JSON: {exc}") from exc

    def __repr__(self) -> str:
        with self._lock:
            return (
                "WorkloadInterpreter("
                f"helium={self.enable_helium_analysis}, "
                f"history={len(self._history)}, "
                f"strict={self._strict})"
            )


__all__ = [
    "InterpretationConfig",
    "InterpretationError",
    "WorkloadInterpreter",
    "WorkloadProfile",
]


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    interp = WorkloadInterpreter()
    print("repr      :", interp)

    task = {
        "task_id": "t-1",
        "hardware_requirements": {"gpu_count": 8, "cpu_cores": 16, "memory_gb": 64},
        "model_config": {"size_gb": 120, "cpu_compatible": False},
        "estimated_training_hours": 200,
        "priority": 3,
        "data_volume_gb": 500,
    }
    profile = interp.analyze_task(task)
    print("profile   :", profile)
    print("helium    :", profile.helium_profile)
    print("stats     :", interp.statistics())

    payload = interp.to_json()
    restored = WorkloadInterpreter.from_json(payload)
    assert restored.to_dict() == interp.to_dict()
    print("Round-trip OK.")
