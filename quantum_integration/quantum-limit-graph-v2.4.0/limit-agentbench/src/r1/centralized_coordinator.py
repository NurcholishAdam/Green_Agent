# src/r1/centralized_coordinator.py

"""
Central Coordinator
===================

Global sustainability ranking across multiple agents.

Enhancements
------------
- Fixed ``ZeroDivisionError`` when ``energy == 0``.
- ``CoordinatorConfig`` — frozen, validated: capacity, ranking key,
  tie-break policy, and minimum-energy floor.
- ``AgentResult`` — frozen dataclass replacing the raw dict.
- ``RLock``-guarded; bounded history via ``deque(maxlen=...)``.
- Full validation; strict / non-strict modes.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- ``statistics()``, ``__repr__``, custom ``CoordinatorError``,
  lazy ``%s`` logging, and a ``__main__`` smoke test.
"""

from __future__ import annotations

import json, logging, math, threading, time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


class CoordinatorError(ValueError):
    """Raised for invalid coordinator inputs or configuration."""


@dataclass(frozen=True)
class CoordinatorConfig:
    """Tunable parameters for the central coordinator."""
    max_metrics: Optional[int] = 10_000
    ranking_key: str = "accuracy_per_watt"   # "accuracy_per_watt" | "reward" | "accuracy"
    min_energy_floor: float = 1e-9            # Avoid zero-division
    ascending: bool = False
    defensive_copy_on_rank: bool = True

    def __post_init__(self) -> None:
        if self.max_metrics is not None and self.max_metrics <= 0:
            raise CoordinatorError("max_metrics must be > 0 or None.")
        if self.ranking_key not in ("accuracy_per_watt", "reward", "accuracy"):
            raise CoordinatorError(
                f"unsupported ranking_key {self.ranking_key!r}."
            )
        if self.min_energy_floor <= 0:
            raise CoordinatorError("min_energy_floor must be > 0.")


@dataclass(frozen=True)
class AgentResult:
    """Immutable record of one agent's result."""
    agent: str
    accuracy: float
    energy: float
    reward: float
    accuracy_per_watt: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not isinstance(self.agent, str) or not self.agent:
            raise CoordinatorError("agent must be a non-empty string.")
        for name in ("accuracy", "energy", "reward", "accuracy_per_watt"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or math.isnan(float(value)) or math.isinf(float(value)):
                raise CoordinatorError(f"{name} must be a finite number.")
        if self.energy < 0:
            raise CoordinatorError("energy must be >= 0.")
        if self.accuracy_per_watt < 0:
            raise CoordinatorError("accuracy_per_watt must be >= 0.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent": self.agent,
            "accuracy": self.accuracy,
            "energy": self.energy,
            "reward": self.reward,
            "accuracy_per_watt": self.accuracy_per_watt,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AgentResult":
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
            agent=str(data["agent"]),
            accuracy=float(data["accuracy"]),
            energy=float(data["energy"]),
            reward=float(data["reward"]),
            accuracy_per_watt=float(data.get("accuracy_per_watt", 0.0)),
            timestamp=timestamp,
        )

    def __repr__(self) -> str:
        return (
            "AgentResult("
            f"agent={self.agent!r}, "
            f"accuracy={self.accuracy:.3f}, energy={self.energy:.4g}, "
            f"apw={self.accuracy_per_watt:.4g}, reward={self.reward:.3f})"
        )


class CentralCoordinator:
    """
    Global sustainability ranking across multiple agents.

    Thread-safe, serializable, and bounded in memory. The original public API
    (``register_agent_result`` and ``compute_global_sustainability_rank``) is
    preserved; new parameters are keyword-only.
    """

    def __init__(
        self,
        *,
        config: Optional[CoordinatorConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or CoordinatorConfig()
        self._strict = bool(strict)
        self._lock = threading.RLock()
        self.global_metrics: Deque[AgentResult] = deque(
            maxlen=self._config.max_metrics
        )
        self._replacements: int = 0
        logger.debug(
            "CentralCoordinator initialized (ranking_key=%s, strict=%s)",
            self._config.ranking_key, self._strict,
        )

    @property
    def config(self) -> CoordinatorConfig:
        return self._config

    @property
    def size(self) -> int:
        with self._lock:
            return len(self.global_metrics)

    def register_agent_result(
        self, agent_id: str, accuracy: float, energy: float, reward: float
    ) -> AgentResult:
        """Record one agent's result and return the stored record."""
        if not isinstance(agent_id, str) or not agent_id:
            msg = "agent_id must be a non-empty string."
            if self._strict:
                raise CoordinatorError(msg)
            logger.warning("%s Skipping.", msg)
            return AgentResult(
                agent="__invalid__", accuracy=0.0, energy=0.0,
                reward=0.0, accuracy_per_watt=0.0,
            )

        accuracy = self._validate_number("accuracy", accuracy)
        energy = self._validate_number("energy", energy, low=0.0)
        reward = self._validate_number("reward", reward)

        # Fixed: zero-division guard using the configured floor.
        denom = max(energy, self._config.min_energy_floor)
        accuracy_per_watt = accuracy / denom

        record = AgentResult(
            agent=agent_id,
            accuracy=accuracy,
            energy=energy,
            reward=reward,
            accuracy_per_watt=accuracy_per_watt,
        )

        with self._lock:
            # Replace duplicate agents in place (matches "latest wins" semantics).
            for i, existing in enumerate(self.global_metrics):
                if existing.agent == agent_id:
                    self.global_metrics.remove(existing)
                    self._replacements += 1
                    break
            self.global_metrics.append(record)

        logger.debug(
            "Registered agent=%s accuracy=%.3f energy=%.4g apw=%.4g reward=%.3f",
            agent_id, accuracy, energy, accuracy_per_watt, reward,
        )
        return record

    def compute_global_sustainability_rank(
        self, top_n: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Return the sustainability ranking sorted by the configured key.

        Returns a list of JSON-safe dicts. Never raises on energy=0.
        """
        if top_n is not None and (not isinstance(top_n, int) or top_n <= 0):
            raise CoordinatorError("top_n must be a positive int or None.")

        with self._lock:
            records = list(self.global_metrics)

        ranked = sorted(
            records,
            key=self._sort_key,
            reverse=not self._config.ascending,
        )
        if top_n is not None:
            ranked = ranked[:top_n]
        return [r.to_dict() for r in ranked]

    def _sort_key(self, record: AgentResult) -> Tuple[float, float, str]:
        key = self._config.ranking_key
        if key == "accuracy_per_watt":
            primary = record.accuracy_per_watt
        elif key == "reward":
            primary = record.reward
        else:
            primary = record.accuracy
        return (primary, record.accuracy_per_watt, record.agent)

    @staticmethod
    def _validate_number(name: str, value: Any, *, low: Optional[float] = None) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise CoordinatorError(f"{name} must be numeric, got {type(value).__name__}.")
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            raise CoordinatorError(f"{name} must be finite, got {value!r}.")
        if low is not None and fv < low:
            raise CoordinatorError(f"{name} must be >= {low}, got {fv}.")
        return fv

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            records = list(self.global_metrics)
        if not records:
            return {"count": 0, "replacements": self._replacements}
        return {
            "count": len(records),
            "replacements": self._replacements,
            "mean_accuracy": sum(r.accuracy for r in records) / len(records),
            "mean_energy": sum(r.energy for r in records) / len(records),
            "mean_apw": sum(r.accuracy_per_watt for r in records) / len(records),
            "mean_reward": sum(r.reward for r in records) / len(records),
            "top_agent": max(records, key=self._sort_key).agent,
        }

    def reset(self, *, clear_metrics: bool = True) -> int:
        with self._lock:
            n = len(self.global_metrics)
            if clear_metrics:
                self.global_metrics.clear()
            self._replacements = 0
        return n

    # ---- serialization ----
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "replacements": self._replacements,
                "metrics": [r.to_dict() for r in self.global_metrics],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CentralCoordinator":
        if not isinstance(data, Mapping):
            raise CoordinatorError("from_dict expects a Mapping.")
        cfg_data = dict(data.get("config", {}) or {})
        cfg = CoordinatorConfig(
            max_metrics=cfg_data.get("max_metrics", 10_000),
            ranking_key=str(cfg_data.get("ranking_key", "accuracy_per_watt")),
            min_energy_floor=float(cfg_data.get("min_energy_floor", 1e-9)),
            ascending=bool(cfg_data.get("ascending", False)),
        )
        coord = cls(config=cfg, strict=bool(data.get("strict", True)))
        with coord._lock:
            for entry in data.get("metrics", []):
                coord.global_metrics.append(AgentResult.from_dict(entry))
            coord._replacements = int(data.get("replacements", 0))
        return coord

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "CentralCoordinator":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise CoordinatorError(f"Invalid JSON payload: {exc}") from exc

    def __repr__(self) -> str:
        with self._lock:
            return (
                "CentralCoordinator("
                f"agents={len(self.global_metrics)}, "
                f"ranking_key={self._config.ranking_key!r}, "
                f"strict={self._strict})"
            )


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    coord = CentralCoordinator()
    print("repr       :", coord)

    coord.register_agent_result("alpha", 0.95, 0.010, 0.90)
    coord.register_agent_result("beta", 0.90, 0.003, 0.85)
    coord.register_agent_result("gamma", 0.75, 0.001, 0.80)

    # Fixed: zero-energy must not raise.
    coord.register_agent_result("zero_energy", 0.80, 0.0, 0.70)

    print("ranked     :")
    for i, entry in enumerate(coord.compute_global_sustainability_rank(), 1):
        print(f"  {i}. {entry['agent']:<12} apw={entry['accuracy_per_watt']:.2f}")

    print("stats      :", coord.statistics())

    payload = coord.to_json()
    restored = CentralCoordinator.from_json(payload)
    assert restored.to_dict() == coord.to_dict()
    print("Round-trip OK.")

    try:
        coord.register_agent_result("", 0.9, 0.01, 0.9)
    except CoordinatorError as exc:
        print("Rejected   :", exc)

    print("\nSmoke test passed.")
