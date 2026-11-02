# src/r1/centralized_coordinator.py

"""
Central Coordinator
===================

Global sustainability ranking across multiple agents.

Design
------
- Frozen, validated :class:`CoordinatorConfig` — ``max_metrics``,
  ``ranking_key``, ``min_energy_floor``, ``ascending``.
- Frozen :class:`AgentResult` with strict validation (no ``bool``, no
  ``NaN``/``Inf``, non-negative ``accuracy``/``energy``/``accuracy_per_watt``).
- **Non-strict mode returns ``None`` and does not mutate state.** The
  coordinator never fabricates an ``"__invalid__"`` agent. Bad
  registrations are counted in ``_skipped``.
- **O(1) duplicate lookup** via an internal ``_by_agent`` dict.
- **Separate eviction counters**: ``_replacements`` (agent re-registered)
  vs. ``_evictions`` (LRU drop under ``max_metrics``) vs. ``_skipped``
  (non-strict invalid input).
- **``statistics()["top_agent"]`` respects ``ascending``**.
- Serialization contract:

    * ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
    * ``from_dict`` validates config and every metric, wraps all
      failures in :class:`CoordinatorError`, refuses over-cap payloads,
      and rejects duplicate ``agent_id`` values in the metrics list.
    * ``to_json`` uses ``allow_nan=False``.

Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Deque, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "AgentResult",
    "CentralCoordinator",
    "CoordinatorConfig",
    "CoordinatorError",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CoordinatorError(ValueError):
    """Raised for invalid coordinator inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CoordinatorConfig:
    """Tunable parameters for the central coordinator."""

    max_metrics: Optional[int] = 10_000
    ranking_key: str = "accuracy_per_watt"   # "accuracy_per_watt" | "reward" | "accuracy"
    min_energy_floor: float = 1e-9
    ascending: bool = False

    def __post_init__(self) -> None:
        if self.max_metrics is not None:
            if (
                isinstance(self.max_metrics, bool)
                or not isinstance(self.max_metrics, int)
                or self.max_metrics <= 0
            ):
                raise CoordinatorError(
                    "max_metrics must be a positive int or None."
                )

        if self.ranking_key not in (
            "accuracy_per_watt", "reward", "accuracy"
        ):
            raise CoordinatorError(
                f"unsupported ranking_key {self.ranking_key!r}."
            )

        # Reject NaN/Inf BEFORE the <= 0 check — ``NaN <= 0`` is False.
        if isinstance(self.min_energy_floor, bool) or not isinstance(
            self.min_energy_floor, (numbers.Real, Decimal)
        ):
            raise CoordinatorError(
                "min_energy_floor must be numeric, "
                f"got {type(self.min_energy_floor).__name__}."
            )
        try:
            mef = float(self.min_energy_floor)
        except (TypeError, ValueError) as exc:
            raise CoordinatorError(
                f"min_energy_floor not convertible to float: {exc}"
            ) from exc
        if not math.isfinite(mef):
            raise CoordinatorError(
                f"min_energy_floor must be finite, "
                f"got {self.min_energy_floor!r}."
            )
        if mef <= 0:
            raise CoordinatorError(
                f"min_energy_floor must be > 0, got {mef}."
            )
        object.__setattr__(self, "min_energy_floor", mef)

        if not isinstance(self.ascending, bool):
            raise CoordinatorError(
                f"ascending must be bool, got "
                f"{type(self.ascending).__name__}."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CoordinatorConfig":
        if not isinstance(data, Mapping):
            raise CoordinatorError(
                f"CoordinatorConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            # Unknown fields (e.g. deprecated ``defensive_copy_on_rank``
            # in payloads written by older versions) are silently
            # ignored — this keeps old snapshots loadable.
            return cls(
                max_metrics=data.get("max_metrics", 10_000),
                ranking_key=data.get(
                    "ranking_key", "accuracy_per_watt"
                ),
                min_energy_floor=data.get(
                    "min_energy_floor", 1e-9
                ),
                ascending=data.get("ascending", False),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, CoordinatorError):
                raise
            raise CoordinatorError(
                f"invalid CoordinatorConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# AgentResult
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AgentResult:
    """Immutable record of one agent's result."""

    agent: str
    accuracy: float
    energy: float
    reward: float
    accuracy_per_watt: float
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.agent, str) or not self.agent:
            raise CoordinatorError("agent must be a non-empty string.")

        for name in (
            "accuracy", "energy", "reward", "accuracy_per_watt"
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(
                value, (numbers.Real, Decimal)
            ):
                raise CoordinatorError(
                    f"{name} must be a real number, "
                    f"got {type(value).__name__}."
                )
            try:
                fv = float(value)
            except (TypeError, ValueError) as exc:
                raise CoordinatorError(
                    f"{name} not convertible to float: {exc}"
                ) from exc
            if not math.isfinite(fv):
                raise CoordinatorError(
                    f"{name} must be finite, got {value!r}."
                )
            # Normalise to float so hash/eq are consistent across
            # Decimal / Fraction / float inputs.
            object.__setattr__(self, name, fv)

        if self.accuracy < 0:
            raise CoordinatorError("accuracy must be >= 0.")
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
        if not isinstance(data, Mapping):
            raise CoordinatorError(
                f"AgentResult.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        ts = data.get("timestamp")
        if isinstance(ts, str):
            try:
                timestamp = datetime.fromisoformat(ts)
            except ValueError as exc:
                raise CoordinatorError(
                    f"invalid timestamp {ts!r}: {exc}"
                ) from exc
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)

        try:
            return cls(
                agent=str(data["agent"]),
                accuracy=data["accuracy"],
                energy=data["energy"],
                reward=data["reward"],
                accuracy_per_watt=data.get(
                    "accuracy_per_watt", 0.0
                ),
                timestamp=timestamp,
            )
        except KeyError as exc:
            raise CoordinatorError(
                f"missing required field {exc.args[0]!r}."
            ) from exc

    def __repr__(self) -> str:
        return (
            "AgentResult("
            f"agent={self.agent!r}, "
            f"accuracy={self.accuracy:.3f}, "
            f"energy={self.energy:.4g}, "
            f"apw={self.accuracy_per_watt:.4g}, "
            f"reward={self.reward:.3f})"
        )


# --------------------------------------------------------------------------- #
# Coordinator
# --------------------------------------------------------------------------- #
class CentralCoordinator:
    """
    Global sustainability ranking across multiple agents.

    Thread-safe, serializable, and bounded in memory.

    Parameters
    ----------
    config : CoordinatorConfig, optional
        Typed configuration.
    strict : bool, default True
        If True, invalid inputs raise :class:`CoordinatorError`. If
        False, they are logged and skipped — ``register_agent_result``
        returns ``None`` and state is not mutated.
    """

    def __init__(
        self,
        *,
        config: Optional[CoordinatorConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config if config is not None else CoordinatorConfig()
        self._strict = bool(strict)
        self._lock = threading.RLock()

        self.global_metrics: Deque[AgentResult] = deque(
            maxlen=self._config.max_metrics
        )
        # O(1) lookup for duplicate detection.
        self._by_agent: Dict[str, AgentResult] = {}

        # Counters.
        self._replacements: int = 0   # agent re-registered (last wins)
        self._evictions: int = 0      # LRU drop under max_metrics
        self._skipped: int = 0        # non-strict invalid input

        logger.debug(
            "CentralCoordinator initialized "
            "(ranking_key=%s, strict=%s, max_metrics=%s)",
            self._config.ranking_key,
            self._strict,
            self._config.max_metrics,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> CoordinatorConfig:
        return self._config

    @property
    def size(self) -> int:
        with self._lock:
            return len(self.global_metrics)

    # ------------------------------------------------------------------ register
    def register_agent_result(
        self,
        agent_id: str,
        accuracy: float,
        energy: float,
        reward: float,
    ) -> Optional[AgentResult]:
        """
        Record one agent's result and return the stored record.

        Returns ``None`` in non-strict mode when any input is invalid;
        in that case, state is not mutated and ``_skipped`` increments.
        """
        if not isinstance(agent_id, str) or not agent_id:
            msg = "agent_id must be a non-empty string."
            if self._strict:
                raise CoordinatorError(msg)
            logger.warning("%s Skipping.", msg)
            with self._lock:
                self._skipped += 1
            return None

        accuracy_v = self._validate_number(
            "accuracy", accuracy, low=0.0,
        )
        energy_v = self._validate_number(
            "energy", energy, low=0.0,
        )
        reward_v = self._validate_number(
            "reward", reward,
        )
        if (
            accuracy_v is None
            or energy_v is None
            or reward_v is None
        ):
            with self._lock:
                self._skipped += 1
            return None

        denom = max(energy_v, self._config.min_energy_floor)
        accuracy_per_watt = accuracy_v / denom

        record = AgentResult(
            agent=agent_id,
            accuracy=accuracy_v,
            energy=energy_v,
            reward=reward_v,
            accuracy_per_watt=accuracy_per_watt,
        )

        with self._lock:
            # Duplicate agent → remove old, count as a replacement.
            existing = self._by_agent.pop(agent_id, None)
            if existing is not None:
                self.global_metrics.remove(existing)
                self._replacements += 1
            else:
                # New agent — will it evict?
                cap = self._config.max_metrics
                if cap is not None and len(self.global_metrics) >= cap:
                    self._evictions += 1
            self.global_metrics.append(record)
            self._by_agent[agent_id] = record

        logger.debug(
            "Registered agent=%s accuracy=%.3f energy=%.4g "
            "apw=%.4g reward=%.3f",
            agent_id, accuracy_v, energy_v,
            accuracy_per_watt, reward_v,
        )
        return record

    # ------------------------------------------------------------------ rank
    def compute_global_sustainability_rank(
        self, top_n: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Return the sustainability ranking sorted by the configured key.

        Returns a list of JSON-safe dicts. Never raises on ``energy=0``.
        """
        if top_n is not None and (
            isinstance(top_n, bool)
            or not isinstance(top_n, int)
            or top_n <= 0
        ):
            raise CoordinatorError(
                "top_n must be a positive int or None."
            )

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

    # ------------------------------------------------------------------ helpers
    def _validate_number(
        self,
        name: str,
        value: Any,
        *,
        low: Optional[float] = None,
    ) -> Optional[float]:
        """
        Coerce to a finite float. Returns ``None`` in non-strict mode on
        failure; raises :class:`CoordinatorError` in strict mode.
        """
        if isinstance(value, bool) or not isinstance(
            value, (numbers.Real, Decimal)
        ):
            msg = (
                f"{name} must be a real number, "
                f"got {type(value).__name__}."
            )
            if self._strict:
                raise CoordinatorError(msg)
            logger.warning("%s Skipping.", msg)
            return None

        try:
            fv = float(value)
        except (TypeError, ValueError) as exc:
            msg = f"{name} not convertible to float: {exc}."
            if self._strict:
                raise CoordinatorError(msg) from exc
            logger.warning("%s Skipping.", msg)
            return None

        if not math.isfinite(fv):
            msg = f"{name} must be finite, got {value!r}."
            if self._strict:
                raise CoordinatorError(msg)
            logger.warning("%s Skipping.", msg)
            return None

        if low is not None and fv < low:
            msg = f"{name} must be >= {low}, got {fv}."
            if self._strict:
                raise CoordinatorError(msg)
            logger.warning("%s Skipping.", msg)
            return None

        return fv

    # ------------------------------------------------------------------ stats
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            records = list(self.global_metrics)
            replacements = self._replacements
            evictions = self._evictions
            skipped = self._skipped

        base = {
            "count": len(records),
            "replacements": replacements,
            "evictions": evictions,
            "skipped": skipped,
        }
        if not records:
            return base

        # "Top" = best under the configured ordering.
        top = (
            min(records, key=self._sort_key)
            if self._config.ascending
            else max(records, key=self._sort_key)
        )
        base.update({
            "mean_accuracy": (
                sum(r.accuracy for r in records) / len(records)
            ),
            "mean_energy": (
                sum(r.energy for r in records) / len(records)
            ),
            "mean_apw": (
                sum(r.accuracy_per_watt for r in records)
                / len(records)
            ),
            "mean_reward": (
                sum(r.reward for r in records) / len(records)
            ),
            "top_agent": top.agent,
        })
        return base

    # ------------------------------------------------------------------ reset
    def reset(self, *, clear_metrics: bool = True) -> int:
        with self._lock:
            n = len(self.global_metrics)
            if clear_metrics:
                self.global_metrics.clear()
                self._by_agent.clear()
            self._replacements = 0
            self._evictions = 0
            self._skipped = 0
        return n

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "counters": {
                    "replacements": self._replacements,
                    "evictions": self._evictions,
                    "skipped": self._skipped,
                },
                "metrics": [
                    r.to_dict() for r in self.global_metrics
                ],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: Optional[bool] = None,
    ) -> "CentralCoordinator":
        if not isinstance(data, Mapping):
            raise CoordinatorError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )

        cfg_data = dict(data.get("config", {}) or {})
        cfg = CoordinatorConfig.from_dict(cfg_data)

        raw_strict = data.get("strict", True)
        if strict is None:
            if not isinstance(raw_strict, bool):
                raise CoordinatorError(
                    f"strict must be bool, got "
                    f"{type(raw_strict).__name__}."
                )
            strict_flag = raw_strict
        else:
            if not isinstance(strict, bool):
                raise CoordinatorError(
                    f"strict must be bool, got "
                    f"{type(strict).__name__}."
                )
            strict_flag = strict

        coord = cls(config=cfg, strict=strict_flag)

        raw_metrics = data.get("metrics", [])
        if not isinstance(raw_metrics, list):
            raise CoordinatorError(
                f"metrics must be a list, got "
                f"{type(raw_metrics).__name__}."
            )

        # Enforce capacity explicitly — a deque would silently drop the
        # oldest entries via maxlen, breaking the round-trip contract.
        cap = cfg.max_metrics
        if cap is not None and len(raw_metrics) > cap:
            raise CoordinatorError(
                f"metrics has {len(raw_metrics)} entries, "
                f"exceeds max_metrics={cap}."
            )

        counters = dict(data.get("counters", {}) or {})

        def _int_counter(key: str) -> int:
            try:
                v = int(counters.get(key, 0))
            except (TypeError, ValueError) as exc:
                raise CoordinatorError(
                    f"invalid counter {key!r}: {exc}"
                ) from exc
            if v < 0:
                raise CoordinatorError(
                    f"counter {key!r} must be >= 0, got {v}."
                )
            return v

        replacements = _int_counter("replacements")
        evictions = _int_counter("evictions")
        skipped = _int_counter("skipped")

        with coord._lock:
            coord.global_metrics.clear()
            coord._by_agent.clear()
            for entry in raw_metrics:
                result = AgentResult.from_dict(entry)
                if result.agent in coord._by_agent:
                    raise CoordinatorError(
                        f"duplicate agent {result.agent!r} in "
                        f"metrics payload."
                    )
                coord.global_metrics.append(result)
                coord._by_agent[result.agent] = result
            coord._replacements = replacements
            coord._evictions = evictions
            coord._skipped = skipped

        return coord

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: Optional[bool] = None,
    ) -> "CentralCoordinator":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise CoordinatorError(
                f"invalid JSON payload: {exc}"
            ) from exc
        try:
            return cls.from_dict(raw, strict=strict)
        except CoordinatorError:
            raise
        except (TypeError, ValueError) as exc:
            raise CoordinatorError(
                f"invalid coordinator payload: {exc}"
            ) from exc

    # ------------------------------------------------------------------ dunder
    def __repr__(self) -> str:
        with self._lock:
            n = len(self.global_metrics)
        return (
            "CentralCoordinator("
            f"agents={n}, "
            f"ranking_key={self._config.ranking_key!r}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1.centralized_coordinator
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    coord = CentralCoordinator()
    print("repr       :", coord)
    print("VERSION    :", VERSION)

    # ---- Happy path ---------------------------------------------- #
    coord.register_agent_result("alpha", 0.95, 0.010, 0.90)
    coord.register_agent_result("beta", 0.90, 0.003, 0.85)
    coord.register_agent_result("gamma", 0.75, 0.001, 0.80)

    # ---- Zero-energy guard (fixed) ------------------------------- #
    coord.register_agent_result("zero_energy", 0.80, 0.0, 0.70)
    rec = coord.compute_global_sustainability_rank()
    assert any(r["agent"] == "zero_energy" for r in rec)
    print("zero-energy: OK (no ZeroDivisionError)")

    print("ranked     :")
    for i, entry in enumerate(
        coord.compute_global_sustainability_rank(), 1
    ):
        print(
            f"  {i}. {entry['agent']:<12} "
            f"apw={entry['accuracy_per_watt']:.4g}"
        )

    # ---- Zero-energy uses min_energy_floor ----------------------- #
    z_energy = next(
        r for r in coord.compute_global_sustainability_rank()
        if r["agent"] == "zero_energy"
    )
    expected_apw = 0.80 / coord.config.min_energy_floor
    assert abs(z_energy["accuracy_per_watt"] - expected_apw) < 1e-6
    print("min_energy_floor applied correctly")

    # ---- statistics() respects ascending ------------------------- #
    stats_desc = coord.statistics()
    print("stats (desc):", {
        k: stats_desc[k]
        for k in ("count", "top_agent", "replacements",
                  "evictions", "skipped")
    })
    # descending → top is highest apw = zero_energy (0.80/1e-9 is huge)
    assert stats_desc["top_agent"] == "zero_energy"

    asc = CentralCoordinator(
        config=CoordinatorConfig(ascending=True)
    )
    asc.register_agent_result("a", 0.9, 0.01, 1.0)
    asc.register_agent_result("b", 0.9, 0.001, 1.0)
    assert asc.statistics()["top_agent"] == "b"
    print("ascending top_agent: b (correct)")

    # ---- Non-strict returns None, no fake agent ----------------- #
    lenient = CentralCoordinator(strict=False)
    lenient.register_agent_result("good", 0.9, 0.01, 0.9)

    # Invalid agent_id
    assert lenient.register_agent_result(
        "", 0.9, 0.01, 0.9
    ) is None
    # Invalid accuracy
    assert lenient.register_agent_result(
        "bad", float("nan"), 0.01, 0.9
    ) is None
    # Invalid energy (negative)
    assert lenient.register_agent_result(
        "bad2", 0.9, -1.0, 0.9
    ) is None
    # Invalid reward (bool)
    assert lenient.register_agent_result(
        "bad3", 0.9, 0.01, True
    ) is None

    # No phantom entries
    agents = [
        r["agent"]
        for r in lenient.compute_global_sustainability_rank()
    ]
    assert agents == ["good"], agents
    assert lenient.statistics()["skipped"] == 4
    print("non-strict   : 4 skipped, no phantom agents")

    # ---- Strict still raises ------------------------------------ #
    strict = CentralCoordinator(strict=True)
    for fn in (
        lambda: strict.register_agent_result(
            "", 0.9, 0.01, 0.9
        ),
        lambda: strict.register_agent_result(
            "s", float("nan"), 0.01, 0.9
        ),
        lambda: strict.register_agent_result(
            "s", 0.9, -1.0, 0.9
        ),
        lambda: strict.register_agent_result(
            "s", 0.9, 0.01, True
        ),
        lambda: strict.register_agent_result(
            "s", -0.5, 0.01, 0.9
        ),  # accuracy < 0, error about accuracy
    ):
        try:
            fn()
        except CoordinatorError as exc:
            print("Rejected   :", exc)

    # ---- Bool rejected in AgentResult directly ------------------ #
    try:
        AgentResult(
            agent="x", accuracy=True, energy=0.1,
            reward=1.0, accuracy_per_watt=1.0,
        )
    except CoordinatorError as exc:
        print("bool in AgentResult:", exc)

    # ---- Decimal / Fraction accepted ---------------------------- #
    from decimal import Decimal
    from fractions import Fraction as F
    num = CentralCoordinator()
    rec_dec = num.register_agent_result(
        "dec", Decimal("0.95"), Decimal("0.01"), Decimal("0.9"),
    )
    assert rec_dec is not None and rec_dec.accuracy == 0.95
    rec_frac = num.register_agent_result(
        "frac", F(9, 10), F(1, 100), F(4, 5),
    )
    assert rec_frac is not None
    print("Decimal/Fraction: accepted")

    # ---- Config validation: NaN / bool -------------------------- #
    for bad_cfg in (
        dict(min_energy_floor=float("nan")),
        dict(min_energy_floor=float("inf")),
        dict(min_energy_floor=0.0),
        dict(min_energy_floor=-1e-9),
        dict(max_metrics=True),
        dict(max_metrics=0),
        dict(ascending=1),
        dict(ranking_key="bogus"),
    ):
        try:
            CoordinatorConfig(**bad_cfg)  # type: ignore[arg-type]
        except CoordinatorError as exc:
            print("Rejected cfg:", exc)

    # ---- Evictions counter -------------------------------------- #
    evicted = CentralCoordinator(
        config=CoordinatorConfig(max_metrics=3)
    )
    for i in range(6):
        evicted.register_agent_result(
            f"a{i}", 0.9, 0.01, 0.9,
        )
    st = evicted.statistics()
    assert st["count"] == 3
    assert st["evictions"] == 3, st["evictions"]
    print("evictions    :", st["evictions"])

    # ---- O(1) duplicate lookup via _by_agent -------------------- #
    dup = CentralCoordinator()
    for _ in range(100):
        dup.register_agent_result("same", 0.9, 0.01, 0.9)
    assert dup.size == 1
    assert dup.statistics()["replacements"] == 99
    print("replacements :", dup.statistics()["replacements"])

    # ---- Serialization round-trip ------------------------------- #
    payload = coord.to_dict()
    assert payload["version"] == VERSION
    rt = CentralCoordinator.from_dict(payload)
    assert rt.to_dict() == coord.to_dict()
    print("Round-trip OK.")

    # ---- from_dict rejects over-cap payload --------------------- #
    try:
        CentralCoordinator.from_dict({
            "config": {"max_metrics": 2},
            "metrics": [
                {"agent": f"a{i}", "accuracy": 0.9,
                 "energy": 0.01, "reward": 0.9}
                for i in range(5)
            ],
        })
    except CoordinatorError as exc:
        print("Rejected over-cap:", exc)

    # ---- from_dict rejects duplicate agents --------------------- #
    try:
        CentralCoordinator.from_dict({
            "config": {},
            "metrics": [
                {"agent": "x", "accuracy": 0.9,
                 "energy": 0.01, "reward": 0.9},
                {"agent": "x", "accuracy": 0.8,
                 "energy": 0.01, "reward": 0.9},
            ],
        })
    except CoordinatorError as exc:
        print("Rejected duplicates:", exc)

    # ---- from_dict wraps malformed entries ---------------------- #
    for bad_payload in (
        {"config": {"min_energy_floor": "abc"}},
        {"config": {}, "metrics": "not a list"},
        {"config": {}, "metrics": [
            {"agent": "x"}  # missing fields
        ]},
        {"config": {}, "counters": {"replacements": "abc"}},
    ):
        try:
            CentralCoordinator.from_dict(bad_payload)
        except CoordinatorError as exc:
            print("Rejected payload:", exc)

    # ---- from_dict tolerates deprecated defensive_copy_on_rank -- #
    old_payload = {
        "config": {
            "max_metrics": 100,
            "ranking_key": "accuracy_per_watt",
            "min_energy_floor": 1e-9,
            "ascending": False,
            "defensive_copy_on_rank": True,   # deprecated
        },
        "strict": True,
        "counters": {"replacements": 0,
                     "evictions": 0, "skipped": 0},
        "metrics": [],
    }
    restored = CentralCoordinator.from_dict(old_payload)
    print("deprecated field: tolerated")

    # ---- to_json refuses NaN ------------------------------------ #
    bad = CentralCoordinator()
    bad.register_agent_result("x", 0.9, 0.01, 0.9)
    # Inject NaN into a metric by bypassing validation.
    object.__setattr__(
        bad.global_metrics[0], "reward", float("nan"),
    )
    try:
        bad.to_json()
    except ValueError as exc:
        print("NaN in to_json rejected:", exc)

    # ---- reset ------------------------------------------------- #
    removed = coord.reset()
    assert removed > 0
    assert coord.size == 0
    print("reset        :", removed, "records removed")

    print("\nSmoke test passed.")
