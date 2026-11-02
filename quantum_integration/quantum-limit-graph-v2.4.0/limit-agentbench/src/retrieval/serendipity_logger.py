# src/retrieval/serendipity_logger.py

"""
Serendipity Trace Logger
========================

Captures unexpected efficiency gains or retrieval shortcuts discovered by
agents. Integrates with the Negawatt Reward System to reward low-energy
retrieval paths.

Design
------
- Frozen, validated ``SerendipityLoggerConfig``.
- ``SerendipityEvent`` / ``NegawattReward``: frozen dataclasses with
  full validation, UTC timestamps, and serialization.
- **Collision-resistant IDs**: SHA-256 over ``(uuid4, description,
  type, time)``.
- **Bounded storage**:
    * events / rewards: ``deque(maxlen=...)``.
    * patterns: capped at ``config.max_patterns``; the least
      cost-effective *pre-existing* signature is evicted first, and
      the just-added pattern is never the eviction victim.
- **Non-strict mode returns ``None``** on any invalid input, including
  empty ``description`` / ``agent_id``.
- **Round-trip serialization**: ``to_dict`` / ``from_dict`` /
  ``to_json`` / ``from_json``. Patterns are preserved when
  ``include_history=True``. ``to_json`` uses ``allow_nan=False``.
- **Atomic export**: ``tempfile`` + ``os.replace``; parent dirs
  created on demand. Export payload reuses ``to_dict`` and is
  re-ingestible via ``from_dict``.

Version: 2.0.0
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import numbers
import os
import tempfile
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "NegawattReward",
    "SerendipityError",
    "SerendipityEvent",
    "SerendipityLoggerConfig",
    "SerendipityTraceLogger",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SerendipityError(ValueError):
    """Raised for invalid serendipity logger inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SerendipityLoggerConfig:
    """Tunable parameters for :class:`SerendipityTraceLogger`."""

    negawatt_reward_rate: float = 100.0
    min_savings_threshold_pct: float = 10.0
    baseline_energy_per_hop_wh: float = 1e-3
    pattern_signature_length: int = 3
    max_events: int = 10_000
    max_rewards: int = 10_000
    max_patterns: int = 1_000
    max_instances_per_pattern: int = 100
    valid_event_types: tuple = (
        "shortcut", "efficiency_gain", "unexpected_path",
    )

    def __post_init__(self) -> None:
        if (
            isinstance(self.negawatt_reward_rate, bool)
            or not isinstance(self.negawatt_reward_rate, (int, float))
            or not math.isfinite(float(self.negawatt_reward_rate))
            or self.negawatt_reward_rate <= 0
        ):
            raise SerendipityError(
                "negawatt_reward_rate must be finite and > 0."
            )
        if not 0.0 <= self.min_savings_threshold_pct <= 100.0:
            raise SerendipityError(
                "min_savings_threshold_pct must be in [0, 100]."
            )
        if (
            isinstance(self.baseline_energy_per_hop_wh, bool)
            or not isinstance(
                self.baseline_energy_per_hop_wh, (int, float)
            )
            or not math.isfinite(
                float(self.baseline_energy_per_hop_wh)
            )
            or self.baseline_energy_per_hop_wh < 0
        ):
            raise SerendipityError(
                "baseline_energy_per_hop_wh must be finite and >= 0."
            )
        if (
            isinstance(self.pattern_signature_length, bool)
            or not isinstance(self.pattern_signature_length, int)
            or self.pattern_signature_length <= 0
        ):
            raise SerendipityError(
                "pattern_signature_length must be a positive int."
            )
        for name in (
            "max_events", "max_rewards",
            "max_patterns", "max_instances_per_pattern",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
            ):
                raise SerendipityError(
                    f"{name} must be a positive int."
                )
        if not isinstance(self.valid_event_types, tuple):
            raise SerendipityError(
                "valid_event_types must be a tuple."
            )
        for t in self.valid_event_types:
            if not isinstance(t, str) or not t:
                raise SerendipityError(
                    "valid_event_types entries must be non-empty "
                    "strings."
                )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "SerendipityLoggerConfig":
        if not isinstance(data, Mapping):
            raise SerendipityError(
                f"SerendipityLoggerConfig.from_dict expects a "
                f"Mapping, got {type(data).__name__}."
            )
        try:
            kwargs: Dict[str, Any] = {}
            for key in (
                "negawatt_reward_rate",
                "min_savings_threshold_pct",
                "baseline_energy_per_hop_wh",
                "pattern_signature_length",
                "max_events", "max_rewards",
                "max_patterns", "max_instances_per_pattern",
            ):
                if key in data:
                    kwargs[key] = data[key]
            if "valid_event_types" in data:
                raw = data["valid_event_types"]
                if not isinstance(raw, (list, tuple)):
                    raise SerendipityError(
                        "valid_event_types must be a list or tuple."
                    )
                kwargs["valid_event_types"] = tuple(raw)
            return cls(**kwargs)
        except (TypeError, ValueError) as exc:
            if isinstance(exc, SerendipityError):
                raise
            raise SerendipityError(
                f"invalid SerendipityLoggerConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Dataclasses
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SerendipityEvent:
    """Immutable record of a serendipitous efficiency discovery."""

    event_id: str
    event_type: str
    description: str
    baseline_energy_wh: float
    actual_energy_wh: float
    energy_saved_wh: float
    savings_pct: float
    retrieval_path: Tuple[str, ...]
    context: Mapping[str, Any]
    timestamp: float
    negawatt_reward: float
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id:
            raise SerendipityError(
                "event_id must be a non-empty string."
            )
        if not isinstance(self.event_type, str) or (
            not self.event_type
        ):
            raise SerendipityError(
                "event_type must be a non-empty string."
            )
        if not isinstance(self.description, str) or (
            not self.description
        ):
            raise SerendipityError(
                "description must be a non-empty string."
            )
        for name in (
            "baseline_energy_wh", "actual_energy_wh",
            "energy_saved_wh", "savings_pct", "negawatt_reward",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
            ):
                raise SerendipityError(
                    f"{name} must be numeric."
                )
            fv = float(value)
            if not math.isfinite(fv):
                raise SerendipityError(
                    f"{name} must be finite, got {value!r}."
                )
        if self.baseline_energy_wh < 0:
            raise SerendipityError(
                "baseline_energy_wh must be >= 0."
            )
        if self.actual_energy_wh < 0:
            raise SerendipityError(
                "actual_energy_wh must be >= 0."
            )
        if self.energy_saved_wh < 0:
            raise SerendipityError(
                "energy_saved_wh must be >= 0."
            )
        if not 0.0 <= self.savings_pct <= 100.0:
            raise SerendipityError(
                f"savings_pct must be in [0, 100], "
                f"got {self.savings_pct}."
            )
        if not isinstance(self.retrieval_path, tuple):
            object.__setattr__(
                self, "retrieval_path",
                tuple(self.retrieval_path),
            )
        for i, node in enumerate(self.retrieval_path):
            if not isinstance(node, str) or not node:
                raise SerendipityError(
                    f"retrieval_path[{i}] must be a non-empty string."
                )
        if not isinstance(self.context, Mapping):
            raise SerendipityError("context must be a Mapping.")
        if (
            isinstance(self.timestamp, bool)
            or not isinstance(self.timestamp, (int, float))
            or not math.isfinite(float(self.timestamp))
        ):
            raise SerendipityError(
                "timestamp must be a finite number."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "description": self.description,
            "baseline_energy_wh": self.baseline_energy_wh,
            "actual_energy_wh": self.actual_energy_wh,
            "energy_saved_wh": self.energy_saved_wh,
            "savings_pct": self.savings_pct,
            "retrieval_path": list(self.retrieval_path),
            "context": dict(self.context),
            "timestamp": self.timestamp,
            "negawatt_reward": self.negawatt_reward,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "SerendipityEvent":
        if not isinstance(data, Mapping):
            raise SerendipityError(
                "SerendipityEvent.from_dict expects a Mapping."
            )
        try:
            ts = data.get("created_at")
            if isinstance(ts, str):
                created_at = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not created_at.tzinfo:
                    created_at = created_at.replace(
                        tzinfo=timezone.utc
                    )
            elif isinstance(ts, datetime):
                created_at = ts
            else:
                created_at = datetime.now(timezone.utc)

            ctx = data.get("context") or {}
            if not isinstance(ctx, Mapping):
                raise SerendipityError(
                    "context must be a Mapping."
                )

            return cls(
                event_id=str(data["event_id"]),
                event_type=str(data["event_type"]),
                description=str(data["description"]),
                baseline_energy_wh=float(
                    data["baseline_energy_wh"]
                ),
                actual_energy_wh=float(
                    data["actual_energy_wh"]
                ),
                energy_saved_wh=float(
                    data["energy_saved_wh"]
                ),
                savings_pct=float(
                    data.get("savings_pct", 0.0)
                ),
                retrieval_path=tuple(
                    data.get("retrieval_path", []) or []
                ),
                context=dict(ctx),
                timestamp=float(
                    data.get("timestamp", time.time())
                ),
                negawatt_reward=float(
                    data.get("negawatt_reward", 0.0)
                ),
                created_at=created_at,
            )
        except KeyError as exc:
            raise SerendipityError(
                f"SerendipityEvent payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, SerendipityError):
                raise
            raise SerendipityError(
                f"invalid SerendipityEvent payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "SerendipityEvent("
            f"id={self.event_id[:8]}..., "
            f"type={self.event_type!r}, "
            f"saved_wh={self.energy_saved_wh:.5f}, "
            f"savings_pct={self.savings_pct:.1f}, "
            f"reward={self.negawatt_reward:.3f})"
        )


@dataclass(frozen=True)
class NegawattReward:
    """Immutable record of an energy-efficiency reward."""

    reward_id: str
    agent_id: str
    energy_saved_wh: float
    reward_points: float
    reason: str
    timestamp: float
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.reward_id, str) or (
            not self.reward_id
        ):
            raise SerendipityError(
                "reward_id must be a non-empty string."
            )
        if not isinstance(self.agent_id, str) or (
            not self.agent_id
        ):
            raise SerendipityError(
                "agent_id must be a non-empty string."
            )
        if not isinstance(self.reason, str) or not self.reason:
            raise SerendipityError(
                "reason must be a non-empty string."
            )
        for name in ("energy_saved_wh", "reward_points"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
            ):
                raise SerendipityError(
                    f"{name} must be numeric."
                )
            fv = float(value)
            if not math.isfinite(fv):
                raise SerendipityError(
                    f"{name} must be finite, got {value!r}."
                )
        if self.energy_saved_wh < 0:
            raise SerendipityError(
                "energy_saved_wh must be >= 0."
            )
        if self.reward_points < 0:
            raise SerendipityError(
                "reward_points must be >= 0."
            )
        if (
            isinstance(self.timestamp, bool)
            or not isinstance(self.timestamp, (int, float))
            or not math.isfinite(float(self.timestamp))
        ):
            raise SerendipityError(
                "timestamp must be a finite number."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "reward_id": self.reward_id,
            "agent_id": self.agent_id,
            "energy_saved_wh": self.energy_saved_wh,
            "reward_points": self.reward_points,
            "reason": self.reason,
            "timestamp": self.timestamp,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "NegawattReward":
        if not isinstance(data, Mapping):
            raise SerendipityError(
                "NegawattReward.from_dict expects a Mapping."
            )
        try:
            ts = data.get("created_at")
            if isinstance(ts, str):
                created_at = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not created_at.tzinfo:
                    created_at = created_at.replace(
                        tzinfo=timezone.utc
                    )
            elif isinstance(ts, datetime):
                created_at = ts
            else:
                created_at = datetime.now(timezone.utc)

            return cls(
                reward_id=str(data["reward_id"]),
                agent_id=str(data["agent_id"]),
                energy_saved_wh=float(
                    data.get("energy_saved_wh", 0.0)
                ),
                reward_points=float(
                    data.get("reward_points", 0.0)
                ),
                reason=str(data.get("reason", "")),
                timestamp=float(
                    data.get("timestamp", time.time())
                ),
                created_at=created_at,
            )
        except KeyError as exc:
            raise SerendipityError(
                f"NegawattReward payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, SerendipityError):
                raise
            raise SerendipityError(
                f"invalid NegawattReward payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "NegawattReward("
            f"id={self.reward_id[:8]}..., "
            f"agent={self.agent_id!r}, "
            f"saved_wh={self.energy_saved_wh:.5f}, "
            f"points={self.reward_points:.3f})"
        )


# --------------------------------------------------------------------------- #
# Logger
# --------------------------------------------------------------------------- #
class SerendipityTraceLogger:
    """
    Logger for serendipitous efficiency discoveries.

    Thread-safe, serializable, bounded in memory.
    """

    def __init__(
        self,
        negawatt_reward_rate: float = 100.0,
        min_savings_threshold_pct: float = 10.0,
        *,
        config: Optional[SerendipityLoggerConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = SerendipityLoggerConfig(
                negawatt_reward_rate=float(
                    negawatt_reward_rate
                ),
                min_savings_threshold_pct=float(
                    min_savings_threshold_pct
                ),
            )
        self._strict = bool(strict)

        self.reward_rate: float = (
            self._config.negawatt_reward_rate
        )
        self.min_threshold: float = (
            self._config.min_savings_threshold_pct
        )

        self._lock = threading.RLock()
        self.serendipity_events: Deque[SerendipityEvent] = deque(
            maxlen=self._config.max_events
        )
        self.negawatt_rewards: Deque[NegawattReward] = deque(
            maxlen=self._config.max_rewards
        )
        self.efficient_patterns: Dict[
            str, Deque[Dict[str, Any]]
        ] = {}
        self._pattern_evictions: int = 0

        self.total_energy_saved: float = 0.0
        self.total_rewards_issued: float = 0.0
        self._started_at: float = time.time()

        logger.debug(
            "SerendipityTraceLogger initialized "
            "(reward_rate=%.3f, min_threshold=%.2f%%, "
            "max_events=%d, max_patterns=%d, strict=%s)",
            self.reward_rate, self.min_threshold,
            self._config.max_events,
            self._config.max_patterns, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> SerendipityLoggerConfig:
        return self._config

    @property
    def event_count(self) -> int:
        with self._lock:
            return len(self.serendipity_events)

    @property
    def reward_count(self) -> int:
        with self._lock:
            return len(self.negawatt_rewards)

    @property
    def pattern_count(self) -> int:
        with self._lock:
            return len(self.efficient_patterns)

    # ---------------------------------------------------------- public API
    def log_efficiency_gain(
        self,
        description: str,
        baseline_energy_wh: float,
        actual_energy_wh: float,
        retrieval_path: Sequence[str],
        context: Optional[Dict[str, Any]] = None,
        agent_id: str = "default_agent",
    ) -> Optional[SerendipityEvent]:
        """
        Log an efficiency-gain discovery.

        Returns ``None`` when any input is invalid in non-strict mode,
        or when the savings are below the configured threshold.
        """
        description_v = self._validate_text(
            "description", description,
        )
        baseline = self._validate_number(
            "baseline_energy_wh", baseline_energy_wh, low=0.0,
        )
        actual = self._validate_number(
            "actual_energy_wh", actual_energy_wh, low=0.0,
        )
        path = self._validate_path(retrieval_path)
        ctx = self._validate_context(context)
        agent_id_v = self._validate_text("agent_id", agent_id)

        if (
            description_v is None
            or baseline is None
            or actual is None
            or path is None
            or agent_id_v is None
        ):
            return None

        energy_saved = baseline - actual
        savings_pct = (
            energy_saved / baseline * 100.0
            if baseline > 0 else 0.0
        )

        if savings_pct < self.min_threshold or (
            energy_saved <= 0
        ):
            logger.debug(
                "Efficiency gain below threshold "
                "(%.3f%% < %.3f%%); not logged.",
                savings_pct, self.min_threshold,
            )
            return None

        reward = energy_saved * self.reward_rate
        event = SerendipityEvent(
            event_id=self._generate_event_id(
                description_v, "efficiency_gain",
            ),
            event_type="efficiency_gain",
            description=description_v,
            baseline_energy_wh=baseline,
            actual_energy_wh=actual,
            energy_saved_wh=energy_saved,
            savings_pct=savings_pct,
            retrieval_path=path,
            context=ctx,
            timestamp=time.time(),
            negawatt_reward=reward,
        )

        with self._lock:
            self.serendipity_events.append(event)
            self.total_energy_saved += energy_saved

        self._issue_negawatt_reward(
            agent_id_v, energy_saved, description_v,
        )
        self._learn_efficient_pattern(path, energy_saved)

        logger.debug(
            "Logged efficiency_gain: saved=%.5f Wh (%.2f%%), "
            "reward=%.3f.",
            energy_saved, savings_pct, reward,
        )
        return event

    def log_shortcut_discovery(
        self,
        description: str,
        original_path: Sequence[str],
        shortcut_path: Sequence[str],
        energy_saved_wh: float,
        context: Optional[Dict[str, Any]] = None,
        agent_id: str = "default_agent",
    ) -> Optional[SerendipityEvent]:
        """Log the discovery of a retrieval shortcut."""
        description_v = self._validate_text(
            "description", description,
        )
        original = self._validate_path(original_path)
        shortcut = self._validate_path(shortcut_path)
        saved = self._validate_number(
            "energy_saved_wh", energy_saved_wh, low=0.0,
        )
        ctx = self._validate_context(context)
        agent_id_v = self._validate_text("agent_id", agent_id)

        if (
            description_v is None
            or original is None
            or shortcut is None
            or saved is None
            or agent_id_v is None
        ):
            return None

        baseline = (
            len(original)
            * self._config.baseline_energy_per_hop_wh
        )
        actual = max(0.0, baseline - saved)
        savings_pct = (
            saved / baseline * 100.0
            if baseline > 0 else 0.0
        )

        reward = saved * self.reward_rate
        event = SerendipityEvent(
            event_id=self._generate_event_id(
                description_v, "shortcut",
            ),
            event_type="shortcut",
            description=description_v,
            baseline_energy_wh=baseline,
            actual_energy_wh=actual,
            energy_saved_wh=saved,
            savings_pct=savings_pct,
            retrieval_path=shortcut,
            context={
                "original_path": list(original),
                "shortcut_path": list(shortcut),
                **dict(ctx),
            },
            timestamp=time.time(),
            negawatt_reward=reward,
        )

        with self._lock:
            self.serendipity_events.append(event)
            self.total_energy_saved += saved

        self._issue_negawatt_reward(
            agent_id_v, saved, description_v,
        )
        self._learn_efficient_pattern(shortcut, saved)

        logger.debug(
            "Logged shortcut: baseline=%.5f, saved=%.5f (%.2f%%).",
            baseline, saved, savings_pct,
        )
        return event

    def log_unexpected_path(
        self,
        description: str,
        path: Sequence[str],
        energy_wh: float,
        expected_energy_wh: float,
        context: Optional[Dict[str, Any]] = None,
        agent_id: str = "default_agent",
    ) -> Optional[SerendipityEvent]:
        """Log an unexpected-but-efficient retrieval path."""
        description_v = self._validate_text(
            "description", description,
        )
        path_validated = self._validate_path(path)
        energy = self._validate_number(
            "energy_wh", energy_wh, low=0.0,
        )
        expected = self._validate_number(
            "expected_energy_wh", expected_energy_wh, low=0.0,
        )
        ctx = self._validate_context(context)
        agent_id_v = self._validate_text("agent_id", agent_id)

        if (
            description_v is None
            or path_validated is None
            or energy is None
            or expected is None
            or agent_id_v is None
        ):
            return None

        energy_saved = expected - energy
        if energy_saved <= 0:
            logger.debug(
                "Path not efficient (saved=%.5f <= 0); skipping.",
                energy_saved,
            )
            return None

        savings_pct = (
            energy_saved / expected * 100.0
            if expected > 0 else 0.0
        )
        if savings_pct < self.min_threshold:
            logger.debug(
                "Unexpected path below threshold "
                "(%.3f%% < %.3f%%); skipping.",
                savings_pct, self.min_threshold,
            )
            return None

        reward = energy_saved * self.reward_rate
        event = SerendipityEvent(
            event_id=self._generate_event_id(
                description_v, "unexpected_path",
            ),
            event_type="unexpected_path",
            description=description_v,
            baseline_energy_wh=expected,
            actual_energy_wh=energy,
            energy_saved_wh=energy_saved,
            savings_pct=savings_pct,
            retrieval_path=path_validated,
            context=ctx,
            timestamp=time.time(),
            negawatt_reward=reward,
        )

        with self._lock:
            self.serendipity_events.append(event)
            self.total_energy_saved += energy_saved

        self._issue_negawatt_reward(
            agent_id_v, energy_saved, description_v,
        )
        self._learn_efficient_pattern(
            path_validated, energy_saved,
        )

        logger.debug(
            "Logged unexpected_path: saved=%.5f Wh (%.2f%%).",
            energy_saved, savings_pct,
        )
        return event

    def get_efficient_patterns(
        self, top_n: int = 10,
    ) -> List[Dict[str, Any]]:
        """Return the top-N patterns by total energy saved."""
        if (
            isinstance(top_n, bool)
            or not isinstance(top_n, int)
            or top_n <= 0
        ):
            raise SerendipityError("top_n must be a positive int.")

        with self._lock:
            patterns = {
                sig: list(instances)
                for sig, instances
                in self.efficient_patterns.items()
            }

        pattern_scores: List[Dict[str, Any]] = []
        for pattern_sig, instances in patterns.items():
            if not instances:
                continue
            total_savings = sum(
                inst["energy_saved"] for inst in instances
            )
            avg_savings = total_savings / len(instances)
            best_instance = max(
                instances, key=lambda i: i["energy_saved"],
            )
            pattern_scores.append({
                "pattern": pattern_sig,
                "instances": len(instances),
                "total_energy_saved_wh": total_savings,
                "avg_energy_saved_wh": avg_savings,
                "example_path": best_instance["path"],
            })

        pattern_scores.sort(
            key=lambda x: (
                -x["total_energy_saved_wh"], x["pattern"],
            )
        )
        return pattern_scores[:top_n]

    def get_serendipity_summary(self) -> Dict[str, Any]:
        """Return the backward-compatible summary of discoveries."""
        with self._lock:
            events = list(self.serendipity_events)
            total_saved = self.total_energy_saved
            total_rewards = self.total_rewards_issued
            pattern_count = len(self.efficient_patterns)

        if not events:
            return {
                "status": "no_events",
                "total_events": 0,
                "total_energy_saved_wh": 0.0,
                "total_rewards_issued": 0.0,
                "avg_savings_pct": 0.0,
                "events_by_type": {},
                "best_discovery": None,
                "efficient_patterns_learned": pattern_count,
            }

        avg_savings_pct = (
            sum(e.savings_pct for e in events) / len(events)
        )
        type_counts: Dict[str, int] = {}
        for event in events:
            type_counts[event.event_type] = (
                type_counts.get(event.event_type, 0) + 1
            )

        best_event = max(
            events,
            key=lambda e: (e.energy_saved_wh, e.event_id),
        )

        return {
            "total_events": len(events),
            "total_energy_saved_wh": total_saved,
            "total_rewards_issued": total_rewards,
            "avg_savings_pct": avg_savings_pct,
            "events_by_type": type_counts,
            "best_discovery": {
                "description": best_event.description,
                "energy_saved_wh": best_event.energy_saved_wh,
                "savings_pct": best_event.savings_pct,
                "reward": best_event.negawatt_reward,
            },
            "efficient_patterns_learned": pattern_count,
        }

    def get_agent_rewards(
        self, agent_id: str,
    ) -> Dict[str, Any]:
        """Return rewards issued to ``agent_id``."""
        agent_id_v = self._validate_text("agent_id", agent_id)
        if agent_id_v is None:
            if self._strict:
                raise SerendipityError(
                    "agent_id must be a non-empty string."
                )
            return {
                "agent_id": "",
                "total_rewards": 0.0,
                "total_energy_saved_wh": 0.0,
                "reward_count": 0,
                "recent_rewards": [],
            }
        with self._lock:
            rewards = [
                r for r in self.negawatt_rewards
                if r.agent_id == agent_id_v
            ]

        if not rewards:
            return {
                "agent_id": agent_id_v,
                "total_rewards": 0.0,
                "total_energy_saved_wh": 0.0,
                "reward_count": 0,
                "recent_rewards": [],
            }

        total_points = sum(
            r.reward_points for r in rewards
        )
        total_energy_saved = sum(
            r.energy_saved_wh for r in rewards
        )

        return {
            "agent_id": agent_id_v,
            "total_rewards": total_points,
            "total_energy_saved_wh": total_energy_saved,
            "reward_count": len(rewards),
            "recent_rewards": [
                r.to_dict() for r in rewards[-5:]
            ],
        }

    def export_serendipity_log(self, filepath: str) -> int:
        """
        Atomically export the full serendipity log to a JSON file.

        Returns the number of bytes written. The payload is exactly
        ``to_dict(include_history=True)`` plus a ``"summary"`` field —
        re-ingestible via :meth:`from_dict`.
        """
        if not isinstance(filepath, str) or not filepath:
            raise SerendipityError(
                "filepath must be a non-empty string."
            )
        path = Path(filepath)

        with self._lock:
            payload = self.to_dict(include_history=True)
            payload["summary"] = self.get_serendipity_summary()

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise SerendipityError(
                f"could not create parent directory for {path}: "
                f"{exc}"
            ) from exc

        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".", suffix=".tmp",
                dir=str(path.parent),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(
                        payload, f,
                        indent=2,
                        allow_nan=False,
                        sort_keys=True,
                    )
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except (OSError, ValueError) as exc:
            raise SerendipityError(
                f"could not write serendipity log to {path}: {exc}"
            ) from exc

        size = path.stat().st_size
        logger.debug(
            "Exported serendipity log to %s (%d bytes).",
            path, size,
        )
        return size

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return an extended statistics snapshot."""
        with self._lock:
            events = list(self.serendipity_events)
            rewards = list(self.negawatt_rewards)
            total_saved = self.total_energy_saved
            total_rewards = self.total_rewards_issued
            pattern_count = len(self.efficient_patterns)
            evictions = self._pattern_evictions
            started_at = self._started_at

        by_type: Dict[str, int] = {}
        for e in events:
            by_type[e.event_type] = (
                by_type.get(e.event_type, 0) + 1
            )

        by_agent: Dict[str, int] = {}
        for r in rewards:
            by_agent[r.agent_id] = (
                by_agent.get(r.agent_id, 0) + 1
            )

        return {
            "total_events": len(events),
            "total_energy_saved_wh": total_saved,
            "total_rewards_issued": total_rewards,
            "efficient_patterns": pattern_count,
            "pattern_evictions": evictions,
            "events_by_type": by_type,
            "rewards_by_agent": by_agent,
            "config": self._config.to_dict(),
            "uptime_seconds": time.time() - started_at,
        }

    def reset(self, *, clear_patterns: bool = True) -> int:
        """Reset counters and (optionally) the pattern knowledge base."""
        with self._lock:
            removed = len(self.serendipity_events)
            self.serendipity_events.clear()
            self.negawatt_rewards.clear()
            if clear_patterns:
                self.efficient_patterns.clear()
            self.total_energy_saved = 0.0
            self.total_rewards_issued = 0.0
            self._pattern_evictions = 0
            self._started_at = time.time()
        logger.debug(
            "SerendipityTraceLogger reset (removed %d).", removed,
        )
        return removed

    # ---------------------------------------------------------- internal
    def _issue_negawatt_reward(
        self,
        agent_id: str,
        energy_saved_wh: float,
        reason: str,
    ) -> Optional[NegawattReward]:
        if energy_saved_wh <= 0:
            return None
        reward_points = energy_saved_wh * self.reward_rate
        reward = NegawattReward(
            reward_id=self._generate_reward_id(agent_id),
            agent_id=agent_id,
            energy_saved_wh=energy_saved_wh,
            reward_points=reward_points,
            reason=reason,
            timestamp=time.time(),
        )
        with self._lock:
            self.negawatt_rewards.append(reward)
            self.total_rewards_issued += reward_points
        return reward

    def _learn_efficient_pattern(
        self, path: Sequence[str], energy_saved: float,
    ) -> None:
        """
        Record an efficient pattern signature (bounded).

        The new instance is appended *before* the eviction check so
        the just-added bucket has a non-zero savings total, and the
        just-added signature is excluded from eviction candidates so
        a fresh pattern is never immediately evicted. This closes the
        previously unbounded-growth bug.
        """
        sig_len = self._config.pattern_signature_length
        pattern_sig = (
            "->".join(path[:sig_len]) if path else "__empty__"
        )

        with self._lock:
            bucket = self.efficient_patterns.get(pattern_sig)
            if bucket is None:
                bucket = deque(
                    maxlen=self._config.max_instances_per_pattern
                )
                self.efficient_patterns[pattern_sig] = bucket

            bucket.append({
                "path": list(path),
                "energy_saved": float(energy_saved),
                "timestamp": time.time(),
            })

            while (
                len(self.efficient_patterns)
                > self._config.max_patterns
            ):
                candidates = [
                    (
                        k,
                        sum(
                            i["energy_saved"]
                            for i in b
                        ),
                    )
                    for k, b in self.efficient_patterns.items()
                    if k != pattern_sig
                ]
                if not candidates:
                    break
                worst_sig, _ = min(
                    candidates, key=lambda kv: (kv[1], kv[0]),
                )
                self.efficient_patterns.pop(worst_sig, None)
                self._pattern_evictions += 1

    def _generate_event_id(
        self, description: str, event_type: str,
    ) -> str:
        """Collision-resistant event ID."""
        raw = (
            f"{uuid.uuid4().hex}:"
            f"{description}:"
            f"{event_type}:"
            f"{time.time()}"
        )
        return hashlib.sha256(
            raw.encode("utf-8", errors="replace")
        ).hexdigest()[:16]

    def _generate_reward_id(self, agent_id: str) -> str:
        raw = f"{uuid.uuid4().hex}:{agent_id}:{time.time()}"
        return hashlib.sha256(
            raw.encode("utf-8", errors="replace")
        ).hexdigest()[:16]

    # ---------------------------------------------------------- validation
    def _validate_text(
        self, name: str, value: Any,
    ) -> Optional[str]:
        if not isinstance(value, str) or not value:
            msg = (
                f"{name} must be a non-empty string, "
                f"got {value!r}."
            )
            if self._strict:
                raise SerendipityError(msg)
            logger.warning("%s Returning None.", msg)
            return None
        return value

    def _validate_number(
        self,
        name: str,
        value: Any,
        *,
        low: Optional[float] = None,
    ) -> Optional[float]:
        if isinstance(value, bool) or not isinstance(
            value, numbers.Real,
        ):
            msg = (
                f"{name} must be numeric, "
                f"got {type(value).__name__}."
            )
            if self._strict:
                raise SerendipityError(msg)
            logger.warning("%s Returning None.", msg)
            return None
        fv = float(value)
        if not math.isfinite(fv):
            msg = f"{name} must be finite, got {value!r}."
            if self._strict:
                raise SerendipityError(msg)
            logger.warning("%s Returning None.", msg)
            return None
        if low is not None and fv < low:
            msg = f"{name} must be >= {low}, got {fv}."
            if self._strict:
                raise SerendipityError(msg)
            logger.warning("%s Returning None.", msg)
            return None
        return fv

    def _validate_path(
        self, path: Any,
    ) -> Optional[Tuple[str, ...]]:
        if not isinstance(path, (list, tuple)):
            msg = (
                f"retrieval_path must be a list or tuple, "
                f"got {type(path).__name__}."
            )
            if self._strict:
                raise SerendipityError(msg)
            logger.warning("%s Returning None.", msg)
            return None
        validated: List[str] = []
        for i, node in enumerate(path):
            if not isinstance(node, str) or not node:
                msg = (
                    f"path[{i}] must be a non-empty string."
                )
                if self._strict:
                    raise SerendipityError(msg)
                logger.warning("%s Skipping.", msg)
                continue
            validated.append(node)
        return tuple(validated)

    def _validate_context(
        self, context: Any,
    ) -> Dict[str, Any]:
        if context is None:
            return {}
        if not isinstance(context, Mapping):
            msg = (
                f"context must be a Mapping or None, got "
                f"{type(context).__name__}."
            )
            if self._strict:
                raise SerendipityError(msg)
            logger.warning("%s Using empty context.", msg)
            return {}
        return dict(context)

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "counters": {
                    "total_energy_saved_wh": (
                        self.total_energy_saved
                    ),
                    "total_rewards_issued": (
                        self.total_rewards_issued
                    ),
                    "pattern_evictions": self._pattern_evictions,
                    "event_count": len(self.serendipity_events),
                    "reward_count": len(self.negawatt_rewards),
                    "pattern_count": len(self.efficient_patterns),
                },
                "started_at": self._started_at,
            }
            if include_history:
                payload["events"] = [
                    e.to_dict() for e in self.serendipity_events
                ]
                payload["rewards"] = [
                    r.to_dict() for r in self.negawatt_rewards
                ]
                payload["efficient_patterns"] = {
                    sig: list(instances)
                    for sig, instances
                    in self.efficient_patterns.items()
                }
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "SerendipityTraceLogger":
        if not isinstance(data, Mapping):
            raise SerendipityError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            raw_config = data.get("config") or {}
            if not isinstance(raw_config, Mapping):
                raise SerendipityError(
                    "config must be a Mapping."
                )
            cfg = SerendipityLoggerConfig.from_dict(
                dict(raw_config)
            )

            strict_raw = data.get("strict", True)
            if not isinstance(strict_raw, bool):
                raise SerendipityError(
                    f"strict must be bool, got "
                    f"{type(strict_raw).__name__}."
                )

            logger_ = cls(config=cfg, strict=strict_raw)

            raw_events = data.get("events") or []
            if not isinstance(raw_events, list):
                raise SerendipityError(
                    f"events must be a list, got "
                    f"{type(raw_events).__name__}."
                )
            raw_rewards = data.get("rewards") or []
            if not isinstance(raw_rewards, list):
                raise SerendipityError(
                    f"rewards must be a list, got "
                    f"{type(raw_rewards).__name__}."
                )
            raw_patterns = data.get("efficient_patterns") or {}
            if not isinstance(raw_patterns, Mapping):
                raise SerendipityError(
                    f"efficient_patterns must be a Mapping, got "
                    f"{type(raw_patterns).__name__}."
                )
            raw_counters = data.get("counters") or {}
            if not isinstance(raw_counters, Mapping):
                raise SerendipityError(
                    "counters must be a Mapping."
                )

            with logger_._lock:
                for entry in raw_events:
                    if not isinstance(entry, Mapping):
                        raise SerendipityError(
                            "events entries must be Mappings."
                        )
                    logger_.serendipity_events.append(
                        SerendipityEvent.from_dict(entry)
                    )
                for entry in raw_rewards:
                    if not isinstance(entry, Mapping):
                        raise SerendipityError(
                            "rewards entries must be Mappings."
                        )
                    logger_.negawatt_rewards.append(
                        NegawattReward.from_dict(entry)
                    )

                # ---- Patterns -------------------------------------
                for sig, instances in raw_patterns.items():
                    if not isinstance(sig, str) or not sig:
                        raise SerendipityError(
                            f"efficient_patterns key {sig!r} "
                            f"must be a non-empty string."
                        )
                    if not isinstance(instances, list):
                        raise SerendipityError(
                            f"efficient_patterns[{sig!r}] must "
                            f"be a list."
                        )
                    bucket = deque(
                        maxlen=cfg.max_instances_per_pattern
                    )
                    for inst in instances:
                        if not isinstance(inst, Mapping):
                            raise SerendipityError(
                                f"efficient_patterns[{sig!r}] "
                                f"entries must be Mappings."
                            )
                        try:
                            bucket.append({
                                "path": list(
                                    inst.get("path") or []
                                ),
                                "energy_saved": float(
                                    inst.get(
                                        "energy_saved", 0.0
                                    )
                                ),
                                "timestamp": float(
                                    inst.get(
                                        "timestamp",
                                        time.time(),
                                    )
                                ),
                            })
                        except (TypeError, ValueError) as exc:
                            raise SerendipityError(
                                f"invalid pattern instance in "
                                f"efficient_patterns[{sig!r}]: "
                                f"{exc}"
                            ) from exc
                    logger_.efficient_patterns[sig] = bucket

                if (
                    len(logger_.efficient_patterns)
                    > cfg.max_patterns
                ):
                    logger.warning(
                        "efficient_patterns has %d entries, "
                        "exceeds max_patterns=%d; truncating to "
                        "top-%d by total savings.",
                        len(logger_.efficient_patterns),
                        cfg.max_patterns, cfg.max_patterns,
                    )
                    scored = [
                        (
                            sig,
                            sum(
                                i["energy_saved"]
                                for i in bucket
                            ),
                        )
                        for sig, bucket
                        in logger_.efficient_patterns.items()
                    ]
                    scored.sort(
                        key=lambda t: (-t[1], t[0])
                    )
                    keep = {
                        sig for sig, _ in scored[: cfg.max_patterns]
                    }
                    for sig in list(
                        logger_.efficient_patterns.keys()
                    ):
                        if sig not in keep:
                            logger_.efficient_patterns.pop(
                                sig, None
                            )
                            logger_._pattern_evictions += 1

                # ---- Counters -------------------------------------
                try:
                    logger_.total_energy_saved = float(
                        raw_counters.get(
                            "total_energy_saved_wh", 0.0,
                        )
                    )
                    logger_.total_rewards_issued = float(
                        raw_counters.get(
                            "total_rewards_issued", 0.0,
                        )
                    )
                    logger_._pattern_evictions = int(
                        raw_counters.get(
                            "pattern_evictions", 0,
                        )
                    )
                except (TypeError, ValueError) as exc:
                    raise SerendipityError(
                        f"invalid counter: {exc}"
                    ) from exc
                if logger_.total_energy_saved < 0:
                    raise SerendipityError(
                        "total_energy_saved_wh must be >= 0."
                    )
                if logger_.total_rewards_issued < 0:
                    raise SerendipityError(
                        "total_rewards_issued must be >= 0."
                    )
                if logger_._pattern_evictions < 0:
                    raise SerendipityError(
                        "pattern_evictions must be >= 0."
                    )
                try:
                    logger_._started_at = float(
                        data.get("started_at", time.time())
                    )
                except (TypeError, ValueError) as exc:
                    raise SerendipityError(
                        f"invalid started_at: {exc}"
                    ) from exc
            return logger_
        except SerendipityError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise SerendipityError(
                f"invalid SerendipityTraceLogger payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_history: bool = False, **kwargs: Any,
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(
            self.to_dict(include_history=include_history),
            **kwargs,
        )

    @classmethod
    def from_json(
        cls, payload: str,
    ) -> "SerendipityTraceLogger":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SerendipityError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "SerendipityTraceLogger":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_type is not None:
            logger.warning(
                "SerendipityTraceLogger scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.event_count

    def __repr__(self) -> str:
        with self._lock:
            return (
                "SerendipityTraceLogger("
                f"events={len(self.serendipity_events)}, "
                f"rewards={len(self.negawatt_rewards)}, "
                f"patterns={len(self.efficient_patterns)}, "
                f"saved_wh={self.total_energy_saved:.5f}, "
                f"reward_rate={self.reward_rate:.1f}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import tempfile

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("VERSION    :", VERSION)

    slog = SerendipityTraceLogger()
    print("repr       :", slog)

    e1 = slog.log_efficiency_gain(
        "Cache hit eliminated a redundant retrieval",
        baseline_energy_wh=0.005,
        actual_energy_wh=0.002,
        retrieval_path=["query", "cache", "answer"],
        agent_id="agent_a",
    )
    assert e1 is not None
    print("e1         :", e1)

    e_none = slog.log_efficiency_gain(
        "Marginal gain",
        baseline_energy_wh=0.005,
        actual_energy_wh=0.0049,
        retrieval_path=["a", "b"],
    )
    assert e_none is None
    print("below-thr  : OK")

    e2 = slog.log_shortcut_discovery(
        "Found a 2-hop shortcut",
        original_path=["a", "b", "c", "d", "e"],
        shortcut_path=["a", "e"],
        energy_saved_wh=0.0025,
        agent_id="agent_b",
    )
    assert e2 is not None

    e3 = slog.log_unexpected_path(
        "Random exploration hit the answer directly",
        path=["a", "z"],
        energy_wh=0.0005,
        expected_energy_wh=0.003,
        agent_id="agent_c",
    )
    assert e3 is not None

    # ---- Pattern cap actually fires ------------------------------ #
    bounded = SerendipityTraceLogger(
        config=SerendipityLoggerConfig(
            max_events=3, max_rewards=3, max_patterns=2,
            max_instances_per_pattern=2,
        )
    )
    for i in range(10):
        bounded.log_efficiency_gain(
            f"event-{i}", 0.01, 0.001,
            [f"n{i}", "b", "c"],
        )
    assert bounded.event_count <= 3
    assert bounded.reward_count <= 3
    assert bounded.pattern_count <= 2, (
        f"pattern cap broken: {bounded.pattern_count} > 2"
    )
    assert bounded._pattern_evictions > 0
    print(
        f"bounded    : events={bounded.event_count} "
        f"rewards={bounded.reward_count} "
        f"patterns={bounded.pattern_count} "
        f"evictions={bounded._pattern_evictions}"
    )

    # ---- Non-strict mode is actually non-strict ----------------- #
    lenient = SerendipityTraceLogger(strict=False)
    assert lenient.log_efficiency_gain(
        "", 0.01, 0.001, ["a"],
    ) is None
    assert lenient.log_efficiency_gain(
        "d", 0.01, 0.001, ["a"], agent_id="",
    ) is None
    print("non-strict : OK (no crash)")

    # ---- ID collision resistance -------------------------------- #
    ids = set()
    fast = SerendipityTraceLogger()
    for _ in range(200):
        e = fast.log_efficiency_gain(
            "same description", 0.01, 0.001, ["a", "b"],
        )
        if e is not None:
            ids.add(e.event_id)
    assert len(ids) == 200
    print(f"unique ids : OK ({len(ids)})")

    # ---- Round-trip preserves patterns ------------------------- #
    payload = slog.to_json(include_history=True)
    restored = SerendipityTraceLogger.from_json(payload)
    assert (
        restored.pattern_count == slog.pattern_count
    ), (
        f"pattern_count drift: {restored.pattern_count} != "
        f"{slog.pattern_count}"
    )
    assert (
        restored.to_dict(include_history=True)
        == slog.to_dict(include_history=True)
    )
    print("round-trip : OK (patterns preserved)")

    # ---- from_dict validation --------------------------------- #
    for bad in (
        {"events": "abc"},
        {"events": None, "rewards": "not-a-list"},
        {"strict": "yes"},
        {"efficient_patterns": "not-a-mapping"},
        {"counters": {"total_energy_saved_wh": "abc"}},
        {"config": {"negawatt_reward_rate": "abc"}},
        {"events": [{"event_id": "x"}]},
    ):
        try:
            SerendipityTraceLogger.from_dict(bad)
        except SerendipityError as exc:
            print(
                "Rejected payload:", str(exc)[:60], "...",
            )

    # ---- to_json rejects NaN ---------------------------------- #
    tainted = SerendipityTraceLogger()
    tainted.log_efficiency_gain("x", 0.01, 0.001, ["a"])
    object.__setattr__(
        tainted._config, "negawatt_reward_rate", float("nan"),
    )
    try:
        tainted.to_json()
    except ValueError as exc:
        print("NaN rejected:", str(exc)[:60])

    # ---- Atomic export ---------------------------------------- #
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "nested" / "log.json"
        size = slog.export_serendipity_log(str(path))
        assert path.exists() and size > 0
        print(f"export     : {size} bytes")

        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        reloaded = SerendipityTraceLogger.from_dict(data)
        assert reloaded.event_count == slog.event_count
        print("re-ingest  : OK")

    # ---- Statistics ------------------------------------------ #
    print("statistics :", {
        k: v for k, v in slog.statistics().items()
        if k not in (
            "config", "uptime_seconds", "rewards_by_agent",
        )
    })

    # ---- Validation failures -------------------------------- #
    for bad_cfg in (
        dict(negawatt_reward_rate=0),
        dict(negawatt_reward_rate=-1),
        dict(min_savings_threshold_pct=-1),
        dict(min_savings_threshold_pct=150),
        dict(baseline_energy_per_hop_wh=-1),
        dict(pattern_signature_length=0),
        dict(max_events=0),
        dict(max_rewards=0),
        dict(max_patterns=0),
        dict(max_instances_per_pattern=0),
    ):
        try:
            SerendipityLoggerConfig(**bad_cfg)  # type: ignore[arg-type]
        except SerendipityError as exc:
            print("Rejected cfg:", exc)

    strict = SerendipityTraceLogger(strict=True)
    for bad_call in (
        lambda: strict.log_efficiency_gain(
            "", 0.01, 0.001, ["a"],
        ),
        lambda: strict.log_efficiency_gain(
            "d", -1.0, 0.001, ["a"],
        ),
        lambda: strict.log_efficiency_gain(
            "d", 0.01, -0.001, ["a"],
        ),
        lambda: strict.log_efficiency_gain(
            "d", 0.01, 0.001, "not-a-path",  # type: ignore[arg-type]
        ),
        lambda: strict.log_efficiency_gain(
            "d", 0.01, 0.001, ["a"],
            context="bad",  # type: ignore[arg-type]
        ),
        lambda: strict.log_shortcut_discovery(
            "d", "bad", ["b"], 0.001,  # type: ignore[arg-type]
        ),
        lambda: strict.log_unexpected_path(
            "d", ["a"], float("nan"), 0.01,
        ),
        lambda: strict.get_efficient_patterns(top_n=0),
        lambda: strict.export_serendipity_log(""),
        lambda: strict.get_agent_rewards(""),
    ):
        try:
            bad_call()
        except SerendipityError as exc:
            print("Rejected   :", str(exc)[:50])

    removed = slog.reset()
    assert slog.event_count == 0
    print("reset      :", removed, "removed")

    print("\nSmoke test passed.")
