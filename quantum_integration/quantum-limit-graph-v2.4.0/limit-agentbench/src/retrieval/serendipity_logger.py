# src/retrieval/serendipity_logger.py

"""
Serendipity Trace Logger
========================

Captures unexpected efficiency gains or retrieval shortcuts discovered by
agents. Integrates with the Negawatt Reward System to reward low-energy
retrieval paths.

Enhancements
------------
- ``SerendipityLoggerConfig`` — frozen, validated: reward rate, threshold,
  baseline cost per hop, bounded storage caps, signature length.
- ``SerendipityEvent`` / ``NegawattReward`` — frozen dataclasses with full
  validation, UTC timestamps, and serialization.
- **Fixed ID collisions** — SHA-256 + uuid4-backed IDs replace the
  microsecond-resolution MD5 scheme.
- **Fixed naive timestamps** — `datetime.now(timezone.utc)` everywhere.
- **Bounded storage** — `deque(maxlen=...)` for events and rewards;
  `max_patterns` cap with LRU eviction of the least efficient signature.
- **Thread safety** — `RLock` guards every mutation and read.
- **Full validation** of every argument; strict / non-strict modes.
- **Atomic export** — `tempfile` + `os.replace`; parent dirs are created.
- **Deterministic tie-breaking** in ``get_efficient_patterns``.
- Serialization on the logger and both dataclasses.
- ``statistics()``, ``__repr__``, custom ``SerendipityError``, lazy ``%s``
  logging, and a comprehensive ``__main__`` smoke test.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import tempfile
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


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

    # Reward / threshold.
    negawatt_reward_rate: float = 100.0          # Points per Wh saved
    min_savings_threshold_pct: float = 10.0      # Minimum savings to log

    # Baseline estimation for shortcut discovery.
    baseline_energy_per_hop_wh: float = 1e-3

    # Pattern signature length (number of leading nodes).
    pattern_signature_length: int = 3

    # Bounded storage caps.
    max_events: int = 10_000
    max_rewards: int = 10_000
    max_patterns: int = 1_000
    max_instances_per_pattern: int = 100

    # Event-type allowlist.
    valid_event_types: tuple = (
        "shortcut", "efficiency_gain", "unexpected_path",
    )

    def __post_init__(self) -> None:
        if self.negawatt_reward_rate <= 0:
            raise SerendipityError(
                "negawatt_reward_rate must be > 0."
            )
        if not 0.0 <= self.min_savings_threshold_pct <= 100.0:
            raise SerendipityError(
                "min_savings_threshold_pct must be in [0, 100]."
            )
        if self.baseline_energy_per_hop_wh < 0:
            raise SerendipityError(
                "baseline_energy_per_hop_wh must be >= 0."
            )
        if self.pattern_signature_length <= 0:
            raise SerendipityError(
                "pattern_signature_length must be > 0."
            )
        for name in (
            "max_events", "max_rewards", "max_patterns",
            "max_instances_per_pattern",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value <= 0:
                raise SerendipityError(f"{name} must be a positive int.")
        if not isinstance(self.valid_event_types, tuple):
            raise SerendipityError("valid_event_types must be a tuple.")
        for t in self.valid_event_types:
            if not isinstance(t, str) or not t:
                raise SerendipityError(
                    "valid_event_types entries must be non-empty strings."
                )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SerendipityLoggerConfig":
        if not isinstance(data, Mapping):
            raise SerendipityError(
                f"SerendipityLoggerConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        kwargs: Dict[str, Any] = {}
        for key in (
            "negawatt_reward_rate", "min_savings_threshold_pct",
            "baseline_energy_per_hop_wh", "pattern_signature_length",
            "max_events", "max_rewards", "max_patterns",
            "max_instances_per_pattern",
        ):
            if key in data:
                kwargs[key] = data[key]
        if "valid_event_types" in data:
            kwargs["valid_event_types"] = tuple(data["valid_event_types"])
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Dataclasses
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SerendipityEvent:
    """Immutable record of a serendipitous efficiency discovery."""

    event_id: str
    event_type: str              # "shortcut" | "efficiency_gain" | "unexpected_path"
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
            raise SerendipityError("event_id must be a non-empty string.")
        if not isinstance(self.event_type, str) or not self.event_type:
            raise SerendipityError(
                "event_type must be a non-empty string."
            )
        if not isinstance(self.description, str) or not self.description:
            raise SerendipityError(
                "description must be a non-empty string."
            )
        for name in (
            "baseline_energy_wh", "actual_energy_wh",
            "energy_saved_wh", "savings_pct", "negawatt_reward",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SerendipityError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise SerendipityError(
                    f"{name} must be finite, got {value!r}."
                )
        if self.baseline_energy_wh < 0:
            raise SerendipityError("baseline_energy_wh must be >= 0.")
        if self.actual_energy_wh < 0:
            raise SerendipityError("actual_energy_wh must be >= 0.")
        if not isinstance(self.retrieval_path, tuple):
            object.__setattr__(
                self, "retrieval_path", tuple(self.retrieval_path)
            )
        if not isinstance(self.context, Mapping):
            raise SerendipityError("context must be a Mapping.")
        if not isinstance(self.timestamp, (int, float)) or isinstance(
            self.timestamp, bool
        ):
            raise SerendipityError("timestamp must be numeric.")

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
    def from_dict(cls, data: Mapping[str, Any]) -> "SerendipityEvent":
        if not isinstance(data, Mapping):
            raise SerendipityError(
                "SerendipityEvent.from_dict expects a Mapping."
            )
        ts = data.get("created_at")
        if isinstance(ts, str):
            created_at = datetime.fromisoformat(ts)
            if not created_at.tzinfo:
                created_at = created_at.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            created_at = ts
        else:
            created_at = datetime.now(timezone.utc)
        return cls(
            event_id=str(data["event_id"]),
            event_type=str(data["event_type"]),
            description=str(data["description"]),
            baseline_energy_wh=float(data["baseline_energy_wh"]),
            actual_energy_wh=float(data["actual_energy_wh"]),
            energy_saved_wh=float(data["energy_saved_wh"]),
            savings_pct=float(data.get("savings_pct", 0.0)),
            retrieval_path=tuple(data.get("retrieval_path", [])),
            context=dict(data.get("context", {})),
            timestamp=float(data.get("timestamp", time.time())),
            negawatt_reward=float(data.get("negawatt_reward", 0.0)),
            created_at=created_at,
        )

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
        if not isinstance(self.reward_id, str) or not self.reward_id:
            raise SerendipityError("reward_id must be a non-empty string.")
        if not isinstance(self.agent_id, str) or not self.agent_id:
            raise SerendipityError("agent_id must be a non-empty string.")
        if not isinstance(self.reason, str) or not self.reason:
            raise SerendipityError("reason must be a non-empty string.")
        for name in ("energy_saved_wh", "reward_points"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SerendipityError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise SerendipityError(
                    f"{name} must be finite, got {value!r}."
                )
        if self.energy_saved_wh < 0:
            raise SerendipityError("energy_saved_wh must be >= 0.")
        if self.reward_points < 0:
            raise SerendipityError("reward_points must be >= 0.")

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
    def from_dict(cls, data: Mapping[str, Any]) -> "NegawattReward":
        if not isinstance(data, Mapping):
            raise SerendipityError(
                "NegawattReward.from_dict expects a Mapping."
            )
        ts = data.get("created_at")
        if isinstance(ts, str):
            created_at = datetime.fromisoformat(ts)
            if not created_at.tzinfo:
                created_at = created_at.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            created_at = ts
        else:
            created_at = datetime.now(timezone.utc)
        return cls(
            reward_id=str(data["reward_id"]),
            agent_id=str(data["agent_id"]),
            energy_saved_wh=float(data.get("energy_saved_wh", 0.0)),
            reward_points=float(data.get("reward_points", 0.0)),
            reason=str(data.get("reason", "")),
            timestamp=float(data.get("timestamp", time.time())),
            created_at=created_at,
        )

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

    Thread-safe, serializable, and bounded in memory. The original public API
    is preserved; new parameters are keyword-only.

    Responsibilities
    ----------------
    - Capture unexpected efficiency gains.
    - Track retrieval shortcuts discovered by agents.
    - Calculate negawatt rewards for energy savings.
    - Build a knowledge base of efficient patterns.
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
                negawatt_reward_rate=float(negawatt_reward_rate),
                min_savings_threshold_pct=float(min_savings_threshold_pct),
            )
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.reward_rate: float = self._config.negawatt_reward_rate
        self.min_threshold: float = self._config.min_savings_threshold_pct

        self._lock = threading.RLock()
        self.serendipity_events: Deque[SerendipityEvent] = deque(
            maxlen=self._config.max_events
        )
        self.negawatt_rewards: Deque[NegawattReward] = deque(
            maxlen=self._config.max_rewards
        )
        self.efficient_patterns: Dict[str, Deque[Dict[str, Any]]] = {}
        self._pattern_evictions: int = 0

        self.total_energy_saved: float = 0.0
        self.total_rewards_issued: float = 0.0
        self._started_at: float = time.time()

        logger.debug(
            "SerendipityTraceLogger initialized "
            "(reward_rate=%.3f, min_threshold=%.2f%%, "
            "max_events=%d, max_patterns=%d, strict=%s)",
            self.reward_rate, self.min_threshold,
            self._config.max_events, self._config.max_patterns,
            self._strict,
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

        Returns the event when the savings exceed the configured threshold,
        otherwise ``None``.
        """
        description = self._validate_text("description", description)
        baseline = self._validate_number(
            "baseline_energy_wh", baseline_energy_wh, low=0.0,
        )
        actual = self._validate_number(
            "actual_energy_wh", actual_energy_wh, low=0.0,
        )
        path = self._validate_path(retrieval_path)
        ctx = self._validate_context(context)
        agent_id = self._validate_text("agent_id", agent_id)

        if baseline is None or actual is None or path is None:
            return None

        energy_saved = baseline - actual
        savings_pct = (energy_saved / baseline * 100.0) if baseline > 0 else 0.0

        if savings_pct < self.min_threshold or energy_saved <= 0:
            logger.debug(
                "Efficiency gain below threshold (%.3f%% < %.3f%%); "
                "not logged.",
                savings_pct, self.min_threshold,
            )
            return None

        reward = energy_saved * self.reward_rate
        event = SerendipityEvent(
            event_id=self._generate_event_id(description, "efficiency_gain"),
            event_type="efficiency_gain",
            description=description,
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

        self._issue_negawatt_reward(agent_id, energy_saved, description)
        self._learn_efficient_pattern(path, energy_saved)

        logger.debug(
            "Logged efficiency_gain: saved=%.5f Wh (%.2f%%), reward=%.3f.",
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
        """
        Log the discovery of a retrieval shortcut.

        The baseline is derived from ``len(original_path) ×
        config.baseline_energy_per_hop_wh`` — configurable now.
        """
        description = self._validate_text("description", description)
        original = self._validate_path(original_path)
        shortcut = self._validate_path(shortcut_path)
        saved = self._validate_number(
            "energy_saved_wh", energy_saved_wh, low=0.0,
        )
        ctx = self._validate_context(context)
        agent_id = self._validate_text("agent_id", agent_id)

        if original is None or shortcut is None or saved is None:
            return None

        baseline = len(original) * self._config.baseline_energy_per_hop_wh
        actual = max(0.0, baseline - saved)
        savings_pct = (saved / baseline * 100.0) if baseline > 0 else 0.0

        reward = saved * self.reward_rate
        event = SerendipityEvent(
            event_id=self._generate_event_id(description, "shortcut"),
            event_type="shortcut",
            description=description,
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

        self._issue_negawatt_reward(agent_id, saved, description)
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
        """
        Log an unexpected-but-efficient retrieval path.

        Returns the event when the savings exceed the configured threshold,
        otherwise ``None``.
        """
        description = self._validate_text("description", description)
        path_validated = self._validate_path(path)
        energy = self._validate_number("energy_wh", energy_wh, low=0.0)
        expected = self._validate_number(
            "expected_energy_wh", expected_energy_wh, low=0.0,
        )
        ctx = self._validate_context(context)
        agent_id = self._validate_text("agent_id", agent_id)

        if path_validated is None or energy is None or expected is None:
            return None

        energy_saved = expected - energy
        if energy_saved <= 0:
            logger.debug(
                "Path not efficient (saved=%.5f <= 0); skipping.",
                energy_saved,
            )
            return None

        savings_pct = (energy_saved / expected * 100.0) if expected > 0 else 0.0
        if savings_pct < self.min_threshold:
            logger.debug(
                "Unexpected path below threshold (%.3f%% < %.3f%%); "
                "skipping.",
                savings_pct, self.min_threshold,
            )
            return None

        reward = energy_saved * self.reward_rate
        event = SerendipityEvent(
            event_id=self._generate_event_id(description, "unexpected_path"),
            event_type="unexpected_path",
            description=description,
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

        self._issue_negawatt_reward(agent_id, energy_saved, description)
        self._learn_efficient_pattern(path_validated, energy_saved)

        logger.debug(
            "Logged unexpected_path: saved=%.5f Wh (%.2f%%).",
            energy_saved, savings_pct,
        )
        return event

    def get_efficient_patterns(
        self, top_n: int = 10
    ) -> List[Dict[str, Any]]:
        """Return the top-N patterns by total energy saved (descending)."""
        if not isinstance(top_n, int) or top_n <= 0:
            raise SerendipityError("top_n must be a positive int.")

        with self._lock:
            patterns = {
                sig: list(instances)
                for sig, instances in self.efficient_patterns.items()
            }

        pattern_scores: List[Dict[str, Any]] = []
        for pattern_sig, instances in patterns.items():
            if not instances:
                continue
            total_savings = sum(inst["energy_saved"] for inst in instances)
            avg_savings = total_savings / len(instances)
            pattern_scores.append({
                "pattern": pattern_sig,
                "instances": len(instances),
                "total_energy_saved_wh": total_savings,
                "avg_energy_saved_wh": avg_savings,
                "example_path": instances[0]["path"],
            })

        # Deterministic tie-break: pattern name ascending.
        pattern_scores.sort(
            key=lambda x: (-x["total_energy_saved_wh"], x["pattern"])
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

        avg_savings_pct = sum(e.savings_pct for e in events) / len(events)
        type_counts: Dict[str, int] = {}
        for event in events:
            type_counts[event.event_type] = (
                type_counts.get(event.event_type, 0) + 1
            )

        best_event = max(
            events, key=lambda e: (e.energy_saved_wh, e.event_id)
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

    def get_agent_rewards(self, agent_id: str) -> Dict[str, Any]:
        """Return rewards issued to ``agent_id``."""
        agent_id = self._validate_text("agent_id", agent_id)
        with self._lock:
            rewards = [
                r for r in self.negawatt_rewards if r.agent_id == agent_id
            ]

        if not rewards:
            return {
                "agent_id": agent_id,
                "total_rewards": 0.0,
                "total_energy_saved_wh": 0.0,
                "reward_count": 0,
                "recent_rewards": [],
            }

        total_points = sum(r.reward_points for r in rewards)
        total_energy_saved = sum(r.energy_saved_wh for r in rewards)

        return {
            "agent_id": agent_id,
            "total_rewards": total_points,
            "total_energy_saved_wh": total_energy_saved,
            "reward_count": len(rewards),
            "recent_rewards": [r.to_dict() for r in rewards[-5:]],
        }

    def export_serendipity_log(self, filepath: str) -> int:
        """
        Atomically export the full serendipity log to a JSON file.

        Returns the number of bytes written. Parent directories are created
        automatically. Uses ``tempfile`` + ``os.replace`` so a crash never
        leaves a truncated file.
        """
        if not isinstance(filepath, str) or not filepath:
            raise SerendipityError(
                "filepath must be a non-empty string."
            )
        path = Path(filepath)

        with self._lock:
            payload = {
                "events": [e.to_dict() for e in self.serendipity_events],
                "rewards": [r.to_dict() for r in self.negawatt_rewards],
                "efficient_patterns": self.get_efficient_patterns(),
                "summary": self.get_serendipity_summary(),
            }

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise SerendipityError(
                f"Could not create parent directory for {path}: {exc}"
            ) from exc

        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".", suffix=".tmp",
                dir=str(path.parent),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, default=str)
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except OSError as exc:
            raise SerendipityError(
                f"Could not write serendipity log to {path}: {exc}"
            ) from exc

        size = path.stat().st_size
        logger.debug(
            "Exported serendipity log to %s (%d bytes).", path, size,
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
            by_type[e.event_type] = by_type.get(e.event_type, 0) + 1

        by_agent: Dict[str, int] = {}
        for r in rewards:
            by_agent[r.agent_id] = by_agent.get(r.agent_id, 0) + 1

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
        logger.debug("SerendipityTraceLogger reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- internal
    def _issue_negawatt_reward(
        self, agent_id: str, energy_saved_wh: float, reason: str
    ) -> Optional[NegawattReward]:
        """Issue a negawatt reward for the given savings."""
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
        self, path: Sequence[str], energy_saved: float
    ) -> None:
        """Record an efficient pattern signature (bounded)."""
        sig_len = self._config.pattern_signature_length
        pattern_sig = "->".join(path[:sig_len]) if path else "__empty__"

        with self._lock:
            bucket = self.efficient_patterns.get(pattern_sig)
            if bucket is None:
                bucket = deque(
                    maxlen=self._config.max_instances_per_pattern
                )
                self.efficient_patterns[pattern_sig] = bucket

                # Enforce global pattern cap by evicting the signature with
                # the lowest cumulative savings.
                if len(self.efficient_patterns) > self._config.max_patterns:
                    worst_sig = min(
                        self.efficient_patterns.keys(),
                        key=lambda k: sum(
                            i["energy_saved"]
                            for i in self.efficient_patterns[k]
                        ),
                    )
                    if worst_sig != pattern_sig:
                        self.efficient_patterns.pop(worst_sig, None)
                        self._pattern_evictions += 1

            bucket.append({
                "path": list(path),
                "energy_saved": float(energy_saved),
                "timestamp": time.time(),
            })

    def _generate_event_id(self, description: str, event_type: str) -> str:
        """
        Return a collision-resistant event ID.

        Uses SHA-256 over (uuid4, description, event_type, time) so two
        events created in the same microsecond still receive distinct IDs.
        """
        raw = (
            f"{uuid.uuid4().hex}:"
            f"{description}:"
            f"{event_type}:"
            f"{time.time()}"
        )
        return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:16]

    def _generate_reward_id(self, agent_id: str) -> str:
        raw = f"{uuid.uuid4().hex}:{agent_id}:{time.time()}"
        return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:16]

    # ---------------------------------------------------------- validation
    def _validate_text(self, name: str, value: Any) -> Optional[str]:
        if not isinstance(value, str) or not value:
            msg = f"{name} must be a non-empty string, got {value!r}."
            if self._strict:
                raise SerendipityError(msg)
            logger.warning("%s", msg)
            return None
        return value

    def _validate_number(
        self, name: str, value: Any, *, low: Optional[float] = None
    ) -> Optional[float]:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = f"{name} must be numeric, got {type(value).__name__}."
            if self._strict:
                raise SerendipityError(msg)
            logger.warning("%s", msg)
            return None
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"{name} must be finite, got {value!r}."
            if self._strict:
                raise SerendipityError(msg)
            logger.warning("%s", msg)
            return None
        if low is not None and fv < low:
            msg = f"{name} must be >= {low}, got {fv}."
            if self._strict:
                raise SerendipityError(msg)
            logger.warning("%s", msg)
            return None
        return fv

    def _validate_path(
        self, path: Any
    ) -> Optional[Tuple[str, ...]]:
        if not isinstance(path, (list, tuple)):
            msg = (
                f"retrieval_path must be a list or tuple, got "
                f"{type(path).__name__}."
            )
            if self._strict:
                raise SerendipityError(msg)
            logger.warning("%s", msg)
            return None
        validated: List[str] = []
        for i, node in enumerate(path):
            if not isinstance(node, str) or not node:
                msg = f"path[{i}] must be a non-empty string."
                if self._strict:
                    raise SerendipityError(msg)
                logger.warning("%s", msg)
                continue
            validated.append(node)
        return tuple(validated)

    def _validate_context(
        self, context: Any
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
            logger.warning("%s", msg)
            return {}
        return dict(context)

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "counters": {
                    "total_energy_saved_wh": self.total_energy_saved,
                    "total_rewards_issued": self.total_rewards_issued,
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
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SerendipityTraceLogger":
        if not isinstance(data, Mapping):
            raise SerendipityError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = SerendipityLoggerConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        logger_ = cls(config=cfg, strict=bool(data.get("strict", True)))

        with logger_._lock:
            for entry in data.get("events", []):
                logger_.serendipity_events.append(
                    SerendipityEvent.from_dict(entry)
                )
            for entry in data.get("rewards", []):
                logger_.negawatt_rewards.append(
                    NegawattReward.from_dict(entry)
                )
            counters = dict(data.get("counters", {}) or {})
            logger_.total_energy_saved = float(
                counters.get("total_energy_saved_wh", 0.0)
            )
            logger_.total_rewards_issued = float(
                counters.get("total_rewards_issued", 0.0)
            )
            logger_._pattern_evictions = int(
                counters.get("pattern_evictions", 0)
            )
            logger_._started_at = float(data.get("started_at", time.time()))
        return logger_

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "SerendipityTraceLogger":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise SerendipityError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "SerendipityTraceLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
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
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "NegawattReward",
    "SerendipityError",
    "SerendipityEvent",
    "SerendipityLoggerConfig",
    "SerendipityTraceLogger",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.serendipity_logger
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import tempfile

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Happy path -------------------------------------------------- #
    slog = SerendipityTraceLogger()
    print("repr       :", slog)

    # Efficiency gain exceeding the 10% threshold.
    e1 = slog.log_efficiency_gain(
        "Cache hit eliminated a redundant retrieval",
        baseline_energy_wh=0.005,
        actual_energy_wh=0.002,
        retrieval_path=["query", "cache", "answer"],
        agent_id="agent_a",
    )
    print("e1         :", e1)

    # Below-threshold gain — returns None.
    e_none = slog.log_efficiency_gain(
        "Marginal gain",
        baseline_energy_wh=0.005,
        actual_energy_wh=0.0049,
        retrieval_path=["a", "b"],
    )
    assert e_none is None
    print("below-thr  : OK (None)")

    # Shortcut discovery.
    e2 = slog.log_shortcut_discovery(
        "Found a 2-hop shortcut",
        original_path=["a", "b", "c", "d", "e"],
        shortcut_path=["a", "e"],
        energy_saved_wh=0.0025,
        agent_id="agent_b",
    )
    print("e2         :", e2)

    # Unexpected path.
    e3 = slog.log_unexpected_path(
        "Random exploration hit the answer directly",
        path=["a", "z"],
        energy_wh=0.0005,
        expected_energy_wh=0.003,
        agent_id="agent_c",
    )
    print("e3         :", e3)

    # Non-efficient unexpected path — returns None.
    e_none2 = slog.log_unexpected_path(
        "Slow path",
        path=["a", "b"],
        energy_wh=0.005,
        expected_energy_wh=0.003,
    )
    assert e_none2 is None
    print("not-eff    : OK (None)")

    # ---- Pattern knowledge base ------------------------------------- #
    print("patterns   :", slog.get_efficient_patterns(top_n=5))

    # ---- Summary ---------------------------------------------------- #
    summary = slog.get_serendipity_summary()
    print("summary    :", {
        k: v for k, v in summary.items()
        if k != "best_discovery"
    })
    print("best       :", summary["best_discovery"])

    # ---- Per-agent rewards ----------------------------------------- #
    print("agent_a    :", {
        k: v for k, v in slog.get_agent_rewards("agent_a").items()
        if k != "recent_rewards"
    })
    print("agent_x    :", slog.get_agent_rewards("agent_x"))

    # ---- Bug fix: collision-resistant IDs -------------------------- #
    # Create many events in a tight loop — the original MD5(desc + ts)
    # scheme would collide on microsecond-resolution timestamps.
    ids = set()
    fast = SerendipityTraceLogger()
    for _ in range(200):
        e = fast.log_efficiency_gain(
            "same description", 0.01, 0.001, ["a", "b"],
        )
        if e is not None:
            ids.add(e.event_id)
    assert len(ids) == 200, f"ID collisions: only {len(ids)} unique IDs"
    print(f"unique ids : OK ({len(ids)} distinct)")

    # ---- Bounded storage ------------------------------------------- #
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
    assert bounded.pattern_count <= 2
    print(f"bounded    : events={bounded.event_count} "
          f"rewards={bounded.reward_count} "
          f"patterns={bounded.pattern_count}")

    # ---- Atomic export --------------------------------------------- #
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "nested" / "log.json"
        size = slog.export_serendipity_log(str(path))
        assert path.exists() and size > 0
        print(f"export     : {size} bytes at {path}")

        # Read back and verify shape.
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        assert "events" in data and "rewards" in data and "summary" in data
        print("readback   : OK")

    # ---- Statistics ------------------------------------------------- #
    print("statistics :", {
        k: v for k, v in slog.statistics().items()
        if k not in ("config", "uptime_seconds", "rewards_by_agent")
    })

    # ---- Serialization round-trip ---------------------------------- #
    payload = slog.to_json()
    restored = SerendipityTraceLogger.from_json(payload)
    assert restored.to_dict() == slog.to_dict()
    print("Round-trip OK.")

    # ---- Dataclass round-trips ------------------------------------- #
    if e1 is not None:
        p = json.dumps(e1.to_dict(), default=str)
        assert SerendipityEvent.from_dict(json.loads(p)).to_dict() == e1.to_dict()
    if e2 is not None:
        p = json.dumps(e2.to_dict(), default=str)
        assert SerendipityEvent.from_dict(json.loads(p)).to_dict() == e2.to_dict()
    print("Dataclass round-trips OK.")

    # ---- Context manager ------------------------------------------- #
    with SerendipityTraceLogger() as scoped:
        scoped.log_efficiency_gain("ctx", 0.01, 0.001, ["a"])
        assert scoped.event_count == 1
    print("Context    : OK")

    # ---- Reset ------------------------------------------------------ #
    removed = slog.reset()
    print("reset      :", removed, "event(s) removed; new size:",
          slog.event_count)

    # ---- Validation failures ---------------------------------------- #
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
        lambda: strict.log_efficiency_gain("", 0.01, 0.001, ["a"]),
        lambda: strict.log_efficiency_gain("d", -1.0, 0.001, ["a"]),
        lambda: strict.log_efficiency_gain("d", 0.01, -0.001, ["a"]),
        lambda: strict.log_efficiency_gain("d", 0.01, 0.001, "not-a-path"),  # type: ignore[arg-type]
        lambda: strict.log_efficiency_gain("d", 0.01, 0.001, ["a"], context="bad"),  # type: ignore[arg-type]
        lambda: strict.log_shortcut_discovery("d", "bad", ["b"], 0.001),  # type: ignore[arg-type]
        lambda: strict.log_unexpected_path("d", ["a"], float("nan"), 0.01),
        lambda: strict.get_efficient_patterns(top_n=0),
        lambda: strict.export_serendipity_log(""),
        lambda: strict.get_agent_rewards(""),
    ):
        try:
            bad_call()
        except SerendipityError as exc:
            print("Rejected   :", exc)

    # ---- Non-strict coerces / skips -------------------------------- #
    lenient = SerendipityTraceLogger(strict=False)
    r = lenient.log_efficiency_gain("", 0.01, 0.001, ["a"])
    assert r is None
    print("lenient    : OK (skipped invalid input)")

    print("\nSmoke test passed.")
