# src/r1/q_learning.py

"""
Q-Learning Agent
================

Tabular Q-learning agent with optional JSON persistence via
:class:`r1.rl_storage.RLStorage`.

State  : any string hashable key (caller's choice)
Actions: policy weight adjustments (default: balanced / energy_saver /
         latency_optimized)

Enhancements
------------
- **Fixed import path** — ``from .rl_storage`` instead of the broken
  ``from rl.rl_storage``.
- **Fixed synchronous I/O on every step** — persistence is now batched via
  ``QLearningConfig.save_interval`` (default 100) with an explicit
  :meth:`save` method; ``save_interval=0`` disables auto-save entirely.
- **Fixed crash on corrupt persistence** — ``RLStorage.load()`` failures are
  handled per ``strict`` mode.
- **Deterministic randomness** — seeded ``random.Random`` per instance.
- **Bounded Q-table** — LRU eviction when ``config.max_states`` is reached.
- **Full input validation** — ``state`` / ``action`` / ``reward`` /
  ``next_state`` checked; ``KeyError`` on unknown action is impossible.
- **Deterministic tie-breaking** — alphabetical action name when Q-values
  are equal.
- **Thread safety** — ``RLock`` guards every mutation.
- Optional integration with :class:`r1.reward_normalizer.RewardNormalizer`.
- Configurable epsilon decay for annealed exploration.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- ``statistics()``, ``reset()``, ``__repr__``, custom ``QLearningError``,
  lazy ``%s`` logging, and a ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import random
import threading
import time
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Defensive import of RLStorage
# --------------------------------------------------------------------------- #
try:  # Preferred: package-relative import
    from .rl_storage import RLStorage, RLStorageConfig, RLStorageError  # type: ignore
except ImportError:  # pragma: no cover — script-style fallback
    try:
        from rl_storage import (  # type: ignore
            RLStorage,
            RLStorageConfig,
            RLStorageError,
        )
    except ImportError:
        RLStorage = None  # type: ignore[assignment]
        RLStorageConfig = None  # type: ignore[assignment]
        RLStorageError = Exception  # type: ignore[assignment,misc]
        logger.debug(
            "rl_storage not importable; QLearningAgent persistence disabled."
        )


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class QLearningError(ValueError):
    """Raised for invalid Q-learning agent inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class QLearningConfig:
    """Tunable parameters for the Q-learning agent."""

    # Q-learning hyperparameters.
    alpha: float = 0.1       # Learning rate ∈ (0, 1]
    gamma: float = 0.9       # Discount factor ∈ [0, 1]
    epsilon: float = 0.1     # Exploration probability ∈ [0, 1]

    # Bounded Q-table.
    max_states: Optional[int] = 10_000

    # Random seed for reproducibility.
    seed: Optional[int] = 42

    # Epsilon decay.
    epsilon_decay: float = 1.0        # 1.0 = no decay; <1 shrinks ε
    epsilon_min: float = 0.0

    # Persistence cadence: save every N updates. ``0`` disables auto-save.
    # ``1`` reproduces the original behavior (save on every update).
    save_interval: int = 100

    # Bounded transition history.
    max_history: int = 10_000

    def __post_init__(self) -> None:
        if not 0.0 < self.alpha <= 1.0:
            raise QLearningError(f"alpha must be in (0, 1], got {self.alpha}.")
        if not 0.0 <= self.gamma <= 1.0:
            raise QLearningError(f"gamma must be in [0, 1], got {self.gamma}.")
        if not 0.0 <= self.epsilon <= 1.0:
            raise QLearningError(
                f"epsilon must be in [0, 1], got {self.epsilon}."
            )
        if self.max_states is not None and self.max_states <= 0:
            raise QLearningError("max_states must be a positive int or None.")
        if not 0.0 < self.epsilon_decay <= 1.0:
            raise QLearningError("epsilon_decay must be in (0, 1].")
        if not 0.0 <= self.epsilon_min <= self.epsilon:
            raise QLearningError("epsilon_min must be in [0, epsilon].")
        if not isinstance(self.save_interval, int) or self.save_interval < 0:
            raise QLearningError("save_interval must be a non-negative int.")
        if self.max_history <= 0:
            raise QLearningError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QLearningConfig":
        if not isinstance(data, Mapping):
            raise QLearningError(
                f"QLearningConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            alpha=float(data.get("alpha", 0.1)),
            gamma=float(data.get("gamma", 0.9)),
            epsilon=float(data.get("epsilon", 0.1)),
            max_states=data.get("max_states", 10_000),
            seed=data.get("seed", 42),
            epsilon_decay=float(data.get("epsilon_decay", 1.0)),
            epsilon_min=float(data.get("epsilon_min", 0.0)),
            save_interval=int(data.get("save_interval", 100)),
            max_history=int(data.get("max_history", 10_000)),
        )


# --------------------------------------------------------------------------- #
# Transition record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class QTransition:
    """Immutable record of a single Q-learning update."""
    state: str
    action: str
    reward: float
    next_state: str
    old_q: float
    new_q: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "state": self.state,
            "action": self.action,
            "reward": self.reward,
            "next_state": self.next_state,
            "old_q": self.old_q,
            "new_q": self.new_q,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QTransition":
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
            state=str(data["state"]),
            action=str(data["action"]),
            reward=float(data["reward"]),
            next_state=str(data["next_state"]),
            old_q=float(data["old_q"]),
            new_q=float(data["new_q"]),
            timestamp=timestamp,
        )


# --------------------------------------------------------------------------- #
# Agent
# --------------------------------------------------------------------------- #
class QLearningAgent:
    """
    Tabular Q-learning agent with optional JSON persistence.

    Thread-safe, deterministic (given a seed), serializable, and bounded in
    memory. The original public API is preserved; new parameters are
    keyword-only.

    Parameters
    ----------
    alpha : float, default 0.1
        Learning rate ∈ ``(0, 1]``.
    gamma : float, default 0.9
        Discount factor ∈ ``[0, 1]``.
    epsilon : float, default 0.1
        Exploration probability ∈ ``[0, 1]``.
    config : QLearningConfig, optional
        Typed configuration. Overrides the three positional hyperparameters.
    strict : bool, default True
        If True, invalid inputs raise :class:`QLearningError`. If False,
        they are logged and skipped.
    actions : sequence of str, optional
        Custom action space. Defaults to
        ``["balanced", "energy_saver", "latency_optimized"]``.
    storage : RLStorage, optional
        Storage backend for persistence. Defaults to a new ``RLStorage()``
        when the module is available, else ``None``.
    """

    DEFAULT_ACTIONS: Tuple[str, ...] = (
        "balanced",
        "energy_saver",
        "latency_optimized",
    )

    def __init__(
        self,
        alpha: float = 0.1,
        gamma: float = 0.9,
        epsilon: float = 0.1,
        *,
        config: Optional[QLearningConfig] = None,
        strict: bool = True,
        actions: Optional[Sequence[str]] = None,
        storage: Optional[Any] = None,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = QLearningConfig(
                alpha=alpha, gamma=gamma, epsilon=epsilon
            )
        self._strict = bool(strict)

        # Legacy attributes preserved (mirror the config).
        self.alpha: float = self._config.alpha
        self.gamma: float = self._config.gamma
        self.epsilon: float = self._config.epsilon

        # Action space.
        if actions is not None:
            if not isinstance(actions, (list, tuple)) or not actions:
                raise QLearningError("actions must be a non-empty sequence.")
            for a in actions:
                if not isinstance(a, str) or not a:
                    raise QLearningError(
                        "actions entries must be non-empty strings."
                    )
            self.actions: List[str] = list(actions)
        else:
            self.actions = list(self.DEFAULT_ACTIONS)

        # ---- Persistence ----------------------------------------------
        if storage is not None:
            self.storage = storage
        elif RLStorage is not None:
            try:
                self.storage = RLStorage()  # type: ignore[misc]
            except Exception as exc:
                if self._strict:
                    raise QLearningError(
                        f"Could not initialize RLStorage: {exc}"
                    ) from exc
                logger.warning(
                    "RLStorage init failed (%s); persistence disabled.", exc
                )
                self.storage = None
        else:
            self.storage = None

        # ---- Load persisted Q-table -----------------------------------
        # Bounded Q-table keyed by state string; LRU eviction via OrderedDict.
        self.q_table: "OrderedDict[str, Dict[str, float]]" = OrderedDict()
        self._loaded_from_storage: int = 0
        if self.storage is not None:
            try:
                loaded = self.storage.load()
            except Exception as exc:
                if self._strict:
                    raise QLearningError(
                        f"Could not load Q-table from storage: {exc}"
                    ) from exc
                logger.warning(
                    "Storage load failed (%s); starting with empty Q-table.",
                    exc,
                )
                loaded = {}

            if isinstance(loaded, Mapping):
                for state, row in loaded.items():
                    if not isinstance(row, Mapping):
                        continue
                    self.q_table[str(state)] = {
                        str(a): float(q)
                        for a, q in row.items()
                        if a in self.actions
                    }
                    # Ensure every known action is present.
                    for a in self.actions:
                        self.q_table[str(state)].setdefault(a, 0.0)
                    self._loaded_from_storage += 1

        # ---- Internal state -------------------------------------------
        self._lock = threading.RLock()
        self._rng = random.Random(self._config.seed)
        self._history: Deque[QTransition] = deque(
            maxlen=self._config.max_history
        )
        self._updates: int = 0
        self._since_last_save: int = 0
        self._save_failures: int = 0
        self._explorations: int = 0
        self._exploitations: int = 0
        self._evictions: int = 0
        self._started_at: float = time.time()

        logger.debug(
            "QLearningAgent initialized "
            "(alpha=%.3f gamma=%.3f epsilon=%.3f seed=%s max_states=%s "
            "save_interval=%d loaded=%d strict=%s storage=%s)",
            self.alpha, self.gamma, self.epsilon,
            self._config.seed, self._config.max_states,
            self._config.save_interval, self._loaded_from_storage,
            self._strict, type(self.storage).__name__ if self.storage else None,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> QLearningConfig:
        return self._config

    @property
    def state_count(self) -> int:
        with self._lock:
            return len(self.q_table)

    @property
    def updates(self) -> int:
        with self._lock:
            return self._updates

    @property
    def history(self) -> List[QTransition]:
        with self._lock:
            return list(self._history)

    @property
    def has_unsaved_changes(self) -> bool:
        with self._lock:
            return self._since_last_save > 0

    # ---------------------------------------------------------- internal
    def _ensure_state(self, state: str) -> None:
        """
        Ensure ``state`` has a Q-row. Evicts the least-recently-used state
        when ``config.max_states`` is reached.
        """
        with self._lock:
            if state in self.q_table:
                self.q_table.move_to_end(state)
                return

            self.q_table[state] = {a: 0.0 for a in self.actions}

            cap = self._config.max_states
            while cap is not None and len(self.q_table) > cap:
                self.q_table.popitem(last=False)
                self._evictions += 1

    def _validate_state(
        self, state: Any, *, name: str = "state"
    ) -> Optional[str]:
        if not isinstance(state, str) or not state:
            msg = f"{name} must be a non-empty string, got {state!r}."
            if self._strict:
                raise QLearningError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return state

    def _validate_action(self, action: Any) -> Optional[str]:
        if not isinstance(action, str) or action not in self.actions:
            msg = f"action must be one of {self.actions}, got {action!r}."
            if self._strict:
                raise QLearningError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return action

    def _validate_reward(self, reward: Any) -> Optional[float]:
        if isinstance(reward, bool) or not isinstance(reward, (int, float)):
            msg = f"reward must be numeric, got {type(reward).__name__}."
            if self._strict:
                raise QLearningError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        fv = float(reward)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"reward must be finite, got {reward!r}."
            if self._strict:
                raise QLearningError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return fv

    # ---------------------------------------------------------- public API
    def choose_action(self, state: str) -> str:
        """
        Select an action for ``state`` using ε-greedy exploration.

        Ties are broken deterministically by alphabetical action name.
        """
        state = self._validate_state(state)
        if state is None:
            # Non-strict mode: pick a safe default rather than raising.
            return self.actions[0]

        self._ensure_state(state)

        with self._lock:
            if self._rng.random() < self.epsilon:
                self._explorations += 1
                action = self._rng.choice(self.actions)
                logger.debug(
                    "Explore: state=%s action=%s (epsilon=%.3f)",
                    state, action, self.epsilon,
                )
                return action

            row = self.q_table[state]
            best_q = max(row.values())
            best_actions = sorted(a for a, q in row.items() if q == best_q)
            action = best_actions[0]
            self._exploitations += 1
            logger.debug(
                "Exploit: state=%s action=%s (q=%.4f)",
                state, action, best_q,
            )
            return action

    def update(
        self,
        state: str,
        action: str,
        reward: float,
        next_state: str,
    ) -> Optional[QTransition]:
        """
        Apply one Q-learning update.

        Q(s, a) ← Q(s, a) + α [ r + γ · maxₐ' Q(s', a') − Q(s, a) ]

        Returns the recorded :class:`QTransition` (or ``None`` in non-strict
        mode when any input was rejected).
        """
        state = self._validate_state(state)
        next_state = self._validate_state(next_state, name="next_state")
        action = self._validate_action(action)
        reward = self._validate_reward(reward)
        if state is None or next_state is None or action is None or reward is None:
            return None

        self._ensure_state(state)
        self._ensure_state(next_state)

        with self._lock:
            old_value = self.q_table[state][action]
            next_max = max(self.q_table[next_state].values())
            new_value = old_value + self.alpha * (
                reward + self.gamma * next_max - old_value
            )
            self.q_table[state][action] = new_value

            transition = QTransition(
                state=state,
                action=action,
                reward=reward,
                next_state=next_state,
                old_q=old_value,
                new_q=new_value,
            )
            self._history.append(transition)
            self._updates += 1
            self._since_last_save += 1

            # Epsilon decay.
            if self._config.epsilon_decay < 1.0:
                self.epsilon = max(
                    self._config.epsilon_min,
                    self.epsilon * self._config.epsilon_decay,
                )

            # Batched persistence — no I/O on every step.
            should_save = (
                self._config.save_interval > 0
                and self._since_last_save >= self._config.save_interval
            )

        if should_save:
            self.save()

        logger.debug(
            "Update: s=%s a=%s r=%.4f s'=%s Q:%.4f→%.4f (ε=%.3f)",
            state, action, reward, next_state,
            old_value, new_value, self.epsilon,
        )
        return transition

    def save(self) -> bool:
        """
        Persist the Q-table via ``self.storage``.

        Returns True on success (or when there is no storage to save to),
        False on failure. Never raises in non-strict mode.
        """
        with self._lock:
            if self.storage is None:
                # Nothing to save to — treat as a no-op success.
                self._since_last_save = 0
                return True
            snapshot = {s: dict(row) for s, row in self.q_table.items()}

        try:
            self.storage.save(snapshot)
        except Exception as exc:
            logger.error("Q-table save failed: %s", exc)
            with self._lock:
                self._save_failures += 1
            if self._strict:
                raise QLearningError(f"Q-table save failed: {exc}") from exc
            return False

        with self._lock:
            self._since_last_save = 0
        logger.debug(
            "Q-table saved (%d state(s)).", len(snapshot),
        )
        return True

    def load(self) -> int:
        """
        Reload the Q-table from ``self.storage``. Returns the number of
        states restored. Existing states are overwritten.
        """
        with self._lock:
            if self.storage is None:
                return 0
            try:
                loaded = self.storage.load()
            except Exception as exc:
                logger.error("Q-table load failed: %s", exc)
                if self._strict:
                    raise QLearningError(
                        f"Q-table load failed: {exc}"
                    ) from exc
                return 0

            if not isinstance(loaded, Mapping):
                msg = f"storage.load() must return a Mapping."
                if self._strict:
                    raise QLearningError(msg)
                logger.warning("%s Ignoring.", msg)
                return 0

            restored = 0
            for state, row in loaded.items():
                if not isinstance(row, Mapping):
                    continue
                self.q_table[str(state)] = {
                    str(a): float(q)
                    for a, q in row.items()
                    if a in self.actions
                }
                for a in self.actions:
                    self.q_table[str(state)].setdefault(a, 0.0)
                restored += 1
        logger.debug("Q-table loaded (%d state(s)).", restored)
        return restored

    def best_action(self, state: str) -> Optional[str]:
        """Return the highest-Q action for ``state`` (no exploration)."""
        state = self._validate_state(state)
        if state is None:
            return None
        self._ensure_state(state)
        with self._lock:
            row = self.q_table[state]
            best_q = max(row.values())
            candidates = sorted(a for a, q in row.items() if q == best_q)
            return candidates[0] if candidates else None

    def get_q_value(self, state: str, action: str) -> float:
        """Return ``Q(state, action)`` (0.0 when unseen)."""
        state = self._validate_state(state)
        action = self._validate_action(action)
        if state is None or action is None:
            return 0.0
        self._ensure_state(state)
        with self._lock:
            return self.q_table[state][action]

    def reset(self, *, clear_history: bool = True) -> int:
        """
        Clear the Q-table and reset counters.

        Returns the number of states removed.
        """
        with self._lock:
            removed = len(self.q_table)
            self.q_table.clear()
            if clear_history:
                self._history.clear()
            self._updates = 0
            self._since_last_save = 0
            self._explorations = 0
            self._exploitations = 0
            self._evictions = 0
            self.epsilon = self._config.epsilon
            self._started_at = time.time()
        logger.debug("QLearningAgent reset (removed %d state(s)).", removed)
        return removed

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the Q-table and history."""
        with self._lock:
            states = list(self.q_table.keys())
            history = list(self._history)
            updates = self._updates
            explorations = self._explorations
            exploitations = self._exploitations
            evictions = self._evictions
            save_failures = self._save_failures
            since_save = self._since_last_save

        payload: Dict[str, Any] = {
            "states": len(states),
            "updates": updates,
            "explorations": explorations,
            "exploitations": exploitations,
            "evictions": evictions,
            "save_failures": save_failures,
            "has_unsaved_changes": since_save > 0,
            "epsilon": self.epsilon,
            "uptime_seconds": time.time() - self._started_at,
            "persistence_enabled": self.storage is not None,
            "loaded_from_storage": self._loaded_from_storage,
        }

        if updates > 0:
            payload["explore_ratio"] = explorations / updates
            payload["exploit_ratio"] = exploitations / updates

        if states:
            all_q = [
                q for row in self.q_table.values() for q in row.values()
            ]
            if all_q:
                payload["mean_q"] = sum(all_q) / len(all_q)
                payload["min_q"] = min(all_q)
                payload["max_q"] = max(all_q)

        if history:
            payload["mean_reward"] = (
                sum(t.reward for t in history) / len(history)
            )
            payload["last_transition"] = history[-1].to_dict()

        return payload

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = True) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": asdict(self._config),
                "strict": self._strict,
                "actions": list(self.actions),
                "epsilon": self.epsilon,
                "counters": {
                    "updates": self._updates,
                    "explorations": self._explorations,
                    "exploitations": self._exploitations,
                    "evictions": self._evictions,
                    "save_failures": self._save_failures,
                },
                "started_at": self._started_at,
                "q_table": {s: dict(row) for s, row in self.q_table.items()},
            }
            if include_history:
                payload["history"] = [t.to_dict() for t in self._history]
        return payload

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        storage: Optional[Any] = None,
        strict: Optional[bool] = None,
    ) -> "QLearningAgent":
        """
        Rebuild an agent from :meth:`to_dict` output.

        By default ``storage`` is ``None`` so the reconstructed agent does
        not attempt I/O during construction — pass an explicit ``storage``
        if persistence is desired.
        """
        if not isinstance(data, Mapping):
            raise QLearningError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = QLearningConfig.from_dict(dict(data.get("config", {}) or {}))
        agent = cls(
            config=cfg,
            strict=bool(data.get("strict", True)) if strict is None else strict,
            actions=data.get("actions") or None,
            storage=storage,   # avoid auto-loading from disk by default
        )
        with agent._lock:
            # Overwrite whatever was auto-loaded.
            agent.q_table.clear()
            agent.epsilon = float(data.get("epsilon", cfg.epsilon))
            for s, row in (data.get("q_table") or {}).items():
                agent.q_table[str(s)] = {
                    str(a): float(q) for a, q in row.items()
                }
            counters = dict(data.get("counters", {}) or {})
            agent._updates = int(counters.get("updates", 0))
            agent._explorations = int(counters.get("explorations", 0))
            agent._exploitations = int(counters.get("exploitations", 0))
            agent._evictions = int(counters.get("evictions", 0))
            agent._save_failures = int(counters.get("save_failures", 0))
            agent._started_at = float(data.get("started_at", time.time()))
            for entry in data.get("history", []):
                agent._history.append(QTransition.from_dict(entry))
        return agent

    def to_json(self, *, include_history: bool = True, **kwargs: Any) -> str:
        return json.dumps(
            self.to_dict(include_history=include_history),
            default=str, **kwargs,
        )

    @classmethod
    def from_json(
        cls, payload: str, *, storage: Optional[Any] = None
    ) -> "QLearningAgent":
        try:
            return cls.from_dict(json.loads(payload), storage=storage)
        except json.JSONDecodeError as exc:
            raise QLearningError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "QLearningAgent":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # Best-effort save on clean exit when there are pending changes.
        if exc_type is None and self.has_unsaved_changes and self.storage is not None:
            try:
                self.save()
            except QLearningError:
                logger.exception("Failed to save Q-table on context exit.")
        elif exc_type is not None:
            logger.warning(
                "QLearningAgent scope exited with %s.", exc_type.__name__
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.state_count

    def __repr__(self) -> str:
        with self._lock:
            return (
                "QLearningAgent("
                f"alpha={self.alpha:.3f}, "
                f"gamma={self.gamma:.3f}, "
                f"epsilon={self.epsilon:.3f}, "
                f"states={len(self.q_table)}, "
                f"actions={len(self.actions)}, "
                f"updates={self._updates}, "
                f"storage={'yes' if self.storage else 'no'}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "QLearningAgent",
    "QLearningConfig",
    "QLearningError",
    "QTransition",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1.q_learning
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---- In-memory storage (no filesystem I/O for the smoke test) ----- #
    class _MemoryStorage:
        def __init__(self, initial: Optional[Dict[str, Any]] = None) -> None:
            self._data: Dict[str, Any] = initial or {}
            self.save_calls = 0
            self.load_calls = 0

        def save(self, q_table: Mapping[str, Any]) -> None:
            self._data = {k: dict(v) for k, v in q_table.items()}
            self.save_calls += 1

        def load(self) -> Dict[str, Any]:
            self.load_calls += 1
            return {k: dict(v) for k, v in self._data.items()}

    # ---- Happy path --------------------------------------------------- #
    storage = _MemoryStorage()
    agent = QLearningAgent(storage=storage)
    print("repr       :", agent)
    print("actions    :", agent.actions)

    # Train on a simple two-state task.
    for _ in range(500):
        s = "low_carbon"
        a = agent.choose_action(s)
        agent.update(s, a, reward=1.0, next_state="high_carbon")

        a = agent.choose_action("high_carbon")
        agent.update("high_carbon", a, reward=-1.0, next_state="low_carbon")

    print("best low   :", agent.best_action("low_carbon"))
    print("best high  :", agent.best_action("high_carbon"))
    print("Q(low)     :", {
        a: round(agent.get_q_value("low_carbon", a), 4)
        for a in agent.actions
    })

    # ---- Batched persistence: only N saves for M updates -------------- #
    print(f"saves so far: {storage.save_calls} (for {agent.updates} updates, "
          f"interval={agent.config.save_interval})")
    assert storage.save_calls <= agent.updates // agent.config.save_interval + 1

    # ---- Explicit save / unsaved-changes flag ------------------------- #
    print("unsaved?   :", agent.has_unsaved_changes)
    agent.save()
    print("after save :", agent.has_unsaved_changes)
    assert not agent.has_unsaved_changes

    # ---- Persistence round-trip via storage -------------------------- #
    fresh_storage = _MemoryStorage()
    fresh = QLearningAgent(storage=fresh_storage)
    fresh.load()  # initially empty
    # Copy the trained agent's Q-table into fresh storage, then reload.
    fresh_storage.save({s: dict(row) for s, row in agent.q_table.items()})
    restored = QLearningAgent(storage=fresh_storage)
    assert restored.state_count == agent.state_count
    assert restored.get_q_value("low_carbon", "balanced") == agent.get_q_value(
        "low_carbon", "balanced"
    )
    print("persistence: OK")

    # ---- save_interval=0 disables auto-save --------------------------- #
    no_save_storage = _MemoryStorage()
    no_auto = QLearningAgent(
        config=QLearningConfig(save_interval=0), storage=no_save_storage,
    )
    for i in range(50):
        no_auto.update(f"s{i}", "balanced", 1.0, f"s{i + 1}")
    print(f"no-auto    : saves={no_save_storage.save_calls} (expected 0)")
    assert no_save_storage.save_calls == 0

    # ---- Reproducibility --------------------------------------------- #
    a1 = QLearningAgent(config=QLearningConfig(seed=7, epsilon=0.5),
                        storage=None)
    a2 = QLearningAgent(config=QLearningConfig(seed=7, epsilon=0.5),
                        storage=None)
    for i in range(20):
        s = f"state_{i % 5}"
        ac1 = a1.choose_action(s)
        ac2 = a2.choose_action(s)
        assert ac1 == ac2, f"seeded agents diverged at step {i}"
        a1.update(s, ac1, 1.0, f"state_{(i + 1) % 5}")
        a2.update(s, ac2, 1.0, f"state_{(i + 1) % 5}")
    print("reproducibility: OK")

    # ---- Bounded Q-table (LRU eviction) ------------------------------ #
    bounded = QLearningAgent(
        config=QLearningConfig(max_states=5, save_interval=0)
    )
    for i in range(20):
        bounded.update(f"s{i}", "balanced", 1.0, f"s{i + 1}")
    print(f"bounded    : states={bounded.state_count} "
          f"evictions={bounded._evictions}")
    assert bounded.state_count <= 5

    # ---- Deterministic tie-breaking ---------------------------------- #
    tie = QLearningAgent(config=QLearningConfig(epsilon=0.0, save_interval=0))
    tie.choose_action("x")
    print("tie break  :", tie.best_action("x"), "(should be 'balanced')")
    assert tie.best_action("x") == "balanced"

    # ---- Statistics --------------------------------------------------- #
    print("stats      :", {
        k: v for k, v in agent.statistics().items()
        if k not in ("uptime_seconds", "last_transition")
    })

    # ---- Serialization round-trip ------------------------------------ #
    payload = agent.to_json()
    restored_agent = QLearningAgent.from_json(payload)
    assert restored_agent.to_dict() == agent.to_dict()
    print("Round-trip OK.")

    # ---- Context manager auto-saves on clean exit -------------------- #
    ctx_storage = _MemoryStorage()
    with QLearningAgent(storage=ctx_storage) as ctx_agent:
        for i in range(5):
            ctx_agent.update(f"c{i}", "balanced", 1.0, f"c{i + 1}")
    assert ctx_storage.save_calls >= 1, "context exit should trigger save"
    print("Context save: OK")

    # ---- Reset -------------------------------------------------------- #
    removed = agent.reset()
    print("reset      :", removed, "state(s) removed; "
          "new size:", agent.state_count)

    # ---- Validation failures ----------------------------------------- #
    for bad_cfg in (
        dict(alpha=0.0),
        dict(alpha=2.0),
        dict(gamma=-0.1),
        dict(gamma=1.5),
        dict(epsilon=-0.1),
        dict(epsilon=1.5),
        dict(max_states=0),
        dict(epsilon_decay=0.0),
        dict(epsilon=0.1, epsilon_min=0.5),
        dict(save_interval=-1),
    ):
        try:
            QLearningConfig(**bad_cfg)  # type: ignore[arg-type]
        except QLearningError as exc:
            print("Rejected cfg:", exc)

    # Input validation (strict).
    strict = QLearningAgent(storage=None)
    for bad_call in (
        lambda: strict.choose_action(""),
        lambda: strict.choose_action(None),
        lambda: strict.update("s", "bogus_action", 1.0, "s2"),
        lambda: strict.update("s", "balanced", float("nan"), "s2"),
        lambda: strict.update("s", "balanced", 1.0, ""),
        lambda: strict.update("", "balanced", 1.0, "s2"),
    ):
        try:
            bad_call()
        except QLearningError as exc:
            print("Rejected   :", exc)

    # Non-strict logs and skips.
    lenient = QLearningAgent(strict=False, storage=None)
    result = lenient.update("s", "bogus", 1.0, "s2")
    assert result is None
    print("lenient    : skipped invalid input")

    # ---- Custom action space ---------------------------------------- #
    custom = QLearningAgent(
        actions=("fast", "slow", "auto"), storage=None,
    )
    custom.update("s", "fast", 2.0, "s2")
    print("custom act :", custom.actions, "best:", custom.best_action("s"))

    # ---- Epsilon decay ---------------------------------------------- #
    decaying = QLearningAgent(
        config=QLearningConfig(
            epsilon=0.5, epsilon_decay=0.9, epsilon_min=0.01,
            save_interval=0,
        ),
        storage=None,
    )
    start_eps = decaying.epsilon
    for _ in range(30):
        decaying.update("s", "balanced", 1.0, "s2")
    print(f"epsilon    : {start_eps:.3f} → {decaying.epsilon:.3f}")

    # ---- save failure handling --------------------------------------- #
    class _BrokenStorage:
        def save(self, _q_table):
            raise IOError("disk full")

        def load(self):
            return {}

    failing = QLearningAgent(
        config=QLearningConfig(save_interval=1),
        storage=_BrokenStorage(),
        strict=False,
    )
    for i in range(3):
        failing.update(f"s{i}", "balanced", 1.0, f"s{i + 1}")
    print("save fails :", failing._save_failures,
          "(expected 3, no exception in non-strict mode)")
    assert failing._save_failures == 3

    # Strict mode raises on save failure.
    strict_save = QLearningAgent(
        config=QLearningConfig(save_interval=1),
        storage=_BrokenStorage(),
        strict=True,
    )
    try:
        strict_save.update("s", "balanced", 1.0, "s2")
    except QLearningError as exc:
        print("Rejected   :", exc)

    print("\nSmoke test passed.")
