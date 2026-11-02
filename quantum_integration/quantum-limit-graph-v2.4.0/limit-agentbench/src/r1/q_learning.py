# src/r1/q_learning.py

"""
Q-Learning Agent
================

Tabular Q-learning agent with optional JSON persistence via an
``RLStorage``-shaped object.

State  : any string hashable key (caller's choice)
Actions: policy weight adjustments (default: balanced / energy_saver /
         latency_optimized)

Design
------
- Frozen, validated :class:`QLearningConfig` — ``alpha ∈ (0,1]``,
  ``gamma ∈ [0,1]``, ``epsilon ∈ [0,1]``, ``save_interval ≥ 0``,
  ``max_state_length``, ``fallback_action``.
- Deterministic per-instance RNG (seeded); RNG state survives
  serialization.
- Bounded LRU Q-table.
- Thread-safe: every Q-table read/write happens under one ``RLock``
  scope — no TOCTOU.
- Batched persistence (``save_interval``). ``0`` disables auto-save.
- Persistence-safe under concurrent updates: ``save()`` snapshots
  ``_since_last_save`` before releasing the lock; concurrent updates
  that arrive during ``storage.save`` are preserved as "unsaved".
- ``load()`` **replaces** the Q-table (all-or-nothing), enforces
  ``max_states``, validates every Q value.
- Strict / non-strict input modes; strict-mode save failures raise
  ``QLearningError`` (the in-memory Q-table is already mutated at that
  point — see :meth:`update` docstring).
- Validated live hyperparameters (property setters).
- Full serialization round-trip: ``to_dict`` / ``from_dict`` /
  ``to_json`` / ``from_json``. ``from_dict`` does **not** auto-load from
  ``storage`` (avoids wasted I/O and strict-mode failures during
  construction).

Version: 2.0.0
"""

from __future__ import annotations

import base64
import json
import logging
import math
import numbers
import pickle
import random
import threading
import time
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
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
    "QLearningAgent",
    "QLearningConfig",
    "QLearningError",
    "QTransition",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Optional RLStorage
# --------------------------------------------------------------------------- #
try:  # Preferred: package-relative import.
    from .rl_storage import RLStorage  # type: ignore
except ImportError:  # pragma: no cover — script-style fallback.
    try:
        from rl_storage import RLStorage  # type: ignore
    except ImportError:
        RLStorage = None  # type: ignore[assignment]
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
    alpha: float = 0.1
    gamma: float = 0.9
    epsilon: float = 0.1

    # Bounded Q-table.
    max_states: Optional[int] = 10_000

    # Reproducibility.
    seed: Optional[int] = 42

    # Epsilon decay (geometric; 1.0 = no decay).
    epsilon_decay: float = 1.0
    epsilon_min: float = 0.0

    # Persistence cadence: save every N updates. ``0`` disables auto-save.
    save_interval: int = 100

    # Bounded transition history.
    max_history: int = 10_000

    # Maximum state-string length; prevents pinning arbitrarily large keys.
    max_state_length: int = 256

    # Fallback action when choose_action receives invalid state in non-strict
    # mode. ``None`` uses ``actions[0]``.
    fallback_action: Optional[str] = None

    def __post_init__(self) -> None:
        if not 0.0 < self.alpha <= 1.0:
            raise QLearningError(
                f"alpha must be in (0, 1], got {self.alpha}."
            )
        if not 0.0 <= self.gamma <= 1.0:
            raise QLearningError(
                f"gamma must be in [0, 1], got {self.gamma}."
            )
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
        if (
            isinstance(self.save_interval, bool)
            or not isinstance(self.save_interval, int)
            or self.save_interval < 0
        ):
            raise QLearningError(
                "save_interval must be a non-negative int."
            )
        if self.max_history <= 0:
            raise QLearningError("max_history must be > 0.")
        if self.max_state_length <= 0:
            raise QLearningError("max_state_length must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QLearningConfig":
        if not isinstance(data, Mapping):
            raise QLearningError(
                f"QLearningConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
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
                max_state_length=int(data.get("max_state_length", 256)),
                fallback_action=data.get("fallback_action"),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, QLearningError):
                raise
            raise QLearningError(
                f"invalid QLearningConfig payload: {exc}"
            ) from exc


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
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

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
            try:
                timestamp = datetime.fromisoformat(ts)
            except ValueError as exc:
                raise QLearningError(
                    f"invalid QTransition timestamp {ts!r}: {exc}"
                ) from exc
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)
        try:
            return cls(
                state=str(data["state"]),
                action=str(data["action"]),
                reward=float(data["reward"]),
                next_state=str(data["next_state"]),
                old_q=float(data["old_q"]),
                new_q=float(data["new_q"]),
                timestamp=timestamp,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise QLearningError(
                f"invalid QTransition payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Agent
# --------------------------------------------------------------------------- #
class QLearningAgent:
    """
    Tabular Q-learning agent with optional persistence.

    Parameters
    ----------
    alpha, gamma, epsilon : float
        Q-learning hyperparameters (validated).
    config : QLearningConfig, optional
        Typed configuration. Overrides the three positional hyperparameters.
    strict : bool, default True
        If True, invalid inputs raise :class:`QLearningError`. If False,
        they are logged and skipped (``choose_action`` falls back to
        ``fallback_action``).
    actions : sequence of str, optional
        Custom action space.
    storage : object, optional
        Persistence backend with ``save(mapping)`` / ``load()``.
    auto_load : bool, default True
        If True and ``storage`` is not None, the Q-table is loaded from
        storage at construction. ``from_dict`` passes False.
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
        auto_load: bool = True,
    ) -> None:
        self._config = config if config is not None else QLearningConfig(
            alpha=float(alpha), gamma=float(gamma), epsilon=float(epsilon)
        )
        self._strict = bool(strict)

        # Validated live hyperparameters (backed by _alpha/_gamma/_epsilon).
        self._alpha = float(self._config.alpha)
        self._gamma = float(self._config.gamma)
        self._epsilon = float(self._config.epsilon)

        # Action space.
        if actions is not None:
            if not isinstance(actions, (list, tuple)) or not actions:
                raise QLearningError(
                    "actions must be a non-empty sequence."
                )
            for a in actions:
                if not isinstance(a, str) or not a:
                    raise QLearningError(
                        "actions entries must be non-empty strings."
                    )
            self.actions: List[str] = list(actions)
        else:
            self.actions = list(self.DEFAULT_ACTIONS)
        self._action_set: frozenset = frozenset(self.actions)

        # Fallback action (used in non-strict mode on invalid state).
        fb = self._config.fallback_action
        if fb is None:
            self._fallback_action = self.actions[0]
        elif fb not in self._action_set:
            raise QLearningError(
                f"fallback_action {fb!r} not in actions {self.actions}."
            )
        else:
            self._fallback_action = fb

        # Internal state.
        self._lock = threading.RLock()
        self.q_table: "OrderedDict[str, Dict[str, float]]" = OrderedDict()
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
        self._history_dropped: int = 0
        self._started_at: float = time.time()
        self._loaded_from_storage: int = 0

        # ---- Storage backend ----------------------------------------
        if storage is not None:
            self.storage = storage
        elif RLStorage is not None:
            try:
                self.storage = RLStorage()  # type: ignore[misc]
            except Exception as exc:
                if self._strict:
                    raise QLearningError(
                        f"could not initialize RLStorage: {exc}"
                    ) from exc
                logger.warning(
                    "RLStorage init failed (%s); persistence disabled.", exc
                )
                self.storage = None
        else:
            self.storage = None

        # ---- Optional initial load ----------------------------------
        if self.storage is not None and auto_load:
            try:
                loaded = self.storage.load()
            except Exception as exc:
                if self._strict:
                    raise QLearningError(
                        f"could not load Q-table from storage: {exc}"
                    ) from exc
                logger.warning(
                    "storage load failed (%s); starting empty.", exc
                )
                loaded = {}

            if isinstance(loaded, Mapping):
                cap = self._config.max_states
                for state, row in loaded.items():
                    if cap is not None and len(self.q_table) >= cap:
                        logger.warning(
                            "auto-load: capping at max_states=%d.", cap
                        )
                        break
                    try:
                        validated = self._validate_restored_row(row, state)
                    except QLearningError:
                        if self._strict:
                            raise
                        logger.warning(
                            "auto-load: skipping invalid row for %r.", state
                        )
                        continue
                    if validated is None:
                        continue
                    self.q_table[str(state)] = validated
                self._loaded_from_storage = len(self.q_table)

        logger.debug(
            "QLearningAgent initialized "
            "(alpha=%.3f gamma=%.3f epsilon=%.3f seed=%s max_states=%s "
            "save_interval=%d loaded=%d strict=%s storage=%s)",
            self._alpha, self._gamma, self._epsilon,
            self._config.seed, self._config.max_states,
            self._config.save_interval, self._loaded_from_storage,
            self._strict,
            type(self.storage).__name__ if self.storage else None,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> QLearningConfig:
        return self._config

    @property
    def alpha(self) -> float:
        return self._alpha

    @alpha.setter
    def alpha(self, value: float) -> None:
        v = float(value)
        if not 0.0 < v <= 1.0:
            raise QLearningError(f"alpha must be in (0, 1], got {v}.")
        self._alpha = v

    @property
    def gamma(self) -> float:
        return self._gamma

    @gamma.setter
    def gamma(self, value: float) -> None:
        v = float(value)
        if not 0.0 <= v <= 1.0:
            raise QLearningError(f"gamma must be in [0, 1], got {v}.")
        self._gamma = v

    @property
    def epsilon(self) -> float:
        return self._epsilon

    @epsilon.setter
    def epsilon(self, value: float) -> None:
        v = float(value)
        if not 0.0 <= v <= 1.0:
            raise QLearningError(f"epsilon must be in [0, 1], got {v}.")
        self._epsilon = v

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

    # ----------------------------------------------------------- validation
    def _ensure_state_locked(self, state: str) -> None:
        """Caller must hold ``self._lock``."""
        if state in self.q_table:
            self.q_table.move_to_end(state)
            return
        self.q_table[state] = {a: 0.0 for a in self.actions}
        cap = self._config.max_states
        while cap is not None and len(self.q_table) > cap:
            self.q_table.popitem(last=False)
            self._evictions += 1

    def _ensure_state(self, state: str) -> None:
        """Backward-compatible wrapper (caller does not hold the lock)."""
        with self._lock:
            self._ensure_state_locked(state)

    def _validate_state(
        self, state: Any, *, name: str = "state"
    ) -> Optional[str]:
        if not isinstance(state, str) or not state:
            msg = f"{name} must be a non-empty string, got {state!r}."
            if self._strict:
                raise QLearningError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        if len(state) > self._config.max_state_length:
            msg = (
                f"{name} length {len(state)} exceeds "
                f"max_state_length={self._config.max_state_length}."
            )
            if self._strict:
                raise QLearningError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return state

    def _validate_action(self, action: Any) -> Optional[str]:
        if not isinstance(action, str) or action not in self._action_set:
            msg = f"action must be one of {self.actions}, got {action!r}."
            if self._strict:
                raise QLearningError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return action

    def _validate_reward(self, reward: Any) -> Optional[float]:
        if isinstance(reward, bool) or not isinstance(
            reward, (numbers.Real, Decimal)
        ):
            msg = f"reward must be real, got {type(reward).__name__}."
            if self._strict:
                raise QLearningError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        try:
            fv = float(reward)
        except (TypeError, ValueError) as exc:
            msg = f"reward not convertible to float: {exc}."
            if self._strict:
                raise QLearningError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        if not math.isfinite(fv):
            msg = f"reward must be finite, got {reward!r}."
            if self._strict:
                raise QLearningError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return fv

    def _validate_restored_row(
        self, row: Any, state_id: Any
    ) -> Optional[Dict[str, float]]:
        """
        Coerce one row from storage or a dict payload.

        Returns ``None`` when the row should be skipped entirely (invalid
        type). Raises :class:`QLearningError` on non-finite Q values.
        Unknown actions are dropped with a warning.
        """
        if not isinstance(row, Mapping):
            logger.warning(
                "restore: skipping non-Mapping row for %r.", state_id
            )
            return None
        out: Dict[str, float] = {a: 0.0 for a in self.actions}
        for act, q in row.items():
            if act not in self._action_set:
                logger.warning(
                    "restore: dropping unknown action %r for %r "
                    "(not in %s).",
                    act, state_id, self.actions,
                )
                continue
            try:
                fv = float(q)
            except (TypeError, ValueError) as exc:
                raise QLearningError(
                    f"invalid Q value for action {act!r}: {q!r}"
                ) from exc
            if not math.isfinite(fv):
                raise QLearningError(
                    f"non-finite Q value for action {act!r}: {q!r}"
                )
            out[act] = fv
        return out

    # ---------------------------------------------------------- public API
    def choose_action(self, state: str) -> str:
        """
        Select an action for ``state`` using ε-greedy exploration.

        Ties are broken deterministically by alphabetical action name.
        Invalid state in non-strict mode returns ``fallback_action``.
        """
        state = self._validate_state(state)
        if state is None:
            logger.warning(
                "choose_action got invalid state; returning fallback %r.",
                self._fallback_action,
            )
            return self._fallback_action

        with self._lock:
            self._ensure_state_locked(state)
            if self._rng.random() < self._epsilon:
                self._explorations += 1
                action = self._rng.choice(self.actions)
                logger.debug(
                    "Explore: state=%s action=%s (epsilon=%.3f)",
                    state, action, self._epsilon,
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

        Q(s, a) ← Q(s, a) + α · [ r + γ · maxₐ' Q(s', a') − Q(s, a) ]

        Returns the recorded :class:`QTransition`, or ``None`` in
        non-strict mode when any input was rejected.

        Notes
        -----
        In strict mode, if auto-save is due and ``storage.save`` raises,
        the in-memory Q-table is already mutated; the ``QLearningError``
        signals that persistence failed. The transition is still returned
        from ``history`` and the Q-table reflects the update.
        """
        state = self._validate_state(state)
        next_state = self._validate_state(next_state, name="next_state")
        action = self._validate_action(action)
        reward = self._validate_reward(reward)
        if (
            state is None
            or next_state is None
            or action is None
            or reward is None
        ):
            return None

        with self._lock:
            self._ensure_state_locked(state)
            self._ensure_state_locked(next_state)

            old_value = self.q_table[state][action]
            next_max = max(self.q_table[next_state].values())
            new_value = old_value + self._alpha * (
                reward + self._gamma * next_max - old_value
            )
            self.q_table[state][action] = new_value

            if (
                self._history.maxlen is not None
                and len(self._history) == self._history.maxlen
            ):
                self._history_dropped += 1

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

            if self._config.epsilon_decay < 1.0:
                self._epsilon = max(
                    self._config.epsilon_min,
                    self._epsilon * self._config.epsilon_decay,
                )

            should_save = (
                self._config.save_interval > 0
                and self._since_last_save >= self._config.save_interval
            )

        if should_save:
            self.save()

        logger.debug(
            "Update: s=%s a=%s r=%.4f s'=%s Q:%.4f→%.4f (ε=%.3f)",
            state, action, reward, next_state,
            old_value, new_value, self._epsilon,
        )
        return transition

    def save(self) -> bool:
        """
        Persist the Q-table via ``self.storage``.

        Safe under concurrent updates: the counter is captured before
        releasing the lock, so updates that arrive during the storage
        call are preserved as unsaved.

        Returns True on success, False on failure (non-strict). In strict
        mode, raises ``QLearningError`` on failure.
        """
        with self._lock:
            if self.storage is None:
                # Nothing to save to — treat as success, no counter change.
                return True
            snapshot = {s: dict(row) for s, row in self.q_table.items()}
            snapshot_counter = self._since_last_save

        try:
            self.storage.save(snapshot)
        except Exception as exc:
            logger.error("Q-table save failed: %s", exc)
            with self._lock:
                self._save_failures += 1
            if self._strict:
                raise QLearningError(
                    f"Q-table save failed: {exc}"
                ) from exc
            return False

        with self._lock:
            # Subtract only what we actually persisted; preserve concurrent
            # increments that happened during storage.save().
            self._since_last_save = max(
                0, self._since_last_save - snapshot_counter
            )
        logger.debug(
            "Q-table saved (%d state(s); %d pending resets).",
            len(snapshot), snapshot_counter,
        )
        return True

    def load(self) -> int:
        """
        Replace the Q-table from ``self.storage``.

        All-or-nothing: validates every row before mutating state. Enforces
        ``max_states``. Resets the "unsaved" counter on success.

        Returns the number of states restored.
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
                msg = "storage.load() must return a Mapping."
                if self._strict:
                    raise QLearningError(msg)
                logger.warning("%s Ignoring.", msg)
                return 0

            cap = self._config.max_states
            if cap is not None and len(loaded) > cap:
                msg = (
                    f"storage has {len(loaded)} states, "
                    f"exceeds max_states={cap}."
                )
                if self._strict:
                    raise QLearningError(msg)
                logger.warning("%s Truncating.", msg)

            # --- Validate into a fresh table, then swap atomically -----
            new_table: "OrderedDict[str, Dict[str, float]]" = OrderedDict()
            for state, row in loaded.items():
                if cap is not None and len(new_table) >= cap:
                    break
                validated = self._validate_restored_row(row, state)
                if validated is None:
                    continue
                new_table[str(state)] = validated

            self.q_table = new_table
            self._loaded_from_storage = len(new_table)
            self._since_last_save = 0
        logger.debug(
            "Q-table loaded (%d state(s)).", len(new_table)
        )
        return len(new_table)

    def best_action(self, state: str) -> Optional[str]:
        """Return the highest-Q action for ``state`` (no exploration)."""
        state = self._validate_state(state)
        if state is None:
            return None
        with self._lock:
            self._ensure_state_locked(state)
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
        with self._lock:
            self._ensure_state_locked(state)
            return self.q_table[state][action]

    def reset(
        self,
        *,
        clear_history: bool = True,
        reset_rng: bool = False,
        restore_epsilon: bool = True,
    ) -> int:
        """
        Clear the Q-table and counters.

        Parameters
        ----------
        clear_history:
            Drop the transition history (and the drop counter).
        reset_rng:
            Re-seed the RNG from ``config.seed`` for exact replay.
        restore_epsilon:
            Restore ``epsilon`` to ``config.epsilon`` (undo decay).

        Returns the number of states removed.
        """
        with self._lock:
            removed = len(self.q_table)
            self.q_table.clear()
            if clear_history:
                self._history.clear()
                self._history_dropped = 0
            self._updates = 0
            self._since_last_save = 0
            self._explorations = 0
            self._exploitations = 0
            self._evictions = 0
            if restore_epsilon:
                self._epsilon = self._config.epsilon
            if reset_rng:
                self._rng = random.Random(self._config.seed)
            self._started_at = time.time()
        logger.debug("QLearningAgent reset (removed %d state(s)).", removed)
        return removed

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Aggregate statistics over the Q-table and history."""
        with self._lock:
            states = len(self.q_table)
            history = list(self._history)
            updates = self._updates
            explorations = self._explorations
            exploitations = self._exploitations
            evictions = self._evictions
            save_failures = self._save_failures
            since_save = self._since_last_save
            history_dropped = self._history_dropped
            eps = self._epsilon
            started = self._started_at
            persistence = self.storage is not None
            loaded = self._loaded_from_storage

            all_q: List[float] = [
                q for row in self.q_table.values() for q in row.values()
            ]

        payload: Dict[str, Any] = {
            "states": states,
            "updates": updates,
            "explorations": explorations,
            "exploitations": exploitations,
            "evictions": evictions,
            "save_failures": save_failures,
            "has_unsaved_changes": since_save > 0,
            "history_size": len(history),
            "history_dropped": history_dropped,
            "epsilon": eps,
            "uptime_seconds": time.time() - started,
            "persistence_enabled": persistence,
            "loaded_from_storage": loaded,
        }

        if updates > 0:
            payload["explore_ratio"] = explorations / updates
            payload["exploit_ratio"] = exploitations / updates

        if all_q:
            payload["mean_q"] = sum(all_q) / len(all_q)
            payload["min_q"] = min(all_q)
            payload["max_q"] = max(all_q)

        if history:
            payload["windowed_mean_reward"] = (
                sum(t.reward for t in history) / len(history)
            )
            payload["windowed_size"] = len(history)
            payload["last_transition"] = history[-1].to_dict()

        return payload

    # ---------------------------------------------------------- serialization
    def _rng_state_b64(self) -> str:
        return base64.b64encode(
            pickle.dumps(self._rng.getstate())
        ).decode("ascii")

    def _restore_rng_state(self, payload: str) -> None:
        try:
            state = pickle.loads(base64.b64decode(payload.encode("ascii")))
        except Exception as exc:
            raise QLearningError(
                f"invalid rng_state payload: {exc}"
            ) from exc
        self._rng.setstate(state)

    def to_dict(self, *, include_history: bool = True) -> Dict[str, Any]:
        with self._lock:
            out: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "actions": list(self.actions),
                "fallback_action": self._fallback_action,
                "alpha": self._alpha,
                "gamma": self._gamma,
                "epsilon": self._epsilon,
                "counters": {
                    "updates": self._updates,
                    "explorations": self._explorations,
                    "exploitations": self._exploitations,
                    "evictions": self._evictions,
                    "save_failures": self._save_failures,
                    "history_dropped": self._history_dropped,
                },
                "since_last_save": self._since_last_save,
                "started_at": self._started_at,
                "q_table": {
                    s: dict(row) for s, row in self.q_table.items()
                },
                "rng_state": self._rng_state_b64(),
            }
            if include_history:
                out["history"] = [t.to_dict() for t in self._history]
        return out

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        storage: Optional[Any] = None,
        strict: Optional[bool] = None,
        restore_rng: bool = True,
    ) -> "QLearningAgent":
        """
        Rebuild an agent from :meth:`to_dict` output.

        By default ``storage`` is ``None`` so no I/O happens during
        construction (``auto_load=False`` is forced).
        """
        if not isinstance(data, Mapping):
            raise QLearningError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = QLearningConfig.from_dict(dict(data.get("config", {}) or {}))
        strict_flag = (
            bool(data.get("strict", True)) if strict is None else strict
        )

        agent = cls(
            config=cfg,
            strict=strict_flag,
            actions=data.get("actions") or None,
            storage=storage,
            auto_load=False,  # never auto-load in from_dict
        )

        # ---- Validate live hyperparameters -------------------------
        try:
            restored_alpha = float(data.get("alpha", cfg.alpha))
            restored_gamma = float(data.get("gamma", cfg.gamma))
            restored_epsilon = float(data.get("epsilon", cfg.epsilon))
        except (TypeError, ValueError) as exc:
            raise QLearningError(
                f"invalid hyperparameter: {exc}"
            ) from exc
        if not 0.0 < restored_alpha <= 1.0:
            raise QLearningError(
                f"restored alpha out of range: {restored_alpha}."
            )
        if not 0.0 <= restored_gamma <= 1.0:
            raise QLearningError(
                f"restored gamma out of range: {restored_gamma}."
            )
        if not 0.0 <= restored_epsilon <= 1.0:
            raise QLearningError(
                f"restored epsilon out of range: {restored_epsilon}."
            )

        # ---- Q-table: build fresh, all-or-nothing ------------------
        raw_table = data.get("q_table") or {}
        if not isinstance(raw_table, Mapping):
            raise QLearningError("q_table must be a Mapping.")
        cap = cfg.max_states
        if cap is not None and len(raw_table) > cap:
            raise QLearningError(
                f"restored q_table has {len(raw_table)} states, "
                f"exceeds max_states={cap}."
            )

        with agent._lock:
            agent.q_table = OrderedDict()
            for s, row in raw_table.items():
                validated = agent._validate_restored_row(row, s)
                if validated is not None:
                    agent.q_table[str(s)] = validated
            agent._loaded_from_storage = len(agent.q_table)

            agent._alpha = restored_alpha
            agent._gamma = restored_gamma
            agent._epsilon = restored_epsilon

            # ---- Counters ------------------------------------------
            counters = dict(data.get("counters", {}) or {})
            for key, attr in (
                ("updates", "_updates"),
                ("explorations", "_explorations"),
                ("exploitations", "_exploitations"),
                ("evictions", "_evictions"),
                ("save_failures", "_save_failures"),
                ("history_dropped", "_history_dropped"),
            ):
                try:
                    setattr(agent, attr, int(counters.get(key, 0)))
                except (TypeError, ValueError) as exc:
                    raise QLearningError(
                        f"invalid counter {key!r}: {exc}"
                    ) from exc
            try:
                agent._since_last_save = int(
                    data.get("since_last_save", 0)
                )
            except (TypeError, ValueError) as exc:
                raise QLearningError(
                    f"invalid since_last_save: {exc}"
                ) from exc
            try:
                agent._started_at = float(
                    data.get("started_at", time.time())
                )
            except (TypeError, ValueError) as exc:
                raise QLearningError(
                    f"invalid started_at: {exc}"
                ) from exc

            # ---- History -------------------------------------------
            for entry in data.get("history", []) or []:
                agent._history.append(QTransition.from_dict(entry))

            # ---- RNG -----------------------------------------------
            if restore_rng and "rng_state" in data:
                agent._restore_rng_state(str(data["rng_state"]))

        return agent

    def to_json(
        self, *, include_history: bool = True, **kwargs: Any
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        return json.dumps(
            self.to_dict(include_history=include_history), **kwargs
        )

    @classmethod
    def from_json(
        cls, payload: str, *, storage: Optional[Any] = None
    ) -> "QLearningAgent":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise QLearningError(
                f"invalid JSON payload: {exc}"
            ) from exc
        try:
            return cls.from_dict(raw, storage=storage)
        except QLearningError:
            raise
        except (TypeError, ValueError) as exc:
            raise QLearningError(
                f"invalid agent payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "QLearningAgent":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if (
            exc_type is None
            and self.has_unsaved_changes
            and self.storage is not None
        ):
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
                f"alpha={self._alpha:.3f}, "
                f"gamma={self._gamma:.3f}, "
                f"epsilon={self._epsilon:.3f}, "
                f"states={len(self.q_table)}, "
                f"actions={len(self.actions)}, "
                f"updates={self._updates}, "
                f"storage={'yes' if self.storage else 'no'}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1.q_learning
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

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

    # ---- Happy path ---------------------------------------------- #
    storage = _MemoryStorage()
    agent = QLearningAgent(storage=storage)
    print("repr       :", agent)
    print("VERSION    :", VERSION)

    for _ in range(500):
        s = "low_carbon"
        a = agent.choose_action(s)
        agent.update(s, a, reward=1.0, next_state="high_carbon")
        a = agent.choose_action("high_carbon")
        agent.update("high_carbon", a, reward=-1.0, next_state="low_carbon")

    print("best low   :", agent.best_action("low_carbon"))
    print("Q(low)     :", {
        a: round(agent.get_q_value("low_carbon", a), 4)
        for a in agent.actions
    })
    print(f"saves      : {storage.save_calls} for {agent.updates} updates")
    assert (
        storage.save_calls
        <= agent.updates // agent.config.save_interval + 1
    )

    # ---- save_interval=0 ----------------------------------------- #
    no_auto = QLearningAgent(
        config=QLearningConfig(save_interval=0),
        storage=_MemoryStorage(),
    )
    for i in range(50):
        no_auto.update(f"s{i}", "balanced", 1.0, f"s{i + 1}")
    assert no_auto.storage.save_calls == 0
    print("no-auto    : saves=0 (interval=0)")

    # ---- load replaces ------------------------------------------- #
    fresh = QLearningAgent(storage=_MemoryStorage())
    fresh.update("A", "balanced", 1.0, "B")
    fresh.storage.save({"C": {"balanced": 5.0}})
    n = fresh.load()
    assert n == 1 and fresh.state_count == 1
    assert fresh.best_action("C") == "balanced"
    print("load       : replace (1 state, not 3)")

    # ---- Reproducibility ----------------------------------------- #
    a1 = QLearningAgent(
        config=QLearningConfig(seed=7, epsilon=0.5), storage=None,
    )
    a2 = QLearningAgent(
        config=QLearningConfig(seed=7, epsilon=0.5), storage=None,
    )
    for i in range(20):
        s = f"state_{i % 5}"
        ac1 = a1.choose_action(s)
        ac2 = a2.choose_action(s)
        assert ac1 == ac2
        a1.update(s, ac1, 1.0, f"state_{(i + 1) % 5}")
        a2.update(s, ac2, 1.0, f"state_{(i + 1) % 5}")
    print("reproducibility: OK")

    # ---- RNG round-trip ----------------------------------------- #
    restored = QLearningAgent.from_json(a1.to_json())
    for _ in range(10):
        assert a1.choose_action("probe") == restored.choose_action("probe")
    print("rng round-trip: OK")

    # ---- save race: concurrent updates preserved ----------------- #
    import threading as _t
    slow_store = _MemoryStorage()

    class _SlowStore:
        def __init__(self, inner):
            self.inner = inner
            self._barrier = None
        def save(self, q):
            # Delay so concurrent updates can land during save.
            time.sleep(0.01)
            self.inner.save(q)
        def load(self):
            return self.inner.load()

    racer = QLearningAgent(
        config=QLearningConfig(save_interval=1), storage=_SlowStore(
            slow_store
        ),
    )
    stop = _t.Event()
    def spam():
        i = 0
        while not stop.is_set():
            racer.update(f"k{i % 3}", "balanced", 1.0, f"k{(i + 1) % 3}")
            i += 1
    thread = _t.Thread(target=spam)
    thread.start()
    time.sleep(0.1)
    stop.set()
    thread.join()
    # After all saves settle, unsaved count should reflect in-memory state
    # not yet persisted (may be zero or more; the invariant is: no
    # mutation of _since_last_save below the true unsaved count).
    print(f"race       : updates={racer.updates}, "
          f"since_save={racer._since_last_save}")

    # ---- Bounded Q-table ---------------------------------------- #
    bounded = QLearningAgent(
        config=QLearningConfig(max_states=5, save_interval=0)
    )
    for i in range(20):
        bounded.update(f"s{i}", "balanced", 1.0, f"s{i + 1}")
    assert bounded.state_count <= 5
    print(f"bounded    : states={bounded.state_count}")

    # ---- Config drift preserved ---------------------------------- #
    agent.alpha = 0.5
    assert QLearningAgent.from_json(agent.to_json()).alpha == 0.5
    print("config drift: alpha preserved across round-trip")

    # ---- Property validation ------------------------------------- #
    for attr, bad in (("alpha", 0.0), ("alpha", 2.0),
                      ("gamma", -0.1), ("epsilon", 1.5)):
        try:
            setattr(agent, attr, bad)
        except QLearningError as exc:
            print(f"Rejected {attr}={bad}: {exc}")

    # ---- Restore validation -------------------------------------- #
    for bad in (
        {"config": {}, "alpha": 5.0},
        {"config": QLearningConfig(max_states=2).to_dict(),
         "q_table": {f"s{i}": {"balanced": 0.0} for i in range(5)}},
        {"config": {}, "q_table": {"s": {"balanced": float("nan")}}},
        {"config": {}, "q_table": {"s": {"bogus": 1.0}}},
    ):
        try:
            QLearningAgent.from_dict(bad)
        except QLearningError as exc:
            print("Rejected payload:", exc)
        else:
            print("Payload ok (warn-only):", bad.get("q_table"))

    # ---- Non-finite reward rejected ------------------------------ #
    strict = QLearningAgent()
    try:
        strict.update("s", "balanced", float("nan"), "s2")
    except QLearningError as exc:
        print("Rejected reward:", exc)

    # ---- Numeric types (Decimal, Fraction, numpy) --------------- #
    from fractions import Fraction as F
    num = QLearningAgent()
    num.update("s", "balanced", Decimal("1.5"), "s2")
    num.update("s", "balanced", F(1, 2), "s2")
    print("Decimal/Fraction rewards: OK")

    # ---- Context manager ---------------------------------------- #
    ctx_store = _MemoryStorage()
    with QLearningAgent(storage=ctx_store) as ctx_agent:
        for i in range(5):
            ctx_agent.update(f"c{i}", "balanced", 1.0, f"c{i + 1}")
    assert ctx_store.save_calls >= 1
    print("Context save: OK")

    # ---- Reset options ------------------------------------------ #
    before = agent.epsilon
    agent.reset(reset_rng=True, restore_epsilon=True)
    assert agent.epsilon == agent.config.epsilon
    print("reset      : rng re-seeded, epsilon restored")

    # ---- Serialization / deserialization ------------------------ #
    payload = agent.to_json()
    restored_agent = QLearningAgent.from_json(payload)
    assert restored_agent.to_dict() == agent.to_dict()
    print("round-trip : OK")

    # ---- to_json rejects NaN ------------------------------------ #
    bad = QLearningAgent()
    bad.update("s", "balanced", 1.0, "s2")
    bad.q_table["s"]["balanced"] = float("nan")
    try:
        bad.to_json()
    except ValueError as exc:
        print("NaN rejected:", exc)

    # ---- save failure handling ---------------------------------- #
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
    assert failing._save_failures == 3
    print(f"save fails : {failing._save_failures} (no exception)")

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
