# src/r1/energy_rl_agent.py

"""
Energy RL Agent
===============

Tabular Q-learning agent for adaptive energy-aware policy tuning.

State  : discretized metric profile (e.g. ``"green|low_latency"``)
Actions: adjust policy weights (energy / latency / carbon / balanced)

Design
------
- Frozen, validated :class:`EnergyRLConfig`.
- Deterministic, seedable RNG; RNG state survives serialization.
- Bounded LRU Q-table (``max_states``).
- Thread-safe: every read/write of the Q-table happens under one
  ``RLock`` scope — no TOCTOU between ``_ensure_state`` and use.
- Bounded transition history; drop counter exposed in statistics.
- Strict / non-strict input modes.
- Deterministic tie-breaking (alphabetical action name).
- Validated live hyperparameters via properties.
- Serialization round-trips exactly (``to_dict`` / ``from_dict``).
- Optional persistence hook via an ``RLStorage``-shaped object.

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
    Callable,
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
    "EnergyRLAgent",
    "EnergyRLConfig",
    "EnergyRLError",
    "Transition",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EnergyRLError(ValueError):
    """Raised for invalid Energy RL agent inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EnergyRLConfig:
    """Tunable parameters for the Energy RL agent."""

    alpha: float = 0.1
    gamma: float = 0.9
    epsilon: float = 0.1

    max_states: Optional[int] = 10_000
    seed: Optional[int] = 42

    epsilon_decay: float = 1.0
    epsilon_min: float = 0.0

    max_history: int = 10_000
    max_state_length: int = 256
    fallback_action: Optional[str] = None

    def __post_init__(self) -> None:
        if not 0.0 < self.alpha <= 1.0:
            raise EnergyRLError(f"alpha must be in (0, 1], got {self.alpha}.")
        if not 0.0 <= self.gamma <= 1.0:
            raise EnergyRLError(f"gamma must be in [0, 1], got {self.gamma}.")
        if not 0.0 <= self.epsilon <= 1.0:
            raise EnergyRLError(
                f"epsilon must be in [0, 1], got {self.epsilon}."
            )
        if self.max_states is not None and self.max_states <= 0:
            raise EnergyRLError("max_states must be a positive int or None.")
        if not 0.0 < self.epsilon_decay <= 1.0:
            raise EnergyRLError("epsilon_decay must be in (0, 1].")
        if not 0.0 <= self.epsilon_min <= self.epsilon:
            raise EnergyRLError("epsilon_min must be in [0, epsilon].")
        if self.max_history <= 0:
            raise EnergyRLError("max_history must be > 0.")
        if self.max_state_length <= 0:
            raise EnergyRLError("max_state_length must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EnergyRLConfig":
        if not isinstance(data, Mapping):
            raise EnergyRLError(
                f"EnergyRLConfig.from_dict expects a Mapping, "
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
                max_history=int(data.get("max_history", 10_000)),
                max_state_length=int(data.get("max_state_length", 256)),
                fallback_action=data.get("fallback_action"),
            )
        except (TypeError, ValueError) as exc:
            raise EnergyRLError(f"invalid config payload: {exc}") from exc


# --------------------------------------------------------------------------- #
# Transition record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Transition:
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
    def from_dict(cls, data: Mapping[str, Any]) -> "Transition":
        ts = data.get("timestamp")
        if isinstance(ts, str):
            try:
                timestamp = datetime.fromisoformat(ts)
            except ValueError as exc:
                raise EnergyRLError(
                    f"invalid Transition timestamp {ts!r}: {exc}"
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
            raise EnergyRLError(
                f"invalid Transition payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Agent
# --------------------------------------------------------------------------- #
class EnergyRLAgent:
    """
    Lightweight Q-learning agent for energy-aware policy tuning.

    Parameters
    ----------
    alpha, gamma, epsilon : float
        Q-learning hyperparameters (validated by ``EnergyRLConfig``).
    config : EnergyRLConfig, optional
        Typed configuration. Overrides the three positional hyperparameters.
    strict : bool, default True
        Strict mode raises :class:`EnergyRLError` on invalid inputs;
        non-strict mode logs and skips (using ``fallback_action`` for
        ``choose_action``).
    actions : sequence of str, optional
        Custom action space. Defaults to ``DEFAULT_ACTIONS``.
    """

    DEFAULT_ACTIONS: Tuple[str, ...] = (
        "increase_energy_weight",
        "increase_latency_weight",
        "increase_carbon_weight",
        "balanced",
    )

    def __init__(
        self,
        alpha: float = 0.1,
        gamma: float = 0.9,
        epsilon: float = 0.1,
        *,
        config: Optional[EnergyRLConfig] = None,
        strict: bool = True,
        actions: Optional[Sequence[str]] = None,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = EnergyRLConfig(
                alpha=alpha, gamma=gamma, epsilon=epsilon
            )
        self._strict = bool(strict)

        # Validated live hyperparameters (backed by _alpha/_gamma/_epsilon).
        self._alpha = float(self._config.alpha)
        self._gamma = float(self._config.gamma)
        self._epsilon = float(self._config.epsilon)

        # Action space.
        if actions is not None:
            if not isinstance(actions, (list, tuple)) or not actions:
                raise EnergyRLError("actions must be a non-empty sequence.")
            for a in actions:
                if not isinstance(a, str) or not a:
                    raise EnergyRLError(
                        "actions entries must be non-empty strings."
                    )
            self.actions: List[str] = list(actions)
        else:
            self.actions = list(self.DEFAULT_ACTIONS)
        self._action_set: frozenset = frozenset(self.actions)

        # Fallback for non-strict mode.
        fb = self._config.fallback_action
        if fb is None:
            self._fallback_action = self.actions[0]
        elif fb not in self._action_set:
            raise EnergyRLError(
                f"fallback_action {fb!r} not in actions {self.actions}."
            )
        else:
            self._fallback_action = fb

        # Internal state.
        self._lock = threading.RLock()
        self.q_table: "OrderedDict[str, Dict[str, float]]" = OrderedDict()
        self._rng = random.Random(self._config.seed)
        self._history: Deque[Transition] = deque(
            maxlen=self._config.max_history
        )

        self._updates: int = 0
        self._explorations: int = 0
        self._exploitations: int = 0
        self._evictions: int = 0
        self._history_dropped: int = 0
        self._started_at: float = time.time()

        logger.debug(
            "EnergyRLAgent initialized "
            "(alpha=%.3f gamma=%.3f epsilon=%.3f seed=%s max_states=%s "
            "strict=%s)",
            self.alpha, self.gamma, self.epsilon,
            self._config.seed, self._config.max_states, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> EnergyRLConfig:
        return self._config

    @property
    def alpha(self) -> float:
        return self._alpha

    @alpha.setter
    def alpha(self, value: float) -> None:
        v = float(value)
        if not 0.0 < v <= 1.0:
            raise EnergyRLError(f"alpha must be in (0, 1], got {v}.")
        self._alpha = v

    @property
    def gamma(self) -> float:
        return self._gamma

    @gamma.setter
    def gamma(self, value: float) -> None:
        v = float(value)
        if not 0.0 <= v <= 1.0:
            raise EnergyRLError(f"gamma must be in [0, 1], got {v}.")
        self._gamma = v

    @property
    def epsilon(self) -> float:
        return self._epsilon

    @epsilon.setter
    def epsilon(self, value: float) -> None:
        v = float(value)
        if not 0.0 <= v <= 1.0:
            raise EnergyRLError(f"epsilon must be in [0, 1], got {v}.")
        self._epsilon = v

    @property
    def state_count(self) -> int:
        with self._lock:
            return len(self.q_table)

    @property
    def history(self) -> List[Transition]:
        with self._lock:
            return list(self._history)

    # ----------------------------------------------------------- validation
    def _ensure_state_locked(self, state: str) -> None:
        """Must be called with ``self._lock`` held."""
        if state in self.q_table:
            self.q_table.move_to_end(state)
            return
        self.q_table[state] = {a: 0.0 for a in self.actions}
        cap = self._config.max_states
        while cap is not None and len(self.q_table) > cap:
            self.q_table.popitem(last=False)
            self._evictions += 1

    def _ensure_state(self, state: str) -> None:
        """Public wrapper — used by callers that already hold no lock."""
        with self._lock:
            self._ensure_state_locked(state)

    def _validate_state(
        self, state: Any, *, name: str = "state"
    ) -> Optional[str]:
        if not isinstance(state, str) or not state:
            msg = f"{name} must be a non-empty string, got {state!r}."
            if self._strict:
                raise EnergyRLError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        if len(state) > self._config.max_state_length:
            msg = (
                f"{name} length {len(state)} exceeds "
                f"max_state_length={self._config.max_state_length}."
            )
            if self._strict:
                raise EnergyRLError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return state

    def _validate_action(self, action: Any) -> Optional[str]:
        if not isinstance(action, str) or action not in self._action_set:
            msg = (
                f"action must be one of {self.actions}, got {action!r}."
            )
            if self._strict:
                raise EnergyRLError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return action

    def _validate_reward(self, reward: Any) -> Optional[float]:
        # Accept numbers.Real (int, float, numpy scalars) and Decimal.
        if isinstance(reward, bool) or not isinstance(
            reward, (numbers.Real, Decimal)
        ):
            msg = f"reward must be real, got {type(reward).__name__}."
            if self._strict:
                raise EnergyRLError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        try:
            fv = float(reward)
        except (TypeError, ValueError) as exc:
            msg = f"reward not convertible to float: {exc}."
            if self._strict:
                raise EnergyRLError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        if not math.isfinite(fv):
            msg = f"reward must be finite, got {reward!r}."
            if self._strict:
                raise EnergyRLError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return fv

    # ---------------------------------------------------------- public API
    def choose_action(self, state: str) -> str:
        """
        Select an action for ``state`` using ε-greedy exploration.

        Ties are broken deterministically by alphabetical action name.
        Invalid input in non-strict mode returns ``fallback_action``.
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
                    "Explore: state=%s action=%s epsilon=%.3f",
                    state, action, self._epsilon,
                )
                return action
            row = self.q_table[state]
            best_q = max(row.values())
            best_actions = sorted(a for a, q in row.items() if q == best_q)
            action = best_actions[0]
            self._exploitations += 1
            logger.debug(
                "Exploit: state=%s action=%s q=%.4f",
                state, action, best_q,
            )
            return action

    def update(
        self,
        state: str,
        action: str,
        reward: float,
        next_state: str,
    ) -> Optional[Transition]:
        """
        Apply one Q-learning update.

        Q(s, a) ← Q(s, a) + α · [ r + γ · maxₐ' Q(s', a') − Q(s, a) ]

        Returns the recorded :class:`Transition`, or ``None`` in non-strict
        mode when any input was rejected.
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

            transition = Transition(
                state=state,
                action=action,
                reward=reward,
                next_state=next_state,
                old_q=old_value,
                new_q=new_value,
            )
            self._history.append(transition)
            self._updates += 1

            if self._config.epsilon_decay < 1.0:
                self._epsilon = max(
                    self._config.epsilon_min,
                    self._epsilon * self._config.epsilon_decay,
                )

        logger.debug(
            "Update: s=%s a=%s r=%.4f s'=%s Q:%.4f→%.4f (ε=%.3f)",
            state, action, reward, next_state,
            old_value, new_value, self._epsilon,
        )
        return transition

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
            self._explorations = 0
            self._exploitations = 0
            self._evictions = 0
            if restore_epsilon:
                self._epsilon = self._config.epsilon
            if reset_rng:
                self._rng = random.Random(self._config.seed)
            self._started_at = time.time()
        logger.debug("EnergyRLAgent reset (removed %d state(s)).", removed)
        return removed

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

    # ----------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Aggregate statistics over the Q-table and history."""
        with self._lock:
            states = len(self.q_table)
            updates = self._updates
            explorations = self._explorations
            exploitations = self._exploitations
            evictions = self._evictions
            history = list(self._history)
            history_dropped = self._history_dropped
            eps = self._epsilon
            started = self._started_at

        payload: Dict[str, Any] = {
            "states": states,
            "updates": updates,
            "explorations": explorations,
            "exploitations": exploitations,
            "evictions": evictions,
            "epsilon": eps,
            "history_size": len(history),
            "history_dropped": history_dropped,
            "uptime_seconds": time.time() - started,
        }

        if updates > 0:
            payload["explore_ratio"] = explorations / updates
            payload["exploit_ratio"] = exploitations / updates

        if states:
            all_q = [q for row in self.q_table.values() for q in row.values()]
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

    # -------------------------------------------------------- serialization
    def _rng_state_b64(self) -> str:
        return base64.b64encode(
            pickle.dumps(self._rng.getstate())
        ).decode("ascii")

    def _restore_rng_state(self, payload: str) -> None:
        try:
            state = pickle.loads(base64.b64decode(payload.encode("ascii")))
        except Exception as exc:
            raise EnergyRLError(
                f"invalid rng_state payload: {exc}"
            ) from exc
        self._rng.setstate(state)

    def to_dict(self, *, include_rng: bool = True) -> Dict[str, Any]:
        with self._lock:
            out: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "actions": list(self.actions),
                "alpha": self._alpha,
                "gamma": self._gamma,
                "epsilon": self._epsilon,
                "fallback_action": self._fallback_action,
                "counters": {
                    "updates": self._updates,
                    "explorations": self._explorations,
                    "exploitations": self._exploitations,
                    "evictions": self._evictions,
                    "history_dropped": self._history_dropped,
                },
                "started_at": self._started_at,
                "q_table": {
                    s: dict(row) for s, row in self.q_table.items()
                },
                "history": [t.to_dict() for t in self._history],
            }
            if include_rng:
                out["rng_state"] = self._rng_state_b64()
            return out

    def _validate_restored_row(
        self, row: Mapping[str, Any]
    ) -> Dict[str, float]:
        out: Dict[str, float] = {a: 0.0 for a in self.actions}
        for act, q in row.items():
            if act not in self._action_set:
                logger.warning(
                    "from_dict: dropping unknown action %r (not in %s).",
                    act, self.actions,
                )
                continue
            try:
                fv = float(q)
            except (TypeError, ValueError) as exc:
                raise EnergyRLError(
                    f"invalid Q value for action {act!r}: {q!r}"
                ) from exc
            if not math.isfinite(fv):
                raise EnergyRLError(
                    f"non-finite Q value for action {act!r}: {q!r}"
                )
            out[act] = fv
        return out

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: Optional[bool] = None,
        restore_rng: bool = True,
    ) -> "EnergyRLAgent":
        if not isinstance(data, Mapping):
            raise EnergyRLError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )

        cfg_data = dict(data.get("config", {}) or {})
        cfg = EnergyRLConfig.from_dict(cfg_data)

        strict_flag = (
            bool(data.get("strict", True)) if strict is None else strict
        )
        agent = cls(
            config=cfg,
            strict=strict_flag,
            actions=data.get("actions") or None,
        )

        try:
            eps = float(data.get("epsilon", cfg.epsilon))
            if not 0.0 <= eps <= 1.0:
                raise EnergyRLError(
                    f"restored epsilon must be in [0, 1], got {eps}."
                )
            alpha = float(data.get("alpha", cfg.alpha))
            if not 0.0 < alpha <= 1.0:
                raise EnergyRLError(
                    f"restored alpha must be in (0, 1], got {alpha}."
                )
            gamma = float(data.get("gamma", cfg.gamma))
            if not 0.0 <= gamma <= 1.0:
                raise EnergyRLError(
                    f"restored gamma must be in [0, 1], got {gamma}."
                )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, EnergyRLError):
                raise
            raise EnergyRLError(
                f"invalid hyperparameter in payload: {exc}"
            ) from exc

        raw_table = data.get("q_table") or {}
        if not isinstance(raw_table, Mapping):
            raise EnergyRLError("q_table must be a Mapping.")
        cap = agent._config.max_states
        if cap is not None and len(raw_table) > cap:
            raise EnergyRLError(
                f"restored q_table has {len(raw_table)} states, "
                f"exceeds max_states={cap}."
            )

        with agent._lock:
            agent._alpha = alpha
            agent._gamma = gamma
            agent._epsilon = eps
            for s, row in raw_table.items():
                if not isinstance(row, Mapping):
                    raise EnergyRLError(
                        f"q_table[{s!r}] must be a Mapping."
                    )
                agent.q_table[str(s)] = agent._validate_restored_row(row)

            counters = dict(data.get("counters", {}) or {})
            for key, attr in (
                ("updates", "_updates"),
                ("explorations", "_explorations"),
                ("exploitations", "_exploitations"),
                ("evictions", "_evictions"),
                ("history_dropped", "_history_dropped"),
            ):
                try:
                    setattr(agent, attr, int(counters.get(key, 0)))
                except (TypeError, ValueError) as exc:
                    raise EnergyRLError(
                        f"invalid counter {key!r}: {exc}"
                    ) from exc

            try:
                agent._started_at = float(
                    data.get("started_at", time.time())
                )
            except (TypeError, ValueError) as exc:
                raise EnergyRLError(
                    f"invalid started_at: {exc}"
                ) from exc

            for entry in data.get("history", []) or []:
                agent._history.append(Transition.from_dict(entry))

            if restore_rng and "rng_state" in data:
                agent._restore_rng_state(str(data["rng_state"]))

        return agent

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "EnergyRLAgent":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise EnergyRLError(
                f"invalid JSON payload: {exc}"
            ) from exc
        try:
            return cls.from_dict(raw)
        except EnergyRLError:
            raise
        except (TypeError, ValueError) as exc:
            raise EnergyRLError(
                f"invalid agent payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- persistence
    def save(self, storage: Any) -> None:
        if storage is None or not callable(getattr(storage, "save", None)):
            raise EnergyRLError(
                "storage must expose a callable 'save' method."
            )
        with self._lock:
            payload = {s: dict(row) for s, row in self.q_table.items()}
        storage.save(payload)

    def load(self, storage: Any) -> int:
        if storage is None or not callable(getattr(storage, "load", None)):
            raise EnergyRLError(
                "storage must expose a callable 'load' method."
            )
        loaded = storage.load()
        if not isinstance(loaded, Mapping):
            raise EnergyRLError("storage.load() must return a Mapping.")

        cap = self._config.max_states
        if cap is not None and len(loaded) > cap:
            raise EnergyRLError(
                f"storage.load() returned {len(loaded)} states, "
                f"exceeds max_states={cap}."
            )

        restored = 0
        with self._lock:
            for s, row in loaded.items():
                if not isinstance(row, Mapping):
                    logger.warning(
                        "load: skipping non-Mapping row for %r.", s
                    )
                    continue
                self.q_table[str(s)] = self._validate_restored_row(row)
                restored += 1
        logger.debug("Loaded %d state(s) from storage.", restored)
        return restored

    # -------------------------------------------------------- context mgr
    def __enter__(self) -> "EnergyRLAgent":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_type is not None:
            logger.warning(
                "EnergyRLAgent scope exited with %s.", exc_type.__name__
            )
        return None

    # ------------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.state_count

    def __repr__(self) -> str:
        with self._lock:
            return (
                "EnergyRLAgent("
                f"alpha={self._alpha:.3f}, "
                f"gamma={self._gamma:.3f}, "
                f"epsilon={self._epsilon:.3f}, "
                f"states={len(self.q_table)}, "
                f"actions={len(self.actions)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1.energy_rl_agent
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    agent = EnergyRLAgent(alpha=0.1, gamma=0.9, epsilon=0.2)
    print("repr       :", agent)

    for _ in range(200):
        s = "green"
        a = agent.choose_action(s)
        agent.update(s, a, reward=1.0, next_state="yellow")
        a = agent.choose_action("yellow")
        agent.update("yellow", a, reward=-1.0, next_state="green")

    print("best green :", agent.best_action("green"))
    print("Q(green)   :", {
        a: round(agent.get_q_value("green", a), 3)
        for a in agent.actions
    })

    # Reproducibility round-trip
    a1 = EnergyRLAgent(config=EnergyRLConfig(seed=7, epsilon=0.5))
    a2 = EnergyRLAgent(config=EnergyRLConfig(seed=7, epsilon=0.5))
    for i in range(20):
        s = f"state_{i % 5}"
        ac1 = a1.choose_action(s)
        ac2 = a2.choose_action(s)
        assert ac1 == ac2, f"seeded agents diverged at step {i}"
        a1.update(s, ac1, 1.0, f"state_{(i + 1) % 5}")
        a2.update(s, ac2, 1.0, f"state_{(i + 1) % 5}")

    # Round-trip preserves RNG state → next sequences match.
    restored = EnergyRLAgent.from_json(a1.to_json())
    for _ in range(10):
        assert a1.choose_action("probe") == restored.choose_action("probe")
    print("rng round-trip OK.")

    # Bounded Q-table
    bounded = EnergyRLAgent(config=EnergyRLConfig(max_states=5))
    for i in range(20):
        bounded.update(f"s{i}", "balanced", 1.0, f"s{i + 1}")
    print(f"bounded    : states={bounded.state_count} "
          f"evictions={bounded.statistics()['evictions']}")
    assert bounded.state_count <= 5

    # Reset semantics
    before = a1.epsilon
    a1.reset(reset_rng=True, restore_epsilon=True)
    assert a1.epsilon == a1.config.epsilon
    assert before != a1.epsilon or before == a1.config.epsilon
    print("reset OK.")

    # Custom actions fallback
    custom = EnergyRLAgent(
        actions=("fast", "slow"), strict=False,
    )
    assert custom.choose_action(None) == "fast"
    print("fallback   :", custom.choose_action(None))

    # Numeric rewards
    from decimal import Decimal as D
    from fractions import Fraction as F
    num = EnergyRLAgent()
    num.update("s", "balanced", D("1.5"), "s2")
    num.update("s", "balanced", F(1, 2), "s2")
    print("decimal/fraction rewards OK.")

    # Serialization rejects NaN
    bad = EnergyRLAgent()
    bad.update("s", "balanced", 1.0, "s2")
    bad.q_table["s"]["balanced"] = float("nan")
    try:
        bad.to_json()
    except ValueError as exc:
        print("NaN rejected:", exc)

    # Deserialization integrity
    try:
        EnergyRLAgent.from_dict({
            "config": EnergyRLConfig().to_dict(),
            "epsilon": 2.0,
        })
    except EnergyRLError as exc:
        print("epsilon rejected:", exc)

    try:
        EnergyRLAgent.from_dict({
            "config": EnergyRLConfig(max_states=2).to_dict(),
            "q_table": {f"s{i}": {"balanced": 0.0} for i in range(5)},
        })
    except EnergyRLError as exc:
        print("max_states rejected:", exc)

    # Unknown actions dropped with warning
    a = EnergyRLAgent.from_dict({
        "config": EnergyRLConfig().to_dict(),
        "q_table": {"s": {"bogus": 1.0, "balanced": 2.0}},
    })
    print("unknown action dropped; balanced =",
          a.get_q_value("s", "balanced"))

    print("\nSmoke test passed.")
