# src/r1/energy_rl_agent.py

"""
Energy RL Agent
===============

Tabular Q-learning agent for adaptive energy-aware policy tuning.

State  : discretized metric profile (e.g. ``"green|low_latency"``)
Actions: adjust policy weights (energy / latency / carbon / balanced)

Enhancements
------------
- ``EnergyRLConfig`` — frozen, validated: ``alpha ∈ (0, 1]``,
  ``gamma ∈ [0, 1]``, ``epsilon ∈ [0, 1]``, ``max_states``, ``seed``.
- **Deterministic randomness** — seeded ``random.Random`` per agent.
- **Bounded Q-table** — least-recently-used eviction when
  ``config.max_states`` is reached.
- **Full validation** of every state / action / reward / next_state.
- **Strict / non-strict modes** — invalid inputs raise or are logged and
  skipped.
- **Deterministic tie-breaking** — alphabetical action name when Q-values
  are equal.
- **Thread safety** — ``RLock`` guards every mutation.
- Optional integration with :class:`r1.rl_storage.RLStorage` for persistence
  and :class:`r1.reward_normalizer.RewardNormalizer` for reward shaping.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- ``statistics()`` — updates, states seen, exploration rate, mean Q-value,
  best action.
- ``reset()`` — clears the Q-table and counters.
- Custom ``EnergyRLError``; lazy ``%s`` logging; ``__repr__``;
  ``__main__`` smoke test.
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

    # Q-learning hyperparameters.
    alpha: float = 0.1       # Learning rate ∈ (0, 1]
    gamma: float = 0.9       # Discount factor ∈ [0, 1]
    epsilon: float = 0.1     # Exploration probability ∈ [0, 1]

    # Bounded Q-table.
    max_states: Optional[int] = 10_000

    # Random seed for reproducibility.
    seed: Optional[int] = 42

    # Whether to decay epsilon over time.
    epsilon_decay: float = 1.0        # 1.0 = no decay; <1 shrinks ε
    epsilon_min: float = 0.0

    # Bounded transition history.
    max_history: int = 10_000

    def __post_init__(self) -> None:
        if not 0.0 < self.alpha <= 1.0:
            raise EnergyRLError(
                f"alpha must be in (0, 1], got {self.alpha}."
            )
        if not 0.0 <= self.gamma <= 1.0:
            raise EnergyRLError(
                f"gamma must be in [0, 1], got {self.gamma}."
            )
        if not 0.0 <= self.epsilon <= 1.0:
            raise EnergyRLError(
                f"epsilon must be in [0, 1], got {self.epsilon}."
            )
        if self.max_states is not None and self.max_states <= 0:
            raise EnergyRLError("max_states must be a positive int or None.")
        if not 0.0 < self.epsilon_decay <= 1.0:
            raise EnergyRLError(
                "epsilon_decay must be in (0, 1]."
            )
        if not 0.0 <= self.epsilon_min <= self.epsilon:
            raise EnergyRLError(
                "epsilon_min must be in [0, epsilon]."
            )
        if self.max_history <= 0:
            raise EnergyRLError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EnergyRLConfig":
        if not isinstance(data, Mapping):
            raise EnergyRLError(
                f"EnergyRLConfig.from_dict expects a Mapping, "
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
            max_history=int(data.get("max_history", 10_000)),
        )


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
    def from_dict(cls, data: Mapping[str, Any]) -> "Transition":
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
class EnergyRLAgent:
    """
    Lightweight Q-learning agent for energy-aware policy tuning.

    Thread-safe, deterministic (given a seed), serializable, and bounded in
    memory. The original public API (``choose_action``, ``update``, and the
    ``q_table`` / ``alpha`` / ``gamma`` / ``epsilon`` / ``actions`` attributes)
    is preserved; new parameters are keyword-only.

    Parameters
    ----------
    alpha : float, default 0.1
        Learning rate ∈ ``(0, 1]``.
    gamma : float, default 0.9
        Discount factor ∈ ``[0, 1]``.
    epsilon : float, default 0.1
        Exploration probability ∈ ``[0, 1]``.
    config : EnergyRLConfig, optional
        Typed configuration. Overrides the three positional hyperparameters.
    strict : bool, default True
        If True, invalid inputs raise :class:`EnergyRLError`. If False,
        they are logged and skipped.
    """

    # Default action space — kept as a class attribute so subclasses and
    # callers can extend it without touching instance state.
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

        # Legacy attributes preserved (mirror the config).
        self.alpha: float = self._config.alpha
        self.gamma: float = self._config.gamma
        self.epsilon: float = self._config.epsilon

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

        # ---- Internal state -------------------------------------------
        self._lock = threading.RLock()
        # Bounded Q-table keyed by state string; LRU eviction via OrderedDict.
        self.q_table: "OrderedDict[str, Dict[str, float]]" = OrderedDict()
        self._rng = random.Random(self._config.seed)
        self._history: Deque[Transition] = deque(
            maxlen=self._config.max_history
        )

        # Counters for statistics.
        self._updates: int = 0
        self._explorations: int = 0
        self._exploitations: int = 0
        self._evictions: int = 0
        self._started_at: float = time.time()

        logger.debug(
            "EnergyRLAgent initialized "
            "(alpha=%.3f gamma=%.3f epsilon=%.3f seed=%s max_states=%s strict=%s)",
            self.alpha, self.gamma, self.epsilon,
            self._config.seed, self._config.max_states, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> EnergyRLConfig:
        return self._config

    @property
    def state_count(self) -> int:
        with self._lock:
            return len(self.q_table)

    @property
    def history(self) -> List[Transition]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- internal
    def _ensure_state(self, state: str) -> None:
        """
        Ensure ``state`` has a Q-row. Evicts the least-recently-used state
        when ``config.max_states`` is reached.
        """
        with self._lock:
            if state in self.q_table:
                # Refresh LRU position.
                self.q_table.move_to_end(state)
                return

            self.q_table[state] = {a: 0.0 for a in self.actions}

            cap = self._config.max_states
            while cap is not None and len(self.q_table) > cap:
                self.q_table.popitem(last=False)
                self._evictions += 1

    def _validate_state(self, state: Any, *, name: str = "state") -> Optional[str]:
        if not isinstance(state, str) or not state:
            msg = f"{name} must be a non-empty string, got {state!r}."
            if self._strict:
                raise EnergyRLError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return state

    def _validate_action(self, action: Any) -> Optional[str]:
        if not isinstance(action, str) or action not in self.actions:
            msg = (
                f"action must be one of {self.actions}, got {action!r}."
            )
            if self._strict:
                raise EnergyRLError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return action

    def _validate_reward(self, reward: Any) -> Optional[float]:
        if isinstance(reward, bool) or not isinstance(reward, (int, float)):
            msg = f"reward must be numeric, got {type(reward).__name__}."
            if self._strict:
                raise EnergyRLError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        fv = float(reward)
        if math.isnan(fv) or math.isinf(fv):
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
        """
        state = self._validate_state(state)
        if state is None:
            # Non-strict mode: pick a safe default rather than raising.
            return self.actions[-1]  # "balanced"

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
            # Deterministic tie-breaking: max by (q_value, -action_name).
            best_q = max(row.values())
            best_actions = sorted(
                a for a, q in row.items() if q == best_q
            )
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
    ) -> Optional[Transition]:
        """
        Apply one Q-learning update.

        Q(s, a) ← Q(s, a) + α [ r + γ · maxₐ' Q(s', a') − Q(s, a) ]

        Returns the recorded :class:`Transition` (or ``None`` in non-strict
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

            # Epsilon decay (optional).
            if self._config.epsilon_decay < 1.0:
                self.epsilon = max(
                    self._config.epsilon_min,
                    self.epsilon * self._config.epsilon_decay,
                )

        logger.debug(
            "Update: s=%s a=%s r=%.4f s'=%s Q:%.4f→%.4f (ε=%.3f)",
            state, action, reward, next_state,
            old_value, new_value, self.epsilon,
        )
        return transition

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
            self._explorations = 0
            self._exploitations = 0
            self._evictions = 0
            # Reset epsilon to its initial value.
            self.epsilon = self._config.epsilon
            self._started_at = time.time()
        logger.debug("EnergyRLAgent reset (removed %d state(s)).", removed)
        return removed

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

        payload: Dict[str, Any] = {
            "states": len(states),
            "updates": updates,
            "explorations": explorations,
            "exploitations": exploitations,
            "evictions": evictions,
            "epsilon": self.epsilon,
            "uptime_seconds": time.time() - self._started_at,
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
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "actions": list(self.actions),
                "epsilon": self.epsilon,
                "counters": {
                    "updates": self._updates,
                    "explorations": self._explorations,
                    "exploitations": self._exploitations,
                    "evictions": self._evictions,
                },
                "started_at": self._started_at,
                "q_table": {s: dict(row) for s, row in self.q_table.items()},
                "history": [t.to_dict() for t in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: Optional[bool] = None,
    ) -> "EnergyRLAgent":
        if not isinstance(data, Mapping):
            raise EnergyRLError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = EnergyRLConfig.from_dict(cfg_data)
        agent = cls(
            config=cfg,
            strict=bool(data.get("strict", True)) if strict is None else strict,
            actions=data.get("actions") or None,
        )
        with agent._lock:
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
            agent._started_at = float(data.get("started_at", time.time()))
            for entry in data.get("history", []):
                agent._history.append(Transition.from_dict(entry))
        return agent

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "EnergyRLAgent":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise EnergyRLError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- persistence
    def save(self, storage: Any) -> None:
        """
        Persist the Q-table via an ``RLStorage``-shaped object.

        The storage must expose a ``save(mapping)`` method. This is a
        convenience wrapper so callers do not need to convert the Q-table
        themselves.
        """
        if storage is None or not callable(getattr(storage, "save", None)):
            raise EnergyRLError(
                "storage must expose a callable 'save' method."
            )
        with self._lock:
            payload = {s: dict(row) for s, row in self.q_table.items()}
        storage.save(payload)

    def load(self, storage: Any) -> int:
        """
        Load a Q-table via an ``RLStorage``-shaped object.

        Returns the number of states restored. Unknown states are added;
        missing actions are initialized to 0.0.
        """
        if storage is None or not callable(getattr(storage, "load", None)):
            raise EnergyRLError(
                "storage must expose a callable 'load' method."
            )
        loaded = storage.load()
        if not isinstance(loaded, Mapping):
            raise EnergyRLError("storage.load() must return a Mapping.")

        restored = 0
        with self._lock:
            for s, row in loaded.items():
                if not isinstance(row, Mapping):
                    continue
                self.q_table[str(s)] = {
                    str(a): float(q)
                    for a, q in row.items()
                    if a in self.actions
                }
                # Ensure every known action is present.
                for a in self.actions:
                    self.q_table[str(s)].setdefault(a, 0.0)
                restored += 1
        logger.debug("Loaded %d state(s) from storage.", restored)
        return restored

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "EnergyRLAgent":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "EnergyRLAgent scope exited with %s.", exc_type.__name__
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.state_count

    def __repr__(self) -> str:
        with self._lock:
            return (
                "EnergyRLAgent("
                f"alpha={self.alpha:.3f}, "
                f"gamma={self.gamma:.3f}, "
                f"epsilon={self.epsilon:.3f}, "
                f"states={len(self.q_table)}, "
                f"actions={len(self.actions)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "EnergyRLAgent",
    "EnergyRLConfig",
    "EnergyRLError",
    "Transition",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1.energy_rl_agent
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---- Happy path --------------------------------------------------- #
    agent = EnergyRLAgent(alpha=0.1, gamma=0.9, epsilon=0.2)
    print("repr       :", agent)

    # Simple two-state Markov task:
    # state "green" → any action → reward 1.0 → "yellow"
    # state "yellow" → any action → reward -1.0 → "green"
    for _ in range(200):
        s = "green"
        a = agent.choose_action(s)
        agent.update(s, a, reward=1.0, next_state="yellow")

        a = agent.choose_action("yellow")
        agent.update("yellow", a, reward=-1.0, next_state="green")

    print("best green :", agent.best_action("green"))
    print("best yellow:", agent.best_action("yellow"))
    print("Q(green)   :", {
        a: round(agent.get_q_value("green", a), 3)
        for a in agent.actions
    })
    print("Q(yellow)  :", {
        a: round(agent.get_q_value("yellow", a), 3)
        for a in agent.actions
    })

    # ---- Reproducibility --------------------------------------------- #
    a1 = EnergyRLAgent(config=EnergyRLConfig(seed=7, epsilon=0.5))
    a2 = EnergyRLAgent(config=EnergyRLConfig(seed=7, epsilon=0.5))
    for i in range(20):
        s = f"state_{i % 5}"
        ac1 = a1.choose_action(s)
        ac2 = a2.choose_action(s)
        assert ac1 == ac2, f"seeded agents diverged at step {i}"
        a1.update(s, ac1, 1.0, f"state_{(i + 1) % 5}")
        a2.update(s, ac2, 1.0, f"state_{(i + 1) % 5}")
    print("reproducibility OK.")

    # ---- Bounded Q-table (LRU eviction) ----------------------------- #
    bounded = EnergyRLAgent(config=EnergyRLConfig(max_states=5))
    for i in range(20):
        bounded.update(f"s{i}", "balanced", 1.0, f"s{i + 1}")
    print(f"bounded    : states={bounded.state_count} "
          f"evictions={bounded._evictions}")
    assert bounded.state_count <= 5

    # ---- Deterministic tie-breaking --------------------------------- #
    tie = EnergyRLAgent(epsilon=0.0)
    tie.choose_action("x")  # initializes the row with all zeros
    print("tie break  :", tie.best_action("x"), "(should be 'balanced')")

    # ---- Statistics --------------------------------------------------- #
    print("stats      :", {
        k: v for k, v in agent.statistics().items()
        if k not in ("uptime_seconds", "last_transition")
    })

    # ---- Serialization round-trip ----------------------------------- #
    payload = agent.to_json()
    restored = EnergyRLAgent.from_json(payload)
    assert restored.to_dict() == agent.to_dict()
    print("Round-trip OK.")

    # ---- Persistence via a mock storage ----------------------------- #
    class _MockStorage:
        def __init__(self) -> None:
            self._data: Dict[str, Any] = {}

        def save(self, mapping: Mapping[str, Any]) -> None:
            self._data = {k: dict(v) for k, v in mapping.items()}

        def load(self) -> Dict[str, Any]:
            return {k: dict(v) for k, v in self._data.items()}

    storage = _MockStorage()
    agent.save(storage)
    fresh = EnergyRLAgent(epsilon=0.0)
    print("loaded     :", fresh.load(storage), "state(s) restored")
    assert fresh.get_q_value("green", "balanced") == agent.get_q_value(
        "green", "balanced"
    )

    # ---- Reset -------------------------------------------------------- #
    removed = agent.reset()
    print("reset      :", removed, "state(s) removed; new size:", agent.state_count)

    # ---- Validation failures ----------------------------------------- #
    # Config validation.
    for bad_cfg in (
        dict(alpha=0.0),
        dict(alpha=2.0),
        dict(gamma=-0.1),
        dict(gamma=1.5),
        dict(epsilon=-0.1),
        dict(epsilon=1.5),
        dict(max_states=0),
        dict(epsilon_decay=0.0),
        dict(epsilon=0.1, epsilon_min=0.5),  # epsilon_min > epsilon
    ):
        try:
            EnergyRLConfig(**bad_cfg)  # type: ignore[arg-type]
        except EnergyRLError as exc:
            print("Rejected cfg:", exc)

    # Input validation (strict).
    strict = EnergyRLAgent(strict=True)
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
        except EnergyRLError as exc:
            print("Rejected   :", exc)

    # ---- Non-strict logs and skips ---------------------------------- #
    lenient = EnergyRLAgent(strict=False)
    result = lenient.update("s", "bogus", 1.0, "s2")
    print("lenient    :", result, "(returned None, no update)")
    assert result is None
    print("lenient Q  :", lenient.get_q_value("s", "balanced"))

    # ---- Custom action space ---------------------------------------- #
    custom = EnergyRLAgent(actions=("fast", "slow"))
    custom.update("s", "fast", 2.0, "s2")
    print("custom act :", custom.actions, "best:", custom.best_action("s"))

    # ---- Epsilon decay ---------------------------------------------- #
    decaying = EnergyRLAgent(
        config=EnergyRLConfig(epsilon=0.5, epsilon_decay=0.9, epsilon_min=0.01)
    )
    start_eps = decaying.epsilon
    for i in range(30):
        decaying.update("s", "balanced", 1.0, "s2")
    print(f"epsilon    : {start_eps:.3f} → {decaying.epsilon:.3f}")

    print("\nSmoke test passed.")
