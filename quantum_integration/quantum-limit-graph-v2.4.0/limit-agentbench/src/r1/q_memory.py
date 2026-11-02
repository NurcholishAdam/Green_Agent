# src/r1/q_memory.py

"""
Q Memory
========

JSON-backed key-value memory for Q-bias terms. Unlike
:class:`r1.q_learning.QLearningAgent`, this module tracks an *exponential
moving average* per ``(state, action)`` pair rather than a full TD update:

    Q(s, a) ← Q(s, a) + α · (reward − Q(s, a))

This is useful as a lightweight bias/prior that other RL agents consult
(e.g. ``PPOAgent`` can initialize its action distribution from the memory).

Design
------
- Frozen, validated :class:`QMemoryConfig`.
- **Atomic writes** — :meth:`save` writes to a temp file in the target
  directory and calls :func:`os.replace`, so a crash never corrupts the
  memory file. ``json.dump(..., allow_nan=False)`` guarantees a corrupt
  in-memory state can never produce a syntactically-valid-but-invalid
  file.
- **Save race safe** — concurrent updates during :meth:`save` are
  preserved as "unsaved" (the counter is captured before releasing the
  lock; only the captured count is subtracted).
- **Load replaces** — :meth:`load` clears the in-memory table, enforces
  ``max_entries`` *and* ``max_actions_per_state``, and rejects
  non-finite Q values. Missing file ⇒ empty table (not stale data).
- **Bounded memory** — LRU state cap; per-state action cap enforced in
  :meth:`update`, :meth:`load`, and :meth:`from_dict`.
- **Full validation** — ``state``/``action``/``reward``/``alpha``
  checked; ``Decimal``, ``Fraction``, ``numpy`` scalars accepted for
  rewards and alphas via ``numbers.Real``.
- **Deterministic** — no randomness; the memory is a pure function of
  observed updates.
- **Serialization** — ``to_dict`` / ``from_dict`` / ``to_json`` /
  ``from_json``. All failure modes raise :class:`QMemoryError`.

Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import os
import tempfile
import threading
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import TracebackType
from typing import Any, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "QMemory",
    "QMemoryConfig",
    "QMemoryError",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class QMemoryError(ValueError):
    """Raised for invalid QMemory inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class QMemoryConfig:
    """Tunable parameters for the Q-memory store."""

    path: str = "q_table.json"
    alpha: float = 0.1
    max_entries: Optional[int] = 10_000
    max_actions_per_state: Optional[int] = 64
    strict: bool = True
    save_interval: int = 100
    auto_load: bool = True
    max_state_length: int = 256

    def __post_init__(self) -> None:
        if not isinstance(self.path, str) or not self.path:
            raise QMemoryError("path must be a non-empty string.")
        if not 0.0 < self.alpha <= 1.0:
            raise QMemoryError(
                f"alpha must be in (0, 1], got {self.alpha}."
            )
        if self.max_entries is not None and self.max_entries <= 0:
            raise QMemoryError("max_entries must be a positive int or None.")
        if (
            self.max_actions_per_state is not None
            and self.max_actions_per_state <= 0
        ):
            raise QMemoryError(
                "max_actions_per_state must be a positive int or None."
            )
        if not isinstance(self.strict, bool):
            raise QMemoryError("strict must be bool.")
        if not isinstance(self.auto_load, bool):
            raise QMemoryError("auto_load must be bool.")
        if (
            isinstance(self.save_interval, bool)
            or not isinstance(self.save_interval, int)
            or self.save_interval < 0
        ):
            raise QMemoryError(
                "save_interval must be a non-negative int."
            )
        if self.max_state_length <= 0:
            raise QMemoryError("max_state_length must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QMemoryConfig":
        if not isinstance(data, Mapping):
            raise QMemoryError(
                f"QMemoryConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            return cls(
                path=str(data.get("path", "q_table.json")),
                alpha=float(data.get("alpha", 0.1)),
                max_entries=data.get("max_entries", 10_000),
                max_actions_per_state=data.get(
                    "max_actions_per_state", 64
                ),
                strict=data.get("strict", True),
                save_interval=int(data.get("save_interval", 100)),
                auto_load=data.get("auto_load", True),
                max_state_length=int(data.get("max_state_length", 256)),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, QMemoryError):
                raise
            raise QMemoryError(
                f"invalid QMemoryConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# QMemory
# --------------------------------------------------------------------------- #
class QMemory:
    """
    JSON-backed key-value memory for Q-bias terms.

    Thread-safe, deterministic, and bounded in memory.

    Parameters
    ----------
    path : str, default ``"q_table.json"``
        Path to the JSON file. Parent directories are created on save.
    config : QMemoryConfig, optional
        Typed configuration. When supplied, ``path`` is ignored.
    strict : bool, optional
        Override for ``config.strict``. Only effective at construction;
        logged at DEBUG when it differs from the config.
    """

    def __init__(
        self,
        path: str = "q_table.json",
        *,
        config: Optional[QMemoryConfig] = None,
        strict: Optional[bool] = None,
    ) -> None:
        self._config = config if config is not None else QMemoryConfig(
            path=path
        )
        if strict is not None:
            if not isinstance(strict, bool):
                raise QMemoryError(
                    f"strict must be bool, got {type(strict).__name__}."
                )
            if strict != self._config.strict:
                logger.debug(
                    "QMemory strict override: %s -> %s",
                    self._config.strict, strict,
                )
            self._config = QMemoryConfig(
                **{**self._config.to_dict(), "strict": strict}
            )

        self.path: str = self._config.path
        self._path_obj = Path(self.path)

        self._lock = threading.RLock()
        self.q_table: "OrderedDict[str, Dict[str, float]]" = OrderedDict()

        self._updates: int = 0
        self._since_last_save: int = 0
        self._save_failures: int = 0
        self._load_failures: int = 0
        self._evictions: int = 0
        self._started_at: float = time.time()
        self._last_saved_at: Optional[float] = None
        self._last_loaded_at: Optional[float] = None

        if self._config.auto_load:
            self.load()

        logger.debug(
            "QMemory initialized (path=%s, alpha=%.3f, max_entries=%s, "
            "max_actions_per_state=%s, save_interval=%d, strict=%s)",
            self.path, self._config.alpha,
            self._config.max_entries,
            self._config.max_actions_per_state,
            self._config.save_interval, self._config.strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> QMemoryConfig:
        return self._config

    @property
    def state_count(self) -> int:
        with self._lock:
            return len(self.q_table)

    @property
    def entry_count(self) -> int:
        """Total number of ``(state, action)`` pairs."""
        with self._lock:
            return sum(len(row) for row in self.q_table.values())

    @property
    def has_unsaved_changes(self) -> bool:
        with self._lock:
            return self._since_last_save > 0

    @property
    def last_saved_at(self) -> Optional[datetime]:
        with self._lock:
            ts = self._last_saved_at
        if ts is None:
            return None
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    # ---------------------------------------------------------- validation
    def _validate_state(
        self,
        state: Any,
        *,
        name: str = "state",
        enforce_length: bool = False,
    ) -> Optional[str]:
        if state is None or (isinstance(state, str) and not state):
            msg = f"{name} must not be None or empty, got {state!r}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        s = str(state)
        if enforce_length and len(s) > self._config.max_state_length:
            msg = (
                f"{name} length {len(s)} exceeds "
                f"max_state_length={self._config.max_state_length}."
            )
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return s

    def _validate_action(self, action: Any) -> Optional[str]:
        if action is None or (isinstance(action, str) and not action):
            msg = f"action must not be None or empty, got {action!r}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return str(action)

    def _validate_reward(self, reward: Any) -> Optional[float]:
        if isinstance(reward, bool) or not isinstance(
            reward, (numbers.Real, Decimal)
        ):
            msg = f"reward must be real, got {type(reward).__name__}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping update.", msg)
            return None
        try:
            fv = float(reward)
        except (TypeError, ValueError) as exc:
            msg = f"reward not convertible to float: {exc}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping update.", msg)
            return None
        if not math.isfinite(fv):
            msg = f"reward must be finite, got {reward!r}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping update.", msg)
            return None
        return fv

    def _validate_alpha(self, alpha: Any) -> Optional[float]:
        if isinstance(alpha, bool) or not isinstance(
            alpha, (numbers.Real, Decimal)
        ):
            msg = f"alpha must be real, got {type(alpha).__name__}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping update.", msg)
            return None
        try:
            fv = float(alpha)
        except (TypeError, ValueError) as exc:
            msg = f"alpha not convertible to float: {exc}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping update.", msg)
            return None
        if not math.isfinite(fv) or not 0.0 < fv <= 1.0:
            msg = f"alpha must be finite in (0, 1], got {alpha!r}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping update.", msg)
            return None
        return fv

    def _enforce_capacity_locked(self) -> None:
        """Evict LRU states when over ``max_entries``. Lock must be held."""
        cap = self._config.max_entries
        if cap is None:
            return
        while len(self.q_table) > cap:
            self.q_table.popitem(last=False)
            self._evictions += 1

    def _truncate_row_locked(
        self, row: Dict[str, float]
    ) -> Dict[str, float]:
        """Keep only the last N actions (MRU-tail) in ``row``."""
        cap = self._config.max_actions_per_state
        if cap is None or len(row) <= cap:
            return row
        # Keep the tail (most recently inserted).
        return dict(list(row.items())[-cap:])

    # ---------------------------------------------------------- persistence
    def load(self) -> int:
        """
        Load the Q-table from disk.

        **Replaces** the in-memory table (including clearing it when the
        file is missing). Returns the number of states restored. Enforces
        ``max_entries`` and ``max_actions_per_state``, and rejects
        non-finite values in strict mode.
        """
        path = self._path_obj
        if not path.exists():
            logger.debug(
                "Q-table file %s does not exist; clearing.", path,
            )
            with self._lock:
                self.q_table.clear()
                self._since_last_save = 0
                self._last_loaded_at = time.time()
            return 0

        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            msg = f"corrupt Q-table file {path}: {exc}"
            logger.error("%s", msg)
            with self._lock:
                self._load_failures += 1
            if self._config.strict:
                raise QMemoryError(msg) from exc
            return 0
        except OSError as exc:
            msg = f"could not read Q-table file {path}: {exc}"
            logger.error("%s", msg)
            with self._lock:
                self._load_failures += 1
            if self._config.strict:
                raise QMemoryError(msg) from exc
            return 0

        if not isinstance(data, Mapping):
            msg = (
                f"Q-table root must be a Mapping, got "
                f"{type(data).__name__}."
            )
            logger.error("%s", msg)
            with self._lock:
                self._load_failures += 1
            if self._config.strict:
                raise QMemoryError(msg)
            return 0

        restored = 0
        with self._lock:
            self.q_table.clear()
            for state, row in data.items():
                if not isinstance(row, Mapping):
                    if self._config.strict:
                        raise QMemoryError(
                            f"state {state!r} maps to "
                            f"{type(row).__name__}; expected Mapping."
                        )
                    logger.warning(
                        "skipping malformed state %r (row type=%s).",
                        state, type(row).__name__,
                    )
                    continue

                parsed_row: Dict[str, float] = {}
                for action, value in row.items():
                    try:
                        fv = float(value)
                    except (TypeError, ValueError):
                        if self._config.strict:
                            raise QMemoryError(
                                f"value for {state!r}/{action!r} is not "
                                f"numeric: {value!r}."
                            )
                        logger.warning(
                            "skipping non-numeric value for %r/%r.",
                            state, action,
                        )
                        continue
                    if not math.isfinite(fv):
                        if self._config.strict:
                            raise QMemoryError(
                                f"value for {state!r}/{action!r} is "
                                f"non-finite: {fv}."
                            )
                        logger.warning(
                            "skipping non-finite value for %r/%r.",
                            state, action,
                        )
                        continue
                    parsed_row[str(action)] = fv

                parsed_row = self._truncate_row_locked(parsed_row)
                if parsed_row:
                    self.q_table[str(state)] = parsed_row
                    restored += 1

            self._enforce_capacity_locked()
            self._since_last_save = 0
            self._last_loaded_at = time.time()

        logger.debug("Q-table loaded (%d state(s)).", restored)
        return restored

    def save(self) -> bool:
        """
        Atomically persist the Q-table to disk.

        Concurrent updates that land during the file write are preserved
        as unsaved (the counter is captured before releasing the lock and
        only the captured count is subtracted on success).

        Returns True on success, False on failure in non-strict mode. In
        strict mode, raises :class:`QMemoryError` on any I/O failure,
        including a NaN/Inf in the in-memory table (``allow_nan=False``).
        """
        path = self._path_obj
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            msg = f"could not create parent directory for {path}: {exc}"
            logger.error("%s", msg)
            with self._lock:
                self._save_failures += 1
            if self._config.strict:
                raise QMemoryError(msg) from exc
            return False

        with self._lock:
            snapshot = {s: dict(row) for s, row in self.q_table.items()}
            snapshot_counter = self._since_last_save

        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".", suffix=".tmp",
                dir=str(path.parent),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(snapshot, f, allow_nan=False)
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except (OSError, ValueError) as exc:
            msg = f"could not save Q-table to {path}: {exc}"
            logger.error("%s", msg)
            with self._lock:
                self._save_failures += 1
            if self._config.strict:
                raise QMemoryError(msg) from exc
            return False

        with self._lock:
            # Preserve concurrent increments that landed during the write.
            self._since_last_save = max(
                0, self._since_last_save - snapshot_counter
            )
            self._last_saved_at = time.time()

        logger.debug("Q-table saved (%d state(s)).", len(snapshot))
        return True

    # ---------------------------------------------------------- updates
    def update(
        self,
        state: Any,
        action: Any,
        reward: Any,
        alpha: Optional[float] = None,
    ) -> Optional[float]:
        """
        Update the Q-bias for ``(state, action)``.

        Q(s, a) ← Q(s, a) + α · (reward − Q(s, a))

        Returns the new Q-value, or ``None`` when any input was rejected
        in non-strict mode.
        """
        state_key = self._validate_state(state, enforce_length=True)
        if state_key is None:
            return None
        action_key = self._validate_action(action)
        if action_key is None:
            return None
        reward_f = self._validate_reward(reward)
        if reward_f is None:
            return None

        if alpha is None:
            a = self._config.alpha
        else:
            a = self._validate_alpha(alpha)
            if a is None:
                return None

        with self._lock:
            row = self.q_table.get(state_key)
            if row is None:
                row = {}
                self.q_table[state_key] = row

            old_value = row.get(action_key, 0.0)
            new_value = old_value + a * (reward_f - old_value)

            # Move to MRU tail — guarantees the just-written action is never
            # the eviction victim below.
            row.pop(action_key, None)
            row[action_key] = new_value

            self.q_table.move_to_end(state_key)

            cap_actions = self._config.max_actions_per_state
            if cap_actions is not None and len(row) > cap_actions:
                while len(row) > cap_actions:
                    oldest = next(iter(row))
                    row.pop(oldest, None)

            self._enforce_capacity_locked()

            self._updates += 1
            self._since_last_save += 1

            should_save = (
                self._config.save_interval > 0
                and self._since_last_save >= self._config.save_interval
            )

        if should_save:
            self.save()

        logger.debug(
            "update: s=%s a=%s r=%.4f Q:%.4f→%.4f (α=%.3f)",
            state_key, action_key, reward_f, old_value, new_value, a,
        )
        return new_value

    def get_bias(self, state: Any, action: Any) -> float:
        """Return the Q-bias for ``(state, action)`` (0.0 when unseen)."""
        state_key = self._validate_state(state)
        action_key = self._validate_action(action)
        if state_key is None or action_key is None:
            return 0.0
        with self._lock:
            row = self.q_table.get(state_key)
            if row is None:
                return 0.0
            self.q_table.move_to_end(state_key)
            return row.get(action_key, 0.0)

    def best_action(self, state: Any) -> Optional[str]:
        """Return the action with the highest Q-bias for ``state``."""
        state_key = self._validate_state(state)
        if state_key is None:
            return None
        with self._lock:
            row = self.q_table.get(state_key)
            if not row:
                return None
            self.q_table.move_to_end(state_key)
            best_q = max(row.values())
            candidates = sorted(a for a, q in row.items() if q == best_q)
            return candidates[0] if candidates else None

    def state_biases(self, state: Any) -> Dict[str, float]:
        """Return a copy of the action → bias map for ``state``."""
        state_key = self._validate_state(state)
        if state_key is None:
            return {}
        with self._lock:
            row = self.q_table.get(state_key)
            if row is None:
                return {}
            self.q_table.move_to_end(state_key)
            return dict(row)

    # ---------------------------------------------------------- lifecycle
    def reset(
        self,
        *,
        clear_table: bool = True,
        delete_file: bool = False,
    ) -> int:
        """Reset in-memory state (and optionally delete the on-disk file)."""
        with self._lock:
            removed = len(self.q_table)
            if clear_table:
                self.q_table.clear()
            self._updates = 0
            self._since_last_save = 0
            self._save_failures = 0
            self._load_failures = 0
            self._evictions = 0
            self._started_at = time.time()

        if delete_file:
            try:
                self._path_obj.unlink(missing_ok=True)
                logger.debug("deleted Q-table file %s.", self._path_obj)
            except OSError as exc:
                logger.warning(
                    "could not delete %s: %s", self._path_obj, exc,
                )

        logger.debug("QMemory reset (removed %d state(s)).", removed)
        return removed

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Aggregate statistics about the memory and its persistence."""
        with self._lock:
            states = len(self.q_table)
            entries = sum(len(row) for row in self.q_table.values())
            updates = self._updates
            since_save = self._since_last_save
            save_failures = self._save_failures
            load_failures = self._load_failures
            evictions = self._evictions
            last_saved = self._last_saved_at
            last_loaded = self._last_loaded_at
            all_values = [
                v for row in self.q_table.values() for v in row.values()
            ]
            started = self._started_at

        payload: Dict[str, Any] = {
            "path": self.path,
            "states": states,
            "entries": entries,
            "updates": updates,
            "evictions": evictions,
            "save_failures": save_failures,
            "load_failures": load_failures,
            "has_unsaved_changes": since_save > 0,
            "uptime_seconds": time.time() - started,
            "last_saved_at": (
                datetime.fromtimestamp(
                    last_saved, tz=timezone.utc
                ).isoformat()
                if last_saved is not None else None
            ),
            "last_loaded_at": (
                datetime.fromtimestamp(
                    last_loaded, tz=timezone.utc
                ).isoformat()
                if last_loaded is not None else None
            ),
        }
        if all_values:
            payload["mean_bias"] = sum(all_values) / len(all_values)
            payload["min_bias"] = min(all_values)
            payload["max_bias"] = max(all_values)
        return payload

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_table: bool = True) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "counters": {
                    "updates": self._updates,
                    "since_last_save": self._since_last_save,
                    "save_failures": self._save_failures,
                    "load_failures": self._load_failures,
                    "evictions": self._evictions,
                },
                "started_at": self._started_at,
                "last_saved_at": self._last_saved_at,
                "last_loaded_at": self._last_loaded_at,
            }
            if include_table:
                payload["q_table"] = {
                    s: dict(row) for s, row in self.q_table.items()
                }
        return payload

    @staticmethod
    def _coerce_timestamp(value: Any, key: str) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(
            value, (int, float)
        ):
            raise QMemoryError(
                f"invalid {key}: {value!r} (expected number or null)."
            )
        return float(value)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        path_override: Optional[str] = None,
    ) -> "QMemory":
        """
        Rebuild a :class:`QMemory` from :meth:`to_dict` output.

        ``path_override`` restores to a different file than the config
        recorded (useful for read-only replay / test isolation).
        """
        if not isinstance(data, Mapping):
            raise QMemoryError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )

        cfg_data = dict(data.get("config", {}) or {})
        cfg = QMemoryConfig.from_dict(cfg_data)
        if path_override is not None:
            cfg = QMemoryConfig(
                **{**cfg.to_dict(), "path": str(path_override)},
            )
        cfg = QMemoryConfig(**{**cfg.to_dict(), "auto_load": False})
        mem = cls(config=cfg)

        raw_table = data.get("q_table") or {}
        if not isinstance(raw_table, Mapping):
            raise QMemoryError("q_table must be a Mapping.")

        with mem._lock:
            mem.q_table.clear()
            for state, row in raw_table.items():
                if not isinstance(row, Mapping):
                    raise QMemoryError(
                        f"state {state!r} maps to "
                        f"{type(row).__name__}; expected Mapping."
                    )
                parsed: Dict[str, float] = {}
                for act, value in row.items():
                    try:
                        fv = float(value)
                    except (TypeError, ValueError) as exc:
                        raise QMemoryError(
                            f"non-numeric Q value for "
                            f"{state!r}/{act!r}: {value!r}"
                        ) from exc
                    if not math.isfinite(fv):
                        raise QMemoryError(
                            f"non-finite Q value for "
                            f"{state!r}/{act!r}: {fv}"
                        )
                    parsed[str(act)] = fv
                parsed = mem._truncate_row_locked(parsed)
                if parsed:
                    mem.q_table[str(state)] = parsed

            mem._enforce_capacity_locked()

            counters = dict(data.get("counters", {}) or {})
            try:
                mem._updates = int(counters.get("updates", 0))
                mem._since_last_save = int(
                    counters.get("since_last_save", 0)
                )
                mem._save_failures = int(
                    counters.get("save_failures", 0)
                )
                mem._load_failures = int(
                    counters.get("load_failures", 0)
                )
                mem._evictions = int(counters.get("evictions", 0))
            except (TypeError, ValueError) as exc:
                raise QMemoryError(
                    f"invalid counter: {exc}"
                ) from exc

            try:
                mem._started_at = float(
                    data.get("started_at", time.time())
                )
            except (TypeError, ValueError) as exc:
                raise QMemoryError(
                    f"invalid started_at: {exc}"
                ) from exc

            mem._last_saved_at = cls._coerce_timestamp(
                data.get("last_saved_at"), "last_saved_at"
            )
            mem._last_loaded_at = cls._coerce_timestamp(
                data.get("last_loaded_at"), "last_loaded_at"
            )

        return mem

    def to_json(
        self, *, include_table: bool = True, **kwargs: Any
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        return json.dumps(
            self.to_dict(include_table=include_table), **kwargs
        )

    @classmethod
    def from_json(
        cls, payload: str, *, path_override: Optional[str] = None
    ) -> "QMemory":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise QMemoryError(
                f"invalid JSON payload: {exc}"
            ) from exc
        try:
            return cls.from_dict(raw, path_override=path_override)
        except QMemoryError:
            raise
        except (TypeError, ValueError) as exc:
            raise QMemoryError(
                f"invalid QMemory payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "QMemory":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_type is None and self.has_unsaved_changes:
            try:
                self.save()
            except QMemoryError:
                logger.exception("failed to save QMemory on context exit.")
        elif exc_type is not None:
            logger.warning(
                "QMemory scope exited with %s.", exc_type.__name__
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.state_count

    def __contains__(self, state: object) -> bool:
        with self._lock:
            return str(state) in self.q_table

    def __repr__(self) -> str:
        with self._lock:
            states = len(self.q_table)
            entries = sum(len(r) for r in self.q_table.values())
            updates = self._updates
            unsaved = self._since_last_save > 0
        return (
            "QMemory("
            f"path={self.path!r}, "
            f"states={states}, "
            f"entries={entries}, "
            f"alpha={self._config.alpha:.3f}, "
            f"updates={updates}, "
            f"unsaved={unsaved}, "
            f"strict={self._config.strict})"
        )


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1.q_memory
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import tempfile

    logging.basicConfig(level=logging.INFO)

    with tempfile.TemporaryDirectory() as td:
        nested_path = Path(td) / "nested" / "dir" / "q_table.json"

        # ---- Happy path ---------------------------------------------- #
        mem = QMemory(
            path=str(nested_path),
            config=QMemoryConfig(save_interval=0),
        )
        print("repr       :", mem)
        print("VERSION    :", VERSION)

        mem.update("green", "energy_saver", reward=1.0)
        mem.update("green", "energy_saver", reward=0.5)
        mem.update("green", "latency_optimized", reward=-0.3)
        mem.update("red", "energy_saver", reward=0.9)

        print("state cnt  :", mem.state_count)
        print("entry cnt  :", mem.entry_count)
        print("bias green/energy_saver:",
              round(mem.get_bias("green", "energy_saver"), 4))
        print("best green :", mem.best_action("green"))

        # ---- Atomic save creates parent dirs ------------------------- #
        assert mem.save()
        assert nested_path.exists()

        # ---- Reload from disk ---------------------------------------- #
        restored = QMemory(
            path=str(nested_path),
            config=QMemoryConfig(save_interval=0, auto_load=True),
        )
        assert restored.get_bias(
            "green", "energy_saver"
        ) == mem.get_bias("green", "energy_saver")
        print("reload     : OK")

        # ---- load() replaces on missing file ------------------------- #
        missing = Path(td) / "missing.json"
        m2 = QMemory(
            path=str(missing),
            config=QMemoryConfig(save_interval=0, auto_load=False),
        )
        m2.update("A", "x", reward=1.0)
        m2.update("B", "x", reward=1.0)
        assert m2.state_count == 2
        # No file on disk → load() must clear the in-memory table.
        n = m2.load()
        assert n == 0 and m2.state_count == 0
        print("load missing: clears table (was {} states)".format(n))

        # ---- Corrupt file handling ----------------------------------- #
        nested_path.write_text("{ not valid json")
        try:
            QMemory(path=str(nested_path), strict=True)
        except QMemoryError as exc:
            print("Rejected   :", exc)
        lenient = QMemory(path=str(nested_path), strict=False)
        print("lenient    :", lenient.state_count, "state(s)")

        # ---- save_interval batching ---------------------------------- #
        batched = QMemory(
            path=str(Path(td) / "batched.json"),
            config=QMemoryConfig(save_interval=5),
        )
        for i in range(20):
            batched.update(f"s{i}", "balanced", reward=1.0)
        print(f"batched    : updates={batched._updates} "
              f"unsaved={batched.has_unsaved_changes}")

        # ---- Bounded memory (LRU eviction) --------------------------- #
        bounded = QMemory(
            path=str(Path(td) / "bounded.json"),
            config=QMemoryConfig(max_entries=3, save_interval=0),
        )
        for i in range(10):
            bounded.update(f"state_{i}", "balanced", reward=1.0)
        assert bounded.state_count <= 3
        print(f"bounded    : states={bounded.state_count} "
              f"evictions={bounded._evictions}")

        # ---- Per-state action cap (the bug fix) ---------------------- #
        capped = QMemory(
            path=str(Path(td) / "capped.json"),
            config=QMemoryConfig(
                max_actions_per_state=2, save_interval=0,
            ),
        )
        # Insert a, b — then re-update a (was oldest at head). After
        # the fix, a must move to MRU tail and c must evict b, not a.
        capped.update("s", "a", reward=1.0)
        capped.update("s", "b", reward=1.0)
        capped.update("s", "a", reward=2.0)   # refresh a
        capped.update("s", "c", reward=1.0)   # over-cap; evicts b
        assert len(capped.state_biases("s")) == 2
        assert "a" in capped.state_biases("s")
        assert "c" in capped.state_biases("s")
        assert "b" not in capped.state_biases("s")
        print("cap        : correct eviction (a survived)")

        # ---- Corrupt payload: NaN Q value ---------------------------- #
        try:
            QMemory.from_dict({
                "config": {},
                "q_table": {"s": {"a": float("nan")}},
            })
        except QMemoryError as exc:
            print("NaN rejected:", exc)

        # ---- to_json refuses NaN ------------------------------------- #
        bad = QMemory(
            path=str(Path(td) / "bad.json"),
            config=QMemoryConfig(save_interval=0, auto_load=False),
        )
        bad.update("s", "a", reward=1.0)
        bad.q_table["s"]["a"] = float("nan")
        try:
            bad.to_json()
        except ValueError as exc:
            print("NaN in to_json rejected:", exc)
        try:
            bad.save()
        except QMemoryError as exc:
            print("NaN in save rejected:", exc)

        # ---- Numeric types ------------------------------------------- #
        from fractions import Fraction as F
        num = QMemory(
            path=str(Path(td) / "num.json"),
            config=QMemoryConfig(save_interval=0, auto_load=False),
        )
        num.update("s", "a", reward=Decimal("1.5"))
        num.update("s", "a", reward=F(1, 2))
        print("Decimal/Fraction rewards: OK")

        # ---- Config validation --------------------------------------- #
        for bad_cfg in (
            dict(path=""),
            dict(alpha=0.0),
            dict(alpha=2.0),
            dict(max_entries=0),
            dict(max_actions_per_state=0),
            dict(save_interval=-1),
            dict(save_interval=True),
            dict(strict="yes"),
            dict(auto_load=1),
            dict(max_state_length=0),
        ):
            try:
                QMemoryConfig(**bad_cfg)  # type: ignore[arg-type]
            except QMemoryError as exc:
                print("Rejected cfg:", exc)

        # ---- Strict input validation --------------------------------- #
        strict = QMemory(
            path=str(Path(td) / "strict.json"),
            config=QMemoryConfig(save_interval=0, strict=True),
        )
        for bad_call in (
            lambda: strict.update("", "a", reward=1.0),
            lambda: strict.update("s", "", reward=1.0),
            lambda: strict.update(None, "a", reward=1.0),
            lambda: strict.update("s", None, reward=1.0),
            lambda: strict.update("s", "a", reward=float("nan")),
            lambda: strict.update("s", "a", reward=float("inf")),
            lambda: strict.update("s", "a", reward=1.0, alpha=0.0),
            lambda: strict.update("s", "a", reward=1.0, alpha=2.0),
            lambda: strict.update(
                "s", "a", reward=1.0, alpha=float("nan")
            ),
            lambda: strict.update("x" * 1000, "a", reward=1.0),
        ):
            try:
                bad_call()
            except QMemoryError as exc:
                print("Rejected   :", exc)

        # ---- Non-strict: invalid alpha now skips (consistent w/ reward) #
        lenient_alpha = QMemory(
            path=str(Path(td) / "lenient_alpha.json"),
            config=QMemoryConfig(
                save_interval=0, auto_load=False, strict=False,
            ),
        )
        assert lenient_alpha.update("s", "a", reward=1.0, alpha=5.0) is None
        print("lenient alpha: skipped (was: fell back to config default)")

        # ---- from_dict validation ------------------------------------ #
        for bad_payload in (
            {"config": {}, "q_table": {"s": {"a": "abc"}}},
            {"config": {}, "q_table": {"s": {"a": float("inf")}}},
            {"config": {}, "q_table": "not-a-mapping"},
            {"config": {}, "started_at": "abc"},
            {"config": {}, "last_saved_at": "abc"},
            {"config": {}, "counters": {"updates": "abc"}},
        ):
            try:
                QMemory.from_dict(bad_payload)
            except QMemoryError as exc:
                print("Rejected payload:", exc)

        # ---- from_dict enforces per-state cap ------------------------ #
        many_actions = {"s": {f"a{i}": 1.0 for i in range(100)}}
        capped_from_dict = QMemory.from_dict({
            "config": QMemoryConfig(
                max_actions_per_state=5, auto_load=False,
            ).to_dict(),
            "q_table": many_actions,
        })
        assert len(capped_from_dict.state_biases("s")) == 5
        print("from_dict  : per-state cap enforced (100 -> 5)")

        # ---- Persistence failure in non-strict mode ------------------ #
        blocker = Path(td) / "blocker"
        blocker.write_text("not a directory")
        bad_path = blocker / "q_table.json"
        failing = QMemory(
            path=str(bad_path),
            config=QMemoryConfig(
                save_interval=0, strict=False, auto_load=False,
            ),
        )
        failing.update("s", "a", reward=1.0)
        assert failing.save() is False
        print("save fail  : returns False (non-strict)")

        # ---- Serialization round-trip -------------------------------- #
        payload = mem.to_json()
        rt = QMemory.from_json(
            payload, path_override=str(Path(td) / "rt.json"),
        )
        assert rt.to_dict() == mem.to_dict()
        print("Round-trip OK.")

        # ---- Context manager auto-save ------------------------------- #
        ctx_path = Path(td) / "ctx.json"
        with QMemory(
            path=str(ctx_path),
            config=QMemoryConfig(save_interval=0, auto_load=False),
        ) as ctx_mem:
            ctx_mem.update("x", "y", reward=1.0)
            assert ctx_mem.has_unsaved_changes
        assert ctx_path.exists()
        print("Context save: OK")

        # ---- reset --------------------------------------------------- #
        removed = mem.reset(delete_file=True)
        assert not nested_path.exists()
        print(f"reset      : {removed} state(s) removed; file deleted")

    print("\nSmoke test passed.")
