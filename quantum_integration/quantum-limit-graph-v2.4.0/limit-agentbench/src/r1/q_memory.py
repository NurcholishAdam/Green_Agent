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

Enhancements
------------
- ``QMemoryConfig`` — frozen, validated: path, ``alpha`` bounds, capacity,
  strict mode, save policy.
- **Fixed corrupt-file crash** — :meth:`load` no longer raises on a
  partially-written JSON file unless ``strict=True``.
- **Atomic writes** — :meth:`save` writes to a temp file and calls
  ``os.replace``, so a crash never corrupts the memory file.
- **Parent-directory creation** — ``path="nested/dir/q_table.json"`` works
  on the first save.
- **Thread safety** — ``RLock`` guards every mutation and read.
- **Bounded memory** — ``max_entries`` LRU cap; ``max_actions_per_state``
  per-state cap.
- **Full validation** of every ``state`` / ``action`` / ``reward`` / ``alpha``.
- **Deterministic** — no randomness; the memory is a pure function of
  observed updates.
- **Serialization** — ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- **Context-manager** — auto-saves on clean exit when there are pending changes.
- ``statistics()``, ``reset()``, ``__repr__``, custom ``QMemoryError``, lazy
  ``%s`` logging, and a comprehensive ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import os
import tempfile
import threading
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


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

    # Filesystem path to the JSON file. Parent dirs are created on save.
    path: str = "q_table.json"

    # Default learning rate for :meth:`update`. Must be in (0, 1].
    alpha: float = 0.1

    # Bounded capacity. ``None`` disables bounding.
    max_entries: Optional[int] = 10_000
    max_actions_per_state: Optional[int] = 64

    # If True, a corrupt file or invalid value raises :class:`QMemoryError`.
    strict: bool = True

    # If True, :meth:`update` auto-saves when the number of pending changes
    # reaches ``save_interval``. ``0`` disables auto-save entirely.
    save_interval: int = 100

    # If True, :meth:`load` is called from ``__init__``.
    auto_load: bool = True

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
        if not isinstance(self.save_interval, int) or self.save_interval < 0:
            raise QMemoryError("save_interval must be a non-negative int.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QMemoryConfig":
        if not isinstance(data, Mapping):
            raise QMemoryError(
                f"QMemoryConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            path=str(data.get("path", "q_table.json")),
            alpha=float(data.get("alpha", 0.1)),
            max_entries=data.get("max_entries", 10_000),
            max_actions_per_state=data.get("max_actions_per_state", 64),
            strict=bool(data.get("strict", True)),
            save_interval=int(data.get("save_interval", 100)),
            auto_load=bool(data.get("auto_load", True)),
        )


# --------------------------------------------------------------------------- #
# QMemory
# --------------------------------------------------------------------------- #
class QMemory:
    """
    JSON-backed key-value memory for Q-bias terms.

    Thread-safe, deterministic, and bounded in memory. The original public
    API is preserved; new parameters are keyword-only.

    Parameters
    ----------
    path : str, default ``"q_table.json"``
        Path to the JSON file. Parent directories are created on save.
    config : QMemoryConfig, optional
        Typed configuration. When supplied, ``path`` is ignored.
    strict : bool, optional
        Override for ``config.strict``.
    """

    # ------------------------------------------------------------------ init
    def __init__(
        self,
        path: str = "q_table.json",
        *,
        config: Optional[QMemoryConfig] = None,
        strict: Optional[bool] = None,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = QMemoryConfig(path=path)
        if strict is not None:
            # Override requires rebuilding the frozen config.
            self._config = QMemoryConfig(
                **{**self._config.to_dict(), "strict": bool(strict)}
            )

        # Legacy attributes preserved.
        self.path: str = self._config.path
        self._path_obj = Path(self.path)

        self._lock = threading.RLock()
        # Bounded memory via OrderedDict-based LRU.
        self.q_table: "OrderedDict[str, Dict[str, float]]" = OrderedDict()

        # Counters.
        self._updates: int = 0
        self._since_last_save: int = 0
        self._save_failures: int = 0
        self._load_failures: int = 0
        self._evictions: int = 0
        self._started_at: float = time.time()
        self._last_saved_at: Optional[float] = None
        self._last_loaded_at: Optional[float] = None

        # Auto-load from disk unless disabled.
        if self._config.auto_load:
            self.load()

        logger.debug(
            "QMemory initialized (path=%s, alpha=%.3f, max_entries=%s, "
            "max_actions_per_state=%s, save_interval=%d, strict=%s)",
            self.path,
            self._config.alpha,
            self._config.max_entries,
            self._config.max_actions_per_state,
            self._config.save_interval,
            self._config.strict,
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
        return (
            datetime.fromtimestamp(ts, tz=timezone.utc)
            if ts is not None
            else None
        )

    # ---------------------------------------------------------- persistence
    def load(self) -> int:
        """
        Load the Q-table from disk.

        Returns the number of states restored (0 when the file is missing
        or unreadable in non-strict mode). Existing in-memory state is
        **replaced**.
        """
        path = self._path_obj
        if not path.exists():
            logger.debug("Q-table file %s does not exist; starting empty.", path)
            return 0

        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            msg = f"Corrupt Q-table file {path}: {exc}"
            logger.error("%s", msg)
            with self._lock:
                self._load_failures += 1
            if self._config.strict:
                raise QMemoryError(msg) from exc
            return 0
        except OSError as exc:
            msg = f"Could not read Q-table file {path}: {exc}"
            logger.error("%s", msg)
            with self._lock:
                self._load_failures += 1
            if self._config.strict:
                raise QMemoryError(msg) from exc
            return 0

        if not isinstance(data, Mapping):
            msg = (
                f"Q-table root must be a Mapping, got {type(data).__name__}."
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
                            f"State {state!r} maps to {type(row).__name__}; "
                            "expected Mapping."
                        )
                    logger.warning(
                        "Skipping malformed state %r (row type=%s).",
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
                                f"Value for {state!r}/{action!r} is not "
                                f"numeric: {value!r}."
                            )
                        logger.warning(
                            "Skipping non-numeric value for %r/%r.",
                            state, action,
                        )
                        continue
                    if math.isnan(fv) or math.isinf(fv):
                        if self._config.strict:
                            raise QMemoryError(
                                f"Value for {state!r}/{action!r} is "
                                f"non-finite: {fv}."
                            )
                        continue
                    parsed_row[str(action)] = fv
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

        Returns True on success, False on failure in non-strict mode. In
        strict mode, raises :class:`QMemoryError` on any I/O failure.
        """
        path = self._path_obj
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            msg = f"Could not create parent directory for {path}: {exc}"
            logger.error("%s", msg)
            with self._lock:
                self._save_failures += 1
            if self._config.strict:
                raise QMemoryError(msg) from exc
            return False

        with self._lock:
            snapshot = {s: dict(row) for s, row in self.q_table.items()}

        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(snapshot, f)
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except OSError as exc:
            msg = f"Could not save Q-table to {path}: {exc}"
            logger.error("%s", msg)
            with self._lock:
                self._save_failures += 1
            if self._config.strict:
                raise QMemoryError(msg) from exc
            return False

        with self._lock:
            self._since_last_save = 0
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

        Parameters
        ----------
        state : any
            Coerced to ``str(state)``. Empty strings are rejected in strict
            mode.
        action : any
            Coerced to ``str(action)``. Empty strings are rejected in strict
            mode.
        reward : float
            Must be finite. NaN / Inf are rejected in strict mode.
        alpha : float, optional
            Learning rate. Must be in ``(0, 1]``. Defaults to
            ``config.alpha``.

        Returns
        -------
        float | None
            The new Q-value, or ``None`` if the input was rejected in
            non-strict mode.
        """
        # ---- Validate ------------------------------------------------
        if state is None or (isinstance(state, str) and not state):
            msg = f"state must not be None or empty, got {state!r}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping update.", msg)
            return None
        if action is None or (isinstance(action, str) and not action):
            msg = f"action must not be None or empty, got {action!r}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping update.", msg)
            return None

        reward_f = self._validate_reward(reward)
        if reward_f is None:
            return None

        a = self._config.alpha if alpha is None else self._validate_alpha(alpha)
        if a is None:
            return None

        state_key = str(state)
        action_key = str(action)

        # ---- Apply update --------------------------------------------
        with self._lock:
            row = self.q_table.get(state_key)
            if row is None:
                row = {}
                self.q_table[state_key] = row

            old_value = row.get(action_key, 0.0)
            new_value = old_value + a * (reward_f - old_value)
            row[action_key] = new_value

            # Refresh LRU position.
            self.q_table.move_to_end(state_key)

            # Per-state action cap.
            cap_actions = self._config.max_actions_per_state
            if cap_actions is not None and len(row) > cap_actions:
                # Drop the least-recently-updated action — approximated by
                # insertion order; dict preserves insertion, and new keys
                # are appended, so the oldest entry is at the front.
                while len(row) > cap_actions:
                    oldest = next(iter(row))
                    if oldest == action_key:
                        # Never evict the entry we just wrote.
                        break
                    row.pop(oldest, None)

            # Overall state cap.
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
            "Update: s=%s a=%s r=%.4f Q:%.4f→%.4f (α=%.3f)",
            state_key, action_key, reward_f, old_value, new_value, a,
        )
        return new_value

    def get_bias(self, state: Any, action: Any) -> float:
        """
        Return the Q-bias for ``(state, action)``.

        Returns ``0.0`` when the pair is unseen.
        """
        if state is None or (isinstance(state, str) and not state):
            if self._config.strict:
                raise QMemoryError(
                    f"state must not be None or empty, got {state!r}."
                )
            return 0.0
        if action is None or (isinstance(action, str) and not action):
            if self._config.strict:
                raise QMemoryError(
                    f"action must not be None or empty, got {action!r}."
                )
            return 0.0

        state_key = str(state)
        action_key = str(action)
        with self._lock:
            row = self.q_table.get(state_key)
            if row is None:
                return 0.0
            self.q_table.move_to_end(state_key)
            return row.get(action_key, 0.0)

    def best_action(self, state: Any) -> Optional[str]:
        """Return the action with the highest Q-bias for ``state`` (or None)."""
        if state is None or (isinstance(state, str) and not state):
            if self._config.strict:
                raise QMemoryError(
                    f"state must not be None or empty, got {state!r}."
                )
            return None
        state_key = str(state)
        with self._lock:
            row = self.q_table.get(state_key)
            if not row:
                return None
            self.q_table.move_to_end(state_key)
            best_q = max(row.values())
            candidates = sorted(a for a, q in row.items() if q == best_q)
            return candidates[0] if candidates else None

    def state_biases(self, state: Any) -> Dict[str, float]:
        """Return a copy of the entire action → bias map for ``state``."""
        if state is None or (isinstance(state, str) and not state):
            if self._config.strict:
                raise QMemoryError(
                    f"state must not be None or empty, got {state!r}."
                )
            return {}
        state_key = str(state)
        with self._lock:
            row = self.q_table.get(state_key)
            if row is None:
                return {}
            self.q_table.move_to_end(state_key)
            return dict(row)

    # ---------------------------------------------------------- validation
    def _validate_reward(self, reward: Any) -> Optional[float]:
        if isinstance(reward, bool) or not isinstance(reward, (int, float)):
            msg = f"reward must be numeric, got {type(reward).__name__}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping update.", msg)
            return None
        fv = float(reward)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"reward must be finite, got {reward!r}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Skipping update.", msg)
            return None
        return fv

    def _validate_alpha(self, alpha: Any) -> Optional[float]:
        if isinstance(alpha, bool) or not isinstance(alpha, (int, float)):
            msg = f"alpha must be numeric, got {type(alpha).__name__}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Using config default.", msg)
            return self._config.alpha
        fv = float(alpha)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"alpha must be finite, got {alpha!r}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Using config default.", msg)
            return self._config.alpha
        if not 0.0 < fv <= 1.0:
            msg = f"alpha must be in (0, 1], got {fv}."
            if self._config.strict:
                raise QMemoryError(msg)
            logger.warning("%s Using config default.", msg)
            return self._config.alpha
        return fv

    def _enforce_capacity_locked(self) -> None:
        """Evict the least-recently-used states when over ``max_entries``."""
        cap = self._config.max_entries
        if cap is None:
            return
        while len(self.q_table) > cap:
            self.q_table.popitem(last=False)
            self._evictions += 1

    # ---------------------------------------------------------- lifecycle
    def reset(
        self,
        *,
        clear_table: bool = True,
        delete_file: bool = False,
    ) -> int:
        """
        Reset the in-memory state.

        Parameters
        ----------
        clear_table : bool, default True
            Clear the Q-table.
        delete_file : bool, default False
            If True, remove the on-disk Q-table file.

        Returns the number of states removed.
        """
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
                logger.debug("Deleted Q-table file %s.", self._path_obj)
            except OSError as exc:
                logger.warning("Could not delete %s: %s", self._path_obj, exc)

        logger.debug("QMemory reset (removed %d state(s)).", removed)
        return removed

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics about the memory and its persistence."""
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

        payload: Dict[str, Any] = {
            "path": self.path,
            "states": states,
            "entries": entries,
            "updates": updates,
            "evictions": evictions,
            "save_failures": save_failures,
            "load_failures": load_failures,
            "has_unsaved_changes": since_save > 0,
            "uptime_seconds": time.time() - self._started_at,
            "last_saved_at": (
                datetime.fromtimestamp(last_saved, tz=timezone.utc).isoformat()
                if last_saved is not None else None
            ),
            "last_loaded_at": (
                datetime.fromtimestamp(last_loaded, tz=timezone.utc).isoformat()
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

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        path_override: Optional[str] = None,
    ) -> "QMemory":
        """
        Rebuild a :class:`QMemory` from :meth:`to_dict` output.

        ``path_override`` lets callers restore to a different file than the
        one recorded in the config (useful for read-only replay or
        test isolation).
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
        # Construct without auto-load so we can safely inject state.
        cfg = QMemoryConfig(**{**cfg.to_dict(), "auto_load": False})
        mem = cls(config=cfg)

        with mem._lock:
            mem.q_table.clear()
            for s, row in (data.get("q_table") or {}).items():
                if not isinstance(row, Mapping):
                    continue
                mem.q_table[str(s)] = {
                    str(a): float(v) for a, v in row.items()
                }
            mem._enforce_capacity_locked()
            counters = dict(data.get("counters", {}) or {})
            mem._updates = int(counters.get("updates", 0))
            mem._since_last_save = int(counters.get("since_last_save", 0))
            mem._save_failures = int(counters.get("save_failures", 0))
            mem._load_failures = int(counters.get("load_failures", 0))
            mem._evictions = int(counters.get("evictions", 0))
            mem._started_at = float(data.get("started_at", time.time()))
            mem._last_saved_at = data.get("last_saved_at")
            mem._last_loaded_at = data.get("last_loaded_at")
        return mem

    def to_json(
        self, *, include_table: bool = True, **kwargs: Any
    ) -> str:
        return json.dumps(
            self.to_dict(include_table=include_table),
            default=str,
            **kwargs,
        )

    @classmethod
    def from_json(
        cls, payload: str, *, path_override: Optional[str] = None
    ) -> "QMemory":
        try:
            return cls.from_dict(
                json.loads(payload), path_override=path_override
            )
        except json.JSONDecodeError as exc:
            raise QMemoryError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "QMemory":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None and self.has_unsaved_changes:
            try:
                self.save()
            except QMemoryError:
                logger.exception("Failed to save QMemory on context exit.")
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
            return (
                "QMemory("
                f"path={self.path!r}, "
                f"states={len(self.q_table)}, "
                f"entries={sum(len(r) for r in self.q_table.values())}, "
                f"alpha={self._config.alpha:.3f}, "
                f"updates={self._updates}, "
                f"unsaved={self._since_last_save > 0}, "
                f"strict={self._config.strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "QMemory",
    "QMemoryConfig",
    "QMemoryError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1.q_memory
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import tempfile

    logging.basicConfig(level=logging.INFO)

    with tempfile.TemporaryDirectory() as td:
        nested_path = Path(td) / "nested" / "dir" / "q_table.json"

        # ---- Happy path ------------------------------------------------ #
        mem = QMemory(
            path=str(nested_path),
            config=QMemoryConfig(save_interval=0),  # manual saves only
        )
        print("repr       :", mem)

        # Update a few (state, action) pairs.
        mem.update("green", "energy_saver", reward=1.0)
        mem.update("green", "energy_saver", reward=0.5)
        mem.update("green", "latency_optimized", reward=-0.3)
        mem.update("red", "energy_saver", reward=0.9)

        print("state cnt  :", mem.state_count)
        print("entry cnt  :", mem.entry_count)
        print("bias green/energy_saver:",
              round(mem.get_bias("green", "energy_saver"), 4))
        print("bias green/latency_opt:",
              round(mem.get_bias("green", "latency_optimized"), 4))
        print("best green :", mem.best_action("green"))
        print("all green  :", mem.state_biases("green"))

        # ---- Atomic save creates parent dirs --------------------------- #
        saved = mem.save()
        print("save       :", saved)
        assert nested_path.exists(), "atomic save should create parents"

        # ---- Reload from disk ----------------------------------------- #
        restored = QMemory(path=str(nested_path), config=QMemoryConfig(
            save_interval=0, auto_load=True,
        ))
        print("reloaded   :", restored.state_count, "state(s)")
        assert restored.get_bias("green", "energy_saver") == mem.get_bias(
            "green", "energy_saver"
        )

        # ---- Corrupt file handling ------------------------------------- #
        nested_path.write_text("{ not valid json")
        try:
            QMemory(path=str(nested_path), strict=True)
        except QMemoryError as exc:
            print("Rejected   :", exc)

        lenient = QMemory(
            path=str(nested_path), strict=False,
        )
        print("lenient    :", lenient.state_count, "state(s) (corrupt → 0)")

        # ---- save_interval batching ------------------------------------ #
        batched = QMemory(
            path=str(Path(td) / "batched.json"),
            config=QMemoryConfig(save_interval=5),
        )
        for i in range(20):
            batched.update(f"s{i}", "balanced", reward=1.0)
        # With save_interval=5, ~4 saves should have happened.
        print(f"batched    : updates={batched._updates} "
              f"unsaved={batched.has_unsaved_changes}")
        batched.save()
        assert not batched.has_unsaved_changes

        # ---- Bounded memory (LRU eviction) ----------------------------- #
        bounded = QMemory(
            path=str(Path(td) / "bounded.json"),
            config=QMemoryConfig(max_entries=3, save_interval=0),
        )
        for i in range(10):
            bounded.update(f"state_{i}", "balanced", reward=1.0)
        print(f"bounded    : states={bounded.state_count} "
              f"evictions={bounded._evictions}")
        assert bounded.state_count <= 3

        # ---- Per-state action cap ------------------------------------- #
        capped = QMemory(
            path=str(Path(td) / "capped.json"),
            config=QMemoryConfig(
                max_actions_per_state=2, save_interval=0,
            ),
        )
        for a in ("a", "b", "c", "d", "e"):
            capped.update("s", a, reward=1.0)
        print("capped     :", capped.state_biases("s"))

        # ---- Statistics ------------------------------------------------ #
        print("stats      :", {
            k: v for k, v in mem.statistics().items()
            if k not in ("uptime_seconds", "last_saved_at", "last_loaded_at")
        })

        # ---- Serialization round-trip ---------------------------------- #
        payload = mem.to_json()
        rt = QMemory.from_json(payload, path_override=str(Path(td) / "rt.json"))
        assert rt.to_dict() == mem.to_dict()
        print("Round-trip OK.")

        # ---- Context manager auto-save --------------------------------- #
        ctx_path = Path(td) / "ctx.json"
        with QMemory(path=str(ctx_path), config=QMemoryConfig(save_interval=0)) as ctx_mem:
            ctx_mem.update("x", "y", reward=1.0)
            assert ctx_mem.has_unsaved_changes
        assert ctx_path.exists(), "context exit should auto-save"
        print("Context save: OK")

        # ---- Reset ------------------------------------------------------ #
        removed = mem.reset(delete_file=True)
        print(f"reset      : removed {removed} state(s); "
              f"file exists={nested_path.exists()}")

        # ---- Validation failures --------------------------------------- #
        for bad_cfg in (
            dict(path=""),
            dict(alpha=0.0),
            dict(alpha=2.0),
            dict(max_entries=0),
            dict(max_actions_per_state=0),
            dict(save_interval=-1),
        ):
            try:
                QMemoryConfig(**bad_cfg)  # type: ignore[arg-type]
            except QMemoryError as exc:
                print("Rejected cfg:", exc)

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
            lambda: strict.update("s", "a", reward=1.0, alpha=float("nan")),
        ):
            try:
                bad_call()
            except QMemoryError as exc:
                print("Rejected   :", exc)

        # ---- Non-strict coerces / skips -------------------------------- #
        lenient2 = QMemory(
            path=str(Path(td) / "lenient2.json"),
            config=QMemoryConfig(save_interval=0, strict=False),
        )
        result = lenient2.update("", "a", reward=1.0)
        assert result is None
        print("lenient    : skipped invalid input")

        # ---- Default alpha from config --------------------------------- #
        defaulted = QMemory(
            path=str(Path(td) / "defaulted.json"),
            config=QMemoryConfig(alpha=0.5, save_interval=0, auto_load=False),
        )
        v = defaulted.update("s", "a", reward=1.0)  # new value: 0.5
        print("default α  :", v)
        assert abs(v - 0.5) < 1e-9

        # ---- Explicit alpha override ----------------------------------- #
        v = defaulted.update("s", "a", reward=1.0, alpha=1.0)  # → 1.0
        print("α=1.0      :", v)
        assert abs(v - 1.0) < 1e-9

        # ---- Persistence failure in non-strict mode -------------------- #
        class _UnwritablePath:
            # A path that cannot be opened: use /proc on Linux, or a dir.
            pass

        # Use a path whose parent is a file to force failure.
        blocker = Path(td) / "blocker"
        blocker.write_text("not a directory")
        bad_path = blocker / "q_table.json"
        failing = QMemory(
            path=str(bad_path),
            config=QMemoryConfig(save_interval=0, strict=False, auto_load=False),
        )
        print("save fail  :", failing.save(), "(expected False)")

    print("\nSmoke test passed.")
