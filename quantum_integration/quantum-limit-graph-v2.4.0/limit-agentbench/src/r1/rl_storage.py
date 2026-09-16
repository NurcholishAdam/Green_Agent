# src/r1/rl_storage.py

"""
RL Storage
==========

JSON persistence for Q-tables with atomic writes.

Enhancements
------------
- **Fixed missing directory** — parent dir created automatically.
- **Atomic writes** via ``tempfile`` + ``os.replace``.
- ``RLStorageConfig`` — frozen, validated: path, create-dirs, strict.
- Full error handling; serialization; ``__repr__``; ``__main__`` smoke test.
"""

from __future__ import annotations

import json, logging, os, tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)


class RLStorageError(ValueError):
    """Raised for invalid storage inputs or I/O failures."""


@dataclass(frozen=True)
class RLStorageConfig:
    """Tunable parameters for RL storage."""
    path: str = "rl/qtable.json"
    create_dirs: bool = True
    strict: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.path, str) or not self.path:
            raise RLStorageError("path must be a non-empty string.")


class RLStorage:
    """
    JSON persistence for Q-tables.

    The original ``save(q_table)`` and ``load()`` signatures are preserved;
    new parameters are keyword-only.
    """

    def __init__(
        self,
        *,
        config: Optional[RLStorageConfig] = None,
    ) -> None:
        self._config = config or RLStorageConfig()
        self.path = Path(self._config.path)
        if self._config.create_dirs:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug("RLStorage initialized (path=%s).", self.path)

    @property
    def config(self) -> RLStorageConfig:
        return self._config

    def save(self, q_table: Mapping[str, Any]) -> None:
        """Atomically persist ``q_table`` to disk."""
        if not isinstance(q_table, Mapping):
            raise RLStorageError(
                f"q_table must be a Mapping, got {type(q_table).__name__}."
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd, tmp = tempfile.mkstemp(
                prefix=self.path.name + ".", suffix=".tmp",
                dir=str(self.path.parent),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(dict(q_table), f)
                os.replace(tmp, self.path)
            except Exception:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        except OSError as exc:
            logger.error("Could not save Q-table to %s: %s", self.path, exc)
            if self._config.strict:
                raise RLStorageError(f"Could not save Q-table: {exc}") from exc

    def load(self) -> Dict[str, Any]:
        """Load the Q-table from disk (missing file → empty dict)."""
        if not self.path.exists():
            return {}
        try:
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Could not load Q-table from %s: %s", self.path, exc)
            if self._config.strict:
                raise RLStorageError(f"Could not load Q-table: {exc}") from exc
            return {}
        if not isinstance(data, dict):
            msg = f"Q-table root must be a dict, got {type(data).__name__}."
            if self._config.strict:
                raise RLStorageError(msg)
            logger.warning("%s Returning empty dict.", msg)
            return {}
        return data

    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": asdict(self._config),
            "path": str(self.path),
        }

    def __repr__(self) -> str:
        return f"RLStorage(path={str(self.path)!r}, strict={self._config.strict})"


if __name__ == "__main__":  # pragma: no cover
    import tempfile
    logging.basicConfig(level=logging.INFO)

    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "nested" / "qtable.json"
        storage = RLStorage(config=RLStorageConfig(path=str(path)))
        print("repr       :", storage)

        table = {"state_0": {"action_a": 1.5, "action_b": 0.3}}
        storage.save(table)
        print("loaded     :", storage.load())

        # Corrupt file handling.
        path.write_text("{ not json")
        try:
            storage.load()
        except RLStorageError as exc:
            print("Rejected   :", exc)

        lenient = RLStorage(
            config=RLStorageConfig(path=str(path), strict=False)
        )
        print("lenient    :", lenient.load())

    print("\nSmoke test passed.")
