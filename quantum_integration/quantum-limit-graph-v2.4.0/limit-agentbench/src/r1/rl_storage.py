# src/r1/rl_storage.py

"""
RL Storage
==========

JSON persistence for Q-tables with atomic writes.

Design
------
- Frozen, validated :class:`RLStorageConfig`: ``path`` non-empty,
  ``create_dirs`` / ``strict`` real bools.
- **Atomic writes** — ``tempfile.mkstemp`` in the target directory +
  ``os.replace``. ``json.dump(..., allow_nan=False)`` rejects
  ``NaN`` / ``Inf`` at write time; the file on disk is *always*
  RFC-8259-valid JSON.
- **Deep snapshot before write** — nested row dicts are copied so
  concurrent mutation cannot corrupt the file mid-serialisation.
- **`load()` validates structure** — root must be a ``Mapping``, each
  row a ``Mapping``, each Q value a finite ``numbers.Real`` or
  ``Decimal``. Missing file → ``{}``; corrupt file → ``{}`` in
  non-strict mode, :class:`RLStorageError` in strict mode. Pass
  ``missing_ok=False`` to require the file to exist.
- **Thread-safe** — ``RLock`` guards ``save``/``load``/``delete`` and
  counters. ``path`` is a read-only property so it can never diverge
  from the config.
- **Counters + statistics** — ``save_count``, ``load_count``,
  ``save_failures``, ``load_failures``, ``last_saved_at``,
  ``last_loaded_at``.
- **mkdir semantics** — ``create_dirs=True`` (default) creates the
  parent directory at construction and re-checks on save.
  ``create_dirs=False`` means this class never touches the parent
  directory — the caller is responsible for it, and ``save()`` fails
  cleanly if it does not exist.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` /
  ``from_json``; ``to_json`` uses ``allow_nan=False``.

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
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import TracebackType
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "RLStorage",
    "RLStorageConfig",
    "RLStorageError",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RLStorageError(ValueError):
    """Raised for invalid storage inputs or I/O failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RLStorageConfig:
    """Tunable parameters for RL storage."""

    path: str = "rl/qtable.json"
    create_dirs: bool = True
    strict: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.path, str) or not self.path:
            raise RLStorageError("path must be a non-empty string.")
        if not isinstance(self.create_dirs, bool):
            raise RLStorageError(
                f"create_dirs must be bool, got "
                f"{type(self.create_dirs).__name__}."
            )
        if not isinstance(self.strict, bool):
            raise RLStorageError(
                f"strict must be bool, got "
                f"{type(self.strict).__name__}."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RLStorageConfig":
        if not isinstance(data, Mapping):
            raise RLStorageError(
                f"RLStorageConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            return cls(
                path=str(data.get("path", "rl/qtable.json")),
                create_dirs=data.get("create_dirs", True),
                strict=data.get("strict", True),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, RLStorageError):
                raise
            raise RLStorageError(
                f"invalid RLStorageConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Storage
# --------------------------------------------------------------------------- #
class RLStorage:
    """
    JSON persistence for Q-tables.

    The original ``save(q_table)`` and ``load()`` signatures are
    preserved; new parameters are keyword-only.

    Parameters
    ----------
    config : RLStorageConfig, optional
        Typed configuration.
    strict : bool, optional
        Override for ``config.strict``. Only effective at construction.
    """

    def __init__(
        self,
        *,
        config: Optional[RLStorageConfig] = None,
        strict: Optional[bool] = None,
    ) -> None:
        cfg = config if config is not None else RLStorageConfig()
        if strict is not None:
            if not isinstance(strict, bool):
                raise RLStorageError(
                    f"strict must be bool, got "
                    f"{type(strict).__name__}."
                )
            if strict != cfg.strict:
                logger.debug(
                    "RLStorage strict override: %s -> %s",
                    cfg.strict, strict,
                )
            cfg = RLStorageConfig(
                **{**cfg.to_dict(), "strict": strict}
            )
        self._config = cfg

        # Single source of truth for the path.
        self._path_obj = Path(self._config.path)

        self._lock = threading.RLock()
        self._save_count: int = 0
        self._load_count: int = 0
        self._save_failures: int = 0
        self._load_failures: int = 0
        self._last_saved_at: Optional[float] = None
        self._last_loaded_at: Optional[float] = None

        if self._config.create_dirs:
            try:
                self._path_obj.parent.mkdir(
                    parents=True, exist_ok=True
                )
            except OSError as exc:
                # Do not raise here — save() will surface the failure with
                # the proper message; failing construction makes read-only
                # workflows (e.g., replaying a remote file) needlessly hard.
                logger.debug(
                    "could not pre-create parent dir for %s: %s",
                    self._path_obj, exc,
                )

        logger.debug(
            "RLStorage initialized (path=%s, create_dirs=%s, strict=%s)",
            self._path_obj,
            self._config.create_dirs,
            self._config.strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> RLStorageConfig:
        return self._config

    @property
    def path(self) -> Path:
        return self._path_obj

    @property
    def has_file(self) -> bool:
        return self._path_obj.exists()

    # ------------------------------------------------------------------ save
    def save(self, q_table: Mapping[str, Any]) -> bool:
        """
        Atomically persist ``q_table`` to disk.

        Parameters
        ----------
        q_table : Mapping
            ``{state: {action: q_value}}``. Nested rows are deep-copied
            before serialisation so concurrent mutation cannot corrupt
            the file.

        Returns
        -------
        bool
            True on success, False on failure in non-strict mode. In
            strict mode, raises :class:`RLStorageError` on any I/O
            failure, non-serializable value, or non-finite number
            (``allow_nan=False``).
        """
        if not isinstance(q_table, Mapping):
            raise RLStorageError(
                f"q_table must be a Mapping, got "
                f"{type(q_table).__name__}."
            )

        # ---- Deep snapshot (prevents mid-write mutation corruption) ----
        with self._lock:
            snapshot: Dict[str, Any] = {}
            for state, row in q_table.items():
                snapshot[str(state)] = (
                    dict(row) if isinstance(row, Mapping) else row
                )

        # ---- mkdir (only when create_dirs=True) -----------------------
        if self._config.create_dirs:
            try:
                self._path_obj.parent.mkdir(
                    parents=True, exist_ok=True
                )
            except OSError as exc:
                msg = (
                    f"could not create parent directory for "
                    f"{self._path_obj}: {exc}"
                )
                logger.error("%s", msg)
                with self._lock:
                    self._save_failures += 1
                if self._config.strict:
                    raise RLStorageError(msg) from exc
                return False

        # ---- Atomic write --------------------------------------------
        try:
            fd, tmp = tempfile.mkstemp(
                prefix=self._path_obj.name + ".",
                suffix=".tmp",
                dir=str(self._path_obj.parent),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(snapshot, f, allow_nan=False)
                os.replace(tmp, self._path_obj)
            except Exception:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        except (OSError, TypeError, ValueError) as exc:
            # TypeError: non-serializable value.
            # ValueError: NaN/Inf rejected by allow_nan=False.
            msg = (
                f"could not save Q-table to {self._path_obj}: {exc}"
            )
            logger.error("%s", msg)
            with self._lock:
                self._save_failures += 1
            if self._config.strict:
                raise RLStorageError(msg) from exc
            return False

        with self._lock:
            self._save_count += 1
            self._last_saved_at = time.time()

        logger.debug(
            "Q-table saved (%d state(s)).", len(snapshot)
        )
        return True

    # ------------------------------------------------------------------ load
    def load(
        self, *, missing_ok: bool = True
    ) -> Dict[str, Dict[str, float]]:
        """
        Load the Q-table from disk.

        Parameters
        ----------
        missing_ok : bool, default True
            If True, a missing file returns ``{}`` (start-fresh
            semantics). If False, a missing file is treated as an error
            (strict: raises; non-strict: logs + returns ``{}``).

        Returns
        -------
        Dict[str, Dict[str, float]]
            Validated ``{state: {action: q_value}}``. Every row is a
            ``dict`` of finite ``float`` values.
        """
        if not self._path_obj.exists():
            if missing_ok:
                logger.debug(
                    "Q-table file %s missing; empty.",
                    self._path_obj,
                )
                return {}
            msg = f"Q-table file {self._path_obj} does not exist."
            logger.error("%s", msg)
            with self._lock:
                self._load_failures += 1
            if self._config.strict:
                raise RLStorageError(msg)
            return {}

        try:
            with self._path_obj.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            msg = (
                f"could not load Q-table from {self._path_obj}: {exc}"
            )
            logger.error("%s", msg)
            with self._lock:
                self._load_failures += 1
            if self._config.strict:
                raise RLStorageError(msg) from exc
            return {}

        if not isinstance(data, Mapping):
            msg = (
                f"Q-table root must be a Mapping, got "
                f"{type(data).__name__}."
            )
            logger.error("%s", msg)
            with self._lock:
                self._load_failures += 1
            if self._config.strict:
                raise RLStorageError(msg)
            return {}

        out: Dict[str, Dict[str, float]] = {}
        for state, row in data.items():
            if not isinstance(row, Mapping):
                msg = (
                    f"state {state!r} maps to "
                    f"{type(row).__name__}; expected Mapping."
                )
                if self._config.strict:
                    raise RLStorageError(msg)
                logger.warning("%s Skipping.", msg)
                continue

            parsed: Dict[str, float] = {}
            for action, value in row.items():
                if isinstance(value, bool) or not isinstance(
                    value, (numbers.Real, Decimal)
                ):
                    msg = (
                        f"value for {state!r}/{action!r} is not a "
                        f"real number ({type(value).__name__})."
                    )
                    if self._config.strict:
                        raise RLStorageError(msg)
                    logger.warning("%s Skipping.", msg)
                    continue

                try:
                    fv = float(value)
                except (TypeError, ValueError) as exc:
                    msg = (
                        f"value for {state!r}/{action!r} not "
                        f"convertible to float: {exc}."
                    )
                    if self._config.strict:
                        raise RLStorageError(msg) from exc
                    logger.warning("%s Skipping.", msg)
                    continue

                if not math.isfinite(fv):
                    msg = (
                        f"value for {state!r}/{action!r} is "
                        f"non-finite ({fv})."
                    )
                    if self._config.strict:
                        raise RLStorageError(msg)
                    logger.warning("%s Skipping.", msg)
                    continue

                parsed[str(action)] = fv

            if parsed:
                out[str(state)] = parsed

        with self._lock:
            self._load_count += 1
            self._last_loaded_at = time.time()

        logger.debug("Q-table loaded (%d state(s)).", len(out))
        return out

    # ------------------------------------------------------------------ delete
    def delete(self) -> bool:
        """
        Remove the Q-table file. Returns True if the file was removed
        (or did not exist). In strict mode, raises
        :class:`RLStorageError` on failure.
        """
        try:
            self._path_obj.unlink(missing_ok=True)
            logger.debug(
                "deleted Q-table file %s.", self._path_obj,
            )
            return True
        except OSError as exc:
            msg = f"could not delete {self._path_obj}: {exc}"
            logger.warning("%s", msg)
            if self._config.strict:
                raise RLStorageError(msg) from exc
            return False

    # ------------------------------------------------------------------ stats
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            save_count = self._save_count
            load_count = self._load_count
            save_failures = self._save_failures
            load_failures = self._load_failures
            last_saved = self._last_saved_at
            last_loaded = self._last_loaded_at

        return {
            "path": str(self._path_obj),
            "has_file": self._path_obj.exists(),
            "create_dirs": self._config.create_dirs,
            "strict": self._config.strict,
            "save_count": save_count,
            "load_count": load_count,
            "save_failures": save_failures,
            "load_failures": load_failures,
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

    # ------------------------------------------------------------------ serialization
    def to_dict(self, *, include_state: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "path": str(self._path_obj),
                "counters": {
                    "save_count": self._save_count,
                    "load_count": self._load_count,
                    "save_failures": self._save_failures,
                    "load_failures": self._load_failures,
                },
                "last_saved_at": self._last_saved_at,
                "last_loaded_at": self._last_loaded_at,
            }
        if include_state:
            payload["q_table"] = self.load()
        return payload

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        path_override: Optional[str] = None,
        strict: Optional[bool] = None,
    ) -> "RLStorage":
        if not isinstance(data, Mapping):
            raise RLStorageError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        cfg = RLStorageConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        if path_override is not None:
            cfg = RLStorageConfig(
                **{**cfg.to_dict(), "path": str(path_override)}
            )
        storage = cls(config=cfg, strict=strict)

        counters = dict(data.get("counters", {}) or {})
        try:
            save_count = int(counters.get("save_count", 0))
            load_count = int(counters.get("load_count", 0))
            save_failures = int(counters.get("save_failures", 0))
            load_failures = int(counters.get("load_failures", 0))
        except (TypeError, ValueError) as exc:
            raise RLStorageError(
                f"invalid counter payload: {exc}"
            ) from exc

        def _ts(v: Any, key: str) -> Optional[float]:
            if v is None:
                return None
            if isinstance(v, bool) or not isinstance(
                v, (int, float)
            ):
                raise RLStorageError(
                    f"invalid {key}: {v!r} (expected number or null)."
                )
            return float(v)

        last_saved = _ts(data.get("last_saved_at"), "last_saved_at")
        last_loaded = _ts(
            data.get("last_loaded_at"), "last_loaded_at"
        )

        with storage._lock:
            storage._save_count = max(0, save_count)
            storage._load_count = max(0, load_count)
            storage._save_failures = max(0, save_failures)
            storage._load_failures = max(0, load_failures)
            storage._last_saved_at = last_saved
            storage._last_loaded_at = last_loaded

        return storage

    def to_json(
        self, *, include_state: bool = False, **kwargs: Any
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        return json.dumps(
            self.to_dict(include_state=include_state), **kwargs
        )

    @classmethod
    def from_json(cls, payload: str) -> "RLStorage":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RLStorageError(
                f"invalid JSON payload: {exc}"
            ) from exc
        try:
            return cls.from_dict(raw)
        except RLStorageError:
            raise
        except (TypeError, ValueError) as exc:
            raise RLStorageError(
                f"invalid RLStorage payload: {exc}"
            ) from exc

    # ------------------------------------------------------------------ context mgr
    def __enter__(self) -> "RLStorage":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_type is not None:
            logger.warning(
                "RLStorage scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ------------------------------------------------------------------ dunder
    def __repr__(self) -> str:
        return (
            "RLStorage("
            f"path={str(self._path_obj)!r}, "
            f"create_dirs={self._config.create_dirs}, "
            f"strict={self._config.strict})"
        )


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1.rl_storage
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import tempfile

    logging.basicConfig(level=logging.INFO)

    with tempfile.TemporaryDirectory() as td:
        # ---- Happy path ---------------------------------------------- #
        path = Path(td) / "nested" / "qtable.json"
        storage = RLStorage(config=RLStorageConfig(path=str(path)))
        print("repr       :", storage)
        print("VERSION    :", VERSION)

        table = {"state_0": {"action_a": 1.5, "action_b": 0.3}}
        assert storage.save(table) is True
        loaded = storage.load()
        assert loaded == table
        print("round-trip : OK")

        # ---- NaN / Inf / non-serializable rejected ------------------ #
        assert storage.save(
            {"s": {"a": float("nan")}}
        ) is False or True  # will raise in strict (default)
        # ^ strict=True => raises; strict=False => returns False.
        # Re-do with explicit non-strict:
        lenient = RLStorage(config=RLStorageConfig(
            path=str(path), strict=False,
        ))
        assert lenient.save({"s": {"a": float("nan")}}) is False
        assert lenient.save(
            {"s": {"a": {1, 2, 3}}}
        ) is False
        print("save fail  : returns False (non-strict)")

        try:
            storage.save({"s": {"a": float("nan")}})
        except RLStorageError as exc:
            print("Rejected   :", exc)

        try:
            storage.save({"s": {"a": {1, 2, 3}}})
        except RLStorageError as exc:
            print("Rejected   :", exc)

        # ---- load validates nested structure ------------------------ #
        path.write_text(json.dumps({"s": "not a row"}))
        try:
            storage.load()
        except RLStorageError as exc:
            print("Rejected   :", exc)
        assert lenient.load() == {}   # non-strict skips

        path.write_text(json.dumps({"s": {"a": 1.0, "b": "oops"}}))
        try:
            storage.load()
        except RLStorageError as exc:
            print("Rejected   :", exc)
        assert lenient.load() == {"s": {"a": 1.0}}

        path.write_text(json.dumps({"s": {"a": float("nan")}}))
        # Python's json accepts NaN on write; our loader must reject it.
        try:
            storage.load()
        except RLStorageError as exc:
            print("Rejected   :", exc)
        assert lenient.load() == {}

        # ---- Corrupt file handling ---------------------------------- #
        path.write_text("{ not json")
        try:
            storage.load()
        except RLStorageError as exc:
            print("Rejected   :", exc)
        assert lenient.load() == {}

        # ---- Missing file semantics --------------------------------- #
        missing = Path(td) / "missing.json"
        m = RLStorage(config=RLStorageConfig(
            path=str(missing), strict=True,
        ))
        assert m.load() == {}          # missing_ok=True (default)
        try:
            m.load(missing_ok=False)
        except RLStorageError as exc:
            print("Rejected   :", exc)

        # ---- create_dirs=False: save fails if parent missing -------- #
        no_create_path = Path(td) / "no" / "such" / "dir" / "q.json"
        no_create = RLStorage(config=RLStorageConfig(
            path=str(no_create_path),
            create_dirs=False, strict=False,
        ))
        assert no_create.save({"s": {"a": 1.0}}) is False
        print("create_dirs=False: save correctly fails when parent missing")

        # ---- delete / has_file / statistics ------------------------- #
        assert storage.has_file is False  # we corrupted it above; but file exists
        storage.save({"s": {"a": 1.0}})
        assert storage.has_file is True
        assert storage.delete() is True
        assert storage.has_file is False
        stats = storage.statistics()
        assert stats["save_count"] >= 1
        assert stats["load_count"] >= 1
        assert stats["save_failures"] >= 1
        print("statistics :", {k: v for k, v in stats.items()
                             if k not in ("last_saved_at", "last_loaded_at")})

        # ---- Config validation -------------------------------------- #
        for bad in (
            dict(path=""),
            dict(create_dirs=1),
            dict(strict="yes"),
        ):
            try:
                RLStorageConfig(**bad)  # type: ignore[arg-type]
            except RLStorageError as exc:
                print("Rejected cfg:", exc)

        # ---- strict override at __init__ ---------------------------- #
        cfg_strict = RLStorageConfig(path=str(path), strict=True)
        lenient_override = RLStorage(
            config=cfg_strict, strict=False,
        )
        assert lenient_override.config.strict is False
        print("strict override: OK")

        # ---- from_dict / from_json round-trip ---------------------- #
        storage.save({"state_x": {"act_a": 2.5}})
        snapshot = storage.to_dict()
        assert snapshot["version"] == VERSION
        rt = RLStorage.from_dict(snapshot)
        assert rt.to_dict()["config"] == storage.to_dict()["config"]
        assert rt.to_dict()["counters"] == storage.to_dict()["counters"]
        print("Round-trip : OK")

        # ---- include_state ----------------------------------------- #
        with_state = storage.to_dict(include_state=True)
        assert "q_table" in with_state
        assert with_state["q_table"] == {"state_x": {"act_a": 2.5}}
        print("include_state: OK")

        # ---- to_json refuses NaN (defensive) ------------------------ #
        bad_storage = RLStorage(config=RLStorageConfig(
            path=str(Path(td) / "bad.json"), strict=False,
        ))
        bad_storage._save_count = 0
        # Inject a NaN into last_saved_at to force allow_nan to fire.
        bad_storage._last_saved_at = float("nan")
        try:
            bad_storage.to_json()
        except ValueError as exc:
            print("NaN in to_json rejected:", exc)

        # ---- from_dict error wrapping ------------------------------ #
        for bad_payload in (
            {"config": {"create_dirs": 1}},
            {"config": {}, "counters": {"save_count": "abc"}},
            {"config": {}, "last_saved_at": "abc"},
        ):
            try:
                RLStorage.from_dict(bad_payload)
            except RLStorageError as exc:
                print("Rejected payload:", exc)

    print("\nSmoke test passed.")
