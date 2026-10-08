# src/quantum_integration/digital_twin/digital_twin_persistence.py

"""Atomic gzip-compressed persistence for the digital twin."""

from __future__ import annotations

import json
import logging
import os
import tempfile
import zlib
from pathlib import Path
from typing import Any, List, Optional

from .digital_twin_errors import (
    DigitalTwinParseError,
    DigitalTwinPersistenceError,
)
from .digital_twin_helpers import _LazyLock, _iso_now

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 1


class DigitalTwinPersistenceManager:
    """Persists a twin's state as atomically-written gzip-compressed JSON."""

    def __init__(
        self,
        path: str,
        *,
        atomic_writes: bool = True,
    ) -> None:
        if not isinstance(path, str) or not path:
            raise DigitalTwinPersistenceError(
                "path must be a non-empty string."
            )
        self.path = path
        self.atomic_writes = bool(atomic_writes)
        self._lock = _LazyLock()

    # ------------------------------------------------------------------ #
    def _extract_q_weights(self, twin: Any) -> Optional[List[float]]:
        """Extract Q-teacher weights by attribute lookup, not by index."""
        distill = getattr(twin, "distillation", None)
        if distill is None:
            return None
        q_teacher = getattr(distill, "q_teacher", None)
        if q_teacher is None:
            return None
        weights = getattr(q_teacher, "weights", None)
        if weights is None:
            return None
        try:
            return weights.tolist()
        except AttributeError:
            return None

    def _atomic_write(self, path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not self.atomic_writes:
            path.write_bytes(payload)
            return
        fd, tmp_path = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp", dir=str(path.parent),
        )
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(payload)
            os.replace(tmp_path, path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    # ------------------------------------------------------------------ #
    async def save_state(self, twin: Any, *, flush: bool = True) -> bool:
        async with self._lock:
            try:
                state = {
                    "schema_version": SCHEMA_VERSION,
                    "twin_version": getattr(twin, "__version__", None),
                    "config": (
                        twin.config.to_dict()
                        if hasattr(twin.config, "to_dict")
                        else dict(twin.config.__dict__)
                    ),
                    "scenario_results": [
                        r.to_dict() if hasattr(r, "to_dict") else r.__dict__
                        for r in getattr(twin, "scenario_results", [])
                    ],
                    "resource_projections": {
                        k: (
                            v.to_dict() if hasattr(v, "to_dict") else v.__dict__
                        )
                        for k, v in getattr(twin, "resource_projections", {}).items()
                    },
                    "priority_weights": dict(getattr(twin, "priority_weights", {})),
                    "resource_correlation": (
                        {k: dict(v) for k, v in twin.resource_correlation.items()}
                        if hasattr(twin, "resource_correlation") else {}
                    ),
                    "substitution_options": (
                        {k: list(v) for k, v in twin.substitution_options.items()}
                        if hasattr(twin, "substitution_options") else {}
                    ),
                    "last_save": _iso_now(),
                    "q_teacher_weights": self._extract_q_weights(twin),
                }
                json_str = json.dumps(state, indent=2, default=str)
                compressed = zlib.compress(json_str.encode("utf-8"))
                path = Path(self.path)
                self._atomic_write(path, compressed)
                return True
            except Exception as exc:  # noqa: BLE001 - defensive
                logger.error("Failed to save state: %s", exc)
                if getattr(twin, "strict", False):
                    raise DigitalTwinPersistenceError(str(exc)) from exc
                return False

    async def load_state(self, twin: Any) -> bool:
        async with self._lock:
            path = Path(self.path)
            if not path.exists():
                return False
            try:
                compressed = path.read_bytes()
                json_str = zlib.decompress(compressed).decode("utf-8")
                state = json.loads(json_str)
                self._assert_compatible(state, strict=getattr(twin, "strict", False))
                await self._deserialize_into(twin, state)
                return True
            except DigitalTwinParseError:
                raise
            except Exception as exc:  # noqa: BLE001 - defensive
                logger.error("Failed to load state: %s", exc)
                if getattr(twin, "strict", False):
                    raise DigitalTwinPersistenceError(str(exc)) from exc
                return False

    @staticmethod
    def _assert_compatible(data: Any, *, strict: bool) -> None:
        if not isinstance(data, dict):
            raise DigitalTwinParseError(
                "State root must be a Mapping."
            )
        v = data.get("schema_version", SCHEMA_VERSION)
        if not isinstance(v, int) or isinstance(v, bool) or v <= 0:
            raise DigitalTwinParseError(
                f"invalid schema_version {v!r}."
            )
        if v > SCHEMA_VERSION:
            raise DigitalTwinParseError(
                f"state schema_version {v} is newer than {SCHEMA_VERSION}."
            )
        if strict and v < SCHEMA_VERSION:
            raise DigitalTwinParseError(
                f"state schema_version {v} is older than {SCHEMA_VERSION}."
            )

    async def _deserialize_into(self, twin: Any, state: dict) -> None:
        from .digital_twin_schemas import (
            DigitalTwinResult,
            ResourceProjection,
        )
        # Priority weights / correlation / substitution options.
        twin.priority_weights = dict(
            state.get("priority_weights") or twin.priority_weights
        )
        twin.resource_correlation = {
            k: dict(v) for k, v in (
                state.get("resource_correlation") or twin.resource_correlation
            ).items()
        }
        twin.substitution_options = {
            k: list(v) for k, v in (
                state.get("substitution_options") or twin.substitution_options
            ).items()
        }
        # Scenario results.
        twin.scenario_results = [
            DigitalTwinResult.from_dict(d)
            for d in state.get("scenario_results", [])
        ]
        # Resource projections.
        twin.resource_projections = {
            k: ResourceProjection.from_dict(v)
            for k, v in (state.get("resource_projections") or {}).items()
        }
        # Q-teacher weights.
        q_weights = state.get("q_teacher_weights")
        if q_weights is not None:
            distill = getattr(twin, "distillation", None)
            q_teacher = getattr(distill, "q_teacher", None) if distill else None
            if q_teacher is not None and hasattr(q_teacher, "weights"):
                import numpy as np
                q_teacher.weights = np.array(q_weights)

    # ------------------------------------------------------------------ #
    async def delete_state(self) -> bool:
        async with self._lock:
            path = Path(self.path)
            if not path.exists():
                return False
            try:
                path.unlink()
                return True
            except OSError as exc:
                logger.error("Failed to delete state: %s", exc)
                return False

    def statistics(self) -> dict:
        path = Path(self.path)
        return {
            "schema_version": SCHEMA_VERSION,
            "path": self.path,
            "exists": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() else 0,
            "atomic_writes": self.atomic_writes,
        }

    def close(self, *, flush: bool = True) -> None:
        return None


__all__ = ["SCHEMA_VERSION", "DigitalTwinPersistenceManager"]
