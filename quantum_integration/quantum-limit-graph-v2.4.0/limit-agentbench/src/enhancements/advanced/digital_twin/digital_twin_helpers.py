# src/quantum_integration/digital_twin/digital_twin_helpers.py

"""Shared validation, freezing and lazy-lock helpers."""

from __future__ import annotations

import asyncio
import math
import threading
from collections.abc import Mapping as ABCMapping
from datetime import datetime, timezone
from typing import Any, Optional, Sequence


def _is_real_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_nonneg(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _is_positive_finite(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z") or s.endswith("z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    k = max(
        0,
        min(
            len(ordered) - 1,
            int(round((pct / 100.0) * (len(ordered) - 1))),
        ),
    )
    return ordered[k]


def _deep_freeze(value: Any, *, depth: int = 0) -> Any:
    """Recursively freeze mappings / sequences into read-only structures."""
    if depth > 32:
        return value
    if isinstance(value, ABCMapping):
        from types import MappingProxyType
        return MappingProxyType({
            str(k): _deep_freeze(v, depth=depth + 1)
            for k, v in value.items()
        })
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(v, depth=depth + 1) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_deep_freeze(v, depth=depth + 1) for v in value)
    return value


def _hashable(value: Any, *, depth: int = 0) -> Any:
    """Convert nested mappings / sequences to hashable tuples."""
    if depth > 32:
        return "<truncated>"
    if isinstance(value, ABCMapping):
        return tuple(sorted(
            (str(k), _hashable(v, depth=depth + 1))
            for k, v in value.items()
        ))
    if isinstance(value, (list, tuple)):
        return tuple(_hashable(v, depth=depth + 1) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_hashable(v, depth=depth + 1) for v in value)
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    try:
        hash(value)
        return value
    except TypeError:
        return repr(value)


def _to_plain(value: Any, *, depth: int = 0) -> Any:
    """Convert frozen structures back to plain dicts / lists."""
    if depth > 32:
        return value
    if isinstance(value, ABCMapping):
        return {
            str(k): _to_plain(v, depth=depth + 1) for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_to_plain(v, depth=depth + 1) for v in value]
    if isinstance(value, (set, frozenset)):
        return [_to_plain(v, depth=depth + 1) for v in value]
    return value


class _LazyLock:
    """``asyncio.Lock`` that binds to the current event loop on first use.

    Fixes the loop-binding bug: ``asyncio.Lock()`` constructed outside a
    running loop binds to the wrong loop under Python 3.10+.
    """

    def __init__(self) -> None:
        self._lock: Optional[asyncio.Lock] = None
        self._guard = threading.Lock()

    def _get(self) -> asyncio.Lock:
        with self._guard:
            if self._lock is None:
                self._lock = asyncio.Lock()
            return self._lock

    async def __aenter__(self) -> "_LazyLock":
        await self._get().acquire()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self._get().release()


__all__ = [
    "_LazyLock",
    "_deep_freeze",
    "_hashable",
    "_is_finite_nonneg",
    "_is_positive_finite",
    "_is_real_int",
    "_iso_now",
    "_parse_iso_datetime",
    "_percentile",
    "_to_plain",
]
