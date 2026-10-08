# src/quantum_integration/digital_twin/digital_twin_helpers.py

"""Shared validation, freezing and lazy-lock helpers.

This module provides small, dependency-free utilities used across the
digital-twin package:

* Numeric predicates that correctly reject ``bool`` (``_is_real_int``,
  ``_is_finite_nonneg``, ``_is_positive_finite``).
* Time helpers (``_iso_now``, ``_parse_iso_datetime``) that normalize to
  timezone-aware UTC datetimes and accept a variety of input shapes.
* Structure helpers (``_deep_freeze``, ``_hashable``, ``_to_plain``) with a
  bounded recursion depth (:data:`_MAX_DEPTH`).
* ``_LazyLock`` — an ``asyncio.Lock`` that binds to the running loop on
  first use and rebuilds itself if the active loop changes.

All depth-bounded helpers raise ``RecursionError`` when
:data:`_MAX_DEPTH` is exceeded rather than silently returning a partially
frozen / unhashable value.
"""

from __future__ import annotations

import asyncio
import math
import threading
from collections.abc import Mapping as ABCMapping
from datetime import date, datetime, timezone
from typing import Any, Optional, Sequence, Tuple

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

#: Maximum recursion depth for ``_deep_freeze`` / ``_hashable`` /
#: ``_to_plain``. Structures nested more deeply raise ``RecursionError``.
_MAX_DEPTH: int = 32

# Epoch-magnitude thresholds used by ``_parse_iso_datetime``. Values are
# compared using ``abs(ts)`` so that pre-1970 timestamps behave the same
# as their post-1970 counterparts.
_EPOCH_MS: float = 1e11       # |ts| >= this -> milliseconds
_EPOCH_US: float = 1e14       # |ts| >= this -> microseconds
_EPOCH_NS: float = 1e17       # |ts| >= this -> nanoseconds
_EPOCH_LIMIT: float = 1e20    # |ts| >= this -> reject (unknown unit)


# --------------------------------------------------------------------------- #
# Numeric predicates
# --------------------------------------------------------------------------- #

def _is_real_int(value: Any) -> bool:
    """Return ``True`` iff ``value`` is an ``int`` but not a ``bool``.

    ``bool`` is a subclass of ``int`` in Python; this predicate exists to
    exclude it because configuration loaders often confuse ``True`` with
    ``1``.
    """
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_nonneg(value: Any) -> bool:
    """Return ``True`` iff ``value`` is a finite, non-negative real number.

    ``bool`` is rejected. ``NaN`` and ``±inf`` are rejected.
    """
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _is_positive_finite(value: Any) -> bool:
    """Return ``True`` iff ``value`` is a finite, strictly positive real.

    ``bool`` is rejected. Zero, ``NaN`` and ``±inf`` are rejected.
    """
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


# --------------------------------------------------------------------------- #
# Time helpers
# --------------------------------------------------------------------------- #

def _iso_now() -> str:
    """Return the current UTC time as an ISO 8601 string.

    The result always includes a timezone offset (``+00:00``) and is safe
    to feed back into :func:`_parse_iso_datetime`.
    """
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_datetime(
    value: Any,
    *,
    strict: bool = False,
) -> Optional[datetime]:
    """Parse a variety of datetime-like values into tz-aware UTC datetime.

    Accepted inputs:

    * ``None`` — returns ``None``.
    * ``datetime`` — naive values are assumed UTC.
    * ``date`` — treated as midnight UTC.
    * ``int`` / ``float`` epoch — the unit is inferred from ``abs(ts)``:
        * ``< 1e11``  -> seconds
        * ``< 1e14``  -> milliseconds
        * ``< 1e17``  -> microseconds
        * ``< 1e20``  -> nanoseconds
        * otherwise   -> rejected
    * ``str`` — ISO 8601; a trailing ``Z`` / ``z`` is normalized to
      ``+00:00`` before parsing.

    Parameters
    ----------
    value:
        The value to parse.
    strict:
        When ``True``, malformed inputs raise ``ValueError`` with a
        descriptive message instead of returning ``None``. Default
        ``False`` preserves the historical "best-effort" behavior.

    Returns
    -------
    Optional[datetime]
        A tz-aware ``datetime`` in UTC, or ``None`` on failure when
        ``strict=False``.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    # ``datetime`` is a subclass of ``date``; check it first (above).
    if isinstance(value, date):
        return datetime(
            value.year, value.month, value.day, tzinfo=timezone.utc,
        )

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = float(value)
        if not math.isfinite(ts):
            return _parse_fail(value, strict, "non-finite epoch")
        mag = abs(ts)
        if mag < _EPOCH_MS:
            seconds = ts
        elif mag < _EPOCH_US:
            seconds = ts / 1e3
        elif mag < _EPOCH_NS:
            seconds = ts / 1e6
        elif mag < _EPOCH_LIMIT:
            seconds = ts / 1e9
        else:
            return _parse_fail(value, strict, "epoch magnitude out of range")
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return _parse_fail(value, strict, "epoch out of representable range")

    if not isinstance(value, str):
        return _parse_fail(
            value, strict, f"unsupported type {type(value).__name__}",
        )

    s = value.strip()
    if not s:
        return _parse_fail(value, strict, "empty string")
    if s[-1] in ("Z", "z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return _parse_fail(value, strict, "invalid ISO 8601")
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _parse_fail(value: Any, strict: bool, reason: str) -> None:
    """Return ``None`` or raise ``ValueError`` depending on ``strict``."""
    if strict:
        raise ValueError(
            f"cannot parse {value!r} as datetime: {reason}"
        )
    return None


# --------------------------------------------------------------------------- #
# Percentile
# --------------------------------------------------------------------------- #

def _percentile(values: Sequence[float], pct: float) -> float:
    """Return the ``pct``-th percentile of ``values`` via linear interpolation.

    ``pct`` must be finite and within ``[0, 100]``; otherwise ``ValueError``
    is raised. Non-finite and non-numeric entries are ignored. An empty
    (or all-ignored) input returns ``0.0``.

    The interpolation follows the "linear" method: for a sorted sample of
    length ``n`` and ``rank = (pct/100) * (n - 1)``, the result is
    ``x[floor(rank)] * (1 - frac) + x[ceil(rank)] * frac``.
    """
    if not math.isfinite(pct) or not (0.0 <= pct <= 100.0):
        raise ValueError(
            f"pct must be finite and in [0, 100], got {pct!r}."
        )

    finite = sorted(
        v for v in values
        if isinstance(v, (int, float))
        and not isinstance(v, bool)
        and math.isfinite(v)
    )
    if not finite:
        return 0.0
    if len(finite) == 1:
        return float(finite[0])

    rank = (pct / 100.0) * (len(finite) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return float(finite[lo])
    frac = rank - lo
    return float(finite[lo] * (1.0 - frac) + finite[hi] * frac)


# --------------------------------------------------------------------------- #
# Structural helpers
# --------------------------------------------------------------------------- #

def _deep_freeze(value: Any, *, depth: int = 0) -> Any:
    """Recursively freeze mappings / sequences into read-only structures.

    * ``Mapping`` -> ``types.MappingProxyType`` (keys coerced to ``str``).
    * ``list`` / ``tuple`` -> ``tuple``.
    * ``set`` / ``frozenset`` -> ``frozenset``.
    * Scalars are returned unchanged.

    Raises
    ------
    RecursionError
        If the structure is nested more deeply than :data:`_MAX_DEPTH`.

    Notes
    -----
    Nested ``MappingProxyType`` values are not directly picklable on all
    Python versions; use :func:`_to_plain` before persisting.
    """
    if depth > _MAX_DEPTH:
        raise RecursionError(
            f"_deep_freeze exceeded _MAX_DEPTH={_MAX_DEPTH}; "
            f"structure is too deeply nested (possible cycle)."
        )
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
    """Convert nested mappings / sequences to deterministic hashable values.

    * ``Mapping`` -> sorted tuple of ``(str(k), hashable(v))`` pairs.
    * ``list`` / ``tuple`` -> tuple.
    * ``set`` / ``frozenset`` -> ``frozenset``.
    * Primitives are returned unchanged.
    * Objects with a ``__dict__`` fall back to a
      ``(qualname, _hashable(vars(obj)))`` tuple.
    * Otherwise a ``TypeError`` is raised — this function never produces
      address-dependent values (the earlier ``repr()`` fallback did).

    Raises
    ------
    RecursionError
        If the structure is nested more deeply than :data:`_MAX_DEPTH`.
    TypeError
        If a leaf value cannot be deterministically hashed.
    """
    if depth > _MAX_DEPTH:
        raise RecursionError(
            f"_hashable exceeded _MAX_DEPTH={_MAX_DEPTH}; "
            f"structure is too deeply nested (possible cycle)."
        )
    if isinstance(value, ABCMapping):
        return tuple(sorted(
            (str(k), _hashable(v, depth=depth + 1))
            for k, v in value.items()
        ))
    if isinstance(value, (list, tuple)):
        return tuple(_hashable(v, depth=depth + 1) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_hashable(v, depth=depth + 1) for v in value)
    if isinstance(value, (str, bytes, int, float, bool, type(None))):
        return value

    # Natively hashable objects (e.g. custom __hash__) are safe to use as-is.
    try:
        hash(value)
    except TypeError:
        pass
    else:
        return value

    # Structured fallback for objects with a __dict__ (dataclasses, etc.).
    try:
        attrs = vars(value)
    except TypeError:
        attrs = None
    if attrs is not None:
        return (
            type(value).__qualname__,
            _hashable(attrs, depth=depth + 1),
        )

    raise TypeError(
        f"_hashable: cannot deterministically hash "
        f"{type(value).__name__!r} value"
    )


def _to_plain(value: Any, *, depth: int = 0) -> Any:
    """Convert frozen structures back to plain ``dict`` / ``list`` values.

    * ``Mapping`` -> ``dict`` (keys coerced to ``str``).
    * ``list`` / ``tuple`` -> ``list``.
    * ``set`` / ``frozenset`` -> ``list`` (ordering is unspecified).

    Raises
    ------
    RecursionError
        If the structure is nested more deeply than :data:`_MAX_DEPTH`.
    """
    if depth > _MAX_DEPTH:
        raise RecursionError(
            f"_to_plain exceeded _MAX_DEPTH={_MAX_DEPTH}; "
            f"structure is too deeply nested (possible cycle)."
        )
    if isinstance(value, ABCMapping):
        return {
            str(k): _to_plain(v, depth=depth + 1)
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_to_plain(v, depth=depth + 1) for v in value]
    if isinstance(value, (set, frozenset)):
        return [_to_plain(v, depth=depth + 1) for v in value]
    return value


# --------------------------------------------------------------------------- #
# Lazy asyncio lock
# --------------------------------------------------------------------------- #

class _LazyLock:
    """``asyncio.Lock`` that binds to the current event loop on first use.

    Fixes the loop-binding bug: ``asyncio.Lock()`` constructed outside a
    running loop binds to the wrong loop under Python 3.10+.

    In addition, if the running loop changes between acquisitions
    (common in tests, REPLs, and worker threads that restart their loop
    between ``asyncio.run`` calls), the underlying lock is transparently
    rebuilt so it always belongs to the active loop.
    """

    def __init__(self) -> None:
        self._lock: Optional[asyncio.Lock] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._guard = threading.Lock()

    def _get(self) -> asyncio.Lock:
        """Return a lock bound to the currently running loop.

        Rebuilds the lock if the loop has changed since last use.
        """
        loop = asyncio.get_running_loop()
        with self._guard:
            if self._lock is None or self._loop is not loop:
                self._lock = asyncio.Lock()
                self._loop = loop
            return self._lock

    async def __aenter__(self) -> "_LazyLock":
        await self._get().acquire()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        # ``_get`` re-resolves the running loop; ``__aexit__`` always runs
        # on the same loop that entered the context, so the returned lock
        # is the one we acquired.
        self._get().release()


__all__ = [
    "_LazyLock",
    "_MAX_DEPTH",
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
