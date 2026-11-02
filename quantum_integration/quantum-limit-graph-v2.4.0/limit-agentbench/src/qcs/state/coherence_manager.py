"""
CoherenceManager — track coherence-like lifecycle state for quantum jobs.

Manages bounded lifetime windows that bound how long a request's
intermediate state is valid. After TTL expiry the caller must re-submit
or reopen. The abstraction is *analogous* to quantum decoherence without
claiming physical equivalence.

Two API surfaces coexist
------------------------
The **legacy sync API** (``open_window`` / ``refresh`` / ``check`` /
``close``) keys windows by ``request_id`` and returns ``CoherenceWindow``
objects. It is preserved for backward compatibility with callers written
against the pre-2.0 contract.

The **enhanced async API** (``start_coherence_window`` /
``end_coherence_window`` / ``get_coherence_status``) is the protocol
that ``qcs.execution.quantum_execution_monitor.QuantumExecutionMonitor``
calls::

    window_id = await manager.start_coherence_window(
        expected_duration_ms=..., initial_fidelity=0.99,
    )
    ...
    await manager.end_coherence_window(
        window_id=window_id, final_fidelity=0.95, successful=True,
    )

Windows are stored in a single dict keyed by ``window_id``. The legacy
API maintains a secondary ``request_id → window_id`` index so a caller
who knows only the ``request_id`` can still reach the window.

Process scope
-------------
``created_at_mono`` uses ``time.monotonic()`` — meaningful only within
the current process. A window serialized to disk or a message queue can
be reconstructed via ``CoherenceWindow.from_dict`` for audit but its
TTL is not portable to another process's monotonic clock. For
multi-process deployments, use ``created_at_utc`` (also stored) as the
authoritative timestamp.

v2 fixes (see changelog in commit message):
  * Implements ``start_coherence_window`` / ``end_coherence_window`` /
    ``get_coherence_status`` — the enhanced monitor's protocol. Previous
    version raised ``AttributeError`` on the first ``await``.
  * Fidelity tracking on the window (``initial_fidelity``,
    ``final_fidelity``, ``successful``, decay model).
  * Dual-key storage: ``window_id`` primary, ``request_id`` index for
    the legacy API.
  * ``ttl_seconds=0.0`` no longer silently becomes the default.
  * ``default_ttl_seconds`` validated; ``nan``/``inf``/``<=0`` rejected.
  * ``open_window`` policy: reject / replace / return-existing.
  * ``refresh()`` returns a new frozen window; no TOCTOU.
  * ``RefreshResult`` with typed ``RefreshStatus`` — ``None`` no longer
    conflates unknown / expired / closed.
  * ``check()`` pure by default; explicit ``purge_expired()``.
  * 128-bit ``window_id`` (was 48 bits, collision at ~10⁶ windows).
  * ``CoherenceWindow`` frozen; ``metadata`` frozen.
  * ``to_dict`` / ``from_dict`` / ``checksum`` on the window.
  * ``CoherenceConfig``; metrics; ``health()``; thread-safe.
  * ``__version__``, ``__all__``, demo.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import secrets
import threading
import time
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Constants
# =============================================================================

DEFAULT_TTL_SECONDS: float = 60.0
DEFAULT_EXPECTED_DURATION_MS: float = 100.0
DEFAULT_MAX_TTL_SECONDS: float = 3600.0        # 1 hour cap per window
DEFAULT_MAX_WINDOWS: int = 10_000
DEFAULT_INITIAL_FIDELITY: float = 0.99


# =============================================================================
# Errors
# =============================================================================

class CoherenceError(Exception):
    """Base class for CoherenceManager errors."""


class CoherenceInputError(CoherenceError, ValueError):
    """Invalid input to the constructor or a method."""


class WindowAlreadyOpenError(CoherenceError):
    """``open_window`` was called for a request that already has a live window."""


# =============================================================================
# Enums
# =============================================================================

class WindowStatus(str, Enum):
    """Lifecycle status of a coherence window."""

    ACTIVE = "active"
    EXPIRED = "expired"
    CLOSED = "closed"


class RefreshStatus(str, Enum):
    """Outcome of ``refresh`` / ``refresh_detailed``."""

    REFRESHED = "refreshed"
    UNKNOWN = "unknown"          # no window for this key
    EXPIRED = "expired"          # window existed but TTL had elapsed
    CLOSED = "closed"            # window was explicitly closed


class OpenWindowPolicy(str, Enum):
    """How ``open_window`` handles an existing live window for the same key."""

    REJECT = "reject"                # raise ``WindowAlreadyOpenError``
    REPLACE = "replace"              # close the old window, open a new one
    RETURN_EXISTING = "return_existing"  # return the existing live window


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class CoherenceConfig:
    """Frozen, validated configuration for ``CoherenceManager``."""

    default_ttl_seconds: float = DEFAULT_TTL_SECONDS
    max_ttl_seconds: float = DEFAULT_MAX_TTL_SECONDS
    default_expected_duration_ms: float = DEFAULT_EXPECTED_DURATION_MS
    default_initial_fidelity: float = DEFAULT_INITIAL_FIDELITY
    max_windows: int = DEFAULT_MAX_WINDOWS
    purge_on_access: bool = False      # if True, check() evicts expired windows
    default_open_policy: OpenWindowPolicy = OpenWindowPolicy.REJECT

    def __post_init__(self) -> None:
        for fname in (
            "default_ttl_seconds", "max_ttl_seconds",
            "default_expected_duration_ms",
        ):
            v = getattr(self, fname)
            if isinstance(v, bool) \
                    or not isinstance(v, (int, float)) \
                    or not math.isfinite(float(v)) \
                    or float(v) <= 0:
                raise CoherenceInputError(
                    f"{fname} must be a positive finite number; "
                    f"got {v!r}"
                )
        if float(self.default_ttl_seconds) > float(self.max_ttl_seconds):
            raise CoherenceInputError(
                "default_ttl_seconds must be <= max_ttl_seconds"
            )
        if not (0.0 <= float(self.default_initial_fidelity) <= 1.0):
            raise CoherenceInputError(
                "default_initial_fidelity must be in [0, 1]"
            )
        if isinstance(self.max_windows, bool) \
                or not isinstance(self.max_windows, int) \
                or self.max_windows <= 0:
            raise CoherenceInputError(
                "max_windows must be a positive int"
            )
        if not isinstance(self.default_open_policy, OpenWindowPolicy):
            try:
                object.__setattr__(
                    self, "default_open_policy",
                    OpenWindowPolicy(self.default_open_policy),
                )
            except ValueError:
                allowed = sorted(p.value for p in OpenWindowPolicy)
                raise CoherenceInputError(
                    f"default_open_policy must be one of {allowed}"
                ) from None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "default_ttl_seconds": self.default_ttl_seconds,
            "max_ttl_seconds": self.max_ttl_seconds,
            "default_expected_duration_ms": self.default_expected_duration_ms,
            "default_initial_fidelity": self.default_initial_fidelity,
            "max_windows": self.max_windows,
            "purge_on_access": self.purge_on_access,
            "default_open_policy": self.default_open_policy.value,
        }


# =============================================================================
# Metrics
# =============================================================================

class MetricsRecorder:
    """Drop-in replaceable with Prometheus / OTel."""

    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _key(name: str, labels: Mapping[str, Any]) -> str:
        if not labels:
            return name
        parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{parts}}}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.counters[k] = self.counters.get(k, 0.0) + value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.histograms.setdefault(k, []).append(float(value))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            hist: Dict[str, Dict[str, float]] = {}
            for k, vals in self.histograms.items():
                if not vals:
                    continue
                s = sorted(vals)
                hist[k] = {
                    "count": len(vals),
                    "mean": sum(vals) / len(vals),
                    "p50": s[len(s) // 2],
                    "p99": s[min(len(s) - 1, int(len(s) * 0.99))],
                    "max": s[-1],
                }
            return {
                "counters": dict(self.counters),
                "histograms": hist,
            }


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _freeze_mapping(m: Any) -> Mapping[str, Any]:
    if m is None:
        return MappingProxyType({})
    if isinstance(m, MappingProxyType):
        return m
    if not isinstance(m, Mapping):
        raise CoherenceInputError(
            f"metadata must be a Mapping; got {type(m).__name__}"
        )
    return MappingProxyType(dict(m))


def _to_jsonable(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return repr(obj)


def _new_window_id() -> str:
    return "win-" + secrets.token_hex(16)   # 128 bits


# =============================================================================
# Data classes
# =============================================================================

@dataclass(frozen=True)
class CoherenceWindow:
    """
    Immutable record of a bounded-lifetime window.

    Fields
    ------
    window_id : str
        Primary key. 128-bit random hex with ``win-`` prefix.
    request_id : Optional[str]
        Correlation ID (optional in the enhanced async path where the
        caller has no request-level context).
    created_at_mono / created_at_utc
        Start instant. Monotonic for TTL arithmetic; UTC for audit.
    ttl_seconds : float
        Bounded lifetime. Positive; the window expires this many seconds
        after ``created_at_mono`` (or after the last ``refresh``/``extend``).
    initial_fidelity / final_fidelity
        Fidelity at window open / at ``end_coherence_window``.
    successful : Optional[bool]
        Populated by ``end_coherence_window``.
    closed_at_mono / closed_at_utc
        Populated by ``close`` / ``end_coherence_window``.
    metadata : Mapping[str, Any]
        Frozen caller-supplied data.
    """

    window_id: str
    request_id: Optional[str] = None
    created_at_mono: float = 0.0
    created_at_utc: datetime = field(default_factory=_utcnow)
    ttl_seconds: float = DEFAULT_TTL_SECONDS
    initial_fidelity: Optional[float] = None
    final_fidelity: Optional[float] = None
    successful: Optional[bool] = None
    closed_at_mono: Optional[float] = None
    closed_at_utc: Optional[datetime] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.window_id, str) or not self.window_id.strip():
            raise CoherenceInputError(
                "window_id must be a non-empty string"
            )
        if self.request_id is not None:
            if not isinstance(self.request_id, str) \
                    or not self.request_id.strip():
                raise CoherenceInputError(
                    "request_id must be a non-empty string or None"
                )
        if not isinstance(self.created_at_mono, (int, float)) \
                or isinstance(self.created_at_mono, bool) \
                or not math.isfinite(float(self.created_at_mono)):
            raise CoherenceInputError(
                "created_at_mono must be a finite number"
            )
        if not isinstance(self.ttl_seconds, (int, float)) \
                or isinstance(self.ttl_seconds, bool) \
                or not math.isfinite(float(self.ttl_seconds)) \
                or self.ttl_seconds < 0:
            raise CoherenceInputError(
                "ttl_seconds must be a non-negative finite number"
            )
        for fname in ("initial_fidelity", "final_fidelity"):
            v = getattr(self, fname)
            if v is None:
                continue
            if not isinstance(v, (int, float)) \
                    or isinstance(v, bool) \
                    or not math.isfinite(float(v)) \
                    or not (0.0 <= float(v) <= 1.0):
                raise CoherenceInputError(
                    f"{fname} must be in [0, 1] or None"
                )
        if self.successful is not None and not isinstance(
            self.successful, bool,
        ):
            raise CoherenceInputError(
                "successful must be a bool or None"
            )
        object.__setattr__(
            self, "created_at_utc", _ensure_utc(self.created_at_utc),
        )
        if self.closed_at_utc is not None:
            object.__setattr__(
                self, "closed_at_utc", _ensure_utc(self.closed_at_utc),
            )
        if not isinstance(self.metadata, MappingProxyType):
            object.__setattr__(
                self, "metadata", _freeze_mapping(self.metadata),
            )

    # ------------------------------------------------------------------ compat
    @property
    def created_at(self) -> float:
        """Deprecated: use ``created_at_mono``."""
        return self.created_at_mono

    # ------------------------------------------------------------------ state
    def is_closed(self) -> bool:
        return self.closed_at_mono is not None

    def is_expired(self, at_mono: Optional[float] = None) -> bool:
        if self.is_closed():
            return False
        now = at_mono if at_mono is not None else time.monotonic()
        return (now - self.created_at_mono) > self.ttl_seconds

    def elapsed_seconds(self, at_mono: Optional[float] = None) -> float:
        now = at_mono if at_mono is not None else time.monotonic()
        return max(0.0, now - self.created_at_mono)

    def remaining_seconds(self, at_mono: Optional[float] = None) -> float:
        if self.is_closed():
            return 0.0
        return max(0.0, self.ttl_seconds - self.elapsed_seconds(at_mono))

    def status(self, at_mono: Optional[float] = None) -> WindowStatus:
        if self.is_closed():
            return WindowStatus.CLOSED
        if self.is_expired(at_mono):
            return WindowStatus.EXPIRED
        return WindowStatus.ACTIVE

    # ------------------------------------------------------------------ fidelity
    def estimated_current_fidelity(
        self, at_mono: Optional[float] = None,
    ) -> Optional[float]:
        """
        Fidelity estimate.

        * Closed window → the recorded final fidelity.
        * Active window with an initial fidelity → ``initial *
          exp(-elapsed / ttl)``, i.e. exponential decay with time
          constant equal to the coherence time (the window's TTL).
        * Otherwise → ``None``.
        """
        if self.is_closed():
            return self.final_fidelity
        if self.initial_fidelity is None:
            return None
        if self.ttl_seconds <= 0:
            return self.initial_fidelity
        elapsed = self.elapsed_seconds(at_mono)
        return float(
            self.initial_fidelity * math.exp(-elapsed / self.ttl_seconds)
        )

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "window_id": self.window_id,
            "request_id": self.request_id,
            "created_at_mono": self.created_at_mono,
            "created_at_utc": self.created_at_utc.isoformat(),
            "ttl_seconds": self.ttl_seconds,
            "initial_fidelity": self.initial_fidelity,
            "final_fidelity": self.final_fidelity,
            "successful": self.successful,
            "closed_at_mono": self.closed_at_mono,
            "closed_at_utc": (
                self.closed_at_utc.isoformat()
                if self.closed_at_utc else None
            ),
            "metadata": _to_jsonable(dict(self.metadata)),
            # Derived views for convenience:
            "status": self.status().value,
            "remaining_seconds": self.remaining_seconds(),
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(
        cls,
        d: Mapping[str, Any],
        *,
        validate: bool = True,
    ) -> "CoherenceWindow":
        if not isinstance(d, Mapping):
            raise CoherenceInputError(
                f"d must be a Mapping; got {type(d).__name__}"
            )
        known = {f.name for f in fields(cls)}
        unknown = set(d.keys()) - known - {
            "created_at", "status", "remaining_seconds",
        }
        if unknown:
            raise CoherenceInputError(
                f"unknown fields in CoherenceWindow dict: "
                f"{sorted(unknown)}"
            )

        created_at_utc = d.get("created_at_utc")
        if isinstance(created_at_utc, str):
            created_at_utc = datetime.fromisoformat(created_at_utc)
        elif created_at_utc is None:
            created_at_utc = _utcnow()

        closed_at_utc = d.get("closed_at_utc")
        if isinstance(closed_at_utc, str):
            closed_at_utc = datetime.fromisoformat(closed_at_utc)

        created_at_mono = d.get("created_at_mono")
        if created_at_mono is None:
            # Legacy key `created_at` or missing (audit reconstruction)
            created_at_mono = d.get("created_at", 0.0)

        return cls(
            window_id=d["window_id"],
            request_id=d.get("request_id"),
            created_at_mono=float(created_at_mono),
            created_at_utc=created_at_utc,
            ttl_seconds=float(d.get("ttl_seconds", DEFAULT_TTL_SECONDS)),
            initial_fidelity=d.get("initial_fidelity"),
            final_fidelity=d.get("final_fidelity"),
            successful=d.get("successful"),
            closed_at_mono=d.get("closed_at_mono"),
            closed_at_utc=closed_at_utc,
            metadata=dict(d.get("metadata") or {}),
        )

    def checksum(self) -> str:
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            f"CoherenceWindow("
            f"id={self.window_id!r}, "
            f"request={self.request_id!r}, "
            f"status={self.status().value}, "
            f"remaining={self.remaining_seconds():.2f}s)"
        )


@dataclass(frozen=True)
class RefreshResult:
    """Typed outcome of ``refresh_detailed``."""

    status: RefreshStatus
    window: Optional[CoherenceWindow] = None

    def __bool__(self) -> bool:
        return self.status == RefreshStatus.REFRESHED

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "window": self.window.to_dict() if self.window else None,
        }


# =============================================================================
# The Enhanced CoherenceManager
# =============================================================================

class CoherenceManager:
    """
    Manages coherence windows. See the module docstring for the dual
    sync / async API surface and process-scope notes.
    """

    def __init__(
        self,
        *,
        default_ttl_seconds: Optional[float] = None,
        config: Optional[CoherenceConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        if config is None:
            cfg_kwargs: Dict[str, Any] = {}
            if default_ttl_seconds is not None:
                cfg_kwargs["default_ttl_seconds"] = float(default_ttl_seconds)
            config = CoherenceConfig(**cfg_kwargs)
        self.config = config
        self.default_ttl_seconds = config.default_ttl_seconds
        self._metrics = metrics or MetricsRecorder()
        self._lock = threading.RLock()
        self._windows_by_id: Dict[str, CoherenceWindow] = {}
        self._by_request_id: Dict[str, str] = {}
        self._started_mono = time.monotonic()

    # ------------------------------------------------------------------ async: enhanced monitor protocol
    async def start_coherence_window(
        self,
        expected_duration_ms: float,
        initial_fidelity: float,
        *,
        request_id: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        policy: Optional[OpenWindowPolicy] = None,
    ) -> str:
        """
        Open a coherence window sized to the expected duration.

        Returns the ``window_id`` the caller must supply to
        ``end_coherence_window`` and ``get_coherence_status``.

        The TTL is derived from ``expected_duration_ms`` (converted to
        seconds) and clamped to ``config.max_ttl_seconds``.
        """
        if not isinstance(expected_duration_ms, (int, float)) \
                or isinstance(expected_duration_ms, bool) \
                or not math.isfinite(float(expected_duration_ms)) \
                or expected_duration_ms <= 0:
            raise CoherenceInputError(
                "expected_duration_ms must be a positive finite number"
            )
        ttl_seconds = float(expected_duration_ms) / 1000.0
        if ttl_seconds > self.config.max_ttl_seconds:
            logger.warning(
                "expected_duration_ms=%s exceeds max_ttl_seconds=%s; "
                "clamping TTL",
                expected_duration_ms, self.config.max_ttl_seconds,
            )
            ttl_seconds = self.config.max_ttl_seconds
        initial_fid = self._validate_fidelity(
            "initial_fidelity", initial_fidelity,
        )
        window = self._open(
            ttl_seconds=ttl_seconds,
            initial_fidelity=initial_fid,
            request_id=request_id,
            metadata=metadata,
            policy=policy,
        )
        return window.window_id

    async def end_coherence_window(
        self,
        window_id: str,
        final_fidelity: float,
        successful: bool,
    ) -> None:
        """
        Close a coherence window with a final fidelity and outcome flag.

        Never raises: if the window isn't found (e.g. it already expired
        or the caller passed a stale ID), a warning is logged and the
        call is a no-op. The caller already has its execution result;
        coherence bookkeeping must not fail it.
        """
        try:
            self._end(
                window_id=window_id,
                final_fidelity=final_fidelity,
                successful=successful,
            )
        except Exception:
            logger.exception(
                "end_coherence_window raised",
                extra={"window_id": window_id},
            )

    async def get_coherence_status(
        self, window_id: str,
    ) -> Dict[str, Any]:
        """
        Return a dict view of a window's status.

        Keys: ``window_id``, ``request_id``, ``status``,
        ``initial_fidelity``, ``final_fidelity``, ``current_fidelity``,
        ``remaining_seconds``, ``elapsed_seconds``, ``successful``,
        ``metadata``.

        Returns ``{"window_id": window_id, "status": "unknown"}`` for an
        unknown window; never raises.
        """
        window = self.get_window(window_id)
        if window is None:
            return {
                "window_id": window_id,
                "request_id": None,
                "status": "unknown",
                "initial_fidelity": None,
                "final_fidelity": None,
                "current_fidelity": None,
                "remaining_seconds": 0.0,
                "elapsed_seconds": 0.0,
                "successful": None,
                "metadata": {},
            }
        return {
            "window_id": window.window_id,
            "request_id": window.request_id,
            "status": window.status().value,
            "initial_fidelity": window.initial_fidelity,
            "final_fidelity": window.final_fidelity,
            "current_fidelity": window.estimated_current_fidelity(),
            "remaining_seconds": window.remaining_seconds(),
            "elapsed_seconds": window.elapsed_seconds(),
            "successful": window.successful,
            "metadata": dict(window.metadata),
        }

    # ------------------------------------------------------------------ sync: legacy API
    def open_window(
        self,
        request_id: str,
        *,
        ttl_seconds: Optional[float] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        policy: Optional[OpenWindowPolicy] = None,
    ) -> CoherenceWindow:
        """
        Open a window keyed by ``request_id``.

        ``ttl_seconds=None`` (the default) uses ``default_ttl_seconds``.
        ``ttl_seconds=0.0`` creates an immediately-expired window — the
        caller asked for it.
        """
        if not isinstance(request_id, str) or not request_id.strip():
            raise CoherenceInputError(
                "request_id must be a non-empty string"
            )
        resolved_ttl = (
            float(ttl_seconds) if ttl_seconds is not None
            else float(self.default_ttl_seconds)
        )
        if not math.isfinite(resolved_ttl) or resolved_ttl < 0:
            raise CoherenceInputError(
                "ttl_seconds must be a non-negative finite number"
            )
        if resolved_ttl > self.config.max_ttl_seconds:
            logger.warning(
                "ttl_seconds=%s exceeds max_ttl_seconds=%s; clamping",
                resolved_ttl, self.config.max_ttl_seconds,
            )
            resolved_ttl = self.config.max_ttl_seconds
        return self._open(
            ttl_seconds=resolved_ttl,
            initial_fidelity=None,
            request_id=request_id,
            metadata=metadata,
            policy=policy,
        )

    def get_window(
        self, window_id: str,
    ) -> Optional[CoherenceWindow]:
        with self._lock:
            return self._windows_by_id.get(window_id)

    def get_window_by_request(
        self, request_id: str,
    ) -> Optional[CoherenceWindow]:
        with self._lock:
            window_id = self._by_request_id.get(request_id)
            if window_id is None:
                return None
            return self._windows_by_id.get(window_id)

    def refresh(
        self, request_id: str,
    ) -> Optional[CoherenceWindow]:
        """
        Refresh a window by ``request_id`` — restarts the TTL clock.

        Returns the refreshed (frozen) window, or ``None`` on unknown /
        expired / closed. Use ``refresh_detailed`` to distinguish those
        cases.
        """
        return self.refresh_detailed(request_id).window

    def refresh_detailed(
        self, request_id: str,
    ) -> RefreshResult:
        with self._lock:
            window_id = self._by_request_id.get(request_id)
            if window_id is None:
                return RefreshResult(RefreshStatus.UNKNOWN)
            window = self._windows_by_id.get(window_id)
            if window is None:
                return RefreshResult(RefreshStatus.UNKNOWN)
            if window.is_closed():
                return RefreshResult(RefreshStatus.CLOSED, window)
            if window.is_expired():
                return RefreshResult(RefreshStatus.EXPIRED, window)
            refreshed = replace(
                window,
                created_at_mono=time.monotonic(),
                created_at_utc=_utcnow(),
            )
            self._windows_by_id[window_id] = refreshed
        self._metrics.inc("windows_refreshed_total")
        logger.debug(
            "coherence window refreshed",
            extra={
                "window_id": window_id,
                "request_id": request_id,
            },
        )
        return RefreshResult(RefreshStatus.REFRESHED, refreshed)

    def extend(
        self, request_id: str, *, additional_seconds: float,
    ) -> Optional[CoherenceWindow]:
        """
        Add seconds to a live window's TTL, keeping the original
        ``created_at``. Complementary to ``refresh`` (which restarts
        the clock instead of extending the budget).
        """
        if not isinstance(additional_seconds, (int, float)) \
                or isinstance(additional_seconds, bool) \
                or not math.isfinite(float(additional_seconds)) \
                or additional_seconds <= 0:
            raise CoherenceInputError(
                "additional_seconds must be a positive finite number"
            )
        with self._lock:
            window_id = self._by_request_id.get(request_id)
            if window_id is None:
                return None
            window = self._windows_by_id.get(window_id)
            if window is None or window.is_closed() or window.is_expired():
                return None
            new_ttl = min(
                window.ttl_seconds + float(additional_seconds),
                self.config.max_ttl_seconds,
            )
            extended = replace(window, ttl_seconds=new_ttl)
            self._windows_by_id[window_id] = extended
        self._metrics.inc("windows_extended_total")
        logger.debug(
            "coherence window extended",
            extra={
                "window_id": window_id,
                "request_id": request_id,
                "additional_seconds": additional_seconds,
            },
        )
        return extended

    def check(self, request_id: str) -> bool:
        """
        Return True iff a live window exists for ``request_id``.

        Pure by default. When ``config.purge_on_access`` is True, an
        expired window is evicted on read (which the pre-2.0 version
        always did — opt in to restore that behavior).
        """
        with self._lock:
            window_id = self._by_request_id.get(request_id)
            if window_id is None:
                return False
            window = self._windows_by_id.get(window_id)
            if window is None:
                return False
            if window.is_closed():
                return False
            if window.is_expired():
                if self.config.purge_on_access:
                    self._evict(window_id)
                return False
            return True

    def check_window(self, window_id: str) -> bool:
        """Return True iff a live window exists for ``window_id``."""
        window = self.get_window(window_id)
        if window is None:
            return False
        if window.is_closed():
            return False
        return not window.is_expired()

    def close(self, request_id: str) -> bool:
        """Close (and evict) the window for ``request_id``. Returns True if found."""
        with self._lock:
            window_id = self._by_request_id.pop(request_id, None)
            if window_id is None:
                return False
            window = self._windows_by_id.get(window_id)
            if window is None:
                return False
            if not window.is_closed():
                closed = self._close_window(window)
                self._windows_by_id[window_id] = closed
        self._metrics.inc("windows_closed_total")
        return True

    def close_window(
        self,
        window_id: str,
        *,
        final_fidelity: Optional[float] = None,
        successful: Optional[bool] = None,
    ) -> Optional[CoherenceWindow]:
        """
        Close a window by ``window_id``. Optionally record fidelity and
        outcome (used internally by ``end_coherence_window``).

        Returns the closed (frozen) window, or ``None`` if not found.
        """
        with self._lock:
            window = self._windows_by_id.get(window_id)
            if window is None:
                return None
            if window.is_closed():
                return window
            closed = self._close_window(
                window,
                final_fidelity=final_fidelity,
                successful=successful,
            )
            self._windows_by_id[window_id] = closed
        self._metrics.inc("windows_closed_total")
        logger.debug(
            "coherence window closed",
            extra={"window_id": window_id},
        )
        return closed

    def reopen(
        self,
        request_id: str,
        *,
        ttl_seconds: Optional[float] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> CoherenceWindow:
        """
        Close the current window (if any) and open a fresh one with a
        new ``window_id`` for the same ``request_id``. The old window
        remains queryable by its old ``window_id``.
        """
        self.close(request_id)
        return self.open_window(
            request_id,
            ttl_seconds=ttl_seconds,
            metadata=metadata,
            policy=OpenWindowPolicy.REPLACE,
        )

    # ------------------------------------------------------------------ sweeps and queries
    def purge_expired(self) -> int:
        """
        Evict all expired (and orphaned closed) windows. Returns the
        number evicted. Intended to be called periodically by a
        background sweeper.
        """
        removed = 0
        now = time.monotonic()
        with self._lock:
            stale_ids = [
                wid for wid, w in self._windows_by_id.items()
                if (not w.is_closed() and w.is_expired(now))
                or (w.is_closed() and w.closed_at_utc is not None
                    and (_utcnow() - w.closed_at_utc).total_seconds() > 3600)
            ]
            for wid in stale_ids:
                self._evict(wid)
                removed += 1
        if removed:
            self._metrics.inc("windows_purged_total", count=removed)
            logger.debug(
                "purged expired windows",
                extra={"count": removed},
            )
        return removed

    def list_active_windows(self) -> Tuple[CoherenceWindow, ...]:
        with self._lock:
            return tuple(
                w for w in self._windows_by_id.values()
                if w.status() == WindowStatus.ACTIVE
            )

    def list_windows(self) -> Tuple[CoherenceWindow, ...]:
        with self._lock:
            return tuple(self._windows_by_id.values())

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            active = 0
            expired = 0
            closed = 0
            for w in self._windows_by_id.values():
                status = w.status()
                if status == WindowStatus.ACTIVE:
                    active += 1
                elif status == WindowStatus.EXPIRED:
                    expired += 1
                else:
                    closed += 1
            return {
                "active_windows": active,
                "expired_windows": expired,
                "closed_windows": closed,
                "total_windows": len(self._windows_by_id),
            }

    def health(self) -> Dict[str, Any]:
        s = self.stats()
        return {
            "ready": True,
            "config": self.config.to_dict(),
            "windows": s,
            "uptime_seconds": time.monotonic() - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }

    # ------------------------------------------------------------------ internals
    def _open(
        self,
        *,
        ttl_seconds: float,
        initial_fidelity: Optional[float],
        request_id: Optional[str],
        metadata: Optional[Mapping[str, Any]],
        policy: Optional[OpenWindowPolicy],
    ) -> CoherenceWindow:
        resolved_policy = policy or self.config.default_open_policy
        if not isinstance(resolved_policy, OpenWindowPolicy):
            try:
                resolved_policy = OpenWindowPolicy(resolved_policy)
            except ValueError:
                allowed = sorted(p.value for p in OpenWindowPolicy)
                raise CoherenceInputError(
                    f"policy must be one of {allowed}; "
                    f"got {resolved_policy!r}"
                ) from None

        with self._lock:
            # Capacity check
            if len(self._windows_by_id) >= self.config.max_windows:
                self.purge_expired()
                if len(self._windows_by_id) >= self.config.max_windows:
                    raise CoherenceError(
                        f"coherence window capacity "
                        f"({self.config.max_windows}) exhausted"
                    )

            # Duplicate check on request_id
            if request_id is not None:
                existing_id = self._by_request_id.get(request_id)
                if existing_id is not None:
                    existing = self._windows_by_id.get(existing_id)
                    if existing is not None \
                            and existing.status() == WindowStatus.ACTIVE:
                        if resolved_policy == OpenWindowPolicy.RETURN_EXISTING:
                            return existing
                        if resolved_policy == OpenWindowPolicy.REJECT:
                            raise WindowAlreadyOpenError(
                                f"request {request_id!r} already has "
                                f"an active coherence window "
                                f"({existing_id!r}); pass "
                                f"policy='replace' to replace it"
                            )
                        # REPLACE falls through: close existing, open new.

            # Close any stale entry (for REPLACE and for expired-by-now)
            if request_id is not None:
                stale_id = self._by_request_id.get(request_id)
                if stale_id is not None:
                    stale = self._windows_by_id.get(stale_id)
                    if stale is not None and not stale.is_closed():
                        self._windows_by_id[stale_id] = self._close_window(
                            stale,
                        )

            window = CoherenceWindow(
                window_id=_new_window_id(),
                request_id=request_id,
                created_at_mono=time.monotonic(),
                created_at_utc=_utcnow(),
                ttl_seconds=ttl_seconds,
                initial_fidelity=initial_fidelity,
                metadata=metadata or {},
            )
            self._windows_by_id[window.window_id] = window
            if request_id is not None:
                self._by_request_id[request_id] = window.window_id

        self._metrics.inc(
            "windows_opened_total",
            has_request=str(request_id is not None).lower(),
        )
        logger.debug(
            "coherence window opened",
            extra={
                "window_id": window.window_id,
                "request_id": request_id,
                "ttl_seconds": ttl_seconds,
                "initial_fidelity": initial_fidelity,
            },
        )
        return window

    def _end(
        self,
        *,
        window_id: str,
        final_fidelity: float,
        successful: bool,
    ) -> Optional[CoherenceWindow]:
        fid = self._validate_fidelity(
            "final_fidelity", final_fidelity,
        )
        if not isinstance(successful, bool):
            raise CoherenceInputError(
                "successful must be a bool"
            )
        with self._lock:
            window = self._windows_by_id.get(window_id)
            if window is None:
                logger.warning(
                    "end_coherence_window: window not found",
                    extra={"window_id": window_id},
                )
                return None
            if window.is_closed():
                # Already closed; keep the earlier fidelity, but record
                # the outcome if it wasn't recorded.
                updated = replace(
                    window,
                    final_fidelity=(
                        window.final_fidelity
                        if window.final_fidelity is not None else fid
                    ),
                    successful=(
                        window.successful
                        if window.successful is not None else bool(successful)
                    ),
                )
                self._windows_by_id[window_id] = updated
                return updated
            closed = self._close_window(
                window,
                final_fidelity=fid,
                successful=bool(successful),
            )
            self._windows_by_id[window_id] = closed
        self._metrics.inc(
            "windows_ended_total",
            successful=str(bool(successful)).lower(),
        )
        logger.debug(
            "coherence window ended",
            extra={
                "window_id": window_id,
                "final_fidelity": fid,
                "successful": successful,
            },
        )
        return closed

    def _close_window(
        self,
        window: CoherenceWindow,
        *,
        final_fidelity: Optional[float] = None,
        successful: Optional[bool] = None,
    ) -> CoherenceWindow:
        return replace(
            window,
            closed_at_mono=time.monotonic(),
            closed_at_utc=_utcnow(),
            final_fidelity=(
                final_fidelity
                if final_fidelity is not None
                else window.final_fidelity
            ),
            successful=(
                successful
                if successful is not None
                else window.successful
            ),
        )

    def _evict(self, window_id: str) -> None:
        """Remove the window and its request-id index entry."""
        window = self._windows_by_id.pop(window_id, None)
        if window is None:
            return
        if window.request_id is not None:
            # Only remove the index if it still points at this window.
            if self._by_request_id.get(window.request_id) == window_id:
                self._by_request_id.pop(window.request_id, None)

    @staticmethod
    def _validate_fidelity(name: str, v: Any) -> float:
        if not isinstance(v, (int, float)) \
                or isinstance(v, bool) \
                or not math.isfinite(float(v)) \
                or not (0.0 <= float(v) <= 1.0):
            raise CoherenceInputError(
                f"{name} must be a number in [0, 1]"
            )
        return float(v)


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "CoherenceConfig",
    "CoherenceError",
    "CoherenceInputError",
    "CoherenceManager",
    "CoherenceWindow",
    "DEFAULT_TTL_SECONDS",
    "MetricsRecorder",
    "OpenWindowPolicy",
    "RefreshResult",
    "RefreshStatus",
    "WindowAlreadyOpenError",
    "WindowStatus",
)


# =============================================================================
# Demo: python -m qcs.state.coherence_manager
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import asyncio
    import json

    logging.basicConfig(level=logging.INFO)

    print("=== §1 Enhanced monitor protocol (async) ===")

    async def demo_async() -> None:
        mgr = CoherenceManager()
        # start_coherence_window — the enhanced monitor's call
        window_id = await mgr.start_coherence_window(
            expected_duration_ms=100.0,
            initial_fidelity=0.99,
        )
        print(f"  started: window_id={window_id!r}")

        # get_coherence_status — mid-execution query
        status = await mgr.get_coherence_status(window_id)
        print(f"  status: {status['status']}, "
              f"current_fidelity={status['current_fidelity']:.4f}, "
              f"remaining={status['remaining_seconds']:.3f}s")
        assert status["status"] == "active"
        assert 0.0 < status["current_fidelity"] <= 0.99

        # sleep a bit so decay is visible
        await asyncio.sleep(0.02)
        status2 = await mgr.get_coherence_status(window_id)
        print(f"  after 20ms: current_fidelity="
              f"{status2['current_fidelity']:.4f}")
        assert status2["current_fidelity"] < status["current_fidelity"]

        # end_coherence_window — the enhanced monitor's close call
        await mgr.end_coherence_window(
            window_id=window_id,
            final_fidelity=0.95,
            successful=True,
        )
        status3 = await mgr.get_coherence_status(window_id)
        print(f"  after end: status={status3['status']}, "
              f"final_fidelity={status3['final_fidelity']}, "
              f"successful={status3['successful']}")
        assert status3["status"] == "closed"
        assert status3["final_fidelity"] == 0.95
        assert status3["successful"] is True

        # Unknown window → dict with "unknown", no crash
        unknown = await mgr.get_coherence_status("nope")
        print(f"  unknown: {unknown['status']}")
        assert unknown["status"] == "unknown"

    asyncio.run(demo_async())

    print("\n=== §2.1 ttl_seconds=0.0 stays 0.0 ===")
    mgr = CoherenceManager()
    w = mgr.open_window("req-zero", ttl_seconds=0.0)
    print(f"  ttl_seconds: {w.ttl_seconds}")
    assert w.ttl_seconds == 0.0
    # Immediately expired
    import time
    time.sleep(0.001)
    print(f"  expired after 1ms: {w.is_expired()}")
    assert w.is_expired()

    print("\n=== §2.2 default_ttl_seconds validation ===")
    for label, val in [
        ("nan", float("nan")),
        ("inf", float("inf")),
        ("negative", -1.0),
        ("zero", 0.0),
    ]:
        try:
            CoherenceManager(default_ttl_seconds=val)
            print(f"  {label}: NOT rejected")
        except CoherenceInputError:
            print(f"  {label}: rejected")

    print("\n=== §2.3 open_window policy ===")
    mgr2 = CoherenceManager()
    w1 = mgr2.open_window("req-1")
    print(f"  first open: {w1.window_id}")
    # Default REJECT: raises
    try:
        mgr2.open_window("req-1")
    except WindowAlreadyOpenError as e:
        print(f"  REJECT: {type(e).__name__}")
    # RETURN_EXISTING: same window
    w2 = mgr2.open_window("req-1", policy="return_existing")
    print(f"  RETURN_EXISTING: same window? {w2.window_id == w1.window_id}")
    assert w2.window_id == w1.window_id
    # REPLACE: new window, old is closed
    w3 = mgr2.open_window("req-1", policy="replace")
    print(f"  REPLACE: new window? {w3.window_id != w1.window_id}")
    assert w3.window_id != w1.window_id
    old = mgr2.get_window(w1.window_id)
    print(f"  old window status: {old.status().value}")
    assert old.is_closed()

    print("\n=== §2.4 refresh() returns a NEW frozen window ===")
    mgr3 = CoherenceManager()
    w = mgr3.open_window("req-r", ttl_seconds=60.0)
    original_created = w.created_at_mono
    time.sleep(0.01)
    refreshed = mgr3.refresh("req-r")
    print(f"  refreshed is new object? {refreshed is not w}")
    assert refreshed is not w
    print(f"  original created_at unchanged? "
          f"{w.created_at_mono == original_created}")
    assert w.created_at_mono == original_created
    print(f"  refreshed created_at newer? "
          f"{refreshed.created_at_mono > original_created}")
    assert refreshed.created_at_mono > original_created

    print("\n=== §2.5 refresh_detailed distinguishes cases ===")
    print(f"  refreshed: {mgr3.refresh_detailed('req-r').status.value}")
    print(f"  unknown:   {mgr3.refresh_detailed('nope').status.value}")
    # Close then refresh
    mgr3.close("req-r")
    print(f"  closed:    {mgr3.refresh_detailed('req-r').status.value}")

    print("\n=== §2.6 check() is pure by default ===")
    mgr4 = CoherenceManager()
    mgr4.open_window("req-c", ttl_seconds=0.01)
    time.sleep(0.02)
    # check() should NOT evict by default
    assert not mgr4.check("req-c")
    assert mgr4.get_window_by_request("req-c") is not None
    print(f"  after check(): window still present? "
          f"{mgr4.get_window_by_request('req-c') is not None}")
    # Explicit purge
    n = mgr4.purge_expired()
    print(f"  purge_expired() removed: {n}")
    assert n == 1

    print("\n=== §3.1/§3.2 Immutability ===")
    w = CoherenceManager().open_window("req-i")
    try:
        w.ttl_seconds = 0  # type: ignore[misc]
    except Exception as e:
        print(f"  ttl mutation → {type(e).__name__}")
    try:
        w.metadata["x"] = 1  # type: ignore[index]
    except TypeError:
        print(f"  metadata mutation → TypeError")

    print("\n=== §3.3/§3.4/§3.5 Serialization round-trip ===")
    from datetime import datetime
    w = CoherenceManager().open_window(
        "req-s", metadata={"created_by": "demo", "at": datetime.now()},
    )
    d = w.to_dict()
    print(f"  has created_at_utc: {'created_at_utc' in d}")
    print(f"  JSON length: {len(json.dumps(d))} bytes")
    assert "created_at_utc" in d
    restored = CoherenceWindow.from_dict(d)
    print(f"  round-trip (key fields): "
          f"{restored.window_id == w.window_id}")
    print(f"  checksum: {w.checksum()[:16]}…")

    print("\n=== §4.1 Purge accumulation ===")
    mgr5 = CoherenceManager()
    for i in range(20):
        mgr5.open_window(f"req-{i}", ttl_seconds=0.001)
    time.sleep(0.01)
    print(f"  before purge: {mgr5.stats()}")
    n = mgr5.purge_expired()
    print(f"  after purge ({n} removed): {mgr5.stats()}")

    print("\n=== §4.2 Thread safety ===")
    mgr6 = CoherenceManager()
    import threading

    def worker(idx: int) -> None:
        for j in range(50):
            rid = f"t-{idx}-{j}"
            mgr6.open_window(rid)
            mgr6.check(rid)
            mgr6.close(rid)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  final stats: {mgr6.stats()}")
    assert mgr6.stats()["total_windows"] >= 0

    print("\n=== §5.3 extend() ===")
    mgr7 = CoherenceManager()
    w = mgr7.open_window("req-e", ttl_seconds=10.0)
    ext = mgr7.extend("req-e", additional_seconds=30.0)
    print(f"  ttl before: 10.0, after: {ext.ttl_seconds}")
    assert ext.ttl_seconds == 40.0

    print("\n=== §5.4 reopen() ===")
    mgr8 = CoherenceManager()
    old = mgr8.open_window("req-reopen")
    new = mgr8.reopen("req-reopen")
    print(f"  old closed? {mgr8.get_window(old.window_id).is_closed()}")
    print(f"  new is different? {new.window_id != old.window_id}")
    assert mgr8.get_window(old.window_id).is_closed()
    assert new.window_id != old.window_id

    print("\n=== §5.5 list_active_windows() ===")
    mgr9 = CoherenceManager()
    mgr9.open_window("a")
    mgr9.open_window("b")
    mgr9.open_window("c", ttl_seconds=0.0)
    active = mgr9.list_active_windows()
    print(f"  active windows: {[w.request_id for w in active]}")
    time.sleep(0.005)
    active = mgr9.list_active_windows()
    print(f"  after 5ms: {[w.request_id for w in active]}")

    print("\n=== §4.4 health() ===")
    print(f"  ready: {mgr.health()['ready']}")
    print(f"  config: {mgr.health()['config']}")

    print("\n=== Legacy API preserved ===")
    mgr_legacy = CoherenceManager(default_ttl_seconds=60.0)
    w = mgr_legacy.open_window("legacy-req", metadata={"k": "v"})
    print(f"  open_window: {w!r}")
    print(f"  check(): {mgr_legacy.check('legacy-req')}")
    assert mgr_legacy.check("legacy-req")
    print(f"  close(): {mgr_legacy.close('legacy-req')}")
    assert not mgr_legacy.check("legacy-req")

    print("\n=== End-to-end: monitor-style usage ===")
    async def demo_integration() -> None:
        mgr = CoherenceManager()
        # Simulate what the enhanced monitor does.
        window_id = await mgr.start_coherence_window(
            expected_duration_ms=100.0,
            initial_fidelity=0.99,
        )
        await asyncio.sleep(0.05)
        status = await mgr.get_coherence_status(window_id)
        print(f"  mid-execution: {status['status']}, "
              f"fidelity={status['current_fidelity']:.3f}")
        await mgr.end_coherence_window(
            window_id=window_id, final_fidelity=0.92, successful=True,
        )
        final = await mgr.get_coherence_status(window_id)
        print(f"  final: {final['status']}, "
              f"successful={final['successful']}")

    asyncio.run(demo_integration())

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
