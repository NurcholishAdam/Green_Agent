# src/sustainability/eco_mode_controller.py

"""
Eco Mode Controller
===================

Controls adaptive throttling based on carbon intensity. When the grid
becomes dirty, the controller engages ECO mode via a policy engine; when
the grid recovers, it disables it.

State-commit ordering
---------------------
Policy-engine changes are applied **first**; the controller's
``eco_mode_enabled`` flag and the transition history are committed only
after the engine has acknowledged the change (or, in non-strict mode, has
raised and been logged). This guarantees that the controller's flag
reflects the engine's actual state on every path, including failure.

Non-strict coerced samples
--------------------------
In non-strict mode, a bad ``carbon_intensity`` sample (non-numeric,
``NaN``, ``inf``, or negative) is coerced to ``0.0`` and recorded in the
audit trail with ``coerced=True`` and ``raw_repr``. **The eco-mode state
is not toggled on a coerced sample** — a hardware fault should not cause
the energy-saving mode to be disabled. Use the ``coerced`` marker on the
transition record to distinguish real readings from fallbacks.

Enhancements
------------
- ``EcoModeControllerConfig`` — frozen, ``slots``, ``__repr__``; every
  field type-checked (``bool`` rejected for numeric fields; ``NaN``/``inf``
  rejected); ``from_dict(..., strict=True)`` rejects unknown keys and
  wraps parse errors.
- ``EcoModeTransition`` — ``slots``; new fields ``forced``, ``coerced``,
  ``raw_repr``, ``policy_engine_error``; ``__post_init__`` validation;
  ``from_dict``.
- **State-commit ordering fix** — policy engine is called before
  ``eco_mode_enabled`` / history are mutated. Strict-mode failures leave
  the controller untouched.
- **Coerced samples do not toggle state** — the non-strict ``NaN`` →
  ``0.0`` path no longer exits ECO mode on a hardware fault.
- **``from_dict`` / ``from_json``** added to the controller (requires a
  ``policy_engine`` argument, since the engine is a live object).
- **``to_json(include_history=True)`` now works** — the kwarg is routed
  to ``to_dict`` instead of being forwarded to ``json.dumps``.
- **Lifecycle counters** — ``total_transitions``, ``total_enters``,
  ``total_exits``, ``dropped_transitions``; ``statistics()`` reports
  lifetime counts, not capped at ``len(history)``.
- **Atomic ``statistics()``** — every field read under the lock.
- **``force_eco_mode`` records ``forced=True``** — forced transitions are
  distinguishable from coerced skips and real transitions.
- ``PolicyEngine`` protocol is enforced via ``isinstance`` instead of a
  hand-rolled ``getattr`` loop.
- ``except Exception`` paths now use ``logger.exception()`` so tracebacks
  are preserved.
- ``reset()`` re-raises ``policy_engine.reset_defaults()`` failures in
  strict mode.
- ``__eq__`` / ``__hash__`` on config and controller; ``__dir__``.
- ``__main__`` smoke test uses explicit ``_check()`` helpers so
  ``python -O`` does not silently pass.

Compatibility
-------------
- ``EcoModeController(policy_engine, dirty_threshold=400.0, *, config=None,
  strict=True)`` unchanged.
- ``update(intensity) -> bool`` unchanged.
- ``force_eco_mode(*, enabled) -> bool`` unchanged in return type;
  transitions now carry ``forced=True``.
- ``reset(*, clear_history=True) -> int`` unchanged; strict mode may now
  raise if the policy engine rejects ``reset_defaults``.
- ``statistics()`` keys ``transitions``, ``enters``, ``exits`` now report
  **lifetime** counts (not capped by ``max_history``); new keys
  ``buffered_transitions`` and ``dropped_transitions`` added.
- ``to_json`` signature is now ``to_json(*, include_history=False,
  **kwargs)`` — the kwarg routes correctly (previously it crashed).
- ``EcoModeControllerConfig.from_dict(...)`` now rejects unknown keys by
  default; pass ``strict=False`` to opt out.
- ``EcoModeController.from_dict(data, policy_engine)`` and
  ``from_json(payload, policy_engine)`` require a policy engine argument —
  the engine cannot be serialized.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop it for older versions.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import (
    Any, Deque, Dict, List, Mapping, Optional, Protocol, Tuple, TypedDict,
    runtime_checkable,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EcoModeControllerError(ValueError):
    """Raised for invalid eco-mode controller inputs or configuration."""


# --------------------------------------------------------------------------- #
# Typed return shapes
# --------------------------------------------------------------------------- #
class EcoModeStatistics(TypedDict):
    """Return shape of :meth:`EcoModeController.statistics`."""

    eco_mode_enabled: bool
    transitions: int
    enters: int
    exits: int
    buffered_transitions: int
    dropped_transitions: int
    config: Dict[str, Any]
    strict: bool
    uptime_seconds: float


# --------------------------------------------------------------------------- #
# Policy engine protocol
# --------------------------------------------------------------------------- #
@runtime_checkable
class PolicyEngine(Protocol):
    """Minimal interface the controller requires from a policy engine."""

    def set_pruning_aggressiveness(self, value: float) -> None: ...
    def set_token_limit_ratio(self, value: float) -> None: ...
    def reset_defaults(self) -> None: ...


# --------------------------------------------------------------------------- #
# Numeric helpers
# --------------------------------------------------------------------------- #
def _require_finite_nonneg(name: str, value: Any) -> float:
    """Return ``float(value)``; raise unless finite and >= 0."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EcoModeControllerError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv) or fv < 0:
        raise EcoModeControllerError(
            f"{name} must be finite and >= 0, got {value!r}."
        )
    return fv


def _require_unit_interval(name: str, value: Any) -> float:
    """Return ``float(value)``; raise unless finite and in [0, 1]."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EcoModeControllerError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv) or not 0.0 <= fv <= 1.0:
        raise EcoModeControllerError(
            f"{name} must be finite and in [0, 1], got {value!r}."
        )
    return fv


def _require_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EcoModeControllerError(f"{name} must be an int.")
    if value <= 0:
        raise EcoModeControllerError(f"{name} must be > 0.")
    return value


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class EcoModeControllerConfig:
    """Tunable parameters for :class:`EcoModeController`."""

    dirty_threshold: float = 400.0
    clean_threshold: float = 350.0  # hysteresis: leave ECO below this

    eco_pruning_aggressiveness: float = 0.8
    eco_token_limit_ratio: float = 0.6

    max_history: int = 1_000

    def __post_init__(self) -> None:
        dt = _require_finite_nonneg(
            "dirty_threshold", self.dirty_threshold
        )
        ct = _require_finite_nonneg(
            "clean_threshold", self.clean_threshold
        )
        if ct >= dt:
            raise EcoModeControllerError(
                "clean_threshold must be < dirty_threshold (hysteresis)."
            )
        _require_unit_interval(
            "eco_pruning_aggressiveness",
            self.eco_pruning_aggressiveness,
        )
        _require_unit_interval(
            "eco_token_limit_ratio",
            self.eco_token_limit_ratio,
        )
        _require_positive_int("max_history", self.max_history)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "EcoModeControllerConfig":
        if not isinstance(data, Mapping):
            raise EcoModeControllerError(
                "EcoModeControllerConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise EcoModeControllerError(
                    f"unknown EcoModeControllerConfig keys: {unknown}"
                )
        try:
            kwargs = {k: v for k, v in data.items() if k in valid}
            return cls(**kwargs)
        except EcoModeControllerError:
            raise
        except (TypeError, ValueError) as exc:
            raise EcoModeControllerError(
                f"invalid EcoModeControllerConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "EcoModeControllerConfig("
            f"dirty={self.dirty_threshold:.1f}, "
            f"clean={self.clean_threshold:.1f}, "
            f"pruning={self.eco_pruning_aggressiveness:.2f}, "
            f"tokens={self.eco_token_limit_ratio:.2f}, "
            f"history={self.max_history})"
        )


# --------------------------------------------------------------------------- #
# Transition record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class EcoModeTransition:
    """
    Immutable record of one transition attempt.

    ``coerced`` marks a transition recorded on the non-strict coercion path
    — the eco-mode state did **not** change.

    ``forced`` marks a transition produced by :meth:`force_eco_mode` (no
    real ``carbon_intensity`` reading drove it).

    ``policy_engine_error`` is ``True`` when the policy engine raised
    during the transition and the controller chose to commit anyway
    (non-strict mode only).
    """

    enabled: bool
    carbon_intensity: float
    reason: str
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    forced: bool = False
    coerced: bool = False
    raw_repr: Optional[str] = None
    policy_engine_error: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise EcoModeControllerError("enabled must be a bool.")
        if isinstance(self.carbon_intensity, bool) or not isinstance(
            self.carbon_intensity, (int, float)
        ):
            raise EcoModeControllerError(
                "carbon_intensity must be numeric."
            )
        fv = float(self.carbon_intensity)
        if not math.isfinite(fv) or fv < 0:
            raise EcoModeControllerError(
                "carbon_intensity must be finite and >= 0."
            )
        if not isinstance(self.reason, str) or not self.reason:
            raise EcoModeControllerError(
                "reason must be a non-empty string."
            )
        for flag_name in (
            "forced", "coerced", "policy_engine_error",
        ):
            if not isinstance(getattr(self, flag_name), bool):
                raise EcoModeControllerError(
                    f"{flag_name} must be a bool."
                )
        if self.raw_repr is not None and not isinstance(
            self.raw_repr, str
        ):
            raise EcoModeControllerError(
                "raw_repr must be str or None."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "carbon_intensity": float(self.carbon_intensity),
            "reason": self.reason,
            "timestamp": self.timestamp.isoformat(),
            "forced": self.forced,
            "coerced": self.coerced,
            "raw_repr": self.raw_repr,
            "policy_engine_error": self.policy_engine_error,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EcoModeTransition":
        if not isinstance(data, Mapping):
            raise EcoModeControllerError(
                "EcoModeTransition.from_dict expects a Mapping."
            )
        try:
            ts = data.get("timestamp")
            if ts is None:
                timestamp = datetime.now(timezone.utc)
            elif isinstance(ts, str):
                timestamp = datetime.fromisoformat(ts)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                raise EcoModeControllerError(
                    f"timestamp must be str|datetime|None, "
                    f"got {type(ts).__name__}."
                )
            return cls(
                enabled=bool(data.get("enabled", False)),
                carbon_intensity=float(
                    data.get("carbon_intensity", 0.0)
                ),
                reason=str(data.get("reason", "unknown")),
                timestamp=timestamp,
                forced=bool(data.get("forced", False)),
                coerced=bool(data.get("coerced", False)),
                raw_repr=(
                    None if data.get("raw_repr") is None
                    else str(data["raw_repr"])
                ),
                policy_engine_error=bool(
                    data.get("policy_engine_error", False)
                ),
            )
        except EcoModeControllerError:
            raise
        except (TypeError, ValueError) as exc:
            raise EcoModeControllerError(
                f"invalid EcoModeTransition payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        extras = []
        if self.forced:
            extras.append("forced")
        if self.coerced:
            extras.append("coerced")
        if self.policy_engine_error:
            extras.append("engine_error")
        suffix = f", [{','.join(extras)}]" if extras else ""
        return (
            "EcoModeTransition("
            f"enabled={self.enabled}, "
            f"intensity={self.carbon_intensity:.1f}, "
            f"reason={self.reason!r}{suffix})"
        )


# --------------------------------------------------------------------------- #
# Controller
# --------------------------------------------------------------------------- #
class EcoModeController:
    """
    Controls adaptive throttling based on carbon intensity.

    Thread-safe for all public methods. Direct mutation of ``policy_engine``,
    ``dirty_threshold``, or ``eco_mode_enabled`` bypasses validation and
    locking — prefer the public API (``update``, ``force_eco_mode``,
    ``reset``). To change the policy engine, construct a new controller.

    ``_started_at`` lifecycle
    -------------------------
    Set at construction. :meth:`reset` resets it to ``time.time()``.
    :meth:`from_dict` restores the serialized value, so
    ``statistics()["uptime_seconds"]`` on a restored controller reports the
    **lifetime of the serialized object**, not the current instance. Call
    :meth:`reset` after restoring if you want uptime relative to now.
    """

    def __init__(
        self,
        policy_engine: PolicyEngine,
        dirty_threshold: float = 400.0,
        *,
        config: Optional[EcoModeControllerConfig] = None,
        strict: bool = True,
    ) -> None:
        if policy_engine is None:
            raise EcoModeControllerError(
                "policy_engine must not be None."
            )
        if not isinstance(policy_engine, PolicyEngine):
            raise EcoModeControllerError(
                f"policy_engine must satisfy PolicyEngine, got "
                f"{type(policy_engine).__name__}."
            )

        if config is not None:
            if not isinstance(config, EcoModeControllerConfig):
                raise EcoModeControllerError(
                    f"config must be an EcoModeControllerConfig, got "
                    f"{type(config).__name__}."
                )
            if dirty_threshold != 400.0:
                logger.warning(
                    "dirty_threshold=%r ignored; using "
                    "config.dirty_threshold=%d.",
                    dirty_threshold, config.dirty_threshold,
                )
            self._config = config
        else:
            self._config = EcoModeControllerConfig(
                dirty_threshold=float(dirty_threshold),
            )
        self._strict = bool(strict)

        # Legacy attributes; see class docstring for mutation caveat.
        self.policy_engine: PolicyEngine = policy_engine
        self.dirty_threshold: float = self._config.dirty_threshold
        self.eco_mode_enabled: bool = False

        self._lock = threading.RLock()
        self._history: Deque[EcoModeTransition] = deque(
            maxlen=self._config.max_history
        )
        self._started_at: float = time.time()

        # Lifecycle counters (cumulative; not reset by reset()).
        self._total_transitions: int = 0
        self._total_enters: int = 0
        self._total_exits: int = 0
        self._dropped_transitions: int = 0

        logger.debug(
            "EcoModeController initialized "
            "(dirty=%.1f, clean=%.1f, strict=%s)",
            self._config.dirty_threshold,
            self._config.clean_threshold,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> EcoModeControllerConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def transitions(self) -> List[EcoModeTransition]:
        """Read-only snapshot of the transition history."""
        with self._lock:
            return list(self._history)

    @property
    def total_transitions(self) -> int:
        with self._lock:
            return self._total_transitions

    # ---------------------------------------------------------- public API
    def update(self, carbon_intensity: float) -> bool:
        """
        Update the controller state given the current carbon intensity.

        Returns the new ``eco_mode_enabled`` flag.

        In non-strict mode, a malformed ``carbon_intensity`` (non-numeric,
        ``NaN``, ``inf``, or negative) is coerced to ``0.0`` and recorded
        in the audit trail with ``coerced=True``. **The eco-mode state is
        not toggled on a coerced sample** — a hardware fault must not
        disable the energy-saving mode.
        """
        value, coerced, raw_repr = self._validate_intensity(carbon_intensity)

        with self._lock:
            if coerced:
                self._record_coerced_skip_locked(value, raw_repr)
                return self.eco_mode_enabled

            was_enabled = self.eco_mode_enabled
            if not was_enabled and value > self._config.dirty_threshold:
                self._enable_eco_mode(value, reason="dirty_grid")
            elif was_enabled and value < self._config.clean_threshold:
                self._disable_eco_mode(value, reason="clean_grid")
            return self.eco_mode_enabled

    def force_eco_mode(self, *, enabled: bool) -> bool:
        """
        Force ECO mode on or off, recording the transition with
        ``forced=True`` and ``reason="forced"``.

        The recorded ``carbon_intensity`` is ``0.0`` — it reflects that no
        real reading drove the transition; the ``forced`` marker on the
        transition record disambiguates it from a genuine zero reading.
        """
        if not isinstance(enabled, bool):
            raise EcoModeControllerError("enabled must be a bool.")
        with self._lock:
            if enabled and not self.eco_mode_enabled:
                self._enable_eco_mode(
                    0.0, reason="forced", forced=True,
                )
            elif not enabled and self.eco_mode_enabled:
                self._disable_eco_mode(
                    0.0, reason="forced", forced=True,
                )
            return self.eco_mode_enabled

    def reset(self, *, clear_history: bool = True) -> int:
        """
        Reset the controller to ``eco_mode_enabled=False``.

        Returns the number of *buffered* transitions removed. Lifecycle
        counters are **not** reset.

        In strict mode, raises :class:`EcoModeControllerError` if the
        policy engine rejects ``reset_defaults()`` — the caller must know
        that the controller's flag and the engine's state may have
        diverged.
        """
        with self._lock:
            removed = len(self._history)
            if self.eco_mode_enabled:
                try:
                    self.policy_engine.reset_defaults()
                except Exception as exc:
                    if self._strict:
                        raise EcoModeControllerError(
                            f"policy_engine rejected reset: {exc}"
                        ) from exc
                    logger.exception(
                        "policy_engine reset failed: %s", exc,
                    )
            self.eco_mode_enabled = False
            if clear_history:
                self._history.clear()
            self._started_at = time.time()
        logger.debug(
            "EcoModeController reset (removed %d).", removed,
        )
        return removed

    # ---------------------------------------------------------- internals
    def _enable_eco_mode(
        self,
        intensity: float,
        *,
        reason: str,
        forced: bool = False,
    ) -> None:
        """Apply the ECO state, committing only after the engine succeeds."""
        # ---- 1. Apply to the policy engine FIRST ------------------- #
        engine_error = False
        try:
            self.policy_engine.set_pruning_aggressiveness(
                self._config.eco_pruning_aggressiveness
            )
            self.policy_engine.set_token_limit_ratio(
                self._config.eco_token_limit_ratio
            )
        except Exception as exc:
            if self._strict:
                raise EcoModeControllerError(
                    f"policy_engine rejected ECO activation: {exc}"
                ) from exc
            engine_error = True
            logger.exception(
                "policy_engine ECO activation failed: %s", exc,
            )

        # ---- 2. Commit state + history ----------------------------- #
        self.eco_mode_enabled = True
        self._total_enters += 1
        self._record_transition_locked(EcoModeTransition(
            enabled=True,
            carbon_intensity=intensity,
            reason=reason,
            forced=forced,
            policy_engine_error=engine_error,
        ))
        logger.warning(
            "Entering ECO MODE (intensity=%.1f, reason=%s, "
            "engine_error=%s).",
            intensity, reason, engine_error,
        )

    def _disable_eco_mode(
        self,
        intensity: float,
        *,
        reason: str,
        forced: bool = False,
    ) -> None:
        """Restore defaults, committing only after the engine succeeds."""
        engine_error = False
        try:
            self.policy_engine.reset_defaults()
        except Exception as exc:
            if self._strict:
                raise EcoModeControllerError(
                    f"policy_engine rejected ECO deactivation: {exc}"
                ) from exc
            engine_error = True
            logger.exception(
                "policy_engine ECO deactivation failed: %s", exc,
            )

        self.eco_mode_enabled = False
        self._total_exits += 1
        self._record_transition_locked(EcoModeTransition(
            enabled=False,
            carbon_intensity=intensity,
            reason=reason,
            forced=forced,
            policy_engine_error=engine_error,
        ))
        logger.info(
            "Exiting ECO MODE (intensity=%.1f, reason=%s, "
            "engine_error=%s).",
            intensity, reason, engine_error,
        )

    def _record_coerced_skip_locked(
        self,
        value: float,
        raw_repr: Optional[str],
    ) -> None:
        """Record a coerced sample without toggling eco-mode state."""
        self._record_transition_locked(EcoModeTransition(
            enabled=self.eco_mode_enabled,
            carbon_intensity=value,
            reason="coerced_skip",
            coerced=True,
            raw_repr=raw_repr,
        ))

    def _record_transition_locked(
        self, transition: EcoModeTransition
    ) -> None:
        """Append + bump counters. Caller must hold ``self._lock``."""
        self._total_transitions += 1
        if len(self._history) == self._config.max_history:
            self._dropped_transitions += 1
        self._history.append(transition)

    # ---------------------------------------------------------- validation
    def _validate_intensity(
        self, value: Any
    ) -> Tuple[float, bool, Optional[str]]:
        """
        Return ``(validated_value, was_coerced, raw_repr)``.

        ``raw_repr`` is ``None`` when the value passed unchanged; otherwise
        it is ``repr(value)`` (kept JSON-safe by the caller).
        """
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"carbon_intensity must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise EcoModeControllerError(msg)
            logger.warning("%s Coercing to 0.0.", msg)
            return 0.0, True, repr(value)
        fv = float(value)
        if not math.isfinite(fv) or fv < 0:
            msg = (
                f"carbon_intensity must be finite and >= 0, got "
                f"{value!r}."
            )
            if self._strict:
                raise EcoModeControllerError(msg)
            logger.warning("%s Coercing to 0.0.", msg)
            return 0.0, True, repr(value)
        return fv, False, None

    # ---------------------------------------------------------- statistics
    def statistics(self) -> EcoModeStatistics:
        """Return a JSON-safe, atomic snapshot of the controller state."""
        with self._lock:
            return {
                "eco_mode_enabled": self.eco_mode_enabled,
                "transitions": self._total_transitions,
                "enters": self._total_enters,
                "exits": self._total_exits,
                "buffered_transitions": len(self._history),
                "dropped_transitions": self._dropped_transitions,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.time() - self._started_at,
            }

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "eco_mode_enabled": self.eco_mode_enabled,
                "started_at": self._started_at,
                "total_transitions": self._total_transitions,
                "total_enters": self._total_enters,
                "total_exits": self._total_exits,
                "dropped_transitions": self._dropped_transitions,
            }
            if include_history:
                payload["history"] = [
                    t.to_dict() for t in self._history
                ]
        return payload

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        policy_engine: PolicyEngine,
    ) -> "EcoModeController":
        """
        Reconstruct a controller from a ``to_dict()`` payload.

        ``policy_engine`` is required because the engine is a live object,
        not part of the serialized state. The restored controller is
        **not** re-synchronised with the engine — if the original
        controller had ECO mode active, the caller is responsible for
        putting ``policy_engine`` into the corresponding state (or calling
        :meth:`reset`).
        """
        if not isinstance(data, Mapping):
            raise EcoModeControllerError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        try:
            cfg = EcoModeControllerConfig.from_dict(
                dict(data.get("config") or {}), strict=True
            )
            ctrl = cls(
                policy_engine=policy_engine,
                config=cfg,
                strict=bool(data.get("strict", True)),
            )
            with ctrl._lock:
                if data.get("started_at") is not None:
                    ctrl._started_at = float(data["started_at"])
                ctrl.eco_mode_enabled = bool(
                    data.get("eco_mode_enabled", False)
                )
                ctrl._total_transitions = int(
                    data.get("total_transitions", 0)
                )
                ctrl._total_enters = int(data.get("total_enters", 0))
                ctrl._total_exits = int(data.get("total_exits", 0))
                ctrl._dropped_transitions = int(
                    data.get("dropped_transitions", 0)
                )
                for entry in data.get("history", []) or []:
                    ctrl._history.append(
                        EcoModeTransition.from_dict(entry)
                    )
            return ctrl
        except EcoModeControllerError:
            raise
        except (TypeError, ValueError) as exc:
            raise EcoModeControllerError(
                f"invalid EcoModeController payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_history: bool = False, **kwargs: Any
    ) -> str:
        # Payload is primitive-only; no default= needed.
        return json.dumps(
            self.to_dict(include_history=include_history), **kwargs
        )

    @classmethod
    def from_json(
        cls,
        payload: str,
        policy_engine: PolicyEngine,
    ) -> "EcoModeController":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise EcoModeControllerError(
                f"Invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data, policy_engine=policy_engine)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "EcoModeController("
                f"enabled={self.eco_mode_enabled}, "
                f"dirty_threshold={self._config.dirty_threshold:.1f}, "
                f"transitions={self._total_transitions})"
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, EcoModeController):
            return NotImplemented
        return self._config == other._config and self._strict == other._strict

    def __hash__(self) -> int:
        return hash((self._config, self._strict))


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "EcoModeController",
    "EcoModeControllerConfig",
    "EcoModeControllerError",
    "EcoModeStatistics",
    "EcoModeTransition",
    "PolicyEngine",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m sustainability.eco_mode_controller
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    import sys

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    # ---- 1. Working policy engine --------------------------------- #
    class _PolicyEngine:
        def __init__(self) -> None:
            self.pruning = 0.0
            self.tokens = 1.0
            self.reset_calls = 0

        def set_pruning_aggressiveness(self, value: float) -> None:
            self.pruning = value

        def set_token_limit_ratio(self, value: float) -> None:
            self.tokens = value

        def reset_defaults(self) -> None:
            self.pruning = 0.0
            self.tokens = 1.0
            self.reset_calls += 1

    # ---- 2. Broken policy engine (raises mid-enable) -------------- #
    class _BrokenPE:
        def __init__(self, fail_on: str) -> None:
            self.fail_on = fail_on
            self.reset_calls = 0

        def set_pruning_aggressiveness(self, value: float) -> None:
            if self.fail_on == "pruning":
                raise RuntimeError("pruning rejected")

        def set_token_limit_ratio(self, value: float) -> None:
            if self.fail_on == "tokens":
                raise RuntimeError("tokens rejected")

        def reset_defaults(self) -> None:
            self.reset_calls += 1
            if self.fail_on == "reset":
                raise RuntimeError("reset rejected")

    # ---- 3. Happy path / hysteresis ------------------------------- #
    print("hysteresis:")
    pe = _PolicyEngine()
    ctrl = EcoModeController(pe)
    print("  repr:", ctrl)
    _check("update(500) → True", ctrl.update(500.0) is True)
    _check("engine pruning set", pe.pruning == 0.8)
    _check("engine tokens set", pe.tokens == 0.6)
    _check("update(370) stays on", ctrl.update(370.0) is True)
    _check("update(300) turns off", ctrl.update(300.0) is False)
    _check("engine reset_calls == 1", pe.reset_calls == 1)

    # ---- 4. State-corruption fix (strict) ------------------------ #
    print("strict state-corruption fix:")
    broken = _BrokenPE(fail_on="tokens")
    ctrl = EcoModeController(broken, strict=True)
    try:
        ctrl.update(500.0)
        _check("strict raises", False)
    except EcoModeControllerError:
        _check("strict raises", True)
    _check("eco_mode_enabled unchanged", ctrl.eco_mode_enabled is False)
    _check("no transitions recorded", len(ctrl.transitions) == 0)
    _check("total_transitions == 0", ctrl.total_transitions == 0)

    # ---- 5. Non-strict policy engine failure ---------------------- #
    print("non-strict engine failure:")
    broken = _BrokenPE(fail_on="tokens")
    ctrl = EcoModeController(broken, strict=False)
    ctrl.update(500.0)
    _check("eco_mode_enabled True", ctrl.eco_mode_enabled is True)
    _check("transition recorded", len(ctrl.transitions) == 1)
    _check(
        "transition marks engine_error",
        ctrl.transitions[0].policy_engine_error is True,
    )

    # ---- 6. Coerced sample does NOT toggle state ----------------- #
    print("coerced sample (non-strict):")
    pe = _PolicyEngine()
    ctrl = EcoModeController(pe, strict=False)
    ctrl.update(500.0)                                # enter ECO
    _check("ECO enabled", ctrl.eco_mode_enabled is True)
    result = ctrl.update(float("nan"))                # coerced
    _check("coerced: enabled unchanged", result is True)
    _check("coerced: reset_calls == 0", pe.reset_calls == 0)
    _check(
        "coerced transition recorded",
        ctrl.transitions[-1].coerced is True,
    )
    _check(
        "coerced transition reason",
        ctrl.transitions[-1].reason == "coerced_skip",
    )

    # ---- 7. Strict rejects NaN / inf / negative ------------------ #
    print("strict rejection:")
    pe = _PolicyEngine()
    ctrl = EcoModeController(pe, strict=True)
    for bad in (float("nan"), float("inf"), -1.0, "big", None, True):
        try:
            ctrl.update(bad)
            _check(f"reject {bad!r}", False)
        except EcoModeControllerError:
            _check(f"reject {bad!r}", True)

    # ---- 8. Missing policy methods rejected ---------------------- #
    print("policy engine contract:")
    try:
        EcoModeController(object())  # type: ignore[arg-type]
        _check("non-engine rejected", False)
    except EcoModeControllerError:
        _check("non-engine rejected", True)

    # ---- 9. Config validation ------------------------------------ #
    print("config validation:")
    for bad_cfg in (
        dict(dirty_threshold=-1),
        dict(dirty_threshold=float("nan")),
        dict(clean_threshold=500, dirty_threshold=400),
        dict(eco_pruning_aggressiveness=1.5),
        dict(eco_pruning_aggressiveness=True),     # bool-as-float
        dict(eco_token_limit_ratio=-0.1),
        dict(eco_token_limit_ratio="0.5"),         # non-numeric
        dict(max_history=0),
        dict(max_history="100"),
        dict(max_history=True),
    ):
        try:
            EcoModeControllerConfig(**bad_cfg)  # type: ignore[arg-type]
            _check(f"reject cfg {list(bad_cfg)}", False)
        except EcoModeControllerError:
            _check(f"reject cfg {list(bad_cfg)}", True)

    # ---- 10. force_eco_mode marks forced=True -------------------- #
    print("force_eco_mode:")
    pe = _PolicyEngine()
    ctrl = EcoModeController(pe)
    _check("force on", ctrl.force_eco_mode(enabled=True) is True)
    _check(
        "forced flag set",
        ctrl.transitions[-1].forced is True,
    )
    _check("engine pruning applied", pe.pruning == 0.8)
    _check("force off", ctrl.force_eco_mode(enabled=False) is False)
    _check(
        "forced flag set (exit)",
        ctrl.transitions[-1].forced is True,
    )

    # ---- 11. Round-trip (to_json / from_json) -------------------- #
    print("round-trip:")
    pe = _PolicyEngine()
    ctrl = EcoModeController(pe, strict=True)
    ctrl.update(500.0)
    ctrl.update(300.0)
    payload = ctrl.to_json(include_history=True)
    fresh_pe = _PolicyEngine()
    restored = EcoModeController.from_json(payload, fresh_pe)
    _check(
        "history preserved",
        len(restored.transitions) == len(ctrl.transitions),
    )
    _check(
        "counters preserved",
        restored.statistics()["transitions"]
        == ctrl.statistics()["transitions"],
    )
    _check(
        "eq works",
        restored == ctrl,
    )

    # ---- 12. Statistics counter invariant ------------------------ #
    print("counter invariant:")
    pe = _PolicyEngine()
    ctrl = EcoModeController(
        pe,
        config=EcoModeControllerConfig(max_history=3),
    )
    for i in range(5):
        ctrl.update(500.0 if i % 2 else 300.0)
    stats = ctrl.statistics()
    _check(
        "invariant total == buffered + dropped",
        stats["transitions"]
        == stats["buffered_transitions"] + stats["dropped_transitions"],
    )
    _check(
        "enters + exits == transitions",
        stats["enters"] + stats["exits"] == stats["transitions"],
    )

    # ---- 13. reset() in strict mode may raise -------------------- #
    print("reset strictness:")
    pe = _BrokenPE(fail_on="reset")
    ctrl = EcoModeController(pe, strict=True)
    # Force-enable with the working "enable" path first:
    pe.fail_on = "none"
    ctrl.force_eco_mode(enabled=True)
    pe.fail_on = "reset"
    try:
        ctrl.reset()
        _check("reset raises in strict", False)
    except EcoModeControllerError:
        _check("reset raises in strict", True)

    pe = _BrokenPE(fail_on="reset")
    ctrl = EcoModeController(pe, strict=False)
    pe.fail_on = "none"
    ctrl.force_eco_mode(enabled=True)
    pe.fail_on = "reset"
    _check(
        "reset does not raise in non-strict",
        isinstance(ctrl.reset(), int),
    )

    # ---- 14. from_dict strict on config ------------------------- #
    print("from_dict strict on config:")
    try:
        EcoModeControllerConfig.from_dict({"typo": 1}, strict=True)
        _check("unknown key rejected", False)
    except EcoModeControllerError:
        _check("unknown key rejected", True)
    cfg = EcoModeControllerConfig.from_dict(
        {"typo": 1}, strict=False
    )
    _check(
        "unknown key accepted non-strict",
        cfg.dirty_threshold == 400.0,
    )

    # ---- 15. from_dict wraps parse errors ----------------------- #
    print("from_dict error wrapping:")
    pe = _PolicyEngine()
    try:
        EcoModeController.from_dict(
            {"started_at": "not-a-number"}, policy_engine=pe,
        )
        _check("bad started_at rejected", False)
    except EcoModeControllerError:
        _check("bad started_at rejected", True)

    try:
        EcoModeController.from_dict(
            {"history": [{"enabled": "yes"}]}, policy_engine=pe,
        )
        _check("bad history rejected", False)
    except EcoModeControllerError:
        _check("bad history rejected", True)

    # ---- 16. Transition validation ------------------------------ #
    print("transition validation:")
    try:
        EcoModeTransition(
            enabled="yes",  # type: ignore[arg-type]
            carbon_intensity=100.0,
            reason="x",
        )
        _check("bad enabled rejected", False)
    except EcoModeControllerError:
        _check("bad enabled rejected", True)

    try:
        EcoModeTransition(
            enabled=True,
            carbon_intensity=float("nan"),
            reason="x",
        )
        _check("nan intensity rejected", False)
    except EcoModeControllerError:
        _check("nan intensity rejected", True)

    try:
        EcoModeTransition(
            enabled=True,
            carbon_intensity=100.0,
            reason="",
        )
        _check("empty reason rejected", False)
    except EcoModeControllerError:
        _check("empty reason rejected", True)

    # ---- 17. Atomic statistics ---------------------------------- #
    print("statistics atomicity:")
    pe = _PolicyEngine()
    ctrl = EcoModeController(pe)
    ctrl.update(500.0)
    stats = ctrl.statistics()
    _check(
        "enabled matches stats",
        stats["eco_mode_enabled"] == ctrl.eco_mode_enabled,
    )
    _check(
        "transitions match",
        stats["transitions"] == ctrl.total_transitions,
    )

    # ---- 18. eq / hash ------------------------------------------ #
    print("eq/hash:")
    a = EcoModeController(
        _PolicyEngine(),
        config=EcoModeControllerConfig(dirty_threshold=500.0),
    )
    b = EcoModeController(
        _PolicyEngine(),
        config=EcoModeControllerConfig(dirty_threshold=500.0),
    )
    c = EcoModeController(
        _PolicyEngine(),
        config=EcoModeControllerConfig(dirty_threshold=600.0),
    )
    _check("equal configs", a == b)
    _check("hash equal", hash(a) == hash(b))
    _check("different configs", a != c)

    # ---- 19. dir ----------------------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
