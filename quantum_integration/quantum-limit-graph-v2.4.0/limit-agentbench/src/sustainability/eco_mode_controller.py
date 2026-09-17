# src/sustainability/eco_mode_controller.py

"""
Eco Mode Controller
===================

Controls adaptive throttling based on carbon intensity. When the grid
becomes dirty, the controller engages ECO mode via a policy engine; when
the grid recovers, it disables it.

Enhancements
------------
- ``EcoModeControllerConfig`` — frozen, validated: thresholds, aggressiveness,
  token ratio, hysteresis.
- **`PolicyEngine` protocol** — the collaborator interface is now explicit.
- **Full validation** of `carbon_intensity` and of the collaborator interface.
- **Hysteresis** — a value oscillating around the threshold does not toggle
  the mode every call.
- **Bounded transition history** — every enter/exit is recorded.
- **Thread safety** — `RLock` guards state and history.
- **`statistics()` / `reset()` / `__repr__`**.
- Serialization: `to_dict` / `from_dict` / `to_json`.
- Custom `EcoModeControllerError(ValueError)`, lazy `%s` logging, and a
  `__main__` smoke test.
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
from typing import Any, Deque, Dict, List, Mapping, Optional, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EcoModeControllerError(ValueError):
    """Raised for invalid eco-mode controller inputs or configuration."""


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
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EcoModeControllerConfig:
    """Tunable parameters for :class:`EcoModeController`."""

    dirty_threshold: float = 400.0
    clean_threshold: float = 350.0  # hysteresis: leave ECO below this

    eco_pruning_aggressiveness: float = 0.8
    eco_token_limit_ratio: float = 0.6

    max_history: int = 1_000

    def __post_init__(self) -> None:
        for name in ("dirty_threshold", "clean_threshold"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise EcoModeControllerError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv) or fv < 0:
                raise EcoModeControllerError(
                    f"{name} must be finite and >= 0."
                )
        if self.clean_threshold >= self.dirty_threshold:
            raise EcoModeControllerError(
                "clean_threshold must be < dirty_threshold (hysteresis)."
            )
        for name in (
            "eco_pruning_aggressiveness", "eco_token_limit_ratio",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise EcoModeControllerError(f"{name} must be in [0, 1].")
        if self.max_history <= 0:
            raise EcoModeControllerError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EcoModeControllerConfig":
        if not isinstance(data, Mapping):
            raise EcoModeControllerError(
                "EcoModeControllerConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Transition record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EcoModeTransition:
    """Immutable record of one enter/exit transition."""

    enabled: bool
    carbon_intensity: float
    reason: str
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "carbon_intensity": self.carbon_intensity,
            "reason": self.reason,
            "timestamp": self.timestamp.isoformat(),
        }

    def __repr__(self) -> str:
        return (
            "EcoModeTransition("
            f"enabled={self.enabled}, "
            f"intensity={self.carbon_intensity:.1f}, "
            f"reason={self.reason!r})"
        )


# --------------------------------------------------------------------------- #
# Controller
# --------------------------------------------------------------------------- #
class EcoModeController:
    """
    Controls adaptive throttling based on carbon intensity.

    The original API (``policy_engine``, ``dirty_threshold``,
    ``eco_mode_enabled``, ``update``) is preserved; new parameters are
    keyword-only.
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
            raise EcoModeControllerError("policy_engine must not be None.")
        for method_name in (
            "set_pruning_aggressiveness",
            "set_token_limit_ratio",
            "reset_defaults",
        ):
            if not callable(getattr(policy_engine, method_name, None)):
                raise EcoModeControllerError(
                    f"policy_engine must expose '{method_name}'."
                )

        if config is not None:
            self._config = config
        else:
            self._config = EcoModeControllerConfig(
                dirty_threshold=float(dirty_threshold),
            )
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.policy_engine: PolicyEngine = policy_engine
        self.dirty_threshold: float = self._config.dirty_threshold
        self.eco_mode_enabled: bool = False

        self._lock = threading.RLock()
        self._history: Deque[EcoModeTransition] = deque(
            maxlen=self._config.max_history
        )
        self._started_at: float = time.time()

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
    def transitions(self) -> List[EcoModeTransition]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- public API
    def update(self, carbon_intensity: float) -> bool:
        """
        Update the controller state given the current carbon intensity.

        Returns the new ``eco_mode_enabled`` flag. Uses hysteresis: engages
        ECO mode above ``dirty_threshold``, leaves it below
        ``clean_threshold``.
        """
        value = self._validate_intensity(carbon_intensity)

        with self._lock:
            was_enabled = self.eco_mode_enabled
            if not was_enabled and value > self._config.dirty_threshold:
                self._enable_eco_mode(value, reason="dirty_grid")
            elif was_enabled and value < self._config.clean_threshold:
                self._disable_eco_mode(value, reason="clean_grid")
            return self.eco_mode_enabled

    def force_eco_mode(self, *, enabled: bool) -> bool:
        """Force ECO mode on or off, recording the transition."""
        with self._lock:
            if enabled and not self.eco_mode_enabled:
                self._enable_eco_mode(0.0, reason="forced")
            elif not enabled and self.eco_mode_enabled:
                self._disable_eco_mode(0.0, reason="forced")
            return self.eco_mode_enabled

    # ---------------------------------------------------------- internals
    def _enable_eco_mode(self, intensity: float, *, reason: str) -> None:
        self.eco_mode_enabled = True
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
            logger.warning("policy_engine ECO activation failed: %s", exc)

        record = EcoModeTransition(
            enabled=True, carbon_intensity=intensity, reason=reason,
        )
        self._history.append(record)
        logger.warning(
            "Entering ECO MODE (intensity=%.1f, reason=%s).",
            intensity, reason,
        )

    def _disable_eco_mode(self, intensity: float, *, reason: str) -> None:
        self.eco_mode_enabled = False
        try:
            self.policy_engine.reset_defaults()
        except Exception as exc:
            if self._strict:
                raise EcoModeControllerError(
                    f"policy_engine rejected ECO deactivation: {exc}"
                ) from exc
            logger.warning("policy_engine ECO deactivation failed: %s", exc)

        record = EcoModeTransition(
            enabled=False, carbon_intensity=intensity, reason=reason,
        )
        self._history.append(record)
        logger.info(
            "Exiting ECO MODE (intensity=%.1f, reason=%s).",
            intensity, reason,
        )

    # ---------------------------------------------------------- validation
    def _validate_intensity(self, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"carbon_intensity must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise EcoModeControllerError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv) or fv < 0:
            msg = (
                f"carbon_intensity must be finite and >= 0, got {value!r}."
            )
            if self._strict:
                raise EcoModeControllerError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        return fv

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_history: bool = True) -> int:
        """Reset the controller. Returns the number of transitions removed."""
        with self._lock:
            removed = len(self._history)
            if self.eco_mode_enabled:
                try:
                    self.policy_engine.reset_defaults()
                except Exception:
                    logger.exception("policy_engine reset failed.")
            self.eco_mode_enabled = False
            if clear_history:
                self._history.clear()
            self._started_at = time.time()
        logger.debug("EcoModeController reset (removed %d).", removed)
        return removed

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            history = list(self._history)
        enters = sum(1 for t in history if t.enabled)
        exits = len(history) - enters
        return {
            "eco_mode_enabled": self.eco_mode_enabled,
            "transitions": len(history),
            "enters": enters,
            "exits": exits,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "uptime_seconds": time.time() - self._started_at,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "eco_mode_enabled": self.eco_mode_enabled,
                "started_at": self._started_at,
            }
            if include_history:
                payload["history"] = [t.to_dict() for t in self._history]
        return payload

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "EcoModeController("
                f"enabled={self.eco_mode_enabled}, "
                f"dirty_threshold={self.dirty_threshold:.1f}, "
                f"transitions={len(self._history)})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "EcoModeController",
    "EcoModeControllerConfig",
    "EcoModeControllerError",
    "EcoModeTransition",
    "PolicyEngine",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m sustainability.eco_mode_controller
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

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

    pe = _PolicyEngine()
    ctrl = EcoModeController(pe)
    print("repr       :", ctrl)

    # ---- Enter ECO mode -------------------------------------------- #
    assert ctrl.update(500.0) is True
    assert pe.pruning == 0.8 and pe.tokens == 0.6
    print("enter      : enabled")

    # ---- Stay in ECO mode (hysteresis) ----------------------------- #
    assert ctrl.update(370.0) is True  # between clean=350 and dirty=400
    print("hysteresis : still enabled")

    # ---- Exit ECO mode --------------------------------------------- #
    assert ctrl.update(300.0) is False
    assert pe.reset_calls == 1
    print("exit       : disabled")

    # ---- Bug fix: NaN / inf / negative ---------------------------- #
    for bad in (float("nan"), float("inf"), -1.0, "big"):
        try:
            ctrl.update(bad)
        except EcoModeControllerError as exc:
            print("Rejected   :", exc)

    # ---- Bug fix: missing policy methods -------------------------- #
    try:
        EcoModeController(object())  # type: ignore[arg-type]
    except EcoModeControllerError as exc:
        print("Rejected   :", exc)

    # ---- Config validation ---------------------------------------- #
    for bad_cfg in (
        dict(dirty_threshold=-1),
        dict(clean_threshold=500, dirty_threshold=400),
        dict(eco_pruning_aggressiveness=1.5),
        dict(eco_token_limit_ratio=-0.1),
        dict(max_history=0),
    ):
        try:
            EcoModeControllerConfig(**bad_cfg)  # type: ignore[arg-type]
        except EcoModeControllerError as exc:
            print("Rejected cfg:", exc)

    # ---- Serialization ------------------------------------------- #
    print("serialization:", json.dumps(ctrl.to_dict(), default=str)[:80], "...")

    print("statistics :", {
        k: v for k, v in ctrl.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    print("\nSmoke test passed.")
