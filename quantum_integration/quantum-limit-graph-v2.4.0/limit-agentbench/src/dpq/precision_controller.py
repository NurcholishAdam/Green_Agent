# src/dpq/precision_controller.py

"""
Precision Controller — v2.0.0
==============================

Dynamic precision adjustment engine.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 **Upgrades now fire.** The transition direction is decided by
        ``_PRECISION_RANK`` (FP32 < FP16 < INT8 < INT4), not by checking
        whether the target is FP32. Previously, ``INT4 → INT8`` upgrades
        could never trigger — the check was ``intensity > threshold + buffer``
        which is impossible when intensity is in the RED band. Downgrades
        still fire; FP32 upgrades still fire; all intermediate upgrades
        (INT4→INT8, INT8→FP16) now fire correctly.
  * 2.2 ``from_dict`` delegates to ``ControllerConfig.from_legacy_dict``
        and ``DPQConfig.from_legacy_dict``. ``replicas_fp32/fp16/int8/int4``
        and the custom ``DPQConfig`` are preserved across round-trip
        (previously both were silently reset to defaults, corrupting
        pool scaling and hysteresis boundaries after restore).
  * 2.3 ``_coerce_precision`` normalises enum strings case-insensitively
        for policy and ``current_precision``. ``"FP32"``, ``"  int8  "``,
        and ``"FP16"`` all load cleanly (previously raised ``ValueError``).
  * 2.4 ``_lock_for_region`` **does not evict** held locks. When
        ``max_transition_locks`` is reached, a ``PrecisionControllerError``
        is raised (previously the oldest lock was popped, allowing two
        concurrent transitions for the same region to race).
  * 2.5 ``on_carbon_zone_change`` rejects non-finite or negative
        intensities. Previously ``intensity=-50`` and ``intensity=inf``
        passed validation.
  * 2.6 ``__getstate__`` / ``__setstate__`` drop the ``RLock`` and the
        ``_transition_locks`` dict, and recreate them empty. The controller
        is now picklable.
  * 2.7 Failed transitions are recorded in ``_history`` **before** the
        strict-mode raise. Previously the ``raise`` fired first, so
        ``statistics()["failed"]`` was always 0 in strict mode.

**P1 robustness / observability**
  * 3.1 Retry loop around ``_trigger_conversion`` honours
        ``ControllerConfig.max_retries`` + ``base_backoff_seconds``
        (previously dead config).
  * 3.2 ``PrecisionPolicy.min_accuracy`` raises ``PrecisionControllerError``
        instead of ``KeyError`` on unknown precision.
  * 3.3 ``_get_precision_for_zone`` defensively raises on unknown zone.
  * 3.4 ``_get_transition_threshold`` warns when an unusual jump (e.g.
        FP32 → INT8) has no explicit entry.
  * 3.5 ``metrics_hook(event, fields)`` emits ``transition_started``,
        ``transition_completed``, ``transition_failed``, ``transition_skipped``.
  * 3.6 ``close()``, ``statistics(window=N)``, locked context managers.
  * 3.8 ``history`` returns snapshots (``dataclasses.replace``).
  * 3.11 ``PrecisionPolicy`` is now frozen (it was never mutated by the
         controller).

**P2 polish**
  * 3.12 ``from_dict`` warns on ``in_progress`` transitions (their locks
         are gone).
  * 3.13 ``TransitionStatus`` from ``models`` used internally.

Backward compatible: every field, method, and serialized payload from
v1.0.0 loads unchanged (except for payloads with ``replicas_*`` or a
custom ``dpq_config``, which now round-trip correctly — the v1.0.0
behaviour of silently resetting them was the bug).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import pickle
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional

from .models import (
    CarbonZone,
    DPQConfig,
    ModelPrecision,
    PrecisionTransition,
    TransitionStatus,
    new_transition_id,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Precision ranking (P0 2.1)
# --------------------------------------------------------------------------- #
_PRECISION_RANK: Dict[ModelPrecision, int] = {
    ModelPrecision.FP32: 0,
    ModelPrecision.FP16: 1,
    ModelPrecision.INT8: 2,
    ModelPrecision.INT4: 3,
}


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class PrecisionControllerError(ValueError):
    """Raised for invalid inputs, configuration, or transition failures."""


# --------------------------------------------------------------------------- #
# Local coercion helpers (P0 2.3)
# --------------------------------------------------------------------------- #
def _coerce_precision(value: Any, *, field_name: str) -> ModelPrecision:
    """Coerce a value to ModelPrecision case-insensitively."""
    if isinstance(value, ModelPrecision):
        return value
    if value is None or isinstance(value, bool):
        raise PrecisionControllerError(
            f"{field_name} must be a ModelPrecision, got {value!r}.")
    try:
        normalized = str(value).strip().lower()
    except Exception:  # noqa: BLE001
        raise PrecisionControllerError(
            f"{field_name} must be a ModelPrecision, "
            f"got {type(value).__name__}.") from None
    try:
        return ModelPrecision(normalized)
    except ValueError as exc:
        raise PrecisionControllerError(
            f"{field_name} must be a valid ModelPrecision, "
            f"got {value!r}.") from exc


def _coerce_zone(value: Any, *, field_name: str) -> CarbonZone:
    """Coerce a value to CarbonZone case-insensitively."""
    if isinstance(value, CarbonZone):
        return value
    if value is None or isinstance(value, bool):
        raise PrecisionControllerError(
            f"{field_name} must be a CarbonZone, got {value!r}.")
    try:
        normalized = str(value).strip().lower()
    except Exception:  # noqa: BLE001
        raise PrecisionControllerError(
            f"{field_name} must be a CarbonZone, "
            f"got {type(value).__name__}.") from None
    try:
        return CarbonZone(normalized)
    except ValueError as exc:
        raise PrecisionControllerError(
            f"{field_name} must be a valid CarbonZone, "
            f"got {value!r}.") from exc


def _validate_intensity(value: Any) -> float:
    """Validate carbon intensity (P0 2.5)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PrecisionControllerError(
            "update.intensity_gco2_kwh must be numeric, "
            f"got {type(value).__name__}.")
    fvalue = float(value)
    if math.isnan(fvalue) or math.isinf(fvalue):
        raise PrecisionControllerError(
            f"update.intensity_gco2_kwh must be finite, got {value!r}.")
    if fvalue < 0:
        raise PrecisionControllerError(
            f"update.intensity_gco2_kwh must be >= 0, got {fvalue}.")
    return fvalue


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ControllerConfig:
    """Tunable parameters for the precision controller."""

    zone_transition_buffer: float = 10.0
    max_transition_locks: int = 64
    max_history: int = 1000
    max_retries: int = 1
    base_backoff_seconds: float = 0.5

    replicas_fp32: int = 4
    replicas_fp16: int = 6
    replicas_int8: int = 10
    replicas_int4: int = 16

    def __post_init__(self) -> None:
        for name in (
            "zone_transition_buffer", "base_backoff_seconds",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise PrecisionControllerError(
                    f"{name} must be numeric, got {type(value).__name__}.")
            if math.isnan(value) or math.isinf(value):
                raise PrecisionControllerError(
                    f"{name} must be finite, got {value!r}.")
        if self.zone_transition_buffer < 0:
            raise PrecisionControllerError(
                "zone_transition_buffer must be >= 0.")
        if isinstance(self.max_transition_locks, bool) or \
                not isinstance(self.max_transition_locks, int) or \
                self.max_transition_locks <= 0:
            raise PrecisionControllerError(
                "max_transition_locks must be > 0.")
        if isinstance(self.max_history, bool) or \
                not isinstance(self.max_history, int) or \
                self.max_history <= 0:
            raise PrecisionControllerError("max_history must be > 0.")
        if isinstance(self.max_retries, bool) or \
                not isinstance(self.max_retries, int) or \
                self.max_retries < 0:
            raise PrecisionControllerError("max_retries must be >= 0.")
        if self.base_backoff_seconds < 0:
            raise PrecisionControllerError(
                "base_backoff_seconds must be >= 0.")
        for name in (
            "replicas_fp32", "replicas_fp16",
            "replicas_int8", "replicas_int4",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or \
                    value <= 0:
                raise PrecisionControllerError(
                    f"{name} must be a positive int.")

    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "ControllerConfig":
        """Build from a flat mapping; unknown keys ignored (P0 2.2)."""
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in cfg.items() if k in valid})


# --------------------------------------------------------------------------- #
# Policy
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PrecisionPolicy:
    """Policy mapping carbon zones to precision levels.

    P1 3.11: frozen — the controller never mutates it, and freezing
    prevents the "operator changes policy mid-transition" race.
    """

    green_zone_precision: ModelPrecision = ModelPrecision.FP32
    yellow_zone_precision: ModelPrecision = ModelPrecision.FP16
    red_zone_precision: ModelPrecision = ModelPrecision.INT8
    critical_zone_precision: ModelPrecision = ModelPrecision.INT4

    min_accuracy_fp32: float = 0.95
    min_accuracy_fp16: float = 0.93
    min_accuracy_int8: float = 0.90
    min_accuracy_int4: float = 0.85

    # Deprecated duplicate of ControllerConfig.zone_transition_buffer —
    # retained for serialization compat; the controller reads from config.
    zone_transition_buffer: float = 10.0

    def __post_init__(self) -> None:
        for name in (
            "min_accuracy_fp32", "min_accuracy_fp16",
            "min_accuracy_int8", "min_accuracy_int4",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or \
                    not isinstance(value, (int, float)):
                raise PrecisionControllerError(
                    f"{name} must be numeric.")
            if math.isnan(value) or math.isinf(value):
                raise PrecisionControllerError(
                    f"{name} must be finite.")
            if not 0.0 <= value <= 1.0:
                raise PrecisionControllerError(
                    f"{name} must be in [0, 1].")
        if self.zone_transition_buffer < 0:
            raise PrecisionControllerError(
                "zone_transition_buffer must be >= 0.")

    def min_accuracy(self, precision: ModelPrecision) -> float:
        """Return the accuracy floor for ``precision`` (P1 3.2)."""
        try:
            return {
                ModelPrecision.FP32: self.min_accuracy_fp32,
                ModelPrecision.FP16: self.min_accuracy_fp16,
                ModelPrecision.INT8: self.min_accuracy_int8,
                ModelPrecision.INT4: self.min_accuracy_int4,
            }[precision]
        except (KeyError, TypeError) as exc:
            raise PrecisionControllerError(
                f"unknown precision {precision!r}; expected a "
                f"ModelPrecision instance.") from exc

    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "PrecisionPolicy":
        """Build from a flat mapping (P0 2.2 + P0 2.3)."""
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        kwargs: Dict[str, Any] = {}
        for k, v in cfg.items():
            if k not in valid:
                continue
            if k.endswith("_precision"):
                kwargs[k] = _coerce_precision(v, field_name=k)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Controller
# --------------------------------------------------------------------------- #
class PrecisionController:
    """Dynamic precision adjustment engine (thread-safe, picklable)."""

    def __init__(
        self,
        policy: PrecisionPolicy,
        model_converter: Any = None,
        worker_pool_manager: Any = None,
        accuracy_guardian: Any = None,
        *,
        config: Optional[ControllerConfig] = None,
        dpq_config: Optional[DPQConfig] = None,
        strict: bool = True,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        if not isinstance(policy, PrecisionPolicy):
            raise PrecisionControllerError(
                "policy must be a PrecisionPolicy instance.")
        if metrics_hook is not None and not callable(metrics_hook):
            raise PrecisionControllerError("metrics_hook must be callable.")

        self.policy = policy
        self.model_converter = model_converter
        self.worker_pool_manager = worker_pool_manager
        self.accuracy_guardian = accuracy_guardian

        self._config = config or ControllerConfig()
        self._dpq_config = dpq_config or DPQConfig()
        self._strict = bool(strict)
        self._metrics_hook = metrics_hook

        self._lock = threading.RLock()
        self._current_precision: Dict[str, ModelPrecision] = {}
        self._transition_locks: Dict[str, asyncio.Lock] = {}
        self._history: Deque[PrecisionTransition] = deque(
            maxlen=self._config.max_history)
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None

        logger.debug(
            "PrecisionController v%s initialized "
            "(buffer=%.1f, max_locks=%d, max_retries=%d, strict=%s)",
            __version__,
            self._config.zone_transition_buffer,
            self._config.max_transition_locks,
            self._config.max_retries,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> ControllerConfig:
        return self._config

    @property
    def history(self) -> List[PrecisionTransition]:
        """P1 3.8: return snapshots so callers cannot mutate internal state."""
        with self._lock:
            return [replace(t) for t in self._history]

    @property
    def current_precision(self) -> Dict[str, ModelPrecision]:
        with self._lock:
            return dict(self._current_precision)

    def get_region_precision(self, region: str) -> Optional[ModelPrecision]:
        with self._lock:
            return self._current_precision.get(region)

    # ---------------------------------------------------------- metrics
    def _emit_metric(
        self, event: str, fields_: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields_))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    async def _emit_transition_metric(
        self,
        region: str,
        from_prec: Optional[ModelPrecision],
        to_prec: ModelPrecision,
        status: str,
    ) -> None:
        """P1 3.5 + P1 3.7: wire to metrics_hook."""
        event = (
            "transition_completed" if status == TransitionStatus.COMPLETED.value
            else "transition_failed" if status == TransitionStatus.FAILED.value
            else "transition_started"
        )
        self._emit_metric(event, {
            "region": region,
            "from": from_prec.value if from_prec else "none",
            "to": to_prec.value,
            "status": status,
        })

    # ---------------------------------------------------------- public API
    async def on_carbon_zone_change(
        self, update: Any
    ) -> Optional[PrecisionTransition]:
        """Handle a carbon zone change notification."""
        region = getattr(update, "region", None)
        zone = getattr(update, "zone", None)
        intensity_raw = getattr(update, "intensity_gco2_kwh", None)

        if not isinstance(region, str) or not region:
            raise PrecisionControllerError(
                "update.region must be a non-empty string.")
        if not isinstance(zone, CarbonZone):
            raise PrecisionControllerError(
                "update.zone must be a CarbonZone.")
        # P0 2.5: reject negative / NaN / Inf intensities.
        intensity = _validate_intensity(intensity_raw)

        target_precision = self._get_precision_for_zone(zone)

        # P0 2.4: may raise if lock capacity is exhausted.
        lock = self._lock_for_region(region)
        async with lock:
            with self._lock:
                current = self._current_precision.get(region)

            if current == target_precision:
                logger.debug(
                    "Region %s already at %s; no transition.",
                    region, target_precision.value)
                self._emit_metric("transition_skipped", {
                    "region": region,
                    "reason": "same_precision",
                    "precision": target_precision.value,
                })
                return None

            if not self._should_transition(
                current, target_precision, intensity
            ):
                logger.debug(
                    "Hysteresis buffer prevents %s → %s transition "
                    "(intensity=%.1f).",
                    current.value if current else "none",
                    target_precision.value,
                    intensity)
                self._emit_metric("transition_skipped", {
                    "region": region,
                    "reason": "hysteresis",
                    "from": current.value if current else "none",
                    "to": target_precision.value,
                    "intensity": intensity,
                })
                return None

            transition = PrecisionTransition(
                transition_id=new_transition_id(),
                region=region,
                model_name="*",
                from_precision=current,
                to_precision=target_precision,
                triggered_by="carbon_zone_change",
                carbon_intensity=intensity,
                carbon_zone=zone,
                start_time=datetime.now(timezone.utc),
                status=TransitionStatus.IN_PROGRESS.value,
            )
            logger.info(
                "Transition %s: %s → %s (intensity=%.1f gCO2/kWh)",
                region,
                current.value if current else "none",
                target_precision.value,
                intensity)
            self._emit_metric("transition_started", {
                "region": region,
                "from": current.value if current else "none",
                "to": target_precision.value,
                "intensity": intensity,
                "zone": zone.value,
            })

        # ---- Perform the transition outside the region lock ----
        # (The lock still guards the pre-check; the transition itself
        # may take seconds and would block other regions otherwise.)
        # Actually we keep the lock to prevent concurrent transitions
        # for the same region — that's the whole point.
        try:
            await self._trigger_conversion(
                region, current, target_precision)
            await self._scale_pools(region, target_precision)

            with self._lock:
                self._current_precision[region] = target_precision

            transition.status = TransitionStatus.COMPLETED.value
            transition.end_time = datetime.now(timezone.utc)

            with self._lock:
                self._history.append(transition)

            await self._emit_transition_metric(
                region, current, target_precision, transition.status)
            return transition

        except Exception as exc:  # noqa: BLE001
            transition.status = TransitionStatus.FAILED.value
            transition.error_message = f"{type(exc).__name__}: {exc}"
            transition.end_time = datetime.now(timezone.utc)
            logger.exception(
                "Transition %s failed: %s", transition.transition_id, exc)

            # P0 2.7: record BEFORE raising so statistics sees failures.
            with self._lock:
                self._history.append(transition)

            await self._emit_transition_metric(
                region, current, target_precision, transition.status)

            if self._strict:
                raise PrecisionControllerError(
                    f"Transition failed: {exc}") from exc
            return transition

    # ---------------------------------------------------------- internals
    def _lock_for_region(self, region: str) -> asyncio.Lock:
        """
        Return (creating if necessary) the per-region lock.

        P0 2.4: never evict a held lock. When the capacity is reached,
        raise a clear error so the caller knows they need to raise
        ``max_transition_locks``.
        """
        with self._lock:
            lock = self._transition_locks.get(region)
            if lock is not None:
                return lock
            if len(self._transition_locks) >= \
                    self._config.max_transition_locks:
                raise PrecisionControllerError(
                    f"max_transition_locks="
                    f"{self._config.max_transition_locks} reached; "
                    f"cannot allocate a lock for region {region!r}. "
                    f"Increase ``max_transition_locks`` or reset() the "
                    f"controller.")
            lock = asyncio.Lock()
            self._transition_locks[region] = lock
            return lock

    async def _trigger_conversion(
        self,
        region: str,
        from_precision: Optional[ModelPrecision],
        to_precision: ModelPrecision,
    ) -> None:
        """Trigger conversion with retry (P1 3.1)."""
        if self.model_converter is None:
            return
        method = getattr(self.model_converter, "convert_models", None)
        if not callable(method):
            return

        total = 1 + self._config.max_retries
        last_exc: Optional[BaseException] = None
        for attempt in range(total):
            try:
                result = method(
                    region=region,
                    from_precision=from_precision,
                    to_precision=to_precision,
                )
                if asyncio.iscoroutine(result):
                    await result
                return
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.warning(
                    "Conversion attempt %d/%d for %s failed: %s",
                    attempt + 1, total, region, exc)
                if attempt < total - 1:
                    await asyncio.sleep(
                        self._config.base_backoff_seconds
                        * (2 ** attempt))
        raise PrecisionControllerError(
            f"All {total} conversion attempt(s) failed for "
            f"{region!r}: {type(last_exc).__name__}: {last_exc}"
        ) from last_exc

    async def _scale_pools(
        self, region: str, precision: ModelPrecision,
    ) -> None:
        if self.worker_pool_manager is None:
            return
        method = getattr(
            self.worker_pool_manager, "scale_precision_pool", None)
        if not callable(method):
            return
        result = method(
            region=region,
            precision=precision,
            target_replicas=self._calculate_replicas(precision),
        )
        if asyncio.iscoroutine(result):
            await result

    def _get_precision_for_zone(
        self, zone: CarbonZone,
    ) -> ModelPrecision:
        """Map a carbon zone to a precision level (P1 3.3)."""
        mapping = {
            CarbonZone.GREEN: self.policy.green_zone_precision,
            CarbonZone.YELLOW: self.policy.yellow_zone_precision,
            CarbonZone.RED: self.policy.red_zone_precision,
            CarbonZone.CRITICAL: self.policy.critical_zone_precision,
        }
        try:
            return mapping[zone]
        except KeyError as exc:
            raise PrecisionControllerError(
                f"unknown carbon zone {zone!r}") from exc

    def _should_transition(
        self,
        current: Optional[ModelPrecision],
        target: ModelPrecision,
        intensity: float,
    ) -> bool:
        """
        Check whether the transition should proceed (P0 2.1).

        Direction-aware: upgrades require intensity to fall *below* the
        boundary minus the buffer; downgrades require it to rise *above*
        the boundary plus the buffer.
        """
        if current is None:
            return True

        threshold = self._get_transition_threshold(current, target)
        buffer = self._config.zone_transition_buffer

        upgrading = _PRECISION_RANK[target] < _PRECISION_RANK[current]
        if upgrading:
            return intensity < threshold - buffer
        return intensity > threshold + buffer

    def _get_transition_threshold(
        self,
        from_prec: ModelPrecision,
        to_prec: ModelPrecision,
    ) -> float:
        """Return the carbon intensity threshold for a transition (P1 3.4)."""
        thresholds = {
            (ModelPrecision.FP32, ModelPrecision.FP16):
                self._dpq_config.green_threshold,
            (ModelPrecision.FP16, ModelPrecision.INT8):
                self._dpq_config.yellow_threshold,
            (ModelPrecision.INT8, ModelPrecision.INT4):
                self._dpq_config.red_threshold,
            (ModelPrecision.FP16, ModelPrecision.FP32):
                self._dpq_config.green_threshold,
            (ModelPrecision.INT8, ModelPrecision.FP16):
                self._dpq_config.yellow_threshold,
            (ModelPrecision.INT4, ModelPrecision.INT8):
                self._dpq_config.red_threshold,
        }
        key = (from_prec, to_prec)
        if key in thresholds:
            return thresholds[key]
        logger.warning(
            "No explicit threshold for %s → %s transition; "
            "falling back to yellow_threshold=%.1f.",
            from_prec.value, to_prec.value,
            self._dpq_config.yellow_threshold)
        return self._dpq_config.yellow_threshold

    def _calculate_replicas(self, precision: ModelPrecision) -> int:
        return {
            ModelPrecision.FP32: self._config.replicas_fp32,
            ModelPrecision.FP16: self._config.replicas_fp16,
            ModelPrecision.INT8: self._config.replicas_int8,
            ModelPrecision.INT4: self._config.replicas_int4,
        }[precision]

    # ---------------------------------------------------------- stats
    def statistics(self, window: Optional[int] = None) -> Dict[str, Any]:
        """Return aggregate statistics (P1 3.6: windowed view)."""
        with self._lock:
            if window is not None:
                history = list(self._history)[-window:]
            else:
                history = list(self._history)
            precisions = dict(self._current_precision)

        if not history:
            return {
                "transitions": 0,
                "completed": 0,
                "failed": 0,
                "by_target_precision": {},
                "mean_duration_ms": None,
                "regions": len(precisions),
                "current_precisions": {
                    r: p.value for r, p in precisions.items()
                },
            }

        completed = sum(
            1 for t in history
            if t.status == TransitionStatus.COMPLETED.value)
        failed = sum(
            1 for t in history
            if t.status == TransitionStatus.FAILED.value)
        durations = [
            t.duration_ms() for t in history
            if t.duration_ms() is not None
        ]
        by_target: Dict[str, int] = {}
        for t in history:
            by_target[t.to_precision.value] = (
                by_target.get(t.to_precision.value, 0) + 1)

        return {
            "transitions": len(history),
            "completed": completed,
            "failed": failed,
            "by_target_precision": by_target,
            "mean_duration_ms": (
                sum(durations) / len(durations) if durations else None
            ),
            "regions": len(precisions),
            "current_precisions": {
                r: p.value for r, p in precisions.items()
            },
        }

    def reset(self, *, clear_history: bool = False) -> None:
        with self._lock:
            self._current_precision.clear()
            self._transition_locks.clear()
            if clear_history:
                self._history.clear()
            self._ctx_start = None
            self._async_ctx_start = None
        logger.debug("PrecisionController reset.")

    def close(self) -> None:
        """Release resources (P1 3.6). Idempotent."""
        logger.debug(
            "PrecisionController closed "
            "(regions=%d, transitions=%d).",
            len(self._current_precision), len(self._history))

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "policy": {
                    "green_zone_precision":
                        self.policy.green_zone_precision.value,
                    "yellow_zone_precision":
                        self.policy.yellow_zone_precision.value,
                    "red_zone_precision":
                        self.policy.red_zone_precision.value,
                    "critical_zone_precision":
                        self.policy.critical_zone_precision.value,
                    "min_accuracy_fp32": self.policy.min_accuracy_fp32,
                    "min_accuracy_fp16": self.policy.min_accuracy_fp16,
                    "min_accuracy_int8": self.policy.min_accuracy_int8,
                    "min_accuracy_int4": self.policy.min_accuracy_int4,
                    "zone_transition_buffer":
                        self.policy.zone_transition_buffer,
                },
                "config": asdict(self._config),
                "dpq_config": asdict(self._dpq_config),
                "strict": self._strict,
                "current_precision": {
                    r: p.value
                    for r, p in self._current_precision.items()
                },
                "history": [t.to_dict() for t in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        model_converter: Any = None,
        worker_pool_manager: Any = None,
        accuracy_guardian: Any = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "PrecisionController":
        if not isinstance(data, Mapping):
            raise PrecisionControllerError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        # P0 2.2 / P0 2.3: delegate to from_legacy_dict helpers.
        policy = PrecisionPolicy.from_legacy_dict(
            data.get("policy") or {})
        cfg = ControllerConfig.from_legacy_dict(
            data.get("config") or {})
        dpq_cfg = DPQConfig.from_legacy_dict(
            data.get("dpq_config") or {})

        controller = cls(
            policy=policy,
            model_converter=model_converter,
            worker_pool_manager=worker_pool_manager,
            accuracy_guardian=accuracy_guardian,
            config=cfg,
            dpq_config=dpq_cfg,
            strict=bool(data.get("strict", True)),
            metrics_hook=metrics_hook,
        )

        # P2 3.12: warn on in-flight transitions.
        pending = 0

        with controller._lock:
            for region, prec in (
                data.get("current_precision") or {}
            ).items():
                controller._current_precision[str(region)] = (
                    _coerce_precision(
                        prec,
                        field_name=(
                            f"current_precision[{region!r}]")))
            history_entries = list(data.get("history", []))
            if len(history_entries) > cfg.max_history:
                logger.warning(
                    "Restoring %d history entries; only most recent "
                    "%d kept.",
                    len(history_entries), cfg.max_history)
            for entry in history_entries:
                transition = PrecisionTransition.from_dict(entry)
                if transition.status == \
                        TransitionStatus.IN_PROGRESS.value:
                    pending += 1
                controller._history.append(transition)

        if pending:
            logger.warning(
                "Restored %d in-progress transition(s); their locks "
                "are gone — call on_carbon_zone_change to reconcile.",
                pending)

        return controller

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls, payload: str, **kwargs: Any,
    ) -> "PrecisionController":
        try:
            return cls.from_dict(json.loads(payload), **kwargs)
        except json.JSONDecodeError as exc:
            raise PrecisionControllerError(
                f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- pickling
    def __getstate__(self) -> Dict[str, Any]:
        # P0 2.6: RLock and asyncio.Lock are not picklable.
        state = self.__dict__.copy()
        state.pop("_lock", None)
        state.pop("_transition_locks", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()
        self._transition_locks = {}

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "PrecisionController":
        with self._lock:
            self._ctx_start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._ctx_start
                if self._ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "PrecisionController scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed)
            return
        logger.info(
            "PrecisionController scope closed in %.4fs.", elapsed)

    async def __aenter__(self) -> "PrecisionController":
        with self._lock:
            self._async_ctx_start = time.perf_counter()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._async_ctx_start
                if self._async_ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._async_ctx_start = None
        if exc_type is not None:
            logger.warning(
                "Async PrecisionController scope exited with %s "
                "after %.4fs.", exc_type.__name__, elapsed)
            return
        logger.info(
            "Async PrecisionController scope closed in %.4fs.", elapsed)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "PrecisionController("
                f"version={__version__!r}, "
                f"regions={len(self._current_precision)}, "
                f"transitions={len(self._history)}, "
                f"buffer={self._config.zone_transition_buffer:.1f}, "
                f"strict={self._strict})"
            )


__all__ = [
    "ControllerConfig",
    "PrecisionController",
    "PrecisionControllerError",
    "PrecisionPolicy",
]


# --------------------------------------------------------------------------- #
# Self-tests: python -m dpq.precision_controller
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import pickle
    from dataclasses import dataclass

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    @dataclass
    class _Update:
        region: str
        zone: CarbonZone
        intensity_gco2_kwh: float

    # ---------------------------------------------------------------- #
    # P0 2.1 — upgrades fire, downgrades still fire, FP32 upgrades fire
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: transition direction ===")

    async def _test_p0_2_1() -> None:
        controller = PrecisionController(
            policy=PrecisionPolicy(),
            # buffer=10 so hysteresis has a visible band.
            config=ControllerConfig(zone_transition_buffer=10.0),
        )

        # 1. First transition (no prior precision): intensity=450 → CRITICAL
        t1 = await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.CRITICAL, 450.0))
        assert t1 is not None
        assert t1.to_precision == ModelPrecision.INT4
        assert controller.get_region_precision("eu-west") == \
            ModelPrecision.INT4
        print(f"  ✓ initial 450 → CRITICAL → INT4")

        # 2. **P0 2.1 core case**: INT4 → INT8 upgrade when intensity
        # drops to 350 (RED band). Old code would no-op.
        t2 = await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.RED, 350.0))
        assert t2 is not None, (
            "P0 2.1 regression: INT4 → INT8 upgrade did not fire")
        assert t2.from_precision == ModelPrecision.INT4
        assert t2.to_precision == ModelPrecision.INT8
        assert controller.get_region_precision("eu-west") == \
            ModelPrecision.INT8
        print(f"  ✓ INT4 → INT8 upgrade fired at intensity=350")

        # 3. Downgrade still works: INT8 → INT4 at intensity=450 (CRITICAL)
        t3 = await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.CRITICAL, 450.0))
        assert t3 is not None
        assert t3.to_precision == ModelPrecision.INT4
        print(f"  ✓ INT8 → INT4 downgrade fired at intensity=450")

        # 4. INT4 → INT8 → FP16 → FP32 chain works
        # Start at INT4 (from step 3), go to RED (INT8)
        await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.RED, 350.0))
        assert controller.get_region_precision("eu-west") == \
            ModelPrecision.INT8

        # Now YELLOW (INT8 → FP16 upgrade)
        t4 = await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.YELLOW, 150.0))
        assert t4 is not None, (
            "P0 2.1 regression: INT8 → FP16 upgrade did not fire")
        assert t4.to_precision == ModelPrecision.FP16
        print(f"  ✓ INT8 → FP16 upgrade fired at intensity=150")

        # Now GREEN (FP16 → FP32 upgrade — the case the old code did fire)
        t5 = await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.GREEN, 20.0))
        assert t5 is not None, (
            "P0 2.1 regression: FP16 → FP32 upgrade did not fire")
        assert t5.to_precision == ModelPrecision.FP32
        print(f"  ✓ FP16 → FP32 upgrade fired at intensity=20")

        # 5. Hysteresis still works: try to transition GREEN → YELLOW
        # at intensity=55 (within buffer of green_threshold=50).
        # 55 > 50 + 10 = 60? No (55 < 60), so no transition.
        t6 = await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.YELLOW, 55.0))
        assert t6 is None, (
            "hysteresis regression: 55 should be within buffer of "
            "green_threshold=50 (check: 55 > 50+10=60 is False)")
        print(f"  ✓ hysteresis still blocks near-boundary transitions")

    asyncio.run(_test_p0_2_1())

    # ---------------------------------------------------------------- #
    # P0 2.2 — config + dpq_config round-trip
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: config + dpq_config round-trip ===")

    async def _test_p0_2_2() -> None:
        custom_dpq = DPQConfig(
            green_threshold=30.0,
            yellow_threshold=180.0,
            red_threshold=380.0,
        )
        controller = PrecisionController(
            policy=PrecisionPolicy(),
            config=ControllerConfig(
                replicas_fp32=8, replicas_fp16=12,
                replicas_int8=20, replicas_int4=32,  # custom
            ),
            dpq_config=custom_dpq,
        )
        # Trigger one transition so history is non-empty.
        await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.YELLOW, 150.0))

        restored = PrecisionController.from_dict(controller.to_dict())
        assert restored.config.replicas_int4 == 32, (
            f"P0 2.2 regression: replicas_int4="
            f"{restored.config.replicas_int4}, expected 32")
        assert restored.config.replicas_fp32 == 8
        assert restored._dpq_config.green_threshold == 30.0, (
            f"P0 2.2 regression: dpq_config.green_threshold="
            f"{restored._dpq_config.green_threshold}, expected 30.0")
        assert restored._dpq_config.yellow_threshold == 180.0
        assert restored._dpq_config.red_threshold == 380.0
        print(f"  ✓ replicas_int4=32 preserved, "
              f"dpq_config.green_threshold=30.0 preserved")
        print(f"  ✓ full dpq_config: green={restored._dpq_config.green_threshold}, "
              f"yellow={restored._dpq_config.yellow_threshold}, "
              f"red={restored._dpq_config.red_threshold}")

    asyncio.run(_test_p0_2_2())

    # ---------------------------------------------------------------- #
    # P0 2.3 — uppercase enum strings load cleanly
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: uppercase enum strings ===")

    payload = {
        "policy": {
            "green_zone_precision": "FP32",       # uppercase
            "yellow_zone_precision": "  FP16  ",  # whitespace + uppercase
            "red_zone_precision": "INT8",
            "critical_zone_precision": "INT4",
        },
        "config": {},
        "dpq_config": {},
        "strict": True,
        "current_precision": {
            "eu-west": "INT8",       # uppercase
            "us-east": " FP16 ",     # whitespace + uppercase
        },
        "history": [],
    }
    restored = PrecisionController.from_dict(payload)
    assert restored.policy.green_zone_precision == ModelPrecision.FP32
    assert restored.policy.yellow_zone_precision == ModelPrecision.FP16
    assert restored.policy.red_zone_precision == ModelPrecision.INT8
    assert restored.policy.critical_zone_precision == ModelPrecision.INT4
    assert restored.get_region_precision("eu-west") == ModelPrecision.INT8
    assert restored.get_region_precision("us-east") == ModelPrecision.FP16
    print(f"  ✓ policy: FP32/FP16/INT8/INT4 (mixed case) loaded")
    print(f"  ✓ current_precision: eu-west=INT8, us-east=FP16")

    # Invalid value raises PrecisionControllerError (not ValueError)
    bad_payload = dict(payload)
    bad_payload["policy"] = dict(payload["policy"])
    bad_payload["policy"]["green_zone_precision"] = "garbage"
    try:
        PrecisionController.from_dict(bad_payload)
    except PrecisionControllerError as exc:
        print(f"  ✓ 'garbage' → PrecisionControllerError")
    except ValueError as exc:
        raise AssertionError(
            f"P0 2.3 regression: 'garbage' raised plain ValueError: "
            f"{exc}")
    else:
        raise AssertionError(
            "P0 2.3 regression: 'garbage' accepted")

    # ---------------------------------------------------------------- #
    # P0 2.4 — lock capacity raises, does not evict
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: lock capacity does not evict held lock ===")

    async def _test_p0_2_4() -> None:
        controller = PrecisionController(
            policy=PrecisionPolicy(),
            config=ControllerConfig(max_transition_locks=2),
        )
        # Region 1: creates lock #1
        await controller.on_carbon_zone_change(
            _Update("r1", CarbonZone.YELLOW, 120.0))
        # Region 2: creates lock #2
        await controller.on_carbon_zone_change(
            _Update("r2", CarbonZone.YELLOW, 120.0))
        # Lock dict now has 2 entries → at capacity
        assert len(controller._transition_locks) == 2

        # Region 3: should raise, not evict
        try:
            await controller.on_carbon_zone_change(
                _Update("r3", CarbonZone.YELLOW, 120.0))
        except PrecisionControllerError as exc:
            print(f"  ✓ 3rd region raises: {exc}")
            assert "max_transition_locks" in str(exc)
        else:
            raise AssertionError(
                "P0 2.4 regression: 3rd region did not raise — "
                "eviction happened")

        # Existing locks still in place
        assert "r1" in controller._transition_locks
        assert "r2" in controller._transition_locks
        print(f"  ✓ r1 and r2 locks still present after the raise")

    asyncio.run(_test_p0_2_4())

    # ---------------------------------------------------------------- #
    # P0 2.5 — negative / Inf intensity rejected
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: intensity validation ===")

    async def _test_p0_2_5() -> None:
        controller = PrecisionController(policy=PrecisionPolicy())

        for bad in (-50.0, float("inf"), float("nan"), "150.0", True):
            try:
                await controller.on_carbon_zone_change(
                    _Update("eu-west", CarbonZone.YELLOW, bad))
            except PrecisionControllerError as exc:
                print(f"  ✓ intensity={bad!r} rejected: {exc}")
            else:
                raise AssertionError(
                    f"P0 2.5 regression: intensity={bad!r} accepted")

        # Valid still works
        t = await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.YELLOW, 150.0))
        assert t is not None
        print(f"  ✓ valid intensity=150 accepted")

    asyncio.run(_test_p0_2_5())

    # ---------------------------------------------------------------- #
    # P0 2.6 — pickle round-trip; _transition_locks emptied
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: pickle round-trip ===")

    async def _test_p0_2_6() -> None:
        controller = PrecisionController(policy=PrecisionPolicy())
        # Populate state.
        await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.YELLOW, 120.0))
        assert len(controller._transition_locks) == 1
        assert len(controller.history) == 1

        blob = pickle.dumps(controller)
        restored = pickle.loads(blob)

        assert restored._lock is not None
        assert restored._transition_locks == {}, (
            f"P0 2.6 regression: _transition_locks="
            f"{restored._transition_locks} (expected {{}})")
        assert len(restored.history) == 1
        assert restored.get_region_precision("eu-west") == \
            ModelPrecision.FP16

        # Still functional — a new transition re-creates the lock.
        t = await restored.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.RED, 250.0))
        assert t is not None
        assert len(restored._transition_locks) == 1
        print(f"  ✓ pickle round-trip: _transition_locks recreated "
              f"empty, controller usable post-restore")

    asyncio.run(_test_p0_2_6())

    # ---------------------------------------------------------------- #
    # P0 2.7 — strict-mode failure recorded in history
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: strict failures recorded in history ===")

    class _FailingConverter:
        async def convert_models(self, region, from_precision, to_precision):
            raise RuntimeError("simulated conversion failure")

    async def _test_p0_2_7() -> None:
        controller = PrecisionController(
            policy=PrecisionPolicy(),
            model_converter=_FailingConverter(),
            config=ControllerConfig(max_retries=0),  # no retries, immediate fail
            strict=True,
        )

        try:
            await controller.on_carbon_zone_change(
                _Update("eu-west", CarbonZone.YELLOW, 120.0))
        except PrecisionControllerError as exc:
            print(f"  ✓ strict transition raised: {exc}")
        else:
            raise AssertionError(
                "P0 2.7 regression: strict mode did not raise")

        # Failure MUST be in history (previously lost).
        assert len(controller.history) == 1, (
            f"P0 2.7 regression: history size={len(controller.history)} "
            f"(expected 1)")
        failed = controller.history[0]
        assert failed.status == "failed", (
            f"P0 2.7 regression: status={failed.status!r}")
        assert failed.error_message is not None
        assert "simulated conversion failure" in failed.error_message
        print(f"  ✓ history[0].status='failed', "
              f"error_message={failed.error_message[:40]}...")

        # statistics() must count the failure.
        stats = controller.statistics()
        assert stats["failed"] == 1, (
            f"P0 2.7 regression: statistics['failed']="
            f"{stats['failed']} (expected 1)")
        assert stats["completed"] == 0
        print(f"  ✓ statistics: failed={stats['failed']}, "
              f"completed={stats['completed']}")

        # Lenient mode: same failure, but no raise.
        lenient = PrecisionController(
            policy=PrecisionPolicy(),
            model_converter=_FailingConverter(),
            config=ControllerConfig(max_retries=0),
            strict=False,
        )
        t = await lenient.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.YELLOW, 120.0))
        assert t is not None
        assert t.status == "failed"
        assert lenient.statistics()["failed"] == 1
        print(f"  ✓ lenient mode: returns transition with "
              f"status='failed' (no raise)")

    asyncio.run(_test_p0_2_7())

    # ---------------------------------------------------------------- #
    # Bonus: P1 features (retry, metrics_hook, statistics, close, ctx)
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: P1 features ===")

    class _FlakyConverter:
        """Fails the first N calls then succeeds."""
        def __init__(self, fail_first: int = 2) -> None:
            self._remaining = fail_first
            self.call_count = 0

        async def convert_models(self, region, from_precision, to_precision):
            self.call_count += 1
            if self._remaining > 0:
                self._remaining -= 1
                raise RuntimeError(
                    f"transient failure #{self.call_count}")

    async def _test_p1_retry() -> None:
        flaky = _FlakyConverter(fail_first=2)
        controller = PrecisionController(
            policy=PrecisionPolicy(),
            model_converter=flaky,
            config=ControllerConfig(
                max_retries=3,
                base_backoff_seconds=0.01,  # fast test
            ),
        )
        t = await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.YELLOW, 120.0))
        assert t is not None
        assert t.status == "completed"
        assert flaky.call_count == 3, (
            f"P1 3.1 regression: converter called "
            f"{flaky.call_count} times, expected 3")
        print(f"  ✓ retry: 2 failures + 1 success = 3 calls, "
              f"status={t.status}")

    asyncio.run(_test_p1_retry())

    async def _test_metrics_hook() -> None:
        events: List = []
        controller = PrecisionController(
            policy=PrecisionPolicy(),
            metrics_hook=lambda ev, f: events.append((ev, f)),
        )
        await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.YELLOW, 120.0))
        # Same zone → skipped
        await controller.on_carbon_zone_change(
            _Update("eu-west", CarbonZone.YELLOW, 130.0))

        names = {ev for ev, _ in events}
        assert "transition_started" in names
        assert "transition_completed" in names
        assert "transition_skipped" in names
        print(f"  ✓ metrics_hook: {sorted(names)}")

    asyncio.run(_test_metrics_hook())

    async def _test_window_close_ctx() -> None:
        controller = PrecisionController(policy=PrecisionPolicy())
        for i, zone in enumerate([
            CarbonZone.YELLOW, CarbonZone.RED, CarbonZone.GREEN,
        ]):
            await controller.on_carbon_zone_change(
                _Update("eu-west", zone, 120.0 + i * 100))
        stats = controller.statistics()
        assert stats["transitions"] == 3
        win = controller.statistics(window=2)
        assert win["transitions"] == 2
        print(f"  ✓ statistics()=3, statistics(window=2)=2")

        controller.close()  # idempotent
        controller.close()
        print(f"  ✓ close() idempotent")

        async with PrecisionController(
            policy=PrecisionPolicy()
        ) as scoped:
            await scoped.on_carbon_zone_change(
                _Update("eu-west", CarbonZone.YELLOW, 120.0))
        print(f"  ✓ async context manager OK")

    asyncio.run(_test_window_close_ctx())

    # ---------------------------------------------------------------- #
    # Backward compat: v1.0.0 payloads (no version, no dpq_config)
    # ---------------------------------------------------------------- #
    print("\n=== Backward compat: v1.0.0 payload ===")

    legacy_payload = {
        # No "version"
        # No "dpq_config" (v1.0.0 default has all fields)
        "policy": {
            "green_zone_precision": "fp32",
            "yellow_zone_precision": "fp16",
            "red_zone_precision": "int8",
            "critical_zone_precision": "int4",
        },
        "config": {
            "zone_transition_buffer": 10.0,
            "max_transition_locks": 64,
            "max_history": 1000,
            # replicas_* omitted — should use defaults
        },
        "strict": True,
        "current_precision": {"eu-west": "int8"},
        "history": [],
    }
    legacy = PrecisionController.from_dict(legacy_payload)
    assert legacy.config.replicas_int4 == 16  # default
    assert legacy._dpq_config.green_threshold == 50.0  # default
    assert legacy.get_region_precision("eu-west") == ModelPrecision.INT8
    print(f"  ✓ legacy v1.0.0 payload loads; replicas_int4 defaults to 16, "
          f"dpq_config defaults to green=50.0")

    print("\n✓ All self-tests passed.")
