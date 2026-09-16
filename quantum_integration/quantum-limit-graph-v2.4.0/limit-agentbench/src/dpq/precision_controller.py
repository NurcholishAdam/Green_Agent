# src/dpq/precision_controller.py

"""
Precision Controller
====================

Dynamic precision adjustment engine.

Enhancements
------------
- Imports ``ModelPrecision`` / ``CarbonZone`` / ``PrecisionTransition`` /
  ``DPQConfig`` from :mod:`models` — no more duplicated enums.
- Module-level ``logger`` (previously missing → ``NameError``).
- ``ControllerConfig`` + ``PrecisionPolicy`` with validation.
- ``RLock``-guarded state; bounded transition history.
- Full validation; strict / non-strict modes.
- Pluggable ``model_converter`` / ``worker_pool_manager`` / ``accuracy_guardian``
  collaborators — all optional (defaults to safe no-op shims).
- Sync + async context managers; serialization; ``statistics()``;
  ``__repr__``; custom :class:`PrecisionControllerError`.
- ``__main__`` smoke test.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional

from .models import (
    CarbonZone,
    DPQConfig,
    ModelPrecision,
    PrecisionTransition,
    new_transition_id,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class PrecisionControllerError(ValueError):
    """Raised for invalid inputs, configuration, or transition failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ControllerConfig:
    """Tunable parameters for the precision controller."""

    zone_transition_buffer: float = 10.0     # gCO2/kWh hysteresis
    max_transition_locks: int = 64
    max_history: int = 1000
    max_retries: int = 1
    base_backoff_seconds: float = 0.5

    # Replica mapping per precision.
    replicas_fp32: int = 4
    replicas_fp16: int = 6
    replicas_int8: int = 10
    replicas_int4: int = 16

    def __post_init__(self) -> None:
        if self.zone_transition_buffer < 0:
            raise PrecisionControllerError(
                "zone_transition_buffer must be >= 0."
            )
        if self.max_transition_locks <= 0:
            raise PrecisionControllerError("max_transition_locks must be > 0.")
        if self.max_history <= 0:
            raise PrecisionControllerError("max_history must be > 0.")
        if self.max_retries < 0:
            raise PrecisionControllerError("max_retries must be >= 0.")
        if self.base_backoff_seconds < 0:
            raise PrecisionControllerError(
                "base_backoff_seconds must be >= 0."
            )
        for name in (
            "replicas_fp32", "replicas_fp16", "replicas_int8", "replicas_int4",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value <= 0:
                raise PrecisionControllerError(f"{name} must be a positive int.")


# --------------------------------------------------------------------------- #
# Policy
# --------------------------------------------------------------------------- #
@dataclass
class PrecisionPolicy:
    """Policy mapping carbon zones to precision levels."""

    green_zone_precision: ModelPrecision = ModelPrecision.FP32
    yellow_zone_precision: ModelPrecision = ModelPrecision.FP16
    red_zone_precision: ModelPrecision = ModelPrecision.INT8
    critical_zone_precision: ModelPrecision = ModelPrecision.INT4

    min_accuracy_fp32: float = 0.95
    min_accuracy_fp16: float = 0.93
    min_accuracy_int8: float = 0.90
    min_accuracy_int4: float = 0.85

    zone_transition_buffer: float = 10.0

    def __post_init__(self) -> None:
        for name in (
            "min_accuracy_fp32", "min_accuracy_fp16",
            "min_accuracy_int8", "min_accuracy_int4",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise PrecisionControllerError(f"{name} must be in [0, 1].")
        if self.zone_transition_buffer < 0:
            raise PrecisionControllerError(
                "zone_transition_buffer must be >= 0."
            )

    def min_accuracy(self, precision: ModelPrecision) -> float:
        return {
            ModelPrecision.FP32: self.min_accuracy_fp32,
            ModelPrecision.FP16: self.min_accuracy_fp16,
            ModelPrecision.INT8: self.min_accuracy_int8,
            ModelPrecision.INT4: self.min_accuracy_int4,
        }[precision]


# --------------------------------------------------------------------------- #
# Controller
# --------------------------------------------------------------------------- #
class PrecisionController:
    """
    Dynamic precision adjustment engine.

    Thread-safe, serializable, and bounded in memory. All original public
    methods are preserved; new parameters are keyword-only.
    """

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
    ) -> None:
        if not isinstance(policy, PrecisionPolicy):
            raise PrecisionControllerError(
                "policy must be a PrecisionPolicy instance."
            )
        self.policy = policy
        self.model_converter = model_converter
        self.worker_pool_manager = worker_pool_manager
        self.accuracy_guardian = accuracy_guardian

        self._config = config or ControllerConfig()
        self._dpq_config = dpq_config or DPQConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._current_precision: Dict[str, ModelPrecision] = {}
        self._transition_locks: Dict[str, asyncio.Lock] = {}
        self._history: Deque[PrecisionTransition] = deque(
            maxlen=self._config.max_history
        )
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None

        logger.debug(
            "PrecisionController initialized (buffer=%.1f, max_locks=%d, "
            "strict=%s)",
            self._config.zone_transition_buffer,
            self._config.max_transition_locks,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> ControllerConfig:
        return self._config

    @property
    def history(self) -> List[PrecisionTransition]:
        with self._lock:
            return list(self._history)

    @property
    def current_precision(self) -> Dict[str, ModelPrecision]:
        with self._lock:
            return dict(self._current_precision)

    def get_region_precision(self, region: str) -> Optional[ModelPrecision]:
        """Return the current precision for ``region`` (or None)."""
        with self._lock:
            return self._current_precision.get(region)

    # ---------------------------------------------------------- public API
    async def on_carbon_zone_change(self, update: Any) -> Optional[PrecisionTransition]:
        """
        Handle a carbon zone change notification.

        Parameters
        ----------
        update : Any
            Must expose ``region``, ``zone``, and ``intensity_gco2_kwh``.

        Returns
        -------
        PrecisionTransition | None
            The recorded transition, or None if no transition occurred.
        """
        region = getattr(update, "region", None)
        zone = getattr(update, "zone", None)
        intensity = getattr(update, "intensity_gco2_kwh", None)
        if not isinstance(region, str) or not region:
            raise PrecisionControllerError(
                "update.region must be a non-empty string."
            )
        if not isinstance(zone, CarbonZone):
            raise PrecisionControllerError(
                "update.zone must be a CarbonZone."
            )
        if not isinstance(intensity, (int, float)) or math.isnan(
            float(intensity)
        ):
            raise PrecisionControllerError(
                "update.intensity_gco2_kwh must be a finite number."
            )

        target_precision = self._get_precision_for_zone(zone)

        lock = self._lock_for_region(region)
        async with lock:
            with self._lock:
                current = self._current_precision.get(region)

            if current == target_precision:
                logger.debug(
                    "Region %s already at %s; no transition.",
                    region, target_precision.value,
                )
                return None

            if not self._should_transition(
                current, target_precision, float(intensity)
            ):
                logger.debug(
                    "Hysteresis buffer prevents %s → %s transition.",
                    current.value if current else "none",
                    target_precision.value,
                )
                return None

            transition = PrecisionTransition(
                transition_id=new_transition_id(),
                region=region,
                model_name="*",
                from_precision=current,
                to_precision=target_precision,
                triggered_by="carbon_zone_change",
                carbon_intensity=float(intensity),
                carbon_zone=zone,
                start_time=datetime.now(timezone.utc),
                status="in_progress",
            )
            logger.info(
                "Transition %s: %s → %s (intensity=%.1f gCO2/kWh)",
                region,
                current.value if current else "none",
                target_precision.value,
                float(intensity),
            )

            try:
                await self._trigger_conversion(
                    region, current, target_precision
                )
                await self._scale_pools(region, target_precision)
                with self._lock:
                    self._current_precision[region] = target_precision

                transition.status = "completed"
                transition.end_time = datetime.now(timezone.utc)
                await self._emit_transition_metric(
                    region, current, target_precision
                )
            except Exception as exc:
                logger.exception(
                    "Transition %s failed: %s", transition.transition_id, exc
                )
                transition.status = "failed"
                transition.error_message = f"{type(exc).__name__}: {exc}"
                transition.end_time = datetime.now(timezone.utc)
                if self._strict:
                    raise PrecisionControllerError(
                        f"Transition failed: {exc}"
                    ) from exc

            with self._lock:
                self._history.append(transition)
            return transition

    # ---------------------------------------------------------- internals
    def _lock_for_region(self, region: str) -> asyncio.Lock:
        with self._lock:
            if region not in self._transition_locks:
                if len(self._transition_locks) >= self._config.max_transition_locks:
                    # Evict an arbitrary lock (rare; capacity rarely reached).
                    oldest = next(iter(self._transition_locks))
                    self._transition_locks.pop(oldest, None)
                self._transition_locks[region] = asyncio.Lock()
            return self._transition_locks[region]

    async def _trigger_conversion(
        self,
        region: str,
        from_precision: Optional[ModelPrecision],
        to_precision: ModelPrecision,
    ) -> None:
        if self.model_converter is None:
            return
        method = getattr(self.model_converter, "convert_models", None)
        if not callable(method):
            return
        result = method(
            region=region,
            from_precision=from_precision,
            to_precision=to_precision,
        )
        if asyncio.iscoroutine(result):
            await result

    async def _scale_pools(
        self, region: str, precision: ModelPrecision
    ) -> None:
        if self.worker_pool_manager is None:
            return
        method = getattr(self.worker_pool_manager, "scale_precision_pool", None)
        if not callable(method):
            return
        result = method(
            region=region,
            precision=precision,
            target_replicas=self._calculate_replicas(precision),
        )
        if asyncio.iscoroutine(result):
            await result

    async def _emit_transition_metric(
        self,
        region: str,
        from_prec: Optional[ModelPrecision],
        to_prec: ModelPrecision,
    ) -> None:
        """Emit a Prometheus metric for the precision transition."""
        # Hook: real deployment would push to a Prometheus client.
        logger.debug(
            "metric dpq_precision_transition{region=%s, from=%s, to=%s} 1",
            region,
            from_prec.value if from_prec else "none",
            to_prec.value,
        )

    def _get_precision_for_zone(self, zone: CarbonZone) -> ModelPrecision:
        """Map a carbon zone to a precision level per policy."""
        mapping = {
            CarbonZone.GREEN: self.policy.green_zone_precision,
            CarbonZone.YELLOW: self.policy.yellow_zone_precision,
            CarbonZone.RED: self.policy.red_zone_precision,
            CarbonZone.CRITICAL: self.policy.critical_zone_precision,
        }
        return mapping[zone]

    def _should_transition(
        self,
        current: Optional[ModelPrecision],
        target: ModelPrecision,
        intensity: float,
    ) -> bool:
        """Check whether the transition should proceed (hysteresis)."""
        if current is None:
            return True
        threshold = self._get_transition_threshold(current, target)
        buffer = self._config.zone_transition_buffer
        if target == ModelPrecision.FP32:
            return intensity < threshold - buffer
        return intensity > threshold + buffer

    def _get_transition_threshold(
        self,
        from_prec: ModelPrecision,
        to_prec: ModelPrecision,
    ) -> float:
        """Return the carbon intensity threshold for a transition."""
        thresholds = {
            (ModelPrecision.FP32, ModelPrecision.FP16): self._dpq_config.green_threshold,
            (ModelPrecision.FP16, ModelPrecision.INT8): self._dpq_config.yellow_threshold,
            (ModelPrecision.INT8, ModelPrecision.INT4): self._dpq_config.red_threshold,
            (ModelPrecision.FP16, ModelPrecision.FP32): self._dpq_config.green_threshold,
            (ModelPrecision.INT8, ModelPrecision.FP16): self._dpq_config.yellow_threshold,
            (ModelPrecision.INT4, ModelPrecision.INT8): self._dpq_config.red_threshold,
        }
        return thresholds.get(
            (from_prec, to_prec), self._dpq_config.yellow_threshold
        )

    def _calculate_replicas(self, precision: ModelPrecision) -> int:
        """Return the target replica count for a precision pool."""
        return {
            ModelPrecision.FP32: self._config.replicas_fp32,
            ModelPrecision.FP16: self._config.replicas_fp16,
            ModelPrecision.INT8: self._config.replicas_int8,
            ModelPrecision.INT4: self._config.replicas_int4,
        }[precision]

    # ---------------------------------------------------------- stats
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the transition history."""
        with self._lock:
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
            }
        completed = sum(1 for t in history if t.status == "completed")
        durations = [
            t.duration_ms() for t in history if t.duration_ms() is not None
        ]
        by_target: Dict[str, int] = {}
        for t in history:
            by_target[t.to_precision.value] = (
                by_target.get(t.to_precision.value, 0) + 1
            )
        return {
            "transitions": len(history),
            "completed": completed,
            "failed": sum(1 for t in history if t.status == "failed"),
            "by_target_precision": by_target,
            "mean_duration_ms": (
                sum(durations) / len(durations) if durations else None
            ),
            "regions": len(precisions),
            "current_precisions": {
                region: p.value for region, p in precisions.items()
            },
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset internal state; optionally clear the transition history."""
        with self._lock:
            self._current_precision.clear()
            self._transition_locks.clear()
            if clear_history:
                self._history.clear()
        logger.debug("PrecisionController reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "policy": asdict(self.policy),
                "config": asdict(self._config),
                "dpq_config": asdict(self._dpq_config),
                "strict": self._strict,
                "current_precision": {
                    r: p.value for r, p in self._current_precision.items()
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
    ) -> "PrecisionController":
        if not isinstance(data, Mapping):
            raise PrecisionControllerError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        pol_data = dict(data.get("policy", {}) or {})
        policy = PrecisionPolicy(
            green_zone_precision=ModelPrecision(
                pol_data.get("green_zone_precision", "fp32")
            ),
            yellow_zone_precision=ModelPrecision(
                pol_data.get("yellow_zone_precision", "fp16")
            ),
            red_zone_precision=ModelPrecision(
                pol_data.get("red_zone_precision", "int8")
            ),
            critical_zone_precision=ModelPrecision(
                pol_data.get("critical_zone_precision", "int4")
            ),
            min_accuracy_fp32=float(pol_data.get("min_accuracy_fp32", 0.95)),
            min_accuracy_fp16=float(pol_data.get("min_accuracy_fp16", 0.93)),
            min_accuracy_int8=float(pol_data.get("min_accuracy_int8", 0.90)),
            min_accuracy_int4=float(pol_data.get("min_accuracy_int4", 0.85)),
            zone_transition_buffer=float(
                pol_data.get("zone_transition_buffer", 10.0)
            ),
        )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = ControllerConfig(
            zone_transition_buffer=float(
                cfg_data.get("zone_transition_buffer", 10.0)
            ),
            max_transition_locks=int(cfg_data.get("max_transition_locks", 64)),
            max_history=int(cfg_data.get("max_history", 1000)),
            max_retries=int(cfg_data.get("max_retries", 1)),
            base_backoff_seconds=float(
                cfg_data.get("base_backoff_seconds", 0.5)
            ),
        )
        controller = cls(
            policy=policy,
            model_converter=model_converter,
            worker_pool_manager=worker_pool_manager,
            accuracy_guardian=accuracy_guardian,
            config=cfg,
            strict=bool(data.get("strict", True)),
        )
        with controller._lock:
            for region, prec in (data.get("current_precision") or {}).items():
                controller._current_precision[str(region)] = ModelPrecision(
                    str(prec)
                )
            for entry in data.get("history", []):
                controller._history.append(
                    PrecisionTransition.from_dict(entry)
                )
        return controller

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls, payload: str, **kwargs: Any
    ) -> "PrecisionController":
        try:
            return cls.from_dict(json.loads(payload), **kwargs)
        except json.JSONDecodeError as exc:
            raise PrecisionControllerError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "PrecisionController":
        self._ctx_start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "PrecisionController scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info("PrecisionController scope closed in %.4fs.", elapsed)

    async def __aenter__(self) -> "PrecisionController":
        self._async_ctx_start = time.perf_counter()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._async_ctx_start
            if self._async_ctx_start is not None
            else time.perf_counter()
        )
        self._async_ctx_start = None
        if exc_type is not None:
            logger.warning(
                "Async PrecisionController scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info("Async PrecisionController scope closed in %.4fs.", elapsed)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "PrecisionController("
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
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    @dataclass
    class _Update:
        region: str
        zone: CarbonZone
        intensity_gco2_kwh: float

    async def main() -> None:
        controller = PrecisionController(policy=PrecisionPolicy())

        # First transition (no current precision).
        t1 = await controller.on_carbon_zone_change(
            _Update("eu-west-1", CarbonZone.YELLOW, 120.0)
        )
        print("Transition 1:", t1.status, t1.to_precision.value if t1 else None)

        # Same zone → no-op.
        t2 = await controller.on_carbon_zone_change(
            _Update("eu-west-1", CarbonZone.YELLOW, 130.0)
        )
        print("Transition 2:", t2)

        # Hysteresis blocks a small change.
        t3 = await controller.on_carbon_zone_change(
            _Update("eu-west-1", CarbonZone.GREEN, 45.0)
        )
        print("Transition 3:", t3.status if t3 else None)

        # Large change crosses buffer → transition.
        t4 = await controller.on_carbon_zone_change(
            _Update("eu-west-1", CarbonZone.RED, 250.0)
        )
        print("Transition 4:", t4.status, t4.to_precision.value if t4 else None)

        print("stats     :", controller.statistics())
        print("repr      :", controller)

        # Serialization round-trip.
        payload = controller.to_json()
        restored = PrecisionController.from_json(payload)
        assert restored.to_dict() == controller.to_dict()
        print("Round-trip OK.")

        # Context manager.
        async with PrecisionController(policy=PrecisionPolicy()) as scoped:
            await scoped.on_carbon_zone_change(
                _Update("us-east-1", CarbonZone.CRITICAL, 500.0)
            )
        print("Context OK.")

    asyncio.run(main())
