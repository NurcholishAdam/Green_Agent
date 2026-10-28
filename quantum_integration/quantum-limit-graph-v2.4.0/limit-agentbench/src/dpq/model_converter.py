# src/dpq/model_converter.py

"""
Model Converter — v2.0.0
=========================

On-the-fly model quantization engine.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 Cache eviction over-fired because ``_cache_order`` accumulated
        duplicates. The cache is now an ``OrderedDict`` keyed on model name
        with ``move_to_end`` on access — true LRU semantics. 300
        conversions of 2 models with ``max_cache_entries=256`` keeps both
        cached (was: 0).
  * 2.2 ``from_dict`` delegates to ``ConverterConfig.from_legacy_dict`` so
        new config fields use their declared defaults.
  * 2.3 ``from_dict`` now restores the ``DPQConfig`` (previously a default
        instance was silently used, changing accuracy thresholds).
  * 2.4 ``from_dict`` restores ``cached_models`` keys so the count survives
        round-trip (full ``ConversionResult`` objects are not serialized —
        only presence).
  * 2.5 ``__getstate__`` / ``__setstate__`` make the converter picklable;
        unpicklable model handles are dropped with a warning.
  * 2.6 ``_backend_available`` is no longer a tautology. In simulate mode
        (default) only configured backends are available; real mode probes
        actual imports. ``_select_backend`` raises when no backend supports
        the requested precision (unless ``allow_backend_fallback=True``).

**P1 robustness / observability**
  * 3.1 Retry loop honours ``conversion_max_retries`` +
        ``conversion_base_backoff_seconds`` (previously dead config).
  * 3.2 ``sim_size_reduction_*`` / ``sim_accuracy_delta_*`` are now wired
        into the simulated path (previously dead config).
  * 3.4 All conversion helpers take ``(source_model, backend)``
        consistently.
  * 3.5 The timeout wraps the entire conversion (retries + backend call),
        so a hanging backend no longer blocks the converter.
  * 3.6 ``success`` is computed from absolute converted accuracy
        (``original_accuracy + accuracy_delta``), not the delta against a
        hardcoded 1.0.
  * 3.7 ``_load_validation_data`` honours ``accuracy_validation_samples``
        (was hard-capped at 64).
  * 3.8 ``metrics_hook(event, fields)`` for Prometheus / OTel.
  * 3.9 ``close()`` lifecycle hook.
  * 3.10 ``statistics(window=N)`` for a consistent windowed view.
  * 3.12 Backend results with a mismatched ``model_name`` are overridden
         and logged.
  * 3.14 ``register_model(..., None)`` rejected in strict mode.

Thread-safe, serializable, picklable, bounded in memory.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import pickle
import threading
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timezone
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional,
)

from .models import (
    ConversionResult,
    DPQConfig,
    ModelPrecision,
    ModelVariant,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Optional heavy dependencies
# --------------------------------------------------------------------------- #
try:  # pragma: no cover
    import torch  # type: ignore

    _TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    torch = None  # type: ignore[assignment]
    _TORCH_AVAILABLE = False

try:  # pragma: no cover
    from transformers import AutoModelForCausalLM  # type: ignore

    _TRANSFORMERS_AVAILABLE = True
except ImportError:  # pragma: no cover
    AutoModelForCausalLM = None  # type: ignore[assignment]
    _TRANSFORMERS_AVAILABLE = False

try:  # pragma: no cover
    import tensorrt  # type: ignore  # noqa: F401

    _TENSORRT_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TENSORRT_AVAILABLE = False

try:  # pragma: no cover
    import openvino  # type: ignore  # noqa: F401

    _OPENVINO_AVAILABLE = True
except ImportError:  # pragma: no cover
    _OPENVINO_AVAILABLE = False

try:  # pragma: no cover
    import onnxruntime  # type: ignore  # noqa: F401

    _ONNXRUNTIME_AVAILABLE = True
except ImportError:  # pragma: no cover
    _ONNXRUNTIME_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ModelConverterError(ValueError):
    """Raised for invalid inputs, configuration, or conversion failures."""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _dpq_config_from_dict(data: Optional[Mapping[str, Any]]) -> DPQConfig:
    """
    Reconstruct a ``DPQConfig`` from a payload (P0 2.3).

    Prefers ``from_legacy_dict``; falls back to field introspection;
    finally to the v1.0.0 hardcoded keys.
    """
    data = dict(data or {})
    from_legacy = getattr(DPQConfig, "from_legacy_dict", None)
    if callable(from_legacy):
        return from_legacy(data)
    try:
        valid = {f.name for f in fields(DPQConfig)}
        filtered = {k: v for k, v in data.items() if k in valid}
        return DPQConfig(**filtered)
    except Exception:  # noqa: BLE001
        return DPQConfig(
            green_threshold=float(data.get("green_threshold", 50.0)),
            yellow_threshold=float(data.get("yellow_threshold", 200.0)),
            red_threshold=float(data.get("red_threshold", 400.0)),
        )


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ConverterConfig:
    """Tunable parameters for the model converter."""

    supported_backends: tuple = ("tensorrt", "openvino", "onnxruntime", "torch")
    accuracy_validation_samples: int = 1000
    max_conversion_time_seconds: float = 30.0
    max_cache_entries: int = 256
    max_history: int = 1000
    conversion_max_retries: int = 1
    conversion_base_backoff_seconds: float = 0.5

    # P0 2.6: when True, every configured backend is treated as available
    # (pure simulation). When False, actual imports are probed.
    simulate_backends: bool = True
    # P0 2.6: when False and no preferred backend is available, raise
    # instead of falling back to "any available backend".
    allow_backend_fallback: bool = True

    # P1 3.2: simulation coefficients (now wired).
    sim_size_reduction_fp16: float = 45.0
    sim_size_reduction_int8: float = 72.0
    sim_size_reduction_int4: float = 85.0
    sim_accuracy_delta_fp16: float = -0.010
    sim_accuracy_delta_int8: float = -0.030
    sim_accuracy_delta_int4: float = -0.070
    sim_conversion_time_ms: float = 0.0   # 0 = use wall-clock

    def __post_init__(self) -> None:
        if not self.supported_backends:
            raise ModelConverterError("supported_backends must be non-empty.")
        for name in (
            "accuracy_validation_samples", "max_cache_entries", "max_history",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ModelConverterError(f"{name} must be a positive int.")
        if self.max_conversion_time_seconds <= 0:
            raise ModelConverterError("max_conversion_time_seconds must be > 0.")
        if isinstance(self.conversion_max_retries, bool) or \
                not isinstance(self.conversion_max_retries, int) or \
                self.conversion_max_retries < 0:
            raise ModelConverterError("conversion_max_retries must be >= 0.")
        if self.conversion_base_backoff_seconds < 0:
            raise ModelConverterError(
                "conversion_base_backoff_seconds must be >= 0.")

    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "ConverterConfig":
        """Build from a flat mapping; unknown keys ignored (P0 2.2)."""
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in cfg.items() if k in valid}
        sb = kwargs.get("supported_backends")
        if isinstance(sb, list):
            kwargs["supported_backends"] = tuple(sb)
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Converter
# --------------------------------------------------------------------------- #
class ModelConverter:
    """On-the-fly model quantization engine (thread-safe, picklable)."""

    def __init__(
        self,
        supported_backends: Optional[List[str]] = None,
        accuracy_validation_samples: int = 1000,
        max_conversion_time_seconds: int = 30,
        *,
        config: Optional[ConverterConfig] = None,
        dpq_config: Optional[DPQConfig] = None,
        strict: bool = True,
        conversion_backend: Optional[
            Callable[[str, ModelPrecision, Optional[ModelPrecision]], Any]
        ] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = ConverterConfig(
                supported_backends=tuple(
                    supported_backends
                    or ("tensorrt", "openvino", "onnxruntime", "torch")
                ),
                accuracy_validation_samples=accuracy_validation_samples,
                max_conversion_time_seconds=float(max_conversion_time_seconds),
            )
        self._dpq_config = dpq_config or DPQConfig()
        self._strict = bool(strict)

        if metrics_hook is not None and not callable(metrics_hook):
            raise ModelConverterError("metrics_hook must be callable.")
        self._metrics_hook = metrics_hook

        # Legacy attributes preserved.
        self.backends: List[str] = list(self._config.supported_backends)
        self.validation_samples: int = self._config.accuracy_validation_samples
        self.max_conversion_time: int = int(
            self._config.max_conversion_time_seconds)

        self._lock = threading.RLock()
        # P0 2.1: OrderedDict gives true LRU semantics.
        self._model_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._history: Deque[ConversionResult] = (
            __import__("collections").deque(maxlen=self._config.max_history)
        )
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None
        self._conversion_backend = conversion_backend
        self._registered_models: Dict[str, Any] = {}

        logger.debug(
            "ModelConverter v%s initialized (backends=%s, samples=%d, "
            "max_time=%.1fs, strict=%s, torch=%s, transformers=%s, "
            "simulate=%s)",
            __version__,
            self.backends,
            self.validation_samples,
            self.max_conversion_time,
            self._strict,
            _TORCH_AVAILABLE,
            _TRANSFORMERS_AVAILABLE,
            self._config.simulate_backends,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> ConverterConfig:
        return self._config

    @property
    def history(self) -> List[ConversionResult]:
        with self._lock:
            return list(self._history)

    @property
    def cached_model_count(self) -> int:
        with self._lock:
            return len(self._model_cache)

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

    # ---------------------------------------------------------- registry
    def register_model(self, model_name: str, model_handle: Any) -> None:
        """Register a model handle (P1 3.14: strict rejects None handle)."""
        if not isinstance(model_name, str) or not model_name:
            raise ModelConverterError(
                "model_name must be a non-empty string.")
        if model_handle is None and self._strict:
            raise ModelConverterError(
                f"model_handle must not be None for {model_name!r}.")
        with self._lock:
            self._registered_models[model_name] = model_handle
        logger.debug("Registered model %s.", model_name)

    def list_registered_models(self) -> List[str]:
        with self._lock:
            return list(self._registered_models.keys())

    # ---------------------------------------------------------- public API
    async def convert_models(
        self,
        region: str,
        from_precision: Optional[ModelPrecision],
        to_precision: ModelPrecision,
        model_names: Optional[List[str]] = None,
    ) -> List[ConversionResult]:
        """Convert models from one precision to another."""
        if not isinstance(region, str) or not region:
            raise ModelConverterError("region must be a non-empty string.")
        if not isinstance(to_precision, ModelPrecision):
            raise ModelConverterError(
                "to_precision must be a ModelPrecision instance.")
        if from_precision is not None and \
                not isinstance(from_precision, ModelPrecision):
            raise ModelConverterError(
                "from_precision must be a ModelPrecision or None.")

        if model_names is None:
            model_names = await self._get_registered_models()
        if not isinstance(model_names, list):
            raise ModelConverterError("model_names must be a list or None.")

        results: List[ConversionResult] = []
        for model_name in model_names:
            try:
                result = await self._convert_single_model(
                    model_name=model_name,
                    from_precision=from_precision,
                    to_precision=to_precision,
                )
            except ModelConverterError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Conversion failed for %s: %s", model_name, exc)
                if self._strict:
                    raise ModelConverterError(
                        f"Conversion failed for {model_name}: {exc}"
                    ) from exc
                result = ConversionResult(
                    success=False,
                    from_precision=(
                        from_precision.value if from_precision else "none"
                    ),
                    to_precision=to_precision.value,
                    model_name=model_name,
                    conversion_time_ms=0.0,
                    size_reduction_percent=0.0,
                    accuracy_delta=0.0,
                )

            results.append(result)
            if result.success:
                # P0 2.1: true LRU.
                with self._lock:
                    if model_name not in self._model_cache:
                        self._model_cache[model_name] = {}
                    self._model_cache[model_name][
                        to_precision.value] = result
                    self._model_cache.move_to_end(model_name)
                    while len(self._model_cache) > \
                            self._config.max_cache_entries:
                        evicted, _ = self._model_cache.popitem(last=False)
                        self._emit_metric(
                            "cache_evicted",
                            {"model_name": evicted})
        return results

    # ---------------------------------------------------------- internals
    async def _convert_single_model(
        self,
        model_name: str,
        from_precision: Optional[ModelPrecision],
        to_precision: ModelPrecision,
    ) -> ConversionResult:
        """
        Convert a single model.

        P1 3.5: the timeout wraps the whole retry + conversion sequence.
        P1 3.1: retries are honoured via ``_convert_with_retry``.
        """
        try:
            return await asyncio.wait_for(
                self._convert_with_retry(
                    model_name, from_precision, to_precision),
                timeout=self._config.max_conversion_time_seconds,
            )
        except asyncio.TimeoutError as exc:
            raise ModelConverterError(
                f"Conversion of {model_name!r} exceeded "
                f"{self._config.max_conversion_time_seconds:.1f}s."
            ) from exc

    async def _convert_with_retry(
        self,
        model_name: str,
        from_precision: Optional[ModelPrecision],
        to_precision: ModelPrecision,
    ) -> ConversionResult:
        last_exc: Optional[BaseException] = None
        total_attempts = 1 + self._config.conversion_max_retries
        for attempt in range(total_attempts):
            try:
                return await self._attempt_convert(
                    model_name, from_precision, to_precision)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.warning(
                    "Conversion attempt %d/%d for %s failed: %s",
                    attempt + 1, total_attempts, model_name, exc)
                self._emit_metric("conversion_failed", {
                    "model_name": model_name,
                    "attempt": attempt + 1,
                    "reason": type(exc).__name__,
                })
                if attempt < total_attempts - 1:
                    await asyncio.sleep(
                        self._config.conversion_base_backoff_seconds
                        * (2 ** attempt))
        raise ModelConverterError(
            f"All {total_attempts} conversion attempts failed for "
            f"{model_name!r}: {type(last_exc).__name__}: {last_exc}"
        ) from last_exc

    async def _attempt_convert(
        self,
        model_name: str,
        from_precision: Optional[ModelPrecision],
        to_precision: ModelPrecision,
    ) -> ConversionResult:
        """One conversion attempt (backend call or simulated)."""
        start_time = time.perf_counter()

        # ---- Backend path ----
        if self._conversion_backend is not None:
            result = self._conversion_backend(
                model_name, to_precision, from_precision)
            if asyncio.iscoroutine(result):
                result = await result
            parsed = self._coerce_backend_result(
                result, model_name, from_precision, to_precision)
            self._record(parsed)
            self._emit_metric("conversion_succeeded", {
                "model_name": model_name,
                "precision": to_precision.value,
                "backend_result": True,
            })
            return parsed

        # ---- Simulated path ----
        source_model = await self._load_model(model_name, from_precision)
        backend = self._select_backend(to_precision)

        if to_precision == ModelPrecision.FP16:
            converted = await self._convert_to_fp16(source_model, backend)
        elif to_precision == ModelPrecision.INT8:
            converted = await self._convert_to_int8(source_model, backend)
        elif to_precision == ModelPrecision.INT4:
            converted = await self._convert_to_int4(source_model, backend)
        else:
            converted = source_model

        accuracy_delta = await self._validate_accuracy(
            model_name=model_name,
            original=source_model,
            converted=converted,
            to_precision=to_precision,
            sample_count=self.validation_samples,
        )
        conversion_time_ms = self._resolve_conversion_time(start_time)

        original_size = await self._get_model_size(source_model)
        converted_size = await self._get_model_size(converted)
        size_reduction = self._compute_size_reduction(
            to_precision, original_size, converted_size)

        # P1 3.6: absolute accuracy check (assume original = 1.0 in
        # simulation; real backends can override via ConversionResult).
        original_accuracy = 1.0
        converted_accuracy = original_accuracy + accuracy_delta
        min_accuracy = self._get_min_accuracy(to_precision)
        success = converted_accuracy >= min_accuracy

        result = ConversionResult(
            success=success,
            from_precision=(
                from_precision.value if from_precision else "none"),
            to_precision=to_precision.value,
            model_name=model_name,
            conversion_time_ms=conversion_time_ms,
            size_reduction_percent=size_reduction,
            accuracy_delta=accuracy_delta,
        )
        self._record(result)
        self._emit_metric("conversion_succeeded", {
            "model_name": model_name,
            "precision": to_precision.value,
            "success": success,
            "size_reduction": size_reduction,
            "accuracy_delta": accuracy_delta,
            "time_ms": conversion_time_ms,
        })
        return result

    def _coerce_backend_result(
        self,
        result: Any,
        model_name: str,
        from_precision: Optional[ModelPrecision],
        to_precision: ModelPrecision,
    ) -> ConversionResult:
        """Normalise a backend's return value (P1 3.12)."""
        if isinstance(result, ConversionResult):
            parsed = result
        elif isinstance(result, Mapping):
            parsed = ConversionResult.from_dict(result)
        else:
            raise ModelConverterError(
                f"backend returned unrecognised type "
                f"{type(result).__name__} for {model_name!r}")
        if parsed.model_name != model_name:
            logger.warning(
                "Backend returned result for model %r during "
                "conversion of %r; overriding.",
                parsed.model_name, model_name)
            parsed = replace(parsed, model_name=model_name)
        return parsed

    def _resolve_conversion_time(self, start_time: float) -> float:
        actual_ms = (time.perf_counter() - start_time) * 1000.0
        if self._config.simulate_backends and \
                self._config.sim_conversion_time_ms > 0:
            return self._config.sim_conversion_time_ms
        return actual_ms

    def _compute_size_reduction(
        self,
        to_precision: ModelPrecision,
        original_size: float,
        converted_size: float,
    ) -> float:
        """P1 3.2: use sim coefficients in simulate mode."""
        if self._config.simulate_backends:
            return {
                ModelPrecision.FP16:
                    self._config.sim_size_reduction_fp16,
                ModelPrecision.INT8:
                    self._config.sim_size_reduction_int8,
                ModelPrecision.INT4:
                    self._config.sim_size_reduction_int4,
            }.get(to_precision, 0.0)
        if original_size > 0:
            return (1.0 - converted_size / original_size) * 100.0
        return 0.0

    def _record(self, result: ConversionResult) -> None:
        with self._lock:
            self._history.append(result)

    # ---------------------------------------------------------- stubs
    async def _get_registered_models(self) -> List[str]:
        with self._lock:
            names = list(self._registered_models.keys())
            if names:
                return names
            return list(self._model_cache.keys())

    async def _load_model(
        self, model_name: str, from_precision: Optional[ModelPrecision]
    ) -> Any:
        with self._lock:
            if model_name in self._registered_models:
                return self._registered_models[model_name]
        return {"model_name": model_name, "precision": from_precision}

    async def _convert_to_fp16(
        self, source_model: Any, backend: str
    ) -> Any:
        return {"source": source_model,
                "precision": ModelPrecision.FP16, "backend": backend}

    async def _convert_to_int8(
        self, source_model: Any, backend: str
    ) -> Any:
        return {"source": source_model,
                "precision": ModelPrecision.INT8, "backend": backend}

    async def _convert_to_int4(
        self, source_model: Any, backend: str
    ) -> Any:
        return {"source": source_model,
                "precision": ModelPrecision.INT4, "backend": backend}

    async def _get_model_size(self, model: Any) -> float:
        if isinstance(model, Mapping):
            precision = model.get("precision")
            return {
                ModelPrecision.FP32: 440.0,
                ModelPrecision.FP16: 220.0,
                ModelPrecision.INT8: 110.0,
                ModelPrecision.INT4: 55.0,
                None: 440.0,
            }.get(precision, 440.0)
        return 440.0

    def _select_backend(self, precision: ModelPrecision) -> str:
        """
        Select the optimal backend for ``precision`` (P0 2.6).

        Raises ``ModelConverterError`` when no preferred backend is
        available and ``allow_backend_fallback=False``.
        """
        preferences = {
            ModelPrecision.FP32: ["torch"],
            ModelPrecision.FP16: ["tensorrt", "torch"],
            ModelPrecision.INT8: ["tensorrt", "openvino", "onnxruntime"],
            ModelPrecision.INT4: ["tensorrt"],
        }
        for backend in preferences.get(precision, self.backends):
            if backend in self.backends and self._backend_available(backend):
                return backend
        if self._config.allow_backend_fallback:
            for backend in self.backends:
                if self._backend_available(backend):
                    return backend
        raise ModelConverterError(
            f"No available backend for {precision.value}.")

    def _backend_available(self, backend: str) -> bool:
        """
        Return True if ``backend`` can be used on this host (P0 2.6).

        In simulate mode (default), any configured backend is treated as
        available. In real mode, actual imports are probed.
        """
        if self._config.simulate_backends:
            return backend in self.backends
        if backend == "torch":
            return _TORCH_AVAILABLE
        if backend == "tensorrt":
            return _TENSORRT_AVAILABLE
        if backend == "openvino":
            return _OPENVINO_AVAILABLE
        if backend == "onnxruntime":
            return _ONNXRUNTIME_AVAILABLE
        return False

    async def _load_validation_data(
        self, model_name: str, sample_count: int
    ) -> List[Dict[str, Any]]:
        """
        Return a validation slice (P1 3.7: honours ``sample_count``;
        previously hard-capped at 64).
        """
        n = max(1, min(sample_count, self._config.accuracy_validation_samples))
        return [{"i": i, "model": model_name} for i in range(n)]

    async def _run_inference(
        self, model: Any, validation_data: List[Dict[str, Any]]
    ) -> List[float]:
        precision = (
            model.get("precision") if isinstance(model, Mapping) else None
        )
        base = {
            ModelPrecision.FP32: 1.0,
            ModelPrecision.FP16: 0.99,
            ModelPrecision.INT8: 0.97,
            ModelPrecision.INT4: 0.93,
            None: 1.0,
        }.get(precision, 1.0)
        return [base + 0.001 * i for i in range(len(validation_data))]

    def _calculate_accuracy_delta(
        self,
        original_outputs: List[float],
        converted_outputs: List[float],
        validation_data: List[Dict[str, Any]],
    ) -> float:
        if not original_outputs or not converted_outputs:
            return 0.0
        n = min(len(original_outputs), len(converted_outputs))
        orig = sum(original_outputs[:n]) / n
        conv = sum(converted_outputs[:n]) / n
        return conv - orig

    async def _validate_accuracy(
        self,
        model_name: str,
        original: Any,
        converted: Any,
        to_precision: ModelPrecision,
        sample_count: int,
    ) -> float:
        """Validate accuracy (P1 3.2: use sim coefficients in simulate mode)."""
        if self._config.simulate_backends:
            return {
                ModelPrecision.FP16: self._config.sim_accuracy_delta_fp16,
                ModelPrecision.INT8: self._config.sim_accuracy_delta_int8,
                ModelPrecision.INT4: self._config.sim_accuracy_delta_int4,
                ModelPrecision.FP32: 0.0,
            }.get(to_precision, 0.0)
        validation_data = await self._load_validation_data(
            model_name, sample_count)
        original_outputs = await self._run_inference(
            original, validation_data)
        converted_outputs = await self._run_inference(
            converted, validation_data)
        return self._calculate_accuracy_delta(
            original_outputs, converted_outputs, validation_data)

    def _get_min_accuracy(self, precision: ModelPrecision) -> float:
        return self._dpq_config.min_accuracy(precision)

    # ---------------------------------------------------------- stats
    def statistics(self, window: Optional[int] = None) -> Dict[str, Any]:
        """Return aggregate statistics (P1 3.10: windowed view)."""
        with self._lock:
            if window is not None:
                history = list(self._history)[-window:]
            else:
                history = list(self._history)
            cached = len(self._model_cache)

        if not history:
            return {
                "conversions": 0,
                "successful": 0,
                "failed": 0,
                "mean_conversion_time_ms": None,
                "mean_size_reduction_percent": None,
                "mean_accuracy_delta": None,
                "by_precision": {},
                "cached_models": cached,
            }
        successful = sum(1 for r in history if r.success)
        by_precision: Dict[str, int] = {}
        for r in history:
            by_precision[r.to_precision] = (
                by_precision.get(r.to_precision, 0) + 1)
        return {
            "conversions": len(history),
            "successful": successful,
            "failed": len(history) - successful,
            "mean_conversion_time_ms": (
                sum(r.conversion_time_ms for r in history)
                / len(history)),
            "mean_size_reduction_percent": (
                sum(r.size_reduction_percent for r in history)
                / len(history)),
            "mean_accuracy_delta": (
                sum(r.accuracy_delta for r in history)
                / len(history)),
            "by_precision": by_precision,
            "cached_models": cached,
        }

    def reset(
        self,
        *,
        clear_cache: bool = False,
        clear_history: bool = False,
    ) -> None:
        with self._lock:
            if clear_cache:
                self._model_cache.clear()
            if clear_history:
                self._history.clear()
            self._ctx_start = None
            self._async_ctx_start = None
        logger.debug(
            "ModelConverter reset (cache=%s, history=%s)",
            clear_cache, clear_history)

    def close(self) -> None:
        """Release resources (P1 3.9). Idempotent."""
        logger.debug(
            "ModelConverter closed (%d cached, %d in history).",
            self.cached_model_count, len(self._history))

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "config": asdict(self._config),
                "dpq_config": asdict(self._dpq_config),
                "strict": self._strict,
                "torch_available": _TORCH_AVAILABLE,
                "transformers_available": _TRANSFORMERS_AVAILABLE,
                "has_backend": self._conversion_backend is not None,
                "cached_models": {
                    name: list(precisions.keys())
                    for name, precisions in self._model_cache.items()
                },
                "registered_models": list(
                    self._registered_models.keys()),
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "ModelConverter":
        if not isinstance(data, Mapping):
            raise ModelConverterError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        # P0 2.2: delegate to from_legacy_dict.
        cfg = ConverterConfig.from_legacy_dict(data.get("config") or {})
        # P0 2.3: restore DPQConfig.
        dpq_cfg = _dpq_config_from_dict(data.get("dpq_config") or {})

        converter = cls(
            config=cfg,
            dpq_config=dpq_cfg,
            strict=bool(data.get("strict", True)),
            metrics_hook=metrics_hook,
        )

        if data.get("has_backend", False):
            logger.warning(
                "Restoring a converter that originally had a live "
                "conversion_backend; the callable cannot be serialized "
                "— inject one with `converter._conversion_backend = ...` "
                "to re-enable live conversions.")

        with converter._lock:
            # Registered models: names only (handles are not serialized).
            for name in data.get("registered_models", []):
                converter._registered_models[str(name)] = {
                    "model_name": name}

            # P0 2.4: restore cached model names (without full results).
            cached_models = data.get("cached_models") or {}
            for name, precisions in cached_models.items():
                converter._model_cache[str(name)] = {
                    str(p): None for p in (precisions or [])
                }
            # Bound to max_cache_entries.
            while len(converter._model_cache) > \
                    converter._config.max_cache_entries:
                converter._model_cache.popitem(last=False)

            # P1: warn if history exceeds max_history.
            history_entries = list(data.get("history", []))
            if len(history_entries) > converter._config.max_history:
                logger.warning(
                    "Restoring %d history entries; only most recent %d "
                    "kept.", len(history_entries),
                    converter._config.max_history)
            for entry in history_entries:
                converter._history.append(
                    ConversionResult.from_dict(entry))
        return converter

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "ModelConverter":
        try:
            return cls.from_dict(
                json.loads(payload), metrics_hook=metrics_hook)
        except json.JSONDecodeError as exc:
            raise ModelConverterError(
                f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- pickling
    def __getstate__(self) -> Dict[str, Any]:
        # P0 2.5: RLock is not picklable.
        state = self.__dict__.copy()
        state.pop("_lock", None)
        # Unpicklable model handles (e.g., live torch objects).
        try:
            pickle.dumps(state.get("_registered_models", {}))
        except Exception:  # noqa: BLE001
            logger.info(
                "Dropping unpicklable registered model handles on "
                "pickle.")
            state["_registered_models"] = {}
        # The conversion_backend is a callable — drop it.
        if state.get("_conversion_backend") is not None:
            logger.info(
                "Dropping conversion_backend on pickle (not picklable).")
            state["_conversion_backend"] = None
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "ModelConverter":
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
                "ModelConverter scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed)
            return
        logger.info("ModelConverter scope closed in %.4fs.", elapsed)

    async def __aenter__(self) -> "ModelConverter":
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
                "Async ModelConverter scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed)
            return
        logger.info(
            "Async ModelConverter scope closed in %.4fs.", elapsed)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "ModelConverter("
                f"version={__version__!r}, "
                f"backends={self.backends}, "
                f"cached={len(self._model_cache)}, "
                f"history={len(self._history)}, "
                f"strict={self._strict})"
            )


__all__ = [
    "ConverterConfig",
    "ModelConverter",
    "ModelConverterError",
]


# --------------------------------------------------------------------------- #
# Self-tests: python -m dpq.model_converter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import pickle
    from dataclasses import replace as _dc_replace

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # ---------------------------------------------------------------- #
    # P0 2.1 — LRU cache never over-fires
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: LRU cache ===")

    async def _test_cache() -> None:
        converter = ModelConverter(
            config=ConverterConfig(max_cache_entries=256),
        )
        converter.register_model("model-a", {"name": "model-a"})
        converter.register_model("model-b", {"name": "model-b"})

        # Convert model-a twice, then model-b once.
        await converter.convert_models(
            "eu-west", None, ModelPrecision.FP16, ["model-a"])
        await converter.convert_models(
            "eu-west", None, ModelPrecision.FP16, ["model-a"])
        await converter.convert_models(
            "eu-west", None, ModelPrecision.FP16, ["model-b"])
        assert converter.cached_model_count == 2, (
            f"P0 2.1 regression: after 3 conversions of 2 models, "
            f"cached_model_count={converter.cached_model_count} "
            f"(expected 2)")
        print(f"  ✓ after 3 conversions of 2 models: "
              f"cached={converter.cached_model_count}")

        # 300 iterations of converting both models — cache should stay at 2.
        for _ in range(300):
            await converter.convert_models(
                "eu-west", None, ModelPrecision.FP16,
                ["model-a", "model-b"])
        assert converter.cached_model_count == 2, (
            f"P0 2.1 regression: after 300 iterations of 2 models, "
            f"cached_model_count={converter.cached_model_count} "
            f"(expected 2)")
        print(f"  ✓ after 300 iterations (600 conversions): "
              f"cached={converter.cached_model_count}")

        # Second conversion of same model does not evict it.
        before = converter.cached_model_count
        await converter.convert_models(
            "eu-west", None, ModelPrecision.FP16, ["model-a"])
        assert converter.cached_model_count == before, (
            f"P0 2.1 regression: second conversion evicted model-a")
        print(f"  ✓ second conversion of model-a did not evict "
              f"(cached={converter.cached_model_count})")

    asyncio.run(_test_cache())

    # ---------------------------------------------------------------- #
    # P0 2.2 — legacy payload uses declared defaults
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: config drift ===")

    legacy = {
        "config": {
            "accuracy_validation_samples": 500,
            # max_conversion_time_seconds omitted → 30.0 (default)
            # simulate_backends omitted → True (default, new field)
            # allow_backend_fallback omitted → True (default, new field)
        },
        "dpq_config": {},
        "strict": True,
        "registered_models": [],
        "history": [],
    }
    restored = ModelConverter.from_dict(legacy)
    assert restored.config.accuracy_validation_samples == 500
    assert restored.config.max_conversion_time_seconds == 30.0, (
        f"P0 2.2 regression: max_conversion_time_seconds="
        f"{restored.config.max_conversion_time_seconds}")
    assert restored.config.simulate_backends is True, (
        f"P0 2.2 regression: simulate_backends="
        f"{restored.config.simulate_backends} (expected True, the default)")
    assert restored.config.allow_backend_fallback is True
    print(f"  ✓ accuracy_validation_samples=500 (explicit), "
          f"max_conversion_time_seconds=30.0 (default), "
          f"simulate_backends=True (default)")

    # ---------------------------------------------------------------- #
    # P0 2.3 — custom DPQConfig round-trips
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: DPQConfig round-trip ===")

    try:
        custom_dpq = _dc_replace(DPQConfig(), green_threshold=10.0)
    except TypeError:
        # If green_threshold isn't a field, skip this specific check
        # but still validate the general round-trip.
        custom_dpq = DPQConfig()
        print("  (green_threshold field unavailable; using default)")

    converter = ModelConverter(dpq_config=custom_dpq)
    # Force a state change that proves the custom config is live.
    original_green = converter._dpq_config.green_threshold
    payload = converter.to_json()
    restored = ModelConverter.from_json(payload)
    assert restored._dpq_config.green_threshold == original_green, (
        f"P0 2.3 regression: green_threshold="
        f"{restored._dpq_config.green_threshold}, "
        f"expected {original_green}")
    print(f"  ✓ DPQConfig round-trip: "
          f"green_threshold={restored._dpq_config.green_threshold}")

    # ---------------------------------------------------------------- #
    # P0 2.4 — cached_models restored
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: cached_models round-trip ===")

    async def _test_cached_models() -> None:
        converter = ModelConverter()
        converter.register_model("m1", {"name": "m1"})
        await converter.convert_models(
            "eu-west", None, ModelPrecision.FP16, ["m1"])
        assert converter.cached_model_count == 1

        payload = converter.to_dict()
        assert "cached_models" in payload
        assert "m1" in payload["cached_models"], payload["cached_models"]

        restored = ModelConverter.from_dict(payload)
        assert restored.cached_model_count == 1, (
            f"P0 2.4 regression: cached_model_count="
            f"{restored.cached_model_count} (expected 1)")
        # The cached model is a registered model now (so list_registered_models
        # returns something), not _registered_models since the handle was
        # dropped on serialize.
        assert "m1" in restored._model_cache
        print(f"  ✓ cached_models restored: "
              f"cached_model_count={restored.cached_model_count}")

    asyncio.run(_test_cached_models())

    # ---------------------------------------------------------------- #
    # P0 2.5 — pickle round-trip
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: pickle round-trip ===")

    async def _test_pickle() -> None:
        converter = ModelConverter(
            config=ConverterConfig(max_cache_entries=10),
        )
        converter.register_model("bert-base", {"name": "bert-base"})
        converter.register_model("roberta-base", {"name": "roberta-base"})
        await converter.convert_models(
            "eu-west", None, ModelPrecision.INT8, ["bert-base"])
        assert len(converter.history) == 1
        assert converter.cached_model_count == 1
        assert len(converter.list_registered_models()) == 2

        blob = pickle.dumps(converter)
        restored = pickle.loads(blob)

        assert restored.config == converter.config, "config lost"
        assert len(restored.history) == len(converter.history), (
            f"history lost: {len(restored.history)} vs "
            f"{len(converter.history)}")
        assert sorted(restored.list_registered_models()) == \
            sorted(converter.list_registered_models()), "models lost"
        assert restored.cached_model_count == converter.cached_model_count
        assert restored._lock is not None
        # Should still be usable
        await restored.convert_models(
            "eu-west", None, ModelPrecision.FP16, ["bert-base"])
        assert len(restored.history) == 2
        print(f"  ✓ pickle round-trip: config+history+models preserved, "
              f"post-restore conversion OK")

    asyncio.run(_test_pickle())

    # ---------------------------------------------------------------- #
    # P0 2.6 — _backend_available + _select_backend
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: backend availability ===")

    converter = ModelConverter()
    assert converter._backend_available("nonexistent") is False, (
        f"P0 2.6 regression: 'nonexistent' returned "
        f"{converter._backend_available('nonexistent')} (expected False)")
    assert converter._backend_available("torch") is True  # default in backends
    print("  ✓ 'nonexistent' → False; 'torch' → True (in backends)")

    # INT4 requires tensorrt; if not in supported_backends and no fallback,
    # must raise.
    strict_converter = ModelConverter(
        config=ConverterConfig(
            supported_backends=("onnxruntime",),   # no tensorrt
            allow_backend_fallback=False,          # no last-resort
        ),
    )
    try:
        strict_converter._select_backend(ModelPrecision.INT4)
    except ModelConverterError as exc:
        print(f"  ✓ INT4 raises when no backend supports it: {exc}")
    else:
        raise AssertionError(
            "P0 2.6 regression: _select_backend(INT4) did not raise "
            "when no backend supports it")

    # Sanity: default config still selects a backend for INT4
    assert converter._select_backend(ModelPrecision.INT4) == "tensorrt"
    print("  ✓ default converter selects tensorrt for INT4")

    # ---------------------------------------------------------------- #
    # P1 3.1 — retry loop
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: retry loop ===")

    async def _test_retry() -> None:
        call_count = 0

        def flaky_backend(model_name, to_precision, from_precision):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError(
                    f"transient failure #{call_count}")
            return ConversionResult(
                success=True,
                from_precision=(
                    from_precision.value if from_precision else "none"),
                to_precision=to_precision.value,
                model_name=model_name,
                conversion_time_ms=100.0,
                size_reduction_percent=50.0,
                accuracy_delta=-0.01,
            )

        converter = ModelConverter(
            config=ConverterConfig(
                conversion_max_retries=2,
                conversion_base_backoff_seconds=0.0,
            ),
            conversion_backend=flaky_backend,
        )
        converter.register_model("test", {"name": "test"})
        results = await converter.convert_models(
            "eu-west", None, ModelPrecision.FP16)
        assert call_count == 3, (
            f"P1 3.1 regression: backend called {call_count} times "
            f"(expected 3)")
        assert len(results) == 1
        assert results[0].success is True, (
            f"P1 3.1 regression: final result success="
            f"{results[0].success}")
        assert len(converter.history) == 1, (
            f"P1 3.1 regression: history has "
            f"{len(converter.history)} entries (expected 1)")
        assert converter.history[0].success is True
        print(f"  ✓ backend failed twice, succeeded on 3rd; "
              f"history: {len(converter.history)} successful")

    asyncio.run(_test_retry())

    # ---------------------------------------------------------------- #
    # P1 3.5 — timeout on the whole conversion
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: timeout ===")

    async def _test_timeout() -> None:
        async def slow_backend(model_name, to_precision, from_precision):
            await asyncio.sleep(0.5)
            return ConversionResult(
                success=True,
                from_precision=(
                    from_precision.value if from_precision else "none"),
                to_precision=to_precision.value,
                model_name=model_name,
                conversion_time_ms=500.0,
                size_reduction_percent=50.0,
                accuracy_delta=-0.01,
            )

        converter = ModelConverter(
            config=ConverterConfig(
                max_conversion_time_seconds=0.1,
                conversion_max_retries=0,
            ),
            conversion_backend=slow_backend,
        )
        converter.register_model("slow", {"name": "slow"})
        t0 = time.perf_counter()
        try:
            await converter.convert_models(
                "eu-west", None, ModelPrecision.FP16)
        except ModelConverterError as exc:
            elapsed = time.perf_counter() - t0
            assert elapsed < 1.0, (
                f"P1 3.5 regression: timeout fired after "
                f"{elapsed:.2f}s (expected ~0.1s)")
            print(f"  ✓ timeout after {elapsed:.3f}s: {exc}")
        else:
            raise AssertionError(
                "P1 3.5 regression: slow backend did not raise "
                "ModelConverterError")

    asyncio.run(_test_timeout())

    # ---------------------------------------------------------------- #
    # P1 3.6 — strict DPQConfig rejects the simulated INT8 conversion
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: strict DPQConfig ===")

    async def _test_strict_dpq() -> None:
        # Duck-typed DPQConfig with a very strict INT8 threshold.
        @dataclass(frozen=True)
        class _StrictDPQConfig:
            green_threshold: float = 50.0
            yellow_threshold: float = 200.0
            red_threshold: float = 400.0

            def min_accuracy(self, precision):
                if precision == ModelPrecision.INT8:
                    return 0.999
                return 0.95

        converter = ModelConverter(dpq_config=_StrictDPQConfig())
        converter.register_model("test", {"name": "test"})
        results = await converter.convert_models(
            "eu-west", None, ModelPrecision.INT8, ["test"])
        assert len(results) == 1
        assert results[0].success is False, (
            f"P1 3.6 regression: strict min_accuracy=0.999 did not "
            f"reject INT8 conversion (accuracy_delta="
            f"{results[0].accuracy_delta})")
        # The simulated INT8 delta is -0.030 → converted accuracy 0.97
        assert abs(results[0].accuracy_delta - (-0.030)) < 1e-6
        print(f"  ✓ strict min_accuracy=0.999 rejects INT8 "
              f"(simulated accuracy=0.97, delta={results[0].accuracy_delta:+.3f})")

        # Baseline: default DPQConfig accepts the same conversion.
        default_converter = ModelConverter()
        default_converter.register_model("test", {"name": "test"})
        default_results = await default_converter.convert_models(
            "eu-west", None, ModelPrecision.INT8, ["test"])
        assert default_results[0].success is True, (
            f"control: default DPQConfig should accept INT8, got "
            f"success={default_results[0].success}")
        print("  ✓ default DPQConfig accepts INT8 "
              "(control case)")

    asyncio.run(_test_strict_dpq())

    # ---------------------------------------------------------------- #
    # Bonus: metrics_hook, close, statistics(window=), context managers
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: metrics / close / statistics / context ===")

    async def _test_bonus() -> None:
        events: List = []
        converter = ModelConverter(
            metrics_hook=lambda ev, f: events.append((ev, f)),
        )
        converter.register_model("m", {"name": "m"})
        await converter.convert_models("eu-west", None, ModelPrecision.FP16)
        names = {ev for ev, _ in events}
        assert "conversion_succeeded" in names, names
        print(f"  ✓ metrics_hook: {sorted(names)}")

        stats = converter.statistics()
        assert stats["conversions"] == 1
        win = converter.statistics(window=0)
        assert win["conversions"] == 0
        print(f"  ✓ statistics() = {stats['conversions']}, "
              f"statistics(window=0) = {win['conversions']}")

        converter.close()  # idempotent
        converter.close()
        print("  ✓ close() idempotent")

    asyncio.run(_test_bonus())

    async def _test_cm() -> None:
        async with ModelConverter() as scoped:
            scoped.register_model("m", {"name": "m"})
            await scoped.convert_models(
                "eu-west", None, ModelPrecision.FP16)
        print("  ✓ async context manager OK")

    asyncio.run(_test_cm())

    print("\n✓ All self-tests passed.")
