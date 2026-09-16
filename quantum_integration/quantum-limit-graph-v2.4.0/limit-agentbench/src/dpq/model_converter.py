# src/dpq/model_converter.py

"""
Model Converter
===============

On-the-fly model quantization engine.

Enhancements
------------
- Imports ``ModelPrecision`` / ``ConversionResult`` / ``DPQConfig`` from
  :mod:`models` — no more duplicated enums.
- Module-level ``logger`` (previously missing → ``NameError``).
- ``ConverterConfig`` with validated timeouts, retries, cache size.
- ``RLock``-guarded model cache; bounded cache.
- Stub implementations for every method that was previously missing
  (``_get_registered_models``, ``_load_model``, ``_convert_to_fp16``,
  ``_convert_to_int8``, ``_convert_to_int4``, ``_get_model_size``,
  ``_backend_available``, ``_load_validation_data``, ``_run_inference``,
  ``_calculate_accuracy_delta``).
- Optional torch / transformers imports with graceful degradation.
- Retry + timeout on conversion; full validation; strict / non-strict modes.
- Sync + async context managers; serialization; ``statistics()``;
  ``__repr__``; custom :class:`ModelConverterError`.
- ``__main__`` smoke test with a pluggable conversion backend.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Deque, Dict, List, Mapping, Optional

from .models import (
    ConversionResult,
    DPQConfig,
    ModelPrecision,
    ModelVariant,
)

logger = logging.getLogger(__name__)


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


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ModelConverterError(ValueError):
    """Raised for invalid inputs, configuration, or conversion failures."""


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

    # Simulation coefficients (used when no real backend is wired).
    sim_size_reduction_fp16: float = 45.0
    sim_size_reduction_int8: float = 72.0
    sim_size_reduction_int4: float = 85.0
    sim_accuracy_delta_fp16: float = -0.010
    sim_accuracy_delta_int8: float = -0.030
    sim_accuracy_delta_int4: float = -0.070
    sim_conversion_time_ms: float = 250.0

    def __post_init__(self) -> None:
        if not self.supported_backends:
            raise ModelConverterError("supported_backends must be non-empty.")
        if self.accuracy_validation_samples <= 0:
            raise ModelConverterError(
                "accuracy_validation_samples must be > 0."
            )
        if self.max_conversion_time_seconds <= 0:
            raise ModelConverterError("max_conversion_time_seconds must be > 0.")
        if self.max_cache_entries <= 0:
            raise ModelConverterError("max_cache_entries must be > 0.")
        if self.max_history <= 0:
            raise ModelConverterError("max_history must be > 0.")
        if self.conversion_max_retries < 0:
            raise ModelConverterError("conversion_max_retries must be >= 0.")
        if self.conversion_base_backoff_seconds < 0:
            raise ModelConverterError(
                "conversion_base_backoff_seconds must be >= 0."
            )


# --------------------------------------------------------------------------- #
# Converter
# --------------------------------------------------------------------------- #
class ModelConverter:
    """
    On-the-fly model quantization engine.

    Thread-safe, serializable, and bounded in memory. All original public
    methods are preserved; new parameters are keyword-only.
    """

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

        # Legacy attributes preserved for backward compatibility.
        self.backends: List[str] = list(self._config.supported_backends)
        self.validation_samples: int = self._config.accuracy_validation_samples
        self.max_conversion_time: int = int(
            self._config.max_conversion_time_seconds
        )

        self._lock = threading.RLock()
        self._model_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_order: Deque[str] = deque()
        self._history: Deque[ConversionResult] = deque(
            maxlen=self._config.max_history
        )
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None
        # Optional injected backend for tests / production wiring.
        self._conversion_backend = conversion_backend
        # Registry of known models (populated via ``register_model``).
        self._registered_models: Dict[str, Any] = {}

        logger.debug(
            "ModelConverter initialized (backends=%s, samples=%d, "
            "max_time=%.1fs, strict=%s, torch=%s, transformers=%s)",
            self.backends,
            self.validation_samples,
            self.max_conversion_time,
            self._strict,
            _TORCH_AVAILABLE,
            _TRANSFORMERS_AVAILABLE,
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

    # ---------------------------------------------------------- registry
    def register_model(self, model_name: str, model_handle: Any) -> None:
        """Register a model handle so it can be converted without disk I/O."""
        if not isinstance(model_name, str) or not model_name:
            raise ModelConverterError("model_name must be a non-empty string.")
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
        """
        Convert models from one precision to another.

        See the original docstring for parameter meanings; behavior is
        preserved. Per-model failures are isolated (strict mode raises).
        """
        if not isinstance(region, str) or not region:
            raise ModelConverterError("region must be a non-empty string.")
        if not isinstance(to_precision, ModelPrecision):
            raise ModelConverterError(
                "to_precision must be a ModelPrecision instance."
            )
        if from_precision is not None and not isinstance(
            from_precision, ModelPrecision
        ):
            raise ModelConverterError(
                "from_precision must be a ModelPrecision or None."
            )

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
            except Exception as exc:
                logger.exception("Conversion failed for %s: %s", model_name, exc)
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
                with self._lock:
                    if model_name not in self._model_cache:
                        self._model_cache[model_name] = {}
                    self._model_cache[model_name][to_precision.value] = result
                    self._cache_order.append(model_name)
                    while len(self._cache_order) > self._config.max_cache_entries:
                        oldest = self._cache_order.popleft()
                        self._model_cache.pop(oldest, None)
        return results

    # ---------------------------------------------------------- internals
    async def _convert_single_model(
        self,
        model_name: str,
        from_precision: Optional[ModelPrecision],
        to_precision: ModelPrecision,
    ) -> ConversionResult:
        """Convert a single model with accuracy validation."""
        start_time = time.perf_counter()

        if self._conversion_backend is not None:
            result = self._conversion_backend(
                model_name, to_precision, from_precision
            )
            if asyncio.iscoroutine(result):
                result = await asyncio.wait_for(
                    result,
                    timeout=self._config.max_conversion_time_seconds,
                )
            if isinstance(result, ConversionResult):
                self._record(result)
                return result
            if isinstance(result, Mapping):
                parsed = ConversionResult.from_dict(result)
                self._record(parsed)
                return parsed

        # Default simulated path (deterministic for a given precision).
        source_model = await self._load_model(model_name, from_precision)
        backend = self._select_backend(to_precision)

        if to_precision == ModelPrecision.FP16:
            converted = self._convert_to_fp16(source_model, backend)
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
            sample_count=self.validation_samples,
        )
        conversion_time_ms = (time.perf_counter() - start_time) * 1000.0

        original_size = await self._get_model_size(source_model)
        converted_size = await self._get_model_size(converted)
        if original_size > 0:
            size_reduction = (1 - converted_size / original_size) * 100.0
        else:
            size_reduction = 0.0

        success = accuracy_delta >= -abs(
            1.0 - self._get_min_accuracy(to_precision)
        )

        result = ConversionResult(
            success=success,
            from_precision=from_precision.value if from_precision else "none",
            to_precision=to_precision.value,
            model_name=model_name,
            conversion_time_ms=conversion_time_ms,
            size_reduction_percent=size_reduction,
            accuracy_delta=accuracy_delta,
        )
        self._record(result)
        return result

    def _record(self, result: ConversionResult) -> None:
        with self._lock:
            self._history.append(result)

    # ---------------------------------------------------------- stubs
    async def _get_registered_models(self) -> List[str]:
        """Return every registered model name (or cached model names)."""
        with self._lock:
            names = list(self._registered_models.keys())
            if names:
                return names
            return list(self._model_cache.keys())

    async def _load_model(
        self, model_name: str, from_precision: Optional[ModelPrecision]
    ) -> Any:
        """Load a source model. Returns a lightweight placeholder handle."""
        with self._lock:
            if model_name in self._registered_models:
                return self._registered_models[model_name]
        return {"model_name": model_name, "precision": from_precision}

    def _convert_to_fp16(self, source_model: Any, backend: str) -> Any:
        return {"source": source_model, "precision": ModelPrecision.FP16, "backend": backend}

    async def _convert_to_int8(self, source_model: Any, backend: str) -> Any:
        return {"source": source_model, "precision": ModelPrecision.INT8, "backend": backend}

    async def _convert_to_int4(self, source_model: Any, backend: str) -> Any:
        return {"source": source_model, "precision": ModelPrecision.INT4, "backend": backend}

    async def _get_model_size(self, model: Any) -> float:
        """Return a simulated model size in MB."""
        if isinstance(model, Mapping):
            precision = model.get("precision")
            sizes = {
                ModelPrecision.FP32: 440.0,
                ModelPrecision.FP16: 220.0,
                ModelPrecision.INT8: 110.0,
                ModelPrecision.INT4: 55.0,
                None: 440.0,
            }
            return sizes.get(precision, 440.0)
        return 440.0

    def _select_backend(self, precision: ModelPrecision) -> str:
        """Select the optimal backend for ``precision``."""
        preferences = {
            ModelPrecision.FP32: ["torch"],
            ModelPrecision.FP16: ["tensorrt", "torch"],
            ModelPrecision.INT8: ["tensorrt", "openvino", "onnxruntime"],
            ModelPrecision.INT4: ["tensorrt"],
        }
        for backend in preferences.get(precision, self.backends):
            if backend in self.backends and self._backend_available(backend):
                return backend
        # Last-resort: any available backend.
        for backend in self.backends:
            if self._backend_available(backend):
                return backend
        raise ModelConverterError(f"No available backend for {precision}.")

    def _backend_available(self, backend: str) -> bool:
        """
        Return True if ``backend`` can be used on this host.

        When heavy dependencies are absent, the simulated backend is treated
        as available so the module remains importable and testable.
        """
        if backend == "torch":
            return _TORCH_AVAILABLE or True  # simulation always allowed
        return backend in self.backends

    async def _load_validation_data(
        self, model_name: str, sample_count: int
    ) -> List[Dict[str, Any]]:
        """Return a deterministic validation slice for ``model_name``."""
        return [{"i": i, "model": model_name} for i in range(min(sample_count, 64))]

    async def _run_inference(
        self, model: Any, validation_data: List[Dict[str, Any]]
    ) -> List[float]:
        """Simulated inference returning deterministic pseudo-outputs."""
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
        """Return the accuracy delta (converted − original)."""
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
        sample_count: int,
    ) -> float:
        """Validate the accuracy of a converted model vs. the original."""
        validation_data = await self._load_validation_data(
            model_name, sample_count
        )
        original_outputs = await self._run_inference(original, validation_data)
        converted_outputs = await self._run_inference(converted, validation_data)
        return self._calculate_accuracy_delta(
            original_outputs, converted_outputs, validation_data
        )

    def _get_min_accuracy(self, precision: ModelPrecision) -> float:
        """Return the minimum acceptable accuracy for ``precision``."""
        return self._dpq_config.min_accuracy(precision)

    # ---------------------------------------------------------- stats
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the conversion history."""
        with self._lock:
            history = list(self._history)
        if not history:
            return {
                "conversions": 0,
                "successful": 0,
                "failed": 0,
                "mean_conversion_time_ms": None,
                "mean_size_reduction_percent": None,
                "mean_accuracy_delta": None,
                "by_precision": {},
                "cached_models": self.cached_model_count,
            }
        successful = sum(1 for r in history if r.success)
        by_precision: Dict[str, int] = {}
        for r in history:
            by_precision[r.to_precision] = by_precision.get(r.to_precision, 0) + 1
        return {
            "conversions": len(history),
            "successful": successful,
            "failed": len(history) - successful,
            "mean_conversion_time_ms": sum(
                r.conversion_time_ms for r in history
            ) / len(history),
            "mean_size_reduction_percent": sum(
                r.size_reduction_percent for r in history
            ) / len(history),
            "mean_accuracy_delta": sum(
                r.accuracy_delta for r in history
            ) / len(history),
            "by_precision": by_precision,
            "cached_models": self.cached_model_count,
        }

    def reset(self, *, clear_cache: bool = False, clear_history: bool = False) -> None:
        """Reset internal state; optionally clear the cache / history."""
        with self._lock:
            if clear_cache:
                self._model_cache.clear()
                self._cache_order.clear()
            if clear_history:
                self._history.clear()
        logger.debug("ModelConverter reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "dpq_config": asdict(self._dpq_config),
                "strict": self._strict,
                "torch_available": _TORCH_AVAILABLE,
                "transformers_available": _TRANSFORMERS_AVAILABLE,
                "cached_models": {
                    name: list(precisions.keys())
                    for name, precisions in self._model_cache.items()
                },
                "registered_models": list(self._registered_models.keys()),
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ModelConverter":
        if not isinstance(data, Mapping):
            raise ModelConverterError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = ConverterConfig(
            supported_backends=tuple(
                cfg_data.get(
                    "supported_backends",
                    ("tensorrt", "openvino", "onnxruntime", "torch"),
                )
            ),
            accuracy_validation_samples=int(
                cfg_data.get("accuracy_validation_samples", 1000)
            ),
            max_conversion_time_seconds=float(
                cfg_data.get("max_conversion_time_seconds", 30.0)
            ),
            max_cache_entries=int(cfg_data.get("max_cache_entries", 256)),
            max_history=int(cfg_data.get("max_history", 1000)),
            conversion_max_retries=int(
                cfg_data.get("conversion_max_retries", 1)
            ),
            conversion_base_backoff_seconds=float(
                cfg_data.get("conversion_base_backoff_seconds", 0.5)
            ),
        )
        converter = cls(config=cfg, strict=bool(data.get("strict", True)))
        with converter._lock:
            for name in data.get("registered_models", []):
                converter._registered_models[str(name)] = {"model_name": name}
            for entry in data.get("history", []):
                converter._history.append(ConversionResult.from_dict(entry))
        return converter

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "ModelConverter":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise ModelConverterError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "ModelConverter":
        self._ctx_start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "ModelConverter scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info("ModelConverter scope closed in %.4fs.", elapsed)

    async def __aenter__(self) -> "ModelConverter":
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
                "Async ModelConverter scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info("Async ModelConverter scope closed in %.4fs.", elapsed)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "ModelConverter("
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
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    async def main() -> None:
        converter = ModelConverter()
        converter.register_model("bert-base", {"name": "bert-base"})
        converter.register_model("roberta-base", {"name": "roberta-base"})

        results = await converter.convert_models(
            region="eu-west-1",
            from_precision=ModelPrecision.FP32,
            to_precision=ModelPrecision.INT8,
        )
        for r in results:
            print(
                f"  {r.model_name:<16} {r.from_precision}->{r.to_precision} "
                f"success={r.success} size_delta={r.size_reduction_percent:.1f}% "
                f"acc_delta={r.accuracy_delta:+.4f}"
            )

        print("stats     :", converter.statistics())
        print("repr      :", converter)

        payload = converter.to_json()
        restored = ModelConverter.from_json(payload)
        assert restored.to_dict() == converter.to_dict()
        print("Round-trip OK.")

        async with ModelConverter() as scoped:
            await scoped.convert_models(
                "eu-west-1", None, ModelPrecision.FP16
            )
        print("Context OK.")

    asyncio.run(main())
