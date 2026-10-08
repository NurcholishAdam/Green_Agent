# src/quantum_integration/digital_twin/digital_twin_telemetry.py

"""Thread-safe telemetry with per-instance Prometheus registry."""

from __future__ import annotations

import logging
import threading
from collections import deque
from typing import Any, Deque, Dict, Mapping, Optional

from .digital_twin_helpers import _LazyLock, _percentile, _is_finite_nonneg

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 1

#: One lock shared across every instance so only one Prometheus server
#: can bind to a port and metric names can't collide.
_prom_lock = threading.Lock()
_bound_ports: set = set()

try:  # pragma: no cover - environment-dependent
    from prometheus_client import (
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        start_http_server,
        generate_latest,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False
    CollectorRegistry = None  # type: ignore[assignment]
    Counter = Gauge = Histogram = None  # type: ignore[assignment]
    start_http_server = generate_latest = None  # type: ignore[assignment]


class DigitalTwinTelemetry:
    """Per-instance telemetry with optional Prometheus backend.

    Uses a dedicated ``CollectorRegistry`` so multiple instances can
    coexist. The HTTP server is guarded by a module-level lock and a
    global set of bound ports.
    """

    def __init__(
        self,
        *,
        prometheus_port: Optional[int] = None,
        ring_size: int = 200,
    ) -> None:
        self._lock = _LazyLock()
        self._ring_size = max(1, int(ring_size))
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Deque[float]] = {}
        self._registry = None
        self._metrics: Dict[str, Any] = {}
        self._prometheus_port = prometheus_port

        if PROMETHEUS_AVAILABLE and prometheus_port is not None:
            self._setup_prometheus()

    def _setup_prometheus(self) -> None:
        with _prom_lock:
            if self._prometheus_port in _bound_ports:
                logger.warning(
                    "Prometheus port %s already bound; disabling server.",
                    self._prometheus_port,
                )
                self._prometheus_port = None
                return
            try:
                self._registry = CollectorRegistry()
                self._metrics = {
                    "dt_scenarios_run": Counter(
                        "dt_scenarios_run", "Scenarios run",
                        registry=self._registry,
                    ),
                    "dt_sustainability_score": Gauge(
                        "dt_sustainability_score", "Sustainability score",
                        registry=self._registry,
                    ),
                    "dt_weighted_score": Gauge(
                        "dt_weighted_score", "Weighted score",
                        registry=self._registry,
                    ),
                    "dt_cache_hits": Counter(
                        "dt_cache_hits", "Cache hits",
                        registry=self._registry,
                    ),
                    "dt_cache_misses": Counter(
                        "dt_cache_misses", "Cache misses",
                        registry=self._registry,
                    ),
                    "dt_circuit_breaker_state": Gauge(
                        "dt_circuit_breaker_state", "Breaker state",
                        registry=self._registry,
                    ),
                }
                start_http_server(self._prometheus_port, registry=self._registry)
                _bound_ports.add(self._prometheus_port)
                logger.info(
                    "Prometheus metrics server on port %s.",
                    self._prometheus_port,
                )
            except Exception as exc:  # pragma: no cover - env dependent
                logger.warning("Failed to start Prometheus: %s", exc)
                self._registry = None
                self._metrics = {}
                self._prometheus_port = None

    @staticmethod
    def _make_key(metric: str, tags: Optional[Mapping[str, str]]) -> str:
        if not tags:
            return metric
        if not isinstance(tags, Mapping):
            raise TypeError("tags must be a Mapping or None.")
        pairs = []
        for k, v in tags.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise TypeError("tags keys/values must be str.")
            pairs.append((k, v))
        tag_str = ",".join(f"{k}={v}" for k, v in sorted(pairs))
        return f"{metric}{{{tag_str}}}"

    def increment(
        self,
        metric: str,
        tags: Optional[Mapping[str, str]] = None,
        value: float = 1.0,
    ) -> None:
        key = self._make_key(metric, tags)
        self._counters[key] = self._counters.get(key, 0.0) + value
        pm = self._metrics.get(metric)
        if pm is not None and Counter is not None and isinstance(pm, Counter):
            pm.inc(value)

    def gauge(
        self,
        metric: str,
        value: float,
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        if not _is_finite_nonneg(abs(value)) and value != 0:
            return
        key = self._make_key(metric, tags)
        self._gauges[key] = value
        pm = self._metrics.get(metric)
        if pm is not None and Gauge is not None and isinstance(pm, Gauge):
            pm.set(value)

    def histogram(
        self,
        metric: str,
        value: float,
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        key = self._make_key(metric, tags)
        bucket = self._histograms.get(key)
        if bucket is None:
            bucket = deque(maxlen=self._ring_size)
            self._histograms[key] = bucket
        bucket.append(value)

    def histogram_stats(
        self,
        metric: str,
        tags: Optional[Mapping[str, str]] = None,
    ) -> Dict[str, float]:
        key = self._make_key(metric, tags)
        values = list(self._histograms.get(key, ()))
        if not values:
            return {"count": 0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "max": 0.0}
        return {
            "count": len(values),
            "mean": sum(values) / len(values),
            "p50": _percentile(values, 50),
            "p95": _percentile(values, 95),
            "max": max(values),
        }

    def export(self) -> str:
        if self._registry is not None and generate_latest is not None:
            return generate_latest(self._registry).decode("utf-8")
        lines = []
        for key, value in self._counters.items():
            lines.append(f"# TYPE {key} counter\n{key} {value}")
        for key, value in self._gauges.items():
            lines.append(f"# TYPE {key} gauge\n{key} {value}")
        for key, bucket in self._histograms.items():
            values = list(bucket)
            lines.append(
                f"# TYPE {key} histogram\n"
                f"{key}_count {len(values)}\n"
                f"{key}_sum {sum(values)}"
            )
        return "\n".join(lines)

    def statistics(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "prometheus_available": PROMETHEUS_AVAILABLE,
            "prometheus_port": self._prometheus_port,
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histogram_keys": sorted(self._histograms.keys()),
        }

    def reset(self) -> None:
        self._counters.clear()
        self._gauges.clear()
        self._histograms.clear()

    def close(self, *, flush: bool = True) -> None:
        return None


__all__ = ["SCHEMA_VERSION", "DigitalTwinTelemetry", "PROMETHEUS_AVAILABLE"]
