# src/quantum_integration/digital_twin/digital_twin_telemetry.py

"""Thread-safe telemetry with optional Prometheus backend.

Thread safety
-------------
Every public method acquires a ``threading.RLock``. Multiple threads
can safely call ``increment``, ``gauge``, ``histogram``, etc. Prometheus
metrics are forwarded under the same lock so the in-memory and exported
views stay consistent. The previous implementation advertised
thread-safety but held an ``asyncio``-only ``_LazyLock`` that was never
acquired from the synchronous methods; the lock primitive is now the
correct one.

Prometheus labels
-----------------
Callers pass ``tags`` as a ``Mapping[str, str]``. Each metric is
registered once with a single ``tags`` label, and each distinct tag
combination becomes a separate time series via ``metric.labels(...)``.
This preserves the in-memory key semantics without requiring a fixed
label schema.

Tag keys and values must be strings. They must not contain ``,``,
``{``, or ``}`` because those characters are reserved by the in-memory
key format.

Server lifecycle
----------------
The Prometheus HTTP server is started once per port. Ports are tracked
in a module-level set (:data:`_bound_ports`) so two instances cannot
bind the same port. :meth:`DigitalTwinTelemetry.close` shuts down the
server (when ``prometheus_client`` returns a stoppable handle) and
removes the port from the set, allowing reuse.

Value validation
----------------
``increment`` requires a finite, non-negative number. ``gauge`` and
``histogram`` require finite numbers (gauges may be negative).
Non-finite or non-numeric values raise :class:`DigitalTwinInputError`
before any state is mutated.
"""

from __future__ import annotations

import logging
import math
import re
import threading
from collections import deque
from collections.abc import Mapping as ABCMapping
from typing import Any, Deque, Dict, List, Mapping, Optional, Set, Tuple

from .digital_twin_errors import DigitalTwinInputError
from .digital_twin_helpers import _percentile

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 2

#: One lock shared across every instance so only one Prometheus server
#: can bind to a port and metric names can't collide.
_prom_lock = threading.Lock()
_bound_ports: Set[int] = set()

#: Pre-registered Prometheus metrics. Each is created with a single
#: ``tags`` label so arbitrary tag combinations become distinct series.
_PROM_METRIC_SPECS: Tuple[Tuple[str, str, str], ...] = (
    ("dt_scenarios_run", "counter", "Scenarios run"),
    ("dt_sustainability_score", "gauge", "Sustainability score"),
    ("dt_weighted_score", "gauge", "Weighted score"),
    ("dt_cache_hits", "counter", "Cache hits"),
    ("dt_cache_misses", "counter", "Cache misses"),
    ("dt_circuit_breaker_state", "gauge", "Breaker state"),
    ("dt_latency", "histogram", "Latency observations"),
)

#: Characters that would make the in-memory key format ambiguous.
_FORBIDDEN_TAG_CHARS = frozenset(",{}")


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


# --------------------------------------------------------------------------- #
# Validation / formatting helpers
# --------------------------------------------------------------------------- #

def _is_finite_number(value: Any) -> bool:
    """Return ``True`` iff ``value`` is a finite ``int``/``float``.

    ``bool`` is rejected because it is a subclass of ``int``.
    """
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _validate_metric_name(name: Any) -> str:
    if not isinstance(name, str) or not name:
        raise DigitalTwinInputError(
            "metric name must be a non-empty string."
        )
    return name


def _validate_tag_part(role: str, value: Any) -> str:
    if not isinstance(value, str):
        raise DigitalTwinInputError(
            f"tag {role} must be a string, got {type(value).__name__}."
        )
    for ch in value:
        if ch in _FORBIDDEN_TAG_CHARS:
            raise DigitalTwinInputError(
                f"tag {role} {value!r} contains forbidden "
                f"character {ch!r}."
            )
    return value


def _format_tags(tags: Optional[Mapping[str, str]]) -> str:
    """Return a deterministic ``k1=v1,k2=v2`` string, or ``""``.

    Raises :class:`DigitalTwinInputError` on non-mapping inputs,
    non-string keys/values, or forbidden characters.
    """
    if tags is None:
        return ""
    if not isinstance(tags, ABCMapping):
        raise DigitalTwinInputError(
            "tags must be a Mapping or None."
        )
    pairs: List[Tuple[str, str]] = []
    for k, v in tags.items():
        kk = _validate_tag_part("key", k)
        vv = _validate_tag_part("value", v)
        pairs.append((kk, vv))
    pairs.sort()
    return ",".join(f"{k}={v}" for k, v in pairs)


def _make_key(metric: str, tags: Optional[Mapping[str, str]]) -> str:
    tag_str = _format_tags(tags)
    return metric if not tag_str else f"{metric}{{{tag_str}}}"


def _parse_key(key: str) -> Tuple[str, List[Tuple[str, str]]]:
    """Reverse of :func:`_make_key`.

    Returns ``(metric, [(k, v), ...])``. Safe because
    :func:`_format_tags` rejects ``{``, ``}``, and ``,`` in values.
    """
    if "{" not in key:
        return key, []
    metric, rest = key.split("{", 1)
    rest = rest.rstrip("}")
    tags: List[Tuple[str, str]] = []
    if rest:
        for pair in rest.split(","):
            k, v = pair.split("=", 1)
            tags.append((k, v))
    return metric, tags


def _escape_label_value(value: str) -> str:
    """Escape a label value for the Prometheus text exposition format."""
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


def _render_prom_line(
    name: str, tags: List[Tuple[str, str]], value: Any,
) -> str:
    """Render a single sample line in Prometheus text format."""
    if not tags:
        return f"{name} {value}"
    inner = ",".join(
        f'{k}="{_escape_label_value(v)}"' for k, v in tags
    )
    return f"{name}{{{inner}}} {value}"


def _hist_stats(values: List[float]) -> Dict[str, float]:
    """Compute count/mean/p50/p95/max for a list of floats."""
    if not values:
        return {
            "count": 0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "max": 0.0,
        }
    return {
        "count": len(values),
        "mean": sum(values) / len(values),
        "p50": _percentile(values, 50),
        "p95": _percentile(values, 95),
        "max": max(values),
    }


# --------------------------------------------------------------------------- #
# Telemetry
# --------------------------------------------------------------------------- #

class DigitalTwinTelemetry:
    """Per-instance telemetry with optional Prometheus backend.

    Uses a dedicated ``CollectorRegistry`` so multiple instances can
    coexist. The HTTP server is guarded by a module-level lock and a
    global set of bound ports, which :meth:`close` releases.

    Parameters
    ----------
    prometheus_port:
        Optional TCP port for the Prometheus HTTP server. ``None``
        disables it. Must be in ``[0, 65535]``.
    ring_size:
        Maximum number of observations kept per histogram key. Must be
        a positive integer.
    """

    def __init__(
        self,
        *,
        prometheus_port: Optional[int] = None,
        ring_size: int = 200,
    ) -> None:
        # ---- Validate inputs ------------------------------------------ #
        if prometheus_port is not None:
            if isinstance(prometheus_port, bool) or not isinstance(
                prometheus_port, int,
            ):
                raise DigitalTwinInputError(
                    "prometheus_port must be an int or None."
                )
            if not (0 <= prometheus_port <= 65535):
                raise DigitalTwinInputError(
                    "prometheus_port must be in [0, 65535]."
                )
        if isinstance(ring_size, bool) or not isinstance(ring_size, int):
            raise DigitalTwinInputError("ring_size must be an integer.")
        if ring_size < 1:
            raise DigitalTwinInputError("ring_size must be >= 1.")

        # ---- State ----------------------------------------------------- #
        self._lock: threading.RLock = threading.RLock()
        self._ring_size = int(ring_size)
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Deque[float]] = {}
        self._registry: Any = None
        self._prom_metrics: Dict[str, Any] = {}
        self._prometheus_port: Optional[int] = prometheus_port
        self._server: Any = None
        self._prometheus_enabled = False
        self._last_setup_error: Optional[str] = None

        if PROMETHEUS_AVAILABLE and prometheus_port is not None:
            self._setup_prometheus()

    # ------------------------------------------------------------------ #
    # Prometheus setup
    # ------------------------------------------------------------------ #

    def _setup_prometheus(self) -> None:
        with _prom_lock:
            port = self._prometheus_port
            if port in _bound_ports:
                self._last_setup_error = (
                    f"port {port} already bound"
                )
                logger.warning(
                    "Prometheus port %s already bound; disabling server.",
                    port,
                )
                self._prometheus_port = None
                return

            try:
                registry = CollectorRegistry()
                metrics: Dict[str, Any] = {}
                for name, kind, help_text in _PROM_METRIC_SPECS:
                    if kind == "counter":
                        metrics[name] = Counter(
                            name, help_text, ["tags"], registry=registry,
                        )
                    elif kind == "gauge":
                        metrics[name] = Gauge(
                            name, help_text, ["tags"], registry=registry,
                        )
                    elif kind == "histogram":
                        metrics[name] = Histogram(
                            name, help_text, ["tags"], registry=registry,
                        )
                server = start_http_server(port, registry=registry)
                self._registry = registry
                self._prom_metrics = metrics
                self._server = server
                self._prometheus_enabled = True
                _bound_ports.add(port)
                logger.info("Prometheus metrics server on port %s.", port)
            except Exception as exc:  # pragma: no cover - env dependent
                self._last_setup_error = str(exc)
                logger.warning("Failed to start Prometheus: %s", exc)
                self._registry = None
                self._prom_metrics = {}
                self._prometheus_port = None
                self._server = None
                self._prometheus_enabled = False

    def _forward_prom(
        self,
        metric: str,
        kind: str,
        tags: Optional[Mapping[str, str]],
        value: float,
        op: str,
    ) -> None:
        """Forward a value to the matching Prometheus metric, if any."""
        pm = self._prom_metrics.get(metric)
        if pm is None:
            return
        if kind == "counter" and (Counter is None or not isinstance(pm, Counter)):
            return
        if kind == "gauge" and (Gauge is None or not isinstance(pm, Gauge)):
            return
        if kind == "histogram" and (
            Histogram is None or not isinstance(pm, Histogram)
        ):
            return
        try:
            child = pm.labels(tags=_format_tags(tags))
            if op == "inc":
                child.inc(value)
            elif op == "set":
                child.set(value)
            elif op == "observe":
                child.observe(value)
        except Exception as exc:  # noqa: BLE001 - defensive
            logger.warning(
                "Prometheus %s failed for metric %r: %s", op, metric, exc,
            )

    # ------------------------------------------------------------------ #
    # Emit
    # ------------------------------------------------------------------ #

    def increment(
        self,
        metric: str,
        tags: Optional[Mapping[str, str]] = None,
        value: float = 1.0,
    ) -> None:
        """Increment a counter by ``value``.

        ``value`` must be a finite, non-negative number.
        """
        _validate_metric_name(metric)
        if not _is_finite_number(value):
            raise DigitalTwinInputError(
                f"counter value must be a finite number, got {value!r}."
            )
        if value < 0:
            raise DigitalTwinInputError(
                f"counter value must be >= 0, got {value!r}."
            )
        v = float(value)
        key = _make_key(metric, tags)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + v
            self._forward_prom(metric, "counter", tags, v, "inc")

    def gauge(
        self,
        metric: str,
        value: float,
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        """Set a gauge to ``value``.

        ``value`` must be a finite number (may be negative).
        """
        _validate_metric_name(metric)
        if not _is_finite_number(value):
            raise DigitalTwinInputError(
                f"gauge value must be a finite number, got {value!r}."
            )
        v = float(value)
        key = _make_key(metric, tags)
        with self._lock:
            self._gauges[key] = v
            self._forward_prom(metric, "gauge", tags, v, "set")

    def histogram(
        self,
        metric: str,
        value: float,
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        """Record one observation in a bounded ring buffer.

        ``value`` must be a finite number.
        """
        _validate_metric_name(metric)
        if not _is_finite_number(value):
            raise DigitalTwinInputError(
                f"histogram value must be a finite number, got {value!r}."
            )
        v = float(value)
        key = _make_key(metric, tags)
        with self._lock:
            bucket = self._histograms.get(key)
            if bucket is None:
                bucket = deque(maxlen=self._ring_size)
                self._histograms[key] = bucket
            bucket.append(v)
            self._forward_prom(metric, "histogram", tags, v, "observe")

    # ------------------------------------------------------------------ #
    # Read
    # ------------------------------------------------------------------ #

    def histogram_stats(
        self,
        metric: str,
        tags: Optional[Mapping[str, str]] = None,
    ) -> Dict[str, float]:
        """Return ``{count, mean, p50, p95, max}`` for a histogram key."""
        _validate_metric_name(metric)
        key = _make_key(metric, tags)
        with self._lock:
            values = list(self._histograms.get(key, ()))
        return _hist_stats(values)

    def remove(
        self,
        metric: str,
        tags: Optional[Mapping[str, str]] = None,
    ) -> bool:
        """Remove all in-memory state for ``(metric, tags)``.

        Returns ``True`` if anything was removed. Prometheus counters
        and gauges registered at construction time are not affected.
        """
        _validate_metric_name(metric)
        key = _make_key(metric, tags)
        removed = False
        with self._lock:
            if key in self._counters:
                del self._counters[key]
                removed = True
            if key in self._gauges:
                del self._gauges[key]
                removed = True
            if key in self._histograms:
                del self._histograms[key]
                removed = True
        return removed

    def export(self) -> str:
        """Return a Prometheus text-format snapshot.

        When the Prometheus registry is available, uses
        ``generate_latest``. Otherwise falls back to a minimal text
        exposition that includes ``# TYPE`` lines, one sample per key,
        and valid histogram ``_bucket``/``_count``/``_sum`` lines.
        """
        if self._registry is not None and generate_latest is not None:
            try:
                return generate_latest(self._registry).decode("utf-8")
            except Exception as exc:  # noqa: BLE001 - defensive
                logger.warning("generate_latest failed: %s", exc)

        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            histograms = {k: list(v) for k, v in self._histograms.items()}

        lines: List[str] = []

        # Counters, grouped by metric name so # TYPE is emitted once.
        counters_by_metric: Dict[str, List[Tuple[List[Tuple[str, str]], float]]] = {}
        for key in sorted(counters):
            metric, tags = _parse_key(key)
            counters_by_metric.setdefault(metric, []).append(
                (tags, counters[key]),
            )
        for metric in sorted(counters_by_metric):
            lines.append(f"# TYPE {metric} counter")
            for tags, value in counters_by_metric[metric]:
                lines.append(_render_prom_line(metric, tags, value))

        # Gauges.
        gauges_by_metric: Dict[str, List[Tuple[List[Tuple[str, str]], float]]] = {}
        for key in sorted(gauges):
            metric, tags = _parse_key(key)
            gauges_by_metric.setdefault(metric, []).append(
                (tags, gauges[key]),
            )
        for metric in sorted(gauges_by_metric):
            lines.append(f"# TYPE {metric} gauge")
            for tags, value in gauges_by_metric[metric]:
                lines.append(_render_prom_line(metric, tags, value))

        # Histograms.
        hists_by_metric: Dict[str, List[Tuple[List[Tuple[str, str]], List[float]]]] = {}
        for key in sorted(histograms):
            metric, tags = _parse_key(key)
            hists_by_metric.setdefault(metric, []).append(
                (tags, histograms[key]),
            )
        for metric in sorted(hists_by_metric):
            lines.append(f"# TYPE {metric} histogram")
            for tags, values in hists_by_metric[metric]:
                n = len(values)
                total = sum(values)
                # Minimal valid histogram: a single +Inf bucket plus
                # the count and sum lines.
                bucket_tags = tags + [("le", "+Inf")]
                lines.append(
                    _render_prom_line(f"{metric}_bucket", bucket_tags, n),
                )
                lines.append(
                    _render_prom_line(f"{metric}_count", tags, n),
                )
                lines.append(
                    _render_prom_line(f"{metric}_sum", tags, total),
                )

        return "\n".join(lines)

    # ------------------------------------------------------------------ #
    # Introspection / lifecycle
    # ------------------------------------------------------------------ #

    def statistics(self) -> Dict[str, Any]:
        """Return a snapshot of telemetry state and configuration."""
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            histogram_keys = sorted(self._histograms.keys())
            histogram_stats = {
                k: _hist_stats(list(self._histograms[k]))
                for k in histogram_keys
            }

        return {
            "schema_version": SCHEMA_VERSION,
            "prometheus_available": PROMETHEUS_AVAILABLE,
            "prometheus_enabled": self._prometheus_enabled,
            "prometheus_port": self._prometheus_port,
            "last_setup_error": self._last_setup_error,
            "ring_size": self._ring_size,
            "counters": counters,
            "gauges": gauges,
            "histogram_keys": histogram_keys,
            "histogram_stats": histogram_stats,
            "counter_count": len(counters),
            "gauge_count": len(gauges),
            "histogram_count": len(histogram_keys),
        }

    def reset(self) -> None:
        """Clear the in-memory counters, gauges, and histograms.

        Prometheus counters and gauges registered at construction time
        are not reset; ``prometheus_client`` does not expose a public
        API for that. Use :meth:`close` and construct a new instance if
        a full reset is required.
        """
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()

    def close(self, *, flush: bool = True) -> None:
        """Shut down the Prometheus HTTP server and release its port.

        ``flush`` is accepted for API symmetry; there is currently no
        buffered state to flush. In-memory counters and histograms are
        preserved so ``statistics()`` and ``export()`` still work after
        ``close()`` (export falls back to the text format).
        """
        server = self._server
        port = self._prometheus_port

        with _prom_lock:
            if port is not None:
                _bound_ports.discard(port)
            self._prometheus_port = None
            self._server = None
            self._prometheus_enabled = False
            self._prom_metrics = {}
            self._registry = None

        if server is not None:
            shutdown = getattr(server, "shutdown", None)
            if callable(shutdown):
                try:
                    shutdown()
                except Exception as exc:  # noqa: BLE001 - defensive
                    logger.warning(
                        "Failed to shut down Prometheus server: %s", exc,
                    )

    def __repr__(self) -> str:
        with self._lock:
            n_c = len(self._counters)
            n_g = len(self._gauges)
            n_h = len(self._histograms)
        return (
            f"{type(self).__name__}("
            f"counters={n_c}, gauges={n_g}, histograms={n_h}, "
            f"ring_size={self._ring_size}, "
            f"prometheus_enabled={self._prometheus_enabled}, "
            f"prometheus_port={self._prometheus_port})"
        )


__all__ = ["SCHEMA_VERSION", "DigitalTwinTelemetry", "PROMETHEUS_AVAILABLE"]
