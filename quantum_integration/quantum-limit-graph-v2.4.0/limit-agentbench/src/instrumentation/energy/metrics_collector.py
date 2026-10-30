# src/instrumentation/energy/metrics_collector.py

"""
Metrics Collector
=================

Aggregates all energy and runtime meters behind a single lifecycle.

Enhancements
------------
- :class:`MeterStatus` — closed enum replacing free-form status strings;
  existing ``== "ok"`` / ``== "failed"`` comparisons still work (``str``
  mixin).
- :class:`MeterResult` / :class:`CollectorReport` — frozen; the report's
  ``from_dict`` validates the ``results`` shape and raises
  :class:`MetricsCollectorError` on bad input.
- **Safety signals preserved end-to-end** — :meth:`MetricsCollector._normalize`
  carries ``sanitized``, ``correlation_id``, and the meter's own
  ``elapsed_seconds`` through to the aggregated :class:`TelemetrySample`.
  A meter that flags its reading as ``sanitized=True`` (partial sample
  failure, no-op tracker, short window, etc.) stays flagged after
  aggregation.
- **Honest aggregation** — ``aggregate_measured_only=True`` excludes
  samples whose ``provenance`` is not ``MEASURED`` **or** whose
  ``sanitized`` flag is ``True``. Fabricated readings never contribute
  to the aggregate sum.
- **Lifecycle guards** — ``stop_all`` raises if ``start_all`` was never
  called; ``start_all`` only reports ``started=True`` when at least one
  meter actually started. A ``fail_fast`` start failure rolls back the
  *explicit* list of successful meters (not a fragile ``[:-1]`` slice).
- ``strict`` no longer forces fail-fast in ``start_all`` — that was a
  ``strict``-only failure mode that contradicted ``fail_fast``'s
  documented purpose and diverged from ``stop_all``.
- ``last_report`` is initialised in ``__init__``.
- ``close()`` for explicit cleanup; ``measure()`` remains the preferred
  usage.
- ``CollectorConfig`` validates ``bool``/``NaN`` for its numeric fields.
- ``__main__`` smoke test extended with sanitized-signal round-trip,
  lifecycle-guard, and aggregate-strictness cases.

Aggregation contract
--------------------
``total_energy_joules`` / ``total_power_watts`` sum the reported values
across meters whose ``status == OK``. When ``aggregate_measured_only``
is True, only samples with ``provenance == MEASURED`` **and**
``sanitized == False`` contribute. Fabricated or estimated readings are
excluded from the aggregate but remain visible in
``report.results[name].sample``.
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from .base_meter import (
    BaseEnergyMeter,
    BaseMeterConfig,
    InstrumentationError,
    Provenance,
    TelemetrySample,
)

logger = logging.getLogger(__name__)

__all__ = [
    "CollectorConfig",
    "CollectorReport",
    "MeterResult",
    "MeterStatus",
    "MetricsCollector",
    "MetricsCollectorError",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MetricsCollectorError(InstrumentationError):
    """Raised for invalid collector inputs or aggregate failures."""


# --------------------------------------------------------------------------- #
# Status enum
# --------------------------------------------------------------------------- #
class MeterStatus(str, Enum):
    """Closed vocabulary for per-meter outcomes."""

    OK = "ok"
    FAILED = "failed"
    UNSUPPORTED = "unsupported"
    NOT_STARTED = "not_started"
    ALREADY_STARTED = "already_started"

    def __str__(self) -> str:
        return self.value


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
def _reject_bool_and_nan(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise MetricsCollectorError(f"{label} must be a number, not bool")
    fv = float(value)
    if not math.isfinite(fv):
        raise MetricsCollectorError(f"{label} must be finite")
    return fv


@dataclass(frozen=True)
class CollectorConfig:
    """Tunable parameters for the metrics collector."""

    fail_fast: bool = False
    name_collision_policy: str = "suffix"  # "suffix" | "skip" | "replace"
    per_meter_warn_seconds: float = 10.0
    strict_meter_types: bool = True

    # If True, the aggregate report only includes samples with
    # provenance == MEASURED **and** sanitized == False.
    aggregate_measured_only: bool = False

    # If True, stop_all() is a no-op when start_all() was never called.
    # If False (default), stop_all() raises MetricsCollectorError.
    allow_stop_without_start: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.fail_fast, bool):
            raise MetricsCollectorError("fail_fast must be a bool")
        if not isinstance(self.strict_meter_types, bool):
            raise MetricsCollectorError(
                "strict_meter_types must be a bool"
            )
        if not isinstance(self.aggregate_measured_only, bool):
            raise MetricsCollectorError(
                "aggregate_measured_only must be a bool"
            )
        if not isinstance(self.allow_stop_without_start, bool):
            raise MetricsCollectorError(
                "allow_stop_without_start must be a bool"
            )

        if self.name_collision_policy not in (
            "suffix", "skip", "replace",
        ):
            raise MetricsCollectorError(
                "name_collision_policy must be 'suffix', 'skip', or "
                "'replace'"
            )

        warn = _reject_bool_and_nan(
            self.per_meter_warn_seconds, "per_meter_warn_seconds",
        )
        if warn <= 0:
            raise MetricsCollectorError(
                "per_meter_warn_seconds must be > 0"
            )
        object.__setattr__(self, "per_meter_warn_seconds", warn)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Per-meter result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MeterResult:
    """Immutable per-meter record from ``stop_all``."""

    meter_name: str
    meter_type: str
    status: MeterStatus
    duration_ms: float
    sample: Optional[TelemetrySample] = None
    raw_metrics: Optional[Mapping[str, Any]] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, MeterStatus):
            try:
                object.__setattr__(
                    self, "status", MeterStatus(self.status),
                )
            except (ValueError, TypeError) as exc:
                raise MetricsCollectorError(
                    f"unknown status {self.status!r}"
                ) from exc
        if self.duration_ms is None or not isinstance(
            self.duration_ms, (int, float)
        ):
            raise MetricsCollectorError(
                "duration_ms must be numeric"
            )
        if self.raw_metrics is not None:
            object.__setattr__(
                self, "raw_metrics", dict(self.raw_metrics),
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "meter_name": self.meter_name,
            "meter_type": self.meter_type,
            "status": self.status.value,
            "duration_ms": float(self.duration_ms),
            "sample": self.sample.to_dict() if self.sample else None,
            "raw_metrics": (
                dict(self.raw_metrics) if self.raw_metrics else None
            ),
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MeterResult":
        if not isinstance(data, Mapping):
            raise MetricsCollectorError(
                f"MeterResult.from_dict expects a Mapping, "
                f"got {type(data).__name__}"
            )
        sample = None
        if data.get("sample") is not None:
            sample = TelemetrySample.from_dict(data["sample"])
        return cls(
            meter_name=str(data["meter_name"]),
            meter_type=str(data.get("meter_type", "unknown")),
            status=MeterStatus(data.get("status", "failed")),
            duration_ms=float(data.get("duration_ms", 0.0)),
            sample=sample,
            raw_metrics=data.get("raw_metrics"),
            error=data.get("error"),
        )

    def __repr__(self) -> str:
        return (
            "MeterResult("
            f"name={self.meter_name!r}, "
            f"type={self.meter_type!r}, "
            f"status={self.status.value!r}, "
            f"duration_ms={self.duration_ms:.2f})"
        )


# --------------------------------------------------------------------------- #
# Aggregate report
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CollectorReport:
    """Immutable aggregate returned by ``MetricsCollector.stop_all``."""

    results: Mapping[str, MeterResult]
    total_energy_joules: float
    total_power_watts: float
    meter_count: int
    successful: int
    failed: int
    unsupported: int
    wall_time_ms: float
    measured_only: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "results", dict(self.results))
        for label in (
            "total_energy_joules",
            "total_power_watts",
            "wall_time_ms",
        ):
            value = getattr(self, label)
            if not isinstance(value, (int, float)) or isinstance(
                value, bool
            ):
                raise MetricsCollectorError(
                    f"{label} must be numeric"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "results": {
                k: v.to_dict() for k, v in self.results.items()
            },
            "total_energy_joules": self.total_energy_joules,
            "total_power_watts": self.total_power_watts,
            "meter_count": self.meter_count,
            "successful": self.successful,
            "failed": self.failed,
            "unsupported": self.unsupported,
            "wall_time_ms": self.wall_time_ms,
            "measured_only": self.measured_only,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CollectorReport":
        if not isinstance(data, Mapping):
            raise MetricsCollectorError(
                f"CollectorReport.from_dict expects a Mapping, "
                f"got {type(data).__name__}"
            )
        raw_results = data.get("results") or {}
        if not isinstance(raw_results, Mapping):
            raise MetricsCollectorError(
                f"results must be a Mapping, got "
                f"{type(raw_results).__name__}"
            )
        results = {
            str(name): MeterResult.from_dict(entry)
            for name, entry in raw_results.items()
        }
        return cls(
            results=results,
            total_energy_joules=float(
                data.get("total_energy_joules", 0.0)
            ),
            total_power_watts=float(
                data.get("total_power_watts", 0.0)
            ),
            meter_count=int(
                data.get("meter_count", len(results))
            ),
            successful=int(data.get("successful", 0)),
            failed=int(data.get("failed", 0)),
            unsupported=int(data.get("unsupported", 0)),
            wall_time_ms=float(data.get("wall_time_ms", 0.0)),
            measured_only=bool(data.get("measured_only", False)),
        )

    @property
    def metrics(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for name, res in self.results.items():
            if res.status is MeterStatus.OK and res.sample is not None:
                out[name] = res.sample.to_dict()
            elif res.raw_metrics is not None:
                out[name] = dict(res.raw_metrics)
        return out

    def __repr__(self) -> str:
        return (
            "CollectorReport("
            f"meters={self.meter_count}, "
            f"ok={self.successful}, failed={self.failed}, "
            f"energy_joules={self.total_energy_joules:.4g}, "
            f"power_watts={self.total_power_watts:.4g}"
            f"{', measured_only' if self.measured_only else ''})"
        )


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    return default


def _read_elapsed(meter: Any, raw: Any) -> Optional[float]:
    """Return the meter's measured elapsed time, from any available source."""
    if isinstance(raw, Mapping):
        value = raw.get("elapsed_seconds")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            fv = float(value)
            if math.isfinite(fv) and fv >= 0:
                return fv
    stored = getattr(meter, "_last_stop_elapsed", None)
    if isinstance(stored, (int, float)) and not isinstance(stored, bool):
        fv = float(stored)
        if math.isfinite(fv) and fv >= 0:
            return fv
    return None


def _read_sanitized(meter: Any, raw: Any) -> bool:
    """Any of the raw dict, the meter's last sample, or the meter itself."""
    if isinstance(raw, Mapping):
        if _coerce_bool(raw.get("sanitized"), False):
            return True
    last = getattr(meter, "last_sample", None)
    if isinstance(last, TelemetrySample) and last.sanitized:
        return True
    if _coerce_bool(getattr(meter, "_last_sanitized", False), False):
        return True
    return False


def _read_correlation_id(meter: Any, raw: Any) -> Optional[str]:
    if isinstance(raw, Mapping):
        cid = raw.get("correlation_id")
        if isinstance(cid, str) and cid:
            return cid
    last = getattr(meter, "last_sample", None)
    if isinstance(last, TelemetrySample) and last.correlation_id:
        return last.correlation_id
    return None


# --------------------------------------------------------------------------- #
# Collector
# --------------------------------------------------------------------------- #
class MetricsCollector:
    """
    Aggregates all energy and runtime meters.

    Thread-safe, serializable, and idempotent for ``start_all``.
    ``stop_all`` requires a prior ``start_all`` unless
    ``config.allow_stop_without_start=True``.
    """

    def __init__(
        self,
        meters: Sequence[Any],
        *,
        config: Optional[CollectorConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None and not isinstance(config, CollectorConfig):
            raise MetricsCollectorError(
                "config must be a CollectorConfig instance"
            )
        if not isinstance(strict, bool):
            raise MetricsCollectorError("strict must be a bool")
        if meters is None or isinstance(meters, (str, bytes)):
            raise MetricsCollectorError(
                f"meters must be a non-string iterable, got "
                f"{type(meters).__name__}"
            )
        try:
            meter_list = list(meters)
        except TypeError as exc:
            raise MetricsCollectorError(
                f"meters must be iterable, got "
                f"{type(meters).__name__}"
            ) from exc

        self._config = config or CollectorConfig()
        self._strict = strict
        self._inner_lock = threading.RLock()
        self._started: bool = False
        self._start_wall_time: Optional[float] = None
        self.last_report: Optional[CollectorReport] = None

        self.meters: List[Any] = []
        self._names: List[str] = []
        self._unsupported: List[Any] = []

        seen: Dict[str, int] = {}
        for idx, m in enumerate(meter_list):
            if m is None:
                msg = f"meters[{idx}] is None"
                if self._strict:
                    raise MetricsCollectorError(msg)
                logger.warning("%s Skipping.", msg)
                self._unsupported.append(m)
                continue
            if not (
                callable(getattr(m, "start", None))
                and callable(getattr(m, "stop", None))
            ):
                msg = (
                    f"meters[{idx}] ({type(m).__name__}) does not "
                    f"implement start()/stop()"
                )
                if self._strict and self._config.strict_meter_types:
                    raise MetricsCollectorError(msg)
                logger.warning("%s Registering as unsupported.", msg)
                self._unsupported.append(m)
                continue

            base_name = type(m).__name__ or f"Meter{idx}"
            resolved, displaced = self._resolve_name(base_name, seen)
            if resolved is None:
                continue
            if displaced is not None:
                try:
                    i = self._names.index(displaced)
                except ValueError:
                    pass
                else:
                    del self._names[i]
                    del self.meters[i]
            self.meters.append(m)
            self._names.append(resolved)

        logger.debug(
            "MetricsCollector initialized with %d supported meter(s) "
            "and %d unsupported entry(ies) (strict=%s, fail_fast=%s)",
            len(self.meters),
            len(self._unsupported),
            self._strict,
            self._config.fail_fast,
        )

    # ---------------------------------------------------------- naming
    def _resolve_name(
        self, base_name: str, seen: Dict[str, int]
    ) -> Tuple[Optional[str], Optional[str]]:
        """Return ``(name, displaced_name)``.

        ``name`` is ``None`` when the policy drops the meter entirely.
        ``displaced_name`` is the earlier name to remove (for the
        ``"replace"`` policy) or ``None``.
        """
        policy = self._config.name_collision_policy
        if base_name not in seen:
            seen[base_name] = 1
            return base_name, None

        seen[base_name] += 1
        if policy == "suffix":
            return f"{base_name}__{seen[base_name]}", None
        if policy == "skip":
            logger.debug(
                "Skipping duplicate meter name %r.", base_name,
            )
            return None, None
        # replace
        return base_name, base_name

    # ---------------------------------------------------------- props
    @property
    def config(self) -> CollectorConfig:
        return self._config

    @property
    def started(self) -> bool:
        with self._inner_lock:
            return self._started

    @property
    def meter_count(self) -> int:
        return len(self.meters)

    @property
    def names(self) -> List[str]:
        return list(self._names)

    @property
    def unsupported(self) -> List[Any]:
        return list(self._unsupported)

    # ---------------------------------------------------------- lifecycle
    def start_all(self) -> Dict[str, str]:
        """Start every meter. Idempotent within a session.

        In ``fail_fast`` mode, a start failure rolls back the meters
        that successfully started and raises. In non-``fail_fast``
        mode, failures are recorded in the return dict and the
        remaining meters still run.
        """
        with self._inner_lock:
            if self._started:
                logger.debug(
                    "start_all() called while already started."
                )
                return {
                    name: MeterStatus.ALREADY_STARTED.value
                    for name in self._names
                }

            self._start_wall_time = time.monotonic()
            statuses: Dict[str, str] = {}
            successful: List[str] = []

            for name, meter in zip(self._names, self.meters):
                start_ts = time.monotonic()
                try:
                    meter.start()
                except Exception as exc:
                    statuses[name] = MeterStatus.FAILED.value
                    logger.exception(
                        "meter %r start() failed: %s", name, exc,
                    )
                    if self._config.fail_fast:
                        self._rollback_started(successful)
                        self._started = False
                        self._start_wall_time = None
                        raise MetricsCollectorError(
                            f"meter {name!r} start() failed: {exc}"
                        ) from exc
                    continue
                duration_ms = (
                    time.monotonic() - start_ts
                ) * 1000.0
                if (
                    duration_ms
                    > self._config.per_meter_warn_seconds * 1000.0
                ):
                    logger.warning(
                        "meter %r start() took %.2fms "
                        "(> per_meter_warn_seconds=%.2f).",
                        name,
                        duration_ms,
                        self._config.per_meter_warn_seconds,
                    )
                statuses[name] = MeterStatus.OK.value
                successful.append(name)

            # Only claim to be started when at least one meter is running.
            self._started = bool(successful)

        logger.info(
            "start_all: %d ok, %d failed",
            sum(
                1 for s in statuses.values()
                if s == MeterStatus.OK.value
            ),
            sum(
                1 for s in statuses.values()
                if s == MeterStatus.FAILED.value
            ),
        )
        return statuses

    def stop_all(self) -> CollectorReport:
        """Stop every meter and return a :class:`CollectorReport`."""
        with self._inner_lock:
            if not self._started:
                if not self._config.allow_stop_without_start:
                    raise MetricsCollectorError(
                        "stop_all() called without start_all()"
                    )
                logger.warning(
                    "stop_all() called without start_all(); "
                    "report will mark every meter not_started."
                )
                results: "OrderedDict[str, MeterResult]" = OrderedDict()
                for name, meter in zip(self._names, self.meters):
                    results[name] = MeterResult(
                        meter_name=name,
                        meter_type=type(meter).__name__,
                        status=MeterStatus.NOT_STARTED,
                        duration_ms=0.0,
                        error="meter was never started",
                    )
                for idx, m in enumerate(self._unsupported):
                    name = (
                        f"unsupported__"
                        f"{type(m).__name__ if m is not None else 'None'}"
                        f"__{idx}"
                    )
                    results[name] = MeterResult(
                        meter_name=name,
                        meter_type=(
                            type(m).__name__
                            if m is not None else "NoneType"
                        ),
                        status=MeterStatus.UNSUPPORTED,
                        duration_ms=0.0,
                        error=(
                            "meter does not implement start()/stop()"
                        ),
                    )
                return CollectorReport(
                    results=dict(results),
                    total_energy_joules=0.0,
                    total_power_watts=0.0,
                    meter_count=len(results),
                    successful=0,
                    failed=0,
                    unsupported=len(self._unsupported),
                    wall_time_ms=0.0,
                    measured_only=self._config.aggregate_measured_only,
                )

            wall_start = time.monotonic()
            results: "OrderedDict[str, MeterResult]" = OrderedDict()

            for name, meter in zip(self._names, self.meters):
                stop_ts = time.monotonic()
                try:
                    raw = meter.stop()
                except Exception as exc:
                    duration_ms = (
                        time.monotonic() - stop_ts
                    ) * 1000.0
                    logger.exception(
                        "meter %r stop() failed: %s", name, exc,
                    )
                    results[name] = MeterResult(
                        meter_name=name,
                        meter_type=type(meter).__name__,
                        status=MeterStatus.FAILED,
                        duration_ms=duration_ms,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                    if self._config.fail_fast:
                        self._started = False
                        self._start_wall_time = None
                        raise MetricsCollectorError(
                            f"meter {name!r} stop() failed: {exc}"
                        ) from exc
                    continue

                duration_ms = (
                    time.monotonic() - stop_ts
                ) * 1000.0
                sample = self._normalize(name, meter, raw)
                if sample is None:
                    results[name] = MeterResult(
                        meter_name=name,
                        meter_type=type(meter).__name__,
                        status=MeterStatus.FAILED,
                        duration_ms=duration_ms,
                        error=(
                            "meter did not return a valid metrics "
                            "mapping."
                        ),
                    )
                    continue

                results[name] = MeterResult(
                    meter_name=name,
                    meter_type=type(meter).__name__,
                    status=MeterStatus.OK,
                    duration_ms=duration_ms,
                    sample=sample,
                    raw_metrics=self._safe_raw(raw),
                )
                if (
                    duration_ms
                    > self._config.per_meter_warn_seconds * 1000.0
                ):
                    logger.warning(
                        "meter %r stop() took %.2fms "
                        "(> per_meter_warn_seconds=%.2f).",
                        name,
                        duration_ms,
                        self._config.per_meter_warn_seconds,
                    )

            for idx, m in enumerate(self._unsupported):
                name = (
                    f"unsupported__"
                    f"{type(m).__name__ if m is not None else 'None'}"
                    f"__{idx}"
                )
                results[name] = MeterResult(
                    meter_name=name,
                    meter_type=(
                        type(m).__name__
                        if m is not None else "NoneType"
                    ),
                    status=MeterStatus.UNSUPPORTED,
                    duration_ms=0.0,
                    error="meter does not implement start()/stop()",
                )

            self._started = False
            self._start_wall_time = None

        # Aggregate (outside the lock, on a stable snapshot).
        total_energy = 0.0
        total_power = 0.0
        for r in results.values():
            if r.status is not MeterStatus.OK or r.sample is None:
                continue
            if self._config.aggregate_measured_only:
                if r.sample.provenance is not Provenance.MEASURED:
                    continue
                if r.sample.sanitized:
                    continue
            total_energy += float(r.sample.energy_joules or 0.0)
            total_power += float(r.sample.power_watts or 0.0)

        successful = sum(
            1 for r in results.values()
            if r.status is MeterStatus.OK
        )
        failed = sum(
            1 for r in results.values()
            if r.status is MeterStatus.FAILED
        )
        unsupported = sum(
            1 for r in results.values()
            if r.status is MeterStatus.UNSUPPORTED
        )

        report = CollectorReport(
            results=dict(results),
            total_energy_joules=total_energy,
            total_power_watts=total_power,
            meter_count=len(results),
            successful=successful,
            failed=failed,
            unsupported=unsupported,
            wall_time_ms=(time.monotonic() - wall_start) * 1000.0,
            measured_only=self._config.aggregate_measured_only,
        )

        logger.info(
            "stop_all: %d ok, %d failed, %d unsupported "
            "(energy=%.6g J, power=%.4g W, wall=%.2fms, "
            "measured_only=%s)",
            successful,
            failed,
            unsupported,
            total_energy,
            total_power,
            report.wall_time_ms,
            self._config.aggregate_measured_only,
        )
        return report

    def close(self) -> Optional[CollectorReport]:
        """Explicit cleanup: call ``stop_all`` if the collector is started."""
        with self._inner_lock:
            if not self._started:
                return None
        try:
            return self.stop_all()
        except MetricsCollectorError:
            logger.exception(
                "close() could not stop all meters cleanly"
            )
            return None

    # ---------------------------------------------------------- internals
    def _rollback_started(self, names: Sequence[str]) -> None:
        """Best-effort stop of the explicit list of started meters."""
        for name in names:
            try:
                idx = self._names.index(name)
            except ValueError:
                continue
            meter = self.meters[idx]
            try:
                meter.stop()
            except Exception as exc:  # pragma: no cover — defensive
                logger.warning(
                    "rollback stop() for %r failed: %s", name, exc,
                )

    def _normalize(
        self, name: str, meter: Any, raw: Any
    ) -> Optional[TelemetrySample]:
        """Convert a raw meter result into a :class:`TelemetrySample`."""
        if raw is None:
            msg = f"meter {name!r} returned None"
            if self._strict:
                raise MetricsCollectorError(msg)
            logger.warning("%s", msg)
            return None

        if isinstance(raw, TelemetrySample):
            return raw

        if isinstance(meter, BaseEnergyMeter):
            try:
                validated = meter._validate_metrics(raw)  # noqa: SLF001
            except Exception as exc:  # noqa: BLE001 — isolate subclasses
                msg = (
                    f"meter {name!r} contract violation: {exc}"
                )
                if self._strict:
                    raise MetricsCollectorError(msg) from exc
                logger.warning("%s", msg)
                return None
            return TelemetrySample.from_metrics(
                validated,
                source=name,
                elapsed_seconds=_read_elapsed(meter, raw),
                sanitized=_read_sanitized(meter, raw),
                correlation_id=_read_correlation_id(meter, raw),
            )

        if isinstance(raw, Mapping):
            try:
                return TelemetrySample.from_metrics(
                    raw,
                    source=name,
                    elapsed_seconds=_read_elapsed(meter, raw),
                    sanitized=_read_sanitized(meter, raw),
                    correlation_id=_read_correlation_id(meter, raw),
                )
            except (
                InstrumentationError,
                KeyError,
                TypeError,
                ValueError,
            ) as exc:
                msg = (
                    f"meter {name!r} returned a mapping that does "
                    f"not match the TelemetrySample contract: {exc}"
                )
                if self._strict:
                    raise MetricsCollectorError(msg) from exc
                logger.warning("%s", msg)
                return None

        msg = (
            f"meter {name!r} returned unsupported type "
            f"{type(raw).__name__}"
        )
        if self._strict:
            raise MetricsCollectorError(msg)
        logger.warning("%s", msg)
        return None

    @staticmethod
    def _safe_raw(raw: Any) -> Optional[Mapping[str, Any]]:
        if isinstance(raw, Mapping):
            return dict(raw)
        if isinstance(raw, TelemetrySample):
            return raw.to_dict()
        return None

    # ---------------------------------------------------------- context manager
    @contextmanager
    def measure(self) -> Iterator["MetricsCollector"]:
        """Context manager that wires ``start_all`` / ``stop_all``."""
        self.last_report = None
        self.start_all()
        try:
            yield self
        finally:
            try:
                self.last_report = self.stop_all()
            except MetricsCollectorError:
                logger.exception(
                    "stop_all failed during context exit."
                )
                if self._strict:
                    raise

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._inner_lock:
            return {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "started": self._started,
                "meter_count": len(self.meters),
                "meters": [
                    {"name": name, "type": type(m).__name__}
                    for name, m in zip(self._names, self.meters)
                ],
                "unsupported_count": len(self._unsupported),
            }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    def __repr__(self) -> str:
        with self._inner_lock:
            return (
                "MetricsCollector("
                f"meters={len(self.meters)}, "
                f"names={self._names}, "
                f"unsupported={len(self._unsupported)}, "
                f"started={self._started}, "
                f"strict={self._strict}, "
                f"fail_fast={self._config.fail_fast})"
            )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    class _FakeMeter(BaseEnergyMeter):
        def __init__(
            self,
            *,
            energy: float = 10.0,
            power: float = 5.0,
            provenance: str = "measured",
            fail_on: str = "",
            **kwargs: Any,
        ) -> None:
            super().__init__(**kwargs)
            self._e = energy
            self._p = power
            self._prov = provenance
            self._fail_on = fail_on

        def start(self) -> None:
            if self._fail_on == "start":
                raise RuntimeError("synthetic start failure")
            self._mark_started()

        def stop(self) -> Dict[str, Any]:
            if self._fail_on == "stop":
                raise RuntimeError("synthetic stop failure")
            self._mark_stopped()
            return self._validate_metrics({
                "energy_joules": self._e,
                "power_watts": self._p,
                "provenance": self._prov,
                "elapsed_seconds": 0.5,
            })

    class _NonMeter:
        pass

    # Happy path
    c = MetricsCollector([
        _FakeMeter(energy=10.0, power=5.0),
        _FakeMeter(energy=20.0, power=8.0),
    ])
    print("repr       :", c)
    print("start      :", c.start_all())
    report = c.stop_all()
    print("report     :", report)
    print("metrics    :", report.metrics)
    assert report.total_energy_joules == 30.0
    # Safety signals preserved
    s = report.results["_FakeMeter"].sample
    assert s is not None
    assert s.elapsed_seconds == 0.5
    print("elapsed    :", s.elapsed_seconds)

    # Duplicate names
    d = MetricsCollector([
        _FakeMeter(energy=1.0),
        _FakeMeter(energy=2.0),
        _FakeMeter(energy=3.0),
    ])
    d.start_all()
    dr = d.stop_all()
    assert len(dr.results) == 3
    print("dupes      :", list(dr.results.keys()))

    # Per-meter isolation
    m = MetricsCollector([
        _FakeMeter(energy=5.0),
        _FakeMeter(energy=0.0, fail_on="stop"),
        _FakeMeter(energy=7.0),
    ])
    m.start_all()
    mr = m.stop_all()
    assert mr.failed == 1 and mr.successful == 2
    print("mixed      :", {
        n: r.status.value for n, r in mr.results.items()
    })

    # stop_all without start_all → raises
    try:
        MetricsCollector([_FakeMeter()]).stop_all()
    except MetricsCollectorError as exc:
        print("guard      :", exc)

    # allow_stop_without_start → NOT_STARTED statuses
    lenient = MetricsCollector(
        [_FakeMeter()],
        config=CollectorConfig(allow_stop_without_start=True),
    )
    lr = lenient.stop_all()
    assert lr.results["_FakeMeter"].status is MeterStatus.NOT_STARTED
    print("lenient    :", lr)

    # Unsupported meter
    u = MetricsCollector(
        [_FakeMeter(energy=1.0), _NonMeter()],
        config=CollectorConfig(strict_meter_types=False),
    )
    u.start_all()
    ur = u.stop_all()
    print("unsup      :", {
        n: r.status.value for n, r in ur.results.items()
    })

    # measured_only excludes estimated *and* sanitized
    class _SanitizedMeter(_FakeMeter):
        def stop(self) -> Dict[str, Any]:
            self._mark_started()  # make elapsed meaningful
            self._mark_stopped()
            self._last_sanitized = True  # force sanitized
            return self._validate_metrics({
                "energy_joules": 100.0,
                "power_watts": 10.0,
                "provenance": "measured",
            })

    strict_agg = MetricsCollector(
        [
            _FakeMeter(energy=10.0, provenance="measured"),
            _FakeMeter(energy=100.0, provenance="estimated"),
            _SanitizedMeter(energy=1000.0, provenance="measured"),
        ],
        config=CollectorConfig(aggregate_measured_only=True),
    )
    strict_agg.start_all()
    sr = strict_agg.stop_all()
    # Only the first meter contributes (10 J)
    assert sr.total_energy_joules == 10.0, sr.total_energy_joules
    print("measured    :", sr.total_energy_joules, "J")

    # Context manager
    with MetricsCollector([
        _FakeMeter(energy=1.0),
        _FakeMeter(energy=2.0),
    ]) as c2:
        time.sleep(0.01)
    print("context     :", c2.last_report)

    # Round-trip
    payload = report.to_dict()
    restored = CollectorReport.from_dict(json.loads(json.dumps(payload)))
    assert restored.to_dict() == report.to_dict()
    print("Round-trip OK.")

    # Validation
    for bad in (None, "not-a-list", 123):
        try:
            MetricsCollector(bad)  # type: ignore[arg-type]
        except MetricsCollectorError as exc:
            print("Rejected   :", exc)

    for bad_cfg in (
        dict(name_collision_policy="bogus"),
        dict(per_meter_warn_seconds=0),
        dict(per_meter_warn_seconds=True),
        dict(per_meter_warn_seconds=float("nan")),
    ):
        try:
            CollectorConfig(**bad_cfg)  # type: ignore[arg-type]
        except MetricsCollectorError as exc:
            print("Rejected cfg:", exc)

    # CollectorReport.from_dict rejects bad results type
    try:
        CollectorReport.from_dict({"results": "nope"})
    except MetricsCollectorError as exc:
        print("Rejected rep:", exc)

    print("\nSmoke test passed.")
