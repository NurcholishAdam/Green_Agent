# src/instrumentation/energy/metrics_collector.py

"""
Metrics Collector
=================

Aggregates all energy and runtime meters behind a single lifecycle.

Enhancements
------------
- ``CollectorConfig`` — frozen, validated, centralizes failure policy,
  per-meter timeout, and name-collision policy.
- ``RLock``-guarded lifecycle; ``start_all`` / ``stop_all`` are idempotent.
- **Per-meter exception isolation** — a failing meter is reported in the
  result under ``status="failed"`` instead of aborting the collection.
- **Duplicate-name de-duplication** — colliding class names are suffixed
  with an index so no results are lost.
- Full validation of the ``meters`` input.
- Unified ``TelemetrySample`` normalization on the way out (via
  ``BaseEnergyMeter._validate_metrics`` for meter-shaped objects).
- Structured ``CollectorReport`` returned by ``stop_all`` — per-meter status,
  duration, sample, and error, plus an aggregate roll-up.
- Context manager (``with collector.measure(): ...``) that wires
  ``start_all`` / ``stop_all`` around a block.
- Serialization on the config and the report.
- Custom ``MetricsCollectorError``; lazy ``%s`` logging; ``__repr__``;
  ``__main__`` smoke test with mixed meter types.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from .base_meter import (
    BaseEnergyMeter,
    BaseMeterConfig,
    InstrumentationError,
    TelemetrySample,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MetricsCollectorError(InstrumentationError):
    """Raised for invalid collector inputs or aggregate failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CollectorConfig:
    """Tunable parameters for the metrics collector."""

    # If True, a failing meter raises immediately. If False, the failure is
    # recorded in the report and the remaining meters still run.
    fail_fast: bool = False

    # Name-collision policy when two meters share a class name:
    #   "suffix"   → append an index ("DCGMMeter", "DCGMMeter__2", ...)
    #   "skip"     → keep the first, drop the rest
    #   "replace"  → keep the last, drop the earlier one
    name_collision_policy: str = "suffix"

    # Per-meter wall-clock budget for start / stop (informational only — the
    # underlying meter is responsible for its own timeouts). The collector
    # records durations and flags meters that exceed this value.
    per_meter_warn_seconds: float = 10.0

    # If True, unknown objects (missing start/stop) raise at construction.
    # If False, they are recorded as "unsupported" entries.
    strict_meter_types: bool = True

    # If True, the aggregate report only includes ``energy_joules`` and
    # ``power_watts`` summed across meters whose ``provenance`` is
    # ``"measured"``. If False, all meters contribute.
    aggregate_measured_only: bool = False

    def __post_init__(self) -> None:
        if self.name_collision_policy not in ("suffix", "skip", "replace"):
            raise MetricsCollectorError(
                "name_collision_policy must be 'suffix', 'skip', or 'replace'."
            )
        if self.per_meter_warn_seconds <= 0:
            raise MetricsCollectorError("per_meter_warn_seconds must be > 0.")


# --------------------------------------------------------------------------- #
# Per-meter result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MeterResult:
    """Immutable per-meter record from ``stop_all``."""

    meter_name: str
    meter_type: str
    status: str          # "ok" | "failed" | "unsupported"
    duration_ms: float
    sample: Optional[TelemetrySample] = None
    raw_metrics: Optional[Mapping[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "meter_name": self.meter_name,
            "meter_type": self.meter_type,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "sample": self.sample.to_dict() if self.sample else None,
            "raw_metrics": dict(self.raw_metrics) if self.raw_metrics else None,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MeterResult":
        if not isinstance(data, Mapping):
            raise MetricsCollectorError(
                f"MeterResult.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        sample = None
        if data.get("sample") is not None:
            sample = TelemetrySample.from_dict(data["sample"])
        return cls(
            meter_name=str(data["meter_name"]),
            meter_type=str(data.get("meter_type", "unknown")),
            status=str(data.get("status", "failed")),
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
            f"status={self.status!r}, "
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

    def to_dict(self) -> Dict[str, Any]:
        return {
            "results": {k: v.to_dict() for k, v in self.results.items()},
            "total_energy_joules": self.total_energy_joules,
            "total_power_watts": self.total_power_watts,
            "meter_count": self.meter_count,
            "successful": self.successful,
            "failed": self.failed,
            "unsupported": self.unsupported,
            "wall_time_ms": self.wall_time_ms,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CollectorReport":
        if not isinstance(data, Mapping):
            raise MetricsCollectorError(
                f"CollectorReport.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        results = {
            str(name): MeterResult.from_dict(entry)
            for name, entry in (data.get("results") or {}).items()
        }
        return cls(
            results=results,
            total_energy_joules=float(data.get("total_energy_joules", 0.0)),
            total_power_watts=float(data.get("total_power_watts", 0.0)),
            meter_count=int(data.get("meter_count", len(results))),
            successful=int(data.get("successful", 0)),
            failed=int(data.get("failed", 0)),
            unsupported=int(data.get("unsupported", 0)),
            wall_time_ms=float(data.get("wall_time_ms", 0.0)),
        )

    # Backward-compatible "raw dict" view: ``report.results`` maps
    # ``meter_name -> MeterResult``; ``report.metrics`` maps
    # ``meter_name -> raw metrics dict``.
    @property
    def metrics(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for name, res in self.results.items():
            if res.status == "ok" and res.sample is not None:
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
            f"power_watts={self.total_power_watts:.4g})"
        )


# --------------------------------------------------------------------------- #
# Collector
# --------------------------------------------------------------------------- #
class MetricsCollector:
    """
    Aggregates all energy and runtime meters.

    Thread-safe, serializable, and idempotent. The original public API
    (``__init__(meters)``, ``start_all()``, ``stop_all()``) is preserved;
    new parameters are keyword-only.

    Parameters
    ----------
    meters : iterable
        Meters to aggregate. Each should expose ``start()`` and ``stop()``;
        objects that do not are handled according to
        ``config.strict_meter_types``.
    config : CollectorConfig, optional
        Failure policy, name-collision policy, per-meter warning threshold.
    strict : bool, default True
        If True, invalid inputs raise :class:`MetricsCollectorError`.
    """

    # ------------------------------------------------------------------ init
    def __init__(
        self,
        meters: Sequence[Any],
        *,
        config: Optional[CollectorConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or CollectorConfig()
        self._strict = bool(strict)

        if meters is None or isinstance(meters, (str, bytes)):
            raise MetricsCollectorError(
                f"meters must be a non-string iterable, "
                f"got {type(meters).__name__}."
            )
        try:
            meter_list = list(meters)
        except TypeError as exc:
            raise MetricsCollectorError(
                f"meters must be iterable, got {type(meters).__name__}."
            ) from exc

        self._inner_lock = threading.RLock()
        self._started: bool = False
        self._start_wall_time: Optional[float] = None

        # Resolve names with a de-duplication pass.
        self.meters: List[Any] = []
        self._names: List[str] = []
        seen: Dict[str, int] = {}
        unsupported: List[Any] = []

        for idx, m in enumerate(meter_list):
            if m is None:
                msg = f"meters[{idx}] is None."
                if self._strict:
                    raise MetricsCollectorError(msg)
                logger.warning("%s Skipping.", msg)
                unsupported.append(m)
                continue
            if not (callable(getattr(m, "start", None))
                    and callable(getattr(m, "stop", None))):
                msg = (
                    f"meters[{idx}] ({type(m).__name__}) does not implement "
                    f"start()/stop()."
                )
                if self._strict and self._config.strict_meter_types:
                    raise MetricsCollectorError(msg)
                logger.warning("%s Registering as unsupported.", msg)
                unsupported.append(m)
                continue

            base_name = type(m).__name__ or f"Meter{idx}"
            name = self._resolve_name(base_name, seen)
            if name is None:
                # Policy dropped this meter.
                continue
            self.meters.append(m)
            self._names.append(name)

        # Keep unsupported meters accessible for the report.
        self._unsupported: List[Any] = unsupported

        logger.debug(
            "MetricsCollector initialized with %d supported meter(s) "
            "and %d unsupported entry(ies) (strict=%s, fail_fast=%s)",
            len(self.meters),
            len(self._unsupported),
            self._strict,
            self._config.fail_fast,
        )

    # ---------------------------------------------------------- naming
    def _resolve_name(self, base_name: str, seen: Dict[str, int]) -> Optional[str]:
        """Apply the name-collision policy and return the resolved name."""
        policy = self._config.name_collision_policy
        if base_name not in seen:
            seen[base_name] = 1
            return base_name

        seen[base_name] += 1
        if policy == "suffix":
            return f"{base_name}__{seen[base_name]}"
        if policy == "skip":
            logger.debug("Skipping duplicate meter name '%s'.", base_name)
            return None
        # "replace" — caller removes the earlier entry outside this helper.
        # Signal a replace by returning the base_name and letting caller
        # handle the removal; we do it inline below.
        for i, existing in enumerate(self._names):
            if existing == base_name:
                del self.meters[i]
                del self._names[i]
                break
        return base_name

    # ------------------------------------------------------------------ props
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
        """
        Start every meter.

        Returns a dict mapping meter name → status (``"ok"`` or ``"failed"``).
        Idempotent: calling twice is a no-op (log-and-return).
        """
        with self._inner_lock:
            if self._started:
                logger.debug("start_all() called while already started.")
                return {name: "already_started" for name in self._names}

            self._started = True
            self._start_wall_time = time.monotonic()

            statuses: Dict[str, str] = {}
            for name, meter in zip(self._names, self.meters):
                start_ts = time.monotonic()
                try:
                    meter.start()
                except Exception as exc:
                    statuses[name] = "failed"
                    logger.exception("meter '%s' start() failed: %s", name, exc)
                    if self._config.fail_fast or self._strict:
                        # Roll back already-started meters before raising.
                        self._rollback_started(list(statuses.keys())[:-1])
                        self._started = False
                        raise MetricsCollectorError(
                            f"meter '{name}' start() failed: {exc}"
                        ) from exc
                    continue
                duration_ms = (time.monotonic() - start_ts) * 1000.0
                if duration_ms > self._config.per_meter_warn_seconds * 1000.0:
                    logger.warning(
                        "meter '%s' start() took %.2fms "
                        "(> per_meter_warn_seconds=%.2f).",
                        name,
                        duration_ms,
                        self._config.per_meter_warn_seconds,
                    )
                statuses[name] = "ok"

        logger.info(
            "start_all: %d ok, %d failed",
            sum(1 for s in statuses.values() if s == "ok"),
            sum(1 for s in statuses.values() if s == "failed"),
        )
        return statuses

    def stop_all(self) -> CollectorReport:
        """
        Stop every meter and return a :class:`CollectorReport`.

        The report is a superset of the original return value: it exposes
        ``report.metrics`` as a backward-compatible ``name -> dict`` view
        alongside structured ``MeterResult`` entries.
        """
        with self._inner_lock:
            wall_start = time.monotonic()
            results: "OrderedDict[str, MeterResult]" = OrderedDict()

            for name, meter in zip(self._names, self.meters):
                stop_ts = time.monotonic()
                try:
                    raw = meter.stop()
                except Exception as exc:
                    duration_ms = (time.monotonic() - stop_ts) * 1000.0
                    logger.exception("meter '%s' stop() failed: %s", name, exc)
                    results[name] = MeterResult(
                        meter_name=name,
                        meter_type=type(meter).__name__,
                        status="failed",
                        duration_ms=duration_ms,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                    if self._config.fail_fast:
                        # Still mark collector as stopped, then raise.
                        self._started = False
                        self._start_wall_time = None
                        raise MetricsCollectorError(
                            f"meter '{name}' stop() failed: {exc}"
                        ) from exc
                    continue

                duration_ms = (time.monotonic() - stop_ts) * 1000.0
                sample = self._normalize(name, meter, raw)
                if sample is None:
                    results[name] = MeterResult(
                        meter_name=name,
                        meter_type=type(meter).__name__,
                        status="failed",
                        duration_ms=duration_ms,
                        error="meter did not return a valid metrics mapping.",
                    )
                    continue

                results[name] = MeterResult(
                    meter_name=name,
                    meter_type=type(meter).__name__,
                    status="ok",
                    duration_ms=duration_ms,
                    sample=sample,
                    raw_metrics=self._safe_raw(raw),
                )
                if duration_ms > self._config.per_meter_warn_seconds * 1000.0:
                    logger.warning(
                        "meter '%s' stop() took %.2fms "
                        "(> per_meter_warn_seconds=%.2f).",
                        name,
                        duration_ms,
                        self._config.per_meter_warn_seconds,
                    )

            # Unsupported entries are reported explicitly.
            for idx, m in enumerate(self._unsupported):
                name = f"unsupported__{type(m).__name__ if m is not None else 'None'}__{idx}"
                results[name] = MeterResult(
                    meter_name=name,
                    meter_type=type(m).__name__ if m is not None else "NoneType",
                    status="unsupported",
                    duration_ms=0.0,
                    error="meter does not implement start()/stop().",
                )

            self._started = False
            self._start_wall_time = None

        # Aggregate energy / power across successful samples.
        total_energy = 0.0
        total_power = 0.0
        for r in results.values():
            if r.status != "ok" or r.sample is None:
                continue
            if (
                self._config.aggregate_measured_only
                and r.sample.provenance != "measured"
            ):
                continue
            total_energy += float(r.sample.energy_joules or 0.0)
            total_power += float(r.sample.power_watts or 0.0)

        successful = sum(1 for r in results.values() if r.status == "ok")
        failed = sum(1 for r in results.values() if r.status == "failed")
        unsupported = sum(1 for r in results.values() if r.status == "unsupported")

        report = CollectorReport(
            results=dict(results),
            total_energy_joules=total_energy,
            total_power_watts=total_power,
            meter_count=len(results),
            successful=successful,
            failed=failed,
            unsupported=unsupported,
            wall_time_ms=(time.monotonic() - wall_start) * 1000.0,
        )

        logger.info(
            "stop_all: %d ok, %d failed, %d unsupported "
            "(energy=%.6g J, power=%.4g W, wall=%.2fms)",
            successful,
            failed,
            unsupported,
            total_energy,
            total_power,
            report.wall_time_ms,
        )
        return report

    # ---------------------------------------------------------- internals
    def _rollback_started(self, names: Sequence[str]) -> None:
        """Best-effort stop of meters that were successfully started."""
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
                    "rollback stop() for '%s' failed: %s", name, exc
                )

    def _normalize(
        self, name: str, meter: Any, raw: Any
    ) -> Optional[TelemetrySample]:
        """Convert a raw meter result into a :class:`TelemetrySample`."""
        if raw is None:
            msg = f"meter '{name}' returned None."
            if self._strict:
                raise MetricsCollectorError(msg)
            logger.warning("%s", msg)
            return None

        # Preferred: the meter already validated its own result.
        if isinstance(raw, TelemetrySample):
            return raw

        # Fallback: ask BaseEnergyMeter subclasses to validate.
        if isinstance(meter, BaseEnergyMeter):
            try:
                validated = meter._validate_metrics(raw)  # noqa: SLF001
            except InstrumentationError as exc:
                msg = f"meter '{name}' contract violation: {exc}"
                if self._strict:
                    raise MetricsCollectorError(msg) from exc
                logger.warning("%s", msg)
                return None
            return TelemetrySample.from_metrics(
                validated,
                source=name,
                elapsed_seconds=getattr(meter, "elapsed_seconds", None),
            )

        # Fallback: try to build a TelemetrySample from a dict directly.
        if isinstance(raw, Mapping):
            try:
                return TelemetrySample.from_metrics(raw, source=name)
            except (InstrumentationError, KeyError, TypeError, ValueError) as exc:
                msg = (
                    f"meter '{name}' returned a mapping that does not match "
                    f"the TelemetrySample contract: {exc}"
                )
                if self._strict:
                    raise MetricsCollectorError(msg) from exc
                logger.warning("%s", msg)
                return None

        msg = f"meter '{name}' returned unsupported type {type(raw).__name__}."
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

    # ---------------------------------------------------------- context mgr
    @contextmanager
    def measure(self) -> Iterator["MetricsCollector"]:
        """
        Sync context manager that wires ``start_all`` / ``stop_all`` around
        a block, exposing the resulting :class:`CollectorReport` on
        ``self.last_report``.
        """
        self.last_report: Optional[CollectorReport] = None
        self.start_all()
        try:
            yield self
        finally:
            try:
                self.last_report = self.stop_all()
            except MetricsCollectorError:
                logger.exception("stop_all failed during context exit.")
                if self._strict:
                    raise

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._inner_lock:
            return {
                "config": asdict(self._config),
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
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "CollectorConfig",
    "CollectorReport",
    "MeterResult",
    "MetricsCollector",
    "MetricsCollectorError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m instrumentation.energy.metrics_collector
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---- Reference meters for the smoke test -------------------------- #
    class _FakeMeter(BaseEnergyMeter):
        def __init__(self, *, energy: float = 10.0, power: float = 5.0,
                     provenance: str = "measured", fail_on: str = "",
                     **kwargs: Any) -> None:
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
            })

    class _BadContract(BaseEnergyMeter):
        """Violates the stop() contract."""
        def start(self) -> None:
            self._mark_started()

        def stop(self) -> Dict[str, Any]:
            self._mark_stopped()
            return {"energy_joules": -1.0}  # type: ignore[return-value]

    class _NonMeter:
        """Not a meter at all."""
        pass

    # ---- Happy path --------------------------------------------------- #
    collector = MetricsCollector([
        _FakeMeter(energy=10.0, power=5.0),
        _FakeMeter(energy=20.0, power=8.0),
    ])
    print("repr       :", collector)
    print("start      :", collector.start_all())
    report: CollectorReport = collector.stop_all()
    print("report     :", report)
    print("metrics    :", report.metrics)

    # ---- Duplicate class names (the original bug) --------------------- #
    dupes = MetricsCollector([
        _FakeMeter(energy=1.0), _FakeMeter(energy=2.0), _FakeMeter(energy=3.0),
    ])
    dupes.start_all()
    dup_report = dupes.stop_all()
    print("duplicates :", list(dup_report.results.keys()))
    assert len(dup_report.results) == 3, "no meter should be lost"

    # ---- Per-meter failure isolation ---------------------------------- #
    mixed = MetricsCollector([
        _FakeMeter(energy=5.0),
        _FakeMeter(energy=0.0, fail_on="stop"),
        _FakeMeter(energy=7.0),
    ])
    mixed.start_all()
    mixed_report = mixed.stop_all()
    print("mixed      :", {
        name: r.status for name, r in mixed_report.results.items()
    })
    assert mixed_report.failed == 1
    assert mixed_report.successful == 2

    # ---- fail_fast raises -------------------------------------------- #
    strict_coll = MetricsCollector(
        [_FakeMeter(energy=0.0, fail_on="stop")],
        config=CollectorConfig(fail_fast=True),
    )
    strict_coll.start_all()
    try:
        strict_coll.stop_all()
    except MetricsCollectorError as exc:
        print("fail_fast  :", exc)

    # ---- Unsupported entries (non-strict) ---------------------------- #
    lenient = MetricsCollector(
        [_FakeMeter(energy=1.0), _NonMeter()],
        config=CollectorConfig(strict_meter_types=False),
    )
    lenient.start_all()
    len_report = lenient.stop_all()
    print("lenient    :", {
        name: r.status for name, r in len_report.results.items()
    })

    # ---- Context manager --------------------------------------------- #
    with MetricsCollector([
        _FakeMeter(energy=1.0), _FakeMeter(energy=2.0),
    ]) as c:
        time.sleep(0.01)
    print("context    :", c.last_report)

    # ---- measured_only aggregation ----------------------------------- #
    measured = MetricsCollector(
        [
            _FakeMeter(energy=10.0, provenance="measured"),
            _FakeMeter(energy=100.0, provenance="estimated"),
        ],
        config=CollectorConfig(aggregate_measured_only=True),
    )
    measured.start_all()
    m_report = measured.stop_all()
    print("measured   :", m_report.total_energy_joules, "J "
          "(expected 10.0 — the estimated meter is excluded)")

    # ---- Serialization round-trip ------------------------------------ #
    payload = report.to_dict()
    restored = CollectorReport.from_dict(json.loads(json.dumps(payload)))
    assert restored.to_dict() == report.to_dict()
    print("Round-trip OK.")

    # ---- Validation failures ----------------------------------------- #
    for bad in (None, "not-a-list", 123):
        try:
            MetricsCollector(bad)  # type: ignore[arg-type]
        except MetricsCollectorError as exc:
            print("Rejected   :", exc)

    for bad_cfg in (
        dict(name_collision_policy="bogus"),
        dict(per_meter_warn_seconds=0),
    ):
        try:
            CollectorConfig(**bad_cfg)  # type: ignore[arg-type]
        except MetricsCollectorError as exc:
            print("Rejected cfg:", exc)

    print("\nSmoke test passed.")
