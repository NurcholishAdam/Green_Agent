#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
docker_metrics_collector.py (Enhanced v2)

Collects runtime, memory, and CPU-based energy metrics from inside a
Docker container for green benchmarking.

Enhancements (enabled via ``use_enhancements=True``):
  - MODP (multi-objective composite score) with caller-supplied reference
    maxima per objective.
  - LIMIT Graph metrics (centrality, connectivity, density) surfaced in the
    result dict.
  - RLHF human feedback score surfaced in the result.
  - Optional distillation / MoE statistics.
  - FlexGen energy — accepted as a static config value **or** measured via
    a callback at end of run.

---- Critical fixes vs v1 ---------------------------------------------------
  FIX #1   cgroup v2 peak memory. v1 read ``memory.current`` (a snapshot,
           not a peak) on cgroup v2, returning values 5–10× lower than
           actual peak. Now prefers ``memory.peak`` (kernel ≥ 5.19), then
           ``memory.max_usage_in_bytes`` (v1), then falls back to
           ``memory.current`` with a RuntimeWarning.
  FIX #2   ``/proc/self/stat`` parsing. ``comm`` can contain spaces and
           parens; v1's naive ``.split()`` shifted field indices and read
           the wrong utime/stime on process names like ``python (worker)``.
           Now locates the closing paren before splitting.
  FIX #3   ``runs < 1`` raised ``StatisticsError`` from ``pvariance`` /
           ``mean``. Now raises ``ValueError`` at the top of
           ``run_and_measure``.
  FIX #4   ``fn()`` returning ``None`` / non-numeric raised ``TypeError``
           from ``statistics.mean``. Now validates the return type and
           raises ``TypeError`` naming the observed type.
  FIX #5   ``fn()`` returning a coroutine (async callable) crashed
           ``statistics.mean`` with a cryptic error. Now rejects async
           callables explicitly.
  FIX #6   ``numpy`` was imported but never used. The two branches of
           ``_compute_modp_score`` were semantically different (unweighted
           vs weighted) yet gated on a numpy flag — removing numpy
           removed the false branch. One implementation, weighted.
  FIX #7   The enhancement schema imports (FeedbackEvent,
           NodeDescriptor, WorkloadDescriptor) were unused. If they failed
           to import, ``use_enhancements`` silently turned off even though
           the enhancement code never touched them. Removed;
           ``use_enhancements`` is honored verbatim.
  FIX #8   MODP normalization constants were hard-coded (1 Wh / 10 s /
           0.1 kg). On real workloads the score was ~0.9 for every
           objective, giving the composite almost no dynamic range.
           Now accepts ``energy_max_wh``, ``latency_max_s``,
           ``carbon_max_kg`` at construction time.
  FIX #9   No warm-up run — the first ``fn()`` call skewed the mean.
           Added ``warmup_runs`` (default 1), discarded from statistics.
  FIX #10  No thread safety — two concurrent callers could interleave
           ``_read_cpu_time`` and produce garbage. Added a
           ``threading.Lock``.
  FIX #11  Negative ``end_cpu - start_cpu`` was possible when the kernel
           migrated the process between CPUs mid-run. Now clamped to 0.
  FIX #12  No timeout on ``fn()``. A hung benchmark blocked the call
           forever. Added ``timeout_s``.
  FIX #13  ``carbon_intensity`` unit ambiguity. The default ``0.0004`` is
           400 gCO2/kWh, but a caller passing ``400`` (thinking g/kWh)
           produced a value 1e6× too large. Now warns when the value is
           implausibly large for kg/Wh.
  FIX #14  ``_read_cgroup_memory_peak`` raised ``RuntimeError`` without
           listing the paths tried. Now includes them in the message.
  FIX #15  Type hint was ``Dict[str, float]``, but the result dict
           contains nested dicts. Now ``Dict[str, Any]``.
  FIX #16  ``fn`` could not take arguments. ``run_and_measure`` now
           accepts ``fn_args`` / ``fn_kwargs`` and forwards them.
  FIX #17  No baseline comparison — added ``run_and_measure_vs`` that
           returns deltas against a previously captured result.
  FIX #18  ``flexgen_energy_joules`` was accepted only as a static config
           value. Now accepts an optional ``flexgen_energy_cb`` callback
           that measures it at end of run.
  FIX #19  Linux-only but undocumented. Class now raises a clear error
           at construction when ``sys.platform`` is not Linux, unless
           ``allow_unsupported=True``.

---- Backward compatibility -------------------------------------------------
  ``DockerMetricsCollector(carbon_intensity, cpu_tdp_watts,
  use_enhancements, enhancement_config)`` keeps its signature and
  default behavior. New keyword arguments have sensible defaults.
  ``run_and_measure(fn, runs)`` keeps its signature; new keyword
  arguments (``warmup_runs``, ``timeout_s``, ``fn_args``, ``fn_kwargs``,
  ``flexgen_energy_cb``) are additive.

---- Run --------------------------------------------------------------------
  python docker_metrics_collector.py        # self-test
"""

from __future__ import annotations

import inspect
import logging
import os
import statistics
import sys
import threading
import time
import warnings
from typing import Any, Callable, Dict, List, Optional


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Platform guard
# ---------------------------------------------------------------------------
IS_LINUX = sys.platform.startswith("linux")


# ---------------------------------------------------------------------------
# The collector
# ---------------------------------------------------------------------------
class DockerMetricsCollector:
    """
    Collect runtime / memory / CPU-based energy metrics from inside a
    container.

    Notes
    -----
    * **Linux only.** cgroup files and ``/proc/self/stat`` are Linux
      concepts. Constructing with ``allow_unsupported=True`` suppresses
      the check, but the numbers will be wrong.
    * Energy is estimated as ``cpu_time × cpu_tdp_watts``. This is a
      *proxy*, not a measurement. For real energy, use ``pyRAPL``
      (host RAPL counters) or ``nvidia-ml-py`` (NVML).
    * Carbon is ``energy_wh × carbon_intensity``, where
      ``carbon_intensity`` is **kg CO2 per Wh**. The default
      ``0.0004`` corresponds to 400 g CO2/kWh — a typical grid value.
    """

    _CARBON_INTENSITY_PLAUSIBLE_MAX = 1.0   # 1.0 kg/Wh = 1000 g/kWh

    def __init__(
        self,
        carbon_intensity: float = 0.0004,
        cpu_tdp_watts: float = 65.0,
        use_enhancements: bool = False,
        enhancement_config: Optional[Dict[str, Any]] = None,
        *,
        energy_max_wh: float = 1.0,
        latency_max_s: float = 10.0,
        carbon_max_kg: float = 0.1,
        allow_unsupported: bool = False,
    ) -> None:
        # FIX #19 — Linux-only guard
        if not IS_LINUX and not allow_unsupported:
            raise RuntimeError(
                f"DockerMetricsCollector requires Linux "
                f"(cgroup + /proc/self/stat); current platform is "
                f"{sys.platform!r}. Pass allow_unsupported=True only if "
                f"you understand the results will be wrong."
            )

        # FIX #13 — plausibility warning for likely unit mistakes
        if carbon_intensity > self._CARBON_INTENSITY_PLAUSIBLE_MAX:
            warnings.warn(
                f"carbon_intensity={carbon_intensity} > "
                f"{self._CARBON_INTENSITY_PLAUSIBLE_MAX} kg/Wh. "
                f"Did you pass gCO2/kWh instead of kgCO2/Wh? "
                f"(400 g/kWh == 0.0004 kg/Wh)",
                RuntimeWarning,
                stacklevel=2,
            )

        if cpu_tdp_watts <= 0:
            raise ValueError(
                f"cpu_tdp_watts must be > 0, got {cpu_tdp_watts}"
            )
        if energy_max_wh <= 0 or latency_max_s <= 0 or carbon_max_kg <= 0:
            raise ValueError(
                "energy_max_wh, latency_max_s, carbon_max_kg must be > 0"
            )

        self.carbon_intensity = carbon_intensity
        self.cpu_tdp_watts = cpu_tdp_watts
        # FIX #7 — honor use_enhancements verbatim
        self.use_enhancements = use_enhancements
        self.enhancement_config = enhancement_config or {}

        # FIX #8 — reference maxima
        self.energy_max_wh = energy_max_wh
        self.latency_max_s = latency_max_s
        self.carbon_max_kg = carbon_max_kg

        # MODP weights (normalized)
        raw_weights = self.enhancement_config.get(
            "modp_weights",
            [0.4, 0.3, 0.2, 0.1],   # accuracy, energy, latency, carbon
        )
        self.modp_weights = list(raw_weights)
        if len(self.modp_weights) != 4:
            raise ValueError(
                f"modp_weights must have exactly 4 entries, "
                f"got {len(self.modp_weights)}"
            )
        total = float(sum(self.modp_weights))
        if total > 0:
            self.modp_weights = [w / total for w in self.modp_weights]
        else:
            self.modp_weights = [0.25, 0.25, 0.25, 0.25]

        # FIX #10 — thread safety
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        return (
            f"DockerMetricsCollector("
            f"carbon_intensity={self.carbon_intensity}kg/Wh, "
            f"cpu_tdp_watts={self.cpu_tdp_watts}, "
            f"use_enhancements={self.use_enhancements}, "
            f"modp_weights={self.modp_weights})"
        )

    # ------------------------------------------------------------------
    # Metric readers
    # ------------------------------------------------------------------
    @staticmethod
    def _read_cgroup_memory_peak() -> float:
        """
        Return peak memory usage in MB.

        Preference order (most accurate first):
            1. ``memory.peak``                  (cgroup v2, kernel ≥ 5.19)
            2. ``memory.max_usage_in_bytes``    (cgroup v1)
            3. ``memory.current``               (cgroup v2 fallback —
               not a peak; a RuntimeWarning is emitted)
        """
        candidates = [
            ("/sys/fs/cgroup/memory.peak", True),
            ("/sys/fs/cgroup/memory.max_usage_in_bytes", True),
            ("/sys/fs/cgroup/memory.current", False),
        ]
        tried: List[str] = []
        for path, is_peak in candidates:
            tried.append(path)
            if not os.path.exists(path):
                continue
            try:
                with open(path, "r") as f:
                    raw = f.read().strip()
            except OSError as exc:
                logger.debug("Cannot open %s: %s", path, exc)
                continue
            if raw == "max":
                logger.debug("%s contains 'max'; skipping", path)
                continue
            try:
                value = int(raw) / (1024 ** 2)
            except ValueError:
                logger.debug("Non-numeric %s: %r", path, raw)
                continue
            if not is_peak:
                warnings.warn(
                    "Only memory.current is available on this kernel; "
                    "peak memory may be under-reported. Upgrade to "
                    "kernel 5.19+ for memory.peak, or use cgroup v1 "
                    "for memory.max_usage_in_bytes.",
                    RuntimeWarning,
                    stacklevel=2,
                )
            return value
        # FIX #14 — include the tried paths
        raise RuntimeError(
            "Unable to read cgroup memory stats; tried paths: "
            + ", ".join(tried)
        )

    @staticmethod
    def _read_cpu_time() -> float:
        """
        Return CPU time used by the current process, in seconds.

        ``/proc/self/stat`` format::

            pid (comm) state ppid ... utime stime ...

        ``comm`` is wrapped in parentheses and may itself contain
        spaces or ``)`` — a naive split shifts every subsequent index.
        We locate the closing paren and split the remainder.
        """
        with open("/proc/self/stat", "r") as f:
            raw = f.read()
        close_paren = raw.rfind(")")
        if close_paren == -1:
            raise RuntimeError(
                f"Unexpected /proc/self/stat format: {raw[:80]!r}"
            )
        # After `pid (comm) ` the next field is state (index 0). The
        # original layout has utime at field 14 (1-indexed), stime at
        # 15; after dropping the first two, they are indices 11 and 12.
        fields = raw[close_paren + 2:].split()
        try:
            utime = int(fields[11])
            stime = int(fields[12])
        except (IndexError, ValueError) as exc:
            raise RuntimeError(
                f"Unexpected /proc/self/stat layout: {exc}; "
                f"fields={fields[:15]!r}"
            ) from exc
        clock_ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        return (utime + stime) / clock_ticks

    # ------------------------------------------------------------------
    # Enhanced metric helpers
    # ------------------------------------------------------------------
    def _compute_modp_score(
        self,
        accuracy: float,
        energy_wh: float,
        latency_s: float,
        carbon_kg: float,
    ) -> float:
        """
        Weighted composite in [0, 1]; higher is better.

        Each objective is normalized against a **caller-supplied
        reference maximum** (``energy_max_wh``, ``latency_max_s``,
        ``carbon_max_kg``).
        """
        acc_norm = max(0.0, min(float(accuracy), 1.0))
        energy_norm = max(0.0, 1.0 - float(energy_wh) / self.energy_max_wh)
        latency_norm = max(0.0, 1.0 - float(latency_s) / self.latency_max_s)
        carbon_norm = max(0.0, 1.0 - float(carbon_kg) / self.carbon_max_kg)

        w = self.modp_weights
        return float(
            w[0] * acc_norm
            + w[1] * energy_norm
            + w[2] * latency_norm
            + w[3] * carbon_norm
        )

    def _get_graph_metrics(self) -> Dict[str, float]:
        gm = self.enhancement_config.get("graph_metrics")
        if not isinstance(gm, dict):
            gm = {}
        return {
            "centrality": float(gm.get("centrality", 0.5)),
            "connectivity": float(gm.get("connectivity", 0.5)),
            "density": float(gm.get("density", 0.5)),
        }

    def _get_human_feedback_score(self) -> Optional[float]:
        value = self.enhancement_config.get("human_feedback_score")
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Runner helper (FIX #5, #12)
    # ------------------------------------------------------------------
    @staticmethod
    def _invoke(
        fn: Callable[..., float],
        timeout_s: Optional[float],
        args: tuple,
        kwargs: dict,
    ) -> float:
        """Invoke ``fn`` with validation and optional timeout."""
        if inspect.iscoroutinefunction(fn):
            raise TypeError(
                f"fn must be a sync callable; got an async function "
                f"{getattr(fn, '__name__', repr(fn))!r}"
            )

        if timeout_s is None:
            result = fn(*args, **kwargs)
        else:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(fn, *args, **kwargs)
                try:
                    result = fut.result(timeout=timeout_s)
                except concurrent.futures.TimeoutError:
                    fut.cancel()
                    raise TimeoutError(
                        f"fn() did not return within {timeout_s}s"
                    ) from None

        # FIX #5 — reject coroutine results (async non-function callables)
        if inspect.iscoroutine(result):
            raise TypeError(
                "fn() returned a coroutine; pass a sync callable or "
                "await the coroutine before calling run_and_measure"
            )
        # FIX #4 — reject bools and non-numerics
        if isinstance(result, bool):
            raise TypeError(
                f"fn() returned bool ({result!r}); expected a numeric score"
            )
        if not isinstance(result, (int, float)):
            raise TypeError(
                f"fn() must return a number, got {type(result).__name__}"
            )
        return float(result)

    # ------------------------------------------------------------------
    # Public API — measure
    # ------------------------------------------------------------------
    def run_and_measure(
        self,
        fn: Callable[..., float],
        runs: int = 5,
        *,
        warmup_runs: int = 1,
        timeout_s: Optional[float] = None,
        fn_args: Optional[tuple] = None,
        fn_kwargs: Optional[dict] = None,
        flexgen_energy_cb: Optional[Callable[[], float]] = None,
    ) -> Dict[str, Any]:
        """
        Execute ``fn`` ``runs`` times and collect metrics.

        Parameters
        ----------
        fn :
            Sync callable returning a numeric score (typically accuracy
            in ``[0, 1]``). Async callables and non-numeric returns
            raise ``TypeError``.
        runs :
            Number of measured iterations. Must be >= 1.
        warmup_runs :
            Discarded iterations used to prime caches and JITs.
            Default 1. Set to 0 to disable.
        timeout_s :
            Per-call timeout for ``fn``. When set, ``fn`` is executed
            in a worker thread and a ``TimeoutError`` is raised if it
            does not return in time.
        fn_args / fn_kwargs :
            Positional / keyword arguments forwarded to ``fn``.
        flexgen_energy_cb :
            Optional callable returning FlexGen energy in joules at end
            of run. Overrides any static
            ``enhancement_config['flexgen_energy_joules']``.

        Returns
        -------
        dict
            Base fields: ``runs``, ``warmup_runs``, ``accuracy``,
            ``latency``, ``latency_variance``, ``latency_median``,
            ``latency_p95``, ``latency_max``, ``cpu_time``, ``energy``,
            ``carbon``, ``memory``.

            When ``use_enhancements=True``: ``modp_score``,
            ``graph_metrics``, optionally ``human_feedback_score``,
            ``distillation_stats``, ``flexgen_energy_joules``.
        """
        # FIX #3 — input validation
        if not callable(fn):
            raise TypeError(f"fn must be callable, got {type(fn).__name__}")
        if not isinstance(runs, int) or isinstance(runs, bool) or runs < 1:
            raise ValueError(f"runs must be int >= 1, got {runs!r}")
        if not isinstance(warmup_runs, int) or \
                isinstance(warmup_runs, bool) or warmup_runs < 0:
            raise ValueError(
                f"warmup_runs must be int >= 0, got {warmup_runs!r}"
            )
        if fn_args is not None and not isinstance(fn_args, tuple):
            raise TypeError(
                f"fn_args must be a tuple, got {type(fn_args).__name__}"
            )
        if fn_kwargs is not None and not isinstance(fn_kwargs, dict):
            raise TypeError(
                f"fn_kwargs must be a dict, got {type(fn_kwargs).__name__}"
            )
        args = fn_args or ()
        kwargs = fn_kwargs or {}

        # FIX #10 — serialize concurrent callers
        with self._lock:
            # FIX #9 — warm-up runs
            for _ in range(warmup_runs):
                self._invoke(fn, timeout_s, args, kwargs)

            latencies: List[float] = []
            cpu_times: List[float] = []
            accuracies: List[float] = []

            for _ in range(runs):
                start_cpu = self._read_cpu_time()
                start_time = time.perf_counter()

                acc = self._invoke(fn, timeout_s, args, kwargs)

                end_time = time.perf_counter()
                end_cpu = self._read_cpu_time()

                latencies.append(end_time - start_time)
                # FIX #11 — clamp negative deltas
                cpu_times.append(max(0.0, end_cpu - start_cpu))
                accuracies.append(acc)

            avg_latency = statistics.mean(latencies)
            avg_cpu_time = statistics.mean(cpu_times)
            avg_accuracy = statistics.mean(accuracies)

            energy_wh = (avg_cpu_time * self.cpu_tdp_watts) / 3600.0
            carbon_kg = energy_wh * self.carbon_intensity

            peak_memory_mb = self._read_cgroup_memory_peak()

            sorted_lat = sorted(latencies)
            result: Dict[str, Any] = {
                "runs": runs,
                "warmup_runs": warmup_runs,
                "accuracy": avg_accuracy,
                "latency": avg_latency,
                "latency_variance": (
                    statistics.pvariance(latencies) if runs > 1 else 0.0
                ),
                "latency_median": statistics.median(latencies),
                "latency_p95": self._percentile(sorted_lat, 95),
                "latency_max": max(latencies),
                "cpu_time": avg_cpu_time,
                "energy": energy_wh,
                "carbon": carbon_kg,
                "memory": peak_memory_mb,
            }

            # Enhanced fields
            if self.use_enhancements:
                result["modp_score"] = self._compute_modp_score(
                    accuracy=avg_accuracy,
                    energy_wh=energy_wh,
                    latency_s=avg_latency,
                    carbon_kg=carbon_kg,
                )
                result["graph_metrics"] = self._get_graph_metrics()

                hf = self._get_human_feedback_score()
                if hf is not None:
                    result["human_feedback_score"] = hf

                if "distillation_stats" in self.enhancement_config:
                    result["distillation_stats"] = (
                        self.enhancement_config["distillation_stats"]
                    )

                # FIX #18 — callback wins over static config
                flexgen_energy: Optional[float] = None
                if flexgen_energy_cb is not None:
                    try:
                        raw = flexgen_energy_cb()
                        if isinstance(raw, (int, float)) and \
                                not isinstance(raw, bool):
                            flexgen_energy = float(raw)
                        else:
                            logger.warning(
                                "flexgen_energy_cb returned %r; ignoring",
                                raw,
                            )
                    except Exception as exc:            # noqa: BLE001
                        logger.warning(
                            "flexgen_energy_cb raised %s: %s",
                            type(exc).__name__, exc,
                        )
                elif "flexgen_energy_joules" in self.enhancement_config:
                    raw = self.enhancement_config["flexgen_energy_joules"]
                    if isinstance(raw, (int, float)) and \
                            not isinstance(raw, bool):
                        flexgen_energy = float(raw)
                if flexgen_energy is not None:
                    result["flexgen_energy_joules"] = flexgen_energy

            return result

    # ------------------------------------------------------------------
    # Public API — baseline comparison (FIX #17)
    # ------------------------------------------------------------------
    def run_and_measure_vs(
        self,
        fn: Callable[..., float],
        baseline: Dict[str, Any],
        runs: int = 5,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Measure ``fn`` and return deltas against ``baseline``.

        ``baseline`` is a dict previously returned by ``run_and_measure``.
        For each numeric key present in both dicts, a
        ``delta_<key>_pct`` field is added. ``accuracy`` gets an absolute
        ``delta_accuracy_points`` instead of a percentage — "accuracy
        went up 2 points" is more useful than "up 2.1%".
        """
        current = self.run_and_measure(fn, runs=runs, **kwargs)
        comparison: Dict[str, Any] = dict(current)
        comparison["baseline"] = dict(baseline)

        for key in ("energy", "carbon", "latency", "cpu_time", "memory"):
            base_val = baseline.get(key)
            new_val = current.get(key)
            if not isinstance(base_val, (int, float)):
                continue
            if not isinstance(new_val, (int, float)):
                continue
            if base_val == 0:
                continue
            comparison[f"delta_{key}_pct"] = (
                (new_val - base_val) / base_val * 100.0
            )

        if isinstance(baseline.get("accuracy"), (int, float)) and \
                isinstance(current.get("accuracy"), (int, float)):
            comparison["delta_accuracy_points"] = (
                current["accuracy"] - baseline["accuracy"]
            )

        return comparison

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------
    @staticmethod
    def _percentile(sorted_values: List[float], p: float) -> float:
        """Linear-interpolation percentile of an already-sorted list."""
        if not sorted_values:
            return 0.0
        if p <= 0:
            return sorted_values[0]
        if p >= 100:
            return sorted_values[-1]
        n = len(sorted_values)
        rank = (n - 1) * (p / 100.0)
        f = int(rank)
        c = min(f + 1, n - 1)
        if f == c:
            return sorted_values[f]
        return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (rank - f)


# ---------------------------------------------------------------------------
# Self-test — validates the pure helpers on any platform; exercises the
# instance methods only on Linux where construction is allowed.
# ---------------------------------------------------------------------------
def _self_test() -> int:
    failures = 0

    def _check(name: str, cond: bool, detail: str = "") -> None:
        nonlocal failures
        if cond:
            print(f"  ✓ {name}")
        else:
            print(f"  ✗ {name}{(': ' + detail) if detail else ''}")
            failures += 1

    # ------------------------------------------------------------------
    print("_percentile:")
    _check("p0 == min",
           DockerMetricsCollector._percentile([1, 2, 3, 4, 5], 0) == 1)
    _check("p100 == max",
           DockerMetricsCollector._percentile([1, 2, 3, 4, 5], 100) == 5)
    _check("p50 == median",
           DockerMetricsCollector._percentile([1, 2, 3, 4, 5], 50) == 3)
    _check("empty list == 0",
           DockerMetricsCollector._percentile([], 50) == 0.0)

    # ------------------------------------------------------------------
    print("_invoke (sync + validation):")
    _check("sync callable",
           DockerMetricsCollector._invoke(lambda: 0.9, None, (), {}) == 0.9)
    _check("with positional args",
           DockerMetricsCollector._invoke(
               lambda x, y: x + y, None, (1, 2), {}) == 3.0)
    _check("with kwargs",
           DockerMetricsCollector._invoke(
               lambda *, x: float(x), None, (), {"x": 5}) == 5.0)

    async def _async_fn():
        return 0.9

    try:
        DockerMetricsCollector._invoke(_async_fn, None, (), {})
        _check("async rejected", False)
    except TypeError:
        _check("async rejected", True)

    try:
        DockerMetricsCollector._invoke(lambda: None, None, (), {})
        _check("None rejected", False)
    except TypeError:
        _check("None rejected", True)

    try:
        DockerMetricsCollector._invoke(lambda: "nope", None, (), {})
        _check("string rejected", False)
    except TypeError:
        _check("string rejected", True)

    try:
        DockerMetricsCollector._invoke(lambda: True, None, (), {})
        _check("bool rejected", False)
    except TypeError:
        _check("bool rejected", True)

    # Coroutine returned by a non-async callable
    async def _inner():
        return 0.9

    try:
        DockerMetricsCollector._invoke(lambda: _inner(), None, (), {})
        _check("coroutine returned rejected", False)
    except TypeError:
        _check("coroutine returned rejected", True)

    # Timeout path
    import time as _time

    def _slow():
        _time.sleep(0.5)
        return 0.9

    try:
        DockerMetricsCollector._invoke(_slow, 0.05, (), {})
        _check("timeout enforced", False)
    except TimeoutError:
        _check("timeout enforced", True)

    # ------------------------------------------------------------------
    print("carbon_intensity unit warning:")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            if IS_LINUX:
                DockerMetricsCollector(carbon_intensity=400.0)
            else:
                DockerMetricsCollector(
                    carbon_intensity=400.0, allow_unsupported=True,
                )
            emitted = any(
                issubclass(w.category, RuntimeWarning)
                and "carbon_intensity" in str(w.message)
                for w in caught
            )
            _check("warned on implausible unit", emitted)
        except Exception as exc:                           # noqa: BLE001
            _check(f"construct raised {type(exc).__name__}: {exc}", False)

    # ------------------------------------------------------------------
    # Construction requires Linux by default — verify the guard.
    if not IS_LINUX:
        print("Linux-only guard:")
        try:
            DockerMetricsCollector()
            _check("raises on non-Linux", False)
        except RuntimeError:
            _check("raises on non-Linux", True)

    # ------------------------------------------------------------------
    # Instance-dependent tests only run on Linux (where construction works)
    collector = None
    if IS_LINUX:
        try:
            collector = DockerMetricsCollector(
                carbon_intensity=0.0004,
                cpu_tdp_watts=65.0,
                use_enhancements=True,
                enhancement_config={
                    "modp_weights": [0.4, 0.3, 0.2, 0.1],
                    "graph_metrics": {"centrality": 0.8, "connectivity": 0.6},
                    "human_feedback_score": 0.7,
                },
                energy_max_wh=0.01,
                latency_max_s=1.0,
                carbon_max_kg=0.004,
            )
        except Exception as exc:                           # noqa: BLE001
            print(f"  ⚠️  Cannot construct collector: {exc}")

    if collector is not None:
        print("__repr__:")
        _check("mentions class name", "DockerMetricsCollector" in repr(collector))
        _check("mentions enhancements",
               "use_enhancements=True" in repr(collector))

        print("_compute_modp_score:")
        # Perfect: accuracy 1.0, all other objectives 0 → 1.0
        perfect = collector._compute_modp_score(1.0, 0.0, 0.0, 0.0)
        _check("perfect == 1.0", abs(perfect - 1.0) < 1e-9)

        # Zero accuracy, all else perfect → sum of non-accuracy weights
        zero_acc = collector._compute_modp_score(0.0, 0.0, 0.0, 0.0)
        w = collector.modp_weights
        expected = w[1] + w[2] + w[3]
        _check("zero accuracy == non-accuracy weight sum",
               abs(zero_acc - expected) < 1e-9)

        # Max energy zeroes the energy term
        max_energy = collector._compute_modp_score(
            1.0, collector.energy_max_wh, 0.0, 0.0,
        )
        expected = w[0] + w[2] + w[3]
        _check("max energy zeroes energy term",
               abs(max_energy - expected) < 1e-9)

        # FIX #8 dynamic-range check: 10× lower energy should move the
        # score by a *visible* amount (the v1 file could not do this)
        low_energy = collector._compute_modp_score(
            0.9, collector.energy_max_wh * 0.9, 0.5, 0.0,
        )
        high_energy = collector._compute_modp_score(
            0.9, collector.energy_max_wh * 0.1, 0.5, 0.0,
        )
        _check(
            "10× lower energy moves score by > 0.1",
            (high_energy - low_energy) > 0.1,
            f"delta={high_energy - low_energy:.4f}",
        )

        print("run_and_measure input validation:")
        try:
            collector.run_and_measure(lambda: 0.9, runs=0)
            _check("runs=0 rejected", False)
        except ValueError:
            _check("runs=0 rejected", True)

        try:
            collector.run_and_measure(lambda: 0.9, runs=-1)
            _check("runs=-1 rejected", False)
        except ValueError:
            _check("runs=-1 rejected", True)

        try:
            collector.run_and_measure("not callable", runs=1)  # type: ignore
            _check("non-callable rejected", False)
        except TypeError:
            _check("non-callable rejected", True)

        try:
            collector.run_and_measure(lambda: 0.9, runs=1,
                                      fn_args=[1, 2])
            _check("non-tuple fn_args rejected", False)
        except TypeError:
            _check("non-tuple fn_args rejected", True)

        # ------------------------------------------------------------------
        print("run_and_measure smoke test:")
        try:
            result = collector.run_and_measure(
                lambda: 0.9, runs=3, warmup_runs=1,
            )
            _check("returns dict", isinstance(result, dict))
            _check("has runs", result.get("runs") == 3)
            _check("has warmup_runs", result.get("warmup_runs") == 1)
            _check("has accuracy", "accuracy" in result)
            _check("accuracy correct",
                   abs(result["accuracy"] - 0.9) < 1e-9)
            _check("has energy", "energy" in result)
            _check("energy >= 0", result["energy"] >= 0.0)
            _check("has latency_median", "latency_median" in result)
            _check("has latency_p95", "latency_p95" in result)
            _check("has latency_max", "latency_max" in result)
            _check("latency_max >= median",
                   result["latency_max"] >= result["latency_median"])
            _check("has modp_score (enhancements on)",
                   "modp_score" in result)
            _check("modp_score in [0,1]",
                   0.0 <= result["modp_score"] <= 1.0)
            _check("has graph_metrics",
                   isinstance(result.get("graph_metrics"), dict))
            _check("graph_metrics has centrality",
                   "centrality" in result["graph_metrics"])
            _check("has human_feedback_score",
                   abs(result.get("human_feedback_score", -1) - 0.7) < 1e-9)
        except Exception as exc:                           # noqa: BLE001
            _check(f"smoke test raised {type(exc).__name__}: {exc}", False)

        # ------------------------------------------------------------------
        print("fn with args/kwargs:")
        try:
            result = collector.run_and_measure(
                lambda a, b, *, factor: float((a + b) * factor),
                runs=2,
                warmup_runs=0,
                fn_args=(1, 2),
                fn_kwargs={"factor": 1.0},
            )
            _check("returned 3.0",
                   abs(result["accuracy"] - 3.0) < 1e-9)
        except Exception as exc:                           # noqa: BLE001
            _check(f"args/kwargs smoke test raised "
                   f"{type(exc).__name__}: {exc}", False)

        # ------------------------------------------------------------------
        print("flexgen_energy_cb:")
        try:
            result = collector.run_and_measure(
                lambda: 0.9, runs=1, warmup_runs=0,
                flexgen_energy_cb=lambda: 12345.0,
            )
            _check("flexgen_energy_joules surfaced",
                   result.get("flexgen_energy_joules") == 12345.0)
        except Exception as exc:                           # noqa: BLE001
            _check(f"flexgen cb smoke test raised "
                   f"{type(exc).__name__}: {exc}", False)

        # ------------------------------------------------------------------
        print("run_and_measure_vs (baseline comparison):")
        try:
            baseline = collector.run_and_measure(
                lambda: 0.90, runs=3, warmup_runs=0,
            )
            faster = collector.run_and_measure_vs(
                lambda: 0.95, baseline, runs=3, warmup_runs=0,
            )
            _check("has baseline key", "baseline" in faster)
            _check("has delta_accuracy_points",
                   "delta_accuracy_points" in faster)
            _check("delta_accuracy_points ≈ +0.05",
                   abs(faster["delta_accuracy_points"] - 0.05) < 1e-9)
        except Exception as exc:                           # noqa: BLE001
            _check(f"baseline comparison raised "
                   f"{type(exc).__name__}: {exc}", False)

    # ------------------------------------------------------------------
    print("")
    if failures:
        print(f"{failures} failure(s)")
        return 1
    print("all self-tests passed")
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sys.exit(_self_test())
