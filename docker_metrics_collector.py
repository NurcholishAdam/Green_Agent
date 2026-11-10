#!/usr/bin/env python3
"""
docker_metrics_collector.py — Docker-aware green benchmarking collector.

Collects runtime, memory, and CPU-based energy metrics from inside a
Docker container (or any Linux host with cgroup support).

Enhanced in v2.0
----------------
- **Fixed cgroup v2 peak memory** — v1.0 read `memory.current` (the *current*
  value at read time), not peak. v2.0 tries `memory.peak` (Linux 5.19+),
  falls back to parsing `memory.stat` for the `peak` key, then to
  `memory.current` with a warning.
- **Fixed cgroup v1 container path** — v1.0 only checked the host-level
  path; v2.0 also checks the containerised `/sys/fs/cgroup/memory/...` path.
- **Fixed `/proc/self/stat` parsing** — process names can contain spaces,
  which shifted the utime/stime field indices and produced silently wrong
  CPU time. v2.0 parses from the last `)` so spaces in the name are handled.
- **Graceful degradation** — energy, carbon, and memory are returned as
  `None` on non-container hosts (macOS, Windows, Linux without cgroups)
  instead of raising. The `memory_source` field explains which source was
  used.
- **Per-run error isolation** — a failing run is logged and skipped; the
  remaining runs still contribute to the aggregates. If all runs fail,
  `MeasurementError` is raised.
- **Validated `modp_weights`** — must be exactly 4 non-negative numbers.
  v1.0 silently accepted `[0.5, 0.5]` and crashed with `IndexError` on the
  third weight access.
- **Validated `runs >= 1`**; optional `warmup` kwarg.
- **Configurable MODP normalization caps** — v1.0 hardcoded `1.0 Wh`,
  `10.0 s`, `0.1 kg`; v2.0 reads them from `enhancement_config["modp_caps"]`
  and logs a warning when a metric exceeds its cap.
- **`copy.deepcopy` on returned sub-objects** — callers cannot corrupt
  `self.enhancement_config` by mutating the result dict.
- **Validated inputs** — `carbon_intensity`, `cpu_tdp_watts`,
  `human_feedback_score`.
- **Self-describing result** — new fields: `runs`, `runs_completed`,
  `warmup`, `errors`, `carbon_intensity`, `cpu_tdp_watts`, `region`,
  `enhancements_enabled`, `modp_weights`, `modp_caps`, `memory_source`,
  `latency_stdev`, `flexgen_energy_wh`, `units`.
- **`region` parameter** — resolves through the canonical grid-intensity
  table from `README.md §5.1`.
- **Async-`fn` detection** — `run_and_measure` raises a clear error rather
  than silently returning a coroutine.
- **Structured `logging`** — a missing cgroup is logged once, a MODP cap
  breach is logged, a fallback to `memory.current` is logged.

Units (documented in `result["units"]`)
---------------------------------------
- accuracy: ratio in [0, 1]
- latency, cpu_time: seconds
- latency_variance: seconds²
- latency_stdev: seconds
- energy: Wh
- carbon: kg CO₂e
- memory: MiB
- flexgen_energy_wh: Wh (new); flexgen_energy_joules preserved for
  backward compatibility
- carbon_intensity: kgCO₂/Wh
- cpu_tdp_watts: W

Backward compatibility
----------------------
The original result keys (`accuracy`, `latency`, `latency_variance`,
`cpu_time`, `energy`, `carbon`, `memory`, `modp_score`, `graph_metrics`,
`human_feedback_score`, `distillation_stats`, `flexgen_energy_joules`) are
all preserved. New keys are additive.

Run
---
    python docker_metrics_collector.py           # runs the built-in smoke test
    python docker_metrics_collector.py --quiet   # suppress info logging
"""
from __future__ import annotations

import argparse
import copy
import inspect
import logging
import math
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

__all__ = [
    "DockerMetricsCollector",
    "DockerMetricsError",
    "MeasurementError",
]


# =============================================================================
# Constants
# =============================================================================

_DEFAULT_RUNS = 5
_DEFAULT_CARBON_INTENSITY = 0.0004  # kgCO2/Wh (~400 gCO2/kWh, mid-range)
_DEFAULT_CPU_TDP_WATTS = 65.0
_DEFAULT_MODP_WEIGHTS: Tuple[float, float, float, float] = (0.4, 0.3, 0.2, 0.1)
_DEFAULT_MODP_CAPS: Dict[str, float] = {
    "energy_wh": 1.0,
    "latency_s": 10.0,
    "carbon_kg": 0.1,
}

# Canonical grid intensities from README.md §5.1, in gCO2e/kWh.
# Multiply by 1e-6 to get kgCO2/Wh.
_GRID_INTENSITY_G_PER_KWH: Dict[str, float] = {
    "US-CA": 220.0,
    "US-TX": 430.0,
    "US-NY": 220.0,
    "EU-FR": 70.0,
    "EU-DE": 380.0,
    "EU-NO": 30.0,
    "EU-PL": 700.0,
    "CN": 580.0,
    "IN": 630.0,
    "JP": 480.0,
    "AU": 550.0,
    "BR": 100.0,
    "GLOBAL": 475.0,
}


# =============================================================================
# Exceptions
# =============================================================================


class DockerMetricsError(RuntimeError):
    """Base exception for collector errors."""


class MeasurementError(DockerMetricsError):
    """Raised when every measured run fails."""


# =============================================================================
# Validation helpers
# =============================================================================


def _validate_positive_float(name: str, value: Any) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric; got {value!r}") from exc
    if not math.isfinite(f):
        raise ValueError(f"{name} must be finite; got {f!r}")
    if f <= 0:
        raise ValueError(f"{name} must be > 0; got {f!r}")
    return f


def _validate_modp_weights(weights: Any) -> List[float]:
    if not isinstance(weights, (list, tuple)):
        raise ValueError(
            "modp_weights must be a list or tuple of 4 numbers "
            "(accuracy, energy, latency, carbon)"
        )
    if len(weights) != 4:
        raise ValueError(
            f"modp_weights must have exactly 4 entries "
            f"(accuracy, energy, latency, carbon); got {len(weights)}"
        )
    out: List[float] = []
    for i, w in enumerate(weights):
        try:
            fw = float(w)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"modp_weights[{i}] must be numeric; got {w!r}"
            ) from exc
        if not math.isfinite(fw):
            raise ValueError(f"modp_weights[{i}] must be finite; got {fw!r}")
        if fw < 0:
            raise ValueError(
                f"modp_weights[{i}] must be non-negative; got {fw!r}"
            )
        out.append(fw)
    total = sum(out)
    if total <= 0:
        raise ValueError("modp_weights must have at least one positive entry")
    return [w / total for w in out]


def _resolve_region_intensity(region: str) -> float:
    """Return kgCO2/Wh for the given canonical region (see README.md §5.1)."""
    if region not in _GRID_INTENSITY_G_PER_KWH:
        raise ValueError(
            f"Unknown region {region!r}; choose from "
            f"{sorted(_GRID_INTENSITY_G_PER_KWH)}"
        )
    return _GRID_INTENSITY_G_PER_KWH[region] / 1_000_000.0


# =============================================================================
# Collector
# =============================================================================


class DockerMetricsCollector:
    """
    Collect green metrics from inside a Docker container (or a Linux host).

    See the module docstring for units, backward compatibility, and the
    list of fixes over v1.0.
    """

    def __init__(
        self,
        carbon_intensity: float = _DEFAULT_CARBON_INTENSITY,
        cpu_tdp_watts: float = _DEFAULT_CPU_TDP_WATTS,
        use_enhancements: bool = False,
        enhancement_config: Optional[Dict[str, Any]] = None,
        region: Optional[str] = None,
    ):
        """
        Parameters
        ----------
        carbon_intensity : float
            Carbon intensity of electricity in **kg CO2 per Wh**
            (default 0.0004, i.e. ~400 gCO2/kWh — a mid-range estimate).
            Ignored when `region` is provided.
        cpu_tdp_watts : float
            Approximate CPU TDP in watts used for energy estimation.
        use_enhancements : bool
            Enable optional advanced-module integration (MODP, LIMIT Graph,
            RLHF, distillation, FlexGen).
        enhancement_config : dict, optional
            May contain:
                modp_weights          — [accuracy, energy, latency, carbon]
                                        (4 non-negative numbers, normalized)
                modp_caps             — {energy_wh, latency_s, carbon_kg}
                graph_metrics         — {centrality, connectivity, ...}
                human_feedback_score  — float in [0, 1]
                distillation_stats    — dict (tracked as-is)
                flexgen_energy_joules — float (also reported as Wh)
        region : str, optional
            If provided, overrides `carbon_intensity` with the canonical
            value for this region (from `README.md §5.1`). Must be a key
            of the canonical grid-intensity table.
        """
        # --- Region vs carbon_intensity --------------------------------
        if region is not None:
            self.carbon_intensity = _resolve_region_intensity(region)
            self.region: Optional[str] = region
        else:
            self.carbon_intensity = _validate_positive_float(
                "carbon_intensity", carbon_intensity
            )
            self.region = None

        self.cpu_tdp_watts = _validate_positive_float(
            "cpu_tdp_watts", cpu_tdp_watts
        )
        self.use_enhancements = bool(use_enhancements)
        # Deep-copy so caller mutation of `enhancement_config` after
        # construction cannot affect us, and vice versa.
        self.enhancement_config: Dict[str, Any] = copy.deepcopy(
            enhancement_config or {}
        )

        # --- MODP weights ---------------------------------------------
        raw_weights = self.enhancement_config.get(
            "modp_weights", list(_DEFAULT_MODP_WEIGHTS)
        )
        self.modp_weights = _validate_modp_weights(raw_weights)

        # --- MODP caps -------------------------------------------------
        raw_caps = self.enhancement_config.get("modp_caps", {})
        if not isinstance(raw_caps, dict):
            raise ValueError("modp_caps must be a dict")
        merged_caps = dict(_DEFAULT_MODP_CAPS)
        merged_caps.update(raw_caps)
        self.modp_caps: Dict[str, float] = {
            "energy_wh": _validate_positive_float(
                "modp_caps.energy_wh", merged_caps["energy_wh"]
            ),
            "latency_s": _validate_positive_float(
                "modp_caps.latency_s", merged_caps["latency_s"]
            ),
            "carbon_kg": _validate_positive_float(
                "modp_caps.carbon_kg", merged_caps["carbon_kg"]
            ),
        }

        # --- Optional human_feedback_score validation -----------------
        hf = self.enhancement_config.get("human_feedback_score")
        if hf is not None:
            try:
                fhf = float(hf)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"human_feedback_score must be numeric; got {hf!r}"
                ) from exc
            if not (0.0 <= fhf <= 1.0):
                raise ValueError(
                    f"human_feedback_score must be in [0, 1]; got {fhf!r}"
                )
            self.enhancement_config["human_feedback_score"] = fhf

        logger.info(
            "DockerMetricsCollector initialized: region=%s "
            "carbon_intensity=%.8f kgCO2/Wh cpu_tdp=%.1f W enhancements=%s",
            self.region,
            self.carbon_intensity,
            self.cpu_tdp_watts,
            self.use_enhancements,
        )

    # ------------------------------------------------------------------
    # Configuration introspection
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Return the collector's configuration (for logging or audit)."""
        return {
            "carbon_intensity": self.carbon_intensity,
            "cpu_tdp_watts": self.cpu_tdp_watts,
            "use_enhancements": self.use_enhancements,
            "region": self.region,
            "modp_weights": list(self.modp_weights),
            "modp_caps": dict(self.modp_caps),
            "enhancement_config_keys": sorted(self.enhancement_config.keys()),
        }

    # ------------------------------------------------------------------
    # Core metric collectors
    # ------------------------------------------------------------------

    def _read_cgroup_memory_peak(
        self,
    ) -> Tuple[Optional[float], Optional[str]]:
        """
        Return ``(peak_memory_mb, source)``.

        ``source`` is one of ``"cgroup_v2_peak"``, ``"cgroup_v2_stat"``,
        ``"cgroup_v2_current"``, ``"cgroup_v1"``, or ``None``.

        Returns ``(None, None)`` when no cgroup memory file is available
        (macOS, Windows, Linux without cgroup). The caller decides whether
        that is fatal — this method never raises.

        cgroup v2 priority:
            1. ``memory.peak``         — exact peak (Linux 5.19+)
            2. ``memory.stat[peak]``   — peak from stat file (Linux 5.19+)
            3. ``memory.current``      — last-resort fallback; NOT peak

        cgroup v1 priority:
            1. ``/sys/fs/cgroup/memory/memory.max_usage_in_bytes``  (container)
            2. ``/sys/fs/cgroup/memory.max_usage_in_bytes``         (host)
        """
        candidates = [
            ("/sys/fs/cgroup/memory.peak", "cgroup_v2_peak", "single"),
            ("/sys/fs/cgroup/memory.stat", "cgroup_v2_stat", "stat_peak"),
            ("/sys/fs/cgroup/memory.current", "cgroup_v2_current", "single"),
            (
                "/sys/fs/cgroup/memory/memory.max_usage_in_bytes",
                "cgroup_v1",
                "single",
            ),
            ("/sys/fs/cgroup/memory.max_usage_in_bytes", "cgroup_v1", "single"),
        ]

        for path_str, source, kind in candidates:
            path = Path(path_str)
            if not path.is_file():
                continue
            try:
                raw = path.read_text(encoding="utf-8", errors="replace")
            except OSError as exc:
                logger.debug("cgroup read %s failed: %s", path_str, exc)
                continue

            if kind == "single":
                try:
                    value = int(raw.strip())
                except ValueError:
                    logger.debug(
                        "cgroup %s has non-integer content: %r",
                        path_str,
                        raw[:80],
                    )
                    continue
                if source == "cgroup_v2_current":
                    logger.warning(
                        "cgroup v2 memory.peak unavailable; falling back to "
                        "memory.current (%s). Peak memory will be "
                        "under-reported.",
                        path_str,
                    )
                return value / (1024 ** 2), source

            if kind == "stat_peak":
                peak_bytes: Optional[int] = None
                for line in raw.splitlines():
                    parts = line.split()
                    if len(parts) == 2 and parts[0] == "peak":
                        try:
                            peak_bytes = int(parts[1])
                        except ValueError:
                            peak_bytes = None
                        break
                if peak_bytes is None:
                    logger.debug(
                        "%s has no 'peak' key; trying next candidate",
                        path_str,
                    )
                    continue
                return peak_bytes / (1024 ** 2), source

        logger.warning(
            "No cgroup memory file available (non-Linux host or cgroups "
            "disabled); memory will be reported as None."
        )
        return None, None

    def _read_cpu_time(self) -> Optional[float]:
        """
        Return CPU time used by this process in seconds, or ``None`` if
        ``/proc/self/stat`` is unavailable.

        The ``/proc/self/stat`` format is:

            pid (comm) state ppid ... utime stime ...

        where ``comm`` is wrapped in parentheses and may contain spaces.
        We therefore locate the last ``)`` first, then parse from there.
        """
        stat_path = Path("/proc/self/stat")
        if not stat_path.is_file():
            logger.debug(
                "/proc/self/stat not available; CPU time will be None "
                "(non-Linux host)"
            )
            return None

        try:
            content = stat_path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            logger.debug("Reading /proc/self/stat failed: %s", exc)
            return None

        close_paren = content.rfind(")")
        if close_paren < 0 or close_paren + 2 > len(content):
            logger.debug(
                "Malformed /proc/self/stat (no closing paren): %r",
                content[:80],
            )
            return None

        # After ")" and one space, fields start at 1-indexed position 3.
        # Original 1-indexed field 14 (utime) → new 0-indexed 11
        # Original 1-indexed field 15 (stime) → new 0-indexed 12
        try:
            fields = content[close_paren + 2:].split()
            utime = float(fields[11])
            stime = float(fields[12])
        except (IndexError, ValueError) as exc:
            logger.debug("Parsing /proc/self/stat failed: %s", exc)
            return None

        try:
            ticks_per_second = os.sysconf("SC_CLK_TCK")
        except (AttributeError, ValueError, OSError) as exc:
            logger.debug("os.sysconf('SC_CLK_TCK') unavailable: %s", exc)
            return None

        if ticks_per_second <= 0:
            logger.debug(
                "SC_CLK_TCK returned invalid value: %r", ticks_per_second
            )
            return None

        return (utime + stime) / ticks_per_second

    # ------------------------------------------------------------------
    # Enhanced helpers
    # ------------------------------------------------------------------

    def _compute_modp_score(
        self,
        accuracy: float,
        energy_wh: Optional[float],
        latency_s: Optional[float],
        carbon_kg: Optional[float],
    ) -> Optional[float]:
        """
        Compute a multi-objective composite score (MODP) in [0, 1].

        Objectives are normalized to [0, 1] with higher = better:

            acc_norm     = accuracy                          (clamped to [0,1])
            energy_norm  = 1 − energy_wh  / modp_caps.energy_wh
            latency_norm = 1 − latency_s  / modp_caps.latency_s
            carbon_norm  = 1 − carbon_kg  / modp_caps.carbon_kg

        Each term is clamped to [0, 1]. If a raw value exceeds its cap, the
        corresponding normalized term becomes 0 and a warning is logged.

        Returns ``None`` if any required input is ``None`` (e.g., on a
        non-container host where energy and carbon cannot be measured).
        """
        if energy_wh is None or latency_s is None or carbon_kg is None:
            logger.warning(
                "Cannot compute MODP: energy=%r latency=%r carbon=%r",
                energy_wh,
                latency_s,
                carbon_kg,
            )
            return None

        caps = self.modp_caps
        if energy_wh > caps["energy_wh"]:
            logger.warning(
                "energy %.6f Wh exceeds MODP cap %.6f Wh; clamped to 0",
                energy_wh,
                caps["energy_wh"],
            )
        if latency_s > caps["latency_s"]:
            logger.warning(
                "latency %.6f s exceeds MODP cap %.6f s; clamped to 0",
                latency_s,
                caps["latency_s"],
            )
        if carbon_kg > caps["carbon_kg"]:
            logger.warning(
                "carbon %.6f kg exceeds MODP cap %.6f kg; clamped to 0",
                carbon_kg,
                caps["carbon_kg"],
            )

        acc_norm = max(0.0, min(float(accuracy), 1.0))
        energy_norm = 1.0 - min(energy_wh / caps["energy_wh"], 1.0)
        latency_norm = 1.0 - min(latency_s / caps["latency_s"], 1.0)
        carbon_norm = 1.0 - min(carbon_kg / caps["carbon_kg"], 1.0)

        w = self.modp_weights
        score = (
            w[0] * acc_norm
            + w[1] * energy_norm
            + w[2] * latency_norm
            + w[3] * carbon_norm
        )
        return float(max(0.0, min(score, 1.0)))

    def _get_graph_metrics(self) -> Dict[str, float]:
        """
        Return LIMIT Graph metrics (deep-copied so callers cannot mutate
        ``self.enhancement_config`` by mutating the result).
        """
        raw = self.enhancement_config.get(
            "graph_metrics",
            {"centrality": 0.5, "connectivity": 0.5},
        )
        if not isinstance(raw, dict):
            logger.warning(
                "graph_metrics in enhancement_config is not a dict (%r); "
                "using defaults",
                type(raw).__name__,
            )
            return {"centrality": 0.5, "connectivity": 0.5}
        return copy.deepcopy(raw)

    def _get_human_feedback_score(self) -> Optional[float]:
        """Retrieve the RLHF human feedback score, if present and valid."""
        hf = self.enhancement_config.get("human_feedback_score")
        if hf is None:
            return None
        try:
            fhf = float(hf)
        except (TypeError, ValueError):
            logger.warning(
                "human_feedback_score in enhancement_config is not numeric; "
                "ignoring"
            )
            return None
        if not (0.0 <= fhf <= 1.0):
            logger.warning(
                "human_feedback_score %.3f out of [0, 1]; ignoring", fhf
            )
            return None
        return fhf

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_and_measure(
        self,
        fn: Callable[[], float],
        runs: int = _DEFAULT_RUNS,
        *,
        warmup: int = 0,
    ) -> Dict[str, Any]:
        """
        Execute ``fn`` ``runs`` times (plus ``warmup`` discarded iterations)
        and collect metrics.

        Parameters
        ----------
        fn : Callable[[], float]
            **Synchronous** callable returning an accuracy in ``[0, 1]``.
            Async callables must be wrapped with ``asyncio.run`` first.
        runs : int
            Number of measured runs. Must be ``>= 1``.
        warmup : int
            Discarded iterations before measurement (default 0). Useful
            for JIT warm-up and disk cache priming.

        Returns
        -------
        Dict[str, Any]
            Result dict. Fields that cannot be measured on this host
            (e.g., memory on macOS) are set to ``None`` rather than
            omitted, so the schema is stable. See the module docstring
            for the full list of keys and units.

        Raises
        ------
        TypeError
            If ``fn`` is not callable or is an async function.
        ValueError
            If ``runs < 1`` or ``warmup < 0``.
        MeasurementError
            If every measured run fails.
        """
        # --- Input validation -----------------------------------------
        if not callable(fn):
            raise TypeError(
                f"fn must be callable; got {type(fn).__name__}"
            )
        if inspect.iscoroutinefunction(fn):
            raise TypeError(
                "run_and_measure only supports sync callables; wrap your "
                "async fn with asyncio.run(...) first."
            )
        if runs < 1:
            raise ValueError(f"runs must be >= 1; got {runs!r}")
        if warmup < 0:
            raise ValueError(f"warmup must be >= 0; got {warmup!r}")

        fn_name = getattr(fn, "__name__", "<callable>")
        logger.info(
            "run_and_measure: fn=%s runs=%d warmup=%d", fn_name, runs, warmup
        )

        # --- Measurement loop -----------------------------------------
        errors: List[str] = []
        measured: List[Dict[str, Any]] = []

        total_iterations = warmup + runs
        for i in range(total_iterations):
            is_warmup = i < warmup
            try:
                start_cpu = self._read_cpu_time()
                start_time = time.perf_counter()
                acc = fn()
                end_time = time.perf_counter()
                end_cpu = self._read_cpu_time()
            except Exception as exc:
                if is_warmup:
                    logger.warning(
                        "Warmup iteration %d failed: %s", i, exc
                    )
                else:
                    n = i - warmup
                    logger.warning("Measured run %d failed: %s", n, exc)
                    errors.append(
                        f"run {n}: {type(exc).__name__}: {exc}"
                    )
                continue

            if is_warmup:
                continue

            if not isinstance(acc, (int, float)):
                n = i - warmup
                msg = (
                    f"run {n}: fn() returned {type(acc).__name__}, "
                    f"not a number"
                )
                logger.warning(msg)
                errors.append(msg)
                continue

            cpu_delta: Optional[float]
            if start_cpu is None or end_cpu is None:
                cpu_delta = None
            else:
                cpu_delta = end_cpu - start_cpu

            measured.append(
                {
                    "latency": end_time - start_time,
                    "cpu_time": cpu_delta,
                    "accuracy": float(acc),
                }
            )

        if not measured:
            raise MeasurementError(
                f"All {runs} measured run(s) failed. Errors: {errors}"
            )

        # --- Aggregates -----------------------------------------------
        latencies = [r["latency"] for r in measured]
        accuracies = [r["accuracy"] for r in measured]
        cpu_times = [
            r["cpu_time"] for r in measured if r["cpu_time"] is not None
        ]

        avg_latency = statistics.mean(latencies)
        latency_variance = statistics.pvariance(latencies)
        latency_stdev = math.sqrt(latency_variance)
        avg_accuracy = statistics.mean(accuracies)

        if cpu_times:
            avg_cpu_time: Optional[float] = statistics.mean(cpu_times)
            energy_wh: Optional[float] = (
                avg_cpu_time * self.cpu_tdp_watts
            ) / 3600.0
            carbon_kg: Optional[float] = energy_wh * self.carbon_intensity
        else:
            avg_cpu_time = None
            energy_wh = None
            carbon_kg = None
            logger.warning(
                "CPU time unavailable on this host; energy and carbon "
                "will be None"
            )

        peak_memory_mb, memory_source = self._read_cgroup_memory_peak()

        # --- Result dict ----------------------------------------------
        result: Dict[str, Any] = {
            # Core metrics — backward-compatible names.
            "accuracy": avg_accuracy,
            "latency": avg_latency,
            "latency_variance": latency_variance,
            "cpu_time": avg_cpu_time,
            "energy": energy_wh,
            "carbon": carbon_kg,
            "memory": peak_memory_mb,
            # New: distribution + metadata.
            "latency_stdev": latency_stdev,
            "memory_source": memory_source,
            "runs": runs,
            "runs_completed": len(measured),
            "warmup": warmup,
            "errors": errors,
            "carbon_intensity": self.carbon_intensity,
            "cpu_tdp_watts": self.cpu_tdp_watts,
            "region": self.region,
            "enhancements_enabled": self.use_enhancements,
            "units": {
                "accuracy": "ratio [0, 1]",
                "latency": "seconds",
                "latency_variance": "seconds^2",
                "latency_stdev": "seconds",
                "cpu_time": "seconds",
                "energy": "Wh",
                "carbon": "kg CO2e",
                "memory": "MiB",
                "carbon_intensity": "kgCO2/Wh",
                "cpu_tdp_watts": "W",
            },
        }

        # --- Enhanced fields ------------------------------------------
        if self.use_enhancements:
            result["modp_weights"] = list(self.modp_weights)
            result["modp_caps"] = dict(self.modp_caps)

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
                result["distillation_stats"] = copy.deepcopy(
                    self.enhancement_config["distillation_stats"]
                )

            if "flexgen_energy_joules" in self.enhancement_config:
                joules = self.enhancement_config["flexgen_energy_joules"]
                try:
                    fj = float(joules)
                except (TypeError, ValueError):
                    logger.warning(
                        "flexgen_energy_joules %r is not numeric; skipping",
                        joules,
                    )
                else:
                    result["flexgen_energy_joules"] = fj
                    result["flexgen_energy_wh"] = fj / 3600.0

        return result


# =============================================================================
# Smoke test
# =============================================================================


def _smoke_test() -> int:
    """
    Minimal smoke test that runs on any platform.

    On non-Linux hosts, energy/carbon/memory are ``None`` — the test
    verifies the *shape* rather than the values.
    """
    failures: List[str] = []

    def check(name: str, condition: bool, detail: str = "") -> None:
        if condition:
            print(f"[PASS] {name}")
        else:
            failures.append(name)
            print(f"[FAIL] {name} {detail}")

    def expect_raises(
        name: str, exc_type: type, thunk: Callable[[], Any]
    ) -> None:
        try:
            thunk()
        except exc_type:
            print(f"[PASS] {name}")
        except Exception as exc:
            failures.append(name)
            print(
                f"[FAIL] {name} — raised {type(exc).__name__}, "
                f"expected {exc_type.__name__}"
            )
        else:
            failures.append(name)
            print(f"[FAIL] {name} — did not raise")

    # --- 1. Basic measurement --------------------------------------
    collector = DockerMetricsCollector()
    result = collector.run_and_measure(lambda: 0.95, runs=3)
    check("accuracy", result["accuracy"] == 0.95)
    check("runs requested", result["runs"] == 3)
    check("runs completed", result["runs_completed"] == 3)
    check("latency present", result["latency"] >= 0)
    check("latency_stdev present", result["latency_stdev"] >= 0)
    check("units dict present", isinstance(result["units"], dict))
    check("memory_source present", "memory_source" in result)
    check("no errors", result["errors"] == [])

    # --- 2. Enhanced measurement -----------------------------------
    collector_e = DockerMetricsCollector(
        use_enhancements=True,
        enhancement_config={
            "human_feedback_score": 0.8,
            "graph_metrics": {"centrality": 0.9, "connectivity": 0.7},
            "modp_weights": [0.4, 0.3, 0.2, 0.1],
            "distillation_stats": {"updates": 5},
        },
    )
    result_e = collector_e.run_and_measure(lambda: 0.9, runs=3)
    check(
        "enhancements_enabled",
        result_e["enhancements_enabled"] is True,
    )
    check("graph_metrics present", "graph_metrics" in result_e)
    check(
        "graph_metrics value",
        result_e["graph_metrics"]["centrality"] == 0.9,
    )
    check(
        "human_feedback_score", result_e["human_feedback_score"] == 0.8
    )
    check(
        "modp_weights echoed",
        result_e["modp_weights"] == [0.4, 0.3, 0.2, 0.1],
    )
    check(
        "distillation_stats present",
        result_e["distillation_stats"] == {"updates": 5},
    )
    check(
        "modp_score type",
        result_e["modp_score"] is None
        or isinstance(result_e["modp_score"], float),
    )

    # --- 3. Config aliasing test -----------------------------------
    # Mutating the returned graph_metrics must NOT affect the collector.
    result_e["graph_metrics"]["centrality"] = 0.0
    result_e2 = collector_e.run_and_measure(lambda: 0.9, runs=1)
    check(
        "config not mutated by result",
        result_e2["graph_metrics"]["centrality"] == 0.9,
    )

    # --- 4. Region resolution --------------------------------------
    collector_r = DockerMetricsCollector(region="US-CA")
    check(
        "region carbon_intensity",
        abs(collector_r.carbon_intensity - 0.00022) < 1e-12,
        f"got {collector_r.carbon_intensity}",
    )
    check("region echoed", collector_r.region == "US-CA")

    # --- 5. Invalid inputs are rejected ----------------------------
    expect_raises(
        "negative carbon_intensity",
        ValueError,
        lambda: DockerMetricsCollector(carbon_intensity=-1.0),
    )
    expect_raises(
        "zero cpu_tdp_watts",
        ValueError,
        lambda: DockerMetricsCollector(cpu_tdp_watts=0.0),
    )
    expect_raises(
        "bad modp_weights length",
        ValueError,
        lambda: DockerMetricsCollector(
            use_enhancements=True,
            enhancement_config={"modp_weights": [0.5, 0.5]},
        ),
    )
    expect_raises(
        "negative modp_weight",
        ValueError,
        lambda: DockerMetricsCollector(
            use_enhancements=True,
            enhancement_config={"modp_weights": [0.5, -0.5, 0.5, 0.5]},
        ),
    )
    expect_raises(
        "unknown region",
        ValueError,
        lambda: DockerMetricsCollector(region="XX"),
    )
    expect_raises(
        "runs < 1",
        ValueError,
        lambda: collector.run_and_measure(lambda: 0.9, runs=0),
    )
    expect_raises(
        "warmup < 0",
        ValueError,
        lambda: collector.run_and_measure(lambda: 0.9, runs=1, warmup=-1),
    )

    async def _async_fn() -> float:
        return 0.9

    expect_raises(
        "async fn rejected",
        TypeError,
        lambda: collector.run_and_measure(_async_fn, runs=1),
    )
    expect_raises(
        "non-callable rejected",
        TypeError,
        lambda: collector.run_and_measure("not a callable"),  # type: ignore[arg-type]
    )

    # --- 6. Partial failure is isolated ----------------------------
    calls = {"n": 0}

    def sometimes_fails() -> float:
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated failure")
        return 0.9

    partial = collector.run_and_measure(sometimes_fails, runs=3)
    check("partial runs requested", partial["runs"] == 3)
    check("partial runs completed", partial["runs_completed"] == 2)
    check("partial errors recorded", len(partial["errors"]) == 1)

    # --- 7. All-failure case raises --------------------------------
    def always_fails() -> float:
        raise RuntimeError("boom")

    expect_raises(
        "all runs fail",
        MeasurementError,
        lambda: collector.run_and_measure(always_fails, runs=2),
    )

    # --- 8. Warmup is honored -------------------------------------
    calls_wu = {"n": 0}

    def count_calls() -> float:
        calls_wu["n"] += 1
        return 0.9

    wu_result = collector.run_and_measure(count_calls, runs=2, warmup=3)
    check("warmup + runs total calls", calls_wu["n"] == 5)
    check("warmup runs completed", wu_result["runs_completed"] == 2)
    check("warmup echoed", wu_result["warmup"] == 3)

    # --- 9. MODP caps configurable ---------------------------------
    tight = DockerMetricsCollector(
        use_enhancements=True,
        enhancement_config={
            "modp_weights": [0.25, 0.25, 0.25, 0.25],
            "modp_caps": {
                "energy_wh": 1e-9,
                "latency_s": 1e-9,
                "carbon_kg": 1e-9,
            },
        },
    )
    tight_result = tight.run_and_measure(lambda: 0.9, runs=1)
    # With tiny caps, energy/latency/carbon norms clamp to 0 → score
    # dominated by accuracy contribution only (0.25 * 0.9 = 0.225).
    if tight_result["energy"] is not None:
        check(
            "MODP caps respected",
            tight_result["modp_score"] is not None
            and tight_result["modp_score"] <= 0.25 + 1e-6,
            f"got {tight_result['modp_score']}",
        )
    else:
        check(
            "MODP caps respected",
            tight_result["modp_score"] is None,
            "energy was None, so MODP should be None",
        )

    # --- Summary --------------------------------------------------
    print()
    if failures:
        print(f"{len(failures)} check(s) failed: {failures}")
        return 1
    print("All checks passed")
    return 0


# =============================================================================
# CLI
# =============================================================================


def _configure_logging(quiet: bool) -> None:
    level = logging.ERROR if quiet else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    )


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="docker_metrics_collector",
        description=(
            "Docker-aware green metrics collector. Running this file with "
            "no arguments executes the built-in smoke test."
        ),
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress info logging; only errors are printed.",
    )
    args = parser.parse_args(argv)

    _configure_logging(args.quiet)
    return _smoke_test()


if __name__ == "__main__":
    sys.exit(main())
