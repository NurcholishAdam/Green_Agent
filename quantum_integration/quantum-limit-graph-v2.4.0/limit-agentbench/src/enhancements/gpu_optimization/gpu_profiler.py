#!/usr/bin/env python3
"""
Enhanced Real GPU profiler using NVML (pynvml).
=================================================
FIXES OVER v2.0:
- `get_gpu_metrics()` refuses to run inside a live event loop.
- Energy-tracking dictionaries keyed consistently by `gpu_index`.
- `device_index` override no longer mutates `self.handles`.
- `update_node_descriptor()` skips writing dummy values.
- Per-field NVML failure isolation; `nvmlDeviceGetName` bytes decoded.
- Energy deltas use `time.monotonic()`; first sample bootstrapped correctly.
- `message_queue.publish` is fire-and-forget; `drain_publishes()` added.
- `shutdown()` sets a flag, cancels the monitor task, then NVML teardown.
  `aclose()` is the async-safe shutdown path used by `__aexit__`.
- Bare `except:` replaced with `except Exception:`.
- `interval_sec`, `carbon_intensity`, `history_size` validated.
- Optional Prometheus metrics.
- Object-style `node.metadata` supported.
- NVML calls run in a worker thread via `asyncio.to_thread`.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

# ---------- Prometheus (optional) ----------
try:
    from prometheus_client import Counter as _PromCounter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- NVML (optional) ----------
try:
    import pynvml  # type: ignore
    NVML_AVAILABLE = True
except ImportError:  # pragma: no cover
    pynvml = None  # type: ignore
    NVML_AVAILABLE = False

# ---------- Relative imports with fallbacks ----------
try:
    from ..async_message_queue import AsyncMessageQueue  # type: ignore
except ImportError:  # pragma: no cover
    AsyncMessageQueue = Any  # type: ignore

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:  # pragma: no cover
    FeedbackEvent = Any  # type: ignore

try:
    from ..schemas.node_descriptor import NodeDescriptor  # type: ignore
except ImportError:  # pragma: no cover
    NodeDescriptor = Any  # type: ignore


# ---------- Preserved module-level logger ----------
logger = logging.getLogger(__name__)


# ---------- Prometheus metrics (module scope: single registration) ----------
if PROMETHEUS_AVAILABLE:
    _M_SAMPLES = _PromCounter(
        "gpu_profiler_samples_total",
        "GPU profiler samples",
        ["is_dummy"],
    )
    _M_SAMPLE_SECONDS = Histogram(
        "gpu_profiler_sample_seconds",
        "Time to collect one sample",
    )
    _M_POWER = Gauge(
        "gpu_profiler_power_watts",
        "GPU power draw",
        ["gpu_index"],
    )
    _M_TEMP = Gauge(
        "gpu_profiler_temperature_c",
        "GPU temperature",
        ["gpu_index"],
    )
    _M_MEM_USED = Gauge(
        "gpu_profiler_memory_used_mb",
        "GPU memory used",
        ["gpu_index"],
    )
    _M_ENERGY = Histogram(
        "gpu_profiler_energy_joules",
        "Energy accumulated per sample interval",
    )
    _M_CARBON = Histogram(
        "gpu_profiler_carbon_g",
        "Carbon accumulated per sample interval",
    )
    _M_PUBLISH_FAILURES = _PromCounter(
        "gpu_profiler_publish_failures_total",
        "Failed FeedbackEvent publishes",
    )
    _M_NVML_ERRORS = _PromCounter(
        "gpu_profiler_nvml_errors_total",
        "NVML call failures",
        ["field"],
    )
else:  # pragma: no cover
    _M_SAMPLES = _M_SAMPLE_SECONDS = _M_POWER = _M_TEMP = _M_MEM_USED = None
    _M_ENERGY = _M_CARBON = _M_PUBLISH_FAILURES = _M_NVML_ERRORS = None


# ---------- Helpers ----------
def _as_float(
    value: Any,
    default: float,
    name: Optional[str] = None,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        logger.warning("Invalid %s: %r; using %s", name or "value", value, default)
        return default
    if not math.isfinite(v):
        logger.warning("Non-finite %s: %s; using %s", name or "value", v, default)
        return default
    if min_val is not None and v < min_val:
        return min_val
    if max_val is not None and v > max_val:
        return max_val
    return v


def _as_int(
    value: Any,
    default: int,
    name: Optional[str] = None,
    min_val: Optional[int] = None,
) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        logger.warning("Invalid %s: %r; using %s", name or "value", value, default)
        return default
    if min_val is not None and v < min_val:
        return min_val
    return v


def _meta_set(node: Any, key: str, value: Any) -> bool:
    """Set a key on node.metadata, supporting dict and object metadata."""
    md = getattr(node, "metadata", None)
    if md is None:
        try:
            node.metadata = {}
        except Exception:
            return False
        md = node.metadata
    if isinstance(md, dict):
        md[key] = value
        return True
    try:
        setattr(md, key, value)
        return True
    except Exception:
        return False


# ==============================================================================
# GPUProfiler
# ==============================================================================
class GPUProfiler:
    """
    GPU profiler that provides real-time metrics for NVIDIA GPUs.
    Falls back to dummy data if NVML is not available.
    """

    # Bounds
    MIN_INTERVAL_SEC: float = 0.05
    DEFAULT_INTERVAL_SEC: float = 5.0

    def __init__(
        self,
        device_index: Optional[int] = None,
        carbon_intensity_g_per_kwh: float = 400.0,
        message_queue: Optional[Any] = None,
        history_size: int = 100,
        *,
        interval_sec: float = DEFAULT_INTERVAL_SEC,
        publish_events: bool = True,
        enable_prometheus: bool = True,
        decode_gpu_name: bool = True,
    ):
        """
        Args:
            device_index: Specific GPU index to monitor. If None, monitor all.
            carbon_intensity_g_per_kwh: Carbon intensity for energy-to-carbon.
            message_queue: Optional queue for publishing metrics.
            history_size: Number of recent metric snapshots to keep.
            interval_sec: Default interval for `start_monitoring()`.
            publish_events: If False, `_monitor_loop` never publishes.
            enable_prometheus: If True and prometheus_client is installed,
                record module-level metrics.
            decode_gpu_name: If True, decode bytes GPU names as UTF-8.
        """
        self.device_index = device_index
        self.carbon_intensity = _as_float(
            carbon_intensity_g_per_kwh, 400.0,
            name="carbon_intensity_g_per_kwh", min_val=0.0,
        )
        self.message_queue = message_queue
        self._history_max = _as_int(history_size, 100, "history_size", min_val=1)
        self.history: Deque[Dict[str, Any]] = deque(maxlen=self._history_max)

        self._interval_sec = _as_float(
            interval_sec, self.DEFAULT_INTERVAL_SEC,
            name="interval_sec", min_val=self.MIN_INTERVAL_SEC,
        )
        self._publish_events = bool(publish_events)
        self._enable_prometheus = bool(enable_prometheus) and PROMETHEUS_AVAILABLE
        self._decode_gpu_name = bool(decode_gpu_name)

        # State
        self.nvml_initialized = False
        self.handles: List[Any] = []
        # Physical GPU indices corresponding to entries in `self.handles`.
        self._handle_indices: List[int] = []
        self._monitor_task: Optional[asyncio.Task] = None
        self._shutting_down = False

        # Locks
        self._lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()

        # Per-GPU energy tracking, keyed by gpu_index.
        self._last_mono: Dict[int, float] = {}
        self._last_wall: Dict[int, float] = {}
        self._last_power_watts: Dict[int, float] = {}

        # Pending FeedbackEvent publishes.
        self._pending_publish_tasks: set = set()

        # NVML init
        if NVML_AVAILABLE:
            try:
                pynvml.nvmlInit()
                self.nvml_initialized = True
                device_count = int(pynvml.nvmlDeviceGetCount())
                if self.device_index is not None:
                    if 0 <= self.device_index < device_count:
                        self.handles = [
                            pynvml.nvmlDeviceGetHandleByIndex(self.device_index)
                        ]
                        self._handle_indices = [self.device_index]
                    else:
                        logger.error(
                            "GPU index %s out of range (count=%s)",
                            self.device_index, device_count,
                        )
                else:
                    self.handles = [
                        pynvml.nvmlDeviceGetHandleByIndex(i)
                        for i in range(device_count)
                    ]
                    self._handle_indices = list(range(device_count))
                logger.info(
                    "NVML initialized, monitoring %d GPU(s)", len(self.handles),
                )
            except Exception as exc:
                logger.warning("NVML init failed: %s", exc)
                self.nvml_initialized = False
                self.handles = []
                self._handle_indices = []
        else:
            logger.info("NVML not available; using dummy GPU data.")

    # ------------------------------------------------------------------
    # NVML helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _safe_call(fn: Any, *args: Any, default: Any = None) -> Tuple[Any, Optional[str]]:
        """Call an NVML function, returning (value, error_message)."""
        try:
            return fn(*args), None
        except Exception as exc:
            return default, str(exc)

    def _collect_metrics_for_handle(
        self, gpu_index: int, handle: Any,
    ) -> Dict[str, Any]:
        """Collect metrics for a single GPU. Never raises."""
        errors: List[str] = []

        def _call(name: str, fn: Any, *args: Any, default: Any = None) -> Any:
            val, err = self._safe_call(fn, *args, default=default)
            if err is not None:
                errors.append(f"{name}: {err}")
                if self._enable_prometheus and _M_NVML_ERRORS is not None:
                    try:
                        _M_NVML_ERRORS.labels(field=name).inc()
                    except Exception:
                        pass
            return val

        mem_info = _call("memory", pynvml.nvmlDeviceGetMemoryInfo, handle, default=None)
        util = _call("utilization", pynvml.nvmlDeviceGetUtilizationRates, handle, default=None)
        power_mw = _call("power", pynvml.nvmlDeviceGetPowerUsage, handle, default=None)
        temp = _call(
            "temperature",
            pynvml.nvmlDeviceGetTemperature,
            handle,
            pynvml.NVML_TEMPERATURE_GPU,
            default=None,
        )
        name = _call("name", pynvml.nvmlDeviceGetName, handle, default="unknown_gpu")
        if isinstance(name, bytes):
            name = name.decode("utf-8", errors="replace") if self._decode_gpu_name else str(name)

        max_power_w = 0.0
        enforced = _call(
            "enforced_power_limit",
            pynvml.nvmlDeviceGetEnforcedPowerLimit,
            handle,
            default=None,
        )
        if enforced is not None:
            try:
                max_power_w = float(enforced) / 1000.0
            except (TypeError, ValueError):
                max_power_w = 0.0

        mem_clock = _call(
            "mem_clock",
            pynvml.nvmlDeviceGetClockInfo,
            handle,
            pynvml.NVML_CLOCK_MEM,
            default=None,
        )
        sm_clock = _call(
            "sm_clock",
            pynvml.nvmlDeviceGetClockInfo,
            handle,
            pynvml.NVML_CLOCK_SM,
            default=None,
        )

        metrics: Dict[str, Any] = {
            "gpu_name": name if name is not None else "unknown_gpu",
            "gpu_index": int(gpu_index),
            "gpu_memory_total_mb": (
                int(mem_info.total) // (1024 * 1024) if mem_info is not None else 0
            ),
            "gpu_memory_free_mb": (
                int(mem_info.free) // (1024 * 1024) if mem_info is not None else 0
            ),
            "gpu_memory_used_mb": (
                int(mem_info.used) // (1024 * 1024) if mem_info is not None else 0
            ),
            "gpu_utilization_pct": (
                float(util.gpu) if util is not None else 0.0
            ),
            "gpu_power_watts": (
                float(power_mw) / 1000.0 if power_mw is not None else None
            ),
            "gpu_temperature_c": (
                int(temp) if temp is not None else None
            ),
            "gpu_max_power_watts": max_power_w,
            "gpu_memory_clock_mhz": (
                int(mem_clock) if mem_clock is not None else None
            ),
            "gpu_sm_clock_mhz": (
                int(sm_clock) if sm_clock is not None else None
            ),
            "cuda_transfer_bandwidth_gbps": 12.0,  # placeholder
            "is_dummy": False,
        }
        if errors:
            metrics["_errors"] = errors
        return metrics

    def _collect_all_metrics(self) -> List[Dict[str, Any]]:
        """Collect metrics for all handles. Never raises."""
        if not self.nvml_initialized or not self.handles:
            return self._get_dummy_metrics()
        try:
            return [
                self._collect_metrics_for_handle(idx, handle)
                for idx, handle in zip(self._handle_indices, self.handles)
            ]
        except Exception as exc:
            logger.error("GPU metric collection failed: %s", exc)
            return self._get_dummy_metrics()

    def _get_dummy_metrics(self) -> List[Dict[str, Any]]:
        """Return dummy data for development."""
        idx = self.device_index if self.device_index is not None else 0
        return [{
            "gpu_name": "dummy_gpu",
            "gpu_index": idx,
            "gpu_memory_total_mb": 16384,
            "gpu_memory_free_mb": 12000,
            "gpu_memory_used_mb": 4384,
            "gpu_utilization_pct": 45.0,
            "gpu_power_watts": 65.0,
            "gpu_temperature_c": 55,
            "gpu_max_power_watts": 250.0,
            "gpu_memory_clock_mhz": 6000,
            "gpu_sm_clock_mhz": 1500,
            "cuda_transfer_bandwidth_gbps": 12.0,
            "is_dummy": True,
        }]

    # ------------------------------------------------------------------
    # Sync / async metrics API
    # ------------------------------------------------------------------
    def _collect_metrics_for_index(self, device_index: int) -> Dict[str, Any]:
        """Synchronous collection for a specific GPU index."""
        if not self.nvml_initialized:
            return self._get_dummy_metrics()[0]
        try:
            handle = pynvml.nvmlDeviceGetHandleByIndex(int(device_index))
        except Exception as exc:
            logger.warning("Failed to get handle for GPU %s: %s", device_index, exc)
            return {}
        return self._collect_metrics_for_handle(int(device_index), handle)

    def get_gpu_metrics(self, device_index: Optional[int] = None) -> Dict[str, Any]:
        """
        Synchronous metrics for a specific GPU (or the first monitored GPU).

        Cannot be called from a running event loop. Use
        `await get_all_gpu_metrics()` instead.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RuntimeError(
                "get_gpu_metrics() cannot be called from a running event loop. "
                "Use `await get_all_gpu_metrics()` instead."
            )

        if device_index is not None and device_index != self.device_index:
            return self._collect_metrics_for_index(device_index)

        metrics = self._collect_all_metrics()
        return metrics[0] if metrics else {}

    async def get_all_gpu_metrics(self) -> List[Dict[str, Any]]:
        """Asynchronously get metrics for all monitored GPUs."""
        async with self._lock:
            return await asyncio.to_thread(self._collect_all_metrics)

    # ------------------------------------------------------------------
    # Node descriptor
    # ------------------------------------------------------------------
    async def update_node_descriptor(self, node: NodeDescriptor) -> NodeDescriptor:
        """Populate node metadata with GPU information from profiler.

        Does nothing if the profiler is returning dummy data, so that real
        node metadata is not silently overwritten with fabricated values.
        """
        if node is None:
            return node
        metrics = await self.get_all_gpu_metrics()
        if not metrics:
            return node
        gpu = metrics[0]
        if gpu.get("is_dummy", False):
            logger.info(
                "Skipping node descriptor update: profiler is returning dummy data."
            )
            return node

        _meta_set(node, "gpu_name", gpu.get("gpu_name"))
        _meta_set(node, "gpu_memory_gb", gpu.get("gpu_memory_total_mb", 0) / 1024.0)
        _meta_set(node, "gpu_max_power_w", gpu.get("gpu_max_power_watts", 250.0))
        _meta_set(
            node,
            "gpu_cpu_bandwidth_gbps",
            gpu.get("cuda_transfer_bandwidth_gbps", 12.0),
        )
        return node

    # ------------------------------------------------------------------
    # Monitoring
    # ------------------------------------------------------------------
    async def start_monitoring(self, interval_sec: float = DEFAULT_INTERVAL_SEC) -> None:
        """Start continuous monitoring loop, publishing metrics to message queue."""
        if self._monitor_task is not None and not self._monitor_task.done():
            logger.warning("Monitoring already active")
            return
        interval_sec = _as_float(
            interval_sec, self._interval_sec,
            name="interval_sec", min_val=self.MIN_INTERVAL_SEC,
        )
        self._shutting_down = False
        self._monitor_task = asyncio.create_task(self._monitor_loop(interval_sec))
        logger.info("GPU monitoring started (interval=%ss)", interval_sec)

    async def stop_monitoring(self) -> None:
        """Stop the monitoring loop and await its cancellation."""
        task = self._monitor_task
        if task is None:
            return
        self._shutting_down = True
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning("Monitor loop raised on shutdown: %s", exc)
        self._monitor_task = None
        logger.info("GPU monitoring stopped")

    async def _monitor_loop(self, interval_sec: float) -> None:
        while not self._shutting_down:
            try:
                await self._sample_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Monitoring loop error: %s", exc)
            try:
                await asyncio.sleep(interval_sec)
            except asyncio.CancelledError:
                raise

    async def _sample_once(self) -> None:
        start = time.monotonic()
        metrics_list = await self.get_all_gpu_metrics()

        for metrics in metrics_list:
            idx = int(metrics.get("gpu_index", 0))
            power = metrics.get("gpu_power_watts")

            now_mono = time.monotonic()
            now_wall = time.time()

            last_mono = self._last_mono.get(idx)
            last_power = self._last_power_watts.get(idx)

            if (
                last_mono is not None
                and last_power is not None
                and power is not None
            ):
                dt = now_mono - last_mono
                if dt < 0:
                    # Monotonic time should never go backwards; guard anyway.
                    dt = 0.0
                avg_power = (last_power + float(power)) / 2.0
                energy_j = avg_power * dt
            else:
                energy_j = 0.0

            carbon_g = (energy_j / 3.6e6) * self.carbon_intensity

            # Update tracking
            self._last_mono[idx] = now_mono
            self._last_wall[idx] = now_wall
            if power is not None:
                self._last_power_watts[idx] = float(power)

            metrics["energy_joules"] = energy_j
            metrics["carbon_g"] = carbon_g
            metrics["timestamp"] = now_wall

            # Append to history
            async with self._history_lock:
                self.history.append(dict(metrics))

            # Prometheus
            if self._enable_prometheus:
                try:
                    _M_SAMPLES.labels(is_dummy=str(metrics.get("is_dummy", False))).inc()
                    _M_SAMPLE_SECONDS.observe(time.monotonic() - start)
                    if power is not None:
                        _M_POWER.labels(gpu_index=str(idx)).set(float(power))
                    if metrics.get("gpu_temperature_c") is not None:
                        _M_TEMP.labels(gpu_index=str(idx)).set(
                            float(metrics["gpu_temperature_c"])
                        )
                    if metrics.get("gpu_memory_used_mb") is not None:
                        _M_MEM_USED.labels(gpu_index=str(idx)).set(
                            float(metrics["gpu_memory_used_mb"])
                        )
                    _M_ENERGY.observe(energy_j)
                    _M_CARBON.observe(carbon_g)
                except Exception:
                    pass

            # Publish (fire-and-forget)
            self._spawn_publish(metrics)

    # ------------------------------------------------------------------
    # FeedbackEvent publishing
    # ------------------------------------------------------------------
    def _spawn_publish(self, metrics: Dict[str, Any]) -> None:
        if not self._publish_events or self.message_queue is None or FeedbackEvent is None:
            return
        try:
            task = asyncio.create_task(self._publish(metrics))
        except RuntimeError:
            return
        self._pending_publish_tasks.add(task)
        task.add_done_callback(self._pending_publish_tasks.discard)

    async def _publish(self, metrics: Dict[str, Any]) -> None:
        try:
            event = FeedbackEvent(
                source="gpu_profiler",
                feedback_type="telemetry",
                task_id="gpu_monitor",
                context={
                    "gpu_index": metrics.get("gpu_index", 0),
                    "carbon_intensity": self.carbon_intensity,
                },
                action={
                    "selected_action": "monitor",
                    "selected_rank": 0,
                    "confidence_score": 1.0,
                },
                performance={
                    "quality_score": 1.0,
                    "latency_ms": 0.0,
                    "energy_joules": float(metrics.get("energy_joules", 0.0) or 0.0),
                    "carbon_g": float(metrics.get("carbon_g", 0.0) or 0.0),
                    "helium_cost": 0.0,
                    "duration_ms": 0.0,
                },
                adaptive_cost_value=0.0,
                tags=["gpu", "monitoring", "energy", "carbon"],
            )
            payload = (
                event.to_json()
                if hasattr(event, "to_json") and callable(event.to_json)
                else str(event)
            )
            await self.message_queue.publish("gpu_metrics", payload)
        except Exception as exc:
            logger.warning("Failed to publish GPU FeedbackEvent: %s", exc)
            if self._enable_prometheus and _M_PUBLISH_FAILURES is not None:
                try:
                    _M_PUBLISH_FAILURES.inc()
                except Exception:
                    pass

    async def drain_publishes(self) -> None:
        """Await any in-flight publish tasks."""
        if not self._pending_publish_tasks:
            return
        pending = list(self._pending_publish_tasks)
        await asyncio.gather(*pending, return_exceptions=True)
        self._pending_publish_tasks.clear()

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------
    def get_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Return recent metric history.

        `deque` mutation/iteration is atomic under CPython's GIL, so this
        is safe to call from any thread without acquiring `_history_lock`.
        """
        snapshot = list(self.history)
        if limit is None or limit <= 0:
            return snapshot
        return snapshot[-limit:]

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    def _nvml_shutdown(self) -> None:
        if not self.nvml_initialized:
            return
        if not NVML_AVAILABLE:
            self.nvml_initialized = False
            return
        try:
            pynvml.nvmlShutdown()
            logger.info("NVML shutdown complete")
        except Exception as exc:
            logger.error("NVML shutdown failed: %s", exc)
        finally:
            self.nvml_initialized = False
            self.handles = []
            self._handle_indices = []

    def shutdown(self) -> None:
        """
        Best-effort synchronous cleanup.

        Cancels the monitor task (if any) and tears down NVML. Because this
        is synchronous, it cannot await the task; prefer `async with
        GPUProfiler(...)` or `await profiler.aclose()` for an orderly
        shutdown.
        """
        task = self._monitor_task
        if task is not None and not task.done():
            self._shutting_down = True
            task.cancel()
            logger.info("Monitor task cancelled by shutdown()")
        self._monitor_task = None
        self._nvml_shutdown()

    async def aclose(self) -> None:
        """Async-safe shutdown: awaits the monitor task and pending publishes."""
        await self.stop_monitoring()
        await self.drain_publishes()
        self._nvml_shutdown()

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------
    async def __aenter__(self) -> "GPUProfiler":
        await self.start_monitoring(self._interval_sec)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()


# ==============================================================================
# Example usage
# ==============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def _demo() -> None:
        profiler = GPUProfiler(history_size=10, interval_sec=0.2)
        try:
            # Sync path works outside a loop... but we are inside one here,
            # so demonstrate the async path.
            metrics = await profiler.get_all_gpu_metrics()
            for m in metrics:
                print(f"GPU {m.get('gpu_index')}: "
                      f"{m.get('gpu_name')}, power={m.get('gpu_power_watts')} W, "
                      f"is_dummy={m.get('is_dummy')}")

            async with profiler:
                await asyncio.sleep(1.0)
            print("History size:", len(profiler.get_history()))
        finally:
            await profiler.aclose()

    asyncio.run(_demo())
