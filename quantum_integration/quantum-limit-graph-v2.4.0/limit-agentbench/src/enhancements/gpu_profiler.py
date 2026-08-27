"""
gpu_profiler.py

Enhanced real‑time hardware metrics collector with:
- GPU (NVML), CPU, memory, disk, network I/O.
- Cumulative energy tracking.
- Multi‑GPU support.
- Persistent SQLite history.
- Streaming callbacks.
- Direct integration with MODP, bio_inspired, moe_system, LIMIT Graph, RLHF, and Multi‑Teacher Policy Distillation.
"""

import time
import psutil
import threading
import sqlite3
import json
import os
import logging
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
from collections import deque

# NVML is optional; fail gracefully if not installed
try:
    import pynvml
    NVML_AVAILABLE = True
    pynvml.nvmlInit()
except (ImportError, pynvml.NVMLError):
    NVML_AVAILABLE = False

# ----------------------------------------------------------------------
# Optional imports for integration with other modules (stubs if missing)
# ----------------------------------------------------------------------
try:
    from enhancements.MODP import ParetoOptimizer
except ImportError:
    class ParetoOptimizer:
        def evaluate(self, metrics, weights):
            return sum(metrics.get(k, 0) * weights.get(k, 1) for k in metrics)

try:
    from enhancements.bio_inspired import FitnessEvaluator
except ImportError:
    class FitnessEvaluator:
        def evaluate(self, metrics, policy=None):
            return 0.0

try:
    from enhancements.moe_system import ContextEncoder
except ImportError:
    class ContextEncoder:
        def encode(self, metrics):
            return [metrics.get("gpu_utilization_pct", 0),
                    metrics.get("cpu_utilization_pct", 0),
                    metrics.get("gpu_memory_used_mb", 0) / 1000]

try:
    from enhancements.limit_graph import LimitGraph
except ImportError:
    class LimitGraph:
        def __init__(self, *args, **kwargs): pass
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass

try:
    from enhancements.rlhf import RLHFOptimizer
except ImportError:
    class RLHFOptimizer:
        def __init__(self, action_space, *args, **kwargs): self.actions = action_space
        def update(self, context, action, reward): pass
        def sample_action(self, context): return self.actions[0] if self.actions else None

try:
    from enhancements.multi_teacher_policy_distillation import MultiTeacherDistiller
except ImportError:
    class MultiTeacherDistiller:
        def __init__(self, teachers, *args, **kwargs): self.teachers = teachers
        def distill(self, context): return self.teachers[0](context) if self.teachers else None

# ----------------------------------------------------------------------
# Enhanced GPUProfiler
# ----------------------------------------------------------------------

@dataclass
class ProfilerConfig:
    """Configuration for the profiler."""
    sample_interval: float = 0.5
    enable_history: bool = True
    history_db_path: str = "gpu_metrics.db"
    max_history_days: int = 7
    enable_callbacks: bool = True
    callback_cooldown: float = 0.1  # minimum time between callbacks
    # New: enable additional modules
    enable_limit_graph: bool = True
    enable_rlhf: bool = True
    enable_distillation: bool = True


class GPUProfiler:
    """
    Enhanced profiler with:
    - Multi‑GPU support.
    - Cumulative energy tracking.
    - Network I/O.
    - Persistent history (SQLite).
    - Streaming callbacks.
    - Integration with MODP, bio_inspired, moe_system, LIMIT Graph, RLHF, and Multi‑Teacher Policy Distillation.
    """

    def __init__(
        self,
        config: Optional[ProfilerConfig] = None,
        modp_weights: Optional[Dict[str, float]] = None,
        bio_evaluator: Optional[Any] = None,
        moe_encoder: Optional[Any] = None,
        limit_graph: Optional[Any] = None,
        rlhf_optimizer: Optional[Any] = None,
        distiller: Optional[Any] = None,
    ):
        self.config = config or ProfilerConfig()
        self.modp_weights = modp_weights or {
            "energy_efficiency": 0.3,
            "carbon_efficiency": 0.3,
            "memory_efficiency": 0.2,
            "throughput": 0.2,
        }
        self.bio = bio_evaluator if bio_evaluator else FitnessEvaluator()
        self.moe = moe_encoder if moe_encoder else ContextEncoder()

        # New modules
        if self.config.enable_limit_graph:
            self.limit_graph = limit_graph if limit_graph else LimitGraph()
        else:
            self.limit_graph = None

        if self.config.enable_rlhf:
            # Action space can be profiles (e.g., power profiles)
            self.rlhf = rlhf_optimizer if rlhf_optimizer else RLHFOptimizer(action_space=["balanced", "performance", "power_save"])
        else:
            self.rlhf = None

        if self.config.enable_distillation:
            # Teachers: simple policy functions based on metrics
            self.distiller = distiller if distiller else self._create_default_distiller()
        else:
            self.distiller = None

        # Internal state
        self._running = False
        self._thread = None
        self._lock = threading.Lock()
        self._latest_metrics = {}
        self._callbacks = []          # list of (callback_fn, cooldown, last_call)
        self._disk_io_start = psutil.disk_io_counters()
        self._net_io_start = psutil.net_io_counters()
        self._last_disk_time = time.time()
        self._last_net_time = time.time()
        self._energy_cumulative_gpu_joules = 0.0
        self._energy_cumulative_cpu_joules = 0.0
        self._last_power_sample_time = time.time()

        # History
        self._conn = None
        if self.config.enable_history:
            self._init_history()

    def _create_default_distiller(self):
        """Create a default distiller with simple teachers."""
        def teacher_performance(ctx):
            return "performance" if ctx.get("gpu_utilization_pct", 0) > 0.7 else "balanced"

        def teacher_power(ctx):
            return "power_save" if ctx.get("gpu_power_watts", 0) > 300 else "balanced"

        def teacher_carbon(ctx):
            # Placeholder: assume high carbon if energy high
            return "power_save" if ctx.get("energy_gpu_joules", 0) > 1000 else "performance"

        return MultiTeacherDistiller([teacher_performance, teacher_power, teacher_carbon])

    # --------------------- History (SQLite) ---------------------
    def _init_history(self):
        """Initialize SQLite database for metrics history."""
        try:
            self._conn = sqlite3.connect(self.config.history_db_path, check_same_thread=False)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    timestamp REAL,
                    gpu_util REAL,
                    gpu_mem_used_mb REAL,
                    gpu_power_watts REAL,
                    cpu_util REAL,
                    cpu_mem_used_mb REAL,
                    disk_read_gbps REAL,
                    disk_write_gbps REAL,
                    net_recv_gbps REAL,
                    net_sent_gbps REAL,
                    energy_gpu_joules REAL,
                    energy_cpu_joules REAL,
                    PRIMARY KEY (timestamp)
                )
            """)
            self._conn.execute("PRAGMA journal_mode=WAL")
            # Clean old records
            self._clean_history()
        except Exception as e:
            logging.warning(f"Failed to initialize history: {e}")
            self._conn = None

    def _clean_history(self):
        """Remove records older than max_history_days."""
        if not self._conn:
            return
        cutoff = time.time() - self.config.max_history_days * 86400
        self._conn.execute("DELETE FROM metrics WHERE timestamp < ?", (cutoff,))
        self._conn.commit()

    def _store_metrics(self, metrics: Dict[str, Any]):
        """Store a snapshot in the database."""
        if not self._conn:
            return
        try:
            self._conn.execute("""
                INSERT INTO metrics (
                    timestamp, gpu_util, gpu_mem_used_mb, gpu_power_watts,
                    cpu_util, cpu_mem_used_mb, disk_read_gbps, disk_write_gbps,
                    net_recv_gbps, net_sent_gbps, energy_gpu_joules, energy_cpu_joules
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                metrics.get("timestamp", time.time()),
                metrics.get("gpu_utilization_pct", 0),
                metrics.get("gpu_memory_used_mb", 0),
                metrics.get("gpu_power_watts", 0),
                metrics.get("cpu_utilization_pct", 0),
                metrics.get("cpu_memory_used_mb", 0),
                metrics.get("disk_read_bandwidth_gbps", 0),
                metrics.get("disk_write_bandwidth_gbps", 0),
                metrics.get("net_recv_bandwidth_gbps", 0),
                metrics.get("net_sent_bandwidth_gbps", 0),
                metrics.get("energy_gpu_joules", 0),
                metrics.get("energy_cpu_joules", 0),
            ))
            self._conn.commit()
        except Exception as e:
            logging.warning(f"Failed to store metrics: {e}")

    # --------------------- Callback System ---------------------
    def register_callback(self, callback: Callable[[Dict[str, Any]], None],
                          cooldown: float = 0.1):
        """Register a function to be called when new metrics arrive."""
        self._callbacks.append((callback, cooldown, 0.0))

    def _trigger_callbacks(self, metrics: Dict[str, Any]):
        """Call registered callbacks, respecting cooldowns."""
        now = time.time()
        for i, (cb, cd, last) in enumerate(self._callbacks):
            if now - last >= cd:
                try:
                    cb(metrics)
                except Exception as e:
                    logging.error(f"Callback error: {e}")
                self._callbacks[i] = (cb, cd, now)

    # --------------------- Core Sampling ---------------------
    def _snapshot(self) -> Dict[str, Any]:
        """Collect a comprehensive metrics snapshot."""
        metrics = {}
        now = time.time()
        metrics["timestamp"] = now

        # ---- GPU Metrics (NVML) ----
        if NVML_AVAILABLE:
            try:
                device_count = pynvml.nvmlDeviceGetCount()
                metrics["gpu_count"] = device_count
                total_used = 0
                total_free = 0
                total_power = 0.0
                max_util = 0.0
                avg_temp = 0.0
                for i in range(device_count):
                    handle = pynvml.nvmlDeviceGetHandleByIndex(i)
                    mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                    util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                    power = pynvml.nvmlDeviceGetPowerUsage(handle) / 1000.0  # mW -> W
                    temp = pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU)
                    total_used += mem_info.used / 1024**2
                    total_free += mem_info.free / 1024**2
                    total_power += power
                    max_util = max(max_util, util.gpu / 100.0)
                    avg_temp += temp
                avg_temp /= device_count
                metrics["gpu_memory_total_mb"] = (total_used + total_free)
                metrics["gpu_memory_used_mb"] = total_used
                metrics["gpu_memory_free_mb"] = total_free
                metrics["gpu_utilization_pct"] = max_util
                metrics["gpu_power_watts"] = total_power
                metrics["gpu_temp_c"] = avg_temp
            except Exception as e:
                logging.warning(f"NVML snapshot error: {e}")
        else:
            metrics["gpu_available"] = False

        # ---- CPU & Memory ----
        vm = psutil.virtual_memory()
        metrics["cpu_memory_total_mb"] = vm.total / 1024**2
        metrics["cpu_memory_free_mb"] = vm.available / 1024**2
        metrics["cpu_memory_used_mb"] = vm.used / 1024**2
        metrics["cpu_utilization_pct"] = psutil.cpu_percent(interval=None) / 100.0

        # ---- Disk I/O ----
        disk_io = psutil.disk_io_counters()
        if self._disk_io_start and now - self._last_disk_time > 0.5:
            delta = now - self._last_disk_time
            read_bytes = disk_io.read_bytes - self._disk_io_start.read_bytes
            write_bytes = disk_io.write_bytes - self._disk_io_start.write_bytes
            metrics["disk_read_bandwidth_gbps"] = (read_bytes / delta) * 8 / 1e9
            metrics["disk_write_bandwidth_gbps"] = (write_bytes / delta) * 8 / 1e9
        self._disk_io_start = disk_io
        self._last_disk_time = now

        # ---- Network I/O ----
        net_io = psutil.net_io_counters()
        if self._net_io_start and now - self._last_net_time > 0.5:
            delta = now - self._last_net_time
            recv_bytes = net_io.bytes_recv - self._net_io_start.bytes_recv
            sent_bytes = net_io.bytes_sent - self._net_io_start.bytes_sent
            metrics["net_recv_bandwidth_gbps"] = (recv_bytes / delta) * 8 / 1e9
            metrics["net_sent_bandwidth_gbps"] = (sent_bytes / delta) * 8 / 1e9
        self._net_io_start = net_io
        self._last_net_time = now

        # ---- Cumulative Energy ----
        # GPU energy: approximate from power * time
        elapsed = now - self._last_power_sample_time
        if "gpu_power_watts" in metrics:
            gpu_energy = metrics["gpu_power_watts"] * elapsed
            self._energy_cumulative_gpu_joules += gpu_energy
        # CPU energy: estimate from TDP or use psutil's sensors if available
        cpu_tdp = 65  # placeholder, could be detected
        cpu_power = cpu_tdp * metrics["cpu_utilization_pct"]
        cpu_energy = cpu_power * elapsed
        self._energy_cumulative_cpu_joules += cpu_energy

        metrics["energy_gpu_joules"] = self._energy_cumulative_gpu_joules
        metrics["energy_cpu_joules"] = self._energy_cumulative_cpu_joules
        self._last_power_sample_time = now

        # ---- Derived metrics for MODP ----
        metrics["energy_efficiency"] = self._compute_energy_efficiency(metrics)
        metrics["carbon_efficiency"] = self._compute_carbon_efficiency(metrics)
        metrics["memory_efficiency"] = self._compute_memory_efficiency(metrics)
        metrics["throughput"] = metrics.get("tokens_per_sec", 0)  # injected from executor

        return metrics

    # --------------------- Derived Metric Helpers ---------------------
    def _compute_energy_efficiency(self, metrics: Dict[str, Any]) -> float:
        """Compute energy efficiency (tokens per joule)."""
        tokens = metrics.get("tokens_per_sec", 0)
        total_power = metrics.get("gpu_power_watts", 0) + 50  # estimate CPU
        if total_power > 0:
            return tokens / total_power
        return 0.0

    def _compute_carbon_efficiency(self, metrics: Dict[str, Any]) -> float:
        """Compute carbon efficiency (tokens per kg CO2)."""
        carbon_intensity = 200.0  # gCO2/kWh
        total_energy = metrics.get("energy_gpu_joules", 0) / 3600 / 1000  # kWh
        carbon_kg = total_energy * carbon_intensity / 1000
        tokens = metrics.get("tokens_per_sec", 0)
        if carbon_kg > 0:
            return tokens / carbon_kg
        return 0.0

    def _compute_memory_efficiency(self, metrics: Dict[str, Any]) -> float:
        """Compute memory efficiency (used / total)."""
        total = metrics.get("gpu_memory_total_mb", 1)
        used = metrics.get("gpu_memory_used_mb", 0)
        if total > 0:
            return used / total
        return 0.0

    # --------------------- Public Methods ---------------------
    def start(self):
        """Start background sampling."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop background sampling and close DB."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
        if self._conn:
            self._conn.close()

    def _sample_loop(self):
        while self._running:
            metrics = self._snapshot()
            with self._lock:
                self._latest_metrics = metrics
            if self.config.enable_history:
                self._store_metrics(metrics)
            if self.config.enable_callbacks:
                self._trigger_callbacks(metrics)
            time.sleep(self.config.sample_interval)

    def get_current_metrics(self) -> Dict[str, Any]:
        """Return the latest snapshot (or take one if not running)."""
        if self._running:
            with self._lock:
                return self._latest_metrics.copy()
        return self._snapshot()

    # --------------------- Integration Interfaces ---------------------
    def get_modp_utility(self, metrics: Optional[Dict[str, Any]] = None) -> float:
        """Return a scalar utility using MODP weights."""
        if metrics is None:
            metrics = self.get_current_metrics()
        objectives = {
            "energy_efficiency": metrics.get("energy_efficiency", 0),
            "carbon_efficiency": metrics.get("carbon_efficiency", 0),
            "memory_efficiency": metrics.get("memory_efficiency", 0),
            "throughput": metrics.get("throughput", 0),
        }
        return ParetoOptimizer().evaluate(objectives, self.modp_weights)

    def get_bio_fitness(self, policy: Dict[str, Any]) -> float:
        """Return a fitness score for a given policy using bio_inspired evaluator."""
        metrics = self.get_current_metrics()
        return self.bio.evaluate(metrics, policy)

    def get_moe_context(self) -> List[float]:
        """Return a context vector for the MoE router."""
        metrics = self.get_current_metrics()
        return self.moe.encode(metrics)

    # --------------------- New Integration Methods (LIMIT, RLHF, Distillation) ---------------------
    def get_policy_from_distillation(self, context: Optional[Dict] = None) -> str:
        """Use distillation to select a policy based on current context."""
        if not self.distiller:
            return "balanced"
        if context is None:
            context = self.get_current_metrics()
        return self.distiller.distill(context)

    def get_policy_from_rlhf(self, context: Optional[Dict] = None) -> str:
        """Sample a policy from RLHF optimizer."""
        if not self.rlhf:
            return "balanced"
        if context is None:
            context = self.get_current_metrics()
        return self.rlhf.sample_action(context)

    def get_limits(self, context: Optional[Dict] = None) -> Dict:
        """Return limits from the LIMIT Graph."""
        if not self.limit_graph:
            return {}
        if context is None:
            context = self.get_current_metrics()
        return self.limit_graph.get_limits(context)

    def update_feedback(self, context: Dict, action: str, reward: float):
        """Update RLHF and LIMIT Graph with feedback."""
        if self.rlhf:
            self.rlhf.update(context, action, reward)
        if self.limit_graph:
            self.limit_graph.update_from_feedback({
                'context': context,
                'action': action,
                'reward': reward,
            })

    # --------------------- Utility ---------------------
    def get_history(self, start_time: float = 0, end_time: float = None) -> List[Dict]:
        """Retrieve historical metrics from database."""
        if not self._conn:
            return []
        if end_time is None:
            end_time = time.time()
        cursor = self._conn.execute("""
            SELECT timestamp, gpu_util, gpu_mem_used_mb, gpu_power_watts,
                   cpu_util, cpu_mem_used_mb, disk_read_gbps, disk_write_gbps,
                   net_recv_gbps, net_sent_gbps, energy_gpu_joules, energy_cpu_joules
            FROM metrics
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp
        """, (start_time, end_time))
        rows = cursor.fetchall()
        return [
            {
                "timestamp": row[0],
                "gpu_utilization_pct": row[1],
                "gpu_memory_used_mb": row[2],
                "gpu_power_watts": row[3],
                "cpu_utilization_pct": row[4],
                "cpu_memory_used_mb": row[5],
                "disk_read_bandwidth_gbps": row[6],
                "disk_write_bandwidth_gbps": row[7],
                "net_recv_bandwidth_gbps": row[8],
                "net_sent_bandwidth_gbps": row[9],
                "energy_gpu_joules": row[10],
                "energy_cpu_joules": row[11],
            }
            for row in rows
        ]


# ----------------------------------------------------------------------
# Example usage
# ----------------------------------------------------------------------
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    # Create profiler with history and callbacks
    profiler = GPUProfiler()
    profiler.start()

    # Register a callback that prints metrics
    def print_metrics(metrics):
        print(f"GPU util: {metrics.get('gpu_utilization_pct', 0)*100:.1f}%, "
              f"Power: {metrics.get('gpu_power_watts', 0):.1f}W")
    profiler.register_callback(print_metrics, cooldown=1.0)

    # Simulate running
    time.sleep(5)

    # Get MODP utility
    utility = profiler.get_modp_utility()
    print(f"MODP utility: {utility:.3f}")

    # Get context for MoE
    context = profiler.get_moe_context()
    print(f"MoE context: {context}")

    # Use distillation and RLHF to select policy
    policy_dist = profiler.get_policy_from_distillation()
    policy_rlhf = profiler.get_policy_from_rlhf()
    print(f"Distilled policy: {policy_dist}, RLHF policy: {policy_rlhf}")

    # Retrieve history
    history = profiler.get_history(start_time=time.time()-10)
    print(f"History entries: {len(history)}")

    profiler.stop()
