"""
gpu_profiler.py — Enhanced v4

Real-time hardware metrics collector. The ten Green Agent enhancements
are now imported from their own modules (see enhancements/*.py). This file
contains only profiler-specific logic.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import random
import signal
import sqlite3
import tempfile
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None
    PSUTIL_AVAILABLE = False

try:
    import pynvml
    try:
        pynvml.nvmlInit()
        NVML_AVAILABLE = True
    except Exception:
        NVML_AVAILABLE = False
except ImportError:
    pynvml = None
    NVML_AVAILABLE = False

from enhancements.common_state import (
    STATE_VECTOR_DIM,
    PROFILER_SCHEMA_VERSION,
    state_vector_from_metrics,
)
from enhancements.precision import (
    PrecisionLevel,
    PRECISION_COST,
    PrecisionController,
    HardwareAwareAdapter,
)
from enhancements.carbon import (
    CarbonMarketClient,
    RECInventory,
)
from enhancements.chaos import (
    ChaosConfig,
    ChaosEngineer,
)
from enhancements.stl import (
    STLOperator,
    STLFormula,
    TemporalLogicMonitor,
    SafetyShield,
)
from enhancements.causal import (
    CausalTransition,
    CausalCounterfactualEstimator,
)
from enhancements.federated import (
    FederatedUpdate,
    FederatedAggregator,
)
from enhancements.multi_agent import (
    AgentRole,
    AgentBid,
    EmergentRoleRegistry,
    MultiAgentCoordinator,
)
from enhancements.xai import (
    Explanation,
    DecisionExplainer,
)
from enhancements.quantum import (
    QuantumInspiredTeacher,
    DistillationEnsemble,
)
from enhancements.hitl import (
    HITLRequest,
    UncertaintyEstimator,
    HumanInTheLoopGate,
    ActiveLearningSampler,
)

try:
    from enhancements.MODP import ParetoOptimizer
except ImportError:
    class ParetoOptimizer:
        def evaluate(self, metrics, weights):
            return sum(metrics.get(k, 0.0) * weights.get(k, 1.0) for k in metrics)

try:
    from enhancements.bio_inspired import FitnessEvaluator
except ImportError:
    class FitnessEvaluator:
        def evaluate(self, metrics, policy=None):
            tokens = metrics.get("tokens_per_sec", 0.0)
            power = max(metrics.get("gpu_power_watts", 1.0), 1.0)
            return tokens / power

try:
    from enhancements.moe_system import ContextEncoder
except ImportError:
    class ContextEncoder:
        def encode(self, metrics):
            return [
                float(metrics.get("gpu_utilization_pct", 0.0)),
                float(metrics.get("cpu_utilization_pct", 0.0)),
                float(metrics.get("gpu_memory_used_mb", 0.0)) / 1000.0,
                float(metrics.get("gpu_power_watts", 0.0)) / 400.0,
                float(metrics.get("gpu_temp_c", 0.0)) / 100.0,
            ]

try:
    from enhancements.limit_graph import LimitGraph
except ImportError:
    class LimitGraph:
        def __init__(self, *a, **kw): pass
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass

try:
    from enhancements.rlhf import RLHFOptimizer
except ImportError:
    class RLHFOptimizer:
        def __init__(self, action_space, *a, **kw):
            self.actions = list(action_space)
        def update(self, context, action, reward): pass
        def sample_action(self, context):
            return self.actions[0] if self.actions else None

try:
    from enhancements.multi_teacher_policy_distillation import MultiTeacherDistiller
except ImportError:
    class MultiTeacherDistiller:
        def __init__(self, teachers, *a, **kw):
            self.teachers = list(teachers)
        def distill(self, context):
            return self.teachers[0](context) if self.teachers else None


@dataclass
class ProfilerConfig:
    sample_interval: float = 0.5
    enable_history: bool = True
    history_db_path: str = "gpu_metrics.db"
    history_batch_size: int = 10
    max_history_days: int = 7
    max_pending_rows: int = 10_000
    enable_callbacks: bool = True
    callback_cooldown: float = 0.1
    enable_limit_graph: bool = True
    enable_rlhf: bool = True
    enable_distillation: bool = True

    instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    node_id: str = "local"

    cpu_tdp_watts: float = 65.0
    cpu_idle_fraction: float = 0.2

    carbon_intensity_g_per_kwh: float = 400.0
    carbon_market_base_price: float = 0.05
    carbon_market_sensitivity: float = 0.0005
    rec_default_kwh: float = 0.0

    enable_quantum_teacher: bool = True
    enable_causal: bool = True
    enable_federated: bool = True
    enable_multi_agent: bool = True
    enable_temporal_logic: bool = True
    enable_xai: bool = True
    enable_precision_switching: bool = True
    enable_carbon_market: bool = True
    enable_chaos: bool = False
    enable_hitl: bool = True

    chaos_fault_prob: float = 0.0
    chaos_latency_ms: float = 0.0
    chaos_carbon_spike_prob: float = 0.0
    chaos_sensor_noise_std: float = 0.0

    hitl_timeout_s: float = 2.0
    hitl_auto_approve: bool = True

    uncertainty_anomaly_z: float = 3.0
    uncertainty_observation_keys: Tuple[str, ...] = (
        "gpu_power_watts", "gpu_temp_c", "energy_gpu_joules",
    )
    sampler_error_backoff_initial: float = 1.0
    sampler_error_backoff_max: float = 60.0

    def __post_init__(self):
        if self.sample_interval <= 0:
            raise ValueError("sample_interval must be > 0")
        if self.history_batch_size < 1:
            raise ValueError("history_batch_size must be >= 1")
        if self.max_history_days < 1:
            raise ValueError("max_history_days must be >= 1")
        if self.max_pending_rows < 1:
            raise ValueError("max_pending_rows must be >= 1")


class GPUProfiler:
    """
    Enhanced profiler with all ten Green Agent enhancements imported from
    the enhancements package.
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
        hitl_approver: Optional[Callable[[HITLRequest], bool]] = None,
        token_source: Optional[Callable[[], float]] = None,
        carbon_source: Optional[Callable[[], float]] = None,
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

        self._token_source = token_source
        self._carbon_source = carbon_source

        self.limit_graph = limit_graph if (self.config.enable_limit_graph and limit_graph) else (
            LimitGraph() if self.config.enable_limit_graph else None
        )
        self.rlhf = rlhf_optimizer if (self.config.enable_rlhf and rlhf_optimizer) else (
            RLHFOptimizer(action_space=["balanced", "performance", "power_save"])
            if self.config.enable_rlhf else None
        )
        self.distiller = distiller if (self.config.enable_distillation and distiller) else None

        self.quantum_teacher = QuantumInspiredTeacher(seed=0) if self.config.enable_quantum_teacher else None
        self.distill_ensemble = (
            DistillationEnsemble(self.quantum_teacher, alpha=0.5)
            if self.quantum_teacher else None
        )

        self.causal: Optional[CausalCounterfactualEstimator] = (
            CausalCounterfactualEstimator(
                state_dim=STATE_VECTOR_DIM, n_actions=len(AgentRole)
            ) if self.config.enable_causal else None
        )

        self.federated = FederatedAggregator() if self.config.enable_federated else None

        self.roles = EmergentRoleRegistry() if self.config.enable_multi_agent else None
        self.coordinator = (
            MultiAgentCoordinator(self.roles, rng=random.Random(0)) if self.roles else None
        )

        self.temporal: Optional[TemporalLogicMonitor] = None
        self.shield: Optional[SafetyShield] = None
        if self.config.enable_temporal_logic:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="thermal_ok",
                predicate=lambda r: r.get("gpu_temp_c", 0.0) <= 85.0,
                operator=STLOperator.ALWAYS,
                horizon=10,
            ))
            self.temporal.add_formula(STLFormula(
                name="power_within_cap",
                predicate=lambda r: r.get("gpu_power_watts", 0.0) <= 400.0,
                operator=STLOperator.ALWAYS,
                horizon=10,
            ))
            self.temporal.add_formula(STLFormula(
                name="eventually_idle",
                predicate=lambda r: r.get("gpu_utilization_pct", 1.0) < 0.1,
                operator=STLOperator.EVENTUALLY,
                horizon=20,
            ))
            self.temporal.add_formula(STLFormula(
                name="carbon_bounded_until_idle",
                predicate=lambda r: r.get("net_carbon_g", 0.0) <= 50.0,
                operator=STLOperator.UNTIL,
                horizon=15,
                until_predicate=lambda r: r.get("gpu_utilization_pct", 1.0) < 0.1,
            ))
            self.shield = SafetyShield(self.temporal)

        self.explainer = DecisionExplainer() if self.config.enable_xai else None
        self.last_explanation: Optional[Explanation] = None

        self.precision_controller = (
            PrecisionController(quality_floor=0.95)
            if self.config.enable_precision_switching else None
        )
        self.precision_adapter = (
            HardwareAwareAdapter(self.precision_controller)
            if self.precision_controller else None
        )
        self.current_precision: Optional[PrecisionLevel] = None

        if self.config.enable_carbon_market:
            self.carbon_market: Optional[CarbonMarketClient] = CarbonMarketClient(
                base_price=self.config.carbon_market_base_price,
                sensitivity=self.config.carbon_market_sensitivity,
            )
            self.recs: Optional[RECInventory] = RECInventory()
            if self.config.rec_default_kwh > 0:
                self.recs.add(self.config.rec_default_kwh)
        else:
            self.carbon_market = None
            self.recs = None

        self.chaos: Optional[ChaosEngineer] = None
        if self.config.enable_chaos:
            self.chaos = ChaosEngineer(ChaosConfig(
                fault_prob=self.config.chaos_fault_prob,
                latency_inject_ms=self.config.chaos_latency_ms,
                carbon_spike_prob=self.config.chaos_carbon_spike_prob,
                sensor_noise_std=self.config.chaos_sensor_noise_std,
            ))

        self.uncertainty = UncertaintyEstimator() if self.config.enable_hitl else None
        self.hitl = (
            HumanInTheLoopGate(
                approver=hitl_approver,
                timeout_s=self.config.hitl_timeout_s,
                auto_approve_on_timeout=self.config.hitl_auto_approve,
            ) if self.config.enable_hitl else None
        )
        self.active_learner = ActiveLearningSampler() if self.config.enable_hitl else None

        # Internal state
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._state_lock = threading.Lock()
        self._conn_lock = threading.Lock()
        self._callbacks_lock = threading.Lock()
        self._latest_metrics: Dict[str, Any] = {}
        self._callbacks: Dict[int, Dict[str, Any]] = {}
        self._next_callback_id = 0

        self._disk_io_start = None
        self._net_io_start = None
        self._last_disk_time = time.time()
        self._last_net_time = time.time()
        self._energy_cumulative_gpu_joules = 0.0
        self._energy_cumulative_cpu_joules = 0.0
        self._last_power_sample_time = time.time()

        self._external_tokens_per_sec: float = 0.0
        self._external_tokens_per_sec_set: bool = False

        self._conn: Optional[sqlite3.Connection] = None
        self._pending_rows: List[Tuple] = []
        self._samples_since_flush = 0

        if PSUTIL_AVAILABLE:
            try:
                self._disk_io_start = psutil.disk_io_counters()
                self._net_io_start = psutil.net_io_counters()
            except Exception:
                self._disk_io_start = None
                self._net_io_start = None

        if self.config.enable_history:
            self._init_history()

        atexit.register(self._atexit_cleanup)
        self._install_signal_handlers()

    # ------------------------------------------------------------------
    # Signal / cleanup
    # ------------------------------------------------------------------
    def _install_signal_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                prev = signal.getsignal(sig)

                def _handler(signum, frame, prev=prev):
                    try:
                        self.stop()
                    finally:
                        if callable(prev):
                            prev(signum, frame)

                signal.signal(sig, _handler)
            except (ValueError, OSError):
                pass

    def __enter__(self) -> "GPUProfiler":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def _atexit_cleanup(self) -> None:
        try:
            self.stop()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------
    def _init_history(self) -> None:
        try:
            self._conn = sqlite3.connect(
                self.config.history_db_path, check_same_thread=False
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    instance_id TEXT,
                    node_id TEXT,
                    schema_version TEXT,
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
                    precision_level TEXT,
                    effective_bits REAL,
                    carbon_intensity REAL,
                    carbon_price REAL,
                    net_carbon_g REAL,
                    rec_offset_kg REAL,
                    uncertainty_score REAL,
                    chaos_events TEXT,
                    provenance TEXT
                )
            """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_metrics_ts ON metrics(timestamp)"
            )
            self._conn.commit()
            self._clean_history()
        except Exception as e:
            logging.warning(f"Failed to initialize history: {e}")
            self._conn = None

    def _clean_history(self) -> None:
        if not self._conn:
            return
        cutoff = time.time() - self.config.max_history_days * 86400
        try:
            with self._conn_lock:
                self._conn.execute("DELETE FROM metrics WHERE timestamp < ?", (cutoff,))
                self._conn.commit()
        except Exception as e:
            logging.warning(f"History cleanup failed: {e}")

    def _queue_row(self, metrics: Dict[str, Any]) -> None:
        if not self._conn:
            return
        chaos_events = list(metrics.get("chaos_events") or [])
        row = (
            metrics.get("timestamp", time.time()),
            self.config.instance_id,
            self.config.node_id,
            PROFILER_SCHEMA_VERSION,
            metrics.get("gpu_utilization_pct", 0.0),
            metrics.get("gpu_memory_used_mb", 0.0),
            metrics.get("gpu_power_watts", 0.0),
            metrics.get("cpu_utilization_pct", 0.0),
            metrics.get("cpu_memory_used_mb", 0.0),
            metrics.get("disk_read_bandwidth_gbps", 0.0),
            metrics.get("disk_write_bandwidth_gbps", 0.0),
            metrics.get("net_recv_bandwidth_gbps", 0.0),
            metrics.get("net_sent_bandwidth_gbps", 0.0),
            metrics.get("energy_gpu_joules", 0.0),
            metrics.get("energy_cpu_joules", 0.0),
            metrics.get("precision_level"),
            metrics.get("effective_bits", 0.0),
            metrics.get("carbon_intensity_g_per_kwh", 0.0),
            metrics.get("carbon_price_per_kg", 0.0),
            metrics.get("net_carbon_g", 0.0),
            metrics.get("rec_offset_kg", 0.0),
            metrics.get("uncertainty_score", 0.0),
            json.dumps(chaos_events),
            json.dumps(metrics.get("provenance", {})),
        )
        with self._conn_lock:
            if len(self._pending_rows) >= self.config.max_pending_rows:
                self._pending_rows.pop(0)
            self._pending_rows.append(row)
            self._samples_since_flush += 1
        if self._samples_since_flush >= self.config.history_batch_size:
            self._flush_rows()

    def _flush_rows(self) -> None:
        if not self._conn:
            return
        with self._conn_lock:
            if not self._pending_rows:
                return
            rows = self._pending_rows
            self._pending_rows = []
            self._samples_since_flush = 0
        try:
            with self._conn_lock:
                self._conn.executemany("""
                    INSERT INTO metrics (
                        timestamp, instance_id, node_id, schema_version,
                        gpu_util, gpu_mem_used_mb, gpu_power_watts,
                        cpu_util, cpu_mem_used_mb,
                        disk_read_gbps, disk_write_gbps,
                        net_recv_gbps, net_sent_gbps,
                        energy_gpu_joules, energy_cpu_joules,
                        precision_level, effective_bits,
                        carbon_intensity, carbon_price,
                        net_carbon_g, rec_offset_kg,
                        uncertainty_score, chaos_events, provenance
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows)
                self._conn.commit()
        except Exception as e:
            logging.warning(f"Failed to flush history rows: {e}")
            with self._conn_lock:
                space = max(0, self.config.max_pending_rows - len(self._pending_rows))
                self._pending_rows = rows[:space] + self._pending_rows
                self._samples_since_flush = len(self._pending_rows)

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------
    def register_callback(
        self,
        callback: Callable[[Dict[str, Any]], None],
        cooldown: float = 0.1,
    ) -> int:
        with self._callbacks_lock:
            cb_id = self._next_callback_id
            self._next_callback_id += 1
            self._callbacks[cb_id] = {
                "callback": callback, "cooldown": cooldown, "last": 0.0,
            }
            return cb_id

    def unregister_callback(self, cb_id: int) -> bool:
        with self._callbacks_lock:
            return self._callbacks.pop(cb_id, None) is not None

    def _trigger_callbacks(self, metrics: Dict[str, Any]) -> None:
        now = time.time()
        with self._callbacks_lock:
            entries = [(cid, dict(v)) for cid, v in self._callbacks.items()]
        for cid, entry in entries:
            if now - entry["last"] < entry["cooldown"]:
                continue
            try:
                entry["callback"](metrics)
            except Exception as e:
                logging.error(f"Callback error: {e}")
            with self._callbacks_lock:
                if cid in self._callbacks:
                    self._callbacks[cid]["last"] = now

    # ------------------------------------------------------------------
    # Sampling
    # ------------------------------------------------------------------
    def _snapshot(self) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}
        now = time.time()
        metrics["timestamp"] = now
        metrics["instance_id"] = self.config.instance_id
        metrics["node_id"] = self.config.node_id

        chaos_faulted = False
        if self.chaos and self.chaos.maybe_fault():
            chaos_faulted = True
            metrics["chaos_events"] = list(self.chaos.events)[-1:]

        if NVML_AVAILABLE and pynvml is not None and not chaos_faulted:
            try:
                device_count = pynvml.nvmlDeviceGetCount()
                metrics["gpu_count"] = device_count
                if device_count > 0:
                    total_used = 0.0
                    total_free = 0.0
                    total_power = 0.0
                    max_util = 0.0
                    temps = []
                    for i in range(device_count):
                        handle = pynvml.nvmlDeviceGetHandleByIndex(i)
                        mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                        util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                        power = pynvml.nvmlDeviceGetPowerUsage(handle) / 1000.0
                        temp = pynvml.nvmlDeviceGetTemperature(
                            handle, pynvml.NVML_TEMPERATURE_GPU
                        )
                        total_used += mem_info.used / 1024 ** 2
                        total_free += mem_info.free / 1024 ** 2
                        total_power += power
                        max_util = max(max_util, util.gpu / 100.0)
                        temps.append(temp)
                    metrics["gpu_memory_total_mb"] = total_used + total_free
                    metrics["gpu_memory_used_mb"] = total_used
                    metrics["gpu_memory_free_mb"] = total_free
                    metrics["gpu_utilization_pct"] = max_util
                    metrics["gpu_power_watts"] = total_power
                    metrics["gpu_temp_c"] = sum(temps) / len(temps)
                    metrics["gpu_available"] = True
                else:
                    metrics["gpu_available"] = False
            except Exception as e:
                logging.warning(f"NVML snapshot error: {e}")
                metrics["gpu_available"] = False
        else:
            metrics["gpu_available"] = False

        if self.chaos and not chaos_faulted:
            for k in ("gpu_power_watts", "gpu_temp_c", "gpu_utilization_pct"):
                if k in metrics:
                    metrics[k] = self.chaos.apply_sensor_noise(metrics[k])

        if PSUTIL_AVAILABLE and not chaos_faulted:
            vm = psutil.virtual_memory()
            metrics["cpu_memory_total_mb"] = vm.total / 1024 ** 2
            metrics["cpu_memory_free_mb"] = vm.available / 1024 ** 2
            metrics["cpu_memory_used_mb"] = vm.used / 1024 ** 2
            try:
                psutil.cpu_percent(interval=None)
                cpu_pct = psutil.cpu_percent(interval=0.0)
            except Exception:
                cpu_pct = 0.0
            metrics["cpu_utilization_pct"] = cpu_pct / 100.0
        else:
            metrics["cpu_utilization_pct"] = 0.0

        if PSUTIL_AVAILABLE and not chaos_faulted:
            try:
                disk_io = psutil.disk_io_counters()
            except Exception:
                disk_io = None
            if disk_io is not None and self._disk_io_start is not None:
                delta = now - self._last_disk_time
                if delta > 1e-3:
                    metrics["disk_read_bandwidth_gbps"] = (
                        (disk_io.read_bytes - self._disk_io_start.read_bytes) / delta
                    ) * 8 / 1e9
                    metrics["disk_write_bandwidth_gbps"] = (
                        (disk_io.write_bytes - self._disk_io_start.write_bytes) / delta
                    ) * 8 / 1e9
            self._disk_io_start = disk_io
            self._last_disk_time = now

            try:
                net_io = psutil.net_io_counters()
            except Exception:
                net_io = None
            if net_io is not None and self._net_io_start is not None:
                delta = now - self._last_net_time
                if delta > 1e-3:
                    metrics["net_recv_bandwidth_gbps"] = (
                        (net_io.bytes_recv - self._net_io_start.bytes_recv) / delta
                    ) * 8 / 1e9
                    metrics["net_sent_bandwidth_gbps"] = (
                        (net_io.bytes_sent - self._net_io_start.bytes_sent) / delta
                    ) * 8 / 1e9
            self._net_io_start = net_io
            self._last_net_time = now

        elapsed = now - self._last_power_sample_time
        elapsed = max(0.0, min(elapsed, self.config.sample_interval * 4.0))
        gpu_power = float(metrics.get("gpu_power_watts", 0.0))
        cpu_util = float(metrics.get("cpu_utilization_pct", 0.0))
        idle_frac = self.config.cpu_idle_fraction
        cpu_power = self.config.cpu_tdp_watts * (idle_frac + (1.0 - idle_frac) * cpu_util)
        self._energy_cumulative_gpu_joules += gpu_power * elapsed
        self._energy_cumulative_cpu_joules += cpu_power * elapsed
        metrics["gpu_power_watts_cpu_model"] = cpu_power
        metrics["energy_gpu_joules"] = self._energy_cumulative_gpu_joules
        metrics["energy_cpu_joules"] = self._energy_cumulative_cpu_joules
        self._last_power_sample_time = now

        if self._token_source is not None:
            try:
                self._external_tokens_per_sec = float(self._token_source())
                self._external_tokens_per_sec_set = True
            except Exception:
                pass
        metrics["tokens_per_sec"] = self._external_tokens_per_sec
        metrics["tokens_per_sec_source"] = (
            "external" if self._external_tokens_per_sec_set else "default"
        )

        carbon_intensity = self.config.carbon_intensity_g_per_kwh
        if self._carbon_source is not None:
            try:
                carbon_intensity = float(self._carbon_source())
            except Exception:
                pass
        if self.chaos:
            carbon_intensity = self.chaos.maybe_carbon_spike(carbon_intensity)
        metrics["carbon_intensity_g_per_kwh"] = carbon_intensity

        carbon_price = 0.0
        if self.carbon_market:
            carbon_price = self.carbon_market.price(
                carbon_intensity, hour_of_day=time.localtime().tm_hour
            )
        metrics["carbon_price_per_kg"] = carbon_price

        energy_kwh = (self._energy_cumulative_gpu_joules + self._energy_cumulative_cpu_joules) / 3.6e6
        gross_carbon_g = energy_kwh * carbon_intensity
        rec_offset_kg = 0.0
        if self.recs and self.recs.total_kwh() > 0 and energy_kwh > 0:
            rec_offset_kg = self.recs.consume(min(energy_kwh, self.recs.total_kwh()))
        metrics["gross_carbon_g"] = gross_carbon_g
        metrics["rec_offset_kg"] = rec_offset_kg
        metrics["rec_kwh_available"] = self.recs.total_kwh() if self.recs else 0.0
        metrics["net_carbon_g"] = max(0.0, gross_carbon_g - rec_offset_kg * 1000.0)

        metrics["energy_efficiency"] = self._compute_energy_efficiency(metrics)
        metrics["carbon_efficiency"] = self._compute_carbon_efficiency(metrics)
        metrics["memory_efficiency"] = self._compute_memory_efficiency(metrics)
        metrics["throughput"] = metrics.get("tokens_per_sec", 0.0)

        if self.precision_adapter:
            headroom = 0.5
            _, level = self.precision_adapter.adapt({}, carbon_intensity, headroom, carbon_price)
            self.current_precision = level
            metrics["precision_level"] = level.value
            metrics["effective_bits"] = PRECISION_COST[level]["bits"]
        else:
            metrics["precision_level"] = None
            metrics["effective_bits"] = 0.0

        if self.uncertainty:
            self.uncertainty.observe(metrics)
            keys = self.config.uncertainty_observation_keys
            metrics["uncertainty_score"] = self.uncertainty.composite_uncertainty(keys)
        else:
            metrics["uncertainty_score"] = 0.0

        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.temporal and self.shield:
            self.temporal.observe(metrics)
            safety_ok, stl_verdict, fallback = self.shield.screen()
        metrics["safety_ok"] = safety_ok
        metrics["stl_verdict"] = stl_verdict

        if self.explainer and self.uncertainty and not chaos_faulted:
            gpu_power_v = float(metrics.get("gpu_power_watts", 0.0))
            is_anom, z = self.uncertainty.is_anomalous(
                "gpu_power_watts", gpu_power_v,
                z_thresh=self.config.uncertainty_anomaly_z,
            )
            if is_anom or not safety_ok:
                baseline = {k: self.uncertainty.mean(k) for k in self.explainer.feature_names}
                scales = {k: self.uncertainty.std(k) for k in self.explainer.feature_names}
                explanation = self.explainer.explain_anomaly(
                    kind="power_anomaly" if is_anom else "safety_violation",
                    current=metrics,
                    baseline=baseline,
                    scales=scales,
                    counterfactuals=self.causal_counterfactuals(metrics),
                    safety_ok=safety_ok,
                    carbon_price=carbon_price,
                    confidence=min(1.0, abs(z) / 5.0) if is_anom else 0.9,
                )
                self.last_explanation = explanation
                metrics["explanation"] = explanation.to_dict()

                if self.hitl and self.uncertainty:
                    unc = float(metrics.get("uncertainty_score", 0.0))
                    if unc > 0.5 or not safety_ok:
                        req = HITLRequest(
                            request_id=uuid.uuid4().hex[:8],
                            reason="high_uncertainty" if unc > 0.5 else "safety_violation",
                            metric_name="gpu_power_watts",
                            value=gpu_power_v,
                            uncertainty=unc,
                            carbon_price=carbon_price,
                        )
                        approved = self.hitl.request_sync(req)
                        metrics["human_approved"] = approved
                        if not approved:
                            metrics["action"] = "fallback_safe_profile"

                if self.active_learner:
                    numeric = {k: v for k, v in metrics.items()
                               if isinstance(v, (int, float)) and not isinstance(v, bool)}
                    self.active_learner.maybe_store(
                        {"metrics": numeric},
                        uncertainty=float(metrics.get("uncertainty_score", 0.0)),
                        threshold=0.4,
                    )

        metrics["provenance"] = {
            "schema": PROFILER_SCHEMA_VERSION,
            "instance_id": self.config.instance_id,
            "node_id": self.config.node_id,
            "nvml_available": NVML_AVAILABLE,
            "chaos_active": self.chaos is not None,
            "chaos_config": (asdict(self.chaos.config) if self.chaos else None),
            "state_vector_dim": STATE_VECTOR_DIM,
        }

        if "chaos_events" not in metrics:
            metrics["chaos_events"] = []

        return metrics

    def _compute_energy_efficiency(self, metrics: Dict[str, Any]) -> float:
        tokens = float(metrics.get("tokens_per_sec", 0.0))
        total_power = (
            float(metrics.get("gpu_power_watts", 0.0))
            + float(metrics.get("gpu_power_watts_cpu_model", 0.0))
        )
        return tokens / total_power if total_power > 0 else 0.0

    def _compute_carbon_efficiency(self, metrics: Dict[str, Any]) -> float:
        carbon_intensity = float(metrics.get("carbon_intensity_g_per_kwh", 0.0))
        total_energy_j = (
            float(metrics.get("energy_gpu_joules", 0.0))
            + float(metrics.get("energy_cpu_joules", 0.0))
        )
        energy_kwh = total_energy_j / 3.6e6
        carbon_kg = energy_kwh * carbon_intensity / 1000.0
        tokens = float(metrics.get("tokens_per_sec", 0.0))
        return tokens / carbon_kg if carbon_kg > 0 else 0.0

    def _compute_memory_efficiency(self, metrics: Dict[str, Any]) -> float:
        total = float(metrics.get("gpu_memory_total_mb", 0.0))
        used = float(metrics.get("gpu_memory_used_mb", 0.0))
        return used / total if total > 0 else 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._running:
            return
        if self.config.enable_history and self._conn is None:
            self._init_history()
        self._running = True
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        was_running = self._running
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        try:
            self._flush_rows()
        except Exception:
            pass
        if self._conn:
            try:
                with self._conn_lock:
                    self._conn.close()
            except Exception:
                pass
            self._conn = None
        with self._conn_lock:
            self._pending_rows = []
            self._samples_since_flush = 0
        if was_running:
            logging.getLogger(__name__).info("GPUProfiler stopped")

    def _sample_loop(self) -> None:
        backoff = self.config.sampler_error_backoff_initial
        while self._running:
            try:
                metrics = self._snapshot()
                with self._state_lock:
                    self._latest_metrics = metrics
                if self.config.enable_history:
                    self._queue_row(metrics)
                if self.config.enable_callbacks:
                    self._trigger_callbacks(metrics)
                backoff = self.config.sampler_error_backoff_initial
            except Exception as e:
                logging.error(f"Sampler error: {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, self.config.sampler_error_backoff_max)
                continue
            time.sleep(self.config.sample_interval)

    def get_current_metrics(self) -> Dict[str, Any]:
        if self._running:
            with self._state_lock:
                return dict(self._latest_metrics)
        return self._snapshot()

    def set_token_rate(self, tokens_per_sec: float) -> None:
        self._external_tokens_per_sec = float(tokens_per_sec)
        self._external_tokens_per_sec_set = True

    def set_carbon_source(self, source: Optional[Callable[[], float]]) -> None:
        self._carbon_source = source

    # ------------------------------------------------------------------
    # Integration interfaces
    # ------------------------------------------------------------------
    def get_modp_utility(self, metrics: Optional[Dict[str, Any]] = None) -> float:
        if metrics is None:
            metrics = self.get_current_metrics()
        objectives = {
            "energy_efficiency": metrics.get("energy_efficiency", 0.0),
            "carbon_efficiency": metrics.get("carbon_efficiency", 0.0),
            "memory_efficiency": metrics.get("memory_efficiency", 0.0),
            "throughput": metrics.get("throughput", 0.0),
        }
        optimizer = getattr(self, "_modp_optimizer", None)
        if optimizer is None:
            optimizer = ParetoOptimizer()
            self._modp_optimizer = optimizer
        return optimizer.evaluate(objectives, self.modp_weights)

    def get_bio_fitness(self, policy: Dict[str, Any]) -> float:
        return self.bio.evaluate(self.get_current_metrics(), policy)

    def get_moe_context(self) -> List[float]:
        return self.moe.encode(self.get_current_metrics())

    def get_policy_from_distillation(self, context: Optional[Dict] = None) -> str:
        if context is None:
            context = self.get_current_metrics()
        if self.distiller is not None:
            try:
                base_policy = self.distiller.distill(context)
            except Exception:
                base_policy = "balanced"
        else:
            base_policy = "balanced"
        if self.distill_ensemble is None:
            return base_policy

        actions = ["balanced", "performance", "power_save"]
        features = np.array([
            float(context.get("gpu_utilization_pct", 0.0)),
            float(context.get("gpu_power_watts", 0.0)) / 400.0,
            float(context.get("gpu_temp_c", 0.0)) / 100.0,
            float(context.get("cpu_utilization_pct", 0.0)),
            float(context.get("carbon_intensity_g_per_kwh", 400.0)) / 1000.0,
        ], dtype=np.float64)
        if base_policy in actions:
            other = np.zeros(len(actions))
            other[actions.index(base_policy)] = 1.0
        else:
            other = np.ones(len(actions)) / len(actions)
        blended = self.distill_ensemble.blend(features, other, len(actions))
        return actions[int(np.argmax(blended))]

    def get_policy_from_rlhf(self, context: Optional[Dict] = None) -> str:
        if not self.rlhf:
            return "balanced"
        if context is None:
            context = self.get_current_metrics()
        try:
            return self.rlhf.sample_action(context) or "balanced"
        except Exception:
            return "balanced"

    def get_limits(self, context: Optional[Dict] = None) -> Dict:
        if not self.limit_graph:
            return {}
        if context is None:
            context = self.get_current_metrics()
        try:
            return self.limit_graph.get_limits(context)
        except Exception:
            return {}

    def update_feedback(self, context: Dict, action: str, reward: float) -> None:
        if self.rlhf:
            try:
                self.rlhf.update(context, action, reward)
            except Exception:
                pass
        if self.limit_graph:
            try:
                self.limit_graph.update_from_feedback({
                    "context": context, "action": action, "reward": reward,
                })
            except Exception:
                pass

        if self.causal is not None:
            try:
                state = state_vector_from_metrics(context)
                if state.size != STATE_VECTOR_DIM:
                    state = np.pad(state, (0, max(0, STATE_VECTOR_DIM - state.size)))[:STATE_VECTOR_DIM]
                actions = ["balanced", "performance", "power_save"]
                a_idx = actions.index(action) if action in actions else 0
                self.causal.update(CausalTransition(
                    state=state, action=a_idx, reward=float(reward), next_state=state,
                ))
            except Exception:
                pass

        if self.roles is not None:
            role: Optional[AgentRole] = None
            if self.coordinator is not None:
                lv = self.coordinator.last_vote() or {}
                roles_seen = lv.get("winning_roles") or []
                if roles_seen:
                    try:
                        role = AgentRole(roles_seen[0])
                    except ValueError:
                        role = None
            if role is None:
                if "carbon" in action:
                    role = AgentRole.CARBON_BROKER
                elif "power" in action:
                    role = AgentRole.SAFETY_OFFICER
                else:
                    role = AgentRole.EXPLOITER
            self.roles.record(role, success=reward > 0.5, reward=reward)

    def _state_vector(self, context: Dict) -> np.ndarray:
        return state_vector_from_metrics(context)

    def vote_metric_emphasis(self, metrics: Optional[Dict[str, Any]] = None) -> str:
        if metrics is None:
            metrics = self.get_current_metrics()
        if self.coordinator is None or self.roles is None:
            return "energy_efficiency"
        objectives = [
            "energy_efficiency",
            "carbon_efficiency",
            "memory_efficiency",
            "throughput",
        ]
        bids: List[AgentBid] = []
        for role in AgentRole:
            if role == AgentRole.CARBON_BROKER:
                idx = objectives.index("carbon_efficiency")
            elif role == AgentRole.EXPLOITER:
                idx = objectives.index("throughput")
            elif role == AgentRole.EXPLORER:
                idx = random.randrange(len(objectives))
            elif role == AgentRole.SAFETY_OFFICER:
                idx = objectives.index("energy_efficiency")
            else:
                idx = objectives.index("memory_efficiency")
            bids.append(AgentBid(
                agent_id=role.value,
                role=role,
                confidence=0.7,
                proposed_action=idx,
                rationale=role.value,
                carbon_score=1.0 / (1.0 + metrics.get("net_carbon_g", 0.0)),
            ))
        winner_idx, _role = self.coordinator.vote(bids, len(objectives))
        return objectives[winner_idx]

    def causal_counterfactuals(
        self, context: Optional[Dict] = None, n_actions: int = 3,
    ) -> Dict[int, float]:
        if self.causal is None:
            return {}
        if context is None:
            context = self.get_current_metrics()
        try:
            return self.causal.counterfactuals(self._state_vector(context), n_actions)
        except Exception:
            return {}

    def export_federated_update(self) -> Optional[Dict[str, Any]]:
        if self.federated is None:
            return None
        m = self.get_current_metrics()
        vec = np.array([
            m.get("energy_efficiency", 0.0),
            m.get("carbon_efficiency", 0.0),
            m.get("memory_efficiency", 0.0),
            m.get("throughput", 0.0),
            m.get("net_carbon_g", 0.0) / 1000.0,
        ], dtype=np.float64)
        update = FederatedUpdate(
            node_id=self.config.node_id,
            weights={"telemetry": vec},
            n_samples=1,
            carbon_intensity=float(m.get("carbon_intensity_g_per_kwh", 400.0)),
        )
        if not self.federated.submit(update):
            return None
        return {"node_id": update.node_id, "vec": vec.tolist()}

    def aggregate_federated(self) -> Optional[Dict[str, np.ndarray]]:
        if self.federated is None:
            return None
        return self.federated.aggregate()

    def save_state(self, path: str) -> bool:
        state: Dict[str, Any] = {"schema": PROFILER_SCHEMA_VERSION}
        if self.causal is not None:
            state["causal"] = self.causal.export_state()
        if self.roles is not None:
            state["roles"] = self.roles.snapshot()
        try:
            d = os.path.dirname(os.path.abspath(path)) or "."
            os.makedirs(d, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=d, prefix=".tmp_", suffix=".json")
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(state, f)
                os.replace(tmp, path)
            except Exception:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
            return True
        except Exception as e:
            logging.error(f"save_state failed: {e}")
            return False

    def load_state(self, path: str) -> bool:
        if not os.path.exists(path):
            return False
        try:
            with open(path, "r") as f:
                state = json.load(f)
        except Exception:
            return False
        if state.get("schema") != PROFILER_SCHEMA_VERSION:
            return False
        if self.causal is not None and "causal" in state:
            self.causal.import_state(state["causal"])
        return True

    def get_stats(self) -> Dict[str, Any]:
        return {
            "schema": PROFILER_SCHEMA_VERSION,
            "instance_id": self.config.instance_id,
            "node_id": self.config.node_id,
            "nvml_available": NVML_AVAILABLE,
            "psutil_available": PSUTIL_AVAILABLE,
            "running": self._running,
            "history_enabled": self._conn is not None,
            "callbacks_registered": len(self._callbacks),
            "quantum_teacher_active": self.quantum_teacher is not None,
            "causal_active": self.causal is not None,
            "federated_active": self.federated is not None,
            "multi_agent_active": self.coordinator is not None,
            "temporal_logic_active": self.temporal is not None,
            "xai_active": self.explainer is not None,
            "precision_switching_active": self.precision_adapter is not None,
            "carbon_market_active": self.carbon_market is not None,
            "chaos_active": self.chaos is not None,
            "hitl_active": self.hitl is not None,
            "current_precision": self.current_precision.value if self.current_precision else None,
            "rec_kwh_available": self.recs.total_kwh() if self.recs else 0.0,
            "last_explanation": (
                self.last_explanation.to_dict() if self.last_explanation else None
            ),
            "active_learning_buffer_size": len(self.active_learner) if self.active_learner else 0,
            "hitl_audit_size": len(self.hitl.audit) if self.hitl else 0,
            "stl_verdict": (self.temporal.verify() if self.temporal else {}),
            "role_snapshot": (self.roles.snapshot() if self.roles else {}),
            "last_vote": (self.coordinator.last_vote() if self.coordinator else None),
            "state_vector_dim": STATE_VECTOR_DIM,
        }

    def get_history(
        self,
        start_time: float = 0.0,
        end_time: Optional[float] = None,
    ) -> List[Dict]:
        if not self._conn:
            return []
        if end_time is None:
            end_time = time.time()
        self._flush_rows()
        try:
            with self._conn_lock:
                cursor = self._conn.execute("""
                    SELECT timestamp, instance_id, node_id, schema_version,
                           gpu_util, gpu_mem_used_mb, gpu_power_watts,
                           cpu_util, cpu_mem_used_mb,
                           disk_read_gbps, disk_write_gbps,
                           net_recv_gbps, net_sent_gbps,
                           energy_gpu_joules, energy_cpu_joules,
                           precision_level, effective_bits,
                           carbon_intensity, carbon_price,
                           net_carbon_g, rec_offset_kg,
                           uncertainty_score, chaos_events, provenance
                    FROM metrics
                    WHERE timestamp BETWEEN ? AND ?
                    ORDER BY timestamp
                """, (start_time, end_time))
                rows = cursor.fetchall()
        except Exception as e:
            logging.warning(f"History query failed: {e}")
            return []
        out: List[Dict] = []
        for row in rows:
            out.append({
                "timestamp": row[0],
                "instance_id": row[1],
                "node_id": row[2],
                "schema_version": row[3],
                "gpu_utilization_pct": row[4],
                "gpu_memory_used_mb": row[5],
                "gpu_power_watts": row[6],
                "cpu_utilization_pct": row[7],
                "cpu_memory_used_mb": row[8],
                "disk_read_bandwidth_gbps": row[9],
                "disk_write_bandwidth_gbps": row[10],
                "net_recv_bandwidth_gbps": row[11],
                "net_sent_bandwidth_gbps": row[12],
                "energy_gpu_joules": row[13],
                "energy_cpu_joules": row[14],
                "precision_level": row[15],
                "effective_bits": row[16],
                "carbon_intensity_g_per_kwh": row[17],
                "carbon_price_per_kg": row[18],
                "net_carbon_g": row[19],
                "rec_offset_kg": row[20],
                "uncertainty_score": row[21],
                "chaos_events": json.loads(row[22]) if row[22] else [],
                "provenance": json.loads(row[23]) if row[23] else {},
            })
        return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    profiler = GPUProfiler()
    profiler.start()

    def print_metrics(metrics):
        print(
            f"GPU util: {metrics.get('gpu_utilization_pct', 0) * 100:.1f}%, "
            f"Power: {metrics.get('gpu_power_watts', 0):.1f}W, "
            f"Precision: {metrics.get('precision_level')}, "
            f"Carbon: {metrics.get('net_carbon_g', 0):.3f}g, "
            f"Uncertainty: {metrics.get('uncertainty_score', 0):.3f}"
        )

    profiler.register_callback(print_metrics, cooldown=1.0)
    time.sleep(5)

    print(f"\nMODP utility: {profiler.get_modp_utility():.4f}")
    print(f"MoE context: {profiler.get_moe_context()}")
    print(f"Distilled policy: {profiler.get_policy_from_distillation()}")
    print(f"RLHF policy: {profiler.get_policy_from_rlhf()}")
    print(f"Metric emphasis vote: {profiler.vote_metric_emphasis()}")
    print(f"Causal counterfactuals: {profiler.causal_counterfactuals()}")

    history = profiler.get_history(start_time=time.time() - 10)
    print(f"\nHistory entries: {len(history)}")
    print(f"Stats: {json.dumps(profiler.get_stats(), indent=2, default=str)}")

    profiler.stop()
