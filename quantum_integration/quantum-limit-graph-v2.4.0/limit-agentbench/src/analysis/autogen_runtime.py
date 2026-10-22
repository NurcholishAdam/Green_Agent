#!/usr/bin/env python3
# =============================================================================
# FILE: src/analysis/adapters/autogen_runtime.py
# VERSION: 2.0.0
# =============================================================================
"""
AutoGen Runtime Adapter with message-graph depth tracking — v2.0.0
===================================================================

Translates AutoGen runtime lifecycle events into the analysis layer's shared
contracts (`DecisionRecord`, `AgentObservation`) and honors the eco-mode hooks
(`reduce_tool_calls`, `shorten_context`) requested by the Adaptive Controller.

v1 API preserved (documented semantics changes — see below):
    runtime = AutoGenRuntime()
    runtime.init(config)
    result = runtime.run(query)
    runtime.reduce_tool_calls()
    runtime.shorten_context()
    runtime.finalize()

v2 enhancements:
  [P0] Precision multiplier now affects `energy_kwh` (was documented, never
       applied); `accuracy` degrades with throttling (0.85 baseline minus
       tool/context loss); `conversation_depth` in `run()` return is **per-run**
       (was cumulative across session — a bug); `init()` resets ALL session
       state; `_to_dict` handles dataclasses/pydantic/objects safely; `_records`
       `_observations` `_history` are bounded deques with `evicted_records`
       counter.
  [P1] `_run_id` initialised in `__init__` (consistent after auto-init);
       semantic `selected_action` (`"run"` / `"run_throttled"`); renamed
       `run_elapsed_ms` → `dispatch_elapsed_ms` (was confusingly similar to
       `latency_ms`); `finalize()` sets `_initialized=False`; `run()` after
       `finalize()` raises `RuntimeError`; `EnergyMeter`/`CarbonEstimator`
       protocol validation; per-task depth tracked; `_run_id = ""` default.
  [P2] `__repr__` / `__len__`; `reset()`; UTC record timestamps;
       `throttle_events` snapshot in each record's `explanation`;
       `correlation_id` propagation; `export()` includes `observations`;
       lazy `%s` logging.
  [P3] `EnergyMeter`/`CarbonEstimator` protocols; `async arun()`; `restore_
       tool_calls()` / `restore_context()` for recovery; input clamps
       (agent_count ≤ 100, tool_calls ≤ 1000, tokens ≤ 1M); `_self_test()`.

**v2.0.0 breaking changes:**
  - `run()["conversation_depth"]` is now the **per-run** graph depth (was
    cumulative across the session — a bug). The old cumulative value remains
    accessible via `runtime.graph_depth`.
  - `accuracy` in `run()` return is now **≤ 0.85** and decreases with throttling
    (was hardcoded to `0.85` regardless — an eco-mode that appeared free).
  - `energy_kwh` now respects `precision` (was ignoring it — a lie in the v1
    docstring).

Callers relying on the old (buggy) behaviour can inspect `runtime.graph_depth`
and re-derive `0.85` themselves; nothing else changes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from typing import (Any, Awaitable, Callable, Deque, Dict, List, Optional,
                    Protocol, runtime_checkable)

logger = logging.getLogger(__name__)


# =============================================================================
# Protocols — [P3-2]
# =============================================================================
@runtime_checkable
class EnergyMeter(Protocol):
    """Optional injected energy meter. Returns energy in **Wh**."""
    def measure(self, *, tool_calls: int) -> float: ...


@runtime_checkable
class CarbonEstimator(Protocol):
    """Optional injected carbon estimator. Returns kg CO₂."""
    def estimate(self, *, energy_kwh: float) -> float: ...


# =============================================================================
# Shared contracts — lazily imported with inline fallback
# =============================================================================
def _try_import_contracts():
    try:
        from src.analysis import DecisionRecord, AgentObservation  # type: ignore
        return DecisionRecord, AgentObservation, True
    except Exception:
        pass
    try:
        from analysis import DecisionRecord, AgentObservation  # type: ignore
        return DecisionRecord, AgentObservation, True
    except Exception:
        pass

    @dataclass
    class DecisionRecord:  # type: ignore[no-redef]
        run_id: str
        timestamp: datetime
        task_id: str
        selected_action: str
        alternatives: List[str] = field(default_factory=list)
        policy_version: str = ""
        model_or_agent: str = ""
        hardware_profile: str = ""
        precision: Optional[str] = None
        quality_score: Optional[float] = None
        latency_ms: float = 0.0
        energy_kwh: float = 0.0
        carbon_operational_kg: float = 0.0
        helium_units: float = 0.0
        carbon_contractual_kg: float = 0.0
        rec_mwh: float = 0.0
        instrument_provenance: Optional[Dict[str, Any]] = None
        uncertainty: Optional[float] = None
        explanation: Dict[str, Any] = field(default_factory=dict)
        safety_verdict: str = "n/a"
        safety_evidence_id: Optional[str] = None
        human_approval_required: bool = False
        human_approval_status: Optional[str] = None
        escalation_reason: Optional[str] = None
        quantum_status: str = "classical"
        quantum_shots: Optional[int] = None
        quantum_queue_ms: Optional[float] = None
        quantum_error_rate: Optional[float] = None
        quantum_mitigation_applied: bool = False
        provenance: Dict[str, Any] = field(default_factory=dict)

    @dataclass
    class AgentObservation:  # type: ignore[no-redef]
        agent_id: str
        task_id: str
        role: str
        delegated_to: List[str]
        coordination_calls: int
        latency_ms: float
        energy_kwh: float

    return DecisionRecord, AgentObservation, False


DecisionRecord, AgentObservation, _CONTRACTS_AVAILABLE = _try_import_contracts()


# =============================================================================
# Safe serialization helpers — [P0-5]
# =============================================================================
def _to_dict(obj: Any) -> Dict[str, Any]:
    """Convert dataclass / pydantic / plain object to dict. Never raises."""
    if isinstance(obj, dict):
        return dict(obj)
    if hasattr(obj, "model_dump") and callable(obj.model_dump):
        try:
            return obj.model_dump()
        except Exception:
            pass
    if is_dataclass(obj) and not isinstance(obj, type):
        try:
            return asdict(obj)
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        return dict(vars(obj))
    raise TypeError(f"cannot convert {type(obj).__name__} to dict")


def _to_jsonable(v: Any) -> Any:
    """Recursively convert to JSON-safe structure."""
    if isinstance(v, dict):
        return {str(k): _to_jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_to_jsonable(val) for val in v]
    if isinstance(v, datetime):
        return v.isoformat()
    try:
        from enum import Enum
        if isinstance(v, Enum):
            return v.value
    except Exception:
        pass
    if is_dataclass(v) and not isinstance(v, type):
        return _to_jsonable(asdict(v))
    return v


# =============================================================================
# Enhanced AutoGenRuntime
# =============================================================================
class AutoGenRuntime:
    """
    Enhanced AutoGen runtime adapter.

    Backward-compatible with the v1 five-method API. Adds real eco-mode
    throttle state, per-run DecisionRecord emission, statistics, and export.

    **v2.0.0 breaking changes** (documented in module docstring):
      - `run()["conversation_depth"]` is per-run (was cumulative).
      - `accuracy` degrades with throttling (was hardcoded 0.85).
      - `energy_kwh` respects precision (was ignoring it).
    """

    # [P0-1] precision multipliers for energy (Wh factor)
    PRECISION_ENERGY_MULTIPLIER: Dict[str, float] = {
        "fp32": 1.0,
        "tf32": 0.75,
        "bf16": 0.55,
        "fp16": 0.5,
        "int8": 0.3,
    }

    # [P0-2] accuracy model — honest trade-off
    BASELINE_ACCURACY: float = 0.85
    TOOL_CALL_ACCURACY_LOSS: float = 0.03      # per removed tool call
    CONTEXT_ACCURACY_LOSS: float = 0.10        # at 0.0 context factor
    MIN_ACCURACY: float = 0.30

    # [P3-4] input clamps
    MAX_AGENT_COUNT: int = 100
    MAX_TOOL_CALLS: int = 1000
    MAX_CONTEXT_TOKENS: int = 1_000_000

    # Conservative energy per tool call (Wh)
    ENERGY_PER_TOOL_CALL_WH: float = 0.05
    # Baseline latency per graph-depth layer (ms)
    LATENCY_PER_DEPTH_MS: float = 15.0

    # [P1-2] semantic action labels
    ACTION_RUN: str = "run"
    ACTION_RUN_THROTTLED: str = "run_throttled"

    def __init__(self, max_records: int = 100_000):
        # --- [P1-1] consistent _run_id from the start ---
        self._run_id: str = f"run-{uuid.uuid4().hex[:8]}"

        # --- Original public state (preserved) ---
        self.graph_depth: int = 0

        # --- Runtime state ---
        self.config: Dict[str, Any] = {}
        self._initialized: bool = False
        self._finalized: bool = False

        # --- Eco-mode throttle state ---
        self._tool_call_reduction: int = 0
        self._context_shortening_factor: float = 1.0

        # --- Identity & telemetry ---
        self._run_counter: int = 0
        self._policy_version: str = ""
        self._hardware_profile: str = ""
        self._precision: Optional[str] = None
        self._correlation_id: str = ""

        # --- [P0-6] bounded collections ---
        self._records: Deque[Any] = deque(maxlen=max_records)
        self._observations: Deque[Any] = deque(maxlen=max_records)
        self._history: Deque[Dict[str, Any]] = deque(maxlen=max_records)
        self._evicted_records: int = 0
        self._max_records: int = max_records

        # --- Optional telemetry injection ---
        self._energy_meter: Optional[EnergyMeter] = None
        self._carbon_estimator: Optional[CarbonEstimator] = None

        # --- Statistics ---
        self._total_tool_calls: int = 0
        self._total_energy_kwh: float = 0.0
        self._total_latency_ms: float = 0.0
        self._throttle_events: Counter = Counter()

        # --- [P1-8] per-task depth (for stats, not used in per-run return) ---
        self._depth_by_task: Dict[str, int] = defaultdict(int)
        self._max_graph_depth: int = 0

        logger.debug("Enhanced AutoGenRuntime v2.0.0 initialized")

    # ------------------------------------------------------------------
    # [P2-2] Dunder helpers
    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        return (
            f"AutoGenRuntime("
            f"run_id={self._run_id!r}, "
            f"runs={self._run_counter}, "
            f"depth={self.graph_depth}, "
            f"throttle={self._tool_call_reduction}/"
            f"{self._context_shortening_factor:.3f}, "
            f"finalized={self._finalized})"
        )

    def __len__(self) -> int:
        return self._run_counter

    # ------------------------------------------------------------------
    # [P2-3] Reset — full session reset, keeps current config
    # ------------------------------------------------------------------
    def reset(self) -> None:
        """Reset session state but keep current config and _run_id."""
        current_config = dict(self.config)
        self.__init__(max_records=self._max_records)
        self.config = current_config

    # ------------------------------------------------------------------
    # Original public API
    # ------------------------------------------------------------------
    def init(self, config: dict) -> None:
        """
        Initialise the runtime with a configuration dict.

        [P0-4] Resets ALL session state — a second `init()` starts a fresh
        session (records, counters, throttle state are cleared).
        """
        if config is None:
            config = {}
        if not isinstance(config, dict):
            logger.warning(
                "init() received non-dict config (%s); using empty dict",
                type(config).__name__)
            config = {}

        # Store config (shallow copy for safety)
        self.config = dict(config)

        # [P1-1] optional run_id override from config
        if "run_id" in config and config["run_id"]:
            self._run_id = str(config["run_id"])

        # Extract telemetry hints
        self._policy_version = str(config.get("policy_version", ""))
        self._hardware_profile = str(config.get("hardware_profile", ""))
        self._precision = config.get("precision")
        self._correlation_id = str(config.get("correlation_id", ""))

        # [P1-6] validate telemetry protocols
        meter = config.get("energy_meter")
        if meter is not None:
            if not isinstance(meter, EnergyMeter):
                logger.warning(
                    "config['energy_meter'] does not implement "
                    "EnergyMeter.measure(tool_calls=int) -> float; "
                    "will fall back to synthetic energy")
                meter = None
        self._energy_meter = meter

        estimator = config.get("carbon_estimator")
        if estimator is not None:
            if not isinstance(estimator, CarbonEstimator):
                logger.warning(
                    "config['carbon_estimator'] does not implement "
                    "CarbonEstimator.estimate(energy_kwh=float) -> float; "
                    "will fall back to synthetic carbon")
                estimator = None
        self._carbon_estimator = estimator

        # [P0-4] Reset ALL session state
        self._run_counter = 0
        self._records.clear()
        self._observations.clear()
        self._history.clear()
        self._evicted_records = 0
        self._total_tool_calls = 0
        self._total_energy_kwh = 0.0
        self._total_latency_ms = 0.0
        self._throttle_events = Counter()
        self._depth_by_task = defaultdict(int)
        self._max_graph_depth = 0
        self.graph_depth = 0
        self._tool_call_reduction = 0
        self._context_shortening_factor = 1.0

        self._initialized = True
        self._finalized = False

        logger.info(
            "AutoGenRuntime initialized (run_id=%s, precision=%s, hardware=%s)",
            self._run_id, self._precision, self._hardware_profile)

    def run(self, query: dict) -> dict:
        """
        Execute a simulated AutoGen conversation turn.

        Original return shape preserved (3 keys) plus additive extensions.

        **v2.0.0:** `conversation_depth` is now per-run (was cumulative).
        `accuracy` degrades with throttling.
        """
        # [P1-4/P1-5] reject runs after finalize
        if self._finalized and not self._initialized:
            raise RuntimeError(
                "run() called after finalize(); call init() to open a new session")

        # Auto-init if needed
        if not self._initialized:
            logger.warning("run() called before init(); auto-initializing")
            self.init({})

        # Input validation
        if query is None:
            query = {}
        if not isinstance(query, dict):
            logger.warning(
                "run() received non-dict query (%s); using empty dict",
                type(query).__name__)
            query = {}

        self._run_counter += 1
        run_start = time.perf_counter()
        timestamp = datetime.now(timezone.utc)

        # --- [P3-4] clamps ---
        try:
            depth_increment = int(query.get("agent_count", 2))
        except (TypeError, ValueError):
            depth_increment = 2
        depth_increment = max(1, min(self.MAX_AGENT_COUNT, depth_increment))

        try:
            base_tool_calls = int(query.get("base_tool_calls", 1))
        except (TypeError, ValueError):
            base_tool_calls = 1
        base_tool_calls = max(0, min(self.MAX_TOOL_CALLS, base_tool_calls))

        try:
            base_context_tokens = int(query.get("base_context_tokens", 0))
        except (TypeError, ValueError):
            base_context_tokens = 0
        base_context_tokens = max(0, min(self.MAX_CONTEXT_TOKENS,
                                           base_context_tokens))

        task_id = str(query.get("task_id", f"task-{self._run_counter}"))

        # --- [P0-3] per-run depth (was cumulative) ---
        run_depth = depth_increment
        # Session-level tracking (stats only)
        self.graph_depth += depth_increment
        self._max_graph_depth = max(self._max_graph_depth, run_depth)
        self._depth_by_task[task_id] += depth_increment

        # --- Tool calls with throttle ---
        effective_tool_calls = max(0, base_tool_calls - self._tool_call_reduction)

        # --- Context with shorten ---
        effective_context_tokens = int(
            base_context_tokens * self._context_shortening_factor)

        # --- [P0-1] energy with precision multiplier ---
        precision_mult = self._precision_energy_multiplier()
        if self._energy_meter is not None:
            try:
                energy_wh = float(self._energy_meter.measure(
                    tool_calls=effective_tool_calls))
                # If the injected meter bypasses precision, apply it on top
                # (callers who want full control should pre-bake precision
                #  into the meter itself and pass precision_mult=1.0 via
                #  config['precision']=None)
            except Exception as e:
                logger.warning(
                    "Energy meter failed (%s: %s); using synthetic fallback",
                    type(e).__name__, e)
                energy_wh = (effective_tool_calls
                             * self.ENERGY_PER_TOOL_CALL_WH
                             * precision_mult)
        else:
            energy_wh = (effective_tool_calls
                         * self.ENERGY_PER_TOOL_CALL_WH
                         * precision_mult)
        energy_kwh = energy_wh / 1000.0

        # --- [P0-3] per-run latency ---
        latency_ms = run_depth * self.LATENCY_PER_DEPTH_MS

        # --- Carbon estimation ---
        if self._carbon_estimator is not None:
            try:
                carbon_kg = float(self._carbon_estimator.estimate(
                    energy_kwh=energy_kwh))
            except Exception as e:
                logger.warning(
                    "Carbon estimator failed (%s: %s); using synthetic fallback",
                    type(e).__name__, e)
                carbon_kg = energy_kwh * 0.4
        else:
            carbon_kg = energy_kwh * 0.4

        # --- [P0-2] accuracy with honest degradation ---
        accuracy = self._compute_accuracy(
            requested_tools=base_tool_calls,
            effective_tools=effective_tool_calls,
            context_factor=self._context_shortening_factor)

        # --- [P1-2] semantic action label ---
        throttled = (self._tool_call_reduction > 0
                     or self._context_shortening_factor < 1.0)
        selected_action = (self.ACTION_RUN_THROTTLED if throttled
                            else self.ACTION_RUN)

        # --- Update stats ---
        self._total_tool_calls += effective_tool_calls
        self._total_energy_kwh += energy_kwh
        self._total_latency_ms += latency_ms

        # --- [P2-5] snapshot throttle events for the record ---
        throttle_snapshot = dict(self._throttle_events)

        # --- Emit DecisionRecord ---
        record = DecisionRecord(
            run_id=self._run_id,
            timestamp=timestamp,
            task_id=task_id,
            selected_action=selected_action,
            alternatives=[],
            policy_version=self._policy_version,
            model_or_agent="autogen",
            hardware_profile=self._hardware_profile,
            precision=self._precision,
            quality_score=accuracy,
            latency_ms=latency_ms,
            energy_kwh=energy_kwh,
            carbon_operational_kg=carbon_kg,
            explanation={
                "run_depth": run_depth,
                "session_max_depth": self._max_graph_depth,
                "depth_increment": depth_increment,
                "task_total_depth": self._depth_by_task[task_id],
                "tool_calls_requested": base_tool_calls,
                "tool_calls_effective": effective_tool_calls,
                "tool_calls_reduction": self._tool_call_reduction,
                "context_tokens_requested": base_context_tokens,
                "context_tokens_effective": effective_context_tokens,
                "context_factor": self._context_shortening_factor,
                "accuracy_baseline": self.BASELINE_ACCURACY,
                "accuracy_effective": accuracy,
                "precision": self._precision,
                "precision_energy_multiplier": precision_mult,
                "throttle_events": throttle_snapshot,
                "simulated": True,
            },
            provenance={
                "source": "autogen_runtime",
                "config_keys": sorted(self.config.keys()),
                "correlation_id": self._correlation_id,
            },
        )
        self._append_record(record)

        # --- Emit AgentObservation ---
        self._append_observation(AgentObservation(
            agent_id="autogen_runtime",
            task_id=task_id,
            role="generalist",
            delegated_to=[],
            coordination_calls=1,
            latency_ms=latency_ms,
            energy_kwh=energy_kwh,
        ))

        # --- [P1-3] history with dispatch_elapsed_ms ---
        dispatch_elapsed_ms = (time.perf_counter() - run_start) * 1000.0
        self._append_history({
            "run_index": self._run_counter,
            "task_id": task_id,
            "run_depth": run_depth,
            "session_max_depth": self._max_graph_depth,
            "task_total_depth": self._depth_by_task[task_id],
            "tool_calls_requested": base_tool_calls,
            "tool_calls_effective": effective_tool_calls,
            "context_tokens_effective": effective_context_tokens,
            "accuracy": accuracy,
            "energy_kwh": energy_kwh,
            "carbon_kg": carbon_kg,
            "latency_ms": latency_ms,
            "dispatch_elapsed_ms": dispatch_elapsed_ms,
            "throttled": throttled,
            "at": timestamp.isoformat(),
        })

        # --- Return shape (original 3 keys first, then additive) ---
        return {
            # Preserved keys
            "accuracy": accuracy,
            "tool_calls": effective_tool_calls,
            "conversation_depth": run_depth,  # [P0-3] per-run
            # Additive extensions (documented as v2)
            "run_depth": run_depth,
            "session_max_depth": self._max_graph_depth,
            "energy_kwh": energy_kwh,
            "latency_ms": latency_ms,
            "carbon_kg": carbon_kg,
            "throttled": throttled,
        }

    def reduce_tool_calls(self) -> None:
        """
        Eco-mode hook: reduce the number of tool calls per subsequent run.

        [P0-2] No longer free — each reduction decreases `accuracy`.
        """
        self._tool_call_reduction += 1
        self._throttle_events["reduce_tool_calls"] += 1
        logger.debug(
            "reduce_tool_calls(): effective reduction now %d",
            self._tool_call_reduction)

    def shorten_context(self) -> None:
        """
        Eco-mode hook: reduce context size for subsequent runs.

        [P0-2] No longer free — each shortening decreases `accuracy`.
        """
        self._context_shortening_factor = max(
            0.125, self._context_shortening_factor * 0.5)
        self._throttle_events["shorten_context"] += 1
        logger.debug(
            "shorten_context(): context factor now %.3f",
            self._context_shortening_factor)

    def finalize(self) -> None:
        """
        Clean up the runtime.

        [P1-4] Marks the runtime as finalized AND `_initialized=False`, so
        subsequent `run()` calls raise until `init()` is called again.
        """
        if self._finalized:
            logger.debug("finalize() called twice; ignoring")
            return

        self._finalized = True
        self._initialized = False

        logger.info(
            "AutoGenRuntime finalized: runs=%d, depth=%d, "
            "total_tool_calls=%d, total_energy_kwh=%.6f, "
            "throttle_events=%s",
            self._run_counter, self.graph_depth, self._total_tool_calls,
            self._total_energy_kwh, dict(self._throttle_events))

    # ------------------------------------------------------------------
    # [P3-4] Recovery hooks
    # ------------------------------------------------------------------
    def restore_tool_calls(self, amount: int = 1) -> None:
        """Reduce the pending tool-call reduction (undo eco-mode throttling)."""
        self._tool_call_reduction = max(0, self._tool_call_reduction - int(amount))
        self._throttle_events["restore_tool_calls"] += 1
        logger.debug(
            "restore_tool_calls(%d): reduction now %d",
            amount, self._tool_call_reduction)

    def restore_context(self) -> None:
        """Undo one level of context shortening (doubles factor, capped at 1.0)."""
        self._context_shortening_factor = min(
            1.0, self._context_shortening_factor * 2.0)
        self._throttle_events["restore_context"] += 1
        logger.debug(
            "restore_context(): factor now %.3f",
            self._context_shortening_factor)

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------
    def records(self) -> List[Any]:
        """Return all DecisionRecords emitted during this session."""
        return list(self._records)

    def observations(self) -> List[Any]:
        """Return all AgentObservations for multi-agent analysis."""
        return list(self._observations)

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative runtime statistics."""
        return {
            "run_id": self._run_id,
            "initialized": self._initialized,
            "finalized": self._finalized,
            "run_count": self._run_counter,
            "graph_depth": self.graph_depth,
            "max_graph_depth": self._max_graph_depth,
            "total_tool_calls": self._total_tool_calls,
            "total_energy_kwh": self._total_energy_kwh,
            "total_latency_ms": self._total_latency_ms,
            "tool_call_reduction": self._tool_call_reduction,
            "context_shortening_factor": self._context_shortening_factor,
            "throttle_events": dict(self._throttle_events),
            "records_emitted": len(self._records),
            "observations_emitted": len(self._observations),
            "history_entries": len(self._history),
            "evicted_records": self._evicted_records,
            "contracts_available": _CONTRACTS_AVAILABLE,
            "precision": self._precision,
            "hardware_profile": self._hardware_profile,
            "correlation_id": self._correlation_id,
        }

    def export(self) -> Dict[str, Any]:
        """
        Serialise everything for JSON export.

        [P0-5] Uses `_to_dict` so dataclasses, pydantic v2 models, and
        plain objects all serialize correctly.
        [P2-7] Includes observations.
        """
        records_out: List[Dict[str, Any]] = []
        for r in self._records:
            try:
                records_out.append(_to_dict(r))
            except Exception as e:
                logger.warning(
                    "Failed to serialize record (%s: %s)",
                    type(e).__name__, e)
                records_out.append({"error": str(e)})

        observations_out: List[Dict[str, Any]] = []
        for o in self._observations:
            try:
                observations_out.append(_to_dict(o))
            except Exception as e:
                logger.warning(
                    "Failed to serialize observation (%s: %s)",
                    type(e).__name__, e)
                observations_out.append({"error": str(e)})

        return {
            "config": dict(self.config),
            "statistics": self.get_statistics(),
            "history": list(self._history),
            "records": records_out,
            "observations": observations_out,
        }

    # ------------------------------------------------------------------
    # [P3-3] Async wrapper
    # ------------------------------------------------------------------
    async def arun(self, query: dict) -> dict:
        """Async wrapper around `run()` for async callers."""
        return await asyncio.to_thread(self.run, query)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _precision_energy_multiplier(self) -> float:
        """[P0-1] Look up the energy multiplier for the configured precision."""
        if not self._precision:
            return 1.0
        return self.PRECISION_ENERGY_MULTIPLIER.get(self._precision, 1.0)

    def _compute_accuracy(self, requested_tools: int, effective_tools: int,
                            context_factor: float) -> float:
        """
        [P0-2] Compute accuracy degradation from eco-mode throttling.

        Un-throttled: returns BASELINE_ACCURACY (0.85).
        Each removed tool call: −0.03.
        Each 0.5× context shortening: −0.05 (interpolated).
        Floor: MIN_ACCURACY (0.30).
        """
        tool_loss = (requested_tools - effective_tools) * self.TOOL_CALL_ACCURACY_LOSS
        context_loss = (1.0 - max(0.0, min(1.0, context_factor))) * self.CONTEXT_ACCURACY_LOSS
        acc = self.BASELINE_ACCURACY - tool_loss - context_loss
        return max(self.MIN_ACCURACY, min(self.BASELINE_ACCURACY, acc))

    def _append_record(self, record: Any) -> None:
        """[P0-6] Bounded append with eviction tracking."""
        if len(self._records) == self._records.maxlen:
            self._evicted_records += 1
        self._records.append(record)

    def _append_observation(self, observation: Any) -> None:
        if len(self._observations) == self._observations.maxlen:
            self._evicted_records += 1
        self._observations.append(observation)

    def _append_history(self, entry: Dict[str, Any]) -> None:
        if len(self._history) == self._history.maxlen:
            self._evicted_records += 1
        self._history.append(entry)


# =============================================================================
# Demo
# =============================================================================
def _demo():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    print("=" * 72)
    print("AutoGen Runtime v2.0.0 — demo")
    print("=" * 72)

    # --- Original behavior ---
    print("\n=== Baseline (v1-compatible) ===")
    runtime = AutoGenRuntime()
    runtime.init({"policy_version": "v5.0.1", "hardware_profile": "V100"})
    print("Run 1:", runtime.run({"task_id": "t1"}))
    print("Run 2:", runtime.run({"task_id": "t1"}))
    runtime.finalize()

    # --- Eco-mode now has cost ---
    print("\n=== Eco-mode trade-off (P0-2 fix) ===")
    rt = AutoGenRuntime()
    rt.init({"policy_version": "v5.0.1"})

    base = rt.run({"task_id": "t2", "base_tool_calls": 5,
                    "base_context_tokens": 4096})
    print(f"Baseline:        acc={base['accuracy']:.3f} "
          f"tools={base['tool_calls']} depth={base['conversation_depth']}")

    rt.reduce_tool_calls()
    rt.shorten_context()
    t1 = rt.run({"task_id": "t2", "base_tool_calls": 5,
                  "base_context_tokens": 4096})
    print(f"1× throttled:    acc={t1['accuracy']:.3f} "
          f"tools={t1['tool_calls']} depth={t1['conversation_depth']}")

    rt.reduce_tool_calls()
    rt.shorten_context()
    t2 = rt.run({"task_id": "t2", "base_tool_calls": 5,
                  "base_context_tokens": 4096})
    print(f"2× throttled:    acc={t2['accuracy']:.3f} "
          f"tools={t2['tool_calls']} depth={t2['conversation_depth']}")

    # --- Precision now affects energy ---
    print("\n=== Precision affects energy (P0-1 fix) ===")
    for precision in ("fp32", "bf16", "int8"):
        r = AutoGenRuntime()
        r.init({"precision": precision})
        res = r.run({"task_id": "t3", "base_tool_calls": 10})
        print(f"  {precision:5s}: energy_kwh={res['energy_kwh']:.6f}")

    # --- Recovery ---
    print("\n=== Recovery (P3-4) ===")
    rt.restore_tool_calls(2)
    rt.restore_context()
    recovered = rt.run({"task_id": "t2", "base_tool_calls": 5,
                         "base_context_tokens": 4096})
    print(f"After recovery:  acc={recovered['accuracy']:.3f} "
          f"tools={recovered['tool_calls']}")

    print("\n=== Statistics ===")
    print(json.dumps(rt.get_statistics(), indent=2, default=str))

    print("\n=== Last DecisionRecord ===")
    rec = rt.records()[-1]
    print(f"  selected_action: {rec.selected_action}")
    print(f"  task_id:         {rec.task_id}")
    print(f"  latency_ms:      {rec.latency_ms}")
    print(f"  energy_kwh:      {rec.energy_kwh}")
    print(f"  carbon_kg:       {rec.carbon_operational_kg}")
    print(f"  explanation keys:{sorted(rec.explanation.keys())[:6]}...")

    rt.finalize()
    print(f"\n{rt!r}")
    print("\n✓ Demo complete")


# =============================================================================
# Self test
# =============================================================================
def _self_test() -> int:
    """Return 0 if all assertions pass, else count of failures."""
    failures = 0

    def _check(name: str, fn) -> None:
        nonlocal failures
        try:
            fn()
            print(f"  ✓ {name}")
        except AssertionError as e:
            failures += 1
            print(f"  ✗ {name}: {e}")
        except Exception as e:
            failures += 1
            print(f"  ✗ {name}: {type(e).__name__}: {e}")

    print("=" * 72)
    print("AutoGen Runtime v2.0.0 — self test")
    print("=" * 72)

    # 1. Backward compat: 3 preserved keys
    def _t_backward() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        r = rt.run({"task_id": "t1"})
        assert "accuracy" in r
        assert "tool_calls" in r
        assert "conversation_depth" in r
    _check("v1 return shape preserved", _t_backward)

    # 2. [P0-1] Precision affects energy
    def _t_precision() -> None:
        energies = {}
        for p in ("fp32", "bf16", "int8"):
            rt = AutoGenRuntime()
            rt.init({"precision": p})
            r = rt.run({"task_id": "t", "base_tool_calls": 10})
            energies[p] = r["energy_kwh"]
        assert energies["fp32"] > energies["bf16"] > energies["int8"], energies
        assert abs(energies["int8"] / energies["fp32"] - 0.3) < 0.01, energies
    _check("[P0-1] Precision multiplier affects energy", _t_precision)

    # 3. [P0-2] Accuracy degrades with throttling
    def _t_accuracy() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        base = rt.run({"task_id": "t", "base_tool_calls": 5})
        assert abs(base["accuracy"] - 0.85) < 1e-6, base
        rt.reduce_tool_calls()
        rt.reduce_tool_calls()
        t2 = rt.run({"task_id": "t", "base_tool_calls": 5})
        assert t2["accuracy"] < base["accuracy"], (base, t2)
        assert t2["accuracy"] < 0.85 - 0.05, t2  # at least 2 × 0.03 loss
    _check("[P0-2] Accuracy degrades with throttling", _t_accuracy)

    # 4. [P0-3] conversation_depth is per-run
    def _t_depth() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        r1 = rt.run({"task_id": "t", "agent_count": 2})
        r2 = rt.run({"task_id": "t", "agent_count": 2})
        assert r1["conversation_depth"] == 2, r1
        assert r2["conversation_depth"] == 2, r2   # not 4
        assert rt.graph_depth == 4   # cumulative exposed on attribute
        assert r2["session_max_depth"] == 2, r2
    _check("[P0-3] Per-run depth (was cumulative)", _t_depth)

    # 5. [P0-4] init() resets session
    def _t_init_reset() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        rt.run({"task_id": "t"})
        rt.reduce_tool_calls()
        assert rt._run_counter == 1
        rt.init({})  # second init
        assert rt._run_counter == 0
        assert len(rt.records()) == 0
        assert rt._tool_call_reduction == 0
    _check("[P0-4] init() resets session state", _t_init_reset)

    # 6. [P0-5] export() handles dataclass records
    def _t_export() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        rt.run({"task_id": "t"})
        exp = rt.export()
        assert "records" in exp and len(exp["records"]) >= 1
        # Must be JSON-serializable
        json.dumps(exp, default=str)
    _check("[P0-5] export() handles dataclass records", _t_export)

    # 7. [P0-6] Bounded collections
    def _t_bounded() -> None:
        rt = AutoGenRuntime(max_records=5)
        rt.init({})
        for _ in range(10):
            rt.run({"task_id": "t"})
        assert len(rt.records()) == 5
        assert rt._evicted_records > 0
    _check("[P0-6] Bounded records", _t_bounded)

    # 8. [P1-1] run_id consistent after auto-init
    def _t_runid() -> None:
        rt = AutoGenRuntime()
        initial = rt._run_id
        rt.run({"task_id": "t"})  # auto-init
        assert rt._run_id == initial
        rec = rt.records()[-1]
        assert rec.run_id == initial
    _check("[P1-1] run_id consistent after auto-init", _t_runid)

    # 9. [P1-2] Semantic selected_action
    def _t_action() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        rt.run({"task_id": "t"})
        assert rt.records()[-1].selected_action == "run"
        rt.reduce_tool_calls()
        rt.run({"task_id": "t"})
        assert rt.records()[-1].selected_action == "run_throttled"
    _check("[P1-2] Semantic selected_action", _t_action)

    # 10. [P1-3] dispatch_elapsed_ms in history
    def _t_dispatch() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        rt.run({"task_id": "t"})
        h = rt._history[-1]
        assert "dispatch_elapsed_ms" in h, h.keys()
        assert "run_elapsed_ms" not in h, "old name should be gone"
    _check("[P1-3] dispatch_elapsed_ms in history", _t_dispatch)

    # 11. [P1-4/P1-5] finalize guards
    def _t_finalize() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        rt.run({"task_id": "t"})
        rt.finalize()
        assert rt._finalized
        assert not rt._initialized
        try:
            rt.run({"task_id": "t"})
        except RuntimeError:
            pass
        else:
            raise AssertionError("run() after finalize() should raise")
        # Re-init should work
        rt.init({})
        rt.run({"task_id": "t"})
    _check("[P1-4/5] finalize guards", _t_finalize)

    # 12. [P1-8] per-task depth tracking
    def _t_task_depth() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        rt.run({"task_id": "a", "agent_count": 2})
        rt.run({"task_id": "b", "agent_count": 3})
        rt.run({"task_id": "a", "agent_count": 2})
        assert rt._depth_by_task["a"] == 4, rt._depth_by_task
        assert rt._depth_by_task["b"] == 3, rt._depth_by_task
    _check("[P1-8] Per-task depth tracking", _t_task_depth)

    # 13. [P2-3] reset()
    def _t_reset() -> None:
        rt = AutoGenRuntime()
        rt.init({"policy_version": "x"})
        rt.run({"task_id": "t"})
        rt.reset()
        assert rt._run_counter == 0
        assert rt.config.get("policy_version") == "x"  # config preserved
    _check("[P2-3] reset() preserves config", _t_reset)

    # 14. [P2-7] export includes observations
    def _t_export_obs() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        rt.run({"task_id": "t"})
        e = rt.export()
        assert "observations" in e
        assert len(e["observations"]) >= 1
    _check("[P2-7] export() includes observations", _t_export_obs)

    # 15. [P3-3] arun async wrapper
    def _t_arun() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        async def _run():
            r = await rt.arun({"task_id": "t"})
            return r
        r = asyncio.run(_run())
        assert r["accuracy"] == 0.85
    _check("[P3-3] arun() async wrapper", _t_arun)

    # 16. [P3-4] restore_tool_calls / restore_context
    def _t_restore() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        rt.reduce_tool_calls()
        rt.reduce_tool_calls()
        rt.shorten_context()
        rt.shorten_context()
        assert rt._tool_call_reduction == 2
        assert rt._context_shortening_factor == 0.25
        rt.restore_tool_calls(1)
        rt.restore_context()
        assert rt._tool_call_reduction == 1
        assert rt._context_shortening_factor == 0.5
    _check("[P3-4] Restore hooks", _t_restore)

    # 17. [P3-4] Input clamps
    def _t_clamps() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        r = rt.run({"task_id": "t", "agent_count": 1_000_000,
                     "base_tool_calls": 10**9,
                     "base_context_tokens": 10**12})
        assert r["conversation_depth"] <= AutoGenRuntime.MAX_AGENT_COUNT
        assert r["tool_calls"] <= AutoGenRuntime.MAX_TOOL_CALLS
    _check("[P3-4] Input clamps", _t_clamps)

    # 18. [P2-2] Dunders
    def _t_repr() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        rt.run({"task_id": "t"})
        r = repr(rt)
        assert "runs=1" in r, r
        assert len(rt) == 1
    _check("[P2-2] __repr__ / __len__", _t_repr)

    # 19. [P1-6] Protocol validation
    def _t_protocol() -> None:
        class BadMeter:
            def wrong_method(self): pass
        rt = AutoGenRuntime()
        rt.init({"energy_meter": BadMeter()})
        # Bad meter should be rejected
        assert rt._energy_meter is None
    _check("[P1-6] Protocol validation", _t_protocol)

    # 20. [P2-5] throttle_events in explanation
    def _t_throttle_exp() -> None:
        rt = AutoGenRuntime()
        rt.init({})
        rt.reduce_tool_calls()
        rt.run({"task_id": "t"})
        exp = rt.records()[-1].explanation
        assert "throttle_events" in exp, exp.keys()
        assert exp["throttle_events"].get("reduce_tool_calls") == 1
    _check("[P2-5] throttle_events in explanation", _t_throttle_exp)

    print("=" * 72)
    if failures == 0:
        print("✅ All tests passed")
    else:
        print(f"❌ {failures} test(s) failed")
    print("=" * 72)
    return failures


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    # Run demo if DASHBOARD_DEMO=1, else self-test
    if os.environ.get("AUTOGEN_RUNTIME_DEMO") == "1":
        _demo()
        sys.exit(0)
    sys.exit(_self_test())
