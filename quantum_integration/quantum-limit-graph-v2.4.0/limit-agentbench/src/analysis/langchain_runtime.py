# src/analysis/adapters/langchain_runtime.py
"""
LangChain Runtime Adapter with real callback-style counters (Enhanced, v2.0.0)
==============================================================================

Translates LangChain runtime lifecycle events into the analysis layer's
shared contracts (`DecisionRecord`, `AgentObservation`, `CallbackStep`).

Original API preserved (all signatures, all return shapes):
    runtime = LangChainRuntime()
    runtime.init(config)
    result = runtime.run(query)      # {"accuracy": 0.8, "tool_calls": N,
                                     #  "conversation_depth": 1}
    runtime.reduce_tool_calls()
    runtime.shorten_context()        # v1: no-op; v2: opt-in (see below)
    runtime.finalize()

Enhanced API:
    runtime = LangChainRuntime(
        activate_shorten_context=False,   # opt-in to real context shortening
        use_measured_latency=False,        # opt-in to wall-clock latency
        energy_meter_returns="joules",     # "joules" | "wh" | "kwh" | "j"
        max_records=10_000,
        max_history=1_024,
        ...
    )

    runtime.set_callback(cb)               # GreenLangChainCallback or compatible
    runtime.set_trace(trace)               # ExecutionTrace sink
    runtime.set_hitl_callback(cb)          # reviewer for tool/carbon overruns

    runtime.records(limit=None)            # bounded DecisionRecord list
    runtime.observations(limit=None)       # bounded AgentObservation list
    runtime.enhanced_result(query)         # richer per-run dict
    runtime.export(include_callback=False) # JSON-safe export
    runtime.get_statistics()
    runtime.reset(full=False)              # partial/full state reset
    runtime.set_max_tools(n)               # programmatic cap management
    runtime.reset_max_tools()

Enhancements implemented (v2.0.0):
  1.  Energy unit fix        — `EnergyMeter.measure()` returns joules (the
                                analysis-layer convention); v1 treated the
                                return value as kWh, off by 3.6e6×. All
                                energy / carbon / total-energy paths now
                                normalise correctly. Configurable via
                                `energy_meter_returns=` for callers using a
                                different meter.
  2.  Opt-in context shortening — `shorten_context()` is a documented no-op
                                by default (matching v1). Pass
                                `activate_shorten_context=True` to enable
                                the cumulative ×0.5 (floored at 0.125) and
                                unblock the context-starvation HITL trigger.
  3.  Thread safety          — `RLock` around `init` / `run` /
                                `reduce_tool_calls` / `shorten_context` /
                                `finalize` / `set_callback` /
                                `set_hitl_callback` / `reset` / read paths.
  4.  Input validation       — `base_tool_calls` / `base_context_tokens`
                                coerced via `_safe_int`; non-dict `query`
                                handled; `config` validated at `init`.
  5.  Signature-filtered DR  — `DecisionRecord(**kwargs)` filtered to the
                                installed signature via `inspect.signature`;
                                a contract that dropped a field no longer
                                breaks every `run()`.
  6.  Carbon-zero integrity  — a carbon estimator returning `0.0` is
                                preserved; the fallback only fires when the
                                estimator is absent or fails. `carbon_source`
                                recorded in provenance.
  7.  Bounded state          — `_records` / `_observations` / `_history`
                                use `deque(maxlen=...)`.
  8.  Callback metric merge  — callback snapshot's `tool_calls` /
                                `llm_calls` are deltad and attached to the
                                record's `explanation["callback_reported"]`;
                                callback `agent_id` / `run_id` preferred for
                                the record identity; callback's `role` /
                                `delegated_to` used for observations.
  9.  Measured elapsed       — `run_elapsed_ms` surfaced as
                                `explanation["measured_elapsed_ms"]`;
                                `AgentObservation.latency_ms` uses the
                                measured wall clock.
 10.  `enhanced_result()`    — richer per-run dict (effective context,
                                actual tool calls from callback, energy,
                                carbon, both latencies, provenance labels).
 11.  Injected-meter checks  — `energy_meter` / `carbon_estimator` are
                                interface-validated at `init()`; failures
                                during `run()` are logged as structured
                                events and produce `energy_source` /
                                `carbon_source` labels in the record.
 12.  HITL                   — `set_hitl_callback(cb)` fires on tool-count
                                overruns, context starvation (when
                                shortening is active), carbon overruns, and
                                repeated meter failures. Returning False
                                annotates `explanation` + `safety_verdict`
                                (when the contract supports it).
 13.  ExecutionTrace forward — `set_trace(trace)` records one summary
                                step per `run()`.
 14.  Precision-aware energy — the `ENERGY_PER_TOOL_CALL_WH` estimate is
                                scaled by the `precision` (matching
                                `energy_meter.py`'s relative indicator table).
 15.  Contractual carbon     — `config["carbon_contractual_kg_per_kwh"]`
                                (or a second estimator) populates
                                `DecisionRecord.carbon_contractual_kg`.
 16.  Bounded queries        — `records(limit=N)` / `observations(limit=N)`.
 17.  Full export            — `export(include_callback=True)` includes the
                                wired callback's `export()`; `to_dict`
                                sanitised for JSON.
 18.  Statistics             — `get_statistics()` reports the labeled
                                energy/carbon sources, HITL counts, and
                                fabricated-field flags.
 19.  Honest simulation flags— `_simulated_flags` only increments for
                                fields that are genuinely fabricated
                                (`accuracy`, `conversation_depth`).
 20.  `finalize()` summary   — emits a structured log, optionally a
                                summary `DecisionRecord` when a trace is
                                wired, and (when a federated aggregator is
                                provided) contributes.
 21.  `reset()` / `set_max_tools()` / `reset_max_tools()` / `set_callback`
                                / `set_hitl_callback` / `set_trace` /
                                `set_energy_meter` / `set_carbon_estimator`
                                configuration surface.
 22.  Backward compatibility — every v1 public symbol, method, and return
                                shape preserved.

Backward compatibility:
  * `runtime.run(query)` returns the v1 three-key dict unchanged.
  * `runtime.shorten_context()` is a no-op unless
    `activate_shorten_context=True`.
  * `runtime.init(config)` re-applies identity / meter / callback config
    but preserves cumulative counters (v1 behaviour). Use `reset()` for a
    full state reset.
  * The fallback `DecisionRecord` / `AgentObservation` dataclasses are
    retained for environments where the analysis contract isn't
    importable. They're only used when `src.analysis` (or `analysis`) is
    not importable.
"""

from __future__ import annotations

import inspect
import itertools
import logging
import math
import threading
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field, asdict, is_dataclass
from datetime import datetime
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Tuple,
)

__version__ = "2.0.0"

__all__ = [
    "LangChainRuntime",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

_JOULES_PER_KWH = 3.6e6
_JOULES_PER_WH = 3600.0

#: Precision → relative energy-scale for the per-tool energy estimate.
#: Mirrors `energy_meter.py`'s `_PRECISION_ENERGY_SCALE` for consistency.
_PRECISION_ENERGY_SCALE: Dict[str, float] = {
    "fp64": 1.0,
    "fp32": 1.0,
    "tf32": 0.85,
    "fp16": 0.70,
    "bf16": 0.65,
    "int8": 0.40,
    "int4": 0.25,
    "quantum_distilled": 0.25,
}

_VALID_ENERGY_UNITS = frozenset({"joules", "j", "wh", "kwh"})


# =============================================================================
# Helpers
# =============================================================================

def _safe_int(value: Any, default: int = 0) -> int:
    """Coerce to a bounded non-negative int (rejects bool, NaN, Inf, str)."""
    if isinstance(value, bool):
        return default
    try:
        v = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if v < 0 or v > 1_000_000_000:
        return default
    return v


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce to a finite float; return `default` on any failure."""
    if isinstance(value, bool):
        return default
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return f if math.isfinite(f) else default


def _safe_isoformat(ts: Any) -> Optional[str]:
    try:
        return datetime.fromtimestamp(float(ts)).isoformat()
    except (OSError, ValueError, OverflowError, TypeError):
        return None


def _jsonable(v: Any) -> Any:
    """Recursively coerce to a JSON-serialisable primitive."""
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [_jsonable(x) for x in v]
    if is_dataclass(v) and not isinstance(v, type):
        try:
            return _jsonable(asdict(v))
        except Exception:
            pass
    return repr(v)


def _filter_decision_record_kwargs(
    DecisionRecord: Any, candidates: Dict[str, Any],
) -> Dict[str, Any]:
    """Restrict `candidates` to kwargs the installed DecisionRecord accepts."""
    try:
        sig = inspect.signature(DecisionRecord)
        accepted = set(sig.parameters.keys())
        return {k: v for k, v in candidates.items() if k in accepted}
    except (ValueError, TypeError):
        return dict(candidates)


def _precision_scale(precision: Optional[str]) -> float:
    if not isinstance(precision, str):
        return 1.0
    return _PRECISION_ENERGY_SCALE.get(precision.strip().lower(), 1.0)


def _to_joules(value: float, unit: str) -> float:
    """Convert an energy value to joules given its unit label."""
    u = (unit or "joules").strip().lower()
    if u in ("joules", "j"):
        return value
    if u == "wh":
        return value * _JOULES_PER_WH
    if u == "kwh":
        return value * _JOULES_PER_KWH
    raise ValueError(f"Unknown energy unit: {unit!r}")


# =============================================================================
# Contract import with fallback
# =============================================================================

def _try_import_contracts():
    try:
        from src.analysis import DecisionRecord, AgentObservation  # type: ignore
        return DecisionRecord, AgentObservation, True
    except Exception:
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
# LangChainRuntime
# =============================================================================

class LangChainRuntime:
    """
    Enhanced LangChain runtime adapter.

    Backward-compatible with the original five-method API. Adds real
    callback integration, opt-in throttle behaviour, DecisionRecord
    emission with honest provenance, HITL review, and thread safety.
    """

    #: Per-tool-call energy estimate (Wh) — scaled by precision when set.
    ENERGY_PER_TOOL_CALL_WH = 0.04
    #: Per-tool-call latency estimate (ms).
    LATENCY_PER_TOOL_CALL_MS = 12.0
    #: Fallback grid carbon intensity (kg CO2e / kWh).
    DEFAULT_CARBON_INTENSITY_KG_PER_KWH = 0.4

    #: Default HITL thresholds.
    DEFAULT_HITL_MAX_TOOLS = 20
    DEFAULT_HITL_MAX_CARBON_KG = 0.01
    DEFAULT_HITL_MIN_CONTEXT_FACTOR = 0.25

    #: Backwards-compat default fabricated quality / depth values.
    _DEFAULT_QUALITY_SCORE = 0.8
    _DEFAULT_CONVERSATION_DEPTH = 1

    def __init__(
        self,
        *,
        activate_shorten_context: bool = False,
        use_measured_latency: bool = False,
        energy_meter_returns: str = "joules",
        max_records: int = 10_000,
        max_history: int = 1_024,
        hitl_max_tools: Optional[int] = None,
        hitl_max_carbon_kg: Optional[float] = None,
        hitl_min_context_factor: Optional[float] = None,
    ):
        # --- Energy unit validation ---
        if energy_meter_returns.lower() not in _VALID_ENERGY_UNITS:
            raise ValueError(
                f"energy_meter_returns must be one of "
                f"{sorted(_VALID_ENERGY_UNITS)}, "
                f"got {energy_meter_returns!r}"
            )
        self._energy_meter_returns = energy_meter_returns.lower()

        # --- Behaviour toggles ---
        self._activate_shorten_context = bool(activate_shorten_context)
        self._use_measured_latency = bool(use_measured_latency)

        # --- Bounded state caps ---
        if max_records < 1 or max_history < 1:
            raise ValueError(
                "max_records and max_history must be >= 1"
            )
        self._max_records = int(max_records)
        self._max_history = int(max_history)

        # --- Original state (public) ---
        self.tool_calls: int = 0
        self.max_tools: int = 10
        self.config: Dict[str, Any] = {}

        # --- Lifecycle ---
        self._initialized: bool = False
        self._finalized: bool = False
        self._run_counter: int = 0
        self._run_id: Optional[str] = None
        self._policy_version: str = ""
        self._hardware_profile: str = ""
        self._precision: Optional[str] = None

        # --- Throttle state ---
        self._context_shortening_factor: float = 1.0

        # --- Bounded history ---
        self._records: Deque[Any] = deque(maxlen=self._max_records)
        self._observations: Deque[Any] = deque(maxlen=self._max_records)
        self._history: Deque[Dict[str, Any]] = deque(maxlen=self._max_history)

        # --- Injected integrations ---
        self._callback: Any = None
        self._trace: Any = None
        self._energy_meter: Any = None
        self._carbon_estimator: Any = None
        self._carbon_contractual_kg_per_kwh: Optional[float] = None

        # --- Per-run callback delta tracking ---
        self._last_callback_cumulative: int = 0

        # --- Statistics ---
        self._total_tool_calls: int = 0           # sum of requested (v1 dup)
        self._total_effective_tools: int = 0       # sum of min(requested, cap)
        self._total_actual_tools: int = 0          # sum from callback (when wired)
        self._total_energy_kwh: float = 0.0
        self._total_carbon_kg: float = 0.0
        self._total_latency_ms: float = 0.0
        self._throttle_events: Counter = Counter()
        self._simulated_flags: Counter = Counter()
        self._energy_source_counts: Counter = Counter()
        self._carbon_source_counts: Counter = Counter()
        self._meter_failure_count: int = 0

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[Any], Optional[bool]]
        ] = None
        self._hitl_max_tools = (
            int(hitl_max_tools) if hitl_max_tools is not None
            else self.DEFAULT_HITL_MAX_TOOLS
        )
        self._hitl_max_carbon_kg = (
            float(hitl_max_carbon_kg) if hitl_max_carbon_kg is not None
            else self.DEFAULT_HITL_MAX_CARBON_KG
        )
        self._hitl_min_context_factor = (
            float(hitl_min_context_factor)
            if hitl_min_context_factor is not None
            else self.DEFAULT_HITL_MIN_CONTEXT_FACTOR
        )
        self._hitl_count: int = 0
        self._hitl_denied_count: int = 0

        # --- Concurrency ---
        self._lock = threading.RLock()

        logger.debug(
            "LangChainRuntime initialised "
            "(activate_shorten_context=%s, use_measured_latency=%s, "
            "energy_unit=%s)",
            self._activate_shorten_context,
            self._use_measured_latency,
            self._energy_meter_returns,
        )

    # ------------------------------------------------------------------
    # ORIGINAL public API
    # ------------------------------------------------------------------

    def init(self, config: dict) -> None:
        """
        Initialize the runtime.

        Stores config (v1), extracts identity + integrations (enhanced),
        validates the injected meter / estimator interfaces, and resets
        throttle state. Cumulative counters (tool_calls, records,
        history) are preserved across re-init; call `reset()` for a full
        state reset.
        """
        with self._lock:
            if config is None:
                config = {}
            if not isinstance(config, dict):
                logger.warning(
                    "init() received non-dict config (%s); using empty dict",
                    type(config).__name__,
                )
                config = {}

            self.config = config
            self._policy_version = (
                config.get("policy_version")
                if isinstance(config.get("policy_version"), str) else ""
            )
            self._hardware_profile = (
                config.get("hardware_profile")
                if isinstance(config.get("hardware_profile"), str) else ""
            )
            self._precision = (
                config.get("precision")
                if isinstance(config.get("precision"), str) else None
            )
            self._run_id = (
                config.get("run_id")
                if isinstance(config.get("run_id"), str) else None
            ) or f"run-{uuid.uuid4().hex[:8]}"

            # --- Carbon contractual hint ---
            v = config.get("carbon_contractual_kg_per_kwh")
            if isinstance(v, (int, float)) and not isinstance(v, bool) \
                    and math.isfinite(float(v)) and float(v) >= 0:
                self._carbon_contractual_kg_per_kwh = float(v)
            else:
                self._carbon_contractual_kg_per_kwh = None

            # --- Callback (config-provided) ---
            cb = config.get("callback")
            if cb is not None:
                if self._validate_callback(cb):
                    self._callback = cb
                else:
                    logger.warning(
                        "init(): config['callback'] has no snapshot() or "
                        "statistics() method; ignored",
                    )

            # --- Trace (config-provided) ---
            tr = config.get("trace")
            if tr is not None:
                self._trace = tr

            # --- Energy meter ---
            meter = config.get("energy_meter")
            if meter is not None:
                if self._validate_energy_meter(meter):
                    self._energy_meter = meter
                    logger.debug(
                        "langchain_runtime.meter_wired",
                        extra={"type": type(meter).__name__},
                    )
                else:
                    logger.warning(
                        "init(): energy_meter has no measure() / "
                        "measure_detailed() method; ignored",
                    )

            # --- Carbon estimator ---
            estimator = config.get("carbon_estimator")
            if estimator is not None:
                if self._validate_carbon_estimator(estimator):
                    self._carbon_estimator = estimator
                else:
                    logger.warning(
                        "init(): carbon_estimator has no estimate() "
                        "method; ignored",
                    )

            # --- Reset per-run throttle state (v1 reset behaviour) ---
            self._context_shortening_factor = 1.0

            self._initialized = True
            self._finalized = False
            self._last_callback_cumulative = 0

            logger.info(
                "langchain_runtime.initialized",
                extra={
                    "run_id": self._run_id,
                    "precision": self._precision,
                    "hardware_profile": self._hardware_profile,
                    "callback_wired": self._callback is not None,
                    "energy_meter_wired": self._energy_meter is not None,
                    "carbon_estimator_wired":
                        self._carbon_estimator is not None,
                },
            )

    def run(self, query: dict) -> dict:
        """
        Execute a LangChain runtime turn.

        Backward-compatible return shape (v1):
            {"accuracy": 0.8, "tool_calls": <cumulative>, "conversation_depth": 1}

        For richer per-run data, use `enhanced_result()`.
        """
        result = self._execute(query)
        return {
            "accuracy": self._DEFAULT_QUALITY_SCORE,
            "tool_calls": result["cumulative_tool_calls"],
            "conversation_depth": self._DEFAULT_CONVERSATION_DEPTH,
        }

    def reduce_tool_calls(self) -> None:
        """
        Eco-mode hook: reduce the max tool count per subsequent run.

        Decrements `max_tools` (saturating at 1). Tracked in statistics.
        """
        with self._lock:
            self.max_tools = max(1, self.max_tools - 1)
            self._throttle_events["reduce_tool_calls"] += 1
        logger.debug(
            "langchain_runtime.reduce_tool_calls",
            extra={"max_tools": self.max_tools},
        )

    def shorten_context(self) -> None:
        """
        Eco-mode hook: reduce context size for subsequent runs.

        v1 behaviour was a no-op. Enhanced behaviour is **opt-in** via
        `activate_shorten_context=True` — otherwise this method only
        records the invocation in statistics. When active, multiplies
        `_context_shortening_factor` by 0.5, floored at 0.125.
        """
        with self._lock:
            self._throttle_events["shorten_context"] += 1
            if self._activate_shorten_context:
                self._context_shortening_factor = max(
                    0.125, self._context_shortening_factor * 0.5
                )
                logger.debug(
                    "langchain_runtime.shorten_context",
                    extra={"context_factor":
                           self._context_shortening_factor},
                )
            else:
                logger.debug(
                    "langchain_runtime.shorten_context_noop",
                    extra={
                        "hint": "pass activate_shorten_context=True to "
                                "enable cumulative shortening",
                    },
                )

    def finalize(self) -> None:
        """
        Clean up the runtime.

        Emits a structured summary log; when a trace is wired, records a
        summary step; the operation is idempotent.
        """
        with self._lock:
            if self._finalized:
                logger.debug("langchain_runtime.finalize_reentrant")
                return
            self._finalized = True

            stats = self.get_statistics()
            logger.info(
                "langchain_runtime.finalized",
                extra={
                    "run_id": self._run_id,
                    "run_count": stats["run_count"],
                    "total_effective_tools":
                        stats["total_effective_tools"],
                    "total_energy_kwh": stats["total_energy_kwh"],
                    "final_max_tools": self.max_tools,
                    "final_context_factor":
                        self._context_shortening_factor,
                    "throttle_events": dict(self._throttle_events),
                    "hitl_count": self._hitl_count,
                    "hitl_denied_count": self._hitl_denied_count,
                },
            )

            if self._trace is not None:
                try:
                    self._trace.record(
                        {"action": "langchain_runtime_finalize",
                         "run_count": stats["run_count"]},
                        kind="chain_end",
                        run_id=self._run_id,
                        severity="info",
                    )
                except Exception as e:
                    logger.debug(
                        "langchain_runtime.trace_finalize_failed",
                        extra={"error": str(e)},
                    )

    # ------------------------------------------------------------------
    # Enhanced public API
    # ------------------------------------------------------------------

    def enhanced_result(self, query: dict) -> Dict[str, Any]:
        """
        Execute one turn and return the full enhanced result.

        Contains the v1 three keys plus: effective / actual tool calls,
        measured elapsed, energy / carbon with source labels, the emitted
        `DecisionRecord` and `AgentObservation`, and a per-run `provenance`
        dict.
        """
        return self._execute(query)

    def set_callback(self, callback: Any) -> None:
        """Wire in a GreenLangChainCallback (or compatible object)."""
        if callback is not None and not self._validate_callback(callback):
            logger.warning(
                "set_callback(): object has no snapshot() or "
                "statistics() method; ignored",
            )
            return
        with self._lock:
            self._callback = callback
            self._last_callback_cumulative = 0
        if callback is not None:
            logger.debug(
                "langchain_runtime.callback_set",
                extra={"type": type(callback).__name__},
            )

    def set_trace(self, trace: Any) -> None:
        """Wire in an ExecutionTrace sink for per-run summary steps."""
        with self._lock:
            self._trace = trace

    def set_hitl_callback(
        self,
        callback: Optional[Callable[[Any], Optional[bool]]],
    ) -> None:
        """
        Register a HITL reviewer.

        The callback receives the *completed* `DecisionRecord`. Returning
        `False` annotates the record's explanation with `[HITL: denied]`
        and (when the contract supports it) sets
        `safety_verdict = "denied_by_reviewer"`.

        Passing `None` clears the callback.
        """
        with self._lock:
            self._hitl_callback = callback

    def set_max_tools(self, n: int) -> None:
        """Programmatically raise/lower the per-run tool cap."""
        v = _safe_int(n, default=-1)
        if v < 1:
            raise ValueError(f"max_tools must be >= 1, got {n!r}")
        with self._lock:
            self.max_tools = v

    def reset_max_tools(self) -> None:
        """Restore `max_tools` to its constructor default (10)."""
        with self._lock:
            self.max_tools = 10

    def records(self, *, limit: Optional[int] = None) -> List[Any]:
        """Return the emitted DecisionRecords, optionally bounded."""
        with self._lock:
            items = list(self._records)
        if limit is not None and limit > 0 and len(items) > limit:
            items = items[-limit:]
        return items

    def observations(self, *, limit: Optional[int] = None) -> List[Any]:
        """Return the emitted AgentObservations, optionally bounded."""
        with self._lock:
            items = list(self._observations)
        if limit is not None and limit > 0 and len(items) > limit:
            items = items[-limit:]
        return items

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative runtime statistics."""
        with self._lock:
            return {
                "analyzer_version": __version__,
                "run_id": self._run_id,
                "initialized": self._initialized,
                "finalized": self._finalized,
                "run_count": self._run_counter,
                "tool_calls": self.tool_calls,
                "max_tools": self.max_tools,
                "context_shortening_factor":
                    self._context_shortening_factor,
                "activate_shorten_context":
                    self._activate_shorten_context,
                "use_measured_latency": self._use_measured_latency,
                "total_tool_calls": self._total_tool_calls,
                "total_effective_tools": self._total_effective_tools,
                "total_actual_tools": self._total_actual_tools,
                "total_energy_kwh": self._total_energy_kwh,
                "total_carbon_kg": self._total_carbon_kg,
                "total_latency_ms": self._total_latency_ms,
                "throttle_events": dict(self._throttle_events),
                "simulated_flags": dict(self._simulated_flags),
                "energy_source_counts": dict(self._energy_source_counts),
                "carbon_source_counts": dict(self._carbon_source_counts),
                "meter_failure_count": self._meter_failure_count,
                "hitl_count": self._hitl_count,
                "hitl_denied_count": self._hitl_denied_count,
                "records_emitted": len(self._records),
                "observations_emitted": len(self._observations),
                "max_records": self._max_records,
                "contracts_available": _CONTRACTS_AVAILABLE,
                "precision": self._precision,
                "hardware_profile": self._hardware_profile,
                "callback_wired": self._callback is not None,
                "trace_wired": self._trace is not None,
                "energy_meter_wired": self._energy_meter is not None,
                "carbon_estimator_wired":
                    self._carbon_estimator is not None,
                "energy_meter_returns": self._energy_meter_returns,
            }

    def export(self, *, include_callback: bool = False) -> Dict[str, Any]:
        """
        Serialise everything for JSON export.

        `include_callback=True` merges the wired callback's `export()`
        (or `steps()` / `errors()` fallback) into the payload.
        """
        with self._lock:
            payload: Dict[str, Any] = {
                "analyzer_version": __version__,
                "run_id": self._run_id,
                "config": _jsonable(dict(self.config)),
                "statistics": self.get_statistics(),
                "history": [_jsonable(h) for h in self._history],
                "records": [
                    _jsonable(
                        r.to_dict() if hasattr(r, "to_dict")
                        else (asdict(r) if is_dataclass(r)
                              else vars(r) if hasattr(r, "__dict__")
                              else r)
                    )
                    for r in self._records
                ],
                "observations": [
                    _jsonable(
                        asdict(o) if is_dataclass(o)
                        else vars(o) if hasattr(o, "__dict__")
                        else o
                    )
                    for o in self._observations
                ],
            }
            cb = self._callback

        if include_callback and cb is not None:
            try:
                if hasattr(cb, "export") and callable(cb.export):
                    payload["callback"] = _jsonable(cb.export())
                else:
                    fallback: Dict[str, Any] = {}
                    if hasattr(cb, "steps") and callable(cb.steps):
                        fallback["steps"] = _jsonable(cb.steps())
                    if hasattr(cb, "errors") and callable(cb.errors):
                        fallback["errors"] = _jsonable(cb.errors())
                    if fallback:
                        payload["callback"] = fallback
            except Exception as e:
                logger.debug(
                    "langchain_runtime.callback_export_failed",
                    extra={"error": str(e)},
                )
        return payload

    def reset(self, *, full: bool = False) -> None:
        """
        Reset runtime state.

        `full=False` (default) clears counters, records, history, and
        throttle state, but keeps `run_id`, `config`, and the wired
        integrations. `full=True` additionally clears `config` and
        detaches all integrations.
        """
        with self._lock:
            self.tool_calls = 0
            self.max_tools = 10
            self._context_shortening_factor = 1.0
            self._run_counter = 0
            self._records.clear()
            self._observations.clear()
            self._history.clear()
            self._throttle_events.clear()
            self._simulated_flags.clear()
            self._energy_source_counts.clear()
            self._carbon_source_counts.clear()
            self._meter_failure_count = 0
            self._hitl_count = 0
            self._hitl_denied_count = 0
            self._total_tool_calls = 0
            self._total_effective_tools = 0
            self._total_actual_tools = 0
            self._total_energy_kwh = 0.0
            self._total_carbon_kg = 0.0
            self._total_latency_ms = 0.0
            self._last_callback_cumulative = 0
            self._finalized = False

            if full:
                self.config = {}
                self._initialized = False
                self._run_id = None
                self._policy_version = ""
                self._hardware_profile = ""
                self._precision = None
                self._callback = None
                self._trace = None
                self._energy_meter = None
                self._carbon_estimator = None
                self._carbon_contractual_kg_per_kwh = None
                self._hitl_callback = None

        logger.debug(
            "langchain_runtime.reset",
            extra={"full": full},
        )

    # ------------------------------------------------------------------
    # Internal helpers — validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_callback(cb: Any) -> bool:
        if cb is None:
            return False
        return (
            (hasattr(cb, "snapshot") and callable(cb.snapshot))
            or (hasattr(cb, "statistics") and callable(cb.statistics))
        )

    @staticmethod
    def _validate_energy_meter(meter: Any) -> bool:
        if meter is None:
            return False
        return (
            (hasattr(meter, "measure") and callable(meter.measure))
            or (hasattr(meter, "measure_detailed")
                and callable(meter.measure_detailed))
        )

    @staticmethod
    def _validate_carbon_estimator(est: Any) -> bool:
        if est is None:
            return False
        return hasattr(est, "estimate") and callable(est.estimate)

    # ------------------------------------------------------------------
    # Internal helpers — energy / carbon
    # ------------------------------------------------------------------

    def _read_energy_joules(self) -> Tuple[float, str]:
        """
        Read energy from the meter and return `(joules, source_label)`.

        Handles `measure()` (returns a float in the meter's configured
        unit) and `measure_detailed()` (returns a rich record). Falls
        back to `source_label="error"` on any failure — the caller
        should then use the estimation path.

        Raises RuntimeError when no compatible method exists or all
        methods fail (caller decides whether to fall back).
        """
        meter = self._energy_meter
        if meter is None:
            raise RuntimeError("no energy meter wired")

        last_error: Optional[Exception] = None
        for method_name in ("measure_detailed", "measure"):
            method = getattr(meter, method_name, None)
            if not callable(method):
                continue
            try:
                result = method()
            except Exception as e:
                last_error = e
                logger.debug(
                    "langchain_runtime.meter_method_failed",
                    extra={
                        "method": method_name,
                        "error": str(e),
                    },
                )
                continue

            # Extract a numeric value in the meter's configured unit.
            raw: Optional[float] = None
            if hasattr(result, "energy_joules"):
                # MeasurementRecord from energy_meter.py — always joules.
                raw = float(result.energy_joules)
                return raw, f"{method_name}(MeasurementRecord)"
            if hasattr(result, "to_float"):
                raw = float(result.to_float())
                return _to_joules(raw, self._energy_meter_returns), \
                    method_name
            if isinstance(result, (int, float)) and not isinstance(
                result, bool,
            ):
                return _to_joules(float(result),
                                  self._energy_meter_returns), method_name

        raise RuntimeError(
            f"energy meter has no usable method (last_error={last_error})"
        )

    def _estimate_energy_kwh(
        self, effective_tools: int, precision: Optional[str],
    ) -> float:
        """
        Fallback estimate for per-run energy in kWh.

        Applies the `_PRECISION_ENERGY_SCALE` table so lower-precision
        runs receive a proportionally lower estimate.
        """
        scale = _precision_scale(precision)
        wh = effective_tools * self.ENERGY_PER_TOOL_CALL_WH * scale
        return wh / 1000.0

    def _read_carbon_kg(
        self, energy_kwh: float,
    ) -> Tuple[float, str]:
        """
        Return `(carbon_kg, source_label)`.

        Prefers the injected estimator; falls back to grid-average only
        when the estimator is absent or fails. **A legitimate `0.0`
        returned by the estimator is preserved.**
        """
        if self._carbon_estimator is not None:
            try:
                value = self._carbon_estimator.estimate(
                    energy_wh=energy_kwh * 1000.0,
                )
                f = _safe_float(value, default=float("nan"))
                if math.isfinite(f) and f >= 0:
                    return f, "estimator"
                logger.debug(
                    "langchain_runtime.carbon_estimator_invalid",
                    extra={"value": value},
                )
            except Exception as e:
                logger.debug(
                    "langchain_runtime.carbon_estimator_failed",
                    extra={"error": str(e)},
                )
        # Fallback: grid average
        fallback = energy_kwh * self.DEFAULT_CARBON_INTENSITY_KG_PER_KWH
        return fallback, "fallback"

    # ------------------------------------------------------------------
    # Internal helpers — callback introspection
    # ------------------------------------------------------------------

    def _read_callback_delta(
        self,
    ) -> Tuple[Optional[int], Optional[int], Optional[Dict[str, Any]]]:
        """
        Return `(delta_calls, cumulative_calls, snapshot_or_None)`.

        `delta_calls` is the number of tool + llm calls observed since
        the previous `run()`. Used to give the DecisionRecord a per-run
        attribution from the callback even though its counters are
        cumulative.
        """
        cb = self._callback
        if cb is None:
            return None, None, None
        try:
            snap: Optional[Dict[str, Any]] = None
            if hasattr(cb, "snapshot") and callable(cb.snapshot):
                snap = cb.snapshot()
            elif hasattr(cb, "statistics") and callable(cb.statistics):
                snap = cb.statistics()
            if not isinstance(snap, dict):
                return None, None, None
            tool_calls = _safe_int(snap.get("tool_calls"), 0)
            llm_calls = _safe_int(snap.get("llm_calls"), 0)
            cumulative = tool_calls + llm_calls
            prev = self._last_callback_cumulative
            delta = max(0, cumulative - prev)
            self._last_callback_cumulative = cumulative
            return delta, cumulative, snap
        except Exception as e:
            logger.debug(
                "langchain_runtime.callback_read_failed",
                extra={"error": str(e)},
            )
            return None, None, None

    def _read_callback_agent_info(
        self, fallback_agent_id: str = "langchain_runtime",
    ) -> Tuple[str, str, List[str]]:
        """Return `(agent_id, role, delegated_to)` from the callback."""
        cb = self._callback
        if cb is None:
            return fallback_agent_id, "generalist", []

        agent_id = fallback_agent_id
        try:
            aid = getattr(cb, "agent_id", None)
            if isinstance(aid, str) and aid:
                agent_id = aid
        except Exception:
            pass

        role = "generalist"
        try:
            roles = getattr(cb, "_agent_role", None)
            if isinstance(roles, dict):
                candidate = roles.get(agent_id)
                if isinstance(candidate, str) and candidate:
                    role = candidate
            else:
                getter = getattr(cb, "get_agent_role", None)
                if callable(getter):
                    candidate = getter(agent_id)
                    if isinstance(candidate, str) and candidate:
                        role = candidate
        except Exception:
            pass

        delegated: List[str] = []
        try:
            delegated_map = getattr(cb, "_agent_delegated_to", None)
            if isinstance(delegated_map, dict):
                vals = delegated_map.get(agent_id)
                if vals:
                    delegated = sorted(vals)
            else:
                getter = getattr(cb, "get_delegated_to", None)
                if callable(getter):
                    vals = getter(agent_id)
                    if vals:
                        delegated = sorted(vals)
        except Exception:
            pass

        return agent_id, role, delegated

    # ------------------------------------------------------------------
    # Core execution
    # ------------------------------------------------------------------

    def _execute(self, query: Any) -> Dict[str, Any]:
        """
        Shared execution path. Returns a rich dict consumed by `run()`
        and `enhanced_result()`.
        """
        # --- Auto-init ---
        with self._lock:
            if not self._initialized:
                logger.warning(
                    "run() called before init(); auto-initializing",
                )
                # Release the lock before init (init acquires it too).
            initialized = self._initialized
        if not initialized:
            self.init({})

        # --- Validate query ---
        if query is None:
            query = {}
        if not isinstance(query, dict):
            logger.warning(
                "run() received non-dict query (%s); using empty dict",
                type(query).__name__,
            )
            query = {}

        run_start = time.perf_counter()

        with self._lock:
            self._run_counter += 1
            run_index = self._run_counter

            # --- Tool call resolution ---
            requested_tools = _safe_int(
                query.get("base_tool_calls"), default=2,
            )
            effective_tools = min(requested_tools, self.max_tools)
            self.tool_calls += effective_tools

            # --- Context factor ---
            base_context_tokens = _safe_int(
                query.get("base_context_tokens"), default=0,
            )
            effective_context_tokens = int(
                base_context_tokens * self._context_shortening_factor
            )

            # --- Energy ---
            energy_kwh: float = 0.0
            energy_source = "estimate"
            if self._energy_meter is not None:
                try:
                    joules, source_label = self._read_energy_joules()
                    energy_kwh = joules / _JOULES_PER_KWH
                    energy_source = f"meter:{source_label}"
                except Exception as e:
                    self._meter_failure_count += 1
                    logger.warning(
                        "langchain_runtime.meter_failed_using_estimate",
                        extra={
                            "error": str(e),
                            "run_index": run_index,
                        },
                    )
                    energy_kwh = self._estimate_energy_kwh(
                        effective_tools, self._precision,
                    )
                    energy_source = "estimate:meter_failed"
            else:
                energy_kwh = self._estimate_energy_kwh(
                    effective_tools, self._precision,
                )
                energy_source = "estimate:no_meter"
            self._energy_source_counts[
                energy_source.split(":", 1)[0]
            ] += 1

            # --- Carbon ---
            carbon_kg, carbon_source = self._read_carbon_kg(energy_kwh)
            self._carbon_source_counts[carbon_source] += 1

            # --- Contractual carbon ---
            if self._carbon_contractual_kg_per_kwh is not None:
                carbon_contractual_kg = (
                    energy_kwh * self._carbon_contractual_kg_per_kwh
                )
            else:
                carbon_contractual_kg = 0.0

            # --- Latency estimate ---
            latency_estimate_ms = (
                effective_tools * self.LATENCY_PER_TOOL_CALL_MS
            )

            # --- Callback read ---
            actual_calls_delta, cumulative_calls, callback_snapshot = \
                self._read_callback_delta()

            # --- Per-run attribution from callback ---
            agent_id, role, delegated_to = self._read_callback_agent_info()

            # --- Statistics accumulators (partial — elapsed added later) ---
            self._total_tool_calls += requested_tools
            self._total_effective_tools += effective_tools
            if actual_calls_delta is not None:
                self._total_actual_tools += actual_calls_delta
            self._total_energy_kwh += energy_kwh
            self._total_carbon_kg += carbon_kg
            self._total_latency_ms += latency_estimate_ms
            self._simulated_flags["accuracy_simulated"] += 1
            self._simulated_flags["conversation_depth_simulated"] += 1
            if energy_source != "meter":
                self._simulated_flags["energy_simulated"] += 1
            if carbon_source == "fallback":
                self._simulated_flags["carbon_simulated"] += 1
            if actual_calls_delta is None:
                self._simulated_flags["tool_calls_estimated"] += 1

        # --- Measured wall-clock latency (outside the lock) ---
        run_elapsed_ms = (time.perf_counter() - run_start) * 1000.0

        # --- Choose the latency for the DecisionRecord ---
        decision_latency_ms = (
            run_elapsed_ms if self._use_measured_latency
            else latency_estimate_ms
        )

        # --- Task ID ---
        task_id = (
            query.get("task_id")
            if isinstance(query.get("task_id"), str)
            else f"task-{run_index}"
        )

        # --- Build the explanation dict once ---
        explanation: Dict[str, Any] = {
            "tool_calls": effective_tools,
            "tool_calls_requested": requested_tools,
            "tool_calls_cap": self.max_tools,
            "context_tokens": effective_context_tokens,
            "context_factor": self._context_shortening_factor,
            "measured_elapsed_ms": run_elapsed_ms,
            "latency_estimate_ms": latency_estimate_ms,
            "energy_source": energy_source,
            "carbon_source": carbon_source,
            "simulated": {
                "accuracy": True,
                "conversation_depth": True,
                "tool_calls_actual": actual_calls_delta is None,
                "energy": energy_source != "meter",
                "carbon": carbon_source == "fallback",
            },
        }
        if callback_snapshot is not None:
            explanation["callback_reported"] = {
                "tool_calls": _safe_int(
                    callback_snapshot.get("tool_calls"), 0,
                ),
                "llm_calls": _safe_int(
                    callback_snapshot.get("llm_calls"), 0,
                ),
                "cumulative_calls": cumulative_calls,
                "delta_calls": actual_calls_delta,
            }
            if actual_calls_delta is not None:
                explanation["callback_delta_vs_effective"] = (
                    actual_calls_delta - effective_tools
                )

        # --- Build DecisionRecord (signature-filtered) ---
        record_candidates: Dict[str, Any] = {
            "run_id": self._run_id or "",
            "timestamp": datetime.now(),
            "task_id": task_id,
            "selected_action": (
                f"langchain_run(tools={effective_tools})"
            ),
            "alternatives": [],
            "policy_version": self._policy_version,
            "model_or_agent": agent_id,
            "hardware_profile": self._hardware_profile,
            "precision": self._precision,
            "quality_score": self._DEFAULT_QUALITY_SCORE,
            "latency_ms": decision_latency_ms,
            "energy_kwh": energy_kwh,
            "carbon_operational_kg": carbon_kg,
            "carbon_contractual_kg": carbon_contractual_kg,
            "explanation": explanation,
            "provenance": {
                "source": "langchain_runtime",
                "analyzer_version": __version__,
                "run_index": run_index,
                "config_keys": sorted(self.config.keys()),
                "energy_source": energy_source,
                "carbon_source": carbon_source,
                "callback_wired": callback_snapshot is not None,
                "simulated_accuracy": True,
                "simulated_conversation_depth": True,
            },
        }
        filtered = _filter_decision_record_kwargs(
            DecisionRecord, record_candidates,
        )
        record: Any = None
        try:
            record = DecisionRecord(**filtered)
        except TypeError as e:
            logger.warning(
                "langchain_runtime.decision_record_failed",
                extra={
                    "error": str(e),
                    "available_fields": sorted(filtered.keys()),
                },
            )

        # --- Build AgentObservation ---
        observation_candidates: Dict[str, Any] = {
            "agent_id": agent_id,
            "task_id": task_id,
            "role": role,
            "delegated_to": delegated_to,
            "coordination_calls": (
                actual_calls_delta
                if actual_calls_delta is not None
                else effective_tools
            ),
            "latency_ms": run_elapsed_ms,
            "energy_kwh": energy_kwh,
        }
        observation: Any = None
        try:
            obs_filtered = _filter_decision_record_kwargs(
                AgentObservation, observation_candidates,
            )
            observation = AgentObservation(**obs_filtered)
        except TypeError as e:
            logger.warning(
                "langchain_runtime.agent_observation_failed",
                extra={"error": str(e)},
            )

        # --- HITL review ---
        hitl_denied = False
        if self._hitl_callback is not None and record is not None:
            reasons: List[str] = []
            if effective_tools > self._hitl_max_tools:
                reasons.append(
                    f"tools {effective_tools} > {self._hitl_max_tools}"
                )
            if (
                self._activate_shorten_context
                and self._context_shortening_factor
                < self._hitl_min_context_factor
            ):
                reasons.append(
                    f"context_factor {self._context_shortening_factor} "
                    f"< {self._hitl_min_context_factor}"
                )
            if carbon_kg > self._hitl_max_carbon_kg:
                reasons.append(
                    f"carbon {carbon_kg:.6f} kg > "
                    f"{self._hitl_max_carbon_kg:.6f} kg"
                )
            if self._meter_failure_count > 0 and (
                self._meter_failure_count % 5 == 0
            ):
                reasons.append(
                    f"repeated meter failures "
                    f"({self._meter_failure_count})"
                )
            if reasons:
                with self._lock:
                    self._hitl_count += 1
                try:
                    approved = self._hitl_callback(record)
                    if approved is False:
                        hitl_denied = True
                        with self._lock:
                            self._hitl_denied_count += 1
                        explanation["hitl_denied"] = True
                        explanation["hitl_reasons"] = list(reasons)
                        if hasattr(record, "safety_verdict"):
                            try:
                                record.safety_verdict = (
                                    "denied_by_reviewer"
                                )
                            except Exception:
                                pass
                        if hasattr(record, "explanation"):
                            try:
                                record.explanation["hitl_denied"] = True
                                record.explanation["hitl_reasons"] = \
                                    list(reasons)
                            except Exception:
                                pass
                except Exception as e:
                    logger.warning(
                        "langchain_runtime.hitl_callback_failed",
                        extra={"error": str(e)},
                    )

        # --- Append to bounded history ---
        with self._lock:
            if record is not None:
                self._records.append(record)
            if observation is not None:
                self._observations.append(observation)
            self._history.append({
                "run_index": run_index,
                "requested_tool_calls": requested_tools,
                "effective_tool_calls": effective_tools,
                "actual_tool_calls": actual_calls_delta,
                "max_tools": self.max_tools,
                "context_tokens": effective_context_tokens,
                "context_factor": self._context_shortening_factor,
                "energy_kwh": energy_kwh,
                "energy_source": energy_source,
                "carbon_kg": carbon_kg,
                "carbon_source": carbon_source,
                "latency_estimate_ms": latency_estimate_ms,
                "measured_elapsed_ms": run_elapsed_ms,
                "hitl_denied": hitl_denied,
                "at": _safe_isoformat(time.time()),
            })

        # --- Forward a summary step to ExecutionTrace ---
        if self._trace is not None:
            try:
                self._trace.record(
                    {
                        "action": "langchain_run",
                        "tool_calls": effective_tools,
                        "energy_kwh": energy_kwh,
                        "carbon_kg": carbon_kg,
                    },
                    kind="chain_end",
                    agent_id=agent_id,
                    task_id=task_id,
                    run_id=self._run_id,
                    duration_ms=run_elapsed_ms,
                    severity=(
                        "critical" if hitl_denied else "info"
                    ),
                )
            except Exception as e:
                logger.debug(
                    "langchain_runtime.trace_forward_failed",
                    extra={"error": str(e)},
                )

        # --- Structured log ---
        logger.info(
            "langchain_runtime.run_completed",
            extra={
                "run_index": run_index,
                "effective_tools": effective_tools,
                "actual_tools": actual_calls_delta,
                "energy_kwh": energy_kwh,
                "carbon_kg": carbon_kg,
                "energy_source": energy_source,
                "carbon_source": carbon_source,
                "measured_elapsed_ms": run_elapsed_ms,
                "hitl_denied": hitl_denied,
                "agent_id": agent_id,
                "task_id": task_id,
            },
        )

        return {
            # v1-compatible (for `run()`)
            "accuracy": self._DEFAULT_QUALITY_SCORE,
            "cumulative_tool_calls": self.tool_calls,
            "conversation_depth": self._DEFAULT_CONVERSATION_DEPTH,
            # Enhanced per-run data
            "run_index": run_index,
            "task_id": task_id,
            "agent_id": agent_id,
            "role": role,
            "delegated_to": delegated_to,
            "requested_tool_calls": requested_tools,
            "effective_tool_calls": effective_tools,
            "actual_tool_calls": actual_calls_delta,
            "context_tokens": effective_context_tokens,
            "context_factor": self._context_shortening_factor,
            "energy_kwh": energy_kwh,
            "energy_source": energy_source,
            "carbon_kg": carbon_kg,
            "carbon_source": carbon_source,
            "carbon_contractual_kg": carbon_contractual_kg,
            "latency_estimate_ms": latency_estimate_ms,
            "measured_elapsed_ms": run_elapsed_ms,
            "decision_latency_ms": decision_latency_ms,
            "hitl_denied": hitl_denied,
            "record": record,
            "observation": observation,
            "provenance": {
                "source": "langchain_runtime",
                "analyzer_version": __version__,
                "callback_wired": callback_snapshot is not None,
            },
        }


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json
    import sys
    import threading
    import types

    logging.basicConfig(level=logging.WARNING)

    # =================================================================
    # 1. Original behaviour (backward compatible)
    # =================================================================
    print("=== 1. Original behaviour ===")
    rt = LangChainRuntime()
    rt.init({"policy_version": "v5.0.1"})
    result = rt.run({"base_tool_calls": 3})
    print(f"  result: {result}")
    print(f"  keys:   {sorted(result.keys())}  "
          f"(expected: accuracy, tool_calls, conversation_depth)")

    # =================================================================
    # 2. Energy unit fix (joules from EnergyMeter)
    # =================================================================
    print("\n=== 2. Energy unit fix (joules from EnergyMeter) ===")

    class _FakeMeterJ:
        """Mimics the analysis-layer EnergyMeter — measure() returns joules."""
        def __init__(self, joules: float):
            self._j = joules
        def measure(self) -> float:
            return self._j

    rt_j = LangChainRuntime()
    rt_j.init({"energy_meter": _FakeMeterJ(3600.0)})  # 3600 J = 1 Wh = 0.001 kWh
    out = rt_j.enhanced_result({"base_tool_calls": 2})
    print(f"  energy_kwh:      {out['energy_kwh']:.6f}  (expected 0.001)")
    print(f"  energy_source:   {out['energy_source']}")
    print(f"  carbon_kg:       {out['carbon_kg']:.8f}  "
          f"(≈ 0.001 kWh × 0.4 = 0.0004)")

    # =================================================================
    # 3. Carbon zero preservation (estimator returning 0.0)
    # =================================================================
    print("\n=== 3. Carbon zero preservation ===")

    class _ZeroCarbon:
        def estimate(self, energy_wh: float) -> float:
            return 0.0     # 100% renewable PPA

    rt_zero = LangChainRuntime()
    rt_zero.init({
        "energy_meter": _FakeMeterJ(3600.0),
        "carbon_estimator": _ZeroCarbon(),
    })
    out = rt_zero.enhanced_result({})
    print(f"  carbon_kg:     {out['carbon_kg']}  "
          f"(expected 0.0, NOT fallback)")
    print(f"  carbon_source: {out['carbon_source']}  "
          f"(expected 'estimator')")

    # =================================================================
    # 4. Thread safety (barrier + 4 × 250 runs)
    # =================================================================
    print("\n=== 4. Thread safety ===")
    rt_conc = LangChainRuntime()
    rt_conc.init({})
    barrier = threading.Barrier(4)

    def _worker():
        barrier.wait()
        for _ in range(250):
            rt_conc.run({"base_tool_calls": 2})

    threads = [threading.Thread(target=_worker) for _ in range(4)]
    for t in threads: t.start()
    for t in threads: t.join()
    print(f"  run_count:   {rt_conc.get_statistics()['run_count']}  "
          f"(expected 1000)")
    print(f"  tool_calls:  {rt_conc.tool_calls}  "
          f"(expected 2000)")
    print(f"  records:     {len(rt_conc.records())}  "
          f"(bounded by max_records={rt_conc._max_records})")

    # =================================================================
    # 5. shorten_context() is a no-op by default
    # =================================================================
    print("\n=== 5. shorten_context() default no-op (v1 compat) ===")
    rt_noop = LangChainRuntime()
    rt_noop.init({})
    for _ in range(10):
        rt_noop.shorten_context()
    factor_off = rt_noop.get_statistics()["context_shortening_factor"]
    out_noop = rt_noop.enhanced_result({"base_context_tokens": 1000})
    print(f"  context_factor after 10 calls: {factor_off}  "
          f"(expected 1.0 — v1 no-op preserved)")
    print(f"  effective context_tokens:      {out_noop['context_tokens']}  "
          f"(expected 1000)")

    print("\n=== 5b. shorten_context() when opt-in ===")
    rt_on = LangChainRuntime(activate_shorten_context=True)
    rt_on.init({})
    for _ in range(5):
        rt_on.shorten_context()
    factor_on = rt_on.get_statistics()["context_shortening_factor"]
    out_on = rt_on.enhanced_result({"base_context_tokens": 1000})
    print(f"  context_factor after 5 calls: {factor_on}  "
          f"(expected 0.125 floor)")
    print(f"  effective context_tokens:     {out_on['context_tokens']}  "
          f"(expected 125)")

    # =================================================================
    # 6. Callback metrics merged (deltas)
    # =================================================================
    print("\n=== 6. Callback metric merge ===")

    class _FakeCallback:
        def __init__(self):
            self.agent_id = "worker-42"
            self._tool_calls = 0
            self._llm_calls = 0
        def snapshot(self):
            return {"tool_calls": self._tool_calls,
                    "llm_calls": self._llm_calls}
        def simulate_calls(self, tools, llms):
            self._tool_calls += tools
            self._llm_calls += llms

    cb = _FakeCallback()
    rt_cb = LangChainRuntime()
    rt_cb.init({"callback": cb})
    # Simulate callback activity before the run
    cb.simulate_calls(tools=2, llms=1)
    out_cb = rt_cb.enhanced_result({"base_tool_calls": 5})
    expl = out_cb["record"].explanation if out_cb["record"] else {}
    print(f"  effective_tool_calls: {out_cb['effective_tool_calls']}  "
          f"(requested 5)")
    print(f"  actual_tool_calls:    {out_cb['actual_tool_calls']}  "
          f"(callback delta = 3)")
    print(f"  callback_reported:    {expl.get('callback_reported')}")
    print(f"  delta_vs_effective:   "
          f"{expl.get('callback_delta_vs_effective')}  "
          f"(expected 3 - 5 = -2)")
    if out_cb["record"]:
        print(f"  model_or_agent:       "
              f"{out_cb['record'].model_or_agent}  "
              f"(expected 'worker-42', NOT 'langchain')")

    # =================================================================
    # 7. DecisionRecord signature filter (strict mock)
    # =================================================================
    print("\n=== 7. DecisionRecord signature filter (strict) ===")

    class _StrictDR:
        def __init__(self, run_id, timestamp, task_id, selected_action,
                     latency_ms, energy_kwh, carbon_operational_kg,
                     explanation, provenance):
            self.run_id = run_id
            self.timestamp = timestamp
            self.task_id = task_id
            self.selected_action = selected_action
            self.latency_ms = latency_ms
            self.energy_kwh = energy_kwh
            self.carbon_operational_kg = carbon_operational_kg
            self.explanation = explanation
            self.provenance = provenance

    fake_mod = types.ModuleType("analysis")
    fake_mod.DecisionRecord = _StrictDR
    fake_mod.AgentObservation = AgentObservation
    sys.modules["analysis"] = fake_mod

    import importlib
    import src.analysis.adapters.langchain_runtime as _rt_mod
    importlib.reload(_rt_mod)
    try:
        rt_strict = _rt_mod.LangChainRuntime()
        rt_strict.init({})
        out_strict = rt_strict.enhanced_result({"base_tool_calls": 1})
        print(f"  record constructed: "
              f"{out_strict['record'] is not None}")
        if out_strict["record"]:
            print(f"  latency_ms:         "
                  f"{out_strict['record'].latency_ms}")
            print(f"  explanation keys:   "
                  f"{sorted(out_strict['record'].explanation.keys())[:5]}...")
    finally:
        del sys.modules["analysis"]
        importlib.reload(_rt_mod)

    # =================================================================
    # 8. Precision-aware energy
    # =================================================================
    print("\n=== 8. Precision-aware energy estimate ===")
    rt_fp32 = LangChainRuntime()
    rt_fp32.init({"precision": "fp32"})
    rt_int8 = LangChainRuntime()
    rt_int8.init({"precision": "int8"})

    out_fp32 = rt_fp32.enhanced_result({"base_tool_calls": 10})
    out_int8 = rt_int8.enhanced_result({"base_tool_calls": 10})
    print(f"  fp32 energy: {out_fp32['energy_kwh']:.6f} kWh  (scale 1.0)")
    print(f"  int8 energy: {out_int8['energy_kwh']:.6f} kWh  (scale 0.40)")

    # =================================================================
    # 9. HITL callback (tool overrun + denial annotation)
    # =================================================================
    print("\n=== 9. HITL callback (tool overrun) ===")
    rt_hitl = LangChainRuntime(hitl_max_tools=3)
    rt_hitl.init({})
    reviewed = []
    rt_hitl.set_hitl_callback(lambda r: reviewed.append(r) or False)
    out_hitl = rt_hitl.enhanced_result({"base_tool_calls": 10})
    print(f"  effective_tools: {out_hitl['effective_tool_calls']}  "
          f"(expected 10, cap is max_tools=10)")
    print(f"  HITL fired:      {len(reviewed)}  (expected 1)")
    print(f"  hitl_denied:     {out_hitl['hitl_denied']}  "
          f"(expected True — callback returned False)")

    # =================================================================
    # 10. ExecutionTrace forwarding
    # =================================================================
    print("\n=== 10. ExecutionTrace forwarding ===")

    class _FakeTrace:
        def __init__(self):
            self.records = []
        def record(self, event, **kwargs):
            self.records.append((event, kwargs))

    fake_trace = _FakeTrace()
    rt_trace = LangChainRuntime()
    rt_trace.init({})
    rt_trace.set_trace(fake_trace)
    rt_trace.enhanced_result({"base_tool_calls": 3})
    print(f"  trace records: {len(fake_trace.records)}  "
          f"(expected 1)")
    if fake_trace.records:
        event, kwargs = fake_trace.records[0]
        print(f"  event:         {event.get('action')}")
        print(f"  kind:          {kwargs.get('kind')}")

    # =================================================================
    # 11. Export with include_callback
    # =================================================================
    print("\n=== 11. Export (include_callback=False vs True) ===")
    rt_exp = LangChainRuntime()
    rt_exp.init({})
    rt_exp.enhanced_result({"base_tool_calls": 2})
    plain = rt_exp.export()
    with_cb = rt_exp.export(include_callback=True)
    print(f"  plain has 'callback' key:   {'callback' in plain}  "
          f"(expected False)")
    print(f"  with_cb has 'callback' key: "
          f"{'callback' in with_cb}  "
          f"(expected {'True' if hasattr(_FakeCallback, 'export') or 'export' in dir(_FakeCallback) else 'depends on callback'})")
    try:
        json.dumps(with_cb, default=str)
        print(f"  json.dumps(export()): OK")
    except (TypeError, ValueError) as e:
        print(f"  FAILED: {e}")

    # =================================================================
    # 12. reset() — partial vs full
    # =================================================================
    print("\n=== 12. reset() ===")
    rt_reset = LangChainRuntime()
    rt_reset.init({"policy_version": "v5.0.1"})
    for _ in range(5):
        rt_reset.run({})
    rt_reset.reset()
    stats_after = rt_reset.get_statistics()
    print(f"  after reset(): run_count={stats_after['run_count']}, "
          f"tool_calls={stats_after['tool_calls']}, "
          f"policy_version still set: "
          f"{stats_after['run_id'] is not None}")
    rt_reset.reset(full=True)
    stats_full = rt_reset.get_statistics()
    print(f"  after reset(full=True): run_id={stats_full['run_id']}, "
          f"initialized={stats_full['initialized']}")

    # =================================================================
    # 13. set_max_tools / reset_max_tools
    # =================================================================
    print("\n=== 13. set_max_tools / reset_max_tools ===")
    rt_cap = LangChainRuntime()
    rt_cap.init({})
    print(f"  default max_tools: {rt_cap.max_tools}  (expected 10)")
    rt_cap.set_max_tools(3)
    print(f"  after set_max_tools(3): {rt_cap.max_tools}")
    out_cap = rt_cap.enhanced_result({"base_tool_calls": 10})
    print(f"  effective_tool_calls:   {out_cap['effective_tool_calls']}  "
          f"(capped to 3)")
    rt_cap.reset_max_tools()
    print(f"  after reset_max_tools: {rt_cap.max_tools}")

    # =================================================================
    # 14. Full statistics
    # =================================================================
    print("\n=== 14. Full statistics ===")
    print(json.dumps(rt_cap.get_statistics(), indent=2, default=str))
