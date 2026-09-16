# src/analysis/adapters/langchain_runtime.py

"""
LangChain Runtime Adapter with real callback-style counters (Enhanced)
======================================================================

Translates LangChain runtime lifecycle events into the analysis layer's
shared contracts (`DecisionRecord`, `AgentObservation`, `CallbackStep`).

Original API preserved:
    runtime = LangChainRuntime()
    runtime.init(config)
    result = runtime.run(query)
    runtime.reduce_tool_calls()
    runtime.shorten_context()
    runtime.finalize()

Enhanced API:
    runtime.set_callback(cb)          # wire in GreenLangChainCallback
    runtime.records()                 # -> list[DecisionRecord]
    runtime.observations()            # -> list[AgentObservation]
    runtime.get_statistics()
    runtime.export()
"""

from __future__ import annotations

import logging
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Callable, Deque, Dict, List, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# Shared contracts — soft import with fallback
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
# Enhanced LangChainRuntime
# =============================================================================

class LangChainRuntime:
    """
    Enhanced LangChain runtime adapter.

    Backward-compatible with the original five-method API. Adds real
    callback integration, actual throttle behavior, DecisionRecord
    emission, statistics, and export.
    """

    # Conservative energy per tool call (Wh); scaled by precision if set
    ENERGY_PER_TOOL_CALL_WH = 0.04
    # Baseline latency per tool call (ms)
    LATENCY_PER_TOOL_CALL_MS = 12.0

    def __init__(self):
        # --- Original state ---
        self.tool_calls: int = 0
        self.max_tools: int = 10

        # --- Enhancement: runtime state ---
        self.config: Dict[str, Any] = {}
        self._initialized: bool = False
        self._finalized: bool = False

        # --- Enhancement: throttle state ---
        self._context_shortening_factor: float = 1.0

        # --- Enhancement: identity & telemetry ---
        self._run_counter: int = 0
        self._run_id: Optional[str] = None
        self._policy_version: str = ""
        self._hardware_profile: str = ""
        self._precision: Optional[str] = None
        self._records: List[Any] = []
        self._observations: List[Any] = []
        self._history: List[Dict[str, Any]] = []

        # --- Enhancement: callback integration ---
        self._callback: Any = None

        # --- Enhancement: injected meters ---
        self._energy_meter: Any = None
        self._carbon_estimator: Any = None

        # --- Enhancement: statistics ---
        self._total_tool_calls: int = 0
        self._total_energy_kwh: float = 0.0
        self._total_latency_ms: float = 0.0
        self._throttle_events: Counter = Counter()
        self._simulated_flags: Counter = Counter()

        logger.debug("Enhanced LangChainRuntime initialized")

    # ------------------------------------------------------------------
    # Original public API
    # ------------------------------------------------------------------

    def init(self, config: dict) -> None:
        """
        Initialize the runtime.

        Original behavior: stores the config.
        Enhanced: also extracts identity, telemetry hints, and validates.
        """
        # --- Input validation ---
        if config is None:
            config = {}
        if not isinstance(config, dict):
            logger.warning(
                f"init() received non-dict config ({type(config).__name__}); "
                "using empty dict"
            )
            config = {}

        # --- Original storage ---
        self.config = config

        # --- Enhancement: extract identity ---
        self._policy_version = str(config.get("policy_version", ""))
        self._hardware_profile = str(config.get("hardware_profile", ""))
        self._precision = config.get("precision")
        self._energy_meter = config.get("energy_meter")
        self._carbon_estimator = config.get("carbon_estimator")
        self._run_id = str(
            config.get("run_id") or f"run-{uuid.uuid4().hex[:8]}"
        )

        # --- Enhancement: allow callback injection via config ---
        cb = config.get("callback")
        if cb is not None:
            self._callback = cb

        # --- Enhancement: reset throttle state ---
        self._context_shortening_factor = 1.0

        self._initialized = True
        self._finalized = False

        logger.info(
            f"LangChainRuntime initialized (run_id={self._run_id}, "
            f"precision={self._precision}, "
            f"hardware={self._hardware_profile}, "
            f"callback={'yes' if cb is not None else 'no'})"
        )

    def run(self, query: dict) -> dict:
        """
        Execute a LangChain runtime turn.

        Backward-compatible return shape preserved:
            {"accuracy": float, "tool_calls": int, "conversation_depth": int}

        Enhanced: honors throttle state, tracks real metrics, emits a
        DecisionRecord per run, and flags simulated values via provenance.
        """
        # --- Input validation ---
        if query is None:
            query = {}
        if not isinstance(query, dict):
            logger.warning(
                f"run() received non-dict query ({type(query).__name__}); "
                "using empty dict"
            )
            query = {}

        # --- Auto-init if not initialized ---
        if not self._initialized:
            logger.warning("run() called before init(); auto-initializing")
            self.init({})

        self._run_counter += 1
        run_start = time.perf_counter()

        # --- Original tool-call logic (preserved shape) ---
        # Use query's hint if provided; fall back to the original 2 for
        # backward compatibility.
        requested_tools = int(query.get("base_tool_calls", 2))
        effective_tools = min(requested_tools, self.max_tools)
        self.tool_calls += effective_tools

        # --- Enhancement: context shortening factor applied ---
        base_context_tokens = int(query.get("base_context_tokens", 0))
        effective_context_tokens = int(
            base_context_tokens * self._context_shortening_factor
        )

        # --- Enhancement: energy estimation ---
        if self._energy_meter is not None:
            try:
                if hasattr(self._energy_meter, "measure"):
                    energy_kwh = float(self._energy_meter.measure())
                else:
                    energy_kwh = (
                        effective_tools * self.ENERGY_PER_TOOL_CALL_WH
                    ) / 1000.0
            except Exception as e:
                logger.debug(f"Energy meter failed: {e}; falling back")
                energy_kwh = (
                    effective_tools * self.ENERGY_PER_TOOL_CALL_WH
                ) / 1000.0
        else:
            energy_kwh = (
                effective_tools * self.ENERGY_PER_TOOL_CALL_WH
            ) / 1000.0

        latency_ms = effective_tools * self.LATENCY_PER_TOOL_CALL_MS

        # --- Enhancement: carbon estimation ---
        carbon_kg = 0.0
        if self._carbon_estimator is not None:
            try:
                if hasattr(self._carbon_estimator, "estimate"):
                    carbon_kg = float(
                        self._carbon_estimator.estimate(
                            energy_wh=energy_kwh * 1000.0,
                        )
                    )
            except Exception as e:
                logger.debug(f"Carbon estimator failed: {e}")
        if carbon_kg == 0.0:
            # Default fallback: 400 gCO2/kWh
            carbon_kg = energy_kwh * 0.4

        # --- Enhancement: callback snapshot (if wired) ---
        callback_snapshot: Optional[Dict[str, Any]] = None
        if self._callback is not None:
            try:
                if hasattr(self._callback, "snapshot"):
                    callback_snapshot = self._callback.snapshot()
                elif hasattr(self._callback, "statistics"):
                    callback_snapshot = self._callback.statistics()
            except Exception as e:
                logger.debug(f"Callback snapshot failed: {e}")

        # --- Enhancement: statistics ---
        self._total_tool_calls += effective_tools
        self._total_energy_kwh += energy_kwh
        self._total_latency_ms += latency_ms
        self._simulated_flags["accuracy_simulated"] += 1
        self._simulated_flags["conversation_depth_simulated"] += 1

        # --- Enhancement: emit DecisionRecord ---
        record = DecisionRecord(
            run_id=self._run_id or "",
            timestamp=datetime.now(),
            task_id=str(query.get("task_id", f"task-{self._run_counter}")),
            selected_action=f"langchain_run(tools={effective_tools})",
            alternatives=[],
            policy_version=self._policy_version,
            model_or_agent="langchain",
            hardware_profile=self._hardware_profile,
            precision=self._precision,
            quality_score=0.8,  # original fabricated value
            latency_ms=latency_ms,
            energy_kwh=energy_kwh,
            carbon_operational_kg=carbon_kg,
            explanation={
                "tool_calls": effective_tools,
                "tool_calls_requested": requested_tools,
                "max_tools": self.max_tools,
                "context_tokens": effective_context_tokens,
                "context_factor": self._context_shortening_factor,
                "callback_snapshot": callback_snapshot,
                # Explicitly flag fabricated values
                "simulated": {
                    "accuracy": True,
                    "conversation_depth": True,
                },
            },
            provenance={
                "source": "langchain_runtime",
                "config_keys": sorted(self.config.keys()),
                "simulated_accuracy": True,
                "simulated_conversation_depth": True,
            },
        )
        self._records.append(record)

        # --- Enhancement: emit AgentObservation ---
        self._observations.append(AgentObservation(
            agent_id="langchain_runtime",
            task_id=record.task_id,
            role="generalist",
            delegated_to=[],
            coordination_calls=effective_tools,
            latency_ms=latency_ms,
            energy_kwh=energy_kwh,
        ))

        # --- Enhancement: history ---
        run_elapsed_ms = (time.perf_counter() - run_start) * 1000.0
        self._history.append({
            "run_index": self._run_counter,
            "tool_calls": effective_tools,
            "max_tools": self.max_tools,
            "context_tokens": effective_context_tokens,
            "context_factor": self._context_shortening_factor,
            "energy_kwh": energy_kwh,
            "carbon_kg": carbon_kg,
            "latency_ms": latency_ms,
            "run_elapsed_ms": run_elapsed_ms,
            "at": record.timestamp.isoformat(),
        })

        # --- Original return shape preserved ---
        return {
            "accuracy": 0.8,
            "tool_calls": self.tool_calls,
            "conversation_depth": 1,
        }

    def reduce_tool_calls(self) -> None:
        """
        Eco-mode hook: reduce the max tool count per subsequent run.

        Original behavior: decrements `self.max_tools` (saturating at 1).
        Enhanced: also tracks invocation in statistics.
        """
        # --- Original behavior preserved exactly ---
        self.max_tools = max(1, self.max_tools - 1)
        # --- Enhancement ---
        self._throttle_events["reduce_tool_calls"] += 1
        logger.debug(
            f"reduce_tool_calls(): max_tools now {self.max_tools}"
        )

    def shorten_context(self) -> None:
        """
        Eco-mode hook: reduce context size for subsequent runs.

        Original behavior: no-op.
        Enhanced: multiplies `_context_shortening_factor` by 0.5, floored
        at 0.125. Tracks invocation in statistics.
        """
        # --- Enhancement: make the hook actually work ---
        self._context_shortening_factor = max(
            0.125, self._context_shortening_factor * 0.5
        )
        self._throttle_events["shorten_context"] += 1
        logger.debug(
            f"shorten_context(): context factor now "
            f"{self._context_shortening_factor}"
        )

    def finalize(self) -> None:
        """
        Clean up the runtime.

        Original behavior: no-op.
        Enhanced: marks finalized, logs a summary.
        """
        if self._finalized:
            logger.debug("finalize() called twice; ignoring")
            return

        self._finalized = True
        logger.info(
            f"LangChainRuntime finalized: "
            f"{self._run_counter} runs, "
            f"total_tool_calls={self._total_tool_calls}, "
            f"final_max_tools={self.max_tools}, "
            f"final_context_factor={self._context_shortening_factor}, "
            f"throttle_events={dict(self._throttle_events)}"
        )

    # ------------------------------------------------------------------
    # Enhancement: public accessors
    # ------------------------------------------------------------------

    def set_callback(self, callback: Any) -> None:
        """
        Wire in a GreenLangChainCallback (or compatible object).

        The callback's `snapshot()` is queried on each `run()` and its
        counts are attached to the DecisionRecord's explanation.
        """
        self._callback = callback
        logger.debug(
            f"LangChainRuntime callback set: {type(callback).__name__}"
        )

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
            "tool_calls": self.tool_calls,
            "max_tools": self.max_tools,
            "context_shortening_factor": self._context_shortening_factor,
            "total_tool_calls": self._total_tool_calls,
            "total_energy_kwh": self._total_energy_kwh,
            "total_latency_ms": self._total_latency_ms,
            "throttle_events": dict(self._throttle_events),
            "simulated_flags": dict(self._simulated_flags),
            "records_emitted": len(self._records),
            "observations_emitted": len(self._observations),
            "contracts_available": _CONTRACTS_AVAILABLE,
            "precision": self._precision,
            "hardware_profile": self._hardware_profile,
            "callback_wired": self._callback is not None,
        }

    def export(self) -> Dict[str, Any]:
        """Serialise everything for JSON export."""
        return {
            "config": dict(self.config),
            "statistics": self.get_statistics(),
            "history": list(self._history),
            "records": [
                r.__dict__ if hasattr(r, "__dict__") else r
                for r in self._records
            ],
        }
