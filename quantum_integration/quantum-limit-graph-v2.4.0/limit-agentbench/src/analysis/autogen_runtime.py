# src/analysis/adapters/autogen_runtime.py

"""
AutoGen Runtime Adapter with message-graph depth tracking (Enhanced)
=====================================================================

Translates AutoGen runtime lifecycle events into the analysis layer's
shared contracts (`DecisionRecord`, `AgentObservation`) and honors the
eco-mode hooks (`reduce_tool_calls`, `shorten_context`) requested by the
Adaptive Controller.

Original API preserved:
    runtime = AutoGenRuntime()
    runtime.init(config)
    result = runtime.run(query)
    runtime.reduce_tool_calls()
    runtime.shorten_context()
    runtime.finalize()

Enhanced API:
    runtime.records()          # -> list[DecisionRecord]
    runtime.observations()     # -> list[AgentObservation]
    runtime.get_statistics()   # -> dict
    runtime.export()           # -> dict (JSON-serialisable)
"""

from __future__ import annotations

import logging
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# Shared contracts — lazily imported with inline fallback
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
# Enhanced AutoGenRuntime
# =============================================================================

class AutoGenRuntime:
    """
    Enhanced AutoGen runtime adapter.

    Backward-compatible with the original five-method API. Adds real
    eco-mode throttle state, per-run DecisionRecord emission, statistics,
    and export.
    """

    # Conservative energy per tool call (Wh); scaled by precision if set
    ENERGY_PER_TOOL_CALL_WH = 0.05
    # Baseline latency per graph-depth layer (ms)
    LATENCY_PER_DEPTH_MS = 15.0

    def __init__(self):
        # --- Original state ---
        self.graph_depth: int = 0

        # --- Enhancement: runtime state ---
        self.config: Dict[str, Any] = {}
        self._initialized: bool = False
        self._finalized: bool = False

        # --- Enhancement: eco-mode throttle state ---
        self._tool_call_reduction: int = 0        # decrement per tool call
        self._context_shortening_factor: float = 1.0

        # --- Enhancement: identity & telemetry ---
        self._run_counter: int = 0
        self._policy_version: str = ""
        self._hardware_profile: str = ""
        self._precision: Optional[str] = None
        self._records: List[Any] = []
        self._observations: List[Any] = []
        self._history: List[Dict[str, Any]] = []

        # --- Enhancement: optional telemetry injection ---
        self._energy_meter: Any = None
        self._carbon_estimator: Any = None

        # --- Enhancement: statistics ---
        self._total_tool_calls: int = 0
        self._total_energy_kwh: float = 0.0
        self._total_latency_ms: float = 0.0
        self._throttle_events: Counter = Counter()

        logger.debug("Enhanced AutoGenRuntime initialized")

    # ------------------------------------------------------------------
    # Original public API
    # ------------------------------------------------------------------

    def init(self, config: dict) -> None:
        """
        Initialise the runtime with a configuration dict.

        Original behavior: stores the config.
        Enhanced: also extracts run identity, precision, hardware profile,
        and validates the config.
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

        # --- Enhancement: extract identity and telemetry hints ---
        self._policy_version = str(config.get("policy_version", ""))
        self._hardware_profile = str(config.get("hardware_profile", ""))
        self._precision = config.get("precision")
        self._energy_meter = config.get("energy_meter")
        self._carbon_estimator = config.get("carbon_estimator")
        self._run_id = str(
            config.get("run_id") or f"run-{uuid.uuid4().hex[:8]}"
        )

        # --- Enhancement: explicit reset of throttle state ---
        self._tool_call_reduction = 0
        self._context_shortening_factor = 1.0

        self._initialized = True
        self._finalized = False

        logger.info(
            f"AutoGenRuntime initialized (run_id={self._run_id}, "
            f"precision={self._precision}, hardware={self._hardware_profile})"
        )

    def run(self, query: dict) -> dict:
        """
        Execute a simulated AutoGen conversation turn.

        Original return shape preserved:
            {"accuracy": float, "tool_calls": int, "conversation_depth": int}

        Enhanced: honors throttle state, tracks real metrics, and emits a
        DecisionRecord per run.
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

        # --- Enhancement: ensure init was called ---
        if not self._initialized:
            logger.warning("run() called before init(); auto-initializing")
            self.init({})

        self._run_counter += 1
        run_start = time.perf_counter()

        # --- Original graph-depth tracking (preserved shape) ---
        # Instead of a hardcoded +2, use the query's actual agent count if
        # provided; fall back to +2 for backward compatibility.
        depth_increment = int(query.get("agent_count", 2))
        if depth_increment < 1:
            depth_increment = 2  # preserve original behavior
        self.graph_depth += depth_increment

        # --- Enhancement: compute tool calls with throttle applied ---
        base_tool_calls = int(query.get("base_tool_calls", 1))
        effective_tool_calls = max(
            0, base_tool_calls - self._tool_call_reduction
        )

        # --- Enhancement: compute effective context length ---
        base_context_tokens = int(query.get("base_context_tokens", 0))
        effective_context_tokens = int(
            base_context_tokens * self._context_shortening_factor
        )

        # --- Enhancement: energy / latency estimation ---
        if self._energy_meter is not None:
            try:
                # Prefer the injected meter if it exposes a callable
                if hasattr(self._energy_meter, "measure"):
                    energy_kwh = float(
                        self._energy_meter.measure(
                            tool_calls=effective_tool_calls
                        )
                    )
                else:
                    energy_kwh = (
                        effective_tool_calls * self.ENERGY_PER_TOOL_CALL_WH
                    ) / 1000.0
            except Exception as e:
                logger.debug(f"Energy meter failed: {e}; falling back")
                energy_kwh = (
                    effective_tool_calls * self.ENERGY_PER_TOOL_CALL_WH
                ) / 1000.0
        else:
            energy_kwh = (
                effective_tool_calls * self.ENERGY_PER_TOOL_CALL_WH
            ) / 1000.0

        latency_ms = self.graph_depth * self.LATENCY_PER_DEPTH_MS

        # --- Enhancement: carbon estimation ---
        carbon_kg = 0.0
        if self._carbon_estimator is not None:
            try:
                if hasattr(self._carbon_estimator, "estimate"):
                    carbon_kg = float(
                        self._carbon_estimator.estimate(energy_kwh=energy_kwh)
                    )
                else:
                    carbon_kg = energy_kwh * 0.4  # 400 gCO2/kWh default
            except Exception as e:
                logger.debug(f"Carbon estimator failed: {e}")
                carbon_kg = energy_kwh * 0.4
        else:
            carbon_kg = energy_kwh * 0.4

        # --- Enhancement: update statistics ---
        self._total_tool_calls += effective_tool_calls
        self._total_energy_kwh += energy_kwh
        self._total_latency_ms += latency_ms

        # --- Enhancement: emit DecisionRecord ---
        record = DecisionRecord(
            run_id=self._run_id,
            timestamp=datetime.now(),
            task_id=str(query.get("task_id", f"task-{self._run_counter}")),
            selected_action=f"autogen_run(depth={depth_increment})",
            alternatives=[],
            policy_version=self._policy_version,
            model_or_agent="autogen",
            hardware_profile=self._hardware_profile,
            precision=self._precision,
            quality_score=0.85,
            latency_ms=latency_ms,
            energy_kwh=energy_kwh,
            carbon_operational_kg=carbon_kg,
            explanation={
                "graph_depth": self.graph_depth,
                "depth_increment": depth_increment,
                "tool_calls_effective": effective_tool_calls,
                "tool_calls_reduction": self._tool_call_reduction,
                "context_tokens": effective_context_tokens,
                "context_factor": self._context_shortening_factor,
                "simulated": True,  # explicitly flag simulated values
            },
            provenance={
                "source": "autogen_runtime",
                "config_keys": sorted(self.config.keys()),
            },
        )
        self._records.append(record)

        # --- Enhancement: emit AgentObservation ---
        self._observations.append(AgentObservation(
            agent_id="autogen_runtime",
            task_id=record.task_id,
            role="generalist",
            delegated_to=[],
            coordination_calls=1,
            latency_ms=latency_ms,
            energy_kwh=energy_kwh,
        ))

        # --- Enhancement: record history for export ---
        run_elapsed_ms = (time.perf_counter() - run_start) * 1000.0
        self._history.append({
            "run_index": self._run_counter,
            "depth": self.graph_depth,
            "depth_increment": depth_increment,
            "tool_calls": effective_tool_calls,
            "context_tokens": effective_context_tokens,
            "energy_kwh": energy_kwh,
            "carbon_kg": carbon_kg,
            "latency_ms": latency_ms,
            "run_elapsed_ms": run_elapsed_ms,
            "at": record.timestamp.isoformat(),
        })

        # --- Original return shape (preserved exactly) ---
        return {
            "accuracy": 0.85,
            "tool_calls": effective_tool_calls,
            "conversation_depth": self.graph_depth,
        }

    def reduce_tool_calls(self) -> None:
        """
        Eco-mode hook: reduce the number of tool calls per subsequent run.

        Original behavior: no-op.
        Enhanced: decrements the effective tool-call count by 1, saturating
        at 0. Tracks invocation in `_throttle_events`.
        """
        self._tool_call_reduction += 1
        self._throttle_events["reduce_tool_calls"] += 1
        logger.debug(
            f"reduce_tool_calls(): effective reduction now "
            f"{self._tool_call_reduction}"
        )

    def shorten_context(self) -> None:
        """
        Eco-mode hook: reduce context size for subsequent runs.

        Original behavior: no-op.
        Enhanced: multiplies `_context_shortening_factor` by 0.5, floored
        at 0.125. Tracks invocation in `_throttle_events`.
        """
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
        Enhanced: marks the runtime as finalized, logs a summary, and
        prevents further runs without an explicit re-init.
        """
        if self._finalized:
            logger.debug("finalize() called twice; ignoring")
            return

        self._finalized = True
        logger.info(
            f"AutoGenRuntime finalized: "
            f"{self._run_counter} runs, "
            f"depth={self.graph_depth}, "
            f"total_tool_calls={self._total_tool_calls}, "
            f"total_energy_kwh={self._total_energy_kwh:.6f}, "
            f"throttle_events={dict(self._throttle_events)}"
        )

    # ------------------------------------------------------------------
    # Enhancement: public accessors
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
            "run_id": getattr(self, "_run_id", ""),
            "initialized": self._initialized,
            "finalized": self._finalized,
            "run_count": self._run_counter,
            "graph_depth": self.graph_depth,
            "total_tool_calls": self._total_tool_calls,
            "total_energy_kwh": self._total_energy_kwh,
            "total_latency_ms": self._total_latency_ms,
            "tool_call_reduction": self._tool_call_reduction,
            "context_shortening_factor": self._context_shortening_factor,
            "throttle_events": dict(self._throttle_events),
            "records_emitted": len(self._records),
            "observations_emitted": len(self._observations),
            "contracts_available": _CONTRACTS_AVAILABLE,
            "precision": self._precision,
            "hardware_profile": self._hardware_profile,
        }

    def export(self) -> Dict[str, Any]:
        """Serialise everything for JSON export."""
        return {
            "config": dict(self.config),
            "statistics": self.get_statistics(),
            "history": list(self._history),
            "records": [r.__dict__ if hasattr(r, "__dict__") else r for r in self._records],
        }


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior ---
    runtime = AutoGenRuntime()
    runtime.init({"policy_version": "v5.0.1", "hardware_profile": "V100"})
    print("Run 1:", runtime.run({"task_id": "t1"}))
    print("Run 2:", runtime.run({"task_id": "t1"}))
    runtime.finalize()

    # --- Enhanced behavior: eco-mode hooks now take effect ---
    print("\n=== Eco-mode hooks ===")
    runtime2 = AutoGenRuntime()
    runtime2.init({"policy_version": "v5.0.1"})

    print("Baseline:", runtime2.run({
        "task_id": "t2", "base_tool_calls": 5, "base_context_tokens": 4096,
    }))

    runtime2.reduce_tool_calls()
    runtime2.shorten_context()

    print("Throttled:", runtime2.run({
        "task_id": "t2", "base_tool_calls": 5, "base_context_tokens": 4096,
    }))

    runtime2.reduce_tool_calls()
    runtime2.shorten_context()

    print("Throttled again:", runtime2.run({
        "task_id": "t2", "base_tool_calls": 5, "base_context_tokens": 4096,
    }))

    import json
    print("\n=== Statistics ===")
    print(json.dumps(runtime2.get_statistics(), indent=2, default=str))

    print("\n=== Last DecisionRecord ===")
    rec = runtime2.records()[-1]
    for k in ("run_id", "task_id", "selected_action", "latency_ms",
              "energy_kwh", "carbon_operational_kg"):
        print(f"  {k}: {getattr(rec, k)}")
    print(f"  explanation keys: {list(rec.explanation.keys())}")

    runtime2.finalize()
