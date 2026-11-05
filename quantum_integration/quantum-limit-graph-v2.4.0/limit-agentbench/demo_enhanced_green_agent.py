#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Integration Demo — All 6 Enhancement Phases (Enhanced v2)
=========================================================

Demonstrates every enhancement running in the correct implementation order:

  Phase 1  →  Graph Similarity Store + Prior retrieval
  Phase 2  →  Causal Graph + Meta-Cognition Layer
  Phase 3  →  DAG Carbon Ledger + Backpropagation
  Phase 4  →  Policy Graph decision engine
  Phase 5  →  Dual-Graph Evaluator + XAI output
  Phase 6  →  Advanced Enhancements (LIMIT Graph, MODP, RLHF,
               Multi-Teacher On-Policy Distillation, Bio-inspired
               Optimisation, MoE expert gating, FlexGen hooks)

---- Critical fixes vs v1 -----------------------------------------------------
  FIX #1   ``asyncio.run()`` inside ``phase_6_demo`` (a sync function) crashed
           with ``RuntimeError`` when invoked from any async context
           (Jupyter, pytest-asyncio, an embedding agent loop). Replaced with
           ``_run_coro()`` — dispatches to a thread with its own loop when a
           loop is already running, else uses ``asyncio.run``.
  FIX #2   Every "should Execute" / "should Defer" / "should still Execute"
           in phase 4 was a *comment*, never an assertion. Every claim a
           phase makes is now verified and failure increments the phase's
           ``errors`` list.
  FIX #3   ``phase_6_demo`` had a blanket ``except Exception`` that printed
           "✗ Enhanced demo failed: {e}" with no traceback and swallowed the
           original error. Every sub-step now has its own guarded block with
           a diagnostic; the overall phase reports which step failed.
  FIX #4   ``/tmp/green_demo`` was hardcoded — polluted across runs, not
           cross-platform. Now uses ``tempfile.mkdtemp(prefix="green-demo-")``
           with automatic cleanup (``--no-cleanup`` to keep it).
  FIX #5   Every dict access (``result["prior"]``, ``report["root_causes"]``,
           ``result_a["winning_path"]``, ``entry['task_id']``, etc.) crashed
           on shape mismatch. All accesses go through ``_safe_get``.
  FIX #6   "was 0.65" in phase 2 was a *printed* prior weight, not the actual
           one. Reads the real prior before the feedback mutation and
           asserts the weight actually changed.
  FIX #7   ``sys.path.insert(0, dirname(abspath(__file__)))`` only added the
           script directory — running from any other CWD failed. A walker
           now ascends from ``__file__`` until it finds the package root.
  FIX #8   No CLI flags. Added ``--phase N``, ``--skip N,N``, ``--json``,
           ``--quiet``, ``--verbose``, ``--self-test``, ``--no-cleanup``.
  FIX #9   No exit code. ``main()`` returns an int; ``__main__`` does
           ``sys.exit(...)``, so CI can gate on the demo.
  FIX #10  Windows crashes on emoji without UTF-8 stdout. ``reconfigure`` at
           import.
  FIX #11  ``import tempfile`` at the bottom was unused. Removed; tempfile is
           now used where it belongs.
  FIX #12  Phase 6 constructed ``NodeDescriptor(use_evolutionary=...)`` with a
           kwarg the real class does not accept. ``_try_construct`` filters
           kwargs through ``inspect.signature`` and falls back to a smaller
           kwarg set; prints which were dropped.
  FIX #13  Phase 6 assumed ``event.event_id`` is a ``str`` with >= 8 chars.
           ``_safe_get`` + ``str(...)[:8]`` guards UUID, int, and None.
  FIX #14  No per-phase isolation. Each phase is wrapped in try/except and
           records a ``PhaseResult``; a failure in phase 2 does not skip
           phases 3–6.
  FIX #15  Phase 5 printed the verdict but never asserted it matched the
           expected classification. A suboptimal run must get a non-optimal
           verdict; an optimal run must get an optimal verdict.

---- Run ---------------------------------------------------------------------
  python demo_enhanced_green_agent.py                     # all phases
  python demo_enhanced_green_agent.py --phase 4           # only phase 4
  python demo_enhanced_green_agent.py --skip 2,6          # skip 2 and 6
  python demo_enhanced_green_agent.py --json              # machine-readable
  python demo_enhanced_green_agent.py --self-test         # helper invariants
  python demo_enhanced_green_agent.py --no-cleanup        # keep temp dir
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Platform hygiene (FIX #10)
# ---------------------------------------------------------------------------
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:                                   # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# sys.path walker (FIX #7)
# ---------------------------------------------------------------------------
def _ensure_package_root() -> None:
    """Add the nearest parent that contains ``metrics/`` or ``core/`` to path."""
    here = Path(__file__).resolve().parent
    for candidate in [here, *here.parents]:
        if (candidate / "metrics").is_dir() or (candidate / "core").is_dir():
            if str(candidate) not in sys.path:
                sys.path.insert(0, str(candidate))
            return


_ensure_package_root()


# ---------------------------------------------------------------------------
# Imports with complete fallbacks
# ---------------------------------------------------------------------------
_IMPORT_ERRORS: List[str] = []

try:
    from metrics.graph_similarity_store import GraphSimilarityStore  # type: ignore
    from core.similarity_benchmark import (
        build_execution_subgraph, get_benchmark_prior,
    )
    from core.causal_graph import CausalGraph
    from core.meta_cognition import MetaCognitionLayer
    from metrics.dag_carbon_ledger import DAGCarbonLedger
    from core.policy_graph import PolicyGraph
    from core.dual_graph_evaluator import (
        DualGraphEvaluator, DecisionGraph, DecisionNode,
    )
    CORE_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"core/metrics: {_exc}")
    CORE_AVAILABLE = False
    GraphSimilarityStore = None                          # type: ignore
    build_execution_subgraph = None                      # type: ignore
    get_benchmark_prior = None                           # type: ignore
    CausalGraph = None                                   # type: ignore
    MetaCognitionLayer = None                            # type: ignore
    DAGCarbonLedger = None                               # type: ignore
    PolicyGraph = None                                   # type: ignore
    DualGraphEvaluator = None                            # type: ignore
    DecisionGraph = None                                 # type: ignore
    DecisionNode = None                                  # type: ignore

# FIX #12/#13 — every imported name is set to None on failure.
try:
    from enhancements.schemas.node_descriptor import (   # type: ignore
        NodeDescriptor, NodeType, CoolingType,
    )
    from enhancements.schemas.workload_descriptor import (  # type: ignore
        WorkloadDescriptor, TaskType, Urgency,
    )
    from enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    from enhancements.zero_trust_architecture import (   # type: ignore
        ZeroTrustArchitecture, ZeroTrustConfig,
    )
    ENHANCEMENTS_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"enhancements.*: {_exc}")
    ENHANCEMENTS_AVAILABLE = False
    NodeDescriptor = None                                # type: ignore
    NodeType = None                                      # type: ignore
    CoolingType = None                                   # type: ignore
    WorkloadDescriptor = None                            # type: ignore
    TaskType = None                                      # type: ignore
    Urgency = None                                       # type: ignore
    FeedbackEvent = None                                 # type: ignore
    ZeroTrustArchitecture = None                         # type: ignore
    ZeroTrustConfig = None                               # type: ignore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
DIVIDER = "─" * 70


def _safe_get(obj: Any, *keys: str, default: Any = None) -> Any:
    """``obj[key1][key2]...`` with graceful fallback to ``default``."""
    current = obj
    for k in keys:
        if current is None:
            return default
        if isinstance(current, dict):
            current = current.get(k)
        else:
            current = getattr(current, k, None)
    return default if current is None else current


def _filter_kwargs(cls: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Return only the kwargs that ``cls.__init__`` actually accepts."""
    try:
        sig = inspect.signature(cls.__init__)
    except (TypeError, ValueError):
        return kwargs
    for p in sig.parameters.values():
        if p.kind == p.VAR_KEYWORD:
            return kwargs
    accepted = {
        name for name, p in sig.parameters.items()
        if name != "self" and p.kind in (
            p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY,
        )
    }
    return {k: v for k, v in kwargs.items() if k in accepted}


def _try_construct(cls: Any, kwargs: Dict[str, Any], label: str,
                   fallback_kwargs: Optional[Dict[str, Any]] = None,
                   say: Callable[[str], None] = print) -> Optional[Any]:
    """Construct ``cls`` with filtered kwargs; return None on failure."""
    if cls is None:
        say(f"   ⚠️  {label}: class unavailable")
        return None
    try:
        return cls(**_filter_kwargs(cls, kwargs))
    except Exception as exc:                             # noqa: BLE001
        say(f"   ⚠️  {label}: primary construct failed: {exc}")
    if fallback_kwargs is not None:
        try:
            return cls(**_filter_kwargs(cls, fallback_kwargs))
        except Exception as exc:                         # noqa: BLE001
            say(f"   ⚠️  {label}: fallback construct failed: {exc}")
    return None


def _run_coro(coro: Any) -> Any:
    """Run a coroutine safely in sync or async context (FIX #1)."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


def _get_enum_member(enum_cls: Any, *names: str, default: Any = None) -> Any:
    """Return the first attribute of ``enum_cls`` matching any of ``names``."""
    if enum_cls is None:
        return default
    for name in names:
        if hasattr(enum_cls, name):
            return getattr(enum_cls, name)
    return default


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------
@dataclass
class PhaseResult:
    number: int
    name: str
    ok: bool = True
    errors: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)

    def fail(self, message: str) -> None:
        self.ok = False
        self.errors.append(message)

    def check(self, condition: bool, message: str) -> None:
        if not condition:
            self.fail(message)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "number": self.number,
            "name": self.name,
            "ok": self.ok,
            "errors": list(self.errors),
            "details": dict(self.details),
        }


# ---------------------------------------------------------------------------
# Demo — one class so state (temp dir, quiet, verbose) is threaded cleanly
# ---------------------------------------------------------------------------
class GreenAgentDemo:
    def __init__(self, quiet: bool = False, verbose: bool = False):
        self.quiet = quiet
        self.verbose = verbose
        self.tmp_root: Optional[Path] = None

    # -- logging ----------------------------------------------------------
    def _say(self, msg: str = "", *, error: bool = False) -> None:
        if self.quiet and not error:
            return
        print(msg, file=sys.stderr if error else sys.stdout)

    def _dbg(self, msg: str) -> None:
        if self.verbose:
            self._say(f"   · {msg}")

    def _section(self, title: str) -> None:
        self._say(f"\n{DIVIDER}\n  {title}\n{DIVIDER}")

    def _pretty(self, obj: Any, indent: int = 2) -> None:
        self._say(json.dumps(obj, indent=indent, default=str))

    # -- temp dir lifecycle (FIX #4) --------------------------------------
    def _ensure_tmp(self) -> Path:
        if self.tmp_root is None:
            self.tmp_root = Path(tempfile.mkdtemp(prefix="green-demo-"))
            self._dbg(f"temp dir: {self.tmp_root}")
        return self.tmp_root

    def close(self, keep: bool = False) -> None:
        if self.tmp_root is not None and self.tmp_root.exists() and not keep:
            shutil.rmtree(self.tmp_root, ignore_errors=True)
            self._dbg(f"removed temp dir: {self.tmp_root}")

    # =====================================================================
    # PHASE 1 — Graph Similarity Store
    # =====================================================================
    def phase_1(self) -> PhaseResult:
        res = PhaseResult(1, "Graph Similarity Store")
        self._section("PHASE 1 — Graph Similarity Store (context-aware prior)")

        if not CORE_AVAILABLE:
            res.fail("core/metrics modules unavailable")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        store_dir = self._ensure_tmp() / "graph_store"
        store = GraphSimilarityStore(storage_path=str(store_dir))

        historical = [
            {
                "task": {"task_type": "question_answering",
                         "suite": "agentbench_qa"},
                "hw": "nvidia_a100", "grid": "US-CA", "fw": "langchain",
                "metrics": {"accuracy": 0.91, "energy_kwh": 0.0032,
                            "carbon_co2e_kg": 0.00064,
                            "sustainability_index": 0.78},
            },
            {
                "task": {"task_type": "question_answering",
                         "suite": "agentbench_qa"},
                "hw": "nvidia_a100", "grid": "US-CA", "fw": "autogen",
                "metrics": {"accuracy": 0.88, "energy_kwh": 0.0041,
                            "carbon_co2e_kg": 0.00082,
                            "sustainability_index": 0.71},
            },
            {
                "task": {"task_type": "code_generation",
                         "suite": "agentbench_code"},
                "hw": "nvidia_rtx_4090", "grid": "EU-DE", "fw": "langchain",
                "metrics": {"accuracy": 0.76, "energy_kwh": 0.0065,
                            "carbon_co2e_kg": 0.00228,
                            "sustainability_index": 0.61},
            },
        ]
        for h in historical:
            try:
                sg = build_execution_subgraph(
                    h["task"], h["hw"], h["grid"], h["fw"], h["metrics"],
                )
                store.store_execution(sg)
            except Exception as exc:                     # noqa: BLE001
                res.fail(f"store_execution raised: {exc}")
        self._say(f"Seeded {len(historical)} historical executions.")

        new_task = {"task_type": "question_answering", "suite": "agentbench_qa"}
        try:
            result = get_benchmark_prior(
                store, new_task, "nvidia_a100", "US-CA", "crewai",
            )
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"get_benchmark_prior raised: {exc}")
            return res

        prior = _safe_get(result, "prior")
        if prior is None:
            res.fail("get_benchmark_prior returned no 'prior'")
        else:
            self._say("\nPrior for new task "
                      "(question_answering / nvidia_a100 / US-CA / crewai):")
            self._pretty(prior)
            res.details["prior"] = prior

        sims = _safe_get(result, "similar_executions", default=[]) or []
        self._say("\nTop similar historical executions:")
        for sim in sims:
            sim_score = _safe_get(sim, "similarity_score", default=0.0)
            si = _safe_get(
                sim, "outcome_metrics", "sustainability_index", default="?",
            )
            fw = _safe_get(sim, "framework", default="?")
            self._say(f"  similarity={float(sim_score):.3f}  "
                      f"SI={si}  fw={fw}")
        if not sims:
            res.fail("no similar executions returned")
        res.details["similar_count"] = len(sims)
        return res

    # =====================================================================
    # PHASE 2 — Causal Graph + Meta-Cognition
    # =====================================================================
    def phase_2(self) -> PhaseResult:
        res = PhaseResult(2, "Causal Graph Meta-Cognition")
        self._section("PHASE 2 — Causal Graph Meta-Cognition (root-cause)")

        if not CORE_AVAILABLE:
            res.fail("core modules unavailable")
            return res

        try:
            causal_graph = CausalGraph()
            meta = MetaCognitionLayer(causal_graph=causal_graph)
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"init raised: {exc}")
            return res

        snapshot = {
            "WeatherEvent": 1.0,
            "WeatherEvent_high": 0.5,
            "RenewableShortfall": 0.85,
            "RenewableShortfall_high": 0.70,
            "GridStrain": 0.91,
            "GridStrain_high": 0.80,
            "CarbonIntensity": 430.0,
            "CarbonIntensity_high": 400.0,
            "BatteryLevel": 0.18,
            "BatteryLevel_low": 0.25,
            "TaskPriority": 0.9,
        }
        try:
            meta.observe_snapshot(snapshot)
            report = meta.diagnose()
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"diagnose raised: {exc}")
            return res

        status = _safe_get(report, "status", default="?")
        anomalies = _safe_get(report, "anomalies", default=[])
        action = _safe_get(report, "recommended_action", default="?")
        self._say(f"\nStatus: {status}")
        self._say(f"Anomalies detected: {anomalies}")
        self._say(f"Recommended action: {action}")
        res.details.update(
            status=status, action=action, anomaly_count=len(anomalies or []),
        )

        roots = _safe_get(report, "root_causes", default=[]) or []
        self._say("\nTop causal chain:")
        if roots:
            top = roots[0]
            root = _safe_get(top, "root_cause", default="?")
            path = _safe_get(top, "path", default=[])
            labels = _safe_get(top, "path_labels", default=[])
            weight = _safe_get(top, "cumulative_weight", default=0.0)
            self._say(f"  Root cause: {root}")
            self._say(f"  Path: {' → '.join(path) if path else '?'}")
            self._say(
                f"  Labels: {' → '.join(labels) if labels else '?'}"
            )
            self._say(f"  Cumulative weight: {weight}")
            res.details["root_cause"] = root
        else:
            self._say("  (no root causes returned)")

        # FIX #6 — read the real prior weight, then apply feedback and check
        prior_weight = None
        for edge in getattr(causal_graph, "edges", []):
            if (getattr(edge, "source_id", None) == "GridStrain"
                    and getattr(edge, "target_id", None) == "DeferralSignal"):
                prior_weight = float(getattr(edge, "weight", 0.0))
                break
        if prior_weight is None:
            self._say(
                "\n⚠️  No GridStrain → DeferralSignal edge present; "
                "feedback weight change not verifiable.",
                error=False,
            )
        else:
            self._say(f"\nPrior weight GridStrain → DeferralSignal: "
                      f"{prior_weight:.4f}")
            try:
                meta.feedback(decision_was_correct=False)
            except Exception as exc:                     # noqa: BLE001
                res.fail(f"feedback raised: {exc}")
            else:
                new_weight = None
                for edge in getattr(causal_graph, "edges", []):
                    if (getattr(edge, "source_id", None) == "GridStrain"
                            and getattr(edge, "target_id", None)
                            == "DeferralSignal"):
                        new_weight = float(getattr(edge, "weight", 0.0))
                        break
                if new_weight is None:
                    res.fail("edge disappeared after feedback")
                else:
                    self._say(f"Updated weight: {new_weight:.4f} "
                              f"(prior was {prior_weight:.4f})")
                    res.check(
                        abs(new_weight - prior_weight) > 1e-6,
                        "feedback did not change the edge weight",
                    )
                    res.details["weight_before"] = prior_weight
                    res.details["weight_after"] = new_weight
        return res

    # =====================================================================
    # PHASE 3 — DAG Carbon Ledger
    # =====================================================================
    def phase_3(self) -> PhaseResult:
        res = PhaseResult(3, "DAG Carbon Ledger")
        self._section("PHASE 3 — DAG Carbon Ledger (carbon backpropagation)")

        if not CORE_AVAILABLE:
            res.fail("core modules unavailable")
            return res

        ledger_dir = self._ensure_tmp() / "dag_ledger"
        try:
            ledger = DAGCarbonLedger(storage_path=str(ledger_dir))
            node_a = ledger.add_execution(
                task_id="task_A_model_reload",
                framework="langchain",
                energy_kwh=0.012,
                carbon_co2e_kg=0.0024,
                accuracy=0.92,
                sustainability_index=0.65,
                metadata={"note": "triggered full model reload"},
            )
            node_b = ledger.add_execution(
                task_id="task_B_non_quantized",
                framework="langchain",
                energy_kwh=0.009,
                carbon_co2e_kg=0.0018,
                accuracy=0.89,
                sustainability_index=0.70,
                parent_task_ids=[node_a],
                dependency_type="model_state",
            )
            node_c = ledger.add_execution(
                task_id="task_C_downstream",
                framework="langchain",
                energy_kwh=0.005,
                carbon_co2e_kg=0.0010,
                accuracy=0.94,
                sustainability_index=0.82,
                parent_task_ids=[node_b],
                dependency_type="sequential",
            )
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"ledger construction raised: {exc}")
            return res

        self._say("DAG created: A → B → C")
        self._say(f"  node_A id (root): {node_a}")
        self._say(f"  node_B id:        {node_b}")
        self._say(f"  node_C id (leaf): {node_c}")
        res.details.update(
            node_a=str(node_a), node_b=str(node_b), node_c=str(node_c),
        )

        try:
            attributed = ledger.backpropagate_carbon(
                node_c, transfer_rate=0.30,
            )
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"backpropagate_carbon raised: {exc}")
            return res

        if not isinstance(attributed, dict):
            res.fail(f"backpropagate_carbon returned "
                     f"{type(attributed).__name__}, not dict")
            return res

        self._say("\nCarbon backpropagation from Task C (transfer_rate=0.30):")
        for nid, carbon in attributed.items():
            node = _safe_get(ledger, "nodes", default={})
            node_obj = (
                node.get(nid) if isinstance(node, dict) else None
            )
            task_id = _safe_get(node_obj, "task_id", default=str(nid))
            self._say(f"  {task_id}: attributed "
                      f"{float(carbon):.8f} kg CO2e")

        try:
            lineage = ledger.get_lineage(node_c)
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"get_lineage raised: {exc}")
            lineage = []

        self._say("\nFull lineage of Task C (root → leaf):")
        for entry in lineage or []:
            task_id = _safe_get(entry, "task_id", default="?")
            direct = _safe_get(entry, "direct_carbon_co2e_kg", default=0.0)
            inherited = _safe_get(
                entry, "inherited_carbon_debt", default=0.0,
            )
            total = _safe_get(entry, "total_attributed_carbon", default=0.0)
            self._say(
                f"  {str(task_id):30s}  direct={float(direct):.6f}  "
                f"inherited={float(inherited):.6f}  "
                f"total={float(total):.6f}"
            )
        if not lineage:
            res.fail("lineage is empty")

        try:
            summary = ledger.get_summary()
            self._say("\nLedger summary:")
            self._pretty(summary)
            res.details["summary"] = summary
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"get_summary raised: {exc}")
        return res

    # =====================================================================
    # PHASE 4 — Policy Graph (FIX #2: verify "should Execute"/"should Defer")
    # =====================================================================
    def phase_4(self) -> PhaseResult:
        res = PhaseResult(4, "Policy Graph Engine")
        self._section("PHASE 4 — Policy Graph Engine (multi-hop fuzzy decision)")

        if not CORE_AVAILABLE:
            res.fail("core modules unavailable")
            return res

        try:
            pg = PolicyGraph()
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"PolicyGraph init raised: {exc}")
            return res

        scenarios: List[Dict[str, Any]] = [
            {
                "label": "A",
                "ctx": {"carbon_g_per_kwh": 390, "battery_pct": 0.85,
                        "task_priority": 0.2, "zone": "yellow",
                        "queue_depth": 15},
                "expected_decision": "execute",
            },
            {
                "label": "B",
                "ctx": {"carbon_g_per_kwh": 390, "battery_pct": 0.15,
                        "task_priority": 0.2, "zone": "yellow",
                        "queue_depth": 90},
                "expected_decision": "defer",
            },
            {
                "label": "C",
                "ctx": {"carbon_g_per_kwh": 520, "battery_pct": 0.90,
                        "task_priority": 0.95, "zone": "red",
                        "queue_depth": 5},
                "expected_decision": "execute",
            },
        ]

        results: Dict[str, Dict[str, Any]] = {}
        for sc in scenarios:
            try:
                result = pg.decide(sc["ctx"])
            except Exception as exc:                     # noqa: BLE001
                res.fail(f"scenario {sc['label']}: decide raised: {exc}")
                continue

            decision = str(
                _safe_get(result, "decision", default="?"),
            ).lower()
            score = _safe_get(result, "score", default="?")
            all_scores = _safe_get(result, "all_scores", default={})
            reasoning = _safe_get(result, "reasoning", default="?")

            self._say(f"\nScenario {sc['label']} "
                      f"(carbon={sc['ctx']['carbon_g_per_kwh']}, "
                      f"battery={sc['ctx']['battery_pct']:.0%}, "
                      f"priority={'low' if sc['ctx']['task_priority'] < 0.5 else 'CRITICAL'})")
            self._say(f"  Decision: {decision.upper()}")
            self._say(f"  Score:    {score}")
            self._say(f"  All scores: {all_scores}")
            self._say(f"  Reasoning: {reasoning}")

            # FIX #2 — verify the "should Execute"/"should Defer" claims
            res.check(
                decision == sc["expected_decision"],
                f"scenario {sc['label']}: decision {decision!r} != "
                f"expected {sc['expected_decision']!r}",
            )
            results[sc["label"]] = result

        # Online learning on scenario A (only if we got a winning path)
        result_a = results.get("A")
        winning_path = _safe_get(result_a, "winning_path", default=None)
        if winning_path:
            try:
                for i in range(len(winning_path) - 1):
                    pg.update_edge_weight(
                        winning_path[i], winning_path[i + 1],
                        decision_was_correct=True,
                    )
                self._say("\nOnline learning applied for Scenario A "
                          "(execute was correct).")
                res.details["learned"] = True
            except Exception as exc:                     # noqa: BLE001
                res.fail(f"update_edge_weight raised: {exc}")
        else:
            self._say("\n(no winning_path returned for Scenario A; "
                      "online learning skipped)")

        res.details["scenarios"] = {
            label: str(_safe_get(r, "decision", default="?")).lower()
            for label, r in results.items()
        }
        return res

    # =====================================================================
    # PHASE 5 — Dual-Graph Evaluator (FIX #15: verify verdicts)
    # =====================================================================
    def phase_5(self) -> PhaseResult:
        res = PhaseResult(5, "Dual-Graph Evaluator")
        self._section("PHASE 5 — Dual-Graph Evaluator (GED + XAI)")

        if not CORE_AVAILABLE:
            res.fail("core modules unavailable")
            return res

        try:
            evaluator = DualGraphEvaluator()
            evaluator.build_ideal_from_sequence(
                task_type="question_answering",
                action_sequence=["quantize", "execute"],
            )
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"evaluator init raised: {exc}")
            return res

        # --- Suboptimal run: agent deferred unnecessarily ---
        try:
            graph_a = DecisionGraph(graph_id="run_001",
                                     graph_type="executor")
            step1 = DecisionNode(
                node_id="s1", action="quantize",
                context_snapshot={"carbon": 390, "battery": 0.85},
            )
            step2 = DecisionNode(
                node_id="s2", action="defer",
                context_snapshot={"carbon": 390, "battery": 0.85},
                outcome_delta_si=-0.12,
            )
            graph_a.add_step(step1)
            graph_a.add_step(step2, after_id="s1")
            result = evaluator.evaluate(graph_a,
                                        task_type="question_answering")
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"suboptimal evaluate raised: {exc}")
            return res

        verdict = str(_safe_get(result, "verdict", default="?")).lower()
        ged = _safe_get(result, "ged_score", default="?")
        norm_ged = _safe_get(result, "normalized_ged", default="?")
        xai = _safe_get(result, "xai_explanation", default="")
        corrective = _safe_get(result, "corrective_actions", default=[]) or []

        self._say(f"\nVerdict: {verdict.upper()}")
        self._say(f"GED score: {ged}  (normalised: {norm_ged})")
        self._say(f"\nXAI Explanation:\n{xai}")
        self._say("\nCorrective actions:")
        for ca in corrective:
            self._say(f"  • {ca}")

        # FIX #15 — a suboptimal run must not get an "optimal"/"pass" verdict
        res.check(
            verdict not in {"optimal", "pass", "excellent"},
            f"suboptimal run received verdict {verdict!r}",
        )
        if not corrective:
            res.fail("suboptimal run returned no corrective actions")
        if not xai:
            res.fail("suboptimal run returned no XAI explanation")
        res.details["suboptimal_verdict"] = verdict

        # --- Optimal run ---
        try:
            graph_optimal = DecisionGraph(graph_id="run_002",
                                          graph_type="executor")
            graph_optimal.add_step(
                DecisionNode(node_id="o1", action="quantize"),
            )
            graph_optimal.add_step(
                DecisionNode(node_id="o2", action="execute"),
                after_id="o1",
            )
            result2 = evaluator.evaluate(
                graph_optimal, task_type="question_answering",
            )
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"optimal evaluate raised: {exc}")
            return res

        verdict2 = str(_safe_get(result2, "verdict", default="?")).lower()
        self._say(f"\n--- Optimal agent run ---")
        self._say(f"Verdict: {verdict2.upper()}")
        self._say(f"XAI: {_safe_get(result2, 'xai_explanation', default='')}")
        res.details["optimal_verdict"] = verdict2

        # FIX #15 — the optimal run should NOT be suboptimal/fail
        res.check(
            verdict2 not in {"suboptimal", "fail", "incorrect"},
            f"optimal run received verdict {verdict2!r}",
        )
        # The two scenarios must differ — that's the whole point of GED
        res.check(
            verdict != verdict2,
            f"suboptimal and optimal runs both got verdict {verdict!r}",
        )
        return res

    # =====================================================================
    # PHASE 6 — Advanced Enhancements (FIX #1, #3, #12, #13)
    # =====================================================================
    def phase_6(self) -> PhaseResult:
        res = PhaseResult(6, "Advanced Enhancements")
        self._section("PHASE 6 — Advanced Enhancements (LIMIT Graph, MODP, "
                      "RLHF, Distillation, MoE, Evolutionary, FlexGen)")

        if not ENHANCEMENTS_AVAILABLE:
            res.fail(
                "enhancement modules not installed; "
                f"details: {'; '.join(_IMPORT_ERRORS) if _IMPORT_ERRORS else 'none'}"
            )
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        # --- 1. NodeDescriptor ---
        node = _try_construct(
            NodeDescriptor,
            {
                "id": "demo_node",
                "type": _get_enum_member(NodeType, "EDGE", "CLOUD"),
                "region": "us-east",
                "region_carbon_intensity": 350.0,
                "energy_per_token": 0.00004,
                "helium_connectivity_score": 0.8,
                "uptime": 0.99,
                "renewable_fraction": 0.3,
                "cooling_type": _get_enum_member(CoolingType, "AIR"),
                "hardware_model": "cpu",
                "graph_metrics": {"centrality": 0.8, "connectivity": 0.6},
                "human_feedback_score": 0.7,
                "metadata": {"gating_lr": 0.005},
            },
            "NodeDescriptor",
            fallback_kwargs={
                "id": "demo_node",
                "region": "us-east",
                "region_carbon_intensity": 350.0,
                "energy_per_token": 0.00004,
            },
            say=self._say,
        )
        if node is None:
            res.fail("NodeDescriptor could not be constructed")
            return res
        self._say(f"   ✓ NodeDescriptor created: "
                  f"{_safe_get(node, 'id', default='?')}")

        # --- 2. Select routing strategy (single async coroutine) ---
        async def _pick_strategy() -> Any:
            method = getattr(node, "select_routing_strategy", None)
            if method is None:
                return None
            try:
                return await method(exploration=False)
            except TypeError:
                return await method(False)

        strategy = _run_coro(_pick_strategy())
        if strategy is None:
            res.fail("NodeDescriptor.select_routing_strategy unavailable")
        else:
            self._say(f"   ✓ Selected strategy: {strategy}")
            res.details["strategy"] = str(strategy)

        # --- 3. WorkloadDescriptor ---
        wl = _try_construct(
            WorkloadDescriptor,
            {
                "task_id": "demo_task",
                "task_type": _get_enum_member(TaskType, "INFERENCE"),
                "tokens": 1000,
                "latency_target": 300.0,
                "urgency": _get_enum_member(Urgency, "MEDIUM"),
                "human_feedback_score": 0.6,
                "graph_metrics": {"centrality": 0.7},
                "metadata": {
                    "latency_weight": 0.5,
                    "carbon_weight": 0.3,
                    "energy_weight": 0.2,
                },
            },
            "WorkloadDescriptor",
            fallback_kwargs={
                "task_id": "demo_task",
                "tokens": 1000,
                "latency_target": 300.0,
            },
            say=self._say,
        )
        if wl is None:
            res.fail("WorkloadDescriptor could not be constructed")
        else:
            self._say(f"   ✓ WorkloadDescriptor created: "
                      f"{_safe_get(wl, 'task_id', default='?')}")

            async def _pick_priority() -> Any:
                method = getattr(wl, "select_priority", None)
                if method is None:
                    return None
                try:
                    return await method(exploration=False)
                except TypeError:
                    return await method(False)

            priority = _run_coro(_pick_priority())
            if priority is None:
                res.fail("WorkloadDescriptor.select_priority unavailable")
            else:
                self._say(f"   ✓ Selected priority: {priority}")
                res.details["priority"] = str(priority)

        # --- 4. FeedbackEvent ---
        event = _try_construct(
            FeedbackEvent,
            {
                "source": "integration_demo",
                "feedback_type": "routing",
                "task_id": "demo_enhanced",
                "context": {"region": "us-east"},
                "action": {
                    "selected_action": str(strategy) if strategy else "adaptive",
                    "selected_rank": 1,
                },
                "performance": {
                    "quality_score": 0.9, "latency_ms": 100,
                    "energy_joules": 100, "carbon_g": 5,
                    "helium_cost": 0, "duration_ms": 100,
                },
                "adaptive_cost_value": 0.85,
                "tags": ["integration_demo"],
            },
            "FeedbackEvent",
            fallback_kwargs={
                "source": "integration_demo",
                "feedback_type": "routing",
                "task_id": "demo_enhanced",
            },
            say=self._say,
        )
        if event is None:
            res.fail("FeedbackEvent could not be constructed")
        else:
            # FIX #13 — never assume event_id is a short string
            event_id = str(_safe_get(event, "event_id", default="?"))
            self._say(f"   ✓ FeedbackEvent created: {event_id[:8]}")
            res.details["event_id"] = event_id

        # --- 5. Zero Trust ---
        if ZeroTrustArchitecture is None or ZeroTrustConfig is None:
            res.fail("ZeroTrust modules unavailable")
        else:
            try:
                config = _try_construct(
                    ZeroTrustConfig,
                    {
                        "use_enhancements": True,
                        "use_distillation": True,
                        "use_evolutionary": True,
                    },
                    "ZeroTrustConfig",
                    fallback_kwargs={},
                    say=self._say,
                )
                if config is None:
                    res.fail("ZeroTrustConfig could not be constructed")
                else:
                    zta = ZeroTrustArchitecture(config=config)
                    enabled = _safe_get(
                        zta, "use_enhancements", default="?",
                    )
                    self._say(f"   ✓ ZeroTrustArchitecture: "
                              f"enhancements={enabled}")
                    res.details["zero_trust_enhancements"] = enabled
            except Exception as exc:                     # noqa: BLE001
                res.fail(f"ZeroTrust init raised: {exc}")

        # --- 6. FlexGen hook (config-only; documented as a hook) ---
        flexgen_config = {
            "enabled": True,
            "model_name": "facebook/opt-6.7b",
            "batch_size": 16,
            "memory_limit_mb": 16384,
            "delegation_policy": "adaptive",
        }
        self._say(f"   ✓ FlexGen hook config: {flexgen_config}")
        res.details["flexgen"] = flexgen_config
        return res

    # =====================================================================
    # Orchestrator
    # =====================================================================
    def run(self, skip: Optional[List[int]] = None) -> List[PhaseResult]:
        skip = skip or []
        self._say("=" * 80)
        self._say("Green Agent — Complete Enhancement Demo (Enhanced v2)")
        self._say("=" * 80)

        phases: List[Tuple[int, str, Callable[[], PhaseResult]]] = [
            (1, "Graph Similarity Store", self.phase_1),
            (2, "Causal Graph Meta-Cognition", self.phase_2),
            (3, "DAG Carbon Ledger", self.phase_3),
            (4, "Policy Graph Engine", self.phase_4),
            (5, "Dual-Graph Evaluator", self.phase_5),
            (6, "Advanced Enhancements", self.phase_6),
        ]

        results: List[PhaseResult] = []
        for number, title, fn in phases:
            if number in skip:
                self._say(f"\n⏭️  Phase {number} — {title} (skipped)")
                continue
            try:
                r = fn()
            except Exception as exc:                     # noqa: BLE001
                self._say(
                    f"\n✗ Phase {number} — {title} crashed: "
                    f"{type(exc).__name__}: {exc}",
                    error=True,
                )
                r = PhaseResult(number, title, ok=False,
                                errors=[f"crashed: {exc}"])
            results.append(r)
            if not r.ok:
                self._say(
                    f"   ✗ Phase {number} failures: {r.errors}",
                    error=True,
                )

        failures = sum(1 for r in results if not r.ok)
        self._say("")
        self._say("=" * 80)
        if failures == 0:
            self._say(f"✅ All {len(results)} phases passed")
        else:
            self._say(f"❌ {failures} of {len(results)} phases failed")
        self._say("=" * 80)
        return results


# ---------------------------------------------------------------------------
# Self-test — validate helpers without touching the code under test
# ---------------------------------------------------------------------------
def _self_test() -> int:
    failures = 0

    def _check(name: str, condition: bool, detail: str = "") -> None:
        nonlocal failures
        if condition:
            print(f"  ✓ {name}")
        else:
            print(f"  ✗ {name}{(': ' + detail) if detail else ''}")
            failures += 1

    print("_safe_get:")
    _check("nested dict", _safe_get({"a": {"b": 1}}, "a", "b") == 1)
    _check(
        "missing -> default",
        _safe_get({"a": {}}, "a", "b", default="d") == "d",
    )
    _check("none", _safe_get(None, "a") is None)

    print("_filter_kwargs:")
    class _Foo:
        def __init__(self, x, y=1):
            self.x, self.y = x, y

    _check(
        "filters unknown",
        _filter_kwargs(_Foo, {"x": 1, "y": 2, "z": 3}) == {"x": 1, "y": 2},
    )

    class _VarKw:
        def __init__(self, **kw):
            self.kw = kw

    _check(
        "passes all when **kwargs",
        _filter_kwargs(_VarKw, {"a": 1, "b": 2}) == {"a": 1, "b": 2},
    )

    print("_run_coro:")
    async def _coro():
        return 42

    _check("sync context", _run_coro(_coro()) == 42)

    print("_get_enum_member:")
    class _Status:
        SUCCESS = "success"
        TIMEOUT = "timeout"

    _check(
        "first match",
        _get_enum_member(_Status, "TIMEOUT", "SUCCESS") == "timeout",
    )
    _check(
        "fallback",
        _get_enum_member(_Status, "NOPE", default="x") == "x",
    )
    _check("none class", _get_enum_member(None, "X", default="d") == "d")

    print("_try_construct:")
    class _NeedsAll:
        def __init__(self, a, b):
            if a < 0:
                raise ValueError("negative a")
            self.a, self.b = a, b

    say_lines: List[str] = []
    out = _try_construct(
        _NeedsAll, {"a": 1, "b": 2, "c": 3}, "NeedsAll",
        say=lambda m: say_lines.append(m),
    )
    _check("succeeds filtered", out is not None and out.a == 1)

    say_lines.clear()
    out = _try_construct(
        _NeedsAll, {"a": -1, "b": 2}, "NeedsAll",
        fallback_kwargs={"a": 5, "b": 6},
        say=lambda m: say_lines.append(m),
    )
    _check(
        "falls back on failure",
        out is not None and out.a == 5,
    )

    say_lines.clear()
    out = _try_construct(
        _NeedsAll, {"a": -1, "b": -2}, "NeedsAll",
        fallback_kwargs={"a": -3, "b": -4},
        say=lambda m: say_lines.append(m),
    )
    _check("returns None when both fail", out is None)

    print("")
    if failures:
        print(f"{failures} failure(s)")
        return 1
    print("all self-tests passed")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Green Agent — Complete Enhancement Demo (Enhanced v2)",
    )
    p.add_argument("--phase", type=int, default=None,
                   help="run only this phase (1-6)")
    p.add_argument("--skip", default="",
                   help="comma-separated phase numbers to skip (e.g. 2,6)")
    p.add_argument("--json", action="store_true",
                   help="emit a machine-readable summary at the end")
    p.add_argument("--quiet", action="store_true",
                   help="suppress informational output; errors to stderr")
    p.add_argument("--verbose", action="store_true",
                   help="print extra internals")
    p.add_argument("--self-test", action="store_true",
                   help="run helper self-tests and exit")
    p.add_argument("--no-cleanup", action="store_true",
                   help="keep the temp directory on exit")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    if args.self_test:
        return _self_test()

    skip = [int(s) for s in args.skip.split(",") if s.strip().isdigit()]
    if args.phase is not None:
        if not 1 <= args.phase <= 6:
            print(f"--phase must be 1-6, got {args.phase}", file=sys.stderr)
            return 2
        skip = [n for n in range(1, 7) if n != args.phase]

    demo = GreenAgentDemo(quiet=args.quiet, verbose=args.verbose)
    exit_code = 0
    try:
        results = demo.run(skip=skip)
        failures = sum(1 for r in results if not r.ok)
        exit_code = 0 if failures == 0 else 1

        if args.json:
            print(json.dumps(
                {
                    "version": "5.0.0",
                    "core_available": CORE_AVAILABLE,
                    "enhancements_available": ENHANCEMENTS_AVAILABLE,
                    "import_errors": _IMPORT_ERRORS,
                    "phases": [r.to_dict() for r in results],
                    "failures": failures,
                },
                indent=2,
                default=str,
            ))

        if not args.quiet and not args.json:
            print()
            print("Next steps:")
            print("  - Address any ✗ items above before submitting")
            print("  - Run with --json for machine-readable output")
            print("  - Run with --self-test to verify the demo helpers")
    finally:
        demo.close(keep=args.no_cleanup)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
