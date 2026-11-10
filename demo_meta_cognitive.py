#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
demo_meta_cognitive.py — Meta-Cognitive Green Agent demo (v2.0)

Demonstrates the meta-cognitive architecture (reflection, long-context
reasoning, RLHF feedback, Pareto analysis, dashboard) with optional
integration of the advanced enhancement modules.

Enhanced in v2.0
----------------
- ``_maybe_await`` helper: works whether the enhancement methods are
  **sync** (per ``INSTALLATION.md §2``) or **async** — so the demo cannot
  break on an async/sync drift between docs and code.
- Fixed ``FeedbackEvent`` construction: ``performance.energy_kwh`` (per
  ``README.md §2.5`` — not ``energy_joules``), documented keys only, and
  no reliance on an undocumented ``event_id`` attribute.
- Per-import diagnostics: a missing enhancement module logs the module and
  the exception, keeps working enhancements enabled, and does not silently
  disable the whole layer.
- CLI flags: ``--enhanced``/``--no-enhanced``, ``--quiet``, ``--steps N``,
  ``--output-dir PATH``, ``--policy-file PATH``, ``--verbose``.
- Artifacts written under ``<output-dir>/<timestamp>/`` — no more
  overwriting user files on re-run.
- ``tempfile.gettempdir()`` for the DAG ledger path (Windows-safe).
- Full type hints, docstrings, structured logging, and exit codes
  (0 = success, 1 = partial, 2 = config error).

Canonical async surface (see ``INSTALLATION.md §2``)
----------------------------------------------------
- ``NodeDescriptor.select_routing_strategy``  — sync
- ``WorkloadDescriptor.select_priority``      — sync
- ``asyncio.run(...)`` is used only at the top level of this script.

Run
---
    python demo_meta_cognitive.py                # base demo only
    python demo_meta_cognitive.py --enhanced     # base + enhancements
    python demo_meta_cognitive.py --steps 5 --quiet

Exit codes
----------
    0  base demo and (if enabled) enhancements succeeded
    1  one or more steps failed but the run completed
    2  CLI / environment error (missing policy file, bad args, etc.)
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import sys
import tempfile
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Optional

logger = logging.getLogger("demo_meta_cognitive")


# =============================================================================
# Named constants (previously inline magic numbers)
# =============================================================================

_STEP_SLEEP_S = 0.10
_SLOW_STEP_SLEEP_S = 0.05
_SLOW_STEP_EXTRA_TOOL_CALLS = 2
_DEFAULT_STEPS = 15
_REFLECTION_FREQUENCY = 5
_HISTORICAL_RUNS = 3
_BASELINE_ENERGY_MULTIPLIER = 1.2
_BASELINE_LATENCY_MULTIPLIER = 0.8
_BACKPROPAGATE_TRANSFER_RATE = 0.3
_DEFAULT_POLICY_FILE = "green_policy.yaml"


# =============================================================================
# Required imports (base demo) — hard requirements
# =============================================================================
try:
    from src.monitoring.metrics_collector import MetricsCollector
    from src.reflection.reflection_engine import ReflectionEngine
    from src.reflection.long_context_reasoner import LongContextReasoner
    from src.policy.policy_engine import PolicyEngine
    from src.policy.policy_feedback import PolicyFeedback
    from src.analysis.pareto_analyzer import ParetoAnalyzer
    from src.memory.run_memory import RunMemory
    from src.dashboard.green_dashboard import GreenDashboard
except ImportError as _exc:  # pragma: no cover - environment error path
    print(
        f"FATAL: missing required dependency for base demo: {_exc}\n"
        "Install with: pip install -e . --no-deps",
        file=sys.stderr,
    )
    sys.exit(2)


# =============================================================================
# Optional enhancement imports — per-module isolation
# =============================================================================
#
# Each enhancement is imported independently so a missing or broken module
# logs exactly which one failed, keeps the rest enabled, and does not
# silently disable the whole layer (the v1.0 bug).
# =============================================================================


def _try_import(
    name: str,
    module: str,
    symbols: tuple[str, ...],
) -> tuple[Optional[ModuleType], dict[str, Any], Optional[BaseException]]:
    """
    Attempt to import ``symbols`` from ``module``.

    Returns ``(module_obj, {symbol: value}, None)`` on success, or
    ``(None, {}, exc)`` on failure. The exception is captured, not raised.
    """
    try:
        mod = __import__(module, fromlist=list(symbols))
    except ImportError as exc:
        logger.debug("Enhancement import failed: %s — %s", name, exc)
        return None, {}, exc
    except Exception as exc:  # a real bug in the module
        logger.warning(
            "Enhancement module %s raised %s during import: %s",
            name,
            type(exc).__name__,
            exc,
        )
        return None, {}, exc

    resolved: dict[str, Any] = {}
    for sym in symbols:
        value = getattr(mod, sym, None)
        if value is None:
            logger.warning(
                "Enhancement module %s imported but missing symbol %r",
                name,
                sym,
            )
        resolved[sym] = value
    return mod, resolved, None


_NODE_DESCRIPTOR_MOD, _node_symbols, _node_exc = _try_import(
    "NodeDescriptor",
    "src.enhancements.schemas.node_descriptor",
    ("NodeDescriptor", "NodeType"),
)
_WORKLOAD_DESCRIPTOR_MOD, _wl_symbols, _wl_exc = _try_import(
    "WorkloadDescriptor",
    "src.enhancements.schemas.workload_descriptor",
    ("WorkloadDescriptor", "TaskType", "Urgency"),
)
_FEEDBACK_EVENT_MOD, _feedback_symbols, _feedback_exc = _try_import(
    "FeedbackEvent",
    "src.enhancements.schemas.feedback_event",
    ("FeedbackEvent",),
)
_ZERO_TRUST_MOD, _zt_symbols, _zt_exc = _try_import(
    "ZeroTrustArchitecture",
    "src.enhancements.zero_trust_architecture",
    ("ZeroTrustArchitecture", "ZeroTrustConfig"),
)
_GRAPH_REGISTRY_MOD, _graph_symbols, _graph_exc = _try_import(
    "GraphRegistry",
    "src.enhancements.core.graph_registry",
    ("GraphRegistry", "GraphType"),
)
_CAUSAL_GRAPH_MOD, _causal_symbols, _causal_exc = _try_import(
    "CausalGraph",
    "src.enhancements.core.causal_graph",
    ("CausalGraph",),
)
_META_COGNITION_MOD, _meta_symbols, _meta_exc = _try_import(
    "MetaCognitionLayer",
    "src.enhancements.core.meta_cognition",
    ("MetaCognitionLayer",),
)
_DAG_LEDGER_MOD, _ledger_symbols, _ledger_exc = _try_import(
    "DAGCarbonLedger",
    "src.enhancements.metrics.dag_carbon_ledger",
    ("DAGCarbonLedger",),
)

# Public aliases (None when the corresponding import failed).
NodeDescriptor: Any = _node_symbols.get("NodeDescriptor")
NodeType: Any = _node_symbols.get("NodeType")
WorkloadDescriptor: Any = _wl_symbols.get("WorkloadDescriptor")
TaskType: Any = _wl_symbols.get("TaskType")
Urgency: Any = _wl_symbols.get("Urgency")
FeedbackEvent: Any = _feedback_symbols.get("FeedbackEvent")
ZeroTrustArchitecture: Any = _zt_symbols.get("ZeroTrustArchitecture")
ZeroTrustConfig: Any = _zt_symbols.get("ZeroTrustConfig")
GraphRegistry: Any = _graph_symbols.get("GraphRegistry")
GraphType: Any = _graph_symbols.get("GraphType")
CausalGraph: Any = _causal_symbols.get("CausalGraph")
MetaCognitionLayer: Any = _meta_symbols.get("MetaCognitionLayer")
DAGCarbonLedger: Any = _ledger_symbols.get("DAGCarbonLedger")

_ENHANCEMENT_STATUS: dict[str, Optional[BaseException]] = {
    "NodeDescriptor": _node_exc,
    "WorkloadDescriptor": _wl_exc,
    "FeedbackEvent": _feedback_exc,
    "ZeroTrustArchitecture": _zt_exc,
    "GraphRegistry": _graph_exc,
    "CausalGraph": _causal_exc,
    "MetaCognitionLayer": _meta_exc,
    "DAGCarbonLedger": _ledger_exc,
}


def _missing_enhancements() -> list[str]:
    """Return names of enhancement modules whose import failed."""
    return [name for name, exc in _ENHANCEMENT_STATUS.items() if exc is not None]


# =============================================================================
# Sync/async compatibility helper
# =============================================================================


async def _maybe_await(value: Any) -> Any:
    """
    Await ``value`` if it is awaitable; return it otherwise.

    This makes the demo resilient to a documentation/code drift where
    ``NodeDescriptor.select_routing_strategy`` might be sync (per
    ``INSTALLATION.md §2``) or async (per prior demos).
    """
    if inspect.isawaitable(value):
        return await value
    return value


def _run(coro_or_value: Any) -> Any:
    """Run a possibly-async value synchronously at the top level."""
    if not inspect.isawaitable(coro_or_value):
        return coro_or_value
    return asyncio.run(_maybe_await(coro_or_value))


def _raise_missing(name: str) -> Any:
    """Raise a clear error when a required enhancement module is missing."""
    raise RuntimeError(
        f"Enhancement module {name!r} is not available. "
        f"Missing: {_missing_enhancements()}"
    )


# =============================================================================
# Base demo
# =============================================================================


def _load_policy_engine(policy_file: str) -> Optional[PolicyEngine]:
    """
    Load ``PolicyEngine`` from ``policy_file``.

    Returns ``None`` if the file is missing and no defaults constructor is
    available — the caller decides whether that is fatal.
    """
    path = Path(policy_file)
    if not path.is_file():
        logger.warning(
            "Policy file %r not found; trying defaults", policy_file,
        )
        try:
            return PolicyEngine(policy_file=None)  # type: ignore[arg-type]
        except Exception:
            return None
    try:
        return PolicyEngine(policy_file=policy_file)
    except Exception as exc:
        logger.error("PolicyEngine(%r) failed: %s", policy_file, exc)
        return None


def simulate_agent_execution(
    *,
    steps: int = _DEFAULT_STEPS,
    output_dir: Path,
    run_enhancements: bool,
    policy_file: str = _DEFAULT_POLICY_FILE,
) -> int:
    """
    Run the base meta-cognitive demo.

    Returns
    -------
    int
        0 on success; 1 if any step failed but the demo completed; 2 if a
        required component could not be initialized.
    """
    if steps < 1:
        raise ValueError(f"steps must be >= 1 (got {steps})")

    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Starting meta-cognitive demo (steps=%d, enhanced=%s)",
        steps, run_enhancements,
    )
    print("🌱 Green Agent Meta-Cognitive Demo")
    print("=" * 60)

    # --- Component initialization (fatal if any fail) -------------------
    try:
        policy = _load_policy_engine(policy_file)
        if policy is None:
            print(
                f"FATAL: could not initialize PolicyEngine "
                f"(policy_file={policy_file!r}). "
                f"Provide a valid policy file or run from the repo root.",
                file=sys.stderr,
            )
            return 2

        metrics_collector = MetricsCollector()
        reflection_engine = ReflectionEngine(
            reflection_frequency=_REFLECTION_FREQUENCY,
            policy_budgets=policy.get_budgets(),
        )
        long_context_reasoner = LongContextReasoner()
        run_memory = RunMemory()
        feedback_system = PolicyFeedback()
        pareto_analyzer = ParetoAnalyzer()
        dashboard = GreenDashboard()
    except Exception:
        logger.exception("Component initialization failed")
        return 2

    # --- Load historical context (tolerate None / [] / list) ----------
    try:
        historical_runs = run_memory.get_recent_runs(_HISTORICAL_RUNS) or []
        for hist_run in historical_runs:
            long_context_reasoner.add_run_to_history(hist_run)
        print(f"📚 Loaded {len(historical_runs)} historical runs\n")
    except Exception:
        logger.exception("Failed to load historical runs; continuing without")
        historical_runs = []

    all_reflections: list[dict[str, Any]] = []
    step_failures = 0

    # --- Main loop -----------------------------------------------------
    for step in range(1, steps + 1):
        print(f"\n{'='*60}")
        print(f"Step {step}/{steps}")
        print(f"{'='*60}")

        try:
            metrics_collector.start_step()

            # Simulate work with varying resource usage.
            time.sleep(_STEP_SLEEP_S)
            if step % 3 == 0:
                time.sleep(_SLOW_STEP_SLEEP_S)
                for _ in range(_SLOW_STEP_EXTRA_TOOL_CALLS):
                    metrics_collector.record_tool_call()
            else:
                metrics_collector.record_tool_call()

            snapshot = metrics_collector.collect_snapshot()
            print(
                f"📊 Metrics: Energy={snapshot.energy_wh:.4f}Wh, "
                f"Latency={snapshot.latency_ms:.1f}ms, "
                f"Memory={snapshot.memory_mb:.1f}MB"
            )

            # Reflection checkpoint
            if reflection_engine.should_reflect(step):
                print("\n🤔 REFLECTION CHECKPOINT")
                print("-" * 60)

                reflection_metrics = (
                    metrics_collector.get_metrics_for_reflection()
                )
                reflection = reflection_engine.generate_reflection(
                    step=step,
                    metrics=reflection_metrics,
                    timestamp=datetime.now(timezone.utc).timestamp(),
                )

                print(f"💭 Self-Explanation:\n   {reflection.self_explanation}")
                print(f"\n🎯 Decision: {reflection.decision}")
                print(f"📈 Confidence: {reflection.confidence:.2f}")

                budget_status = getattr(reflection, "budget_status", {}) or {}
                violations = budget_status.get("violations", [])
                warnings = budget_status.get("warnings", [])
                if violations:
                    print(f"⚠️  Violations: {violations}")
                if warnings:
                    print(f"⚡ Warnings: {warnings}")

                all_reflections.append(reflection.to_dict())

                if policy.should_self_adjust(reflection_metrics):
                    print("\n⚙️  SELF-ADJUSTMENT TRIGGERED")
                    adjustment = policy.apply_adaptive_adjustment(
                        reflection.decision,
                    ) or {}
                    print(f"   Changes: {adjustment.get('changes', [])}")

                insights = long_context_reasoner.compare_with_past_runs(
                    reflection_metrics,
                ) or []
                if insights:
                    print("\n🔍 Long-Context Insights:")
                    for insight in insights[:2]:
                        print(
                            f"   • {insight.description} "
                            f"(confidence: {insight.confidence:.2f})"
                        )

                patterns = reflection_engine.identify_patterns() or []
                if patterns:
                    print("\n📈 Patterns Identified:")
                    for pattern in patterns:
                        print(f"   • {pattern}")

                print("-" * 60)
        except Exception:
            step_failures += 1
            logger.exception("Step %d failed; continuing to next step", step)
            print(f"\n[FAIL] Step {step} raised; continuing.")

    # --- Post-run analysis --------------------------------------------
    print(f"\n\n{'='*60}")
    print("EXECUTION COMPLETE - GENERATING ANALYSIS")
    print(f"{'='*60}\n")

    try:
        cumulative = metrics_collector.get_cumulative_metrics()
    except Exception:
        logger.exception("get_cumulative_metrics failed")
        return 1

    print("📊 Cumulative Metrics:")
    print(f"   Total Energy: {cumulative.get('total_energy_wh', 0.0):.4f} Wh")
    print(f"   Total Carbon: {cumulative.get('total_carbon_kg', 0.0):.6f} kg")
    print(
        f"   Total Latency: "
        f"{cumulative.get('total_latency_ms', 0.0):.1f} ms"
    )
    print(f"   Total Tool Calls: {cumulative.get('total_tool_calls', 0)}")
    print(f"   Total Reflections: {len(all_reflections)}")

    try:
        run_memory.add_run({
            "cumulative": cumulative,
            "reflections": all_reflections,
            "budget_status": (
                all_reflections[-1].get("budget_status", {})
                if all_reflections else {}
            ),
        })
    except Exception:
        logger.exception("run_memory.add_run failed; continuing")

    # --- Meta-policy ------------------------------------------------
    print("\n🧬 Generating Meta-Policy...")
    try:
        meta_policy = run_memory.generate_meta_policy() or {}
        for rec in meta_policy.get("recommendations", []) or []:
            print(
                f"   • {rec.get('metric', '?')}: "
                f"{rec.get('action', '?')} - {rec.get('reason', '?')}"
            )
    except Exception:
        logger.exception("generate_meta_policy failed")

    # --- Long-term patterns ----------------------------------------
    print("\n🔬 Long-Term Pattern Analysis...")
    try:
        long_term_patterns = (
            long_context_reasoner.identify_long_term_patterns() or []
        )
        for pattern in long_term_patterns:
            print(f"   • {pattern.description}")
    except Exception:
        logger.exception("identify_long_term_patterns failed")

    # --- Pareto analysis -------------------------------------------
    pareto_position: dict[str, Any] = {}
    try:
        agents = [
            {"query_id": "demo_agent", **cumulative},
            {
                "query_id": "baseline",
                "total_energy_wh": (
                    cumulative.get("total_energy_wh", 0.0)
                    * _BASELINE_ENERGY_MULTIPLIER
                ),
                "total_carbon_kg": (
                    cumulative.get("total_carbon_kg", 0.0)
                    * _BASELINE_ENERGY_MULTIPLIER
                ),
                "total_latency_ms": (
                    cumulative.get("total_latency_ms", 0.0)
                    * _BASELINE_LATENCY_MULTIPLIER
                ),
            },
        ]
        pareto_position = (
            pareto_analyzer.analyze_agent_position(agents[0], agents) or {}
        )
        print("\n🏆 Pareto Analysis:")
        print(f"   Position: {pareto_position.get('position', '?')}")
        eff = pareto_position.get("efficiency_score")
        if isinstance(eff, (int, float)):
            print(f"   Efficiency Score: {eff:.3f}")
        else:
            print("   Efficiency Score: n/a")
    except Exception:
        logger.exception("Pareto analysis failed")
        pareto_position = {}

    # --- Dual-layer feedback ---------------------------------------
    print("\n📋 Dual-Layer Feedback:")
    try:
        dual_feedback = feedback_system.generate_dual_layer_feedback(
            pareto_analysis=pareto_position,
            reflections=all_reflections,
            metrics={"cumulative": cumulative},
        ) or {}
        synthesis = dual_feedback.get("synthesis", {}) or {}
        print(f"   Alignment: {synthesis.get('alignment', '?')}")
        print(f"   Synthesis: {synthesis.get('synthesis_text', '?')}")
    except Exception:
        logger.exception("dual-layer feedback failed")

    # --- Dashboard ------------------------------------------------
    try:
        dashboard.add_agent_data(
            agent_id="demo_agent",
            metrics={"cumulative": cumulative},
            reflections=all_reflections,
            pareto_position=pareto_position,
        )
    except Exception:
        logger.exception("dashboard.add_agent_data failed")

    # --- Export artifacts ----------------------------------------
    print("\n💾 Exporting Artifacts...")

    def _try_export(
        label: str,
        fn: Callable[[], Any],
    ) -> None:
        try:
            fn()
            print(f"   ✓ {label}")
        except Exception:
            logger.exception("export %s failed", label)
            print(f"   ✗ {label} (see logs)")

    _try_export(
        "metrics_history.json",
        lambda: metrics_collector.export_history(
            str(output_dir / "metrics_history.json"),
        ),
    )
    _try_export(
        "reflections.json",
        lambda: reflection_engine.export_reflections(
            str(output_dir / "reflections.json"),
        ),
    )
    _try_export(
        "reasoning_insights.json",
        lambda: long_context_reasoner.export_reasoning_history(
            str(output_dir / "reasoning_insights.json"),
        ),
    )
    _try_export(
        "dashboard_data.json",
        lambda: dashboard.export_dashboard(
            str(output_dir / "dashboard_data.json"),
        ),
    )
    _try_export(
        "dashboard.html",
        lambda: dashboard.generate_html_report(
            str(output_dir / "dashboard.html"),
        ),
    )

    # --- Historical summary --------------------------------------
    print("\n📚 Historical Summary:")
    try:
        hist_summary = run_memory.get_historical_summary() or {}
        print(f"   Total Runs: {hist_summary.get('total_runs', '?')}")
        avg = hist_summary.get("avg_energy_per_run")
        if isinstance(avg, (int, float)):
            print(f"   Avg Energy/Run: {avg:.4f} Wh")
        else:
            print("   Avg Energy/Run: n/a")
        print(
            f"   Meta-Policies Generated: "
            f"{hist_summary.get('meta_policies_generated', '?')}"
        )
    except Exception:
        logger.exception("get_historical_summary failed")

    # --- Base demo complete --------------------------------------
    print(f"\n{'='*60}")
    if step_failures == 0:
        print("✅ Base demo complete!")
    else:
        print(f"⚠️  Base demo finished with {step_failures} step failure(s)")
    print(f"{'='*60}")
    print(f"\n📊 Artifacts written to: {output_dir.resolve()}")

    # --- Advanced enhancements (if enabled) ----------------------
    enhancement_rc = 0
    if run_enhancements:
        enhancement_rc = _demonstrate_enhancements(output_dir=output_dir)
    else:
        print("\n[i] Enhancements disabled (use --enhanced to enable).")

    # --- Final exit code -----------------------------------------
    if step_failures > 0 or enhancement_rc != 0:
        return 1
    return 0


# =============================================================================
# Enhancement demo
# =============================================================================


def _demonstrate_enhancements(*, output_dir: Path) -> int:
    """
    Run the optional advanced-enhancements demo.

    Each of the nine sections is isolated — one failing section logs and
    continues, so the demo produces maximum diagnostic value in one run.

    Returns
    -------
    int
        0 if all sections succeeded; the number of failed sections otherwise.
    """
    print("\n" + "=" * 60)
    print("ADVANCED ENHANCEMENTS DEMO")
    print("=" * 60)

    missing = _missing_enhancements()
    if missing:
        print(
            f"\n⚠️  {len(missing)} enhancement module(s) unavailable: "
            f"{', '.join(missing)}."
        )
        print("    Available modules will still be exercised.\n")

    failures = 0

    def _section(label: str, fn: Callable[[], Any]) -> Any:
        nonlocal failures
        print(f"\n{label}")
        try:
            result = fn()
            print("   ✓ done")
            return result
        except Exception:
            failures += 1
            logger.exception("%s failed", label)
            print("   ✗ failed (see logs)")
            return None

    # 1. NodeDescriptor
    node = _section(
        "1. Creating NodeDescriptor (LIMIT Graph + RLHF + MoE + evolutionary)...",
        lambda: (
            NodeDescriptor(
                id="demo_meta_node",
                type=NodeType.EDGE,
                region="us-east",
                region_carbon_intensity=350.0,
                energy_per_token=0.00004,
                use_evolutionary=True,
                human_feedback_score=0.7,
                graph_metrics={"centrality": 0.8, "connectivity": 0.6},
                metadata={"gating_lr": 0.005},
            )
            if NodeDescriptor and NodeType
            else _raise_missing("NodeDescriptor")
        ),
    )
    if node is not None:
        centrality = (
            getattr(node, "graph_metrics", {}) or {}
        ).get("centrality")
        print(f"     - Graph centrality: {centrality}")
        print(
            f"     - Human feedback: "
            f"{getattr(node, 'human_feedback_score', '?')}"
        )

    # 2. Routing strategy (sync OR async — _run handles both)
    strategy = _section(
        "2. Selecting routing strategy (distillation + MoE)...",
        lambda: (
            _run(node.select_routing_strategy(exploration=False))
            if node is not None
            else _raise_missing("NodeDescriptor")
        ),
    )
    if strategy is not None:
        print(f"     - Strategy: {strategy}")

    # 3. WorkloadDescriptor
    workload = _section(
        "3. Creating WorkloadDescriptor (MODP weights + RLHF)...",
        lambda: (
            WorkloadDescriptor(
                task_id="meta_demo_task",
                task_type=TaskType.INFERENCE,
                tokens=1000,
                latency_target=300.0,
                urgency=Urgency.MEDIUM,
                use_evolutionary=True,
                human_feedback_score=0.6,
                graph_metrics={"centrality": 0.7},
                metadata={
                    "latency_weight": 0.5,
                    "carbon_weight": 0.3,
                    "energy_weight": 0.2,
                },
            )
            if WorkloadDescriptor and TaskType and Urgency
            else _raise_missing("WorkloadDescriptor")
        ),
    )
    if workload is not None:
        print(f"     - Task: {getattr(workload, 'task_id', '?')}")

    # 4. Priority selection
    priority = _section(
        "4. Selecting priority (distillation + MoE)...",
        lambda: (
            _run(workload.select_priority(exploration=False))
            if workload is not None
            else _raise_missing("WorkloadDescriptor")
        ),
    )
    if priority is not None:
        print(f"     - Priority: {priority}")

    # 5. FeedbackEvent (schema-compliant: energy_kwh, documented keys)
    event = _section(
        "5. Creating FeedbackEvent (enhanced fields, schema-compliant)...",
        lambda: (
            FeedbackEvent(
                source="meta_cognitive_demo",
                feedback_type="routing",
                task_id="demo_enhanced",
                action={
                    "selected_action": str(strategy),
                    "selected_rank": 1,
                },
                performance={
                    # Per README.md §2.5: energy_kwh (not energy_joules).
                    "quality_score": 0.9,
                    "latency_ms": 100.0,
                    "energy_kwh": 0.0001,       # 100 J → 0.0001 kWh
                },
                adaptive_cost_value=0.85,
                graph_metrics={"centrality": 0.8},
                human_feedback_score=0.7,
                modp_score=0.75,
            )
            if FeedbackEvent
            else _raise_missing("FeedbackEvent")
        ),
    )
    if event is not None:
        print(
            f"     - MODP score: {getattr(event, 'modp_score', '?')}, "
            f"graph centrality: "
            f"{(getattr(event, 'graph_metrics', {}) or {}).get('centrality')}"
        )

    # 6. Zero Trust
    _section(
        "6. Initializing Zero Trust with enhancements...",
        lambda: (
            ZeroTrustArchitecture(
                config=ZeroTrustConfig(
                    use_enhancements=True,
                    use_distillation=True,
                    use_evolutionary=True,
                ),
            )
            if ZeroTrustArchitecture and ZeroTrustConfig
            else _raise_missing("ZeroTrustArchitecture")
        ),
    )

    # 7. LIMIT Graph + Meta-Cognition
    def _graph_and_meta() -> None:
        if not (GraphRegistry and GraphType and MetaCognitionLayer):
            _raise_missing(
                "GraphRegistry / GraphType / MetaCognitionLayer"
            )
        registry = GraphRegistry()
        causal_graph = registry.get_or_create(GraphType.CAUSAL)
        meta = MetaCognitionLayer(causal_graph=causal_graph)
        meta.observe_snapshot({
            "CarbonIntensity": 430.0,
            "CarbonIntensity_high_threshold": 400.0,
            "GridStrain": 0.91,
        })
        report = meta.diagnose() or {}
        print(f"     - Diagnosis status: {report.get('status', '?')}")
        print(
            f"     - Recommended action: "
            f"{report.get('recommended_action', '?')}"
        )

    _section("7. LIMIT Graph + Meta-Cognition...", _graph_and_meta)

    # 8. DAG Carbon Ledger (tempdir for cross-platform)
    def _ledger_run() -> None:
        if not DAGCarbonLedger:
            _raise_missing("DAGCarbonLedger")
        with tempfile.TemporaryDirectory(prefix="meta_demo_ledger_") as tmp:
            ledger = DAGCarbonLedger(storage_path=tmp)
            node_a = ledger.add_execution(
                task_id="A",
                framework="langchain",
                energy_kwh=0.01,
                carbon_co2e_kg=0.002,
                accuracy=0.9,
                sustainability_index=0.8,
            )
            node_b = ledger.add_execution(
                task_id="B",
                framework="langchain",
                energy_kwh=0.008,
                carbon_co2e_kg=0.0016,
                accuracy=0.88,
                sustainability_index=0.75,
                parent_task_ids=[node_a],
                dependency_type="model_state",
            )
            attributed_kg_co2e = ledger.backpropagate_carbon(
                node_b, transfer_rate=_BACKPROPAGATE_TRANSFER_RATE,
            )
            print(f"     - Attributed: {attributed_kg_co2e} kg CO₂e")

    _section("8. DAG Carbon Ledger (tempdir-scoped)...", _ledger_run)

    # 9. FlexGen config (illustrative only — no module required)
    print("\n9. FlexGen integration hooks (illustrative config)...")
    flexgen_config = {
        "enabled": True,
        "model_name": "facebook/opt-6.7b",
        "batch_size": 16,
        "delegation_policy": "adaptive",
    }
    print(f"   ✓ FlexGen config: {flexgen_config}")

    # --- Summary --------------------------------------------------
    print("\n" + "=" * 60)
    if failures == 0:
        print("✅ Advanced Enhancements demo completed successfully")
    else:
        print(
            f"⚠️  Advanced Enhancements finished with {failures} failure(s)"
        )
    print("=" * 60 + "\n")
    return failures


# =============================================================================
# CLI
# =============================================================================


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="demo_meta_cognitive",
        description=(
            "Meta-Cognitive Green Agent demo. Runs the base demo by "
            "default; add --enhanced to exercise the optional advanced "
            "modules."
        ),
    )
    p.add_argument(
        "--steps", type=int, default=_DEFAULT_STEPS,
        help=f"Number of simulated steps (default: {_DEFAULT_STEPS})",
    )
    p.add_argument(
        "--enhanced", dest="enhanced", action="store_true", default=None,
        help="Force-enable enhancements (overrides auto-detect).",
    )
    p.add_argument(
        "--no-enhanced", dest="enhanced", action="store_false",
        help="Skip the enhancements demo even if modules are installed.",
    )
    p.add_argument(
        "--output-dir", type=Path, default=None,
        help=(
            "Directory for artifacts "
            "(default: ./demo_output/<timestamp>/)."
        ),
    )
    p.add_argument(
        "--policy-file", default=_DEFAULT_POLICY_FILE,
        help=f"Path to green_policy.yaml (default: {_DEFAULT_POLICY_FILE})",
    )
    p.add_argument(
        "--quiet", action="store_true",
        help="Suppress info logging; errors are still printed.",
    )
    p.add_argument(
        "--verbose", action="store_true",
        help="Enable debug logging.",
    )
    return p


def _configure_logging(args: argparse.Namespace) -> None:
    if args.verbose:
        level = logging.DEBUG
    elif args.quiet:
        level = logging.ERROR
    else:
        level = logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    )


def _resolve_output_dir(arg: Optional[Path]) -> Path:
    if arg is not None:
        return arg
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("demo_output") / stamp


def _resolve_run_enhancements(flag: Optional[bool]) -> bool:
    """
    ``--enhanced`` forces True; ``--no-enhanced`` forces False; default
    (None) means "auto" = run if any enhancement module was importable.
    """
    if flag is not None:
        return flag
    return any(
        name is not None for name in [
            NodeDescriptor, WorkloadDescriptor, FeedbackEvent,
            ZeroTrustArchitecture, GraphRegistry, MetaCognitionLayer,
            DAGCarbonLedger,
        ]
    )


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    _configure_logging(args)

    try:
        output_dir = _resolve_output_dir(args.output_dir)
    except OSError as exc:
        print(
            f"FATAL: could not resolve output dir: {exc}",
            file=sys.stderr,
        )
        return 2

    run_enh = _resolve_run_enhancements(args.enhanced)
    if run_enh and _missing_enhancements():
        logger.warning(
            "Enhancements enabled but %d module(s) missing: %s",
            len(_missing_enhancements()),
            ", ".join(_missing_enhancements()),
        )

    try:
        return simulate_agent_execution(
            steps=args.steps,
            output_dir=output_dir,
            run_enhancements=run_enh,
            policy_file=args.policy_file,
        )
    except ValueError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
