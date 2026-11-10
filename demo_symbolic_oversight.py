#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
demo_neuro_symbolic.py — Neuro-Symbolic Oversight demo (v2.0)

Demonstrates the symbolic reasoning engine (rule evaluation, formal
violation traces, category filtering, dashboard visualization, sustained
reflection) with optional integration of the advanced enhancement modules.

Enhanced in v2.0
----------------
- ``_maybe_await`` / ``_run`` helpers: works whether the enhancement
  methods are **sync** (per ``INSTALLATION.md §2``) or **async** — so the
  demo cannot break on a docs/code drift.
- Fixed ``FeedbackEvent`` construction: ``performance.energy_kwh`` (per
  ``README.md §2.5`` — not ``energy_joules``), documented keys only, and
  no reliance on an undocumented ``event_id`` attribute.
- Per-module enhancement import diagnostics — a missing module logs which
  one failed, and working enhancements stay enabled.
- CLI flags: ``--enhanced`` / ``--no-enhanced``, ``--quiet`` / ``--verbose``,
  ``--steps N``, ``--output-dir PATH``, ``--policy-file PATH``.
- Demo 6 (enhancements) is **opt-in** via ``--enhanced``; auto-detected
  when modules are present but can be skipped with ``--no-enhanced``.
- HTML artifact written to ``<output-dir>/demo_symbolic_dashboard.html``
  — no more cwd overwrites.
- ``tempfile.TemporaryDirectory()`` for the DAG ledger path (Windows-safe).
- ``violation.rule_id`` parsed with a regex; any non-matching rule falls
  back to ``OTHER`` instead of raising ``IndexError``.
- Guarded ``dict`` / attribute access throughout.
- Policy preflight: reads ``symbolic_policy.yaml`` thresholds if available
  and prints them, so "found N violations" is verifiable.
- Demo 1 warns loudly if it found 0 violations (likely means the policy
  file thresholds differ from the demo's expectations).
- Demo 5's "meta-insight" and "recommendation" are labelled illustrative.
- Full type hints, docstrings, structured logging, exit codes.

Canonical async surface (see ``INSTALLATION.md §2``)
----------------------------------------------------
- ``NodeDescriptor.select_routing_strategy`` — sync
- ``WorkloadDescriptor.select_priority``     — sync
- ``asyncio.run(...)`` is used only via the local ``_run`` helper.

Run
---
    python demo_neuro_symbolic.py                      # base demos only
    python demo_neuro_symbolic.py --enhanced           # + Demo 6
    python demo_neuro_symbolic.py --steps 3 --quiet

Exit codes
----------
    0  all requested demos succeeded
    1  one or more demos (or steps) failed but the run completed
    2  CLI / environment error (missing policy file, missing required
       dependency, bad args)
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import re
import sys
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Optional

logger = logging.getLogger("demo_neuro_symbolic")


# =============================================================================
# Constants
# =============================================================================

_DEFAULT_STEPS = 4
_DEFAULT_POLICY_FILE = "symbolic_policy.yaml"
_BACKPROPAGATE_TRANSFER_RATE = 0.3
_RULE_CATEGORY_RE = re.compile(r"^([A-Za-z][A-Za-z0-9_]*)")


# =============================================================================
# Required imports (base demos) — hard requirements
# =============================================================================
try:
    from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
    from src.dashboard.symbolic_visualizer import SymbolicVisualizer
except ImportError as _exc:  # pragma: no cover - environment error path
    print(
        f"FATAL: missing required dependency for symbolic demo: {_exc}\n"
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
    except Exception as exc:
        logger.warning(
            "Enhancement module %s raised %s during import: %s",
            name, type(exc).__name__, exc,
        )
        return None, {}, exc

    resolved: dict[str, Any] = {}
    for sym in symbols:
        value = getattr(mod, sym, None)
        if value is None:
            logger.warning(
                "Enhancement module %s imported but missing symbol %r",
                name, sym,
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
MetaCognitionLayer: Any = _meta_symbols.get("MetaCognitionLayer")
DAGCarbonLedger: Any = _ledger_symbols.get("DAGCarbonLedger")

_ENHANCEMENT_STATUS: dict[str, Optional[BaseException]] = {
    "NodeDescriptor": _node_exc,
    "WorkloadDescriptor": _wl_exc,
    "FeedbackEvent": _feedback_exc,
    "ZeroTrustArchitecture": _zt_exc,
    "GraphRegistry": _graph_exc,
    "MetaCognitionLayer": _meta_exc,
    "DAGCarbonLedger": _ledger_exc,
}


def _missing_enhancements() -> list[str]:
    """Return names of enhancement modules whose import failed."""
    return [name for name, exc in _ENHANCEMENT_STATUS.items() if exc is not None]


def _any_enhancement_available() -> bool:
    return any(
        x is not None for x in (
            NodeDescriptor, WorkloadDescriptor, FeedbackEvent,
            ZeroTrustArchitecture, GraphRegistry, MetaCognitionLayer,
            DAGCarbonLedger,
        )
    )


# =============================================================================
# Sync/async compatibility helper
# =============================================================================


async def _maybe_await(value: Any) -> Any:
    """
    Await ``value`` if it is awaitable; return it otherwise.

    Makes the demo resilient to docs/code drift where
    ``NodeDescriptor.select_routing_strategy`` might be sync (per
    ``INSTALLATION.md §2``) or async.
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
# Utilities
# =============================================================================


def _rule_category(rule_id: str) -> str:
    """
    Extract the category prefix from a rule ID.

    Rules typically look like ``ENERGY-001`` or ``ENERGY_OVER_BUDGET``.
    Returns ``"OTHER"`` for anything that does not match.
    """
    if not isinstance(rule_id, str) or not rule_id:
        return "OTHER"
    match = _RULE_CATEGORY_RE.match(rule_id)
    return match.group(1).upper() if match else "OTHER"


def _safe_get(obj: Any, *path: str, default: Any = "?") -> Any:
    """
    Safely traverse nested dict-like objects.

    Example: ``_safe_get(report, "data", "status", default="?")`` returns
    ``report["data"]["status"]`` or ``"?"`` if any intermediate is missing
    or not dict-like.
    """
    cur = obj
    for key in path:
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(key, default)
        else:
            cur = getattr(cur, key, default)
    return cur


def _read_policy_thresholds(policy_file: Path) -> Optional[dict[str, Any]]:
    """
    Best-effort: read thresholds from ``symbolic_policy.yaml``.

    Returns ``None`` if the file is not present or cannot be parsed.
    The exact schema is owned by ``SymbolicReasoningEngine``; this is a
    diagnostic helper, not a contract.
    """
    if not policy_file.is_file():
        return None
    try:
        import yaml  # type: ignore
    except ImportError:
        return None
    try:
        with policy_file.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else None
    except Exception as exc:
        logger.debug("Could not read policy file %s: %s", policy_file, exc)
        return None


# =============================================================================
# Demo 1 — Basic symbolic rule evaluation
# =============================================================================


def demo_basic_rule_evaluation(
    *,
    policy_file: str,
) -> tuple[SymbolicReasoningEngine, list[Any], int]:
    """
    Evaluate a set of metrics that are expected to violate several rules.

    Returns ``(engine, violations, status)`` where ``status`` is
    ``0`` on success, ``1`` if the demo produced 0 violations (likely a
    policy-file mismatch — surfaced as a warning).
    """
    print("=" * 60)
    print("Demo 1: Basic Symbolic Rule Evaluation")
    print("=" * 60)

    engine = SymbolicReasoningEngine(policy_file=policy_file)

    test_metrics = {
        "energy": 6.0,
        "carbon": 0.0025 * 1000,
        "latency": 150000,
        "memory": 600,
        "tool_calls": 60,
        "cpu_percent": 85,
        "cumulative": {
            "total_energy_wh": 6.0,
            "total_carbon_kg": 0.0025,
            "total_latency_ms": 150000,
            "max_memory_mb": 600,
            "total_tool_calls": 60,
            "step_count": 10,
        },
    }

    violations = engine.evaluate_rules(test_metrics, step=1)
    n_active = len(engine.get_active_rules())

    print(f"\n✓ Evaluated {n_active} active rules")
    print(f"⚠️  Found {len(violations)} violation(s)\n")

    if not violations:
        print(
            "   NOTE: 0 violations found. This usually means the policy "
            "file thresholds differ from the demo's expectations. "
            f"Check {policy_file!r}."
        )
        return engine, [], 1

    for violation in violations:
        print(f"Rule: {getattr(violation, 'rule_name', '?')} "
              f"({getattr(violation, 'rule_id', '?')})")
        print(f"Severity: {getattr(violation, 'severity', '?')}")
        print(f"Condition: {getattr(violation, 'condition', '?')}")
        print(f"Action: {getattr(violation, 'action_triggered', '?')}")
        print(f"Explanation: {getattr(violation, 'explanation', '?')}")
        print("-" * 60)

    return engine, violations, 0


# =============================================================================
# Demo 2 — Formal violation traces
# =============================================================================


def demo_violation_traces(*, policy_file: str) -> tuple[list[Any], int]:
    """Generate formal violation traces for a multi-rule scenario."""
    print("\n" + "=" * 60)
    print("Demo 2: Formal Violation Traces")
    print("=" * 60)

    engine = SymbolicReasoningEngine(policy_file=policy_file)

    metrics = {
        "energy": 4.5,
        "carbon": 65,
        "latency": 2500,
        "memory": 300,
        "tool_calls": 25,
        "cumulative": {
            "total_energy_wh": 4.5,
            "total_carbon_kg": 0.065,
            "total_latency_ms": 2500,
            "max_memory_mb": 300,
            "total_tool_calls": 25,
            "step_count": 5,
        },
    }

    violations = engine.evaluate_rules(metrics, step=5)
    print(f"\n✓ Generated {len(violations)} violation trace(s)\n")

    for violation in violations:
        print("Formal Trace:")
        print(getattr(violation, "violation_details", "<missing>"))
        print("\n" + "=" * 60 + "\n")

    return violations, 0


# =============================================================================
# Demo 3 — Category-based filtering
# =============================================================================


def demo_category_filtering(*, policy_file: str) -> tuple[Any, int]:
    """Filter violations by category across two scenarios."""
    print("=" * 60)
    print("Demo 3: Category-Based Filtering")
    print("=" * 60)

    engine = SymbolicReasoningEngine(policy_file=policy_file)

    scenarios = [
        {
            "name": "High Energy",
            "metrics": {
                "energy": 6.0, "carbon": 50, "latency": 1000,
                "memory": 200, "tool_calls": 10,
                "cumulative": {
                    "total_energy_wh": 6.0,
                    "total_carbon_kg": 0.05,
                    "total_latency_ms": 1000,
                    "max_memory_mb": 200,
                    "total_tool_calls": 10,
                    "step_count": 3,
                },
            },
        },
        {
            "name": "Memory Overflow",
            "metrics": {
                "energy": 2.0, "carbon": 20, "latency": 1000,
                "memory": 550, "tool_calls": 10,
                "cumulative": {
                    "total_energy_wh": 2.0,
                    "total_carbon_kg": 0.02,
                    "total_latency_ms": 1000,
                    "max_memory_mb": 550,
                    "total_tool_calls": 10,
                    "step_count": 3,
                },
            },
        },
    ]

    for scenario in scenarios:
        print(f"\nScenario: {scenario['name']}")
        violations = engine.evaluate_rules(scenario["metrics"], step=1)
        print(f"  Violations: {len(violations)}")
        for v in violations:
            category = _rule_category(getattr(v, "rule_id", ""))
            print(f"    - [{category}] {getattr(v, 'rule_name', '?')}")

    print("\n" + "-" * 60)
    print("Cumulative Summary by Category (across Demo 3 scenarios):")
    try:
        summary = engine.get_violation_summary() or {}
    except Exception:
        logger.exception("engine.get_violation_summary() failed")
        summary = {}
    by_cat = summary.get("by_category", {}) if isinstance(summary, dict) else {}
    if not by_cat:
        print("  (no violations recorded)")
    else:
        for category, count in by_cat.items():
            print(f"  {str(category).upper()}: {count} violation(s)")

    return engine, 0


# =============================================================================
# Demo 4 — Dashboard visualization
# =============================================================================


def demo_dashboard_visualization(
    *,
    policy_file: str,
    output_dir: Path,
) -> tuple[Any, int]:
    """Generate violation views and write an HTML section to output_dir."""
    print("\n" + "=" * 60)
    print("Demo 4: Dashboard Visualization")
    print("=" * 60)

    engine = SymbolicReasoningEngine(policy_file=policy_file)
    visualizer = SymbolicVisualizer()

    metrics = {
        "energy": 5.5, "carbon": 70, "latency": 130000,
        "memory": 520, "tool_calls": 55,
        "cumulative": {
            "total_energy_wh": 5.5,
            "total_carbon_kg": 0.07,
            "total_latency_ms": 130000,
            "max_memory_mb": 520,
            "total_tool_calls": 55,
            "step_count": 8,
        },
    }

    violations = engine.evaluate_rules(metrics, step=8)
    visualizer.add_violations([v.to_dict() for v in violations])

    timeline = visualizer.generate_violation_timeline() or []
    category_view = visualizer.generate_category_view() or {}
    severity_summary = visualizer.generate_severity_summary() or {}

    print("\n✓ Generated visualization data")
    print(f"  Timeline entries: {len(timeline)}")
    print(f"  Categories: {len(category_view)}")
    print(f"  Severity breakdown: {severity_summary.get('counts', {})}")

    html_section = visualizer.generate_dashboard_section() or ""
    out_file = output_dir / "demo_symbolic_dashboard.html"
    output_dir.mkdir(parents=True, exist_ok=True)
    if html_section:
        # Escaping user-controlled content here (defense in depth) — the
        # visualizer is trusted, but the HTML wrapper is not.
        try:
            from html import escape as _esc
            safe_section = _esc(html_section, quote=False)
        except Exception:
            safe_section = html_section
        out_file.write_text(
            f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Symbolic Oversight Demo</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .violation-card {{ margin: 15px 0; }}
    </style>
</head>
<body>
    <h1>Green Agent — Symbolic Oversight Demo</h1>
    {safe_section}
</body>
</html>
""",
            encoding="utf-8",
        )
        print(f"✓ Dashboard HTML saved to: {out_file}")
    else:
        print("⚠️  Visualizer returned an empty section — HTML not written.")

    return visualizer, 0


# =============================================================================
# Demo 5 — Sustained reflection
# =============================================================================


def demo_sustained_reflection(
    *,
    policy_file: str,
    steps: int = _DEFAULT_STEPS,
) -> tuple[Any, int]:
    """Track violations across `steps` simulated execution steps."""
    print("\n" + "=" * 60)
    print(f"Demo 5: Sustained Reflection ({steps} step(s))")
    print("=" * 60)

    engine = SymbolicReasoningEngine(policy_file=policy_file)

    # Escalating trajectory — modest at start, breaches thresholds by the
    # final steps. If you change `steps`, the trajectory is truncated or
    # extended with the last known values.
    base_trajectory = [
        {"energy": 3.0, "carbon": 40, "latency": 50000, "memory": 200},
        {"energy": 4.0, "carbon": 50, "latency": 80000, "memory": 300},
        {"energy": 5.5, "carbon": 65, "latency": 120000, "memory": 450},
        {"energy": 6.0, "carbon": 75, "latency": 140000, "memory": 550},
    ]
    if steps <= len(base_trajectory):
        trajectory = base_trajectory[:steps]
    else:
        trajectory = (
            base_trajectory
            + [base_trajectory[-1]] * (steps - len(base_trajectory))
        )

    print("\nTracking violations across execution steps:\n")
    total_violations = 0
    for step_idx, step_data in enumerate(trajectory, start=1):
        metrics = {
            **step_data,
            "tool_calls": step_idx * 10,
            "cumulative": {
                "total_energy_wh": step_data["energy"],
                "total_carbon_kg": step_data["carbon"] / 1000,
                "total_latency_ms": step_data["latency"],
                "max_memory_mb": step_data["memory"],
                "total_tool_calls": step_idx * 10,
                "step_count": step_idx,
            },
        }
        violations = engine.evaluate_rules(metrics, step=step_idx)
        total_violations += len(violations)

        print(f"Step {step_idx}:")
        print(
            f"  Energy: {step_data['energy']:.1f} Wh | "
            f"Carbon: {step_data['carbon']} g"
        )
        print(f"  Violations: {len(violations)}")
        if violations:
            for v in violations:
                print(
                    f"    ⚠️  {getattr(v, 'rule_name', '?')} "
                    f"[{getattr(v, 'severity', '?')}]"
                )
        else:
            print("    ✅ No violations")
        print()

    print("-" * 60)
    print("Trajectory Summary:")
    try:
        summary = engine.get_violation_summary() or {}
    except Exception:
        logger.exception("engine.get_violation_summary() failed")
        summary = {}
    print(f"  Total evaluations: {summary.get('evaluations', '?')}")
    print(f"  Total violations: {summary.get('total_violations', '?')}")
    rate = summary.get("violation_rate")
    if isinstance(rate, (int, float)):
        print(f"  Violation rate: {rate:.2%}")
    else:
        print("  Violation rate: n/a")

    if total_violations > 0:
        print(
            "\n[Illustrative conclusion — not computed] "
            "Agent shows degrading compliance over time. "
            "Consider adaptive throttling once violations begin."
        )
    else:
        print(
            "\n[No violations detected — policy thresholds may not match "
            "the demo trajectory. Check the policy file.]"
        )

    return engine, 0


# =============================================================================
# Demo 6 — Advanced enhancements (opt-in)
# =============================================================================


def demo_advanced_enhancements(*, output_dir: Path) -> int:
    """
    Run the advanced enhancements demo.

    Each of the nine sections is isolated — one failing section logs and
    continues, so the demo produces maximum diagnostic value in one run.

    Returns
    -------
    int
        0 if all sections succeeded; the number of failed sections
        otherwise.
    """
    print("\n" + "=" * 60)
    print("Demo 6: Advanced Enhancements (LIMIT Graph, MODP, RLHF, "
          "Distillation, MoE, Evolutionary, FlexGen)")
    print("=" * 60)

    if not _any_enhancement_available():
        print("⚠️  No advanced enhancement modules installed. Skipping.")
        return 0

    missing = _missing_enhancements()
    if missing:
        print(
            f"\n⚠️  {len(missing)} enhancement module(s) unavailable: "
            f"{', '.join(missing)}"
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
                id="demo_node",
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
                task_id="demo_task",
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

    # 5. FeedbackEvent (schema-compliant: energy_kwh, documented keys only)
    event = _section(
        "5. Creating FeedbackEvent (enhanced fields, schema-compliant)...",
        lambda: (
            FeedbackEvent(
                source="demo_symbolic",
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
        print(f"     - Diagnosis status: {_safe_get(report, 'status')}")
        print(
            f"     - Recommended action: "
            f"{_safe_get(report, 'recommended_action')}"
        )

    _section("7. LIMIT Graph + Meta-Cognition...", _graph_and_meta)

    # 8. DAG Carbon Ledger (tempdir for cross-platform)
    def _ledger_run() -> None:
        if not DAGCarbonLedger:
            _raise_missing("DAGCarbonLedger")
        with tempfile.TemporaryDirectory(prefix="demo_ledger_") as tmp:
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

    # 9. FlexGen config (illustrative)
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
            f"⚠️  Advanced Enhancements finished with {failures} "
            f"failure(s)"
        )
    print("=" * 60 + "\n")
    return failures


# =============================================================================
# CLI
# =============================================================================


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="demo_neuro_symbolic",
        description=(
            "Neuro-Symbolic Oversight demo for Green Agent. Runs the base "
            "demos (1–5); add --enhanced to also run Demo 6."
        ),
    )
    p.add_argument(
        "--steps", type=int, default=_DEFAULT_STEPS,
        help=(
            f"Steps for Demo 5 (sustained reflection). "
            f"Default: {_DEFAULT_STEPS}"
        ),
    )
    p.add_argument(
        "--enhanced", dest="enhanced", action="store_true", default=None,
        help="Force-enable Demo 6 (overrides auto-detect).",
    )
    p.add_argument(
        "--no-enhanced", dest="enhanced", action="store_false",
        help="Skip Demo 6 even if enhancement modules are installed.",
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
        help=(
            f"Path to symbolic_policy.yaml "
            f"(default: {_DEFAULT_POLICY_FILE})"
        ),
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
    (None) = auto = run if any enhancement module was importable.
    """
    if flag is not None:
        return flag
    return _any_enhancement_available()


# =============================================================================
# Main
# =============================================================================


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    _configure_logging(args)

    try:
        output_dir = _resolve_output_dir(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"FATAL: could not resolve output dir: {exc}", file=sys.stderr)
        return 2

    policy_path = Path(args.policy_file)
    if not policy_path.is_file():
        print(
            f"FATAL: policy file {args.policy_file!r} not found. "
            f"Pass --policy-file PATH or run from the repo root.",
            file=sys.stderr,
        )
        return 2

    if args.steps < 1:
        print(f"FATAL: --steps must be >= 1 (got {args.steps})",
              file=sys.stderr)
        return 2

    # Optional policy preflight — helpful diagnostics only.
    thresholds = _read_policy_thresholds(policy_path)
    if thresholds is not None:
        logger.info(
            "Policy preflight: %d top-level key(s) in %s",
            len(thresholds), policy_path,
        )
    else:
        logger.debug(
            "Could not read policy file %s (missing or unparsable); "
            "proceeding anyway",
            policy_path,
        )

    print("\n🌱 Green Agent — Neuro-Symbolic Oversight Demo")
    print("Inspired by FormalJudge paradigm\n")

    failures = 0

    def _run_demo(label: str, fn: Callable[[], Any]) -> Any:
        nonlocal failures
        try:
            result = fn()
            # Demos return (value, status) tuples; honor status.
            if isinstance(result, tuple) and len(result) == 2:
                value, status = result
                if status != 0:
                    failures += status
                return value
            return result
        except Exception:
            failures += 1
            logger.exception("%s failed", label)
            print(f"\n[FAIL] {label} raised; continuing.")
            return None

    _run_demo(
        "Demo 1 (basic rule evaluation)",
        lambda: demo_basic_rule_evaluation(policy_file=args.policy_file),
    )
    _run_demo(
        "Demo 2 (violation traces)",
        lambda: demo_violation_traces(policy_file=args.policy_file),
    )
    _run_demo(
        "Demo 3 (category filtering)",
        lambda: demo_category_filtering(policy_file=args.policy_file),
    )
    _run_demo(
        "Demo 4 (dashboard)",
        lambda: demo_dashboard_visualization(
            policy_file=args.policy_file, output_dir=output_dir,
        ),
    )
    _run_demo(
        "Demo 5 (sustained reflection)",
        lambda: demo_sustained_reflection(
            policy_file=args.policy_file, steps=args.steps,
        ),
    )

    # Demo 6 — opt-in
    run_enhancements = _resolve_run_enhancements(args.enhanced)
    if run_enhancements:
        try:
            failures += demo_advanced_enhancements(output_dir=output_dir)
        except Exception:
            failures += 1
            logger.exception("Demo 6 (enhancements) failed")
    else:
        print("\n[i] Demo 6 (enhancements) skipped. "
              "Use --enhanced to include it.")

    # --- Final summary ------------------------------------------------
    print("\n" + "=" * 60)
    if failures == 0:
        print("✅ Demo Complete")
    else:
        print(f"⚠️  Demo finished with {failures} failure(s)")
    print("=" * 60)
    print("\nKey features demonstrated:")
    print("  ✓ Symbolic rule evaluation")
    print("  ✓ Formal violation traces")
    print("  ✓ Category-based filtering")
    print("  ✓ Dashboard visualization")
    print("  ✓ Sustained reflection patterns")
    if run_enhancements:
        print(
            "  ✓ Advanced enhancements "
            "(LIMIT Graph, MODP, RLHF, distillation, MoE, "
            "evolutionary, FlexGen)"
        )
    print("\nNext steps:")
    print(f"  1. Review {args.policy_file!r} to customize rules")
    print("  2. Run: python run_agent.py --config example_config.json")
    print(f"  3. Check {output_dir.resolve() / 'demo_symbolic_dashboard.html'}")
    print("  4. Review symbolic_violations.json for detailed traces")

    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
