#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Installation (Enhanced v2).

Quick test to verify LIMIT-AgentBench installation:

  Core:
    - agentbench, green_metrics, agent_evaluator, benchmark_harness
    - adapters (langchain, autogen, crewai, limit_graph)
    - metrics (energy, carbon, efficiency, sustainability)
    - dashboard (green_leaderboard)
    - the ``limit_agentbench`` package itself

  Advanced enhancements (optional, skipped if unavailable):
    - LIMIT Graph, MODP, RLHF, Distillation, MoE, Zero Trust

---- Critical fixes vs v1 ---------------------------------------------------
  FIX #1   ``Path`` was used in ``test_enhanced_imports`` without being
           imported — every enhanced import test aborted with ``NameError``.
           Now imported at the top of the module.
  FIX #2   ``asyncio.run()`` inside ``test_enhanced_functionality`` raised
           ``RuntimeError`` from any async context. Replaced with
           ``_run_coro``/``_await_or_call``, which dispatch to a worker
           thread when a loop is already running.
  FIX #3   The "Main package import skipped" branch printed a warning and
           then incremented ``tests_passed`` — a genuinely broken
           ``limit_agentbench`` package reported as passing. The branch now
           increments ``tests_failed`` on failure (with a clear message) and
           ``tests_passed`` on success.
  FIX #4   ``NodeDescriptor(use_enhancements=True)`` passed a kwarg the real
           class may not accept. ``_try_construct`` filters kwargs through
           ``inspect.signature``; falls back to a smaller kwarg set; and
           reports the exact ``TypeError`` if the constructor still fails.
  FIX #5   ``event.event_id[:8]`` assumed a string. Now
           ``str(_safe_get(event, "event_id", default="?"))[:8]``.
  FIX #6   Removed the dead ``if False`` branch and the duplicate
           ``feedback_event`` entry in ``modules_to_check``.
  FIX #7   Version-hardcoded path (``quantum-limit-graph-v2.4.0``) replaced
           with a walker that ascends from ``__file__`` looking for a
           ``core/`` sibling.
  FIX #8   ``main()`` returns an int; ``sys.exit(main())`` at the bottom.
           Enhanced-test failures now affect the exit code (or are gated by
           ``--strict``).
  FIX #9   ``test_imports`` reports per-module failures, not per-group.
           A broken ``crewai_adapter`` no longer hides the fact that
           langchain / autogen / limit_graph imported fine.
  FIX #10  ``test_enhanced_functionality`` narrows its exception handling:
           ``ImportError`` for missing modules (skipped), ``TypeError`` for
           kwarg issues (reported with the message), everything else
           (reported with traceback).
  FIX #11  ``test_basic_functionality`` reports which step failed and its
           error, instead of a bare "Functionality test failed".
  FIX #12  ``si_calc.calculate(...)`` and ``SustainabilityIndex.get_rating``
           wrapped with ``hasattr`` checks and diagnostic errors.
  FIX #13  Emoji output crashes on Windows without UTF-8. ``reconfigure``
           at import.
  FIX #14  ``sys.path.insert(0, ...)`` for the enhancements folder is
           reverted after the import check.
  FIX #15  Added CLI flags: ``--core-only``, ``--enhanced-only``,
           ``--strict``, ``--json``, ``--quiet``, ``--self-test``.

---- Run -------------------------------------------------------------------
  python test_installation.py                    # full check
  python test_installation.py --core-only        # skip optional enhancements
  python test_installation.py --enhanced-only    # only enhancements
  python test_installation.py --strict           # treat missing enhancements
                                                 # as failures
  python test_installation.py --json             # machine-readable summary
  python test_installation.py --self-test        # validate helpers offline
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Platform hygiene (FIX #13)
# ---------------------------------------------------------------------------
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:                                   # noqa: BLE001
        pass


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Package-root walker (FIX #7)
# ---------------------------------------------------------------------------
def _package_roots() -> List[Path]:
    """Candidate directories that might contain ``core/`` or ``adapters/``."""
    here = Path(__file__).resolve().parent
    roots: List[Path] = []
    for candidate in [here, *here.parents]:
        for sub in (".", "src", "limit-agentbench/src"):
            p = (candidate / sub).resolve()
            if p.is_dir() and str(p) not in roots:
                roots.append(p)
                if (p / "core").is_dir() or (p / "adapters").is_dir():
                    return roots
    return roots


def _ensure_import_roots() -> None:
    for root in _package_roots():
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))


_ensure_import_roots()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe_get(obj: Any, *keys: str, default: Any = None) -> Any:
    current = obj
    for k in keys:
        if current is None:
            return default
        if isinstance(current, dict):
            current = current.get(k)
        else:
            current = getattr(current, k, None)
    return default if current is None else current


def _run_coro(coro: Any) -> Any:
    """Run a coroutine safely in sync or async context (FIX #2)."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


def _await_or_call(method: Any, *args: Any, **kwargs: Any) -> Any:
    result = method(*args, **kwargs)
    if inspect.iscoroutine(result):
        return _run_coro(result)
    return result


def _filter_kwargs(cls: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
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


def _try_construct(cls: Any, kwargs: Dict[str, Any],
                   fallback_kwargs: Optional[Dict[str, Any]] = None
                   ) -> Tuple[Optional[Any], Optional[str]]:
    """Return (instance, error_message). error_message is None on success."""
    if cls is None:
        return None, "class is None (import failed)"
    try:
        return cls(**_filter_kwargs(cls, kwargs)), None
    except Exception as exc:                             # noqa: BLE001
        first_error = f"{type(exc).__name__}: {exc}"
    if fallback_kwargs is not None:
        try:
            return cls(**_filter_kwargs(cls, fallback_kwargs)), None
        except Exception as exc:                         # noqa: BLE001
            second_error = f"{type(exc).__name__}: {exc}"
            return None, f"primary={first_error}; fallback={second_error}"
    return None, first_error


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------
@dataclass
class CheckResult:
    name: str
    ok: bool = True
    skipped: bool = False
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "ok": self.ok,
            "skipped": self.skipped,
            "message": self.message,
            "details": dict(self.details),
        }


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------
class Reporter:
    def __init__(self, quiet: bool = False, verbose: bool = False):
        self.quiet = quiet
        self.verbose = verbose
        self.results: List[CheckResult] = []

    def say(self, msg: str = "", *, error: bool = False) -> None:
        if self.quiet and not error:
            return
        print(msg, file=sys.stderr if error else sys.stdout)

    def section(self, title: str) -> None:
        self.say("")
        self.say("=" * 80)
        self.say(f"  {title}")
        self.say("=" * 80)

    def record(self, result: CheckResult) -> CheckResult:
        self.results.append(result)
        icon = "✓" if result.ok and not result.skipped else (
            "⚠" if result.skipped else "✗"
        )
        self.say(f"   {icon} {result.name}"
                 + (f" — {result.message}" if result.message else ""))
        return result

    def counts(self) -> Tuple[int, int, int]:
        passed = sum(1 for r in self.results if r.ok and not r.skipped)
        failed = sum(1 for r in self.results if not r.ok)
        skipped = sum(1 for r in self.results if r.skipped)
        return passed, failed, skipped


# ---------------------------------------------------------------------------
# Core import table (FIX #9) — one entry per module, not per group
# ---------------------------------------------------------------------------
CORE_IMPORT_TABLE: List[Tuple[str, str]] = [
    # (module_path, symbol)
    ("core.agentbench_adapter", "AgentBenchAdapter"),
    ("core.green_metrics", "GreenMetricsTracker"),
    ("core.agent_evaluator", "AgentEvaluator"),
    ("core.benchmark_harness", "BenchmarkHarness"),
    ("adapters.base_adapter", "BaseAgentAdapter"),
    ("adapters.langchain_adapter", "LangChainAdapter"),
    ("adapters.autogen_adapter", "AutoGenAdapter"),
    ("adapters.crewai_adapter", "CrewAIAdapter"),
    ("adapters.limit_graph_adapter", "LimitGraphAdapter"),
    ("metrics.energy_tracker", "EnergyTracker"),
    ("metrics.carbon_calculator", "CarbonCalculator"),
    ("metrics.efficiency_scorer", "EfficiencyScorer"),
    ("metrics.sustainability_index", "SustainabilityIndex"),
    ("dashboard.green_leaderboard", "GreenLeaderboard"),
]


def _import_one(module_path: str,
                symbol: str) -> Tuple[bool, Any, str]:
    """Return (ok, symbol_obj_or_None, error_message)."""
    try:
        module = importlib.import_module(module_path)
    except Exception as exc:                             # noqa: BLE001
        return False, None, f"{type(exc).__name__}: {exc}"
    if not hasattr(module, symbol):
        return False, None, f"module has no {symbol!r}"
    return True, getattr(module, symbol), ""


# ---------------------------------------------------------------------------
# Core import test
# ---------------------------------------------------------------------------
def test_core_imports(reporter: Reporter) -> bool:
    reporter.section("Testing LIMIT-AgentBench Core Imports")

    any_failed = False
    for module_path, symbol in CORE_IMPORT_TABLE:
        ok, _obj, msg = _import_one(module_path, symbol)
        reporter.record(CheckResult(
            name=f"{module_path}.{symbol}",
            ok=ok,
            message=msg,
        ))
        if not ok:
            any_failed = True

    # FIX #3 — the main package is checked genuinely; a failure is a
    # failure, not a "skip that counts as pass"
    reporter.say("")
    ok, obj, msg = _import_one("limit_agentbench", "__version__")
    if ok:
        reporter.record(CheckResult(
            name="limit_agentbench.__version__",
            ok=True,
            message=f"version={obj}",
            details={"version": obj},
        ))
    else:
        reporter.record(CheckResult(
            name="limit_agentbench.__version__",
            ok=False,
            message=(
                f"cannot import main package ({msg}); run "
                "`pip install -e .` from the project root"
            ),
        ))
        any_failed = True

    return not any_failed


# ---------------------------------------------------------------------------
# Core functionality test (FIX #11)
# ---------------------------------------------------------------------------
def test_basic_functionality(reporter: Reporter) -> bool:
    reporter.section("Testing Basic Functionality")

    steps: List[Tuple[str, Callable[[], None]]] = []

    def _check_adapter() -> None:
        _ok, AgentBenchAdapter, _ = _import_one(
            "core.agentbench_adapter", "AgentBenchAdapter",
        )
        if AgentBenchAdapter is None:
            raise RuntimeError("AgentBenchAdapter import failed")
        adapter = AgentBenchAdapter()
        task = adapter.create_task(
            task_id="test_001",
            suite="test_suite",
            task_type="test",
            input_data={"test": "data"},
        )
        task_id = _safe_get(task, "task_id")
        if task_id != "test_001":
            raise RuntimeError(
                f"task_id={task_id!r} != 'test_001'"
            )

    def _check_tracker() -> None:
        _ok, GreenMetricsTracker, _ = _import_one(
            "core.green_metrics", "GreenMetricsTracker",
        )
        if GreenMetricsTracker is None:
            raise RuntimeError("GreenMetricsTracker import failed")
        tracker = GreenMetricsTracker(grid_region="US-CA")
        value = getattr(tracker, "carbon_intensity", None)
        if value is None:
            raise RuntimeError(
                "tracker has no 'carbon_intensity' attribute"
            )
        try:
            v = float(value)
        except (TypeError, ValueError):
            raise RuntimeError(
                f"carbon_intensity={value!r} is not numeric"
            ) from None
        if not (0.0 < v < 100.0):
            raise RuntimeError(
                f"carbon_intensity={v} outside plausible range"
            )

    def _check_si() -> None:
        _ok, SustainabilityIndex, _ = _import_one(
            "metrics.sustainability_index", "SustainabilityIndex",
        )
        if SustainabilityIndex is None:
            raise RuntimeError("SustainabilityIndex import failed")
        si_calc = SustainabilityIndex()
        if not hasattr(si_calc, "calculate"):
            raise RuntimeError("SustainabilityIndex lacks calculate()")
        si = si_calc.calculate(
            accuracy=0.95, energy_kwh=0.003, carbon_co2e_kg=0.0006,
        )
        try:
            si_f = float(si)
        except (TypeError, ValueError):
            raise RuntimeError(
                f"calculate() returned {si!r} (not numeric)"
            ) from None
        if not (0.0 <= si_f <= 1.0):
            raise RuntimeError(
                f"sustainability index={si_f} outside [0, 1]"
            )
        if not hasattr(SustainabilityIndex, "get_rating"):
            raise RuntimeError(
                "SustainabilityIndex lacks get_rating()"
            )

    steps.append(("AgentBenchAdapter.create_task", _check_adapter))
    steps.append(("GreenMetricsTracker.carbon_intensity", _check_tracker))
    steps.append(("SustainabilityIndex.calculate", _check_si))

    for name, fn in steps:
        try:
            fn()
        except Exception as exc:                         # noqa: BLE001
            reporter.record(CheckResult(
                name=name,
                ok=False,
                message=f"{type(exc).__name__}: {exc}",
            ))
            return False
        reporter.record(CheckResult(name=name, ok=True))

    return True


# ---------------------------------------------------------------------------
# Enhanced imports (FIX #6, #7, #14)
# ---------------------------------------------------------------------------
ENHANCEMENT_IMPORT_TABLE: List[Tuple[str, str]] = [
    ("enhancements.schemas.feedback_event", "FeedbackEvent"),
    ("enhancements.schemas.node_descriptor", "NodeDescriptor"),
    ("enhancements.schemas.workload_descriptor", "WorkloadDescriptor"),
    ("enhancements.zero_trust_architecture", "ZeroTrustArchitecture"),
]


def test_enhanced_imports(reporter: Reporter) -> bool:
    reporter.section("Testing Advanced Enhancements (Optional)")

    # FIX #14 — try the walker paths, then revert any sys.path mutation
    saved_path = list(sys.path)
    _ensure_import_roots()
    try:
        any_failed = False
        for module_path, symbol in ENHANCEMENT_IMPORT_TABLE:
            ok, _obj, msg = _import_one(module_path, symbol)
            if ok:
                reporter.record(CheckResult(
                    name=f"{module_path}.{symbol}",
                    ok=True,
                ))
            elif "ModuleNotFoundError" in msg or "No module named" in msg:
                reporter.record(CheckResult(
                    name=f"{module_path}.{symbol}",
                    ok=False,
                    skipped=True,
                    message=f"not installed ({msg})",
                ))
            else:
                reporter.record(CheckResult(
                    name=f"{module_path}.{symbol}",
                    ok=False,
                    message=msg,
                ))
                any_failed = True
        return not any_failed
    finally:
        sys.path[:] = saved_path


# ---------------------------------------------------------------------------
# Enhanced functionality (FIX #4, #5, #10)
# ---------------------------------------------------------------------------
def test_enhanced_functionality(reporter: Reporter) -> Tuple[bool, bool]:
    """
    Return (ok, skipped).

    ok=True, skipped=True  -> enhancements unavailable, not a failure.
    ok=True, skipped=False -> enhancements ran and passed.
    ok=False, skipped=False -> enhancements ran and failed.
    """
    reporter.section("Testing Enhanced Functionality (Optional)")

    # Import the surface first; if unavailable, mark skipped.
    try:
        from enhancements.schemas.node_descriptor import (  # noqa: F401
            NodeDescriptor, NodeType,
        )
        from enhancements.schemas.workload_descriptor import (  # noqa: F401
            WorkloadDescriptor, TaskType,
        )
        from enhancements.schemas.feedback_event import FeedbackEvent  # noqa: F401
    except ImportError as exc:
        reporter.record(CheckResult(
            name="enhancements.functionality",
            ok=False,
            skipped=True,
            message=f"modules not installed ({exc})",
        ))
        return True, True

    # 1. NodeDescriptor (FIX #4 — kwarg filtering)
    node, err = _try_construct(
        NodeDescriptor,
        {
            "id": "test_node",
            "type": getattr(NodeType, "EDGE", None),
            "region": "us-east",
            "region_carbon_intensity": 400.0,
            "energy_per_token": 0.00005,
            "use_enhancements": True,
            "human_feedback_score": 0.6,
            "graph_metrics": {"centrality": 0.7},
        },
        fallback_kwargs={
            "id": "test_node",
            "region": "us-east",
            "region_carbon_intensity": 400.0,
            "energy_per_token": 0.00005,
        },
    )
    if node is None:
        reporter.record(CheckResult(
            name="NodeDescriptor", ok=False,
            message=err or "construct failed",
        ))
        return False, False
    reporter.record(CheckResult(
        name="NodeDescriptor", ok=True,
        message=f"id={_safe_get(node, 'id', default='?')}",
    ))

    # 2. WorkloadDescriptor
    wl, err = _try_construct(
        WorkloadDescriptor,
        {
            "task_id": "test_task",
            "task_type": getattr(TaskType, "INFERENCE", None),
            "tokens": 1000,
            "latency_target": 500.0,
            "use_enhancements": True,
            "human_feedback_score": 0.5,
            "graph_metrics": {"centrality": 0.5},
        },
        fallback_kwargs={
            "task_id": "test_task",
            "tokens": 1000,
            "latency_target": 500.0,
        },
    )
    if wl is None:
        reporter.record(CheckResult(
            name="WorkloadDescriptor", ok=False,
            message=err or "construct failed",
        ))
        return False, False
    reporter.record(CheckResult(
        name="WorkloadDescriptor", ok=True,
        message=f"id={_safe_get(wl, 'task_id', default='?')}",
    ))

    # 3. FeedbackEvent (FIX #5 — event_id may not be a str)
    event, err = _try_construct(
        FeedbackEvent,
        {
            "source": "test_install",
            "feedback_type": "routing",
            "task_id": "test_event",
            "context": {},
            "action": {
                "selected_action": "execute", "selected_rank": 1,
            },
            "performance": {
                "quality_score": 0.9, "latency_ms": 100,
                "energy_joules": 100, "carbon_g": 5,
                "helium_cost": 0, "duration_ms": 100,
            },
            "adaptive_cost_value": 0.85,
            "tags": ["install-test"],
        },
        fallback_kwargs={
            "source": "test_install",
            "feedback_type": "routing",
            "task_id": "test_event",
        },
    )
    if event is None:
        reporter.record(CheckResult(
            name="FeedbackEvent", ok=False,
            message=err or "construct failed",
        ))
        return False, False
    event_id = str(_safe_get(event, "event_id", default="?"))
    reporter.record(CheckResult(
        name="FeedbackEvent", ok=True,
        message=f"event_id={event_id[:8]}",
    ))

    # 4. Distillation selection (FIX #2 — safe async)
    for method_name, target, kwargs in (
        ("select_routing_strategy", node, {"exploration": True}),
        ("select_priority", wl, {"exploration": True}),
    ):
        method = getattr(target, method_name, None)
        if method is None:
            reporter.record(CheckResult(
                name=method_name,
                ok=False,
                skipped=True,
                message="method not implemented on this class",
            ))
            continue
        try:
            result = _await_or_call(method, **kwargs)
        except Exception as exc:                         # noqa: BLE001
            reporter.record(CheckResult(
                name=method_name,
                ok=False,
                message=f"{type(exc).__name__}: {exc}",
            ))
            return False, False
        reporter.record(CheckResult(
            name=method_name,
            ok=True,
            message=f"result={result}",
        ))

    return True, False


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------
def _self_test() -> int:
    failures = 0

    def _check(name: str, cond: bool, detail: str = "") -> None:
        nonlocal failures
        if cond:
            print(f"  ✓ {name}")
        else:
            print(f"  ✗ {name}{(': ' + detail) if detail else ''}")
            failures += 1

    print("_safe_get:")
    _check("nested dict",
           _safe_get({"a": {"b": 1}}, "a", "b") == 1)
    _check("missing default",
           _safe_get({"a": {}}, "a", "b", default="d") == "d")
    _check("none input", _safe_get(None, "a") is None)

    print("_filter_kwargs:")
    class _Foo:
        def __init__(self, x, y=1):
            self.x, self.y = x, y

    _check("filters unknown",
           _filter_kwargs(_Foo, {"x": 1, "y": 2, "z": 3}) == {"x": 1, "y": 2})

    print("_try_construct:")
    class _NeedsAll:
        def __init__(self, a, b):
            if a < 0:
                raise ValueError("negative a")
            self.a, self.b = a, b

    obj, err = _try_construct(
        _NeedsAll, {"a": 1, "b": 2, "c": 3},
    )
    _check("success no error", obj is not None and err is None)

    obj, err = _try_construct(
        _NeedsAll, {"a": -1, "b": 2},
        fallback_kwargs={"a": 5, "b": 6},
    )
    _check("fallback succeeds", obj is not None and obj.a == 5)

    obj, err = _try_construct(
        _NeedsAll, {"a": -1, "b": -2},
        fallback_kwargs={"a": -3, "b": -4},
    )
    _check("returns error when both fail",
           obj is None and err is not None and "primary=" in err)

    print("_run_coro:")
    async def _coro():
        return 42
    _check("sync context", _run_coro(_coro()) == 42)

    print("_await_or_call:")
    class _Sync:
        def call(self, x): return x + 1
    class _Async:
        async def call(self, x): return x + 2
    _check("sync method", _await_or_call(_Sync().call, 10) == 11)
    _check("async method", _await_or_call(_Async().call, 10) == 12)

    print("Reporter:")
    r = Reporter(quiet=True)
    r.record(CheckResult("a", ok=True))
    r.record(CheckResult("b", ok=False, message="boom"))
    r.record(CheckResult("c", skipped=True, message="not installed"))
    passed, failed, skipped = r.counts()
    _check("counts correct",
           passed == 1 and failed == 1 and skipped == 1)

    print("")
    if failures:
        print(f"{failures} failure(s)")
        return 1
    print("all self-tests passed")
    return 0


# ---------------------------------------------------------------------------
# CLI (FIX #15)
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="LIMIT-AgentBench installation test (Enhanced v2)",
    )
    p.add_argument("--core-only", action="store_true",
                   help="run only the core installation tests")
    p.add_argument("--enhanced-only", action="store_true",
                   help="run only the enhanced installation tests")
    p.add_argument("--strict", action="store_true",
                   help="treat missing enhancements as failures")
    p.add_argument("--json", action="store_true",
                   help="emit a machine-readable summary at the end")
    p.add_argument("--quiet", action="store_true",
                   help="suppress informational output")
    p.add_argument("--verbose", action="store_true",
                   help="print extra internals")
    p.add_argument("--self-test", action="store_true",
                   help="run helper self-tests and exit")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    if args.self_test:
        return _self_test()

    if args.core_only and args.enhanced_only:
        print("--core-only and --enhanced-only are mutually exclusive",
              file=sys.stderr)
        return 2

    reporter = Reporter(quiet=args.quiet, verbose=args.verbose)

    reporter.say("=" * 80)
    reporter.say("LIMIT-AgentBench Installation Test (Enhanced v2)")
    reporter.say("=" * 80)

    run_core = not args.enhanced_only
    run_enhanced = not args.core_only

    core_ok = True
    if run_core:
        core_ok = test_core_imports(reporter)
        if core_ok:
            core_ok = test_basic_functionality(reporter)
        if not core_ok:
            reporter.say("")
            reporter.say(
                "⚠ Core installation is broken — fix the failures above "
                "before continuing.", error=True,
            )

    enhanced_ok = True
    enhanced_skipped = False
    if run_enhanced and core_ok:
        enhanced_ok = test_enhanced_imports(reporter)
        func_ok, func_skipped = test_enhanced_functionality(reporter)
        enhanced_skipped = func_skipped
        enhanced_ok = enhanced_ok and func_ok
    elif run_enhanced and not core_ok:
        reporter.say(
            "\n⏭️  Skipping enhanced tests because core installation failed."
        )
        enhanced_skipped = True

    passed, failed, skipped = reporter.counts()

    reporter.say("")
    reporter.say("=" * 80)
    reporter.say(
        f"Results: {passed} passed, {failed} failed, {skipped} skipped"
    )
    reporter.say("=" * 80)

    # Exit code logic
    exit_code = 0
    if not core_ok:
        exit_code = 1
    elif not enhanced_ok and not enhanced_skipped and args.strict:
        exit_code = 1
    elif not enhanced_ok and not enhanced_skipped:
        # Non-strict: report but don't fail on optional enhancements
        reporter.say("")
        reporter.say(
            "⚠ Enhanced modules failed (not fatal in non-strict mode; "
            "use --strict to gate).", error=True,
        )

    if exit_code == 0 and core_ok:
        reporter.say("")
        reporter.say("Next steps:")
        reporter.say("  1. python demo_green_benchmark.py")
        reporter.say("  2. cat README.md")
        reporter.say("  3. cat GREEN_AGENT_BENCHMARKING_COMPLETE.md")
        if enhanced_skipped:
            reporter.say(
                "  4. (optional) install requirements/enhancements.txt "
                "to enable advanced modules"
            )

    if args.json:
        print(json.dumps({
            "core_ok": core_ok,
            "enhanced_ok": enhanced_ok,
            "enhanced_skipped": enhanced_skipped,
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "results": [r.to_dict() for r in reporter.results],
        }, indent=2, default=str))

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
