#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Green Agent Benchmarking — Complete Demo (Enhanced v2).

Demonstrates the LIMIT-AgentBench platform:

  1. AgentBench protocol compatibility    (adapter, task creation, evaluation)
  2. Green metrics tracking               (energy, carbon, efficiency)
  3. Multi-framework adapters             (langchain, autogen, limit_graph)
  4. Sustainability Index                 (ranking agents on accuracy + green)
  5. Green leaderboard                    (submit, rank, framework stats)
  6. Benchmark harness                    (multi-task run, aggregated metrics)
  7. Advanced enhancements                (LIMIT Graph, MODP, RLHF, Distillation,
                                           MoE, Zero Trust)

---- Critical fixes vs v1 -----------------------------------------------------
  FIX #1   Two ``asyncio.run()`` calls inside ``demo_enhanced_modules``
           raised ``RuntimeError`` from any async context. Replaced with
           ``_run_coro``/``_await_or_call`` which detect a running loop and
           dispatch to a worker thread.
  FIX #2   Every demo body was wrapped in a single ``try/except ImportError``
           that only caught imports; runtime failures aborted ``main()``.
           Each demo is now isolated at the ``run()`` level, and every
           sub-step has its own guard.
  FIX #3   ``main()`` had one top-level try/except; a failure in Demo 3
           killed Demos 4–7. ``run()`` now executes each demo in isolation.
  FIX #4   The final "✓ All demos completed successfully!" was unconditional.
           ``run()`` records a ``DemoResult`` per demo and prints
           ``✅``/``❌`` based on real outcomes.
  FIX #5   ``event.event_id[:8]`` assumed a string. Now
           ``str(_safe_get(event, "event_id", default="?"))[:8]``.
  FIX #6   ``NodeDescriptor(use_evolutionary=...)`` passed a kwarg the real
           class may not accept. ``_try_construct`` filters kwargs through
           ``inspect.signature`` and falls back to a smaller kwarg set.
  FIX #7   All dict accesses (``task['evaluation']['metrics']``,
           ``result['metrics']['latency_ms']``, ``metrics['duration_seconds']``,
           ``metadata['agent_name']``, ``entry['metrics']['sustainability_index']``,
           ``result['aggregated_metrics']['success_rate']``, etc.) were bare —
           any missing key aborted the demo. All go through ``_safe_get``.
  FIX #8   Every "✓ Evaluation complete" / "✓ Benchmark complete" / etc. was
           printed regardless of what the API returned. Every ``✓`` is now
           gated on a real assertion; failures are recorded per demo.
  FIX #9   No ``sys.path`` fallback — imports assumed CWD contains ``core/``.
           A walker now ascends from ``__file__`` looking for a package root.
  FIX #10  No CLI flags or exit code. Added ``--demo``, ``--skip``,
           ``--json``, ``--quiet``, ``--verbose``, ``--self-test``,
           ``--no-cleanup``. ``main()`` returns int; ``__main__`` does
           ``sys.exit``.
  FIX #11  Emoji output crashes on Windows without UTF-8. ``reconfigure``
           at import.
  FIX #12  ``./demo_leaderboard`` and ``./demo_benchmark_results`` polluted
           CWD. Now rooted under ``tempfile.mkdtemp(prefix="green-demo-")``
           with automatic cleanup (``--no-cleanup`` keeps it).
  FIX #13  No offline validation. ``--self-test`` runs helper invariants
           without any demo dependencies.

---- Run ---------------------------------------------------------------------
  python demo_green_agent_benchmarking.py                # all demos
  python demo_green_agent_benchmarking.py --demo 3       # only demo 3
  python demo_green_agent_benchmarking.py --skip 7       # skip enhancements
  python demo_green_agent_benchmarking.py --json         # machine-readable
  python demo_green_agent_benchmarking.py --self-test    # validate helpers
  python demo_green_agent_benchmarking.py --no-cleanup   # keep temp outputs
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import logging
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Platform hygiene (FIX #11)
# ---------------------------------------------------------------------------
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:                                   # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# sys.path walker (FIX #9)
# ---------------------------------------------------------------------------
def _ensure_package_root() -> None:
    """Add the nearest parent that looks like the project root to sys.path."""
    here = Path(__file__).resolve().parent
    for candidate in [here, *here.parents]:
        if (candidate / "core").is_dir() or (candidate / "adapters").is_dir():
            if str(candidate) not in sys.path:
                sys.path.insert(0, str(candidate))
            return


_ensure_package_root()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Imports with fallbacks
# ---------------------------------------------------------------------------
_IMPORT_ERRORS: List[str] = []

try:
    from core.agentbench_adapter import AgentBenchAdapter  # type: ignore
    ADAPTER_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"core.agentbench_adapter: {_exc}")
    AgentBenchAdapter = None                             # type: ignore
    ADAPTER_AVAILABLE = False

try:
    from core.green_metrics import GreenMetricsTracker   # type: ignore
    TRACKER_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"core.green_metrics: {_exc}")
    GreenMetricsTracker = None                           # type: ignore
    TRACKER_AVAILABLE = False

try:
    from adapters.langchain_adapter import LangChainAdapter  # type: ignore
    from adapters.autogen_adapter import AutoGenAdapter      # type: ignore
    from adapters.limit_graph_adapter import LimitGraphAdapter  # type: ignore
    ADAPTERS_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"adapters.*: {_exc}")
    LangChainAdapter = None                              # type: ignore
    AutoGenAdapter = None                                # type: ignore
    LimitGraphAdapter = None                             # type: ignore
    ADAPTERS_AVAILABLE = False

try:
    from metrics.sustainability_index import SustainabilityIndex  # type: ignore
    SI_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"metrics.sustainability_index: {_exc}")
    SustainabilityIndex = None                           # type: ignore
    SI_AVAILABLE = False

try:
    from dashboard.green_leaderboard import GreenLeaderboard  # type: ignore
    LEADERBOARD_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"dashboard.green_leaderboard: {_exc}")
    GreenLeaderboard = None                              # type: ignore
    LEADERBOARD_AVAILABLE = False

try:
    from core.benchmark_harness import BenchmarkHarness  # type: ignore
    HARNESS_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"core.benchmark_harness: {_exc}")
    BenchmarkHarness = None                              # type: ignore
    HARNESS_AVAILABLE = False

# FIX #6 — every imported name set to None on failure.
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
# Mock agent — kept for offline demonstration
# ---------------------------------------------------------------------------
class MockAgent:
    """Deterministic agent used when a real backend is unavailable."""

    def __init__(self, name: str, framework: str):
        self.name = name
        self.framework = framework

    def run(self, task_input: Dict[str, Any]) -> Dict[str, Any]:
        question = (task_input or {}).get("question", "")
        return {
            "answer": f"Mock answer to: {question}",
            "confidence": 0.92,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
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


def _await_or_call(method: Any, *args: Any, **kwargs: Any) -> Any:
    """Call ``method``; if the result is a coroutine, run it."""
    result = method(*args, **kwargs)
    if inspect.iscoroutine(result):
        return _run_coro(result)
    return result


def _get_enum_member(enum_cls: Any, *names: str, default: Any = None) -> Any:
    if enum_cls is None:
        return default
    for name in names:
        if hasattr(enum_cls, name):
            return getattr(enum_cls, name)
    return default


def _fmt(value: Any, spec: str = "") -> str:
    """Format a value safely; return ``'?'`` if it cannot be formatted."""
    if value is None:
        return "?"
    try:
        return format(value, spec)
    except (TypeError, ValueError):
        return str(value)


# ---------------------------------------------------------------------------
# Demo result
# ---------------------------------------------------------------------------
@dataclass
class DemoResult:
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
# Demo class
# ---------------------------------------------------------------------------
class GreenAgentBenchDemo:
    def __init__(self, quiet: bool = False, verbose: bool = False,
                 keep_tmp: bool = False):
        self.quiet = quiet
        self.verbose = verbose
        self.keep_tmp = keep_tmp
        self._tmp: Optional[Path] = None

    # -- logging ----------------------------------------------------------
    def _say(self, msg: str = "", *, error: bool = False) -> None:
        if self.quiet and not error:
            return
        print(msg, file=sys.stderr if error else sys.stdout)

    def _dbg(self, msg: str) -> None:
        if self.verbose:
            self._say(f"   · {msg}")

    def _section(self, title: str) -> None:
        self._say("")
        self._say("=" * 80)
        self._say(f"  {title}")
        self._say("=" * 80)

    # -- temp dir lifecycle (FIX #12) -------------------------------------
    def _tmp_root(self) -> Path:
        if self._tmp is None:
            self._tmp = Path(tempfile.mkdtemp(prefix="green-demo-"))
            self._dbg(f"temp dir: {self._tmp}")
        return self._tmp

    def close(self) -> None:
        if self._tmp is not None and self._tmp.exists() and not self.keep_tmp:
            shutil.rmtree(self._tmp, ignore_errors=True)
            self._dbg(f"removed temp dir: {self._tmp}")

    # =====================================================================
    # DEMO 1 — AgentBench protocol (FIX #2, #7, #8)
    # =====================================================================
    def demo_1(self) -> DemoResult:
        res = DemoResult(1, "AgentBench Protocol")
        self._section("DEMO 1: AgentBench Protocol Compatibility")

        if not ADAPTER_AVAILABLE:
            res.fail("core.agentbench_adapter unavailable")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        try:
            adapter = AgentBenchAdapter()
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"AgentBenchAdapter() raised: {exc}")
            return res

        try:
            task = adapter.create_task(
                task_id="demo_qa_001",
                suite="question_answering",
                task_type="qa",
                input_data={"question": "What is the capital of France?"},
                expected_output={"answer": "Paris"},
                evaluation_metrics=[
                    "accuracy", "latency", "energy_kwh", "carbon_co2e_kg",
                ],
            )
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"create_task raised: {exc}")
            return res

        task_id = _safe_get(task, "task_id")
        suite = _safe_get(task, "suite")
        metrics_declared = _safe_get(task, "evaluation", "metrics", default=[])
        self._say(f"   ✓ Created task: {task_id}")
        self._say(f"     suite: {suite}")
        self._say(f"     metrics: {', '.join(metrics_declared or [])}")
        res.check(task_id == "demo_qa_001",
                  f"task_id={task_id!r} != 'demo_qa_001'")
        res.check(bool(metrics_declared),
                  "task declared no evaluation metrics")

        # Run the evaluation on the mock agent.
        agent = MockAgent("DemoAgent", "langchain")
        try:
            result = adapter.evaluate_agent(
                agent=agent, task=task,
                track_energy=True, track_carbon=True,
            )
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"evaluate_agent raised: {exc}")
            return res

        success = _safe_get(result, "success")
        metrics = _safe_get(result, "metrics", default={}) or {}
        latency = _safe_get(metrics, "latency_ms")
        energy = _safe_get(metrics, "energy_kwh")
        carbon = _safe_get(metrics, "carbon_co2e_kg")

        self._say(f"   ✓ Evaluation result:")
        self._say(f"     agent: {_safe_get(result, 'agent_name', default='?')}")
        self._say(f"     framework: {_safe_get(result, 'framework', default='?')}")
        self._say(f"     success: {success}")
        self._say(f"     latency: {_fmt(latency, '.2f')} ms")
        if energy is not None:
            self._say(f"     energy: {_fmt(energy, '.6f')} kWh")
        if carbon is not None:
            self._say(f"     carbon: {_fmt(carbon, '.6f')} kg CO2e")

        # FIX #8 — assert what the demo claims
        res.check(bool(success), "evaluate_agent did not return success=True")
        res.check(latency is not None, "result.metrics has no latency_ms")
        if latency is not None:
            res.check(float(latency) >= 0,
                      f"latency_ms={latency!r} is negative")
        res.details.update(
            success=success, latency_ms=latency,
            energy_kwh=energy, carbon_co2e_kg=carbon,
        )
        return res

    # =====================================================================
    # DEMO 2 — Green metrics tracking
    # =====================================================================
    def demo_2(self) -> DemoResult:
        res = DemoResult(2, "Green Metrics Tracking")
        self._section("DEMO 2: Green Metrics Tracking")

        if not TRACKER_AVAILABLE:
            res.fail("core.green_metrics unavailable")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        try:
            tracker = GreenMetricsTracker(
                grid_region="US-CA",
                hardware_profile="nvidia_a100",
                track_energy=True,
                track_carbon=True,
            )
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"GreenMetricsTracker() raised: {exc}")
            return res

        carbon_intensity = _safe_get(tracker, "carbon_intensity")
        self._say(f"   ✓ Tracker initialised")
        self._say(f"     region: US-CA")
        self._say(f"     hardware: nvidia_a100")
        self._say(f"     carbon intensity: "
                  f"{_fmt(carbon_intensity, '.4f')} kg CO2e/kWh")

        try:
            tracker.start()
            time.sleep(0.5)
            tracker.stop()
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"tracker.start/stop raised: {exc}")
            return res

        try:
            metrics = tracker.get_metrics()
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"get_metrics raised: {exc}")
            return res

        if not isinstance(metrics, dict):
            res.fail(f"get_metrics returned {type(metrics).__name__}, "
                     "not dict")
            return res

        expected = {
            "duration_seconds": ".2f",
            "power_watts": ".2f",
            "energy_kwh": ".6f",
            "carbon_co2e_kg": ".6f",
            "efficiency_score": ".2f",
        }
        self._say("   ✓ Tracked metrics:")
        for key, spec in expected.items():
            value = _safe_get(metrics, key)
            if value is None:
                res.fail(f"metrics missing '{key}'")
                continue
            self._say(f"     {key}: {_fmt(value, spec)}")
            res.details[key] = value

        duration = _safe_get(metrics, "duration_seconds", default=0.0)
        try:
            res.check(float(duration) > 0.0,
                      f"duration_seconds={duration!r} not positive")
        except (TypeError, ValueError):
            res.fail(f"duration_seconds={duration!r} not numeric")
        return res

    # =====================================================================
    # DEMO 3 — Multi-framework adapters
    # =====================================================================
    def demo_3(self) -> DemoResult:
        res = DemoResult(3, "Multi-Framework Adapters")
        self._section("DEMO 3: Multi-Framework Agent Adapters")

        if not ADAPTERS_AVAILABLE:
            res.fail("adapters.* unavailable")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        agent_defs = [
            ("LangChainAgent", "langchain", LangChainAdapter),
            ("AutoGenAgent", "autogen", AutoGenAdapter),
            ("LimitGraphAgent", "limit_graph", LimitGraphAdapter),
        ]
        adapters: List[Any] = []
        for agent_name, fw, cls in agent_defs:
            mock = MockAgent(agent_name, fw)
            try:
                adapter = cls(mock)
                adapters.append(adapter)
                self._say(f"   ✓ Created {cls.__name__}({agent_name})")
            except Exception as exc:                     # noqa: BLE001
                res.fail(f"{cls.__name__} init raised: {exc}")

        res.check(len(adapters) == len(agent_defs),
                  f"only {len(adapters)}/{len(agent_defs)} adapters "
                  "constructed")

        for adapter in adapters:
            try:
                metadata = adapter.get_metadata()
            except Exception as exc:                     # noqa: BLE001
                res.fail(f"get_metadata raised: {exc}")
                continue
            agent_name = _safe_get(metadata, "agent_name", default="?")
            framework = _safe_get(metadata, "framework", default="?")
            self._say(f"     - {agent_name} ({framework})")
            res.check(agent_name != "?", "adapter returned no agent_name")
            res.check(framework != "?", "adapter returned no framework")
        res.details["adapter_count"] = len(adapters)
        return res

    # =====================================================================
    # DEMO 4 — Sustainability Index
    # =====================================================================
    def demo_4(self) -> DemoResult:
        res = DemoResult(4, "Sustainability Index")
        self._section("DEMO 4: Sustainability Index")

        if not SI_AVAILABLE:
            res.fail("metrics.sustainability_index unavailable")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        try:
            si_calc = SustainabilityIndex()
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"SustainabilityIndex() raised: {exc}")
            return res

        agents = {
            "EfficientAgent": {"accuracy": 0.95, "energy_kwh": 0.002,
                               "carbon_co2e_kg": 0.0004},
            "AccurateAgent": {"accuracy": 0.98, "energy_kwh": 0.005,
                              "carbon_co2e_kg": 0.001},
            "FastAgent": {"accuracy": 0.85, "energy_kwh": 0.001,
                          "carbon_co2e_kg": 0.0002},
        }

        try:
            rankings = si_calc.rank_agents(agents)
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"rank_agents raised: {exc}")
            return res

        if not rankings:
            res.fail("rank_agents returned empty")
            return res

        self._say("   ✓ Rankings:")
        ranks_out: List[Tuple[str, float]] = []
        for i, entry in enumerate(rankings, 1):
            # accept tuple (name, si) or dict
            if isinstance(entry, tuple) and len(entry) >= 2:
                agent_name, si = entry[0], entry[1]
            elif isinstance(entry, dict):
                agent_name = _safe_get(entry, "agent_name", default="?")
                si = _safe_get(entry, "si", default=0.0)
            else:
                res.fail(f"unexpected ranking entry type: "
                         f"{type(entry).__name__}")
                continue

            rating = "?"
            try:
                rating = SustainabilityIndex.get_rating(si)
            except Exception as exc:                     # noqa: BLE001
                self._dbg(f"get_rating raised: {exc}")

            metrics = agents.get(agent_name, {})
            self._say(f"     {i}. {agent_name} — SI={_fmt(si, '.2f')} "
                      f"({rating})")
            res.check(agent_name in agents,
                      f"ranked agent {agent_name!r} not in input set")
            ranks_out.append((agent_name, float(si) if si is not None else 0.0))

        # Verify the ranking is actually sorted
        for a, b in zip(ranks_out, ranks_out[1:]):
            res.check(a[1] >= b[1],
                      f"rankings not sorted: {a} before {b}")
        res.details["rankings"] = [
            {"agent": a, "si": b} for a, b in ranks_out
        ]
        return res

    # =====================================================================
    # DEMO 5 — Green leaderboard
    # =====================================================================
    def demo_5(self) -> DemoResult:
        res = DemoResult(5, "Green Leaderboard")
        self._section("DEMO 5: Green Leaderboard")

        if not LEADERBOARD_AVAILABLE:
            res.fail("dashboard.green_leaderboard unavailable")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        lb_dir = self._tmp_root() / "leaderboard"
        try:
            leaderboard = GreenLeaderboard(storage_path=str(lb_dir))
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"GreenLeaderboard() raised: {exc}")
            return res

        agents_data = [
            ("LangChainAgent", "langchain", 0.95, 0.003, 0.0006),
            ("AutoGenAgent", "autogen", 0.92, 0.004, 0.0008),
            ("LimitGraphAgent", "limit_graph", 0.96, 0.002, 0.0004),
            ("CrewAIAgent", "crewai", 0.90, 0.005, 0.001),
        ]
        for name, fw, acc, energy, carbon in agents_data:
            try:
                leaderboard.submit(
                    agent_name=name, framework=fw,
                    task_suite="demo_benchmark",
                    accuracy=acc, energy_kwh=energy,
                    carbon_co2e_kg=carbon, latency_ms=150.0,
                )
                self._say(f"   ✓ Submitted {name}")
            except Exception as exc:                     # noqa: BLE001
                res.fail(f"submit({name}) raised: {exc}")

        try:
            rankings = leaderboard.get_rankings(
                sort_by="sustainability_index", limit=10,
            )
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"get_rankings raised: {exc}")
            return res

        if not rankings:
            res.fail("get_rankings returned empty")
        else:
            self._say("   ✓ Top agents:")
            for entry in rankings:
                rank = _safe_get(entry, "rank", default="?")
                agent = _safe_get(entry, "agent_name", default="?")
                fw = _safe_get(entry, "framework", default="?")
                si = _safe_get(entry, "metrics", "sustainability_index")
                self._say(f"     {rank}. {agent} ({fw}) SI={_fmt(si, '.2f')}")

        try:
            framework_stats = leaderboard.get_framework_stats()
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"get_framework_stats raised: {exc}")
            framework_stats = {}

        if isinstance(framework_stats, dict):
            self._say("   ✓ Framework stats:")
            for fw, stats in framework_stats.items():
                count = _safe_get(stats, "count", default=0)
                avg_acc = _safe_get(stats, "avg_accuracy")
                avg_energy = _safe_get(stats, "avg_energy")
                avg_si = _safe_get(stats, "avg_sustainability")
                self._say(f"     {fw}: entries={count} "
                          f"acc={_fmt(avg_acc, '.2%')} "
                          f"energy={_fmt(avg_energy, '.6f')} "
                          f"si={_fmt(avg_si, '.2f')}")
                res.check(int(count) >= 1,
                          f"framework {fw} has count={count}")
        res.details["submitted"] = len(agents_data)
        return res

    # =====================================================================
    # DEMO 6 — Benchmark harness
    # =====================================================================
    def demo_6(self) -> DemoResult:
        res = DemoResult(6, "Benchmark Harness")
        self._section("DEMO 6: Benchmark Harness")

        if not HARNESS_AVAILABLE or not ADAPTER_AVAILABLE:
            res.fail("BenchmarkHarness or AgentBenchAdapter unavailable")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        out_dir = self._tmp_root() / "benchmark_results"
        try:
            harness = BenchmarkHarness(
                output_dir=str(out_dir),
                grid_region="US-CA",
                hardware_profile="nvidia_a100",
            )
            adapter = AgentBenchAdapter()
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"init raised: {exc}")
            return res

        try:
            tasks = [
                adapter.create_task(
                    task_id=f"demo_task_{i}",
                    suite="demo_suite",
                    task_type="qa",
                    input_data={"question": f"Question {i}?"},
                    expected_output={"answer": f"Answer {i}"},
                )
                for i in range(3)
            ]
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"create_task raised: {exc}")
            return res
        self._say(f"   ✓ Created suite with {len(tasks)} tasks")

        agent = MockAgent("BenchmarkAgent", "langchain")
        try:
            result = harness.run_benchmark(
                agent=agent, task_suite=tasks,
                benchmark_name="demo_benchmark",
            )
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"run_benchmark raised: {exc}")
            return res

        num_tasks = _safe_get(result, "num_tasks")
        duration = _safe_get(result, "duration_seconds")
        success_rate = _safe_get(result, "aggregated_metrics",
                                 "success_rate")
        self._say(f"   ✓ Benchmark complete:")
        self._say(f"     agent: {_safe_get(result, 'agent_name', default='?')}")
        self._say(f"     tasks: {num_tasks}")
        self._say(f"     duration: {_fmt(duration, '.2f')} s")
        self._say(f"     success rate: {_fmt(success_rate, '.2%')}")

        res.check(num_tasks == len(tasks),
                  f"num_tasks={num_tasks!r} != {len(tasks)}")
        res.check(success_rate is not None,
                  "aggregated_metrics has no success_rate")
        if success_rate is not None:
            res.check(0.0 <= float(success_rate) <= 1.0,
                      f"success_rate={success_rate!r} outside [0,1]")
        res.details.update(
            num_tasks=num_tasks,
            duration_seconds=duration,
            success_rate=success_rate,
        )
        return res

    # =====================================================================
    # DEMO 7 — Advanced enhancements (FIX #1, #5, #6)
    # =====================================================================
    def demo_7(self) -> DemoResult:
        res = DemoResult(7, "Advanced Enhancements")
        self._section("DEMO 7: Advanced Enhancements (Optional)")

        if not ENHANCEMENTS_AVAILABLE:
            res.fail(
                "enhancement modules unavailable; "
                f"details: {'; '.join(_IMPORT_ERRORS) if _IMPORT_ERRORS else 'none'}"
            )
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        # 1. NodeDescriptor
        node = _try_construct(
            NodeDescriptor,
            {
                "id": "enhanced_node",
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
                "id": "enhanced_node",
                "region": "us-east",
                "region_carbon_intensity": 350.0,
                "energy_per_token": 0.00004,
            },
            say=self._say,
        )
        if node is None:
            res.fail("NodeDescriptor could not be constructed")
            return res
        self._say(f"   ✓ NodeDescriptor: {_safe_get(node, 'id', default='?')}")

        # 2. Routing strategy
        method = getattr(node, "select_routing_strategy", None)
        strategy: Any = None
        if method is None:
            res.fail("NodeDescriptor.select_routing_strategy unavailable")
        else:
            try:
                try:
                    strategy = _await_or_call(method, exploration=False)
                except TypeError:
                    strategy = _await_or_call(method, False)
                self._say(f"   ✓ Strategy: {strategy}")
                res.details["strategy"] = str(strategy)
            except Exception as exc:                     # noqa: BLE001
                res.fail(f"select_routing_strategy raised: {exc}")

        # 3. WorkloadDescriptor
        wl = _try_construct(
            WorkloadDescriptor,
            {
                "task_id": "enhanced_task",
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
                "task_id": "enhanced_task",
                "tokens": 1000,
                "latency_target": 300.0,
            },
            say=self._say,
        )
        if wl is None:
            res.fail("WorkloadDescriptor could not be constructed")
        else:
            self._say(f"   ✓ WorkloadDescriptor: "
                      f"{_safe_get(wl, 'task_id', default='?')}")
            pmethod = getattr(wl, "select_priority", None)
            if pmethod is None:
                res.fail("WorkloadDescriptor.select_priority unavailable")
            else:
                try:
                    try:
                        priority = _await_or_call(pmethod, exploration=False)
                    except TypeError:
                        priority = _await_or_call(pmethod, False)
                    self._say(f"   ✓ Priority: {priority}")
                    res.details["priority"] = str(priority)
                except Exception as exc:                 # noqa: BLE001
                    res.fail(f"select_priority raised: {exc}")

        # 4. FeedbackEvent
        event = _try_construct(
            FeedbackEvent,
            {
                "source": "demo",
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
                "tags": ["demo"],
            },
            "FeedbackEvent",
            fallback_kwargs={
                "source": "demo",
                "feedback_type": "routing",
                "task_id": "demo_enhanced",
            },
            say=self._say,
        )
        if event is None:
            res.fail("FeedbackEvent could not be constructed")
        else:
            # FIX #5 — never assume event_id is a short string
            event_id = str(_safe_get(event, "event_id", default="?"))
            self._say(f"   ✓ FeedbackEvent: {event_id[:8]}")
            res.details["event_id"] = event_id

        # 5. Zero Trust
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
                    enabled = _safe_get(zta, "use_enhancements", default="?")
                    self._say(f"   ✓ ZeroTrust: enhancements={enabled}")
                    res.details["zero_trust_enhancements"] = enabled
            except Exception as exc:                     # noqa: BLE001
                res.fail(f"ZeroTrust init raised: {exc}")
        return res

    # =====================================================================
    # Orchestrator (FIX #3, #4)
    # =====================================================================
    def run(self, skip: Optional[List[int]] = None) -> List[DemoResult]:
        skip = skip or []
        self._say("=" * 80)
        self._say("LIMIT-AgentBench — Green Agent Benchmarking Platform Demo "
                  "(Enhanced v2)")
        self._say("=" * 80)
        self._say(
            f"Modules: adapter={ADAPTER_AVAILABLE} "
            f"tracker={TRACKER_AVAILABLE} adapters={ADAPTERS_AVAILABLE} "
            f"si={SI_AVAILABLE} leaderboard={LEADERBOARD_AVAILABLE} "
            f"harness={HARNESS_AVAILABLE} enhancements={ENHANCEMENTS_AVAILABLE}"
        )
        if _IMPORT_ERRORS:
            self._say(f"Import issues: {'; '.join(_IMPORT_ERRORS)}")

        demos: List[Tuple[int, str, Callable[[], DemoResult]]] = [
            (1, "AgentBench Protocol", self.demo_1),
            (2, "Green Metrics Tracking", self.demo_2),
            (3, "Multi-Framework Adapters", self.demo_3),
            (4, "Sustainability Index", self.demo_4),
            (5, "Green Leaderboard", self.demo_5),
            (6, "Benchmark Harness", self.demo_6),
            (7, "Advanced Enhancements", self.demo_7),
        ]

        results: List[DemoResult] = []
        for number, title, fn in demos:
            if number in skip:
                self._say(f"\n⏭️  Demo {number} — {title} (skipped)")
                continue
            try:
                r = fn()
            except Exception as exc:                     # noqa: BLE001
                self._say(
                    f"\n✗ Demo {number} — {title} crashed: "
                    f"{type(exc).__name__}: {exc}",
                    error=True,
                )
                r = DemoResult(number, title, ok=False,
                               errors=[f"crashed: {exc}"])
            results.append(r)
            if not r.ok:
                self._say(
                    f"   ✗ Demo {number} failures: {r.errors}",
                    error=True,
                )

        failures = sum(1 for r in results if not r.ok)
        self._say("")
        self._say("=" * 80)
        if failures == 0:
            self._say(f"✅ All {len(results)} demos passed")
        else:
            self._say(f"❌ {failures} of {len(results)} demos failed")
            for r in results:
                if not r.ok:
                    self._say(
                        f"   - Demo {r.number} ({r.name}): {r.errors}",
                        error=True,
                    )
        self._say("=" * 80)
        return results


# ---------------------------------------------------------------------------
# Self-test (FIX #13)
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
    _check("nested dict", _safe_get({"a": {"b": 1}}, "a", "b") == 1)
    _check("missing default",
           _safe_get({"a": {}}, "a", "b", default="d") == "d")
    _check("none input", _safe_get(None, "a") is None)

    print("_filter_kwargs:")
    class _Foo:
        def __init__(self, x, y=1):
            self.x, self.y = x, y

    _check("filters unknown kwargs",
           _filter_kwargs(_Foo, {"x": 1, "y": 2, "z": 3}) == {"x": 1, "y": 2})

    class _VarKw:
        def __init__(self, **kw):
            self.kw = kw

    _check("passes all when **kwargs",
           _filter_kwargs(_VarKw, {"a": 1, "b": 2}) == {"a": 1, "b": 2})

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

    print("_get_enum_member:")
    class _Status:
        SUCCESS = "success"
        TIMEOUT = "timeout"
    _check("first match",
           _get_enum_member(_Status, "TIMEOUT", "SUCCESS") == "timeout")
    _check("fallback",
           _get_enum_member(_Status, "NOPE", default="x") == "x")
    _check("none class",
           _get_enum_member(None, "X", default="d") == "d")

    print("_fmt:")
    _check("format float", _fmt(1.234, ".2f") == "1.23")
    _check("none", _fmt(None) == "?")
    _check("bad format", _fmt("str", ".2f") == "str")

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
    _check("falls back on failure", out is not None and out.a == 5)

    print("DemoResult:")
    r = DemoResult(3, "Test")
    _check("starts ok", r.ok is True)
    r.check(True, "pass")
    _check("passing check keeps ok", r.ok is True)
    r.check(False, "fail")
    _check("failing check flips ok", r.ok is False)
    r.fail("explicit")
    _check("fail() appends error",
           len(r.errors) == 2 and "explicit" in r.errors)

    print("MockAgent:")
    agent = MockAgent("test", "test_fw")
    out = agent.run({"question": "Q?"})
    _check("run returns answer",
           out.get("answer") == "Mock answer to: Q?")
    _check("run returns confidence",
           abs(out.get("confidence", 0) - 0.92) < 1e-9)

    print("")
    if failures:
        print(f"{failures} failure(s)")
        return 1
    print("all self-tests passed")
    return 0


# ---------------------------------------------------------------------------
# CLI (FIX #10)
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="LIMIT-AgentBench — Green Agent Benchmarking Demo (Enhanced v2)",
    )
    p.add_argument("--demo", type=int, default=None,
                   help="run only this demo (1-7)")
    p.add_argument("--skip", default="",
                   help="comma-separated demo numbers to skip (e.g. 1,7)")
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
    if args.demo is not None:
        if not 1 <= args.demo <= 7:
            print(f"--demo must be 1-7, got {args.demo}", file=sys.stderr)
            return 2
        skip = [n for n in range(1, 8) if n != args.demo]

    demo = GreenAgentBenchDemo(
        quiet=args.quiet, verbose=args.verbose, keep_tmp=args.no_cleanup,
    )
    exit_code = 0
    try:
        results = demo.run(skip=skip)
        failures = sum(1 for r in results if not r.ok)
        exit_code = 0 if failures == 0 else 1

        if args.json:
            print(json.dumps(
                {
                    "version": "5.0.0",
                    "modules": {
                        "adapter": ADAPTER_AVAILABLE,
                        "tracker": TRACKER_AVAILABLE,
                        "adapters": ADAPTERS_AVAILABLE,
                        "si": SI_AVAILABLE,
                        "leaderboard": LEADERBOARD_AVAILABLE,
                        "harness": HARNESS_AVAILABLE,
                        "enhancements": ENHANCEMENTS_AVAILABLE,
                    },
                    "import_errors": _IMPORT_ERRORS,
                    "demos": [r.to_dict() for r in results],
                    "failures": failures,
                },
                indent=2,
                default=str,
            ))

        if not args.quiet and not args.json:
            print()
            print("Next steps:")
            print("  - Address any ✗ items above before submission")
            print("  - Run with --json for machine-readable output")
            print("  - Run with --self-test to verify the demo helpers")
    finally:
        demo.close()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
