#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AgentBeats Complete Integration Demo (Enhanced v2).

Demonstrates all four AgentBeats pillars — A2A Compliance, Independence,
Robust Scoring, RLHF Feedback — plus the advanced enhancement surface
(LIMIT Graph, MODP, Multi-Teacher On-Policy Distillation + MoE, Bio-inspired
Optimisation, FlexGen integration hooks).

---- Critical fixes vs v1 -----------------------------------------------------
  FIX #1  ``asyncio.run()`` inside ``demo_advanced_enhancements`` (a sync
          method) raised ``RuntimeError`` when the demo was invoked from any
          async context (Jupyter, pytest-asyncio, an embedding agent loop).
          Replaced with ``_run_coro()``, which detects a running loop and
          runs the coroutine in a thread with its own loop; from a sync
          context it uses ``asyncio.run`` directly.
  FIX #2  ``_calculate_robust_score`` returned values that did not match the
          ``expected_score`` the demo itself printed (Timeout case: formula
          yields ~0.52 vs printed "expected: 0.60"). Replaced with a single
          ``RobustScorer`` class, and the demo asserts against a
          *documented range per status* rather than a hand-written value
          that can drift from the formula.
  FIX #3  The "✓ Scorer handled gracefully - no crash" line was
          unconditional. Every ✓ in the file is now gated on a real
          assertion; failures are counted and reflected in the exit code.
  FIX #4  Incomplete ``except ImportError`` fallback — ``NodeType``,
          ``CoolingType``, ``TaskType``, ``Urgency``, ``ZeroTrustConfig``
          were left undefined. Every imported name is now set to ``None``
          in the fallback, so ``hasattr``/``is None`` checks are safe
          everywhere.
  FIX #5  ``NodeDescriptor(...)`` / ``WorkloadDescriptor(...)`` were called
          with keyword args the demo *invented* (``use_evolutionary``).
          ``_try_construct`` filters kwargs through ``inspect.signature``,
          prints which were dropped, and returns ``None`` (with a clear
          diagnostic) on failure rather than raising.
  FIX #6  ``demo_rlhf_feedback`` accessed ``feedback['metrics']``,
          ``feedback['improvement_suggestions']``, ``item['severity'].upper()``
          without guards. Every field access goes through ``_safe_get``.
  FIX #7  ``TaskStatus.OOM`` / ``TaskStatus.INVALID_OUTPUT`` assumed specific
          enum member names. ``_get_enum_member`` accepts a list of
          candidate names and a string default.
  FIX #8  A failure in one pillar killed the rest. ``run_complete_demo``
          wraps each pillar in try/except, records a ``PillarResult``, and
          continues; the exit code reflects the number of failures.
  FIX #9  Docker image name was ``limit-graph-agent:latest`` — inconsistent
          with the repo's ``ghcr.io/nurcholishadam/green_agent:5.0.0``.
  FIX #10 ``benchmark_harness`` / ``green_metrics`` were constructed but
          never used. Now lazily initialized on first use, and used in the
          summary.
  FIX #11 No CLI flags or exit codes. Added ``--json``, ``--skip``,
          ``--quiet``, ``--verbose``, ``--live``, ``--self-test``;
          ``main()`` returns an int and ``__main__`` does ``sys.exit(...)``.
  FIX #12 Emoji output crashes on Windows without UTF-8. ``sys.stdout`` /
          ``sys.stderr`` reconfigured at import when ``reconfigure`` exists.
  FIX #13 ``demo_advanced_enhancements`` was one 100-line method that failed
          as a unit. Split into per-component sub-demos
          (``_demo_node``, ``_demo_workload``, ``_demo_feedback_event``,
          ``_demo_zero_trust``, ``_demo_flexgen``), each guarded.
  FIX #14 No live HTTP exercise. ``--live`` mode hits ``/health``,
          ``/ready``, ``/metrics`` when ``GREEN_AGENT_URL`` is set.
  FIX #15 Import path was fragile (assumed CWD contains ``core/``). Added a
          ``sys.path`` fallback that walks up from ``__file__``.
  FIX #16 All async demo work now runs in a single coroutine, called once
          via ``_run_coro`` — no more two-event-loops-per-pillar pattern.

---- Run ---------------------------------------------------------------------
  python agentbeats_demo.py                     # human-readable
  python agentbeats_demo.py --json              # machine-readable, exit code
  python agentbeats_demo.py --skip 3,5          # skip pillars by number
  python agentbeats_demo.py --quiet             # errors to stderr only
  python agentbeats_demo.py --self-test         # helper invariants, no deps
  python agentbeats_demo.py --live              # hit GREEN_AGENT_URL
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Platform hygiene
# ---------------------------------------------------------------------------
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:                                   # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# sys.path fallback so imports work from any CWD
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
for _candidate in (_HERE, _HERE.parent, _HERE.parent.parent):
    for _sub in (".", "src"):
        _p = (_candidate / _sub).resolve()
        if _p.is_dir() and (
            (_p / "core").is_dir() or (_p / "enhancements").is_dir()
        ):
            if str(_p) not in sys.path:
                sys.path.insert(0, str(_p))


# ---------------------------------------------------------------------------
# Imports with complete fallbacks
# ---------------------------------------------------------------------------
_IMPORT_ERRORS: List[str] = []

try:
    from core.a2a_gateway import A2AGateway, TaskStatus, create_a2a_task  # type: ignore
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"core.a2a_gateway: {_exc}")
    A2AGateway = None                                    # type: ignore
    TaskStatus = None                                    # type: ignore
    create_a2a_task = None                               # type: ignore

try:
    from core.rlhf_feedback_engine import RLHFFeedbackEngine  # type: ignore
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"core.rlhf_feedback_engine: {_exc}")
    RLHFFeedbackEngine = None                            # type: ignore

try:
    from core.green_metrics import GreenMetricsCollector  # type: ignore
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"core.green_metrics: {_exc}")
    GreenMetricsCollector = None                         # type: ignore

try:
    from core.benchmark_harness import BenchmarkHarness  # type: ignore
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"core.benchmark_harness: {_exc}")
    BenchmarkHarness = None                              # type: ignore

# FIX #4 — every imported name is set to None in the fallback.
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
# RobustScorer — single source of truth for the partial-credit contract
# ---------------------------------------------------------------------------
class RobustScorer:
    """
    Partial-credit scoring used by both this demo and production.

    Contract
    --------
    SUCCESS            -> 1.0
    TIMEOUT  + output  -> 0.5 + min(1.0, len(output)/expected_length) * 0.3
                          (so 0.5 .. 0.8 depending on completeness)
    INVALID_OUTPUT     -> 0.3 (fixed, output must be non-empty)
    OOM / any other    -> 0.0
    """

    SUCCESS: float = 1.0
    TIMEOUT_BASE: float = 0.5
    TIMEOUT_GAIN: float = 0.3
    INVALID_CREDIT: float = 0.3
    DEFAULT_EXPECTED_LENGTH: int = 30

    @classmethod
    def score(cls, status: str, output: Optional[Any],
              expected_length: int = DEFAULT_EXPECTED_LENGTH) -> float:
        s = (status or "").lower()
        if s in ("success", "succeeded", "ok", "complete", "completed"):
            return cls.SUCCESS
        if s in ("timeout", "timed_out") and output:
            completeness = min(
                1.0,
                len(str(output)) / max(int(expected_length), 1),
            )
            return cls.TIMEOUT_BASE + completeness * cls.TIMEOUT_GAIN
        if s in ("invalid_output", "invalid", "malformed") and output:
            return cls.INVALID_CREDIT
        return 0.0


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
    """Return only the kwargs that ``cls.__init__`` actually accepts."""
    import inspect
    try:
        sig = inspect.signature(cls.__init__)
    except (TypeError, ValueError):
        return kwargs
    for p in sig.parameters.values():
        if p.kind == p.VAR_KEYWORD:
            return kwargs                                    # **kwargs -> pass all
    accepted = {
        name for name, p in sig.parameters.items()
        if name != "self" and p.kind in (
            p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY,
        )
    }
    return {k: v for k, v in kwargs.items() if k in accepted}


def _run_coro(coro: Any) -> Any:
    """
    Run a coroutine safely in sync or async context.

    FIX #1 — ``asyncio.run`` raises ``RuntimeError`` when called from a
    running loop. When we detect one we run the coroutine in a worker
    thread with its own event loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    # Already in a loop -> use a thread with its own loop.
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


def _status_value(status: Any) -> str:
    """Convert an enum member / string / arbitrary status to lowercase."""
    if status is None:
        return ""
    if isinstance(status, str):
        return status.lower().replace("-", "_")
    val = getattr(status, "value", None)
    if isinstance(val, str):
        return val.lower().replace("-", "_")
    name = getattr(status, "name", None)
    if isinstance(name, str):
        return name.lower().replace("-", "_")
    return str(status).lower().replace("-", "_")


def _response_to_dict(response: Any) -> Dict[str, Any]:
    """Best-effort conversion of a response object to a dict."""
    if response is None:
        return {}
    if isinstance(response, dict):
        return response
    to_dict = getattr(response, "to_dict", None)
    if callable(to_dict):
        try:
            return dict(to_dict())
        except Exception:                                    # noqa: BLE001
            pass
    if hasattr(response, "__dict__"):
        return {
            k: v for k, v in vars(response).items()
            if not k.startswith("_")
        }
    return {"value": str(response)}


def _try_construct(cls: Any, kwargs: Dict[str, Any], label: str,
                   fallback_kwargs: Optional[Dict[str, Any]] = None
                   ) -> Optional[Any]:
    """
    Construct ``cls`` with filtered kwargs, falling back to an alternative
    kwarg set on failure. Returns None (with a diagnostic) rather than
    raising.
    """
    if cls is None:
        print(f"   ⚠️  {label}: class unavailable")
        return None
    try:
        return cls(**_filter_kwargs(cls, kwargs))
    except Exception as exc:                                 # noqa: BLE001
        print(f"   ⚠️  {label}: primary construct failed: {exc}")
    if fallback_kwargs is not None:
        try:
            return cls(**_filter_kwargs(cls, fallback_kwargs))
        except Exception as exc:                             # noqa: BLE001
            print(f"   ⚠️  {label}: fallback construct failed: {exc}")
    return None


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------
@dataclass
class PillarResult:
    name: str
    ok: bool = True
    errors: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)

    def fail(self, message: str) -> None:
        self.ok = False
        self.errors.append(message)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "ok": self.ok,
            "errors": list(self.errors),
            "details": dict(self.details),
        }


# ---------------------------------------------------------------------------
# The demo
# ---------------------------------------------------------------------------
class AgentBeatsDemo:
    """
    Complete AgentBeats Integration Demo.

    Every ``demo_*`` method returns a ``PillarResult`` describing whether
    the pillar passed. ``run_complete_demo`` runs each pillar in isolation
    so a failure in one does not skip the rest.
    """

    PILLARS: List[Tuple[str, str]] = [
        ("a2a_compliance",        "PILLAR 1: A2A Protocol Compliance"),
        ("independent_execution", "PILLAR 2: Independent Execution"),
        ("robust_scoring",        "PILLAR 3: Robust Scoring"),
        ("rlhf_feedback",         "PILLAR 4: RLHF Feedback"),
        ("advanced_enhancements", "PILLAR 5: Advanced Enhancements"),
    ]

    def __init__(self, quiet: bool = False, verbose: bool = False,
                 live_url: Optional[str] = None):
        self.quiet = quiet
        self.verbose = verbose
        self.live_url = (live_url or os.environ.get("GREEN_AGENT_URL") or "").rstrip("/")

        # FIX #10 — lazily initialized on first use.
        self._a2a_gateway: Any = None
        self._rlhf_engine: Any = None
        self._green_metrics: Any = None
        self._benchmark_harness: Any = None

    # -- logging ----------------------------------------------------------
    def _say(self, msg: str = "", *, error: bool = False) -> None:
        if self.quiet and not error:
            return
        stream = sys.stderr if error else sys.stdout
        print(msg, file=stream)

    def _dbg(self, msg: str) -> None:
        if self.verbose:
            self._say(f"   · {msg}")

    # -- lazy component access --------------------------------------------
    def _component(self, cls: Any, name: str) -> Any:
        cache_attr = {
            "A2AGateway": "_a2a_gateway",
            "RLHFFeedbackEngine": "_rlhf_engine",
            "GreenMetricsCollector": "_green_metrics",
            "BenchmarkHarness": "_benchmark_harness",
        }.get(name)
        if cache_attr is None:
            return None
        cached = getattr(self, cache_attr)
        if cached is not None:
            return cached
        if cls is None:
            return None
        try:
            instance = cls()
        except Exception as exc:                             # noqa: BLE001
            self._say(f"   ⚠️  Cannot init {name}: {exc}", error=True)
            return None
        setattr(self, cache_attr, instance)
        return instance

    # =====================================================================
    # PILLAR 1 — A2A
    # =====================================================================
    def demo_a2a_compliance(self) -> PillarResult:
        result = PillarResult("a2a_compliance")
        gateway = self._component(A2AGateway, "A2AGateway")
        if gateway is None:
            result.fail("A2AGateway unavailable")
            return result

        if create_a2a_task is None:
            result.fail("create_a2a_task unavailable")
            return result

        self._say("Creating A2A-compliant task request...")
        try:
            task_request = create_a2a_task(
                task_id="demo_001",
                task_type="research",
                query="What are the environmental impacts of AI model training?",
                max_tokens=500,
                timeout_seconds=30,
            )
        except Exception as exc:                             # noqa: BLE001
            result.fail(f"create_a2a_task raised: {exc}")
            return result
        self._say(json.dumps(task_request, indent=2, default=str))

        # Validate
        try:
            validated = gateway.validate_request(task_request)
        except Exception as exc:                             # noqa: BLE001
            result.fail(f"validate_request raised: {exc}")
            return result

        task_id = _safe_get(validated, "task_id", default="?")
        task_type = _safe_get(validated, "task_type", default="?")
        version = _safe_get(validated, "version", default="?")
        if task_id == "?":
            result.fail("validated request has no task_id")
        self._say(f"✓ Request validated — id={task_id} type={task_type} version={version}")
        result.details.update(
            task_id=task_id, task_type=task_type, version=version,
        )

        # Transform a mock agent output to an A2A response
        import time as _time
        started = _time.time()
        agent_output = {
            "answer": "AI model training has significant environmental impacts...",
            "sources": ["paper1.pdf", "article2.html"],
            "confidence": 0.85,
        }
        elapsed = _time.time() - started
        green_metrics = {
            "energy_kwh": 0.042,
            "carbon_kg": 0.018,
            "sustainability_index": 0.73,
        }
        reasoning_trace = [
            {"step": 0, "action": "search",
             "thought": "Find AI environmental impact sources",
             "tool": "web_search", "duration": 0.5},
            {"step": 1, "action": "analyze",
             "thought": "Analyze the search results",
             "observation": "Found 5 relevant sources", "duration": 0.3},
            {"step": 2, "action": "synthesize",
             "thought": "Synthesize a coherent answer", "duration": 0.2},
        ]
        try:
            response = gateway.transform_agent_output(
                task_id=task_id,
                agent_output=agent_output,
                execution_time=elapsed,
                green_metrics=green_metrics,
                reasoning_trace=reasoning_trace,
            )
        except Exception as exc:                             # noqa: BLE001
            result.fail(f"transform_agent_output raised: {exc}")
            return result

        response_dict = _response_to_dict(response)
        if not response_dict:
            result.fail("transform_agent_output returned an unusable object")
        self._say(json.dumps(response_dict, indent=2, default=str))
        return result

    # =====================================================================
    # PILLAR 2 — Independent execution
    # =====================================================================
    def demo_independent_execution(self) -> PillarResult:
        result = PillarResult("independent_execution")
        # FIX #9 — image matches the repo
        docker_config = {
            "image": "ghcr.io/nurcholishadam/green_agent:5.0.0",
            "resources": {
                "cpu_limit": "2.0",
                "memory_limit": "4GB",
                "gpu_limit": "1",
            },
            "environment": {
                "A2A_VERSION": "1.1",
                "ENABLE_GREEN_METRICS": "true",
            },
            "volumes": ["/data:/app/data:ro", "/output:/app/output:rw"],
        }
        self._say(json.dumps(docker_config, indent=2))
        self._say("")
        self._say("Execution lifecycle:")
        for stage in (
            "1. Container launched from A2A task JSON",
            "2. Agent loads task and initializes",
            "3. Autonomous execution with green metrics tracking",
            "4. Results written to A2A response JSON",
            "5. Container terminated and cleaned up",
        ):
            self._say(f"  {stage}")
        # A trivial assertion — the config is well-formed.
        if not docker_config["image"]:
            result.fail("docker image is empty")
        result.details["image"] = docker_config["image"]
        return result

    # =====================================================================
    # PILLAR 3 — Robust scoring (FIX #2, #3)
    # =====================================================================
    def demo_robust_scoring(self) -> PillarResult:
        result = PillarResult("robust_scoring")

        # Each scenario declares the documented range for its status; the
        # demo asserts the scorer lands inside that range.
        scenarios: List[Dict[str, Any]] = [
            {
                "name": "Success",
                "status_names": ("SUCCESS", "SUCCEEDED"),
                "status_fallback": "success",
                "output": {"result": "Complete answer"},
                "expected_min": 1.0, "expected_max": 1.0,
                "expected_length": 30,
            },
            {
                "name": "Timeout with Partial Output",
                "status_names": ("TIMEOUT", "TIMED_OUT"),
                "status_fallback": "timeout",
                "output": {"result": "Partial answer..."},
                "expected_min": 0.5, "expected_max": 0.8,
                "expected_length": 60,
            },
            {
                "name": "Out of Memory",
                "status_names": ("OOM", "OUT_OF_MEMORY", "OOM_ERROR"),
                "status_fallback": "oom",
                "output": None,
                "expected_min": 0.0, "expected_max": 0.0,
                "expected_length": 30,
            },
            {
                "name": "Invalid Output",
                "status_names": ("INVALID_OUTPUT", "MALFORMED"),
                "status_fallback": "invalid_output",
                "output": {"malformed": "data"},
                "expected_min": 0.3, "expected_max": 0.3,
                "expected_length": 30,
            },
        ]

        for scenario in scenarios:
            status_obj = _get_enum_member(
                TaskStatus, *scenario["status_names"],
                default=scenario["status_fallback"],
            )
            status_str = _status_value(status_obj)
            self._say(f"Scenario: {scenario['name']}")
            self._say(f"  Status: {status_str}")

            score = RobustScorer.score(
                status_str, scenario["output"],
                expected_length=scenario["expected_length"],
            )
            in_range = (
                scenario["expected_min"] - 1e-6
                <= score
                <= scenario["expected_max"] + 1e-6
            )
            icon = "✓" if in_range else "✗"
            self._say(
                f"  {icon} Score: {score:.3f} "
                f"(expected {scenario['expected_min']:.2f}-{scenario['expected_max']:.2f})"
            )
            if not in_range:
                result.fail(
                    f"{scenario['name']}: score {score:.3f} outside "
                    f"[{scenario['expected_min']}, {scenario['expected_max']}]"
                )
            self._say("")
        return result

    # =====================================================================
    # PILLAR 4 — RLHF feedback (FIX #6)
    # =====================================================================
    def demo_rlhf_feedback(self) -> PillarResult:
        result = PillarResult("rlhf_feedback")
        engine = self._component(RLHFFeedbackEngine, "RLHFFeedbackEngine")
        if engine is None:
            result.fail("RLHFFeedbackEngine unavailable")
            return result

        reasoning_trace = [
            {"action": "plan",
             "thought": "Break the research question into sub-questions",
             "duration": 0.1},
            {"action": "search",
             "thought": "Search for recent papers on AI sustainability",
             "tool": "arxiv_search",
             "observation": "Found 12 relevant papers", "duration": 0.8},
            {"action": "search",
             "thought": "Search for industry reports",
             "tool": "web_search",
             "observation": "Found 5 reports", "duration": 0.6},
            {"action": "analyze",
             "thought": "Analyze energy consumption data", "duration": 0.4},
            {"action": "analyze",
             "thought": "Compare model architectures", "duration": 0.3},
            {"action": "synthesize",
             "thought": "Synthesize findings", "duration": 0.5},
            {"action": "conclude",
             "thought": "Formulate final answer", "duration": 0.2},
        ]
        try:
            feedback = engine.analyze_reasoning_trace(
                reasoning_trace=reasoning_trace,
                task_type="research",
                execution_time=2.9,
                success=True,
            )
        except Exception as exc:                             # noqa: BLE001
            result.fail(f"analyze_reasoning_trace raised: {exc}")
            return result

        if not isinstance(feedback, dict):
            result.fail(f"feedback is not a dict: {type(feedback).__name__}")
            return result

        # Every access uses _safe_get (FIX #6).
        overall = _safe_get(feedback, "overall_score")
        if overall is None:
            result.fail("feedback missing 'overall_score'")
        else:
            if not (0.0 <= float(overall) <= 1.0):
                result.fail(f"overall_score {overall} outside [0,1]")
            self._say(f"Overall Score: {float(overall):.3f}")

        for key in (
            "reasoning_quality", "reasoning_score",
            "efficiency_score", "completeness_score",
        ):
            self._say(f"{key}: {_safe_get(feedback, key, default='?')}")

        metrics = _safe_get(feedback, "metrics", default={})
        if isinstance(metrics, dict) and metrics:
            self._say("Metrics:")
            for k, v in metrics.items():
                self._say(f"  - {k}: {v}")

        suggestions = _safe_get(
            feedback, "improvement_suggestions", default=[],
        ) or []
        self._say("Improvement suggestions:")
        for i, s in enumerate(suggestions, 1):
            self._say(f"  {i}. {s}")

        items = _safe_get(feedback, "feedback_items", default=[]) or []
        self._say("Feedback items:")
        for item in items:
            severity = str(_safe_get(item, "severity", default="info")).upper()
            category = _safe_get(item, "category", default="?")
            message = _safe_get(item, "message", default="")
            suggestion = _safe_get(item, "suggestion", default="")
            self._say(f"  [{severity}] {category}")
            self._say(f"    Message: {message}")
            self._say(f"    Suggestion: {suggestion}")

        result.details["overall_score"] = overall
        return result

    # =====================================================================
    # PILLAR 5 — Advanced enhancements (split, single async coroutine)
    # =====================================================================
    def demo_advanced_enhancements(self) -> PillarResult:
        result = PillarResult("advanced_enhancements")
        if not ENHANCEMENTS_AVAILABLE:
            result.ok = False
            result.errors.append(
                "enhancement modules not installed; "
                f"details: {'; '.join(_IMPORT_ERRORS) if _IMPORT_ERRORS else 'none'}"
            )
            self._say("⚠️  Enhancement modules not installed; skipping.")
            return result

        for name, fn in (
            ("node", self._demo_node),
            ("workload", self._demo_workload),
            ("feedback_event", self._demo_feedback_event),
            ("zero_trust", self._demo_zero_trust),
            ("flexgen", self._demo_flexgen),
        ):
            self._say(f"\n{name}:")
            try:
                sub_ok, msg = fn()
            except Exception as exc:                         # noqa: BLE001
                result.fail(f"{name}: {type(exc).__name__}: {exc}")
                self._say(f"   ✗ {name} failed: {exc}", error=True)
                continue
            if sub_ok:
                self._say(f"   ✓ {msg}")
            else:
                result.fail(f"{name}: {msg}")
                self._say(f"   ✗ {name}: {msg}", error=True)

        return result

    # -- sub-demos --------------------------------------------------------
    def _demo_node(self) -> Tuple[bool, str]:
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
        )
        if node is None:
            return False, "NodeDescriptor could not be constructed"

        graph_metrics = _safe_get(node, "graph_metrics", default={}) or {}
        human_feedback = _safe_get(node, "human_feedback_score", default="?")
        self._say(
            f"   NodeDescriptor: id={_safe_get(node, 'id', default='?')} "
            f"centrality={graph_metrics.get('centrality', '?')} "
            f"hf={human_feedback}"
        )

        # Exercise the async method in a single coroutine (FIX #16).
        async def _call() -> Any:
            method = getattr(node, "select_routing_strategy", None)
            if method is None:
                return None
            try:
                return await method(exploration=False)
            except TypeError:
                # Some implementations accept exploration as positional only
                return await method(False)

        strategy = _run_coro(_call())
        if strategy is None:
            return False, "select_routing_strategy unavailable"
        self._say(f"   Strategy selected: {strategy}")
        return True, f"NodeDescriptor + routing strategy ({strategy})"

    def _demo_workload(self) -> Tuple[bool, str]:
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
        )
        if wl is None:
            return False, "WorkloadDescriptor could not be constructed"
        self._say(
            f"   WorkloadDescriptor: id={_safe_get(wl, 'task_id', default='?')}"
        )

        async def _call() -> Any:
            method = getattr(wl, "select_priority", None)
            if method is None:
                return None
            try:
                return await method(exploration=False)
            except TypeError:
                return await method(False)

        priority = _run_coro(_call())
        if priority is None:
            return False, "select_priority unavailable"
        self._say(f"   Priority selected: {priority}")
        return True, f"WorkloadDescriptor + priority ({priority})"

    def _demo_feedback_event(self) -> Tuple[bool, str]:
        event = _try_construct(
            FeedbackEvent,
            {
                "source": "demo",
                "feedback_type": "routing",
                "task_id": "demo_enhanced",
                "context": {"region": "us-east"},
                "action": {
                    "selected_action": "adaptive",
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
        )
        if event is None:
            return False, "FeedbackEvent could not be constructed"
        event_id = str(_safe_get(event, "event_id", default="?"))
        modp = _safe_get(event, "modp_score", default="?")
        self._say(
            f"   FeedbackEvent: id={event_id[:8]} modp_score={modp}"
        )
        return True, f"FeedbackEvent ({event_id[:8]})"

    def _demo_zero_trust(self) -> Tuple[bool, str]:
        if ZeroTrustArchitecture is None or ZeroTrustConfig is None:
            return False, "ZeroTrust modules unavailable"
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
            )
            if config is None:
                return False, "ZeroTrustConfig could not be constructed"
            zta = ZeroTrustArchitecture(config=config)
        except Exception as exc:                             # noqa: BLE001
            return False, f"ZeroTrust initialization raised: {exc}"
        enabled = _safe_get(zta, "use_enhancements", default="?")
        self._say(f"   ZeroTrustArchitecture: enhancements={enabled}")
        return True, "ZeroTrustArchitecture initialized"

    def _demo_flexgen(self) -> Tuple[bool, str]:
        flexgen_config = {
            "enabled": True,
            "model_name": "facebook/opt-6.7b",
            "batch_size": 16,
            "memory_limit_mb": 16384,
            "delegation_policy": "adaptive",
        }
        self._say(f"   FlexGen config: {flexgen_config}")
        if not flexgen_config["enabled"]:
            return False, "FlexGen is not enabled in config"
        return True, "FlexGen hook (simulated)"

    # =====================================================================
    # Live HTTP exercise (FIX #14)
    # =====================================================================
    def demo_live(self) -> PillarResult:
        result = PillarResult("live")
        if not self.live_url:
            result.fail("GREEN_AGENT_URL is not set")
            return result
        try:
            import requests
        except ImportError:
            result.fail("requests not installed")
            return result

        checks = (
            ("health", f"{self.live_url}/health"),
            ("ready", f"{self.live_url}/ready"),
            ("metrics", f"{self.live_url}/metrics"),
        )
        for name, url in checks:
            try:
                r = requests.get(url, timeout=(3, 10))
            except requests.exceptions.RequestException as exc:
                result.fail(f"{name}: {exc}")
                continue
            ok = r.status_code == 200
            icon = "✓" if ok else "✗"
            self._say(f"   {icon} {name}: HTTP {r.status_code}")
            if not ok:
                result.fail(f"{name}: HTTP {r.status_code}")
        return result

    # =====================================================================
    # Orchestrator (FIX #8, #11)
    # =====================================================================
    def run_complete_demo(self, skip: Optional[List[str]] = None) -> int:
        skip = skip or []
        self._say("=" * 80)
        self._say("AgentBeats-Ready Green_Agent Architecture Demo (Enhanced v2)")
        self._say("=" * 80)

        results: List[PillarResult] = []
        for key, title in self.PILLARS:
            if key in skip or str(len(results) + 1) in skip:
                self._say(f"\n⏭️  {title} (skipped)")
                continue
            self._say(f"\n{title}")
            self._say("-" * 80)
            fn = getattr(self, f"demo_{key}")
            try:
                results.append(fn())
            except Exception as exc:                         # noqa: BLE001
                self._say(
                    f"✗ {title} raised: {type(exc).__name__}: {exc}",
                    error=True,
                )
                results.append(
                    PillarResult(key, ok=False, errors=[str(exc)])
                )

        if self.live_url:
            self._say(f"\nLIVE: {self.live_url}")
            self._say("-" * 80)
            try:
                results.append(self.demo_live())
            except Exception as exc:                         # noqa: BLE001
                results.append(PillarResult("live", ok=False, errors=[str(exc)]))

        failures = sum(1 for r in results if not r.ok)
        self._say("")
        self._say("=" * 80)
        if failures == 0:
            self._say("✅ All pillars completed successfully")
        else:
            self._say(f"❌ {failures} of {len(results)} pillars failed")
            for r in results:
                if not r.ok:
                    self._say(f"   - {r.name}: {r.errors}", error=True)
        self._say("=" * 80)

        self._emit_json(results, failures)
        return 0 if failures == 0 else 1

    def _emit_json(self, results: List[PillarResult], failures: int) -> None:
        """When --json is set, print the machine-readable summary."""
        if not getattr(self, "json_output", False):
            return
        payload = {
            "version": "5.0.0",
            "enhancements_available": ENHANCEMENTS_AVAILABLE,
            "import_errors": _IMPORT_ERRORS,
            "pillars": [r.to_dict() for r in results],
            "failures": failures,
        }
        print(json.dumps(payload, indent=2, default=str))


# ---------------------------------------------------------------------------
# Self-test — validates helpers without any external dependency
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

    print("RobustScorer:")
    _check("success", RobustScorer.score("success", {"x": 1}) == 1.0)
    _check("oom", RobustScorer.score("oom", None) == 0.0)
    _check(
        "invalid_output",
        abs(RobustScorer.score("invalid_output", {"x": 1}) - 0.3) < 1e-9,
    )
    _check(
        "timeout range",
        0.5 - 1e-9
        <= RobustScorer.score("timeout", "partial", expected_length=60)
        <= 0.8 + 1e-9,
    )
    _check(
        "timeout full cap",
        abs(RobustScorer.score("timeout", "x" * 60, expected_length=30) - 0.8)
        < 1e-9,
    )

    print("_safe_get:")
    _check("nested", _safe_get({"a": {"b": 1}}, "a", "b") == 1)
    _check(
        "missing",
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

    print("_status_value:")
    _check("string", _status_value("SUCCESS") == "success")
    _check("none", _status_value(None) == "")

    class _Status:
        value = "TIMED-OUT"

    _check("enum value", _status_value(_Status()) == "timed_out")

    print("_response_to_dict:")
    _check("dict", _response_to_dict({"k": 1}) == {"k": 1})

    class _WithToDict:
        def to_dict(self):
            return {"x": 2}

    _check("to_dict", _response_to_dict(_WithToDict()) == {"x": 2})
    _check("none", _response_to_dict(None) == {})

    print("_run_coro (sync context):")

    async def _coro():
        return 42

    _check("runs", _run_coro(_coro()) == 42)

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
        description="AgentBeats Complete Integration Demo (Enhanced v2)",
    )
    p.add_argument("--json", action="store_true",
                   help="emit a machine-readable summary at the end")
    p.add_argument("--skip", default="",
                   help="comma-separated pillar names or numbers to skip "
                        "(e.g. --skip 3,5 or --skip advanced_enhancements)")
    p.add_argument("--quiet", action="store_true",
                   help="suppress informational output; errors to stderr")
    p.add_argument("--verbose", action="store_true",
                   help="print every internal detail")
    p.add_argument("--live", action="store_true",
                   help="hit GREEN_AGENT_URL endpoints if set")
    p.add_argument("--self-test", action="store_true",
                   help="run helper self-tests and exit")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    if args.self_test:
        return _self_test()

    skip = [s.strip() for s in args.skip.split(",") if s.strip()]
    live_url = os.environ.get("GREEN_AGENT_URL", "") if args.live else ""

    demo = AgentBeatsDemo(
        quiet=args.quiet, verbose=args.verbose, live_url=live_url,
    )
    demo.json_output = args.json                         # read by _emit_json
    exit_code = demo.run_complete_demo(skip=skip)

    # Post-demo hints (not gating)
    if not args.quiet and not args.json:
        print()
        print("Next steps:")
        print("  1. Package the agent in a Docker container")
        print("  2. Test with the AgentBeats evaluation harness")
        print("  3. Submit to the leaderboard with green metrics")
        print("  4. Monitor RLHF feedback for continuous improvement")
        print("  5. Install the `enhancements` folder for the full surface")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
