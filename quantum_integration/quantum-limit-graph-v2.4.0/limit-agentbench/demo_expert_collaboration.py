#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Expert Collaboration System — Complete Demo (Enhanced v2)
==========================================================

Demonstrates every component of the expert collaboration layer:

  1. Expert Model Gateway         (multi-provider, caching, sustainability)
  2. Domain-Specific Connectors   (compiler, static analysis, energy benchmark)
  3. Selective Invocation Policy  (confidence, sustainability, criticality)
  4. Human Review Portal          (critical-task escalation queue)
  5. Audit Logger & Transparency  (event log + accountability report)
  6. End-to-End Workflow          (integration across the components)
  7. Advanced Enhancements        (LIMIT Graph, MODP, RLHF, Distillation,
                                   MoE gating, Zero Trust)

---- Critical fixes vs v1 -----------------------------------------------------
  FIX #1   ``asyncio.run()`` inside ``demo_advanced_enhancements`` (a sync
           method) raised ``RuntimeError`` when the demo ran from any async
           context. Replaced with ``_run_coro``/``_await_or_call`` which
           dispatches to a worker thread when a loop is already running.
  FIX #2   Demo 1 imported ``create_multi_provider_gateway`` but **never
           called it** — the "✓ initialized" line was printed unconditionally.
           The gateway is now actually constructed, and the demo passes only
           if construction succeeds or reports a documented "needs API key"
           skip.
  FIX #3   Demo 3 printed "Should escalate: {decision.should_escalate}"
           without ever comparing against the documented expected value.
           Every case now asserts both ``should_escalate`` and the reason
           substring.
  FIX #4   Demo 4 never verified the review status was recorded as submitted.
           The test now walks the completed list, finds the matching review,
           and asserts its status reflects ``NEEDS_REVISION``.
  FIX #5   Demo 5 accessed ``report.total_tasks`` etc. as bare attributes;
           a missing field raised ``AttributeError`` and killed the script.
           Every field goes through ``_safe_get``, with type coercion and
           sanity-range checks (e.g. ``tasks_escalated <= total_tasks``).
  FIX #6   Demo 6 was entirely fabricated ``print`` statements — no API calls,
           no state, no verification. It now attempts the *actual* workflow
           via the components from Demos 3–5, and when a component is not
           available it prints a clearly labelled "(ILLUSTRATIVE)" narrative
           that makes no claim of execution.
  FIX #7   The final summary printed "✅ Advanced Enhancements" unconditionally
           even when Demo 7 crashed. ``run()`` now tracks each demo's
           ``DemoResult`` and the summary reflects real outcomes.
  FIX #8   ``event.event_id[:8]`` assumed a string of length >= 8. Now
           ``str(_safe_get(event, "event_id", default="?"))[:8]``.
  FIX #9   ``NodeDescriptor(use_evolutionary=...)`` passed a kwarg the real
           class may not accept. ``_try_construct`` filters kwargs through
           ``inspect.signature`` and falls back to a smaller kwarg set.
  FIX #10  ``try/except ImportError`` wrapped whole demo bodies but only
           caught import errors, not runtime failures. Each demo now runs in
           its own try/except at the ``run()`` level, and per-step guards
           within each demo catch and record runtime failures.
  FIX #11  No ``if __name__ == "__main__"`` guard — the file could not be
           imported without executing the whole demo. Added.
  FIX #12  No CLI flags or exit code. Added ``--demo``, ``--skip``,
           ``--json``, ``--quiet``, ``--verbose``, ``--self-test``;
           ``main()`` returns an int and ``__main__`` does ``sys.exit``.
  FIX #13  Windows crashed on emoji without UTF-8 stdout. ``reconfigure``
           at import.
  FIX #14  Imports assumed CWD contains the modules. A ``sys.path`` walker
           ascends from ``__file__`` looking for a package root.
  FIX #15  No offline validation. ``--self-test`` runs helper invariants
           without requiring any of the demo modules.

---- Run ---------------------------------------------------------------------
  python expert_collaboration_demo.py                # all demos
  python expert_collaboration_demo.py --demo 3       # only demo 3
  python expert_collaboration_demo.py --skip 1,7     # skip 1 and 7
  python expert_collaboration_demo.py --json         # machine-readable
  python expert_collaboration_demo.py --self-test    # validate helpers
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
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


# ---------------------------------------------------------------------------
# sys.path walker (FIX #14)
# ---------------------------------------------------------------------------
def _ensure_package_root() -> None:
    here = Path(__file__).resolve().parent
    for candidate in [here, *here.parents]:
        if (candidate / "expert_gateway.py").is_file() or \
           (candidate / "enhancements").is_dir():
            if str(candidate) not in sys.path:
                sys.path.insert(0, str(candidate))
            return


_ensure_package_root()


# ---------------------------------------------------------------------------
# Imports with complete fallbacks
# ---------------------------------------------------------------------------
_IMPORT_ERRORS: List[str] = []

try:
    from expert_gateway import (                        # type: ignore
        create_multi_provider_gateway, ExpertDomain, ExpertModelType,
    )
    GATEWAY_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"expert_gateway: {_exc}")
    GATEWAY_AVAILABLE = False
    create_multi_provider_gateway = None                 # type: ignore
    ExpertDomain = None                                  # type: ignore
    ExpertModelType = None                               # type: ignore

try:
    from expert_connectors import (                     # type: ignore
        ExpertConnectorRegistry, ExpertConnectorType,
    )
    CONNECTORS_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"expert_connectors: {_exc}")
    CONNECTORS_AVAILABLE = False
    ExpertConnectorRegistry = None                       # type: ignore
    ExpertConnectorType = None                           # type: ignore

try:
    from invocation_policy import (                     # type: ignore
        SelectiveInvocationPolicy, EscalationReason,
    )
    POLICY_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"invocation_policy: {_exc}")
    POLICY_AVAILABLE = False
    SelectiveInvocationPolicy = None                     # type: ignore
    EscalationReason = None                              # type: ignore

try:
    from expert_collaboration_system import (           # type: ignore
        HumanReviewPortal, ReviewStatus,
    )
    PORTAL_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"expert_collaboration_system: {_exc}")
    PORTAL_AVAILABLE = False
    HumanReviewPortal = None                             # type: ignore
    ReviewStatus = None                                  # type: ignore

try:
    from audit_logger import AuditLogger, AuditEventType  # type: ignore
    AUDIT_AVAILABLE = True
except ImportError as _exc:                              # noqa: BLE001
    _IMPORT_ERRORS.append(f"audit_logger: {_exc}")
    AUDIT_AVAILABLE = False
    AuditLogger = None                                   # type: ignore
    AuditEventType = None                                # type: ignore

# FIX #9 — every imported name is set to None on failure.
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


def _await_or_call(method: Any, *args: Any, **kwargs: Any) -> Any:
    """Call ``method``; if the result is a coroutine, run it."""
    result = method(*args, **kwargs)
    if inspect.iscoroutine(result):
        return _run_coro(result)
    return result


def _get_enum_member(enum_cls: Any, *names: str, default: Any = None) -> Any:
    """Return the first attribute of ``enum_cls`` matching any of ``names``."""
    if enum_cls is None:
        return default
    for name in names:
        if hasattr(enum_cls, name):
            return getattr(enum_cls, name)
    return default


def _status_str(status: Any) -> str:
    """Best-effort string for a review-status enum / string."""
    if status is None:
        return ""
    if isinstance(status, str):
        return status.upper()
    name = getattr(status, "name", None)
    if isinstance(name, str):
        return name.upper()
    val = getattr(status, "value", None)
    if isinstance(val, str):
        return val.upper()
    return str(status).upper()


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
class ExpertCollaborationDemo:
    def __init__(self, quiet: bool = False, verbose: bool = False):
        self.quiet = quiet
        self.verbose = verbose
        self._gw: Any = None
        self._registry: Any = None
        self._policy: Any = None
        self._portal: Any = None
        self._audit: Any = None

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

    # =====================================================================
    # DEMO 1 — Expert Model Gateway (FIX #2)
    # =====================================================================
    def demo_1(self) -> DemoResult:
        res = DemoResult(1, "Expert Model Gateway")
        self._section("DEMO 1: Expert Model Gateway")

        if not GATEWAY_AVAILABLE:
            res.fail("expert_gateway module unavailable")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        self._say("Constructing multi-provider gateway...")
        try:
            gateway = create_multi_provider_gateway()
        except Exception as exc:                         # noqa: BLE001
            text = str(exc).lower()
            if "api" in text and ("key" in text or "credential" in text):
                res.fail(f"needs API credentials: {exc}")
                self._say("   ⚠️  No API key configured; "
                          "gateway construction could not be verified")
                self._say("     (set OPENAI_API_KEY or ANTHROPIC_API_KEY to enable)")
                return res
            res.fail(f"create_multi_provider_gateway raised: {exc}")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        self._gw = gateway
        self._say(f"   ✓ Gateway constructed: {type(gateway).__name__}")
        res.details["gateway_type"] = type(gateway).__name__

        # Verify documented features exist
        for feature in ("invoke", "invoke_expert", "get_statistics"):
            if hasattr(gateway, feature):
                self._say(f"     - has {feature}()")
                res.details.setdefault("features", []).append(feature)

        self._say("")
        self._say("   Illustrative invocation (no live API call):")
        self._say("     task: Optimize memory allocation")
        self._say("     domain: CODE_GENERATION")
        self._say("     estimated energy: 0.05 Wh")
        return res

    # =====================================================================
    # DEMO 2 — Domain-Specific Connectors
    # =====================================================================
    def demo_2(self) -> DemoResult:
        res = DemoResult(2, "Domain-Specific Connectors")
        self._section("DEMO 2: Domain-Specific Expert Connectors")

        if not CONNECTORS_AVAILABLE:
            res.fail("expert_connectors module unavailable")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        try:
            registry = ExpertConnectorRegistry()
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"ExpertConnectorRegistry() raised: {exc}")
            return res

        self._registry = registry

        try:
            connectors = list(registry.list_connectors())
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"list_connectors raised: {exc}")
            return res

        self._say(f"Registered connectors ({len(connectors)}):")
        for c in connectors:
            self._say(f"  • {getattr(c, 'value', str(c))}")
        res.check(bool(connectors), "registry has no connectors")

        self._say("")
        self._say("   Illustrative outputs:")
        self._say("     Compiler expert → unused variable warning, vectorization hint")
        self._say("     Energy expert   → CPU 2.5 J, RAM 0.8 J, carbon 0.00035 g")
        res.details["connector_count"] = len(connectors)
        return res

    # =====================================================================
    # DEMO 3 — Selective Invocation Policy (FIX #3)
    # =====================================================================
    def demo_3(self) -> DemoResult:
        res = DemoResult(3, "Selective Invocation Policy")
        self._section("DEMO 3: Selective Invocation Policy")

        if not POLICY_AVAILABLE:
            res.fail("invocation_policy module unavailable")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        try:
            policy = SelectiveInvocationPolicy(
                confidence_threshold=0.7,
                sustainability_threshold_wh=0.1,
                enable_criticality_check=True,
            )
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"policy init raised: {exc}")
            return res
        self._policy = policy

        cases: List[Dict[str, Any]] = [
            {
                "label": "Low confidence",
                "kwargs": {
                    "task": "Implement encryption algorithm",
                    "agent_confidence": 0.65,
                    "estimated_energy_wh": 0.05,
                    "domain": "security",
                },
                "expected_escalate": True,
                "expected_reason_contains": "confidence",
            },
            {
                "label": "High energy",
                "kwargs": {
                    "task": "Train deep learning model",
                    "agent_confidence": 0.85,
                    "estimated_energy_wh": 0.15,
                    "domain": "machine_learning",
                },
                "expected_escalate": True,
                "expected_reason_contains": "sustain",
            },
            {
                "label": "Critical safety",
                "kwargs": {
                    "task": "Fix memory segfault in safety-critical system",
                    "agent_confidence": 0.80,
                    "estimated_energy_wh": 0.05,
                    "domain": "safety",
                },
                "expected_escalate": True,
                "expected_reason_contains": "critical",
            },
        ]

        for case in cases:
            try:
                decision = policy.decide_escalation(**case["kwargs"])
            except Exception as exc:                     # noqa: BLE001
                res.fail(f"{case['label']}: decide_escalation raised: {exc}")
                continue

            should = _safe_get(decision, "should_escalate")
            reasons_raw = _safe_get(decision, "reasons", default=[]) or []
            reasons = [
                getattr(r, "value", str(r)) for r in reasons_raw
            ]
            reasons_lower = " ".join(reasons).lower()

            self._say(f"\n   {case['label']}")
            self._say(f"     task: {case['kwargs']['task']}")
            self._say(f"     confidence: {case['kwargs']['agent_confidence']}")
            self._say(f"     energy: {case['kwargs']['estimated_energy_wh']} Wh")
            self._say(f"     should_escalate: {should}")
            self._say(f"     reasons: {reasons}")

            res.check(
                should == case["expected_escalate"],
                f"{case['label']}: should_escalate={should!r} != "
                f"expected {case['expected_escalate']!r}",
            )
            if case["expected_reason_contains"]:
                res.check(
                    case["expected_reason_contains"] in reasons_lower,
                    f"{case['label']}: reasons {reasons!r} missing "
                    f"{case['expected_reason_contains']!r}",
                )

        # Stats
        try:
            stats = policy.get_policy_stats()
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"get_policy_stats raised: {exc}")
            stats = None
        if isinstance(stats, dict):
            total = int(_safe_get(stats, "total_decisions", default=0))
            esc = int(_safe_get(stats, "escalations", default=0))
            rate = float(_safe_get(stats, "escalation_rate", default=0.0))
            self._say(f"\n   Policy stats: total={total} escalations={esc} "
                      f"rate={rate:.1%}")
            res.check(total >= len(cases),
                      f"total_decisions={total} < cases={len(cases)}")
            res.check(esc <= total,
                      f"escalations={esc} > total={total}")
            res.details.update(total=total, escalations=esc, rate=rate)
        return res

    # =====================================================================
    # DEMO 4 — Human Review Portal (FIX #4)
    # =====================================================================
    def demo_4(self) -> DemoResult:
        res = DemoResult(4, "Human Review Portal")
        self._section("DEMO 4: Human Review Portal")

        if not PORTAL_AVAILABLE:
            res.fail("expert_collaboration_system module unavailable")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        try:
            portal = HumanReviewPortal()
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"HumanReviewPortal() raised: {exc}")
            return res
        self._portal = portal

        try:
            request_id = portal.submit_for_review(
                task_id="critical_001",
                agent_output="Proposed safety fix: disable_checks()",
                escalation_reasons=["critical_safety", "low_confidence"],
                criticality_level="critical",
            )
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"submit_for_review raised: {exc}")
            return res
        self._say(f"   ✓ Submitted: request_id={request_id}")

        try:
            portal.assign_reviewer(request_id, "reviewer_alice")
            self._say("   ✓ Assigned to reviewer_alice")
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"assign_reviewer raised: {exc}")

        try:
            portal.submit_review(
                request_id=request_id,
                reviewer_id="reviewer_alice",
                status=ReviewStatus.NEEDS_REVISION,
                notes="Safety fix is inadequate. Recommend formal verification.",
            )
            self._say("   ✓ Review submitted: NEEDS_REVISION")
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"submit_review raised: {exc}")
            return res

        # FIX #4 — verify the review is recorded with the submitted status.
        completed: Any = None
        for attr in ("completed_reviews",):
            if hasattr(portal, attr):
                completed = getattr(portal, attr)
                break
        if completed is None and hasattr(portal, "get_completed_reviews"):
            try:
                completed = portal.get_completed_reviews()
            except Exception as exc:                     # noqa: BLE001
                res.fail(f"get_completed_reviews raised: {exc}")

        matching = None
        if isinstance(completed, dict):
            matching = completed.get(request_id)
        elif isinstance(completed, (list, tuple)):
            for r in completed:
                if _safe_get(r, "request_id") == request_id:
                    matching = r
                    break
        if completed is not None:
            res.check(matching is not None,
                      f"review {request_id!r} not found in completed list")
        if matching is not None:
            status = _status_str(_safe_get(matching, "status"))
            self._say(f"   Recorded status: {status}")
            res.check(
                "REVISION" in status or "NEEDS_REVISION" in status,
                f"recorded status {status!r} != NEEDS_REVISION",
            )
            res.details["status"] = status
        return res

    # =====================================================================
    # DEMO 5 — Audit Logger & Transparency Report (FIX #5)
    # =====================================================================
    def demo_5(self) -> DemoResult:
        res = DemoResult(5, "Audit Logger & Transparency")
        self._section("DEMO 5: Audit Logger & Transparency Reports")

        if not AUDIT_AVAILABLE:
            res.fail("audit_logger module unavailable")
            self._say(f"   ✗ {res.errors[-1]}", error=True)
            return res

        try:
            audit = AuditLogger(enable_persistence=False)
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"AuditLogger() raised: {exc}")
            return res
        self._audit = audit

        try:
            audit.log_expert_invocation(
                task_id="task_001", agent_id="agent_alpha",
                expert_type="gpt-4",
                energy_wh=0.05, carbon_kg=0.00002,
            )
            self._say("   ✓ Logged expert invocation (gpt-4, 0.05 Wh)")
            audit.log_escalation(
                task_id="task_002", agent_id="agent_beta",
                reasons=["low_confidence", "critical_security"],
                expert_type="security_expert",
            )
            self._say("   ✓ Logged escalation (security)")
            audit.log_energy_saved(
                task_id="task_003", agent_id="agent_gamma",
                energy_saved_wh=0.03, mechanism="expert_optimization",
            )
            self._say("   ✓ Logged energy saved (0.03 Wh)")
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"audit logging raised: {exc}")
            return res

        try:
            report = audit.generate_transparency_report()
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"generate_transparency_report raised: {exc}")
            return res

        # FIX #5 — safe field access with type coercion.
        fields = {
            "total_tasks": int,
            "tasks_escalated": int,
            "escalation_rate": float,
            "expert_invocations": int,
            "feedback_received": int,
            "feedback_integrated": int,
            "feedback_integration_rate": float,
            "total_energy_consumed_wh": float,
            "energy_saved_wh": float,
            "net_energy_wh": float,
            "carbon_saved_kg": float,
            "sustainability_improvement_pct": float,
            "transparency_score": float,
        }
        values: Dict[str, Any] = {}
        for name, typ in fields.items():
            raw = _safe_get(report, name)
            if raw is None:
                res.fail(f"report missing {name}")
                continue
            try:
                values[name] = typ(raw)
            except (TypeError, ValueError) as exc:
                res.fail(f"report.{name}={raw!r} not convertible to {typ.__name__}: {exc}")

        self._say("\n   Transparency report:")
        for name, value in values.items():
            self._say(f"     {name}: {value}")

        # Sanity-range checks
        if "tasks_escalated" in values and "total_tasks" in values:
            res.check(
                values["tasks_escalated"] <= values["total_tasks"],
                f"tasks_escalated={values['tasks_escalated']} > "
                f"total_tasks={values['total_tasks']}",
            )
        if "transparency_score" in values:
            res.check(
                0.0 <= values["transparency_score"] <= 1.0,
                f"transparency_score={values['transparency_score']} outside [0,1]",
            )
        res.details.update(values)
        return res

    # =====================================================================
    # DEMO 6 — End-to-End Workflow (FIX #6)
    # =====================================================================
    def demo_6(self) -> DemoResult:
        res = DemoResult(6, "End-to-End Workflow")
        self._section("DEMO 6: Complete Expert Collaboration System")

        critical_ready = POLICY_AVAILABLE and PORTAL_AVAILABLE and AUDIT_AVAILABLE
        if not critical_ready:
            missing = []
            if not POLICY_AVAILABLE: missing.append("invocation_policy")
            if not PORTAL_AVAILABLE: missing.append("expert_collaboration_system")
            if not AUDIT_AVAILABLE: missing.append("audit_logger")
            self._say("   (ILLUSTRATIVE — the narrative below describes the")
            self._say("    intended workflow; no calls are executed.)")
            self._say(f"   Missing modules: {', '.join(missing)}")
            self._say("")
            for line in (
                "Step 1: agent analyzes code — confidence 0.65 (LOW)",
                "Step 2: policy ESCALATES (confidence < threshold)",
                "Step 3: gateway dispatches to an expert model",
                "Step 4: expert returns a vulnerability analysis",
                "Step 5: feedback integrated into the agent's output",
                "Step 6: audit log records the invocation",
                "Step 7: transparency report attributes energy",
            ):
                self._say(f"   · {line}")
            res.fail(f"illustrative only — missing: {', '.join(missing)}")
            return res

        self._say("   Running end-to-end workflow with actual components...")

        # Step 1 — policy escalation on low confidence + security domain
        try:
            policy = SelectiveInvocationPolicy(
                confidence_threshold=0.7,
                sustainability_threshold_wh=0.1,
                enable_criticality_check=True,
            )
            decision = policy.decide_escalation(
                task="Review security-critical code",
                agent_confidence=0.65,
                estimated_energy_wh=0.05,
                domain="security",
            )
            should = _safe_get(decision, "should_escalate")
            self._say(f"   ✓ Step 1 (policy): should_escalate={should}")
            res.check(should is True,
                      "e2e: policy did not escalate low-confidence security task")
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"e2e policy step raised: {exc}")
            return res

        # Step 2 — portal records the escalation
        request_id = None
        try:
            portal = HumanReviewPortal()
            request_id = portal.submit_for_review(
                task_id="e2e_001",
                agent_output="…",
                escalation_reasons=["low_confidence", "critical_security"],
                criticality_level="critical",
            )
            self._say(f"   ✓ Step 2 (portal): request_id={request_id}")
            res.check(request_id is not None,
                      "e2e: portal returned no request_id")
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"e2e portal step raised: {exc}")

        # Step 3 — audit records both events
        try:
            audit = AuditLogger(enable_persistence=False)
            audit.log_expert_invocation(
                task_id="e2e_001", agent_id="agent_alpha",
                expert_type="security_expert",
                energy_wh=0.05, carbon_kg=0.00002,
            )
            audit.log_escalation(
                task_id="e2e_001", agent_id="agent_alpha",
                reasons=["low_confidence", "critical_security"],
                expert_type="security_expert",
            )
            report = audit.generate_transparency_report()
            total = int(_safe_get(report, "total_tasks", default=0))
            self._say(f"   ✓ Step 3 (audit): total_tasks={total}")
            res.check(total >= 1, "e2e: audit report shows 0 tasks")
            res.details["total_tasks"] = total
        except Exception as exc:                         # noqa: BLE001
            res.fail(f"e2e audit step raised: {exc}")

        res.details["request_id"] = request_id
        return res

    # =====================================================================
    # DEMO 7 — Advanced Enhancements (FIX #1, #8, #9)
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
                "id": "expert_node",
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
                "id": "expert_node",
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

        # 2. Select routing strategy
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
                "task_id": "expert_task",
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
                "task_id": "expert_task",
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
                "source": "expert_demo",
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
                "tags": ["expert_demo"],
            },
            "FeedbackEvent",
            fallback_kwargs={
                "source": "expert_demo",
                "feedback_type": "routing",
                "task_id": "demo_enhanced",
            },
            say=self._say,
        )
        if event is None:
            res.fail("FeedbackEvent could not be constructed")
        else:
            # FIX #8 — never assume event_id is a short string.
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
    # Orchestrator (FIX #7)
    # =====================================================================
    def run(self, skip: Optional[List[int]] = None) -> List[DemoResult]:
        skip = skip or []
        self._say("=" * 80)
        self._say("Expert Collaboration System — Complete Demo (Enhanced v2)")
        self._say("=" * 80)
        self._say(f"Modules: gateway={GATEWAY_AVAILABLE} connectors="
                  f"{CONNECTORS_AVAILABLE} policy={POLICY_AVAILABLE} "
                  f"portal={PORTAL_AVAILABLE} audit={AUDIT_AVAILABLE} "
                  f"enhancements={ENHANCEMENTS_AVAILABLE}")

        demos: List[Tuple[int, str, Callable[[], DemoResult]]] = [
            (1, "Expert Model Gateway", self.demo_1),
            (2, "Domain-Specific Connectors", self.demo_2),
            (3, "Selective Invocation Policy", self.demo_3),
            (4, "Human Review Portal", self.demo_4),
            (5, "Audit Logger & Transparency", self.demo_5),
            (6, "End-to-End Workflow", self.demo_6),
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
                self._say(f"   ✗ Demo {number} failures: {r.errors}",
                          error=True)

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
# Self-test (FIX #15)
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

    _check(
        "filters unknown kwargs",
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

    print("_await_or_call:")
    class _Sync:
        def call(self, x):
            return x + 1

    class _Async:
        async def call(self, x):
            return x + 2

    _check("sync method",
           _await_or_call(_Sync().call, 10) == 11)
    _check("async method",
           _await_or_call(_Async().call, 10) == 12)

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

    print("_status_str:")
    _check("string input", _status_str("needs_revision") == "NEEDS_REVISION")

    class _EnumLike:
        name = "NEEDS_REVISION"

    _check("enum name", _status_str(_EnumLike()) == "NEEDS_REVISION")
    _check("none", _status_str(None) == "")

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

    say_lines.clear()
    out = _try_construct(
        _NeedsAll, {"a": -1, "b": -2}, "NeedsAll",
        fallback_kwargs={"a": -3, "b": -4},
        say=lambda m: say_lines.append(m),
    )
    _check("returns None when both fail", out is None)

    print("DemoResult:")
    r = DemoResult(3, "Test")
    _check("starts ok", r.ok is True)
    r.check(True, "should pass")
    _check("passing check keeps ok", r.ok is True)
    r.check(False, "should fail")
    _check("failing check flips ok", r.ok is False)
    r.fail("explicit failure")
    _check("fail() appends error",
           len(r.errors) == 2 and "explicit failure" in r.errors)

    print("")
    if failures:
        print(f"{failures} failure(s)")
        return 1
    print("all self-tests passed")
    return 0


# ---------------------------------------------------------------------------
# CLI (FIX #12)
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Expert Collaboration System — Complete Demo (Enhanced v2)",
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

    demo = ExpertCollaborationDemo(
        quiet=args.quiet, verbose=args.verbose,
    )
    results = demo.run(skip=skip)
    failures = sum(1 for r in results if not r.ok)
    exit_code = 0 if failures == 0 else 1

    if args.json:
        print(json.dumps(
            {
                "version": "5.0.0",
                "modules": {
                    "gateway": GATEWAY_AVAILABLE,
                    "connectors": CONNECTORS_AVAILABLE,
                    "policy": POLICY_AVAILABLE,
                    "portal": PORTAL_AVAILABLE,
                    "audit": AUDIT_AVAILABLE,
                    "enhancements": ENHANCEMENTS_AVAILABLE,
                },
                "import_errors": _IMPORT_ERRORS,
                "demos": [r.to_dict() for r in results],
                "failures": failures,
            },
            indent=2,
            default=str,
        ))

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
