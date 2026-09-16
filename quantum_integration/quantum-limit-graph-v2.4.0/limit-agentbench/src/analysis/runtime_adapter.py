# src/analysis/adapters/runtime_adapter.py

"""
Runtime adapter for Green Agent (Enhanced)
==========================================

Defines the abstract contract that all agent-runtime adapters (LangChain,
AutoGen, custom) must satisfy so the analysis layer can consume them
uniformly.

Original API preserved:
    class MyRuntime(AgentRuntime):
        def init(self, config): ...
        def run(self, query): ...
        # finalize is optional
    runtime = MyRuntime()

Enhanced API:
    class MyRuntime(AgentRuntime):
        # init, run, finalize as before
        def reduce_tool_calls(self): ...        # optional, override
        def shorten_context(self): ...          # optional, override
        def capabilities(self) -> RuntimeCapabilities: ...   # optional
        def records(self): ...
        def observations(self): ...

    # Parallel async interface
    class MyAsyncRuntime(AsyncAgentRuntime):
        async def run(self, query): ...

    # Contract verification for tests
    verify_contract(MyRuntime)

Enhancements:
  1. Quantum-Distillation      — capabilities declare route/precision support
  2. Causal RL                 — RuntimeResult carries run_id for attribution
  3. Federated Analytics       — deployment_id in capabilities
  4. Multi-Agent Coordination  — observations() hook declared
  5. Temporal Logic            — INTERFACE_VERSION for contract evolution
  6. Explainable AI            — RuntimeResult carries explanation slot
  7. Adaptive Precision        — reduce_tool_calls / shorten_context declared
  8. Carbon Markets            — RuntimeResult carries carbon slot
  9. Resilience & Chaos        — RuntimeError for the error contract
 10. Human-in-the-Loop         — HITL hooks documented
 +   RuntimeCapabilities + RuntimeResult
 +   verify_contract() helper
 +   AsyncAgentRuntime parallel ABC
 +   Lifecycle documentation on the ABC
"""

from __future__ import annotations

import abc
import asyncio
import logging
import math
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import (
    Any, AsyncIterator, Awaitable, Callable, Dict, Iterator, List,
    Optional, Protocol, runtime_checkable,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Interface version
# =============================================================================

INTERFACE_VERSION: str = "5.0.0"
"""Semantic version of the AgentRuntime contract.

Implementations may declare compatibility via `capabilities().interface_version`.
Consumers can verify with `verify_contract(cls, required_version="5.0.0")`.
"""


# =============================================================================
# Enums
# =============================================================================

class RuntimeKind(Enum):
    """The framework family a runtime belongs to."""
    LANGCHAIN = "langchain"
    AUTOGEN = "autogen"
    CREWAI = "crewai"
    CUSTOM = "custom"
    UNKNOWN = "unknown"


class RunStatus(Enum):
    """Outcome of a single run."""
    SUCCESS = "success"
    PARTIAL = "partial"       # completed but with warnings
    FAILED = "failed"
    TIMEOUT = "timeout"
    INVALID = "invalid"       # input rejected before execution


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


# =============================================================================
# Error contract
# =============================================================================

class RuntimeError_(Exception):
    """
    Base exception for runtime adapter failures.

    Named with a trailing underscore to avoid shadowing the builtin
    `RuntimeError`.

    Implementations may raise subclasses to signal specific failure modes.
    Callers should catch `RuntimeError_` to handle any adapter failure
    uniformly.
    """
    def __init__(
        self,
        message: str,
        *,
        run_id: Optional[str] = None,
        cause: Optional[BaseException] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.run_id = run_id
        self.cause = cause
        self.details = details or {}


class NotInitializedError(RuntimeError_):
    """Raised when `run()` is called before `init()`."""


class AlreadyInitializedError(RuntimeError_):
    """Raised when `init()` is called twice without `finalize()` between."""


class AlreadyFinalizedError(RuntimeError_):
    """Raised when `run()` is called after `finalize()`."""


class InvalidConfigError(RuntimeError_):
    """Raised when `init(config)` receives an invalid config."""


class InvalidQueryError(RuntimeError_):
    """Raised when `run(query)` receives an invalid query."""


# =============================================================================
# RuntimeCapabilities — introspection
# =============================================================================

@dataclass
class RuntimeCapabilities:
    """
    Declares what a runtime adapter supports.

    Consumers (AdaptiveController, HITL gates, analyzers) can query this
    to decide which calls are safe.
    """
    interface_version: str = INTERFACE_VERSION
    runtime_kind: str = RuntimeKind.UNKNOWN.value

    # --- Lifecycle ---
    supports_async: bool = False
    supports_reinit: bool = False

    # --- Eco-mode hooks (AdaptiveController) ---
    supports_reduce_tool_calls: bool = True
    supports_shorten_context: bool = False

    # --- Analysis contracts ---
    supports_decision_records: bool = False
    supports_agent_observations: bool = False
    supports_step_records: bool = False

    # --- Precision ---
    supports_precision_control: bool = False

    # --- Callbacks ---
    supports_callback_injection: bool = False

    # --- Failure semantics ---
    raises_on_failure: bool = True
    returns_error_dict: bool = False

    # --- Extra metadata ---
    deployment_id: Optional[str] = None
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# RuntimeResult — the enriched return type
# =============================================================================

@dataclass
class RuntimeResult:
    """
    Structured result of a `run()` call.

    The original `AgentRuntime.run()` contract returns a plain dict with
    `accuracy`, `tool_calls`, and `conversation_depth`. This dataclass is
    a superset — a runtime may return either the raw dict or a
    `RuntimeResult`. Consumers can call `RuntimeResult.from_dict(...)` on
    the former.

    `to_dict()` returns a dict containing all original keys, so callers
    that expect the original shape continue to work.
    """
    # --- Original contract keys ---
    accuracy: float = 0.0
    tool_calls: int = 0
    conversation_depth: int = 0

    # --- Extended metrics ---
    latency_s: float = 0.0
    energy_wh: float = 0.0
    carbon_g: float = 0.0
    helium_units: float = 0.0
    llm_calls: int = 0
    tokens: int = 0

    # --- Context ---
    precision: Optional[str] = None
    region: Optional[str] = None

    # --- Provenance ---
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    agent_id: Optional[str] = None
    deployment_id: Optional[str] = None

    # --- Status ---
    status: str = RunStatus.SUCCESS.value
    error: Optional[str] = None

    # --- Trust ---
    simulated: bool = False

    # --- Explanation (XAI) ---
    explanation: Optional[Dict[str, Any]] = None

    # --- Metadata ---
    at: datetime = field(default_factory=datetime.now)
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Return a dict with the original contract keys first."""
        out = {
            "accuracy": self.accuracy,
            "tool_calls": self.tool_calls,
            "conversation_depth": self.conversation_depth,
        }
        out.update({
            "latency_s": self.latency_s,
            "energy_wh": self.energy_wh,
            "carbon_g": self.carbon_g,
            "helium_units": self.helium_units,
            "llm_calls": self.llm_calls,
            "tokens": self.tokens,
            "precision": self.precision,
            "region": self.region,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "deployment_id": self.deployment_id,
            "status": self.status,
            "error": self.error,
            "simulated": self.simulated,
            "explanation": self.explanation,
            "at": self.at.isoformat(),
        })
        out.update(self.extra or {})
        return out

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RuntimeResult":
        """Construct from a raw dict, ignoring unknown keys."""
        if not isinstance(data, dict):
            return cls()
        known = {f for f in cls.__dataclass_fields__}
        kwargs = {k: v for k, v in data.items() if k in known}
        extra = {k: v for k, v in data.items() if k not in known}
        if "at" in kwargs and isinstance(kwargs["at"], str):
            try:
                kwargs["at"] = datetime.fromisoformat(kwargs["at"])
            except ValueError:
                pass
        if extra:
            kwargs.setdefault("extra", {}).update(extra)
        return cls(**kwargs)

    def is_success(self) -> bool:
        return self.status == RunStatus.SUCCESS.value

    def is_valid(self) -> bool:
        """Sanity check on the numeric fields."""
        for name, value in (
            ("accuracy", self.accuracy),
            ("tool_calls", self.tool_calls),
            ("conversation_depth", self.conversation_depth),
        ):
            if isinstance(value, bool):
                return False
            try:
                f = float(value)
            except (TypeError, ValueError):
                return False
            if not math.isfinite(f):
                return False
        return True


# =============================================================================
# Runtime-level Protocol (structural typing)
# =============================================================================

@runtime_checkable
class RuntimeProtocol(Protocol):
    """
    Structural typing protocol — parallel to AgentRuntime.

    Adapters that cannot inherit from AgentRuntime (e.g., wrapping an
    existing third-party class) can still satisfy this protocol by
    implementing the same methods. `isinstance(runtime, RuntimeProtocol)`
    returns True if the required methods are present.
    """
    def init(self, config: Dict[str, Any]) -> None: ...
    def run(self, query: Dict[str, Any]) -> Dict[str, Any]: ...
    def finalize(self) -> None: ...


# =============================================================================
# ORIGINAL AgentRuntime — preserved exactly
# =============================================================================

class AgentRuntime(abc.ABC):
    """
    Abstract base class for synchronous agent runtimes.

    Lifecycle
    ---------
    1. `init(config)` — called exactly once to configure the runtime.
    2. `run(query)` — called zero or more times to execute turns.
    3. `finalize()` — called exactly once to release resources.

    Contract
    --------
    `run(query)` must return a dict containing:
        - accuracy: float
        - tool_calls: int
        - conversation_depth: int

    Implementations MAY return a `RuntimeResult` instead (which has a
    `to_dict()` producing the same keys), or a superset dict.

    Failure semantics
    -----------------
    Implementations SHOULD raise `RuntimeError_` (or a subclass) on
    unrecoverable failure. They SHOULD NOT raise bare `Exception` types,
    as callers expect to catch `RuntimeError_` uniformly.

    Enhanced hooks (all optional, defaults are no-ops)
    --------------------------------------------------
    `reduce_tool_calls()` — called by `AdaptiveController` in low_energy
                            mode. Default: no-op.
    `shorten_context()`   — called by `AdaptiveController` in low_memory
                            mode. Default: no-op.

    Introspection (optional, default in this ABC)
    ---------------------------------------------
    `capabilities()` — returns a `RuntimeCapabilities`. Default reports
                       what the ABC guarantees.

    Analysis contracts (optional, override to enable)
    -------------------------------------------------
    `records()`       — return `DecisionRecord`s emitted during the session.
    `observations()`  — return `AgentObservation`s for multi-agent analysis.
    `steps()`         — return `StepRecord`s for execution tracing.
    """

    # --- Class-level metadata ---
    interface_version: str = INTERFACE_VERSION

    # ------------------------------------------------------------------
    # Original abstract methods
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def init(self, config: Dict[str, Any]) -> None:
        """Configure the runtime. Called once before any `run()`."""
        ...

    @abc.abstractmethod
    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single agent turn.

        Must return a dict containing at least:
            accuracy: float
            tool_calls: int
            conversation_depth: int

        Implementations SHOULD raise `RuntimeError_` (or a subclass) on
        unrecoverable failure.
        """
        ...

    def finalize(self) -> None:
        """
        Release resources. Called exactly once at end of session.

        The default implementation is a no-op. Override to clean up
        threads, connections, or subprocesses.
        """
        pass

    # ------------------------------------------------------------------
    # Enhanced hooks — optional, defaults preserve original behavior
    # ------------------------------------------------------------------

    def reduce_tool_calls(self) -> None:
        """
        Called by `AdaptiveController` in low_energy mode.

        Default: no-op. Override to actually reduce tool calls in
        subsequent `run()` invocations.
        """
        pass

    def shorten_context(self) -> None:
        """
        Called by `AdaptiveController` in low_memory mode.

        Default: no-op. Override to actually shorten context windows in
        subsequent `run()` invocations.
        """
        pass

    def capabilities(self) -> RuntimeCapabilities:
        """
        Return this runtime's capabilities.

        The default implementation reports only what the ABC guarantees.
        Override to declare additional support (async, callbacks,
        DecisionRecords, etc.).
        """
        return RuntimeCapabilities(
            interface_version=self.interface_version,
            runtime_kind=RuntimeKind.UNKNOWN.value,
            supports_async=False,
            supports_reinit=False,
            supports_reduce_tool_calls=(
                type(self).reduce_tool_calls is not AgentRuntime.reduce_tool_calls
            ),
            supports_shorten_context=(
                type(self).shorten_context is not AgentRuntime.shorten_context
            ),
            supports_decision_records=hasattr(self, "records"),
            supports_agent_observations=hasattr(self, "observations"),
            supports_step_records=hasattr(self, "steps"),
        )

    # ------------------------------------------------------------------
    # Analysis contract accessors — optional, defaults return empty
    # ------------------------------------------------------------------

    def records(self) -> List[Any]:
        """Return DecisionRecords emitted this session. Default: []."""
        return []

    def observations(self) -> List[Any]:
        """Return AgentObservations for multi-agent analysis. Default: []."""
        return []

    def steps(self) -> List[Any]:
        """Return StepRecords for execution tracing. Default: []."""
        return []

    def set_callback(self, callback: Any) -> None:
        """
        Wire in a framework callback (e.g., GreenLangChainCallback).

        Default: no-op. Override to accept real callbacks.
        """
        logger.debug(
            f"{type(self).__name__}.set_callback called but not "
            "implemented; no-op"
        )

    # ------------------------------------------------------------------
    # Convenience accessors (non-abstract)
    # ------------------------------------------------------------------

    def run_as_result(self, query: Dict[str, Any]) -> RuntimeResult:
        """
        Run and normalize the result to a `RuntimeResult`.

        Accepts either a dict or a `RuntimeResult` from `run()`.
        """
        raw = self.run(query)
        if isinstance(raw, RuntimeResult):
            return raw
        return RuntimeResult.from_dict(raw or {})


# =============================================================================
# AsyncAgentRuntime — parallel ABC for async runtimes
# =============================================================================

class AsyncAgentRuntime(abc.ABC):
    """
    Abstract base class for asynchronous agent runtimes.

    Same contract as `AgentRuntime`, but `run()` is a coroutine.
    """

    interface_version: str = INTERFACE_VERSION

    @abc.abstractmethod
    async def init(self, config: Dict[str, Any]) -> None:
        ...

    @abc.abstractmethod
    async def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        ...

    async def finalize(self) -> None:
        pass

    async def reduce_tool_calls(self) -> None:
        pass

    async def shorten_context(self) -> None:
        pass

    def capabilities(self) -> RuntimeCapabilities:
        caps = RuntimeCapabilities(
            interface_version=self.interface_version,
            runtime_kind=RuntimeKind.UNKNOWN.value,
            supports_async=True,
        )
        return caps

    def records(self) -> List[Any]:
        return []

    def observations(self) -> List[Any]:
        return []

    def steps(self) -> List[Any]:
        return []

    def set_callback(self, callback: Any) -> None:
        pass

    async def run_as_result(self, query: Dict[str, Any]) -> RuntimeResult:
        raw = await self.run(query)
        if isinstance(raw, RuntimeResult):
            return raw
        return RuntimeResult.from_dict(raw or {})


# =============================================================================
# Contract verification helper
# =============================================================================

@dataclass
class ContractReport:
    """Result of a `verify_contract()` check."""
    class_name: str
    ok: bool
    issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def verify_contract(
    cls: Any,
    *,
    required_version: Optional[str] = None,
    require_async: bool = False,
) -> ContractReport:
    """
    Verify that `cls` satisfies the `AgentRuntime` contract.

    Useful in tests. Returns a `ContractReport` describing missing or
    non-compliant members.

    Checked items:
      - Subclass of `AgentRuntime` (or `AsyncAgentRuntime`)
      - Has `init` and `run` methods
      - Has `finalize` method (may be inherited)
      - Has `reduce_tool_calls` and `shorten_context` methods
      - Has `capabilities` method
      - Declared `interface_version` is >= `required_version`
    """
    report = ContractReport(class_name=getattr(cls, "__name__", str(cls)), ok=True)

    # --- Is it the right kind? ---
    is_sync = isinstance(cls, type) and issubclass(cls, AgentRuntime)
    is_async = isinstance(cls, type) and issubclass(cls, AsyncAgentRuntime)
    if not (is_sync or is_async):
        report.ok = False
        report.issues.append(
            "not a subclass of AgentRuntime or AsyncAgentRuntime"
        )
        return report

    if require_async and not is_async:
        report.ok = False
        report.issues.append(
            "require_async=True but class is not AsyncAgentRuntime"
        )

    # --- Required methods ---
    required_methods = ("init", "run", "finalize")
    for name in required_methods:
        if not hasattr(cls, name) or not callable(getattr(cls, name, None)):
            report.ok = False
            report.issues.append(f"missing method: {name}")

    # --- Enhanced hooks (warn if missing, not error) ---
    for name in ("reduce_tool_calls", "shorten_context", "capabilities"):
        if not hasattr(cls, name) or not callable(getattr(cls, name, None)):
            report.warnings.append(
                f"optional method missing: {name} "
                "(consumers may rely on it)"
            )

    # --- Interface version ---
    cls_version = getattr(cls, "interface_version", None)
    if cls_version is None:
        report.warnings.append("no interface_version declared")
    elif required_version is not None:
        if not _version_gte(cls_version, required_version):
            report.ok = False
            report.issues.append(
                f"interface_version '{cls_version}' < "
                f"required '{required_version}'"
            )

    return report


def _version_gte(a: str, b: str) -> bool:
    """Return True if semver `a` >= `b` (best-effort)."""
    def parse(s: str) -> tuple:
        parts = []
        for x in s.split("."):
            try:
                parts.append(int(x))
            except ValueError:
                parts.append(0)
        while len(parts) < 3:
            parts.append(0)
        return tuple(parts[:3])
    try:
        return parse(a) >= parse(b)
    except Exception:
        return True


# =============================================================================
# Demo — reference implementations + contract verification
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original usage: subclass that satisfies only the original ABC ---
    class MinimalRuntime(AgentRuntime):
        """A runtime that only implements the original contract."""
        def init(self, config):
            self.config = config
        def run(self, query):
            return {
                "accuracy": 0.9,
                "tool_calls": 1,
                "conversation_depth": 1,
            }
        # finalize() inherited — no-op

    r = MinimalRuntime()
    r.init({"policy_version": "v5.0.1"})
    out = r.run({"task_id": "t1"})
    print("=== Original usage ===")
    print(f"  run(): {out}")
    print(f"  finalize(): {r.finalize()}")

    # --- Enhanced usage: runtime with all the hooks ---
    class FullRuntime(AgentRuntime):
        """A runtime that implements every optional hook."""
        interface_version = "5.0.0"

        def __init__(self):
            self._initialized = False
            self._finalized = False
            self._max_tools = 10
            self._tool_reduction = 0
            self._context_factor = 1.0
            self._records = []
            self._observations = []
            self._callback = None

        def init(self, config):
            if self._initialized:
                raise AlreadyInitializedError("already initialized")
            self._initialized = True
            self._config = config

        def run(self, query):
            if not self._initialized:
                raise NotInitializedError("call init() first")
            if self._finalized:
                raise AlreadyFinalizedError("runtime was finalized")
            effective_tools = max(0, self._max_tools - self._tool_reduction)
            result = RuntimeResult(
                accuracy=0.92,
                tool_calls=effective_tools,
                conversation_depth=2,
                latency_s=1.5,
                energy_wh=0.05,
                carbon_g=0.02,
                precision=self._config.get("precision"),
                run_id=self._config.get("run_id"),
                task_id=query.get("task_id"),
                status=RunStatus.SUCCESS.value,
                explanation={"context_factor": self._context_factor},
            )
            return result.to_dict()

        def finalize(self):
            self._finalized = True

        def reduce_tool_calls(self):
            self._tool_reduction += 1
            logger.info(f"  reduce_tool_calls → reduction={self._tool_reduction}")

        def shorten_context(self):
            self._context_factor = max(0.125, self._context_factor * 0.5)
            logger.info(f"  shorten_context → factor={self._context_factor}")

        def capabilities(self):
            return RuntimeCapabilities(
                interface_version=self.interface_version,
                runtime_kind=RuntimeKind.CUSTOM.value,
                supports_async=False,
                supports_reduce_tool_calls=True,
                supports_shorten_context=True,
                supports_decision_records=False,
                supports_agent_observations=False,
                supports_precision_control=True,
                supports_callback_injection=True,
            )

        def records(self):
            return list(self._records)

        def observations(self):
            return list(self._observations)

        def set_callback(self, cb):
            self._callback = cb

    fr = FullRuntime()
    fr.init({"run_id": "run-001", "precision": "int8"})
    print("\n=== Enhanced usage ===")
    print(f"  run(): {fr.run({'task_id': 't1'})}")
    fr.reduce_tool_calls()
    fr.reduce_tool_calls()
    fr.shorten_context()
    print(f"  run() after throttle: {fr.run({'task_id': 't1'})}")
    caps = fr.capabilities()
    print(f"  capabilities: {caps.to_dict()}")
    fr.finalize()

    # --- Contract verification ---
    print("\n=== Contract verification ===")
    report = verify_contract(MinimalRuntime)
    print(f"  MinimalRuntime: ok={report.ok}")
    for issue in report.issues:
        print(f"    issue: {issue}")
    for w in report.warnings:
        print(f"    warning: {w}")

    report2 = verify_contract(FullRuntime, required_version="5.0.0")
    print(f"  FullRuntime: ok={report2.ok}")
    for issue in report2.issues:
        print(f"    issue: {issue}")
    for w in report2.warnings:
        print(f"    warning: {w}")

    # --- Protocol-based structural typing ---
    print("\n=== Protocol-based structural typing ===")

    class PlainClass:
        """Not an AgentRuntime, but structurally compatible."""
        def init(self, config): pass
        def run(self, query):
            return {"accuracy": 1.0, "tool_calls": 0, "conversation_depth": 0}
        def finalize(self): pass

    plain = PlainClass()
    print(f"  isinstance(plain, RuntimeProtocol): "
          f"{isinstance(plain, RuntimeProtocol)}")
    print(f"  isinstance(fr, RuntimeProtocol):    "
          f"{isinstance(fr, RuntimeProtocol)}")

    # --- Error contract ---
    print("\n=== Error contract ===")
    try:
        FullRuntime().run({})  # never initialized
    except NotInitializedError as e:
        print(f"  NotInitializedError: {e}")

    # --- Async runtime ---
    print("\n=== Async runtime ===")

    class SimpleAsyncRuntime(AsyncAgentRuntime):
        def __init__(self):
            self._initialized = False
            self._tool_reduction = 0

        async def init(self, config):
            self._config = config
            self._initialized = True

        async def run(self, query):
            await asyncio.sleep(0)
            return RuntimeResult(
                accuracy=0.88,
                tool_calls=max(0, 5 - self._tool_reduction),
                conversation_depth=1,
                status=RunStatus.SUCCESS.value,
            ).to_dict()

        async def reduce_tool_calls(self):
            self._tool_reduction += 1

    async def _demo_async():
        ar = SimpleAsyncRuntime()
        await ar.init({"run_id": "async-001"})
        result = await ar.run_as_result({"task_id": "t1"})
        print(f"  async run: accuracy={result.accuracy}, "
              f"tool_calls={result.tool_calls}")
        await ar.reduce_tool_calls()
        result2 = await ar.run_as_result({"task_id": "t1"})
        print(f"  async run after throttle: tool_calls={result2.tool_calls}")
        await ar.finalize()

    asyncio.run(_demo_async())

    # --- Statistics ---
    print("\n=== Summary ===")
    print(f"  INTERFACE_VERSION: {INTERFACE_VERSION}")
    print(f"  AgentRuntime is ABC: {abc.ABC in AgentRuntime.__mro__}")
    print(f"  AsyncAgentRuntime is ABC: {abc.ABC in AsyncAgentRuntime.__mro__}")
