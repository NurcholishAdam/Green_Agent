# src/analysis/adapters/runtime_adapter.py
"""
Runtime adapter for Green Agent (Enhanced, v2.0.0)
===================================================

Defines the abstract contract that all agent-runtime adapters (LangChain,
AutoGen, custom) must satisfy so the analysis layer can consume them
uniformly.

Original API preserved (all signatures, all side effects):
    class MyRuntime(AgentRuntime):
        def init(self, config): ...
        def run(self, query): ...
        # finalize is optional

    runtime = MyRuntime()
    runtime.init({...})
    result = runtime.run({...})
    runtime.finalize()

    verify_contract(MyRuntime)                 # -> ContractReport

Enhanced API:
    # Rich results
    result = RuntimeResult(accuracy=0.92, tool_calls=3,
                           energy_wh=0.05, carbon_g=0.02,
                           explanation={"headline": "..."})
    d = result.to_dict(safe=True)               # JSON-safe
    ok, violations = result.is_well_formed()    # structured check
    result.severity                             # Severity enum

    # Runtimes with all hooks
    class MyRuntime(AgentRuntime):
        def capabilities(self): return RuntimeCapabilities(...)
        def records(self): return [...]
        def observations(self): return [...]
        def set_callback(self, cb): ...
        def reduce_tool_calls(self): ...        # opt-in
        def shorten_context(self): ...          # opt-in

    result = runtime.run_as_result(query, validate=True, catch=True)

    # Structural typing
    isinstance(runtime, MinimalRuntimeProtocol)   # 3 v1 methods
    isinstance(runtime, EnhancedRuntimeProtocol)  # + hooks

    # Verification
    report = verify_contract(MyRuntime,
                             required_version="5.0.0",
                             require_async=False,
                             check_signatures=True,
                             instantiate=True)
    report = verify_instance(runtime, smoke_test=False)

Enhancements implemented (v2.0.0):
  1.  No extra shadowing    — `RuntimeResult.to_dict()` filters `extra`
                              against the primary field set. v1 let
                              `extra={"accuracy": 0.1}` silently override
                              `result.accuracy` in the emitted dict.
                              `filtered_extra_keys` surfaces the collision.
  2.  Coerced from_dict     — `from_dict` coerces numeric fields
                              (`"0.9"` → `0.9`, `"3"` → `3`) and parses
                              ISO timestamps. v1 stored the raw strings.
  3.  Complete well-formedness — `is_well_formed() -> (bool, violations)`
                              checks every numeric field (finite,
                              non-negative, accuracy ∈ [0,1]), every
                              int field (non-negative), `status` against
                              `RunStatus`, `explanation` type, and the
                              `status=SUCCESS + error!=None` inconsistency.
                              `is_valid()` = `is_well_formed()[0]`.
  4.  Capability truth      — `capabilities()` uses
                              `type(self).X is not AgentRuntime.X` for
                              `records` / `observations` / `steps` (v1's
                              `hasattr(...)` always returned True
                              because the ABC defines those methods).
  5.  Contract rigor        — `verify_contract` rejects classes with
                              unimplemented abstract methods, non-instantiable
                              `__init__`, and incompatible method signatures.
                              `instantiate=True` also cross-checks
                              `capabilities().interface_version`.
  6.  Async auto-detection  — `AsyncAgentRuntime.capabilities()` mirrors
                              the sync identity checks (v1 always reported
                              the dataclass defaults).
  7.  Safer default         — `RuntimeCapabilities.supports_reduce_tool_calls`
                              defaults to `False` (was `True`). Explicit
                              opt-in is safer than implicit support.
  8.  Split protocols       — `MinimalRuntimeProtocol` (three v1 methods)
                              and `EnhancedRuntimeProtocol` (adds hooks).
                              v1's `RuntimeProtocol` is aliased to
                              `MinimalRuntimeProtocol` for backward
                              compatibility.
  9.  Severity on results   — `RuntimeResult.severity` derived from
                              `status` + `simulated` + `error`.
 10.  Well-formedness vs.   — `is_well_formed()` (numeric integrity),
      success                 `is_success()` (status), `is_valid()` =
                              `is_well_formed()[0]`.
 11.  Fixed exception chaining — `RuntimeError_.caused_by` is stored and
                              also assigned to `self.__cause__` so
                              `raise ... from ...` and
                              `RuntimeError_("x", cause=e)` are equivalent.
 12.  Fail-closed version  — `_version_gte` returns `False` on parse
                              errors (was `True`). Handles `-rc1` /
                              `-alpha` suffixes.
 13.  Immutable extra merge — `from_dict` copies `extra` rather than
                              mutating the source dict.
 14.  UTC timestamps       — `RuntimeResult.at` is
                              `datetime.now(timezone.utc)`. The ISO
                              serialisation is timezone-aware.
 15.  Capability version   — `verify_contract(instantiate=True)` calls
                              `capabilities()` and cross-checks its
                              `interface_version`.
 16.  `run_as_result`      — `validate=True` raises `RuntimeError_` on a
                              malformed result; `catch=True` wraps
                              `run()` exceptions as `RuntimeResult(
                              status=FAILED)`. Defaults preserve v1.
 17.  `to_dict(safe=True)` — `RuntimeResult`, `RuntimeCapabilities`,
                              `ContractReport` support JSON-safe output.
 18.  Structured logging   — `runtime_adapter.contract_ok` /
                              `contract_failed` / `invalid_result` /
                              `extra_shadowing` / `verify_instance_failed`.
 19.  `verify_instance()`  — live-instance verification with optional
                              smoke test (`smoke_test=True` runs
                              `init({}) / run({}) / finalize()`).
 20.  Module version       — `__version__ = "2.0.0"` is the module release;
                              `INTERFACE_VERSION = "5.0.0"` remains the
                              contract version (distinct concepts).
 21.  `__all__`            — explicit public surface.
 22.  Async docstrings     — every abstract method on
                              `AsyncAgentRuntime` documents its contract.

Backward compatibility:
  * `RuntimeResult(accuracy=0.9).to_dict()["accuracy"] == 0.9` — unchanged
    for callers who don't put primary-key names in `extra`.
  * `RuntimeResult(accuracy=0.9, extra={"accuracy": 0.1}).to_dict()["accuracy"]`
    is now `0.9` (was `0.1`). Callers who relied on extra overriding
    primary keys — a data-integrity hazard — must switch to explicit
    fields.
  * `RuntimeResult(energy_wh=-5).is_valid()` is now `False` (was `True`).
    The negative value was always semantically broken; the check now
    surfaces it.
  * `AgentRuntime().capabilities().supports_decision_records` is `False`
    (was `True` on all subclasses). Subclasses that genuinely emit
    DecisionRecords should override `capabilities()` or `records()`.
  * `verify_contract(SomeAbstractClass)` returns `ok=False` with an
    "unimplemented abstract methods" issue (was `ok=True`).
  * `verify_contract(SomeBadSignature)` returns `ok=False` with a
    signature issue (was `ok=True`).
  * `_version_gte("garbage", "5.0.0")` returns `False` (was `True`).
  * `RuntimeCapabilities.supports_reduce_tool_calls` default changed to
    `False`. Subclasses that use the base `capabilities()` method are
    unaffected (the method detects overrides via identity check).
  * `RuntimeError_("x", cause=inner).__cause__ is inner` — callers using
    `e.cause` still work (`caused_by` alias); callers using
    `raise ... from ...` now get the same attribute populated.
  * `RuntimeResult.at` is timezone-aware UTC. Callers comparing two
    timestamps for equality across machines get correct results;
    callers relying on naive-local-time semantics should strip the
    tzinfo explicitly.
"""

from __future__ import annotations

import abc
import asyncio
import inspect
import logging
import math
import uuid
from dataclasses import dataclass, field, fields, asdict, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, AsyncIterator, Awaitable, Callable, ClassVar, Dict, Iterable,
    Iterator, List, Optional, Protocol, Set, Tuple, runtime_checkable,
)

__version__ = "2.0.0"

__all__ = [
    # Version constants
    "INTERFACE_VERSION",
    # Enums
    "RuntimeKind",
    "RunStatus",
    "Severity",
    # Errors
    "RuntimeError_",
    "NotInitializedError",
    "AlreadyInitializedError",
    "AlreadyFinalizedError",
    "InvalidConfigError",
    "InvalidQueryError",
    # Data models
    "RuntimeCapabilities",
    "RuntimeResult",
    # Protocols
    "MinimalRuntimeProtocol",
    "EnhancedRuntimeProtocol",
    "RuntimeProtocol",       # backward-compat alias
    # ABCs
    "AgentRuntime",
    "AsyncAgentRuntime",
    # Verification
    "ContractReport",
    "verify_contract",
    "verify_instance",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Version constants
# =============================================================================

#: The *contract* version — what methods and fields must exist. Bump this
#: when the ABC adds or changes required members. Implementations declare
#: compatibility via `capabilities().interface_version` or the class
#: attribute `interface_version`.
INTERFACE_VERSION: str = "5.0.0"


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
    PARTIAL = "partial"
    FAILED = "failed"
    TIMEOUT = "timeout"
    INVALID = "invalid"


class Severity(Enum):
    """Severity of a result or report — aligned with the layer."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


_VALID_RUN_STATUSES = frozenset(s.value for s in RunStatus)


# =============================================================================
# Helpers
# =============================================================================

def _jsonable(v: Any) -> Any:
    """Recursively coerce to a JSON-serialisable primitive."""
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [_jsonable(x) for x in v]
    if is_dataclass(v) and not isinstance(v, type):
        try:
            return _jsonable(asdict(v))
        except Exception:
            pass
    return repr(v)


def _parse_datetime(value: Any) -> datetime:
    """Parse a datetime from an ISO string, or fall back to now (UTC)."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except (ValueError, TypeError):
            pass
    return datetime.now(timezone.utc)


def _parse_version(s: Any) -> Optional[Tuple[int, int, int, str]]:
    """Parse a semver-ish string into `(major, minor, patch, prerelease)`."""
    if not isinstance(s, str):
        return None
    stripped = s.strip()
    if not stripped:
        return None
    prerelease = ""
    if "-" in stripped:
        stripped, prerelease = stripped.split("-", 1)
    parts = stripped.split(".")
    nums: List[int] = []
    for x in parts[:3]:
        try:
            nums.append(int(x))
        except (TypeError, ValueError):
            return None
    while len(nums) < 3:
        nums.append(0)
    return (nums[0], nums[1], nums[2], prerelease)


def _version_gte(a: str, b: str) -> bool:
    """
    Return True if `a >= b` under semantic versioning.

    Fails closed: any parse error → `False`. Handles prerelease suffixes
    (`-rc1` < release). v1 returned `True` on parse error — the wrong
    default for a contract check.
    """
    pa = _parse_version(a)
    pb = _parse_version(b)
    if pa is None or pb is None:
        return False
    core_a = pa[:3]
    core_b = pb[:3]
    if core_a != core_b:
        return core_a > core_b
    pre_a = pa[3]
    pre_b = pb[3]
    # No prerelease > any prerelease for the same core version
    if not pre_a and pre_b:
        return True
    if pre_a and not pre_b:
        return False
    # Both prereleases or both none — lexicographic on the prerelease tag
    return pre_a >= pre_b


# =============================================================================
# Error contract
# =============================================================================

class RuntimeError_(Exception):
    """
    Base exception for runtime adapter failures.

    Named with a trailing underscore to avoid shadowing the builtin
    `RuntimeError`. Implementations may raise subclasses to signal
    specific failure modes; callers should catch `RuntimeError_` for a
    uniform error path.
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
        self.caused_by = cause
        self.details = details or {}
        # Also populate __cause__ so `raise ... from ...` and
        # `RuntimeError_("x", cause=e)` are equivalent for callers that
        # inspect `__cause__` directly.
        if cause is not None:
            try:
                self.__cause__ = cause
            except Exception:
                pass

    # Backward-compat alias (v1 exposed `.cause`)
    @property
    def cause(self) -> Optional[BaseException]:
        return self.caused_by


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
# RuntimeCapabilities
# =============================================================================

@dataclass
class RuntimeCapabilities:
    """
    Declares what a runtime adapter supports.

    Consumers (AdaptiveController, HITL gates, analyzers) query this to
    decide which calls are safe. All fields are self-declared — a runtime
    that reports a capability it doesn't implement is a contract bug
    (verifiable via `verify_contract` + a real invocation).

    `runtime_kind` and `supports_reinit` are **self-declared and not
    verified** — a runtime could claim `"langchain"` while wrapping
    AutoGen, or claim reinit support that fails on the second call.
    """
    interface_version: str = INTERFACE_VERSION
    runtime_kind: str = RuntimeKind.UNKNOWN.value

    # --- Lifecycle ---
    supports_async: bool = False
    supports_reinit: bool = False

    # --- Eco-mode hooks (opt-in; default False per v2) ---
    supports_reduce_tool_calls: bool = False
    supports_shorten_context: bool = False

    # --- Analysis contracts ---
    supports_decision_records: bool = False
    supports_agent_observations: bool = False
    supports_step_records: bool = False

    # --- Precision / callbacks ---
    supports_precision_control: bool = False
    supports_callback_injection: bool = False

    # --- Failure semantics ---
    raises_on_failure: bool = True
    returns_error_dict: bool = False

    # --- Extra metadata ---
    deployment_id: Optional[str] = None
    description: Optional[str] = None

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        if safe:
            return _jsonable(out)
        return out


# =============================================================================
# RuntimeResult
# =============================================================================

#: Field classification tables (used by `from_dict` coercion and
#: `is_well_formed` range checks).
_FLOAT_FIELDS: Set[str] = frozenset({
    "accuracy", "latency_s", "energy_wh", "carbon_g", "helium_units",
})
_INT_FIELDS: Set[str] = frozenset({
    "tool_calls", "conversation_depth", "llm_calls", "tokens",
})
_PRIMARY_FIELDS: Set[str] = frozenset({
    "accuracy", "tool_calls", "conversation_depth",
    "latency_s", "energy_wh", "carbon_g", "helium_units",
    "llm_calls", "tokens", "precision", "region",
    "run_id", "task_id", "agent_id", "deployment_id",
    "status", "error", "simulated", "explanation", "at", "severity",
})


@dataclass
class RuntimeResult:
    """
    Structured result of a `run()` call.

    A superset of the v1 dict contract (`accuracy`, `tool_calls`,
    `conversation_depth`). `to_dict()` preserves those keys first and
    **filters `extra` against the primary field set** so a caller can't
    accidentally shadow `accuracy` by putting it in `extra`.
    """
    # --- v1 contract ---
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

    # --- XAI ---
    explanation: Optional[Dict[str, Any]] = None

    # --- Metadata ---
    at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    extra: Dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Post-init
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:
        if self.extra is None:
            self.extra = {}
        elif not isinstance(self.extra, dict):
            try:
                self.extra = dict(self.extra)
            except (TypeError, ValueError):
                self.extra = {}

        # Normalise `at` to timezone-aware UTC
        self.at = _parse_datetime(self.at)

        # Warn if `extra` would shadow primary keys
        shadowed = self.filtered_extra_keys
        if shadowed:
            logger.warning(
                "runtime_adapter.extra_shadows_primary_keys",
                extra={"keys": shadowed},
            )

    # ------------------------------------------------------------------
    # Extra-key filtering
    # ------------------------------------------------------------------

    @property
    def filtered_extra_keys(self) -> List[str]:
        """
        Keys in `extra` that `to_dict()` will drop because they collide
        with a primary field name. Surfaced so callers can detect the
        (previously silent) shadowing.
        """
        if not self.extra:
            return []
        return sorted(
            k for k in self.extra if k in _PRIMARY_FIELDS
        )

    # ------------------------------------------------------------------
    # Severity
    # ------------------------------------------------------------------

    @property
    def severity(self) -> Severity:
        """Derive a severity from status / simulated / error."""
        if self.status == RunStatus.SUCCESS.value:
            return Severity.WARNING if self.simulated else Severity.INFO
        if self.status == RunStatus.PARTIAL.value:
            return Severity.WARNING
        if self.status == RunStatus.INVALID.value:
            return Severity.EMERGENCY
        if self.status in (
            RunStatus.FAILED.value, RunStatus.TIMEOUT.value,
        ):
            return Severity.CRITICAL
        return Severity.INFO

    # ------------------------------------------------------------------
    # Well-formedness / success
    # ------------------------------------------------------------------

    def is_well_formed(self) -> Tuple[bool, List[str]]:
        """
        Return `(ok, violations)` after checking every numeric field.

        Checks:
          - Every numeric field is finite
          - Every non-negative field is >= 0
          - `accuracy` is in [0, 1]
          - Every int field is non-negative
          - `status` is a valid `RunStatus` value
          - `explanation` is dict or None
          - `status=SUCCESS` and `error != None` is inconsistent
        """
        violations: List[str] = []

        # Float fields
        for name in ("accuracy", "latency_s", "energy_wh",
                     "carbon_g", "helium_units"):
            value = getattr(self, name, 0.0)
            if isinstance(value, bool):
                violations.append(f"{name}_bool")
                continue
            try:
                f = float(value)
            except (TypeError, ValueError, OverflowError):
                violations.append(f"{name}_non_numeric")
                continue
            if not math.isfinite(f):
                violations.append(f"{name}_non_finite")
                continue
            if name == "accuracy":
                if not (0.0 <= f <= 1.0):
                    violations.append(f"accuracy_out_of_range({f})")
            else:
                if f < 0:
                    violations.append(f"{name}_negative({f})")

        # Int fields
        for name in ("tool_calls", "conversation_depth",
                     "llm_calls", "tokens"):
            value = getattr(self, name, 0)
            if isinstance(value, bool):
                violations.append(f"{name}_bool")
                continue
            try:
                i = int(value)
            except (TypeError, ValueError, OverflowError):
                violations.append(f"{name}_non_numeric")
                continue
            if i < 0:
                violations.append(f"{name}_negative({i})")

        # Status
        if self.status not in _VALID_RUN_STATUSES:
            violations.append(f"status_invalid({self.status!r})")

        # Explanation type
        if (
            self.explanation is not None
            and not isinstance(self.explanation, dict)
        ):
            violations.append(
                f"explanation_type({type(self.explanation).__name__})"
            )

        # Error consistency
        if self.status == RunStatus.SUCCESS.value and self.error is not None:
            violations.append("success_with_error")

        return (len(violations) == 0, violations)

    def is_valid(self) -> bool:
        """Backward-compatible alias for `is_well_formed()[0]`."""
        return self.is_well_formed()[0]

    def is_success(self) -> bool:
        """True iff `status == success` and the result is well-formed."""
        return (
            self.status == RunStatus.SUCCESS.value
            and self.is_well_formed()[0]
        )

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        """
        Return a dict with the v1 keys first, then extended fields.

        `extra` is filtered against the primary field set — a caller who
        puts `{"accuracy": 0.1}` in `extra` cannot shadow the real value.
        """
        ordered: Dict[str, Any] = {
            # v1 keys first
            "accuracy": self.accuracy,
            "tool_calls": self.tool_calls,
            "conversation_depth": self.conversation_depth,
            # Extended
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
            "at": (
                self.at.isoformat()
                if isinstance(self.at, datetime) else self.at
            ),
            "severity": self.severity.value,
        }
        # Extra keys (filtered)
        for k, v in (self.extra or {}).items():
            if k not in ordered:
                ordered[k] = v
        if safe:
            return _jsonable(ordered)
        return ordered

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RuntimeResult":
        """
        Construct from a raw dict.

        - Known numeric fields are coerced (`"0.9"` → `0.9`).
        - ISO timestamps are parsed.
        - Unknown keys are preserved in `extra`.
        - `extra` from the input is copied, not mutated.
        """
        if not isinstance(data, dict):
            return cls()

        known = {f.name for f in fields(cls)} - {"extra"}
        kwargs: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        for k, v in data.items():
            if k == "extra":
                if isinstance(v, dict):
                    extra.update(v)
                continue
            if k in known:
                kwargs[k] = v
            else:
                extra[k] = v

        # Coerce float fields
        for k in _FLOAT_FIELDS & kwargs.keys():
            v = kwargs[k]
            try:
                kwargs[k] = float(v) if v is not None else 0.0
            except (TypeError, ValueError, OverflowError):
                pass  # leave raw; is_well_formed will flag it

        # Coerce int fields
        for k in _INT_FIELDS & kwargs.keys():
            v = kwargs[k]
            try:
                kwargs[k] = int(v) if v is not None else 0
            except (TypeError, ValueError, OverflowError):
                pass

        # Parse datetime
        if "at" in kwargs:
            kwargs["at"] = _parse_datetime(kwargs["at"])

        # Normalise explanation
        if "explanation" in kwargs and not isinstance(
            kwargs["explanation"], (dict, type(None)),
        ):
            extra["_explanation_unparseable"] = repr(
                kwargs["explanation"]
            )
            kwargs["explanation"] = None

        if extra:
            kwargs["extra"] = extra
        return cls(**kwargs)


# =============================================================================
# Runtime protocols (structural typing)
# =============================================================================

@runtime_checkable
class MinimalRuntimeProtocol(Protocol):
    """
    Structural-typing protocol for the v1 three-method contract.

    `isinstance(x, MinimalRuntimeProtocol)` is True for any object that
    has callable `init`, `run`, and `finalize` — including third-party
    classes that don't inherit from `AgentRuntime`.
    """
    def init(self, config: Dict[str, Any]) -> None: ...
    def run(self, query: Dict[str, Any]) -> Dict[str, Any]: ...
    def finalize(self) -> None: ...


@runtime_checkable
class EnhancedRuntimeProtocol(Protocol):
    """
    Structural-typing protocol for a full-featured runtime.

    Adds the enhanced hooks (`capabilities`, `records`, `observations`,
    `set_callback`) on top of the v1 contract. `isinstance` checks
    presence only — a runtime may implement these methods as no-ops.
    """
    def init(self, config: Dict[str, Any]) -> None: ...
    def run(self, query: Dict[str, Any]) -> Dict[str, Any]: ...
    def finalize(self) -> None: ...
    def capabilities(self) -> RuntimeCapabilities: ...
    def records(self) -> List[Any]: ...
    def observations(self) -> List[Any]: ...
    def set_callback(self, callback: Any) -> None: ...


#: Backward-compatible alias for `MinimalRuntimeProtocol`.
RuntimeProtocol = MinimalRuntimeProtocol


# =============================================================================
# AgentRuntime — sync ABC
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
    `run(query)` must return a dict containing at least:
        - accuracy: float
        - tool_calls: int
        - conversation_depth: int

    Implementations MAY return a `RuntimeResult` (which has a `to_dict()`
    producing the same keys) or any dict superset. Consumers calling
    `run_as_result()` normalise the result automatically.

    Failure semantics
    -----------------
    Implementations SHOULD raise `RuntimeError_` (or a subclass) on
    unrecoverable failure.

    Enhanced hooks (all optional, defaults are no-ops)
    --------------------------------------------------
    `reduce_tool_calls()` — called by AdaptiveController in low_energy.
    `shorten_context()`   — called by AdaptiveController in low_memory.

    Analysis contracts (override to enable)
    ---------------------------------------
    `records()`       — return `DecisionRecord`s emitted this session.
    `observations()`  — return `AgentObservation`s for multi-agent analysis.
    `steps()`         — return `StepRecord`s for execution tracing.
    """

    #: Contract version — override to declare a different interface.
    interface_version: ClassVar[str] = INTERFACE_VERSION

    # ------------------------------------------------------------------
    # Abstract methods (the v1 contract)
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

    # ------------------------------------------------------------------
    # Default implementations
    # ------------------------------------------------------------------

    def finalize(self) -> None:
        """Release resources. Called once at end of session. Default: no-op."""
        pass

    def reduce_tool_calls(self) -> None:
        """AdaptiveController eco-mode hook. Default: no-op."""
        pass

    def shorten_context(self) -> None:
        """AdaptiveController low-memory hook. Default: no-op."""
        pass

    def capabilities(self) -> RuntimeCapabilities:
        """
        Return this runtime's capabilities.

        The default reports only what the ABC can *detect* via identity
        checks on the type — a subclass that overrides `records()` gets
        `supports_decision_records=True`, one that doesn't gets `False`.
        Override to declare additional capabilities (async, callbacks,
        precision control).
        """
        cls = type(self)
        return RuntimeCapabilities(
            interface_version=self.interface_version,
            runtime_kind=RuntimeKind.UNKNOWN.value,
            supports_async=False,
            supports_reinit=False,
            supports_reduce_tool_calls=(
                cls.reduce_tool_calls is not AgentRuntime.reduce_tool_calls
            ),
            supports_shorten_context=(
                cls.shorten_context is not AgentRuntime.shorten_context
            ),
            supports_decision_records=(
                cls.records is not AgentRuntime.records
            ),
            supports_agent_observations=(
                cls.observations is not AgentRuntime.observations
            ),
            supports_step_records=(
                cls.steps is not AgentRuntime.steps
            ),
            supports_callback_injection=(
                cls.set_callback is not AgentRuntime.set_callback
            ),
        )

    # ------------------------------------------------------------------
    # Analysis contract accessors (default: empty)
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
        Wire in a framework callback (e.g. GreenLangChainCallback).

        Default: no-op with a single-shot debug log.
        """
        if not getattr(type(self), "_set_callback_warned", False):
            logger.debug(
                "runtime_adapter.set_callback_noop",
                extra={"class": type(self).__name__},
            )
            try:
                type(self)._set_callback_warned = True  # type: ignore
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Convenience — normalise any run() output
    # ------------------------------------------------------------------

    def run_as_result(
        self,
        query: Dict[str, Any],
        *,
        validate: bool = False,
        catch: bool = False,
    ) -> RuntimeResult:
        """
        Run and normalise the result to a `RuntimeResult`.

        `validate=True` raises `RuntimeError_` on a malformed result.
        `catch=True` wraps any exception from `run()` into a
        `RuntimeResult(status=failed, error=...)`.

        Defaults preserve v1 behaviour: return the result unvalidated,
        propagate exceptions.
        """
        try:
            raw = self.run(query)
        except RuntimeError_:
            raise
        except Exception as e:
            if not catch:
                raise
            logger.warning(
                "runtime_adapter.run_failed",
                extra={
                    "class": type(self).__name__,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            return RuntimeResult(
                status=RunStatus.FAILED.value,
                error=f"{type(e).__name__}: {e}",
            )

        result: RuntimeResult
        if isinstance(raw, RuntimeResult):
            result = raw
        else:
            result = RuntimeResult.from_dict(raw or {})

        if validate:
            ok, violations = result.is_well_formed()
            if not ok:
                logger.warning(
                    "runtime_adapter.invalid_result",
                    extra={
                        "class": type(self).__name__,
                        "violations": violations,
                    },
                )
                raise RuntimeError_(
                    "runtime result failed validation: "
                    f"{violations}",
                )
        return result


# =============================================================================
# AsyncAgentRuntime — async ABC
# =============================================================================

class AsyncAgentRuntime(abc.ABC):
    """
    Abstract base class for asynchronous agent runtimes.

    Mirrors `AgentRuntime` exactly, except `run()` is a coroutine.
    """

    interface_version: ClassVar[str] = INTERFACE_VERSION

    @abc.abstractmethod
    async def init(self, config: Dict[str, Any]) -> None:
        """Configure the runtime. Called once before any `run()`."""
        ...

    @abc.abstractmethod
    async def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single agent turn. Must return a dict with at least
        `accuracy`, `tool_calls`, `conversation_depth`.
        """
        ...

    async def finalize(self) -> None:
        """Release resources. Called once at end of session. Default: no-op."""
        pass

    async def reduce_tool_calls(self) -> None:
        """AdaptiveController eco-mode hook. Default: no-op."""
        pass

    async def shorten_context(self) -> None:
        """AdaptiveController low-memory hook. Default: no-op."""
        pass

    def capabilities(self) -> RuntimeCapabilities:
        """
        Return this runtime's capabilities.

        Mirrors `AgentRuntime.capabilities()` — identity checks detect
        overridden hooks, so an async subclass that overrides
        `reduce_tool_calls()` correctly reports support.
        """
        cls = type(self)
        return RuntimeCapabilities(
            interface_version=self.interface_version,
            runtime_kind=RuntimeKind.UNKNOWN.value,
            supports_async=True,
            supports_reinit=False,
            supports_reduce_tool_calls=(
                cls.reduce_tool_calls
                is not AsyncAgentRuntime.reduce_tool_calls
            ),
            supports_shorten_context=(
                cls.shorten_context
                is not AsyncAgentRuntime.shorten_context
            ),
            supports_decision_records=(
                cls.records is not AsyncAgentRuntime.records
            ),
            supports_agent_observations=(
                cls.observations is not AsyncAgentRuntime.observations
            ),
            supports_step_records=(
                cls.steps is not AsyncAgentRuntime.steps
            ),
            supports_callback_injection=(
                cls.set_callback is not AsyncAgentRuntime.set_callback
            ),
        )

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
        """Wire in a framework callback. Default: no-op."""
        pass

    async def run_as_result(
        self,
        query: Dict[str, Any],
        *,
        validate: bool = False,
        catch: bool = False,
    ) -> RuntimeResult:
        """Async variant of `AgentRuntime.run_as_result`."""
        try:
            raw = await self.run(query)
        except RuntimeError_:
            raise
        except Exception as e:
            if not catch:
                raise
            logger.warning(
                "runtime_adapter.run_failed",
                extra={
                    "class": type(self).__name__,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            return RuntimeResult(
                status=RunStatus.FAILED.value,
                error=f"{type(e).__name__}: {e}",
            )

        result: RuntimeResult
        if isinstance(raw, RuntimeResult):
            result = raw
        else:
            result = RuntimeResult.from_dict(raw or {})

        if validate:
            ok, violations = result.is_well_formed()
            if not ok:
                logger.warning(
                    "runtime_adapter.invalid_result",
                    extra={
                        "class": type(self).__name__,
                        "violations": violations,
                    },
                )
                raise RuntimeError_(
                    "async runtime result failed validation: "
                    f"{violations}",
                )
        return result


# =============================================================================
# Contract verification
# =============================================================================

@dataclass
class ContractReport:
    """Result of a `verify_contract()` or `verify_instance()` check."""
    class_name: str
    ok: bool
    issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        if safe:
            return _jsonable(out)
        return out


def _check_bindable(cls: Any, name: str, *sample_args: Any) -> Optional[str]:
    """
    Check that `cls.name` can be called with `sample_args` (plus `self`
    for instance methods). Returns `None` on success, else an issue msg.
    """
    method = getattr(cls, name, None)
    if method is None or not callable(method):
        return f"missing method: {name}"
    try:
        sig = inspect.signature(method)
    except (ValueError, TypeError):
        # Can't introspect (e.g., C extension) — assume OK
        return None
    # Instance-method interpretation: first arg is self
    try:
        sig.bind(None, *sample_args)
        return None
    except TypeError:
        pass
    # Plain-function interpretation (staticmethod / classmethod bound)
    try:
        sig.bind(*sample_args)
        return None
    except TypeError:
        pass
    return f"{name}() signature incompatible with expected call pattern"


def verify_contract(
    cls: Any,
    *,
    required_version: Optional[str] = None,
    require_async: bool = False,
    check_signatures: bool = True,
    instantiate: bool = False,
) -> ContractReport:
    """
    Verify that `cls` satisfies the `AgentRuntime` contract.

    Parameters
    ----------
    cls : Any
        The class to check.
    required_version : Optional[str]
        Reject classes whose `interface_version` is older.
    require_async : bool
        Require `AsyncAgentRuntime` subclass.
    check_signatures : bool
        Verify `init` / `run` / `finalize` accept the expected
        positional args. Default True.
    instantiate : bool
        Also instantiate `cls()` (with no args) and cross-check
        `capabilities().interface_version`. Default False.
    """
    class_name = getattr(cls, "__name__", str(cls))
    report = ContractReport(class_name=class_name, ok=True)

    # --- Kind check ---
    is_sync = isinstance(cls, type) and issubclass(cls, AgentRuntime)
    is_async = isinstance(cls, type) and issubclass(cls, AsyncAgentRuntime)
    if not (is_sync or is_async):
        report.ok = False
        report.issues.append(
            "not a subclass of AgentRuntime or AsyncAgentRuntime"
        )
        _log_contract(report)
        return report

    if require_async and not is_async:
        report.ok = False
        report.issues.append(
            "require_async=True but class is not AsyncAgentRuntime"
        )

    # --- Abstract methods check ---
    abstract = getattr(cls, "__abstractmethods__", frozenset())
    if abstract:
        report.ok = False
        report.issues.append(
            f"unimplemented abstract methods: {sorted(abstract)}"
        )

    # --- Required methods ---
    for name in ("init", "run", "finalize"):
        if not hasattr(cls, name) or not callable(getattr(cls, name, None)):
            report.ok = False
            report.issues.append(f"missing method: {name}")

    # --- Method signature check ---
    if check_signatures and report.ok:
        sig_issue = _check_bindable(cls, "init", {})
        if sig_issue:
            report.ok = False
            report.issues.append(sig_issue)
        sig_issue = _check_bindable(cls, "run", {})
        if sig_issue:
            report.ok = False
            report.issues.append(sig_issue)
        sig_issue = _check_bindable(cls, "finalize")
        if sig_issue:
            report.ok = False
            report.issues.append(sig_issue)

    # --- Enhanced hooks (warn if missing, not error) ---
    for name in ("reduce_tool_calls", "shorten_context", "capabilities"):
        if not hasattr(cls, name) or not callable(
            getattr(cls, name, None),
        ):
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
                f"interface_version {cls_version!r} < "
                f"required {required_version!r}"
            )

    # --- Optional instantiation ---
    if instantiate and report.ok:
        try:
            instance = cls()
        except Exception as e:
            report.ok = False
            report.issues.append(
                f"cannot instantiate cls() with no args: "
                f"{type(e).__name__}: {e}"
            )
        else:
            try:
                caps = instance.capabilities()
                if isinstance(caps, RuntimeCapabilities):
                    if (
                        required_version is not None
                        and not _version_gte(
                            caps.interface_version, required_version,
                        )
                    ):
                        report.ok = False
                        report.issues.append(
                            f"capabilities().interface_version "
                            f"{caps.interface_version!r} < "
                            f"required {required_version!r}"
                        )
                else:
                    report.warnings.append(
                        f"capabilities() returned "
                        f"{type(caps).__name__}, expected "
                        f"RuntimeCapabilities"
                    )
            except Exception as e:
                report.warnings.append(
                    f"capabilities() raised {type(e).__name__}: {e}"
                )

    _log_contract(report)
    return report


def _log_contract(report: ContractReport) -> None:
    level = (
        logger.info if report.ok else logger.warning
    )
    level(
        "runtime_adapter.contract_ok" if report.ok
        else "runtime_adapter.contract_failed",
        extra={
            "class_name": report.class_name,
            "n_issues": len(report.issues),
            "n_warnings": len(report.warnings),
        },
    )


def verify_instance(
    runtime: Any,
    *,
    required_version: Optional[str] = None,
    smoke_test: bool = False,
    smoke_query: Optional[Dict[str, Any]] = None,
) -> ContractReport:
    """
    Verify a live runtime instance.

    Checks the class contract via `verify_contract()`, then inspects the
    instance's `capabilities()`. With `smoke_test=True` also runs
    `init({}) / run(query or {}) / finalize()` and validates the returned
    `RuntimeResult`.
    """
    report = verify_contract(
        type(runtime),
        required_version=required_version,
        instantiate=False,
        check_signatures=False,
    )

    # --- Instance-level capabilities check ---
    try:
        caps = runtime.capabilities()
        if not isinstance(caps, RuntimeCapabilities):
            report.warnings.append(
                f"capabilities() returned {type(caps).__name__}, "
                f"expected RuntimeCapabilities"
            )
        elif (
            required_version is not None
            and not _version_gte(caps.interface_version, required_version)
        ):
            report.ok = False
            report.issues.append(
                f"capabilities().interface_version "
                f"{caps.interface_version!r} < "
                f"required {required_version!r}"
            )
    except Exception as e:
        report.warnings.append(
            f"capabilities() raised {type(e).__name__}: {e}"
        )

    # --- Smoke test ---
    if smoke_test and report.ok:
        query = smoke_query or {}
        try:
            if isinstance(runtime, AsyncAgentRuntime):
                async def _run_async():
                    await runtime.init({})
                    try:
                        raw = await runtime.run(query)
                    finally:
                        try:
                            await runtime.finalize()
                        except Exception:
                            pass
                    return raw
                raw = asyncio.run(_run_async())
            else:
                runtime.init({})
                try:
                    raw = runtime.run(query)
                finally:
                    try:
                        runtime.finalize()
                    except Exception:
                        pass
            result = (
                raw if isinstance(raw, RuntimeResult)
                else RuntimeResult.from_dict(raw or {})
            )
            ok, violations = result.is_well_formed()
            if not ok:
                report.ok = False
                report.issues.append(
                    f"smoke-test result malformed: {violations}"
                )
        except Exception as e:
            report.ok = False
            report.issues.append(
                f"smoke test raised {type(e).__name__}: {e}"
            )

    _log_contract(report)
    return report


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json
    from decimal import Decimal

    logging.basicConfig(level=logging.WARNING)

    # =================================================================
    # 1. Original usage (backward compatible)
    # =================================================================
    print("=== 1. Original usage ===")

    class MinimalRuntime(AgentRuntime):
        def init(self, config):
            self.config = config
        def run(self, query):
            return {
                "accuracy": 0.9,
                "tool_calls": 1,
                "conversation_depth": 1,
            }

    r = MinimalRuntime()
    r.init({"policy_version": "v5.0.1"})
    print(f"  run():      {r.run({'task_id': 't1'})}")
    print(f"  finalize(): {r.finalize()}")
    caps = r.capabilities()
    print(f"  supports_decision_records: "
          f"{caps.supports_decision_records}  "
          f"(v1 was True via hasattr; v2 correctly False)")

    # =================================================================
    # 2. Extra shadowing prevention
    # =================================================================
    print("\n=== 2. Extra shadowing prevention ===")
    result_shadowed = RuntimeResult(
        accuracy=0.92,
        extra={"accuracy": 0.1, "custom_field": "hello"},
    )
    d = result_shadowed.to_dict()
    print(f"  to_dict()['accuracy']:     {d['accuracy']}  "
          f"(expected 0.92, NOT 0.1)")
    print(f"  filtered_extra_keys:       "
          f"{result_shadowed.filtered_extra_keys}")
    print(f"  custom_field preserved:    {d.get('custom_field')}")

    # =================================================================
    # 3. from_dict coercion + extra isolation
    # =================================================================
    print("\n=== 3. from_dict coercion + isolation ===")
    source = {
        "accuracy": "0.85",
        "tool_calls": "3",
        "energy_wh": "0.05",
        "at": "2024-06-15T10:30:00Z",
        "extra": {"source": "test"},
        "future_field": "preserved",
    }
    r_coerced = RuntimeResult.from_dict(source)
    print(f"  accuracy type:  {type(r_coerced.accuracy).__name__}  "
          f"value={r_coerced.accuracy}")
    print(f"  tool_calls type: {type(r_coerced.tool_calls).__name__}  "
          f"value={r_coerced.tool_calls}")
    print(f"  at tz-aware:     {r_coerced.at.tzinfo is not None}")
    print(f"  extra preserved: {r_coerced.extra}")
    print(f"  source['extra'] unmutated: "
          f"{source['extra'] == {'source': 'test'}}")

    # =================================================================
    # 4. is_well_formed catches negative energy (v1 returned True)
    # =================================================================
    print("\n=== 4. is_well_formed completeness ===")
    bad = RuntimeResult(accuracy=0.9, energy_wh=-5.0)
    ok, violations = bad.is_well_formed()
    print(f"  is_well_formed: {ok}  "
          f"(v1 is_valid would have returned True)")
    print(f"  violations:     {violations}")
    print(f"  is_valid:       {bad.is_valid()}")

    nan_result = RuntimeResult(accuracy=0.9, latency_s=float("nan"))
    ok2, v2 = nan_result.is_well_formed()
    print(f"  NaN latency caught: {not ok2}, violations: {v2}")

    # =================================================================
    # 5. verify_contract catches abstract class
    # =================================================================
    print("\n=== 5. verify_contract catches abstract class ===")

    class StillAbstract(AgentRuntime):
        def init(self, config): pass
        # run() not implemented → abstract

    report = verify_contract(StillAbstract)
    print(f"  ok: {report.ok}")
    print(f"  issues: {report.issues}")

    # =================================================================
    # 6. verify_contract catches bad signature
    # =================================================================
    print("\n=== 6. verify_contract catches bad signature ===")

    class BadSig(AgentRuntime):
        def init(self):
            pass  # ← missing config arg
        def run(self, query):
            return {}

    report_sig = verify_contract(BadSig)
    print(f"  ok: {report_sig.ok}")
    for issue in report_sig.issues:
        print(f"    - {issue}")

    # =================================================================
    # 7. verify_contract(instantiate=True) cross-checks capabilities
    # =================================================================
    print("\n=== 7. verify_contract instantiation cross-check ===")

    class WrongCapsVersion(AgentRuntime):
        interface_version = "5.0.0"       # class says 5.0.0
        def init(self, config): pass
        def run(self, query):
            return {"accuracy": 0.9, "tool_calls": 0, "conversation_depth": 0}
        def capabilities(self):
            return RuntimeCapabilities(interface_version="4.0.0")  # claims 4.0

    rep_inst = verify_contract(
        WrongCapsVersion,
        required_version="5.0.0",
        instantiate=True,
    )
    print(f"  ok: {rep_inst.ok}")
    for issue in rep_inst.issues:
        print(f"    - {issue}")

    # =================================================================
    # 8. Async capabilities auto-detection
    # =================================================================
    print("\n=== 8. Async capabilities auto-detection ===")

    class SimpleAsync(AsyncAgentRuntime):
        def __init__(self):
            self._tool_reduction = 0
        async def init(self, config):
            pass
        async def run(self, query):
            await asyncio.sleep(0)
            return {"accuracy": 0.88, "tool_calls": 2, "conversation_depth": 1}
        async def reduce_tool_calls(self):
            self._tool_reduction += 1

    async def _demo_async():
        ar = SimpleAsync()
        await ar.init({})
        caps = ar.capabilities()
        print(f"  supports_async:               {caps.supports_async}")
        print(f"  supports_reduce_tool_calls:   "
              f"{caps.supports_reduce_tool_calls}  "
              f"(v1 didn't detect overrides)")
        print(f"  supports_shorten_context:     "
              f"{caps.supports_shorten_context}")
        result = await ar.run_as_result({}, validate=True)
        print(f"  run_as_result validate:       {result.is_valid()}")
        await ar.finalize()

    asyncio.run(_demo_async())

    # =================================================================
    # 9. run_as_result with catch=True
    # =================================================================
    print("\n=== 9. run_as_result(catch=True) ===")

    class BrokenRuntime(AgentRuntime):
        def init(self, config): pass
        def run(self, query):
            raise ValueError("simulated failure")

    broken = BrokenRuntime()
    broken.init({})
    try:
        broken.run_as_result({})  # default: propagate
        print("  no exception (unexpected)")
    except ValueError as e:
        print(f"  propagated: {type(e).__name__}: {e}")

    result_caught = broken.run_as_result({}, catch=True)
    print(f"  caught:     status={result_caught.status}, "
          f"error={result_caught.error}")
    print(f"  severity:   {result_caught.severity.value}")

    # =================================================================
    # 10. _version_gte with pre-release
    # =================================================================
    print("\n=== 10. _version_gte pre-release handling ===")
    from src.analysis.adapters.runtime_adapter import _version_gte
    cases = [
        ("5.0.0", "5.0.0", True),
        ("5.0.1", "5.0.0", True),
        ("4.9.9", "5.0.0", False),
        ("5.0.0-rc1", "5.0.0", False),   # prerelease < release
        ("5.0.0", "5.0.0-rc1", True),
        ("garbage", "5.0.0", False),     # fail-closed
    ]
    for a, b, expected in cases:
        actual = _version_gte(a, b)
        status = "OK" if actual == expected else "FAIL"
        print(f"  _version_gte({a!r}, {b!r}) = {actual}  "
              f"(expected {expected}) [{status}]")

    # =================================================================
    # 11. Protocol structural typing
    # =================================================================
    print("\n=== 11. Protocol structural typing ===")

    class PlainRuntime:
        def init(self, config): pass
        def run(self, query):
            return {"accuracy": 1.0, "tool_calls": 0, "conversation_depth": 0}
        def finalize(self): pass

    class FullRuntime(AgentRuntime):
        def init(self, config): pass
        def run(self, query):
            return {"accuracy": 0.9, "tool_calls": 0, "conversation_depth": 0}
        def capabilities(self):
            return RuntimeCapabilities(runtime_kind="custom")
        def records(self): return []
        def observations(self): return []
        def set_callback(self, cb): pass

    plain = PlainRuntime()
    full = FullRuntime()
    print(f"  plain is MinimalRuntimeProtocol:  "
          f"{isinstance(plain, MinimalRuntimeProtocol)}")
    print(f"  plain is EnhancedRuntimeProtocol: "
          f"{isinstance(plain, EnhancedRuntimeProtocol)}")
    print(f"  full is MinimalRuntimeProtocol:   "
          f"{isinstance(full, MinimalRuntimeProtocol)}")
    print(f"  full is EnhancedRuntimeProtocol:  "
          f"{isinstance(full, EnhancedRuntimeProtocol)}")

    # =================================================================
    # 12. verify_instance with smoke_test
    # =================================================================
    print("\n=== 12. verify_instance(smoke_test=True) ===")

    rep_inst2 = verify_instance(
        MinimalRuntime(),
        smoke_test=True,
    )
    print(f"  ok: {rep_inst2.ok}")
    if rep_inst2.issues:
        for issue in rep_inst2.issues:
            print(f"    - {issue}")

    # =================================================================
    # 13. JSON-safe to_dict
    # =================================================================
    print("\n=== 13. JSON-safe to_dict ===")
    weird = RuntimeResult(
        accuracy=0.9,
        explanation={"k": Decimal("1.5"), "v": object()},
    )
    try:
        json.dumps(weird.to_dict(safe=True))
        print("  json.dumps(to_dict(safe=True)): OK")
    except (TypeError, ValueError) as e:
        print(f"  FAILED: {e}")

    # =================================================================
    # 14. Error contract — cause / __cause__ unification
    # =================================================================
    print("\n=== 14. Error contract ===")
    inner = ValueError("inner")
    exc = RuntimeError_("wrapper", cause=inner)
    print(f"  exc.caused_by is inner:    {exc.caused_by is inner}")
    print(f"  exc.__cause__ is inner:    {exc.__cause__ is inner}")
    print(f"  exc.cause (v1 alias):      {exc.cause is inner}")

    # =================================================================
    # 15. Severity derivation
    # =================================================================
    print("\n=== 15. Severity derivation ===")
    for status, simulated in [
        ("success", False),
        ("success", True),
        ("partial", False),
        ("failed", False),
        ("timeout", False),
        ("invalid", False),
    ]:
        r = RuntimeResult(status=status, simulated=simulated)
        print(f"  status={status:8s} simulated={simulated}: "
              f"severity={r.severity.value}")
