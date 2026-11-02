"""
QCS — Quantum Context/Continuity Service for Green Agent
=========================================================

A bounded Quantum Expert service that decides whether a quantum or
quantum-distilled route is justified versus a classical route. It
returns recommendations and evidence — never final policy authority.

Boundaries
----------
QCS owns:

    - Quantum task representation and classical-to-quantum translation
    - Backend discovery, selection, execution lifecycle monitoring
    - Quantum state/result metadata, signatures, reproducibility
    - Quantum-specific uncertainty, queue, shot, noise information
    - A decision interface that returns evidence, not decisions

QCS does NOT own (these live in sibling packages):

    - Carbon ledgers              → green-agent-governance
    - Federated training          → green-agent-federation
    - Temporal logic verification → green-agent-governance
    - Human approvals             → green-agent-governance
    - Global coordination         → green-agent-orchestration
    - Charts & dashboards         → green-agent-visualization

Lazy loading
------------
``import qcs`` succeeds on any environment: only package metadata,
feature flags, and helper functions are imported eagerly. All domain
types (contracts, execution, state, distillation, safety) are resolved
lazily on first attribute access via PEP 562 ``__getattr__``.

If a symbol's submodule depends on a package you do not have installed
(e.g. ``torch`` for distillation), accessing that symbol raises a clear
``ImportError`` naming the missing dependency — while every other
symbol continues to work.

Feature flags (safe to read anywhere)::

    >>> import qcs
    >>> qcs.HAS_NUMPY, qcs.HAS_CRYPTO, qcs.HAS_QISKIT
    (True, True, False)
    >>> qcs.require_dep("numpy")  # raises ImportError if missing
    >>> qcs.capability_report()["lazy_exports"]  # list of available names

Stability tiers
---------------
Every public symbol has a tier: ``"stable"``, ``"beta"``, or
``"experimental"``. Query with ``qcs.stability_of("QuantumDistiller")``.
Only ``"stable"`` symbols carry a backward-compatibility promise across
minor releases.

Usage example
-------------
::

    import asyncio
    from qcs import (
        QcsConfig, health, validate_wiring,
        StateBuffer, CoherenceManager, SignaturePreserver,
        QuantumExecutionMonitor,
    )
    from qcs.contracts.task_spec import QuantumTaskSpec, ProblemFamily

    async def run() -> None:
        config = QcsConfig.defaults()  # or QcsConfig.from_yaml("qcs.yaml")

        buffer = StateBuffer(config=config.state_buffer)
        coherence = CoherenceManager()
        preserver = SignaturePreserver(
            carbon_ledger_endpoint="memory://ledger",
            config=config.signature_preserver,
        )
        monitor = QuantumExecutionMonitor(
            buffer, coherence, preserver,
            qpu_endpoint="mock://qpu/local",
            config=config.monitor,
        )

        warnings = validate_wiring(
            buffer=buffer, monitor=monitor, preserver=preserver,
        )
        if warnings:
            raise RuntimeError(f"QCS wiring warnings: {warnings}")

        async with monitor:
            result = await monitor.execute_quantum_circuit(
                buffer_id="buf-x",
                circuit_definition={"shots": 1000},
            )

        print(health({"monitor": monitor, "buffer": buffer}))

    asyncio.run(run())  # doctest: +SKIP

Notes
-----
- The sub-modules take **Protocols** (``StateBufferProtocol``,
  ``CoherenceManagerProtocol``, ``SignaturePreserverProtocol``,
  ``QpuBackendProtocol``) as arguments so tests can inject mocks
  without subclassing the concrete types.
- Sub-modules must **not** import from the top-level ``qcs`` package —
  always import from ``qcs.contracts...`` or ``qcs.<layer>...`` to
  avoid circular imports.
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)


# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# =============================================================================
# Version (source of truth = pyproject.toml via importlib.metadata)
# =============================================================================

try:
    from importlib.metadata import (
        PackageNotFoundError as _PkgNotFound,
        version as _pkg_version,
    )
    try:
        __version__: str = _pkg_version("green-agent-qcs")
    except _PkgNotFound:
        __version__ = "0.0.0+unknown"
except ImportError:  # pragma: no cover — Python < 3.8
    __version__ = "0.0.0+unknown"


def get_version_tuple() -> Tuple[int, int, int]:
    """
    Return (major, minor, patch) — safe for numeric comparison.

    Do NOT compare versions as strings: ``"0.10.0" < "0.9.0"`` is
    True under string ordering, which is a well-known footgun.
    """
    base = __version__.split("+", 1)[0].split("-", 1)[0]
    parts = base.split(".")
    out: List[int] = []
    for p in parts[:3]:
        try:
            out.append(int(p))
        except ValueError:
            out.append(0)
    while len(out) < 3:
        out.append(0)
    return (out[0], out[1], out[2])


# =============================================================================
# Optional-dependency detection
# =============================================================================

def _can_import(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except (ImportError, ValueError):
        return False


def _detect_optional_deps() -> Dict[str, bool]:
    return {
        "numpy": _can_import("numpy"),
        "cryptography": _can_import("cryptography"),
        "qiskit": _can_import("qiskit"),
        "pennylane": _can_import("pennylane"),
        "torch": _can_import("torch"),
        "yaml": _can_import("yaml"),
    }


OPTIONAL_DEPS: Mapping[str, bool] = MappingProxyType(
    _detect_optional_deps()
)
HAS_NUMPY: bool = OPTIONAL_DEPS["numpy"]
HAS_CRYPTO: bool = OPTIONAL_DEPS["cryptography"]
HAS_QISKIT: bool = OPTIONAL_DEPS["qiskit"]
HAS_PENNYLANE: bool = OPTIONAL_DEPS["pennylane"]
HAS_TORCH: bool = OPTIONAL_DEPS["torch"]
HAS_YAML: bool = OPTIONAL_DEPS["yaml"]


def require_dep(name: str) -> None:
    """Raise ``ImportError`` if an optional dependency group is missing."""
    if not OPTIONAL_DEPS.get(name, False):
        raise ImportError(
            f"qcs requires '{name}'. Install it with "
            f"`pip install green-agent-qcs[{name}]`."
        )


# =============================================================================
# Lazy export map
# =============================================================================

_LAZY_IMPORTS: Mapping[str, Tuple[str, str]] = MappingProxyType({
    # --- Contracts ---
    "QuantumTaskSpec": (
        ".contracts.task_spec", "QuantumTaskSpec",
    ),
    "ProblemFamily": (
        ".contracts.task_spec", "ProblemFamily",
    ),
    "QuantumExecutionRequest": (
        ".contracts.execution_request", "QuantumExecutionRequest",
    ),
    "BackendPreference": (
        ".contracts.execution_request", "BackendPreference",
    ),
    "QuantumExecutionResult": (
        ".contracts.execution_result", "QuantumExecutionResult",
    ),
    "ExecutionStatus": (
        ".contracts.execution_result", "ExecutionStatus",
    ),
    "FallbackReason": (
        ".contracts.execution_result", "FallbackReason",
    ),
    "BackendProfile": (
        ".contracts.backend_profile", "BackendProfile",
    ),
    "BackendKind": (
        ".contracts.backend_profile", "BackendKind",
    ),
    "QubitTechnology": (
        ".contracts.backend_profile", "QubitTechnology",
    ),
    "QuantumEvidence": (
        ".contracts.quantum_evidence", "QuantumEvidence",
    ),
    "RouteJustification": (
        ".contracts.quantum_evidence", "RouteJustification",
    ),
    # --- Translation ---
    "ClassicalTokenTranslator": (
        ".translation.classical_token_translator",
        "ClassicalTokenTranslator",
    ),
    "EncodingStrategy": (
        ".translation.encoding_strategy", "EncodingStrategy",
    ),
    "AmplitudeEncoding": (
        ".translation.encoding_strategy", "AmplitudeEncoding",
    ),
    "DecodingStrategy": (
        ".translation.decoding_strategy", "DecodingStrategy",
    ),
    "SampleDecoding": (
        ".translation.decoding_strategy", "SampleDecoding",
    ),
    # --- Execution ---
    "BackendRouter": (
        ".execution.backend_router", "BackendRouter",
    ),
    "QuantumExecutionMonitor": (
        ".execution.quantum_execution_monitor",
        "QuantumExecutionMonitor",
    ),
    "ShotBudget": (
        ".execution.shot_budget", "ShotBudget",
    ),
    "ShotBudgetAllocation": (
        ".execution.shot_budget", "ShotBudgetAllocation",
    ),
    "NoiseCharacterization": (
        ".execution.noise_characterization",
        "NoiseCharacterization",
    ),
    "ClassicalFallback": (
        ".execution.classical_fallback", "ClassicalFallback",
    ),
    "FallbackPlan": (
        ".execution.classical_fallback", "FallbackPlan",
    ),
    # --- State ---
    "StateBuffer": (
        ".state.state_buffer", "StateBuffer",
    ),
    "CoherenceManager": (
        ".state.coherence_manager", "CoherenceManager",
    ),
    "SignaturePreserver": (
        ".state.signature_preserver", "SignaturePreserver",
    ),
    "ExperimentRegistry": (
        ".state.experiment_registry", "ExperimentRegistry",
    ),
    "ExperimentRecord": (
        ".state.experiment_registry", "ExperimentRecord",
    ),
    # --- Distillation ---
    "QuantumDistiller": (
        ".distillation.quantum_distiller", "QuantumDistiller",
    ),
    "StudentModelEvaluator": (
        ".distillation.student_model_evaluator",
        "StudentModelEvaluator",
    ),
    "FidelityQualityChecker": (
        ".distillation.fidelity_quality_checker",
        "FidelityQualityChecker",
    ),
    # --- Safety ---
    "QuantumGuardrails": (
        ".safety.quantum_guardrails", "QuantumGuardrails",
    ),
    "GuardrailVerdict": (
        ".safety.quantum_guardrails", "GuardrailVerdict",
    ),
    "InputValidator": (
        ".safety.input_validation", "InputValidator",
    ),
    "ValidationResult": (
        ".safety.input_validation", "ValidationResult",
    ),
})


# =============================================================================
# Deprecation map (extend as renames happen)
# =============================================================================

_DEPRECATED: Mapping[str, Tuple[str, str]] = MappingProxyType({
    # old_name: (new_name, "removed in vX.Y.Z")
    # "Monitor": ("QuantumExecutionMonitor", "0.3.0"),
})


# =============================================================================
# Stability tiers
# =============================================================================

_STABILITY: Mapping[str, str] = MappingProxyType({
    # Contracts — stable
    "QuantumTaskSpec": "stable",
    "ProblemFamily": "stable",
    "QuantumExecutionRequest": "stable",
    "BackendPreference": "stable",
    "QuantumExecutionResult": "stable",
    "ExecutionStatus": "stable",
    "FallbackReason": "stable",
    "BackendProfile": "stable",
    "BackendKind": "stable",
    "QubitTechnology": "stable",
    "QuantumEvidence": "stable",
    "RouteJustification": "stable",
    # Translation — stable
    "ClassicalTokenTranslator": "stable",
    "EncodingStrategy": "stable",
    "AmplitudeEncoding": "stable",
    "DecodingStrategy": "stable",
    "SampleDecoding": "stable",
    # Execution — mixed
    "BackendRouter": "stable",
    "QuantumExecutionMonitor": "stable",
    "ShotBudget": "stable",
    "ShotBudgetAllocation": "experimental",
    "NoiseCharacterization": "beta",
    "ClassicalFallback": "stable",
    "FallbackPlan": "stable",
    # State — mostly stable
    "StateBuffer": "stable",
    "CoherenceManager": "stable",
    "SignaturePreserver": "stable",
    "ExperimentRegistry": "beta",
    "ExperimentRecord": "beta",
    # Distillation — beta
    "QuantumDistiller": "beta",
    "StudentModelEvaluator": "beta",
    "FidelityQualityChecker": "beta",
    # Safety — stable
    "QuantumGuardrails": "stable",
    "GuardrailVerdict": "stable",
    "InputValidator": "stable",
    "ValidationResult": "stable",
    # Helpers
    "QcsConfig": "stable",
    "capability_report": "stable",
    "get_version_tuple": "stable",
    "health": "stable",
    "require_dep": "stable",
    "stability_of": "stable",
    "validate_wiring": "beta",
})


def stability_of(name: str) -> str:
    """Return ``"stable"``, ``"beta"``, ``"experimental"``, or ``"unknown"``."""
    return _STABILITY.get(name, "unknown")


# =============================================================================
# Static-analysis view (never executed at runtime)
# =============================================================================

if TYPE_CHECKING:
    # Contracts
    from .contracts.task_spec import QuantumTaskSpec, ProblemFamily
    from .contracts.execution_request import (
        QuantumExecutionRequest, BackendPreference,
    )
    from .contracts.execution_result import (
        QuantumExecutionResult, ExecutionStatus, FallbackReason,
    )
    from .contracts.backend_profile import (
        BackendProfile, BackendKind, QubitTechnology,
    )
    from .contracts.quantum_evidence import (
        QuantumEvidence, RouteJustification,
    )
    # Translation
    from .translation.classical_token_translator import (
        ClassicalTokenTranslator,
    )
    from .translation.encoding_strategy import (
        EncodingStrategy, AmplitudeEncoding,
    )
    from .translation.decoding_strategy import (
        DecodingStrategy, SampleDecoding,
    )
    # Execution
    from .execution.backend_router import BackendRouter
    from .execution.quantum_execution_monitor import QuantumExecutionMonitor
    from .execution.shot_budget import ShotBudget, ShotBudgetAllocation
    from .execution.noise_characterization import NoiseCharacterization
    from .execution.classical_fallback import ClassicalFallback, FallbackPlan
    # State
    from .state.state_buffer import StateBuffer
    from .state.coherence_manager import CoherenceManager
    from .state.signature_preserver import SignaturePreserver
    from .state.experiment_registry import (
        ExperimentRegistry, ExperimentRecord,
    )
    # Distillation
    from .distillation.quantum_distiller import QuantumDistiller
    from .distillation.student_model_evaluator import StudentModelEvaluator
    from .distillation.fidelity_quality_checker import FidelityQualityChecker
    # Safety
    from .safety.quantum_guardrails import (
        QuantumGuardrails, GuardrailVerdict,
    )
    from .safety.input_validation import InputValidator, ValidationResult


# =============================================================================
# PEP 562 — lazy resolution
# =============================================================================

def __getattr__(name: str) -> Any:
    """
    Resolve domain symbols lazily on first access.

    Order:
      1. Deprecated names → warn and redirect.
      2. ``_LAZY_IMPORTS`` entries → import submodule, extract attribute,
         cache in the module globals.
      3. Anything else → ``AttributeError``.
    """
    # Dunder lookups (pickle, copy, etc.) must not be intercepted.
    if name.startswith("__") and name.endswith("__"):
        raise AttributeError(name)

    # --- Deprecation shim ---
    if name in _DEPRECATED:
        new_name, removal = _DEPRECATED[name]
        warnings.warn(
            f"qcs.{name} is deprecated; use qcs.{new_name}. "
            f"Will be removed in qcs {removal}.",
            DeprecationWarning,
            stacklevel=2,
        )
        return getattr(sys.modules[__name__], new_name)

    # --- Lazy import ---
    target = _LAZY_IMPORTS.get(name)
    if target is None:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        )
    module_path, attr_name = target
    try:
        mod = importlib.import_module(module_path, __name__)
    except Exception as exc:
        raise ImportError(
            f"qcs.{name} could not be loaded because an optional "
            f"dependency is missing or the submodule failed to import "
            f"({type(exc).__name__}: {exc}). "
            f"Run `qcs.capability_report()` to see which optional "
            f"dependencies are installed, then "
            f"`pip install green-agent-qcs[full]` if needed."
        ) from exc
    value = getattr(mod, attr_name)
    # Cache so subsequent lookups skip the dict lookup.
    globals()[name] = value
    return value


def __dir__() -> List[str]:
    return sorted(
        set(globals().keys())
        | set(_LAZY_IMPORTS.keys())
        | set(_STABILITY.keys())
    )


# =============================================================================
# Aggregate config
# =============================================================================

@dataclass(frozen=True)
class QcsConfig:
    """
    Aggregate config that nests every QCS subsystem's config.

    Field types are ``Any`` at the dataclass level to allow the class
    to be defined before the submodule configs are imported. The
    actual types are:

      * ``monitor``            → ``QuantumMonitorConfig``
      * ``signature_preserver``→ ``SignaturePreserverConfig``
      * ``state_buffer``       → ``StateBufferConfig``

    Use :meth:`defaults` or :meth:`from_yaml` for properly typed
    instances.
    """

    monitor: Any = None
    signature_preserver: Any = None
    state_buffer: Any = None

    @classmethod
    def defaults(cls) -> "QcsConfig":
        """Return a config populated with each subsystem's defaults."""
        from .execution.quantum_execution_monitor import (
            QuantumMonitorConfig,
        )
        from .state.signature_preserver import SignaturePreserverConfig
        from .state.state_buffer import StateBufferConfig
        return cls(
            monitor=QuantumMonitorConfig(),
            signature_preserver=SignaturePreserverConfig(),
            state_buffer=StateBufferConfig(),
        )

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> "QcsConfig":
        """
        Load nested configs from a YAML file of the form::

            monitor:
              qpu_timeout_s: 10.0
            signature_preserver:
              hash_algorithm: sha256
            state_buffer:
              max_buffer_size: 500
        """
        require_dep("yaml")
        import yaml

        from .execution.quantum_execution_monitor import (
            QuantumMonitorConfig,
        )
        from .state.signature_preserver import SignaturePreserverConfig
        from .state.state_buffer import StateBufferConfig

        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            raise ValueError(
                f"QCS config at {path!r} must be a YAML mapping"
            )

        return cls(
            monitor=QuantumMonitorConfig(**raw.get("monitor", {})),
            signature_preserver=SignaturePreserverConfig(
                **raw.get("signature_preserver", {})
            ),
            state_buffer=StateBufferConfig(
                **raw.get("state_buffer", {})
            ),
        )


# =============================================================================
# Aggregate health
# =============================================================================

def health(components: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Aggregate ``.health()`` across QCS subsystems.

    Each component must expose ``health() -> Mapping[str, Any]`` with a
    ``"ready"`` key. Returns a dict with a top-level ``"ready"`` flag
    that is ``True`` only when every component reports ready.

    Suitable for a k8s readiness probe::

        return jsonify(qcs.health({"buffer": buf, "monitor": mon}))
    """
    aggregate: Dict[str, Any] = {"ready": True, "components": {}}
    for name, comp in components.items():
        try:
            comp_health = comp.health()
        except Exception as exc:
            aggregate["ready"] = False
            aggregate["components"][name] = {
                "ready": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
            continue
        aggregate["components"][name] = comp_health
        if not comp_health.get("ready", False):
            aggregate["ready"] = False
    return aggregate


# =============================================================================
# Wiring validation
# =============================================================================

def validate_wiring(
    *,
    buffer: Any,
    monitor: Any,
    preserver: Optional[Any] = None,
) -> List[str]:
    """
    Best-effort check that components were wired from a consistent
    configuration.

    Returns a list of human-readable warnings; empty on success.
    Raises on hard incompatibilities (a caller can opt in by treating
    warnings as errors at the call site).
    """
    warnings_out: List[str] = []

    required_buffer = (
        "get_buffered_state",
        "complete_state",
        "mark_decohered",
        "get_next_for_execution",
    )
    for meth in required_buffer:
        if not callable(getattr(buffer, meth, None)):
            warnings_out.append(
                f"buffer is missing method {meth!r}"
            )

    if getattr(monitor, "state_buffer", None) is not buffer:
        warnings_out.append(
            "monitor.state_buffer is not the buffer passed to "
            "validate_wiring"
        )

    required_monitor = ("execute_quantum_circuit", "health")
    for meth in required_monitor:
        if not callable(getattr(monitor, meth, None)):
            warnings_out.append(
                f"monitor is missing method {meth!r}"
            )

    if preserver is not None:
        if not callable(getattr(preserver, "verify", None)):
            warnings_out.append(
                "preserver is missing method 'verify'"
            )

    return warnings_out


# =============================================================================
# Capability report
# =============================================================================

def capability_report() -> Dict[str, Any]:
    """
    Summarize what is available in this environment.

    Safe to call from any environment — reads only module metadata and
    the optional-dependency probe; never triggers lazy imports.
    """
    return {
        "version": __version__,
        "version_tuple": list(get_version_tuple()),
        "optional_deps": dict(OPTIONAL_DEPS),
        "flags": {
            "HAS_NUMPY": HAS_NUMPY,
            "HAS_CRYPTO": HAS_CRYPTO,
            "HAS_QISKIT": HAS_QISKIT,
            "HAS_PENNYLANE": HAS_PENNYLANE,
            "HAS_TORCH": HAS_TORCH,
            "HAS_YAML": HAS_YAML,
        },
        "lazy_exports": sorted(_LAZY_IMPORTS.keys()),
        "deprecated": sorted(_DEPRECATED.keys()),
        "helpers": [
            "QcsConfig",
            "capability_report",
            "get_version_tuple",
            "health",
            "require_dep",
            "stability_of",
            "validate_wiring",
        ],
    }


# =============================================================================
# __all__
# =============================================================================

__all__: Tuple[str, ...] = (
    # --- Contracts ---
    "QuantumTaskSpec",
    "ProblemFamily",
    "QuantumExecutionRequest",
    "BackendPreference",
    "QuantumExecutionResult",
    "ExecutionStatus",
    "FallbackReason",
    "BackendProfile",
    "BackendKind",
    "QubitTechnology",
    "QuantumEvidence",
    "RouteJustification",
    # --- Translation ---
    "ClassicalTokenTranslator",
    "EncodingStrategy",
    "AmplitudeEncoding",
    "DecodingStrategy",
    "SampleDecoding",
    # --- Execution ---
    "BackendRouter",
    "QuantumExecutionMonitor",
    "ShotBudget",
    "ShotBudgetAllocation",
    "NoiseCharacterization",
    "ClassicalFallback",
    "FallbackPlan",
    # --- State ---
    "StateBuffer",
    "CoherenceManager",
    "SignaturePreserver",
    "ExperimentRegistry",
    "ExperimentRecord",
    # --- Distillation ---
    "QuantumDistiller",
    "StudentModelEvaluator",
    "FidelityQualityChecker",
    # --- Safety ---
    "QuantumGuardrails",
    "GuardrailVerdict",
    "InputValidator",
    "ValidationResult",
    # --- Metadata & helpers ---
    "QcsConfig",
    "capability_report",
    "get_version_tuple",
    "health",
    "require_dep",
    "stability_of",
    "validate_wiring",
)


# =============================================================================
# Run as module: `python -m qcs`
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json

    print(
        json.dumps(
            capability_report(),
            indent=2,
            default=str,
        )
    )
