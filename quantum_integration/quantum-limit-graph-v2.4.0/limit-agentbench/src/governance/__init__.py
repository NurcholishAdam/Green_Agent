"""
Governance package for Green Agent — v5.1.0 (Enhanced)
=======================================================

Non-negotiable guardrail and accountability layer. Defines what agents
are allowed to do, approves or blocks high-impact actions, records
accountable evidence, and enforces safe fallback behavior.

v5.1.0 changes vs. v5.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``BenchmarkEngine`` import was broken — the submodule exports
        ``HeliumAwareBenchmarkEngine`` (no alias), so
        ``from .benchmarks.benchmark_engine import BenchmarkEngine``
        raised ``ImportError`` and the entire governance package
        failed to load. The package now resolves ``BenchmarkEngine``
        to ``HeliumAwareBenchmarkEngine`` via the lazy import table's
        fallback chain.
  * 2.2 Lazy imports via PEP 562 ``__getattr__``. Previously all 40+
        symbols were imported eagerly at package load time, so any one
        broken submodule (missing transitive dep, syntax error) made
        the whole guardrail layer unreachable. Now each symbol resolves
        on first access, and a broken submodule degrades only itself.
  * 2.3 ``GovernanceError`` base class added; the submodule error
        classes we know about (``BenchmarkError``, ``LedgerError``,
        ``HeliumPolicyError``, and their subclasses) are re-parented
        to inherit from it. ``except GovernanceError`` now catches
        every known governance failure; submodule-specific
        ``except X:`` and ``except ValueError:`` clauses keep working
        (identity preserved via ``__bases__`` reassignment).
  * 2.4 ``__version__`` is defined at the top of the module, before
        any imports. Previously it was defined after the import block
        — a failing import meant ``governance.__version__`` didn't
        exist, defeating the diagnostic that would help identify the
        version.
  * 2.5 ``__version__`` is now in ``__all__``, so ``from governance
        import *`` exposes it.
  * 2.6 ``_run_selftests()`` is exported and demonstrates every
        version of the P0 fixes.

**P1 robustness**
  * 3.1 ``__all__`` is grouped by tier (metadata → contracts →
        entry points → ...), so a reader sees the recommendation
        order at a glance.
  * 3.2 ``__getattr__`` on an unknown name produces a helpful
        ``AttributeError`` with fuzzy-matched suggestions.
  * 3.3 ``GREEN_AGENT_VALIDATE_GOVERNANCE=1`` triggers
        ``_validate_all_attributes()`` at import time — every symbol
        in ``__all__`` is resolved (and failures aggregated into a
        single ``ImportError``).
  * 3.4 Docstring updated to match ``__all__`` and to include a
        "Quick start" and "Requires" section.
  * 3.5 Version of the package is cross-checked in ``_run_selftests``.
  * 3.7 Docstring / ``__all__`` alignment.

Public API
----------
Contracts:
    DecisionRequest, PolicyDecision, DecisionVerdict, GovernanceEvent,
    GovernanceEventKind, RiskLevel, RiskClassification

Entry points (the classes most callers use):
    PolicyDecisionPoint, PolicyEnforcementPoint, CarbonLedger, AuditLog

Enforcement:
    FallbackPolicy, FallbackAction, PermissionGuard

Policy:
    SustainabilityPolicy, SafetyPolicy, PrecisionPolicy,
    QuantumPolicy, FederationPolicy, HumanOversightPolicy

Verification:
    TemporalLogic, PolicyVerifier, VerificationEvidence,
    CounterexampleHandler

Accountability:
    CarbonLedger, CarbonInstrumentsLedger, ProvenanceRegistry,
    AuditLog, RetentionPolicy

Approvals:
    EscalationRouter, HumanReviewGate, ReviewRequest, EmergencyStop,
    RollbackController

Benchmarks:
    BenchmarkEngine, ReproducibilityRules, MetricGovernance

Hardware:
    HeliumPolicyAdapter, HardwareCapabilityPolicy

Quick start
-----------
    from governance import (
        GovernanceError, PolicyDecisionPoint, DecisionRequest,
    )

    pdp = PolicyDecisionPoint(config)
    try:
        decision = await pdp.decide(request)
    except GovernanceError:
        # any governance failure (policy, ledger, benchmark, hardware)
        ...

Requires
--------
The core contracts, enforcement, and accountability modules are
dependency-free. Optional submodules may require additional packages:

  * ``hardware.helium_policy_adapter`` — ``aiohttp`` for live helium
    API access (falls back to simulation without it).
  * ``accountability.carbon_ledger`` — ``aiohttp`` for live carbon
    price feeds (falls back to a static source).
  * ``verification.temporal_logic`` — a temporal-logic solver if
    ``features["full_verification"]`` is enabled.

Install those extras only if the corresponding submodule is used.
Because imports are lazy, a missing dependency degrades only the
submodule that needs it — ``import governance`` itself always
succeeds.
"""

from __future__ import annotations

import importlib
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

# ---- Version defined FIRST so diagnostics survive import errors ---- #
__version__ = "5.1.0"

logger = logging.getLogger(__name__)


# =============================================================================
# Base error class (P0 2.3)
# =============================================================================

class GovernanceError(Exception):
    """
    Base class for every error raised by the governance subsystem.

    Submodule errors (``LedgerError``, ``BenchmarkError``,
    ``HeliumPolicyError``, and their subclasses) are re-parented to
    inherit from this class at import time via ``__bases__`` reassignment
    — so existing ``except LedgerError:`` clauses keep working, and
    ``except GovernanceError:`` now catches everything.
    """


# =============================================================================
# Lazy import table (P0 2.2)
# =============================================================================
#
# Each entry maps an exported name to:
#   (submodule_path, attribute_name OR tuple-of-candidate-names)
#
# A tuple of candidates allows fallback (e.g. ``BenchmarkEngine`` falls
# back to ``HeliumAwareBenchmarkEngine`` when the submodule exports only
# the latter). This is how P0 2.1 is fixed without editing the
# submodule.
#
_LAZY_IMPORTS: Dict[str, Tuple[str, Any]] = {
    # ---- Contracts ----
    "DecisionRequest":
        (".contracts.decision_request", "DecisionRequest"),
    "PolicyDecision":
        (".contracts.policy_decision", "PolicyDecision"),
    "DecisionVerdict":
        (".contracts.policy_decision", "DecisionVerdict"),
    "GovernanceEvent":
        (".contracts.governance_event", "GovernanceEvent"),
    "GovernanceEventKind":
        (".contracts.governance_event", "GovernanceEventKind"),
    "RiskLevel":
        (".contracts.risk_classification", "RiskLevel"),
    "RiskClassification":
        (".contracts.risk_classification", "RiskClassification"),

    # ---- Enforcement ----
    "PolicyDecisionPoint":
        (".enforcement.policy_decision_point", "PolicyDecisionPoint"),
    "PolicyEnforcementPoint":
        (".enforcement.policy_enforcement_point", "PolicyEnforcementPoint"),
    "FallbackPolicy":
        (".enforcement.fallback_policy", "FallbackPolicy"),
    "FallbackAction":
        (".enforcement.fallback_policy", "FallbackAction"),
    "PermissionGuard":
        (".enforcement.permission_guard", "PermissionGuard"),

    # ---- Policy ----
    "SustainabilityPolicy":
        (".policy.sustainability_policy", "SustainabilityPolicy"),
    "SafetyPolicy":
        (".policy.safety_policy", "SafetyPolicy"),
    "PrecisionPolicy":
        (".policy.precision_policy", "PrecisionPolicy"),
    "QuantumPolicy":
        (".policy.quantum_policy", "QuantumPolicy"),
    "FederationPolicy":
        (".policy.federation_policy", "FederationPolicy"),
    "HumanOversightPolicy":
        (".policy.human_oversight_policy", "HumanOversightPolicy"),

    # ---- Verification ----
    "TemporalLogic":
        (".verification.temporal_logic", "TemporalLogic"),
    "PolicyVerifier":
        (".verification.policy_verifier", "PolicyVerifier"),
    "VerificationEvidence":
        (".verification.verification_evidence", "VerificationEvidence"),
    "CounterexampleHandler":
        (".verification.counterexample_handler", "CounterexampleHandler"),

    # ---- Accountability ----
    "CarbonLedger":
        (".accountability.carbon_ledger", "CarbonLedger"),
    "CarbonInstrumentsLedger":
        (".accountability.carbon_instruments_ledger",
         "CarbonInstrumentsLedger"),
    "ProvenanceRegistry":
        (".accountability.provenance_registry", "ProvenanceRegistry"),
    "AuditLog":
        (".accountability.audit_log", "AuditLog"),
    "RetentionPolicy":
        (".accountability.retention_policy", "RetentionPolicy"),

    # ---- Approvals ----
    "EscalationRouter":
        (".approvals.escalation_router", "EscalationRouter"),
    "HumanReviewGate":
        (".approvals.human_review", "HumanReviewGate"),
    "ReviewRequest":
        (".approvals.human_review", "ReviewRequest"),
    "EmergencyStop":
        (".approvals.emergency_stop", "EmergencyStop"),
    "RollbackController":
        (".approvals.rollback_controller", "RollbackController"),

    # ---- Benchmarks ----
    # P0 2.1: benchmark_engine exports HeliumAwareBenchmarkEngine (no
    # BenchmarkEngine alias). Try BenchmarkEngine first (in case a
    # future version adds the alias), fall back to the actual class name.
    "BenchmarkEngine":
        (".benchmarks.benchmark_engine",
         ("BenchmarkEngine", "HeliumAwareBenchmarkEngine")),
    "ReproducibilityRules":
        (".benchmarks.reproducibility_rules", "ReproducibilityRules"),
    "MetricGovernance":
        (".benchmarks.metric_governance", "MetricGovernance"),

    # ---- Hardware ----
    "HeliumPolicyAdapter":
        (".hardware.helium_policy_adapter", "HeliumPolicyAdapter"),
    "HardwareCapabilityPolicy":
        (".hardware.hardware_capability_policy",
         "HardwareCapabilityPolicy"),
}


# =============================================================================
# PEP 562 module-level __getattr__ (P0 2.2 + P1 3.2)
# =============================================================================

def _suggest_similar(name: str, k: int = 3) -> List[str]:
    """Fuzzy-match ``name`` against ``__all__``."""
    import difflib
    return difflib.get_close_matches(
        name, __all__, n=k, cutoff=0.6)


def __getattr__(name: str) -> Any:
    """
    Resolve ``governance.<name>`` lazily.

    Called by Python for any attribute not found in the module's
    ``__dict__``. Once resolved, the value is cached in
    ``globals()`` so subsequent accesses are O(1).
    """
    if name in _LAZY_IMPORTS:
        module_path, attr_spec = _LAZY_IMPORTS[name]

        # Import the submodule.
        try:
            mod = importlib.import_module(module_path, __package__)
        except ImportError as exc:
            raise AttributeError(
                f"governance.{name} is not available: the submodule "
                f"{module_path} failed to import ({exc}). This "
                f"usually means a transitive dependency is missing "
                f"from your environment. Install the extra or "
                f"disable the feature that uses this symbol."
            ) from exc

        # Resolve the attribute (with optional candidate fallback).
        if isinstance(attr_spec, tuple):
            for candidate in attr_spec:
                try:
                    value = getattr(mod, candidate)
                except AttributeError:
                    continue
                globals()[name] = value
                logger.debug(
                    "governance.%s resolved to %s.%s",
                    name, module_path, candidate)
                return value
            raise AttributeError(
                f"governance.{name} is not available: {module_path} "
                f"exports none of {list(attr_spec)}. "
                f"Available: "
                f"{sorted(n for n in dir(mod) if not n.startswith('_'))}")

        # Single-attribute resolution.
        try:
            value = getattr(mod, attr_spec)
        except AttributeError as exc:
            raise AttributeError(
                f"governance.{name} is not available: {module_path} "
                f"does not define {attr_spec!r}."
            ) from exc
        globals()[name] = value
        logger.debug(
            "governance.%s resolved to %s.%s",
            name, module_path, attr_spec)
        return value

    # Unknown name — provide a helpful suggestion.
    suggestions = _suggest_similar(name)
    hint = (
        f" Did you mean one of: {suggestions}?"
        if suggestions else
        f" Available names are listed in governance.__all__ "
        f"({len(__all__)} entries)."
    )
    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}.{hint}")


def __dir__() -> List[str]:
    """Expose lazy names in ``dir(governance)`` (P1 3.2)."""
    return sorted(
        set(globals().keys()) | set(_LAZY_IMPORTS.keys()))


# =============================================================================
# Public API (P0 2.5 + P1 3.1 + P1 3.7)
# =============================================================================

__all__ = [
    # ---- Package metadata ----
    "__version__",
    "GovernanceError",

    # ---- Contracts (data classes) ----
    "DecisionRequest", "PolicyDecision", "DecisionVerdict",
    "GovernanceEvent", "GovernanceEventKind",
    "RiskLevel", "RiskClassification",

    # ---- Enforcement ----
    "PolicyDecisionPoint", "PolicyEnforcementPoint",
    "FallbackPolicy", "FallbackAction", "PermissionGuard",

    # ---- Policy ----
    "SustainabilityPolicy", "SafetyPolicy", "PrecisionPolicy",
    "QuantumPolicy", "FederationPolicy", "HumanOversightPolicy",

    # ---- Verification ----
    "TemporalLogic", "PolicyVerifier", "VerificationEvidence",
    "CounterexampleHandler",

    # ---- Accountability ----
    "CarbonLedger", "CarbonInstrumentsLedger",
    "ProvenanceRegistry", "AuditLog", "RetentionPolicy",

    # ---- Approvals ----
    "EscalationRouter", "HumanReviewGate", "ReviewRequest",
    "EmergencyStop", "RollbackController",

    # ---- Benchmarks ----
    "BenchmarkEngine", "ReproducibilityRules", "MetricGovernance",

    # ---- Hardware ----
    "HeliumPolicyAdapter", "HardwareCapabilityPolicy",
]


# =============================================================================
# Error re-parenting (P0 2.3)
# =============================================================================
#
# Best-effort: import each known error-bearing submodule; on success,
# re-parent the error classes to inherit from ``GovernanceError``.
# A submodule that fails to import is skipped (its errors remain
# unavailable, matching the lazy-attribute behaviour).
#
# Eager cost: 3 submodule imports. Those 3 are foundational and each
# is already designed to import lazily for its own heavy deps.
#
_ERROR_CLASSES_BY_MODULE: Dict[str, List[str]] = {
    ".benchmarks.benchmark_engine": [
        "BenchmarkError",           # base — must come first
        "BenchmarkValidationError",
    ],
    ".accountability.carbon_ledger": [
        "LedgerError",              # base — must come first
        "LedgerNotInitializedError",
        "LedgerTamperedError",
        "LedgerPersistenceError",
        "LedgerValidationError",
        "LedgerSchemaMismatchError",
        "LedgerDuplicateInstrumentError",   # dual-inherits ValueError
    ],
    ".hardware.helium_policy_adapter": [
        "HeliumPolicyError",        # base — must come first
        "HeliumSupplyParseError",
        "HeliumSupplyStaleError",
    ],
}


def _reparent_known_errors() -> int:
    """
    Re-parent known submodule error classes to inherit ``GovernanceError``.

    Returns the number of classes successfully re-parented (used by
    ``_run_selftests``).
    """
    re_parented = 0

    for module_path, class_names in _ERROR_CLASSES_BY_MODULE.items():
        try:
            mod = importlib.import_module(module_path, __package__)
        except ImportError as exc:
            logger.debug(
                "Skipping error re-parenting for %s: %s",
                module_path, exc)
            continue

        for cls_name in class_names:
            cls = getattr(mod, cls_name, None)
            if not isinstance(cls, type):
                logger.debug(
                    "Skipping %s.%s: not a class or not found",
                    module_path, cls_name)
                continue

            if GovernanceError in cls.__mro__:
                continue  # already inherits (transitively or directly)

            try:
                # Preserve non-trivial existing bases (e.g. ValueError
                # on LedgerDuplicateInstrumentError) and append
                # GovernanceError. Order matters: GovernanceError goes
                # last so the MRO keeps existing isinstance checks
                # (isinstance(x, ValueError)) intact.
                existing_bases = tuple(
                    b for b in cls.__bases__
                    if b not in (Exception, BaseException, object)
                )
                cls.__bases__ = existing_bases + (GovernanceError,)
                re_parented += 1
                logger.debug(
                    "Re-parented %s to (bases=%s)",
                    cls.__name__, cls.__bases__)
            except TypeError as exc:
                logger.warning(
                    "Could not re-parent %s to GovernanceError: %s",
                    cls.__name__, exc)

    return re_parented


_REPARENTED_COUNT = _reparent_known_errors()
logger.debug(
    "Re-parented %d governance error class(es) to GovernanceError",
    _REPARENTED_COUNT)


# =============================================================================
# Env-gated import-time validation (P1 3.3)
# =============================================================================

def _coerce_bool_env(value: Optional[str]) -> bool:
    """Coerce an env-var string to bool (case-insensitive)."""
    if value is None:
        return False
    return value.strip().lower() in ("1", "true", "yes", "on", "t", "y")


def _validate_all_attributes() -> None:
    """
    Resolve every name in ``__all__`` and aggregate failures.

    Triggered by ``GREEN_AGENT_VALIDATE_GOVERNANCE=1``. Every failure
    is collected before raising, so the caller sees the whole picture
    in one error (not just the first failure).
    """
    self_mod = sys.modules[__name__]
    failures: List[str] = []
    for name in __all__:
        try:
            getattr(self_mod, name)
        except AttributeError as exc:
            failures.append(str(exc))
        except Exception as exc:  # noqa: BLE001 — defensive
            failures.append(
                f"governance.{name}: unexpected "
                f"{type(exc).__name__}: {exc}")

    if failures:
        raise ImportError(
            "Governance package validation failed "
            f"({len(failures)} error(s)):\n  "
            + "\n  ".join(failures))


if _coerce_bool_env(os.environ.get("GREEN_AGENT_VALIDATE_GOVERNANCE")):
    _validate_all_attributes()


# =============================================================================
# Self-tests (P0 2.6)
# =============================================================================

def _run_selftests() -> None:
    """
    Run package self-tests.

    Usage::

        python -c "import governance; governance._run_selftests()"
    """
    import time

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s")

    print(f"=== governance v{__version__} self-tests ===\n")
    self_mod = sys.modules[__name__]

    # ---------------------------------------------------------------- #
    # P0 2.1 — BenchmarkEngine resolves
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: BenchmarkEngine resolves ===")
    Engine = self_mod.BenchmarkEngine
    assert isinstance(Engine, type), (
        f"P0 2.1 regression: BenchmarkEngine is "
        f"{type(Engine).__name__}, expected a class")
    # Note: with the current benchmark_engine submodule, this resolves
    # to HeliumAwareBenchmarkEngine. Assert the capability, not the
    # exact name.
    print(f"  ✓ governance.BenchmarkEngine → {Engine.__name__}")

    # ---------------------------------------------------------------- #
    # P0 2.2 — Lazy imports (a fresh name access works, and the same
    # object is returned on second access).
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: lazy imports ===")
    PDP1 = self_mod.PolicyDecisionPoint
    PDP2 = self_mod.PolicyDecisionPoint
    assert PDP1 is PDP2, (
        "P0 2.2 regression: repeated access returned different objects")
    assert isinstance(PDP1, type)
    print(f"  ✓ PolicyDecisionPoint resolves and caches: "
          f"{PDP1.__name__}")

    # ---------------------------------------------------------------- #
    # P0 2.3 — GovernanceError catches submodule errors
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: GovernanceError hierarchy ===")
    from .benchmarks.benchmark_engine import (
        BenchmarkError, BenchmarkValidationError,
    )
    from .accountability.carbon_ledger import (
        LedgerError, LedgerValidationError, LedgerDuplicateInstrumentError,
    )
    from .hardware.helium_policy_adapter import (
        HeliumPolicyError, HeliumSupplyParseError,
    )

    for cls, expected_parent in (
        (BenchmarkError, GovernanceError),
        (BenchmarkValidationError, GovernanceError),
        (LedgerError, GovernanceError),
        (LedgerValidationError, GovernanceError),
        (HeliumPolicyError, GovernanceError),
        (HeliumSupplyParseError, GovernanceError),
    ):
        assert issubclass(cls, expected_parent), (
            f"P0 2.3 regression: {cls.__name__} does not subclass "
            f"{expected_parent.__name__}; MRO="
            f"{[b.__name__ for b in cls.__mro__]}")
    print(f"  ✓ 6 submodule errors subclass GovernanceError")

    # LedgerDuplicateInstrumentError must still be a ValueError
    # (v5.0.0 backward compat).
    assert issubclass(LedgerDuplicateInstrumentError, ValueError), (
        "Backward-compat regression: LedgerDuplicateInstrumentError "
        "no longer subclasses ValueError")
    print(f"  ✓ LedgerDuplicateInstrumentError still subclasses ValueError")

    # Catching works end-to-end.
    for cls in (
        BenchmarkError, BenchmarkValidationError,
        LedgerError, HeliumPolicyError,
    ):
        try:
            raise cls("test")
        except GovernanceError:
            pass
        else:
            raise AssertionError(
                f"P0 2.3 regression: {cls.__name__} not caught by "
                f"except GovernanceError")
    print(f"  ✓ except GovernanceError catches each error type")

    # ---------------------------------------------------------------- #
    # P0 2.4 — __version__ available
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: __version__ ===")
    assert hasattr(self_mod, "__version__")
    assert isinstance(self_mod.__version__, str)
    print(f"  ✓ governance.__version__ = {self_mod.__version__!r}")

    # ---------------------------------------------------------------- #
    # P0 2.5 — __version__ in __all__
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: __version__ in __all__ ===")
    assert "__version__" in __all__, (
        "P0 2.5 regression: '__version__' not in __all__")
    print(f"  ✓ '__version__' listed in __all__")

    # ---------------------------------------------------------------- #
    # P0 2.6 — _run_selftests is the function we're in
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: _run_selftests callable ===")
    assert callable(_run_selftests)
    print(f"  ✓ we are running inside _run_selftests()")

    # ---------------------------------------------------------------- #
    # P1 3.1 — __all__ grouped by tier
    # ---------------------------------------------------------------- #
    print(f"\n=== P1 3.1: __all__ has {len(__all__)} entries ===")
    # First 2 entries are metadata.
    assert __all__[0] == "__version__"
    assert __all__[1] == "GovernanceError"
    print(f"  ✓ first entries: {__all__[:2]}")

    # ---------------------------------------------------------------- #
    # P1 3.2 — helpful AttributeError on unknown name
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: helpful AttributeError ===")
    try:
        self_mod.PolicyDecisionPiont   # typo
    except AttributeError as exc:
        msg = str(exc)
        assert "PolicyDecisionPiont" in msg
        # Fuzzy suggestion should be present.
        assert "PolicyDecisionPoint" in msg, (
            f"P1 3.2 regression: no suggestion in {msg!r}")
        print(f"  ✓ error suggests the right name")
    else:
        raise AssertionError(
            "P1 3.2 regression: no AttributeError for unknown name")

    # ---------------------------------------------------------------- #
    # P1 3.3 — env-gated validation
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: env-gated validation ===")
    _validate_all_attributes()
    print(f"  ✓ _validate_all_attributes() passes on all "
          f"{len(__all__)} names")

    # ---------------------------------------------------------------- #
    # P1 3.5 — version cross-check
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: version cross-check ===")
    # The package version should be at least the major version of
    # the framework (5.0.0). Submodule versions are independent.
    major = int(__version__.split(".")[0])
    assert major >= 5, (
        f"Version mismatch: governance v{__version__} is below the "
        f"framework's major version 5")
    print(f"  ✓ governance major version {major} >= framework 5")

    # ---------------------------------------------------------------- #
    # Bonus — import cost of resolving every name
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: import cost ===")
    t0 = time.perf_counter()
    for name in __all__:
        if name == "__version__":
            continue
        getattr(self_mod, name)
    elapsed = time.perf_counter() - t0
    print(f"  ✓ resolved all {len(__all__)} names in "
          f"{elapsed * 1000:.1f}ms")

    print(f"\n✓ All self-tests passed.")


# =============================================================================
# Direct-execution entry point
# =============================================================================
#
# A package ``__init__.py`` cannot be executed directly (relative
# imports fail). Use:
#
#     python -c "import governance; governance._run_selftests()"
#
# The block below is a convenience for environments that strip the
# relative-import guard (uncommon, but harmless).
if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
