# src/ml_governance/__init__.py

"""
ML Governance (v5.1)
====================

Carbon-aware governance layer for ML adaptation:

- :class:`AdaptationStrategyClassifier` — recommends fine-tuning strategies
  based on task characteristics and carbon budget.
- :class:`CarbonLedgerService` — tracks carbon budgets and expenditures per
  team / model / experiment (with two-phase reservation).
- :class:`ParameterEfficiencyPolicyEngine` — enforces parameter-efficiency
  policies (soft / moderate / strict, with a justification override path).

Fixes in v5.1 (see review)
--------------------------
  * P0 — Re-exports ``BudgetReservation`` (from ``carbon_ledger``) and
         ``PolicyConfig`` (from ``policy_engine``). Both were public
         API in their submodules' ``__all__`` but not importable from
         the package.
  * P0 — The env-var validation hook is now implemented via
         ``test_smoke()``, which exercises one classify, one
         record-transaction, one enforce, and one integration flow
         (classifier → policy → ledger). The old loop wrapped
         ``logger.info()`` in a ``try`` that could not fail and never
         touched the actual components.
  * P1 — New ``GovernanceError(ValueError)``; ``get_governance_component``
         raises it instead of ``KeyError``, so callers catching
         ``ValueError`` catch the full governance surface.
  * P1 — Registry supports ``override=True``,
         ``unregister_governance_component``, and an
         ``override_governance_component`` context manager for test
         isolation.
  * P1 — ``register_governance_component`` accepts an optional
         ``expected_base`` for plugin type-checking and fixes the
         argument-check order.
  * P1 — Package ``__version__`` plus a submodule version check
         (warns on partial upgrade).
  * P1 — ``import os`` at the top of the file (previously
         ``import os as _os`` mid-file).
  * P2 — ``get_capabilities()``, ``health()``, ``test_smoke()``,
         ``__dir__()``.
  * P3 — ``__main__`` smoke test that exits nonzero on failure;
         ``logger.info`` → ``logger.debug`` on the hot path.

Backward compatibility
----------------------
- All names that were in the v5.0 ``__all__`` remain importable.
- ``get_governance_component`` for an existing name is unchanged; only
  the error type on the *unknown-name* path changed (``KeyError`` →
  ``GovernanceError``). ``GovernanceError`` subclasses ``ValueError``,
  so ``except ValueError`` still works.
- ``register_governance_component(name, cls)`` keeps its positional
  signature; ``override=`` and ``expected_base=`` are keyword-only
  with defaults.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Type

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

#: Oldest submodule version this package can safely drive.
MIN_SUBMODULE_VERSION = "5.1.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class GovernanceError(ValueError):
    """
    Raised for invalid governance registry / lookup operations.

    Submodule-specific errors (``AdaptationClassifierError``,
    ``CarbonLedgerError``, ``PolicyEngineError``) all subclass
    ``ValueError``, so ``except ValueError`` catches all of them plus
    ``GovernanceError``.
    """


# --------------------------------------------------------------------------- #
# Submodule imports + version diagnostics
# --------------------------------------------------------------------------- #
from . import adaptation_classifier as _classifier_module
from . import carbon_ledger as _ledger_module
from . import policy_engine as _policy_module

from .adaptation_classifier import (
    AdaptationClassifierError,
    AdaptationStrategy,
    AdaptationStrategyClassifier,
    ClassifierConfig,
    StrategyRecommendation,
)

from .carbon_ledger import (
    BudgetReservation,
    CarbonLedgerError,
    CarbonLedgerService,
    CarbonTransaction,
    LedgerConfig,
    TeamBudget,
)

from .policy_engine import (
    ParameterEfficiencyPolicyEngine,
    PolicyConfig,
    PolicyDecision,
    PolicyEngineError,
    PolicyMode,
)


def _check_submodule_versions() -> None:
    """Warn (not raise) when submodules are older than the minimum."""
    try:
        from packaging.version import InvalidVersion, Version
    except ImportError:
        logger.debug("packaging not installed; skipping version check")
        return
    required = Version(MIN_SUBMODULE_VERSION)
    for mod, label in (
        (_classifier_module, "adaptation_classifier"),
        (_ledger_module, "carbon_ledger"),
        (_policy_module, "policy_engine"),
    ):
        v = getattr(mod, "__version__", None)
        if not v:
            logger.debug(
                "%s has no __version__; skipping check", label,
            )
            continue
        try:
            installed = Version(v)
        except InvalidVersion:
            logger.warning(
                "%s has non-PEP-440 version %r; skipping check",
                label, v,
            )
            continue
        if installed < required:
            logger.warning(
                "ml_governance requires %s >= %s; found %s. "
                "Some features may be missing.",
                label, MIN_SUBMODULE_VERSION, v,
            )


_check_submodule_versions()


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
_MISSING = object()

GOVERNANCE_REGISTRY: Dict[str, Type[Any]] = {
    "classifier": AdaptationStrategyClassifier,
    "ledger":     CarbonLedgerService,
    "policy":     ParameterEfficiencyPolicyEngine,
}


def register_governance_component(
    name: str,
    cls: Type[Any],
    *,
    override: bool = False,
    expected_base: Optional[type] = None,
) -> None:
    """
    Register a governance component class under ``name``.

    Parameters
    ----------
    name : str
        Non-empty identifier.
    cls : type
        The component class.
    override : bool, default False
        When True, replace any existing registration. When False, a
        conflicting registration raises ``ValueError``; re-registering
        the *same* class is a no-op.
    expected_base : type, optional
        When provided, ``cls`` must be a subclass.

    Raises
    ------
    ValueError
        ``name`` is empty, or a conflicting registration exists without
        ``override=True``.
    TypeError
        ``cls`` isn't a class, or ``expected_base`` isn't matched.
    """
    if not isinstance(name, str) or not name:
        raise ValueError(
            f"name must be a non-empty string, "
            f"got {type(name).__name__}."
        )
    if not isinstance(cls, type):
        raise TypeError(
            f"Expected a class, got {type(cls).__name__}."
        )
    if expected_base is not None and not issubclass(cls, expected_base):
        raise TypeError(
            f"'{name}' must be a subclass of {expected_base.__name__}, "
            f"got {cls.__name__}"
        )
    existing = GOVERNANCE_REGISTRY.get(name, _MISSING)
    if existing is not _MISSING and existing is not cls and not override:
        raise ValueError(
            f"Governance component '{name}' already registered to "
            f"{existing.__name__}. Pass override=True to replace."
        )
    GOVERNANCE_REGISTRY[name] = cls
    logger.debug(
        "Registered governance component '%s' -> %s",
        name, cls.__name__,
    )


def unregister_governance_component(name: str) -> None:
    """Remove the registration under ``name``."""
    if name not in GOVERNANCE_REGISTRY:
        raise GovernanceError(
            f"Unknown governance component '{name}'. "
            f"Available: {sorted(GOVERNANCE_REGISTRY)}"
        )
    GOVERNANCE_REGISTRY.pop(name)
    logger.debug("Unregistered governance component '%s'", name)


@contextmanager
def override_governance_component(
    name: str, cls: Type[Any],
) -> Iterator[None]:
    """
    Temporarily override (or add) a governance registration; restore the
    previous entry on exit. Intended for test isolation::

        with override_governance_component("classifier", FakeClassifier):
            ...
    """
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    prev = GOVERNANCE_REGISTRY.get(name, _MISSING)
    GOVERNANCE_REGISTRY[name] = cls
    try:
        yield
    finally:
        if prev is _MISSING:
            GOVERNANCE_REGISTRY.pop(name, None)
        else:
            GOVERNANCE_REGISTRY[name] = prev  # type: ignore[assignment]


def get_governance_component(name: str) -> Type[Any]:
    """Return the governance component class registered under ``name``."""
    try:
        return GOVERNANCE_REGISTRY[name]
    except KeyError:
        raise GovernanceError(
            f"Unknown governance component '{name}'. "
            f"Available: {sorted(GOVERNANCE_REGISTRY)}"
        ) from None


def list_governance_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {
        n: c.__name__
        for n, c in GOVERNANCE_REGISTRY.items()
        if c is not None
    }


# --------------------------------------------------------------------------- #
# Diagnostics
# --------------------------------------------------------------------------- #
def get_capabilities() -> Dict[str, Any]:
    """Structured capability report suitable for logs and tickets."""
    return {
        "package_version": __version__,
        "submodule_versions": {
            "adaptation_classifier": getattr(
                _classifier_module, "__version__", None,
            ),
            "carbon_ledger": getattr(
                _ledger_module, "__version__", None,
            ),
            "policy_engine": getattr(
                _policy_module, "__version__", None,
            ),
        },
        "min_submodule_version": MIN_SUBMODULE_VERSION,
        "registered_components": list_governance_components(),
        "component_count": len(GOVERNANCE_REGISTRY),
        "features": {
            "adaptation_classifier": True,
            "carbon_ledger": True,
            "policy_engine": True,
            "two_phase_budget": hasattr(
                CarbonLedgerService, "reserve",
            ),
            "policy_override": "justification"
                in ParameterEfficiencyPolicyEngine.enforce.__code__.co_varnames,
        },
    }


def health() -> Dict[str, Any]:
    """
    Compact health report suitable for ``/health`` endpoints and CI gates.

    ``ok`` is True when ``test_smoke()`` passes.
    """
    caps = get_capabilities()
    smoke = test_smoke()
    return {
        "ok": smoke["ok"],
        "package_version": caps["package_version"],
        "component_count": caps["component_count"],
        "instantiation": smoke["steps"],
    }


def test_smoke() -> Dict[str, Any]:
    """
    End-to-end self-check exercising one real call per submodule.

    Steps:

    1. Instantiate ``AdaptationStrategyClassifier`` and classify.
    2. Instantiate ``CarbonLedgerService`` in a temp dir, set a budget,
       record a transaction, and verify the budget updates.
    3. Instantiate ``ParameterEfficiencyPolicyEngine`` and enforce one
       approving and one blocking context.
    4. Integration: classifier → policy → ledger.

    Never raises; returns a structured report.
    """
    import tempfile
    from pathlib import Path

    results: Dict[str, Any] = {
        "ok": False,
        "package_version": __version__,
        "steps": [],
    }

    # --- 1. Adaptation classifier ---
    try:
        classifier = AdaptationStrategyClassifier()
        rec = classifier.classify(
            task_scope="single_task",
            dataset_size=1_000,
            domain_shift="small",
            carbon_budget=1.0,
            target_accuracy=0.9,
            record=False,
        )
        results["steps"].append({
            "name": "classifier",
            "ok": True,
            "strategy": rec.strategy.value,
        })
    except Exception as exc:  # noqa: BLE001
        results["steps"].append({
            "name": "classifier", "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        })

    # --- 2. Carbon ledger ---
    try:
        with tempfile.TemporaryDirectory() as td:
            ledger = CarbonLedgerService(
                ledger_path=Path(td) / "smoke.json",
            )
            ledger.set_team_budget("smoke", "2026-03", 1.0)
            txn = ledger.record_transaction(
                "smoke", "smoke-task", 0.01, 0.005, 0.001,
            )
            budget = ledger.get_team_budget("smoke", "2026-03")
            assert budget is not None
            assert budget.used_kgco2e == 0.005
            results["steps"].append({
                "name": "ledger",
                "ok": True,
                "transaction_id": txn.transaction_id,
                "used_kgco2e": budget.used_kgco2e,
            })
    except Exception as exc:  # noqa: BLE001
        results["steps"].append({
            "name": "ledger", "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        })

    # --- 3. Policy engine ---
    try:
        policy = ParameterEfficiencyPolicyEngine(
            policy_mode=PolicyMode.STRICT,
        )
        d_ok = policy.enforce("lora", "lora", {
            "carbon_budget": 1.0,
            "carbon_remaining": 0.5,
            "dataset_size": 10_000,
            "model_params": 110_000_000,
        })
        d_block = policy.enforce("full_fine_tuning", "lora", {
            "carbon_budget": 1.0,
            "carbon_remaining": 0.001,
            "dataset_size": 500,
            "model_params": 110_000_000,
        })
        assert d_ok.approved is True
        assert d_block.approved is False
        results["steps"].append({
            "name": "policy_engine",
            "ok": True,
            "ok_approved": d_ok.approved,
            "block_approved": d_block.approved,
        })
    except Exception as exc:  # noqa: BLE001
        results["steps"].append({
            "name": "policy_engine", "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        })

    # --- 4. Integration: classifier → policy → ledger ---
    try:
        with tempfile.TemporaryDirectory() as td:
            classifier = AdaptationStrategyClassifier()
            policy = ParameterEfficiencyPolicyEngine(
                policy_mode=PolicyMode.STRICT,
            )
            ledger = CarbonLedgerService(
                ledger_path=Path(td) / "integration.json",
            )
            ledger.set_team_budget("t", "2026-03", 10.0)

            rec = classifier.classify(
                task_scope="single_task",
                dataset_size=20_000,
                domain_shift="large",
                carbon_budget=5.0,
                target_accuracy=0.95,
                record=False,
            )
            d = policy.enforce(rec.strategy.value, rec.strategy.value, {
                "carbon_budget": 10.0,
                "carbon_remaining": 9.5,
                "dataset_size": 20_000,
                "model_params": 110_000_000,
            })
            assert d.approved
            txn = ledger.record_transaction(
                "t", "integration-task",
                0.01, d.carbon_levy + 0.001, 0.0,
            )
            results["steps"].append({
                "name": "integration",
                "ok": True,
                "strategy": rec.strategy.value,
                "transaction_id": txn.transaction_id,
            })
    except Exception as exc:  # noqa: BLE001
        results["steps"].append({
            "name": "integration", "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        })

    results["ok"] = (
        len(results["steps"]) == 4
        and all(s.get("ok") for s in results["steps"])
    )
    return results


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # --- Adaptation classifier ---
    "AdaptationClassifierError",
    "AdaptationStrategy",
    "AdaptationStrategyClassifier",
    "ClassifierConfig",
    "StrategyRecommendation",
    # --- Carbon ledger ---
    "BudgetReservation",
    "CarbonLedgerError",
    "CarbonLedgerService",
    "CarbonTransaction",
    "LedgerConfig",
    "TeamBudget",
    # --- Policy engine ---
    "ParameterEfficiencyPolicyEngine",
    "PolicyConfig",
    "PolicyDecision",
    "PolicyEngineError",
    "PolicyMode",
    # --- Package errors ---
    "GovernanceError",
    # --- Registry ---
    "GOVERNANCE_REGISTRY",
    "get_governance_component",
    "list_governance_components",
    "register_governance_component",
    "unregister_governance_component",
    "override_governance_component",
    # --- Diagnostics ---
    "get_capabilities",
    "health",
    "test_smoke",
    # --- Metadata ---
    "__version__",
]


# --------------------------------------------------------------------------- #
# Optional env-var gate for CI smoke test
# --------------------------------------------------------------------------- #
if os.environ.get("GREEN_AGENT_VALIDATE_GOVERNANCE") == "1":
    _report = test_smoke()
    if not _report["ok"]:
        raise RuntimeError(
            f"ML governance validation failed: {_report}"
        )
    logger.info(
        "ML governance validation passed (package v%s, "
        "%d step(s) exercised).",
        __version__, len(_report["steps"]),
    )


# --------------------------------------------------------------------------- #
# Local smoke test: python -m ml_governance
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import json

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    report = test_smoke()
    print(json.dumps(report, indent=2, default=str))
    assert report["ok"], report

    print("\nRegistered components:", list_governance_components())
    print("Capabilities        :", json.dumps(
        get_capabilities(), indent=2, default=str,
    ))

    # --- Registry override + unregister round-trip ---
    class _FakeComponent:
        pass

    with override_governance_component("classifier", _FakeComponent):
        assert (
            get_governance_component("classifier") is _FakeComponent
        )
    assert (
        get_governance_component("classifier")
        is AdaptationStrategyClassifier
    )

    register_governance_component("temp", _FakeComponent)
    assert get_governance_component("temp") is _FakeComponent
    unregister_governance_component("temp")
    try:
        get_governance_component("temp")
    except GovernanceError:
        pass
    else:
        raise AssertionError(
            "unregister_governance_component did not remove the entry."
        )

    # --- Unknown component raises GovernanceError ---
    try:
        get_governance_component("does_not_exist")
    except GovernanceError as exc:
        print("Unknown component:", exc)
    else:
        raise AssertionError(
            "get_governance_component did not raise."
        )

    # --- Conflict without override ---
    try:
        register_governance_component("classifier", _FakeComponent)
    except ValueError as exc:
        print("Conflict rejected:", exc)
    else:
        raise AssertionError(
            "register_governance_component should have rejected."
        )

    # --- expected_base type guard ---
    try:
        register_governance_component(
            "bad",
            _FakeComponent,
            expected_base=AdaptationStrategyClassifier,
        )
    except TypeError as exc:
        print("expected_base rejected:", exc)
    else:
        raise AssertionError(
            "expected_base should have rejected the non-subclass."
        )

    # --- All v5.1 exports importable ---
    from ml_governance import (  # noqa: F401
        BudgetReservation,
        PolicyConfig,
        GovernanceError,
    )
    print("\nAll v5.1 package-level exports import OK.")
    print("\nSmoke test passed.")
