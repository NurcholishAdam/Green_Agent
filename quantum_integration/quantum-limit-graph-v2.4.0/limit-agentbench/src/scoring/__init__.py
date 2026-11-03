# src/scoring/__init__.py

"""
Scoring modules for Green_Agent
===============================

Robust scoring and failure-classification for agent evaluation:

- :class:`FailureClassifier` — categorizes agent failures for debugging.
- :class:`RobustSustainabilityScorer` — handles all failure modes gracefully
  and returns consistent scoring dictionaries.

The two sub-modules form a **pipeline**: the classifier labels an error,
the scorer produces a reward for the agent result that accompanies it.
The façade exposes both, plus:

- :func:`crosswalk_category` — translate a ``RobustSustainabilityScorer``
  failure category (``{timeout, error, oom, unknown}``) into its closest
  :class:`FailureClassifier` category. ``"error"`` → ``"unknown"`` is the
  only non-trivial mapping, since :class:`FailureCategory` has no
  ``"error"`` member (its closest is ``"tool_error"``).

Enhancements
------------
- ``_MissingComponent`` stub — missing sub-modules raise a clear
  ``ImportError`` on first use instead of returning ``None``.
- ``ScoringComponent`` protocol — structural check for the two adapters;
  registry rejects classes that do not implement ``to_dict`` /
  ``statistics``.
- Read-only public views: ``SCORING_REGISTRY`` and
  ``SCORING_AVAILABILITY`` are ``MappingProxyType``; internal dicts are
  private.
- ``ScoringRegistryError(ValueError)`` — narrow error type for registry
  operations.
- Full re-export of the enhanced sub-module public API:
  ``ClassificationRecord``, ``ClassifyResult``, ``ClassifierStatistics``,
  ``FAILURE_CATEGORIES``, ``ScoreMetrics``, ``ScorerStatistics``,
  ``KNOWN_FAILURE_CATEGORIES``.
- Category crosswalk: ``CATEGORY_VOCABULARIES`` and
  ``crosswalk_category()`` — surfaces the divergence between the two
  vocabularies so downstream code can join them safely.
- ``create_classifier()`` / ``create_scorer()`` factories — validated
  construction with the registry's default components.
- ``health()`` — aggregate availability + registry + category summary.
- ``_registry_lock`` guards registration and lookup.
- ``__dir__`` for clean REPL discoverability.
- Optional import-time validation via ``GREEN_AGENT_VALIDATE_SCORING=1``
  (now checks ``ScoringComponent`` conformance, not just ``callable``).
- ``__version__`` constant.

Requires Python 3.8+. ``runtime_checkable`` Protocols work on 3.8+.
"""

from __future__ import annotations

import importlib
import logging
import os
import threading
from types import MappingProxyType
from typing import (
    Any, Dict, List, Mapping, Optional, Protocol, Tuple, Type, Union,
    runtime_checkable,
)

logger = logging.getLogger(__name__)

__version__ = "2.4.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ScoringRegistryError(ValueError):
    """Raised for invalid scoring-registry operations."""


# --------------------------------------------------------------------------- #
# Minimal component contract
# --------------------------------------------------------------------------- #
@runtime_checkable
class ScoringComponent(Protocol):
    """
    Structural interface shared by every scoring component.

    Both :class:`FailureClassifier` and :class:`RobustSustainabilityScorer`
    conform: they expose ``to_dict`` (serialization) and ``statistics``
    (JSON-safe diagnostic snapshot). Custom classes registered via
    :func:`register_scoring_component` are checked against this protocol
    (method presence only — signatures are not inspected).
    """

    def to_dict(self, *, include_history: bool = False) -> Mapping[str, Any]:
        ...

    def statistics(self) -> Mapping[str, Any]:
        ...


# --------------------------------------------------------------------------- #
# Missing-component stub
# --------------------------------------------------------------------------- #
class _MissingComponent:
    """
    Placeholder for an optional component whose sub-module failed to import.

    Raises :class:`ImportError` on first use instead of silently returning
    ``None``. Handles call, attribute access, subscript, iteration, and
    ``in`` tests — the operations most likely to be attempted on a
    component that turned out to be unavailable.
    """

    __slots__ = ("_name", "_reason")

    def __init__(self, name: str, reason: BaseException) -> None:
        self._name = name
        self._reason = reason

    def _fail(self) -> None:
        raise ImportError(
            f"Scoring component '{self._name}' is unavailable: {self._reason}"
        ) from self._reason

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self._fail()

    def __getattr__(self, item: str) -> Any:
        self._fail()

    def __getitem__(self, key: Any) -> Any:
        self._fail()

    def __iter__(self):
        self._fail()

    def __contains__(self, item: Any) -> bool:
        self._fail()

    def __bool__(self) -> bool:
        self._fail()

    def __repr__(self) -> str:
        return f"<unavailable scoring component {self._name!r}>"


# --------------------------------------------------------------------------- #
# Read-only public views
# --------------------------------------------------------------------------- #
_availability: Dict[str, bool] = {}
SCORING_AVAILABILITY: Mapping[str, bool] = MappingProxyType(_availability)

_registry: Dict[str, Union[Type[Any], _MissingComponent]] = {}
SCORING_REGISTRY: Mapping[str, Any] = MappingProxyType(_registry)
_registry_lock = threading.RLock()


# --------------------------------------------------------------------------- #
# Guarded loader
# --------------------------------------------------------------------------- #
def _load(module: str, names: Tuple[str, ...]) -> Dict[str, Any]:
    """
    Import ``names`` from ``.{module}``; return ``_MissingComponent``
    stubs on ``ImportError``.
    """
    try:
        mod = importlib.import_module(f".{module}", package=__package__)
        _availability[module] = True
        return {n: getattr(mod, n) for n in names}
    except ImportError as exc:
        logger.info("%s unavailable: %s", module, exc)
        _availability[module] = False
        return {n: _MissingComponent(n, exc) for n in names}


# --------------------------------------------------------------------------- #
# Load every scoring sub-module
# --------------------------------------------------------------------------- #
globals().update(_load(
    "failure_classifier",
    (
        "ClassificationRecord", "ClassifyResult", "ClassifierStatistics",
        "FAILURE_CATEGORIES", "FailureCategory", "FailureClassifier",
        "FailureClassifierConfig", "FailureClassifierError",
    ),
))

globals().update(_load(
    "robust_scorer",
    (
        "KNOWN_FAILURE_CATEGORIES", "RobustScorerConfig",
        "RobustScorerError", "RobustSustainabilityScorer",
        "ScoreMetrics", "ScorerStatistics", "ScoringResult",
    ),
))


# --------------------------------------------------------------------------- #
# Category crosswalk
# --------------------------------------------------------------------------- #
# Robust scorer's vocabulary uses "error" where failure_classifier uses
# "tool_error" (or, more conservatively, "unknown"). The classifier has
# categories the scorer cannot produce (invalid_output, hallucination,
# energy_exceeded). This crosswalk maps scorer categories to classifier
# categories for downstream joins.
_SCORE_TO_CLASSIFIER: Mapping[str, str] = MappingProxyType({
    "timeout": "timeout",
    "oom":     "oom",
    "error":   "unknown",   # conservative: nothing in FailureCategory is "error"
    "unknown": "unknown",
})

# Read-only view of both vocabularies side by side, for operators and
# downstream consumers who need to join records across the two modules.
CATEGORY_VOCABULARIES: Mapping[str, Mapping[str, str]] = MappingProxyType({})


def _build_vocabulary_view() -> Mapping[str, Mapping[str, str]]:
    """Assemble the cross-vocabulary view, tolerating missing submodules."""
    view: Dict[str, Mapping[str, str]] = {}
    if not isinstance(FAILURE_CATEGORIES, _MissingComponent):
        view["failure_classifier"] = FAILURE_CATEGORIES
    if not isinstance(KNOWN_FAILURE_CATEGORIES, _MissingComponent):
        view["robust_scorer"] = KNOWN_FAILURE_CATEGORIES
    return MappingProxyType(view)


# Rebind after load (Python doesn't allow reassigning a module-level
# constant cleanly in a `from __future__ import annotations` file without
# this dance; the binding is stable for the life of the process).
CATEGORY_VOCABULARIES = _build_vocabulary_view()


def crosswalk_category(category: str) -> str:
    """
    Map a ``RobustSustainabilityScorer`` failure category to its closest
    ``FailureClassifier`` category.

    - ``"timeout"`` → ``"timeout"``
    - ``"oom"``     → ``"oom"``
    - ``"error"``   → ``"unknown"`` (no exact match in ``FailureCategory``)
    - ``"unknown"`` → ``"unknown"``
    - any other input is returned unchanged, so this function doubles as
      an identity mapping for classifier-side categories.

    Non-string inputs raise :class:`TypeError`.
    """
    if not isinstance(category, str):
        raise TypeError(
            f"category must be a string, got {type(category).__name__}."
        )
    return _SCORE_TO_CLASSIFIER.get(category, category)


# --------------------------------------------------------------------------- #
# Registry API
# --------------------------------------------------------------------------- #
def _register_if_available(name: str, cls: Any) -> None:
    if isinstance(cls, _MissingComponent):
        logger.debug(
            "Scoring component '%s' unavailable; skipping registration.",
            name,
        )
        return
    if not (isinstance(cls, type) and issubclass(cls, ScoringComponent)):
        # A default registration failing the protocol check means the
        # user's environment has a mismatched submodule. Warn loudly.
        logger.warning(
            "Default scoring component '%s' (%r) does not satisfy "
            "ScoringComponent; not registering.",
            name, cls,
        )
        return
    with _registry_lock:
        _registry[name] = cls


_register_if_available("classifier", FailureClassifier)
_register_if_available("scorer", RobustSustainabilityScorer)


def register_scoring_component(
    name: str,
    cls: Type[Any],
    *,
    strict: bool = True,
) -> None:
    """
    Register a scoring component class under ``name``.

    Parameters
    ----------
    name : str
        Registry key. Must be a non-empty string.
    cls : type
        Class to register.
    strict : bool, default True
        If True, raise :class:`ScoringRegistryError` when ``cls`` does not
        satisfy :class:`ScoringComponent`.

    Raises
    ------
    ScoringRegistryError
        If ``name`` is empty, already registered to a different class, or
        (with ``strict=True``) ``cls`` does not satisfy
        :class:`ScoringComponent`.
    TypeError
        If ``cls`` is not a class.
    """
    if not isinstance(name, str) or not name:
        raise ScoringRegistryError(
            "Component name must be a non-empty string."
        )
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    if strict and not issubclass(cls, ScoringComponent):
        raise ScoringRegistryError(
            f"{cls.__name__} does not satisfy ScoringComponent "
            f"(missing to_dict / statistics)."
        )
    with _registry_lock:
        existing = _registry.get(name)
        if isinstance(existing, _MissingComponent):
            existing = None
        if existing is not None and existing is not cls:
            raise ScoringRegistryError(
                f"Scoring component '{name}' already registered to "
                f"{getattr(existing, '__name__', existing)!r}."
            )
        _registry[name] = cls
        logger.debug(
            "Registered scoring component '%s' -> %s", name, cls.__name__,
        )


def get_scoring_component(name: str) -> Type[Any]:
    """
    Return the scoring component class registered under ``name``.

    Raises
    ------
    ScoringRegistryError
        If ``name`` is not registered, or is registered but its submodule
        failed to import.
    """
    with _registry_lock:
        try:
            cls = _registry[name]
        except KeyError as exc:
            raise ScoringRegistryError(
                f"Unknown scoring component '{name}'. "
                f"Available: {sorted(_registry)}"
            ) from exc
    if isinstance(cls, _MissingComponent):
        raise ScoringRegistryError(
            f"Scoring component '{name}' is registered but unavailable."
        )
    return cls


def list_scoring_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    with _registry_lock:
        return {
            n: getattr(c, "__name__", type(c).__name__)
            for n, c in _registry.items()
        }


# --------------------------------------------------------------------------- #
# Convenience factories
# --------------------------------------------------------------------------- #
def create_classifier(
    *,
    config: Optional[Any] = None,
    strict: bool = True,
) -> ScoringComponent:
    """
    Construct a :class:`FailureClassifier` using the registered component.

    Raises :class:`ScoringRegistryError` if ``classifier`` is not
    registered.
    """
    cls = get_scoring_component("classifier")
    if config is not None:
        return cls(config=config, strict=strict)  # type: ignore[call-arg]
    return cls(strict=strict)  # type: ignore[call-arg]


def create_scorer(
    *,
    config: Optional[Any] = None,
    strict: bool = True,
) -> ScoringComponent:
    """
    Construct a :class:`RobustSustainabilityScorer` using the registered
    component.

    Raises :class:`ScoringRegistryError` if ``scorer`` is not registered.
    """
    cls = get_scoring_component("scorer")
    if config is not None:
        return cls(config=config, strict=strict)  # type: ignore[call-arg]
    return cls(strict=strict)  # type: ignore[call-arg]


# --------------------------------------------------------------------------- #
# Health / diagnostics
# --------------------------------------------------------------------------- #
def health() -> Dict[str, Any]:
    """
    Return an aggregate summary suitable for operator dashboards and bug
    reports.
    """
    with _registry_lock:
        registered = sorted(_registry)
    return {
        "version": __version__,
        "submodules": dict(_availability),
        "registered": registered,
        "category_vocabularies": {
            k: sorted(v) for k, v in CATEGORY_VOCABULARIES.items()
        },
    }


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if os.environ.get("GREEN_AGENT_VALIDATE_SCORING") == "1":
    for _module, _ok in _availability.items():
        _level = logging.INFO if _ok else logging.WARNING
        logger.log(
            _level, "Scoring submodule %-22s : %s",
            _module, "available" if _ok else "MISSING",
        )

    for _name, _cls in list(_registry.items()):
        if isinstance(_cls, _MissingComponent):
            logger.warning(
                "Scoring component '%s' is unavailable: %s",
                _name, _cls._reason,
            )
            continue
        if not issubclass(_cls, ScoringComponent):
            logger.warning(
                "Scoring component '%s' (%s) does not satisfy "
                "ScoringComponent.",
                _name, _cls.__name__,
            )
            continue
        if not (_cls.__doc__ or "").strip():
            logger.warning(
                "Scoring component '%s' (%s) has no docstring.",
                _name, _cls.__name__,
            )
        else:
            logger.debug(
                "Scoring component '%s' (%s) OK.", _name, _cls.__name__,
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = (
    "__version__",
    # Errors / protocol
    "ScoringRegistryError",
    "ScoringComponent",
    # failure_classifier
    "ClassificationRecord",
    "ClassifyResult",
    "ClassifierStatistics",
    "FAILURE_CATEGORIES",
    "FailureCategory",
    "FailureClassifier",
    "FailureClassifierConfig",
    "FailureClassifierError",
    # robust_scorer
    "KNOWN_FAILURE_CATEGORIES",
    "RobustScorerConfig",
    "RobustScorerError",
    "RobustSustainabilityScorer",
    "ScoreMetrics",
    "ScorerStatistics",
    "ScoringResult",
    # registry / manifest
    "SCORING_REGISTRY",
    "SCORING_AVAILABILITY",
    "get_scoring_component",
    "list_scoring_components",
    "register_scoring_component",
    # factories
    "create_classifier",
    "create_scorer",
    # crosswalk / health
    "CATEGORY_VOCABULARIES",
    "crosswalk_category",
    "health",
)


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m scoring
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    print("version     :", __version__)
    print("health      :", health())

    # ---- Missing-component stub ------------------------------------- #
    stub = _MissingComponent("stub", ImportError("simulated"))
    for probe in ("call", "attr", "getitem", "iter", "bool"):
        try:
            if probe == "call":
                stub()
            elif probe == "attr":
                stub.some_attr
            elif probe == "getitem":
                stub["key"]
            elif probe == "iter":
                list(stub)
            elif probe == "bool":
                if stub:
                    pass
        except ImportError as exc:
            print(f"stub {probe:<8}: {exc}")

    # ---- Re-exports present ----------------------------------------- #
    for name in (
        "ScoreMetrics", "ScorerStatistics",
        "ClassificationRecord", "ClassifyResult", "ClassifierStatistics",
        "FAILURE_CATEGORIES", "KNOWN_FAILURE_CATEGORIES",
        "FailureClassifier", "RobustSustainabilityScorer",
    ):
        assert name in __all__, f"missing from __all__: {name}"
        assert getattr(__import__(__name__), name) is not None
    print("re-exports : OK")

    # ---- Registry is frozen ----------------------------------------- #
    try:
        SCORING_REGISTRY["hack"] = object     # type: ignore[index]
        print("registry    : MUTABLE — this is a bug")
    except TypeError:
        print("registry    : frozen  [OK]")

    try:
        SCORING_AVAILABILITY["hack"] = True   # type: ignore[index]
        print("avail       : MUTABLE — this is a bug")
    except TypeError:
        print("avail       : frozen  [OK]")

    # ---- Registry operations ---------------------------------------- #
    print("components  :", list_scoring_components())
    assert "classifier" in SCORING_REGISTRY
    assert "scorer" in SCORING_REGISTRY

    try:
        get_scoring_component("nope")
    except ScoringRegistryError as exc:
        print("unknown     :", exc)

    # ---- Factories -------------------------------------------------- #
    scorer = create_scorer()
    classifier = create_classifier()
    print("scorer      :", type(scorer).__name__)
    print("classifier  :", type(classifier).__name__)

    # ---- Category crosswalk ----------------------------------------- #
    assert crosswalk_category("timeout") == "timeout"
    assert crosswalk_category("oom") == "oom"
    assert crosswalk_category("error") == "unknown"
    assert crosswalk_category("unknown") == "unknown"
    assert crosswalk_category("tool_error") == "tool_error"  # passthrough
    print("crosswalk   : OK")

    try:
        crosswalk_category(42)               # type: ignore[arg-type]
    except TypeError as exc:
        print("non-str cat :", exc)

    # ---- Vocabularies exposed --------------------------------------- #
    print("vocab keys  :", sorted(CATEGORY_VOCABULARIES))
    assert "failure_classifier" in CATEGORY_VOCABULARIES
    assert "robust_scorer" in CATEGORY_VOCABULARIES

    # ---- End-to-end pipeline --------------------------------------- #
    gt = {"output": "The answer is 42."}

    classification = classifier.classify_detailed(  # type: ignore[union-attr]
        TimeoutError("request timed out")
    )
    score = scorer.score(                            # type: ignore[union-attr]
        {"status": "timeout", "partial_output": "The answer is",
         "energy_used": 0.02, "time_limit": 30.0},
        gt,
    )
    mapped = crosswalk_category(score["failure_category"])
    print(
        f"pipeline    : class={classification['category']!r} "
        f"score_cat={score['failure_category']!r} "
        f"crosswalked={mapped!r}"
    )
    assert classification["category"] == "timeout"
    assert score["failure_category"] == "timeout"
    assert mapped == "timeout"

    # ---- dir() sanity ---------------------------------------------- #
    public = dir(__import__(__name__))
    for name in __all__:
        assert name in public, f"missing from dir(): {name}"
    print("dir         : OK")

    print("\nSmoke test passed.")
