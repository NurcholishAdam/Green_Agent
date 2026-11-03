# src/visualization/__init__.py

"""
Visualization package for Green Agent
=====================================

Interactive evidence-exploration layer. Turns validated data from
``analysis`` and ``reporting`` into charts, graphs, timelines, and
inspection views. Performs no policy execution and modifies no live
decisions.

Guarded imports
---------------
Every submodule is loaded through a guarded loader. A submodule that
fails to import — because an optional dependency is missing, an install
is partial, or a path is wrong — is recorded in
:data:`VISUALIZATION_AVAILABILITY` and substituted with a
:class:`_MissingComponent` stub. The stub raises ``ImportError`` on
first use, so a caller who tries to use a missing feature gets a loud
error rather than a silent ``None``. Call :func:`health` to check what
is available before depending on specific names.

Public API surface
------------------
See ``__all__`` for the full list; broadly grouped as:

- **Contracts** — chart context, theme, accessibility.
- **Renderers** — backend selection, figure export, ``ParetoExplorer``.
- **Pareto wrappers** — the four legacy ``plot_*`` functions.
- **ParetoPlotter** — the class-based API (still supported).
- **Decision cards** — XAI renderables.
- **Additional trade-off visuals** — energy/carbon, quality/latency.

Recommended vs compatibility APIs
---------------------------------
Several subsystems expose a **compatibility wrapper** (returns a bare
enum or bool) alongside a **recommended API** (returns a structured
record with failure reasons):

- ``get_renderer`` (compat)      vs ``select_backend`` (recommended)
- ``save_figure`` / ``export_html`` (compat)
  vs ``save_figure_detailed`` / ``export_html_detailed`` (recommended)

Both are re-exported. New code should prefer the recommended variants.

Pareto plot function mapping
----------------------------
Four legacy ``plot_*`` function names span three source files. The
façade picks a canonical source for each so callers do not have to know
the mapping:

=================================  =========================
Name                               Canonical source
=================================  =========================
``plot_accuracy_vs_energy``        ``energy_pareto``
                                   (fallback: ``leaderboard_plots``)
``plot_accuracy_vs_carbon``        ``pareto_plots``
``plot_latency_vs_energy``         ``pareto_plots``
``plot_carbon_vs_energy``          ``leaderboard_plots``
=================================  =========================

Version
-------
``visualization`` is versioned independently of the rest of the
``src/`` tree. Its ``__version__`` reflects the recommendation
milestone the visualization layer tracks, not the internal code-release
cadence used by ``rlhf``, ``runtime``, ``scoring``, ``scripts``,
``sustainability``, and ``symbolic`` (all of which report ``2.4.0``).
The difference is intentional — the façade reports the layer's
recommendation-product version.

Requires Python 3.8+.
"""

import importlib
import logging
import sys
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Version
# --------------------------------------------------------------------------- #
__version__: str = "5.0.0"


# --------------------------------------------------------------------------- #
# Missing-component stub
# --------------------------------------------------------------------------- #
class _MissingComponent:
    """
    Placeholder for a submodule export whose module failed to import.

    Every operation raises ``ImportError`` chained to the original
    cause — including ``__bool__``, so a caller who writes
    ``if SomeName is None: skip()`` (which never fires on a stub) or
    ``if not SomeName: skip()`` (which now raises) cannot silently
    degrade behaviour.
    """

    __slots__ = ("_name", "_reason")

    def __init__(self, name: str, reason: BaseException) -> None:
        self._name = name
        self._reason = reason

    def _fail(self) -> None:
        raise ImportError(
            f"visualization component '{self._name}' is unavailable: "
            f"{self._reason}"
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
        return f"<unavailable visualization component {self._name!r}>"


# --------------------------------------------------------------------------- #
# Availability manifest (read-only public view)
# --------------------------------------------------------------------------- #
_availability: Dict[str, bool] = {}
VISUALIZATION_AVAILABILITY: Mapping[str, bool] = MappingProxyType(
    _availability
)


# --------------------------------------------------------------------------- #
# Guarded loader
# --------------------------------------------------------------------------- #
def _load(module: str, names: Tuple[str, ...]) -> Dict[str, Any]:
    """
    Import ``names`` from ``.{module}``; return stubs on ``ImportError``.

    A name that is importable but missing from the loaded submodule
    is also stubbed (with an ``AttributeError`` cause).
    """
    try:
        mod = importlib.import_module(f".{module}", package=__package__)
        _availability[module] = True
        return {
            n: getattr(mod, n, _MissingComponent(n, AttributeError(n)))
            for n in names
        }
    except ImportError as exc:
        logger.info("%s unavailable: %s", module, exc)
        _availability[module] = False
        return {n: _MissingComponent(n, exc) for n in names}


def _load_xai_card() -> Dict[str, Any]:
    """
    Load the XAI decision-card module.

    The tree has been reorganised at least once — the original location
    was ``explainability/xai_decision_card.py``; the consolidated
    version lives under ``visualization/assurance/``. Try both paths
    in priority order and use whichever imports first.
    """
    names: Tuple[str, ...] = (
        "DEFAULT_STYLE",
        "DecisionCardRenderer",
        "RejectedAlternative",
        "XAIDecisionCard",
        "XAIDecisionCardError",
        "render_decision_card",
    )
    candidates = (
        "assurance.xai_decision_card",
        "explainability.xai_decision_card",
    )
    for path in candidates:
        try:
            mod = importlib.import_module(f".{path}", package=__package__)
            _availability[path] = True
            return {
                n: getattr(mod, n, _MissingComponent(n, AttributeError(n)))
                for n in names
            }
        except ImportError:
            _availability[path] = False
            continue
    tried = ", ".join(repr(p) for p in candidates)
    reason = ImportError(f"tried: {tried}")
    return {n: _MissingComponent(n, reason) for n in names}


# --------------------------------------------------------------------------- #
# Load every submodule
# --------------------------------------------------------------------------- #
# Contracts
globals().update(_load(
    "contracts.chart_schema",
    (
        "ChartContext",
        "ChartContextDict",
        "ChartSchemaError",
        "DataSource",
        "TradeoffKind",
        "TrustLevel",
    ),
))
globals().update(_load(
    "contracts.theme",
    (
        "COLORBLIND_SAFE_THEME",
        "DEFAULT_THEME",
        "Palette",
        "ThemeError",
        "VisualTheme",
        "accessibility_palette",
    ),
))
globals().update(_load(
    "contracts.accessibility",
    (
        "COLORBLIND_SAFE",
        "PALETTE_DESCRIPTIONS",
        "PaletteRole",
        "WCAG_AA_LARGE",
        "WCAG_AA_NORMAL",
        "WCAG_AAA_LARGE",
        "WCAG_AAA_NORMAL",
        "WcagResult",
        "AccessibilityError",
        "check_contrast",
        "check_palette_contrast",
        "check_wcag",
        "colorblind_safe_palette",
        "passes_aa",
        "passes_aaa",
        "relative_luminance",
    ),
))

# Renderers
globals().update(_load(
    "renderers.base",
    (
        "RendererBackend",
        "RendererError",
        "RendererSelection",
        "available_backends",
        "get_renderer",
        "is_available",
        "select_backend",
    ),
))
globals().update(_load(
    "renderers.export",
    (
        "ExportError",
        "ExportResult",
        "export_html",
        "export_html_detailed",
        "save_figure",
        "save_figure_detailed",
    ),
))
globals().update(_load(
    "renderers.pareto_plots",
    (
        "ParetoExplorer",
        "ParetoExplorerError",
        "compute_frontier",
    ),
))

# Backward-compatible plot functions.
# Load order encodes canonical-source priority: later loads override
# earlier ones, so the last import for a given name wins.
globals().update(_load(
    "pareto_plots",
    ("plot_accuracy_vs_carbon", "plot_latency_vs_energy"),
))
globals().update(_load(
    "leaderboard_plots",
    ("plot_carbon_vs_energy", "plot_accuracy_vs_energy"),
))
# Canonical source for plot_accuracy_vs_energy. If energy_pareto is
# missing, the leaderboard_plots version (loaded just above) remains.
globals().update(_load(
    "energy_pareto",
    ("plot_accuracy_vs_energy",),
))

# Class-based API
globals().update(_load(
    "pareto_plotter",
    ("ParetoPlotter", "ParetoPlotterError"),
))

# Decision cards (two-path load)
globals().update(_load_xai_card())

# Additional trade-off visuals (likely not present; guarded)
globals().update(_load(
    "tradeoffs.energy_carbon",
    ("plot_energy_carbon_tradeoff",),
))
globals().update(_load(
    "tradeoffs.quality_latency",
    ("plot_quality_latency_tradeoff",),
))


# --------------------------------------------------------------------------- #
# Health / diagnostics
# --------------------------------------------------------------------------- #
def health() -> Dict[str, Any]:
    """
    Return an aggregate availability snapshot.

    Reports which submodules loaded, whether the two plotting backends
    are importable, and a summary count. Intended for operator
    dashboards, CI jobs, and bug reports.

    Answers "will a user of this package get a working chart?" without
    requiring any submodule to have been constructed yet.
    """
    # Backend availability is delegated to renderers.base.is_available
    # (the enhanced version that uses importlib.util.find_spec and
    # caches results). Fall back to a defensive False pair if the
    # renderers.base module itself is not available.
    backends: Dict[str, bool] = {"plotly": False, "matplotlib": False}
    try:
        from .renderers.base import is_available as _is_available
        if not isinstance(_is_available, _MissingComponent):
            backends["plotly"] = bool(_is_available("plotly"))
            backends["matplotlib"] = bool(_is_available("matplotlib"))
    except Exception:  # pragma: no cover — defensive
        pass

    unavailable = sorted(
        name for name, ok in _availability.items() if not ok
    )
    return {
        "version": __version__,
        "submodules": dict(_availability),
        "backends": dict(backends),
        "unavailable": unavailable,
        "unavailable_count": len(unavailable),
    }


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: Tuple[str, ...] = (
    "__version__",
    # Manifest / health
    "VISUALIZATION_AVAILABILITY",
    "health",
    # Contracts — chart_schema
    "ChartContext",
    "ChartContextDict",
    "ChartSchemaError",
    "DataSource",
    "TradeoffKind",
    "TrustLevel",
    # Contracts — theme
    "COLORBLIND_SAFE_THEME",
    "DEFAULT_THEME",
    "Palette",
    "ThemeError",
    "VisualTheme",
    "accessibility_palette",
    # Contracts — accessibility
    "COLORBLIND_SAFE",
    "PALETTE_DESCRIPTIONS",
    "PaletteRole",
    "WCAG_AA_LARGE",
    "WCAG_AA_NORMAL",
    "WCAG_AAA_LARGE",
    "WCAG_AAA_NORMAL",
    "WcagResult",
    "AccessibilityError",
    "check_contrast",
    "check_palette_contrast",
    "check_wcag",
    "colorblind_safe_palette",
    "passes_aa",
    "passes_aaa",
    "relative_luminance",
    # Renderers — base
    "RendererBackend",
    "RendererError",
    "RendererSelection",
    "available_backends",
    "get_renderer",
    "is_available",
    "select_backend",
    # Renderers — export
    "ExportError",
    "ExportResult",
    "export_html",
    "export_html_detailed",
    "save_figure",
    "save_figure_detailed",
    # Renderers — pareto
    "ParetoExplorer",
    "ParetoExplorerError",
    "compute_frontier",
    # Legacy plot functions
    "plot_accuracy_vs_carbon",
    "plot_accuracy_vs_energy",
    "plot_carbon_vs_energy",
    "plot_latency_vs_energy",
    # Class-based API
    "ParetoPlotter",
    "ParetoPlotterError",
    # Decision cards
    "DEFAULT_STYLE",
    "DecisionCardRenderer",
    "RejectedAlternative",
    "XAIDecisionCard",
    "XAIDecisionCardError",
    "render_decision_card",
    # Additional trade-off visuals
    "plot_energy_carbon_tradeoff",
    "plot_quality_latency_tradeoff",
)


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m visualization
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    # ---- 1. Module loaded and manifest is a Mapping -------------- #
    print("module structure:")
    _check("module loaded", True)
    _check(
        "VISUALIZATION_AVAILABILITY is a Mapping",
        isinstance(VISUALIZATION_AVAILABILITY, Mapping),
    )

    # Manifest is read-only
    try:
        VISUALIZATION_AVAILABILITY["hack"] = True  # type: ignore[index]
        _check("manifest is read-only", False)
    except TypeError:
        _check("manifest is read-only", True)

    # ---- 2. All __all__ names are present in the module ---------- #
    print("__all__ completeness:")
    module = sys.modules[__name__]
    missing = [n for n in __all__ if not hasattr(module, n)]
    _check("all __all__ names present", not missing)
    if missing:
        print(f"    missing: {missing}")

    # ---- 3. Correct import paths -------------------------------- #
    print("import paths:")
    for sub in (
        "contracts.chart_schema",
        "contracts.theme",
        "contracts.accessibility",
        "renderers.base",
        "renderers.export",
        "renderers.pareto_plots",
    ):
        _check(
            f"{sub} available",
            VISUALIZATION_AVAILABILITY.get(sub, False),
        )

    # ---- 4. health() returns a well-formed snapshot -------------- #
    print("health:")
    h = health()
    _check("health is a dict", isinstance(h, dict))
    _check("health has version", h.get("version") == __version__)
    _check(
        "health has submodules dict",
        isinstance(h.get("submodules"), dict),
    )
    _check(
        "health has backends dict",
        isinstance(h.get("backends"), dict)
        and set(h["backends"]) == {"plotly", "matplotlib"},
    )
    _check(
        "health has unavailable list",
        isinstance(h.get("unavailable"), list),
    )
    _check(
        "health unavailable_count matches",
        h.get("unavailable_count") == len(h.get("unavailable", [])),
    )

    # ---- 5. _MissingComponent raises loudly --------------------- #
    print("_MissingComponent:")
    stub = _MissingComponent("demo", ImportError("simulated"))
    for label, probe in (
        ("call", lambda: stub()),
        ("attribute", lambda: stub.some_attr),
        ("bool", lambda: bool(stub)),
    ):
        try:
            probe()
            _check(f"stub {label} raises", False)
        except ImportError:
            _check(f"stub {label} raises", True)

    # ---- 6. __dir__ matches __all__ ----------------------------- #
    print("dir:")
    _check("__dir__ sorted __all__", __dir__() == sorted(__all__))

    # ---- 7. Version is set ------------------------------------- #
    print("version:")
    _check(
        "version is a non-empty string",
        isinstance(__version__, str) and bool(__version__),
    )

    # ---- 8. Real exports when their submodule loads ------------- #
    print("live exports (best-effort):")
    # Only check a handful — the rest are validated by __all__ above.
    for name in (
        "ChartContext",
        "VisualTheme",
        "DEFAULT_THEME",
        "ParetoExplorer",
        "RendererBackend",
        "save_figure",
    ):
        obj = getattr(module, name, None)
        if isinstance(obj, _MissingComponent):
            print(f"  ⚠️  {name} is a stub (submodule unavailable)")
        else:
            _check(f"{name} is a live object", obj is not None)

    # ---- 9. Summary -------------------------------------------- #
    unavailable = h.get("unavailable", [])
    if unavailable:
        print()
        print(f"submodules unavailable in this environment: {unavailable}")
    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
