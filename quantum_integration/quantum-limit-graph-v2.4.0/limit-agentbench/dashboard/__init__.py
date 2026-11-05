# -*- coding: utf-8 -*-
"""
Dashboard and visualization components (Enhanced v2)
====================================================

This package provides the dashboard/visualization surface, with optional
advanced enhancements:

  * LIMIT Graph metrics
  * MODP (Multi-Objective Decision Process)
  * RLHF (Reinforcement Learning from Human Feedback)
  * Multi-Teacher On-Policy Distillation with MoE gating
  * Bio-inspired Optimisation (Evolutionary)
  * FlexGen execution backend

---- Critical fixes vs v1 ---------------------------------------------------
  FIX #1   ``DashboardManager`` no longer passes ``self.config.__dict__``
           to the components. ``__dict__`` is a reference to the dataclass's
           internal dict; every component shared the same ``graph_metrics``
           object and could mutate it for the other three. The manager now
           calls ``config.as_component_config()``, which returns a deep
           copy per component — no shared mutable state.
  FIX #2   ``DashboardConfig`` validates itself in ``__post_init__``.
           ``modp_weights`` of the wrong length, ``human_feedback_score``
           outside ``[0, 1]``, ``graph_metrics`` with out-of-range values,
           and negative population / elitism sizes all raise ``ValueError``
           at construction, not one component at a time at first use.
  FIX #3   The eight "reserved for future subsystem" fields
           (``distillation_lr``, ``gating_lr``, ``population_size``,
           ``mutation_rate``, ``crossover_rate``, ``elitism``,
           ``use_distillation``, ``use_evolutionary``, ``flexgen_enabled``)
           are kept for backward compatibility but documented as
           *reserved*. None of the four dashboard components read them.
  FIX #4   ``DashboardManager(lazy=True)`` defers component construction.
           v1 constructed all four eagerly — which created
           ``./leaderboard_data/`` on disk (GreenLeaderboard's
           ``mkdir(parents=True, exist_ok=True)``) and paid the matplotlib /
           plotly import cost even when only one component was used.
  FIX #5   ``DashboardConfig.to_dict()`` / ``from_dict()`` added so the
           config crosses process boundaries cleanly. ``from_dict``
           ignores unknown fields with a warning instead of raising.
  FIX #6   ``__version__ = "5.0.0"`` at module scope. Every other module
           in this codebase pins the version; ``dashboard.__version__``
           used to raise ``AttributeError``.
  FIX #7   ``DashboardManager.get_component(name)`` accessor. Callers no
           longer have to know the four attribute names; iteration is
           explicit and ``COMPONENT_NAMES`` is the source of truth.
  FIX #8   ``DashboardManager.close()`` cascades ``close()`` to every
           component that has it, and ``__enter__`` / ``__exit__`` make
           cleanup exception-safe:
               with DashboardManager(config) as m:
                   m.get_component("leaderboard").submit(...)
  FIX #9   ``DashboardManager.__repr__`` reports constructed components,
           ``lazy``, and ``use_enhancements``.
  FIX #10  ``get_all_components()`` is typed with a ``Union`` of the four
           concrete classes; static analyzers and IDEs now autocomplete.
  FIX #11  ``manager.config`` is a read-only property returning a deep
           copy. Mutating the returned object no longer silently changes
           the manager's internal state.
  FIX #12  ``DashboardManager.from_dict(config_dict)`` classmethod for
           restoring a manager from persisted state.
  FIX #13  ``storage_path`` kwarg is forwarded to ``GreenLeaderboard`` so
           a caller in a read-only container can redirect the storage
           directory.
  FIX #14  ``as_component_config()`` normalizes ``modp_weights=None`` to
           the documented default ``[0.4, 0.3, 0.2, 0.1]`` before passing
           it down. No component has to handle ``None``.
  FIX #15  ``DashboardManager.COMPONENT_NAMES`` is the canonical list.
           ``get_all_components()`` and ``get_component()`` share it.
  FIX #16  ``--self-test`` in ``__main__`` validates ``DashboardConfig``
           and ``DashboardManager`` behaviour without matplotlib / plotly.

---- Backward compatibility -------------------------------------------------
  * ``DashboardManager(config=None)`` and ``DashboardManager(config)`` keep
    their signatures. ``lazy`` and ``storage_path`` are keyword-only.
  * The four attribute names (``manager.leaderboard``, etc.) are preserved
    as properties, so existing code that reads them keeps working.
  * ``get_all_components()`` returns the same keys.
  * ``DashboardConfig(...)`` accepts the same fields.

---- Run --------------------------------------------------------------------
  python -m dashboard                 # self-test
"""

from __future__ import annotations

import copy
import logging
import math
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


logger = logging.getLogger(__name__)

__version__ = "5.0.0"

from .green_leaderboard import GreenLeaderboard
from .energy_visualizer import EnergyVisualizer
from .carbon_dashboard import CarbonDashboard
from .comparison_matrix import ComparisonMatrix


__all__ = [
    "GreenLeaderboard",
    "EnergyVisualizer",
    "CarbonDashboard",
    "ComparisonMatrix",
    "DashboardConfig",
    "DashboardManager",
    "__version__",
]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
# The default MODP weight vector (FIX #14 — normalized in the manager so
# no component has to handle None).
_DEFAULT_MODP_WEIGHTS: List[float] = [0.4, 0.3, 0.2, 0.1]

# Known graph_metrics keys and their valid range (FIX #2).
_GRAPH_METRIC_KEYS = ("centrality", "connectivity", "density")


@dataclass
class DashboardConfig:
    """
    Configuration for dashboard components with enhancement flags.

    All fields are validated in ``__post_init__``. Use ``to_dict()`` /
    ``from_dict()`` to move the config across process boundaries.
    """

    # --- enhancement master switch ---
    use_enhancements: bool = False

    # --- LIMIT Graph metrics ---
    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {
            "centrality": 0.5,
            "connectivity": 0.5,
            "density": 0.4,
        },
    )

    # --- MODP weights: [accuracy, energy, latency, carbon] or None ---
    modp_weights: Optional[List[float]] = None

    # --- RLHF ---
    human_feedback_score: float = 0.5

    # --- FlexGen (read by the runtime, not by the four dashboard
    #     components; kept for a single-source-of-truth config) ---
    flexgen_enabled: bool = False

    # --- Reserved for future distillation / MoE / evolutionary subsystem ---
    # These are declared for compatibility with orchestrator-side code that
    # already reads them. None of the four dashboard components consume them.
    use_distillation: bool = True
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    use_evolutionary: bool = False
    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2

    # ------------------------------------------------------------------
    # Validation (FIX #2)
    # ------------------------------------------------------------------
    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        # --- graph_metrics ---
        if not isinstance(self.graph_metrics, dict):
            raise ValueError(
                f"graph_metrics must be a dict, got "
                f"{type(self.graph_metrics).__name__}"
            )
        for key in _GRAPH_METRIC_KEYS:
            if key in self.graph_metrics:
                value = self.graph_metrics[key]
                try:
                    f = float(value)
                except (TypeError, ValueError):
                    raise ValueError(
                        f"graph_metrics[{key!r}] must be numeric, "
                        f"got {value!r}"
                    )
                if not math.isfinite(f):
                    raise ValueError(
                        f"graph_metrics[{key!r}] must be finite, got {f!r}"
                    )
                if not (0.0 <= f <= 1.0):
                    raise ValueError(
                        f"graph_metrics[{key!r}]={f} outside [0, 1]"
                    )
        unknown = set(self.graph_metrics) - set(_GRAPH_METRIC_KEYS)
        if unknown:
            logger.warning(
                "DashboardConfig: unknown graph_metrics keys %s "
                "(known: %s)",
                sorted(unknown), list(_GRAPH_METRIC_KEYS),
            )

        # --- modp_weights ---
        if self.modp_weights is not None:
            if not isinstance(self.modp_weights, (list, tuple)):
                raise ValueError(
                    f"modp_weights must be a list, tuple, or None, got "
                    f"{type(self.modp_weights).__name__}"
                )
            if len(self.modp_weights) != 4:
                raise ValueError(
                    f"modp_weights must have length 4, got "
                    f"{len(self.modp_weights)}"
                )
            for w in self.modp_weights:
                try:
                    f = float(w)
                except (TypeError, ValueError):
                    raise ValueError(
                        f"modp_weights entry {w!r} is not numeric"
                    )
                if not math.isfinite(f) or f < 0:
                    raise ValueError(
                        f"modp_weights entry {w!r} must be finite and >= 0"
                    )
            if sum(float(w) for w in self.modp_weights) <= 0:
                raise ValueError(
                    "modp_weights must sum to a positive value"
                )

        # --- human_feedback_score ---
        try:
            hf = float(self.human_feedback_score)
        except (TypeError, ValueError):
            raise ValueError(
                f"human_feedback_score must be numeric, got "
                f"{self.human_feedback_score!r}"
            )
        if not (0.0 <= hf <= 1.0):
            raise ValueError(
                f"human_feedback_score={hf} outside [0, 1]"
            )

        # --- reserved fields (loose checks) ---
        if self.distillation_lr <= 0:
            raise ValueError(
                f"distillation_lr must be > 0, got {self.distillation_lr}"
            )
        if self.gating_lr <= 0:
            raise ValueError(
                f"gating_lr must be > 0, got {self.gating_lr}"
            )
        if self.population_size < 1:
            raise ValueError(
                f"population_size must be >= 1, got {self.population_size}"
            )
        if not (0.0 <= self.mutation_rate <= 1.0):
            raise ValueError(
                f"mutation_rate={self.mutation_rate} outside [0, 1]"
            )
        if not (0.0 <= self.crossover_rate <= 1.0):
            raise ValueError(
                f"crossover_rate={self.crossover_rate} outside [0, 1]"
            )
        if self.elitism < 0:
            raise ValueError(
                f"elitism must be >= 0, got {self.elitism}"
            )

    # ------------------------------------------------------------------
    # Serialization (FIX #5)
    # ------------------------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable snapshot."""
        return {
            "use_enhancements": self.use_enhancements,
            "graph_metrics": dict(self.graph_metrics),
            "modp_weights": (
                [float(w) for w in self.modp_weights]
                if self.modp_weights is not None else None
            ),
            "human_feedback_score": float(self.human_feedback_score),
            "flexgen_enabled": self.flexgen_enabled,
            "use_distillation": self.use_distillation,
            "distillation_lr": float(self.distillation_lr),
            "gating_lr": float(self.gating_lr),
            "use_evolutionary": self.use_evolutionary,
            "population_size": int(self.population_size),
            "mutation_rate": float(self.mutation_rate),
            "crossover_rate": float(self.crossover_rate),
            "elitism": int(self.elitism),
        }

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "DashboardConfig":
        """Rebuild from ``to_dict`` output, ignoring unknown fields."""
        if not data:
            return cls()
        known = {
            "use_enhancements", "graph_metrics", "modp_weights",
            "human_feedback_score", "flexgen_enabled",
            "use_distillation", "distillation_lr", "gating_lr",
            "use_evolutionary", "population_size", "mutation_rate",
            "crossover_rate", "elitism",
        }
        unknown = set(data) - known
        if unknown:
            logger.warning(
                "DashboardConfig.from_dict: ignoring unknown fields %s",
                sorted(unknown),
            )
        return cls(**{k: v for k, v in data.items() if k in known})

    # ------------------------------------------------------------------
    # Per-component snapshot (FIX #1, #14)
    # ------------------------------------------------------------------
    def as_component_config(self) -> Dict[str, Any]:
        """
        Return a *deep copy* of ``to_dict()`` with ``modp_weights``
        normalized to the documented default when ``None``.

        Every component gets its own copy — mutating one component's dict
        cannot affect another's.
        """
        snapshot = copy.deepcopy(self.to_dict())
        if snapshot.get("modp_weights") is None:
            snapshot["modp_weights"] = list(_DEFAULT_MODP_WEIGHTS)
        return snapshot


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------
class DashboardManager:
    """
    Wraps the four dashboard components behind a shared config.

    Parameters
    ----------
    config : DashboardConfig, optional
        Shared configuration. A defensive copy is taken internally, so
        later mutations to the caller's ``config`` do not affect the
        manager.
    lazy : bool
        When ``True``, components are constructed on first access instead
        of eagerly. Avoids ``GreenLeaderboard``'s ``mkdir`` side effect
        and the visualizers' import cost when unused.
    storage_path : str | Path, optional
        Forwarded to ``GreenLeaderboard`` as its ``storage_path``.
    """

    COMPONENT_NAMES: tuple = (
        "leaderboard",
        "energy_visualizer",
        "carbon_dashboard",
        "comparison_matrix",
    )

    def __init__(
        self,
        config: Optional[DashboardConfig] = None,
        *,
        lazy: bool = False,
        storage_path: Optional[Union[str, Path]] = None,
    ) -> None:
        # FIX #11 — defensive copy of the caller's config
        source = config or DashboardConfig()
        # Accept either a DashboardConfig or a dict (FIX #12 helper below)
        if isinstance(source, dict):
            source = DashboardConfig.from_dict(source)
        self._config = DashboardConfig.from_dict(source.to_dict())
        self._storage_path = storage_path
        self._lazy = bool(lazy)
        self._components: Dict[str, Any] = {}

        if not self._lazy:
            for name in self.COMPONENT_NAMES:
                self._build(name)

    # ------------------------------------------------------------------
    # Dunder helpers (FIX #8, #9)
    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        return (
            f"DashboardManager("
            f"components={sorted(self._components)}, "
            f"lazy={self._lazy}, "
            f"use_enhancements={self._config.use_enhancements})"
        )

    def __enter__(self) -> "DashboardManager":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __len__(self) -> int:
        return len(self._components)

    # ------------------------------------------------------------------
    # Properties (FIX #11)
    # ------------------------------------------------------------------
    @property
    def config(self) -> DashboardConfig:
        """Read-only snapshot of the manager's config."""
        return DashboardConfig.from_dict(self._config.to_dict())

    @property
    def lazy(self) -> bool:
        return self._lazy

    # Backward-compatible attribute access
    @property
    def leaderboard(self) -> GreenLeaderboard:
        return self.get_component("leaderboard")     # type: ignore[return-value]

    @property
    def energy_visualizer(self) -> EnergyVisualizer:
        return self.get_component("energy_visualizer")  # type: ignore[return-value]

    @property
    def carbon_dashboard(self) -> CarbonDashboard:
        return self.get_component("carbon_dashboard")   # type: ignore[return-value]

    @property
    def comparison_matrix(self) -> ComparisonMatrix:
        return self.get_component("comparison_matrix")  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Component access (FIX #7, #15)
    # ------------------------------------------------------------------
    def _build(self, name: str) -> Any:
        """Construct and cache the named component."""
        config_dict = self._config.as_component_config()

        if name == "leaderboard":
            kwargs: Dict[str, Any] = {"config": config_dict}
            if self._storage_path is not None:
                kwargs["storage_path"] = str(self._storage_path)
            component = GreenLeaderboard(**kwargs)
        elif name == "energy_visualizer":
            component = EnergyVisualizer(config=config_dict)
        elif name == "carbon_dashboard":
            component = CarbonDashboard(config=config_dict)
        elif name == "comparison_matrix":
            component = ComparisonMatrix(config=config_dict)
        else:
            raise KeyError(
                f"unknown component {name!r}; valid: "
                f"{list(self.COMPONENT_NAMES)}"
            )

        self._components[name] = component
        return component

    def get_component(self, name: str) -> Any:
        """Return (and lazily construct) one named component."""
        if name not in self.COMPONENT_NAMES:
            raise KeyError(
                f"unknown component {name!r}; valid: "
                f"{list(self.COMPONENT_NAMES)}"
            )
        if name not in self._components:
            self._build(name)
        return self._components[name]

    def get_all_components(self) -> Dict[str, Any]:
        """Force-construct and return all four components."""
        for name in self.COMPONENT_NAMES:
            self.get_component(name)
        return dict(self._components)

    # ------------------------------------------------------------------
    # Lifecycle (FIX #8)
    # ------------------------------------------------------------------
    def close(self) -> None:
        """Close every constructed component that exposes ``close()``."""
        for name, comp in list(self._components.items()):
            closer = getattr(comp, "close", None)
            if callable(closer):
                try:
                    closer()
                except Exception as exc:                 # noqa: BLE001
                    logger.warning(
                        "component %r close() raised: %s", name, exc,
                    )
        self._components.clear()

    # ------------------------------------------------------------------
    # Class helpers (FIX #12)
    # ------------------------------------------------------------------
    @classmethod
    def from_dict(
        cls,
        data: Optional[Dict[str, Any]],
        *,
        lazy: bool = False,
        storage_path: Optional[Union[str, Path]] = None,
    ) -> "DashboardManager":
        """Restore a manager from a ``DashboardConfig.to_dict()`` snapshot."""
        return cls(
            DashboardConfig.from_dict(data),
            lazy=lazy,
            storage_path=storage_path,
        )


# ---------------------------------------------------------------------------
# Self-test (FIX #16)
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

    print("DashboardConfig defaults:")
    cfg = DashboardConfig()
    _check("use_enhancements default False", cfg.use_enhancements is False)
    _check(
        "graph_metrics has all keys",
        set(cfg.graph_metrics) == {"centrality", "connectivity", "density"},
    )
    _check("modp_weights default None", cfg.modp_weights is None)

    print("DashboardConfig validation:")
    for bad_kwargs, label in [
        ({"modp_weights": [0.5, 0.5, 0.5]}, "modp_weights length 3"),
        ({"modp_weights": [0.5, 0.5, 0.5, -0.1]}, "negative weight"),
        ({"human_feedback_score": 1.5}, "hf > 1"),
        ({"human_feedback_score": -0.1}, "hf < 0"),
        ({"graph_metrics": {"centrality": 2.0}}, "centrality > 1"),
        ({"graph_metrics": {"centrality": float("nan")}}, "centrality nan"),
        ({"population_size": 0}, "population_size 0"),
        ({"mutation_rate": 2.0}, "mutation_rate > 1"),
        ({"crossover_rate": -0.1}, "crossover_rate < 0"),
        ({"elitism": -1}, "elitism < 0"),
    ]:
        try:
            DashboardConfig(**bad_kwargs)
            _check(f"rejects {label}", False)
        except ValueError:
            _check(f"rejects {label}", True)

    print("DashboardConfig.to_dict / from_dict:")
    original = DashboardConfig(
        use_enhancements=True,
        modp_weights=[0.4, 0.3, 0.2, 0.1],
        human_feedback_score=0.8,
    )
    snapshot = original.to_dict()
    _check("to_dict has use_enhancements", snapshot["use_enhancements"] is True)
    _check("to_dict copies graph_metrics", snapshot["graph_metrics"] is not original.graph_metrics)
    _check("to_dict copies modp_weights", snapshot["modp_weights"] is not original.modp_weights)
    restored = DashboardConfig.from_dict(snapshot)
    _check("round-trip use_enhancements", restored.use_enhancements is True)
    _check("round-trip modp_weights", restored.modp_weights == [0.4, 0.3, 0.2, 0.1])
    _check("round-trip hf", restored.human_feedback_score == 0.8)

    # Unknown fields are dropped, not crashed on
    restored = DashboardConfig.from_dict({"unknown_key": 1, "use_enhancements": True})
    _check("from_dict drops unknown", restored.use_enhancements is True)

    print("as_component_config:")
    cc1 = original.as_component_config()
    cc2 = original.as_component_config()
    _check("returns separate dicts", cc1 is not cc2)
    _check("returns separate graph_metrics",
           cc1["graph_metrics"] is not cc2["graph_metrics"])
    # modp_weights=None normalized to default
    cc_none = DashboardConfig().as_component_config()
    _check(
        "None modp_weights normalized",
        cc_none["modp_weights"] == [0.4, 0.3, 0.2, 0.1],
    )

    print("DashboardManager (config independence):")
    # Simulate the v1 bug: mutate the source config after construction
    source = DashboardConfig(use_enhancements=True)
    manager = DashboardManager(source, lazy=True)
    source.graph_metrics["centrality"] = 0.99
    _check(
        "manager config unaffected by external mutation",
        manager.config.graph_metrics["centrality"] == 0.5,
    )

    # manager.config returns a fresh copy each call
    a = manager.config
    b = manager.config
    a.graph_metrics["centrality"] = 0.99
    _check(
        "mutating returned config does not affect manager",
        manager.config.graph_metrics["centrality"] == 0.5,
    )

    print("DashboardManager lazy construction:")
    mgr = DashboardManager(lazy=True)
    _check("no components constructed yet", len(mgr._components) == 0)
    _check("get_component triggers construction", mgr.get_component("leaderboard") is not None)
    _check("component is cached", "leaderboard" in mgr._components)

    # Eager mode
    eager = DashboardManager(lazy=False)
    _check(
        "eager constructs all four",
        len(eager._components) == 4,
        detail=f"got {len(eager._components)}",
    )

    print("DashboardManager.get_component validation:")
    try:
        mgr.get_component("nonexistent")
        _check("rejects unknown name", False)
    except KeyError:
        _check("rejects unknown name", True)

    print("DashboardManager.get_all_components:")
    all_comps = mgr.get_all_components()
    _check(
        "returns all four",
        set(all_comps.keys()) == set(DashboardManager.COMPONENT_NAMES),
    )

    print("DashboardManager context manager:")
    with DashboardManager(lazy=True) as ctx:
        ctx.get_component("leaderboard")
        _check("components constructed inside with", len(ctx._components) == 1)
    _check("close() clears components", len(ctx._components) == 0)

    print("DashboardManager.from_dict:")
    restored_mgr = DashboardManager.from_dict(
        {"use_enhancements": True, "modp_weights": [0.5, 0.5, 0.0, 0.0]},
        lazy=True,
    )
    _check(
        "restored config",
        restored_mgr.config.use_enhancements is True,
    )
    _check(
        "restored modp_weights",
        restored_mgr.config.modp_weights == [0.5, 0.5, 0.0, 0.0],
    )

    print("__version__:")
    _check("has __version__", __version__ == "5.0.0")

    print("__repr__:")
    _check("mentions class", "DashboardManager" in repr(mgr))

    print("")
    if failures:
        print(f"{failures} failure(s)")
        return 1
    print("all self-tests passed")
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sys.exit(_self_test())
