# -*- coding: utf-8 -*-
"""
Green metrics tracking modules (Enhanced, v2)
==============================================

Core green metrics calculators, re-exported from their sub-modules:

    EnergyTracker         — sampling power integration (file 30)
    CarbonCalculator      — regional carbon emissions + region selection (file 27)
    EfficiencyScorer      — accuracy/energy efficiency + composite (file 29)
    SustainabilityIndex   — raw + normalized sustainability (file 32)

Plus ``GreenMetricsManager``, which composes them into a single report.

Fixes applied vs. v1
--------------------
  P0  Composite score was 0.0 for every report because the inner
      distillation model was initialized to zeros and its update() was
      never called.                                        -> delegate to
                                                              Sustainability
                                                              Index.
                                                              calculate_
                                                              enhanced;
                                                              fall back to
                                                              MODP only
                                                              when the
                                                              enhanced API
                                                              is unavailable
  P0  ``use_enhancements=True`` on the manager did NOT propagate to
      the sub-calculators (each constructed with its own default
      config).                                            -> pass
                                                              ``config=
                                                              self.config``
                                                              to each;
                                                              fall back
                                                              gracefully
                                                              when the
                                                              sub-module's
                                                              constructor
                                                              doesn't
                                                              accept it
  P0  Two "carbon" fields appeared in the report with different
      values (caller's ``carbon_co2e_kg`` vs.
      ``carbon_calculator.calculate_emissions(energy_kwh)``).
                                                          -> single
                                                              canonical
                                                              field;
                                                              caller's
                                                              value wins;
                                                              computed
                                                              value
                                                              exposed only
                                                              when caller
                                                              did not
                                                              supply one
  P0  Four incompatible scales in one report              -> every
                                                              score field
                                                              documented
                                                              with a
                                                              ``*_scale``
                                                              companion
                                                              key; bounded
                                                              scores go in
                                                              ``*_normalized``
  P0  MODP validation crashed on ``[]`` / wrong-length / negative
                                                          -> frozen config
                                                              with
                                                              ``__post_init__``
  P0  Config was mutated in ``__init__``                  -> frozen config
                                                              + local
                                                              normalized
                                                              copy in
                                                              ``_modp_weights``
  P0  11th MODP ordering variant                          -> ``MODP_SCHEMA``
                                                              constant
  P0  Distillation training was never invoked             -> delegate
                                                              to
                                                              Sustainability
                                                              Index.
                                                              record_outcome
  P0  ``update_fitness`` was ``pass`` and never called    -> delete; use
                                                              the evolutionary
                                                              optimizer of
                                                              the sub-calculator
                                                              when available
  P0  ``EnergyTracker`` was instantiated but never
      started; only its static method was used            -> use only
                                                              when a
                                                              ``power_reader``
                                                              is configured;
                                                              otherwise fall
                                                              back to the
                                                              static method
  P1  Eager imports meant one broken sub-module killed
      the whole package                                   -> PEP 562 lazy
                                                              imports
  P1  ``try/except ImportError`` wrapped stdlib modules   -> only numpy
  P1  Hardcoded ceilings (10 kWh, 1 kg, 1 s)              -> ``1/(1+q/scale)``
                                                              with scales
                                                              from config
  P1  Optimizers were inner classes                       -> delegate to
                                                              sub-calculator
                                                              optimizers;
                                                              no inner
                                                              classes
  P1  No ``record_outcome`` / ``record_human_feedback``   -> added
  P1  No metrics                                          -> ``MetricsSink``
  P1  No thread safety                                    -> ``RLock``
  P2  ``region`` was smuggled through ``graph_metrics``   -> dedicated
                                                              ``region``
                                                              config field
  P2  ``get_energy_score`` used default ``scale_kwh=1.0`` -> configurable
                                                              ``energy_scale_kwh``
  P2  ``carbon_score`` variable name was misleading       -> ``carbon_
                                                              emissions_kg``
  P2  ``distillation_stats`` naming                       -> ``optimizer_stats``
  P2  ``feature_dim=7`` was hardcoded                     -> ``FEATURE_DIM``
  P2  No ``save`` / ``load``                              -> delegating to
                                                              sub-calculators
  P3  Report schema was undocumented                      -> schema
                                                              documented
                                                              in this
                                                              module
                                                              docstring

Scale contract
--------------
The report mixes fields on several scales. Every score field is paired with
a ``*_scale`` companion and a ``*_normalized`` variant on the bounded [0,1]
scale:

    ``accuracy``                    raw, [0, 1]
    ``energy_kwh``                  raw, kWh
    ``carbon_co2e_kg``              raw, kg CO2e (canonical)
    ``carbon_emissions_kg``         raw, kg CO2e (computed from energy,
                                    only present when the caller did not
                                    supply ``carbon_co2e_kg``)
    ``latency_ms``                  raw, ms
    ``cost_usd``                    raw, USD
    ``efficiency_score``            raw ratio (unbounded, ~10^0–10^3)
    ``efficiency_normalized``       bounded [0,1]
    ``sustainability_index``        raw ratio (unbounded, ~10^0–10^6)
    ``sustainability_normalized``   bounded [0,1]
    ``energy_score``                bounded [0,1] — 1 = zero energy
    ``composite_score``             bounded [0,1] — only when enhancements
                                    are enabled
    ``composite_scale``             literal string ``"normalized_0_to_1"``
    ``method``                      ``"distillation"`` / ``"evolutionary"``
                                    / ``"modp"`` / ``"normalized"``

A consumer comparing fields **must** use the paired ``*_scale`` /
``*_normalized`` values. Any two raw-ratio fields, or any two bounded
fields, are on the same scale; mixing raw and bounded is always wrong.

The ``get_rating`` helpers from ``SustainabilityIndex`` use different
thresholds for raw vs. normalized output — use the matching helper.

Public API (preserved + extended)
---------------------------------
    Preserved: ``EnergyTracker``, ``CarbonCalculator``, ``EfficiencyScorer``,
    ``SustainabilityIndex``, ``GreenMetricsManager``, ``GreenMetricsConfig``

    New: ``MetricsSink``, ``InMemoryMetrics``, ``LoggingMetrics``,
    ``MODP_SCHEMA``, ``available_components()``, plus methods on the
    manager: ``record_outcome``, ``record_human_feedback``, ``save``,
    ``load``, ``get_optimizer_stats``
"""

from __future__ import annotations

import importlib
import json
import logging
import math
import os
import threading
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Tuple

try:
    import numpy as np
    ENHANCEMENT_LIBS_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    ENHANCEMENT_LIBS_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================================
# Section 1 — Helpers
# ============================================================================

def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return float(x)


def _bounded_inverse(q: float, scale: float) -> float:
    """1 / (1 + q/scale) — bounded on (0, 1], higher = better."""
    q = max(float(q), 0.0)
    s = max(float(scale), 1e-9)
    return 1.0 / (1.0 + q / s)


def _finite_nonneg(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite non-negative number")
    try:
        f = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be a finite non-negative number"
        ) from exc
    if not math.isfinite(f) or f < 0:
        raise ValueError(f"{name} must be a finite non-negative number")
    return f


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Ignoring corrupt checkpoint %s: %s", path, exc)
        return None


# ============================================================================
# Section 2 — MODP schema (single source of truth for this package)
# ============================================================================

# Canonical ordering used by the manager's fallback composite. Other files
# in the codebase use different orderings — never import weights across
# files without translating through their respective schema. This schema
# matches file 4 (``green_metrics.py``) and file 33's own reports.
MODP_SCHEMA: Tuple[str, ...] = (
    "energy", "carbon", "latency", "accuracy",
)


# ============================================================================
# Section 3 — Metrics sink
# ============================================================================

class MetricsSink(Protocol):
    def observe(self, name: str, value: float, **labels: str) -> None: ...


class InMemoryMetrics:
    def __init__(self) -> None:
        self._data: Dict[str, float] = {}
        self._lock = threading.Lock()

    def observe(self, name: str, value: float, **labels: str) -> None:
        key = name + (
            ":" + ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            if labels else ""
        )
        with self._lock:
            self._data[key] = value

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._data)


class LoggingMetrics:
    def __init__(self) -> None:
        self._log = logging.getLogger("green_metrics.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# Section 4 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class GreenMetricsConfig:
    """Configuration for green metrics aggregation and the sub-calculators.

    Frozen and validated. Passing an invalid ``modp_weights`` raises at
    construction time (not at calculation time).
    """
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5}
    )
    # Dedicated field for the carbon region — no longer smuggled through
    # ``graph_metrics["region"]``.
    region: str = "GLOBAL"

    # 4-tuple, MODP_SCHEMA order: (energy, carbon, latency, accuracy).
    modp_weights: Tuple[float, float, float, float] = (0.3, 0.3, 0.2, 0.2)
    human_feedback_score: float = 0.5

    use_distillation: bool = True
    use_evolutionary: bool = False

    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    batch_size: int = 16
    epsilon: float = 0.1
    epsilon_min: float = 0.02
    epsilon_decay: float = 0.995
    distill_w: float = 0.5
    rl_w: float = 0.5

    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2

    # Bounded-normalization scales for the manager's fallback composite.
    energy_scale_kwh: float = 0.01
    carbon_scale_kg: float = 0.001
    latency_scale_ms: float = 500.0

    seed: Optional[int] = None
    model_path: Optional[Path] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if not isinstance(w, (tuple, list)) or len(w) != 4:
            raise ValueError(
                f"modp_weights must have 4 components {MODP_SCHEMA}, got {w!r}"
            )
        if any(float(x) < 0 for x in w):
            raise ValueError(f"modp_weights must be non-negative, got {w!r}")
        if sum(float(x) for x in w) <= 0:
            raise ValueError(f"modp_weights sum must be > 0, got {w!r}")
        object.__setattr__(self, "modp_weights", tuple(float(x) for x in w))

        if not 0.0 <= self.human_feedback_score <= 1.0:
            raise ValueError("human_feedback_score must be in [0,1]")
        if not 0.0 <= self.epsilon <= 1.0:
            raise ValueError("epsilon must be in [0,1]")
        if not 0.0 <= self.epsilon_min <= self.epsilon:
            raise ValueError("epsilon_min must be in [0, epsilon]")
        if not 0.0 < self.epsilon_decay <= 1.0:
            raise ValueError("epsilon_decay must be in (0,1]")
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if self.population_size < 2:
            raise ValueError("population_size must be >= 2")
        if not 0 <= self.elitism < self.population_size:
            raise ValueError("elitism must be in [0, population_size)")
        if self.distill_w < 0 or self.rl_w < 0 or (self.distill_w + self.rl_w) <= 0:
            raise ValueError("distill_w and rl_w must be non-negative, sum > 0")
        for name in ("energy_scale_kwh", "carbon_scale_kg", "latency_scale_ms"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        for k, v in self.graph_metrics.items():
            if not isinstance(v, (int, float)) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(
                    f"graph_metric {k!r} must be in [0,1], got {v!r}"
                )
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "GreenMetricsConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "GreenMetricsConfig ignoring unknown keys: %s", sorted(unknown)
            )
        kwargs: Dict[str, Any] = {}
        for k, v in d.items():
            if k not in known:
                continue
            if k == "modp_weights":
                if v is None:
                    continue
                kwargs[k] = tuple(float(x) for x in v)
            elif k == "graph_metrics":
                kwargs[k] = dict(v)
            elif k == "model_path":
                kwargs[k] = Path(v) if v is not None else None
            else:
                kwargs[k] = v
        return cls(**kwargs)


# ============================================================================
# Section 5 — Manager (main class)
# ============================================================================

# Constant used by the fallback composite. Exposed for callers who want
# to inspect the feature vector.
FALLBACK_FEATURE_DIM = 4   # (energy, carbon, latency, accuracy)


class GreenMetricsManager:
    """Unified manager for the four green-metrics calculators.

    Delegates each sub-calculation to its calculator. When enhancements
    are enabled, delegates the composite score to
    ``SustainabilityIndex.calculate_enhanced`` (which is the file 32 v2
    optimizer). Falls back to a local MODP composite when that API is
    unavailable.

    All four sub-calculators receive ``config=self.config`` when their
    constructor accepts it, so ``use_enhancements`` is propagated.
    """

    def __init__(
        self,
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if config is None:
            self.config = GreenMetricsConfig()
        elif isinstance(config, GreenMetricsConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = GreenMetricsConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be GreenMetricsConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self._lock = threading.RLock()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if ENHANCEMENT_LIBS_AVAILABLE
            else None
        )

        # Local normalized MODP weights — never mutate config.
        w = self.config.modp_weights
        total = sum(w)
        self._modp_weights: Tuple[float, float, float, float] = (
            w[0] / total, w[1] / total, w[2] / total, w[3] / total,
        )

        self.use_enhancements = bool(
            self.config.use_enhancements and ENHANCEMENT_LIBS_AVAILABLE
        )
        if self.config.use_enhancements and not ENHANCEMENT_LIBS_AVAILABLE:
            logger.warning(
                "use_enhancements=True but numpy is unavailable; "
                "falling back to base path"
            )

        self._human_feedback_score = float(self.config.human_feedback_score)

        # Lazy-import the four calculators here so the package facade can
        # still be imported when a sub-module is broken.
        EnergyTracker = _lazy("energy_tracker", "EnergyTracker")
        CarbonCalculator = _lazy("carbon_calculator", "CarbonCalculator")
        EfficiencyScorer = _lazy("efficiency_scorer", "EfficiencyScorer")
        SustainabilityIndex = _lazy(
            "sustainability_index", "SustainabilityIndex"
        )

        # Try passing our config through; fall back gracefully when the
        # sub-calculator doesn't accept it.
        self.carbon_calculator = _construct_with_config(
            CarbonCalculator,
            self.config,
            grid_region=self.config.region,
        )
        self.efficiency_scorer = _construct_with_config(
            EfficiencyScorer, self.config
        )
        self.sustainability_index = _construct_with_config(
            SustainabilityIndex, self.config
        )

        # EnergyTracker is only useful when a power_reader is available.
        # Otherwise, the base class's static ``get_energy_score`` is used.
        self.energy_tracker = None
        power_reader = getattr(self.config, "power_reader", None)
        if power_reader is not None:
            self.energy_tracker = _construct_with_config(
                EnergyTracker, self.config, power_reader=power_reader
            )

        # Pending decision for record_outcome
        self._pending: Optional[Dict[str, Any]] = None

        # Detected capabilities
        self._has_enhanced_sustainability = hasattr(
            self.sustainability_index, "calculate_enhanced"
        )
        self._has_record_outcome = hasattr(
            self.sustainability_index, "record_outcome"
        )
        self._has_record_human_feedback = hasattr(
            self.sustainability_index, "record_human_feedback"
        )

        logger.info(
            "Initialized GreenMetricsManager: enhancements=%s, "
            "enhanced_sustainability=%s, region=%s",
            self.use_enhancements,
            self._has_enhanced_sustainability,
            self.config.region,
        )

    # ------------------------------------------------------------------
    # Public: compute_report
    # ------------------------------------------------------------------
    def compute_report(
        self,
        energy_kwh: float,
        carbon_co2e_kg: Optional[float] = None,
        accuracy: float = 0.0,
        latency_ms: float = 0.0,
        cost_usd: float = 0.0,
        graph_metrics: Optional[Mapping[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Compute a comprehensive green metrics report.

        ``carbon_co2e_kg`` is optional. When omitted, the manager computes
        it from ``energy_kwh`` and the ``CarbonCalculator``'s regional
        intensity table; the caller-supplied value is always preserved as
        the canonical ``carbon_co2e_kg`` when provided.
        """
        energy_kwh = _finite_nonneg(energy_kwh, "energy_kwh")
        accuracy = _clamp(_finite_nonneg(accuracy, "accuracy"))
        latency_ms = _finite_nonneg(latency_ms, "latency_ms")
        cost_usd = _finite_nonneg(cost_usd, "cost_usd")

        gm = dict(graph_metrics or self.config.graph_metrics)
        hf = (
            _clamp(_finite_nonneg(human_feedback_score, "human_feedback_score"))
            if human_feedback_score is not None
            else float(self._human_feedback_score)
        )

        # Resolve carbon: caller's value wins; otherwise compute from energy.
        if carbon_co2e_kg is not None:
            carbon_canonical = _finite_nonneg(carbon_co2e_kg, "carbon_co2e_kg")
            carbon_emissions_kg = carbon_canonical
            carbon_source = "caller"
        else:
            carbon_emissions_kg = self._calculate_emissions_from_energy(energy_kwh)
            carbon_canonical = carbon_emissions_kg
            carbon_source = "computed_from_energy"

        # Base sub-calculations (always)
        energy_score = self._energy_score(energy_kwh)
        efficiency_raw = self._efficiency_raw(accuracy, energy_kwh)
        efficiency_normalized = _bounded_inverse(
            efficiency_raw, 1.0   # efficiency is already a ratio
        ) if efficiency_raw > 0 else 1.0
        sustainability_raw = self._sustainability_raw(
            accuracy, energy_kwh, carbon_canonical
        )
        sustainability_normalized = self._sustainability_normalized(
            accuracy, energy_kwh, carbon_canonical
        )

        report: Dict[str, Any] = {
            "energy_kwh": energy_kwh,
            "carbon_co2e_kg": carbon_canonical,
            "carbon_emissions_kg": carbon_emissions_kg,
            "carbon_source": carbon_source,
            "accuracy": accuracy,
            "latency_ms": latency_ms,
            "cost_usd": cost_usd,

            # Raw ratio (unbounded)
            "efficiency_score": efficiency_raw,
            "efficiency_score_scale": "raw_ratio",
            # Bounded [0, 1]
            "efficiency_normalized": round(efficiency_normalized, 4),

            # Raw ratio (unbounded)
            "sustainability_index": sustainability_raw,
            "sustainability_index_scale": "raw_ratio",
            # Bounded [0, 1]
            "sustainability_normalized": round(sustainability_normalized, 4),

            # Bounded [0, 1]
            "energy_score": round(energy_score, 4),

            "region": self.config.region,
        }

        # Enhanced composite (only when enhancements are active)
        with self._lock:
            self._pending = None

        if self.use_enhancements:
            composite = self._compute_composite_score(
                energy_kwh=energy_kwh,
                carbon_co2e_kg=carbon_canonical,
                accuracy=accuracy,
                latency_ms=latency_ms,
                cost_usd=cost_usd,
                graph_metrics=gm,
                human_feedback=hf,
            )
            report["composite_score"] = round(float(composite["score"]), 4)
            report["composite_scale"] = "normalized_0_to_1"
            report["method"] = composite["method"]
            report["graph_metrics"] = gm
            report["human_feedback_score"] = hf
            if composite.get("optimizer_stats"):
                report["optimizer_stats"] = composite["optimizer_stats"]

        # Observability
        try:
            self.metrics.observe("green_metrics_reports_total", 1.0,
                                 region=self.config.region)
            self.metrics.observe("green_metrics_energy_score",
                                 energy_score)
            if "composite_score" in report:
                self.metrics.observe(
                    "green_metrics_composite_score",
                    report["composite_score"],
                    method=report["method"],
                )
        except Exception:  # noqa: BLE001
            pass

        return report

    # ------------------------------------------------------------------
    # Public: training + live feedback
    # ------------------------------------------------------------------
    def record_outcome(self, actual_sustainability: float) -> bool:
        """Train the enhanced optimizer against the true target.

        Delegates to ``SustainabilityIndex.record_outcome`` when available.
        Falls back to training the local fallback weights (not implemented
        for v2 — the fallback is stateless).
        """
        if not self.use_enhancements:
            return False
        if not self._has_record_outcome:
            logger.warning(
                "SustainabilityIndex does not expose record_outcome; "
                "composite training is unavailable"
            )
            return False
        try:
            ok = bool(
                self.sustainability_index.record_outcome(actual_sustainability)
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("record_outcome failed: %s", exc)
            return False
        if ok:
            try:
                self.metrics.observe(
                    "green_metrics_actual_sustainability",
                    float(actual_sustainability),
                )
            except Exception:  # noqa: BLE001
                pass
        return ok

    def record_human_feedback(self, score: float) -> None:
        score = _clamp(_finite_nonneg(score, "human_feedback_score"))
        with self._lock:
            self._human_feedback_score = score
        if self._has_record_human_feedback:
            try:
                self.sustainability_index.record_human_feedback(score)
            except Exception as exc:  # noqa: BLE001
                logger.warning("record_human_feedback failed: %s", exc)
        try:
            self.metrics.observe("green_metrics_human_feedback", float(score))
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # Public: stats + persistence
    # ------------------------------------------------------------------
    def get_optimizer_stats(self) -> Optional[Dict[str, Any]]:
        """Return the sub-calculator's optimizer stats when available."""
        getter = getattr(
            self.sustainability_index, "get_optimizer_stats", None
        )
        if callable(getter):
            try:
                return getter()
            except Exception:  # noqa: BLE001
                return None
        return None

    def save(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None:
            return
        for name in ("sustainability_index", "efficiency_scorer",
                     "carbon_calculator", "energy_tracker"):
            sub = getattr(self, name, None)
            if sub is None:
                continue
            saver = getattr(sub, "save", None)
            if not callable(saver):
                continue
            try:
                saver(Path(str(target) + f".{name}.json"))
            except Exception as exc:  # noqa: BLE001
                logger.warning("save(%s) failed: %s", name, exc)

    def load(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None:
            return
        for name in ("sustainability_index", "efficiency_scorer",
                     "carbon_calculator", "energy_tracker"):
            sub = getattr(self, name, None)
            if sub is None:
                continue
            loader = getattr(sub, "load", None)
            if not callable(loader):
                continue
            try:
                loader(Path(str(target) + f".{name}.json"))
            except Exception as exc:  # noqa: BLE001
                logger.warning("load(%s) failed: %s", name, exc)

    # ------------------------------------------------------------------
    # Internal: sub-calculator delegation with defensive fallbacks
    # ------------------------------------------------------------------
    def _calculate_emissions_from_energy(self, energy_kwh: float) -> float:
        calc = getattr(self.carbon_calculator, "calculate_emissions", None)
        if callable(calc):
            try:
                return float(calc(energy_kwh))
            except Exception as exc:  # noqa: BLE001
                logger.debug("calculate_emissions failed: %s", exc)
        # Fallback: GLOBAL intensity × energy.
        return energy_kwh * 0.475

    def _energy_score(self, energy_kwh: float) -> float:
        # Prefer a static ``get_energy_score`` on EnergyTracker, else the
        # manager's own bounded-inverse normalization.
        EnergyTracker = _lazy("energy_tracker", "EnergyTracker")
        static = getattr(EnergyTracker, "get_energy_score", None)
        if callable(static):
            try:
                return float(static(
                    energy_kwh, self.config.energy_scale_kwh
                ))
            except Exception:  # noqa: BLE001
                pass
        return _bounded_inverse(energy_kwh, self.config.energy_scale_kwh)

    def _efficiency_raw(self, accuracy: float, energy_kwh: float) -> float:
        method = getattr(
            self.efficiency_scorer, "calculate_efficiency_score", None
        )
        if callable(method):
            try:
                return float(method(accuracy, energy_kwh))
            except Exception as exc:  # noqa: BLE001
                logger.debug("calculate_efficiency_score failed: %s", exc)
        return (accuracy / energy_kwh) if energy_kwh > 0 else 0.0

    def _sustainability_raw(
        self, accuracy: float, energy_kwh: float, carbon_co2e_kg: float
    ) -> float:
        method = getattr(self.sustainability_index, "calculate", None)
        if callable(method):
            try:
                return float(method(accuracy, energy_kwh, carbon_co2e_kg))
            except Exception as exc:  # noqa: BLE001
                logger.debug("sustainability calculate failed: %s", exc)
        return 0.0

    def _sustainability_normalized(
        self, accuracy: float, energy_kwh: float, carbon_co2e_kg: float
    ) -> float:
        method = getattr(
            self.sustainability_index, "calculate_normalized", None
        )
        if callable(method):
            try:
                return float(method(accuracy, energy_kwh, carbon_co2e_kg))
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "sustainability calculate_normalized failed: %s", exc
                )
        # Fallback: bounded-inverse of the raw ratio.
        raw = self._sustainability_raw(accuracy, energy_kwh, carbon_co2e_kg)
        return _bounded_inverse(raw, 100.0)

    # ------------------------------------------------------------------
    # Internal: composite score
    # ------------------------------------------------------------------
    def _compute_composite_score(
        self,
        *,
        energy_kwh: float,
        carbon_co2e_kg: float,
        accuracy: float,
        latency_ms: float,
        cost_usd: float,
        graph_metrics: Mapping[str, float],
        human_feedback: float,
    ) -> Dict[str, Any]:
        # Preferred path: delegate to SustainabilityIndex.calculate_enhanced,
        # which is the file 32 v2 optimizer. It has a real trained student,
        # trained gate, and (optionally) an evolutionary optimizer.
        if self._has_enhanced_sustainability:
            try:
                result = self.sustainability_index.calculate_enhanced(
                    accuracy=accuracy,
                    energy_kwh=energy_kwh,
                    carbon_co2e_kg=carbon_co2e_kg,
                    latency_ms=latency_ms,
                    cost_usd=cost_usd,
                    graph_metrics=graph_metrics,
                    human_feedback_score=human_feedback,
                )
                with self._lock:
                    self._pending = {
                        "delegated_to": "sustainability_index",
                        "result": result,
                    }
                return {
                    "score": float(result["score"]),
                    "method": result["method"],
                    "optimizer_stats": result.get("optimizer_stats"),
                }
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "SustainabilityIndex.calculate_enhanced failed: %s", exc
                )

        # Fallback: local MODP composite with bounded-inverse normalization.
        energy_norm = _bounded_inverse(
            energy_kwh, self.config.energy_scale_kwh
        )
        carbon_norm = _bounded_inverse(
            carbon_co2e_kg, self.config.carbon_scale_kg
        )
        latency_norm = _bounded_inverse(
            latency_ms, self.config.latency_scale_ms
        )
        acc_norm = _clamp(accuracy)

        w = self._modp_weights
        score = _clamp(
            w[0] * energy_norm
            + w[1] * carbon_norm
            + w[2] * latency_norm
            + w[3] * acc_norm
        )

        # Human feedback tilt: ±5% at the extremes.
        tilt = (human_feedback - 0.5) * 0.1
        score = _clamp(score + tilt)

        return {"score": score, "method": "modp", "optimizer_stats": None}


# ============================================================================
# Section 6 — Lazy imports (PEP 562)
# ============================================================================

_LAZY_SUBMODULES: Dict[str, str] = {
    "EnergyTracker": "energy_tracker",
    "CarbonCalculator": "carbon_calculator",
    "EfficiencyScorer": "efficiency_scorer",
    "SustainabilityIndex": "sustainability_index",
    "GreenMetricsManager": "__init__",  # self (no lazy import)
    "GreenMetricsConfig": "__init__",   # self (no lazy import)
    "MetricsSink": "__init__",
    "InMemoryMetrics": "__init__",
    "LoggingMetrics": "__init__",
    "MODP_SCHEMA": "__init__",
}


def _lazy(submodule: str, attr: str) -> Any:
    """Import ``attr`` from ``green_metrics.<submodule>`` lazily."""
    if submodule == "__init__":
        return globals().get(attr)
    full_name = f"{__name__}.{submodule}"
    try:
        module = importlib.import_module(full_name)
    except ImportError as exc:
        raise ImportError(
            f"green_metrics.{attr} is unavailable: failed to import "
            f"{full_name}. Cause: {exc}. "
            f"Ensure all dependencies of the sub-module are installed."
        ) from exc
    return getattr(module, attr)


def _construct_with_config(cls: type, config: Any, **extra: Any) -> Any:
    """Construct ``cls`` with ``config=config`` when accepted; fall back
    to ``cls(**extra)`` when the constructor doesn't accept a config."""
    try:
        return cls(config=config, **extra)
    except TypeError:
        # Sub-calculator doesn't accept a config or doesn't accept our
        # extra kwargs. Try the extra kwargs only.
        try:
            return cls(**extra)
        except TypeError:
            return cls()


def __getattr__(name: str) -> Any:
    if name in _LAZY_SUBMODULES:
        submodule = _LAZY_SUBMODULES[name]
        if submodule == "__init__":
            # For self-resident names, they should already be in globals().
            if name in globals():
                return globals()[name]
        return _lazy(submodule, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> List[str]:
    return sorted(
        list(globals().keys())
        + [n for n, m in _LAZY_SUBMODULES.items() if m != "__init__"]
    )


def available_components() -> Dict[str, bool]:
    """Return a dict indicating which re-exported components can be imported.

    Useful for pre-flight checks in operators / tests.
    """
    result: Dict[str, bool] = {}
    for name, submodule in _LAZY_SUBMODULES.items():
        if submodule == "__init__":
            result[name] = True
            continue
        try:
            getattr(__import__(__name__), name)
            result[name] = True
        except Exception as exc:  # noqa: BLE001
            logger.warning("Component %s unavailable: %s", name, exc)
            result[name] = False
    return result


# ============================================================================
# Section 7 — Public surface
# ============================================================================

__all__ = [
    "EnergyTracker",
    "CarbonCalculator",
    "EfficiencyScorer",
    "SustainabilityIndex",
    "GreenMetricsManager",
    "GreenMetricsConfig",
    "MetricsSink",
    "InMemoryMetrics",
    "LoggingMetrics",
    "MODP_SCHEMA",
    "available_components",
]
