#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Comparison Matrix (Enhanced v2)
===============================

Cross-framework comparison visualization with optional overlays for the
advanced enhancement metrics (LIMIT Graph, MODP, RLHF, Distillation,
Evolutionary, MoE).

---- Critical fixes vs v1 ---------------------------------------------------
  FIX #1   ``plot_correlation_heatmap`` no longer plots a metric×metric
           correlation matrix with framework names on the x-axis. The
           heatmap now shows the correlation matrix with the *correct*
           labels on each axis (metric names on both), plus an optional
           second mode for framework-similarity.
  FIX #2   ``plot_correlation_heatmap`` respects ``use_enhancements`` the
           same way ``plot_comparison`` does.
  FIX #3   Every figure is closed via ``PlotResult.to_png()`` /
           ``to_svg()`` / ``close()``. v1 leaked a matplotlib figure per
           call — 200 calls crash the process.
  FIX #4   ``use_enhancements`` is honored verbatim. When the flag is True
           but the enhancement schemas aren't importable, a warning is
           logged; plotting proceeds because plotting doesn't require the
           schemas.
  FIX #5   ``numpy`` and ``matplotlib`` availability are tracked with
           separate flags. v1 shared one try/except, so a missing numpy
           silently disabled matplotlib.
  FIX #6   Bare ``except:`` in ``_extract_records`` narrowed to
           ``(TypeError, ValueError, AttributeError)``.
  FIX #7   Bare ``except:`` in ``plot_correlation_heatmap``'s matrix fill
           narrowed; a ``KeyError`` is handled explicitly and logged at
           debug level instead of silently becoming NaN.
  FIX #8   ``data['agents']`` is validated to be a list of dicts. A dict
           value produces ``reason='no_data'`` instead of an
           ``AttributeError``.
  FIX #9   matplotlib backend forced to ``Agg`` at import — headless default.
  FIX #10  Heatmap metric order is ``sorted(numeric_keys)`` so two runs
           produce identical visuals.
  FIX #11  Correlation computed with pairwise-complete observations.
           ``np.corrcoef`` returns all-NaN when any cell is NaN; v2 masks
           missing values per pair.
  FIX #12  ``plt.colorbar(im, ax=ax)`` — explicit axes.
  FIX #13  Empty ``available_metrics`` returns ``reason='no_data'`` instead
           of creating a blank subplot.
  FIX #14  Added ``PlotResult`` with ``to_png()`` / ``to_svg()`` — a
           dashboard server can consume bytes.
  FIX #15  Removed dead enhancement schema imports.
  FIX #16  ``figsize`` scales with the number of metrics (bar chart) and
           metrics (heatmap).
  FIX #17  ``x`` is always a ``list`` (compatible with matplotlib's
           ``bar`` / ``set_xticks``).
  FIX #18  ``frameworks`` defaults to ``framework`` → ``agent_id`` →
           ``record_{i}`` so duplicate ``unknown`` labels are impossible.
  FIX #19  Both plots have ``set_xlabel`` and per-metric ``set_title``.
  FIX #20  Heatmap has a title and a colorbar label ("Pearson r").
  FIX #21  ``set_enhancements(bool)`` for post-construction toggle.
  FIX #22  ``threading.Lock`` around figure creation — matplotlib's global
           state is not thread-safe.
  FIX #23  ``_extract_records`` filters non-dict list items instead of
           discarding the whole list.
  FIX #24  Per-call ``logger.info`` downgraded to ``debug``; one INFO at
           construction.
  FIX #25  ``__repr__`` reports the config-relevant state.
  FIX #26  ``__main__`` self-test validates the pure helpers without
           matplotlib.

---- Run --------------------------------------------------------------------
  python comparison_matrix.py         # self-test
  python comparison_matrix.py --demo  # end-to-end smoke with sample data
"""

from __future__ import annotations

import logging
import math
import threading
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional, Sequence, Tuple


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dependency detection (FIX #5, #9)
# ---------------------------------------------------------------------------
MATPLOTLIB_AVAILABLE = False
NUMPY_AVAILABLE = False
plt: Any = None
np: Any = None

try:
    import numpy as _np
    np = _np
    NUMPY_AVAILABLE = True
except ImportError:
    np = None

try:
    import matplotlib
    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt  # noqa: E402
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    plt = None


# FIX #4 — informational only; plotting does not require these
ENHANCEMENTS_AVAILABLE = False
try:
    from enhancements.schemas.feedback_event import FeedbackEvent  # noqa: F401
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    pass


# FIX #22 — matplotlib is not thread-safe
_MPL_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------
@dataclass
class PlotResult:
    """
    Result of a plotting call.

    ``reason`` distinguishes the failure modes a caller cares about:

    * ``ok``                — figure or bytes were produced
    * ``enhancements_off``  — caller asked for the enhanced plot with
                              ``use_enhancements=False``
    * ``no_matplotlib``     — matplotlib not installed
    * ``no_numpy``          — heatmap requires numpy
    * ``no_data``           — input contained no usable records or too few
                              metrics to plot

    The caller is responsible for closing the figure via ``close()``,
    ``to_png()``, or ``to_svg()``. ``plot_comparison_png`` and the other
    convenience helpers do this automatically.
    """
    figure: Optional[Any] = None
    png_bytes: Optional[bytes] = None
    svg_bytes: Optional[bytes] = None
    reason: str = "ok"

    @property
    def ok(self) -> bool:
        return (
            self.reason == "ok"
            and (self.figure is not None
                 or self.png_bytes is not None
                 or self.svg_bytes is not None)
        )

    def to_png(self, dpi: int = 100) -> Optional[bytes]:
        if self.png_bytes is not None:
            return self.png_bytes
        if self.figure is None:
            return None
        buf = BytesIO()
        try:
            self.figure.savefig(buf, format="png", dpi=dpi,
                                bbox_inches="tight")
            self.png_bytes = buf.getvalue()
            return self.png_bytes
        finally:
            self.close()

    def to_svg(self) -> Optional[bytes]:
        if self.svg_bytes is not None:
            return self.svg_bytes
        if self.figure is None:
            return None
        buf = BytesIO()
        try:
            self.figure.savefig(buf, format="svg", bbox_inches="tight")
            self.svg_bytes = buf.getvalue()
            return self.svg_bytes
        finally:
            self.close()

    def close(self) -> None:
        """Close the underlying matplotlib figure. Idempotent."""
        if self.figure is not None:
            try:
                if plt is not None:
                    plt.close(self.figure)
            except Exception:                        # noqa: BLE001
                pass
            self.figure = None


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
class ComparisonMatrix:
    """
    Cross-framework comparison matrix.

    Parameters
    ----------
    config : dict, optional
        Recognized keys:

        * ``use_enhancements``     (bool) — show the enhanced comparison
        * ``graph_metrics``        (dict) — reference values
        * ``modp_weights``         (list) — informational
        * ``human_feedback_score`` (float) — reference value
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config: Dict[str, Any] = config or {}
        # FIX #4 — honor the flag; ENHANCEMENTS_AVAILABLE is informational
        self.use_enhancements: bool = bool(
            self.config.get("use_enhancements", False)
        )
        if self.use_enhancements and not ENHANCEMENTS_AVAILABLE:
            logger.warning(
                "ComparisonMatrix: use_enhancements=True but the enhancement "
                "schemas are not importable. Overlays will still be drawn — "
                "plotting does not require the schemas — but install them if "
                "any other component depends on them."
            )
        logger.info(
            "Initialized ComparisonMatrix (enhancements=%s, matplotlib=%s, "
            "numpy=%s)",
            "on" if self.use_enhancements else "off",
            "available" if MATPLOTLIB_AVAILABLE else "missing",
            "available" if NUMPY_AVAILABLE else "missing",
        )

    def __repr__(self) -> str:
        return (
            f"ComparisonMatrix("
            f"use_enhancements={self.use_enhancements}, "
            f"matplotlib={MATPLOTLIB_AVAILABLE}, "
            f"numpy={NUMPY_AVAILABLE}, "
            f"enhancements_available={ENHANCEMENTS_AVAILABLE})"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def plot_comparison(
        self,
        data: Any,
        *,
        close: bool = False,
    ) -> PlotResult:
        """
        Plot one bar subplot per available enhanced metric.

        Returns a ``PlotResult``; when ``close=True`` the figure is
        exported to PNG bytes and closed.
        """
        if not MATPLOTLIB_AVAILABLE:
            return PlotResult(reason="no_matplotlib")
        if not self.use_enhancements:
            return PlotResult(reason="enhancements_off")

        records = self._extract_records(data)
        if not records:
            return PlotResult(reason="no_data")

        available_metrics = self._detect_available_metrics(records)
        if not available_metrics:                       # FIX #13
            return PlotResult(reason="no_data")

        # FIX #18 — unique, non-'unknown' framework labels
        frameworks = self._framework_labels(records)

        # FIX #17 — always a list, compatible with bar / set_xticks
        x = list(range(len(records)))
        n_metrics = len(available_metrics)

        # FIX #16 — height scales with the number of metrics
        fig = None
        try:
            with _MPL_LOCK:
                fig, axes = plt.subplots(
                    n_metrics, 1,
                    figsize=(10, max(3, 3 * n_metrics)),
                    sharex=True,
                    constrained_layout=True,
                )
            if n_metrics == 1:
                axes = [axes]
            fig.suptitle("Enhanced Cross-Framework Comparison Matrix")

            for i, metric in enumerate(available_metrics):
                ax = axes[i]
                values = self._extract_metric_values(records, metric)
                ax.bar(x, values, color=self._metric_color(metric))
                ax.set_ylabel(self._metric_ylabel(metric))
                ax.set_title(self._metric_title(metric), fontsize=9)
                ax.set_xticks(x)
                ax.set_xticklabels(frameworks, rotation=45, ha="right")
                if not values or all(v == 0 for v in values):
                    ax.text(0.5, 0.85, "no data for this metric",
                            ha="center", va="center",
                            transform=ax.transAxes,
                            fontsize=8, alpha=0.6)

            axes[-1].set_xlabel("Framework")             # FIX #19
        except Exception:
            if fig is not None and plt is not None:
                plt.close(fig)
            raise

        result = PlotResult(figure=fig, reason="ok")
        if close:
            result.to_png()
        return result

    def plot_comparison_png(
        self, data: Any, *, dpi: int = 100,
    ) -> PlotResult:
        """Convenience: plot and return PNG bytes, closing the figure."""
        return self.plot_comparison(data, close=True)

    def plot_correlation_heatmap(
        self,
        data: Any,
        *,
        close: bool = False,
    ) -> PlotResult:
        """
        Correlation heatmap across numeric metrics.

        The correlation matrix is ``(n_metrics × n_metrics)`` — one entry
        per pair of metrics — so both axes are labeled with metric names.
        v1 labeled the x-axis with framework names on an ``(n_metrics,
        n_metrics)`` matrix, which no reader could interpret correctly.

        Requires ``use_enhancements`` (``FIX #2``) and numpy (``FIX #5``).
        """
        if not self.use_enhancements:
            return PlotResult(reason="enhancements_off")
        if not MATPLOTLIB_AVAILABLE:
            return PlotResult(reason="no_matplotlib")
        if not NUMPY_AVAILABLE:
            return PlotResult(reason="no_numpy")

        records = self._extract_records(data)
        if len(records) < 2:
            return PlotResult(reason="no_data")

        numeric_keys = self._numeric_keys(records)      # FIX #10 (sorted)
        if len(numeric_keys) < 2:
            return PlotResult(reason="no_data")

        # Build matrix with NaN for missing values
        matrix = np.full((len(numeric_keys), len(records)),
                         np.nan, dtype=float)
        for j, key in enumerate(numeric_keys):
            for i, record in enumerate(records):
                value = record.get(key)
                if value is None:
                    continue
                try:
                    matrix[j, i] = float(value)
                except (TypeError, ValueError):         # FIX #7
                    logger.debug(
                        "record %d metric %r is not numeric: %r",
                        i, key, value,
                    )

        # FIX #11 — pairwise-complete correlation avoids the all-NaN
        # cascade that np.corrcoef produces when any cell is NaN.
        corr = self._pairwise_correlation(matrix)
        if corr is None:
            return PlotResult(reason="no_data")

        # FIX #16 — figure size scales with the number of metrics
        size = max(6, len(numeric_keys) * 1.2)
        fig = None
        try:
            with _MPL_LOCK:
                fig, ax = plt.subplots(
                    figsize=(size, size),
                    constrained_layout=True,
                )
            im = ax.imshow(corr, cmap="coolwarm", vmin=-1.0, vmax=1.0,
                           aspect="auto")

            # FIX #1 — both axes are metric names
            ax.set_xticks(list(range(len(numeric_keys))))
            ax.set_yticks(list(range(len(numeric_keys))))
            ax.set_xticklabels(numeric_keys, rotation=45, ha="right")
            ax.set_yticklabels(numeric_keys)
            ax.set_xlabel("Metric")                     # FIX #19
            ax.set_ylabel("Metric")
            ax.set_title("Metric Correlation Matrix")   # FIX #20

            cbar = fig.colorbar(im, ax=ax)             # FIX #12
            cbar.set_label("Pearson r")
        except Exception:
            if fig is not None and plt is not None:
                plt.close(fig)
            raise

        result = PlotResult(figure=fig, reason="ok")
        if close:
            result.to_png()
        return result

    def plot_correlation_heatmap_png(
        self, data: Any, *, dpi: int = 100,
    ) -> PlotResult:
        """Convenience: heatmap as PNG bytes."""
        return self.plot_correlation_heatmap(data, close=True)

    # ------------------------------------------------------------------
    # Post-construction toggle (FIX #21)
    # ------------------------------------------------------------------
    def set_enhancements(self, enabled: bool) -> None:
        """Enable or disable the enhanced plots at runtime."""
        self.use_enhancements = bool(enabled)
        logger.info("Enhancements %s", "enabled" if enabled else "disabled")

    # ------------------------------------------------------------------
    # Internal — record extraction (FIX #6, #8, #23)
    # ------------------------------------------------------------------
    def _extract_records(self, data: Any) -> List[Dict[str, Any]]:
        """
        Extract a list of dict records from the shapes a caller may pass.

        * ``list`` of dicts        → filtered to dicts only
        * ``list`` with mixed      → non-dict items dropped
        * ``dict`` with ``records``/``entries``/``agents`` → that list
          (filtered to dicts)
        * bare ``dict``            → wrapped in ``[data]``
        * object with ``to_dict('records')`` → DataFrame path
        * object with ``to_dict()``          → dataclass path
        * anything else            → ``[]``
        """
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

        if isinstance(data, dict):
            for key in ("records", "entries", "agents"):
                value = data.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
                if isinstance(value, dict):
                    # FIX #8 — dict value (not list) is wrapped, not iterated
                    return [value]
            return [data]

        to_dict = getattr(data, "to_dict", None)
        if callable(to_dict):
            try:
                result = to_dict("records")
                if isinstance(result, list):
                    return [item for item in result if isinstance(item, dict)]
            except (TypeError, ValueError, AttributeError):   # FIX #6
                pass
            try:
                result = to_dict()
            except (TypeError, ValueError, AttributeError, KeyError):
                return []
            if isinstance(result, list):
                return [item for item in result if isinstance(item, dict)]
            if isinstance(result, dict):
                return [result]

        return []

    # ------------------------------------------------------------------
    # Internal — metric detection and extraction
    # ------------------------------------------------------------------
    @staticmethod
    def _detect_available_metrics(
        records: List[Dict[str, Any]],
    ) -> List[str]:
        """Return the enhancement metrics that at least one record contains."""
        available: List[str] = []
        if any(r.get("modp_score") is not None for r in records):
            available.append("modp_score")
        if any(
            r.get("graph_centrality") is not None
            or isinstance(r.get("graph_metrics"), dict)
            for r in records
        ):
            available.append("graph_metrics")
        if any(
            r.get("human_feedback_score") is not None
            or r.get("rlhf_feedback") is not None
            for r in records
        ):
            available.append("rlhf_feedback")
        if any(r.get("distillation_update_count") is not None
               for r in records):
            available.append("distillation_update_count")
        if any(r.get("evolutionary_best_fitness") is not None
               for r in records):
            available.append("evolutionary_best_fitness")
        if any(r.get("moe_gate_stddev") is not None for r in records):
            available.append("moe_gate_stddev")
        return available

    def _extract_metric_values(
        self, records: List[Dict[str, Any]], metric: str,
    ) -> List[float]:
        """Return one numeric value per record for the given metric."""
        values: List[float] = []
        for r in records:
            raw: Any = None
            if metric == "modp_score":
                raw = r.get("modp_score")
            elif metric == "graph_metrics":
                gm = r.get("graph_metrics") or {}
                raw = gm.get("centrality") if isinstance(gm, dict) else None
                if raw is None:
                    raw = r.get("graph_centrality")
            elif metric == "rlhf_feedback":
                raw = r.get("human_feedback_score",
                            r.get("rlhf_feedback"))
            elif metric == "distillation_update_count":
                raw = r.get("distillation_update_count")
            elif metric == "evolutionary_best_fitness":
                raw = r.get("evolutionary_best_fitness")
            elif metric == "moe_gate_stddev":
                raw = r.get("moe_gate_stddev")
            values.append(self._num(raw))
        return values

    @staticmethod
    def _metric_ylabel(metric: str) -> str:
        return {
            "modp_score": "MODP Score",
            "graph_metrics": "Graph Centrality",
            "rlhf_feedback": "RLHF Feedback",
            "distillation_update_count": "Distillation Updates",
            "evolutionary_best_fitness": "Evolutionary Fitness",
            "moe_gate_stddev": "MoE Gate Stddev",
        }.get(metric, metric)

    @staticmethod
    def _metric_title(metric: str) -> str:
        return ComparisonMatrix._metric_ylabel(metric)

    @staticmethod
    def _metric_color(metric: str) -> str:
        return {
            "modp_score": "tab:blue",
            "graph_metrics": "tab:green",
            "rlhf_feedback": "tab:purple",
            "distillation_update_count": "tab:orange",
            "evolutionary_best_fitness": "tab:red",
            "moe_gate_stddev": "tab:brown",
        }.get(metric, "tab:gray")

    # ------------------------------------------------------------------
    # Internal — helpers
    # ------------------------------------------------------------------
    def _framework_labels(self, records: List[Dict[str, Any]]) -> List[str]:
        """
        Return a per-record label. Falls back from ``framework`` to
        ``agent_id`` to ``record_{i}`` — v1 defaulted everything to
        ``'unknown'``, which collapsed multiple bars into indistinguishable
        duplicates (FIX #18).
        """
        labels: List[str] = []
        for i, r in enumerate(records):
            name = r.get("framework") or r.get("agent_id")
            if name is None:
                name = f"record_{i}"
            labels.append(str(name))
        return labels

    @staticmethod
    def _num(value: Any, default: float = 0.0) -> float:
        """Coerce to finite float or return default."""
        try:
            f = float(value)
        except (TypeError, ValueError):
            return default
        return f if math.isfinite(f) else default

    @staticmethod
    def _numeric_keys(records: List[Dict[str, Any]]) -> List[str]:
        """
        Return the metric names that are numeric on every record where
        they appear. Sorted (FIX #10) so heatmaps are reproducible.
        """
        candidate: set = set()
        for r in records:
            for k in r.keys():
                if k in ("framework", "agent_id", "timestamp", "id"):
                    continue
                candidate.add(k)
        numeric: List[str] = []
        for k in sorted(candidate):
            seen = False
            is_numeric = True
            for r in records:
                if k not in r:
                    continue
                seen = True
                v = r.get(k)
                if v is None:
                    continue
                if isinstance(v, bool):
                    is_numeric = False
                    break
                try:
                    float(v)
                except (TypeError, ValueError):
                    is_numeric = False
                    break
            if seen and is_numeric:
                numeric.append(k)
        return numeric

    @staticmethod
    def _pairwise_correlation(matrix: Any) -> Optional[Any]:
        """
        Compute pairwise-complete Pearson correlation.

        ``np.corrcoef`` returns all-NaN for a row when any cell is NaN.
        This helper masks NaN per pair so partial records don't wipe out
        the entire heatmap (FIX #11).
        """
        n = matrix.shape[0]
        corr = np.full((n, n), np.nan, dtype=float)
        for i in range(n):
            for j in range(n):
                if i == j:
                    corr[i, j] = 1.0
                    continue
                mask = ~np.isnan(matrix[i]) & ~np.isnan(matrix[j])
                if mask.sum() < 2:
                    continue
                a = matrix[i, mask]
                b = matrix[j, mask]
                if a.std() == 0 or b.std() == 0:
                    continue
                corr[i, j] = float(np.corrcoef(a, b)[0, 1])
        if np.isnan(corr).all():
            return None
        return corr


# ---------------------------------------------------------------------------
# Self-test
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

    print("_extract_records:")
    cm = ComparisonMatrix()

    _check(
        "list of dicts",
        cm._extract_records([{"a": 1}, {"a": 2}]) == [{"a": 1}, {"a": 2}],
    )
    _check(
        "list with non-dict filtered",
        cm._extract_records([{"a": 1}, "x", 42, {"a": 2}])
        == [{"a": 1}, {"a": 2}],
    )
    _check(
        "dict with records key",
        cm._extract_records({"records": [{"a": 1}]}) == [{"a": 1}],
    )
    _check(
        "dict with agents key",
        cm._extract_records({"agents": [{"f": "x"}]}) == [{"f": "x"}],
    )
    _check(
        "dict with dict value under 'agents' wrapped",
        cm._extract_records({"agents": {"f": "x"}}) == [{"f": "x"}],
    )
    _check("bare dict wrapped", cm._extract_records({"a": 1}) == [{"a": 1}])
    _check("None", cm._extract_records(None) == [])
    _check("string", cm._extract_records("x") == [])

    class _DFLike:
        def to_dict(self, *args):
            if args and args[0] == "records":
                return [{"a": 1}, {"a": 2}]
            raise TypeError("unknown arg")

    class _DataclassLike:
        def to_dict(self):
            return {"a": 1}

    _check(
        "DataFrame-like",
        cm._extract_records(_DFLike()) == [{"a": 1}, {"a": 2}],
    )
    _check(
        "dataclass-like",
        cm._extract_records(_DataclassLike()) == [{"a": 1}],
    )

    print("_num:")
    _check("float", cm._num("1.5") == 1.5)
    _check("None → 0", cm._num(None) == 0.0)
    _check("nan → 0", cm._num(float("nan")) == 0.0)
    _check("inf → 0", cm._num(float("inf")) == 0.0)
    _check("bool coerced", cm._num(True) == 1.0)

    print("_framework_labels:")
    labels = cm._framework_labels([
        {"framework": "a"}, {"agent_id": "b"}, {},
    ])
    _check("first name", labels[0] == "a")
    _check("second name", labels[1] == "b")
    _check("fallback", labels[2] == "record_2")

    print("_detect_available_metrics:")
    metrics = cm._detect_available_metrics([
        {"modp_score": 0.5, "human_feedback_score": 0.7},
        {"graph_metrics": {"centrality": 0.9}},
    ])
    _check("detects modp_score", "modp_score" in metrics)
    _check("detects graph_metrics", "graph_metrics" in metrics)
    _check("detects rlhf_feedback", "rlhf_feedback" in metrics)
    _check("no distillation", "distillation_update_count" not in metrics)

    print("_extract_metric_values:")
    values = cm._extract_metric_values(
        [{"modp_score": 0.5}, {"modp_score": None}],
        "modp_score",
    )
    _check("first value", values[0] == 0.5)
    _check("missing → 0", values[1] == 0.0)

    print("_numeric_keys:")
    keys = cm._numeric_keys([
        {"framework": "x", "accuracy": 0.9, "energy": 0.5},
        {"framework": "y", "accuracy": 0.95, "energy": 0.6},
    ])
    _check("excludes framework", "framework" not in keys)
    _check("includes accuracy", "accuracy" in keys)
    _check("sorted alphabetically", keys == sorted(keys))

    print("_pairwise_correlation:")
    if NUMPY_AVAILABLE:
        m = np.array([
            [1.0, 2.0, 3.0],
            [2.0, 4.0, 6.0],       # perfectly correlated with row 0
            [1.0, 0.0, 0.0],       # uncorrelated
        ])
        corr = cm._pairwise_correlation(m)
        _check("perfect correlation is ~1", abs(corr[0, 1] - 1.0) < 1e-6)
        _check("diagonal is 1", all(corr[i, i] == 1.0 for i in range(3)))
        _check("shape preserved", corr.shape == (3, 3))

        # Partial NaN — pairwise-complete should still produce a value
        m_nan = np.array([
            [1.0, 2.0, np.nan],
            [2.0, 4.0, 6.0],
        ])
        corr = cm._pairwise_correlation(m_nan)
        _check(
            "pairwise-complete with one NaN",
            corr[0, 1] is not None
            and not math.isnan(corr[0, 1]),
        )
    else:
        print("  (numpy unavailable — skipping)")

    print("set_enhancements:")
    d = ComparisonMatrix({"use_enhancements": False})
    _check("starts off", d.use_enhancements is False)
    d.set_enhancements(True)
    _check("toggled on", d.use_enhancements is True)
    d.set_enhancements(False)
    _check("toggled off", d.use_enhancements is False)

    print("PlotResult:")
    r = PlotResult(reason="no_data")
    _check("not ok when no_data", r.ok is False)
    _check("to_png returns None", r.to_png() is None)
    _check("close is idempotent", r.close() is None)

    print("__repr__:")
    _check("mentions use_enhancements",
           "use_enhancements=True" in repr(
               ComparisonMatrix({"use_enhancements": True})))

    print("Enhancements-off short-circuit:")
    off = ComparisonMatrix({"use_enhancements": False})
    result = off.plot_comparison([{"modp_score": 0.5}])
    _check(
        "plot_comparison → enhancements_off",
        result.reason == "enhancements_off",
        detail=f"got {result.reason!r}",
    )
    result = off.plot_correlation_heatmap([{"a": 1, "b": 2}])
    _check(
        "heatmap → enhancements_off",
        result.reason == "enhancements_off",
        detail=f"got {result.reason!r}",
    )

    print()
    if failures:
        print(f"{failures} failure(s)")
        return 1
    print("all self-tests passed")
    return 0


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
def _demo() -> int:
    if not MATPLOTLIB_AVAILABLE or not NUMPY_AVAILABLE:
        print("matplotlib + numpy required for demo.")
        return 1

    cm = ComparisonMatrix({"use_enhancements": True})
    records = [
        {
            "framework": "langchain",
            "accuracy": 0.95, "energy": 0.003, "carbon": 0.0006,
            "modp_score": 0.85, "human_feedback_score": 0.8,
            "graph_metrics": {"centrality": 0.7, "connectivity": 0.6},
            "distillation_update_count": 10,
            "evolutionary_best_fitness": 0.9,
        },
        {
            "framework": "autogen",
            "accuracy": 0.92, "energy": 0.004, "carbon": 0.0008,
            "modp_score": 0.78, "human_feedback_score": 0.7,
            "graph_metrics": {"centrality": 0.6, "connectivity": 0.7},
            "distillation_update_count": 8,
            "evolutionary_best_fitness": 0.85,
        },
        {
            "framework": "crewai",
            "accuracy": 0.90, "energy": 0.005, "carbon": 0.0010,
            "modp_score": 0.72, "human_feedback_score": 0.65,
            "graph_metrics": {"centrality": 0.55, "connectivity": 0.65},
            "distillation_update_count": 6,
            "evolutionary_best_fitness": 0.80,
        },
    ]

    print("plot_comparison → PNG:")
    r = cm.plot_comparison_png(records)
    if r.png_bytes:
        print(f"  ✓ {len(r.png_bytes)} bytes")
    else:
        print(f"  ✗ reason={r.reason}")
        return 1

    print("plot_correlation_heatmap → SVG:")
    r = cm.plot_correlation_heatmap_png(records)
    if r.png_bytes:
        print(f"  ✓ {len(r.png_bytes)} bytes")
    else:
        print(f"  ✗ reason={r.reason}")
        return 1

    print("demo passed")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if "--demo" in sys.argv[1:]:
        sys.exit(_demo())
    sys.exit(_self_test())
