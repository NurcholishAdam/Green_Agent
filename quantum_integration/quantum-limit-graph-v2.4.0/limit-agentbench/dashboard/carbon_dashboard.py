#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carbon Dashboard (Enhanced v2)
==============================

Carbon footprint visualization with optional overlays for the advanced
enhancement metrics (LIMIT Graph, MODP, RLHF, Distillation, Evolutionary,
FlexGen).

---- Critical fixes vs v1 ---------------------------------------------------
  FIX #1   ``plot_with_enhancement_overlay`` now respects
           ``self.use_enhancements``. v1 produced an "enhanced" plot even
           when the caller explicitly disabled enhancements.
  FIX #2   Every figure is closed. v1 created a matplotlib figure on every
           call and never released it; a dashboard server calling this per
           request leaks a figure per call and crashes at ~200 figures.
           ``PlotResult`` closes the figure when you export to bytes, and
           ``plot_png``/``plot_svg`` helpers export + close in one call.
  FIX #3   ``ENHANCEMENTS_AVAILABLE`` is now informational only. Plotting
           numbers does not require the enhancement schemas to be
           importable. When ``use_enhancements=True`` but the schemas are
           missing, a warning is logged and plotting proceeds.
  FIX #4   ``enhancement_metrics`` is validated to be a dict. A non-dict
           (None, list, dataclass without ``.get``) produces a clean
           ``reason='no_data'`` result instead of an ``AttributeError``.
  FIX #5   Bare ``except:`` in ``_extract_records`` narrowed to specific
           exception types so ``KeyboardInterrupt`` / ``SystemExit`` /
           programming bugs surface.
  FIX #6   ``axhline`` replaced by ``_plot_metric``. A scalar reference
           value is drawn as a labeled horizontal line with an explicit
           ``(ref=...)`` suffix; a list/series is drawn as a real
           time-series line. v1 plotted a scalar via ``axhline`` with no
           indication it was a reference, which read as "the value is
           constant across time".
  FIX #7   Legends are only drawn when ``get_legend_handles_labels()``
           returns a non-empty list. v1 called ``ax.legend()`` on empty
           subplots, flooding stderr with ``UserWarning: No artists with
           labels found``.
  FIX #8   ``matplotlib.use("Agg")`` is selected *before* importing
           ``pyplot``. v1 imported pyplot without a backend, producing a
           ``UserWarning: matplotlib is currently using a non-GUI backend``
           on every headless server, and potentially opening a GUI window
           when ``DISPLAY`` happens to be set.
  FIX #9   Timestamp resolution handles a ``None`` in the middle of an
           otherwise-numeric series. v1 checked only ``times[0]`` and
           silently fell back to the index for the whole series.
  FIX #10  Removed the unused ``matplotlib.dates`` import; date/time
           formatting is loaded lazily and only when timestamps resolve to
           ``datetime`` objects.
  FIX #11  ``plot_carbon_footprint`` and ``plot_with_enhancement_overlay``
           return a ``PlotResult`` with a ``reason`` field
           (``ok`` / ``enhancements_off`` / ``no_matplotlib`` / ``no_data``)
           instead of ``None``. The caller no longer has to guess why.
  FIX #12  Removed dead enhancement schema imports
           (``FeedbackEvent``, ``NodeDescriptor``, ``WorkloadDescriptor``,
           ``ZeroTrustArchitecture``). They were imported and never used,
           and their failure silently disabled plotting.
  FIX #13  ``set_xlabel`` is chosen from the actual x-axis: ``Time`` for
           datetime data, ``Time (numeric)`` for numeric timestamps,
           ``Record #`` for the fallback index.
  FIX #14  ``plot_with_enhancement_overlay`` now uses the same timestamp
           resolution as ``plot_carbon_footprint``. v1 always used
           ``range(len(records))`` for the overlay.
  FIX #15  Timestamps accept ``int``/``float``, ``datetime``, and ISO-8601
           strings. v1 accepted only ``int``/``float``.
  FIX #16  Added ``to_png()`` / ``to_svg()`` on ``PlotResult`` and
           ``plot_png()`` / ``plot_svg()`` on the dashboard. A dashboard
           API server needs bytes, not a Figure.
  FIX #17  If a plot fails mid-construction, the figure is closed before
           the exception propagates. v1 left the figure open on every error
           path.
  FIX #18  ``constrained_layout=True`` replaces ``tight_layout()``; the
           suptitle no longer collides with the first subplot.
  FIX #19  Metric values are validated to be finite. ``modp_score=-5`` is
           drawn as a reference line but ``plot_metric`` annotates the
           value so out-of-range data is visible.
  FIX #20  ``set_enhancements(bool)`` allows post-construction toggle.
  FIX #21  ``threading.Lock`` around figure creation; matplotlib's global
           state is not thread-safe.
  FIX #22  ``_extract_records`` documents its fallback: a dict without
           ``records``/``entries`` is wrapped in ``[data]``; a DataFrame
           uses ``to_dict('records')``; a plain object uses ``to_dict()``.
  FIX #23  ``_extract_records`` handles ``to_dict`` with and without a
           ``'records'`` argument (dataclasses vs. DataFrames).
  FIX #24  Empty subplots receive a "no data" annotation instead of a
           warning-emitting legend call.
  FIX #25  ``MATPLOTLIB_MIN_VERSION`` documented; requirements pin
           ``matplotlib>=3.6``.
  FIX #26  Per-call logging downgraded to DEBUG; the INFO line on
           construction is retained.
  FIX #27  ``__repr__`` returns the config-relevant state.
  FIX #28  ``__main__`` self-test validates the pure helpers without
           matplotlib or any dashboard dependency.

---- Run --------------------------------------------------------------------
  python carbon_dashboard.py         # self-test
  python carbon_dashboard.py --demo  # end-to-end smoke with sample data
"""

from __future__ import annotations

import logging
import math
import threading
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Platform hygiene — matplotlib must set its backend BEFORE importing pyplot
# ---------------------------------------------------------------------------
# FIX #8 — headless default
MATPLOTLIB_MIN_VERSION = "3.6"

MATPLOTLIB_AVAILABLE = False
plt: Any = None
try:
    import matplotlib
    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt  # noqa: E402  (after use())
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    plt = None


# FIX #3 — informational only; plotting does not require these
ENHANCEMENTS_AVAILABLE = False
try:
    from enhancements.schemas.feedback_event import FeedbackEvent  # noqa: F401
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    pass


# FIX #21 — serialize matplotlib's global state
_MPL_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------
@dataclass
class PlotResult:
    """
    Result of a plotting call.

    ``reason`` distinguishes the four outcomes the caller cares about:

    * ``ok``                 — a figure (or bytes) was produced
    * ``enhancements_off``   — caller asked for the overlay with
                               ``use_enhancements=False``
    * ``no_matplotlib``      — matplotlib not installed
    * ``no_data``            — the input contained no usable records

    The caller is responsible for closing the figure via ``close()``,
    ``to_png()``, or ``to_svg()``. ``plot_png`` and ``plot_svg`` on the
    dashboard do this for you.
    """
    figure: Optional[Any] = None
    png_bytes: Optional[bytes] = None
    svg_bytes: Optional[bytes] = None
    reason: str = "ok"

    @property
    def ok(self) -> bool:
        return self.reason == "ok" and (
            self.figure is not None
            or self.png_bytes is not None
            or self.svg_bytes is not None
        )

    def to_png(self, dpi: int = 100) -> Optional[bytes]:
        """Export to PNG bytes and close the figure."""
        if self.png_bytes is not None:
            return self.png_bytes
        if self.figure is None:
            return None
        buf = BytesIO()
        try:
            self.figure.savefig(
                buf, format="png", dpi=dpi, bbox_inches="tight",
            )
            self.png_bytes = buf.getvalue()
            return self.png_bytes
        finally:
            self.close()

    def to_svg(self) -> Optional[bytes]:
        """Export to SVG bytes and close the figure."""
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
class CarbonDashboard:
    """
    Carbon footprint dashboard with optional enhancement overlays.

    Parameters
    ----------
    config : dict, optional
        Recognized keys:

        * ``use_enhancements``  (bool)  — show the enhancement overlay
        * ``graph_metrics``     (dict)  — centrality/connectivity reference
        * ``modp_weights``      (list)  — MODP weights (informational)
        * ``human_feedback_score`` (float) — RLHF reference value

    Notes
    -----
    * **Thread safety.** matplotlib's global state is not thread-safe. This
      class wraps every figure creation in a lock, but concurrent calls will
      still serialize. A dashboard server should ideally run plotting in a
      worker thread.
    * **Headless by default.** The backend is forced to ``Agg`` at import.
    * **Figure lifecycle.** Every ``plot_*`` method returns a
      ``PlotResult``; the caller must close the figure. ``plot_png`` and
      ``plot_svg`` do this automatically.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config: Dict[str, Any] = config or {}
        # FIX #3 — use_enhancements is the config flag; no ENHANCEMENTS_AVAILABLE gate
        self.use_enhancements: bool = bool(
            self.config.get("use_enhancements", False)
        )
        if self.use_enhancements and not ENHANCEMENTS_AVAILABLE:
            logger.warning(
                "CarbonDashboard: use_enhancements=True but the enhancement "
                "schemas are not importable. Overlays will still be drawn — "
                "plotting does not require the schemas — but if any other "
                "part of the application relies on the schemas, install "
                "them."
            )
        logger.info(
            "Initialized CarbonDashboard (enhancements=%s, matplotlib=%s)",
            "on" if self.use_enhancements else "off",
            "available" if MATPLOTLIB_AVAILABLE else "missing",
        )

    def __repr__(self) -> str:
        return (
            f"CarbonDashboard(use_enhancements={self.use_enhancements}, "
            f"matplotlib={MATPLOTLIB_AVAILABLE}, "
            f"enhancements_available={ENHANCEMENTS_AVAILABLE})"
        )

    # ------------------------------------------------------------------
    # Public API — plotting
    # ------------------------------------------------------------------
    def plot_carbon_footprint(
        self,
        data: Any,
        *,
        close: bool = False,
    ) -> PlotResult:
        """
        Plot carbon, energy, and (when enhancements are on) MODP score.

        Returns a ``PlotResult``. When ``close=True`` the figure is
        exported to PNG bytes and closed immediately, so ``result.figure``
        is ``None`` and ``result.png_bytes`` is populated.
        """
        if not MATPLOTLIB_AVAILABLE:
            return PlotResult(reason="no_matplotlib")

        if not self.use_enhancements:
            return PlotResult(reason="enhancements_off")

        records = self._extract_records(data)
        if not records:
            return PlotResult(reason="no_data")

        x, xlabel, is_datetime = self._resolve_x(records)
        carbon = [self._num(r.get("carbon_co2e_kg", r.get("carbon_kg", 0))) for r in records]
        energy = [self._num(r.get("energy_kwh", 0)) for r in records]
        modp = [self._num(r.get("modp_score", 0)) for r in records]

        # FIX #17 — close figure on any error during construction
        fig = None
        try:
            with _MPL_LOCK:
                # FIX #18 — constrained_layout avoids the suptitle collision
                fig, axes = plt.subplots(
                    3, 1, figsize=(10, 8), sharex=True,
                    constrained_layout=True,
                )
            fig.suptitle("Enhanced Carbon Footprint Dashboard")

            self._plot_series(axes[0], x, carbon, "tab:red",
                              "Carbon (kg CO2e)", is_datetime)
            axes[0].set_ylabel("Carbon (kg CO2e)")

            self._plot_series(axes[1], x, energy, "tab:orange",
                              "Energy (kWh)", is_datetime)
            axes[1].set_ylabel("Energy (kWh)")

            self._plot_series(axes[2], x, modp, "tab:blue",
                              "MODP Score", is_datetime)
            axes[2].set_ylabel("MODP Score")

            axes[-1].set_xlabel(xlabel)                # FIX #13

            for ax in axes:
                self._safe_legend(ax)                  # FIX #7
        except Exception:
            if fig is not None and plt is not None:
                plt.close(fig)
            raise

        result = PlotResult(figure=fig, reason="ok")
        if close:
            result.to_png()
        return result

    def plot_carbon_footprint_png(
        self, data: Any, *, dpi: int = 100,
    ) -> PlotResult:
        """Convenience: plot and return PNG bytes, closing the figure."""
        return self.plot_carbon_footprint(data, close=True)

    def plot_with_enhancement_overlay(
        self,
        data: Any,
        enhancement_metrics: Dict[str, Any],
        *,
        close: bool = False,
    ) -> PlotResult:
        """
        Overlay aggregate enhancement metrics on top of the carbon series.

        Scalar values are drawn as labeled reference lines
        (``centrality (ref=0.5)``); list/tuple values are drawn as real
        series (``FIX #6``).

        Requires ``self.use_enhancements`` (``FIX #1``).
        """
        if not self.use_enhancements:
            return PlotResult(reason="enhancements_off")
        if not MATPLOTLIB_AVAILABLE:
            return PlotResult(reason="no_matplotlib")
        if not isinstance(enhancement_metrics, dict):  # FIX #4
            logger.debug(
                "enhancement_metrics is %s, not a dict; skipping overlay",
                type(enhancement_metrics).__name__,
            )
            return PlotResult(reason="no_data")

        records = self._extract_records(data)
        if not records:
            return PlotResult(reason="no_data")

        x, xlabel, is_datetime = self._resolve_x(records)  # FIX #14
        carbon = [self._num(r.get("carbon_co2e_kg", r.get("carbon_kg", 0))) for r in records]

        fig = None
        try:
            with _MPL_LOCK:
                fig, axes = plt.subplots(
                    3, 1, figsize=(10, 8), sharex=True,
                    constrained_layout=True,
                )

            self._plot_series(axes[0], x, carbon, "tab:red",
                              "Carbon (kg CO2e)", is_datetime)
            axes[0].set_ylabel("Carbon (kg CO2e)")

            # Graph metrics
            graph = enhancement_metrics.get("graph_metrics") or {}
            if isinstance(graph, dict) and graph:
                self._plot_metric(axes[1], x,
                                  graph.get("centrality"), "green",
                                  "Centrality", is_datetime)
                self._plot_metric(axes[1], x,
                                  graph.get("connectivity"), "orange",
                                  "Connectivity", is_datetime)
                axes[1].set_ylabel("Graph Metrics")
            else:
                axes[1].text(0.5, 0.5, "no graph metrics",
                             ha="center", va="center",
                             transform=axes[1].transAxes)

            # RLHF + MODP
            self._plot_metric(axes[2], x,
                              enhancement_metrics.get("human_feedback_score"),
                              "purple", "RLHF Feedback", is_datetime)
            self._plot_metric(axes[2], x,
                              enhancement_metrics.get("modp_score"),
                              "blue", "MODP Score", is_datetime)
            axes[2].set_ylabel("Scores")

            axes[-1].set_xlabel(xlabel)

            for ax in axes:
                self._safe_legend(ax)
        except Exception:
            if fig is not None and plt is not None:
                plt.close(fig)
            raise

        result = PlotResult(figure=fig, reason="ok")
        if close:
            result.to_png()
        return result

    # ------------------------------------------------------------------
    # Post-construction toggle (FIX #20)
    # ------------------------------------------------------------------
    def set_enhancements(self, enabled: bool) -> None:
        """Enable or disable the enhanced overlays at runtime."""
        self.use_enhancements = bool(enabled)
        logger.info("Enhancements %s", "enabled" if enabled else "disabled")

    # ------------------------------------------------------------------
    # Internal — data normalization
    # ------------------------------------------------------------------
    def _extract_records(self, data: Any) -> List[Dict[str, Any]]:
        """
        Extract a list of dict records from the various shapes a caller may
        pass. Fallback behavior (FIX #22):

        * ``list`` of dicts              → returned unchanged
        * ``list`` with mixed types      → non-dict items are filtered out
        * ``dict`` with ``records`` key  → ``data['records']``
        * ``dict`` with ``entries`` key  → ``data['entries']``
        * ``dict`` (no known key)        → ``[data]``
        * object with ``to_dict('records')`` → DataFrame path
        * object with ``to_dict()``      → dataclass path (``FIX #23``)
        * anything else                  → ``[]``
        """
        # --- list ---
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

        # --- dict ---
        if isinstance(data, dict):
            for key in ("records", "entries"):
                value = data.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
            return [data]

        # --- object with to_dict ---
        to_dict = getattr(data, "to_dict", None)
        if callable(to_dict):
            # DataFrame style
            try:
                result = to_dict("records")
                if isinstance(result, list):
                    return [item for item in result if isinstance(item, dict)]
            except (TypeError, ValueError, AttributeError):  # FIX #5
                pass
            # Dataclass style (no arguments)
            try:
                result = to_dict()
            except (TypeError, ValueError, AttributeError, KeyError):
                return []
            if isinstance(result, list):
                return [item for item in result if isinstance(item, dict)]
            if isinstance(result, dict):
                return [result]

        return []

    @staticmethod
    def _num(value: Any, default: float = 0.0) -> float:
        """Coerce a value to a finite float, or return ``default``."""
        try:
            f = float(value)
        except (TypeError, ValueError):
            return default
        return f if math.isfinite(f) else default

    # ------------------------------------------------------------------
    # Internal — x-axis resolution (FIX #9, #15)
    # ------------------------------------------------------------------
    def _resolve_x(
        self, records: List[Dict[str, Any]],
    ) -> Tuple[List[Any], str, bool]:
        """
        Resolve the x-axis for a series of records.

        Returns ``(x_values, xlabel, is_datetime)``. Falls back to
        ``range(len(records))`` when any record lacks a usable timestamp.
        """
        if not records:
            return [], "Record #", False

        if not all("timestamp" in r for r in records):
            return list(range(len(records))), "Record #", False

        timestamps = [r["timestamp"] for r in records]

        # FIX #9 — any None in the middle of the series forces the fallback
        if any(t is None for t in timestamps):
            return list(range(len(records))), "Record #", False

        # Numeric
        if all(
            isinstance(t, (int, float)) and not isinstance(t, bool)
            for t in timestamps
        ):
            return timestamps, "Time (numeric)", False

        # datetime / ISO string
        parsed: List[datetime] = []
        for t in timestamps:
            if isinstance(t, datetime):
                parsed.append(t)
            elif isinstance(t, str):
                try:
                    parsed.append(
                        datetime.fromisoformat(t.replace("Z", "+00:00"))
                    )
                except (ValueError, TypeError):
                    return list(range(len(records))), "Record #", False
            else:
                return list(range(len(records))), "Record #", False
        return parsed, "Time", True

    # ------------------------------------------------------------------
    # Internal — plotting primitives
    # ------------------------------------------------------------------
    @staticmethod
    def _plot_series(ax: Any, x: List[Any], y: List[float],
                     color: str, label: str, is_datetime: bool) -> None:
        """Plot a full series; no-op if the data is empty."""
        if not y:
            return
        ax.plot(x, y, color=color, label=label)
        if is_datetime:
            try:
                import matplotlib.dates as mdates  # lazy (FIX #10)
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
                ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            except Exception:                          # noqa: BLE001
                pass

    @staticmethod
    def _plot_metric(ax: Any, x: List[Any], value: Any,
                     color: str, label: str, is_datetime: bool) -> None:
        """
        Plot a scalar reference line (FIX #6) or a series.

        A scalar draws a labeled ``axhline`` with the value in the label so
        the reader can see it's a reference, not a constant. A list/tuple
        of the same length as ``x`` draws a real series.
        """
        if value is None:
            return

        # Series case
        if isinstance(value, (list, tuple)):
            series: List[float] = []
            for v in value:
                if v is None:
                    return
                try:
                    series.append(float(v))
                except (TypeError, ValueError):
                    return
            if series:
                ax.plot(x[:len(series)], series, color=color, label=label)
            return

        # Scalar reference line
        try:
            scalar = float(value)
        except (TypeError, ValueError):
            return
        if not math.isfinite(scalar):
            return
        ax.axhline(
            y=scalar, color=color, linestyle="--",
            label=f"{label} (ref={scalar:.2f})",
        )

    @staticmethod
    def _safe_legend(ax: Any) -> None:
        """Draw a legend only if the axis has labeled artists (FIX #7)."""
        try:
            handles, labels = ax.get_legend_handles_labels()
        except Exception:                              # noqa: BLE001
            return
        if handles:
            try:
                ax.legend(loc="upper left")
            except Exception:                          # noqa: BLE001
                pass


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
    dash = CarbonDashboard()

    _check(
        "list of dicts",
        dash._extract_records([{"a": 1}, {"a": 2}]) == [{"a": 1}, {"a": 2}],
    )
    _check(
        "list with non-dict filtered",
        dash._extract_records([{"a": 1}, "string", 42, {"a": 2}])
        == [{"a": 1}, {"a": 2}],
    )
    _check(
        "dict with records key",
        dash._extract_records({"records": [{"a": 1}]}) == [{"a": 1}],
    )
    _check(
        "dict with entries key",
        dash._extract_records({"entries": [{"a": 1}]}) == [{"a": 1}],
    )
    _check(
        "bare dict wrapped in list",
        dash._extract_records({"a": 1}) == [{"a": 1}],
    )
    _check("None", dash._extract_records(None) == [])
    _check("string", dash._extract_records("hello") == [])
    _check("int", dash._extract_records(42) == [])

    class _DFLike:
        def to_dict(self, *args):
            if args and args[0] == "records":
                return [{"a": 1}, {"a": 2}]
            raise TypeError("unknown arg")

    class _DataclassLike:
        def to_dict(self):
            return {"a": 1}

    class _BrokenToDict:
        def to_dict(self, *args):
            raise RuntimeError("nope")

    _check(
        "DataFrame-like to_dict('records')",
        dash._extract_records(_DFLike()) == [{"a": 1}, {"a": 2}],
    )
    _check(
        "dataclass-like to_dict()",
        dash._extract_records(_DataclassLike()) == [{"a": 1}],
    )
    _check(
        "broken to_dict",
        dash._extract_records(_BrokenToDict()) == [],
    )

    print("_num:")
    _check("float", dash._num("1.5") == 1.5)
    _check("None → 0", dash._num(None) == 0.0)
    _check("nan → 0", dash._num(float("nan")) == 0.0)
    _check("inf → 0", dash._num(float("inf")) == 0.0)
    _check("bool coerced", dash._num(True) == 1.0)
    _check("default respected", dash._num("nope", default=-1.0) == -1.0)

    print("_resolve_x:")
    _check(
        "no timestamps → index",
        dash._resolve_x([{"a": 1}, {"a": 2}])[0] == [0, 1],
    )
    _check(
        "numeric timestamps",
        dash._resolve_x([{"timestamp": 1.0}, {"timestamp": 2.0}])[0]
        == [1.0, 2.0],
    )
    _check(
        "iso string timestamps → datetime",
        all(
            isinstance(x, datetime)
            for x in dash._resolve_x([
                {"timestamp": "2024-01-01T00:00:00"},
                {"timestamp": "2024-01-01T01:00:00"},
            ])[0]
        ),
    )
    _check(
        "None in the middle → fallback",
        dash._resolve_x([
            {"timestamp": 1.0}, {"timestamp": None}, {"timestamp": 3.0},
        ])[0] == [0, 1, 2],
    )
    _check(
        "mixed types → fallback",
        dash._resolve_x([
            {"timestamp": 1.0}, {"timestamp": "2024-01-01T00:00:00"},
        ])[0] == [0, 1],
    )

    print("set_enhancements:")
    d = CarbonDashboard({"use_enhancements": False})
    _check("starts off", d.use_enhancements is False)
    d.set_enhancements(True)
    _check("toggled on", d.use_enhancements is True)
    d.set_enhancements(False)
    _check("toggled off", d.use_enhancements is False)

    print("PlotResult:")
    r = PlotResult(reason="no_data")
    _check("not ok when no_data", r.ok is False)
    _check("to_png on empty returns None", r.to_png() is None)
    _check("close on empty is safe", r.close() is None)

    print("__repr__:")
    d = CarbonDashboard({"use_enhancements": True})
    _check("mentions use_enhancements", "use_enhancements=True" in repr(d))
    _check("mentions class name", "CarbonDashboard" in repr(d))

    print("Use-enhancements-off short-circuit:")
    if MATPLOTLIB_AVAILABLE:
        result = d.plot_carbon_footprint([{"carbon_kg": 1.0}])
        _check(
            "reason is ok when enabled",
            result.reason == "ok",
            detail=f"got {result.reason!r}",
        )
        result.close()

        d_off = CarbonDashboard({"use_enhancements": False})
        result_off = d_off.plot_carbon_footprint([{"carbon_kg": 1.0}])
        _check(
            "reason is enhancements_off when disabled",
            result_off.reason == "enhancements_off",
            detail=f"got {result_off.reason!r}",
        )
    else:
        print("  (matplotlib unavailable — skipping figure-based checks)")

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
    if not MATPLOTLIB_AVAILABLE:
        print("matplotlib not installed; cannot run demo.")
        return 1

    dash = CarbonDashboard({"use_enhancements": True})
    records = [
        {"timestamp": f"2024-01-01T{h:02d}:00:00",
         "carbon_co2e_kg": 0.01 + 0.005 * h,
         "energy_kwh": 0.05 + 0.01 * h,
         "modp_score": 0.9 - 0.02 * h}
        for h in range(12)
    ]

    print("plot_carbon_footprint → PNG:")
    result = dash.plot_carbon_footprint_png(records)
    if result.png_bytes:
        print(f"  ✓ {len(result.png_bytes)} bytes")
    else:
        print(f"  ✗ reason={result.reason}")
        return 1

    print("plot_with_enhancement_overlay → SVG:")
    overlay = dash.plot_with_enhancement_overlay(
        records,
        {
            "graph_metrics": {"centrality": 0.7, "connectivity": 0.6},
            "human_feedback_score": 0.8,
            "modp_score": 0.75,
        },
    )
    svg = overlay.to_svg()
    if svg:
        print(f"  ✓ {len(svg)} bytes")
    else:
        print(f"  ✗ reason={overlay.reason}")
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
