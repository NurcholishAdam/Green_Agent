#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Energy Visualizer (Enhanced v2)
===============================

Energy consumption visualization with optional overlays for the advanced
enhancement metrics (LIMIT Graph, MODP, RLHF, Distillation, Evolutionary,
MoE).

---- Critical fixes vs v1 ---------------------------------------------------
  FIX #1   Every figure is closed via ``PlotResult.to_png()`` /
           ``to_svg()`` / ``close()``. v1 leaked a matplotlib figure per
           call — a dashboard server crashes at ~200 calls.
  FIX #2   ``ENHANCEMENTS_AVAILABLE`` is informational only. Plotting
           numbers does not require the enhancement schemas; when the flag
           is set but the schemas are missing, a warning is logged and
           plotting proceeds.
  FIX #3   ``plot_with_enhancement_overlay`` now respects
           ``self.use_enhancements`` (v1 drew an "advanced" plot regardless
           of the flag).
  FIX #4   ``matplotlib.use("Agg")`` selected before importing ``pyplot``;
           headless default, no more ``non-GUI backend`` warnings, no
           accidental GUI windows on ``DISPLAY`` hosts.
  FIX #5   ``axhline`` replaced by ``_plot_metric``. Scalars are drawn as
           labeled reference lines (``Centrality (ref=0.50)``); list/tuple
           values are drawn as real series.
  FIX #6   Legends are only drawn when ``get_legend_handles_labels()``
           returns a non-empty list — no more warnings from empty subplots.
  FIX #7   Bare ``except:`` in ``_extract_records`` narrowed to specific
           exception types.
  FIX #8   ``enhancement_metrics`` is validated to be a dict before use.
  FIX #9   Timestamp resolution requires **every** record to have a usable
           timestamp; a ``None`` in the middle no longer silently falls
           back to the index for the whole series.
  FIX #10  ``plot_with_enhancement_overlay`` uses the same timestamp
           resolution as ``plot_energy_consumption``.
  FIX #11  Removed unused ``matplotlib.dates`` import; loaded lazily only
           when the x-axis is datetime.
  FIX #12  Removed unused ``os``, ``Tuple``, and enhancement schema imports.
  FIX #13  ``plot_energy_consumption`` and ``plot_with_enhancement_overlay``
           return a ``PlotResult`` with a ``reason`` field
           (``ok`` / ``enhancements_off`` / ``no_matplotlib`` /
           ``no_data``) instead of ``None``.
  FIX #14  Figure closed on any construction error — no leak on the
           exception path.
  FIX #15  ``constrained_layout=True`` replaces ``tight_layout()`` — the
           suptitle no longer overlaps the first subplot.
  FIX #16  ``set_xlabel`` selected from the actual x-axis
           (``Time`` / ``Time (numeric)`` / ``Record #``).
  FIX #17  Accepts ``int`` / ``float``, ``datetime``, and ISO-8601 string
           timestamps.
  FIX #18  ``to_png()`` / ``to_svg()`` on ``PlotResult``; convenience
           methods ``plot_energy_consumption_png`` /
           ``plot_with_enhancement_overlay_png``.
  FIX #19  Metric values validated to be finite; out-of-range reference
           values are labeled with the actual value.
  FIX #20  ``set_enhancements(bool)`` for post-construction toggle.
  FIX #21  ``threading.Lock`` around figure creation; matplotlib's global
           state is not thread-safe.
  FIX #22  ``_extract_records`` filters non-dict list items; wraps a bare
           dict in ``[data]``; handles both ``to_dict('records')`` and
           ``to_dict()``.
  FIX #23  Empty subplot gets a "no data" annotation instead of a
           warning-emitting legend call.
  FIX #24  Per-call logging downgraded to ``debug``; one INFO at
           construction.
  FIX #25  ``__repr__`` reports the config-relevant state.
  FIX #26  ``__main__`` self-test validates the pure helpers without
           matplotlib.

---- Run --------------------------------------------------------------------
  python energy_visualizer.py         # self-test
  python energy_visualizer.py --demo  # end-to-end smoke with sample data
"""

from __future__ import annotations

import logging
import math
import threading
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Platform hygiene (FIX #4)
# ---------------------------------------------------------------------------
MATPLOTLIB_AVAILABLE = False
plt: Any = None
try:
    import matplotlib
    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt  # noqa: E402  (after use())
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    plt = None


# FIX #2 — informational only; plotting does not require these
ENHANCEMENTS_AVAILABLE = False
try:
    from enhancements.schemas.feedback_event import FeedbackEvent  # noqa: F401
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    pass


# FIX #21 — matplotlib global state is not thread-safe
_MPL_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Result type (FIX #1, #13, #18)
# ---------------------------------------------------------------------------
@dataclass
class PlotResult:
    """
    Result of a plotting call.

    ``reason`` distinguishes the failure modes:

    * ``ok``                — figure or bytes were produced
    * ``enhancements_off``  — caller asked for the enhanced plot with
                              ``use_enhancements=False``
    * ``no_matplotlib``     — matplotlib not installed
    * ``no_data``           — input contained no usable records

    The caller is responsible for closing the figure via ``close()``,
    ``to_png()``, or ``to_svg()``. The ``plot_*_png`` helpers do this
    automatically.
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
# Visualizer
# ---------------------------------------------------------------------------
class EnergyVisualizer:
    """
    Energy consumption visualizer.

    Parameters
    ----------
    config : dict, optional
        Recognized keys:

        * ``use_enhancements``     (bool) — show the enhanced plot
        * ``graph_metrics``        (dict) — centrality/connectivity reference
        * ``modp_weights``         (list) — informational
        * ``human_feedback_score`` (float) — reference value
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config: Dict[str, Any] = config or {}
        # FIX #2 — honor the flag; ENHANCEMENTS_AVAILABLE is informational
        self.use_enhancements: bool = bool(
            self.config.get("use_enhancements", False)
        )
        if self.use_enhancements and not ENHANCEMENTS_AVAILABLE:
            logger.warning(
                "EnergyVisualizer: use_enhancements=True but the enhancement "
                "schemas are not importable. Overlays will still be drawn — "
                "plotting does not require the schemas — but install them if "
                "any other component depends on them."
            )
        logger.info(
            "Initialized EnergyVisualizer (enhancements=%s, matplotlib=%s)",
            "on" if self.use_enhancements else "off",
            "available" if MATPLOTLIB_AVAILABLE else "missing",
        )

    def __repr__(self) -> str:
        return (
            f"EnergyVisualizer("
            f"use_enhancements={self.use_enhancements}, "
            f"matplotlib={MATPLOTLIB_AVAILABLE}, "
            f"enhancements_available={ENHANCEMENTS_AVAILABLE})"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def plot_energy_consumption(
        self,
        data: Any,
        *,
        close: bool = False,
    ) -> PlotResult:
        """
        Plot energy, carbon, and MODP score as three stacked subplots.

        Returns a ``PlotResult``. When ``close=True`` the figure is
        exported to PNG bytes and closed immediately.
        """
        if not MATPLOTLIB_AVAILABLE:
            return PlotResult(reason="no_matplotlib")
        if not self.use_enhancements:
            return PlotResult(reason="enhancements_off")

        records = self._extract_records(data)
        if not records:
            return PlotResult(reason="no_data")

        x, xlabel, is_datetime = self._resolve_x(records)
        energy = [self._num(r.get("energy_kwh", 0)) for r in records]
        carbon = [self._num(r.get("carbon_co2e_kg", 0)) for r in records]
        modp = [self._num(r.get("modp_score", 0)) for r in records]

        fig = None
        try:
            with _MPL_LOCK:
                fig, axes = plt.subplots(
                    3, 1, figsize=(10, 8), sharex=True,
                    constrained_layout=True,        # FIX #15
                )
            fig.suptitle("Enhanced Energy Consumption Visualization")

            self._plot_series(axes[0], x, energy, "tab:red",
                              "Energy (kWh)", is_datetime)
            axes[0].set_ylabel("Energy (kWh)")

            self._plot_series(axes[1], x, carbon, "tab:green",
                              "Carbon (kg CO2e)", is_datetime)
            axes[1].set_ylabel("Carbon (kg CO2e)")

            self._plot_series(axes[2], x, modp, "tab:blue",
                              "MODP Score", is_datetime)
            axes[2].set_ylabel("MODP Score")

            axes[-1].set_xlabel(xlabel)             # FIX #16

            for ax in axes:
                self._safe_legend(ax)               # FIX #6
        except Exception:
            if fig is not None and plt is not None:
                plt.close(fig)                      # FIX #14
            raise

        result = PlotResult(figure=fig, reason="ok")
        if close:
            result.to_png()
        return result

    def plot_energy_consumption_png(
        self, data: Any, *, dpi: int = 100,
    ) -> PlotResult:
        """Convenience: plot and return PNG bytes, closing the figure."""
        return self.plot_energy_consumption(data, close=True)

    def plot_with_enhancement_overlay(
        self,
        data: Any,
        enhancement_metrics: Dict[str, Any],
        *,
        close: bool = False,
    ) -> PlotResult:
        """
        Overlay aggregate enhancement metrics on top of the energy series.

        Scalar values are drawn as labeled reference lines
        (``Centrality (ref=0.50)``); list/tuple values are drawn as real
        series (``FIX #5``).

        Requires ``self.use_enhancements`` (``FIX #3``).
        """
        if not self.use_enhancements:
            return PlotResult(reason="enhancements_off")
        if not MATPLOTLIB_AVAILABLE:
            return PlotResult(reason="no_matplotlib")
        if not isinstance(enhancement_metrics, dict):  # FIX #8
            logger.debug(
                "enhancement_metrics is %s, not a dict; skipping overlay",
                type(enhancement_metrics).__name__,
            )
            return PlotResult(reason="no_data")

        records = self._extract_records(data)
        if not records:
            return PlotResult(reason="no_data")

        # FIX #10 — same timestamp resolution as the main plot
        x, xlabel, is_datetime = self._resolve_x(records)
        energy = [self._num(r.get("energy_kwh", 0)) for r in records]

        fig = None
        try:
            with _MPL_LOCK:
                fig, axes = plt.subplots(
                    3, 1, figsize=(10, 8), sharex=True,
                    constrained_layout=True,
                )

            self._plot_series(axes[0], x, energy, "tab:red",
                              "Energy", is_datetime)
            axes[0].set_ylabel("Energy (kWh)")

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

    def plot_with_enhancement_overlay_png(
        self, data: Any, enhancement_metrics: Dict[str, Any],
        *, dpi: int = 100,
    ) -> PlotResult:
        """Convenience: overlay plot as PNG bytes."""
        return self.plot_with_enhancement_overlay(
            data, enhancement_metrics, close=True,
        )

    # ------------------------------------------------------------------
    # Post-construction toggle (FIX #20)
    # ------------------------------------------------------------------
    def set_enhancements(self, enabled: bool) -> None:
        """Enable or disable the enhanced overlays at runtime."""
        self.use_enhancements = bool(enabled)
        logger.info("Enhancements %s", "enabled" if enabled else "disabled")

    # ------------------------------------------------------------------
    # Internal — record extraction (FIX #7, #22)
    # ------------------------------------------------------------------
    def _extract_records(self, data: Any) -> List[Dict[str, Any]]:
        """
        Extract a list of dict records from the shapes a caller may pass.

        * ``list`` of dicts        → filtered to dicts only
        * ``list`` with mixed      → non-dict items dropped
        * ``dict`` with ``records``/``entries`` → that list (filtered)
        * bare ``dict``            → ``[data]``
        * object with ``to_dict('records')`` → DataFrame path
        * object with ``to_dict()``          → dataclass path
        * anything else            → ``[]``
        """
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

        if isinstance(data, dict):
            for key in ("records", "entries"):
                value = data.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
            return [data]

        to_dict = getattr(data, "to_dict", None)
        if callable(to_dict):
            try:
                result = to_dict("records")
                if isinstance(result, list):
                    return [item for item in result if isinstance(item, dict)]
            except (TypeError, ValueError, AttributeError):  # FIX #7
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

    @staticmethod
    def _num(value: Any, default: float = 0.0) -> float:
        """Coerce to finite float or return ``default``."""
        try:
            f = float(value)
        except (TypeError, ValueError):
            return default
        return f if math.isfinite(f) else default

    # ------------------------------------------------------------------
    # Internal — x-axis resolution (FIX #9, #17)
    # ------------------------------------------------------------------
    @staticmethod
    def _resolve_x(
        records: List[Dict[str, Any]],
    ) -> Tuple[List[Any], str, bool]:
        """
        Resolve the x-axis for a series of records.

        Returns ``(x_values, xlabel, is_datetime)``. Falls back to
        ``range(len(records))`` when **any** record lacks a usable
        timestamp.
        """
        if not records:
            return [], "Record #", False

        if not all("timestamp" in r for r in records):
            return list(range(len(records))), "Record #", False

        timestamps = [r["timestamp"] for r in records]

        # Any None forces the fallback (v1 only checked the first)
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
    # Internal — plotting primitives (FIX #5, #6, #19)
    # ------------------------------------------------------------------
    @staticmethod
    def _plot_series(ax: Any, x: List[Any], y: List[float],
                     color: str, label: str, is_datetime: bool) -> None:
        """Plot a full series; no-op when the data is empty."""
        if not y:
            return
        ax.plot(x, y, color=color, label=label)
        if is_datetime:
            try:
                import matplotlib.dates as mdates  # lazy (FIX #11)
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
                ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            except Exception:                        # noqa: BLE001
                pass

    @staticmethod
    def _plot_metric(ax: Any, x: List[Any], value: Any,
                     color: str, label: str, is_datetime: bool) -> None:
        """
        Plot a scalar reference line (FIX #5) or a series.

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
        """Draw a legend only if the axis has labeled artists (FIX #6)."""
        try:
            handles, labels = ax.get_legend_handles_labels()
        except Exception:                            # noqa: BLE001
            return
        if handles:
            try:
                ax.legend(loc="upper left")
            except Exception:                        # noqa: BLE001
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
    ev = EnergyVisualizer()

    _check(
        "list of dicts",
        ev._extract_records([{"a": 1}, {"a": 2}]) == [{"a": 1}, {"a": 2}],
    )
    _check(
        "list with non-dict filtered",
        ev._extract_records([{"a": 1}, "x", 42, {"a": 2}])
        == [{"a": 1}, {"a": 2}],
    )
    _check(
        "dict with records key",
        ev._extract_records({"records": [{"a": 1}]}) == [{"a": 1}],
    )
    _check(
        "dict with entries key",
        ev._extract_records({"entries": [{"a": 1}]}) == [{"a": 1}],
    )
    _check(
        "bare dict wrapped in list",
        ev._extract_records({"a": 1}) == [{"a": 1}],
    )
    _check("None", ev._extract_records(None) == [])
    _check("string", ev._extract_records("x") == [])

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
        ev._extract_records(_DFLike()) == [{"a": 1}, {"a": 2}],
    )
    _check(
        "dataclass-like",
        ev._extract_records(_DataclassLike()) == [{"a": 1}],
    )

    print("_num:")
    _check("float", ev._num("1.5") == 1.5)
    _check("None → 0", ev._num(None) == 0.0)
    _check("nan → 0", ev._num(float("nan")) == 0.0)
    _check("inf → 0", ev._num(float("inf")) == 0.0)
    _check("bool coerced", ev._num(True) == 1.0)
    _check("default respected", ev._num("nope", default=-1.0) == -1.0)

    print("_resolve_x:")
    _check(
        "no timestamps → index",
        ev._resolve_x([{"a": 1}, {"a": 2}])[0] == [0, 1],
    )
    _check(
        "numeric timestamps",
        ev._resolve_x([{"timestamp": 1.0}, {"timestamp": 2.0}])[0]
        == [1.0, 2.0],
    )
    _check(
        "iso string timestamps → datetime",
        all(
            isinstance(x, datetime)
            for x in ev._resolve_x([
                {"timestamp": "2024-01-01T00:00:00"},
                {"timestamp": "2024-01-01T01:00:00"},
            ])[0]
        ),
    )
    _check(
        "None in the middle → fallback",
        ev._resolve_x([
            {"timestamp": 1.0}, {"timestamp": None}, {"timestamp": 3.0},
        ])[0] == [0, 1, 2],
    )
    _check(
        "mixed types → fallback",
        ev._resolve_x([
            {"timestamp": 1.0}, {"timestamp": "2024-01-01T00:00:00"},
        ])[0] == [0, 1],
    )

    print("set_enhancements:")
    d = EnergyVisualizer({"use_enhancements": False})
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
    _check(
        "mentions use_enhancements",
        "use_enhancements=True" in repr(
            EnergyVisualizer({"use_enhancements": True})
        ),
    )
    _check(
        "mentions class name",
        "EnergyVisualizer" in repr(EnergyVisualizer()),
    )

    print("Enhancements-off short-circuit:")
    off = EnergyVisualizer({"use_enhancements": False})
    result = off.plot_energy_consumption([{"energy_kwh": 1.0}])
    _check(
        "plot_energy_consumption → enhancements_off",
        result.reason == "enhancements_off",
        detail=f"got {result.reason!r}",
    )
    result = off.plot_with_enhancement_overlay(
        [{"energy_kwh": 1.0}],
        {"modp_score": 0.9},
    )
    _check(
        "overlay → enhancements_off",
        result.reason == "enhancements_off",
        detail=f"got {result.reason!r}",
    )

    print("overlay rejects non-dict enhancement_metrics:")
    on = EnergyVisualizer({"use_enhancements": True})
    if MATPLOTLIB_AVAILABLE:
        for bad in (None, [1, 2, 3], "nope", 42):
            r = on.plot_with_enhancement_overlay(
                [{"energy_kwh": 1.0}, {"energy_kwh": 2.0}],
                bad,  # type: ignore[arg-type]
            )
            _check(
                f"non-dict {type(bad).__name__} → no_data",
                r.reason == "no_data",
                detail=f"got {r.reason!r}",
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

    ev = EnergyVisualizer({"use_enhancements": True})
    records = [
        {"timestamp": f"2024-01-01T{h:02d}:00:00",
         "energy_kwh": 0.05 + 0.01 * h,
         "carbon_co2e_kg": 0.01 + 0.005 * h,
         "modp_score": 0.9 - 0.02 * h}
        for h in range(12)
    ]

    print("plot_energy_consumption → PNG:")
    r = ev.plot_energy_consumption_png(records)
    if r.png_bytes:
        print(f"  ✓ {len(r.png_bytes)} bytes")
    else:
        print(f"  ✗ reason={r.reason}")
        return 1

    print("plot_with_enhancement_overlay → PNG:")
    r = ev.plot_with_enhancement_overlay_png(
        records,
        {
            "graph_metrics": {"centrality": 0.7, "connectivity": 0.6},
            "human_feedback_score": 0.8,
            "modp_score": 0.75,
        },
    )
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
