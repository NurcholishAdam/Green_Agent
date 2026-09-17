# src/dashboard/app.py

"""
Streamlit Dashboard for Green Agent (entry point).
=================================================

Run with::

    streamlit run src/dashboard/app.py

The dashboard renders:

1. A metrics overview (``Metrics Overview``).
2. A Pareto-style scatter chart (``Pareto Visualization``).
3. A reflection summary (``Reflection Summary``).

Enhancements
------------
- **Defensive ``pareto_visualizer`` import** — the module does not exist in
  the current package, so the app previously crashed at import time. The
  enhanced version falls back to a plotly-based scatter (or to
  ``st.scatter_chart`` when plotly is unavailable).
- **Package-relative imports with fallbacks** — the app works both under
  ``streamlit run`` and when imported as ``dashboard.app``.
- ``DashboardAppConfig`` — frozen, validated: page layout, uploader label,
  chart axes, color column.
- ``main(config=None)`` entry point for programmatic / ``streamlit.testing``
  use.
- **Full error handling** — malformed uploads, missing columns, and Plotly
  failures are surfaced as ``st.error`` / ``st.info`` instead of crashing.
- **Column validation** — the scatter only renders when the configured
  axes exist in the dataframe.
- ``if __name__ == "__main__"`` guard — a plain ``python -m dashboard.app``
  prints a helpful hint instead of silently doing nothing.
- ``logger`` diagnostics, custom ``DashboardAppError(ValueError)``, and
  ``to_dict`` / ``from_dict`` on the config.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import streamlit as st

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class DashboardAppError(ValueError):
    """Raised for invalid dashboard-app inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DashboardAppConfig:
    """Tunable parameters for the Streamlit dashboard entry point."""

    page_title: str = "🌱 Green Agent Dashboard"
    page_icon: str = "🌱"
    layout: str = "wide"

    uploader_label: str = "Upload green_agent_report.json"
    uploader_types: tuple = ("json",)

    # Pareto chart axes (must match the flattened dataframe column names).
    pareto_x: str = "energy_kwh"
    pareto_y: str = "latency"
    pareto_color: Optional[str] = "accuracy"

    # Section headings.
    metrics_heading: str = "📊 Metrics Overview"
    pareto_heading: str = "📈 Pareto Visualization"
    reflection_heading: str = "🧠 Reflection Summary"

    def __post_init__(self) -> None:
        if self.layout not in ("wide", "centered"):
            raise DashboardAppError("layout must be 'wide' or 'centered'.")
        for name in (
            "page_title", "uploader_label",
            "pareto_x", "pareto_y",
            "metrics_heading", "pareto_heading", "reflection_heading",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise DashboardAppError(f"{name} must be a non-empty string.")
        if not isinstance(self.uploader_types, tuple):
            raise DashboardAppError("uploader_types must be a tuple.")
        if self.pareto_color is not None and not isinstance(self.pareto_color, str):
            raise DashboardAppError(
                "pareto_color must be a string or None."
            )

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["uploader_types"] = list(self.uploader_types)
        return d

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DashboardAppConfig":
        if not isinstance(data, Mapping):
            raise DashboardAppError("DashboardAppConfig.from_dict expects a Mapping.")
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k == "uploader_types":
                kwargs[k] = tuple(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Defensive imports
# --------------------------------------------------------------------------- #
def _load_telemetry_loader() -> Optional[Any]:
    """Import ``TelemetryLoader`` (package-relative, then flat)."""
    try:
        from .telemetry_loader import TelemetryLoader  # type: ignore
        return TelemetryLoader
    except ImportError:
        pass
    try:
        from telemetry_loader import TelemetryLoader  # type: ignore
        return TelemetryLoader
    except ImportError:
        logger.warning("TelemetryLoader is unavailable.")
        return None


def _load_pareto_visualizer() -> Optional[Any]:
    """
    Import ``ParetoVisualizer`` if present.

    The module is **not** part of the current dashboard package; the app
    degrades to a plotly-based scatter, or to ``st.scatter_chart`` when
    plotly is unavailable. The defensive wrapper means a missing module no
    longer crashes the app at import time.
    """
    try:
        from .pareto_visualizer import ParetoVisualizer  # type: ignore
        return ParetoVisualizer
    except ImportError:
        pass
    try:
        from pareto_visualizer import ParetoVisualizer  # type: ignore
        return ParetoVisualizer
    except ImportError:
        logger.debug("pareto_visualizer not found; using in-line chart.")
        return None


def _load_plotly() -> Optional[Any]:
    """Return ``plotly.express`` if available, else ``None``."""
    try:
        import plotly.express as px  # type: ignore
        return px
    except ImportError:
        logger.debug("plotly not available; falling back to st.scatter_chart.")
        return None


# --------------------------------------------------------------------------- #
# Rendering helpers
# --------------------------------------------------------------------------- #
def _scatter_fallback(df: Any, cfg: DashboardAppConfig) -> None:
    """Render ``st.scatter_chart`` when no figure object is available."""
    missing = [c for c in (cfg.pareto_x, cfg.pareto_y) if c not in df.columns]
    if missing:
        st.info(
            f"Cannot render the Pareto chart: missing column(s) {missing}. "
            f"Expected '{cfg.pareto_x}' and '{cfg.pareto_y}'."
        )
        return
    try:
        st.scatter_chart(df, x=cfg.pareto_x, y=cfg.pareto_y)
    except Exception as exc:  # pragma: no cover — Streamlit internals
        logger.exception("st.scatter_chart failed: %s", exc)
        st.error(f"Could not render the scatter chart: {exc}")


def _build_pareto_chart(
    df: Any,
    cfg: DashboardAppConfig,
    ParetoVisualizer: Optional[Any],
) -> Optional[Any]:
    """
    Return a plotly figure for ``st.plotly_chart`` or ``None`` to fall back.

    Order of preference:

    1. ``ParetoVisualizer.plot(df)`` if the module is available.
    2. A direct ``plotly.express.scatter`` on the configured axes.
    3. ``None`` — the caller falls back to ``st.scatter_chart``.
    """
    if ParetoVisualizer is not None:
        try:
            return ParetoVisualizer.plot(df)
        except Exception as exc:
            logger.warning("ParetoVisualizer.plot failed: %s", exc)

    px = _load_plotly()
    if px is None:
        return None

    missing = [c for c in (cfg.pareto_x, cfg.pareto_y) if c not in df.columns]
    if missing:
        return None

    color = cfg.pareto_color if (
        cfg.pareto_color and cfg.pareto_color in df.columns
    ) else None
    try:
        return px.scatter(df, x=cfg.pareto_x, y=cfg.pareto_y, color=color)
    except Exception as exc:  # pragma: no cover — plotly edge cases
        logger.warning("plotly.express.scatter failed: %s", exc)
        return None


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def main(config: Optional[DashboardAppConfig] = None) -> None:
    """Render the Streamlit dashboard."""
    cfg = config or DashboardAppConfig()

    # ``st.set_page_config`` must be the first Streamlit call.
    st.set_page_config(
        page_title=cfg.page_title,
        page_icon=cfg.page_icon,
        layout=cfg.layout,
    )
    st.title(cfg.page_title)

    # ---- Dependency resolution -----------------------------------
    TelemetryLoader = _load_telemetry_loader()
    if TelemetryLoader is None:
        st.error(
            "TelemetryLoader is not available. Verify the dashboard "
            "package is installed and importable."
        )
        return

    uploaded = st.file_uploader(
        cfg.uploader_label, type=list(cfg.uploader_types),
    )
    if uploaded is None:
        return

    # ---- Load the report ------------------------------------------
    try:
        report = TelemetryLoader.load(uploaded)
    except Exception as exc:
        logger.exception("TelemetryLoader.load failed: %s", exc)
        st.error(f"Could not load the uploaded report: {exc}")
        return

    # ---- Convert to a dataframe -----------------------------------
    try:
        df = TelemetryLoader.to_dataframe(report)
    except Exception as exc:
        logger.exception("TelemetryLoader.to_dataframe failed: %s", exc)
        st.error(f"Could not convert the report to a dataframe: {exc}")
        return

    # ---- Metrics overview ----------------------------------------
    st.subheader(cfg.metrics_heading)
    st.dataframe(df)

    # ---- Pareto visualization ------------------------------------
    st.subheader(cfg.pareto_heading)
    ParetoVisualizer = _load_pareto_visualizer()
    fig = _build_pareto_chart(df, cfg, ParetoVisualizer)
    if fig is not None:
        try:
            # ``use_container_width`` is the historical kwarg; newer
            # Streamlit versions accept ``width="stretch"``.
            st.plotly_chart(fig, use_container_width=True)
        except Exception as exc:
            logger.warning(
                "st.plotly_chart failed: %s; using scatter_chart.", exc,
            )
            _scatter_fallback(df, cfg)
    else:
        _scatter_fallback(df, cfg)

    # ---- Reflection summary --------------------------------------
    st.subheader(cfg.reflection_heading)
    if isinstance(report, Mapping):
        reflection = report.get("reflection")
    else:  # pragma: no cover — defensive
        reflection = None
    if reflection is None:
        st.write("No reflection provided in this report.")
    else:
        st.write(reflection)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "DashboardAppConfig",
    "DashboardAppError",
    "main",
]


# --------------------------------------------------------------------------- #
# Streamlit / smoke-test entry
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    in_streamlit = False
    try:
        # Streamlit's script runner sets a context when the file is executed
        # by ``streamlit run``. When the module is executed as a plain script
        # (``python -m dashboard.app``), the context is missing and we print
        # a hint instead of spinning up a no-op UI.
        from streamlit.runtime.scriptrunner import get_script_run_ctx  # type: ignore

        in_streamlit = get_script_run_ctx() is not None
    except ImportError:
        in_streamlit = False

    if in_streamlit:
        main()
    else:
        print(
            f"Run with: streamlit run {Path(__file__).resolve()}",
            file=sys.stderr,
        )
        sys.exit(0)
