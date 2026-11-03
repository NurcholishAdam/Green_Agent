# src/dashboard/app.py

"""
Streamlit Dashboard for Green Agent (entry point).
=================================================

Run with::

    streamlit run src/dashboard/app.py

The dashboard renders:

1. A metrics overview (``Metrics Overview``) with summary cards when
   numeric columns are present.
2. A Pareto-style scatter chart (``Pareto Visualization``).
3. A reflection summary (``Reflection Summary``).

Enhancements
------------
- **Defensive ``pareto_visualizer`` import** — the module does not exist
  in the current package, so the app previously crashed at import time.
  The enhanced version falls back to a plotly-based scatter (or to
  ``st.scatter_chart`` when plotly is unavailable). The loader also
  catches ``SyntaxError``, ``RuntimeError``, and other unexpected
  failures inside a broken-but-present module — not only
  ``ModuleNotFoundError``.
- **Package-relative imports with fallbacks** — the app works both under
  ``streamlit run`` and when imported as ``dashboard.app``.
- ``DashboardAppConfig`` — frozen, ``__repr__``, fully validated:
  page layout, page icon, uploader label, uploader types (non-empty
  strings), chart axes, color column, section headings.
  ``from_dict(..., strict=True)`` rejects unknown keys.
- **Lazy ``streamlit`` import** — the config class, the ``_load_*``
  helpers, and ``health()`` are usable in a Python process that does
  not have Streamlit installed. Only ``main()`` and the rendering
  helpers import it.
- ``main(config=None) -> int`` entry point. Returns ``0`` on any
  successful render, ``1`` on any error path. Usable from
  ``streamlit.testing.v1.AppTest`` and from CI harnesses.
- **Full error handling** — malformed uploads, missing columns, broken
  ``ParetoVisualizer.plot``, and Plotly failures are surfaced as
  ``st.error`` / ``st.info`` / ``st.warning`` instead of crashing.
- **Column validation** — the scatter only renders when the configured
  axes exist in the dataframe.
- **Summary cards** — accuracy / energy / carbon metric cards rendered
  when those columns are present (parity with
  :mod:`dashboard.streamlit_dashboard`).
- **Empty-reflection semantics documented** — an empty string renders
  as the "No reflection provided" message, matching a missing value.
- ``if __name__ == "__main__"`` guard — a plain ``python -m dashboard.app``
  prints a helpful hint instead of silently doing nothing.
- ``logger`` diagnostics, custom ``DashboardAppError(ValueError)``,
  ``to_dict`` / ``from_dict`` on the config, ``health()``, and
  ``__version__``.

Compatibility
-------------
- ``main(config=None)`` — **behaviour change**: returns ``int`` instead
  of ``None``. Callers that used ``main() is None`` must switch to
  ``main() == 0``.
- ``DashboardAppConfig`` — same fields. Behaviour changes:
  * ``page_icon`` is now validated as a non-empty string (it was
    previously skipped by the validation loop).
  * ``uploader_types`` must be a non-empty tuple of non-empty strings.
    The default grows to ``("json", "jsonl", "ndjson")`` to match the
    JSONL streaming format used by the telemetry loader.
  * ``pareto_color=""`` is now rejected at construction. Pass
    ``None`` to disable coloring.
- ``DashboardAppConfig.from_dict(..., strict=True)`` — the default
  flipped from "drop unknown keys" to "reject unknown keys". Pass
  ``strict=False`` to preserve the previous lenient behaviour.
- ``__main__`` — the Streamlit-runtime detection now uses the
  documented :func:`streamlit.runtime.exists` public API. Environments
  where Streamlit exposes only the internal
  ``get_script_run_ctx`` are handled by the same broad exception
  handler; the plain-script fallback still prints the hint.

Requires Python 3.8+.
"""

from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class DashboardAppError(ValueError):
    """Raised for invalid dashboard-app inputs or configuration."""


# --------------------------------------------------------------------------- #
# Lazy streamlit import
# --------------------------------------------------------------------------- #
def _import_streamlit() -> Any:
    """Import :mod:`streamlit` or raise a clear :class:`DashboardAppError`."""
    try:
        import streamlit as st  # type: ignore
    except ImportError as exc:
        raise DashboardAppError(
            "streamlit is required to render the dashboard; install it "
            "with `pip install streamlit`."
        ) from exc
    return st


# --------------------------------------------------------------------------- #
# Config validators
# --------------------------------------------------------------------------- #
def _require_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise DashboardAppError(
            f"{name} must be a non-empty string, got "
            f"{type(value).__name__}."
        )
    return value


def _coerce_config(config: Any) -> "DashboardAppConfig":
    if config is None:
        return DashboardAppConfig()
    if not isinstance(config, DashboardAppConfig):
        raise DashboardAppError(
            f"config must be a DashboardAppConfig or None, got "
            f"{type(config).__name__}."
        )
    return config


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
    uploader_types: Tuple[str, ...] = ("json", "jsonl", "ndjson")

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
            raise DashboardAppError(
                "layout must be 'wide' or 'centered'."
            )
        for name in (
            "page_title", "page_icon", "uploader_label",
            "pareto_x", "pareto_y",
            "metrics_heading", "pareto_heading", "reflection_heading",
        ):
            _require_nonempty_str(name, getattr(self, name))

        if not isinstance(self.uploader_types, (tuple, list)) or (
            not self.uploader_types
        ):
            raise DashboardAppError(
                "uploader_types must be a non-empty tuple of strings."
            )
        for i, t in enumerate(self.uploader_types):
            _require_nonempty_str(f"uploader_types[{i}]", t)
        # Freeze to a tuple regardless of input sequence type.
        if not isinstance(self.uploader_types, tuple):
            object.__setattr__(
                self, "uploader_types", tuple(self.uploader_types),
            )

        if self.pareto_color is not None:
            if (
                not isinstance(self.pareto_color, str)
                or not self.pareto_color
            ):
                raise DashboardAppError(
                    "pareto_color must be None or a non-empty string."
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "page_title": self.page_title,
            "page_icon": self.page_icon,
            "layout": self.layout,
            "uploader_label": self.uploader_label,
            "uploader_types": list(self.uploader_types),
            "pareto_x": self.pareto_x,
            "pareto_y": self.pareto_y,
            "pareto_color": self.pareto_color,
            "metrics_heading": self.metrics_heading,
            "pareto_heading": self.pareto_heading,
            "reflection_heading": self.reflection_heading,
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "DashboardAppConfig":
        if not isinstance(data, Mapping):
            raise DashboardAppError(
                "DashboardAppConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise DashboardAppError(
                    f"unknown DashboardAppConfig keys: {unknown}"
                )
        try:
            kwargs: Dict[str, Any] = {}
            for k, v in data.items():
                if k not in valid:
                    continue
                if k == "uploader_types":
                    kwargs[k] = tuple(v or ())
                else:
                    kwargs[k] = v
            return cls(**kwargs)
        except DashboardAppError:
            raise
        except (TypeError, ValueError) as exc:
            raise DashboardAppError(
                f"invalid DashboardAppConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "DashboardAppConfig("
            f"page_title={self.page_title!r}, "
            f"layout={self.layout!r}, "
            f"axes=({self.pareto_x!r}, {self.pareto_y!r}), "
            f"color={self.pareto_color!r})"
        )


# --------------------------------------------------------------------------- #
# Defensive imports
# --------------------------------------------------------------------------- #
def _try_import(module: str, attr: str) -> Optional[Any]:
    """
    Import ``attr`` from ``module`` with fallbacks.

    Distinguishes three failure modes:

    - ``ModuleNotFoundError`` — the module does not exist. Silent
      fallback (the previous behaviour, correct for optional
      dependencies).
    - ``ImportError`` — the module exists but failed to import for a
      reason (missing transitive dependency, circular import, name not
      found). Logged at ``WARNING`` so a broken install is visible.
    - Any other ``Exception`` (``SyntaxError``, ``RuntimeError``, ...)
      — the module is broken. Logged at ``ERROR`` with a traceback.
    """
    try:
        mod = importlib.import_module(module)
    except ModuleNotFoundError:
        return None
    except ImportError as exc:
        logger.warning(
            "could not import %r: %s", module, exc,
        )
        return None
    except Exception:
        logger.exception(
            "unexpected error importing %r", module,
        )
        return None
    return getattr(mod, attr, None)


def _load_telemetry_loader() -> Optional[Any]:
    """Import ``TelemetryLoader`` (package-relative, then flat)."""
    cls = _try_import(".telemetry_loader", "TelemetryLoader")
    if cls is None:
        cls = _try_import("telemetry_loader", "TelemetryLoader")
    if cls is None:
        logger.warning(
            "TelemetryLoader is unavailable; the dashboard cannot load "
            "telemetry reports."
        )
    return cls


def _load_pareto_visualizer() -> Optional[Any]:
    """
    Import ``ParetoVisualizer`` if present.

    The module is **not** part of the current dashboard package; the
    app degrades to a plotly-based scatter, or to ``st.scatter_chart``
    when plotly is unavailable.
    """
    cls = _try_import(".pareto_visualizer", "ParetoVisualizer")
    if cls is None:
        cls = _try_import("pareto_visualizer", "ParetoVisualizer")
    return cls


# Module-level plotly cache.
_PLOTLY_CACHE: Optional[Any] = None
_PLOTLY_PROBED: bool = False


def _load_plotly() -> Optional[Any]:
    """Return ``plotly.express`` if available, else ``None``. Cached."""
    global _PLOTLY_CACHE, _PLOTLY_PROBED
    if _PLOTLY_PROBED:
        return _PLOTLY_CACHE
    _PLOTLY_PROBED = True
    try:
        import plotly.express as px  # type: ignore
        _PLOTLY_CACHE = px
    except ImportError:
        _PLOTLY_CACHE = None
    return _PLOTLY_CACHE


# --------------------------------------------------------------------------- #
# Health
# --------------------------------------------------------------------------- #
def health() -> Dict[str, Any]:
    """Return an aggregate availability snapshot for this app."""
    streamlit_available = True
    try:
        import streamlit  # noqa: F401
    except ImportError:
        streamlit_available = False

    return {
        "version": __version__,
        "streamlit": streamlit_available,
        "telemetry_loader": _load_telemetry_loader() is not None,
        "pareto_visualizer": _load_pareto_visualizer() is not None,
        "plotly": _load_plotly() is not None,
    }


# --------------------------------------------------------------------------- #
# Rendering helpers
# --------------------------------------------------------------------------- #
def _coerce_numeric_series(df: Any, name: str) -> Any:
    """Return ``df[name]`` coerced to numeric."""
    import pandas as pd  # type: ignore
    series = df[name]
    if pd.api.types.is_numeric_dtype(series):
        return series
    return pd.to_numeric(series, errors="coerce")


def _render_summary(df: Any) -> None:
    """Render summary metric cards for accuracy / energy / carbon."""
    st = _import_streamlit()
    import pandas as pd  # type: ignore

    candidates = ("accuracy", "energy_kwh", "carbon_kg")
    available = [c for c in candidates if c in df.columns]
    if not available:
        return
    st.subheader("Summary")
    cols = st.columns(len(available))
    for col, name in zip(cols, available):
        series = _coerce_numeric_series(df, name)
        mean = series.mean(skipna=True)
        if pd.isna(mean):
            col.metric(name, "—")
        else:
            col.metric(name, f"{float(mean):.4g}")


def _build_pareto_chart(
    df: Any,
    cfg: DashboardAppConfig,
    ParetoVisualizer: Optional[Any],
) -> Optional[Any]:
    """
    Return a plotly figure for ``st.plotly_chart`` or ``None`` to fall back.

    Order of preference:

    1. ``ParetoVisualizer.plot(df)`` if the module is available **and**
       exposes a callable ``plot`` attribute.
    2. A direct ``plotly.express.scatter`` on the configured axes.
    3. ``None`` — the caller falls back to ``st.scatter_chart``.
    """
    if ParetoVisualizer is not None:
        plot_fn = getattr(ParetoVisualizer, "plot", None)
        if not callable(plot_fn):
            logger.warning(
                "ParetoVisualizer is importable but has no callable "
                "'plot' attribute; falling back to plotly."
            )
        else:
            try:
                fig = plot_fn(df)
                if fig is not None:
                    return fig
            except Exception as exc:
                logger.warning(
                    "ParetoVisualizer.plot failed: %s; falling back to "
                    "plotly.", exc,
                )

    px = _load_plotly()
    if px is None:
        return None

    missing = [c for c in (cfg.pareto_x, cfg.pareto_y) if c not in df.columns]
    if missing:
        return None

    color = cfg.pareto_color if (
        cfg.pareto_color and cfg.pareto_color in df.columns
    ) else None
    if cfg.pareto_color and color is None:
        logger.warning(
            "pareto_color=%r is not a column in the dataframe; "
            "rendering without a color grouping.",
            cfg.pareto_color,
        )
    try:
        return px.scatter(df, x=cfg.pareto_x, y=cfg.pareto_y, color=color)
    except Exception as exc:  # pragma: no cover — plotly edge cases
        logger.warning("plotly.express.scatter failed: %s", exc)
        return None


def _render_pareto(
    df: Any,
    cfg: DashboardAppConfig,
    ParetoVisualizer: Optional[Any],
) -> None:
    """
    Render the Pareto chart section.

    Precedence: ``ParetoVisualizer`` → plotly.express → st.scatter_chart.
    Missing columns produce a single ``st.info`` message rather than
    being checked twice.
    """
    st = _import_streamlit()

    missing = [c for c in (cfg.pareto_x, cfg.pareto_y) if c not in df.columns]
    if missing:
        st.info(
            f"Cannot render the Pareto chart: missing column(s) "
            f"{missing}. Expected '{cfg.pareto_x}' and '{cfg.pareto_y}'."
        )
        return

    fig = _build_pareto_chart(df, cfg, ParetoVisualizer)
    if fig is not None:
        try:
            st.plotly_chart(fig, use_container_width=True)
            return
        except Exception as exc:
            logger.warning(
                "st.plotly_chart failed: %s; falling back to "
                "st.scatter_chart.", exc,
            )

    try:
        st.scatter_chart(df, x=cfg.pareto_x, y=cfg.pareto_y)
    except Exception as exc:  # pragma: no cover — Streamlit internals
        logger.exception("st.scatter_chart failed: %s", exc)
        st.error(f"Could not render the scatter chart: {exc}")


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def main(config: Optional[DashboardAppConfig] = None) -> int:
    """
    Render the Streamlit dashboard.

    Must be called from within a Streamlit script run (``streamlit
    run`` or ``streamlit.testing.v1.AppTest``).

    Parameters
    ----------
    config : DashboardAppConfig, optional

    Returns
    -------
    int
        ``0`` on success (including the "no upload yet" and "no
        reflection" paths), ``1`` on any error path (unavailable
        ``TelemetryLoader``, malformed upload, read failure, missing
        columns fallback was already handled by ``st.info``).

    Raises
    ------
    DashboardAppError
        If ``config`` is not a :class:`DashboardAppConfig` or ``None``,
        or if Streamlit is not installed. Raised **before** the first
        ``st`` call.
    """
    cfg = _coerce_config(config)
    st = _import_streamlit()

    st.set_page_config(
        page_title=cfg.page_title,
        page_icon=cfg.page_icon,
        layout=cfg.layout,
    )
    st.title(cfg.page_title)

    # ---- Dependency resolution --------------------------------- #
    TelemetryLoader = _load_telemetry_loader()
    if TelemetryLoader is None:
        st.error(
            "TelemetryLoader is not available. Verify the dashboard "
            "package is installed and importable."
        )
        return 1

    # ---- File uploader ----------------------------------------- #
    try:
        uploaded = st.file_uploader(
            cfg.uploader_label, type=list(cfg.uploader_types),
        )
    except Exception as exc:
        logger.exception("st.file_uploader failed: %s", exc)
        st.error(f"Could not render the uploader: {exc}")
        return 1

    if uploaded is None:
        return 0

    # ---- Load the report --------------------------------------- #
    try:
        report = TelemetryLoader.load(uploaded)
    except Exception as exc:
        logger.exception("TelemetryLoader.load failed: %s", exc)
        st.error(f"Could not load the uploaded report: {exc}")
        return 1

    # ---- Convert to a dataframe -------------------------------- #
    try:
        df = TelemetryLoader.to_dataframe(report)
    except Exception as exc:
        logger.exception("TelemetryLoader.to_dataframe failed: %s", exc)
        st.error(f"Could not convert the report to a dataframe: {exc}")
        return 1

    # ---- Metrics overview -------------------------------------- #
    st.subheader(cfg.metrics_heading)
    try:
        st.dataframe(df, use_container_width=True)
    except TypeError:
        # Older Streamlit versions do not accept use_container_width
        # for dataframe; fall back to the default width.
        st.dataframe(df)
    _render_summary(df)

    # ---- Pareto visualization ---------------------------------- #
    st.subheader(cfg.pareto_heading)
    ParetoVisualizer = _load_pareto_visualizer()
    _render_pareto(df, cfg, ParetoVisualizer)

    # ---- Reflection summary ------------------------------------ #
    st.subheader(cfg.reflection_heading)
    reflection: Any = None
    if isinstance(report, Mapping):
        reflection = report.get("reflection")
    # An empty string is treated as "no reflection" so the reader
    # does not see a blank paragraph.
    if reflection is None or reflection == "":
        st.write("No reflection provided in this report.")
    else:
        st.write(reflection)

    return 0


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "DashboardAppConfig",
    "DashboardAppError",
    "health",
    "main",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Script entry point
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    _under_streamlit = False
    try:
        # ``streamlit.runtime.exists`` is a documented public API; it
        # returns True when running under ``streamlit run``. If the
        # function is missing (older versions) or the module raises for
        # any reason, fall through to the plain-script path.
        from streamlit import runtime as _st_runtime  # type: ignore
        _under_streamlit = bool(_st_runtime.exists())
    except Exception:
        _under_streamlit = False

    if _under_streamlit:
        sys.exit(main())
    else:
        print(
            f"Run with: streamlit run {Path(__file__).resolve()}",
            file=sys.stderr,
        )
        sys.exit(0)
