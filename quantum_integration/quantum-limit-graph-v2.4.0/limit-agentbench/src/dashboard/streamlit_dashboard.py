# src/dashboard/streamlit_dashboard.py

"""
Streamlit Sustainability Dashboard
==================================

Reads a JSONL telemetry stream (one JSON object per line) and renders:

- A raw telemetry table.
- A Pareto-style scatter chart (energy vs. latency).
- Aggregate metrics (accuracy, energy, carbon).
- A warning when no telemetry is available.

Run with::

    streamlit run src/dashboard/streamlit_dashboard.py

Enhancements
------------
- ``if __name__ == "__main__"`` guard — the module is importable without
  rendering the UI.
- ``main(config=None)`` entry point so the app can be tested with
  ``streamlit.testing`` or driven programmatically.
- ``DashboardConfig`` — frozen, validated: telemetry path, bounded tail,
  required columns, default chart axes.
- **Line-by-line error handling** — a single malformed line is logged and
  skipped instead of crashing the whole dashboard.
- **Bounded tail** — only the last ``max_lines`` entries are held in memory,
  so the dashboard works on arbitrarily large JSONL files.
- **Column validation** — the scatter chart only renders when the required
  columns exist; otherwise a clear informational message is shown.
- **Optional file upload** — when the on-disk file is missing, the user can
  upload a JSONL file directly from the browser.
- **Aggregate metric cards** — accuracy / energy / carbon summaries when
  the corresponding columns are present.
- ``logger``-based diagnostics, ``__repr__``, custom ``DashboardError``, and a
  ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Deque, Dict, Iterator, List, Mapping, Optional, Tuple

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class DashboardError(ValueError):
    """Raised for invalid dashboard inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DashboardConfig:
    """Tunable parameters for the Streamlit dashboard."""

    telemetry_file: Path = Path("telemetry_stream.json")
    max_lines: int = 10_000

    # Chart axes.
    x_column: str = "energy_kwh"
    y_column: str = "latency"

    # Columns surfaced in the summary metric cards (rendered only when
    # the underlying column exists in the dataframe).
    summary_columns: Tuple[str, ...] = ("accuracy", "energy_kwh", "carbon_kg")

    # UI text.
    title: str = "Green Agent Sustainability Dashboard"
    empty_warning: str = "No telemetry data yet."
    malformed_warning: str = (
        "Some telemetry lines could not be parsed and were skipped."
    )

    def __post_init__(self) -> None:
        if not isinstance(self.telemetry_file, Path):
            raise DashboardError("telemetry_file must be a pathlib.Path.")
        if self.max_lines <= 0:
            raise DashboardError("max_lines must be > 0.")
        for name in ("x_column", "y_column"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise DashboardError(f"{name} must be a non-empty string.")
        if not isinstance(self.summary_columns, tuple) or not self.summary_columns:
            raise DashboardError(
                "summary_columns must be a non-empty tuple."
            )
        for name in ("title", "empty_warning", "malformed_warning"):
            value = getattr(self, name)
            if not isinstance(value, str):
                raise DashboardError(f"{name} must be a string.")

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["telemetry_file"] = str(self.telemetry_file)
        d["summary_columns"] = list(self.summary_columns)
        return d

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DashboardConfig":
        if not isinstance(data, Mapping):
            raise DashboardError("DashboardConfig.from_dict expects a Mapping.")
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k == "telemetry_file":
                kwargs[k] = Path(str(v))
            elif k == "summary_columns":
                kwargs[k] = tuple(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Loader
# --------------------------------------------------------------------------- #
def iter_jsonl_lines(source: Any) -> Iterator[str]:
    """
    Yield non-empty lines from a file-like object or a path.

    Preserves line order; the caller is responsible for JSON parsing.
    """
    if hasattr(source, "read"):
        # Streamlit file_uploader returns a BytesIO-like object.
        raw = source.read()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        for line in raw.splitlines():
            if line.strip():
                yield line
    else:
        # A file path.
        with Path(str(source)).open("r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield line


def load_telemetry(
    source: Any,
    *,
    max_lines: int = 10_000,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Load a JSONL telemetry stream.

    Parameters
    ----------
    source : path | file-like
        A path or an object exposing ``.read()`` (Streamlit uploader).
    max_lines : int, default 10_000
        Number of most recent lines to keep. Older lines are dropped.

    Returns
    -------
    (rows, malformed_count)
        ``rows`` is the list of parsed dicts; ``malformed_count`` is the
        number of lines that failed to parse (or were not JSON objects).
    """
    if not isinstance(max_lines, int) or max_lines <= 0:
        raise DashboardError("max_lines must be a positive int.")

    buffer: Deque[Dict[str, Any]] = deque(maxlen=max_lines)
    malformed = 0
    for line_number, line in enumerate(iter_jsonl_lines(source), start=1):
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            malformed += 1
            logger.warning(
                "Skipping malformed JSON on line %d: %s", line_number, exc,
            )
            continue
        if not isinstance(obj, Mapping):
            malformed += 1
            logger.warning(
                "Line %d is not a JSON object (got %s); skipping.",
                line_number, type(obj).__name__,
            )
            continue
        buffer.append(dict(obj))

    return list(buffer), malformed


# --------------------------------------------------------------------------- #
# Rendering helpers
# --------------------------------------------------------------------------- #
def _render_summary(df: pd.DataFrame, columns: Tuple[str, ...]) -> None:
    """Render metric cards for the available summary columns."""
    available = [c for c in columns if c in df.columns]
    if not available:
        return
    st.subheader("Summary")
    cols = st.columns(len(available))
    for col, name in zip(cols, available):
        try:
            value = float(df[name].mean())
        except (TypeError, ValueError):
            continue
        col.metric(name, f"{value:.4g}")


def _render_scatter(
    df: pd.DataFrame, x_column: str, y_column: str
) -> None:
    """
    Render the Pareto-style scatter chart.

    Streamlit raises ``StreamlitAPIException`` when either axis column is
    missing from the dataframe; we guard against that here and show a
    clear message instead.
    """
    st.subheader("Pareto Frontier")
    missing = [c for c in (x_column, y_column) if c not in df.columns]
    if missing:
        st.info(
            f"Cannot render the Pareto chart: missing column(s) "
            f"{missing}. Expected '{x_column}' and '{y_column}'."
        )
        return
    try:
        st.scatter_chart(df, x=x_column, y=y_column)
    except Exception as exc:  # pragma: no cover — Streamlit internals
        logger.exception("scatter_chart failed: %s", exc)
        st.error(f"Could not render the scatter chart: {exc}")


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def main(config: Optional[DashboardConfig] = None) -> None:
    """Render the Streamlit dashboard."""
    cfg = config or DashboardConfig()

    st.title(cfg.title)

    path = cfg.telemetry_file
    source: Any = None
    if path.exists():
        source = path
    else:
        # Offer an upload fallback so the dashboard is still usable.
        uploaded = st.file_uploader(
            f"Telemetry file '{path}' not found. Upload a JSONL file to continue.",
            type=["json", "jsonl", "ndjson"],
        )
        if uploaded is None:
            st.warning(cfg.empty_warning)
            return
        source = uploaded

    # ---- Load (with bounded tail + per-line error handling) -------
    try:
        rows, malformed = load_telemetry(source, max_lines=cfg.max_lines)
    except DashboardError as exc:
        st.error(str(exc))
        return
    except OSError as exc:
        st.error(f"Could not read telemetry: {exc}")
        return

    if not rows:
        st.warning(cfg.empty_warning)
        return

    if malformed:
        st.warning(
            f"{cfg.malformed_warning} ({malformed} line(s) skipped.)"
        )

    df = pd.DataFrame(rows)

    st.subheader("Raw Telemetry")
    st.dataframe(df)

    _render_summary(df, cfg.summary_columns)
    _render_scatter(df, cfg.x_column, cfg.y_column)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "DashboardConfig",
    "DashboardError",
    "load_telemetry",
    "main",
]


# --------------------------------------------------------------------------- #
# Smoke test / CLI: python -m dashboard.streamlit_dashboard
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser(
        prog="streamlit_dashboard",
        description=(
            "Render the Streamlit dashboard. When run directly, falls back "
            "to a plain text summary that does not require Streamlit's "
            "runtime; use `streamlit run` for the real UI."
        ),
    )
    parser.add_argument(
        "telemetry_file",
        nargs="?",
        default=str(DashboardConfig.telemetry_file),
        help="Path to the JSONL telemetry file.",
    )
    parser.add_argument("--max-lines", type=int, default=10_000)
    args = parser.parse_args()

    cfg = DashboardConfig(
        telemetry_file=Path(args.telemetry_file),
        max_lines=args.max_lines,
    )

    # If Streamlit is executing this file, main() drives the UI.
    # If we're being run as a plain script, fall back to a text summary so
    # the smoke test still works without a Streamlit runtime.
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        if get_script_run_ctx() is not None:
            main(cfg)
        else:  # pragma: no cover — plain-script fallback
            rows, malformed = load_telemetry(
                cfg.telemetry_file, max_lines=cfg.max_lines,
            ) if cfg.telemetry_file.exists() else ([], 0)
            print(f"rows      : {len(rows)}")
            print(f"malformed : {malformed}")
            for row in rows[-3:]:
                print("  ", row)
    except ImportError:  # pragma: no cover — Streamlit unavailable
        print(
            "Streamlit is not installed; install it with "
            "`pip install streamlit` to render the UI.",
            file=sys.stderr,
        )
        sys.exit(2)
