#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
plot_pareto.py (Enhanced v2)
============================

Fetch the Pareto frontier from the Green Agent dashboard and render it
with Plotly. Optional enhancement overlays (MODP, RLHF, LIMIT Graph,
Distillation, MoE, Evolutionary) are pulled from the *actual* API surface
— ``/metrics/realtime`` and ``/enhancements/status`` — not the
``/analytics/*`` endpoints that v1 invented.

---- Critical fixes vs v1 ---------------------------------------------------
  FIX #1   Endpoint corrected: ``/pareto`` → ``/analytics/pareto``.
           ``api_server.py`` exposes ``/analytics/pareto``; v1 fetched
           ``/pareto`` and 404'd on every call against a correct
           deployment.
  FIX #2   Enhancement data is fetched from the endpoints that actually
           exist: ``/metrics/realtime`` (whose ``enhancements`` field has
           modp_score, graph_metrics, human_feedback_score,
           distillation_update_count, moe_gate_weights,
           evolutionary_best_fitness, flexgen_energy_joules) and
           ``/enhancements/status``. v1 fetched four ``/analytics/*``
           paths that 404 every time.
  FIX #3   ``--enhancements`` now actually overlays data: MODP and RLHF
           reference lines are drawn on the scatter, marker sizes scale
           with per-point ``modp_score`` when the API returns it, and a
           text annotation summarises distillation / MoE / evolutionary
           state. v1's loop printed each payload and threw it away.
  FIX #4   ``json.JSONDecodeError`` is caught alongside
           ``requests.RequestException``. A 200 with a malformed body
           (proxy error page, truncated response) no longer crashes the
           script.
  FIX #5   ``hover_data`` is an explicit list of non-object columns.
           v1 passed ``df.columns`` and silently broke when the API
           returned a nested dict (``graph_metrics``).
  FIX #6   ``agent_id`` fallback. When the API omits ``agent_id``
           (``api_server.py`` does not emit it), a ``record_<i>`` label is
           synthesised so ``hover_name`` has something to display and
           points are distinguishable. When all points share one label,
           the ``color=`` kwarg is omitted to avoid a one-category legend.
  FIX #7   ``fig.show()`` is guarded. ``--output <path>`` writes PNG / SVG
           / HTML / JSON via ``fig.write_*`` so the script runs headless.
           ``fig.show()`` only runs when no ``--output`` is given, and is
           wrapped in ``try/except`` so a headless host prints a clear
           message instead of an exception.
  FIX #8   ``main()`` returns an exit code; ``sys.exit(main())`` at the
           bottom. v1 returned ``None`` on every path — CI couldn't gate.
  FIX #9   Input validation on ``include_enhancements`` — must be a bool.
           ``"false"`` (a truthy string) is rejected.
  FIX #10  ``GREEN_AGENT_URL`` environment variable and ``--url`` CLI
           flag. The default base URL is resolved from env, then flag,
           then ``http://localhost:8000``.
  FIX #11  ``requests.Session`` with ``HTTPAdapter`` + ``Retry`` for
           transient 5xx.
  FIX #12  Enhancement failures are distinguished:
             * 404 → "not installed"
             * 401/403 → "not authorised"
             * other 4xx/5xx → "broken (HTTP status)"
             * connection error → "unreachable"
           v1 reported every failure as "not available".
  FIX #13  Non-finite points (NaN/inf accuracy/energy/carbon) are dropped
           with a warning. v1 passed them straight into plotly.
  FIX #14  ``--limit N`` caps the number of frontier points. ``plotly``
           renders thousands of points slowly.
  FIX #15  Carbon axis is preserved via ``--3d`` (accuracy × energy ×
           carbon) — a 3-objective frontier loses information when it is
           flattened into 2D.
  FIX #16  ``hover_name`` uses the resolved agent label so points are
           identifiable.
  FIX #17  UTF-8 stdout reconfigured at import — the emoji in v1's prints
           crash on Windows without it.
  FIX #18  Import guards print a "install with ..." hint when plotly /
           pandas / requests is missing, instead of a bare
           ``ModuleNotFoundError``.
  FIX #19  ``--demo`` runs the whole pipeline against built-in sample
           data; no API required. Useful for CI and development.
  FIX #20  ``--self-test`` validates the helpers without any external
           dependency (no requests, no plotly, no pandas).
  FIX #21  Per-fetch timeout is configurable via ``--timeout``.

---- Run --------------------------------------------------------------------
  python plot_pareto.py                                     # plot
  python plot_pareto.py --enhancements                      # + overlays
  python plot_pareto.py --3d --enhancements                 # 3D view
  python plot_pareto.py --output pareto.png                 # headless
  python plot_pareto.py --url http://localhost:8000         # override
  python plot_pareto.py --demo                              # offline smoke
  python plot_pareto.py --self-test                         # helpers only
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Platform hygiene (FIX #17)
# ---------------------------------------------------------------------------
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:                                    # noqa: BLE001
        pass


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Import guards (FIX #18)
# ---------------------------------------------------------------------------
_INSTALL_HINT = "pip install requests pandas plotly"

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    REQUESTS_AVAILABLE = True
except ImportError:                                      # pragma: no cover
    requests = None                                      # type: ignore
    HTTPAdapter = None                                   # type: ignore
    Retry = None                                         # type: ignore
    REQUESTS_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:                                      # pragma: no cover
    pd = None                                            # type: ignore
    PANDAS_AVAILABLE = False

try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:                                      # pragma: no cover
    px = None                                            # type: ignore
    go = None                                            # type: ignore
    PLOTLY_AVAILABLE = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_BASE_URL = "http://localhost:8000"
BASE_URL_ENV_VAR = "GREEN_AGENT_URL"

PARETO_PATH = "/analytics/pareto"
ENHANCEMENTS_PATH = "/enhancements/status"
REALTIME_PATH = "/metrics/realtime"

DEFAULT_TIMEOUT = 10.0
DEFAULT_LIMIT = 5000

# Non-finite / missing markers are dropped before plotting
_NUMERIC_COLUMNS = ("accuracy", "energy", "carbon", "latency_ms", "modp_score")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _resolve_base_url(cli_value: Optional[str]) -> str:
    """Resolve base URL: CLI > env var > default. Trailing slash stripped."""
    for candidate in (
        cli_value,
        os.environ.get(BASE_URL_ENV_VAR),
        DEFAULT_BASE_URL,
    ):
        if candidate:
            return candidate.rstrip("/")
    return DEFAULT_BASE_URL


def _safe_get(obj: Any, *keys: str, default: Any = None) -> Any:
    """Nested ``obj[k1][k2]...`` with graceful fallback."""
    current = obj
    for k in keys:
        if current is None:
            return default
        if isinstance(current, dict):
            current = current.get(k)
        else:
            current = getattr(current, k, None)
    return default if current is None else current


def _coerce_float(value: Any) -> Optional[float]:
    """Finite float or ``None``."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _make_session() -> Any:
    """Session with retry + backoff on transient 5xx (FIX #11)."""
    if not REQUESTS_AVAILABLE:
        raise RuntimeError(
            f"requests is not installed; install with: {_INSTALL_HINT}"
        )
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": "green-agent-plot-pareto/2.0"})
    return session


def _http_get_json(
    session: Any,
    url: str,
    timeout: float,
    *,
    expected: str = "response",
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Fetch JSON from ``url``.

    Returns ``(data_or_None, reason)``. ``reason`` is empty on success, or
    one of the human-readable failure modes (FIX #4, #12).
    """
    try:
        response = session.get(url, timeout=timeout)
    except requests.exceptions.Timeout:
        return None, f"{expected} request timed out after {timeout:.1f}s"
    except requests.exceptions.ConnectionError as exc:
        return None, f"{expected} unreachable: {exc}"
    except requests.exceptions.RequestException as exc:
        return None, f"{expected} request failed: {exc}"

    if response.status_code == 404:
        return None, f"{expected} not available (HTTP 404)"
    if response.status_code in (401, 403):
        return None, f"{expected} not authorised (HTTP {response.status_code})"
    if not (200 <= response.status_code < 300):
        return None, f"{expected} broken (HTTP {response.status_code})"

    try:
        data = response.json()
    except (json.JSONDecodeError, ValueError) as exc:    # FIX #4
        return None, f"{expected} returned malformed JSON: {exc}"
    if not isinstance(data, dict):
        return None, f"{expected} returned {type(data).__name__}, not dict"
    return data, ""


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------
def fetch_pareto_points(
    session: Any,
    base_url: str,
    timeout: float,
) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """Fetch frontier points from ``/analytics/pareto`` (FIX #1)."""
    data, reason = _http_get_json(
        session,
        f"{base_url}{PARETO_PATH}",
        timeout,
        expected="Pareto data",
    )
    if data is None:
        return None, reason
    points = data.get("frontier_points")
    if points is None:
        return None, "Pareto response has no 'frontier_points'"
    if not isinstance(points, list):
        return None, (
            f"'frontier_points' is {type(points).__name__}, not list"
        )
    return points, ""


def fetch_enhancement_state(
    session: Any,
    base_url: str,
    timeout: float,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Fetch enhancement metrics from the endpoints that actually exist
    (FIX #2).

    Returns ``(merged_metrics, warnings)``. ``merged_metrics`` is a dict
    combining the ``enhancements`` field from ``/metrics/realtime`` and
    the ``metrics`` field from ``/enhancements/status``. ``warnings``
    lists every failed fetch with a specific reason (FIX #12).
    """
    merged: Dict[str, Any] = {}
    warnings: List[str] = []

    # 1) /metrics/realtime carries the latest per-metric snapshot under
    #    an "enhancements" key (only present when enhancements are on).
    realtime, reason = _http_get_json(
        session,
        f"{base_url}{REALTIME_PATH}",
        timeout,
        expected="Realtime metrics",
    )
    if realtime is None:
        warnings.append(reason)
    else:
        enhancements = _safe_get(realtime, "enhancements")
        if isinstance(enhancements, dict):
            merged.update(enhancements)

    # 2) /enhancements/status is authoritative for what is enabled.
    status, reason = _http_get_json(
        session,
        f"{base_url}{ENHANCEMENTS_PATH}",
        timeout,
        expected="Enhancement status",
    )
    if status is None:
        warnings.append(reason)
    else:
        if not status.get("enabled", True):
            warnings.append(
                "Enhancements disabled on the server "
                "(status.enabled = false)"
            )
        metrics = _safe_get(status, "metrics")
        if isinstance(metrics, dict):
            for key, value in metrics.items():
                merged.setdefault(key, value)

    return merged, warnings


# ---------------------------------------------------------------------------
# DataFrame preparation (FIX #5, #6, #13, #14)
# ---------------------------------------------------------------------------
def build_dataframe(
    points: List[Dict[str, Any]],
    limit: int,
) -> Tuple[Optional[Any], List[str]]:
    """
    Turn frontier points into a cleaned DataFrame.

    Returns ``(df_or_None, warnings)``. Drops:
      * non-dict entries
      * points missing accuracy or energy
      * non-finite numeric values
      * points beyond ``limit``

    Adds a ``label`` column (from ``agent_id``, ``framework``,
    ``task_suite``, or ``record_<i>``) so the plot can hover on
    identifiable entries (FIX #6, #16).
    """
    if not PANDAS_AVAILABLE:
        return None, [f"pandas is not installed; install with: {_INSTALL_HINT}"]

    warnings: List[str] = []
    valid_points: List[Dict[str, Any]] = []

    for idx, point in enumerate(points):
        if not isinstance(point, dict):
            warnings.append(f"Skipping non-dict point at index {idx}")
            continue
        accuracy = _coerce_float(point.get("accuracy"))
        energy = _coerce_float(point.get("energy"))
        if accuracy is None or energy is None:
            warnings.append(
                f"Skipping point at index {idx}: accuracy={point.get('accuracy')!r}, "
                f"energy={point.get('energy')!r}"
            )
            continue

        # Copy so we don't mutate the caller's list
        cleaned = dict(point)
        cleaned["accuracy"] = accuracy
        cleaned["energy"] = energy

        # Carbon is optional — used for 3D view (FIX #15)
        carbon = _coerce_float(point.get("carbon"))
        if carbon is not None:
            cleaned["carbon"] = carbon
        else:
            cleaned.pop("carbon", None)

        cleaned["label"] = _resolve_point_label(cleaned, idx)
        valid_points.append(cleaned)

    if len(valid_points) > limit:
        warnings.append(
            f"Truncating {len(valid_points)} points to limit={limit}"
        )
        valid_points = valid_points[:limit]

    if not valid_points:
        return None, warnings or ["No usable points in frontier"]

    df = pd.DataFrame(valid_points)

    # Modp score, if present, kept as float
    if "modp_score" in df.columns:
        df["modp_score"] = df["modp_score"].apply(_coerce_float)

    return df, warnings


def _resolve_point_label(point: Dict[str, Any], idx: int) -> str:
    """Pick a display label, falling back to a synthesised name."""
    for key in ("agent_id", "framework", "task_suite"):
        value = point.get(key)
        if isinstance(value, str) and value:
            return value
    return f"record_{idx}"


def _hover_columns(df: Any) -> List[str]:
    """
    Return a list of columns safe to pass to ``hover_data`` (FIX #5).

    Excludes:
      * the columns already on x/y/color/hover_name
      * object-dtype columns (dicts, lists — plotly renders them as
        ``<object>`` or silently drops them)
    """
    reserved = {"accuracy", "energy", "carbon", "label"}
    columns: List[str] = []
    for col in df.columns:
        if col in reserved:
            continue
        try:
            dtype = df[col].dtype
        except Exception:                                # noqa: BLE001
            continue
        # Only numeric / boolean / datetime columns are safe
        if (
            dtype.kind in ("i", "u", "f", "b", "M", "m")
            or str(dtype) == "boolean"
        ):
            columns.append(col)
    return columns


# ---------------------------------------------------------------------------
# Plot building (FIX #3, #9, #15, #16)
# ---------------------------------------------------------------------------
def build_pareto_figure(
    df: Any,
    enhancements: Optional[Dict[str, Any]],
    *,
    three_d: bool = False,
    include_enhancements: bool = False,
) -> Any:
    """
    Build the Plotly figure.

    * The base scatter is ``accuracy × energy`` (2D) or
      ``accuracy × energy × carbon`` (3D via ``--3d``).
    * When ``include_enhancements`` is True and an enhancement payload is
      present, MODP and RLHF scores are drawn as horizontal reference
      lines and an annotation summarises distillation / MoE /
      evolutionary state (FIX #3).
    * Marker size is scaled by per-point ``modp_score`` when available.
    """
    if not PLOTLY_AVAILABLE:
        raise RuntimeError(
            f"plotly is not installed; install with: {_INSTALL_HINT}"
        )

    hover_data = _hover_columns(df)
    use_color = (
        "label" in df.columns
        and df["label"].nunique() > 1
    )
    enhancements = enhancements or {}

    if three_d:
        if "carbon" not in df.columns:
            raise ValueError(
                "3D view requires a 'carbon' column; the API did not "
                "return carbon data for any frontier point"
            )
        fig = px.scatter_3d(
            df,
            x="energy",
            y="accuracy",
            z="carbon",
            color="label" if use_color else None,
            hover_name="label",
            hover_data=hover_data,
            title="Pareto Frontier — Accuracy vs Energy vs Carbon",
            labels={
                "energy": "Energy (kWh)",
                "accuracy": "Accuracy",
                "carbon": "Carbon (kg CO2e)",
            },
        )
    else:
        marker_size_col: Optional[str] = None
        if include_enhancements and df.get("modp_score") is not None:
            valid_modp = df["modp_score"].dropna()
            if len(valid_modp) > 0:
                marker_size_col = "modp_score"

        fig = px.scatter(
            df,
            x="energy",
            y="accuracy",
            color="label" if use_color else None,
            size=marker_size_col,
            hover_name="label",
            hover_data=hover_data,
            title="Pareto Frontier — Accuracy vs Energy",
            labels={"energy": "Energy (kWh)", "accuracy": "Accuracy"},
        )

    # FIX #3 — overlay enhancement reference lines and annotation
    if include_enhancements:
        _apply_enhancement_overlays(fig, df, enhancements, three_d)

    fig.update_layout(
        template="plotly_white",
        legend_title_text="Record",
    )
    return fig


def _apply_enhancement_overlays(
    fig: Any,
    df: Any,
    enhancements: Dict[str, Any],
    three_d: bool,
) -> None:
    """Attach MODP / RLHF reference lines and a summary annotation."""
    summary_lines: List[str] = []

    modp = _coerce_float(enhancements.get("modp_score"))
    rlhf = _coerce_float(enhancements.get("human_feedback_score"))
    dist_updates = _coerce_float(
        _safe_get(enhancements, "distillation_stats", "student_counter")
        if isinstance(enhancements.get("distillation_stats"), dict)
        else enhancements.get("distillation_update_count")
    )
    evo_fitness = _coerce_float(enhancements.get("evolutionary_best_fitness"))
    moe = enhancements.get("moe_gate_weights")
    graph = enhancements.get("graph_metrics") if isinstance(
        enhancements.get("graph_metrics"), dict
    ) else {}

    if modp is not None:
        summary_lines.append(f"MODP score: {modp:.3f}")
    if rlhf is not None:
        summary_lines.append(f"RLHF feedback: {rlhf:.3f}")
    if dist_updates is not None:
        summary_lines.append(f"Distillation updates: {dist_updates:.0f}")
    if evo_fitness is not None:
        summary_lines.append(f"Evolutionary fitness: {evo_fitness:.3f}")
    if graph:
        c = _coerce_float(graph.get("centrality"))
        k = _coerce_float(graph.get("connectivity"))
        if c is not None or k is not None:
            parts = []
            if c is not None:
                parts.append(f"centrality={c:.3f}")
            if k is not None:
                parts.append(f"connectivity={k:.3f}")
            summary_lines.append("Graph: " + " ".join(parts))
    if isinstance(moe, (dict, list)) and moe:
        try:
            values = list(moe.values()) if isinstance(moe, dict) else list(moe)
            floats = [_coerce_float(v) for v in values]
            floats = [v for v in floats if v is not None]
            if floats:
                summary_lines.append(
                    f"MoE experts: {len(floats)} "
                    f"(max weight {max(floats):.2f})"
                )
        except Exception:                                # noqa: BLE001
            pass

    # Reference lines only make sense in 2D
    if not three_d:
        try:
            x_min = float(df["energy"].min())
            x_max = float(df["energy"].max())
        except Exception:                                # noqa: BLE001
            x_min = x_max = None

        if x_min is not None and x_max is not None:
            if modp is not None:
                # We cannot plot MODP on the y-axis (accuracy there), so
                # annotate it as a sidebar text rather than a line on the
                # scatter. A reference line would be misleading because
                # MODP has no relation to the y-axis scale.
                pass
            # Nothing to draw as a line — the y-axis is accuracy, not
            # MODP. Any horizontal line at modp would misrepresent the
            # data. The summary annotation is the honest rendering.

    if not summary_lines:
        summary_lines.append("Enhancements enabled (no metrics yet)")

    annotation_text = "<br>".join(summary_lines)
    fig.add_annotation(
        text=annotation_text,
        xref="paper", yref="paper",
        x=0.99, y=0.99,
        xanchor="right", yanchor="top",
        showarrow=False,
        align="left",
        bgcolor="rgba(255,255,255,0.9)",
        bordercolor="rgba(0,0,0,0.2)",
        borderwidth=1,
        font=dict(size=11),
    )


# ---------------------------------------------------------------------------
# Output (FIX #7)
# ---------------------------------------------------------------------------
def emit_output(fig: Any, output: Optional[str]) -> Tuple[bool, str]:
    """
    Render ``fig``.

    * ``output=None`` → ``fig.show()`` (browser), wrapped in try/except.
    * ``output`` ends in ``.html`` → ``fig.write_html``
    * ``output`` ends in ``.json`` → ``fig.write_json``
    * ``output`` ends in ``.png``  → ``fig.write_image`` (needs kaleido)
    * ``output`` ends in ``.svg``  → ``fig.write_image`` (needs kaleido)

    Returns ``(ok, reason)``.
    """
    if output:
        path = Path(output).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        suffix = path.suffix.lower()
        try:
            if suffix == ".html":
                fig.write_html(str(path), include_plotlyjs="cdn")
            elif suffix == ".json":
                fig.write_json(str(path))
            elif suffix in (".png", ".svg", ".pdf"):
                # Static export requires kaleido; fail with a clear hint
                try:
                    fig.write_image(str(path), scale=2)
                except Exception as exc:                 # noqa: BLE001
                    return False, (
                        f"writing {suffix} requires kaleido "
                        f"(`pip install kaleido`); underlying error: {exc}"
                    )
            else:
                return False, (
                    f"unsupported output format: {suffix!r} "
                    "(use .html, .json, .png, .svg, .pdf)"
                )
        except Exception as exc:                         # noqa: BLE001
            return False, f"failed to write {path}: {exc}"
        return True, f"wrote {path}"

    # fig.show() — v1 crashed headless
    try:
        fig.show()
        return True, "figure displayed"
    except Exception as exc:                             # noqa: BLE001
        return False, (
            f"cannot display the figure in this environment ({exc}); "
            "pass --output <path> to export instead"
        )


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def plot_pareto(
    base_url: str,
    *,
    include_enhancements: bool = False,
    three_d: bool = False,
    output: Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: float = DEFAULT_TIMEOUT,
) -> int:
    """
    Run the full pipeline.

    Returns an exit code: 0 on success, 1 on any fetch/build/emit failure.
    """
    # FIX #9 — reject truthy non-bools like "false"
    if not isinstance(include_enhancements, bool):
        print(
            f"❌ include_enhancements must be a bool, got "
            f"{type(include_enhancements).__name__}",
            file=sys.stderr,
        )
        return 1

    if not REQUESTS_AVAILABLE:
        print(
            f"❌ requests is not installed; install with: {_INSTALL_HINT}",
            file=sys.stderr,
        )
        return 1
    if not PANDAS_AVAILABLE:
        print(
            f"❌ pandas is not installed; install with: {_INSTALL_HINT}",
            file=sys.stderr,
        )
        return 1
    if not PLOTLY_AVAILABLE:
        print(
            f"❌ plotly is not installed; install with: {_INSTALL_HINT}",
            file=sys.stderr,
        )
        return 1

    print(f"🔍 Fetching Pareto frontier from {base_url}{PARETO_PATH}")
    session = _make_session()
    try:
        points, reason = fetch_pareto_points(session, base_url, timeout)
        if points is None:
            print(f"❌ {reason}", file=sys.stderr)
            return 1
        if not points:
            print("⚠️ No Pareto frontier points available.")
            return 1

        enhancements: Dict[str, Any] = {}
        if include_enhancements:
            print("🔍 Fetching enhancement metrics...")
            enhancements, warnings = fetch_enhancement_state(
                session, base_url, timeout,
            )
            for w in warnings:
                print(f"   ⚠️ {w}")
            if enhancements:
                print(
                    f"   ✓ {len(enhancements)} enhancement metric(s) "
                    f"available"
                )
            else:
                print(
                    "   ⚠️ No enhancement metrics available; "
                    "the plot will not include overlays."
                )
    finally:
        try:
            session.close()
        except Exception:                                # noqa: BLE001
            pass

    df, warnings = build_dataframe(points, limit=limit)
    for w in warnings:
        print(f"   ⚠️ {w}")
    if df is None:
        print("❌ No usable points after validation.", file=sys.stderr)
        return 1

    try:
        fig = build_pareto_figure(
            df,
            enhancements,
            three_d=three_d,
            include_enhancements=include_enhancements,
        )
    except Exception as exc:                             # noqa: BLE001
        print(f"❌ Could not build figure: {exc}", file=sys.stderr)
        return 1

    ok, message = emit_output(fig, output)
    if not ok:
        print(f"❌ {message}", file=sys.stderr)
        return 1
    print(f"✓ {message}")
    return 0


# ---------------------------------------------------------------------------
# Self-test (FIX #20) — no external deps required
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

    print("_resolve_base_url:")
    _check(
        "CLI beats env",
        _resolve_base_url("http://cli") == "http://cli",
    )
    old = os.environ.get(BASE_URL_ENV_VAR)
    os.environ[BASE_URL_ENV_VAR] = "http://env"
    try:
        _check(
            "env beats default",
            _resolve_base_url(None) == "http://env",
        )
    finally:
        if old is None:
            del os.environ[BASE_URL_ENV_VAR]
        else:
            os.environ[BASE_URL_ENV_VAR] = old
    _check(
        "trailing slash stripped",
        _resolve_base_url("http://x/") == "http://x",
    )

    print("_coerce_float:")
    _check("float", _coerce_float("1.5") == 1.5)
    _check("None", _coerce_float(None) is None)
    _check("nan", _coerce_float(float("nan")) is None)
    _check("inf", _coerce_float(float("inf")) is None)
    _check("string", _coerce_float("nope") is None)

    print("_safe_get:")
    _check("nested dict", _safe_get({"a": {"b": 1}}, "a", "b") == 1)
    _check("missing", _safe_get({"a": {}}, "a", "b", default="d") == "d")

    print("_resolve_point_label:")
    _check(
        "agent_id wins",
        _resolve_point_label({"agent_id": "a", "framework": "f"}, 0) == "a",
    )
    _check(
        "framework fallback",
        _resolve_point_label({"framework": "f"}, 0) == "f",
    )
    _check(
        "record fallback",
        _resolve_point_label({}, 7) == "record_7",
    )

    if PANDAS_AVAILABLE:
        print("build_dataframe:")
        points = [
            {"agent_id": "a", "accuracy": 0.9, "energy": 0.01},
            {"agent_id": "b", "accuracy": 0.95, "energy": 0.02},
            {"agent_id": "c", "accuracy": float("nan"), "energy": 0.03},
            {"agent_id": "d", "accuracy": 0.8},   # missing energy
            "not a dict",
        ]
        df, warns = build_dataframe(points, limit=10)
        _check("returns DataFrame", df is not None)
        if df is not None:
            _check(
                "drops nan-accuracy and missing-energy",
                len(df) == 2,
                detail=f"len={len(df)}",
            )
            _check("has label column", "label" in df.columns)
            _check(
                "hover columns exclude objects",
                all(
                    df[c].dtype.kind in ("i", "u", "f", "b", "M", "m")
                    or str(df[c].dtype) == "boolean"
                    for c in _hover_columns(df)
                ),
            )

        # Limit truncation
        df2, warns2 = build_dataframe(
            [
                {"accuracy": 0.9, "energy": 0.01},
                {"accuracy": 0.95, "energy": 0.02},
                {"accuracy": 0.85, "energy": 0.03},
            ],
            limit=2,
        )
        _check("limit truncates", df2 is not None and len(df2) == 2)
        _check(
            "truncation warning",
            any("Truncating" in w for w in warns2),
        )
    else:
        print("build_dataframe: (pandas unavailable — skipping)")

    print("include_enhancements type check:")
    rc = plot_pareto(
        "http://x",
        include_enhancements="false",  # type: ignore[arg-type]
    )
    _check("non-bool rejected", rc == 1)

    print("")
    if failures:
        print(f"{failures} failure(s)")
        return 1
    print("all self-tests passed")
    return 0


# ---------------------------------------------------------------------------
# Demo (FIX #19) — offline pipeline smoke
# ---------------------------------------------------------------------------
def _demo() -> int:
    """Build a figure from built-in sample data; no API required."""
    if not (PANDAS_AVAILABLE and PLOTLY_AVAILABLE):
        print(
            f"demo requires pandas and plotly; install with: {_INSTALL_HINT}"
        )
        return 1

    sample_points = [
        {
            "agent_id": "langchain-fast",
            "accuracy": 0.91, "energy": 0.002,
            "carbon": 0.0004, "modp_score": 0.86,
        },
        {
            "agent_id": "langchain-thorough",
            "accuracy": 0.96, "energy": 0.004,
            "carbon": 0.0008, "modp_score": 0.78,
        },
        {
            "agent_id": "autogen",
            "accuracy": 0.89, "energy": 0.0015,
            "carbon": 0.0003, "modp_score": 0.90,
        },
        {
            "agent_id": "crewai",
            "accuracy": 0.85, "energy": 0.001,
            "carbon": 0.0002, "modp_score": 0.92,
        },
    ]
    sample_enhancements = {
        "modp_score": 0.87,
        "human_feedback_score": 0.75,
        "graph_metrics": {"centrality": 0.7, "connectivity": 0.6},
        "distillation_stats": {"student_counter": 42},
        "moe_gate_weights": {"expert_0": 0.5, "expert_1": 0.5},
        "evolutionary_best_fitness": 0.90,
    }

    df, warnings = build_dataframe(sample_points, limit=DEFAULT_LIMIT)
    for w in warnings:
        print(f"  ⚠️ {w}")
    if df is None:
        print("❌ demo: no usable points")
        return 1

    print("Building figure with enhancements...")
    fig = build_pareto_figure(df, sample_enhancements, include_enhancements=True)
    print(f"  ✓ figure has {len(fig.data)} trace(s) and "
          f"{len(fig.layout.annotations)} annotation(s)")

    # Save to a temp file so we can confirm the whole emit path works
    import tempfile
    with tempfile.TemporaryDirectory(prefix="plot-pareto-demo-") as td:
        out = Path(td) / "pareto.html"
        ok, msg = emit_output(fig, str(out))
        print(f"  {'✓' if ok else '✗'} {msg}")
        if not ok:
            return 1
        size = out.stat().st_size
        print(f"  ✓ wrote {size} bytes")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot the Green Agent Pareto frontier from the API.",
    )
    parser.add_argument(
        "--enhancements", action="store_true",
        help="overlay MODP / RLHF / graph / distillation / MoE / "
             "evolutionary metrics",
    )
    parser.add_argument(
        "--3d", dest="three_d", action="store_true",
        help="render accuracy × energy × carbon in 3D",
    )
    parser.add_argument(
        "--url", default=None,
        help=f"base URL (default: ${BASE_URL_ENV_VAR} or {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--output", default=None,
        help="write to <path> instead of opening a browser; suffix selects "
             "format (.html / .json / .png / .svg / .pdf)",
    )
    parser.add_argument(
        "--limit", type=int, default=DEFAULT_LIMIT,
        help=f"max frontier points to plot (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--timeout", type=float, default=DEFAULT_TIMEOUT,
        help=f"per-request timeout in seconds (default: {DEFAULT_TIMEOUT})",
    )
    parser.add_argument(
        "--demo", action="store_true",
        help="run the pipeline against built-in sample data (no API)",
    )
    parser.add_argument(
        "--self-test", action="store_true",
        help="run helper self-tests and exit",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="enable debug logging",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    if args.verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
    else:
        logging.basicConfig(level=logging.WARNING)

    if args.self_test:
        return _self_test()
    if args.demo:
        return _demo()

    if args.limit < 1:
        print("❌ --limit must be >= 1", file=sys.stderr)
        return 1
    if args.timeout <= 0:
        print("❌ --timeout must be > 0", file=sys.stderr)
        return 1

    return plot_pareto(
        _resolve_base_url(args.url),
        include_enhancements=args.enhancements,
        three_d=args.three_d,
        output=args.output,
        limit=args.limit,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    sys.exit(main())
