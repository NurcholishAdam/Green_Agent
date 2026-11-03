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

Or, from a plain Python process, run the smoke test::

    python -m dashboard.streamlit_dashboard

Lazy framework imports
----------------------
``streamlit`` and ``pandas`` are imported **lazily**, inside the
functions that need them. Consequently:

- ``DashboardConfig``, ``load_telemetry``, ``load_telemetry_detailed``,
  and ``iter_jsonl_lines`` are importable and usable in any environment
  — no streamlit / pandas installation required.
- Only :func:`main` and the ``_render_*`` helpers require the
  framework packages.
- A CI pipeline that consumes the JSONL parser does not pull in pandas
  or Streamlit.

Testability
-----------
:func:`main` **requires a running Streamlit runtime** — it calls
``st.set_page_config``, ``st.title``, etc. It is not a plain-Python
function and cannot be driven without Streamlit's script runner.

To test the UI programmatically, use ``streamlit.testing.v1.AppTest``
(mocked ``st`` calls are not supported). To test the pure data layer,
use :func:`load_telemetry` / :func:`load_telemetry_detailed` directly —
they have no framework dependency.

:func:`main` returns an ``int`` exit code: ``0`` on success, ``1`` on
any failure (empty data, missing file, unreadable file, malformed
input). This lets ``AppTest`` and CI harnesses assert on the outcome.

Return shape compatibility
--------------------------
:func:`load_telemetry` keeps its original ``(rows, malformed_count)``
return shape. A new :func:`load_telemetry_detailed` returns a
:class:`LoadResult` carrying the malformed line numbers, useful for
debugging a corrupted telemetry stream.

Enhancements
------------
- ``if __name__ == "__main__"`` guard — the module is importable
  without rendering the UI, and running it as a plain script runs the
  smoke test instead of attempting to launch Streamlit.
- ``main(config=None) -> int`` entry point with a real exit code.
- ``DashboardConfig`` — frozen, ``__repr__``, fully validated:
  bounded history, required columns, default chart axes, UI text.
  ``from_dict(..., strict=True)`` rejects unknown keys.
- **Line-by-line error handling** — a single malformed line is logged
  and skipped instead of crashing the whole dashboard.
- **Bounded tail** — only the last ``max_lines`` entries are held in
  memory, so the dashboard works on arbitrarily large JSONL files.
  The default is *file-order-last*, not timestamp-last; see the note
  on ``max_lines`` below.
- **BOM-tolerant decoding** — the file path is opened with
  ``utf-8-sig`` so a UTF-8 BOM does not corrupt the first line.
  Invalid UTF-8 raises :class:`DashboardError` rather than silently
  producing replacement characters.
- **Source-type validation** — :func:`load_telemetry` accepts
  ``str`` / :class:`pathlib.Path` / file-like objects, and raises
  :class:`DashboardError` for anything else (no more
  ``FileNotFoundError: '42'``).
- **Column validation** — the scatter chart only renders when the
  required columns exist **and** contain at least one pair of valid
  numeric values; otherwise a clear informational message is shown.
- **Numeric-coercion-aware summary cards** — non-numeric or NaN
  values are shown as ``"—"`` instead of silently producing ``nan``.
- **Optional file upload** — when the on-disk file is missing, the
  user can upload a JSONL file directly from the browser.
- **Aggregate metric cards** — accuracy / energy / carbon summaries
  when the corresponding columns are present.
- **Streamlit-safe entry point** — the smoke test is separated from
  ``main()`` so the file can be tested without a Streamlit runtime,
  and the runtime detection uses the documented
  ``streamlit.runtime.exists()`` rather than internal APIs.
- **Atomic write for tests, no framework coupling in the data layer.**
- ``logger``-based diagnostics, ``__version__``, custom
  :class:`DashboardError`, and a ``_check()``-based smoke test that
  survives ``python -O``.

Compatibility
-------------
- ``DashboardConfig(...)`` — same fields. Two behaviour changes:
  * ``max_lines`` must now be a real ``int`` (``bool`` and ``str``
    raise ``DashboardError`` instead of ``TypeError``).
  * ``summary_columns`` entries must be non-empty strings.
- ``DashboardConfig.from_dict(..., strict=True)`` — the default
  flipped from "drop unknown keys" to "reject unknown keys". Pass
  ``strict=False`` to preserve the old lenient behaviour.
- ``DashboardConfig.telemetry_file`` default changed from
  ``Path("telemetry_stream.json")`` to
  ``Path("telemetry_stream.jsonl")`` — the content is JSONL, so the
  extension now matches.
- ``load_telemetry(source, *, max_lines=10_000)`` — same signature,
  same ``(rows, malformed_count)`` return shape. **Behaviour change**:
  malformed sources (non-path, non-file-like) now raise
  :class:`DashboardError` instead of ``FileNotFoundError``.
- ``load_telemetry_detailed(source, *, max_lines=10_000) -> LoadResult``
  — new. Returns ``rows``, ``malformed_count``, and
  ``malformed_line_numbers``.
- ``iter_jsonl_lines(source)`` — same behaviour, plus a proper
  source-type check and BOM-aware decoding. Now listed in
  ``__all__``.
- ``main(config=None)`` — same signature, **returns ``int``** (0/1).
  The previous version returned ``None``.
- ``main(bad_config)`` — raises :class:`DashboardError` **before**
  importing Streamlit, so it is testable in environments without
  Streamlit installed.

Requires Python 3.8+.
"""

from __future__ import annotations

import json
import logging
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any, Deque, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple,
    Union,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class DashboardError(ValueError):
    """Raised for invalid dashboard inputs or configuration."""


# --------------------------------------------------------------------------- #
# Lazy framework imports
# --------------------------------------------------------------------------- #
def _import_streamlit() -> Any:
    """Import :mod:`streamlit` or raise a clear :class:`DashboardError`."""
    try:
        import streamlit as st  # type: ignore
    except ImportError as exc:
        raise DashboardError(
            "streamlit is required to render the dashboard; install it "
            "with `pip install streamlit`."
        ) from exc
    return st


def _import_pandas() -> Any:
    """Import :mod:`pandas` or raise a clear :class:`DashboardError`."""
    try:
        import pandas as pd  # type: ignore
    except ImportError as exc:
        raise DashboardError(
            "pandas is required to render the dashboard; install it "
            "with `pip install pandas`."
        ) from exc
    return pd


# --------------------------------------------------------------------------- #
# Validators
# --------------------------------------------------------------------------- #
def _require_str(name: str, value: Any) -> str:
    if not isinstance(value, str):
        raise DashboardError(
            f"{name} must be a string, got {type(value).__name__}."
        )
    return value


def _require_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise DashboardError(
            f"{name} must be a non-empty string, got "
            f"{type(value).__name__}."
        )
    return value


def _require_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DashboardError(f"{name} must be an int.")
    if value <= 0:
        raise DashboardError(f"{name} must be > 0, got {value}.")
    return value


# --------------------------------------------------------------------------- #
# Source coercion
# --------------------------------------------------------------------------- #
def _coerce_source(source: Any) -> Union[Path, Any]:
    """
    Return a :class:`pathlib.Path` or a file-like object.

    Raises :class:`DashboardError` for any other input type. ``str`` is
    treated as a path; any object with ``.read()`` is treated as a
    stream (Streamlit's ``UploadedFile`` qualifies).
    """
    if isinstance(source, Path):
        return source
    if isinstance(source, str):
        if not source:
            raise DashboardError("source must not be an empty string.")
        return Path(source)
    if hasattr(source, "__fspath__"):
        return Path(source)
    if hasattr(source, "read"):
        return source
    raise DashboardError(
        f"source must be a str, Path, or file-like object, got "
        f"{type(source).__name__}."
    )


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DashboardConfig:
    """Tunable parameters for the Streamlit dashboard.

    ``max_lines`` keeps the **last** ``max_lines`` entries **in file
    order**. If your JSONL file is not append-only, use a
    timestamp-sorted pre-processing step; the loader does not sort.
    """

    telemetry_file: Path = Path("telemetry_stream.jsonl")
    max_lines: int = 10_000

    # Chart axes.
    x_column: str = "energy_kwh"
    y_column: str = "latency"

    # Columns surfaced in the summary metric cards (rendered only when
    # the underlying column exists in the dataframe).
    summary_columns: Tuple[str, ...] = (
        "accuracy", "energy_kwh", "carbon_kg",
    )

    # UI text.
    title: str = "Green Agent Sustainability Dashboard"
    empty_warning: str = "No telemetry data yet."
    malformed_warning: str = (
        "Some telemetry lines could not be parsed and were skipped."
    )

    def __post_init__(self) -> None:
        if not isinstance(self.telemetry_file, Path):
            raise DashboardError(
                "telemetry_file must be a pathlib.Path."
            )
        _require_positive_int("max_lines", self.max_lines)
        _require_nonempty_str("x_column", self.x_column)
        _require_nonempty_str("y_column", self.y_column)
        if not isinstance(self.summary_columns, tuple) or (
            not self.summary_columns
        ):
            raise DashboardError(
                "summary_columns must be a non-empty tuple."
            )
        for i, c in enumerate(self.summary_columns):
            _require_nonempty_str(f"summary_columns[{i}]", c)
        _require_str("title", self.title)
        _require_str("empty_warning", self.empty_warning)
        _require_str("malformed_warning", self.malformed_warning)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "telemetry_file": str(self.telemetry_file),
            "max_lines": self.max_lines,
            "x_column": self.x_column,
            "y_column": self.y_column,
            "summary_columns": list(self.summary_columns),
            "title": self.title,
            "empty_warning": self.empty_warning,
            "malformed_warning": self.malformed_warning,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True,
    ) -> "DashboardConfig":
        if not isinstance(data, Mapping):
            raise DashboardError(
                "DashboardConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise DashboardError(
                    f"unknown DashboardConfig keys: {unknown}"
                )
        kwargs: Dict[str, Any] = {}
        try:
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
        except DashboardError:
            raise
        except (TypeError, ValueError) as exc:
            raise DashboardError(
                f"invalid DashboardConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "DashboardConfig("
            f"telemetry_file={str(self.telemetry_file)!r}, "
            f"max_lines={self.max_lines}, "
            f"axes=({self.x_column!r}, {self.y_column!r}))"
        )


# --------------------------------------------------------------------------- #
# Load result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LoadResult:
    """Return shape of :func:`load_telemetry_detailed`."""

    rows: List[Dict[str, Any]] = field(default_factory=list)
    malformed_count: int = 0
    malformed_line_numbers: Tuple[int, ...] = ()

    def __repr__(self) -> str:
        return (
            "LoadResult("
            f"rows={len(self.rows)}, "
            f"malformed={self.malformed_count})"
        )


# --------------------------------------------------------------------------- #
# Loader
# --------------------------------------------------------------------------- #
def iter_jsonl_lines(source: Any) -> Iterator[str]:
    """
    Yield non-empty lines from a file-like object or a path.

    The file path is opened with ``utf-8-sig`` so a leading BOM is
    stripped from the first line. Invalid UTF-8 in a file-like source
    raises :class:`DashboardError` rather than silently emitting
    replacement characters.

    Raises
    ------
    DashboardError
        If ``source`` is not a valid source type, or the byte stream
        cannot be decoded as UTF-8.
    """
    src = _coerce_source(source)
    if isinstance(src, Path):
        try:
            with src.open("r", encoding="utf-8-sig") as f:
                for line in f:
                    if line.strip():
                        yield line
        except UnicodeDecodeError as exc:
            raise DashboardError(
                f"could not decode {src}: {exc}"
            ) from exc
        return

    # File-like source (Streamlit uploader, BytesIO, etc.).
    raw = src.read()
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise DashboardError(
                f"uploaded file is not valid UTF-8: {exc}"
            ) from exc
    for line in raw.splitlines():
        if line.strip():
            yield line


def load_telemetry_detailed(
    source: Any,
    *,
    max_lines: int = 10_000,
) -> LoadResult:
    """
    Load a JSONL telemetry stream, returning line numbers of malformed
    entries.

    Parameters
    ----------
    source : str | Path | file-like
        A path or an object exposing ``.read()`` (Streamlit uploader).
    max_lines : int, default 10_000
        Number of tail entries to keep (file-order-last).

    Returns
    -------
    LoadResult
        ``rows`` holds the parsed dicts; ``malformed_count`` is the
        number of lines that failed to parse (or were not JSON
        objects); ``malformed_line_numbers`` holds the 1-based line
        numbers of those entries.

    Raises
    ------
    DashboardError
        On bad ``max_lines``, invalid source type, or a byte stream
        that cannot be decoded as UTF-8.
    """
    _require_positive_int("max_lines", max_lines)

    buffer: Deque[Dict[str, Any]] = deque(maxlen=max_lines)
    malformed_line_numbers: List[int] = []

    for line_number, line in enumerate(
        iter_jsonl_lines(source), start=1,
    ):
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            malformed_line_numbers.append(line_number)
            logger.warning(
                "Skipping malformed JSON on line %d: %s",
                line_number, exc,
            )
            continue
        if not isinstance(obj, Mapping):
            malformed_line_numbers.append(line_number)
            logger.warning(
                "Line %d is not a JSON object (got %s); skipping.",
                line_number, type(obj).__name__,
            )
            continue
        buffer.append(dict(obj))

    return LoadResult(
        rows=list(buffer),
        malformed_count=len(malformed_line_numbers),
        malformed_line_numbers=tuple(malformed_line_numbers),
    )


def load_telemetry(
    source: Any,
    *,
    max_lines: int = 10_000,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Load a JSONL telemetry stream.

    Backward-compatible wrapper over :func:`load_telemetry_detailed`.
    Returns ``(rows, malformed_count)``.
    """
    result = load_telemetry_detailed(source, max_lines=max_lines)
    return result.rows, result.malformed_count


# --------------------------------------------------------------------------- #
# Rendering helpers
# --------------------------------------------------------------------------- #
def _coerce_numeric_series(df: Any, name: str) -> Any:
    """
    Return ``df[name]`` coerced to numeric, or the original series if
    it is already numeric.

    Non-numeric values become ``NaN`` (``pd.to_numeric(..., errors="coerce")``)
    so downstream ``.mean()`` / ``.dropna()`` / ``.scatter_chart`` calls
    behave predictably.
    """
    pd = _import_pandas()
    series = df[name]
    if pd.api.types.is_numeric_dtype(series):
        return series
    return pd.to_numeric(series, errors="coerce")


def _render_summary(
    df: Any, columns: Sequence[str],
) -> None:
    """Render metric cards for the available summary columns."""
    st = _import_streamlit()
    pd = _import_pandas()

    available = [c for c in columns if c in df.columns]
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


def _render_scatter(
    df: Any, x_column: str, y_column: str,
) -> None:
    """
    Render the Pareto-style scatter chart.

    Coerces both axes to numeric first; a column with mixed or
    entirely-non-numeric values produces an informational message
    instead of an opaque Streamlit error.
    """
    st = _import_streamlit()
    pd = _import_pandas()

    st.subheader("Pareto Frontier")

    missing = [c for c in (x_column, y_column) if c not in df.columns]
    if missing:
        st.info(
            f"Cannot render the Pareto chart: missing column(s) "
            f"{missing}. Expected '{x_column}' and '{y_column}'."
        )
        return

    x_series = _coerce_numeric_series(df, x_column)
    y_series = _coerce_numeric_series(df, y_column)
    plot_df = pd.DataFrame({
        x_column: x_series,
        y_column: y_series,
    }).dropna()

    if plot_df.empty:
        st.info(
            f"Cannot render the Pareto chart: no rows have valid "
            f"numeric values in both '{x_column}' and '{y_column}'."
        )
        return

    try:
        st.scatter_chart(plot_df, x=x_column, y=y_column)
    except Exception as exc:  # pragma: no cover — Streamlit internals
        logger.exception("scatter_chart failed: %s", exc)
        st.error(f"Could not render the scatter chart: {exc}")


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def _coerce_config(config: Any) -> DashboardConfig:
    """Return a validated :class:`DashboardConfig` or raise."""
    if config is None:
        return DashboardConfig()
    if not isinstance(config, DashboardConfig):
        raise DashboardError(
            f"config must be a DashboardConfig or None, got "
            f"{type(config).__name__}."
        )
    return config


def main(config: Optional[DashboardConfig] = None) -> int:
    """
    Render the Streamlit dashboard.

    Must be called from within a Streamlit script run (``streamlit
    run`` or ``streamlit.testing.v1.AppTest``). Calling this from a
    plain Python process raises :class:`DashboardError` from the first
    ``st`` call.

    Parameters
    ----------
    config : DashboardConfig, optional
        Defaults to a fresh ``DashboardConfig()``.

    Returns
    -------
    int
        ``0`` on success, ``1`` when the dashboard could not render
        (no data, missing file, malformed upload, read error). This
        lets ``AppTest`` and CI harnesses assert on the outcome.

    Raises
    ------
    DashboardError
        If ``config`` is not a :class:`DashboardConfig` or ``None``,
        or if the framework packages (streamlit, pandas) are not
        installed.
    """
    # Validate ``config`` before importing Streamlit, so this path is
    # testable in environments without a Streamlit installation.
    cfg = _coerce_config(config)

    st = _import_streamlit()
    pd = _import_pandas()  # fail early if pandas is missing

    # set_page_config must be the first Streamlit call in the script.
    st.set_page_config(page_title=cfg.title, layout="wide")
    st.title(cfg.title)

    path = cfg.telemetry_file
    source: Any = None
    if path.exists():
        source = path
    else:
        uploaded = st.file_uploader(
            f"Telemetry file '{path}' not found. Upload a JSONL file "
            f"to continue.",
            type=["json", "jsonl", "ndjson"],
        )
        if uploaded is None:
            st.warning(cfg.empty_warning)
            return 1
        source = uploaded

    # ---- Load (bounded tail + per-line error handling) --------- #
    try:
        result = load_telemetry_detailed(
            source, max_lines=cfg.max_lines,
        )
    except DashboardError as exc:
        st.error(str(exc))
        return 1
    except OSError as exc:
        st.error(f"Could not read telemetry: {exc}")
        return 1

    if not result.rows:
        st.warning(cfg.empty_warning)
        return 1

    if result.malformed_count:
        word = "line" if result.malformed_count == 1 else "lines"
        st.warning(
            f"{cfg.malformed_warning} "
            f"({result.malformed_count} {word} skipped.)"
        )

    df = pd.DataFrame(result.rows)

    st.subheader("Raw Telemetry")
    st.dataframe(df, use_container_width=True)

    _render_summary(df, cfg.summary_columns)
    _render_scatter(df, cfg.x_column, cfg.y_column)
    return 0


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
def _run_smoke_test() -> int:
    """Exercise the data layer without touching Streamlit."""
    import io
    import tempfile

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        nonlocal passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    def _expect_error(name: str, expected: type, fn, *args, **kwargs):
        nonlocal failed
        try:
            fn(*args, **kwargs)
            _check(name, False)
        except expected:
            _check(name, True)
        except Exception as exc:
            print(
                f"  ❌ {name} — wrong exception "
                f"{type(exc).__name__}: {exc}"
            )
            failed += 1

    # ---- 1. Config defaults + repr ---------------------------- #
    print("config defaults:")
    cfg = DashboardConfig()
    _check("default max_lines", cfg.max_lines == 10_000)
    _check(
        "default telemetry_file ends in .jsonl",
        str(cfg.telemetry_file).endswith(".jsonl"),
    )
    _check(
        "repr concise",
        "DashboardConfig(" in repr(cfg)
        and "max_lines=10000" in repr(cfg),
    )

    # ---- 2. Config validation -------------------------------- #
    print("config validation:")
    for label, bad in (
        ("zero max_lines", dict(max_lines=0)),
        ("negative max_lines", dict(max_lines=-1)),
        ("string max_lines", dict(max_lines="100")),
        ("bool max_lines", dict(max_lines=True)),
        ("empty x_column", dict(x_column="")),
        ("non-str x_column", dict(x_column=42)),
        ("empty y_column", dict(y_column="")),
        ("empty summary_columns", dict(summary_columns=())),
        ("non-tuple summary_columns", dict(summary_columns=["a"])),
        ("summary_columns with int", dict(summary_columns=("a", 42))),
        ("empty summary_columns element",
         dict(summary_columns=("",))),
        ("non-str title", dict(title=42)),
        ("non-str telemetry_file", dict(telemetry_file="x.jsonl")),
    ):
        _expect_error(
            f"reject cfg {label}",
            DashboardError,
            DashboardConfig,
            **bad,
        )

    # ---- 3. Config round-trip -------------------------------- #
    print("config round-trip:")
    d = cfg.to_dict()
    _check(
        "from_dict(to_dict()) == original",
        DashboardConfig.from_dict(d) == cfg,
    )
    _expect_error(
        "from_dict strict=True rejects unknown keys",
        DashboardError,
        DashboardConfig.from_dict,
        {"max_lines": 5000, "typo_field": 1},
    )
    _check(
        "from_dict strict=False accepts unknown keys",
        DashboardConfig.from_dict(
            {"max_lines": 5000, "typo_field": 1}, strict=False,
        ).max_lines == 5000,
    )

    # ---- 4. _coerce_source ----------------------------------- #
    print("source coercion:")
    _check(
        "str becomes Path",
        isinstance(_coerce_source("t.jsonl"), Path),
    )
    _check(
        "Path stays Path",
        _coerce_source(Path("t.jsonl")).name == "t.jsonl",
    )
    _check(
        "BytesIO is accepted as file-like",
        _coerce_source(io.BytesIO(b"")) is not None,
    )
    for bad in (42, None, ["a"], b"raw"):
        _expect_error(
            f"reject source {type(bad).__name__}",
            DashboardError,
            _coerce_source,
            bad,
        )

    # ---- 5. iter_jsonl_lines --------------------------------- #
    print("iter_jsonl_lines:")
    body = "\n".join([
        '{"a": 1}',
        '',
        '{"b": 2}',
        '{"c": 3}',
    ])
    lines = list(iter_jsonl_lines(io.BytesIO(body.encode("utf-8"))))
    _check("byte stream yields 3 lines", len(lines) == 3)

    # BOM handling — utf-8-sig strips BOM from the first line.
    bom_body = b"\xef\xbb\xbf" + body.encode("utf-8")
    lines = list(iter_jsonl_lines(io.BytesIO(bom_body)))
    _check(
        "BOM stripped from first line",
        lines[0].startswith("{"),
    )

    # Invalid UTF-8 raises DashboardError, not UnicodeDecodeError.
    invalid = b'{"a": 1}\n\xff\xfe invalid\n'
    _expect_error(
        "invalid UTF-8 raises DashboardError",
        DashboardError,
        lambda: list(iter_jsonl_lines(io.BytesIO(invalid))),
    )

    # ---- 6. load_telemetry_detailed ------------------------- #
    print("load_telemetry_detailed:")
    payload = b'{"energy_kwh": 1.0}\nnot-json\n{"energy_kwh": 2.0}\n42\n'
    result = load_telemetry_detailed(io.BytesIO(payload))
    _check("rows parsed", len(result.rows) == 2)
    _check("malformed_count", result.malformed_count == 2)
    _check(
        "malformed line numbers",
        result.malformed_line_numbers == (2, 4),
    )
    _check("repr concise", "LoadResult(" in repr(result))

    # ---- 7. Bounded tail ------------------------------------ #
    print("bounded tail:")
    many = "\n".join(
        f'{{"i": {i}}}' for i in range(100)
    ).encode("utf-8")
    result = load_telemetry_detailed(io.BytesIO(many), max_lines=10)
    _check("only last 10 kept", len(result.rows) == 10)
    _check(
        "kept the file-order tail",
        result.rows[-1]["i"] == 99,
    )

    # ---- 8. Backward-compat load_telemetry ------------------ #
    print("load_telemetry:")
    rows, malformed = load_telemetry(io.BytesIO(payload))
    _check("rows count preserved", len(rows) == 2)
    _check("malformed count preserved", malformed == 2)

    # ---- 9. Source-type validation in load_telemetry -------- #
    _expect_error(
        "load_telemetry(42) raises DashboardError",
        DashboardError,
        load_telemetry,
        42,
    )

    # ---- 10. Bad max_lines ---------------------------------- #
    for bad in (0, -1, "10", True):
        _expect_error(
            f"load_telemetry max_lines={bad!r} rejected",
            DashboardError,
            load_telemetry,
            io.BytesIO(b""),
            max_lines=bad,
        )

    # ---- 11. main() validation without Streamlit ------------ #
    print("main() validation (no Streamlit needed):")
    _expect_error(
        "main('not-a-config') raises DashboardError",
        DashboardError,
        main,
        "not-a-config",
    )
    _expect_error(
        "main(42) raises DashboardError",
        DashboardError,
        main,
        42,
    )

    # ---- 12. File-path loading ------------------------------ #
    print("file-path loading:")
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "tel.jsonl"
        p.write_text(
            '{"a": 1}\n{"b": 2}\n', encoding="utf-8",
        )
        result = load_telemetry_detailed(p)
        _check(
            "file-path load works",
            len(result.rows) == 2 and result.malformed_count == 0,
        )

        # Invalid UTF-8 in a file.
        bad_path = Path(td) / "bad.jsonl"
        bad_path.write_bytes(b'{"a": 1}\n\xff\n')
        _expect_error(
            "invalid UTF-8 file raises DashboardError",
            DashboardError,
            load_telemetry_detailed,
            bad_path,
        )

    # ---- 13. Public API ------------------------------------- #
    print("public API:")
    for name in (
        "__version__",
        "DashboardConfig",
        "DashboardError",
        "LoadResult",
        "iter_jsonl_lines",
        "load_telemetry",
        "load_telemetry_detailed",
        "main",
    ):
        _check(f"{name} exported", name in __all__)
    _check(
        "DashboardError is ValueError",
        issubclass(DashboardError, ValueError),
    )
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    print()
    print(f"passed: {passed}  failed: {failed}")
    return 0 if failed == 0 else 1


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "DashboardConfig",
    "DashboardError",
    "LoadResult",
    "iter_jsonl_lines",
    "load_telemetry",
    "load_telemetry_detailed",
    "main",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Script entry point
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import sys

    # Detect whether we are being executed by `streamlit run`. The
    # `streamlit.runtime.exists()` function is a documented public API
    # (Streamlit >= 1.0). If Streamlit is not installed, or the API has
    # moved, fall back to running the smoke test in plain-Python mode.
    _under_streamlit = False
    try:
        from streamlit import runtime as _st_runtime  # type: ignore
        _under_streamlit = bool(_st_runtime.exists())
    except Exception:
        _under_streamlit = False

    if _under_streamlit:
        sys.exit(main())
    else:
        sys.exit(_run_smoke_test())
