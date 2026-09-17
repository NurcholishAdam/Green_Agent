# src/dashboard/telemetry_loader.py

"""
Telemetry Loader
================

Loads Green Agent telemetry JSON reports and converts them to pandas
DataFrames for dashboard consumption.

Enhancements
------------
- ``TelemetryLoaderConfig`` — frozen, validated.
- **Full error handling** — ``FileNotFoundError`` / ``json.JSONDecodeError``
  / missing ``metrics`` key all surface as ``TelemetryLoaderError``.
- **Accepts file paths, file-like objects, or dicts** — the loader is now
  usable from both Streamlit and tests without touching the filesystem.
- **Flattening helper** — ``to_dataframe`` flattens nested metrics so
  columns are usable directly.
- ``__repr__`` and a ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union

logger = logging.getLogger(__name__)

# Optional pandas import.
try:  # pragma: no cover — optional dependency
    import pandas as pd  # type: ignore

    _PANDAS_AVAILABLE = True
except ImportError:  # pragma: no cover
    pd = None  # type: ignore[assignment]
    _PANDAS_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class TelemetryLoaderError(ValueError):
    """Raised for invalid loader inputs or malformed reports."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TelemetryLoaderConfig:
    """Tunable parameters for :class:`TelemetryLoader`."""

    max_file_bytes: int = 100 * 1024 * 1024  # 100 MB
    flatten_nested: bool = True
    separator: str = "."

    def __post_init__(self) -> None:
        if self.max_file_bytes <= 0:
            raise TelemetryLoaderError("max_file_bytes must be > 0.")
        if not isinstance(self.separator, str):
            raise TelemetryLoaderError("separator must be a string.")


# --------------------------------------------------------------------------- #
# Loader
# --------------------------------------------------------------------------- #
class TelemetryLoader:
    """
    Loads ``green_agent_report.json`` files and converts them to DataFrames.

    All original public methods are preserved; new parameters are keyword-only.
    """

    @staticmethod
    def load(
        source: Union[str, Path, Any],
        *,
        config: Optional[TelemetryLoaderConfig] = None,
    ) -> Dict[str, Any]:
        """
        Load a telemetry report.

        Parameters
        ----------
        source : str | Path | file-like
            Path to a JSON file or a file-like object with a ``.read()``
            method (as returned by Streamlit's ``st.file_uploader``).
        config : TelemetryLoaderConfig, optional
            Loader configuration.

        Returns
        -------
        dict
            Parsed report.
        """
        cfg = config or TelemetryLoaderConfig()

        # ---- Resolve the raw content ---------------------------------
        try:
            if hasattr(source, "read"):
                raw = source.read()
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
            else:
                path = Path(str(source))
                if not path.exists():
                    raise TelemetryLoaderError(f"file not found: {path}")
                size = path.stat().st_size
                if size > cfg.max_file_bytes:
                    raise TelemetryLoaderError(
                        f"file too large: {size} > {cfg.max_file_bytes}"
                    )
                raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise TelemetryLoaderError(
                f"could not read {source!r}: {exc}"
            ) from exc

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise TelemetryLoaderError(
                f"invalid JSON in {source!r}: {exc}"
            ) from exc

        if not isinstance(data, Mapping):
            raise TelemetryLoaderError(
                f"report root must be an object, got {type(data).__name__}"
            )
        return dict(data)

    @staticmethod
    def to_dataframe(
        report: Mapping[str, Any],
        *,
        config: Optional[TelemetryLoaderConfig] = None,
    ) -> Any:
        """Convert a telemetry report to a pandas DataFrame."""
        if not _PANDAS_AVAILABLE:
            raise TelemetryLoaderError(
                "pandas is required for to_dataframe; install pandas."
            )
        if not isinstance(report, Mapping):
            raise TelemetryLoaderError("report must be a Mapping.")

        cfg = config or TelemetryLoaderConfig()
        metrics = report.get("metrics")
        if metrics is None:
            raise TelemetryLoaderError(
                "report is missing the 'metrics' field."
            )
        if not isinstance(metrics, Mapping):
            raise TelemetryLoaderError(
                f"'metrics' must be an object, got {type(metrics).__name__}"
            )

        if cfg.flatten_nested:
            flat = _flatten(dict(metrics), separator=cfg.separator)
        else:
            flat = dict(metrics)

        return pd.DataFrame([flat])

    def __repr__(self) -> str:
        return (
            f"TelemetryLoader(pandas_available={_PANDAS_AVAILABLE})"
        )


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _flatten(
    data: Mapping[str, Any], *, separator: str = ".", prefix: str = ""
) -> Dict[str, Any]:
    """Flatten a nested mapping into a single-level dict."""
    out: Dict[str, Any] = {}
    for key, value in data.items():
        full_key = f"{prefix}{separator}{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            out.update(_flatten(value, separator=separator, prefix=full_key))
        else:
            out[full_key] = value
    return out


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "TelemetryLoader",
    "TelemetryLoaderConfig",
    "TelemetryLoaderError",
    "_PANDAS_AVAILABLE",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m dashboard.telemetry_loader
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import tempfile

    logging.basicConfig(level=logging.INFO)
    loader = TelemetryLoader()
    print("repr       :", loader)

    # ---- Happy path via path ------------------------------------- #
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "report.json"
        path.write_text(json.dumps({
            "metrics": {
                "accuracy": 0.95,
                "cumulative": {"total_energy_wh": 0.01, "total_carbon_kg": 0.001},
            }
        }))
        report = TelemetryLoader.load(path)
        print("loaded     :", list(report.keys()))
        if _PANDAS_AVAILABLE:
            df = TelemetryLoader.to_dataframe(report)
            print("dataframe  :", list(df.columns))

    # ---- Bug fix: error handling --------------------------------- #
    for bad in ("/does/not/exist.json", Path("/does/not/exist.json")):
        try:
            TelemetryLoader.load(bad)
        except TelemetryLoaderError as exc:
            print("Rejected   :", exc)

    try:
        TelemetryLoader.load(type("F", (), {"read": lambda self: "not json"})())
    except TelemetryLoaderError as exc:
        print("Rejected   :", exc)

    try:
        TelemetryLoader.to_dataframe({"no_metrics": True})
    except TelemetryLoaderError as exc:
        print("Rejected   :", exc)

    print("\nSmoke test passed.")
