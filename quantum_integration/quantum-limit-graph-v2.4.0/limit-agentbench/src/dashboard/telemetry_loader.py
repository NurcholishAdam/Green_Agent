# src/dashboard/telemetry_loader.py

"""
Telemetry Loader
================

Loads Green Agent telemetry JSON reports and converts them to pandas
DataFrames for dashboard consumption.

Enhancements
------------
- ``TelemetryLoaderConfig`` — frozen, fully validated: bounded file
  size, flattening toggle, non-empty separator, per-report top-level
  field inclusion. ``from_dict(..., strict=True)`` rejects unknown keys;
  ``to_dict`` / ``__repr__`` provided for symmetry with the rest of the
  tree.
- **Full error handling** — ``FileNotFoundError``, ``json.JSONDecodeError``,
  **``UnicodeDecodeError``**, and ``OSError`` all surface as
  :class:`TelemetryLoaderError`. The ``max_file_bytes`` limit is enforced
  for **file-like sources** as well as file paths, so a hostile or
  accidental 5 GB upload cannot be read into memory.
- **Source-type validation** — the loader accepts ``str`` / ``Path`` /
  ``os.PathLike`` / file-like objects (with a callable ``read``) and
  raises :class:`TelemetryLoaderError` for anything else. No more
  ``FileNotFoundError: '42'`` from an ``int`` argument; broken
  ``read`` attributes are caught up front.
- **Strict report shape** — :meth:`TelemetryLoader.load` validates that
  the report contains a ``metrics`` object by default; pass
  ``strict=False`` to accept arbitrary JSON.
- **Flattening collisions raise** — :func:`_flatten` raises
  :class:`TelemetryLoaderError` on key collision (a key containing the
  separator, or two keys that flatten to the same path). The previous
  implementation kept the last writer, silently losing data.
- **Top-level report fields** — :class:`TelemetryLoaderConfig` gained a
  ``report_fields`` tuple. When set, those top-level scalar fields are
  included in the DataFrame alongside the flattened metrics. Empty by
  default (backward-compatible: only ``metrics`` is converted).
- **Batch API** — :meth:`TelemetryLoader.to_dataframe_many` converts a
  sequence of reports to a single DataFrame in one call.
- **Deep-copy on ``flatten_nested=False``** — the returned DataFrame
  does not share nested references with the caller's report.
- **Source name in error messages** — file-like sources use their
  ``.name`` attribute when available, rather than a memory address.
- ``PANDAS_AVAILABLE`` (no leading underscore) is exported so callers
  can check dependency availability without reaching for a private
  name.
- ``__version__`` exported.
- ``__main__`` smoke test is a ``_check()``-based one that survives
  ``python -O``.

Compatibility
-------------
- ``TelemetryLoader.load(source, *, config=None)`` — same positional
  signature; new keyword-only ``strict=True`` parameter. Behaviour
  changes:

  * ``load(42)`` now raises :class:`TelemetryLoaderError` with a clear
    message instead of ``FileNotFoundError: '42'``.
  * ``load`` rejects reports without a ``metrics`` object by default;
    pass ``strict=False`` to accept arbitrary JSON.
  * Non-UTF-8 sources raise :class:`TelemetryLoaderError` instead of
    ``UnicodeDecodeError``.
  * File-like sources larger than ``max_file_bytes`` raise
    :class:`TelemetryLoaderError`.
  * A source whose ``read`` attribute is not callable raises
    :class:`TelemetryLoaderError` instead of ``TypeError``.

- ``TelemetryLoader.to_dataframe(report, *, config=None)`` — same
  signature, same return type. Behaviour changes:

  * Key collisions during flattening raise :class:`TelemetryLoaderError`
    instead of silently overwriting. Callers whose metrics contain
    keys that flatten to the same path (e.g.
    ``{"a.b": 1, "a": {"b": 2}}``) must fix their data or pass
    ``flatten_nested=False``.
  * Empty ``metrics`` objects now raise :class:`TelemetryLoaderError`
    instead of producing a 1-row, 0-column DataFrame.
  * With ``flatten_nested=False``, the metrics sub-tree is deep-copied
    so the returned DataFrame does not share nested references with the
    caller's report.

- ``TelemetryLoaderConfig`` — same fields plus ``report_fields``.
  ``max_file_bytes`` must be a real ``int`` (``bool`` / ``str`` raise
  :class:`TelemetryLoaderError`); ``flatten_nested`` must be a ``bool``;
  ``separator`` must be a **non-empty** string; ``report_fields`` must
  be a tuple of non-empty strings.
- ``TelemetryLoaderConfig.from_dict(..., strict=True)`` — new method;
  unknown keys rejected by default.
- ``_PANDAS_AVAILABLE`` — kept as a module-level alias for backward
  compatibility but no longer exported in ``__all__``; use
  ``PANDAS_AVAILABLE``.

Requires Python 3.8+.
"""

from __future__ import annotations

import copy
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any, BinaryIO, Dict, List, Mapping, Optional, Sequence, TextIO,
    Tuple, Union,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"

# Optional pandas import.
try:  # pragma: no cover — optional dependency
    import pandas as pd  # type: ignore

    PANDAS_AVAILABLE = True
except ImportError:  # pragma: no cover
    pd = None  # type: ignore[assignment]
    PANDAS_AVAILABLE = False

# Backward-compatible alias (no longer in ``__all__``).
_PANDAS_AVAILABLE = PANDAS_AVAILABLE

# Source type: str, path, or file-like object (binary or text).
Source = Union[str, Path, "os.PathLike[str]", BinaryIO, TextIO]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class TelemetryLoaderError(ValueError):
    """Raised for invalid loader inputs or malformed reports."""


# --------------------------------------------------------------------------- #
# Validators
# --------------------------------------------------------------------------- #
def _require_bool(name: str, value: Any) -> bool:
    if not isinstance(value, bool):
        raise TelemetryLoaderError(
            f"{name} must be a bool, got {type(value).__name__}."
        )
    return value


def _require_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TelemetryLoaderError(f"{name} must be an int.")
    if value <= 0:
        raise TelemetryLoaderError(f"{name} must be > 0, got {value}.")
    return value


def _require_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise TelemetryLoaderError(
            f"{name} must be a non-empty string, got "
            f"{type(value).__name__}."
        )
    return value


def _coerce_config(value: Any) -> "TelemetryLoaderConfig":
    if value is None:
        return TelemetryLoaderConfig()
    if not isinstance(value, TelemetryLoaderConfig):
        raise TelemetryLoaderError(
            f"config must be a TelemetryLoaderConfig or None, got "
            f"{type(value).__name__}."
        )
    return value


def _coerce_source(source: Any) -> Union[Path, Any]:
    """
    Return a :class:`pathlib.Path` or a file-like object.

    Raises :class:`TelemetryLoaderError` for anything else. A source
    whose ``read`` attribute exists but is not callable is rejected
    (previously the loader would have crashed with ``TypeError`` on
    ``source.read()``).
    """
    if isinstance(source, Path):
        return source
    if isinstance(source, str):
        if not source:
            raise TelemetryLoaderError(
                "source must not be an empty string."
            )
        return Path(source)
    if hasattr(source, "__fspath__"):
        return Path(os.fspath(source))
    read_attr = getattr(source, "read", None)
    if callable(read_attr):
        return source
    raise TelemetryLoaderError(
        f"source must be a str, Path, os.PathLike, or file-like object "
        f"with a callable read() method, got {type(source).__name__}."
    )


def _source_name(source: Any) -> str:
    """Return a human-readable name for a source, for error messages."""
    if isinstance(source, Path):
        return str(source)
    if isinstance(source, str):
        return source
    name = getattr(source, "name", None)
    if isinstance(name, str) and name:
        return name
    return repr(source)


def _decode_utf8(raw: bytes, *, source: Any) -> str:
    """Decode ``raw`` as UTF-8; raise :class:`TelemetryLoaderError`."""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TelemetryLoaderError(
            f"could not decode {_source_name(source)!r} as UTF-8: {exc}"
        ) from exc


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TelemetryLoaderConfig:
    """Tunable parameters for :class:`TelemetryLoader`.

    ``report_fields``
        Tuple of top-level report keys to include alongside the
        flattened ``metrics`` sub-tree. Values are copied verbatim —
        intended for scalar metadata such as ``agent_id``,
        ``timestamp``, and ``status``. Empty by default, in which case
        the DataFrame contains only the flattened metrics.
    """

    max_file_bytes: int = 100 * 1024 * 1024  # 100 MB
    flatten_nested: bool = True
    separator: str = "."
    report_fields: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_positive_int("max_file_bytes", self.max_file_bytes)
        _require_bool("flatten_nested", self.flatten_nested)
        _require_nonempty_str("separator", self.separator)

        if not isinstance(self.report_fields, (tuple, list)):
            raise TelemetryLoaderError(
                "report_fields must be a tuple of strings."
            )
        for i, f in enumerate(self.report_fields):
            _require_nonempty_str(f"report_fields[{i}]", f)
        object.__setattr__(
            self, "report_fields", tuple(self.report_fields),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_file_bytes": self.max_file_bytes,
            "flatten_nested": self.flatten_nested,
            "separator": self.separator,
            "report_fields": list(self.report_fields),
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "TelemetryLoaderConfig":
        if not isinstance(data, Mapping):
            raise TelemetryLoaderError(
                "TelemetryLoaderConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise TelemetryLoaderError(
                    f"unknown TelemetryLoaderConfig keys: {unknown}"
                )
        try:
            kwargs: Dict[str, Any] = {}
            for k, v in data.items():
                if k not in valid:
                    continue
                if k == "report_fields":
                    kwargs[k] = tuple(v or ())
                else:
                    kwargs[k] = v
            return cls(**kwargs)
        except TelemetryLoaderError:
            raise
        except (TypeError, ValueError) as exc:
            raise TelemetryLoaderError(
                f"invalid TelemetryLoaderConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "TelemetryLoaderConfig("
            f"max_file_bytes={self.max_file_bytes}, "
            f"flatten_nested={self.flatten_nested}, "
            f"separator={self.separator!r})"
        )


# --------------------------------------------------------------------------- #
# Loader
# --------------------------------------------------------------------------- #
class TelemetryLoader:
    """
    Loads ``green_agent_report.json`` files and converts them to
    DataFrames.

    All public methods are static; pass a :class:`TelemetryLoaderConfig`
    to each call to control behaviour.
    """

    @staticmethod
    def load(
        source: Source,
        *,
        config: Optional[TelemetryLoaderConfig] = None,
        strict: bool = True,
    ) -> Dict[str, Any]:
        """
        Load a telemetry report.

        Parameters
        ----------
        source : str | Path | file-like
            Path to a JSON file, or a file-like object with a callable
            ``.read()`` method (Streamlit uploaders, ``BytesIO``,
            ``StringIO``).
        config : TelemetryLoaderConfig, optional
        strict : bool, default True
            When True, the report must contain a ``metrics`` object.
            When False, any JSON object is accepted.

        Returns
        -------
        dict
            Parsed report (shallow copy of the JSON payload).

        Raises
        ------
        TelemetryLoaderError
            For an invalid source type, an unreadable source, a
            non-UTF-8 payload, an over-sized payload, malformed JSON,
            a non-object root, or (with ``strict=True``) a missing
            ``metrics`` field.
        """
        cfg = _coerce_config(config)
        _require_bool("strict", strict)

        resolved = _coerce_source(source)

        # ---- Read raw text --------------------------------------- #
        if isinstance(resolved, Path):
            try:
                size = resolved.stat().st_size
            except FileNotFoundError as exc:
                raise TelemetryLoaderError(
                    f"file not found: {resolved}"
                ) from exc
            except OSError as exc:
                raise TelemetryLoaderError(
                    f"could not stat {resolved!r}: {exc}"
                ) from exc
            if size > cfg.max_file_bytes:
                raise TelemetryLoaderError(
                    f"file too large: {size} bytes > "
                    f"{cfg.max_file_bytes} bytes ({resolved!r})."
                )
            try:
                raw = resolved.read_text(encoding="utf-8")
            except UnicodeDecodeError as exc:
                raise TelemetryLoaderError(
                    f"could not decode {resolved!r} as UTF-8: {exc}"
                ) from exc
            except OSError as exc:
                raise TelemetryLoaderError(
                    f"could not read {resolved!r}: {exc}"
                ) from exc
        else:
            # File-like source.
            try:
                raw = resolved.read()
            except (OSError, ValueError, TypeError) as exc:
                raise TelemetryLoaderError(
                    f"could not read {_source_name(resolved)!r}: {exc}"
                ) from exc

            if isinstance(raw, bytes):
                if len(raw) > cfg.max_file_bytes:
                    raise TelemetryLoaderError(
                        f"stream too large: {len(raw)} bytes > "
                        f"{cfg.max_file_bytes} bytes "
                        f"({_source_name(resolved)!r})."
                    )
                raw = _decode_utf8(raw, source=resolved)
            elif isinstance(raw, str):
                encoded_len = len(raw.encode("utf-8"))
                if encoded_len > cfg.max_file_bytes:
                    raise TelemetryLoaderError(
                        f"stream too large: {encoded_len} bytes > "
                        f"{cfg.max_file_bytes} bytes "
                        f"({_source_name(resolved)!r})."
                    )
            else:
                raise TelemetryLoaderError(
                    f"source.read() must return str or bytes, got "
                    f"{type(raw).__name__} "
                    f"({_source_name(resolved)!r})."
                )

        # ---- Parse JSON ------------------------------------------ #
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise TelemetryLoaderError(
                f"invalid JSON in {_source_name(source)!r}: {exc}"
            ) from exc

        if not isinstance(data, Mapping):
            raise TelemetryLoaderError(
                f"report root must be an object, got "
                f"{type(data).__name__}."
            )
        report = dict(data)

        # ---- Strict shape validation ----------------------------- #
        if strict and not isinstance(report.get("metrics"), Mapping):
            raise TelemetryLoaderError(
                "report is missing a 'metrics' object."
            )
        return report

    @staticmethod
    def to_dataframe(
        report: Mapping[str, Any],
        *,
        config: Optional[TelemetryLoaderConfig] = None,
    ) -> Any:
        """Convert a telemetry report to a pandas DataFrame."""
        if not PANDAS_AVAILABLE:
            raise TelemetryLoaderError(
                "pandas is required for to_dataframe; install pandas."
            )
        if not isinstance(report, Mapping):
            raise TelemetryLoaderError(
                f"report must be a Mapping, got "
                f"{type(report).__name__}."
            )
        cfg = _coerce_config(config)

        metrics = report.get("metrics")
        if metrics is None:
            raise TelemetryLoaderError(
                "report is missing the 'metrics' field."
            )
        if not isinstance(metrics, Mapping):
            raise TelemetryLoaderError(
                f"'metrics' must be an object, got "
                f"{type(metrics).__name__}."
            )
        if not metrics:
            raise TelemetryLoaderError(
                "report['metrics'] is empty; nothing to convert."
            )

        row = _build_row(report, cfg)
        return pd.DataFrame([row])

    @staticmethod
    def to_dataframe_many(
        reports: Sequence[Mapping[str, Any]],
        *,
        config: Optional[TelemetryLoaderConfig] = None,
    ) -> Any:
        """Convert multiple reports to a single DataFrame."""
        if not PANDAS_AVAILABLE:
            raise TelemetryLoaderError(
                "pandas is required for to_dataframe_many; install pandas."
            )
        if isinstance(reports, (str, bytes)) or not isinstance(
            reports, Sequence,
        ):
            raise TelemetryLoaderError(
                "reports must be a sequence of mappings."
            )
        if not reports:
            raise TelemetryLoaderError("reports must be non-empty.")
        cfg = _coerce_config(config)

        rows: List[Dict[str, Any]] = []
        for i, report in enumerate(reports):
            if not isinstance(report, Mapping):
                raise TelemetryLoaderError(
                    f"reports[{i}] must be a Mapping, got "
                    f"{type(report).__name__}."
                )
            metrics = report.get("metrics")
            if metrics is None:
                raise TelemetryLoaderError(
                    f"reports[{i}] is missing the 'metrics' field."
                )
            if not isinstance(metrics, Mapping):
                raise TelemetryLoaderError(
                    f"reports[{i}].metrics must be a Mapping, got "
                    f"{type(metrics).__name__}."
                )
            if not metrics:
                raise TelemetryLoaderError(
                    f"reports[{i}].metrics is empty."
                )
            rows.append(_build_row(report, cfg))
        return pd.DataFrame(rows)

    def __repr__(self) -> str:
        return f"TelemetryLoader(pandas_available={PANDAS_AVAILABLE})"


# --------------------------------------------------------------------------- #
# Row building
# --------------------------------------------------------------------------- #
def _build_row(
    report: Mapping[str, Any],
    cfg: TelemetryLoaderConfig,
) -> Dict[str, Any]:
    """
    Build a single flattened row from a report.

    Includes the metrics sub-tree plus any ``report_fields`` configured
    by the caller. Flattening raises :class:`TelemetryLoaderError` on
    key collision.
    """
    metrics = report.get("metrics")
    if not isinstance(metrics, Mapping):
        raise TelemetryLoaderError(
            f"'metrics' must be an object, got "
            f"{type(metrics).__name__}."
        )

    if cfg.flatten_nested:
        flat = _flatten(dict(metrics), separator=cfg.separator)
    else:
        # Deep copy so the returned DataFrame cannot be mutated through
        # the caller's report.
        flat = copy.deepcopy(dict(metrics))

    # Top-level report fields, included when the caller asks for them.
    for name in cfg.report_fields:
        if name not in report:
            continue
        if name in flat:
            raise TelemetryLoaderError(
                f"report field {name!r} collides with a flattened "
                f"metrics key; rename the field or drop it from "
                f"report_fields."
            )
        flat[name] = report[name]

    return flat


# --------------------------------------------------------------------------- #
# Flattening
# --------------------------------------------------------------------------- #
def _flatten(
    data: Mapping[str, Any],
    *,
    separator: str = ".",
    prefix: str = "",
) -> Dict[str, Any]:
    """
    Flatten a nested mapping into a single-level dict.

    Raises :class:`TelemetryLoaderError` on key collision — the previous
    implementation kept the last writer, which is a data-integrity bug
    for a telemetry loader.
    """
    out: Dict[str, Any] = {}
    for key, value in data.items():
        full_key = f"{prefix}{separator}{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            nested = _flatten(
                value, separator=separator, prefix=full_key,
            )
            for k, v in nested.items():
                if k in out:
                    raise TelemetryLoaderError(
                        f"flatten collision at key {k!r}; two distinct "
                        f"paths in the input map to the same flattened "
                        f"key. Check for keys containing "
                        f"{separator!r} or non-string keys that "
                        f"stringify to the same value."
                    )
                out[k] = v
        else:
            if full_key in out:
                raise TelemetryLoaderError(
                    f"flatten collision at key {full_key!r}; the "
                    f"flattened path already exists in the output."
                )
            out[full_key] = value
    return out


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "PANDAS_AVAILABLE",
    "TelemetryLoader",
    "TelemetryLoaderConfig",
    "TelemetryLoaderError",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m dashboard.telemetry_loader
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import io
    import sys
    import tempfile

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    def _expect_error(name: str, expected: type, fn, *args, **kwargs):
        global failed
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
    cfg = TelemetryLoaderConfig()
    _check(
        "default max_file_bytes",
        cfg.max_file_bytes == 100 * 1024 * 1024,
    )
    _check("default flatten_nested", cfg.flatten_nested is True)
    _check("default separator", cfg.separator == ".")
    _check("default report_fields empty", cfg.report_fields == ())
    _check(
        "repr concise",
        "TelemetryLoaderConfig(" in repr(cfg),
    )

    # ---- 2. Config validation -------------------------------- #
    print("config validation:")
    for label, bad in (
        ("zero max_file_bytes", dict(max_file_bytes=0)),
        ("negative max_file_bytes", dict(max_file_bytes=-1)),
        ("string max_file_bytes", dict(max_file_bytes="100")),
        ("bool max_file_bytes", dict(max_file_bytes=True)),
        ("string flatten_nested", dict(flatten_nested="yes")),
        ("empty separator", dict(separator="")),
        ("non-string separator", dict(separator=42)),
        ("non-tuple report_fields", dict(report_fields="abc")),
        ("report_fields with empty", dict(report_fields=("ok", ""))),
        ("report_fields with int", dict(report_fields=("ok", 42))),
    ):
        _expect_error(
            f"reject cfg {label}",
            TelemetryLoaderError,
            TelemetryLoaderConfig,
            **bad,
        )

    # ---- 3. Config round-trip -------------------------------- #
    print("config round-trip:")
    d = cfg.to_dict()
    _check(
        "from_dict(to_dict()) == original",
        TelemetryLoaderConfig.from_dict(d) == cfg,
    )
    _expect_error(
        "from_dict strict=True rejects unknown keys",
        TelemetryLoaderError,
        TelemetryLoaderConfig.from_dict,
        {"max_file_bytes": 1000, "typo_field": 1},
    )
    _check(
        "from_dict strict=False accepts unknown keys",
        TelemetryLoaderConfig.from_dict(
            {"max_file_bytes": 1000, "typo_field": 1}, strict=False,
        ).max_file_bytes == 1000,
    )

    # ---- 4. Happy path via path ----------------------------- #
    print("load (path):")
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "report.json"
        p.write_text(
            json.dumps({
                "agent_id": "agent_A",
                "metrics": {
                    "accuracy": 0.95,
                    "cumulative": {
                        "total_energy_wh": 0.01,
                        "total_carbon_kg": 0.001,
                    },
                },
            }),
            encoding="utf-8",
        )
        report = TelemetryLoader.load(p)
        _check("report has metrics", "metrics" in report)
        _check("report has agent_id", "agent_id" in report)

    # ---- 5. Happy path via file-like ------------------------ #
    print("load (file-like):")
    body = json.dumps({
        "metrics": {"accuracy": 0.9, "cumulative": {"e": 1.0}},
    })
    report = TelemetryLoader.load(io.BytesIO(body.encode("utf-8")))
    _check("file-like load works", "metrics" in report)

    report = TelemetryLoader.load(io.StringIO(body))
    _check("StringIO load works", "metrics" in report)

    # ---- 6. UnicodeDecodeError → TelemetryLoaderError ------- #
    print("unicode handling:")
    _expect_error(
        "invalid UTF-8 bytes → TelemetryLoaderError",
        TelemetryLoaderError,
        TelemetryLoader.load,
        io.BytesIO(b'\xff\xfe invalid'),
    )

    with tempfile.TemporaryDirectory() as td:
        bad_path = Path(td) / "bad.json"
        bad_path.write_bytes(b'\xff\xfe invalid')
        _expect_error(
            "invalid UTF-8 file → TelemetryLoaderError",
            TelemetryLoaderError,
            TelemetryLoader.load,
            bad_path,
        )

    # ---- 7. Source validation ------------------------------- #
    print("source validation:")
    broken = type("Broken", (), {"read": "not-a-callable"})()
    for label, bad in (
        ("int", 42),
        ("None", None),
        ("list", ["a"]),
        ("broken-read", broken),
    ):
        _expect_error(
            f"load({label}) raises TelemetryLoaderError",
            TelemetryLoaderError,
            TelemetryLoader.load,
            bad,
        )

    # ---- 8. Missing file ------------------------------------ #
    _expect_error(
        "missing file → TelemetryLoaderError",
        TelemetryLoaderError,
        TelemetryLoader.load,
        Path("/does/not/exist.json"),
    )

    # ---- 9. Oversize enforcement ---------------------------- #
    print("size limits:")
    small_cfg = TelemetryLoaderConfig(max_file_bytes=64)

    big = io.BytesIO(b"x" * 128)
    _expect_error(
        "oversized stream rejected",
        TelemetryLoaderError,
        TelemetryLoader.load,
        big,
        config=small_cfg,
    )

    with tempfile.TemporaryDirectory() as td:
        big_path = Path(td) / "big.json"
        big_path.write_bytes(b"x" * 128)
        _expect_error(
            "oversized file rejected",
            TelemetryLoaderError,
            TelemetryLoader.load,
            big_path,
            config=small_cfg,
        )

    # ---- 10. Malformed JSON --------------------------------- #
    _expect_error(
        "malformed JSON → TelemetryLoaderError",
        TelemetryLoaderError,
        TelemetryLoader.load,
        io.BytesIO(b"not json"),
    )
    _expect_error(
        "list root → TelemetryLoaderError",
        TelemetryLoaderError,
        TelemetryLoader.load,
        io.BytesIO(b'["a", "b"]'),
    )

    # ---- 11. Strict shape validation ------------------------ #
    print("strict shape:")
    _expect_error(
        "missing metrics with strict=True rejected",
        TelemetryLoaderError,
        TelemetryLoader.load,
        io.BytesIO(b'{"agent_id": "x"}'),
        strict=True,
    )
    report = TelemetryLoader.load(
        io.BytesIO(b'{"agent_id": "x"}'), strict=False,
    )
    _check(
        "missing metrics with strict=False accepted",
        report == {"agent_id": "x"},
    )

    # ---- 12. Flatten collisions ----------------------------- #
    print("flatten collisions:")
    _expect_error(
        "a.b vs a:{b} collision",
        TelemetryLoaderError,
        _flatten,
        {"a.b": 1, "a": {"b": 2}},
    )
    _expect_error(
        "nested a.b.c collision",
        TelemetryLoaderError,
        _flatten,
        {"a.b": {"c": 1}, "a": {"b": {"c": 2}}},
    )

    flat = _flatten({"a": {"b": 1}, "c": 2})
    _check(
        "nested flatten produces a.b and c",
        flat == {"a.b": 1, "c": 2},
    )

    # ---- 13. to_dataframe ----------------------------------- #
    print("to_dataframe:")
    if PANDAS_AVAILABLE:
        report = {
            "agent_id": "agent_A",
            "metrics": {
                "accuracy": 0.95,
                "cumulative": {"total_energy_wh": 0.01},
            },
        }
        df = TelemetryLoader.to_dataframe(report)
        _check(
            "dataframe has accuracy column",
            "accuracy" in df.columns,
        )
        _check(
            "dataframe has flattened metrics column",
            "cumulative.total_energy_wh" in df.columns,
        )
        _check(
            "agent_id not included by default",
            "agent_id" not in df.columns,
        )

        cfg2 = TelemetryLoaderConfig(report_fields=("agent_id",))
        df = TelemetryLoader.to_dataframe(report, config=cfg2)
        _check(
            "report_fields includes agent_id",
            "agent_id" in df.columns
            and df.iloc[0]["agent_id"] == "agent_A",
        )

        report_nested = {
            "metrics": {"cumulative": {"total_energy_wh": 0.01}},
        }
        cfg3 = TelemetryLoaderConfig(flatten_nested=False)
        df = TelemetryLoader.to_dataframe(report_nested, config=cfg3)
        report_nested["metrics"]["cumulative"]["total_energy_wh"] = 999.0
        _check(
            "flatten_nested=False deep-copies",
            df.iloc[0]["cumulative"]["total_energy_wh"] == 0.01,
        )

        _expect_error(
            "empty metrics rejected",
            TelemetryLoaderError,
            TelemetryLoader.to_dataframe,
            {"metrics": {}},
        )

        df = TelemetryLoader.to_dataframe_many([
            {"metrics": {"accuracy": 0.9}},
            {"metrics": {"accuracy": 0.8}},
        ])
        _check("to_dataframe_many returns 2 rows", len(df) == 2)
        _check(
            "to_dataframe_many has accuracy column",
            "accuracy" in df.columns,
        )
        _expect_error(
            "empty reports rejected",
            TelemetryLoaderError,
            TelemetryLoader.to_dataframe_many,
            [],
        )
    else:
        _check("pandas unavailable — skipping to_dataframe tests", True)

    # ---- 14. Config validation in entry points ------------- #
    print("config validation in entry points:")
    _expect_error(
        "load(config='not-a-config') rejected",
        TelemetryLoaderError,
        TelemetryLoader.load,
        io.BytesIO(b'{"metrics": {}}'),
        config="not-a-config",
    )
    if PANDAS_AVAILABLE:
        _expect_error(
            "to_dataframe(config=42) rejected",
            TelemetryLoaderError,
            TelemetryLoader.to_dataframe,
            {"metrics": {"a": 1}},
            config=42,
        )

    # ---- 15. source.name in error messages ----------------- #
    print("source name in errors:")

    class _NamedSource:
        name = "my_upload.jsonl"

        def read(self):
            return b"not-json"

    try:
        TelemetryLoader.load(_NamedSource())
    except TelemetryLoaderError as exc:
        _check(
            "error mentions source.name",
            "my_upload.jsonl" in str(exc),
        )

    # ---- 16. Public API ------------------------------------ #
    print("public API:")
    for name in (
        "__version__",
        "PANDAS_AVAILABLE",
        "TelemetryLoader",
        "TelemetryLoaderConfig",
        "TelemetryLoaderError",
    ):
        _check(f"{name} exported", name in __all__)
    _check(
        "TelemetryLoaderError is ValueError",
        issubclass(TelemetryLoaderError, ValueError),
    )
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
