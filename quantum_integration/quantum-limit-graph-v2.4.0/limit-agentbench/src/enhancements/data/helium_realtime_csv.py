# File: helium_realtime_csv.py
"""
Loader / validator / enhancer for the enhanced helium realtime CSV.

This module is the canonical entry point for reading
``helium_realtime.csv`` (v2 schema) and producing a validated DataFrame
ready for the distillation agents, forecast models, and dashboard.

Schema (28 columns, all persisted):
    date, row_id, global_production_tonnes, global_demand_tonnes,
    price_index, shortage_severity_0_1, supply_risk_score_0_1,
    recycling_rate_0_1, substitution_feasibility_0_1,
    cooling_load_sensitivity, geopolitical_risk_index,
    logistics_disruption_index, new_production_capacity_tonnes,
    demand_supply_ratio, helium_scarcity_impact, price_volatility,
    market_regime, circularity_potential, thermal_impact_factor,
    future_supply_potential_pct, capacity_utilization_rate,
    carbon_intensity_associated, renewable_energy_pct, esg_score,
    regulatory_risk_score, data_quality_score, source, is_forecast
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

__version__ = "1.0.0"
SCHEMA_VERSION = "1.0"

DEFAULT_CSV_PATH = Path("./helium_realtime.csv")
DEFAULT_METADATA_PATH = Path("./helium_realtime.metadata.json")

# Column order matters: it's what the writer emits and what the validator enforces.
CANONICAL_COLUMNS: Tuple[str, ...] = (
    "date",
    "row_id",
    "global_production_tonnes",
    "global_demand_tonnes",
    "price_index",
    "shortage_severity_0_1",
    "supply_risk_score_0_1",
    "recycling_rate_0_1",
    "substitution_feasibility_0_1",
    "cooling_load_sensitivity",
    "geopolitical_risk_index",
    "logistics_disruption_index",
    "new_production_capacity_tonnes",
    "demand_supply_ratio",
    "helium_scarcity_impact",
    "price_volatility",
    "market_regime",
    "circularity_potential",
    "thermal_impact_factor",
    "future_supply_potential_pct",
    "capacity_utilization_rate",
    "carbon_intensity_associated",
    "renewable_energy_pct",
    "esg_score",
    "regulatory_risk_score",
    "data_quality_score",
    "source",
    "is_forecast",
)

# Base columns from the original schema (v1). These are the required inputs
# when enhancing a legacy file.
BASE_COLUMNS: Tuple[str, ...] = (
    "date",
    "global_production_tonnes",
    "global_demand_tonnes",
    "price_index",
    "shortage_severity_0_1",
    "supply_risk_score_0_1",
    "recycling_rate_0_1",
    "substitution_feasibility_0_1",
    "cooling_load_sensitivity",
    "geopolitical_risk_index",
    "logistics_disruption_index",
    "new_production_capacity_tonnes",
)

VALID_REGIMES: Tuple[str, ...] = ("low", "moderate", "high", "critical")
VALID_SOURCES: Tuple[str, ...] = (
    "usgs", "commodity", "news", "synthetic_v2", "import", "derived",
)

# Bounds enforced by the validator: (min, max). None means unbounded.
NUMERIC_BOUNDS: Dict[str, Tuple[Optional[float], Optional[float]]] = {
    "global_production_tonnes": (0, None),
    "global_demand_tonnes": (0, None),
    "price_index": (0, None),
    "shortage_severity_0_1": (0, 1),
    "supply_risk_score_0_1": (0, 1),
    "recycling_rate_0_1": (0, 1),
    "substitution_feasibility_0_1": (0, 1),
    "cooling_load_sensitivity": (0, None),
    "geopolitical_risk_index": (0, 1),
    "logistics_disruption_index": (0, 1),
    "new_production_capacity_tonnes": (0, None),
    "demand_supply_ratio": (0, None),
    "helium_scarcity_impact": (0, 1),
    "price_volatility": (0, None),
    "circularity_potential": (0, 1),
    "thermal_impact_factor": (0, None),
    "future_supply_potential_pct": (0, None),
    "capacity_utilization_rate": (0, 1),
    "carbon_intensity_associated": (0, None),
    "renewable_energy_pct": (0, 100),
    "esg_score": (0, 100),
    "regulatory_risk_score": (0, 1),
    "data_quality_score": (0, 1),
}


# ============================================================================
# Validation
# ============================================================================
@dataclass
class ValidationReport:
    """Result of a schema validation pass."""
    ok: bool
    missing_columns: List[str] = field(default_factory=list)
    extra_columns: List[str] = field(default_factory=list)
    null_counts: Dict[str, int] = field(default_factory=dict)
    out_of_range: Dict[str, int] = field(default_factory=dict)
    invalid_regimes: List[str] = field(default_factory=list)
    invalid_sources: List[str] = field(default_factory=list)
    duplicate_dates: List[str] = field(default_factory=list)
    non_monotonic_dates: bool = False
    row_count: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "row_count": self.row_count,
            "missing_columns": self.missing_columns,
            "extra_columns": self.extra_columns,
            "null_counts": self.null_counts,
            "out_of_range": self.out_of_range,
            "invalid_regimes": self.invalid_regimes,
            "invalid_sources": self.invalid_sources,
            "duplicate_dates": self.duplicate_dates,
            "non_monotonic_dates": self.non_monotonic_dates,
        }


def validate_helium_realtime(
    df: pd.DataFrame,
    *,
    strict: bool = False,
) -> ValidationReport:
    """
    Validate a helium realtime DataFrame against the canonical schema.

    ``strict=True`` requires exactly the canonical columns; ``strict=False``
    (default) allows a superset and warns about missing columns.
    """
    report = ValidationReport(ok=True, row_count=len(df))

    # 1. Column presence
    missing = [c for c in CANONICAL_COLUMNS if c not in df.columns]
    extra = [c for c in df.columns if c not in CANONICAL_COLUMNS]
    report.missing_columns = missing
    report.extra_columns = extra
    if missing:
        report.ok = False
    if strict and extra:
        report.ok = False

    # 2. Null counts
    for col in CANONICAL_COLUMNS:
        if col in df.columns:
            n = int(df[col].isna().sum())
            if n > 0:
                report.null_counts[col] = n
                report.ok = False

    # 3. Numeric bounds
    for col, (lo, hi) in NUMERIC_BOUNDS.items():
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        bad = 0
        if lo is not None:
            bad += int((s < lo).sum())
        if hi is not None:
            bad += int((s > hi).sum())
        if bad > 0:
            report.out_of_range[col] = bad
            report.ok = False

    # 4. Regime vocabulary
    if "market_regime" in df.columns:
        unknown = sorted(set(df["market_regime"].dropna().unique()) - set(VALID_REGIMES))
        if unknown:
            report.invalid_regimes = list(unknown)
            report.ok = False

    # 5. Source vocabulary
    if "source" in df.columns:
        unknown = sorted(set(df["source"].dropna().unique()) - set(VALID_SOURCES))
        if unknown:
            report.invalid_sources = list(unknown)
            report.ok = False

    # 6. Date checks
    if "date" in df.columns:
        dates = pd.to_datetime(df["date"], errors="coerce")
        if dates.isna().any():
            report.ok = False
            report.missing_columns.append("date(parse)")
        dupes = df.loc[dates.duplicated(keep=False), "date"].unique().tolist()
        if dupes:
            report.duplicate_dates = [str(d) for d in dupes]
            report.ok = False
        if not dates.is_monotonic_increasing:
            report.non_monotonic_dates = True
            report.ok = False

    return report


# ============================================================================
# Enhancement: build derived columns from base
# ============================================================================
_REGIME_MAP: Dict[str, str] = {
    "low": "low", "low scarcity": "low", "stable": "low",
    "moderate": "moderate", "moderate scarcity": "moderate", "normal": "moderate",
    "high": "high", "high scarcity": "high", "tightening": "high",
    "critical": "critical", "critical scarcity": "critical", "crisis": "critical",
}


def _normalize_regime(regime: Any) -> str:
    if regime is None:
        return "low"
    return _REGIME_MAP.get(str(regime).strip().lower(), "low")


def enhance_helium_realtime(
    base_df: pd.DataFrame,
    *,
    source: str = "synthetic_v2",
    seed: int = 42,
) -> pd.DataFrame:
    """
    Produce a fully-derived DataFrame from a base (v1) CSV.

    Adds all canonical columns; leaves the base columns intact.
    ``base_df`` must contain at least the columns in ``BASE_COLUMNS``.
    """
    missing = [c for c in BASE_COLUMNS if c not in base_df.columns]
    if missing:
        raise ValueError(f"Missing base columns: {missing}")

    df = base_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # row_id
    df["row_id"] = df["date"].dt.strftime("HEL-%Y-%m")

    # 1. demand_supply_ratio
    df["demand_supply_ratio"] = (
        df["global_demand_tonnes"] / df["global_production_tonnes"].replace(0, np.nan)
    ).round(4)

    # 2. helium_scarcity_impact
    df["helium_scarcity_impact"] = np.clip(
        0.6 * df["shortage_severity_0_1"] + 0.4 * df["supply_risk_score_0_1"],
        0.0, 1.0,
    ).round(3)

    # 3. price_volatility (6-month rolling std, seed 2.5 for the first 5 rows)
    vol = df["price_index"].rolling(6, min_periods=2).std()
    df["price_volatility"] = vol.fillna(2.5).round(2)

    # 4. market_regime from scarcity bands
    def _band(x: float) -> str:
        if x < 0.3:
            return "low"
        if x < 0.6:
            return "moderate"
        if x < 0.8:
            return "high"
        return "critical"

    df["market_regime"] = df["helium_scarcity_impact"].apply(_band)

    # 5. circularity_potential
    df["circularity_potential"] = (
        (df["recycling_rate_0_1"] + df["substitution_feasibility_0_1"]) / 2
    ).round(3)

    # 6. thermal_impact_factor
    df["thermal_impact_factor"] = (
        df["cooling_load_sensitivity"] * df["helium_scarcity_impact"]
    ).round(4)

    # 7. future_supply_potential_pct
    df["future_supply_potential_pct"] = (
        df["new_production_capacity_tonnes"]
        / df["global_production_tonnes"].replace(0, np.nan) * 100
    ).round(2)

    # 8. capacity_utilization_rate
    df["capacity_utilization_rate"] = (
        df["global_production_tonnes"]
        / (df["global_production_tonnes"] + df["new_production_capacity_tonnes"])
    ).round(4)

    # 9. carbon_intensity_associated
    df["carbon_intensity_associated"] = np.clip(
        300 + 200 * df["helium_scarcity_impact"], 50, 800
    ).round(0).astype(int)

    # 10. renewable_energy_pct
    df["renewable_energy_pct"] = np.clip(
        30 + 40 * (1 - df["helium_scarcity_impact"]), 5, 95
    ).round(1)

    # 11. esg_score
    df["esg_score"] = (
        (
            df["recycling_rate_0_1"] * 0.4
            + (1 - df["supply_risk_score_0_1"]) * 0.3
            + (1 - df["geopolitical_risk_index"]) * 0.3
        ) * 100
    ).round(1)

    # 12. regulatory_risk_score
    df["regulatory_risk_score"] = (
        df["geopolitical_risk_index"] * 0.5
        + df["logistics_disruption_index"] * 0.5
    ).round(3)

    # 13. data_quality_score: default 0.94, dip during anomalous months.
    # Detect anomalies as rows where volatility or logistics disruption
    # exceed typical envelopes.
    dq = np.full(len(df), 0.94)
    dq[df["price_volatility"] > df["price_volatility"].median() * 1.5] = 0.88
    dq[df["logistics_disruption_index"] > 0.5] = 0.88
    df["data_quality_score"] = dq.round(2)

    # 14. source + is_forecast
    df["source"] = source
    df["is_forecast"] = False

    # Ensure canonical column order, filling any missing with NaN.
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan
    return df[list(CANONICAL_COLUMNS)]


# ============================================================================
# Loader / writer
# ============================================================================
def load_helium_realtime(
    path: Union[str, Path] = DEFAULT_CSV_PATH,
    *,
    validate: bool = True,
    strict: bool = False,
) -> pd.DataFrame:
    """
    Load and validate the enhanced helium realtime CSV.

    Raises ``ValueError`` if validation fails and ``validate=True``.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Helium realtime CSV not found: {p}")

    df = pd.read_csv(p, parse_dates=["date"])
    if validate:
        report = validate_helium_realtime(df, strict=strict)
        if not report.ok:
            raise ValueError(
                f"Helium realtime validation failed: {json.dumps(report.as_dict(), indent=2)}"
            )
    return df


def write_helium_realtime(
    df: pd.DataFrame,
    path: Union[str, Path] = DEFAULT_CSV_PATH,
    *,
    write_metadata: bool = True,
    metadata_path: Optional[Union[str, Path]] = None,
) -> Tuple[Path, Optional[Path]]:
    """Write a validated DataFrame and (optionally) its metadata sidecar."""
    report = validate_helium_realtime(df, strict=False)
    if not report.ok:
        raise ValueError(
            f"Refusing to write invalid data: {json.dumps(report.as_dict(), indent=2)}"
        )

    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

    metadata_path_resolved: Optional[Path] = None
    if write_metadata:
        mp = Path(metadata_path) if metadata_path else out.with_suffix(out.suffix + ".metadata.json")
        mp.parent.mkdir(parents=True, exist_ok=True)
        payload = _build_metadata(df, out)
        mp.write_text(json.dumps(payload, indent=2, default=str))
        metadata_path_resolved = mp

    return out, metadata_path_resolved


def _build_metadata(df: pd.DataFrame, csv_path: Path) -> Dict[str, Any]:
    checksum = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    return {
        "schema_version": SCHEMA_VERSION,
        "module_version": __version__,
        "csv_path": str(csv_path),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "columns": list(df.columns),
        "date_range": [
            str(pd.to_datetime(df["date"]).min()),
            str(pd.to_datetime(df["date"]).max()),
        ],
        "checksum_sha256": checksum,
        "bounds": NUMERIC_BOUNDS,
        "valid_regimes": list(VALID_REGIMES),
        "valid_sources": list(VALID_SOURCES),
    }


# ============================================================================
# Offline enhancer (CLI)
# ============================================================================
def _cli_enhance(input_path: Path, output_path: Path, source: str) -> None:
    base = pd.read_csv(input_path)
    enhanced = enhance_helium_realtime(base, source=source)
    out, mp = write_helium_realtime(enhanced, output_path)
    print(f"Enhanced CSV written: {out}")
    if mp:
        print(f"Metadata written: {mp}")


def _cli_validate(path: Path, strict: bool) -> None:
    df = pd.read_csv(path, parse_dates=["date"])
    report = validate_helium_realtime(df, strict=strict)
    print(json.dumps(report.as_dict(), indent=2))
    if not report.ok:
        raise SystemExit(1)


def _cli_summary(path: Path) -> None:
    df = load_helium_realtime(path)
    print(df.describe(include="all").to_string())
    print(f"\nRegimes: {df['market_regime'].value_counts().to_dict()}")
    print(f"Sources: {df['source'].value_counts().to_dict()}")


# ============================================================================
# Tests
# ============================================================================
def _test_roundtrip(tmp_path: Path) -> None:
    base = pd.DataFrame({
        "date": pd.date_range("2020-01-31", periods=12, freq="ME"),
        "global_production_tonnes": np.linspace(27500, 29500, 12),
        "global_demand_tonnes": np.linspace(27800, 30000, 12),
        "price_index": np.linspace(102.5, 118.0, 12),
        "shortage_severity_0_1": np.linspace(0.12, 0.27, 12),
        "supply_risk_score_0_1": np.linspace(0.23, 0.34, 12),
        "recycling_rate_0_1": np.linspace(0.12, 0.16, 12),
        "substitution_feasibility_0_1": np.linspace(0.09, 0.14, 12),
        "cooling_load_sensitivity": np.linspace(0.95, 1.01, 12),
        "geopolitical_risk_index": np.linspace(0.32, 0.43, 12),
        "logistics_disruption_index": np.linspace(0.18, 0.29, 12),
        "new_production_capacity_tonnes": np.linspace(2500, 3600, 12),
    })
    enhanced = enhance_helium_realtime(base)
    assert list(enhanced.columns) == list(CANONICAL_COLUMNS)
    assert len(enhanced) == 12

    out = tmp_path / "helium_realtime.csv"
    csv_path, meta_path = write_helium_realtime(enhanced, out)
    assert csv_path.exists()
    assert meta_path is not None and meta_path.exists()

    loaded = load_helium_realtime(csv_path)
    pd.testing.assert_frame_equal(
        loaded.sort_values("date").reset_index(drop=True).drop(columns=["date"]),
        enhanced.sort_values("date").reset_index(drop=True).drop(columns=["date"]),
        check_dtype=False,
    )


def _test_validation_rejects_bad(tmp_path: Path) -> None:
    df = pd.DataFrame({
        "date": ["2020-01-31", "2020-02-29"],
        "row_id": ["HEL-2020-01", "HEL-2020-02"],
    })
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = 0.5
    df.loc[0, "market_regime"] = "banana"
    df.loc[1, "recycling_rate_0_1"] = 1.5

    report = validate_helium_realtime(df)
    assert not report.ok
    assert "market_regime" in report.invalid_regimes or \
           "banana" in report.invalid_regimes or \
           report.invalid_regimes
    assert "recycling_rate_0_1" in report.out_of_range


def _test_derived_match_manual(tmp_path: Path) -> None:
    base = pd.DataFrame({
        "date": ["2020-01-31"],
        "global_production_tonnes": [27500.0],
        "global_demand_tonnes": [27800.0],
        "price_index": [102.5],
        "shortage_severity_0_1": [0.12],
        "supply_risk_score_0_1": [0.23],
        "recycling_rate_0_1": [0.12],
        "substitution_feasibility_0_1": [0.09],
        "cooling_load_sensitivity": [0.95],
        "geopolitical_risk_index": [0.32],
        "logistics_disruption_index": [0.18],
        "new_production_capacity_tonnes": [2500.0],
    })
    enhanced = enhance_helium_realtime(base)
    row = enhanced.iloc[0]
    assert abs(row["demand_supply_ratio"] - 27800/27500) < 1e-3
    assert abs(row["helium_scarcity_impact"] - 0.164) < 1e-3
    assert abs(row["circularity_potential"] - 0.105) < 1e-3
    assert abs(row["thermal_impact_factor"] - 0.95 * 0.164) < 1e-3
    assert row["market_regime"] == "low"
    assert abs(row["regulatory_risk_score"] - 0.25) < 1e-3


def _run_all_tests() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        _test_roundtrip(tmp)
        _test_validation_rejects_bad(tmp)
        _test_derived_match_manual(tmp)
    print("All helium_realtime_csv tests passed.")


# ============================================================================
# Entry point
# ============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="helium_realtime CSV tools")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_enh = sub.add_parser("enhance", help="Add derived columns to a base CSV")
    p_enh.add_argument("input", type=Path)
    p_enh.add_argument("output", type=Path)
    p_enh.add_argument("--source", default="synthetic_v2")

    p_val = sub.add_parser("validate", help="Validate a CSV against the schema")
    p_val.add_argument("path", type=Path)
    p_val.add_argument("--strict", action="store_true")

    p_sum = sub.add_parser("summary", help="Print a DataFrame summary")
    p_sum.add_argument("path", type=Path)

    sub.add_parser("test", help="Run the built-in test suite")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    if args.cmd == "enhance":
        _cli_enhance(args.input, args.output, args.source)
    elif args.cmd == "validate":
        _cli_validate(args.path, args.strict)
    elif args.cmd == "summary":
        _cli_summary(args.path)
    elif args.cmd == "test":
        _run_all_tests()


if __name__ == "__main__":
    main()
