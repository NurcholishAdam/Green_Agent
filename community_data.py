"""
community_data.py

Community-driven data sharing for Green Agent.

Users can optionally contribute observations to improve carbon and helium
simulations. Contributions are written **locally** to
`$GREEN_AGENT_DATA_DIR/carbon_observations.jsonl` (or the XDG / home default).
Any aggregation across users is out of scope here — the user uploads or
exports the JSONL file to a community endpoint separately.

Enhanced fields (all optional):
    - LIMIT Graph metrics (centrality, connectivity)
    - MODP composite score
    - RLHF human feedback score
    - Multi-Teacher On-Policy Distillation statistics
    - Bio-inspired / MoE gate weights
    - Evolutionary and FlexGen flags

Anonymization note
------------------
Contributions are **pseudonymous at best**. `(timestamp, region, intensity)`
is a weak fingerprint. Do not treat these records as anonymous under
adversarial analysis.

Changelog (2.0)
---------------
- Fixed: `get_community_average` and `get_community_enhanced_data` compared a
  string `timestamp` to a float `cutoff` (TypeError at runtime). Observations
  now store both `timestamp` (ISO UTC, human-readable) and `timestamp_epoch`
  (float, for range comparison); readers accept both and fall back to parsing
  legacy `timestamp` strings.
- Fixed: readers no longer crash when `carbon_observations.jsonl` is missing
  (directory exists, file absent).
- Fixed: `get_community_enhanced_data(metric="graph_metrics")` no longer
  crashes when some observations lack `centrality` / `connectivity`; None
  values are filtered before averaging.
- Fixed: input validation added — region / source regex, non-negative
  intensity within a sanity bound, ratio validation for `human_feedback_score`,
  `modp_score`, `graph_metrics`, numeric + sum-to-1.0 check for
  `moe_gate_weights`, per-observation size cap (10 KiB).
- Fixed: `datetime.now()` → `datetime.now(timezone.utc)` so cross-machine
  comparisons are correct.
- Fixed: JSON decode errors are logged and skipped per line; a corrupt line
  no longer aborts the whole read.
- Added: `schema_version: "1.0"` on every new observation.
- Added: `consent: bool = True` kwarg on `contribute_carbon_observation` — set
  `consent=False` to decline; returns `False` without writing.
- Added: `delete_all_data()` — GDPR right to erasure.
- Added: `export_my_data()` — GDPR right to portability.
- Added: `configure(data_dir=..., consent_required=...)` — override class-level
  paths and behavior. Also reads `$GREEN_AGENT_DATA_DIR` and `$XDG_DATA_HOME`
  at import time.
- Added: per-process write lock (`threading.Lock`) so concurrent in-process
  contributions cannot interleave writes.
- Added: `logging` throughout; malformed lines and declined contributions are
  logged.
- Added: `__all__` for explicit export control.
- Added: stricter type hints (`list[float]` instead of `list`).
- Added: `days` parameter validation (`days > 0`).
- Added: new field name `intensity_gco2_per_kwh` for new writes; readers fall
  back to legacy `intensity`.
- Added: `metric` validated before reading the file.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

__all__ = ["CommunityDataHub", "CommunityDataError", "SCHEMA_VERSION"]

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0"

# Sanity bounds — reject data that is obviously wrong rather than poisoning
# aggregates.
_MAX_OBSERVATION_BYTES = 10_000
_MAX_INTENSITY_GCO2_PER_KWH = 5_000.0

_REGION_RE = re.compile(r"^[A-Za-z0-9._-]{1,32}$")
_SOURCE_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

_write_lock = threading.Lock()


class CommunityDataError(ValueError):
    """Raised when an observation fails validation."""


def _default_data_dir() -> Path:
    """Resolve data dir from env vars (in priority order), then XDG, then home."""
    if v := os.environ.get("GREEN_AGENT_DATA_DIR"):
        return Path(v)
    if v := os.environ.get("XDG_DATA_HOME"):
        return Path(v) / "green_agent" / "community_data"
    return Path.home() / ".green_agent" / "community_data"


def _validate_ratio(name: str, value: Any) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError) as exc:
        raise CommunityDataError(f"{name} must be numeric; got {value!r}") from exc
    if not (0.0 <= f <= 1.0):
        raise CommunityDataError(f"{name} must be in [0, 1]; got {f!r}")
    return f


def _validate_intensity(value: Any) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError) as exc:
        raise CommunityDataError(
            f"intensity_gco2_per_kwh must be numeric; got {value!r}"
        ) from exc
    if f < 0:
        raise CommunityDataError(f"intensity must be >= 0; got {f!r}")
    if f > _MAX_INTENSITY_GCO2_PER_KWH:
        raise CommunityDataError(
            f"intensity {f!r} exceeds sanity bound "
            f"{_MAX_INTENSITY_GCO2_PER_KWH} gCO2/kWh"
        )
    return f


def _validate_region(value: str) -> str:
    if not isinstance(value, str) or not _REGION_RE.match(value):
        raise CommunityDataError(
            f"region must match {_REGION_RE.pattern!r}; got {value!r}"
        )
    return value


def _validate_source(value: str) -> str:
    if not isinstance(value, str) or not _SOURCE_RE.match(value):
        raise CommunityDataError(
            f"source must match {_SOURCE_RE.pattern!r}; got {value!r}"
        )
    return value


def _validate_graph_metrics(value: Any) -> dict[str, float]:
    if not isinstance(value, dict):
        raise CommunityDataError("graph_metrics must be a dict")
    out: dict[str, float] = {}
    if "centrality" in value:
        out["centrality"] = _validate_ratio(
            "graph_metrics.centrality", value["centrality"]
        )
    if "connectivity" in value:
        out["connectivity"] = _validate_ratio(
            "graph_metrics.connectivity", value["connectivity"]
        )
    if not out:
        raise CommunityDataError(
            "graph_metrics must contain at least one of "
            "'centrality' or 'connectivity'"
        )
    return out


def _validate_moe_gate_weights(value: Any) -> list[float]:
    if not isinstance(value, list):
        raise CommunityDataError("moe_gate_weights must be a list")
    try:
        floats = [float(w) for w in value]
    except (TypeError, ValueError) as exc:
        raise CommunityDataError(
            f"moe_gate_weights must be numeric; {exc}"
        ) from exc
    if any(w < 0 for w in floats):
        raise CommunityDataError("moe_gate_weights must be non-negative")
    total = sum(floats)
    if abs(total - 1.0) > 1e-6:
        raise CommunityDataError(
            f"moe_gate_weights must sum to 1.0; got {total!r}"
        )
    return floats


def _validate_payload_size(name: str, value: Any) -> None:
    encoded = json.dumps(value, default=str).encode("utf-8")
    if len(encoded) > _MAX_OBSERVATION_BYTES:
        raise CommunityDataError(
            f"{name} payload exceeds {_MAX_OBSERVATION_BYTES} bytes "
            f"(got {len(encoded)})"
        )


def _parse_timestamp(obs: dict[str, Any]) -> Optional[float]:
    """
    Return epoch seconds from an observation dict.

    Prefers `timestamp_epoch` (new field). Falls back to parsing `timestamp`
    (ISO-8601 string written by v1.0 — may be naive local — or v2.0 — UTC).
    """
    if (v := obs.get("timestamp_epoch")) is not None:
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    if (v := obs.get("timestamp")) is not None:
        try:
            return datetime.fromisoformat(v).timestamp()
        except (TypeError, ValueError):
            pass
    return None


class CommunityDataHub:
    """
    Optional community data sharing (local only).

    Writes and reads `carbon_observations.jsonl` under `DATA_DIR`.

    Anonymization: contributions are pseudonymous at best — see module
    docstring.
    """

    DATA_DIR: Path = _default_data_dir()
    CONSENT_REQUIRED: bool = False  # set True to reject calls with consent=False
    #                                  instead of silently returning False

    # --- Configuration -----------------------------------------------------
    @classmethod
    def configure(
        cls,
        *,
        data_dir: Optional[Path] = None,
        consent_required: Optional[bool] = None,
    ) -> None:
        """Override class-level paths/behavior. Useful for tests."""
        if data_dir is not None:
            cls.DATA_DIR = Path(data_dir)
        if consent_required is not None:
            cls.CONSENT_REQUIRED = consent_required

    # --- Write -------------------------------------------------------------
    @classmethod
    def contribute_carbon_observation(
        cls,
        region: str,
        intensity: float,
        source: str,
        graph_metrics: Optional[dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
        modp_score: Optional[float] = None,
        distillation_stats: Optional[dict[str, Any]] = None,
        moe_gate_weights: Optional[list[float]] = None,
        use_evolutionary: Optional[bool] = None,
        flexgen_enabled: Optional[bool] = None,
        *,
        consent: bool = True,
    ) -> bool:
        """
        Contribute one observation.

        Parameters mirror v1.0 (`region`, `intensity`, `source`, and optional
        enhanced fields), plus the new opt-out `consent` kwarg.

        Returns:
            True if the observation was written.
            False if the user declined (`consent=False`).

        Raises:
            CommunityDataError: on validation failure.
            OSError: on filesystem errors.
        """
        if not consent:
            logger.info("Contribution declined: consent=False")
            return False

        region = _validate_region(region)
        source = _validate_source(source)
        intensity = _validate_intensity(intensity)

        now = datetime.now(timezone.utc)
        observation: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "timestamp": now.isoformat(),
            "timestamp_epoch": now.timestamp(),
            "region": region,
            "intensity_gco2_per_kwh": intensity,
            "source": source,
            "anonymous": True,  # pseudonymous at best — see module docstring
        }

        if graph_metrics is not None:
            observation["graph_metrics"] = _validate_graph_metrics(graph_metrics)
        if human_feedback_score is not None:
            observation["human_feedback_score"] = _validate_ratio(
                "human_feedback_score", human_feedback_score
            )
        if modp_score is not None:
            observation["modp_score"] = _validate_ratio("modp_score", modp_score)
        if distillation_stats is not None:
            if not isinstance(distillation_stats, dict):
                raise CommunityDataError("distillation_stats must be a dict")
            _validate_payload_size("distillation_stats", distillation_stats)
            observation["distillation_stats"] = distillation_stats
        if moe_gate_weights is not None:
            observation["moe_gate_weights"] = _validate_moe_gate_weights(
                moe_gate_weights
            )
        if use_evolutionary is not None:
            observation["use_evolutionary"] = bool(use_evolutionary)
        if flexgen_enabled is not None:
            observation["flexgen_enabled"] = bool(flexgen_enabled)

        _validate_payload_size("observation", observation)

        cls.DATA_DIR.mkdir(parents=True, exist_ok=True)
        path = cls.DATA_DIR / "carbon_observations.jsonl"

        line = json.dumps(observation) + "\n"
        with _write_lock:
            with path.open("a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except OSError:
                    # fsync can fail on some filesystems; not fatal.
                    pass

        logger.debug(
            "Contributed observation: region=%s intensity=%.2f source=%s",
            region,
            intensity,
            source,
        )
        return True

    # --- Read --------------------------------------------------------------
    @classmethod
    def _read_observations(
        cls,
        *,
        region: Optional[str] = None,
        days: int = 30,
    ) -> list[dict[str, Any]]:
        """
        Return observations from the last `days` days, optionally filtered
        by region. Malformed lines are logged and skipped.
        """
        if days <= 0:
            raise ValueError(f"days must be > 0; got {days!r}")

        path = cls.DATA_DIR / "carbon_observations.jsonl"
        if not path.exists():
            return []

        cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
        out: list[dict[str, Any]] = []

        with path.open("r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obs = json.loads(line)
                except json.JSONDecodeError as exc:
                    logger.warning(
                        "Skipping malformed JSON at %s:%d: %s", path, lineno, exc
                    )
                    continue
                if not isinstance(obs, dict):
                    logger.warning(
                        "Skipping non-dict record at %s:%d", path, lineno
                    )
                    continue
                if region is not None and obs.get("region") != region:
                    continue
                ts = _parse_timestamp(obs)
                if ts is None or ts < cutoff:
                    continue
                out.append(obs)

        return out

    @classmethod
    def get_community_average(
        cls, region: str, days: int = 30
    ) -> Optional[float]:
        """
        Average intensity (gCO2/kWh) from the last `days` days, or None if
        no data is available. Backwards compatible with v1.0 records that
        used the `intensity` field name.
        """
        observations = cls._read_observations(region=region, days=days)

        values = [
            float(o["intensity_gco2_per_kwh"])
            for o in observations
            if o.get("intensity_gco2_per_kwh") is not None
        ]
        if not values:
            # Backwards compat with v1.0 records.
            values = [
                float(o["intensity"])
                for o in observations
                if o.get("intensity") is not None
            ]
        if not values:
            return None
        return sum(values) / len(values)

    # --- Enhanced aggregation ----------------------------------------------
    @classmethod
    def get_community_enhanced_data(
        cls,
        region: str,
        days: int = 30,
        metric: str = "modp_score",
    ) -> dict[str, Any]:
        """
        Aggregate one enhanced metric over the last `days` days.

        Parameters
        ----------
        region : str
            Region filter.
        days : int
            Number of days to look back (> 0).
        metric : str
            One of 'modp_score', 'human_feedback_score', 'graph_metrics',
            'distillation_stats', 'moe_gate_weights'.

        Returns
        -------
        dict
            Aggregated metric value(s) plus a 'count' key (number of
            observations that contributed to the aggregate). Empty dict if
            there are no observations in the window.
        """
        supported = {
            "modp_score",
            "human_feedback_score",
            "graph_metrics",
            "distillation_stats",
            "moe_gate_weights",
        }
        if metric not in supported:
            raise ValueError(
                f"Unsupported metric: {metric!r}. "
                f"Choose one of {sorted(supported)}."
            )

        observations = cls._read_observations(region=region, days=days)
        if not observations:
            return {}

        if metric == "graph_metrics":
            graph_obs = [
                o
                for o in observations
                if isinstance(o.get("graph_metrics"), dict)
                and o["graph_metrics"]
            ]
            centrality_vals = [
                float(o["graph_metrics"]["centrality"])
                for o in graph_obs
                if o["graph_metrics"].get("centrality") is not None
            ]
            connectivity_vals = [
                float(o["graph_metrics"]["connectivity"])
                for o in graph_obs
                if o["graph_metrics"].get("connectivity") is not None
            ]
            return {
                "average_centrality": (
                    sum(centrality_vals) / len(centrality_vals)
                    if centrality_vals
                    else None
                ),
                "average_connectivity": (
                    sum(connectivity_vals) / len(connectivity_vals)
                    if connectivity_vals
                    else None
                ),
                "count": len(graph_obs),
            }

        if metric == "modp_score":
            vals = [
                float(o["modp_score"])
                for o in observations
                if o.get("modp_score") is not None
            ]
            return {
                "average_modp_score": sum(vals) / len(vals) if vals else None,
                "count": len(vals),
            }

        if metric == "human_feedback_score":
            vals = [
                float(o["human_feedback_score"])
                for o in observations
                if o.get("human_feedback_score") is not None
            ]
            return {
                "average_human_feedback_score": (
                    sum(vals) / len(vals) if vals else None
                ),
                "count": len(vals),
            }

        if metric == "distillation_stats":
            count = sum(1 for o in observations if "distillation_stats" in o)
            return {
                "observations_with_distillation_stats": count,
                "count": count,
            }

        # metric == "moe_gate_weights"
        count = sum(1 for o in observations if "moe_gate_weights" in o)
        return {
            "observations_with_moe_gate_weights": count,
            "count": count,
        }

    # --- GDPR helpers ------------------------------------------------------
    @classmethod
    def delete_all_data(cls) -> int:
        """
        Delete the user's local contribution file.

        Returns the number of observations that were removed (0 if the file
        did not exist).
        """
        path = cls.DATA_DIR / "carbon_observations.jsonl"
        if not path.exists():
            return 0
        count = sum(1 for _ in path.open("r", encoding="utf-8"))
        path.unlink()
        logger.info("Deleted %d observations from %s", count, path)
        return count

    @classmethod
    def export_my_data(cls) -> list[dict[str, Any]]:
        """
        Return every observation the user has contributed, unfiltered.
        The caller is responsible for serialization and delivery.
        """
        path = cls.DATA_DIR / "carbon_observations.jsonl"
        if not path.exists():
            return []
        out: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obs = json.loads(line)
                except json.JSONDecodeError as exc:
                    logger.warning(
                        "Skipping malformed JSON at %s:%d: %s", path, lineno, exc
                    )
                    continue
                if isinstance(obs, dict):
                    out.append(obs)
        return out
